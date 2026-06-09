"""CockroachSync — 本地 SQLite → CockroachDB 同步（RU 优化版）

RU 优化要点：
- 单次连接批量处理全部记录（不再逐条 acquire/release）
- INSERT ... ON CONFLICT DO NOTHING 替代 SELECT + INSERT（省 1 次往返/条）
- 批量写入 code_bot_mapping（1 次事务内完成）
- 移除逐条 sleep(0.1)，改为批量提交后短暂停顿
- 支持共享外部连接池（避免多池占用）
"""

import json
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from loguru import logger

from config import settings
from database import Storage


class CockroachSync:
    def __init__(self, storage: Storage, db_pool: Optional[asyncpg.Pool] = None):
        self.storage = storage
        self._pool: Optional[asyncpg.Pool] = db_pool  # 支持共享外部连接池
        self._owns_pool = False

    async def connect(self) -> bool:
        if not settings.COCKROACHDB_URL:
            logger.warning("[Sync] COCKROACHDB_URL 未配置，跳过同步")
            return False
        if self._pool and not getattr(self._pool, "_closed", False):
            return True
        try:
            self._pool = await asyncpg.create_pool(
                settings.COCKROACHDB_URL,
                min_size=1,
                max_size=2,  # 从 3 降到 2，减少空闲连接 RU
                statement_cache_size=0,
            )
            self._owns_pool = True
            logger.info("[Sync] 已连接到 CockroachDB")
            return True
        except Exception as e:
            logger.error(f"[Sync] 连接 CockroachDB 失败: {e}")
            return False

    async def close(self):
        if self._owns_pool and self._pool:
            await self._pool.close()
            self._pool = None

    # ── 核心：批量同步文件码 ──────────────────────────────────

    async def sync_codes(self, limit: int = 500) -> int:
        """批量同步未导出的文件码到 CockroachDB。

        RU 优化：单连接批量 INSERT ... ON CONFLICT DO NOTHING，
        原来 4 次往返/条 → 现在每批只做 2 次批量查询。
        """
        if not self._pool or getattr(self._pool, "_closed", False):
            ok = await self.connect()
            if not ok:
                return 0

        codes = self.storage.get_uneported_codes(limit=limit)
        if not codes:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        synced = 0

        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 1) 批量插入 file_records（单条 SQL，每个 code 一行）
                    file_values = []
                    file_params = []
                    for i, code_row in enumerate(codes):
                        code = code_row["code"]
                        file_values.append(
                            f"(${i * 8 + 1}, ${i * 8 + 2}, ${i * 8 + 3}, "
                            f"${i * 8 + 4}, ${i * 8 + 5}, ${i * 8 + 6}, "
                            f"${i * 8 + 7}, ${i * 8 + 8})"
                        )
                        file_params.extend([
                            code, 0, 0, 0,
                            json.dumps({"external": True}),
                            "active", 0, now,
                        ])

                    if file_values:
                        await conn.execute(
                            f"""INSERT INTO file_records
                                (file_code, uploader_id, primary_channel_id,
                                 primary_channel_msg_id, file_types,
                                 status, request_count, create_time)
                                VALUES {','.join(file_values)}
                                ON CONFLICT (file_code) DO NOTHING""",
                            *file_params,
                        )

                    # 2) 批量插入 code_bot_mapping（去重）
                    bot_values = []
                    bot_params = []
                    for i, code_row in enumerate(codes):
                        code = code_row["code"]
                        bot_username = code_row.get("bot_username", "")
                        if not bot_username:
                            continue
                        idx = len(bot_values)
                        bot_values.append(f"(${idx * 3 + 1}, ${idx * 3 + 2}, ${idx * 3 + 3})")
                        bot_params.extend([code, bot_username, now])

                    if bot_values:
                        await conn.execute(
                            f"""INSERT INTO code_bot_mapping (code, bot_username, created_at)
                                VALUES {','.join(bot_values)}
                                ON CONFLICT (code) DO NOTHING""",
                            *bot_params,
                        )

            # 3) 标记本地已导出
            code_ids = [c["id"] for c in codes if c.get("id")]
            if code_ids:
                self.storage.mark_exported(code_ids)

            synced = len(codes)
            logger.info(f"[Sync] 批量同步完成: {synced} 个文件码")

        except Exception as e:
            logger.error(f"[Sync] 批量同步失败: {e}")

        return synced

    async def sync_all(self, batch_size: int = 500) -> int:
        total = 0
        while True:
            count = await self.sync_codes(limit=batch_size)
            total += count
            if count < batch_size:
                break
        return total

    # ── 批量同步已解析记录 ────────────────────────────────────

    async def sync_resolved_records(self, limit: int = 200) -> int:
        """批量同步已解析（有存储位置）的记录到 CockroachDB。"""
        if not self._pool or getattr(self._pool, "_closed", False):
            ok = await self.connect()
            if not ok:
                return 0

        records = self.storage.get_resolved_unsynced(limit=limit)
        if not records:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        synced = 0

        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 1) 批量 INSERT/UPDATE file_records
                    file_values = []
                    file_params = []
                    for i, row in enumerate(records):
                        code = row["code"]
                        storage_channel_id = row.get("storage_channel_id", 0) or 0
                        storage_msg_id = row.get("storage_msg_id", 0) or 0
                        batch_msg_ids = row.get("storage_batch_msg_ids", "") or ""

                        file_values.append(
                            f"(${i * 9 + 1}, ${i * 9 + 2}, ${i * 9 + 3}, "
                            f"${i * 9 + 4}, ${i * 9 + 5}, ${i * 9 + 6}, "
                            f"${i * 9 + 7}, ${i * 9 + 8}, ${i * 9 + 9})"
                        )
                        file_params.extend([
                            code, 0, storage_channel_id, storage_msg_id,
                            batch_msg_ids,
                            json.dumps({"external": True}),
                            "active", 0, now,
                        ])

                    if file_values:
                        await conn.execute(
                            f"""INSERT INTO file_records
                                (file_code, uploader_id, primary_channel_id,
                                 primary_channel_msg_id, batch_msg_ids,
                                 file_types, status, request_count, create_time)
                                VALUES {','.join(file_values)}
                                ON CONFLICT (file_code) DO UPDATE SET
                                    primary_channel_id = EXCLUDED.primary_channel_id,
                                    primary_channel_msg_id = EXCLUDED.primary_channel_msg_id,
                                    batch_msg_ids = EXCLUDED.batch_msg_ids,
                                    status = 'active'""",
                            *file_params,
                        )

                    # 2) 批量插入 code_bot_mapping
                    bot_values = []
                    bot_params = []
                    for row in records:
                        bot_username = row.get("bot_username", "")
                        if not bot_username:
                            continue
                        idx = len(bot_values)
                        bot_values.append(f"(${idx * 3 + 1}, ${idx * 3 + 2}, ${idx * 3 + 3})")
                        bot_params.extend([row["code"], bot_username, now])

                    if bot_values:
                        await conn.execute(
                            f"""INSERT INTO code_bot_mapping (code, bot_username, created_at)
                                VALUES {','.join(bot_values)}
                                ON CONFLICT (code) DO NOTHING""",
                            *bot_params,
                        )

            # 3) 标记本地已同步
            synced_ids = [r["id"] for r in records if r.get("id")]
            if synced_ids:
                self.storage.mark_crdb_synced(synced_ids)

            synced = len(records)
            logger.info(f"[Sync] 已解析记录批量同步完成: {synced} 条")

        except Exception as e:
            logger.error(f"[Sync] 批量同步已解析记录失败: {e}")

        return synced

    async def sync_all_resolved(self, batch_size: int = 200) -> int:
        total = 0
        while True:
            count = await self.sync_resolved_records(limit=batch_size)
            total += count
            if count < batch_size:
                break
        return total