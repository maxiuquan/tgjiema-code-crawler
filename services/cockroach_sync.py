import asyncio
import json
from datetime import datetime, timezone
from typing import List, Optional

import asyncpg
from loguru import logger

from config import settings
from database import Storage


class CockroachSync:
    def __init__(self, storage: Storage):
        self.storage = storage
        self._pool: Optional[asyncpg.Pool] = None
        self._last_resolved_sync: float = 0

    async def connect(self) -> bool:
        if not settings.COCKROACHDB_URL:
            logger.warning("[Sync] COCKROACHDB_URL 未配置，跳过同步")
            return False
        try:
            self._pool = await asyncpg.create_pool(
                settings.COCKROACHDB_URL,
                min_size=1,
                max_size=2,
                statement_cache_size=0,
            )
            logger.info("[Sync] 已连接到 CockroachDB")
            return True
        except Exception as e:
            logger.error(f"[Sync] 连接 CockroachDB 失败: {e}")
            return False

    async def close(self):
        if self._pool:
            await self._pool.close()

    async def sync_codes(self, limit: int = 500) -> int:
        if not self._pool or getattr(self._pool, '_closed', False):
            ok = await self.connect()
            if not ok:
                return 0

        codes = self.storage.get_uneported_codes(limit=limit)
        if not codes:
            logger.info("[Sync] 没有新码需要同步")
            return 0

        now = datetime.now(timezone.utc).isoformat()
        synced = 0
        exported_ids = []

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for code_row in codes:
                    code = code_row["code"]
                    bot_username = code_row.get("bot_username", "")
                    code_id = code_row.get("id")

                    try:
                        # 检查 file_records 是否已存在
                        existing = await conn.fetchval(
                            "SELECT 1 FROM file_records WHERE file_code = $1", code
                        )
                        if not existing:
                            await conn.execute(
                                """INSERT INTO file_records
                                   (file_code, uploader_id, primary_channel_id, primary_channel_msg_id,
                                    backup_channel_msg_ids, batch_msg_ids, batch_file_meta, file_ids,
                                    file_types, status, request_count, create_time, expire_time)
                                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
                                code, 0, 0, 0, None, None, None, None,
                                json.dumps({"external": True}),
                                "active", 0, now, None,
                            )

                            # 检查 code_bot_mapping 是否已存在
                            existing_cb = await conn.fetchval(
                                "SELECT 1 FROM code_bot_mapping WHERE code_prefix = $1", code
                            )
                            if not existing_cb:
                                await conn.execute(
                                    """INSERT INTO code_bot_mapping (code_prefix, bot_username, created_at)
                                       VALUES ($1, $2, $3)""",
                                    code, bot_username, now,
                                )

                        exported_ids.append(code_id) if code_id else None
                        synced += 1

                    except Exception as e:
                        logger.error(f"[Sync] 同步码 {code} 失败: {e}")

        if exported_ids:
            self.storage.mark_exported(exported_ids)

        logger.info(f"[Sync] 同步完成: 已同步 {synced} 个文件码到 CockroachDB")
        return synced

    async def sync_all(self, batch_size: int = 500) -> int:
        total = 0
        while True:
            count = await self.sync_codes(limit=batch_size)
            total += count
            if count < batch_size:
                break
        return total

    async def sync_resolved_records(self, limit: int = 200) -> int:
        if not self._pool or getattr(self._pool, '_closed', False):
            ok = await self.connect()
            if not ok:
                return 0

        records = self.storage.get_resolved_unsynced(limit=limit)
        if not records:
            return 0

        synced = 0
        synced_ids = []

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for row in records:
                    code = row["code"]
                    bot_username = row.get("bot_username", "")
                    storage_channel_id = row.get("storage_channel_id", 0) or 0
                    storage_msg_id = row.get("storage_msg_id", 0) or 0
                    batch_msg_ids = row.get("storage_batch_msg_ids", "") or ""
                    row_id = row.get("id")

                    try:
                        now = datetime.now(timezone.utc).isoformat()
                        existing = await conn.fetchval(
                            "SELECT 1 FROM file_records WHERE file_code = $1", code
                        )

                        if existing:
                            await conn.execute(
                                """UPDATE file_records SET
                                   primary_channel_id = $1,
                                   primary_channel_msg_id = $2,
                                   batch_msg_ids = $3,
                                   status = 'active'
                                   WHERE file_code = $4""",
                                storage_channel_id,
                                storage_msg_id,
                                batch_msg_ids,
                                code,
                            )
                        else:
                            await conn.execute(
                                """INSERT INTO file_records
                                   (file_code, uploader_id, primary_channel_id, primary_channel_msg_id,
                                    backup_channel_msg_ids, batch_msg_ids, batch_file_meta, file_ids,
                                    file_types, status, request_count, create_time, expire_time)
                                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
                                code, 0, storage_channel_id, storage_msg_id,
                                None, batch_msg_ids, None, None,
                                json.dumps({"external": True}),
                                "active", 0, now, None,
                            )

                        # 检查 code_bot_mapping 是否已存在
                        existing_cb = await conn.fetchval(
                            "SELECT 1 FROM code_bot_mapping WHERE code_prefix = $1", code
                        )
                        if not existing_cb:
                            await conn.execute(
                                """INSERT INTO code_bot_mapping (code_prefix, bot_username, created_at)
                                   VALUES ($1, $2, $3)""",
                                code, bot_username, now,
                            )

                        if row_id:
                            synced_ids.append(row_id)
                        synced += 1

                    except Exception as e:
                        logger.error(f"[Sync] 同步解析记录 {code} 失败: {e}")

        if synced_ids:
            self.storage.mark_crdb_synced(synced_ids)

        if synced > 0:
            logger.info(f"[Sync] 已解析记录同步完成: {synced} 条")
        return synced

    async def sync_all_resolved(self, batch_size: int = 200) -> int:
        total = 0
        while True:
            count = await self.sync_resolved_records(limit=batch_size)
            total += count
            if count < batch_size:
                break
        return total