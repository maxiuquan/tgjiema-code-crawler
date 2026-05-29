import asyncio
import json
from datetime import datetime, timezone
from typing import List, Optional

import asyncpg
from loguru import logger

from config import settings
from storage import Storage


class CockroachSync:
    def __init__(self, storage: Storage):
        self.storage = storage
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> bool:
        if not settings.COCKROACHDB_URL:
            logger.warning("[Sync] COCKROACHDB_URL 未配置，跳过同步")
            return False
        try:
            self._pool = await asyncpg.create_pool(
                settings.COCKROACHDB_URL,
                min_size=1,
                max_size=3,
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

        synced = 0
        for code_row in codes:
            try:
                async with self._pool.acquire() as conn:
                    code = code_row["code"]
                    bot_username = code_row.get("bot_username", "")

                    existing = await conn.fetchrow(
                        "SELECT file_code FROM file_records WHERE file_code = $1", code
                    )
                    if existing:
                        logger.debug(f"[Sync] 码已存在，跳过: {code}")
                    else:
                        now = datetime.now(timezone.utc).isoformat()
                        await conn.execute(
                            """INSERT INTO file_records
                               (file_code, uploader_id, primary_channel_id, primary_channel_msg_id,
                                file_types, status, request_count, create_time)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
                            code, 0, 0, 0, json.dumps({"external": True}),
                            "active", 0, now,
                        )

                        code_bot = await conn.fetchrow(
                            "SELECT code FROM code_bot_mapping WHERE code = $1", code
                        )
                        if not code_bot:
                            await conn.execute(
                                """INSERT INTO code_bot_mapping (code, bot_username, created_at)
                                   VALUES ($1, $2, $3)""",
                                code, bot_username, now,
                            )

                    if code_row["id"]:
                        self.storage.mark_exported([code_row["id"]])
                    synced += 1

            except Exception as e:
                logger.error(f"[Sync] 同步码 {code_row.get('code', '?')} 失败: {e}")

            await asyncio.sleep(0.1)

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