"""CodeMapper — 外部码 ↔ 系统码 映射管理（RU 优化版 v3）
将外部码（如 QQfile2_bot:xxx）映射到主系统生成的系统码（tgwenjian_xxx）。

RU 优化要点：
- 不再复制 file_records（主系统通过 external_code_mapping → 系统码 → file_records 查询，复制行是冗余写入）
- 批量写入使用 executemany，减少事务开销
- code_bot_mapping 使用 INSERT ON CONFLICT DO NOTHING（幂等）
"""

from datetime import datetime, timezone
from typing import Optional

import asyncpg
from loguru import logger


class CodeMapper:
    """管理外部码映射，直写主系统 CockroachDB。"""

    def __init__(self, db_pool: asyncpg.Pool):
        self._pool = db_pool

    @property
    def pool(self) -> asyncpg.Pool:
        """暴露连接池供外部复用（避免创建多个独立连接池）。"""
        return self._pool

    async def init_tables(self):
        """确保相关表存在。"""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS external_code_mapping (
                    external_code TEXT PRIMARY KEY,
                    system_code TEXT NOT NULL,
                    bot_username TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_external_code_mapping_system
                ON external_code_mapping(system_code)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_external_code_mapping_bot
                ON external_code_mapping(bot_username)
            """)
        logger.info("[CodeMapper] external_code_mapping 表已就绪")

    # ── 批量写入（核心 RU 优化）───────────────────────────────

    async def set_mapping_batch(
        self,
        mappings: list[tuple[str, str, str]],
    ) -> int:
        """批量写入多条外部码 → 系统码映射（多行 UPSERT，减少事务开销）。

        Args:
            mappings: [(external_code, system_code, bot_username), ...]

        Returns:
            成功写入的映射数
        """
        if not mappings:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        n = len(mappings)

        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 批量 UPSERT 映射表
                    await conn.executemany(
                        """UPSERT INTO external_code_mapping
                           (external_code, system_code, bot_username, created_at, updated_at)
                           VALUES ($1, $2, $3, $4, $4)""",
                        [(ec, sc, bu, now) for ec, sc, bu in mappings],
                    )
                    # 批量 INSERT code_bot_mapping（幂等写入）
                    await conn.executemany(
                        """INSERT INTO code_bot_mapping (code, bot_username, created_at)
                           VALUES ($1, $2, $3) ON CONFLICT (code) DO NOTHING""",
                        [(ec, bu, now) for ec, _, bu in mappings],
                    )

            logger.info(f"[CodeMapper] 批量写入完成: {n} 条映射")
        except Exception as e:
            logger.error(f"[CodeMapper] 批量写入事务失败: {e}")
            return 0

        return n

    async def _upsert_mapping_in_txn(
        self,
        conn: asyncpg.Connection,
        external_code: str,
        system_code: str,
        bot_username: str,
        now: str,
    ):
        """在已开启的事务中执行单条映射写入（精简版：仅 UPSERT 映射表 + code_bot_mapping）。"""

        # 1) UPSERT 映射表
        await conn.execute("""
            UPSERT INTO external_code_mapping
                (external_code, system_code, bot_username, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $4)
        """, external_code, system_code, bot_username, now)

        # 2) code_bot_mapping 去重写入
        await conn.execute("""
            INSERT INTO code_bot_mapping (code, bot_username, created_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (code) DO NOTHING
        """, external_code, bot_username, now)

    # ── 单条写入（兼容旧接口，内部复用批量逻辑）────────────────

    async def set_mapping(
        self,
        external_code: str,
        system_code: str,
        bot_username: str = "",
    ) -> bool:
        """单条写入外部码 → 系统码映射。"""
        now = datetime.now(timezone.utc).isoformat()
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await self._upsert_mapping_in_txn(
                        conn, external_code, system_code, bot_username, now
                    )

            logger.info(
                f"[CodeMapper] 映射已写入: {external_code} → {system_code}"
                f" (@{bot_username})"
            )
            return True

        except Exception as e:
            logger.error(f"[CodeMapper] 写入映射失败 (external={external_code}): {e}")
            return False

    # ── 读取接口（无变更）───────────────────────────────────────

    async def get_system_code(self, external_code: str) -> Optional[str]:
        """查询外部码对应的系统码。"""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT system_code FROM external_code_mapping WHERE external_code = $1",
                    external_code,
                )
                if row:
                    return row["system_code"]
        except Exception as e:
            logger.debug(f"[CodeMapper] 查询映射失败 (external={external_code}): {e}")
        return None

    async def has_mapping(self, external_code: str) -> bool:
        """检查外部码是否已有映射。"""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT 1 FROM external_code_mapping WHERE external_code = $1",
                    external_code,
                )
                return row is not None
        except Exception:
            return False