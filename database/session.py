import csv
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import List, Optional

from loguru import logger

from config import settings


class Storage:
    def __init__(self):
        self._local = threading.local()
        self._db_path = settings.SQLITE_DB_PATH

    @property
    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self._db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._init_db()
            self._migrate_db()
        return self._local.conn

    def _init_db(self):
        conn = self._conn
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS channels (
                channel_id INTEGER PRIMARY KEY,
                channel_username TEXT,
                title TEXT,
                channel_type TEXT DEFAULT 'channel',
                member_count INTEGER DEFAULT 0,
                discovered_at TEXT,
                last_crawled_at TEXT,
                crawl_count INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS file_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                bot_username TEXT NOT NULL,
                source_channel_id INTEGER,
                source_channel_title TEXT,
                source_message_id INTEGER,
                discovered_at TEXT,
                file_type TEXT,
                file_name TEXT,
                file_size INTEGER DEFAULT 0,
                is_verified INTEGER DEFAULT 0,
                is_exported INTEGER DEFAULT 0,
                is_resolved INTEGER DEFAULT 0,
                resolve_attempts INTEGER DEFAULT 0,
                resolve_error TEXT,
                storage_channel_id INTEGER DEFAULT 0,
                storage_msg_id INTEGER DEFAULT 0,
                storage_batch_msg_ids TEXT,
                UNIQUE(code)
            );

            CREATE TABLE IF NOT EXISTS crawl_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER,
                channel_title TEXT,
                started_at TEXT,
                completed_at TEXT,
                messages_scanned INTEGER DEFAULT 0,
                codes_found INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running'
            );

            CREATE TABLE IF NOT EXISTS resolve_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                bot_username TEXT,
                status TEXT DEFAULT 'pending',
                storage_msg_id INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TEXT,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS bot_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code_prefix TEXT NOT NULL,
                override_bot_username TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TEXT,
                updated_at TEXT,
                note TEXT,
                UNIQUE(code_prefix)
            );

            CREATE INDEX IF NOT EXISTS idx_file_codes_code ON file_codes(code);
            CREATE INDEX IF NOT EXISTS idx_file_codes_bot ON file_codes(bot_username);
            CREATE INDEX IF NOT EXISTS idx_file_codes_exported ON file_codes(is_exported);
            CREATE INDEX IF NOT EXISTS idx_file_codes_verified ON file_codes(is_verified);
            CREATE INDEX IF NOT EXISTS idx_file_codes_resolved ON file_codes(is_resolved);
            CREATE INDEX IF NOT EXISTS idx_resolve_log_status ON resolve_log(status);
            CREATE INDEX IF NOT EXISTS idx_bot_overrides_prefix ON bot_overrides(code_prefix);
        """)
        conn.commit()

    def _migrate_db(self):
        conn = self._conn
        existing_columns = [r[1] for r in conn.execute("PRAGMA table_info(file_codes)").fetchall()]
        new_columns = {
            "is_resolved": "ALTER TABLE file_codes ADD COLUMN is_resolved INTEGER DEFAULT 0",
            "resolve_attempts": "ALTER TABLE file_codes ADD COLUMN resolve_attempts INTEGER DEFAULT 0",
            "resolve_error": "ALTER TABLE file_codes ADD COLUMN resolve_error TEXT",
            "storage_channel_id": "ALTER TABLE file_codes ADD COLUMN storage_channel_id INTEGER DEFAULT 0",
            "storage_msg_id": "ALTER TABLE file_codes ADD COLUMN storage_msg_id INTEGER DEFAULT 0",
            "storage_batch_msg_ids": "ALTER TABLE file_codes ADD COLUMN storage_batch_msg_ids TEXT",
            "is_crdb_synced": "ALTER TABLE file_codes ADD COLUMN is_crdb_synced INTEGER DEFAULT 0",
        }
        for col, sql in new_columns.items():
            if col not in existing_columns:
                try:
                    conn.execute(sql)
                    logger.info(f"[Storage] 迁移: 添加列 {col}")
                except Exception as e:
                    logger.warning(f"[Storage] 迁移 {col} 失败: {e}")

        existing_resolve = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='resolve_log'"
        ).fetchone()
        if not existing_resolve:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS resolve_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    bot_username TEXT,
                    status TEXT DEFAULT 'pending',
                    storage_msg_id INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at TEXT,
                    completed_at TEXT
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_resolve_log_status ON resolve_log(status)"
            )

        existing_overrides = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bot_overrides'"
        ).fetchone()
        if not existing_overrides:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_overrides (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code_prefix TEXT NOT NULL,
                    override_bot_username TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT,
                    updated_at TEXT,
                    note TEXT,
                    UNIQUE(code_prefix)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_bot_overrides_prefix ON bot_overrides(code_prefix)"
            )
            logger.info("[Storage] 迁移: 创建 bot_overrides 表")
        conn.commit()

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    def save_channel(self, channel_id: int, username: str, title: str,
                     channel_type: str = "channel", member_count: int = 0) -> bool:
        conn = self._conn
        now = datetime.now(timezone.utc).isoformat()
        try:
            existing = conn.execute(
                "SELECT channel_id FROM channels WHERE channel_id = ?",
                (channel_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE channels SET title=?, member_count=?,
                       channel_type=? WHERE channel_id=?""",
                    (title, member_count, channel_type, channel_id)
                )
                return False
            else:
                conn.execute(
                    """INSERT INTO channels (channel_id, channel_username, title,
                       channel_type, member_count, discovered_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (channel_id, username, title, channel_type, member_count, now)
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"[Storage] 保存频道失败 {channel_id}: {e}")
            return False

    def update_channel_crawled(self, channel_id: int, scan_count: int, code_count: int):
        conn = self._conn
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute(
                """UPDATE channels SET last_crawled_at=?,
                   crawl_count=crawl_count+1 WHERE channel_id=?""",
                (now, channel_id)
            )
            conn.execute(
                """INSERT INTO crawl_log (channel_id, channel_title,
                   started_at, completed_at, messages_scanned, codes_found, status)
                   VALUES (?, (SELECT title FROM channels WHERE channel_id=?), ?, ?, ?, ?, 'completed')""",
                (channel_id, channel_id, now, now, scan_count, code_count)
            )
            conn.commit()
        except Exception as e:
            logger.error(f"[Storage] 更新频道爬取状态失败: {e}")

    def save_code(self, code: str, bot_username: str,
                  source_channel_id: int = None, source_channel_title: str = None,
                  source_message_id: int = None,
                  file_type: str = None, file_name: str = None,
                  file_size: int = 0) -> bool:
        conn = self._conn
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute(
                """INSERT OR IGNORE INTO file_codes
                   (code, bot_username, source_channel_id, source_channel_title,
                    source_message_id, discovered_at, file_type, file_name, file_size)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (code, bot_username, source_channel_id, source_channel_title,
                 source_message_id, now, file_type, file_name, file_size)
            )
            conn.commit()
            return conn.total_changes > 0
        except Exception as e:
            logger.error(f"[Storage] 保存文件码失败 {code}: {e}")
            return False

    def code_exists(self, code: str) -> bool:
        conn = self._conn
        row = conn.execute(
            "SELECT 1 FROM file_codes WHERE code = ?", (code,)
        ).fetchone()
        return row is not None

    def get_code_by_code(self, code: str) -> Optional[dict]:
        conn = self._conn
        row = conn.execute(
            "SELECT * FROM file_codes WHERE code = ?", (code,)
        ).fetchone()
        return dict(row) if row else None

    def get_all_codes(self, limit: int = None, offset: int = 0,
                      verified_only: bool = False) -> List[dict]:
        conn = self._conn
        query = "SELECT * FROM file_codes"
        params = []
        conditions = []
        if verified_only:
            conditions.append("is_verified = 1")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY discovered_at DESC"
        if limit:
            query += f" LIMIT {int(limit)} OFFSET {int(offset)}"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_unresolved_codes(self, limit: int = 10, max_attempts: int = 3) -> List[dict]:
        conn = self._conn
        rows = conn.execute(
            """SELECT * FROM file_codes
               WHERE is_resolved = 0 AND resolve_attempts < ?
               ORDER BY resolve_attempts ASC, discovered_at ASC
               LIMIT ?""",
            (max_attempts, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_unresolved_count(self) -> int:
        conn = self._conn
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM file_codes WHERE is_resolved = 0"
        ).fetchone()
        return row["cnt"] if row else 0

    def mark_resolved(self, code_id: int, storage_channel_id: int = 0,
                      storage_msg_id: int = 0, storage_batch_ids: str = ""):
        conn = self._conn
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """UPDATE file_codes SET is_resolved=1, is_verified=1,
               storage_channel_id=?, storage_msg_id=?, storage_batch_msg_ids=?,
               resolve_error=NULL
               WHERE id=?""",
            (storage_channel_id, storage_msg_id, storage_batch_ids, code_id)
        )
        conn.execute(
            """INSERT INTO resolve_log (code, status,
               storage_msg_id, completed_at, started_at)
               VALUES ((SELECT code FROM file_codes WHERE id=?), 'done', ?, ?, ?)""",
            (code_id, storage_msg_id, now, now)
        )
        conn.commit()

    def mark_resolve_failed(self, code_id: int, error: str):
        conn = self._conn
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """UPDATE file_codes SET
               resolve_attempts = resolve_attempts + 1,
               resolve_error = ?
               WHERE id=?""",
            (error, code_id)
        )
        conn.execute(
            """INSERT INTO resolve_log (code, status, error_message, completed_at, started_at)
               VALUES ((SELECT code FROM file_codes WHERE id=?), 'failed', ?, ?, ?)""",
            (code_id, error, now, now)
        )
        conn.commit()

    def get_uneported_codes(self, limit: int = 1000) -> List[dict]:
        conn = self._conn
        rows = conn.execute(
            "SELECT * FROM file_codes WHERE is_exported = 0 ORDER BY discovered_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_exported(self, code_ids: List[int]):
        conn = self._conn
        if not code_ids:
            return
        placeholders = ",".join("?" * len(code_ids))
        conn.execute(
            f"UPDATE file_codes SET is_exported = 1 WHERE id IN ({placeholders})",
            code_ids
        )
        conn.commit()

    def get_resolved_unsynced(self, limit: int = 200) -> List[dict]:
        conn = self._conn
        rows = conn.execute(
            """SELECT * FROM file_codes
               WHERE is_resolved = 1 AND (is_crdb_synced IS NULL OR is_crdb_synced = 0)
               ORDER BY discovered_at ASC
               LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_crdb_synced(self, code_ids: List[int]):
        conn = self._conn
        if not code_ids:
            return
        placeholders = ",".join("?" * len(code_ids))
        conn.execute(
            f"UPDATE file_codes SET is_crdb_synced = 1 WHERE id IN ({placeholders})",
            code_ids
        )
        conn.commit()

    def get_all_channels(self, active_only: bool = True) -> List[dict]:
        conn = self._conn
        query = "SELECT * FROM channels"
        params = []
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY member_count DESC"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_channel_ids(self) -> List[int]:
        conn = self._conn
        rows = conn.execute("SELECT channel_id FROM channels WHERE is_active = 1").fetchall()
        return [r["channel_id"] for r in rows]

    def get_channel_crawl_count(self, channel_id: int) -> int:
        conn = self._conn
        row = conn.execute(
            "SELECT crawl_count FROM channels WHERE channel_id = ?", (channel_id,)
        ).fetchone()
        return row["crawl_count"] if row else 0

    def get_channel_count(self) -> int:
        conn = self._conn
        row = conn.execute("SELECT COUNT(*) as cnt FROM channels WHERE is_active = 1").fetchone()
        return row["cnt"] if row else 0

    def get_code_count(self) -> int:
        conn = self._conn
        row = conn.execute("SELECT COUNT(*) as cnt FROM file_codes").fetchone()
        return row["cnt"] if row else 0

    def get_code_count_by_bot(self) -> List[dict]:
        conn = self._conn
        rows = conn.execute(
            "SELECT bot_username, COUNT(*) as cnt FROM file_codes GROUP BY bot_username ORDER BY cnt DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_crawl_stats(self) -> dict:
        conn = self._conn
        channels = conn.execute("SELECT COUNT(*) as cnt FROM channels").fetchone()["cnt"]
        active_channels = conn.execute(
            "SELECT COUNT(*) as cnt FROM channels WHERE is_active = 1"
        ).fetchone()["cnt"]
        codes = conn.execute("SELECT COUNT(*) as cnt FROM file_codes").fetchone()["cnt"]
        resolved = conn.execute(
            "SELECT COUNT(*) as cnt FROM file_codes WHERE is_resolved = 1"
        ).fetchone()["cnt"]
        unresolved = conn.execute(
            "SELECT COUNT(*) as cnt FROM file_codes WHERE is_resolved = 0"
        ).fetchone()["cnt"]
        exported = conn.execute(
            "SELECT COUNT(*) as cnt FROM file_codes WHERE is_exported = 1"
        ).fetchone()["cnt"]
        last_crawl = conn.execute(
            "SELECT MAX(completed_at) as last FROM crawl_log"
        ).fetchone()["last"]
        return {
            "channels": channels,
            "active_channels": active_channels,
            "codes": codes,
            "resolved": resolved,
            "unresolved": unresolved,
            "exported": exported,
            "last_crawl": last_crawl,
        }

    def get_resolve_stats(self) -> dict:
        conn = self._conn
        total = conn.execute("SELECT COUNT(*) as cnt FROM resolve_log").fetchone()["cnt"]
        done = conn.execute(
            "SELECT COUNT(*) as cnt FROM resolve_log WHERE status='done'"
        ).fetchone()["cnt"]
        failed = conn.execute(
            "SELECT COUNT(*) as cnt FROM resolve_log WHERE status='failed'"
        ).fetchone()["cnt"]
        return {"total": total, "done": done, "failed": failed}

    def export_to_json(self, filepath: str = None, resolved_only: bool = True) -> str:
        if resolved_only:
            codes = self._get_resolved_and_uneported(limit=10000)
        else:
            codes = self.get_uneported_codes(limit=10000)
        if not filepath:
            os.makedirs(settings.EXPORT_DIR, exist_ok=True)
            filename = f"codes_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = os.path.join(settings.EXPORT_DIR, filename)
        data = []
        code_ids = []
        for c in codes:
            data.append(c)
            code_ids.append(c["id"])
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        if code_ids:
            self.mark_exported(code_ids)
        logger.info(f"[Storage] 已导出 {len(data)} 个文件码到 {filepath}")
        return filepath

    def export_to_csv(self, filepath: str = None, resolved_only: bool = True) -> str:
        if resolved_only:
            codes = self._get_resolved_and_uneported(limit=10000)
        else:
            codes = self.get_uneported_codes(limit=10000)
        if not filepath:
            os.makedirs(settings.EXPORT_DIR, exist_ok=True)
            filename = f"codes_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(settings.EXPORT_DIR, filename)
        code_ids = []
        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "code", "bot_username", "source_channel_title",
                "discovered_at", "file_type", "file_name", "file_size",
                "is_resolved", "storage_channel_id", "storage_msg_id",
            ])
            for c in codes:
                writer.writerow([
                    c["code"], c["bot_username"], c["source_channel_title"],
                    c["discovered_at"], c["file_type"], c["file_name"], c["file_size"],
                    c.get("is_resolved", 0), c.get("storage_channel_id", 0),
                    c.get("storage_msg_id", 0),
                ])
                code_ids.append(c["id"])
        if code_ids:
            self.mark_exported(code_ids)
        logger.info(f"[Storage] 已导出 {len(codes)} 个文件码到 {filepath}")
        return filepath

    def _get_resolved_and_uneported(self, limit: int = 10000) -> List[dict]:
        conn = self._conn
        rows = conn.execute(
            """SELECT * FROM file_codes
               WHERE is_exported = 0 AND is_resolved = 1
               ORDER BY discovered_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def import_from_json(self, filepath: str) -> int:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        count = 0
        for item in data:
            ok = self.save_code(
                code=item["code"],
                bot_username=item.get("bot_username", ""),
                source_channel_id=item.get("source_channel_id"),
                source_channel_title=item.get("source_channel_title"),
                source_message_id=item.get("source_message_id"),
                file_type=item.get("file_type"),
                file_name=item.get("file_name"),
                file_size=item.get("file_size", 0),
            )
            if ok:
                count += 1
        logger.info(f"[Storage] 从 {filepath} 导入 {count} 个文件码")
        return count

    def clear_codes(self):
        conn = self._conn
        conn.execute("DELETE FROM file_codes")
        conn.execute("DELETE FROM resolve_log")
        conn.commit()
        logger.info("[Storage] 已清空所有文件码和解析记录")

    # ─── Bot 覆盖规则管理 ─────────────────────────────────

    def add_bot_override(self, code_prefix: str, override_bot_username: str,
                         note: str = "") -> bool:
        """添加 Bot 覆盖规则：以 code_prefix 开头的文件码使用 override_bot_username 解析"""
        conn = self._conn
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute(
                """INSERT INTO bot_overrides (code_prefix, override_bot_username, is_active, created_at, note)
                   VALUES (?, ?, 1, ?, ?)
                   ON CONFLICT(code_prefix) DO UPDATE SET
                   override_bot_username=excluded.override_bot_username,
                   is_active=1,
                   updated_at=excluded.created_at,
                   note=excluded.note""",
                (code_prefix, override_bot_username, now, note)
            )
            conn.commit()
            return conn.total_changes > 0
        except Exception as e:
            logger.error(f"[Storage] 添加 Bot 覆盖失败 {code_prefix}: {e}")
            return False

    def remove_bot_override(self, code_prefix: str) -> bool:
        """删除 Bot 覆盖规则"""
        conn = self._conn
        try:
            conn.execute("DELETE FROM bot_overrides WHERE code_prefix = ?", (code_prefix,))
            conn.commit()
            return conn.total_changes > 0
        except Exception as e:
            logger.error(f"[Storage] 删除 Bot 覆盖失败 {code_prefix}: {e}")
            return False

    def toggle_bot_override(self, code_prefix: str) -> bool:
        """切换 Bot 覆盖规则的启用/禁用状态"""
        conn = self._conn
        try:
            conn.execute(
                """UPDATE bot_overrides SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END,
                   updated_at = ?
                   WHERE code_prefix = ?""",
                (datetime.now(timezone.utc).isoformat(), code_prefix)
            )
            conn.commit()
            return conn.total_changes > 0
        except Exception as e:
            logger.error(f"[Storage] 切换 Bot 覆盖状态失败 {code_prefix}: {e}")
            return False

    def list_bot_overrides(self) -> list:
        """列出所有 Bot 覆盖规则"""
        conn = self._conn
        rows = conn.execute(
            "SELECT * FROM bot_overrides ORDER BY is_active DESC, created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_bot_override(self, code: str) -> dict | None:
        """检查文件码是否匹配覆盖规则，返回匹配的覆盖规则（按前缀长度降序优先匹配最长前缀）"""
        conn = self._conn
        rows = conn.execute(
            """SELECT * FROM bot_overrides
               WHERE is_active = 1 AND ? LIKE (code_prefix || '%')
               ORDER BY LENGTH(code_prefix) DESC
               LIMIT 1""",
            (code,)
        ).fetchall()
        if rows:
            return dict(rows[0])
        return None


def get_db() -> Storage:
    """获取数据库实例（工厂函数，匹配主项目风格）。"""
    return Storage()