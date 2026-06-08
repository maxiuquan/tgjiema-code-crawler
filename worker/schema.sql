-- Cloudflare D1 数据库初始化 schema
-- 部署命令: wrangler d1 execute bot-overrides-db --file=schema.sql

CREATE TABLE IF NOT EXISTS bot_overrides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code_prefix TEXT NOT NULL UNIQUE,
    override_bot_username TEXT NOT NULL,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT,
    note TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_bot_overrides_prefix ON bot_overrides(code_prefix);
CREATE INDEX IF NOT EXISTS idx_bot_overrides_active ON bot_overrides(is_active);