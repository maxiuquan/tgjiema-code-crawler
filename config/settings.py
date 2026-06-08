from typing import List, Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Telegram API 凭证 ──────────────────────────────
    TELEGRAM_API_ID: int = 0
    TELEGRAM_API_HASH: str = ""
    TELEGRAM_PHONE: str = ""

    # ── 爬取配置 ────────────────────────────────────────
    CRAWL_INTERVAL_MINUTES: int = 30
    CRAWL_MESSAGE_LIMIT_PER_CHANNEL: int = 200
    MAX_CONCURRENT_CRAWLS: int = 3

    SEARCH_KEYWORDS: str = ""
    SEARCH_LIMIT: int = 50

    # ── 存储频道 ────────────────────────────────────────
    STORAGE_CHANNEL_ID: int = 0

    # ── 解析器配置 ──────────────────────────────────────
    RESOLVE_TIMEOUT_SECONDS: int = 30
    RESOLVE_BATCH_SIZE: int = 5
    RESOLVE_INTERVAL_SECONDS: int = 10
    RESOLVE_MAX_RETRIES: int = 3
    RESOLVE_DELAY_BETWEEN_CODES: float = 3.0

    # ── 守护模式配置 ────────────────────────────────────
    DAEMON_CRAWL_FIRST: bool = True
    DAEMON_CYCLE_INTERVAL: int = 60

    # ── CockroachDB ─────────────────────────────────────
    COCKROACHDB_URL: str = ""

    # ── 本地存储 ────────────────────────────────────────
    SQLITE_DB_PATH: str = "codes.db"
    EXPORT_DIR: str = "exports"

    # ── 日志 ────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"

    # ── 管理员 Bot 配置 ──────────────────────────────────
    # 独立 Bot 的 Token（从 @BotFather 获取）
    TELEGRAM_BOT_TOKEN: str = ""
    # 允许使用管理命令的用户 ID 列表（Telegram 数字 ID）
    ADMIN_USER_IDS: List[int] = []
    # 是否启用管理员 Bot 监听模式
    ADMIN_BOT_ENABLED: bool = False

    # ── Cloudflare Worker API（覆盖规则存储）────────────
    # Worker 部署后的 URL，如 https://bot-override-api.your-name.workers.dev
    CLOUDFLARE_API_URL: str = ""
    # Bearer Token，与 Worker 中的 AUTH_TOKEN secret 一致
    CLOUDFLARE_AUTH_TOKEN: str = ""

    # ── 目标文件扩展名 ──────────────────────────────────
    TARGET_FILE_EXTENSIONS: List[str] = [
        "zip", "rar", "7z", "tar", "gz",
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
        "jpg", "jpeg", "png", "gif", "mp4", "mkv", "avi",
        "mp3", "flac", "wav", "apk", "iso", "exe",
    ]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()