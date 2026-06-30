from typing import List, Optional

from loguru import logger
from pydantic import model_validator
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

    # ── 解析器配置 ──────────────────────────────────────
    RESOLVE_TIMEOUT_SECONDS: int = 30
    RESOLVE_BATCH_SIZE: int = 5
    RESOLVE_INTERVAL_SECONDS: int = 10
    RESOLVE_MAX_RETRIES: int = 3
    RESOLVE_DELAY_BETWEEN_CODES: float = 3.0

    # ── 守护模式配置 ────────────────────────────────────
    DAEMON_CRAWL_FIRST: bool = True
    DAEMON_CYCLE_INTERVAL: int = 60

    # ── 主系统 Bot 用户名 ───────────────────────────────
    UPLOAD_BOT_USERNAME: str = ""
    DECODER_BOT_USERNAME: str = ""
    SENDER_BOT_USERNAME: str = ""

    # ── 文件码前缀（与主系统一致） ───────────────────────
    FILE_CODE_PREFIX: str = "tgwenjian"

    # ── CockroachDB ─────────────────────────────────────
    COCKROACHDB_URL: str = ""

    # ── 本地存储 ────────────────────────────────────────
    SQLITE_DB_PATH: str = "codes.db"
    EXPORT_DIR: str = "exports"

    # ── 日志 ────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"

    # ── 管理员 Bot 配置 ──────────────────────────────────
    TELEGRAM_BOT_TOKEN: str = ""
    ADMIN_USER_IDS: List[int] = []
    ADMIN_BOT_ENABLED: bool = False

    # ── Cloudflare Worker API（覆盖规则存储）────────────
    CLOUDFLARE_API_URL: str = ""
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
        extra = "ignore"

    @model_validator(mode='after')
    def validate_required_fields(self):
        """验证必填字段,在启动时尽早发现问题。"""
        missing = []
        if not self.TELEGRAM_API_ID:
            missing.append('TELEGRAM_API_ID')
        if not self.TELEGRAM_API_HASH:
            missing.append('TELEGRAM_API_HASH')
        if missing:
            logger.warning(f"[Settings] 以下环境变量未配置: {', '.join(missing)}")
        return self


settings = Settings()