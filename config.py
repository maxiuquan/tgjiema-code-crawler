import os
from typing import List, Optional


def _load_dotenv(env_path: str | None = None):
    if env_path is None:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and not os.environ.get(key):
                os.environ[key] = value


_load_dotenv()


class Settings:
    TELEGRAM_API_ID: int = 0
    TELEGRAM_API_HASH: str = ""
    TELEGRAM_PHONE: str = ""

    CRAWL_INTERVAL_MINUTES: int = 30
    CRAWL_MESSAGE_LIMIT_PER_CHANNEL: int = 200

    COCKROACHDB_URL: str = ""

    SQLITE_DB_PATH: str = "codes.db"
    EXPORT_DIR: str = "exports"

    LOG_LEVEL: str = "INFO"

    TARGET_FILE_EXTENSIONS: List[str] = [
        "zip", "rar", "7z", "tar", "gz",
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
        "jpg", "jpeg", "png", "gif", "mp4", "mkv", "avi",
        "mp3", "flac", "wav", "apk", "iso", "exe",
    ]

    MAX_CONCURRENT_CRAWLS: int = 3

    STORAGE_CHANNEL_ID: int = 0

    RESOLVE_TIMEOUT_SECONDS: int = 30
    RESOLVE_BATCH_SIZE: int = 5
    RESOLVE_INTERVAL_SECONDS: int = 10
    RESOLVE_MAX_RETRIES: int = 3
    RESOLVE_DELAY_BETWEEN_CODES: float = 3.0

    DAEMON_CRAWL_FIRST: bool = True
    DAEMON_CYCLE_INTERVAL: int = 60

    @classmethod
    def load(cls):
        obj = cls()
        obj.TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
        obj.TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
        obj.TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE", "")
        obj.CRAWL_INTERVAL_MINUTES = int(os.getenv("CRAWL_INTERVAL_MINUTES", "30"))
        obj.CRAWL_MESSAGE_LIMIT_PER_CHANNEL = int(os.getenv("CRAWL_MESSAGE_LIMIT_PER_CHANNEL", "200"))
        obj.COCKROACHDB_URL = os.getenv("COCKROACHDB_URL", "")
        obj.SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "codes.db")
        obj.EXPORT_DIR = os.getenv("EXPORT_DIR", "exports")
        obj.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        obj.STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", "0"))
        obj.RESOLVE_TIMEOUT_SECONDS = int(os.getenv("RESOLVE_TIMEOUT_SECONDS", "30"))
        obj.RESOLVE_BATCH_SIZE = int(os.getenv("RESOLVE_BATCH_SIZE", "5"))
        obj.RESOLVE_INTERVAL_SECONDS = int(os.getenv("RESOLVE_INTERVAL_SECONDS", "10"))
        obj.RESOLVE_MAX_RETRIES = int(os.getenv("RESOLVE_MAX_RETRIES", "3"))
        obj.RESOLVE_DELAY_BETWEEN_CODES = float(os.getenv("RESOLVE_DELAY_BETWEEN_CODES", "3.0"))
        obj.DAEMON_CRAWL_FIRST = os.getenv("DAEMON_CRAWL_FIRST", "true").lower() == "true"
        obj.DAEMON_CYCLE_INTERVAL = int(os.getenv("DAEMON_CYCLE_INTERVAL", "60"))
        return obj


settings = Settings.load()