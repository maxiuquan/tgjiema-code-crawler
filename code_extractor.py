import re
from typing import List, Tuple

_BOT_PATTERN = re.compile(r"^[a-zA-Z0-9_]+bot", re.IGNORECASE)

_BOT_PATTERN_IN_MESSAGE = re.compile(r"([a-zA-Z0-9_]+bot)", re.IGNORECASE)

_CODE_WITH_BOT = re.compile(
    r"(?:(?P<bot>[a-zA-Z0-9_]+bot)[:\s]+(?P<code>\S+))"
    r"|(?:(?P<code2>\S+)[:\s]+(?P<bot2>[a-zA-Z0-9_]+bot))",
    re.IGNORECASE,
)

_CODE_ONLY_BOT_INLINE = re.compile(
    r"(?:^|\s)([a-zA-Z0-9_]+bot:[a-zA-Z0-9_]+)(?:\s|$)",
)

_FILE_EXTENSION_PATTERN = re.compile(
    r"\.(zip|rar|7z|tar|gz|pdf|doc|docx|xls|xlsx|ppt|pptx|"
    r"jpg|jpeg|png|gif|mp4|mkv|avi|mp3|flac|wav|apk|iso|exe)(?:\s|$)",
    re.IGNORECASE,
)


def extract_bot_username(text: str) -> str:
    match = _BOT_PATTERN.match(text)
    if match:
        return match.group(0)
    return ""


def is_external_code(text: str) -> bool:
    return bool(_BOT_PATTERN.match(text))


def extract_codes_from_message(text: str) -> List[Tuple[str, str, int]]:
    codes = []
    seen = set()

    for match in _CODE_WITH_BOT.finditer(text):
        bot = match.group("bot") or match.group("bot2")
        code = match.group("code") or match.group("code2")
        if bot and code:
            normalized = re.sub(r'[^\w:]', '', f"{bot}:{code}")
            if normalized not in seen:
                seen.add(normalized)
                codes.append((normalized, bot, 100))
            continue

    for match in _CODE_ONLY_BOT_INLINE.finditer(text):
        raw = match.group(1)
        bot = extract_bot_username(raw)
        if bot:
            normalized = raw.strip()
            if normalized not in seen:
                seen.add(normalized)
                codes.append((normalized, bot, 90))

    tokens = re.split(r'[\s,;:]+', text)
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        bot = extract_bot_username(token)
        if bot and token not in seen:
            seen.add(token)
            codes.append((token, bot, 80))

    return codes


def extract_codes_from_caption(text: str) -> List[Tuple[str, str, int]]:
    codes = []
    seen = set()

    for match in _CODE_WITH_BOT.finditer(text):
        bot = match.group("bot") or match.group("bot2")
        code = match.group("code") or match.group("code2")
        if bot and code:
            normalized = re.sub(r'[^\w:]', '', f"{bot}:{code}")
            if normalized not in seen:
                seen.add(normalized)
                codes.append((normalized, bot, 100))

    return codes


def extract_code(text: str) -> str:
    codes = extract_codes_from_message(text)
    return codes[0][0] if codes else ""


def has_file_extension(text: str) -> bool:
    return bool(_FILE_EXTENSION_PATTERN.search(text))


def extract_all_codes(text: str) -> List[str]:
    return [c[0] for c in extract_codes_from_message(text)]


def normalize_code(code: str) -> str:
    code = code.strip().lower()
    code = re.sub(r'^@', '', code)
    return code