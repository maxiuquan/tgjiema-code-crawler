import re
from typing import List, Tuple

_BOT_PATTERN = re.compile(r"^[a-zA-Z0-9_]+bot", re.IGNORECASE)

_BOT_PATTERN_IN_MESSAGE = re.compile(r"([a-zA-Z0-9_]+bot)", re.IGNORECASE)

_CODE_WITH_BOT = re.compile(
    r"(?:(?P<bot>[a-zA-Z0-9_]+bot)[:_](?P<code>[a-zA-Z0-9_\-+/=]{4,}))"
    r"|(?:(?P<code2>[a-zA-Z0-9_\-+/=]{4,})[:_](?P<bot2>[a-zA-Z0-9_]+bot))",
    re.IGNORECASE,
)

_CODE_WITH_BOT_SPACED = re.compile(
    r"(?:^|\s)(?P<bot>[a-zA-Z0-9_]+bot)\s+(?P<code>[a-zA-Z0-9_\-+/=]{8,})(?:\s|$)",
    re.IGNORECASE,
)

_CODE_BOT_ATTACHED = re.compile(
    r"(?:^|\s)(?P<bot>[a-zA-Z0-9]+bot)(?P<code>[a-zA-Z0-9][a-zA-Z0-9_\-+/=]{7,})(?:\s|$)",
    re.IGNORECASE,
)

_CODE_ONLY_BOT_INLINE = re.compile(
    r"(?:^|\s)([a-zA-Z0-9_]+bot)[:_]([a-zA-Z0-9_\-+/=]{4,})(?:\s|$)",
    re.IGNORECASE,
)

_FILE_EXTENSION_PATTERN = re.compile(
    r"\.(zip|rar|7z|tar|gz|pdf|doc|docx|xls|xlsx|ppt|pptx|"
    r"jpg|jpeg|png|gif|mp4|mkv|avi|mp3|flac|wav|apk|iso|exe)(?:\s|$)",
    re.IGNORECASE,
)

_NON_BOT_WORDS = {
    "robot", "chatbot", "adbot", "spambot", "bot", "webbot",
    "knowbot", "microbot", "nanobot", "tbot", "autobot",
    "megabot", "minibot", "superbot", "ultrabot", "hyperbot",
    "cryptobot_excluded", "infobot", "databot", "netbot",
    "searchbot", "crawlerbot", "parserbot", "scanbot",
    "proxybot", "apitbot", "testbot", "debugbot",
}


def _is_valid_bot_name(bot_name: str) -> bool:
    if len(bot_name) <= 3:
        return False
    if len(bot_name) > 25:
        return False
    if bot_name.lower() in _NON_BOT_WORDS:
        return False
    if not _BOT_PATTERN.match(bot_name):
        return False
    return True


def _is_valid_code_part(code: str) -> bool:
    if len(code) < 4:
        return False
    if not re.match(r'^[a-zA-Z0-9_\-+/=]+$', code):
        return False
    alpha_count = sum(1 for c in code if c.isalpha())
    if alpha_count == 0:
        return False
    return True


def extract_bot_username(text: str) -> str:
    match = _BOT_PATTERN.match(text)
    if match:
        return match.group(0)
    return ""


def is_external_code(text: str) -> bool:
    return bool(_BOT_PATTERN.match(text))


def extract_codes_from_message(text: str) -> List[Tuple[str, str, int]]:
    """从消息文本中提取所有文件码。

    支持多种格式:
      - BotName_bot:code1234
      - BotName_bot code1234 (空格分隔)
      - BotName_botcode1234 (紧凑粘连)
      - code1234:BotName_bot (反序)
      - 同一消息中多个码（空格、换行、中文分隔均可）
    """
    codes = []
    seen = set()
    structured_code_parts: set = set()

    # ─── 将全文 + 逐行分别提取，确保换行分隔的多码不遗漏 ───
    text_variants = [text]
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if len(lines) > 1:
        text_variants.extend(lines)

    for variant in text_variants:
        _extract_from_single_text(variant, codes, seen, structured_code_parts)

    # ─── 按置信度降序排列 ───
    codes.sort(key=lambda x: x[2], reverse=True)
    return codes


def _extract_from_single_text(
    text: str,
    codes: List[Tuple[str, str, int]],
    seen: set,
    structured_code_parts: set,
):
    """从单个文本片段中提取文件码（内部辅助函数）。"""

    for match in _CODE_WITH_BOT.finditer(text):
        bot = match.group("bot") or match.group("bot2")
        code = match.group("code") or match.group("code2")
        if bot and code and _is_valid_bot_name(bot) and _is_valid_code_part(code):
            normalized = f"{bot}:{code}"
            if normalized not in seen:
                seen.add(normalized)
                codes.append((normalized, bot, 100))
                structured_code_parts.add(code)
            continue

    for match in _CODE_ONLY_BOT_INLINE.finditer(text):
        bot = match.group(1)
        code = match.group(2)
        if _is_valid_bot_name(bot) and _is_valid_code_part(code):
            normalized = f"{bot}:{code}"
            if normalized not in seen:
                seen.add(normalized)
                codes.append((normalized, bot, 90))
                structured_code_parts.add(code)

    for match in _CODE_WITH_BOT_SPACED.finditer(text):
        bot = match.group("bot")
        code = match.group("code")
        if _is_valid_bot_name(bot) and _is_valid_code_part(code):
            normalized = f"{bot}:{code}"
            if normalized not in seen:
                seen.add(normalized)
                codes.append((normalized, bot, 80))
                structured_code_parts.add(code)

    for match in _CODE_BOT_ATTACHED.finditer(text):
        bot = match.group("bot")
        code = match.group("code")
        if _is_valid_bot_name(bot) and _is_valid_code_part(code):
            normalized = f"{bot}:{code}"
            if normalized not in seen:
                seen.add(normalized)
                codes.append((normalized, bot, 70))
                structured_code_parts.add(code)

    # ─── 空格/逗号/分号分隔的 token 级扫描 ───
    seen_bots = set()
    tokens = re.split(r'[\s,;]+', text)
    for i, token in enumerate(tokens):
        token = token.strip()
        if not token or not _BOT_PATTERN.match(token):
            continue
        if not _is_valid_bot_name(token):
            continue
        if token in seen_bots:
            continue
        seen_bots.add(token)

        for j in range(i + 1, min(i + 4, len(tokens))):
            next_token = tokens[j].strip()
            if not next_token:
                continue
            if _is_valid_code_part(next_token) and len(next_token) >= 6:
                normalized = f"{token}:{next_token}"
                if normalized not in seen:
                    seen.add(normalized)
                    codes.append((normalized, token, 75))
                break

    # ─── 孤儿 token 关联已知 bot ───
    all_bot_mentions: List[str] = []
    for token in re.split(r'[\s,;:_]+', text):
        token = token.strip().lstrip('@')
        if _BOT_PATTERN.match(token) and _is_valid_bot_name(token) and token not in all_bot_mentions:
            all_bot_mentions.append(token)

    if all_bot_mentions:
        orphan_tokens: List[str] = []
        for token in re.split(r'[\s,;:_]+', text):
            token = token.strip()
            if not token or len(token) < 6:
                continue
            if token in structured_code_parts:
                continue
            if _BOT_PATTERN.match(token):
                continue
            if _is_valid_code_part(token) and token not in orphan_tokens:
                orphan_tokens.append(token)

        for orphan in orphan_tokens:
            for bot in all_bot_mentions:
                normalized = f"{bot}:{orphan}"
                if normalized not in seen:
                    seen.add(normalized)
                    codes.append((normalized, bot, 60))


def extract_codes_from_caption(text: str) -> List[Tuple[str, str, int]]:
    codes = []
    seen = set()

    for match in _CODE_WITH_BOT.finditer(text):
        bot = match.group("bot") or match.group("bot2")
        code = match.group("code") or match.group("code2")
        if bot and code and _is_valid_bot_name(bot) and _is_valid_code_part(code):
            normalized = f"{bot}:{code}"
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