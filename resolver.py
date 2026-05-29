import asyncio
import json
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import asyncpg
from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError, UsernameNotOccupiedError, ChatWriteForbiddenError,
)
from telethon.tl.types import (
    Message, MessageMediaDocument, MessageMediaPhoto,
    MessageMediaWebPage,
)
from telethon.utils import pack_bot_file_id

from code_extractor import extract_bot_username
from config import settings
from storage import Storage

_SETTLE_WAIT = 5
_INITIAL_SETTLE_WAIT = 10
_MEDIA_GROUP_FLUSH_WAIT = 3


class CodeResolver:
    def __init__(self, client: TelegramClient, storage: Storage):
        self.client = client
        self.storage = storage
        self._running = False
        self._db_pool: Optional[asyncpg.Pool] = None
        self._handler_registered = False
        self._bot_exchange: dict[str, dict] = {}
        self._media_buffers: dict[str, dict] = {}
        self._cache_locks: dict[str, asyncio.Lock] = {}

    @property
    def _event_loop(self):
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    # ─── 存储频道 ────────────────────────────────────────

    async def ensure_storage_channel(self) -> int:
        cid = settings.STORAGE_CHANNEL_ID
        if not cid:
            logger.error("[Resolver] STORAGE_CHANNEL_ID 未配置，无法存储解析后的文件")
            return 0
        try:
            entity = await self.client.get_entity(cid)
            me = await self.client.get_me()
            try:
                permissions = await self.client.get_permissions(entity, me)
                if not permissions.is_sender:
                    logger.warning(f"[Resolver] 当前账号在存储频道 {cid} 中可能无发送权限")
            except Exception:
                pass
            logger.info(f"[Resolver] 存储频道验证通过: {cid}")
            return cid
        except Exception as e:
            logger.error(f"[Resolver] 无法访问存储频道 {cid}: {e}")
            return 0

    # ─── CockroachDB ─────────────────────────────────────

    async def _init_db_pool(self) -> bool:
        if self._db_pool:
            return True
        if not settings.COCKROACHDB_URL:
            logger.warning("[Resolver] COCKROACHDB_URL 未配置，解析后的文件不会写入主数据库")
            return False
        try:
            self._db_pool = await asyncpg.create_pool(
                settings.COCKROACHDB_URL,
                min_size=1,
                max_size=3,
                statement_cache_size=0,
            )
            logger.info("[Resolver] 已连接 CockroachDB")
            return True
        except Exception as e:
            logger.error(f"[Resolver] 连接 CockroachDB 失败: {e}")
            return False

    async def close(self):
        if self._db_pool:
            await self._db_pool.close()
            self._db_pool = None
        for bot in list(self._bot_exchange.keys()):
            self._cleanup_exchange(bot)

    # ─── 事件处理器 ──────────────────────────────────────

    def _register_handlers(self):
        if self._handler_registered:
            return
        self._handler_registered = True

        @self.client.on(events.NewMessage(incoming=True))
        async def on_new_message(event):
            now_ts = asyncio.get_event_loop().time()
            expired = [
                k for k, v in list(self._bot_exchange.items())
                if v.get("_expires", 0) < now_ts
            ]
            for k in expired:
                self._cleanup_exchange(k)

            sender = await event.get_sender()
            if not sender or not getattr(sender, "bot", False):
                return

            bot_username = (sender.username or "").lower()
            if not bot_username or bot_username not in self._bot_exchange:
                return

            exchange = self._bot_exchange[bot_username]
            exchange["_expires"] = now_ts + 120

            msg = event.message

            if msg.reply_markup and hasattr(msg.reply_markup, "rows") and msg.reply_markup.rows:
                exchange["_keyboard_msg"] = msg

            media_group_id = getattr(msg, "grouped_id", None)
            if media_group_id:
                gid_str = str(media_group_id)
                buf = self._media_buffers.get(gid_str)
                if buf:
                    buf["events"].append(event)
                    buf["_expires"] = now_ts + _MEDIA_GROUP_FLUSH_WAIT
                    return
                self._media_buffers[gid_str] = {
                    "events": [event],
                    "bot_username": bot_username,
                    "_expires": now_ts + _MEDIA_GROUP_FLUSH_WAIT,
                }
                self._create_flush_task(gid_str, bot_username)
                return

            if msg.media and not isinstance(msg.media, MessageMediaWebPage):
                exchange.setdefault("media_events", []).append(event)
                logger.info(
                    f"[Resolver] 捕获外部机器人文件 @{bot_username}: "
                    f"msg_id={msg.id}"
                )
                self._restart_settle(exchange, bot_username)
                return

            text = getattr(msg, "message", None) or ""
            if text:
                exchange.setdefault("text_responses", []).append(
                    {"msg_id": msg.id, "text": text}
                )
                logger.debug(
                    f"[Resolver] 外部机器人 @{bot_username} 文本响应: "
                    f"{text[:80]}{'...' if len(text) > 80 else ''}"
                )

        @self.client.on(events.MessageEdited(incoming=True))
        async def on_message_edited(event):
            sender = await event.get_sender()
            if not sender or not getattr(sender, "bot", False):
                return
            bot_username = (sender.username or "").lower()
            if not bot_username:
                return
            exchange = self._bot_exchange.get(bot_username)
            if not exchange:
                return
            if not (
                event.message.reply_markup
                and hasattr(event.message.reply_markup, "rows")
                and event.message.reply_markup.rows
            ):
                return
            exchange["_keyboard_msg"] = event.message
            exchange["_board_version"] = exchange.get("_board_version", 0) + 1

        logger.info("[Resolver] 事件处理器已注册")

    # ─── 媒体组缓冲 ──────────────────────────────────────

    def _create_flush_task(self, gid_str: str, bot_username: str):
        async def _flush():
            await asyncio.sleep(_MEDIA_GROUP_FLUSH_WAIT)
            now_ts = asyncio.get_event_loop().time()
            buf = self._media_buffers.get(gid_str)
            if not buf:
                return
            if buf["_expires"] > now_ts:
                await asyncio.sleep(0.5)
                buf = self._media_buffers.get(gid_str)
                if not buf:
                    return
            buf = self._media_buffers.pop(gid_str, None)
            if not buf:
                return

            exchange = self._bot_exchange.get(bot_username)
            if not exchange:
                return

            events_list = buf["events"]
            logger.info(
                f"[Resolver] 媒体组 {gid_str} 共 {len(events_list)} 条，已收集完成"
            )

            for ev in events_list:
                msg = ev.message
                if msg.media and not isinstance(msg.media, MessageMediaWebPage):
                    exchange.setdefault("media_events", []).append(ev)
            self._restart_settle(exchange, bot_username)

        asyncio.create_task(_flush())

    # ─── settle 机制 ─────────────────────────────────────

    def _restart_settle(self, exchange: dict, bot_username: str):
        exchange["_settle_version"] = exchange.get("_settle_version", 0) + 1
        old = exchange.get("_settle_task")
        if old and not old.done():
            old.cancel()
        exchange["_settle_task"] = asyncio.create_task(
            self._settle_loop(bot_username)
        )

    async def _settle_loop(self, bot_username: str):
        await asyncio.sleep(_SETTLE_WAIT)
        exchange = self._bot_exchange.get(bot_username)
        if not exchange:
            return

        exchange["_collection_done"] = True
        exchange["_collect_event"].set()
        logger.info(f"[Resolver] 外部机器人 @{bot_username} settle 完成，停止收集")

    # ─── 翻页检测和点击 ──────────────────────────────────

    @staticmethod
    def _extract_number(text: str) -> int | None:
        digits = "".join(ch for ch in text if ch.isdigit())
        if digits:
            return int(digits)
        return None

    def _detect_next_button(self, exchange: dict) -> tuple | None:
        _NEXT_KW = (
            "next", "下一页", "下一頁", "下一组",
            "→", "▶", "➡", ">>", "»",
        )
        keyboard_msg = exchange.get("_keyboard_msg")
        if not keyboard_msg or not keyboard_msg.reply_markup:
            return None
        rp = keyboard_msg.reply_markup
        if not hasattr(rp, "rows"):
            return None

        exchange.pop("_keyboard_msg", None)

        for row_idx, row in enumerate(rp.rows):
            for col_idx, btn in enumerate(row.buttons):
                btn_text = (getattr(btn, "text", None) or "").lower().strip()
                if any(kw in btn_text for kw in _NEXT_KW):
                    return (row_idx, col_idx)

        for row in rp.rows:
            btn_texts = [(getattr(b, "text", None) or "").strip() for b in row.buttons]
            numbers = []
            for col_idx, t in enumerate(btn_texts):
                n = self._extract_number(t)
                if n is not None:
                    numbers.append((col_idx, t, n))
            if len(numbers) >= 3:
                last = exchange.get("_last_page_num")
                if last is None:
                    target = 2 if 2 in [n for _, _, n in numbers] else numbers[1][2]
                else:
                    target = last + 1
                    all_nums = sorted([n for _, _, n in numbers])
                    if target > all_nums[-1]:
                        return None
                for col_idx, t, n in numbers:
                    if n == target:
                        exchange["_last_page_num"] = target
                        row_idx = rp.rows.index(row)
                        return (row_idx, col_idx)
                break

        return None

    async def _click_button(self, bot_username: str, row: int, col: int) -> bool:
        exchange = self._bot_exchange.get(bot_username)
        if not exchange:
            return False

        keyboard_msg = exchange.get("_keyboard_msg")
        if not keyboard_msg or not keyboard_msg.reply_markup:
            for ev in reversed(exchange.get("media_events", [])):
                msg = ev.message
                if msg.reply_markup and hasattr(msg.reply_markup, "rows") and msg.reply_markup.rows:
                    keyboard_msg = msg
                    exchange["_keyboard_msg"] = msg
                    break

        if not keyboard_msg or not keyboard_msg.reply_markup:
            return False

        rp = keyboard_msg.reply_markup
        if not hasattr(rp, "rows"):
            return False

        try:
            target_row = rp.rows[row]
            target_btn = target_row.buttons[col]
        except (IndexError, AttributeError):
            return False

        exchange.pop("_keyboard_msg", None)

        try:
            if hasattr(target_btn, "data") and target_btn.data:
                await keyboard_msg.click(data=target_btn.data)
                btn_text = getattr(target_btn, "text", "") or "(图标按钮)"
                logger.info(f"[Resolver] 已点击翻页按钮 [{row},{col}] {btn_text} (bot=@{bot_username})")
                return True
            if hasattr(target_btn, "url") and target_btn.url:
                url = str(target_btn.url)
                if "t.me/" in url or "telegram.me/" in url:
                    from urllib.parse import urlparse, parse_qs
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)
                    start_param = params.get("start", [None])[0]
                    if start_param:
                        entity = await self.client.get_entity(parsed.path.strip("/"))
                        await self.client.send_message(entity, f"/start {start_param}")
                        logger.info(f"[Resolver] 已通过 deep link 翻页 (bot=@{bot_username})")
                        return True
        except Exception as e:
            logger.warning(f"[Resolver] 点击按钮失败 [{row},{col}]: {e}")
        return False

    # ─── 主入口 ──────────────────────────────────────────

    async def resolve_next_batch(self, batch_size: int = None) -> int:
        if batch_size is None:
            batch_size = settings.RESOLVE_BATCH_SIZE

        storage_channel = await self.ensure_storage_channel()
        if not storage_channel:
            logger.error("[Resolver] 存储频道不可用，跳过解析")
            return 0

        codes = self.storage.get_unresolved_codes(
            limit=batch_size, max_attempts=settings.RESOLVE_MAX_RETRIES,
        )
        if not codes:
            return 0

        logger.info(f"[Resolver] 本轮取到 {len(codes)} 个待解析码")

        self._register_handlers()
        db_ok = await self._init_db_pool()
        resolved_count = 0

        for code_row in codes:
            ok = await self._resolve_one(code_row, storage_channel, db_ok)
            if ok:
                resolved_count += 1
            await asyncio.sleep(settings.RESOLVE_DELAY_BETWEEN_CODES)

        return resolved_count

    # ─── 解析单个码 ──────────────────────────────────────

    async def _resolve_one(self, code_row: dict, storage_channel: int, db_ok: bool) -> bool:
        code = code_row["code"]
        bot_username = code_row.get("bot_username", "")
        code_id = code_row["id"]

        if not bot_username:
            bot_username = extract_bot_username(code)
            if not bot_username:
                self.storage.mark_resolve_failed(code_id, "no_bot_username")
                return False

        logger.info(f"[Resolver] 解析文件码: {code} -> @{bot_username}")

        bot_username_lower = bot_username.lower()

        if bot_username_lower in self._bot_exchange:
            logger.debug(f"[Resolver] @{bot_username} 正在被处理中，跳过重复")
            return False

        # ── 获取外部机器人实体 ──
        try:
            entity = await self.client.get_entity(bot_username)
        except (ValueError, UsernameNotOccupiedError) as e:
            logger.warning(f"[Resolver] 外部机器人 @{bot_username} 不存在: {e}")
            self.storage.mark_resolve_failed(code_id, f"bot_not_found:{e}")
            return False
        except FloodWaitError as e:
            logger.warning(f"[Resolver] 触发频率限制，等待 {e.seconds}s")
            await asyncio.sleep(e.seconds)
            return False
        except Exception as e:
            logger.warning(f"[Resolver] 获取机器人实体失败 @{bot_username}: {e}")
            self.storage.mark_resolve_failed(code_id, f"get_entity_error:{e}")
            return False

        # ── 创建 exchange（事件驱动响应收集）──
        collect_event = asyncio.Event()
        now_ts = asyncio.get_event_loop().time()
        self._bot_exchange[bot_username_lower] = {
            "code": code,
            "code_id": code_id,
            "media_events": [],
            "_collect_event": collect_event,
            "_collection_done": False,
            "_expires": now_ts + 300,
            "_settle_task": None,
            "_settle_version": 0,
            "_keyboard_msg": None,
            "_board_version": 0,
            "_last_page_num": None,
            "_page_count": 0,
        }

        # ── 发送码到外部机器人 ──
        try:
            sent = await self.client.send_message(entity, code)
            logger.info(f"[Resolver] 已发送码到 @{bot_username}: {code} (msg_id={sent.id})")
        except FloodWaitError as e:
            logger.warning(f"[Resolver] 发送消息触发频率限制，等待 {e.seconds}s")
            await asyncio.sleep(e.seconds)
            self._cleanup_exchange(bot_username_lower)
            return False
        except ChatWriteForbiddenError:
            logger.warning(f"[Resolver] 无法向 @{bot_username} 发送消息")
            self.storage.mark_resolve_failed(code_id, "chat_write_forbidden")
            self._cleanup_exchange(bot_username_lower)
            return False
        except Exception as e:
            logger.warning(f"[Resolver] 向 @{bot_username} 发送码失败: {e}")
            self.storage.mark_resolve_failed(code_id, f"send_error:{e}")
            self._cleanup_exchange(bot_username_lower)
            return False

        # ── 启动初始 settle ──
        exchange = self._bot_exchange[bot_username_lower]
        exchange["_settle_task"] = asyncio.create_task(
            self._settle_loop_initial(bot_username_lower)
        )

        # ── 等待收集完成 ──
        try:
            await asyncio.wait_for(
                collect_event.wait(),
                timeout=settings.RESOLVE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(f"[Resolver] @{bot_username} 响应超时")
            self.storage.mark_resolve_failed(code_id, "no_response_timeout")
            self._cleanup_exchange(bot_username_lower)
            return False

        # ── 翻页循环：检测并点击"下一页" ──
        exchange = self._bot_exchange.get(bot_username_lower)
        if exchange:
            await self._pagination_loop(bot_username_lower)

        # ── 整理所有响应 ──
        # mediagroup 可能还有未 flush 的
        await asyncio.sleep(1)

        exchange = self._bot_exchange.get(bot_username_lower)
        if not exchange:
            self.storage.mark_resolve_failed(code_id, "exchange_lost")
            return False

        media_events = exchange.get("media_events", [])
        if not media_events:
            logger.warning(f"[Resolver] @{bot_username} 未返回任何文件")
            self.storage.mark_resolve_failed(code_id, "no_files_returned")
            self._cleanup_exchange(bot_username_lower)
            return False

        logger.info(f"[Resolver] @{bot_username} 返回了 {len(media_events)} 个文件")

        # ── 复制到存储频道 ──
        all_storage_ids: list[int] = []
        all_media_meta: list[dict] = []

        for ev in media_events:
            msg = ev.message
            if not msg.media or isinstance(msg.media, MessageMediaWebPage):
                continue

            try:
                forwarded = await self.client.forward_messages(
                    storage_channel,
                    messages=msg.id,
                    from_peer=msg.chat_id if msg.chat_id else entity,
                )
                copied = forwarded[0]
                storage_msg_id = copied.id
                all_storage_ids.append(storage_msg_id)

                fid, ftype = self._extract_media_info(msg)
                all_media_meta.append({"msg_id": str(msg.id), "file_id": fid, "type": ftype})

                logger.debug(
                    f"[Resolver] 已复制文件到存储频道: "
                    f"msg_id={storage_msg_id}, type={ftype}, file_id={fid[:30] if fid else 'N/A'}"
                )
            except Exception as e:
                logger.error(f"[Resolver] 复制 message_id={msg.id} 到存储频道失败: {e}")

            await asyncio.sleep(0.5)

        if not all_storage_ids:
            logger.warning(f"[Resolver] 未能成功复制任何文件到存储频道")
            self.storage.mark_resolve_failed(code_id, "copy_to_storage_failed")
            self._cleanup_exchange(bot_username_lower)
            return False

        batch_ids_str = ",".join(str(s) for s in all_storage_ids)

        # ── 写入 CockroachDB ──
        if db_ok:
            await self._cache_external_file(
                code=code,
                bot_username=bot_username,
                storage_channel=storage_channel,
                storage_ids=all_storage_ids,
                media_meta=all_media_meta,
            )

        self.storage.mark_resolved(
            code_id=code_id,
            storage_channel_id=storage_channel,
            storage_msg_id=all_storage_ids[0],
            storage_batch_ids=batch_ids_str,
        )

        logger.info(
            f"[Resolver] 文件码解析完成: {code} -> 存储频道 {storage_channel}, "
            f"msg_ids=[{batch_ids_str}], 共 {len(all_storage_ids)} 个文件"
        )

        self._cleanup_exchange(bot_username_lower)
        return True

    # ─── 初始 settle（等待第一批响应） ──

    async def _settle_loop_initial(self, bot_username: str):
        await asyncio.sleep(_INITIAL_SETTLE_WAIT)
        exchange = self._bot_exchange.get(bot_username)
        if not exchange:
            return
        exchange["_collection_done"] = True
        exchange["_collect_event"].set()

    # ─── 限速检测 ──────────────────────────────────────────

    _RATE_LIMIT_PATTERNS = [
        (re.compile(r"(?:请|等待?|需\s*要?)\s*(\d+)\s*秒"), 1),
        (re.compile(r"(\d+)\s*秒\s*(?:后|再|之?后)"), 1),
        (re.compile(r"wait\s+(\d+)\s*sec(?:ond)?s?", re.IGNORECASE), 1),
        (re.compile(r"try\s+again\s+(?:in|after)\s+(\d+)\s*sec(?:ond)?s?", re.IGNORECASE), 1),
        (re.compile(r"(\d+)\s*sec(?:ond)?s?\s*(?:later|after)", re.IGNORECASE), 1),
        (re.compile(r"(?:频率|操作)\s*(?:过快|频繁|过于频繁)"), None),
        (re.compile(r"too\s+(?:fast|frequent|many\s+requests)", re.IGNORECASE), None),
        (re.compile(r"(?:请稍[候后]|稍[候后]再试|请勿频繁)"), None),
        (re.compile(r"flood\s*wait", re.IGNORECASE), None),
    ]

    _DEFAULT_RATE_LIMIT_WAIT = 5

    def _check_rate_limit(self, exchange: dict) -> float:
        text_responses = exchange.get("text_responses", [])
        if not text_responses:
            return 0

        # 只看最近的文本响应
        recent = text_responses[-5:]
        for entry in recent:
            text = entry.get("text", "")
            if not text:
                continue
            for pattern, group_idx in self._RATE_LIMIT_PATTERNS:
                m = pattern.search(text)
                if m:
                    if group_idx is not None:
                        try:
                            seconds = int(m.group(group_idx))
                        except (IndexError, ValueError):
                            seconds = self._DEFAULT_RATE_LIMIT_WAIT
                    else:
                        seconds = self._DEFAULT_RATE_LIMIT_WAIT

                    wait_time = min(max(seconds, 1), 60)
                    logger.info(
                        f"[Resolver] 检测到翻页限速: \"{text[:60]}\", "
                        f"等待 {wait_time}s"
                    )
                    return wait_time

        return 0

    # ─── 翻页循环 ──

    async def _pagination_loop(self, bot_username: str):
        max_pages = 10
        for _ in range(max_pages):
            exchange = self._bot_exchange.get(bot_username)
            if not exchange:
                break

            btn_pos = self._detect_next_button(exchange)
            if not btn_pos:
                logger.debug(f"[Resolver] @{bot_username} 无翻页按钮，翻页结束")
                break

            row, col = btn_pos

            rate_wait = self._check_rate_limit(exchange)
            if rate_wait > 0:
                logger.info(f"[Resolver] @{bot_username} 翻页限速等待 {rate_wait}s")
                exchange.pop("text_responses", None)
                await asyncio.sleep(rate_wait)

            media_before = len(exchange.get("media_events", []))
            exchange["_collection_done"] = False
            new_event = asyncio.Event()
            exchange["_collect_event"] = new_event

            exchange["_last_settle_task"] = asyncio.create_task(
                self._settle_after_page(bot_username, new_event)
            )

            clicked = await self._click_button(bot_username, row, col)
            if not clicked:
                break

            try:
                await asyncio.wait_for(new_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                logger.debug(f"[Resolver] @{bot_username} 翻页后无新响应")

            exchange = self._bot_exchange.get(bot_username)
            if not exchange:
                break

            media_after = len(exchange.get("media_events", []))
            if media_after <= media_before:
                logger.debug(f"[Resolver] @{bot_username} 翻页后无新文件，结束翻页")
                break

            exchange["_page_count"] = exchange.get("_page_count", 0) + 1
            logger.info(
                f"[Resolver] @{bot_username} 第 {exchange['_page_count']} 次翻页: "
                f"新增 {media_after - media_before} 个文件 (累计 {media_after})"
            )

    async def _settle_after_page(self, bot_username: str, event: asyncio.Event):
        await asyncio.sleep(_SETTLE_WAIT)
        exchange = self._bot_exchange.get(bot_username)
        if not exchange:
            return
        exchange["_collection_done"] = True
        event.set()

    # ─── 文件信息提取 ──────────────────────────────────

    def _extract_media_info(self, msg: Message) -> Tuple[str, str]:
        try:
            from telethon.tl.types import (
                MessageMediaPhoto as MPhoto,
                MessageMediaDocument as MDoc,
            )
            if isinstance(msg.media, MPhoto):
                fid = pack_bot_file_id(msg.media.photo) or ""
                return fid, "photo"

            if isinstance(msg.media, MDoc) and msg.media.document:
                fid = pack_bot_file_id(msg.media.document) or ""
                mime = getattr(msg.media.document, "mime_type", "") or ""
                if mime.startswith("video/"):
                    ftype = "video"
                elif mime.startswith("audio/"):
                    ftype = "audio"
                elif mime.startswith("image/"):
                    ftype = "photo"
                else:
                    ftype = "document"
                return fid, ftype

            if hasattr(msg, "document") and msg.document:
                fid = pack_bot_file_id(msg.document) or ""
                return fid, "document"
            if hasattr(msg, "video") and msg.video:
                fid = pack_bot_file_id(msg.video) or ""
                return fid, "video"
            if hasattr(msg, "audio") and msg.audio:
                fid = pack_bot_file_id(msg.audio) or ""
                return fid, "audio"
            if hasattr(msg, "photo") and msg.photo:
                fid = pack_bot_file_id(msg.photo) or ""
                return fid, "photo"
        except Exception as e:
            logger.debug(f"[Resolver] 提取 file_id 失败: {e}")

        return "", "document"

    # ─── CockroachDB 缓存 ─────────────────────────────────

    async def _cache_external_file(
        self,
        code: str,
        bot_username: str,
        storage_channel: int,
        storage_ids: List[int],
        media_meta: List[dict],
    ):
        if not self._db_pool or getattr(self._db_pool, "_closed", False):
            return

        lock = self._cache_locks.setdefault(code, asyncio.Lock())
        async with lock:
            batch_ids_str = ",".join(str(s) for s in storage_ids)
            batch_meta_json = json.dumps(media_meta) if media_meta else ""
            file_ids_str = ",".join(
                m.get("file_id", "") for m in media_meta if m.get("file_id")
            )

            try:
                async with self._db_pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT file_code, batch_msg_ids, file_ids, batch_file_meta "
                        "FROM file_records WHERE file_code = $1",
                        code,
                    )

                    if existing:
                        raw_batch = existing["batch_msg_ids"] or ""
                        if not isinstance(raw_batch, str):
                            raw_batch = str(raw_batch)
                        batch_ids = [mid for mid in raw_batch.split(",") if mid.strip()]
                        for sid_str in (str(s) for s in storage_ids):
                            if sid_str not in batch_ids:
                                batch_ids.append(sid_str)

                        raw_fids = existing["file_ids"] or ""
                        if not isinstance(raw_fids, str):
                            raw_fids = str(raw_fids)
                        existing_fids = [f for f in raw_fids.split(",") if f.strip()]
                        for fid in (m.get("file_id", "") for m in media_meta):
                            if fid and fid not in existing_fids:
                                existing_fids.append(fid)

                        old_meta = existing["batch_file_meta"] or ""
                        try:
                            meta_list = (
                                json.loads(old_meta)
                                if isinstance(old_meta, str) and old_meta
                                else (old_meta if isinstance(old_meta, list) else [])
                            )
                        except (json.JSONDecodeError, TypeError):
                            meta_list = []
                        if not isinstance(meta_list, list):
                            meta_list = []
                        existing_mids = {
                            str(e.get("msg_id", "")) for e in meta_list
                            if isinstance(e, dict)
                        }
                        for m in media_meta:
                            if str(m.get("msg_id", "")) not in existing_mids:
                                meta_list.append(m)

                        await conn.execute(
                            """UPDATE file_records SET
                               batch_msg_ids = $1,
                               batch_file_meta = $2,
                               file_ids = $3,
                               primary_channel_id = $4,
                               status = 'active'
                               WHERE file_code = $5""",
                            ",".join(batch_ids),
                            json.dumps(meta_list),
                            ",".join(existing_fids),
                            storage_channel,
                            code,
                        )
                    else:
                        now = datetime.now(timezone.utc)
                        await conn.execute(
                            """INSERT INTO file_records
                               (file_code, uploader_id, primary_channel_id,
                                primary_channel_msg_id, file_types,
                                backup_channel_msg_ids, batch_msg_ids,
                                batch_file_meta, file_ids, status,
                                request_count, create_time, expire_time)
                               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)""",
                            code,
                            0,
                            storage_channel,
                            storage_ids[0],
                            json.dumps({"external": True, "source_bot": bot_username}),
                            "",
                            batch_ids_str,
                            batch_meta_json,
                            file_ids_str,
                            "active",
                            0,
                            now,
                            None,
                        )

                    mapping = await conn.fetchrow(
                        "SELECT code FROM code_bot_mapping WHERE code = $1", code
                    )
                    if not mapping:
                        await conn.execute(
                            "INSERT INTO code_bot_mapping (code, bot_username, created_at) "
                            "VALUES ($1, $2, $3)",
                            code, bot_username, datetime.now(timezone.utc),
                        )

                logger.info(
                    f"[Resolver] 文件码 {code} 已缓存到 CockroachDB: "
                    f"channel={storage_channel}, files={len(storage_ids)}"
                )

            except Exception as e:
                logger.error(f"[Resolver] 缓存外部码失败 (code={code}): {e}")

    # ─── 清理 ────────────────────────────────────────────

    def _cleanup_exchange(self, bot_username: str):
        exchange = self._bot_exchange.pop(bot_username, None)
        if not exchange:
            return
        old_task = exchange.get("_settle_task")
        if old_task and not old_task.done():
            old_task.cancel()
        ev = exchange.get("_collect_event")
        if ev and not ev.is_set():
            ev.set()

    # ─── 持续解析 ────────────────────────────────────────

    async def continuous_resolve(self):
        self._running = True
        cycle = 0
        self._register_handlers()

        logger.info(f"[Resolver] 启动持续解析模式，间隔 {settings.RESOLVE_INTERVAL_SECONDS}s")

        while self._running:
            cycle += 1
            unresolved = self.storage.get_unresolved_count()
            if unresolved == 0:
                logger.debug(f"[Resolver] 第 {cycle} 轮: 无待解析码，等待中...")
            else:
                logger.info(f"[Resolver] === 第 {cycle} 轮解析开始 (待解析: {unresolved}) ===")
                resolved = await self.resolve_next_batch()
                logger.info(f"[Resolver] === 第 {cycle} 轮完成: 解析成功 {resolved} 个 ===")

            if self._running:
                await asyncio.sleep(settings.RESOLVE_INTERVAL_SECONDS)

        logger.info("[Resolver] 持续解析已停止")

    def stop(self):
        self._running = False
        for bot in list(self._bot_exchange.keys()):
            self._cleanup_exchange(bot)
        logger.info("[Resolver] 正在停止解析器...")