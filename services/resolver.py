"""Resolver — 向外部机器人查询文件码 → 通过 up_bot 上传到主系统
核心流程: 发码给外部 Bot → 收集返回文件 → up_bot 上传 → 等待 idx_bot 确认 → 标记已解析
采集器作为普通用户，全程走主系统 up_bot，不直连主系统数据库。
"""

import asyncio
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple

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

from config import settings
from database import Storage
from utils.code_extractor import extract_bot_username

_SETTLE_WAIT = 5
_INITIAL_SETTLE_WAIT = 10
_MEDIA_GROUP_FLUSH_WAIT = 3


class CodeResolver:
    def __init__(self, client: TelegramClient, storage: Storage):
        self.client = client
        self.storage = storage
        self._running = False
        self._handler_registered = False
        self._bot_exchange: dict[str, dict] = {}
        self._media_buffers: dict[str, dict] = {}
        self._cache_locks: dict[str, asyncio.Lock] = {}
        self._upbot: object = None  # UpbotUploader, 延迟初始化

    def _init_upbot(self):
        """延迟初始化 UpbotUploader"""
        if self._upbot is not None:
            return
        if settings.UPLOAD_BOT_USERNAME:
            from services.upbot_uploader import UpbotUploader
            self._upbot = UpbotUploader(self.client)
            logger.info("[Resolver] UpbotUploader 已就绪")

    async def close(self):
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
        _FINISH_KW = {"finish", "done", "完成", "结束"}
        keyboard_msg = exchange.get("_keyboard_msg")
        if not keyboard_msg or not keyboard_msg.reply_markup:
            return None
        rp = keyboard_msg.reply_markup
        if not hasattr(rp, "rows"):
            return None

        exchange.pop("_keyboard_msg", None)

        # Phase 1: text-based next detection
        for row_idx, row in enumerate(rp.rows):
            for col_idx, btn in enumerate(row.buttons):
                btn_text = (getattr(btn, "text", None) or "").lower().strip()
                if any(kw in btn_text for kw in _NEXT_KW):
                    return (row_idx, col_idx)

        # Phase 2: number pagination
        for row in rp.rows:
            btn_texts = [(getattr(b, "text", None) or "").strip() for b in row.buttons]
            numbers = []
            for col_idx, t in enumerate(btn_texts):
                n = self._extract_number(t)
                if n is not None:
                    numbers.append((col_idx, t, n))
            if len(numbers) >= 3:
                all_nums = sorted([n for _, _, n in numbers])
                last = exchange.get("_last_page_num")
                if last is None:
                    target = 2 if 2 in all_nums else all_nums[1] if len(all_nums) > 1 else all_nums[0]
                    exchange["_last_button_range"] = tuple(all_nums)
                else:
                    target = last + 1
                    if target > all_nums[-1]:
                        current_range = tuple(all_nums)
                        prev_range = exchange.get("_last_button_range")
                        if current_range != prev_range:
                            exchange["_last_button_range"] = current_range
                            if target > all_nums[-1]:
                                exchange["_last_page_num"] = None
                                target = 2 if 2 in all_nums else all_nums[1] if len(all_nums) > 1 else all_nums[0]
                        else:
                            break
                for col_idx, t, n in numbers:
                    if n == target:
                        exchange["_last_page_num"] = target
                        row_idx = rp.rows.index(row)
                        return (row_idx, col_idx)
                break

        # Phase 3: icon-only — click rightmost button with callback_data
        for row_idx in range(len(rp.rows) - 1, -1, -1):
            row = rp.rows[row_idx]
            if not row.buttons:
                continue
            btn_texts = [(getattr(b, "text", None) or "").strip() for b in row.buttons]
            all_empty = all(not t for t in btn_texts)
            if all_empty:
                last_btn = row.buttons[-1]
                if getattr(last_btn, "data", None):
                    return (row_idx, len(row.buttons) - 1)

        # Phase 4: any remaining callback button as potential next
        clicked = exchange.get("_clicked_buttons") or set()
        for row_idx, row in enumerate(rp.rows):
            for col_idx, btn in enumerate(row.buttons):
                if getattr(btn, "data", None):
                    btn_text = (getattr(btn, "text", None) or "").strip().lower()
                    if any(kw in btn_text for kw in _FINISH_KW):
                        continue
                    if (row_idx, col_idx) in clicked:
                        continue
                    return (row_idx, col_idx)

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

        async def _do_click():
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
            return False

        try:
            return await _do_click()
        except FloodWaitError as e:
            logger.warning(f"[Resolver] 触发 FloodWait，等待 {e.seconds}s (bot=@{bot_username})")
            exchange["_min_click_interval"] = max(
                exchange.get("_min_click_interval", 0), e.seconds
            )
            await asyncio.sleep(e.seconds)
            try:
                return await _do_click()
            except Exception as retry_e:
                logger.warning(f"[Resolver] FloodWait 后重试失败 [{row},{col}]: {retry_e}")
                return False
        except Exception as e:
            logger.warning(f"[Resolver] 点击按钮失败 [{row},{col}]: {e}")
            return False

    # ─── 主入口 ──────────────────────────────────────────

    async def resolve_next_batch(self, batch_size: int = None) -> int:
        if batch_size is None:
            batch_size = settings.RESOLVE_BATCH_SIZE

        codes = self.storage.get_unresolved_codes(
            limit=batch_size, max_attempts=settings.RESOLVE_MAX_RETRIES,
        )
        if not codes:
            return 0

        logger.info(f"[Resolver] 本轮取到 {len(codes)} 个待解析码")

        self._register_handlers()
        self._init_upbot()

        resolved_count = 0

        for code_row in codes:
            ok = await self._resolve_one(code_row)
            if ok:
                resolved_count += 1
            await asyncio.sleep(settings.RESOLVE_DELAY_BETWEEN_CODES)

        return resolved_count

    # ─── 解析单个码 ──────────────────────────────────────

    async def _resolve_one(self, code_row: dict) -> bool:
        code = code_row["code"]
        bot_username = code_row.get("bot_username", "")
        code_id = code_row["id"]

        # ─── 检查 Bot 覆盖规则（本地 SQLite）───
        override = self.storage.get_bot_override(code)

        if override:
            original_bot = bot_username
            bot_username = override["override_bot_username"]
            logger.info(
                f"[Resolver] 覆盖规则匹配: {code} "
                f"原 bot=@{original_bot} -> 新 bot=@{bot_username} "
                f"(前缀匹配: {override['code_prefix']})"
            )

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

        # ── 检查是否已有映射（本地 SQLite 缓存）──
        if self.storage.is_code_mapped(code):
            logger.debug(f"[Resolver] 文件码 {code} 本地缓存命中，跳过")
            self.storage.mark_resolved(code_id=code_id)
            return True

        # ─── 获取外部机器人实体 ───
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

        # ─── 创建 exchange ───
        collect_event = asyncio.Event()
        now_ts = asyncio.get_event_loop().time()
        self._bot_exchange[bot_username_lower] = {
            "code": code,
            "code_id": code_id,
            "bot_username": bot_username,
            "media_events": [],
            "text_responses": [],
            "_collect_event": collect_event,
            "_collection_done": False,
            "_expires": now_ts + 300,
            "_settle_task": None,
            "_settle_version": 0,
            "_keyboard_msg": None,
            "_board_version": 0,
            "_last_page_num": None,
            "_page_count": 0,
            "_last_button_range": None,
            "_clicked_buttons": set(),
            "_min_click_interval": 0,
            "_last_click_time": 0,
        }

        # ─── 发送码到外部机器人 ───
        cooldown = self.storage.get_bot_cooldown(bot_username)
        if cooldown > 0:
            logger.info(
                f"[Resolver] @{bot_username} 在冷却期，等待 {cooldown:.0f}s"
            )
            self._cleanup_exchange(bot_username_lower)
            await asyncio.sleep(cooldown)

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

        # ─── 启动初始 settle ───
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

        # ─── 翻页循环 ───
        exchange = self._bot_exchange.get(bot_username_lower)
        if exchange:
            await self._pagination_loop(bot_username_lower)

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

        # ============================================================
        # ★ 通过 up_bot 上传文件到主系统，等待 idx_bot 确认
        # ============================================================

        if self._upbot:
            success = await self._upbot.upload_files(media_events, external_code=code)
        else:
            logger.warning("[Resolver] UpbotUploader 未初始化，无法上传")
            self.storage.mark_resolve_failed(code_id, "upbot_not_available")
            self._cleanup_exchange(bot_username_lower)
            return False

        if success:
            logger.info(
                f"[Resolver] 文件码解析完成: {code} → 已上传至主系统, idx_bot 已确认"
            )
            self.storage.mark_code_mapped(code)
            self.storage.mark_resolved(code_id=code_id)
            self._cleanup_exchange(bot_username_lower)
            return True

        # ─── 上传失败 ───
        logger.warning(f"[Resolver] 上传失败，文件码 {code} 标记为失败")
        self.storage.mark_resolve_failed(code_id, "upload_failed")
        self._cleanup_exchange(bot_username_lower)
        return False

    # ─── 初始 settle ──

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
        started_at = asyncio.get_event_loop().time()
        timeout = settings.RESOLVE_PAGINATION_TIMEOUT
        stale_clicks = 0

        while True:
            if timeout > 0:
                elapsed = asyncio.get_event_loop().time() - started_at
                if elapsed > timeout:
                    logger.warning(
                        f"[Resolver] @{bot_username} 翻页总耗时 {elapsed:.0f}s，"
                        f"超过上限 {timeout}s，强制终止翻页"
                    )
                    break

            exchange = self._bot_exchange.get(bot_username)
            if not exchange:
                break

            board_before = exchange.get("_board_version", 0)

            btn_pos = self._detect_next_button(exchange)
            if not btn_pos:
                logger.debug(f"[Resolver] @{bot_username} 无翻页按钮，翻页结束")
                break

            row, col = btn_pos

            exchange = self._bot_exchange.get(bot_username)
            if exchange and exchange.get("_board_version", 0) != board_before:
                logger.debug(f"[Resolver] @{bot_username} 按钮已更新，重新评估")
                continue

            rate_wait = self._check_rate_limit(exchange)
            if rate_wait > 0:
                logger.info(f"[Resolver] @{bot_username} 翻页限速等待 {rate_wait}s")
                self.storage.set_bot_cooldown(bot_username, int(rate_wait))
                exchange.pop("text_responses", None)
                await asyncio.sleep(rate_wait)

            exchange = self._bot_exchange.get(bot_username)
            if exchange:
                min_interval = exchange.get("_min_click_interval", 0)
                last_click = exchange.get("_last_click_time", 0)
                now = asyncio.get_event_loop().time()
                remaining = (last_click + min_interval) - now
                if remaining > 0:
                    logger.debug(
                        f"[Resolver] @{bot_username} 点击间隔限制，等待 {remaining:.1f}s"
                    )
                    await asyncio.sleep(remaining)

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

            exchange = self._bot_exchange.get(bot_username)
            if exchange:
                exchange.setdefault("_clicked_buttons", set()).add((row, col))
                exchange["_last_click_time"] = asyncio.get_event_loop().time()

            try:
                await asyncio.wait_for(new_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                logger.debug(f"[Resolver] @{bot_username} 翻页后无新响应")

            exchange = self._bot_exchange.get(bot_username)
            if not exchange:
                break

            media_after = len(exchange.get("media_events", []))
            if media_after > media_before:
                stale_clicks = 0
                exchange["_page_count"] = exchange.get("_page_count", 0) + 1
                logger.info(
                    f"[Resolver] @{bot_username} 第 {exchange['_page_count']} 次翻页: "
                    f"新增 {media_after - media_before} 个文件 (累计 {media_after})"
                )
            else:
                stale_clicks += 1
                logger.debug(
                    f"[Resolver] @{bot_username} 翻页无新文件 (stale_clicks={stale_clicks}/3)"
                )
                if stale_clicks >= 3:
                    logger.info(f"[Resolver] @{bot_username} 连续翻页无新内容，结束收集")
                    break

    async def _settle_after_page(self, bot_username: str, event: asyncio.Event):
        await asyncio.sleep(_SETTLE_WAIT)
        exchange = self._bot_exchange.get(bot_username)
        if not exchange:
            return
        exchange["_collection_done"] = True
        event.set()

    # ─── 清理 ────────────────────────────────────────────

    def _cleanup_exchange(self, bot_username: str):
        exchange = self._bot_exchange.pop(bot_username, None)
        if not exchange:
            return
        old_task = exchange.get("_settle_task")
        if old_task and not old_task.done():
            old_task.cancel()
        last_settle = exchange.get("_last_settle_task")
        if last_settle and not last_settle.done():
            last_settle.cancel()
        ev = exchange.get("_collect_event")
        if ev and not ev.is_set():
            ev.set()

    # ─── 持续解析 ────────────────────────────────────────

    async def continuous_resolve(self):
        self._running = True
        cycle = 0
        self._register_handlers()
        self._init_upbot()

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