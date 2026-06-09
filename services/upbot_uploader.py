"""UpbotUploader — 通过主系统 up_bot 上传文件并监听 idx_bot 返回的系统码
采集器解析到外部文件后，作为"用户"向 up_bot 发送文件，走完整的主系统上传流程。
"""

import asyncio
import re
import time
from typing import Optional

from loguru import logger
from telethon import TelegramClient, events

from config import settings


# idx_bot 返回码的消息格式: ✅ 文件码：`tgwenjian_xxx`...
_CODE_PATTERN = re.compile(rf"`\s*({re.escape(settings.FILE_CODE_PREFIX)}\w+)`")


class UpbotUploader:
    """通过 up_bot 上传文件到主系统存储频道，并监听 idx_bot 返回的系统码。"""

    def __init__(self, client: TelegramClient):
        self.client = client
        self._handler_registered = False
        self._code_queue: asyncio.Queue = asyncio.Queue()
        self._code_events: dict[str, asyncio.Event] = {}
        self._code_results: dict[str, str] = {}
        self._seq = 0

    def register_handler(self):
        """注册事件处理器，捕获 idx_bot（或 up_bot 转发）返回的系统码。"""
        if self._handler_registered:
            return
        self._handler_registered = True

        idx_username = settings.DECODER_BOT_USERNAME.lower().lstrip("@")
        up_username = settings.UPLOAD_BOT_USERNAME.lower().lstrip("@")

        @self.client.on(events.NewMessage(incoming=True))
        async def on_code_response(event):
            sender = await event.get_sender()
            if not sender or not sender.bot:
                return

            sender_username = (sender.username or "").lower()
            if sender_username not in (idx_username, up_username):
                return

            text = event.message.message or ""
            if not text or settings.FILE_CODE_PREFIX not in text:
                return

            match = _CODE_PATTERN.search(text)
            if not match:
                logger.debug(f"[Upbot] idx/up 消息未匹配系统码: {text[:120]}")
                return

            system_code = match.group(1)
            logger.info(f"[Upbot] 捕获系统码: {system_code} (来源=@{sender_username})")
            await self._code_queue.put(system_code)

            for ev in list(self._code_events.values()):
                if not ev.is_set():
                    ev.set()

    async def upload_files(self, media_events: list) -> Optional[str]:
        """将解析到的文件发送给 up_bot，等待 idx_bot 返回系统码。

        Args:
            media_events: 外部机器人返回的媒体事件列表

        Returns:
            系统码 (tgwenjian_xxx)，失败返回 None
        """
        self.register_handler()

        up_bot_username = settings.UPLOAD_BOT_USERNAME.lstrip("@")
        if not up_bot_username:
            logger.error("[Upbot] UPLOAD_BOT_USERNAME 未配置")
            return None

        try:
            entity = await self.client.get_entity(up_bot_username)
        except Exception as e:
            logger.error(f"[Upbot] 无法获取 up_bot 实体 @{up_bot_username}: {e}")
            return None

        # 先清空残留队列
        while not self._code_queue.empty():
            try:
                self._code_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        # 1) 发送 /start_upload
        try:
            await self.client.send_message(entity, "/start_upload")
            await asyncio.sleep(1.5)
        except Exception as e:
            logger.error(f"[Upbot] /start_upload 失败: {e}")
            return None

        # 2) 逐个发送文件
        file_count = 0
        for ev in media_events:
            msg = ev.message
            if not msg.media:
                continue
            try:
                await self.client.send_file(entity, msg.media, caption="")
                file_count += 1
                await asyncio.sleep(0.8)
            except Exception as e:
                logger.error(f"[Upbot] 发送文件到 up_bot 失败: {e}")

        if file_count == 0:
            logger.warning("[Upbot] 没有可发送的文件，取消上传")
            await self.client.send_message(entity, "/cancel_upload")
            return None

        await asyncio.sleep(1)

        # 3) 发送 /end_upload
        try:
            await self.client.send_message(entity, "/end_upload")
            logger.info(f"[Upbot] 已向 up_bot 上传 {file_count} 个文件，等待 idx_bot 生成系统码...")
        except Exception as e:
            logger.error(f"[Upbot] /end_upload 失败: {e}")
            return None

        # 4) 等待 idx_bot 返回系统码（带超时）
        wait_event = asyncio.Event()
        self._seq += 1
        self._code_events[str(self._seq)] = wait_event

        try:
            # 先检查队列中是否已有码（可能在 /end_upload 前就到了）
            try:
                code = self._code_queue.get_nowait()
                self._code_events.pop(str(self._seq), None)
                logger.info(f"[Upbot] 已获取系统码 (即时): {code}")
                return code
            except asyncio.QueueEmpty:
                pass

            await asyncio.wait_for(wait_event.wait(), timeout=120)

            try:
                code = self._code_queue.get_nowait()
                self._code_events.pop(str(self._seq), None)
                logger.info(f"[Upbot] 已获取系统码: {code}")
                return code
            except asyncio.QueueEmpty:
                self._code_events.pop(str(self._seq), None)
                logger.warning("[Upbot] 等待系统码超时（事件触发但队列为空）")
                return None

        except asyncio.TimeoutError:
            self._code_events.pop(str(self._seq), None)
            logger.warning("[Upbot] 等待系统码超时 (120s)")
            return None