"""UpbotUploader — 通过主系统 up_bot 上传文件（EXTERNAL_RELAY 协议）
采集器作为普通用户向主系统上传文件，全部走 up_bot，不直连主系统数据库。
主系统 idx_bot 负责生成系统码、写入 file_records 和 external_code_mapping。
"""

import asyncio
import re
from typing import Optional

from loguru import logger
from telethon import TelegramClient, events

from config import settings

# idx_bot 外部文件就绪通知: "外部文件 {code} 已就绪"
_EXT_READY_PATTERN = re.compile(r"外部文件\s+(\S+)\s+已就绪")


class UpbotUploader:
    """通过主系统 EXTERNAL_RELAY 协议上传文件到存储频道，等待 idx_bot 确认。"""

    def __init__(self, client: TelegramClient):
        self.client = client
        self._handler_registered = False
        self._me_id: Optional[int] = None
        self._ready_events: dict[str, asyncio.Event] = {}  # external_code → Event

    async def _get_me_id(self) -> int:
        if self._me_id is None:
            me = await self.client.get_me()
            self._me_id = me.id
        return self._me_id

    def register_handler(self):
        """注册事件处理器，捕获 idx_bot 返回的外部文件就绪通知。"""
        if self._handler_registered:
            return
        self._handler_registered = True

        idx_username = settings.DECODER_BOT_USERNAME.lower().lstrip("@")

        @self.client.on(events.NewMessage(incoming=True))
        async def on_code_response(event):
            sender = await event.get_sender()
            if not sender or not sender.bot:
                return

            sender_username = (sender.username or "").lower()
            if sender_username != idx_username:
                return

            text = event.message.message or ""
            if not text:
                return

            # 匹配外部文件就绪通知 "外部文件 xxx 已就绪"
            ext_match = _EXT_READY_PATTERN.search(text)
            if ext_match:
                ext_code = ext_match.group(1)
                logger.info(f"[Upbot] 外部文件就绪通知: {ext_code}")
                ev = self._ready_events.get(ext_code)
                if ev and not ev.is_set():
                    ev.set()

    async def upload_files(self, media_events: list, external_code: str = "") -> bool:
        """通过 EXTERNAL_RELAY 协议上传文件到主系统，等待 idx_bot 确认处理完成。

        Args:
            media_events: 外部机器人返回的媒体事件列表
            external_code: 外部文件码

        Returns:
            True 表示上传成功且主系统已确认处理，False 表示失败
        """
        self.register_handler()

        up_bot_username = settings.UPLOAD_BOT_USERNAME.lstrip("@")
        if not up_bot_username:
            logger.error("[Upbot] UPLOAD_BOT_USERNAME 未配置")
            return False

        try:
            entity = await self.client.get_entity(up_bot_username)
        except Exception as e:
            logger.error(f"[Upbot] 无法获取 up_bot 实体 @{up_bot_username}: {e}")
            return False

        my_id = await self._get_me_id()

        if not external_code:
            logger.warning("[Upbot] 未提供 external_code，无法关联映射")
            return False

        # ─── 1) 逐个发送文件，带 EXTERNAL_RELAY 标记 ───
        file_count = 0
        caption = f"EXTERNAL_RELAY:{my_id}:{external_code}"

        for ev in media_events:
            msg = ev.message
            if not msg.media:
                continue
            try:
                await self.client.send_file(entity, msg.media, caption=caption)
                file_count += 1
                await asyncio.sleep(0.8)
            except Exception as e:
                logger.error(f"[Upbot] 发送文件到 up_bot 失败: {e}")

        if file_count == 0:
            logger.warning("[Upbot] 没有可发送的文件")
            return False

        await asyncio.sleep(1)

        # ─── 2) 发送 EXTERNAL_DONE 信号，触发批量写入 pending_uploads ───
        try:
            await self.client.send_message(entity, f"EXTERNAL_DONE:{my_id}:{external_code}")
            logger.info(
                f"[Upbot] 已向 up_bot 上传 {file_count} 个文件 "
                f"(external_code={external_code}), 等待 idx_bot 确认..."
            )
        except Exception as e:
            logger.error(f"[Upbot] EXTERNAL_DONE 发送失败: {e}")
            return False

        # ─── 3) 等待 idx_bot 返回"外部文件已就绪"通知 ───
        wait_event = asyncio.Event()
        self._ready_events[external_code] = wait_event

        try:
            # 等待消息推送，最多 120 秒
            done, _ = await asyncio.wait(
                [asyncio.create_task(wait_event.wait())],
                timeout=120.0,
            )
            self._ready_events.pop(external_code, None)

            if done:
                logger.info(f"[Upbot] idx_bot 已确认外部文件就绪: {external_code}")
                return True
            else:
                logger.warning(f"[Upbot] 等待 idx_bot 确认超时 (external_code={external_code})")
                return False
        except Exception:
            self._ready_events.pop(external_code, None)
            return False