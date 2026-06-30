"""UpbotUploader — 通过主系统 up_bot 上传文件（EXTERNAL_RELAY 协议）并获取系统码
采集器解析到外部文件后，走主系统专用的中继上传协议，避免普通上传流程的交互式按钮。
"""

import asyncio
import re
from typing import Optional

from loguru import logger
from telethon import TelegramClient, events

from config import settings

# idx_bot 系统码通知消息格式: "外部文件 xxx 已就绪，请重新发送文件码即可查收。"
# 或 "文件码：`tgwenjian_xxx`..."
_CODE_PATTERN = re.compile(rf"`\s*({re.escape(settings.FILE_CODE_PREFIX)}\w+)`")
# 外部文件就绪通知: "外部文件 {code} 已就绪"
_EXT_READY_PATTERN = re.compile(r"外部文件\s+(\S+)\s+已就绪")


class UpbotUploader:
    """通过主系统 EXTERNAL_RELAY 协议上传文件到存储频道，获取系统码。"""

    def __init__(self, client: TelegramClient):
        self.client = client
        self._handler_registered = False
        self._me_id: Optional[int] = None
        self._code_queue: asyncio.Queue = asyncio.Queue()
        self._pending: dict[str, asyncio.Event] = {}  # external_code → Event

    async def _get_me_id(self) -> int:
        if self._me_id is None:
            me = await self.client.get_me()
            self._me_id = me.id
        return self._me_id

    def register_handler(self):
        """注册事件处理器，捕获 idx_bot 返回的系统码通知。"""
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

            # 方式1: 匹配系统码格式 `tgwenjian_xxx`
            match = _CODE_PATTERN.search(text)
            if match:
                system_code = match.group(1)
                logger.info(f"[Upbot] 捕获系统码: {system_code} (来源=@{sender_username})")
                await self._code_queue.put(system_code)
                for ev in list(self._pending.values()):
                    if not ev.is_set():
                        ev.set()
                return

            # 方式2: 匹配外部文件就绪通知 "外部文件 xxx 已就绪"
            ext_match = _EXT_READY_PATTERN.search(text)
            if ext_match:
                ext_code = ext_match.group(1)
                logger.info(f"[Upbot] 外部文件就绪通知: {ext_code}")
                await self._code_queue.put(f"__ext_ready__:{ext_code}")
                ev = self._pending.get(ext_code)
                if ev and not ev.is_set():
                    ev.set()

    async def upload_files(self, media_events: list, external_code: str = "") -> Optional[str]:
        """通过 EXTERNAL_RELAY 协议上传文件到主系统，获取系统码。

        Args:
            media_events: 外部机器人返回的媒体事件列表
            external_code: 外部文件码（用于关联映射）

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

        my_id = await self._get_me_id()

        # 清空残留队列
        while not self._code_queue.empty():
            try:
                self._code_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        if not external_code:
            logger.warning("[Upbot] 未提供 external_code，无法关联映射")
            return None

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
            return None

        await asyncio.sleep(1)

        # ─── 2) 发送 EXTERNAL_DONE 信号，触发批量写入 pending_uploads ───
        try:
            await self.client.send_message(entity, f"EXTERNAL_DONE:{my_id}:{external_code}")
            logger.info(
                f"[Upbot] 已向 up_bot 上传 {file_count} 个文件 "
                f"(external_code={external_code}), 等待 idx_bot 生成系统码..."
            )
        except Exception as e:
            logger.error(f"[Upbot] EXTERNAL_DONE 发送失败: {e}")
            return None

        # ─── 3) 等待 idx_bot 返回系统码（双重机制：消息监听 + 数据库轮询）───
        wait_event = asyncio.Event()
        self._pending[external_code] = wait_event

        try:
            # 先快速检查队列
            try:
                item = self._code_queue.get_nowait()
                if item.startswith("__ext_ready__:"):
                    # 收到就绪通知，继续等系统码
                    pass
                else:
                    self._pending.pop(external_code, None)
                    logger.info(f"[Upbot] 已获取系统码 (即时): {item}")
                    return item
            except asyncio.QueueEmpty:
                pass

            # 等待（消息推送或数据库轮询）
            for _ in range(60):  # 最多等 120 秒
                done, _ = await asyncio.wait(
                    [asyncio.create_task(wait_event.wait())],
                    timeout=2.0,
                )
                if done:
                    break

                # 每 2 秒检查一次队列
                try:
                    item = self._code_queue.get_nowait()
                    if item.startswith("__ext_ready__:"):
                        pass  # 就绪通知，继续等系统码
                    else:
                        self._pending.pop(external_code, None)
                        logger.info(f"[Upbot] 已获取系统码: {item}")
                        return item
                except asyncio.QueueEmpty:
                    pass

                # 数据库轮询：查 external_code_mapping
                system_code = await self._poll_db_mapping(external_code)
                if system_code:
                    self._pending.pop(external_code, None)
                    logger.info(f"[Upbot] 通过数据库轮询获取系统码: {system_code}")
                    return system_code

            # 最后再检查一次队列
            try:
                item = self._code_queue.get_nowait()
                if not item.startswith("__ext_ready__:"):
                    self._pending.pop(external_code, None)
                    return item
            except asyncio.QueueEmpty:
                pass

            self._pending.pop(external_code, None)
            logger.warning(f"[Upbot] 等待系统码超时 (external_code={external_code})")
            return None

        except asyncio.TimeoutError:
            self._pending.pop(external_code, None)
            logger.warning("[Upbot] 等待系统码超时 (120s)")
            return None

    async def _poll_db_mapping(self, external_code: str) -> Optional[str]:
        """从 CockroachDB 轮询 external_code_mapping 表。"""
        try:
            import asyncpg
            if not settings.COCKROACHDB_URL:
                return None
            conn = await asyncpg.connect(settings.COCKROACHDB_URL, statement_cache_size=0)
            try:
                row = await conn.fetchrow(
                    "SELECT system_code FROM external_code_mapping WHERE external_code = $1",
                    external_code,
                )
                if row:
                    return row["system_code"]
            finally:
                await conn.close()
        except Exception:
            pass
        return None