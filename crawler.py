import asyncio
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

from loguru import logger
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError, ChatWriteForbiddenError, FloodWaitError,
    UsernameNotOccupiedError, InviteHashExpiredError, InviteRequestSentError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import (
    Channel, Chat, Document, Message, MessageMediaDocument, MessageMediaPhoto, Photo,
)

from code_extractor import extract_codes_from_message, extract_codes_from_caption, is_external_code
from config import settings
from storage import Storage


class CodeCrawler:
    def __init__(self, client: TelegramClient, storage: Storage):
        self.client = client
        self.storage = storage
        self._running = False
        self._discovered_sources: List[str] = []
        self._processed_messages = set()

    async def login_check(self) -> bool:
        if not await self.client.is_user_authorized():
            logger.error("[Crawler] Telethon 客户端未授权，请先登录")
            return False
        me = await self.client.get_me()
        logger.info(f"[Crawler] 已登录为: {me.first_name} (@{me.username})")
        return True

    async def discover_channels_by_search(self) -> List[dict]:
        found = []
        for keyword in settings.SEARCH_KEYWORDS:
            try:
                logger.info(f"[Discover] 搜索关键词: {keyword}")
                results = await self.client.get_dialogs()
                search_terms = [keyword.lower()]

                for dialog in results:
                    if not isinstance(dialog.entity, (Channel, Chat)):
                        continue
                    entity = dialog.entity
                    title = (getattr(entity, "title", "") or "").lower()
                    username = getattr(entity, "username", "") or ""

                    if any(term in title for term in search_terms):
                        if entity.id not in [c["channel_id"] for c in found]:
                            found.append({
                                "channel_id": entity.id,
                                "username": username,
                                "title": getattr(entity, "title", ""),
                                "type": "channel" if isinstance(entity, Channel) else "group",
                                "members": getattr(entity, "participants_count", 0),
                            })

                try:
                    search_result = await self.client(
                        SearchRequest(q=keyword, limit=settings.SEARCH_LIMIT)
                    )
                    for chat in search_result.chats:
                        if isinstance(chat, (Channel, Chat)):
                            cid = chat.id
                            if cid not in [c["channel_id"] for c in found]:
                                found.append({
                                    "channel_id": cid,
                                    "username": getattr(chat, "username", "") or "",
                                    "title": getattr(chat, "title", ""),
                                    "type": "channel" if isinstance(chat, Channel) else "group",
                                    "members": getattr(chat, "participants_count", 0),
                                })
                except Exception as e:
                    logger.warning(f"[Discover] contacts.Search 失败: {e}")

                await asyncio.sleep(1)

            except FloodWaitError as e:
                logger.warning(f"[Discover] 触发频率限制，等待 {e.seconds} 秒")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"[Discover] 搜索关键字 {keyword} 失败: {e}")

        logger.info(f"[Discover] 搜索完成，找到 {len(found)} 个频道/群组")
        return found

    async def discover_channels_from_dialogs(self) -> List[dict]:
        found = []
        try:
            dialogs = await self.client.get_dialogs(limit=200)
            for dialog in dialogs:
                if not isinstance(dialog.entity, (Channel, Chat)):
                    continue
                entity = dialog.entity
                found.append({
                    "channel_id": entity.id,
                    "username": getattr(entity, "username", "") or "",
                    "title": getattr(entity, "title", ""),
                    "type": "channel" if isinstance(entity, Channel) else "group",
                    "members": getattr(entity, "participants_count", 0),
                })
            logger.info(f"[Discover] 对话列表中找到 {len(found)} 个频道/群组")
        except Exception as e:
            logger.error(f"[Discover] 获取对话列表失败: {e}")
        return found

    async def join_channel(self, entity) -> bool:
        try:
            await self.client(JoinChannelRequest(entity))
            logger.info(f"[Crawler] 已加入频道: {getattr(entity, 'title', entity)}")
            await asyncio.sleep(2)
            return True
        except FloodWaitError as e:
            logger.warning(f"[Crawler] 加入频道触发频率限制，等待 {e.seconds} 秒")
            await asyncio.sleep(e.seconds)
            return False
        except (ChannelPrivateError, ChatWriteForbiddenError) as e:
            logger.warning(f"[Crawler] 无法加入频道 {getattr(entity, 'title', entity)}: {e}")
            return False
        except Exception as e:
            logger.warning(f"[Crawler] 加入频道失败 {getattr(entity, 'title', entity)}: {e}")
            return False

    async def crawl_channel(self, channel_info: dict) -> int:
        channel_id = channel_info["channel_id"]
        title = channel_info.get("title", f"channel_{channel_id}")
        codes_found = 0

        try:
            entity = await self.client.get_entity(channel_id)
        except (ValueError, UsernameNotOccupiedError) as e:
            logger.warning(f"[Crawl] 无法获取频道实体 {channel_id}: {e}")
            return 0
        except Exception as e:
            logger.warning(f"[Crawl] 获取频道实体失败 {channel_id}: {e}")
            return 0

        is_member = False
        try:
            dialogs = await self.client.get_dialogs()
            for d in dialogs:
                if d.entity.id == channel_id:
                    is_member = True
                    break
        except Exception:
            pass

        if not is_member:
            logger.info(f"[Crawl] 尚未加入频道 {title}，尝试加入...")
            joined = await self.join_channel(entity)
            if not joined:
                return 0

        self.storage.save_channel(
            channel_id=channel_id,
            username=getattr(entity, "username", "") or "",
            title=title,
            channel_type=channel_info.get("type", "channel"),
            member_count=channel_info.get("members", 0),
        )

        try:
            logger.info(f"[Crawl] 开始爬取频道: {title} (ID: {channel_id})")
            msg_count = 0
            scan_count = 0

            async for message in self.client.iter_messages(
                entity,
                limit=settings.CRAWL_MESSAGE_LIMIT_PER_CHANNEL,
                wait_time=2,
            ):
                if not message or not self._running:
                    break

                scan_count += 1
                found_in_message = self._process_message(message, channel_id, title)
                msg_count += found_in_message

                if scan_count % 50 == 0:
                    logger.debug(f"[Crawl] {title}: 已扫描 {scan_count} 条消息，发现 {msg_count} 个码")
                    await asyncio.sleep(1)

            self.storage.update_channel_crawled(channel_id, scan_count, msg_count)
            codes_found = msg_count
            logger.info(
                f"[Crawl] 频道 {title} 爬取完成: 扫描 {scan_count} 条消息，发现 {msg_count} 个文件码"
            )

        except FloodWaitError as e:
            logger.warning(f"[Crawl] 爬取 {title} 触发频率限制，等待 {e.seconds} 秒")
            await asyncio.sleep(e.seconds)
        except ChannelPrivateError:
            logger.warning(f"[Crawl] 频道 {title} 为私有频道，跳过")
        except ChatWriteForbiddenError:
            logger.warning(f"[Crawl] 频道 {title} 无读取权限，跳过")
        except Exception as e:
            logger.error(f"[Crawl] 爬取频道 {title} 失败: {e}")

        return codes_found

    def _process_message(self, message: Message, channel_id: int, channel_title: str) -> int:
        msg_id = message.id
        msg_key = f"{channel_id}_{msg_id}"
        if msg_key in self._processed_messages:
            return 0
        self._processed_messages.add(msg_key)

        found_count = 0
        text = getattr(message, "text", None) or (message.message or "").strip()
        caption = (getattr(message, "caption", None) or "").strip()

        all_texts = []
        if text:
            all_texts.append(text.strip())
        if caption and caption != text:
            all_texts.append(caption)

        file_type = None
        file_name = None
        file_size = 0

        if message.media:
            if isinstance(message.media, MessageMediaDocument):
                doc: Document = message.media.document
                file_name = self._extract_filename(doc)
                file_type = doc.mime_type or "unknown"
                file_size = doc.size or 0
            elif isinstance(message.media, MessageMediaPhoto):
                file_type = "image/jpeg"
                file_size = 0

        for msg_text in all_texts:
            if not msg_text:
                continue

            codes = extract_codes_from_message(msg_text)
            for code, bot_username, confidence in codes:
                if not self.storage.code_exists(code):
                    ok = self.storage.save_code(
                        code=code,
                        bot_username=bot_username,
                        source_channel_id=channel_id,
                        source_channel_title=channel_title,
                        source_message_id=msg_id,
                        file_type=file_type,
                        file_name=file_name,
                        file_size=file_size,
                    )
                    if ok:
                        found_count += 1
                        logger.info(f"[Crawl] 发现文件码: {code} (来自 {channel_title})")

        return found_count

    def _extract_filename(self, doc: Document) -> str:
        for attr in doc.attributes:
            from telethon.tl.types import DocumentAttributeFilename
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name
        return ""

    async def discover_and_crawl(self) -> dict:
        if not await self.login_check():
            return {"status": "error", "message": "未登录"}

        self._running = True
        stats = {"channels_found": 0, "channels_crawled": 0, "codes_found": 0}
        all_channels = []

        logger.info("[Crawler] 自动发现频道（从对话列表）...")
        dialog_channels = await self.discover_channels_from_dialogs()
        for ch in dialog_channels:
            if not self._running:
                break
            cid = ch["channel_id"]
            self.storage.save_channel(
                channel_id=cid,
                username=ch.get("username", ""),
                title=ch.get("title", ""),
                channel_type=ch.get("type", "channel"),
                member_count=ch.get("members", 0),
            )
            all_channels.append(ch)

        logger.info("[Crawler] 自动发现频道（从全局搜索）...")
        search_channels = await self.discover_channels_by_search()
        for ch in search_channels:
            if not self._running:
                break
            new = self.storage.save_channel(
                channel_id=ch["channel_id"],
                username=ch.get("username", ""),
                title=ch.get("title", ""),
                channel_type=ch.get("type", "channel"),
                member_count=ch.get("members", 0),
            )
            if new:
                stats["channels_found"] += 1

        if search_channels:
            existing_ids = {c["channel_id"] for c in all_channels}
            for ch in search_channels:
                if ch["channel_id"] not in existing_ids:
                    all_channels.append(ch)

        logger.info(f"[Crawler] 共 {len(all_channels)} 个频道，开始爬取...")

        semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_CRAWLS)

        async def _crawl(ch):
            async with semaphore:
                if not self._running:
                    return 0
                codes = await self.crawl_channel(ch)
                return codes

        tasks = [_crawl(ch) for ch in all_channels]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, int):
                if r > 0:
                    stats["channels_crawled"] += 1
                stats["codes_found"] += r

        self._running = False
        unresolved = self.storage.get_unresolved_count()
        logger.info(
            f"[Crawler] 爬取完成! 发现 {stats['channels_found']} 个新频道, "
            f"爬取 {stats['channels_crawled']} 个频道, "
            f"共找到 {stats['codes_found']} 个文件码, "
            f"待解析: {unresolved} 个"
        )
        return stats

    async def continuous_crawl(self):
        if not await self.login_check():
            logger.error("[Crawler] 未登录，无法启动连续爬取")
            return

        self._running = True
        cycle = 0

        logger.info(f"[Crawler] 启动连续爬取模式，间隔 {settings.CRAWL_INTERVAL_MINUTES} 分钟")

        while self._running:
            cycle += 1
            logger.info(f"[Crawler] === 第 {cycle} 轮爬取开始 ===")

            stats = await self.discover_and_crawl()

            logger.info(
                f"[Crawler] === 第 {cycle} 轮完成: "
                f"发现 {stats['codes_found']} 个文件码 ==="
            )

            overall = self.storage.get_crawl_stats()
            logger.info(
                f"[Crawler] 总计: {overall['channels']} 个频道, "
                f"{overall['codes']} 个文件码"
            )

            if self._running:
                logger.info(f"[Crawler] 等待 {settings.CRAWL_INTERVAL_MINUTES} 分钟后开始下一轮...")
                for _ in range(settings.CRAWL_INTERVAL_MINUTES * 60):
                    if not self._running:
                        break
                    await asyncio.sleep(1)

        logger.info("[Crawler] 连续爬取已停止")

    def stop(self):
        self._running = False
        logger.info("[Crawler] 正在停止爬虫...")