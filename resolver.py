import asyncio
import json
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import asyncpg
from loguru import logger
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, UsernameNotOccupiedError, ChatWriteForbiddenError,
)
from telethon.tl.types import (
    Message, MessageMediaDocument, MessageMediaPhoto,
    MessageMediaWebPage,
)

from code_extractor import extract_bot_username
from config import settings
from storage import Storage

_MEDIA_GROUP_BUFFER_WAIT = 3


class CodeResolver:
    def __init__(self, client: TelegramClient, storage: Storage):
        self.client = client
        self.storage = storage
        self._running = False
        self._db_pool: Optional[asyncpg.Pool] = None
        self._media_group_buffer: dict[str, list] = {}
        self._media_group_timers: dict[str, asyncio.Task] = {}

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
        for tid in list(self._media_group_timers.keys()):
            task = self._media_group_timers.pop(tid, None)
            if task and not task.done():
                task.cancel()

    # ─── 主入口 ──────────────────────────────────────────

    async def resolve_next_batch(self, batch_size: int = None) -> int:
        if batch_size is None:
            batch_size = settings.RESOLVE_BATCH_SIZE

        storage_channel = await self.ensure_storage_channel()
        if not storage_channel:
            logger.error("[Resolver] 存储频道不可用，跳过解析")
            return 0

        codes = self.storage.get_unresolved_codes(limit=batch_size,
                                                   max_attempts=settings.RESOLVE_MAX_RETRIES)
        if not codes:
            logger.debug("[Resolver] 没有待解析的文件码")
            return 0

        logger.info(f"[Resolver] 本轮取到 {len(codes)} 个待解析码")

        db_ok = await self._init_db_pool()
        resolved_count = 0

        for code_row in codes:
            if not self._running:
                break
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

        # 获取外部机器人实体
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

        # 发送码到外部机器人
        try:
            sent = await self.client.send_message(entity, code)
            logger.debug(f"[Resolver] 已发送码到 @{bot_username}: {code} (msg_id={sent.id})")
        except FloodWaitError as e:
            logger.warning(f"[Resolver] 发送消息触发频率限制，等待 {e.seconds}s")
            await asyncio.sleep(e.seconds)
            return False
        except ChatWriteForbiddenError:
            logger.warning(f"[Resolver] 无法向 @{bot_username} 发送消息（可能被限制）")
            self.storage.mark_resolve_failed(code_id, "chat_write_forbidden")
            return False
        except Exception as e:
            logger.warning(f"[Resolver] 向 @{bot_username} 发送码失败: {e}")
            self.storage.mark_resolve_failed(code_id, f"send_error:{e}")
            return False

        # ─── 等待外部机器人响应 ──────────────────────────
        # 复用主系统的媒体组缓冲策略（_MEDIA_GROUP_BUFFER_WAIT = 3秒）
        # 先等待响应到达，然后用媒体组缓冲获得所有同组消息

        response_messages = await self._wait_for_bot_responses(entity, sent.id)
        if not response_messages:
            logger.warning(f"[Resolver] @{bot_username} 对码 {code} 无响应（超时）")
            self.storage.mark_resolve_failed(code_id, "no_response_timeout")
            return False

        logger.info(f"[Resolver] @{bot_username} 返回了 {len(response_messages)} 个消息")

        # ─── 按媒体组分组 ────────────────────────────────
        # 与主系统 _flush_media_group_buffer 的流程一致：
        # 先等待3秒收集同组消息，再统一复制到存储频道

        grouped: dict[str, list[Message]] = {}
        for msg in response_messages:
            gid = getattr(msg, "grouped_id", None)
            key = str(gid) if gid else f"single_{msg.id}"
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(msg)

        # 如果有媒体组，等待3秒让同组其他消息到达（主系统标准策略）
        has_group = any(k.startswith("group") or not k.startswith("single_") for k in grouped)
        if has_group:
            logger.debug(f"[Resolver] 检测到媒体组，等待 {_MEDIA_GROUP_BUFFER_WAIT}s 收集同组消息...")
            await asyncio.sleep(_MEDIA_GROUP_BUFFER_WAIT)

            for gid_str in list(grouped.keys()):
                if gid_str.startswith("single_"):
                    continue
                try:
                    async for msg in self.client.iter_messages(
                        entity,
                        offset_id=sent.id,
                        limit=100,
                        wait_time=1,
                    ):
                        if getattr(msg, "grouped_id", None) and str(msg.grouped_id) == gid_str:
                            if msg.id not in {m.id for m in grouped[gid_str]}:
                                grouped[gid_str].append(msg)
                except Exception:
                    pass

        # ─── 复制到存储频道 + 写入 DB ───────────────────
        # 与主系统 handle_external_file_response / _flush_media_group_buffer 完全一致

        all_storage_ids: list[int] = []
        all_media_meta: list[dict] = []

        for group_key, group_msgs in grouped.items():
            for msg in group_msgs:
                if not msg.media or isinstance(msg.media, MessageMediaWebPage):
                    continue

                try:
                    # client.copy_message 对应主系统的 bot.copy_message
                    copied = await self.client.copy_message(
                        to_peer=storage_channel,
                        from_peer=msg.chat_id if msg.chat_id else entity,
                        message_id=msg.id,
                    )
                    storage_msg_id = copied.id
                    all_storage_ids.append(storage_msg_id)

                    # _extract_media_info 与主系统的提取逻辑一致
                    fid, ftype = self._extract_media_info(msg)
                    all_media_meta.append({"file_id": fid, "type": ftype})

                    logger.debug(f"[Resolver] 已复制文件到存储频道: msg_id={storage_msg_id}, type={ftype}")
                except Exception as e:
                    logger.error(f"[Resolver] 复制 message_id={msg.id} 到存储频道失败: {e}")

                await asyncio.sleep(0.5)

        if not all_storage_ids:
            logger.warning(f"[Resolver] 未能成功复制任何文件到存储频道")
            self.storage.mark_resolve_failed(code_id, "copy_to_storage_failed")
            return False

        batch_ids_str = ",".join(str(s) for s in all_storage_ids)

        # 写入 CockroachDB（与主系统 _cache_external_file + make_file_record 一致）
        if db_ok:
            await self._cache_external_file(
                code=code,
                bot_username=bot_username,
                storage_channel=storage_channel,
                storage_ids=all_storage_ids,
                media_meta=all_media_meta,
            )

        # 标记本地解析完成
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
        return True

    # ─── _extract_media_info ─────────────────────────────
    # 与主系统 decoder_bot.py L199-L212 完全一致
    # 主系统: msg.photo → msg.photo[-1].file_id
    #         msg.video → msg.video.file_id
    # 因为 Telethon 消息结构与 python-telegram-bot 不同，这里用 Telethon 的 API 实现相同逻辑

    def _extract_media_info(self, msg: Message) -> Tuple[str, str]:
        if isinstance(msg.media, MessageMediaPhoto):
            photo_sizes = getattr(msg.media, "sizes", [])
            if photo_sizes:
                return getattr(photo_sizes[-1], "location", ""), "photo"
            return "", "photo"

        if hasattr(msg, "document") and msg.document:
            fid = getattr(msg.document, "id", "") or ""
            mime = getattr(msg.document, "mime_type", "") or ""
            ftype = "document"
            if mime:
                if mime.startswith("video/"):
                    ftype = "video"
                elif mime.startswith("audio/"):
                    ftype = "audio"
                elif mime.startswith("image/"):
                    ftype = "photo"
            return str(fid), ftype

        if hasattr(msg, "video") and msg.video:
            return getattr(msg.video, "file_id", "") or "", "video"
        if hasattr(msg, "audio") and msg.audio:
            return getattr(msg.audio, "file_id", "") or "", "audio"
        if hasattr(msg, "voice") and msg.voice:
            return getattr(msg.voice, "file_id", "") or "", "voice"
        if hasattr(msg, "animation") and msg.animation:
            return getattr(msg.animation, "file_id", "") or "", "animation"
        if hasattr(msg, "document") and msg.document:
            return str(getattr(msg.document, "id", "")), "document"

        return "", "document"

    # ─── _cache_external_file ────────────────────────────
    # 与主系统 decoder_bot.py L361-L392 完全一致
    # 主系统逻辑:
    #   1. 查 file_records 是否已有该 code
    #   2. 已有 → 追加 batch_msg_ids
    #   3. 没有 → 用 make_file_record 创建新记录插入

    async def _cache_external_file(
        self,
        code: str,
        bot_username: str,
        storage_channel: int,
        storage_ids: List[int],
        media_meta: List[dict],
    ):
        if not self._db_pool:
            return

        batch_ids_str = ",".join(str(s) for s in storage_ids)
        batch_meta_json = json.dumps(media_meta) if media_meta else ""
        file_ids_str = ",".join(m.get("file_id", "") for m in media_meta)

        try:
            async with self._db_pool.acquire() as conn:
                existing = await conn.fetchrow(
                    "SELECT file_code, batch_msg_ids, file_ids FROM file_records WHERE file_code = $1",
                    code,
                )

                if existing:
                    # ── 追加 batch_msg_ids ──
                    # 与主系统 _cache_external_file L367-L377 完全一致
                    raw_batch = existing["batch_msg_ids"] or ""
                    if not isinstance(raw_batch, str):
                        raw_batch = str(raw_batch)
                    batch_ids = [mid for mid in raw_batch.split(",") if mid.strip()]
                    for sid_str in (str(s) for s in storage_ids):
                        if sid_str not in batch_ids:
                            batch_ids.append(sid_str)
                    new_batch_str = ",".join(batch_ids)

                    # 同时追加 file_ids
                    raw_fids = existing["file_ids"] or ""
                    if not isinstance(raw_fids, str):
                        raw_fids = str(raw_fids)
                    existing_fids = [f for f in raw_fids.split(",") if f.strip()]
                    for fid in (m.get("file_id", "") for m in media_meta):
                        if fid and fid not in existing_fids:
                            existing_fids.append(fid)
                    new_file_ids = ",".join(existing_fids)

                    await conn.execute(
                        """UPDATE file_records SET
                           batch_msg_ids = $1,
                           batch_file_meta = $2,
                           file_ids = $3,
                           primary_channel_id = $4,
                           status = 'active'
                           WHERE file_code = $5""",
                        new_batch_str, batch_meta_json, new_file_ids,
                        storage_channel, code,
                    )
                    logger.info(
                        f"[_cache_external_file] 外部码 {code} 追加: "
                        f"batch_msg_ids={new_batch_str}, file_ids={new_file_ids}"
                    )
                else:
                    # ── 创建新记录 ──
                    # 与主系统 make_file_record L44-L72 完全一致
                    # 所有字段与 file_records DDL 严格对应:
                    #   file_code, uploader_id, primary_channel_id, primary_channel_msg_id,
                    #   file_types, backup_channel_msg_ids, batch_msg_ids, batch_file_meta,
                    #   file_ids, status, request_count, create_time, expire_time
                    now = datetime.now(timezone.utc)
                    await conn.execute(
                        """INSERT INTO file_records
                           (file_code, uploader_id, primary_channel_id, primary_channel_msg_id,
                            file_types, backup_channel_msg_ids, batch_msg_ids, batch_file_meta,
                            file_ids, status, request_count, create_time, expire_time)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
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
                    logger.info(
                        f"[_cache_external_file] 外部码已缓存到本地: {code}, "
                        f"channel={storage_channel}, msg_ids=[{batch_ids_str}]"
                    )

                # ── code_bot_mapping ──
                # 与主系统 save_code_bot_mapping 一致
                mapping = await conn.fetchrow(
                    "SELECT code FROM code_bot_mapping WHERE code = $1", code
                )
                if not mapping:
                    await conn.execute(
                        """INSERT INTO code_bot_mapping (code, bot_username, created_at)
                           VALUES ($1, $2, $3)""",
                        code, bot_username, datetime.now(timezone.utc),
                    )

        except Exception as e:
            logger.error(f"[_cache_external_file] 缓存外部码失败 (code={code}): {e}")

    # ─── 等待外部机器人响应 ─────────────────────────────

    async def _wait_for_bot_responses(
        self, bot_entity, after_msg_id: int
    ) -> List[Message]:
        all_responses: List[Message] = []
        deadline = time.time() + settings.RESOLVE_TIMEOUT_SECONDS
        seen_ids = set()
        bot_id = getattr(bot_entity, "id", None)

        while time.time() < deadline:
            try:
                async for msg in self.client.iter_messages(
                    bot_entity,
                    offset_id=after_msg_id,
                    limit=20,
                    wait_time=2,
                ):
                    if msg.id in seen_ids:
                        continue
                    seen_ids.add(msg.id)

                    if msg.out:
                        continue
                    if msg.sender_id and msg.sender_id == bot_id:
                        continue

                    if msg.sender_id:
                        try:
                            sender = await msg.get_sender()
                            if sender:
                                if not getattr(sender, "bot", False):
                                    continue
                        except Exception:
                            pass

                    if msg.media and not isinstance(msg.media, MessageMediaWebPage):
                        all_responses.append(msg)

                if all_responses:
                    break

            except FloodWaitError as e:
                logger.warning(f"[Resolver] 等待响应时触发频率限制: {e.seconds}s")
                await asyncio.sleep(min(e.seconds, 10))
            except Exception as e:
                logger.debug(f"[Resolver] 轮询响应时发生临时错误: {e}")

            remaining = deadline - time.time()
            if remaining > 0 and not all_responses:
                await asyncio.sleep(min(1, remaining))

        if not all_responses:
            return []

        all_responses.sort(key=lambda m: m.id)

        final_msgs = list(all_responses)
        final_ids = {m.id for m in final_msgs}

        try:
            await asyncio.sleep(1)
            async for msg in self.client.iter_messages(
                bot_entity, offset_id=after_msg_id, limit=30, wait_time=1,
            ):
                if msg.id in final_ids:
                    continue
                if msg.out:
                    continue
                if msg.sender_id and msg.sender_id == bot_id:
                    continue
                if msg.media and not isinstance(msg.media, MessageMediaWebPage):
                    final_msgs.append(msg)
                    final_ids.add(msg.id)
        except Exception:
            pass

        final_msgs.sort(key=lambda m: m.id)
        return final_msgs

    # ─── 持续解析 ────────────────────────────────────────

    async def continuous_resolve(self):
        self._running = True
        cycle = 0

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
        logger.info("[Resolver] 正在停止解析器...")