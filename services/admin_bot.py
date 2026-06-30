"""管理员 Bot 服务（独立 Telegram Bot）
使用 Bot Token 运行，通过 Cloudflare Worker API 管理 Bot 解码覆盖规则，
并支持通过 Telegram 命令导出文件码 TXT。
仅响应 ADMIN_USER_IDS 中配置的授权用户。

启动方式:
  python run.py admin-bot

依赖:
  需要先部署 Cloudflare Worker (worker/) 并设置 CLOUDFLARE_API_URL / CLOUDFLARE_AUTH_TOKEN
"""

import os
import re
from datetime import datetime
from typing import Optional

from loguru import logger
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes, filters,
)

from config import settings
from database import Storage
from utils.cloudflare_api import CloudflareOverrideAPI


class AdminBot:
    """独立的管理员 Bot — 基于 python-telegram-bot，数据存储在 Cloudflare D1"""

    def __init__(self, storage: Optional[Storage] = None):
        self._app: Optional[Application] = None
        self._api: Optional[CloudflareOverrideAPI] = None
        self._storage: Optional[Storage] = storage

    # ─── 启动 / 停止 ──────────────────────────────────

    async def start(self):
        """启动 Bot"""
        if not settings.TELEGRAM_BOT_TOKEN:
            logger.error("[AdminBot] TELEGRAM_BOT_TOKEN 未配置")
            return

        if not settings.CLOUDFLARE_API_URL or not settings.CLOUDFLARE_AUTH_TOKEN:
            logger.error("[AdminBot] CLOUDFLARE_API_URL / CLOUDFLARE_AUTH_TOKEN 未配置")
            return

        if not settings.ADMIN_USER_IDS:
            logger.error("[AdminBot] ADMIN_USER_IDS 未配置")
            return

        # 初始化 API 客户端
        self._api = CloudflareOverrideAPI(
            base_url=settings.CLOUDFLARE_API_URL,
            auth_token=settings.CLOUDFLARE_AUTH_TOKEN,
        )

        # 健康检查
        if not await self._api.health_check():
            logger.error(
                f"[AdminBot] Cloudflare API 无法连接: {settings.CLOUDFLARE_API_URL}"
            )
            await self._api.close()
            return

        logger.info(f"[AdminBot] Cloudflare API 连接成功: {settings.CLOUDFLARE_API_URL}")

        # 构建 Application
        self._app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()

        # 注册命令
        self._app.add_handler(CommandHandler("start", self._cmd_help))
        self._app.add_handler(CommandHandler("help", self._cmd_help))
        self._app.add_handler(CommandHandler("add_override", self._cmd_add))
        self._app.add_handler(CommandHandler("remove_override", self._cmd_remove))
        self._app.add_handler(CommandHandler("toggle_override", self._cmd_toggle))
        self._app.add_handler(CommandHandler("list_overrides", self._cmd_list))
        self._app.add_handler(CommandHandler("export_codes", self._cmd_export_codes))

        logger.info(
            f"[AdminBot] Bot 已启动, 授权用户: {settings.ADMIN_USER_IDS}"
        )
        await self._app.run_polling()

    async def stop(self):
        """停止 Bot"""
        if self._app:
            await self._app.stop()
        if self._api:
            await self._api.close()
        logger.info("[AdminBot] Bot 已停止")

    # ─── 鉴权检查 ──────────────────────────────────────

    def _is_authorized(self, update: Update) -> bool:
        """检查发送者是否为授权管理员"""
        user = update.effective_user
        if not user:
            return False
        return user.id in settings.ADMIN_USER_IDS

    # ─── /help /start ─────────────────────────────────

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text(
            "**管理员 Bot 命令列表**\n\n"
            "`/add_override <前缀> <bot用户名> [备注]`\n"
            "  添加覆盖规则，以指定前缀开头的文件码使用指定 bot 解析\n\n"
            "`/remove_override <前缀>`\n"
            "  删除覆盖规则\n\n"
            "`/toggle_override <前缀>`\n"
            "  启用/禁用覆盖规则\n\n"
            "`/list_overrides`\n"
            "  列出所有覆盖规则\n\n"
            "`/export_codes`\n"
            "  导出所有已解析的文件码为纯文本 TXT，一行一个\n\n"
            "`/help`\n"
            "  显示此帮助信息",
            parse_mode="Markdown",
        )

    # ─── /add_override ────────────────────────────────

    async def _cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        msg = update.message.text.strip()
        m = re.match(r'^/add_override\s+(\S+)\s+(\S+)\s*(.*)', msg, re.IGNORECASE)
        if not m:
            await update.message.reply_text(
                "用法: `/add_override <前缀> <bot用户名> [备注]`\n\n"
                "例如: `/add_override Search_ MyNewBot 原bot失效了`",
                parse_mode="Markdown",
            )
            return

        prefix, bot, note = m.group(1), m.group(2), m.group(3).strip()
        ok = await self._api.add_override(prefix, bot, note)
        if ok:
            await update.message.reply_text(
                f"已添加覆盖规则:\n"
                f"  前缀: `{prefix}`\n"
                f"  解码 Bot: @{bot}\n"
                f"  备注: {note or '(无)'}\n\n"
                f"以 `{prefix}` 开头的文件码将使用 @{bot} 进行解析。",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(f"添加失败: {prefix}")

    # ─── /remove_override ─────────────────────────────

    async def _cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        msg = update.message.text.strip()
        m = re.match(r'^/remove_override\s+(\S+)', msg, re.IGNORECASE)
        if not m:
            await update.message.reply_text(
                "用法: `/remove_override <前缀>`",
                parse_mode="Markdown",
            )
            return

        prefix = m.group(1)
        ok = await self._api.remove_override(prefix)
        if ok:
            await update.message.reply_text(f"已删除覆盖规则: `{prefix}`", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"未找到覆盖规则: `{prefix}`")

    # ─── /toggle_override ─────────────────────────────

    async def _cmd_toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        msg = update.message.text.strip()
        m = re.match(r'^/toggle_override\s+(\S+)', msg, re.IGNORECASE)
        if not m:
            await update.message.reply_text(
                "用法: `/toggle_override <前缀>`",
                parse_mode="Markdown",
            )
            return

        prefix = m.group(1)
        result = await self._api.toggle_override(prefix)
        if result is None:
            await update.message.reply_text(f"未找到覆盖规则: `{prefix}`", parse_mode="Markdown")
        else:
            status = "启用" if result else "禁用"
            await update.message.reply_text(
                f"覆盖规则 `{prefix}` 已{status}", parse_mode="Markdown"
            )

    # ─── /list_overrides ──────────────────────────────

    async def _cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        overrides = await self._api.list_overrides()
        if not overrides:
            await update.message.reply_text(
                "当前没有覆盖规则。\n\n使用 `/add_override <前缀> <bot用户名>` 添加。",
                parse_mode="Markdown",
            )
            return

        lines = [f"**Bot 覆盖规则列表** ({len(overrides)} 条):\n"]
        for i, o in enumerate(overrides, 1):
            status = "启用" if o.get("is_active") else "禁用"
            note = f" — {o.get('note', '')}" if o.get("note") else ""
            lines.append(
                f"{i}. `{o['code_prefix']}` -> @{o['override_bot_username']}"
                f"  [{status}]{note}"
            )
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    # ─── /export_codes ─────────────────────────────────

    async def _cmd_export_codes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        if not self._storage:
            await update.message.reply_text("存储未初始化，请联系管理员检查配置。")
            return

        # 先发送一条提示消息
        status_msg = await update.message.reply_text("正在导出文件码，请稍候...")

        try:
            # 导出纯码模式 TXT
            filepath = self._storage.export_to_txt(resolved_only=True, include_meta=False)
            if not filepath or not os.path.exists(filepath):
                await status_msg.edit_text("没有已解析的文件码需要导出。")
                return

            # 统计码数量
            with open(filepath, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]
            code_count = len(lines)

            # 发送 TXT 文件
            await update.message.reply_document(
                document=open(filepath, "rb"),
                filename=os.path.basename(filepath),
                caption=f"文件码导出 — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n共 {code_count} 个码",
            )

            # 清理提示消息
            await status_msg.delete()
            logger.info(f"[AdminBot] 管理员 {update.effective_user.id} 导出了 {code_count} 个文件码")

        except Exception as e:
            logger.error(f"[AdminBot] 导出文件码失败: {e}")
            await status_msg.edit_text(f"导出失败: {e}")