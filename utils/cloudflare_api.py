"""Cloudflare Worker API 客户端
封装对 Bot 覆盖规则 API 的 HTTP 调用（Bearer Token 鉴权）。
"""

from typing import Optional

import httpx
from loguru import logger


class CloudflareOverrideAPI:
    """Cloudflare D1 覆盖规则 API 客户端"""

    def __init__(self, base_url: str, auth_token: str, timeout: float = 10.0):
        """
        Args:
            base_url: Worker 部署后的 URL，如 https://bot-override-api.your-subdomain.workers.dev
            auth_token: Bearer Token，与 Worker 中的 AUTH_TOKEN secret 一致
            timeout: 请求超时（秒）
        """
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    async def close(self):
        await self._client.aclose()

    # ─── 列表 ──────────────────────────────────────

    async def list_overrides(self) -> list[dict]:
        """获取所有覆盖规则"""
        try:
            r = await self._client.get("/api/overrides")
            r.raise_for_status()
            data = r.json()
            return data.get("overrides", [])
        except Exception as e:
            logger.error(f"[CloudflareAPI] 获取覆盖规则失败: {e}")
            return []

    # ─── 添加/更新 ──────────────────────────────────

    async def add_override(
        self, code_prefix: str, override_bot_username: str, note: str = ""
    ) -> bool:
        """添加或更新覆盖规则"""
        try:
            r = await self._client.post(
                "/api/overrides",
                json={
                    "code_prefix": code_prefix,
                    "override_bot_username": override_bot_username,
                    "note": note,
                },
            )
            r.raise_for_status()
            logger.info(
                f"[CloudflareAPI] 已添加覆盖: {code_prefix} -> @{override_bot_username}"
            )
            return True
        except Exception as e:
            logger.error(f"[CloudflareAPI] 添加覆盖失败: {e}")
            return False

    # ─── 删除 ──────────────────────────────────────

    async def remove_override(self, code_prefix: str) -> bool:
        """删除覆盖规则"""
        try:
            r = await self._client.delete(
                "/api/overrides", params={"prefix": code_prefix}
            )
            r.raise_for_status()
            data = r.json()
            ok = data.get("deleted", False)
            if ok:
                logger.info(f"[CloudflareAPI] 已删除覆盖: {code_prefix}")
            return ok
        except Exception as e:
            logger.error(f"[CloudflareAPI] 删除覆盖失败: {e}")
            return False

    # ─── 开关 ──────────────────────────────────────

    async def toggle_override(self, code_prefix: str) -> Optional[bool]:
        """切换覆盖规则的启用/禁用，返回新状态（None 表示失败）"""
        try:
            r = await self._client.patch(
                "/api/overrides/toggle", params={"prefix": code_prefix}
            )
            if r.status_code == 404:
                return None
            r.raise_for_status()
            data = r.json()
            is_active = data.get("is_active", False)
            logger.info(
                f"[CloudflareAPI] 覆盖 {code_prefix} 已{'启用' if is_active else '禁用'}"
            )
            return is_active
        except Exception as e:
            logger.error(f"[CloudflareAPI] 切换覆盖失败: {e}")
            return None

    # ─── 前缀匹配 ──────────────────────────────────

    async def match_override(self, code: str) -> Optional[dict]:
        """查找文件码匹配的覆盖规则（最长前缀匹配），返回 None 表示无匹配"""
        try:
            r = await self._client.get(
                "/api/overrides/match", params={"code": code}
            )
            r.raise_for_status()
            data = r.json()
            return data.get("override")
        except Exception as e:
            logger.debug(f"[CloudflareAPI] 查询覆盖匹配失败 (code={code}): {e}")
            return None

    # ─── 健康检查 ──────────────────────────────────

    async def health_check(self) -> bool:
        """检查 API 是否可用"""
        try:
            r = await self._client.get("/health")
            return r.status_code == 200
        except Exception:
            return False