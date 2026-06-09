# 多账号解析支持 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 CodeResolver 接入多个 Telegram 账号，通过轮询方式分散第三方解码机器人请求，突破单账号限频。

**Architecture:** 新增 `ResolverPool` 管理多个 `(TelegramClient, CodeResolver, UpbotUploader)` 三元组，共用 `Storage` 和 CockroachDB 连接池。配置层新增 `AccountConfig` 和账号列表解析，CLI 新增多账号登录命令。

**Tech Stack:** Python 3.11+, Telethon, asyncpg, Pydantic Settings, asyncio

---

## 文件结构

| 文件 | 操作 | 职责 |
|---|---|---|
| `config/settings.py` | 修改 | 新增 `TELEGRAM_ACCOUNT_PHONES` 配置项 + `AccountConfig` dataclass + `get_account_configs()` |
| `services/resolver_pool.py` | **新建** | `ResolverPool` — 管理多账号 client/resolver/upbot 实例，轮询调度 |
| `services/resolver.py` | 修改 | 新增 `set_shared_resources()`，从 `resolve_next_batch` 中移除资源初始化 |
| `services/__init__.py` | 修改 | 导出 `ResolverPool` |
| `run.py` | 修改 | 新增 `cmd_login_all`，修改 `cmd_resolve`/`cmd_daemon` 使用 `ResolverPool` |

---

### Task 1: 配置层 — 多账号支持

**Files:**
- Modify: `config/settings.py`

- [ ] **Step 1: 在 Settings 中新增多账号配置项**

在 `config/settings.py` 的 `Settings` 类中，`TELEGRAM_PHONE` 之后新增：

```python
    # ── 多账号（Resolver 专用）───────────────────────────
    TELEGRAM_ACCOUNT_PHONES: str = ""  # 逗号分隔的手机号列表，为空则单账号模式
```

在文件末尾 `settings = Settings()` 之前新增 `AccountConfig` dataclass 和 `get_account_configs()` 函数：

```python
from dataclasses import dataclass, field
from typing import List


@dataclass
class AccountConfig:
    name: str
    api_id: int
    api_hash: str
    phone: str
    session_file: str = ""


def get_account_configs() -> List[AccountConfig]:
    """解析多账号配置，返回 AccountConfig 列表。
    若 TELEGRAM_ACCOUNT_PHONES 为空，退化为单账号模式（返回单元素列表）。
    """
    phones_str = settings.TELEGRAM_ACCOUNT_PHONES.strip()
    if not phones_str:
        # 单账号模式：使用原有的 TELEGRAM_PHONE
        return [
            AccountConfig(
                name="default",
                api_id=settings.TELEGRAM_API_ID,
                api_hash=settings.TELEGRAM_API_HASH,
                phone=settings.TELEGRAM_PHONE,
                session_file="code_crawler_session",
            )
        ]

    phones = [p.strip() for p in phones_str.split(",") if p.strip()]
    configs = []
    for i, phone in enumerate(phones):
        # 检查是否有独立 API 凭证（ACCOUNT_0_API_ID 等环境变量）
        env_prefix = f"ACCOUNT_{i}_"
        api_id_raw = os.environ.get(f"{env_prefix}API_ID")
        api_hash_raw = os.environ.get(f"{env_prefix}API_HASH")

        configs.append(
            AccountConfig(
                name=f"account_{i}",
                api_id=int(api_id_raw) if api_id_raw else settings.TELEGRAM_API_ID,
                api_hash=api_hash_raw or settings.TELEGRAM_API_HASH,
                phone=phone,
                session_file=f"session_account_{i}",
            )
        )
    return configs
```

> 注意：`get_account_configs()` 中使用了 `os.environ`，文件顶部已有隐式的 os 导入？检查 `settings.py` 顶部，如果没有 `import os` 则需添加。

- [ ] **Step 2: 确认 settings.py 顶部有 `import os`**

检查 `config/settings.py` 顶部，若没有则添加 `import os`（dataclasses 导入也一起加）。

```python
import os
from dataclasses import dataclass
from typing import List

from pydantic_settings import BaseSettings
```

- [ ] **Step 3: 验证配置解析逻辑**

```bash
python -c "from config.settings import get_account_configs; print(get_account_configs())"
```

预期：当前 `.env` 中无 `TELEGRAM_ACCOUNT_PHONES` 时返回单账号 `[AccountConfig(name='default', ...)]`。

---

### Task 2: ResolverPool — 新建多账号调度器

**Files:**
- Create: `services/resolver_pool.py`

- [ ] **Step 1: 创建 `services/resolver_pool.py`**

```python
"""ResolverPool — 多账号解析池，轮询调度多个 CodeResolver 实例。"""

import asyncio
from typing import List, Optional

import asyncpg
from loguru import logger
from telethon import TelegramClient

from config import settings
from config.settings import AccountConfig
from database import Storage
from services.resolver import CodeResolver
from services.upbot_uploader import UpbotUploader


class ResolverPool:
    """管理多个 Telegram 账号的解析器池，轮询分配解析任务。"""

    def __init__(self, accounts: List[AccountConfig], storage: Storage):
        self._accounts = accounts
        self._storage = storage
        self._clients: List[TelegramClient] = []
        self._resolvers: List[CodeResolver] = []
        self._upbots: List[UpbotUploader] = []
        self._index: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()
        self._db_pool: Optional[asyncpg.Pool] = None
        self._mapper = None
        self._cf_api = None
        self._running: bool = False

    # ── 初始化 ──────────────────────────────────────────

    async def init_db(self) -> bool:
        """初始化 CockroachDB 连接池和 CodeMapper。"""
        if self._db_pool:
            return True
        if not settings.COCKROACHDB_URL:
            logger.warning("[ResolverPool] COCKROACHDB_URL 未配置")
            return False
        try:
            self._db_pool = await asyncpg.create_pool(
                settings.COCKROACHDB_URL,
                min_size=1,
                max_size=3,
                statement_cache_size=0,
            )
            logger.info("[ResolverPool] CockroachDB 连接池已创建")

            from services.code_mapper import CodeMapper
            self._mapper = CodeMapper(self._db_pool)
            await self._mapper.init_tables()
            return True
        except Exception as e:
            logger.error(f"[ResolverPool] CockroachDB 连接失败: {e}")
            return False

    def _init_cf_api(self):
        """初始化 Cloudflare API（共用）。"""
        if self._cf_api is not None:
            return
        if settings.CLOUDFLARE_API_URL and settings.CLOUDFLARE_AUTH_TOKEN:
            from utils.cloudflare_api import CloudflareOverrideAPI
            self._cf_api = CloudflareOverrideAPI(
                base_url=settings.CLOUDFLARE_API_URL,
                auth_token=settings.CLOUDFLARE_AUTH_TOKEN,
            )
            logger.info("[ResolverPool] Cloudflare 覆盖规则 API 已启用")

    async def start_all(self):
        """并行启动所有账号的 TelegramClient + CodeResolver + UpbotUploader。"""
        self._init_cf_api()

        for acct in self._accounts:
            client = TelegramClient(
                acct.session_file,
                acct.api_id,
                acct.api_hash,
                device_model="CodeCrawler",
                app_version="1.0.0",
            )
            try:
                await client.start(phone=acct.phone or None)
                if not await client.is_user_authorized():
                    logger.error(
                        f"[ResolverPool] {acct.name} ({acct.phone}) 未授权，"
                        f"请先运行 login --account {acct.name}"
                    )
                    await client.disconnect()
                    continue
                me = await client.get_me()
                logger.info(f"[ResolverPool] {acct.name} 已登录: {me.first_name} (@{me.username})")

                resolver = CodeResolver(client, self._storage)
                # 注入共享资源
                resolver.set_shared_resources(
                    db_pool=self._db_pool,
                    mapper=self._mapper,
                    cf_api=self._cf_api,
                )
                resolver._register_handlers()

                upbot = UpbotUploader(client) if settings.UPLOAD_BOT_USERNAME else None

                self._clients.append(client)
                self._resolvers.append(resolver)
                self._upbots.append(upbot)

            except Exception as e:
                logger.error(f"[ResolverPool] {acct.name} 启动失败: {e}")
                await client.disconnect()

        if not self._resolvers:
            raise RuntimeError("[ResolverPool] 没有成功启动任何账号，无法继续")
        logger.info(f"[ResolverPool] 共启动 {len(self._resolvers)} 个解析器实例")

    # ── 轮询调度 ──────────────────────────────────────────

    async def _get_next_index(self) -> int:
        """轮询获取下一个可用 resolver 的索引。"""
        async with self._lock:
            idx = self._index
            self._index = (self._index + 1) % len(self._resolvers)
        return idx

    # ── 批量解析 ──────────────────────────────────────────

    async def resolve_batch(self, batch_size: int = None) -> int:
        """取一批待解析码，轮询分配给各 resolver 并行处理。"""
        if batch_size is None:
            batch_size = settings.RESOLVE_BATCH_SIZE

        codes = self._storage.get_unresolved_codes(
            limit=batch_size, max_attempts=settings.RESOLVE_MAX_RETRIES,
        )
        if not codes:
            return 0

        logger.info(f"[ResolverPool] 本轮取到 {len(codes)} 个待解析码，分配到 {len(self._resolvers)} 个账号")

        async def _resolve_with(code_row: dict) -> bool:
            idx = await self._get_next_index()
            resolver = self._resolvers[idx]
            logger.debug(
                f"[ResolverPool] {code_row['code']} → {self._accounts[idx].name}"
            )
            # 检查已有映射
            db_ok = self._db_pool is not None
            if db_ok and self._mapper:
                already = await self._mapper.has_mapping(code_row["code"])
                if already:
                    logger.info(
                        f"[ResolverPool] 文件码 {code_row['code']} 已有映射，标记为已解析"
                    )
                    self._storage.mark_resolved(code_id=code_row["id"])
                    return True

            return await resolver._resolve_one(code_row, db_ok)

        tasks = [_resolve_with(c) for c in codes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        resolved = sum(1 for r in results if r is True)
        return resolved

    async def continuous_resolve(self, batch_size: int = None):
        """持续解析模式。"""
        if batch_size is None:
            batch_size = settings.RESOLVE_BATCH_SIZE

        self._running = True
        cycle = 0

        logger.info(
            f"[ResolverPool] 启动持续解析模式，"
            f"间隔 {settings.RESOLVE_INTERVAL_SECONDS}s，"
            f"{len(self._resolvers)} 个账号"
        )

        while self._running:
            cycle += 1
            unresolved = self._storage.get_unresolved_count()
            if unresolved == 0:
                logger.debug(f"[ResolverPool] 第 {cycle} 轮: 无待解析码，等待中...")
            else:
                logger.info(
                    f"[ResolverPool] === 第 {cycle} 轮解析开始 "
                    f"(待解析: {unresolved}) ==="
                )
                resolved = await self.resolve_batch(batch_size)
                logger.info(
                    f"[ResolverPool] === 第 {cycle} 轮完成: 解析成功 {resolved} 个 ==="
                )

            if self._running:
                await asyncio.sleep(settings.RESOLVE_INTERVAL_SECONDS)

        logger.info("[ResolverPool] 持续解析已停止")

    def stop_all(self):
        """停止所有 resolver。"""
        self._running = False
        for resolver in self._resolvers:
            resolver.stop()

    async def close_all(self):
        """关闭所有连接。"""
        self._running = False
        for resolver in self._resolvers:
            await resolver.close()
        for client in self._clients:
            await client.disconnect()
        if self._db_pool:
            await self._db_pool.close()
            self._db_pool = None
        if self._cf_api:
            await self._cf_api.close()
            self._cf_api = None
        logger.info("[ResolverPool] 所有连接已关闭")
```

- [ ] **Step 2: 验证文件语法**

```bash
python -c "import services.resolver_pool; print('OK')"
```

预期：`OK`（无导入错误，仅确认语法）

---

### Task 3: CodeResolver 微调 — 支持共享资源注入

**Files:**
- Modify: `services/resolver.py`

- [ ] **Step 1: 新增 `set_shared_resources()` 方法**

在 `CodeResolver.__init__` 之后、`_init_upbot` 之前，新增方法：

```python
    def set_shared_resources(self, db_pool=None, mapper=None, cf_api=None, upbot=None):
        """由 ResolverPool 注入共享资源，避免每个实例重复创建连接。"""
        if db_pool is not None:
            self._db_pool = db_pool
        if mapper is not None:
            self._mapper = mapper
        if cf_api is not None:
            self._cf_api = cf_api
        if upbot is not None:
            self._upbot = upbot
        if settings.UPLOAD_BOT_USERNAME and self._upbot is None:
            from services.upbot_uploader import UpbotUploader
            self._upbot = UpbotUploader(self.client)
            logger.info(f"[Resolver] UpbotUploader 已就绪 (延迟初始化)")
```

> 放在 `_init_upbot` 之前（约第 44 行之后），原 `_init_upbot` 保留用于单账号模式兼容。

- [ ] **Step 2: 修改 `resolve_next_batch` — 移除资源初始化调用**

在 `services/resolver.py` 的 `resolve_next_batch` 方法中（约第 377-381 行），将：

```python
        self._register_handlers()
        self._init_cf_api()
        self._init_upbot()
        db_ok = await self._init_db_pool()
        self._init_mapper()
```

替换为：

```python
        self._register_handlers()
        # 共享资源由 ResolverPool 注入，单账号模式下仍走原有初始化
        if self._db_pool is None:
            self._init_cf_api()
            self._init_upbot()
            db_ok = await self._init_db_pool()
            self._init_mapper()
        else:
            db_ok = True
```

- [ ] **Step 3: 验证 CodeResolver 单账号模式仍然正常**

```bash
python -c "from services.resolver import CodeResolver; print('OK')"
```

预期：`OK`

---

### Task 4: 服务层导出 & CLI 入口适配

**Files:**
- Modify: `services/__init__.py`
- Modify: `run.py`

- [ ] **Step 1: 在 `services/__init__.py` 导出 `ResolverPool`**

```python
from .crawler import CodeCrawler
from .resolver import CodeResolver
from .resolver_pool import ResolverPool
from .cockroach_sync import CockroachSync
from .upbot_uploader import UpbotUploader
from .code_mapper import CodeMapper
```

- [ ] **Step 2: 新增 `cmd_login_all` — 多账号登录命令**

在 `run.py` 的 `cmd_login` 之后（约第 64 行之后）新增：

```python
async def cmd_login_all(args):
    """为所有已配置账号执行登录。"""
    configure_logging()
    from config.settings import get_account_configs

    accounts = get_account_configs()
    if args.account is not None:
        # 登录指定账号
        try:
            acct = accounts[args.account]
        except IndexError:
            logger.error(f"账号索引 {args.account} 超出范围 (共 {len(accounts)} 个)")
            return
        accounts = [acct]

    for acct in accounts:
        if not acct.api_id or not acct.api_hash:
            logger.error(f"{acct.name}: 缺少 API 凭证")
            continue

        logger.info(f"正在登录 {acct.name} ({acct.phone})...")
        client = TelegramClient(
            acct.session_file,
            acct.api_id,
            acct.api_hash,
        )
        try:
            await client.start(phone=acct.phone or None)
            me = await client.get_me()
            logger.info(f"{acct.name} 登录成功: {me.first_name} (@{me.username})")
        except Exception as e:
            logger.error(f"{acct.name} 登录失败: {e}")
        finally:
            await client.disconnect()
```

- [ ] **Step 3: 修改 `cmd_resolve` — 使用 `ResolverPool`**

将 `run.py` 中 `cmd_resolve`（约第 94-119 行）改为：

```python
async def cmd_resolve(args):
    configure_logging()
    from config.settings import get_account_configs

    accounts = get_account_configs()
    storage = Storage()
    pool = ResolverPool(accounts, storage)

    def _signal_handler():
        logger.info("收到停止信号，正在停止解析器...")
        pool.stop_all()

    try:
        await pool.init_db()
        await pool.start_all()

        if args.daemon:
            _setup_signal_handlers(_signal_handler)
            await pool.continuous_resolve(batch_size=args.batch)
        else:
            count = await pool.resolve_batch(batch_size=args.batch)
            logger.info(f"解析完成: 成功解析 {count} 个文件码")
            unresolved = storage.get_unresolved_count()
            if unresolved:
                logger.info(
                    f"仍有 {unresolved} 个文件码待解析"
                )
    finally:
        await pool.close_all()
        storage.close()
```

- [ ] **Step 4: 修改 `cmd_daemon` — 解析部分改用 `ResolverPool`**

将 `run.py` 中 `cmd_daemon`（约第 122-187 行）改为：

```python
async def cmd_daemon(args):
    configure_logging()
    from config.settings import get_account_configs

    accounts = get_account_configs()
    client = await _create_client()
    storage = Storage()
    crawler = CodeCrawler(client, storage)
    pool = ResolverPool(accounts, storage)

    global _crawler_instance
    _crawler_instance = crawler

    running = True
    await pool.init_db()
    await pool.start_all()

    def _signal_handler():
        nonlocal running
        logger.info("收到停止信号，正在停止所有任务...")
        running = False
        crawler.stop()
        pool.stop_all()

    _setup_signal_handlers(_signal_handler)
    logger.info(
        f"[Daemon] 启动全自动模式: 爬取已加入频道 → 解析（上传+映射）交替循环, "
        f"{len(accounts)} 个解析账号"
    )

    cycle = 0
    while running:
        cycle += 1
        logger.info(f"[Daemon] === 第 {cycle} 轮开始 ===")

        if settings.DAEMON_CRAWL_FIRST:
            logger.info("[Daemon] 阶段1: 爬取已加入的频道发现文件码...")
            try:
                stats = await crawler.crawl_joined_channels()
                logger.info(f"[Daemon] 爬取完成: 发现 {stats['codes_found']} 个新码")
            except Exception as e:
                logger.error(f"[Daemon] 爬取阶段失败: {e}")

        logger.info("[Daemon] 阶段2: 解析待处理文件码（上传+映射）...")
        try:
            resolved = await pool.resolve_batch(batch_size=args.resolve_batch)
            logger.info(f"[Daemon] 解析完成: 成功 {resolved} 个")
        except Exception as e:
            logger.error(f"[Daemon] 解析阶段失败: {e}")

        overall = storage.get_crawl_stats()
        resolve_st = storage.get_resolve_stats()
        logger.info(
            f"[Daemon] === 第 {cycle} 轮完成 === "
            f"总计: 频道 {overall['channels']}, "
            f"码 {overall['codes']} (已解析 {overall['resolved']}/{overall['unresolved']} 待解析), "
            f"解析历史: {resolve_st['done']} 成功 / {resolve_st['failed']} 失败"
        )

        if running and args.interval > 0:
            logger.info(f"[Daemon] 等待 {args.interval} 秒后开始下一轮...")
            for _ in range(args.interval):
                if not running:
                    break
                await asyncio.sleep(1)

    await pool.close_all()
    await client.disconnect()
    storage.close()
    logger.info("[Daemon] 全自动模式已停止")
```

- [ ] **Step 5: 在 CLI 参数解析中添加 `login-all` 子命令**

在 `run.py` 的 `main()` 函数中，`login_parser` 之后（约第 498 行）添加：

```python
    login_all_parser = subparsers.add_parser("login-all", help="为所有已配置账号登录")
    login_all_parser.add_argument(
        "--account", type=int, default=None,
        help="仅登录指定索引的账号（0-based）"
    )
```

在 `commands` 字典中（约第 554-569 行）添加：

```python
        "login-all": cmd_login_all,
```

- [ ] **Step 6: 在 `run.py` 顶部添加 `ResolverPool` 导入**

```python
from services.resolver_pool import ResolverPool
```

放在原有的 `from services.crawler import CodeCrawler` 等导入行之后。

- [ ] **Step 7: 验证 CLI 帮助输出**

```bash
python run.py --help
```

预期：能看到 `login-all` 子命令。

---

### Task 5: 端到端验证

- [ ] **Step 1: 配置多账号环境**

在 `.env` 文件中添加（使用实际手机号）：

```env
TELEGRAM_ACCOUNT_PHONES=+8613800000001,+8613800000002
```

- [ ] **Step 2: 登录所有账号**

```bash
python run.py login-all
```

预期：每个账号依次登录成功，生成 `session_account_0.session` 和 `session_account_1.session`。

- [ ] **Step 3: 测试单次解析**

```bash
python run.py resolve --batch 5
```

预期：日志显示轮询分配码到不同账号。

- [ ] **Step 4: 验证单账号回退**

清空 `TELEGRAM_ACCOUNT_PHONES` 后：

```bash
python run.py resolve --batch 5
```

预期：退化为单账号模式，行为不变。

- [ ] **Step 5: 提交**

```bash
git add config/settings.py services/resolver_pool.py services/resolver.py services/__init__.py run.py docs/superpowers/specs/2026-06-09-multi-account-resolver-design.md docs/superpowers/plans/2026-06-09-multi-account-resolver-plan.md
git commit -m "feat: add multi-account resolver support with round-robin scheduling"
```