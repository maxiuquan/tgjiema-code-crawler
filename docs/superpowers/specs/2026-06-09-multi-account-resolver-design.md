# 多账号解析支持 — 设计文档

**日期**: 2026-06-09
**状态**: 待审阅

---

## 背景与动机

当前系统的 `CodeResolver` 仅使用单个 Telegram 账号向第三方解码机器人发送文件码请求。第三方机器人通常对单个账号有严格的频率限制（如每小时 N 次、每次间隔 M 秒），导致解析吞吐量受限于单账号的限频策略。

**目标**: 接入多个 Telegram 账号，通过轮询方式分散解析请求，突破单账号限频瓶颈。

---

## 设计概览

```
                        ┌──────────────┐
                        │  SQLite (WAL) │ ← 共用 Storage
                        └──────┬───────┘
                               │
                        ┌──────┴───────────┐
                        │   ResolverPool   │
                        │   (轮询调度)       │
                        └──────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   ┌──────┴──────┐    ┌──────┴──────┐    ┌──────┴──────┐
   │ Account 0   │    │ Account 1   │    │ Account 2   │
   │             │    │             │    │             │
   │ TgClient    │    │ TgClient    │    │ TgClient    │
   │ CodeResolver│    │ CodeResolver│    │ CodeResolver│
   │ UpbotUpload.│    │ UpbotUpload.│    │ UpbotUpload.│
   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
          │                    │                    │
          └────────────────────┼────────────────────┘
                               │
                    ┌──────────┴───────────┐
                    │   CockroachDB        │ ← 共用 asyncpg Pool
                    │   CodeMapper         │
                    └──────────────────────┘
```

---

## 组件设计

### 1. 配置层 (`config/settings.py`)

在 `Settings` 中新增字段：

```python
# 多账号：逗号分隔的手机号列表，为空则退化为单账号模式
TELEGRAM_ACCOUNT_PHONES: str = ""

# 可选：单个账号的独立 API 凭证（不设则使用全局 TELEGRAM_API_ID / TELEGRAM_API_HASH）
# 格式: ACCOUNT_<N>_API_ID, ACCOUNT_<N>_API_HASH
```

同时新增 `AccountConfig` dataclass 和解析函数 `get_account_configs()`：

```python
@dataclass
class AccountConfig:
    name: str           # "account_0", "account_1" ...
    api_id: int
    api_hash: str
    phone: str
    session_file: str   # "session_account_0"
```

### 2. ResolverPool (`services/resolver_pool.py`) — 新文件

核心职责：管理多个 `(TelegramClient, CodeResolver, UpbotUploader)` 三元组，提供轮询分配。

```
ResolverPool
├── _accounts: list[AccountConfig]
├── _clients: list[TelegramClient]
├── _resolvers: list[CodeResolver]
├── _upbots: list[UpbotUploader]
├── _storage: Storage          ← 共用
├── _db_pool: asyncpg.Pool     ← 共用
├── _mapper: CodeMapper        ← 共用 (延迟初始化)
├── _index: int                ← 轮询指针
├── _lock: asyncio.Lock        ← 保护轮询指针
│
├── __init__(accounts, storage)
├── async init_db()            → 初始化 CockroachDB 连接池 + CodeMapper
├── async start_all()          → 并行登录所有账号
├── get_next_resolver()        → 轮询返回下一个可用的 resolver 实例
├── async resolve_batch(n)     → 取 N 个待解析码，轮询分配给各 resolver
├── async close_all()          → 关闭所有连接
└── stop_all()                 → 停止所有 resolver
```

**轮询逻辑**:
```python
async def get_next_resolver(self) -> tuple[CodeResolver, UpbotUploader]:
    async with self._lock:
        idx = self._index
        self._index = (self._index + 1) % len(self._resolvers)
    return self._resolvers[idx], self._upbots[idx]
```

**批量解析逻辑** (`resolve_batch`):
1. 从 `Storage` 取 `batch_size` 个待解析码
2. 遍历每个码，调用 `get_next_resolver()` 获取分配到的 resolver
3. 由该 resolver 执行 `_resolve_one()`
4. 不同 resolver 之间可以并行处理不同的码（用 `asyncio.gather`）

### 3. CodeResolver 微调 (`services/resolver.py`)

`CodeResolver` 当前已经接收 `(client, storage)` 参数，无需大改。需要调整的点：

- `_db_pool` / `_mapper` / `_cf_api` 改为由外部注入（通过 `ResolverPool` 在构造后设置），避免每个 resolver 实例各自创建重复连接：

```python
# 新增方法
def set_shared_resources(self, db_pool=None, mapper=None, cf_api=None, upbot=None):
    ...
```

- `resolve_next_batch()` 中移除 `_init_db_pool()` / `_init_mapper()` / `_init_cf_api()` / `_init_upbot()` 的调用（由 pool 统一管理）
- `_register_handlers()` 保持不变 — 每个 client 独立注册自己的事件监听

### 4. UpbotUploader 不变

`UpbotUploader` 绑定到各自的 `TelegramClient`，每个账号独立向 up_bot 发送文件和等待 idx_bot 返回系统码。无需修改。

### 5. CodeMapper 共用

`CodeMapper` 依赖 `asyncpg.Pool`，由 `ResolverPool` 统一创建一个连接池，注入到各 resolver。`set_mapping()` 和 `has_mapping()` 的写入逻辑不变。

### 6. Storage 共用

SQLite 配置为 WAL 模式，支持并发读写。多个 resolver 共享同一个 `Storage` 实例，通过 `get_unresolved_codes()` 取任务，`mark_resolved()` / `mark_resolve_failed()` 写结果。

### 7. CLI 入口 (`run.py`)

```python
async def cmd_resolve(args):
    accounts = get_account_configs()
    storage = Storage()
    pool = ResolverPool(accounts, storage)
    try:
        await pool.start_all()
        await pool.init_db()
        if args.daemon:
            await pool.continuous_resolve(args.batch)
        else:
            count = await pool.resolve_batch(args.batch)
    finally:
        await pool.close_all()
        storage.close()
```

`cmd_daemon` 同理，爬取仍用单账号（`CodeCrawler` 不变），解析部分改用 `ResolverPool`。

---

## 账号登录流程

1. 首次使用：用户运行 `python run.py login --account 0`（或 `--all`）为每个账号生成独立 session 文件
2. session 文件命名: `session_account_0.session`, `session_account_1.session` ...
3. 后续启动时 `TelegramClient(session_file, ...)` 自动加载已保存的 session，无需重复登录

---

## 数据一致性

- **去重**: 从 SQLite 取待解析码时，`WHERE is_resolved = 0 AND resolve_attempts < N` 保证不会重复取到正在处理的码
- **并发安全**: 多 resolver 之间不共享可变状态（各自独立 `_bot_exchange`、`_processed_messages`），仅共享只读的 `Storage` 和 `CodeMapper`
- **同一 bot 同时请求**: 不同账号可以同时向同一个外部解码机器人发请求，这是预期行为 — 不同账号的限频是独立的

---

## 向后兼容

- 当 `TELEGRAM_ACCOUNT_PHONES` 为空时，退化为单账号模式（行为不变）
- 现有 session 文件 `code_crawler_session.session` 继续可用（作为 account_0 的 session）
- 现有 `.env` 配置无需修改即可正常运行

---

## 不改动的部分

| 组件 | 原因 |
|---|---|
| `CodeCrawler` | 爬取仍然只需单账号 |
| `UpbotUploader` | 已是 per-client 设计 |
| `CodeMapper` | 共用 CockroachDB 连接池 |
| `Storage` / SQLite | WAL 模式已支持并发 |
| `AdminBot` | 独立 Bot Token，不涉及 |
| `code_extractor.py` | 纯工具函数 |