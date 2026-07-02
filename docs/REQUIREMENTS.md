# tgjiema-code-crawler 采集器需求文档

> 版本：v1.0
> 状态：基于现有代码审计重写，待实施
> 审查说明：所有条款均带编号（REQ-xxx / PRE-xxx），便于交叉审查逐条核验

***

## 0. 文档目的

本文档定义 `tgjiema-code-crawler` 采集器项目的功能边界、数据契约、与主系统 `tgjiema` 的对接规范、以及基于代码审计发现的问题修复要求。

**采集器核心职责**：使用独立的 Telethon user client，从已加入的 Telegram 频道发现文件码，向第三方 Bot 请求文件，通过主系统的 `up_bot`（EXTERNAL\_RELAY 协议）上传到主系统存储频道。采集器作为普通用户，全程走主系统 up_bot，不直连主系统数据库。**主系统文件码生成和第三方文件码映射全部由主系统 idx\_bot 完成**（详见主系统 `bots/idx_bot.py` L454-569：`_process_one_pending` → `generate_unique_code` 生成系统码 → `set_external_code_mapping` 写入映射）。

本文档基于对采集器项目实际代码的逐文件审计，所有引用的文件路径和行号均为真实代码位置。

***

## 1. 现有架构与工作流

### 1.1 组件构成

采集器项目包含以下组件（基于实际代码）：

| 组件            | 文件                           | 职责                                                                                                   |
| ------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------- |
| 入口            | `run.py`                     | CLI 调度，支持 login/crawl/resolve/daemon/status/export/admin-bot 子命令                                |
| 爬虫            | `services/crawler.py`        | 遍历已加入频道消息，提取文件码存入本地 SQLite                                                                           |
| 解析器           | `services/resolver.py`       | 向第三方 Bot 发码 → 收集响应文件 → 通过 up\_bot 上传 → 等待 idx\_bot 确认            |
| UpBot 上传器     | `services/upbot_uploader.py` | 通过 EXTERNAL\_RELAY 协议向主系统 up\_bot 发送文件，等待 idx\_bot "外部文件已就绪" 通知                                                         |
| 管理员 Bot       | `services/admin_bot.py`      | 独立 ptb Bot，管理覆盖规则、导出码                                                                                |
| 本地存储          | `database/session.py`        | SQLite 本地存储（channels/file\_codes/crawl\_log/resolve\_log/bot\_overrides/mapped\_codes/bot\_cooldown） |
| 码提取器          | `utils/code_extractor.py`    | 从消息文本/caption 提取文件码与 bot 用户名                                                                         |

### 1.2 实际工作流

```
┌─────────────────────────────────────────────────────────────────┐
│ tgjiema-code-crawler (独立进程, 独立 Telethon client)            │
│                                                                  │
│  1. Crawler: 遍历已加入频道消息                                   │
│     → extract_codes_from_message 提取 (code, bot_username)       │
│     → save_code 到本地 SQLite file_codes 表                      │
│                                                                  │
│  2. Resolver: 取未解析码                                         │
│     → send_message(code) 给第三方 Bot                            │
│     → 收集 media_events (NewMessage 事件)                        │
│     → 翻页循环 (_pagination_loop)                                │
│                                                                  │
│  3. UpbotUploader: 通过 EXTERNAL_RELAY 协议上传                  │
│     → send_file(entity, msg.media, caption="EXTERNAL_RELAY:...") │
│     → send_message("EXTERNAL_DONE:...") 触发批量写入             │
│     → 等待 idx_bot "外部文件 xxx 已就绪" 通知                    │
│                                                                  │
│  4. 本地标记: mark_code_mapped + mark_resolved                   │
│     → 主系统 idx_bot 已生成系统码并写入 external_code_mapping   │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ tgjiema 主系统 (独立项目)                                         │
│                                                                  │
│  up_bot: 接收 EXTERNAL_RELAY 文件 → copy 到存储频道              │
│         接收 EXTERNAL_DONE → 写 pending_uploads（带 note）       │
│  idx_bot: 处理 pending_uploads → generate_unique_code 生成系统码 │
│         → 写 file_records / codes 表                             │
│         → set_external_code_mapping 写入外部码映射（主写入方）   │
│         → 通知采集器（"外部文件 xxx 已就绪"）                    │
│  dsp_bot: 用户解码时从 external_code_mapping 查系统码            │
│         → 从 file_records 取 primary_channel_id/msg_id           │
│         → copy_message 投递给用户                                │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 与主系统的对接点

| 采集器 → 主系统 | 协议                                                               | 位置                                                       |
| --------- | ---------------------------------------------------------------- | -------------------------------------------------------- |
| 文件上传      | Telegram 消息 `caption="EXTERNAL_RELAY:{user_id}:{external_code}"` | `upbot_uploader.py` L103 → `tgjiema/bots/up_bot.py` L726 |
| 完成通知      | Telegram 消息 `"EXTERNAL_DONE:{user_id}:{external_code}"`          | `upbot_uploader.py` L117 → `tgjiema/bots/up_bot.py` L789 |
| 就绪确认      | idx_bot 发送 `"外部文件 {code} 已就绪"`   | `upbot_uploader.py` L57-63 → `tgjiema/bots/idx_bot.py` L574 |
| 映射写入      | idx_bot 处理 pending_uploads 时调用 `set_external_code_mapping`        | `tgjiema/bots/idx_bot.py` L565-566 → `tgjiema/database/session.py` L1223 |

***

## 2. 前置依赖（主系统必须先修复的审查问题）

以下问题来自对主系统 `tgjiema` 的代码审计，**必须在采集器大规模使用前修复**，否则采集的文件将出现"降级后取不到""缓存不失效""绕过权限上传"等问题：

| 编号         | 主系统问题                                                                        | 修复要求                                                                             | 核验方式                  |
| ---------- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | --------------------- |
| PRE-01     | `demoted_ch_to_promoted_ch` 映射未持久化（主系统 mon\_bot L402 局部变量）                   | 持久化到 SQLite，delivery\_resolver 解析时读取                                             | 代码 review + 降级回归测试    |
| PRE-02     | delivery\_resolver 缓存 120 秒未失效（`invalidate_cell_cache` 无人调用）                 | mon\_bot 轮转后通过 `cells_change_notify` 触发 dsp\_bot 失效缓存                            | 代码 review             |
| PRE-03     | `invalidate_file_record` 缓存键前缀错误（`file_record:` vs 实际 `file:`）               | 主系统 `cache.py` L123 改为 `f"file:{file_code}"`                                     | 代码 review             |
| PRE-04     | `cells_local` 表缺 `prev_slot_id` 字段                                           | 补全 DDL 与 CRUD                                                                    | schema 对比             |
| PRE-05     | `user_relay.py` L1167 仍用 `find_one` 热读                                       | 替换为 `get_file_record_cached(code)`                                               | grep 确认               |
| PRE-06     | `report:detach` / `report:block` 未失效缓存                                       | admin\_bot/callback.py update\_one 后调用 `invalidate_file_record`                  | 代码 review             |
| PRE-07     | `db_backup` 默认启用且间隔 10 天                                                     | settings 改为 `DB_BACKUP_ENABLED=False`、`DB_BACKUP_INTERVAL_MINUTES=360`           | 配置核验                  |
| PRE-08     | `RELAY_ENCRYPTION_KEY` 未做必填校验                                                | `validate_required_fields` 增加校验 + Fernet key 格式校验                                | 启动缺 key 应报错           |
| PRE-09     | `MAIN_STORAGE_CHANNEL_ID` 默认占位无效                                             | 默认值改 0，启动校验非 0                                                                   | 配置核验                  |
| PRE-10     | cryptography 缺失/密钥无效时静默降级明文                                                  | `relay_db.py` 改为 `raise RuntimeError(...)`                                       | 移除 cryptography 后启动失败 |
| **PRE-11** | **up\_bot L726-735 EXTERNAL\_RELAY 路径绕过限速和权限检查**（最关键）                        | `_handle_external_relay_file` 入口校验消息来源是否为已注册中继账号（白名单），并校验 `external_user_id` 合法性 | 代码 review + 越权测试      |
| **PRE-12** | **up\_bot** **`_pending_media_groups`** **缺** **`created_at`** **字段**（审查 D1） | 创建条目时添加 `"created_at": time.time()`                                              | 代码 review             |
| **PRE-13** | **up\_bot** **`end_upload`** **flush 全局所有用户的 media group**（审查 D2）            | 仅 flush 当前用户的 media group                                                        | 代码 review             |
| **PRE-14** | **up\_bot** **`_finalize_upload`** **在状态缺失时写无效数据**（primary\_channel\_id=0）   | 插入前校验 main\_channel/channel\_msg\_id 非零                                          | 代码 review             |
| **PRE-15** | **up\_bot 外部中继文件 insert 缺 protect\_content/file\_ttl\_days 字段**              | \_flush\_external\_buffer 的 insert 补全字段（L850-862）                                 | 代码 review             |

> **PRE-11 \~ PRE-15 是采集器直接依赖的入口**，不修复会导致：任意用户可伪造 EXTERNAL\_RELAY 上传绕过权限、媒体组丢失、外部文件字段不一致。

***

## 3. 采集器核心约束

### 3.1 file\_id 跨账号问题（最关键）

Telegram 的 `file_id` 绑定到首次接收该文件的具体账号。采集器账号 A 从第三方 Bot 收到的 file\_id，**不能直接传给主系统 dsp\_bot 账号 B 投递**（会返回 `BAD_REQUEST: invalid file_id`）。

当前采集器通过 `upbot_uploader.py` L125 `send_file(entity, msg.media, caption=...)` 上传——这里传的是 `msg.media`（FileLocation），Telethon 会**重新上传文件**而非传 file\_id，因此**当前实现规避了跨账号 file\_id 问题**。

- **REQ-T01**：采集器**必须**通过 `send_file(entity, msg.media, ...)` 传递 `msg.media` 对象（FileLocation），禁止仅传 `file_id` 字符串。
- **REQ-T02**：主系统 up\_bot 接收 EXTERNAL\_RELAY 后，通过 `safe_copy_message` 复制到存储频道（已实现，up\_bot L756），生成的 `primary_channel_msg_id` 是主系统存储频道中的消息 ID，dsp\_bot 投递时用此 ID `copy_message`，**与采集器账号无关**。
- **REQ-T03**：file\_records 的 `file_ids` 字段在采集器路径下**不应被使用**（采集器不写 file\_ids），主系统 idx\_bot 处理 pending\_uploads 时应基于 `primary_channel_id + primary_channel_msg_id`。

> 审查交叉核验点：采集器上传的文件，主系统 dsp\_bot 投递时不依赖采集器账号的 file\_id，仅依赖主系统存储频道的消息 ID。

### 3.2 媒体组完整性

- **REQ-T04**：第三方 Bot 以 media\_group 形式发送的多文件，采集器 resolver 必须按 `grouped_id` 聚合所有消息后再通过 upbot\_uploader 上传（当前 `_media_buffers` + `_create_flush_task` 已实现，L141-147）。
- **REQ-T05**：媒体组聚合缓冲条目必须有过期时间 `_expires`（已实现，L144），禁止无过期清理导致内存泄漏。
- **REQ-T06**：媒体组 flush 后，所有文件必须**带相同的** **`EXTERNAL_RELAY:{user_id}:{external_code}`** **caption** 逐个上传到 up\_bot，再发 `EXTERNAL_DONE` 触发主系统批量写入。当前实现 L120-128 已正确。
- **REQ-T07**：单文件之间发送间隔 ≥ 0.8 秒（已实现 L127），避免触发 up\_bot 的 FloodWait。

### 3.3 文件码格式

- **REQ-T08**：采集器不生成系统码（系统码由主系统 idx\_bot 生成），仅生成 `external_code`（即第三方 Bot 的原始码，如 `BotName_bot:code1234`）。
- **REQ-T09**：`external_code` 格式由 `utils/code_extractor.py` 提取，当前格式为 `{bot_username}:{code_part}`（L119 等处 `normalized = f"{bot}:{code}"`）。禁止修改此格式，否则主系统 `external_code_mapping` 表的主键语义会变。
- **REQ-T10**：`external_code` 必须全局唯一。主系统 idx\_bot 的 `set_external_code_mapping` 处理冲突（UPSERT）。同一 `external_code` 多次采集，主系统 idx\_bot 应返回相同的 system\_code 或跳过重复上传。

### 3.4 路由配置

- **REQ-T11**：Bot 路由规则由本地 SQLite `bot_overrides` 表管理（`database/session.py` L655-724），通过 CLI `override-add/remove/toggle/list` 命令或 admin_bot 管理。采集器不写入主系统 `code_bot_mapping` 表。

### 3.5 跨进程一致性

- **REQ-T12**：主系统 idx\_bot 写入 `external_code_mapping` 后，其内存缓存 `_external_code_mapping_cache`（TTL 60 秒，主系统 session.py L1176-1178）最长 60 秒内刷新。采集器通过本地 `mapped_codes` 表（SQLite）缓存已知映射，避免重复上传。
  - **可接受窗口**：用户解码第三方码后，最长 60 秒延迟可被接受。

***

## 4. 数据模型

### 4.1 采集器本地 SQLite（codes.db）

#### 4.1.1 file\_codes 表

当前 schema（database/session.py L70-90）：

| 字段                       | 类型                       | 说明                                       |
| ------------------------ | ------------------------ | ---------------------------------------- |
| id                       | INTEGER PK AUTOINCREMENT | <br />                                   |
| code                     | TEXT NOT NULL UNIQUE     | external\_code（如 `BotName_bot:code1234`） |
| bot\_username            | TEXT NOT NULL            | 第三方 Bot 用户名                              |
| source\_channel\_id      | INTEGER                  | 发现该码的频道 ID                               |
| source\_channel\_title   | TEXT                     | <br />                                   |
| source\_message\_id      | INTEGER                  | 发现该码的消息 ID                               |
| discovered\_at           | TEXT                     | ISO 时间戳                                  |
| file\_type               | TEXT                     | 消息中的文件 MIME 类型                           |
| file\_name               | TEXT                     | <br />                                   |
| file\_size               | INTEGER                  | <br />                                   |
| is\_verified             | INTEGER DEFAULT 0        | 是否已验证（解析成功）                              |
| is\_exported             | INTEGER DEFAULT 0        | 是否已导出                                    |
| is\_resolved             | INTEGER DEFAULT 0        | 是否已解析                                    |
| resolve\_attempts        | INTEGER DEFAULT 0        | 解析重试次数                                   |
| resolve\_error           | TEXT                     | 最近错误                                     |
| storage\_channel\_id     | INTEGER DEFAULT 0        | **已废弃**（采集器不直接写主系统存储频道，走 up\_bot）        |
| storage\_msg\_id         | INTEGER DEFAULT 0        | **已废弃**                                  |
| storage\_batch\_msg\_ids | TEXT                     | **已废弃**                                  |
| is\_crdb\_synced         | INTEGER DEFAULT 0        | 是否已同步到 CRDB（cockroach\_sync 用）           |

- **REQ-D01**：`storage_channel_id` / `storage_msg_id` / `storage_batch_msg_ids` 三个字段在当前架构下**已无意义**（采集器通过 up\_bot 上传，不直接接触主系统存储频道）。建议保留字段但标注废弃，避免破坏旧数据。
- **REQ-D02**：`code` 字段的 UNIQUE 约束保证不重复采集（crawler.py L168 `code_exists` + L272 `INSERT OR IGNORE` 已正确）。

#### 4.1.2 mapped\_codes 表

- **REQ-D03**：`mapped_codes` 表（L125-128）是本地缓存，记录 external\_code 是否已写入 CRDB 的 external\_code\_mapping。resolver.py L470 `is_code_mapped` 命中时跳过，避免重复查 CRDB。

#### 4.1.3 bot\_overrides 表

- **REQ-D04**：覆盖规则 `bot_overrides (code_prefix, override_bot_username, is_active)` 用于强制指定某前缀的码用特定 Bot 解析（如原 Bot 失效时）。`get_bot_override` 用 `? LIKE (code_prefix || '%')` 最长前缀匹配（L717-720）已正确。

#### 4.1.4 bot\_cooldown 表

- **REQ-D05**：`bot_cooldown` 记录第三方 Bot 的限速冷却。resolver.py L525 `get_bot_cooldown` + L717 `set_bot_cooldown` 已正确。`cleanup_cooldowns` 定期清理过期记录（L763-776）。

### 4.2 主系统数据（由主系统 idx_bot 写入）

采集器不直连主系统数据库。以下数据全部由主系统 idx_bot 写入，采集器不关心。

#### 4.2.1 external\_code\_mapping 表

**唯一写入方**：主系统 idx\_bot（`tgjiema/bots/idx_bot.py` L565-566），在 `_process_one_pending` 中处理 pending\_uploads 时，解析 `note` 字段中的 `{"type": "external", "code": "..."}` ，调用 `set_external_code_mapping(ext_code, file_code)` 写入。

| 字段             | 类型            | 要求                             |
| -------------- | ------------- | ------------------------------ |
| external\_code | TEXT PK       | 第三方码（如 `BotName_bot:code1234`） |
| system\_code   | TEXT NOT NULL | 主系统生成的系统码（`tgwenjian_xxx`）     |
| bot\_username  | TEXT          | 第三方 Bot 用户名                    |
| created\_at    | TEXT          | ISO 时间戳                        |
| updated\_at    | TEXT          | ISO 时间戳                        |

- **REQ-D06**：`updated_at` 必填（主系统增量同步依赖，缺失会导致主系统 Mon Bot 崩溃）。
- **REQ-D07**：主系统 idx\_bot 的 `set_external_code_mapping`（`tgjiema/database/session.py` L1223-1249）已正确处理重复写入（UPSERT）。

#### 4.2.2 file\_records 表

- **REQ-D08**：采集器**禁止**直接向主系统 file\_records 表写入数据。所有 file\_records 必须由主系统 idx\_bot 处理 pending\_uploads 后生成。

***

## 5. 接口契约

### 5.1 采集器 → 主系统 up\_bot（EXTERNAL\_RELAY 协议）

#### 5.1.1 文件上传

- **REQ-I01**：采集器通过 `client.send_file(up_bot_entity, msg.media, caption=caption)` 上传（upbot\_uploader.py L125）。
- **REQ-I02**：caption 格式必须为 `EXTERNAL_RELAY:{user_id}:{external_code}`，其中 `user_id` 是采集器账号的 Telegram user\_id（`my_id`，L103）。
- **REQ-I03**：主系统 up\_bot `_handle_external_relay_file`（L726-786）解析 caption 提取 `external_user_id` 和 `external_code`，将文件 copy 到存储频道，积累到 `_external_buffers[external_code]`。

#### 5.1.2 完成通知

- **REQ-I04**：所有文件上传完成后，采集器发送 `EXTERNAL_DONE:{user_id}:{external_code}` 文本消息（upbot\_uploader.py L139）。
- **REQ-I05**：主系统 up\_bot `_handle_external_done`（L789-809）接收后调用 `_flush_external_buffer`（L812-871）将缓冲区数据写入 `pending_uploads` 表。

#### 5.1.3 就绪确认

- **REQ-I06**：主系统 idx\_bot 处理 `pending_uploads` 后，向采集器账号发送 `"外部文件 {ext_code} 已就绪，请重新发送文件码即可查收。"` 通知（`tgjiema/bots/idx_bot.py` L574）。采集器通过消息监听捕获此通知（`upbot_uploader.py` L57-63），确认上传成功。
- **REQ-I07**：等待超时为 120 秒（`upbot_uploader.py` L133）。超时后标记解析失败，可重试。

### 5.2 采集器本地命令（CLI）

当前 `run.py` 已实现的子命令（无需修改）：

| 命令                                | 功能          | 状态               |
| --------------------------------- | ----------- | ---------------- |
| `login`                           | 登录 Telegram | ✓                |
| `crawl [--daemon]`                | 爬取文件码       | ✓                |
| `resolve [--daemon] [--batch N]`  | 解析文件码       | ✓                |
| `daemon [--interval N]`           | 全自动模式       | ✓                |
| `status`                          | 查看状态        | ✓                |
| `export --format json/csv/txt`    | 导出          | ✓                |
| `import <file>`                   | 导入          | ✓                |
| `channels [--all]`                | 查看频道        | ✓                |
| `sync`                            | 已移除        | 采集器不再直连 CRDB |
| `retry-failed`                    | 重置失败码       | ✓                |
| `override-add/remove/toggle/list` | 覆盖规则管理      | ✓                |
| `admin-bot`                       | 启动管理员 Bot   | ✓                |

### 5.3 管理员 Bot 命令

当前 `services/admin_bot.py` 已实现（基于 python-telegram-bot）：

| 命令                                    | 功能     | 状态 |
| ------------------------------------- | ------ | -- |
| `/add_override <prefix> <bot> [note]` | 添加覆盖规则 | ✓  |
| `/remove_override <prefix>`           | 删除覆盖规则 | ✓  |
| `/toggle_override <prefix>`           | 启用/禁用  | ✓  |
| `/list_overrides`                     | 列出覆盖规则 | ✓  |
| `/export_codes`                       | 导出 TXT | ✓  |

- **REQ-A01**：管理员 Bot 仅响应 `settings.ADMIN_USER_IDS` 中的用户（L74-79 已校验）。
- **REQ-A02**：管理员 Bot 与采集器共享同一个 SQLite（L45 `Storage()`），不独立部署数据库。

***

## 6. 配置项

当前 `config/settings.py` 已定义的配置项：

| 配置项                               | 默认值         | 说明               | 需修改              |
| --------------------------------- | ----------- | ---------------- | ---------------- |
| `TELEGRAM_API_ID`                 | 0           | 采集器账号 API\_ID    | 必填校验已实现          |
| `TELEGRAM_API_HASH`               | ""          | 采集器账号 API\_HASH  | 必填校验已实现          |
| `TELEGRAM_PHONE`                  | ""          | 采集器账号手机号         | <br />           |
| `CRAWL_INTERVAL_MINUTES`          | 30          | 爬取间隔             | <br />           |
| `CRAWL_MESSAGE_LIMIT_PER_CHANNEL` | 200         | 每频道扫描上限          | <br />           |
| `MAX_CONCURRENT_CRAWLS`           | 3           | 并发爬取数            | <br />           |
| `RESOLVE_TIMEOUT_SECONDS`         | 30          | 单码响应超时           | <br />           |
| `RESOLVE_BATCH_SIZE`              | 5           | 每批解析数            | <br />           |
| `RESOLVE_INTERVAL_SECONDS`        | 10          | 解析轮询间隔           | <br />           |
| `RESOLVE_MAX_RETRIES`             | 3           | 最大重试次数           | <br />           |
| `RESOLVE_DELAY_BETWEEN_CODES`     | 3.0         | 码间延迟             | <br />           |
| `RESOLVE_PAGINATION_TIMEOUT`      | 600         | 翻页总超时            | <br />           |
| `DAEMON_CRAWL_FIRST`              | True        | 先爬后解析            | <br />           |
| `DAEMON_CYCLE_INTERVAL`           | 60          | 守护轮询间隔           | <br />           |
| `UPLOAD_BOT_USERNAME`             | ""          | 主系统 up\_bot 用户名  | **必填**（当前未校验）    |
| `DECODER_BOT_USERNAME`            | ""          | 主系统 idx\_bot 用户名 | **必填**（当前未校验）    |
| `SENDER_BOT_USERNAME`             | ""          | 主系统 dsp\_bot 用户名 | 当前未使用            |
| `FILE_CODE_PREFIX`                | "tgwenjian" | 系统码前缀            | 必须与主系统一致         |
| `SQLITE_DB_PATH`                  | "codes.db"  | 本地 SQLite 路径     | <br />           |
| `EXPORT_DIR`                      | "exports"   | 导出目录             | <br />           |
| `LOG_LEVEL`                       | "INFO"      | 日志级别             | <br />           |
| `TELEGRAM_BOT_TOKEN`              | ""          | 管理员 Bot Token    | 启用 admin-bot 时必填 |
| `ADMIN_USER_IDS`                  | \[]         | 管理员用户 ID 列表      | 启用 admin-bot 时必填 |
| `ADMIN_BOT_ENABLED`               | False       | 管理员 Bot 开关       | <br />           |
| `TARGET_FILE_EXTENSIONS`          | \[...]      | 目标文件扩展名          | 当前未使用            |

- **REQ-C01**：`validate_required_fields`（settings.py L70-80）当前只校验 `TELEGRAM_API_ID` 和 `TELEGRAM_API_HASH`，**遗漏**：
  - `UPLOAD_BOT_USERNAME`：启用解析时必填
  - `DECODER_BOT_USERNAME`：启用解析时必填（监听 idx_bot 就绪通知）
  - `FILE_CODE_PREFIX`：非空校验
- **REQ-C02**：校验逻辑应为"按需必填"——仅当对应功能启用时才报错。例如 daemon 模式要求所有字段都配齐，仅 crawl 模式则不需要 UPLOAD\_BOT\_USERNAME。

***

## 7. 错误处理与重试

### 7.1 当前已实现

- **REQ-E01**：单码解析失败重试 `RESOLVE_MAX_RETRIES` 次（resolver.py L411-412），超过后 `mark_resolve_failed`（L567 等）。
- **REQ-E02**：失败码可通过 `retry-failed` 命令重置（database/session.py L644-651）。
- **REQ-E03**：FloodWait 处理（resolver.py L109-111 爬虫、L390-394 翻页、L490-493 发消息、L536-538 发送码）均已实现等待重试。
- **REQ-E04**：第三方 Bot 限速文本检测（resolver.py L634-676 `_check_rate_limit`）已实现，可解析中文/英文限速提示。
- **REQ-E05**：冷却期管理（bot\_cooldown 表）已实现。

### 7.2 需改进

- **REQ-E06**：`upbot_uploader.upload_files` 等待系统码超时 120 秒后返回 None（L204），resolver L615-617 标记失败。但**未记录失败原因到 resolve\_log**——L617 `mark_resolve_failed(code_id, "no_system_code_from_upbot")` 已记录，OK。
- **REQ-E07**：`upbot_uploader._poll_db_mapping` 每次创建新连接（L218 `asyncpg.connect`），**未复用连接池**，高频轮询会泄漏连接。
  - **修复要求**：复用 resolver 的 `_db_pool`（resolver.py L70），通过依赖注入传给 upbot\_uploader。
- **REQ-E08**：采集器进程崩溃重启后，未完成的 `_bot_exchange` 会丢失，但本地 SQLite 的 `file_codes.is_resolved = 0` 状态保留，可继续解析（已正确）。
- **REQ-E09**：`_bot_exchange` 的 `_expires` 字段（L510 `now_ts + 300`）控制交换生命周期，超时后 `on_new_message` L110-115 清理。但**仅在被新消息触发时才清理**，无主动清理任务。若长期无新消息，过期条目会驻留。
  - **修复要求**：增加后台定期清理任务，或在 `_resolve_one` 结束时确保 `_cleanup_exchange` 被调用（L611, L619 已调用，OK）。
- **REQ-E10**：`_media_buffers` 的 flush task（L192-223）在 `_flush` 内 `pop(gid_str)`（L204），若 flush 期间有新消息到达会丢失。当前用 `_expires > now_ts` 延长窗口（L199-203）缓解，但仍有边界情况。
  - **修复要求**：flush 时若 buf 的 `_expires` 仍大于当前时间，重新调度 flush task 而非立即 pop。

***

## 8. 安全要求

- **REQ-S01**：采集器账号的 `TELEGRAM_API_HASH` 存储在 `.env` 明文文件中，**未加密**。这是 Telethon 标准做法（user API 凭据无法像 Bot Token 那样加密），但要求：
  - `.env` 文件权限必须为 600（仅 owner 可读）
  - `.env` 必须在 `.gitignore` 中（已确认）
- **REQ-S02**：采集器日志禁止输出完整 `TELEGRAM_API_HASH`。当前 `run.py` L66 仅打印"未配置"提示，未输出值，OK。
- **REQ-S03**：采集器账号**禁止**加入任何非必要频道（仅加入需要爬取的频道）。
- **REQ-S04**：管理员 Bot 仅响应 `ADMIN_USER_IDS`（已实现）。
- **REQ-S05**：`external_user_id`（upbot\_uploader L118 caption 中的 `my_id`）是采集器账号的 user\_id，主系统 up\_bot L694 解析后用作 `pending_uploads.uploader_id`。**这意味着采集器上传的文件在主系统中归属于采集器账号 user\_id**，而非真实用户。
  - **影响**：主系统 `permission.check_decode_permission` 对此类文件的配额扣减会落在采集器账号上，而非解码用户。
  - **修复要求**：主系统 idx\_bot 处理 EXTERNAL\_RELAY 来源的 pending\_uploads 时，应将 `uploader_id` 设为 `0`（系统公共库），解码时跳过个人配额（主系统 permission.py 需修改，见 PRE-11 关联）。

***

## 9. 监控与可观测性

### 9.1 当前已实现

- **REQ-M01**：`run.py status` 命令展示频道数、码数、已解析/待解析/已导出、按 Bot 统计 Top 10（L206-232）。
- **REQ-M02**：`run.py channels` 命令展示频道列表（L273-291）。
- **REQ-M03**：loguru 日志写入文件 `crawler_{time:YYYY-MM-DD}.log`，1 天轮转，保留 7 天（run.py L49-55）。

### 9.2 需改进

- **REQ-M04**：采集器**未向主系统** **`bot_heartbeat`** **表写心跳**（主系统有此表供 admin\_bot 监控各 Bot 状态）。采集器是独立项目，不共享 SQLite，因此**不强制要求**。
- **REQ-M05**：采集器进程异常退出时，systemd 自动重启（需配置 `tgjiema-collector.service` 单元，参照主系统 deploy\_vps\_per\_bot.sh）。
- **REQ-M06**：采集器应有独立的 systemd 服务单元，与主系统 5 个 bot 平级。

***

## 10. 部署

### 10.1 当前部署方式

- **REQ-D01**：采集器作为独立进程运行，`python run.py daemon` 启动全自动模式。
- **REQ-D02**：session 文件 `code_crawler_session.session`（run.py L70, L427）持久化，重启后自动恢复登录。

### 10.2 需补充

- **REQ-D03**：需要 systemd 服务单元文件 `tgjiema-collector.service`，参照主系统模板：
  ```ini
  [Unit]
  Description=TGjiema Code Crawler
  After=network.target

  [Service]
  Type=simple
  WorkingDirectory=/path/to/tgjiema-code-crawler
  ExecStart=/path/to/python run.py daemon --interval 60
  Restart=always
  RestartSec=3
  StartLimitBurst=3

  [Install]
  WantedBy=multi-user.target
  ```
- **REQ-D04**：采集器部署在**与主系统不同的 VPS**或不同 IP 上，避免采集账号被封禁时连累主系统中继账号（采集账号向第三方 Bot 高频发消息，更易触发风控）。
- **REQ-D05**：采集器与主系统通过 Telegram EXTERNAL_RELAY 协议通信，不共享数据库。采集器拥有独立本地 SQLite（codes.db）。

***

## 11. 验收标准

实施完成后，审查方按以下标准逐项核验：

### 11.1 前置依赖（PRE-01 \~ PRE-15）

- [ ] 主系统 15 项前置问题均已修复，代码 review 通过
- [ ] 降级回归测试：模拟主系统 Active 频道降级，验证采集的文件仍可通过 Shadow 频道投递
- [ ] 越权测试：非授权账号向 up\_bot 发送 `EXTERNAL_RELAY:...` 应被拒绝（PRE-11）

### 11.2 采集器功能验收

- [ ] `python run.py login` 能正常登录采集器账号
- [ ] `python run.py crawl` 能从已加入频道发现文件码并存入本地 SQLite
- [ ] `python run.py resolve` 能向第三方 Bot 发码、收集文件、通过 up\_bot 上传、获取系统码、写映射
- [ ] `python run.py daemon` 全自动模式能持续运行
- [ ] 媒体组完整上传（多文件不丢失）
- [ ] 翻页正确收集所有页的文件
- [ ] `python run.py admin-bot` 管理员 Bot 命令全部可用

### 11.3 跨系统一致性验收

- [ ] 采集器上传文件后，主系统 idx\_bot 能处理 pending\_uploads 生成系统码
- [ ] 采集器写入 external\_code\_mapping 后，主系统 idx\_bot 解码时能查到映射
- [ ] 用户通过主系统 idx\_bot 解码第三方码，dsp\_bot 能正确投递文件（不依赖采集器账号 file\_id）
- [ ] 主系统 file\_records 表中**不应出现**采集器直接写入的 `file_code = external_code` 记录（REQ-D10 废弃 cockroach\_sync 后）

### 11.4 安全验收

- [ ] 采集器账号 API\_HASH 不出现在日志中
- [ ] 管理员 Bot 仅响应授权用户
- [ ] 非授权账号向 up\_bot 发送 EXTERNAL\_RELAY 被拒绝（PRE-11）

### 11.5 反回归验收

- [ ] 主系统现有 5 个 bot 功能不受影响
- [ ] 采集器现有功能（crawl/resolve/daemon/admin-bot）不受修复影响

***

## 12. 风险与未决问题

### 12.1 已知风险

- **R1**：第三方 Bot 可能随时更改协议或封禁采集账号。缓解：失败重试 + 覆盖规则 + 冷却期。
- **R2**：采集器账号向第三方 Bot 高频发消息易触发 Telegram 风控。缓解：`RESOLVE_DELAY_BETWEEN_CODES=3.0` 秒间隔 + FloodWait 退避。
- **R3**：采集器不直连主系统数据库，通过 up_bot 上传文件。主系统 idx_bot 负责写入映射。采集器本地 `mapped_codes` 缓存防止重复上传。

### 12.2 待决策问题

- **Q1**：采集器是否需要支持多账号轮换？当前单账号，高频采集易触发风控。需业务评估。
- **Q2**：`cockroach_sync.py` 已删除。采集器不再直连 CRDB，所有文件通过 up_bot → idx_bot 路径处理。
- **Q3**：采集器是否需要向主系统 `pending_notify` 表写通知，加速 idx\_bot 缓存失效？当前依赖 60 秒 TTL，可接受。如需加速需采集器访问主系统 SQLite，增加耦合。
- **Q4**：主系统 up\_bot 的 `external_user_id`（采集器账号 user\_id）是否应统一改为 `0`？REQ-S05 建议改为 0，需主系统 idx\_bot 配合修改。

***

## 13. 变更记录

| 版本   | 日期         | 变更              |
| ---- | ---------- | --------------- |
| v1.0 | 2026-07-02 | 基于采集器项目实际代码审计重写 |

***

## 附录 A：采集器代码审计发现的问题

基于对采集器项目代码的逐文件审计，发现以下问题需修复：

| 编号    | 文件                           | 行号       | 问题                                                                                                                  | 严重等级 |
| ----- | ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------- | ---- |
| CA-01 | `services/code_mapper.py`    | --       | **已解决**：文件已删除，采集器不再直连 CRDB                                                              | 高    |
| CA-02 | `services/cockroach_sync.py` | --       | **已解决**：文件已删除，采集器不再直连 CRDB                                                              | 高    |
| CA-03 | `services/upbot_uploader.py` | --       | **已解决**：`_poll_db_mapping` 已移除，采集器仅靠消息监听确认就绪                                                              | 中    |
| CA-04 | `services/resolver.py`       | L192-223 | `_media_buffers` flush task 在 pop 后若有新消息到达会丢失（REQ-E10）                                                              | 中    |
| CA-05 | `config/settings.py`         | L70-80   | `validate_required_fields` 遗漏 UPLOAD\_BOT\_USERNAME/DECODER\_BOT\_USERNAME 校验（REQ-C01）             | 中    |
| CA-06 | `services/admin_bot.py`      | L62      | `run_polling()` 是阻塞调用，但函数声明为 `async`，可能导致事件循环冲突                                                                     | 低    |
| CA-07 | `services/resolver.py`       | L462-464 | 同一 bot\_username 正在被处理时跳过，但若前一次处理失败未清理 `_bot_exchange`，会导致该 bot 的所有码都被跳过                                            | 中    |
| CA-08 | `database/session.py`        | L42-53   | `Storage._conn` 使用 `threading.local()`，但 Telethon 是 asyncio 单线程，`threading.local` 在 async 上下文下行为正常但语义混乱，建议改为模块级单例连接 | 低    |
| CA-09 | `services/crawler.py`        | L128-129 | `_processed_messages` 清理用 `set(list(...)[-50000:])` 是切片而非按时间清理，可能保留旧数据丢弃新数据                                         | 低    |
| CA-10 | `services/upbot_uploader.py` | L65-67   | 收到系统码后 `for ev in list(self._pending.values()): if not ev.is_set(): ev.set()` 会唤醒所有等待的 external\_code，导致并发解析时串号     | 中    |

***

## 附录 B：与主系统审查报告的对应关系

| 文档引用   | 主系统审查问题       | 严重等级 |
| ------ | ------------- | ---- |
| PRE-01 | 存储投递层 #1（A1）  | 高    |
| PRE-02 | 存储投递层 #3（A2）  | 高    |
| PRE-03 | 数据库层 1.1（C1）  | 高    |
| PRE-04 | 数据库层 1.3（V5）  | 高    |
| PRE-05 | 服务层 H4（V3）    | 高    |
| PRE-06 | Bot 层 H8（C2）  | 高    |
| PRE-07 | 服务层 M1（V1/V2） | 中    |
| PRE-08 | 存储投递层 #37（S3） | 高    |
| PRE-09 | 存储投递层 #38（V4） | 高    |
| PRE-10 | 服务层 H7（S2）    | 高    |
| PRE-11 | Bot 层 H1（S1）  | 高    |
| PRE-12 | Bot 层 H3（D1）  | 高    |
| PRE-13 | Bot 层 H2（D2）  | 高    |
| PRE-14 | Bot 层 M2      | 中    |
| PRE-15 | Bot 层 M3      | 中    |

> 审查方在核验本需求文档时，应同时核验主系统上述审查问题是否已修复。

