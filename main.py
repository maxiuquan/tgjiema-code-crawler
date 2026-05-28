import argparse
import asyncio
import os
import signal
import sys

from loguru import logger
from telethon import TelegramClient

from config import settings
from crawler import CodeCrawler
from storage import Storage
from resolver import CodeResolver
from cockroach_sync import CockroachSync

logging_configured = False


def configure_logging():
    global logging_configured
    if logging_configured:
        return
    logger.remove()
    log_level = settings.LOG_LEVEL.upper()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> | {message}",
        level=log_level,
        colorize=True,
    )
    logger.add(
        "crawler_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name} | {message}",
        level="DEBUG",
    )
    logging_configured = True


_crawler_instance = None
_resolver_instance = None


async def cmd_login(args):
    configure_logging()
    if not settings.TELEGRAM_API_ID or not settings.TELEGRAM_API_HASH:
        logger.error("请设置 TELEGRAM_API_ID 和 TELEGRAM_API_HASH 环境变量")
        return

    client = TelegramClient(
        "code_crawler_session",
        settings.TELEGRAM_API_ID,
        settings.TELEGRAM_API_HASH,
    )
    await client.start(phone=settings.TELEGRAM_PHONE or None)
    me = await client.get_me()
    logger.info(f"登录成功: {me.first_name} (@{me.username})")
    await client.disconnect()


async def cmd_crawl(args):
    configure_logging()
    client = await _create_client()
    storage = Storage()
    crawler = CodeCrawler(client, storage)
    global _crawler_instance
    _crawler_instance = crawler

    def _signal_handler():
        logger.info("收到停止信号，正在停止爬虫...")
        crawler.stop()

    try:
        if args.daemon:
            _setup_signal_handlers(_signal_handler)
            await crawler.continuous_crawl()
        else:
            stats = await crawler.discover_and_crawl()
            logger.info(f"爬取完成: {stats}")
            unresolved = storage.get_unresolved_count()
            if unresolved:
                logger.info(f"有 {unresolved} 个文件码待解析，可运行 'python main.py resolve' 进行解析")
    finally:
        await client.disconnect()
        storage.close()


async def cmd_resolve(args):
    configure_logging()
    client = await _create_client()
    storage = Storage()
    resolver = CodeResolver(client, storage)
    global _resolver_instance
    _resolver_instance = resolver

    def _signal_handler():
        logger.info("收到停止信号，正在停止解析器...")
        resolver.stop()

    try:
        if args.daemon:
            _setup_signal_handlers(_signal_handler)
            await resolver.continuous_resolve()
        else:
            count = await resolver.resolve_next_batch(batch_size=args.batch)
            logger.info(f"解析完成: 成功解析 {count} 个文件码")
            unresolved = storage.get_unresolved_count()
            if unresolved:
                logger.info(f"仍有 {unresolved} 个文件码待解析")
    finally:
        await resolver.close()
        await client.disconnect()
        storage.close()


async def cmd_daemon(args):
    configure_logging()
    client = await _create_client()
    storage = Storage()
    crawler = CodeCrawler(client, storage)
    resolver = CodeResolver(client, storage)
    syncer = CockroachSync(storage)

    global _crawler_instance, _resolver_instance
    _crawler_instance = crawler
    _resolver_instance = resolver

    running = True

    def _signal_handler():
        nonlocal running
        logger.info("收到停止信号，正在停止所有任务...")
        running = False
        crawler.stop()
        resolver.stop()

    _setup_signal_handlers(_signal_handler)
    logger.info("[Daemon] 启动全自动模式: 爬取 → 解析 → 同步 循环")

    cycle = 0
    while running:
        cycle += 1
        logger.info(f"[Daemon] === 第 {cycle} 轮开始 ===")

        if settings.DAEMON_CRAWL_FIRST:
            logger.info("[Daemon] 阶段1: 爬取频道发现文件码...")
            try:
                stats = await crawler.discover_and_crawl()
                logger.info(f"[Daemon] 爬取完成: 发现 {stats['codes_found']} 个新码")
            except Exception as e:
                logger.error(f"[Daemon] 爬取阶段失败: {e}")

        logger.info("[Daemon] 阶段2: 解析待处理文件码...")
        try:
            resolved = await resolver.resolve_next_batch(batch_size=args.resolve_batch)
            logger.info(f"[Daemon] 解析完成: 成功 {resolved} 个")
        except Exception as e:
            logger.error(f"[Daemon] 解析阶段失败: {e}")

        if settings.COCKROACHDB_URL:
            logger.info("[Daemon] 阶段3: 同步到 CockroachDB...")
            try:
                synced = await syncer.sync_all(batch_size=1000)
                logger.info(f"[Daemon] 同步完成: {synced} 个")
            except Exception as e:
                logger.error(f"[Daemon] 同步阶段失败: {e}")
            finally:
                await syncer.close()

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

    await resolver.close()
    await client.disconnect()
    storage.close()
    logger.info("[Daemon] 全自动模式已停止")


async def cmd_status(args):
    configure_logging()
    storage = Storage()
    stats = storage.get_crawl_stats()
    resolve_st = storage.get_resolve_stats()

    print(f"\n{'=' * 55}")
    print(f"  文件码爬虫状态报告")
    print(f"{'=' * 55}")
    print(f"  频道总数:      {stats['channels']}")
    print(f"  活跃频道数:    {stats['active_channels']}")
    print(f"  文件码总数:    {stats['codes']}")
    print(f"  已解析:         {stats['resolved']}")
    print(f"  待解析:         {stats['unresolved']}")
    print(f"  已导出:         {stats['exported']}")
    print(f"  解析总次数:     {resolve_st['total']}")
    print(f"  解析成功:       {resolve_st['done']}")
    print(f"  解析失败:       {resolve_st['failed']}")
    print(f"  最后爬取:       {stats['last_crawl'] or '从未爬取'}")

    bot_stats = storage.get_code_count_by_bot()
    if bot_stats:
        print(f"\n  按机器人统计 (Top 10):")
        for i, bs in enumerate(bot_stats[:10], 1):
            print(f"    {i}. @{bs['bot_username']}: {bs['cnt']} 个码")

    print(f"{'=' * 55}\n")
    storage.close()


async def cmd_export(args):
    configure_logging()
    storage = Storage()
    filepath = None
    resolved_only = not args.all

    if args.format == "json":
        filepath = storage.export_to_json(filepath=args.output, resolved_only=resolved_only)
    elif args.format == "csv":
        filepath = storage.export_to_csv(filepath=args.output, resolved_only=resolved_only)

    if filepath:
        logger.info(f"导出成功: {filepath}")
    else:
        logger.info("没有新码需要导出")

    storage.close()


async def cmd_import_(args):
    configure_logging()
    storage = Storage()

    if not os.path.exists(args.file):
        logger.error(f"文件不存在: {args.file}")
        return

    count = storage.import_from_json(args.file)
    logger.info(f"从 {args.file} 导入 {count} 个文件码")
    storage.close()


async def cmd_channels(args):
    configure_logging()
    storage = Storage()
    channels = storage.get_all_channels(active_only=not args.all)

    print(f"\n{'=' * 65}")
    print(f"  频道列表 (共 {len(channels)} 个)")
    print(f"{'=' * 65}")

    for i, ch in enumerate(channels, 1):
        title = ch.get("title", "N/A")
        username = ch.get("channel_username", "")
        members = ch.get("member_count", 0)
        crawled = ch.get("crawl_count", 0)
        last = ch.get("last_crawled_at", "从未") or "从未"
        print(f"  {i:3d}. {title[:35]:35s} | @{username or '无':15s} | {members:5d} 人 | 爬取 {crawled} 次 | 最后: {last[:19]}")

    print(f"{'=' * 65}\n")
    storage.close()


async def cmd_sync(args):
    configure_logging()
    storage = Storage()
    syncer = CockroachSync(storage)
    try:
        total = await syncer.sync_all(batch_size=args.batch)
        logger.info(f"同步完成，共同步 {total} 个文件码")
    finally:
        await syncer.close()
        storage.close()


async def cmd_retry_failed(args):
    configure_logging()
    storage = Storage()

    conn = storage._conn
    conn.execute(
        "UPDATE file_codes SET resolve_attempts = 0, resolve_error = NULL WHERE is_resolved = 0"
    )
    conn.commit()
    count = storage.get_unresolved_count()
    logger.info(f"已重置 {count} 个失败码的解析状态，可以重新尝试解析")
    storage.close()


def _setup_signal_handlers(handler):
    if sys.platform != "win32":
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, handler)
            except NotImplementedError:
                signal.signal(sig, lambda s, f: handler())


async def _create_client() -> TelegramClient:
    if not settings.TELEGRAM_API_ID or not settings.TELEGRAM_API_HASH:
        logger.error("请设置 TELEGRAM_API_ID 和 TELEGRAM_API_HASH 环境变量")
        sys.exit(1)

    client = TelegramClient(
        "code_crawler_session",
        settings.TELEGRAM_API_ID,
        settings.TELEGRAM_API_HASH,
        device_model="CodeCrawler",
        app_version="1.0.0",
    )
    await client.start(phone=settings.TELEGRAM_PHONE or None)

    if not await client.is_user_authorized():
        logger.error("Telegram 未授权，请先运行 login 命令")
        await client.disconnect()
        sys.exit(1)

    me = await client.get_me()
    logger.info(f"Telegram 客户端已连接: {me.first_name}")
    return client


def main():
    configure_logging()

    parser = argparse.ArgumentParser(
        description="Telegram 文件码爬虫+解析器 - 自动发现、抓取、解析第三方文件码并缓存到本地",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 登录 Telegram 账号（首次使用）
  python main.py login

  # 单次爬取所有已发现的频道
  python main.py crawl

  # 持续爬取模式
  python main.py crawl --daemon

  # 解析未处理的文件码（主动向外部机器人请求文件）
  python main.py resolve

  # 持续解析模式（自动轮询未解析码）
  python main.py resolve --daemon

  # 全自动模式：爬取 → 解析 → 同步 CockroachDB 循环
  python main.py daemon

  # 查看爬取和解析状态
  python main.py status

  # 导出已解析的文件码
  python main.py export --format json

  # 重置失败的解析记录，重新尝试
  python main.py retry-failed

  # 同步到 CockroachDB
  python main.py sync
        """,
    )
    parser.add_argument("--env-file", default=".env", help=".env 文件路径")

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    login_parser = subparsers.add_parser("login", help="登录 Telegram 账号")

    crawl_parser = subparsers.add_parser("crawl", help="爬取文件码")
    crawl_parser.add_argument("--daemon", action="store_true", help="持续爬取模式")

    resolve_parser = subparsers.add_parser("resolve", help="解析文件码（向外部机器人请求文件）")
    resolve_parser.add_argument("--daemon", action="store_true", help="持续解析模式")
    resolve_parser.add_argument("--batch", type=int, default=settings.RESOLVE_BATCH_SIZE,
                                help="每批解析数量")

    daemon_parser = subparsers.add_parser("daemon", help="全自动模式: 爬取→解析→同步循环")
    daemon_parser.add_argument("--interval", type=int, default=settings.DAEMON_CYCLE_INTERVAL,
                               help="轮询间隔（秒）")
    daemon_parser.add_argument("--resolve-batch", type=int, default=settings.RESOLVE_BATCH_SIZE,
                               help="每批解析数量")

    status_parser = subparsers.add_parser("status", help="查看爬取和解析状态")

    export_parser = subparsers.add_parser("export", help="导出文件码")
    export_parser.add_argument("--format", choices=["json", "csv"], default="json", help="导出格式")
    export_parser.add_argument("--output", "-o", help="输出文件路径")
    export_parser.add_argument("--all", action="store_true", help="导出所有未导出的码（含未解析）")

    import_parser = subparsers.add_parser("import", help="导入文件码 (JSON)")
    import_parser.add_argument("file", help="JSON 文件路径")

    channels_parser = subparsers.add_parser("channels", help="查看频道列表")
    channels_parser.add_argument("--all", action="store_true", help="显示所有频道（含非活跃）")

    sync_parser = subparsers.add_parser("sync", help="同步文件码到 CockroachDB")
    sync_parser.add_argument("--batch", type=int, default=500, help="每批同步数量")

    retry_parser = subparsers.add_parser("retry-failed", help="重置失败码的状态，允许重新解析")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    if args.env_file and os.path.exists(args.env_file):
        _load_env_file(args.env_file)

    commands = {
        "login": cmd_login,
        "crawl": cmd_crawl,
        "resolve": cmd_resolve,
        "daemon": cmd_daemon,
        "status": cmd_status,
        "export": cmd_export,
        "import": cmd_import_,
        "channels": cmd_channels,
        "sync": cmd_sync,
        "retry-failed": cmd_retry_failed,
    }

    cmd = commands.get(args.command)
    if cmd:
        asyncio.run(cmd(args))


def _load_env_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            value = value.strip("\"'")
            if key and not os.environ.get(key):
                os.environ[key] = value


if __name__ == "__main__":
    main()