import argparse
import logging
import signal
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from kiteconnect import KiteConnect

import config
from holidays import is_trading_day as is_nse_trading_day, get_holiday_name, next_trading_day
from instruments import InstrumentManager
from constituents import fetch_and_save as fetch_constituents
from ticker import TickerManager
from storage import TickStorage
from monitor import Monitor
from postmarket import run_postmarket

IST = ZoneInfo("Asia/Kolkata")

def setup_logging():
    from datetime import date

    log_file = config.LOG_DIR / f"{date.today()}.log"

    # Root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, config.LOG_LEVEL))

    # Console handler (INFO and above)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(config.LOG_FORMAT, config.LOG_DATE_FORMAT))
    root.addHandler(console)

    # File handler (DEBUG and above)
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(config.LOG_FORMAT, config.LOG_DATE_FORMAT))
    root.addHandler(file_handler)

    return logging.getLogger("main")

def now_ist() -> datetime:
    return datetime.now(IST)

def wait_until(hour: int, minute: int, logger):
    while True:
        n = now_ist()
        target = n.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if n >= target:
            return
        delta = (target - n).total_seconds()
        logger.info(f"Waiting until {hour:02d}:{minute:02d} IST ({delta:.0f}s)...")
        time.sleep(min(delta, 30))

def is_market_hours() -> bool:
    n = now_ist()
    open_time = n.replace(
        hour=config.MARKET_OPEN_HOUR,
        minute=config.MARKET_OPEN_MINUTE,
        second=0,
    )
    close_time = n.replace(
        hour=config.MARKET_CLOSE_HOUR,
        minute=config.MARKET_CLOSE_MINUTE,
        second=0,
    )
    return open_time <= n <= close_time

def is_trading_day() -> bool:
    return is_nse_trading_day()

def run_session(logger, run_once: bool = False):

    logger.info("=" * 60)
    logger.info("  KITE CAPTURE — Starting session")
    logger.info(f"  Date: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}")
    logger.info("=" * 60)

    if not config.ACCESS_TOKEN:
        logger.error("No access token! Run: python auth.py")
        sys.exit(1)

    kite = KiteConnect(api_key=config.API_KEY)
    kite.set_access_token(config.ACCESS_TOKEN)

    # Verify token is valid
    try:
        profile = kite.profile()
        logger.info(f"Authenticated as: {profile['user_name']} ({profile['user_id']})")
    except Exception as e:
        logger.error(f"Invalid access token: {e}")
        logger.error("Run: python auth.py")
        sys.exit(1)

    im = InstrumentManager(kite)
    im.resolve_all()

    if not im.connection_1_tokens and not im.connection_2_tokens:
        logger.error("No instruments resolved! Check your config.")
        sys.exit(1)

    logger.info("Fetching NIFTY 50 constituent data...")
    try:
        constituents = fetch_constituents()
        if constituents is not None:
            logger.info(f"Constituents: {len(constituents)} stocks loaded")
        else:
            logger.warning("Could not fetch constituents — continuing without")
    except Exception as e:
        logger.warning(f"Constituent fetch failed: {e} — continuing without")

    storage = TickStorage(im)

    monitor = None

    def on_tick(tick):
        storage.on_tick(tick)
        if monitor:
            token = tick.get("instrument_token", 0)
            monitor.on_tick(token)

    ticker_mgr = TickerManager(
        api_key=config.API_KEY,
        access_token=config.ACCESS_TOKEN,
        on_tick_callback=on_tick,
    )

    monitor = Monitor(ticker_mgr, storage, im)

    n = now_ist()
    connect_time = n.replace(
        hour=config.CONNECT_HOUR, minute=config.CONNECT_MINUTE, second=0
    )
    postmarket_time = n.replace(
        hour=config.POSTMARKET_HOUR, minute=config.POSTMARKET_MINUTE, second=0
    )

    if n < connect_time:
        wait_until(config.CONNECT_HOUR, config.CONNECT_MINUTE, logger)

    if n > postmarket_time:
        logger.warning("Market has already closed for today!")
        if run_once:
            return
        # Continue anyway for testing

    logger.info("Starting tick capture...")
    storage.start_flush_loop()
    monitor.start()
    ticker_mgr.start(
        tokens_1=im.connection_1_tokens,
        tokens_2=im.connection_2_tokens,
    )

    logger.info("System is LIVE — capturing ticks")
    logger.info(
        f"Will run until {config.POSTMARKET_HOUR:02d}:"
        f"{config.POSTMARKET_MINUTE:02d} IST"
    )

    import threading

    def watchdog():
        while ticker_mgr._running:
            n = now_ist()
            if n.hour == config.POSTMARKET_HOUR and n.minute >= config.POSTMARKET_MINUTE:
                logger.info("Market close reached — watchdog stopping ticker")
                ticker_mgr.stop()
                break

            if ticker_mgr.is_circuit_open():
                logger.critical(
                    "Circuit breaker tripped! "
                    "Shutting down to save resources."
                )
                ticker_mgr.stop()
                break

            time.sleep(5)

    watchdog_thread = threading.Thread(target=watchdog, daemon=True, name="watchdog")
    watchdog_thread.start()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        ticker_mgr.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    ticker_mgr.run_blocking()

    logger.info("Ticker stopped. Cleaning up...")

    logger.info("Stopping flush loop...")
    storage.stop_flush_loop()

    logger.info("Stopping monitor...")
    monitor.stop()

    # Daily summary
    monitor.daily_summary()

    logger.info("Running post-market processing...")
    run_postmarket(storage)

    logger.info("Session complete!")

def main():
    parser = argparse.ArgumentParser(description="Kite Capture — Tick Data Collector")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one session and exit (for cron/systemd)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even on holidays/weekends (for testing)",
    )
    args = parser.parse_args()

    logger = setup_logging()

    if not args.force and not is_trading_day():
        from datetime import date
        today = date.today()
        holiday = get_holiday_name(today)
        if holiday:
            logger.info(f"Today is an NSE holiday: {holiday}. Not a trading day.")
        elif today.weekday() >= 5:
            logger.info(f"Today is {'Saturday' if today.weekday() == 5 else 'Sunday'}. Not a trading day.")
        nxt = next_trading_day(today)
        logger.info(f"Next trading day: {nxt} ({nxt.strftime('%A')})")
        logger.info("Exiting. Use --force to override.")
        return

    run_session(logger, run_once=True)

if __name__ == "__main__":
    main()
