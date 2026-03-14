import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("KITE_API_KEY", "")
API_SECRET = os.getenv("KITE_API_SECRET", "")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
TICK_DIR = DATA_DIR / "ticks"
INSTRUMENT_DIR = DATA_DIR / "instruments"
CONSTITUENT_DIR = DATA_DIR / "constituents"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = BASE_DIR / "logs"

# Create all directories
for d in [TICK_DIR, INSTRUMENT_DIR, CONSTITUENT_DIR, ARCHIVE_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

CONNECT_HOUR = 9
CONNECT_MINUTE = 0

POSTMARKET_HOUR = 15
POSTMARKET_MINUTE = 35

EXCHANGE_NSE = "NSE"
EXCHANGE_NFO = "NFO"

INDICES = {
    "NIFTY": {
        "exchange": "NFO",
        "name": "NIFTY",
        "strikes_around_atm": 20,  # ±20 strikes
        "expiries": 3,
    },
    "BANKNIFTY": {
        "exchange": "NFO",
        "name": "BANKNIFTY",
        "strikes_around_atm": 20,
        "expiries": 3,
    },
}

STOCK_OPTIONS = {
    "strikes_around_atm": 5,    # ±5 strikes for stock options
    "expiries": 2,              # current month + next month
}

ETFS = ["NIFTYBEES"]

TRACK_VIX = True
VIX_SYMBOL = "INDIA VIX"

MAX_TOKENS_PER_CONNECTION = 3000

RECONNECT_MAX_RETRIES = 50
RECONNECT_DELAY_SECONDS = 5

FLUSH_INTERVAL_SECONDS = 30

MAX_CHUNKS_PER_DAY = 1000

UNCOMPRESSED_RETENTION_DAYS = 7

HEALTH_INTERVAL_SECONDS = 10

TICK_TIMEOUT_SECONDS = 5

MIN_TICK_RATE = 100

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
