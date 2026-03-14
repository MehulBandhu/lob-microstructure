import logging
from datetime import datetime, date
from typing import Optional
from zoneinfo import ZoneInfo

import polars as pl
from kiteconnect import KiteConnect

import config

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

# NIFTY 100 constituents (NIFTY 50 + NIFTY Next 50)
# Update this list when NSE rebalances (quarterly)
NIFTY_50_SYMBOLS = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BPCL", "BHARTIARTL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC",
    "INDUSINDBK", "INFY", "JSWSTEEL", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA",
    "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM",
    "TITAN", "ULTRACEMCO", "UPL", "WIPRO", "LTIM",
]

NIFTY_NEXT50_SYMBOLS = [
    "ABB", "ADANIGREEN", "AMBUJACEM", "ATGL", "BAJAJHLDNG",
    "BANKBARODA", "BEL", "BERGEPAINT", "BOSCHLTD", "CANBK",
    "CHOLAFIN", "COLPAL", "DABUR", "DLF", "GAIL",
    "GODREJCP", "HAL", "HAVELLS", "ICICIPRULI", "IDEA",
    "INDHOTEL", "INDUSTOWER", "IOC", "IRCTC", "JIOFIN",
    "JUBLFOOD", "LICI", "LODHA", "LUPIN", "MARICO",
    "MOTHERSON", "NHPC", "NYKAA", "OFSS", "PAGEIND",
    "PERSISTENT", "PIDILITIND", "PNB", "POLYCAB", "SAIL",
    "SRF", "SHREECEM", "SIEMENS", "TATAELXSI", "TATAPOWER",
    "TORNTPHARM", "TRENT", "VBL", "VEDL", "ZOMATO",
]

NIFTY_100_SYMBOLS = NIFTY_50_SYMBOLS + NIFTY_NEXT50_SYMBOLS

# Update quarterly when NSE rebalances
MIDCAP_100_SYMBOLS = [
    "APLAPOLLO", "AUBANK", "ABCAPITAL", "ALKEM", "ASHOKLEY",
    "ASTRAL", "AUROPHARMA", "BALKRISIND", "BANDHANBNK", "BHARATFORG",
    "BHEL", "BIOCON", "CESC", "CGPOWER", "COFORGE",
    "CONCOR", "CROMPTON", "CUB", "CUMMINSIND", "DALBHARAT",
    "DEEPAKNTR", "DIXON", "ESCORTS", "EXIDEIND", "FEDERALBNK",
    "FORTIS", "GMRAIRPORT", "GLENMARK", "GNFC", "GODREJPROP",
    "HONAUT", "IDFCFIRSTB", "IEX", "INDIANB", "INDIACEM",
    "IRFC", "JKCEMENT", "JSL", "JUBLINGREA", "KALYANKJIL",
    "KEI", "LAURUSLABS", "LICHSGFIN", "LTTS",
    "MFSL", "MANAPPURAM", "MPHASIS", "MRF", "MUTHOOTFIN",
    "NATIONALUM", "NAUKRI", "NAVINFLUOR", "NMDC",
    "OBEROIRLTY", "OIL", "PAYTM", "PEL", "PETRONET",
    "PFC", "PHOENIXLTD", "PIIND",
    "PRESTIGE", "PVRINOX", "RAMCOCEM", "RECLTD",
    "SBICARD", "SJVN", "SONACOMS", "STARHEALTH", "SUNDARMFIN",
    "SUPREMEIND", "SYNGENE", "TATACHEM", "TATACOMM",
    "TATATECH", "THERMAX", "TIINDIA", "TORNTPOWER", "TVSMOTOR",
    "UBL", "UNIONBANK", "UNOMINDA", "VOLTAS", "WHIRLPOOL",
    "YESBANK", "ZEEL", "ZYDUSLIFE",
]

class InstrumentManager:

    def __init__(self, kite: KiteConnect):
        self.kite = kite
        self.today = date.today()
        self.all_instruments: Optional[pl.DataFrame] = None

        # Token → metadata lookup (populated after resolve)
        self.token_map: dict[int, dict] = {}

        # Subscription lists for 2 connections
        self.connection_1_tokens: list[int] = []  # equities, ETFs, VIX, index F&O
        self.connection_2_tokens: list[int] = []  # stock F&O

        # Category tags for storage routing
        self.token_categories: dict[int, str] = {}

    def fetch_and_save_instruments(self) -> pl.DataFrame:
        logger.info("Fetching instrument master from Kite...")
        raw = self.kite.instruments()
        logger.info(f"Fetched {len(raw)} instruments across all exchanges")

        # Normalize to strings before creating DataFrame.
        for item in raw:
            item["expiry"] = str(item["expiry"]) if item["expiry"] else ""

        df = pl.DataFrame(raw, infer_schema_length=None)

        # Save daily snapshot
        path = config.INSTRUMENT_DIR / f"{self.today}.parquet"
        df.write_parquet(str(path))
        logger.info(f"Saved instrument master to {path} ({len(df)} rows)")

        self.all_instruments = df
        return df

    def _lookup(self, exchange: str, tradingsymbol: str) -> Optional[dict]:
        rows = self.all_instruments.filter(
            (pl.col("exchange") == exchange) &
            (pl.col("tradingsymbol") == tradingsymbol)
        )
        if len(rows) == 0:
            return None
        return rows.row(0, named=True)

    def _lookup_token(self, exchange: str, tradingsymbol: str) -> Optional[int]:
        inst = self._lookup(exchange, tradingsymbol)
        return inst["instrument_token"] if inst else None

    def _get_ltp(self, exchange: str, tradingsymbol: str) -> Optional[float]:
        try:
            key = f"{exchange}:{tradingsymbol}"
            data = self.kite.ltp([key])
            return data[key]["last_price"]
        except Exception as e:
            logger.warning(f"Failed to get LTP for {exchange}:{tradingsymbol}: {e}")
            return None

    def _register(self, token: int, inst: dict, category: str):
        self.token_map[token] = {
            "instrument_token": token,
            "tradingsymbol": inst.get("tradingsymbol", ""),
            "exchange": inst.get("exchange", ""),
            "segment": inst.get("segment", ""),
            "instrument_type": inst.get("instrument_type", ""),
            "strike": inst.get("strike", 0),
            "expiry": inst.get("expiry", ""),
            "lot_size": inst.get("lot_size", 0),
            "name": inst.get("name", ""),
            "tick_size": inst.get("tick_size", 0),
        }
        self.token_categories[token] = category

    def resolve_equities(self) -> list[int]:
        tokens = []
        missing = []
        for symbol in NIFTY_100_SYMBOLS:
            inst = self._lookup("NSE", symbol)
            if inst:
                token = inst["instrument_token"]
                self._register(token, inst, "equities")
                tokens.append(token)
            else:
                missing.append(symbol)

        if missing:
            logger.warning(f"Could not find {len(missing)} equities: {missing}")

        logger.info(f"Resolved {len(tokens)} equity tokens")
        return tokens

    def resolve_midcap_equities(self) -> list[int]:
        tokens = []
        missing = []
        for symbol in MIDCAP_100_SYMBOLS:
            if symbol in NIFTY_100_SYMBOLS:
                continue  # skip duplicates
            inst = self._lookup("NSE", symbol)
            if inst:
                token = inst["instrument_token"]
                self._register(token, inst, "midcap_equities")
                tokens.append(token)
            else:
                missing.append(symbol)

        if missing:
            logger.warning(f"Could not find {len(missing)} midcap equities: {missing}")

        logger.info(f"Resolved {len(tokens)} midcap equity tokens")
        return tokens

    def resolve_etfs(self) -> list[int]:
        tokens = []
        for symbol in config.ETFS:
            inst = self._lookup("NSE", symbol)
            if inst:
                token = inst["instrument_token"]
                self._register(token, inst, "etf")
                tokens.append(token)
            else:
                logger.warning(f"ETF not found: {symbol}")

        logger.info(f"Resolved {len(tokens)} ETF tokens")
        return tokens

    def resolve_vix(self) -> list[int]:
        if not config.TRACK_VIX:
            return []

        inst = self._lookup("NSE", config.VIX_SYMBOL)
        if inst:
            token = inst["instrument_token"]
            self._register(token, inst, "etf")  # group with ETF for storage
            logger.info(f"Resolved India VIX: {token}")
            return [token]
        else:
            logger.warning(f"India VIX not found with symbol '{config.VIX_SYMBOL}'")
            return []

    def resolve_futures(
        self, name: str, exchange: str, num_expiries: int, category: str
    ) -> list[int]:
        # Filter futures for this underlying
        futures = self.all_instruments.filter(
            (pl.col("exchange") == exchange) &
            (pl.col("name") == name) &
            (pl.col("instrument_type") == "FUT") &
            (pl.col("expiry") >= str(self.today))
        ).sort("expiry")

        if len(futures) == 0:
            logger.warning(f"No futures found for {name} on {exchange}")
            return []

        # Take nearest N expiries
        selected = futures.head(num_expiries)
        tokens = []
        for row in selected.iter_rows(named=True):
            token = row["instrument_token"]
            self._register(token, row, category)
            tokens.append(token)

        symbols = selected["tradingsymbol"].to_list()
        logger.info(f"Resolved {len(tokens)} futures for {name}: {symbols}")
        return tokens

    def resolve_option_chain(
        self,
        name: str,
        exchange: str,
        spot_exchange: str,
        spot_symbol: str,
        strikes_around_atm: int,
        num_expiries: int,
        category: str,
    ) -> list[int]:
        # Get spot price for ATM resolution
        ltp = self._get_ltp(spot_exchange, spot_symbol)
        if ltp is None:
            logger.error(f"Cannot resolve options for {name}: no LTP available")
            return []

        logger.info(f"{name} LTP: {ltp}")

        # Get all options for this underlying
        options = self.all_instruments.filter(
            (pl.col("exchange") == exchange) &
            (pl.col("name") == name) &
            (pl.col("instrument_type").is_in(["CE", "PE"])) &
            (pl.col("expiry") >= str(self.today))
        )

        if len(options) == 0:
            logger.warning(f"No options found for {name} on {exchange}")
            return []

        # Get unique expiries, sorted
        expiries = (
            options.select("expiry")
            .unique()
            .sort("expiry")
            .head(num_expiries)["expiry"]
            .to_list()
        )
        logger.info(f"{name} option expiries selected: {expiries}")

        # Get unique strikes to find ATM
        all_strikes = options["strike"].unique().sort().to_list()
        all_strikes = [s for s in all_strikes if s > 0]

        if not all_strikes:
            logger.warning(f"No valid strikes for {name}")
            return []

        # Find ATM strike (closest to LTP)
        atm_strike = min(all_strikes, key=lambda s: abs(s - ltp))
        atm_idx = all_strikes.index(atm_strike)

        # Select ±N strikes around ATM
        start_idx = max(0, atm_idx - strikes_around_atm)
        end_idx = min(len(all_strikes), atm_idx + strikes_around_atm + 1)
        selected_strikes = all_strikes[start_idx:end_idx]

        logger.info(
            f"{name} ATM: {atm_strike}, "
            f"selected {len(selected_strikes)} strikes: "
            f"{selected_strikes[0]} to {selected_strikes[-1]}"
        )

        # Filter options to selected strikes and expiries
        selected = options.filter(
            (pl.col("strike").is_in(selected_strikes)) &
            (pl.col("expiry").is_in(expiries))
        )

        tokens = []
        for row in selected.iter_rows(named=True):
            token = row["instrument_token"]
            self._register(token, row, category)
            tokens.append(token)

        logger.info(
            f"Resolved {len(tokens)} option tokens for {name} "
            f"({len(selected_strikes)} strikes × {len(expiries)} expiries × 2)"
        )
        return tokens

    def resolve_stock_futures(self) -> list[int]:
        all_tokens = []
        for symbol in NIFTY_50_SYMBOLS:
            tokens = self.resolve_futures(
                name=symbol,
                exchange="NFO",
                num_expiries=config.STOCK_OPTIONS["expiries"],
                category="equity_futures",
            )
            all_tokens.extend(tokens)
        logger.info(f"Total stock futures tokens: {len(all_tokens)}")
        return all_tokens

    def resolve_stock_options(self) -> list[int]:
        all_tokens = []
        for symbol in NIFTY_50_SYMBOLS:
            tokens = self.resolve_option_chain(
                name=symbol,
                exchange="NFO",
                spot_exchange="NSE",
                spot_symbol=symbol,
                strikes_around_atm=config.STOCK_OPTIONS["strikes_around_atm"],
                num_expiries=config.STOCK_OPTIONS["expiries"],
                category="equity_options",
            )
            all_tokens.extend(tokens)
        logger.info(f"Total stock options tokens: {len(all_tokens)}")
        return all_tokens

    def resolve_all(self):
        self.fetch_and_save_instruments()

        conn1 = []
        conn1.extend(self.resolve_equities())
        conn1.extend(self.resolve_midcap_equities())
        conn1.extend(self.resolve_etfs())
        conn1.extend(self.resolve_vix())

        # Index futures
        for idx_name, idx_cfg in config.INDICES.items():
            # Determine spot symbol for LTP
            if idx_name == "NIFTY":
                spot_sym = "NIFTY 50"
            elif idx_name == "BANKNIFTY":
                spot_sym = "NIFTY BANK"
            else:
                spot_sym = idx_name

            conn1.extend(self.resolve_futures(
                name=idx_name,
                exchange=idx_cfg["exchange"],
                num_expiries=3,  # current + next + far
                category="index_futures",
            ))
            conn1.extend(self.resolve_option_chain(
                name=idx_name,
                exchange=idx_cfg["exchange"],
                spot_exchange="NSE",
                spot_symbol=spot_sym,
                strikes_around_atm=idx_cfg["strikes_around_atm"],
                num_expiries=idx_cfg["expiries"],
                category="index_options",
            ))

        self.connection_1_tokens = conn1

        conn2 = []
        conn2.extend(self.resolve_stock_futures())
        conn2.extend(self.resolve_stock_options())
        self.connection_2_tokens = conn2

        total = len(conn1) + len(conn2)
        logger.info("=" * 60)
        logger.info("INSTRUMENT RESOLUTION COMPLETE")
        logger.info(f"  Connection 1: {len(conn1)} tokens")
        logger.info(f"  Connection 2: {len(conn2)} tokens")
        logger.info(f"  Total:        {total} tokens")
        logger.info(f"  Token map:    {len(self.token_map)} entries")
        logger.info("=" * 60)

        # Validate limits
        if len(conn1) > config.MAX_TOKENS_PER_CONNECTION:
            logger.error(
                f"Connection 1 exceeds limit! "
                f"{len(conn1)} > {config.MAX_TOKENS_PER_CONNECTION}"
            )
        if len(conn2) > config.MAX_TOKENS_PER_CONNECTION:
            logger.error(
                f"Connection 2 exceeds limit! "
                f"{len(conn2)} > {config.MAX_TOKENS_PER_CONNECTION}"
            )

        # Print category breakdown
        from collections import Counter
        cats = Counter(self.token_categories.values())
        for cat, count in sorted(cats.items()):
            logger.info(f"  {cat}: {count} tokens")

    def get_metadata(self, token: int) -> dict:
        return self.token_map.get(token, {})

    def get_tradingsymbol(self, token: int) -> str:
        meta = self.token_map.get(token)
        return meta["tradingsymbol"] if meta else f"UNKNOWN_{token}"

    def get_category(self, token: int) -> str:
        return self.token_categories.get(token, "unknown")
