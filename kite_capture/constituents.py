import logging
import json
from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
import requests

import config

logger = logging.getLogger(__name__)

NSE_INDEX_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}

NSE_API_URL = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

def _get_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    # Hit main page first to get cookies
    try:
        session.get("https://www.nseindia.com/", timeout=10)
    except Exception:
        pass
    return session

def fetch_nifty50_constituents() -> Optional[pl.DataFrame]:
    session = _get_nse_session()

    try:
        # Try NSE API endpoint first (has weight data)
        resp = session.get(NSE_API_URL, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            records = []
            for item in data.get("data", []):
                symbol = item.get("symbol", "")
                if not symbol or symbol == "NIFTY 50":
                    continue
                records.append({
                    "symbol": symbol,
                    "company_name": item.get("meta", {}).get("companyName", ""),
                    "industry": item.get("meta", {}).get("industry", ""),
                    "isin": item.get("meta", {}).get("isin", ""),
                    "weight": float(item.get("ffmc", 0) or 0),
                    "last_price": float(item.get("lastPrice", 0) or 0),
                    "change_pct": float(item.get("pChange", 0) or 0),
                })
            if records:
                df = pl.DataFrame(records)
                logger.info(f"Fetched {len(df)} NIFTY 50 constituents from NSE API")
                return df
    except Exception as e:
        logger.warning(f"NSE API fetch failed: {e}")

    # Fallback: try CSV endpoint
    try:
        resp = session.get(NSE_INDEX_URL, timeout=15)
        if resp.status_code == 200:
            # Parse CSV
            import io
            df = pl.read_csv(io.StringIO(resp.text))
            # Normalize column names
            df = df.rename({c: c.strip().lower().replace(" ", "_") for c in df.columns})
            logger.info(f"Fetched {len(df)} NIFTY 50 constituents from CSV")
            return df
    except Exception as e:
        logger.warning(f"NSE CSV fetch failed: {e}")

    logger.error("All NIFTY 50 constituent fetch methods failed")
    return None

def save_daily_constituents(df: pl.DataFrame, today: Optional[date] = None):
    today = today or date.today()
    out_dir = config.CONSTITUENT_DIR / "nifty50"
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / f"{today}.parquet"
    df.write_parquet(path)
    logger.info(f"Saved constituents to {path} ({len(df)} rows)")
    return path

def load_constituents(target_date: Optional[date] = None) -> Optional[pl.DataFrame]:
    target_date = target_date or date.today()
    path = config.CONSTITUENT_DIR / "nifty50" / f"{target_date}.parquet"

    if path.exists():
        return pl.read_parquet(path)

    # Fallback: find most recent file
    out_dir = config.CONSTITUENT_DIR / "nifty50"
    if not out_dir.exists():
        return None

    files = sorted(out_dir.glob("*.parquet"), reverse=True)
    if files:
        logger.warning(
            f"No constituents for {target_date}, "
            f"falling back to {files[0].stem}"
        )
        return pl.read_parquet(files[0])

    return None

def detect_changes(today_df: pl.DataFrame) -> Optional[dict]:
    out_dir = config.CONSTITUENT_DIR / "nifty50"
    files = sorted(out_dir.glob("*.parquet"), reverse=True)

    # Need at least 2 files (today + previous)
    if len(files) < 2:
        return None

    prev_df = pl.read_parquet(files[1])

    today_symbols = set(today_df["symbol"].to_list())
    prev_symbols = set(prev_df["symbol"].to_list())

    added = today_symbols - prev_symbols
    removed = prev_symbols - today_symbols

    if not added and not removed:
        return None

    changes = {
        "date": str(date.today()),
        "previous_date": files[1].stem,
        "added": sorted(added),
        "removed": sorted(removed),
    }

    logger.info(f"NIFTY 50 composition change detected!")
    if added:
        logger.info(f"  Added: {sorted(added)}")
    if removed:
        logger.info(f"  Removed: {sorted(removed)}")

    # Append to changelog
    changelog_path = config.CONSTITUENT_DIR / "changelog.json"
    history = []
    if changelog_path.exists():
        with open(changelog_path) as f:
            history = json.load(f)
    history.append(changes)
    with open(changelog_path, "w") as f:
        json.dump(history, f, indent=2)

    return changes

def fetch_and_save() -> Optional[pl.DataFrame]:
    df = fetch_nifty50_constituents()
    if df is None:
        logger.error("Could not fetch constituent data — using cached if available")
        return load_constituents()

    save_daily_constituents(df)
    detect_changes(df)
    return df
