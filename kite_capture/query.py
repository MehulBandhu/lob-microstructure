import sys
from datetime import date
from pathlib import Path

import polars as pl

import config

def show_today():
    today_dir = config.TICK_DIR / str(date.today())
    if not today_dir.exists():
        print(f"No data for {date.today()}")
        return

    print(f"\nData for {date.today()}:")
    print("-" * 50)
    total = 0
    for f in sorted(today_dir.glob("*.parquet")):
        df = pl.read_parquet(f)
        size_mb = f.stat().st_size / 1024 / 1024
        total += len(df)
        print(f"  {f.name:<35s} {len(df):>10,} rows  {size_mb:>6.1f} MB")
        if "exchange_timestamp" in df.columns and len(df) > 0:
            ts_min = df["exchange_timestamp"].min()
            ts_max = df["exchange_timestamp"].max()
            print(f"    Time range: {ts_min} → {ts_max}")
    print(f"  {'TOTAL':<35s} {total:>10,} rows")

def peek(category: str, n: int = 10):
    today_dir = config.TICK_DIR / str(date.today())
    # Try consolidated file first, then chunks
    path = today_dir / f"{category}.parquet"
    if not path.exists():
        chunks = sorted(today_dir.glob(f"{category}_*.parquet"))
        if chunks:
            path = chunks[-1]  # Latest chunk
        else:
            print(f"No data found for category '{category}'")
            return

    df = pl.read_parquet(path)
    print(f"\n{path.name} — {len(df)} total rows, showing last {n}:")
    print(df.tail(n))

def list_symbols(category: str):
    today_dir = config.TICK_DIR / str(date.today())
    path = today_dir / f"{category}.parquet"
    if not path.exists():
        chunks = sorted(today_dir.glob(f"{category}_*.parquet"))
        if not chunks:
            print(f"No data for '{category}'")
            return
        # Read all chunks
        dfs = [pl.read_parquet(c) for c in chunks]
        df = pl.concat(dfs)
    else:
        df = pl.read_parquet(path)

    symbols = df["tradingsymbol"].unique().sort().to_list()
    print(f"\n{len(symbols)} unique symbols in {category}:")
    for s in symbols:
        count = df.filter(pl.col("tradingsymbol") == s).height
        print(f"  {s:<30s} {count:>8,} ticks")

def count_date(target_date: str):
    day_dir = config.TICK_DIR / target_date
    if not day_dir.exists():
        # Try archive
        archive = config.ARCHIVE_DIR / f"{target_date}.tar.zst"
        if archive.exists():
            print(f"Data for {target_date} is archived at {archive}")
            print(f"Run: python query.py extract {target_date}")
        else:
            print(f"No data found for {target_date}")
        return

    print(f"\nData for {target_date}:")
    print("-" * 50)
    total = 0
    for f in sorted(day_dir.glob("*.parquet")):
        df = pl.read_parquet(f)
        total += len(df)
        print(f"  {f.name:<35s} {len(df):>10,} rows")
    print(f"  {'TOTAL':<35s} {total:>10,} rows")

def extract_archive(target_date: str):
    import tarfile
    import zstandard as zstd
    import io

    archive = config.ARCHIVE_DIR / f"{target_date}.tar.zst"
    if not archive.exists():
        print(f"Archive not found: {archive}")
        return

    out_dir = config.TICK_DIR / target_date
    out_dir.mkdir(parents=True, exist_ok=True)

    dctx = zstd.ZstdDecompressor()
    with open(str(archive), "rb") as f_in:
        decompressed = dctx.stream_reader(f_in)
        with tarfile.open(fileobj=decompressed, mode="r|") as tar:
            tar.extractall(str(out_dir))

    print(f"Extracted to {out_dir}")
    count_date(target_date)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python query.py <command> [args]")
        print("  today              — Show today's stats")
        print("  peek <category>    — Show latest rows")
        print("  symbols <category> — List unique symbols")
        print("  count <date>       — Row counts for date")
        print("  extract <date>     — Extract archive")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "today":
        show_today()
    elif cmd == "peek" and len(sys.argv) >= 3:
        peek(sys.argv[2])
    elif cmd == "symbols" and len(sys.argv) >= 3:
        list_symbols(sys.argv[2])
    elif cmd == "count" and len(sys.argv) >= 3:
        count_date(sys.argv[2])
    elif cmd == "extract" and len(sys.argv) >= 3:
        extract_archive(sys.argv[2])
    else:
        print(f"Unknown command: {cmd}")
