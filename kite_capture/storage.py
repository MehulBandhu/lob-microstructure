import logging
import os
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

import config

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

DEPTH_LEVELS = 5

_fields = [
    pa.field("exchange_timestamp", pa.timestamp("us", tz="Asia/Kolkata")),
    pa.field("received_timestamp", pa.timestamp("us", tz="Asia/Kolkata")),
    pa.field("instrument_token", pa.uint32()),
    pa.field("tradingsymbol", pa.string()),
    pa.field("last_price", pa.float64()),
    pa.field("volume", pa.uint64()),
    pa.field("oi", pa.uint64()),
]

# Add depth fields
for side in ["bid", "ask"]:
    for level in range(1, DEPTH_LEVELS + 1):
        _fields.append(pa.field(f"{side}_price_{level}", pa.float64()))
        _fields.append(pa.field(f"{side}_qty_{level}", pa.uint64()))
        _fields.append(pa.field(f"{side}_orders_{level}", pa.uint32()))

TICK_SCHEMA = pa.schema(_fields)

CATEGORIES = [
    "equities",
    "midcap_equities",
    "etf",
    "equity_futures",
    "equity_options",
    "index_futures",
    "index_options",
]

def _empty_buffer() -> dict[str, list]:
    return {cat: [] for cat in CATEGORIES}

def parse_tick(tick: dict, tradingsymbol: str, category: str) -> dict:
    now = datetime.now(IST)

    # Exchange timestamp from tick
    exchange_ts = tick.get("exchange_timestamp") or tick.get("timestamp")
    if exchange_ts and not exchange_ts.tzinfo:
        exchange_ts = exchange_ts.replace(tzinfo=IST)

    row = {
        "exchange_timestamp": exchange_ts or now,
        "received_timestamp": now,
        "instrument_token": tick.get("instrument_token", 0),
        "tradingsymbol": tradingsymbol,
        "last_price": tick.get("last_price", 0.0),
        "volume": tick.get("volume_traded", 0) or tick.get("volume", 0),
        "oi": tick.get("oi", 0),
    }

    # Parse depth (top 5 bid/ask)
    depth = tick.get("depth", {})
    buy_depth = depth.get("buy", [])
    sell_depth = depth.get("sell", [])

    for level in range(DEPTH_LEVELS):
        lvl = level + 1
        if level < len(buy_depth):
            row[f"bid_price_{lvl}"] = buy_depth[level].get("price", 0.0)
            row[f"bid_qty_{lvl}"] = buy_depth[level].get("quantity", 0)
            row[f"bid_orders_{lvl}"] = buy_depth[level].get("orders", 0)
        else:
            row[f"bid_price_{lvl}"] = 0.0
            row[f"bid_qty_{lvl}"] = 0
            row[f"bid_orders_{lvl}"] = 0

        if level < len(sell_depth):
            row[f"ask_price_{lvl}"] = sell_depth[level].get("price", 0.0)
            row[f"ask_qty_{lvl}"] = sell_depth[level].get("quantity", 0)
            row[f"ask_orders_{lvl}"] = sell_depth[level].get("orders", 0)
        else:
            row[f"ask_price_{lvl}"] = 0.0
            row[f"ask_qty_{lvl}"] = 0
            row[f"ask_orders_{lvl}"] = 0

    return row

class TickStorage:

    def __init__(self, instrument_manager):
        self.im = instrument_manager
        self.today = date.today()
        self.today_dir = config.TICK_DIR / str(self.today)
        self.today_dir.mkdir(parents=True, exist_ok=True)

        # Double buffer
        self._buffer_a = _empty_buffer()
        self._buffer_b = _empty_buffer()
        self._active_buffer = self._buffer_a
        self._lock = threading.Lock()

        # Chunk counter per category
        self._chunk_counts: dict[str, int] = {cat: 0 for cat in CATEGORIES}

        # Stats
        self.total_ticks_received = 0
        self.total_ticks_flushed = 0
        self.total_flush_count = 0
        self.last_flush_time: Optional[datetime] = None
        self.last_flush_duration: float = 0.0
        self.last_flush_rows: int = 0

        # Flush thread control
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None

    def on_tick(self, tick: dict):
        token = tick.get("instrument_token", 0)
        tradingsymbol = self.im.get_tradingsymbol(token)
        category = self.im.get_category(token)

        if category == "unknown":
            return  # Unknown token, skip

        row = parse_tick(tick, tradingsymbol, category)

        with self._lock:
            self._active_buffer[category].append(row)
            self.total_ticks_received += 1

    def start_flush_loop(self):
        self._running = True
        self._flush_thread = threading.Thread(
            target=self._flush_loop, daemon=True, name="tick-flusher"
        )
        self._flush_thread.start()
        logger.info(
            f"Flush loop started (interval={config.FLUSH_INTERVAL_SECONDS}s)"
        )

    def stop_flush_loop(self):
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=30)
        # Final flush of remaining data
        self._do_flush()
        logger.info("Flush loop stopped, final flush complete")

    def _flush_loop(self):
        while self._running:
            time.sleep(config.FLUSH_INTERVAL_SECONDS)
            if self._running:
                self._do_flush()

    def _do_flush(self):
        # Swap under lock (fast — just pointer swap)
        with self._lock:
            flush_buffer = self._active_buffer
            if self._active_buffer is self._buffer_a:
                self._buffer_b = _empty_buffer()
                self._active_buffer = self._buffer_b
            else:
                self._buffer_a = _empty_buffer()
                self._active_buffer = self._buffer_a

        start = time.monotonic()
        total_rows = 0

        for category in CATEGORIES:
            rows = flush_buffer[category]
            if not rows:
                continue

            try:
                table = pa.Table.from_pylist(rows, schema=TICK_SCHEMA)
                chunk_num = self._chunk_counts[category]
                chunk_path = self.today_dir / f"{category}_{chunk_num:04d}.parquet"
                pq.write_table(table, chunk_path, compression="zstd")

                # fsync to ensure data is on disk
                fd = os.open(str(chunk_path), os.O_RDONLY)
                os.fsync(fd)
                os.close(fd)

                self._chunk_counts[category] += 1
                total_rows += len(rows)

            except Exception as e:
                logger.error(f"Flush failed for {category}: {e}", exc_info=True)

        elapsed = time.monotonic() - start

        self.total_ticks_flushed += total_rows
        self.total_flush_count += 1
        self.last_flush_time = datetime.now(IST)
        self.last_flush_duration = elapsed
        self.last_flush_rows = total_rows

        if total_rows > 0:
            logger.debug(
                f"Flush #{self.total_flush_count}: "
                f"{total_rows} rows in {elapsed:.3f}s"
            )

    def get_stats(self) -> dict:
        buffer_size = 0
        with self._lock:
            for cat in CATEGORIES:
                buffer_size += len(self._active_buffer[cat])

        return {
            "total_ticks_received": self.total_ticks_received,
            "total_ticks_flushed": self.total_ticks_flushed,
            "pending_in_buffer": buffer_size,
            "total_flushes": self.total_flush_count,
            "last_flush_time": self.last_flush_time,
            "last_flush_duration_ms": round(self.last_flush_duration * 1000, 1),
            "last_flush_rows": self.last_flush_rows,
            "chunks_written": dict(self._chunk_counts),
        }

    def consolidate(self):
        logger.info(f"Consolidating chunks for {self.today}...")

        for category in CATEGORIES:
            chunks = sorted(self.today_dir.glob(f"{category}_*.parquet"))
            if not chunks:
                continue

            # Read and concatenate all chunks
            tables = []
            for chunk in chunks:
                try:
                    tables.append(pq.read_table(chunk))
                except Exception as e:
                    logger.error(f"Failed to read chunk {chunk}: {e}")

            if not tables:
                continue

            merged = pa.concat_tables(tables)

            # Sort by exchange_timestamp
            indices = merged.column("exchange_timestamp").to_pylist()
            sort_order = sorted(range(len(indices)), key=lambda i: indices[i])
            merged = merged.take(sort_order)

            # Write consolidated file
            final_path = self.today_dir / f"{category}.parquet"
            pq.write_table(merged, final_path, compression="zstd")

            # Remove chunks
            for chunk in chunks:
                chunk.unlink()

            logger.info(
                f"  {category}: {len(chunks)} chunks → "
                f"{final_path.name} ({len(merged)} rows, "
                f"{final_path.stat().st_size / 1024 / 1024:.1f} MB)"
            )

        logger.info("Consolidation complete")
