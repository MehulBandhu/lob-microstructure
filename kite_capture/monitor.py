import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, date
from typing import Optional
from zoneinfo import ZoneInfo

import config

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

class Monitor:

    def __init__(self, ticker_manager, storage, instrument_manager):
        self.ticker = ticker_manager
        self.storage = storage
        self.im = instrument_manager

        # Per-instrument tick counter (for dead detection)
        self._instrument_ticks: dict[int, int] = defaultdict(int)
        self._lock = threading.Lock()

        # Rate tracking
        self._interval_ticks = 0
        self._last_rate_check = time.monotonic()

        # Running
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def on_tick(self, token: int):
        with self._lock:
            self._instrument_ticks[token] += 1
            self._interval_ticks += 1

    def start(self):
        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name="monitor"
        )
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)

    def _monitor_loop(self):
        while self._running:
            time.sleep(config.HEALTH_INTERVAL_SECONDS)
            if self._running:
                self._print_health()

    def _print_health(self):
        now = datetime.now(IST)
        elapsed = time.monotonic() - self._last_rate_check

        with self._lock:
            ticks_in_interval = self._interval_ticks
            self._interval_ticks = 0
        self._last_rate_check = time.monotonic()

        tick_rate = ticks_in_interval / elapsed if elapsed > 0 else 0

        # Connection health
        health = self.ticker.get_health()
        c1 = health["connection_1"]
        c2 = health["connection_2"]
        c1_status = "✓" if c1["connected"] else ("⊘" if c1["circuit_open"] else "✗")
        c2_status = "✓" if c2["connected"] else ("⊘" if c2["circuit_open"] else "✗")

        # Storage stats
        stats = self.storage.get_stats()

        line = (
            f"[{now.strftime('%H:%M:%S')}] "
            f"rate={tick_rate:,.0f}/s | "
            f"conn1={c1_status} ({c1['ticks_received']:,}) "
            f"conn2={c2_status} ({c2['ticks_received']:,}) | "
            f"buf={stats['pending_in_buffer']:,} "
            f"flush={stats['last_flush_duration_ms']:.0f}ms "
            f"({stats['last_flush_rows']:,} rows) | "
            f"total={stats['total_ticks_received']:,}"
        )
        print(line)

        # Warnings
        if tick_rate < config.MIN_TICK_RATE and tick_rate > 0:
            logger.warning(f"Low tick rate: {tick_rate:.0f}/s")

        if not c1["connected"]:
            logger.warning(
                f"Connection 1 DOWN! "
                f"Reconnects: {c1['reconnect_count']}, "
                f"Last error: {c1['last_error']}"
            )
        if not c2["connected"]:
            logger.warning(
                f"Connection 2 DOWN! "
                f"Reconnects: {c2['reconnect_count']}, "
                f"Last error: {c2['last_error']}"
            )

    def get_dead_instruments(self) -> list[dict]:
        all_tokens = set(self.im.token_map.keys())
        with self._lock:
            active_tokens = set(self._instrument_ticks.keys())

        dead_tokens = all_tokens - active_tokens
        dead_list = []
        for token in sorted(dead_tokens):
            meta = self.im.get_metadata(token)
            dead_list.append({
                "token": token,
                "tradingsymbol": meta.get("tradingsymbol", "UNKNOWN"),
                "category": self.im.get_category(token),
            })

        return dead_list

    def get_low_activity_instruments(self, min_ticks: int = 10) -> list[dict]:
        low_list = []
        with self._lock:
            for token, count in self._instrument_ticks.items():
                if count < min_ticks:
                    meta = self.im.get_metadata(token)
                    low_list.append({
                        "token": token,
                        "tradingsymbol": meta.get("tradingsymbol", "UNKNOWN"),
                        "category": self.im.get_category(token),
                        "tick_count": count,
                    })

        return sorted(low_list, key=lambda x: x["tick_count"])

    def daily_summary(self) -> str:
        stats = self.storage.get_stats()
        health = self.ticker.get_health()
        dead = self.get_dead_instruments()
        low = self.get_low_activity_instruments()

        lines = [
            "",
            "=" * 70,
            f"  DAILY SUMMARY — {date.today()}",
            "=" * 70,
            "",
            "── Tick Capture ──",
            f"  Total ticks received:  {stats['total_ticks_received']:>12,}",
            f"  Total ticks flushed:   {stats['total_ticks_flushed']:>12,}",
            f"  Total flushes:         {stats['total_flushes']:>12,}",
            "",
            "── Chunks Written ──",
        ]

        for cat, count in sorted(stats["chunks_written"].items()):
            lines.append(f"  {cat:<20s}: {count:>6,} chunks")

        lines.extend([
            "",
            "── Connection Stats ──",
            f"  Connection 1: {health['connection_1']['ticks_received']:>12,} ticks, "
            f"{health['connection_1']['reconnect_count']} reconnects",
            f"  Connection 2: {health['connection_2']['ticks_received']:>12,} ticks, "
            f"{health['connection_2']['reconnect_count']} reconnects",
        ])

        # Per-category tick counts
        lines.append("")
        lines.append("── Ticks by Category ──")
        cat_counts = defaultdict(int)
        with self._lock:
            for token, count in self._instrument_ticks.items():
                cat = self.im.get_category(token)
                cat_counts[cat] += count
        for cat, count in sorted(cat_counts.items()):
            lines.append(f"  {cat:<20s}: {count:>12,}")

        # Dead instruments
        lines.append("")
        lines.append("── Dead Instruments (0 ticks) ──")
        if dead:
            lines.append(f"  {len(dead)} instruments received zero ticks:")
            # Group by category
            dead_by_cat = defaultdict(list)
            for d in dead:
                dead_by_cat[d["category"]].append(d["tradingsymbol"])
            for cat, symbols in sorted(dead_by_cat.items()):
                lines.append(f"  {cat} ({len(symbols)}): {', '.join(symbols[:10])}")
                if len(symbols) > 10:
                    lines.append(f"    ... and {len(symbols) - 10} more")
        else:
            lines.append("  None — all instruments received ticks ✓")

        # Low activity
        lines.append("")
        lines.append("── Low Activity (<10 ticks) ──")
        if low:
            lines.append(f"  {len(low)} instruments with <10 ticks")
            for item in low[:20]:
                lines.append(
                    f"  {item['tradingsymbol']:<25s} "
                    f"{item['category']:<18s} "
                    f"{item['tick_count']:>5} ticks"
                )
        else:
            lines.append("  None ✓")

        lines.extend(["", "=" * 70, ""])

        report = "\n".join(lines)
        logger.info(report)

        # Save report to file
        report_path = config.LOG_DIR / f"summary_{date.today()}.txt"
        with open(report_path, "w") as f:
            f.write(report)

        return report
