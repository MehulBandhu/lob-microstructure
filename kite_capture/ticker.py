import logging
import time
from datetime import datetime
from typing import Optional, Callable
from zoneinfo import ZoneInfo

from kiteconnect import KiteTicker

import config

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

MAX_CONSECUTIVE_FAILURES = 10
BACKOFF_INITIAL_SECONDS = 5
BACKOFF_MAX_SECONDS = 120
BACKOFF_MULTIPLIER = 2

class ConnectionState:

    def __init__(self, name: str):
        self.name = name
        self.connected = False
        self.ever_connected = False
        self.last_tick_time: Optional[datetime] = None
        self.ticks_received = 0
        self.reconnect_count = 0
        self.consecutive_failures = 0
        self.last_error: Optional[str] = None
        self.connect_time: Optional[datetime] = None
        self.circuit_open = False

    def on_connect(self):
        self.connected = True
        self.ever_connected = True
        self.consecutive_failures = 0
        self.circuit_open = False
        self.connect_time = datetime.now(IST)

    def on_disconnect(self):
        self.connected = False

    def on_tick(self, count: int = 1):
        self.last_tick_time = datetime.now(IST)
        self.ticks_received += count

    def on_reconnect(self):
        self.reconnect_count += 1

    def on_failure(self, error: str):
        self.last_error = error
        self.consecutive_failures += 1
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            self.circuit_open = True

    def on_error(self, error: str):
        self.last_error = error

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "connected": self.connected,
            "ever_connected": self.ever_connected,
            "last_tick_time": self.last_tick_time,
            "ticks_received": self.ticks_received,
            "reconnect_count": self.reconnect_count,
            "consecutive_failures": self.consecutive_failures,
            "circuit_open": self.circuit_open,
            "last_error": self.last_error,
            "connect_time": self.connect_time,
        }

class TickerManager:

    def __init__(
        self,
        api_key: str,
        access_token: str,
        on_tick_callback: Callable,
    ):
        self.api_key = api_key
        self.access_token = access_token
        self.on_tick_callback = on_tick_callback

        self.ticker: Optional[KiteTicker] = None
        self.state_1 = ConnectionState("conn_1")
        self.state_2 = ConnectionState("conn_2")
        self.all_tokens: list[int] = []

        self._running = False
        self._started = False

    def start(self, tokens_1: list[int], tokens_2: list[int]):
        self.all_tokens = tokens_1 + tokens_2
        total = len(self.all_tokens)

        if total > config.MAX_TOKENS_PER_CONNECTION:
            logger.error(
                f"Total tokens ({total}) exceeds single connection limit "
                f"({config.MAX_TOKENS_PER_CONNECTION})! "
                f"Reduce instruments or implement multi-process connections."
            )
            # Truncate to limit rather than crash
            self.all_tokens = self.all_tokens[:config.MAX_TOKENS_PER_CONNECTION]
            logger.warning(
                f"Truncated to {len(self.all_tokens)} tokens"
            )

        self._running = True
        logger.info(f"Ticker prepared with {len(self.all_tokens)} tokens (single connection)")

    def run_blocking(self):
        backoff = BACKOFF_INITIAL_SECONDS

        while self._running:
            if self.state_1.circuit_open:
                logger.critical(
                    f"CIRCUIT BREAKER OPEN - "
                    f"{self.state_1.consecutive_failures} consecutive failures. "
                    f"Last error: {self.state_1.last_error}"
                )
                if not self.state_1.ever_connected:
                    logger.critical(
                        "Never connected successfully. "
                        "Possible causes: market holiday, token expired, "
                        "network issue, or Kite API down."
                    )
                break

            self.ticker = self._create_ticker()

            try:
                logger.info(f"Attempting connection ({len(self.all_tokens)} tokens)...")
                self.ticker.connect(threaded=False)
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received, stopping...")
                self._running = False
                break
            except Exception as e:
                self.state_1.on_failure(str(e))
                logger.error(
                    f"Connection failed "
                    f"(attempt {self.state_1.consecutive_failures}/"
                    f"{MAX_CONSECUTIVE_FAILURES}): {e}"
                )

            if not self._running:
                break

            if self.state_1.circuit_open:
                continue

            # a real disconnect — use shorter backoff
            if self.state_1.ever_connected:
                backoff = BACKOFF_INITIAL_SECONDS

            logger.info(
                f"Retrying in {backoff}s "
                f"(failure {self.state_1.consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})..."
            )
            time.sleep(backoff)
            backoff = min(backoff * BACKOFF_MULTIPLIER, BACKOFF_MAX_SECONDS)

    def _create_ticker(self) -> KiteTicker:
        ticker = KiteTicker(
            self.api_key,
            self.access_token,
            reconnect=True,
            reconnect_max_tries=10,
            reconnect_max_delay=60,
        )

        tokens = self.all_tokens

        def on_connect(ws, response):
            self.state_1.on_connect()
            # Mirror to state_2 for monitor compatibility
            self.state_2.connected = True
            self.state_2.ever_connected = True
            self.state_2.connect_time = datetime.now(IST)

            logger.info(f"Connected! Subscribing {len(tokens)} tokens...")
            batch_size = 500
            for i in range(0, len(tokens), batch_size):
                batch = tokens[i : i + batch_size]
                ws.subscribe(batch)
                ws.set_mode(ws.MODE_FULL, batch)
                logger.info(
                    f"Subscribed batch {i // batch_size + 1}: "
                    f"{len(batch)} tokens"
                )
                if i + batch_size < len(tokens):
                    time.sleep(0.5)
            logger.info(f"All {len(tokens)} tokens subscribed in FULL mode")

        def on_ticks(ws, ticks):
            self.state_1.on_tick(len(ticks))
            self.state_2.ticks_received = self.state_1.ticks_received
            self.state_2.last_tick_time = self.state_1.last_tick_time
            for tick in ticks:
                try:
                    self.on_tick_callback(tick)
                except Exception as e:
                    logger.error(f"Tick callback error: {e}")

        def on_close(ws, code, reason):
            self.state_1.on_disconnect()
            self.state_2.connected = False
            logger.warning(f"Connection closed: code={code}, reason={reason}")

        def on_error(ws, code, reason):
            self.state_1.on_error(f"code={code}, reason={reason}")
            logger.error(f"Connection error: code={code}, reason={reason}")

        def on_reconnect(ws, attempts):
            self.state_1.on_reconnect()
            logger.info(f"KiteTicker reconnecting... attempt {attempts}")

        def on_noreconnect(ws):
            logger.warning("KiteTicker internal retries exhausted.")
            # Our outer loop in run_blocking() will handle this

        ticker.on_connect = on_connect
        ticker.on_ticks = on_ticks
        ticker.on_close = on_close
        ticker.on_error = on_error
        ticker.on_reconnect = on_reconnect
        ticker.on_noreconnect = on_noreconnect

        return ticker

    def stop(self):
        self._running = False
        if self.ticker:
            try:
                self.ticker.close()
            except Exception:
                pass
        # Stop Twisted reactor to unblock run_blocking()
        try:
            from twisted.internet import reactor
            if reactor.running:
                reactor.callFromThread(reactor.stop)
        except Exception:
            pass
        logger.info("Ticker connection stopped")

    def is_healthy(self) -> bool:
        return self.state_1.connected and self.state_1.ticks_received > 0

    def is_circuit_open(self) -> bool:
        return self.state_1.circuit_open

    def get_health(self) -> dict:
        return {
            "connection_1": self.state_1.to_dict(),
            "connection_2": self.state_2.to_dict(),
        }
