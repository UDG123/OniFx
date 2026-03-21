"""
OniQuant v6.0 — Trade Memory Engine (Async Background Worker)
===============================================================
Monitors the Redis ZSET `oniquant:pending_trades` for pending signals.
Polls mock price sources, promotes fills to `oniquant:match_validation`,
and auto-purges expired entries based on ZSET score (UNIX timestamp TTL).

Deploy: Railway.app background worker.
    railway run python -m workers.memory_worker

ZSET Schema:
    Key:    oniquant:pending_trades
    Score:  UNIX expiration timestamp (signal TTL deadline)
    Member: orjson-encoded trade payload bytes
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(serializer=orjson.dumps),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        int(os.getenv("LOG_LEVEL", "10")),
    ),
    cache_logger_on_first_use=True,
)
log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.memory_worker")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
PENDING_ZSET: str = "oniquant:pending_trades"
MATCH_STREAM: str = "oniquant:match_validation"
MATCH_STREAM_MAXLEN: int = int(os.getenv("MATCH_STREAM_MAXLEN", "25000"))
POLL_INTERVAL: float = float(os.getenv("POLL_INTERVAL", "0.1"))   # 100ms
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))


# ---------------------------------------------------------------------------
# Mock Price Source
# ---------------------------------------------------------------------------

# In-memory mock prices — replaced by live Alpaca/Bybit feed in production
_MOCK_PRICES: dict[str, float] = {
    "SPY": 452.30,
    "QQQ": 385.15,
    "AAPL": 178.50,
    "MSFT": 374.20,
    "NVDA": 495.00,
    "TSLA": 248.75,
    "BTC/USD": 43250.00,
    "BTCUSDT": 43250.00,
    "ETH/USD": 2280.50,
    "ETHUSDT": 2280.50,
    "SOL/USD": 98.40,
    "SOLUSDT": 98.40,
    "ES_F": 4520.00,
    "NQ_F": 15850.00,
    "GC_F": 2035.50,
}


async def get_current_price(symbol: str) -> float | None:
    """
    Fetch the current market price for a symbol.

    Production replacement targets:
        - Alpaca Market Data v2 (equities/futures)
        - Bybit V5 tickers (crypto)
        - Redis cache populated by dedicated market data workers

    Returns None if the symbol is unknown (prevents false fills).
    """
    return _MOCK_PRICES.get(symbol)


# ---------------------------------------------------------------------------
# Trade Memory Engine
# ---------------------------------------------------------------------------

class TradeMemoryEngine:
    """
    Async worker managing the pending-trade lifecycle via Redis ZSET.

    The ZSET score-as-expiry pattern:
        score  = UNIX timestamp when the signal expires
        member = orjson-encoded payload with trade parameters

    Each poll cycle:
        1. Purge expired signals (score <= now) via ZREMRANGEBYSCORE.
        2. Scan non-expired signals (score > now) via ZRANGEBYSCORE.
        3. For each pending signal:
           a. Fetch current price for the asset.
           b. Compare against target_price respecting direction.
           c. If crossed → XADD to match_validation stream + ZREM from ZSET.
        4. Sleep for POLL_INTERVAL (100ms default).
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._redis_url = redis_url
        self._pool: aioredis.Redis | None = None
        self._running: bool = False

        # Metrics
        self._promoted: int = 0
        self._purged: int = 0
        self._cycles: int = 0

    async def connect(self) -> None:
        self._pool = aioredis.from_url(
            self._redis_url,
            decode_responses=False,
            max_connections=int(os.getenv("REDIS_MAX_CONN", "10")),
            socket_connect_timeout=5.0,
            socket_keepalive=True,
            retry_on_timeout=True,
        )
        await self._pool.ping()
        await log.ainfo("memory_engine_connected", zset=PENDING_ZSET, stream=MATCH_STREAM)

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.aclose()
            self._pool = None
        await log.ainfo(
            "memory_engine_shutdown",
            promoted=self._promoted,
            purged=self._purged,
            cycles=self._cycles,
        )

    # ------------------------------------------------------------------
    # Phase 1: Purge Expired Signals
    # ------------------------------------------------------------------

    async def _purge_expired(self, now: float) -> int:
        """
        Remove all ZSET members with score (expiry) <= current time.

        ZREMRANGEBYSCORE is O(log N + M) where M = entries removed.
        Runs BEFORE price checks to avoid wasting cycles on dead signals.
        """
        removed: int = await self._pool.zremrangebyscore(
            PENDING_ZSET,
            min="-inf",
            max=now,
        )
        if removed > 0:
            self._purged += removed
            await log.ainfo("signals_purged", count=removed, total=self._purged)
        return removed

    # ------------------------------------------------------------------
    # Phase 2: Price Check & Promotion
    # ------------------------------------------------------------------

    async def _scan_and_promote(self, now: float) -> None:
        """
        Scan non-expired pending trades and check price crossings.

        Fetches up to BATCH_SIZE entries with score > now (still alive),
        checks each against current market price, and promotes to the
        match_validation stream if the target is hit.
        """
        # Fetch pending trades that haven't expired yet
        pending: list[tuple[bytes, float]] = await self._pool.zrangebyscore(
            PENDING_ZSET,
            min=now,
            max="+inf",
            start=0,
            num=BATCH_SIZE,
            withscores=True,
        )

        if not pending:
            return

        pipe = self._pool.pipeline(transaction=False)
        promoted_members: list[bytes] = []

        for member_bytes, expiry_score in pending:
            # Deserialize payload
            try:
                payload: dict[str, Any] = orjson.loads(member_bytes)
            except orjson.JSONDecodeError:
                await log.awarning("corrupt_payload_purged", raw=member_bytes[:100])
                pipe.zrem(PENDING_ZSET, member_bytes)
                continue

            symbol: str = payload.get("asset_symbol", "UNKNOWN")
            target: float = payload.get("target_price", 0.0)
            direction: int = payload.get("signal_direction", 1)

            # Fetch current price
            current_price = await get_current_price(symbol)
            if current_price is None:
                continue  # Unknown symbol — skip, don't purge

            # Price crossing check:
            #   LONG  (1):  current >= target → target hit
            #   SHORT (-1): current <= target → target hit
            crossed = (
                (direction == 1 and current_price >= target)
                or (direction == -1 and current_price <= target)
            )

            if crossed:
                # Enrich with promotion metadata
                payload["promoted_at"] = now
                payload["market_price_at_trigger"] = current_price
                payload["slippage_estimate"] = abs(current_price - target)

                # XADD to match_validation stream for Synthetic Matcher
                pipe.xadd(
                    MATCH_STREAM,
                    {"payload": orjson.dumps(payload), "source": b"memory_engine"},
                    maxlen=MATCH_STREAM_MAXLEN,
                    approximate=True,
                )
                promoted_members.append(member_bytes)

        # Batch-remove promoted entries from ZSET
        if promoted_members:
            pipe.zrem(PENDING_ZSET, *promoted_members)

        # Execute pipeline — single Redis round-trip
        await pipe.execute()

        if promoted_members:
            self._promoted += len(promoted_members)
            await log.ainfo(
                "signals_promoted",
                count=len(promoted_members),
                total=self._promoted,
            )

    # ------------------------------------------------------------------
    # Main Loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Continuous poll loop. Each cycle: purge → scan → sleep.

        Per-cycle exception isolation ensures a single Redis timeout
        or corrupt payload never crashes the container.
        """
        self._running = True
        await log.ainfo("memory_engine_started", poll_interval=POLL_INTERVAL)

        while self._running:
            try:
                now = time.time()
                await self._purge_expired(now)
                await self._scan_and_promote(now)
                self._cycles += 1

                # Heartbeat every 1000 cycles (~100s at 100ms)
                if self._cycles % 1000 == 0:
                    pending_count = await self._pool.zcard(PENDING_ZSET)
                    await log.ainfo(
                        "memory_heartbeat",
                        cycles=self._cycles,
                        promoted=self._promoted,
                        purged=self._purged,
                        pending=pending_count,
                    )

            except aioredis.ConnectionError as e:
                await log.aerror("redis_connection_lost", error=str(e))
                await asyncio.sleep(2.0)

            except aioredis.TimeoutError:
                await log.awarning("redis_timeout", cycle=self._cycles)

            except Exception as e:
                await log.aerror(
                    "memory_engine_error",
                    error=str(e),
                    error_type=type(e).__name__,
                    cycle=self._cycles,
                )
                await asyncio.sleep(1.0)

            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def main() -> None:
    engine = TradeMemoryEngine()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, engine.stop)
    try:
        await engine.connect()
        await engine.run()
    finally:
        await engine.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
