"""
OniQuant v6.0 — Phase 4: Trade Memory Engine (Asynchronous Worker)
===================================================================
Manages delayed/pending trade signals via Redis Sorted Set (ZSET).
Promotes matured signals to the execution stream; purges expired ones.

Deploy: Railway.app background worker process.
    railway run python memory_worker.py

Architecture:
    ZSET `oniquant:pending_trades`
        score  = UNIX expiration timestamp (signal TTL)
        member = orjson-encoded trade payload

    Stream `oniquant:active_executions`
        XADD target for signals that cross their target price before expiry.
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
# Structured Logger (mirrors main.py config)
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(serializer=orjson.dumps),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        int(os.getenv("LOG_LEVEL", structlog.logging.DEBUG)),
    ),
    cache_logger_on_first_use=True,
)
log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.memory_worker")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PENDING_ZSET: str = "oniquant:pending_trades"
EXEC_STREAM: str = "oniquant:active_executions"
EXEC_STREAM_MAXLEN: int = int(os.getenv("EXEC_STREAM_MAXLEN", "25000"))

# Polling interval in seconds — trades for scanning pending set.
# 100ms gives sub-second promotion latency without hammering Redis.
POLL_INTERVAL: float = float(os.getenv("POLL_INTERVAL", "0.1"))

# Batch size for ZRANGEBYSCORE per poll cycle
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))


# ---------------------------------------------------------------------------
# Price Oracle — Placeholder for Live Market Data Feed
# ---------------------------------------------------------------------------
async def get_current_price(symbol: str) -> float:
    """
    Placeholder: Returns the current market price for `symbol`.

    In production, this resolves to one of:
        - Alpaca Market Data WebSocket (real-time last trade)
        - Redis cache populated by a dedicated market data worker
        - TimescaleDB last() continuous aggregate

    Current implementation returns a static mock price for development.
    Replace with actual market data connector before live deployment.
    """
    # Mock prices — deterministic for integration testing
    _mock_prices: dict[str, float] = {
        "SPY": 452.30,
        "QQQ": 385.15,
        "BTC/USD": 43250.00,
        "ETH/USD": 2280.50,
        "ES_F": 4520.00,
    }
    return _mock_prices.get(symbol, 100.00)


# ---------------------------------------------------------------------------
# Trade Memory Engine — Core Worker
# ---------------------------------------------------------------------------
class TradeMemoryEngine:
    """
    Asynchronous worker that manages the pending trade lifecycle.

    Lifecycle of a pending trade:
        1. Signal arrives → scored into ZSET with expiration timestamp.
        2. Poll loop checks current price vs. target price.
        3a. Price crossed target → XADD to execution stream, ZREM from ZSET.
        3b. Expiration passed → ZREMRANGEBYSCORE purges stale signals.

    The ZSET score-as-expiry pattern gives us O(log N) inserts and
    O(log N + M) range scans where M = number of expired entries.
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._redis_url = redis_url
        self._pool: aioredis.Redis | None = None
        self._running: bool = False
        # Counters for observability
        self._promoted: int = 0
        self._purged: int = 0
        self._cycles: int = 0

    async def connect(self) -> None:
        """Initialize the Redis connection pool with hiredis acceleration."""
        self._pool = aioredis.from_url(
            self._redis_url,
            decode_responses=False,  # orjson works with bytes natively
            max_connections=int(os.getenv("REDIS_MAX_CONN", "10")),
            socket_connect_timeout=5.0,
            socket_keepalive=True,
            retry_on_timeout=True,
        )
        await self._pool.ping()
        await log.ainfo(
            "memory_engine_connected",
            redis=self._redis_url,
            zset=PENDING_ZSET,
            stream=EXEC_STREAM,
        )

    async def disconnect(self) -> None:
        """Drain and close the Redis connection pool."""
        if self._pool is not None:
            await self._pool.aclose()
            self._pool = None
        await log.ainfo(
            "memory_engine_disconnected",
            promoted=self._promoted,
            purged=self._purged,
            cycles=self._cycles,
        )

    # ------------------------------------------------------------------
    # Step 1: Purge Expired Signals
    # ------------------------------------------------------------------
    async def _purge_expired(self, now: float) -> int:
        """
        Remove all ZSET members whose score (expiration) <= current time.

        ZREMRANGEBYSCORE is O(log N + M) where M = number removed.
        This runs BEFORE the price check to avoid wasting cycles on
        already-dead signals.

        Returns count of purged entries.
        """
        removed: int = await self._pool.zremrangebyscore(
            PENDING_ZSET,
            min="-inf",
            max=now,
        )
        if removed > 0:
            self._purged += removed
            await log.ainfo("signals_purged", count=removed, total_purged=self._purged)
        return removed

    # ------------------------------------------------------------------
    # Step 2: Scan Active Pending Trades
    # ------------------------------------------------------------------
    async def _scan_pending(self, now: float) -> None:
        """
        Fetch non-expired pending trades and check price crossings.

        Uses ZRANGEBYSCORE with score range (now, +inf) to get only
        trades that haven't expired yet. Limits to BATCH_SIZE per cycle
        to bound per-iteration latency.

        For each trade:
            - Deserialize payload (orjson, zero-copy).
            - Fetch current market price.
            - If price has crossed the target → promote to execution stream.
        """
        # Fetch pending trades scored ABOVE current time (not yet expired)
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

        # Pipeline promotions for batch efficiency
        pipe = self._pool.pipeline(transaction=False)
        promoted_members: list[bytes] = []

        for member_bytes, expiry_score in pending:
            try:
                payload: dict[str, Any] = orjson.loads(member_bytes)
            except orjson.JSONDecodeError:
                # Corrupted payload — remove it, log, and continue
                await log.awarning("corrupt_payload", raw=member_bytes[:200])
                pipe.zrem(PENDING_ZSET, member_bytes)
                continue

            symbol: str = payload.get("asset_symbol", "UNKNOWN")
            target: float = payload.get("target_price", 0.0)
            direction: int = payload.get("signal_direction", 1)

            # Fetch current market price
            current_price = await get_current_price(symbol)

            # Price crossing logic:
            #   LONG  (direction=1):  current >= target → promote
            #   SHORT (direction=-1): current <= target → promote
            crossed = (
                (direction == 1 and current_price >= target)
                or (direction == -1 and current_price <= target)
            )

            if crossed:
                # Enrich payload with execution metadata
                payload["promoted_at"] = now
                payload["market_price_at_promotion"] = current_price
                payload["slippage"] = abs(current_price - target)

                # XADD to execution stream (fire-and-forget in pipeline)
                pipe.xadd(
                    EXEC_STREAM,
                    {"payload": orjson.dumps(payload), "source": b"memory_engine"},
                    maxlen=EXEC_STREAM_MAXLEN,
                    approximate=True,
                )
                # Mark for removal from ZSET
                promoted_members.append(member_bytes)

        # Remove promoted trades from ZSET in batch
        if promoted_members:
            pipe.zrem(PENDING_ZSET, *promoted_members)

        # Execute pipeline (single round-trip for all operations)
        await pipe.execute()

        if promoted_members:
            self._promoted += len(promoted_members)
            await log.ainfo(
                "signals_promoted",
                count=len(promoted_members),
                total_promoted=self._promoted,
            )

    # ------------------------------------------------------------------
    # Main Run Loop
    # ------------------------------------------------------------------
    async def run(self) -> None:
        """
        Continuous polling loop — runs indefinitely on Railway.

        Each cycle:
            1. Purge expired signals (ZREMRANGEBYSCORE).
            2. Scan pending trades for price crossings.
            3. Sleep for POLL_INTERVAL.

        Exception handling wraps each cycle independently so a single
        Redis timeout or decode error doesn't crash the worker.
        """
        self._running = True
        await log.ainfo("memory_engine_started", poll_interval=POLL_INTERVAL)

        while self._running:
            try:
                now = time.time()

                # Phase 1: Purge stale signals
                await self._purge_expired(now)

                # Phase 2: Price-check and promote
                await self._scan_pending(now)

                self._cycles += 1

                # Periodic health log every 1000 cycles (~100s at 100ms interval)
                if self._cycles % 1000 == 0:
                    await log.ainfo(
                        "memory_engine_heartbeat",
                        cycles=self._cycles,
                        promoted=self._promoted,
                        purged=self._purged,
                        pending=await self._pool.zcard(PENDING_ZSET),
                    )

            except aioredis.ConnectionError as e:
                # Redis connection lost — back off and retry
                await log.aerror("redis_connection_lost", error=str(e))
                await asyncio.sleep(2.0)

            except aioredis.TimeoutError:
                # Redis timeout — skip this cycle, retry next
                await log.awarning("redis_timeout", cycle=self._cycles)

            except Exception as e:
                # Catch-all: log and continue — never crash the worker
                await log.aerror(
                    "memory_engine_error",
                    error=str(e),
                    error_type=type(e).__name__,
                    cycle=self._cycles,
                )
                await asyncio.sleep(1.0)

            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        """Signal the run loop to exit gracefully."""
        self._running = False
        # Synchronous log since we may be in a signal handler context
        print(f"[memory_engine] shutdown requested (cycles={self._cycles})")


# ---------------------------------------------------------------------------
# Entrypoint — Graceful Signal Handling for Railway
# ---------------------------------------------------------------------------

async def main() -> None:
    """
    Bootstrap the Trade Memory Engine with SIGTERM/SIGINT handling.

    Railway sends SIGTERM on deploy/restart. We trap it to ensure
    the Redis pool drains cleanly before the container exits.
    """
    engine = TradeMemoryEngine()

    # Register OS signal handlers for graceful shutdown
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
