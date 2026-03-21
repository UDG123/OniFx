"""
OniQuant v6.0 — Phase 4.5: Synthetic Matching Engine
======================================================
Simulates realistic order fills using L2 order book data.
Evaluates Queue Position and Adverse Selection to determine
whether a pending order would have been filled in a real venue.

This replaces broker-side paper trading simulators, which
unrealistically assume instant fills at the limit price.

Mathematical Model:
    An order is filled IFF:
        cumulative_volume_at_price >= (order_size + adverse_selection_buffer)

    Where:
        adverse_selection_buffer = order_size × PENALTY_RATIO (20%)

    This models the real-world phenomenon where:
        1. Your order sits in a queue behind existing resting orders.
        2. Informed flow (adverse selection) causes some queue participants
           to cancel, but you absorb that flow as the naive participant.
        3. The 20% penalty approximates the adverse fill rate observed
           in empirical microstructure studies (Cont, Stoikov & Talreja 2010).

Deploy: Imported by memory_worker.py and broker_tradfi.py.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.matcher")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PENDING_HASH_PREFIX: str = "oniquant:pending_match"
EXEC_STREAM: str = "oniquant:active_executions"
EXEC_STREAM_MAXLEN: int = int(os.getenv("EXEC_STREAM_MAXLEN", "25000"))

# Adverse selection penalty: 20% buffer above order size.
# Derived from empirical LOB studies — the fraction of queue volume
# that represents informed flow acting against the passive side.
PENALTY_RATIO: float = float(os.getenv("ADVERSE_SELECTION_PENALTY", "0.20"))


# ---------------------------------------------------------------------------
# 1. L2 Snapshot Data Structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class L2Level:
    """
    Single price level in the L2 order book.

    Attributes:
        price: Price at this level.
        size: Total resting volume at this level.
        side: "BID" or "ASK".
        position: Queue position index (0 = best bid/ask).
    """
    price: float
    size: float
    side: str
    position: int


@dataclass(slots=True)
class L2Snapshot:
    """
    Full L2 order book snapshot at a point in time.

    Separates bids and asks, sorted by price priority:
        bids: descending by price (best bid first)
        asks: ascending by price (best ask first)
    """
    bids: list[L2Level] = field(default_factory=list)
    asks: list[L2Level] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def from_raw(cls, raw_levels: list[dict[str, Any]]) -> L2Snapshot:
        """
        Construct L2Snapshot from a list of price-level dicts.

        Expected dict format (from broker_tradfi.py or broker_crypto.py):
            {"price": 452.30, "size": 150, "side": "BID", "position": 0}
        """
        bids: list[L2Level] = []
        asks: list[L2Level] = []

        for lvl in raw_levels:
            entry = L2Level(
                price=float(lvl["price"]),
                size=float(lvl["size"]),
                side=lvl["side"].upper(),
                position=int(lvl.get("position", 0)),
            )
            if entry.side == "BID":
                bids.append(entry)
            else:
                asks.append(entry)

        # Ensure price-priority ordering
        bids.sort(key=lambda x: x.price, reverse=True)   # best bid first
        asks.sort(key=lambda x: x.price)                   # best ask first

        return cls(bids=bids, asks=asks)

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> float | None:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2.0
        return None

    @property
    def spread(self) -> float | None:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None


# ---------------------------------------------------------------------------
# 2. Queue Position Model
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class QueuePositionResult:
    """
    Result of queue position evaluation for a pending order.

    Attributes:
        filled: Whether the order would have been filled.
        fill_price: The simulated fill price (weighted by level).
        cumulative_volume: Total volume at/through the order's price level.
        required_volume: Minimum volume needed for fill (order + penalty).
        queue_depth: Number of L2 levels consumed to reach fill threshold.
        adverse_selection_cost: The penalty buffer in shares/contracts.
        reason: Human-readable explanation of the fill/no-fill decision.
    """
    filled: bool
    fill_price: float
    cumulative_volume: float
    required_volume: float
    queue_depth: int
    adverse_selection_cost: float
    reason: str


def evaluate_queue_position(
    order_size: float,
    limit_price: float,
    direction: int,
    l2: L2Snapshot,
    penalty_ratio: float = PENALTY_RATIO,
) -> QueuePositionResult:
    """
    Core queue position and adverse selection evaluation.

    Mathematical Model:
    ────────────────────
    Given:
        Q = order_size (shares/contracts to fill)
        P_limit = limit price
        α = penalty_ratio (adverse selection buffer, default 0.20)
        V_i = volume at L2 level i
        P_i = price at L2 level i

    Required volume for fill:
        V_required = Q + (Q × α) = Q × (1 + α)

    The adverse selection buffer Q×α models the empirical observation
    that passive limit orders face negative expected P&L per fill due to
    informed order flow. By requiring MORE volume to clear at the price
    level, we simulate the reality that:
        - Your order sits behind existing queue participants.
        - Some of those participants are informed and will cancel.
        - The remaining fills disproportionately represent adverse flow.

    Fill condition:
        For a BUY order at P_limit:
            Σ V_i (for all ASK levels where P_i ≤ P_limit) ≥ V_required

        For a SELL order at P_limit:
            Σ V_i (for all BID levels where P_i ≥ P_limit) ≥ V_required

    Fill Price:
        Volume-weighted average price (VWAP) across consumed levels,
        modeling partial fills at multiple price tiers.

    Args:
        order_size: Number of shares/contracts.
        limit_price: Limit price of the pending order.
        direction: 1 = BUY (aggresses into asks), -1 = SELL (aggresses into bids).
        l2: L2Snapshot from the market data feed.
        penalty_ratio: Adverse selection penalty (default 0.20 = 20%).

    Returns:
        QueuePositionResult with fill determination and diagnostics.
    """
    # --- Adverse selection buffer ---
    # This is the mathematical core: we inflate the required volume
    # by (1 + α) to model queue priority and informed flow toxicity.
    adverse_cost = order_size * penalty_ratio
    required_volume = order_size + adverse_cost  # Q × (1 + α)

    # --- Select relevant book side ---
    # BUY orders aggress into the ASK side; SELL into the BID side.
    if direction == 1:
        # BUY: walk up the ask ladder, consuming levels at or below limit
        levels = [lvl for lvl in l2.asks if lvl.price <= limit_price]
    else:
        # SELL: walk down the bid ladder, consuming levels at or above limit
        levels = [lvl for lvl in l2.bids if lvl.price >= limit_price]

    # --- No available levels at this price ---
    if not levels:
        return QueuePositionResult(
            filled=False,
            fill_price=limit_price,
            cumulative_volume=0.0,
            required_volume=required_volume,
            queue_depth=0,
            adverse_selection_cost=adverse_cost,
            reason="no_liquidity_at_price",
        )

    # --- Walk through levels, accumulating volume ---
    # Track VWAP numerator: Σ(P_i × min(V_i, remaining))
    cumulative_volume: float = 0.0
    vwap_numerator: float = 0.0
    depth: int = 0
    remaining = required_volume

    for lvl in levels:
        depth += 1
        consumed = min(lvl.size, remaining)
        cumulative_volume += consumed
        vwap_numerator += lvl.price * consumed
        remaining -= consumed

        if remaining <= 0:
            break

    # --- Fill determination ---
    filled = cumulative_volume >= required_volume

    # --- Calculate fill price (VWAP of consumed volume) ---
    if cumulative_volume > 0:
        fill_price = vwap_numerator / cumulative_volume
    else:
        fill_price = limit_price

    # --- Build result ---
    if filled:
        reason = (
            f"filled: cumulative_vol={cumulative_volume:.1f} >= "
            f"required={required_volume:.1f} "
            f"(order={order_size:.1f} + penalty={adverse_cost:.1f}) "
            f"across {depth} level(s), vwap={fill_price:.4f}"
        )
    else:
        shortfall = required_volume - cumulative_volume
        reason = (
            f"not_filled: cumulative_vol={cumulative_volume:.1f} < "
            f"required={required_volume:.1f}, "
            f"shortfall={shortfall:.1f} across {depth} level(s)"
        )

    return QueuePositionResult(
        filled=filled,
        fill_price=round(fill_price, 8),
        cumulative_volume=cumulative_volume,
        required_volume=required_volume,
        queue_depth=depth,
        adverse_selection_cost=adverse_cost,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# 3. Async Fill Evaluation — Redis Integration
# ---------------------------------------------------------------------------

async def evaluate_market_fill(
    order_params: dict[str, Any],
    l2_snapshot: list[dict[str, Any]],
    redis_pool: aioredis.Redis | None = None,
) -> QueuePositionResult:
    """
    Full synthetic fill evaluation pipeline.

    Steps:
        1. Parse order parameters and L2 snapshot.
        2. Run queue position model with adverse selection.
        3. If FILLED → promote to `oniquant:active_executions` stream
           and remove from `oniquant:pending_match:{order_id}` hash.
        4. If NOT FILLED → leave in pending state for retry on next tick.

    Args:
        order_params: Dict with keys: order_id, symbol, direction, quantity,
                      order_type, limit_price, target_price, desk_id, metadata.
        l2_snapshot: List of L2 level dicts from broker connector.
        redis_pool: Shared Redis connection (optional, creates one if None).

    Returns:
        QueuePositionResult from the matching evaluation.
    """
    # --- Parse inputs ---
    order_id: str = order_params["order_id"]
    symbol: str = order_params["symbol"]
    direction: int = order_params["direction"]
    quantity: float = order_params["quantity"]
    order_type: str = order_params.get("order_type", "LIMIT")
    limit_price: float = order_params.get("limit_price", 0.0)

    # For MARKET orders, use best available price as the limit
    l2 = L2Snapshot.from_raw(l2_snapshot)
    if order_type == "MARKET":
        if direction == 1 and l2.best_ask is not None:
            limit_price = l2.best_ask * 1.005  # 0.5% slippage allowance
        elif direction == -1 and l2.best_bid is not None:
            limit_price = l2.best_bid * 0.995
        else:
            await log.awarning("no_l2_price_for_market_order", order_id=order_id)
            return QueuePositionResult(
                filled=False,
                fill_price=0.0,
                cumulative_volume=0.0,
                required_volume=quantity * (1 + PENALTY_RATIO),
                queue_depth=0,
                adverse_selection_cost=quantity * PENALTY_RATIO,
                reason="no_l2_data_for_market_order",
            )

    # --- Run matching model ---
    result = evaluate_queue_position(
        order_size=quantity,
        limit_price=limit_price,
        direction=direction,
        l2=l2,
    )

    await log.ainfo(
        "fill_evaluation",
        order_id=order_id,
        symbol=symbol,
        filled=result.filled,
        fill_price=result.fill_price,
        queue_depth=result.queue_depth,
        cumulative_vol=result.cumulative_volume,
        required_vol=result.required_volume,
    )

    # --- Redis state transitions ---
    if redis_pool is not None and result.filled:
        now = time.time()

        # Build execution record
        execution_payload: dict[str, Any] = {
            **order_params,
            "fill_price": result.fill_price,
            "fill_time": now,
            "adverse_selection_cost": result.adverse_selection_cost,
            "queue_depth": result.queue_depth,
            "cumulative_volume_at_fill": result.cumulative_volume,
            "l2_mid_price": l2.mid_price,
            "l2_spread": l2.spread,
            "match_reason": result.reason,
        }

        # Atomic pipeline: XADD to execution stream + remove pending hash + remove from index
        pipe = redis_pool.pipeline(transaction=True)  # MULTI/EXEC for atomicity

        # Promote to execution stream
        pipe.xadd(
            EXEC_STREAM,
            {
                "payload": orjson.dumps(execution_payload),
                "source": b"synthetic_matcher",
            },
            maxlen=EXEC_STREAM_MAXLEN,
            approximate=True,
        )

        # Remove from pending match hash
        hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"
        pipe.delete(hash_key)

        # Remove from pending index set
        pipe.srem(f"{PENDING_HASH_PREFIX}:index", order_id)

        await pipe.execute()

        await log.ainfo(
            "order_filled_and_promoted",
            order_id=order_id,
            symbol=symbol,
            fill_price=result.fill_price,
            stream=EXEC_STREAM,
        )

    return result


# ---------------------------------------------------------------------------
# 4. Matcher Worker — Continuous Evaluation Loop
# ---------------------------------------------------------------------------

class SyntheticMatcherWorker:
    """
    Background worker that continuously evaluates pending orders
    against live L2 data.

    Scans the `oniquant:pending_match:index` set for all pending order IDs,
    fetches their parameters from the corresponding hash, requests an L2
    snapshot, and runs the synthetic fill evaluation.

    Orders that are filled get promoted; expired orders get purged.
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._redis_url = redis_url
        self._pool: aioredis.Redis | None = None
        self._running: bool = False
        self._eval_count: int = 0
        self._fill_count: int = 0
        self._purge_count: int = 0

    async def connect(self) -> None:
        self._pool = aioredis.from_url(
            self._redis_url,
            decode_responses=False,
            max_connections=15,
            socket_connect_timeout=5.0,
            socket_keepalive=True,
        )
        await self._pool.ping()
        await log.ainfo("matcher_worker_connected")

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.aclose()
        await log.ainfo(
            "matcher_worker_disconnected",
            evaluations=self._eval_count,
            fills=self._fill_count,
            purges=self._purge_count,
        )

    async def _get_l2_for_symbol(self, symbol: str) -> list[dict[str, Any]]:
        """
        Fetch L2 snapshot for a symbol.

        In production, this reads from a Redis cache populated by:
            - broker_tradfi.py (IBKR Market Depth)
            - broker_crypto.py (Bybit WebSocket orderbook)

        Placeholder returns a synthetic 10-level book.
        """
        # Check Redis L2 cache first
        cache_key = f"oniquant:l2_cache:{symbol}"
        cached = await self._pool.get(cache_key)
        if cached:
            return orjson.loads(cached)

        # Fallback: empty book (order will not fill)
        return []

    async def _process_pending_orders(self) -> None:
        """Scan all pending orders and evaluate fills."""
        now = time.time()

        # Get all pending order IDs from the index set
        order_ids: set[bytes] = await self._pool.smembers(
            f"{PENDING_HASH_PREFIX}:index"
        )

        if not order_ids:
            return

        for oid_bytes in order_ids:
            order_id = oid_bytes.decode() if isinstance(oid_bytes, bytes) else oid_bytes
            hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"

            # Fetch order data
            raw_data = await self._pool.hget(hash_key, "data")
            if raw_data is None:
                # Hash expired or was deleted — clean up index
                await self._pool.srem(f"{PENDING_HASH_PREFIX}:index", order_id)
                continue

            try:
                order_params: dict[str, Any] = orjson.loads(raw_data)
            except orjson.JSONDecodeError:
                await log.awarning("corrupt_order_data", order_id=order_id)
                await self._pool.delete(hash_key)
                await self._pool.srem(f"{PENDING_HASH_PREFIX}:index", order_id)
                continue

            # Check expiration
            if now > order_params.get("expires_at", 0):
                await self._pool.delete(hash_key)
                await self._pool.srem(f"{PENDING_HASH_PREFIX}:index", order_id)
                self._purge_count += 1
                await log.adebug("order_expired", order_id=order_id)
                continue

            # Fetch L2 data for this symbol
            l2_data = await self._get_l2_for_symbol(order_params["symbol"])
            if not l2_data:
                continue  # No L2 data available — skip this cycle

            # Evaluate fill
            result = await evaluate_market_fill(
                order_params=order_params,
                l2_snapshot=l2_data,
                redis_pool=self._pool,
            )

            self._eval_count += 1
            if result.filled:
                self._fill_count += 1

    async def run(self, poll_interval: float = 0.05) -> None:
        """
        Main loop — evaluates pending orders every poll_interval seconds.

        Default 50ms interval gives ~20 evaluation cycles/sec, which
        is sufficient for sub-second fill latency on pending orders.
        """
        self._running = True
        await log.ainfo("matcher_worker_started", poll_interval_ms=poll_interval * 1000)

        while self._running:
            try:
                await self._process_pending_orders()

                # Periodic heartbeat
                if self._eval_count > 0 and self._eval_count % 500 == 0:
                    await log.ainfo(
                        "matcher_heartbeat",
                        evaluations=self._eval_count,
                        fills=self._fill_count,
                        purges=self._purge_count,
                    )

            except aioredis.ConnectionError as e:
                await log.aerror("matcher_redis_lost", error=str(e))
                await asyncio.sleep(2.0)
            except Exception as e:
                await log.aerror(
                    "matcher_error",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                await asyncio.sleep(0.5)

            await asyncio.sleep(poll_interval)

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    import signal

    async def main() -> None:
        worker = SyntheticMatcherWorker()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, worker.stop)
        try:
            await worker.connect()
            await worker.run()
        finally:
            await worker.disconnect()

    asyncio.run(main())
