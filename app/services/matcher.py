"""
OniQuant v6.0 — Synthetic Matching Engine ("The Liar's Filter")
================================================================
Validates whether a pending trade order would realistically fill
against a live L2 orderbook snapshot.

Core Invariant:
    An order is FILLED if and only if:

        cumulative_volume_at_price > (order_size + 20% adverse selection buffer)

    i.e., V_cum > Q × (1 + α),  where α = 0.20

This replaces broker-side paper trading simulators that unrealistically
assume instant fills at the limit price. Real markets require you to:
    1. Wait in the queue behind existing resting orders.
    2. Absorb adverse selection (informed flow fills against you).

The 20% buffer models the empirical adverse fill rate from
LOB microstructure research (Cont, Stoikov & Talreja 2010).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import orjson
import structlog

from app.core.config import get_settings
from app.core.database import log_simulated_fill

log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.matcher")


# ---------------------------------------------------------------------------
# 1. L2 Data Structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class L2Level:
    """Single price level in the L2 order book."""
    price: float
    size: float
    side: str       # "BID" or "ASK"
    position: int   # 0 = best bid/ask


@dataclass(slots=True)
class L2Snapshot:
    """
    Full L2 orderbook snapshot at a point in time.

    Bids: descending by price (best bid first).
    Asks: ascending by price (best ask first).
    """
    bids: list[L2Level] = field(default_factory=list)
    asks: list[L2Level] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def from_raw(cls, raw_levels: list[dict[str, Any]]) -> L2Snapshot:
        """Construct from a list of price-level dicts."""
        bids, asks = [], []
        for lvl in raw_levels:
            entry = L2Level(
                price=float(lvl["price"]),
                size=float(lvl["size"]),
                side=lvl["side"].upper(),
                position=int(lvl.get("position", 0)),
            )
            (bids if entry.side == "BID" else asks).append(entry)
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)
        return cls(bids=bids, asks=asks)

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> float | None:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2.0
        return None


# ---------------------------------------------------------------------------
# 2. Fill Evaluation Result
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class FillResult:
    """
    Outcome of the synthetic fill evaluation.

    Attributes:
        filled: Whether the order would have been filled.
        simulated_fill_price: VWAP across consumed L2 levels.
        latency_ms: Time taken for the evaluation in milliseconds.
        cumulative_volume: Total volume available at/through price.
        required_volume: Minimum volume for fill (order + penalty).
        queue_depth: Number of L2 levels consumed.
        adverse_buffer: The 20% penalty in absolute volume.
        reason: Human-readable fill/no-fill explanation.
    """
    filled: bool
    simulated_fill_price: float
    latency_ms: float
    cumulative_volume: float
    required_volume: float
    queue_depth: int
    adverse_buffer: float
    reason: str


# ---------------------------------------------------------------------------
# 3. Core Matching Logic — "The Liar's Filter"
# ---------------------------------------------------------------------------

def evaluate_l2_fill(
    order_size: float,
    price: float,
    l2_snapshot: list[dict[str, Any]],
    direction: int = 1,
    penalty_ratio: float | None = None,
) -> FillResult:
    """
    Evaluate whether an order would fill against an L2 orderbook.

    ┌─────────────────────────────────────────────────────────────────┐
    │                    FILL CONDITION                               │
    │                                                                 │
    │    V_cum > Q × (1 + α)                                         │
    │                                                                 │
    │    where:                                                       │
    │      V_cum = Σ V_i  for all levels at/through the limit price  │
    │      Q     = order_size                                         │
    │      α     = adverse_selection_penalty (default 0.20)          │
    │                                                                 │
    │    The 20% buffer (α) models:                                  │
    │      • Queue priority: your order sits behind existing rest     │
    │      • Adverse selection: informed flow fills against you       │
    │      • Partial cancels: some queue participants withdraw        │
    │                                                                 │
    │    Fill Price = VWAP across consumed levels:                    │
    │      P_fill = Σ(P_i × min(V_i, remaining)) / V_consumed       │
    └─────────────────────────────────────────────────────────────────┘

    Args:
        order_size: Number of shares/contracts to fill.
        price: Limit price of the order.
        l2_snapshot: List of L2 level dicts from broker connectors.
        direction: 1 = BUY (aggress into asks), -1 = SELL (aggress into bids).
        penalty_ratio: Override adverse selection penalty (default from config).

    Returns:
        FillResult with fill determination, VWAP, latency, and diagnostics.
    """
    t0 = time.perf_counter()

    if penalty_ratio is None:
        penalty_ratio = get_settings().adverse_selection_penalty

    # --- Parse L2 snapshot ---
    l2 = L2Snapshot.from_raw(l2_snapshot)

    # --- Compute required volume with adverse selection buffer ---
    adverse_buffer = order_size * penalty_ratio
    required_volume = order_size + adverse_buffer  # Q × (1 + α)

    # --- Select relevant book side ---
    if direction == 1:
        # BUY: consume ASK levels at or below limit price
        levels = [lvl for lvl in l2.asks if lvl.price <= price]
    else:
        # SELL: consume BID levels at or above limit price
        levels = [lvl for lvl in l2.bids if lvl.price >= price]

    # --- No liquidity at this price ---
    if not levels:
        elapsed = (time.perf_counter() - t0) * 1000
        return FillResult(
            filled=False,
            simulated_fill_price=price,
            latency_ms=round(elapsed, 3),
            cumulative_volume=0.0,
            required_volume=required_volume,
            queue_depth=0,
            adverse_buffer=adverse_buffer,
            reason="no_liquidity_at_price",
        )

    # --- Walk through levels, accumulating volume ---
    cumulative_volume = 0.0
    vwap_numerator = 0.0
    depth = 0
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
    filled = cumulative_volume > required_volume

    # --- VWAP fill price ---
    fill_price = (vwap_numerator / cumulative_volume) if cumulative_volume > 0 else price

    elapsed = (time.perf_counter() - t0) * 1000

    # --- Build reason ---
    if filled:
        reason = (
            f"FILLED: V_cum={cumulative_volume:.1f} > "
            f"V_req={required_volume:.1f} "
            f"(Q={order_size:.1f} + α={adverse_buffer:.1f}) "
            f"across {depth} level(s), VWAP={fill_price:.6f}"
        )
    else:
        shortfall = required_volume - cumulative_volume
        reason = (
            f"REJECTED: V_cum={cumulative_volume:.1f} ≤ "
            f"V_req={required_volume:.1f}, "
            f"shortfall={shortfall:.1f} across {depth} level(s)"
        )

    return FillResult(
        filled=filled,
        simulated_fill_price=round(fill_price, 8),
        latency_ms=round(elapsed, 3),
        cumulative_volume=cumulative_volume,
        required_volume=required_volume,
        queue_depth=depth,
        adverse_buffer=adverse_buffer,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# 4. Full Pipeline — Evaluate + Persist to TimescaleDB
# ---------------------------------------------------------------------------

async def evaluate_and_persist(
    order_params: dict[str, Any],
    l2_snapshot: list[dict[str, Any]],
) -> FillResult:
    """
    Run L2 fill evaluation and persist the result to signal_ledger.

    This is the main entry point called by the match_validation
    stream consumer. It:
        1. Runs evaluate_l2_fill() for the synthetic fill check.
        2. If filled, logs simulated_fill_price + latency_ms to TimescaleDB.
        3. Returns the FillResult for downstream routing.
    """
    result = evaluate_l2_fill(
        order_size=order_params.get("quantity", 0),
        price=order_params.get("target_price", 0),
        l2_snapshot=l2_snapshot,
        direction=order_params.get("signal_direction", 1),
    )

    await log.ainfo(
        "l2_fill_evaluation",
        symbol=order_params.get("asset_symbol"),
        filled=result.filled,
        fill_price=result.simulated_fill_price,
        latency_ms=result.latency_ms,
        volume=result.cumulative_volume,
        required=result.required_volume,
        depth=result.queue_depth,
    )

    # Persist filled signals to TimescaleDB for performance tracking
    if result.filled:
        try:
            await log_simulated_fill(
                ts=datetime.now(timezone.utc),
                desk_id=order_params.get("desk_id", "unknown"),
                asset_symbol=order_params.get("asset_symbol", "UNKNOWN"),
                signal_direction=order_params.get("signal_direction", 1),
                target_price=order_params.get("target_price", 0),
                simulated_fill_price=result.simulated_fill_price,
                fill_latency_ms=result.latency_ms,
                indicator_metadata=order_params.get("indicator_metadata"),
            )
        except Exception as e:
            # DB write failure must not block the matching pipeline
            await log.aerror("fill_persist_error", error=str(e))

    return result
