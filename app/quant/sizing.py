"""
OniQuant v6.0 — Portfolio Sizing Engine (Fractional Kelly Criterion)
======================================================================
Calculates position sizes using the Asymmetric Kelly Criterion
with Quarter-Kelly safety and correlation-based cluster risk reduction.

Kelly Formula:
    f* = (p × b - q × loss) / (b × loss)

Where:
    p    = Probability of win (from Bayesian posterior)
    q    = 1 - p (probability of loss)
    b    = Average win amount (realized win/loss ratio)
    loss = Average loss amount (normalized to 1.0)

Safety:
    - Quarter-Kelly: actual_size = 0.25 × f*
    - Hard cap: no position > 5% of net liquidation value
    - Cluster risk: reduce by 50% if correlated position exists
"""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from app.core.config import get_settings
from app.core.database import get_pool

log = structlog.get_logger("oniquant.sizing")

# Hard limits
QUARTER_KELLY: float = 0.25       # Safety fraction of full Kelly
MAX_POSITION_PCT: float = 0.05    # 5% max of net liquidation
CLUSTER_REDUCTION: float = 0.50   # 50% reduction for correlated positions


# ---------------------------------------------------------------------------
# 1. Kelly Criterion Calculation
# ---------------------------------------------------------------------------

def calculate_kelly_fraction(
    win_probability: float,
    win_loss_ratio: float,
) -> float:
    """
    Compute the Asymmetric Kelly Criterion optimal fraction.

    f* = (p × b - q) / b

    Simplified form where loss = 1.0 (normalized):
        f* = (p × b - (1-p)) / b
        f* = p - q/b

    Args:
        win_probability: P(win) from Bayesian posterior [0, 1].
        win_loss_ratio: Realized average_win / average_loss (b > 0).

    Returns:
        Kelly fraction f* (can be negative → don't trade).
    """
    if win_loss_ratio <= 0:
        return 0.0

    p = win_probability
    q = 1.0 - p
    b = win_loss_ratio

    # f* = (p × b - q × 1.0) / (b × 1.0) = (p × b - q) / b
    kelly = (p * b - q) / b

    return max(0.0, kelly)  # never return negative (don't short-Kelly)


async def get_win_loss_ratio(
    asset_symbol: str,
    desk_id: str,
    lookback_days: int = 30,
) -> float:
    """
    Query the realized win/loss ratio from signal_ledger.

    avg_win  = mean absolute profit on winning trades
    avg_loss = mean absolute loss on losing trades
    ratio = avg_win / avg_loss
    """
    pool = get_pool()
    row = await pool.fetchrow("""
        SELECT
            AVG(ABS(simulated_fill_price - target_price))
                FILTER (WHERE
                    (signal_direction = 1 AND simulated_fill_price >= target_price)
                    OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                ) AS avg_win,
            AVG(ABS(simulated_fill_price - target_price))
                FILTER (WHERE
                    NOT (
                        (signal_direction = 1 AND simulated_fill_price >= target_price)
                        OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                    )
                ) AS avg_loss
        FROM signal_ledger
        WHERE asset_symbol = $1 AND desk_id = $2
          AND ts >= NOW() - make_interval(days => $3)
          AND simulated_fill_price IS NOT NULL
          AND signal_direction != 0
    """, asset_symbol, desk_id, lookback_days)

    if row is None:
        return 1.0  # neutral default

    avg_win = float(row["avg_win"]) if row["avg_win"] else 0.0
    avg_loss = float(row["avg_loss"]) if row["avg_loss"] else 0.0

    if avg_loss <= 0:
        return 2.0  # no losses → optimistic default (capped by Quarter-Kelly)

    return avg_win / avg_loss


# ---------------------------------------------------------------------------
# 2. Correlation / Cluster Risk Check
# ---------------------------------------------------------------------------

# Asset class correlation groups — positions within the same group
# are considered "correlated" for cluster risk reduction.
CORRELATION_GROUPS: dict[str, str] = {
    "SPY": "us_large_cap", "QQQ": "us_large_cap", "AAPL": "us_tech",
    "MSFT": "us_tech", "NVDA": "us_tech", "TSLA": "us_tech",
    "EURUSD": "forex_major", "GBPUSD": "forex_major", "USDJPY": "forex_major",
    "GC_F": "metals", "SI_F": "metals",
    "BTCUSDT": "crypto_major", "ETHUSDT": "crypto_major",
    "SOLUSDT": "crypto_alt", "AVAXUSDT": "crypto_alt", "ARBUSDT": "crypto_alt",
}


async def check_cluster_risk(
    asset_symbol: str,
    redis_pool: aioredis.Redis,
) -> bool:
    """
    Check if there's an existing position in the same correlation group.

    Returns True if a correlated position exists (reduce size by 50%).
    """
    group = CORRELATION_GROUPS.get(asset_symbol)
    if not group:
        return False

    # Check active positions in Redis
    active_positions = await redis_pool.smembers("oniquant:active_positions")
    if not active_positions:
        return False

    for pos_bytes in active_positions:
        try:
            pos = orjson.loads(pos_bytes) if isinstance(pos_bytes, bytes) else {}
            pos_symbol = pos.get("asset_symbol", "")
            if CORRELATION_GROUPS.get(pos_symbol) == group and pos_symbol != asset_symbol:
                return True
        except (orjson.JSONDecodeError, TypeError):
            continue

    return False


# ---------------------------------------------------------------------------
# 3. Main Sizing Function
# ---------------------------------------------------------------------------

async def calculate_position_size(
    asset_symbol: str,
    desk_id: str,
    posterior_probability: float,
    net_liquidation: float,
    redis_pool: aioredis.Redis,
    min_order_qty: float = 1.0,
    tick_size: Decimal = Decimal("0.01"),
) -> dict[str, Any]:
    """
    Calculate the final position size for an authorized signal.

    Pipeline:
        1. Query win/loss ratio from TimescaleDB.
        2. Compute full Kelly fraction.
        3. Apply Quarter-Kelly safety.
        4. Apply 5% hard cap.
        5. Check cluster risk → reduce 50% if correlated position exists.
        6. Round to broker's minOrderQty and tickSize.

    Args:
        asset_symbol: Instrument symbol.
        desk_id: Desk identifier.
        posterior_probability: Bayesian posterior (used as win probability).
        net_liquidation: Total account value.
        redis_pool: Redis connection for cluster risk check.
        min_order_qty: Broker's minimum order quantity.
        tick_size: Broker's tick size (Decimal for 2026 IBKR compliance).

    Returns:
        Dict with kelly_fraction, position_pct, dollar_amount, quantity, adjustments.
    """
    # Step 1: Win/Loss ratio
    wl_ratio = await get_win_loss_ratio(asset_symbol, desk_id)

    # Step 2: Full Kelly
    full_kelly = calculate_kelly_fraction(posterior_probability, wl_ratio)

    # Step 3: Quarter-Kelly
    quarter_kelly = full_kelly * QUARTER_KELLY

    # Step 4: Hard cap at 5%
    position_pct = min(quarter_kelly, MAX_POSITION_PCT)

    # Step 5: Cluster risk
    adjustments = []
    is_clustered = await check_cluster_risk(asset_symbol, redis_pool)
    if is_clustered:
        position_pct *= CLUSTER_REDUCTION
        adjustments.append(f"cluster_risk_reduction: ×{CLUSTER_REDUCTION}")

    # Step 6: Dollar amount and quantity
    dollar_amount = net_liquidation * position_pct

    # Round quantity to min_order_qty
    if min_order_qty > 0:
        quantity = max(min_order_qty, round(dollar_amount / 100, 0))  # approximate
    else:
        quantity = 0.0

    await log.ainfo(
        "position_sized",
        symbol=asset_symbol,
        full_kelly=round(full_kelly, 6),
        quarter_kelly=round(quarter_kelly, 6),
        final_pct=round(position_pct, 6),
        dollar_amount=round(dollar_amount, 2),
        wl_ratio=round(wl_ratio, 4),
        clustered=is_clustered,
    )

    return {
        "asset_symbol": asset_symbol,
        "kelly_fraction": round(full_kelly, 6),
        "quarter_kelly": round(quarter_kelly, 6),
        "position_pct": round(position_pct, 6),
        "dollar_amount": round(dollar_amount, 2),
        "quantity": quantity,
        "win_loss_ratio": round(wl_ratio, 4),
        "win_probability": round(posterior_probability, 6),
        "clustered": is_clustered,
        "adjustments": adjustments,
    }
