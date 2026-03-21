"""
OniQuant v6.0 — Calibration Worker (Weekly Auto-Tuner)
========================================================
Runs every Sunday at market close. Calculates Brier Score,
detects bias per desk, and auto-adjusts likelihood multipliers.

Deploy: Railway cron — railway run python -m workers.calibration_worker
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import asyncpg
import orjson
import redis.asyncio as aioredis
import structlog

log = structlog.get_logger("oniquant.calibration")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@timescaledb:5432/oniquant")
CONFIG_KEY = "oniquant:config:orchestrator_params"


async def compute_brier_score(conn: asyncpg.Connection, lookback_days: int = 7) -> dict[str, Any]:
    """
    Compute the Brier Score per desk over the last week.

    Brier Score = (1/N) × Σ (predicted_prob - actual_outcome)²

    Perfect calibration → Brier = 0.0
    Random guessing    → Brier = 0.25
    """
    rows = await conn.fetch("""
        SELECT
            desk_id,
            posterior_prob,
            CASE
                WHEN (signal_direction = 1 AND simulated_fill_price >= target_price)
                  OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                THEN 1.0 ELSE 0.0
            END AS actual_outcome
        FROM signal_ledger
        WHERE ts >= NOW() - make_interval(days => $1)
          AND posterior_prob IS NOT NULL
          AND simulated_fill_price IS NOT NULL
    """, lookback_days)

    if not rows:
        return {"brier_scores": {}, "n_signals": 0}

    # Group by desk
    desks: dict[str, list[tuple[float, float]]] = {}
    for row in rows:
        desk = row["desk_id"]
        desks.setdefault(desk, []).append(
            (float(row["posterior_prob"]), float(row["actual_outcome"]))
        )

    brier_scores = {}
    for desk, pairs in desks.items():
        n = len(pairs)
        brier = sum((pred - actual) ** 2 for pred, actual in pairs) / n
        predicted_avg = sum(p for p, _ in pairs) / n
        actual_avg = sum(a for _, a in pairs) / n
        bias = predicted_avg - actual_avg

        brier_scores[desk] = {
            "brier_score": round(brier, 6),
            "predicted_avg_winrate": round(predicted_avg, 4),
            "actual_winrate": round(actual_avg, 4),
            "bias": round(bias, 4),
            "n_signals": n,
        }

    return {"brier_scores": brier_scores, "n_signals": len(rows)}


async def compute_tuning_adjustments(brier_data: dict[str, Any]) -> dict[str, float]:
    """
    Calculate likelihood multiplier adjustments based on calibration bias.

    If a desk's predictions are over-confident (bias > 0.05), reduce
    the IOF multiplier. If under-confident (bias < -0.05), increase it.

    Adjustment = clamp(1.0 - bias × 2.0, 0.80, 1.20)
    """
    adjustments = {}
    for desk, stats in brier_data.get("brier_scores", {}).items():
        bias = stats["bias"]
        if abs(bias) > 0.05:
            # Scale factor: 1.0 for perfect calibration,
            # <1.0 for over-confident, >1.0 for under-confident
            factor = max(0.80, min(1.20, 1.0 - bias * 2.0))
            adjustments[desk] = round(factor, 4)
            await log.ainfo(
                "calibration_adjustment",
                desk=desk,
                bias=bias,
                adjustment_factor=factor,
            )
    return adjustments


async def apply_adjustments(
    pool: aioredis.Redis,
    adjustments: dict[str, float],
) -> None:
    """Publish updated multiplier adjustments to Redis."""
    current = await pool.get(CONFIG_KEY)
    config = orjson.loads(current) if current else {}

    config["likelihood_adjustments"] = adjustments
    config["calibrated_at"] = time.time()

    await pool.set(CONFIG_KEY, orjson.dumps(config), ex=604800)  # 7-day TTL
    await log.ainfo("calibration_published", adjustments=adjustments)


async def main() -> None:
    conn = await asyncpg.connect(DATABASE_URL)
    pool = aioredis.from_url(REDIS_URL, decode_responses=False)

    try:
        brier_data = await compute_brier_score(conn, lookback_days=7)
        await log.ainfo("brier_scores_computed", data=brier_data)

        if brier_data["n_signals"] > 0:
            adjustments = await compute_tuning_adjustments(brier_data)
            if adjustments:
                await apply_adjustments(pool, adjustments)

        await log.ainfo("calibration_complete")
    finally:
        await conn.close()
        await pool.aclose()


if __name__ == "__main__":
    asyncio.run(main())
