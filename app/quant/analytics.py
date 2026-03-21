"""
OniQuant v6.0 — Win-Rate Analytics Engine
============================================
Queries signal_ledger in TimescaleDB for performance metrics.
Calculates Win Rate per Desk, Profit Factor, Average Slippage,
and Bayesian Accuracy (calibration of posterior predictions).
"""

from __future__ import annotations

from typing import Any

import asyncpg
import structlog

from app.core.config import get_settings
from app.core.database import get_pool

log = structlog.get_logger("oniquant.analytics")


async def get_desk_performance(lookback_days: int = 30) -> list[dict[str, Any]]:
    """
    Calculate Win Rate, Profit Factor, and Avg Slippage per desk.

    Win = fill direction matches prediction (long filled above target, etc.)
    Profit Factor = gross wins / gross losses
    Avg Slippage = mean(|target_price - simulated_fill_price|)
    """
    pool = get_pool()
    rows = await pool.fetch("""
        SELECT
            desk_id,
            asset_symbol,
            COUNT(*) FILTER (WHERE simulated_fill_price IS NOT NULL) AS total_fills,
            COUNT(*) FILTER (
                WHERE simulated_fill_price IS NOT NULL
                AND (
                    (signal_direction = 1 AND simulated_fill_price >= target_price)
                    OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                )
            ) AS wins,
            COUNT(*) FILTER (
                WHERE simulated_fill_price IS NOT NULL
                AND NOT (
                    (signal_direction = 1 AND simulated_fill_price >= target_price)
                    OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                )
            ) AS losses,
            AVG(ABS(target_price - simulated_fill_price))
                FILTER (WHERE simulated_fill_price IS NOT NULL) AS avg_slippage,
            AVG(fill_latency_ms)
                FILTER (WHERE fill_latency_ms IS NOT NULL) AS avg_latency_ms
        FROM signal_ledger
        WHERE ts >= NOW() - make_interval(days => $1)
          AND signal_direction != 0
        GROUP BY desk_id, asset_symbol
        ORDER BY desk_id, asset_symbol
    """, lookback_days)

    results = []
    for row in rows:
        total = row["total_fills"] or 0
        wins = row["wins"] or 0
        losses = row["losses"] or 0
        win_rate = wins / total if total > 0 else 0.0
        profit_factor = wins / losses if losses > 0 else float("inf") if wins > 0 else 0.0

        results.append({
            "desk_id": row["desk_id"],
            "asset_symbol": row["asset_symbol"],
            "total_fills": total,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 4),
            "profit_factor": round(profit_factor, 2),
            "avg_slippage": float(row["avg_slippage"]) if row["avg_slippage"] else 0.0,
            "avg_latency_ms": float(row["avg_latency_ms"]) if row["avg_latency_ms"] else 0.0,
        })
    return results


async def get_bayesian_accuracy(lookback_days: int = 30) -> dict[str, Any]:
    """
    Measure Bayesian Accuracy: how often high-posterior signals actually win.

    Bins posterior probabilities into ranges and calculates actual win rate
    within each bin. Perfect calibration: 70% posterior → 70% actual wins.
    """
    pool = get_pool()
    rows = await pool.fetch("""
        WITH binned AS (
            SELECT
                CASE
                    WHEN posterior_prob >= 0.90 THEN '0.90+'
                    WHEN posterior_prob >= 0.80 THEN '0.80-0.90'
                    WHEN posterior_prob >= 0.70 THEN '0.70-0.80'
                    WHEN posterior_prob >= 0.60 THEN '0.60-0.70'
                    ELSE '<0.60'
                END AS posterior_bin,
                posterior_prob,
                CASE
                    WHEN (signal_direction = 1 AND simulated_fill_price >= target_price)
                      OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                    THEN 1 ELSE 0
                END AS is_win
            FROM signal_ledger
            WHERE ts >= NOW() - make_interval(days => $1)
              AND simulated_fill_price IS NOT NULL
              AND posterior_prob IS NOT NULL
        )
        SELECT
            posterior_bin,
            COUNT(*) AS n_signals,
            AVG(is_win) AS actual_win_rate,
            AVG(posterior_prob) AS avg_posterior
        FROM binned
        GROUP BY posterior_bin
        ORDER BY posterior_bin DESC
    """, lookback_days)

    bins = []
    for row in rows:
        bins.append({
            "bin": row["posterior_bin"],
            "n_signals": row["n_signals"],
            "actual_win_rate": round(float(row["actual_win_rate"]), 4),
            "avg_posterior": round(float(row["avg_posterior"]), 4),
            "calibration_error": round(
                abs(float(row["actual_win_rate"]) - float(row["avg_posterior"])), 4
            ),
        })

    avg_error = sum(b["calibration_error"] for b in bins) / len(bins) if bins else 0.0
    return {
        "bins": bins,
        "avg_calibration_error": round(avg_error, 4),
        "well_calibrated": avg_error < 0.05,
    }
