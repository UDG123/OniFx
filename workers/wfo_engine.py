"""
OniQuant v6.0 — Walk-Forward Optimization Engine
===================================================
Pulls 30 days of 1-minute OHLCV from TimescaleDB continuous aggregates,
wraps Alpha Stack indicators via vbt.IndicatorFactory, and runs a
rolling IS/OOS grid search to find optimal parameters.

Optimal params are published to Redis: oniquant:config:{desk_id}

WFO Window Structure:
    In-Sample:       21 days (training)
    Out-of-Sample:    7 days (validation)
    Step:             7 days (non-overlapping OOS)

Target: Maximize (Win Rate > 69.5%) + Sharpe Ratio.

Deploy: Railway.app scheduled worker (cron or on-demand).
    railway run python -m workers.wfo_engine
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import numpy as np
import pandas as pd
import orjson
import redis.asyncio as aioredis
import structlog
import vectorbt as vbt
from vectorbt.indicators.factory import IndicatorFactory

log = structlog.get_logger("oniquant.wfo_engine")

REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@timescaledb:5432/oniquant")
CONFIG_PREFIX: str = "oniquant:config"
MIN_WIN_RATE: float = 0.695


# ---------------------------------------------------------------------------
# 1. Data Loading — TimescaleDB Continuous Aggregate
# ---------------------------------------------------------------------------

async def fetch_ohlcv_from_timescale(
    desk_id: str,
    asset_symbol: str,
    lookback_days: int = 30,
    bucket: str = "1m",
) -> pd.DataFrame:
    """
    Pull OHLCV data from TimescaleDB 1-minute continuous aggregate.

    In production, executes:
        SELECT bucket, open, high, low, close, volume
        FROM cagg_signals_1m
        WHERE asset_symbol = $1 AND desk_id = $2
          AND bucket >= NOW() - INTERVAL '30 days'
        ORDER BY bucket ASC;

    Falls back to synthetic GBM data if DB is unavailable.
    """
    try:
        import asyncpg
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            cagg = "cagg_signals_1m" if bucket == "1m" else "cagg_signals_5m"
            rows = await conn.fetch(f"""
                SELECT bucket, open, high, low, close, volume
                FROM {cagg}
                WHERE asset_symbol = $1 AND desk_id = $2
                  AND bucket >= NOW() - make_interval(days => $3)
                ORDER BY bucket ASC
            """, asset_symbol, desk_id, lookback_days)

            if rows:
                df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df.set_index("timestamp", inplace=True)
                return df
        finally:
            await conn.close()
    except Exception as e:
        await log.awarning("timescale_fetch_fallback", error=str(e))

    # Fallback: synthetic GBM data for development/testing
    return _generate_synthetic_ohlcv(lookback_days)


def _generate_synthetic_ohlcv(days: int = 30) -> pd.DataFrame:
    """Generate synthetic 1-minute OHLCV via geometric Brownian motion."""
    bars_per_day = 390  # 6.5 hours of market data
    n = days * bars_per_day
    rng = np.random.default_rng(seed=42)

    mu, sigma, S0 = 0.00001, 0.001, 450.0
    log_ret = (mu - 0.5 * sigma**2) + sigma * rng.standard_normal(n)
    close = S0 * np.exp(np.cumsum(log_ret))

    noise = rng.uniform(0.999, 1.001, (n, 3))
    periods = pd.date_range(end=pd.Timestamp.now(), periods=n, freq="1min")

    return pd.DataFrame({
        "open": close * noise[:, 0],
        "high": close * np.maximum(noise[:, 1], 1.0005),
        "low": close * np.minimum(noise[:, 2], 0.9995),
        "close": close,
        "volume": rng.lognormal(np.log(5000), 0.5, n),
    }, index=pd.DatetimeIndex(periods, name="timestamp"))


# ---------------------------------------------------------------------------
# 2. Vectorbt Indicator Wrapper — Alpha Stack Signal Generator
# ---------------------------------------------------------------------------

def _alpha_signal_logic(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    volume: np.ndarray,
    knn_k: int,
    spline_window: int,
    iof_threshold: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Combined Alpha Stack signal generator for vectorbt grid search.

    Simplified vectorized proxy that captures the essence of:
        - kNN pattern matching (via momentum regime detection)
        - Spline quantile boundaries (via rolling percentiles)
        - IOF strength filtering (via volume displacement)

    Returns (signal_strength, entries, exits) as 1-D arrays.
    """
    n = len(close)
    s_close = pd.Series(close)
    s_volume = pd.Series(volume)
    s_high = pd.Series(high)
    s_low = pd.Series(low)

    # --- kNN proxy: momentum regime via rolling return rank ---
    returns = s_close.pct_change(knn_k)
    momentum = returns.rolling(knn_k).mean().to_numpy()

    # --- Spline proxy: rolling quantile boundaries ---
    q05 = s_close.rolling(spline_window).quantile(0.05).to_numpy()
    q95 = s_close.rolling(spline_window).quantile(0.95).to_numpy()

    # --- IOF proxy: displacement × relative volume ---
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    atr = pd.Series(tr).rolling(20).mean().to_numpy()
    atr = np.where(atr > 0, atr, 1e-8)

    open_prices = np.roll(close, 1)  # approximate open
    open_prices[0] = close[0]
    displacement = np.abs(close - open_prices) / atr
    rel_vol = s_volume / s_volume.rolling(20).mean()
    iof = (0.6 * displacement + 0.4 * rel_vol.to_numpy())

    # --- Signal generation ---
    # Entry: price near lower quantile + positive momentum + IOF above threshold
    entries = (close <= q05 * 1.01) & (momentum > 0) & (iof > iof_threshold)
    # Exit: price near upper quantile OR IOF drops
    exits = (close >= q95 * 0.99) | (iof < iof_threshold * 0.5)

    # Signal strength for analysis
    signal_strength = np.where(entries, iof, 0.0)

    return signal_strength, entries.astype(float), exits.astype(float)


AlphaStackIndicator = IndicatorFactory(
    class_name="AlphaStack",
    short_name="alpha",
    input_names=["close", "high", "low", "volume"],
    param_names=["knn_k", "spline_window", "iof_threshold"],
    output_names=["signal_strength", "entries", "exits"],
).with_apply_func(_alpha_signal_logic)


# ---------------------------------------------------------------------------
# 3. Walk-Forward Grid Search
# ---------------------------------------------------------------------------

def run_wfo_grid(
    df: pd.DataFrame,
    knn_k_values: list[int],
    spline_window_values: list[int],
    iof_threshold_values: list[float],
    is_days: int = 21,
    oos_days: int = 7,
) -> list[dict[str, Any]]:
    """
    Execute rolling Walk-Forward Optimization.

    For each (IS, OOS) window:
        1. Run grid search on IS data.
        2. Select best params (win rate > 69.5%, max Sharpe).
        3. Validate on OOS data.
        4. Report robustness.
    """
    bars_per_day = 390
    is_bars = is_days * bars_per_day
    oos_bars = oos_days * bars_per_day
    window_bars = is_bars + oos_bars
    step_bars = oos_bars  # non-overlapping OOS windows
    n = len(df)

    results = []
    window_idx = 0

    i = 0
    while i + window_bars <= n:
        window_df = df.iloc[i : i + window_bars]
        is_df = window_df.iloc[:is_bars]
        oos_df = window_df.iloc[is_bars:]

        # --- In-Sample grid search ---
        is_indicator = AlphaStackIndicator.run(
            close=is_df["close"],
            high=is_df["high"],
            low=is_df["low"],
            volume=is_df["volume"],
            knn_k=knn_k_values,
            spline_window=spline_window_values,
            iof_threshold=iof_threshold_values,
            param_product=True,
        )

        is_entries = is_indicator.entries.astype(bool)
        is_exits = is_indicator.exits.astype(bool)

        is_pf = vbt.Portfolio.from_signals(
            close=is_df["close"],
            entries=is_entries,
            exits=is_exits,
            init_cash=100_000,
            fees=0.001,
            freq="1min",
        )

        # Extract metrics
        win_rates = is_pf.trades.win_rate()
        sharpes = is_pf.sharpe_ratio()
        trade_counts = is_pf.trades.count()

        # Filter: win rate >= 69.5% and at least 10 trades
        metrics = pd.DataFrame({
            "win_rate": win_rates,
            "sharpe": sharpes,
            "n_trades": trade_counts,
        })
        candidates = metrics[(metrics["win_rate"] >= MIN_WIN_RATE) & (metrics["n_trades"] >= 10)]

        if candidates.empty:
            results.append({"window": window_idx, "robust": False, "reason": "no_is_candidate"})
            i += step_bars
            window_idx += 1
            continue

        best_idx = candidates["sharpe"].idxmax()
        is_best = candidates.loc[best_idx]

        # Resolve param values from the multi-index
        if isinstance(best_idx, tuple):
            best_knn_k, best_spline_w, best_iof_t = best_idx
        else:
            best_knn_k = knn_k_values[0]
            best_spline_w = spline_window_values[0]
            best_iof_t = iof_threshold_values[0]

        # --- Out-of-Sample validation ---
        oos_indicator = AlphaStackIndicator.run(
            close=oos_df["close"],
            high=oos_df["high"],
            low=oos_df["low"],
            volume=oos_df["volume"],
            knn_k=[best_knn_k],
            spline_window=[best_spline_w],
            iof_threshold=[best_iof_t],
        )

        oos_pf = vbt.Portfolio.from_signals(
            close=oos_df["close"],
            entries=oos_indicator.entries.astype(bool),
            exits=oos_indicator.exits.astype(bool),
            init_cash=100_000,
            fees=0.001,
            freq="1min",
        )

        oos_wr = float(oos_pf.trades.win_rate()) if oos_pf.trades.count() > 0 else 0.0
        oos_sharpe = float(oos_pf.sharpe_ratio()) if oos_pf.trades.count() > 0 else 0.0

        # Robustness: OOS win rate >= threshold + bounded Sharpe decay
        is_sharpe_val = float(is_best["sharpe"]) if not pd.isna(is_best["sharpe"]) else 0.0
        sharpe_decay = 1.0 - (oos_sharpe / is_sharpe_val) if is_sharpe_val > 0 else 1.0
        robust = oos_wr >= MIN_WIN_RATE and sharpe_decay <= 0.40

        results.append({
            "window": window_idx,
            "robust": robust,
            "is_win_rate": float(is_best["win_rate"]),
            "is_sharpe": is_sharpe_val,
            "oos_win_rate": oos_wr,
            "oos_sharpe": oos_sharpe,
            "sharpe_decay": round(sharpe_decay, 4),
            "best_params": {
                "knn_k": int(best_knn_k),
                "spline_window": int(best_spline_w),
                "iof_threshold": float(best_iof_t),
            },
        })

        i += step_bars
        window_idx += 1

    return results


# ---------------------------------------------------------------------------
# 4. Param Publishing — Redis Config Store
# ---------------------------------------------------------------------------

async def publish_optimal_params(
    desk_id: str,
    params: dict[str, Any],
    redis_url: str = REDIS_URL,
) -> None:
    """
    Publish optimal WFO parameters to Redis for live consumption.

    Key: oniquant:config:{desk_id}
    TTL: 86400s (24 hours — force re-optimization daily)
    """
    pool = aioredis.from_url(redis_url, decode_responses=False)
    try:
        config_key = f"{CONFIG_PREFIX}:{desk_id}"
        payload = orjson.dumps(params)
        await pool.set(config_key, payload, ex=86400)
        await log.ainfo("wfo_params_published", desk_id=desk_id, params=params)
    finally:
        await pool.aclose()


# ---------------------------------------------------------------------------
# 5. Main Pipeline
# ---------------------------------------------------------------------------

async def run_wfo_pipeline(
    desk_id: str = "luxalgo",
    asset_symbol: str = "SPY",
) -> list[dict[str, Any]]:
    """
    Full WFO pipeline: fetch data → grid search → validate → publish.
    """
    await log.ainfo("wfo_pipeline_started", desk_id=desk_id, asset=asset_symbol)

    # Fetch data
    df = await fetch_ohlcv_from_timescale(desk_id, asset_symbol, lookback_days=30)
    await log.ainfo("wfo_data_loaded", bars=len(df))

    # Parameter grid (conservative to avoid OOM on Railway 8GB instances)
    knn_k_values = [5, 7, 10, 15]
    spline_window_values = [20, 30, 50]
    iof_threshold_values = [0.4, 0.6, 0.8]
    # Total combos: 4 × 3 × 3 = 36

    results = run_wfo_grid(
        df=df,
        knn_k_values=knn_k_values,
        spline_window_values=spline_window_values,
        iof_threshold_values=iof_threshold_values,
        is_days=21,
        oos_days=7,
    )

    # Find the most recent robust window's params
    robust_results = [r for r in results if r.get("robust")]
    if robust_results:
        best = robust_results[-1]  # most recent robust window
        await publish_optimal_params(desk_id, best["best_params"])
        await log.ainfo("wfo_pipeline_complete", robust_windows=len(robust_results), total=len(results))
    else:
        await log.awarning("wfo_no_robust_params", total_windows=len(results))

    return results


if __name__ == "__main__":
    asyncio.run(run_wfo_pipeline())
