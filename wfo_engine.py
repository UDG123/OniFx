"""
OniQuant v6.0 — Phase 5: Walk-Forward Optimization (WFO) Engine
================================================================
Vectorized parameter grid search over custom Alpha Stack indicators
using vectorbt. Designed to maintain >69.5% aggregate win rate via
rolling In-Sample / Out-of-Sample validation against overfitting.

All heavy lifting is NumPy/Pandas vectorized — zero Python for-loops
on the hot path.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import vectorbt as vbt
from vectorbt.indicators.factory import IndicatorFactory

# ---------------------------------------------------------------------------
# 1. Data Mocking — TimescaleDB Continuous Aggregate Placeholder
# ---------------------------------------------------------------------------

def fetch_timescale_aggregates(
    symbol: str,
    timeframe: str = "1h",
    start: str = "2020-01-01",
    end: str = "2024-01-01",
) -> pd.DataFrame:
    """
    Placeholder for TimescaleDB hypertable query.

    In production this executes:
        SELECT * FROM ohlcv_1h
        WHERE symbol = $1 AND bucket >= $2 AND bucket < $3
        ORDER BY bucket ASC;

    Returns a DatetimeIndex DataFrame with columns: open, high, low, close, volume.
    Mock uses geometric Brownian motion for realistic price dynamics.
    """
    periods = pd.date_range(start=start, end=end, freq=timeframe)
    n = len(periods)

    # GBM parameters calibrated to SPY hourly vol (~0.8% daily)
    mu = 0.0001       # drift per bar
    sigma = 0.005     # vol per bar
    S0 = 450.0        # initial price

    # Vectorized GBM path: S(t) = S0 * exp(cumsum(log-returns))
    rng = np.random.default_rng(seed=42)
    log_returns = (mu - 0.5 * sigma**2) + sigma * rng.standard_normal(n)
    close = S0 * np.exp(np.cumsum(log_returns))

    # Derive OHLC from close with realistic intra-bar noise
    noise = rng.uniform(0.998, 1.002, size=(n, 3))
    high = close * np.maximum(noise[:, 0], 1.001)
    low = close * np.minimum(noise[:, 1], 0.999)
    open_ = close * noise[:, 2]

    # Volume: mean-reverting log-normal
    volume = rng.lognormal(mean=np.log(1e6), sigma=0.5, size=n)

    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
        index=pd.DatetimeIndex(periods[:n], name="timestamp"),
    )


# ---------------------------------------------------------------------------
# 2. Custom Indicator Factory — Spline Quantile Volatility Ratio (SQVR)
# ---------------------------------------------------------------------------
# Concept: Ratio of short-window quantile spread to long-window ATR.
# High SQVR → regime expansion (trend); Low SQVR → contraction (mean-revert).
# This is a simplified proxy for the full Spline Quantile Regression model.

def _sqvr_logic(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    short_window: int,
    long_window: int,
    quantile_upper: float,
    quantile_lower: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Vectorized SQVR computation across the full price array.

    Steps:
        1. Rolling quantile spread on close (short window).
        2. Rolling ATR on high/low/close (long window).
        3. SQVR = quantile_spread / ATR (normalized ratio).
        4. Signal: SQVR > 1.0 → bullish expansion; SQVR < threshold → bearish.

    Returns (sqvr_ratio, entries, exits) as 1-D numpy arrays.
    """
    # --- Convert to pandas for rolling ops (vectorbt unwraps back to numpy) ---
    close_s = pd.Series(close)
    high_s = pd.Series(high)
    low_s = pd.Series(low)

    # Step 1: Rolling quantile spread (short window)
    # Upper quantile - lower quantile captures the dispersion of recent prices.
    q_upper = close_s.rolling(short_window).quantile(quantile_upper)
    q_lower = close_s.rolling(short_window).quantile(quantile_lower)
    quantile_spread = q_upper - q_lower  # shape: (n,)

    # Step 2: Rolling ATR (long window) — true range = max(H-L, |H-Cprev|, |L-Cprev|)
    prev_close = close_s.shift(1)
    tr = pd.concat([
        high_s - low_s,
        (high_s - prev_close).abs(),
        (low_s - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(long_window).mean()  # SMA-based ATR for speed

    # Step 3: SQVR ratio — avoid div-by-zero with clamp
    sqvr = (quantile_spread / atr.clip(lower=1e-8)).to_numpy()

    # Step 4: Binary signal generation (vectorized boolean masks)
    entries = sqvr > 1.0    # expansion regime → enter long
    exits = sqvr < 0.5      # contraction regime → exit

    return sqvr, entries, exits


# Register with vectorbt's IndicatorFactory for grid-search compatibility.
# param_names defines what the optimizer will sweep over.
SQVR = IndicatorFactory(
    class_name="SQVR",
    short_name="sqvr",
    input_names=["close", "high", "low"],
    param_names=["short_window", "long_window", "quantile_upper", "quantile_lower"],
    output_names=["sqvr_ratio", "entries", "exits"],
).with_apply_func(_sqvr_logic)


# ---------------------------------------------------------------------------
# 3. Rolling Window Splitter — IS/OOS Walk-Forward Windows
# ---------------------------------------------------------------------------

def build_wfo_splits(
    df: pd.DataFrame,
    is_period: str = "730D",   # 2 years in-sample
    oos_period: str = "180D",  # 6 months out-of-sample
    step: str = "90D",         # 3-month step between windows
) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Generate rolling (In-Sample, Out-of-Sample) DataFrame pairs.

    Uses vectorbt's datetime-aware range splitter to produce walk-forward
    windows that slide across the full dataset. Each IS window trains the
    parameter grid; each OOS window validates against overfitting.

    Returns list of (is_df, oos_df) tuples.
    """
    splitter = vbt.Splitter.from_rolling(
        df.index,
        length=pd.Timedelta(is_period) + pd.Timedelta(oos_period),
        offset=pd.Timedelta(step),
    )

    splits = []
    is_len = pd.Timedelta(is_period)

    for i in range(len(splitter)):
        mask = splitter[i]
        # mask is a RangeMask — extract the underlying index range
        window_idx = df.index[mask]
        if len(window_idx) == 0:
            continue
        cutoff = window_idx[0] + is_len
        is_df = df.loc[window_idx[window_idx < cutoff]]
        oos_df = df.loc[window_idx[window_idx >= cutoff]]
        if len(is_df) > 0 and len(oos_df) > 0:
            splits.append((is_df, oos_df))

    return splits


# ---------------------------------------------------------------------------
# 4. Vectorized Portfolio Simulation — Grid Search over SQVR Parameters
# ---------------------------------------------------------------------------

def run_grid_backtest(
    df: pd.DataFrame,
    short_windows: list[int],
    long_windows: list[int],
    quantile_uppers: list[float],
    quantile_lowers: list[float],
    init_cash: float = 100_000.0,
    fees: float = 0.001,
) -> vbt.Portfolio:
    """
    Run a fully vectorized parameter grid backtest.

    vectorbt broadcasts all parameter combinations into a single NumPy
    operation — no Python for-loops. For M parameter combos and N bars,
    this creates an (N x M) signal matrix evaluated in one pass.

    Returns a vbt.Portfolio object containing all combo results.
    """
    # --- Step 1: Run SQVR indicator across full parameter grid ---
    # .run() with param lists creates the cartesian product automatically.
    indicator = SQVR.run(
        close=df["close"],
        high=df["high"],
        low=df["low"],
        short_window=short_windows,
        long_window=long_windows,
        quantile_upper=quantile_uppers,
        quantile_lower=quantile_lowers,
        param_product=True,  # cartesian product of all param combos
    )

    # --- Step 2: Extract entry/exit boolean matrices ---
    # Shape: (n_bars, n_param_combos) — fully vectorized signal grids
    entries: pd.DataFrame = indicator.entries.astype(bool)
    exits: pd.DataFrame = indicator.exits.astype(bool)

    # --- Step 3: Simulate portfolios across all combos in one call ---
    # from_signals handles position sizing, fees, slippage vectorized.
    portfolio = vbt.Portfolio.from_signals(
        close=df["close"],
        entries=entries,
        exits=exits,
        init_cash=init_cash,
        fees=fees,
        freq="1h",  # must match data timeframe for annualization
    )

    return portfolio


def extract_top_params(
    portfolio: vbt.Portfolio,
    min_win_rate: float = 0.695,
) -> dict | None:
    """
    Extract the best parameter combination from a grid backtest.

    Selection criteria (ordered):
        1. Filter: Win Rate >= 69.5% (hard threshold).
        2. Rank: Maximum Sharpe Ratio among survivors.

    Returns dict with param values and metrics, or None if no combo passes.
    """
    # --- Pull key metrics as Series indexed by param combo ---
    stats = pd.DataFrame({
        "win_rate": portfolio.trades.win_rate(),
        "sharpe": portfolio.sharpe_ratio(),
        "total_return": portfolio.total_return(),
        "max_drawdown": portfolio.max_drawdown(),
        "n_trades": portfolio.trades.count(),
    })

    # --- Step 1: Hard filter on win rate ---
    candidates = stats[stats["win_rate"] >= min_win_rate]
    if candidates.empty:
        return None

    # --- Step 2: Rank by Sharpe ---
    best_idx = candidates["sharpe"].idxmax()
    best_row = candidates.loc[best_idx]

    return {
        "param_index": best_idx,
        "win_rate": float(best_row["win_rate"]),
        "sharpe": float(best_row["sharpe"]),
        "total_return": float(best_row["total_return"]),
        "max_drawdown": float(best_row["max_drawdown"]),
        "n_trades": int(best_row["n_trades"]),
    }


# ---------------------------------------------------------------------------
# 5. Walk-Forward Validation — Overfit Detection
# ---------------------------------------------------------------------------

def validate_oos(
    is_df: pd.DataFrame,
    oos_df: pd.DataFrame,
    param_grid: dict[str, list],
    min_win_rate: float = 0.695,
    max_sharpe_decay: float = 0.40,
) -> dict:
    """
    Full Walk-Forward Validation cycle for one IS/OOS pair.

    Process:
        1. Run grid backtest on IS data → find best params.
        2. Apply those exact params to OOS data.
        3. Compare: if OOS win rate >= threshold AND Sharpe decay < 40%,
           the params are ROBUST. Otherwise, flagged as OVERFIT.

    Args:
        param_grid: dict with keys matching SQVR param_names, values are lists.
                    e.g. {"short_windows": [10, 20], "long_windows": [50, 100], ...}
        min_win_rate: minimum acceptable win rate on OOS.
        max_sharpe_decay: max allowed Sharpe drop from IS → OOS (fraction).

    Returns dict with is_metrics, oos_metrics, robust flag, and decay stats.
    """
    # --- In-Sample Optimization ---
    is_portfolio = run_grid_backtest(
        df=is_df,
        short_windows=param_grid["short_windows"],
        long_windows=param_grid["long_windows"],
        quantile_uppers=param_grid["quantile_uppers"],
        quantile_lowers=param_grid["quantile_lowers"],
    )
    is_best = extract_top_params(is_portfolio, min_win_rate=min_win_rate)

    if is_best is None:
        return {
            "robust": False,
            "reason": "no_is_candidate",
            "is_metrics": None,
            "oos_metrics": None,
        }

    # --- Extract winning params by index ---
    # The param_index maps back to the cartesian product position.
    # We reconstruct the exact params from the indicator's param grid.
    best_idx = is_best["param_index"]

    # Resolve individual param values from the multi-index
    if isinstance(best_idx, tuple):
        sw, lw, qu, ql = best_idx
    else:
        # Single-column index fallback
        sw, lw, qu, ql = (
            param_grid["short_windows"][0],
            param_grid["long_windows"][0],
            param_grid["quantile_uppers"][0],
            param_grid["quantile_lowers"][0],
        )

    # --- Out-of-Sample Validation (single param set, no grid) ---
    oos_portfolio = run_grid_backtest(
        df=oos_df,
        short_windows=[sw],
        long_windows=[lw],
        quantile_uppers=[qu],
        quantile_lowers=[ql],
    )
    oos_metrics = extract_top_params(oos_portfolio, min_win_rate=0.0)  # no filter on OOS

    if oos_metrics is None:
        return {
            "robust": False,
            "reason": "no_oos_trades",
            "is_metrics": is_best,
            "oos_metrics": None,
        }

    # --- Robustness Check ---
    # Criterion 1: OOS win rate must still exceed the target
    oos_win_ok = oos_metrics["win_rate"] >= min_win_rate

    # Criterion 2: Sharpe decay from IS → OOS must be bounded
    is_sharpe = is_best["sharpe"]
    oos_sharpe = oos_metrics["sharpe"]
    if is_sharpe > 0:
        sharpe_decay = 1.0 - (oos_sharpe / is_sharpe)
    else:
        sharpe_decay = 1.0  # IS Sharpe was non-positive → auto-fail

    sharpe_ok = sharpe_decay <= max_sharpe_decay

    robust = oos_win_ok and sharpe_ok

    return {
        "robust": robust,
        "reason": "passed" if robust else (
            "oos_winrate_fail" if not oos_win_ok else "sharpe_decay_fail"
        ),
        "is_metrics": is_best,
        "oos_metrics": oos_metrics,
        "sharpe_decay": round(sharpe_decay, 4),
    }


# ---------------------------------------------------------------------------
# Entrypoint — Full WFO Pipeline Demo
# ---------------------------------------------------------------------------

def run_wfo_pipeline(symbol: str = "SPY") -> list[dict]:
    """
    Execute the complete Walk-Forward Optimization pipeline.

    Steps:
        1. Fetch historical OHLCV data.
        2. Split into rolling IS/OOS windows.
        3. For each window, run grid search + OOS validation.
        4. Return per-window robustness results.
    """
    # --- Fetch data ---
    df = fetch_timescale_aggregates(symbol, timeframe="1h", start="2020-01-01", end="2024-01-01")

    # --- Define parameter grid ---
    param_grid = {
        "short_windows": [10, 15, 20, 30],
        "long_windows": [50, 75, 100, 150],
        "quantile_uppers": [0.75, 0.85, 0.95],
        "quantile_lowers": [0.05, 0.15, 0.25],
    }
    # Total combos: 4 * 4 * 3 * 3 = 144 param sets, all vectorized.

    # --- Build WFO splits ---
    splits = build_wfo_splits(df, is_period="730D", oos_period="180D", step="90D")

    # --- Run validation on each window ---
    results = []
    for i, (is_df, oos_df) in enumerate(splits):
        result = validate_oos(is_df, oos_df, param_grid)
        result["window"] = i
        result["is_start"] = str(is_df.index[0])
        result["is_end"] = str(is_df.index[-1])
        result["oos_start"] = str(oos_df.index[0])
        result["oos_end"] = str(oos_df.index[-1])
        results.append(result)

    # --- Summary ---
    robust_count = sum(1 for r in results if r["robust"])
    total = len(results)
    print(f"\nWFO Complete: {robust_count}/{total} windows robust "
          f"({100 * robust_count / total:.1f}%)")

    return results


if __name__ == "__main__":
    results = run_wfo_pipeline("SPY")
    for r in results:
        tag = "ROBUST" if r["robust"] else "OVERFIT"
        print(f"  Window {r['window']}: [{tag}] {r['reason']} | "
              f"IS: {r['is_start']} → {r['is_end']} | "
              f"OOS: {r['oos_start']} → {r['oos_end']}")
