"""
OniQuant v6.0 — Combinatorial Purged Cross-Validation (CPCV) Engine
=====================================================================
Validates Tri-State regime performance using CPCV methodology
(de Prado, "Advances in Financial Machine Learning", Ch. 12).

Why CPCV over Walk-Forward:
    WFO uses a single expanding/rolling train/test split.
    CPCV generates ALL combinatorial train/test splits from N groups,
    producing a distribution of performance metrics rather than a
    single point estimate. This lets us compute:

        1. Deflated Sharpe Ratio (DSR) — adjusts for multiple testing
        2. Probability of Backtest Overfitting (PBO) — fraction of
           CPCV paths where in-sample > out-of-sample

Pipeline:
    1. Query Shadow Ledger for tri-state decisions + hypothetical fills.
    2. Divide time series into N=6 temporal groups.
    3. Generate C(N, N/2) combinatorial train/test splits.
    4. Apply 1% temporal embargo between train/test to prevent leakage.
    5. For each split, compute per-regime Sharpe on IS and OOS.
    6. Aggregate into DSR and PBO across all paths.

Usage:
    from app.quant.cpcv_optimizer import run_cpcv_analysis
    results = await run_cpcv_analysis(redis_pool)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any

import numpy as np
import structlog

log = structlog.get_logger("oniquant.cpcv")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
N_GROUPS: int = 6               # Number of temporal groups
TEST_GROUPS: int = 3            # N/2 groups held out for testing (C(6,3) = 20 paths)
EMBARGO_PCT: float = 0.01       # 1% embargo buffer between train/test
MIN_TRADES_PER_GROUP: int = 5   # Minimum trades per group to be valid
RISK_FREE_RATE: float = 0.05    # Annualized (for Sharpe calculation)
ANNUALIZATION_FACTOR: float = np.sqrt(252)  # Daily → annual


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class RegimeTradeResult:
    """A single hypothetical trade result from shadow evaluation."""
    signal_id: str
    timestamp: float
    regime: str
    decision: str
    posterior: float
    pnl: float              # Hypothetical P&L from synthetic matcher
    target_price: float
    fill_price: float
    slippage_bps: float
    asset_symbol: str
    desk_id: str


@dataclass
class CVFoldResult:
    """Results from a single CPCV fold for one regime."""
    regime: str
    fold_id: int
    train_groups: tuple[int, ...]
    test_groups: tuple[int, ...]
    is_sharpe: float         # In-sample Sharpe
    oos_sharpe: float        # Out-of-sample Sharpe
    is_win_rate: float
    oos_win_rate: float
    is_trades: int
    oos_trades: int
    is_mean_pnl: float
    oos_mean_pnl: float


@dataclass
class RegimeCPCVResult:
    """Aggregated CPCV results for a single regime."""
    regime: str
    label: str
    n_paths: int
    mean_oos_sharpe: float
    std_oos_sharpe: float
    deflated_sharpe: float    # DSR
    pbo: float                # Probability of Backtest Overfitting
    mean_oos_win_rate: float
    mean_oos_pnl: float
    ev_per_trade: float       # Expected Value per trade
    total_oos_trades: int


# ---------------------------------------------------------------------------
# Sharpe Ratio Calculation
# ---------------------------------------------------------------------------

def _compute_sharpe(pnl_array: np.ndarray) -> float:
    """
    Annualized Sharpe Ratio from an array of per-trade P&L.

    SR = (mean(R) - Rf/252) / std(R) × sqrt(252)

    Returns 0.0 if insufficient data or zero variance.
    """
    if len(pnl_array) < 2:
        return 0.0
    daily_rf = RISK_FREE_RATE / 252.0
    mean_r = np.mean(pnl_array) - daily_rf
    std_r = np.std(pnl_array, ddof=1)
    if std_r < 1e-12:
        return 0.0
    return float((mean_r / std_r) * ANNUALIZATION_FACTOR)


# ---------------------------------------------------------------------------
# Deflated Sharpe Ratio (DSR)
# ---------------------------------------------------------------------------

def _compute_deflated_sharpe(
    observed_sharpe: float,
    n_trials: int,
    variance_of_sharpes: float,
    n_observations: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    """
    Deflated Sharpe Ratio (Bailey & de Prado, 2014).

    Adjusts for multiple testing by comparing the observed Sharpe
    against the expected maximum Sharpe under the null hypothesis
    of n_trials independent strategies.

    DSR = Φ[ (SR_obs - E[max(SR)]) / σ(SR) × √(1 - γ₃·SR + (γ₄-1)/4·SR²) ]

    where E[max(SR)] ≈ σ(SR) × [(1-γ) × Φ⁻¹(1 - 1/n) + γ × Φ⁻¹(1 - 1/(n·e))]
    (γ = Euler-Mascheroni constant ≈ 0.5772)

    Returns probability that the observed Sharpe is genuine (0 to 1).
    """
    from scipy import stats

    if n_trials < 1 or n_observations < 2:
        return 0.0
    if variance_of_sharpes < 1e-12:
        return 1.0 if observed_sharpe > 0 else 0.0

    std_sharpe = math.sqrt(variance_of_sharpes)

    # Expected maximum Sharpe under null (Euler-Mascheroni approximation)
    euler_gamma = 0.5772156649
    if n_trials == 1:
        e_max_sr = 0.0
    else:
        z1 = stats.norm.ppf(1.0 - 1.0 / n_trials) if n_trials > 1 else 0.0
        z2 = stats.norm.ppf(1.0 - 1.0 / (n_trials * math.e)) if n_trials > 1 else 0.0
        e_max_sr = std_sharpe * ((1 - euler_gamma) * z1 + euler_gamma * z2)

    # Correction for non-normal returns (skewness, kurtosis)
    sr = observed_sharpe
    correction = 1.0 - skewness * sr + ((kurtosis - 1.0) / 4.0) * sr ** 2
    if correction <= 0:
        correction = 1e-6

    denominator = std_sharpe * math.sqrt(correction)
    if denominator < 1e-12:
        return 0.5

    test_stat = (sr - e_max_sr) / denominator
    return float(stats.norm.cdf(test_stat))


# ---------------------------------------------------------------------------
# Temporal Grouping with Embargo
# ---------------------------------------------------------------------------

def _split_into_groups(
    trades: list[RegimeTradeResult],
    n_groups: int = N_GROUPS,
) -> list[list[RegimeTradeResult]]:
    """
    Split trades into N equal-sized temporal groups.

    Trades must be pre-sorted by timestamp.
    """
    if not trades:
        return [[] for _ in range(n_groups)]

    group_size = max(1, len(trades) // n_groups)
    groups: list[list[RegimeTradeResult]] = []
    for i in range(n_groups):
        start = i * group_size
        end = start + group_size if i < n_groups - 1 else len(trades)
        groups.append(trades[start:end])
    return groups


def _apply_embargo(
    groups: list[list[RegimeTradeResult]],
    train_indices: tuple[int, ...],
    test_indices: tuple[int, ...],
    embargo_pct: float = EMBARGO_PCT,
) -> tuple[list[RegimeTradeResult], list[RegimeTradeResult]]:
    """
    Assemble train/test sets with temporal embargo purging.

    Embargo removes `embargo_pct` of observations from the boundary
    between each adjacent train→test group transition to prevent
    forward-looking label leakage.
    """
    # Determine boundary groups (train groups immediately before a test group)
    embargo_set: set[int] = set()
    for train_idx in train_indices:
        for test_idx in test_indices:
            if test_idx == train_idx + 1:
                embargo_set.add(train_idx)

    train_trades: list[RegimeTradeResult] = []
    test_trades: list[RegimeTradeResult] = []

    for idx in train_indices:
        group = groups[idx]
        if idx in embargo_set and len(group) > 0:
            # Remove last embargo_pct of the group
            n_purge = max(1, int(len(group) * embargo_pct))
            train_trades.extend(group[:-n_purge])
        else:
            train_trades.extend(group)

    for idx in test_indices:
        group = groups[idx]
        # Purge first embargo_pct if preceded by a train group
        preceded_by_train = any(t == idx - 1 for t in train_indices)
        if preceded_by_train and len(group) > 0:
            n_purge = max(1, int(len(group) * embargo_pct))
            test_trades.extend(group[n_purge:])
        else:
            test_trades.extend(group)

    return train_trades, test_trades


# ---------------------------------------------------------------------------
# CPCV Engine
# ---------------------------------------------------------------------------

def _evaluate_fold(
    groups: list[list[RegimeTradeResult]],
    train_indices: tuple[int, ...],
    test_indices: tuple[int, ...],
    regime: str,
    fold_id: int,
) -> CVFoldResult | None:
    """Evaluate a single CPCV fold for one regime."""
    train_trades, test_trades = _apply_embargo(groups, train_indices, test_indices)

    # Filter to this regime's authorized trades only
    train_auth = [t for t in train_trades if t.regime == regime and t.decision == "AUTHORIZE"]
    test_auth = [t for t in test_trades if t.regime == regime and t.decision == "AUTHORIZE"]

    if len(train_auth) < MIN_TRADES_PER_GROUP or len(test_auth) < MIN_TRADES_PER_GROUP:
        return None

    is_pnl = np.array([t.pnl for t in train_auth])
    oos_pnl = np.array([t.pnl for t in test_auth])

    return CVFoldResult(
        regime=regime,
        fold_id=fold_id,
        train_groups=train_indices,
        test_groups=test_indices,
        is_sharpe=_compute_sharpe(is_pnl),
        oos_sharpe=_compute_sharpe(oos_pnl),
        is_win_rate=float(np.mean(is_pnl > 0)),
        oos_win_rate=float(np.mean(oos_pnl > 0)),
        is_trades=len(train_auth),
        oos_trades=len(test_auth),
        is_mean_pnl=float(np.mean(is_pnl)),
        oos_mean_pnl=float(np.mean(oos_pnl)),
    )


def run_cpcv_for_regime(
    trades: list[RegimeTradeResult],
    regime: str,
    regime_label: str,
) -> RegimeCPCVResult:
    """
    Run full CPCV analysis for a single regime.

    Generates C(N, N/2) = C(6,3) = 20 combinatorial paths.
    For each path, computes IS and OOS Sharpe ratios.
    Aggregates into DSR and PBO.
    """
    # Sort by timestamp
    sorted_trades = sorted(trades, key=lambda t: t.timestamp)
    groups = _split_into_groups(sorted_trades, N_GROUPS)

    all_group_indices = list(range(N_GROUPS))
    fold_results: list[CVFoldResult] = []

    for fold_id, test_combo in enumerate(combinations(all_group_indices, TEST_GROUPS)):
        train_combo = tuple(i for i in all_group_indices if i not in test_combo)
        result = _evaluate_fold(groups, train_combo, test_combo, regime, fold_id)
        if result is not None:
            fold_results.append(result)

    if not fold_results:
        return RegimeCPCVResult(
            regime=regime,
            label=regime_label,
            n_paths=0,
            mean_oos_sharpe=0.0,
            std_oos_sharpe=0.0,
            deflated_sharpe=0.0,
            pbo=1.0,
            mean_oos_win_rate=0.0,
            mean_oos_pnl=0.0,
            ev_per_trade=0.0,
            total_oos_trades=0,
        )

    # Aggregate OOS Sharpes
    oos_sharpes = np.array([f.oos_sharpe for f in fold_results])
    is_sharpes = np.array([f.is_sharpe for f in fold_results])

    mean_oos = float(np.mean(oos_sharpes))
    std_oos = float(np.std(oos_sharpes, ddof=1)) if len(oos_sharpes) > 1 else 0.0

    # PBO: fraction of paths where IS Sharpe > OOS Sharpe
    # (overfitting indicator: model looks better in-sample than out-of-sample)
    n_overfit = int(np.sum(is_sharpes > oos_sharpes))
    pbo = n_overfit / len(fold_results)

    # DSR: deflate the mean OOS Sharpe
    total_oos_trades = sum(f.oos_trades for f in fold_results)
    variance_sharpes = float(np.var(oos_sharpes, ddof=1)) if len(oos_sharpes) > 1 else 0.0

    # Compute skewness and kurtosis from OOS PnLs
    all_oos_pnl = np.concatenate([
        np.array([t.pnl for t in sorted_trades
                  if t.regime == regime and t.decision == "AUTHORIZE"])
    ]) if sorted_trades else np.array([0.0])

    from scipy import stats as sp_stats
    skew = float(sp_stats.skew(all_oos_pnl)) if len(all_oos_pnl) > 2 else 0.0
    kurt = float(sp_stats.kurtosis(all_oos_pnl, fisher=False)) if len(all_oos_pnl) > 3 else 3.0

    dsr = _compute_deflated_sharpe(
        observed_sharpe=mean_oos,
        n_trials=3,  # 3 regimes tested
        variance_of_sharpes=variance_sharpes,
        n_observations=total_oos_trades,
        skewness=skew,
        kurtosis=kurt,
    )

    mean_oos_win_rate = float(np.mean([f.oos_win_rate for f in fold_results]))
    mean_oos_pnl = float(np.mean([f.oos_mean_pnl for f in fold_results]))

    return RegimeCPCVResult(
        regime=regime,
        label=regime_label,
        n_paths=len(fold_results),
        mean_oos_sharpe=round(mean_oos, 4),
        std_oos_sharpe=round(std_oos, 4),
        deflated_sharpe=round(dsr, 4),
        pbo=round(pbo, 4),
        mean_oos_win_rate=round(mean_oos_win_rate, 4),
        mean_oos_pnl=round(mean_oos_pnl, 4),
        ev_per_trade=round(mean_oos_pnl, 4),
        total_oos_trades=total_oos_trades,
    )


# ---------------------------------------------------------------------------
# Shadow Ledger Query
# ---------------------------------------------------------------------------

async def _fetch_shadow_trades(
    redis_pool: Any,
    lookback_days: int = 7,
) -> list[RegimeTradeResult]:
    """
    Fetch tri-state shadow trade results from the Redis shadow ledger.

    Shadow trades are stored as a sorted set:
        oniquant:shadow_ledger  (score = timestamp, member = orjson payload)
    """
    import orjson

    now = time.time()
    cutoff = now - (lookback_days * 86400)

    raw_entries = await redis_pool.zrangebyscore(
        "oniquant:shadow_ledger",
        min=cutoff,
        max=now,
        withscores=True,
    )

    trades: list[RegimeTradeResult] = []
    for member, score in raw_entries:
        try:
            data = orjson.loads(member)
            trades.append(RegimeTradeResult(
                signal_id=data.get("signal_id", ""),
                timestamp=score,
                regime=data.get("regime", "unknown"),
                decision=data.get("decision", "REJECT"),
                posterior=data.get("posterior", 0.0),
                pnl=data.get("pnl", 0.0),
                target_price=data.get("target_price", 0.0),
                fill_price=data.get("fill_price", 0.0),
                slippage_bps=data.get("slippage_bps", 0.0),
                asset_symbol=data.get("asset_symbol", "UNKNOWN"),
                desk_id=data.get("desk_id", "unknown"),
            ))
        except Exception:
            continue

    return trades


import time


async def run_cpcv_analysis(
    redis_pool: Any,
    lookback_days: int = 7,
) -> dict[str, RegimeCPCVResult]:
    """
    Run full CPCV analysis across all three regimes.

    Returns a dict mapping regime name → RegimeCPCVResult.
    """
    from app.services.orchestrator import RISK_PROFILES

    trades = await _fetch_shadow_trades(redis_pool, lookback_days)
    regimes = RISK_PROFILES.get("regimes", {})

    results: dict[str, RegimeCPCVResult] = {}
    for regime_name, regime_cfg in regimes.items():
        label = regime_cfg.get("label", regime_name)
        result = run_cpcv_for_regime(trades, regime_name, label)
        results[regime_name] = result

        await log.ainfo(
            "cpcv_regime_result",
            regime=regime_name,
            n_paths=result.n_paths,
            dsr=result.deflated_sharpe,
            pbo=result.pbo,
            mean_oos_sharpe=result.mean_oos_sharpe,
            ev_per_trade=result.ev_per_trade,
        )

    return results
