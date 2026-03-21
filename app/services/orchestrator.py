"""
OniQuant v6.0 — Bayesian Decision Engine (Tri-State Orchestrator)
===================================================================
Final arbiter for all Alpha Stack signals.

Tri-State Regime Evaluation:
    Every signal is evaluated in parallel across three risk regimes
    (Conservative, Active, Aggressive) using Power Priors, Likelihood
    Tempering, and regime-specific thresholds.

    Power Priors:     P_regime(A) = P(A) ^ a0        (a0 ∈ [0.15, 0.80])
    Tempering:        L_tempered  = L ^ (1/T)         (T ∈ [1.0, 2.5])
    Threshold:        AUTHORIZE if posterior > floor   (floor ∈ [0.51, 0.70])

    The active regime controls live order flow.
    All three regimes are always logged for CPCV shadow analysis.

Bayes' Theorem (Odds Form):
    posterior_odds = prior_odds × likelihood_ratio
    P(A|B) = posterior_odds / (1 + posterior_odds)

Likelihood Multipliers:
    IOF > 80% → ×1.50 | Macro aligned → ×1.20 | Hurst regime match → ×1.30
"""

from __future__ import annotations

import math
import os
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from app.core.config import get_settings
from app.core.database import query_win_rate

log = structlog.get_logger("oniquant.orchestrator")


# ---------------------------------------------------------------------------
# Risk Regime Configuration Loader
# ---------------------------------------------------------------------------

def _load_risk_profiles() -> dict[str, Any]:
    """Load tri-state regime definitions from config/risk_profiles.yaml."""
    try:
        import yaml
    except ImportError:
        # Fallback if PyYAML not installed — use hardcoded defaults
        return _default_risk_profiles()

    config_path = Path(__file__).resolve().parents[2] / "config" / "risk_profiles.yaml"
    if not config_path.exists():
        return _default_risk_profiles()

    with open(config_path) as f:
        return yaml.safe_load(f)


def _default_risk_profiles() -> dict[str, Any]:
    """Hardcoded fallback matching config/risk_profiles.yaml."""
    return {
        "active_regime": "conservative",
        "regimes": {
            "conservative": {
                "label": "Conservative",
                "threshold": 0.70,
                "prior_discount_a0": 0.80,
                "tempering_T": 1.0,
                "kelly_fraction": 0.10,
                "max_cap_pct": 0.01,
            },
            "active": {
                "label": "Active",
                "threshold": 0.60,
                "prior_discount_a0": 0.50,
                "tempering_T": 1.5,
                "kelly_fraction": 0.25,
                "max_cap_pct": 0.025,
            },
            "aggressive": {
                "label": "Aggressive",
                "threshold": 0.51,
                "prior_discount_a0": 0.15,
                "tempering_T": 2.5,
                "kelly_fraction": 0.50,
                "max_cap_pct": 0.05,
            },
        },
    }


RISK_PROFILES: dict[str, Any] = _load_risk_profiles()


# ---------------------------------------------------------------------------
# Indicator Correlation Matrix (HIGH-01 fix — Bayesian De-correlation)
# ---------------------------------------------------------------------------
# Pairwise correlation estimates between likelihood factors.
# When two indicators share underlying signal overlap (e.g., IOF and kNN
# both respond to volume/momentum), their combined evidence is overstated
# by naive multiplication. The square-root penalty corrects for this:
#
#     L_adjusted = L ^ (1 / sqrt(1 + ρ))
#
# where ρ is the max pairwise correlation among active boosting factors.
#
# Keys: frozenset of factor-pair names → estimated correlation coefficient.
# These values should be calibrated empirically via backtest; defaults
# are conservative estimates from microstructure literature.
# ---------------------------------------------------------------------------
INDICATOR_CORRELATION_MATRIX: dict[frozenset[str], float] = {
    frozenset({"iof", "hurst_knn"}): 0.60,   # IOF displacement + kNN volume features overlap
    frozenset({"iof", "hurst_spline"}): 0.20, # IOF and spline have low structural overlap
    frozenset({"iof", "macro"}): 0.10,        # IOF is micro; macro is macro — near-independent
    frozenset({"hurst_knn", "macro"}): 0.15,  # kNN features are asset-level, not macro-driven
    frozenset({"hurst_spline", "macro"}): 0.10,
}


class Decision(str, Enum):
    AUTHORIZE = "AUTHORIZE"
    REJECT = "REJECT"


ROUTING_MAP: dict[str, str] = {
    "equity": "IBKR_TWS", "forex": "IBKR_TWS", "metals": "IBKR_TWS",
    "futures": "IBKR_TWS", "index": "IBKR_TWS",
    "crypto": "BYBIT_V5", "perpetual": "BYBIT_V5", "spot_crypto": "BYBIT_V5",
}


# ---------------------------------------------------------------------------
# Bayesian Arbiter
# ---------------------------------------------------------------------------

class BayesianArbiter:
    """
    Tri-State Bayesian decision engine with Power Priors and Likelihood Tempering.

    Pipeline per signal:
        1. Fetch Prior P(A) from TimescaleDB (30-day win rate).
        2. Compute Likelihood via IOF, Macro, Hurst adjustments.
        3. For each regime (Conservative, Active, Aggressive):
           a. Apply Power Prior:  P_regime = P(A) ^ a0
           b. Apply Tempering:    L_regime = L ^ (1/T)
           c. Compute Posterior via odds-form Bayes.
           d. AUTHORIZE if posterior > regime threshold, else REJECT.
        4. Return tri-state decision dict + active regime decision for routing.
    """

    def __init__(self, redis_pool: aioredis.Redis) -> None:
        self._redis = redis_pool
        self._settings = get_settings()
        self._risk_profiles = RISK_PROFILES
        self._evaluated = 0
        self._authorized = 0
        self._rejected = 0

    # ------------------------------------------------------------------
    # Prior
    # ------------------------------------------------------------------

    async def _fetch_prior(self, asset_symbol: str, desk_id: str) -> float:
        """Fetch 30-day win rate from TimescaleDB, cached in Redis."""
        cache_key = f"oniquant:prior:{asset_symbol}:{desk_id}"
        cached = await self._redis.get(cache_key)
        if cached:
            return max(0.05, min(0.95, float(cached)))

        # Query TimescaleDB
        try:
            win_rate = await query_win_rate(asset_symbol, desk_id, lookback_days=30)
            if win_rate is not None:
                await self._redis.set(cache_key, str(win_rate), ex=300)
                return max(0.05, min(0.95, win_rate))
        except Exception:
            pass

        return 0.50  # uninformative prior

    # ------------------------------------------------------------------
    # Macro Context
    # ------------------------------------------------------------------

    async def _fetch_macro(self) -> dict[str, Any]:
        """Fetch macro context from Redis (populated by Macro-MCP worker)."""
        cached = await self._redis.get("oniquant:macro:latest")
        if cached:
            try:
                return orjson.loads(cached)
            except orjson.JSONDecodeError:
                pass
        return {"dxy_trend": "neutral", "sentiment": "neutral", "vix_regime": "normal"}

    # ------------------------------------------------------------------
    # Likelihood
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_likelihood(
        iof_strength: float,
        macro: dict[str, Any],
        hurst: float,
        model_source: str,
        direction: int,
        asset_class: str,
    ) -> tuple[float, str]:
        """
        Multiplicative likelihood adjustment with correlation de-duplication.

        Factor 1: IOF — >0.80 → ×1.50, >0.60 → ×1.20, <0.30 → ×0.70
        Factor 2: Macro — aligned → ×1.20, opposed → ×0.80
        Factor 3: Hurst — H<0.45+Spline → ×1.30, H>0.55+kNN → ×1.30, mismatch → ×0.75

        De-correlation (HIGH-01 fix):
            Tracks which factors actively boosted the likelihood (multiplier > 1.0).
            If any pair of boosting factors has a high correlation in
            INDICATOR_CORRELATION_MATRIX, applies a square-root penalty:

                L_adjusted = L ^ (1 / sqrt(1 + ρ_max))

            This dampens the compound evidence to account for shared
            information, keeping the posterior grounded near the 69.5% threshold.
        """
        L = 1.0
        parts = []
        active_factors: list[str] = []  # factors that boosted L (mult > 1.0)

        # IOF
        if iof_strength > 0.80:
            L *= 1.50; parts.append(f"IOF={iof_strength:.2f}→×1.50")
            active_factors.append("iof")
        elif iof_strength > 0.60:
            L *= 1.20; parts.append(f"IOF={iof_strength:.2f}→×1.20")
            active_factors.append("iof")
        elif iof_strength < 0.30:
            L *= 0.70; parts.append(f"IOF={iof_strength:.2f}→×0.70")
        else:
            parts.append(f"IOF={iof_strength:.2f}→×1.00")

        # Macro
        sentiment = macro.get("sentiment", "neutral")
        dxy = macro.get("dxy_trend", "neutral")
        macro_mult = 1.0
        ac = asset_class.lower()
        if ac in ("metals", "gold"):
            if (direction == 1 and dxy == "falling") or (direction == -1 and dxy == "rising"):
                macro_mult = 1.20
            elif (direction == 1 and dxy == "rising") or (direction == -1 and dxy == "falling"):
                macro_mult = 0.80
        elif ac in ("equity", "index", "futures"):
            if (direction == 1 and sentiment == "risk_on") or (direction == -1 and sentiment == "risk_off"):
                macro_mult = 1.20
            elif (direction == 1 and sentiment == "risk_off") or (direction == -1 and sentiment == "risk_on"):
                macro_mult = 0.80
        elif ac in ("crypto", "perpetual"):
            if direction == 1 and sentiment == "risk_on":
                macro_mult = 1.20
            elif direction == 1 and sentiment == "risk_off":
                macro_mult = 0.80
        L *= macro_mult
        parts.append(f"macro→×{macro_mult:.2f}")
        if macro_mult > 1.0:
            active_factors.append("macro")

        # Hurst
        model = model_source.lower()
        hurst_factor_name: str | None = None
        if hurst < 0.45:
            if model == "spline":
                L *= 1.30; parts.append(f"H={hurst:.3f}+spline→×1.30")
                hurst_factor_name = "hurst_spline"
            elif model == "knn":
                L *= 0.75; parts.append(f"H={hurst:.3f}+knn→×0.75")
            else:
                parts.append(f"H={hurst:.3f}→×1.00")
        elif hurst > 0.55:
            if model == "knn":
                L *= 1.30; parts.append(f"H={hurst:.3f}+knn→×1.30")
                hurst_factor_name = "hurst_knn"
            elif model == "spline":
                L *= 0.75; parts.append(f"H={hurst:.3f}+spline→×0.75")
            else:
                parts.append(f"H={hurst:.3f}→×1.00")
        else:
            parts.append(f"H={hurst:.3f}→×1.00")

        if hurst_factor_name:
            active_factors.append(hurst_factor_name)

        # ── De-correlation Penalty (HIGH-01 fix) ─────────────────
        # Find the maximum pairwise correlation among active boosting factors.
        # If correlated indicators are both boosting, dampen the combined
        # likelihood to prevent double-counting shared evidence.
        rho_max = 0.0
        if len(active_factors) >= 2:
            for i in range(len(active_factors)):
                for j in range(i + 1, len(active_factors)):
                    pair = frozenset({active_factors[i], active_factors[j]})
                    rho = INDICATOR_CORRELATION_MATRIX.get(pair, 0.0)
                    rho_max = max(rho_max, rho)

        if rho_max > 0.0 and L > 1.0:
            # Square-root penalty: L_adj = L ^ (1 / sqrt(1 + ρ))
            # When ρ=0 → exponent=1.0 (no change)
            # When ρ=0.6 → exponent≈0.79 (dampens L=1.95 to ~1.73)
            # When ρ=1.0 → exponent≈0.71 (maximum dampening)
            exponent = 1.0 / math.sqrt(1.0 + rho_max)
            L_raw = L
            L = L ** exponent
            parts.append(f"ρ_max={rho_max:.2f}→L^{exponent:.3f}={L:.4f} (was {L_raw:.4f})")

        # Clamp
        raw_L = L
        L = max(0.3, min(3.0, L))
        if L != raw_L:
            parts.append(f"CLAMPED:{raw_L:.4f}→{L:.4f}")

        return L, " | ".join(parts)

    # ------------------------------------------------------------------
    # Posterior
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_posterior(prior: float, likelihood: float) -> float:
        """Odds-form Bayes: numerically stable posterior computation."""
        prior_odds = prior / (1.0 - prior)
        posterior_odds = prior_odds * likelihood
        return max(0.0001, min(0.9999, posterior_odds / (1.0 + posterior_odds)))

    @staticmethod
    def _apply_power_prior(prior: float, a0: float) -> float:
        """
        Power Prior discount: P_regime(A) = P(A) ^ a0.

        a0=1.0 → full historical trust (Conservative)
        a0=0.5 → sqrt of historical rate (Active)
        a0=0.15 → near-flat prior (Aggressive: live data dominates)

        The result is clamped to (0.05, 0.95) for numerical safety.
        """
        discounted = prior ** a0
        return max(0.05, min(0.95, discounted))

    @staticmethod
    def _apply_tempering(likelihood: float, T: float) -> float:
        """
        Likelihood Tempering: L_tempered = L ^ (1/T).

        T=1.0 → raw likelihood (Conservative: full penalty curve)
        T=1.5 → softened curve (Active: borderline setups pass easier)
        T=2.5 → heavily flattened (Aggressive: reduced impact of IOF/macro)

        Result clamped to [0.3, 3.0] after tempering.
        """
        if T <= 0:
            T = 1.0
        tempered = likelihood ** (1.0 / T)
        return max(0.3, min(3.0, tempered))

    # ------------------------------------------------------------------
    # Tri-State Evaluation
    # ------------------------------------------------------------------

    def _evaluate_regime(
        self,
        regime_name: str,
        regime_cfg: dict[str, Any],
        base_prior: float,
        base_likelihood: float,
        asset_class: str,
    ) -> dict[str, Any]:
        """
        Evaluate a single regime against the signal.

        Selection Bias Guard:
            For regimes with a0 < 0.30 (e.g., AGGRESSIVE), the flat power
            prior makes historical win rate nearly irrelevant. To prevent
            authorizing assets with genuinely poor track records purely on
            live momentum, a minimum base prior floor of 0.40 is enforced.
            If the raw historical win rate is below 40%, the regime REJECTS
            regardless of posterior — this is a structural safeguard against
            selection bias, not a Bayesian one.

        Returns per-regime decision dict:
            {regime, posterior, decision, threshold, power_prior, tempered_L}
        """
        a0 = regime_cfg.get("prior_discount_a0", 1.0)
        T = regime_cfg.get("tempering_T", 1.0)
        threshold = regime_cfg.get("threshold", 0.695)

        # ── Selection Bias Guard ──────────────────────────────────
        # Flat-prior regimes (a0 < 0.30) can authorize signals from
        # assets with terrible historical win rates. Require base
        # prior >= 0.40 to prevent structural selection bias.
        selection_bias_blocked = False
        if a0 < 0.30 and base_prior < 0.40:
            selection_bias_blocked = True

        # Apply Power Prior
        regime_prior = self._apply_power_prior(base_prior, a0)

        # Apply Likelihood Tempering
        regime_likelihood = self._apply_tempering(base_likelihood, T)

        # Compute posterior
        posterior = self._compute_posterior(regime_prior, regime_likelihood)

        # Decision (with selection bias override)
        if selection_bias_blocked:
            decision = Decision.REJECT
        else:
            decision = Decision.AUTHORIZE if posterior > threshold else Decision.REJECT

        return {
            "regime": regime_name,
            "label": regime_cfg.get("label", regime_name),
            "posterior": round(posterior, 6),
            "decision": decision.value,
            "threshold": threshold,
            "power_prior": round(regime_prior, 6),
            "tempered_L": round(regime_likelihood, 6),
            "a0": a0,
            "T": T,
            "kelly_fraction": regime_cfg.get("kelly_fraction", 0.25),
            "max_cap_pct": regime_cfg.get("max_cap_pct", 0.025),
            "selection_bias_blocked": selection_bias_blocked,
        }

    async def evaluate(self, signal: dict[str, Any]) -> dict[str, Any]:
        """
        Full Tri-State Bayesian evaluation of a signal.

        Evaluates all three regimes in parallel and returns the
        active regime's decision for routing, plus the full tri-state
        audit log for shadow analysis and CPCV optimization.

        Returns:
            {signal_id, posterior_probability, decision, routing_target,
             reasoning_math, tri_state, active_regime}
        """
        signal_id = signal.get("signal_id", str(uuid.uuid4()))
        asset_symbol = signal.get("asset_symbol", "UNKNOWN")
        asset_class = signal.get("asset_class", "equity")
        desk_id = signal.get("desk_id", "unknown")
        direction = signal.get("signal_direction", 1)

        # Step 1: Prior (shared base across regimes)
        base_prior = await self._fetch_prior(asset_symbol, desk_id)

        # Step 2: Macro
        macro = await self._fetch_macro()

        # Step 3: Base Likelihood (shared — regimes apply tempering on top)
        base_likelihood, reasoning = self._compute_likelihood(
            iof_strength=signal.get("iof_strength", 0.5),
            macro=macro,
            hurst=signal.get("hurst_exponent", 0.5),
            model_source=signal.get("model_source", "unknown"),
            direction=direction,
            asset_class=asset_class,
        )

        # Step 4: Tri-State parallel evaluation
        regimes = self._risk_profiles.get("regimes", {})
        active_regime_name = self._risk_profiles.get("active_regime", "conservative")

        tri_state: dict[str, dict[str, Any]] = {}
        for regime_name, regime_cfg in regimes.items():
            tri_state[regime_name] = self._evaluate_regime(
                regime_name, regime_cfg, base_prior, base_likelihood, asset_class,
            )

        # Step 5: Active regime controls routing
        active_result = tri_state.get(active_regime_name)
        if active_result is None:
            # Fallback to conservative if active regime not found
            active_result = tri_state.get("conservative", next(iter(tri_state.values())))
            active_regime_name = active_result["regime"]

        posterior = active_result["posterior"]
        if active_result["decision"] == "AUTHORIZE":
            decision = Decision.AUTHORIZE
            routing = ROUTING_MAP.get(asset_class.lower())
            self._authorized += 1
        else:
            decision = Decision.REJECT
            routing = None
            self._rejected += 1

        self._evaluated += 1

        # Build reasoning string
        regime_summary = " | ".join(
            f"{r['label']}:{r['decision']}(P={r['posterior']:.4f}>{r['threshold']})"
            for r in tri_state.values()
        )
        math_str = (
            f"P(A)={base_prior:.4f} × L={base_likelihood:.4f} "
            f"→ [{regime_summary}] "
            f"ACTIVE={active_regime_name}:P(A|B)={posterior:.6f}"
        )
        full_reasoning = f"{math_str} [{reasoning}]"

        # Tri-state decision map for audit log
        tri_state_decisions = {
            r["label"]: r["decision"] for r in tri_state.values()
        }

        # Check if ANY regime authorized (for shadow mode routing)
        any_authorized = any(r["decision"] == "AUTHORIZE" for r in tri_state.values())

        output = {
            "signal_id": signal_id,
            "posterior_probability": posterior,
            "decision": decision.value,
            "routing_target": routing,
            "reasoning_math": full_reasoning,
            "tri_state": tri_state,
            "tri_state_decisions": tri_state_decisions,
            "any_regime_authorized": any_authorized,
            "active_regime": active_regime_name,
            "base_prior": round(base_prior, 6),
            "base_likelihood": round(base_likelihood, 6),
        }

        await log.ainfo(
            "bayesian_tri_state_decision",
            signal_id=signal_id,
            symbol=asset_symbol,
            active_regime=active_regime_name,
            active_posterior=posterior,
            active_decision=decision.value,
            tri_state_decisions=tri_state_decisions,
        )

        return output

    def reload_risk_profiles(self) -> None:
        """Hot-reload risk profiles from YAML (called by dashboard deploy)."""
        self._risk_profiles = _load_risk_profiles()

    @property
    def active_regime(self) -> str:
        return self._risk_profiles.get("active_regime", "conservative")

    @property
    def stats(self) -> dict[str, Any]:
        total = max(self._evaluated, 1)
        return {
            "evaluated": self._evaluated,
            "authorized": self._authorized,
            "rejected": self._rejected,
            "auth_rate": round(self._authorized / total, 4),
            "active_regime": self.active_regime,
        }
