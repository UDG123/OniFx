"""
OniQuant v6.0 — Bayesian Decision Engine (Orchestrator)
=========================================================
Final arbiter for all Alpha Stack signals.
Authorizes or rejects based on Bayesian Posterior > 69.5%.

Bayes' Theorem (Odds Form):
    posterior_odds = prior_odds × likelihood_ratio
    P(A|B) = posterior_odds / (1 + posterior_odds)

Likelihood Multipliers:
    IOF > 80% → ×1.50 | Macro aligned → ×1.20 | Hurst regime match → ×1.30
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from app.core.config import get_settings
from app.core.database import query_win_rate

log = structlog.get_logger("oniquant.orchestrator")


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
    Bayesian decision engine with configurable likelihood multipliers.

    Pipeline per signal:
        1. Fetch Prior P(A) from TimescaleDB (30-day win rate).
        2. Compute Likelihood via IOF, Macro, Hurst adjustments.
        3. Compute Posterior via odds-form Bayes.
        4. AUTHORIZE if posterior > 69.5%, else REJECT.
    """

    def __init__(self, redis_pool: aioredis.Redis) -> None:
        self._redis = redis_pool
        self._settings = get_settings()
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
        Multiplicative likelihood adjustment.

        Factor 1: IOF — >0.80 → ×1.50, >0.60 → ×1.20, <0.30 → ×0.70
        Factor 2: Macro — aligned → ×1.20, opposed → ×0.80
        Factor 3: Hurst — H<0.45+Spline → ×1.30, H>0.55+kNN → ×1.30, mismatch → ×0.75
        """
        L = 1.0
        parts = []

        # IOF
        if iof_strength > 0.80:
            L *= 1.50; parts.append(f"IOF={iof_strength:.2f}→×1.50")
        elif iof_strength > 0.60:
            L *= 1.20; parts.append(f"IOF={iof_strength:.2f}→×1.20")
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

        # Hurst
        model = model_source.lower()
        if hurst < 0.45:
            if model == "spline": L *= 1.30; parts.append(f"H={hurst:.3f}+spline→×1.30")
            elif model == "knn": L *= 0.75; parts.append(f"H={hurst:.3f}+knn→×0.75")
            else: parts.append(f"H={hurst:.3f}→×1.00")
        elif hurst > 0.55:
            if model == "knn": L *= 1.30; parts.append(f"H={hurst:.3f}+knn→×1.30")
            elif model == "spline": L *= 0.75; parts.append(f"H={hurst:.3f}+spline→×0.75")
            else: parts.append(f"H={hurst:.3f}→×1.00")
        else:
            parts.append(f"H={hurst:.3f}→×1.00")

        # Clamp
        L = max(0.3, min(3.0, L))
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

    # ------------------------------------------------------------------
    # Evaluate Signal
    # ------------------------------------------------------------------

    async def evaluate(self, signal: dict[str, Any]) -> dict[str, Any]:
        """
        Full Bayesian evaluation of a signal.

        Returns the decision object:
            {signal_id, posterior_probability, decision, routing_target, reasoning_math}
        """
        signal_id = signal.get("signal_id", str(uuid.uuid4()))
        asset_symbol = signal.get("asset_symbol", "UNKNOWN")
        asset_class = signal.get("asset_class", "equity")
        desk_id = signal.get("desk_id", "unknown")
        direction = signal.get("signal_direction", 1)

        # Step 1: Prior
        prior = await self._fetch_prior(asset_symbol, desk_id)

        # Step 2: Macro
        macro = await self._fetch_macro()

        # Step 3: Likelihood
        likelihood, reasoning = self._compute_likelihood(
            iof_strength=signal.get("iof_strength", 0.5),
            macro=macro,
            hurst=signal.get("hurst_exponent", 0.5),
            model_source=signal.get("model_source", "unknown"),
            direction=direction,
            asset_class=asset_class,
        )

        # Step 4: Posterior
        posterior = self._compute_posterior(prior, likelihood)

        # Step 5: Decision
        floor = self._settings.posterior_floor
        if posterior > floor:
            decision = Decision.AUTHORIZE
            routing = ROUTING_MAP.get(asset_class.lower())
            self._authorized += 1
        else:
            decision = Decision.REJECT
            routing = None
            self._rejected += 1

        self._evaluated += 1

        math_str = f"P(A)={prior:.4f} × L={likelihood:.4f} → P(A|B)={posterior:.6f} {'>' if posterior > floor else '≤'} {floor}"
        full_reasoning = f"{math_str} [{reasoning}]"

        output = {
            "signal_id": signal_id,
            "posterior_probability": round(posterior, 6),
            "decision": decision.value,
            "routing_target": routing,
            "reasoning_math": full_reasoning,
        }

        await log.ainfo(
            "bayesian_decision",
            signal_id=signal_id,
            symbol=asset_symbol,
            posterior=round(posterior, 6),
            decision=decision.value,
        )

        return output

    @property
    def stats(self) -> dict[str, Any]:
        total = max(self._evaluated, 1)
        return {
            "evaluated": self._evaluated,
            "authorized": self._authorized,
            "rejected": self._rejected,
            "auth_rate": round(self._authorized / total, 4),
        }
