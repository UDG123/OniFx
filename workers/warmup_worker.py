"""
OniQuant v6.0 — Warmup Worker (Runs ONCE at Startup)
=======================================================
Fetches the last 1000 bars of 1-minute OHLCV for each desk asset,
caches in Redis, and pre-calculates initial kNN pivots + Spline boundaries.

Deploy: railway run python -m workers.warmup_worker
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import numpy as np
import orjson
import redis.asyncio as aioredis
import structlog
import yaml

from app.quant.alpha_stack import KNNMarketArchitecture, SplineQuantileRegression, IOFStrengthClassifier

log = structlog.get_logger("oniquant.warmup")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
SYMBOLS_PATH = os.getenv("SYMBOLS_PATH", "config/symbols.yaml")
HISTORY_PREFIX = "oniquant:history"
BASELINE_PREFIX = "oniquant:baseline"


def load_symbol_universe() -> dict[str, list[str]]:
    """Load desk → symbols mapping from config/symbols.yaml."""
    with open(SYMBOLS_PATH) as f:
        config = yaml.safe_load(f)
    return {desk_id: desk["symbols"] for desk_id, desk in config.get("desks", {}).items()}


def generate_mock_ohlcv(symbol: str, bars: int = 1000) -> dict[str, Any]:
    """Generate synthetic OHLCV for warmup (mock data source)."""
    rng = np.random.default_rng(seed=hash(symbol) % 2**31)
    S0 = 100.0 + (hash(symbol) % 400)
    log_ret = 0.00005 + 0.002 * rng.standard_normal(bars)
    close = S0 * np.exp(np.cumsum(log_ret))
    noise = rng.uniform(0.999, 1.001, (bars, 3))

    return {
        "symbol": symbol,
        "bars": bars,
        "open": (close * noise[:, 0]).tolist(),
        "high": (close * np.maximum(noise[:, 1], 1.0005)).tolist(),
        "low": (close * np.minimum(noise[:, 2], 0.9995)).tolist(),
        "close": close.tolist(),
        "volume": rng.lognormal(np.log(5000), 0.5, bars).tolist(),
    }


async def warmup_asset(
    pool: aioredis.Redis,
    desk_id: str,
    symbol: str,
) -> None:
    """Fetch history, cache, and pre-calculate baselines for one asset."""
    # Fetch OHLCV (mock — replace with IBKR/Bybit API in production)
    ohlcv = generate_mock_ohlcv(symbol, bars=1000)

    # Cache raw history in Redis Hash
    await pool.set(
        f"{HISTORY_PREFIX}:{symbol}",
        orjson.dumps(ohlcv),
        ex=86400,  # 24-hour TTL
    )

    # Pre-calculate kNN baseline
    close = np.array(ohlcv["close"])
    high = np.array(ohlcv["high"])
    low = np.array(ohlcv["low"])
    volume = np.array(ohlcv["volume"])
    opens = np.array(ohlcv["open"])

    # Build simple feature vectors for kNN
    returns = np.diff(close) / close[:-1]
    features = np.column_stack([
        returns[-100:],
        (high[-100:] - low[-100:]) / close[-100:],  # normalized range
        volume[-100:] / np.mean(volume[-200:-100]),   # relative volume
    ])
    labels = (returns[-100:] > 0).astype(float)

    knn = KNNMarketArchitecture(k=7)
    knn.fit(features, labels)
    baseline_prob = knn.predict_proba(features[-1])

    # Pre-calculate Spline boundaries
    spline = SplineQuantileRegression(window=50)
    spline.fit(close)
    spline_bounds = spline.evaluate(float(len(close) - 1))

    # Pre-calculate IOF
    iof = IOFStrengthClassifier(lookback=20)
    iof_scores = iof.score(opens, close, high, low, volume)
    current_iof = float(iof_scores[-1])

    # Store baselines
    baseline = {
        "symbol": symbol,
        "desk_id": desk_id,
        "knn_baseline_prob": baseline_prob,
        "spline_q05": spline_bounds["q05"],
        "spline_q50": spline_bounds["q50"],
        "spline_q95": spline_bounds["q95"],
        "iof_current": current_iof,
        "iof_class": iof.classify(current_iof),
        "warmup_at": time.time(),
    }

    await pool.set(
        f"{BASELINE_PREFIX}:{symbol}",
        orjson.dumps(baseline),
        ex=86400,
    )

    await log.ainfo("asset_warmed", symbol=symbol, desk=desk_id, knn_prob=baseline_prob, iof=current_iof)


async def main() -> None:
    pool = aioredis.from_url(REDIS_URL, decode_responses=False, max_connections=10)
    await pool.ping()

    universe = load_symbol_universe()
    tasks = []
    for desk_id, symbols in universe.items():
        for symbol in symbols:
            tasks.append(warmup_asset(pool, desk_id, symbol))

    await asyncio.gather(*tasks)
    await pool.aclose()
    await log.ainfo("warmup_complete", total_assets=sum(len(s) for s in universe.values()))


if __name__ == "__main__":
    asyncio.run(main())
