"""
OniQuant v6.0 — Mock Tick Engine (Synthetic Market Data)
==========================================================
Pushes synthetic ticks via Brownian Motion into Redis L2 feed
for all 6 desks during broker maintenance windows.

Usage: python scripts/mock_tick_engine.py
"""

from __future__ import annotations

import asyncio
import os
import time

import numpy as np
import orjson
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
L2_CACHE_PREFIX = "oniquant:l2_cache"
TICK_INTERVAL = 0.1  # 100ms between ticks (10 Hz)

SYMBOLS = {
    "SPY": 452.30,
    "QQQ": 385.15,
    "EURUSD": 1.0850,
    "BTCUSDT": 43250.00,
    "ETHUSDT": 2280.50,
    "SOLUSDT": 98.40,
}


async def run_mock_ticks():
    pool = aioredis.from_url(REDIS_URL, decode_responses=False)
    rng = np.random.default_rng()
    prices = dict(SYMBOLS)

    print(f"Mock Tick Engine started — {len(prices)} symbols at {1/TICK_INTERVAL:.0f} Hz")

    while True:
        pipe = pool.pipeline(transaction=False)

        for symbol, price in prices.items():
            # Brownian motion step
            price += price * rng.normal(0, 0.0002)
            prices[symbol] = price

            # Generate 10-level L2 snapshot
            levels = []
            for i in range(10):
                spread = price * 0.0001 * (i + 1)
                bid_size = int(rng.lognormal(np.log(500), 0.5))
                ask_size = int(rng.lognormal(np.log(500), 0.5))
                levels.append({"price": round(price - spread, 6), "size": bid_size, "side": "BID", "position": i})
                levels.append({"price": round(price + spread, 6), "size": ask_size, "side": "ASK", "position": i})

            pipe.set(f"{L2_CACHE_PREFIX}:{symbol}", orjson.dumps(levels), ex=5)

        await pipe.execute()
        await asyncio.sleep(TICK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run_mock_ticks())
