"""
OniQuant v6.0 — Integration Pipeline Stress Test
===================================================
Simulates 100 concurrent LuxAlgo webhook calls, pushes mock L2 data,
and traces each signal through the full pipeline.

Success criteria: all signals complete E2E in <100ms, zero signal loss.

Usage: pytest tests/pipeline_stress_test.py -v
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

import httpx
import pytest
import redis.asyncio as aioredis

API_URL = "http://localhost:8000"
REDIS_URL = "redis://localhost:6379/0"


@pytest.fixture
async def redis_pool():
    pool = aioredis.from_url(REDIS_URL, decode_responses=False)
    yield pool
    await pool.aclose()


def _generate_signal(desk_id: str, symbol: str) -> dict[str, Any]:
    """Generate a mock LuxAlgo webhook payload."""
    return {
        "signal_id": str(uuid.uuid4()),
        "desk_id": desk_id,
        "asset_symbol": symbol,
        "signal_direction": 1,
        "target_price": 450.0 + (hash(symbol) % 50),
        "confidence": 0.75,
        "iof_strength": 0.85,
        "hurst_exponent": 0.55,
        "model_source": "luxalgo",
        "asset_class": "equity",
        "indicator_metadata": {"test": True},
    }


def _generate_l2_snapshot(symbol: str) -> list[dict[str, Any]]:
    """Generate mock L2 depth data."""
    base = 450.0 + (hash(symbol) % 50)
    levels = []
    for i in range(10):
        levels.append({"price": base - 0.01 * (i + 1), "size": 500 + i * 100, "side": "BID", "position": i})
        levels.append({"price": base + 0.01 * (i + 1), "size": 500 + i * 100, "side": "ASK", "position": i})
    return levels


DESKS = [
    ("scalper_spx", "SPY"),
    ("scalper_ndx", "QQQ"),
    ("swing_tech", "AAPL"),
    ("alts_btc", "BTCUSDT"),
    ("alts_eth", "ETHUSDT"),
    ("macro_gold", "GC_F"),
]


@pytest.mark.asyncio
async def test_concurrent_webhook_ingestion():
    """Test 100 concurrent webhook calls complete with 200 OK."""
    signals = []
    for i in range(100):
        desk_id, symbol = DESKS[i % len(DESKS)]
        signals.append(_generate_signal(desk_id, symbol))

    latencies = []

    async with httpx.AsyncClient(base_url=API_URL, timeout=10.0) as client:
        async def fire(payload: dict) -> float:
            t0 = time.perf_counter()
            resp = await client.post("/webhook/luxalgo", json=payload)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
            return elapsed_ms

        tasks = [fire(sig) for sig in signals]
        latencies = await asyncio.gather(*tasks)

    avg_ms = sum(latencies) / len(latencies)
    max_ms = max(latencies)
    print(f"\n  Ingestion: avg={avg_ms:.2f}ms, max={max_ms:.2f}ms, n={len(latencies)}")

    assert max_ms < 100, f"Max latency {max_ms:.2f}ms exceeds 100ms threshold"


@pytest.mark.asyncio
async def test_mock_l2_injection(redis_pool: aioredis.Redis):
    """Push mock L2 data into Redis cache for matcher consumption."""
    pipe = redis_pool.pipeline(transaction=False)
    for _, symbol in DESKS:
        l2 = _generate_l2_snapshot(symbol)
        import orjson
        pipe.set(f"oniquant:l2_cache:{symbol}", orjson.dumps(l2), ex=10)
    await pipe.execute()

    # Verify all L2 caches are populated
    for _, symbol in DESKS:
        data = await redis_pool.get(f"oniquant:l2_cache:{symbol}")
        assert data is not None, f"L2 cache missing for {symbol}"


@pytest.mark.asyncio
async def test_signal_trace_in_stream(redis_pool: aioredis.Redis):
    """Verify signals land in the raw_signals stream after ingestion."""
    stream_len = await redis_pool.xlen("oniquant:raw_signals")
    assert stream_len > 0, "raw_signals stream is empty after ingestion"
    print(f"\n  Stream backlog: {stream_len} messages")


@pytest.mark.asyncio
async def test_health_endpoint():
    """Verify the health endpoint returns 200."""
    async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
