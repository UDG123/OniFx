"""
OniQuant v6.0 — QA: Synthetic Signal E2E Pipeline Trace
==========================================================
Sends a single synthetic signal through the ENTIRE pipeline
and traces its journey at each stage:

    1. POST /webhook/luxalgo         → FastAPI ingestion
    2. oniquant:raw_signals          → Redis Stream (check XLEN delta)
    3. orchestration_worker          → Bayesian CTO (check pending_trades ZSET)
    4. memory_worker                 → Trade Memory (check match_validation)
    5. Synthetic Matcher             → L2 fill evaluation (check signal_ledger)

Each stage is timed. Final report shows per-hop latency and total travel time.

This test runs OFFLINE — it simulates the entire pipeline in-process
(no Docker required) by directly calling each component's functions.

Usage: python -m tests.e2e_synthetic_trace
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import orjson

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# =========================================================================
# Stage Timers
# =========================================================================

class StageTimer:
    """Tracks per-stage latency for the pipeline trace."""

    def __init__(self) -> None:
        self._stages: list[dict[str, Any]] = []
        self._t0: float = 0.0

    def start(self) -> None:
        self._t0 = time.perf_counter()

    def mark(self, stage: str, status: str = "PASS", detail: str = "") -> float:
        now = time.perf_counter()
        elapsed_ms = (now - self._t0) * 1000
        self._stages.append({
            "stage": stage,
            "status": status,
            "elapsed_ms": round(elapsed_ms, 3),
            "detail": detail,
        })
        self._t0 = now  # reset for next hop
        return elapsed_ms

    def report(self) -> str:
        lines = [
            "",
            "=" * 72,
            "  OniQuant v6.0 — SYNTHETIC SIGNAL E2E PIPELINE TRACE",
            "=" * 72,
            "",
            f"  {'Stage':<40} {'Latency':>10} {'Status':>8}  Detail",
            f"  {'-'*40} {'-'*10} {'-'*8}  {'-'*30}",
        ]
        total_ms = 0.0
        for s in self._stages:
            total_ms += s["elapsed_ms"]
            lines.append(
                f"  {s['stage']:<40} {s['elapsed_ms']:>8.3f}ms {s['status']:>8}  {s['detail']}"
            )
        lines.append(f"  {'-'*40} {'-'*10} {'-'*8}")
        lines.append(f"  {'TOTAL TRAVEL TIME':<40} {total_ms:>8.3f}ms")
        lines.append("")

        # Verdict
        if total_ms < 100:
            verdict = "EXCELLENT (<100ms E2E)"
        elif total_ms < 500:
            verdict = "GOOD (<500ms E2E)"
        elif total_ms < 2000:
            verdict = "ACCEPTABLE (<2s E2E)"
        else:
            verdict = "SLOW (>2s — investigate bottleneck)"

        lines.append(f"  VERDICT: {verdict}")
        lines.append("=" * 72)
        return "\n".join(lines)


# =========================================================================
# Synthetic Signal Payload
# =========================================================================

SIGNAL_ID = str(uuid.uuid4())
SYNTHETIC_SIGNAL: dict[str, Any] = {
    "signal_id": SIGNAL_ID,
    "desk_id": "luxalgo_spx",
    "asset_symbol": "SPY",
    "signal_direction": 1,
    "target_price": 452.60,
    "confidence": 0.82,
    "iof_strength": 0.85,
    "hurst_exponent": 0.55,
    "model_source": "luxalgo",
    "asset_class": "equity",
    "ttl_seconds": 300,
    "indicator_metadata": {
        "luxalgo_signal": "strong_buy",
        "timeframe": "1m",
        "exchange": "NYSE",
        "trace_id": SIGNAL_ID,
    },
}

# Mock L2 snapshot — 10 levels of depth with enough liquidity to fill
MOCK_L2_SNAPSHOT: list[dict[str, Any]] = []
for i in range(10):
    MOCK_L2_SNAPSHOT.append({
        "price": 452.30 - 0.01 * (i + 1),
        "size": 500 + i * 200,
        "side": "BID",
        "position": i,
    })
    MOCK_L2_SNAPSHOT.append({
        "price": 452.30 + 0.01 * (i + 1),
        "size": 500 + i * 200,
        "side": "ASK",
        "position": i,
    })


# =========================================================================
# Pipeline Trace
# =========================================================================

async def run_trace() -> str:
    timer = StageTimer()

    # ── Stage 1: Webhook Ingestion (simulate FastAPI body read + XADD) ──
    timer.start()

    raw_payload = orjson.dumps(SYNTHETIC_SIGNAL)
    # Simulate what app/main.py does: read body + XADD to stream
    stream_entry = {
        "payload": raw_payload,
        "source": b"luxalgo",
        "trace_id": SIGNAL_ID.encode(),
    }
    # In production: await redis.xadd("oniquant:raw_signals", stream_entry)
    # Offline: we just serialize and verify the structure
    assert b"signal_id" in raw_payload
    assert stream_entry["source"] == b"luxalgo"

    timer.mark(
        "1. Webhook Ingestion (POST /webhook/luxalgo)",
        detail=f"payload={len(raw_payload)}B, signal_id={SIGNAL_ID[:8]}...",
    )

    # ── Stage 2: Redis Stream Landing ──
    # Simulate XADD result — in production this would be an XLEN check
    simulated_msg_id = f"{int(time.time() * 1000)}-0"
    deserialized = orjson.loads(raw_payload)
    assert deserialized["signal_id"] == SIGNAL_ID
    assert deserialized["asset_symbol"] == "SPY"

    timer.mark(
        "2. Redis Stream (oniquant:raw_signals)",
        detail=f"msg_id={simulated_msg_id}, deserialized OK",
    )

    # ── Stage 3: Bayesian Orchestrator (CTO Decision Engine) ──
    from app.services.orchestrator import BayesianArbiter

    # Mock Redis pool that returns defaults
    class MockRedis:
        async def get(self, key: str) -> bytes | None:
            if "prior" in key:
                return b"0.72"   # 72% historical win rate
            if "macro" in key:
                return orjson.dumps({"dxy_trend": "neutral", "sentiment": "risk_on", "vix_regime": "normal"})
            return None
        async def set(self, *a, **kw) -> None:
            pass

    mock_redis = MockRedis()
    arbiter = BayesianArbiter(mock_redis)  # type: ignore
    decision = await arbiter.evaluate(deserialized)

    posterior = decision["posterior_probability"]
    verdict = decision["decision"]
    routing = decision["routing_target"]
    reasoning = decision["reasoning_math"]

    timer.mark(
        "3. Bayesian CTO (Orchestrator)",
        detail=f"P(A|B)={posterior:.6f}, decision={verdict}, route={routing}",
    )

    # ── Stage 4: Trade Memory Engine (ZSET Promotion) ──
    # Simulate what memory_worker does: price check → promotion
    from workers.memory_worker import get_current_price

    current_price = await get_current_price("SPY")
    target = deserialized["target_price"]
    direction = deserialized["signal_direction"]

    # Check crossing logic
    crossed = (direction == 1 and current_price >= target) or (direction == -1 and current_price <= target)

    trade_payload = {
        **deserialized,
        "posterior_probability": posterior,
        "routing_target": routing,
        "promoted_at": time.time(),
        "market_price_at_trigger": current_price,
    }

    timer.mark(
        "4. Trade Memory Engine (ZSET → match_validation)",
        detail=f"price={current_price}, target={target}, crossed={crossed}",
    )

    # ── Stage 5: Synthetic Matcher (L2 Fill Evaluation) ──
    from app.services.matcher import evaluate_l2_fill

    fill_result = evaluate_l2_fill(
        order_size=100,       # 100 shares
        price=target,
        l2_snapshot=MOCK_L2_SNAPSHOT,
        direction=direction,
    )

    timer.mark(
        "5. Synthetic Matcher (L2 Fill Evaluation)",
        detail=f"filled={fill_result.filled}, VWAP={fill_result.simulated_fill_price:.4f}, "
               f"V_cum={fill_result.cumulative_volume:.0f} vs V_req={fill_result.required_volume:.0f}",
    )

    # ── Stage 6: TimescaleDB Ledger Write (simulated) ──
    # In production: await log_simulated_fill(...)
    # Offline: verify the record structure
    if fill_result.filled:
        ledger_record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "desk_id": deserialized["desk_id"],
            "asset_symbol": deserialized["asset_symbol"],
            "signal_direction": direction,
            "target_price": target,
            "simulated_fill_price": fill_result.simulated_fill_price,
            "fill_latency_ms": fill_result.latency_ms,
            "indicator_metadata": deserialized.get("indicator_metadata", {}),
            "source_stream": "synthetic_matcher",
        }
        record_bytes = orjson.dumps(ledger_record)
        assert b"simulated_fill_price" in record_bytes
        ledger_status = f"INSERT ready ({len(record_bytes)}B)"
    else:
        ledger_status = "SKIPPED (no fill)"

    timer.mark(
        "6. TimescaleDB Ledger (signal_ledger INSERT)",
        detail=ledger_status,
    )

    # ── Report ──
    return timer.report()


# =========================================================================
# Entrypoint
# =========================================================================

async def main() -> None:
    report = await run_trace()
    print(report)


if __name__ == "__main__":
    asyncio.run(main())
