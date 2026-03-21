"""
OniQuant v6.0 — Orchestration Worker
=======================================
Consumes signals from oniquant:raw_signals via Consumer Group.
Enriches with Alpha Stack math, passes to BayesianArbiter,
routes AUTHORIZED signals to pending_trades, logs REJECTED to TimescaleDB.

Resilience:
    - XREADGROUP with explicit XACK (at-least-once delivery)
    - Failed signals stay in PEL for reclaim by another worker
    - Dead Letter Queue after 3 failed attempts

Deploy: railway run python -m workers.orchestration_worker
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from app.core.config import get_settings
from app.core.database import init_pool, insert_signal
from app.core.kill_switch import is_kill_switch_active
from app.services.orchestrator import BayesianArbiter

log = structlog.get_logger("oniquant.orchestration_worker")

REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
INPUT_STREAM: str = "oniquant:raw_signals"
CONSUMER_GROUP: str = "orchestrator_cg"
CONSUMER_NAME: str = os.getenv("CONSUMER_NAME", f"orch_worker_{os.getpid()}")
PENDING_ZSET: str = "oniquant:pending_trades"
DLQ_STREAM: str = "oniquant:dead_letter_queue"
MAX_RETRIES: int = 3

# ---------------------------------------------------------------------------
# Lua Script: Atomic XACK + ZADD
# ---------------------------------------------------------------------------
# Guarantees that acknowledging a message and adding the trade to the
# pending ZSET happen as a single atomic operation. If the worker crashes
# mid-execution, Redis either completes BOTH or NEITHER — eliminating
# the ghost-trade duplication vector from the Red Team audit (CRITICAL-02).
#
# KEYS[1] = input stream    (oniquant:raw_signals)
# KEYS[2] = consumer group  (orchestrator_cg)
# KEYS[3] = pending ZSET    (oniquant:pending_trades)
# ARGV[1] = message ID      (e.g., "1679000000000-0")
# ARGV[2] = ZSET member     (orjson-encoded trade payload bytes)
# ARGV[3] = ZSET score      (expiry timestamp as string)
# ---------------------------------------------------------------------------
LUA_ATOMIC_ACK_AND_ROUTE = """
redis.call('XACK', KEYS[1], KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
return 1
"""


class OrchestrationWorker:
    """
    Stream consumer that orchestrates the full signal pipeline.

    Signal Flow:
        1. XREADGROUP from oniquant:raw_signals
        2. Enrich with Alpha Stack metadata
        3. BayesianArbiter → AUTHORIZE / REJECT
        4. AUTHORIZE → ZADD to oniquant:pending_trades (Memory Engine)
        5. REJECT → INSERT to signal_ledger (TimescaleDB audit log)
        6. XACK on successful processing
        7. Failed 3× → move to Dead Letter Queue
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._redis_url = redis_url
        self._pool: aioredis.Redis | None = None
        self._arbiter: BayesianArbiter | None = None
        self._running: bool = False
        self._processed: int = 0
        self._retry_counts: dict[str, int] = {}
        self._atomic_ack_route: Any | None = None  # registered Lua script

    async def connect(self) -> None:
        self._pool = aioredis.from_url(
            self._redis_url,
            decode_responses=False,
            max_connections=15,
            socket_connect_timeout=5.0,
            socket_keepalive=True,
        )
        await self._pool.ping()

        # Initialize database pool
        try:
            await init_pool()
        except Exception as e:
            await log.awarning("db_init_deferred", error=str(e))

        self._arbiter = BayesianArbiter(self._pool)

        # Register Lua script for atomic XACK + ZADD (CRITICAL-02 fix)
        self._atomic_ack_route = self._pool.register_script(LUA_ATOMIC_ACK_AND_ROUTE)

        # Create consumer group (idempotent)
        try:
            await self._pool.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        await log.ainfo("orchestration_worker_started", consumer=CONSUMER_NAME)

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.aclose()
        await log.ainfo(
            "orchestration_worker_stopped",
            processed=self._processed,
            stats=self._arbiter.stats if self._arbiter else {},
        )

    async def _process_signal(self, msg_id: bytes, payload: dict[str, Any]) -> bool:
        """
        Process a single signal through the pipeline.

        Kill Switch Gate (CRITICAL-01 fix):
            Checked before ANY authorization to ensure halted state
            is respected even for signals already in the stream.

        Atomic ACK+ZADD (CRITICAL-02 fix):
            AUTHORIZED signals are routed via a Lua script that performs
            XACK and ZADD as a single atomic Redis operation, eliminating
            the ghost-trade duplication vector on crash recovery.

        Returns True if successfully processed, False on error.
        """
        signal_id = payload.get("signal_id", str(uuid.uuid4()))

        # ── Kill Switch Gate ──────────────────────────────────────
        if await is_kill_switch_active(self._pool):
            await log.acritical(
                "kill_switch_blocked_signal",
                signal_id=signal_id,
                symbol=payload.get("asset_symbol"),
            )
            # ACK the message so it doesn't re-enter the pipeline,
            # but do NOT route it — the signal is silently dropped.
            await self._pool.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
            return True

        # Step 1: Enrich — pull live WFO params from Redis
        desk_id = payload.get("desk_id", "luxalgo")
        live_params = await self._pool.get(f"oniquant:config:{desk_id}")
        if live_params:
            payload["live_wfo_params"] = orjson.loads(live_params)

        # Step 2: Bayesian evaluation
        decision = await self._arbiter.evaluate(payload)

        if decision["decision"] == "AUTHORIZE":
            # Step 3a: Atomic XACK + ZADD via Lua script (CRITICAL-02)
            now = time.time()
            ttl = payload.get("ttl_seconds", 300)
            expiry = now + ttl

            trade_payload = {
                **payload,
                "signal_id": signal_id,
                "posterior_probability": decision["posterior_probability"],
                "routing_target": decision["routing_target"],
                "authorized_at": now,
            }

            await self._atomic_ack_route(
                keys=[INPUT_STREAM, CONSUMER_GROUP, PENDING_ZSET],
                args=[msg_id, orjson.dumps(trade_payload), str(expiry)],
            )
        else:
            # Step 3b: Log rejected signal to TimescaleDB, then ACK
            try:
                await insert_signal(
                    ts=datetime.now(timezone.utc),
                    desk_id=payload.get("desk_id", "unknown"),
                    asset_symbol=payload.get("asset_symbol", "UNKNOWN"),
                    signal_direction=payload.get("signal_direction", 0),
                    target_price=payload.get("target_price", 0),
                    confidence=payload.get("confidence", 0),
                    indicator_metadata={
                        "decision": "REJECT",
                        "posterior": decision["posterior_probability"],
                        "reasoning": decision["reasoning_math"],
                    },
                )
            except Exception as e:
                await log.awarning("reject_log_error", error=str(e))

            # ACK rejected signals (non-atomic is safe — reprocessing
            # a rejection is idempotent and produces no trade)
            await self._pool.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)

        self._processed += 1
        return True

    async def _handle_dead_letter(self, msg_id: bytes, payload: dict[str, Any]) -> None:
        """Move a signal to the Dead Letter Queue after MAX_RETRIES failures."""
        await self._pool.xadd(
            DLQ_STREAM,
            {
                "payload": orjson.dumps(payload),
                "original_msg_id": msg_id,
                "reason": b"max_retries_exceeded",
                "failed_at": str(time.time()).encode(),
            },
            maxlen=10000,
            approximate=True,
        )
        # ACK to remove from PEL
        await self._pool.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
        await log.awarning("signal_to_dlq", msg_id=msg_id)

    async def _reclaim_pending(self) -> None:
        """
        Reclaim stale messages from the PEL (Pending Entries List).

        Messages idle for >30s are likely from crashed workers.
        We XCLAIM them and re-process.
        """
        try:
            pending = await self._pool.xpending_range(
                INPUT_STREAM, CONSUMER_GROUP, min="-", max="+", count=50
            )
            for entry in pending:
                msg_id = entry["message_id"]
                idle_ms = entry["time_since_delivered"]
                delivery_count = entry["times_delivered"]

                if idle_ms > 30000:  # 30 seconds idle
                    if delivery_count >= MAX_RETRIES:
                        # DLQ — too many retries
                        claimed = await self._pool.xclaim(
                            INPUT_STREAM, CONSUMER_GROUP, CONSUMER_NAME,
                            min_idle_time=30000, message_ids=[msg_id]
                        )
                        for cid, cdata in claimed:
                            raw = cdata.get(b"payload", b"{}")
                            await self._handle_dead_letter(cid, orjson.loads(raw))
                    else:
                        # Reclaim for reprocessing
                        await self._pool.xclaim(
                            INPUT_STREAM, CONSUMER_GROUP, CONSUMER_NAME,
                            min_idle_time=30000, message_ids=[msg_id]
                        )
        except Exception as e:
            await log.adebug("reclaim_error", error=str(e))

    async def run(self) -> None:
        """Main consumer loop with PEL reclaim and DLQ support."""
        self._running = True

        cycle = 0
        while self._running:
            try:
                # Periodic PEL reclaim (every 100 cycles ≈ 100s)
                if cycle % 100 == 0:
                    await self._reclaim_pending()

                # Read new messages
                messages = await self._pool.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={INPUT_STREAM: ">"},
                    count=10,
                    block=1000,
                )

                if not messages:
                    cycle += 1
                    continue

                for stream_name, entries in messages:
                    for msg_id, msg_data in entries:
                        try:
                            raw = msg_data.get(b"payload", b"{}")
                            payload = orjson.loads(raw)
                            # XACK is now handled atomically inside
                            # _process_signal (via Lua for AUTHORIZE,
                            # via explicit call for REJECT/kill-switch).
                            await self._process_signal(msg_id, payload)
                        except Exception as e:
                            await log.aerror(
                                "signal_processing_error",
                                msg_id=msg_id, error=str(e),
                            )
                            # Don't ACK — stays in PEL for reclaim

                cycle += 1

                # Heartbeat
                if cycle % 500 == 0:
                    await log.ainfo(
                        "orchestration_heartbeat",
                        processed=self._processed,
                        stats=self._arbiter.stats if self._arbiter else {},
                    )

            except aioredis.ConnectionError as e:
                await log.aerror("redis_lost", error=str(e))
                await asyncio.sleep(2.0)
            except Exception as e:
                await log.aerror("worker_error", error=str(e))
                await asyncio.sleep(0.5)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    worker = OrchestrationWorker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.connect()
        await worker.run()
    finally:
        await worker.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
