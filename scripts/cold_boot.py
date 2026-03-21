"""
OniQuant v6.0 — Cold-Boot State Reconstruction
=================================================
Runs when Redis is detected as empty (cluster crash recovery).
Reconstructs pending/active state from TimescaleDB signal_ledger.

Target: full operational state in <30 seconds.

Usage: python scripts/cold_boot.py
"""

from __future__ import annotations

import asyncio
import os
import time

import asyncpg
import orjson
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@timescaledb:5432/oniquant")


async def main():
    pool = aioredis.from_url(REDIS_URL, decode_responses=False)
    conn = await asyncpg.connect(DATABASE_URL)
    t0 = time.perf_counter()

    try:
        # Check if Redis is empty
        key_count = await pool.dbsize()
        if key_count > 10:
            print(f"Redis has {key_count} keys — not a cold boot. Exiting.")
            return

        print("Cold boot detected — reconstructing state from TimescaleDB...")

        # Reconstruct pending trades (recent signals without fills)
        pending = await conn.fetch("""
            SELECT ts, desk_id, asset_symbol, signal_direction,
                   target_price, confidence, indicator_metadata
            FROM signal_ledger
            WHERE ts >= NOW() - INTERVAL '1 hour'
              AND simulated_fill_price IS NULL
              AND signal_direction != 0
            ORDER BY ts DESC
            LIMIT 500
        """)

        pipe = pool.pipeline(transaction=False)
        for row in pending:
            payload = {
                "asset_symbol": row["asset_symbol"],
                "desk_id": row["desk_id"],
                "signal_direction": row["signal_direction"],
                "target_price": float(row["target_price"]),
                "confidence": float(row["confidence"]) if row["confidence"] else 0.5,
                "reconstructed": True,
                "original_ts": row["ts"].isoformat(),
            }
            expiry = time.time() + 300  # 5-minute TTL for reconstructed signals
            pipe.zadd("oniquant:pending_trades", {orjson.dumps(payload): expiry})

        # Reconstruct recent executions stream
        fills = await conn.fetch("""
            SELECT ts, desk_id, asset_symbol, signal_direction,
                   target_price, simulated_fill_price, indicator_metadata
            FROM signal_ledger
            WHERE ts >= NOW() - INTERVAL '1 hour'
              AND simulated_fill_price IS NOT NULL
            ORDER BY ts DESC
            LIMIT 100
        """)

        for row in fills:
            payload = {
                "asset_symbol": row["asset_symbol"],
                "desk_id": row["desk_id"],
                "signal_direction": row["signal_direction"],
                "target_price": float(row["target_price"]),
                "simulated_fill_price": float(row["simulated_fill_price"]),
                "reconstructed": True,
            }
            pipe.xadd(
                "oniquant:active_executions",
                {"payload": orjson.dumps(payload), "source": b"cold_boot"},
                maxlen=25000,
                approximate=True,
            )

        await pipe.execute()

        elapsed = (time.perf_counter() - t0) * 1000
        print(f"Cold boot complete: {len(pending)} pending, {len(fills)} fills reconstructed in {elapsed:.0f}ms")

    finally:
        await conn.close()
        await pool.aclose()


if __name__ == "__main__":
    asyncio.run(main())
