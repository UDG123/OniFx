"""
OniQuant v6.0 — AsyncPG Connection Pool for TimescaleDB
=========================================================
High-performance async PostgreSQL connectivity via asyncpg.
Manages a connection pool that is initialized on app startup
and drained on shutdown.

asyncpg advantages over psycopg2/SQLAlchemy for our use case:
    - Binary protocol (no text serialization overhead)
    - Built-in prepared statement caching
    - Native COPY support for bulk ingestion
    - 3-5x throughput vs psycopg2 on benchmarks
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any

import asyncpg
import structlog

from app.core.config import get_settings

log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.database")

# ---------------------------------------------------------------------------
# Module-level pool singleton
# ---------------------------------------------------------------------------
_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """
    Initialize the asyncpg connection pool.

    Called during FastAPI lifespan startup. The pool maintains
    persistent connections to TimescaleDB, avoiding per-query
    TCP handshake + TLS negotiation overhead.

    Pool sizing:
        min=5:  keeps warm connections for baseline load
        max=20: caps connections to prevent exhausting
                TimescaleDB's max_connections (default 100)
    """
    global _pool
    settings = get_settings()

    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_min_pool_size,
        max_size=settings.db_max_pool_size,
        command_timeout=30.0,
        # Enable statement caching for repeated INSERT patterns
        statement_cache_size=100,
    )

    # Verify connectivity
    async with _pool.acquire() as conn:
        version = await conn.fetchval("SELECT version()")
        await log.ainfo("database_connected", version=version[:60])

    return _pool


async def close_pool() -> None:
    """Drain all connections gracefully on shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        await log.ainfo("database_pool_closed")


def get_pool() -> asyncpg.Pool:
    """
    Return the active connection pool.

    Raises RuntimeError if called before init_pool().
    This is intentional — fail fast on misconfigured startup order.
    """
    if _pool is None:
        raise RuntimeError("Database pool not initialized — call init_pool() first")
    return _pool


# ---------------------------------------------------------------------------
# Signal Ledger Operations
# ---------------------------------------------------------------------------

async def insert_signal(
    ts: datetime,
    desk_id: str,
    asset_symbol: str,
    signal_direction: int,
    target_price: Decimal | float,
    confidence: float = 0.0,
    entry_price: float | None = None,
    indicator_metadata: dict[str, Any] | None = None,
    source_stream: str = "oniquant:raw_signals",
) -> None:
    """
    Insert a single signal record into the signal_ledger hypertable.

    Uses a prepared statement (cached by asyncpg) for repeated calls.
    The INSERT is a single-row operation; for bulk ingestion, use
    insert_signals_batch() with COPY instead.
    """
    pool = get_pool()

    # orjson → str for asyncpg JSONB binding
    import orjson
    metadata_json = orjson.dumps(indicator_metadata or {}).decode()

    await pool.execute(
        """
        INSERT INTO signal_ledger (
            ts, desk_id, asset_symbol, signal_direction,
            target_price, confidence, entry_price,
            indicator_metadata, source_stream
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
        """,
        ts, desk_id, asset_symbol, signal_direction,
        Decimal(str(target_price)), confidence,
        Decimal(str(entry_price)) if entry_price is not None else None,
        metadata_json, source_stream,
    )


async def insert_signals_batch(
    records: list[tuple],
) -> int:
    """
    Bulk insert signals via asyncpg COPY (binary protocol).

    10-100x faster than individual INSERTs for batch ingestion.
    Each record tuple must match the column order:
        (ts, desk_id, asset_symbol, signal_direction, target_price,
         confidence, entry_price, indicator_metadata, source_stream)

    Returns the number of records inserted.
    """
    pool = get_pool()

    async with pool.acquire() as conn:
        result = await conn.copy_records_to_table(
            "signal_ledger",
            records=records,
            columns=[
                "ts", "desk_id", "asset_symbol", "signal_direction",
                "target_price", "confidence", "entry_price",
                "indicator_metadata", "source_stream",
            ],
        )
        # result is a status string like "COPY 1000"
        count = int(result.split()[-1]) if result else len(records)
        return count


async def log_simulated_fill(
    ts: datetime,
    desk_id: str,
    asset_symbol: str,
    signal_direction: int,
    target_price: float,
    simulated_fill_price: float,
    fill_latency_ms: float,
    indicator_metadata: dict[str, Any] | None = None,
) -> None:
    """
    Insert a signal record with simulated fill data from the Synthetic Matcher.

    This is the write path for matcher.py — records both the original signal
    and the synthetic execution metadata for performance analysis.
    """
    pool = get_pool()

    import orjson
    metadata_json = orjson.dumps(indicator_metadata or {}).decode()

    await pool.execute(
        """
        INSERT INTO signal_ledger (
            ts, desk_id, asset_symbol, signal_direction,
            target_price, simulated_fill_price, fill_latency_ms,
            indicator_metadata, source_stream
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
        """,
        ts, desk_id, asset_symbol, signal_direction,
        Decimal(str(target_price)),
        Decimal(str(simulated_fill_price)),
        Decimal(str(fill_latency_ms)),
        metadata_json, "synthetic_matcher",
    )


async def query_win_rate(
    asset_symbol: str,
    desk_id: str,
    lookback_days: int = 30,
) -> float | None:
    """
    Query the 30-day historical win rate for an asset+desk pair.

    Used by the Bayesian Orchestrator to compute the Prior P(A).
    Returns None if insufficient data.
    """
    pool = get_pool()

    row = await pool.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE signal_direction != 0) AS total,
            COUNT(*) FILTER (
                WHERE simulated_fill_price IS NOT NULL
                AND (
                    (signal_direction = 1 AND simulated_fill_price >= target_price)
                    OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                )
            ) AS wins
        FROM signal_ledger
        WHERE asset_symbol = $1
          AND desk_id = $2
          AND ts >= NOW() - make_interval(days => $3)
        """,
        asset_symbol, desk_id, lookback_days,
    )

    if row is None or row["total"] == 0:
        return None

    return float(row["wins"]) / float(row["total"])
