"""
OniQuant v6.0 — Black Box Recorder (Decision Audit Logger)
=============================================================
Saves the complete state of every Bayesian decision (inputs,
likelihoods, MCP outputs, system metrics) into the decision_audit_log
table for post-mortem analysis.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import orjson
import structlog

from app.core.database import get_pool

log = structlog.get_logger("oniquant.blackbox")

# DDL for the audit table (run via schema migration)
AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS decision_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    signal_id       VARCHAR(64) NOT NULL,
    asset_symbol    VARCHAR(32) NOT NULL,
    desk_id         VARCHAR(64) NOT NULL,
    decision        VARCHAR(16) NOT NULL,
    posterior_prob   DECIMAL(7, 6),
    full_state      JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_signal ON decision_audit_log (signal_id);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON decision_audit_log (ts DESC);
"""


async def record_decision(
    signal_id: str,
    asset_symbol: str,
    desk_id: str,
    decision: str,
    posterior: float,
    full_state: dict[str, Any],
) -> None:
    """
    Record a complete Bayesian decision snapshot to the audit log.

    full_state includes:
        - All signal inputs (direction, target, confidence, metadata)
        - Prior probability and source
        - All likelihood multipliers (IOF, Macro, Hurst)
        - MCP tool responses (if any)
        - System metrics (redis_mem, network_latency, cpu_load)
        - Final posterior and decision
    """
    pool = get_pool()
    try:
        await pool.execute(
            """
            INSERT INTO decision_audit_log (signal_id, asset_symbol, desk_id, decision, posterior_prob, full_state)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """,
            signal_id,
            asset_symbol,
            desk_id,
            decision,
            Decimal(str(round(posterior, 6))),
            orjson.dumps(full_state).decode(),
        )
    except Exception as e:
        await log.awarning("blackbox_write_error", error=str(e), signal_id=signal_id)


async def query_recent_decisions(limit: int = 100) -> list[dict[str, Any]]:
    """Query the last N decisions for the audit report."""
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT * FROM decision_audit_log ORDER BY ts DESC LIMIT $1", limit
    )
    return [dict(row) for row in rows]
