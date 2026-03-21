"""
OniQuant v6.0 — Operational Streamlit Dashboard
==================================================
Real-time system vitals: signal feed, backpressure, equity curve.

Run: streamlit run app/dashboard.py --server.port 8501
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime

import streamlit as st
import plotly.graph_objects as go
import redis
import psycopg2

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DB_URL = os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@timescaledb:5432/oniquant")

st.set_page_config(page_title="OniQuant v6.0 CTO Dashboard", layout="wide")
st.title("OniQuant v6.0 — Operational Dashboard")


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
@st.cache_resource
def get_redis_conn():
    return redis.from_url(REDIS_URL, decode_responses=True)


@st.cache_resource
def get_db_conn():
    return psycopg2.connect(DB_URL)


# ---------------------------------------------------------------------------
# 1. Live Signal Feed
# ---------------------------------------------------------------------------
st.header("Live Authorized Signals (Last 10)")

try:
    r = get_redis_conn()
    # Read last 10 entries from active_executions stream
    entries = r.xrevrange("oniquant:active_executions", count=10)

    if entries:
        import json
        signals_data = []
        for msg_id, fields in entries:
            payload = json.loads(fields.get("payload", "{}"))
            signals_data.append({
                "Time": msg_id.split("-")[0],
                "Symbol": payload.get("asset_symbol", "—"),
                "Direction": "LONG" if payload.get("signal_direction", 0) == 1 else "SHORT",
                "Posterior": f"{payload.get('posterior_probability', 0):.4f}",
                "Target": payload.get("target_price", "—"),
                "Routing": payload.get("routing_target", "—"),
                "Desk": payload.get("desk_id", "—"),
            })
        st.dataframe(signals_data, use_container_width=True)
    else:
        st.info("No authorized signals yet.")
except Exception as e:
    st.error(f"Redis connection error: {e}")


# ---------------------------------------------------------------------------
# 2. Health Monitor — Stream Backpressure
# ---------------------------------------------------------------------------
st.header("Stream Health")

try:
    r = get_redis_conn()
    col1, col2, col3, col4 = st.columns(4)

    raw_len = r.xlen("oniquant:raw_signals")
    col1.metric("Raw Signals Queue", raw_len, delta_color="inverse")

    exec_len = r.xlen("oniquant:active_executions")
    col2.metric("Active Executions", exec_len)

    pending_count = r.zcard("oniquant:pending_trades")
    col3.metric("Pending Trades (ZSET)", pending_count)

    dlq_len = r.xlen("oniquant:dead_letter_queue") if r.exists("oniquant:dead_letter_queue") else 0
    col4.metric("Dead Letter Queue", dlq_len, delta_color="inverse")

except Exception as e:
    st.error(f"Redis health check failed: {e}")


# ---------------------------------------------------------------------------
# 3. Performance — Equity Curve (Synthetic Fills)
# ---------------------------------------------------------------------------
st.header("Synthetic Fill Equity Curve")

try:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            time_bucket('1 hour', ts) AS bucket,
            desk_id,
            SUM(CASE
                WHEN signal_direction = 1 THEN simulated_fill_price - target_price
                WHEN signal_direction = -1 THEN target_price - simulated_fill_price
                ELSE 0
            END) AS pnl
        FROM signal_ledger
        WHERE simulated_fill_price IS NOT NULL
          AND ts >= NOW() - INTERVAL '7 days'
        GROUP BY bucket, desk_id
        ORDER BY bucket
    """)
    rows = cur.fetchall()
    cur.close()

    if rows:
        import pandas as pd
        df = pd.DataFrame(rows, columns=["bucket", "desk_id", "pnl"])

        fig = go.Figure()
        for desk in df["desk_id"].unique():
            desk_df = df[df["desk_id"] == desk].sort_values("bucket")
            desk_df["cumulative_pnl"] = desk_df["pnl"].cumsum()
            fig.add_trace(go.Scatter(
                x=desk_df["bucket"],
                y=desk_df["cumulative_pnl"],
                name=desk,
                mode="lines",
            ))

        fig.update_layout(
            title="Cumulative P&L by Desk (Last 7 Days)",
            xaxis_title="Time",
            yaxis_title="Cumulative P&L ($)",
            template="plotly_dark",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No synthetic fill data available yet.")

except Exception as e:
    st.error(f"TimescaleDB query error: {e}")


# ---------------------------------------------------------------------------
# 4. Decision Audit View
# ---------------------------------------------------------------------------
st.header("Decision Audit — Bayesian Reasoning")

try:
    r = get_redis_conn()
    entries = r.xrevrange("oniquant:active_executions", count=5)
    if entries:
        import json
        for msg_id, fields in entries:
            payload = json.loads(fields.get("payload", "{}"))
            with st.expander(
                f"{payload.get('asset_symbol', '?')} — "
                f"P(A|B)={payload.get('posterior_probability', 0):.4f}"
            ):
                st.json(payload)
                reasoning = payload.get("reasoning_math", "N/A")
                st.markdown(f"**Reasoning:** `{reasoning}`")
except Exception as e:
    st.warning(f"Could not load audit data: {e}")
