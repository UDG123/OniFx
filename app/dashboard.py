"""
OniQuant v6.0 — Operational Streamlit Dashboard
==================================================
Real-time system vitals: signal feed, backpressure, equity curve,
and Tri-State Regime Optimization (CPCV) panel.

Run: streamlit run app/dashboard.py --server.port 8501
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from pathlib import Path

import streamlit as st
import plotly.graph_objects as go
import redis
import psycopg2

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DB_URL = os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@timescaledb:5432/oniquant")
RISK_PROFILES_PATH = Path(__file__).resolve().parents[1] / "config" / "risk_profiles.yaml"

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


def _load_risk_profiles_yaml() -> dict:
    """Load risk profiles YAML for display and editing."""
    try:
        import yaml
        with open(RISK_PROFILES_PATH) as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


def _save_risk_profiles_yaml(data: dict) -> None:
    """Write updated risk profiles back to YAML."""
    import yaml
    with open(RISK_PROFILES_PATH, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Tab Layout
# ---------------------------------------------------------------------------
tab_ops, tab_cpcv = st.tabs(["Operations", "Regime Optimization (CPCV)"])

# ==========================================================================
# TAB 1: Operations (existing dashboard)
# ==========================================================================
with tab_ops:

    # -----------------------------------------------------------------------
    # 1. Live Signal Feed
    # -----------------------------------------------------------------------
    st.header("Live Authorized Signals (Last 10)")

    try:
        r = get_redis_conn()
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

    # -----------------------------------------------------------------------
    # 2. Health Monitor — Stream Backpressure
    # -----------------------------------------------------------------------
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

    # -----------------------------------------------------------------------
    # 3. Performance — Equity Curve (Synthetic Fills)
    # -----------------------------------------------------------------------
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

    # -----------------------------------------------------------------------
    # 4. Decision Audit View
    # -----------------------------------------------------------------------
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


# ==========================================================================
# TAB 2: Regime Optimization (CPCV)
# ==========================================================================
with tab_cpcv:

    st.header("Tri-State Regime Optimization (CPCV)")
    st.markdown("""
    Runs **Combinatorial Purged Cross-Validation** across Conservative, Active,
    and Aggressive regimes using shadow ledger data. Evaluates each regime's
    Deflated Sharpe Ratio (DSR) and Probability of Backtest Overfitting (PBO)
    across 20 combinatorial train/test paths (C(6,3) with 1% temporal embargo).
    """)

    # -----------------------------------------------------------------------
    # Current Regime Display
    # -----------------------------------------------------------------------
    profiles = _load_risk_profiles_yaml()
    current_regime = profiles.get("active_regime", "conservative")
    regimes_cfg = profiles.get("regimes", {})

    st.subheader("Current Active Regime")
    regime_label = regimes_cfg.get(current_regime, {}).get("label", current_regime)
    st.info(f"Active Regime: **{regime_label}** (`{current_regime}`)")

    if regimes_cfg:
        regime_cols = st.columns(len(regimes_cfg))
        for col, (rname, rcfg) in zip(regime_cols, regimes_cfg.items()):
            is_active = rname == current_regime
            col.metric(
                rcfg.get("label", rname),
                f"Floor: {rcfg.get('threshold', 0):.0%}",
                delta="ACTIVE" if is_active else None,
                delta_color="normal" if is_active else "off",
            )
            col.caption(
                f"a0={rcfg.get('prior_discount_a0')}, "
                f"T={rcfg.get('tempering_T')}, "
                f"Kelly={rcfg.get('kelly_fraction')}, "
                f"Cap={rcfg.get('max_cap_pct')}"
            )

    # -----------------------------------------------------------------------
    # CPCV Analysis Button
    # -----------------------------------------------------------------------
    st.subheader("Run CPCV Analysis")

    lookback = st.slider(
        "Shadow Ledger Lookback (days)", min_value=1, max_value=30, value=7
    )

    if st.button("Run Tri-State CPCV Analysis", type="primary", use_container_width=True):
        with st.spinner("Running CPCV across 20 combinatorial paths..."):
            try:
                import redis.asyncio as aioredis
                from app.quant.cpcv_optimizer import run_cpcv_analysis

                async def _run():
                    pool = aioredis.from_url(REDIS_URL, decode_responses=False)
                    try:
                        return await run_cpcv_analysis(pool, lookback_days=lookback)
                    finally:
                        await pool.aclose()

                cpcv_results = asyncio.run(_run())
                st.session_state["cpcv_results"] = cpcv_results
                st.success("CPCV analysis complete.")
            except Exception as e:
                st.error(f"CPCV analysis failed: {e}")

    # -----------------------------------------------------------------------
    # Results Display
    # -----------------------------------------------------------------------
    if "cpcv_results" in st.session_state:
        results = st.session_state["cpcv_results"]

        st.subheader("CPCV Results — Regime Comparison")

        # Comparative table
        table_data = []
        for rname, res in results.items():
            table_data.append({
                "Regime": res.label,
                "CPCV Paths": res.n_paths,
                "OOS Win Rate": f"{res.mean_oos_win_rate:.2%}",
                "OOS Sharpe (mean)": f"{res.mean_oos_sharpe:.4f}",
                "OOS Sharpe (std)": f"{res.std_oos_sharpe:.4f}",
                "Deflated Sharpe (DSR)": f"{res.deflated_sharpe:.4f}",
                "PBO": f"{res.pbo:.2%}",
                "EV/Trade": f"${res.ev_per_trade:.4f}",
                "Total OOS Trades": res.total_oos_trades,
            })
        st.dataframe(table_data, use_container_width=True)

        # EV per trade bar chart
        st.subheader("Expected Value (EV) per Trade by Regime")
        regime_names = [res.label for res in results.values()]
        ev_values = [res.ev_per_trade for res in results.values()]
        dsr_values = [res.deflated_sharpe for res in results.values()]

        fig_ev = go.Figure()
        fig_ev.add_trace(go.Bar(
            x=regime_names,
            y=ev_values,
            name="EV per Trade ($)",
            marker_color=["#2ecc71", "#3498db", "#e74c3c"],
            text=[f"${v:.4f}" for v in ev_values],
            textposition="outside",
        ))
        fig_ev.update_layout(
            title="Expected Value per Trade — Tri-State Comparison",
            yaxis_title="EV per Trade ($)",
            template="plotly_dark",
            showlegend=False,
        )
        st.plotly_chart(fig_ev, use_container_width=True)

        # DSR + PBO comparison chart
        fig_dsr = go.Figure()
        fig_dsr.add_trace(go.Bar(
            x=regime_names,
            y=dsr_values,
            name="Deflated Sharpe (DSR)",
            marker_color=["#2ecc71", "#3498db", "#e74c3c"],
            text=[f"{v:.4f}" for v in dsr_values],
            textposition="outside",
        ))
        pbo_values = [res.pbo for res in results.values()]
        fig_dsr.add_trace(go.Scatter(
            x=regime_names,
            y=pbo_values,
            name="PBO (Prob. Overfitting)",
            mode="markers+text",
            marker=dict(size=14, color="#f39c12"),
            text=[f"{v:.1%}" for v in pbo_values],
            textposition="top center",
            yaxis="y2",
        ))
        fig_dsr.update_layout(
            title="Deflated Sharpe Ratio vs Probability of Backtest Overfitting",
            yaxis_title="DSR",
            yaxis2=dict(title="PBO", overlaying="y", side="right", range=[0, 1]),
            template="plotly_dark",
            legend=dict(x=0.01, y=0.99),
        )
        st.plotly_chart(fig_dsr, use_container_width=True)

        # -------------------------------------------------------------------
        # One-Click Deploy Selector
        # -------------------------------------------------------------------
        st.subheader("Deploy Regime for Live Execution")
        st.markdown(
            "Select the regime validated by CPCV and deploy it as the "
            "active regime for the Monday morning open. This updates "
            "`config/risk_profiles.yaml` — no terminal required."
        )

        regime_options = {
            res.label: rname for rname, res in results.items()
        }
        selected_label = st.selectbox(
            "Select Active Regime for Live Deployment",
            options=list(regime_options.keys()),
            index=list(regime_options.values()).index(current_regime)
            if current_regime in regime_options.values() else 0,
        )
        selected_regime = regime_options[selected_label]

        # Show selected regime's stats
        selected_res = results[selected_regime]
        st.markdown(
            f"**{selected_label}** — "
            f"DSR: {selected_res.deflated_sharpe:.4f}, "
            f"PBO: {selected_res.pbo:.2%}, "
            f"EV/Trade: ${selected_res.ev_per_trade:.4f}"
        )

        if selected_res.pbo > 0.50:
            st.warning(
                f"PBO for {selected_label} is {selected_res.pbo:.0%} — "
                f"this regime may be overfit. Proceed with caution."
            )

        if st.button(
            "Lock Regime & Update Orchestrator",
            type="primary",
            use_container_width=True,
        ):
            try:
                profiles["active_regime"] = selected_regime
                _save_risk_profiles_yaml(profiles)

                # Signal the orchestrator to hot-reload via Redis pub/sub
                try:
                    r = get_redis_conn()
                    r.publish("oniquant:regime_update", selected_regime)
                except Exception:
                    pass  # Non-critical: worker will pick up on next restart

                st.success(
                    f"Regime updated to **{selected_label}** (`{selected_regime}`). "
                    f"Config written to `config/risk_profiles.yaml`. "
                    f"Orchestrator will apply on next signal."
                )
                st.balloons()
            except Exception as e:
                st.error(f"Failed to update regime: {e}")
