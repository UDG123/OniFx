"""
OniQuant v6.0 — Weekly Performance Report Generator
======================================================
Generates a Markdown summary of the week's Bayesian drift,
desk performance, and indicator accuracy.

Output is sent to Discord/Slack via the alert webhook.

Usage: python scripts/generate_weekly_report.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@timescaledb:5432/oniquant")


async def generate_report() -> str:
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Desk performance
        desk_rows = await conn.fetch("""
            SELECT desk_id,
                   COUNT(*) FILTER (WHERE simulated_fill_price IS NOT NULL) AS fills,
                   COUNT(*) FILTER (
                       WHERE simulated_fill_price IS NOT NULL
                       AND ((signal_direction=1 AND simulated_fill_price>=target_price)
                         OR (signal_direction=-1 AND simulated_fill_price<=target_price))
                   ) AS wins,
                   AVG(ABS(target_price - simulated_fill_price))
                       FILTER (WHERE simulated_fill_price IS NOT NULL) AS avg_slip
            FROM signal_ledger
            WHERE ts >= NOW() - INTERVAL '7 days' AND signal_direction != 0
            GROUP BY desk_id ORDER BY desk_id
        """)

        # Bayesian calibration
        cal_rows = await conn.fetch("""
            SELECT desk_id,
                   AVG(posterior_prob) AS avg_posterior,
                   AVG(CASE WHEN (signal_direction=1 AND simulated_fill_price>=target_price)
                             OR (signal_direction=-1 AND simulated_fill_price<=target_price)
                        THEN 1.0 ELSE 0.0 END) AS actual_wr
            FROM signal_ledger
            WHERE ts >= NOW() - INTERVAL '7 days'
              AND posterior_prob IS NOT NULL AND simulated_fill_price IS NOT NULL
            GROUP BY desk_id
        """)

    finally:
        await conn.close()

    # Build Markdown
    report = [
        f"# OniQuant v6.0 — Weekly Report",
        f"**Generated:** {datetime.utcnow().isoformat()}Z\n",
        "## Desk Performance (Last 7 Days)",
        "| Desk | Fills | Wins | Win Rate | Avg Slippage |",
        "|------|-------|------|----------|-------------|",
    ]

    best_desk, best_wr = None, 0.0
    for r in desk_rows:
        fills = r["fills"] or 0
        wins = r["wins"] or 0
        wr = wins / fills if fills > 0 else 0
        slip = float(r["avg_slip"]) if r["avg_slip"] else 0
        report.append(f"| {r['desk_id']} | {fills} | {wins} | {wr:.1%} | {slip:.4f} |")
        if wr > best_wr:
            best_wr, best_desk = wr, r["desk_id"]

    report.append(f"\n**Best Desk:** {best_desk} ({best_wr:.1%})\n")

    report.append("## Bayesian Calibration Drift")
    report.append("| Desk | Avg Posterior | Actual WR | Drift |")
    report.append("|------|-------------|-----------|-------|")

    worst_drift, worst_desk = 0.0, None
    for r in cal_rows:
        avg_p = float(r["avg_posterior"]) if r["avg_posterior"] else 0
        actual = float(r["actual_wr"]) if r["actual_wr"] else 0
        drift = avg_p - actual
        report.append(f"| {r['desk_id']} | {avg_p:.3f} | {actual:.3f} | {drift:+.3f} |")
        if abs(drift) > abs(worst_drift):
            worst_drift, worst_desk = drift, r["desk_id"]

    report.append(f"\n**Most Inaccurate Desk:** {worst_desk} (drift {worst_drift:+.3f})\n")

    return "\n".join(report)


async def main():
    report = await generate_report()
    print(report)

    # Optionally send to webhook
    from app.core.logger import send_alert
    await send_alert(
        title="Weekly Performance Report",
        message=report[:1900],  # Discord embed limit
        severity="info",
    )


if __name__ == "__main__":
    asyncio.run(main())
