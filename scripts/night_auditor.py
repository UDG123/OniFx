"""
OniFx — Night Auditor (Weekly Autonomous Verification)
========================================================
Scheduled job that audits all three floors against actual performance.

Checks:
    1. Model Drift: Compares realized win rates vs Bayesian priors.
       If deviation > 15%, flags the floor and recommends prior update.
    2. R:R Decay (AGGRESSIVE): Verifies wins >= 3x losses.
       If the ratio is shrinking week-over-week, flags strategy decay.
    3. Payload Schema Sync: Ensures DOCS/TRADINGVIEW_PAYLOAD.txt
       matches the Pydantic schema in app/schemas.py.
    4. Telegram Report: Sends a structured weekly summary to admin.

Scheduling:
    # crontab (Sunday 22:00 UTC)
    0 22 * * 0  cd /app && python scripts/night_auditor.py

    # Railway.app scheduled job
    railway run --cron "0 22 * * 0" python scripts/night_auditor.py

    # systemd timer
    See deploy/night-auditor.timer

Usage:
    python scripts/night_auditor.py [--dry-run] [--lookback-days 7]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import structlog

log = structlog.get_logger("onifx.night_auditor")

REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql://oniquant:oniquant@timescaledb:5432/oniquant",
)
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_CHANNEL: str = os.getenv("TELEGRAM_ADMIN_CHANNEL", "")

# Thresholds
DRIFT_THRESHOLD: float = 0.15       # 15% deviation triggers prior update recommendation
RR_MINIMUM_AGGRESSIVE: float = 3.0  # AGGRESSIVE wins must be >= 3x losses
RR_DECAY_THRESHOLD: float = 0.20    # 20% week-over-week R:R decline = strategy decay


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class FloorAuditResult:
    """Audit results for a single floor."""
    floor: str
    realized_win_rate: float
    bayesian_prior: float
    prior_drift_pct: float
    drift_exceeded: bool
    total_signals: int
    authorized: int
    wins: int
    losses: int
    avg_win: float
    avg_loss: float
    rr_ratio: float
    rr_previous_week: float | None
    rr_decaying: bool
    recommendations: list[str] = field(default_factory=list)


@dataclass
class AuditReport:
    """Full night audit report."""
    timestamp: str
    lookback_days: int
    floors: list[FloorAuditResult]
    schema_in_sync: bool
    schema_issues: list[str]
    overall_health: str  # "GREEN", "YELLOW", "RED"


# ---------------------------------------------------------------------------
# 1. Model Drift Check
# ---------------------------------------------------------------------------

async def _check_model_drift(
    pool: Any,
    floor_name: str,
    regime_name: str,
    lookback_days: int,
) -> FloorAuditResult:
    """
    Compare realized win rate vs Bayesian prior for a single floor.

    Queries TimescaleDB signal_ledger for:
        - Total authorized signals (decision = AUTHORIZE in metadata)
        - Wins (fill_price hit target in correct direction)
        - Losses (fill_price did not hit target)
        - Average win/loss magnitudes (for R:R ratio)
    """
    import asyncpg

    try:
        conn = await asyncpg.connect(DATABASE_URL, timeout=10)
    except Exception as e:
        await log.awarning("db_connection_failed", error=str(e))
        return FloorAuditResult(
            floor=floor_name, realized_win_rate=0, bayesian_prior=0,
            prior_drift_pct=0, drift_exceeded=False, total_signals=0,
            authorized=0, wins=0, losses=0, avg_win=0, avg_loss=0,
            rr_ratio=0, rr_previous_week=None, rr_decaying=False,
            recommendations=["DB connection failed — manual review required"],
        )

    try:
        # Current week stats
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE
                    (indicator_metadata->>'decision') = 'AUTHORIZE'
                    OR (indicator_metadata->'tri_state'->>$2) = 'AUTHORIZE'
                ) AS authorized,
                COUNT(*) FILTER (WHERE
                    simulated_fill_price IS NOT NULL
                    AND (
                        (signal_direction = 1 AND simulated_fill_price >= target_price)
                        OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                    )
                ) AS wins,
                COUNT(*) FILTER (WHERE
                    simulated_fill_price IS NOT NULL
                    AND NOT (
                        (signal_direction = 1 AND simulated_fill_price >= target_price)
                        OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                    )
                ) AS losses,
                AVG(ABS(simulated_fill_price - target_price)) FILTER (WHERE
                    simulated_fill_price IS NOT NULL
                    AND (
                        (signal_direction = 1 AND simulated_fill_price >= target_price)
                        OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                    )
                ) AS avg_win,
                AVG(ABS(simulated_fill_price - target_price)) FILTER (WHERE
                    simulated_fill_price IS NOT NULL
                    AND NOT (
                        (signal_direction = 1 AND simulated_fill_price >= target_price)
                        OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                    )
                ) AS avg_loss
            FROM signal_ledger
            WHERE ts >= NOW() - make_interval(days => $1)
              AND simulated_fill_price IS NOT NULL
        """, lookback_days, floor_name)

        # Previous week (for R:R trend comparison)
        prev_row = await conn.fetchrow("""
            SELECT
                AVG(ABS(simulated_fill_price - target_price)) FILTER (WHERE
                    (signal_direction = 1 AND simulated_fill_price >= target_price)
                    OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                ) AS avg_win,
                AVG(ABS(simulated_fill_price - target_price)) FILTER (WHERE
                    NOT (
                        (signal_direction = 1 AND simulated_fill_price >= target_price)
                        OR (signal_direction = -1 AND simulated_fill_price <= target_price)
                    )
                ) AS avg_loss
            FROM signal_ledger
            WHERE ts >= NOW() - make_interval(days => $1)
              AND ts < NOW() - make_interval(days => $2)
              AND simulated_fill_price IS NOT NULL
        """, lookback_days * 2, lookback_days)

    finally:
        await conn.close()

    total = int(row["total"]) if row["total"] else 0
    authorized = int(row["authorized"]) if row["authorized"] else 0
    wins = int(row["wins"]) if row["wins"] else 0
    losses = int(row["losses"]) if row["losses"] else 0
    avg_win = float(row["avg_win"]) if row["avg_win"] else 0.0
    avg_loss = float(row["avg_loss"]) if row["avg_loss"] else 0.0

    # Realized win rate
    total_decided = wins + losses
    realized_wr = wins / total_decided if total_decided > 0 else 0.5

    # Get Bayesian prior from Redis cache
    import redis.asyncio as aioredis
    redis_pool = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        cached_prior = await redis_pool.get(f"oniquant:prior:*:{regime_name}")
        bayesian_prior = float(cached_prior) if cached_prior else 0.50
    except Exception:
        bayesian_prior = 0.50
    finally:
        await redis_pool.aclose()

    # Drift calculation
    drift = abs(realized_wr - bayesian_prior)
    drift_exceeded = drift > DRIFT_THRESHOLD

    # R:R ratio
    rr_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0

    # Previous week R:R
    prev_avg_win = float(prev_row["avg_win"]) if prev_row and prev_row["avg_win"] else 0.0
    prev_avg_loss = float(prev_row["avg_loss"]) if prev_row and prev_row["avg_loss"] else 0.0
    rr_previous = (prev_avg_win / prev_avg_loss) if prev_avg_loss > 0 else None

    # R:R decay detection
    rr_decaying = False
    if rr_previous is not None and rr_previous > 0:
        rr_decline = (rr_previous - rr_ratio) / rr_previous
        rr_decaying = rr_decline > RR_DECAY_THRESHOLD

    # Build recommendations
    recommendations = []
    if drift_exceeded:
        recommendations.append(
            f"PRIOR DRIFT: Realized WR={realized_wr:.2%} vs Prior={bayesian_prior:.2%} "
            f"(drift={drift:.2%} > {DRIFT_THRESHOLD:.0%} threshold). "
            f"Recommend updating prior in config/risk_profiles.yaml."
        )

    if floor_name == "AGGRESSIVE" and rr_ratio < RR_MINIMUM_AGGRESSIVE and total_decided > 5:
        recommendations.append(
            f"R:R BELOW MINIMUM: {rr_ratio:.2f}x < {RR_MINIMUM_AGGRESSIVE:.1f}x required. "
            f"AGGRESSIVE floor should be paused until R:R recovers."
        )

    if rr_decaying:
        recommendations.append(
            f"STRATEGY DECAY: R:R dropped from {rr_previous:.2f}x to {rr_ratio:.2f}x "
            f"({(rr_previous - rr_ratio) / rr_previous:.0%} decline). "
            f"Review signal generation logic for this floor."
        )

    return FloorAuditResult(
        floor=floor_name,
        realized_win_rate=round(realized_wr, 4),
        bayesian_prior=round(bayesian_prior, 4),
        prior_drift_pct=round(drift, 4),
        drift_exceeded=drift_exceeded,
        total_signals=total,
        authorized=authorized,
        wins=wins,
        losses=losses,
        avg_win=round(avg_win, 4),
        avg_loss=round(avg_loss, 4),
        rr_ratio=round(rr_ratio, 4),
        rr_previous_week=round(rr_previous, 4) if rr_previous is not None else None,
        rr_decaying=rr_decaying,
        recommendations=recommendations,
    )


# ---------------------------------------------------------------------------
# 2. Payload Schema Sync Check
# ---------------------------------------------------------------------------

def _check_schema_sync() -> tuple[bool, list[str]]:
    """
    Verify that DOCS/TRADINGVIEW_PAYLOAD.txt is in sync with
    the Pydantic schema in app/schemas.py.

    Returns (in_sync: bool, issues: list[str]).
    """
    from app.schemas import TradingViewPayload

    docs_path = Path(__file__).resolve().parents[1] / "DOCS" / "TRADINGVIEW_PAYLOAD.txt"
    if not docs_path.exists():
        return False, ["DOCS/TRADINGVIEW_PAYLOAD.txt does not exist"]

    docs_content = docs_path.read_text()

    # Extract field names from the Pydantic model
    schema_fields = set(TradingViewPayload.model_fields.keys())

    # Check that each schema field is mentioned in the docs
    issues = []
    for field_name in schema_fields:
        if field_name not in docs_content:
            issues.append(f"Field '{field_name}' missing from TRADINGVIEW_PAYLOAD.txt")

    return len(issues) == 0, issues


# ---------------------------------------------------------------------------
# 3. Telegram Report
# ---------------------------------------------------------------------------

async def _send_admin_report(report: AuditReport) -> None:
    """Send the weekly audit report to the admin Telegram channel."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ADMIN_CHANNEL:
        print("[SKIP] No Telegram credentials configured. Report printed to stdout only.")
        return

    lines = [
        "<b>OniFx Night Auditor — Weekly Report</b>",
        f"Date: {report.timestamp}",
        f"Lookback: {report.lookback_days} days",
        f"Overall Health: <b>{report.overall_health}</b>",
        "",
    ]

    for f in report.floors:
        status_emoji = "\u2705" if not f.drift_exceeded and not f.rr_decaying else "\u26A0\uFE0F"
        lines.append(f"{status_emoji} <b>{f.floor}</b>")
        lines.append(f"  Win Rate: {f.realized_win_rate:.2%} (Prior: {f.bayesian_prior:.2%}, Drift: {f.prior_drift_pct:.2%})")
        lines.append(f"  Signals: {f.total_signals} | Auth: {f.authorized} | W/L: {f.wins}/{f.losses}")
        lines.append(f"  R:R Ratio: {f.rr_ratio:.2f}x" + (f" (prev: {f.rr_previous_week:.2f}x)" if f.rr_previous_week else ""))
        if f.recommendations:
            for rec in f.recommendations:
                lines.append(f"  \u26A0\uFE0F {rec}")
        lines.append("")

    if not report.schema_in_sync:
        lines.append("\u26A0\uFE0F <b>Schema Sync Issues:</b>")
        for issue in report.schema_issues:
            lines.append(f"  - {issue}")

    message = "\n".join(lines)

    try:
        import httpx
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json={
                "chat_id": TELEGRAM_ADMIN_CHANNEL,
                "text": message,
                "parse_mode": "HTML",
            })
        await log.ainfo("admin_report_sent")
    except Exception as e:
        await log.awarning("admin_report_failed", error=str(e))


# ---------------------------------------------------------------------------
# 4. Main Orchestrator
# ---------------------------------------------------------------------------

async def run_audit(lookback_days: int = 7, dry_run: bool = False) -> AuditReport:
    """Run the full Night Auditor pipeline."""

    floors = [
        ("STABLE", "conservative"),
        ("ACTIVE", "active"),
        ("AGGRESSIVE", "aggressive"),
    ]

    print(f"{'=' * 60}")
    print(f"OniFx Night Auditor — {datetime.now(timezone.utc).isoformat()}")
    print(f"Lookback: {lookback_days} days | Dry Run: {dry_run}")
    print(f"{'=' * 60}")

    # Run floor audits
    floor_results: list[FloorAuditResult] = []
    for floor_name, regime_name in floors:
        print(f"\n--- Auditing {floor_name} floor ---")
        try:
            result = await _check_model_drift(None, floor_name, regime_name, lookback_days)
            floor_results.append(result)
            print(f"  Win Rate: {result.realized_win_rate:.2%}")
            print(f"  Prior:    {result.bayesian_prior:.2%}")
            print(f"  Drift:    {result.prior_drift_pct:.2%} {'EXCEEDED' if result.drift_exceeded else 'OK'}")
            print(f"  R:R:      {result.rr_ratio:.2f}x")
            if result.rr_decaying:
                print(f"  ** STRATEGY DECAY DETECTED **")
            for rec in result.recommendations:
                print(f"  >> {rec}")
        except Exception as e:
            print(f"  ERROR: {e}")
            floor_results.append(FloorAuditResult(
                floor=floor_name, realized_win_rate=0, bayesian_prior=0,
                prior_drift_pct=0, drift_exceeded=False, total_signals=0,
                authorized=0, wins=0, losses=0, avg_win=0, avg_loss=0,
                rr_ratio=0, rr_previous_week=None, rr_decaying=False,
                recommendations=[f"Audit failed: {e}"],
            ))

    # Schema sync check
    print(f"\n--- Checking Payload Schema Sync ---")
    schema_ok, schema_issues = _check_schema_sync()
    print(f"  Schema in sync: {schema_ok}")
    for issue in schema_issues:
        print(f"  >> {issue}")

    # Determine overall health
    any_drift = any(f.drift_exceeded for f in floor_results)
    any_decay = any(f.rr_decaying for f in floor_results)
    any_rr_low = any(
        f.floor == "AGGRESSIVE" and f.rr_ratio < RR_MINIMUM_AGGRESSIVE and f.wins + f.losses > 5
        for f in floor_results
    )

    if any_decay or any_rr_low:
        health = "RED"
    elif any_drift or not schema_ok:
        health = "YELLOW"
    else:
        health = "GREEN"

    report = AuditReport(
        timestamp=datetime.now(timezone.utc).isoformat(),
        lookback_days=lookback_days,
        floors=floor_results,
        schema_in_sync=schema_ok,
        schema_issues=schema_issues,
        overall_health=health,
    )

    print(f"\n{'=' * 60}")
    print(f"Overall Health: {health}")
    print(f"{'=' * 60}")

    # Send Telegram report (unless dry run)
    if not dry_run:
        await _send_admin_report(report)

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="OniFx Night Auditor")
    parser.add_argument("--dry-run", action="store_true", help="Skip Telegram dispatch")
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback window")
    args = parser.parse_args()

    asyncio.run(run_audit(lookback_days=args.lookback_days, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
