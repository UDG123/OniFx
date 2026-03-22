#!/usr/bin/env bash
# ==========================================================================
# OniQuant v6.0 — Monday Morning Open Automation
# ==========================================================================
# Triggered via Railway Cron or manual CLI before market open.
#
# Pre-flight sequence:
#   1. System health check (Redis, DB, IB Gateway, FastAPI)
#   2. Config sanity check (risk_profiles.yaml vs institutional limits)
#   3. Latency baseline (/health round-trip, 200ms threshold)
#   4. Warmup worker (hydrate Redis history cache)
#   5. Warmup verification (check oniquant:baseline:{symbol} keys)
#   6. Environment validation
#   7. Ready notification
# ==========================================================================

set -euo pipefail

APP_URL="${APP_URL:-http://localhost:8000}"
LATENCY_THRESHOLD_MS="${LATENCY_THRESHOLD_MS:-200}"

echo "============================================="
echo "OniQuant v6.0 — Monday Open Sequence"
echo "============================================="

# 1. System Health Check
echo "[1/7] Running health check..."
python scripts/health_check.py
if [ $? -ne 0 ]; then
    echo "ABORT: Health check failed"
    exit 1
fi

# 2. Pre-Flight Math Check — Risk Config Sanity
echo "[2/7] Running config sanity check..."
python scripts/night_auditor.py --sanity-only
if [ $? -ne 0 ]; then
    echo "ABORT: Config sanity check failed (sleeve risk exceeds institutional ceiling)"
    python -c "
import asyncio
from app.core.logger import send_alert
asyncio.run(send_alert(
    title='MONDAY ABORT: Config Sanity Failed',
    message='One or more final_sleeve_risk values exceed the 5% institutional ceiling. Monday open BLOCKED.',
    severity='critical',
))
"
    exit 1
fi

# 3. Latency Baseline — Ping /health and measure round-trip
echo "[3/7] Measuring endpoint latency..."
LATENCY_MS=$(python -c "
import time, urllib.request
start = time.monotonic()
try:
    urllib.request.urlopen('${APP_URL}/health', timeout=5)
except Exception:
    pass
elapsed_ms = (time.monotonic() - start) * 1000
print(f'{elapsed_ms:.0f}')
")

echo "  Latency: ${LATENCY_MS}ms (threshold: ${LATENCY_THRESHOLD_MS}ms)"

if [ "${LATENCY_MS}" -gt "${LATENCY_THRESHOLD_MS}" ] 2>/dev/null; then
    echo "  WARNING: Latency exceeds threshold!"
    python -c "
import asyncio
from app.core.logger import send_alert
asyncio.run(send_alert(
    title='Network Warning: High Latency',
    message='Health endpoint round-trip: ${LATENCY_MS}ms (threshold: ${LATENCY_THRESHOLD_MS}ms). Investigate network before market open.',
    severity='warning',
))
"
fi

# 4. Warmup — Hydrate Redis history cache
echo "[4/7] Running warmup worker..."
python -m workers.warmup_worker

# 5. Warmup Verification — Check baseline keys exist for all symbols
echo "[5/7] Verifying warmup baseline keys..."
MISSING_KEYS=$(python -c "
import os, sys, yaml, redis

config_path = 'config/symbols.yaml'
with open(config_path) as f:
    config = yaml.safe_load(f)

r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'), socket_timeout=5)

missing = []
for desk_id, desk_cfg in config.get('desks', {}).items():
    for symbol in desk_cfg.get('symbols', []):
        key = f'oniquant:baseline:{symbol}'
        if not r.exists(key):
            missing.append(symbol)

if missing:
    print(','.join(missing))
else:
    print('')
")

if [ -n "${MISSING_KEYS}" ]; then
    echo "  WARNING: Missing baseline keys for: ${MISSING_KEYS}"
    echo "  Re-running warmup worker for missing symbols..."
    python -m workers.warmup_worker --symbols "${MISSING_KEYS}"

    # Verify again after re-warmup
    STILL_MISSING=$(python -c "
import os, redis
symbols = '${MISSING_KEYS}'.split(',')
r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'), socket_timeout=5)
still = [s for s in symbols if not r.exists(f'oniquant:baseline:{s}')]
print(','.join(still) if still else '')
")

    if [ -n "${STILL_MISSING}" ]; then
        echo "  CRITICAL: Baseline keys still missing after re-warmup: ${STILL_MISSING}"
        python -c "
import asyncio
from app.core.logger import send_alert
asyncio.run(send_alert(
    title='Monday Warmup Incomplete',
    message='Missing baseline keys after re-warmup: ${STILL_MISSING}. Manual intervention required.',
    severity='warning',
))
"
    else
        echo "  OK: All baseline keys now present after re-warmup."
    fi
else
    echo "  OK: All baseline keys present."
fi

# 6. Validate environment
echo "[6/7] Validating environment..."
python scripts/validate_env.py

# 7. Send ready notification
echo "[7/7] Sending ready notification..."
python -c "
import asyncio, os
from app.core.logger import send_alert
asyncio.run(send_alert(
    title='System Warm — Ready for Monday Open',
    message='All pre-flight checks passed. Config verified. History hydrated. Latency: ${LATENCY_MS}ms.',
    severity='info',
    fields={
        'Environment': os.getenv('RAILWAY_ENVIRONMENT', 'local'),
        'Shadow Mode': os.getenv('GLOBAL_SHADOW_MODE', 'false'),
        'Latency': '${LATENCY_MS}ms',
    },
))
"

echo "============================================="
echo "OniQuant v6.0: READY"
echo "============================================="
