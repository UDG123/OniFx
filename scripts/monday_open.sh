#!/usr/bin/env bash
# ==========================================================================
# OniQuant v6.0 — Monday Morning Open Automation
# ==========================================================================
# Triggered via Railway Cron or manual CLI before market open.
# ==========================================================================

set -euo pipefail

echo "============================================="
echo "OniQuant v6.0 — Monday Open Sequence"
echo "============================================="

# 1. System Health Check
echo "[1/4] Running health check..."
python scripts/health_check.py
if [ $? -ne 0 ]; then
    echo "ABORT: Health check failed"
    exit 1
fi

# 2. Warmup — Hydrate Redis history cache
echo "[2/4] Running warmup worker..."
python -m workers.warmup_worker

# 3. Validate environment
echo "[3/4] Validating environment..."
python scripts/validate_env.py

# 4. Send ready notification
echo "[4/4] Sending ready notification..."
python -c "
import asyncio, os
from app.core.logger import send_alert
asyncio.run(send_alert(
    title='System Warm — Ready for Monday Open',
    message='All health checks passed. History hydrated. Workers online.',
    severity='info',
    fields={
        'Environment': os.getenv('RAILWAY_ENVIRONMENT', 'local'),
        'Shadow Mode': os.getenv('GLOBAL_SHADOW_MODE', 'false'),
    },
))
"

echo "============================================="
echo "OniQuant v6.0: READY"
echo "============================================="
