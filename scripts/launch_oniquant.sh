#!/usr/bin/env bash
# ==========================================================================
# OniQuant v6.0 — Master Launch Sequence
# ==========================================================================
# Single command to bring the full system online for the Monday Open.
# ==========================================================================

set -euo pipefail

echo "╔══════════════════════════════════════════════╗"
echo "║     OniQuant v6.0 — LAUNCH SEQUENCE          ║"
echo "╚══════════════════════════════════════════════╝"

# Step 1: Environment Validation
echo ""
echo "[T-5] Validating environment secrets..."
python scripts/validate_env.py
if [ $? -ne 0 ]; then
    echo "ABORT: Environment validation failed."
    exit 1
fi
echo "  ✓ Environment validated"

# Step 2: Health Check
echo ""
echo "[T-4] Running system health check..."
python scripts/health_check.py
if [ $? -ne 0 ]; then
    echo "WARNING: Some health checks failed. Proceeding with caution."
fi
echo "  ✓ Health check complete"

# Step 3: Cold Boot Recovery (if needed)
echo ""
echo "[T-3] Checking for cold boot..."
python scripts/cold_boot.py
echo "  ✓ State reconstruction complete"

# Step 4: Warmup
echo ""
echo "[T-2] Hydrating market context..."
python -m workers.warmup_worker
echo "  ✓ Warmup complete — kNN/Spline baselines initialized"

# Step 5: Start services
echo ""
echo "[T-1] Starting all services..."
docker-compose up -d
echo "  ✓ All containers launched"

# Step 6: System live notification
echo ""
echo "[T-0] SYSTEM LIVE"
python -c "
import asyncio, os
from app.core.logger import send_alert
asyncio.run(send_alert(
    title='OniQuant v6.0: SYSTEM LIVE',
    message='All systems online. Launch sequence complete.',
    severity='info',
    fields={
        'Mode': os.getenv('GLOBAL_SHADOW_MODE', 'false') == 'true' and 'SHADOW' or 'LIVE',
        'Canary': os.getenv('CANARY_TRAFFIC_PERCENT', '5') + '%',
        'Environment': os.getenv('RAILWAY_ENVIRONMENT', 'local'),
    },
))
"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║     OniQuant v6.0: READY FOR MARKET OPEN     ║"
echo "╚══════════════════════════════════════════════╝"
