"""
OniQuant v6.0 — System Health Check
======================================
Verifies all system components are reachable:
    1. FastAPI ingestion endpoint
    2. Redis connection
    3. TimescaleDB hypertable
    4. IB Gateway (IBeam) port 4001

Exit code 0 = all healthy, 1 = one or more failures.
Usage: python scripts/health_check.py
"""

from __future__ import annotations

import os
import socket
import sys

import psycopg2
import redis
import urllib.request


def check_fastapi(url: str = "http://localhost:8000/health") -> bool:
    try:
        resp = urllib.request.urlopen(url, timeout=5)
        return resp.status == 200
    except Exception as e:
        print(f"  FAIL FastAPI: {e}")
        return False


def check_redis(url: str = "") -> bool:
    url = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        r = redis.from_url(url, socket_timeout=5)
        r.ping()
        return True
    except Exception as e:
        print(f"  FAIL Redis: {e}")
        return False


def check_timescaledb(dsn: str = "") -> bool:
    dsn = dsn or os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@localhost:5432/oniquant")
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'signal_ledger'")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        if count == 0:
            print("  WARN: signal_ledger hypertable not found")
            return False
        return True
    except Exception as e:
        print(f"  FAIL TimescaleDB: {e}")
        return False


def check_ib_gateway(host: str = "", port: int = 0) -> bool:
    host = host or os.getenv("IB_GATEWAY_HOST", "localhost")
    port = port or int(os.getenv("IB_GATEWAY_PORT", "4001"))
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        return True
    except Exception as e:
        print(f"  FAIL IB Gateway ({host}:{port}): {e}")
        return False


def main() -> int:
    checks = {
        "FastAPI": check_fastapi,
        "Redis": check_redis,
        "TimescaleDB": check_timescaledb,
        "IB Gateway": check_ib_gateway,
    }

    print("OniQuant v6.0 — System Health Check")
    print("=" * 45)

    all_ok = True
    for name, fn in checks.items():
        ok = fn()
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {name}")
        if not ok:
            all_ok = False

    print("=" * 45)
    if all_ok:
        print("All systems healthy.")
        return 0
    else:
        print("One or more checks failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
