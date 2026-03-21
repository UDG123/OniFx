# OniQuant v6.0 — Red Team Audit Report

**Audit Type:** Zero-Trust Quantitative & Infrastructure Security Audit
**Auditor:** Senior Quantitative Auditor / Cyber-Security Engineer
**Date:** 2026-03-21
**Scope:** Mathematical integrity, look-ahead bias, Bayesian correctness, Redis atomicity, 2FA resilience, kill switch enforcement
**Target Win Rate:** >69.5%

---

## Executive Summary

The OniQuant v6.0 system demonstrates **strong mathematical foundations** in its Bayesian decision engine, synthetic matching engine, and Alpha Stack signal generators. The odds-form Bayes computation is correct, the adverse selection model is properly applied to volume, and the L2 fill simulation is free of look-ahead bias.

However, the audit identified **2 CRITICAL**, **2 HIGH**, and **3 STABILIZATION** findings that must be addressed before live deployment with real capital.

**Verdict: Certificate of Mathematical Integrity is WITHHELD** pending resolution of CRITICAL-01 and CRITICAL-02.

---

## Findings

### CRITICAL-01: Kill Switch Is Alert-Only — No Enforcement Gate

**Location:** Entire order execution path
**Files:** `app/core/logger.py:101`, `app/services/broker_tradfi.py:245-248`, `workers/memory_worker.py:174-252`, `workers/orchestration_worker.py:103-158`

**Description:**
The "Global Kill Switch" referenced in `logger.py:55` (`Global Kill Switch activation (drawdown > 3%)`) is implemented **only as a Discord/Slack alerting function** (`alert_kill_switch` at line 101). It sends a webhook notification but **does not set any Redis flag, database state, or in-memory gate** that would actually halt order flow.

The three critical order-path functions have **zero kill switch checks**:

| Component | Function | Kill Switch Check? |
|---|---|---|
| `orchestration_worker.py` | `_process_signal()` → ZADD to pending_trades | **NO** |
| `memory_worker.py` | `_scan_and_promote()` → XADD to match_validation | **NO** |
| `broker_tradfi.py` | `place_order()` → IB Gateway submission | **NO** |

**Impact:** If the system detects a 3%+ drawdown, it sends an alert to Discord but **continues authorizing and routing trades**. In a flash crash or adverse regime, the system will keep firing orders into the market with no circuit breaker.

**Remediation:**
```python
# In broker_tradfi.py, before every place_order():
kill = await self._redis.get("oniquant:kill_switch")
if kill and kill == b"ACTIVE":
    raise KillSwitchActiveError("Global kill switch is ACTIVE")

# In orchestration_worker.py, at top of _process_signal():
kill = await self._pool.get("oniquant:kill_switch")
if kill and kill == b"ACTIVE":
    await log.acritical("kill_switch_blocked_signal", signal_id=signal_id)
    return False
```

The kill switch activation logic (wherever drawdown is computed) must `SET oniquant:kill_switch ACTIVE` in Redis. Every order-path component must check this key **on every cycle**.

**Severity: CRITICAL** — Unchecked capital exposure in adverse conditions.

---

### CRITICAL-02: Ghost Trade Risk — Non-Atomic ZADD + XACK in Orchestration Worker

**Location:** `workers/orchestration_worker.py:120-158` and `workers/orchestration_worker.py:241-243`

**Description:**
The `_process_signal` method performs two operations sequentially without atomicity:

1. **Line 134:** `ZADD oniquant:pending_trades` (trade is now live in the pipeline)
2. **Line 243:** `XACK oniquant:raw_signals` (signal acknowledged in consumer group)

If the worker crashes **after ZADD but before XACK**, the signal remains in the Redis Pending Entries List (PEL). The `_reclaim_pending()` method at line 177 will XCLAIM the message and reprocess it. This reprocessing calls `_process_signal` again, which executes a **second ZADD**.

Critically, the ZADD member payload includes `"authorized_at": now` (line 132), which changes on each invocation. Since Redis ZSET members are compared by byte-equality, the second ZADD creates a **duplicate entry** — a ghost trade that will be independently promoted by the Memory Engine and potentially executed twice.

**Impact:** Duplicate order execution. In a crash-recovery scenario, the same signal can produce 2+ live orders, doubling position size and risk exposure.

**Remediation:**
```python
# Option A: Use a Redis pipeline with MULTI/EXEC for atomicity
pipe = self._pool.pipeline(transaction=True)
pipe.zadd(PENDING_ZSET, {orjson.dumps(trade_payload): expiry})
pipe.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
await pipe.execute()

# Option B: Idempotency key — use signal_id as the ZSET member key
# and store payload separately in a HASH, so re-ZADD is a no-op
```

**Severity: CRITICAL** — Uncontrolled position doubling on crash recovery.

---

### HIGH-01: Bayesian Naive Independence Assumption — Potential Double-Counting of IOF and kNN

**Location:** `app/services/orchestrator.py:107-174`

**Description:**
The `_compute_likelihood` method multiplies three factors independently:

```
L = IOF_factor × Macro_factor × Hurst_factor
```

This multiplicative combination assumes **conditional independence** between the factors — the Naive Bayes assumption. However:

- **IOF Strength** (Factor 1) is computed from displacement and relative volume (`alpha_stack.py:290-396`).
- **kNN Model** (used in Factor 3 / Hurst) is fitted on features that likely include volume ratio and momentum — signals that are **correlated with IOF**.

When IOF is high (strong institutional flow), the kNN model will also likely predict higher win probability (because it sees similar volume/momentum features). The Hurst factor then boosts the likelihood by ×1.30 on top of the IOF's ×1.50, yielding a combined ×1.95 that **overstates the true evidence**.

**Quantitative Impact:**
With a 50% prior and both IOF+Hurst boosting:
- Independent: L = 1.50 × 1.30 = 1.95 → posterior = 66.1%
- If correlation ρ ≈ 0.6, effective L ≈ 1.55 → posterior = 60.8%

The ~5pp overestimation could push borderline signals above the 69.5% threshold, inflating the authorization rate with false positives.

**Remediation:**
1. Compute the empirical correlation between IOF scores and kNN predictions over the training set.
2. If ρ > 0.4, apply a correlation discount: `L_adjusted = L^(1/√(1+ρ))` or use a copula-based joint likelihood.
3. Alternatively, orthogonalize the features: pass IOF-residualized features to kNN so the signals are decorrelated by construction.

**Severity: HIGH** — Systematic overestimation of posterior near the decision boundary.

---

### HIGH-02: 2FA TOTP Has No Clock Drift Resilience

**Location:** `scripts/generate_2fa.py:42-43`, `docker/Dockerfile.ibeam`

**Description:**
The TOTP generation command is:
```python
["oathtool", "--totp", "--base32", secret]
```

This uses the **default 30-second time step** with **no window tolerance flag** (`-w`). The `oathtool` binary generates a code for the current 30-second window based on the container's system clock.

**Clock Drift Scenario:**
Docker containers inherit the host clock, but clock synchronization can drift on cloud VMs (Railway.app runs on shared infrastructure). If the container clock drifts by **5+ seconds**:

- At window boundaries (e.g., t=28s into a 30s window), the generated code is for window N.
- By the time IBKR validates (network latency + processing), the server may have rolled to window N+1.
- IBKR's TOTP validator typically accepts ±1 adjacent window, but this is **not guaranteed** in their documentation.

**Additionally:** The Dockerfile installs no NTP client. There is no `chronyd`, `ntpd`, or `timedatectl` synchronization configured. The container relies entirely on the host's clock accuracy.

**Impact:** Intermittent 2FA failures during the daily IB Gateway login, causing the entire TradFi pipeline to go offline until manual intervention.

**Remediation:**
```python
# Add window tolerance to oathtool:
["oathtool", "--totp", "--base32", "-w", "1", secret]
# This generates codes for windows N-1, N, N+1 — pick the middle one

# In Dockerfile.ibeam, add NTP:
RUN apt-get install -y chrony && \
    echo "server time.google.com iburst" >> /etc/chrony/chrony.conf
```

Also consider generating the TOTP slightly before the window midpoint (at t=5-10s into a 30s window) by adding a deliberate delay in the login script.

**Severity: HIGH** — Intermittent TradFi pipeline outage during daily authentication.

---

### STABILIZATION-01: L2 Snapshot Timestamp Not Validated Against Signal Timestamp

**Location:** `app/services/matcher.py:63`, `app/services/matcher.py:264-312`

**Description:**
The `L2Snapshot` records a `timestamp` at construction time (`time.time()`), and `evaluate_and_persist` passes an `l2_snapshot` from the caller. However, **no validation exists** to ensure the L2 snapshot is temporally close to the signal's generation time.

In the pipeline: Signal → Orchestrator → pending_trades ZSET → Memory Engine → match_validation → Matcher. By the time the matcher evaluates, the L2 snapshot could be **seconds to minutes old** depending on queue depth and processing latency.

This is **not a look-ahead bias** (the snapshot is from the past, not the future), but it is a **stale data risk** — the fill simulation runs against an orderbook that no longer reflects reality.

**Current Status:** No look-ahead bias detected. The data flow is temporally correct (snapshot precedes evaluation). The concern is purely staleness.

**Remediation:**
```python
# In evaluate_l2_fill, add staleness check:
snapshot_age = time.time() - l2.timestamp
if snapshot_age > MAX_SNAPSHOT_AGE_SECONDS:  # e.g., 5s
    return FillResult(filled=False, ..., reason="stale_l2_snapshot")
```

**Severity: STABILIZATION** — Degraded fill simulation accuracy under load.

---

### STABILIZATION-02: Adverse Selection Penalty Correctly Applied to Volume (Confirmed Sound)

**Location:** `app/services/matcher.py:177-178`

**Description:**
The 20% adverse selection penalty is applied as:
```python
adverse_buffer = order_size * penalty_ratio      # 20% of order volume
required_volume = order_size + adverse_buffer     # Q × (1 + 0.20) = 1.2Q
```

This is **mathematically correct**. The penalty inflates the **required volume** (not price), meaning the order only fills if cumulative L2 liquidity exceeds 120% of the order size. This accurately models queue priority and adverse selection per Cont, Stoikov & Talreja (2010).

**However:** The penalty is a fixed 20% for all asset classes and market conditions. During high-volatility regimes (VIX > 30) or illiquid assets, the empirical adverse selection rate can exceed 40%. A regime-adaptive penalty would improve fill simulation accuracy.

**Remediation (Optional Enhancement):**
```python
# Regime-adaptive penalty:
if vix_regime == "elevated":
    penalty_ratio = 0.30
elif vix_regime == "extreme":
    penalty_ratio = 0.40
```

**Severity: STABILIZATION** — Core math is sound; enhancement for regime adaptivity.

---

### STABILIZATION-03: Likelihood Ratio Clamping Masks Extreme Signals

**Location:** `app/services/orchestrator.py:173`

**Description:**
The likelihood ratio is clamped to `[0.3, 3.0]`:
```python
L = max(0.3, min(3.0, L))
```

The maximum combined likelihood (IOF strong + Macro aligned + Hurst matched) is:
```
1.50 × 1.20 × 1.30 = 2.34  →  within bounds ✓
```

The minimum (IOF weak + Macro opposed + Hurst mismatched) is:
```
0.70 × 0.80 × 0.75 = 0.42  →  within bounds ✓
```

The clamping is **currently inert** for the defined multiplier ranges. However, if future multipliers are added or existing ones are tuned upward, the clamp will silently cap the likelihood without warning, potentially masking genuinely strong signals.

**Remediation:** Add a log warning when clamping activates:
```python
raw_L = L
L = max(0.3, min(3.0, L))
if L != raw_L:
    log.warning("likelihood_clamped", raw=raw_L, clamped=L)
```

**Severity: STABILIZATION** — No current impact; future-proofing recommendation.

---

## Bayesian Mathematics Verification

| Property | Expected | Actual | Status |
|---|---|---|---|
| Posterior formula (odds-form) | `P/(1-P) × L → back to prob` | `orchestrator.py:183-185` | **CORRECT** |
| Prior clamped to (0, 1) | `max(0.05, min(0.95, ...))` | `orchestrator.py:76,83` | **CORRECT** |
| Posterior clamped to (0, 1) | `max(0.0001, min(0.9999, ...))` | `orchestrator.py:185` | **CORRECT** |
| L=1.0 → posterior=prior | Identity property | Verified by `tests/math_properties.py:77-82` | **CORRECT** |
| Posterior monotonic in L | Higher L → higher posterior | Verified by `tests/math_properties.py:91-101` | **CORRECT** |
| Posterior bounded (0,1) | Never exceeds bounds | Verified by `tests/math_properties.py:65-70` | **CORRECT** |
| Decision threshold | posterior > 0.695 | `config.py:55`, `orchestrator.py:225` | **CORRECT** |

---

## Redis Resilience Assessment

| Mechanism | Implementation | Assessment |
|---|---|---|
| Consumer Groups | XREADGROUP + explicit XACK | **SOUND** — at-least-once delivery |
| PEL Reclaim | XCLAIM after 30s idle | **SOUND** — orphaned messages recovered |
| Dead Letter Queue | After 3 retries → DLQ stream | **SOUND** — prevents infinite retry loops |
| Crash Recovery | PEL + XCLAIM + reprocessing | **FLAWED** — see CRITICAL-02 (ghost trades) |
| Signal Reconciliation | `reconciler.py` — Redis vs TimescaleDB | **SOUND** — detects lost signals |

---

## Summary Table

| ID | Severity | Finding | File(s) |
|---|---|---|---|
| CRITICAL-01 | **CRITICAL** | Kill switch is alert-only, no enforcement gate | `logger.py`, `broker_tradfi.py`, `orchestration_worker.py`, `memory_worker.py` |
| CRITICAL-02 | **CRITICAL** | Ghost trades on crash recovery (non-atomic ZADD+XACK) | `orchestration_worker.py:120-158,241-243` |
| HIGH-01 | **HIGH** | Bayesian double-counting of correlated IOF/kNN signals | `orchestrator.py:107-174` |
| HIGH-02 | **HIGH** | TOTP 2FA has no clock drift tolerance or NTP sync | `generate_2fa.py:42-43`, `Dockerfile.ibeam` |
| STAB-01 | **STABILIZATION** | L2 snapshot staleness not validated | `matcher.py:63,264-312` |
| STAB-02 | **STABILIZATION** | Fixed 20% adverse selection penalty (regime-blind) | `matcher.py:177-178` |
| STAB-03 | **STABILIZATION** | Likelihood clamping is inert but unmonitored | `orchestrator.py:173` |

---

## Certification Status

```
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║   CERTIFICATE OF MATHEMATICAL INTEGRITY                         ║
║                                                                  ║
║   Status: ██ WITHHELD                                           ║
║                                                                  ║
║   The Bayesian decision engine, odds-form posterior computation, ║
║   adverse selection model, and Alpha Stack signal generators     ║
║   are mathematically sound.                                      ║
║                                                                  ║
║   Certification is BLOCKED by:                                   ║
║     • CRITICAL-01: No kill switch enforcement                   ║
║     • CRITICAL-02: Ghost trade risk on crash recovery           ║
║                                                                  ║
║   Upon remediation of CRITICAL findings, this system             ║
║   qualifies for certification with the >69.5% win rate target.  ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

---

*Report generated: 2026-03-21 | Auditor: Red Team Quantitative Audit*
