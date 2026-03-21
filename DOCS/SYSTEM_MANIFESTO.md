# OniFx v6.0 — Technical Manifesto

**Date:** 2026-03-21
**Status:** Production-Ready (All Red Team Findings Resolved)

---

## 1. The Core Engine: 3 Autonomous Floors

OniFx operates three independent signal floors. Each floor runs its own
Bayesian decision engine with Power Priors, Likelihood Tempering, and
regime-specific thresholds. All three evaluate every signal in parallel;
the active regime controls live order flow while the others shadow-log
for CPCV optimization.

### Floor Parameters (from `config/risk_profiles.yaml`)

| Parameter | STABLE | ACTIVE | AGGRESSIVE |
|-----------|--------|--------|------------|
| **Bayesian Threshold** | 70% | 60% | 51% |
| **Power Prior (a0)** | 0.80 | 0.50 | 0.15 |
| **Likelihood Tempering (T)** | 1.0 | 1.5 | 2.5 |
| **Raw Kelly** | 0.325 | 0.333 | 0.292 |
| **RSSF Multiplier** | 0.10 | 0.10 | 0.10 |
| **Final Sleeve Risk** | 3.25% | 3.33% | 2.92% |
| **Signal Type** | Mean Reversion | Trend Following | Volatility Breakout |

### Bayesian Pipeline (per signal, per floor)

```
1. Fetch Prior           P(A) = 30-day win rate from TimescaleDB
2. Power Prior           P_floor(A) = P(A) ^ a0
3. Compute Likelihood    L = IOF × Macro × Hurst (with de-correlation penalty)
4. Temper Likelihood     L_floor = L ^ (1/T)
5. Posterior (Odds Form) P(A|B) = (P_floor/(1-P_floor) × L_floor) / (1 + same)
6. Decision              AUTHORIZE if P(A|B) > threshold, else REJECT
```

### Selection Bias Guard

The AGGRESSIVE floor's flat power prior (`a0=0.15`) maps even a 40% historical
win rate to a 0.87 regime prior — making history nearly irrelevant. To prevent
authorizing assets with genuinely poor track records purely on live momentum:

> **If `a0 < 0.30` AND `base_prior < 0.40`, the floor REJECTS regardless of posterior.**

This is a structural safeguard, not a Bayesian one. It ensures the AGGRESSIVE
floor cannot be tricked by strong short-term signals on fundamentally weak assets.

Source: `app/services/orchestrator.py`, `_evaluate_regime()`

### Indicator De-correlation (HIGH-01 Fix)

When correlated indicators both boost the likelihood (e.g., IOF + kNN,
correlation = 0.60), a square-root penalty dampens the compound evidence:

```
L_adjusted = L ^ (1 / sqrt(1 + rho_max))
```

This prevents the posterior from being inflated by double-counting shared
information between overlapping signal sources.

Source: `app/services/orchestrator.py`, `INDICATOR_CORRELATION_MATRIX`

---

## 2. The Capital Sleeve Model: The 50/30/20 Rule

Each floor is backed by an isolated capital sleeve (sub-account). Sleeves
never share capital. Risk percentages are always relative to the sleeve,
never the total portfolio.

### Allocation

| Floor | Sleeve Allocation | Per-Trade Risk (of sleeve) | NLV Impact |
|-------|------------------|---------------------------|------------|
| **STABLE** | 50% of NLV | 3.25% | 1.625% |
| **ACTIVE** | 30% of NLV | 3.33% | 0.999% |
| **AGGRESSIVE** | 20% of NLV | 2.92% | 0.584% |

### The 1/10th Kelly Rule

Every floor uses an RSSF (Risk-Scaled Sizing Factor) of exactly 0.10:

```
Position Risk = Raw Kelly Edge x 0.10
```

This is deliberately more conservative than the industry-standard
quarter-Kelly (0.25x). The rationale:

- Full Kelly: maximizes growth but produces 50%+ drawdowns
- Quarter Kelly: 12%+ drawdowns on correlated losing streaks
- **1/10th Kelly: per-trade risk capped at 2.92%-3.33% of sleeve**

### Variance Drag Protection

The AGGRESSIVE floor receives the smallest capital allocation (20%) despite
having the widest stop-loss tolerances. This is intentional variance management:

- AGGRESSIVE signals are high-conviction but low-frequency with fat-tailed
  outcomes. A single losing streak has bounded impact because the sleeve
  is small relative to total NLV.
- 7 consecutive AGGRESSIVE losses = 20.4% sleeve drawdown = 4.1% of total NLV.
- Reaching 20% total NLV drawdown from AGGRESSIVE alone requires 34
  consecutive full losses — an astronomically unlikely event.

### Maximum Theoretical Drawdown

If all three floors simultaneously experience a maximum single-trade loss:

```
STABLE:      3.25% x 50% = 1.625% of NLV
ACTIVE:      3.33% x 30% = 0.999% of NLV
AGGRESSIVE:  2.92% x 20% = 0.584% of NLV
                            ───────────────
TOTAL:                      3.208% of NLV
```

This worst-case scenario requires three simultaneous full-loss signals
across all three floors — practically impossible in normal market conditions.

---

## 3. The Security Perimeter: The Iron Gate

### Layer 1: Rate Limiting

Redis-backed sliding window rate limiter on all webhook endpoints.

| Parameter | Value | Source |
|-----------|-------|--------|
| Requests per window | 60 | `WEBHOOK_RATE_LIMIT` env var |
| Window duration | 60 seconds | `WEBHOOK_RATE_WINDOW` env var |
| Response on breach | 429 + `Retry-After` header | `app/main.py` |
| IP extraction | `X-Forwarded-For` aware | `_get_client_ip()` |

Implementation: Atomic Lua script (`INCR` + `EXPIRE`) for zero-race-condition
counting. Source: `app/main.py`

### Layer 2: Payload Size Gate

| Gate | Limit | HTTP Response |
|------|-------|---------------|
| Total payload | 8,192 bytes (8 KiB) | 413 Request Entity Too Large |
| `indicator_metadata` | 4,096 bytes (4 KiB) | 422 Validation Error |

Oversized payloads are rejected *before* JSON parsing to prevent
decompression bombs and memory exhaustion.

### Layer 3: Pydantic v2 Schema Validation

All incoming payloads are validated against `TradingViewPayload` in
`app/schemas.py`. The schema enforces:

| Field | Constraint |
|-------|-----------|
| `desk_id` | Whitelist: `scalper_spx`, `scalper_fx`, `scalper_ndx`, `swing_tech`, `macro_metals`, `macro_gold`, `alts_major`, `alts_mid`, `alts_btc`, `alts_eth`, `luxalgo` |
| `asset_symbol` | Regex: `^[a-zA-Z0-9_\-./: ]{1,128}$` |
| `signal_direction` | Integer, `-1` or `1` only |
| `target_price` | Float, `> 0`, `< 1,000,000` |
| `confidence` | Float, `0.0` to `1.0` |
| `model_source` | Enum: `luxalgo`, `spline`, `knn`, `unknown` |
| `asset_class` | Enum: `equity`, `forex`, `metals`, `gold`, `futures`, `index`, `crypto`, `perpetual`, `spot_crypto` |

Payloads that fail validation return 422 with structured error details.
Unknown fields are silently stripped via Pydantic re-serialization.

### Layer 4: Kill Switch

| Parameter | Value |
|-----------|-------|
| Redis Key | `oniquant:global_kill_switch` |
| TTL | 86,400 seconds (24 hours) |
| Activation | `alert_kill_switch()` in `app/core/logger.py` |
| Enforcement Points | `broker_tradfi.py`, `orchestration_worker.py`, `memory_worker.py` |

The kill switch halts ALL order flow across ALL floors when activated.
It is checked before every order placement, signal authorization, and
price-cross promotion.

### Layer 5: Atomic Redis Operations

The XACK + ZADD race condition (ghost trade duplication on crash recovery)
is eliminated by a Lua script that performs both operations atomically:

```lua
redis.call('XACK', KEYS[1], KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
return 1
```

Source: `workers/orchestration_worker.py`, `LUA_ATOMIC_ACK_AND_ROUTE`

---

## 4. The Night Auditor: The Quality Controller

`scripts/night_auditor.py` — Cron-scheduled weekly verification job.

### Schedule

```
0 22 * * 0  cd /app && python scripts/night_auditor.py
```

### Check 1: Model Drift Detection

| Parameter | Value |
|-----------|-------|
| Drift Threshold | 15% (`DRIFT_THRESHOLD = 0.15`) |
| Lookback | 7 days (configurable via `--lookback-days`) |
| Data Source | TimescaleDB `signal_ledger` |

For each floor, the auditor:
1. Queries the realized win rate from the last 7 days of signal outcomes
2. Compares against the Bayesian prior cached in Redis
3. If `|realized - prior| > 15%`, flags the floor and recommends a prior update

The auditor **detects and recommends**. A human reviews and applies config changes.
No autonomous config mutation.

### Check 2: Asymmetric R:R Verification (AGGRESSIVE)

| Parameter | Value |
|-----------|-------|
| Minimum R:R | 3.0x (`RR_MINIMUM_AGGRESSIVE = 3.0`) |
| Decay Threshold | 20% week-over-week decline (`RR_DECAY_THRESHOLD = 0.20`) |

The AGGRESSIVE floor is designed for fat-tailed outcomes. If the reward-to-risk
ratio drops below 3:1, or declines by more than 20% from the previous week,
the auditor flags **"Strategy Decay"** and recommends pausing the floor.

### Check 3: Payload Schema Sync

The auditor programmatically extracts all field names from the
`TradingViewPayload` Pydantic model and verifies each one is documented
in `DOCS/TRADINGVIEW_PAYLOAD.txt`. Any missing fields are flagged.

This prevents silent schema drift where code changes break the user-facing
webhook instructions.

### Health Classification

| Status | Condition |
|--------|-----------|
| **GREEN** | No drift exceeded, no decay, schema in sync |
| **YELLOW** | Prior drift >15% on any floor, or schema out of sync |
| **RED** | Strategy decay detected, or AGGRESSIVE R:R below 3:1 |

### Output

- Console: Structured per-floor report with specific recommendations
- Telegram: HTML-formatted summary to admin channel (unless `--dry-run`)

---

## 5. System Architecture Summary

```
TradingView Alert
       |
       v
  [Rate Limiter] ── 429 if > 60/min
       |
  [Size Gate] ── 413 if > 8 KiB
       |
  [Pydantic Schema] ── 422 if invalid
       |
       v
  Redis Stream (oniquant:raw_signals)
       |
       v
  [Kill Switch Gate] ── drop if active
       |
       v
  Tri-State Bayesian Arbiter
  ┌──────────┬──────────┬──────────┐
  │  STABLE  │  ACTIVE  │ AGGRESS. │
  │  a0=0.80 │  a0=0.50 │  a0=0.15 │
  │  T=1.0   │  T=1.5   │  T=2.5   │
  │  >70%    │  >60%    │  >51%    │
  └──────────┴──────────┴──────────┘
       |              |
  Active Regime    Shadow Ledger
  routes live      logs all 3 for
  orders           CPCV analysis
       |
       v
  [Atomic XACK+ZADD] ── Lua script
       |
       v
  Memory Engine (pending_trades ZSET)
       |
       v
  L2 Synthetic Matcher
  (staleness gate + VIX-adaptive penalty)
       |
       v
  Broker (IBKR_TWS / BYBIT_V5)
```

---

## 6. File Reference

| Component | File | Purpose |
|-----------|------|---------|
| Webhook Ingestion | `app/main.py` | Rate limiting, schema validation, XADD |
| Payload Schema | `app/schemas.py` | Pydantic v2 strict validation |
| Bayesian Engine | `app/services/orchestrator.py` | Tri-state Power Priors + Tempering |
| Kill Switch | `app/core/kill_switch.py` | Redis-backed circuit breaker |
| Alert Dispatch | `app/core/logger.py` | Discord/Slack webhooks + kill switch activation |
| Telegram Signals | `app/core/telegram.py` | Per-floor signal formatting + dispatch |
| L2 Matcher | `app/services/matcher.py` | Staleness gate + VIX-adaptive penalty |
| Broker Gateway | `app/services/broker_tradfi.py` | IBKR order placement + kill switch gate |
| Alpha Stack | `app/quant/alpha_stack.py` | kNN + Spline + IOF signal generation |
| Position Sizing | `app/quant/sizing.py` | Kelly Criterion + cluster risk |
| CPCV Optimizer | `app/quant/cpcv_optimizer.py` | Combinatorial purged cross-validation |
| Orchestration Worker | `workers/orchestration_worker.py` | Stream consumer + atomic Lua + shadow ledger |
| Memory Worker | `workers/memory_worker.py` | Price-cross promotion + kill switch gate |
| Night Auditor | `scripts/night_auditor.py` | Weekly drift, R:R decay, schema sync |
| Risk Profiles | `config/risk_profiles.yaml` | 3-floor regime configuration |
| Symbol Universe | `config/symbols.yaml` | Desk subscriptions + global defaults |
| Dashboard | `app/dashboard.py` | Streamlit ops + CPCV optimization panel |
| Payload Docs | `DOCS/TRADINGVIEW_PAYLOAD.txt` | User-facing webhook format reference |
| Sleeve Guide | `DOCS/SLEEVE_MANAGEMENT.md` | Client onboarding + position sizing |
| Audit Report | `DOCS/RED_TEAM_AUDIT_REPORT.md` | All 7 findings resolved |
| Integrity Hashes | `DOCS/INTEGRITY_HASH.txt` | SHA-256 of critical files |

---

*OniFx v6.0 — Technical Manifesto*
*All values sourced directly from production code as of 2026-03-21.*
