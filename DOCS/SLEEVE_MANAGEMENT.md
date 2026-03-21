# OniFx Sleeve Management Guide

## The 3-Floor Capital Sleeve Model

OniFx operates three **autonomous trading floors**, each isolated into its own
capital sleeve (sub-account). Every signal you receive belongs to exactly one
floor. This guide explains how to set up your brokerage accounts, size your
positions, and manage risk across all three floors.

---

## The 50/30/20 Rule

Your total Net Liquidation Value (NLV) is split into three sleeves:

| Floor | Allocation | Signal Type | Risk per Trade | Telegram |
|-------|-----------|-------------|----------------|----------|
| **STABLE** | 50% of NLV | Mean Reversion | 3.25% of sleeve | `[OniFx STABLE]` |
| **ACTIVE** | 30% of NLV | Trend Following | 3.33% of sleeve | `[OniFx ACTIVE]` |
| **AGGRESSIVE** | 20% of NLV | Volatility Breakout | 2.92% of sleeve | `[OniFx AGGRESSIVE]` |

### Example: $100,000 Total Account

| Floor | Capital | Risk per Trade | Dollar Risk |
|-------|---------|----------------|-------------|
| STABLE | $50,000 | 3.25% | $1,625 |
| ACTIVE | $30,000 | 3.33% | $999 |
| AGGRESSIVE | $20,000 | 2.92% | $584 |

**Worst-case simultaneous hit across all 3 floors: $3,208 = 3.21% of total NLV.**

---

## The 1/10th Kelly Rule (RSSF)

OniFx does **not** use full Kelly, half-Kelly, or even quarter-Kelly. Every
floor uses a **Risk-Scaled Sizing Factor (RSSF)** of exactly 0.10 (one-tenth):

```
Position Risk = Raw Kelly Edge x RSSF Multiplier
             = Raw Kelly x 0.10
```

| Floor | Raw Kelly | RSSF | Final Sleeve Risk |
|-------|----------|------|-------------------|
| STABLE | 0.325 | 0.10 | 3.25% |
| ACTIVE | 0.333 | 0.10 | 3.33% |
| AGGRESSIVE | 0.292 | 0.10 | 2.92% |

### Why 1/10th Kelly?

- **Full Kelly** maximizes long-run growth but produces drawdowns of 50%+ that
  no human can tolerate psychologically.
- **Quarter Kelly** (0.25x) reduces variance but still implies ~12% drawdowns
  on correlated losing streaks.
- **1/10th Kelly** keeps per-trade risk under 3.5% of the *sleeve*, which
  translates to under 1.75% of total NLV on the largest floor (STABLE).
  This is the sweet spot between edge capture and survivability.

---

## Account Setup Instructions

### Step 1: Create 3 Sub-Accounts

At your broker (Interactive Brokers recommended), create three sub-accounts
or allocate funds into three mental buckets:

1. **STABLE Sub-Account** — Fund with 50% of your total trading capital
2. **ACTIVE Sub-Account** — Fund with 30% of your total trading capital
3. **AGGRESSIVE Sub-Account** — Fund with 20% of your total trading capital

### Step 2: Subscribe to Telegram Channels

Each floor publishes signals to a dedicated Telegram channel:

- **STABLE Channel** — Mean reversion signals with tight stops
- **ACTIVE Channel** — Trend-following signals with moderate hold periods
- **AGGRESSIVE Channel** — Volatility breakout signals with wide stops

### Step 3: Position Sizing Worksheet

When you receive a signal, calculate your position size:

```
Dollar Risk = Sleeve Capital x Final Sleeve Risk %
Shares = Dollar Risk / (Entry Price - Stop Price)
```

**Example — STABLE BUY SPY at $450, Stop at $447:**
```
Dollar Risk = $50,000 x 3.25% = $1,625
Shares = $1,625 / ($450 - $447) = $1,625 / $3.00 = 541 shares
```

**Example — AGGRESSIVE BUY BTCUSDT at $65,000, Stop at $63,500:**
```
Dollar Risk = $20,000 x 2.92% = $584
Position = $584 / ($65,000 - $63,500) = $584 / $1,500 = 0.389 BTC
```

---

## Risk Manifesto

### Rule 1: Sleeves Are Autonomous
A drawdown on the AGGRESSIVE floor does **not** affect STABLE or ACTIVE
capital. Each sleeve manages its own equity curve independently. Never
"borrow" capital from one sleeve to cover losses on another.

### Rule 2: Risk Is Always Per-Sleeve
When a signal says "RISK: 3.25%", that means 3.25% of the **STABLE sleeve**
($50,000), not 3.25% of your total account ($100,000). The per-sleeve
isolation is what keeps your maximum total drawdown bounded.

### Rule 3: Maximum Theoretical Drawdown
If all three floors simultaneously hit their maximum single-trade loss:
```
STABLE:      3.25% x 50% = 1.625% of NLV
ACTIVE:      3.33% x 30% = 0.999% of NLV
AGGRESSIVE:  2.92% x 20% = 0.584% of NLV
                            ─────────────
TOTAL:                      3.208% of NLV
```
This is the **absolute worst-case** for a single simultaneous signal on each floor.
In practice, signals rarely fire simultaneously across all three floors.

### Rule 4: Rebalance Monthly
At the start of each month, rebalance your sleeves back to 50/30/20. If
the AGGRESSIVE sleeve has grown to 25% of NLV, skim the excess into STABLE.
If STABLE has shrunk, replenish it from the other sleeves.

### Rule 5: Kill Switch = All Floors Halt
If the global kill switch activates (drawdown > 3% intraday across any
floor), **all three floors** are halted simultaneously. This is a
circuit breaker, not a per-floor control. Resume trading only after
manual investigation.

---

## Signal Format Reference

### STABLE Signal
```
[OniFx STABLE] SIGNAL: BUY SPY
Type: MEAN REVERSION | Confidence: 73.2%
RISK: 3.25% of your STABLE SLEEVE (Sub-Account)

Posterior: 0.7324 | Threshold: 70%
Target Price: 451.20
Raw Kelly: 0.325 | RSSF: 0.1
Sleeve Allocation: 50% of NLV
```

### ACTIVE Signal
```
[OniFx ACTIVE] SIGNAL: BUY AAPL
Type: TREND FOLLOWING | Confidence: 65.8%
RISK: 3.33% of your ACTIVE SLEEVE (Sub-Account)

Posterior: 0.6581 | Threshold: 60%
Target Price: 198.50
Raw Kelly: 0.333 | RSSF: 0.1
Sleeve Allocation: 30% of NLV
```

### AGGRESSIVE Signal
```
[OniFx AGGRESSIVE] SIGNAL: BUY BTCUSDT
Type: VOLATILITY BREAKOUT | Confidence: 54.1%
RISK: 2.92% of your AGGRESSIVE SLEEVE (Sub-Account)

Posterior: 0.5412 | Threshold: 51%
Target Price: 67500.00
Raw Kelly: 0.292 | RSSF: 0.1
Sleeve Allocation: 20% of NLV
```

---

## FAQ

**Q: Can I just follow one floor?**
Yes. Each floor is independent. If you only want conservative mean-reversion
signals, subscribe to the STABLE channel only and allocate 100% of your
desired capital to that sleeve (still risk 3.25% per trade).

**Q: What if I have a small account ($10,000)?**
The same percentages apply: STABLE=$5,000, ACTIVE=$3,000, AGGRESSIVE=$2,000.
Per-trade dollar risk on STABLE would be $162.50. If that's too small for
your instrument's minimum lot size, consolidate into fewer floors.

**Q: Why not use quarter-Kelly like everyone else?**
Quarter-Kelly on the *total account* sounds safe until you realize that 3
correlated positions at quarter-Kelly can produce 15%+ drawdowns. By isolating
into sleeves AND applying 1/10th Kelly per sleeve, we keep the maximum
theoretical drawdown at 3.21% of NLV, even in the worst case.

**Q: How are the Raw Kelly values calculated?**
They are derived from the CPCV-validated out-of-sample Sharpe Ratios and
win rates for each floor's signal profile. The values are hardcoded after
validation and only change after a full CPCV re-run and manual review.
