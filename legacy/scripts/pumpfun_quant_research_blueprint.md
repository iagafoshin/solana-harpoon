# Pump.fun Quant Research Blueprint  
**Version:** 1.0  
**Owner:** Ivan  
**Objective:** Build a robust, institutional-grade research pipeline to discover and validate durable alpha in a Pump.fun historical archive.

---

## 1) Executive Summary

You already completed the most important foundation step: **data cleaning + QA**.  
From here, the goal is to move from “feature tinkering” to a **quant research machine**:

1. Hypothesis-driven research  
2. Point-in-time correct feature/label construction  
3. Purged walk-forward validation  
4. Strategy-first evaluation (net PnL, EV, drawdown), not vanity ML metrics  
5. Production readiness with drift controls and kill-switches

---

## 2) Current State (based on your notebooks)

### Confirmed assets
- `00_investigation.ipynb` — dataset inventory, schema coverage, exploration.
- `01_data_qa.ipynb` — missingness checks, dedup, typing (`TRY_CAST`), time consistency, canonical `trades_clean`.

### Why this matters
- You already eliminated a huge class of hidden bugs (schema drift, malformed types, duplicates).
- You are ready for **research-grade labeling, feature engineering, and model/strategy testing**.

---

## 3) Research Principles (Non-Negotiable)

1. **No leakage**  
   Any feature for `(mint, t_decision)` can only use data available at or before `t_decision`.

2. **Economic objective first**  
   Optimize for **net expected value**, risk-adjusted return, and robustness—not AUC alone.

3. **Temporal validation only**  
   No random split. Use walk-forward with purge/embargo.

4. **Reproducibility**  
   Every experiment has fixed config, seed, data snapshot, and report template.

5. **Falsification mindset**  
   Try to break your own hypothesis before trusting it.

---

## 4) Research Contract (must be written before experiments)

Create `research_contract.md` with:

- **Universe definition:** which tokens/mints are included/excluded.
- **Decision timestamps:** e.g. `t0+60s`, `t0+180s`.
- **Forecast horizons:** e.g. +10m, +30m, +1h.
- **Execution assumptions:** fee, slippage, latency, partial fill.
- **Risk/capacity limits:** max SOL/trade, max concurrent positions, daily loss cap.
- **Success criteria:** required net EV, max drawdown bounds, stability requirements.

This prevents accidental hindsight bias.

---

## 5) Data Architecture (Layered)

Use explicit layers:

- `raw_*` — immutable source ingest
- `clean_*` — typed + deduped + QA-safe
- `feature_*` — point-in-time safe engineered features
- `label_*` — target construction
- `panel_*` — final train/infer table keyed by `(mint, t_decision)`

Recommended key fields in `panel_*`:
- `mint`
- `t0` (launch or first trade timestamp)
- `t_decision`
- all feature columns
- all target columns
- metadata (`regime`, `liquidity_bucket`, etc.)

---

## 6) Target Engineering (PnL-aware labels)

Binary migration alone is not enough.

### Core targets
For each `(mint, t_decision, horizon h)`:

- **MFE_h** — maximum favorable excursion
- **MAE_h** — maximum adverse excursion
- **HitTPBeforeSL_h** — TP reached before SL
- **TimeToTP / TimeToSL / TimeToDeath**
- **Survival_h** — token remains tradable/active

### Why this is better
You can directly model reward/risk profile and build policies like:
- enter only if `E[MFE] / E[MAE] > threshold`
- enter only when calibrated probability supports positive EV after costs

---

## 7) Feature Stack (Hardcore Quant)

## 7.1 Microstructure proxies
- Buy/sell intensity in multi-scale windows (5s/15s/60s)
- Inter-arrival statistics (mean, std, CV)
- Burstiness score
- Signed order-flow imbalance
- Local price-impact proxies

## 7.2 Bonding curve dynamics
- First derivative (flow speed)
- Second derivative (flow acceleration)
- Convexity regime indicators
- Liquidity absorption proxy (how much volume is needed to move price)

## 7.3 Information theory
- **User entropy** (Shannon over volume shares)
- **Time entropy** (trade-time bin distribution)
- Permutation entropy of event sequence
- KL divergence vs organic baseline flow profile

## 7.4 Graph / synchronization alpha
Use behavioral synchronization (soft links), not only on-chain funding links:

1. **Co-entry graph** (same mint, near-synchronous entry across many launches)  
2. **Creator-follower bipartite graph**  
3. **Super-node burst graph** (slot-level coordinated groups)

Feature examples:
- Cluster density
- Clustering coefficient
- Early-buyer eigenvector centrality
- Share of early volume by coordinated clusters
- Historical hit-rate of participating cluster

## 7.5 Wallet priors (strictly historical)
- Rolling win rate
- Historical median MFE/MAE
- Behavioral archetype stats (sniper/chaser/distributor proxies)

---

## 8) Validation Framework (Anti-overfitting)

## 8.1 Purged walk-forward CV
- Time-ordered folds
- Purge window around fold boundaries
- Embargo to reduce leakage from adjacent events

## 8.2 Multiple-testing control
- Benjamini-Hochberg FDR for large feature screens
- Bootstrap confidence intervals for EV
- Permutation tests for key edges

## 8.3 Stability checks
- Rolling feature-importance stability
- Drift tests (PSI / KS)
- Regime-conditioned performance consistency

---

## 9) Modeling Stack

## 9.1 Baselines first
- Logistic baseline
- Calibrated trees (CatBoost/XGBoost)

## 9.2 Time-to-event models
- Cox/AFT for event timing
- Competing risks (TP vs SL vs collapse)

## 9.3 Hawkes module as signal block
Estimate `(μ, α, β)` on early event flow and test if it adds incremental EV.

## 9.4 Calibration
- Platt / Isotonic
- Reliability curves
- Trade only above calibrated confidence thresholds

---

## 10) Strategy Layer (Model ≠ Trading System)

## 10.1 Policy
Map model outputs to actions:
- Entry threshold by expected net value
- Position sizing by confidence and liquidity penalty
- No-trade zone under uncertain regimes

## 10.2 Risk engine
- Max daily loss
- Max consecutive losses
- Exposure caps by cluster
- Data-quality kill-switch

## 10.3 Cost model
Include:
- Fees
- Slippage
- Latency penalty
- Fill uncertainty

Stress over a grid: `cost × latency × fill_ratio`.

---

## 11) Backtesting Standard (Institutional-lite)

## 11.1 Event-driven simulation
- Decision only at `t_decision`
- No future reads
- Realistic execution assumptions

## 11.2 Reporting
Always separate:
- Gross vs net PnL
- Performance by regime/time/liquidity bucket
- Contribution by feature family/cluster type

## 11.3 Robustness suite
- Bootstrap backtests
- Latency/cost Monte Carlo
- Feature ablation (`ΔEV` when removing each block)

---

## 12) Production Readiness Checklist

Before deploying real capital:

1. Shadow mode for 2–4 weeks  
2. Signal integrity monitors (feature nulls, schema drift, stale inputs)  
3. Input/output drift monitoring  
4. Auto-disable rules on degradation

---

## 13) 14-Day Execution Plan

### Days 1–2
- Finalize `research_contract.md`
- Build unified decision panel (`t_decision = 60s` and `180s`)
- Generate MFE/MAE labels

### Days 3–5
- Implement entropy features
- Implement sync-graph features
- Add rolling wallet priors (past-only)

### Days 6–7
- Run purged walk-forward baseline (logit + CatBoost/XGBoost)
- Evaluate calibration and EV curves

### Days 8–10
- Add policy + cost model + risk constraints
- Run stress grid (latency/cost/fill)

### Days 11–12
- Run ablation + stability report
- Select minimal robust feature set

### Days 13–14
- Launch shadow pipeline
- Define production kill-switch rules

---

## 14) Definition of Done (Alpha Gate)

Alpha is considered “real” only if **all** hold:

1. Net EV > 0 out-of-sample **after costs/latency**
2. Stable performance across at least 3 walk-forward windows
3. Drawdown within predefined risk budget
4. No critical drift in shadow mode
5. Edge survives feature ablation (not a single-fragile-factor artifact)

---

## 15) Suggested Repository Structure

```text
research/
  contracts/
    research_contract.md
  data/
    raw/
    clean/
    feature/
    label/
    panel/
  configs/
    feature_spec.yaml
    cv_config.yaml
    backtest_config.yaml
  notebooks/
    00_investigation.ipynb
    01_data_qa.ipynb
    02_feature_lab.ipynb
  src/
    features/
    labels/
    models/
    backtest/
    monitoring/
  reports/
    experiment_YYYYMMDD_*.md
```

---

## 16) Immediate Next Step

Start with the highest signal-to-noise blocks:

1. **Entropy block** (user/time entropy + divergence metrics)  
2. **Sync-graph block** (co-entry, cluster density, early centrality)

Then measure **incremental EV uplift** over baseline, not just metric uplift.

---

## 17) Final Note

This framework is designed to avoid the two classic traps:
- “Looks good in notebook, dies in production”
- “Great AUC, negative net PnL”

Your current QA foundation is strong enough to execute this at a true quant-research level.
