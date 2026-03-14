# QueueingModelAnalyzer

**Location:** `internal/engines/analyzers/queueingmodel/analyzer.go`

The QueueingModelAnalyzer is a capacity analyzer that uses queueing theory to determine
how many requests each model variant can handle while meeting latency SLOs. It learns
hardware characteristics online and reasons about capacity mathematically.

## Table of Contents

1. [Background: What Problem Does This Solve?](#background)
2. [Core Concepts](#core-concepts)
3. [Architecture Overview](#architecture-overview)
4. [Activating the Queueing Model Analyzer](#activating)
5. [ConfigMap Reference](#configmap-reference)
6. [SLO Targeting](#slo-targeting)
7. [Initial Parameter Estimation (Cold Start)](#cold-start)
8. [Data Flow](#data-flow)
9. [Key Files](#key-files)
10. [Defaults Reference](#defaults-reference)

---

<a name="background"></a>
## 1. Background: What Problem Does This Solve?

The WVA (Workload Variant Autoscaler) needs to answer: *"given the current request arrival
rate, how many replicas of each model variant are required to keep latency within SLO?"*

The QueueingModelAnalyzer answers this by:

1. **Learning** the three hardware parameters `(alpha, beta, gamma)` from live traffic using
   an Extended Kalman Filter — no manual calibration needed.
2. **Predicting** the maximum request rate that a single replica can sustain while meeting
   user-specified (or automatically inferred) TTFT and ITL SLOs.
3. **Deciding** the required replica count as `ceil(total_arrival_rate / max_request_rate)`.

---

<a name="core-concepts"></a>
## 2. Core Concepts

### 2.1 Workload Aggregation across Pods

All components — the tuner, the SLO inference, and the capacity calculation — operate on a
single workload summary per variant, derived by aggregating per-pod metrics. Only pods with
active traffic (`arrival_rate > 0`) contribute. Token lengths, TTFT, and ITL are averaged
weighted by each pod's arrival rate, so that a busier pod contributes proportionally
more to the aggregate:

```
avg_input_len  = Σ(arrival_rate_i × input_len_i)  / Σ(arrival_rate_i)
avg_output_len = Σ(arrival_rate_i × output_len_i) / Σ(arrival_rate_i)
avg_TTFT       = Σ(arrival_rate_i × TTFT_i)       / Σ(arrival_rate_i)
avg_ITL        = Σ(arrival_rate_i × ITL_i)        / Σ(arrival_rate_i)
```

The per-variant arrival rate used for capacity sizing is the sum across all busy pods:
`total_arrival_rate = Σ arrival_rate_i`.

### 2.2 Service Time Model

A vLLM server processes requests in batched iterations. Each iteration handles `n` concurrent
requests. The time taken per iteration grows linearly with batch size:

```
IterationTime(n) = alpha + n × (beta × tokensCompute + gamma × tokensMemory)
```

| Parameter | Meaning                                      | Typical range |
|-----------|----------------------------------------------|---------------|
| **alpha** | Baseline iteration overhead (ms) — constant per iteration regardless of batch | 1–20 ms |
| **beta**  | Compute time per token (ms/token) — GPU FLOP cost | 0.01–1 ms |
| **gamma** | KV-cache memory access time per token (ms/token) — memory bandwidth cost | 0.00001–0.1 ms |

These three numbers fully characterise a hardware/model combination. The tuner learns them
online from TTFT and ITL observations.

### 2.3 TTFT and ITL from the Queueing Model

A request's latency has two observable components:

- **TTFT (Time-to-First-Token)**: queueing wait + prefill time.
- **ITL (Inter-Token Latency)**: time per generated token during decode.

The queueing model predicts them as (all values in milliseconds):

```
TTFT = T_iter + (beta + gamma) × input_len
ITL  = T_iter + beta + gamma × (input_len + (output_len + 1) / 2)
```

where `T_iter` is the mean iteration time under the current load level, which depends on
arrival rate, batch size, and the `(alpha, beta, gamma)` parameters.

### 2.4 Queueing Model (State-Dependent M/M/1-like)

The server is modelled as a state-dependent queue. The key insight is that the iteration
time changes with the number of concurrently running requests (`n`), making this an
M/M/1 queue with state-dependent service rates. Given parameters and an arrival rate
`lambda`, the model computes steady-state distributions over the number of requests in the
system, from which TTFT and ITL are derived analytically.

### 2.5 Capacity Calculation

Once parameters are learned, `QueueAnalyzer.Size(targetPerf)` binary-searches for the
maximum arrival rate `lambda*` such that the predicted TTFT and ITL both remain within
the SLO targets. This `lambda*` is the **per-replica capacity**.

The required replica count for a variant is then:

```
required_replicas = ceil(total_arrival_rate / lambda*)
```

### 2.6 Per-Variant Failure Behavior

If analysis of an individual variant fails at any step — no metrics, no active traffic, no
learned parameters yet, or a queueing model error — the variant is **not dropped**. Instead
it is included in the result with a zero-capacity placeholder:

```
PerReplicaCapacity = 0
TotalCapacity      = 0
TotalDemand        = 0
Utilization        = 0
```

The variant's current replica count and pending replicas are still reported accurately.
This ensures the optimizer sees the full variant roster and can apply safe-hold behavior
rather than making decisions on an incomplete picture.

An error is returned at the model level only if **every** variant produces a zero-capacity
result (i.e. the `variantCapacities` slice is entirely composed of failures), in which case
no scaling decision is emitted for that model during that cycle.

---

<a name="architecture-overview"></a>
## 3. Architecture Overview

The analyzer has two collaborating components:

### 3.1 The Tuner (Online Parameter Estimator)

**Location:** `internal/engines/analyzers/queueingmodel/tuner/tuner.go`

The Tuner wraps an **Extended Kalman Filter (EKF)** that treats `(alpha, beta, gamma)` as
the hidden state and `(AvgTTFT, AvgITL)` as observations. On each reconcile cycle it:

1. **Restores** the previous state estimate and error covariance from the `ParameterStore`
   (or bootstraps from a cold-start guess — see [Section 7](#cold-start)).
2. **Predicts** the next state using an identity transition (parameters are assumed slowly
   varying).
3. **Updates** by comparing the predicted `(TTFT, ITL)` produced by `QueueAnalyzer.Analyze()`
   against the newly observed values.
4. **Validates** the update using the Normalized Innovation Squared (NIS):
   - NIS follows a chi-squared distribution with 2 degrees of freedom under correct model
     assumptions.
   - Updates with NIS ≥ 7.378 (95th percentile of χ²₂) are rejected as outliers.
   - On rejection, the previous state is restored — the filter never accepts a bad update.
5. **Stores** the accepted `(alpha, beta, gamma)` and covariance back in the `ParameterStore`
   for the next cycle.

### 3.2 The QueueAnalyzer (Queueing Model)

**Location:** `pkg/analyzer/queueanalyzer.go`

Serves two roles:

| Role | Method | Called by |
|------|--------|-----------|
| Observation function inside EKF | `Analyze(lambda)` | Tuner, on every filter update |
| Capacity sizing | `Size(targetPerf)` | Analyzer, after tuning |

`Analyze(lambda)` returns predicted `(TTFT, ITL, throughput)` at a given arrival rate,
which lets the Kalman filter compare prediction against observation to refine the state.

`Size(targetPerf)` binary-searches for the maximum `lambda` that keeps both TTFT and ITL
within the SLO. The result is the **per-replica capacity** in requests/second.

---

<a name="activating"></a>
## 4. Activating the Queueing Model Analyzer

The queueing model analyzer is activated by **applying the ConfigMap**
`deploy/configmap-queueing-model.yaml` to the cluster. When this ConfigMap
(`wva-queueing-model-config`) exists and contains a `default` key, the WVA controller uses
the queueing model path.

```bash
kubectl apply -f deploy/configmap-queueing-model.yaml
```

> The ConfigMap is read on every reconcile cycle, so changes take effect within one
> reconcile interval — no controller restart required.

---

<a name="configmap-reference"></a>
## 5. ConfigMap Reference

The ConfigMap has two types of entries:

- **`default`** — global settings applied to all models.
- **Per-model entries** — any other key name is treated as a per-model override; the actual
  model is identified by `model_id` + `namespace` inside the value.

### Full annotated example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: wva-queueing-model-config
  namespace: workload-variant-autoscaler-system
  labels:
    app.kubernetes.io/name: workload-variant-autoscaler
    app.kubernetes.io/managed-by: kustomize
data:
  # ── Global defaults (required) ─────────────────────────────────────────────
  default: |
    # sloMultiplier (k) controls the target server utilisation.
    # The queueing model predicts that mean iteration time at utilisation rho
    # equals alpha/(1-rho). Setting target T_iter = k×alpha yields rho = 1-1/k.
    #
    # Common values:
    #   k=2.0  -> rho=0.50  (conservative; use when latency variance matters)
    #   k=3.0  -> rho=0.67  (default; good balance for most deployments)
    #   k=5.0  -> rho=0.80  (aggressive; maximise throughput, higher tail latency)
    #
    # Constraint: must be > 1.0.
    # Default: 3.0
    sloMultiplier: 3.0

    # tuningEnabled controls whether the Kalman filter runs each cycle.
    # true  (default): learns alpha/beta/gamma from live TTFT/ITL observations.
    # false: skips the tuner; requires explicit targetTTFT/targetITL per model,
    #        or falls back to the observation-based heuristic (see Section 6.3).
    tuningEnabled: true

  # ── Per-model overrides (optional) ─────────────────────────────────────────
  # Key name is arbitrary. model_id + namespace identify the target model.
  # Per-model entries override sloMultiplier and tuningEnabled from the default,
  # and can provide explicit SLO targets.
  #
  # Rules:
  #   - targetTTFT and targetITL must both be set, or both omitted (0 = infer).
  #   - If both are set, the explicit SLOs are used directly; the inferred SLO
  #     path is skipped (but tuning can still be enabled to keep parameters fresh).
  #   - If omitted, SLOs are inferred from learned parameters and sloMultiplier
  #     (or from observations if tuning hasn't converged yet).

  # Example: explicit SLOs for a production Llama model
  llama-prod: |
    model_id: "meta-llama/Meta-Llama-3.1-8B-Instruct"
    namespace: "llm-d-prod"
    targetTTFT: 500.0    # ms — time-to-first-token budget
    targetITL: 50.0      # ms — per-token decode budget
    sloMultiplier: 3.0
    tuningEnabled: true

  # Example: inferred SLOs with a more aggressive utilisation target
  mistral-staging: |
    model_id: "mistralai/Mistral-7B-Instruct-v0.2"
    namespace: "llm-d-staging"
    sloMultiplier: 4.0   # rho=0.75; SLO derived from k and learned parameters
    tuningEnabled: true

  # Example: tuning disabled, explicit SLOs required
  llama-large: |
    model_id: "meta-llama/Meta-Llama-3.1-70B-Instruct"
    namespace: "llm-d-large"
    targetTTFT: 2000.0
    targetITL: 200.0
    tuningEnabled: false
```

### Configuration field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sloMultiplier` | float | `3.0` | Utilisation target multiplier k (must be > 1.0). Target rho = 1 - 1/k. |
| `tuningEnabled` | bool | `true` | Enable online Kalman filter parameter learning. |
| `model_id` | string | — | Model identifier (per-model entries only). |
| `namespace` | string | — | Kubernetes namespace (per-model entries only). |
| `targetTTFT` | float | `0` | Explicit TTFT SLO in milliseconds. `0` = infer automatically. |
| `targetITL` | float | `0` | Explicit ITL SLO in milliseconds. `0` = infer automatically. |

---

<a name="slo-targeting"></a>
## 6. SLO Targeting

The analyzer determines SLO targets for each model through a three-level priority chain:

### 6.1 Explicit SLOs (highest priority)

If `targetTTFT > 0` and `targetITL > 0` are set in the per-model ConfigMap entry, those
values are used directly as the TTFT and ITL budgets. This is the recommended mode for
production deployments where latency requirements are well-defined.

> **Both must be set together, or neither.** Setting only one is a validation error.

### 6.2 Model-Inferred SLOs (preferred automatic mode)

When explicit targets are absent and at least one variant has learned parameters
`(alpha, beta, gamma)`, the SLO is derived from the queueing model using `sloMultiplier` k:

```
TargetTTFT = k×alpha + (beta + gamma) × avg_input_len
TargetITL  = k×alpha + beta + gamma × (avg_input_len + (avg_output_len + 1) / 2)
```

The intuition: the queueing model predicts `T_iter = alpha / (1 - rho)` in steady state.
Setting `T_iter = k×alpha` yields exactly `rho = 1 - 1/k`. The SLO is therefore the
latency that corresponds to the target utilisation, with token-processing work components
added at their true cost (not inflated by k).

When multiple variants serve the same model, the SLO is taken as the **maximum** across
variants — a single SLO per model, since all variants serve the same traffic.

This mode requires tuning to have converged (typically 3–10 reconcile cycles after a variant
first receives traffic).

### 6.3 Observation-Based Fallback (cold start)

If no variant has learned parameters yet (the very first few cycles after deployment, or
after a variant is freshly added with no prior state), the SLO is estimated from observed
latency metrics with a headroom multiplier:

```
TargetTTFT = min(avg_observed_TTFT × 1.5,  10000 ms)
TargetITL  = min(avg_observed_ITL  × 1.5,    500 ms)
```

This is intentionally conservative: the system scales out rather than under-provisioning
while the model is still learning. Once the tuner has learned `(alpha, beta, gamma)`, the
SLO resolution automatically transitions to path 6.2.

> **Summary of SLO resolution order:**
> 1. Explicit `targetTTFT` + `targetITL` from per-model ConfigMap entry.
> 2. Derived from learned parameters using `sloMultiplier` (requires tuning convergence).
> 3. Observed latency × 1.5 headroom (cold start only).

---

<a name="cold-start"></a>
## 7. Initial Parameter Estimation (Cold Start)

When a variant receives traffic for the first time, the `ParameterStore` has no prior
`(alpha, beta, gamma)`. The tuner cannot run without an initial state. The analyzer
bootstraps using an analytic estimate derived from the queueing model equations:

**Step 1: Estimate alpha from observed ITL**

At light-to-moderate load the iteration time is approximately equal to alpha (the idle
baseline). ITL is dominated by T_iter plus minimal decode overhead, so:

```
alpha ≈ BaseFactor × avg_ITL     (BaseFactor = 0.9)
```

**Step 2: Estimate (beta + gamma) from observed TTFT**

From the TTFT equation, substituting T_iter ≈ alpha:

```
(beta + gamma) = (avg_TTFT - alpha) / avg_input_len
```

**Step 3: Separate beta and gamma using ITL**

From the ITL equation, with beta = (beta+gamma) − gamma:

```
gamma = ((avg_ITL - alpha) - (beta+gamma)) / (avg_input_len + (avg_output_len+1)/2 - 1)
beta  = (beta+gamma) - gamma
```

**Validity check:** if any derived value is ≤ 0 (e.g. when TTFT < alpha, or when the
token sequence lengths make the denominator nonpositive), the bootstrap fails. In that case
the tuner falls back to hardcoded defaults (`alpha=5.0 ms, beta=0.05 ms, gamma=0.00005 ms`)
and the EKF converges from there over subsequent cycles.

After a successful bootstrap, the EKF's initial parameter bounds are narrowed to
`[state × 0.01, state × 100]` to allow faster convergence compared to the wider defaults.

> **Consequence:** scaling decisions during the first few reconcile cycles may be less
> accurate than after convergence. The observation-based SLO fallback (Section 6.3) ensures
> the system is not under-provisioned during this warm-up period.

---

<a name="data-flow"></a>
## 8. Data Flow

```
Prometheus / vLLM metrics per pod
  (arrival_rate, avg_TTFT, avg_ITL, avg_input_tokens, avg_output_tokens)
        │
        ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Collector (replica_metrics.go)                         │
  │  Groups metrics by model → variant → pod                │
  └───────────────────────┬─────────────────────────────────┘
                          │ []ReplicaMetrics
                          ▼
  ┌─────────────────────────────────────────────────────────┐
  │  optimizeQueueingModel (engine_queueing_model.go)        │
  │  - reads ConfigMap → builds QMConfig                    │
  │  - calls QueueingModelAnalyzer.Analyze()                │
  └───────────────────────┬─────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          │                               │
          ▼  (if tuningEnabled)           ▼
  ┌───────────────┐               ┌────────────────────────┐
  │  Tuner (EKF)  │               │  SLO Resolution        │
  │               │               │  1. Explicit config    │
  │  Predict      │               │  2. k×alpha model      │
  │  Update obs   │               │  3. 1.5× observed      │
  │  NIS validate │               └──────────┬─────────────┘
  │  Store params │                          │ SLOTarget
  └───────┬───────┘                          │
          │ (alpha, beta, gamma)             │
          └──────────────┬───────────────────┘
                         │
                         ▼
  ┌─────────────────────────────────────────────────────────┐
  │  computeAllVariantCapacities                            │
  │  For each variant:                                      │
  │    qa = QueueAnalyzer(alpha, beta, gamma, maxBatchSize) │
  │    lambda* = qa.Size(SLOTarget)   ← binary search      │
  │    required = ceil(totalArrival / lambda*)              │
  └───────────────────────┬─────────────────────────────────┘
                          │ []VariantCapacity
                          ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Optimizer → VariantDecisions → Enforcer → Apply        │
  └─────────────────────────────────────────────────────────┘
```

---

<a name="key-files"></a>
## 9. Key Files

| Component             | Path                                                             |
|-----------------------|------------------------------------------------------------------|
| QueueingModelAnalyzer | `internal/engines/analyzers/queueingmodel/analyzer.go`           |
| Config types          | `internal/engines/analyzers/queueingmodel/config.go`             |
| Parameter Store       | `internal/engines/analyzers/queueingmodel/parameters.go`         |
| Defaults              | `internal/engines/analyzers/queueingmodel/defaults.go`           |
| Tuner (EKF)           | `internal/engines/analyzers/queueingmodel/tuner/tuner.go`        |
| Tuner defaults        | `internal/engines/analyzers/queueingmodel/tuner/defaults.go`     |
| Tuner configurator    | `internal/engines/analyzers/queueingmodel/tuner/configurator.go` |
| Tuner environment     | `internal/engines/analyzers/queueingmodel/tuner/environment.go`  |
| QueueAnalyzer         | `pkg/analyzer/queueanalyzer.go`                                  |
| Engine integration    | `internal/engines/saturation/engine_queueing_model.go`           |
| ConfigMap interface   | `internal/interfaces/queueing_model_scaling.go`                  |
| ConfigMap YAML        | `deploy/configmap-queueing-model.yaml`                           |

---

<a name="defaults-reference"></a>
## 10. Defaults Reference

| Constant | Value | Meaning |
|----------|-------|---------|
| `DefaultSLOMultiplier` | `3.0` | k=3 → target utilisation rho=0.67 |
| `DefaultMaxBatchSize` | `256` | Max concurrent requests per replica when not parseable from deployment spec |
| `DefaultMaxQueueSize` | `100` | Queue depth limit in the queueing model |
| `DefaultFallbackHeadroom` | `1.5` | Multiplier on observed latency for cold-start SLO |
| `DefaultMaxFallbackTTFT` | `10000 ms` | Cap on fallback TTFT SLO |
| `DefaultMaxFallbackITL` | `500 ms` | Cap on fallback ITL SLO |
| `DefaultMaxNIS` | `7.378` | χ²₂ at 95th percentile — EKF update rejection threshold |
| `DefaultAlpha` | `5.0 ms` | EKF initial guess when bootstrap fails |
| `DefaultBeta` | `0.05 ms/token` | EKF initial guess when bootstrap fails |
| `DefaultGamma` | `0.00005 ms/token` | EKF initial guess when bootstrap fails |
| `BaseFactor` | `0.9` | Fraction of avg ITL used as initial alpha estimate during bootstrap |
| `TransientDelaySeconds` | `120 s` | Grace period before tuning a freshly scaled-up replica |
