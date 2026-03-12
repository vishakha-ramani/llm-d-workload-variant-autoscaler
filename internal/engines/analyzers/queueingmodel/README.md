# QueueingModelAnalyzer

**Location:** `internal/engines/analyzers/queueingmodel/analyzer.go`

The QueueingModelAnalyzer is a model-based capacity analyzer that uses **queueing theory** to determine how many requests each model variant can handle while meeting latency SLOs. It has two core dependencies:

- **Tuner** — an Extended Kalman Filter that learns service parameters from observed metrics
- **QueueAnalyzer** — a Markovian state-dependent queueing model that maps parameters to performance predictions

## Connection to the Saturation Engine

The saturation engine (`internal/engines/saturation/engine.go`) holds a single `queueingModelAnalyzer` instance. When `optimizeQueueingModel()` runs (`engine_queueing_model.go`), it follows a three-stage pipeline:

1. **Collect** metrics and variant states per model
2. **Analyze** via the QueueingModelAnalyzer — produces `AnalyzerResult` with capacity signals (total supply, demand, utilization, per-variant capacities)
3. **Optimize** — the optimizer turns capacity signals into scaling decisions

## The Tuner (Parameter Estimator)

**Location:** `internal/engines/analyzers/queueingmodel/tuner/tuner.go`

The Tuner uses an **Extended Kalman Filter** to learn three service parameters online from observed metrics:

| Parameter | Meaning                                    |
|-----------|--------------------------------------------|
| **Alpha** | Baseline iteration overhead (ms)           |
| **Beta**  | Compute time per token (ms)                |
| **Gamma** | KV-cache memory access time per token (ms) |

### How It Works Per Cycle

1. Retrieves stored parameters + covariance from the `ParameterStore` (or guesses initial state from metrics if first time)
2. **Predict step** — propagates the state estimate forward
3. **Update step** — incorporates new observations `[AvgTTFT, AvgITL]` using an observation function that internally calls `QueueAnalyzer.Analyze()` to map `(alpha, beta, gamma)` to predicted `(TTFT, ITL)`
4. **NIS Validation** — if the Normalized Innovation Squared exceeds 7.378 (chi-squared threshold), the update is rejected and previous state is restored
5. Results (alpha, beta, gamma, covariance) are stored back for the next cycle

## The QueueAnalyzer (Queueing Model)

**Location:** `pkg/analyzer/queueanalyzer.go`

The QueueAnalyzer implements an **Markovian state-dependent queueing model** that maps service parameters to performance predictions.

### Service Time Model

``` text
IterationTime(n) = Alpha + n * (Beta * tokensCompute + Gamma * tokensMemory)
```

where `n` is the batch size.

### Key Methods

- **`Analyze(lambda)`** — given an arrival rate, computes expected TTFT, ITL, and throughput using the queueing model
- **`Size(targetPerf)`** — binary searches for the **maximum request rate** that still meets TTFT and ITL targets

## Data Flow

``` text
Observed Metrics (arrival rate, TTFT, ITL, token sizes)
        |
        v
    +----------+    Extended Kalman Filter    +---------------+
    |  Tuner   | <---- observation func ----> | QueueAnalyzer |
    +----+-----+   h(a,b,g) -> [TTFT, ITL]    +---------------+
         |
         v
   Learned Parameters (alpha, beta, gamma)
         |
         v
    +---------------+   binary search    +---------------+
    | Capacity Calc | -----------------> | QueueAnalyzer |
    +-------+-------+  qa.Size(SLO)      +---------------+
            |
            v
   maxRequestRate per replica
            |
            v
   AnalyzerResult {supply, demand, utilization, per-variant capacities}
            |
            v
   Optimizer -> Scaling Decisions
```

The QueueAnalyzer serves **dual roles**:

1. **Inside the Tuner's observation function** — predicts what TTFT/ITL the current parameter estimates would produce, enabling Kalman filter updates
2. **After tuning** — finds the maximum throughput achievable under SLO constraints via binary search

The Tuner ensures the queueing model stays calibrated to reality through continuous online learning.

## Key Files

| Component             | Path                                                             |
|-----------------------|------------------------------------------------------------------|
| QueueingModelAnalyzer | `internal/engines/analyzers/queueingmodel/analyzer.go`           |
| Config                | `internal/engines/analyzers/queueingmodel/config.go`             |
| Parameter Store       | `internal/engines/analyzers/queueingmodel/parameters.go`         |
| Defaults              | `internal/engines/analyzers/queueingmodel/defaults.go`           |
| Tuner                 | `internal/engines/analyzers/queueingmodel/tuner/tuner.go`        |
| Tuner Configurator    | `internal/engines/analyzers/queueingmodel/tuner/configurator.go` |
| Tuner Environment     | `internal/engines/analyzers/queueingmodel/tuner/environment.go`  |
| QueueAnalyzer         | `pkg/analyzer/queueanalyzer.go`                                  |
| Engine Integration    | `internal/engines/saturation/engine_queueing_model.go`           |
| Interfaces            | `internal/interfaces/analyzer.go`                                |
