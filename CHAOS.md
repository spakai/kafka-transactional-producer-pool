# Kafka chaos testing guide

This guide introduces the disposable Kafka chaos environment, explains how to
run and read a campaign, and records the live results available as of
2026-10-23. It is written for engineers who are new to chaos testing.

> **Safety:** Run these commands only against the local disposable Docker
> cluster. Fault injection requires both `--chaos-enabled true` and the exact
> runtime Kafka cluster ID in `--cluster-allowlist`. Never point this harness
> at production or a shared cluster.

## 1. What this test is doing

The producer continuously writes small transactions. Each transaction has a
run ID, a publish ID, and record indexes in its headers. The harness records
what the producer believes happened, injects a broker or network fault where
requested, then independently reads the bounded Kafka offset range using both
consumer isolation settings. It compares the read-back against its publish
ledger.

The comparison is the key idea: a successful callback alone does not prove
that every expected record is present exactly once, and a broker being healthy
again does not prove that the application recovered correctly.

### Local topology

```mermaid
flowchart LR
    H[Chaos harness and transactional producer]
    P[Toxiproxy external listeners]
    B1[Kafka broker 1]
    B2[Kafka broker 2]
    B3[Kafka broker 3]
    H -->|produce and consume| P
    P --> B1
    P --> B2
    P --> B3
    B1 <-->|replication and controller traffic| B2
    B2 <-->|replication and controller traffic| B3
    B1 <-->|replication and controller traffic| B3
```

Producer connections pass through Toxiproxy. Kafka replication and controller
traffic stays on Docker's internal network, so CH-02/03/04 partition the
producer from brokers while leaving the Kafka cluster itself connected. This
distinction matters when interpreting a result: these runs test client
connectivity and pool recovery, not a Kafka inter-broker network partition.

### Run and verification flow

```mermaid
flowchart TD
    A[Check Docker cluster and exact cluster ID] --> B[Capture starting offsets and cluster snapshot]
    B --> C[Publish transactions and append outcomes to run ledger]
    C --> D{Scenario has a fault?}
    D -->|No| E[Continue workload]
    D -->|Yes| F[Apply one configured broker or network fault]
    F --> G[Observe workload, pool, broker, and fault events]
    G --> H[Heal or restart and confirm recovery]
    E --> I[Stop submissions and drain transactions]
    H --> I
    I --> J[Capture ending offsets; read bounded range]
    J --> K[Compare ledger with read_committed and read_uncommitted]
    K --> L[Write summary, verification, offsets, events, and samples]
```

An experiment is meaningful only if injection, recovery, bounded read-back,
and cleanup all have evidence. A missing event or verifier timeout lowers the
evidence quality even if the producer reported no error.

## 2. Start the disposable environment

From the repository root:

```bash
./scripts/chaos-cluster.sh up
CLUSTER_ID="$(./scripts/chaos-cluster.sh cluster-id)"
./scripts/chaos-cluster.sh status
```

`up` starts three Kafka brokers and Toxiproxy, initializes the three producer
listeners, and creates `chaos-perf-test` and `chaos-audit-test` with 12
partitions, replication factor 3, and `min.insync.replicas=2`. Keep the printed
cluster ID; fault scenarios must use that exact value as a safety allowlist.

For CH-01, prepare its disposable topic with preferred leader counts 6/3/3:

```bash
./scripts/chaos-cluster.sh prepare-ch01
```

The Maven command must name the runner explicitly. For example, a baseline run
is:

```bash
mvn -Pchaos compile exec:java \
  -Dexec.mainClass=com.kafka.producer.chaos.ChaosRunner \
  -Dexec.args="--scenario MB-01 --duration-sec 120 \
  --output-topics chaos-perf-test,chaos-audit-test \
  --results-dir chaos-results"
```

For a fault run, add the opt-in, exact cluster ID, and scenario-specific hook.
For example, a one-broker producer partition:

```bash
mvn -Pchaos compile exec:java \
  -Dexec.mainClass=com.kafka.producer.chaos.ChaosRunner \
  -Dexec.args="--scenario CH-02 --duration-sec 60 --fault-at-sec 20 \
  --fault-duration-sec 20 --chaos-enabled true \
  --cluster-allowlist ${CLUSTER_ID} \
  --partition-broker-command './scripts/chaos-network.sh partition-broker {brokerId}' \
  --heal-network-command './scripts/chaos-network.sh heal' \
  --results-dir chaos-results"
```

## 3. Results from the completed Docker campaigns

These are observed results from the artifacts listed below, not promises about
future runs or performance guarantees. “Committed” is the number of ledger
transactions whose commit callback completed successfully. “Records” is the
number of records found in the verifier's captured offset range. Each
transaction contained 10 records in these runs. `read_uncommitted` can include
records from aborted transactions, so its total is not expected to equal the
`read_committed` total in MB-01.

| Scenario | Parameters and fault | Counted result | Correctness result | Artifact and status |
|---|---|---|---|---|
| MB-01 | 120s; 2 topics; default deliberate abort every 10 publishes | 32,250 ledger outcomes: 29,029 committed, 3,221 aborted; 580,580 records read committed; 645,000 read uncommitted, including 64,420 aborted records | 0 missing, duplicate, partial, or cross-topic mismatches; aborted records absent from `read_committed` and present in `read_uncommitted` | `MB-01-2026-10-23T04-42-06.359142003Z` — **PASS** |
| MB-02 | 30s; 12 partitions round-robin across 2 topics | 8,569 committed; 171,380 records under each isolation level; all 24 topic-partitions sampled | 0 ordering violations | `chaos-results-recheck/MB-02-2026-10-23T04-44-16.741371760Z` — **PASS** |
| CH-01 | 180s; broker 1 restarted; preferred leader distribution 6/3/3 | 46,156 committed; 923,120 records under each isolation level | Restart and healthy recovery confirmed; 0 missing or duplicate records | `CH-01-2026-10-23T05-01-21.160003912Z` — **PASS** |
| MB-03 | 60s; non-leader replica stopped for 20s | 15,403 committed; 154,030 records under each isolation level | Broker restart/recovery confirmed | `MB-03-2026-10-23T06-13-14.625158912Z` — **PASS** |
| MB-04 | 60s; target partition leader stopped for 20s | 16,270 committed; 162,700 records under each isolation level | Broker restart/recovery confirmed | `MB-04-2026-10-23T06-14-27.406718152Z` — **PASS** |
| CH-02 | 60s; producer blocked from one leader broker for 20s | 11,300 outcomes: 11,299 committed, 1 ambiguous; 112,990 read committed and 113,000 read uncommitted | No missing acknowledged records or duplicates; network healing confirmed | `CH-02-2026-10-23T06-15-35.404673402Z` — **PASS** |
| CH-03 | 60s; producer blocked from all brokers for 20s | 8,180 outcomes: 8,172 committed, 8 ambiguous; 81,720 read committed and 81,780 read uncommitted | No missing acknowledged records or duplicates; network healing confirmed | `CH-03-2026-10-23T06-16-43.386937395Z` — **PASS** |
| CH-04 | 215s; 2 workers; six blocked/healed cycles | 30,516 committed; 305,160 records under each isolation level | 6/6 faults healed; 0 missing, duplicate, partial, or cross-topic mismatches; blocked windows measured 9.6–11.6s and five healthy intervals 20.2–21.6s | `CH-04-2026-10-23T06-35-12.631653134Z` — **PASS** |
| CH-05 | 60s; 20s TCP downstream-response-drop approximation | 8,132 outcomes: 8,124 committed, 8 ambiguous; 81,240 read committed and 81,310 read uncommitted | Network healed, but no protocol evidence that an `EndTxn` request reached Kafka and its matching response was dropped | `CH-05-2026-10-23T06-17-54.870349108Z` — **INVALID/INCOMPLETE** |

The artifact directory contains the run ID in each filename. The detailed
assessment, including earlier harness defects and the historical AF-05
interpretation, is in
[`agentic-results/coverage-assessment-2026-10-23.md`](agentic-results/coverage-assessment-2026-10-23.md).
Earlier diagnostic runs are retained in summaries; use the exact artifact IDs
in this table when citing the corrected live results.

## 4. Parameters: what they control and why they are used

The table describes the direct chaos runner's important options. Defaults are
from the current harness; campaign values above are the values used in the
recorded runs. Explicitly set a parameter when the experiment depends on it so
the result can be repeated and interpreted later.

| Parameter | Default / example | Why this parameter matters | What evidence checks it |
|---|---|---|---|
| `--scenario` | Required; e.g. `MB-01`, `CH-04` | Selects one defined fault hypothesis and its required hooks | Run summary and event log identify scenario and outcome |
| `--duration-sec` | 600s default; recorded runs used 30–215s | Gives a known workload window; include time before, during, and after a fault | Start/end timestamps, samples, ledger totals, and ending offsets |
| `--fault-at-sec` | 180s | Schedules fault relative to the run so baseline and fault periods can be compared | Event timestamps show requested/applied fault relative to run |
| `--fault-duration-sec` | 60s | Bounds an outage and allows an explicit recovery interval | Fault/heal events and recovery checks; CH-04 uses its six-cycle schedule |
| `--topic` / `--output-topics` | One topic unless configured; examples use publish and audit topics | Multiple topics let the verifier test whether each transaction's records commit or abort together | Per-publish verification compares expected records across output topics |
| `--partition-count` | 12 | Gives MB-02 a defined topology and routes records across partitions | Per-partition totals; MB-02 requires a committed sample from each configured topic-partition |
| `--records-per-tx` | 10 | Makes transaction completeness countable; changing it changes expected records per publish | Ledger's expected record count compared with observed IDs/indexes |
| `--abort-every` | 10 for MB-01; 0 for other scenarios | Deliberate aborts create a known negative case to verify isolation semantics | Aborted IDs/record counts must be absent in `read_committed`, present in `read_uncommitted` |
| `--chaos-enabled` | `false`; must be `true` for faults | Prevents accidental fault injection when running a baseline or misconfigured command | Config validation refuses fault runs without the opt-in |
| `--cluster-allowlist` | Empty; set to runtime cluster ID | Binds the fault to the disposable cluster the operator inspected | Startup compares connected cluster identity to the allowed ID |
| Fault and heal command hooks | Scenario-specific scripts | Make fault action and cleanup explicit, reviewable, and repeatable | Event log records action; post-run checks require heal/restart and healthy state |
| `--recovery-timeout-sec` | 120s | Gives the application and cluster a finite recovery deadline | Recovery events and pending transaction/lease checks at drain |

Other load controls such as worker count, record size, and lease timeout should
also be recorded when varied. A result is comparable only when its workload
shape and fault timing are understood; a count without its duration and load
does not say how fast the system was operating.

## 5. What is checked, and why the evidence is useful

| Check | Pass meaning | Why it is useful / trust basis | Limit |
|---|---|---|---|
| Missing committed publishes | Every acknowledged publish ID and expected record index is visible exactly once in `read_committed` | Reconciles producer callback outcomes to an independent Kafka read-back | Only covers the run IDs and captured offset interval |
| Duplicate IDs or indexes | No publish ID/record index appears more than once | Detects replay or duplicate output even if total record counts look correct | A simple total count alone would not detect a missing record balanced by a duplicate; use the per-publish CSV |
| Partial transactions | A publish has either all expected records or none in committed view | Checks transaction boundaries rather than only throughput | Depends on configured `records-per-tx` and topic set |
| Cross-topic consistency | Each transaction's expected records agree across configured output topics | Directly checks atomic outcome across the topics in that run | Does not establish atomicity for topics that were not configured |
| Aborted visibility | Known aborted records are hidden in `read_committed` and accounted for in `read_uncommitted` | Tests both Kafka isolation views with a known set of intentional aborts | Requires the intentional abort scenario and complete scan of the captured range |
| Ordering | Sequence order is checked by topic, partition, and key | Kafka guarantees order within a partition, not globally across partitions/topics | Not a global ordering guarantee; business keys must route consistently |
| Fault and recovery | Fault application, healing/restart, and recovery are recorded within deadlines | Distinguishes a real injected experiment from a workload that merely ran near a fault command | Docker health and proxy events prove environment state, not every client-level effect |
| Ambiguous outcomes | Unknown commit outcomes are reported as ambiguous and not counted as acknowledged success | Avoids falsely claiming success or retrying a possibly committed transaction | Ambiguity by itself does not prove a specific protocol response was lost; this is why CH-05 is incomplete |
| Cleanup | No active toxics; brokers healthy; full ISR restored before next run | Ensures one campaign does not contaminate the next | Verify this explicitly after interruption or timeout |

### Why the measurements are reasonably trustworthy

The verifier uses more than a producer's success counter. It correlates
run-scoped publish IDs and record indexes from the producer ledger with records
read back from Kafka. It captures starting and ending offsets and limits the
scan to that range, then writes per-publish and per-partition detail. Event
records separately show fault request/application, healing, and verification.
Together these let a reviewer reproduce the count and inspect individual
exceptions.

Trust is bounded: these are finite runs on one local Docker topology, under
the recorded workload and timing. They do not prove behavior for every broker
version, production load, or untested configuration. In particular, CH-05 is
not valid protocol-level commit-response-loss evidence. `drop-responses`
suppresses downstream Kafka traffic broadly; it does not identify an `EndTxn`
frame or prove its corresponding response was lost. A valid CH-05 needs a
protocol-aware proxy and broker transaction-state corroboration.

## 6. Inspecting a run

The direct runner writes files under `chaos-results/` (MB-02's corrected run
is under `chaos-results-recheck/`). Start with the summary, then inspect the
per-publish and event evidence for the run ID:

```bash
cat chaos-results/summary.md
ls -1 chaos-results/MB-01-2026-10-23T04-42-06.359142003Z*
head -n 5 chaos-results/MB-01-2026-10-23T04-42-06.359142003Z-verification.csv
cat chaos-results/MB-01-2026-10-23T04-42-06.359142003Z-events.jsonl
```

Artifact meanings:

| File | What it contains |
|---|---|
| `summary.md` | Numeric totals and overall verification status |
| `*-verification.csv` | Per-publish ledger outcome, expected records, observed records under each isolation level, callback attempts/errors |
| `*-offsets.csv` | Start/end offsets defining the verifier's scan boundaries |
| `*-partition-counts.csv` | Read-committed record counts by topic and partition |
| `*-events.jsonl` | Cluster snapshots, fault request/application, heal, cleanup, and verification events |
| `*.csv` time-series | Per-second throughput, outcomes, latency, pool/lease/producer, memory, and broker health samples |

For example, the number `64,420` in MB-01 is the total expected aborted data
records observed under `read_uncommitted`; it is 3,221 aborted transactions
multiplied by 20 records per transaction across two output topics. Those same
aborted records must contribute zero to the committed view. When reporting a
number, say which artifact field and which view it came from.

## 7. Scenario catalog

| ID | Scenario | Fault or purpose | Required hook |
|---|---|---|---|
| `MB-01` | Three-broker baseline | Transaction correctness and isolation baseline | None |
| `MB-02` | Partition/leader distribution | Partition routing and per-partition sampling | None |
| `MB-03` | Non-leader broker loss | Stop a non-leader replica for the target partition | Docker |
| `MB-04` | Leader broker loss | Stop the target partition's leader | Docker |
| `CH-01` | Leader-heavy broker restart | Restart a broker with more leaders during sustained load | Docker |
| `CH-02` | One-broker network partition | Block producer traffic to one broker | `partition-broker` and `heal` |
| `CH-03` | Full network partition | Block producer traffic to all brokers | `partition-cluster` and `heal` |
| `CH-04` | Flapping network | Six cycles of 10s blocked and 20s healthy | `partition-cluster` and `heal` |
| `CH-05` | Commit-response loss | Intended protocol-level response ambiguity; current hook is only an approximation | `drop-responses` and `heal` |

MB means multi-broker baseline/failure coverage; CH means chaos fault scenarios.
The `AF-*` agentic hypothesis families propose these same fault primitives
through an additional policy boundary. The planner does not execute directly:
cluster identity, health, duration, load, telemetry, duplicate, budget, and
cleanup checks must accept its typed plan first.

## 8. Fault and cleanup commands

```bash
# Block producer traffic to one broker in both directions.
./scripts/chaos-network.sh partition-broker 1

# Block producer traffic to all brokers in both directions.
./scripts/chaos-network.sh partition-cluster

# Allow requests upstream but suppress downstream responses from all brokers.
./scripts/chaos-network.sh drop-responses

# Remove every network fault; safe to repeat.
./scripts/chaos-network.sh heal

# Show each proxy and its number of active toxics.
./scripts/chaos-network.sh status
```

After an interrupted run or uncertain cleanup, heal and check the environment
before continuing:

```bash
./scripts/chaos-network.sh heal
./scripts/chaos-network.sh status
./scripts/chaos-cluster.sh status
```

Each proxy should report zero active toxics, every container should be healthy,
and all topic partitions should have all three brokers in ISR. Stop the
environment after experiments:

```bash
./scripts/chaos-network.sh heal
./scripts/chaos-cluster.sh down
```
