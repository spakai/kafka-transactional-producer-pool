# Kafka Chaos Testing Runbook

This runbook covers the disposable three-broker environment used by Specs 004
and 007. Never use these commands against production or a shared Kafka cluster.
Fault injection requires both the chaos opt-in and the exact runtime cluster ID.

## Topology

The Docker Compose stack runs three Kafka brokers and one Toxiproxy container.
Producer traffic to `localhost:19092`, `localhost:29092`, and
`localhost:39092` passes through Toxiproxy. Kafka replication and controller
traffic stays on the internal Docker network and is not affected by producer
network partitions.

## Scenario catalog

The direct chaos harness accepts `MB-01` through `MB-04` and `CH-01` through
`CH-05`. `MB` scenarios establish multi-broker behavior; `CH` scenarios inject
an outage during sustained transactional load.

| ID | Scenario | Injected condition | Required hook |
|---|---|---|---|
| `MB-01` | Three-broker baseline | None | None |
| `MB-02` | Partition/leader distribution | None | None |
| `MB-03` | Non-leader broker loss | Stops a non-leader for the target partition | Docker |
| `MB-04` | Leader broker loss | Stops the target partition's leader | Docker |
| `CH-01` | Leader broker restart mid-soak | Stops a leader-heavy broker, then restarts it | Docker |
| `CH-02` | One-broker network partition | Blocks producer traffic to one leader broker | `partition-broker` and `heal` |
| `CH-03` | Full network partition | Blocks producer traffic to all brokers | `partition-cluster` and `heal` |
| `CH-04` | Flapping network | Six cycles of 10s blocked and 20s healthy | `partition-cluster` and `heal` |
| `CH-05` | Commit-response loss | Allows requests and suppresses downstream responses | `drop-responses` and `heal` |

### MB-01: three-broker transactional baseline

Runs transactional load without a fault. It establishes replicated-cluster
throughput and latency and verifies that acknowledged transactions are visible
exactly once to a `read_committed` consumer. It should complete with no
unexpected ambiguous outcomes, producer leaks, or under-replicated partitions.

### MB-02: partition leader distribution

Runs without a fault while observing traffic across partitions led by different
brokers. It is intended to expose partition-specific starvation, skew, or
ordering problems. The current runner maps `MB-02` to the baseline execution
path, so leader distribution must be confirmed from the cluster snapshot and
result samples rather than by a separate fault action.

### MB-03: non-leader broker loss

Selects a broker that does not lead the configured target partition, stops it
at `--fault-at-sec`, and restarts it after `--fault-duration-sec`. Publishing
should continue while ISR shrinks. The test checks that losing only a replica
does not make the pool unavailable, lose acknowledged transactions, or trigger
unnecessary producer churn. Cleanup waits for the broker and full ISR recovery.

### MB-04: leader broker loss

Selects and stops the configured target partition's leader. Kafka must elect a
replacement while transactional load continues. The test checks bounded
failures, correct ambiguous-outcome handling, recovery without an application
restart, no stuck leases, and no partially visible transactions.

### CH-01: broker restart mid-soak

Selects a broker that leads test-topic partitions, stops it during a sustained
run, restarts it, and observes the post-recovery window. Unlike `MB-04`, broker
selection is based on cluster-wide leadership rather than only the configured
target partition. The scenario checks sustained stability, pool recovery,
throughput restoration, resource reconciliation, and committed-record
correctness across a complete stop/start cycle.

### CH-02: producer-to-one-broker partition

Keeps the selected broker healthy inside Kafka but blocks the producer's
external connection to it in both directions. Other brokers and internal Kafka
traffic remain reachable. This tests whether unaffected work continues,
blocked operations respect their timeout envelope, ambiguous work is not
replayed, and normal throughput returns after healing.

### CH-03: producer-to-cluster partition

Blocks both directions between the producer and all three external listeners
while leaving the Kafka cluster itself healthy. Calls must apply bounded
backpressure or fail within configured deadlines; threads, memory, leases, and
outstanding requests must remain bounded. Publishing must resume without an
application restart after all paths are healed.

### CH-04: short flapping network

Alternates six 10-second full producer partitions with six 20-second healthy
windows. This repeated transition is designed to expose retry storms,
unbounded producer replacement, recovery-supervisor contention, and unstable
pool state. The direct harness requires at least 180 seconds for the cycles plus
a final post-fault recovery window.

### CH-05: transaction response partition

Allows upstream requests to reach Kafka while suppressing downstream responses
during the fault window. It exercises the path where a commit may have happened
but the client cannot prove the outcome. Unknown outcomes must be reported as
ambiguous, the affected producer must not be reused unsafely, and the
transaction callback must never be automatically replayed. The provided
Toxiproxy hook suppresses all downstream Kafka traffic, not only protocol-level
`EndTxn` frames.

## Spec 007 hypothesis families

The agentic runner proposes the same primitives through a typed policy boundary:

| Family | Coverage |
|---|---|
| `AF-01` | Healthy baseline and bounded load variation |
| `AF-02A` | Non-leader replica loss, corresponding to `MB-03` |
| `AF-02B` | Leader loss, corresponding to `MB-04` |
| `AF-03A` | One-broker producer partition, corresponding to `CH-02` |
| `AF-03B` | Full producer partition, corresponding to `CH-03` |
| `AF-04` | Intermittent connectivity, corresponding to `CH-04` |
| `AF-05` | Commit-response ambiguity, corresponding to `CH-05` |
| `AF-06` | One permitted infrastructure fault under bounded high contention |

The planner cannot execute a scenario directly. Its proposal must pass cluster
identity, health, duration, load, telemetry, duplicate, budget, and cleanup
checks before the controller receives a resolved plan. Only one fault primitive
may be active at a time.

## Start and inspect the environment

```bash
./scripts/chaos-cluster.sh up
CLUSTER_ID="$(./scripts/chaos-cluster.sh cluster-id)"
./scripts/chaos-cluster.sh status
```

`up` waits for all services, initializes the three proxies, and creates the
12-partition, replication-factor-3 topic with `min.insync.replicas=2`.

## Fault and healing commands

The planner never constructs commands. It selects a typed action, policy checks
that a cleanup command exists, and the controller invokes one of these bounded
scripts:

```bash
# Block producer traffic to one broker in both directions.
./scripts/chaos-network.sh partition-broker 1

# Block producer traffic to all brokers in both directions.
./scripts/chaos-network.sh partition-cluster

# Allow requests upstream but suppress downstream responses from all brokers.
./scripts/chaos-network.sh drop-responses

# Remove every network fault. Safe to run repeatedly.
./scripts/chaos-network.sh heal

# Show every proxy and its number of active toxics.
./scripts/chaos-network.sh status
```

`drop-responses` is a transport-level approximation of commit-response loss. It
suppresses all downstream Kafka traffic during its short fault window; it does
not parse Kafka protocol frames to target only `EndTxn` responses.

If a run is interrupted or cleanup is uncertain, heal first and verify zero
active toxics:

```bash
./scripts/chaos-network.sh heal
./scripts/chaos-network.sh status
./scripts/chaos-cluster.sh status
```

Each proxy must report `0` toxics, every container must be healthy, and every
topic partition must list all three brokers in ISR before another experiment.

## Agentic network replays

Set the command hooks once for a shell session:

```bash
NETWORK_ARGS="--partition-broker-command './scripts/chaos-network.sh partition-broker {brokerId}' \
--partition-cluster-command './scripts/chaos-network.sh partition-cluster' \
--commit-response-command './scripts/chaos-network.sh drop-responses' \
--heal-network-command './scripts/chaos-network.sh heal'"
```

Run a deterministic partial-partition replay:

```bash
mvn -Pagentic compile exec:java \
  -Dexec.args="--agentic-enabled true --chaos-enabled true \
  --environment-label disposable --cluster-allowlist ${CLUSTER_ID} \
  --campaign-duration-sec 300 --max-experiments 1 \
  --replay specs/007-agentic-producer-fault-injection/replay-partition-broker.json \
  ${NETWORK_ARGS}"
```

Other checked-in replays:

- `replay-partition-cluster.json` blocks the producer from all brokers.
- `replay-drop-responses.json` exercises ambiguous response handling.

For adaptive campaigns, omit `--replay` and allow enough experiments for the
rule-based planner to progress from baseline and broker failures into the
network experiment families.

## Verification and evidence

Every experiment verifies committed visibility, failed-transaction isolation,
partial transactions, duplicates, ordering, safety limits, recovery, and
cleanup. Agentic evidence is written below
`agentic-results/<campaign-id>/`; direct chaos-harness evidence is written below
`chaos-results/`.

The network qualification performed for Spec 007 passed:

- one-broker producer partition;
- full producer-to-cluster partition; and
- downstream response loss.

## Stop the environment

Heal before teardown, then remove the disposable containers and their data:

```bash
./scripts/chaos-network.sh heal
./scripts/chaos-cluster.sh down
```
