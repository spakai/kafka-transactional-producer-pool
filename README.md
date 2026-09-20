# Kafka Transactional Producer Pool

A bounded, thread-safe pool of Apache Kafka transactional producers for Java applications that need atomic batch publishing without sharing a `KafkaProducer` between concurrent transactions.

The pool owns producer creation, exclusive leasing, transaction lifecycle, bounded retries, fatal-producer eviction and asynchronous replacement, graceful shutdown, health reporting, and Micrometer metrics.

> This project is currently a source-built `1.0.0-SNAPSHOT`; it is not published to a Maven repository.

## Features

- One unique `transactional.id` per pool slot
- Preferred callback API that begins, commits or aborts, and releases automatically
- Bounded lease waits and hard lease deadlines
- Retriable, abort-required, and fatal Kafka error classification
- Automatic replacement after fencing or another fatal producer error
- `HEALTHY`, `DEGRADED`, and `UNAVAILABLE` health states
- Micrometer gauges, counters, and timers
- Graceful drain with a configurable shutdown deadline
- Unit tests and eight broker/JMH performance scenarios

## Requirements

- JDK 17 or newer
- Maven 3.8 or newer
- Apache Kafka reachable through `bootstrap.servers`
- Docker only if you want to run the local broker or performance scenarios

The client dependency is Apache Kafka `3.6.1`. The recorded performance run used JDK 21 and a single Kafka 3.7.0 broker in KRaft mode.

## Build and test

```bash
git clone https://github.com/spakai/kafka-transactional-producer-pool.git
cd kafka-transactional-producer-pool
mvn clean test
```

The resulting snapshot JAR is written to `target/kafka-pooled-transactional-producer-1.0.0-SNAPSHOT.jar`.

## Quick start

Configure the underlying Kafka producers, then create and initialize the pool:

```java
import com.kafka.producer.pool.ExecutionOptions;
import com.kafka.producer.pool.PoolConfig;
import com.kafka.producer.pool.TransactionalProducerPool;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

Properties kafka = new Properties();
kafka.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
kafka.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

PoolConfig config = PoolConfig.builder()
        .poolSize(4)
        .minHealthyProducers(2)
        .leaseTimeoutMs(5_000)
        .leaseHardTimeoutMs(30_000)
        .shutdownGracePeriodMs(10_000)
        .retryMaxAttempts(3)
        .serviceIdentity("orders-api")
        .instanceIdentifier(System.getenv().getOrDefault("HOSTNAME", "local"))
        .kafkaProperties(kafka)
        .build();

TransactionalProducerPool pool = TransactionalProducerPool.create(config);

try {
    pool.initialize();

    String orderId = "order-123";
    pool.executeInTransaction(lease -> {
        lease.send(new ProducerRecord<>(
                "orders",
                orderId.getBytes(StandardCharsets.UTF_8),
                "{\"status\":\"created\"}".getBytes(StandardCharsets.UTF_8)));
        return null;
    }, ExecutionOptions.builder()
            .correlationId(orderId)
            .build());
} finally {
    pool.shutdown();
}
```

`transactional.id`, `enable.idempotence=true`, and `acks=all` are set by the pool. Byte-array serializers are supplied unless you override them.

### Transactional ID safety

Each slot uses this format:

```text
<serviceIdentity>-<instanceIdentifier>-<slotIndex>
```

`serviceIdentity` and `instanceIdentifier` must make the prefix globally unique for every simultaneously running application instance. Reusing it in two live instances allows one instance to fence the other's producers.

### Retry contract

The callback may run more than once after a retriable failure. Keep it deterministic and free of non-Kafka side effects, or make those effects independently idempotent. A failed or ambiguous commit is surfaced to the caller; exactly-once processing also requires consumers to use `isolation.level=read_committed` and any consumed offsets to participate in the same transaction.

## Configuration

`PoolConfig.builder()` provides these pool-level settings:

| Setting | Default | Meaning |
|---|---:|---|
| `poolSize` | `5` | Number of transactional producer slots |
| `minHealthyProducers` | `1` | Minimum initialized producers required at startup |
| `leaseTimeoutMs` | `5000` | Maximum default wait for an available producer |
| `leaseHardTimeoutMs` | `30000` | Deadline for using an acquired lease |
| `shutdownGracePeriodMs` | `10000` | Time allowed for active leases to finish |
| `retryMaxAttempts` | `3` | Retries after the initial transaction attempt |
| `retryBaseDelayMs` | `100` | Initial exponential-backoff delay |
| `retryMaxDelayMs` | `5000` | Backoff ceiling |
| `recoveryMaxConcurrentRebuilds` | `1` | Concurrent producer replacement limit |
| `serviceIdentity` | required | Stable application/service name |
| `instanceIdentifier` | required | Unique identity of this running instance |
| `kafkaProperties` | empty | Properties passed to each `KafkaProducer` |

`ExecutionOptions` can override the lease timeout and retry count for one transaction and can attach a correlation ID.

For production metrics, construct the pool with your application's registry instead of the convenience factory:

```java
TransactionalProducerPool pool = new TransactionalProducerPool(
        config,
        new DefaultKafkaProducerFactory(),
        meterRegistry);
```

## Minimum and maximum pool sizing

Use separate minimum and maximum pool sizes so startup safety and operating
capacity are not treated as the same thing.

- `minPoolSize` is the minimum number of initialized producers required for the
  pod to be considered capable of serving traffic.
- `maxPoolSize` is the desired/maximum number of producer slots the pool should
  maintain during normal operation.

For example:

```text
minPoolSize = 3
maxPoolSize = 10
```

Startup behavior should be:

```text
Pod starts
   |
   v
Create producers up to maxPoolSize
   |
   +--> ready == maxPoolSize
   |        HEALTHY
   |        pod starts normally
   |
   +--> minPoolSize <= ready < maxPoolSize
   |        DEGRADED
   |        pod stays up
   |        readiness = READY
   |        alarm/metric raised
   |        background recovery continues
   |
   +--> ready < minPoolSize
            UNAVAILABLE
            startup fails
            pod exits
            alarm raised
```

With `minPoolSize=3` and `maxPoolSize=10`:

```text
10/10 -> HEALTHY
 7/10 -> DEGRADED, continue
 3/10 -> DEGRADED, continue
 2/10 -> startup failure
 0/10 -> startup failure
```

Runtime behavior should be different from startup behavior. A temporary Kafka
outage should not immediately cause every running application pod to terminate.

```text
ready >= maxPoolSize
    HEALTHY

minPoolSize <= ready < maxPoolSize
    DEGRADED
    continue serving
    rebuild missing producer slots
    raise warning/alarm

ready < minPoolSize
    UNAVAILABLE
    readiness = false
    stop accepting new work
    keep recovery running
    raise critical alarm
```

If desired, a pod may be terminated only after the pool remains below
`minPoolSize` for a configurable recovery window, for example:

```text
minPoolSize = 3
recoveryFailureThreshold = 5
unavailableGracePeriod = 60s
```

This avoids a failure mode where a short Kafka outage causes every application
pod to restart at the same time and increases recovery pressure on Kafka.

### Kubernetes health semantics

Liveness and readiness must not mean the same thing.

```text
Liveness
--------
Is the application process itself functioning?

Temporary Kafka outage:
liveness = UP

Readiness
---------
Can this pod safely process new work?

ready producers >= minPoolSize:
READY

ready producers < minPoolSize:
NOT READY
```

A Kafka outage or producer shortage should normally fail readiness first, not
liveness. The pod should remain alive long enough to attempt producer recovery.

The pool health model should follow the same contract:

```java
enum PoolHealth {
    HEALTHY,       // ready >= maxPoolSize
    DEGRADED,      // minPoolSize <= ready < maxPoolSize
    UNAVAILABLE    // ready < minPoolSize
}
```

Recommended metrics include:

```text
producer_pool_ready
producer_pool_min_size
producer_pool_max_size
producer_pool_recovery_failures_total
producer_pool_slot_rebuild_total
producer_pool_startup_failures_total
```

Recommended alarms:

```text
WARN
ready < maxPoolSize for a sustained interval

CRITICAL
ready < minPoolSize

CRITICAL
pool remains UNAVAILABLE beyond the recovery grace period

CRITICAL
producer rebuilds repeatedly fail
```

Recommended logging:

```text
INFO
Producer pool initialized
ready=10 min=3 max=10 health=HEALTHY

WARN
Producer pool started with reduced capacity
ready=6 min=3 max=10 health=DEGRADED
action=background_recovery

ERROR
Producer pool failed minimum startup requirement
ready=2 min=3 max=10
action=startup_failed

WARN
Producer pool capacity degraded
previousReady=10 ready=7 min=3 max=10
action=rebuild_slots

ERROR
Producer pool unavailable
ready=2 min=3 max=10
action=readiness_failed,recovery_continuing
```

The operational rule is therefore:

> At startup, fail the pod if `minPoolSize` cannot be created. At runtime, if
> capacity falls below `minPoolSize`, first remove the pod from service, alarm,
> and continue bounded recovery rather than immediately killing the pod.

## Health and metrics

Use `pool.getHealth()` for the coarse health state, or inspect `getPoolState()`, `getReadyCount()`, `getLeasedCount()`, and `getTotalCount()` for diagnostics.

The supplied `MeterRegistry` receives:

- `pool_size_total`, `pool_size_ready`, `pool_size_leased`
- `lease_wait_ms`, `lease_timeout_total`
- `transaction_begin_total`, `transaction_commit_total`, `transaction_abort_total`
- `transaction_outcome_total` tagged by `outcome`
- `transaction_duration_ms`
- `pool_health` tagged by `state`
- `producer_fenced_total`, `producer_recovery_total` tagged by `outcome`
- `publish_retry_total` tagged by `error_class`

## Prometheus and Grafana

Spec 006 provides a local Prometheus and Grafana stack with a live Kafka workload:

```bash
docker compose -f observability/compose.yaml up --build
```

After the services become healthy:

- Grafana dashboard: [http://localhost:3000/d/producer-pool](http://localhost:3000/d/producer-pool)
- Prometheus targets: [http://localhost:9090/targets](http://localhost:9090/targets)
- Raw demo metrics: [http://localhost:9404/metrics](http://localhost:9404/metrics)

Grafana is provisioned automatically with anonymous viewer access for this disposable local environment. Do not copy that authentication setting to production.

To run only the demo against an existing Kafka broker:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
mvn -Pobservability compile exec:java
```

Stop the stack while retaining local dashboard data:

```bash
docker compose -f observability/compose.yaml down
```

Stop it and remove the disposable Prometheus and Grafana volumes:

```bash
docker compose -f observability/compose.yaml down --volumes
```

Alert investigation guidance is in [the producer-pool runbook](observability/runbooks/producer-pool-alerts.md). Production deployments must protect `/metrics`, configure real alert thresholds and Grafana authentication, and provide their own persistence and retention policy.

## Local Kafka broker

Start the same single-node KRaft image used for the recorded performance run:

```bash
docker run -d --name kafka-pool-broker -p 9092:9092 apache/kafka:3.7.0
```

Create the default performance topic after the broker is ready:

```bash
docker exec kafka-pool-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic perf-test --partitions 12 --replication-factor 1
```

This setup is intended for local development and benchmarks, not production.

## Performance harness

Scenarios live under `src/perf/java/com/kafka/producer/perf`:

| Scenario | Measurement |
|---|---|
| S-01 | Non-transactional baseline |
| S-02 | One record per transaction |
| S-03 | Batch transaction throughput |
| S-04 | Throughput by pool size |
| S-05 | Lease contention and saturation |
| S-06 | Sustained-load soak |
| S-07 | Fatal producer recovery latency |
| S-08 | JMH transaction hot path |

With the broker and `perf-test` topic running:

```bash
mvn test -Pperf -Dscenario=S-04 -DpoolSize=8
```

CSV output is written to `perf-results/`. See [`perf-results/summary.md`](perf-results/summary.md) for the checked-in environment, results, and acceptance criteria. Those figures are observations from one localhost, single-broker run—not general capacity guarantees.

## Design notes and limitations

- A producer lease is exclusive; callers block only up to the configured acquisition timeout.
- `executeInTransaction` is safer than manually calling `acquireLease`, `beginTransaction`, `commit`/`abort`, and `release` because cleanup is centralized.
- The public record API uses `ProducerRecord<byte[], byte[]>`; serialization is the caller's responsibility.
- Recovery is asynchronous, so the pool can operate at reduced capacity while a slot is rebuilt.
- Runtime configuration reload and consumer offset transaction APIs are not currently provided.
- Unit tests use mocked producers. Run the performance harness against Kafka for broker-level validation.

## Multi-broker chaos harness

Spec 004 is implemented as an opt-in Maven profile and a disposable three-broker
Kafka environment. See [CHAOS.md](CHAOS.md) for the complete operational
runbook, network fault commands, emergency healing, deterministic replays, and
teardown. Start the cluster and create the replicated test topic:

```bash
./scripts/chaos-cluster.sh up
./scripts/chaos-cluster.sh cluster-id
```

Compile and run the non-destructive three-broker baseline:

```bash
mvn test -Pchaos
mvn -Pchaos exec:java \
  -Dexec.mainClass=com.kafka.producer.chaos.ChaosRunner \
  -Dexec.args="--scenario MB-01 --duration-sec 60"
```

Fault scenarios require both an explicit opt-in and the exact cluster ID:

```bash
mvn -Pchaos exec:java \
  -Dexec.mainClass=com.kafka.producer.chaos.ChaosRunner \
  -Dexec.args="--scenario CH-01 --duration-sec 180 --fault-at-sec 60 \
  --fault-duration-sec 30 --chaos-enabled true --cluster-allowlist <cluster-id>"
```

Broker stop/start faults are performed through Docker. Network scenarios require
explicit injection and cleanup commands so the same harness can use a proxy,
`iptables`, or `tc` without embedding environment-specific privileged commands:

```text
--partition-broker-command "<inject command; {brokerId} and {container} are substituted>"
--partition-cluster-command "<inject command>"
--commit-response-command "<proxy command that drops commit responses>"
--heal-network-command "<cleanup command>"
```

Only run chaos scenarios against disposable infrastructure. Per-second samples,
fault events, and publish-ID correctness results are written to `chaos-results/`.
Stop and remove the local cluster with `./scripts/chaos-cluster.sh down`.

### Transaction failure-injection test plan

The performance harness and the chaos harness answer different questions. **JMH is for
micro-benchmarking; it is not the primary tool for reproducing Kafka transaction
failures.** Transaction coordinator outages, fencing, producer epoch changes,
ambiguous commits, leader failover, packet loss, and delayed responses are
distributed-systems correctness scenarios and should be tested with deterministic
integration tests plus controlled fault injection.

Use the following layers:

| Layer | Recommended tool | Purpose |
|---|---|---|
| Deterministic correctness | JUnit + a real Kafka broker/Testcontainers | Transaction timeout, duplicate `transactional.id`, producer fencing, invalid timeout configuration, retry/eviction behavior |
| Network fault injection | Toxiproxy through `scripts/chaos-network.sh` | Delay/drop requests or responses, isolate one broker, isolate the whole cluster, reproduce ambiguous commit outcomes |
| Infrastructure chaos | Three-broker chaos environment / Kubernetes chaos tooling | Broker crash, coordinator failover, leader election, pod restart, network partition |
| Performance | JMH and the `perf` profile | Pool overhead, lease contention, transaction hot path, producer-pool sizing and throughput |

#### Failure scenarios to cover

| ID | Failure to reproduce | How to trigger it | Expected behavior |
|---|---|---|---|
| TX-01 | `initTransactions()` timeout | Block or delay access to the transaction coordinator and use a bounded `max.block.ms` | Initialization fails within the configured deadline; the slot is not exposed as ready |
| TX-02 | Transaction coordinator unavailable | Stop/restart the broker currently acting as transaction coordinator | Producer sees a retriable coordinator error or timeout; pool follows bounded retry/recovery policy |
| TX-03 | Coordinator failover during transaction | Start a transaction, publish records, then restart the coordinator broker | No partial committed transaction is visible to `read_committed` consumers |
| TX-04 | Producer fenced by a newer epoch | Start Producer A with `transactional.id=X`, then initialize Producer B with the same ID | A becomes stale/fenced and must be treated as fatal, evicted, and never reused |
| TX-05 | Stale producer attempts commit | Fence Producer A as in TX-04, then call `send()` or `commitTransaction()` from A | Fatal fencing/epoch error; do not blindly retry using the stale producer |
| TX-06 | Transaction timeout | `beginTransaction()`, publish, wait longer than `transaction.timeout.ms`, then commit | Transaction is aborted/fails and no partial data becomes visible |
| TX-07 | Invalid transaction timeout configuration | Set producer `transaction.timeout.ms` above broker `transaction.max.timeout.ms` | Initialization fails deterministically with configuration/timeout validation error |
| TX-08 | Produce request timeout | Delay/drop produce traffic or isolate the partition leader during a transaction | Retriable/fatal classification matches Kafka semantics; pool never reports an unverified success |
| TX-09 | Leader failure during large transaction | Publish a large transaction and stop the leader broker mid-send | Kafka elects a replacement; transaction either completes safely or fails without partial committed visibility |
| TX-10 | Commit request does not reach coordinator | Partition the producer from the cluster immediately before `commitTransaction()` | Commit fails/times out; caller receives failure and no success is inferred |
| TX-11 | Commit succeeds but response is lost | Allow the commit request to reach Kafka, then drop downstream broker responses | Producer observes an **ambiguous commit outcome**; application must not blindly replay the same logical transaction |
| TX-12 | Metadata unavailable | Block all brokers before or during metadata refresh | Calls fail within configured bounds; pool does not hang indefinitely |
| TX-13 | Producer buffer exhaustion | Use small `buffer.memory`, slow broker/network, and high publish concurrency | Backpressure/timeout is surfaced; pool health and lease metrics remain consistent |
| TX-14 | Broker throttling / high latency | Apply broker quota or network latency | Throughput degrades predictably without transactional correctness violations |

The most important epoch/fencing test is intentionally simple:

```text
Producer A
transactional.id = rerating-job-123
initTransactions()
beginTransaction()
send(...)

        |
        v

Producer B starts
transactional.id = rerating-job-123
initTransactions()

        |
        v

Kafka assigns B the newer producer epoch.
Producer A is now stale/fenced.

        |
        v

Producer A calls send() or commitTransaction().

Expected:
- fatal fencing/epoch failure;
- Producer A is evicted;
- Producer A is never returned to the pool;
- no automatic replay is attempted using the stale producer.
```

This also models a pod-restart race: Pod A becomes slow or disconnected, Pod B
starts with the same transactional identity, and Pod A later resumes. Kafka's
epoch/fencing mechanism must prevent the stale producer from committing.

#### Ambiguous commit test

A normal timeout test is not enough for `commitTransaction()`. Exercise two
different cases:

1. **Request lost before Kafka commits.** Block the producer before the commit
   request reaches the transaction coordinator.
2. **Kafka commits, response is lost.** Permit producer-to-Kafka traffic but drop
   Kafka-to-producer responses immediately after the commit request is sent.

Both can look like a timeout to the application, but only the second case may
already be durably committed. The pool must surface that uncertainty rather than
claim success or blindly replay the transaction.

For the existing Toxiproxy-backed environment, use the bounded network commands
documented above. The `drop-responses` toxic is the primary primitive for the
second case; verify the final result with a `read_committed` consumer and a
logical publish ID/correlation ID ledger.

#### Suggested execution order

Run transaction-failure coverage in this order:

1. Deterministic fencing, transaction-timeout, and invalid-configuration tests.
2. Single-broker network faults: initialization timeout, send timeout, metadata
   loss, and full producer isolation.
3. Three-broker leader and coordinator restart/failover scenarios.
4. Ambiguous commit tests with request-loss and response-loss separated.
5. Long-running soak tests that combine load with bounded broker/network faults.
6. JMH/performance runs separately, after correctness passes, to measure the cost
   of the recovery and pooling behavior rather than to create failures.

Every correctness scenario should verify, where applicable:

- only complete committed transactions are visible with
  `isolation.level=read_committed`;
- aborted/failed transactions are not visible;
- no duplicate logical publish IDs are introduced by pool retries;
- fenced/fatal producers are evicted and replaced;
- retries are bounded;
- leases are always released;
- health and Micrometer counters reflect the failure and recovery path; and
- ambiguous commit outcomes are surfaced explicitly instead of being converted
  into a false success.

### What the chaos tests prove

The validated broker-restart run demonstrated that the producer pool can survive
loss of a Kafka leader broker without an application restart or transactional
data corruption. Kafka elected replacement leaders while the pool handled
transient failures using its existing retry and error-classification policy.
Safe retriable operations were retried, while unsafe producers were evicted and
rebuilt asynchronously.

The `read_committed` verifier confirmed that:

- committed transactions were complete;
- aborted transactions were not visible;
- no partial transactions or duplicate publish IDs were observed; and
- per-key ordering checks passed.

These tests do not guarantee zero failed publish attempts during an outage,
cross-cluster failover, production-scale capacity, or automatic resolution of
ambiguous commits. Network-partition scenarios additionally require an
environment-specific proxy or firewall fault command.

## Agentic fault-injection campaigns

Spec 007 adds a deterministic, state-aware campaign runner over the existing
chaos primitives. It uses an in-process rule-based planner; it does not require
an external AI service or third-party chaos product. Proposed experiments pass
through a typed policy engine before the controller can execute them, and
correctness results are decided by the existing publish-ledger verifier.

Compile and test the isolated profile:

```bash
mvn -Pagentic test
mvn -Pagentic compile exec:java -Dexec.args="--dry-run true"
```

A real campaign requires both opt-ins, a disposable environment label, and the
exact ID of the local three-broker cluster:

```bash
CLUSTER_ID="$(./scripts/chaos-cluster.sh cluster-id)"

mvn -Pagentic compile exec:java \
  -Dexec.args="--agentic-enabled true --chaos-enabled true \
  --environment-label disposable --cluster-allowlist ${CLUSTER_ID} \
  --campaign-duration-sec 600 --max-experiments 3"
```

Without network commands, the policy permits only baseline and broker
stop/start experiments. Network and commit-response proposals are rejected
unless the corresponding injection command and `--heal-network-command` are
provided. The runner never generates or executes arbitrary commands.

The Docker chaos stack includes Toxiproxy and initializes one proxy per Kafka
external listener. Enable the Spec 007 network scenarios with these bounded
repository commands:

```bash
NETWORK_ARGS="--partition-broker-command './scripts/chaos-network.sh partition-broker {brokerId}' \
--partition-cluster-command './scripts/chaos-network.sh partition-cluster' \
--commit-response-command './scripts/chaos-network.sh drop-responses' \
--heal-network-command './scripts/chaos-network.sh heal'"
```

`partition-broker` blocks both directions for one external broker listener;
`partition-cluster` blocks all producer-to-cluster traffic; and
`drop-responses` blocks downstream traffic after requests can reach Kafka.
Healing removes every fault toxic idempotently. Inspect the active proxy rules
with `./scripts/chaos-network.sh status`.

Deterministic replay fixtures for the partial and full network partitions are
stored with Spec 007 and still require all authorization flags:

```bash
mvn -Pagentic compile exec:java \
  -Dexec.args="--agentic-enabled true --chaos-enabled true \
  --environment-label disposable --cluster-allowlist ${CLUSTER_ID} \
  --replay specs/007-agentic-producer-fault-injection/replay-partition-broker.json \
  ${NETWORK_ARGS}"
```

Campaign evidence is stored below `agentic-results/<campaign-id>/`, including
events, per-experiment samples, deterministic results, and `replay.json`. Replay
still requires the full authorization and policy checks:

```bash
mvn -Pagentic compile exec:java \
  -Dexec.args="--agentic-enabled true --chaos-enabled true \
  --environment-label disposable --cluster-allowlist ${CLUSTER_ID} \
  --replay agentic-results/<campaign-id>/experiments/exp-1/replay.json"
```

## Spring Kafka transactional baseline

Spec 005 adds an opt-in `spring-baseline` profile; Spring Kafka is not a
dependency of the default library artifact. The harness normalizes pool and
Spring transaction workloads and writes raw latency samples plus comparison
rows to `baseline-results/`. Each run is checked by a `read_committed` consumer,
and a failed correctness run is rejected.

Run each implementation in its own JVM with identical arguments, alternating
their order and repeating each pair at least three times:

```bash
mvn test -Pspring-baseline

mvn -Pspring-baseline compile exec:java \
  -Dexec.mainClass=com.kafka.producer.baseline.BaselineRunner \
  -Dscenario=B-02 -Dimplementation=pool -Dtopology=single-broker \
  -DproducerCount=4 -Dthreads=4 -DrecordsPerTransaction=50 -DrunNumber=1

mvn -Pspring-baseline compile exec:java \
  -Dexec.mainClass=com.kafka.producer.baseline.BaselineRunner \
  -Dscenario=B-02 -Dimplementation=spring-kafka -Dtopology=single-broker \
  -DproducerCount=4 -Dthreads=4 -DrecordsPerTransaction=50 -DrunNumber=1
```

Scenarios B-01 through B-06 are available. B-06 requires the three-broker
environment and an operator-controlled broker restart; the harness does not
replay transactions whose commit outcome is ambiguous.

## Project layout

```text
src/main/java/com/kafka/producer/pool/   Pool implementation and public API
src/test/java/com/kafka/producer/pool/   Unit tests
src/perf/java/com/kafka/producer/perf/   Load scenarios and JMH benchmark
src/chaos/java/com/kafka/producer/chaos/ Multi-broker and chaos test harness
src/agentic/java/com/kafka/producer/agentic/ Policy-bounded campaign runner
src/baseline/java/com/kafka/producer/baseline/ Spring comparison harness
specs/                                   Functional and performance specifications
perf-results/                            Checked-in benchmark output
```

## Further reading

- [`specs/001-pooled-kafka-producer/spec.md`](specs/001-pooled-kafka-producer/spec.md)
- [`specs/002-performance-testing/spec.md`](specs/002-performance-testing/spec.md)
- [`specs/007-agentic-producer-fault-injection/spec.md`](specs/007-agentic-producer-fault-injection/spec.md)
