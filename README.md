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
