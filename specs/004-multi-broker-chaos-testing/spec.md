# Specification: Multi-Broker Resilience and Chaos Testing

## 1. Purpose

Define a reproducible integration and performance test suite that validates the transactional producer pool from spec 001 against a realistic multi-broker Kafka cluster under broker failure and network disruption.

This specification extends spec 002 with:

- Multi-broker cluster scenarios.
- Broker restart during sustained transactional publishing.
- Network partition simulation.
- Recovery, atomicity, ordering, and resource-safety assertions during faults.

## 2. Goals

- Verify transactional publishing against a replicated, three-broker Kafka cluster.
- Measure the effect of partition leadership and replication on throughput and latency.
- Confirm the producer pool remains safe during leader changes and broker restarts.
- Validate recovery after temporary loss of broker connectivity.
- Confirm ambiguous commits are surfaced and never automatically retried.
- Detect producer leaks, lease leaks, stuck transactions, and recovery storms.
- Produce repeatable evidence that can gate releases.

## 3. Scope

### In Scope

- A local three-broker Kafka cluster using KRaft mode.
- Topics with multiple partitions and replication factor three.
- Baseline, broker-loss, broker-restart, and network-partition scenarios.
- Transactional publishes through `TransactionalProducerPool`.
- Fault injection at controlled points during sustained load.
- Measurement of availability, recovery time, throughput, latency, and correctness.
- Verification using `read_committed` and `read_uncommitted` consumers.
- Structured CSV and Markdown results.

## 2.1 Ownership and evidence boundaries

- A shared baseline/evidence mission owns MB-01 and MB-02. MB-01 establishes correctness and steady-state measurements; MB-02 records topology and per-partition routing evidence consumed by broker-failure scenarios.
- Baseline ownership does not imply that every measurement is a gate. Each scenario below names its hard invariants; throughput and skew remain diagnostic unless a numeric threshold is stated.
- “Acknowledged” means the transaction callback returned normally after `commitTransaction` completed. A successful `send()` future alone is not a committed transaction.
- Every transaction must have a run-scoped unique `publish_id`; every record within it has a stable `record_index`. Producer transactional IDs are diagnostic identifiers, not deduplication keys, because pool slots reuse them.
- The direct harness accepts `--output-topics` as a comma-separated set. It writes each publish's records to every configured topic inside the same transaction. MB-01 injects one intentional abort per ten publishes by default (`--abort-every 0` disables the control sample).
- The harness captures per-topic partition high-water offsets before load starts and after workers drain. It scans the same bounded ranges under both isolation levels and reports per-partition committed counts.

### Out of Scope

- Multi-cluster failover from spec 003.
- Cross-region latency and public cloud failure injection.
- Kafka cluster provisioning for production.
- Consumer group failover benchmarking.
- Kafka Connect or cross-cluster replication.
- Permanent data-loss scenarios where the topic loses its in-sync replica quorum.
- Operating-system or JVM tuning recommendations.

## 4. Definitions

- **Fault window:** Time from confirmed fault injection until confirmed fault removal.
- **Recovery time:** Time from fault removal or broker readiness until successful transactional publishing returns to the steady-state threshold.
- **Steady state:** Pre-fault throughput and latency measured during the baseline window.
- **Availability:** Percentage of attempted transactions that commit successfully during a measurement window.
- **Ambiguous commit:** A transaction commit whose result cannot be determined by the producer.
- **Leader broker:** Broker currently leading at least one partition of the test topic.
- **Non-leader broker:** Broker that is not the leader for any selected target partition at injection time.
- **Network partition:** A controlled packet drop or connection interruption between the producer process and one or more brokers.

## 5. Safety Requirements

- Chaos scenarios shall run only against an explicitly identified disposable test cluster.
- The harness shall reject bootstrap servers not allow-listed by the test configuration.
- Fault injection shall require `--chaos-enabled=true`.
- Each fault shall have a bounded duration and a cleanup action registered before injection.
- Cleanup shall execute on normal completion, assertion failure, timeout, and process shutdown where possible.
- Test runs shall not log record payloads or credentials.
- The harness shall stop the scenario if cluster identity changes during a run.
- CI chaos jobs shall be isolated from shared developer and production infrastructure.

## 6. Test Environment

### 6.1 Kafka Cluster

- Three Kafka brokers in KRaft mode.
- Kafka version matching the `kafka-clients` compatibility target.
- Separate listener and storage configuration for each broker.
- Test topic:
  - Name: `chaos-perf-test`.
  - Partitions: 12.
  - Replication factor: 3.
  - `min.insync.replicas=2`.
  - `unclean.leader.election.enable=false`.
- Transaction state topic:
  - Replication factor: 3.
  - Minimum in-sync replicas: 2.
- Offsets topic replication factor: 3.
- Automatic topic creation disabled.

### 6.2 Producer Client

- Java 21 for recorded performance runs.
- Fixed heap: `-Xms512m -Xmx512m`.
- Producer configuration from spec 001:
  - `enable.idempotence=true`.
  - `acks=all`.
  - Bounded delivery and request timeouts.
  - Unique transactional ID per pool slot.
- Bootstrap servers shall include all three brokers.

### 6.3 Test Load

Unless a scenario overrides these values:

- Pool size: 8.
- Concurrent publisher threads: 8.
- Records per transaction: 10.
- Record size: 1 KB.
- Deterministic record keys.
- Test duration: 10 minutes.
- Pre-fault baseline: 2 minutes.
- Fault window: 2 minutes.
- Post-fault recovery window: 6 minutes.

### 6.4 Repeatability

- Run each scenario three times.
- Report median and standard deviation for latency, throughput, and recovery time.
- Record Kafka version, client version, JVM version, host resources, container runtime, and operating system.
- Record broker-to-container mapping and partition leadership immediately before injection.
- Use the same load seed for repeated runs.

## 7. Fault Injection Interface

The chaos controller shall expose operations conceptually equivalent to:

```text
inspectCluster()
selectLeaderBroker(topic, partition)
selectNonLeaderBroker(topic)
stopBroker(brokerId)
startBroker(brokerId)
waitForBrokerReady(brokerId, timeout)
partitionProducerFromBroker(brokerId)
partitionProducerFromCluster()
healNetwork()
verifyCleanup()
```

The first implementation may control Docker containers and host firewall rules. Fault injection commands shall be abstracted behind a `ChaosController` so other environments can provide compatible implementations.

Every operation shall return a timestamped result and enough evidence to distinguish a requested fault from a successfully applied fault.

## 8. Test Scenarios

### MB-01: Three-Broker Transactional Baseline

**Purpose:** establish performance and correctness with replication enabled and no injected failure.

- Run the standard load for 10 minutes.
- Capture partition leadership and in-sync replica state at start and end.
- Consume committed records using `isolation.level=read_committed`.

**Capture:**

- Transactions and records per second.
- p50, p95, and p99 transaction latency.
- p50, p95, and p99 lease wait latency.
- Transaction abort and retry counts.
- Pool health and producer replacement count.
- Under-replicated partition count.

**Pass criteria:**

- No unexpected aborted or ambiguous transactions.
- After submissions stop, every open transaction must resolve within the configured drain deadline. Unresolved transactions fail the run and are reported as pending; they are neither success nor loss.
- No lease or producer leaks remain after drain.
- Every acknowledged publish ID and expected record index is visible exactly once to the `read_committed` verifier on every transactional output topic.
- Every explicitly aborted transaction is absent from `read_committed`. Its presence in `read_uncommitted` is expected and is not a failure.
- Do not compare raw `read_committed` and `read_uncommitted` counts for equality. Reconcile each isolation view against the publish ledger and captured partition offset range.

### MB-02: Partition Leader Distribution

**Purpose:** verify publishing across partitions led by different brokers.

- Confirm the test topic has leaders distributed across all three brokers.
- Publish round-robin keyed traffic to every partition using `--partition-count`.
- Run for 5 minutes.

**Capture:**

- Throughput and latency per partition.
- Throughput and latency grouped by leader broker.
- Record count and ordering violations per key.

**Pass criteria:**

- This is primarily an observational topology/routing baseline. Fail if any expected partition lacks a leader/replica assignment, any partition receives fewer than the configured minimum sample, or a defined per-key ordering invariant is violated.
- Check order within a topic-partition; check business-key order only when that key is consistently routed to one partition and its expected sequence is defined. Kafka does not provide global or cross-topic ordering.
- Leader-count skew and throughput differences are reported, not failed, unless a run explicitly configures a sample size and numeric skew/starvation threshold in advance.

### MB-03: Non-Leader Broker Loss

**Purpose:** measure the impact of losing a replica broker while partition leaders remain available.

- Identify a broker that leads none of the selected target partitions, or move target partition leadership before the run.
- Stop that broker after the baseline window.
- Keep it offline for the fault window.
- Restart it and wait for replicas to rejoin the ISR.

**Capture:**

- Publish success rate during the fault.
- Latency increase relative to baseline.
- Under-replicated partitions.
- Time until all replicas rejoin the ISR.

**Pass criteria:**

- Publishing continues without an unavailable pool state.
- No acknowledged transaction is lost or duplicated.
- The pool does not rebuild producers solely because a non-leader replica stopped.

### MB-04: Leader Broker Loss

**Purpose:** validate transactional publishing through partition leader election.

- Select a broker leading at least one target partition.
- Stop the broker after the baseline window.
- Continue the load while Kafka elects new leaders.
- Restart the broker after the fault window.

**Capture:**

- Leader-election duration.
- Failed, retried, aborted, and ambiguous transaction counts.
- Maximum consecutive publish-failure duration.
- p95 and p99 latency during election.
- Pool state transitions and producer replacements.

**Pass criteria:**

- Publishing resumes without restarting the application.
- No producer or lease remains stuck after recovery.
- No transaction is partially visible to a `read_committed` consumer.
- Any ambiguous commit is reported as ambiguous and is not retried by the pool.

### CH-01: Broker Restart Mid-Soak

**Purpose:** validate stability through a full broker stop-and-start cycle during sustained load.

- Start a 10-minute transactional soak.
- Use a dedicated temporary test topic with enough partitions to produce an imbalance. At minute 3, snapshot leadership and select a broker whose leader count is strictly greater than every other broker's, then stop it.
- If the distribution is tied or balanced, mark the attempt invalid/ineligible and do not inject a fault. Do not silently substitute a single-partition leader selection.
- Keep it stopped for 60 seconds.
- Restart the same broker.
- Continue the soak until minute 10.

**Capture:**

- Per-second throughput, success rate, and latency.
- Exact stop, readiness, leader-election, and ISR-recovery timestamps.
- Pool health, ready count, leased count, and total producer count.
- Transaction retry, abort, fatal error, fencing, and recovery counts.
- Heap use and active worker count.

**Pass criteria:**

- The harness completes the full soak without deadlock.
- Throughput recovers to at least 90% of the pre-fault median within 60 seconds of broker readiness.
- Pool state returns to `HEALTHY` within 30 seconds of cluster readiness.
- Ready plus leased producer counts reconcile with total healthy producers.
- No lease remains active after the scenario.
- No acknowledged transaction is missing from the committed-record verification.

### CH-02: Producer-to-One-Broker Network Partition

**Purpose:** validate behaviour when the application cannot reach one broker but the broker remains healthy for the rest of the cluster.

- Select a broker leading target partitions.
- Drop traffic between the producer host or container and that broker only.
- Maintain the partition for 60 seconds.
- Heal the network without restarting the producer or broker.

**Capture:**

- Connection and request timeout counts.
- Transaction retries, aborts, and ambiguous commits.
- Per-partition publish success and latency.
- Recovery time after network healing.

**Pass criteria:**

- Unaffected partitions continue publishing when Kafka metadata and leadership permit.
- The pool applies bounded waits and does not hang indefinitely.
- All blocked worker calls complete or time out within the configured delivery envelope.
- No ambiguous transaction is automatically retried.
- Normal throughput returns after network healing.

### CH-03: Producer-to-Cluster Network Partition

**Purpose:** validate deterministic backpressure and recovery during complete temporary loss of Kafka connectivity.

- Drop traffic between the producer and all three brokers.
- Maintain the partition for 60 seconds.
- Continue making bounded publish attempts.
- Heal all network paths simultaneously.

**Capture:**

- Time from partition start to pool `DEGRADED` or `UNAVAILABLE`.
- Lease wait, lease timeout, transaction failure, and retry counts.
- Maximum outstanding request count.
- Heap and thread count during the outage.
- Time to first committed transaction after healing.
- Time to steady-state throughput.

**Pass criteria:**

- Publish calls fail within their configured timeout envelope.
- Memory, thread, and request counts remain bounded.
- The process remains responsive and shutdown remains possible.
- Publishing resumes without an application restart.
- No record from an aborted transaction is visible to a `read_committed` consumer.
- Ambiguous outcomes are reported distinctly from conclusively failed transactions.

### CH-04: Short Flapping Network Partition

**Purpose:** detect retry storms and producer churn under intermittent connectivity.

- Apply six cycles of 10 seconds partitioned and 20 seconds healthy.
- Partition the producer from all brokers during each fault interval.
- Continue fixed-rate transactional load.

**Capture:**

- Retry attempts per transaction.
- Producer eviction and replacement rate.
- Pool health transitions.
- Broker connection attempts.
- Throughput and latency after each healing interval.

**Pass criteria:**

- Retry attempts remain within configured bounds.
- Producer replacements do not grow without bound.
- The recovery supervisor respects its concurrency limit.
- The final healthy interval returns the pool to `HEALTHY`.

### CH-05: Transaction Commit Response Partition

**Purpose:** verify fail-closed handling when connectivity is lost around commit.

- Use a fault hook or network proxy to interrupt the producer connection after the commit request may have reached Kafka but before the response is observed.
- Current Toxiproxy downstream-drop behavior suppresses all responses, not only `EndTxn`; it is approximate transport-level ambiguity evidence and does not prove request arrival or response loss for a particular commit.
- Prefer a protocol-aware proxy that records `EndTxn` and selectively drops its correlated response. Broker transaction-state instrumentation is useful corroborating evidence. A TCP firewall rule alone has the same timing ambiguity as Toxiproxy.
- A trial is valid only if evidence shows an `EndTxn` request reached Kafka and the corresponding response was dropped. If no qualifying commit overlaps the fault, classify the trial invalid/incomplete, not pass or fail.

**Capture:**

- Ambiguous outcome count.
- Producer quarantine and replacement count.
- Callback execution count by publish ID.
- Records observed by `read_committed` and `read_uncommitted` verifiers.

**Pass criteria:**

- The application returns a typed ambiguous outcome/error; it never reports success based only on timeout.
- The callback runs once for that publish ID. The affected slot is quarantined from new leases and is not reused until the configured producer recovery/fencing boundary is met.
- No automatic callback replay occurs. Business reconciliation uses publish ID and observed committed records.
- Final broker-visible outcome is reconciled independently: committed records appear once and complete, or aborted records are absent from `read_committed`.
- A mistimed/unproven injection is invalid/incomplete. It cannot count as a pass. Violating any application behavior above is a failure.

## 9. Correctness Verification

Each record shall contain:

- Run ID.
- Scenario ID.
- Publish ID.
- Transaction attempt ID.
- Deterministic ordering key.
- Per-key sequence number.
- Producer timestamp.
- Stable record index within its publish/transaction.

After each scenario, the verifier shall:

1. Capture start and end offsets for every partition before/after the run; verify only that bounded range, without timestamp-based range inference.
2. Consume every transactional output topic with `read_committed` and reconcile publish IDs plus record indexes against the ledger.
3. Use `read_uncommitted` as diagnostic evidence for aborted/open transaction records; never require its total count to equal `read_committed`.
4. Compare attempted, acknowledged, rejected, aborted, pending, and ambiguous outcomes separately.
5. Assert every acknowledged transaction is complete on every output topic and every conclusively aborted transaction is absent from `read_committed`.
6. Detect duplicates by `(run_id, publish_id, record_index)`; report mismatched or partial cross-topic sets.
7. Check sequence gaps/inversions only for an explicitly defined ordering scope (per partition or serialized business key).
8. Report ambiguous IDs separately; do not automatically classify them as lost, committed, or safe to replay.

The direct harness persists numeric verification totals, per-publish rows, per-partition committed counts, and start/end offset ranges. Agentic campaign artifacts persist the same numeric totals and verification rows. The current Toxiproxy CH-05 implementation remains explicitly invalid/incomplete because it cannot prove a correlated `EndTxn` request and dropped response; protocol-aware injection is still required before CH-05 can pass.

The earlier AF-05 artifact was produced before the protocol-proof gate existed. Its `PASSED` label is historical and must be interpreted as invalid/incomplete CH-05 evidence under this specification.

The verifier shall retain raw evidence for every failed assertion.

## 10. Metrics

The test harness shall capture:

| Metric | Unit |
|---|---|
| Transaction throughput | tx/s |
| Record throughput | msg/s |
| Publish availability | percent |
| Transaction latency | ms, p50/p95/p99 |
| Lease wait latency | ms, p50/p95/p99 |
| Recovery time | ms |
| Maximum consecutive failure window | ms |
| Transaction retries | count |
| Transaction aborts | count |
| Ambiguous commits | count |
| Producer evictions and replacements | count |
| Pool state and producer counts | time series |
| Under-replicated partitions | count |
| Offline partitions | count |
| ISR recovery time | ms |
| JVM heap and live threads | MB/count |

Broker metrics and producer-pool metrics shall share a synchronized timestamp source.

## 11. Test Harness Design

### Entry Point

```text
com.kafka.producer.chaos.ChaosRunner
```

Supported arguments shall include:

```text
--scenario MB-01..MB-04|CH-01..CH-05
--bootstrap-servers <brokers>
--topic <topic>
--pool-size <n>
--threads <n>
--duration-sec <n>
--fault-at-sec <n>
--fault-duration-sec <n>
--chaos-controller <docker|proxy|custom>
--cluster-allowlist <id>
--chaos-enabled <true|false>
--results-dir <path>
```

### Structure

```text
src/chaos/java/com/kafka/producer/chaos/
    ChaosRunner.java
    ChaosConfig.java
    ChaosController.java
    ChaosEvent.java
    ClusterInspector.java
    CorrectnessVerifier.java
    ChaosResultWriter.java
    controller/
        DockerChaosController.java
        ProxyChaosController.java
    scenarios/
        MultiBrokerBaselineScenario.java
        BrokerLossScenario.java
        BrokerRestartSoakScenario.java
        NetworkPartitionScenario.java
        FlappingPartitionScenario.java
        CommitAmbiguityScenario.java
```

The chaos source set shall reuse measurement and load-generation utilities from spec 002 where practical, without coupling fault-control code to the core producer library.

## 12. Execution Phases

Every chaos scenario shall follow these phases:

1. **Validate:** confirm the cluster allow-list, topic configuration, broker count, and chaos opt-in.
2. **Warm up:** initialize producers and exclude warm-up measurements.
3. **Baseline:** establish pre-fault throughput, latency, and pool health.
4. **Inject:** apply the fault and confirm it took effect.
5. **Observe:** maintain load and record behaviour throughout the fault.
6. **Heal:** reverse the fault and confirm connectivity or broker readiness.
7. **Recover:** continue load until steady state or recovery timeout.
8. **Verify:** drain publishers and run committed/uncommitted correctness checks.
9. **Clean up:** remove all fault rules and confirm all brokers are healthy.
10. **Report:** write results and pass/fail evidence.

A scenario shall fail if fault injection or cleanup cannot be confirmed.

## 13. Results Reporting

Each run shall produce:

- `chaos-results/<scenario>-<timestamp>.csv` containing per-second samples.
- `chaos-results/<scenario>-<timestamp>-events.jsonl` containing fault and state-transition events.
- `chaos-results/<scenario>-<timestamp>-verification.csv` containing publish-ID verification.
- `chaos-results/summary.md` containing environment, configuration, results, and pass/fail status.

The summary shall distinguish:

- Failure to inject the requested fault.
- Failure of the producer pool under a confirmed fault.
- Failure to clean up the environment.
- Correctness failure.
- Performance or recovery-SLO failure.

## 14. Aggregate Acceptance Criteria

The suite passes when:

- All acknowledged transactions are fully visible to a `read_committed` consumer.
- No conclusively aborted transaction is visible to a `read_committed` consumer.
- No ambiguous transaction is automatically replayed.
- Per-key committed order is preserved.
- Broker restart and network healing require no application restart.
- All publish calls remain bounded by configured timeouts.
- No scenario leaks producer leases, worker threads, or fault rules.
- Producer recovery remains within configured concurrency limits.
- Each scenario produces complete, reproducible evidence.

Scenario-specific thresholds may be tuned after the first validated baseline, but correctness and bounded-execution criteria are mandatory and may not be relaxed.

## 15. CI Strategy

- Run MB-01 and MB-02 on pull requests when multi-broker infrastructure is available.
- Run MB-03, MB-04, CH-01, and CH-02 on a scheduled or release-gate job.
- Run CH-03 through CH-05 on an isolated scheduled environment.
- Serialize chaos jobs that share a cluster.
- Always publish reports as CI artifacts, including failed-run cleanup evidence.
- Do not run privileged network fault injection on untrusted contributions.

## 16. Risks and Mitigations

- **Risk:** A fault targets the wrong Kafka environment.
  - **Mitigation:** Cluster allow-list, explicit chaos opt-in, and disposable infrastructure.
- **Risk:** Firewall or proxy rules remain after a failed test.
  - **Mitigation:** Pre-registered cleanup, shutdown hooks, bounded faults, and cleanup verification.
- **Risk:** Broker readiness is mistaken for replica recovery.
  - **Mitigation:** Track both broker API readiness and ISR restoration.
- **Risk:** Timing-dependent results are difficult to reproduce.
  - **Mitigation:** Record an event timeline, leadership, configuration, and deterministic load seed.
- **Risk:** Local Docker behaviour differs from production.
  - **Mitigation:** Treat local thresholds as regression baselines rather than universal capacity guarantees.
- **Risk:** A correctness verifier misclassifies ambiguous commits.
  - **Mitigation:** Report ambiguous publish IDs as a separate outcome and retain broker-visible evidence.

## 17. Implementation Sequence

1. Add the three-broker disposable Kafka environment and readiness checks.
2. Add run IDs, publish IDs, attempt IDs, and sequence metadata to chaos records.
3. Implement cluster inspection and the correctness verifier.
4. Implement MB-01 and MB-02 to establish the replicated-cluster baseline.
5. Implement broker stop, start, readiness, and cleanup controls.
6. Implement MB-03, MB-04, and CH-01.
7. Implement single-broker and whole-cluster network partitions.
8. Implement CH-02 through CH-04.
9. Add the commit-response fault hook or proxy required for CH-05.
10. Add CI isolation, reporting, and release-gate thresholds.

## 18. Future Extensions

- Multi-region managed Kafka chaos testing.
- Disk latency and broker storage exhaustion.
- Transaction coordinator failover targeting.
- Controller-quorum member loss.
- Schema Registry disruption.
- Consumer-side rebalance and recovery scenarios.
