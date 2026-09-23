# Specification: Durable Outbox Publisher for Transactional Producer Pool

## 1. Purpose

Add an optional durable outbox publishing capability around the existing `TransactionalProducerPool`.

The outbox capability must let an application durably record "this logical event must be published" in the same database consistency boundary as its business mutation, then publish pending records to Kafka using the existing producer pool.

The design must not pretend to provide a distributed ACID transaction across an application database and Kafka. It provides durable handoff, bounded recovery, explicit ambiguous outcomes, and at-least-once logical delivery when paired with a stable `publishId` and downstream deduplication.

## 2. Architectural Position

The outbox is not part of Kafka's transaction coordinator and is not a Saga coordinator.

```text
Application request/job
        |
        v
Application DB transaction / atomic mutation boundary
   +-------------------------------+
   | business state change         |
   | outbox record INSERT          |
   +-------------------------------+
        |
        | durable commit
        v
OutboxStore
        |
        | claim batch
        v
OutboxPublisher
        |
        v
TransactionalProducerPool
        |
        | Kafka transaction
        v
Kafka
        |
        | success / failure / ambiguous
        v
OutboxStore state transition
```

The producer pool remains responsible for Kafka producer lifecycle, leasing, begin/send/commit/abort, retry classification, fencing recovery, health, and metrics.

The outbox module is responsible for durable work discovery, leases/claims, batching, publish metadata, retry scheduling, and durable delivery state.

The application or database-specific adapter remains responsible for ensuring the business mutation and creation of the outbox record use the strongest atomicity guarantee available in that datastore.

## 3. Goals

- Durable DB-to-Kafka handoff without introducing a mandatory new database.
- Reuse the existing `TransactionalProducerPool` for Kafka publication.
- Support multiple application pods draining the same logical outbox safely.
- Avoid permanent ownership of an outbox item by the pod that created it.
- Use claim leases so crashed publishers self-heal.
- Use stable `publishId` values across retries.
- Distinguish conclusive Kafka failure from ambiguous commit outcome.
- Apply bounded retries with backoff and poison-record isolation.
- Expose backlog, age, retry, ambiguity, and claim metrics.
- Allow database-specific implementations for VoltDB, DataStax/Cassandra, JDBC, or other stores.
- Preserve the existing producer-pool public API for applications that do not use an outbox.

## 4. Non-Goals

- Cross-database ACID transactions.
- Exactly-once delivery across an arbitrary database and Kafka.
- Saga orchestration or compensation.
- Business-state rollback.
- A built-in VoltDB or DataStax driver dependency in the core library.
- Automatic replay of an ambiguous Kafka commit.
- Replacing Kafka Connect/Debezium CDC where CDC is a better fit.
- Treating the outbox database as a second mandatory infrastructure tier.

## 5. Key Design Rule

The library must not require a standalone "outbox database".

The preferred deployment is:

```text
VoltDB-owned business mutation   -> VoltDB-local outbox record
DataStax-owned business mutation -> DataStax-local outbox record
```

A separate outbox database is permitted only when the application can prove a reliable consistency protocol between the business datastore and that outbox. Otherwise it merely moves the dual-write problem.

## 6. Delivery Semantics

### 6.1 Guaranteed

When the database-specific write path successfully commits both business state and the outbox item:

- The publish intent survives process restart.
- Any healthy publisher pod may claim the item.
- A claimed item is eventually reclaimable after lease expiry if the claiming pod dies.
- One publisher instance at a time should own a valid claim for an item.
- Kafka records in one publisher batch use one `TransactionalProducerPool` transaction.
- A conclusively failed Kafka transaction does not cause the outbox item to be marked published.
- A producer fencing error is delegated to the existing pool fatal-producer handling.
- A Kafka commit classified as ambiguous is never automatically treated as success.

### 6.2 Not Guaranteed

- Atomic commit between the database and Kafka.
- Global exactly-once logical delivery after a crash between Kafka commit and outbox acknowledgement.
- Automatic determination that an ambiguous Kafka commit did or did not commit.
- Atomic creation of outbox rows across unrelated DataStax partitions unless the chosen DataStax data model/protocol provides the required guarantee.
- Exactly-once delivery across Kafka clusters.

Therefore every logical outbox item must have a stable globally unique `publishId`, and consumers that require logical exactly-once effects must deduplicate using that identifier.

## 7. Outbox Record

Conceptual model:

```java
record OutboxRecord(
    String publishId,
    String aggregateType,
    String aggregateId,
    String correlationId,
    String topic,
    byte[] key,
    byte[] value,
    Map<String, byte[]> headers,
    Instant createdAt,
    Instant availableAt,
    int attemptCount,
    OutboxState state,
    String claimedBy,
    Instant claimUntil,
    String lastFailureClass,
    String lastFailureMessage
) {}
```

Required fields:

- `publishId`: globally unique logical publish identity; immutable across retries.
- `topic`: destination Kafka topic.
- `key`: Kafka key when applicable.
- `value`: serialized payload.
- `createdAt`: original creation time.
- `availableAt`: earliest retry time.
- `state`: durable delivery state.

Recommended fields:

- `aggregateType`, `aggregateId`: business ownership and ordering context.
- `correlationId`: tracing/job correlation.
- headers for schema/version/business metadata.
- failure metadata for diagnostics.

Payloads and sensitive business keys must not be written to logs by default.

## 8. State Model

```text
PENDING
   |
   | claim
   v
CLAIMED
   |
   +---- Kafka committed ----> PUBLISHED
   |
   +---- conclusive failure -> RETRY_WAIT -> PENDING
   |
   +---- ambiguous commit ---> AMBIGUOUS
   |
   +---- poison/exhausted ---> DEAD
```

### States

- `PENDING`: eligible for claim when `availableAt <= now`.
- `CLAIMED`: temporarily owned by one publisher using a lease.
- `RETRY_WAIT`: conclusive failure; retry scheduled in the future.
- `PUBLISHED`: Kafka commit reported successful and outbox acknowledgement persisted.
- `AMBIGUOUS`: Kafka commit outcome cannot be proven; no automatic retry.
- `DEAD`: administratively quarantined after policy-defined terminal failure.

A lease expiry on `CLAIMED` makes the record eligible for reclaim.

## 9. Outbox Store SPI

The core library shall depend only on an SPI.

```java
public interface OutboxStore {

    List<ClaimedOutboxRecord> claimBatch(
        String workerId,
        int maxRecords,
        Duration leaseDuration,
        Instant now);

    void markPublished(
        String workerId,
        Collection<OutboxReceipt> receipts,
        Instant publishedAt);

    void markRetry(
        String workerId,
        Collection<OutboxFailure> failures,
        Instant nextAttemptAt);

    void markAmbiguous(
        String workerId,
        Collection<OutboxAmbiguity> ambiguities);

    void markDead(
        String workerId,
        Collection<OutboxFailure> failures);

    void renewClaims(
        String workerId,
        Collection<String> publishIds,
        Instant claimUntil);

    OutboxBacklogSnapshot getBacklogSnapshot();
}
```

The SPI must document that claim and state-transition methods are conditional on current ownership so a stale pod cannot acknowledge work after its lease has expired.

The core library does not provide the business-write API because atomic insertion must occur inside the application's datastore-specific mutation boundary.

## 10. Database-Specific Expectations

### 10.1 VoltDB

A VoltDB implementation should create the business mutation and outbox item in the same VoltDB transaction/stored procedure whenever both belong to the same transaction scope.

Claims should be implemented using a VoltDB-safe atomic procedure that selects eligible work and assigns `claimedBy` / `claimUntil` without allowing two publishers to own the same item.

### 10.2 DataStax / Cassandra

A DataStax implementation must be designed around Cassandra partitioning rather than copying a relational `SELECT ... FOR UPDATE SKIP LOCKED` design.

The adapter should:

- partition pending work so scans are bounded and queryable,
- avoid unrestricted `ALLOW FILTERING`,
- use idempotent mutations,
- use conditional/LWT operations only where ownership correctness requires them,
- avoid one globally hot outbox partition,
- bucket by time and/or shard,
- make lease recovery queryable,
- document the atomicity limitation when business data and outbox data are not in the same partition/batch consistency boundary.

A recommended logical key is:

```text
bucket = timeBucket + shard
clustering = availableAt, publishId
```

with a separate immutable event identity keyed by `publishId` if needed for reconciliation.

### 10.3 JDBC

A JDBC adapter may use row locks and `SKIP LOCKED` where supported, but that behavior must not be assumed by the core SPI.

## 11. Publisher

New optional component:

```java
OutboxPublisher publisher = OutboxPublisher.builder()
    .store(outboxStore)
    .producerPool(pool)
    .workerId(instanceId)
    .batchSize(1000)
    .leaseDuration(Duration.ofSeconds(60))
    .pollInterval(Duration.ofMillis(500))
    .retryPolicy(retryPolicy)
    .build();
```

The publisher may run embedded in every application pod or in dedicated publisher pods.

Embedded multi-pod mode is preferred when application scaling and outbox drain scaling should move together.

No outbox item is permanently bound to the pod that created it.

## 12. Publish Algorithm

For each polling cycle:

1. Claim up to `batchSize` eligible items.
2. Validate all claimed items and group them into a Kafka transaction batch according to configured constraints.
3. Add standard headers:
   - `publish.id`
   - `outbox.created-at`
   - `outbox.attempt`
   - `correlation.id` when present.
4. Execute one Kafka transaction using `TransactionalProducerPool.executeInTransaction`.
5. On `COMMITTED`, conditionally mark the claimed records `PUBLISHED`.
6. On a conclusive retryable failure, schedule `RETRY_WAIT` with bounded backoff.
7. On a terminal application/configuration error, move to `DEAD` according to policy.
8. On an ambiguous commit, move the records to `AMBIGUOUS` and raise an alert; do not automatically replay.
9. Always release the producer through existing pool lifecycle rules.

## 13. Batch Boundaries

The publisher must not assume all pending rows belong in one Kafka transaction.

Batch limits must support:

- maximum record count,
- maximum estimated byte size,
- maximum transaction execution time budget,
- optional topic grouping,
- optional routing/ordering grouping.

Example:

```text
100,000 pending rows
 -> 100 transactions x 1,000 records
```

instead of one 100,000-record Kafka transaction.

The default implementation should prefer bounded recoverability over maximum transaction size.

## 14. Crash Windows and Recovery

### 14.1 Crash Before Kafka Transaction Starts

Lease eventually expires. Another pod reclaims the records.

### 14.2 Crash During Kafka Transaction Before Commit

Kafka transaction is aborted by coordinator timeout/session loss according to Kafka semantics. Lease eventually expires and records may be reclaimed.

### 14.3 Kafka Commit Fails Conclusively

Do not mark published. Schedule retry.

### 14.4 Kafka Commit Outcome Is Ambiguous

Mark `AMBIGUOUS` if the process remains alive long enough to persist that classification.

Do not automatically republish.

### 14.5 Crash After Kafka Commit but Before markPublished

This is the unavoidable outbox acknowledgement gap:

```text
Kafka COMMIT succeeds
        |
        X process dies
        |
outbox still CLAIMED/PENDING after lease expiry
```

A later publisher may publish the logical event again.

Mitigation:

- stable `publishId`,
- downstream deduplication,
- optional reconciliation tooling,
- short acknowledgement path,
- metrics and alerts for lease-expired reclaim after a prior publish attempt.

This project must not claim exactly-once DB-to-Kafka semantics for this window.

## 15. Ordering

When per-aggregate ordering matters:

- use `aggregateId` as the Kafka key,
- ensure the outbox store claim strategy does not concurrently publish later events for an aggregate while an earlier event is blocked/ambiguous,
- optionally include an aggregate sequence number,
- consumers may reject or defer out-of-sequence records.

Global outbox ordering is not required.

## 16. Retry Policy

Retries occur at two layers and must remain distinct.

### Pool-level retry

Short-lived Kafka transaction retry handled by the existing producer pool when the failure is safe to retry.

### Outbox-level retry

Durable retry after the current publish attempt has conclusively failed.

Recommended fields/configuration:

```text
outbox.retry.maxAttempts
outbox.retry.baseDelayMs
outbox.retry.maxDelayMs
outbox.retry.jitter
outbox.retry.deadAfter
```

The publisher must not create a tight loop when Kafka or the pool is unavailable.

## 17. Health and Backpressure

The outbox creates a durability buffer; it must not hide prolonged Kafka failure.

Health states:

- `HEALTHY`: pool usable, publisher draining, backlog age below thresholds.
- `DEGRADED`: backlog growing or pool below desired capacity.
- `BLOCKED`: pool unavailable or store unavailable; no new claims.
- `CRITICAL`: oldest backlog age or ambiguous/dead count exceeds policy.

Applications may continue business writes while Kafka is unavailable only if the business contract permits asynchronous publication and the outbox store itself remains durable.

Backpressure policy may reject new business work when backlog count/age exceeds configured safety limits.

## 18. Observability

Metrics:

```text
outbox_backlog_records
outbox_oldest_record_age_seconds
outbox_claim_total{outcome}
outbox_claimed_records
outbox_publish_batch_total{outcome}
outbox_publish_records_total{outcome}
outbox_publish_duration_ms
outbox_retry_total{failure_class}
outbox_ambiguous_total
outbox_dead_total
outbox_claim_expired_total
outbox_store_operation_total{operation,outcome}
```

Structured logs should include:

- publishId,
- correlationId,
- workerId,
- attempt,
- batchId,
- pool transaction outcome,
- failure class,
- next retry time.

Payloads must not be logged by default.

## 19. Configuration

Conceptual configuration:

```text
outbox.enabled=false
outbox.workerId=<instance>
outbox.batchSize=1000
outbox.pollIntervalMs=500
outbox.leaseDurationMs=60000
outbox.claimRenewIntervalMs=20000
outbox.maxBatchBytes=<optional>
outbox.maxTransactionDurationMs=<optional>
outbox.retry.maxAttempts=10
outbox.retry.baseDelayMs=1000
outbox.retry.maxDelayMs=60000
outbox.ambiguous.autoRetry=false
outbox.deadLetter.enabled=true
```

Validation must reject `ambiguous.autoRetry=true` in the first implementation.

## 20. Relationship to Existing Specs

### Spec 001

The outbox publisher uses `TransactionalProducerPool`; it does not alter producer leasing or transaction lifecycle semantics.

### Spec 003

The outbox provides the durable ledger explicitly identified as missing from multi-cluster failover.

If outbox publication is combined with multi-cluster routing:

- `publishId` remains stable across cluster routing,
- ambiguous commit still blocks automatic cross-cluster replay,
- cluster/routing metadata should be persisted with the delivery attempt.

### Spec 004 / 007

Chaos testing must add outbox-specific failure windows, especially process death after Kafka commit and before `markPublished`.

## 21. Failure Scenarios

| ID | Failure | Expected behavior |
|---|---|---|
| OB-01 | Kafka unavailable before claim | Do not claim aggressively; backlog remains durable |
| OB-02 | Kafka unavailable after claim | Mark retry or allow bounded lease recovery |
| OB-03 | Pod dies after claim | Claim expires and another pod reclaims |
| OB-04 | Pod dies during send | Kafka transaction not reported successful; item later reclaims |
| OB-05 | Send fails conclusively | Abort transaction; durable retry |
| OB-06 | Commit fails conclusively | Do not acknowledge outbox; durable retry |
| OB-07 | Commit response lost | Mark AMBIGUOUS when possible; no auto replay |
| OB-08 | Pod dies after Kafka commit before ack | Duplicate logical delivery possible; stable publishId required |
| OB-09 | Outbox store unavailable | Stop claims; alarm; do not discard events |
| OB-10 | Poison payload/topic/ACL | Quarantine to DEAD after policy; alert |
| OB-11 | Claim lease expires while worker still running | Conditional acknowledgement must fail for stale owner |
| OB-12 | Two pods claim same candidate concurrently | Store adapter guarantees only one valid ownership token |
| OB-13 | 100k backlog after Kafka recovery | Drain in bounded transactions; no single giant transaction |
| OB-14 | Producer fenced | Pool evicts/rebuilds producer; outbox record remains durable |
| OB-15 | Pool below min healthy producers | Pause/reduce draining according to pool health and backlog policy |

## 22. Security

- Do not log outbox payloads.
- Encrypt datastore and Kafka connections according to deployment policy.
- Do not store credentials in outbox rows.
- Treat outbox retention as business-data retention because payloads may contain sensitive content.
- Support payload encryption by the application before insertion if required.

## 23. Acceptance Criteria

The feature is acceptable when:

1. Existing producer-pool users see no API behavior change when outbox is disabled.
2. Two publisher instances can drain one test store without a valid record being concurrently owned.
3. A killed publisher's claims become reclaimable after lease expiry.
4. Kafka outage produces backlog rather than record loss.
5. Recovery drains backlog in bounded Kafka transactions.
6. Commit ambiguity never triggers automatic replay.
7. Crash after Kafka commit/before outbox ack is demonstrated and documented as a possible duplicate window.
8. Stable `publishId` survives all retries.
9. Metrics expose backlog count and oldest age.
10. Chaos tests cover OB-01 through OB-15 where technically reproducible.
11. A fake/in-memory `OutboxStore` supports deterministic unit tests.
12. At least one reference adapter demonstrates database-specific claim semantics without adding that database driver to the core artifact.

## 24. Open Decisions

- Which reference adapter should be implemented first: JDBC, VoltDB, or DataStax.
- Whether embedded publisher execution belongs in the main artifact or an `outbox` module.
- Whether outbox batches may span topics.
- Whether aggregate ordering gates are mandatory or optional.
- Whether reconciliation of `AMBIGUOUS` records belongs in this repository or an application-level repair/EDR service.
