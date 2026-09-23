# Implementation Plan: Durable Outbox Publisher

## 1. Recommended Shape

Implement the outbox as an optional module layered above `TransactionalProducerPool`.

Do not modify the core pool to know about VoltDB, DataStax, SQL, EDR, or application business transactions.

Proposed package layout:

```text
src/main/java/com/kafka/producer/pool/
    existing producer-pool implementation

src/main/java/com/kafka/producer/outbox/
    OutboxPublisher.java
    OutboxConfig.java
    OutboxStore.java
    OutboxRecord.java
    ClaimedOutboxRecord.java
    OutboxState.java
    OutboxRetryPolicy.java
    OutboxPublishResult.java
    OutboxMetrics.java
    ClaimToken.java

src/test/java/com/kafka/producer/outbox/
    InMemoryOutboxStore.java
    OutboxPublisherTest.java
    OutboxClaimRecoveryTest.java
    OutboxAmbiguousCommitTest.java
    OutboxCrashWindowTest.java
```

If dependency isolation is preferred, move `com.kafka.producer.outbox` into a separate Maven module after the first implementation stabilizes.

## 2. Phase 1 - Contracts First

Implement data types and interfaces only.

### Tasks

- Add `OutboxState`.
- Add immutable `OutboxRecord`.
- Add `ClaimToken` containing owner, lease expiry, and a monotonically changing/version value where the store supports it.
- Add `OutboxStore` SPI.
- Add `OutboxRetryPolicy`.
- Add `OutboxConfig`.
- Add `OutboxPublishResult`.
- Document stable `publishId` requirement.
- Document conditional state updates by claim ownership.

### Exit criteria

The contracts can represent pending, claimed, retry, published, ambiguous, and dead states without adding a database dependency.

## 3. Phase 2 - Deterministic In-Memory Store

Implement `InMemoryOutboxStore` for tests only.

It must model atomic claim, claim lease, lease expiry, stale-owner rejection, retry scheduling, ambiguity, and dead state.

Tests:

- one worker claims item,
- second worker cannot claim same live lease,
- second worker can reclaim after expiry,
- stale first worker cannot mark the reclaimed item published,
- retry does not become visible until `availableAt`,
- ambiguous item cannot be claimed,
- dead item cannot be claimed.

## 4. Phase 3 - Publisher Loop

Implement `OutboxPublisher` using the current `TransactionalProducerPool.executeInTransaction` callback API.

```java
while (running) {
    if (!poolUsable()) {
        sleepWithBackoff();
        continue;
    }

    List<ClaimedOutboxRecord> batch =
        store.claimBatch(workerId, config.batchSize(), config.leaseDuration(), clock.instant());

    if (batch.isEmpty()) {
        sleep(config.pollInterval());
        continue;
    }

    try {
        pool.executeInTransaction(lease -> {
            for (ClaimedOutboxRecord item : batch) {
                lease.send(toProducerRecord(item));
            }
            return null;
        }, executionOptions(batch));

        store.markPublished(workerId, receipts(batch), clock.instant());

    } catch (AmbiguousTransactionException e) {
        store.markAmbiguous(workerId, ambiguities(batch, e));
        alertAmbiguous(batch, e);

    } catch (Exception e) {
        FailureClass c = classify(e);
        applyDurableFailurePolicy(batch, c, e);
    }
}
```

Do not create a second Kafka producer implementation for the outbox.

## 5. Phase 4 - Outcome Integration

The current pool should expose enough structured outcome information for the outbox to distinguish committed, safe/conclusive failure, abort-required failure, fatal producer failure, and ambiguous commit.

If current exceptions are too generic, add a backward-compatible classified result/exception type instead of parsing exception strings.

Candidate:

```java
enum PublishCertainty {
    COMMITTED,
    NOT_COMMITTED,
    AMBIGUOUS
}
```

Do not change existing callback semantics for current users.

## 6. Phase 5 - Claim Renewal

Large batches may approach the claim lease duration.

Add optional lease heartbeat:

```text
leaseDuration = 60 s
renew every    = 20 s
```

Renew only records still owned by the worker.

If renewal fails:

- stop adding new work to that batch,
- do not acknowledge using a stale claim,
- finish/abort Kafka transaction according to current transaction safety,
- surface a critical ownership warning.

Prefer sizing batches so renewal is uncommon.

## 7. Phase 6 - Metrics and Health

Extend Micrometer support with:

```text
outbox_backlog_records
outbox_oldest_record_age_seconds
outbox_claimed_records
outbox_publish_batch_total
outbox_publish_records_total
outbox_retry_total
outbox_ambiguous_total
outbox_dead_total
outbox_claim_expired_total
outbox_store_operation_total
```

Add thresholds for warning backlog age, critical backlog age, ambiguous count, and store availability.

Pool liveness and outbox health must remain distinct.

Kafka outage should generally degrade readiness/draining, not kill the process immediately.

## 8. Phase 7 - Reference Persistence Adapter

Implement one production-grade adapter only after the SPI and correctness tests are stable.

### Option A - VoltDB first

Good fit when the application already performs its business mutation in VoltDB.

Implement stored procedures for insert business change + outbox record, claim pending batch, renew claim, mark published, mark retry, mark ambiguous, and mark dead.

### Option B - DataStax first

Use time bucket + shard partitions.

Example logical table:

```text
outbox_by_bucket (
    bucket text,
    available_at timestamp,
    publish_id uuid,
    state text,
    claimed_by text,
    claim_until timestamp,
    topic text,
    key blob,
    value blob,
    ...
    PRIMARY KEY ((bucket), available_at, publish_id)
)
```

Do not rely on full-table scans or `ALLOW FILTERING`.

Use LWT only for claim ownership transitions where required; benchmark contention before adopting it broadly.

A second table keyed by `publish_id` may be needed for direct lookup/reconciliation.

### Recommendation

For this repository, start with an in-memory deterministic adapter plus one simple JDBC reference adapter if the goal is a generally reusable library.

For the rerating deployment, implement VoltDB and DataStax adapters in the owning application/repository because their data models and business transaction boundaries are application-specific.

## 9. Phase 8 - Crash/Fault Tests

Extend the existing chaos harness with:

```text
OB-01 Kafka unavailable before claim
OB-02 Kafka unavailable after claim
OB-03 kill pod after claim
OB-04 kill pod during send
OB-05 inject send failure
OB-06 conclusive commit failure
OB-07 drop commit response
OB-08 kill immediately after commit before markPublished
OB-09 make OutboxStore unavailable
OB-10 invalid topic / ACL / poison payload
OB-11 force claim expiry during processing
OB-12 concurrent claim race from two publishers
OB-13 recover with 100k pending rows
OB-14 fence active producer
OB-15 pool falls below min healthy producer threshold
```

For OB-08, assert duplicate possibility explicitly rather than writing a test that incorrectly expects exactly once.

## 10. Phase 9 - Consumer Dedup Reference

Add a small example demonstrating:

```text
publish.id = stable UUID
consumer receives event
consumer checks processed_publish_id
if seen -> no-op
if new  -> apply side effect and persist publish.id
```

This example should make clear that Kafka producer idempotence is not a replacement for application-level dedup across a DB/Kafka acknowledgement gap.

## 11. Phase 10 - EDR / Saga Integration Boundary

Do not make EDR a mandatory dependency of the outbox module.

Provide hooks/events:

```java
OutboxListener {
    onPublished(...)
    onRetryScheduled(...)
    onAmbiguous(...)
    onDead(...)
}
```

The rerating application can translate:

```text
AMBIGUOUS -> create/update EDR repair work order
DEAD      -> create/update EDR manual intervention work order
```

Outbox handles delivery state.

EDR handles business repair/reconciliation.

Saga handles compensation.

## 12. Suggested Public API

```java
OutboxPublisher publisher =
    OutboxPublisher.builder()
        .store(store)
        .producerPool(pool)
        .config(OutboxConfig.builder()
            .workerId(instanceId)
            .batchSize(1000)
            .leaseDuration(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(500))
            .build())
        .meterRegistry(meterRegistry)
        .listener(listener)
        .build();

publisher.start();
...
publisher.shutdown();
```

Prefer a start/stop lifecycle separate from `TransactionalProducerPool.initialize()` so applications may initialize their database layer and Kafka layer independently.

## 13. Compatibility

- `outbox.enabled=false` by default.
- No behavior change to `TransactionalProducerPool`.
- No database client dependency in the core artifact.
- Existing performance and chaos profiles continue to run unchanged.
- New tests/profile may be added under `outbox` or `chaos`.

## 14. Documentation Work

Update README after implementation with a "When to use Outbox" section, architecture diagram, stable `publishId` rule, acknowledgement-gap warning, example publisher, multi-pod claim behavior, distinction between Outbox/Saga/EDR, and a link to Spec 008.

Update Spec 003 wording so "durable outbox or ledger" links to Spec 008.

## 15. Implementation Order

1. Spec and API contracts.
2. In-memory store.
3. Publisher loop.
4. Structured Kafka outcome classification.
5. Unit tests.
6. Metrics.
7. Claim renewal.
8. Chaos tests.
9. Reference persistence adapter.
10. README and operational runbook.
11. EDR integration example.
12. Performance/backlog recovery benchmark.

## 16. Definition of Done

- All unit tests pass.
- Existing producer-pool tests remain unchanged/passing.
- Multi-pod claim race is deterministic in tests.
- Ambiguous commit cannot auto-retry.
- Crash-after-commit window is documented and demonstrated.
- 100k-record backlog drains in bounded transactions.
- Metrics expose backlog age and ambiguous records.
- No mandatory new database is introduced.
- Core producer-pool artifact remains datastore-agnostic.
