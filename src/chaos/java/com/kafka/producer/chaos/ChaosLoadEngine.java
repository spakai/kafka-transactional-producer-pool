package com.kafka.producer.chaos;

import com.kafka.producer.pool.ExecutionOptions;
import com.kafka.producer.pool.PoolConfig;
import com.kafka.producer.pool.TransactionalProducerPool;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public final class ChaosLoadEngine implements AutoCloseable {
    private final ChaosConfig config;
    private final String runId;
    private final PublishLedger ledger;
    private final TransactionalProducerPool pool;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final LongAdder attempted = new LongAdder();
    private final LongAdder committed = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder aborted = new LongAdder();
    private final LongAdder ambiguous = new LongAdder();
    private final LatencyWindow latencyNanos = new LatencyWindow();
    private final List<Future<?>> workers = new ArrayList<>();
    private final ExecutorService executor;
    private volatile boolean started;

    public ChaosLoadEngine(ChaosConfig config, String runId, PublishLedger ledger) {
        this.config = config;
        this.runId = runId;
        this.ledger = ledger;
        this.pool = new TransactionalProducerPool(
                poolConfig(config, runId),
                new com.kafka.producer.pool.DefaultKafkaProducerFactory(),
                new SimpleMeterRegistry());
        this.executor = Executors.newFixedThreadPool(config.threads());
    }

    public void start() {
        if (started) {
            throw new IllegalStateException("Load engine already started");
        }
        pool.initialize();
        started = true;
        for (int worker = 0; worker < config.threads(); worker++) {
            int workerId = worker;
            workers.add(executor.submit(() -> runWorker(workerId)));
        }
    }

    private void runWorker(int workerId) {
        byte[] key = ("worker-" + workerId).getBytes(StandardCharsets.UTF_8);
        byte[] payload = "x".repeat(Math.max(1, config.recordSizeBytes()))
                .getBytes(StandardCharsets.UTF_8);
        long sequence = 0;
        while (!stop.get()) {
            String publishId = UUID.randomUUID().toString();
            long publishSequence = sequence++;
            PublishLedger.Entry entry = ledger.attempted(
                    publishId, new String(key, StandardCharsets.UTF_8),
                    publishSequence, config.recordsPerTransaction() * config.outputTopics().size());
            attempted.increment();
            long startedAt = System.nanoTime();
            try {
                pool.executeInTransaction(lease -> {
                    entry.recordCallbackAttempt();
                    Integer partition = selectedPartition(publishSequence);
                    for (String outputTopic : config.outputTopics()) {
                        for (int index = 0; index < config.recordsPerTransaction(); index++) {
                            ProducerRecord<byte[], byte[]> record = partition == null
                                    ? new ProducerRecord<>(outputTopic, key, payload)
                                    : new ProducerRecord<>(outputTopic, partition, key, payload);
                            addHeader(record, "chaos.run-id", runId);
                            addHeader(record, "chaos.scenario", config.scenario());
                            addHeader(record, "publish.id", publishId);
                            addHeader(record, "publish.attempt",
                                    String.valueOf(entry.callbackAttempts()));
                            addHeader(record, "publish.sequence", String.valueOf(publishSequence));
                            addHeader(record, "publish.record-index", String.valueOf(index));
                            lease.send(record);
                        }
                    }
                    if (config.abortEvery() > 0
                            && (publishSequence + 1) % config.abortEvery() == 0) {
                        throw new IntentionalAbortException("configured abort sample");
                    }
                    return null;
                }, ExecutionOptions.builder()
                        .leaseTimeoutMs(config.leaseTimeoutMs())
                        .correlationId(publishId)
                        .build());
                entry.complete(PublishLedger.Outcome.COMMITTED, null);
                committed.increment();
            } catch (RuntimeException error) {
                if (hasCause(error, IntentionalAbortException.class)) {
                    entry.complete(PublishLedger.Outcome.ABORTED, error);
                    aborted.increment();
                } else if (hasCause(error, TimeoutException.class)) {
                    entry.complete(PublishLedger.Outcome.AMBIGUOUS, error);
                    ambiguous.increment();
                } else {
                    entry.complete(PublishLedger.Outcome.FAILED, error);
                    failed.increment();
                }
            } finally {
                latencyNanos.record(System.nanoTime() - startedAt);
            }
        }
    }

    private static void addHeader(
            ProducerRecord<byte[], byte[]> record, String name, String value) {
        record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
    }

    private Integer selectedPartition(long publishSequence) {
        if (config.scenario().equals("MB-03") || config.scenario().equals("MB-04")) {
            return config.targetPartition();
        }
        if (config.scenario().equals("MB-02")) {
            return (int) (publishSequence % config.partitionCount());
        }
        return null;
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    public Sample sample(long elapsedSecond, ClusterInspector.ClusterSnapshot cluster) {
        LatencyWindow.Snapshot interval = latencyNanos.snapshotAndReset();
        long heap = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return new Sample(
                Instant.now(),
                elapsedSecond,
                attempted.sum(),
                committed.sum(),
                failed.sum(),
                aborted.sum(),
                ambiguous.sum(),
                interval.percentileMillis(50),
                interval.percentileMillis(95),
                interval.percentileMillis(99),
                pool.getPoolState().name(),
                pool.getReadyCount(),
                pool.getLeasedCount(),
                pool.getTotalCount(),
                heap / (1024.0 * 1024.0),
                cluster == null ? -1 : cluster.underReplicatedPartitions(),
                cluster == null ? -1 : cluster.offlinePartitions());
    }

    public void stopAndAwait() throws Exception {
        if (!started || stop.getAndSet(true)) {
            return;
        }
        executor.shutdown();
        if (!executor.awaitTermination(
                Math.max(15_000, config.leaseTimeoutMs() * 3), TimeUnit.MILLISECONDS)) {
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Chaos load workers did not terminate");
            }
        }
        for (Future<?> worker : workers) {
            worker.get();
        }
        pool.shutdown();
    }

    @Override
    public void close() throws Exception {
        stopAndAwait();
    }

    private static PoolConfig poolConfig(ChaosConfig config, String runId) {
        Properties kafka = new Properties();
        kafka.setProperty("bootstrap.servers", config.bootstrapServers());
        kafka.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafka.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafka.setProperty("acks", "all");
        kafka.setProperty("request.timeout.ms", "10000");
        kafka.setProperty("delivery.timeout.ms", "30000");
        kafka.setProperty("max.block.ms", "15000");

        return PoolConfig.builder()
                .poolSize(config.poolSize())
                .minHealthyProducers(Math.max(1, config.poolSize() / 2))
                .leaseTimeoutMs(config.leaseTimeoutMs())
                .leaseHardTimeoutMs(Math.max(30_000, config.leaseTimeoutMs() * 3))
                .shutdownGracePeriodMs(15_000)
                .retryMaxAttempts(2)
                .retryBaseDelayMs(100)
                .retryMaxDelayMs(2_000)
                .recoveryMaxConcurrentRebuilds(Math.max(1, config.poolSize() / 4))
                .serviceIdentity("chaos")
                .instanceIdentifier(runId.substring(0, Math.min(12, runId.length())))
                .kafkaProperties(kafka)
                .build();
    }

    public record Sample(
            Instant timestamp,
            long elapsedSecond,
            long attempted,
            long committed,
            long failed,
            long aborted,
            long ambiguous,
            double p50LatencyMs,
            double p95LatencyMs,
            double p99LatencyMs,
            String poolState,
            int readyProducers,
            int leasedProducers,
            int totalProducers,
            double heapUsedMb,
            int underReplicatedPartitions,
            int offlinePartitions) {}

    private static final class IntentionalAbortException extends RuntimeException {
        private IntentionalAbortException(String message) { super(message); }
    }
}
