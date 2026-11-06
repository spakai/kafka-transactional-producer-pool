package com.kafka.producer.chaos;

import com.kafka.producer.pool.DefaultKafkaProducerFactory;
import com.kafka.producer.pool.PoolConfig;
import com.kafka.producer.pool.TransactionalProducerPool;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = "brokerExpiry.bootstrapServers", matches = ".+")
class BrokerTransactionExpiryTest {
    private static final Logger log = LoggerFactory.getLogger(BrokerTransactionExpiryTest.class);
    private static final int TRANSACTION_TIMEOUT_MS = 3_000;

    @Test
    @Timeout(90)
    void brokerExpiresTransactionAndPoolRecoversWithoutPublishingAbortedRecords() throws Exception {
        String bootstrap = System.getProperty("brokerExpiry.bootstrapServers");
        String id = UUID.randomUUID().toString();
        String topic = "transaction-expiry-" + id;
        Properties kafka = new Properties();
        kafka.setProperty("bootstrap.servers", bootstrap);
        kafka.setProperty("transaction.timeout.ms", Integer.toString(TRANSACTION_TIMEOUT_MS));
        kafka.setProperty("max.block.ms", "10000");
        PoolConfig config = PoolConfig.builder()
                .poolSize(1)
                .serviceIdentity("expiry-test")
                .instanceIdentifier(id)
                .leaseHardTimeoutMs(60_000)
                .kafkaProperties(kafka)
                .build();

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try (Admin admin = Admin.create(Map.of(
                "bootstrap.servers", bootstrap,
                "request.timeout.ms", "5000",
                "default.api.timeout.ms", "10000"))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
            TransactionalProducerPool pool = new TransactionalProducerPool(
                    config, new DefaultKafkaProducerFactory(), registry);
            try {
                pool.initialize();
                RuntimeException failure = assertThrows(RuntimeException.class,
                        () -> pool.executeInTransaction(lease -> {
                            lease.send(new ProducerRecord<>(topic, bytes("expired")))
                                    .get(10, TimeUnit.SECONDS);
                            long sentAt = System.nanoTime();

                            // Observe the broker's abort before commit or producer close can cause it.
                            awaitBrokerAbort(admin, lease.getTransactionalId());
                            log.info("Broker confirmed COMPLETE_ABORT after {}ms; transaction.timeout.ms={}",
                                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sentAt),
                                    TRANSACTION_TIMEOUT_MS);
                            return null;
                        }));
                assertTrue(failure instanceof InvalidProducerEpochException
                                || failure instanceof ProducerFencedException,
                        () -> "Expected broker epoch/fencing rejection, not a client timeout: " + failure);
                log.info("Commit after broker expiry failed with {}", failure.getClass().getSimpleName());

                await().atMost(Duration.ofSeconds(15)).until(() -> pool.getReadyCount() == 1);
                pool.executeInTransaction(lease -> {
                    assertEquals(config.buildTransactionalId(0), lease.getTransactionalId());
                    lease.send(new ProducerRecord<>(topic, bytes("committed")))
                            .get(10, TimeUnit.SECONDS);
                    return null;
                });

                assertEquals(List.of("expired", "committed"), readToEnd(bootstrap, topic, "read_uncommitted"));
                assertEquals(List.of("committed"), readToEnd(bootstrap, topic, "read_committed"));
                assertEquals(1, pool.getReadyCount());
                assertEquals(0, pool.getLeasedCount());
                log.info("Recovery succeeded; read_uncommitted saw both records, read_committed saw only committed");
            } finally {
                pool.shutdown();
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            }
        } finally {
            registry.close();
        }
    }

    @ParameterizedTest(name = "reuse original producer, abortBeforeCommit={0}")
    @ValueSource(booleans = {false, true})
    @Timeout(90)
    void expiredProducerCannotBeReusedWithoutReplacement(
            boolean abortBeforeCommit) throws Exception {
        String bootstrap = System.getProperty("brokerExpiry.bootstrapServers");
        String id = UUID.randomUUID().toString();
        String topic = "transaction-expiry-reuse-" + id;
        String transactionalId = "expiry-reuse-" + id;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("transactional.id", transactionalId);
        properties.setProperty("transaction.timeout.ms", Integer.toString(TRANSACTION_TIMEOUT_MS));
        properties.setProperty("max.block.ms", "10000");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Admin admin = Admin.create(Map.of(
                "bootstrap.servers", bootstrap,
                "request.timeout.ms", "5000",
                "default.api.timeout.ms", "10000"))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
            try {
                try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(topic, bytes("expired")))
                            .get(10, TimeUnit.SECONDS);
                    awaitBrokerAbort(admin, transactionalId);
                    System.out.printf("abortBeforeCommit=%s: brokerState=COMPLETE_ABORT%n", abortBeforeCommit);

                    RuntimeException failure = assertThrows(RuntimeException.class, () -> {
                        if (abortBeforeCommit) {
                            producer.abortTransaction();
                        } else {
                            producer.commitTransaction();
                        }
                    });
                    assertTrue(failure instanceof InvalidProducerEpochException
                                    || failure instanceof ProducerFencedException,
                            () -> "Expected broker epoch/fencing rejection: " + failure);
                    System.out.printf("%s after expiry: %s%n",
                            abortBeforeCommit ? "abortTransaction" : "commitTransaction", failure);

                    KafkaException abortFailure = assertThrows(KafkaException.class, producer::abortTransaction);
                    KafkaException beginFailure = assertThrows(KafkaException.class, producer::beginTransaction);
                    KafkaException initFailure = assertThrows(KafkaException.class, producer::initTransactions);
                    for (KafkaException rejected : List.of(abortFailure, beginFailure, initFailure)) {
                        assertTrue(rejected instanceof ProducerFencedException
                                        || rejected instanceof InvalidProducerEpochException
                                        || rejected.getCause() instanceof ProducerFencedException
                                        || rejected.getCause() instanceof InvalidProducerEpochException,
                                () -> "Expected fatal epoch/fencing rejection or a wrapper: " + rejected);
                    }
                    System.out.printf("Reuse rejected: abortTransaction=%s; beginTransaction=%s; initTransactions=%s%n",
                            abortFailure, beginFailure, initFailure);
                    assertEquals(List.of("expired"), readToEnd(bootstrap, topic, "read_uncommitted"));
                    assertEquals(List.of(), readToEnd(bootstrap, topic, "read_committed"));
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void awaitBrokerAbort(Admin admin, String transactionalId) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> admin.describeTransactions(List.of(transactionalId))
                        .description(transactionalId)
                        .get(5, TimeUnit.SECONDS)
                        .state() == TransactionState.COMPLETE_ABORT);
    }

    @Test
    @Timeout(90)
    @EnabledIfSystemProperty(named = "brokerExpiry.testCommitTimeout", matches = "true")
    void clientCommitTimeoutAllowsSameCommitRetryAndProducerReuse() throws Exception {
        String bootstrap = System.getProperty("brokerExpiry.bootstrapServers");
        assertEquals("localhost:49092", bootstrap, "Pause test requires the dedicated disposable stack");
        String id = UUID.randomUUID().toString();
        String topic = "client-commit-timeout-" + id;
        String transactionalId = "commit-timeout-" + id;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("transactional.id", transactionalId);
        properties.setProperty("transaction.timeout.ms", "60000");
        properties.setProperty("max.block.ms", "3000");
        properties.setProperty("request.timeout.ms", "1000");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Admin admin = Admin.create(Map.of(
                "bootstrap.servers", bootstrap,
                "request.timeout.ms", "5000",
                "default.api.timeout.ms", "10000"))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
            try {
                try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(topic, bytes("first")))
                            .get(10, TimeUnit.SECONDS);
                    assertEquals(TransactionState.ONGOING, admin.describeTransactions(List.of(transactionalId))
                            .description(transactionalId).get(5, TimeUnit.SECONDS).state());
                    try {
                        controlDisposableBroker("pause");
                        long started = System.nanoTime();
                        TimeoutException failure = assertThrows(TimeoutException.class, producer::commitTransaction);
                        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                        assertTrue(elapsedMs >= 2_500, "Commit should wait for max.block.ms");
                        System.out.printf("Client commit timed out after %dms: %s%n", elapsedMs, failure);
                    } finally {
                        controlDisposableBroker("unpause");
                    }

                    // Retry only the pending commit; never replay the records.
                    await().atMost(Duration.ofSeconds(15))
                            .ignoreException(TimeoutException.class)
                            .until(() -> {
                                producer.commitTransaction();
                                return true;
                            });
                    assertEquals(TransactionState.COMPLETE_COMMIT,
                            admin.describeTransactions(List.of(transactionalId))
                                    .description(transactionalId).get(5, TimeUnit.SECONDS).state());
                    System.out.println("Same producer: retry of the original commit succeeded; brokerState=COMPLETE_COMMIT");

                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(topic, bytes("second")))
                            .get(10, TimeUnit.SECONDS);
                    producer.commitTransaction();
                    assertEquals(List.of("first", "second"), readToEnd(bootstrap, topic, "read_committed"));
                    assertEquals(List.of("first", "second"), readToEnd(bootstrap, topic, "read_uncommitted"));
                    System.out.println("Same producer: subsequent transaction committed; each record visible exactly once");
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void controlDisposableBroker(String action) throws Exception {
        Process process = new ProcessBuilder("docker", "compose", "-f",
                "docker-compose.transaction-expiry.yml", action, "kafka")
                .inheritIO().start();
        boolean finished = process.waitFor(15, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        assertTrue(finished, "Docker broker control timed out: " + action);
        assertEquals(0, process.exitValue(), "Docker broker control failed: " + action);
    }

    private static List<String> readToEnd(String bootstrap, String topic, String isolation) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("isolation.level", isolation);
        properties.setProperty("default.api.timeout.ms", "10000");
        TopicPartition partition = new TopicPartition(topic, 0);
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            assertTrue(end > 0, "Test topic must contain records");
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(record ->
                        values.add(new String(record.value(), StandardCharsets.UTF_8)));
                return consumer.position(partition) >= end;
            });
        }
        return values;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
