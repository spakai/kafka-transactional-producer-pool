package com.kafka.producer.chaos;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CorrectnessVerifier {
    private final ChaosConfig config;

    public CorrectnessVerifier(ChaosConfig config) {
        this.config = config;
    }

    public VerificationReport verify(String runId, PublishLedger ledger) {
        List<PublishLedger.Entry> ledgerEntries = new ArrayList<>(ledger.entries());
        Map<String, ObservedPublish> observed = readCommitted(runId, ledgerEntries);
        Map<String, PublishLedger.Entry> byId = ledgerEntries.stream()
                .collect(Collectors.toMap(PublishLedger.Entry::publishId, Function.identity()));

        List<String> issues = new ArrayList<>();
        List<VerificationReport.VerificationRow> rows = new ArrayList<>();
        int missingCommitted = 0;
        int visibleFailed = 0;
        int duplicates = 0;
        int partial = 0;

        for (PublishLedger.Entry entry : ledgerEntries) {
            ObservedPublish observedPublish =
                    observed.getOrDefault(entry.publishId(), ObservedPublish.EMPTY);
            int count = observedPublish.totalRecords;
            if (entry.outcome() == PublishLedger.Outcome.COMMITTED
                    && count != entry.expectedRecords()) {
                missingCommitted++;
                issues.add("Committed publish " + entry.publishId() + " expected "
                        + entry.expectedRecords() + " records but observed " + count);
            }
            if (entry.outcome() == PublishLedger.Outcome.FAILED && count > 0) {
                visibleFailed++;
                issues.add("Conclusive failure is visible: " + entry.publishId());
            }
            if (count > entry.expectedRecords() || observedPublish.duplicateRecordIndexes > 0) {
                duplicates++;
                issues.add("Duplicate records for publish " + entry.publishId());
            } else if (count > 0 && count < entry.expectedRecords()) {
                partial++;
                issues.add("Partial transaction visible for publish " + entry.publishId());
            }
            rows.add(new VerificationReport.VerificationRow(
                    entry.publishId(), entry.outcome().name(), entry.expectedRecords(), count,
                    entry.callbackAttempts(), entry.errorClass()));
        }

        int orderingViolations = orderingViolations(observed, byId, issues);
        long observedRecords = observed.values().stream()
                .mapToLong(value -> value.totalRecords).sum();
        return new VerificationReport(
                ledgerEntries.size(), observed.size(), observedRecords,
                missingCommitted, visibleFailed, duplicates, partial,
                orderingViolations, issues, rows);
    }

    private Map<String, ObservedPublish> readCommitted(
            String runId, List<PublishLedger.Entry> ledgerEntries) {
        if (ledgerEntries.isEmpty()) {
            return Map.of();
        }
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "chaos-verifier-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(100 * 1024 * 1024));
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(10 * 1024 * 1024));

        Map<String, ObservedPublish> result = new HashMap<>();
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            List<PartitionInfo> infos = consumer.partitionsFor(config.topic(), Duration.ofSeconds(30));
            List<TopicPartition> partitions = infos.stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            consumer.assign(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            seekToExperimentWindow(consumer, partitions, endOffsets, ledgerEntries);

            long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
            while (!reachedEnd(consumer, endOffsets)) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException(
                            "Timed out reading committed records through captured end offsets: "
                                    + remainingOffsets(consumer, endOffsets));
                }
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (!runId.equals(header(record, "chaos.run-id"))) {
                        continue;
                    }
                    String publishId = header(record, "publish.id");
                    String sequence = header(record, "publish.sequence");
                    String index = header(record, "publish.record-index");
                    if (publishId == null || sequence == null || index == null) {
                        continue;
                    }
                    String key = record.key() == null
                            ? "" : new String(record.key(), StandardCharsets.UTF_8);
                    ObservedPublish observed = result.computeIfAbsent(publishId,
                            ignored -> new ObservedPublish(key, Long.parseLong(sequence)));
                    observed.totalRecords++;
                    if (!observed.recordIndexes.add(Integer.parseInt(index))) {
                        observed.duplicateRecordIndexes++;
                    }
                    observed.firstOffset = Math.min(observed.firstOffset, record.offset());
                }
            }
        }
        return result;
    }

    private static void seekToExperimentWindow(
            Consumer<byte[], byte[]> consumer,
            List<TopicPartition> partitions,
            Map<TopicPartition, Long> endOffsets,
            List<PublishLedger.Entry> ledgerEntries) {
        long firstAttemptMillis = ledgerEntries.stream()
                .map(PublishLedger.Entry::attemptedAt)
                .min(Instant::compareTo)
                .orElseThrow()
                .minusSeconds(5)
                .toEpochMilli();
        Map<TopicPartition, Long> timestamps = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), ignored -> firstAttemptMillis));
        Map<TopicPartition, OffsetAndTimestamp> offsets =
                consumer.offsetsForTimes(timestamps, Duration.ofSeconds(30));
        for (TopicPartition partition : partitions) {
            OffsetAndTimestamp offset = offsets.get(partition);
            consumer.seek(partition, offset == null ? endOffsets.get(partition) : offset.offset());
        }
    }

    private static boolean reachedEnd(
            Consumer<byte[], byte[]> consumer, Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> end : endOffsets.entrySet()) {
            if (consumer.position(end.getKey()) < end.getValue()) {
                return false;
            }
        }
        return true;
    }

    private static Map<TopicPartition, String> remainingOffsets(
            Consumer<byte[], byte[]> consumer, Map<TopicPartition, Long> endOffsets) {
        Map<TopicPartition, String> remaining = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> end : endOffsets.entrySet()) {
            long position = consumer.position(end.getKey());
            if (position < end.getValue()) {
                remaining.put(end.getKey(), position + "/" + end.getValue());
            }
        }
        return remaining;
    }

    private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int orderingViolations(
            Map<String, ObservedPublish> observed,
            Map<String, PublishLedger.Entry> ledger,
            List<String> issues) {
        Map<String, List<ObservedPublish>> byKey = observed.entrySet().stream()
                .filter(entry -> ledger.containsKey(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(Collectors.groupingBy(value -> value.key));
        int violations = 0;
        for (Map.Entry<String, List<ObservedPublish>> keyEntry : byKey.entrySet()) {
            List<Long> sequence = keyEntry.getValue().stream()
                    .sorted(Comparator.comparingLong(value -> value.firstOffset))
                    .map(value -> value.sequence)
                    .toList();
            long previous = Long.MIN_VALUE;
            for (long current : sequence) {
                if (current < previous) {
                    violations++;
                    issues.add("Ordering inversion for key " + keyEntry.getKey()
                            + ": " + previous + " before " + current);
                }
                previous = current;
            }
        }
        return violations;
    }

    private static final class ObservedPublish {
        private static final ObservedPublish EMPTY = new ObservedPublish("", -1);
        private final String key;
        private final long sequence;
        private final Set<Integer> recordIndexes = new HashSet<>();
        private int totalRecords;
        private int duplicateRecordIndexes;
        private long firstOffset = Long.MAX_VALUE;

        private ObservedPublish(String key, long sequence) {
            this.key = key;
            this.sequence = sequence;
        }
    }
}
