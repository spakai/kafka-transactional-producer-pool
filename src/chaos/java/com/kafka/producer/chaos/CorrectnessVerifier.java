package com.kafka.producer.chaos;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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

/** Verifies transaction visibility against the run ledger in a captured offset window. */
public final class CorrectnessVerifier {
    private final ChaosConfig config;

    public CorrectnessVerifier(ChaosConfig config) { this.config = config; }

    /** Capture high-water offsets before load starts. Use a disposable, isolated test topic. */
    public Map<TopicPartition, Long> captureStartOffsets() {
        try (Consumer<byte[], byte[]> consumer = newConsumer("read_uncommitted")) {
            List<TopicPartition> partitions = partitions(consumer);
            consumer.assign(partitions);
            return Map.copyOf(consumer.endOffsets(partitions, Duration.ofSeconds(30)));
        }
    }

    /** Capture high-water offsets after workers stop and all callbacks have drained. */
    public Map<TopicPartition, Long> captureEndOffsets() {
        return captureStartOffsets();
    }

    public VerificationReport verify(String runId, PublishLedger ledger,
            Map<TopicPartition, Long> startOffsets, Map<TopicPartition, Long> endOffsets) {
        List<PublishLedger.Entry> entries = new ArrayList<>(ledger.entries());
        ReadResult committedRead = read(runId, startOffsets, endOffsets, "read_committed");
        ReadResult uncommittedRead = read(runId, startOffsets, endOffsets, "read_uncommitted");
        Map<String, ObservedPublish> committed = committedRead.publishes;
        Map<String, ObservedPublish> uncommitted = uncommittedRead.publishes;
        Map<String, PublishLedger.Entry> byId = entries.stream()
                .collect(Collectors.toMap(PublishLedger.Entry::publishId, Function.identity()));
        List<String> issues = new ArrayList<>();
        List<VerificationReport.VerificationRow> rows = new ArrayList<>();
        int missing = 0, visibleFailed = 0, duplicates = 0, partial = 0;
        int abortedCount = 0, abortedCommitted = 0, abortedUncommittedMismatch = 0;
        int missingAbortControlSamples = 0, crossTopic = 0, replay = 0, pending = 0;
        long abortedUncommittedRecords = 0, observedRecords = 0;

        for (PublishLedger.Entry entry : entries) {
            ObservedPublish rc = committed.getOrDefault(entry.publishId(), new ObservedPublish());
            ObservedPublish ru = uncommitted.getOrDefault(entry.publishId(), new ObservedPublish());
            int committedCount = rc.recordCount();
            int uncommittedCount = ru.recordCount();
            observedRecords += committedCount;
            int perTopic = config.recordsPerTransaction();
            boolean topicSetComplete = config.outputTopics().stream()
                    .allMatch(topic -> rc.count(topic) == perTopic);
            boolean topicSetEmpty = config.outputTopics().stream()
                    .allMatch(topic -> rc.count(topic) == 0);

            if (entry.outcome() == PublishLedger.Outcome.COMMITTED && committedCount != entry.expectedRecords()) {
                missing++;
                issues.add("Committed publish " + entry.publishId() + " expected "
                        + entry.expectedRecords() + " committed records but observed " + committedCount);
            }
            if (entry.outcome() == PublishLedger.Outcome.FAILED && committedCount > 0) {
                visibleFailed++;
                issues.add("Conclusive failure is visible: " + entry.publishId());
            }
            if (entry.outcome() == PublishLedger.Outcome.ABORTED) {
                abortedCount++;
                if (!topicSetEmpty) {
                    abortedCommitted++;
                    issues.add("Aborted publish is visible to read_committed: " + entry.publishId());
                }
                abortedUncommittedRecords += uncommittedCount;
                if (uncommittedCount != entry.expectedRecords()) {
                    abortedUncommittedMismatch++;
                    issues.add("Aborted publish " + entry.publishId() + " expected "
                            + entry.expectedRecords() + " read_uncommitted records but observed " + uncommittedCount);
                }
            }
            if (entry.outcome() == PublishLedger.Outcome.AMBIGUOUS
                    && committedCount > 0 && !topicSetComplete && !topicSetEmpty) {
                partial++;
                issues.add("Ambiguous publish is partially visible across topics: " + entry.publishId());
            }
            if ((entry.outcome() == PublishLedger.Outcome.COMMITTED
                    || entry.outcome() == PublishLedger.Outcome.AMBIGUOUS)
                    && committedCount > 0 && !topicSetComplete) {
                crossTopic++;
                issues.add("Cross-topic record set incomplete for publish " + entry.publishId());
            }
            if (committedCount > entry.expectedRecords() || rc.hasDuplicateIndexes()) {
                duplicates++;
                issues.add("Duplicate records for publish " + entry.publishId());
            } else if (committedCount > 0 && committedCount < entry.expectedRecords()
                    && entry.outcome() != PublishLedger.Outcome.AMBIGUOUS) {
                partial++;
                issues.add("Partial transaction visible for publish " + entry.publishId());
            }
            if (entry.outcome() == PublishLedger.Outcome.AMBIGUOUS && entry.callbackAttempts() > 1) {
                replay++;
                issues.add("Ambiguous transaction callback replayed " + entry.callbackAttempts()
                        + " times: " + entry.publishId());
            }
            if (entry.outcome() == PublishLedger.Outcome.ATTEMPTED) {
                pending++;
                issues.add("Publish remained unresolved after drain: " + entry.publishId());
            }
            rows.add(new VerificationReport.VerificationRow(entry.publishId(), entry.outcome().name(),
                    entry.expectedRecords(), committedCount, uncommittedCount,
                    entry.callbackAttempts(), entry.errorClass()));
        }

        int ordering = orderingViolations(committed, byId, issues);
        if (config.scenario().equals("MB-01") && config.abortEvery() > 0 && abortedCount == 0) {
            missingAbortControlSamples++;
            issues.add("MB-01 configured an aborted-transaction control sample but observed none");
        }
        int underSampled = 0;
        if (config.scenario().equals("MB-02")) {
            for (String topic : config.outputTopics()) for (int partition = 0;
                    partition < config.partitionCount(); partition++) {
                TopicPartition tp = new TopicPartition(topic, partition);
                if (committedRead.recordsByPartition.getOrDefault(tp, 0L) < 1) underSampled++;
            }
            if (underSampled > 0) issues.add("MB-02 has " + underSampled + " under-sampled partitions");
        }
        Map<TopicPartition, VerificationReport.OffsetRange> ranges = new HashMap<>();
        for (var entry : startOffsets.entrySet()) {
            ranges.put(entry.getKey(), new VerificationReport.OffsetRange(entry.getValue(),
                    endOffsets.getOrDefault(entry.getKey(), entry.getValue())));
        }
        return new VerificationReport(entries.size(), committed.size(), observedRecords,
                missing, visibleFailed, duplicates, partial, ordering, abortedCount,
                abortedCommitted, abortedUncommittedRecords, abortedUncommittedMismatch,
                missingAbortControlSamples, crossTopic, replay, pending, ranges, committedRead.recordsByPartition,
                underSampled, issues, rows);
    }

    /** Compatibility entry point; new runners must pass captured offset boundaries. */
    public VerificationReport verify(String runId, PublishLedger ledger) {
        Map<TopicPartition, Long> start;
        try (Consumer<byte[], byte[]> consumer = newConsumer("read_uncommitted")) {
            List<TopicPartition> parts = partitions(consumer);
            consumer.assign(parts);
            start = Map.copyOf(consumer.beginningOffsets(parts, Duration.ofSeconds(30)));
        }
        Map<TopicPartition, Long> end = captureEndOffsets();
        return verify(runId, ledger, start, end);
    }

    private ReadResult read(String runId, Map<TopicPartition, Long> starts,
            Map<TopicPartition, Long> ends, String isolation) {
        Map<String, ObservedPublish> result = new HashMap<>();
        Map<TopicPartition, Long> recordsByPartition = new HashMap<>();
        try (Consumer<byte[], byte[]> consumer = newConsumer(isolation)) {
            List<TopicPartition> partitions = partitions(consumer);
            consumer.assign(partitions);
            partitions.forEach(tp -> recordsByPartition.put(tp, 0L));
            Map<TopicPartition, Long> boundedEnds = new HashMap<>();
            for (TopicPartition tp : partitions) {
                long start = starts.getOrDefault(tp, 0L);
                long end = ends.getOrDefault(tp, start);
                boundedEnds.put(tp, end);
                consumer.seek(tp, start);
            }
            long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
            while (!reachedEnd(consumer, boundedEnds)) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Timed out reading " + isolation
                            + " through captured offsets: " + remaining(consumer, boundedEnds));
                }
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.offset() >= boundedEnds.getOrDefault(
                            new TopicPartition(record.topic(), record.partition()), 0L)) continue;
                    if (!runId.equals(header(record, "chaos.run-id"))) continue;
                    TopicPartition recordPartition = new TopicPartition(record.topic(), record.partition());
                    recordsByPartition.merge(recordPartition, 1L, Long::sum);
                    String id = header(record, "publish.id");
                    String sequence = header(record, "publish.sequence");
                    String index = header(record, "publish.record-index");
                    if (id == null || sequence == null || index == null) continue;
                    String key = record.key() == null ? "" : new String(record.key(), StandardCharsets.UTF_8);
                    ObservedPublish observed = result.computeIfAbsent(id, ignored -> new ObservedPublish());
                    observed.add(record.topic(), record.partition(), key, Long.parseLong(sequence),
                            Integer.parseInt(index), record.offset());
                }
            }
        }
        return new ReadResult(result, recordsByPartition);
    }

    private Consumer<byte[], byte[]> newConsumer(String isolation) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "chaos-verifier-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
        return new KafkaConsumer<>(p);
    }

    private List<TopicPartition> partitions(Consumer<byte[], byte[]> consumer) {
        List<TopicPartition> result = new ArrayList<>();
        for (String topic : config.outputTopics()) {
            List<PartitionInfo> info = consumer.partitionsFor(topic, Duration.ofSeconds(30));
            if (info == null || info.isEmpty()) throw new IllegalStateException("No partitions for " + topic);
            info.stream().map(p -> new TopicPartition(topic, p.partition())).forEach(result::add);
        }
        return result;
    }

    private static boolean reachedEnd(Consumer<byte[], byte[]> consumer, Map<TopicPartition, Long> ends) {
        for (var end : ends.entrySet()) if (consumer.position(end.getKey()) < end.getValue()) return false;
        return true;
    }

    private static Map<TopicPartition, String> remaining(Consumer<byte[], byte[]> consumer,
            Map<TopicPartition, Long> ends) {
        Map<TopicPartition, String> result = new HashMap<>();
        for (var end : ends.entrySet()) {
            long pos = consumer.position(end.getKey());
            if (pos < end.getValue()) result.put(end.getKey(), pos + "/" + end.getValue());
        }
        return result;
    }

    private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static int orderingViolations(Map<String, ObservedPublish> observed,
            Map<String, PublishLedger.Entry> ledger, List<String> issues) {
        Map<String, List<OrderedPublish>> byTopicAndKey = new HashMap<>();
        for (var entry : observed.entrySet()) {
            if (!ledger.containsKey(entry.getKey())) continue;
            entry.getValue().firstOffsets.forEach((topic, offset) -> {
                String[] topicPartition = topic.split("\u0000", -1);
                String actualTopic = topicPartition[0];
                int partition = Integer.parseInt(topicPartition[1]);
                String key = entry.getValue().keys.get(topic);
                long seq = entry.getValue().sequences.get(topic);
                byTopicAndKey.computeIfAbsent(actualTopic + "\u0000" + partition + "\u0000" + key,
                                ignored -> new ArrayList<>())
                        .add(new OrderedPublish(offset, seq, actualTopic, partition, key));
            });
        }
        int violations = 0;
        for (List<OrderedPublish> group : byTopicAndKey.values()) {
            group.sort(Comparator.comparingLong(OrderedPublish::offset));
            long prior = Long.MIN_VALUE;
            for (OrderedPublish current : group) {
                if (current.sequence < prior) {
                    violations++;
                    issues.add("Ordering inversion on topic " + current.topic + " partition "
                            + current.partition + " key " + current.key);
                }
                prior = current.sequence;
            }
        }
        return violations;
    }

    private record OrderedPublish(long offset, long sequence, String topic, int partition, String key) {}
    private record ReadResult(Map<String, ObservedPublish> publishes,
                              Map<TopicPartition, Long> recordsByPartition) {}

    private static final class ObservedPublish {
        private final Map<String, Set<Integer>> indexes = new HashMap<>();
        private final Map<String, Long> firstOffsets = new HashMap<>();
        private final Map<String, String> keys = new HashMap<>();
        private final Map<String, Long> sequences = new HashMap<>();
        private int count(String topic) { return indexes.getOrDefault(topic, Set.of()).size(); }
        private int recordCount() { return indexes.values().stream().mapToInt(Set::size).sum(); }
        private boolean hasDuplicateIndexes() {
            // Duplicate index insertions are tracked independently below.
            return duplicateIndexes > 0;
        }
        private int duplicateIndexes;
        private void add(String topic, int partition, String key, long sequence, int index, long offset) {
            Set<Integer> set = indexes.computeIfAbsent(topic, ignored -> new HashSet<>());
            if (!set.add(index)) duplicateIndexes++;
            String topicPartition = topic + "\u0000" + partition;
            firstOffsets.merge(topicPartition, offset, Math::min);
            keys.put(topicPartition, key);
            sequences.put(topicPartition, sequence);
        }
    }
}
