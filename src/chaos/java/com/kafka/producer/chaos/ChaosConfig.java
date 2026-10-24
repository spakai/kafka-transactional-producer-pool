package com.kafka.producer.chaos;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ChaosConfig {
    private final String scenario;
    private final String bootstrapServers;
    private final String topic;
    private final java.util.List<String> outputTopics;
    private final int poolSize;
    private final int threads;
    private final int recordsPerTransaction;
    private final int recordSizeBytes;
    private final int targetPartition;
    private final int partitionCount;
    private final int abortEvery;
    private final Duration duration;
    private final Duration faultAt;
    private final Duration faultDuration;
    private final Duration recoveryTimeout;
    private final long leaseTimeoutMs;
    private final boolean chaosEnabled;
    private final String allowedClusterId;
    private final Path resultsDirectory;
    private final Map<Integer, String> brokerContainers;
    private final String partitionBrokerCommand;
    private final String partitionClusterCommand;
    private final String commitResponseCommand;
    private final String captureCommitResponseCommand;
    private final String healNetworkCommand;
    private final long requestTimeoutMs;
    private final long transactionTimeoutMs;
    private final long shutdownGraceMs;
    private final long poisonIntervalSec;

    private ChaosConfig(Map<String, String> values) {
        scenario = value(values, "scenario", "MB-01").toUpperCase();
        bootstrapServers = value(values, "bootstrap-servers",
                "localhost:19092,localhost:29092,localhost:39092");
        topic = value(values, "topic", "chaos-perf-test");
        outputTopics = java.util.Arrays.stream(value(values, "output-topics", topic).split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).distinct().toList();
        poolSize = positiveInt(values, "pool-size", 8);
        threads = positiveInt(values, "threads", 8);
        recordsPerTransaction = positiveInt(values, "records-per-tx", 10);
        recordSizeBytes = positiveInt(values, "record-size", 1024);
        targetPartition = (int) nonNegativeLong(values, "target-partition", 0);
        partitionCount = positiveInt(values, "partition-count", 12);
        long abortEveryValue = nonNegativeLong(values, "abort-every", scenario.equals("MB-01") ? 10 : 0);
        if (abortEveryValue > Integer.MAX_VALUE) throw new IllegalArgumentException("abort-every is too large");
        abortEvery = (int) abortEveryValue;
        duration = Duration.ofSeconds(positiveLong(values, "duration-sec", 600));
        faultAt = Duration.ofSeconds(nonNegativeLong(values, "fault-at-sec", 180));
        faultDuration = Duration.ofSeconds(positiveLong(values, "fault-duration-sec", 60));
        recoveryTimeout = Duration.ofSeconds(positiveLong(values, "recovery-timeout-sec", 120));
        leaseTimeoutMs = positiveLong(values, "lease-timeout-ms", 5_000);
        chaosEnabled = Boolean.parseBoolean(value(values, "chaos-enabled", "false"));
        allowedClusterId = value(values, "cluster-allowlist", "");
        resultsDirectory = Paths.get(value(values, "results-dir", "chaos-results"));
        partitionBrokerCommand = value(values, "partition-broker-command", "");
        partitionClusterCommand = value(values, "partition-cluster-command", "");
        commitResponseCommand = value(values, "commit-response-command", "");
        captureCommitResponseCommand = value(values, "capture-commit-response-command", "");
        healNetworkCommand = value(values, "heal-network-command", "");
        requestTimeoutMs = positiveLong(values, "request-timeout-ms", 10_000);
        transactionTimeoutMs = nonNegativeLong(values, "transaction-timeout-ms",
                scenario.equals("CH-14") ? 6_000 : 0);
        shutdownGraceMs = positiveLong(values, "shutdown-grace-ms",
                scenario.equals("CH-11") ? 1 : 15_000);
        poisonIntervalSec = positiveLong(values, "poison-interval-sec", 10);

        Map<Integer, String> containers = new LinkedHashMap<>();
        containers.put(1, value(values, "broker-1-container", "kafka-pool-chaos-1"));
        containers.put(2, value(values, "broker-2-container", "kafka-pool-chaos-2"));
        containers.put(3, value(values, "broker-3-container", "kafka-pool-chaos-3"));
        brokerContainers = Map.copyOf(containers);
        validate();
    }

    public static ChaosConfig fromArgs(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + args[i]);
            }
            String key = args[i].substring(2);
            String next = i + 1 < args.length ? args[i + 1] : "true";
            if (next.startsWith("--")) {
                values.put(key, "true");
            } else {
                values.put(key, next);
                i++;
            }
        }
        return new ChaosConfig(values);
    }

    private void validate() {
        if (!scenario.matches("(MB-0[1-4]|CH-0[1-9]|CH-1[0-5])")) {
            throw new IllegalArgumentException("Unknown chaos scenario: " + scenario);
        }
        if (outputTopics.isEmpty()) throw new IllegalArgumentException("output-topics must not be empty");
        if (!outputTopics.contains(topic)) {
            throw new IllegalArgumentException("output-topics must include the primary --topic");
        }
        if (faultAt.compareTo(duration) >= 0 && !scenario.equals("MB-01") && !scenario.equals("MB-02")) {
            throw new IllegalArgumentException("fault-at-sec must be less than duration-sec");
        }
        if (faultAt.plus(faultDuration).compareTo(duration) >= 0
                && !scenario.equals("MB-01") && !scenario.equals("MB-02")) {
            throw new IllegalArgumentException("Scenario needs a post-fault recovery window");
        }
        if (scenario.equals("CH-04")
                && faultAt.plusSeconds(6L * (10 + 20)).compareTo(duration) >= 0) {
            throw new IllegalArgumentException(
                    "CH-04 requires 180 seconds plus a post-fault recovery window");
        }
    }

    public void requireChaosAuthorization() {
        if (!chaosEnabled) {
            throw new IllegalStateException(
                    "Fault injection is disabled; pass --chaos-enabled true for a disposable cluster");
        }
        if (allowedClusterId.isBlank()) {
            throw new IllegalStateException(
                    "Fault injection requires --cluster-allowlist with the expected Kafka cluster ID");
        }
    }

    private static String value(Map<String, String> values, String key, String defaultValue) {
        return values.getOrDefault(key, System.getProperty("chaos." + key, defaultValue));
    }

    private static int positiveInt(Map<String, String> values, String key, int defaultValue) {
        long parsed = positiveLong(values, key, defaultValue);
        if (parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(key + " is too large");
        }
        return (int) parsed;
    }

    private static long positiveLong(Map<String, String> values, String key, long defaultValue) {
        long parsed = Long.parseLong(value(values, key, String.valueOf(defaultValue)));
        if (parsed <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return parsed;
    }

    private static long nonNegativeLong(Map<String, String> values, String key, long defaultValue) {
        long parsed = Long.parseLong(value(values, key, String.valueOf(defaultValue)));
        if (parsed < 0) {
            throw new IllegalArgumentException(key + " must not be negative");
        }
        return parsed;
    }

    public String scenario() { return scenario; }
    public String bootstrapServers() { return bootstrapServers; }
    public String topic() { return topic; }
    public java.util.List<String> outputTopics() { return outputTopics; }
    public int poolSize() { return poolSize; }
    public int threads() { return threads; }
    public int recordsPerTransaction() { return recordsPerTransaction; }
    public int recordSizeBytes() { return recordSizeBytes; }
    public int targetPartition() { return targetPartition; }
    public int partitionCount() { return partitionCount; }
    public int abortEvery() { return abortEvery; }
    public Duration duration() { return duration; }
    public Duration faultAt() { return faultAt; }
    public Duration faultDuration() { return faultDuration; }
    public Duration recoveryTimeout() { return recoveryTimeout; }
    public long leaseTimeoutMs() { return leaseTimeoutMs; }
    public boolean chaosEnabled() { return chaosEnabled; }
    public String allowedClusterId() { return allowedClusterId; }
    public Path resultsDirectory() { return resultsDirectory; }
    public Map<Integer, String> brokerContainers() { return brokerContainers; }
    public String partitionBrokerCommand() { return partitionBrokerCommand; }
    public String partitionClusterCommand() { return partitionClusterCommand; }
    public String commitResponseCommand() { return commitResponseCommand; }
    public String captureCommitResponseCommand() { return captureCommitResponseCommand; }
    public String healNetworkCommand() { return healNetworkCommand; }
    public long requestTimeoutMs() { return requestTimeoutMs; }
    /** Producer {@code transaction.timeout.ms}; zero keeps the Kafka client default. */
    public long transactionTimeoutMs() { return transactionTimeoutMs; }
    public long shutdownGraceMs() { return shutdownGraceMs; }
    public long poisonIntervalSec() { return poisonIntervalSec; }
}
