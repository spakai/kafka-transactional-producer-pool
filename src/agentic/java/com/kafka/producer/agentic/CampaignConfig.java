package com.kafka.producer.agentic;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public record CampaignConfig(
        boolean agenticEnabled,
        boolean chaosEnabled,
        String clusterAllowlist,
        String environmentLabel,
        String bootstrapServers,
        String topic,
        int poolSize,
        int baselineThreads,
        int recordsPerTransaction,
        int targetPartition,
        Duration campaignDuration,
        int maxExperiments,
        Duration maxFaultDuration,
        double maxLoadMultiplier,
        Duration recoveryTimeout,
        long seed,
        boolean dryRun,
        Path resultsDirectory,
        Path replayFile,
        Map<Integer, String> brokerContainers,
        String partitionBrokerCommand,
        String partitionClusterCommand,
        String commitResponseCommand,
        String healNetworkCommand) {

    private static final Set<String> ENVIRONMENTS = Set.of("disposable", "ci-chaos");

    public CampaignConfig {
        brokerContainers = Map.copyOf(brokerContainers);
        if (poolSize <= 0 || baselineThreads <= 0 || recordsPerTransaction <= 0
                || maxExperiments <= 0 || targetPartition < 0 || maxLoadMultiplier < 1.0) {
            throw new IllegalArgumentException("invalid positive campaign limit");
        }
        if (campaignDuration.isZero() || campaignDuration.isNegative()
                || maxFaultDuration.isZero() || maxFaultDuration.isNegative()
                || recoveryTimeout.isZero() || recoveryTimeout.isNegative()) {
            throw new IllegalArgumentException("durations must be positive");
        }
    }

    public static CampaignConfig fromArgs(String[] args) {
        Map<String, String> v = parse(args);
        Map<Integer, String> brokers = Map.of(
                1, value(v, "broker-1-container", "kafka-pool-chaos-1"),
                2, value(v, "broker-2-container", "kafka-pool-chaos-2"),
                3, value(v, "broker-3-container", "kafka-pool-chaos-3"));
        String replay = value(v, "replay", "");
        return new CampaignConfig(
                bool(v, "agentic-enabled", false), bool(v, "chaos-enabled", false),
                value(v, "cluster-allowlist", ""), value(v, "environment-label", ""),
                value(v, "bootstrap-servers", "localhost:19092,localhost:29092,localhost:39092"),
                value(v, "topic", "chaos-perf-test"), integer(v, "pool-size", 8),
                integer(v, "threads", 8), integer(v, "records-per-tx", 10),
                integer(v, "target-partition", 0), seconds(v, "campaign-duration-sec", 1800),
                integer(v, "max-experiments", 8), seconds(v, "max-fault-duration-sec", 60),
                decimal(v, "max-load-multiplier", 2.0), seconds(v, "recovery-timeout-sec", 120),
                number(v, "seed", 7), bool(v, "dry-run", false),
                Path.of(value(v, "results-dir", "agentic-results")),
                replay.isBlank() ? null : Path.of(replay), brokers,
                value(v, "partition-broker-command", ""),
                value(v, "partition-cluster-command", ""),
                value(v, "commit-response-command", ""),
                value(v, "heal-network-command", ""));
    }

    public void requireAuthorization() {
        if (!agenticEnabled || !chaosEnabled) {
            throw new IllegalStateException("agentic and chaos opt-ins are required");
        }
        if (clusterAllowlist.isBlank()) throw new IllegalStateException("cluster allow-list is required");
        if (!ENVIRONMENTS.contains(environmentLabel)) {
            throw new IllegalStateException("environment must be disposable or ci-chaos");
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("Unexpected argument: " + args[i]);
            String key = args[i].substring(2);
            String next = i + 1 < args.length ? args[i + 1] : "true";
            if (next.startsWith("--")) values.put(key, "true");
            else { values.put(key, next); i++; }
        }
        return values;
    }

    private static String value(Map<String, String> v, String k, String d) {
        return v.getOrDefault(k, System.getProperty("agentic." + k, d));
    }
    private static boolean bool(Map<String, String> v, String k, boolean d) { return Boolean.parseBoolean(value(v, k, String.valueOf(d))); }
    private static int integer(Map<String, String> v, String k, int d) { return Math.toIntExact(number(v, k, d)); }
    private static long number(Map<String, String> v, String k, long d) { return Long.parseLong(value(v, k, String.valueOf(d))); }
    private static double decimal(Map<String, String> v, String k, double d) { return Double.parseDouble(value(v, k, String.valueOf(d))); }
    private static Duration seconds(Map<String, String> v, String k, long d) { return Duration.ofSeconds(number(v, k, d)); }
}
