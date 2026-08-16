package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosLoadEngine;
import com.kafka.producer.chaos.ClusterInspector;

import java.util.Optional;

public final class SafetyMonitor {
    private final String clusterId;
    private int missingTelemetrySeconds;

    public SafetyMonitor(String clusterId) { this.clusterId = clusterId; }

    public Optional<String> evaluate(ClusterInspector.ClusterSnapshot cluster, ChaosLoadEngine.Sample sample) {
        if (cluster == null || sample == null) {
            if (++missingTelemetrySeconds > 15) return Optional.of("TELEMETRY_LOST");
            return Optional.empty();
        }
        missingTelemetrySeconds = 0;
        if (!clusterId.equals(cluster.clusterId())) return Optional.of("CLUSTER_ID_CHANGED");
        if (cluster.offlinePartitions() > 0) return Optional.of("OFFLINE_PARTITIONS");
        if (cluster.brokerIds().size() < 2) return Optional.of("BROKER_QUORUM_AT_RISK");
        if (sample.heapUsedMb() > 2048) return Optional.of("HEAP_LIMIT_EXCEEDED");
        return Optional.empty();
    }
}
