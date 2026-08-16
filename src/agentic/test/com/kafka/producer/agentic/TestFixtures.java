package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ClusterInspector;
import org.apache.kafka.common.TopicPartition;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

final class TestFixtures {
    private TestFixtures() {}
    static CampaignConfig config() {
        return new CampaignConfig(true, true, "cluster-1", "disposable", "localhost:9092",
                "test", 4, 4, 10, 0, Duration.ofMinutes(30), 8, Duration.ofSeconds(60),
                2.0, Duration.ofSeconds(120), 7, false, Path.of("target/agentic-test"), null,
                Map.of(1, "b1", 2, "b2", 3, "b3"), "partition-one", "partition-all",
                "drop-commit", "heal");
    }
    static ClusterInspector.ClusterSnapshot cluster() {
        return new ClusterInspector.ClusterSnapshot("cluster-1", List.of(1, 2, 3),
                Map.of(new TopicPartition("test", 0), 1), 0, 0);
    }
    static ExperimentProposal proposal(FaultType type) {
        return new ExperimentProposal("hypothesis", "AF-X", type, 0, Duration.ofSeconds(5),
                type == FaultType.NONE ? Duration.ZERO : Duration.ofSeconds(10), 4, 10,
                Duration.ofSeconds(10), List.of("pool_health", "transaction_outcome_total"));
    }
    static ExperimentPlan plan(FaultType type) {
        return new ExperimentPlan("exp-1", "hypothesis", "AF-X", type, 1, 0,
                Duration.ofSeconds(5), type == FaultType.NONE ? Duration.ZERO : Duration.ofSeconds(10),
                4, 10, Duration.ofSeconds(10), List.of("pool_health", "transaction_outcome_total"));
    }
}
