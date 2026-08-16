package com.kafka.producer.agentic.planner;

import com.kafka.producer.agentic.CampaignConfig;
import com.kafka.producer.agentic.ExperimentProposal;
import com.kafka.producer.agentic.FaultType;
import com.kafka.producer.chaos.ClusterInspector;

import java.time.Duration;
import java.util.List;

public final class RuleBasedPlanner implements ExperimentPlanner {
    private final CampaignConfig config;

    public RuleBasedPlanner(CampaignConfig config) { this.config = config; }

    @Override
    public ExperimentProposal propose(ClusterInspector.ClusterSnapshot ignored, List<String> completed) {
        String family = List.of("AF-01", "AF-02A", "AF-02B", "AF-03A", "AF-03B", "AF-04", "AF-05", "AF-06")
                .stream().filter(f -> !completed.contains(f)).findFirst().orElse(null);
        if (family == null) return null;
        FaultType type = switch (family) {
            case "AF-01" -> FaultType.NONE;
            case "AF-02A" -> FaultType.STOP_NON_LEADER_BROKER;
            case "AF-02B" -> FaultType.STOP_LEADER_BROKER;
            case "AF-03A" -> FaultType.PARTITION_PRODUCER_FROM_ONE_BROKER;
            case "AF-03B" -> FaultType.PARTITION_PRODUCER_FROM_CLUSTER;
            case "AF-04" -> FaultType.FLAP_PRODUCER_NETWORK;
            case "AF-05" -> FaultType.DROP_COMMIT_RESPONSE;
            default -> FaultType.STOP_LEADER_BROKER;
        };
        int threads = family.equals("AF-06")
                ? Math.max(config.baselineThreads(), (int) Math.floor(config.baselineThreads() * config.maxLoadMultiplier()))
                : config.baselineThreads();
        Duration fault = type == FaultType.NONE ? Duration.ZERO
                : min(Duration.ofSeconds(30), config.maxFaultDuration());
        return new ExperimentProposal(
                hypothesis(family), family, type, config.targetPartition(), Duration.ofSeconds(15),
                fault, threads, config.recordsPerTransaction(), Duration.ofSeconds(30),
                List.of("pool_health", "transaction_outcome_total", "cluster_state"));
    }

    @Override public String identity() { return "rule-based-v1"; }

    private static Duration min(Duration a, Duration b) { return a.compareTo(b) <= 0 ? a : b; }
    private static String hypothesis(String family) {
        return switch (family) {
            case "AF-01" -> "Healthy load remains bounded";
            case "AF-02A" -> "Replica loss does not churn healthy producers";
            case "AF-02B" -> "Leader loss recovers without an application restart";
            case "AF-03A" -> "A partial partition preserves unaffected progress";
            case "AF-03B" -> "A full partition produces bounded backpressure";
            case "AF-04" -> "Intermittent connectivity does not create a retry storm";
            case "AF-05" -> "Commit ambiguity is surfaced without callback replay";
            default -> "Recovery remains bounded under contention";
        };
    }
}
