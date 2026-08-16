package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ClusterInspector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public final class ExperimentPolicy {
    private static final Set<String> REQUIRED_SIGNALS = Set.of("pool_health", "transaction_outcome_total");

    public PolicyDecision evaluate(
            CampaignConfig config,
            ExperimentProposal proposal,
            ClusterInspector.ClusterSnapshot cluster,
            int completedExperiments,
            Duration elapsed,
            Set<String> priorPlanSignatures) {
        try {
            config.requireAuthorization();
        } catch (RuntimeException e) {
            return PolicyDecision.reject("AUTHORIZATION: " + e.getMessage());
        }
        if (!config.clusterAllowlist().equals(cluster.clusterId())) {
            return PolicyDecision.reject("CLUSTER_ID_MISMATCH");
        }
        if (cluster.brokerIds().size() != 3 || cluster.offlinePartitions() > 0
                || cluster.underReplicatedPartitions() > 0) {
            return PolicyDecision.reject("CLUSTER_NOT_FULLY_HEALTHY");
        }
        if (completedExperiments >= config.maxExperiments()) return PolicyDecision.reject("EXPERIMENT_BUDGET_EXHAUSTED");
        if (elapsed.plus(proposal.startAfter()).plus(proposal.faultDuration()).plus(proposal.observeFor())
                .compareTo(config.campaignDuration()) > 0) return PolicyDecision.reject("CAMPAIGN_TIME_BUDGET_EXHAUSTED");
        if (proposal.faultDuration().compareTo(config.maxFaultDuration()) > 0) return PolicyDecision.reject("FAULT_DURATION_EXCEEDED");
        if (proposal.threads() > Math.floor(config.baselineThreads() * config.maxLoadMultiplier())) {
            return PolicyDecision.reject("LOAD_LIMIT_EXCEEDED");
        }
        if (!new HashSet<>(proposal.expectedSignals()).containsAll(REQUIRED_SIGNALS)) {
            return PolicyDecision.reject("REQUIRED_TELEMETRY_MISSING");
        }
        if (!cleanupConfigured(config, proposal.faultType())) return PolicyDecision.reject("CLEANUP_NOT_CONFIGURED");

        int brokerId = resolveBroker(cluster, config.topic(), proposal);
        String signature = proposal.hypothesisFamily() + '|' + proposal.faultType() + '|'
                + brokerId + '|' + proposal.targetPartition() + '|' + proposal.threads();
        if (priorPlanSignatures.contains(signature)) return PolicyDecision.reject("DUPLICATE_EXPERIMENT");
        ExperimentPlan plan = new ExperimentPlan(
                "exp-" + (completedExperiments + 1), proposal.hypothesis(), proposal.hypothesisFamily(),
                proposal.faultType(), brokerId, proposal.targetPartition(), proposal.startAfter(),
                proposal.faultDuration(), proposal.threads(), proposal.recordsPerTransaction(),
                proposal.observeFor(), proposal.expectedSignals());
        return PolicyDecision.approve(plan);
    }

    public static String signature(ExperimentPlan p) {
        return p.hypothesisFamily() + '|' + p.faultType() + '|' + p.brokerId() + '|'
                + p.targetPartition() + '|' + p.threads();
    }

    private static int resolveBroker(ClusterInspector.ClusterSnapshot c, String topic, ExperimentProposal p) {
        Integer leader = c.leaders().entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topic) && e.getKey().partition() == p.targetPartition())
                .map(java.util.Map.Entry::getValue).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("target partition has no leader"));
        if (p.faultType() == FaultType.STOP_NON_LEADER_BROKER) {
            return c.brokerIds().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        }
        if (p.faultType() == FaultType.STOP_LEADER_BROKER
                || p.faultType() == FaultType.PARTITION_PRODUCER_FROM_ONE_BROKER) return leader;
        return -1;
    }

    private static boolean cleanupConfigured(CampaignConfig c, FaultType type) {
        return switch (type) {
            case NONE, STOP_LEADER_BROKER, STOP_NON_LEADER_BROKER -> true;
            case PARTITION_PRODUCER_FROM_ONE_BROKER -> !c.partitionBrokerCommand().isBlank() && !c.healNetworkCommand().isBlank();
            case PARTITION_PRODUCER_FROM_CLUSTER, FLAP_PRODUCER_NETWORK -> !c.partitionClusterCommand().isBlank() && !c.healNetworkCommand().isBlank();
            case DROP_COMMIT_RESPONSE -> !c.commitResponseCommand().isBlank() && !c.healNetworkCommand().isBlank();
        };
    }
}
