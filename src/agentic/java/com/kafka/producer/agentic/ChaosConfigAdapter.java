package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosConfig;

import java.util.ArrayList;
import java.util.List;

final class ChaosConfigAdapter {
    private ChaosConfigAdapter() {}

    static ChaosConfig create(CampaignConfig campaign, ExperimentPlan plan, String resultsDir) {
        List<String> args = new ArrayList<>(List.of(
                "--scenario", scenario(plan.faultType()),
                "--bootstrap-servers", campaign.bootstrapServers(),
                "--topic", campaign.topic(),
                "--pool-size", String.valueOf(campaign.poolSize()),
                "--threads", String.valueOf(plan.threads()),
                "--records-per-tx", String.valueOf(plan.recordsPerTransaction()),
                "--target-partition", String.valueOf(plan.targetPartition()),
                "--duration-sec", String.valueOf(Math.max(2, plan.totalDuration().toSeconds())),
                "--fault-at-sec", String.valueOf(plan.startAfter().toSeconds()),
                "--fault-duration-sec", String.valueOf(Math.max(1, plan.faultDuration().toSeconds())),
                "--recovery-timeout-sec", String.valueOf(campaign.recoveryTimeout().toSeconds()),
                "--chaos-enabled", "true",
                "--cluster-allowlist", campaign.clusterAllowlist(),
                "--results-dir", resultsDir,
                "--broker-1-container", campaign.brokerContainers().get(1),
                "--broker-2-container", campaign.brokerContainers().get(2),
                "--broker-3-container", campaign.brokerContainers().get(3)));
        add(args, "partition-broker-command", campaign.partitionBrokerCommand());
        add(args, "partition-cluster-command", campaign.partitionClusterCommand());
        add(args, "commit-response-command", campaign.commitResponseCommand());
        add(args, "heal-network-command", campaign.healNetworkCommand());
        return ChaosConfig.fromArgs(args.toArray(String[]::new));
    }

    private static void add(List<String> args, String key, String value) {
        if (!value.isBlank()) { args.add("--" + key); args.add(value); }
    }

    private static String scenario(FaultType type) {
        return switch (type) {
            case NONE -> "MB-01";
            case STOP_NON_LEADER_BROKER -> "MB-03";
            case STOP_LEADER_BROKER -> "MB-04";
            case PARTITION_PRODUCER_FROM_ONE_BROKER -> "CH-02";
            case PARTITION_PRODUCER_FROM_CLUSTER -> "CH-03";
            case FLAP_PRODUCER_NETWORK -> "CH-04";
            case DROP_COMMIT_RESPONSE -> "CH-05";
        };
    }
}
