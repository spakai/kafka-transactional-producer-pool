package com.kafka.producer.chaos.scenarios;

import java.util.Map;

public final class BrokerFailureScenario implements ChaosScenario {
    private final boolean preferLeader;
    private final boolean targetPartition;
    private int brokerId;
    private boolean stopped;
    private boolean restarted;

    public BrokerFailureScenario(boolean preferLeader, boolean targetPartition) {
        this.preferLeader = preferLeader;
        this.targetPartition = targetPartition;
    }

    @Override
    public void onStart(ScenarioContext context) {
        if (targetPartition) {
            brokerId = preferLeader
                    ? context.inspector().selectLeaderForPartition(
                            context.initialCluster(), context.config().topic(),
                            context.config().targetPartition())
                    : context.inspector().selectNonLeaderForPartition(
                            context.initialCluster(), context.config().topic(),
                            context.config().targetPartition());
        } else {
            brokerId = preferLeader
                    ? context.inspector().selectStrictlyLeaderHeavyBroker(context.initialCluster())
                    : context.inspector().selectLeastLeadingBroker(context.initialCluster());
        }
        context.event("broker-selected", "confirmed",
                preferLeader ? "leader-heavy broker" : "least-leading broker",
                Map.of("brokerId", brokerId,
                        "targetPartition", targetPartition
                                ? context.config().targetPartition() : "all"));
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) throws Exception {
        long faultAt = context.config().faultAt().toSeconds();
        long healAt = faultAt + context.config().faultDuration().toSeconds();
        if (!stopped && elapsedSecond >= faultAt) {
            context.controller().stopBroker(brokerId);
            stopped = true;
        }
        if (stopped && !restarted && elapsedSecond >= healAt) {
            context.controller().startBroker(brokerId);
            context.controller().waitForBrokerReady(brokerId, context.config().recoveryTimeout());
            restarted = true;
        }
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        if (stopped && !restarted) {
            context.controller().startBroker(brokerId);
            context.controller().waitForBrokerReady(brokerId, context.config().recoveryTimeout());
            restarted = true;
        }
    }
}
