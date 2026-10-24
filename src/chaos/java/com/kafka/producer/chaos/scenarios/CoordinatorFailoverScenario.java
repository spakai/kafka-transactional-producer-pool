package com.kafka.producer.chaos.scenarios;

import java.util.Map;

/**
 * CH-15: stops the broker coordinating a pool transactional ID. Coordinator errors are
 * retriable and must not be treated as fatal; recovery to full capacity is required.
 * The producer_fenced_total value is recorded for review of the retriable/fatal boundary.
 */
public final class CoordinatorFailoverScenario implements ChaosScenario {
    private int brokerId = -1;
    private boolean stopped;
    private boolean restarted;

    @Override
    public void onStart(ScenarioContext context) {
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) throws Exception {
        long faultAt = context.config().faultAt().toSeconds();
        long healAt = faultAt + context.config().faultDuration().toSeconds();
        if (!stopped && elapsedSecond >= faultAt) {
            brokerId = context.inspector().findTransactionCoordinator(
                    context.load().transactionalId(0));
            context.event("coordinator-selected", "confirmed", "transaction coordinator",
                    Map.of("brokerId", brokerId,
                            "transactionalId", context.load().transactionalId(0)));
            context.controller().stopBroker(brokerId);
            stopped = true;
        }
        if (stopped && !restarted && elapsedSecond >= healAt) {
            restart(context);
        }
    }

    private void restart(ScenarioContext context) throws Exception {
        context.controller().startBroker(brokerId);
        context.controller().waitForBrokerReady(brokerId, context.config().recoveryTimeout());
        restarted = true;
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        if (stopped && !restarted) {
            restart(context);
        }
        ProducerPoisonScenario.awaitFullCapacity(context);
        context.event("coordinator-failover", "recorded", "fencing observed during failover",
                Map.of("producerFencedTotal", context.load().producerFencedTotal()));
    }

    @Override
    public String evidenceGap(ScenarioContext context, boolean commitResponseProofConfirmed) {
        return stopped ? null : "Transaction coordinator was never stopped";
    }
}
