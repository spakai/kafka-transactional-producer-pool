package com.kafka.producer.chaos.scenarios;

/**
 * CH-09: poisons a producer and then blocks the cluster, so the replacement's
 * initTransactions fails and RecoverySupervisor must retry after the network heals.
 */
public final class RebuildFailureScenario implements ChaosScenario {
    private boolean poisoned;
    private boolean partitioned;
    private boolean healed;
    private boolean sawReducedCapacity;

    @Override
    public void onStart(ScenarioContext context) {
        context.event("rebuild-failure", "scheduled", "fence slot 0 then block cluster");
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) throws Exception {
        long faultAt = context.config().faultAt().toSeconds();
        long healAt = faultAt + context.config().faultDuration().toSeconds();
        if (!poisoned && elapsedSecond >= faultAt) {
            context.controller().fenceProducer(context.load().transactionalId(0));
            poisoned = true;
        }
        if (poisoned && !partitioned && elapsedSecond >= faultAt + 1) {
            context.controller().partitionProducerFromCluster();
            partitioned = true;
        }
        if (partitioned && !healed && elapsedSecond >= healAt) {
            context.controller().healNetwork();
            healed = true;
        }
        if (partitioned && !context.load().atFullCapacity()) {
            sawReducedCapacity = true;
        }
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        if (partitioned && !healed) {
            context.controller().healNetwork();
            healed = true;
        }
        ProducerPoisonScenario.awaitFullCapacity(context);
    }

    @Override
    public String evidenceGap(ScenarioContext context, boolean commitResponseProofConfirmed) {
        return sawReducedCapacity ? null
                : "Pool never dropped below full capacity, so rebuild failure was not exercised";
    }
}
