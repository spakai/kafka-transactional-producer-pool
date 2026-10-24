package com.kafka.producer.chaos.scenarios;

import java.util.Map;

/**
 * CH-06/07/08: fences pool producers from outside by running initTransactions with the
 * same transactional ID, then requires the pool to evict, rebuild and return to full capacity.
 */
public final class ProducerPoisonScenario implements ChaosScenario {
    public enum Mode { SINGLE_SLOT, ALL_SLOTS, REPEATED }

    private static final long RECOVERY_POLL_MS = 250;

    private final Mode mode;
    private boolean poisoned;
    private boolean awaitingRecovery;
    private long poisonedAtSecond;
    private long fencedAtPoison;
    private long lastPoisonSecond;
    private int poisonCount;
    private int nextSlot;

    public ProducerPoisonScenario(Mode mode) {
        this.mode = mode;
    }

    @Override
    public void onStart(ScenarioContext context) {
        context.event("poison-plan", "scheduled", mode.name(),
                Map.of("poolSize", context.config().poolSize()));
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) throws Exception {
        long faultAt = context.config().faultAt().toSeconds();
        long endAt = faultAt + context.config().faultDuration().toSeconds();
        if (elapsedSecond >= faultAt && shouldPoison(context, elapsedSecond, endAt)) {
            poison(context, elapsedSecond);
        }
        if (awaitingRecovery && context.load().producerFencedTotal() > fencedAtPoison
                && context.load().atFullCapacity()) {
            awaitingRecovery = false;
            context.event("pool-recovered", "confirmed", "pool at full capacity",
                    Map.of("rebuildSeconds", elapsedSecond - poisonedAtSecond,
                            "fencedTotal", context.load().producerFencedTotal()));
        }
    }

    private boolean shouldPoison(ScenarioContext context, long elapsedSecond, long endAt) {
        if (mode != Mode.REPEATED) {
            return !poisoned;
        }
        return elapsedSecond < endAt
                && (poisonCount == 0
                || elapsedSecond - lastPoisonSecond >= context.config().poisonIntervalSec());
    }

    private void poison(ScenarioContext context, long elapsedSecond) throws Exception {
        int poolSize = context.load().poolSize();
        fencedAtPoison = context.load().producerFencedTotal();
        switch (mode) {
            case SINGLE_SLOT -> context.controller().fenceProducer(context.load().transactionalId(0));
            case ALL_SLOTS -> {
                for (int slot = 0; slot < poolSize; slot++) {
                    context.controller().fenceProducer(context.load().transactionalId(slot));
                }
            }
            case REPEATED -> {
                context.controller().fenceProducer(context.load().transactionalId(nextSlot));
                nextSlot = (nextSlot + 1) % poolSize;
            }
        }
        poisoned = true;
        awaitingRecovery = true;
        poisonedAtSecond = elapsedSecond;
        lastPoisonSecond = elapsedSecond;
        poisonCount++;
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        awaitFullCapacity(context);
    }

    static void awaitFullCapacity(ScenarioContext context) throws Exception {
        long deadline = System.nanoTime() + context.config().recoveryTimeout().toNanos();
        while (!context.load().atFullCapacity()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Pool did not return to full capacity; state="
                        + context.load().poolState());
            }
            Thread.sleep(RECOVERY_POLL_MS);
        }
        context.event("pool-capacity", "confirmed", "pool at full capacity after fault");
    }

    @Override
    public String evidenceGap(ScenarioContext context, boolean commitResponseProofConfirmed) {
        if (poisonCount == 0) {
            return "No producer was poisoned";
        }
        if (context.load().producerFencedTotal() == 0) {
            return "Producers were poisoned but the pool recorded no fencing (producer_fenced_total=0)";
        }
        return null;
    }
}
