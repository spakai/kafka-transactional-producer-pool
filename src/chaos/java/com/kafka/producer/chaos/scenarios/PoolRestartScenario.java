package com.kafka.producer.chaos.scenarios;

import java.util.Map;

/**
 * CH-11/12/13: in-process pod lifecycle faults. The pool is shut down mid-load and a new
 * pool reuses the same transactional IDs, as a restarted pod would. CH-11 forces shutdown
 * with a 1ms grace period; CH-12 drains gracefully; CH-13 repeats restarts as a rolling update.
 */
public final class PoolRestartScenario implements ChaosScenario {
    private static final int ROLLING_RESTARTS = 3;

    private final int plannedRestarts;
    private int restarted;
    private long lastRestartSecond;

    private PoolRestartScenario(int plannedRestarts) {
        this.plannedRestarts = plannedRestarts;
    }

    public static PoolRestartScenario once() { return new PoolRestartScenario(1); }

    public static PoolRestartScenario rolling() { return new PoolRestartScenario(ROLLING_RESTARTS); }

    @Override
    public void onStart(ScenarioContext context) {
        context.event("pool-restart", "scheduled", "restarts=" + plannedRestarts,
                Map.of("shutdownGraceMs", context.config().shutdownGraceMs()));
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) {
        long faultAt = context.config().faultAt().toSeconds();
        if (restarted >= plannedRestarts || elapsedSecond < faultAt
                || (restarted > 0
                && elapsedSecond - lastRestartSecond < context.config().poisonIntervalSec())) {
            return;
        }
        context.event("pool-restart", "requested", "restart " + (restarted + 1));
        context.load().restartPool();
        restarted++;
        lastRestartSecond = elapsedSecond;
        context.event("pool-restart", "applied", "restart " + restarted,
                Map.of("shutdownRejections", context.load().shutdownRejections()));
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        ProducerPoisonScenario.awaitFullCapacity(context);
    }

    @Override
    public String evidenceGap(ScenarioContext context, boolean commitResponseProofConfirmed) {
        return context.load().poolRestarts() >= plannedRestarts ? null
                : "Only " + context.load().poolRestarts() + " of " + plannedRestarts
                        + " pool restarts completed";
    }
}
