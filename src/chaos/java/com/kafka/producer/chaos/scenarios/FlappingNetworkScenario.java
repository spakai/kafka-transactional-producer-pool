package com.kafka.producer.chaos.scenarios;

public final class FlappingNetworkScenario implements ChaosScenario {
    private static final long PARTITION_SECONDS = 10;
    private static final long HEALTHY_SECONDS = 20;
    private static final int MAX_CYCLES = 6;
    private boolean partitioned;
    private int completedCycles;
    private long nextTransitionSecond;
    private long nextTransitionNanos;

    @Override
    public void onStart(ScenarioContext context) {
        nextTransitionSecond = context.config().faultAt().toSeconds();
        context.event("network-flapping", "scheduled", "six 10s/20s cycles");
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) throws Exception {
        if (completedCycles >= MAX_CYCLES) {
            return;
        }
        long now = System.nanoTime();
        if (!partitioned) {
            boolean due = completedCycles == 0
                    ? elapsedSecond >= nextTransitionSecond
                    : now >= nextTransitionNanos;
            if (!due) return;
            context.controller().partitionProducerFromCluster();
            partitioned = true;
            nextTransitionNanos = System.nanoTime()
                    + java.util.concurrent.TimeUnit.SECONDS.toNanos(PARTITION_SECONDS);
        } else if (now >= nextTransitionNanos) {
            context.controller().healNetwork();
            partitioned = false;
            completedCycles++;
            nextTransitionNanos = System.nanoTime()
                    + java.util.concurrent.TimeUnit.SECONDS.toNanos(HEALTHY_SECONDS);
        }
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        if (partitioned) {
            context.controller().healNetwork();
            partitioned = false;
        }
    }

    @Override
    public boolean shouldInspectCluster() {
        // A metadata request cannot complete while the producer is partitioned,
        // and its timeout would extend the intended fault window.
        return !partitioned;
    }
}
