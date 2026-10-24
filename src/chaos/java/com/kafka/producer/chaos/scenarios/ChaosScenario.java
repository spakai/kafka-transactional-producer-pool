package com.kafka.producer.chaos.scenarios;

public interface ChaosScenario {
    void onStart(ScenarioContext context) throws Exception;

    void onTick(ScenarioContext context, long elapsedSecond) throws Exception;

    void onFinish(ScenarioContext context) throws Exception;

    /**
     * Returns a description of missing proof that the intended fault took effect,
     * or {@code null}. A non-null value downgrades a passing run to INVALID/INCOMPLETE.
     */
    default String evidenceGap(ScenarioContext context, boolean commitResponseProofConfirmed) {
        return null;
    }

    /** Whether a cluster metadata request should run on this sample tick. */
    default boolean shouldInspectCluster() { return true; }
}
