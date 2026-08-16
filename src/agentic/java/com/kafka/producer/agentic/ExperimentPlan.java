package com.kafka.producer.agentic;

import java.time.Duration;
import java.util.List;

/** Fully resolved and policy-approved execution plan. */
public record ExperimentPlan(
        String experimentId,
        String hypothesis,
        String hypothesisFamily,
        FaultType faultType,
        int brokerId,
        int targetPartition,
        Duration startAfter,
        Duration faultDuration,
        int threads,
        int recordsPerTransaction,
        Duration observeFor,
        List<String> expectedSignals) {

    public ExperimentPlan {
        expectedSignals = List.copyOf(expectedSignals);
    }

    public Duration totalDuration() {
        return startAfter.plus(faultDuration).plus(observeFor);
    }
}
