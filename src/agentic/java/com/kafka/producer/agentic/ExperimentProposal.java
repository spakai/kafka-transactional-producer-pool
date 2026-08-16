package com.kafka.producer.agentic;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Untrusted planner output. It must pass ExperimentPolicy before execution. */
public record ExperimentProposal(
        String hypothesis,
        String hypothesisFamily,
        FaultType faultType,
        int targetPartition,
        Duration startAfter,
        Duration faultDuration,
        int threads,
        int recordsPerTransaction,
        Duration observeFor,
        List<String> expectedSignals) {

    public ExperimentProposal {
        hypothesis = requireText(hypothesis, "hypothesis");
        hypothesisFamily = requireText(hypothesisFamily, "hypothesisFamily");
        faultType = Objects.requireNonNull(faultType, "faultType");
        startAfter = nonNegative(startAfter, "startAfter");
        faultDuration = nonNegative(faultDuration, "faultDuration");
        observeFor = positive(observeFor, "observeFor");
        expectedSignals = List.copyOf(Objects.requireNonNull(expectedSignals, "expectedSignals"));
        if (targetPartition < 0 || threads <= 0 || recordsPerTransaction <= 0) {
            throw new IllegalArgumentException("partition must be non-negative and load values positive");
        }
        if (faultType != FaultType.NONE && faultDuration.isZero()) {
            throw new IllegalArgumentException("faultDuration must be positive for a fault");
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
        return value;
    }

    private static Duration nonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) throw new IllegalArgumentException(name + " must not be negative");
        return value;
    }

    private static Duration positive(Duration value, String name) {
        nonNegative(value, name);
        if (value.isZero()) throw new IllegalArgumentException(name + " must be positive");
        return value;
    }
}
