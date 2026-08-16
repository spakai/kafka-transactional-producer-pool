package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosLoadEngine;
import com.kafka.producer.chaos.VerificationReport;

import java.util.List;

public record ExperimentResult(
        ExperimentPlan plan,
        FailureClass failureClass,
        String detail,
        boolean cleanupConfirmed,
        List<ChaosLoadEngine.Sample> samples,
        VerificationReport verification) {
    public ExperimentResult { samples = List.copyOf(samples); }
    public boolean passed() { return failureClass == FailureClass.PASSED; }
}
