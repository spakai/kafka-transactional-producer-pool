package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosLoadEngine;
import com.kafka.producer.chaos.VerificationReport;

import java.util.List;

public final class ExperimentOracle {
    public ExperimentResult evaluate(
            ExperimentPlan plan, List<ChaosLoadEngine.Sample> samples,
            VerificationReport verification, boolean cleanupConfirmed, String safetyAbort) {
        if (!cleanupConfirmed) return new ExperimentResult(plan, FailureClass.CLEANUP_FAILURE,
                "cleanup was not confirmed", false, samples, verification);
        if (safetyAbort != null) return new ExperimentResult(plan, FailureClass.PRODUCER_SAFETY_FAILURE,
                safetyAbort, true, samples, verification);
        if (!verification.passed()) return new ExperimentResult(plan, FailureClass.PRODUCER_CORRECTNESS_FAILURE,
                String.join("; ", verification.issues()), true, samples, verification);
        if (samples.isEmpty()) return new ExperimentResult(plan, FailureClass.HARNESS_FAILURE,
                "no telemetry samples", true, samples, verification);
        ChaosLoadEngine.Sample last = samples.get(samples.size() - 1);
        if (!"HEALTHY".equals(last.poolState())) {
            return new ExperimentResult(plan, FailureClass.PRODUCER_RECOVERY_FAILURE,
                    "pool did not reconcile after recovery", true, samples, verification);
        }
        // The current Toxiproxy hook drops all downstream Kafka traffic and
        // emits no protocol-level proof that an EndTxn response was targeted.
        // Never promote this approximation to a CH-05 pass.
        if (plan.faultType() == FaultType.DROP_COMMIT_RESPONSE) {
            return new ExperimentResult(plan, FailureClass.INVALID_INCOMPLETE,
                    "commit-response attempt lacks correlated EndTxn request/response-drop evidence",
                    true, samples, verification);
        }
        return new ExperimentResult(plan, FailureClass.PASSED, "all deterministic oracles passed",
                true, samples, verification);
    }
}
