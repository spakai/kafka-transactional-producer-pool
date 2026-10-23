package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosLoadEngine;
import com.kafka.producer.chaos.VerificationReport;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExperimentOracleTest {
    @Test
    void doesNotPassTcpOnlyCommitResponseAttempt() {
        ExperimentPlan plan = new ExperimentPlan("exp-1", "ambiguous commit", "AF-05",
                FaultType.DROP_COMMIT_RESPONSE, -1, 0, Duration.ofSeconds(1),
                Duration.ofSeconds(1), 1, 1, Duration.ofSeconds(1), List.of());
        ChaosLoadEngine.Sample sample = new ChaosLoadEngine.Sample(Instant.now(), 3,
                1, 1, 0, 0, 0, 1, 1, 1, "HEALTHY", 1, 0, 1, 10, 0, 0);
        VerificationReport verification = new VerificationReport(
                1, 1, 1, 0, 0, 0, 0, 0, List.of(), List.of());

        ExperimentResult result = new ExperimentOracle().evaluate(
                plan, List.of(sample), verification, true, null);

        assertEquals(FailureClass.INVALID_INCOMPLETE, result.failureClass());
    }
}
