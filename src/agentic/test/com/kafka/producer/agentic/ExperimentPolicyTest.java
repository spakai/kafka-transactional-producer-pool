package com.kafka.producer.agentic;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentPolicyTest {
    private final ExperimentPolicy policy = new ExperimentPolicy();

    @Test void resolvesLeaderOnlyAfterApproval() {
        PolicyDecision decision = policy.evaluate(TestFixtures.config(),
                TestFixtures.proposal(FaultType.STOP_LEADER_BROKER), TestFixtures.cluster(),
                0, Duration.ZERO, Set.of());
        assertTrue(decision.approved());
        assertEquals(1, decision.plan().brokerId());
    }

    @Test void rejectsLoadAboveBlastRadius() {
        ExperimentProposal base = TestFixtures.proposal(FaultType.NONE);
        ExperimentProposal excessive = new ExperimentProposal(base.hypothesis(), base.hypothesisFamily(),
                base.faultType(), 0, base.startAfter(), base.faultDuration(), 9, 10,
                base.observeFor(), base.expectedSignals());
        assertEquals("LOAD_LIMIT_EXCEEDED", policy.evaluate(TestFixtures.config(), excessive,
                TestFixtures.cluster(), 0, Duration.ZERO, Set.of()).reasons().get(0));
    }

    @Test void rejectsDuplicateResolvedExperiment() {
        PolicyDecision first = policy.evaluate(TestFixtures.config(), TestFixtures.proposal(FaultType.NONE),
                TestFixtures.cluster(), 0, Duration.ZERO, Set.of());
        PolicyDecision duplicate = policy.evaluate(TestFixtures.config(), TestFixtures.proposal(FaultType.NONE),
                TestFixtures.cluster(), 1, Duration.ZERO, Set.of(ExperimentPolicy.signature(first.plan())));
        assertFalse(duplicate.approved());
        assertEquals("DUPLICATE_EXPERIMENT", duplicate.reasons().get(0));
    }

    @Test void rejectsStaleUnsafeClusterState() {
        var c = TestFixtures.cluster();
        var unsafe = new com.kafka.producer.chaos.ClusterInspector.ClusterSnapshot(c.clusterId(),
                c.brokerIds(), c.leaders(), 1, 0);
        assertFalse(policy.evaluate(TestFixtures.config(), TestFixtures.proposal(FaultType.NONE),
                unsafe, 0, Duration.ZERO, Set.of()).approved());
    }
}
