package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosLoadEngine;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class SafetyMonitorTest {
    @Test void abortsOnClusterIdentityChange() {
        SafetyMonitor monitor = new SafetyMonitor("expected");
        var c = TestFixtures.cluster();
        var sample = new ChaosLoadEngine.Sample(Instant.now(), 0, 0, 0, 0, 0,
                0, 0, 0, "HEALTHY", 4, 0, 4, 10, 0, 0);
        assertEquals("CLUSTER_ID_CHANGED", monitor.evaluate(c, sample).orElseThrow());
    }

    @Test void toleratesOnlyBoundedTelemetryLoss() {
        SafetyMonitor monitor = new SafetyMonitor("cluster-1");
        for (int i = 0; i < 15; i++) assertTrue(monitor.evaluate(null, null).isEmpty());
        assertEquals("TELEMETRY_LOST", monitor.evaluate(null, null).orElseThrow());
    }
}
