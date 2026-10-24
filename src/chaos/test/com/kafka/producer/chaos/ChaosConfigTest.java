package com.kafka.producer.chaos;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChaosConfigTest {

    @Test
    void parsesScenarioAndSafetyConfiguration() {
        ChaosConfig config = ChaosConfig.fromArgs(new String[]{
                "--scenario", "mb-04",
                "--duration-sec", "120",
                "--fault-at-sec", "20",
                "--fault-duration-sec", "20",
                "--target-partition", "7",
                "--chaos-enabled", "true",
                "--cluster-allowlist", "test-cluster"
        });

        assertEquals("MB-04", config.scenario());
        assertEquals(7, config.targetPartition());
        assertTrue(config.chaosEnabled());
        assertEquals("test-cluster", config.allowedClusterId());
        config.requireChaosAuthorization();
    }

    @Test
    void parsesMultipleOutputTopicsAndEnablesBaselineAbortSamples() {
        ChaosConfig config = ChaosConfig.fromArgs(new String[]{
                "--scenario", "MB-01",
                "--topic", "audit-a",
                "--output-topics", "audit-a,audit-b",
                "--duration-sec", "30"
        });
        assertEquals(java.util.List.of("audit-a", "audit-b"), config.outputTopics());
        assertEquals(10, config.abortEvery());
    }

    @Test
    void rejectsFaultOutsideDuration() {
        assertThrows(IllegalArgumentException.class, () -> ChaosConfig.fromArgs(new String[]{
                "--scenario", "CH-01",
                "--duration-sec", "60",
                "--fault-at-sec", "30",
                "--fault-duration-sec", "30"
        }));
    }

    @Test
    void rejectsChaosWithoutExplicitAuthorization() {
        ChaosConfig config = ChaosConfig.fromArgs(new String[]{
                "--scenario", "CH-01",
                "--duration-sec", "90",
                "--fault-at-sec", "20",
                "--fault-duration-sec", "20"
        });

        assertThrows(IllegalStateException.class, config::requireChaosAuthorization);
    }

    @Test
    void rejectsShortFlappingScenario() {
        assertThrows(IllegalArgumentException.class, () -> ChaosConfig.fromArgs(new String[]{
                "--scenario", "CH-04",
                "--duration-sec", "200",
                "--fault-at-sec", "30",
                "--fault-duration-sec", "10"
        }));
    }

    @Test
    void acceptsPoolLifecycleScenariosWithScenarioDefaults() {
        ChaosConfig timeout = ChaosConfig.fromArgs(new String[]{
                "--scenario", "CH-14", "--duration-sec", "120",
                "--fault-at-sec", "20", "--fault-duration-sec", "10"});
        assertEquals(6_000, timeout.transactionTimeoutMs());
        assertEquals(10_000, timeout.requestTimeoutMs());
        assertEquals(15_000, timeout.shutdownGraceMs());
        ChaosConfig forced = ChaosConfig.fromArgs(new String[]{
                "--scenario", "CH-11", "--duration-sec", "120",
                "--fault-at-sec", "20", "--fault-duration-sec", "10"});
        assertEquals(1, forced.shutdownGraceMs());
        assertEquals(0, forced.transactionTimeoutMs());
        assertThrows(IllegalArgumentException.class, () -> ChaosConfig.fromArgs(
                new String[]{"--scenario", "CH-16"}));
    }
}
