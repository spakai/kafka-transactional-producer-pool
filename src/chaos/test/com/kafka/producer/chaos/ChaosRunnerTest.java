package com.kafka.producer.chaos;

import com.kafka.producer.chaos.scenarios.BaselineScenario;
import com.kafka.producer.chaos.scenarios.BrokerFailureScenario;
import com.kafka.producer.chaos.scenarios.FlappingNetworkScenario;
import com.kafka.producer.chaos.scenarios.NetworkPartitionScenario;
import com.kafka.producer.chaos.scenarios.CommitResponsePartitionScenario;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChaosRunnerTest {

    @Test
    void dispatchesEveryScenarioFamily() {
        assertInstanceOf(BaselineScenario.class, ChaosRunner.scenario("MB-01"));
        assertInstanceOf(BrokerFailureScenario.class, ChaosRunner.scenario("MB-03"));
        assertInstanceOf(BrokerFailureScenario.class, ChaosRunner.scenario("CH-01"));
        assertInstanceOf(NetworkPartitionScenario.class, ChaosRunner.scenario("CH-02"));
        assertInstanceOf(NetworkPartitionScenario.class, ChaosRunner.scenario("CH-03"));
        assertInstanceOf(FlappingNetworkScenario.class, ChaosRunner.scenario("CH-04"));
        assertInstanceOf(CommitResponsePartitionScenario.class, ChaosRunner.scenario("CH-05"));
        assertThrows(IllegalArgumentException.class, () -> ChaosRunner.scenario("UNKNOWN"));
    }

    @Test
    void ch05RequiresProtocolEvidenceBeforeItCanPass() {
        assertEquals("INVALID/INCOMPLETE", ChaosRunner.resultStatus("CH-05", true, false, 1));
        assertEquals("INVALID/INCOMPLETE", ChaosRunner.resultStatus("CH-05", true, true, 0));
        assertEquals("PASS", ChaosRunner.resultStatus("CH-05", true, true, 1));
        assertEquals("FAIL", ChaosRunner.resultStatus("CH-05", false, true, 1));
    }
}
