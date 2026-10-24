package com.kafka.producer.chaos;

import com.kafka.producer.chaos.scenarios.BaselineScenario;
import com.kafka.producer.chaos.scenarios.BrokerFailureScenario;
import com.kafka.producer.chaos.scenarios.FlappingNetworkScenario;
import com.kafka.producer.chaos.scenarios.NetworkPartitionScenario;
import com.kafka.producer.chaos.scenarios.CommitResponsePartitionScenario;
import com.kafka.producer.chaos.scenarios.CoordinatorFailoverScenario;
import com.kafka.producer.chaos.scenarios.PoisonDuringCommitScenario;
import com.kafka.producer.chaos.scenarios.PoolRestartScenario;
import com.kafka.producer.chaos.scenarios.ProducerPoisonScenario;
import com.kafka.producer.chaos.scenarios.RebuildFailureScenario;
import com.kafka.producer.chaos.scenarios.TransactionTimeoutScenario;
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
    void dispatchesPoolLifecycleScenarios() {
        for (String id : new String[] {"CH-06", "CH-07", "CH-08"}) {
            assertInstanceOf(ProducerPoisonScenario.class, ChaosRunner.scenario(id));
        }
        assertInstanceOf(RebuildFailureScenario.class, ChaosRunner.scenario("CH-09"));
        assertInstanceOf(PoisonDuringCommitScenario.class, ChaosRunner.scenario("CH-10"));
        for (String id : new String[] {"CH-11", "CH-12", "CH-13"}) {
            assertInstanceOf(PoolRestartScenario.class, ChaosRunner.scenario(id));
        }
        assertInstanceOf(TransactionTimeoutScenario.class, ChaosRunner.scenario("CH-14"));
        assertInstanceOf(CoordinatorFailoverScenario.class, ChaosRunner.scenario("CH-15"));
    }

    @Test
    void ch10RequiresProtocolEvidenceBeforeItCanPass() {
        assertEquals("INVALID/INCOMPLETE", ChaosRunner.resultStatus("CH-10", true, false, 1));
        assertEquals("PASS", ChaosRunner.resultStatus("CH-10", true, true, 1));
    }

    @Test
    void ch05RequiresProtocolEvidenceBeforeItCanPass() {
        assertEquals("INVALID/INCOMPLETE", ChaosRunner.resultStatus("CH-05", true, false, 1));
        assertEquals("INVALID/INCOMPLETE", ChaosRunner.resultStatus("CH-05", true, true, 0));
        assertEquals("PASS", ChaosRunner.resultStatus("CH-05", true, true, 1));
        assertEquals("FAIL", ChaosRunner.resultStatus("CH-05", false, true, 1));
    }
}
