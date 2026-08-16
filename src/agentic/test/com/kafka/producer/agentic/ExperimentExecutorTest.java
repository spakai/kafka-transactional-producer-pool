package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosController;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentExecutorTest {
    @Test void appliesAndHealsTypedBrokerFault() throws Exception {
        FakeController controller = new FakeController();
        ExperimentExecutor executor = new ExperimentExecutor(
                TestFixtures.plan(FaultType.STOP_LEADER_BROKER), controller, ignored -> {});
        executor.tick(Duration.ofSeconds(5));
        assertTrue(controller.brokerStopped);
        executor.tick(Duration.ofSeconds(15));
        assertFalse(controller.brokerStopped);
        executor.verifyCleanup();
        assertTrue(executor.healed());
    }

    @Test void closeHealsActiveFault() throws Exception {
        FakeController controller = new FakeController();
        ExperimentExecutor executor = new ExperimentExecutor(
                TestFixtures.plan(FaultType.PARTITION_PRODUCER_FROM_CLUSTER), controller, ignored -> {});
        executor.tick(Duration.ofSeconds(5));
        assertTrue(controller.networkFault);
        executor.close();
        assertFalse(controller.networkFault);
    }

    private static final class FakeController implements ChaosController {
        boolean brokerStopped;
        boolean networkFault;
        public void stopBroker(int brokerId) { brokerStopped = true; }
        public void startBroker(int brokerId) { brokerStopped = false; }
        public void waitForBrokerReady(int brokerId, Duration timeout) {}
        public void partitionProducerFromBroker(int brokerId) { networkFault = true; }
        public void partitionProducerFromCluster() { networkFault = true; }
        public void partitionCommitResponse() { networkFault = true; }
        public void healNetwork() { networkFault = false; }
        public void verifyCleanup() { if (brokerStopped || networkFault) throw new IllegalStateException(); }
        public void close() { brokerStopped = false; networkFault = false; }
    }
}
