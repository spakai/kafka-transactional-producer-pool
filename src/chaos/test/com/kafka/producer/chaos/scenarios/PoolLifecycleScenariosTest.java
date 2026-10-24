package com.kafka.producer.chaos.scenarios;

import com.kafka.producer.chaos.ChaosConfig;
import com.kafka.producer.chaos.ChaosController;
import com.kafka.producer.chaos.LoadFaults;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PoolLifecycleScenariosTest {

    static final class FakeLoad implements LoadFaults {
        long fenced;
        long restarts;
        long stalls;
        Duration stall;
        boolean full = true;

        public int poolSize() { return 3; }
        public String transactionalId(int slot) { return "chaos-run-" + slot; }
        public void stallNextTransaction(Duration d) { stall = d; stalls++; }
        public long stallsConsumed() { return stalls; }
        public void restartPool() { restarts++; }
        public long poolRestarts() { return restarts; }
        public long shutdownRejections() { return 0; }
        public long producerFencedTotal() { return fenced; }
        public boolean atFullCapacity() { return full; }
        public String poolState() { return "HEALTHY"; }
    }

    private static ChaosConfig config(String scenario, String... extra) {
        List<String> args = new ArrayList<>(List.of("--scenario", scenario,
                "--duration-sec", "120", "--fault-at-sec", "10", "--fault-duration-sec", "30"));
        if (scenario.equals("CH-10")) {
            args.addAll(List.of("--commit-response-command", "arm",
                    "--capture-commit-response-command", "capture",
                    "--heal-network-command", "heal"));
        }
        args.addAll(List.of(extra));
        return ChaosConfig.fromArgs(args.toArray(new String[0]));
    }

    private static ScenarioContext context(ChaosConfig config, FakeLoad load, List<String> fenced) {
        ChaosController controller = (ChaosController) Proxy.newProxyInstance(
                ChaosController.class.getClassLoader(), new Class<?>[] {ChaosController.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("fenceProducer")) {
                        fenced.add((String) args[0]);
                        load.fenced++;
                    }
                    if (method.getName().equals("captureCommitResponseProof")) {
                        fenced.add("capture-proof");
                    }
                    return null;
                });
        ScenarioContext context = new ScenarioContext(config, controller, null, event -> { }, null);
        context.attachLoad(load);
        return context;
    }

    @Test
    void singleSlotPoisonFencesOnceAndRequiresFencingEvidence() throws Exception {
        FakeLoad load = new FakeLoad();
        List<String> fenced = new ArrayList<>();
        ScenarioContext context = context(config("CH-06"), load, fenced);
        ProducerPoisonScenario scenario = new ProducerPoisonScenario(
                ProducerPoisonScenario.Mode.SINGLE_SLOT);

        assertNotNull(scenario.evidenceGap(context, false));
        scenario.onTick(context, 9);
        assertEquals(0, fenced.size());
        scenario.onTick(context, 10);
        scenario.onTick(context, 11);
        assertEquals(List.of("chaos-run-0"), fenced);
        assertNull(scenario.evidenceGap(context, false));
    }

    @Test
    void allSlotsPoisonFencesEveryPoolTransactionalId() throws Exception {
        FakeLoad load = new FakeLoad();
        List<String> fenced = new ArrayList<>();
        ScenarioContext context = context(config("CH-07"), load, fenced);
        new ProducerPoisonScenario(ProducerPoisonScenario.Mode.ALL_SLOTS).onTick(context, 10);
        assertEquals(List.of("chaos-run-0", "chaos-run-1", "chaos-run-2"), fenced);
    }

    @Test
    void repeatedPoisonRotatesSlotsOnIntervalUntilFaultWindowEnds() throws Exception {
        FakeLoad load = new FakeLoad();
        List<String> fenced = new ArrayList<>();
        ScenarioContext context = context(config("CH-08", "--poison-interval-sec", "5"), load, fenced);
        ProducerPoisonScenario scenario = new ProducerPoisonScenario(
                ProducerPoisonScenario.Mode.REPEATED);
        for (long second = 0; second < 60; second++) {
            scenario.onTick(context, second);
        }
        assertEquals(List.of("chaos-run-0", "chaos-run-1", "chaos-run-2", "chaos-run-0",
                "chaos-run-1", "chaos-run-2"), fenced);
    }

    @Test
    void poolRestartScenarioFlagsIncompleteRestarts() {
        FakeLoad load = new FakeLoad();
        ScenarioContext context = context(config("CH-13", "--poison-interval-sec", "5"),
                load, new ArrayList<>());
        PoolRestartScenario scenario = PoolRestartScenario.rolling();
        for (long second = 0; second < 40; second++) {
            scenario.onTick(context, second);
        }
        assertEquals(3, load.restarts);
        assertNull(scenario.evidenceGap(context, false));
        load.restarts = 2;
        assertNotNull(scenario.evidenceGap(context, false));
    }

    @Test
    void transactionTimeoutStallsOnceAndNeedsFencing() {
        FakeLoad load = new FakeLoad();
        ScenarioContext context = context(config("CH-14", "--fault-duration-sec", "10"),
                load, new ArrayList<>());
        TransactionTimeoutScenario scenario = new TransactionTimeoutScenario();
        scenario.onStart(context);
        scenario.onTick(context, 10);
        scenario.onTick(context, 11);
        assertEquals(1, load.stalls);
        assertEquals(Duration.ofSeconds(10), load.stall);
        assertNotNull(scenario.evidenceGap(context, false));
        load.fenced = 1;
        assertNull(scenario.evidenceGap(context, false));
    }

    @Test
    void transactionTimeoutRejectsStallShorterThanBrokerTimeout() {
        ScenarioContext context = context(config("CH-14", "--fault-duration-sec", "5"),
                new FakeLoad(), new ArrayList<>());
        assertThrows(IllegalStateException.class,
                () -> new TransactionTimeoutScenario().onStart(context));
    }

    @Test
    void poisonDuringCommitCapturesBrokerProofAfterRequestTimeoutBeforeFencing() throws Exception {
        FakeLoad load = new FakeLoad();
        List<String> actions = new ArrayList<>();
        ScenarioContext context = context(config("CH-10"), load, actions);
        PoisonDuringCommitScenario scenario = new PoisonDuringCommitScenario();

        scenario.onStart(context);
        scenario.onTick(context, 10);
        scenario.onTick(context, 21);
        assertEquals(List.of(), actions);

        scenario.onTick(context, 22);
        assertEquals(List.of("capture-proof", "chaos-run-0", "chaos-run-1", "chaos-run-2"),
                actions);
        assertNull(scenario.evidenceGap(context, true));
    }

    @Test
    void poisonDuringCommitRejectsFaultWindowShorterThanRequestTimeoutGrace() {
        ScenarioContext context = context(config("CH-10", "--fault-duration-sec", "12"),
                new FakeLoad(), new ArrayList<>());
        assertThrows(IllegalStateException.class,
                () -> new PoisonDuringCommitScenario().onStart(context));
    }

    @Test
    void poisonDuringCommitNeedsProxyProof() {
        ScenarioContext context = context(config("CH-10"), new FakeLoad(), new ArrayList<>());
        assertNotNull(new PoisonDuringCommitScenario().evidenceGap(context, false));
    }
}
