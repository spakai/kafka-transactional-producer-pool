package com.kafka.producer.chaos;

import com.kafka.producer.chaos.controller.DockerChaosController;
import com.kafka.producer.chaos.scenarios.BaselineScenario;
import com.kafka.producer.chaos.scenarios.BrokerFailureScenario;
import com.kafka.producer.chaos.scenarios.ChaosScenario;
import com.kafka.producer.chaos.scenarios.CommitResponsePartitionScenario;
import com.kafka.producer.chaos.scenarios.FlappingNetworkScenario;
import com.kafka.producer.chaos.scenarios.NetworkPartitionScenario;
import com.kafka.producer.chaos.scenarios.ScenarioContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class ChaosRunner {
    private static final Set<String> FAULT_SCENARIOS =
            Set.of("MB-03", "MB-04", "CH-01", "CH-02", "CH-03", "CH-04", "CH-05");

    private ChaosRunner() {}

    public static void main(String[] args) throws Exception {
        ChaosConfig config = ChaosConfig.fromArgs(args);
        String runId = config.scenario().toLowerCase() + "-" + UUID.randomUUID();
        EventRecorder events = new EventRecorder();
        PublishLedger ledger = new PublishLedger();
        List<ChaosLoadEngine.Sample> samples = new ArrayList<>();

        events.accept(ChaosEvent.of("run", "started", runId));
        VerificationReport verification;
        CorrectnessVerifier verifier = new CorrectnessVerifier(config);
        java.util.Map<org.apache.kafka.common.TopicPartition, Long> startOffsets;
        try (ClusterInspector inspector =
                     new ClusterInspector(config.bootstrapServers(), Duration.ofSeconds(15));
             DockerChaosController controller = new DockerChaosController(config, events)) {
            ClusterInspector.ClusterSnapshot initial = inspector.inspect(config.topic());
            validateCluster(config, inspector, initial);
            recordClusterSnapshot(events, "start", initial);

            ScenarioContext context =
                    new ScenarioContext(config, controller, inspector, events, initial);
            ChaosScenario scenario = scenario(config.scenario());
            scenario.onStart(context);
            startOffsets = verifier.captureStartOffsets();

            try (ChaosLoadEngine load = new ChaosLoadEngine(config, runId, ledger)) {
                load.start();
                long started = System.nanoTime();
                long durationSeconds = config.duration().toSeconds();
                long nextSample = started;
                while (true) {
                    long elapsedSecond = Duration.ofNanos(
                            System.nanoTime() - started).toSeconds();
                    if (elapsedSecond >= durationSeconds) {
                        break;
                    }
                    scenario.onTick(context, elapsedSecond);
                    ClusterInspector.ClusterSnapshot cluster = scenario.shouldInspectCluster()
                            ? inspectBestEffort(inspector, config.topic(), events) : null;
                    samples.add(load.sample(elapsedSecond, cluster));
                    nextSample += Duration.ofSeconds(1).toNanos();
                    sleepUntil(nextSample);
                }
                scenario.onFinish(context);
            }

            if (FAULT_SCENARIOS.contains(config.scenario())) {
                controller.verifyCleanup();
                inspector.awaitBrokerCount(config.topic(), 3, config.recoveryTimeout());
                inspector.awaitFullyReplicated(config.topic(), config.recoveryTimeout());
            }
            java.util.Map<org.apache.kafka.common.TopicPartition, Long> endOffsets =
                    verifier.captureEndOffsets();
            recordClusterSnapshot(events, "end", inspector.inspect(config.topic()));
            verification = verifier.verify(runId, ledger, startOffsets, endOffsets);
            events.accept(ChaosEvent.of("verification",
                    verification.passed() ? "passed" : "failed",
                    "issues=" + verification.issues().size()));
        } catch (Exception error) {
            events.accept(ChaosEvent.of("run", "failed", error.toString()));
            throw error;
        }

        events.accept(ChaosEvent.of("run", "completed", runId));
        String resultStatus = config.scenario().equals("CH-05")
                ? "INVALID/INCOMPLETE" : verification.passed() ? "PASS" : "FAIL";
        String resultDetail = config.scenario().equals("CH-05")
                ? "TCP response drop does not prove a correlated EndTxn request/response loss" : "";
        ChaosResultWriter.ResultFiles files = new ChaosResultWriter()
                .write(config, runId, samples, events.snapshot(), verification,
                        resultStatus, resultDetail);
        System.out.println("Chaos scenario " + config.scenario()
                + " completed: " + resultStatus);
        System.out.println("Samples: " + files.samples());
        System.out.println("Events: " + files.events());
        System.out.println("Verification: " + files.verification());
        System.out.println("Offset ranges: " + files.offsets());
        System.out.println("Partition counts: " + files.partitionCounts());
        if (!resultStatus.equals("PASS")) {
            throw new IllegalStateException(
                    "Chaos scenario " + resultStatus + ": "
                            + (verification.issues().isEmpty() ? resultDetail : verification.issues()));
        }
    }

    private static void validateCluster(
            ChaosConfig config,
            ClusterInspector inspector,
            ClusterInspector.ClusterSnapshot initial) {
        if (initial.brokerIds().size() != 3) {
            throw new IllegalStateException(
                    "Spec04 requires exactly three visible brokers; found " + initial.brokerIds());
        }
        if (FAULT_SCENARIOS.contains(config.scenario())) {
            config.requireChaosAuthorization();
            inspector.verifyAllowlisted(initial, config.allowedClusterId());
        } else if (!config.allowedClusterId().isBlank()
                && !initial.clusterId().equals(config.allowedClusterId())) {
            throw new IllegalStateException(
                    "Connected cluster does not match --cluster-allowlist");
        }
    }

    private static ClusterInspector.ClusterSnapshot inspectBestEffort(
            ClusterInspector inspector, String topic, EventRecorder events) {
        try {
            return inspector.inspect(topic);
        } catch (Exception error) {
            events.accept(ChaosEvent.of("cluster-snapshot", "unavailable", error.toString()));
            return null;
        }
    }

    private static void recordClusterSnapshot(EventRecorder events, String phase,
            ClusterInspector.ClusterSnapshot snapshot) {
        events.accept(ChaosEvent.of("cluster-snapshot", "recorded", phase,
                java.util.Map.of("clusterId", snapshot.clusterId(),
                        "brokers", snapshot.brokerIds(), "leaders", snapshot.leaders(),
                        "underReplicated", snapshot.underReplicatedPartitions(),
                        "offline", snapshot.offlinePartitions())));
    }

    private static void sleepUntil(long deadlineNanos) throws InterruptedException {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining > 0) {
            java.util.concurrent.TimeUnit.NANOSECONDS.sleep(remaining);
        }
    }

    static ChaosScenario scenario(String scenario) {
        return switch (scenario) {
            case "MB-01", "MB-02" -> new BaselineScenario();
            case "MB-03" -> new BrokerFailureScenario(false, true);
            case "MB-04" -> new BrokerFailureScenario(true, true);
            case "CH-01" -> new BrokerFailureScenario(true, false);
            case "CH-02" -> new NetworkPartitionScenario(true);
            case "CH-03" -> new NetworkPartitionScenario(false);
            case "CH-05" -> new CommitResponsePartitionScenario();
            case "CH-04" -> new FlappingNetworkScenario();
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        };
    }
}
