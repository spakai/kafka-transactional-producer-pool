package com.kafka.producer.chaos.controller;

import com.kafka.producer.chaos.ChaosConfig;
import com.kafka.producer.chaos.ChaosController;
import com.kafka.producer.chaos.ChaosEvent;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public final class DockerChaosController implements ChaosController {
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(30);

    private final ChaosConfig config;
    private final Consumer<ChaosEvent> events;
    private final Set<Integer> stoppedBrokers = new HashSet<>();
    private boolean networkPartitioned;
    private boolean commitResponseProofConfirmed;

    public DockerChaosController(ChaosConfig config, Consumer<ChaosEvent> events) {
        this.config = config;
        this.events = events;
    }

    @Override
    public synchronized void stopBroker(int brokerId) throws Exception {
        config.requireChaosAuthorization();
        String container = container(brokerId);
        events.accept(ChaosEvent.of("broker-stop", "requested", container,
                Map.of("brokerId", brokerId)));
        CommandExecutor.run(java.util.List.of("docker", "stop", "--time", "15", container),
                COMMAND_TIMEOUT);
        stoppedBrokers.add(brokerId);
        events.accept(ChaosEvent.of("broker-stop", "applied", container,
                Map.of("brokerId", brokerId)));
    }

    @Override
    public synchronized void startBroker(int brokerId) throws Exception {
        config.requireChaosAuthorization();
        String container = container(brokerId);
        events.accept(ChaosEvent.of("broker-start", "requested", container,
                Map.of("brokerId", brokerId)));
        CommandExecutor.run(java.util.List.of("docker", "start", container), COMMAND_TIMEOUT);
        stoppedBrokers.remove(brokerId);
        events.accept(ChaosEvent.of("broker-start", "applied", container,
                Map.of("brokerId", brokerId)));
    }

    @Override
    public void waitForBrokerReady(int brokerId, Duration timeout) throws Exception {
        String container = container(brokerId);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            String state = CommandExecutor.run(
                    java.util.List.of("docker", "inspect", "--format", "{{.State.Health.Status}}", container),
                    COMMAND_TIMEOUT);
            if ("healthy".equals(state)) {
                events.accept(ChaosEvent.of("broker-ready", "confirmed", container,
                        Map.of("brokerId", brokerId)));
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Broker " + brokerId + " did not become healthy");
    }

    @Override
    public synchronized void partitionProducerFromBroker(int brokerId) throws Exception {
        config.requireChaosAuthorization();
        if (config.partitionBrokerCommand().isBlank()) {
            throw new IllegalStateException(
                    "CH-02 requires --partition-broker-command and --heal-network-command");
        }
        String command = expand(config.partitionBrokerCommand(), brokerId);
        events.accept(ChaosEvent.of("network-partition-broker", "requested", "broker=" + brokerId));
        CommandExecutor.runShell(command, COMMAND_TIMEOUT);
        networkPartitioned = true;
        events.accept(ChaosEvent.of("network-partition-broker", "applied", "broker=" + brokerId));
    }

    @Override
    public synchronized void partitionProducerFromCluster() throws Exception {
        config.requireChaosAuthorization();
        if (config.partitionClusterCommand().isBlank()) {
            throw new IllegalStateException(
                    "Network chaos requires --partition-cluster-command and --heal-network-command");
        }
        events.accept(ChaosEvent.of("network-partition-cluster", "requested", "all brokers"));
        CommandExecutor.runShell(config.partitionClusterCommand(), COMMAND_TIMEOUT);
        networkPartitioned = true;
        events.accept(ChaosEvent.of("network-partition-cluster", "applied", "all brokers"));
    }

    @Override
    public synchronized void partitionCommitResponse() throws Exception {
        config.requireChaosAuthorization();
        if (config.commitResponseCommand().isBlank()) {
            throw new IllegalStateException(
                    "CH-05 requires --commit-response-command and --heal-network-command");
        }
        events.accept(ChaosEvent.of("commit-response-partition", "requested",
                "commit response fault hook"));
        String output = CommandExecutor.runShell(config.commitResponseCommand(), COMMAND_TIMEOUT);
        networkPartitioned = true;
        events.accept(ChaosEvent.of("commit-response-partition", "applied",
                "commit response fault hook", Map.of("hookOutput", output)));
    }

    @Override
    public synchronized void captureCommitResponseProof() throws Exception {
        if (!config.scenario().equals("CH-10")) {
            throw new IllegalStateException("Early broker proof capture is only used by CH-10");
        }
        if (config.captureCommitResponseCommand().isBlank()) {
            throw new IllegalStateException(
                    "CH-10 requires --capture-commit-response-command");
        }
        String output = CommandExecutor.runShell(
                config.captureCommitResponseCommand(), COMMAND_TIMEOUT);
        boolean captured = output.contains("ENDTXN_RESPONSE_DROPPED")
                && output.contains("\"broker_state_proof\": true");
        commitResponseProofConfirmed |= captured;
        events.accept(ChaosEvent.of("commit-response-proof-capture",
                captured ? "confirmed" : "missing",
                "proxy and broker state captured before producer fencing",
                Map.of("proof", output)));
    }

    @Override
    public void fenceProducer(String transactionalId) throws Exception {
        config.requireChaosAuthorization();
        events.accept(ChaosEvent.of("producer-fence", "requested", transactionalId));
        java.util.Properties props = new java.util.Properties();
        props.setProperty("bootstrap.servers", config.bootstrapServers());
        props.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty("transactional.id", transactionalId);
        props.setProperty("max.block.ms", "15000");
        try (org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> intruder =
                     new org.apache.kafka.clients.producer.KafkaProducer<>(props)) {
            intruder.initTransactions();
        }
        events.accept(ChaosEvent.of("producer-fence", "applied", transactionalId,
                Map.of("transactionalId", transactionalId)));
    }

    @Override
    public synchronized void healNetwork() throws Exception {
        if (!networkPartitioned) {
            return;
        }
        if (config.healNetworkCommand().isBlank()) {
            throw new IllegalStateException("Network partition active but no heal command configured");
        }
        events.accept(ChaosEvent.of("network-heal", "requested", "all configured rules"));
        String output = CommandExecutor.runShell(config.healNetworkCommand(), COMMAND_TIMEOUT);
        networkPartitioned = false;
        if (config.scenario().equals("CH-05") || config.scenario().equals("CH-10")) {
            boolean healedProof = output.contains("ENDTXN_RESPONSE_DROPPED")
                    && output.contains("\"broker_state_proof\": true");
            commitResponseProofConfirmed |= healedProof;
            if (healedProof || !commitResponseProofConfirmed) {
                events.accept(ChaosEvent.of("commit-response-proof",
                        commitResponseProofConfirmed ? "confirmed" : "missing",
                        "protocol proxy evidence", Map.of("proxyEvidence", output)));
            } else {
                events.accept(ChaosEvent.of("commit-response-proof", "retained",
                        "broker state proof was captured before producer fencing",
                        Map.of("proxyEvidence", output)));
            }
        }
        events.accept(ChaosEvent.of("network-heal", "applied", "all configured rules",
                Map.of("hookOutput", output)));
    }

    @Override
    public synchronized void verifyCleanup() throws Exception {
        if (networkPartitioned || !stoppedBrokers.isEmpty()) {
            throw new IllegalStateException(
                    "Chaos cleanup incomplete: networkPartitioned=" + networkPartitioned
                            + ", stoppedBrokers=" + stoppedBrokers);
        }
        for (Map.Entry<Integer, String> broker : config.brokerContainers().entrySet()) {
            String running = CommandExecutor.run(
                    java.util.List.of("docker", "inspect", "--format", "{{.State.Running}}",
                            broker.getValue()),
                    COMMAND_TIMEOUT);
            if (!"true".equals(running)) {
                throw new IllegalStateException("Broker container is not running: " + broker);
            }
        }
        events.accept(ChaosEvent.of("cleanup", "confirmed", "network healed and brokers running"));
    }

    public synchronized boolean commitResponseProofConfirmed() {
        return commitResponseProofConfirmed;
    }

    @Override
    public synchronized void close() throws Exception {
        Exception failure = null;
        try {
            healNetwork();
        } catch (Exception e) {
            failure = e;
        }
        for (int brokerId : Set.copyOf(stoppedBrokers)) {
            try {
                startBroker(brokerId);
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private String container(int brokerId) {
        String container = config.brokerContainers().get(brokerId);
        if (container == null) {
            throw new IllegalArgumentException("No container mapping for broker " + brokerId);
        }
        return container;
    }

    private String expand(String command, int brokerId) {
        return command
                .replace("{brokerId}", String.valueOf(brokerId))
                .replace("{container}", container(brokerId));
    }
}
