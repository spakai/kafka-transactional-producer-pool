package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosController;
import com.kafka.producer.chaos.ChaosEvent;

import java.time.Duration;
import java.util.function.Consumer;

/** Stateful, typed fault executor. Cleanup is registered before apply and is idempotent. */
public final class ExperimentExecutor implements AutoCloseable {
    private final ExperimentPlan plan;
    private final ChaosController controller;
    private final Consumer<ChaosEvent> events;
    private boolean applied;
    private boolean healed;
    private long flapStep = -1;

    public ExperimentExecutor(ExperimentPlan plan, ChaosController controller, Consumer<ChaosEvent> events) {
        this.plan = plan;
        this.controller = controller;
        this.events = events;
    }

    public void tick(Duration elapsed) throws Exception {
        if (plan.faultType() == FaultType.NONE) return;
        if (plan.faultType() == FaultType.FLAP_PRODUCER_NETWORK) {
            tickFlap(elapsed);
            return;
        }
        if (!applied && elapsed.compareTo(plan.startAfter()) >= 0) apply();
        if (applied && !healed && elapsed.compareTo(plan.startAfter().plus(plan.faultDuration())) >= 0) heal();
    }

    private void apply() throws Exception {
        events.accept(ChaosEvent.of("cleanup", "registered", plan.experimentId()));
        switch (plan.faultType()) {
            case STOP_NON_LEADER_BROKER, STOP_LEADER_BROKER -> controller.stopBroker(plan.brokerId());
            case PARTITION_PRODUCER_FROM_ONE_BROKER -> controller.partitionProducerFromBroker(plan.brokerId());
            case PARTITION_PRODUCER_FROM_CLUSTER -> controller.partitionProducerFromCluster();
            case DROP_COMMIT_RESPONSE -> controller.partitionCommitResponse();
            default -> throw new IllegalStateException("Unsupported apply: " + plan.faultType());
        }
        applied = true;
        events.accept(ChaosEvent.of("fault", "confirmed", plan.faultType().name()));
    }

    private void tickFlap(Duration elapsed) throws Exception {
        if (elapsed.compareTo(plan.startAfter()) < 0 || healed) return;
        long segmentSeconds = Math.max(1, plan.faultDuration().toSeconds() / 6);
        long sinceStart = elapsed.minus(plan.startAfter()).toSeconds();
        if (sinceStart >= plan.faultDuration().toSeconds()) { heal(); return; }
        long step = sinceStart / segmentSeconds;
        if (step == flapStep) return;
        flapStep = step;
        if (step % 2 == 0) {
            if (!applied) events.accept(ChaosEvent.of("cleanup", "registered", plan.experimentId()));
            controller.partitionProducerFromCluster();
            applied = true;
        } else controller.healNetwork();
    }

    public void heal() throws Exception {
        if (healed || !applied) { healed = true; return; }
        if (plan.faultType() == FaultType.STOP_LEADER_BROKER
                || plan.faultType() == FaultType.STOP_NON_LEADER_BROKER) {
            controller.startBroker(plan.brokerId());
            controller.waitForBrokerReady(plan.brokerId(), Duration.ofSeconds(90));
        } else controller.healNetwork();
        healed = true;
        events.accept(ChaosEvent.of("fault", "healed", plan.faultType().name()));
    }

    public void verifyCleanup() throws Exception { controller.verifyCleanup(); }
    public boolean applied() { return applied; }
    public boolean healed() { return healed; }

    @Override public void close() throws Exception {
        Exception failure = null;
        try { heal(); } catch (Exception e) { failure = e; }
        try { controller.close(); } catch (Exception e) {
            if (failure == null) failure = e; else failure.addSuppressed(e);
        }
        if (failure != null) throw failure;
    }
}
