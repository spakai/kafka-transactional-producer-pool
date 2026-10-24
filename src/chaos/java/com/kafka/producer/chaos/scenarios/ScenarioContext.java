package com.kafka.producer.chaos.scenarios;

import com.kafka.producer.chaos.ChaosConfig;
import com.kafka.producer.chaos.ChaosController;
import com.kafka.producer.chaos.ChaosEvent;
import com.kafka.producer.chaos.ClusterInspector;
import com.kafka.producer.chaos.LoadFaults;

import java.util.Map;
import java.util.function.Consumer;

public final class ScenarioContext {
    private final ChaosConfig config;
    private final ChaosController controller;
    private final ClusterInspector inspector;
    private final Consumer<ChaosEvent> events;
    private final ClusterInspector.ClusterSnapshot initialCluster;
    private volatile LoadFaults load;

    public ScenarioContext(
            ChaosConfig config,
            ChaosController controller,
            ClusterInspector inspector,
            Consumer<ChaosEvent> events,
            ClusterInspector.ClusterSnapshot initialCluster) {
        this.config = config;
        this.controller = controller;
        this.inspector = inspector;
        this.events = events;
        this.initialCluster = initialCluster;
    }

    public void event(String type, String outcome, String detail) {
        events.accept(ChaosEvent.of(type, outcome, detail));
    }

    public void event(String type, String outcome, String detail, Map<String, Object> attributes) {
        events.accept(ChaosEvent.of(type, outcome, detail, attributes));
    }

    public void attachLoad(LoadFaults load) { this.load = load; }

    public LoadFaults load() {
        LoadFaults attached = load;
        if (attached == null) {
            throw new IllegalStateException("Load engine is not attached yet");
        }
        return attached;
    }

    public ChaosConfig config() { return config; }
    public ChaosController controller() { return controller; }
    public ClusterInspector inspector() { return inspector; }
    public ClusterInspector.ClusterSnapshot initialCluster() { return initialCluster; }
}
