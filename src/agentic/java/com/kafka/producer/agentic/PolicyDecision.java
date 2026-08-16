package com.kafka.producer.agentic;

import java.util.List;

public record PolicyDecision(boolean approved, ExperimentPlan plan, List<String> reasons) {
    public PolicyDecision { reasons = List.copyOf(reasons); }
    public static PolicyDecision approve(ExperimentPlan plan) { return new PolicyDecision(true, plan, List.of()); }
    public static PolicyDecision reject(String... reasons) { return new PolicyDecision(false, null, List.of(reasons)); }
}
