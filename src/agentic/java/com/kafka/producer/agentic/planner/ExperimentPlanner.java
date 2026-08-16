package com.kafka.producer.agentic.planner;

import com.kafka.producer.agentic.ExperimentProposal;
import com.kafka.producer.chaos.ClusterInspector;

import java.util.List;

public interface ExperimentPlanner {
    ExperimentProposal propose(ClusterInspector.ClusterSnapshot snapshot, List<String> completedFamilies);
    String identity();
}
