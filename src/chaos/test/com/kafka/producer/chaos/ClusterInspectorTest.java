package com.kafka.producer.chaos;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterInspectorTest {
    private final ClusterInspector inspector = new ClusterInspector("localhost:1", java.time.Duration.ofMillis(1));

    @Test
    void selectsStrictlyHeaviestLeaderWhenDistributionIsUneven() {
        var snapshot = new ClusterInspector.ClusterSnapshot("id", List.of(1, 2, 3),
                Map.of(new TopicPartition("t", 0), 1, new TopicPartition("t", 1), 1,
                        new TopicPartition("t", 2), 2), 0, 0);
        assertEquals(1, inspector.selectStrictlyLeaderHeavyBroker(snapshot));
    }

    @Test
    void rejectsBalancedLeaderDistribution() {
        var snapshot = new ClusterInspector.ClusterSnapshot("id", List.of(1, 2, 3),
                Map.of(new TopicPartition("t", 0), 1, new TopicPartition("t", 1), 2,
                        new TopicPartition("t", 2), 3), 0, 0);
        assertThrows(IllegalStateException.class,
                () -> inspector.selectStrictlyLeaderHeavyBroker(snapshot));
    }
}
