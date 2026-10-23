package com.kafka.producer.chaos;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class ClusterInspector implements AutoCloseable {
    private final Admin admin;
    private final Duration timeout;

    public ClusterInspector(String bootstrapServers, Duration timeout) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
        this.admin = Admin.create(properties);
        this.timeout = timeout;
    }

    public ClusterSnapshot inspect(String topic) throws Exception {
        long timeoutMs = timeout.toMillis();
        DescribeClusterResult cluster = admin.describeCluster();
        String clusterId = normalizeClusterId(
                cluster.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS));
        List<Integer> brokers = cluster.nodes().get(timeoutMs, TimeUnit.MILLISECONDS)
                .stream().map(Node::id).sorted().toList();
        TopicDescription description = admin.describeTopics(List.of(topic))
                .allTopicNames().get(timeoutMs, TimeUnit.MILLISECONDS).get(topic);

        Map<TopicPartition, Integer> leaders = new LinkedHashMap<>();
        int underReplicated = 0;
        int offline = 0;
        for (TopicPartitionInfo partition : description.partitions()) {
            TopicPartition key = new TopicPartition(topic, partition.partition());
            if (partition.leader() == null) {
                offline++;
            } else {
                leaders.put(key, partition.leader().id());
            }
            if (partition.isr().size() < partition.replicas().size()) {
                underReplicated++;
            }
        }
        return new ClusterSnapshot(clusterId, brokers, leaders, underReplicated, offline);
    }

    public int selectLeaderBroker(ClusterSnapshot snapshot) {
        return snapshot.leaders().values().stream()
                .collect(Collectors.groupingBy(id -> id, Collectors.counting()))
                .entrySet().stream()
                .max(Map.Entry.<Integer, Long>comparingByValue()
                        .thenComparing(Map.Entry.comparingByKey()))
                .orElseThrow(() -> new IllegalStateException("Topic has no online partition leaders"))
                .getKey();
    }

    /** Selects a strictly leader-heavy broker, rejecting tied or balanced layouts. */
    public int selectStrictlyLeaderHeavyBroker(ClusterSnapshot snapshot) {
        Map<Integer, Long> leaderCounts = snapshot.brokerIds().stream()
                .collect(Collectors.toMap(id -> id, id -> 0L));
        snapshot.leaders().values().forEach(id -> leaderCounts.computeIfPresent(id,
                (key, count) -> count + 1));
        long maximum = leaderCounts.values().stream().mapToLong(Long::longValue).max()
                .orElseThrow(() -> new IllegalStateException("Cluster has no brokers"));
        long minimum = leaderCounts.values().stream().mapToLong(Long::longValue).min()
                .orElseThrow(() -> new IllegalStateException("Cluster has no brokers"));
        if (maximum == minimum) {
            throw new IllegalStateException("No strictly leader-heavy broker; leader counts=" + leaderCounts);
        }
        return leaderCounts.entrySet().stream()
                .filter(entry -> entry.getValue() == maximum)
                .map(Map.Entry::getKey)
                .min(Integer::compareTo)
                .orElseThrow();
    }

    public int selectLeastLeadingBroker(ClusterSnapshot snapshot) {
        Map<Integer, Long> leaderCounts = snapshot.brokerIds().stream()
                .collect(Collectors.toMap(id -> id, id -> 0L));
        snapshot.leaders().values().forEach(id -> leaderCounts.computeIfPresent(id, (key, count) -> count + 1));
        return leaderCounts.entrySet().stream()
                .min(Map.Entry.<Integer, Long>comparingByValue()
                        .thenComparing(Map.Entry.comparingByKey()))
                .orElseThrow(() -> new IllegalStateException("Cluster has no brokers"))
                .getKey();
    }

    public int selectLeaderForPartition(ClusterSnapshot snapshot, String topic, int partition) {
        Integer leader = snapshot.leaders().get(new TopicPartition(topic, partition));
        if (leader == null) {
            throw new IllegalStateException(
                    "No online leader for " + topic + "-" + partition);
        }
        return leader;
    }

    public int selectNonLeaderForPartition(ClusterSnapshot snapshot, String topic, int partition) {
        int leader = selectLeaderForPartition(snapshot, topic, partition);
        return snapshot.brokerIds().stream()
                .filter(id -> id != leader)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No non-leader broker for " + topic + "-" + partition));
    }

    public void awaitBrokerCount(String topic, int minimumBrokers, Duration wait) throws Exception {
        long deadline = System.nanoTime() + wait.toNanos();
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                if (inspect(topic).brokerIds().size() >= minimumBrokers) {
                    return;
                }
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Timed out waiting for " + minimumBrokers + " brokers", last);
    }

    public void awaitFullyReplicated(String topic, Duration wait) throws Exception {
        long deadline = System.nanoTime() + wait.toNanos();
        ClusterSnapshot last = null;
        while (System.nanoTime() < deadline) {
            try {
                last = inspect(topic);
                if (last.underReplicatedPartitions() == 0 && last.offlinePartitions() == 0) {
                    return;
                }
            } catch (Exception ignored) {
                // Cluster may be electing leaders; retry within the caller's bound.
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Timed out waiting for full replication; last snapshot=" + last);
    }

    public void verifyAllowlisted(ClusterSnapshot snapshot, String allowedClusterId) {
        if (!snapshot.clusterId().equals(normalizeClusterId(allowedClusterId))) {
            throw new IllegalStateException(
                    "Refusing chaos against cluster " + snapshot.clusterId()
                            + "; expected allow-listed cluster " + allowedClusterId);
        }
        if (snapshot.brokerIds().size() != 3) {
            throw new IllegalStateException(
                    "Spec04 requires exactly three visible brokers; found " + snapshot.brokerIds());
        }
    }

    /** Kafka tooling and some client versions render the ID as {@code Some(id)}. */
    static String normalizeClusterId(String clusterId) {
        if (clusterId == null) {
            return null;
        }
        String value = clusterId.trim();
        if (value.startsWith("Some(") && value.endsWith(")")) {
            return value.substring("Some(".length(), value.length() - 1);
        }
        return value;
    }

    @Override
    public void close() {
        admin.close(timeout);
    }

    public record ClusterSnapshot(
            String clusterId,
            List<Integer> brokerIds,
            Map<TopicPartition, Integer> leaders,
            int underReplicatedPartitions,
            int offlinePartitions) {

        public ClusterSnapshot {
            brokerIds = List.copyOf(brokerIds);
            leaders = Map.copyOf(leaders);
        }

        public Set<Integer> leaderBrokerIds() {
            return Set.copyOf(leaders.values());
        }
    }
}
