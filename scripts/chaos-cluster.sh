#!/usr/bin/env sh
set -eu

compose_file="docker-compose.chaos.yml"
bootstrap="localhost:19092"
topic="chaos-perf-test"
audit_topic="chaos-audit-test"
ch01_topic="chaos-ch01-leader-heavy"

case "${1:-}" in
  up)
    docker compose -f "$compose_file" up -d --wait
    ./scripts/chaos-network.sh init
    docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --if-not-exists \
      --topic "$topic" \
      --partitions 12 \
      --replication-factor 3 \
      --config min.insync.replicas=2 \
      --config unclean.leader.election.enable=false
    docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$audit_topic" \
      --partitions 12 --replication-factor 3 \
      --config min.insync.replicas=2 \
      --config unclean.leader.election.enable=false
    ;;
  down)
    docker compose -f "$compose_file" down -v
    ;;
  status)
    docker compose -f "$compose_file" ps
    ./scripts/chaos-network.sh status
    docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --describe --topic "$topic"
    ;;
  prepare-ch01)
    docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$ch01_topic" \
      --replica-assignment '1:2:3,1:3:2,1:2:3,1:3:2,1:2:3,1:3:2,2:1:3,2:3:1,2:1:3,3:1:2,3:2:1,3:1:2' \
      --config min.insync.replicas=2 \
      --config unclean.leader.election.enable=false
    for partition in 0 1 2 3 4 5 6 7 8 9 10 11; do
      docker compose -f "$compose_file" exec -T kafka-1 \
        /opt/kafka/bin/kafka-leader-election.sh \
        --bootstrap-server localhost:9092 \
        --election-type preferred --topic "$ch01_topic" --partition "$partition"
    done
    docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --describe --topic "$ch01_topic"
    ;;
  cluster-id)
    docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-cluster.sh \
      cluster-id --bootstrap-server localhost:9092 \
      | sed -n 's/^Cluster ID: Some(\(.*\))/\1/p; s/^Cluster ID: \(.*\)$/\1/p'
    ;;
  *)
    echo "Usage: $0 {up|down|status|cluster-id|prepare-ch01}" >&2
    exit 2
    ;;
esac
