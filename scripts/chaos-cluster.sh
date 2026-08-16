#!/usr/bin/env sh
set -eu

compose_file="docker-compose.chaos.yml"
bootstrap="localhost:19092"
topic="chaos-perf-test"

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
  cluster-id)
    docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-cluster.sh \
      cluster-id --bootstrap-server localhost:9092 \
      | sed -n 's/^Cluster ID: Some(\(.*\))/\1/p; s/^Cluster ID: \(.*\)$/\1/p'
    ;;
  *)
    echo "Usage: $0 {up|down|status|cluster-id}" >&2
    exit 2
    ;;
esac
