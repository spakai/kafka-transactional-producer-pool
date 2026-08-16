#!/usr/bin/env sh
set -eu

proxy_container="kafka-pool-chaos-proxy"
proxy_url="http://localhost:8474"

toxiproxy() {
  docker exec "$proxy_container" /toxiproxy-cli --host "$proxy_url" "$@"
}

proxy_name() {
  case "$1" in
    1) echo "kafka-1" ;;
    2) echo "kafka-2" ;;
    3) echo "kafka-3" ;;
    *) echo "Unknown broker ID: $1" >&2; exit 2 ;;
  esac
}

add_timeout() {
  name="$1"
  direction="$2"
  toxic_name="agentic-${direction}"
  if toxiproxy toxic remove --toxicName "$toxic_name" "$name" >/dev/null 2>&1; then :; fi
  toxiproxy toxic add --toxicName "$toxic_name" --type timeout \
    --attribute timeout=0 --upstream="$(test "$direction" = upstream && echo true || echo false)" \
    "$name" >/dev/null
}

heal_proxy() {
  name="$1"
  if toxiproxy toxic remove --toxicName agentic-upstream "$name" >/dev/null 2>&1; then :; fi
  if toxiproxy toxic remove --toxicName agentic-downstream "$name" >/dev/null 2>&1; then :; fi
}

case "${1:-}" in
  init)
    for broker_id in 1 2 3; do
      name="$(proxy_name "$broker_id")"
      listen_port="${broker_id}9092"
      if ! toxiproxy inspect "$name" >/dev/null 2>&1; then
        toxiproxy create --listen "0.0.0.0:${listen_port}" \
          --upstream "${name}:${listen_port}" "$name" >/dev/null
      fi
    done
    ;;
  partition-broker)
    name="$(proxy_name "${2:?broker ID required}")"
    add_timeout "$name" upstream
    add_timeout "$name" downstream
    ;;
  partition-cluster)
    for name in kafka-1 kafka-2 kafka-3; do
      add_timeout "$name" upstream
      add_timeout "$name" downstream
    done
    ;;
  drop-responses)
    for name in kafka-1 kafka-2 kafka-3; do
      add_timeout "$name" downstream
    done
    ;;
  heal)
    for name in kafka-1 kafka-2 kafka-3; do
      heal_proxy "$name"
    done
    ;;
  status)
    toxiproxy list
    ;;
  *)
    echo "Usage: $0 {init|partition-broker <id>|partition-cluster|drop-responses|heal|status}" >&2
    exit 2
    ;;
esac
