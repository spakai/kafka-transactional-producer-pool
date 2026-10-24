#!/usr/bin/env sh
set -eu

admin="http://127.0.0.1:18080"

with_broker_state() {
  printf '%s\n' "$1" | python3 -c '
import json, subprocess, sys
d = json.load(sys.stdin)
transactional_ids = sorted({
    e["transactional_id"] for e in d.get("events", [])
    if e.get("event") == "endtxn_request_forwarded" and e.get("transactional_id")
})
states = []
for transactional_id in transactional_ids:
    try:
        output = subprocess.check_output([
            "docker", "exec", "kafka-pool-chaos-1",
            "/opt/kafka/bin/kafka-transactions.sh",
            "--bootstrap-server", "localhost:9092", "describe",
            "--transactional-id", transactional_id,
        ], text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as error:
        output = error.output
    states.append({"transactional_id": transactional_id, "describe": output.strip()})
d["broker_transaction_states"] = states
d["broker_state_proof"] = bool(states) and all(
    "CompleteCommit" in item["describe"] for item in states
)
print(json.dumps(d, sort_keys=True))
'
}

case "${1:-}" in
  arm)
    curl --fail --silent --show-error --request POST "$admin/arm"
    ;;
  capture)
    response="$(curl --fail --silent --show-error "$admin/status")"
    with_broker_state "$response"
    ;;
  heal)
    response="$(curl --fail --silent --show-error --request POST "$admin/heal")"
    with_broker_state "$response"
    ;;
  status)
    curl --fail --silent --show-error "$admin/status"
    ;;
  *)
    echo "Usage: $0 {arm|capture|heal|status}" >&2
    exit 2
    ;;
esac
