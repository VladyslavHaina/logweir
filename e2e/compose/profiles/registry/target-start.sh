#!/bin/bash
# Profile `registry`, service `registry-target` (PROD-03.2): a Redpanda node
# used only for its Schema Registry, the TARGET of a schema-aware restore.
# Its Schema Registry listener asks for HTTP basic auth; the one user,
# `logweir`, is a SCRAM user with the stack's fixture password, created once
# the node is healthy. `/tmp/ready` is the healthcheck's marker.
set -euo pipefail
: "${REGISTRY_PASSWORD:?the compose file sets REGISTRY_PASSWORD}"

rpk redpanda config set schema_registry.schema_registry_api \
  '[{name: sr, address: 0.0.0.0, port: 8081, authentication_method: http_basic}]'

rpk redpanda start \
  --mode dev-container \
  --smp 1 \
  --memory 512M \
  --default-log-level=warn \
  --kafka-addr internal://0.0.0.0:9092 \
  --advertise-kafka-addr internal://registry-target:9092 \
  --rpc-addr registry-target:33145 \
  --advertise-rpc-addr registry-target:33145 &
pid=$!
for _ in $(seq 1 90); do
  if rpk cluster health -X admin.hosts=localhost:9644 2>/dev/null | grep -q 'Healthy:.*true'; then
    break
  fi
  sleep 1
done
rpk security user create logweir -p "$REGISTRY_PASSWORD" --mechanism SCRAM-SHA-256 \
  -X admin.hosts=localhost:9644 \
  || rpk security user update logweir --new-password "$REGISTRY_PASSWORD" \
    --mechanism SCRAM-SHA-256 -X admin.hosts=localhost:9644
# Setting a subject's mode (IMPORT, and back) needs a superuser on Redpanda.
rpk cluster config set superusers '["logweir"]' -X admin.hosts=localhost:9644
touch /tmp/ready
wait "$pid"
