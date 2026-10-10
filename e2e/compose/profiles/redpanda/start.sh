#!/bin/bash
# Profile `redpanda` (PROD-01.2): write the node's listeners, then start it.
#
# `rpk redpanda start` takes listener addresses as flags but not a listener's
# authentication method, which is a node property (`redpanda.kafka_api[]`), so
# the three listeners are written into the node configuration first:
#   internal  :9094  no authentication (in-network clients, and this profile's setup)
#   external  :9092  no authentication (the published PLAINTEXT port)
#   sasl      :9093  SASL (SCRAM-SHA-256 and SCRAM-SHA-512), no TLS
# ADVERTISED comes from the compose file, so the two host-facing names carry the
# published ports' LOGWEIR_E2E_*_PORT variables and a slot moves them
# (e2e/tests/stack_params.rs checks it).
set -euo pipefail
: "${ADVERTISED:?the compose file sets ADVERTISED (NAME://host:port,...)}"

rpk redpanda config set redpanda.kafka_api \
  '[{name: internal, address: 0.0.0.0, port: 9094},
    {name: external, address: 0.0.0.0, port: 9092},
    {name: sasl, address: 0.0.0.0, port: 9093, authentication_method: sasl}]'

exec rpk redpanda start \
  --mode dev-container \
  --smp 1 \
  --memory 1G \
  --default-log-level=warn \
  --advertise-kafka-addr "$ADVERTISED" \
  --rpc-addr redpanda:33145 \
  --advertise-rpc-addr redpanda:33145
