#!/usr/bin/env bash
# Print the environment for ONE compose stack (PROD-01.5), for `eval`:
#
#   eval "$(e2e/compose/stack-env.sh --slot 2 --kafka 4.1 --profiles auth)"
#   just e2e-up && ./scripts/e2e-seed.sh && just e2e && just e2e-down
#
#   --slot N       0 = the default stack (project logweir-e2e, ports 9092,
#                  9095, 9097, 9000, 9001 — every variable is UNSET, so the
#                  compose file's own defaults apply). 1..4 = project
#                  logweir-e2e-sN with every host port moved by N*10000.
#   --kafka LINE   a broker line (see --lines) or an exact Apache Kafka
#                  version; exported as KAFKA_VERSION. Omitted: KAFKA_VERSION
#                  is left alone, so e2e/compose/.env's pin applies.
#   --profiles P   comma-separated optional profiles (see --profiles-list),
#                  exported as COMPOSE_PROFILES. Omitted: none.
#   --check        exit 1, saying why, when the CURRENT environment's project
#                  and ports disagree (`just e2e-up` / `e2e-down` run this).
#   --lines        print the broker lines and their pinned versions.
#   --profiles-list  print the optional profiles and what each provides.
#
# Slots never share a project, a host port, a volume or a network, so two
# slots run at once with no lock. Slot 0 is the stack other people use: hold
# `claude/compose-lock.sh` for it as before. The guide is e2e/README.md.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

# THE PORT TABLE: every published host port of docker-compose.yml, as
# VARIABLE DEFAULT. `e2e/tests/stack_params.rs` checks it against the compose
# file's `${VARIABLE:-DEFAULT}` spellings and against e2e/tests/harness/stack.rs.
PORTS="
LOGWEIR_E2E_KAFKA_PORT 9092
LOGWEIR_E2E_K8S_PORT 9095
LOGWEIR_E2E_SASL_PORT 9097
LOGWEIR_E2E_S3_PORT 9000
LOGWEIR_E2E_S3_CONSOLE_PORT 9001
LOGWEIR_E2E_AUTH_PLAIN_PORT 9102
LOGWEIR_E2E_AUTH_SCRAM256_PORT 9103
LOGWEIR_E2E_AUTH_MTLS_PORT 9104
LOGWEIR_E2E_C3_1_PORT 9112
LOGWEIR_E2E_C3_2_PORT 9113
LOGWEIR_E2E_C3_3_PORT 9114
LOGWEIR_E2E_CLUSTER2_PORT 9122
LOGWEIR_E2E_OBJSTORE_PORT 9130
"
STRIDE=10000
MAX_SLOT=4

# THE BROKER LINES: LINE PINNED-VERSION STATUS. The pins are the newest patch
# of each line on Docker Hub's apache/kafka on 2026-09-28; the support matrix
# (docs/support-matrix.md, "Broker versions") records what each one proved.
LINES="
3.7 3.7.1 legacy
3.9 3.9.2 supported
4.1 4.1.2 supported
4.3 4.3.1 supported
"

# THE OPTIONAL PROFILES: NAME DESCRIPTION. `setup` and `tools` are internal
# (one-shot services `just e2e-up` runs itself) and are not listed.
PROFILES_LIST="
auth      kafka-auth: SASL_SSL/PLAIN :9102, SASL_PLAINTEXT/SCRAM-SHA-256 :9103, SSL+client-cert :9104 (certs in .e2e/auth/<project>/)
cluster3  kafka-c3-1..3: a three-node KRaft cluster, RF 3 / min ISR 2 by default, :9112-:9114
cluster2  kafka-cluster2: a second single-node cluster with its own cluster id and the marker topic, :9122
streams   streams-wordcount: Apache Kafka's WordCountDemo on kafka-broker-1 (group logweir-e2e-wordcount)
objectstore  objectstore: SeaweedFS 4.48 S3 :9130 with kafka-backups, logweir-evidence, kafka-backups-locked (Object Lock), kafka-backups-2
"

die() { echo "stack-env: $*" >&2; exit 2; }

slot=0 kafka="" profiles="" mode=env
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) slot=${2:-}; shift 2 ;;
    --kafka) kafka=${2:-}; shift 2 ;;
    --profiles) profiles=${2:-}; shift 2 ;;
    --check) mode=check; shift ;;
    --lines) mode=lines; shift ;;
    --profiles-list) mode=profiles; shift ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
done

case "$mode" in
  check)
    # shellcheck source=e2e/compose/stack-lib.sh
    . "$here/stack-lib.sh"
    lw_e2e_check_coherent || exit 1
    echo "e2e stack: project $LW_E2E_PROJECT, bootstrap $LW_E2E_BOOTSTRAP, S3 $LW_E2E_S3_ENDPOINT (coherent)"
    exit 0 ;;
  lines) printf '%s\n' "$LINES" | sed '/^$/d'; exit 0 ;;
  profiles) printf '%s\n' "$PROFILES_LIST" | sed '/^$/d'; exit 0 ;;
esac

# EVERYTHING IS VALIDATED BEFORE ANYTHING IS PRINTED: `eval "$(...)"` runs
# whatever reached stdout even when this script fails, so a half-printed set
# (a slot's ports without its project, say) must never exist.
case "$slot" in ''|*[!0-9]*) die "--slot takes 0..$MAX_SLOT, not '$slot'" ;; esac
[ "$slot" -le "$MAX_SLOT" ] || die "--slot takes 0..$MAX_SLOT, not $slot (slot 5 would reach macOS's ephemeral ports)"

version=""
if [ -n "$kafka" ]; then
  version=$(printf '%s\n' "$LINES" | awk -v l="$kafka" '$1 == l { print $2 }')
  if [ -z "$version" ]; then
    case "$kafka" in
      [0-9]*.[0-9]*.[0-9]*) version=$kafka ;;
      *) die "--kafka takes a line ($(printf '%s\n' "$LINES" | awk 'NF{printf "%s ", $1}')) or an exact version X.Y.Z, not '$kafka'" ;;
    esac
  fi
fi

if [ -n "$profiles" ]; then
  for p in $(printf '%s' "$profiles" | tr ',' ' '); do
    printf '%s\n' "$PROFILES_LIST" | awk -v p="$p" '$1 == p { f = 1 } END { exit !f }' \
      || die "unknown profile '$p'; --profiles-list names them"
  done
fi

out=""
if [ "$slot" -eq 0 ]; then
  out="unset COMPOSE_PROJECT_NAME"
  while read -r var def; do
    if [ -n "$var" ]; then out="$out
unset $var"; fi
  done <<EOF_PORTS
$PORTS
EOF_PORTS
else
  out="export COMPOSE_PROJECT_NAME=logweir-e2e-s$slot"
  while read -r var def; do
    if [ -n "$var" ]; then out="$out
export $var=$((def + STRIDE * slot))"; fi
  done <<EOF_PORTS
$PORTS
EOF_PORTS
fi
if [ -n "$version" ]; then out="$out
export KAFKA_VERSION=$version"; fi
if [ -n "$profiles" ]; then out="$out
export COMPOSE_PROFILES=$profiles"; else out="$out
unset COMPOSE_PROFILES"; fi
printf '%s\n' "$out"
