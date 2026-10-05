#!/usr/bin/env bash
# Print the environment for ONE compose stack (PROD-01.5), for `eval`:
#
#   eval "$(e2e/compose/stack-env.sh --slot 2 --kafka 4.1 --profiles auth)"
#   just e2e-up && ./scripts/e2e-seed.sh && just e2e && just e2e-down
#
#   --slot N       0 = the default stack (project logweir-e2e, ports 9092,
#                  9095, 9097, 9000, 9001 — every stack variable is UNSET, so
#                  the compose file's own defaults apply). 1..4 = project
#                  logweir-e2e-sN with EVERY host port moved by N*10000.
#   --kafka LINE   a broker line (see --lines): KAFKA_VERSION and KAFKA_IMAGE
#                  (the line's image pinned by digest). An exact version X.Y.Z
#                  exports KAFKA_VERSION only (the image is then by tag).
#                  Omitted: BOTH are UNSET, so e2e/compose/.env's pin applies —
#                  a line chosen for one slot never carries over to the next.
#   --profiles P   comma-separated optional profiles (see --profiles-list),
#                  exported as COMPOSE_PROFILES. Omitted: UNSET.
#   --check        exit 1, saying why, unless the CURRENT environment is one
#                  coherent stack (`just e2e-up` / `e2e-down` run this).
#   --lines        print the broker lines, their pins and status.
#   --profiles-list  print the optional profiles and what each provides.
#
# The variables and their defaults are e2e/compose/stack-lib.sh's ONE list;
# this script only prints them. Every variable it can export it also unsets
# when not asked for it, so switching slots never inherits the last one.
# Slots never share a project, a host port, a cluster id, a volume or a
# network, so two slots run at once with no lock. Slot 0 is the stack other
# people use: hold `claude/compose-lock.sh` for it as before. The guide is
# e2e/README.md.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=e2e/compose/stack-lib.sh
. "$here/stack-lib.sh"

# THE BROKER LINES: LINE PINNED-VERSION IMAGE-DIGEST STATUS. The newest patch
# of each line on Docker Hub's apache/kafka on 2026-09-28, pinned by the index
# digest measured then; docs/support-matrix.md, "Broker versions", records
# what each one proved (and stack_params.rs checks the two agree).
LINES="
3.7 3.7.1 sha256:ed74d7d115968d5e8b00ba6822ac6a384cbaaf54ca38991828647000d7089b68 legacy
3.9 3.9.2 sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02 supported
4.1 4.1.2 sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e supported
4.3 4.3.1 sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837 supported
"

# THE OPTIONAL PROFILES: NAME DESCRIPTION. `setup` and `tools` are internal
# (one-shot services `just e2e-up` runs itself) and are not listed.
PROFILES_LIST="
auth      kafka-auth: SASL_SSL/PLAIN :9102, SASL_PLAINTEXT/SCRAM-SHA-256 :9103, SSL+client-cert :9104 (certs in .e2e/auth/<project>/)
cluster3  kafka-c3-1..3: a three-node KRaft cluster, RF 3 / min ISR 2 by default, :9112-:9114
cluster2  kafka-cluster2: a second single-node cluster with its own cluster id and the marker topic, :9122
streams   streams-wordcount: Apache Kafka's WordCountDemo on kafka-broker-1 (group logweir-e2e-wordcount)
objectstore  objectstore: SeaweedFS 4.48 S3 :9130 with kafka-backups, logweir-evidence, kafka-backups-locked (Object Lock), kafka-backups-2
registry  registry: Karapace 6.2.3 (Schema-Registry-compatible) :9141, schemas in _schemas on kafka-broker-1
acl       kafka-acl: StandardAuthorizer; PLAINTEXT :9150 as ANONYMOUS (super user), SASL_PLAINTEXT/SCRAM-SHA-512 :9151 as logweir (restricted by the row's ACLs)
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
    -h|--help) sed -n '2,29p' "$0"; exit 0 ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
done

case "$mode" in
  check)
    lw_e2e_check_coherent || exit 1
    echo "e2e stack: project $LW_E2E_PROJECT, bootstrap $LW_E2E_BOOTSTRAP, S3 $LW_E2E_S3_ENDPOINT (coherent)"
    exit 0 ;;
  lines) printf '%s\n' "$LINES" | sed '/^$/d'; exit 0 ;;
  profiles) printf '%s\n' "$PROFILES_LIST" | sed '/^$/d'; exit 0 ;;
esac

# EVERYTHING IS VALIDATED BEFORE ANYTHING IS PRINTED: `eval "$(...)"` runs
# whatever reached stdout even when this script fails, so a half-printed set
# (a slot's ports without its project, say) must never exist.
case "$slot" in ''|*[!0-9]*) die "--slot takes 0..$LW_E2E_MAX_SLOT, not '$slot'" ;; esac
[ "$slot" -le "$LW_E2E_MAX_SLOT" ] || die "--slot takes 0..$LW_E2E_MAX_SLOT, not $slot (slot 5 would reach macOS's ephemeral ports)"

version="" image=""
if [ -n "$kafka" ]; then
  version=$(printf '%s\n' "$LINES" | awk -v l="$kafka" '$1 == l { print $2 }')
  if [ -n "$version" ]; then
    image="apache/kafka:$version@$(printf '%s\n' "$LINES" | awk -v l="$kafka" '$1 == l { print $3 }')"
  else
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
  while read -r var def prof; do
    if [ -n "$var" ]; then out="$out
unset $var"; fi
  done <<EOF_PORTS
$(lw_e2e_ports)
EOF_PORTS
else
  out="export COMPOSE_PROJECT_NAME=$LW_E2E_DEFAULT_PROJECT-s$slot"
  while read -r var def prof; do
    if [ -n "$var" ]; then out="$out
export $var=$((def + LW_E2E_STRIDE * slot))"; fi
  done <<EOF_PORTS
$(lw_e2e_ports)
EOF_PORTS
fi
if [ -n "$version" ]; then out="$out
export KAFKA_VERSION=$version"; else out="$out
unset KAFKA_VERSION"; fi
if [ -n "$image" ]; then out="$out
export KAFKA_IMAGE=$image"; else out="$out
unset KAFKA_IMAGE"; fi
if [ -n "$profiles" ]; then out="$out
export COMPOSE_PROFILES=$profiles"; else out="$out
unset COMPOSE_PROFILES"; fi
printf '%s\n' "$out"
