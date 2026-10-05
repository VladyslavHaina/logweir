# shellcheck shell=bash
# SOURCED, never executed. THE ONE LIST of the compose stack's variables, and
# the functions every script uses to read which stack it addresses and to
# refuse an environment that is not ONE coherent stack (PROD-01.5).
#
# Nothing else carries its own copy: `e2e/compose/stack-env.sh` sources this
# file, the Rust harness compiles it in (`include_str!` in
# e2e/tests/harness/stack.rs), and `e2e/tests/stack_params.rs` checks the
# compose file's `${VAR:-N}` spellings against it. The guide is e2e/README.md.
# Bash 3.2 (macOS's /bin/bash) must be able to source it: no associative
# arrays, no `${!name}`.

# The DEFAULT stack's project (the compose file's `name:`), and the slot rule:
# slot N (1..LW_E2E_MAX_SLOT) is project `logweir-e2e-sN`, and every port below
# is its default + N*LW_E2E_STRIDE. Nothing else is a stack.
LW_E2E_DEFAULT_PROJECT=logweir-e2e
LW_E2E_STRIDE=10000
LW_E2E_MAX_SLOT=4

# Every published host port of docker-compose.yml: VARIABLE DEFAULT PROFILE
# ('-' for the always-on services). Profile ports publish only while their
# profile is active, but a slot moves them all, always.
LW_E2E_PORT_TABLE="
LOGWEIR_E2E_KAFKA_PORT 9092 -
LOGWEIR_E2E_K8S_PORT 9095 -
LOGWEIR_E2E_SASL_PORT 9097 -
LOGWEIR_E2E_S3_PORT 9000 -
LOGWEIR_E2E_S3_CONSOLE_PORT 9001 -
LOGWEIR_E2E_AUTH_PLAIN_PORT 9102 auth
LOGWEIR_E2E_AUTH_SCRAM256_PORT 9103 auth
LOGWEIR_E2E_AUTH_MTLS_PORT 9104 auth
LOGWEIR_E2E_C3_1_PORT 9112 cluster3
LOGWEIR_E2E_C3_2_PORT 9113 cluster3
LOGWEIR_E2E_C3_3_PORT 9114 cluster3
LOGWEIR_E2E_CLUSTER2_PORT 9122 cluster2
LOGWEIR_E2E_OBJSTORE_PORT 9130 objectstore
LOGWEIR_E2E_REGISTRY_PORT 9141 registry
LOGWEIR_E2E_ACL_PORT 9150 acl
LOGWEIR_E2E_ACL_SASL_PORT 9151 acl
"

# The table's rows, one "VARIABLE DEFAULT PROFILE" per line.
lw_e2e_ports() { printf '%s\n' "$LW_E2E_PORT_TABLE" | awk 'NF == 3'; }

# The default of port variable $1.
lw_e2e_port_default() { lw_e2e_ports | awk -v v="$1" '$1 == v { print $2 }'; }

# The value this shell gives port variable $1: the environment's, else its
# default.
lw_e2e_port() {
  local val
  eval "val=\${$1:-}"
  if [ -n "$val" ]; then printf '%s\n' "$val"; else lw_e2e_port_default "$1"; fi
}

LW_E2E_PROJECT="${COMPOSE_PROJECT_NAME:-$LW_E2E_DEFAULT_PROJECT}"
LW_E2E_KAFKA_PORT=$(lw_e2e_port LOGWEIR_E2E_KAFKA_PORT)
LW_E2E_K8S_PORT=$(lw_e2e_port LOGWEIR_E2E_K8S_PORT)
LW_E2E_SASL_PORT=$(lw_e2e_port LOGWEIR_E2E_SASL_PORT)
LW_E2E_S3_PORT=$(lw_e2e_port LOGWEIR_E2E_S3_PORT)
LW_E2E_S3_CONSOLE_PORT=$(lw_e2e_port LOGWEIR_E2E_S3_CONSOLE_PORT)

# What a HOST-SIDE client dials. The broker advertises exactly these, so a
# client bootstrapped here is never redirected to another stack's port.
LW_E2E_BOOTSTRAP="localhost:${LW_E2E_KAFKA_PORT}"
LW_E2E_S3_ENDPOINT="http://localhost:${LW_E2E_S3_PORT}"

# The slot of project $1: 0 for the default project, N for
# `logweir-e2e-sN` (1..LW_E2E_MAX_SLOT), NOTHING for any other name.
lw_e2e_slot_of() {
  local n
  case "$1" in
    "$LW_E2E_DEFAULT_PROJECT") echo 0 ;;
    "$LW_E2E_DEFAULT_PROJECT"-s[1-9])
      n=${1##*-s}
      if [ "$n" -le "$LW_E2E_MAX_SLOT" ]; then echo "$n"; fi ;;
  esac
}

# True when this shell addresses the default stack.
lw_e2e_is_default() { [ "$LW_E2E_PROJECT" = "$LW_E2E_DEFAULT_PROJECT" ]; }

# A per-stack scratch directory: `$1` itself for the default stack (where it
# has always been), `$1/<project>` for any other, so two stacks driven from one
# checkout never share a key, an approval or a scorecard.
lw_e2e_scratch() {
  if lw_e2e_is_default; then printf '%s\n' "$1"; else printf '%s/%s\n' "$1" "$LW_E2E_PROJECT"; fi
}

# Rewrites the DEFAULT stack's host-side addresses in a checked-in spec
# (stdin) to this stack's (stdout). A no-op on the default stack, byte for byte.
lw_e2e_rebind() {
  sed -e "s|localhost:9092|${LW_E2E_BOOTSTRAP}|g" \
      -e "s|http://localhost:9000|${LW_E2E_S3_ENDPOINT}|g"
}

# Refuses (returns 1, printing every reason) an environment that is not ONE
# coherent stack. Coherent means: COMPOSE_PROJECT_NAME is the default project
# (or unset) or `logweir-e2e-sN`, and EVERY port in the table is exactly that
# slot's (unset only counts as the default, i.e. only on slot 0); no other
# LOGWEIR_E2E_*_PORT name is set. Anything else is refused because compose
# would act on a stack nobody meant: the default project with any moved port
# RECREATES (`up`) or REMOVES (`down -v`) the shared default stack under its
# user, one slot's project with another slot's ports recreates that slot, and
# a typo'd variable is silently ignored.
lw_e2e_check_coherent() {
  local bad="" slot var def prof val want
  slot=$(lw_e2e_slot_of "$LW_E2E_PROJECT")
  if [ -z "$slot" ]; then
    bad="$bad
  COMPOSE_PROJECT_NAME=$LW_E2E_PROJECT is not a stack project ($LW_E2E_DEFAULT_PROJECT is slot 0, $LW_E2E_DEFAULT_PROJECT-s1..s$LW_E2E_MAX_SLOT are slots 1..$LW_E2E_MAX_SLOT)"
  fi
  while read -r var def prof; do
    [ -n "$var" ] || continue
    eval "val=\${$var:-}"
    if [ -z "$val" ]; then
      if [ -n "$slot" ] && [ "$slot" != 0 ]; then
        bad="$bad
  $var is unset, but $LW_E2E_PROJECT is slot $slot, which needs $((def + LW_E2E_STRIDE * slot))"
      fi
      continue
    fi
    case "$val" in
      *[!0-9]*) bad="$bad
  $var=$val is not a TCP port"; continue ;;
    esac
    [ -n "$slot" ] || continue
    want=$((def + LW_E2E_STRIDE * slot))
    if [ "$val" != "$want" ]; then
      bad="$bad
  $var=$val, but $LW_E2E_PROJECT is slot $slot, which needs $want"
    fi
  done <<EOF
$(lw_e2e_ports)
EOF
  for var in $(env | sed -n 's/^\(LOGWEIR_E2E_[A-Za-z0-9_]*_PORT\)=.*/\1/p'); do
    if ! lw_e2e_ports | awk -v v="$var" '$1 == v { f = 1 } END { exit !f }'; then
      bad="$bad
  $var is not a stack variable (e2e/compose/stack-lib.sh lists them)"
    fi
  done
  if [ -n "$bad" ]; then
    printf 'e2e stack: this environment is not one coherent stack:%s\nSet all of them at once: eval "$(e2e/compose/stack-env.sh --slot N)" (N=0 is the default stack).\n' "$bad" >&2
    return 1
  fi
  return 0
}

# Refuses (returns 1) unless this shell addresses the DEFAULT stack, coherently.
# For the scripts that know only the default stack's addresses — the Kubernetes
# demos dial host.docker.internal:9095 and :9000 — which on a slot would take
# the slot's stack and then reach whoever owns the default one.
lw_e2e_require_default() {
  lw_e2e_check_coherent || return 1
  if ! lw_e2e_is_default; then
    printf 'e2e stack: %s addresses the DEFAULT stack only, and this shell addresses %s. Run it with no stack variable set: eval "$(e2e/compose/stack-env.sh --slot 0)".\n' "${1:-this script}" "$LW_E2E_PROJECT" >&2
    return 1
  fi
  return 0
}
