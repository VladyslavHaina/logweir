# shellcheck shell=bash
# SOURCED, never executed: the one place a script reads WHICH compose stack it
# addresses (PROD-01.5). Every value defaults to the stack's historical fixed
# value, so a script that sources this with no variable set behaves exactly as
# it did before the stack was parameterized.
#
#   variable                      default       what it moves
#   COMPOSE_PROJECT_NAME          logweir-e2e   compose's own override of `name:`
#   LOGWEIR_E2E_KAFKA_PORT        9092          EXTERNAL host port + advertisement
#   LOGWEIR_E2E_K8S_PORT          9095          K8S host port + advertisement
#   LOGWEIR_E2E_SASL_PORT         9097          SASLEXT host port + advertisement
#   LOGWEIR_E2E_S3_PORT           9000          MinIO S3 API host port
#   LOGWEIR_E2E_S3_CONSOLE_PORT   9001          MinIO console host port
#
# `e2e/compose/stack-env.sh --slot N` prints one coherent set; the guide is
# e2e/README.md. `e2e/tests/stack_params.rs` compares every default below with
# the compose file's `${VAR:-N}` and with `e2e/tests/harness/stack.rs`, so the
# three cannot drift apart silently. Keep one `LW_E2E_X="${VAR:-N}"`
# assignment per line, and one "VAR:$LW_E2E_X:N" tuple per port in
# lw_e2e_check_coherent: that test parses both shapes.

LW_E2E_DEFAULT_PROJECT=logweir-e2e
LW_E2E_PROJECT="${COMPOSE_PROJECT_NAME:-logweir-e2e}"
LW_E2E_KAFKA_PORT="${LOGWEIR_E2E_KAFKA_PORT:-9092}"
LW_E2E_K8S_PORT="${LOGWEIR_E2E_K8S_PORT:-9095}"
LW_E2E_SASL_PORT="${LOGWEIR_E2E_SASL_PORT:-9097}"
LW_E2E_S3_PORT="${LOGWEIR_E2E_S3_PORT:-9000}"
LW_E2E_S3_CONSOLE_PORT="${LOGWEIR_E2E_S3_CONSOLE_PORT:-9001}"

# What a HOST-SIDE client dials. The broker advertises exactly these, so a
# client bootstrapped here is never redirected to another stack's port.
LW_E2E_BOOTSTRAP="localhost:${LW_E2E_KAFKA_PORT}"
LW_E2E_S3_ENDPOINT="http://localhost:${LW_E2E_S3_PORT}"

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

# Refuses (returns 1, printing why) an environment whose project and ports
# disagree. The dangerous half is the DEFAULT project with a moved port:
# `docker compose up` would RECREATE the default stack's containers under
# whoever is using them, and `down` would remove them. The other half (a new
# project on a default port) would collide with the default stack's ports.
lw_e2e_check_coherent() {
  local bad="" pair var val def
  for pair in \
    "LOGWEIR_E2E_KAFKA_PORT:$LW_E2E_KAFKA_PORT:9092" \
    "LOGWEIR_E2E_K8S_PORT:$LW_E2E_K8S_PORT:9095" \
    "LOGWEIR_E2E_SASL_PORT:$LW_E2E_SASL_PORT:9097" \
    "LOGWEIR_E2E_S3_PORT:$LW_E2E_S3_PORT:9000" \
    "LOGWEIR_E2E_S3_CONSOLE_PORT:$LW_E2E_S3_CONSOLE_PORT:9001"; do
    var=${pair%%:*}; val=${pair#*:}; def=${val#*:}; val=${val%%:*}
    case "$val" in ''|*[!0-9]*) bad="$bad
  $var=$val is not a TCP port"; continue ;; esac
    if lw_e2e_is_default && [ "$val" != "$def" ]; then
      bad="$bad
  $var=$val but COMPOSE_PROJECT_NAME is the default ($LW_E2E_DEFAULT_PROJECT)"
    fi
    if ! lw_e2e_is_default && [ "$val" = "$def" ]; then
      bad="$bad
  $var=$val is the DEFAULT stack's port but COMPOSE_PROJECT_NAME=$LW_E2E_PROJECT"
    fi
  done
  if [ -n "$bad" ]; then
    printf 'e2e stack: the project and its ports disagree:%s\nSet all of them at once: eval "$(e2e/compose/stack-env.sh --slot N)" (N=0 is the default stack).\n' "$bad" >&2
    return 1
  fi
  return 0
}
