#!/usr/bin/env bash
# THE CONSUMER-GROUP FIXTURE (PROD-04.0d): one group of every type the broker
# line supports, on the stack this shell addresses, with live members that
# stop and start cleanly; and PROD-04.0 §3.9's visibility state on the `acl`
# profile.
#
#   eval "$(e2e/compose/stack-env.sh --slot 1 --kafka 4.3)"
#   just e2e-up
#   e2e/compose/groups.sh up                   # the set below; idempotent
#   e2e/compose/groups.sh stop consumer-live   # waits until its group is Empty
#   e2e/compose/groups.sh start consumer-live  # waits until it is Stable again
#   e2e/compose/groups.sh list                 # GROUP TYPE STATE, as the broker says
#   just e2e-down                              # removes all of it
#
#   up                 create the set on kafka-broker-1 and wait until every
#                      group is in its state; a member already running is left
#   stop MEMBER...     stop live members, each waited on until its group is
#                      Empty and its process is gone
#   start MEMBER...    start them again, each waited on until Stable
#   down               stop every live member (the groups and commits stay)
#   list               every fixture group of this line, with the type and
#                      state the broker reports ('-' where its tools cannot
#                      report a type: 3.7.1)
#   visibility apply   §3.9's setup on the `acl` profile's kafka-acl (below)
#   visibility remove  undo it
#
# MEMBERS: classic-live, consumer-live, share-live, streams.
#
# THE SET follows what kafka-broker-1's FINALIZED features allow
# (`kafka-features.sh describe`): classic groups always; consumer groups
# (KIP-848) with group.version >= 1; share groups (KIP-932) with
# share.version >= 1; streams groups (KIP-1071) with streams.version >= 1.
# On 4.3.1 that is all four (PROD-04.0 §3); on 3.9.2, classic only (§3.6).
#
#   pa-classic-empty   classic   Empty   a consumer read 12 records and left
#   pa-classic-live    classic   Stable  member classic-live
#   pa-consumer-empty  consumer  Empty   as pa-classic-empty, group.protocol=consumer
#   pa-consumer-live   consumer  Stable  member consumer-live
#   pa-share-idle      share     Empty   a share consumer read 10 records and left
#   pa-share-live      share     Stable  member share-live
#   logweir-e2e-streams-protocol  streams  Stable  member streams: the
#                      `streams-protocol` profile's application, which `up`
#                      starts (docker compose) on a line with streams groups
#
# Topics on kafka-broker-1: pa-orders (3 partitions, 30 keyed records) and
# pa-share-in (2 partitions, 40 keyed records); the streams application reads
# streams-plaintext-input (3 lines are produced when it is empty). Both share
# groups carry the group config share.auto.offset.reset=earliest, set before
# their first member joins, so they start at the beginning of pa-share-in;
# their share-partition state is readable because every single-node broker
# sizes `__share_group_state` for one broker (docker-compose.yml).
#
# HOW MEMBERS RUN AND STOP. The three live consumers run INSIDE kafka-broker-1
# (`docker compose exec -d`), each bounded by `timeout` (GROUPS_MEMBER_SECONDS,
# default 7200) and gone with the container at `just e2e-down`. `stop` sends
# SIGTERM: the console consumer's shutdown hook closes it, so it LEAVES its
# group, which is Empty at the first check after the process exits (`stop`
# says so) rather than after a session timeout. Every
# pkill/pgrep pattern is BRACKETED, `[g]roup NAME$`: PROD-04.0's first control
# step ran `pkill -f "group pa-classic-live"` inside `sh -c`, which matched
# its own shell and killed it before the other two pkills ran (§3.3). The
# bracket matches the member's `--group NAME` and never the command line that
# carries the pattern; `$` keeps one group's pattern off a longer name. The
# streams member is a compose service, stopped and started with compose.
#
# VISIBILITY (PROD-04.0 §3.9), on the `acl` profile's kafka-acl, as its super
# user. OPT-IN, and not part of the profile's setup: its cluster ACL denies
# the restricted principal EVERY cluster operation (allow.everyone.if.no.acl.found
# opens only resources with no ACL at all), and the profile's other rows
# (FX-4's config_coverage) expect it to have them until they deny one.
#   apply   topic pa-orders (3 partitions, 60 keyed records); groups pa-visible
#           and pa-hidden, each created by a NON-MEMBER commit (offsets 5 and
#           7 on pa-orders 0); ACLs naming only User:ops: Describe and Read on
#           group pa-hidden, Describe on the cluster. User:logweir (SCRAM-
#           SHA-512, the profile's restricted principal) then has neither
#           Describe on the cluster nor on pa-hidden: its group listing omits
#           pa-hidden with no error (T14), and a targeted describe of
#           pa-hidden is refused GROUP_AUTHORIZATION_FAILED.
#   remove  deletes those ACLs, both groups and the topic.
#
# Every container call is bounded (`timeout`, `gtimeout` or this host's
# /tmp/lwtimeout) and checks first that the environment is one coherent
# stack. The guide is e2e/README.md, "The groups fixture".
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
# shellcheck source=e2e/compose/stack-lib.sh
. e2e/compose/stack-lib.sh
lw_e2e_check_coherent || exit 1

if command -v timeout >/dev/null 2>&1; then BOUND=timeout
elif command -v gtimeout >/dev/null 2>&1; then BOUND=gtimeout
elif [ -x /tmp/lwtimeout ]; then BOUND=/tmp/lwtimeout
else
  BOUND=""
  echo "groups: WARNING: no timeout, gtimeout or /tmp/lwtimeout; the calls run UNBOUNDED" >&2
fi
bounded() { # seconds command...
  local s=$1; shift
  if [ -n "$BOUND" ]; then "$BOUND" "$s" "$@"; else "$@"; fi
}

COMPOSE_FILE_PATH=e2e/compose/docker-compose.yml
T=/opt/kafka/bin
B=kafka-broker-1:9094
A=kafka-acl:9094
MEMBER_SECONDS=${GROUPS_MEMBER_SECONDS:-7200}
STREAMS_GROUP=logweir-e2e-streams-protocol

say() { echo "groups: $*" >&2; }
die() { echo "groups: $*" >&2; exit 1; }

# A bash command inside kafka-broker-1 (kexec) or kafka-acl (aexec), bounded.
# stdin is /dev/null: `docker compose exec` attaches stdin even with -T, and
# inside a `while read … done <<EOF` loop it would swallow the loop's input
# (measured: the first exec ate the rest of the group list).
kexec() { bounded 120 docker compose -f "$COMPOSE_FILE_PATH" exec -T kafka-broker-1 bash -c "$1" </dev/null; }
aexec() { bounded 120 docker compose -f "$COMPOSE_FILE_PATH" --profile acl exec -T kafka-acl bash -c "$1" </dev/null; }
# docker compose on the streams-protocol profile's application.
streams_compose() { bounded "$1" docker compose -f "$COMPOSE_FILE_PATH" --profile streams-protocol "${@:2}" </dev/null; }

# ---------------------------------------------------------------- the line
FEATURES=""
HAS_CONSUMER=0 HAS_SHARE=0 HAS_STREAMS=0
feature_level() { # feature -> its finalized level, 0 when the broker has none
  local v
  v=$(printf '%s\n' "$FEATURES" | awk -v f="$1" '$1 == "Feature:" && $2 == f {
        for (i = 3; i < NF; i++) if ($i == "FinalizedVersionLevel:") print $(i + 1) }' | head -1)
  case "$v" in ''|*[!0-9]*) echo 0 ;; *) echo "$v" ;; esac
}
load_types() {
  FEATURES=$(kexec "$T/kafka-features.sh --bootstrap-server $B describe" 2>/dev/null) \
    || die "kafka-features.sh describe failed on kafka-broker-1: is the stack up (just e2e-up)?"
  [ "$(feature_level group.version)" -ge 1 ] && HAS_CONSUMER=1
  [ "$(feature_level share.version)" -ge 1 ] && HAS_SHARE=1
  [ "$(feature_level streams.version)" -ge 1 ] && HAS_STREAMS=1
  return 0
}
has_kind() {
  case "$1" in
    classic) return 0 ;;
    consumer) [ "$HAS_CONSUMER" = 1 ] ;;
    share) [ "$HAS_SHARE" = 1 ] ;;
    streams) [ "$HAS_STREAMS" = 1 ] ;;
    *) return 1 ;;
  esac
}

# THE SET: GROUP KIND STATE MEMBER ('-' for none).
SET="
pa-classic-empty classic Empty -
pa-classic-live classic Stable classic-live
pa-consumer-empty consumer Empty -
pa-consumer-live consumer Stable consumer-live
pa-share-idle share Empty -
pa-share-live share Stable share-live
$STREAMS_GROUP streams Stable streams
"
set_rows() { printf '%s\n' "$SET" | awk 'NF == 4'; }
member_row() { set_rows | awk -v m="$1" '$4 == m'; }

# The state the broker reports for GROUP of KIND, '' when it has none. Every
# describe tool prints `GROUP COORDINATOR (ID) [STRATEGY] STATE #MEMBERS`.
state_of() {
  local tool
  case "$2" in
    classic|consumer) tool=kafka-consumer-groups.sh ;;
    share) tool=kafka-share-groups.sh ;;
    streams) tool=kafka-streams-groups.sh ;;
  esac
  kexec "$T/$tool --bootstrap-server $B --describe --group $1 --state 2>/dev/null" 2>/dev/null \
    | awk -v g="$1" '$1 == g && NF >= 4 { s = $(NF - 1) } END { print s }'
}
wait_state() { # group kind want seconds
  local s=""
  for _ in $(seq 1 $(($4 / 3 + 1))); do
    s=$(state_of "$1" "$2")
    [ "$s" = "$3" ] && return 0
    sleep 3
  done
  say "$1 ($2) is '${s:-absent}' after $4 s, want $3"
  return 1
}

# ---------------------------------------------------------------- members
# The bracketed pattern of GROUP's member (see the header).
pattern() { printf '[g]roup %s$' "$1"; }
running() { kexec "pgrep -f '$(pattern "$1")' >/dev/null" >/dev/null 2>&1; }
member_cmd() { # group kind [extra options]
  case "$2" in
    classic|consumer)
      printf '%s/kafka-console-consumer.sh --bootstrap-server %s --topic pa-orders --from-beginning %s --consumer-property group.protocol=%s --group %s' \
        "$T" "$B" "${3:-}" "$2" "$1" ;;
    share)
      printf '%s/kafka-console-share-consumer.sh --bootstrap-server %s --topic pa-share-in %s --group %s' \
        "$T" "$B" "${3:-}" "$1" ;;
  esac
}

start_member() {
  local row g kind
  row=$(member_row "$1")
  [ -n "$row" ] || die "no member '$1' (classic-live consumer-live share-live streams)"
  set -- $row
  g=$1 kind=$2
  has_kind "$kind" || die "this broker line has no $kind groups (kafka-features.sh describe)"
  if [ "$kind" = streams ]; then
    say "start streams: the streams-protocol profile's streams-protocol-wordcount"
    streams_compose 300 up -d --wait streams-protocol-wordcount >&2 \
      || die "streams-protocol-wordcount did not come up healthy"
  elif running "$g"; then
    say "$g: a member is already running"
  else
    say "start $g ($kind)"
    bounded 60 docker compose -f "$COMPOSE_FILE_PATH" exec -d -T kafka-broker-1 bash -c \
      "mkdir -p /tmp/lw-groups; exec timeout $MEMBER_SECONDS $(member_cmd "$g" "$kind") > /tmp/lw-groups/$g.log 2>&1" </dev/null \
      || die "could not start $g's member"
  fi
  wait_state "$g" "$kind" Stable 120 || die "$g did not become Stable"
}

stop_member() {
  local row g kind
  row=$(member_row "$1")
  [ -n "$row" ] || die "no member '$1' (classic-live consumer-live share-live streams)"
  set -- $row
  g=$1 kind=$2
  has_kind "$kind" || die "this broker line has no $kind groups (kafka-features.sh describe)"
  if [ "$kind" = streams ]; then
    say "stop streams: streams-protocol-wordcount"
    streams_compose 120 stop streams-protocol-wordcount >&2 || die "could not stop streams-protocol-wordcount"
  else
    say "stop $g (SIGTERM to '$(pattern "$g")')"
    kexec "pkill -TERM -f '$(pattern "$g")'" >/dev/null 2>&1 || say "$g: no running member to stop"
    for _ in $(seq 1 20); do running "$g" || break; sleep 2; done
    running "$g" && die "$g's member is still running 40 s after SIGTERM"
  fi
  # Did the member LEAVE? Then the group is Empty at the first look after its
  # process is gone. A member that died without leaving (SIGKILL) holds the
  # group Stable until session.timeout.ms (45 s by default), far longer than
  # one look takes, so this line tells the two apart whatever the host's load
  # (`profile-smoke.sh groups` requires it).
  local first
  first=$(state_of "$g" "$kind")
  if [ "$first" = Empty ]; then
    say "$g: Empty at the first check after its member exited (it left the group)"
  else
    say "$g: still '${first:-absent}' after its member exited (it did not leave); waiting for Empty"
    wait_state "$g" "$kind" Empty 90 || die "$g did not become Empty"
  fi
}

# ---------------------------------------------------------------- setup
end_offsets_sum() { # exec-function bootstrap topic [partition]
  "$1" "$T/kafka-get-offsets.sh --bootstrap-server $2 --topic $3" 2>/dev/null \
    | awk -F: -v t="$3" -v p="${4:-}" '$1 == t && (p == "" || $2 == p) { s += $3 } END { print s + 0 }'
}
produce_keyed() { # exec-function bootstrap topic first last
  "$1" "seq $4 $5 | awk '{print \"k\" \$1 \":v\" \$1}' | $T/kafka-console-producer.sh --bootstrap-server $2 --topic $3 --property parse.key=true --property key.separator=:" >/dev/null 2>&1
}
create_topic() { # exec-function bootstrap topic partitions
  "$1" "$T/kafka-topics.sh --bootstrap-server $2 --create --if-not-exists --topic $3 --partitions $4 --replication-factor 1" >/dev/null 2>&1 \
    || "$1" "$T/kafka-topics.sh --bootstrap-server $2 --describe --topic $3" >/dev/null 2>&1 \
    || die "could not create topic $3 on $2"
}

# A group that a bounded consumer creates by reading `max` records and leaving.
leave_after() { # group kind max
  local s
  s=$(state_of "$1" "$2")
  if [ -n "$s" ]; then say "$1 exists ($s)"; return 0; fi
  say "create $1 ($2): read $3 records and leave"
  kexec "timeout 90 $(member_cmd "$1" "$2" "--max-messages $3") >/dev/null 2>&1" \
    || die "the bounded $2 consumer for $1 failed"
}

cmd_up() {
  load_types
  say "kafka-broker-1: classic yes, consumer $([ $HAS_CONSUMER = 1 ] && echo yes || echo no), share $([ $HAS_SHARE = 1 ] && echo yes || echo no), streams $([ $HAS_STREAMS = 1 ] && echo yes || echo no)"
  create_topic kexec "$B" pa-orders 3
  [ "$(end_offsets_sum kexec "$B" pa-orders)" -gt 0 ] || produce_keyed kexec "$B" pa-orders 1 30
  [ "$(end_offsets_sum kexec "$B" pa-orders)" -ge 30 ] || die "pa-orders holds fewer than 30 records"
  leave_after pa-classic-empty classic 12
  start_member classic-live
  if has_kind consumer; then
    leave_after pa-consumer-empty consumer 12
    start_member consumer-live
  fi
  if has_kind share; then
    create_topic kexec "$B" pa-share-in 2
    [ "$(end_offsets_sum kexec "$B" pa-share-in)" -gt 0 ] || produce_keyed kexec "$B" pa-share-in 1 40
    for g in pa-share-idle pa-share-live; do
      kexec "$T/kafka-configs.sh --bootstrap-server $B --alter --entity-type groups --entity-name $g --add-config share.auto.offset.reset=earliest" >/dev/null 2>&1 \
        || die "could not set share.auto.offset.reset=earliest on $g"
    done
    leave_after pa-share-idle share 10
    start_member share-live
  fi
  if has_kind streams; then
    start_member streams
    [ "$(end_offsets_sum kexec "$B" streams-plaintext-input)" -gt 0 ] \
      || kexec "printf 'a b c\nb c d\nc d e\n' | $T/kafka-console-producer.sh --bootstrap-server $B --topic streams-plaintext-input" >/dev/null 2>&1
  fi
  local rc=0 g kind want m
  while read -r g kind want m; do
    has_kind "$kind" || continue
    wait_state "$g" "$kind" "$want" 120 || rc=1
  done <<EOF
$(set_rows)
EOF
  cmd_list
  [ "$rc" = 0 ] || die "not every group reached its state"
}

cmd_list() {
  [ -n "$FEATURES" ] || load_types
  local types="" g kind want m t
  # The TYPE is always the broker's answer (ListGroups v5), never this
  # script's: kafka-groups.sh on 4.x (GROUP TYPE PROTOCOL), else
  # kafka-consumer-groups.sh --list --type (GROUP TYPE; 3.9.2 has it). A line
  # whose tools cannot say (3.7.1 has no --type) prints '-'.
  if kexec "test -x $T/kafka-groups.sh" >/dev/null 2>&1; then
    types=$(kexec "$T/kafka-groups.sh --bootstrap-server $B --list" 2>/dev/null)
  else
    types=$(kexec "$T/kafka-consumer-groups.sh --bootstrap-server $B --list --type 2>/dev/null" 2>/dev/null)
  fi
  printf '%-32s %-9s %s\n' GROUP TYPE STATE
  while read -r g kind want m; do
    has_kind "$kind" || continue
    t=$(printf '%s\n' "$types" | awk -v g="$g" '$1 == g { print $2 }')
    printf '%-32s %-9s %s\n' "$g" "${t:--}" "$(state_of "$g" "$kind")"
  done <<EOF
$(set_rows)
EOF
}

cmd_down() {
  load_types
  local g kind want m
  while read -r g kind want m; do
    [ "$m" = - ] && continue
    has_kind "$kind" || continue
    stop_member "$m"
  done <<EOF
$(set_rows)
EOF
}

# ---------------------------------------------------------------- visibility
# True when `kafka-consumer-groups.sh --reset-offsets --execute` output $1
# reports GROUP TOPIC PARTITION NEW-OFFSET = $2 $3 $4 $5. Read as one stream of
# words: the 3.9.2 tool prints its row on the HEADER's line, with no newline
# between them (measured), so a per-line match never finds it there.
reset_row() {
  printf ' %s ' "$1" | tr -s '[:space:]' ' ' | grep -q -F " $2 $3 $4 $5 "
}
VIS_ACLS="--allow-principal User:ops --operation Describe --operation Read --group pa-hidden
--allow-principal User:ops --operation Describe --cluster"

cmd_visibility_apply() {
  aexec true >/dev/null 2>&1 || die "kafka-acl is not running: bring the stack up with --profiles acl"
  create_topic aexec "$A" pa-orders 3
  [ "$(end_offsets_sum aexec "$A" pa-orders 0)" -ge 7 ] || produce_keyed aexec "$A" pa-orders 1 60
  [ "$(end_offsets_sum aexec "$A" pa-orders 0)" -ge 7 ] || die "pa-orders partition 0 on kafka-acl holds fewer than 7 records"
  local gv g v out
  for gv in pa-visible:5 pa-hidden:7; do
    g=${gv%%:*} v=${gv##*:}
    # A non-member commit (AdminClient alterConsumerGroupOffsets): the id did
    # not exist, so the broker creates a simple classic group. A fresh broker's
    # coordinator may not be ready yet (§3.9's first run met NotCoordinator):
    # retry, bounded.
    for _ in $(seq 1 24); do
      out=$(aexec "$T/kafka-consumer-groups.sh --bootstrap-server $A --reset-offsets --group $g --topic pa-orders:0 --to-offset $v --execute" 2>&1)
      reset_row "$out" "$g" pa-orders 0 "$v" && break
      sleep 5
    done
    reset_row "$out" "$g" pa-orders 0 "$v" \
      || die "could not create $g by a non-member commit: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
    say "$g: pa-orders 0 committed at $v"
  done
  while read -r acl; do
    [ -n "$acl" ] || continue
    aexec "$T/kafka-acls.sh --bootstrap-server $A --add $acl" >/dev/null 2>&1 || die "kafka-acls --add $acl failed"
  done <<EOF
$VIS_ACLS
EOF
  say "applied: User:logweir now has no Describe on the cluster or on pa-hidden"
  aexec "$T/kafka-acls.sh --bootstrap-server $A --list" 2>/dev/null
}

cmd_visibility_remove() {
  aexec true >/dev/null 2>&1 || die "kafka-acl is not running"
  local acl rc=0
  while read -r acl; do
    [ -n "$acl" ] || continue
    aexec "$T/kafka-acls.sh --bootstrap-server $A --remove --force $acl" >/dev/null 2>&1 || rc=1
  done <<EOF
$VIS_ACLS
EOF
  aexec "$T/kafka-consumer-groups.sh --bootstrap-server $A --delete --group pa-visible --group pa-hidden" >/dev/null 2>&1
  aexec "$T/kafka-topics.sh --bootstrap-server $A --delete --if-exists --topic pa-orders" >/dev/null 2>&1 || rc=1
  [ "$rc" = 0 ] || die "could not remove every part of the visibility setup"
  say "removed: the visibility ACLs, pa-visible, pa-hidden and pa-orders"
}

case "${1:-}" in
  up) cmd_up ;;
  list) cmd_list ;;
  down) cmd_down ;;
  stop|start)
    op=$1; shift
    [ $# -gt 0 ] || die "$op needs a member: classic-live consumer-live share-live streams"
    load_types
    for m in "$@"; do "${op}_member" "$m"; done ;;
  visibility)
    case "${2:-}" in
      apply) cmd_visibility_apply ;;
      remove) cmd_visibility_remove ;;
      *) die "visibility takes apply or remove" ;;
    esac ;;
  -h|--help|"") sed -n '2,24p' "$0"; [ -n "${1:-}" ] ;;
  *) die "unknown subcommand '$1' (see --help)" ;;
esac
