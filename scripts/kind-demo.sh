#!/usr/bin/env bash
# **Demo 1, in CI, on the `kind` cluster the workflow created.** Task 31.
#
# Spec §16 clause 2: *Demo 1 (§2) runs end-to-end in CI on a `kind` cluster
# created by the workflow, with the compose broker reached over the published
# `K8S` listener.* This is the driver for that run. It sets the cluster, sources
# `scripts/demo-steps.sh` — the same twelve steps `scripts/laptop-demo.sh`
# runs, defined once — performs three pre-steps of its own, and then runs them.
#
# ===========================================================================
# STANDING RULE 16: `kind` IS A CI-ONLY CLUSTER
# ===========================================================================
# This script is run by `.github/workflows/kind-demo.yml` on a GitHub-hosted
# runner (Global Constraint 17 — `kind` there costs nothing), with ONE
# exception: a single local proving run explicitly authorised by the controller
# at dispatch, which deletes its cluster in the same session and records the
# transcript. It creates no cluster of its own in either case: the cluster is
# the workflow's, and `kind delete cluster` is the workflow's too.
#
# It never touches the laptop cluster, and the context this file names appears
# nowhere else in it: `LOGWEIR_KUBE_CONTEXT` is `kind-logweir`, every `kubectl`
# line here and in `demo-steps.sh` reads `kubectl --context
# "$LOGWEIR_KUBE_CONTEXT" …`, and
# `crates/logweir/tests/laptop_demo_lint.rs::laptop_demo_names_every_kubectl_context`
# asserts the OTHER demo's context is not spelt anywhere in this file — prose
# included, because a comment naming it is one copy-paste away from a command
# naming it.
#
# ===========================================================================
# THE PRE-STEPS: RESOLVE THE NAME, NOT THE ADDRESS
# ===========================================================================
# The demo's broker is the compose stack on the RUNNER'S HOST, and the runner
# Job is a POD. Task 7 published the listener a pod is supposed to use —
# `K8S://host.docker.internal:9095` (STANDING RULE 15) — and STANDING RULE 15
# also makes Task 7 the only task that may edit `docker-compose.yml`, so this
# script may not re-advertise it.
#
# A Kafka client does not keep talking to the address it bootstrapped at: the
# broker's metadata response REDIRECTS it to the advertised listener. So
# computing the kind gateway address and passing it as `--bootstrap` fixes the
# first packet and nothing after it — the advertised NAME still has to resolve
# inside the pod. `extraPortMappings` is not the mechanism either: it maps host
# to node, inbound, and this is the other direction.
#
# What works, and needs no compose change and no change to the Job's PodSpec:
# add a CoreDNS `hosts` block mapping `host.docker.internal` to the kind
# network's gateway, inside the cluster's own DNS. Then the advertised listener
# resolves in every pod and **the bootstrap string stays the literal
# `host.docker.internal:9095` in both demos** — byte-identical to spec §2's
# Demo 1. What was three substitutions becomes two.
#
# So, before step 1:
#   1. the kind network's gateway, PRINTED and asserted non-empty. There is no
#      fallback to `localhost`: a step whose address resolution failed must fail
#      loudly, because a silent fallback would dial the POD's own loopback and
#      time out eleven steps later with a message about a `Backup`.
#   2. the CoreDNS patch, as four operations whose exit codes are read directly
#      and none of which is piped (STANDING RULE 20). The ConfigMap render and
#      its apply are TWO lines, never `… --dry-run=client -o yaml | kubectl
#      apply -f -`: that pipe reports `kubectl apply`'s status and swallows the
#      render's.
#   3. the reachability probe (interface register I14, Task 15c), from INSIDE
#      the cluster, before any demo step runs. `logweir cluster-probe` prints
#      `cluster-id=<id>` and `reachable=true|false` and exits 0 or 1; the
#      image's `ENTRYPOINT` is `/usr/local/bin/logweir`, so the words after
#      `--` are the CLI's own arguments, and `--attach --restart=Never` makes
#      `kubectl`'s exit code the container's. `logweir doctor` is NOT used:
#      `cli.rs` makes `--allowed-clusters` and `--approver-key` mandatory there
#      and `doctor.rs` hard-codes `Plaintext`.
#
# ===========================================================================
# STEP 10 IS THE ONLY STEP WHOSE MEANING DIFFERS HERE
# ===========================================================================
# There is no browser on a runner, so X-UIWRITE's in-browser half cannot run.
# The workflow performs the same-origin `POST` through `kubectl proxy` with
# `curl` and asserts 201 — the MECHANICAL half — and its step name says so and
# cites `e2e/k8s/laptop-demo.md`, where the half spec §10 actually requires
# ("a `create` of a `Restore` from the page") is recorded. `curl` is not the
# page, and this file does not pretend otherwise.
#
# ===========================================================================
# WHAT A GREEN RUN OF THIS PROVES, AND WHAT IT DOES NOT
# ===========================================================================
# Global Constraint 37: a locally built or locally loaded image is AUTHOR-ONLY
# and never satisfies spec §16 clause 1, the `registry:2` fallback included.
# The workflow has two install branches and their step names say which ran. On
# the author-only branch a green run proves the code path — the manifests, the
# controller, the runner, the DNS mechanism, the whole walk — and proves
# nothing about publication.
set -euo pipefail
cd "$(dirname "$0")/.."

export LOGWEIR_KUBE_CONTEXT=kind-logweir
echo "kind-demo: kubectl context $LOGWEIR_KUBE_CONTEXT (STANDING RULE 12)"

# `demo-steps.sh` is SOURCED and runs nothing: it defines the twelve steps, the
# helpers and the three parameters. `demo_run` at the bottom of this file is
# what runs them.
. scripts/demo-steps.sh

KIND_NETWORK=kind
KIND_OUT=.demo/kind
BOOTSTRAP_PROBE=host.docker.internal:9095
mkdir -p "$KIND_OUT"

# ---------------------------------------------------------------------------
# PRE-STEP 1/3 — THE KIND NETWORK'S IPv4 GATEWAY.
#
# `docker network inspect kind` is the docker network `kind` creates for its
# nodes; its IPAM gateway is the address the HOST answers on from inside a node
# and therefore from inside a pod. Printed, then asserted non-empty.
#
# **EVERY GATEWAY, AND THE IPv4 ONE — MEASURED, NOT ASSUMED.** kind's network is
# dual-stack, so `.IPAM.Config` holds two entries, and their ORDER IS NOT
# FIXED: on the 2026-09-11 authorised proving run `index .IPAM.Config 0` was
# the IPv6 entry (`fc00:f853:ccd:e793::1`) and the IPv4 gateway
# (`172.22.0.1`) was second. The compose stack publishes 9095 and 9000 through
# docker's port publisher, which binds IPv4, so a `hosts` block carrying the
# IPv6 gateway resolves and then fails to connect — the exact silent-wrong-
# address failure the rest of this pre-step exists to prevent. So this reads
# ALL the gateways and takes the first IPv4 one, and refuses if there is none
# rather than using an address it cannot reach a published port on.
#
# NO FALLBACK. `localhost` inside a pod is the pod, and a demo that quietly
# dialled it would fail at step 7 with a timeout that says nothing about DNS.
# ---------------------------------------------------------------------------
echo
echo "==> pre 1/3 the kind network's IPv4 gateway"
set +e
docker network inspect "$KIND_NETWORK" -f '{{range .IPAM.Config}}{{println .Gateway}}{{end}}' > "$KIND_OUT/gateways.txt" 2> "$KIND_OUT/gateway.err"
rc=$?
set -e
echo "    rc=$rc  (docker network inspect $KIND_NETWORK -f '{{range .IPAM.Config}}{{println .Gateway}}{{end}}')"
echo "    gateways: $(tr '\n' ' ' < "$KIND_OUT/gateways.txt")"
gw=""
while read -r candidate; do
  case "$candidate" in
    *:*) : ;;                  # IPv6: a published port is bound on IPv4.
    ?*)
      if [ -z "$gw" ]; then
        gw="$candidate"
      fi
      ;;
  esac
done < "$KIND_OUT/gateways.txt"
echo "    the IPv4 gateway -> ${gw:-<none>}"
if [ -z "$gw" ]; then
  echo "kind-demo: could not resolve an IPv4 gateway of the docker network \`$KIND_NETWORK\` (docker network inspect $KIND_NETWORK -f '{{range .IPAM.Config}}{{println .Gateway}}{{end}}' exited $rc and printed \"$(tr '\n' ' ' < "$KIND_OUT/gateways.txt")\"). That address is what CoreDNS maps host.docker.internal to, and there is deliberately no fallback to localhost: inside a pod, localhost is the pod." >&2
  cat "$KIND_OUT/gateway.err" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# PRE-STEP 2/3 — THE CoreDNS `hosts` BLOCK.
#
# Four operations, every exit code on its own line, nothing piped:
#   (1) read the `coredns` ConfigMap's `Corefile`;
#   (2) write a new one with the `hosts` block inside the `.:53` server block,
#       render it with `create configmap … --dry-run=client -o yaml > <file>`
#       and `apply -f <file>` — TWO lines, never a pipe;
#   (3) `rollout restart deployment/coredns`;
#   (4) `rollout status deployment/coredns --timeout=120s`.
#
# `fallthrough` is what keeps the rest of cluster DNS working: without it the
# `hosts` plugin answers NXDOMAIN for every name it does not hold, and
# `kubernetes.default` stops resolving.
# ---------------------------------------------------------------------------
echo
echo "==> pre 2/3 CoreDNS resolves host.docker.internal to $gw"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}' > "$KIND_OUT/Corefile" 2> "$KIND_OUT/corefile.err"
rc=$?
set -e
echo "    rc=$rc  (kubectl -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}')"
[ "$rc" -eq 0 ] || die "could not read the coredns ConfigMap (rc=$rc); see $KIND_OUT/corefile.err"

# IDEMPOTENT — MEASURED, NOT ASSUMED — AND AGAINST AN UNMARKED BLOCK TOO.
# A second pass over an already-patched Corefile inserts a SECOND `hosts` block,
# and CoreDNS refuses a server block that declares one plugin twice: the new
# pods crash-loop, the old ones stay, and `rollout status` times out 120 s later
# saying only that coredns did not become ready. That happened on the 2026-09-11
# authorised proving run.
#
# So the awk drops TWO things before writing its own: anything bracketed by its
# markers, AND any `hosts { … }` block whose body names `host.docker.internal`,
# marked or not. The second half is what an older form of this script left
# behind — an unmarked block — and against which the marker-only form produced
# exactly the duplicate-plugin timeout above, whose message names neither the
# block nor this script. What it drops it PRINTS, on stderr, so a transcript
# says what was replaced instead of leaving a reader to diff two Corefiles. A
# `hosts` block for some other name is not this script's and is copied through
# in place, untouched.
#
# THIS CANNOT ARISE IN CI: `.github/workflows/kind-demo.yml` creates the cluster
# it patches and deletes it in the same run, so the Corefile it reads is always
# the one the node image shipped. It arises on a REUSED local cluster, which is
# the state that cost the proving run a delete-and-recreate.
#
# The markers are Corefile comments and CoreDNS ignores them.
set +e
awk -v gw="$gw" '
  /^ *# logweir-kind-demo: BEGIN/ { skip = 1; next }
  /^ *# logweir-kind-demo: END/   { skip = 0; next }
  skip { next }

  # A `hosts` block is BUFFERED to its closing brace, because whether it
  # belongs to this script is decided by its BODY and not by its first line.
  $1 == "hosts" && index($0, "{") > 0 && buffering == 0 {
    buffering = 1; n = 0; hit = 0; depth = 0
  }
  buffering {
    buf[++n] = $0
    if (index($0, "host.docker.internal") > 0) { hit = 1 }
    t = $0; depth += gsub(/\{/, "", t)
    t = $0; depth -= gsub(/\}/, "", t)
    if (depth > 0) { next }
    buffering = 0
    if (hit == 1) {
      # A pipe to `cat 1>&2`, not `> "/dev/stderr"`: POSIX awk names no special
      # files, and the runner`s awk is mawk, not the one this was written under.
      print "kind-demo: replaced a stale hosts block for host.docker.internal (" n " lines):" | "cat 1>&2"
      for (i = 1; i <= n; i++) { print "    " buf[i] | "cat 1>&2" }
    } else {
      for (i = 1; i <= n; i++) { print buf[i] }
    }
    next
  }

  { print }
  /^\.:53 \{/ && inserted == 0 {
    print "    # logweir-kind-demo: BEGIN — the compose stack is on the host"
    print "    hosts {"
    print "        " gw " host.docker.internal"
    print "        fallthrough"
    print "    }"
    print "    # logweir-kind-demo: END"
    inserted = 1
  }
  # A Corefile that ends inside a `hosts` block is malformed already; give its
  # bytes back rather than swallowing them.
  END {
    if (buffering == 1) { for (i = 1; i <= n; i++) { print buf[i] } }
    exit inserted == 1 ? 0 : 3
  }
' "$KIND_OUT/Corefile" > "$KIND_OUT/Corefile.new"
rc=$?
set -e
echo "    rc=$rc  (awk: replace any block this script wrote before, then insert the hosts block into the .:53 server block)"
[ "$rc" -eq 0 ] || die "the coredns Corefile has no \`.:53 {\` server block to patch (awk exited $rc). Corefile as read:
$(cat "$KIND_OUT/Corefile")"
echo "    the patched Corefile:"
sed 's/^/        /' "$KIND_OUT/Corefile.new"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n kube-system create configmap coredns --from-file=Corefile="$KIND_OUT/Corefile.new" --dry-run=client -o yaml > "$KIND_OUT/coredns-configmap.yaml" 2> "$KIND_OUT/coredns-render.err"
rc=$?
set -e
echo "    rc=$rc  (kubectl -n kube-system create configmap coredns --from-file=Corefile=... --dry-run=client -o yaml > $KIND_OUT/coredns-configmap.yaml)"
[ "$rc" -eq 0 ] || die "could not render the coredns ConfigMap (rc=$rc); see $KIND_OUT/coredns-render.err"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n kube-system apply -f "$KIND_OUT/coredns-configmap.yaml" > "$KIND_OUT/coredns-apply.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl -n kube-system apply -f $KIND_OUT/coredns-configmap.yaml)"
[ "$rc" -eq 0 ] || die "could not apply the coredns ConfigMap (rc=$rc); see $KIND_OUT/coredns-apply.log"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n kube-system rollout restart deployment/coredns > "$KIND_OUT/coredns-restart.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl -n kube-system rollout restart deployment/coredns)"
[ "$rc" -eq 0 ] || die "could not restart coredns (rc=$rc); see $KIND_OUT/coredns-restart.log"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n kube-system rollout status deployment/coredns --timeout=120s > "$KIND_OUT/coredns-status.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl -n kube-system rollout status deployment/coredns --timeout=120s)"
cat "$KIND_OUT/coredns-status.log"
[ "$rc" -eq 0 ] || die "coredns did not become ready within 120s (rc=$rc)"

# ---------------------------------------------------------------------------
# PRE-STEP 3/3 — THE REACHABILITY PROBE, FROM INSIDE THE CLUSTER.
#
# Interface register I14. `reachable=true` and exit 0 is the precondition for
# step 1; anything else stops the run here, where the message is about the
# broker and the DNS block, rather than eleven steps later where it would be
# about a `Backup` that never finished.
# ---------------------------------------------------------------------------
echo
echo "==> pre 3/3 cluster-probe --bootstrap $BOOTSTRAP_PROBE, from a pod"
# NOT `--rm --attach`. `kubectl run --attach` connects to the container AFTER
# the pod is created; a container that has already exited by then — this one
# lives for a fraction of a second — hands attach nothing, and `kubectl` still
# exits 0. The tenth CI run (2026-09-12) printed neither I14 line and this gate
# refused; the ninth had won that race. So the pod runs to completion on its
# own, its exit code is read from its own status, and its two lines are read
# from the container log, which the kubelet keeps whether or not anyone was
# watching. Then the pod is deleted. Every exit code on its own line.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete pod bootstrap-probe --ignore-not-found > /dev/null 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl delete pod bootstrap-probe --ignore-not-found — a leftover from an aborted run, if any)"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" run bootstrap-probe --restart=Never --image="$LOGWEIR_DEMO_IMAGE_REF" --image-pull-policy="$LOGWEIR_DEMO_PULL_POLICY" -- cluster-probe --bootstrap "$BOOTSTRAP_PROBE" > "$KIND_OUT/probe-run.txt" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl run bootstrap-probe --restart=Never --image=$LOGWEIR_DEMO_IMAGE_REF --image-pull-policy=$LOGWEIR_DEMO_PULL_POLICY -- cluster-probe --bootstrap $BOOTSTRAP_PROBE)"
[ "$rc" -eq 0 ] || die "could not create the probe pod (rc=$rc); see $KIND_OUT/probe-run.txt"

# Bounded: up to 120 s for the container to terminate, whichever way it does.
phase=""
for i in $(seq 1 60); do
  set +e
  phase="$(kubectl --context "$LOGWEIR_KUBE_CONTEXT" get pod bootstrap-probe -o jsonpath='{.status.phase}' 2>/dev/null)"
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || die "could not read the probe pod's phase (rc=$rc)"
  case "$phase" in Succeeded|Failed) break ;; esac
  sleep 2
done
echo "    phase=${phase:-<none>}  (kubectl get pod bootstrap-probe -o jsonpath='{.status.phase}', polled up to 120 s)"
case "$phase" in Succeeded|Failed) ;; *) die "the probe pod did not terminate within 120 s (phase: ${phase:-<none>}); \`kubectl describe pod bootstrap-probe\` says why" ;; esac

set +e
probe_exit="$(kubectl --context "$LOGWEIR_KUBE_CONTEXT" get pod bootstrap-probe -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null)"
rc=$?
set -e
echo "    rc=$rc  (kubectl get pod bootstrap-probe -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}') -> container exit ${probe_exit:-<none>}"
[ "$rc" -eq 0 ] || die "could not read the probe container's exit code (rc=$rc)"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" logs bootstrap-probe > "$KIND_OUT/probe.txt" 2> "$KIND_OUT/probe-logs.err"
rc=$?
set -e
echo "    rc=$rc  (kubectl logs bootstrap-probe — the container log, which the kubelet keeps whether or not anyone attached)"
[ "$rc" -eq 0 ] || die "could not read the probe pod's log (rc=$rc); see $KIND_OUT/probe-logs.err"
cat "$KIND_OUT/probe.txt"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete pod bootstrap-probe > /dev/null 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl delete pod bootstrap-probe)"

[ "${probe_exit:-1}" = "0" ] || die "the in-cluster probe could not reach $BOOTSTRAP_PROBE (container exit ${probe_exit:-<none>}). The CoreDNS hosts block above maps that name to $gw; if the name resolved and the dial still failed, the compose stack's K8S listener is not published on the runner host."
case "$(cat "$KIND_OUT/probe.txt")" in
  *reachable=true*) echo "    reachable=true — the advertised listener resolves in-cluster." ;;
  *) die "the probe exited 0 without printing \`reachable=true\`. I14's contract is two lines, \`cluster-id=<id>\` then \`reachable=true|false\`; what it printed is above." ;;
esac

# ---------------------------------------------------------------------------
# AND NOW THE TWELVE STEPS, from `scripts/demo-steps.sh`, in the one order
# there is.
# ---------------------------------------------------------------------------
demo_run
