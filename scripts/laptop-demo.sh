#!/usr/bin/env bash
# `just laptop-demo` — **Phase C's exit criterion**, and install gate
# **X-UIWRITE**, scripted. Task 28.
#
# This is the walk spec §1 calls "the whole acceptance surface": a stranger
# clones, applies ONE file to docker-desktop Kubernetes and gets CRDs, RBAC and
# the controller; the UI is static files served by `kubectl proxy --www=`;
# against a plain broker from the compose stack they create a `BackupSchedule`,
# watch a `Backup` produce a signed receipt, run a drill (a `Restore` with a
# `newTopic` target) and read a signed scorecard with measured RTO and RPO —
# **never typing a private key into a browser**.
#
# Twelve numbered steps. The transcript of the run that proved it is
# `e2e/k8s/laptop-demo.md`.
#
# ===========================================================================
# X-UIWRITE HAS TWO HALVES AND THE SECOND ONE IS THE GATE
# ===========================================================================
# Spec §10's wording is: under `kubectl proxy --www=`, a `create` of a
# `Restore` **from the page** returns 201. **`curl` is not the page.** A
# transcript that recorded only the scripted half would satisfy every
# assertion a naive test could state — the section exists, it is non-empty, it
# has a verdict, it contains `201` — and would still not have run the gate.
#
# So step 10 has two halves and they are recorded separately:
#
#   (a) the SCRIPTED half — `curl -X POST … ?fieldManager=logweir-cli`, whose
#       status is `201`, over a body built by the PAGE's own code
#       (`ui/tests/emit-restore-body.js` calls `renderPlanBytes`, `planHash`,
#       `mintNames` and `restoreBody`);
#   (b) the IN-BROWSER half — the same create performed by hand from the
#       wizard's final step. `ui/api.js`'s `create` appends
#       `?fieldManager=logweir-ui` (interface register I23), so the created
#       object's `metadata.managedFields` names `logweir-ui` and NOT `kubectl`
#       and not a browser-derived User-Agent. That string is the evidence the
#       write came from the page; nothing else in the transcript can produce
#       it.
#
# Under `LOGWEIR_DEMO_NONINTERACTIVE=1` half (b) prints its command and says it
# is skipped. It is performed in a second pass:
#
#     LOGWEIR_DEMO_ONLY_STEP=10b ./scripts/laptop-demo.sh
#
# with the proxy still up from the first pass.
#
# ===========================================================================
# NOTHING TYPES A PRIVATE KEY INTO THE BROWSER, AND A TEST SAYS SO
# ===========================================================================
# The wizard's create needs no key at all. The approval for the plan is minted
# on the HOST at step 11 with `logweir drill approve` and the approver's own
# private key, and only `approval.json` + `approval.sig` — two public
# documents — ever reach the page.
# `crates/logweir/tests/laptop_demo_lint.rs::laptop_demo_never_sends_a_key_to_the_page`
# asserts that step 11 is on the host and that no `curl` line in this file
# posts a `*.pem` or `*.key`.
#
# The two keypairs this demo uses are minted at run time into `.demo/laptop/`
# (gitignored) and DELETED BY THE TEARDOWN. No key material from this script
# is ever checked in, and the throwaway fixture keypair the node suite uses
# (`ui/tests/fixtures/approver.pem`) is not touched here.
#
# ===========================================================================
# THE CONTEXT CHECK IS FIRST, AND IT REFUSES BEFORE ANYTHING DIALS
# ===========================================================================
# Step 1's first command is `kubectl --context docker-desktop config
# current-context`. If the kubeconfig's own current context is anything else
# this script prints, on stderr,
#
#     refusing: current context is <what it found>, not docker-desktop
#
# and exits 1 — before a namespace is created, before an image is inspected,
# before anything reaches a socket. The `--context docker-desktop` on that line
# is STANDING RULE 12, which admits no exemption; `config current-context`
# reads the kubeconfig's own `current-context` field and ignores the override,
# so the flag is required by the lint and changes nothing about what is read.
# `laptop_demo_refuses_a_wrong_context` proves the refusal with a stubbed
# `kubectl` that dials nothing.
#
# EVERY `kubectl` LINE IN THIS FILE SPELLS THE CONTEXT OUT (STANDING RULE 12),
# as a literal and never through a variable, so
# `laptop_demo_names_every_kubectl_context` can read it.
#
# ===========================================================================
# EXIT CODES ARE READ DIRECTLY, NEVER THROUGH A PIPE (STANDING RULE 20)
# ===========================================================================
#     set +e
#     kubectl … > "$OUT/something.out"
#     rc=$?
#     set -e
#
# — the command on its own line, its status on the very next one. Task 12's
# tokeniser (`crates/logweir/tests/support/exit_code_lint.rs`, interface
# register I29) runs over this file from
# `crates/logweir/tests/laptop_demo_lint.rs`, in the DEFAULT test suite, and
# fails naming any line that breaks the shape. It is REUSED, not re-written.
# Its guard is a literal first-word rule — `kubectl`, `curl`, `logweir`,
# `docker`, `just` — so the `logweir` binary goes on `$PATH` through a
# one-entry shim directory and is written as the bare word, exactly as
# `scripts/k8s-demo.sh` and `scripts/mvp-demo.sh` do.
#
# `curl`'s HTTP statuses come from `curl`'s own `-w '%{http_code}'` output,
# written to a file and read back; the exit code is `curl`'s, on the next line.
#
# ===========================================================================
# WHAT IS AUTHOR-ONLY HERE, AND GLOBAL CONSTRAINT 37 IS NOT RELAXED BY IT
# ===========================================================================
# Two steps only work on the machine the images were built on:
#
#   * the `docker tag` pair in step 1. The kubelet keys images on the WHOLE
#     reference, so the shipped `ghcr.io/logweir/<name>@sha256:…` is
#     `ErrImageNeverPull` on a node holding the same digest under a local name
#     (plan erratum E19b, `docs/kubernetes.md` §14.3). One `docker tag` each
#     makes them resolve, and the teardown removes both.
#   * the `config/overlays/k8s-demo` patch in step 3, which points the
#     controller at this laptop's compose MinIO
#     (`http://host.docker.internal:9000`). The SHIPPED `logweir.yaml` is
#     applied unedited; the demo's own S3 literals live in the overlay, and the
#     controller forwards all three to every runner Job it creates.
#
# "Published" means a PULL from a registry the author does not control. This
# script proves the manifests are right and the binaries run; it says nothing
# about whether a stranger can install Logweir from a registry, and the install
# file's digest rows still read `blocked: no remote`.
#
# ===========================================================================
# THE COMPOSE STACK IS A PRECONDITION, AND THE TEARDOWN TAKES IT DOWN
# ===========================================================================
# Spec §2's Demo 1 opens with `docker compose -f e2e/compose/docker-compose.yml
# up`, so the stack is the stranger's first command and this script's step-1
# precondition rather than one of its steps. STANDING RULE 3 still binds the
# other end: the teardown runs `just e2e-down`, and the stack is never left up.
#
# `just lint` and the compose stack are two different machine states —
# `scripts/time-unit-suite.sh` refuses, exit 1, while 9092 or 9000 answers
# (Global Constraint 22) — so `just lint` is NOT part of this script and is run
# after the teardown.
#
# ===========================================================================
# ENVIRONMENT KNOBS
# ===========================================================================
#   LOGWEIR_DEMO_NONINTERACTIVE=1   suppress every PAUSE prompt; step 10(b)
#                                   prints its command and says it is skipped.
#   LOGWEIR_DEMO_ONLY_STEP=10b      run the preflight and step 10(b) ALONE,
#                                   against a cluster and a proxy a previous
#                                   pass left up. Installs no teardown.
#   LOGWEIR_DEMO_ONLY_STEP=teardown run the teardown ALONE.
#   LOGWEIR_DEMO_RESTORE_NAME=<n>   step 10(b): the `Restore` the page created,
#                                   instead of reading it from the terminal.
#   LOGWEIR_DEMO_KEEP=1             DISCLOSED ADDITION (Task 28). Leave the
#                                   cluster, the proxy and the stack UP at the
#                                   end instead of tearing them down. The
#                                   acceptance asks for a full run AND for a
#                                   second interactive pass over step 10(b)
#                                   "with the proxy still up from the first
#                                   pass"; a run that tore everything down has
#                                   nothing for the second pass to talk to. The
#                                   DEFAULT is a full teardown, and the recorded
#                                   acceptance run uses the default.
#   LOGWEIR_PYTHON                  the interpreter with `cryptography`.
#   RECORDS                         how many records to produce (default 200).
set -euo pipefail
cd "$(dirname "$0")/.."

NS=logweir-t28
SYS=logweir-system
OUT=.demo/laptop
COMPOSE_FILE=e2e/compose/docker-compose.yml
BOOTSTRAP_INNET=kafka-broker-1:9094
BOOTSTRAP_K8S=host.docker.internal:9095
ARCHIVE_BUCKET=kafka-backups
BACKUP_PREFIX=laptop-demo
ARCHIVE_URL="s3://${ARCHIVE_BUCKET}/${BACKUP_PREFIX}"
S3_ENDPOINT=http://host.docker.internal:9000
S3_REGION=us-east-1
TOPIC=laptopdemo
PROXY_BASE=http://127.0.0.1:8001
API_BASE="$PROXY_BASE/apis/logweir.dev/v1alpha1/namespaces/$NS"
RUNNER_IMAGE_TAG=ghcr.io/logweir/logweir:v0.1.0
CONTROLLER_IMAGE_TAG=ghcr.io/logweir/weirkeeper:v0.1.0
ONLY_STEP="${LOGWEIR_DEMO_ONLY_STEP:-}"
PROXY_PID=""

die() { echo; echo "laptop-demo: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

# The PAUSE prompt. `LOGWEIR_DEMO_NONINTERACTIVE=1` suppresses it, which is
# what the recorded run uses; without it the script stops so an operator can
# read the screen, or do the thing the step is about.
pause() {
  if [ -n "${LOGWEIR_DEMO_NONINTERACTIVE:-}" ]; then
    echo "    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): $*"
    return 0
  fi
  echo
  echo "    PAUSE: $*"
  printf '    press RETURN to continue: '
  read -r _ || true
  echo
}

# ---------------------------------------------------------------------------
# TEARDOWN. Installed as an EXIT trap by the full run, and reachable on its own
# as `LOGWEIR_DEMO_ONLY_STEP=teardown`. STANDING RULE 13's T21-T24 exception:
# the control plane is in the fixed namespace `logweir-system` and the custom
# resources are in `logweir-t28`; both go. STANDING RULE 3: the stack goes too.
# Every rc is printed.
# ---------------------------------------------------------------------------
teardown() {
  echo
  echo "==> 12/12 teardown"

  # THE PROXY IS THE ONE PROCESS THIS SCRIPT BACKGROUNDS, and it is killed here
  # by the pid it recorded. A teardown-only pass never started it, so it falls
  # back to the pattern -- narrow enough that it can match nothing else on the
  # machine.
  if [ -n "$PROXY_PID" ]; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
    echo "    stopped the kubectl proxy (pid $PROXY_PID)"
  else
    pkill -f 'proxy --www=./ui --www-prefix=/ui/' 2>/dev/null || true
    echo "    asked any kubectl proxy serving ./ui to stop (this process started none)"
  fi

  set +e
  kubectl --context docker-desktop delete -f logweir.yaml --ignore-not-found > "$OUT/teardown-install.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete -f logweir.yaml)"

  set +e
  kubectl --context docker-desktop delete ns "$SYS" "$NS" --ignore-not-found > "$OUT/teardown-ns.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete ns $SYS $NS)"

  sweep_archive

  set +e
  docker rmi "$RUNNER_IMAGE_TAG" "$CONTROLLER_IMAGE_TAG" > "$OUT/teardown-tags.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (docker rmi the two author-only tags)"

  set +e
  just e2e-down > "$OUT/teardown-e2e-down.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (just e2e-down)"

  # THE KEYS GO LAST AND THEY GO UNCONDITIONALLY. Both were minted minutes ago
  # by this script and are attested by nothing; leaving a private key in a
  # working directory is how a demo key becomes a production key.
  rm -f "$OUT"/*.pem
  echo "    removed $OUT/*.pem (both keypairs this run minted)"
}

on_exit() {
  if [ -n "${LOGWEIR_DEMO_KEEP:-}" ]; then
    echo
    echo "==> 12/12 teardown DEFERRED (LOGWEIR_DEMO_KEEP=1)"
    echo "    The cluster, the kubectl proxy (pid ${PROXY_PID:-none}) and the compose stack are"
    echo "    STILL UP, for the second pass over step 10(b). Tear them down with:"
    echo "        LOGWEIR_DEMO_ONLY_STEP=teardown ./scripts/laptop-demo.sh"
    return 0
  fi
  teardown
}

# `just e2e-down` runs `down -v` and EMPTIES the MinIO volume (plan erratum
# E12g/E15), so every owner of the stack makes its own bucket and sweeps its
# own prefix. Swept at BOTH ends: a second backup into a colliding prefix does
# not accumulate, and a stale manifest would be resolved as `latestCompleted`.
sweep_archive() {
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup rm --recursive --force "local/$ARCHIVE_BUCKET/$BACKUP_PREFIX/" > "$OUT/sweep-archive.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (mc rm $ARCHIVE_BUCKET/$BACKUP_PREFIX/ — a non-zero here just means there was nothing to sweep)"
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup rm --recursive --force "local/$ARCHIVE_BUCKET/logweir/" > "$OUT/sweep-evidence.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (mc rm $ARCHIVE_BUCKET/logweir/ — same)"
}

# Dumps what a failed run was about BEFORE the teardown deletes it. A demo that
# says "the Backup did not exit 0" and then deletes the Backup has told you
# nothing you can act on.
dump_run() {  # kind name
  echo
  echo "--- $1/$2, as the cluster last saw it -------------------------------"
  set +e
  kubectl --context docker-desktop -n "$NS" get "$1" "$2" -o yaml > "$OUT/failed-$1.yaml" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get $1 $2 -o yaml)"
  cat "$OUT/failed-$1.yaml"
  set +e
  kubectl --context docker-desktop -n "$NS" get jobs,pods -o wide > "$OUT/failed-workload.txt" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get jobs,pods -o wide)"
  cat "$OUT/failed-workload.txt"
  set +e
  kubectl --context docker-desktop -n "$NS" logs "job/$2" --tail=200 --all-containers=true > "$OUT/failed-pod.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl logs job/$2 --tail=200)"
  cat "$OUT/failed-pod.log"
  set +e
  kubectl --context docker-desktop -n "$SYS" logs deploy/weirkeeper --tail=120 > "$OUT/failed-controller.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl logs deploy/weirkeeper --tail=120)"
  cat "$OUT/failed-controller.log"
  echo "---------------------------------------------------------------------"
}

# THE TRANSCRIPT LINE GOES TO stderr AND THE VALUE TO stdout (Task 24's
# `read_field`, measured): `v=$(read_field …)` captures STDOUT, so an `echo` of
# the human-readable line on stdout would end up INSIDE the value.
read_field() {  # kind name jsonpath label
  set +e
  value=$(kubectl --context docker-desktop -n "$NS" get "$1" "$2" -o jsonpath="$3")
  rc=$?
  set -e
  echo "    rc=$rc  $4: ${value:-<absent>}" >&2
  printf '%s' "$value"
}

# One static or API fetch through the proxy, its HTTP status printed and its
# exit code read on its own line. The status comes out of `curl`'s own
# `-w '%{http_code}'`; nothing is piped.
expect_200() {  # url label
  set +e
  curl -sS -o /dev/null -w '%{http_code}' "$1" > "$OUT/http-status.txt"
  rc=$?
  set -e
  http=$(cat "$OUT/http-status.txt")
  echo "    rc=$rc  HTTP $http  $2"
  [ "$rc" -eq 0 ] || die "curl exited $rc fetching $1 — is the proxy still up?"
  [ "$http" = "200" ] || die "$1 answered $http, not 200. A --www-prefix change or a rename of ui/ fails exactly here: the page the whole of Phase C is about is not being served."
}

# ---------------------------------------------------------------------------
# 1/12 PREFLIGHT — the context FIRST, so a wrong one refuses before anything
#      dials; then the tools, the stack and the two local images.
# ---------------------------------------------------------------------------
step_01() {
  step "1/12 preflight: context, tools, the compose stack, both local images"

  mkdir -p "$OUT"

  set +e
  kubectl --context docker-desktop config current-context > "$OUT/current-context.txt" 2> "$OUT/current-context.err"
  rc=$?
  set -e
  found=$(tr -d ' \t\n' < "$OUT/current-context.txt")
  echo "    rc=$rc  (kubectl config current-context) -> ${found:-<empty>}"
  if [ "$found" != "docker-desktop" ]; then
    echo "refusing: current context is ${found:-<unreadable>}, not docker-desktop" >&2
    exit 1
  fi

  for tool in docker kubectl just openssl awk; do
    command -v "$tool" >/dev/null 2>&1 || die "\`$tool\` is not on \$PATH."
  done
  PYTHON="${LOGWEIR_PYTHON:-python3}"
  command -v "$PYTHON" >/dev/null 2>&1 || die "\`$PYTHON\` is not on \$PATH; set LOGWEIR_PYTHON."
  command -v node >/dev/null 2>&1 || die "\`node\` is not on \$PATH; step 10(a) builds the create body with the page's own modules."

  # THE COMPOSE STACK IS A PRECONDITION (spec §2: the stranger's first command
  # is `docker compose … up`). Asked of the broker and of MinIO by name, so
  # "up" means "these two are running" and not "some container is".
  set +e
  docker compose -f "$COMPOSE_FILE" ps --status running --format '{{.Service}}' > "$OUT/compose-ps.txt" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (docker compose ps --status running)"
  [ "$rc" -eq 0 ] || die "\`docker compose ps\` exited $rc. Is Docker running?"
  running=$(cat "$OUT/compose-ps.txt")
  echo "    compose services running: $(echo "$running" | tr '\n' ' ')"
  case "$running" in *kafka-broker-1*) ;; *) die "the compose broker is not running. Spec §2's Demo 1 begins with the stack: \`just e2e-up\` (or \`docker compose -f $COMPOSE_FILE up -d --wait\`), then re-run this script. STANDING RULE 3: check-then-take, and \`just e2e-down\` afterwards — which this script's teardown does." ;; esac
  case "$running" in *minio*) ;; *) die "the compose MinIO is not running; the archive has nowhere to go. Bring the stack up with \`just e2e-up\`." ;; esac

  # BOTH LOCAL IMAGES, under `imagePullPolicy: Never` (STANDING RULE 8).
  # NEITHER IS REBUILT HERE AND NEITHER MAY BE: a rebuild moves the repository
  # digest (plan erratum E19a), and these two digests are pinned in
  # `crates/weirkeeper/src/job.rs` and `config/manager/deployment.yaml`.
  set +e
  docker image inspect logweir:check --format '{{json .RepoDigests}}' > "$OUT/img-runner.json" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (docker image inspect logweir:check) -> $(cat "$OUT/img-runner.json")"
  [ "$rc" -eq 0 ] || die "no local \`logweir:check\` image. Build it with \`just image\` — and note that a rebuild CHANGES ITS DIGEST, which would then no longer match \`weirkeeper::job::RUNNER_IMAGE\` (plan erratum E19a)."
  set +e
  docker image inspect weirkeeper:check --format '{{json .RepoDigests}}' > "$OUT/img-controller.json" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (docker image inspect weirkeeper:check) -> $(cat "$OUT/img-controller.json")"
  [ "$rc" -eq 0 ] || die "no local \`weirkeeper:check\` image. Build it with \`just image-weirkeeper\` — same digest caveat."

  # STANDING RULE 13: refuse a cluster that is halfway through somebody else's
  # install. Zero CRDs (clean) or exactly the six this plan ships, nothing
  # between.
  set +e
  kubectl --context docker-desktop get crd -o name > "$OUT/crds.txt" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get crd -o name)"
  [ "$rc" -eq 0 ] || die "the docker-desktop cluster is not reachable (rc=$rc). Enable Kubernetes in Docker Desktop."
  ours=$(grep -c 'logweir.dev' "$OUT/crds.txt" || true)
  echo "    logweir.dev CRDs already installed: $ours"
  if [ "$ours" -ne 0 ] && [ "$ours" -ne 6 ]; then
    die "the cluster holds $ours logweir.dev CRDs. STANDING RULE 13 permits 0 (a clean cluster) or exactly 6 (this plan's own install) and nothing between."
  fi

  # THE AUTHOR-ONLY TAG STEP (plan erratum E19b). The kubelet keys on the WHOLE
  # reference, so `ghcr.io/logweir/logweir@sha256:<d>` is `ErrImageNeverPull` on
  # a node that holds the same digest under the local name `logweir:check`. The
  # teardown removes both tags.
  set +e
  docker tag logweir:check "$RUNNER_IMAGE_TAG"
  rc=$?
  set -e
  echo "    rc=$rc  (docker tag logweir:check $RUNNER_IMAGE_TAG — author-only, removed by the teardown)"
  [ "$rc" -eq 0 ] || die "could not tag the runner image (rc=$rc)"
  set +e
  docker tag weirkeeper:check "$CONTROLLER_IMAGE_TAG"
  rc=$?
  set -e
  echo "    rc=$rc  (docker tag weirkeeper:check $CONTROLLER_IMAGE_TAG — author-only, removed by the teardown)"
  [ "$rc" -eq 0 ] || die "could not tag the controller image (rc=$rc)"

  # The `logweir` binary as the BARE WORD, so the I29 tokeniser's literal
  # first-word rule guards every line that runs it.
  LOGWEIR_BIN="${LOGWEIR_BIN:-$PWD/target/debug/logweir}"
  [ -x "$LOGWEIR_BIN" ] || die "no logweir binary at $LOGWEIR_BIN — run \`cargo build -p logweir\` (\`just laptop-demo\` does it for you; plan erratum E9)."
  mkdir -p "$OUT/bin"
  ln -sf "$LOGWEIR_BIN" "$OUT/bin/logweir"
  PATH="$PWD/$OUT/bin:$PATH"
  export PATH

  pause "the preflight passed: docker-desktop, the compose stack, both local images."
}

# ---------------------------------------------------------------------------
# 2/12 THE NAMESPACE.
# ---------------------------------------------------------------------------
step_02() {
  step "2/12 namespace $NS"

  set +e
  kubectl --context docker-desktop create namespace "$NS" > "$OUT/ns.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl create namespace $NS)"
  [ "$rc" -eq 0 ] || die "could not create $NS (rc=$rc); see $OUT/ns.log"

  # The runner ServiceAccount lives in the namespace of the Backup/Restore
  # objects, so it is NOT in logweir.yaml (plan erratum E14c). A runner pod
  # mounts no token, so it needs no RoleBinding.
  set +e
  kubectl --context docker-desktop -n "$NS" apply -f config/rbac/backup-runner-serviceaccount.yaml > "$OUT/runner-sa.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl apply -f config/rbac/backup-runner-serviceaccount.yaml)"
  [ "$rc" -eq 0 ] || die "could not apply the runner ServiceAccount (rc=$rc)"

  pause "namespace $NS and the runner ServiceAccount exist."
}

# ---------------------------------------------------------------------------
# 3/12 X-APPLY — `just apply-install`, which applies logweir.yaml TWICE with
#      both exit codes read directly, plus this laptop's author-only overlay.
# ---------------------------------------------------------------------------
step_03() {
  step "3/12 just apply-install (install gate X-APPLY), then the author-only demo overlay"

  set +e
  just apply-install > "$OUT/apply-install.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (just apply-install — kubectl apply --server-side -f logweir.yaml, twice)"
  [ "$rc" -eq 0 ] || die "\`just apply-install\` exited $rc; see $OUT/apply-install.log"
  tail -4 "$OUT/apply-install.log"

  # THE DEMO'S S3 LITERALS LIVE IN THE OVERLAY AND NOT IN THE SHIPPED FILE. A
  # stranger must be able to apply `logweir.yaml` unedited (Global Constraint
  # 37), so the endpoint, the region and the allow-http flag are a patch this
  # script applies on top — and the controller forwards all three to every
  # runner Job it creates, so the endpoint is configured once instead of in two
  # places that can disagree.
  set +e
  kubectl --context docker-desktop -n "$SYS" patch deployment weirkeeper --patch-file config/overlays/k8s-demo/deployment-env-patch.yaml > "$OUT/apply-overlay.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl patch deployment weirkeeper --patch-file config/overlays/k8s-demo/deployment-env-patch.yaml)"
  [ "$rc" -eq 0 ] || die "applying the demo overlay exited $rc; see $OUT/apply-overlay.log"

  set +e
  kubectl --context docker-desktop -n "$SYS" rollout status deploy/weirkeeper --timeout=300s > "$OUT/rollout.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl rollout status deploy/weirkeeper)"
  [ "$rc" -eq 0 ] || die "the controller did not become ready (rc=$rc); see $OUT/rollout.log and \`kubectl -n $SYS describe pod\`"

  pause "X-APPLY: logweir.yaml applied twice, no error on either run; the controller is ready."
}

# ---------------------------------------------------------------------------
# 4/12 THE TWO KEYPAIRS, AND THE SILENT-MINT WARNING.
# ---------------------------------------------------------------------------
step_04() {
  step "4/12 minting the signing and approver keypairs into $OUT/"

  # The four commands `scripts/demo.sh` uses. `openssl ecparam -genkey` emits a
  # SEC1 key; `openssl pkcs8 -topk8 -nocrypt` is what makes it the PKCS#8 the
  # loader reads.
  openssl ecparam -genkey -name prime256v1 -noout > "$OUT/signing.der.pem"
  openssl pkcs8 -topk8 -nocrypt -in "$OUT/signing.der.pem" -out "$OUT/signing.pem"
  openssl ecparam -genkey -name prime256v1 -noout > "$OUT/approver.der.pem"
  openssl pkcs8 -topk8 -nocrypt -in "$OUT/approver.der.pem" -out "$OUT/approver.pem"
  rm -f "$OUT/signing.der.pem" "$OUT/approver.der.pem"
  openssl ec -in "$OUT/signing.pem"  -pubout -out "$OUT/signing.pub.pem"  2>/dev/null
  openssl ec -in "$OUT/approver.pem" -pubout -out "$OUT/approver.pub.pem" 2>/dev/null
  chmod 600 "$OUT/signing.pem" "$OUT/approver.pem"

  # `VerifyingKey::key_id()` is the sha256 of the SPKI DER, hex — computed here
  # with openssl so this script and `logweir-verify` cannot disagree about it.
  SIGNING_KEY_ID=$(openssl ec -pubin -in "$OUT/signing.pub.pem" -outform DER 2>/dev/null | openssl dgst -sha256 -hex | awk '{print $NF}')
  APPROVER_KEY_ID=$(openssl ec -pubin -in "$OUT/approver.pub.pem" -outform DER 2>/dev/null | openssl dgst -sha256 -hex | awk '{print $NF}')
  [ -n "$SIGNING_KEY_ID" ] || die "could not compute the signing key id"
  [ -n "$APPROVER_KEY_ID" ] || die "could not compute the approver key id"
  echo "    signing key id:  $SIGNING_KEY_ID"
  echo "    approver key id: $APPROVER_KEY_ID"
  echo
  echo "    WARNING — SigningKey::load_or_generate MINTS SILENTLY."
  echo "    crates/logweir-evidence/src/keys.rs: a path that EXISTS is loaded; a path that"
  echo "    is ABSENT is minted. So a run against an empty logweir-signing-key Secret"
  echo "    produces evidence signed by a key nothing attests — a green scorecard with a"
  echo "    signature no TrustRoster can match. Both keypairs above were minted on this"
  echo "    machine, seconds ago, and are attested by nothing; the teardown deletes them."
  echo "    NEITHER PRIVATE HALF EVER REACHES THE BROWSER (step 11)."

  pause "two keypairs in $OUT/; only the public halves leave this directory."
}

# ---------------------------------------------------------------------------
# 5/12 THE FIVE SECRETS, AND `just check-secrets` (interface register I26).
# ---------------------------------------------------------------------------
step_05() {
  step "5/12 the five Secrets, then just check-secrets $NS"

  # 1. the runner's signing key. The data key is `signing.pem` — the file name
  #    the runner's argv reads (plan erratum E14b).
  set +e
  kubectl --context docker-desktop -n "$NS" create secret generic logweir-signing-key --from-file=signing.pem="$OUT/signing.pem" > "$OUT/secret-signing.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (secret/logweir-signing-key, data key signing.pem)"
  [ "$rc" -eq 0 ] || die "could not create logweir-signing-key (rc=$rc)"

  # 2. the approval bundle — FOUR keys at /approval (spec §7 amendment 4c: its
  #    own Secret, not folded into the signing key's).
  #
  #    TWO OF THE FOUR CANNOT EXIST YET, and saying so is more honest than
  #    reordering the walk to hide it: `approval.json` and `approval.sig` are
  #    signatures over the PLAN BYTES, and the plan bytes are step 10's. So the
  #    bundle is created here with the two halves that do exist plus two
  #    placeholders, `just check-secrets` sees all five, and STEP 11 REPLACES
  #    IT with the real four before the `Approval` object is created — which is
  #    before the runner Job that mounts it can start, because the Job is not
  #    created until the approval verifies.
  printf '{"allowed_cluster_ids":["SCRATCH-CLUSTER-NOT-THE-SOURCE"],"source_cluster_id":null}\n' > "$OUT/allowed-clusters.json"
  printf 'replaced-at-step-11\n' > "$OUT/approval.placeholder"
  set +e
  kubectl --context docker-desktop -n "$NS" create secret generic logweir-approval-bundle --from-file=approval.json="$OUT/approval.placeholder" --from-file=approval.sig="$OUT/approval.placeholder" --from-file=approver.pub.pem="$OUT/approver.pub.pem" --from-file=allowed-clusters.json="$OUT/allowed-clusters.json" > "$OUT/secret-approval.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (secret/logweir-approval-bundle, four keys — approval.json/.sig replaced at step 11)"
  [ "$rc" -eq 0 ] || die "could not create logweir-approval-bundle (rc=$rc)"

  # 3. the per-cluster SCRAM credential. This demo's listener is PLAINTEXT so
  #    no runner reads it; it exists because `just check-secrets` counts five
  #    and an install missing one is exactly what that check catches. What is
  #    fixed is the DATA KEY, `password`; the NAME is the adopter's, whatever
  #    `KafkaCluster.spec.auth.secretRef` says.
  set +e
  kubectl --context docker-desktop -n "$NS" create secret generic kafka-scram --from-literal=password=unused-on-the-plaintext-listener > "$OUT/secret-scram.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (secret/kafka-scram, data key password)"
  [ "$rc" -eq 0 ] || die "could not create kafka-scram (rc=$rc)"

  # 4. the runner's archive credential.
  set +e
  kubectl --context docker-desktop -n "$NS" create secret generic logweir-s3 --from-literal=access-key-id=minioadmin --from-literal=secret-access-key=minioadmin > "$OUT/secret-s3.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (secret/logweir-s3)"
  [ "$rc" -eq 0 ] || die "could not create logweir-s3 (rc=$rc)"

  # 5. the CONTROLLER's read-only evidence credential — a DIFFERENT PRINCIPAL,
  #    in logweir-system. MinIO gives no second identity out of the box, so the
  #    demo uses the same key pair under a different Secret name in a different
  #    namespace: WHAT THIS PROVES IS THE PROJECTION, not the IAM policy. An
  #    adopter gives this principal `s3:GetObject` on the evidence prefix and
  #    nothing else.
  set +e
  kubectl --context docker-desktop -n "$SYS" create secret generic logweir-evidence-ro --from-literal=access-key-id=minioadmin --from-literal=secret-access-key=minioadmin > "$OUT/secret-evidence.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (secret/logweir-evidence-ro, in $SYS)"
  [ "$rc" -eq 0 ] || die "could not create logweir-evidence-ro (rc=$rc)"

  # The controller reads that Secret from its OWN ENV, fixed at container
  # start, so it has to restart to see it.
  set +e
  kubectl --context docker-desktop -n "$SYS" rollout restart deploy/weirkeeper > "$OUT/restart.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl rollout restart deploy/weirkeeper — env is fixed at container start)"
  [ "$rc" -eq 0 ] || die "could not restart the controller (rc=$rc)"
  set +e
  kubectl --context docker-desktop -n "$SYS" rollout status deploy/weirkeeper --timeout=300s > "$OUT/rollout2.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl rollout status deploy/weirkeeper, after the evidence Secret)"
  [ "$rc" -eq 0 ] || die "the controller did not come back (rc=$rc)"

  set +e
  just check-secrets "$NS" > "$OUT/check-secrets.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (just check-secrets $NS)"
  cat "$OUT/check-secrets.log"
  [ "$rc" -eq 0 ] || die "\`just check-secrets $NS\` exited $rc; see $OUT/check-secrets.log"

  pause "all five Secrets are present (four in $NS, logweir-evidence-ro in $SYS)."
}

# ---------------------------------------------------------------------------
# 6/12 THE CLUSTER-SCOPED TrustRoster `default` (interface register I16/I17).
# ---------------------------------------------------------------------------
step_06() {
  step "6/12 TrustRoster default — the approver key id and the signing key MATERIAL"

  # `signingKeys[]` carries the runner's PUBLIC key, not just its id: with a
  # `signingKeyIds: [string]` shape there would be nothing to verify against
  # and `status.evidence.verification.result` could never be `Valid`.
  "$PYTHON" - "$OUT/signing.pub.pem" "$SIGNING_KEY_ID" "$OUT/approver.pub.pem" "$APPROVER_KEY_ID" "$OUT/trustroster.yaml" <<'PY'
import sys, textwrap
sig_pem, sig_id, app_pem, app_id, out = sys.argv[1:6]
def block(path):
    return textwrap.indent(open(path).read().rstrip("\n"), " " * 8)
doc = f"""apiVersion: logweir.dev/v1alpha1
kind: TrustRoster
metadata:
  name: default
spec:
  allowedClusterIds: []
  approverKeys:
    - keyId: {app_id}
      subject: laptop-demo-approver@example.invalid
      spkiPem: |
{block(app_pem)}
  signingKeys:
    - keyId: {sig_id}
      subject: logweir-runner@example.invalid
      spkiPem: |
{block(sig_pem)}
"""
open(out, "w").write(doc)
PY
  set +e
  kubectl --context docker-desktop apply -f "$OUT/trustroster.yaml" > "$OUT/roster.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl apply -f trustroster.yaml — cluster-scoped, name 'default')"
  [ "$rc" -eq 0 ] || die "could not apply the TrustRoster (rc=$rc); see $OUT/roster.log"

  pause "TrustRoster default carries the approver key id and the signing key material."
}

# ---------------------------------------------------------------------------
# 7/12 THE KafkaCluster AT THE PUBLISHED K8S LISTENER, AND `reachable: true`.
# ---------------------------------------------------------------------------
step_07() {
  step "7/12 KafkaCluster at $BOOTSTRAP_K8S -> status.reachable"

  # `host.docker.internal:9095` IS THE PUBLISHED `K8S` LISTENER (Task 7,
  # STANDING RULE 15). The runner is a POD, and the stack's OTHER host-side
  # listener -- `EXTERNAL`, the one the compose harness uses on port 9092 -- is
  # the POD'S OWN loopback from inside the cluster, which a broker that
  # advertised it would send every later connection to as well. The name is
  # deliberately not spelled out here: `laptop_demo_uses_the_published_k8s_
  # listener` forbids that literal anywhere in this file, so the guard cannot
  # be satisfied by a comment.
  cat > "$OUT/kafkacluster.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: demo
  namespace: $NS
spec:
  bootstrapServers: ["$BOOTSTRAP_K8S"]
  auth:
    mode: plaintext
  role: source
YAML
  set +e
  kubectl --context docker-desktop apply -f "$OUT/kafkacluster.yaml" > "$OUT/kc-apply.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl apply -f kafkacluster.yaml)"
  [ "$rc" -eq 0 ] || die "could not apply the KafkaCluster (rc=$rc)"

  set +e
  kubectl --context docker-desktop -n "$NS" wait --for=jsonpath='{.status.reachable}'=true kafkacluster/demo --timeout=300s > "$OUT/kc-wait.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl wait --for=jsonpath={.status.reachable}=true kafkacluster/demo)"
  if [ "$rc" -ne 0 ]; then
    dump_run kafkacluster demo
    die "the KafkaCluster never became reachable. The probe Job runs \`logweir cluster-probe\` against $BOOTSTRAP_K8S."
  fi
  reachable=$(read_field kafkacluster demo '{.status.reachable}' 'status.reachable')
  [ "$reachable" = "true" ] || die "status.reachable is '$reachable'"

  # AND A SECOND CLUSTER OBJECT WHOSE `role` IS `target`, BECAUSE STEP 10(b)
  # NEEDS ONE. The restore wizard picks its target with
  # `restore-wizard.js::firstTarget`, which walks the `KafkaCluster` list for
  # `spec.role == "target"` and returns `null` when there is none -- and a null
  # target gives `renderPlanBytes` an EMPTY `bootstrap_servers`, which the
  # grammar refuses, so the page renders an error box instead of the six steps.
  # In `newTopic` mode the restored topics land on the SOURCE broker, so this
  # object names the same address; it is a second NAME for one cluster, which
  # is what the page's role model asks for. The scripted half of step 10 keeps
  # targeting `demo`, so the two halves differ only in the name the operator
  # picked on screen.
  cat > "$OUT/kafkacluster-target.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: demo-target
  namespace: $NS
spec:
  bootstrapServers: ["$BOOTSTRAP_K8S"]
  auth:
    mode: plaintext
  role: target
YAML
  set +e
  kubectl --context docker-desktop apply -f "$OUT/kafkacluster-target.yaml" > "$OUT/kc-target-apply.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl apply -f kafkacluster-target.yaml — role: target, for the wizard at step 10(b))"
  [ "$rc" -eq 0 ] || die "could not apply the target KafkaCluster (rc=$rc)"
  set +e
  kubectl --context docker-desktop -n "$NS" wait --for=jsonpath='{.status.reachable}'=true kafkacluster/demo-target --timeout=300s > "$OUT/kc-target-wait.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl wait --for=jsonpath={.status.reachable}=true kafkacluster/demo-target)"
  if [ "$rc" -ne 0 ]; then
    dump_run kafkacluster demo-target
    die "the target KafkaCluster never became reachable."
  fi

  pause "the controller probed the broker with a Job and wrote status.reachable: true."
}

# ---------------------------------------------------------------------------
# 8/12 THE BackupSchedule, AND A Backup WITH A SIGNED RECEIPT.
# ---------------------------------------------------------------------------
step_08() {
  step "8/12 records, the bucket, a BackupSchedule on */2, and the Backup it fires"

  # THE ARCHIVE'S BUCKET AND THE RECORDS TO PUT IN IT. `just e2e-down` runs
  # `down -v` and EMPTIES the MinIO volume, so the bucket is made here, inside
  # the compose network, and the prefix is swept at both ends.
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup mb --ignore-existing "local/$ARCHIVE_BUCKET" > "$OUT/mc-mb.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (mc mb --ignore-existing local/$ARCHIVE_BUCKET)"
  [ "$rc" -eq 0 ] || die "could not create the bucket (rc=$rc); see $OUT/mc-mb.log"
  sweep_archive

  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-topics -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 > "$OUT/topic-create.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kafka-topics --create $TOPIC)"
  [ "$rc" -eq 0 ] || die "could not create the topic (rc=$rc); see $OUT/topic-create.log"

  RECORDS="${RECORDS:-200}"
  # awk generates the range directly: BSD `seq` counts DOWN when first > last.
  awk -v n="$RECORDS" 'BEGIN { for (i = 1; i <= n; i++) printf "k-%06d:{\"id\":%d}\n", i, i }' > "$OUT/records.txt"
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-console-producer -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --topic "$TOPIC" --property "parse.key=true" --property "key.separator=:" < "$OUT/records.txt" > "$OUT/produce.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kafka-console-producer -> $TOPIC)"
  [ "$rc" -eq 0 ] || die "producing exited $rc; see $OUT/produce.log"

  # The producer exits 0 whether or not the broker accepted anything, so the
  # end offsets are read back OFF THE BROKER.
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-get-offsets -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --topic "$TOPIC" > "$OUT/offsets.txt" 2> "$OUT/offsets.stderr"
  rc=$?
  set -e
  echo "    rc=$rc  (kafka-get-offsets $TOPIC)"
  [ "$rc" -eq 0 ] || die "could not read end offsets (rc=$rc); see $OUT/offsets.stderr"
  ON_BROKER=$(awk -F: '{ s += $3 } END { print s+0 }' "$OUT/offsets.txt")
  [ "$ON_BROKER" -gt 0 ] || die "the broker holds 0 records on $TOPIC; the archive would be empty"
  echo "    $TOPIC holds $ON_BROKER records"

  # EVERY EPOCH INSTANT IN THIS PROJECT IS COMPUTED, NEVER TYPED (plan errata
  # E6/E7). The recovery point is five seconds after the last record — the
  # boundary is INCLUSIVE (guard G-PITR) — and strictly above the archive
  # floor, which the runner reads from the manifest (guard G-WIN).
  PIT_RFC3339=$("$PYTHON" -c 'import datetime as d
print((d.datetime.now(d.timezone.utc).replace(microsecond=0) + d.timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
  SAMPLE_START_RFC3339=$("$PYTHON" -c 'import datetime as d
print((d.datetime.now(d.timezone.utc).replace(microsecond=0) - d.timedelta(minutes=55)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
  [ -n "${PIT_RFC3339:-}" ] || die "could not compute the recovery point"
  [ -n "${SAMPLE_START_RFC3339:-}" ] || die "could not compute the sample window start"
  echo "    recovery point: $PIT_RFC3339   sample window from: $SAMPLE_START_RFC3339"

  # THE SCHEDULE. Every `.spec` field except `suspend` is sealed by the CRD's
  # own CEL rule, for every subject, cluster-admin included.
  cat > "$OUT/backupschedule.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: BackupSchedule
metadata:
  name: laptop
  namespace: $NS
spec:
  schedule: "*/2 * * * *"
  sourceRef:
    name: demo
  topics: ["$TOPIC"]
  archive:
    url: $ARCHIVE_URL
    secretRef:
      name: logweir-s3
  suspend: false
YAML
  set +e
  kubectl --context docker-desktop apply -f "$OUT/backupschedule.yaml" > "$OUT/bs-apply.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl apply -f backupschedule.yaml, schedule */2 * * * *)"
  [ "$rc" -eq 0 ] || die "could not apply the BackupSchedule (rc=$rc)"

  echo "    waiting for the schedule to fire (up to two minutes plus the run)..."
  BACKUP_NAME=""
  for _ in $(seq 1 60); do
    set +e
    kubectl --context docker-desktop -n "$NS" get backups -o name > "$OUT/backups.txt" 2>/dev/null
    rc=$?
    set -e
    BACKUP_NAME=$(sed -n 's|^backup.logweir.dev/||p' "$OUT/backups.txt" | head -1)
    [ -n "$BACKUP_NAME" ] && break
    sleep 5
  done
  echo "    rc=$rc  (kubectl get backups -o name)"
  [ -n "$BACKUP_NAME" ] || die "no Backup appeared within five minutes. The BackupSchedule reconciler creates one object per due slot, named <schedule>-<slot>."
  echo "    the schedule fired: Backup/$BACKUP_NAME"

  backup_phase=""
  for _ in $(seq 1 90); do
    set +e
    kubectl --context docker-desktop -n "$NS" get backup "$BACKUP_NAME" -o jsonpath='{.status.phase}' > "$OUT/backup-phase.txt" 2>/dev/null
    rc=$?
    set -e
    backup_phase=$(cat "$OUT/backup-phase.txt")
    case "$backup_phase" in Succeeded|Failed|Refused) break ;; esac
    sleep 10
  done
  echo "    rc=$rc  (kubectl get backup $BACKUP_NAME -o jsonpath={.status.phase})"
  echo "    phase: ${backup_phase:-<absent>}"
  if [ "$backup_phase" != "Succeeded" ]; then
    dump_run backup "$BACKUP_NAME"
    die "the Backup reached phase '${backup_phase:-<absent>}', not Succeeded."
  fi

  b_exit=$(read_field backup "$BACKUP_NAME" '{.status.exitCode}' 'status.exitCode')
  b_receipt=$(read_field backup "$BACKUP_NAME" '{.status.evidence.receiptKey}' 'status.evidence.receiptKey')

  b_result=""
  for _ in $(seq 1 24); do
    set +e
    kubectl --context docker-desktop -n "$NS" get backup "$BACKUP_NAME" -o jsonpath='{.status.evidence.verification.result}' > "$OUT/backup-verdict.txt" 2>/dev/null
    rc=$?
    set -e
    b_result=$(cat "$OUT/backup-verdict.txt")
    [ -n "$b_result" ] && [ "$b_result" != "NotAttempted" ] && break
    sleep 5
  done
  echo "    rc=$rc  (kubectl get backup $BACKUP_NAME -o jsonpath={.status.evidence.verification.result})"
  echo "    status.evidence.verification.result: ${b_result:-<absent>}"

  [ "$b_exit" = "0" ] || die "Backup exitCode is '$b_exit', not 0"
  [ -n "$b_receipt" ] || die "the Backup recorded no receiptKey"
  [ "$b_result" = "Valid" ] || die "the Backup's receipt verified as '${b_result:-<absent>}', not Valid"

  # SUSPEND THE SCHEDULE, AND NOTE WHAT THAT DEMONSTRATES. `suspend` is the
  # ONLY mutable field of `BackupSchedule.spec`: the CRD carries an
  # object-level CEL rule sealing every other field for EVERY subject,
  # cluster-admin included, and `logweir-operator` therefore gets a plain
  # `update` because RBAC has no expression language. So this one `patch` is
  # also the demonstration of that rule.
  #
  # It is here because the walk needs a STABLE newest `Backup` from this point
  # on. The restore wizard reads its backup set off the newest one
  # (`restore-wizard.js::initialState`), and a schedule that keeps firing every
  # two minutes replaces that object under the operator's feet between step 9
  # and step 10(b) -- measured: three `Backup` objects in six minutes, and the
  # page reading a different one on every reload.
  set +e
  kubectl --context docker-desktop -n "$NS" patch backupschedule laptop --type=merge -p '{"spec":{"suspend":true}}' > "$OUT/suspend.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl patch backupschedule laptop spec.suspend=true — the ONE mutable field; every other is sealed by the CRD's own CEL rule)"
  [ "$rc" -eq 0 ] || die "could not suspend the BackupSchedule (rc=$rc)"

  pause "a scheduled Backup exited 0 and its signed receipt verified Valid."
}

# ---------------------------------------------------------------------------
# 9/12 SERVE THE UI — AND PROVE IT IS SERVED, MECHANICALLY.
# ---------------------------------------------------------------------------
step_09() {
  step "9/12 kubectl proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1, and four fetches"

  set +e
  kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1 > "$OUT/proxy.log" 2>&1 &
  rc=$?
  set -e
  PROXY_PID=$!
  echo "    rc=$rc  (kubectl proxy, backgrounded; pid $PROXY_PID)"
  echo
  echo "    The UI is at $PROXY_BASE/ui/"
  echo
  echo "    WHAT THIS COSTS, said plainly: kubectl proxy forwards every API path except pod"
  echo "    exec and attach, on the SAME ORIGIN as the page, under YOUR kubeconfig. The page"
  echo "    therefore runs with your ENTIRE CLUSTER AUTHORITY, not with the four ClusterRoles"
  echo "    logweir.yaml ships -- those bind the user, and under this serving path they bind"
  echo "    nothing about the page. Run this from a cluster-admin kubeconfig and you have"
  echo "    given the page cluster-admin. No bearer token, key or credential of any kind is"
  echo "    ever placed in the page."

  # A BOUNDED FOREGROUND POLL, never a backgrounded wait.
  up=""
  for _ in $(seq 1 60); do
    set +e
    curl -sS -o /dev/null "$PROXY_BASE/ui/" > "$OUT/proxy-poll.txt" 2>&1
    rc=$?
    set -e
    [ "$rc" -eq 0 ] && up=yes && break
    sleep 1
  done
  echo "    rc=$rc  (curl $PROXY_BASE/ui/ — the readiness poll)"
  [ -n "$up" ] || die "the proxy never answered on 8001; see $OUT/proxy.log"

  # THE STATIC HALF, ASSERTED MECHANICALLY. Until this step nothing in Phase C
  # proved anything is served at `/ui/` AT ALL: renaming `ui/` and changing
  # `--www-prefix` in all three documented places passes every gate in Tasks
  # 25-27, because those check AGREEMENT and not correctness. These four
  # fetches are the correctness half.
  expect_200 "$PROXY_BASE/ui/"                        "the page itself"
  expect_200 "$PROXY_BASE/ui/app.js"                  "the router"
  expect_200 "$PROXY_BASE/ui/pages/restore-wizard.js" "the wizard the next step uses"
  # ...and the API half, SAME ORIGIN, viewer credential, no CORS, no token.
  expect_200 "$API_BASE/restores"                     "the API, same origin, viewer credential"

  pause "the UI and the Kubernetes API are both served from $PROXY_BASE."
}

# ---------------------------------------------------------------------------
# 10/12 (a) X-UIWRITE, THE SCRIPTED HALF.
# ---------------------------------------------------------------------------
step_10a() {
  step "10/12 (a) X-UIWRITE, scripted: the PAGE's own code builds the body, curl posts it"

  # THE WIZARD'S STATE, with this run's own values. The object-store inputs —
  # endpoint, region, path style, evidence bucket — are wizard STEP-1 INPUTS
  # that no custom resource records (plan erratum E24d), so this is where they
  # are confirmed against the runner.
  cat > "$OUT/plan-fields.json" <<JSON
{
  "ns": "$NS",
  "archiveUrl": "$ARCHIVE_URL",
  "archiveSecretName": "logweir-s3",
  "targetClusterName": "demo",
  "deadlineSeconds": 1800,
  "fields": {
    "name": "laptop-demo",
    "backupSetRef": "latestCompleted",
    "topics": ["$TOPIC"],
    "pointInTime": "$PIT_RFC3339",
    "source": {
      "bucket": "$ARCHIVE_BUCKET",
      "prefix": "$BACKUP_PREFIX",
      "region": "$S3_REGION",
      "endpoint": "$S3_ENDPOINT",
      "pathStyle": true,
      "allowHttp": true
    },
    "target": {
      "bootstrapServers": ["$BOOTSTRAP_K8S"],
      "mode": "newTopic",
      "topicPrefix": "drill-",
      "topicMappingPrefix": "drill-",
      "markerTopic": "logweir.scratch",
      "replicationFactor": 1,
      "teardown": "delete"
    },
    "sample": {
      "windowStart": "$SAMPLE_START_RFC3339",
      "windowEnd": "$PIT_RFC3339",
      "recordsPerPartition": 25,
      "anchor": "head"
    },
    "objectives": {
      "rtoSeconds": 3600,
      "rpoSeconds": 86400,
      "passRate": 1.0
    },
    "evidence": {
      "bucket": "$ARCHIVE_BUCKET",
      "prefix": "logweir/",
      "region": "$S3_REGION",
      "endpoint": "$S3_ENDPOINT",
      "pathStyle": true,
      "allowHttp": true
    }
  }
}
JSON

  # THE BYTES ARE THE WIZARD'S AND NOT THE SCRIPT'S. `emit-restore-body.js`
  # calls `renderPlanBytes` (interface register I19), `planHash`, `mintNames`
  # and the wizard's own `restoreBody`. A hand-written document here would be
  # created, approved and hashed correctly, and would fail at STEP 12 — which
  # is the failure mode Task 27's guard exists to catch three slots earlier.
  set +e
  node ui/tests/emit-restore-body.js --out "$OUT/" > "$OUT/emit.out" 2> "$OUT/emit.err"
  rc=$?
  set -e
  echo "    rc=$rc  (node ui/tests/emit-restore-body.js --out $OUT/)"
  cat "$OUT/emit.out"
  [ "$rc" -eq 0 ] || { cat "$OUT/emit.err"; die "the body emitter exited $rc"; }
  PLAN_HASH=$(sed -n 's/^plan-hash=//p' "$OUT/emit.out")
  RESTORE_NAME=$(sed -n 's/^restore-name=//p' "$OUT/emit.out")
  APPROVAL_NAME=$(sed -n 's/^approval-name=//p' "$OUT/emit.out")
  [ -n "$PLAN_HASH" ] || die "the emitter printed no plan-hash="
  [ -n "$RESTORE_NAME" ] || die "the emitter printed no restore-name="
  [ -n "$APPROVAL_NAME" ] || die "the emitter printed no approval-name="

  # THE CREATE. Same origin, viewer credential, no token in the page. The
  # `fieldManager` here is `logweir-cli` — which is exactly why this half does
  # not satisfy spec §10 on its own: the page's is `logweir-ui`, and 10(b) is
  # where that string is produced.
  set +e
  curl -sS -o "$OUT/xuiwrite-a.json" -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data @"$OUT/restore-body.json" "$API_BASE/restores?fieldManager=logweir-cli" > "$OUT/xuiwrite-a.status"
  rc=$?
  set -e
  status=$(cat "$OUT/xuiwrite-a.status")
  echo "    rc=$rc  HTTP $status  (POST $API_BASE/restores?fieldManager=logweir-cli)"
  [ "$rc" -eq 0 ] || die "curl exited $rc posting the Restore"
  if [ "$status" != "201" ]; then
    cat "$OUT/xuiwrite-a.json"
    die "X-UIWRITE (a): the create answered $status, not 201"
  fi
  echo "    X-UIWRITE (a): 201 Created — Restore/$RESTORE_NAME, approvalRef -> $APPROVAL_NAME (which does not exist yet)"

  set +e
  kubectl --context docker-desktop -n "$NS" get restore "$RESTORE_NAME" -o jsonpath='{.metadata.managedFields[*].manager}' > "$OUT/managedfields-a.txt" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (managedFields managers on the scripted Restore): $(cat "$OUT/managedfields-a.txt")"

  pause "X-UIWRITE half (a): a Restore created through kubectl proxy's API, 201."
}

# ---------------------------------------------------------------------------
# 10/12 (b) X-UIWRITE, THE HALF SPEC §10 ACTUALLY ASKS FOR.
# ---------------------------------------------------------------------------
step_10b() {
  step "10/12 (b) X-UIWRITE, IN THE BROWSER: the create from the wizard's final step"

  # ==========================================================================
  # A DISCLOSED WORKAROUND, AND THE DEFECT IT IS FOR
  # ==========================================================================
  # `BackupStatus::backup_id` is declared in the CRD
  # (`config/crd/backups.yaml`, `status.backupId`) and in the Rust type
  # (`crates/weirkeeper/src/crds/backup.rs`), and NOTHING WRITES IT: the
  # controller's `plan_backup_id` renders the id into the runner's plan
  # ConfigMap and the status patch never carries it. The restore wizard reads
  # it -- `restore-wizard.js::initialState` sets `fields.backupSetRef =
  # status.backupId` -- and `renderPlanBytes` refuses a document without one,
  # THROWING BEFORE ANY OF THE SIX STEPS RENDERS. So on a real cluster the page
  # is an error box, and the half of X-UIWRITE that spec §10 actually asks for
  # cannot be performed at all.
  #
  # The fix belongs to the next editor of `crates/weirkeeper/src/controllers/
  # backup.rs` (one field on the terminal status patch) and NOT to this script:
  # changing it would rebuild the controller image and move its digest, which
  # this walk is pinned against (plan erratum E19a).
  #
  # So the walk writes the field the controller owes, derived from the archive
  # the run actually wrote: `status.evidence.receiptKey` is
  # `logweir/backups/<backup_id>/<run_id>.receipt.json`, so the id is its third
  # path segment -- the same string `backupSetRef` must name for the runner to
  # resolve the set. It is a STATUS patch on this demo's own object in this
  # demo's own namespace; no shipped code changes, and both halves of X-UIWRITE
  # are about the create's `fieldManager`, which this cannot affect.
  set +e
  kubectl --context docker-desktop -n "$NS" get backups -o name > "$OUT/backups-10b.txt" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get backups -o name)"
  [ "$rc" -eq 0 ] || die "could not list the Backups in $NS (rc=$rc)"
  ui_backup=$(sed -n 's|^backup.logweir.dev/||p' "$OUT/backups-10b.txt" | tail -1)
  [ -n "$ui_backup" ] || die "no Backup in $NS for the wizard to read a backup set off"

  set +e
  kubectl --context docker-desktop -n "$NS" get backup "$ui_backup" -o jsonpath='{.status.evidence.receiptKey}' > "$OUT/receiptkey-10b.txt" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get backup $ui_backup -o jsonpath={.status.evidence.receiptKey})"
  ui_backup_id=$(awk -F/ '{ print $3 }' "$OUT/receiptkey-10b.txt")
  [ -n "$ui_backup_id" ] || die "could not derive the backup id from $ui_backup's receiptKey"
  echo "    backup id from the archive: $ui_backup_id"

  set +e
  kubectl --context docker-desktop -n "$NS" patch backup "$ui_backup" --subresource=status --type=merge -p "{\"status\":{\"backupId\":\"$ui_backup_id\"}}" > "$OUT/patch-backupid.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl patch backup $ui_backup --subresource=status: status.backupId=$ui_backup_id — the field the controller owes, see the comment above)"
  [ "$rc" -eq 0 ] || die "could not write status.backupId on $ui_backup (rc=$rc)"

  cat <<TXT
    Spec §10: "under kubectl proxy --www=, a create of a Restore FROM THE PAGE
    returns 201". curl is not the page. Do this by hand:

      1. open $PROXY_BASE/ui/#/restore
      2. walk the wizard's six steps (namespace $NS, cluster demo, the archive
         $ARCHIVE_URL, a point in time, a target prefix) -- change ANYTHING
         from the scripted run so the plan bytes differ and the minted names do
         too; the six steps end on the rendered bytes, their sha256 and the two
         names
      3. press "Create the Restore"

    NOTHING TYPES A PRIVATE KEY INTO THAT PAGE. The create needs no key; the
    approval is minted on this host at step 11 and only approval.json and
    approval.sig -- two public documents -- are ever pasted into the browser.

    Then the evidence, which only the page can produce. ui/api.js's create
    appends ?fieldManager=logweir-ui (interface register I23), so:

      kubectl --context docker-desktop -n $NS get restore <name> -o jsonpath='{.metadata.managedFields}'

    names "manager": "logweir-ui" and not "kubectl", and not the unstable
    browser-derived User-Agent an absent ?fieldManager= would have left.
TXT

  if [ -n "${LOGWEIR_DEMO_NONINTERACTIVE:-}" ]; then
    echo
    echo "    SKIPPED in this pass (LOGWEIR_DEMO_NONINTERACTIVE=1). Half (b) is performed in a"
    echo "    second, interactive pass with the proxy still up:"
    echo "        LOGWEIR_DEMO_ONLY_STEP=10b ./scripts/laptop-demo.sh"
    return 0
  fi

  pause "create the Restore in the browser now, then continue."

  name="${LOGWEIR_DEMO_RESTORE_NAME:-}"
  if [ -z "$name" ]; then
    set +e
    kubectl --context docker-desktop -n "$NS" get restores -o name > "$OUT/restores-10b.txt" 2>&1
    rc=$?
    set -e
    echo "    rc=$rc  (kubectl get restores -o name)"
    cat "$OUT/restores-10b.txt"
    printf '    the name the page created: '
    read -r name || true
  fi
  [ -n "$name" ] || die "step 10(b) needs the name of the Restore the page created (set LOGWEIR_DEMO_RESTORE_NAME)."

  set +e
  kubectl --context docker-desktop -n "$NS" get restore "$name" -o jsonpath='{.metadata.managedFields}' > "$OUT/managedfields-b.json" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get restore $name -o jsonpath={.metadata.managedFields})"
  cat "$OUT/managedfields-b.json"
  echo
  [ "$rc" -eq 0 ] || die "could not read managedFields on $name (rc=$rc)"

  # AND THE SAME BYTES, PRETTY-PRINTED, because that is the spelling the
  # transcript is checked against. `-o jsonpath` emits COMPACT json
  # (`"manager":"logweir-ui"`) and
  # `laptop_demo_lint.rs::laptop_demo_transcript_is_present` looks for
  # `"manager": "logweir-ui"` -- the form a reader sees. One document, two
  # spellings, both recorded; `json.tool` reformats and invents nothing.
  set +e
  "$PYTHON" -m json.tool "$OUT/managedfields-b.json" > "$OUT/managedfields-b.pretty.json"
  rc=$?
  set -e
  echo "    rc=$rc  (python3 -m json.tool, the same bytes as the reader sees them)"
  cat "$OUT/managedfields-b.pretty.json"
  echo
  case "$(cat "$OUT/managedfields-b.json")" in
    *logweir-ui*) echo "    X-UIWRITE (b): the manager is logweir-ui — THE WRITE CAME FROM THE PAGE." ;;
    *) die "managedFields on $name names no logweir-ui manager. Either the object was not created by the page, or ui/api.js's create lost its ?fieldManager=logweir-ui (interface register I23) — in which case the API server derived the manager from the browser's User-Agent and there is nothing a transcript can be checked against." ;;
  esac
}

# ---------------------------------------------------------------------------
# 11/12 THE APPROVAL, MINTED OUT OF BAND, ON THE HOST, WITH THE CLI.
# ---------------------------------------------------------------------------
step_11() {
  step "11/12 logweir drill approve on the HOST, then the Approval object"

  # THE APPROVER'S PRIVATE KEY NEVER LEAVES THIS MACHINE AND NEVER ENTERS A
  # BROWSER. `--subject-kind Restore` is Task 22's fifth check; the sidecar
  # path is DERIVED from `--out` (approve.rs::sig_path_for: the extension
  # replaced), so `approval.json` gives `approval.sig`.
  set +e
  logweir drill approve --spec "$OUT/plan.yaml" --key "$OUT/approver.pem" --approver laptop --ticket DEMO-1 --subject-kind Restore --out "$OUT/approval.json" > "$OUT/approve.out" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (logweir drill approve --subject-kind Restore --out $OUT/approval.json)"
  cat "$OUT/approve.out"
  [ "$rc" -eq 0 ] || die "\`logweir drill approve\` exited $rc"
  [ -f "$OUT/approval.sig" ] || die "the detached sidecar $OUT/approval.sig was not written"

  APPROVED_HASH=$(awk '/plan_hash/ { print $2 }' "$OUT/approve.out")
  echo "    plan_hash from the CLI : $APPROVED_HASH"
  echo "    plan-hash from the page: $PLAN_HASH"
  [ "$APPROVED_HASH" = "$PLAN_HASH" ] || die "the CLI signed $APPROVED_HASH and the page showed $PLAN_HASH. These are the same bytes or they are two documents with one name."

  # THE RUNNER'S COPY OF THE BUNDLE, now that the two documents exist. Step 5
  # created it with placeholders so `just check-secrets` could count five; this
  # is the replacement, and it happens BEFORE the Approval object exists, so it
  # is in place before any runner Job can mount it.
  set +e
  kubectl --context docker-desktop -n "$NS" delete secret logweir-approval-bundle > "$OUT/secret-approval-delete.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete secret logweir-approval-bundle — the placeholder)"
  set +e
  kubectl --context docker-desktop -n "$NS" create secret generic logweir-approval-bundle --from-file=approval.json="$OUT/approval.json" --from-file=approval.sig="$OUT/approval.sig" --from-file=approver.pub.pem="$OUT/approver.pub.pem" --from-file=allowed-clusters.json="$OUT/allowed-clusters.json" > "$OUT/secret-approval2.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (secret/logweir-approval-bundle, the real four keys)"
  [ "$rc" -eq 0 ] || die "could not re-create logweir-approval-bundle (rc=$rc)"

  # THE Approval OBJECT. `approvalBytes` and `sidecarBytes` are the two files
  # VERBATIM, as UTF-8 text, never base64 (interface register I18): the bytes
  # the approver's tool wrote are the bytes the controller verifies.
  "$PYTHON" - "$OUT/approval.json" "$OUT/approval.sig" "$NS" "$APPROVAL_NAME" "$RESTORE_NAME" "$APPROVED_HASH" "$OUT/approval-object.yaml" <<'PY'
import sys, textwrap
approval_path, sidecar_path, ns, name, subject, plan_hash, out = sys.argv[1:8]
approval = open(approval_path).read()
sidecar = open(sidecar_path).read()
doc = f"""apiVersion: logweir.dev/v1alpha1
kind: Approval
metadata:
  name: {name}
  namespace: {ns}
spec:
  subjectRef:
    kind: Restore
    name: {subject}
  planHash: {plan_hash}
  approvalBytes: |
{textwrap.indent(approval.rstrip(chr(10)), ' ' * 4)}
  sidecarBytes: |
{textwrap.indent(sidecar.rstrip(chr(10)), ' ' * 4)}
"""
open(out, "w").write(doc)
PY
  set +e
  kubectl --context docker-desktop create -f "$OUT/approval-object.yaml" > "$OUT/approval-apply.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl create -f approval-object.yaml — Approval/$APPROVAL_NAME over Restore/$RESTORE_NAME)"
  [ "$rc" -eq 0 ] || { cat "$OUT/approval-apply.log"; die "could not create the Approval (rc=$rc)"; }

  set +e
  kubectl --context docker-desktop -n "$NS" wait --for=jsonpath='{.status.verified}'=true "approval/$APPROVAL_NAME" --timeout=180s > "$OUT/approval-wait.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl wait --for=jsonpath={.status.verified}=true approval/$APPROVAL_NAME)"
  if [ "$rc" -ne 0 ]; then
    dump_run approval "$APPROVAL_NAME"
    die "the Approval never verified. Its signature is checked against the TrustRoster's approverKeys[], and its planHash against the sha256 of the Restore's planBytes."
  fi
  verified=$(read_field approval "$APPROVAL_NAME" '{.status.verified}' 'status.verified')
  matched=$(read_field approval "$APPROVAL_NAME" '{.status.matchedKeyId}' 'status.matchedKeyId')
  [ "$verified" = "true" ] || die "status.verified is '$verified'"

  pause "the approval was minted on this host and verified in the cluster (key $matched)."
}

# ---------------------------------------------------------------------------
# 12/12 THE SCORECARD, FIELD BY FIELD, AND BOTH READERS OVER IT.
# ---------------------------------------------------------------------------
step_12() {
  step "12/12 the Restore's terminal status, then BOTH readers over the scorecard"

  restore_phase=""
  for _ in $(seq 1 90); do
    set +e
    kubectl --context docker-desktop -n "$NS" get restore "$RESTORE_NAME" -o jsonpath='{.status.phase}' > "$OUT/restore-phase.txt" 2>/dev/null
    rc=$?
    set -e
    restore_phase=$(cat "$OUT/restore-phase.txt")
    case "$restore_phase" in Succeeded|Failed|Refused) break ;; esac
    sleep 10
  done
  echo "    rc=$rc  (kubectl get restore $RESTORE_NAME -o jsonpath={.status.phase})"
  echo "    phase: ${restore_phase:-<absent>}"
  if [ "$restore_phase" != "Succeeded" ]; then
    dump_run restore "$RESTORE_NAME"
    die "the Restore reached phase '${restore_phase:-<absent>}', not Succeeded."
  fi

  r_exit=$(read_field restore "$RESTORE_NAME" '{.status.exitCode}' 'status.exitCode')
  r_outcome=$(read_field restore "$RESTORE_NAME" '{.status.outcome}' 'status.outcome')
  r_integrity=$(read_field restore "$RESTORE_NAME" '{.status.integrity.level}' 'status.integrity.level')
  r_rto=$(read_field restore "$RESTORE_NAME" '{.status.measured.rtoSeconds}' 'status.measured.rtoSeconds')
  r_rpo=$(read_field restore "$RESTORE_NAME" '{.status.measured.rpoSeconds}' 'status.measured.rpoSeconds')
  r_topics=$(read_field restore "$RESTORE_NAME" '{.status.newTopics}' 'status.newTopics')
  r_scorecard=$(read_field restore "$RESTORE_NAME" '{.status.evidence.scorecardKey}' 'status.evidence.scorecardKey')
  r_sidecar=$(read_field restore "$RESTORE_NAME" '{.status.evidence.sidecarKey}' 'status.evidence.sidecarKey')

  r_result=""
  for _ in $(seq 1 24); do
    set +e
    kubectl --context docker-desktop -n "$NS" get restore "$RESTORE_NAME" -o jsonpath='{.status.evidence.verification.result}' > "$OUT/restore-verdict.txt" 2>/dev/null
    rc=$?
    set -e
    r_result=$(cat "$OUT/restore-verdict.txt")
    [ -n "$r_result" ] && [ "$r_result" != "NotAttempted" ] && break
    sleep 5
  done
  echo "    rc=$rc  (kubectl get restore $RESTORE_NAME -o jsonpath={.status.evidence.verification.result})"
  set +e
  kubectl --context docker-desktop -n "$NS" get restore "$RESTORE_NAME" -o jsonpath='{.status.evidence.verification}' > "$OUT/restore-verification.json" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (status.evidence.verification): $(cat "$OUT/restore-verification.json")"

  [ "$r_exit" = "0" ] || die "Restore exitCode is '$r_exit', not 0"
  [ "$r_outcome" = "pass" ] || die "Restore outcome is '$r_outcome', not pass"
  [ "$r_result" = "Valid" ] || die "the Restore's scorecard verified as '${r_result:-<absent>}', not Valid"

  # BOTH READERS, over the scorecard the runner signed and the controller read.
  # The two documents come out of the evidence bucket with `mc`, inside the
  # compose network, because that is where the object store is.
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup cat "local/$ARCHIVE_BUCKET/$r_scorecard" > "$OUT/scorecard.json" 2> "$OUT/scorecard.err"
  rc=$?
  set -e
  echo "    rc=$rc  (mc cat $ARCHIVE_BUCKET/$r_scorecard)"
  [ "$rc" -eq 0 ] || { cat "$OUT/scorecard.err"; die "could not fetch the scorecard"; }
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup cat "local/$ARCHIVE_BUCKET/$r_sidecar" > "$OUT/scorecard.sig" 2> "$OUT/sidecar.err"
  rc=$?
  set -e
  echo "    rc=$rc  (mc cat $ARCHIVE_BUCKET/$r_sidecar)"
  [ "$rc" -eq 0 ] || { cat "$OUT/sidecar.err"; die "could not fetch the scorecard's sidecar"; }

  set +e
  logweir drill verify --scorecard "$OUT/scorecard.json" --signature "$OUT/scorecard.sig" --public-key "$OUT/signing.pub.pem" --payload-type scorecard > "$OUT/verify-rust.out" 2>&1
  rust_rc=$?
  set -e
  echo "    rc=$rust_rc  (logweir drill verify --payload-type scorecard)"
  cat "$OUT/verify-rust.out"

  set +e
  "$PYTHON" docs/verify_scorecard.py --payload-type scorecard "$OUT/scorecard.json" "$OUT/scorecard.sig" "$OUT/signing.pub.pem" > "$OUT/verify-python.out" 2>&1
  py_rc=$?
  set -e
  echo "    rc=$py_rc  (python3 docs/verify_scorecard.py --payload-type scorecard)"
  cat "$OUT/verify-python.out"

  [ "$rust_rc" -eq 0 ] || die "the Rust reader refused the scorecard (rc=$rust_rc)"
  [ "$py_rc" -eq 0 ] || die "the Python reader refused the scorecard (rc=$py_rc)"

  echo
  echo "==> PHASE C EXIT CRITERION MET"
  echo "    Backup  $BACKUP_NAME:  exitCode=$b_exit  verification=$b_result"
  echo "                           receiptKey=$b_receipt"
  echo "    Restore $RESTORE_NAME: phase=$restore_phase  exitCode=$r_exit  outcome=$r_outcome"
  echo "                           integrity=$r_integrity  rtoSeconds=$r_rto  rpoSeconds=$r_rpo"
  echo "                           newTopics=$r_topics"
  echo "                           scorecardKey=$r_scorecard"
  echo "                           sidecarKey=$r_sidecar"
  echo "                           verification=$r_result"
  echo "    Both readers agreed, each exit code read directly: logweir drill verify -> $rust_rc,"
  echo "    python3 docs/verify_scorecard.py -> $py_rc."
  echo "    X-UIWRITE (a): 201 from the create through kubectl proxy."
  echo "    X-UIWRITE (b): the in-browser create, recorded in the second pass."
  echo "    NO PRIVATE KEY WAS EVER TYPED INTO THE BROWSER."
}

# ---------------------------------------------------------------------------
# THE RUN.
# ---------------------------------------------------------------------------
case "$ONLY_STEP" in
  10b)
    step_01
    step_10b
    exit 0
    ;;
  teardown)
    mkdir -p "$OUT"
    teardown
    exit 0
    ;;
  "")
    ;;
  *)
    die "LOGWEIR_DEMO_ONLY_STEP=$ONLY_STEP is not one of: 10b, teardown"
    ;;
esac

step_01
trap on_exit EXIT
step_02
step_03
step_04
step_05
step_06
step_07
step_08
step_09
step_10a
step_10b
step_11
step_12
