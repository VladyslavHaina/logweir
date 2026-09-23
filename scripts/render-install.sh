#!/usr/bin/env bash
# Render `logweir.yaml` from `config/` — and the ONLY thing that does.
#
# `logweir.yaml` is the one file a stranger applies (spec §1, §16 clause 1), it
# is checked in, and it is lint-gated exactly as the scorecard schema and the
# six CRDs are. A checked-in generated file is only worth having if a hand edit
# is a red build, so:
#
#   ./scripts/render-install.sh            writes logweir.yaml
#   ./scripts/render-install.sh --check    renders to a temp file and diff -u's
#
# `just install-yaml` is the first; `crates/logweir/tests/manifest_lint.rs`'s
# `install_yaml_has_no_drift` is not — see below.
#
# WHY `kubectl kustomize` AND NOT `kustomize`. It is built into the client
# (v1.35.0 carries Kustomize v5.7.1), so this needs no tool the install docs do
# not already require and reaches no network: Global Constraint 17 clean, and a
# gate that had to `go install` something would be an e2e gate.
#
# WHY THE HEADER IS WRITTEN HERE AND NOT IN A SOURCE FILE. `kubectl kustomize`
# strips comments: every `#` line in `config/**` is gone from its output
# (measured — `config/manager/networkpolicy.yaml`'s `[UNVERIFIED]` block does
# not survive the render). So the header this script prints is the only comment
# `logweir.yaml` can carry, and Global Constraint 37's `blocked: images not published` line
# has to be part of the renderer rather than part of the input.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT="logweir.yaml"
KUBECTL="${KUBECTL:-kubectl}"

# The header, verbatim. `blocked: images not published` is Global Constraint 37's literal
# and `crates/logweir/tests/manifest_lint.rs`'s
# `the_install_file_records_blocked_images_not_published` asserts it is in this file.
# Task 23 added `the_install_file_still_records_blocked_images_not_published` beside it,
# which asserts the literal SURVIVED the digest pin and that the qualifying
# sentence ("author-only ... never satisfies spec §16 clause 1") is still there:
# deleting that clause while leaving the four words behind is how a local
# registry run comes to be recorded as a publication.
header() {
  cat <<'HEADER'
# GENERATED FILE — do not edit by hand.
#
# Rendered by `./scripts/render-install.sh` (`just install-yaml`) from
# `config/`, which is the composable source. `./scripts/render-install.sh
# --check` re-renders into a temporary file and `diff -u`s this one against it,
# so a hand edit is a red build rather than a silent divergence.
#
# INSTALL:
#
#     kubectl --context docker-desktop apply --server-side -f logweir.yaml
#
# It is safe to run twice; that is X-APPLY, spec §16 clause 1. It contains the
# Namespace, the fourteen CustomResourceDefinitions, the RBAC and the controller
# Deployment, and NO custom resource — so the CRD-not-yet-established ordering
# failure cannot occur. Samples are separate files, applied second:
# `config/samples/`. Minimum Kubernetes: 1.29.
#
# UNINSTALL, and what it leaves behind:
#
#     kubectl --context docker-desktop delete -f logweir.yaml
#
# removes the control plane and DELETES NOTHING ELSE — scratch topics, archive
# objects and evidence objects all survive it, by design. `docs/kubernetes.md`
# §13 carries the exact command to remove each.
#
# digest rows: blocked: images not published — the images below are referenced by digest (Global Constraint 7, Task 23) and the digest is a LOCALLY BUILT one until release.yml has run on a pushed tag (no tag has been pushed, so it never has); a locally built or locally loaded image is author-only and never satisfies spec §16 clause 1
#
# Consequence, stated plainly: applying this file on a cluster with no access to
# `docker.io/vladyslavhaina/weirkeeper` leaves the Deployment's pod in `ImagePullBackOff`.
# X-APPLY proves `kubectl apply` exits 0; it does not start a pod. For an
# author-only local run, `kubectl --context docker-desktop apply --server-side
# -k config/overlays/local-images` rewrites the image to a locally loaded tag.
#
# The digest above was MEASURED, not fabricated, and what it does and does not
# buy is measured too (X-DIGEST, docs/kubernetes.md §14): a pod referencing it
# with `imagePullPolicy: Never` starts only once the image has been tagged into
# that repository name on the node, and the digest a locally built image reports
# changes on every build. That is why this row reads `blocked: images not published` and
# not `pinned`.
#
# [UNVERIFIED — docker-desktop runs no CNI that enforces NetworkPolicy, so a
# deny is never observed here; only the kind+Calico probe would make this claim
# real, and it is backlogged] — applies to the NetworkPolicy below. The same
# mark is carried in `config/manager/networkpolicy.yaml` with the sentence that
# would verify it.
HEADER
}

render() {
  header
  "$KUBECTL" kustomize config
}

# THE ENFORCEMENT JOB'S IMAGE CARRIES THE ENFORCEMENT BINARY — defect
# RET-NOIMAGE, found live on docker-desktop 2026-09-18.
#
# WHAT WENT WRONG. `logweir.yaml` installs a controller that, for a
# `RetentionPolicy` in `mode: Enforce`, creates a Job whose image is the
# controller's runner image and whose command is `logweir-retention`. The
# runner image is built by `Dockerfile`, and `Dockerfile` built `-p logweir`
# only — so the file this script renders installed a control plane that asks
# the kubelet for an executable no image this repository produces contains.
# The failure is `exec: "logweir-retention": executable file not found in
# $PATH`, exitCode 127, and NOTHING in the rendered YAML shows it: the
# Deployment is healthy, the image reference is valid, and the defect appears
# only the first time an administrator approves a plan.
#
# WHY THE CHECK LIVES HERE. This script is the one thing that produces the
# install file, so it is the one place that can refuse to produce an install
# file whose enforcement path cannot start. The three tokens below are the
# whole of the chain, and each is read from the file that owns it:
#
#   1. the Job's command IS `logweir-retention`     — retention_policy.rs
#   2. the Job's image IS the runner image          — retention_policy.rs
#   3. the runner image BUILDS AND SHIPS that binary — Dockerfile
#
# Break any link and this refuses, naming the link. It is a tripwire on
# spellings and says so: it reads source text and cannot prove the built image
# runs. `.github/workflows/images.yml` runs the binary out of the built image,
# which is the half a grep cannot do.
#
# COMMENT LINES ARE STRIPPED FIRST, in both files, because both explain this
# defect at length and a grep over the prose would find the explanation and
# call it the proof. `sed` deletes whole-line comments only and always exits 0,
# so no exit code is masked and nothing is read through a pipe (STANDING
# RULE 20).
check_enforcement_image() {
  retention_src="crates/weirkeeper/src/controllers/retention_policy.rs"
  controller="$(sed '/^[[:space:]]*\/\//d' "$retention_src")"
  dockerfile="$(sed '/^[[:space:]]*#/d' Dockerfile)"

  case "$controller" in
    *'RETENTION_BINARY: &str = "logweir-retention"'*) ;;
    *)
      echo "render-install: $retention_src no longer names \`logweir-retention\` as the" >&2
      echo "  enforcement Job's binary. If the binary was renamed, rename it in Dockerfile" >&2
      echo "  and in .github/workflows/images.yml too — this check is the link between them." >&2
      exit 1
      ;;
  esac

  case "$controller" in
    *'image: self.ctx.runner_image.image'*) ;;
    *)
      echo "render-install: $retention_src no longer takes the enforcement Job's image from" >&2
      echo "  \`self.ctx.runner_image\`. The check below proves the RUNNER image carries" >&2
      echo "  \`logweir-retention\`; if enforcement now names a different image, this script" >&2
      echo "  must check THAT image's recipe instead, or logweir.yaml goes back to" >&2
      echo "  installing an enforcement path that exits 127." >&2
      exit 1
      ;;
  esac

  case "$dockerfile" in
    *'-p logweir-retention'*) ;;
    *)
      echo "render-install: Dockerfile does not build \`-p logweir-retention\`." >&2
      echo "  The controller this file installs creates enforcement Jobs that run" >&2
      echo "  \`logweir-retention\` from the image Dockerfile builds. Without that package" >&2
      echo "  the Job dies at the kubelet with exitCode 127 (\`executable file not found" >&2
      echo "  in \$PATH\`) — observed live, defect RET-NOIMAGE. Refusing to render an" >&2
      echo "  install file whose enforcement path cannot start." >&2
      exit 1
      ;;
  esac

  case "$dockerfile" in
    *'/usr/local/bin/logweir-retention'*) ;;
    *)
      echo "render-install: Dockerfile builds \`logweir-retention\` but COPYs it nowhere on" >&2
      echo "  PATH. The binary must land at /usr/local/bin/logweir-retention: that is the" >&2
      echo "  bare name the controller sets as the container's command, and \$PATH is how" >&2
      echo "  the kubelet resolves it. Refusing to render." >&2
      exit 1
      ;;
  esac
}

# ---------------------------------------------------------------------------
# THE CONSOLE PRINCIPAL CAN DO WHAT THE CONSOLE DOES — `auth can-i`, offline
# ---------------------------------------------------------------------------
# WHAT THIS CLOSES, AND WHY IT IS IN THIS SCRIPT. `logweir.yaml` and the chart
# install one control plane between them: an administrator applies this file
# and then, if they run the console, turns `api.enabled` on. The console's
# grants are part of THIS installation's RBAC even though they are not in this
# file — and until now nothing asked, anywhere, the one question an operator
# would ask on a live cluster:
#
#   kubectl auth can-i <verb> <resource> \
#     --as system:serviceaccount:<namespace>:<release>-api
#
# So it is asked here, offline, against the checked-in render, in BOTH
# directions — which is what makes it an audit rather than a spot check:
#
#   1. every (verb, resource) the sealed adapter spends is granted. A `no` here
#      is a route that 403s in production while every route-table test in
#      `logweir-api` stays green. That exact defect shipped once already, on the
#      controller: see `config/rbac/role.yaml`'s header, "EVERY CALL HAS A
#      GRANT, WHICH COST A P0".
#   2. every (verb, resource) granted is one the adapter spends. A `yes` to a
#      question no route asks is a capability nobody audits (critique B M18),
#      on the one service in this product that holds `create` on Secrets.
#
# `crates/logweir/tests/chart_lint.rs` holds the same render to the same table
# from Rust, and derives the read half from the adapter's own seals. This is the
# half that needs no cargo and that refuses BEFORE the install file is written.
#
# EVERY EXIT STATUS IS READ DIRECTLY (STANDING RULE 20). `awk`, `sort` and
# `comm` write to files; nothing load-bearing is tested through a pipeline.
console_grants() {
  awk '
    function inline(l,   t, c, i) {
      t = l; sub(/^[^[]*\[/, "", t); sub(/\].*$/, "", t); gsub(/[",]/, " ", t)
      c = split(t, parts, " "); n = 0
      for (i = 1; i <= c; i++) if (parts[i] != "") res[++n] = parts[i]
    }
    /^kind: ClusterRole$/                 { isrole = 1; inrole = 0; next }
    /^kind: /                             { isrole = 0; inrole = 0; next }
    /^  name: logweir-api$/               { if (isrole) inrole = 1; next }
    /^  name: logweir-api-trustpolicies$/ { if (isrole) inrole = 1; next }
    /^  name: logweir-api-trustroster$/   { if (isrole) inrole = 1; next }
    inrole && /^    resources: \[/        { inline($0); collecting = 0; next }
    inrole && /^    resources:$/          { n = 0; collecting = 1; next }
    inrole && collecting && /^      - /   { sub(/^      - /, ""); res[++n] = $0; next }
    inrole && /^    verbs: \[/ {
      collecting = 0
      line = $0
      sub(/^[^[]*\[/, "", line); sub(/\].*$/, "", line); gsub(/[",]/, " ", line)
      vc = split(line, verbs, " ")
      for (i = 1; i <= vc; i++)
        if (verbs[i] != "")
          for (j = 1; j <= n; j++) print verbs[i] " " res[j]
      next
    }
  ' "$1"
}

check_console_grants() {
  render_file="charts/logweir/rendered/demo.yaml"
  adapter="crates/logweir-api/src/kube.rs"
  dir="$(mktemp -d "${TMPDIR:-/tmp}/logweir-console-cani.XXXXXX")"

  # THE ANSWER SHEET: one line per question, and the whole of what may be
  # answered `yes`. The eight kinds sealed into `ProductResource`, all eight
  # with a POST route (`approvals` since PLAT-19.2: the console creates the
  # Approval its ordinary confirmation IS, the governed confirmation object and
  # the approver's countersigned Approval on `POST .../restores/{name}/approval`),
  # the two named merge patches plus the two `CancellableCheck`
  # kinds, D3 W11's four namespaced reads and its ONE write, the cluster-scoped
  # `TrustPolicy` read, the ONE roster read (`get trustrosters`, held to
  # `resourceNames: ["default"]` by the arm below — PREFLIGHT-TRUSTROSTER-STALE),
  # and the two core objects with one verb each.
  printf '%s\n' \
    'get approvals'          'list approvals'          'create approvals' \
    'get backupdestinations' 'list backupdestinations' 'create backupdestinations' 'patch backupdestinations' \
    'get backups'            'list backups'            'create backups' \
    'get backupschedules'    'list backupschedules'    'create backupschedules'    'patch backupschedules' \
    'get kafkaclusters'      'list kafkaclusters'      'create kafkaclusters' \
    'get preflights'         'list preflights'         'create preflights'         'patch preflights' \
    'get restores'           'list restores'           'create restores' \
    'get topicdiscoveries'   'list topicdiscoveries'   'create topicdiscoveries'   'patch topicdiscoveries' \
    'get protectionpolicies' 'list protectionpolicies' \
    'get recoverycatalogs'   'list recoverycatalogs'   'create recoverycatalogs' \
    'get rehearsalschedules' 'list rehearsalschedules' \
    'get retentionpolicies'  'list retentionpolicies' \
    'get trustpolicies'      'list trustpolicies' \
    'get trustrosters' \
    'get configmaps' \
    'create secrets' \
    > "$dir/expected.raw"
  sort "$dir/expected.raw" > "$dir/expected"
  console_grants "$render_file" > "$dir/granted.raw"
  sort "$dir/granted.raw" > "$dir/granted"

  if [ ! -s "$dir/granted" ]; then
    rm -rf "$dir"
    echo "render-install: no console ClusterRole rule was found in $render_file." >&2
    echo "  Either \`api.enabled\` is no longer on in charts/logweir/examples/demo.values.yaml," >&2
    echo "  or the render is stale (\`bash scripts/check-chart.sh --write\`). A gate that reads" >&2
    echo "  nothing answers \`yes\` to every question." >&2
    exit 1
  fi

  comm -23 "$dir/expected" "$dir/granted" > "$dir/missing"
  if [ -s "$dir/missing" ]; then
    echo "render-install: the console principal CANNOT do what the console does." >&2
    echo "  \`kubectl auth can-i\` would answer \`no\` for each pair below, and each one is a" >&2
    echo "  route that 403s in production while every route-table test stays green:" >&2
    sed 's/^/    /' "$dir/missing" >&2
    echo "  Widen charts/logweir/templates/ui/api-rbac.yaml deliberately, say why in its" >&2
    echo "  header, and run \`bash scripts/check-chart.sh --write\`." >&2
    rm -rf "$dir"
    exit 1
  fi

  comm -13 "$dir/expected" "$dir/granted" > "$dir/extra"
  if [ -s "$dir/extra" ]; then
    echo "render-install: the console principal can do MORE than the console does." >&2
    echo "  Each pair below is a \`yes\` to a question no route asks — a capability nobody" >&2
    echo "  audits, on the one service that holds \`create\` on Secrets:" >&2
    sed 's/^/    /' "$dir/extra" >&2
    echo "  Narrow charts/logweir/templates/ui/api-rbac.yaml, or add the pair to the answer" >&2
    echo "  sheet in this function TOGETHER WITH the route that spends it." >&2
    rm -rf "$dir"
    exit 1
  fi

  # THE ROSTER GRANT IS ONE NAME. The pair sheet above cannot see
  # `resourceNames`, so a `get trustrosters` with the name list dropped — every
  # roster in the cluster — would pass it. This arm reads the one rule and
  # requires exactly `resourceNames: ["default"]`, the one roster
  # `KubeAdapter::get_trust_roster` reads.
  awk '
    /^kind: ClusterRole$/                { isrole = 1; inrole = 0; next }
    /^kind: /                            { isrole = 0; inrole = 0; next }
    /^  name: logweir-api-trustroster$/  { if (isrole) inrole = 1; next }
    inrole && /^    resourceNames:/      { print }
  ' "$render_file" > "$dir/roster-names"
  printf '%s\n' '    resourceNames: ["default"]' > "$dir/roster-names.want"
  if ! cmp -s "$dir/roster-names" "$dir/roster-names.want"; then
    echo "render-install: the console's roster grant is not \`resourceNames: [\"default\"]\`." >&2
    echo "  ClusterRole logweir-api-trustroster in $render_file carries:" >&2
    sed 's/^/    /' "$dir/roster-names" >&2
    echo "  The adapter reads TrustRoster/default and nothing else; a wider name list is a" >&2
    echo "  read of every roster's key material that no route makes." >&2
    rm -rf "$dir"
    exit 1
  fi

  # THE FOUR VERBS ARE STILL THE ADAPTER'S FOUR. The sheet above is a list this
  # file owns; these two arms read the ADAPTER, so a fifth kube verb reaching it
  # cannot be authorised by editing one list.
  adapter_src="$(sed '/^[[:space:]]*\/\//d' "$adapter")"
  case "$adapter_src" in
    *'pub trait ProductResource'*) ;;
    *)
      rm -rf "$dir"
      echo "render-install: $adapter no longer seals its custom resources behind" >&2
      echo "  \`ProductResource\`. The console's ClusterRole is written from that seal; without" >&2
      echo "  it there is nothing to hold the grant to, and the answer sheet in this function" >&2
      echo "  becomes a list nobody derives." >&2
      exit 1
      ;;
  esac
  for verb in delete deletecollection watch update; do
    case "$(cat "$dir/granted")" in
      *"$verb "*)
        rm -rf "$dir"
        echo "render-install: the console principal is granted \`$verb\`." >&2
        echo "  The sealed adapter spends four kube verbs — list, get, create, patch — and has" >&2
        echo "  no method that could issue this one. \`watch\` would additionally be a" >&2
        echo "  long-lived connection per browser tab, and D3 §10's event stream is server-sent" >&2
        echo "  events over this service's own reads. Remove the verb." >&2
        exit 1
        ;;
    esac
  done
  rm -rf "$dir"
  echo "render-install: the console principal's grants are the sealed adapter's, both ways."
}

if [ "${1:-}" = "--check" ]; then
  check_enforcement_image
  check_console_grants
  # `mktemp` and not a fixed path: two agents running this at once must not
  # write the same temporary file.
  tmp="$(mktemp "${TMPDIR:-/tmp}/logweir-install-check.XXXXXX")"
  # shellcheck disable=SC2064 # expand $tmp now, on purpose.
  trap "rm -f '$tmp'" EXIT
  render > "$tmp"
  # THE EXIT STATUS IS READ FROM `diff` DIRECTLY, NOT THROUGH A PIPE (STANDING
  # RULE 20). `diff -u` prints the drift and exits 1; that 1 is this script's 1.
  if diff -u "$OUT" "$tmp"; then
    echo "render-install: $OUT is what config/ renders to (no drift)."
    exit 0
  fi
  cat >&2 <<MSG

render-install: $OUT DIFFERS from what config/ renders to.

The diff above is `$OUT` (-) against a fresh render (+). Either config/ changed
and $OUT was not regenerated, or $OUT was edited by hand. Both are fixed the
same way:

    just install-yaml

MSG
  exit 1
fi

if [ "$#" -ne 0 ]; then
  echo "usage: scripts/render-install.sh [--check]" >&2
  exit 2
fi

check_enforcement_image
check_console_grants
render > "$OUT"
echo "render-install: wrote $OUT ($(grep -c '^kind:' "$OUT") documents)."
