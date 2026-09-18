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

if [ "${1:-}" = "--check" ]; then
  check_enforcement_image
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
render > "$OUT"
echo "render-install: wrote $OUT ($(grep -c '^kind:' "$OUT") documents)."
