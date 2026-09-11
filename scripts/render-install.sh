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
# `logweir.yaml` can carry, and Global Constraint 37's `blocked: no remote` line
# has to be part of the renderer rather than part of the input.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT="logweir.yaml"
KUBECTL="${KUBECTL:-kubectl}"

# The header, verbatim. `blocked: no remote` is Global Constraint 37's literal
# and `crates/logweir/tests/manifest_lint.rs`'s
# `the_install_file_records_blocked_no_remote` asserts it is in this file.
# Task 23 added `the_install_file_still_records_blocked_no_remote` beside it,
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
# Namespace, the six CustomResourceDefinitions, the RBAC and the controller
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
# digest rows: blocked: no remote — the images below are referenced by digest (Global Constraint 7, Task 23) and the digest is a LOCALLY BUILT one until release.yml has run against a git remote; a locally built or locally loaded image is author-only and never satisfies spec §16 clause 1
#
# Consequence, stated plainly: applying this file on a cluster with no access to
# `ghcr.io/logweir/weirkeeper` leaves the Deployment's pod in `ImagePullBackOff`.
# X-APPLY proves `kubectl apply` exits 0; it does not start a pod. For an
# author-only local run, `kubectl --context docker-desktop apply --server-side
# -k config/overlays/local-images` rewrites the image to a locally loaded tag.
#
# The digest above was MEASURED, not fabricated, and what it does and does not
# buy is measured too (X-DIGEST, docs/kubernetes.md §14): a pod referencing it
# with `imagePullPolicy: Never` starts only once the image has been tagged into
# that repository name on the node, and the digest a locally built image reports
# changes on every build. That is why this row reads `blocked: no remote` and
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

if [ "${1:-}" = "--check" ]; then
  # `mktemp` and not a fixed path: two agents running this at once must not
  # write the same temporary file.
  tmp="$(mktemp -t logweir-install-check.XXXXXX)"
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

render > "$OUT"
echo "render-install: wrote $OUT ($(grep -c '^kind:' "$OUT") documents)."
