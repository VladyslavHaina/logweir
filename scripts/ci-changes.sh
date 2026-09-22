#!/usr/bin/env bash
# Decide whether a CI run touches anything the integration job or the images
# consume. Prints `code=true` or `code=false` (append it to "$GITHUB_OUTPUT").
#
#   scripts/ci-changes.sh <base-sha> <head-sha>
#
# `code=false` only when EVERY changed path is under `docs/`. Documentation is
# read by the `check` job's contract tests (docs/api.md, docs/kubernetes.md,
# docs/install.md, the doc footer and link lints), so `check` always runs; but
# no binary embeds a document and no image copies one out of its build stage,
# so a docs-only change cannot alter what `e2e` measures or what `publish`
# ships. `docs/to-do/**` never starts a run at all (`paths-ignore` in ci.yml).
#
# Anything this script cannot classify runs everything: no base (a new branch,
# a tag push, workflow_dispatch, a release's workflow_call), an all-zero base,
# or a base the checkout does not contain.
set -euo pipefail

base="${1:-}"
head="${2:-HEAD}"

if [[ -z "$base" || "$base" =~ ^0+$ ]] || ! git cat-file -e "${base}^{commit}" 2>/dev/null; then
  echo "code=true"
  exit 0
fi

changed="$(git diff --name-only "$base" "$head")"
if grep -qv '^docs/' <<< "$changed"; then
  echo "code=true"
else
  echo "code=false"
fi
