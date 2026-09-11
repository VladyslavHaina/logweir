#!/usr/bin/env bash
# `just laptop-demo` — **Phase C's exit criterion**, and install gate
# **X-UIWRITE**, scripted. Task 28; reduced to a driver by Task 31.
#
# THE TWELVE STEPS ARE IN `scripts/demo-steps.sh` AND THEY ARE THERE ONCE.
# Task 31 added a second walk — the same twelve steps in CI, on a `kind`
# cluster the workflow creates (spec §16 clause 2) — and a copy of the steps
# would have been two walks drifting apart while the checklist claimed CI ran
# the one the laptop transcript recorded. So the steps, the helpers and the
# teardown moved to `demo-steps.sh` verbatim, this file sets the cluster and
# sources them, and `scripts/kind-demo.sh` does the same after three pre-steps
# of its own.
#
# Everything this script used to say about what the walk IS — X-UIWRITE's two
# halves and why `curl` is not the page, the private key that never reaches the
# browser, the context refusal, STANDING RULE 20's shape, what is author-only
# here and the compose stack precondition — is in `scripts/demo-steps.sh`'s
# header and in `e2e/k8s/laptop-demo.md`, the transcript of the run that proved
# it. The environment knobs (`LOGWEIR_DEMO_NONINTERACTIVE`,
# `LOGWEIR_DEMO_ONLY_STEP`, `LOGWEIR_DEMO_RESTORE_NAME`, `LOGWEIR_DEMO_KEEP`,
# `LOGWEIR_PYTHON`, `RECORDS`) are unchanged and are documented there.
#
# `LOGWEIR_KUBE_CONTEXT` is set HERE and not defaulted away in `demo-steps.sh`:
# STANDING RULE 12 is satisfied by every `kubectl` line naming a context that
# is always passed, and by each driver saying out loud, on its first line of
# output, which cluster this run is about.
set -euo pipefail
cd "$(dirname "$0")/.."
export LOGWEIR_KUBE_CONTEXT=docker-desktop
echo "laptop-demo: kubectl context $LOGWEIR_KUBE_CONTEXT (STANDING RULE 12)"
. scripts/demo-steps.sh
demo_run
