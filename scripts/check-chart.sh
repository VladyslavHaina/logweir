#!/usr/bin/env bash
# Validate chart rendering, image references, value types and the install's grants.
# Use --write to refresh snapshots; default checks do not change tracked files.
set -uo pipefail
# --write explicitly refreshes committed render snapshots. Normal checks never edit files.
write=false
case "${1:-}" in
  --write) write=true ;;
  "") ;;
  *) echo "usage: check-chart.sh [--write]" >&2; exit 2 ;;
esac
cd "$(dirname "$0")/.." || exit 2

CHART=charts/logweir
RENDERED="$CHART/rendered"
EXAMPLES="$CHART/examples"
RELEASE=logweir
NAMESPACE=logweir-system
HELM_MIN_MAJOR=4
# The one rendered file the digest arm exempts, by name, for the reason in the
# header. A literal, never a pattern.
DIGEST_EXEMPT="author-only.yaml"
# THE TAG THE TWO LOGWEIR IMAGES CARRY IN THIS CHART'S DEFAULTS — the owner's
# decision of 2026-09-12. Arms 7 and 5 both read it; the REPOSITORIES are
# derived from the tree (arm 7) and are never spelt in this file.
LOGWEIR_TAG="latest"

fail=0
tmp="$(mktemp -d "${TMPDIR:-/tmp}/logweir-chart-check.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

echo "== the chart gate: $CHART held to the tree =="

# ---------------------------------------------------------------- 0. helm
if ! command -v helm >/dev/null 2>&1; then
  echo "FAIL: \`helm\` is not on \$PATH. This gate needs Helm >= ${HELM_MIN_MAJOR}.0.0 (the" >&2
  echo "      rendered files were produced by v4.0.1); install it and re-run \`just chart-check\`." >&2
  exit 1
fi
helm_version="$(helm version --template '{{.Version}}' 2>/dev/null)"
helm_major="${helm_version#v}"
helm_major="${helm_major%%.*}"
case "$helm_major" in
  ''|*[!0-9]*) echo "FAIL: could not read helm's version (\`helm version\` printed '${helm_version}')" >&2; exit 1 ;;
esac
if [ "$helm_major" -lt "$HELM_MIN_MAJOR" ]; then
  echo "FAIL: helm ${helm_version} is older than the ${HELM_MIN_MAJOR}.0.0 this gate wants. The rendered" >&2
  echo "      files under $RENDERED were produced by v4.0.1, and a different major may" >&2
  echo "      serialise a scalar differently — a drift you could not act on." >&2
  exit 1
fi
echo "   helm ${helm_version}"

# ---------------------------------------------------------------- 2. the UI
# The page ships inside the console image (`Dockerfile.console`), whose served
# bytes `scripts/check-image-api.sh` hashes against `console/dist`. What a chart gate can
# say about the page is that the chart carries NO copy of it: a directory that
# came back would be a second source of the page that nothing hashes.
echo "== 2. the chart carries NO copy of the page (it is the console image's) =="
if [ -e "$CHART/ui" ]; then
  echo "FAIL: $CHART/ui exists. The page ships inside the console image" >&2
  echo "      (Dockerfile.console), asserted by scripts/check-image-api.sh against the" >&2
  echo "      bytes a browser receives. A copy under the chart is a second source of the" >&2
  echo "      page that nothing hashes." >&2
  fail=1
else
  echo "   ok: no $CHART/ui — the page is delivered by api.console.image, not by a ConfigMap"
fi

# ---------------------------------------------------------------- 7. the pins
# Early, because a wrong value makes every rendered file wrong too — and
# because arm 5 below uses the two repositories this arm derives.
echo "== 7. values.yaml names the tree's own repositories at $LOGWEIR_TAG =="
values_controller="$(sed -n 's/^controllerImage:[[:space:]]*//p' "$CHART/values.yaml")"
values_runner="$(sed -n 's/^runnerImage:[[:space:]]*//p' "$CHART/values.yaml")"
values_bootstrap="$(sed -n 's/^[[:space:]]*bootstrapImage:[[:space:]]*"\{0,1\}\([^"[:space:]]*\)"\{0,1\}.*$/\1/p' "$CHART/values.yaml")"
# `api.console.image` IS NESTED (four spaces) and is read INSIDE its block
# rather than by indentation alone, so moving a block cannot silently repoint
# the expression at another block's `image:`.
values_console="$(awk '
  /^api:/     { inapi = 1; next }
  /^[a-zA-Z]/ { inapi = 0 }
  inapi && /^    image:[[:space:]]/ {
    sub(/^    image:[[:space:]]*/, ""); sub(/[[:space:]].*$/, ""); print; exit
  }
' "$CHART/values.yaml")"
# THE TREE'S RUNNER REFERENCE, WHOLE — still a digest under Global Constraint 7.
tree_runner="$(grep -A1 '^pub const RUNNER_IMAGE: &str =' crates/weirkeeper/src/job.rs | sed -n 's/^[[:space:]]*"\(.*\)";$/\1/p')"
# THE REPOSITORY HALVES, DERIVED. Never spelt in this script: a namespace change
# in the tree must propagate to the chart without editing this gate. The
# controller has no digest pin in the tree since the `config/` tree went
# (PROD-17.1 C6), so its namespace is the runner pin's, like the UI's below.
runner_repo="${tree_runner%@sha256:*}"
CONTROLLER_IMAGE_NAME="weirkeeper"
controller_repo="${runner_repo%/*}/$CONTROLLER_IMAGE_NAME"
if [ -z "$runner_repo" ] || [ "$runner_repo" = "$tree_runner" ]; then
  echo "FAIL: could not derive the tree's repositories from the runner digest '$tree_runner'" >&2
  echo "      crates/weirkeeper/src/job.rs must pin <repository>@sha256:<64 hex>" >&2
  fail=1
fi
bootstrap_test_image="$runner_repo@sha256:0000000000000000000000000000000000000000000000000000000000000000"
# A PINNED DEFAULT IS RENDERED AS SHIPPED. Only a pre-publication checkout (an
# empty value) substitutes the test-only digest, so the committed snapshots
# carry the exact bootstrap reference a default install pulls.
#
# EVERY RENDER HERE IS A PREVIEW (PROD-14.1). `helm template` cannot ask a
# cluster, and the chart refuses such a render unless `identity.renderedOffline`
# says what it is; `false` renders what `helm install` creates, which is what
# the snapshots and the arms below hold. Arm 12 is the refusal and `true`.
if [ -n "$values_bootstrap" ]; then
  bootstrap_render_args=(--set identity.renderedOffline=false)
  bootstrap_render_note=" --set identity.renderedOffline=false"
  bootstrap_target_note="the pinned bootstrap digest"
else
  bootstrap_render_args=(--set identity.renderedOffline=false --set-string "identity.bootstrapImage=$bootstrap_test_image")
  bootstrap_render_note=" --set identity.renderedOffline=false --set-string identity.bootstrapImage=<unpublished-test-digest>"
  bootstrap_target_note="an unpublished test-only bootstrap digest"
fi
# render [args...]: the gate's render, the bootstrap override first.
render() {
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} "$@"
}
# refuses <what> <diagnostic> [args...]: the render must fail, and for the reason
# the row was written for. A status alone is not enough: when the chart lost its
# default `api.console.mode`, several console rows started refusing for the
# missing mode instead, and only the named diagnostic showed it.
refuses() {
  what="$1"
  expect="$2"
  shift 2
  render "$@" > /dev/null 2> "$tmp/refusal.err"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "FAIL: $what was ACCEPTED; it must be refused before anything installs" >&2
    fail=1
    return
  fi
  grep -F -q -- "$expect" "$tmp/refusal.err"
  grc=$?
  if [ "$grc" -ne 0 ]; then
    echo "FAIL: $what was refused for the WRONG reason. Expected the diagnostic to name" >&2
    echo "      '$expect'; helm said:" >&2
    sed 's/^/      /' "$tmp/refusal.err" >&2
    fail=1
    return
  fi
  echo "   rc=$rc  ($what refused, naming $expect)"
}
# renders <what> <out> [args...]: the render must succeed into <out>; otherwise
# FAIL with helm's message and return 1, so the caller skips its own checks.
renders() {
  what="$1"
  out="$2"
  shift 2
  render "$@" > "$out" 2> "$out.err"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: $what did not render; rc=$rc" >&2
    sed 's/^/      /' "$out.err" >&2
    fail=1
    return 1
  fi
}
# THE THIRD REPOSITORY, D0 stage 7. The console image has no digest pin in the
# tree to strip a digest off — it is referenced by this chart alone — so its
# NAMESPACE is taken from the runner's and only the NAME is this chart's: a
# namespace move must carry all three without editing this gate. (The legacy
# `logweir-ui` image was withdrawn in v1, PROD-17.1 C4.) The NAME is D0's word
# for the deliverable (`Dockerfile.console`); the chart's values block and the
# binary keep the crate's (`api.*`, `logweir-api`), and
# charts/logweir/README.md reconciles the two.
CONSOLE_IMAGE_NAME="logweir-console"
console_repo="${runner_repo%/*}/$CONSOLE_IMAGE_NAME"
if [ "${runner_repo%/*}" = "$runner_repo" ]; then
  echo "FAIL: could not derive the console repository's namespace from the runner pin '$tree_runner'" >&2
  echo "      crates/weirkeeper/src/job.rs's RUNNER_IMAGE must name <host>/<namespace>/<name>@sha256:<64 hex>" >&2
  fail=1
fi
want_controller="$controller_repo:$LOGWEIR_TAG"
want_runner="$runner_repo:$LOGWEIR_TAG"
want_console="$console_repo:$LOGWEIR_TAG"
if [ "$values_controller" != "$want_controller" ]; then
  echo "FAIL: values.yaml controllerImage is '$values_controller'; it must be exactly '$want_controller'" >&2
  echo "      (arm 7: the namespace is the runner pin's, the name is weirkeeper, the tag is this chart's ruling of 2026-09-12)" >&2
  fail=1
fi
if [ "$values_runner" != "$want_runner" ]; then
  echo "FAIL: values.yaml runnerImage is '$values_runner'; it must be exactly '$want_runner'" >&2
  echo "      (arm 7: the repository is crates/weirkeeper/src/job.rs's RUNNER_IMAGE, the tag is this chart's ruling of 2026-09-12)" >&2
  fail=1
fi
if [ "$values_console" != "$want_console" ]; then
  echo "FAIL: values.yaml api.console.image is '$values_console'; it must be exactly '$want_console'" >&2
  echo "      (arm 7, D0 stage 7: the namespace is the runner pin's, the name is logweir-console —" >&2
  echo "      the image Dockerfile.console builds — and the tag is this chart's ruling of" >&2
  echo "      2026-09-12.)" >&2
  fail=1
fi
[ "$values_controller" = "$want_controller" ] && [ "$values_runner" = "$want_runner" ] &&
  [ "$values_console" = "$want_console" ] &&
  echo "   ok: controllerImage, runnerImage and api.console.image are the tree's repositories at $LOGWEIR_TAG"

bootstrap_required_diagnostic="identity.bootstrapImage must name a reviewed runner digest"
if [ -z "$values_bootstrap" ]; then
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" > /dev/null 2> "$tmp/bootstrap-release-blocked.err"
  rc=$?
  if [ "$rc" -eq 0 ] || ! grep -q "$bootstrap_required_diagnostic" "$tmp/bootstrap-release-blocked.err"; then
    echo "FAIL: empty identity.bootstrapImage did not stop the unsupported default render with its required-digest diagnostic" >&2
    fail=1
  else
    echo "   rc=$rc  (unpublished bootstrap default refused, as required)"
  fi
else
  # THE PIN IS THE TREE'S OWN RUNNER REPOSITORY BY DIGEST, NEVER ANOTHER
  # REPOSITORY AND NEVER A TAG: its container can patch the retained signer.
  case "$values_bootstrap" in
    "$runner_repo"@sha256:????????????????????????????????????????????????????????????????)
      echo "   ok: identity.bootstrapImage pins $values_bootstrap" ;;
    *) echo "FAIL: identity.bootstrapImage must be '$runner_repo@sha256:<64 hex>', found '$values_bootstrap'" >&2; fail=1 ;;
  esac
  if ! grep -Eq '^[[:space:]]+allowMutableBootstrapImageForDevelopment:[[:space:]]+false([[:space:]]|$)' "$CHART/values.yaml"; then
    echo "FAIL: a pinned default must ship identity.allowMutableBootstrapImageForDevelopment: false" >&2
    fail=1
  fi
  # AN EXPLICITLY EMPTIED VALUE STILL REFUSES TO RENDER, so removing the pin
  # can never silently fall back to an image without `identity bootstrap`.
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --set-string identity.bootstrapImage= \
    > /dev/null 2> "$tmp/bootstrap-emptied.err"
  rc=$?
  if [ "$rc" -eq 0 ] || ! grep -q "$bootstrap_required_diagnostic" "$tmp/bootstrap-emptied.err"; then
    echo "FAIL: an emptied identity.bootstrapImage rendered or lost its required-digest diagnostic" >&2
    fail=1
  else
    echo "   rc=$rc  (emptied bootstrap image refused, as required)"
  fi
fi

for unsafe_policy in Always IfNotPresent; do
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --set identity.renderedOffline=false \
    --set-string identity.bootstrapImage=logweir:mutable-development \
    --set identity.allowMutableBootstrapImageForDevelopment=true \
    --set "identity.bootstrapImagePullPolicy=$unsafe_policy" \
    > /dev/null 2> "$tmp/bootstrap-mutable-$unsafe_policy.err"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "FAIL: mutable identity bootstrap image was accepted with pull policy $unsafe_policy" >&2
    fail=1
  else
    echo "   rc=$rc  (mutable bootstrap with $unsafe_policy refused, as required)"
  fi
done
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --set identity.renderedOffline=false \
  --set-string identity.bootstrapImage=logweir:mutable-development \
  --set identity.allowMutableBootstrapImageForDevelopment=true \
  --set identity.bootstrapImagePullPolicy=Never > /dev/null 2> "$tmp/bootstrap-mutable-Never.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: explicit local-only mutable bootstrap image with pull policy Never was refused" >&2
  cat "$tmp/bootstrap-mutable-Never.err" >&2
  fail=1
else
  echo "   rc=$rc  (explicit mutable bootstrap development image accepted only with Never)"
fi

# ---------------------------------------------------------------- 3 + 4. lint and render
echo "== 3. helm lint, release defaults/examples with $bootstrap_target_note =="
render_targets=("default:")
for ex in "$EXAMPLES"/*.values.yaml; do
  name="$(basename "$ex" .values.yaml)"
  render_targets+=("$name:$ex")
done
[ "${#render_targets[@]}" -ge 2 ] || { echo "FAIL: no example under $EXAMPLES — a gate over one values file proves little" >&2; fail=1; }
for target in "${render_targets[@]}"; do
  name="${target%%:*}"
  file="${target#*:}"
  # `${array[@]+...}` keeps an empty argument list safe under Bash 3.2's `set -u`.
  if [ -z "$file" ]; then
    helm lint "$CHART" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} > "$tmp/lint-$name.log" 2>&1
    rc=$?
  else
    helm lint "$CHART" -f "$file" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} > "$tmp/lint-$name.log" 2>&1
    rc=$?
  fi
  echo "   rc=$rc  (helm lint $CHART${file:+ -f $file}$bootstrap_render_note)"
  if [ "$rc" -ne 0 ]; then
    cat "$tmp/lint-$name.log" >&2
    fail=1
  fi
done

echo "== 4. helm template into $RENDERED, then no drift =="
# The renders carry no CRDs (PROD-17.1 A5): they are checked once, in
# `$CHART/crds/`, by `just crds-check`.
mkdir -p "$RENDERED"
for target in "${render_targets[@]}"; do
  name="${target%%:*}"
  file="${target#*:}"
  if [ -z "$file" ]; then
    helm template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} > "$tmp/$name.yaml" 2> "$tmp/render-$name.err"
    rc=$?
  else
    helm template "$RELEASE" "$CHART" -n "$NAMESPACE" -f "$file" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} > "$tmp/$name.yaml" 2> "$tmp/render-$name.err"
    rc=$?
  fi
  echo "   rc=$rc  (helm template $RELEASE $CHART -n $NAMESPACE${file:+ -f $file}$bootstrap_render_note > $RENDERED/$name.yaml)"
  if [ "$rc" -ne 0 ]; then
    cat "$tmp/render-$name.err" >&2
    fail=1
    continue
  fi
  {
    echo "# GENERATED FILE — do not edit by hand."
    echo "# Rendered by scripts/check-chart.sh (just chart-check):"
    echo "#   helm template $RELEASE $CHART -n $NAMESPACE${file:+ -f $file}$bootstrap_render_note"
    echo "# It is checked in so a template change lands as a diff a reviewer reads; the"
    echo "# gate compares a temporary render and fails on drift."
    cat "$tmp/$name.yaml"
  } > "$tmp/expected-$name.yaml"
  if [ "$write" = true ]; then
    cp "$tmp/expected-$name.yaml" "$RENDERED/$name.yaml"
  elif ! diff -u "$RENDERED/$name.yaml" "$tmp/expected-$name.yaml"; then
    echo "FAIL: render drift in $name; run bash scripts/check-chart.sh --write" >&2
    fail=1
  fi
done

# ---------------------------------------------------------------- 5. GC7
# The two Logweir repositories come from arm 7 above, which is why that arm runs
# first. They are the ONLY references allowed to be a tag here, and the only tag
# allowed is $LOGWEIR_TAG — exactly.
echo "== 5. every rendered image is a digest, except the three Logweir images at $LOGWEIR_TAG (and $DIGEST_EXEMPT, by name) =="
images=0
logweir_tagged=0
for f in "$RENDERED"/*.yaml; do
  base="$(basename "$f")"
  if [ "$base" = "$DIGEST_EXEMPT" ]; then
    echo "   skip: $base — locally built tags under imagePullPolicy: Never, by design (author-only)"
    continue
  fi
  while IFS= read -r line; do
    ref="${line#*image: }"
    ref="${ref%%#*}"
    ref="${ref//\"/}"
    ref="${ref//\'/}"
    ref="$(printf '%s' "$ref" | tr -d '[:space:]')"
    images=$((images + 1))
    if [ "$ref" = "$bootstrap_test_image" ]; then
      continue
    fi
    # The shipped bootstrap pin is the runner repository BY DIGEST, which arm 7
    # already held to exactly `$runner_repo@sha256:<64 hex>`; it is the one
    # Logweir reference that must not carry the `:latest` tag.
    if [ -n "$values_bootstrap" ] && [ "$ref" = "$values_bootstrap" ]; then
      continue
    fi
    # THE THREE LOGWEIR IMAGES, BY REPOSITORY. Each must be exactly
    # `<repository>:$LOGWEIR_TAG` — a digest there is this chart's ruling
    # reverted, and any other tag is a value nobody chose.
    #
    # `logweir-console` IS NOT MATCHED BY THE RUNNER'S PATTERN, which is worth
    # stating because the names share a prefix: `$runner_repo:*` requires the
    # colon immediately after `.../logweir`.
    case "$ref" in
      "$controller_repo":*|"$runner_repo":*|"$console_repo":*|"$controller_repo"@*|"$runner_repo"@*|"$console_repo"@*)
        logweir_tagged=$((logweir_tagged + 1))
        if [ "$ref" != "$controller_repo:$LOGWEIR_TAG" ] &&
           [ "$ref" != "$runner_repo:$LOGWEIR_TAG" ] &&
           [ "$ref" != "$console_repo:$LOGWEIR_TAG" ]; then
          echo "FAIL: $base references a Logweir image as '$ref'; this chart names all three by '<repository>:$LOGWEIR_TAG' (arm 7)" >&2
          fail=1
        fi
        continue
        ;;
    esac
    # EVERY OTHER IMAGE: a digest, and never a tag of any kind.
    case "$ref" in
      *@sha256:????????????????????????????????????????????????????????????????) : ;;
      *)
        echo "FAIL: $base references an image by TAG: '$ref' (Global Constraint 7 pins by digest, never by tag)" >&2
        fail=1
        ;;
    esac
    case "$ref" in
      *:latest*) echo "FAIL: $base references :latest ('$ref') — only the three Logweir images may carry a tag here" >&2; fail=1 ;;
    esac
  done < <(grep -E '^[[:space:]]+(- )?image:[[:space:]]' "$f")
done
if [ "$images" -lt 3 ]; then
  echo "FAIL: only $images image reference(s) found across the rendered files — this arm would be asserting almost nothing" >&2
  fail=1
elif [ "$logweir_tagged" -lt 1 ]; then
  echo "FAIL: no rendered file references a Logweir repository at all — the tag arm would be asserting nothing" >&2
  fail=1
else
  echo "   ok: $images image reference(s) — $logweir_tagged Logweir at :$LOGWEIR_TAG, the rest @sha256:<64 hex>"
fi

# ---------------------------------------------------------------- 6. the schema
echo "== 6. values.schema.json refuses a non-boolean flag =="
for flag in demoKafka.enabled minio.enabled identity.enabled admissionPolicy.enabled api.enabled api.console.enabled; do
  refuses "--set $flag=yes" "/${flag//.//}': got string, want boolean" --set "$flag=yes"
done
refuses "a broad bootstrap API CIDR (0.0.0.0/0)" "/identity/kubernetesApiCIDRs/0': '0.0.0.0/0' does not match pattern" \
  --set-string 'identity.kubernetesApiCIDRs[0]=0.0.0.0/0'

# The installation policy's bounds. `weirkeeper::check::policy` parses the
# rendered `policy.json` with `deny_unknown_fields` and range checks, and a
# document it refuses fails CLOSED and SILENTLY (no attestations, no evidence
# allowlist, one advisory row), so the chart refuses the same values at install:
# keeping no discovery, a partial attestation (it would match nothing while
# looking relied on), a hard topic cap above the parser's, and a total below the
# per-workspace ceiling (cross-field, so `_validate.tpl` and not the schema).
refuses "checks.discovery.keepPerConnection=0" "/checks/discovery/keepPerConnection': minimum: got 0, want 1" \
  --set 'checks.discovery.keepPerConnection=0'
refuses "a visibility attestation naming only its id" "/checks/discovery/visibilityAttestations/0': missing properties" \
  --set-string 'checks.discovery.visibilityAttestations[0].id=att-partial'
refuses "checks.discovery.hardMaxTopics above the parser's ceiling" "/checks/discovery/hardMaxTopics': maximum" \
  --set 'checks.discovery.hardMaxTopics=100000'
refuses "checks.maxActiveTotal below checks.maxActivePerWorkspace" "must be at least checks.maxActivePerWorkspace" \
  --set 'checks.maxActiveTotal=2'

# The manual-run blocks render only away from the binaries' defaults, so an
# image-only rollback of a default install reads the documents it always did
# (chart_lint holds the default render to carrying neither); a non-default
# value IS rendered, or the knob would do nothing.
if renders "runs.maxManualBackupsActivePerWorkspace=6" "$tmp/runs-nondefault.yaml" \
  --set 'runs.maxManualBackupsActivePerWorkspace=6'; then
  if grep -q '"maxManualBackupsActivePerWorkspace": 6' "$tmp/runs-nondefault.yaml"; then
    echo "   rc=0  (a non-default runs.* renders the runs block)"
  else
    echo "FAIL: runs.maxManualBackupsActivePerWorkspace=6 did not render the policy's runs block" >&2
    fail=1
  fi
fi
if renders "api.console.rateLimits.manualRestoresPerMinute=3" "$tmp/ratelimits-nondefault.yaml" \
  -f "$CHART/examples/console.values.yaml" --set 'api.console.rateLimits.manualRestoresPerMinute=3'; then
  if grep -q 'manualRestoresPerMinute: 3' "$tmp/ratelimits-nondefault.yaml"; then
    echo "   rc=0  (a non-default api.console.rateLimits.* renders the rateLimits block)"
  else
    echo "FAIL: api.console.rateLimits.manualRestoresPerMinute=3 did not render the console's rateLimits" >&2
    fail=1
  fi
fi

# FX-10 — THE TWO WITHDRAWN VALUES ARE ACCEPTED, IGNORED, AND SAID TO BE.
#
# `checks.discovery.defaultMaxTopics` and `checks.preflight.defaultTimeoutSeconds`
# never changed anything (the CRDs default the request fields at admission), so
# they left `values.yaml`. Three properties, each a way the withdrawal could go
# wrong:
#   1. a values file or a `--reuse-values` upgrade that still carries them must
#      not fail on the closed schema — the render succeeds;
#   2. they are IGNORED — the render is byte-identical to one without them, so
#      the document carries the fixed compatibility values, not the configured
#      ones (a pre-FX-10 controller requires both keys);
#   3. the operator is TOLD — the install notes name each one, and say nothing
#      when neither is set. `--dry-run=client` with `KUBECONFIG=/dev/null`
#      renders NOTES.txt and cannot reach any cluster.
render \
  > "$tmp/withdrawn-base.yaml" 2> "$tmp/withdrawn-base.err"
rc_base=$?
render \
  --set 'checks.discovery.defaultMaxTopics=60000' --set 'checks.preflight.defaultTimeoutSeconds=900' \
  > "$tmp/withdrawn-set.yaml" 2> "$tmp/withdrawn-set.err"
rc=$?
if [ "$rc_base" -ne 0 ] || [ "$rc" -ne 0 ]; then
  cat "$tmp/withdrawn-set.err" >&2
  echo "FAIL: a render carrying the two withdrawn values was refused (rc=$rc, base rc=$rc_base); an upgrade from an older values file would fail" >&2
  fail=1
elif ! cmp -s "$tmp/withdrawn-base.yaml" "$tmp/withdrawn-set.yaml"; then
  diff -u "$tmp/withdrawn-base.yaml" "$tmp/withdrawn-set.yaml" >&2
  echo "FAIL: setting a withdrawn value changed the render; it must reach nothing" >&2
  fail=1
elif ! grep -q '"defaultMaxTopics": 20000' "$tmp/withdrawn-set.yaml" \
  || ! grep -q '"defaultTimeoutSeconds": 120' "$tmp/withdrawn-set.yaml"; then
  echo "FAIL: the policy document lost the fixed compatibility values a pre-FX-10 controller requires" >&2
  fail=1
else
  echo "   rc=$rc  (the two withdrawn values render, change nothing, and the document keeps 20000/120)"
fi
KUBECONFIG=/dev/null helm install "$RELEASE" "$CHART" -n "$NAMESPACE" --dry-run=client \
  ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
  --set 'checks.discovery.defaultMaxTopics=60000' --set 'checks.preflight.defaultTimeoutSeconds=900' \
  > "$tmp/withdrawn-notes.txt" 2> "$tmp/withdrawn-notes.err"
rc=$?
KUBECONFIG=/dev/null helm install "$RELEASE" "$CHART" -n "$NAMESPACE" --dry-run=client \
  ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
  > "$tmp/withdrawn-notes-none.txt" 2> "$tmp/withdrawn-notes-none.err"
rc_none=$?
if [ "$rc" -ne 0 ] || [ "$rc_none" -ne 0 ]; then
  cat "$tmp/withdrawn-notes.err" "$tmp/withdrawn-notes-none.err" >&2
  echo "FAIL: the client-side dry run that renders NOTES.txt failed (rc=$rc, without the values rc=$rc_none)" >&2
  fail=1
elif ! grep -q 'WITHDRAWN VALUES ARE SET AND IGNORED' "$tmp/withdrawn-notes.txt" \
  || ! grep -q 'checks.discovery.defaultMaxTopics=60000' "$tmp/withdrawn-notes.txt" \
  || ! grep -q 'checks.preflight.defaultTimeoutSeconds=900' "$tmp/withdrawn-notes.txt"; then
  echo "FAIL: the install notes do not name both withdrawn values as ignored" >&2
  fail=1
elif grep -q 'WITHDRAWN VALUES' "$tmp/withdrawn-notes-none.txt"; then
  echo "FAIL: the install notes warn about withdrawn values that nobody set" >&2
  fail=1
else
  echo "   rc=$rc  (the install notes name both withdrawn values as ignored, and are silent without them)"
fi

# FX-10 — EVERY CHART VALUE, CHANGED ON ITS OWN, CHANGES THE RENDER. A value
# whose non-default setting renders exactly what its default does reaches
# nothing; `scripts/check-chart-values.sh` holds one row per `values.yaml` leaf
# and `chart_lint` holds that table to the values file.
bash scripts/check-chart-values.sh ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"}
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: scripts/check-chart-values.sh found a chart value that reaches nothing (rc=$rc)" >&2
  fail=1
else
  echo "   rc=$rc  (every chart value, changed alone, changes the render)"
fi

# The admission policy's subject is its whole effect: a null name used to render
# `%!s(<nil>)`, a fence that installs, reads as enabled and matches nobody.
# `--set` for a real null and `--set-string` for the empty string (`--set-string
# x=null` is the legal name "null" and proves nothing).
refuses "a null admissionPolicy.consoleServiceAccountName" "/admissionPolicy': missing property 'consoleServiceAccountName'" \
  --set admissionPolicy.enabled=true --set 'admissionPolicy.consoleServiceAccountName=null'
refuses "an empty admissionPolicy.consoleServiceAccountName" "/admissionPolicy/consoleServiceAccountName': minLength" \
  --set admissionPolicy.enabled=true --set-string 'admissionPolicy.consoleServiceAccountName='

# ---- THE FENCE'S SUBJECT UNDER A NON-DEFAULT RELEASE NAME (review F3) ----
#
# Every other render in this gate uses $RELEASE, which is `logweir`, and that is
# exactly the one release name under which the bug could not be seen: the
# console ServiceAccount is `<release>-api` and
# `admissionPolicy.consoleServiceAccountName` defaults to the fixed string
# `logweir-api`, so they agree there and NOWHERE ELSE. Under any other name the
# policy used to install, read as enabled, and fence nobody — leaving the
# console's `create` on Secrets, which RBAC cannot narrow by shape and which
# this policy is the only bound on, with no bound at all.
#
# Two arms, because a refusal that refuses everything is not a fence either:
# the divergent pair is REFUSED, and the aligned pair RENDERS and carries the
# release-derived principal in its one matchConditions expression.
OTHER_RELEASE=notlogweir
helm template "$OTHER_RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
  --set api.enabled=true --set admissionPolicy.enabled=true \
  > /dev/null 2> "$tmp/vap-release-diverged.err"
rc=$?
if [ "$rc" -eq 0 ]; then
  echo "FAIL: release '$OTHER_RELEASE' rendered ServiceAccount $OTHER_RELEASE-api beside a policy fencing the DEFAULT name; the fence would match nobody" >&2
  fail=1
else
  echo "   rc=$rc  (a console account and a fence subject that disagree refused, as they must be)"
fi
helm template "$OTHER_RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
  --set api.enabled=true --set admissionPolicy.enabled=true \
  --set "admissionPolicy.consoleServiceAccountName=$OTHER_RELEASE-api" \
  > "$tmp/vap-release-aligned.yaml" 2> "$tmp/vap-release-aligned.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: release '$OTHER_RELEASE' with a matching consoleServiceAccountName must RENDER; rc=$rc" >&2
  sed 's/^/      /' "$tmp/vap-release-aligned.err" >&2
  fail=1
else
  want="system:serviceaccount:$NAMESPACE:$OTHER_RELEASE-api"
  grep -F -q "$want" "$tmp/vap-release-aligned.yaml"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: the aligned render's policy does not name $want" >&2
    fail=1
  fi
  grep -F -q "name: $OTHER_RELEASE-api" "$tmp/vap-release-aligned.yaml"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: the aligned render carries no ServiceAccount $OTHER_RELEASE-api for the policy to fence" >&2
    fail=1
  fi
  echo "   rc=0  (release '$OTHER_RELEASE': the account and the fence subject are both $OTHER_RELEASE-api)"
fi

# ---- 8. THE CONSOLE'S REFUSALS, RUN RATHER THAN READ (D0 stage 7) ----
#
# `crates/logweir/tests/chart_lint.rs` asserts these from checked-in bytes —
# the schema's `pattern`, the `fail` strings in
# `templates/_validate.tpl` — because Global Constraint 22 forbids a
# `#[test]` that shells out. THIS is the half that proves `helm` ACTS on them,
# and the difference matters: a `fail` inside a `define` that no rendered path
# reaches is a refusal that reads correctly and refuses nothing.
#
# EACH ROW NAMES THE DIAGNOSTIC IT EXPECTS, not merely a non-zero status. A
# refusal that fires for the WRONG reason is not a guard: when the chart lost
# its default `api.console.mode`, several rows below started refusing for the
# missing mode instead of the thing they were written for, and a status-only
# check would have stayed green through all of them.
echo "== 8. the console's refusals, run =="
CONSOLE_ON=(--set api.enabled=true --set api.console.enabled=true)
CONSOLE_KEY=(--set api.console.keySecret=logweir-console-keys)
CONSOLE_LOCAL=(--set api.console.mode=localAdmin)
# PROD-16.1: the bootstrap image's PROD-16.1 features. The default turned ON
# with the re-pin to a runner that carries them (rev 2fe8d907); the rows below
# still state it, so they keep their meaning if the default ever moves again.
FEATURE=(--set identity.bootstrapFeatures.consoleKey=true)
# OD-23: one namespace, workspaces inside it. The base shared shape binds its
# viewers in workspace team-a.
CONSOLE_WS=team-a
CONSOLE_SHARED=(
  --set api.console.mode=shared
  --set "workspaces={$CONSOLE_WS}"
  --set api.console.oidc.issuer=https://idp.example.com/realms/logweir
  --set api.console.oidc.clientId=logweir-console
  --set api.console.oidc.clientSecret=logweir-console-oidc
  --set api.console.roles.revision=2026-09-21.1
  --set api.console.roles.bindings[0].role=viewer
  --set "api.console.roles.bindings[0].workspace=$CONSOLE_WS"
  --set "api.console.roles.bindings[0].groups[0]=logweir-viewers"
)
# Release-notes item 74: `notify` is withdrawn with the alert sinks, and a
# values file that still sets it is refused by name. CONTROL: `notify=null`,
# which an upgrade with --reuse-values passes to clear it, renders.
refuses "a values file that still sets notify.allowInsecureSinks" "notify.* is withdrawn" \
  --set notify.allowInsecureSinks=false
# D-C1 (poc-14-1b): the refusal names no sub-key, so a values file that sets
# another one is not told about `allowInsecureSinks`, a key it never set.
# CONTROL: the message before D-C1 named it, and this row fails on it.
refuses "a values file that sets another notify key" "notify.* is withdrawn" \
  --set notify.webhookUrl=https://example.invalid/hook
grep -F -q "allowInsecureSinks" "$tmp/refusal.err"
grc=$?
if [ "$grc" -eq 0 ]; then
  echo "FAIL: the notify refusal names notify.allowInsecureSinks, which this values file never set:" >&2
  sed 's/^/      /' "$tmp/refusal.err" >&2
  fail=1
else
  echo "   rc=$grc  (the refusal names no notify sub-key the values did not set)"
fi
renders "the control --set notify=null (an upgrade clearing the withdrawn block)" "$tmp/notify-null.yaml" \
  --set notify=null \
  && echo "   rc=0  (notify=null renders: the control)"
refuses "the console with NO mode at all" "api.console.mode is empty" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}"
refuses "the console workload without its principal (api.enabled=false)" "requires api.enabled" \
  --set api.enabled=false --set api.console.enabled=true "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}"
refuses "the console with no key Secret" "api.console.keySecret is empty" \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}"
# PLAT-19.2 — the approval policy's three refusals, run. The first two need no
# console at all: the controller mounts the same document.
refuses "an Ordinary approval policy without the installation floor" \
  "is Ordinary but approvalPolicy.allowOrdinaryConfirmation is not true" \
  --set approvalPolicy.policies[0].name=team-ordinary \
  --set approvalPolicy.policies[0].mode=Ordinary
refuses "a workspace bound to an undeclared approval policy" \
  "which approvalPolicy.policies does not declare" \
  --set approvalPolicy.policies[0].name=prod-governed \
  --set approvalPolicy.policies[0].mode=Governed \
  --set "approvalPolicy.workspaces.default=nothing"
# PROD-16.1: with the managed identity the hook GENERATES the console key, so
# the refusal is for an install with neither the managed identity nor a named
# key Secret; the same values with the identity render (the managed key).
refuses "a workspace bound to a policy with no confirmation key" \
  "approvalPolicy.confirmationKeySecret is empty" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" \
  --set identity.enabled=false \
  --set approvalPolicy.policies[0].name=prod-governed \
  --set approvalPolicy.policies[0].mode=Governed \
  --set "approvalPolicy.workspaces.default=prod-governed"
refuses "default: confirm with no confirmation key" \
  "approvalPolicy.default is confirm and there is no console confirmation key" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" \
  --set identity.enabled=false \
  --set approvalPolicy.allowOrdinaryConfirmation=true \
  --set approvalPolicy.default=confirm
# PROD-16.1: WITH THE BOOTSTRAP FEATURE OFF (set explicitly: an operator pinning
# an older bootstrap image turns it off) a console render passes the hook NONE
# of the PROD-16.1 flags and renders none of their objects — the chart works
# with a pinned image that predates them.
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" \
  --set identity.bootstrapFeatures.consoleKey=false > "$tmp/feature-off.yaml" 2> "$tmp/feature-off.err"
rc=$?
if [ "$rc" -ne 0 ] || grep -q -E -- "--console-secret-name|--console-public-configmap-name|--installation-trust-policy|--allowed-target-cluster-id|--mark-fresh-install-confirm|--revoke-trust-binding" "$tmp/feature-off.yaml" \
  || grep -q "name: $RELEASE-identity-trust$" "$tmp/feature-off.yaml" \
  || grep -q "logweir-console-confirmation" "$tmp/feature-off.yaml" \
  || grep -q "confirmationKeyManaged" "$tmp/feature-off.yaml"; then
  echo "FAIL: with identity.bootstrapFeatures.consoleKey off, a console render passed a PROD-16.1 hook flag or rendered its objects (or failed)" >&2
  sed 's/^/      /' "$tmp/feature-off.err" >&2
  fail=1
else
  echo "   rc=$rc  (bootstrap feature off: no PROD-16.1 hook flag, no managed key, no trust grant)"
fi
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" "${FEATURE[@]}" \
  --set approvalPolicy.policies[0].name=prod-governed \
  --set approvalPolicy.policies[0].mode=strict \
  --set "approvalPolicy.workspaces.default=prod-governed" \
  > "$tmp/managed-key.yaml" 2> "$tmp/managed-key.err"
rc=$?
if [ "$rc" -ne 0 ] || ! grep -q "secretName: logweir-console-confirmation" "$tmp/managed-key.yaml" \
  || ! grep -q "confirmationKeyManaged: true" "$tmp/managed-key.yaml"; then
  echo "FAIL: a console-served binding with the managed identity did not render the managed console key (and confirmationKeyManaged)" >&2
  sed 's/^/      /' "$tmp/managed-key.err" >&2
  fail=1
else
  echo "   rc=$rc  (a console-served binding with the managed identity renders the generated key)"
fi
# PROD-16.1 security review: THE INSTALL-ONLY TRUST GRANT. An upgrade render
# carries neither the ClusterRole/Binding that lets the hook create a
# TrustPolicy nor the arguments that would spend it; a first-install render
# carries both. (A rollback replays the stored revision's hooks, and the grant is
# a `post-install` hook only, so a rollback never runs it. An install over an
# EXISTING identity is the `lookup` half of the same guard, which only a
# connected render can see; docs/kubernetes.md §8.)
render --is-upgrade \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" "${FEATURE[@]}" > "$tmp/upgrade.yaml" 2> "$tmp/upgrade.err"
rc=$?
if [ "$rc" -ne 0 ] || grep -q "name: $RELEASE-identity-trust$" "$tmp/upgrade.yaml" \
  || grep -q -- "--installation-trust-policy" "$tmp/upgrade.yaml" \
  || grep -q -- "--mark-fresh-install-confirm" "$tmp/upgrade.yaml" \
  || ! grep -q -- "--revoke-trust-binding" "$tmp/upgrade.yaml"; then
  echo "FAIL: the install-only trust grant is absent from an upgrade render — it was not (or the render failed)" >&2
  sed 's/^/      /' "$tmp/upgrade.err" >&2
  fail=1
else
  echo "   rc=$rc  (the install-only trust grant is absent from an upgrade render; the revocation stays)"
fi
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" "${FEATURE[@]}" > "$tmp/install.yaml" 2> "$tmp/install.err"
if ! grep -q "name: $RELEASE-identity-trust$" "$tmp/install.yaml" \
  || ! grep -q -- "--mark-fresh-install-confirm" "$tmp/install.yaml"; then
  echo "FAIL: a first-install render with a console lost the fresh install's trust grant or marker" >&2
  fail=1
else
  echo "   rc=0  (a first-install render with a console carries the transient grant and the marker)"
fi
# THE FIX ROUND (review M2): an install that never uses confirm gets no grant
# and no automatic trust — no console, or an external identity Secret (never
# `generated`) — while the revocation is still passed.
for case in "no console" "an external identity Secret"; do
  case "$case" in
    "no console") extra=() ;;
    *) extra=("${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" --set identity.externalSecret.name=company-signer) ;;
  esac
  render \
    "${FEATURE[@]}" ${extra[@]+"${extra[@]}"} > "$tmp/no-confirm.yaml" 2> "$tmp/no-confirm.err"
  rc=$?
  if [ "$rc" -ne 0 ] || grep -q "name: $RELEASE-identity-trust$" "$tmp/no-confirm.yaml" \
    || grep -q -- "--installation-trust-policy" "$tmp/no-confirm.yaml" \
    || ! grep -q -- "--revoke-trust-binding" "$tmp/no-confirm.yaml"; then
    echo "FAIL: a fresh install with $case rendered the trust grant or the trust step (or lost the revocation, or failed)" >&2
    sed 's/^/      /' "$tmp/no-confirm.err" >&2
    fail=1
  else
    echo "   rc=$rc  (a fresh install with $case renders no trust grant and no trust step)"
  fi
done
# THE FIX ROUND (review H1): THE HELM FLOOR. `logweir.helmCleansFailedHooks` is
# the one decision; Helm cannot be asked to report another version, so its
# definition is lifted out of the template into a throwaway chart and called with
# fixed versions — the versions whose `hooks.go` the review read.
mkdir -p "$tmp/floor/templates"
printf 'apiVersion: v2\nname: floor\nversion: 0.0.1\n' > "$tmp/floor/Chart.yaml"
awk '/define "logweir.helmCleansFailedHooks"/,/^\{\{- end -\}\}$/' "$CHART/templates/_validate.tpl" > "$tmp/floor/templates/_floor.tpl"
{
  for v in v3.12.0 v3.18.6 v3.19.0-rc.1 v3.19.0 v4.0.1; do
    printf '# %s=[{{ include "logweir.helmCleansFailedHooks" "%s" }}]\n' "$v" "$v"
  done
} > "$tmp/floor/templates/floor.yaml"
floor="$(helm template floor "$tmp/floor" 2> "$tmp/floor.err" | grep '^# v' | tr '\n' ' ')"
expected="# v3.12.0=[] # v3.18.6=[] # v3.19.0-rc.1=[true] # v3.19.0=[true] # v4.0.1=[true] "
if [ "$floor" != "$expected" ] || ! grep -q -F '{{- if and (include "logweir.identity.freshTrust" .) (not (include "logweir.helmCleansFailedHooks" .Capabilities.HelmVersion.Version)) }}' "$CHART/templates/_validate.tpl"; then
  echo "FAIL: the Helm floor for the install-only trust grant: expected '$expected', got '$floor' (or _validate.tpl no longer gates the grant on it)" >&2
  sed 's/^/      /' "$tmp/floor.err" >&2
  fail=1
else
  echo "   rc=0  (the trust grant's Helm floor: 3.12.0 and 3.18.6 refused, 3.19.0-rc.1, 3.19.0 and 4.0.1 allowed)"
fi
# And, when a pre-3.19 Helm is at hand (LOGWEIR_HELM_PRE_3_19=<binary>), the
# real refusal: the fresh console install fails naming the floor, and the same
# values with the trust disabled render.
if [ -n "${LOGWEIR_HELM_PRE_3_19:-}" ]; then
  old_version="$("$LOGWEIR_HELM_PRE_3_19" version --short 2>/dev/null)"
  "$LOGWEIR_HELM_PRE_3_19" template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
    "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" "${FEATURE[@]}" > /dev/null 2> "$tmp/old-helm.err"
  rc=$?
  "$LOGWEIR_HELM_PRE_3_19" template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
    "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_LOCAL[@]}" "${FEATURE[@]}" \
    --set identity.installationTrust.enabled=false > "$tmp/old-helm-strict.yaml" 2> "$tmp/old-helm-strict.err"
  rc_strict=$?
  if [ "$rc" -eq 0 ] || ! grep -q "needs Helm 3.19.0 or newer" "$tmp/old-helm.err" || [ "$rc_strict" -ne 0 ] \
    || grep -q "name: $RELEASE-identity-trust$" "$tmp/old-helm-strict.yaml"; then
    echo "FAIL: $old_version did not refuse the install-only trust grant by name, or refused the strict install too" >&2
    sed 's/^/      /' "$tmp/old-helm.err" "$tmp/old-helm-strict.err" >&2
    fail=1
  else
    echo "   rc=$rc  ($old_version refuses the fresh install's trust grant, naming the floor; with the trust off it renders)"
  fi
else
  echo "   skip  (LOGWEIR_HELM_PRE_3_19 unset: the live pre-3.19 refusal is not run here; the floor self-test above is)"
fi
# PROD-16.1: the operator's mode names render as the internal ones, and an
# explicit unbound default renders as defaultMode.
render \
  --set approvalPolicy.allowOrdinaryConfirmation=true \
  --set approvalPolicy.default=confirm \
  --set approvalPolicy.policies[0].name=team --set approvalPolicy.policies[0].mode=confirm \
  --set approvalPolicy.policies[1].name=prod --set approvalPolicy.policies[1].mode=strict \
  --show-only templates/approval-policy.yaml > "$tmp/modes.yaml" 2> "$tmp/modes.err"
rc=$?
if [ "$rc" -ne 0 ] || ! grep -q "mode: Ordinary" "$tmp/modes.yaml" || ! grep -q "mode: Governed" "$tmp/modes.yaml" \
  || grep -q "mode: confirm" "$tmp/modes.yaml" || ! grep -q "defaultMode: confirm" "$tmp/modes.yaml"; then
  echo "FAIL: the operator mode names render as the internal ones (confirm -> Ordinary, strict -> Governed, default -> defaultMode)" >&2
  sed 's/^/      /' "$tmp/modes.err" >&2
  fail=1
else
  echo "   rc=$rc  (the operator mode names render as the internal ones)"
fi
# PROD-16.2: TWO-PERSON APPROVAL RENDERS ONLY WITH THE SHARED CONSOLE. The
# second person is a second identity the provider vouches for: the in-cluster
# administrator console has one, and an install with no console has nobody to
# click. A CHART-ONLY refusal (the document is one the binary reads), so it
# lives here and not in scripts/approval-policy-refusals/. Both spellings of
# the setting, without a console and with the administrator console; then the
# control: with the shared console the same policy renders, as the internal
# names, with `approverSignature: Console` and no operator word.
TWO_PERSON=(--set approvalPolicy.policies[0].name=pair --set approvalPolicy.policies[0].mode=two-person)
TWO_PERSON_INTERNAL=(--set approvalPolicy.policies[0].name=pair --set approvalPolicy.policies[0].mode=Governed
  --set approvalPolicy.policies[0].approverSignature=Console)
PAIR_KEY=(--set approvalPolicy.confirmationKeySecret=logweir-console-confirmation)
refuses "a two-person policy with no console at all" "api.console.mode=shared" \
  "${TWO_PERSON[@]}"
refuses "approverSignature: Console with no console at all" "api.console.mode=shared" \
  "${TWO_PERSON_INTERNAL[@]}"
refuses "a two-person policy with the principal and no console workload" "api.console.mode=shared" \
  --set api.enabled=true "${TWO_PERSON[@]}"
refuses "a two-person policy with the in-cluster administrator console" "api.console.mode=localAdmin" \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}" "${PAIR_KEY[@]}" "${TWO_PERSON[@]}"
refuses "approverSignature: Console with the in-cluster administrator console" "cannot be two people" \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}" "${PAIR_KEY[@]}" "${TWO_PERSON_INTERNAL[@]}"
two_person_renders() {
  what="$1"
  shift
  render \
    "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
    --set-string api.console.publicBaseUrl=https://console.example.com \
    "${PAIR_KEY[@]}" "$@" \
    --show-only templates/approval-policy.yaml > "$tmp/two-person.yaml" 2> "$tmp/two-person.err"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: a two-person policy ($what) with the shared console did not render; rc=$rc" >&2
    sed 's/^/      /' "$tmp/two-person.err" >&2
    fail=1
    return
  fi
  if ! grep -q "approverSignature: Console" "$tmp/two-person.yaml" \
    || ! grep -q "mode: Governed" "$tmp/two-person.yaml" || grep -q "two-person" "$tmp/two-person.yaml"; then
    echo "FAIL: a two-person policy ($what) must render mode: Governed and approverSignature: Console, and no operator word:" >&2
    sed 's/^/      /' "$tmp/two-person.yaml" >&2
    fail=1
    return
  fi
  echo "   rc=$rc  (a two-person policy with the shared console renders Governed + approverSignature: Console; $what)"
}
two_person_renders "mode: two-person" "${TWO_PERSON[@]}"
two_person_renders "mode: Governed, approverSignature: Console" "${TWO_PERSON_INTERNAL[@]}"
# ... and a policy WITHOUT the setting renders no `approverSignature` key, so an
# older binary still reads every document that does not ask for two-person.
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com "${PAIR_KEY[@]}" \
  --set approvalPolicy.policies[0].name=prod --set approvalPolicy.policies[0].mode=strict \
  --show-only templates/approval-policy.yaml > "$tmp/strict-only.yaml" 2> "$tmp/strict-only.err"
rc=$?
if [ "$rc" -ne 0 ] || grep -q "approverSignature" "$tmp/strict-only.yaml"; then
  echo "FAIL: a strict policy renders no approverSignature key" >&2
  sed 's/^/      /' "$tmp/strict-only.err" >&2
  fail=1
else
  echo "   rc=$rc  (a strict policy renders no approverSignature key)"
fi
refuses "shared mode over plain HTTP" "publicBaseUrl" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=http://console.example.com
refuses "shared mode with a publicBaseUrl carrying a trailing slash" "publicBaseUrl" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com/
refuses "a shared-console Ingress with no TLS Secret" "api.console.ingress.tlsSecretName is empty" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set api.console.ingress.enabled=true --set api.console.ingress.host=console.example.com
refuses "an Ingress host that is not publicBaseUrl's authority" "They must name the SAME authority" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set api.console.ingress.enabled=true --set api.console.ingress.host=console.internal.example \
  --set api.console.ingress.tlsSecretName=logweir-console-tls
refuses "an Ingress in front of the in-cluster administrator mode" "is refused" \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}" \
  --set api.console.ingress.enabled=true --set api.console.ingress.host=console.example.com \
  --set api.console.ingress.tlsSecretName=logweir-console-tls
# FX-24c: a shared console the chart publishes through its Ingress names that
# proxy (trustedProxyService or trustedProxyCidrs), or opts out by name. Without
# it the per-peer connection cap is off and every sign-in through the ingress
# shares one budget, so leaving it unnamed by accident is refused.
CONSOLE_INGRESS=(
  --set-string api.console.publicBaseUrl=https://console.example.com
  --set api.console.ingress.enabled=true --set api.console.ingress.host=console.example.com
  --set api.console.ingress.tlsSecretName=logweir-console-tls
)
refuses "a shared-console Ingress with no trusted proxy named" \
  "api.console.ingress.enabled in shared mode needs the trusted proxy named" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${CONSOLE_INGRESS[@]}"
refuses "the trusted-proxy opt-out beside a named proxy" "trustedProxy=none says the console trusts no proxy" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${CONSOLE_INGRESS[@]}" \
  --set api.console.trustedProxy=none --set "api.console.trustedProxyCidrs={10.42.0.0/24}"
refuses "the trusted-proxy opt-out in the in-cluster administrator mode" \
  "api.console.trustedProxy belongs to api.console.mode=shared" \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}" --set api.console.trustedProxy=none
refuses "a trusted-proxy opt-out that is not \`none\`" "/api/console/trustedProxy': value must be one of" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${CONSOLE_INGRESS[@]}" \
  --set api.console.trustedProxy=off
# …and the deliberate opt-out renders the Ingress, with no proxy named.
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${CONSOLE_INGRESS[@]}" \
  --set api.console.trustedProxy=none \
  > "$tmp/console-optout.yaml" 2> "$tmp/console-optout.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: the shared-console Ingress with api.console.trustedProxy=none did NOT render; rc=$rc" >&2
  sed 's/^/      /' "$tmp/console-optout.err" >&2
  fail=1
elif ! grep -q "^kind: Ingress$" "$tmp/console-optout.yaml" || grep -F -q "trustedProxyService:" "$tmp/console-optout.yaml" \
  || grep -F -q "trustedProxyCidrs:" "$tmp/console-optout.yaml"; then
  echo "FAIL: the opt-out render must carry the Ingress and name no trusted proxy in the console's configuration" >&2
  fail=1
else
  echo "   rc=0  (the deliberate opt-out, api.console.trustedProxy=none, renders the Ingress with no proxy named)"
fi
refuses "a product role binding for a workspace the install does not have" "names workspace \"qa\", which is not in workspaces" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set "api.console.roles.bindings[0].workspace=qa"
refuses "a product role binding that still names a namespace" "names a namespace; it is renamed workspace" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set "api.console.roles.bindings[0].namespace=team-a"
refuses "a wildcard in a product role binding" "contains \`*\` or \`?\`" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set-string "api.console.roles.bindings[0].groups[0]=logweir-*"
refuses "a shared-console NetworkPolicy with no ingress-controller selector" "needs both networkPolicy.ingressNamespace" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set api.console.networkPolicy.enabled=true
# OD-23 (PROD-19.1 §5, C1 and C2): ONE NAMESPACE, WORKSPACES INSIDE IT. Every
# value that named another namespace, and every per-namespace one, is refused
# by name, saying where it went; `null` (how --reuse-values clears one) renders.
refuses "the removed kubernetes.namespace" "kubernetes.namespace is removed" \
  --set kubernetes.namespace=team-a
refuses "the removed kubernetes.connectionsNamespace" "kubernetes.connectionsNamespace is removed" \
  --set kubernetes.connectionsNamespace=team-a
refuses "the removed controller.watchNamespaces" "controller.watchNamespaces is removed" \
  --set "controller.watchNamespaces={team-a}"
refuses "the removed identity.authorizedRunnerNamespaces" "identity.authorizedRunnerNamespaces is removed" \
  --set "identity.authorizedRunnerNamespaces={team-a}"
refuses "the removed api.namespaces" "api.namespaces is removed" \
  --set "api.namespaces={team-a}"
refuses "the renamed approvalPolicy.namespaces" "renamed approvalPolicy.workspaces" \
  --set approvalPolicy.policies[0].name=p --set approvalPolicy.policies[0].mode=strict \
  --set "approvalPolicy.namespaces.team-a=p"
for cap in checks.maxActivePerNamespace checks.maxEvidenceFetchActivePerNamespace \
  runs.maxManualBackupsActivePerNamespace runs.maxManualRestoresActivePerNamespace; do
  refuses "the renamed $cap" "renamed ${cap%PerNamespace}PerWorkspace" --set "$cap=3"
done
renders "the control --set controller.watchNamespaces=null" "$tmp/watch-null.yaml" \
  --set controller.watchNamespaces=null \
  && echo "   rc=0  (controller.watchNamespaces=null renders: the control)"
refuses "an empty workspaces list" "/workspaces': minItems" --set-json 'workspaces=[]'
refuses "a duplicate workspace" "/workspaces': items at 0 and 1 are equal" --set "workspaces={dev,dev}"
refuses "a workspace that is not a DNS label" "/workspaces/0': 'Dev_1' does not match pattern" --set "workspaces={Dev_1}"
refuses "an approval binding for a workspace the install does not have" \
  "approvalPolicy.workspaces.qa binds a workspace that is not in workspaces" \
  --set approvalPolicy.policies[0].name=p --set approvalPolicy.policies[0].mode=strict \
  --set "workspaces={dev,prd}" --set "approvalPolicy.workspaces.qa=p"
# AND FOUR WORKSPACES, BOUND AND APPROVED PER WORKSPACE, RENDER (C2): the
# controller's list, the console's list and the approval bindings, from one value.
if renders "four workspaces with bindings and approval bindings" "$tmp/four.yaml" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com --set api.console.trustedProxy=none \
  --set "workspaces={dev,tst,stg,prd}" \
  --set "api.console.roles.bindings[0].workspace=prd" \
  --set approvalPolicy.allowOrdinaryConfirmation=true \
  --set approvalPolicy.policies[0].name=one-person --set approvalPolicy.policies[0].mode=confirm \
  --set approvalPolicy.policies[1].name=two-person --set approvalPolicy.policies[1].mode=two-person \
  --set approvalPolicy.workspaces.prd=two-person --set approvalPolicy.workspaces.dev=one-person; then
  if grep -F -q 'value: "dev,tst,stg,prd"' "$tmp/four.yaml" \
    && grep -A4 -F -- '    workspaces:' "$tmp/four.yaml" | grep -F -q -- '      - prd' \
    && grep -A2 -F -- '    workspaces:' "$tmp/four.yaml" | grep -F -q -- '      prd: two-person'; then
    echo "   rc=0  (four workspaces render: LOGWEIR_WORKSPACES, the console's list, prd bound two-person)"
  else
    echo "FAIL: four workspaces did not reach LOGWEIR_WORKSPACES, the console's list and the approval bindings" >&2
    fail=1
  fi
fi
# PROD-17.1 C4 — `ui.*` IS WITHDRAWN AND REFUSED BY NAME, whatever it carries
# (an upgraded values file that kept `ui.enabled: false` included), pointing at
# the console. The control is the same render without the block.
refuses "the withdrawn ui.* block (ui.enabled=true)" "ui.* is withdrawn" \
  --set ui.enabled=true
refuses "the withdrawn ui.* block (ui.enabled=false)" "use the console, api.console.*" \
  --set ui.enabled=false
render \
  > /dev/null 2> "$tmp/ui-withdrawn-control.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: the control for the ui.* refusal — the same render with no ui block — exited $rc" >&2
  sed 's/^/      /' "$tmp/ui-withdrawn-control.err" >&2
  fail=1
else
  echo "   rc=0  (control: the same render with no ui block renders)"
fi
# kafka.markerTopic (the source's) was typed by the schema and read by nothing;
# it is refused by name, not by the schema's generic unknown-key error.
# CONTROL: the target's markerTopic, the key that does mark a scratch target,
# still renders onto the target KafkaCluster.
refuses "the withdrawn source kafka.markerTopic" "kafka.markerTopic is not read by this chart; set kafka.target.markerTopic instead" \
  --set kafka.enabled=true --set kafka.bootstrapServers=source:9092 \
  --set kafka.markerTopic=logweir.scratch
if renders "the control: kafka.target.markerTopic" "$tmp/marker-target.yaml" \
  --set kafka.enabled=true --set kafka.bootstrapServers=source:9092 \
  --set kafka.target.bootstrapServers=target:9092; then
  grep -F -q 'markerTopic: "logweir.scratch"' "$tmp/marker-target.yaml"
  grc=$?
  if [ "$grc" -ne 0 ]; then
    echo "FAIL: the control for the kafka.markerTopic refusal rendered no target markerTopic" >&2
    fail=1
  else
    echo "   rc=0  (control: kafka.target.markerTopic renders onto the target)"
  fi
fi
refuses "a trusted-proxy requirement with no range to trust" "requireTrustedProxy needs" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set api.console.requireTrustedProxy=true
refuses "a trusted-proxy range wide enough to trust every pod" "wider than /16" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set api.console.requireTrustedProxy=true --set "api.console.trustedProxyCidrs={10.0.0.0/8}"
refuses "a trusted-proxy range of everybody (IPv6)" "wider than /48" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set api.console.requireTrustedProxy=true --set "api.console.trustedProxyCidrs={::/0}"
refuses "a trusted-proxy requirement in the in-cluster administrator mode" "belongs to api.console.mode=shared" \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}" \
  --set api.console.requireTrustedProxy=true

# THE PoC CHART GAPS (G1, G2, G3, G6) — each refusal names its value.
SHARED_URL=(--set-string api.console.publicBaseUrl=https://console.example.com)
refuses "half a trusted-proxy Service" "names only half a Service" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set api.console.trustedProxyService.name=traefik
refuses "a trusted-proxy Service in the in-cluster administrator mode" "trustedProxyService belongs to api.console.mode=shared" \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}" \
  --set api.console.trustedProxyService.namespace=traefik --set api.console.trustedProxyService.name=traefik
refuses "an issuer CA bundle named twice" "names both ConfigMap" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set api.console.oidc.caBundle.configMap=ca --set api.console.oidc.caBundle.secret=ca
refuses "an issuer CA bundle with no key" "/api/console/oidc/caBundle/key': minLength" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set api.console.oidc.caBundle.configMap=ca --set-string api.console.oidc.caBundle.key=
refuses "no trust anchor at all for the issuer" "systemRoots=false without" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set api.console.oidc.systemRoots=false
# F6: the two OIDC options the API reads. A scope list without openid, and a
# display-name claim that is not a plain claim name, are refused by name.
refuses "OIDC scopes without openid" "scopes is [profile email] without openid" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set-json 'api.console.oidc.scopes=["profile","email"]'
refuses "a display-name claim that is not a plain claim name" "/api/console/oidc/displayNameClaim'" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set-string api.console.oidc.displayNameClaim=https://idp.example.com/claims/name
# AND SET RIGHT, BOTH REACH THE API'S CONFIG; unset (every committed render),
# neither is written and the API keeps its defaults.
renders "the two OIDC options" "$tmp/oidc-options.yaml" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set-json 'api.console.oidc.scopes=["openid","profile","groups"]' \
  --set-string api.console.oidc.displayNameClaim=preferred_username &&
  if grep -F -q -- '      displayNameClaim: "preferred_username"' "$tmp/oidc-options.yaml" &&
    grep -A3 -F -- '      scopes:' "$tmp/oidc-options.yaml" | grep -F -q -- '        - groups'; then
    echo "   rc=0  (oidc.scopes and oidc.displayNameClaim reach the API's config)"
  else
    echo "FAIL: oidc.scopes / oidc.displayNameClaim did not reach the API's config" >&2
    fail=1
  fi
refuses "a hostAliases entry with no hostname" "missing property 'hostnames'" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set "api.console.hostAliases[0].ip=10.96.0.50"
refuses "an OIDC egress peer with no pod selector" "missing property 'podLabels'" \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set "api.console.networkPolicy.oidcPeers[0].namespace=traefik" \
  --set "api.console.networkPolicy.oidcPeers[0].port=8443"

# AND THE SAME VALUES, SET RIGHT, RENDER WHAT THEY SAY. One shared console with
# every PoC gap value: the CA bundle from a Secret, a host alias, an OIDC egress
# peer by selector, the ingress by its Service, and the connection objects in
# the release namespace, labelled with the first workspace.
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
  --set api.console.oidc.caBundle.secret=issuer-ca --set api.console.oidc.systemRoots=false \
  --set "api.console.hostAliases[0].ip=10.96.0.50" --set "api.console.hostAliases[0].hostnames[0]=idp.example.com" \
  --set api.console.networkPolicy.enabled=true --set api.console.networkPolicy.ingressNamespace=traefik \
  --set api.console.networkPolicy.ingressPodLabels.app\\.kubernetes\\.io/name=traefik \
  --set "api.console.networkPolicy.oidcPeers[0].namespace=traefik" \
  --set "api.console.networkPolicy.oidcPeers[0].podLabels.app\\.kubernetes\\.io/name=traefik" \
  --set "api.console.networkPolicy.oidcPeers[0].port=8443" \
  --set api.console.trustedProxyService.namespace=traefik --set api.console.trustedProxyService.name=traefik \
  --set api.console.requireTrustedProxy=true \
  --set kafka.enabled=true --set-string kafka.bootstrapServers=b1:9092 --set minio.enabled=true \
  > "$tmp/console-gaps.yaml" 2> "$tmp/console-gaps.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: the shared console with the PoC gap values did NOT render; rc=$rc" >&2
  sed 's/^/      /' "$tmp/console-gaps.err" >&2
  fail=1
else
  gaps_ok=1
  for needle in \
    'caBundleFile: /var/run/logweir/oidc-ca/ca.crt' \
    'systemRoots: false' \
    'secretName: issuer-ca' \
    'mountPath: /var/run/logweir/oidc-ca' \
    'ip: 10.96.0.50' \
    'trustedProxyService:' \
    'name: logweir-api-trusted-proxy' \
    'resources: ["endpointslices"]' \
    'port: 8443'; do
    if ! grep -F -q -- "$needle" "$tmp/console-gaps.yaml"; then
      echo "FAIL: the PoC gap render does not carry \`$needle\`" >&2
      gaps_ok=0
      fail=1
    fi
  done
  # The two connection objects land in the release namespace, in workspace team-a.
  kc_ns="$(awk '/^kind: KafkaCluster$/{k=1} k && /^  namespace:/{print $2; exit}' "$tmp/console-gaps.yaml")"
  s3_ns="$(awk '/^kind: Secret$/{k=1} k && /^  name: logweir-s3$/{n=1} n && /^  namespace:/{print $2; exit} /^---/{k=0;n=0}' "$tmp/console-gaps.yaml")"
  kc_ws="$(awk '/^kind: KafkaCluster$/{k=1} k && /logweir.dev\/workspace:/{print $2; exit}' "$tmp/console-gaps.yaml")"
  if [ "$kc_ns" != "$NAMESPACE" ] || [ "$s3_ns" != "$NAMESPACE" ] || [ "$kc_ws" != '"team-a"' ]; then
    echo "FAIL: the KafkaCluster landed in '$kc_ns' (workspace $kc_ws) and logweir-s3 in '$s3_ns'; both belong in $NAMESPACE, workspace team-a" >&2
    gaps_ok=0
    fail=1
  fi
  if [ "$gaps_ok" -eq 1 ]; then
    echo "   rc=$rc  (the PoC gap values render: CA bundle, host alias, egress peer, proxy Service, connections in workspace team-a)"
  fi
fi

# AND THE TWO SUPPORTED SHAPES STILL RENDER — a gate whose every arm refuses
# proves only that the template fails. The Service is the interesting half: the
# in-cluster administrator mode must render NONE, and shared mode must render
# one, because that is what keeps a loopback listener from also publishing an
# object every monitor reads as an outage and every pod can dial.
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_LOCAL[@]}" "${CONSOLE_KEY[@]}" \
  --set api.console.networkPolicy.enabled=true \
  > "$tmp/console-local.yaml" 2> "$tmp/console-local.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: the in-cluster administrator mode did NOT render; rc=$rc" >&2
  sed 's/^/      /' "$tmp/console-local.err" >&2
  fail=1
else
  grep -F -q "name: $RELEASE-api" "$tmp/console-local.yaml"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: the in-cluster administrator render carries no $RELEASE-api object at all" >&2
    fail=1
  fi
  awk '/^kind: Service$/ { inservice = 1; next }
       /^kind: /         { inservice = 0 }
       inservice && /^  name: / { print }' "$tmp/console-local.yaml" > "$tmp/console-local-services.txt"
  grep -F -q "$RELEASE-api" "$tmp/console-local-services.txt"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "FAIL: the in-cluster administrator mode rendered a Service named $RELEASE-api." >&2
    echo "      That listener is loopback-only, so the Service would have populated Endpoints" >&2
    echo "      and refuse every connection: a security property to a reader and an outage to" >&2
    echo "      every monitor. \`shared\` is the only mode that may be exposed through one." >&2
    fail=1
  else
    echo "   rc=0  (the in-cluster administrator mode renders no Service)"
  fi
  grep -F -q "ingress: []" "$tmp/console-local.yaml"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: the in-cluster administrator mode's NetworkPolicy does not deny ingress outright" >&2
    fail=1
  else
    echo "   rc=0  (…and its NetworkPolicy admits nothing)"
  fi
fi
render \
  "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" \
  --set-string api.console.publicBaseUrl=https://console.example.com \
  --set api.console.ingress.enabled=true --set api.console.ingress.host=console.example.com \
  --set api.console.ingress.tlsSecretName=logweir-console-tls \
  --set api.console.trustedProxyService.namespace=traefik --set api.console.trustedProxyService.name=traefik \
  > "$tmp/console-supported.yaml" 2> "$tmp/console-supported.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: the supported shared-console configuration did NOT render; rc=$rc" >&2
  sed 's/^/      /' "$tmp/console-supported.err" >&2
  fail=1
else
  grep -F -q "secretName: \"logweir-console-tls\"" "$tmp/console-supported.yaml"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: the supported shared-console render carries no TLS secretName" >&2
    fail=1
  fi
  awk '/^kind: Service$/ { inservice = 1; next }
       /^kind: /         { inservice = 0 }
       inservice && /^  name: / { print }' "$tmp/console-supported.yaml" > "$tmp/console-shared-services.txt"
  grep -F -q "$RELEASE-api" "$tmp/console-shared-services.txt"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: shared mode rendered no Service named $RELEASE-api for the Ingress to back" >&2
    fail=1
  else
    echo "   rc=0  (the supported shared console renders, with its TLS Secret and its Service)"
  fi
fi

# ---- 9. EVERY APPROVAL-POLICY DOCUMENT THE BINARY REFUSES, THE CHART REFUSES ----
#
# A document `ApprovalPolicySet::parse` refuses stops the WHOLE controller at
# start (PLAT-19.2 review M3). The cases are listed once, in
# `scripts/approval-policy-refusals/`; `chart_lint.rs` proves the binary refuses
# each, and this proves `helm template` does -- through the schema AND, where
# this helm can skip the schema, through the template's own `fail` arms alone,
# so neither layer is the only guard.
echo "== 9. every approval-policy document the binary refuses, the chart refuses =="
skip_schema=""
if helm template --help 2>/dev/null | grep -F -q -- "--skip-schema-validation"; then
  skip_schema="--skip-schema-validation"
fi
for case_file in scripts/approval-policy-refusals/*.values.yaml; do
  expect="$(sed -n 's/^# expect: //p' "$case_file")"
  label="approval policy $(basename "$case_file" .values.yaml)"
  refuses "$label" "$expect" -f "$case_file"
  if [ -n "$skip_schema" ]; then
    refuses "$label (template arms only)" "$expect" "$skip_schema" -f "$case_file"
  fi
done

# The three PoC-gap refusals the schema answers first are ALSO the template's
# own, so a helm that skips the schema still refuses them by name.
if [ -n "$skip_schema" ]; then
  refuses "an issuer CA bundle with no key (template arms only)" "caBundle.key is empty" \
    "$skip_schema" "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
    --set api.console.oidc.caBundle.configMap=ca --set-string api.console.oidc.caBundle.key=
  refuses "a hostAliases entry with no hostname (template arms only)" "at least one hostname" \
    "$skip_schema" "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
    --set "api.console.hostAliases[0].ip=10.96.0.50"
  refuses "an OIDC egress peer with no pod selector (template arms only)" "oidcPeers entries need" \
    "$skip_schema" "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
    --set "api.console.networkPolicy.oidcPeers[0].namespace=traefik" \
    --set "api.console.networkPolicy.oidcPeers[0].port=8443"
  refuses "a display-name claim that is not a plain claim name (template arms only)" "plain name of one ID-token claim" \
    "$skip_schema" "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
    --set-string api.console.oidc.displayNameClaim=https://idp.example.com/claims/name
  refuses "an OIDC scope with whitespace (template arms only)" "a scope is one word" \
    "$skip_schema" "${CONSOLE_ON[@]}" "${CONSOLE_KEY[@]}" "${CONSOLE_SHARED[@]}" "${SHARED_URL[@]}" \
    --set-json 'api.console.oidc.scopes=["openid","offline access"]'
fi

# CHART GAP G4 — THE PUBLISHED PACKAGE, BUILT HERE WITH THE REAL HELM. The
# publish step (`scripts/ci-images.sh chart`, images.yml's promote job) runs
# only on a main push or a release tag; this arm builds the same package from
# this tree, offline, and checks what an installer of it gets.
echo "== 10. the chart package the publish step pushes =="
pkg_sha="$(printf 'a%.0s' $(seq 1 40))"
pkg_dir="$tmp/package"
package="$(GITHUB_SHA="$pkg_sha" NS=vladyslavhaina TAG="sha-$pkg_sha" \
  bash scripts/ci-images.sh chart-package "$pkg_dir" 2> "$tmp/package.err")"
rc=$?
if [ "$rc" -ne 0 ] || [ ! -f "$package" ]; then
  echo "FAIL: scripts/ci-images.sh chart-package did not produce a package; rc=$rc" >&2
  sed 's/^/      /' "$tmp/package.err" >&2
  fail=1
else
  helm show chart "$package" > "$tmp/package-chart.yaml" 2>&1
  helm template logweir "$package" -n "$NAMESPACE" --set identity.renderedOffline=false --set api.enabled=true --set api.console.enabled=true \
    --set api.console.mode=localAdmin --set api.console.keySecret=k \
    > "$tmp/package-render.yaml" 2> "$tmp/package-render.err"
  rc=$?
  pkg_ok=1
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: the packaged chart does not render; rc=$rc" >&2
    sed 's/^/      /' "$tmp/package-render.err" >&2
    pkg_ok=0
  fi
  for needle in "name: logweir-chart" "version: 0.1.0-sha-$pkg_sha" "appVersion: sha-$pkg_sha"; do
    if ! grep -F -x -q -- "$needle" "$tmp/package-chart.yaml"; then
      echo "FAIL: the package's Chart.yaml lacks \`$needle\`" >&2
      pkg_ok=0
    fi
  done
  for image in weirkeeper logweir logweir-console; do
    if ! grep -F -q "docker.io/vladyslavhaina/$image:sha-$pkg_sha" "$tmp/package-render.yaml"; then
      echo "FAIL: the packaged chart does not install $image at :sha-$pkg_sha" >&2
      pkg_ok=0
    fi
  done
  if grep -F -q ':latest' "$tmp/package-render.yaml"; then
    echo "FAIL: the packaged chart still installs a :latest image" >&2
    pkg_ok=0
  fi
  long_label="$(awk '/helm.sh\/chart:/ { v = $2; if (length(v) > 63) print v }' "$tmp/package-render.yaml" | head -1)"
  if [ -n "$long_label" ]; then
    echo "FAIL: a helm.sh/chart label is longer than 63 characters: $long_label" >&2
    pkg_ok=0
  fi
  if [ "$pkg_ok" -eq 1 ]; then
    echo "   ok: logweir-chart 0.1.0-sha-<commit>, appVersion sha-<commit>, the three images at that tag, labels <= 63"
  else
    fail=1
  fi
fi

echo "== 11. the enforcement image, the console's grants and who may create a TrustPolicy =="

# The retention enforcement Job runs `logweir-retention` from the runner image
# (defect RET-NOIMAGE: without it the Job exits 127 at the kubelet). Three
# links, each read from the file that owns it: the Job's command and its image
# (retention_policy.rs), and the runner image building and shipping the binary
# (Dockerfile). Whole-line comments are stripped first, so the prose that
# explains the defect cannot satisfy the check. It reads spellings; images.yml
# runs the binary out of the built image.
check_enforcement_image() {
  retention_src="crates/weirkeeper/src/controllers/retention_policy.rs"
  controller="$(sed '/^[[:space:]]*\/\//d' "$retention_src")"
  dockerfile="$(sed '/^[[:space:]]*#/d' Dockerfile)"

  case "$controller" in
    *'RETENTION_BINARY: &str = "logweir-retention"'*) ;;
    *)
      echo "check-chart: $retention_src no longer names \`logweir-retention\` as the" >&2
      echo "  enforcement Job's binary. If the binary was renamed, rename it in Dockerfile" >&2
      echo "  and in .github/workflows/images.yml too — this check is the link between them." >&2
      return 1
      ;;
  esac

  case "$controller" in
    *'image: self.ctx.runner_image.image'*) ;;
    *)
      echo "check-chart: $retention_src no longer takes the enforcement Job's image from" >&2
      echo "  \`self.ctx.runner_image\`. The check below proves the RUNNER image carries" >&2
      echo "  \`logweir-retention\`; if enforcement now names a different image, this script" >&2
      echo "  must check THAT image's recipe instead, or the chart goes back to" >&2
      echo "  installing an enforcement path that exits 127." >&2
      return 1
      ;;
  esac

  case "$dockerfile" in
    *'-p logweir-retention'*) ;;
    *)
      echo "check-chart: Dockerfile does not build \`-p logweir-retention\`." >&2
      echo "  The controller this file installs creates enforcement Jobs that run" >&2
      echo "  \`logweir-retention\` from the image Dockerfile builds. Without that package" >&2
      echo "  the Job dies at the kubelet with exitCode 127 (\`executable file not found" >&2
      echo "  in \$PATH\`) — observed live, defect RET-NOIMAGE. Refusing a chart" >&2
      echo "  whose enforcement path cannot start." >&2
      return 1
      ;;
  esac

  case "$dockerfile" in
    *'/usr/local/bin/logweir-retention'*) ;;
    *)
      echo "check-chart: Dockerfile builds \`logweir-retention\` but COPYs it nowhere on" >&2
      echo "  PATH. The binary must land at /usr/local/bin/logweir-retention: that is the" >&2
      echo "  bare name the controller sets as the container's command, and \$PATH is how" >&2
      echo "  the kubelet resolves it. Refusing to render." >&2
      return 1
      ;;
  esac
}

# The console principal can do exactly what the console does, in every
# rendered variant (`kubectl auth can-i`, offline, both ways): every
# (verb, resource) the sealed adapter spends is granted, so no route 403s, and
# every grant is one it spends, on the one service that holds `create` on
# Secrets. The roles are found through every binding that reaches the account,
# never by name, and such a binding must name the account and nobody else.
# `chart_lint.rs` holds the same renders to the same table from Rust.
CONSOLE_NAMESPACE="logweir-system"
CONSOLE_ACCOUNT="logweir-api"

# One line per binding that reaches `system:serviceaccount:<ns>:<sa>` — as the
# account, as its user name, or through a group every ServiceAccount token
# carries:
#   ROLE <ClusterRole|Role> <namespace, or - for a ClusterRole> <name>
#   SUBJECTS <kind>/<name>   when that binding names anyone besides the account
console_bindings() {
  awk -v ns="$CONSOLE_NAMESPACE" -v sa="$CONSOLE_ACCOUNT" '
    function val(   v) { v = $0; sub(/^[^:]*:[ ]*/, "", v); gsub(/["'\'']/, "", v); return v }
    function reaches(i) {
      if (skind[i] == "ServiceAccount") return sname[i] == sa && sns[i] == ns
      if (skind[i] == "User")           return sname[i] == "system:serviceaccount:" ns ":" sa
      if (skind[i] == "Group")          return sname[i] == "system:authenticated" || sname[i] == "system:serviceaccounts" || sname[i] == "system:serviceaccounts:" ns
      return 1
    }
    function flush(   i, hit) {
      if (kind != "") {
        hit = 0
        for (i = 1; i <= nsub; i++) if (reaches(i)) hit = 1
        if (hit) {
          if (!(nsub == 1 && skind[1] == "ServiceAccount" && sname[1] == sa && sns[1] == ns))
            print "SUBJECTS " kind "/" bname
          print "ROLE " rkind " " (rkind == "Role" ? bns : "-") " " rname
        }
      }
      kind = ""; bname = ""; bns = ""; rkind = ""; rname = ""; nsub = 0; sect = ""
    }
    /^---/                         { flush(); next }
    /^ *#/                         { next }
    /^kind: (Cluster)?RoleBinding$/ { kind = val(); next }
    /^metadata:$/                  { sect = "meta"; next }
    /^roleRef:$/                   { sect = "ref"; next }
    /^subjects:$/                  { sect = "subj"; next }
    /^[^ ]/                        { sect = ""; next }
    sect == "meta" && /^  name:/      { bname = val(); next }
    sect == "meta" && /^  namespace:/ { bns = val(); next }
    sect == "ref"  && /^  kind:/      { rkind = val(); next }
    sect == "ref"  && /^  name:/      { rname = val(); next }
    sect == "subj" && /^  - / {
      nsub++; skind[nsub] = ""; sname[nsub] = ""; sns[nsub] = ""
      sub(/^  - /, "    ")
    }
    sect == "subj" && /^    kind:/      { skind[nsub] = val(); next }
    sect == "subj" && /^    name:/      { sname[nsub] = val(); next }
    sect == "subj" && /^    namespace:/ { sns[nsub] = val(); next }
    END { flush() }
  ' "$1"
}

# Every `verb resource[@name]` pair the roles listed in $1 (lines of
# `<kind> <namespace|-> <name>`) grant in the render $2. A rule with
# `resourceNames` yields one pair per name, so a roster grant that drops its
# name list reads as `get trustrosters` and is refused by the sheet. A rule
# naming `nonResourceURLs` yields `verb url:<path>`, which no sheet carries.
console_grants() {
  awk '
    function val(   v) { v = $0; sub(/^[^:]*:[ ]*/, "", v); gsub(/["'\'']/, "", v); return v }
    function inline(arr,   t, c, i, n) {
      t = $0; sub(/^[^[]*\[/, "", t); sub(/\].*$/, "", t); gsub(/[",'\'']/, " ", t)
      c = split(t, parts, " "); n = 0
      for (i = 1; i <= c; i++) if (parts[i] != "") arr[++n] = parts[i]
      return n
    }
    function emit(   i, j, k) {
      if (!inrule) return
      for (i = 1; i <= nv; i++) {
        for (j = 1; j <= nu; j++) print verbs[i] " url:" urls[j]
        for (j = 1; j <= nr; j++) {
          if (nn == 0) print verbs[i] " " res[j]
          for (k = 1; k <= nn; k++) print verbs[i] " " res[j] "@" names[k]
        }
      }
      inrule = 0
    }
    function key(k) {
      coll = ""
      if ($0 ~ /\[/) {
        if (k == "resources") nr = inline(res)
        else if (k == "resourceNames") nn = inline(names)
        else if (k == "verbs") nv = inline(verbs)
        else if (k == "nonResourceURLs") nu = inline(urls)
      } else coll = k
    }
    function reset_doc() { emit(); kind = ""; name = ""; ns = ""; sect = ""; wanted = 0 }
    FNR == NR { bound[$1 " " $2 " " $3] = 1; next }
    /^---/                             { reset_doc(); next }
    /^ *#/                             { next }
    /^kind: /                          { kind = val(); next }
    /^metadata:$/                      { sect = "meta"; next }
    /^rules:$/ {
      sect = "rules"
      wanted = (kind == "ClusterRole" && (("ClusterRole - " name) in bound)) || \
               (kind == "Role" && (("Role " ns " " name) in bound))
      if (wanted) seen[kind " " (kind == "Role" ? ns : "-") " " name] = 1
      next
    }
    /^aggregationRule:/                { if (kind == "ClusterRole" && (("ClusterRole - " name) in bound)) print "aggregated role:" name; sect = ""; next }
    /^[^ ]/                            { emit(); sect = ""; next }
    sect == "meta" && /^  name:/       { name = val(); next }
    sect == "meta" && /^  namespace:/  { ns = val(); next }
    sect == "rules" && wanted && /^  - / {
      emit(); inrule = 1; nr = 0; nn = 0; nv = 0; nu = 0; coll = ""
      sub(/^  - /, "    ")
    }
    sect == "rules" && wanted && /^    resources:/       { key("resources"); next }
    sect == "rules" && wanted && /^    resourceNames:/   { key("resourceNames"); next }
    sect == "rules" && wanted && /^    verbs:/           { key("verbs"); next }
    sect == "rules" && wanted && /^    nonResourceURLs:/ { key("nonResourceURLs"); next }
    sect == "rules" && wanted && /^    apiGroups:/       { coll = "apiGroups"; if ($0 ~ /\[/) coll = ""; next }
    sect == "rules" && wanted && /^      - / {
      v = $0; sub(/^      - /, "", v); gsub(/["'\'']/, "", v)
      if (coll == "resources") res[++nr] = v
      else if (coll == "resourceNames") names[++nn] = v
      else if (coll == "verbs") verbs[++nv] = v
      else if (coll == "nonResourceURLs") urls[++nu] = v
      next
    }
    END {
      reset_doc()
      for (b in bound) if (!(b in seen)) print "unrendered role:" b
    }
  ' "$1" "$2"
}

check_console_grants() {
  demo="charts/logweir/rendered/demo.yaml"
  adapter="crates/logweir-api/src/kube.rs"
  dir="$(mktemp -d "${TMPDIR:-/tmp}/logweir-console-cani.XXXXXX")"

  # THE ANSWER SHEET: one line per question, and the whole of what may be
  # answered `yes`. The eight kinds sealed into `ProductResource`, all eight
  # with a POST route (`approvals` since PLAT-19.2: the console creates the
  # Approval its ordinary confirmation IS, the governed confirmation object and
  # the approver's countersigned Approval on `POST .../restores/{name}/approval`),
  # the two named merge patches plus the two `CancellableCheck`
  # kinds, D3 W11's three namespaced reads and its ONE write, the cluster-scoped
  # `TrustPolicy` read, the ONE roster read (`get trustrosters@default`: the
  # rule's `resourceNames` is part of the pair, so the name list cannot be
  # dropped — PREFLIGHT-TRUSTROSTER-STALE), and the two core objects with one
  # verb each.
  printf '%s\n' \
    'get approvals'          'list approvals'          'create approvals' \
    'get backupdestinations' 'list backupdestinations' 'create backupdestinations' 'patch backupdestinations' \
    'get backups'            'list backups'            'create backups' \
    'get backupschedules'    'list backupschedules'    'create backupschedules'    'patch backupschedules' \
    'get kafkaclusters'      'list kafkaclusters'      'create kafkaclusters' \
    'get preflights'         'list preflights'         'create preflights'         'patch preflights' \
    'get restores'           'list restores'           'create restores' \
    'get switchovers'        'list switchovers'        'create switchovers' \
    'get topicdiscoveries'   'list topicdiscoveries'   'create topicdiscoveries'   'patch topicdiscoveries' \
    'get protectionpolicies' 'list protectionpolicies' \
    'get recoverycatalogs'   'list recoverycatalogs'   'create recoverycatalogs' \
    'get retentionpolicies'  'list retentionpolicies' \
    'get trustpolicies'      'list trustpolicies' \
    'get trustrosters@default' \
    'get configmaps' \
    'create secrets' \
    > "$dir/expected.raw"
  sort -u "$dir/expected.raw" > "$dir/expected"

  # EVERY RENDERED VARIANT, not only the demo: a variant that turns
  # `api.enabled` on must grant exactly the sheet, and one that leaves it off
  # must grant the account nothing at all. The demo must grant something — a
  # gate that reads nothing answers `yes` to every question.
  for render_file in charts/logweir/rendered/*.yaml; do
    console_bindings "$render_file" > "$dir/bindings"
    awk '/^SUBJECTS / { print $2 }' "$dir/bindings" > "$dir/bad-subjects"
    if [ -s "$dir/bad-subjects" ]; then
      echo "check-chart: a binding in $render_file hands the console's grant to someone else." >&2
      echo "  Each binding below names system:serviceaccount:$CONSOLE_NAMESPACE:$CONSOLE_ACCOUNT" >&2
      echo "  (or a group it belongs to) AND another subject; the other subject then holds" >&2
      echo "  every verb the console holds. Its subjects must be the account and nothing else:" >&2
      sed 's/^/    /' "$dir/bad-subjects" >&2
      rm -rf "$dir"
      return 1
    fi
    awk '/^ROLE / { print $2, $3, $4 }' "$dir/bindings" > "$dir/roles"
    # An empty role list is no grant at all — and `awk`'s `FNR == NR` idiom
    # would read the render as the list, so it is not handed one.
    : > "$dir/granted.raw"
    if [ -s "$dir/roles" ]; then
      console_grants "$dir/roles" "$render_file" > "$dir/granted.raw"
    fi
    sort -u "$dir/granted.raw" > "$dir/granted"

    # CHART GAP G6 — THE ONE CONDITIONAL PAIR. A render whose console
    # configuration names `trustedProxyService` spends `list endpointslices`
    # (`KubeAdapter::list_service_endpoints`, pinned by `linkage.rs` as
    # `list_page endpointslices`), and must be granted it; a render that does
    # not must not be. The pair is the variant's, so the sheet is too.
    cp "$dir/expected" "$dir/expected.variant"
    if grep -q '^    trustedProxyService:$' "$render_file"; then
      case "$(sed '/^[[:space:]]*\/\//d' "$adapter")" in
        *'"list_page", "endpointslices"'*) ;;
        *)
          rm -rf "$dir"
          echo "check-chart: $render_file configures trustedProxyService but $adapter no" >&2
          echo "  longer spends \`list_page endpointslices\`; the grant would be one nobody uses." >&2
          return 1
          ;;
      esac
      printf '%s\n' 'list endpointslices' >> "$dir/expected.variant"
      sort -u "$dir/expected.variant" -o "$dir/expected.variant"
    fi

    # PROD-16.1 — THE SECOND CONDITIONAL PAIR. A render whose console
    # configuration names `installationIdentity` spends `get configmaps` on
    # that ONE public identity ConfigMap (`KubeAdapter::get_installation_identity`,
    # the fresh-install marker read), and must be granted exactly it.
    if grep -q '^    installationIdentity:$' "$render_file"; then
      case "$(sed '/^[[:space:]]*\/\//d' "$adapter")" in
        *'pub async fn get_installation_identity'*) ;;
        *)
          rm -rf "$dir"
          echo "check-chart: $render_file configures installationIdentity but $adapter no" >&2
          echo "  longer reads it (\`get_installation_identity\`); the grant would be one nobody uses." >&2
          return 1
          ;;
      esac
      identity_cm="$(sed -n 's/^      publicConfigMap: *//p' "$render_file" | head -1)"
      printf '%s\n' "get configmaps@$identity_cm" >> "$dir/expected.variant"
      sort -u "$dir/expected.variant" -o "$dir/expected.variant"
    fi

    if [ ! -s "$dir/granted" ]; then
      if [ "$render_file" = "$demo" ]; then
        rm -rf "$dir"
        echo "check-chart: no console grant was found in $demo." >&2
        echo "  Either \`api.enabled\` is no longer on in charts/logweir/examples/demo.values.yaml," >&2
        echo "  or the render is stale (\`bash scripts/check-chart.sh --write\`). A gate that reads" >&2
        echo "  nothing answers \`yes\` to every question." >&2
        return 1
      fi
      continue
    fi

    # The wider direction first: a grant nobody spends is the finding that
    # matters more when a rule both loses and gains a pair (a dropped
    # `resourceNames` turns `get trustrosters@default` into `get trustrosters`).
    comm -13 "$dir/expected.variant" "$dir/granted" > "$dir/extra"
    if [ -s "$dir/extra" ]; then
      echo "check-chart: the console principal can do MORE than the console does ($render_file)." >&2
      echo "  Each pair below is a \`yes\` to a question no route asks — a capability nobody" >&2
      echo "  audits, on the one service that holds \`create\` on Secrets. It is reached" >&2
      echo "  through SOME binding of the account, whatever the role is called:" >&2
      sed 's/^/    /' "$dir/extra" >&2
      echo "  Narrow charts/logweir/templates/ui/api-rbac.yaml, or add the pair to the answer" >&2
      echo "  sheet in this function TOGETHER WITH the route that spends it." >&2
      rm -rf "$dir"
      return 1
    fi
    comm -23 "$dir/expected.variant" "$dir/granted" > "$dir/missing"
    if [ -s "$dir/missing" ]; then
      echo "check-chart: the console principal CANNOT do what the console does ($render_file)." >&2
      echo "  \`kubectl auth can-i\` would answer \`no\` for each pair below, and each one is a" >&2
      echo "  route that 403s in production while every route-table test stays green:" >&2
      sed 's/^/    /' "$dir/missing" >&2
      echo "  Widen charts/logweir/templates/ui/api-rbac.yaml deliberately, say why in its" >&2
      echo "  header, and run \`bash scripts/check-chart.sh --write\`." >&2
      rm -rf "$dir"
      return 1
    fi

    if [ "$render_file" = "$demo" ]; then
      cp "$dir/granted" "$dir/demo-granted"
    fi
  done
  if [ ! -s "$dir/demo-granted" ]; then
    rm -rf "$dir"
    echo "check-chart: $demo was not audited; is it missing?" >&2
    return 1
  fi

  # THE FOUR VERBS ARE STILL THE ADAPTER'S FOUR. The sheet above is a list this
  # file owns; these two arms read the ADAPTER, so a fifth kube verb reaching it
  # cannot be authorised by editing one list.
  adapter_src="$(sed '/^[[:space:]]*\/\//d' "$adapter")"
  case "$adapter_src" in
    *'pub trait ProductResource'*) ;;
    *)
      rm -rf "$dir"
      echo "check-chart: $adapter no longer seals its custom resources behind" >&2
      echo "  \`ProductResource\`. The console's ClusterRole is written from that seal; without" >&2
      echo "  it there is nothing to hold the grant to, and the answer sheet in this function" >&2
      echo "  becomes a list nobody derives." >&2
      return 1
      ;;
  esac
  for verb in delete deletecollection watch update; do
    case "$(cat "$dir/demo-granted")" in
      *"$verb "*)
        rm -rf "$dir"
        echo "check-chart: the console principal is granted \`$verb\`." >&2
        echo "  The sealed adapter spends four kube verbs — list, get, create, patch — and has" >&2
        echo "  no method that could issue this one. \`watch\` would additionally be a" >&2
        echo "  long-lived connection per browser tab, and D3 §10's event stream is server-sent" >&2
        echo "  events over this service's own reads. Remove the verb." >&2
        return 1
        ;;
    esac
  done
  rm -rf "$dir"
  echo "check-chart: the console principal's grants are the sealed adapter's, both ways,"
  echo "  through every binding that reaches it, in every rendered variant."
}

# Who may create a TrustPolicy (PROD-16.1 security review). `create` cannot be
# narrowed by name, so in every rendered variant only two ClusterRoles may hold
# it: the human `logweir-trust-admin`, and the identity hook's
# `<release>-identity-trust` as an install-only, self-deleting post-install hook.
check_trust_creators() {
  for render_file in charts/logweir/rendered/*.yaml; do
    awk -v file="$render_file" '
      function flush() {
        if (kind == "ClusterRole" && creates) {
          if (name == "logweir-trust-admin") {
            # the human role, unbound by the chart
          } else if (name ~ /-identity-trust$/) {
            if (hook != "post-install" || policy !~ /hook-succeeded/ || policy !~ /hook-failed/) {
              print file ": ClusterRole " name " grants create on trustpolicies as a standing grant (helm.sh/hook=" hook ", delete-policy=" policy "); it must be a post-install hook deleted when the install hooks finish"
            }
          } else {
            print file ": ClusterRole " name " grants create on trustpolicies; only logweir-trust-admin and the install-only identity hook may"
          }
        }
        kind = ""; name = ""; hook = ""; policy = ""; creates = 0; trust_rule = 0
      }
      /^---/ { flush(); next }
      /^kind: / { kind = $2 }
      /^  name: / { if (name == "") name = $2 }
      /^    helm.sh\/hook: / { hook = $2 }
      /^    helm.sh\/hook-delete-policy: / { policy = $2 }
      /^ *resources:/ { trust_rule = ($0 ~ /"trustpolicies"/) }
      /^ *- trustpolicies$/ { trust_rule = 1 }
      /^ *verbs:/ { if (trust_rule && $0 ~ /"create"/) creates = 1; trust_rule = 0 }
      END { flush() }
    ' "$render_file"
  done > "${TMPDIR:-/tmp}/logweir-trust-creators.$$"
  if [ -s "${TMPDIR:-/tmp}/logweir-trust-creators.$$" ]; then
    echo "check-chart: a TrustPolicy creator outside the two allowed holders:" >&2
    sed 's/^/  /' "${TMPDIR:-/tmp}/logweir-trust-creators.$$" >&2
    rm -f "${TMPDIR:-/tmp}/logweir-trust-creators.$$"
    return 1
  fi
  rm -f "${TMPDIR:-/tmp}/logweir-trust-creators.$$"
  # NOT VACUOUS (PROD-16.1 fix round): at least one variant renders the
  # install-only grant (every console variant with the default bootstrap
  # feature), so the rule above reads a real one.
  if ! grep -l -E '^  name: [a-z0-9-]+-identity-trust$' charts/logweir/rendered/*.yaml > /dev/null; then
    echo "check-chart: no rendered variant carries the install-only identity trust grant; the" >&2
    echo "  least-privilege rule above would pass over nothing (examples/console.values.yaml)" >&2
    return 1
  fi
  echo "check-chart: only logweir-trust-admin and the install-only identity hook may create a"
  echo "  TrustPolicy, and the hook's grant is a self-deleting post-install hook, in every variant."
}

check_enforcement_image || fail=1
check_console_grants || fail=1
check_trust_creators || fail=1

# ---------------------------------------------------------------- 12. offline renders
# PROD-14.1. `lookup` decides the identity's create-once objects, and it
# answers nothing under `helm template` and Argo CD. Argo CD applies the empty
# placeholders as PreSync hooks and deletes and re-creates them on every sync
# (its default hook policy), and `kubectl apply` re-creates the install-only
# trust grant that nothing then deletes. So a render that cannot ask the
# cluster is refused unless `identity.renderedOffline` says what it is, and
# `true` renders no placeholder and no grant. Every render here is offline;
# each control is the same render with the other answer, and renders exactly
# what its case says is absent.
echo "== 12. an offline render is refused unless identity.renderedOffline says what it is =="
OFFLINE_SHAPE=(-f "$EXAMPLES/console.values.yaml")
offline_diagnostic="identity.renderedOffline is unset and this render cannot ask the cluster"
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" "${OFFLINE_SHAPE[@]}" > /dev/null 2> "$tmp/offline-unset.err"
rc=$?
if [ "$rc" -eq 0 ] || ! grep -F -q "$offline_diagnostic" "$tmp/offline-unset.err"; then
  echo "FAIL: an offline render with identity.renderedOffline unset was not refused by name (rc=$rc)" >&2
  sed 's/^/      /' "$tmp/offline-unset.err" >&2
  fail=1
else
  echo "   rc=$rc  (an offline render with identity.renderedOffline unset: refused by name)"
fi
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --set identity.enabled=false > /dev/null 2> "$tmp/offline-noidentity.err"
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: control: with identity.enabled=false an offline render must not be refused (rc=$rc)" >&2
  sed 's/^/      /' "$tmp/offline-noidentity.err" >&2
  fail=1
else
  echo "   rc=$rc  (control: identity.enabled=false renders; the refusal is the identity's)"
fi
# One render per answer, and per adoption mode: the offline objects, then the
# preview of the same values as the control.
offline_ok=1
for mode in true false; do
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" "${OFFLINE_SHAPE[@]}" \
    --set "identity.renderedOffline=$mode" > "$tmp/offline-$mode.yaml" 2> "$tmp/offline-$mode.err"
  rc=$?
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" -f "$EXAMPLES/identity-external.values.yaml" \
    --set "identity.renderedOffline=$mode" > "$tmp/offline-external-$mode.yaml" 2> "$tmp/offline-external-$mode.err"
  rc_external=$?
  if [ "$rc" -ne 0 ] || [ "$rc_external" -ne 0 ]; then
    echo "FAIL: identity.renderedOffline=$mode did not render (rc=$rc, external rc=$rc_external)" >&2
    sed 's/^/      /' "$tmp/offline-$mode.err" "$tmp/offline-external-$mode.err" >&2
    offline_ok=0
  fi
done
placeholders() { grep -c '^    logweir.dev/identity-state: uninitialized$' "$1"; }
grant() { grep -c -E "^  name: $RELEASE-identity-trust$" "$1"; }
arg() { grep -c -F -x -- "            - $2" "$1"; }
if [ "$offline_ok" -eq 1 ]; then
  off="$tmp/offline-true.yaml"; pre="$tmp/offline-false.yaml"
  # The offline install: the hook Jobs still run and still manage the console
  # key, but nothing the chart renders can empty a filled object or grant.
  got="placeholders=$(placeholders "$off") grant=$(grant "$off") trust-arg=$(arg "$off" --installation-trust-policy) marker-arg=$(arg "$off" --mark-fresh-install-confirm) console-arg=$(arg "$off" --console-secret-name)"
  if [ "$got" != "placeholders=0 grant=0 trust-arg=0 marker-arg=0 console-arg=1" ]; then
    echo "FAIL: identity.renderedOffline=true must render no placeholder and no trust grant, and still the hook Job; got $got" >&2
    offline_ok=0
  fi
  # Control: the same values as a preview render all four placeholders (the
  # signer, its public record, the console key, its public record), the grant (three names: the ClusterRole, the binding, its roleRef)
  # and both trust arguments.
  got_pre="placeholders=$(placeholders "$pre") grant=$(grant "$pre") trust-arg=$(arg "$pre" --installation-trust-policy) marker-arg=$(arg "$pre" --mark-fresh-install-confirm) console-arg=$(arg "$pre" --console-secret-name)"
  if [ "$got_pre" != "placeholders=4 grant=3 trust-arg=1 marker-arg=1 console-arg=1" ]; then
    echo "FAIL: control: the preview of the same values must render the four placeholders, the grant and both trust arguments; got $got_pre" >&2
    offline_ok=0
  fi
  # Adoption works offline too: the external Secret is still named, and the
  # managed copy is the operator's to create. Control: its preview renders two.
  got_ext="placeholders=$(placeholders "$tmp/offline-external-true.yaml") external-arg=$(arg "$tmp/offline-external-true.yaml" --external-secret-name) preview-placeholders=$(placeholders "$tmp/offline-external-false.yaml")"
  if [ "$got_ext" != "placeholders=0 external-arg=1 preview-placeholders=2" ]; then
    echo "FAIL: identity.externalSecret with identity.renderedOffline=true: expected no placeholder and the adoption argument (preview: two); got $got_ext" >&2
    offline_ok=0
  fi
  # No answer ever renders private key material: no PEM block anywhere, and
  # every identity Secret the preview renders is empty.
  pem="$(cat "$tmp"/offline-*.yaml | grep -c -E 'BEGIN [A-Z ]*PRIVATE KEY|PRIVATE KEY-----')"
  nonempty="$(cat "$tmp"/offline-*.yaml | awk '
    /^---/ { secret = 0; identity = 0 }
    /^kind: Secret$/ { secret = 1 }
    /app.kubernetes.io\/component: (signing-identity|console-confirmation)$/ { identity = 1 }
    /^data:/ { if (secret && identity && $0 != "data: {}") n++ }
    END { print n + 0 }')"
  if [ "$pem" -ne 0 ] || [ "$nonempty" -ne 0 ]; then
    echo "FAIL: an offline or preview render carries key material (PEM lines $pem, identity Secrets with data $nonempty)" >&2
    offline_ok=0
  fi
  [ "$offline_ok" -eq 1 ] && echo "   ok: true renders no placeholder and no grant ($got); the preview control renders them ($got_pre); adoption offline ($got_ext); no key material"
fi
[ "$offline_ok" -eq 1 ] || fail=1

# ---------------------------------------------------------------- 13. kubeVersion
# PROD-14.1: the Kubernetes floor is Chart.yaml's `kubeVersion`, and Helm's own
# check refuses an older cluster on install, upgrade and template (Argo CD
# passes the cluster's version), so the chart adds no `fail` of its own. The
# control is the floor itself.
echo "== 13. Helm refuses a cluster below Chart.yaml's kubeVersion =="
render \
  --kube-version 1.28.15 > /dev/null 2> "$tmp/kube-old.err"
rc=$?
render \
  --kube-version 1.29.0 > /dev/null 2> "$tmp/kube-floor.err"
rc_floor=$?
if [ "$rc" -eq 0 ] || ! grep -F -q "chart requires kubeVersion: >=1.29.0-0" "$tmp/kube-old.err"; then
  echo "FAIL: Kubernetes 1.28.15 was not refused by Chart.yaml's kubeVersion (rc=$rc)" >&2
  sed 's/^/      /' "$tmp/kube-old.err" >&2
  fail=1
elif [ "$rc_floor" -ne 0 ]; then
  echo "FAIL: control: Kubernetes 1.29.0, the floor, did not render (rc=$rc_floor)" >&2
  sed 's/^/      /' "$tmp/kube-floor.err" >&2
  fail=1
else
  echo "   rc=$rc  (Kubernetes 1.28.15 refused by kubeVersion >=1.29.0-0; control: 1.29.0 renders)"
fi

# ---------------------------------------------------------------- 14. restore targets
# poc-14-1a D7: values that list the demo target demoKafka also adds give the
# identity hook each cluster id once, in the order first given.
echo "== 14. the fresh install's allowed restore targets name each cluster id once =="
render \
  -f "$EXAMPLES/demo.values.yaml" --set demoKafka.clusterIds.target=demoTargetClusterId000 \
  --set-json 'identity.installationTrust.allowedTargetClusterIds=["demoTargetClusterId000","other-target"]' \
  > "$tmp/targets.yaml" 2> "$tmp/targets.err"
rc=$?
targets="$(grep -A1 -x -F -- '            - --allowed-target-cluster-id' "$tmp/targets.yaml" \
  | sed -n 's/^            - "\(.*\)"$/\1/p' | paste -sd, -)"
if [ "$rc" -ne 0 ] || [ "$targets" != "demoTargetClusterId000,other-target" ]; then
  echo "FAIL: the demo target, in the values and added by demoKafka, must reach the hook once; got '$targets' (rc=$rc)" >&2
  sed 's/^/      /' "$tmp/targets.err" >&2
  fail=1
else
  echo "   rc=$rc  (allowed targets $targets: the demo target once, in the order given)"
fi

# ---------------------------------------------------------------- 15. the demo archive's PVC
# poc-14-1a B1: `helm uninstall` keeps an object only when the manifest Helm
# STORED says `helm.sh/resource-policy: keep`; a hand-set annotation is ignored.
# minio.persistence.keep renders it on the demo MinIO's PVC and changes nothing
# else. The control is the same render with keep=false: the diff is the two lines.
echo "== 15. minio.persistence.keep keeps the demo MinIO's PVC through helm uninstall =="
for keep in true false; do
  render \
    --set minio.enabled=true --set "minio.persistence.keep=$keep" > "$tmp/keep-$keep.yaml" 2> "$tmp/keep-$keep.err" \
    || { echo "FAIL: minio.persistence.keep=$keep did not render" >&2; sed 's/^/      /' "$tmp/keep-$keep.err" >&2; fail=1; }
done
kept="$(diff "$tmp/keep-false.yaml" "$tmp/keep-true.yaml" | grep '^[<>]' | paste -sd'|' -)"
pvc_kept="$(awk '/^---/ { pvc = 0 } /^kind: PersistentVolumeClaim$/ { pvc = 1 }
  pvc && $0 == "    helm.sh/resource-policy: keep" { n++ } END { print n + 0 }' "$tmp/keep-true.yaml")"
if [ "$kept" != ">   annotations:|>     helm.sh/resource-policy: keep" ] || [ "$pvc_kept" -ne 1 ]; then
  echo "FAIL: minio.persistence.keep=true must add only the keep annotation, on the MinIO PVC; diff from false '$kept', on the PVC $pvc_kept" >&2
  fail=1
else
  echo "   ok: keep=true adds only helm.sh/resource-policy: keep, on the MinIO PVC; control: keep=false renders none"
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "FAIL: the chart is not what the tree says it is; the lines above name what drifted." >&2
  exit 1
fi
echo "ok: charts/logweir — no copy of the page (it is the console image's),"
echo "    helm lint clean, rendered files current, every image a digest except the three Logweir"
echo "    images at :$LOGWEIR_TAG (author-only exempt by name), the schema refuses a non-boolean"
echo "    flag, values.yaml names the tree's own repositories at :$LOGWEIR_TAG, and every"
echo "    console configuration that would install a console that looks like it works is refused;"
echo "    the runner image ships the enforcement binary, the console principal's grants are the"
echo "    adapter's both ways, and only the two allowed holders may create a TrustPolicy."
exit 0
