#!/usr/bin/env bash
# Validate chart CRDs, rendering, image references and value types.
# Use --write to refresh snapshots; default checks do not change tracked files.
set -uo pipefail
# --write explicitly refreshes committed render snapshots. Normal checks never edit files.
write=false
case "${1:-}" in
  --write) write=true ;;
  "") ;;
  *) echo "usage: check-chart.sh [--write]" >&2; exit 2 ;;
esac
cd "$(dirname "$0")/.."

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

# ---------------------------------------------------------------- 1. CRDs
# The kind count of `weirkeeper::crds::KINDS`: six from ADR 0008 Amendment A,
# Amendment F's `BackupDestination`, `TopicDiscovery` and `Preflight`, and
# Amendment G's `TrustPolicy`, `ProtectionPolicy`, `RehearsalSchedule`,
# `RecoveryCatalog` and `RetentionPolicy`.
EXPECTED_CRDS=14
echo "== 1. crds/ is a byte-identical copy of config/crd/ =="
crd_count=0
for src in config/crd/*.yaml; do
  base="$(basename "$src")"
  [ "$base" = "kustomization.yaml" ] && continue
  crd_count=$((crd_count + 1))
  if [ ! -f "$CHART/crds/$base" ]; then
    echo "FAIL: $CHART/crds/$base is missing (config/crd/$base exists)" >&2
    fail=1
    continue
  fi
  cmp -s "$src" "$CHART/crds/$base"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: $CHART/crds/$base is not byte-identical to $src (cmp rc=$rc). Copy it:" >&2
    echo "      cp $src $CHART/crds/$base" >&2
    fail=1
  fi
done
for extra in "$CHART"/crds/*.yaml; do
  base="$(basename "$extra")"
  if [ ! -f "config/crd/$base" ]; then
    echo "FAIL: $CHART/crds/$base has no counterpart under config/crd/ — a new kind arrives in the emitter first" >&2
    fail=1
  fi
done
# THE NUMBER IS WRITTEN DOWN ON PURPOSE. The loops above prove the two
# directories AGREE; only a literal count proves neither of them quietly lost a
# kind. It tracks `weirkeeper::crds::KINDS`, and changing it is a reviewable
# diff on this line.
[ "$crd_count" -eq "$EXPECTED_CRDS" ] || { echo "FAIL: expected $EXPECTED_CRDS CRDs under config/crd/, found $crd_count" >&2; fail=1; }
[ "$fail" -eq 0 ] && echo "   ok: $crd_count CRDs, byte for byte"

# ---------------------------------------------------------------- 2. the UI
# THE ARM IS GONE AND SO IS WHAT IT GUARDED. Task 39 moved the page into the
# `logweir-ui` image; `charts/logweir/ui/` no longer exists, so there is no copy
# to `cmp`. Its replacement is `scripts/check-image-ui.sh`, which hashes the
# files INSIDE THE IMAGE against `ui/` — see this file's header, item 2.
#
# WHAT IS ASSERTED HERE INSTEAD is the one thing a chart gate still can say
# about the page: that the chart carries NO copy of it. A directory that came
# back — a merge that resurrected it, a `cp -r` someone found convenient —
# would be a second source of the page that nothing hashes, and the ConfigMap
# would follow it.
echo "== 2. the chart carries NO copy of ui/ (the page is the logweir-ui image) =="
if [ -e "$CHART/ui" ]; then
  echo "FAIL: $CHART/ui exists. Task 39 deleted it: the page ships as the logweir-ui" >&2
  echo "      image (Dockerfile.ui, values.yaml's ui.image), asserted by" >&2
  echo "      scripts/check-image-ui.sh against the bytes a browser receives — run it" >&2
  echo "      with \`just smoke-ui\`. A copy under the chart is a second source of the" >&2
  echo "      page that nothing hashes." >&2
  fail=1
else
  echo "   ok: no $CHART/ui — the page is delivered by ui.image, not by a ConfigMap"
fi

# ---------------------------------------------------------------- 7. the pins
# Early, because a wrong value makes every rendered file wrong too — and
# because arm 5 below uses the two repositories this arm derives.
echo "== 7. values.yaml names the tree's own repositories at $LOGWEIR_TAG =="
values_controller="$(sed -n 's/^controllerImage:[[:space:]]*//p' "$CHART/values.yaml")"
values_runner="$(sed -n 's/^runnerImage:[[:space:]]*//p' "$CHART/values.yaml")"
values_bootstrap="$(sed -n 's/^[[:space:]]*bootstrapImage:[[:space:]]*"\{0,1\}\([^"[:space:]]*\)"\{0,1\}.*$/\1/p' "$CHART/values.yaml")"
# `ui.image` IS INDENTED AND CARRIES A TRAILING COMMENT (ruling 15's short
# style), so it is read with its own expression rather than with the two above:
# two leading spaces, the key, the value up to the first whitespace. `tail -1`
# because `minio:` and `demoKafka:` carry an `image:` at the same indent and
# `ui:` is the last block in the file.
values_ui="$(sed -n 's/^[[:space:]]\{2\}image:[[:space:]]*\([^[:space:]]*\).*$/\1/p' "$CHART/values.yaml" | tail -1)"
# THE TREE'S REFERENCES, WHOLE — each still a digest under Global Constraint 7.
tree_controller="$(sed -n 's/^[[:space:]]*image:[[:space:]]*\([^[:space:]]*\)[[:space:]]*$/\1/p' config/manager/deployment.yaml)"
tree_runner="$(grep -A1 '^pub const RUNNER_IMAGE: &str =' crates/weirkeeper/src/job.rs | sed -n 's/^[[:space:]]*"\(.*\)";$/\1/p')"
# THE REPOSITORY HALVES, DERIVED. Never spelt in this script: a namespace change
# in the tree must propagate to the chart without editing this gate.
controller_repo="${tree_controller%@sha256:*}"
runner_repo="${tree_runner%@sha256:*}"
if [ -z "$controller_repo" ] || [ -z "$runner_repo" ] ||
   [ "$controller_repo" = "$tree_controller" ] || [ "$runner_repo" = "$tree_runner" ]; then
  echo "FAIL: could not derive the tree's repositories from its digests (controller '$tree_controller', runner '$tree_runner')" >&2
  echo "      config/manager/deployment.yaml and crates/weirkeeper/src/job.rs must each pin <repository>@sha256:<64 hex>" >&2
  fail=1
fi
bootstrap_test_image="$runner_repo@sha256:0000000000000000000000000000000000000000000000000000000000000000"
# A PINNED DEFAULT IS RENDERED AS SHIPPED. Only a pre-publication checkout (an
# empty value) substitutes the test-only digest, so the committed snapshots
# carry the exact bootstrap reference a default install pulls.
if [ -n "$values_bootstrap" ]; then
  bootstrap_render_args=()
  bootstrap_render_note=""
  bootstrap_target_note="the pinned bootstrap digest"
else
  bootstrap_render_args=(--set-string "identity.bootstrapImage=$bootstrap_test_image")
  bootstrap_render_note=" --set-string identity.bootstrapImage=<unpublished-test-digest>"
  bootstrap_target_note="an unpublished test-only bootstrap digest"
fi
# THE THIRD REPOSITORY, TASK 39. The UI image has no digest pin in the tree to
# strip a digest off — it is referenced by this chart alone — so its NAMESPACE
# is taken from the runner's and only the NAME is this chart's. A namespace
# move therefore carries all three, which is the property arm 7 exists for.
UI_IMAGE_NAME="logweir-ui"
ui_repo="${runner_repo%/*}/$UI_IMAGE_NAME"
if [ "${runner_repo%/*}" = "$runner_repo" ]; then
  echo "FAIL: could not derive the UI repository's namespace from the runner pin '$tree_runner'" >&2
  echo "      crates/weirkeeper/src/job.rs's RUNNER_IMAGE must name <host>/<namespace>/<name>@sha256:<64 hex>" >&2
  fail=1
fi
want_controller="$controller_repo:$LOGWEIR_TAG"
want_runner="$runner_repo:$LOGWEIR_TAG"
want_ui="$ui_repo:$LOGWEIR_TAG"
if [ "$values_controller" != "$want_controller" ]; then
  echo "FAIL: values.yaml controllerImage is '$values_controller'; it must be exactly '$want_controller'" >&2
  echo "      (arm 7: the repository is config/manager/deployment.yaml's, the tag is this chart's ruling of 2026-09-12)" >&2
  fail=1
fi
if [ "$values_runner" != "$want_runner" ]; then
  echo "FAIL: values.yaml runnerImage is '$values_runner'; it must be exactly '$want_runner'" >&2
  echo "      (arm 7: the repository is crates/weirkeeper/src/job.rs's RUNNER_IMAGE, the tag is this chart's ruling of 2026-09-12)" >&2
  fail=1
fi
if [ "$values_ui" != "$want_ui" ]; then
  echo "FAIL: values.yaml ui.image is '$values_ui'; it must be exactly '$want_ui'" >&2
  echo "      (arm 7, Task 39: the namespace is the runner pin's, the name is logweir-ui — the" >&2
  echo "      image Dockerfile.ui builds and release.yml publishes — and the tag is this chart's" >&2
  echo "      ruling of 2026-09-12. A kubectl digest here is the page back in a ConfigMap.)" >&2
  fail=1
fi
[ "$values_controller" = "$want_controller" ] && [ "$values_runner" = "$want_runner" ] &&
  [ "$values_ui" = "$want_ui" ] &&
  echo "   ok: controllerImage, runnerImage and ui.image are the tree's repositories at $LOGWEIR_TAG"

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
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" \
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
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" \
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
mkdir -p "$RENDERED"
for target in "${render_targets[@]}"; do
  name="${target%%:*}"
  file="${target#*:}"
  if [ -z "$file" ]; then
    helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --include-crds ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} > "$tmp/$name.yaml" 2> "$tmp/render-$name.err"
    rc=$?
  else
    helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --include-crds -f "$file" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} > "$tmp/$name.yaml" 2> "$tmp/render-$name.err"
    rc=$?
  fi
  echo "   rc=$rc  (helm template $RELEASE $CHART -n $NAMESPACE --include-crds${file:+ -f $file}$bootstrap_render_note > $RENDERED/$name.yaml)"
  if [ "$rc" -ne 0 ]; then
    cat "$tmp/render-$name.err" >&2
    fail=1
    continue
  fi
  {
    echo "# GENERATED FILE — do not edit by hand."
    echo "# Rendered by scripts/check-chart.sh (just chart-check):"
    echo "#   helm template $RELEASE $CHART -n $NAMESPACE --include-crds${file:+ -f $file}$bootstrap_render_note"
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
    # `logweir-ui` IS NOT MATCHED BY THE RUNNER'S PATTERN, which is worth
    # stating because the names share a prefix: `$runner_repo:*` requires the
    # colon immediately after `.../logweir`, and `.../logweir-ui:latest` has
    # `-ui` there. Before Task 39 added the third arm, the UI reference fell
    # through to the digest arm below and was reported as an un-digested tag.
    case "$ref" in
      "$controller_repo":*|"$runner_repo":*|"$ui_repo":*|"$controller_repo"@*|"$runner_repo"@*|"$ui_repo"@*)
        logweir_tagged=$((logweir_tagged + 1))
        if [ "$ref" != "$controller_repo:$LOGWEIR_TAG" ] &&
           [ "$ref" != "$runner_repo:$LOGWEIR_TAG" ] &&
           [ "$ref" != "$ui_repo:$LOGWEIR_TAG" ]; then
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
for flag in demoKafka.enabled minio.enabled ui.enabled identity.enabled admissionPolicy.enabled; do
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} --set "$flag=yes" > /dev/null 2> "$tmp/schema-$flag.err"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "FAIL: \`helm template --set $flag=yes\` exited 0; values.schema.json must type $flag as a boolean and refuse the string" >&2
    fail=1
  else
    echo "   rc=$rc  (helm template --set $flag=yes — refused, as it must be)"
  fi
done
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
  --set-string 'identity.kubernetesApiCIDRs[0]=0.0.0.0/0' > /dev/null 2> "$tmp/schema-broad-api.err"
rc=$?
if [ "$rc" -eq 0 ]; then
  echo "FAIL: identity.kubernetesApiCIDRs accepted broad 0.0.0.0/0 egress" >&2
  fail=1
else
  echo "   rc=$rc  (broad bootstrap API CIDR refused, as it must be)"
fi

# D2 W11 — THE INSTALLATION POLICY'S OWN REFUSALS. The rendered `policy.json`
# is parsed by `weirkeeper::check::policy` with `deny_unknown_fields` and no
# per-field default inside `checks`/`discovery`/`preflight`, and a document it
# refuses fails CLOSED and SILENTLY: empty attestations, empty evidence
# allowlist, and one advisory `configuration.policy notReady` row nobody sees
# unless they read a Preflight. So the schema has to refuse the same values at
# INSTALL time, where the operator is still looking.
#
#   keepPerConnection=0   would delete a discovery the moment it finished
#   a partial attestation  matches nothing while LOOKING like an attestation an
#                          operator can rely on for `attestedComplete`
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
  --set 'checks.discovery.keepPerConnection=0' > /dev/null 2> "$tmp/schema-keep-zero.err"
rc=$?
if [ "$rc" -eq 0 ]; then
  echo "FAIL: checks.discovery.keepPerConnection accepted 0; keeping none deletes a discovery the moment it finishes" >&2
  fail=1
else
  echo "   rc=$rc  (checks.discovery.keepPerConnection=0 refused, as it must be)"
fi
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" ${bootstrap_render_args[@]+"${bootstrap_render_args[@]}"} \
  --set-string 'checks.discovery.visibilityAttestations[0].id=att-partial' > /dev/null 2> "$tmp/schema-partial-attestation.err"
rc=$?
if [ "$rc" -eq 0 ]; then
  echo "FAIL: an attestation naming only \`id\` rendered; every field of a completeness attestation is required" >&2
  fail=1
else
  echo "   rc=$rc  (a partial visibility attestation refused, as it must be)"
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "FAIL: the chart is not what the tree says it is; the lines above name what drifted." >&2
  exit 1
fi
echo "ok: charts/logweir — CRDs byte-identical, no copy of ui/ (the page is the logweir-ui image),"
echo "    helm lint clean, rendered files current, every image a digest except the three Logweir"
echo "    images at :$LOGWEIR_TAG (author-only exempt by name), the schema refuses a non-boolean"
echo "    flag, and values.yaml names the tree's own repositories at :$LOGWEIR_TAG."
exit 0
