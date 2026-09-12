#!/usr/bin/env bash
# THE CHART GATE — holds `charts/logweir` to the tree it is derived from.
# Joined to `just gate` as `just chart-check` (Task 35).
#
# WHAT THIS PROVES, exactly and only:
#
#   1. `charts/logweir/crds/*.yaml` are BYTE-IDENTICAL to `config/crd/*.yaml`
#      (`cmp`, exit 1 naming the file otherwise) — Helm installs `crds/` once
#      and never upgrades it, so a CRD that drifted from the emitter's output
#      would be a CRD nobody regenerates.
#   2. `charts/logweir/ui/**` is a byte-identical copy of the fourteen shipped
#      UI files (`ui/*.html`, `ui/*.js`, `ui/*.css`, `ui/pages/*`) — the bytes
#      `kubectl proxy --www=/ui` serves in-cluster are the bytes
#      `scripts/check-ui-offline.sh` scanned; and the copy holds NOTHING else
#      (no `tests/`, no README): Global Constraint 28, no key material in the
#      ConfigMap.
#   3. `helm lint` passes for the default values and for EVERY file under
#      `charts/logweir/examples/`.
#   4. `helm template` renders each into `charts/logweir/rendered/<name>.yaml`
#      (checked in; regenerated HERE) and `git status --porcelain --
#      charts/logweir/rendered` is EMPTY afterwards — the drift idiom of
#      `just crds-check`: a template change lands as a diff a reviewer reads.
#   5. Every container image reference in the rendered files is
#      `<name>@sha256:<64 hex>` (Global Constraint 7) — with TWO exceptions,
#      and nothing else:
#        * THE TWO LOGWEIR REPOSITORIES, which must be exactly
#          `<repository>:latest` (the owner's decision of 2026-09-12; arm 7
#          derives the repositories). A digest there, or any other tag, is a
#          red; so is `:latest` on ANY OTHER image, which is still refused.
#        * `rendered/author-only.yaml`, exempt BY NAME: that example's whole
#          premise is a locally built tag under `imagePullPolicy: Never`, and a
#          locally built digest changes on every build (plan erratum E19(a)),
#          the same reason `config/overlays/local-images` rewrites to a tag.
#   6. `values.schema.json` REFUSES a non-boolean flag: `--set
#      demoKafka.enabled=yes` (and `minio.`/`ui.`) must exit non-zero.
#   7. `values.yaml` names THE TREE'S OWN REPOSITORIES AT `latest`:
#      `controllerImage` is the repository half of the image
#      `config/manager/deployment.yaml` carries and `runnerImage` is the
#      repository half of `weirkeeper::job::RUNNER_IMAGE`, each followed by
#      `:latest`. The repositories are DERIVED, never spelt here, so a
#      namespace change in the tree propagates; the TAG is this chart's ruling
#      (the owner's decision of 2026-09-12) and `config/`, `logweir.yaml` and
#      that Rust constant keep their digests under Global Constraint 7. A
#      digest written back into either value is a red.
#
# WHAT IT DOES NOT PROVE: that the chart installs. That is a cluster gate —
# `just helm-demo` on docker-desktop, `.github/workflows/helm-demo.yml` on
# kind — and it is in `docs/gates.md`'s stack/cluster table, not here.
#
# IT REFUSES WITHOUT `helm`, NAMING THE VERSION IT WANTS — the
# `check-ui-behaviour.sh` precedent (node >= 20). Helm >= 4.0.0, because the
# checked-in rendered files were produced by v4.0.1 and a different major may
# serialise a long scalar differently; a false drift from a helm the tree did
# not render with would be a red nobody can act on. `ci.yml` pins the same
# major with `azure/setup-helm`.
#
# EVERY EXIT CODE IS READ ON ITS OWN LINE, NEVER THROUGH A PIPE (STANDING
# RULE 20). `just` and `git` output is captured to a file first.
set -uo pipefail
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
    echo "FAIL: $CHART/crds/$base has no counterpart under config/crd/ — a seventh kind arrives in the emitter first" >&2
    fail=1
  fi
done
[ "$crd_count" -eq 6 ] || { echo "FAIL: expected six CRDs under config/crd/, found $crd_count" >&2; fail=1; }
[ "$fail" -eq 0 ] && echo "   ok: $crd_count CRDs, byte for byte"

# ---------------------------------------------------------------- 2. the UI
echo "== 2. ui/ is a byte-identical copy of the fourteen shipped UI files =="
ui_files=()
while IFS= read -r -d '' f; do
  ui_files+=("$f")
done < <(find ui -type f ! -name '*.md' ! -path 'ui/tests/*' -print0)
ui_fail=0
for src in "${ui_files[@]}"; do
  rel="${src#ui/}"
  if [ ! -f "$CHART/ui/$rel" ]; then
    echo "FAIL: $CHART/ui/$rel is missing ($src exists)" >&2
    ui_fail=1
    continue
  fi
  cmp -s "$src" "$CHART/ui/$rel"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: $CHART/ui/$rel is not byte-identical to $src (cmp rc=$rc). Copy it:" >&2
    echo "      cp $src $CHART/ui/$rel" >&2
    ui_fail=1
  fi
done
chart_ui_count=0
while IFS= read -r -d '' f; do
  chart_ui_count=$((chart_ui_count + 1))
  rel="${f#"$CHART"/ui/}"
  if [ ! -f "ui/$rel" ]; then
    echo "FAIL: $CHART/ui/$rel has no counterpart under ui/ — the chart's copy carries the shipped page and nothing else" >&2
    ui_fail=1
  fi
done < <(find "$CHART/ui" -type f -print0)
[ "${#ui_files[@]}" -eq 14 ] || { echo "FAIL: expected fourteen shipped UI files under ui/, found ${#ui_files[@]}" >&2; ui_fail=1; }
[ "$chart_ui_count" -eq "${#ui_files[@]}" ] || { echo "FAIL: $CHART/ui holds $chart_ui_count file(s), ui/ holds ${#ui_files[@]}" >&2; ui_fail=1; }
if [ "$ui_fail" -eq 0 ]; then
  echo "   ok: ${#ui_files[@]} files, byte for byte, and nothing else"
else
  fail=1
fi

# ---------------------------------------------------------------- 7. the pins
# Early, because a wrong value makes every rendered file wrong too — and
# because arm 5 below uses the two repositories this arm derives.
echo "== 7. values.yaml names the tree's own repositories at $LOGWEIR_TAG =="
values_controller="$(sed -n 's/^controllerImage:[[:space:]]*//p' "$CHART/values.yaml")"
values_runner="$(sed -n 's/^runnerImage:[[:space:]]*//p' "$CHART/values.yaml")"
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
want_controller="$controller_repo:$LOGWEIR_TAG"
want_runner="$runner_repo:$LOGWEIR_TAG"
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
[ "$values_controller" = "$want_controller" ] && [ "$values_runner" = "$want_runner" ] && echo "   ok: controllerImage and runnerImage are the tree's repositories at $LOGWEIR_TAG"

# ---------------------------------------------------------------- 3 + 4. lint and render
echo "== 3. helm lint, default values and every example =="
render_targets=("default:")
for ex in "$EXAMPLES"/*.values.yaml; do
  name="$(basename "$ex" .values.yaml)"
  render_targets+=("$name:$ex")
done
[ "${#render_targets[@]}" -ge 2 ] || { echo "FAIL: no example under $EXAMPLES — a gate over one values file proves little" >&2; fail=1; }
for target in "${render_targets[@]}"; do
  name="${target%%:*}"
  file="${target#*:}"
  if [ -z "$file" ]; then
    helm lint "$CHART" > "$tmp/lint-$name.log" 2>&1
    rc=$?
  else
    helm lint "$CHART" -f "$file" > "$tmp/lint-$name.log" 2>&1
    rc=$?
  fi
  echo "   rc=$rc  (helm lint $CHART${file:+ -f $file})"
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
    helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --include-crds > "$tmp/$name.yaml" 2> "$tmp/render-$name.err"
    rc=$?
  else
    helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --include-crds -f "$file" > "$tmp/$name.yaml" 2> "$tmp/render-$name.err"
    rc=$?
  fi
  echo "   rc=$rc  (helm template $RELEASE $CHART -n $NAMESPACE --include-crds${file:+ -f $file} > $RENDERED/$name.yaml)"
  if [ "$rc" -ne 0 ]; then
    cat "$tmp/render-$name.err" >&2
    fail=1
    continue
  fi
  {
    echo "# GENERATED FILE — do not edit by hand."
    echo "# Rendered by scripts/check-chart.sh (just chart-check):"
    echo "#   helm template $RELEASE $CHART -n $NAMESPACE --include-crds${file:+ -f $file}"
    echo "# It is checked in so a template change lands as a diff a reviewer reads; the"
    echo "# gate regenerates it and fails on any drift (git status --porcelain)."
    cat "$tmp/$name.yaml"
  } > "$RENDERED/$name.yaml"
done
git status --porcelain -- "$RENDERED" > "$tmp/porcelain.txt" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "FAIL: git status exited $rc over $RENDERED" >&2
  fail=1
fi
if [ -s "$tmp/porcelain.txt" ]; then
  echo "FAIL: regenerating the rendered files CHANGED $RENDERED — the templates and the checked-in" >&2
  echo "      render disagree. Land the regenerated files in the same commit as the template change:" >&2
  cat "$tmp/porcelain.txt" >&2
  fail=1
else
  echo "   ok: $RENDERED is what the templates render to (no drift)"
fi

# ---------------------------------------------------------------- 5. GC7
# The two Logweir repositories come from arm 7 above, which is why that arm runs
# first. They are the ONLY references allowed to be a tag here, and the only tag
# allowed is $LOGWEIR_TAG — exactly.
echo "== 5. every rendered image is a digest, except the two Logweir images at $LOGWEIR_TAG (and $DIGEST_EXEMPT, by name) =="
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
    # THE TWO LOGWEIR IMAGES, BY REPOSITORY. Each must be exactly
    # `<repository>:$LOGWEIR_TAG` — a digest there is this chart's ruling
    # reverted, and any other tag is a value nobody chose.
    case "$ref" in
      "$controller_repo":*|"$runner_repo":*|"$controller_repo"@*|"$runner_repo"@*)
        logweir_tagged=$((logweir_tagged + 1))
        if [ "$ref" != "$controller_repo:$LOGWEIR_TAG" ] && [ "$ref" != "$runner_repo:$LOGWEIR_TAG" ]; then
          echo "FAIL: $base references a Logweir image as '$ref'; this chart names both by '<repository>:$LOGWEIR_TAG' (arm 7)" >&2
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
      *:latest*) echo "FAIL: $base references :latest ('$ref') — only the two Logweir images may carry a tag here" >&2; fail=1 ;;
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
for flag in demoKafka.enabled minio.enabled ui.enabled; do
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" --set "$flag=yes" > /dev/null 2> "$tmp/schema-$flag.err"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "FAIL: \`helm template --set $flag=yes\` exited 0; values.schema.json must type $flag as a boolean and refuse the string" >&2
    fail=1
  else
    echo "   rc=$rc  (helm template --set $flag=yes — refused, as it must be)"
  fi
done

echo
if [ "$fail" -ne 0 ]; then
  echo "FAIL: the chart is not what the tree says it is; the lines above name what drifted." >&2
  exit 1
fi
echo "ok: charts/logweir — CRDs and UI byte-identical, helm lint clean, rendered files current,"
echo "    every image a digest except the two Logweir images at :$LOGWEIR_TAG (author-only exempt by"
echo "    name), the schema refuses a non-boolean flag, and values.yaml names the tree's own"
echo "    repositories at :$LOGWEIR_TAG."
exit 0
