#!/usr/bin/env bash
# deploy/poc/validate.sh — render every chart of the PoC profile with its pinned
# version and this directory's values, without a cluster.
#
#   bash deploy/poc/validate.sh [charts-dir]
#
# The Logweir chart renders from this checkout. The three upstream charts are
# read from `charts-dir` when it holds the pinned tarballs (`helm pull … --version`),
# and are otherwise pulled into a temporary directory, which needs network
# access to their repositories. Nothing is applied and no cluster is contacted.
# Every render must exit 0 and every image in every render must be pinned by
# digest or by an immutable Logweir `sha-` tag.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
# shellcheck disable=SC1091
. deploy/poc/versions.env

work="$(mktemp -d "${TMPDIR:-/tmp}/logweir-poc-validate.XXXXXX")"
trap 'rm -rf "$work"' EXIT
charts="${1:-$work}"
fail=0

pull() { # repo name version
  local tgz="$charts/$2-$3.tgz"
  [ -f "$tgz" ] && { echo "$tgz"; return 0; }
  helm pull "$2" --repo "$1" --version "$3" -d "$charts" >/dev/null 2>&1 || return 1
  echo "$tgz"
}

render() { # label chart namespace values...
  local label="$1" chart="$2" ns="$3"; shift 3
  local args=()
  for v in "$@"; do args+=(-f "$v"); done
  if helm template "$label" "$chart" -n "$ns" "${args[@]}" > "$work/$label.yaml" 2> "$work/$label.err"; then
    echo "   ok: $label ($(grep -c '^kind:' "$work/$label.yaml") objects)"
  else
    echo "FAIL: $label did not render:" >&2
    cat "$work/$label.err" >&2
    fail=1
    return
  fi
  while IFS= read -r ref; do
    case "$ref" in
      *@sha256:????????????????????????????????????????????????????????????????) : ;;
      docker.io/vladyslavhaina/*:sha-????????????????????????????????????????) : ;;
      *) echo "FAIL: $label renders an image that is neither a digest nor a sha- tag: $ref" >&2; fail=1 ;;
    esac
  done < <(grep -E '^[[:space:]]+(- )?image:[[:space:]]' "$work/$label.yaml" \
             | sed -E 's/.*image:[[:space:]]*//; s/["'\'']//g; s/[[:space:]]*$//')
}

echo "== the PoC profile, rendered (no cluster) =="
if ! grep -q "$LOGWEIR_TAG" deploy/poc/logweir.values.yaml; then
  echo "FAIL: logweir.values.yaml does not name versions.env's LOGWEIR_TAG ($LOGWEIR_TAG)" >&2
  fail=1
fi
render logweir charts/logweir "$LOGWEIR_NAMESPACE" deploy/poc/logweir.values.yaml
helm lint charts/logweir -f deploy/poc/logweir.values.yaml > "$work/lint.log" 2>&1 \
  || { echo "FAIL: helm lint" >&2; cat "$work/lint.log" >&2; fail=1; }

if nginx="$(pull "$INGRESS_NGINX_REPO" ingress-nginx "$INGRESS_NGINX_CHART_VERSION")"; then
  render ingress-nginx "$nginx" "$INGRESS_NAMESPACE" deploy/poc/ingress-nginx.values.yaml
else
  echo "FAIL: could not obtain ingress-nginx $INGRESS_NGINX_CHART_VERSION" >&2; fail=1
fi
if cm="$(pull "$CERT_MANAGER_REPO" cert-manager "$CERT_MANAGER_CHART_VERSION")"; then
  render cert-manager "$cm" "$CERT_MANAGER_NAMESPACE" deploy/poc/cert-manager.values.yaml
else
  echo "FAIL: could not obtain cert-manager $CERT_MANAGER_CHART_VERSION" >&2; fail=1
fi
if dex="$(pull "$DEX_REPO" dex "$DEX_CHART_VERSION")"; then
  render dex "$dex" "$DEX_NAMESPACE" deploy/poc/dex.values.yaml
  # The Dex config is rendered base64-encoded into a Secret. Decode it: no
  # password hash and no client secret may be in it, only the names of the
  # environment variables that carry them (hashFromEnv, secretEnv).
  sed -n 's/^  config.yaml: "\(.*\)"$/\1/p' "$work/dex.yaml" | base64 -d > "$work/dex-config.yaml" 2>/dev/null
  if ! grep -q 'hashFromEnv' "$work/dex-config.yaml"; then
    echo "FAIL: could not read the rendered Dex config (no hashFromEnv found)" >&2; fail=1
  elif grep -Eq '^[[:space:]]*(hash|secret):' "$work/dex-config.yaml"; then
    echo "FAIL: the rendered Dex config carries an inline hash or client secret" >&2; fail=1
  else
    echo "   ok: the Dex config names its secrets by environment variable only"
  fi
else
  echo "FAIL: could not obtain dex $DEX_CHART_VERSION" >&2; fail=1
fi

[ "$fail" -eq 0 ] && echo "ok: every chart renders, every image is pinned"
exit "$fail"
