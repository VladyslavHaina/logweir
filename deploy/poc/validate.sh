#!/usr/bin/env bash
# deploy/poc/validate.sh — render every chart of the PoC profile with its pinned
# version and this directory's values, without a cluster.
#
#   bash deploy/poc/validate.sh [charts-dir]
#
# What it renders, and what each render must show:
#
#   1. Logweir, TWO ways that must agree: the package `images.yml` publishes
#      (built here by `scripts/ci-images.sh chart-package` for versions.env's
#      commit — byte-for-byte what `helm install oci://…` installs), and this
#      checkout's chart with the four images `--set` from versions.env (the
#      development path). Every image pinned; no `kubectl patch` in the profile;
#      the console's host alias, trusted proxy Service, egress peer and CA
#      bundle agree with the Traefik render and with each other.
#   2. The two upgrade-rehearsal baselines with THEIR OWN charts (git archive at
#      R1_COMMIT / R2_COMMIT) and their values under rehearsals/, and the
#      candidate chart REFUSING R2's unmigrated shared-console values
#      (release-note item 6) — the refusal the rehearsal must meet live.
#   3. Traefik, cert-manager and Dex at their pinned versions. The upstream
#      charts are read from `charts-dir` when it holds the pinned tarballs, and
#      are otherwise pulled into a temporary directory (network access to their
#      repositories). Dex's rendered configuration must carry no secret.
#
# Nothing is applied and no cluster is contacted. Every subprocess that can
# reach the network runs under a timeout.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
# shellcheck disable=SC1091
. deploy/poc/versions.env

work="$(mktemp -d "${TMPDIR:-/tmp}/logweir-poc-validate.XXXXXX")"
trap 'rm -rf "$work"' EXIT
charts="${1:-$work}"
fail=0

bounded() { # seconds command...
  local seconds="$1"; shift
  if [ -x /tmp/lwtimeout ]; then /tmp/lwtimeout "$seconds" "$@"
  elif command -v timeout >/dev/null 2>&1; then timeout "$seconds" "$@"
  else "$@"; fi
}

pull() { # repo name version
  local tgz="$charts/$2-$3.tgz"
  [ -f "$tgz" ] && { echo "$tgz"; return 0; }
  bounded 120 helm pull "$2" --repo "$1" --version "$3" -d "$charts" >/dev/null 2>&1 || return 1
  echo "$tgz"
}

check_images() { # label rendered-file
  while IFS= read -r ref; do
    case "$ref" in
      *@sha256:????????????????????????????????????????????????????????????????) : ;;
      docker.io/vladyslavhaina/*:sha-????????????????????????????????????????) : ;;
      docker.io/vladyslavhaina/*:v[0-9]*.[0-9]*.[0-9]*) : ;;
      *) echo "FAIL: $1 renders an image that is neither a digest, a sha- tag nor a release tag: $ref" >&2; fail=1 ;;
    esac
  done < <(grep -E '^[[:space:]]+(- )?image:[[:space:]]' "$2" \
             | sed -E 's/.*image:[[:space:]]*//; s/["'\'']//g; s/[[:space:]]*$//')
}

render() { # label chart namespace [helm args...]; RELEASE overrides the release name
  local label="$1" chart="$2" ns="$3"; shift 3
  if helm template "${RELEASE:-$label}" "$chart" -n "$ns" "$@" > "$work/$label.yaml" 2> "$work/$label.err"; then
    echo "   ok: $label ($(grep -c '^kind:' "$work/$label.yaml") objects)"
  else
    echo "FAIL: $label did not render:" >&2
    cat "$work/$label.err" >&2
    fail=1
    return 1
  fi
  check_images "$label" "$work/$label.yaml"
}

need() { # label file needle...
  local label="$1" file="$2"; shift 2
  for needle in "$@"; do
    if ! grep -F -q -- "$needle" "$file"; then
      echo "FAIL: $label does not carry \`$needle\`" >&2
      fail=1
    fi
  done
}

echo "== 1. Logweir, as published and from the checkout =="
case "$LOGWEIR_COMMIT" in
  0000000000000000000000000000000000000000)
    echo "   NOTE: LOGWEIR_COMMIT is the pre-publication placeholder. The profile renders, and"
    echo "         cannot be installed until versions.env names the first main publication that"
    echo "         carries this profile's chart (versions.env says why)." ;;
esac
if grep -rn -E 'kubectl[^|]*[[:space:]]patch[[:space:]]' deploy/poc/README.md deploy/poc/*.sh deploy/poc/*.yaml >/dev/null 2>&1; then
  echo "FAIL: the profile still patches an installed object with kubectl:" >&2
  grep -rn -E 'kubectl[^|]*[[:space:]]patch[[:space:]]' deploy/poc/README.md deploy/poc/*.sh deploy/poc/*.yaml >&2
  fail=1
fi
package="$(GITHUB_SHA="$LOGWEIR_COMMIT" NS=vladyslavhaina TAG="$LOGWEIR_TAG" \
  bash scripts/ci-images.sh chart-package "$work/package" 2> "$work/package.err")"
if [ ! -f "$package" ]; then
  echo "FAIL: scripts/ci-images.sh chart-package produced no package:" >&2
  cat "$work/package.err" >&2
  fail=1
elif [ "$(basename "$package")" != "logweir-chart-$LOGWEIR_CHART_VERSION.tgz" ]; then
  echo "FAIL: the package is $(basename "$package"), versions.env expects logweir-chart-$LOGWEIR_CHART_VERSION" >&2
  fail=1
else
  render logweir "$package" "$LOGWEIR_NAMESPACE" -f deploy/poc/logweir.values.yaml
  bounded 60 helm lint "$package" -f deploy/poc/logweir.values.yaml > "$work/lint.log" 2>&1 \
    || { echo "FAIL: helm lint of the package" >&2; cat "$work/lint.log" >&2; fail=1; }
fi
RELEASE=logweir render logweir-checkout charts/logweir "$LOGWEIR_NAMESPACE" -f deploy/poc/logweir.values.yaml \
  --set "controllerImage=docker.io/vladyslavhaina/weirkeeper:$LOGWEIR_TAG" \
  --set "runnerImage=docker.io/vladyslavhaina/logweir:$LOGWEIR_TAG" \
  --set "api.console.image=docker.io/vladyslavhaina/logweir-console:$LOGWEIR_TAG" \
  --set "ui.image=docker.io/vladyslavhaina/logweir-ui:$LOGWEIR_TAG"
if [ -f "$work/logweir.yaml" ] && [ -f "$work/logweir-checkout.yaml" ]; then
  # The two paths must install the same objects; only the chart's own label
  # (`helm.sh/chart: logweir-chart-…` against `logweir-0.1.0`) may differ.
  grep -v 'helm.sh/chart:' "$work/logweir.yaml" | sed 's/^# Source: logweir-chart\//# Source: logweir\//' > "$work/a"
  grep -v 'helm.sh/chart:' "$work/logweir-checkout.yaml" > "$work/b"
  if ! diff -q "$work/a" "$work/b" >/dev/null; then
    echo "FAIL: the published package and the checkout install different objects:" >&2
    diff -u "$work/b" "$work/a" | head -40 >&2
    fail=1
  else
    echo "   ok: the published package and the checkout (images from versions.env) render the same objects"
  fi
  need "the Logweir render" "$work/logweir.yaml" \
    "docker.io/vladyslavhaina/weirkeeper:$LOGWEIR_TAG" \
    "docker.io/vladyslavhaina/logweir-console:$LOGWEIR_TAG" \
    "caBundleFile: /var/run/logweir/oidc-ca/ca.crt" \
    "name: logweir-poc-ca" \
    "ip: $DEX_CLUSTER_IP" \
    "- $DEX_HOST" \
    "trustedProxyService:" \
    "name: logweir-api-trusted-proxy" \
    "namespace: $INGRESS_NAMESPACE" \
    "kubernetes.io/metadata.name: $DEX_NAMESPACE" \
    "port: 5554" \
    "livenessProbe:" \
    '["/usr/local/bin/weirkeeper", "--probe", "live"]'
  if grep -A1 'ipBlock' "$work/logweir.yaml" | grep -q "$DEX_CLUSTER_IP"; then
    echo "FAIL: an egress ipBlock names Dex's ClusterIP; an enforcing CNI matches after DNAT (use oidcPeers)" >&2
    fail=1
  fi
fi

echo "== 2. The upgrade-rehearsal baselines, each with its own chart =="
for r in R1 R2; do
  commit_var="${r}_COMMIT"; commit="${!commit_var}"
  lower="$(echo "$r" | tr 'R' 'r')"
  values="$(find deploy/poc/rehearsals -name "$lower-*.values.yaml" 2>/dev/null | head -1)"
  if [ -z "$values" ]; then echo "FAIL: no rehearsals/ values file for $r" >&2; fail=1; continue; fi
  if ! git cat-file -e "$commit^{commit}" 2>/dev/null; then
    echo "FAIL: $r's commit $commit is not in this clone (fetch the full history, or the tag)" >&2
    fail=1; continue
  fi
  mkdir -p "$work/$r"
  git archive "$commit" charts/logweir | tar -x -C "$work/$r"
  RELEASE=logweir render "baseline-$lower" "$work/$r/charts/logweir" "$LOGWEIR_NAMESPACE" -f "$values"
done
if [ -f "$work/baseline-r1.yaml" ]; then
  need "the R1 baseline" "$work/baseline-r1.yaml" "docker.io/vladyslavhaina/weirkeeper:$R1_TAG"
  if grep -q 'logweir-console' "$work/baseline-r1.yaml"; then
    echo "FAIL: the R1 baseline renders a console; v0.1.5 had none" >&2; fail=1
  fi
fi
[ -f "$work/baseline-r2.yaml" ] && need "the R2 baseline" "$work/baseline-r2.yaml" \
  "docker.io/vladyslavhaina/logweir-console:$R2_TAG" "mode: shared"
# Item 6, statically: the candidate refuses R2's unmigrated shared console.
if helm template logweir charts/logweir -n "$LOGWEIR_NAMESPACE" -f deploy/poc/rehearsals/r2-f49849d.values.yaml \
     > /dev/null 2> "$work/item6.err"; then
  echo "FAIL: the candidate chart ACCEPTED R2's unmigrated shared-console values (release-note item 6)" >&2
  fail=1
elif grep -q 'requires controller.watchNamespaces' "$work/item6.err"; then
  echo "   ok: the candidate refuses R2's unmigrated shared console, naming controller.watchNamespaces (item 6)"
else
  echo "FAIL: the candidate refused R2's values for another reason:" >&2; cat "$work/item6.err" >&2; fail=1
fi

echo "== 3. Traefik, cert-manager and Dex, pinned =="
if traefik="$(pull "$TRAEFIK_REPO" traefik "$TRAEFIK_CHART_VERSION")"; then
  if render traefik "$traefik" "$INGRESS_NAMESPACE" -f deploy/poc/traefik.values.yaml; then
    need "the Traefik render" "$work/traefik.yaml" \
      "--entryPoints.websecure.http.middlewares=traefik-hsts@kubernetescrd" \
      "--providers.kubernetesingress.namespaces=$DEX_NAMESPACE,$LOGWEIR_NAMESPACE" \
      "--providers.kubernetescrd.namespaces=$INGRESS_NAMESPACE" \
      "--accesslog.fields.queryparameters.defaultmode=drop" \
      "--entryPoints.web.http.redirections.entryPoint.scheme=https" \
      "containerPort: 8443" \
      "app.kubernetes.io/name: traefik"
    # The console trusts the endpoints of Service traefik/traefik.
    if ! awk '/^kind: Service$/{s=1} s && /^  name: traefik$/{n=1} n && /^  namespace: traefik$/{found=1} /^---/{s=0;n=0} END{exit !found}' "$work/traefik.yaml"; then
      echo "FAIL: the Traefik render has no Service traefik/traefik for trustedProxyService to name" >&2
      fail=1
    fi
  fi
else
  echo "FAIL: could not obtain traefik $TRAEFIK_CHART_VERSION" >&2; fail=1
fi
if cm="$(pull "$CERT_MANAGER_REPO" cert-manager "$CERT_MANAGER_CHART_VERSION")"; then
  render cert-manager "$cm" "$CERT_MANAGER_NAMESPACE" -f deploy/poc/cert-manager.values.yaml
else
  echo "FAIL: could not obtain cert-manager $CERT_MANAGER_CHART_VERSION" >&2; fail=1
fi
if dex="$(pull "$DEX_REPO" dex "$DEX_CHART_VERSION")"; then
  render dex "$dex" "$DEX_NAMESPACE" -f deploy/poc/dex.values.yaml
  need "the Dex render" "$work/dex.yaml" "ingressClassName: traefik" \
    "clusterIP: $DEX_CLUSTER_IP" "port: 443" "targetPort: https" "- --web-https-addr" \
    "mountPath: /etc/dex/tls" "secretName: dex-internal-tls"
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
  need "the Dex config" "$work/dex-config.yaml" "tlsCert: /etc/dex/tls/tls.crt" "tlsKey: /etc/dex/tls/tls.key"
else
  echo "FAIL: could not obtain dex $DEX_CHART_VERSION" >&2; fail=1
fi

# THE CONSOLE'S ROUTE TO ITS IdP NEVER CROSSES THE SHARED INGRESS (the review's
# M1). The console's host alias for the issuer must be Dex's OWN Service, whose
# TLS Dex terminates with a certificate for that name; its egress peer must be
# Dex's pods; nothing Traefik renders may own that address; and Traefik routes
# Ingresses from the profile's namespaces only (checked above).
if [ -f "$work/logweir.yaml" ] && [ -f "$work/dex.yaml" ] && [ -f "$work/traefik.yaml" ]; then
  alias_ip="$(awk '/hostAliases:/{h=1} h && /ip:/{print $NF; exit}' "$work/logweir.yaml")"
  dex_ip="$(awk '/^kind: Service$/{s=1} s && /^  clusterIP:/{print $2; exit}' "$work/dex.yaml")"
  if [ -z "$alias_ip" ] || [ "$alias_ip" != "$dex_ip" ]; then
    echo "FAIL: the console's alias for $DEX_HOST is '$alias_ip', not Dex's own Service ($dex_ip)" >&2; fail=1
  elif grep -q "clusterIP: $alias_ip" "$work/traefik.yaml"; then
    echo "FAIL: the console's alias for $DEX_HOST is Traefik's address — the shared ingress is on its IdP path" >&2; fail=1
  elif awk '/^kind: NetworkPolicy$/{n=1} n && /egress:/{e=1} e && /kubernetes.io\/metadata.name: '"$INGRESS_NAMESPACE"'/{bad=1} /^---/{n=0;e=0} END{exit !bad}' "$work/logweir.yaml"; then
    echo "FAIL: the console's egress names the ingress namespace; its IdP path must be Dex's pods" >&2; fail=1
  else
    echo "   ok: the console reaches $DEX_HOST at Dex's own Service ($dex_ip), never through Traefik"
  fi
fi
if ! grep -q 'name: dex-internal-tls' deploy/poc/issuers.yaml || ! grep -q "dnsNames: \[$DEX_HOST\]" deploy/poc/issuers.yaml; then
  echo "FAIL: issuers.yaml does not ask the local CA for Dex's own serving certificate" >&2; fail=1
fi
# The retired controller may be NAMED in a comment (why it is gone), never used.
if grep -rn -i 'nginx' deploy/poc/*.yaml deploy/poc/*.env deploy/poc/rehearsals/ | grep -v -E ':[0-9]+:[[:space:]]*#' > "$work/nginx.hits"; then
  echo "FAIL: the profile still uses the retired ingress-nginx:" >&2
  cat "$work/nginx.hits" >&2
  fail=1
fi

[ "$fail" -eq 0 ] && echo "ok: every chart renders, every image is pinned, the two Logweir paths agree"
exit "$fail"
