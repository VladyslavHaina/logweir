#!/usr/bin/env bash
# FX-10: every chart value, changed ON ITS OWN to a non-default value, changes
# what the chart renders. Called by scripts/check-chart.sh (`just chart-check`);
# runnable alone. Arguments are passed to every `helm template` FIRST, so the
# gate's bootstrap-image override applies and a row can still override it.
#
# WHY. FX-10 found two values (`checks.discovery.defaultMaxTopics`,
# `checks.preflight.defaultTimeoutSeconds`) that were documented, typed,
# rendered and parsed, and changed nothing. A gate that renders each value only
# at its default cannot tell a value the templates read from one they ignore:
# the committed renders were byte-identical either way. This table is the other
# half. For each `values.yaml` leaf it renders a BASE and the base plus that one
# value at a non-default setting, and fails unless the second differs from the
# first.
#
# WHAT A ROW PROVES, AND WHAT IT DOES NOT. "Changing it changes the render" is
# the property that would have caught a template that ignores a value. It does
# not prove the value lands in the RIGHT field; the chart_lint and
# weirkeeper/tests/chart_policy.rs rows that read the committed renders do that
# for the values a binary consumes. And it is the template half only: whether
# the BINARY that reads a rendered value acts on it is that binary's own suite
# (the FX-10 report's class-sweep table names each row).
#
# ROW FORMAT, one per line between `<<'ROWS'` and `ROWS`:
#   <leaf>|<base>|<companions>|<flag>|<value>
#     leaf        the dotted values.yaml path. `chart_lint` holds this set to
#                 EXACTLY the values file's leaves (a mapping that is empty in
#                 values.yaml, and every list, is one leaf).
#     base        `default`, or an example under charts/logweir/examples/ that
#                 turns on what the leaf configures.
#     companions  `-`, or space-separated `--set…` tokens applied to BOTH
#                 renders: what the base needs so that only the leaf differs.
#                 No token may contain a space.
#     flag        --set | --set-string | --set-json, or the same prefixed with
#                 `!` for a value the chart REFUSES at every non-default
#                 setting (see below).
#     value       the non-default value, verbatim (it may contain spaces).
#
# A PASS is: the probe renders (rc 0), and EITHER its output differs from the
# base's OR the base is refused where the probe renders (the leaf's default is
# refused there, and the value is what makes the chart install — e.g.
# `identity.allowMutableBootstrapImageForDevelopment` over a tag image).
#
# A `!` ROW is the class's other legal answer, "refused": the chart fails the
# render at the non-default value, and its message names the leaf's last path
# segment (so an unrelated failure cannot pass for it). `kafka.secretKey` is the
# example — the operator projects exactly one key name, so the value is fixed
# and any other is refused by name.
# Exit status: 0 when every row passes, 1 otherwise, 2 on a malformed table.
set -uo pipefail
cd "$(dirname "$0")/.."

CHART=charts/logweir
RELEASE=logweir
NAMESPACE=logweir-system
extra=("$@")

if ! command -v helm > /dev/null 2>&1; then
  echo "FAIL: helm is required (the chart gate wants helm 4)" >&2
  exit 1
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/logweir-chart-values.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

fail=0
rows=0
# Bash 3.2 (macOS) has no associative arrays: the base cache is a directory of
# files named by a hash of (base, companions).
render() {
  # render <out> <err> <base> [args...]
  local out="$1" err="$2" base="$3"
  shift 3
  local file_args=()
  if [ "$base" != "default" ]; then
    file_args=(-f "$CHART/examples/$base.values.yaml")
  fi
  helm template "$RELEASE" "$CHART" -n "$NAMESPACE" \
    ${extra[@]+"${extra[@]}"} ${file_args[@]+"${file_args[@]}"} "$@" > "$out" 2> "$err"
}

while IFS='|' read -r leaf base companions flag value; do
  case "$leaf" in '' | '#'*) continue ;; esac
  if [ -z "$base" ] || [ -z "$companions" ] || [ -z "$flag" ] || [ -z "$value" ]; then
    echo "FAIL: malformed row for '$leaf': want <leaf>|<base>|<companions>|<flag>|<value>" >&2
    exit 2
  fi
  expect=renders
  case "$flag" in '!'*) expect=refused; flag="${flag#!}" ;; esac
  case "$flag" in --set | --set-string | --set-json) ;; *)
    echo "FAIL: row '$leaf' uses flag '$flag'; only --set, --set-string and --set-json, optionally prefixed with !" >&2
    exit 2
    ;;
  esac
  if [ "$base" != "default" ] && [ ! -f "$CHART/examples/$base.values.yaml" ]; then
    echo "FAIL: row '$leaf' names base '$base', which is not an example under $CHART/examples" >&2
    exit 2
  fi
  rows=$((rows + 1))
  comp=()
  if [ "$companions" != "-" ]; then
    read -r -a comp <<< "$companions"
  fi
  key="$(printf '%s|%s' "$base" "$companions" | cksum | tr ' ' '-')"
  if [ ! -f "$tmp/base-$key.rc" ]; then
    render "$tmp/base-$key.yaml" "$tmp/base-$key.err" "$base" ${comp[@]+"${comp[@]}"}
    echo "$?" > "$tmp/base-$key.rc"
  fi
  base_rc="$(cat "$tmp/base-$key.rc")"
  render "$tmp/probe.yaml" "$tmp/probe.err" "$base" ${comp[@]+"${comp[@]}"} "$flag" "$leaf=$value"
  probe_rc=$?
  if [ "$expect" = refused ]; then
    if [ "$base_rc" -ne 0 ]; then
      echo "FAIL: $leaf: the base ($base) is refused too, so the refusal proves nothing about this value" >&2
      fail=1
    elif [ "$probe_rc" -eq 0 ]; then
      echo "FAIL: $leaf: $flag $leaf=$value over $base RENDERED; the row says the chart refuses every non-default value" >&2
      fail=1
    elif ! grep -q -- "${leaf##*.}" "$tmp/probe.err"; then
      sed -n '1,4p' "$tmp/probe.err" >&2
      echo "FAIL: $leaf: refused, but the message does not name '${leaf##*.}'" >&2
      fail=1
    else
      echo "   ok  $leaf  (refused by name at $value: the only accepted value is the default)"
    fi
  elif [ "$probe_rc" -ne 0 ]; then
    sed -n '1,4p' "$tmp/probe.err" >&2
    echo "FAIL: $leaf: the probe value ($flag $leaf=$value over $base) does not render; pick one the chart accepts" >&2
    fail=1
  elif [ "$base_rc" -ne 0 ]; then
    echo "   ok  $leaf  (refused at its default over $base; renders at $value)"
  elif cmp -s "$tmp/base-$key.yaml" "$tmp/probe.yaml"; then
    echo "FAIL: $leaf: $flag $leaf=$value over $base renders exactly what its default does. A configured value that reaches nothing (FX-10): read it in a template, or withdraw it" >&2
    fail=1
  else
    echo "   ok  $leaf"
  fi
done <<'ROWS'
# ---- admissionPolicy.*
admissionPolicy.consoleServiceAccountName|admission-policy|-|--set-string|logweir-api-fx10
admissionPolicy.enabled|default|-|--set|true
admissionPolicy.extraPrincipals|admission-policy|-|--set-json|["system:serviceaccount:team-x:fx10-probe"]
# ---- api.* and api.console.*
api.enabled|default|-|--set|true
api.namespaces|demo|-|--set-json|["team-x"]
api.console.enabled|demo|--set-string api.console.mode=localAdmin --set-string api.console.keySecret=fx10-keys|--set|true
api.console.mode|demo|--set api.console.enabled=true --set-string api.console.keySecret=fx10-keys|--set-string|localAdmin
api.console.image|console|-|--set-string|example.invalid/fx10-probe/logweir-console:probe
api.console.imagePullPolicy|console|-|--set-string|IfNotPresent
api.console.replicas|console|-|--set|2
api.console.port|console|-|--set|8485
api.console.localAdminSubject|console|-|--set-string|fx10-admin
api.console.keySecret|console|-|--set-string|fx10-keys
api.console.keyVersion|console-shared|-|--set|2
api.console.publicBaseUrl|console-shared|--set api.console.ingress.enabled=false|--set-string|https://console2.example.com
api.console.sessionMaxAgeSeconds|console-shared|-|--set|600
api.console.trustedProxyCidrs|console-shared|-|--set-json|["198.51.100.0/24"]
api.console.trustedProxyService.namespace|console-shared|-|--set-string|fx10-ingress
api.console.trustedProxyService.name|console-shared|-|--set-string|fx10-proxy
api.console.requireTrustedProxy|console-shared|--set api.console.requireTrustedProxy=false|--set|true
api.console.rateLimits.manualBackupsPerMinute|console|-|--set|11
api.console.rateLimits.manualRestoresPerMinute|console|-|--set|3
api.console.hostAliases|console|-|--set-json|[{"ip":"192.0.2.10","hostnames":["idp.fx10.example"]}]
api.console.oidc.issuer|console-shared|-|--set-string|https://idp.example.com/realms/fx10
api.console.oidc.clientId|console-shared|-|--set-string|fx10-client
api.console.oidc.clientSecret|console-shared|-|--set-string|fx10-oidc
api.console.oidc.caBundle.configMap|console-shared|-|--set-string|fx10-ca
api.console.oidc.caBundle.secret|console-shared|--set-string api.console.oidc.caBundle.configMap=|--set-string|fx10-ca-secret
api.console.oidc.caBundle.key|console-shared|-|--set-string|fx10.crt
api.console.oidc.systemRoots|console-shared|-|--set|false
api.console.roles.revision|console-shared|-|--set-string|fx10-revision
api.console.roles.bindings|console-shared|-|--set-json|[{"role":"viewer","namespace":"team-b","groups":["fx10-viewers"]}]
api.console.ingress.enabled|console-shared|--set api.console.ingress.enabled=false|--set|true
api.console.ingress.className|console-shared|-|--set-string|fx10-class
api.console.ingress.host|console-shared|--set-string api.console.publicBaseUrl=https://console2.example.com|--set-string|console2.example.com
api.console.ingress.tlsSecretName|console-shared|-|--set-string|fx10-tls
api.console.ingress.annotations|console-shared|-|--set-json|{"fx10.example/probe":"yes"}
api.console.networkPolicy.enabled|console|--set api.console.networkPolicy.enabled=false|--set|true
api.console.networkPolicy.ingressNamespace|console-shared|-|--set-string|fx10-ingress
api.console.networkPolicy.ingressPodLabels|console-shared|-|--set-json|{"fx10":"probe"}
api.console.networkPolicy.oidcCIDRs|console-shared|-|--set-json|["198.51.100.7/32"]
api.console.networkPolicy.oidcPeers|console-shared|-|--set-json|[{"namespace":"dex","podLabels":{"app":"dex"},"port":5556}]
api.console.resources.requests.cpu|console|-|--set-string|30m
api.console.resources.requests.memory|console|-|--set-string|70Mi
api.console.resources.limits.memory|console|-|--set-string|300Mi
api.console.nodeSelector|console|-|--set-json|{"fx10":"probe"}
api.console.tolerations|console|-|--set-json|[{"key":"fx10","operator":"Exists"}]
api.console.affinity|console|-|--set-json|{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"fx10","operator":"Exists"}]}]}}}
# ---- approvalPolicy.*
approvalPolicy.default|approval-policy|-|--set-string|strict
approvalPolicy.allowOrdinaryConfirmation|approval-policy|--set approvalPolicy.allowOrdinaryConfirmation=false|--set|true
approvalPolicy.policies|approval-policy|-|--set-json|[{"name":"team-ordinary","mode":"Ordinary","maxAgeSeconds":600},{"name":"prod-governed","mode":"Governed","maxAgeSeconds":86400,"requireDistinctPrincipal":true}]
approvalPolicy.namespaces|approval-policy|-|--set-json|{"team-c":"prod-governed"}
approvalPolicy.confirmationKeySecret|approval-policy|-|--set-string|fx10-confirmation
# ---- archive.*
archive.url|msk|-|--set-string|s3://fx10-bucket/logweir
archive.s3.endpoint|msk|-|--set-string|http://fx10-minio.kafka.svc:9000
archive.s3.region|msk|-|--set-string|eu-west-1
archive.s3.allowHttp|msk|--set archive.s3.allowHttp=false|--set|true
archive.s3.virtualHostedStyle|msk|-|--set|true
# ---- the installation policy: checks.*, runs.*, engine.*, evidence.*
checks.maxActivePerNamespace|default|-|--set|5
checks.maxActiveTotal|default|-|--set|21
checks.maxActiveDiscoveriesPerConnection|default|-|--set|2
checks.maxEvidenceFetchActivePerNamespace|default|-|--set|5
checks.discovery.freshSeconds|default|-|--set|901
checks.discovery.retentionSeconds|default|-|--set|86401
checks.discovery.keepPerConnection|default|-|--set|6
checks.discovery.hardMaxTopics|default|-|--set|40000
checks.discovery.visibilityAttestations|default|-|--set-json|[{"id":"att-fx10","namespace":"team-a","kafkaCluster":"source","clusterId":"M29I2S7FQPyHBEX12Vx7XA","principal":"User:backup","attestedBy":"platform-admin@example.invalid","attestedAt":"2026-09-15T00:00:00Z","expiresAt":"2026-12-15T00:00:00Z","statement":"FX-10 probe"}]
checks.preflight.retentionSeconds|default|-|--set|3601
runs.maxManualBackupsActivePerNamespace|default|-|--set|5
runs.maxManualRestoresActivePerNamespace|default|-|--set|3
engine.allowUnverifiedCustomCa|default|-|--set|true
evidence.controllerIdentityLocations|default|-|--set-json|[{"endpoint":"","region":"","bucket":"fx10-bucket"}]
# ---- the controller
controllerImage|default|-|--set-string|example.invalid/fx10-probe/weirkeeper:probe
imagePullPolicy|default|-|--set-string|IfNotPresent
runnerImage|default|-|--set-string|example.invalid/fx10-probe/logweir:probe
runnerImagePullPolicy|default|-|--set-string|IfNotPresent
imagePullSecrets|default|-|--set-json|[{"name":"fx10-regcred"}]
controller.logLevel|default|-|--set-string|debug
controller.failFastSeconds|default|-|--set|120
controller.jobTtlSeconds|default|-|--set|7200
controller.watchNamespaces|default|-|--set-json|["team-x"]
controller.resources.requests.cpu|default|-|--set-string|60m
controller.resources.requests.memory|default|-|--set-string|130Mi
controller.resources.limits.memory|default|-|--set-string|600Mi
controller.nodeSelector|default|-|--set-json|{"fx10":"probe"}
controller.tolerations|default|-|--set-json|[{"key":"fx10","operator":"Exists"}]
controller.affinity|default|-|--set-json|{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"fx10","operator":"Exists"}]}]}}}
notify.allowInsecureSinks|default|-|--set|true
retention.enabled|default|-|--set|true
# ---- identity, environment, placement
environment|default|-|--set-string|fx10
kubernetes.namespace|demo|-|--set-string|team-x
kubernetes.connectionsNamespace|msk|-|--set-string|fx10-connections
kubernetes.nodeSelector|default|-|--set-json|{"fx10":"probe"}
kubernetes.tolerations|default|-|--set-json|[{"key":"fx10","operator":"Exists"}]
kubernetes.affinity|default|-|--set-json|{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"fx10","operator":"Exists"}]}]}}}
identity.enabled|default|-|--set|false
identity.bootstrapImage|default|-|--set-string|example.invalid/fx10-probe/logweir@sha256:0000000000000000000000000000000000000000000000000000000000000f10
identity.bootstrapImagePullPolicy|default|-|--set-string|Always
identity.allowMutableBootstrapImageForDevelopment|default|--set-string identity.bootstrapImage=example.invalid/fx10-probe/logweir:probe --set-string identity.bootstrapImagePullPolicy=Never|--set|true
identity.publicConfigMapName|default|-|--set-string|fx10-signing-trust
identity.authorizedRunnerNamespaces|default|-|--set-json|["team-x"]
identity.kubernetesApiCIDRs|default|-|--set-json|["10.96.0.1/32"]
identity.externalSecret.name|default|-|--set-string|fx10-signer
identity.externalSecret.key|identity-external|-|--set-string|fx10.pem
identity.installationTrust.enabled|default|-|--set|false
identity.installationTrust.policyName|default|-|--set-string|fx10-installation
identity.installationTrust.allowedTargetClusterIds|default|-|--set-json|["fx10-target"]
identity.resources.requests.cpu|default|-|--set-string|12m
identity.resources.requests.memory|default|-|--set-string|40Mi
identity.resources.limits.memory|default|-|--set-string|150Mi
# ---- kafka.*
kafka.enabled|default|--set-string kafka.bootstrapServers=b-1.example:9096|--set|true
kafka.name|msk|-|--set-string|fx10-source
kafka.bootstrapServers|msk|-|--set-string|b-9.example.kafka.us-west-2.amazonaws.com:9096
kafka.security.protocol|msk|-|--set-string|SASL_PLAINTEXT
kafka.security.mechanism|msk|--set-string kafka.security.mechanism=|--set-string|SCRAM-SHA-512
kafka.username|msk|-|--set-string|fx10-user
kafka.secretRef|msk|-|--set-string|fx10-secret
kafka.secretKey|msk|-|!--set-string|fx10-password
kafka.target.name|msk|-|--set-string|fx10-target
kafka.target.bootstrapServers|msk|-|--set-string|b-9.scratch.kafka.us-west-2.amazonaws.com:9096
kafka.target.security.protocol|msk|-|--set-string|SASL_PLAINTEXT
kafka.target.security.mechanism|msk|--set-string kafka.target.security.mechanism=|--set-string|SCRAM-SHA-512
kafka.target.username|msk|-|--set-string|fx10-user
kafka.target.secretRef|msk|-|--set-string|fx10-scratch
kafka.target.secretKey|msk|-|!--set-string|fx10-password
kafka.target.markerTopic|msk|-|--set-string|fx10.scratch
# ---- minio.*
minio.enabled|default|-|--set|true
minio.image|demo|-|--set-string|example.invalid/fx10-probe/minio@sha256:0000000000000000000000000000000000000000000000000000000000000f10
minio.mcImage|demo|-|--set-string|example.invalid/fx10-probe/mc@sha256:0000000000000000000000000000000000000000000000000000000000000f10
minio.rootUser|demo|-|--set-string|fx10-root
minio.rootPassword|demo|-|--set-string|fx10-password
minio.persistence.enabled|demo|--set minio.persistence.enabled=true|--set|false
minio.persistence.size|demo|--set minio.persistence.enabled=true|--set-string|6Gi
minio.persistence.storageClassName|demo|--set minio.persistence.enabled=true|--set-string|fx10-sc
minio.resources.requests.cpu|demo|-|--set-string|60m
minio.resources.requests.memory|demo|-|--set-string|300Mi
minio.nodeSelector|demo|-|--set-json|{"fx10":"probe"}
minio.tolerations|demo|-|--set-json|[{"key":"fx10","operator":"Exists"}]
minio.affinity|demo|-|--set-json|{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"fx10","operator":"Exists"}]}]}}}
# ---- demoKafka.*
demoKafka.enabled|default|-|--set|true
demoKafka.image|demo|-|--set-string|example.invalid/fx10-probe/kafka@sha256:0000000000000000000000000000000000000000000000000000000000000f10
demoKafka.clusterIds.source|demo|-|--set-string|FX10sourceAAAAAAAAAAAA
demoKafka.clusterIds.target|demo|-|--set-string|FX10targetAAAAAAAAAAAA
demoKafka.seed.recordsPerTopic|demo|-|--set|201
demoKafka.resources.requests.cpu|demo|-|--set-string|150m
demoKafka.resources.requests.memory|demo|-|--set-string|600Mi
demoKafka.resources.limits.memory|demo|-|--set-string|2Gi
demoKafka.nodeSelector|demo|-|--set-json|{"fx10":"probe"}
demoKafka.tolerations|demo|-|--set-json|[{"key":"fx10","operator":"Exists"}]
demoKafka.affinity|demo|-|--set-json|{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"fx10","operator":"Exists"}]}]}}}
# ---- ui.*
ui.enabled|default|-|--set|true
ui.image|demo|-|--set-string|example.invalid/fx10-probe/logweir-ui:probe
ui.imagePullPolicy|demo|-|--set-string|IfNotPresent
ui.namespaces|demo|-|--set-json|["team-x"]
ui.nodeSelector|demo|-|--set-json|{"fx10":"probe"}
ui.tolerations|demo|-|--set-json|[{"key":"fx10","operator":"Exists"}]
ui.affinity|demo|-|--set-json|{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"fx10","operator":"Exists"}]}]}}}
ROWS

if [ "$rows" -lt 100 ]; then
  echo "FAIL: only $rows row(s) ran; the table has gone quiet" >&2
  fail=1
fi
if [ "$fail" -ne 0 ]; then
  echo "check-chart-values: FAIL ($rows rows)" >&2
  exit 1
fi
echo "check-chart-values: every one of $rows chart values, changed alone, changes the render"
