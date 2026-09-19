{{- /*
The chart's helpers. Every namespaced object renders into `.Release.Namespace`
and nothing here spells a namespace by name; the release name prefixes only
the OPTIONAL components (`<release>-minio`, `<release>-kafka-*`, `<release>-ui`),
while the control plane keeps the names `logweir.yaml` ships — `weirkeeper`,
`logweir-viewer`/`-operator`/`-approver`, `logweir-runner-egress`,
`logweir-runner` — because those are cluster-scoped or code-named constants and
Global Constraint 30 allows one controller per cluster anyway.
*/ -}}

{{- define "logweir.labels" -}}
app.kubernetes.io/name: logweir
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- include "logweir.environmentLabel" . }}
{{- end -}}

{{- /* The in-cluster MinIO's Service DNS name, short form. */ -}}
{{- define "logweir.minio.host" -}}
{{ .Release.Name }}-minio.{{ .Release.Namespace }}.svc
{{- end -}}

{{- /*
The archive settings, with `minio.enabled` supplying the defaults an empty value
leaves open. Each helper returns "" when there is nothing to render, which is
the shipped install: no archive handle, no retention report, no addressing env.
*/ -}}
{{- define "logweir.archive.url" -}}
{{- if .Values.archive.url -}}
{{ .Values.archive.url }}
{{- else if .Values.minio.enabled -}}
s3://kafka-backups/{{ .Release.Name }}
{{- end -}}
{{- end -}}

{{- define "logweir.archive.endpoint" -}}
{{- if .Values.archive.s3.endpoint -}}
{{ .Values.archive.s3.endpoint }}
{{- else if .Values.minio.enabled -}}
http://{{ include "logweir.minio.host" . }}:9000
{{- end -}}
{{- end -}}

{{- define "logweir.archive.region" -}}
{{- if .Values.archive.s3.region -}}
{{ .Values.archive.s3.region }}
{{- else if .Values.minio.enabled -}}
us-east-1
{{- end -}}
{{- end -}}

{{- /* One demo broker's ClusterIP Service DNS name. Takes a dict {release, namespace, role}. */ -}}
{{- define "logweir.kafka.host" -}}
{{ .release }}-kafka-{{ .role }}.{{ .namespace }}.svc.cluster.local
{{- end -}}

{{- /*
`logweir.ui.key` LIVED HERE UNTIL TASK 39 and is DELETED, not kept beside its
replacement. It turned a UI file's path into a ConfigMap key
(`ui/pages/approvals.js` -> `pages__approvals.js`) for the ConfigMap this chart
built from its own copy of `ui/`. The page now arrives as the `logweir-ui`
IMAGE — `ui.image`, built by `Dockerfile.ui`, asserted by
`scripts/check-image-ui.sh` — so there is no ConfigMap, no copy of `ui/` under
`charts/`, and nothing for this helper to key. A template helper with no caller
is a helper the next reader has to prove is dead.
*/ -}}

{{- /*
Task 38. The three cross-cutting values the owner's own file asked for, in one
place: a label, node placement, and pull secrets. Each renders NOTHING when its
value is empty, so the shipped default render is byte-identical to what it was.
*/ -}}

{{- /*
`environment` — a LABEL and nothing else (it switches no behaviour). Included by
`logweir.labels`, so every object this chart renders carries it.
*/ -}}
{{- define "logweir.environmentLabel" -}}
{{- with .Values.environment }}
logweir.dev/environment: {{ . | quote }}
{{- end }}
{{- end -}}

{{- /*
Node placement for EVERY pod this chart renders. Takes a dict:
  root — the top-level context
  over — a component's own values map, or omitted; a non-empty `nodeSelector`,
         `tolerations` or `affinity` there overrides `kubernetes.<key>`
Returns "" when nothing is set, so the call site renders nothing at all.

RUNNER JOBS ARE NOT HERE. Jobs are created by the operator, not by this chart,
and node placement for them is not implemented — `README.md` says so by name.
`imagePullSecrets` DO reach them, through the runner ServiceAccount.
*/ -}}
{{- define "logweir.placement" -}}
{{- $k := .root.Values.kubernetes -}}
{{- $c := .over | default dict -}}
{{- $out := dict -}}
{{- with ($c.nodeSelector | default $k.nodeSelector) }}{{- $_ := set $out "nodeSelector" . }}{{- end }}
{{- with ($c.tolerations | default $k.tolerations) }}{{- $_ := set $out "tolerations" . }}{{- end }}
{{- with ($c.affinity | default $k.affinity) }}{{- $_ := set $out "affinity" . }}{{- end }}
{{- if $out }}{{ toYaml $out }}{{- end }}
{{- end -}}

{{- /*
`imagePullSecrets`, as the block a ServiceAccount and a PodSpec both spell the
same way. Empty renders nothing.
*/ -}}
{{- define "logweir.imagePullSecrets" -}}
{{- with .Values.imagePullSecrets -}}
imagePullSecrets:
{{ toYaml . }}
{{- end -}}
{{- end -}}

{{- /*
The namespaces the UI is bound in: the release namespace, plus `ui.namespaces`,
plus `kubernetes.namespace` when that is set and `ui.namespaces` is empty —
ruling 11's ONE use of that key, because a RoleBinding needs a namespace name
the chart cannot derive. DEDUPLICATED: `-n kafka` beside
`kubernetes.namespace: kafka` would otherwise render the same RoleBinding twice
and refuse the install.
*/ -}}
{{- define "logweir.ui.namespaces" -}}
{{- $extra := .Values.ui.namespaces -}}
{{- if and (not $extra) .Values.kubernetes.namespace -}}
{{- $extra = list .Values.kubernetes.namespace -}}
{{- end -}}
{{- (prepend ($extra | default list) .Release.Namespace) | uniq | toJson -}}
{{- end -}}

{{- /*
THE CONSOLE/API ServiceAccount'S NAME, IN ONE PLACE — review finding **F3**.

Two templates need it and they used to spell it differently: `ui/api-rbac.yaml`
rendered `{{ .Release.Name }}-api` while `admission-policy.yaml` took the fixed
string `admissionPolicy.consoleServiceAccountName`, whose default is
`logweir-api`. Those agree under the default release name and NOWHERE ELSE:
`helm install myrel …` rendered `ServiceAccount myrel-api` beside a
ValidatingAdmissionPolicy whose whole effect is
`request.userInfo.username in ["system:serviceaccount:<ns>:logweir-api"]` — a
fence that installs, reads as enabled, and matches nobody, while the principal
it was meant to bound holds `create` on Secrets that RBAC cannot narrow by
shape.

So the name is defined once, here, and both files call it. The value is still
the one an installation OVERRIDES with (a console run out of band has whatever
account its operator gave it, and `extraPrincipals` exists for the rest), but a
value that DISAGREES with an account this chart is itself rendering is refused
at render time rather than installed — see `admission-policy.yaml`.
*/ -}}
{{- define "logweir.api.serviceAccountName" -}}
{{- printf "%s-api" .Release.Name -}}
{{- end -}}

{{- /*
The namespaces the console/API ServiceAccount is bound in — the same shape, and
a SEPARATE key on purpose. The legacy proxy and the console are two principals
with two arguments (`templates/ui/api-rbac.yaml`'s header), and an installation
that serves the console for ten namespaces while the local proxy is bound in
one must be able to say so without widening the proxy. Empty `api.namespaces`
falls back to `kubernetes.namespace` exactly as the proxy's does, so the common
case still needs one key and not two.
*/ -}}
{{- define "logweir.api.namespaces" -}}
{{- $extra := .Values.api.namespaces -}}
{{- if and (not $extra) .Values.kubernetes.namespace -}}
{{- $extra = list .Values.kubernetes.namespace -}}
{{- end -}}
{{- (prepend ($extra | default list) .Release.Namespace) | uniq | toJson -}}
{{- end -}}

{{- /*
One `KafkaCluster.spec.auth` from the owner's flat `security:` shape. Takes the
per-cluster values map. THE MAPPING IS THE CHART'S JOB, and every combination
outside it is a `fail` at render time rather than an object that installs and
cannot authenticate: the CRD's enum is `plaintext|scramSha512` and the client
speaks SCRAM-SHA-512 only.
*/ -}}
{{- define "logweir.kafka.auth" -}}
{{- $c := .cluster -}}
{{- $protocol := $c.security.protocol | default "PLAINTEXT" -}}
{{- $mechanism := $c.security.mechanism | default "" -}}
{{- $tls := false -}}
{{- $mode := "" -}}
{{- if eq $protocol "PLAINTEXT" -}}
{{- $mode = "plaintext" -}}
{{- else if and (eq $protocol "SASL_SSL") (eq $mechanism "SCRAM-SHA-512") -}}
{{- $mode = "scramSha512" -}}
{{- $tls = true -}}
{{- else if and (eq $protocol "SASL_PLAINTEXT") (eq $mechanism "SCRAM-SHA-512") -}}
{{- $mode = "scramSha512" -}}
{{- else -}}
{{- fail (printf "kafka: security.protocol %q with security.mechanism %q is not supported. Logweir speaks PLAINTEXT, or SCRAM-SHA-512 over SASL_SSL or SASL_PLAINTEXT, and nothing else — the KafkaCluster CRD's auth.mode enum is plaintext|scramSha512. A silently rendered PLAIN or SCRAM-SHA-256 would be a chart that installs and cannot authenticate." $protocol $mechanism) -}}
{{- end -}}
{{- if ne $mode "plaintext" -}}
{{- if not $c.username }}{{ fail "kafka: security.mechanism SCRAM-SHA-512 needs a username" }}{{ end -}}
{{- if not $c.secretRef }}{{ fail "kafka: security.mechanism SCRAM-SHA-512 needs a secretRef — the name of a Secret in the release namespace holding the password" }}{{ end -}}
{{- end -}}
{{- if and $c.secretKey (ne $c.secretKey "password") -}}
{{- fail (printf "kafka: secretKey is %q. The operator projects exactly one key name into the probe — `password` (weirkeeper::controllers::restore::TARGET_PASSWORD_SECRET_KEY) — so any other value renders a KafkaCluster whose probe cannot read its credential." $c.secretKey) -}}
{{- end -}}
mode: {{ $mode }}
tls: {{ $tls }}
{{- with $c.username }}
username: {{ . | quote }}
{{- end }}
{{- with $c.secretRef }}
secretRef:
  name: {{ . | quote }}
{{- end }}
{{- end -}}

{{- /*
`bootstrapServers` as the CRD wants it — a LIST — from either shape the owner
may write: a comma-separated string (their MSK endpoint, copied from the AWS
console) or a YAML list. Whitespace around each entry is trimmed.
*/ -}}
{{- define "logweir.kafka.bootstrapServers" -}}
{{- $v := .servers -}}
{{- $list := list -}}
{{- if kindIs "string" $v -}}
{{- range (splitList "," $v) -}}
{{- $t := trim . -}}
{{- if $t }}{{ $list = append $list $t }}{{ end -}}
{{- end -}}
{{- else -}}
{{- range $v -}}
{{- $t := trim (toString .) -}}
{{- if $t }}{{ $list = append $list $t }}{{ end -}}
{{- end -}}
{{- end -}}
{{- if not $list }}{{ fail "kafka: bootstrapServers is empty — give a comma-separated string or a list of host:port" }}{{ end -}}
{{ toYaml $list }}
{{- end -}}
