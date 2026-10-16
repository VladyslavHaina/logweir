{{- /*
Namespaced objects render into `.Release.Namespace`. The release name prefixes
only the optional components (`<release>-minio`, `<release>-kafka-*`,
`<release>-api`); the control plane keeps fixed names, because they are
cluster-scoped or constants the code names, and one controller runs per cluster.
*/ -}}

{{- define "logweir.labels" -}}
app.kubernetes.io/name: logweir
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- /*
A label value is at most 63 characters and ends alphanumeric; the published
chart's `<version>-sha-<40 hex>` is 64 here, so it is cut the way Helm's own
scaffold cuts it. The full version stays in `helm list`.
*/}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" | trimSuffix "." }}
{{- include "logweir.environmentLabel" . }}
{{- end -}}

{{- /* The in-cluster MinIO's Service DNS name, short form. */ -}}
{{- define "logweir.minio.host" -}}
{{ .Release.Name }}-minio.{{ .Release.Namespace }}.svc
{{- end -}}

{{- /*
The archive settings, with `minio.enabled` supplying the defaults an empty value
leaves open. Each helper returns "" when there is nothing to render, which is
the shipped install: no archive handle, no addressing env.
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

{{- /* `environment`: a label on every object, and nothing else. */ -}}
{{- define "logweir.environmentLabel" -}}
{{- with .Values.environment }}
logweir.dev/environment: {{ . | quote }}
{{- end }}
{{- end -}}

{{- /*
Node placement for every pod this chart renders. Takes a dict: `root`, the top
context, and `over`, a component's own values, whose non-empty `nodeSelector`,
`tolerations` or `affinity` overrides `kubernetes.<key>`. Empty renders nothing.
Runner Jobs are the operator's, not the chart's: placement does not reach them
(not implemented); `imagePullSecrets` do, through the runner ServiceAccount.
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

{{- /* `imagePullSecrets` as a ServiceAccount and a PodSpec both spell it. */ -}}
{{- define "logweir.imagePullSecrets" -}}
{{- with .Values.imagePullSecrets -}}
imagePullSecrets:
{{ toYaml . }}
{{- end -}}
{{- end -}}

{{- /*
The console/API ServiceAccount's name, defined once: `ui/api-rbac.yaml` renders
the account and the admission policy fences it. Two spellings agreed only under
the default release name, and a fence naming nobody installs and protects
nothing. `_validate.tpl` refuses an `admissionPolicy.consoleServiceAccountName`
that disagrees with the account this chart renders.
*/ -}}
{{- define "logweir.api.serviceAccountName" -}}
{{- printf "%s-api" .Release.Name -}}
{{- end -}}

{{- /*
One `KafkaCluster.spec.auth` from the flat `security:` values. The CRD's enum is
`plaintext|scramSha512` and the client speaks SCRAM-SHA-512 only; every other
pair is refused in `_validate.tpl` before this runs.
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
`bootstrapServers` as the CRD's list, from a comma-separated string (an MSK
endpoint as the AWS console shows it) or a list; each entry trimmed.
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
{{ toYaml $list }}
{{- end -}}

{{- /*
The workspace label of the chart's own objects (`kafka.enabled`'s KafkaClusters
and MinIO's `logweir-s3`): the first entry of `workspaces`.
*/ -}}
{{- define "logweir.workspaceLabel" -}}
logweir.dev/workspace: {{ first .Values.workspaces | quote }}
{{- end -}}
