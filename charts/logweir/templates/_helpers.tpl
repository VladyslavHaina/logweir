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
A UI file's ConfigMap key. A ConfigMap key may not contain `/`, so
`ui/pages/approvals.js` becomes `pages__approvals.js`; the Deployment's volume
`items` map each key back to its path under `/ui`.
*/ -}}
{{- define "logweir.ui.key" -}}
{{ . | trimPrefix "ui/" | replace "/" "__" }}
{{- end -}}
