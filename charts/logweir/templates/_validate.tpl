{{- /*
Every refusal this chart makes, in the order they are evaluated.
`ui/api-rbac.yaml` includes `logweir.validate` first thing: Helm renders the
deepest template paths first and each level in reverse name order, so that is
the first template rendered, and a values file is refused before anything
renders, with the first refusal below that it meets.
*/ -}}
{{- define "logweir.validate" -}}
{{- /*
Withdrawn keys are refused by name, never ignored, so an upgrade with an older
values file cannot lose what the key used to configure without being told.
*/ -}}
{{- if hasKey .Values "ui" -}}
{{- fail "ui.* is withdrawn (PROD-17.1, release-notes item 75): the legacy kubectl proxy page and the logweir-ui image are gone. Remove the ui block from your values file and use the console, api.console.* (docs/install.md §5e)." -}}
{{- end -}}
{{- end -}}
