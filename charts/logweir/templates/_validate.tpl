{{- /*
Every refusal this chart makes, in the order they are evaluated.
`ui/api-rbac.yaml` includes `logweir.validate` first thing: Helm renders the
deepest template paths first and each level in reverse name order, so that is
the first template rendered, and a values file is refused before anything
renders, with the first refusal below that it meets. The sections follow the
order the templates that used to hold them were rendered in, so a values file
that breaks two rules is refused for the same one as before.

`fail` stops `helm template`, `helm lint`, `helm install` and `helm upgrade`
alike: each refusal is a chart that does not install rather than an
installation that comes up wrong. Several repeat a refusal the binaries make
at start; this one is made while the operator still holds the values file.
*/ -}}
{{- define "logweir.validate" -}}
{{- /*
Withdrawn keys are refused by name, never ignored, so an upgrade with an older
values file cannot lose what the key used to configure without being told.
(`notify`, the other withdrawn block, is refused further down, where
deployment.yaml used to refuse it.)
*/ -}}
{{- if hasKey .Values "ui" -}}
{{- fail "ui.* is withdrawn (PROD-17.1, release-notes item 75): the legacy kubectl proxy page and the logweir-ui image are gone. Remove the ui block from your values file and use the console, api.console.* (docs/install.md §5e)." -}}
{{- end -}}
{{- if hasKey .Values.kafka "markerTopic" -}}
{{- fail "kafka.markerTopic is not read by this chart; set kafka.target.markerTopic instead (rc.2 accepted and ignored it). Remove kafka.markerTopic from your values file, or on helm upgrade --reuse-values pass --set kafka.markerTopic=null" -}}
{{- end -}}
{{- /* The console (api.console.*). */ -}}
{{- if .Values.api.console.enabled -}}
{{- $c := .Values.api.console -}}
{{- if not .Values.api.enabled -}}
{{- fail "api.console.enabled requires api.enabled: the console runs AS the principal `api.enabled` renders (its ServiceAccount, its two ClusterRoles and its per-namespace RoleBindings). A pod without them holds no grant on any Logweir kind and every route would 403. Set api.enabled=true." -}}
{{- end -}}
{{- if not $c.image -}}
{{- fail "api.console.image is empty. It must name the logweir-console image (Dockerfile.console): `logweir-api` plus the twenty-six page files it serves at /ui/." -}}
{{- end -}}
{{- if not $c.keySecret -}}
{{- fail "api.console.keySecret is empty. The console needs a cursor MAC key of at least 32 bytes (and, in shared mode, a session key), and this chart DELIBERATELY does not generate one: a Helm-generated key changes on every render, which would make the rendered fixtures unstable and the key unrecoverable on upgrade. D0 requires these to be persistent, versioned Secrets created or adopted EXPLICITLY. Create one and name it here; charts/logweir/README.md carries the command." -}}
{{- end -}}
{{- if lt (int $c.replicas) 1 -}}
{{- fail "api.console.replicas must be at least 1." -}}
{{- end -}}
{{- if not $c.mode -}}
{{- fail "api.console.mode is empty, and there is no default. The two modes are `localAdmin` — THE IN-CLUSTER ADMINISTRATOR MODE: a loopback listener with no Service at all, reached only with `kubectl port-forward deploy/<release>-api`, one configured administrator, no identity provider, the narrowly bound <release>-api ServiceAccount, and readiness not gated on OIDC — and `shared`, the SSO console, which is the ONLY mode that may be exposed through a Service or an Ingress. `crates/logweir-api/src/config.rs` refuses a configuration file that forgets to name a mode rather than reading it as the more permissive one; this chart refuses the same way and for the same reason. Name one." -}}
{{- end -}}
{{- if eq $c.mode "localAdmin" -}}
{{- if not $c.localAdminSubject -}}
{{- fail "api.console.localAdminSubject is empty. localAdmin mode has no identity provider: every request is attributed to this one configured administrator, so the name it is recorded under is not optional." -}}
{{- end -}}
{{- if $c.requireTrustedProxy -}}
{{- fail "api.console.requireTrustedProxy belongs to api.console.mode=shared. The in-cluster administrator mode binds loopback and is reached only through kubectl port-forward; there is no proxy in front of it to trust." -}}
{{- end -}}
{{- if ($c.trustedProxyService | default dict).name -}}
{{- fail "api.console.trustedProxyService belongs to api.console.mode=shared. The in-cluster administrator mode binds loopback and is reached only through kubectl port-forward; there is no proxy in front of it to trust, and no Role in the ingress namespace should be granted for one." -}}
{{- end -}}
{{- if $c.trustedProxy -}}
{{- fail "api.console.trustedProxy belongs to api.console.mode=shared. The in-cluster administrator mode binds loopback and is reached only through kubectl port-forward; there is no proxy in front of it to name or to opt out of." -}}
{{- end -}}
{{- if $c.ingress.enabled -}}
{{- fail "api.console.ingress.enabled with api.console.mode=localAdmin is refused. D0: the local administrator path \"must not bind 0.0.0.0, get an Ingress, or be described as shared-console mode\" — it authenticates nobody, so an Ingress in front of it is an unauthenticated shared console. Set api.console.mode=shared and configure OIDC and TLS, or reach localAdmin mode with `kubectl port-forward`." -}}
{{- end -}}
{{- else if eq $c.mode "shared" -}}
{{- if not (hasPrefix "https://" $c.publicBaseUrl) -}}
{{- fail (printf "api.console.publicBaseUrl is %q. Shared mode requires the EXACT https:// URL the browser uses — TLS at the shared entry point is required, not recommended (D0). The OIDC redirect URI is this value plus /auth/callback, so a value that is wrong here is a login that cannot complete." $c.publicBaseUrl) -}}
{{- end -}}
{{- if or (hasSuffix "/" $c.publicBaseUrl) (contains "@" $c.publicBaseUrl) -}}
{{- fail (printf "api.console.publicBaseUrl is %q. It carries no path, no trailing slash and no userinfo: the redirect URI registered with the provider is this value plus /auth/callback." $c.publicBaseUrl) -}}
{{- end -}}
{{- if not $c.oidc.issuer -}}
{{- fail "api.console.oidc.issuer is empty. Shared mode is SSO: the issuer is compared for exact equality with the ID token's `iss`, so there is no default and no discovery from the public URL." -}}
{{- end -}}
{{- if not $c.oidc.clientId -}}
{{- fail "api.console.oidc.clientId is empty. It is also the expected audience of every ID token." -}}
{{- end -}}
{{- if not $c.oidc.clientSecret -}}
{{- fail "api.console.oidc.clientSecret is empty. It is the NAME of a Secret in the release namespace holding the client secret under the key `clientSecret` — never the secret itself: a literal here would be a credential in a rendered manifest, in `helm get manifest` and in charts/logweir/rendered/." -}}
{{- end -}}
{{- if not $c.roles.revision -}}
{{- fail "api.console.roles.revision is empty. It is the provenance of every authorization decision in the audit log; a role table with no revision is a decision nobody can trace to the values file that made it." -}}
{{- end -}}
{{- if not $c.roles.bindings -}}
{{- fail "api.console.roles.bindings is empty. Shared mode has no default namespace and no implicit grant: with no binding every authenticated user is authorized for nothing, which is a console that installs and serves 403." -}}
{{- end -}}
{{- /*
Shared mode is never rendered inside the controller's Job-create authority. The
console's keys live in the release namespace, and a Job the controller creates
there could mount them; so shared mode needs a scoped controller
(`controller.watchNamespaces`) whose list leaves the release namespace out.
*/ -}}
{{- $watch := .Values.controller.watchNamespaces | default list -}}
{{- if not $watch -}}
{{- fail (printf "api.console.mode=shared requires controller.watchNamespaces (D0 stage 5). The console's session and cursor keys live in the release namespace %q, and with the default cluster-wide ClusterRoleBinding the controller may create a Job there that mounts them — the signing-oracle residual O1 extended to the console's keys, which D0 says must not be declared secure. List the namespaces Backups and Restores run in; the controller then holds Job-create authority there and nowhere else. charts/logweir/README.md §controller.watchNamespaces has the migration." .Release.Namespace) -}}
{{- end -}}
{{- if has .Release.Namespace $watch -}}
{{- fail (printf "controller.watchNamespaces includes the release namespace %q, which holds the shared console's keys (api.console.keySecret). The controller may create Jobs in every listed namespace, so this list would put the keys back inside its Job-create authority (D0 stage 5). Run Backups and Restores in other namespaces and remove %q from the list." .Release.Namespace .Release.Namespace) -}}
{{- end -}}
{{- $proxySvc := $c.trustedProxyService | default dict -}}
{{- if or (and $proxySvc.name (not $proxySvc.namespace)) (and $proxySvc.namespace (not $proxySvc.name)) -}}
{{- fail (printf "api.console.trustedProxyService names only half a Service (namespace %q, name %q). Name both — the ingress controller's Service and the namespace it runs in — or neither." (toString $proxySvc.namespace) (toString $proxySvc.name)) -}}
{{- end -}}
{{- if and (eq (toString $c.trustedProxy) "none") (or $c.trustedProxyCidrs $proxySvc.name) -}}
{{- fail "api.console.trustedProxy=none says the console trusts no proxy, but api.console.trustedProxyService or api.console.trustedProxyCidrs names one. Remove one side: name the ingress controller (and leave trustedProxy empty), or opt out." -}}
{{- end -}}
{{- if and $c.requireTrustedProxy (not $c.trustedProxyCidrs) (not $proxySvc.name) -}}
{{- fail "api.console.requireTrustedProxy needs api.console.trustedProxyService (the ingress controller's Service, whose serving pods are then trusted) or api.console.trustedProxyCidrs: with nothing to trust, the entry point would refuse every request (421) and the console would look like an outage." -}}
{{- end -}}
{{- $ca := $c.oidc.caBundle | default dict -}}
{{- if and $ca.configMap $ca.secret -}}
{{- fail (printf "api.console.oidc.caBundle names both ConfigMap %q and Secret %q. The issuer's CA bundle is ONE object; name the one that holds it." $ca.configMap $ca.secret) -}}
{{- end -}}
{{- if and (or $ca.configMap $ca.secret) (not $ca.key) -}}
{{- fail "api.console.oidc.caBundle.key is empty. It is the key, inside the ConfigMap or Secret, of the PEM file holding the issuer's CA certificates (ca.crt by convention)." -}}
{{- end -}}
{{- if and (eq (toString $c.oidc.systemRoots) "false") (not (or $ca.configMap $ca.secret)) -}}
{{- fail "api.console.oidc.systemRoots=false without api.console.oidc.caBundle: the console would trust no certificate at all and could never reach its issuer. Name the bundle that replaces the system roots, or keep them." -}}
{{- end -}}
{{- range $alias := ($c.hostAliases | default list) -}}
{{- if or (not $alias.ip) (not $alias.hostnames) -}}
{{- fail "api.console.hostAliases entries need an `ip` and at least one hostname: {ip: <address>, hostnames: [<name>, …]}." -}}
{{- end -}}
{{- if contains "*" (toString $alias.hostnames) -}}
{{- fail (printf "api.console.hostAliases for %s names a wildcard; /etc/hosts has none, so it would resolve nothing." $alias.ip) -}}
{{- end -}}
{{- end -}}
{{- if $c.requireTrustedProxy -}}
{{- range $cidr := $c.trustedProxyCidrs -}}
{{- $prefix := int (last (splitList "/" (toString $cidr))) -}}
{{- $floor := ternary 48 16 (contains ":" (toString $cidr)) -}}
{{- if lt $prefix $floor -}}
{{- fail (printf "api.console.trustedProxyCidrs names %q, wider than /%d. With requireTrustedProxy a range this wide trusts the pods the gate exists to refuse: on most clusters every pod is inside 10.0.0.0/8 and can dial the console Service directly and send the proxy's header itself. Name the ingress controller's own pod range (the binary refuses the same)." (toString $cidr) $floor) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $granted := (include "logweir.api.namespaces" . | fromJsonArray) -}}
{{- range $b := $c.roles.bindings -}}
{{- if not (has $b.namespace $granted) -}}
{{- fail (printf "api.console.roles.bindings names namespace %q, which is not one the console is bound in (%s). The RoleBindings come from api.namespaces and kubernetes.namespace; a product binding for a namespace the ServiceAccount cannot read is a role that grants a 403." $b.namespace (join ", " $granted)) -}}
{{- end -}}
{{- if or (contains "*" (toString (concat ($b.groups | default list) ($b.subjects | default list)))) (contains "?" (toString (concat ($b.groups | default list) ($b.subjects | default list)))) -}}
{{- fail (printf "api.console.roles.bindings for role %q in namespace %q contains `*` or `?`. Bindings are EXACT strings: there is no regex, no wildcard and no email-domain inference, and a `*` is refused by name rather than silently matching nothing." $b.role $b.namespace) -}}
{{- end -}}
{{- if not (has $b.namespace $watch) -}}
{{- fail (printf "api.console.roles.bindings names namespace %q, which is not in controller.watchNamespaces (%s). The console would create Backups, Restores and checks there that no controller reconciles. Add it to controller.watchNamespaces, or remove the binding." $b.namespace (join ", " $watch)) -}}
{{- end -}}
{{- end -}}
{{- if $c.ingress.enabled -}}
{{- if not $c.ingress.host -}}
{{- fail "api.console.ingress.host is empty. The Ingress rule needs the exact host the browser uses, and it must be publicBaseUrl's own authority." -}}
{{- end -}}
{{- if not $c.ingress.tlsSecretName -}}
{{- fail "api.console.ingress.tlsSecretName is empty. An Ingress for the shared console without a TLS block is a console published over plain HTTP: the session cookie is `__Host-`/`Secure` and would never be sent, and the OIDC redirect URI is https. TLS at the shared entry point is required, not recommended (D0)." -}}
{{- end -}}
{{- if ne $c.ingress.host (trimPrefix "https://" $c.publicBaseUrl) -}}
{{- fail (printf "api.console.ingress.host is %q but api.console.publicBaseUrl is %q. They must name the SAME authority. The OIDC redirect URI is publicBaseUrl plus /auth/callback, and the service answers 421 misdirected_request to any Host it does not serve — so a mismatched pair renders, lints and installs clean, and then refuses every browser that arrives through the Ingress it just published. Every other way to get this wrong is refused here by name; so is this one." $c.ingress.host $c.publicBaseUrl) -}}
{{- end -}}
{{- /*
A chart that renders the Ingress knows there is a proxy, so it must be named
(or opted out of with `trustedProxy: none`): without it the console cannot tell
the ingress from any pod that dials it, its per-peer connection cap is off, and
every sign-in through the ingress shares one rate-limit budget.
*/ -}}
{{- if and (not $c.trustedProxyCidrs) (not $proxySvc.name) (ne (toString $c.trustedProxy) "none") -}}
{{- fail "api.console.ingress.enabled in shared mode needs the trusted proxy named: set api.console.trustedProxyService to the ingress controller's Service (namespace and name; its serving pods are then trusted), or api.console.trustedProxyCidrs to its pods' range. Without it the console cannot tell the ingress from any pod that dials it directly, so one in-cluster address may hold every one of its 256 connections (the per-peer cap, FX-24c, is off) and every sign-in through the ingress shares one rate-limit budget (FX-13). To run without a named proxy deliberately — a service mesh that re-originates every connection, say — set api.console.trustedProxy=none." -}}
{{- end -}}
{{- end -}}
{{- else -}}
{{- fail (printf "api.console.mode is %q. The two modes are `localAdmin` (a loopback listener reached with kubectl port-forward, one configured administrator, no identity provider) and `shared` (SSO: OIDC, an encrypted session cookie, exact role bindings and TLS). There is no default: a file that forgets to say which it wants is refused rather than read as the more permissive one." $c.mode) -}}
{{- end -}}
{{- /*
A namespace the console serves that is bound to an approval policy needs the
console's ConsoleConfirmation key (both modes sign with it); `logweir-api`
refuses to start without it.
*/ -}}
{{- $served := (include "logweir.api.namespaces" . | fromJsonArray) -}}
{{- $consoleKeySecret := include "logweir.consoleKey.secret" . -}}
{{- range $ns, $policy := (.Values.approvalPolicy.namespaces | default dict) -}}
{{- if and (has $ns $served) (not $consoleKeySecret) -}}
{{- fail (printf "approvalPolicy.namespaces binds %s, which the console serves, to policy %q and there is no console confirmation key: approvalPolicy.confirmationKeySecret is empty and the identity hook does not manage one (identity.enabled with identity.bootstrapFeatures.consoleKey). Both ordinary and governed requests carry the console's ConsoleConfirmation signature (D0): create a Secret with key `confirmation.key` (a PKCS#8 PEM private key whose PUBLIC half is on the namespace's TrustPolicy with usage ConsoleConfirmation) and name it, or use a chart whose bootstrap image generates the key, or unbind the namespace." $ns $policy) -}}
{{- end -}}
{{- end -}}
{{- if and (eq (.Values.approvalPolicy.default | default "") "confirm") (not $consoleKeySecret) -}}
{{- fail "approvalPolicy.default is confirm and there is no console confirmation key: approvalPolicy.confirmationKeySecret is empty and the identity hook does not manage one (identity.enabled with identity.bootstrapFeatures.consoleKey). Every unbound namespace would be created and then never authorisable; name the key's Secret, or use a chart whose bootstrap image generates the key." -}}
{{- end -}}
{{- range $peer := ($c.networkPolicy.oidcPeers | default list) -}}
{{- if or (not $peer.namespace) (not $peer.podLabels) (lt (int $peer.port) 1) -}}
{{- fail "api.console.networkPolicy.oidcPeers entries need a namespace, non-empty podLabels and a port: an empty selector would admit egress to every pod in the namespace, which is not what `the identity provider's path` means." -}}
{{- end -}}
{{- end -}}
{{- if and $c.networkPolicy.enabled (eq $c.mode "shared") -}}
{{- if or (not $c.networkPolicy.ingressNamespace) (not $c.networkPolicy.ingressPodLabels) -}}
{{- fail "api.console.networkPolicy.enabled in shared mode needs both networkPolicy.ingressNamespace and networkPolicy.ingressPodLabels. An ingress rule with no `from` selector admits nothing at all, which would be a console nobody can reach; one with an empty podSelector admits every pod in the controller's namespace, which is not what `only the configured ingress controller` means. (In the in-cluster administrator mode there is no ingress rule to write: nothing may dial this pod, so the policy denies ingress outright and neither selector is read.)" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- /* Connection objects land where a scoped controller reads them. */ -}}
{{- if .Values.minio.enabled -}}
{{- include "logweir.validate.connectionsNamespace" . -}}
{{- end -}}
{{- /* kafka.enabled: the operator's own clusters, source then target. */ -}}
{{- if .Values.kafka.enabled -}}
{{- if .Values.demoKafka.enabled }}
{{- fail "kafka.enabled and demoKafka.enabled are both true. demoKafka brings its own two throwaway brokers and their own KafkaCluster objects (scripts/helm-demo.sh creates them); kafka: points Logweir at a cluster you already run. Turn one off — demoKafka.enabled: false to use your own cluster, kafka.enabled: false to walk the demo." }}
{{- end }}
{{- include "logweir.validate.connectionsNamespace" . -}}
{{- include "logweir.validate.kafkaCluster" .Values.kafka -}}
{{- with .Values.kafka.target -}}
{{- if .bootstrapServers -}}
{{- include "logweir.validate.kafkaCluster" . -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if .Values.demoKafka.enabled }}
{{- if eq .Values.demoKafka.clusterIds.source .Values.demoKafka.clusterIds.target }}
{{- fail "demoKafka.clusterIds.source and .target must differ: two brokers with one KRaft cluster id are one cluster to phase 0's target != source rail" }}
{{- end }}
{{- end }}
{{- /*
The installation policy. `Policy::validate` requires maxActiveTotal >=
maxActivePerNamespace, a rule between two siblings JSON Schema draft-07 cannot
state; a document the controller refuses fails closed and silently.
*/ -}}
{{- $maxNs := int .Values.checks.maxActivePerNamespace -}}
{{- $maxTotal := int .Values.checks.maxActiveTotal -}}
{{- if lt $maxTotal $maxNs -}}
{{- fail (printf "checks.maxActiveTotal (%d) must be at least checks.maxActivePerNamespace (%d). weirkeeper::check::policy::Policy::validate refuses the rendered document otherwise, and a refused policy fails CLOSED: every visibilityAttestation and every controllerIdentityLocation is discarded and the install still looks healthy." $maxTotal $maxNs) -}}
{{- end -}}
{{- /* The installation signing identity and its hook. */ -}}
{{- if .Values.identity.enabled }}
{{- $_ := required "identity.publicConfigMapName must be set when identity.enabled=true" .Values.identity.publicConfigMapName }}
{{- $bootstrapImage := required "identity.bootstrapImage must name a reviewed runner digest containing `identity bootstrap` (the chart default pins one); use a digest, or the explicit local-development override" .Values.identity.bootstrapImage }}
{{- if not (regexMatch "^.+@sha256:[0-9a-f]{64}$" $bootstrapImage) }}
{{- if not .Values.identity.allowMutableBootstrapImageForDevelopment }}
{{- fail "identity.bootstrapImage must be an immutable @sha256 digest; mutable/local references require identity.allowMutableBootstrapImageForDevelopment=true and identity.bootstrapImagePullPolicy=Never" }}
{{- end }}
{{- if ne .Values.identity.bootstrapImagePullPolicy "Never" }}
{{- fail "a mutable identity.bootstrapImage is development-only and requires both identity.allowMutableBootstrapImageForDevelopment=true and identity.bootstrapImagePullPolicy=Never" }}
{{- end }}
{{- end }}
{{- $externalName := .Values.identity.externalSecret.name | default "" }}
{{- $externalKey := .Values.identity.externalSecret.key | default "" }}
{{- if and $externalName (not $externalKey) }}{{ fail "identity.externalSecret.key must be set when identity.externalSecret.name is set" }}{{ end }}
{{- if and (not $externalName) (ne $externalKey "signing.pem") }}{{ fail "identity.externalSecret.name must be set when identity.externalSecret.key is customized" }}{{ end }}
{{- /*
A render that cannot ask the cluster (`lookup` finds no `default/kubernetes`
Service: helm template, helm lint, --dry-run=client, Argo CD) looks like a
fresh install every time and could replace the filled signing Secret with an
empty placeholder, unless `identity.renderedOffline` says what it is.
*/ -}}
{{- if and (kindIs "invalid" .Values.identity.renderedOffline) (not (lookup "v1" "Service" "default" "kubernetes")) }}
{{- fail "identity.renderedOffline is unset and this render cannot ask the cluster (Helm's lookup finds no default/kubernetes Service: helm template, helm lint, --dry-run=client, Argo CD), so it cannot tell a fresh install from an established one; applied, it could replace the filled signing Secret with an empty placeholder. Install with helm install or helm upgrade (preview with --dry-run=server). For Argo CD or `helm template | kubectl apply` set identity.renderedOffline=true and create the retained objects first (docs/install.md, \"Argo CD and other offline renders\"). identity.renderedOffline=false renders what helm install would, only for a preview that is never applied." }}
{{- end }}
{{- /*
The fresh install's TrustPolicy grant is an install-only hook. Helm before
3.19 keeps it bound when a later hook fails (`logweir.helmCleansFailedHooks`),
a standing cluster-wide `create trustpolicies`, so it is refused there.
*/ -}}
{{- if and (include "logweir.identity.freshTrust" .) (not (include "logweir.helmCleansFailedHooks" .Capabilities.HelmVersion.Version)) }}
{{- fail (printf "identity.installationTrust needs Helm 3.19.0 or newer (this is %s): the fresh install's TrustPolicy grant is an install-only hook, and Helm before 3.19 keeps it bound when the identity hook fails, leaving a standing cluster-wide `create trustpolicies`. Install with Helm 3.19+ or 4.x, or set identity.installationTrust.enabled=false (the install then starts strict, and you trust the console key yourself; docs/install.md §5f)." .Capabilities.HelmVersion.Version) }}
{{- end }}
{{- range $namespace := .Values.identity.authorizedRunnerNamespaces }}
{{- if eq $namespace $.Release.Namespace }}{{ fail "identity.authorizedRunnerNamespaces must not repeat the release namespace; it is always authorized" }}{{ end }}
{{- end }}
{{- $singleton := lookup "rbac.authorization.k8s.io/v1" "ClusterRole" "" "logweir-identity-singleton" }}
{{- if $singleton }}
{{- $owner := $singleton.metadata.annotations | default dict }}
{{- $ownerName := index $owner "logweir.dev/release-name" | default "<unknown>" }}
{{- $ownerNamespace := index $owner "logweir.dev/release-namespace" | default "<unknown>" }}
{{- if or (ne $ownerName .Release.Name) (ne $ownerNamespace .Release.Namespace) }}
{{- fail (printf "cluster installation identity is already claimed by Helm release %s/%s; Logweir supports one installation identity per cluster until PLAT-19.1" $ownerNamespace $ownerName) }}
{{- end }}
{{- end }}
{{- end }}
{{- /* Withdrawn: notify.* (release notes, item 74). */ -}}
{{- if .Values.notify }}
{{- fail "notify.* is withdrawn (release notes, item 74): Logweir no longer delivers ProtectionPolicy alerts, so no key under notify configures anything. Remove the notify block from your values, or on helm upgrade --reuse-values pass --set notify=null" }}
{{- end }}
{{- range $ns := (.Values.controller.watchNamespaces | default list) }}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$" $ns) }}
{{- fail (printf "controller.watchNamespaces names %q, which is not a namespace name (a DNS label). The controller refuses the same list at startup rather than leave a namespace silently unreconciled." $ns) }}
{{- end }}
{{- end }}
{{- /* The approval policy: the binary's refusals, made earlier. */ -}}
{{- if include "logweir.approvalPolicy.configured" . -}}
{{- $p := .Values.approvalPolicy -}}
{{- $names := list -}}
{{- if $p.default -}}
{{- if eq $p.default "two-person" -}}
{{- fail "approvalPolicy.default is two-person, which is not a default: two-person approval in the console is set per namespace. Declare a policy with mode: two-person in approvalPolicy.policies and bind each namespace to it in approvalPolicy.namespaces. approvalPolicy.default is confirm, strict, or empty (a fresh install with a console then starts in confirm, and an upgraded one keeps legacy-governed-v1)." -}}
{{- end -}}
{{- if not (has $p.default (list "confirm" "strict")) -}}
{{- fail (printf "approvalPolicy.default is %q; it is `confirm`, `strict`, or empty (the fresh-install marker decides)." $p.default) -}}
{{- end -}}
{{- if and (eq $p.default "confirm") (not $p.allowOrdinaryConfirmation) -}}
{{- fail "approvalPolicy.default is confirm (one-person confirmation for every unbound namespace) but approvalPolicy.allowOrdinaryConfirmation is not true. Ordinary confirmation is an explicit installation decision (D0) and is never enabled by one field alone: set the floor too. An older install opting in also needs the console key on the TrustPolicy that governs those namespaces (docs/install.md §5f)." -}}
{{- end -}}
{{- end -}}
{{- range $i, $policy := ($p.policies | default list) -}}
{{- if not (and $policy.name (regexMatch "^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$" $policy.name)) -}}
{{- fail (printf "approvalPolicy.policies[%d].name %q must be a DNS label." $i ($policy.name | default "")) -}}
{{- end -}}
{{- if has $policy.name (list "legacy-governed-v1" "default-confirm-v1") -}}
{{- fail (printf "approvalPolicy.policies names %q, which is reserved for the policy an unbound namespace resolves to. Leave the namespace unbound instead." $policy.name) -}}
{{- end -}}
{{- if has $policy.name $names -}}
{{- fail (printf "approvalPolicy.policies declares %q twice." $policy.name) -}}
{{- end -}}
{{- $names = append $names $policy.name -}}
{{- if not (has $policy.mode (list "Governed" "Ordinary" "confirm" "strict" "two-person")) -}}
{{- fail (printf "approvalPolicy.policies[%d].mode is %q; it is `confirm` (Ordinary), `two-person` (Governed, approved by a second person in the console) or `strict` (Governed, approved with a personal key)." $i ($policy.mode | default "")) -}}
{{- end -}}
{{- $mode := include "logweir.approvalPolicy.internalMode" $policy.mode -}}
{{- /* Who signs the approval: the binary's three refusals. */ -}}
{{- if hasKey $policy "approverSignature" -}}
{{- if ne (toString $policy.approverSignature) "Console" -}}
{{- fail (printf "approvalPolicy.policies[%d].approverSignature is %q; it is `Console` (a second person approves in the console) or absent (a personal GovernedApproval key)." $i (toString $policy.approverSignature)) -}}
{{- end -}}
{{- if eq $mode "Ordinary" -}}
{{- fail (printf "approvalPolicy.policies[%d] (%s) sets approverSignature: Console on a confirm (Ordinary) policy, which has no approver; two-person approval in the console is mode: two-person." $i $policy.name) -}}
{{- end -}}
{{- if eq $policy.mode "strict" -}}
{{- fail (printf "approvalPolicy.policies[%d] (%s) sets approverSignature: Console and mode: strict, which is a personal-key approval; say mode: two-person for approval in the console, or drop approverSignature for a personal key." $i $policy.name) -}}
{{- end -}}
{{- end -}}
{{- if and (eq $mode "Ordinary") (not $p.allowOrdinaryConfirmation) -}}
{{- fail (printf "approvalPolicy.policies[%d] (%s) is Ordinary but approvalPolicy.allowOrdinaryConfirmation is not true. Ordinary confirmation is an explicit installation decision (D0): set the floor, or use a Governed policy." $i $policy.name) -}}
{{- end -}}
{{- if and (eq $mode "Governed") (hasKey $policy "requireDistinctPrincipal") (not $policy.requireDistinctPrincipal) -}}
{{- fail (printf "approvalPolicy.policies[%d].requireDistinctPrincipal is false on a Governed policy; the supported baseline requires the approver's principal to differ from the requester's." $i) -}}
{{- end -}}
{{- if and (eq $mode "Ordinary") $policy.requireDistinctPrincipal -}}
{{- fail (printf "approvalPolicy.policies[%d].requireDistinctPrincipal is true on an Ordinary policy, which has no approver to compare; use a Governed policy." $i) -}}
{{- end -}}
{{- if hasKey $policy "maxAgeSeconds" -}}
{{- if or (lt (int64 $policy.maxAgeSeconds) 60) (gt (int64 $policy.maxAgeSeconds) 604800) -}}
{{- fail (printf "approvalPolicy.policies[%d].maxAgeSeconds is %v; it must be from 60 to 604800." $i $policy.maxAgeSeconds) -}}
{{- end -}}
{{- end -}}
{{- range $key, $_ := $policy -}}
{{- if not (has $key (list "name" "mode" "maxAgeSeconds" "requireDistinctPrincipal" "approverSignature")) -}}
{{- fail (printf "approvalPolicy.policies[%d] carries unknown field %q; a policy has name, mode, maxAgeSeconds, requireDistinctPrincipal and approverSignature." $i $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if gt (len ($p.policies | default list)) 64 -}}
{{- fail "approvalPolicy.policies declares more than 64 policies." -}}
{{- end -}}
{{- if gt (len ($p.namespaces | default dict)) 256 -}}
{{- fail "approvalPolicy.namespaces declares more than 256 bindings." -}}
{{- end -}}
{{- range $ns, $policy := ($p.namespaces | default dict) -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$" $ns) -}}
{{- fail (printf "approvalPolicy.namespaces.%s is not a namespace name (a DNS label)." $ns) -}}
{{- end -}}
{{- if not (has $policy $names) -}}
{{- fail (printf "approvalPolicy.namespaces.%s names policy %q, which approvalPolicy.policies does not declare." $ns $policy) -}}
{{- end -}}
{{- end -}}
{{- /*
Two-person approval needs the shared console. Last, so a malformed policy is
named as that first; this refusal is the chart's alone (the document is well
formed, there are just not two identities to tell apart).
*/ -}}
{{- range $i, $policy := ($p.policies | default list) -}}
{{- if include "logweir.approvalPolicy.console" $policy -}}
{{- if not (and $.Values.api.enabled $.Values.api.console.enabled (eq ($.Values.api.console.mode | default "") "shared")) -}}
{{- fail (printf "approvalPolicy.policies[%d] (%s) is two-person: a second person signs in to the console and clicks Approve, which needs api.console.enabled=true and api.console.mode=shared (this render has %s). The in-cluster administrator console has one identity, which cannot be two people, and without a console nobody can approve. Run the shared console, or use mode: strict (a personal key) or mode: confirm (one person) for this policy." $i $policy.name (ternary (printf "api.console.mode=%s" ($.Values.api.console.mode | default "\"\"")) "no console" (and $.Values.api.enabled $.Values.api.console.enabled))) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- /* The admission policy's subject, its whole effect. */ -}}
{{- if .Values.admissionPolicy.enabled }}
{{- $name := required "admissionPolicy.consoleServiceAccountName must be set (a non-empty ServiceAccount name in the release namespace) whenever admissionPolicy.enabled is true: the policy's only effect is its matchConditions subject list, and an empty or null name renders a principal that matches no request at all — the fence would install, look enabled, and protect nothing." .Values.admissionPolicy.consoleServiceAccountName -}}
{{- $rendered := include "logweir.api.serviceAccountName" . -}}
{{- if and .Values.api.enabled (ne $name $rendered) -}}
{{- fail (printf "admissionPolicy.consoleServiceAccountName is %q, but api.enabled renders the console ServiceAccount %q in namespace %q. The policy's ONLY effect is its matchConditions subject list, so a name that is not the account this chart creates fences NOBODY: the install succeeds, the policy reads as enabled, and the console's `create` on Secrets — which RBAC cannot narrow by shape — has no bound at all. Set admissionPolicy.consoleServiceAccountName to %q, or add it to admissionPolicy.extraPrincipals if you meant to fence a different console as well." $name $rendered .Release.Namespace $rendered) -}}
{{- end -}}
{{- end }}
{{- end -}}

{{- /*
`kafka.enabled` and `minio.enabled` render their connection objects into
`kubernetes.connectionsNamespace`; a scoped controller must watch it, or no
Backup could ever use them.
*/ -}}
{{- define "logweir.validate.connectionsNamespace" -}}
{{- $ns := .Values.kubernetes.connectionsNamespace | default .Release.Namespace -}}
{{- $watch := .Values.controller.watchNamespaces | default list -}}
{{- if and $watch (not (has $ns $watch)) -}}
{{- fail (printf "kafka.enabled / minio.enabled render their connection objects (KafkaClusters, the logweir-s3 archive credential) into namespace %q, which controller.watchNamespaces (%s) does not include: the controller would never read them and no Backup could use them. Set kubernetes.connectionsNamespace to a watched namespace (the one Backups run in)." $ns (join ", " $watch)) -}}
{{- end -}}
{{- end -}}

{{- /*
One `kafka:` cluster block. The CRD's auth.mode enum is plaintext|scramSha512
and the client speaks SCRAM-SHA-512 only, so every other protocol/mechanism
pair is refused rather than rendered into an object that cannot authenticate.
*/ -}}
{{- define "logweir.validate.kafkaCluster" -}}
{{- $c := . -}}
{{- if not (include "logweir.kafka.bootstrapServers" (dict "servers" $c.bootstrapServers) | fromYamlArray) }}{{ fail "kafka: bootstrapServers is empty — give a comma-separated string or a list of host:port" }}{{ end -}}
{{- $protocol := $c.security.protocol | default "PLAINTEXT" -}}
{{- $mechanism := $c.security.mechanism | default "" -}}
{{- if not (or (eq $protocol "PLAINTEXT") (and (eq $mechanism "SCRAM-SHA-512") (or (eq $protocol "SASL_SSL") (eq $protocol "SASL_PLAINTEXT")))) -}}
{{- fail (printf "kafka: security.protocol %q with security.mechanism %q is not supported. Logweir speaks PLAINTEXT, or SCRAM-SHA-512 over SASL_SSL or SASL_PLAINTEXT, and nothing else — the KafkaCluster CRD's auth.mode enum is plaintext|scramSha512. A silently rendered PLAIN or SCRAM-SHA-256 would be a chart that installs and cannot authenticate." $protocol $mechanism) -}}
{{- end -}}
{{- if ne $protocol "PLAINTEXT" -}}
{{- if not $c.username }}{{ fail "kafka: security.mechanism SCRAM-SHA-512 needs a username" }}{{ end -}}
{{- if not $c.secretRef }}{{ fail "kafka: security.mechanism SCRAM-SHA-512 needs a secretRef — the name of a Secret in the release namespace holding the password" }}{{ end -}}
{{- end -}}
{{- if and $c.secretKey (ne $c.secretKey "password") -}}
{{- fail (printf "kafka: secretKey is %q. The operator projects exactly one key name into the probe — `password` (weirkeeper::controllers::restore::TARGET_PASSWORD_SECRET_KEY) — so any other value renders a KafkaCluster whose probe cannot read its credential." $c.secretKey) -}}
{{- end -}}
{{- end -}}

{{- /*
Whether this Helm deletes an earlier `hook-succeeded` hook when a later hook of
the same event fails: 3.19.0 and 4.x do (`pkg/action/hooks.go`, `execHook`),
3.12-3.18 delete only the failed hook. Called with
`.Capabilities.HelmVersion.Version`; `scripts/check-chart.sh` calls it with
fixed versions as its self-test.
*/ -}}
{{- define "logweir.helmCleansFailedHooks" -}}
{{- if semverCompare ">=3.19.0-0" . -}}true{{- end -}}
{{- end -}}
