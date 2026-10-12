# The Logweir Helm chart

**Before you install:** Kubernetes **1.29** or newer (`kubeVersion` in
[`Chart.yaml`](Chart.yaml); Helm refuses an older cluster), Helm **3.19** or
newer, or 4.x (a first install with the console needs 3.19 for its trust
grant), and nodes on **linux/amd64 or linux/arm64**: the controller, runner
and console images, the identity hook's included, are published for both. Brokers
([support matrix](../../docs/support-matrix.md#brokers)): Apache Kafka 3.7.1,
3.9.2, 4.1.2 and 4.3.1 (KRaft) and Confluent Platform 8.3.2 are supported;
Redpanda is a backup source only; anything else is untested.

## Copy this directory and install it

`charts/logweir/` is self-contained. Copy the whole directory into your own
repository, write a short values file, and install:

```bash
helm upgrade --install logweir . -n <namespace> --create-namespace -f my-values.yaml
```

The chart has no subchart and no dependency lock; its CRDs are under `crds/`.
It is the only install: the static `logweir.yaml` bundle is withdrawn in v1
(release-notes item 76). [`docs/install.md`](../../docs/install.md) is the
install guide; this file is the values reference, and
[`values.yaml`](values.yaml) lists every key with its default. You supply:

1. **compatible published images**: `controllerImage`, `runnerImage` and the
   digest-pinned `identity.bootstrapImage` (*Images*). Override all three
   together only for your own registry (install.md, *(d) Bring your own
   registry*); the engine rollback is install.md, *Rolling the engine back*;
2. **an archive**: `archive.url` and, for anything S3-compatible,
   `archive.s3.endpoint` and `archive.s3.region`, or `minio.enabled: true`;
3. **the `kafka:` block**: the cluster to back up, and optionally the scratch
   cluster to restore into.

`examples/` holds a values file per shape: `minimal` (the shipped images and
the three archive values), [`msk`](examples/msk.values.yaml) (Amazon MSK,
SASL/SCRAM, a tainted node pool, a private registry), `demo`, the `console*`
and `approval-policy` shapes, `identity-*`, `admission-policy` and `tuned`.
`author-only.values.yaml` is for images you built and loaded yourself: **an
author-only install is not evidence of publication.**

## What it installs

| object | when |
|---|---|
| the thirteen `CustomResourceDefinition`s under `logweir.dev/v1alpha1` | always, from `crds/`, once (*Upgrading the CRDs by hand*) |
| `ServiceAccount`, `ClusterRole`, `ClusterRoleBinding` `weirkeeper` | always; RoleBindings instead with `controller.watchNamespaces` |
| `Deployment` `weirkeeper` | always; probed by `weirkeeper --probe live` / `--probe ready` against `127.0.0.1:8081` (`LOGWEIR_HEALTH_ADDR`), restarted after three misses 20 s apart |
| `ConfigMap` `weirkeeper-policy` | always: the installation policy |
| `ClusterRole`s `logweir-viewer`, `logweir-operator`, `logweir-approver`, `logweir-trust-admin`, `logweir-retention-admin` | always, **unbound**: you bind the human roles |
| retained `Secret` `logweir-signing-key` and `ConfigMap` `logweir-signing-trust`, `ClusterRole` `logweir-identity-singleton`, scoped Roles, hook Jobs | `identity.enabled` |
| `ServiceAccount` `logweir-runner`, `NetworkPolicy`s `logweir-runner-egress` and `logweir-identity-kubernetes-api-egress` | the release namespace and each `identity.authorizedRunnerNamespaces` entry |
| `ValidatingAdmissionPolicy` and binding `logweir-console-credentials-only` | `admissionPolicy.enabled` |
| MinIO with a PVC, `Secret`s `<release>-minio-root` and `logweir-s3`, a seed `Job` | `minio.enabled` |
| two KRaft brokers, `<release>-kafka-source` and `-target`, a seed `Job` | `demoKafka.enabled` |
| `KafkaCluster`s `source` (and `target`) | `kafka.enabled` |
| `ServiceAccount` `logweir-retention`, with no Role or binding | `retention.enabled` |
| `ServiceAccount`, ClusterRoles and bindings `<release>-api` | `api.enabled` (RBAC only) |
| `ConfigMap` `<release>-api-config-<digest>`, `Deployment` `<release>-api` | `api.console.enabled` |
| `Service` `<release>-api`, plain HTTP/1.1 (an `h2c` or gRPC backend is refused) | `api.console.mode: shared` |
| `Ingress`, `NetworkPolicy`, `PodDisruptionBudget` `<release>-api` | `api.console.ingress.enabled`, `.networkPolicy.enabled`, `.replicas` above 1 |
| `Role` and `RoleBinding` `<release>-api-trusted-proxy` (`list` on `endpointslices` in the ingress namespace) | `api.console.trustedProxyService` |
| immutable `ConfigMap` `<release>-approval-policy-<digest>` | any `approvalPolicy.*` value |

Nothing optional is on by default. `manifest_lint.rs` holds the controller's
role to its calls: no verb on `secrets`, no `update`, and `delete` only on
`topicdiscoveries` and `preflights`. Every status write is a merge `PATCH`
carrying a `metadata.resourceVersion` precondition.

## Values

### Images, placement and identity

| key | default | meaning |
|---|---|---|
| `controllerImage` | `docker.io/vladyslavhaina/weirkeeper:latest` | the controller |
| `runnerImage` | `docker.io/vladyslavhaina/logweir:latest` | every Job, retention included (there is no `retentionImage`) |
| `imagePullPolicy` | `Always` | the controller: `Always`, `IfNotPresent` or `Never` |
| `runnerImagePullPolicy` | `Always` | runner Jobs (`LOGWEIR_RUNNER_PULL_POLICY`; unset, the controller uses `Never`); another value stops the controller |
| `imagePullSecrets` | `[]` | `[{name: regcred}]` on both ServiceAccounts and every pod |
| `environment` | `""` | the label `logweir.dev/environment` on every object but the CRDs; switches nothing |
| `kubernetes.namespace` | `""` | the name `api.namespaces` defaults to; `-n` decides the install |
| `kubernetes.connectionsNamespace` | `""` (release) | where `kafka.enabled`'s objects and MinIO's `logweir-s3` go; must be watched |
| `kubernetes.nodeSelector`, `.tolerations`, `.affinity` | empty | every chart pod; a component's own block replaces them |
| `identity.enabled` | `true` | the managed signer; `false` only for an external lifecycle |
| `identity.bootstrapImage` | a reviewed runner digest (rev `d4e32956`, linux/amd64 and linux/arm64) | must be `@sha256`; an emptied value refuses to render |
| `identity.bootstrapImagePullPolicy` | `IfNotPresent` | |
| `identity.allowMutableBootstrapImageForDevelopment` | `false` | only the author-only override, with `Never` |
| `identity.renderedOffline` | `null` | `true` for Argo CD and `helm template \| kubectl apply`: no placeholders, no trust grant (*The identity lifecycle*); `false` only for a preview; unset, such a render is refused |
| `identity.bootstrapFeatures.consoleKey` | `true` | `false` only with a bootstrap image older than the console-key flags |
| `identity.publicConfigMapName` | `logweir-signing-trust` | public key, key id, algorithm, trust reference |
| `identity.authorizedRunnerNamespaces` | `[]` | existing namespaces given the same signer and runner prerequisites |
| `identity.kubernetesApiCIDRs` | `[]` | exact API `/32` or `/128` endpoints, TCP 443 or 6443 |
| `identity.externalSecret.name`, `.key` | `""`, `signing.pem` | adopt a P-256 or Ed25519 PKCS#8 Secret; never marked fresh |
| `identity.installationTrust.enabled`, `.policyName` | `true`, `logweir-installation` | the fresh install's `TrustPolicy` (*`approvalPolicy`*) |
| `identity.installationTrust.allowedTargetClusterIds` | `[]` | written into it once; `demoKafka.enabled` adds its target, each id listed once |
| `identity.resources` | 10m / 32Mi, limit 128Mi | |

### Controller and the installation policy

| key | default | meaning |
|---|---|---|
| `controller.logLevel` | `info` | `RUST_LOG`; below `info` the per-object lines are lost |
| `controller.failFastSeconds` | `""` (300 s) | patience for a stuck run; `0` is never (for a Secret materialised late); under 60 is clamped up; unparseable is the default |
| `controller.jobTtlSeconds` | `""` (604800 s) | how long a finished Job stays; under 3600 is clamped up |
| `controller.watchNamespaces` | `[]` (all) | the execution namespaces (below) |
| `controller.resources` | 50m / 128Mi, limit 512Mi | also `nodeSelector`, `tolerations`, `affinity` |
| `checks.maxActivePerNamespace`, `.maxActiveTotal` | `4`, `20` | check Jobs at once; the total must be at least the per-namespace value |
| `checks.maxActiveDiscoveriesPerConnection` | `1` | |
| `checks.maxEvidenceFetchActivePerNamespace` | `4` | its own pool, so verification is never starved |
| `checks.discovery.freshSeconds` | `900` | after this an inventory reads stale |
| `checks.discovery.retentionSeconds`, `.keepPerConnection` | `86400`, `5` | when, and beyond how many, terminal discoveries are collected |
| `checks.discovery.hardMaxTopics` | `50000` | every request's `maxTopics` (1 to 50 000) is clamped to it |
| `checks.discovery.visibilityAttestations` | `[]` | the only route to `attestedComplete` |
| `checks.preflight.retentionSeconds` | `3600` | a terminal `Preflight` is collected after this |
| `runs.maxManualBackupsActivePerNamespace`, `runs.maxManualRestoresActivePerNamespace` | `4`, `2` | manual runs at once per namespace; the rest wait `Queued` with nothing created |
| `engine.allowUnverifiedCustomCa` | `false` | a destination CA the engine cannot verify |
| `evidence.controllerIdentityLocations` | `[]` | `[{endpoint, region, bucket}]` the controller may read evidence from; others are `ControllerIdentityNotAllowlisted` |

`weirkeeper-policy` is the one place an operator cannot reach. **The chart
refuses what the controller would refuse** (the schema pins every bound;
`templates/policy.yaml` refuses a total below the per-namespace ceiling),
because a refused document fails closed almost silently: attestations and
evidence locations are dropped and the ceilings revert, with one advisory
`Preflight` row and one `WARN` line. The `runs` block renders only when a value
differs from its default; an install that set it rolls the controller back with
the chart. `legacyArchiveAddressing` is rendered from `archive.s3.*`, and empty
without an endpoint; its `allowHttp` and `virtualHostedStyle` are published
for older readers and read by nothing (FX-15). An attestation has nine required fields: `id`, `namespace`,
`kafkaCluster`, `clusterId`, `principal`, `attestedBy`, `attestedAt`,
`expiresAt`, `statement`. It applies on an exact match of the four identifiers,
before `expiresAt`, to an untruncated listing, and **Logweir never verifies the
statement**.

### Archive, Kafka and the demo components

| key | default | meaning |
|---|---|---|
| `archive.url` | `""` | `s3://bucket/prefix`; empty with `minio.enabled` uses the chart's MinIO. A point with no saved destination is verified only in this bucket (elsewhere `NotAttempted`) |
| `archive.s3.endpoint`, `.region` | `""` | empty endpoint for Amazon S3 |
| `archive.s3.allowHttp`, `.virtualHostedStyle` | `false` | rendered only with an endpoint; `false` is path-style |
| `kafka.enabled` | `false` | render `KafkaCluster`s; not with `demoKafka.enabled` |
| `kafka.name`, `.bootstrapServers`, `.username` | `source`, `""`, `""` | servers as `"h1:9096,h2:9096"` or a list |
| `kafka.security.protocol`, `.mechanism` | `PLAINTEXT`, `""` | the mapping in *`kafka:`* |
| `kafka.secretRef`, `.secretKey` | `""`, `password` | the only accepted key |
| `kafka.target.*` | `name: target`, same keys | the optional restore target; `markerTopic: logweir.scratch` proves it scratch |
| `minio.enabled` | `false` | a demo archive (`kafka-backups`, `logweir-evidence`); its root user also mints `logweir-s3` |
| `minio.image`, `.mcImage` | mirror digests | *Third-party images* |
| `minio.rootUser`, `.rootPassword` | `minioadmin` | |
| `minio.persistence.enabled`, `.size`, `.storageClassName` | `true`, `5Gi`, `""` | `false` is an emptyDir: the archive dies with the pod |
| `minio.resources` | 50m / 256Mi | also placement |
| `demoKafka.enabled` | `false` | two PLAINTEXT brokers on emptyDir, `orders` and `payments` seeded |
| `demoKafka.image`, `.clusterIds.source`, `.clusterIds.target` | digests, two ids | the ids must differ: equal ids are one cluster to phase 0 |
| `demoKafka.seed.recordsPerTopic`, `.resources` | `200`; 100m / 512Mi, limit 1Gi | also placement |

### Retention, the admission fence and approvals

| key | default | meaning |
|---|---|---|
| `retention.enabled` | `false` | the `logweir-retention` identity (below) |
| `admissionPolicy.enabled` | `false` | fence the console's `create secrets`; Kubernetes 1.30+ |
| `admissionPolicy.consoleServiceAccountName` | `logweir-api` | must be `<release>-api` with `api.enabled`; the render refuses another or empty |
| `admissionPolicy.extraPrincipals` | `[]` | more `system:serviceaccount:<ns>:<name>` subjects |
| `approvalPolicy.default` | `""` | unbound namespaces: `""` (confirm on a fresh install with a console, else `legacy-governed-v1`), `confirm` or `strict` |
| `approvalPolicy.allowOrdinaryConfirmation` | `false` | needed by a `confirm` policy and `default: confirm` |
| `approvalPolicy.policies` | `[]` | `[{name, mode: confirm\|two-person\|strict, maxAgeSeconds, requireDistinctPrincipal}]` |
| `approvalPolicy.namespaces` | `{}` | `{<namespace>: <policy>}`; a binding wins over the default |
| `approvalPolicy.confirmationKeySecret` | `""` | the hook's managed key; or your Secret with key `confirmation.key` |

### The console (`api.*`)

| key | default | meaning |
|---|---|---|
| `api.enabled` | `false` | the console's principal, RBAC only |
| `api.namespaces` | `[]` | where it is bound; defaults to `kubernetes.namespace` |
| `api.console.enabled` | `false` | run the console; needs `api.enabled` |
| `api.console.image`, `.imagePullPolicy` | `…/logweir-console:latest`, `Always` | `Never` for a loaded image |
| `api.console.mode` | `""`, required | `localAdmin` or `shared` |
| `api.console.replicas`, `.port` | `1`, `8484` | above 1 adds a `PodDisruptionBudget` |
| `api.console.localAdminSubject` | `admin` | localAdmin's one actor |
| `api.console.keySecret`, `.keyVersion` | `""` (required), `1` | *The key Secret* |
| `api.console.publicBaseUrl` | `""` | shared: exact `https://` URL, no path, no trailing slash |
| `api.console.sessionMaxAgeSeconds` | `900` | shared: 60 to 900 |
| `api.console.trustedProxyCidrs`, `.trustedProxyService` | `[]`, empty | shared: the ingress pods, or its Service |
| `api.console.requireTrustedProxy` | `false` | shared: `421` for a request not from a trusted proxy over HTTPS |
| `api.console.trustedProxy` | `""` | `none` opts out of naming a proxy behind the chart's Ingress |
| `api.console.rateLimits.manualBackupsPerMinute`, `.manualRestoresPerMinute` | `10`, `5` | runs one person may start per namespace per minute |
| `api.console.hostAliases` | `[]` | `[{ip, hostnames}]` pod `/etc/hosts` entries |
| `api.console.oidc.issuer`, `.clientId` | `""` | exact |
| `api.console.oidc.clientSecret` | `""` | the name of a Secret with key `clientSecret` |
| `api.console.oidc.caBundle`, `.systemRoots` | `{configMap, secret, key: ca.crt}`, `true` | a private issuer CA added to the system roots |
| `api.console.roles.revision`, `.bindings` | `""`, `[]` | `[{role, namespace, groups, subjects}]`, exact strings |
| `api.console.ingress.*` | off | shared only: `className`, `host`, `tlsSecretName` (required), `annotations` (HSTS) |
| `api.console.networkPolicy.*` | off | `ingressNamespace` and `ingressPodLabels` (both required), `oidcCIDRs`, `oidcPeers` |
| `api.console.resources` | 25m / 64Mi, limit 256Mi | also placement |

## Withdrawn values

| value | since | what to do |
|---|---|---|
| `checks.discovery.defaultMaxTopics`, `checks.preflight.defaultTimeoutSeconds` | FX-10 | nothing: they never changed a request. Remove them |
| `notify`, `notify.allowInsecureSinks` | v1, release-notes item 74 | refused at render: remove the block, or `--set notify=null` with `--reuse-values` |
| `ui.*` and the `logweir-ui` image | v1, release-notes item 75 | refused at render: use `api.console.*`; the legacy page is gone |

The two FX-10 keys are accepted and ignored, and `NOTES.txt` prints `WITHDRAWN
VALUES ARE SET AND IGNORED`. The CRDs default `maxTopics` (20 000) and
`timeoutSeconds` (120), and a request sets its own (`maxTopics` 1 to 50 000,
clamped by `hardMaxTopics`; `timeoutSeconds` 30 to 600). The policy document
keeps both keys at fixed values (20 000 or `hardMaxTopics` if lower, and 120)
for an older controller during a rolling upgrade or an image-only rollback. If
you had set either to another value, the digest changes once and retained
`ready` `Preflight`s read `unknown` (`policyChanged`): run them again.
`--reuse-values` can carry old defaults forward; upgrade once with
`--reset-then-reuse-values`.

## `retention.enabled`

It renders `ServiceAccount` `logweir-retention` (no token automount, no Role,
no binding) in the release namespace and each
`identity.authorizedRunnerNamespaces` entry, where enforcement Jobs run; for
another namespace apply `config/samples/retention-serviceaccount.yaml`. **It
deletes nothing.** A deletion needs all four:

1. `mode: Enforce` on a `RetentionPolicy`, which only `logweir-retention-admin`
   may write;
2. this ServiceAccount in that namespace;
3. an object-store credential scoped to the policy's prefix and never
   `logweir/`: the hard boundary;
4. an administrator's `approve-plan` with the current `planSha256`, per run.

[`docs/stability.md`](../../docs/stability.md) states the residual: enforcement
turns the controller's Job-create authority into a deletion capability in the
namespace that holds the credential.

## `controller.watchNamespaces`

`[]` binds the controller cluster-wide, and Job-create authority in a namespace
is every Secret there (residual O1). A list renders one `RoleBinding` per
namespace to the unchanged `weirkeeper` ClusterRole,
`ClusterRole/weirkeeper-cluster-scope` (only `trustrosters` and
`trustpolicies`), `Role/weirkeeper-installation-policy` (`get` on
`weirkeeper-policy`), and `LOGWEIR_WATCH_NAMESPACES`, so every watch and read
covers those namespaces. A non-DNS-label name is refused by the schema, the
template and the controller. `auth can-i` as the `weirkeeper` ServiceAccount
with `team-a` listed:

| question | answer |
|---|---|
| `create jobs`, `list pods`, `get pods/log`, `create configmaps`, `watch backups` in `team-a` | yes |
| `create jobs` or `list jobs` in the release namespace; anything in an unlisted namespace; `watch backups --all-namespaces` | **no** |
| `get configmaps/weirkeeper-policy` in the release namespace | yes; any other ConfigMap there, or `get secrets` anywhere: **no** |
| `list trustpolicies`, `get trustrosters`, `patch trustpolicies` (the compromise-revocation finalizer, `docs/keys.md`) | yes |

**Shared mode requires it**, and the release namespace may not be in it.

**Migration.** Without a shared console nothing changes. A shared release stops
rendering until scoped: `helm upgrade` refuses, naming the field, and the
running console keeps serving. It meets, in order: no list; the release
namespace listed; any `ui.*`; a binding namespace outside the list; with
`requireTrustedProxy`, a range wider than `/16`. List every namespace with
Logweir objects (`kubectl get backups,restores,backupschedules,kafkaclusters
-A`), give each the runner ServiceAccount and the signer
(`identity.authorizedRunnerNamespaces`), move runs out of the release
namespace, set the list and upgrade. Unlisted namespaces are not reconciled
until added. Rollback is `watchNamespaces: []`. Each change restarts the
controller. install.md 5e has the table of fixes.

## `approvalPolicy`

**confirm**: one person clicks Create, no key (internal `Ordinary`).
**two-person**: a second signed-in person approves in the shared console, no
key (internal `Governed` with `approverSignature: Console`). **strict**: an
approver's personal key (`Governed`, or `legacy-governed-v1`). A **fresh install
with a console** (first `helm install` on Helm 3.19+ or 4.x, `api.console.enabled`,
the managed identity, `consoleKey`) starts unbound namespaces in **confirm**:
the hook creates the console key, a default `TrustPolicy` for it and the signer
when the cluster has no trust of its own, and marks the install. An upgraded
install keeps `legacy-governed-v1` until you opt in (install.md 5f). The
default `TrustPolicy` is made only when the cluster has no `TrustPolicy` and no
`TrustRoster/default`; on older Helm the chart refuses, naming
`identity.installationTrust.enabled`. Its `create` grant is a `post-install`
hook Helm deletes when the hooks finish. After a failed first install, run
`kubectl delete clusterrolebinding,clusterrole <release>-identity-trust
--ignore-not-found`.

- `Ordinary`, `Governed` and `approverSignature: Console` are older spellings.
  `maxAgeSeconds` is 60 to 604800 (default 900 for confirm, 86400 otherwise);
  `requireDistinctPrincipal` must be true for two-person and strict, and
  compares the approver key's `principal.id`, which must be
  `<issuer>#<subject>`; `default-confirm-v1` and `legacy-governed-v1` are
  reserved.
- One immutable ConfigMap is mounted into the controller
  (`LOGWEIR_APPROVAL_POLICY_FILE`) and the console, and both refuse a bad
  document at start, so **the chart refuses at render every document the binary
  refuses** (cases in `scripts/approval-policy-refusals/`).
- Editing a policy rolls both Deployments; a confirmed, unadmitted Restore is
  refused `ApprovalPolicyMismatch` and is submitted again.
- **two-person** needs `api.console.mode: shared` and cannot be the default.
  The namespace's `TrustPolicy` needs the console key under
  `ConsoleConfirmation` and no approver key. The mode is in the policy's
  snapshot digest, so a request made under it is never approved with a personal
  key, nor a strict request in the console. Roll the controller, runner and
  console together (an older runner exits 3).
- **Rollback:** rebind or remove the policies before rolling binaries back.
- **The residual** (`SECURITY.md`): whoever controls the console pod, its key
  Secret or the identity provider can approve alone under confirm and
  two-person; use strict where that is not acceptable. `docs/keys.md` lists the
  keys each `TrustPolicy` needs.

## The console

The values block, ServiceAccount, Service and Deployment are `api.*` and
`<release>-api`; the image and its Dockerfile are `logweir-console`.
**`api.enabled` starts nothing**, so you can ask `kubectl auth can-i` first. It
grants `get`/`list` and `create` on the eight product kinds, `patch` on
`backupschedules`, `backupdestinations`, `topicdiscoveries` and `preflights`,
`get`/`list` on `protectionpolicies`, `recoverycatalogs`, `retentionpolicies`
and `trustpolicies`, `create` on `recoverycatalogs`, `get` on
`trustrosters/default` and `configmaps`, and `create` on `secrets`; no `watch`,
no `delete`, no Secret read (so a console-written credential is write-only), no
`list` on `configmaps`, no trust write. `scripts/check-chart.sh` answers each
`can-i` against every render. **`admissionPolicy.enabled`** limits that
`create secrets` to the types `logweir.dev/object-store-credential`,
`logweir.dev/kafka-sasl-password` and `logweir.dev/kafka-client-certificate`
with the `app.kubernetes.io/managed-by: logweir` label, so the console cannot
mint a ServiceAccount token; other principals are skipped, which makes
`failurePolicy: Fail` safe. On Kubernetes 1.29 the kind does not exist.

### localAdmin, the in-cluster administrator mode

It binds `127.0.0.1` and is reached only with `kubectl port-forward
deploy/<release>-api`: no Service, no Ingress (refused), `ingress: []`.

```console
$ head -c 32 /dev/urandom > cursor.key
$ kubectl -n logweir-system create secret generic logweir-console-keys --from-file=cursor.key
$ rm cursor.key
$ helm install logweir charts/logweir -n logweir-system -f charts/logweir/examples/console.values.yaml
$ kubectl -n logweir-system port-forward deploy/logweir-api 8484:8484
$ open http://127.0.0.1:8484/ui/
```

The local port must be `8484`, because `publicOrigin` carries it. **`create
pods/portforward` in the namespace is full console administrator authority**
over every bound namespace. There are no probes (a kubelet cannot reach
loopback), so a refused configuration shows as `CrashLoopBackOff`, exit code 2,
and one line in `kubectl logs` naming the field.

### The key Secret

| key in `api.console.keySecret` | mode | content |
|---|---|---|
| `cursor.key` | both | at least 32 bytes: raw in localAdmin, `version:` and `key:` lines in shared |
| `session.key` | shared | the same two-line form |

**The chart generates no key**: a generated one changes on every render and is
lost on upgrade. `keyVersion` must equal the files' `version:`; bumping both
ends every session, and a file changed without it is refused at start. The
OIDC client secret and the TLS Secret are yours too. The rendered ConfigMap
holds only paths under `/var/run/logweir/`.

### What the chart refuses in shared mode

`examples/console-shared.values.yaml` is the whole shape. Refused at render,
naming the field:

| refused | why |
|---|---|
| `publicBaseUrl` not `https://`, or with a path, trailing slash or userinfo | the redirect URI is this plus `/auth/callback`; the schema refuses it for `--set` too |
| `ingress.enabled` without `tlsSecretName`, or in localAdmin mode | a secure cookie never crosses plain HTTP; localAdmin authenticates nobody |
| `ingress.host` not `publicBaseUrl`'s authority | every browser would get `421` |
| a binding namespace outside the bound set or outside `controller.watchNamespaces`; `*` or `?` in a binding | |
| `networkPolicy.enabled` without both ingress selectors | |
| `api.console.enabled` without `api.enabled` or without `mode` | |
| shared without `controller.watchNamespaces`, or with the release namespace in it | O1 |
| any `ui.*` | withdrawn, release-notes item 75 |
| `requireTrustedProxy` with no proxy, in localAdmin, or with a range wider than `/16` (IPv4) or `/48` (IPv6) | |
| `trustedProxyService` with one of `namespace`/`name`, or in localAdmin | |
| `ingress.enabled` in shared mode with no trusted proxy and `trustedProxy` not `none` | no per-peer cap and one sign-in budget for everybody |
| `trustedProxy: none` beside a named proxy, another value, or in localAdmin | |
| `oidc.caBundle` naming a ConfigMap and a Secret, or an empty `key`; `systemRoots: false` without a bundle | |
| a `hostAliases` entry without `ip` and a hostname, or with a wildcard; an `oidcPeers` entry without `namespace`, `podLabels` and `port` | |

**The trusted proxy** decides who reaches the console, whose sign-in budget a
request spends, and who is capped at 32 connections
([docs/api.md](../../docs/api.md#conventions), *Rate limits*).
`trustedProxyService` (recommended) trusts the serving endpoints of the ingress
controller's Service, re-read every five seconds: whoever can edit that Service,
its EndpointSlices or a matching Pod or Deployment there can add an address,
and a `hostNetwork` ingress publishes its node's address, so its node's other
pods and processes are trusted too. `trustedProxyCidrs` works only when the
range holds the ingress pods and no other pod (the example's `192.0.2.0/24` is
a placeholder). Keep client addresses with `externalTrafficPolicy: Local`, the
PROXY protocol or a trusted L7 hop; Traefik's `forwardedHeaders.insecure`
would let a client choose its budget. A service mesh that re-originates
connections (Istio `127.0.0.6`, Linkerd `127.0.0.1`) is one peer: name it, or
opt out.

### The identity provider inside the cluster

- **`oidc.caBundle: {configMap | secret, key}`** mounts a private issuer CA's
  public certificates from one object in the release namespace, never inline
  PEM. It must exist before the pod; an unusable bundle stops the console with
  exit 2.
- **`hostAliases`** maps the issuer's name to an in-cluster address; the URL,
  the TLS name and `iss` are still checked against `oidc.issuer`. **Point it
  only at the IdP's own Service, with TLS terminated by the IdP**, never at a
  shared ingress, where an Ingress from any namespace can claim the issuer's
  host behind the owner's certificate. Real DNS needs none. A recreated Service
  gets a new ClusterIP, and the alias must follow.
- **`networkPolicy.oidcPeers`** allows egress to in-cluster IdP pods by
  selector, because an enforcing CNI matches after a Service's DNAT (the PoC's
  Dex: namespace `dex`, its labels, port `5554`). `oidcCIDRs` must be the IdP's
  real endpoints; empty under an enforcing CNI, the console starts NotReady.
  The policy's egress is DNS, the Kubernetes API (with
  `identity.kubernetesApiCIDRs`) and the IdP, and no broker or object-store
  port. **Docker Desktop commonly
  enforces nothing**: a local install proves nothing about deny behaviour.

Turning the console off removes its ConfigMap, Deployment and Service and stops
no backup, restore or evidence. The ConfigMap is immutable and
content-addressed. `rateLimits` renders only when it differs from the default,
so an install that set it rolls the console image back with the chart. Going
from shared to localAdmin ends every session.

## Images

**`controllerImage` and `runnerImage` default to the `latest` tag**: the owner's
decision of 2026-09-12. A tag names no fixed bytes, so the pull policies default
to `Always`. Main CI publishes `sha-<commit>`, `main` and `latest` after its
checks; a release tags the same digests and publishes a chart pinned by digest
([docs/gates.md](../../docs/gates.md)). A new tag does not restart a Deployment.

**`identity.bootstrapImage` is always a digest**, because its container can read
and patch the retained signer. Check it, and re-pin only to a reviewed runner
whose `identity bootstrap` and `identity distribute` match the templates, then
run `just chart-check`:

```bash
docker buildx imagetools inspect <identity.bootstrapImage from values.yaml>
docker run --rm --platform linux/amd64 <identity.bootstrapImage from values.yaml> identity bootstrap --help
```

**The pinned bootstrap image is an index of linux/amd64 and linux/arm64**, like
every published image since PROD-00.2: each node pulls its own variant. Re-pin
only to such an index (`docker buildx imagetools inspect` lists both).

**To pin the tags back**, resolve each with a manifest read, then set all six
values together:

```bash
docker buildx imagetools inspect <repository>:latest
helm install logweir charts/logweir -n logweir-system --create-namespace \
  --set controllerImage=<repository>@sha256:… --set runnerImage=<repository>@sha256:… \
  --set-string identity.bootstrapImage=<runner-repository>@sha256:… \
  --set imagePullPolicy=IfNotPresent --set runnerImagePullPolicy=IfNotPresent \
  --set identity.bootstrapImagePullPolicy=IfNotPresent
```

### Third-party images

- **`minio.image`, `minio.mcImage`**: the references
  `e2e/compose/docker-compose.yml` pins, the project's rebuild mirror of MinIO
  `RELEASE.2025-09-07T16-13-09Z` and mc `RELEASE.2025-08-13T08-35-41Z`, built on
  2026-09-24 after MinIO withdrew its public images. A stopgap;
  [third_party/minio-mirror/README.md](../../third_party/minio-mirror/README.md)
  has the recipe, provenance and AGPL-3.0 notes.
- **`demoKafka.image`**: `apache/kafka:3.7.1` by manifest-list digest, resolved
  on **2026-09-12** with `docker buildx imagetools inspect apache/kafka:3.7.1`.
  The two `demoKafka.clusterIds` were minted that day with the image's own
  `kafka-storage.sh random-uuid`.

### The identity lifecycle

Managed identity needs connected `helm install`, `upgrade` and `rollback` with
credentials for every `lookup`. A render that cannot ask the cluster
(`helm template`, `helm lint`, `--dry-run=client`, Argo CD) is refused, because
it would render the empty placeholders again. For Argo CD or `helm template |
kubectl apply` set `identity.renderedOffline: true`, which renders no placeholder
and no fresh-install trust grant, and create the empty retained objects once
first ([install.md, *Argo CD and other offline renders*](../../docs/install.md#argo-cd-and-other-offline-renders)).
`identity.renderedOffline: false` renders what `helm install` would, for a
preview that is never applied. The bootstrap and distribution Jobs are
`post-install,post-upgrade,post-rollback` hooks, so a rollback validates only
when its target has them. The retained Secret, ConfigMap and singleton are
creation-only `pre-install,pre-upgrade` hooks: no rollback overwrites a live
identity, and no private byte is in Helm's state. The hooks' ServiceAccounts,
Roles and RoleBindings persist as release objects; their tokens are mounted
only in hook pods. `logweir-identity-singleton` grants nothing and stops a
second release claiming `TrustRoster/default`: one installation per cluster, so
do not delete it to install a second signer. On a managed control plane list
every API endpoint in `identity.kubernetesApiCIDRs`, and validate the egress
policy on the production CNI.

## `kafka:`, pointing the chart at a real cluster

| `security.protocol` | `security.mechanism` | renders |
|---|---|---|
| `PLAINTEXT` | any | `mode: plaintext`, `tls: false` |
| `SASL_SSL` | `SCRAM-SHA-512` | `mode: scramSha512`, `tls: true` |
| `SASL_PLAINTEXT` | `SCRAM-SHA-512` | `mode: scramSha512`, `tls: false` |
| anything else | | refused at render, naming the supported set |

The Secret is yours and Logweir never reads it; the password reaches the probe
pod, which runs as `logweir-runner`, as a `secretKeyRef`. The objects go in `kubernetes.connectionsNamespace`,
which must exist and, with `controller.watchNamespaces`, be watched. Changing it
moves them, so move the Backups that name them too. A password key other than
`password`, or a private broker CA, needs a `KafkaCluster` you write yourself
([docs/kubernetes.md](../../docs/kubernetes.md)) with `kafka.enabled: false`.

## Node placement, pull secrets and Docker Hub

**Runner Jobs are the operator's objects, not the chart's.** `imagePullSecrets`
reach them through the `logweir-runner` ServiceAccount, because a pod inherits
its ServiceAccount's pull secrets. Node placement **does not**: there is no
`nodeSelector`, toleration or affinity path to a runner Job, so on a cluster
whose Kafka-adjacent nodes are tainted they will not schedule there. Resources
reach them per object (`Restore.spec.runnerResources`, at most 4 CPUs and 8Gi);
other Jobs state none, so a quota that requires limits needs a `LimitRange`, or
their pods are rejected (`PodCreateRejected`, `PodCreationForbidden`).

A Docker Hub repository **created by a push is public by default**, where a GHCR
package starts private; a private one answers an anonymous pull with `failed to
fetch anonymous token: … 403 Forbidden`. Anonymous pulls are
**rate-limited per source IP**, shared behind one NAT, so a pull secret helps on
a public image too.

## Amazon MSK

[`examples/msk.values.yaml`](examples/msk.values.yaml) is the shape. These facts
date from 2026-09-12 and **were never measured on MSK**
([support-matrix.md](../../docs/support-matrix.md#managed-kafka-providers)):

- SASL/SCRAM on MSK is port **9096**, `SASL_SSL` with `SCRAM-SHA-512`. The
  mode has a repeatable local row (PROD-01.2a); MSK itself has not been run
  against.
- The credential lives in **AWS Secrets Manager**: sync it into a Kubernetes
  Secret under the key `password` and name it in `kafka.secretRef`.
- **A password containing `"`, `'`, `$`, CR or LF is refused by the runner**
  (`logweir_core::guard::UNRENDERABLE_CREDENTIAL_CHARACTERS`).
- ACLs, measured one grant at a time on Apache Kafka 4.3.1
  ([support-matrix.md](../../docs/support-matrix.md#minimum-permissions)):
  source: Read, and DescribeConfigs so configurations read `captured`; the
  compose broker needed no cluster grant, and this list once named
  DescribeCluster, which nobody has checked on MSK. Target: Create, Write and
  Read on the target topics, Describe on the marker topic, DescribeConfigs on
  the Cluster and the target topics, and Delete on the scratch prefix, without
  which a scratch restore passes and leaves its topics behind
  (`Restore.status.teardown.failed`).
- **MSK IAM authentication is not implemented.** `AuthConfig::Token` is a named
  refusal; SASL/SCRAM is the path.

## Upgrading the CRDs by hand

Helm installs `crds/` once and never upgrades or deletes it. When a release
changes a CRD, apply it before `helm upgrade`:

```bash
kubectl apply --server-side --force-conflicts -f charts/logweir/crds/
kubectl diff --server-side --force-conflicts -f charts/logweir/crds/   # prints nothing
helm upgrade logweir charts/logweir -n logweir-system
```

Without `--force-conflicts` an existing CRD is refused (`conflicts with "helm"`)
and keeps its old schema while an `Established` wait still passes.

## Uninstall

`helm uninstall logweir -n logweir-system` leaves the thirteen CRDs (`kubectl
delete crd <name>` removes each with every object under it), the namespace,
`TrustRoster`, the retained signing Secrets, `logweir-signing-trust`,
`logweir-identity-singleton` and your RoleBindings. Keep the identity objects
for recovery and old-archive verification. **The demo MinIO's PVC is deleted**,
and under a `Delete` reclaim policy the demo archive with it. Logweir never
deletes an archive or evidence object.

## The checks that hold the chart to the tree

`just crds-check` holds `crds/` to the Rust emitter. `scripts/check-chart.sh`
(`just chart-check`) refuses a copy of `ui/` in the chart, checks the bootstrap
pin, lints every example, regenerates `rendered/` with no drift, requires a
digest on every image but the three Logweir images at `:latest`, refuses `ui.*`,
and holds the console's grants to the adapter. `scripts/check-image-api.sh`
(`just smoke-console`) compares the twenty-six files the console image serves
with `ui/`. `chart_lint.rs` checks what each flag renders, and
`scripts/helm-demo.sh` (`just helm-demo`) walks a real cluster.

Documentation is licensed [CC-BY-4.0](../../docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
