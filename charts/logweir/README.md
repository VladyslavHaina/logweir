# The Logweir Helm chart

## Copy this directory and install it

`charts/logweir/` is self-contained. Copy the whole directory into your own
repository, write a short values file, and install:

```bash
helm upgrade --install logweir . -n <namespace> --create-namespace -f my-values.yaml
```

That is the whole command. The chart has no subchart or dependency lock; its
CRDs are carried under `crds/`, while Kubernetes pulls the configured runtime
images (including the UI image when enabled). The only things you must supply are

1. **compatible published images** — `controllerImage`, `runnerImage`, and the
   separately digest-pinned `identity.bootstrapImage`. The chart already pins
   the last one to a reviewed runner digest (see *Runtime tags and the
   privileged bootstrap digest*); override all three together only for your own
   registry (*Bring your own registry* in
   [`docs/install.md`](../../docs/install.md));
2. **an archive** — `archive.url` and, for anything S3-compatible,
   `archive.s3.endpoint` and `archive.s3.region`; or `minio.enabled: true` to
   get one in the cluster;
3. **the `kafka:` block** — the cluster Logweir backs up, and optionally the
   scratch cluster it restores into.

Everything else has a default. For images you built and loaded yourself, use
the explicit `author-only.values.yaml` local override; the chart refuses to give
a mutable image signing-key access.
[`examples/msk.values.yaml`](examples/msk.values.yaml) is a complete one for a
real cluster: Amazon MSK over SASL/SCRAM, a tainted nodepool, a private
registry. [`values.yaml`](values.yaml) lists **every** option with its default,
one line each — it is deliberately short, and every explanation lives in this
file.

One chart that installs Logweir's control plane — the same objects
[`logweir.yaml`](../../logweir.yaml) ships — and, optionally, its own
object-store backend, two throwaway Kafka clusters and the UI. It is
**derived from `config/`**, never the other way round: `scripts/check-chart.sh`
holds the chart's CRDs byte-identical to the tree (and the page's bytes are
held to it by `scripts/check-image-ui.sh`, against the image that serves
them), and
`crates/logweir/tests/chart_lint.rs` holds the rendered control plane to the
install file. [`docs/install.md`](../../docs/install.md) is still the single
install document; this README is the chart's own.

## What it installs

| object | when | why |
|---|---|---|
| the fourteen `CustomResourceDefinition`s under `logweir.dev/v1alpha1` | always — from `crds/`, **once**, on `helm install` | Helm never upgrades or deletes the contents of `crds/`; see *Upgrading the CRDs* below |
| `ServiceAccount`, `ClusterRole`, `ClusterRoleBinding` `weirkeeper` | always | the one API client in the design; every granted verb has a caller and every call has a grant, no verb on `secrets`, no `update` on anything, and `delete` on **exactly** `topicdiscoveries` and `preflights` — the transient check kinds, whose retention windows nothing else can enforce ([`docs/kubernetes.md`](../../docs/kubernetes.md) §22.3). Status writes are merge `PATCH`es carrying a `metadata.resourceVersion` precondition. Since PLAT-05.2 it also holds `patch` on `backups`, for the one caller that detaches a terminal run from its schedule so that deleting the schedule stops collecting its history: metadata only, never `backups/status` (a separate resource string), and never a CEL-sealed `spec` (see §9 and §13) |
| `Deployment` `weirkeeper` | always | the control plane. Image `controllerImage`, pull policy `imagePullPolicy`, `LOGWEIR_RUNNER_IMAGE` from `runnerImage`, `LOGWEIR_RUNNER_PULL_POLICY` from `runnerImagePullPolicy`, the archive env from `archive.*`, and `LOGWEIR_POLICY_CONFIGMAP` / `LOGWEIR_INSTALLATION_NAMESPACE` for the policy below, plus `LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS` when `notify.allowInsecureSinks` is true |
| `ConfigMap` `weirkeeper-policy` | always | the installation policy — check ceilings, retention windows, discovery bounds, completeness attestations, the evidence allowlist and the legacy addressing. Rendered from `checks.*`, `engine.*`, `evidence.*` and `archive.s3.*`; see *The installation policy* below |
| `ClusterRole`s `logweir-viewer`, `logweir-operator`, `logweir-approver`, `logweir-trust-admin` | always, **unbound** | the four human roles; who may act where is your decision. `logweir-trust-admin` is cluster-scoped and needs a `ClusterRoleBinding` |
| `ValidatingAdmissionPolicy` + binding `logweir-console-credentials-only` | `admissionPolicy.enabled` | fences the console API's `create secrets` to the two Logweir credential types. **Kubernetes 1.30+ only** — see below |
| retained Secret `logweir-signing-key`, retained ConfigMap `logweir-signing-trust`, authority-free singleton `ClusterRole`, scoped Role/Binding, short-lived Job | `identity.enabled` | atomically provision/adopt one cluster installation signer without Helm ever carrying private bytes; validate on install, upgrade and supported rollback |
| `NetworkPolicy` `logweir-runner-egress`, `ServiceAccount` `logweir-runner` | release namespace and every `identity.authorizedRunnerNamespaces` entry | runner prerequisites; additional namespaces receive the same retained signer through scoped short-lived distribution, never an independently minted key |
| `NetworkPolicy` `logweir-identity-kubernetes-api-egress` | every identity-enabled runner namespace | excludes bootstrap from runner arbitrary-443 egress; permits DNS plus discovered/configured Kubernetes API destinations only |
| `Deployment` + `Service` `<release>-minio`, a PVC, `Secret` `<release>-minio-root`, `Secret` `logweir-s3`, `Job` `<release>-minio-seed` | `minio.enabled` | an in-cluster archive with the buckets `kafka-backups` and `logweir-evidence` |
| `StatefulSet` + two `Service`s `<release>-kafka-source` and `-target`, `Job` `<release>-kafka-seed` | `demoKafka.enabled` | two single-broker KRaft clusters; `orders` and `payments` seeded on the source, the marker topic `logweir.scratch` on the target |
| `Deployment`, `Service`, `ServiceAccount`, `ClusterRole`s, `RoleBinding` `<release>-ui` | `ui.enabled` | `kubectl proxy` serving the twenty-two UI files and the API on one origin, with its own authority (below). The files come from the image `ui.image`, not from a ConfigMap |

Nothing optional is on by default. The release gate renders the snapshots with
the pinned bootstrap digest exactly as shipped; the rest of the default render
matches the same
controller, env, security context and RBAC rules as `logweir.yaml`
(`chart_lint_default_render_agrees_with_the_install_file`).

## The installation policy (`checks.*`, `engine.*`, `evidence.*`)

The chart renders one administrator-owned `ConfigMap`, `weirkeeper-policy`, in
the release namespace. It is what tunes the check framework —
`TopicDiscovery`, `Preflight` and evidence fetches — and it is the **only**
place an operator cannot reach: writing it needs `create`/`update` on a
ConfigMap in this namespace, and `logweir-operator` names no `configmaps` at
all. [`docs/kubernetes.md`](../../docs/kubernetes.md) §22.2 is the field
reference; this is what the values do.

| value | default | what it decides |
|---|---|---|
| `checks.maxActivePerNamespace` | `4` | check Jobs running at once in one namespace. Over it a request is `Queued`, not failed |
| `checks.maxActiveTotal` | `20` | and across the installation |
| `checks.maxActiveDiscoveriesPerConnection` | `1` | concurrent inventories against one connection |
| `checks.maxEvidenceFetchActivePerNamespace` | `4` | a **separate** pool, so verification is never starved by interactive checks |
| `checks.discovery.freshSeconds` | `900` | after this an inventory reads *stale*, never *wrong* |
| `checks.discovery.retentionSeconds` | `86400` | a terminal `TopicDiscovery` is collected after this |
| `checks.discovery.keepPerConnection` | `5` | and never more than this many are kept per connection |
| `checks.discovery.defaultMaxTopics` | `20000` | for a request that names none |
| `checks.discovery.hardMaxTopics` | `50000` | the ceiling a request is clamped to. It only ever LOWERS a request |
| `checks.discovery.visibilityAttestations` | `[]` | the **only** route to `visibility.state: attestedComplete` |
| `checks.preflight.defaultTimeoutSeconds` | `120` | the default check budget |
| `checks.preflight.retentionSeconds` | `3600` | a terminal `Preflight` is collected after this |
| `engine.allowUnverifiedCustomCa` | `false` | whether a destination may carry a CA the archive engine cannot verify |
| `evidence.controllerIdentityLocations` | `[]` | where the controller's own identity may read evidence from. An unlisted location is refused |

**`helm install` refuses what the controller would refuse.** Every per-field
bound in `values.schema.json` is pinned to the constant the parser uses —
`hardMaxTopics` to `check_contract::MAX_TOPICS_CEILING` (50 000), the preflight
timeout to `1..=600` — and `templates/policy.yaml` fails the render, naming both
values, for the two rules JSON Schema cannot express:
`checks.maxActiveTotal >= checks.maxActivePerNamespace` and
`checks.discovery.defaultMaxTopics <= checks.discovery.hardMaxTopics`. That
matters because a document the controller refuses fails **closed and almost
silently**: every attestation and every evidence location is discarded, the
ceilings revert to the compiled-in defaults, and the only signals are one
advisory row on a `Preflight` and one `WARN` line in the controller log.

**The two empty lists are the safe direction, not an oversight.** With no
attestation nothing can ever be `attestedComplete`; with no allowlist an
unlisted evidence location is refused with
`ControllerIdentityNotAllowlisted`.

**An attestation is nine fields and every one is required**, because a blank
`clusterId` or `principal` matches nothing while *looking* like an attestation
somebody can rely on. `values.schema.json` refuses a partial one at install
time:

```yaml
checks:
  discovery:
    visibilityAttestations:
      - id: att-orders-prod
        namespace: team-a
        kafkaCluster: source
        clusterId: M29I2S7FQPyHBEX12Vx7XA   # the id the runner reads from the broker
        principal: User:backup               # the principal Logweir presents
        attestedBy: platform-admin@example.invalid
        attestedAt: "2026-09-15T00:00:00Z"
        expiresAt: "2026-12-15T00:00:00Z"
        statement: >-
          User:backup has DESCRIBE on literal Topic:* with no DENY;
          reviewed ACL export 2026-09-14
```

It applies only on an exact match of all four identifiers, only before
`expiresAt`, and only to a listing that was not truncated. **Logweir never
verifies the statement** — the UI renders "attested by *X* at *T*; not verified
by Logweir".

`legacyArchiveAddressing` is not a value of its own: it is rendered from
`archive.s3.*`, the same values the Deployment's `AWS_*` env comes from and
behind the same "only when an endpoint is set" guard. An install with no
endpoint publishes an empty block rather than `allowHttp: true`.

## `admissionPolicy.enabled` — fencing the console's `create secrets`

Off by default, for **one** reason: `admissionregistration.k8s.io/v1`
`ValidatingAdmissionPolicy` is Kubernetes **1.30+** and this chart's floor is
1.29, where the document is rejected with `no matches for kind`. It is not a
security opinion — turn it on wherever the API server has the kind.

```yaml
admissionPolicy:
  enabled: true
  consoleServiceAccountName: logweir-api          # in the release namespace
  extraPrincipals:                                 # full subjects, for other namespaces
    - system:serviceaccount:team-a:logweir-api
```

**It is inert until D0 stage 7 lands `console.*`**: this chart ships no
`logweir-api` ServiceAccount, so the subject list names a principal that does
not exist yet. Enabling it early costs one object and it becomes load-bearing
the moment the console arrives — but "enabled" is not "fenced" before then, and
the console's ServiceAccount name must then equal
`admissionPolicy.consoleServiceAccountName` (which is REQUIRED and non-empty:
an absent, empty or null one renders a subject that matches nobody, so the
schema and the template both refuse it).

It requires that a Secret the console creates carries one of the two Logweir
credential types (`logweir.dev/object-store-credential`,
`logweir.dev/kafka-sasl-password`) and the
`app.kubernetes.io/managed-by: logweir` label — which closes the one thing an
unfenced `create secrets` could otherwise do, minting a
`kubernetes.io/service-account-token` for another ServiceAccount. Every other
principal in the cluster is skipped by the policy's `matchConditions`, which is
what makes `failurePolicy: Fail` safe. A cluster administrator can still delete
the policy; see [`docs/kubernetes.md`](../../docs/kubernetes.md) §22.4 for what
it does and does not prove, including the live check that has **not** been run.

## The three flags, in plain words

* **`minio.enabled`** — *bring a backend.* A MinIO with the two buckets the
  runner writes and the controller reads, and the `logweir-s3` Secret minted
  from its root credential. Demo-only: the root user is not a read-only
  principal, and a production install brings its own archive and its own
  Secrets and leaves this off. With it on and `archive.*` empty, the
  controller is pointed at it automatically.
* **`demoKafka.enabled`** — *bring two clusters to back up from and restore
  into.* PLAINTEXT, emptyDir, one broker each. The demo's transport, not a
  recommendation.
* **`ui.enabled`** — *serve the page from the cluster.* Read *The UI's
  authority* before turning it on.

## `notify.allowInsecureSinks` — the one notification setting

| value | default | what it decides |
|---|---|---|
| `notify.allowInsecureSinks` | `false` | whether a delivery Job may POST an alert to a `http://` webhook or Slack URL |

`logweir notify deliver` refuses a non-`https://` sink **before it dials**, so
a scratch receiver on `http://echo.<ns>.svc:8080` receives nothing on a default
install. Set this true and the chart renders
`LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS=1` on the `weirkeeper` Deployment; the
controller forwards `NOTIFY_ALLOW_INSECURE_SINKS=1` into every delivery Job.
Left alone it renders **nothing** — not the variable with a falsy value — so
the Deployment and the Jobs are byte-identical to what they were before the
value existed.

It is an **installation** setting and a `ProtectionPolicy` cannot turn it on: a
namespaced object that could would let whoever creates a policy downgrade their
own alerts' transport to cleartext, carrying the event, the policy's name, its
health and — on Slack, where the URL *is* the credential — a bearer token.
**Local development only; production leaves it `false`.**

## The five-minute path

With published images, `demo.values.yaml` needs no image override because the
reviewed bootstrap digest is pinned. To run images built from this checkout
instead, build/load all three local images and add the explicit development
override:

```bash
just image && just image-weirkeeper && just image-ui
helm install logweir charts/logweir -n logweir-system --create-namespace \
  -f charts/logweir/examples/demo.values.yaml \
  -f charts/logweir/examples/author-only.values.yaml --wait --timeout 10m
cargo build -p logweir                # the walk mints the approval with the shipped CLI
bash scripts/helm-demo.sh             # or: just helm-demo
```

`--wait` returns when both brokers, MinIO, the UI and the controller are
Ready and the identity/seed Jobs have succeeded (they are Helm hooks; a
succeeded one is deleted, a failed one stays for `kubectl logs`). The walk uses
the chart-managed installation signer, mints only an approver key, creates the
remaining demo credentials, applies the
`TrustRoster`, probes two `KafkaCluster`s to `reachable: true`, fires a
`BackupSchedule`, restores from its `Backup` with an approval minted on the
host, verifies the scorecard with both readers, fetches the page through a
port-forward, and tears everything down — the cluster ends with no
`logweir-*` namespace. Every step prints its exit codes.

On a cluster that holds images you built yourself (`just image && just
image-weirkeeper`), add `examples/author-only.values.yaml` — and read its
header: **an author-only install is not evidence of publication.**

## Pointing a real install at a real archive

[`examples/minimal.values.yaml`](examples/minimal.values.yaml) is the operator
alone at the completed release's shipped image defaults with the three values a stranger sets:
`archive.url`, `archive.s3.endpoint` (empty for Amazon S3 proper) and
`archive.s3.region`. The chart creates the installation signer and public
record; there is no local signing-key ceremony. Then, **before any custom
resource**, create the archive/SCRAM/controller credentials in
`docs/install.md`, create only the independent approver identity, and authorize
the published signer in cluster-scoped `TrustRoster/default`. Additional runner
namespaces must already exist and be listed in
`identity.authorizedRunnerNamespaces`. Run `just check-secrets <namespace>`
only after the Helm hooks succeed.

The controller's read-only evidence credential is the `logweir-evidence-ro`
Secret in the release namespace. It is `optional: true` on the Deployment: the
controller starts without it and every verification reads `NotAttempted`,
which is a choice and not a bad document.

## Runtime tags and the privileged bootstrap digest

**The chart's defaults name `controllerImage` and `runnerImage` by the `latest`
TAG, not by a digest. That is the owner's decision of 2026-09-12**, taken after
the trade-off below was put to them, and it is this chart's ruling alone:
`config/manager/deployment.yaml`, `logweir.yaml` and
`weirkeeper::job::RUNNER_IMAGE` still pin digests under Global Constraint 7, and
so do the four third-party images this chart can bring (MinIO, `mc`,
`apache/kafka`, `kubectl`). The two repositories are the tree's own — the gate
derives them from those two files rather than spelling them — so a namespace
change propagates here on its own.

`identity.bootstrapImage` is deliberately stricter. Its short-lived container
can read and atomically patch the retained private signer, so the value must be
an immutable `@sha256` reference even while ordinary controller/runner jobs use
tags. The only exception is
`identity.allowMutableBootstrapImageForDevelopment: true`, paired with a local
image and `bootstrapImagePullPolicy: Never` in `author-only.values.yaml`.
The shipped default is the runner image main CI published for revision
`4956785` (Actions run 35019727967): the images job pulled that exact digest
back on its native amd64 host and ran `scripts/check-image.sh`, including
`identity bootstrap --help`, before any public tag moved, and the image config
carries `org.opencontainers.image.revision` for that commit. Check it yourself
with a manifest read and one run of the pinned reference from `values.yaml`:

```bash
docker buildx imagetools inspect <identity.bootstrapImage from values.yaml>
docker run --rm --platform linux/amd64 <identity.bootstrapImage from values.yaml> \
  identity bootstrap --help
```

An emptied value still refuses to render rather than borrowing a tag. Re-pin
only to a newer reviewed runner digest whose `identity bootstrap` and
`identity distribute` arguments match these templates, then rerun
`just chart-check`.

**The bootstrap image is amd64-only, like every runner image.** The hook Jobs
execute the runner binary, so on an arm64 node without amd64 emulation their
container fails with `exec format error`, `helm install --wait` reports the
failed post-install hook, and no identity is written. Docker Desktop on Apple
silicon emulates amd64 and runs it. On a mixed-architecture cluster schedule the
hooks with `kubernetes.nodeSelector: {kubernetes.io/arch: amd64}` (set
`controller.nodeSelector` explicitly if the controller should run elsewhere).
Runner Jobs have no placement path yet (*Node placement* below), so Logweir's
data plane needs amd64-capable nodes either way.

**What a mutable tag does not promise.** The bytes behind `:latest` can change
under you: the same reference can resolve to different content tomorrow, on a
different node, or mid-rollout. A digest named bytes, and those bytes carried
the org-root anchor baked into the image (`/etc/logweir/org-root.fingerprint`);
under a tag, an image can be replaced upstream without a single Kubernetes
object changing. That is the guarantee this default trades away, and it is why
the two pull policies below are what they are.

**The pull policies follow the tag.** `imagePullPolicy` defaults to `Always` —
Kubernetes' own default for a `:latest` reference; `IfNotPresent` under a
mutable tag is a pod running whatever bytes its node happened to cache first.
`runnerImagePullPolicy` is a separate value, rendered as
`LOGWEIR_RUNNER_PULL_POLICY` on the Deployment and read once at startup by the
controller, and it defaults to `Always` for the same reason. The compiled-in
default the controller falls back to when that variable is unset is still
`Never`, which is right for an image LOADED onto a node — the laptop path,
`kind`, and `examples/author-only.values.yaml`, which sets
`runnerImagePullPolicy: Never` explicitly. A value outside `Never` /
`IfNotPresent` / `Always` makes the controller refuse to start, because the API
server would otherwise reject every runner Job it created.

Main CI publishes all three images under `sha-<commit>`, `main` and `latest`
after quality, backup/restore and image checks pass. Version releases publish
version tags. See [the workflow guide](../../docs/gates.md) and the exact
[Actions run](https://github.com/VladyslavHaina/logweir/actions) for digests.
The UI also defaults to `ui.imagePullPolicy: Always`; use `Never` for an image
loaded locally. Publishing a tag does not restart an existing Deployment.

**How to pin them back.** Resolve each tag to the bytes it names — a manifest
read, not a pull:

```bash
docker buildx imagetools inspect <repository>:latest
```

then install with all four values together, because a digest names bytes that
cannot change and re-pulling them buys nothing:

```bash
helm install logweir charts/logweir -n logweir-system --create-namespace \
  --set controllerImage=<repository>@sha256:… \
  --set runnerImage=<repository>@sha256:… \
  --set-string identity.bootstrapImage=<runner-repository>@sha256:… \
  --set imagePullPolicy=IfNotPresent \
  --set runnerImagePullPolicy=IfNotPresent \
  --set identity.bootstrapImagePullPolicy=IfNotPresent
```

### Identity values

| value | default / contract |
|---|---|
| `identity.enabled` | `true`; set `false` only for an explicitly external/low-level identity lifecycle |
| `identity.bootstrapImage` | the reviewed runner digest main CI published for `4956785` (amd64); an emptied or mutable value refuses to render unless the development override below is set |
| `identity.bootstrapImagePullPolicy` | `IfNotPresent`; immutable bytes do not need an `Always` pull |
| `identity.allowMutableBootstrapImageForDevelopment` | `false`; only the local Docker Desktop/kind override sets it true with pull policy `Never` |
| `identity.publicConfigMapName` | `logweir-signing-trust`; public SPKI, key id, algorithm and trust reference only |
| `identity.externalSecret.{name,key}` | optional get-only P-256/Ed25519 PKCS#8 adoption source in the release namespace |
| `identity.authorizedRunnerNamespaces` | `[]`; release namespace is implicit, each listed existing namespace receives the same protected signer and runner prerequisites |
| `identity.kubernetesApiCIDRs` | `[]`; additional exact API `/32` (IPv4) or `/128` (IPv6) endpoints for provider/CNI DNAT behavior, alongside Helm-discovered service/endpoint addresses; broad CIDRs are schema-rejected |

Managed identity requires connected `helm install`, `helm upgrade`, and
`helm rollback` with credentials able to perform every chart `lookup`. Offline
`helm template`/GitOps output is unsupported and must not be applied with
`identity.enabled=true`: it cannot observe retained identity objects or the
cluster singleton and may render fresh empty placeholders. Use connected Helm,
or set `identity.enabled=false` and follow the documented low-level/manual
identity lifecycle. `identity.externalSecret` remains a connected adoption
mode, not an offline exception.

For managed control planes, list every real Kubernetes API endpoint as an exact
IPv4 `/32` or IPv6 `/128`. The bootstrap policy supports only TCP 443 and 6443;
other API ports are unsupported. Its kube-apiserver pod selector generally does
not reach a provider-hosted endpoint. NetworkPolicy/DNAT behavior must be
validated on the production CNI; Docker Desktop is only structural evidence.

The bootstrap and distribution Jobs declare
`post-install,post-upgrade,post-rollback`. Rollback validation therefore runs
only when the rollback target itself contains these hooks; a pre-bootstrap
target provides retention but no active validation.

The retained empty Secret/ConfigMap/singleton objects are creation-only
`pre-install,pre-upgrade` hooks and never `pre-rollback` hooks or ordinary
release-manifest resources. This prevents Helm 3 as well as Helm 4 from
reconciling an old empty placeholder over live identity during rollback. Helm
stores only the empty hook definition; the bootstrap patch and all private bytes
remain outside Helm release manifests/state.

Only the hook processes and projected tokens are short-lived. Their
resource-name-scoped ServiceAccounts, Roles, and RoleBindings persist as normal
release resources for later hooks; Helm does not revoke them after success.
Those ServiceAccounts default token automount off, and only hook Pods opt in.

The fixed retained `ClusterRole/logweir-identity-singleton` grants no verbs; it
prevents a second release in another namespace from independently claiming the
same global `TrustRoster/default` contract. This v0.1 one-installation-per-cluster
rule remains until PLAT-19.1 introduces explicit trust references.

## The four third-party images, and where their digests came from

None of them is part of the `latest` ruling: MinIO, `mc`, `apache/kafka` and
`kubectl` are pinned by digest under Global Constraint 7, and this is the
provenance of each, so nobody has to trust a bare hash.

* **`minio.image` and `minio.mcImage`** are the references
  `e2e/compose/docker-compose.yml` pins, copied byte for byte and never
  resolved again. They are **quay.io**, not Docker Hub, because Docker Hub
  refuses anonymous pulls of `minio/minio` (plan erratum E30(a)).
* **`demoKafka.image`** — `apache/kafka:3.7.1`, the compose stack's broker,
  pinned by its **manifest-list** digest so the same reference resolves on an
  amd64 CI runner and on an arm64 development host. Resolved once, on
  **2026-09-12**, with the `kindest/node` provenance idiom:

  ```bash
  docker buildx imagetools inspect apache/kafka:3.7.1
  ```

* **`ui.image`** — a kubectl image at the cluster's minor version, also a
  manifest-list digest, resolved once on 2026-09-12 with:

  ```bash
  docker buildx imagetools inspect registry.k8s.io/kubectl:v1.34.1
  ```

  `registry.k8s.io` is the Kubernetes project's own registry. The brief named
  `bitnami/kubectl`; on 2026-09-12 `docker buildx imagetools inspect
  bitnami/kubectl:1.34.1` (and `:1.34`) answered `not found` — Bitnami's Docker
  Hub catalogue no longer publishes versioned tags, and `:latest` is a tag,
  which Global Constraint 7 forbids. `kubectl proxy` is a reverse proxy and a
  file server, so the client/server skew rules do not touch it; that is why the
  same digest serves a 1.29 `kind` node in CI.

`demoKafka.clusterIds` are not images but were minted the same way and on the
same day: the image's own `kafka-storage.sh random-uuid`, twice, because the
image's built-in default is **one fixed id** and two brokers reporting the same
id are one cluster to phase 0 — measured on 2026-09-12, on the second walk of
`scripts/helm-demo.sh`. Keep them different from each other; the template
refuses equal ids.

## `kafka:` — pointing the chart at a real cluster

A Kafka connection is not a chart value in Logweir's design: it is a
`KafkaCluster` custom resource the controller reconciles, probes, and reports
`status.clusterId` for. `kafka.enabled: true` makes the chart render those
objects from a flat block, so a stranger writes addresses and a Secret name
rather than a custom resource:

```yaml
kafka:
  enabled: true
  bootstrapServers: "b-1.example…:9096,b-2.example…:9096"   # or a YAML list
  security: { protocol: SASL_SSL, mechanism: SCRAM-SHA-512 }
  username: kafbat
  secretRef: my-scram-secret
  target:                       # optional — a restore needs one, a backup does not
    bootstrapServers: "b-1.scratch…:9096"
    security: { protocol: SASL_SSL, mechanism: SCRAM-SHA-512 }
    username: kafbat
    secretRef: my-scratch-secret
```

**The protocol/mechanism mapping is the chart's job, and it refuses what it
cannot speak.** The CRD's `auth.mode` enum is `plaintext | scramSha512` and the
client speaks SCRAM-SHA-512 only, so:

| `security.protocol` | `security.mechanism` | renders |
|---|---|---|
| `PLAINTEXT` | (any) | `mode: plaintext`, `tls: false` |
| `SASL_SSL` | `SCRAM-SHA-512` | `mode: scramSha512`, `tls: true` |
| `SASL_PLAINTEXT` | `SCRAM-SHA-512` | `mode: scramSha512`, `tls: false` |
| anything else | | **`helm` fails at render time**, naming the supported set |

A silently rendered `PLAIN` or `SCRAM-SHA-256` would be a chart that installs
and cannot authenticate, which is why it is a refusal and not a warning.
`kafka.enabled` and `demoKafka.enabled` together are a refusal too — `demoKafka`
brings its own two brokers and its own cluster objects — with a message saying
which one to turn off.

**The Secret is yours, and Logweir never reads it.** `secretRef` names a Secret
**in the release namespace** holding the SASL password under the key
`secretKey`. That key is `password` and can be nothing else: the operator
projects exactly one name into the probe
(`weirkeeper::controllers::restore::TARGET_PASSWORD_SECRET_KEY`), so the chart
refuses any other value rather than render a `KafkaCluster` whose probe cannot
read its credential. The password reaches the probe pod as a
`valueFrom.secretKeyRef` and never enters a status field, a log line or a
rendered document.

The rendered objects go in the release namespace with the chart's labels, and
the probe Job runs under the `logweir-runner` ServiceAccount the chart already
creates there.

**Two connection-contract fields have no `kafka:` value, deliberately.** The
`KafkaCluster` CRD accepts `auth.secretRef.passwordKey` (a password under a key
other than `password`) and `auth.tlsCa` (a private CA in a Secret or ConfigMap
key, for brokers whose certificate the runner image does not already trust) —
see `docs/kubernetes.md` §20. The flat `kafka:` block renders neither: it
exists so a stranger can point the chart at a cluster without writing a custom
resource, and both of those are choices an adopter with a private CA or a
managed secret store makes on the object itself. Write the `KafkaCluster` by
hand (leave `kafka.enabled: false`) when you need them; nothing else about the
install changes, and no chart value or controller permission is involved,
because both fields are references the kubelet resolves in the Job's own
namespace.

## Node placement, and where it does not reach

`kubernetes.nodeSelector`, `kubernetes.tolerations` and `kubernetes.affinity`
apply to **every pod this chart renders** — the controller, MinIO and its seed
Job, both demo brokers and their seed Job, and the UI. Each optional component
may override all three with its own block (`minio.nodeSelector`,
`demoKafka.tolerations`, `controller.affinity`, …); an override replaces the
top-level value for that component's pods rather than merging with it.

**Runner Jobs are the operator's objects, not the chart's**, and the gap is
named rather than silent:

* `imagePullSecrets` **do** reach them — through the `logweir-runner`
  ServiceAccount, because a pod inherits its ServiceAccount's pull secrets.
* Node placement **does not**. There is no `nodeSelector`, toleration or
  affinity path to a runner Job today. On a cluster whose only Kafka-adjacent
  nodes are tainted, a runner Job will not schedule there. Implementing it is
  an operator change and is out of this chart's scope.

`kubernetes.namespace` does **not** move the install. `helm -n` / `--namespace`
decides that, and every object carries `Release.Namespace`; the key exists for
the one place the chart needs a namespace *name* it cannot derive — the UI's
`RoleBinding` list, where it supplies `ui.namespaces` when that is empty.

## `imagePullSecrets`, and what Docker Hub does differently

`imagePullSecrets: [{name: regcred}]` is rendered on the controller
ServiceAccount, on the **runner** ServiceAccount (so every runner Job inherits
it) and on the optional components' pods. It is how a private registry is
pulled from — an ECR, or a Docker Hub repository that is not public.

Three Docker Hub properties decide whether a pull works, and two of them are
the opposite of GHCR's:

* A Docker Hub repository **created by a push is public by default**; a GHCR
  package starts **private**.
* That default is the account's setting, and an auto-created repository takes
  it. A private one answers an anonymous pull with `failed to fetch anonymous
  token: … 403 Forbidden`. Make it public, or set `imagePullSecrets`.
* Anonymous Docker Hub pulls are **rate-limited per source IP**, shared by
  every node behind one NAT — so a pull secret carrying a Docker Hub login is
  useful on a *public* image too.

## `environment:`

A label, and nothing else: `logweir.dev/environment: <value>` on every object
the chart renders (the six CRDs excepted — Helm copies `crds/` verbatim and
never templates it). It switches no behaviour, changes no name and gates
nothing. A key that silently did something would be worse than no key.

## Amazon MSK

[`examples/msk.values.yaml`](examples/msk.values.yaml) is the shape; these are
the facts it does not have room for, measured 2026-09-12:

* SASL/SCRAM on MSK is port **9096** (not 9092), `SASL_SSL` +
  `SCRAM-SHA-512` — which is exactly the one SASL combination Logweir speaks.
* The SCRAM credential lives in **AWS Secrets Manager**, associated with the
  cluster. Kubernetes cannot read it directly: sync it into a Kubernetes Secret
  in the release namespace (External Secrets Operator, the Secrets Store CSI
  driver, or by hand) under the key `password`, and name that Secret in
  `kafka.secretRef`.
* **A password containing `"`, `'`, `$`, CR or LF is refused by the runner** —
  `logweir_core::guard::UNRENDERABLE_CREDENTIAL_CHARACTERS`. Mint one without
  them; a refusal at drill time is worse than a refusal at creation time.
* The IAM/ACL principal needs **Describe and Read on the source topics plus
  DescribeCluster**, and **Describe, Create and Write on a target**. Logweir
  never deletes (Global Constraint 6), so no delete action is required.
* **MSK IAM authentication is not implemented.** `AuthConfig::Token` is a named
  refusal in the operator, not an oversight — SASL/SCRAM is the path.

## The UI's authority

With `ui.enabled`, one pod runs `kubectl proxy --www=/ui --www-prefix=/ui/
--address=0.0.0.0 --port=8001 --accept-hosts='.*'
--accept-paths='^/(ui/|apis/logweir\.dev/v1alpha1/)'` from **`ui.image`**. The
proxy attaches the pod's ServiceAccount credential — `<release>-ui` — to every
request it forwards, so **anyone who can reach that Service acts with that
ServiceAccount's authority.** The page holds no credential and asks for none
(Global Constraint 28: no key material in the page, none in the image that
serves it).

### The page is an image, not a ConfigMap

`ui.image` defaults to `docker.io/vladyslavhaina/logweir-ui:latest`. **What is
in it:** the pinned `registry.k8s.io/kubectl` (v1.34.1, resolved by digest on
2026-09-12 — the command is below) with the **twenty-two shipped UI files copied
in at `/ui`** and nothing else: no `README.md`, no `ui/tests/` (which carries a
throwaway keypair), no key material of any kind. It also carries Logweir's
`LICENSE` and `NOTICE` and, under `/usr/share/licenses/kubectl/`, kubectl's
Apache-2.0 licence and an inventory naming the base digest.

**The arguments are NOT in the image.** Its entrypoint is `kubectl` and it
declares no `CMD`; every flag above — `--accept-paths` above all, which is the
authorisation boundary — is the chart's, where `helm template` shows it to you
and three tests assert it.

**Until Task 39 the page arrived as a ConfigMap** the chart built from its own
byte-identical copy of `ui/`. Both the copy and the ConfigMap are gone. An
image is pinned, immutable once pushed and resolvable by digest; a ConfigMap is
a mutable API object, so the page a browser loaded was whatever the last holder
of `patch configmaps` had written. The guarantee got stronger, not weaker:
`scripts/check-image-ui.sh` (`just smoke-ui`) computes the sha256 of every file
the image serves and of every file under `ui/` and compares them, which is a
statement about the bytes your browser receives; the chart's old `cmp` loop
could only compare two directories in the Logweir repository.

**To override it** — an air-gapped cluster, or your own registry:

```bash
helm install logweir charts/logweir -n logweir-system \
  --set ui.enabled=true \
  --set ui.image=registry.example.com/logweir-ui@sha256:<digest>
```

Mirror it the same way you mirror the other two Logweir images
(`docs/install.md`). Its own `docker pull` is the only network access the UI
pod needs.
The ServiceAccount is bound to a ClusterRole carrying exactly the verbs the
page issues, measured from `ui/api.js` and `ui/pages/*.js`: `get`/`list` on
the five namespaced kinds, `create` on approvals, kafkaclusters,
backupschedules and restores, `patch` on backupschedules, and `list` on the
cluster-scoped trustrosters — never `watch`, never `delete`, never the shipped
`logweir-operator`'s `update`. The path filter admits only `/ui/` and
`/apis/logweir.dev/v1alpha1/`; the core API, pod exec and attach are refused
by the proxy before RBAC is consulted. There is no Ingress. Reach it with

```bash
kubectl port-forward -n logweir-system svc/logweir-ui 8001:8001
```

and open `http://127.0.0.1:8001/ui/`. The chart binds the page's role in the
release namespace and in each namespace listed under `ui.namespaces`. It also
mounts that exact, explicit set into the served page's runtime namespace picker;
the page does not list namespaces and cannot select a namespace the chart did
not bind. A single permitted namespace is selected automatically; with more
than one, select it in the picker. For any other namespace, add it to
`ui.namespaces` and upgrade the release (rather than only creating a binding):

The runtime context is an immutable, content-addressed ConfigMap, so an
upgrade rolls the UI to the new set and a later ConfigMap patch cannot change
the JavaScript the proxy serves.

```bash
kubectl create rolebinding logweir-ui --clusterrole=logweir-ui \
  --serviceaccount=logweir-system:logweir-ui -n <namespace>
```

The laptop path — `kubectl proxy --www=./ui` under your own kubeconfig, with
the page holding *your* authority — is unchanged and documented in
`docs/install.md`, *Serving the UI*.

### Reproducing the PLAT-13 live UI harness

This maintainer check needs Docker Desktop, `kubectl`, Helm, Node.js, and
Playwright with Chromium installed. It deploys only the UI/proxy template; it
does not deploy the controller or runner and is not a full-stack recovery test.
The temporary chart is rebuilt from the current, unmodified UI template and
helpers, so it does not depend on a pre-existing `/tmp` directory:

```bash
set -euo pipefail
image="logweir-ui:plat13-e2e-local-$$"
wrapper=
port_forward_pid=
port_forward_log=$(mktemp "${TMPDIR:-/tmp}/plat13-ui-forward.XXXXXX")
release_attempted=false
owned_namespaces=()

stop_port_forward() {
  if test -n "${port_forward_pid:-}"; then
    if kill -0 "$port_forward_pid" 2>/dev/null; then
      kill "$port_forward_pid" 2>/dev/null || true
    fi
    wait "$port_forward_pid" 2>/dev/null || true
    port_forward_pid=
  fi
}

cleanup_plat13() {
  original_status=$?
  trap - EXIT
  set +e
  cleanup_status=0
  stop_port_forward
  if test "$release_attempted" = true; then
    helm uninstall plat13-ui-e2e --kube-context docker-desktop \
      --namespace plat13-ui-e2e --ignore-not-found --wait --timeout=120s || cleanup_status=1
  fi
  # Bash 3.2 with `set -u` treats a declared-but-empty array expansion as an
  # unbound variable. The `+` guard makes zero owned namespaces a zero-iteration
  # loop without weakening nounset for the rest of the recipe.
  for namespace in ${owned_namespaces[@]+"${owned_namespaces[@]}"}; do
    label=$(kubectl --context docker-desktop get namespace "$namespace" \
      --ignore-not-found=true \
      -o jsonpath='{.metadata.labels.plat13\.logweir\.dev/environment}')
    label_status=$?
    if test "$label_status" -ne 0; then
      cleanup_status=1
    elif test -z "$label"; then
      :
    elif test "$label" != ui-e2e; then
      echo "refusing to delete namespace $namespace with ownership label $label" >&2
      cleanup_status=1
    else
      kubectl --context docker-desktop delete namespace "$namespace" \
        --wait=true --timeout=120s || cleanup_status=1
    fi
  done
  if docker --context desktop-linux image inspect "$image" >/dev/null 2>&1; then
    docker --context desktop-linux image rm "$image" >/dev/null 2>&1 || cleanup_status=1
  fi
  if test -n "${wrapper:-}"; then
    rm -rf "$wrapper" || cleanup_status=1
  fi
  rm -f "$port_forward_log" || cleanup_status=1
  if test "$original_status" -ne 0; then
    exit "$original_status"
  fi
  exit "$cleanup_status"
}

start_port_forward() {
  : > "$port_forward_log"
  kubectl --context docker-desktop -n plat13-ui-e2e port-forward \
    svc/plat13-ui-e2e-ui 18132:8001 > "$port_forward_log" 2>&1 &
  port_forward_pid=$!
}

wait_for_ui() {
  for attempt in $(seq 1 30); do
    if ! kill -0 "$port_forward_pid" 2>/dev/null; then
      echo "PLAT-13 port-forward exited before readiness" >&2
      tail -n 20 "$port_forward_log" >&2
      return 1
    fi
    if curl --fail --silent --show-error \
      http://127.0.0.1:18132/ui/ > "$wrapper/ui-readiness.html"; then
      if ! kill -0 "$port_forward_pid" 2>/dev/null; then
        echo "PLAT-13 port-forward exited during readiness probe" >&2
        tail -n 20 "$port_forward_log" >&2
        return 1
      fi
      if grep -Fq 'Forwarding from 127.0.0.1:18132 -> 8001' "$port_forward_log" \
        && grep -Fq '<title>Logweir</title>' "$wrapper/ui-readiness.html" \
        && grep -Fq 'id="view-slot"' "$wrapper/ui-readiness.html"; then
        return 0
      fi
    fi
    sleep 1
  done
  echo "PLAT-13 UI did not become ready within 30 seconds" >&2
  tail -n 20 "$port_forward_log" >&2
  return 1
}

trap cleanup_plat13 EXIT
wrapper=$(mktemp -d "${TMPDIR:-/tmp}/plat13-ui-chart.XXXXXX")
mkdir -p "$wrapper/templates"
cp charts/logweir/templates/ui/ui.yaml "$wrapper/templates/ui.yaml"
cp charts/logweir/templates/_helpers.tpl "$wrapper/templates/_helpers.tpl"
printf '%s\n' 'apiVersion: v2' 'name: logweir-ui-e2e' 'type: application' \
  'version: 0.0.0' 'appVersion: test' > "$wrapper/Chart.yaml"
printf '%s\n' 'environment: ""' 'kubernetes:' '  namespace: ""' \
  '  nodeSelector: {}' '  tolerations: []' '  affinity: {}' \
  'imagePullSecrets: []' 'ui:' '  enabled: false' '  image: ""' \
  '  imagePullPolicy: Never' '  namespaces: []' '  nodeSelector: {}' \
  '  tolerations: []' '  affinity: {}' > "$wrapper/values.yaml"

docker --context desktop-linux build --platform linux/arm64 \
  -f Dockerfile.ui -t "$image" .
bash scripts/check-image-ui.sh "$image"

kubectl --context docker-desktop create namespace plat13-ui-e2e
owned_namespaces+=(plat13-ui-e2e)
kubectl --context docker-desktop label namespace plat13-ui-e2e \
  plat13.logweir.dev/environment=ui-e2e
kubectl --context docker-desktop create namespace plat13-ui-e2e-second
owned_namespaces+=(plat13-ui-e2e-second)
kubectl --context docker-desktop label namespace plat13-ui-e2e-second \
  plat13.logweir.dev/environment=ui-e2e
kubectl --context docker-desktop create namespace plat13-ui-e2e-missing
owned_namespaces+=(plat13-ui-e2e-missing)
kubectl --context docker-desktop label namespace plat13-ui-e2e-missing \
  plat13.logweir.dev/environment=ui-e2e

release_attempted=true
helm upgrade --install plat13-ui-e2e "$wrapper" \
  --kube-context docker-desktop --namespace plat13-ui-e2e \
  --set ui.enabled=true --set "ui.image=$image" \
  --set ui.imagePullPolicy=Never
kubectl --context docker-desktop -n plat13-ui-e2e rollout status \
  deployment/plat13-ui-e2e-ui --timeout=120s
start_port_forward
wait_for_ui

NODE_PATH="$(npm root -g)" PLAT13_STAGE=single \
  PLAT13_BASE_URL=http://127.0.0.1:18132/ui/ \
  PLAT13_PRIMARY_NAMESPACE=plat13-ui-e2e \
  PLAT13_UI_SERVICE_ACCOUNT=plat13-ui-e2e/plat13-ui-e2e-ui \
  node scripts/plat13-ui-e2e.mjs > /tmp/plat13-ui-e2e-single.json

stop_port_forward
helm upgrade plat13-ui-e2e "$wrapper" --kube-context docker-desktop \
  --namespace plat13-ui-e2e --set ui.enabled=true --set "ui.image=$image" \
  --set ui.imagePullPolicy=Never \
  --set-string 'ui.namespaces[0]=plat13-ui-e2e-second' \
  --set-string 'ui.namespaces[1]=plat13-ui-e2e-missing'
kubectl --context docker-desktop -n plat13-ui-e2e rollout status \
  deployment/plat13-ui-e2e-ui --timeout=120s
kubectl --context docker-desktop delete namespace plat13-ui-e2e-missing \
  --wait=true --timeout=120s
start_port_forward
wait_for_ui

NODE_PATH="$(npm root -g)" PLAT13_STAGE=multi \
  PLAT13_BASE_URL=http://127.0.0.1:18132/ui/ \
  PLAT13_PRIMARY_NAMESPACE=plat13-ui-e2e \
  PLAT13_SECOND_NAMESPACE=plat13-ui-e2e-second \
  PLAT13_MISSING_NAMESPACE=plat13-ui-e2e-missing \
  PLAT13_UI_SERVICE_ACCOUNT=plat13-ui-e2e/plat13-ui-e2e-ui \
  node scripts/plat13-ui-e2e.mjs > /tmp/plat13-ui-e2e-multi.json

fake_kubectl=$(mktemp -d "${TMPDIR:-/tmp}/plat13-fake-kubectl.XXXXXX")/kubectl
printf '%s\n' '#!/bin/sh' \
  'case " $* " in *" --context docker-desktop "*) ;; *) exit 96 ;; esac' \
  'case " $* " in *" --ignore-not-found=true "*) ;; *) exit 95 ;; esac' \
  'echo "Error from server (Forbidden): controlled cleanup denial" >&2' \
  'exit 1' > "$fake_kubectl"
chmod +x "$fake_kubectl"
set +e
NODE_PATH="$(npm root -g)" PLAT13_STAGE=single \
  PLAT13_BASE_URL=http://127.0.0.1:18132/ui/ \
  PLAT13_PRIMARY_NAMESPACE=plat13-ui-e2e \
  PLAT13_UI_SERVICE_ACCOUNT=plat13-ui-e2e/plat13-ui-e2e-ui \
  PLAT13_KUBECTL="$fake_kubectl" PLAT13_CLEANUP_NEGATIVE_CONTROL=forbidden \
  node scripts/plat13-ui-e2e.mjs > /tmp/plat13-ui-e2e-cleanup-negative.json
negative_status=$?
set -e
test "$negative_status" -eq 1
node -e 'const r=require("/tmp/plat13-ui-e2e-cleanup-negative.json");
  if (r.ok || !r.cleanupFailure || !r.cleanupFailure.includes("Forbidden")) process.exit(1)'
rm -rf "$(dirname "$fake_kubectl")"
```

The direct redirections preserve Node's exit code. The harness emits every
created object name and UID to stderr as soon as it has them, then verifies
exact-object cleanup in `finally`. The bounded fake-kubectl control uses that
same cleanup path and proves a Forbidden lookup sets `cleanupFailure` and exits
1. The `EXIT` trap safely stops or reaps the current forward, preserves an
earlier failing status, and removes only the release and explicitly labeled
namespaces created by this recipe. It leaves every other release, including
`scram-local`, untouched.

## Upgrading the CRDs by hand

Helm installs `crds/` once and, by its own rule, never upgrades or deletes it.
When a release changes a CRD, apply the new definitions yourself before
`helm upgrade`:

```bash
kubectl apply --server-side -f charts/logweir/crds/
helm upgrade logweir charts/logweir -n logweir-system
```

`charts/logweir/crds/*.yaml` is byte-identical to `config/crd/*.yaml`
(`scripts/check-chart.sh` compares them with `cmp`), so applying either
directory is the same act.

## Uninstall, and what it leaves behind

```bash
helm uninstall logweir -n logweir-system
```

removes everything the release created **except**: the six CRDs (Helm never
deletes `crds/`; `kubectl delete crd <name>` removes each and every custom
resource stored under it), the MinIO `PersistentVolumeClaim` when
`minio.persistence.enabled` (delete it yourself, or keep the archive), the
namespace `--create-namespace` made, the cluster-scoped `TrustRoster`, retained
`Secret/logweir-signing-key` in the release and authorized runner namespaces,
retained `ConfigMap/logweir-signing-trust`, the authority-free retained
`ClusterRole/logweir-identity-singleton`, and any RoleBinding you created by
hand. Preserve those identity objects for same-installation recovery and old
archive verification; do not delete the singleton marker merely to install a
second independent signer. And, as with `kubectl delete -f
logweir.yaml`: no archive object and no evidence object is ever deleted by
Logweir — Global Constraint 6.

## The checks that hold the chart to the tree

* `scripts/check-chart.sh` (`just chart-check`, in `just gate`): CRDs
  byte-identical to the tree and no copy of `ui/` under the chart at all;
  `identity.bootstrapImage` pinned as `<runner repository>@sha256:<64 hex>`
  with the development override off, an emptied value still refused, plus
  `helm lint` for the defaults and every example as shipped;
  `helm template` regenerated into `rendered/` with no drift; every rendered
  image a digest EXCEPT the three Logweir images, which must be exactly
  `<repository>:latest` (and `:latest` on any other image is still refused),
  with the author-only render exempt by name — its whole premise is a locally
  built tag; the schema refusing `--set demoKafka.enabled=yes`; `values.yaml`
  naming the tree's own repositories at `:latest` — `ui.image` among them, its
  namespace derived from the runner pin.
* `scripts/check-image-ui.sh` (`just smoke-ui`; needs a Docker daemon, so it is
  in `docs/gates.md`'s stack/cluster table rather than in `just gate`): the
  twenty-two files the `logweir-ui` image serves, sha256 for sha256 against
  `ui/`, and nothing else under `/ui`. This is what replaced the chart's
  byte-copy arm.
* The existing image publication path runs `scripts/check-image.sh` against the
  exact pulled candidate digest and requires `identity bootstrap --help` before
  any public tag moves. The emitted compatible runner digest is what
  `identity.bootstrapImage` pins; no separate workflow or end-user hash step
  exists.
* `crates/logweir/tests/chart_lint.rs`: the rendered defaults agree with
  `logweir.yaml`; nothing optional renders under defaults; each flag renders
  its named objects; the chart carries no copy of `ui/` and mounts no ConfigMap
  into the proxy — the page's bytes are asserted against the image instead, by
  `scripts/check-image-ui.sh`.
* `scripts/helm-demo.sh` (`just helm-demo`; `.github/workflows/helm-demo.yml`
  on `kind`): the walk above, on a real cluster.

Documentation is licensed [CC-BY-4.0](../../docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
