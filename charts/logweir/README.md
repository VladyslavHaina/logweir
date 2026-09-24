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
| `Deployment` `weirkeeper` | always | the control plane. Image `controllerImage`, pull policy `imagePullPolicy`, `LOGWEIR_RUNNER_IMAGE` from `runnerImage`, `LOGWEIR_RUNNER_PULL_POLICY` from `runnerImagePullPolicy`, the archive env from `archive.*`, and `LOGWEIR_POLICY_CONFIGMAP` / `LOGWEIR_INSTALLATION_NAMESPACE` for the policy below, plus `LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS` when `notify.allowInsecureSinks` is true. **Probed**: `weirkeeper --probe live` / `--probe ready` by exec against the controller's own loopback health listener (`127.0.0.1:8081`, `LOGWEIR_HEALTH_ADDR`; no port is exposed). Liveness fails when the runtime is wedged — the listener shares the reconcilers' thread — or a controller task has ended, and three misses 20 s apart restart the pod; readiness waits for the client and every controller |
| `ConfigMap` `weirkeeper-policy` | always | the installation policy — check ceilings, retention windows, discovery bounds, completeness attestations, the evidence allowlist and the legacy addressing. Rendered from `checks.*`, `engine.*`, `evidence.*` and `archive.s3.*`; see *The installation policy* below |
| `ClusterRole`s `logweir-viewer`, `logweir-operator`, `logweir-approver`, `logweir-trust-admin`, `logweir-retention-admin` | always, **unbound** | the five human roles; who may act where is your decision. `logweir-trust-admin` is cluster-scoped and needs a `ClusterRoleBinding`; `logweir-retention-admin` is namespaced and is the only holder of a write verb on `retentionpolicies` — see *`retention.enabled`* below |
| `ValidatingAdmissionPolicy` + binding `logweir-console-credentials-only` | `admissionPolicy.enabled` | fences the console API's `create secrets` to the two Logweir credential types. **Kubernetes 1.30+ only** — see below |
| retained Secret `logweir-signing-key`, retained ConfigMap `logweir-signing-trust`, authority-free singleton `ClusterRole`, scoped Role/Binding, short-lived Job | `identity.enabled` | atomically provision/adopt one cluster installation signer without Helm ever carrying private bytes; validate on install, upgrade and supported rollback |
| `NetworkPolicy` `logweir-runner-egress`, `ServiceAccount` `logweir-runner` | release namespace and every `identity.authorizedRunnerNamespaces` entry | runner prerequisites; additional namespaces receive the same retained signer through scoped short-lived distribution, never an independently minted key |
| `NetworkPolicy` `logweir-identity-kubernetes-api-egress` | every identity-enabled runner namespace | excludes bootstrap from runner arbitrary-443 egress; permits DNS plus discovered/configured Kubernetes API destinations only |
| `Deployment` + `Service` `<release>-minio`, a PVC, `Secret` `<release>-minio-root`, `Secret` `logweir-s3`, `Job` `<release>-minio-seed` | `minio.enabled` | an in-cluster archive with the buckets `kafka-backups` and `logweir-evidence` |
| `StatefulSet` + two `Service`s `<release>-kafka-source` and `-target`, `Job` `<release>-kafka-seed` | `demoKafka.enabled` | two single-broker KRaft clusters; `orders` and `payments` seeded on the source, the marker topic `logweir.scratch` on the target |
| `Deployment`, `Service`, `ServiceAccount`, `ClusterRole`s, `RoleBinding` `<release>-ui` | `ui.enabled` | `kubectl proxy` serving the twenty-six UI files and the API on one origin, with its own authority (below). The files come from the image `ui.image`, not from a ConfigMap |
| `ServiceAccount` `logweir-retention`, in the release namespace **and every `identity.authorizedRunnerNamespaces` entry** | `retention.enabled` | the identity every `mode: Enforce` Job names, in every namespace that runs one. **No Role and no RoleBinding**: the retention worker makes zero Kubernetes API calls |
| `ServiceAccount`, two `ClusterRole`s, `ClusterRoleBinding`, `RoleBinding` `<release>-api` | `api.enabled` | the console/API principal's grants. **RBAC only** — no Deployment, no image, no Service |
| `ConfigMap` `<release>-api-config-<digest>` + `Deployment` `<release>-api` | `api.console.enabled` | the console itself: `logweir-api` out of the `logweir-console` image, serving `/ui/` and `/api/v1` on one origin as the principal above. `api.console.mode` is **required** — see *`api.console.mode`* below |
| `Service` `<release>-api` | `api.console.mode: shared` | the Ingress's backend. The in-cluster administrator mode binds loopback and renders **no Service at all** |
| `Ingress` `<release>-api` | `api.console.ingress.enabled` | the shared console's public entry point. **Shared mode only, TLS required, host must be `publicBaseUrl`'s authority**; refused in front of the in-cluster administrator mode |
| `NetworkPolicy` `<release>-api` | `api.console.networkPolicy.enabled` | in shared mode, ingress from the configured ingress-controller pods only; in the in-cluster administrator mode, `ingress: []` — deny. Egress in both: DNS, the Kubernetes API and the configured IdP CIDRs, and **never** a broker or object-store port |
| `Role` + `RoleBinding` `<release>-api-trusted-proxy`, in the ingress controller's namespace | `api.console.trustedProxyService` (shared mode) | `list` on `discovery.k8s.io/endpointslices` there and nothing else: the console trusts the serving endpoints of the ingress controller's Service (see *`shared` mode* below) |

Nothing optional is on by default. The release gate renders the snapshots with
the pinned bootstrap digest exactly as shipped; the rest of the default render
matches the same
controller, env, security context and RBAC rules as `logweir.yaml`
(`chart_lint_default_render_agrees_with_the_install_file`).

## The installation policy (`checks.*`, `runs.*`, `engine.*`, `evidence.*`)

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
| `runs.maxManualBackupsActivePerNamespace` | `4` | P10: manual `Backup` runs ("Back up now") holding a runner slot at once in one namespace. Over it a run is `phase: Queued` with nothing created, and starts in creation order as slots free |
| `runs.maxManualRestoresActivePerNamespace` | `2` | P10: admitted manual `Restore` runs at once in one namespace; the rest wait `Queued`, approval intact |
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

**`runs.*` bounds MANUAL runs only (P10).** Before it, nothing bounded "Back up
now": one operator's hundred accepted requests became a hundred runner pods on
one node, which hit its 110-pod limit and went `NotReady`. Scheduled, catch-up
and retry `Backup`s keep their own `concurrencyPolicy`/`maxActiveRuns` and are
neither counted nor queued, and a `RehearsalSchedule`'s `Restore`s likewise. A
queued run has no plan, no Job and no execution claim; it is
`Admitted=False/ConcurrencyLimited` with `status.queue.limit`, and the console
shows "Queued (limit N active)". A queued `Restore` still re-checks its
approval when it leaves the queue, so an authorization with a maximum age can
expire while it waits (`AuthorizationExpired`) — the deadline is shown on the
queued object. **Upgrade and rollback:** the chart renders the `runs` block
**only when a value differs from the defaults above**, because a controller
that predates it refuses the whole `policy.json` (`deny_unknown_fields`) and
fails closed. A default install therefore carries no block and survives an
image-only rollback; an install that set `runs.*` rolls the controller back
together with the chart (`helm rollback`), never on its own. A newer controller
reading a document without the block uses the defaults.
`api.console.rateLimits` is the console's half: how many runs one person may
START per namespace per minute (`10` backups, `5` restores; then `429` with
`Retry-After`).

`legacyArchiveAddressing` is not a value of its own: it is rendered from
`archive.s3.*`, the same values the Deployment's `AWS_*` env comes from and
behind the same "only when an endpoint is set" guard. An install with no
endpoint publishes an empty block rather than `allowHttp: true`. A restore
readiness check over a recovery point with no saved destination fills a region
or endpoint its plan leaves out from this block, exactly as the legacy restore
Job fills them from that env.

**`archive.url` is also where a point with no saved destination is verified.**
The controller reads an inline-archive run's evidence only through its handle
over `archive.url` (`LOGWEIR_ARCHIVE_URL`), and only in that URL's bucket; a
legacy schedule writing to another bucket, or a legacy restore plan writing its
evidence elsewhere, reads `NotAttempted` ([docs/kubernetes.md](../../docs/kubernetes.md)
§15.1a).

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

**It fences the console this chart renders.** With `api.enabled` the chart
renders the console's ServiceAccount, `<release>-api` (`logweir-api` for a
release named `logweir`), and it refuses a
`admissionPolicy.consoleServiceAccountName` that is not that account — a
subject naming nobody would install, look enabled and fence nothing. The value
is REQUIRED and non-empty: an absent, empty or null one renders a subject that
matches nobody, so the schema and the template both refuse it. (Earlier
revisions called the policy inert until D0 stage 7 landed the console; it has.)

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

Two more flags render RBAC and nothing else, so neither starts a pod and
neither is in the list above:

* **`retention.enabled`** — *let a retention enforcement Job be admitted.*
* **`api.enabled`** — *give the console/API service a reviewed identity.*

And one more starts the console itself, and needs `api.enabled`:

* **`api.console.enabled`** — *run the console in this cluster.* It needs
  `api.console.mode`, which has **no default**. Read *`api.console.enabled`*
  before turning it on, and read it before assuming the word "console" means the
  same thing as "reachable".

## `retention.enabled` — the identity a deletion needs, and the three gates it is not

It renders exactly one kind of object: the `ServiceAccount` `logweir-retention`,
with `automountServiceAccountToken: false` and **no Role, no RoleBinding and no
ClusterRole**. The controller compiles that name into every `mode: Enforce`
Job, so without it the Job's pod is admitted by nobody — fail-closed, but by
accident rather than by decision, which is why the account exists as an
explicit switch.

**In the release namespace and in every `identity.authorizedRunnerNamespaces`
entry**, because that is where the Jobs are: the controller creates an
enforcement Job in the namespace of the `RetentionPolicy` that produced it, and
a `RetentionPolicy` lives with the workload it protects. It reads that list even
when `identity.enabled` is off — the list is about where Jobs run, and only the
signer distribution beside it is about identity. For the low-level
`logweir.yaml` path there is a fragment,
`config/rbac/retention-serviceaccount.yaml`, applied once per namespace exactly
as the runner's is (`docs/install.md` step 4).

**Turning it on deletes nothing and authorises nobody to delete anything.** The
account holds no Kubernetes verb. The deletion capability lives in an
object-store credential scoped to the policy's own prefix, and the account
exists so that the pod which mounts that credential has a name an audit trail,
a NetworkPolicy selector and a `kubectl get pods` can use — separate from
`logweir-runner`, which runs backups, restores, checks, catalog syncs and
notification deliveries. "What has run as the deleter" should have an answer.

A deletion needs all four of these, and they are deliberately in four places:

1. `mode: Enforce` on a `RetentionPolicy`, which only `logweir-retention-admin`
   may write;
2. this ServiceAccount in that namespace;
3. an object-store credential whose scope is the policy's prefix and never
   `logweir/` — **this is the hard boundary**;
4. an administrator's `approve-plan` carrying the current `planSha256`, per run.

There is no `retentionImage`: the enforcement binary ships in `runnerImage`
beside the `logweir` entrypoint, and the controller overrides only the
container's command.

[`docs/stability.md`](../../docs/stability.md), "`RetentionPolicy` in `Enforce`
is where the deletion boundary moves", states the residual plainly: enabling
enforcement does not give the controller a delete verb, it turns the
controller's existing authority to create a Job into a deletion capability in
the namespace that holds the credential.

## `api.enabled` — the console/API principal, RBAC only

| object | name |
|---|---|
| `ServiceAccount` | `<release>-api`, in the release namespace |
| `ClusterRole` + one `RoleBinding` per `api.namespaces` entry | `<release>-api` |
| `ClusterRole` + `ClusterRoleBinding` | `<release>-api-trustpolicies` |
| `ClusterRole` + `ClusterRoleBinding` | `<release>-api-trustroster` |

**No Deployment, no image, no Service and no Ingress.** The console's image and
its deployment are a separate piece of work; this flag exists so that an
installation running the product API has a reviewed identity to run it as
instead of inventing one, and so that the admission policy below has a subject
that really exists.

**It is wider than `<release>-ui`, and the two are not comparable.** The proxy
acts for *whoever reaches its Service*, so its role is measured from the page's
own request sites and is as small as the page. This account acts for a
*service* that authenticates every request and resolves the actor's roles
itself, so its role is the union of what every route may need and the per-actor
narrowing happens above it. Copying either argument onto the other is how a
console ends up with a page's authority or a page ends up with a console's.

What it holds: `get`/`list` on the eight product kinds; `create` on all eight
(`approvals` since PLAT-19.2 — the console's signed ordinary confirmation, a
governed request's confirmation object and the approver's countersigned
Approval); `patch` on `backupschedules`, `backupdestinations`,
`topicdiscoveries` and `preflights`; `get`/`list` on `protectionpolicies`,
`recoverycatalogs`, `rehearsalschedules` and `retentionpolicies`; `create` on
`recoverycatalogs` for "connect existing archive"; `get`/`list` on the
cluster-scoped `trustpolicies`; `get` on the one cluster-scoped `trustrosters/default`
(`resourceNames: ["default"]`, so a readiness verdict's roster referent can be
compared rather than reported `unverifiable`); `get` on `configmaps`; `create` on
`secrets`.

What it does not, each for a reason: **no `watch`** (the service's adapter has
no watch method, and the operation event stream is server-sent events over its
own reads), **no `delete`**, **no read verb on `secrets`** — that missing verb
is what makes a console-written credential write-only — **no `list` on
`configmaps`**, **no write verb on `trustpolicies`**, and **no `list` on
`trustrosters`** nor a `get` on any roster but `default`.

Set `admissionPolicy.consoleServiceAccountName` to this account: the fence's
whole effect is its subject list, and a subject that names nobody is a policy
that installs, reads as enabled and fences nothing — leaving the console's
`create` on Secrets, which RBAC cannot narrow by shape, with no bound at all.
Both names now come from one template helper, and **the render REFUSES** when
`api.enabled` is on and the two disagree, naming both strings. Under the default
release name they are both `logweir-api`; under `helm install myrel …` they are
both `myrel-api` once you set the value, and neither can be silently wrong.
(An installation fencing a console deployed out of band leaves `api.enabled`
off, and the refusal does not apply.)

`./scripts/render-install.sh --check` answers the `kubectl auth can-i` question
for every pair above, in both directions, against every checked-in render. It
finds the console's roles through the bindings that name its ServiceAccount,
whatever the roles are called, and refuses a binding that names anyone else
beside the account.

## `api.console.enabled` — the console itself, and the two names it goes by

**The name, first, because two are in use and both are correct.** The decision
document that specified this service (`docs/to-do/decisions/D0-product-api-and-identity.md`)
calls it the *console* and gives its deployment worker `Dockerfile.console` and
`console.*` values. The crate, the binary, the image's entrypoint and the
already-shipped RBAC values block call it the *API*: `crates/logweir-api`,
`logweir-api`, `api.enabled`. Rather than rename a landed values key, a landed
RBAC template and `admissionPolicy.consoleServiceAccountName` for a word, the
chart nests one inside the other:

| what | called |
|---|---|
| the values block, the ServiceAccount, the Service, the Deployment | `api.*`, `<release>-api` |
| the image and its Dockerfile | `logweir-console`, `Dockerfile.console` |
| the workload's own values | `api.console.*` — D0's word, under the chart's |

So `api.console.image` names `logweir-console` and the pod runs as
`<release>-api`, and that is the whole of the discrepancy.

### Two switches, because D0's rollout has two steps

`api.enabled` renders the principal and **starts nothing**: an account you can
interrogate with `kubectl auth can-i` before anything runs as it. That state is
deliberate and is D0's own first step ("deploy console disabled; configure …
OIDC, exact role/namespace bindings, TLS ingress … enable read-only console").
`api.console.enabled` runs the workload as that principal, and **requires**
`api.enabled` — the render refuses the pair, naming both flags, rather than
quietly producing nothing.

### `api.console.mode` has no default, and the two modes are two shapes

**There is no default, here or in the binary.** `crates/logweir-api/src/config.rs`
refuses a configuration file that forgets to name a mode rather than reading it
as the more permissive one, and this chart refuses the same way:
`api.console.enabled: true` with no `api.console.mode` is a render-time failure
naming the field. Name one.

#### `localAdmin` — the **in-cluster administrator mode**

`logweir-api` **refuses any non-loopback listener** in this mode, so the pod
binds `127.0.0.1`. The chart then renders nothing to go with it that anything in
the cluster could dial:

* **no Service.** `kubectl port-forward` takes a Deployment directly, so one is
  not needed — and a Service in front of a loopback listener would advertise a
  ready endpoint (there is no readiness probe; see below) while refusing every
  connection: a security property to a reader of the template, and an outage to
  every monitor in the cluster.
* **no Ingress** — the render refuses one in front of this mode by name.
* **no ingress rule in the NetworkPolicy**: `ingress: []`, which with `Ingress`
  in `policyTypes` is deny.
* the identity is the `<release>-api` ServiceAccount, narrowly bound by
  `templates/ui/api-rbac.yaml`, and never a kubeconfig.
* **readiness is not gated on OIDC**, because there is no OIDC.

It exists for isolated labs and break-glass administration. It does not
expose Ordinary confirmation and it is **not** a shared console.

**`shared` is the only mode that may be exposed through a Service or an
Ingress.**

That is the difference between this component and the legacy proxy below.
`ui.enabled` renders a `kubectl proxy` where **anyone who can reach that Service
acts with that ServiceAccount's authority**. Turning `api.console.enabled` on
cannot produce that in either mode: the administrator mode has no Service to
reach, and `shared` mode authenticates and authorizes every request before it
reaches a route.

```console
$ head -c 32 /dev/urandom > cursor.key
$ kubectl --context docker-desktop -n logweir-system \
    create secret generic logweir-console-keys --from-file=cursor.key
$ rm cursor.key
$ helm install logweir charts/logweir -n logweir-system \
    -f charts/logweir/examples/console.values.yaml
$ kubectl --context docker-desktop -n logweir-system \
    port-forward deploy/logweir-api 8484:8484
$ open http://127.0.0.1:8484/ui/
```

The local port must be `8484` too: the configuration's `publicOrigin` carries
the listen port and `logweir-api` refuses a mismatch, because an origin that
does not match the one the browser sends is a CSRF check that cannot pass.

**`create pods/portforward` in this namespace is equivalent to full console
administrator authority over every bound namespace — grant it as you would grant
that.** This mode has no identity provider and no product role check: every
request is attributed to the one configured `localAdminSubject`, so that
Kubernetes verb *is* the authorization boundary, and it is the whole of it.

**There are no probes in this mode, and that is a consequence rather than an
omission.** A kubelet HTTP probe is made from the node's network namespace
against the *Pod IP*; a loopback-only listener refuses it, so a readiness probe
would hold a working console permanently NotReady and a liveness probe would
restart it forever. (`httpGet.host: 127.0.0.1` is not the escape it looks like —
that is the node's loopback, not the pod's.) What you lose is the signal: a
configuration the binary refuses shows up as `CrashLoopBackOff` with an exit
code of 2 and a one-line message naming the field, rather than as a NotReady
endpoint. `kubectl logs` is where you read it. In `shared` mode the listener is
on the Pod IP and both probes are rendered: `/healthz` for liveness (process
liveness only, consults nothing) and `/readyz` for readiness.

### The key Secret is yours to create, and the chart will not invent one

`api.console.keySecret` is required and names a Secret in the release namespace:

| key | mode | what it is |
|---|---|---|
| `cursor.key` | both | the pagination-cursor MAC key, at least 32 bytes. In `localAdmin` mode it is raw bytes; in `shared` mode it is two lines, `version:` and `key:` |
| `session.key` | `shared` | the session-cookie key, same two-line form |

`api.console.keyVersion` must equal the `version:` those files declare. Bumping
both is a deliberate rotation that ends every live session; a file that changes
while the number does not is a key someone rewrote underneath the service, and
startup refuses it (exit 2, naming the field).

**This chart generates neither.** A Helm-generated key changes on every render —
which would make the checked-in rendered files unstable — and is unrecoverable
on upgrade, and D0 requires these to be persistent Secrets created or adopted
explicitly. The same applies to `api.console.oidc.clientSecret`, which is the
**name** of a Secret holding the client secret under the key `clientSecret`, and
to `api.console.ingress.tlsSecretName`.

**Where this Secret sits relative to the controller's authority.** It lives in
the release namespace. Residual **O1** (`docs/kubernetes.md` §15.4: *"Job CRUD
in a namespace that holds `logweir-signing-key` is equivalent to holding that
key, because a Job the controller creates can mount it"*) applies to ANY Secret
in a namespace where `weirkeeper` may create Jobs. Under the default
`templates/clusterrolebinding.yaml` that is every namespace, the release
namespace included — which is fine for the in-cluster administrator mode, whose
authority is the port-forward permission anyway, and is why **`shared` mode
renders only with `controller.watchNamespaces`** (D0 stage 5,
§`controller.watchNamespaces` below): the controller then holds Job-create authority in the listed execution
namespaces and nowhere else, and the chart refuses a list that includes the
release namespace.

**No credential is ever in the ConfigMap.** The rendered configuration carries
*paths* — `oidc.clientSecretFile`, `sessionKey.file`, `cursorKey.file` — into
read-only Secret mounts under `/var/run/logweir/`, and
`chart_lint_the_console_config_map_carries_no_credential` walks every rendered
console ConfigMap refusing a value under any credential-shaped key. A ConfigMap
is readable by anything with `get configmaps` in the namespace, it is in
`helm get manifest`, and it is checked into `charts/logweir/rendered/`.

### `shared` mode, and what it refuses before it installs

`examples/console-shared.values.yaml` is the whole shape. What the chart will
not let you install, each refused at render time with the field named:

| refused | why |
|---|---|
| `publicBaseUrl` that is not `https://` | TLS at the shared entry point is required, not recommended. `values.schema.json` types this too, so `--set` is refused before a template runs |
| `publicBaseUrl` with a path, trailing slash or userinfo | the OIDC redirect URI is this value plus `/auth/callback` |
| `ingress.enabled` with no `tlsSecretName` | a `__Host-`/`Secure` session cookie is never sent over plain HTTP, so it would be a console that cannot log anyone in — after publishing it |
| `ingress.enabled` in `localAdmin` mode | an Ingress in front of a mode that authenticates nobody is an unauthenticated shared console |
| a `roles.bindings` namespace outside the console's bound set | a product role for a namespace the ServiceAccount cannot read is a role that grants a 403 |
| `*` or `?` in a binding | bindings are EXACT strings; a wildcard is refused by name rather than silently matching nothing |
| `networkPolicy.enabled` without both ingress-controller selectors | an ingress rule with no `from` admits nothing; one with an empty pod selector admits the whole namespace |
| `api.console.enabled` without `api.enabled` | a pod with no grants, which 403s on every route |
| `api.console.enabled` with no `api.console.mode` | there is no default, here or in the binary; a mode read by fall-through is the more permissive one nobody chose |
| `ingress.host` that is not `publicBaseUrl`'s authority | the redirect URI is `publicBaseUrl` + `/auth/callback` and the service answers `421 misdirected_request` to any other `Host`, so a mismatch publishes a console every browser is refused by |
| `shared` without `controller.watchNamespaces` | D0 stage 5: with the cluster-wide binding the controller may create a Job in the release namespace that mounts the console's keys (O1) |
| `controller.watchNamespaces` containing the release namespace | the same authority, put back by name |
| `shared` with `ui.enabled` | the legacy `kubectl proxy` Service is a second, unauthenticated way to the same objects (PLAT-17.2: remove or isolate the legacy proxy) |
| a `roles.bindings` namespace outside `controller.watchNamespaces` | the console would create objects no controller reconciles |
| `requireTrustedProxy` with neither `trustedProxyService` nor `trustedProxyCidrs`, or in `localAdmin` mode | a gate with nothing to trust refuses every request; a loopback listener has no proxy in front of it |
| `trustedProxyService` with only one of `namespace`/`name`, or in `localAdmin` mode | half a Service names nothing; a loopback listener has no proxy, and no Role in the ingress namespace should be granted for one |
| `oidc.caBundle` naming both a ConfigMap and a Secret, or with an empty `key` | the bundle is one object and one file |
| `oidc.systemRoots: false` without `oidc.caBundle` | the console would trust no certificate and never reach its issuer |
| a `hostAliases` entry without an `ip` and a hostname, or with a wildcard | `/etc/hosts` has no wildcard; an entry that resolves nothing is a typo |
| a `networkPolicy.oidcPeers` entry without `namespace`, `podLabels` and `port` | an empty selector would allow egress to every pod in the namespace |
| `requireTrustedProxy` with a `trustedProxyCidrs` range wider than `/16` (IPv4) or `/48` (IPv6) | a range that wide contains the pods the gate exists to refuse |

**`requireTrustedProxy: true`** makes the console answer `421` to every request
(the two probes excepted) whose socket peer is not a trusted proxy, or that the
ingress did not mark `X-Forwarded-Proto: https`. It can only refuse; identity
stays the OIDC session. It is defence in depth; an enforcing NetworkPolicy
remains the network boundary. There are two ways to say who the proxy is:

- **`trustedProxyService: {namespace, name}` — the ingress controller's
  Service, and the recommended one.** The console lists that Service's
  `EndpointSlice`s every five seconds and trusts each **serving** endpoint
  address as a single host: the ingress pods of the moment, and no other pod.
  An ingress pod recreated on a new address is trusted after one refresh and
  its old address distrusted at the same moment, so nothing has to be re-read
  or patched after an ingress restart (chart gap G6). A refresh that fails keeps
  the last complete set for at most thirty seconds; past that the console
  trusts nobody through the Service, answers `421` and reports NotReady — a
  visible outage, never a silent stale grant. The chart renders one `Role` and
  `RoleBinding` in **that** namespace granting `list` on
  `discovery.k8s.io/endpointslices` and nothing else. **Who can add an
  address:** whoever can edit that Service (its selector) or write an
  `EndpointSlice` there, and whoever can create or relabel a Pod — or a
  Deployment — that matches the Service's selector in that namespace, because
  the EndpointSlice controller then lists it. All of them are the ingress
  namespace's own administrators, who already terminate the console's TLS.
  **An ingress controller on `hostNetwork`** publishes the NODE's address as its
  endpoint: the console then trusts every hostNetwork pod and node process on
  that node, and anything the CNI masquerades to the node address — not "the
  ingress pods and no other pod". For such a controller, either accept node
  trust knowingly or decide a `trustedProxyCidrs` `/32` knowingly. The Service's
  endpoints must be the ingress controller's own pods (the Traefik chart's
  `traefik` Service is; an admission-webhook Service that selects the same pods
  is equivalent).
- **`trustedProxyCidrs`** — static ranges. It tells the ingress from a pod that
  dialled the Service directly **only when the range is the ingress
  controller's own pod range and contains no other pod** (kube-proxy keeps the
  source pod IP through a ClusterIP), so the chart and the binary refuse a range
  wider than `/16` (IPv4) or `/48` (IPv6) when the gate is on. A dedicated
  ingress node pool's pod ranges satisfy it; a single-node cluster's pod range
  does not (it contains every pod). The example ships the placeholder
  `192.0.2.0/24` beside the Service.

Both may be set; a peer trusted by either is trusted.

### The identity provider inside the cluster: a private CA, a name, a path

Three values exist for an issuer the console cannot reach with the defaults —
an IdP whose certificate a private CA issued, or one whose public name does not
resolve to a reachable address from inside the cluster (split-horizon DNS, a
laptop cluster where `*.localtest.me` is `127.0.0.1`, a Dex behind the
cluster's own ingress). None of them changes what the console validates.

- **`oidc.caBundle: {configMap | secret, key}`** (chart gap G1) mounts the
  issuer CA's **public** certificates from one ConfigMap or Secret in the
  release namespace — never inline PEM, so no certificate text is in a values
  file, a rendered manifest or Helm's release history — and the console trusts
  them **in addition to** the system roots. `oidc.systemRoots: false` drops the
  system roots and is refused without a bundle. The object must exist before the
  pod starts (it is not `optional`); an unreadable or empty bundle, or one that
  carries a private key, stops `logweir-api` at exit 2. The certificate chain,
  its validity and the host name in the issuer URL are verified exactly as for a
  public CA.
- **`hostAliases: [{ip, hostnames}]`** (chart gap G2) adds pod `/etc/hosts`
  entries, e.g. the issuer's public name mapped to the IdP's own in-cluster
  Service. **Why this and not a separate back-channel URL.** A
  back-channel discovery/JWKS URL would either have to rewrite every endpoint
  the discovery document names — including the token endpoint, which receives
  the client secret — onto another host, or verify the provider's TLS
  certificate against a name the browser never uses: a second trust decision
  the operator would have to get right. A host alias changes only where the
  name resolves. The URL, the TLS name check, the discovery document's
  `issuer` and the ID token's `iss` are all still compared with the one
  configured `oidc.issuer`, exactly. **But that holds only when the alias
  target is an endpoint that only the IdP's owner can route.** Map the name to
  the IdP's OWN Service, with TLS terminated by the IdP itself — never to a
  shared ingress controller. A shared ingress serves Ingresses from other
  namespaces too: anyone who may create one there can claim the issuer's host
  (longer path rules outrank the owner's) and answer discovery, JWKS and the
  token request — which carries the client secret — behind the owner's own
  certificate, and the console then believes tokens they signed. The CA in
  `oidc.caBundle` must likewise not issue the issuer's name to anyone who could
  be on that path. With both conditions met, an alias pointing somewhere wrong
  is a handshake that fails; without them it is not. A host alias is for a
  cluster whose DNS does not resolve the issuer (a laptop cluster, split-horizon
  DNS); a production IdP is resolved through real DNS and needs none. A
  ClusterIP is stable for the Service's lifetime; recreate the Service and the
  alias must follow.
- **`networkPolicy.oidcPeers: [{namespace, podLabels, port}]`** allows the
  console's egress to an in-cluster provider path by selector. An enforcing CNI
  matches egress **after** a Service's DNAT, against the backend pod and its
  port, so an `oidcCIDRs` entry for a ClusterIP matches nothing there; name the
  pods instead (for the PoC's Dex, which serves the console's TLS itself:
  namespace `dex`, its pod labels, port `5554`).

**Every object the console creates is attributed.** The rendered configuration
names `kubernetes.principal: system:serviceaccount:<namespace>:<release>-api`,
and the console stamps it — with the actor, how they authenticated, the product
action, the binding revision and, for a restore, the selected recovery point —
onto every CR and credential Secret it creates (`docs/api.md` §*The audit
record*), so a Kubernetes audit entry for `<release>-api` and a Logweir audit
line join on `api.logweir.dev/request-id`.
`replicas` above 1 also renders a `PodDisruptionBudget` with
`maxUnavailable: 1`. At one replica it renders none, deliberately: a budget over
a single pod makes `kubectl drain` block forever on the node carrying it.

### What the NetworkPolicy does and does not prove

It allows ingress **only** from the configured ingress-controller pods on the
console port, and egress to DNS, the Kubernetes API (the `kubernetes.default`
ClusterIP, its visible endpoints and `identity.kubernetesApiCIDRs`) and the
`networkPolicy.oidcCIDRs` on 443 and the `networkPolicy.oidcPeers` pods on
their ports. It lists **no** broker port and **no**
object-store port, which is the structural half of D0's "does not dial Kafka or
object storage" — the other half is that `logweir-api` links no Kafka client and
no object-store client at all.

NetworkPolicy has no FQDN concept, so an identity provider behind a rotating
address cannot be expressed: `oidcCIDRs` must be its actual endpoints. Left
empty under an enforcing CNI, this policy blocks OIDC discovery and the console
starts NotReady — the honest failure, and not a reason to widen the rule.

And Docker Desktop commonly has **no enforcing CNI**, so a local install proves
these objects are well-formed and nothing whatever about deny behaviour. D0 says
so by name. Production support needs the chosen CNI's own evidence.

### `api.console.rateLimits` — how fast one person can start runs (P10)

| value | default | what it decides |
|---|---|---|
| `api.console.rateLimits.manualBackupsPerMinute` | `10` | `POST …/backups` ("Back up now") per person, per namespace, per minute; then `429 rate_limited` with `Retry-After` |
| `api.console.rateLimits.manualRestoresPerMinute` | `5` | `POST …/restores`, the same way |

Keyed by the stable `issuer#subject` id, so two people are two windows and one
person in two namespaces is two windows. The window is per console **process**:
`replicas: 2` permits twice the rate. It bounds how fast runs are QUEUED; how
many RUN at once is the controller's `runs.*` pool, which holds whatever this
lets through. `logweir-api` refuses `0` and anything above `600` at start. The
chart renders `rateLimits` into the configuration **only when a value differs
from these defaults**, because a console image that predates the key refuses
the whole file at start (exit 2): a default install survives an image-only
rollback, and one that set `api.console.rateLimits.*` rolls the console image
back with the chart.

### Upgrade, rollback, and what an existing installation sees

An installation that is not setting `api.console.enabled` sees **no change at
all**: the flag defaults to false, `api.enabled` keeps meaning exactly what it
meant before (an account and its grants, no pod), and the default and demo
renders are unchanged. Nothing converts.

Turning it on is additive — a ConfigMap, a Deployment and a Service — and
turning it off removes those three and leaves the principal, its grants and
every Logweir custom resource untouched. The console creates and reads objects;
it executes nothing, so removing it stops no backup, cancels no restore and
loses no evidence. Rolling back to a chart version without these templates is
the same operation performed by Helm.

The configuration ConfigMap is **immutable and content-addressed**: its name
carries the digest of the document, so an upgrade that changes one role binding
creates a new object and rolls the pod onto it, and nobody with `patch
configmaps` can change the role table under a running console. Going the other
way — from `shared` back to `localAdmin` — ends every live session, because the
listener moves to loopback and the session cookie's origin no longer exists;
plan it as a withdrawal of access rather than as a setting change.

## `controller.watchNamespaces` — the controller's authority, scoped (D0 stage 5)

| value | default | what it decides |
|---|---|---|
| `controller.watchNamespaces` | `[]` | the execution namespaces the controller watches and creates Jobs in; `[]` is every namespace |

**Why it exists.** `weirkeeper` creates Jobs, and a Job can mount any Secret in
its namespace, so Job-create authority in a namespace is every Secret there —
residual **O1**. The default install binds the `weirkeeper` ClusterRole with a
cluster-wide `ClusterRoleBinding`, so that authority covers every namespace.
Kubernetes RBAC is additive; nothing can subtract a namespace from a
ClusterRoleBinding, so the only narrowing is not to grant it.

**What a list renders instead** (`templates/controller-scope.yaml`):

* no `ClusterRoleBinding/weirkeeper`;
* one `RoleBinding/weirkeeper` per listed namespace, to the **unchanged**
  `weirkeeper` ClusterRole — under a RoleBinding its namespaced rules apply in
  that namespace only;
* `ClusterRole/weirkeeper-cluster-scope`, bound cluster-wide, holding the two
  cluster-scoped trust kinds (`trustrosters`, `trustpolicies` and their status)
  and nothing namespaced;
* `Role/weirkeeper-installation-policy` in the release namespace: `get` on the
  `weirkeeper-policy` ConfigMap by name, and nothing else;
* `LOGWEIR_WATCH_NAMESPACES` on the controller, from the same list, so every
  namespaced reconciler runs one `Api::namespaced` watch per listed namespace
  and every cross-object read (the retention protection set, the check ceiling)
  reads those namespaces only. A name that is not a DNS label is refused by the
  schema, by the template and by the controller at startup.

**What an administrator can ask Kubernetes before anything runs** (`auth can-i`
as `system:serviceaccount:<release-namespace>:weirkeeper`, scoped to `team-a`):

| question | answer |
|---|---|
| `create jobs -n team-a`, `list pods -n team-a`, `get pods/log -n team-a`, `create configmaps -n team-a` | yes |
| `watch backups -n team-a` | yes |
| `create jobs -n <release-namespace>`, `list jobs -n <release-namespace>` | **no** |
| `create jobs -n team-z` (not listed), `watch backups --all-namespaces` | **no** |
| `get configmaps/weirkeeper-policy -n <release-namespace>` | yes |
| `get configmaps/<any other> -n <release-namespace>`, `get secrets` anywhere | **no** |
| `list trustpolicies`, `get trustrosters` (cluster-scoped) | yes |

**Required for `api.console.mode: shared`**, which the chart refuses without a
list or with one that names the release namespace: the console's session and
cursor keys live there.

**Migration.** Existing installs that do not run a shared console see no
change: the default is `[]` and renders byte-identically. **An existing
`api.console.mode: shared` release does not render any more until it is scoped**
— `helm upgrade` refuses at render time, naming the field, and applies nothing
(the running console keeps serving). It meets, in order: no
`controller.watchNamespaces`; the release namespace in that list; `ui.enabled`;
a `roles.bindings` namespace outside the list; and, with `requireTrustedProxy`,
a `trustedProxyCidrs` range wider than `/16`. The why is residual O1 above, and
`docs/install.md` §5e *Upgrading a shared console installed before this release*
has the table of fixes and the rollback. To scope an existing install: list every namespace that holds
Logweir objects (`kubectl get backups,restores,backupschedules,kafkaclusters -A`),
make sure each has the runner ServiceAccount and the signing identity
(`docs/install.md` step 4, `identity.authorizedRunnerNamespaces`), set the list
and upgrade. Objects in a namespace left off the list are **not reconciled** —
they keep their status and evidence, and resume when the namespace is added. If
Backups or Restores currently run in the release namespace, move them before
enabling a shared console there. Rollback is `watchNamespaces: []`, which
restores the cluster-wide binding; the controller restarts either way because
its environment changes. Adding a namespace later is an upgrade (a new
RoleBinding and a controller restart), not a cluster-admin grant.

**What it does not change.** The execution namespaces still carry O1 for their
own signing key — moving the signer out of the controller's reach is not this
setting. Each listed namespace costs one watch per namespaced kind (twelve, plus
their owned Jobs), which is the price of not holding a cluster-wide list.

## `approvalPolicy` — ordinary confirmation and governed approval (PLAT-19.2)

| value | meaning |
|---|---|
| `approvalPolicy.policies` | named policies: `name`, `mode: Governed\|Ordinary`, `maxAgeSeconds` (60..604800; default 900 Ordinary, 86400 Governed), `requireDistinctPrincipal` (Governed only, must be true) |
| `approvalPolicy.namespaces` | `{<namespace>: <policy>}`; an unbound namespace keeps today's governed approval (`legacy-governed-v1`) |
| `approvalPolicy.allowOrdinaryConfirmation` | D0's installation floor, default `false`; an Ordinary policy is refused at render and at start without it |
| `approvalPolicy.confirmationKeySecret` | the console's `ConsoleConfirmation` private key, a Secret with key `confirmation.key`; required when a console-served namespace is bound |

**Nothing renders when nothing is set**, so an existing installation sees no
change. When set, the chart renders one **immutable, content-addressed**
ConfigMap `<release>-approval-policy-<digest>` and mounts the same object into
the `weirkeeper` Deployment (`LOGWEIR_APPROVAL_POLICY_FILE`) and into the
console (`approvalPolicyFile` in its config), and mounts the key Secret into the
console only (`confirmationKeyFile`). Both processes log the document's digest
at start and refuse to start on a document that does not validate — and a
controller that refuses to start stops every reconciler, backups included — so
**the chart refuses at render every document the binary refuses**: the
`requireDistinctPrincipal` rules, the `maxAgeSeconds` range, unknown policy
fields, namespace keys that are not DNS labels, and the rest. The cases are
listed once in `scripts/approval-policy-refusals/`, which `scripts/check-chart.sh`
renders and `crates/logweir/tests/chart_lint.rs` feeds to the binary's parser.
`requireDistinctPrincipal` compares the approver key's `principal.id`, which must
be `<issuer>#<subject>`; any other form is refused under Governed. Editing a
policy renames the ConfigMap and rolls both Deployments — the installation-admin
rollout D0 asks for; a Restore confirmed under the old policy and not yet
admitted is refused `ApprovalPolicyMismatch` and is submitted again. Rolling
back (removing the values) returns every namespace to legacy governed approval,
and an older controller refuses every document the new console signed: both
directions fail closed. The keys this needs on each namespace's `TrustPolicy`
are in `docs/keys.md`; the example is `examples/approval-policy.values.yaml`,
rendered to `rendered/approval-policy.yaml`.

## `controller.failFastSeconds` and `controller.jobTtlSeconds`

| value | default | what it decides |
|---|---|---|
| `controller.failFastSeconds` | `""` (300 s) | how long a non-transient diagnostic may hold before the Job is cancelled |
| `controller.jobTtlSeconds` | `""` (604800 s) | how long a finished Job is kept |

Both ship as the **empty string**, which means *this controller build's own
default* and renders no environment variable at all — so a default install is
byte-identical to what it was before these existed.

`failFastSeconds: 0` is not "no patience", it is **never fail fast**: every Job
runs to its own `activeDeadlineSeconds`. It is the lever for a cluster where an
external controller materialises a Secret a few minutes behind the Job, where a
healthy run would otherwise be cancelled at 300 s.

Values below the build's floors (60 s and 3600 s) are clamped **up** by the
controller, which logs what it used; an unparseable value is the default. The
chart does not refuse either, because a typo should not fail the upgrade of a
whole release. A job TTL below an hour would mean an operator cannot fetch the
pod log of a run that failed overnight, which is why that floor is where it is.

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

**How small those archive credentials may be is measured, not guessed.**
[`docs/kubernetes.md`](../../docs/kubernetes.md) §7a carries the per-role
minimal S3 action set with its resource scope — one bisected row per action,
each naming the live harness row that proved the role's own operation fails
without it. A wider grant than that table is not required by anything this
chart installs.

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

The rendered objects go in **`kubernetes.connectionsNamespace`** (default: the
release namespace) with the chart's labels, and the probe Job runs under the
`logweir-runner` ServiceAccount there. `secretRef` must be in that same
namespace. With `controller.watchNamespaces` set, the chart **refuses to
render** unless `kubernetes.connectionsNamespace` is one of the watched
namespaces (chart gap G3): the default, the release namespace, is one a shared
console's scoped controller may not watch, and a `KafkaCluster` there would be
one no controller reads and no Backup can use. The same value places
`minio.enabled`'s `logweir-s3` archive credential, which a Backup's
`secretRef` names in its own namespace. The namespace must exist before the
install, like every execution namespace. Changing the value on an upgrade moves
the objects: Helm deletes them from the old namespace and creates them in the
new one, where it is probed afresh; Backups in the old namespace lose the
objects they named, so move them before or with the value.

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
the chart renders (the fourteen CRDs excepted — Helm copies `crds/` verbatim and
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
2026-09-12 — the command is below) with the **twenty-six shipped UI files copied
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
kubectl apply --server-side --force-conflicts -f charts/logweir/crds/
kubectl diff --server-side --force-conflicts -f charts/logweir/crds/   # prints nothing
helm upgrade logweir charts/logweir -n logweir-system
```

`--force-conflicts` because Helm created these CRDs and owns their fields:
without it every CRD that already exists is refused (`conflicts with "helm"`)
and keeps its old schema, while an `Established` wait still passes.

`charts/logweir/crds/*.yaml` is byte-identical to `config/crd/*.yaml`
(`scripts/check-chart.sh` compares them with `cmp`), so applying either
directory is the same act.

## Uninstall, and what it leaves behind

```bash
helm uninstall logweir -n logweir-system
```

removes everything the release created **except**: the fourteen CRDs (Helm never
deletes `crds/`; `kubectl delete crd <name>` removes each and every custom
resource stored under it), the
namespace `--create-namespace` made, the cluster-scoped `TrustRoster`, retained
`Secret/logweir-signing-key` in the release and authorized runner namespaces,
retained `ConfigMap/logweir-signing-trust`, the authority-free retained
`ClusterRole/logweir-identity-singleton`, and any RoleBinding you created by
hand. Preserve those identity objects for same-installation recovery and old
archive verification; do not delete the singleton marker merely to install a
second independent signer. **The demo MinIO's `PersistentVolumeClaim` is NOT
kept:** it is an ordinary release object, so `helm uninstall` deletes it and,
under a `Delete` reclaim policy (docker-desktop's `hostpath`), the demo archive
with it — copy anything you need out of the bucket first (seen live by the PoC
install, 2026-09-24). And, as with `kubectl delete -f
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
  twenty-six files the `logweir-ui` image serves, sha256 for sha256 against
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
