# Running Logweir on Kubernetes

Operational reference for the CLI CronJob and the `weirkeeper` controller.
Start with [install.md](install.md) for deployment or [quickstart.md](quickstart.md)
for demos. The [CronJob example](../examples/cronjob-drill.yaml) schedules the CLI.
Numbered sections remain stable for existing source and documentation references.
Historical test results below describe the named environment and date, not a
new validation of this checkout.

## 1. Reading a drill result

Kubernetes displays every non-zero container exit as `Error`; Job status does
not carry the runner's exit code. The [exit-code contract](stability.md) is:

| Code | Meaning |
|---|---|
| 0 | Pass. |
| 1 | Operational failure; no artifact. |
| 2 | A measured result that did not pass; a scorecard was signed. |
| 3 | A guard refused the run. |
| 4 | Signing or lock proof failed; nothing was uploaded. |

Use `backoffLimit: 0` and `restartPolicy: Never`. Retrying a legitimate exit 2
repeats the restore; `OnFailure` can also lose the pod that carries its code.
Read the named container: the CronJob example calls it `logweir`, while
controller-created Jobs call it `runner`.

```bash
# The standalone CronJob:
kubectl --context docker-desktop get pod -l job-name=<job> \
  -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="logweir")].state.terminated.exitCode}'
# A controller-created Backup or Restore Job:
kubectl --context docker-desktop get pod -l job-name=<job> \
  -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="runner")].state.terminated.exitCode}'
```

The controller copies this value to `Backup.status.exitCode` or
`Restore.status.exitCode` (§10 and §12), before enabling Job cleanup.
Controller-built Jobs also use `podFailurePolicy: FailJob` for disruption and
exits 2, 3 and 4. A policy match was observed on docker-desktop v1.34.1 on
2026-09-10. Its independent effect on retries with a positive `backoffLimit`
has not been tested; the shipped limit remains zero.

### 1b. Correlating logs, metrics and evidence

`drill run` writes JSON logs to stdout, at `info` by default. Logweir events
carry `fields.run_id` or `span.run_id`; the same id appears in the scorecard
and the metrics file's leading comment. Every terminal path logs
`drill finished`, with `fields.exit_code` and `fields.meaning`.

A non-blank `RUST_LOG` overrides the default. Dependency logging enabled by
that override may lack the run id. The CronJob example explicitly sets `info`.
Save logs without replacing the drill's exit status with a pipeline's status:

```bash
kubectl --context docker-desktop logs job/<job> > drill.log
jq -r 'select(.level=="ERROR") | .fields.run_id' drill.log
```

`jq` runs on the operator's machine; it is not in the runtime image.

## 2. Image architecture

The pinned engine and runner are `linux/amd64`. The controller must be built
natively for its target architecture; `Dockerfile.weirkeeper` refuses
cross-architecture builds because its `aws-lc-sys` build needs native headers.
See [install.md](install.md) for the build and registry paths.

Docker Desktop's local image store allowed the amd64 runner on the author's
arm64 host after a host-side pull. This does not generalize to an arm64 `kind`
node: its CRI image service did not expose the loaded amd64 image. Use amd64
runner nodes for a deployment; the chart's node-placement values do not yet
propagate to controller-created Jobs.

For local images, `imagePullPolicy: Never` requires the exact reference to be
loaded on the node. `ErrImageNeverPull` can mean a missing reference or an
architecture mismatch. Image pruning can remove that prerequisite. Do not add
an amd64 node selector to a cluster with no amd64 node.

## 3. Permissions and mounts

Runner Jobs use `logweir-runner`, with `automountServiceAccountToken: false`.
The runner makes no Kubernetes API calls and needs no Role or RoleBinding.
Create the account in each runner namespace as described in [install.md](install.md).

The non-root container runs as uid/gid 65532. Projected signing Secrets need
`fsGroup: 65532`, with `defaultMode: 0440`. The original docker-desktop probe
on 2026-09-05 measured:

| Requested mode | fsGroup | Mounted owner/mode | Readable by uid 65532 |
|---|---|---|---|
| 0400 | absent | root:root 0400 | No |
| 0440 | absent | root:root 0440 | No |
| 0400 | 65532 | root:65532 0440 | Yes |
| 0440 | 65532 | root:65532 0440 | Yes |

Changing the mode alone does not change group ownership. ConfigMaps use 0644
and are readable without this group; credentials must remain Secrets.

The restore approval bundle mounts at `/approval`: `approval.json`,
`approval.sig`, `approver.pub.pem`, and `allowed-clusters.json`. Keeping the
allowlist in a Secret prevents a subject with only `patch configmaps` from
widening a restore's target set. This does not constrain a cluster-admin.
Use repeatable `--approver-key-ids` flags to restrict the accepted approver
keys; the operator supplies the unexpired roster ids.

`emptyDir` is suitable for `/work` and the engine checkpoint. It is lost with
the pod and cannot provide resumable recovery or persistent metrics. No PVC
execution path was exercised in the recorded local probes.

## 4. What the pod still needs from you

The container image carries the engine; the environment does not carry itself:

| Variable | Why |
|---|---|
| `LOGWEIR_ENGINE_BIN` | Set by the image to `/usr/local/bin/kafka-backup`. Override only if you mount an engine elsewhere. |
| `LOGWEIR_ENGINE_VERSION`, `LOGWEIR_ENGINE_DIGEST` | **Mandatory.** An empty value is refused with exit 1: a signed scorecard must name the engine image that produced the restore. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`, or an IRSA / Pod Identity setup | `object_store`'s own credential chain — **not** the AWS SDK's. `~/.aws/credentials`, `AWS_PROFILE` and SSO are unsupported. See [stability.md](stability.md). |
| `TMPDIR` | Where the rendered `restore.yaml` and the restore checkpoint land. Point it at a writable volume; the checkpoint is pod-local and is never uploaded, so a crashed restore is not resumable in v0.1. |

## 5. Persisting metrics

The CLI emits a Prometheus textfile at `--metrics-file`; it has no HTTP
metrics endpoint. [metrics.md](metrics.md) is the metric and alert reference.
Use [the CronJob example](../examples/cronjob-drill.yaml) for the mount shape.

The textfile must survive the pod and be visible to node_exporter's collector.
An `emptyDir` meets neither requirement. The example uses a node-local
`hostPath` with `type: Directory`; prepare it on every eligible node:

```bash
sudo install -d -m 0775 -g 65532 /var/lib/node_exporter/textfile
```

`DirectoryOrCreate` creates root-owned 0755 directories that uid 65532 cannot
write. `fsGroup` does not change hostPath ownership. `Directory` instead fails
at mount time when the directory is missing. Persistence and these permission
cases were measured on docker-desktop on 2026-09-05.

Pod Security `baseline` and `restricted` forbid hostPath. For either profile,
remove the metrics volume, its mount and `--metrics-file` together. The
restricted variant emits no textfile on any exit path. There is no shipped PVC,
sidecar, Pushgateway or OTLP fallback; use the signed result and pod status.

### Is the drill still running at all?

Use node_exporter's file mtime to detect a stale existing textfile; see
[metrics.md](metrics.md#is-the-drill-still-running-at-all) for the query and
its missing-file limitation. No alert rules ship with the repository.

## 6. Runtime boundaries

The repository includes the CLI, `weirkeeper`, six CRDs and an optional Helm
UI deployment. The CLI runs the engine as a local subprocess inside its runner
pod; the operator creates that Job. A scratch restore uses a marker topic and
cluster allowlist, not namespace labels, to guard the target.

Approval is a DSSE signature checked against rostered public keys, not a
`SubjectAccessReview`. Browser verification, resumable crashed restores and
an HTTP metrics endpoint are not implemented. See [stability.md](stability.md)
for the full limitations and deferred features.

## 7. The control plane: fourteen kinds on `logweir.dev/v1alpha1`

**Minimum Kubernetes: 1.29.** That floor is not about the client library — it
is about **CEL validation rules** (`x-kubernetes-validations`), which reached GA
in 1.29 and are how the CRDs below seal the parts of their `.spec` that may not
change. On an older API server the rules are dropped rather than rejected,
and a dropped immutability rule is worse than no rule: the object would accept
an edit after approval and nothing would say so.

The CRDs are checked in at [../config/crd/](../config/crd/) and regenerated
with `just crds`; CI re-renders them and diffs the result, so a schema change
arrives as a reviewable diff. Do not hand-edit those files.

| Kind | Scope | What it is |
|---|---|---|
| `KafkaCluster` | Namespaced | A saved connection (contract v1, §20): bootstrap servers, `auth{mode, username, secretRef, tls, tlsCa}`, role, and the marker topic that proves a scratch target. The password and the private CA are REFERENCES into this namespace, never values. `status.clusterId` is read from the broker, never from the spec. |
| `BackupSchedule` | Namespaced | A recurring backup of a topic set — a **named** allowlist (no wildcard, no glob metacharacter), or `topics: []` with an `allUserTopics` block resolved per run. `spec.concurrencyPolicy` is `Forbid` by default or explicitly `Allow`. **Every field is editable except `sourceRef`**, which is sealed by CEL; each created `Backup` copies the policy and records `spec.scheduleRef {uid, generation, runPolicySha256}`, so an edit reaches the next run and never a run that exists. `spec.timeZone`, `startingDeadlineSeconds`, `catchUpPolicy`, `retry` and `activeDeadlineSeconds` are optional and absent means today's behaviour. `retention{keepLast, keepDays}` **reports** what it would remove and deletes nothing. |
| `Backup` | Namespaced | One archive run, as a Job. Its name and `status.backupId` are a pure function of the trigger, so a duplicate reconcile gets `AlreadyExists` rather than a second partial archive. |
| `Restore` | Namespaced | One restore run, as a Job. **A drill is a `Restore` with `spec.target.mode: scratch`** — there is no `Drill` kind. A `Restore` only ever writes a *new* topic, so it is non-destructive by construction. |
| `Approval` | Namespaced | A DSSE-signed authorisation for one `Restore` or `Backup`. **Four required spec fields**; `approvalBytes` and `sidecarBytes` are the UTF-8 document text, verbatim, never base64. |
| `TrustRoster` | **Cluster** | **DEPRECATED** in favour of `TrustPolicy`, and still served and reconciled. The keys that may authorise (`approverKeys`) and the keys that may attest (`signingKeys`) — **both carrying public key material** — plus `allowedClusterIds`. Cluster-scoped so a namespace tenant cannot widen its own allowlist. With no `TrustPolicy` in the cluster the controller synthesises `legacy-roster-v1` from `TrustRoster/default`, so nothing has to be migrated on upgrade. |
| `BackupDestination` | Namespaced | Where archives live, saved once and referenced by name (ADR 0008 Amendment F). `spec.storage` and `spec.transport.security` are **immutable**; the description, the CA `ConfigMap` reference and all four credential references are mutable, so rotation needs no new object. It holds **no credential value** — only Secret and `ConfigMap` names and key names. |
| `TopicDiscovery` | Namespaced | One bounded observation of the topics a saved connection can see, run as an isolated Job with no Kubernetes token. `spec.request` is immutable; `spec.cancelRequested` moves `false` → `true` only. The result is **advisory**. |
| `Preflight` | Namespaced | One bounded readiness observation for a `Backup`, a `Restore` or a destination's grants, run the same way. `spec.request` is immutable; `spec.cancelRequested` moves `false` → `true` only. A `ready` verdict **authorises nothing**: every execution-time guard still runs. |
| `TrustPolicy` | **Cluster** | The keys that may authorise and the keys that may attest, with an explicit **lifecycle** (ADR 0008 Amendment G). The policy names the namespaces it governs; a namespace never names its own trust, and one claimed by two policies resolves to **nothing**. The spec is mutable and every change is one-way: keys are append-only with identical public material, `notAfter` only shortens, `state` moves `Active → Retired → Revoked` and never back, and the revocation instants are write-once. |
| `ProtectionPolicy` | Namespaced | The recovery objective a set of schedules is meant to meet, and who hears about it when they do not. **The one kind with no CEL seal**: it is evaluation policy, never an execution input, so editing it changes what Logweir *says* about results and never the results. Notification channels are `secretKeyRef` references; no credential value appears in the spec. |
| `RehearsalSchedule` | Namespaced | A recurring recovery rehearsal on a cron. `spec.suspend` is the only mutable field, because the standing authorisation binds a sha256 of this spec minus `suspend` — an editable template would authorise work nobody approved. The controller deletes no topic; teardown is the runner's phase 9 inside a prefix guard. |
| `RecoveryCatalog` | Namespaced | The durable list of recovery points in one destination, read from object storage by a short-lived check Job. `spec.syncRequest` is the only mutable field. The Kubernetes view is a bounded newest-first window in immutable `ConfigMap` pages owned by the sync Job; it expires with that Job's TTL and reports `Stale` then `ViewExpired` rather than an empty archive. **Adds no `delete` permission anywhere.** |
| `RetentionPolicy` | Namespaced | What may be removed from one destination, and under whose authority. `spec.destinationRef`, `spec.catalogRef` and `spec.scope` are immutable, because an approved deletion plan names point ids. `mode: Report` is the **default and deletes nothing**; see §9. |

`Switchover` is tag 2 and ships in none of the above, not even as a value of
`Approval.spec.subjectRef.kind`. `MetadataSnapshot` is reserved and unbuilt.

**Absent-field behaviour for the D1 run contract.** `Backup.spec` gained
`trigger {kind, attempt, retryOf, timeZone}`, three optional fields inside
`scheduleRef` (`uid`, `generation`, `runPolicySha256`), `allUserTopics` and
`status.selection`. Every one of them is optional. A `Backup` with no `trigger`
is read as `Scheduled`/attempt 0 when `triggeredBy` is `schedule` and `Manual`
otherwise — **exactly what the controller that created it did**, so upgrading
converts nothing and writes nothing. A scheduled run with no
`scheduleRef.uid` still takes its UID from the `BackupSchedule` controller
ownerReference, as before.

`spec.topics` STAYS REQUIRED. A dynamic run carries `topics: []` beside
`spec.allUserTopics`, so an older controller still deserializes the object,
renders an empty list and its runner refuses before contacting the engine; an
absent `topics` would instead be a reflector decode error that stalls every
`Backup` reconcile in the namespace. `allUserTopics.incompleteDiscovery` is
**required and has no default**: Kafka silently omits topics a principal cannot
describe, so no discovery proves whole-cluster visibility, and both possible
defaults are wrong in a way an operator would not notice. A run records what it
actually covered in `status.selection.coverage`, and no surface renders "all
topics" unless that reads `AllUserTopicsAttested`.

**Absent-field behaviour for the editable schedule (PLAT-04.2, PLAT-05.1).**
`BackupSchedule.spec` gained `timeZone`, `startingDeadlineSeconds`,
`catchUpPolicy`, `retry {maxRetries, delaySeconds}`, `activeDeadlineSeconds` and
`allUserTopics`. **Every one is optional and absent reproduces the behaviour of
the controller that stored the object**, field by field: UTC evaluation, a
one-hour starting deadline, no catch-up, no retries, a 3600-second run deadline
and a named allowlist. No stored schedule is rewritten, its
`metadata.generation` does not move, and the first reconcile after the upgrade
only ADDS `status.observedGeneration` and `status.policy`. Nothing about slot
identity changes: a slot is the UTC instant whatever the zone, so
`logweir-backup-<schedule>-<slot>` is the name it always was.

`sourceRef` IS THE ONE FIELD AN EDIT MAY NOT REACH. The CRD carries three CEL
rules: `spec.sourceRef` is immutable (a schedule's identity is the cluster it
protects, and one schedule's history must not mix two clusters);
`spec.allUserTopics` requires `spec.topics` to be empty; and a schedule with
`retry.maxRetries > 0` must be named in 29 characters or fewer, because a retry
`Backup` is named `…-<slot>-r<N>`. All three reference a required field compared
to itself or a field no stored object has, so **no existing object can fail them
on the 1.29 floor**, where there is no validation ratcheting. Two things are
deliberately NOT in CEL and are controller checks instead: "exactly one of a
non-empty `topics` or `allUserTopics`" (a schedule stored with `topics: []`
would otherwise be unable to accept even a `suspend` flip) and cron/time-zone
validity (not expressible). Those produce `Ready=False` with a reason and admit
nothing; see §9.

**Editing is safe because a run is not editable.** Every `Backup` a schedule
creates copies the policy and records `spec.scheduleRef {name, uid, generation,
runPolicySha256}`, and its resolved settings are frozen into an immutable
ConfigMap before any Job exists. The schedule reconciler's entire write surface
against `backups` is a `POST` — no PUT, no PATCH, no DELETE — so an edit cannot
reach a run's spec, its frozen inputs or its Job. `status.policy` reports the
revision in force and the digest of what a run under it does; `runPolicySha256`
covers the source, the topic selection, the archive and the run deadline and
excludes cadence, zone, deadlines, catch-up, retry, concurrency, retention and
`suspend`, so suspending and resuming a schedule visibly leaves it unchanged
while a topic-list edit visibly does not.

**Absent-field behaviour for the D3 additions.** `Backup.status.progress`,
`Backup.status.capture`, `Restore.status.progress`, `Restore.status.completion`,
`Restore.status.teardown` and the `signedAt` / `trust` fields inside
`status.evidence.verification` are all optional and are ABSENT on every object
an older controller reconciled — that is the documented behaviour, not a
degraded state, and a console reads their absence as "not observed" rather than
as a failure. `Restore.spec.approvalRef` became optional and
`Restore.spec.authorization` joined it, with CEL requiring **exactly one**: an
existing `Restore` names an `approvalRef` and is unaffected, and an older
controller reading a standing-authorised one refuses terminally with
`ApprovalNotReceived`. `Approval.spec.subjectRef.kind` gained
`RehearsalSchedule`; no existing spelling changed. A build that does not yet
carry the rehearsal controller refuses such an `Approval` visibly with
`ReferentHasNoPlanBytes` rather than verifying it against bytes nobody hashed.

**Absent-field behaviour for the additive fields.** `Backup.spec.destinationRef`,
`BackupSchedule.spec.destinationRef`, `Restore.spec.sourceDestinationRef` and
`Restore.spec.evidenceDestinationRef` are all optional: absent means the object
carries its location inline in `archive` / `sourceArchive`, exactly as before
saved destinations existed, and nothing about its behaviour changes. Absent
`BackupDestination.spec.readiness.writeProbe` means `Disabled` (no readiness test
ever writes an object). Absent `spec.access.archiveRead` or
`spec.access.evidenceWrite` means `archiveWrite` is used; absent
`spec.access.evidenceRead` means verification is `NotAttempted`, with a detail
naming the field — never a silent fallback to a wider grant. Absent
`TopicDiscovery.spec.request` fields take the defaults printed in the CRD schema.

**When a destination is referenced, `archive.url` is a sentinel.** A
destination-backed `Backup` carries `archive.url:
logweir-destination://<destinationRef.name>` and no `archive.secretRef`, and CEL
refuses any other combination — including the scheme with no reference. The field
stays REQUIRED on purpose. An older controller that decoded an object with no
`archive` at all would fail the whole list/watch with a reflector decode error and
stall *every* `Backup` reconcile; with the sentinel it decodes the object, fails
to parse the unknown scheme, and writes the terminal `ArchiveUrlUnreadable` before
any Job is created. One object fails closed and says so, instead of the kind going
dark.

**Every `kubectl` command line in this repository names its context
explicitly** — `kubectl --context docker-desktop …`, the `proxy` subcommand
included. That covers every copy-pasteable block and every quoted transcript
above, §1's two `bash` blocks and its verified transcripts included. A bare
`kubectl get pods` in prose or in a code comment names what the tool renders
rather than a line to run, and is not one of them.

### The immutability seals

`KafkaCluster`, `Approval` and `TrustRoster` carry one rule on `.spec`:
`self == oldSelf`, message *spec is immutable; create a new object instead*.
`Backup` and `Restore` carry the same seal plus the destination-sentinel
validation rules below. `BackupSchedule` carries one **object-level** rule
instead, naming every field except `suspend`.

`BackupDestination` carries four rules on `.spec`: `spec.storage` and
`spec.transport.security` are compared against `oldSelf` (a different location
or transport is a different destination), the endpoint scheme must match the
declared transport, and a `caBundle` requires `TLS`. **`spec.storage.addressing`
never appears in any of them, in either direction** — path-style versus
virtual-hosted addressing is not a transport choice, and `InsecureHTTP` is the
only thing that permits plaintext.

`RehearsalSchedule` carries a `suspend`-only seal of its own, plus one
validation rule (`point.requireVerifiedEvidence` must be `true` in v1).
`RecoveryCatalog` seals everything but `spec.syncRequest`. `RetentionPolicy`
seals `destinationRef`, `catalogRef` and `scope` and ties `mode` to its block.
`TrustPolicy` is not sealed at all. One object-level rule says an existing
`keyId` may not be removed; everything else about a key — its public material,
its `notAfter`, its `state` and its revocation instants — is a **transition
rule on one item** of `spec.keys`, which is an associative list keyed by
`keyId`, so the API server correlates each entry with its own previous value and
a NEWLY added key is simply not evaluated. That shape is what makes the rules
installable at all: the quadratic form was refused by a live API server for
exceeding the CEL cost budget, and `spec.keys[].keyId` carries an explicit
`maxLength` for the same reason. `ProtectionPolicy` carries no `.spec` rule, and that
is the decision rather than an omission.

`TopicDiscovery` and `Preflight` seal a REQUIRED sub-object, `spec.request`,
rather than the whole `.spec`. That works for the same reason the schedule's
object-level rule does: a transition rule fires only when `oldSelf` has the
field, and a required sub-object is present in every stored object. The one
field an operator may change, `spec.cancelRequested`, sits outside the sealed
object with a rule that lets it move `false` → `true` and never back.

The object level is load-bearing and not a style choice. A **per-field**
transition rule is evaluated only when `oldSelf` exists for that field, so an
optional field — `retention`, and `retention.keepDays` inside it — could be
**added** after creation (absent → present) and a per-field `self == oldSelf`
would never fire. `optionalOldSelf` closes exactly that and is **1.30+**, above
this floor. An object-level rule is evaluated on every update, and its
`has(self.x) == has(oldSelf.x)` halves are what refuse the absent → present
transition.

**The `ValidatingAdmissionPolicy` example is 1.30+ and ships commented.** It is
an alternative expression of the same property, at cluster scope rather than
per-CRD, and it is not a substitute: nothing in the shipped install depends on
it, and uncommenting it on a 1.29 API server would fail to apply.

### 7a. What `VALID` means on a `BackupDestination`, and what it does not

`kubectl get backupdestination` renders a `VALID` column. It is the reconciler's
answer to the two questions CEL cannot reach, and it is **not** a reachability
claim.

```bash
kubectl --context docker-desktop get backupdestination -n team-a
# NAME     BUCKET   ENDPOINT                             TRANSPORT   VALID   AGE
# primary  lw-a     https://minio-a.storage.svc:9000     TLS         True    4m
```

**There is no periodic health probe.** The reconciler dials no endpoint, lists
no bucket and creates no Job; a destination is exercised by the operations that
use it and by an explicit `Preflight` with `operation: DestinationAccess`. A
`VALID` column that meant *reachable four minutes ago* is exactly the defect
PLAT-03 is about, and this one does not mean that.

What it does mean, in the order the controller checks it:

| `status.reason` | What was wrong |
|---|---|
| `Valid` | Every check below passed. |
| `DestinationNotValid` | The stored object does not satisfy the location rules this controller enforces — re-evaluated here, so an object admitted by an older CRD revision is reported rather than discovered by a run. The message names the field and the rule. |
| `AddressingUnsupportedByEngine` | `storage.addressing: VirtualHosted` with a `storage.endpoint` set. The pinned engine forces path-style addressing whenever an endpoint is present, so that combination is refused rather than served as path-style behind your back. `VirtualHosted` with **no** endpoint is AWS S3's own default and is fine. |
| `CaBundleNotFound` | `transport.caBundle` names a `ConfigMap` this namespace does not have. |
| `CaBundleKeyMissing` | The `ConfigMap` exists and carries no such key. |
| `CaBundleTooLarge` | Over 64 KiB. Every byte is copied into every run's immutable plan `ConfigMap`. |
| `CaBundleInvalid` | No parseable `-----BEGIN CERTIFICATE-----` block. A private key, a raw `.der` file or a truncated paste is refused here rather than at the first TLS handshake of a run. |

`status.canonicalUrl` and `status.locationDigest` are written on **every**
verdict, including the refusals: an operator debugging a CA problem still needs
to see which bucket they were pointing at. `status.caBundleSha256` is written
only when bytes were readable, so its absence is *no digest*, never *empty
bundle*. The reconcile requeues every 300 s, because rotating a private CA edits
the `ConfigMap` and not the `BackupDestination`, and nothing else would notice.

**It reads a `ConfigMap` and never a Secret.** A CA certificate is public
material by construction, which is why `transport.caBundle` names a `ConfigMap`.
The four access grants name Secrets, and the controller holds **no verb on
`secrets`** — so it validates their SHAPE and their SPELLING (a DNS-1123 object
name in this namespace, legal Secret data keys) and never their existence or
their contents. A Secret that is genuinely missing is reported by the kubelet,
to the pod that needed it; a `<namespace>/<name>` spelling is refused by the
controller with `DestinationRoleNotConfigured`, naming the rule, because that
spelling is an attempt at a cross-namespace reference and "Secret not found"
would send you looking for the wrong thing.

**How legible that kubelet answer is depends on a wiring that is not in this
build.** The friendly code — `CredentialSecretNotFound`, naming the Secret and
the key — comes from the check framework's pod-waiting classification, which
reaches the `Backup` and `Restore` controllers only with the execution wiring
§7b describes. Until then an absent Secret surfaces as a pod that never starts
and a Job that ends at its `activeDeadlineSeconds`. The pre-flight answer that
*does* work today is a `Preflight` with `operation: DestinationAccess`, which
runs a pod in the object's own namespace and reports what the kubelet said.

### 7b. A destination-backed run carries a complete `AWS_*` set, and none of it is the controller's

**NOT IN THIS BUILD.** The resolver, the environment below and the frozen
snapshot exist and are tested; nothing calls them yet. No `Backup`, `Restore` or
`BackupSchedule` in this release reads a `destinationRef`, so an object that
carries one gets the legacy inline path — and, because `archive.url` must then
be the `logweir-destination://` sentinel, that path refuses it terminally with
`ArchiveUrlUnreadable` rather than writing anywhere. This section describes the
contract the execution wiring will keep, so that the shape is reviewable before
it is load-bearing; until it lands, `AWS_ALLOW_HTTP` on a runner Job is still
whatever the legacy path forwards.

The controller's own environment reaches no destination-backed runner Job. That
is not a convention — it is the defect the design closes. The legacy inline path
forwards `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` and
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST` from the controller process, and the engine
builds its object-store client from *every* `AWS_*` variable it finds — so a
controller started with `AWS_ALLOW_HTTP=true` enables plaintext HTTP inside a
runner whose approved plan says `allow_http: false`.

A destination-backed Job instead carries a complete, explicit set computed from
the `BackupDestination` alone:

| Variable | Value |
|---|---|
| `LOGWEIR_STORE_CONTRACT_VERSION` | `1`, with the matching `--store-contract-version 1` argv. A runner that does not know the contract rejects the flag before dispatch, so a new controller can never drive an old runner into using ambient credentials. |
| `AWS_ALLOW_HTTP` | `true` **only** when `transport.security` is `InsecureHTTP`, which itself requires an explicit `http://` endpoint. Always rendered, including the `false` case: an absent variable is one an ambient value in the pod could still answer. |
| `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | From `storage.addressing`, and from nothing else. **Addressing never changes transport, in either direction.** |
| `AWS_METADATA_ENDPOINT` | A dead loopback (`http://127.0.0.1:1`), so a missing workload identity is a refusal and never a silent fall-back to the node's instance role. |
| `AWS_REGION` | Only when `storage.region` is set. |
| `LOGWEIR_ARCHIVE_CREDENTIALS` | `static` or `workloadIdentity`. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | `valueFrom.secretKeyRef` into the grant's Secret and keys, resolved by the kubelet in the pod's own namespace. The session token only when the grant configures one. |
| `LOGWEIR_ARCHIVE_CA_FILE` | Only with a `transport.caBundle`; the bytes are frozen into the run's own plan `ConfigMap`, so a rotation cannot change a run that already exists. |

`AWS_ENDPOINT_URL` is **absent by construction**. The endpoint travels inside the
plan's own `storage` block, which the runner reads explicitly, so there is no
variable in the pod that could relocate the store.

A restore whose evidence destination differs from its archive destination gets
`LOGWEIR_EVIDENCE_CREDENTIALS` (`archive`, `static` or `workloadIdentity`) and,
when the grant differs, the separately named `LOGWEIR_EVIDENCE_AWS_*` references
— separately named so neither store's credential can shadow the other's. Two
different workload-identity ServiceAccounts in one pod is refused as
`ExecutionContextConflict`: a pod has exactly one ServiceAccount, which is a fact
about Kubernetes and not a policy.

The location a run executes against is **frozen** into that run's immutable
execution inputs, with the destination's UID, generation, location digest and CA
digest. Editing a destination afterwards — rotating a credential, rotating a CA —
cannot change a run that already exists, and a destination deleted and recreated
under the same name is a different input.

### 7c. A `TopicDiscovery` is one observation, and `unknown` is its honest default

**PARTLY IN THIS BUILD.** The reconciler described here exists and is tested: it
resolves the connection, renders the plan, creates the check Job, stores the
chunks and writes the status. The runner's `logweir check run` subcommand, which
prints the frames it reads, is a separate change that has landed since. Against a
runner image without it a discovery ends `Failed` with
`RunnerContractUnsupported` or `ResultUnreadable`; nothing is stored and nothing
is claimed. **No end-to-end run against a real broker has been performed for this
section**, and nothing here is live evidence.

**And `attestedComplete` is unreachable in this build.** The attestation route
below is implemented and tested, but the controller only reads an installation
policy when `LOGWEIR_POLICY_CONFIGMAP` — or `LOGWEIR_INSTALLATION_NAMESPACE`, for
the default `weirkeeper-policy` name — is set on the `weirkeeper` Deployment, and
**neither variable is set anywhere in `config/` or in the chart today**. Until
the chart renders `weirkeeper-policy` and those variables (W11), every check runs
under the built-in defaults: no attestations, so every honest observation is
`unknown` or `limited`, and the concurrency ceilings and `freshSeconds` are the
compiled-in numbers rather than an administrator's. Writing the `ConfigMap` by
hand does not help while the variables are unset.

A `TopicDiscovery` is a **request**, not a cache. `spec.request` is immutable, so
one object is one observation with one recorded instant, and refreshing means
creating another object. That is deliberate: the previous result stays readable
and correctly labelled while the new one runs, and nothing can be quietly
rewritten under a reader who is paging through it.

The observation runs as an isolated Job in the object's own namespace, with the
same credential projection an execution pod gets and **no Kubernetes token**. The
controller never dials a broker itself and never reads a Secret.

**The phases.**

| `status.phase` | What it means |
|---|---|
| *(absent)* / `Pending` | The controller has not looked at it yet. It writes no `Pending` of its own. |
| `Queued` | Over an installation concurrency ceiling (`reason: ConcurrencyLimited`). Retried every ten seconds; nothing was created. |
| `Running` | The check Job exists and has not finished. `reason` carries what the pod is waiting for. |
| `Succeeded` | A verified relay was decoded and its chunks committed. |
| `Failed` | Terminal, with a closed code in `reason` — `ConnectionNotFound`, `ConnectionInvalid`, `DeadlineExceeded`, `ResultUnreadable`, `CheckPlanConflict`, `ResultStorageConflict`, `Stalled`, or a pod-waiting code such as `CredentialSecretNotFound`. |
| `Cancelled` | `spec.cancelRequested` reached a non-terminal object. The Job's deadline was collapsed and **no chunks were written**. |

`ConnectionNotFound` and `ConnectionInvalid` are terminal rather than retried,
because `spec.request` is immutable: the object can never name a different
connection, so a later pass would ask the same question. Create the
`KafkaCluster`, then create a new `TopicDiscovery`.

**`unknown` is not a degraded answer, it is the true one.** An all-topics Kafka
metadata request silently omits every topic the principal may not `DESCRIBE`, and
the broker does not say that it did. A completely clean listing therefore proves
nothing about completeness, and `status.result.visibility.state` says so:

| State | What was observed |
|---|---|
| `unknown` | The listing succeeded and nothing contradicted it. **This is what a healthy run looks like.** |
| `limited` | An authorization failure was *observed* — a listing entry carrying `TopicAuthorizationFailed`, or an expected topic the broker refused to describe. |
| `attestedComplete` | An administrator attestation in the installation policy `ConfigMap` matched this namespace, this `KafkaCluster`, the cluster id the broker actually answered with and the principal Logweir presented, and had not expired, and the inventory was not truncated. |

`visibility.basis` lists why, in a closed vocabulary, including the near misses:
`attestationPrincipalMismatch` and `attestationClusterIdMismatch` are recorded
rather than dropped, because an attestation drifting onto the wrong credential is
exactly the failure that would make `attestedComplete` meaningless.

**Logweir does not verify an attestation.** Only a principal who can write
`weirkeeper-policy` in the release namespace can create one — a chart or cluster
administrator, never a namespace operator — and what the status records is the
attestation's id. Who made it, when, and the statement itself stay in the policy
`ConfigMap`. Render it as "attested by *X* at *T*; not verified by Logweir".

**Empty, failed, stale and permission-limited are four different things**, which
is the point of the kind:

* **empty** — `phase: Succeeded` with `result.counts.returned: 0` and an empty
  `result.chunks`. A fact about what this principal can see, and not an error.
* **failed** — `phase: Failed` plus `reason`.
* **stale** — `status.freshUntil` is `observedAt` plus the policy's
  `discovery.freshSeconds` (900 s by default). Past it, the inventory is old, not
  wrong. Two further staleness rules — the connection binding changed, or a newer
  success supersedes this one — are the API's, because both compare this object
  against something that changes after it is written. `status.binding` records
  the connection UID, generation, principal, auth mode and bootstrap digest the
  observation was taken against, and is written once and never refreshed, so that
  comparison has something to compare. The one exception is
  `binding.policyDigest`, which is written at plan time and then **updated at
  commit** to the digest of the policy the completeness verdict was actually
  computed under — the verdict is a commit-time computation, and an
  administrator adding an attestation while the Job ran would otherwise leave
  `attestedComplete` beside the digest of the pre-attestation policy.
* **permission-limited** — `visibility.state: limited`.

**`observedAt` is the runner container's own `finishedAt`**, not the instant a
reconcile noticed. That is the same rule the `KafkaCluster` probe uses, and it is
what makes a re-read of the same Job produce a byte-identical status patch
instead of a hot reconcile loop.

**The topic names are not in the status.** An inventory is unbounded and a status
is not a store. The names live in immutable `ConfigMap`s owned by the
`TopicDiscovery`, at most 2,500 entries and 768 KiB each, one TSV line per topic
(`name`, partitions, flags). `status.result.chunks` carries each chunk's name,
digest, count and first and last topic name — enough to page and to skip whole
chunks on a prefix search — and `status.result.topicsSha256` is the digest of the
whole canonical inventory, computed by the controller over the frames it verified
and never copied from the runner's own claim. A reader that fetches the chunks
and hashes them gets the value the status published.

**At most 64 chunks, because that is what the status schema can index.** The
entries are cut to the plan's own `maxTopics` (and, as a backstop, to
64 × 2,500 = 160,000) **before** any `ConfigMap` is written, and the result is
then reported `truncated: true` with `truncationReason: MaxTopics`. The cut is
made on the entries and never on the index alone, so the chunks that exist, the
index that names them and `topicsSha256` are always one set. A runner that
relayed more than its plan allowed therefore produces a smaller, honest,
truncated result rather than a status the API server refuses — a `/status` PATCH
rejected for violating its own schema would leave the chunks in etcd behind an
object that never reaches a terminal phase and whose Job is never collected.

Reading those chunks with `kubectl` needs `get` on `configmaps`, which no human
Logweir role grants. `kubectl` users get the summary in the status; browsing the
inventory is the console API's job.

**Internal topics are excluded by default** and counted in
`counts.internalExcluded`, so you can see that they exist without them filling the
list. The rule is Kafka's own reserved-name convention — a name starting with
`__` — and nothing else is guessed: `_schemas` and `_confluent-*` are
configurable names, and a wrong "this is internal" would silently drop a user's
data from a selection. `spec.request.includeInternal: true` returns them, flagged.

**Cancel, TTL and cleanup.** `spec.cancelRequested` may move `false` → `true`
and never back. Cancelling collapses the Job's `activeDeadlineSeconds` to 1 —
the controller holds `delete` on nothing — after verifying that the Job's
controller owner is this object, so a Job that merely shares the name is never
touched. A finished Job gets `ttlSecondsAfterFinished: 600`, patched **only
after** the status write returned 200 — on the failed path exactly as on the
successful one: the relay lives on the pod, and the TTL controller removes a Job
and its pods together, so a TTL set after a status write that answered 409 would
let garbage collection take the reason an operator still has to read. The plan and chunk `ConfigMap`s
carry an owner reference with `blockOwnerDeletion`, so deleting the
`TopicDiscovery` removes everything it owns by cascade.

**Retention is not yet automatic.** D2 §5.8's 24-hour retention and keep-last-five
per connection are implemented as a pure rule and are *not wired*: deleting the
custom resource needs a `delete` verb the `weirkeeper` ClusterRole grants on
nothing today. Until that grant lands, terminal `TopicDiscovery` objects
accumulate and are deleted by an operator or by namespace cleanup.

**Status writes clear what they no longer claim.** The `/status` merge PATCH
carries an explicit `null` for every clearable field this pass computed as
absent — `lastAvailablePoint`, `lastAttempt`, `missed`, `schedules`,
`rehearsal`, `staleSince`, `alerts`. RFC 7386 removes a key only for a `null`,
so without them the console would keep serving a recovery point the controller
had just decided was not available, with `health: Unknown` beside it. The
no-op skip is unaffected: a `null` for a key the object does not have changes
nothing, so a steady object still sends no patch at all.

**One write per interval, not one per reconcile.** `evaluatedAt`,
`lastAvailablePoint.ageSeconds` and `missed.sinceLastFire` are all measured
*at* `evaluatedAt` and move only when it does; the condition `message`s carry
no clock-derived number at all. A pass over unchanged cluster state inside the
half-interval window therefore sends nothing, and past it sends exactly one
patch in which only those three fields differ.

**RBAC.** This kind adds exactly two rules to the `weirkeeper` ClusterRole:
`list`/`watch` on `topicdiscoveries` (the controller's watch) and `patch` on
`topicdiscoveries/status`. No `get`: the reconciler never re-reads a discovery.
No verb on `secrets`, and no `delete` on anything.

**Upgrade and rollback.** The kind is additive: nothing existing references it,
and an installation that never creates one behaves exactly as before. An older
controller running against the newer CRDs simply does not reconcile
`TopicDiscovery` objects, which then sit with no status — visibly pending rather
than silently wrong. Rolling the CRD back deletes any `TopicDiscovery` objects
and, by owner cascade, their result `ConfigMap`s and check Jobs; no `Backup`,
`Restore` or archive is affected, because a discovery result is never an
execution input.
### 7d. The `RecoveryCatalog` view: bounded, expiring, and never a second source of truth

A `RecoveryCatalog` indexes one destination's durable catalog —
`logweir/catalog/v1/` in object storage, written by the backup runner beside every
receipt (`docs/formats/catalog-point.md`). **The archive is the truth.** What
lives in Kubernetes is a window onto it, and the window is deliberately small,
deliberately immutable, and deliberately temporary.

```bash
kubectl --context docker-desktop get recoverycatalog -n team-a
# NAME     DESTINATION  POINTS  AVAILABLE  SYNCED  TRUNCATED  AGE
# primary  archive      5312    5287       2m      true       6h
```

**What a sync is.** A short-lived check Job — the same `logweir check run` every
other check uses, with the `catalogSync` plan kind — reads the catalog with the
destination's own credential, projected by `secretKeyRef`, and prints its result
through the ordinary result frames. **The Job is created before its plan `ConfigMap`, and the plan is owned by the
Job.** Every object one sync produces — the plan, the pages, the fence pointer —
is owned by that Job, so the Job's TTL is the one thing that removes any of
them. An `ownerReference` needs the owner's UID and a Job's UID exists only once
the API server has created it, so the order inverts, and the consequence is
worth knowing: **there is a window in which the Job exists and its plan does
not**, and a pod scheduled inside it sits `ContainerCreating` on the missing
volume. The kubelet retries that mount indefinitely, so the ordinary case
resolves in milliseconds. A controller that crashed inside the window leaves a
pod pending until the Job's `activeDeadlineSeconds` fires — and nothing is
recorded in `status.lastSyncJob` until both objects exist, so the next reconcile
computes the same name, gets `AlreadyExists` on the Job, and creates the plan
the pod is waiting for.

The controller reads that Job's stdout from
the pod it can prove the Job owns, verifies each page's digest over the page's
own entry lines, and writes the result. It runs on `spec.sync.intervalSeconds`,
or immediately when `spec.syncRequest` changes — that token is the **only mutable
field**, and the controller records the one it acted on in
`status.observedSyncRequest`, so asking twice is one sync and a new token is not
mistaken for a retry.

**What the view is bounded by.**

| Bound | Value | Why |
|---|---|---|
| `spec.sync.viewLimit` | 100–5000, default 2000 | How many **newest** points are materialised. Points beyond it are counted, histogrammed and reachable with `logweir catalog list` against the archive — never silently dropped, and `status.truncated` says the view is a window. |
| `status.pages[]` | at most 8 `ConfigMap`s | Each page is immutable, carries its entries one compact JSON per line under `entries.jsonl`, and records its own `sha256`. |
| `status.indexConfigMap` | one `ConfigMap` | The fence pointer: per page, the point-id range and the recovery-point range. A fence pointer **excludes** pages; a hit inside the range is not an existence proof. |
| `status.histogram` | at most 400 days | Points per day over the whole walk, not only the window. |
| `status.signers[]` | at most 16 | Untrusted keys first, so the row an administrator has to act on is never the one that is dropped. |

**How the view is collected, with no `delete` permission anywhere.** Every page,
the fence pointer and the plan are owned by the **sync Job**, with
`blockOwnerDeletion: false` (`true` would ask for `update` on `jobs/finalizers`
under the `OwnerReferencesPermissionEnforcement` admission plugin, which this
`ClusterRole` grants on nothing). The Job carries
`ttlSecondsAfterFinished = max(3 × intervalSeconds, 3600)`; when Kubernetes
removes the Job, garbage collection removes everything it owns. Each successful
sync publishes a new set and the previous one ages out.

**How many generations coexist, exactly.** One Job lives per slot, so the number
alive at once is `ttl / intervalSeconds` — **three** at an hourly cadence, and
**twelve** at `spec.sync.intervalSeconds: 300`, which a CEL rule on the CRD
enforces as the floor (`intervalSeconds` is `0`, meaning manual only, or at
least 300). The worst case per catalog is therefore 12 generations × (≤ 8 page
`ConfigMap`s + 1 fence pointer + 1 plan) = **at most 120 `ConfigMap`s and 12
Jobs**, and at the default hourly cadence 30 and 3. That bound is the reason the
floor exists: an hour's TTL with a one-second cadence would be 3 600 live
generations. If syncing stops for longer
than the TTL the view disappears and the object reports `Stale` and then
`Ready=False/ViewExpired`. **`status.pages`, `status.indexConfigMap` and
`status.truncated` are cleared on every path that writes a status** — the idle
one, a running sync, a sync whose result did not read, and a refusal about
something else entirely, such as a destination that went invalid meanwhile. A
`status.pages[]` naming a `ConfigMap` the API server no longer has is a link to
a 404, and a reader cannot tell it from a page it simply has not fetched. **The archive is untouched by any of this**: the
controller holds no `delete` verb on anything, and `logweir catalog list` still
reads the durable catalog.

**Two axes, and nothing merges them.** Each entry carries an `availability` and a
`verification`, and a `selectable` flag that is their conjunction.

| `availability` | meaning |
|---|---|
| `Available` | receipt, sidecar and manifest readable; the manifest digest equals the receipt's |
| `Missing` | a definite `NotFound` |
| `Unreadable` | any other storage error — 403, timeout, truncated. **"Could not tell", never "is not there".** |
| `Deleted` | a completed retention tombstone exists |
| `Conflict` | two records disagree for one identity, or a record's facts contradict the receipt |
| `UnsupportedFormat` | the record's major version is above this build's |
| `Partial` | a sampled segment the manifest lists is missing |

| `verification` | meaning |
|---|---|
| `Verified` | the receipt's DSSE verifies under a key this installation accepts |
| `VerifiedHistorical` | the same, under a retired or expired key, for evidence signed while it was valid |
| `UntrustedSigner` | the signature verifies under a key this installation does **not** list |
| `Revoked` | the key was revoked for compromise |
| `Invalid` | the signature does not verify, or a digest does not match |
| `NoEvidence` | a manifest with no receipt at all |
| `NotAttempted` | nothing could be fetched, or this installation holds no trust material |

A point is offered for an ordinary restore only when it is `Available` **and**
(`Verified` or `VerifiedHistorical`). Everything else is listed with its exact
state and a remedy sentence; nothing is hidden and nothing unverified is
presented as verified evidence.

**The two axes merge in opposite directions across a point's locations.** D3 §5.1
makes one archive copied to a second bucket ONE point in TWO places, so
**availability merges best-of**: a point present in bucket A and absent from
bucket B is still fully recoverable from A, and hiding it because the second copy
went missing is the opposite of what a second copy is for. Each entry's
`locations[]` carries its own `availability`, and every degraded copy is **named
in `remedy`**, so a best-of answer never costs you the knowledge of which bucket
to repair. The **signature merges worst-of**, with the signer key id following
the worse verdict: bytes that fail verification in one place are evidence about
the point and not about the place. A receipt-derived disagreement between two
records overrides both and is `Conflict`.

**What a sync does NOT verify.** It does not decide trust. The Job is handed this
installation's **public** signing keys in an immutable `ConfigMap` (public
material only — nothing private ever reaches a `ConfigMap`) and reports whether a
signature verified and under which key id. Whether that key is one this
installation accepts, has retired or has revoked is decided by the controller
against the trust source, once, and applied to both the entries and
`status.counts.untrustedSigner` — which is summed over **every** signer the walk
saw, not over the sixteen `status.signers` can display. `signers[].trusted`
answers *does this installation accept evidence from this key*, not *is it
listed*: a key the trust source lists with `state: Revoked` is reported
`trusted: false`, because its private half is in someone else's hands. A public key found beside an archive is a
**claim**: it is displayed with its fingerprint and is never trusted by proximity
(`docs/keys.md`). With no trust material at all, `TrustAvailable=False` and every
point is `NotAttempted` — an installation that holds no key has not disproved
anything.

A sync also does not re-derive the facts it displays. The signed receipt is the
verification root: the point id is `sha256` of the receipt bytes, and a **restore
re-verifies its point at execution**, so nothing here is evidence — it is an
index onto evidence.

**Counts do not sum to `total`, and that is stated rather than smoothed over.**
`status.counts` has ten fields across the two seven-state axes.
`counts.unverified` folds `NotAttempted` and `NoEvidence`; `Partial`, `Revoked`
and `VerifiedHistorical` have no field of their own and are named in the `Synced`
condition's message instead, each with the scope it is known for — `Partial` over
the whole archive, the other two over the materialised view, because the
verification axis is re-decided here and only for the entries this controller
re-classified.

**What the sync Job must print, and what bounds it.** The result body travels in
the `details` relay stream and is read line by line:

```
catalog-format=1                                     # required; a higher major is refused
catalog-page=<i>/<n> count=<c> sha256=<64 hex>       # 1..=n in order, n <= 8
catalog-entry=<compact json>                         # exactly <c> per page
catalog-counts=<json>   catalog-cursor=<json>   catalog-signers=<json>
```

The page digest covers **that page's raw entry lines and nothing else** — the
three summary lines are covered by the relay's own frame-stream digest, which
D2's decoder verifies first, and a second digest over them would be a second
answer to a settled question. The bounds are **bytes and points, never line
counts**: at most 5 MB of `details` (the relay's own budget is about 5.9 MB of
raw bytes after base64 part-frame expansion, so this sits inside it), at most
`spec.sync.viewLimit` entry lines, at most 64 `catalog-signers` rows and at most
16 `locations[]` on one point. A summary line that arrives **twice is an error**,
not last-wins: two `catalog-counts=` lines mean the runner disagreed with itself
about the walk. A malformed entry is skipped and counted; a malformed page is
fatal for the sync, and every failure is reported with D2's closed
`ResultUnreadable` code and none of the body's content.

**Conditions.** `Ready` (a usable view exists now), `Synced` (what the last sync
did — `Succeeded`, `PartialScan` when part of the archive could not be read,
`ScanIncomplete` when the object budget ran out and the cursor was recorded, or a
closed check code on a failure), `Stale` (the view is older than two intervals; a
`intervalSeconds: 0` catalog is manual-only and never stale by the clock) and
`TrustAvailable`.

**A failed sync keeps the previous view.** The status patch omits `pages`, which
an RFC 7386 merge patch reads as "leave it alone", and those pages age out on
their own Job's TTL. A view that is still true is not blanked because the next
walk could not run.

**Upgrade and rollback.** The kind and its controller are additive: nothing
exists until an operator creates a `RecoveryCatalog`, and a cluster with none is
byte-for-byte unaffected. On rollback the objects become inert — the controller
stops syncing, the last sync Job's TTL removes its pages, and the durable catalog
in object storage is untouched. `spec.legacyArchive` is admitted by the CRD for
PLAT-15.2's connect-an-existing-archive path and **this build creates no sync Job
for it**: such a catalog reports `Ready=False/LegacyArchiveUnsupported` and names
the supported path (a `BackupDestination` with a read-only `archiveRead` grant).

### 7e. A `ProtectionPolicy` says whether you can recover, and a green schedule does not

An enabled schedule is not protection. It says the cron is firing; it says
nothing about whether a recoverable point exists, whether the archive still
holds it, or whether the evidence verifies under a key this installation
accepts. PLAT-14.2's `ProtectionPolicy` is the object that answers the second
question, and the console renders the two side by side and never collapses
them.

**The spec is mutable, and it is the only new kind of this group that is.** An
objective is evaluation policy, never an execution input: nothing here is
frozen into a run, no run reads it, and editing it cannot change a recorded
result — only what Logweir *says* about results that already exist. There is
therefore no CEL seal on `.spec` and `status.observedGeneration` is how you
tell which revision a verdict came from.

**What `recoveryPointAt` is, and what it is not.** It is the **capture start**
of the newest available point (`Backup.status.capture.startedAt`). It is not
`windowCovered.toMs` — that is the newest *record* instant, so an idle topic
would look stale forever — and it is not `finishedAt`, which would under-report
the gap by the length of the run. The newest record instant is published beside
it as `status.lastAvailablePoint.newestRecordAt`, under its own name, so the
two are never conflated.

**A point is AVAILABLE when all four hold:** the run is `phase: Succeeded` with
`exitCode: 0`; its evidence is `Valid` or `ValidHistorical` (unless
`objectives.requireVerifiedEvidence` is turned off); it covers this policy's
source, destination and topics; and — when `protects.catalogRef` is set and
`objectives.requireCatalogAvailability` is on — the catalog's materialised view
is current and says the point is `Available`. `Untrusted` evidence is
deliberately not a pass: the bytes verify, and the installation said it does not
accept that key.

**`health` has five values and `Unknown` is one of them.**

| `status.health` | what it means |
|---|---|
| `Healthy` | an available point inside `maxRecoveryPointAgeSeconds`, failures below the threshold, no suspended or not-ready schedule, no missed slot |
| `AtRisk` | inside the objective, but failures at the threshold, a suspended/not-ready schedule, or a missed slot |
| `Stale` | the newest available point is older than the objective |
| `Unprotected` | there is no available point at all |
| `Unknown` | the evaluation could not happen: the catalog view expired or could not be read, or the referenced source or destination is gone |

`Unknown` is **never rendered as healthy and never as a failure**. The
`Protected` condition is `True` only for `Healthy`, `Unknown` for `Unknown`,
and `False` otherwise — a `False` on an evaluation that did not happen reads
everywhere as "Logweir checked and you are not protected", which is a claim the
controller did not make. `status.availabilityBasis` says which claim you have:
`KubernetesStatus` (from `Backup.status` alone), `Catalog` (a fresh view
answered) or `CatalogStale`.

**Failure accounting counts SLOTS, not attempts.** A retry chain (D1's
`spec.trigger.kind=Retry`) is one failed slot — its final attempt — so
`maxConsecutiveFailedRuns: 2` means two failed nights and not two retries of
one. A slot still running stops the walk: counting past it would open an alert
for a condition a run in flight may be about to end. A missed slot counts
towards `AtRisk` only when it is **newer than the schedule's last fire** —
`status.lastMissedSlot` is an audit trail that is never cleared, so reading it
as a live signal would pin a perfectly protected schedule to `Protected=False`
for its whole life. It is still reported under `status.missed`. Membership uses
`spec.scheduleRef.uid` (the authority) and not the `logweir.dev/schedule-uid`
label (the index), which is why **a manual run of a schedule counts as that
schedule's history**.

**Alerts are a ledger, not a log.** One open alert per `(policy, kind)` under a
stable key, `logweir-protection-<policyUID>-<kind>`. **`Staleness` covers
`Unprotected` too** — there being nothing to recover from at all is the worst
state the enum has, and it pages under the same key rather than opening a second
incident for the same objective. **A resolve is only ever sent when `health` is
back to `Healthy` or `AtRisk`**: under `Stale`, `Unprotected` or `Unknown` an
open entry is left exactly as it is, because protection getting worse, or
becoming unmeasurable, is not the condition clearing and a `resolve` on the
shared dedup key would close a real incident. `transition` increments on
open, on resolve and on each re-notify; `notifiedTransition` records the last
transition a delivery Job was created for. A condition that stays true produces
**no further messages** — not one per reconcile — until it resolves or until
`notifications.renotifyAfterSeconds` elapses. `RecoveryCompleted` is the
exception in three ways: it keys on the **Restore** UID (a policy's points are
restored many times), it is webhook/Slack only, and it auto-resolves the instant
it opens.

**Delivery is a Job, and that is a boundary and not an implementation detail.**
The controller holds no verb on `secrets` and gains no HTTP egress. For each
alert transition it creates one Job `<policy>-n-<sha8>-<attempt>` running
`logweir notify deliver --event /event/event.json` in the runner image, and one
immutable `ConfigMap` `<policy>-ev-<sha8>` holding the unsigned protection
event that Job mounts, with the sink credentials projected as
`valueFrom.secretKeyRef` — a reference the kubelet resolves, never a value this
controller read. Both names are pure functions of
`(policyUID, alertKey, transition)`, so a duplicate reconcile is a **409** and
not a second page. **Three attempts in total** for one transition, waiting 60 s
and then 300 s; exhaustion sets `NotificationsDelivered=False` with reason
`DeliveryFailed` **and does nothing else**. (D3 §3.4 reads "at most 3 times with
60 s/300 s/900 s", which is four attempts if the first is not a retry; three is
what shipped, because it bounds a transition's delivery inside the interval the
policy is re-evaluated on, so a failure is visible in status before the next
pass.) A notification failure never
rewrites a backup result: this controller patches `protectionpolicies/status`
and nothing else, and it reads `Backup` and `Restore` objects through bounded
`list` calls only.

**The ordering rules, all three of which are load-bearing.** The delivery Job
is created **before** the event `ConfigMap` it mounts, and that `ConfigMap` is
owned by the FIRST Job of its transition so the API server's TTL controller
collects it — owned by the policy, an immutable object that this role cannot
delete accumulated one per `(alertKey, transition)` for the life of the policy.
The cost of the order is stated rather than buried: a pod scheduled in the
window between the two creates sits `ContainerCreating` on a mount the kubelet
retries, and a crash inside that window leaves a Job whose ConfigMap never
arrives — which the next pass repairs, because the ledger records a delivery
only once both objects exist. `ttlSecondsAfterFinished` is patched
onto a finished delivery Job **only after** the `/status` patch carrying that
delivery's verdict returned 200 (the exit code lives on the pod, and the TTL
controller removes the Job and its pod together). And the pod is proved by the
Job's own `metadata.uid` on its controller `ownerReference` **before** one byte
of its log is read — a `notify-result=` line becomes a status field and then an
API response, so reading a stranger's is defect `SEC-PODLOG`. Only the seven
`notify-result=` values this build knows are read; nothing else from a pod log
can reach a status.

**`verificationScope` is `sampled`, `degraded` or `none` — never `complete`.**
Logweir compares a sample of records. The value reaches a PagerDuty incident
title and a Slack channel where someone decides, during an incident, whether an
archive can be trusted, so the type has three variants and `logweir notify
deliver` refuses a fourth at parse time. See
[`docs/formats/protection-event.md`](formats/protection-event.md).

**Reading the catalog, and what a field this build cannot find means.** The
policy reads the catalog's own materialised `selectable` — BOTH of D3 §5.4's
axes — so a point that is `Available` but whose signer this installation has
retired or revoked is not protection, and `ArchiveUnavailable` opens for it. An
entry whose axes come through EMPTY (a rename in the catalog's page schema) is
read as `CatalogUnreadable` and therefore `Unknown`, never as "not available":
the second reading would tell every catalog-backed policy in the cluster that
its backups are gone because a field moved. Nothing yet holds the two spellings
together — W8 or W13 should add one fixture line asserting a serialized
`catalog_view::ViewEntry` deserializes into `protection::CatalogEntry` with both
axes preserved.

**Bounds.** At most 16 ledger entries (8 of them recoveries), 16 schedules, 64
topics on `lastAvailablePoint` (`topicsTruncated: true` beyond that), the newest
50 runs considered per evaluation over at most 5 API pages of 200, and at most 4
delivery Jobs created per reconcile pass. Over a window `W` one policy therefore
creates at most `5 kinds × (2 + W / renotifyAfterSeconds) × 3 attempts` delivery
Jobs.

**Deviations from decision D3 §3, recorded here rather than in a commit
message.** The delivery Job runs as `logweir-runner` and not as a new
`logweir-notifier` ServiceAccount: `logweir-runner` is bound to no Role or
ClusterRole, is granted no verb on anything, its token is not mounted (on the
account and again on the PodSpec), and creating a second zero-verb account would
touch four files this worker does not own. **The NetworkPolicy half is not
covered and W13 owns it:** `logweir-runner-egress` selects
`batch.kubernetes.io/job-name Exists`, so a delivery pod inherits egress to the
broker ports as well as 443, where D3 §9 wants a notifier reaching DNS and 443
only. The `logweir.dev/component=notification` label is already on the Job and
its pod template, so the narrower policy is a selector away; until then, on an
enforcing CNI a compromised runner image in a delivery pod can reach a broker.
A policy may declare up to four notification routes, but `logweir notify
deliver` reads one environment variable per sink kind, so one Job addresses at
most one PagerDuty, one webhook and one Slack — the **first** of each in route
order.

**RBAC.** This kind adds exactly two rules to the `weirkeeper` ClusterRole —
`list`/`watch` on `protectionpolicies` and `patch` on
`protectionpolicies/status` — and widens one that already existed: `get` joins
`list`/`watch` on `recoverycatalogs`, for the ONE catalog a policy names
(narrower than the cluster-wide `list` on `configmaps` the alternative would
need, which `config/rbac/role.yaml` refuses by name). No `get` on
`protectionpolicies` itself — the reconciler never re-reads a policy the watcher
handed it — no verb on `secrets`, and no `delete` on anything.

**Upgrade and rollback.** The kind is additive and off by default: nothing
references it and an installation that never creates a `ProtectionPolicy`
behaves exactly as before. An older controller running against the newer CRDs
does not reconcile them, so they sit with no status — visibly pending rather
than silently wrong. Rolling the CRD back deletes any `ProtectionPolicy` objects
and, by owner cascade, their event `ConfigMap`s and delivery Jobs; no `Backup`,
`Restore` or archive is affected, because a protection verdict is a derived
projection and never an execution input. An absent `status.alerts` after a
rollback and re-apply means only that no alert has been recorded yet: the ledger
is state, not history, and a re-opened condition opens at transition 1 again.

## 8. The approval flow

An `Approval` object that exists is **not** an approval. An `Approval` whose
status the controller set to `Verified=True` is. Creating the object is a
`POST` any namespace tenant can make; what turns it into an authorisation is a
DSSE signature, by a key on the roster, over bytes that bind *this* plan and
*this* kind of subject.

### Install step 1: the roster

**Before any custom resource, create the one cluster-scoped `TrustRoster`, and
name it `default`.**

```bash
kubectl --context docker-desktop get trustroster default
```

`default` is a **fact, not a convention**. The controller resolves
`trustrosters/default` and nothing else — not a name read off the `Approval`,
not one from a flag, not one from the environment, because a roster whose name
the subject supplies is a roster the subject can choose. A roster under any
other name authorises nothing, and every `Approval` in the cluster is refused
with:

```
Verified=False  reason=RosterNotFound
no cluster-scoped TrustRoster named 'default'; see docs/kubernetes.md install step 1
```

That is a refusal you can read, never a silent one. `spkiPem` on every entry is
a **public** key in SubjectPublicKeyInfo PEM form; nothing in this API group has
a field a private key could go in.

`kubectl --context docker-desktop get trustroster default` renders `LOADED` and
`EXPIRED` from the roster's own status, and those two columns mean something
because a reconciler writes them: it parses every `approverKeys[].spkiPem`
**and every `signingKeys[].spkiPem`**, sets `status.loaded`, and lists in
`status.expiredKeyIds` every entry from **either** list whose `notAfter` has
passed. A roster with one unparseable PEM is `Loaded=False` naming the `keyId`
— and refuses every approval, because a partially loaded roster is not a
roster. Expiry is reported here so no consumer has to derive it from a clock it
does not share with the controller.

### Trust resolution: which keys govern a namespace

> **WIRED.** Every `Approval` admission, every evidence verification and every
> governed-restore bundle now resolves trust as below: each asks which trust
> governs the object's own namespace instead of loading `TrustRoster/default`.
> **A key revoked on a `TrustPolicy` IS withdrawn** — a fresh approval refuses
> it, a fresh verification refuses it, no runner Job is given its public half,
> and an object that already finished has its verdict re-derived when the
> policy changes. See
> [What a revocation changes, and when](#152b-what-a-revocation-changes-and-when).

`TrustRoster/default` is the **fallback**, not the only answer. A cluster may
carry cluster-scoped `TrustPolicy` objects (PLAT-19.1), and each one names the
namespaces it governs. A namespace still never names its own trust — for
exactly the reason the roster's name is fixed: a roster whose name the subject
supplies is a roster the subject can choose.

Resolution, for one namespace, in this order:

1. **An exact `spec.namespaces` match** on some `TrustPolicy` → that policy.
2. **The one policy with `spec.default: true`** → that policy.
3. **The synthesised `legacy-roster-v1`**, built in memory from
   `TrustRoster/default`. Nothing writes it as an object.
4. Neither a policy nor a roster → the same `RosterNotFound` refusal above.

And one rule that is not an ordering:

> **A namespace two policies both claim resolves to NOTHING.** Every approval
> and every verification there is refused with `TrustPolicyConflict`.

Picking one — the first listed, the newest, the alphabetically smallest — would
be a trust decision made by a sort order, and two administrators who each
believe they govern `team-a` disagree about which keys may authorise a restore
there. The safe reading of a disagreement about authority is that there is
none. The conflict is reported on **both** policies' `status.conflicts`, so it
is visible in `kubectl get trustpolicy` and not only in a refusal message. Two
policies setting `default: true` are the same fault one level up: every
namespace that would have fallen to a default is refused instead, and both
policies report it under the sentinel namespace `*`, which is not a DNS-1123
label and so can never be a real namespace.

`kubectl --context docker-desktop get trustpolicy` renders `DEFAULT`, `KEYS`,
`LOADED` and `BOUND`, and those columns mean something because a reconciler
writes them. Per key it reports `effectiveState`
(`Active`/`NotYetValid`/`Expired`/`Retired`/`Revoked`/`Unparseable`),
`usableForNewSignatures` and `usableForVerification`
(`Full`/`Historical`/`None`), beside an `evaluatedAt` heartbeat that is
refreshed at most every five minutes. That heartbeat is what lets a consumer
tell "not evaluated" from "evaluated and valid" — the thing
`TrustRoster.status.expiredKeyIds` could not say — and the console renders
`unknown`, never `valid`, when the status is absent, when `observedGeneration`
lags `metadata.generation`, or when `evaluatedAt` is more than fifteen minutes
old.

A `TrustPolicy` key declares **exactly one** usage — `EvidenceSigning`,
`GovernedApproval` or `ConsoleConfirmation` — and the API server enforces it
(CEL rule G8). It has to be enforced at admission rather than later: `usages` is
immutable and `spec.keys` is append-only, so a key that both attested and
authorised could never afterwards be narrowed or removed. `logweir trust
migrate-roster` therefore refuses a roster key that is on both `approverKeys`
and `signingKeys`, naming it, instead of emitting a document `kubectl apply`
would reject. The in-memory `legacy-roster-v1` still merges them, which is what
keeps an unmigrated cluster working.

An unparseable `spkiPem` on a `TrustPolicy` is **one key** reported
`Unparseable` and `Loaded=False`; the other keys keep working. That is
deliberately different from the roster's all-or-nothing rule above, and the
difference is the status shape: the roster has one `loaded` boolean and nowhere
to record which entry failed, while the policy reports every key by id. An
unparseable key verifies nothing, so excluding it widens no trust — whereas
refusing a 64-key policy over one bad paste would stop every restore in every
bound namespace.

`TrustRoster/default` carries a `Superseded` condition, and it has **two**
states. It reads `Superseded=True/SupersededByTrustPolicy` only when one
`TrustPolicy` sets `spec.default: true` — that is the only policy that displaces
the roster for *every* namespace, because resolution reaches the roster only
after the default has been tried. With no policy, with policies that name only
some namespaces, or with two contesting defaults, it reads
`Superseded=False/RosterStillConsulted` with a message naming which: every
namespace no policy governs still resolves to `legacy-roster-v1`, **synthesised
from that roster**, so it must keep being maintained. The condition is
recomputed by the roster's own reconciler as well as by the policy's, so
deleting every policy clears it within one 300 s requeue rather than leaving the
roster advertising a supersession that has been rolled back.

**Its spec is not touched and it is not deleted**, which is the rollback path:
an older controller reads only the roster, still present and unchanged. See
[`keys.md`](keys.md) for the rotation procedure, the migration command, the one
deliberate tightening the synthesis applies, and the one thing rollback does not
carry.


### The checks

Each `Approval` event resolves the roster, fetches the referent named by
`spec.subjectRef`, reads its `spec.planBytes`, and runs the checks **in this
order**. The order is load-bearing: the DSSE verifier refuses a `payloadType`
mismatch itself and returns the same error kind for "no signature by this key",
so an implementation that simply tried the roster's keys would report *one*
refusal for a substituted document, an attacker's key and a genuinely broken
signature alike.

| # | Check | `reason` on failure |
|---|---|---|
| 1 | `sidecarBytes.payloadType` is the approval payload type — **before any key is tried** | `PayloadTypeMismatch` (both strings, in full) |
| 2 | **Every** `approverKeys[]` entry parses | `SignatureInvalid`, naming the `keyId` |
| 3 | Each declared `keyId` is SHA-256 of the public key's DER SPKI | `KeyIdNotInRoster`, naming both ids |
| 4 | Some `sidecarBytes.signatures[].keyid` is on `approverKeys` | `KeyIdNotInRoster`, naming the sidecar's key ids |
| 5 | The signature verifies, under the **matched** key | `SignatureInvalid` |
| 6 | The matched entry's `notAfter` is in the future | `KeyIdExpired` |
| 7 | The document's `plan_hash` equals the sha256 of the referent's `spec.planBytes`, **recomputed** | `PlanHashMismatch` |
| 8 | The document's `subject_kind` equals the referent's kind | `SubjectKindMismatch` |
| 9 | The object's own `spec.planHash` -- the unsigned field beside the documents -- equals that same recomputed hash | `PlanHashMismatch` |

Two more `reason`s reach the same `Verified` condition without being checks on
a signature at all. They are properties of the **referent** — the object
`spec.subjectRef` points at — and are kept in their own vocabulary because
"your cluster is missing an object" is not a verdict about anybody's approval:

| — | Referent problem | `reason` |
|---|---|---|
| — | `spec.subjectRef` names an object that does not exist in this namespace | `ReferentNotFound` |
| — | The referent exists and its KIND carries no `spec.planBytes` for check 7 to recompute a hash from — in tag 1 that is `subjectRef.kind: Backup` | `ReferentHasNoPlanBytes` |

**So `Verified`'s `reason` is one of FOURTEEN strings, and this is the one place
all fourteen are named**: `PayloadTypeMismatch`, `SignatureInvalid`,
`KeyIdNotInRoster`, `KeyIdExpired`, `KeyRetired`, `KeyRevoked`,
`KeyNotYetValid`, `TrustPolicyConflict`, `PlanHashMismatch`,
`SubjectKindMismatch`, `RosterNotFound` (install step 1, above),
`ReferentNotFound`, `ReferentHasNoPlanBytes` and `ReferentUidChanged`. A
fifteenth would be a compile error rather than a surprise: each reason is the
name of the enum variant that produced it, and both `match`es are wildcard-free
on purpose.

Four of those arrived with PLAT-19.1 and three of them say what `KeyIdExpired`
used to say badly. **Admission is a NEW use of a key** (decision D3 §7.4): an
`Approval` that arrives today carrying a signature by a key retired last week
is asking whether that key may authorise something *now*, which is a different
question from whether an archive it signed last year still verifies. A key that
may not is refused — and the refusal says which lifecycle event refused it,
because a retirement, an expiry, a revocation and a key staged for a rotation
that has not started are four different things to do something about. Reporting
a `KeyCompromise` revocation as an expiry would send an operator to extend a
`notAfter` instead of to an investigation.

`ReferentNotFound` is also the one reason that can be a RACE rather than a
problem: an `Approval` reconciled before its `Restore` exists reports it, and
the `Restore` reconciler (§12) reads that reason, holds for thirty seconds and
tries again rather than refusing — which is what makes minting both names
before creating either object workable.

Checks 1–6 validate the roster and signature; 7 and 8
are what bind the signature to a particular plan and a particular kind of
object. **A key outside the roster is `KeyIdNotInRoster`, never
`SignatureInvalid`** — the signature may verify perfectly; the signer is simply
not authorised, and an operator has to be able to see which of those two
happened.

**Check 5 reports the key that matched, never `signatures[0]`.** A sidecar
carrying two signatures — one by a rostered key and one by anyone else's —
verifies under exactly one of them, and only that one appears in
`status.matchedKeyId`.

**Check 7 is recomputed, never read from a status.** A status field is written
by a controller and is not part of anything anyone signed, so it could never
rescue an approval that binds a different plan. `Restore.status` carries no
plan hash at all.

**Check 9 is about what a reader SEES, and it carries check 7's reason because
it is check 7's fact.** `spec.planHash` is a plain CRD field beside the two
documents: nothing authorises anything on it -- checks 1-8 read the hash INSIDE
the signed bytes and recompute the referent's -- but it is the value
`kubectl get approval -o yaml` prints and the UI shows, and the field exists so
an operator can compare. Without check 9 an `Approval` could be `Verified=True`
while displaying a plan hash that is not the plan it authorises, which is the
one thing that field must never be able to do. The refusal names both hashes
and says both places must agree.

*Upgrade and rollback.* A controller carrying check 9 refuses an `Approval`
whose `spec.planHash` disagrees with the plan it signed; every producer in this
repository -- the UI, `scripts/demo-steps.sh`, `scripts/helm-demo.sh`,
`scripts/k8s-demo.sh` -- writes the `sha256:<hex>` the CLI printed, so a
correctly recorded approval is unaffected. An existing `Approval` that
disagreed and was verified by an older controller flips to `Verified=False`
with `PlanHashMismatch` on its next reconcile, and the `Restore` it names
HOLDS at `Pending`/`ApprovalNotVerified` rather than starting: fail-closed, and
no in-flight Job is touched, because a Restore's approval is re-read only
before its Job is created. Rolling the controller back restores the older
behaviour with no conversion, because nothing on the object changed.

**Check 8 exists because without it the second approval degenerates.** An
`Approval` whose `planHash` matched a `Restore` would be accepted for a
`Switchover`, and "a valid signature by a rostered key exists in this
namespace" is a property any approved restore in that namespace already
produced. `Switchover` is tag 2, so the check is cheap now and expensive to
retrofit — and its absence would be invisible until then. The subject kind is
part of the **signed bytes**: `logweir drill approve` writes
`"subject_kind": "Restore"` into `approval.json`.

In tag 1 `spec.subjectRef.kind: Backup` is refused with
`ReferentHasNoPlanBytes`: the `Backup` kind carries no `spec.planBytes` for
check 7 to recompute a hash from, and hashing something nobody signed is not an
alternative. Tag 1's approval flow is the restore path.

### `approvalBytes` and `sidecarBytes` are document text, never base64

They carry **the UTF-8 document text, verbatim**. Paste exactly what
`logweir drill approve` wrote. Neither field declares `format: byte`, the
controller passes `spec.approvalBytes` straight into the checks with no decode
step, and the hash is over exactly those bytes. A base64 layer between the
approver's file and the verified bytes is the class of transformation
`planBytes` exists to forbid.

### What lands on the status, and what does not

On success: `verified: true`, `matchedKeyId`, `approver`, `ticket`,
`selfAttestedRisk`, and a `Verified` condition. On a refusal: `verified: false`
and a `Verified` condition whose `reason` is the name from the table above and
whose `message` says what was compared — and **no** `approver` and **no**
`matchedKeyId`, because a name lifted out of bytes whose signature nobody
authorised is an attacker-controlled string on a field a UI renders.

`selfAttestedRisk` is `true` when the matched approver key id also appears in
`spec.signingKeys[].keyId`. It is **labelled, never refused**: `false` means
only "two different key ids", and one operator holding both keys satisfies it.

**The controller patches `/status` and nothing else.** It never patches a
`spec` — every `spec` in this group is sealed by the CEL rule of §7, and an
approval whose bytes a controller could edit is not an approval — and it never
deletes. **A refused `Approval` stays in the cluster**, as the audit trail of a
rejected attempt:

```bash
kubectl --context docker-desktop get approvals
kubectl --context docker-desktop get approval a1 \
  -o jsonpath='{.status.conditions[?(@.type=="Verified")].reason}'
```

Both reconcilers re-examine their objects periodically as well as on change,
because two of these verdicts are functions of the clock: a key that lapses
overnight must appear in `expiredKeyIds`, and an `Approval` refused with
`RosterNotFound` at 09:00 must stop saying so once the roster is installed at
09:05.

### What the controller does not claim

It verifies. It links the verifying half of the evidence machinery and never
the signing half, which is a **linkage** property and the whole of what is
claimed: the *capability* to sign is unbroken while the controller holds Job
CRUD over the signing key's namespace, and that residual is stated rather than
designed away. The controller reads no Secret, and its archive handle is
read-only.

## 9. Schedules and retention: Logweir deletes nothing

### A schedule's name has a 32-character budget

A `BackupSchedule` named `nightly` produces `Backup` objects called
`logweir-backup-nightly-20260909-030000`, where the tail is the UTC instant
the slot came due. The cap is **63 characters** — a *label value* limit and
not the 253-character name limit, because the runner Job's pods carry
`batch.kubernetes.io/job-name` as a label derived from this name — and the
fixed parts of the template take 31 of them: 15 for `logweir-backup-`, 1 for
the separator, 15 for the slot. **A schedule's own name therefore has a
32-character budget**: 32 fits exactly, and 33 is the first that does not.

This is not advice. A schedule whose name is longer produces no `Backup` at
all: the reconciler refuses to mint a name it cannot use, records a `Ready`
condition whose reason is `NameTooLong`, and says so in the message —

```bash
kubectl --context docker-desktop get backupschedule my-schedule \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}'
```

— rather than truncating, hashing or silently firing under a different name.
An object name that is a pure function of the trigger is what makes a
duplicate reconcile a 409 `AlreadyExists` instead of a second, partial archive
(guard **G-SLOT**), and a name that had to be shortened to fit would not be
that function any more.

### Editing a schedule's policy (PLAT-05.1)

**Every `spec` field is editable except `sourceRef`.** Change the cron
expression, the time zone, the topic list, the archive, the deadlines, the
catch-up policy, the retry policy, the concurrency policy, the retention rules
or `suspend` in place:

```bash
kubectl --context docker-desktop -n <namespace> \
  patch backupschedule nightly --type merge \
  -p '{"spec":{"schedule":"0 3 * * *","timeZone":"Europe/Berlin"}}'
```

`kubectl edit` and `kubectl apply` work too, which is why `logweir-operator`
grants `patch` beside `update`: both commands send a PATCH, and an
`update`-only grant could not perform the edit the CRD permits. Neither verb
widens what may change — the API server applies the CRD's CEL rules to every
subject, cluster-admin included.

**Re-pointing a schedule at a different destination IS now possible**, and it is
the edit PLAT-05.1 exists for: `spec.destinationRef` and `spec.archive` are both
editable, and the CEL sentinel keeps them consistent with each other. A run that
was frozen before the edit keeps its own snapshot — `scheduled_run` copies the
destination into the created `Backup`, `Backup.spec` is sealed whole, and
PLAT-06.1 freezes the resolved settings into an immutable ConfigMap before any
Job exists — so an edit reaches the next admission and **cannot** move a run
that is already going, its inputs or its Job.

One operator-facing consequence, because it is reachable by an edit and was not
before. Retention evaluates the **current** `archive.url`, so after a move the
old destination's sets stop being reported; and the open defect
**RET-WRONGBUCKET** (`docs/to-do/platform-improvements.md`, PLAT-16.1 — the
controller lists manifests through its single global store while rendering
removal commands for the schedule's own URL) is now reachable by editing a
schedule's destination, not only by creating a schedule on another bucket. The
wrong-bucket report can therefore appear **between** runs; it can never appear
mid-run, because the run that is going froze its destination before the edit
landed. PLAT-16.1 owns the fix.

Re-pointing a schedule at a different `KafkaCluster` is refused:

```
spec.sourceRef is immutable; create a new BackupSchedule to protect a different cluster
```

A schedule's identity is the cluster it protects, and one schedule's history
must not mix two clusters. Create a second schedule instead.

**An edit reaches the next admission and never a run that exists.** Each
`Backup` copies the policy at creation and records the revision it copied:

```bash
kubectl --context docker-desktop -n <namespace> \
  get backup logweir-backup-nightly-20260910-030000 \
  -o jsonpath='{.spec.scheduleRef}'
# {"generation":7,"name":"nightly","runPolicySha256":"sha256:…","uid":"3f0c…"}
```

and the schedule reports the revision in force:

```bash
kubectl --context docker-desktop -n <namespace> \
  get backupschedule nightly -o jsonpath='{.status.policy}'
# {"effectiveSince":"…","evaluatedAt":"…","generation":7,
#  "runPolicySha256":"sha256:…","timeZone":"UTC","tzdb":"chrono-tz 0.10.4"}
```

`runPolicySha256` digests **what a run does** — source, topic selection,
archive, run deadline — and excludes **when it runs**. So suspending and
resuming a schedule moves `metadata.generation` twice and leaves the digest
alone, while a topic-list edit moves both. `status.policy.evaluatedAt` is the
instant this status last MOVED, not the instant the controller last looked: the
reconciler deliberately writes nothing when the computed status equals the
stored one, so a staleness check reads `status.nextRuns[0].at` instead.

`status.policy.effectiveSince` is when the current revision was first observed.
A catch-up never runs a slot older than it — editing a schedule at noon does not
retroactively back up the morning under the new policy.

### An edit the schema accepts but the controller cannot run

Two classes of mistake are not expressible in CEL on the 1.29 floor: "exactly
one of a non-empty `topics` or `allUserTopics`" would strand every object
already stored with an empty list, and cron and time-zone validity are not
expressible at all. The controller is the gate for both, and it **fails
closed**:

| Edit | Where it is refused | What happens |
|---|---|---|
| Change `sourceRef` | API server (CEL), 422 | The object is unchanged |
| `allUserTopics` beside a non-empty `topics` | API server (CEL), 422 | Unchanged |
| `retry.maxRetries > 0` on a name longer than 29 characters | API server (CEL), 422 | Unchanged |
| A deadline out of range, a bad enum, a zone name that is not shaped like one, an exclusion that is not a legal topic name | API server (OpenAPI), 422 | Unchanged |
| An unparseable cron expression | Controller | `Ready=False`, reason `UnparseableSchedule` |
| A zone name this build's database does not have | Controller | `Ready=False`, reason `UnknownTimeZone` |
| `topics: []` with no `allUserTopics`, a glob metacharacter, a name that is not Kafka-legal | Controller | `Ready=False`, reason `InvalidTopicSelection` |
| Any other unusable run-policy field | Controller | `Ready=False`, reason `InvalidRunPolicy` |

In every controller row: **no slot, catch-up or retry is admitted**, a pending
reservation is released rather than turned into a run the `Backup` controller
would refuse, `Backup`s that are already running are untouched, and fixing the
spec resumes the schedule within one reconcile. There is no last-known-good
fallback — silently continuing an old policy after an edit would contradict
what the operator sees.

### Deleting a schedule keeps its history (PLAT-05.2)

**A `Backup` is not its schedule's dependent.** Since PLAT-05.2 a run the
schedule creates carries *no* ownerReference to it; membership is
`spec.scheduleRef {name, uid}` and the `logweir.dev/schedule-uid` label. So

```bash
kubectl --context docker-desktop delete backupschedule nightly
```

— with the default propagation, with `--cascade=foreground`, and with
`--cascade=orphan` — stops future admissions and **leaves every run, every
immutable plan ConfigMap, every Job (until its own 7-day TTL) and every archive
object in place**. Runs that are already frozen finish: their Jobs are owned by
the `Backup`, not by the schedule. A scheduled run that has *not* frozen yet
ends `Failed` with reason `ScheduleNotFound` — "deleting a schedule stops
future work" — and manual runs are unaffected either way.

There is no finalizer. A finalizer cannot stop foreground garbage collection of
`blockOwnerDeletion` dependents, and it would strand schedules after an
uninstall or a rollback.

### `HistoryRetained`, and the one window in which deletion is still unsafe

Runs created *before* this controller still carry the old ownerReference, and
the controller detaches them in the background. Until it has finished, the
default `kubectl delete` can still collect them. The schedule says which state
it is in:

```bash
kubectl --context docker-desktop get backupschedules -A \
  -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name} {.status.conditions[?(@.type=="HistoryRetained")].reason}{"\n"}{end}'
```

| `HistoryRetained` | Reason | What it means |
|---|---|---|
| `True` | `Retained` | No run is owned by this schedule. Delete it however you like. |
| `True` | `HistoryLarge` | The same, **and** the retained history is past the advisory below — prune. |
| `False` | `LegacyOwnerReferencesRemain` | Terminal runs are still being detached; the next passes finish it. |
| `False` | `ActiveLegacyRunsOwned` | The only owned runs left are still running. A pre-upgrade run keeps its ownerReference until it is terminal, because its scheduled identity derives from that UID. |
| `False` | `MigrationBlocked` | The API server refused a detach (`403`, `422`) or the run could not be detached safely. `status.history.migrationBlocked` names up to ten. |

`kubectl --context docker-desktop delete backupschedule <name> --cascade=orphan`
is always safe, before or after the migration, and it is what to use while the
condition says `False`.

The detach itself is one JSON merge `PATCH` per run, carrying that object's
`metadata.resourceVersion` as the update precondition. It removes only the
schedule's own controller entry — every other ownerReference, label and
annotation survives byte for byte — and adds
`logweir.dev/schedule-uid: <uid>` and
`logweir.dev/history-retained-from-owner: <uid>`, which is how a detached run is
still recognised as that schedule's. Progress is derived from observation, not
from a cursor: a controller that crashes between two patches leaves every object
either fully migrated or untouched, and the next inventory continues.

### Recreating a schedule under the same name

A recreated `nightly` has a **new UID**, so it sees none of the old runs as its
own: `status.history.runCount` counts only new runs, `concurrencyPolicy` ignores
old active runs, execution ids differ (`<uid>-<slot>`) and archive prefixes
cannot collide. List the two generations apart with

```bash
kubectl --context docker-desktop -n <ns> get backups -l logweir.dev/schedule-uid=<uid>
```

If the recreation lands inside a slot or retry window whose deterministic name
is still held by the old generation, that slot is recorded
`Ready=True reason=SlotNameUnavailable` with
`status.lastSlot.disposition: NameUnavailable`, and nothing is created under a
different name.

Recreation is now only for changing `sourceRef`: everything else is an edit
(see *Editing a schedule's policy*, above). The drain-and-replace procedure this
section used to carry is gone with the ownerReference that made it necessary.

### What retained history costs, and how to prune it

Nothing here is deleted by Logweir — see *Retention **reports***, below, and
the explicit rules under it. Retaining history is therefore an etcd cost you
choose:

Per retained run: the `Backup` CR is roughly 4–6 KiB (spec, status with three
or four conditions and evidence, managedFields), the plan ConfigMap roughly
2 KiB plus twice the topic-name bytes (the names appear both in `backup.yaml`
and in `execution-inputs.json`). Jobs and pods (about 11 KiB per run, two Jobs
for a dynamically-selected run) are bounded by the 7-day TTL.

| Schedule | Per run retained | Runs/year | etcd growth/year | TTL-window Jobs/pods |
|---|---|---|---|---|
| daily, 20 named topics | ≈ 8 KiB | 365 | ≈ 3 MiB | ≈ 0.08 MiB |
| hourly, 20 named topics | ≈ 8 KiB | 8 760 | ≈ 70 MiB | ≈ 1.8 MiB |
| every 15 min, dynamic, 1 000 topics × 30 bytes | ≈ 79 KiB | 35 040 | ≈ 2.6 GiB | ≈ 14 MiB |

The last row exceeds a default etcd quota inside a year, so pruning is
**required** for high-frequency dynamic schedules. The controller publishes what
it measured in `status.history {runCount, runCountCapped, estimatedBytes}` and
raises `HistoryRetained=True reason=HistoryLarge` above 2 000 runs or 64 MiB per
schedule. (`runCountCapped: true` means the inventory stopped at its page bound
and the two numbers are floors, not totals.)

**The cleanup rules, in full. Logweir applies none of them for you.**

1. No Logweir component deletes a `Backup`, a ConfigMap or an archive object.
   The controller ClusterRole carries no `delete` verb on any resource.
2. You may delete **terminal** `Backup` CRs with your own RBAC. Deleting one
   also removes its plan ConfigMap (the frozen inputs) and any remaining Job,
   and removes the run from Kubernetes-backed history and from restore
   selection until PLAT-15.1's catalog-backed discovery lands. The archive data
   and the signed receipts stay in object storage — record
   `status.backupId`, `status.evidence.receiptKey` and `sidecarKey` first.
3. **Never delete a nonterminal `Backup`.** Its Job is collected with it,
   mid-run: `NoExitCode` and a partial archive.
4. Keep, per schedule UID, at least the newest `Succeeded` run whose evidence
   verification is `Valid`, every run newer than
   `max(startingDeadlineSeconds, maxRetries × delaySeconds + activeDeadlineSeconds)`,
   and any run whose `backupId` appears in a nonterminal `Restore` plan.
5. Select by label, filter terminal runs locally, then delete by name:

```bash
kubectl --context docker-desktop -n <ns> get backups \
  -l logweir.dev/schedule-uid=<uid> -o json \
  | jq -r '.items[]
           | select(.status.phase == "Succeeded" or .status.phase == "Failed")
           | select(.metadata.creationTimestamp < "2026-01-01T00:00:00Z")
           | .metadata.name' \
  | xargs -r -n1 kubectl --context docker-desktop -n <ns> delete backup
```

6. A pruned current-slot run is never re-run under the same execution id: the
   scheduler's `S <= lastFireTime` guard reports `AlreadyFired` instead.

### What a schedule costs to read, now that it keeps everything

Listing every `Backup` on every reconcile stops being affordable once history is
retained, so the controller does not. Per reconcile it `GET`s the ≤ 10 names in
`status.activeRuns`, the ≤ 1 `status.pendingRun` name and the ≤ 4 deterministic
names of the latest slot — at most 15 reads, O(active) and independent of how
much history exists.

A full **inventory** is taken instead: a paginated list (`limit=500`, a bounded
number of pages) filtered by `logweir.dev/schedule-uid` once the migration is
done, and namespace-wide while it is not, because a pre-upgrade object carries
no UID label. It runs when `status.history` is absent, when `status.activeRuns`
is, while there is migration work a pass can do, and otherwise once an hour —
`status.history.inventoriedAt` records when. That hourly pass is the one thing
that moves an otherwise settled schedule's status: 24 writes a day, against the
2 880 a rewrite-on-every-reconcile would make.

Admission never depends on how fresh that list is. Every schedule-created run is
recorded in `status.pendingRun`/`status.activeRuns` by a
resourceVersion-conditional status write *before* the run is created, so the
reservation — not the list — is what makes a duplicate reconcile impossible.

### Upgrading to, and rolling back from, retained history

**Upgrade:** apply the CRD, roll the controller, then wait for
`HistoryRetained=True` on every schedule (the `jsonpath` above) before deleting
any schedule without `--cascade=orphan`. Nothing is rewritten: a pre-upgrade
`Backup` keeps its ownerReference until the controller detaches it, and keeps
every other field forever.

**Rollback (controller only, never the CRD):** an older controller identifies a
schedule's children only by ownerReference, so for runs that have been detached
it neither counts them for `Forbid` — two runs of one schedule can then overlap
— nor shows them as history; and it creates *owned* runs again, so the
garbage-collection hazard returns for those new runs only. Migrated history
stays retained whatever happens. Before rolling back, suspend any schedule whose
overlap matters. After rolling forward, the migration picks up whatever the old
controller owned.

**PLAT-15.1** indexes recovery points from object storage by execution id, so a
schedule's history view becomes "CR history (by `schedule-uid` label) ∪ catalog
points attributed to that schedule UID", deduplicated by execution id. Pruning
CRs then stops removing discoverability. A manual run's execution id is its own
`Backup` UID and encodes no schedule, so attributing one needs origin metadata
the receipt does not carry yet.

### Upgrading to, and rolling back from, the editable schedule

**Apply the CRD before rolling the controller**, in that order, and never
downgrade the CRD on its own. The three CEL rules are additive and no stored
object can fail them, so applying them changes nothing about what is installed;
the controller rollout is what starts writing `status.observedGeneration` and
`status.policy`. There is one stored version, no conversion webhook and no
object rewrite: a `Backup` created before PLAT-05.1 simply has no
`scheduleRef.uid`/`generation`/`runPolicySha256`, and a console reads that as
"revision not recorded (created before PLAT-05.1)".

**Rolling back means rolling back the controller, not the CRD.** An older
controller running against the new CRD honours edits of `schedule`, `topics` and
`archive` naturally — it copies them at creation — but records no revision on
the runs it creates, ignores the fields it does not declare, and would copy
`topics: []` for a dynamically-selected schedule, whose runs then fail safe at
the runner (exit 3) rather than backing up nothing silently.

Re-applying the OLD CRD would re-seal the spec and prune the new fields from
every API response. If a CRD downgrade is unavoidable, first remove `timeZone`,
`retry`, `catchUpPolicy`, the two deadline fields and `allUserTopics` from every
schedule and record them somewhere you can re-apply them from; suspend any
schedule whose behaviour depended on them before the swap.

### Concurrency policy uses owned Backup state

`spec.concurrencyPolicy` controls whether different scheduled slots may run at
the same time. The field has two values: `Forbid` (recommended and default) and
`Allow` (explicit opt-in). An existing `BackupSchedule` stored before the field
was added behaves as `Forbid` without an object rewrite. Since PLAT-05.1 the
field is editable in place, so moving an old omitted-field schedule to `Allow`
is a one-field patch and no longer needs a replacement object.

Under `Forbid`, **no** nonterminal schedule-created run of this schedule may
overlap a new admission. Under `Allow`, up to **ten** may; the eleventh is
refused with `Ready=True reason=ActiveRunLimit` and the ten are named in
`status.activeRuns`, so a schedule whose runs outlast its period is visible
rather than silently unbounded. An absent phase, an unknown phase, and a
nonterminal Backup whose Job is missing all remain active conservatively; the
Backup controller may still create or recreate that Job. Terminal `Succeeded`,
`Failed` and legacy `Refused` runs do not block. **Manual runs never block and
are never blocked** — they are not schedule-created.

Admission is a resource-version-checked status reservation **for every trigger
kind and both policies**, so two controller replicas cannot admit different
slots from the same schedule state and every run has the same crash semantics.
`status.pendingRun {name, slot, attempt, kind, generation}` identifies accepted
work between the reservation and the child's creation, and
`status.pendingBackupRef` mirrors its name for readers written before it
existed; a restart resumes that deterministic child. Once the child is observed
or created, the controller clears both spellings and reports the run in
`status.activeRuns` (with `status.activeBackupRef` mirroring the first entry).
This uses no separate Kubernetes Lease.

### Cadence: zones, deadlines, catch-up and retries

`spec.schedule` is the single source of truth for cadence — there is no stored
preset — and its five fields are read in `spec.timeZone`.

- **Absent `timeZone` means UTC**, and reproduces the slots an older controller
  computed instant for instant. A **slot is always the UTC instant**, spelled
  `yyyymmdd-hhmmss`, so object names stay unique, monotonic and DNS-1123
  whatever the zone. The run records the zone it was computed in
  (`spec.trigger.timeZone`), so a history row keeps its local time after
  somebody edits the schedule.
- **DST:** every real instant whose local wall time matches fires once; a
  matching local time that does NOT exist (a spring-forward gap) fires once at
  the end of the gap. A fixed local time inside a repeated autumn hour therefore
  fires at **both** occurrences — `status.nextRuns` marks them
  `RepeatedLocalTimeFirst` / `RepeatedLocalTimeSecond`, and a shifted gap match
  `NonexistentLocalTimeShifted`, so the outcome is previewed rather than
  surprising. An interval schedule (`*/15 * * * *`) keeps its UTC cadence
  through both transitions with no burst and no hole.
- **A zone this build's database does not have** is `Ready=False` with reason
  `UnknownTimeZone` and admits nothing. It is never read as UTC.
  `status.policy.tzdb` names the database that resolved it.
- `status.nextRuns` carries the next **five** firings, computed by the
  controller. The browser never evaluates cron.

**Only the latest due slot is ever eligible.** Older ones are counted in
`status.missedSlots {count, countCapped, lastEvaluatedSlot, recent}` — the
enumeration is capped at 1000 slots per evaluation, and `countCapped` says when
a count is a floor. `lastEvaluatedSlot` advances only when the current slot
reaches a disposition it cannot come back from, so a slot that waits for
concurrency and is then superseded is counted exactly once.

**Retries** are opt-in (`spec.retry {maxRetries 0..3, delaySeconds}`), only for
**retryable** failures, only for the latest due slot, and only until the next
slot comes due. A retry is a new `Backup` named `…-<slot>-r<k>` with a new
execution id — a failed attempt may have written part of an archive, and
reusing its `backup_id` would append into that partial prefix. The attempt chain
is discovered by GETTING those deterministic names, never by listing.

| Terminal record | Retried? |
|---|---|
| `exitCode: 1` (`operational`, no artifact) | **yes** |
| an exit code outside `0..=4` — 137/143 after the Job deadline, OOM | **yes** |
| no exit code: `DisruptedMidDrill`, `PodUnschedulable`, `NoExitCode`, `DiscoveryFailed` | **yes** |
| `exitCode: 2` (not a pass), `3` (refused by a guard), `4` (signing or lock) | no |
| any controller refusal made before the POST | no |
| anything else, including a terminal state this build does not know | no |

The classification is an **allowlist**: a state nobody has classified falls to
"not retried", because a decision the product already made is not a blip. A
terminal run with no terminal condition has no instant to measure a delay from
and is likewise never retried.

### Manual runs, and what may sit on a slot's name

A **manual** run (`spec.trigger.kind: Manual`, "Back up now") is part of its
schedule's history — it carries `spec.scheduleRef {name, uid}` and appears in
the schedule's history view — and it is **not** a schedule-created run. Only
`Scheduled`, `CatchUp` and `Retry` participate in `concurrencyPolicy`: a manual
run neither occupies a `Forbid` slot nor is blocked by one, and it never appears
in `status.activeRuns`. That follows the CronJob "run now" precedent, and it is
what makes "Back up now" usable on a schedule whose nightly run is still going.

Membership and accounting are therefore two different questions asked of the
same object, and the code asks them with two different predicates. Anything that
counts runs for `concurrencyPolicy` or reports them in `status.activeRuns` adds
the trigger-kind clause; anything that asks "is this part of this schedule's
history" — the PLAT-05.2 inventory, the ownerReference migration — does not.

The naming rule constrains **scheduled** names only, so nothing in the API
refuses a manual `Backup` called `logweir-backup-<schedule>-<slot>`. The
scheduler discovers a slot's attempts by GETTING exactly those names, so it has
to say what such an object means, and it says **foreign occupant**: a manual run
executes under its own UID and therefore its own archive id, so reading it as
attempt 0 would report a window as covered by a run that covered a different
one. The slot is reported `Ready=True reason=SlotNameUnavailable`, recorded in
`status.lastMissedSlot` and `status.lastSlot.disposition: NameUnavailable`, and
never re-run under a different name — the same answer an object belonging to
another schedule gets, for the same reason.

### The policy truth table

Evaluation is top to bottom; the first matching row decides. "Blocked" means
`Forbid` with any nonterminal schedule-created run, or `Allow` with ten.

| # | Observed state | Creates | `Ready` reason / `lastSlot.disposition` |
|---|---|---|---|
| 1 | Accepted reservation, child absent, run policy valid | the reserved name, under the generation the controller can read now | `Scheduled`/`CaughtUp`/`RetryScheduled` |
| 2 | Accepted reservation, child absent, run policy invalid | nothing; the reservation is released | `InvalidRunPolicy` / `Released` |
| 3 | `suspend: true` | nothing; `nextRuns` empty | `Suspended` (False) |
| 4 | Cron, zone, selection or run policy invalid | nothing; running work continues | `UnparseableSchedule` / `UnknownTimeZone` / `InvalidTopicSelection` / `InvalidRunPolicy` (all False) |
| 5 | No due slot inside the walk bound | nothing | `NoDueSlot` (False) |
| 6 | The slot's deterministic name is held by an object that is not a scheduled run of this schedule | nothing | `SlotNameUnavailable` / `NameUnavailable` |
| 7 | Some attempt of S succeeded | nothing | `Scheduled` / `Admitted` |
| 8 | The highest attempt of S is nonterminal | nothing | `Scheduled` |
| 9 | The highest attempt failed, not retryable | nothing | `RunFailed` / `Failed` |
| 10 | Failed retryably, attempt ≥ `maxRetries` | nothing | `RetryExhausted` / `Exhausted` (`RunFailed` when `spec.retry` is absent) |
| 11 | Failed retryably, the delay has not elapsed | nothing | `RetryPending` |
| 12 | Failed retryably, delay elapsed, blocked | nothing | `RetryBlocked` |
| 13 | Failed retryably, delay elapsed, not blocked | `…-r<k+1>`, kind `Retry` | `RetryScheduled` / `Retried` |
| 14 | No attempt, `S ≤ status.lastFireTime` (history pruned) | nothing | `Scheduled` |
| 15 | No attempt, the name would not fit | nothing | `NameTooLong` (False) |
| 16 | No attempt, inside `startingDeadlineSeconds`, blocked | nothing | `ConcurrencyBlocked` / `Blocked` (or `ActiveRunLimit` under `Allow`) |
| 17 | No attempt, inside the deadline, not blocked | `name(S,0)`, kind `Scheduled` | `Scheduled` / `Admitted` |
| 18 | No attempt, past the deadline, `catchUpPolicy: None` | nothing; counted in `missedSlots` | `SlotMissed` / `Missed` |
| 19 | No attempt, past the deadline, `Latest`, `S` older than `status.policy.effectiveSince` | nothing | `SlotMissed`, missed reason `BeforeRevision` |
| 20 | No attempt, past the deadline, `Latest`, blocked | nothing | `CatchUpBlocked` / `Blocked` |
| 21 | No attempt, past the deadline, `Latest`, not blocked | `name(S,0)`, kind `CatchUp` | `CaughtUp` / `CaughtUp` |
| 22 | A newer slot comes due while 11, 12, 16 or 20 wait | per the new slot | per the new slot; the old one is counted once |

What an operator can predict from it: a controller down for a week with
`catchUpPolicy: None` runs **nothing** until the next slot and records the
skipped count; with `Latest` it runs **exactly one** `CatchUp`, for the most
recent slot only. Per schedule there is at most one admission per reconcile,
only for the latest due slot, at most `1 + maxRetries ≤ 4` runs per slot, at
most one nonterminal schedule-created run under `Forbid` and ten under `Allow`.

Bounded(N) catch-up was rejected: `logweir backup run` captures what the broker
retains **when it runs**, so N catch-up runs executed back to back produce
near-identical archives at N times the broker and storage load, with no
recovery-point benefit.

Apply the regenerated CRD before starting the new controller. For a strict
no-overlap upgrade, suspend schedules or stop the old controller before the
rollout: an old and new replica briefly running together do not share the new
reservation protocol. No schedule rewrite is needed, and existing Backup
children remain the run-state authority.

After the updated CRD is installed, an older controller ignores the additive
fields — it fires in UTC, never catches up and never retries — and does not
enforce cross-slot `Forbid` against runs it cannot see. It also clears a
`-r<k>` reservation as unparseable, which is safe: no retry is created. Suspend
schedules that set `timeZone`, `retry`, `catchUpPolicy` or the deadline fields
before rolling back, and remove neither accepted Backup children nor their
pending reservation during the handoff.

### A slot past its starting deadline is skipped, and the skip is recorded

The controller has no timer and no leader lease: it re-examines every schedule
every 30 seconds (sooner when a retry delay expires first), works out which slot
is due, and reserves before it creates. A slot that came due more than
`spec.startingDeadlineSeconds` before the controller looked is **skipped**,
unless `catchUpPolicy: Latest` lets the most recent one still run. **That field
is absent on every schedule stored before it existed, and absent means 3600** —
so an unconfigured schedule still skips a slot that came due more than **one
hour** before the controller looked, byte for byte the horizon an older
controller hard-coded. A controller restarted after a week must not fire six
days of backlog, because a `Backup` for a window nobody is waiting for costs the
same broker read as one somebody is.

A skip is a fact, not a silence. It lands in `status.missedSlots` with a `Ready`
condition whose reason is `SlotMissed`, and in the older single-valued
`status.lastMissedSlot`, which is never cleared — it is the audit trail of the
skip:

```bash
kubectl --context docker-desktop get backupschedule nightly \
  -o jsonpath='{.status.missedSlots}'
kubectl --context docker-desktop get backupschedule nightly \
  -o jsonpath='{.status.lastMissedSlot}'
```

`kubectl explain backupschedule.spec.startingDeadlineSeconds` states the
default, and `kubectl explain backupschedule.status.lastMissedSlot` states the
one-hour horizon it reproduces, so both are discoverable from the cluster and
not only from this page.

### Retention **reports**. It never deletes

`spec.retention{keepLast, keepDays}` is evaluated by the controller on every
reconcile against the manifests it lists, and the result goes into
`status.retentionReport`: which sets it would keep, which sets it **would**
remove and why, and **the exact commands an operator would run**, rendered as
strings.

```bash
kubectl --context docker-desktop get backupschedule nightly \
  -o jsonpath='{.status.retentionReport.awsCli[*]}'
# aws s3 rm 's3://kafka-backups/mvp-demo/backup-001/' --recursive
```

**Logweir prints them. An operator runs them.** Nothing in the report was
deleted, and **no Logweir component in tag 1 holds any delete capability
against object storage** — the controller's archive handle is built with the
read-only constructor, which refuses every write before it checks anything
else, and guard **G-RET** (`scripts/check-no-archive-write.sh`, in `just
lint`) fails the build if a source file in the control plane names the
writable constructor or an object-store put, or names a delete on a
store-shaped receiver, or if a source file in the store crate names an
object-store delete at all or defines a `delete` method. That gate reads
source text, so it is a tripwire on the spellings a delete would be written
in and not a proof: a raw HTTP `DELETE` issued through some crate that is not
in the dependency graph would pass it, and what actually makes that
unwritable is the type — `Store` exposes no delete method and its inner
object-store handle is private — together with Global Constraint 38, which
closes the graph. The adopter's own bucket lifecycle policy does the deleting.
Retention never touches a Kafka topic, in any tag.

#### The version-scoped form of that claim (ADR 0008 Amendment H)

Amendment H extends Global Constraint 6 with one narrow exception: *a
separately linked, separately credentialed, optional retention worker may
delete objects under an explicitly configured archive prefix, never under
`logweir/`, only from an administrator-approved plan, and only with an
attributable signed record.* The sentence above therefore becomes
version-scoped: it holds wherever `RetentionPolicy.mode != Enforce`.

**In this build it holds unqualified**, because the worker does not exist yet.
`RetentionPolicy` ships as a shape: `mode` defaults to `Report`, which
evaluates and reports exactly as `spec.retention` does, and
`status.enforcement` reads `RecommendationOnly`. `ExternalLifecycle` is a
DECLARATION — it records that a bucket lifecycle rule exists so a console can
stop claiming retention is unenforced, and `status.guarantees` marks it
`ProviderEnforcedUnverified`, because Logweir does not read the provider's rule
and will not claim it is in force. `logweir-store` stays delete-free and the
control plane stays delete-free in every mode.

The CRD's own rails, at admission rather than at 04:17 in a Job log:
`spec.scope.prefix` may never be empty and may never name `logweir/`, the three
fields that decide *where* deletion could happen are immutable, and `mode` and
its block travel together in both directions — a delete-capable credential
configured under a mode that never deletes is a credential mounted for nothing.

The rendered commands are **shell-quoted**. A backup id, bucket or prefix is
whatever the archive's own keys say it is, and an S3 key may legally contain a
space, a `$`, a backtick, a newline — or a `;`. Every interpolated value is
therefore rendered as one single-quoted POSIX shell word, so the command you
paste acts on exactly one path and a key like `a;rm -rf ~` is a path and not a
second command.

An unreadable manifest under the prefix is **skipped, not fatal**. A sibling
JSON object the `manifest.json` filter picked up used to cost the whole
report; it now lands in `status.retentionReport.skipped` with the key and the
reason, and every other backup set is still evaluated:

```bash
kubectl --context docker-desktop get backupschedule nightly \
  -o jsonpath='{.status.retentionReport.skipped[*].key}'
```

A skipped set is in neither `setsKept` nor `setsThatWouldBeRemoved`, and ranks
are counted over the sets that were read — so a partly unreadable archive
under-reports what would be removed and never over-reports it.

The report is a union of the two rules, and it says which one applies:
`reason` is `BeyondKeepLast` with the set's `rank`, or `OlderThanKeepDays`
with the configured `days`. When both rules select one set the report says
`OlderThanKeepDays`, because a set that is too old stays too old however the
count changes. With neither field set, `note` reads *no retention rule is
configured; nothing would be removed* and nothing is listed as removable.

**The controller reads the archive only if you tell it where the archive is.**
The one read-only handle is built once, at controller start, from the
environment variable **`LOGWEIR_ARCHIVE_URL`** (`s3://…`, `gs://…`, `az://…`
or `file://…`; region, endpoint and credentials come from the object store's
own environment chain). With it unset the controller holds no archive handle,
writes no `retentionReport` at all, and logs one line saying so — schedules
still fire, because a report is not a backup. An absent `retentionReport`
block therefore means *no evaluation has happened*; an empty
`setsThatWouldBeRemoved` means *the evaluation found nothing to remove*, and
they are different answers.

#### And the report is withheld when it would be about another bucket

That one handle is the controller's, built from `LOGWEIR_ARCHIVE_URL`. A
schedule's `archive.url` is the schedule's. On an installation where those name
**different buckets**, listing through the handle while rendering `aws s3 rm`
commands for the schedule's own URL produces a report about bucket A printed as
though it described bucket B — with commands naming keys in B that were listed in
A. An operator who runs them deletes the wrong objects, or nothing; either way
the report was never about their catalogue.

So the report is evaluated only when the schedule's bucket equals the handle's.
Otherwise it is omitted and one INFO line names both buckets. A different
**prefix** in the same bucket still reports, because the listing is prefix-scoped
by the report itself; a different bucket cannot be.

**A destination-backed schedule gets no retention report at all** through this
path, even at the same bucket: the global handle's credential is not the
destination's, and a report produced with the wrong principal under-reports
whatever that principal cannot list. The per-destination replacement is the
archive-inventory check kind, which is not in this build. An absent
`retentionReport` on a destination-backed schedule is therefore the documented
behaviour and not a failure.

## 10. The exit-code contract, as the `Backup` reconciler makes it visible

§1 says the exit code is nearly invisible in Kubernetes and gives you the two
lines that keep it readable. This section is the other half: what the control
plane does with the code once it can read it, and where you look for it.

### The table

`weirkeeper`'s `Backup` reconciler lifts the code off the one path that carries
it and writes it to **`Backup.status.exitCode`**, together with a wire reason on
`status.exitReason` and a condition. One row per Global-Constraint-11 code:

| Exit | `status.phase` | `status.exitReason` | Condition | What it means |
|---|---|---|---|---|
| **0** | `Succeeded` | `ok` | `Complete=True`, reason `Ok` | The archive was captured and the receipt was signed. |
| **1** | `Failed` | `operational` | `Failed=True`, reason `Operational` | The run could not be attempted or continued. **No artifact was written.** |
| **2** | `Failed` | `drill-not-pass` | `Failed=True`, reason `DrillNotPass` | A result that is not a pass — **a document WAS written and signed.** Not produced by `backup run`; it is the drill path's code and the row is here because `exitReason`'s vocabulary is one vocabulary across both paths. |
| **3** | `Failed` | the terminal state off the log's `refusal-reason=` line, or `GuardRefusedUnknownReason` | `Failed=True`, reason `GuardRefused` | A guard refused before anything ran. |
| **4** | `Failed` | `signing-or-lock`, or `OrphanedScorecard` | `Failed=True`, reason `SigningOrLock` | Signing or the lock proof failed and **nothing was uploaded**. |
| *(absent)* | `Failed` | `operational` | `Failed=True`, reason `DisruptedMidDrill` / `PodUnschedulable` / `NoExitCode` | The Job finished and no container named `runner` reported a terminated state. See "the crashed Job" below. |
| *(absent)* | `Failed` | `operational` | `Failed=True`, reason `NameTooLong` | The `Backup`'s own name is longer than 63 characters, so **nothing was created**. See below. |
| *(absent)* | `Failed` | `operational` | `Failed=True`, reason `ExecutionSpecInvalid` | The typed spec states no runnable run identity (see "Manual backups" below), so **nothing was created**. |
| *(absent)* | `Failed` | `operational` | `Failed=True`, reason `PlanConfigMapConflict` | An object already holds the plan name and is not this run's frozen inputs. Nothing was created, and it was not rewritten. |
| *(absent)* | `Failed` | `operational` | `Failed=True`, reason `JobNameConflict` | A Job already holds this `Backup`'s name and is not controlled by it. It was neither observed nor adopted. |

**TWO VOCABULARIES, AND EACH STAYS IN ITS OWN FIELD.** `exitReason`'s five wire
values are lowercase-hyphenated (`ok`, `operational`, `drill-not-pass`,
`guard-refused`, `signing-or-lock`) — the same spellings `logweir drill`'s own
outcome strings use — and it also carries the CamelCase terminal states, which
are the more specific answer when there is one. A **condition `reason` is
always CamelCase**: `metav1.Condition`'s upstream validation pattern admits a
leading letter and then only letters, digits, `_`, `,` and `:` — it **forbids
`-`** — so a hyphenated reason is a value the API server rejects the day this
CRD's hand-rolled condition schema is given that pattern. Nothing accepts both
spellings of either:
`the_two_reason_vocabularies_never_overlap` asserts the sets are disjoint and
`the_condition_reasons_are_valid_metav1_reasons` walks every reason this
controller can write against that regex.

Reading it back:

```bash
kubectl --context docker-desktop get backups
kubectl --context docker-desktop get backup <name> \
  -o jsonpath='{.status.exitCode} {.status.exitReason}{"\n"}'
```

The `EXIT` printer column is why `kubectl get backups` is worth running at all:
without it, §1's problem is unchanged and exit 2 still looks like exit 1.

### The Job the controller generates

One Job per `Backup`, **named after the `Backup` object verbatim** — not
`logweir-backup-<name>`, because a scheduled `Backup` is already called
`logweir-backup-<schedule>-<slot>` and re-prefixing would push the
`batch.kubernetes.io/job-name` label past its 63-character cap for any schedule
name of 18 characters or more. That label is how the pod carrying the exit code
is found, so a name that overflows it loses the code.

The shape, which is `examples/cronjob-drill.yaml`'s shape with a controller
behind it:

```yaml
spec:
  backoffLimit: 0                    # exactly ONE pod
  activeDeadlineSeconds: <spec.deadlineSeconds>
  podFailurePolicy:
    rules:
      - onPodConditions: [{type: DisruptionTarget, status: "True"}]
        action: FailJob
      - onExitCodes: {containerName: runner, operator: In, values: [2, 3, 4]}
        action: FailJob
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: logweir-runner
      automountServiceAccountToken: false
      securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532}
      containers:
        - name: runner                # ALWAYS this name
          imagePullPolicy: Never
```

Four things about it are worth knowing before you debug one:

- **There is no `Ignore` rule on exit 1.** The obvious-looking third rule ("code
  1 was operational, retry it") needs `backoffLimit > 0`, which yields several
  pods while `status.exitCode` is a single value. Two pods with two codes and
  one field is a status that is either wrong or arbitrary.
- **`automountServiceAccountToken: false`, and the ServiceAccount is still
  named.** The runner makes zero Kubernetes API calls, and it is the component
  that holds the signing key — so it gets no token. The *name* still matters:
  with none set, a pod silently gets `default`, which is the account most likely
  to have been granted something.
- **`fsGroup: 65532` is what makes the signing key readable**, not the `0440`
  mode beside it. Kubelet writes Secret files `root:root`; without `fsGroup` the
  container reads nothing through the owner bits and the run exits 1 having done
  nothing. All four combinations were run live (§3).
- **No `ttlSecondsAfterFinished` at creation time.** The controller patches one
  on (7 days) **only after** the status patch carrying the exit code has
  returned 200. The TTL controller deletes the Job *and its pods*, and the code
  lives on the pod, so a TTL that existed earlier would be a race pod garbage
  collection can win.

### Manual backups: the typed contract, and no annotation anywhere

A `Backup` is an ordinary object. This is the whole of what a person, a script
or the UI has to create for a run to happen:

```yaml
apiVersion: logweir.dev/v1alpha1
kind: Backup
metadata:
  name: nightly-catchup          # any DNS-1123 name of 63 characters or fewer
  namespace: <your namespace>
spec:
  sourceRef: { name: source }    # a KafkaCluster in this namespace
  topics: [orders, payments]     # a NAMED allowlist; a wildcard is refused
  archive:
    url: s3://kafka-backups/logweir
    secretRef: { name: logweir-s3 }
  triggeredBy: manual
  deadlineSeconds: 3600
```

`config/samples/backup.yaml` is that object. **No annotation is required and
none is read.** Controllers before this contract executed the JSON argv array
on `logweir.dev/runner-argv`, which meant a manual `Backup` without it could
not run at all, and anyone who could annotate a `Backup` could change the
subcommand, the spec path, the signing key path or the archive prefix of a run
holding the signing key.

**The run identity is the control plane's, not the client's.** It is derived
from the object and nothing else — no clock, no status field, no annotation —
by one function, so the name a schedule mints and the identity the reconciler
executes cannot disagree.

`spec.trigger.kind` is the finer trigger and is what identity is derived from;
`spec.triggeredBy` is unchanged, is still `manual` or `schedule`, and is still
what the signed receipt carries. The two must agree, or the run is refused: a
manual run may not sign a receipt saying `schedule`.

| `spec.trigger.kind` | `triggeredBy` | What must also be true | `metadata.name` | The run identity (`status.execution.id`, the archive `backup_id`) |
|---|---|---|---|---|
| `Scheduled` | `schedule` | `spec.scheduleRef` with a `uid` **or** a `BackupSchedule` controller owner reference of the same name; `spec.slot` a real UTC instant `yyyymmdd-hhmmss`; `attempt: 0` | `logweir-backup-<schedule>-<slot>` | `<BackupSchedule UID>-<slot>` |
| `CatchUp` | `schedule` | the same — **it is slot S, started late** | the same as `Scheduled` | the same as `Scheduled` |
| `Retry` | `schedule` | `attempt` 1–3 and `trigger.retryOf.name` equal to the previous attempt's name | `logweir-backup-<schedule>-<slot>-r<k>` | `<BackupSchedule UID>-<slot>-r<k>` |
| `Manual` | `manual` | no `spec.slot`, no `attempt` | any DNS-1123 name; the API mints `logweir-manual-<26 base32>` | this object's **API-server UID** |
| *absent* | `schedule` | as `Scheduled`/attempt 0 — every `Backup` created before `spec.trigger` existed | as `Scheduled` | as `Scheduled` |
| *absent* | `manual` | as `Manual` | any | as `Manual` |

A **catch-up shares the scheduled run's identity** because it is the same slot:
giving it one of its own would let a controller restart write a second archive
of one window. A **retry does not**, because the attempt it retries may have
written part of an archive, and re-using that `backup_id` would append into the
partial prefix.

Two further checks run before anything is created:

- **The schedule must exist, for a scheduled kind, and only before the freeze.**
  A `BackupSchedule` of that name, in this namespace, with that UID — otherwise
  terminal `ScheduleNotFound`. A schedule deleted and recreated under the same
  name is a *different* schedule and does not adopt the run. Once
  `status.execution` is recorded nothing is re-checked: a frozen run executes
  the policy it copied, and deleting its schedule mid-run does not stop it.
  **A manual run never requires the schedule**, even when it copied one.
- **A copied run policy digest must be the one this object's own fields
  produce.** `spec.scheduleRef.runPolicySha256` is recomputed from the `Backup`
  itself; a mismatch is terminal `RunPolicyDigestMismatch`. This is an integrity
  check against control-plane bugs and **not** a security boundary: a subject
  who can create `Backup`s in a namespace can already run any policy there.

Anything else is terminal before anything is created:
`ScheduledIdentityMismatch` when the object's own fields do not compose the run
it claims to be — a `manual` Backup naming a slot, a `Retry` at attempt 0, a
scheduled object under a name its trigger does not compose, a `spec.slot` that
is fifteen digits and not a date, a scheduled kind with no UID anywhere — and
`ExecutionSpecInvalid` for what is wrong with the spec as an execution request,
such as an unknown `triggeredBy` or a non-positive `deadlineSeconds`. A
hand-written object may not claim `schedule`: its signed receipt would say
`schedule` about a run no schedule created.

**Topic selection.** `spec.topics` is a mandatory named allowlist; a glob
metacharacter in it is terminal `GuardRefused`. `spec.allUserTopics` beside a
non-empty `spec.topics` is two answers to one question and is refused by
admission and by the controller alike (`InvalidTopicSelection`). `topics: []`
with `spec.allUserTopics` is the **dynamic** shape, resolved per run by a
topic discovery Job — see "Dynamic selection" below. What never happens in
either shape is an empty `source.topics` in `backup.yaml`: that is the "no
allowlist means everything" shape the mandatory allowlist exists to make
impossible, and the freeze boundary refuses an empty or patterned resolved list
whatever produced it.

**`status.selection` is written at the freeze**, in the same patch as
`status.execution`, and says what the run may honestly claim to have covered:

```json
{"mode":"SelectedTopics","coverage":"NamedTopics","resolvedTopicCount":2,"resolvedTopicBytes":14}
```

Only `coverage: AllUserTopicsAttested` may ever be rendered as "all topics".
A named allowlist is `NamedTopics` and claims nothing about the cluster. The
block is **absent** on a `Backup` frozen by a controller that predates it,
which is the documented absent-field behaviour and not a degraded state.

### Dynamic selection: one discovery Job per run

`spec.allUserTopics` means "every user topic this run's principal can see,
minus the exclusions". It is resolved **per run**, in the run's own Job, and
frozen into the run's own immutable plan — never read from a `TopicDiscovery`,
which is an interactive observation and never an execution input.

```yaml
spec:
  topics: []                      # required, and empty in this mode
  allUserTopics:
    exclude:
      topics: ["payments"]        # exact names
      prefixes: ["tmp-"]          # LITERAL prefixes, never patterns
    incompleteDiscovery: BackUpVisibleTopics   # or Refuse. REQUIRED, no default
```

**`spec.deadlineSeconds` must fund a discovery.** The discovery Job's
`activeDeadlineSeconds` is `min(300, spec.deadlineSeconds)` and ninety seconds
of that is image pull, scheduling and container start, so a dynamic run needs
**at least 120 seconds** — thirty for the runner itself. A smaller
`deadlineSeconds` is refused terminally as `ExecutionSpecInvalid`, naming the
field and the floor, **before** any Job or ConfigMap is created; dispatching a
Job with a one-second budget would die `DiscoveryFailed` with nothing naming
the deadline as the cause. A named allowlist has no such floor.

**What the controller does, in order.**

1. Resolves `spec.sourceRef` once with the saved-connection resolver and
   records the digest of that resolution. The digest covers the cluster's UID
   and its connection settings, and deliberately **not**
   `KafkaCluster.status.clusterId` — that field is written by the probe and
   cleared on any unreachable or unreadable pass, so pinning it would turn
   ordinary probe churn during the discovery window into a refused run.
2. Creates `lwd-<backup uid>` — a `topicInventory` check Job, running
   `logweir check run --plan /check/check-plan.json --check-contract-version 1`
   as the runner ServiceAccount with **no** Kubernetes token, **no** signing key
   and **no** archive credential — plus its immutable plan ConfigMap
   `lwd-<backup uid>-plan`. Both are owned by the `Backup` with
   `controller: true`, so deleting the `Backup` collects them; the Job carries
   `logweir.dev/purpose=topic-discovery` and
   `activeDeadlineSeconds: min(300, spec.deadlineSeconds)`. The phase becomes
   `Resolving` with `TopicsResolved=False/DiscoveryRunning`, and the reconcile
   requeues.
3. When the Job finishes, reads the **full** stdout of the pod whose controller
   owner reference is that Job — a label match is never enough — verifies the
   relay's frames against the digest the plan ConfigMap pinned, and holds the
   runner's result document to the frames it travelled with.
4. Classifies: internal is `entry.internal` **or** a `__` prefix; a topic the
   broker refused to describe is `limited`; an exact or prefix rule hit is
   `excludedByRule`; the rest is the resolved list, byte-sorted and
   deduplicated. Every resolved name is then re-validated against the Kafka
   grammar `^[a-zA-Z0-9._-]{1,249}$` — this list came off a runner's stdout,
   not out of a CRD field the API server pattern-checked — and a name the
   broker could not hold is `DiscoveryResultUnreadable`.
5. Freezes it through exactly the path a named allowlist takes, records
   `status.selection`, writes `TopicsResolved=True/Resolved`, and only then
   patches the discovery Job's `ttlSecondsAfterFinished`.

**The discovery Job is collected after every outcome, refusals included**, by
patching its `ttlSecondsAfterFinished` once the run's terminal status is on the
server — never before, because the relay lives on the pod and the TTL
controller deletes a Job and its pods together. Ten minutes later the Job, its
pod and the plan ConfigMap are gone; a `Backup` deleted sooner takes them by
owner cascade.

**The terminal states, and which of them a new `Backup` could survive.** All of
them are `Failed=True`, `exitReason: operational`, with no `exitCode`, and none
of them starts a runner Job.

| Reason | When | A new `Backup` could succeed |
|---|---|---|
| `DiscoveryFailed` | The check Job did not produce a usable result: an unreachable broker, a pod that never started, a deadline | yes |
| `DiscoveryResultUnreadable` | It produced output that did not verify — frames that do not decode, a result document whose counts or digest the frames do not support, a missing plan ConfigMap | no, not without fixing the runner |
| `DiscoveryIncomplete` | Visibility was not established and the policy is `Refuse` | only with more permission, or an attestation |
| `SelectionEmpty` | Nothing was left after internal topics, exclusions and the topics the broker would not describe | only if the cluster changes |
| `SelectionTooLarge` | Over 5,000 resolved names, or over 256 KiB of them | only with more exclusions |
| `SelectionTooLarge` (truncated listing) | The runner had to cut the listing at the plan's 20,000-topic `maxTopics` or at its relay budget, so the names are a **prefix** of what the principal can see | **not** with more exclusions — they are applied controller-side, after the listing. Name the topics explicitly, or split the cluster across schedules |
| `SourceChangedDuringResolution` | The broker's `clusterId` is not the one the `KafkaCluster` observed, or the saved connection changed while the discovery ran | yes |
| `JobNameConflict` | Something else owns `lwd-<backup uid>` | remove it first |

**The two `SelectionTooLarge` rows share one reason and one condition**, and
are told apart by the message: the truncated case says the names it returned
are a `PREFIX` of what the principal can see. They are not split into two
reasons because from a consumer's side "this cluster has more topics than one
run may name" is one fact — but the remedies differ, so a surface that renders
a remedy must read the message and not only the reason.

`spec` is CEL-immutable and a terminal run is never restarted, so "retryable"
always means **a new `Backup`** — which discovers afresh, in its own Job. That
is also why a topic created between two dynamic runs is in the second run's
frozen list and in nothing the first run recorded.

**Completeness is never assumed.** Kafka silently omits topics a principal
cannot describe, so a successful listing alone is `visibility: unknown` and
never proof. `limited` requires an observed authorization failure.
`attestedComplete` requires an administrator attestation in the installation
policy ConfigMap naming the namespace, the `KafkaCluster`, the principal and the
observed cluster id, and not expired — a blank principal or cluster id fails
closed. **Today no chart renders that policy reference, so
`attestedComplete` is unreachable and `coverage: AllUserTopicsAttested` is
never written.** The two reachable labels are `VisibleUserTopicsOnly` (with
`incompleteDiscovery: BackUpVisibleTopics`) and, for a named allowlist,
`NamedTopics`.

**What the frozen plan records.** `topics` is the exact list handed to
`backup.yaml`; `selection.discovery` is its provenance — `observedAt`,
`clusterId`, `visibility`, `basis`, `resultSha256`, `visibleTopicCount`,
`internalExcluded` and `excludedByRule`, `limitedTopicCount` and
`discoveryJob`. The names are recorded once, in the plan; `status.selection`
carries only counts, because a status is not a store.

`internalExcluded.names` and `excludedByRule.names` are a **bounded sample**,
not the whole list: at most 50 and 200 names, and **at most 16 KiB of names
between them**, spent in that order. `truncated` says when a sample was cut.
The `count` beside each one is always exact — a reader that needs the number
reads `count`, and a reader that needs every name reads the plan's own `topics`
or asks the cluster. Both bounds are there because either alone is escapable:
the count bound admits 250 names of 249 bytes, and a byte bound alone admits a
hundred thousand one-character ones. The plan `ConfigMap` a run mounts is one
MiB, and the topic list already has 256 KiB of it.

**The discovery Job's labels.** `app.kubernetes.io/component=run-discovery`
(not `check`) and `logweir.dev/purpose=topic-discovery`. The shared component
key is what lets the controller's single installation-wide `list` see
interactive checks and per-run discoveries together, so a run's discovery counts
against `maxActiveDiscoveriesPerConnection` — the ceiling that bounds
simultaneous dials at one broker. The distinct **value** is what keeps it out of
the interactive namespace and installation pools: nothing queues a run's
discovery (an operator already scheduled the work), and a Job counted against a
pool it is never queued by would spend the console's budget for free.

**Upgrade and rollback.** `spec.allUserTopics` is an additive CRD field, and a
named schedule or `Backup` behaves exactly as before — no discovery Job, no
`TopicsResolved` condition, `coverage: NamedTopics`. An **older** controller
handed a dynamic `Backup` deserialises it (because `topics` stays present),
renders `topics: []`, and the runner refuses with exit 3 before it contacts the
engine; it never creates a discovery Job. Roll back by suspending dynamic
schedules first: a dynamic `Backup` that is created but not yet frozen must be
allowed to fail or be deleted.

**A manual run from a schedule.** "Back up now" copies the schedule's current
policy into an ordinary `Backup` and records which revision it copied:

```yaml
metadata:
  name: logweir-manual-2v4qk7bhq8nwz3xr9fcm5td6ea   # the API derives it from the idempotency scope
  labels:
    logweir.dev/schedule: nightly
    logweir.dev/schedule-uid: <uid>
    logweir.dev/trigger: manual
spec:
  sourceRef: { name: source }            # copied from the schedule at generation 7
  topics: [orders, payments]
  archive: { url: s3://kafka-backups/logweir, secretRef: { name: logweir-s3 } }
  deadlineSeconds: 3600
  triggeredBy: manual
  trigger: { kind: Manual, attempt: 0 }
  scheduleRef: { name: nightly, uid: <uid>, generation: 7, runPolicySha256: sha256:… }
```

`scheduleRef` on a manual run is a **record, not an identity**: the run still
executes under its own UID, and the reconciler never reads the
`BackupSchedule`. So the run is allowed while the schedule is **suspended**,
while another run of it is **active**, and after the schedule has been
**deleted** — a "Back up now" pressed a second before someone deletes the
schedule still completes. The labels are hints for selection and are never
authority. Omit `scheduleRef` entirely for an ad-hoc run against a cluster.

**Idempotence, which is what PLAT-06.2's "Back up now" needs.** Creating a
`Backup` under a **new name** is a new run. Re-creating the same name while the
object exists is the API server's own `AlreadyExists` — one run, however many
times the button is pressed. Deleting the object and creating the name again
mints a new UID and is therefore a new run, under a new archive prefix.

```bash
kubectl --context docker-desktop -n <ns> apply -f config/samples/backup.yaml
kubectl --context docker-desktop -n <ns> get backup nightly-catchup \
  -o jsonpath='{.status.execution.id}{"\t"}{.status.phase}{"\n"}'
```

### The plan ConfigMap: frozen inputs, created before any Job exists

The Job mounts a ConfigMap named `<backup name>-plan` at `/plan`, and the
runner argv points `--spec` at `/plan/backup.yaml` and `--allowed-clusters` at
`/plan/allowed-clusters.json`. **The `Backup` reconciler freezes that ConfigMap
in the same pass that creates the Job, and the ConfigMap comes first.** The
order is the whole point: a Job created first is a pod stuck in
`ContainerCreating` on `MountVolume.SetUp failed for volume "plan": configmap
"<name>-plan" not found` until `activeDeadlineSeconds` fires, after which the
job controller deletes the pod and the exit code goes with it — a terminal
`NoExitCode` that explains nothing.

It is **create-only and `immutable: true`**, owner-referenced to the `Backup`
with `controller: true` and `blockOwnerDeletion: true` (so deleting the
`Backup` collects the plan and a half-deleted `Backup` cannot orphan one), and
it carries **three keys**:

- **`execution-inputs.json`** — the canonical typed snapshot of everything the
  run executes. Its grammar is versioned. **This controller writes
  `logweir.dev/backup-execution-inputs/v2` and reads both `v2` and `v1`**; a
  snapshot naming any other version is a conflict and never a best-effort
  parse. The document is:

  | Key | Grammar | What it holds |
  |---|---|---|
  | `version` | `v1` | the grammar string above |
  | `execution` | `v1` | the run identity: `id`, `trigger` (`manual`/`schedule`), the `Backup`'s namespace, name and UID, and `schedule {name, uid, slot}` for a scheduled kind |
  | `trigger` | **`v2`** | `{kind, attempt, retryOf?, timeZone?}` — the finer trigger of the table above. `timeZone` is informational: the zone the slot was computed in, so a history row keeps its local time after somebody edits the schedule's `timeZone` |
  | `scheduleRef` | **`v2`** | `{name, uid?, generation?, runPolicySha256?}` — the `BackupSchedule` **revision this run copied**, verbatim from the object. It is copied and never re-resolved, so a schedule edited between admission and freeze cannot rewrite what a created run records; and it survives the schedule's deletion, which is what makes a retained history answerable |
  | `runPolicySha256` | **`v2`** | the digest of **this run's own** policy fields, recorded for every run including an ad-hoc manual one that copied no schedule |
  | `source` | `v1` | the source `KafkaCluster`'s **UID** and everything the one resolver (§20) decided about its connection — bootstrap addresses, auth mode, SCRAM username, the TLS flag and the `auth.tlsCa` reference |
  | `topics` | `v1` | **the exact list this run executes**, in the order `backup.yaml` carries it: `spec.topics` verbatim in named mode. Never empty and never a pattern |
  | `selection` | **`v2`** | `{mode, coverage, resolvedTopicCount, resolvedTopicBytes, exclude?, incompleteDiscovery?, discovery?}` — where `topics` came from and what the run may claim to have covered. **The names are not repeated here**: `topics` is the one copy, and `selection` is its provenance |
  | `destination` | **`v2`** | **reserved**. The resolved saved `BackupDestination` a run writes to. Nothing writes it yet; the block is declared so that destination-backed execution lands inside this one document rather than in a second freeze |
  | `archive` | `v1` | the archive URL, the resolved `storage` block and the object-store addressing variables this controller forwards |
  | `runner` | `v1` | the runner argv, deadline and engine tunables |

  **Every `v2` block is optional and omitted when unset** — never written as
  `null` — and `tlsCa` is likewise absent for a connection that names no CA.
  That is what lets a `v1` snapshot frozen by an older controller parse under
  this grammar and **re-encode to the exact bytes it was stored as**, which is
  the property the digest, the annotation and the whole verification below rest
  on.
- **`backup.yaml`** — the typed `BackupSpec` document `logweir backup run
  --spec` parses, rendered **from that snapshot**. `source.bootstrapServers`
  and `source.auth` come from the `KafkaCluster` that `spec.sourceRef` names,
  never from `Backup.spec`, which carries neither; `source.topics` is
  `spec.topics` **verbatim**; `storage` is `spec.archive.url` through the same
  parser the controller's own read-only archive handle is built with. The
  document is built as the Rust type and serialised, not assembled as text:
  `storage` is an internally tagged enum whose variants have incompatible
  required fields, and a stringly-typed renderer emits `backend: filesystem`
  beside a `bucket:` key, which fails the engine's config load.
- **`allowed-clusters.json`** — the cluster allowlist, in the format the CLI's
  own reader parses.

Two annotations name what it holds: `logweir.dev/execution-id` and
`logweir.dev/execution-inputs-sha256`, the `sha256:` digest of the
`execution-inputs.json` bytes. The same two are stamped on the runner Job and
its pod template, and the digest is recorded on the object **before the Job
exists**:

```bash
kubectl --context docker-desktop -n <ns> get backup <name> -o jsonpath='{.status.execution}'
# {"id":"…","inputsRef":{"name":"<name>-plan"},"inputsSha256":"sha256:…"}
```

**What a later pass does with it.** Every pass that would create a Job
re-resolves the inputs from the spec, the referenced `KafkaCluster` and this
controller's addressing, and admits the existing ConfigMap only when all of
this holds: exactly one owner reference, this `Backup`'s, complete;
`immutable: true`; exactly those three keys and no binary data; a snapshot of a
grammar it understands, in canonical form, whose digest is the annotated one;
runner documents that are byte-for-byte what that snapshot renders; a snapshot
bound to this `Backup`'s namespace, name, UID and derived identity; the digest
`status.execution` recorded; and inputs equal to the fresh resolution in
everything executable. Anything else is terminal `PlanConfigMapConflict`,
naming which of those failed. **Nothing is ever patched, replaced or deleted**:
a plan another pass may already have mounted is never rewritten, a foreign,
extra-owner or mismatched object is never adopted, and a mutable plan left by a
controller that predates frozen inputs is refused rather than reused.

**The comparison is made at the stored document's own grammar.** A stored `v2`
snapshot is compared whole, so a changed `trigger`, `scheduleRef`,
`runPolicySha256`, `selection` or `destination` is a conflict — and the refusal
names *which* block moved. A stored **`v1`** snapshot is compared against the
`v1` view of the fresh resolution: it is asked only the question a `v1` plan
can answer, "are the fields it actually froze still the fields this run
resolves?". Without that rule every `Backup` in flight at the moment an
upgraded controller starts would become a terminal `PlanConfigMapConflict` at
its next pass, for no reason but an added optional block.

The cost of that rule, stated plainly: **a `v1` plan carries no provenance and
none is compared.** The trigger, the schedule revision, the policy digest and
the selection are simply not in the document, so a run executing one records no
`status.selection` and no revision — nothing executable is weakened (the source,
the topic list, the archive and the runner argv including the execution id are
all still compared), but the answer to "which policy did this run?" is absent by
construction. That is correct and unavoidable for a run frozen before the
upgrade. It also means **a `v1` plan appearing under a controller that writes
`v2` is worth an operator's attention**: every freeze this controller performs
writes `v2`, so a `v1` plan on a `Backup` created after the rollout was not
written by it.

One difference is informational and deliberate: a newly observed
`KafkaCluster.status.clusterId` changes the snapshot's bytes but not its
executable inputs, so a probe that lands between the freeze and the Job does
not invalidate the run. Everything else — a recreated source cluster (its UID
is pinned), different bootstrap addresses, a changed auth mode, username or TLS
flag, a changed `auth.tlsCa` reference, a different topic list, a different
archive or endpoint — does.

**A Job that disappears from a nonterminal `Backup` is re-created from those
same frozen inputs** (the ConfigMap is read and verified, never re-rendered),
which is what makes deleting a running Job a retry of one run rather than the
start of another. The Job keeps the `Backup`'s name, so the pod carrying the
exit code is selected by the job-name label **and** by its owner Job's UID: a
deleted Job's pod is never read as the new Job's evidence.

The source connection is configured once on `KafkaCluster`, and one resolver
(§20) turns it into every Job: the probe and each backup reuse that object's
bootstrap servers, SCRAM username, TLS setting, `auth.secretRef` and
`auth.tlsCa`. For SCRAM, both project the Secret's `auth.secretRef.passwordKey`
(default `password`) into `LOGWEIR_SOURCE_PASSWORD` with
`valueFrom.secretKeyRef`; a named `auth.tlsCa` is projected as a read-only file
and its pod-local path handed to the runner in `LOGWEIR_SOURCE_TLS_CA_FILE`.
The controller reads neither object.

**What the snapshot carries, and what it deliberately does not.** The frozen
inputs record everything the resolver decided about the connection — bootstrap
addresses, auth mode, SCRAM username, the TLS flag and the `auth.tlsCa`
reference — so a later pass that re-resolves the same `KafkaCluster` compares
equal, and a connection edited after the freeze is `PlanConfigMapConflict`
rather than frozen plan bytes executed against a changed connection. A CA
*certificate* is public material, which is why the object holding it may be
named there. **No credential Secret name and no credential key is written into
the snapshot or the status**: the password reference and the object-store
credential reference are fixed by the pinned `KafkaCluster` UID and the
CEL-immutable `KafkaCluster.spec` and `Backup.spec`, so the Job builder derives
them from those objects and the ConfigMap — which has no encryption at rest and
a much wider read surface than a Secret — names neither. Archive credentials
remain separate, under `Backup.spec.archive.secretRef`.

These Jobs open separate connections: a completed probe leaves no running
client to share with a later backup or its engine subprocess. Each new pod
resolves the same Secret reference, so password rotation applies to new Jobs
without copying credentials into every Backup. A successful probe establishes
reachability at that time; backup permissions and the engine's credential
rendering restrictions are still checked by the backup runner.

A SCRAM source with a missing or blank `auth.secretRef.name` or `auth.username`
produces `CredentialNotRenderable` before a plan or Job is created, and every
other conflicting connection is refused just as early with its own named state
(§20.4). An absent Secret or a missing password key is reported by Kubernetes
when it starts the pod -- the controller holds no `get` on Secrets and cannot
know it earlier.

If an older controller created a Job that failed with
`LOGWEIR_SOURCE_PASSWORD is unset`, deploy the corrected **controller** image
and start a new Backup (or wait for the next scheduled slot). Updating a
Deployment does not change existing Job pod templates, and a terminal Backup
is deliberately not rerun. No connection or Secret needs to be recreated when
the existing `KafkaCluster` reference is valid.

Other refusals on this path are **terminal and never a requeue**, because
`Backup.spec` is CEL-immutable and the next pass would read the same spec:
`spec.sourceRef` naming a `KafkaCluster` that does not exist is
`ReferentNotFound`, and a topic carrying a glob metacharacter (`*`, `?`, `[`,
`]`, `{`, `}`) is `GuardRefused` **before anything is created** — topics are a
mandatory named allowlist, and the rail is the same one the runner uses. A Job
that already occupies the `Backup`'s name but is not controlled by exactly this
`Backup` is `JobNameConflict`: it is neither observed nor adopted, so a
stranger's exit code and evidence keys cannot reach this object.

**Why the allowlist needs a stronger binding on the drill path.** On that path
`allowedClusterIds` authorises a restore *target*, so the per-Restore bundle is
immutable and its exact bytes are pinned in the Job template and checked by
the runner before phase 0. On the backup path the direction is
reversed: **the allowlist is a consistency rail, not a boundary.** The address
the run dials comes from the CEL-immutable `sourceRef`, and the backup guard
*refuses* a run whose broker-observed source cluster id appears in
`allowedClusterIds` — a cluster cannot be both the source of an archive and a
scratch cluster whose topics a drill deletes. So the rendered file carries an
**empty** `allowedClusterIds` and the observed cluster id in `sourceClusterId`,
which the backup path does not read; every id an attacker could add makes the
backup **refuse**, and none widens it.

**The residual, stated plainly.** The backup runner does not yet re-check the
mounted plan against a digest of its own, the way the restore runner checks its
approval bundle (§12). The controller's checks all happen before the Job is
created, and `immutable: true` stops an in-place edit; a subject with
`delete`+`create` on ConfigMaps in the namespace could still delete the frozen
plan and re-create it under the same name in the window before the kubelet
projects it. Such a subject can create Backups outright, so this widens no
boundary — but a runner-side digest check (the `Restore` path's shape) is the
remaining hardening, and it is not claimed here.

### Backups created under the previous execution contract

Every `Backup` a PLAT-06.1 controller created or froze keeps working, unchanged
and unconverted. Nothing is rewritten and no write happens on upgrade.

| What the stored object carries | How this controller reads it |
|---|---|
| No `spec.trigger` and `triggeredBy: schedule` | `Scheduled`, attempt 0 — exactly what the controller that created it did |
| No `spec.trigger` and `triggeredBy: manual` | `Manual` |
| `spec.scheduleRef: {name}` with no `uid`, plus the `BackupSchedule` controller owner reference | The UID comes from that owner reference, so the execution id is `<owner uid>-<slot>`, the value PLAT-06.1 computed. The owner's **name** must still equal `scheduleRef.name` |
| `spec.scheduleRef` with a `uid` and **no** owner reference (what a D1 schedule writes) | The UID comes from the reference. This is the shape that lets deleting a schedule keep its history |
| No `spec.allUserTopics` | Named mode, coverage `NamedTopics`. Existing allowlists are unaffected: no discovery Job, and no `TopicsResolved` condition |
| A `v1` plan ConfigMap | Read, verified, re-encoded to its own bytes and executed. See the comparison rule above |
| No `status.selection` | Absent, and stays absent: the block is written at the freeze and this run was frozen before it existed |
| A `logweir.dev/runner-argv` annotation | Observed, never executed, surfaced as `RunnerArgvAnnotationIgnored` — as before |

Two behaviours **change** for a stored object, both deliberately and both only
before its inputs are frozen:

1. A scheduled `Backup` whose `BackupSchedule` no longer exists (or exists
   under a new UID) is now terminal `ScheduleNotFound` instead of running. That
   is the rule "deleting a schedule stops future work", which has to be stated
   by the run itself once the owner reference that used to state it is gone. A
   run whose inputs **are** frozen is unaffected.
2. The terminal state for a bad identity moved from `ExecutionSpecInvalid` to
   `ScheduledIdentityMismatch` (and `RunPolicyDigestMismatch`,
   `ScheduleNotFound`, `NameTooLong` where they apply). Every one of these was
   already terminal and already refused before any `POST`; only the name an
   operator reads has become more specific. `ExecutionSpecInvalid` keeps the
   spec-as-request cases: an unknown `triggeredBy`, a non-positive
   `deadlineSeconds`.

**Rollback to a PLAT-06.1 controller.** A `v1` plan keeps working under both
controllers. A **`v2`** plan does not: every struct in the snapshot grammar
denies unknown fields and the older loader requires its version string exactly,
so an older controller handed a `v2` document writes `PlanConfigMapConflict`
rather than mounting a plan it cannot read. That is fail-closed — the run stops
visibly instead of executing a plan the controller did not understand — but it
means a `Backup` frozen under `v2` must be allowed to finish, or deleted and
re-created, before rolling back. `spec.trigger`, the new `spec.scheduleRef`
fields and `status.selection` are additive CRD fields and are inert for an
older controller; leave the CRD in place.

### Legacy Backups, upgrade and rollback

**New scheduled Backups carry no `logweir.dev/runner-argv` annotation**, and
this controller never executes one. A `Backup` that still carries the
annotation — created by an older controller, or copied from an old runbook —
is handled like this:

| Situation | What happens |
|---|---|
| The annotation is present and no Job exists yet | The run proceeds from the typed spec and the derived identity. The annotation raises `RunnerArgvAnnotationIgnored=True`, reason `AnnotationIgnored`, whose message names the annotation's **size and `sha256:` digest** and whether it equals the derived argv — never its content. |
| The annotation is not a JSON array of strings | The same, with reason `AnnotationMalformed`. |
| The annotation names another subcommand, spec path, signing key or backup id | The same: it is not executed, and the message says it **DIFFERS** from the derived argv. |
| A Job already exists and predates frozen inputs (no digest annotation, no `status.execution`) | **It is observed to completion and never changed.** No plan is read or written, the Job is not re-created, and the status carries `ExecutionInputsUnverified=True`, reason `LegacyExecution`. Its terminal `status.backupId` is the value the older controller would have reported. |
| A Job exists whose digest does not match `status.execution` (for example one an older controller created after a rollback) | The same observation, reason `JobInputsMismatch`. |
| A mutable `<name>-plan` written by an older controller exists and no Job does | Terminal `PlanConfigMapConflict`. It cannot carry the input snapshot and it is not rewritten; delete that `Backup` and create a new one (a schedule fires its next slot normally). |

**Upgrade.** `status.execution` is an additive CRD field. Apply the CRD before
rolling out the controller that writes it, or the API server prunes the field
and every run reports `ExecutionInputsUnverified` for its own Job:

```bash
export LOGWEIR_CONTEXT=<production-context>
kubectl --context "$LOGWEIR_CONTEXT" apply -f config/crd/backups.yaml
kubectl --context "$LOGWEIR_CONTEXT" wait \
  --for=condition=Established crd/backups.logweir.dev --timeout=60s
kubectl --context "$LOGWEIR_CONTEXT" get crd/backups.logweir.dev \
  -o jsonpath='{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties.execution.properties.inputsSha256.type}'
# string
```

Backups already in flight keep their Jobs (the rows above). Nothing rewrites an
existing plan ConfigMap, so no drain is required for correctness — but a
`Backup` caught **between** an older controller's plan write and its Job create
becomes `PlanConfigMapConflict` and has to be re-created, so a quiet moment is
still the kinder time to roll.

**Rollback, and why it fails safe.** An older controller reads the
`logweir.dev/runner-argv` annotation and nothing else, so after a rollback:

- a **new-style Backup** (no annotation) gets no argv: the old controller
  reports `the object … carries no parseable … annotation`, requeues, and
  **creates nothing** — no Job, no partial archive. Nothing runs until the new
  controller is back;
- an **annotated legacy Backup** whose inputs this controller already froze is
  refused by the old controller too: the immutable three-key plan does not
  match the two-key document it renders, so it writes `PlanConfigMapConflict`
  rather than mounting a plan it did not produce;
- a Job that already exists keeps running under either controller, and both
  read its exit code the same way;
- `status.execution` on existing objects is inert for the old controller. Leave
  the CRD in place: it is additive, and deleting the field would strip the
  record of what a running Job was built from.

Scheduled runs are unaffected by a rollback in one direction only: the old
controller creates its Backups **with** the annotation again, so its own runs
continue. New-style annotation-less scheduled Backups created just before the
rollback stall until roll-forward, which is the fail-safe half of the same
rule: this controller refuses to invent an argv, and the old one refuses to run
without one.

### A name longer than 63 characters is refused, and the refusal is on the object

A Kubernetes object *name* may be 253 characters; a **label value** may be 63.
The runner Job's pods carry `batch.kubernetes.io/job-name`, whose value is the
Job's name, which is the `Backup`'s name verbatim — so a `Backup` named longer
than 63 characters yields a Job the API server refuses outright:

```
Job.batch "bbb…" is invalid: spec.template.labels: Invalid value: "bbb…":
  must be no more than 63 characters
```

The reconciler checks the length **before any `POST`** and writes a terminal
status — `phase: Failed`, `exitReason: operational`, condition `Failed=True`
reason `NameTooLong`, `exitCode` absent because nothing ran. It used to turn
the API server's refusal into a 15-second requeue instead, which left the
`Backup` with `status: null` and an empty `PHASE` column **forever**: an object
nothing would ever explain. `BackupSchedule` refuses the same thing earlier, at
name-minting time, under the same reason string; this is the same refusal for a
`Backup` created by hand or by the UI.

### Where the two evidence keys come from

`logweir backup run` prints, as its **final two stdout lines** and in this
order, `receipt-key=<key>` then `sidecar-key=<key>`. The controller reads them
back through the **`pods/log` subresource** and writes them to
`status.evidence.receiptKey` and `status.evidence.sidecarKey`.

It matches them **by key name, not by position.** A log body with the two lines
reversed still puts each key in its own field, and a log body with neither
leaves **both keys unset**. No key is ever derived from the backup id: a
guessed key points at an object that may not exist, and a verifier would then
report `Invalid` for a run whose evidence was merely unread.

**The evidence fact is its own condition, and it exists only at exit 0.**
`EvidenceRecorded` is `True` with reason `EvidenceKeysRecorded` when both lines
were read, `False` with reason `EvidenceKeysUnreadable` when they were not, and
**absent at exits 1, 3 and 4** — those runs write no artifact by contract
(Global Constraint 11), so there is nothing about them that could be
"unreadable". A failed `Backup` therefore carries exactly **one** `Failed`
condition. It did not always: before this was fixed, every refused `Backup`
came back with `Failed=True` *and* a second `Failed=False` reason
`EvidenceKeysUnreadable`, measured live. That is a malformed status, not a
cosmetic one — a condition array is a **map keyed by `type`**, so a standard
`FindStatusCondition` reader sees whichever comes first, `kubectl wait
--for=condition=Failed` matched the `True` one only by array order, and the day
the array is given the standard `x-kubernetes-list-type: map` the API server
would reject every failed `Backup`'s status patch.

Three RBAC notes, because each is easy to get wrong:

- **`pods/log` is a subresource and `pods` does not cover it.** A role granting
  `get` on `pods` reads every pod's spec and cannot read one line of any pod's
  stdout — and the failure is a 403 that looks like a transient API error.
- The controller requests **no verb on `secrets`, and no `pods/exec` or
  `pods/attach`**. The runner's key reaches its pod because kubelet projects it;
  the controller never reads it and cannot start a process inside the pod that
  holds it. `config/rbac/` carries the request; the install file carries the
  grant.
- **Every status write is a merge `PATCH`, and the role grants no `update`.**
  A `PUT` — `Api::replace_status` in kube — is authorised as the verb `update`
  on `<kind>/status`, which no rule in this install grants; the controller
  therefore uses `Api::patch_status` with `Patch::Merge` everywhere, including
  the `BackupSchedule` slot reservation `concurrencyPolicy: Forbid` makes
  before it creates a run. Compare-and-set is not given up by that choice: the
  patch body carries `metadata.resourceVersion`, which the API server applies
  as an update precondition and answers `409 Conflict` on a mismatch, so two
  controller replicas reading the same object still produce exactly one
  winner. A reservation sent as a replace is a 403 on every due slot of every
  schedule, and the tests that answer whatever route they are asked for cannot
  see it; `manifest_lint.rs`'s `every_call_site_has_a_grant` is what does,
  by deriving each `Api<T>` call in `crates/weirkeeper/src/` and requiring a
  grant for it in every shipped copy of the role.

### Which pod is read: the Job's owner reference, never the label alone

**A pod is read only when its controller `ownerReference` is a `batch/v1`
`Job` whose `uid` is the run's own Job UID, and only when exactly one pod
claims it.** The `batch.kubernetes.io/job-name` label (and its legacy
unprefixed spelling, still set on 1.29) narrows the listing, because a Job's
pod name is generated and cannot be known in advance — but the label is a
*selector*, never a *decision*. Anything that can create a pod in the namespace
can set it, so a controller that read "the first pod with this label" would
lift a planted pod's `state.terminated.exitCode`, its `refusal-reason=` line
and its evidence keys onto somebody else's `Backup`, `Restore` or
`KafkaCluster` — a tenant-authored exit code and a tenant-authored pair of
object keys on an object an approver reads.

**What the owner check buys, stated exactly.** `ownerReferences` is ordinary
metadata written by whoever creates the pod. **Kubernetes does not validate
it**: the API server does not check that the named owner exists, that the UID
is right, or that the creator is entitled to claim it —
`OwnerReferencesPermissionEnforcement` is not in the default admission chain,
and where it is enabled it checks `delete` on the owner and never the UID. A
pod's own `metadata.uid` is unforgeable; the `uid` inside an owner reference is
not. So the check raises the bar from *anyone who can create a pod in this
namespace* to *anyone who can create a pod **and** read the Job's
`metadata.uid`* — a real reduction, because the label is guessable from the
object's name and the UID is not, and that is the whole of it. The
operator-side control is the one that closes it: **do not grant pod-create in a
namespace where runs execute.** Kubernetes' built-in `edit` role carries both
`pods: create` and `jobs: get`, which is exactly the pair this needs, so a
namespace where untrusted tenants hold `edit` is not a namespace to run backups
in. See [install.md](install.md) for the runner namespace layout.

**Ambiguity is refused, not ranked.** Runner Jobs pin `backoffLimit: 0` and
`restartPolicy: Never`, so the job controller cannot produce two pods for one
Job. If two pods nevertheless claim it, at least one was minted by somebody who
read the Job's UID, and the controller cannot tell which — so it reads **none**
of them and writes the terminal state `PodOwnershipContested` (`exitCode`
absent, phase `Failed`; for a `KafkaCluster` probe, `Reachable=Unknown` and
`reachable` left unset). Picking the newest would be worse than picking at
random: a planted pod is created after the genuine one by construction, so a
newest-wins rule decides every contest in the planter's favour.
`PodOwnershipContested` is deliberately **not** retryable — re-running the Job
into the same namespace invites the same second claimant. A pod that fails the
owner check is not a claimant and cannot contest anything, so the label alone
still buys an attacker nothing at all.

`controller: true` is required because a pod may carry several owner references
and only one of them is the controller; an added non-controller reference is an
association its own author made. The `batch/v1` group is checked because `Job`
is not a `batch/v1`-exclusive kind. The same rule covers a Job deleted and
re-created under the same name (`Backup` and `Restore` Jobs *are* named after
their object), whose predecessor's pod keeps the label until garbage collection
finishes.

**There is no fallback.** Zero claiming pods is "no pod yet" and takes the
crashed-Job branch below — `exitCode` absent, phase `Failed`, reason
`NoExitCode`, and for a probe `reachable` left untouched — which is also what
happens when the pod was genuinely garbage-collected. A Job with no
`metadata.uid` is not even listed for. Every candidate that was not read is
logged once, with code `ForeignPodIgnored` and the namespace, the Job and the
pod name; in the contested case **all** claimants are named, including the one
a newest-wins rule would have chosen. Nothing here is configurable and nothing
changes an object's schema, so there is no migration step; an operator sees the
change only as a run whose pod was never really its own no longer producing a
status.

### The crashed Job: when there is no exit code at all

A Job can finish having produced no terminated state for `runner` — the node
went away, the pod never scheduled, the pod was garbage-collected. There is no
code to read and there never will be, so the controller writes a **terminal**
status rather than watching forever, and **never invents a code**: a fabricated
`1` is indistinguishable from a real operational failure, and a fabricated `0`
turns a lost run into a green badge.

| What the pod says | `status.exitReason` | Condition reason |
|---|---|---|
| `DisruptionTarget=True` | `operational` | `DisruptedMidDrill` |
| `Pending` with `PodScheduled=False` reason `Unschedulable` | `operational` | `PodUnschedulable` |
| the job-name label selector returns **zero** pods | `operational` | `NoExitCode` |
| anything else | `operational` | `NoExitCode` |

`status.exitCode` is **absent** in all four rows. An absent `EXIT` column with
`PHASE=Failed` is therefore a real, distinct state and not a rendering gap.

The pod is found by `batch.kubernetes.io/job-name=<job>`, falling back to the
legacy unprefixed `job-name=<job>` when that returns nothing — both are set on
1.29 and only the prefixed one is current — and the container is selected **by
name**, never by index, because an init container or a logging sidecar would put
an unrelated `exitCode: 0` at index 0.

### One surprise worth knowing: `refusal-reason=` is the last line of *stdout*, not of the pod log

`logweir backup run` prints `refusal-reason=<TerminalState>` as its **final
stdout line** for exit 3 — that is the CLI's contract and it holds. But a pod
log is **stdout and stderr merged in nondeterministic order**, and `backup run`
also writes the guard's full explanation to stderr. The controller therefore scans the final eight non-empty log lines and
matches by key name (`KEY_SCAN_TAIL_LINES = 8`). Missing evidence keys remain
unset; a missing exit-3 discriminator becomes `GuardRefusedUnknownReason`.

### What a green `Backup` requires

`status.evidence.verification.result == Valid` **and** `status.exitCode == 0`.
Both, and nothing else. A `Backup` carries **no `outcome`** — that field is
`Restore`'s — so there is no second source of truth for the badge to disagree
with. Verification itself is not this reconciler's work; it records the two
keys and leaves `evidence.verification` alone.

## 11. Compose listeners and Kubernetes access

The [compose stack](../e2e/compose/docker-compose.yml) exposes six listeners,
including the KRaft controller listener:

| Listener | Advertised address | Protocol | Consumer |
|---|---|---|---|
| PLAINTEXT | kafka-broker-1:9094 | PLAINTEXT | In-network setup/inter-broker |
| EXTERNAL | localhost:9092 | PLAINTEXT | Host harness |
| CONTROLLER | Not advertised | PLAINTEXT | KRaft quorum on 9093 |
| SASL | kafka-broker-1:9096 | SASL_PLAINTEXT | In-network SCRAM |
| SASLEXT | localhost:9097 | SASL_PLAINTEXT | Host SCRAM |
| K8S | `${LOGWEIR_K8S_ADVERTISED_HOST:-host.docker.internal}:9095` | PLAINTEXT | Pods |

Kafka redirects clients to its advertised address after bootstrap. A pod given
`localhost:9092` therefore attempts to reach itself. Docker Desktop resolves
`host.docker.internal`; the kind demo installs a CoreDNS mapping (§18).
Other harnesses can set `LOGWEIR_K8S_ADVERTISED_HOST` before starting compose.

`just e2e-up` runs `scram-setup` after the broker is ready, using its PLAINTEXT
listener to create the SCRAM credential. The setup is idempotent and its exit
status must succeed before a SASL client runs.

The Kafka image maps `___` to `-`, `__` to `_`, and `_` to `.` after removing
`KAFKA_`. For example,
`KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG` becomes
`listener.name.sasl.scram-sha-512.sasl.jaas.config`. Both SASL listeners need
that property; the SCRAM test reads the running broker's properties to catch
silent misspellings.

Logweir's librdkafka client calls the mechanism `SCRAM-SHA-512`; the engine's
configuration calls it `SCRAM-SHA512`. Both are exercised by
[e2e/tests/scram.rs](../e2e/tests/scram.rs). The compose password is a
throwaway fixture, not a deployment credential.

## 12. A `Restore` runs only against a verified approval, and the hash is recomputed here

A `Restore` is one restore run, executed as one Job. **A drill is a `Restore`
with `spec.target.mode: scratch`** — there is no separate kind, and the mode's
four differences (a marker topic, an allowlisted cluster, a source-may-equal-
target rule and phase-9 teardown) are decisions the *runner* makes from the
plan document it parses. The two Jobs are byte-identical; the two plans are
not.

### Why the admission is here and not in the pod

Exit 3 from a runner pod used to be ambiguous, and the corpus says why: phase 0
dials before phase 1 runs, so a `plan_hash` mismatch or a bad approval
signature only became exit 3 **when the target answered**. With a dead broker
the same spec exits `1` — so a UI that maps exit 3 to *"your approval does not
match this spec"* mislabels that case every time the scratch cluster is down.

The controller removes the ambiguity **at the source**. Before any pod exists
it runs four checks, in this order:

| # | Check | `reason`, and what happens |
|---|---|---|
| 0 | The object's own name is at most 63 characters | `NameTooLong`, terminal. Nothing is created |
| 1 | `spec.approvalRef` names something | `ApprovalNotReceived`, **terminal** |
| 2 | That `Approval` exists and is `Verified=True` | `ApprovalNotVerified`, **held and retried in 30 s** |
| 3 | `sha256(spec.planBytes)` equals the `plan_hash` **inside** `Approval.spec.approvalBytes` | `PlanHashMismatch`, terminal, naming both hashes |
| 4 | `spec.target.clusterRef` resolves to a `KafkaCluster` with `status.reachable: true` | `ClusterNotReachable`, terminal |

**An unapproved plan creates no ConfigMap or Job.** The runner still validates
its mounted approval bundle and credentials, so an exit 3 requires reading
`refusal-reason=` rather than assuming one particular guard fired.

Check 1 and check 2 are different facts and are reported under different
names. Check 1 is *you did not ask for authorisation*: the ref is empty, `spec`
is sealed by the CEL rule of §7, and no `Approval` anyone creates can ever bind
to this object — so it is terminal. Check 2 is *your authorisation has not
arrived*, which can change without anybody touching the object.

### The thirty-second hold is what makes the wizard work

The restore wizard mints **both** `metadata.name`s from the plan bytes before
it creates either object, and an approver may take an afternoon over the
`Approval`. So a `Restore` whose `spec.approvalRef` names an `Approval` that
does not exist yet is not an error:

```bash
kubectl --context docker-desktop get restore r1 \
  -o jsonpath='{.status.phase}{"  "}{.status.conditions[?(@.type=="Admitted")].reason}'
# Pending  ApprovalNotVerified
```

`phase: Pending`, one `Admitted=False` condition, no Job, no `exitCode` — and
the object is released the moment the `Approval` verifies. `Admitted` is its
own condition type on purpose: a condition array is a map keyed by `type`, so a
`Failed=False` written while waiting and a `Failed=True` written if the run
later fails would be one field with two values.

The same hold covers the mirror-image race: an `Approval` reconciled *before*
its `Restore` existed reports `ReferentNotFound` (§8), which this reconciler
routes on explicitly, logs as the race it is, and retries.

### The hash is recomputed, and never read from a status

`sha256(spec.planBytes)` is computed **at Job-creation time** and compared
against the `plan_hash` **inside the approval document's own bytes**. Neither
half is read from a `status`:

* a spec schema change invalidates every approval, and a status is a cache;
* a status field is written by a controller and is not part of anything anyone
  signed, so it could rescue an approval that binds a different plan.

`spec.approvalBytes` is the **UTF-8 document text, verbatim, never base64**
(§8). A decode step between the approver's file and the parsed document would
find no JSON, no `plan_hash`, and would report every approval in the cluster as
a mismatch.

### `spec.planBytes` reaches the pod byte for byte

The controller writes `spec.planBytes` into a ConfigMap `<restore name>-plan`
as the single key `restore.yaml`, **verbatim** — no parse, no re-serialise, no
`trim`. It is `POST`ed **before** the Job, because the Job mounts it at
`/plan`; a Job created first is a pod that sits in `ContainerCreating` on
`configmap not found` until its `activeDeadlineSeconds` fires.

That document has ONE grammar and it is the runner's own: the shipped
`examples/restore.yaml`, which `logweir restore run --spec` parses. `planBytes`
stays an opaque string on the way through precisely because the API server
normalises YAML and a typed round-trip silently invalidates every approval —
`plan_hash` binds these exact bytes, trailing whitespace included.

```bash
kubectl --context docker-desktop get cm r1-plan -o jsonpath='{.data.restore\.yaml}' \
  | sha256sum
kubectl --context docker-desktop get restore r1 -o jsonpath='{.spec.planBytes}' \
  | sha256sum
# the two digests are equal, and both equal the plan_hash the approval names
```

### What the Job carries

Beyond §10's shape — `restartPolicy: Never`, `backoffLimit: 0`, the two-rule
`podFailurePolicy`, one container named `runner`, no ServiceAccount token, no
TTL at creation time:

| Volume | From | At | Why |
|---|---|---|---|
| `approval` | immutable ConfigMap `<restore>-approval-bundle` | `/approval` | `approval.json`, `approval.sig`, `approver.pub.pem`, `allowed-clusters.json` |
| `signing` | Secret `logweir-signing-key`, `0440` | `/signing` | the runner's own signing key, readable only because `fsGroup: 65532` is set |
| `plan` | ConfigMap `<name>-plan` | `/plan` | `spec.planBytes`, verbatim |
| `work` | `emptyDir` | `/work` | the scorecard, the offset report and the checkpoint state, on a pod whose root filesystem is read-only |

The Job template pins the SHA-256 of the plan and every approval-bundle member,
including `allowed-clusters.json`, plus the Restore and Approval identities.
New Jobs also pass `--execution-contract-version 1`, matching
`LOGWEIR_EXECUTION_CONTRACT_VERSION=1` in the immutable pod template. The
runner captures the projected bytes once, checks every digest, verifies the
approval signature and plan hash, and only then constructs Kafka, archive or
engine clients. A current runner rejects a missing, partial or mismatched
argv/environment contract. A pre-contract runner rejects the new argv flag as
unknown before command dispatch, so a new controller cannot silently run a
vulnerable old binary. `immutable: true` prevents updates; the runner-side
digest check also closes delete/recreate substitution under the same ConfigMap
name. Failure notifications retain only the routing fields parsed from these
authenticated bytes; reporting never reopens `/plan/restore.yaml`.

#### Upgrade, rollback and legacy Jobs

Upgrade the Approval CRD before rolling out a controller that writes or relies
on `status.verifiedSubjectRef`. Helm installs CRDs on a fresh release but does
not upgrade existing CRDs. For a real installation, deliberately select its
context and namespace; do not rely on the kubeconfig's current context:

```bash
export LOGWEIR_CONTEXT=<production-context>
export LOGWEIR_NAMESPACE=<logweir-namespace>

# 1. Apply the additive API schema before any new controller pod can start.
kubectl --context "$LOGWEIR_CONTEXT" apply -f config/crd/approvals.yaml

# 2. Wait for API discovery, then verify the provenance object and UID field.
kubectl --context "$LOGWEIR_CONTEXT" wait \
  --for=condition=Established crd/approvals.logweir.dev --timeout=60s
approval_schema="$(kubectl --context "$LOGWEIR_CONTEXT" get \
  crd/approvals.logweir.dev \
  -o jsonpath='{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties.verifiedSubjectRef.type}{" "}{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.status.properties.verifiedSubjectRef.properties.uid.type}')"
test "$approval_schema" = "object string"

# 3. Only after both checks succeed, perform the controller/chart rollout.
helm upgrade --install logweir ./charts/logweir \
  --kube-context "$LOGWEIR_CONTEXT" --namespace "$LOGWEIR_NAMESPACE"
kubectl --context "$LOGWEIR_CONTEXT" -n "$LOGWEIR_NAMESPACE" rollout status \
  deployment/logweir-weirkeeper
```

If either CRD check fails, stop before step 3. Starting the new controller
against an older structural schema can prune `verifiedSubjectRef`; a Restore
can then wait forever for provenance the API server discarded.

An already-created Job is the compatibility boundary. If its complete
single controller owner reference names the same Restore UID, the upgraded
controller observes it without changing its legacy namespace-wide Secret
mount. A same-named Job with no owner, a malformed owner, an older Restore UID,
or an appended secondary owner is a terminal `JobNameConflict` and is never
adopted. Unrelated annotations remain compatible.

If the old controller created `<restore>-plan` and crashed before creating the
Job, the new controller accepts it only when its bytes and complete owner
reference exactly match. The new Job still carries the execution digests, so
later replacement is refused before phase 0. Legacy Verified Approval status
is held until the Approval controller records `verifiedSubjectRef`; an Approval
already bound to a deleted UID is refused and must be recreated.

##### The one widening that is NOT rollback-safe: `Approval.spec.subjectRef.kind`

ADR 0008 Amendment G adds a third value, `RehearsalSchedule`. The **schema**
change is additive — the enum only grows — but the *decode* is not.
`weirkeeper`'s `SubjectKind` is a closed serde enum with no unknown-value
fallback, so a controller image that predates Amendment G **cannot deserialize
an `Approval` whose `subjectRef.kind` is `RehearsalSchedule`**. That is a
reflector decode error, and a reflector decode error takes down the whole
watch: **every** `Approval` reconcile in the cluster stalls, not just that one
object. It is exactly the failure the `Backup` archive sentinel was designed to
avoid, and the enum cannot be given the same treatment — a sentinel needs a
field to hide in, and this is the field.

**In this build the hazard is latent and nothing triggers it.** No component
creates such an `Approval`: the rehearsal controller does not exist, and this
build's `Approval` reconciler refuses a `RehearsalSchedule` subject visibly with
`ReferentHasNoPlanBytes` rather than verifying it. So an operator who applies
these CRDs and rolls the controller back is unaffected.

**The rule for the rehearsal worker, and the rollback order:**

1. Do **not** create an `Approval` with `subjectRef.kind: RehearsalSchedule`
   until every controller image you might roll back to already understands the
   value. That floor is the commit that adds Amendment G.
2. If one exists and you must roll back further, **delete or recreate those
   `Approval` objects first**, before the controller image changes. Deleting
   the CRD is not an option — it would delete every `Approval` in the cluster.
3. Check for them with
   `kubectl --context "$LOGWEIR_CONTEXT" get approvals -A -o jsonpath='{range .items[?(@.spec.subjectRef.kind=="RehearsalSchedule")]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}'`
   and expect no output before rolling back.

The CRD's own description says the same thing, so
`kubectl explain approval.spec.subjectRef.kind` carries the warning.

The rest of the Approval CRD change is additive and may remain installed during
and after a controller rollback; do not attempt to downgrade or delete the CRD
as part of rollback. Fence controller changes by scaling weirkeeper to zero, then let or
cancel every pending/running Restore and verify none is between ConfigMap
materialization and Job creation. Roll back controller and runner images only
after that drain. An older controller does not understand the new execution
contract and could otherwise create a legacy-transport Job from a partially
materialized new Restore. A current runner deliberately accepts a Job only when
both contract channels are absent (the preserved legacy/standalone shape) or
when both carry the complete matching current contract. Keep
`logweir-approval-bundle` until all pre-upgrade Jobs finish; after the drain,
delete it only when no remaining Job pod template references it.

The object-store credential reaches the pod as `secretKeyRef` env
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) from
`spec.sourceArchive.secretRef` — the Secret this install calls `logweir-s3`.
A `scramSha512` target additionally gets `LOGWEIR_TARGET_PASSWORD` from that
`KafkaCluster`'s own `auth.secretRef`, under `passwordKey` (default
`password`), and a `auth.tlsCa` target gets the projected CA file and
`LOGWEIR_TARGET_TLS_CA_FILE` (§20.2). The approved plan's `target` must name
the same connection the `clusterRef` resolves to, or the Restore is refused
with `ConnectionPlanMismatch` before any Job exists: the runner dials the
PLAN's address with THIS connection's credential.

### The credential is validated by the RUNNER, and the controller checks nothing

`weirkeeper` holds **no `get` on Secrets anywhere** (§9), so it never sees the
projected value and has nothing to validate. The check happens in the runner,
at the moment it reads `LOGWEIR_SOURCE_PASSWORD` / `LOGWEIR_TARGET_PASSWORD`,
and it exits **3** with `refusal-reason=CredentialNotRenderable`. The
controller maps that refusal onto the terminal state of the same name and does
nothing else with it. Do not look for a controller-side check; "no `get` on
Secrets" forbids one.

### Exit 3: the discriminator is a KEY NAME in a bounded tail

`TargetTopicConfigRefused` and `CredentialNotRenderable` are both exit 3, and
the only thing that tells them apart is the runner's `refusal-reason=` line.
**Read §10's note on `refusal-reason=` before writing any reader of it**
(plan erratum **E4**): the line is the last line of the runner's *stdout*, but
a pod log is stdout and stderr merged in nondeterministic order, and the pod
log API has no stream selector — so the position is not a rule in either
direction. This reconciler scans the final eight non-empty lines and matches by
key name, exactly as the `Backup` path does and through the same shared
function. **A log body with no such line at exit 3 yields
`GuardRefusedUnknownReason`, never a guess at which guard fired.**

Interface **I8**'s three evidence keys are read the same way, by name:

```
scorecard-key=logweir/drills/<run_id>.json
sidecar-key=logweir/drills/<run_id>.sig
offset-report-key=logweir/drills/<run_id>.offsets.json
```

**The third line is conditional.** It is printed exactly when the engine wrote
an offset report, so two lines at exit 0 is a complete, truthful answer;
`EvidenceRecorded=False` / `EvidenceKeysUnreadable` is raised only when one of
the two *mandatory* keys is missing, and only at exit 0, because Global
Constraint 11 says exits 1, 3 and 4 write no artifact at all.

### What the status carries, and what it copies

`exitCode` and the condition are decided from the pod. Everything else is
**copied verbatim** out of the signed scorecard, fetched with the controller's
read-only archive credential: `outcome`, `lastPhaseCompleted`, `objectives`
(`rtoSeconds`, `rpoSeconds`, `passRate`, `met`), `integrity`
(`level`, `result`, `partialReason`) and `measured`. The controller **never
parses the scorecard into a typed struct and re-emits it**: that type accepts
unknown fields and defaults every one of its own, so a field the reader does
not declare is silently dropped — and a re-emitted status block would quietly
disagree with the document an auditor reads. It lifts the values it needs out
of the JSON by pointer and copies them.

If the archive was not observed — no `LOGWEIR_ARCHIVE_URL`, an unreachable
bucket, an object that 404s — **every one of those keys is omitted**, and the
exit code is still recorded. Absent means "nobody looked", which is different
from `null`.

`newTopics` and `oldTopics` are derived from `spec.planBytes` through the one
prefix rule the runner uses, so they are a function of the bytes the approver
signed. They exist in tag 1 **solely** so tag 2's `Switchover` retirement has a
list to validate against; nothing in tag 1 reads them, and nothing in any tag
writes to a topic named in `oldTopics`.

`topicPreflight` is **always absent in tag 1, and that is a declared gap rather
than an oversight**. Guard G-TS's observation is returned by phase 0 inside the
runner and is deliberately not a scorecard field, and interface I8 fixes three
stdout key lines of which none is a preflight — so nothing carries it out of
the pod. Closing the gap means a fourth machine-read stdout line, which is the
runner's interface to change. An absent field is truthful; a guessed one is
not.

### `REASON` reads `status.reason`, not `status.exitReason`

`Restore.status` carries **two** reason fields, because they answer two
questions:

| Field | Vocabulary | Answers |
|---|---|---|
| `status.exitReason` | GC11's wire strings (`ok`, `operational`, `drill-not-pass`, …) plus the runner's own CamelCase terminal state off `refusal-reason=` | *What did the run exit with?* |
| `status.reason` | the CamelCase `reason` of the condition describing the object's terminal or current state, **verbatim** | *What state is this object in?* |

The `REASON` printer column reads **`status.reason`**, and that is a fix rather
than a preference. `exitReason` is written from an exit code, and the four
admission refusals plus `NameTooLong` happen **before any `POST`** — no run, no
code, so the only wire string GC11 offers is `operational`. Measured live at the
Task 20 review on two objects: `kubectl get restore` printed `operational` for
both an empty `approvalRef` and an unreachable cluster, while the actual states
(`ApprovalNotReceived`, `ClusterNotReachable`) existed only inside
`status.conditions`. For a `Restore` those are exactly the states an operator
scans a list for.

```bash
kubectl --context docker-desktop get restores
# NAME  MODE     PHASE    EXIT  REASON                OUTCOME ...
# r1    scratch  Pending        ApprovalNotVerified
# r2    scratch  Failed   3     GuardRefused
```

`status.reason` is **not a third vocabulary** beside errata E5b's two: it is
always the `reason` of the condition the same patch writes, so it is CamelCase
everywhere and never one of `exitReason`'s hyphenated wire strings.
`every_status_write_sets_the_scalar_reason` asserts the equality for every
status-patch builder and, by a source scan of `controllers/restore.rs`, that a
patch writing `conditions` without a `reason` cannot be added.

**The `Backup` kind is unaffected and unchanged:** its printer columns are
`PHASE/EXIT/RECORDS/SIGNED/AGE` with **no `REASON` column**, so nothing on that
path was reading `exitReason` for a state it could not express. §10's table is
the `Backup` contract and stands as written.

### A green `Restore`

```bash
kubectl --context docker-desktop get restore r1 \
  -o jsonpath='{.status.evidence.verification.result}{"  "}{.status.outcome}'
# Valid  pass
```

**Both**, and the badge reads both. `Valid` alone says the document is
authentic; it says nothing about whether the drill passed. `pass` alone says
the drill passed according to a document nobody verified. The `SIGNED` and
`OUTCOME` printer columns are those two fields, side by side, for exactly that
reason.

### The TTL, last

The status patch carrying the exit code happens **before** the Job is patched
with `ttlSecondsAfterFinished` — the same ordering, and the same measured
reason, as §10: the TTL controller deletes the Job *and its pods*, and the exit
code lives only on the pod.

---

## 13. Install, RBAC and network policy

[install.md](install.md) is the installation and uninstall procedure. Older
error messages saying “docs/kubernetes.md install step 1” refer to that guide's
keypair, roster and Secret preparation. See §16, Serving the UI, for the local
proxy and its authority.

`logweir-operator` grants ordinary `update` on BackupSchedules. CEL in the CRD,
not RBAC, restricts mutation to `spec.suspend`. The controller has reads on the
six kinds, status patches, Backup creation **and one `patch` on Backups**, Job
create/read/patch, Pod and `pods/log` reads, and ConfigMap create/get. It has no
Secret read, pod exec, pod attach or delete permission, and **no `update` on
anything**: every status write, the `Forbid` slot reservation included, is a
merge `PATCH` whose body carries `metadata.resourceVersion` as the
compare-and-set precondition (§10's three RBAC notes). Inspect
[config/rbac](../config/rbac/) for the authoritative grants.

The `patch` on `backups` is PLAT-05.2's history detach, and it is narrow by
construction: it authorises the main resource and **not** `backups/status`,
which is a separate resource string with its own rule, and it cannot change a
`spec` the CRD's CEL rules seal — the API server evaluates those on every
update however it is spelled. The one caller writes
`metadata.ownerReferences`, one label and one annotation, always with
`metadata.resourceVersion` in the body. There is no `update`, no `delete` and
no `deletecollection` on `backups`.

Job creation still allows the controller to mount a signing Secret. No Secret
read permission is not isolation from the signing key; see §15.

`logweir-runner-egress` selects all Job pods in its namespace using
`batch.kubernetes.io/job-name: Exists`. It permits DNS, the configured broker
ports, and object-store ports 443/9000. The base installs it in
`logweir-system`; apply it to each additional runner namespace using the
command in [install.md](install.md).

[UNVERIFIED — enforcement needs a kind cluster with Calico and a runner probe that times out to a disallowed address while reaching Kafka.]
Docker Desktop did not enforce the policy in the recorded tests.

The [ValidatingAdmissionPolicy example](../config/samples/validatingadmissionpolicy.yaml)
is commented out, requires 1.30+, and is excluded from the install.
[UNVERIFIED — needs a 1.30+ cluster to apply and exercise the policy.]
The CRDs' own CEL rules remain the installed immutability control.

X-APPLY was recorded against docker-desktop v1.34.1: applying the manifest
twice returned zero, while the controller remained in `ImagePullBackOff` for
an unpublished image. A successful apply establishes object creation, not a
working installation or publication. Release evidence is tracked in
[tag1-checklist.md](tag1-checklist.md).

## 14. Image references and the org-root anchor

The historical X-DIGEST probes on docker-desktop (2026-09-11) established that
local images had repository digests and pods could start from those digests
when the complete reference existed in the node's image store. The same digest
under another repository produced `ErrImageNeverPull`. Retagging resolved that
case only when the image bytes still matched the pinned digest.

BuildKit provenance changed the observed manifest-list digest even on a cached
rebuild. Read current pins from `config/manager/deployment.yaml` and
`crates/weirkeeper/src/job.rs`; historical build digests are not install values.
The local-image path and the registry path are documented in [install.md](install.md).
Local builds are **author-only** and do not establish public pullability.

The controller reads `LOGWEIR_RUNNER_IMAGE` and `LOGWEIR_RUNNER_PULL_POLICY`
once at startup. Blank values use the compiled defaults: the pinned runner
image and `Never`. Policy values are `Never`, `IfNotPresent`, or `Always`;
any other value refuses startup. These overrides let a newly built controller
use the runner image actually loaded or published for the deployment.

Both images embed `third_party/org-root.fingerprint` at
`/etc/logweir/org-root.fingerprint`. It is SHA-256 of the public key's DER SPKI:

```bash
openssl pkey -pubin -in third_party/org-root.pub.pem -outform DER | openssl dgst -sha256
cat third_party/org-root.fingerprint
just check-org-root
```

The shipped public key's private half was not retained in the repository.
Adopters can replace the public key and fingerprint and rebuild both images.
**No runtime code reads this anchor yet**: it is not an authorization control.
See [keys.md](keys.md) for key identity and attestation.

## 15. The evidence credential, the verdict, and the signing-oracle residual

`weirkeeper` verifies evidence for the UI using read-only object-store access.

### 15.1 The fifth Secret, and the documented switch

`logweir-evidence-ro` is a **read-only** object-store credential and a
**different principal** from the runner's `logweir-s3` (spec §9; Global
Constraint 6 already contemplates "a separate bucket and a separate
principal"). It needs `s3:GetObject` on the evidence prefix and nothing else.
It cannot write or delete in any bucket, and — because retention only reports
(guard **G-RET**) — **no Logweir component has any delete capability against
object storage in tag 1.**

It is projected into the controller's own pod as environment, with
`optional: true`:

```yaml
# config/manager/deployment.yaml
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef: { name: logweir-evidence-ro, key: access-key-id, optional: true }
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef: { name: logweir-evidence-ro, key: secret-access-key, optional: true }
```

**`optional: true` is load-bearing and not tidiness.** Mandatory, a cluster
that has not created this Secret gets a pod stuck in
`CreateContainerConfigError` — all six reconcilers down for want of a *display*
feature. Optional, the controller starts, holds no evidence handle, and every
`status.evidence.verification` reads:

```json
{ "result": "NotAttempted",
  "detail": "no evidence credential is configured; run the printed logweir drill verify command instead" }
```

**That is the documented switch.** An adopter who declines to give the control
plane bucket access runs with verification display off and verifies with the
CLI instead — `logweir drill verify --scorecard … --signature … --public-key …`
— and it is why `NotAttempted` exists as a verdict **distinct from `Invalid`**:
`Invalid` is a claim about the *document*, and declining to hand over a
credential has not produced a bad document.

**The controller reads that Secret from its own environment and never through
the API.** The `weirkeeper` ClusterRole grants no verb on `secrets` (§9), and
`crates/weirkeeper/tests/linkage.rs::the_controller_never_reads_a_secret` keeps
it that way. The consequence for operators is one line: after creating or
rotating `logweir-evidence-ro`, **restart the Deployment** — container
environment is fixed at start.

### 15.2 The badge is two rules, one per kind

Spec §8's green badge is not one rule, because "passed" is a different field on
each kind:

| kind      | green when                                                       |
|-----------|------------------------------------------------------------------|
| `Backup`  | `status.evidence.verification.result == Valid` **and** `status.exitCode == 0` |
| `Restore` | `status.evidence.verification.result == Valid` **and** `status.outcome == pass` |

There is **no `outcome` on the `Backup` path at all** — `Backup.status` carries
`exitCode` and no `outcome` — so a single shared rule would render every
`Backup` ungreen. Either badge is labelled

> verified by weirkeeper at `<verifiedAt>` against key `<matchedKeyId>`

and **never** "verified in your browser": the in-browser WASM verifier is cut
from tag 1, and rendering no browser-computed verdict is strictly more honest
than a green badge over a verification that did not happen there. Anything that
is not green renders the literal word **`unverified`**, never `pass`.

Both rules also appear on the object itself, as a `Verified` condition whose
`reason` is `Verified`, `VerificationInvalid`, `VerificationNotAttempted`,
`VerificationUntrusted`, `ExitCodeNotZero` or `OutcomeNotPass`, and whose
`message` is the badge label.

Since PLAT-19.1 the green rule reads `result == Valid` **and**
`trust.basis` is `Current` or `Historical`. The second clause is redundant
against what the controller writes — a `Valid` is only ever reached on those
two bases — and it is read anyway, because a badge rule that consults one field
is one edit away from rendering green over a basis nobody intended. An object
written by a controller that predates the `trust` block carries no `basis` at
all, and that is **not** a downgrade: the block is additive and an absent one
renders exactly as it did before.

A `Historical` badge carries a qualifier:

> verified by weirkeeper at `<verifiedAt>` against key `<matchedKeyId>` (signed before that key was retired)

**`Historical` is a pass, not a warning.** The key was valid when it signed and
has since been retired, which is what key rotation is supposed to look like
(§7.6 of decision D3). The qualifier is there so nobody has to guess why a
green badge names a retired key.

### 15.2a A fourth verdict: `Untrusted`

`result` is now one of `Valid`, `Invalid`, `NotAttempted` **or** `Untrusted`.

| verdict | what it is a claim about |
|---|---|
| `Valid` | the document, and the signer: the bytes verify and this installation accepts the key |
| `Invalid` | the DOCUMENT: the digest did not match, or no key verified the sidecar |
| `NotAttempted` | the CONTROLLER: no evidence credential, an unreadable object, no trust material, or a namespace two policies contest |
| `Untrusted` | the SIGNER: the bytes are authentic and the key that made them is one this installation will not accept |

`Untrusted` is deliberately not `Invalid`. Telling an operator their archive is
corrupt when their key was revoked sends them to re-run a backup instead of to
their trust policy. It is also deliberately not a silent `NotAttempted`: a
verdict *was* reached, and `detail` names the row that reached it —
`UntrustedSigner`, `KeyUsageMismatch`, `SignedOutsideValidity`, `Revoked` or
`RecordedBeforeRevocation` — beside the key id and the policy name. Every
reader that predates the value treats anything that is not `Valid` as
unverified (`ui/pages/backups.js`), so it fails closed on every old surface.

Two new fields sit beside it, both additive:

```yaml
signedAt: 2026-09-03T09:00:00Z          # the document's OWN claimed signing time
trust:
  basis: Current                        # Current | Historical | RecordedBeforeRevocation | None
  keyState: Active                      # Active | Retired | Expired | Revoked | Unknown
  policy: {name: org-default, uid: …, generation: 4}
```

`signedAt` is **recorded, not trusted**. For a retired or expired key it is
what distinguishes "signed while valid" from "signed afterwards". For a key
revoked as `KeyCompromise` it is attacker-controlled, so it is not consulted at
all: the only observation accepted there is one a controller of this
installation wrote itself — the `verifiedAt` of an earlier reconcile. An
imported archive with no such history and a compromise-revoked signer fails
closed, and `detail` says so.

### 15.2b What a revocation changes, and when

A fresh verification — a run finishing now, or a `Backup` still being
reconciled — asks the current policy and gets the current answer.

An object that is **already terminal** is not re-read (that is the rule that
keeps this controller quiet), so its verdict does not change by itself. When it
is re-derived, it is re-derived from what is already on the status: the stored
`matchedKeyId`, `signedAt` and `verifiedAt` are the three facts the decision
takes, so there is **no storage read, no signature check and no Job**. Such a
pass writes `evidence.verification.{result,trust,detail}` and the `Verified`
condition that says the same thing, and touches `phase`, `exitCode`, `outcome`,
the evidence keys, `matchedKeyId`, `signedAt` and `verifiedAt` not at all.

`verifiedAt` in particular is **never** refreshed by a trust change. It is the
one independent observation this installation has about when it saw the
document, and a pass that recorded its own conclusion over it would read that
write as corroboration on the next pass — flipping a
`RecordedBeforeRevocation` verdict to `Revoked` and leaving it there. So the
instant moves only when the *signature* changes: a different key, or a
different media type.

**What triggers it.** The `Backup` and `Restore` controllers watch
`TrustPolicy`. An event on one maps to the objects those controllers already
hold in the namespaces that policy could govern — out of the controller's own
watch cache, so the trigger costs **no** additional API calls and is bounded by
the objects the controller holds rather than by the cluster. The mapping
deliberately over-approximates: a `default: true` policy claims every namespace
there, although resolution would hand an explicitly-named namespace to its own
policy. Enqueuing an object a policy does not govern costs one re-derivation
that writes nothing; failing to enqueue one would leave a revoked key green,
and the two errors are not symmetric.

The re-derivation runs for objects that already carry a `matchedKeyId` and only
once the policy cache has synced. A cluster whose `trustpolicies` CRD is not
installed never syncs and simply never re-derives — it does not stop the
controllers from reconciling anything else.

`kubectl --context <ctx> get trustpolicy <name> -o yaml` remains the direct
answer for "what does this cluster think of this key now": it reports every
key's `effectiveState` and `usableForVerification` without waiting for a
reconcile.

### 15.3 What the controller actually checks, in order

1. **No credential** → `NotAttempted`. A fact about the install.
2. **`get` the payload and its detached sidecar** through the read-only
   handle. `NotFound` and *every other* storage error → `NotAttempted` with the
   error's own message. A storage failure is never `Invalid`.
3. **Recompute the digest** and compare it against the one the status recorded
   when the run finished (`Backup.status.evidence.receiptSha256`,
   `Restore.status.evidence.scorecardSha256`). A mismatch **is** `Invalid`, and
   the detail names both digests. Skipping this and checking the signature
   alone would accept a genuinely-signed *older* document put in this one's
   place.
4. **Resolve the object's namespace's trust** (§8's "Trust resolution"), then
   for each resolved key declared for **`EvidenceSigning`** build a verifying
   key from its `spkiPem` and check the DSSE sidecar. The first success selects
   that entry's own `keyId`; otherwise `Invalid` with the last error. A key
   declared for `GovernedApproval` or `ConsoleConfirmation` is never offered to
   the verifier at all, so it cannot become a `matchedKeyId` by accident. An
   **empty** list is `NotAttempted` with

   > the TrustRoster lists no signing key material; add the runner's public key to spec.signingKeys

   on a roster-only cluster, and

   > the TrustPolicy bound to this namespace lists no key with usage EvidenceSigning; add the runner's public key to spec.keys

   under a policy — each names the object an operator would actually edit,
   rather than failing silently. An `Invalid` there would blame every document
   in the cluster for one missing line in one cluster-scoped object. A
   namespace two policies claim is `NotAttempted` naming
   `TrustPolicyConflict` and both claimants.

   **`matchedKeyId` is the id the policy DECLARES, not one recomputed from the
   key material.** It is the string an operator can grep for in the object they
   edit, and it is what the roster path has always reported. An entry whose
   declared `keyId` is not the sha256 of its own `spkiPem` is therefore still
   offered to the verifier and still reported under the id it declares — and
   the same fault is reported independently on the policy's own status as
   `Loaded=False` with reason `KeyIdMismatch`, which is where a disagreement
   between the two shows up. (The approval path is stricter: an entry that
   disagrees with its own material refuses every approval, `KeyIdNotInRoster`.)
5. **Ask whether that key is still trusted** — §15.2a. A key that is retired,
   expired, revoked or was used outside its window gives `Untrusted`, never
   `Invalid`: the bytes are exactly what they claim to be.

This is written in a **second, separate `PATCH …/status`** after the terminal
one, so a verification failure — or a 500 on that patch — can never prevent the
exit code from being recorded.

### 15.4 The residual, stated plainly

`weirkeeper` is a **signing oracle**: Job CRUD in a namespace that holds
`logweir-signing-key` is equivalent to holding that key, because a Job the
controller creates can mount it. That is residual **O1**, and O0's default
**(a)** — accepted, and stated here rather than implied.

Global Constraint 27 is the narrowed position and the only one this repository
makes: **no control-plane crate links the signer** (`weirkeeper` depends on
`logweir-verify` and never on `logweir-evidence`;
`scripts/check-one-signer.sh` computes the reachable set from the dependency
graph), and the *capability* to sign is unbroken while the controller holds Job
CRUD over the signing key's namespace. Both halves are true at once and the
second is not softened by the first.

To remove that capability, the signing key must live beyond the controller's
Job-create authority and a separate trusted execution mechanism must perform
the signing. Merely adding a namespace-scoped RoleBinding does not narrow the
shipped cluster-wide grant. This split is not implemented by the default install.

None of this stops a cluster-admin — `docs/stability.md`, **O0**.

### 15.5 Where the object store is, for a local demo

`just k8s-demo` (Phase B's exit criterion; the transcript is
[../e2e/k8s/phase-b-demo.md](../e2e/k8s/phase-b-demo.md)) reaches the compose
stack's MinIO from inside docker-desktop as **`http://host.docker.internal:9000`**,
with `AWS_REGION=us-east-1` and `AWS_ALLOW_HTTP=true`. Those three are
**author-only** and live in
`config/overlays/k8s-demo/deployment-env-patch.yaml`, not in
`config/manager/deployment.yaml`: a shipped manifest naming one laptop's
hostname is not a file a stranger can apply, and an S3 endpoint that does not
resolve looks exactly like an empty bucket.

The controller **forwards** `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP`
and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` from its own environment to every runner
Job it creates (`weirkeeper::controllers::backup::ARCHIVE_ADDRESSING_ENV`), so
an adopter on MinIO or Ceph configures the endpoint **once**, on the
Deployment, rather than in two places that can disagree. A variable that is not
set is not forwarded, so the default install's runner Job env is exactly what
it was before.

## 16. Serving the UI

The local UI serves static files from `ui/` through `kubectl proxy` after the
install and roster setup in [install.md](install.md). The optional Helm UI is
an in-cluster alternative with a different credential boundary (§19).

Serve the directory and the Kubernetes API from one process:

```bash
kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1
```

Then open `http://127.0.0.1:8001/ui/`.

**One process, one origin, and that is the whole design.** `kubectl proxy` serves
the static files under `--www-prefix` **and** proxies the Kubernetes API on the
same origin, `127.0.0.1:8001`, attaching the viewer's own kubeconfig credential
to every request it forwards, server side. The page therefore addresses
`/apis/logweir.dev/v1alpha1/...` as a relative path on the origin that served
it: not a cross-origin request, no preflight, no granted header, and
**no bearer token, key or credential of any kind is ever placed in the page**.
It stores nothing either -- no browser storage, no cookie of its own.

A write is the same story. `POST /apis/logweir.dev/v1alpha1/namespaces/<ns>/restores`
goes to the proxy's own origin, so it is not cross-origin, triggers no preflight
and needs no granted header. `kubectl proxy`'s own defaults admit it: the shipped
v1.35.0 client's `--reject-methods='^$'` is a regular expression that matches only
the empty string, so POST, PUT and PATCH all pass.

`kubectl port-forward` cannot serve this page. It forwards a port to a pod, gives
the browser no credential, and leaves every call to the apiserver a cross-origin
request to a server that sends no CORS headers unless it was started with
`--cors-allowed-origins`, which no adopter has set.

### What that costs, said plainly

`kubectl proxy` forwards every API path except pod exec and attach, on the same
origin as the page, under the viewer's kubeconfig. So the page runs with the
**viewer's entire cluster authority**, not with the roles `logweir.yaml` ships:
`logweir-viewer`, `logweir-operator` and `logweir-approver` bind the **user**,
and under this serving path they bind nothing at all about the page. Anyone who
runs the UI from a cluster-admin kubeconfig gives the shipped bundle
cluster-admin. That residual is why there is no telemetry in this bundle, why
nothing in it is fetched from anywhere else, and why its contents are listed by
digest in the release notes.

**Two flags are the one-line escalation of exactly that residual, and neither may
change:**

- `--address=127.0.0.1` -- the proxy binds loopback only.
- `--disable-filter` -- **never pass it.** The default, `false`, keeps the
  `--accept-hosts` cross-site request filter on; the shipped client's default is
  `--accept-hosts='^localhost$,^127\.0\.0\.1$,^\[::1\]$'`.

**Changing either turns a local page holding your cluster authority into a network service holding it.**
On the LAN, unauthenticated, with your cluster credential attached to every
request it receives.

### The hardened alternative: a kubeconfig that holds less

The residual above is the viewer's own authority, so the way to narrow it is to
start the proxy under a kubeconfig that holds less. Bind a subject to
`logweir-viewer` (read) and, if the page should be able to write,
`logweir-operator` -- and nothing else:

```bash
kubectl --context docker-desktop create rolebinding logweir-ui-viewer \
  --clusterrole=logweir-viewer --user=logweir-ui -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-ui-operator \
  --clusterrole=logweir-operator --user=logweir-ui -n <namespace>
```

Then build a throwaway kubeconfig that carries that subject and nothing else,
and serve from it. The context in it is **named `docker-desktop` on purpose**, so
the serving command above is unchanged and still names its context explicitly:

```bash
export KUBECONFIG="$PWD/logweir-ui.kubeconfig"
kubectl config set-cluster docker-desktop --server=https://127.0.0.1:6443 --certificate-authority=<ca.crt> --embed-certs=true
kubectl config set-credentials logweir-ui --client-certificate=<logweir-ui.crt> --client-key=<logweir-ui.key> --embed-certs=true
kubectl config set-context docker-desktop --cluster=docker-desktop --user=logweir-ui --namespace=<namespace>
kubectl config use-context docker-desktop
kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1
```

`kubectl config` writes the kubeconfig itself and takes no `--context`. Other
commands name either `--context docker-desktop` for this guide's local examples
or the deliberately selected `$LOGWEIR_CONTEXT` in the production CRD-upgrade
procedure.

Under that kubeconfig a 403 from the page is the API server refusing
`logweir-ui`, which is the story the page tells: every error it shows carries the
API server's own `reason` and `message`, verbatim, because the page's whole
authorisation story is "the API server evaluated the viewer's RBAC".


### What the page can and cannot write

`api.js` validates every write against a frozen allowlist of five plurals --
`kafkaclusters`, `backupschedules`, `backups`, `restores`, `approvals` -- and
refuses anything else before the request is built. `trustrosters` is deliberately
absent: it is cluster-scoped, it carries the public key material every approval
check reads, and a namespace tenant that could write one could widen its own
allowlist. The page also has exactly one update beyond `create`: a JSON-merge
patch over a `BackupSchedule` touching `spec.suspend` and nothing else, which is
the one field that CRD's own `x-kubernetes-validations` rule leaves mutable.

Those are the page's limits, not the cluster's. The API server's limits are
whatever the kubeconfig that started the proxy carries, which is the subject of
"What that costs" above, and they are the ones that actually hold.

### The gates that keep it that way

`just lint` runs `scripts/check-ui-offline.sh`, which reads every shipped byte
under `ui/` except `*.md` and `tests/` and fails, naming file and line, on any
external resource, any credential in the page, any browser-storage write and any
module specifier that is not relative. `crates/logweir/tests/ui_lint.rs` asserts
the rest from the Rust side, including that the serving command above is byte
identical in `ui/README.md`, in this document and in the `ui` recipe of the
`justfile`.

## 17. Approving a restore out of band

A `Restore` does not run because it exists. It runs when an `Approval` naming it
carries a signature by a key the cluster-scoped `TrustRoster` lists, over the
exact bytes of that restore's plan. Nothing in this product can produce that
signature: `weirkeeper` holds no approver key, and the UI holds no key of any
kind. The approver signs on their own machine, and this section is that flow as
an operator performs it.

### Why both names are minted before either object exists

`Restore.spec.approvalRef` names an `Approval`, `Approval.spec.subjectRef` names
the `Restore`, and **both `.spec`s are sealed by an object-level CEL rule**
(`self == oldSelf`; every `.spec` in this group except
`BackupSchedule.spec.suspend`). So neither reference can be filled in afterwards,
and "create one, then create the other and point them at each other" is not a
sequence that exists.

The rule is therefore: **mint both `metadata.name`s from the plan bytes first.**

    restore-<first 8 hex of sha256(planBytes)>
    approval-<the same 8 hex>

Create the `Restore` **first**, with `spec.approvalRef.name` set to an approval
that does not exist yet. The reconciler sets `Admitted=False` with reason
`ApprovalNotVerified` and **requeues every 30 s** until the `Approval` arrives;
it creates no Job in the meantime. Create the `Approval` second. Neither name is
ever edited, because neither spec can be -- and the shared suffix is what lets
you read the pair off one `kubectl get` without a join.

### The flow

**1. Render the plan and read its hash.** The restore wizard (§16) renders the
plan document, shows its sha256 and shows both minted names before either object
exists. The plan document's grammar is the runner's own `restore.yaml` -- the
controller writes `spec.planBytes` verbatim into the runner's plan `ConfigMap` as
`data["restore.yaml"]` and the runner parses it with `serde_yaml` -- so what the
wizard emits is a document `logweir restore run --spec` accepts and nothing else.

**2. Get the exact bytes.** Use the wizard's **download** button, or take them
from the cluster after the `Restore` exists:

```bash
kubectl --context docker-desktop get restore <name> -o jsonpath='{.spec.planBytes}' > <name>.yaml
```

Copying out of the page is the one route that can lose bytes: several browsers
strip trailing whitespace from a `<pre>` copy, and a plan whose last line ends in
spaces has a different sha256 from the one the page showed. Hash exactly what you
downloaded.

**3. Sign it, on the machine that holds the private key.** Nothing about this
step involves the cluster or the page:

```bash
logweir drill approve --spec <file> --key <privkey> --approver <id> --ticket <id>   --subject-kind Restore --out <file>
```

`--out` is shown because it defaults to `approval.json` in the caller's current
working directory, and the DSSE sidecar lands beside it with the extension
replaced by `.sig`. The command prints

```
plan_hash  sha256:<64 lowercase hex>
```

which must equal the hash the wizard showed, character for character. If it does
not, you signed different bytes.

**4. Record it.** The approvals page takes the two files as text and does one
`create` on `approvals` with them **verbatim** -- never base64, and never parsed.
It refuses a file whose name ends `.pem` or `.key`, or whose content carries a
private-key header, with the message *this page never accepts a private key*,
and clears the refused text from the field. The object it posts carries all four
spec fields, and **every one of them is read from the `Restore` itself**: the
page GETs the Restore the route names, takes `metadata.name`, `metadata.uid` and
`spec.approvalRef.name` off it and computes the sha256 of its own
`spec.planBytes` in the browser, shows all of them read-only, and at submission
checks both that what it SHOWS is what it would SEND and that the Restore is
still the object it read. The route only says WHICH Restore; a link whose
`hash` or `name` disagrees with that Restore is refused and no form is offered
from it. The page is still forbidden from parsing the two documents, so no value
is ever lifted out of `approval.json` -- and the controller recomputes the hash
from the referent's own bytes regardless (checks 7 and 9).

**5. The controller decides.** It recomputes the plan hash from the referent's
own bytes, resolves the signing key id out of the signature (the **matched** key
id, never the first one listed), checks that id against
`TrustRoster.spec.approverKeys[]`, compares the `payloadType` in full, and
compares the subject *kind* inside the signed bytes with the referent's actual
kind -- so a restore's approval can never authorise anything else. Only then does
`Approval.status.verified` become `true`, the `Restore`'s next reconcile passes,
and the Job is created.

An `Approval` that exists is not an approval. One whose status the controller set
to `Verified=True` is. A submission that does not verify stays in the cluster
with `Verified=False` and its reason, which is the record of a rejected attempt
and is worth keeping.

### Checking a key out of band

A roster row says which key id the controller will accept. It cannot say that the
material behind that id is still the one its holder has -- that is what an
undisclosed rotation changes and nothing in the cluster observes. Ask the holder
for the public half and compute its fingerprint yourself:

```bash
openssl pkey -pubin -outform DER -in <key>.pub.pem | openssl dgst -sha256
```

The digest is the `keyId` the roster carries and the `matchedKeyId` the
controller records.

### Editing the roster

`TrustRoster` is cluster-scoped and admin-only, so the keys page renders this and
does not submit it (the plural is absent from the page's writable set, and
`api.create` refuses it by name before any request is built):

```yaml
apiVersion: logweir.dev/v1alpha1
kind: TrustRoster
metadata:
  name: default
spec:
  approverKeys:
    - keyId: <sha256 of the DER SPKI, lowercase hex>
      spkiPem: |
        -----BEGIN PUBLIC KEY-----
        <the approver's PUBLIC half>
        -----END PUBLIC KEY-----
      subject: <who holds it>
      notAfter: "2027-01-01T00:00:00Z"
  signingKeys:
    - keyId: <sha256 of the DER SPKI, lowercase hex>
      spkiPem: |
        -----BEGIN PUBLIC KEY-----
        <the runner's PUBLIC half>
        -----END PUBLIC KEY-----
      subject: <the runner identity>
      notAfter: "2027-01-01T00:00:00Z"
  allowedClusterIds: []
```

```bash
kubectl --context docker-desktop apply -f roster.yml
```

`signingKeys[]` carries key **material**, not ids: `verify_evidence` resolves a
runner's signing key from the roster and has nothing to verify against without
it. An empty `signingKeys[]` makes every verification `NotAttempted`, and the UI
renders that as what it is.

### Editing a plan

There is no such thing. `Restore.spec` is immutable, so the wizard's "edit"
prefills a **new** draft, mints new names from the new bytes, and says so in the
page:

> Restore.spec is immutable. This creates a NEW Restore with a new plan hash; the
> existing approval does not cover it.

The old approval still covers the old restore, which is the correct outcome: an
approval binds bytes, and these are different bytes.

## 18. Demo 1 in CI

[scripts/demo-steps.sh](../scripts/demo-steps.sh) defines the twelve-step walk
shared by the laptop and kind drivers. The kind driver sets
`LOGWEIR_KUBE_CONTEXT=kind-logweir`, discovers the Docker network's IPv4
gateway, installs an idempotent CoreDNS `hosts` mapping for
`host.docker.internal` with `fallthrough`, and probes Kafka from a pod before
starting the walk. The bootstrap stays `host.docker.internal:9095`.
`extraPortMappings` maps the opposite direction and does not solve this path.

The workflow selects `LOGWEIR_INSTALL_PATH` from the dispatch input, repository
variable, or `author-only` default. The author-only branch loads locally built
images. The published branch instead uses
`vars.LOGWEIR_PUBLISHED_RUNNER_REF` and waits for the pulled controller to
become Ready. After checking the unmodified install, the demo supplies
`LOGWEIR_RUNNER_IMAGE`; local image and pull-policy overrides affect only the
live Deployment.

Recorded evidence: [kind demo run 34700987743](https://github.com/VladyslavHaina/logweir/actions/runs/34700987743),
commit `a113dd2`, 2026-09-12, completed all twelve steps using author-only
images. It closed the CI demo requirement, not the image-publication requirement.
The earlier local arm64 kind probe established DNS and host-port access but
could not start the amd64 runner; it is not a full demo pass.

CI exercises the mechanical half of X-UIWRITE using the page's own request
emitter. The actual browser interaction is recorded in
[e2e/k8s/laptop-demo.md](../e2e/k8s/laptop-demo.md). A `fieldManager=logweir-ui`
value alone is not proof of browser interaction: an API client can supply it.

## 19. The Helm chart

[charts/logweir/README.md](../charts/logweir/README.md) is the canonical chart
reference, including all values, registry overrides, optional Kafka resources,
MinIO, demo brokers and the in-cluster UI. [install.md](install.md) path (c)
connects it to the same key, Secret and roster prerequisites as the manifests.

The chart copies its CRDs from `config/`. The UI is packaged separately by
`Dockerfile.ui`, which copies `ui/` into `/ui` over a pinned kubectl base.
The chart carries no duplicate UI files or asset ConfigMap. `just smoke-ui`
compares the image’s served assets with `ui/`.
`just chart-check` checks CRD parity, rendered manifests, schema and Helm lint;
`chart_lint.rs` checks the control-plane contract. Helm installs CRDs from
`crds/` but does not upgrade or delete them; manage CRD changes separately.

The controller, runner and optional UI images default to mutable `latest`
tags. All three pull policies default to `Always`; `ui.imagePullPolicy` can
be overridden for local images. The base manifests and compiled runner default remain digest-pinned;
third-party chart images remain pinned too. The author-only values use locally
loaded tags with `Never`. Runner pull policy reaches Jobs through
`LOGWEIR_RUNNER_PULL_POLICY` (§14).

The optional UI uses its own ServiceAccount, not the viewer's kubeconfig.
Anyone who can reach its Service acts with that account's authority. The
proxy restricts paths to `/ui/` and the Logweir API; the chart has no Ingress.
The chart reference documents bindings and the port-forward command. For the
laptop proxy's distinct authority, see §16, Serving the UI.

Recorded evidence from 2026-09-12: the local Helm walk and
[CI run 34718123956](https://github.com/VladyslavHaina/logweir/actions/runs/34718123956)
(commit `1f77f79`) completed a scheduled Backup, a scratch Restore, both
independent verifiers and UI checks (200 for its three allowed requests;
403 for pod exec and core API). Local follow-ups exercised `Never` and
`IfNotPresent` runner policies. These used author-only images and do not prove
that the chart's default registry references are published. A local walk on
2026-09-14 additionally exercised the image-based UI: the served `app.js`
matched the checkout, with the same three allowed and two rejected requests.
Consult
[tag1-checklist.md](tag1-checklist.md) for release gates.

## 20. The saved-connection contract (`KafkaCluster`), version 1

One `KafkaCluster` is one saved connection, and **one function resolves it**:
`weirkeeper::connection::resolve(cluster, use)`. The probe Job, the backup Job
and the restore Job are all built from that single resolution, so a probe can
never report `reachable: true` for settings a backup then dials differently.
The `use` argument selects the runner variable family (source or target) and
the execution context; it never changes an answer, so a future discovery or
preflight check refuses exactly what a run refuses.

`spec` stays CEL-immutable. Changing a connection means creating a new object.

### 20.1 The fields, and what each one is

| Field | Absent means | Reference or value |
|---|---|---|
| `bootstrapServers` | required, at least one entry | value (public address) |
| `auth.mode` | required: `plaintext` or `scramSha512` | value |
| `auth.username` | required for `scramSha512` | value (public identity) |
| `auth.secretRef.name` | required for `scramSha512` | **reference** to a Secret in this namespace |
| `auth.secretRef.passwordKey` | `password` | key name, not a value |
| `auth.tls` | `false` | value (the only switch that turns TLS on) |
| `auth.tlsCa` | the runner image's `ca-certificates` and the engine's bundled roots | **reference** to a Secret or ConfigMap key in this namespace |
| `role` | required: `source` or `target` | value |
| `markerTopic` | no marker topic; a `mode: scratch` restore against this cluster is refused | value |

**Every absent field above behaves exactly as it did before contract v1
existed.** An object that names neither `passwordKey` nor `tlsCa` produces the
same Job environment, the same mounts, the same ServiceAccount, the same
`backup.yaml` and the same `allowed-clusters.json`, byte for byte; the
`legacy-golden` fixtures in `crates/weirkeeper/tests/fixtures/connection/` are
output captured from the pre-contract controller at `4956785` and are compared
against, not re-derived. Two differences in that comparison are **PLAT-06.1's**
and not this contract's, and the fixture test names them rather than tolerating
them: the runner argv is derived from the run identity instead of read off the
`logweir.dev/runner-argv` annotation, and the plan ConfigMap gained the
`execution-inputs.json` snapshot, `immutable: true` and its two annotations
(§9).

A credential and a certificate are **references, never values**. The
controller holds no `get` on Secrets (§9); it validates only that a reference
has a legal shape (a DNS-1123 name, a legal `data` key) and writes it into the
pod spec. Whether the named object exists is answered by the kubelet when it
starts the pod, not by a controller read.

### 20.2 TLS and the private CA

`auth.tls` is the transport switch and `auth.tlsCa` is trust material. A CA
reference can only **add** trust to a transport that is already TLS; it never
turns TLS on. That separation is the whole rollback story in §20.5.

`auth.tlsCa` names exactly one of `secretKeyRef` or `configMapKeyRef` — one
object name and one key holding PEM certificate(s). A CA certificate is
public, so a ConfigMap is an ordinary home for it. The key is projected
read-only into the runner pod as `ca.crt` under `/connection/source-ca` or
`/connection/target-ca`, and its path — **the path, never the certificate
text** — is the value of `LOGWEIR_SOURCE_TLS_CA_FILE` or
`LOGWEIR_TARGET_TLS_CA_FILE`.

A runner has **two** TLS clients with two trust stores: librdkafka uses the
image's `ca-certificates`, and the engine its bundled roots (Global Constraint
29). The runner reads that one variable once and hands the path to both —
librdkafka's `ssl.ca.location` and the engine's `ssl_ca_location` — so they
cannot disagree about what they trust. With `ssl.ca.location` set, librdkafka
does **not** also consult the default verify paths: the connection trusts
exactly the projected CA. `ssl.endpoint.identification.algorithm` is pinned to
`https`, so broker hostnames are verified and an upgrade cannot quietly stop
verifying them.

**A private CA is a trust anchor and not an extra.** Supplying `tlsCa` for a
connection that is not TLS is refused at three independent points — the CRD's
CEL rule, the resolver, and the runner's own client construction — because an
author who configured a trust anchor believes the connection is verified, and
dialling it in the clear would be a silent downgrade. Removing the CA from a
TLS connection that needs it makes the dial **fail**, closed; it never falls
back to plaintext.

### 20.3 Rotation

Nothing is copied, so nothing has to be re-entered.

Rotating the password means writing the new value into the Secret the
connection already names. The **next** Job the controller creates resolves the
reference at container start and gets the new value; a Job already running
keeps the environment it started with and finishes against the credential it
began with. The same holds for the CA file, which the kubelet projects at pod
start. No `KafkaCluster` edit, no new Secret and no re-approval is involved.

A Job that starts **after** the broker credential changed but **before** the
Secret did — or the reverse — fails the way it always has: the runner cannot
render or authenticate the credential and exits 3 with
`CredentialNotRenderable`, or the broker rejects the SASL exchange and the run
reports the cluster unreachable. Rotate the Secret and the broker together,
then start a new run.

The username, the bootstrap servers and the TLS switch are **identity, not
credentials**: a restore plan binds them in `planBytes` and therefore in the
approval's plan hash, so changing them invalidates an existing approval. The
password and the CA are bound by no hash at all — binding them would turn
every rotation into a re-approval.

### 20.4 What is refused, and when

Every refusal below happens **before any ConfigMap, plan or Job exists**, and
lands on the object's status as a named terminal state with the offending
field. None of them is ever dialled.

| Configuration | State |
|---|---|
| `auth.mode: plaintext` with `auth.tls: true` (TLS without SASL) | `ConnectionConfigInvalid` |
| `auth.tlsCa` with `auth.tls: false` | `ConnectionConfigInvalid` (also rejected at admission by CEL) |
| `auth.tlsCa` naming both or neither of `secretKeyRef`/`configMapKeyRef` | `ConnectionConfigInvalid` (also CEL) |
| no `bootstrapServers`, or an entry that is empty or carries a comma or whitespace | `ConnectionConfigInvalid` |
| `auth.mode: scramSha512` with no `auth.username` | `CredentialNotRenderable` |
| `auth.mode: scramSha512` with no `auth.secretRef.name` | `CredentialNotRenderable` |
| a Secret/ConfigMap name that is not a DNS-1123 subdomain, or a key that is not a legal `data` key | `ConnectionReferenceInvalid` |
| a Job in a namespace other than the `KafkaCluster`'s | `ConnectionReferenceInvalid` |
| a field the running controller does not implement | `ConnectionFieldUnsupported` |
| an approved restore plan whose `target` is not this connection | `ConnectionPlanMismatch` |

**References never cross a namespace.** `auth.secretRef` and `auth.tlsCa` have
no `namespace` field, by construction: a cross-namespace reference is a
privilege-escalation surface, because the referrer's RBAC does not cover the
referent's namespace. A Secret that exists only in another namespace is simply
not found by the kubelet, and the pod never starts.

A refusal on the `KafkaCluster` itself **clears `status.reachable`** rather
than leaving a stale `true`. A `Restore` admits a target only on
`reachable: true`, and a `true` written by an earlier controller that dialled
different settings is not an observation this controller stands behind.

### 20.5 Upgrade and rollback

**Upgrade: apply the CRD, then the controller.** Helm installs CRDs from
`crds/` but never upgrades them (§19), so apply
`config/crd/kafkaclusters.yaml` yourself before rolling the controller:

```
kubectl --context <ctx> apply -f config/crd/kafkaclusters.yaml
kubectl --context <ctx> -n <ns> set image deployment/weirkeeper weirkeeper=<new image>
```

In the other order the API server **prunes** `passwordKey` and `tlsCa` from
any object that names them, because pruning is what an installed CRD that does
not declare a field does. An object would then resolve without the CA its
author asked for. The new controller therefore also refuses an object carrying
a field it does not implement (`ConnectionFieldUnsupported`, naming the
field) instead of resolving a connection whose meaning it does not know.

**Before you upgrade: four existing shapes stop being accepted.** The API
server never validated any of them, so an object written under an earlier
release can carry one and has been running. Each becomes a named refusal on the
first reconcile after the upgrade, with `status.reachable` cleared and no probe
Job created. **`KafkaCluster.spec` is CEL-immutable, so every one of them is
fixed by delete-and-recreate, not by an edit** — and a `Backup` or `Restore`
whose referent is refused goes terminal, because its own `spec` is immutable
too. Audit for them first:

| What the object carries | What the operator sees | Why it is refused rather than dialled |
|---|---|---|
| a `bootstrapServers` **entry** that is empty, or carries a comma or whitespace — e.g. the single entry `"b0:9092, b1:9092"` | `Reachable=Unknown`, reason **`ConnectionConfigInvalid`**, message naming `spec.bootstrapServers[<i>]` | the probe joins the list with commas and a backup plan keeps it as a list, so one such entry is **two different dials on two paths**. The old probe happened to work; the plan the backup ran did not name the same brokers |
| an `auth.secretRef.name` that is not a DNS-1123 subdomain, or an `auth.secretRef.passwordKey` outside `[-._a-zA-Z0-9]+` | `Reachable=Unknown`, reason **`ConnectionReferenceInvalid`**, message naming the field | the kubelet could never resolve it, so the pod would fail at container start with no controller-side explanation |
| `auth.mode: plaintext` with `auth.tls: true` | `Reachable=Unknown`, reason **`ConnectionConfigInvalid`**, message naming `spec.auth.tls` | earlier releases **dialled it in the clear**. Refusing is the only answer that is not a silent downgrade |
| `auth.mode: scramSha512` with no `auth.username`, or no `auth.secretRef.name` | `Reachable=Unknown`, reason **`CredentialNotRenderable`**, message naming the field | it used to produce a Job with no credential variable that reported `reachable: false` — a configuration mistake reported as a fact about somebody's cluster |

`ConnectionConfigInvalid`, `ConnectionReferenceInvalid` and
`ConnectionFieldUnsupported` are **not terminal on the `KafkaCluster` itself**:
they are re-evaluated on every reconcile, so rolling a controller that
understands the object forward clears them with no edit. They **are** terminal
for a `Backup` or `Restore` that references such an object, whose `spec` and
referent cannot change. The full refusal table is §20.4.

**Rollback: the CRD may stay ahead of the controller, and a TLS-CA object
fails closed.** Roll the controller Deployment back to the previous image and
leave the CRD in place. Then:

- An object that names **neither** new field is unaffected. This is the
  measured legacy case: same Job, same plan, same environment.
- An object that names only `auth.secretRef.passwordKey` reverts to the old
  controller's behaviour, which always projects the `password` key. If the
  Secret carries the password under a different key, the pod fails to start
  with `CreateContainerConfigError` naming the missing key — **visible and
  closed**, never a run with no credential.
- An object that names `auth.tlsCa` is the case that matters. The old
  controller does not know the field, so it projects no CA volume and no
  `LOGWEIR_*_TLS_CA_FILE`. It still reads `auth.tls`, so it still builds a
  **TLS** connection — and that connection then fails to verify the broker
  certificate, because a private CA is by definition not in the image's public
  roots. The run **fails closed** with a TLS handshake error. It does not, and
  cannot, fall back to a plaintext dial: `auth.tls` alone decides the
  transport, and the CA reference can only ever add trust to it. That is why
  the TLS switch was not folded into `auth.tlsCa`.

An old **runner** image behaves the same way: it ignores an environment
variable it does not read, so it dials TLS with the image's default roots and
fails to verify. Roll the runner image forward with the controller.

### 20.6 A frozen run and a changed connection

A `Backup` freezes what this resolver decided **before** its Job exists (§9):
the bootstrap addresses, the auth mode, the SCRAM username, the TLS flag and
the `auth.tlsCa` reference go into the immutable `execution-inputs.json`
snapshot. The password reference does not — it is derived from the pinned
`KafkaCluster` at Job-build time and resolved by the kubelet, so a rotation
needs no new plan (§20.3).

That is what makes a changed connection visible rather than silent. A later
pass re-resolves the same `KafkaCluster` and compares; a `tlsCa` that now names
another object or another key, a changed address list, a changed username or a
flipped TLS switch is terminal `PlanConfigMapConflict`, and the frozen plan
bytes are never executed against it. A snapshot frozen before `tlsCa` existed
carries no `tlsCa` key, re-encodes to the same bytes and is still admitted
under grammar `logweir.dev/backup-execution-inputs/v1`, which this controller
still reads beside the `logweir.dev/backup-execution-inputs/v2` it writes
(§10).

### 20.7 Redaction: no credential value leaves the Secret

For this contract the statement is unconditional and is asserted by seeding
recognisable values and grepping every rendered output
(`crates/weirkeeper/tests/connection.rs`):

- **No API response or status field** carries a password or a CA private key.
  A refusal message names fields, object names and data keys, and the resolver
  has no value to name in the first place.
- **No log line** carries one. The controller never reads a Secret, and
  `AuthConfig`'s `Debug` prints `password: "***"` by hand rather than by
  derive.
- **No ConfigMap** carries one. The plan ConfigMap holds bootstrap servers,
  auth mode, username, TLS and — in the frozen snapshot only — the `auth.tlsCa`
  reference, all of them public settings or a pointer to public certificate
  material. The credential `secretKeyRef`, its `passwordKey` and the
  object-store credential reference are not written into it at all (§9).
- **No Job manifest** carries one: the password is `valueFrom.secretKeyRef`
  and the CA is a projected volume, both resolved by the kubelet.
- **No download or evidence artifact** carries one. A receipt records the
  username, which is identity.
- **No readback path exists.** `connection::credential` can build a
  credential Secret for a write-only entry flow and can never read one; the
  caller keeps `metadata` from the create response and nothing else.

The CA **certificate** is public by nature and may be held in a ConfigMap. A
CA **private key** has no place in this contract, in any object Logweir reads,
or in a ConfigMap (§9).

**Prefer `auth.tlsCa.configMapKeyRef` to `secretKeyRef`** unless something else
already keeps the certificate in a Secret. Both are equally safe — a name is
not a grant, and reading the plan confers nothing on the referenced object —
but a Secret-backed CA is the one case where a Secret's name and key appear in
a plan ConfigMap (§20.6), and an adopter whose policy is "no Secret name in a
ConfigMap" gets that for free by putting the certificate where public material
belongs.

## 21. `Preflight`: what a readiness check proves, and what it cannot

A `Preflight` answers one question — *would this operation fail, and why?* —
by running **one short-lived Job in the execution pod's own shape**: the same
image, the same ServiceAccount, the same Secret projections, the same CA and
signing mounts, and `automountServiceAccountToken: false`. Nothing else can
answer it. The controller holds no verb on `secrets`, and "the bytes exist" is
not "the credential works".

```bash
kubectl --context docker-desktop get preflight -n team-a
# NAME    OPERATION   PHASE       RESULT     EXPIRES   AGE
# pf-9a1b Restore     Completed   notReady   14m       28s
```

### 21.1 A `ready` verdict authorizes nothing

Every execution-time guard still runs and **none of them reads a `Preflight`**:
the backup controller's glob rail and destination admission, the runner's
phase −1 source rails, the restore admission checks 0–9, phase 0's collision
check and LogAppendTime probe, G-WIN in phase 5, and PLAT-02.2's signer
validation. That is not a convention — `preflight_controller.rs`'s
`no_execution_path_reads_preflight_or_discovery` is a source scan over the four
execution reconcilers and both runner paths, and its planted mutant is an
`import` of this kind into `controllers/restore.rs`.

The reason is the shape of the problem. A preview is a statement about a
moment; a run happens later. A colliding topic created in between is caught at
execution and nowhere else, and a reconciler that trusted a green badge would
have removed the only check that could see it. A client — the API or the UI —
**may** require an applicable `ready` preflight as friction before submitting a
run. It may never bypass a guard, and the absence of a preflight is never a
server refusal.

### 21.2 `Completed` is not `ready`, and `Failed` is neither

| `phase` | What happened |
|---|---|
| `Pending` | the plan is rendered and the Job is being created |
| `Queued` | a concurrency ceiling is holding it back (`ConcurrencyLimited`) |
| `Running` | the Job exists and has not finished |
| `Completed` | **a result exists.** The verdict is `status.result.state` |
| `Failed` | **no result could be produced** — `ResultUnreadable`, `RunnerContractUnsupported`, `DeadlineExceeded`, `Stalled` |
| `Cancelled` | `spec.cancelRequested` was set and the Job's deadline was collapsed |

`status.result.state` is `ready`, `notReady` or `unknown`, aggregated exactly
as D2 §6.4 states it: `notReady` if any blocking check is `notReady`;
otherwise `unknown` if any blocking check is `unknown` **or skipped**;
otherwise `ready`. An EMPTY check set is `unknown` and never `ready` —
"nothing was checked" is not "everything passed".

A pod that never started is a real answer and not a failure: a missing Secret
is reported as `connection.credentialProjected notReady
CredentialSecretNotFound` **with a remedy**, and every check the Job would have
answered becomes `unknown` with `BlockedByPrerequisite` rather than silently
passing. The Job is cancelled immediately in that case — waiting out
`activeDeadlineSeconds` for a Secret that does not exist is the experience this
kind exists to replace.

### 21.3 Every row carries a code, a remedy, a scope and a check time

`status.result.checks[]` is the whole verdict, and each entry says who
answered:

| `authority` | Who |
|---|---|
| `controller` | facts about Kubernetes objects — the connection, the destination, the plan, the recovery point, the approval, the roster, the policy |
| `checkJob` | what the pod observed with real credentials — SASL, the bucket, the manifest, the target's topics |
| `podStatus` | what the kubelet said about a pod that has not run yet |

`gating` is `blocking`, `advisory` or `executionOnly`. **An
`executionOnly` row is `unknown` forever and says so**: `connection.topicsReadable`,
`destination.archivePrefixWritable`, `target.logAppendTime` and
`configuration.egress` cannot be proved without doing the thing they are about.
`configuration.egress` in particular is decided by a NetworkPolicy no check can
observe; its remedy names the broker and object-store ports instead.

Two fields of the per-check record of D2 §6.4 are deliberately **not** in the
CRD: `facts` and `detail`. A free-form JSON object in a structural schema needs
`x-kubernetes-preserve-unknown-fields`, which turns off pruning and makes a
status a place arbitrary bytes can be parked. The non-secret facts an operator
needs — `clusterId`, `brokerCount`, `imageID`, `signerKeyId` — are rendered
into `message` as `[key=value; …]`, and the long form (missing segment keys,
colliding topic names) goes to the immutable `ConfigMap` named by
`status.result.detailsRef`.

**Nothing in a status is a raw error.** Every message and remedy passes the
redaction chokepoint: URL userinfo, AWS access key ids, secret and token value
forms, PEM blocks, S3 XML bodies and long base64 or hex runs are replaced, and
the result is capped. That cap is why a message quotes a SHORT digest
(`sha256:1f4c2b8a9e07…`) while the whole hash is in `status.binding.planHash`,
which is a field and not prose.

### 21.4 Staleness: a green preview cannot outlive its inputs

`status.binding` is recorded **before** the Job is created, from the objects
the pass resolved: the recomputed `planHash`, every referent with its UID and
generation, the CA digests, the roster, the approval's resource version and the
policy digest, reduced to one `inputsDigest`.

A stored result applies only while **all** of these hold:

- `phase: Completed`;
- `now < status.result.expiresAt`;
- the recorded `planHash` equals the draft's;
- the recomputed `inputsDigest` equals the recorded one.

So, concretely:

- editing the **target**, the **recovery point**, the **point in time**, the
  **topic subset** or the **mapping prefix** changes the plan bytes, so the
  hash changes and the result is stale;
- choosing **another destination**, or a **recreated** one, changes a referent
  UID;
- **editing a destination's access or CA** changes its generation;
- **deleting and recreating a recovery point** changes a `Backup` UID, which is
  the identity PLAT-11.1 fixes;
- an **approval moving from pending to verified** changes its
  `resourceVersion`.

**What the binding does NOT cover, and it is not a complete list of triggers.**
Two inputs a preflight exists to prove are absent from `inputsDigest`:

- **The credential Secrets.** `BindingInputs` carries no Secret reference of any
  kind, so rotating the SASL password or the object-store access key between a
  green preflight and the run leaves the stored verdict "applicable". The
  preflight proved that the OLD credential worked.
- **The recovery point's topic set.** A `Backup` referent is recorded by UID
  alone (its `generation` is not meaningful), and `backup.spec.topics` — which
  `plan.bindings` compares the plan against — is not in the digest. Editing a
  recovery point's topic set is invisible to the applicability test.

Both are properties of the shared `BindingInputs` contract rather than of this
controller, and closing them means adding `secret.resourceVersion` and a
recovery-point topic digest to it. Until then, treat a `ready` verdict as a
statement about the objects **as they were**, and re-run the check after
rotating a credential.

Each row also carries its own `expiresAt`, and the verdict's is the soonest of
them. `target.mappedTopics` and `target.topicCreate` expire in **five minutes**
— the shortest in the catalogue, because a topic can appear on the target
between a preview and a run. `plan.parse` and `plan.names` carry **no** expiry:
they are functions of bytes, and `binding.planHash` already pins those.

The controller keeps the same promise from its own side. A terminal
`Preflight` whose result is still `ready` is revalidated: if `expiresAt` has
passed, or a referent moved under it, `status.result.state` is **downgraded to
`unknown`** and `status.message` names the reason
(`expired`, `planHashChanged`, `referentChanged:<Kind>/<name>`,
`caBundleChanged`, `policyChanged`, `inputsDigestChanged`). It is downgraded to
`unknown` and never to `notReady`: nothing was found wrong, the answer simply
stopped being about the current objects.

### 21.5 The one key a check may write, and only when you ask

`destination.evidenceWritable` is answered by actually creating the create-only
readiness marker `logweir/readiness/<destinationUid>.json` — but **only** when
the destination opts in with `spec.readiness.writeProbe: CreateOnlyMarker`. With
the field absent or `Disabled` nothing is written and the row is
execution-only with `WriteNotProbed`.

The opt-in is read from the object on every pass. It used to be hard-coded off,
which gave an operator who had opted in a row whose message said their
destination configured no probe — a status contradicting the spec, which is the
defect this kind exists to close.

### 21.6 A restore preflight writes nothing

No topic is created, altered or deleted. The collision answer comes from
targeted metadata per mapped name and from a **validate-only** `CreateTopics`
(`AdminOptions::validate_only(true)`) — both inside the check pod, with the
restore's own credential, and never from the controller. The controller patches
its own `/status`, its own Job's `ttlSecondsAfterFinished` (after the status
commit, so garbage collection cannot race the relay) and its own owned
`ConfigMap`s, and nothing else.

### 21.7 Skipping a check is not answering it

`spec.request.skipChecks` leaves a row out of the run. The row is still
reported, with `state: skipped`, and **a skipped blocking check keeps the
overall verdict `unknown`**. The same applies to the one row a draft cannot
answer: a `Preflight` over `planBytes` has no `Restore` for an approver to sign,
so `approval.state` is `skipped` with `SubjectNotCreated` and the verdict is
`unknown` however green everything else is.

### 21.8 What this build does not do

- **An inline `legacyArchive` / `legacySourceArchive` is not checked.** Turning
  an `s3://…` URL into the location a check plan needs is the legacy-addressing
  block of the installation policy, which belongs to the destination resolver.
  Such a request is reported `phase: Failed` with `ArchiveUrlUnreadable` and a
  message saying to create a `BackupDestination` and use `destinationRef`. It
  is never a verdict about the operation.
- **`recoveryPoint.state` cannot report `RecoveryPointLocationMismatch`.** The
  frozen destination snapshot does not reach `Backup.status` in this build, so
  there is no digest to compare and the comparison is skipped rather than
  guessed at.
- **No `gc.rs`.** A terminal `Preflight` is not deleted by the controller; the
  `weirkeeper` ClusterRole grants `delete` on nothing. Remove them with
  `kubectl delete preflight`, and the owner cascade takes the Job and the
  `ConfigMap`s with each one.
- **`signer.rostered` is EXECUTION-ONLY for a restore.** The verdict needs the
  runner's public `signerKeyId`, which rides on `signer.privateKeyUsable`; the
  landed `restorePreflight` check-plan contract carries no `signer_path`, so a
  restore check pod is never given a key to report. The row is still published,
  with `SignerKeyIdNotObserved` and a message naming the reason, and it does not
  gate — a blocking row nobody can answer would pin every restore preflight at
  `unknown`. The runner still validates its signer before it writes anything
  (PLAT-02.2). Closing it means adding `signer_path` to the restore request.
- **`destination.archivePrefixWritable` is not requested.** A backup readiness
  plan asks for the `archiveRead`, `evidenceWrite` and — when the destination
  configures one — `evidenceRead` grants. The archive-WRITE grant is what the
  run itself exercises, and its row's whole content is "verified by the run", so
  requesting it would add a line and no information.
- **`destination.evidenceReadable` is only requested when the destination
  configures an `evidenceRead` grant.** A check plan carries ONE credential for
  its destination, so probing a role the object leaves unconfigured would
  exercise the wrong credential and report a refusal about a grant nobody asked
  for. Absent means verification is `NotAttempted` (§7b), and the advisory row
  is then simply absent.
- **The binding covers no credential Secret and no recovery-point topic set** —
  see §21.4.

### 21.9 Upgrade and rollback

Additive in both directions. The kind, its two RBAC rules and the
`events: list` rule arrive with the controller image and leave with it; an
older controller that does not know the kind simply never reconciles a
`Preflight`, which then sits with no status and authorises nothing — which is
what it does when it is `ready`, too. Nothing on an execution path reads one,
so no `Backup`, `Restore` or `BackupSchedule` behaves differently on either
side of the upgrade. An absent `status.binding` means "never bound" and a
consumer must treat the result as inapplicable.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
