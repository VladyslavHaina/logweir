# Running Logweir on Kubernetes

Operational reference for the CLI CronJob and the `weirkeeper` controller.
A new operator starts at [quickstart.md](quickstart.md), *The supported path*,
which walks install to disaster restore and links here for each detail;
[install.md](install.md) is the deployment reference. The [CronJob example](../examples/cronjob-drill.yaml) schedules the CLI.
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

All four images are built for `linux/amd64` and `linux/arm64`. The runner
joined arm64 with PROD-00.2: its engine is Logweir's build of the vendored OSO
source, cross-compiled for each platform, where OSO publishes its own binary
for amd64 only. An arm64 node runs the runner natively, so a runner Job needs
no particular node architecture (the chart's node-placement values still do
not propagate to controller-created Jobs). The controller must be built
natively for its target architecture; `Dockerfile.weirkeeper` refuses
cross-architecture builds because its `aws-lc-sys` build needs native headers.
See [install.md](install.md) for the build and registry paths, and for the
amd64-only engine rollback (`ENGINE_SOURCE=oso`), whose Jobs need amd64 nodes.

Runner images published before PROD-00.2 are amd64-only. Docker Desktop's
local image store allowed such a runner on the author's arm64 host after a
host-side pull; this does not generalize to an arm64 `kind` node, whose CRI
image service did not expose the loaded amd64 image.

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
`approval.sig`, `approver.pub.pem`, and `allowed-clusters.json` — or, for a
standing-authorized **rehearsal**, `standing-authorization.json`,
`standing-authorization.sig`, `authorization-keys.json`, `approver.pub.pem` and
`allowed-clusters.json`, with no per-run approval slot at all (§7g). Keeping the
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
| `LOGWEIR_ENGINE_VERSION`, `LOGWEIR_ENGINE_DIGEST` | **Not needed in the runner image** since PROD-00.2: the image declares its engine in `/etc/logweir/engine-identity`, which is what is signed, and the controller never sets these two. Outside such an image they are mandatory: an empty value is refused with exit 1, because a signed scorecard must name the engine that produced the restore. Either way the run asks the engine for its `--version` and refuses one that does not match the identity it would sign. |
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

The repository includes the CLI, `weirkeeper`, the fourteen CRDs of §7, the
optional product API and console (`logweir-api`, [api.md](api.md)) and an
optional Helm UI deployment. The CLI runs the engine as a local subprocess inside its runner
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
| `Preflight` | Namespaced | One bounded readiness observation for a `Backup`, a `Restore`, a destination's grants or a source connection on its own, run the same way. `spec.request` is immutable; `spec.cancelRequested` moves `false` → `true` only. A `ready` verdict **authorises nothing**: every execution-time guard still runs. |
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

**Where `Restore.status.completion` comes from.** The controller copies it by
JSON pointer out of the run's signed scorecard and computes none of it:
`newTopics` (name and partitions) from `target_diff.would_create`,
`recordsExpected` and `recordsRestored` from `sample`, `recordsSampled`,
`recordsSampledMatching` and `integrityLevel` from `integrity`, and
`sampleWindow` from `sample.window_start`/`window_end`. It is written in the
same resourceVersion-preconditioned status patch that records the evidence
verdict over that scorecard, and **only for a run that passed**: the verdict is
`Valid` (trust basis `Current` or `Historical`), the scorecard's `outcome` is
`pass` **and** the run's `exitCode` is `0` — the `Restore` badge rule of §15.2.
An exit-2 run publishes its signed failure since interface I8's amendment, and
that failure verifies `Valid` like any document; its counts are still readable
off the scorecard and `status.integrity`, but it gets no completion panel and
no cutover guidance ("point applications at the new names…") over data that
did not reconcile. It is written by the verification write when the controller
reads the archive itself, or the evidence-fetch verdict write when the
evidence-fetch Job relays a scorecard bound to this run. It is never on the
terminal write, which lands before any verification. It stays **absent**
while verification is pending or `NotAttempted`, when the relayed document
names another run, for any run that did not exit `0` with `outcome: pass`,
and when the verdict is `Invalid` or `Untrusted` — the
completion panel has no trust caption of its own, so a scorecard whose
signature did not verify never reaches it. (`outcome`, `integrity` and
`measured` are still copied from a run-bound scorecard whatever the verdict;
the badge is what says whether to trust them.) A field the document does not
carry is omitted rather than written as zero. `recordsRestored` is
the count consumed back from the target **in the sampled window**, not the
restore's total record count — the console labels it *records verified in the
sampled window*, and the count that matched byte for byte is
`recordsSampledMatching` (*records sampled and matching*).

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

**How that kubelet answer reaches you.** The friendly code —
`CredentialSecretNotFound`, naming the Secret and the key — comes from the
pod-waiting classification. On a run it is `status.progress.diagnostics[]` and
the `RunnerReady` condition (§10, *`RunnerReady`, and the four states a run can
reach with no exit code*), and a run whose pod never started ends with that
reason rather than `NoExitCode`. Before any run, a `Preflight` with
`operation: DestinationAccess` runs a pod in the object's own namespace and
reports what the kubelet said.

#### The object-storage permission each grant actually needs, measured

**This table was bisected, not recorded.** What existed before 2026-09-21 was
the set of MinIO policies that happened to work when the destination contract
was first exercised — a starting point that D2 §15's item U6 asked to have
measured and that nobody had. Every row below was measured on docker-desktop
against real objects, with `e2e/k8s/d2/d2_live.py`'s `U6` phases: for each
role, the role's own operation was run once with the recorded set, once for
every single action removed from it, and once more with exactly the actions
whose removal broke it. **A removal counts only when the product itself
classified the failure** — an `AccessDenied` check row, or a run's own nonzero
`exitCode` and refusal text — never a timeout and never a crash.

To re-measure it, against a MinIO the harness deploys in a namespace it owns:

```bash
export LOGWEIR_PYTHON=/path/to/python3   # needs `cryptography`
$LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py u6setup u6point u6b u6f u6a u6c u6d u6e
$LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py u6table   # prints the rows below
```

The `Harness row` column names the phase that measured the line; the
`Harness object` column in the bisection table names the `Backup`,
`Preflight`, `RecoveryCatalog` or `Job` whose own status carries the answer.

| Role | Minimal actions, each at the resource scope shown | Proved by | Harness row |
|---|---|---|---|
| `archiveWrite` | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`); `s3:GetObject` on `<bucket>/<prefix>/*`; `s3:PutObject` on `<bucket>/<prefix>/*`; `s3:PutObject` on `<bucket>/logweir/*` | Backup `u6-bk-045` | `U6/archive-write` |
| `archiveRead` | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`); `s3:GetObject` on `<bucket>/<prefix>/*`. **For a restore of a catalog point (a plan bound to a recovery point) and its preflight, also:** `s3:GetObject` on `<bucket>/logweir/backups/*` and, on a versioned bucket, `s3:GetObjectVersion` on `<bucket>/<prefix>/*` — see *The read of a pinned version* below | Preflight `u6-da-010+u6-rp-011` (the first two actions); the point-bound additions were exercised on MinIO, not bisected (FX-14 `live-tip/grants/`) | `U6/archive-read` |
| `evidenceWrite` | `s3:PutObject` on `<bucket>/logweir/*` | Backup `u6-bk-033` | `U6/evidence-write` |
| `evidenceRead` | `s3:GetObject` on `<bucket>/logweir/*` | Preflight `u6-da-027` | `U6/evidence-read` |
| write probe | `s3:PutObject` on `<bucket>/logweir/readiness/*` | Preflight `u6-bp-019` | `U6/write-probe` |
| `catalogSync` reader | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `logweir/*`); `s3:GetObject` on `<bucket>/<prefix>/*`; `s3:GetObject` on `<bucket>/logweir/*`; on a versioned bucket, `s3:GetObjectVersion` on `<bucket>/<prefix>/*` — see *The read of a pinned version* below | RecoveryCatalog `u6-cat-052` (the first three actions) | `U6/catalog-sync` |
| retention enforcer | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`); `s3:DeleteObject` on `<bucket>/<prefix>/*` — **SUPERSEDED: this build also needs `s3:GetObject` on `<bucket>/<prefix>/*`**, see the note below the bisection | Job `u6-ret-060` (measured before OBJECT-LOCK-DELETE-MARKER) | `U6/retention-enforcer` |

| Role | Action removed | Verdict, and the product's own answer | Harness object |
|---|---|---|---|
| `archiveWrite` | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`) | **the operation fails**: `exitCode 1` / `operational`, the runner's own log naming the refusal | `u6-bk-035` |
| `archiveWrite` | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `logweir/*`) | the operation still succeeds — **not required** | `u6-bk-036` |
| `archiveWrite` | `s3:GetBucketLocation` on the BUCKET arn | the operation still succeeds — **not required** | `u6-bk-037` |
| `archiveWrite` | `s3:GetObject` on `<bucket>/<prefix>/*` | **the operation fails**: `exitCode 1` / `operational`, the runner's own log naming the refusal | `u6-bk-038` |
| `archiveWrite` | `s3:PutObject` on `<bucket>/<prefix>/*` | **the operation fails**: `exitCode 1` / `operational`, the runner's own log naming the refusal | `u6-bk-039` |
| `archiveWrite` | `s3:AbortMultipartUpload` on `<bucket>/<prefix>/*` | the operation still succeeds — **not required** | `u6-bk-040` |
| `archiveWrite` | `s3:DeleteObject` on `<bucket>/<prefix>/*` | the operation still succeeds — **not required** | `u6-bk-041` |
| `archiveWrite` | `s3:GetObject` on `<bucket>/logweir/*` | the operation still succeeds — **not required** | `u6-bk-042` |
| `archiveWrite` | `s3:PutObject` on `<bucket>/logweir/*` | **the operation fails**: `exitCode 4` / `signing-or-lock`, the runner's own log naming the refusal | `u6-bk-043` |
| `archiveWrite` | `s3:AbortMultipartUpload` on `<bucket>/logweir/*` | the operation still succeeds — **not required** | `u6-bk-044` |
| `archiveRead` | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`) | **the operation fails**: `archive.segments` → `AccessDenied`; `destination.archiveListable` → `AccessDenied` | `u6-da-004+u6-rp-005` |
| `archiveRead` | `s3:GetObject` on `<bucket>/<prefix>/*` | **the operation fails**: `archive.backupSet` → `AccessDenied`; `archive.segments` → `BlockedByPrerequisite` | `u6-da-006+u6-rp-007` |
| `archiveRead` | `s3:GetBucketLocation` on the BUCKET arn | the operation still succeeds — **not required** | `u6-da-008+u6-rp-009` |
| `evidenceWrite` | `s3:PutObject` on `<bucket>/logweir/*` | **the operation fails**: `exitCode 4` / `signing-or-lock`, the runner's own log naming the refusal | `u6-bk-029` |
| `evidenceWrite` | `s3:GetObject` on `<bucket>/logweir/*` | the operation still succeeds — **not required** | `u6-bk-030` |
| `evidenceWrite` | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `logweir/*`) | the operation still succeeds — **not required** | `u6-bk-031` |
| `evidenceWrite` | `s3:GetBucketLocation` on the BUCKET arn | the operation still succeeds — **not required** | `u6-bk-032` |
| `evidenceRead` | `s3:GetObject` on `<bucket>/logweir/*` | **the operation fails**: `destination.evidenceReadable` → `AccessDenied` | `u6-da-024` |
| `evidenceRead` | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `logweir/*`) | the operation still succeeds — **not required** | `u6-da-025` |
| `evidenceRead` | `s3:GetBucketLocation` on the BUCKET arn | the operation still succeeds — **not required** | `u6-da-026` |
| write probe | `s3:PutObject` on `<bucket>/logweir/readiness/*` | **the operation fails**: `destination.evidenceWritable` → `AccessDenied` | `u6-bp-013` |
| write probe | `s3:GetObject` on `<bucket>/logweir/*` | the operation still succeeds — **not required** | `u6-bp-014` |
| write probe | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `logweir/*`) | the operation still succeeds — **not required** | `u6-bp-015` |
| write probe | `s3:GetBucketLocation` on the BUCKET arn | the operation still succeeds — **not required** | `u6-bp-016` |
| write probe | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`) | the operation still succeeds — **not required** | `u6-bp-017` |
| write probe | `s3:GetObject` on `<bucket>/<prefix>/*` | the operation still succeeds — **not required** | `u6-bp-018` |
| `catalogSync` reader | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`) | the operation still succeeds — **not required** | `u6-cat-047` |
| `catalogSync` reader | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `logweir/*`) | **the operation fails**: `ResultUnreadable` | `u6-cat-048` |
| `catalogSync` reader | `s3:GetBucketLocation` on the BUCKET arn | the operation still succeeds — **not required** | `u6-cat-049` |
| `catalogSync` reader | `s3:GetObject` on `<bucket>/<prefix>/*` | **the operation fails**: `PartialScan` | `u6-cat-050` |
| `catalogSync` reader | `s3:GetObject` on `<bucket>/logweir/*` | **the operation fails**: `PartialScan` | `u6-cat-051` |
| retention enforcer | `s3:ListBucket` on the BUCKET arn (`s3:prefix` in `<prefix>/*`) | **the operation fails**: `state=Kept`, `code=ListRefused:AccessDenied` and `retention-result=deleted=0 failed=1 objects=0` | `u6-ret-056` |
| retention enforcer | `s3:GetBucketLocation` on the BUCKET arn | the operation still succeeds — **not required** | `u6-ret-057` |
| retention enforcer | `s3:GetObject` on `<bucket>/<prefix>/*` | measured before OBJECT-LOCK-DELETE-MARKER: the operation still succeeded — **SUPERSEDED: now required**; without it every key is `Kept` with `code=VersionProbeRefused` and nothing is deleted (see below) | `u6-ret-058` |
| retention enforcer | `s3:DeleteObject` on `<bucket>/<prefix>/*` | **the operation fails**: `state=Kept`, `code=AccessDenied` and `retention-result=deleted=0 failed=1 objects=0` | `u6-ret-059` |

**Since RECEIPT-DUP's fix, the `s3:PutObject` on `logweir/*` rows
(`u6-bk-029`, `u6-bk-043`) fail EARLIER, with the same exit code.** The runner's
first write under `logweir/` is now its create-only execution claim, put before
the engine starts, so without that grant the Backup exits `4` /
`signing-or-lock` naming `ExecutionClaimUnproven` with no archive written,
instead of after the engine. The claim needs no action the table does not
already grant. Re-measured on lab-refresh-10 (2026-09-24, row RD-3): without
`s3:PutObject` on `logweir/*` the run exits `4` `ExecutionClaimUnproven`, the
engine never starts, and nothing is written for that backup, in the archive or
under `logweir/backups/`.

**A wider grant than this table is not required by anything in this build.**
Every action outside a role's row was removed and the role's operation still
succeeded, on the same fixture, in the same run. `s3:ListBucket`'s two
`s3:prefix` legs are withdrawn separately, so a row naming one root has been
shown not to need the other: `archiveWrite` lists only under `<prefix>/*` even
though it writes its receipt under `logweir/`, and the `catalogSync` reader
lists only under `logweir/*` even though it reads manifests under the archive
prefix. Where a scope narrower than the whole evidence root is documented it is
the one measured: the readiness probe's `s3:PutObject` is granted at
`<bucket>/logweir/readiness/*` and nothing wider was needed. Three of them are worth
naming because the recorded policies carried them: **`s3:DeleteObject` is not
needed by `archiveWrite`** — the harness added it as a deliberate over-grant,
because D2 §3.11 states that no role is ever granted it, and the Backup
succeeded without it, so the code agrees with the constraint rather than merely
never being asked; that is the deployment property the margin in *A
`RetentionPolicy` in `Enforce` is the one thing Logweir does that cannot be
undone* rests on. **`s3:GetObject` under `logweir/*`
is not needed by `evidenceWrite`**, whose puts are create-only; and
**`s3:GetBucketLocation` is not needed by any role**, because the engine and
the store are given an explicit region and never ask the bucket for one.

**The write probe is the `evidenceWrite` principal's, and this is where that is
said.** `destination.evidenceWritable` creates its marker AS the destination's
`evidenceWrite` grant. When that grant is the one the check's destination
credential already came from (no `spec.access.evidenceWrite`, which falls back to
`archiveWrite`), nothing second is projected; when it is a different Secret or
ServiceAccount, the check plan names it (`evidenceWrite: {credentials, secretName
| serviceAccountName}`, references only) and the check pod is projected its keys
as `LOGWEIR_EVIDENCE_AWS_*` beside the unprefixed `AWS_*`, or runs as its
ServiceAccount. The row carries the fact `grant=evidenceWrite` or
`grant=destination` and names the principal. The probe asks that principal only
for create-only `PUT`s of the one key `logweir/readiness/<destinationUid>.json` — no
read, no list, no delete — so the `write probe` row's `s3:PutObject` on
`logweir/readiness/*` is already inside `evidenceWrite`'s own `s3:PutObject` on
`logweir/*`. **Builds before this one** projected only the archive grant into a
check pod, so on a destination that separates `evidenceWrite` the row measured
whether the ARCHIVE principal could create under `logweir/*` and said nothing
about the evidence-write grant (defect PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL);
the `write probe` row above was measured by that build, with the policy on the
destination's archive grant (`U6.writeProbe` in `e2e/k8s/d2/d2_live.py`, whose
notes still describe the pre-fix behaviour). The `evidenceWrite` row itself is measured where the
grant is really used: a run writing its own signed receipt.

**And the `catalogSync` reader is not `archiveRead`, though it uses that
grant.** A catalog walk opens the destination's `archiveRead` handle and then
reads the record log, the receipts and the signature sidecars under
`logweir/` as well as the manifests under the archive prefix — so a principal
scoped to `archiveRead`'s own measured minimum publishes no view. The two are
not even the same shape: the walk LISTS only under `logweir/*` (it enumerates
`logweir/catalog/v1/log/` and then opens every other object by the key a
record names) and READS under both roots, where `archiveRead` lists under the
archive prefix and reads only there. Neither row is a superset of the other. It does not
say `AccessDenied` when that happens, either: a walk whose points would not
open lands `Synced=False` with `PartialScan`, whose message is exact — "a
permission or transport failure, which is NOT the same as absent; those
entries say `Unreadable` and never `Missing`" — and a walk whose first
listing was refused relays no body and lands `ResultUnreadable`. **If you
separate the two, give the destination a `archiveRead` grant wide enough for
the catalog, or accept that `RecoveryCatalog` will not sync.**

**The read of a pinned version (FX-7, FX-14): `s3:GetObjectVersion`, and what
was measured.** A point whose receipt pins its manifest's version is checked,
wherever the manifest's current version is not the pin — on every copy of the
archive, and after a set was written again — by reading the pinned version by
id (`GET ?versionId=`). Three readers make that read, each with the
destination's `archiveRead` grant: the `catalogSync` deep check, a point-bound
restore's runner, and (since FX-14) the restore preflight of a point-bound
plan. The two rows above name the action for them.

- **AWS S3** authorises a read that names a version as `s3:GetObjectVersion`,
  a different action from `s3:GetObject`, on `<bucket>/<prefix>/*`
  [UNVERIFIED — needs a real AWS S3 bucket and a credential source]. Without it
  the read is a 403: the point is `Unreadable` in the catalog, its point-bound
  restore exits 1, and its preflight answers `archive.backupSet` `AccessDenied`
  with a remedy that names the action — "could not tell", never a silent pass
  ([the pin](formats/backup-receipt.md#the-pinned-manifest-version-versioned-buckets)).
- **MinIO does not ask for it separately. Measured** (2026-10-09, FX-14,
  compose, the stack's MinIO `RELEASE.2025-09-07T16-13-09Z` rebuild, one
  deny-by-default principal, the manifest written again so the pin is not
  current): with `s3:ListBucket` alone both the current read and the read by
  version are refused and the preflight answers `AccessDenied`; with
  `s3:GetObject` added, and no `s3:GetObjectVersion`, the read by version is
  served and the preflight answers `ManifestSuperseded`; with
  `s3:GetObjectVersion` and no `s3:GetObject`, MinIO serves both reads. So on
  MinIO the grant is already inside `s3:GetObject`, and naming
  `s3:GetObjectVersion` as well costs nothing and is what moves to AWS S3
  unchanged.
- **The read is made only when the current version is not the pin.** Measured
  in the same run: over an unchanged manifest the preflight of a point-bound
  plan is `ready` for the principal without `s3:GetObjectVersion`. A bucket
  that is not versioned issues no pin, so its points never ask for the read; a
  COPY of a pinned point in such a bucket does (the read answers "no such
  version" and the digest decides).

**A plan bound to a recovery point reads its receipt through `archiveRead`
too.** The runner's binding, and since FX-14 the restore preflight, read the
bound receipt at `logweir/backups/<backupId>/<run id>.receipt.json` (the runner
also reads its signature beside it) with the source destination's `archiveRead`
grant, which is why that row adds `s3:GetObject` on `<bucket>/logweir/backups/*`
for such a restore. The `catalogSync` reader row already grants it under
`logweir/*`, and a destination whose catalog you sync carries that row on the
same grant, so a point picked from a synced catalog needs nothing more
[UNVERIFIED — the receipt read was not bisected on a restricted principal; the
reads are `drill::binding` and `check::kinds::restore`]. The docs lint
`the_documented_grants_name_the_version_read_of_every_role_that_makes_one`
holds the two tables (here and `install.md`) to the readers that make the read.

**SUPERSEDED — the retention enforcer now needs `s3:GetObject`.** When this
row was measured the worker listed a set's objects and deleted them by the key
list its approved plan carries, never reading one, and removing
`s3:GetObject` on `<bucket>/<prefix>/*` changed nothing about the run. Since
OBJECT-LOCK-DELETE-MARKER it HEADs every key before deleting it (the note
below), and that HEAD is a `GetObject`.

**One note about that row, and which principal it was measured on.** *A
`RetentionPolicy` in `Enforce` is the one thing Logweir does that cannot be
undone* (§7f) — says the tombstones and the record are written with the destination's
own `evidenceWrite` grant, and that this is what makes a deletion attributable
by a principal that cannot delete. When the `retention enforcer` row above was
measured the reconciler did not do that: it resolved the destination under
`ArchiveRead` — deliberately, for the location — and then projected that same
grant as the run's `LOGWEIR_EVIDENCE_AWS_*`, so on a destination separating the
two an `Enforce` run's first intent tombstone was refused `403`, the point was
`Kept` with `code=TombstoneRefused`, and nothing was deleted (defect
**RET-EVIDENCE-GRANT-IS-ARCHIVEREAD**). The reconciler matches the contract
since that defect was fixed: the record credential is the destination's
`evidenceWrite` role — `spec.access.evidenceWrite`, or `spec.access.archiveWrite`
when it is absent, never `archiveRead` — whose own measured permission is the
`evidenceWrite` row of the table above (`s3:PutObject` on `<bucket>/logweir/*`).
The `retention enforcer` row itself is
about the **delete** credential and is unchanged by that fix — but its live
baseline was taken with the record written by the wrong principal, so
`U6/retention-enforcer` is re-run at the next lab refresh.

**`s3:GetObject` on `<bucket>/<prefix>/*` is now required for the retention
enforcer** (defect OBJECT-LOCK-DELETE-MARKER): the worker HEADs every key
before it deletes it, to refuse a versioned bucket where a delete by key would
only write a delete marker. Rows `u6-ret-058` ("not required") and `u6-ret-060`
(the minimal set without it) were measured on the build before that change;
without the grant a run now deletes nothing and records every key `Kept` with
`code=VersionProbeRefused`. [UNVERIFIED — re-measured by U6/retention-enforcer at the next lab refresh.]

### 7b. A destination-backed run carries a complete `AWS_*` set, and none of it is the controller's

**IN THIS BUILD**, for `Backup`, `BackupSchedule` and `Restore`. A `Backup`
naming a `destinationRef` resolves it for `archiveWrite` before anything is
created, freezes the resolution into its execution inputs (§10's `destination`
block) and renders the environment below into its runner Job from that FROZEN
block; a `Restore` naming `sourceDestinationRef` and `evidenceDestinationRef`
resolves both and checks the approved plan against them (§7b.1). A schedule
propagates its `destinationRef` to the `Backup`s it creates.

What is NOT in this build is listed in §7b.3: a destination carrying a
`transport.caBundle` for a Backup or Restore is refused, not ignored. A
`SecretKeys`, `WorkloadIdentity` or `ArchiveReadGrant` `evidenceRead` IS read,
by an evidence-fetch check Job (§7b.3). The frozen destination now DOES reach `Backup.status.destination`;
§10 says what it holds and §21.8 what the `Preflight` does with it.

(The three subsections that follow were numbered `7d`, `7f` and `7e` until
PLAT-20.2, which collided with §7d, §7e and §7f further down. They are
§7b.1–§7b.3 now; every other section keeps its number.)

The controller's own environment reaches no destination-backed runner Job. That
is not a convention — it is the defect the design closes. The legacy inline path
forwards `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` and
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST` from the controller process, and the engine
builds its object-store client from *every* `AWS_*` variable it finds — so a
controller started with `AWS_ALLOW_HTTP=true` enables plaintext HTTP inside a
runner whose approved plan says `allow_http: false`.

**So the defect is closed for destination-backed runs and open for legacy
inline-`archive` runs, by design.** Those four variables are how every existing
installation points its runners at MinIO or Ceph; removing them would close the
defect by breaking every upgrade at the moment of the upgrade. The route out is
per object and not per release: create a `BackupDestination` (§3.12's
`destinations:from-legacy` derives one from a succeeded run's own frozen
inputs), then point the schedule at it. Until an operator does that for a given
object, a controller-level `AWS_ALLOW_HTTP=true` is a cluster-wide setting that
overrides that object's approved plan.

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
| `LOGWEIR_EVIDENCE_CREDENTIALS` | Which credential the run's **receipt** store uses. `archive` when `evidenceWrite` resolves to the same grant as `archiveWrite` — the default, because `evidenceWrite` falls back to it — and then nothing second is projected. `static` or `workloadIdentity` when an operator separated the principals, and then the three `LOGWEIR_EVIDENCE_AWS_*` references come with it. **The runner refuses a run that does not name it**: a backup writes its archive AND its signed receipt, and a store it cannot build is a local refusal, not a guess. |

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

### 7b.1 What a destination-backed `Restore` is checked against

A `Restore` names TWO destinations and they are set together or neither is: it
reads its archive under the source destination's `archiveRead` and writes its
scorecard under the evidence destination's `evidenceWrite`. Half a pair is
refused by CEL on admission and refused again by the controller, because an
object admitted by an older CRD revision reaches the controller unchecked and
would read from a saved destination while writing its evidence wherever the
inline block says.

After the existing approval and target checks — so today's reason precedence is
unchanged — the controller adds five:

| # | Check | On failure |
|---|---|---|
| 5 | Both destinations resolve and carry `Valid=True` for their current generation | **Hold**, `phase: Pending` with `Admitted=False`, requeued at 30 s. Not terminal: an operator still creating the destination is in the position of an approver who has not signed yet, and `Restore.spec` is immutable |
| 6 | `spec.planBytes` parses as a restore plan (read only; the bytes are never re-emitted) | `PlanUnparseable`, terminal |
| 7 | `plan.source.storage` is the source destination's archive location, **exact on all six fields** | `PlanDestinationMismatch`, terminal; the message names both locations and no credential |
| 8 | `plan.evidence` is the evidence destination's evidence location | `PlanEvidenceDestinationMismatch`, terminal |
| 9 | Both grants resolve and can be satisfied by ONE pod | `DestinationRoleNotConfigured` / `ExecutionContextConflict`, terminal |

**Checks 7 and 8 are what make a saved destination mean anything here.** The
runner reads its location out of the PLAN — those are the bytes an approver
signed — so a destination whose location differs contributes only its
CREDENTIALS, and the run would then present that credential at a location
nobody approved.

**A destination edit never invalidates an approval.** `spec.storage` and
`spec.transport.security` are immutable on a `BackupDestination`, so the plan
bytes cannot go stale through an edit; only preflights do (§21).

**And the recovery point has to be IN the source destination.** Checks 7 and 8
hold the plan to the destination; what holds the *archive* to it is the
recovery point's own frozen `status.destination.locationDigest` (§10), compared
against the source destination in the `Preflight`'s `recoveryPoint.state` row.
A point written to the other destination is `RecoveryPointLocationMismatch`
with both digests named, and a point archived before that field existed is
`unknown` rather than green — §21.8 has the full table and the upgrade case.
Nothing in this controller's checks 5–9 changed: a preflight reports, it does
not admit.

Both destinations' CA bundles are copied into the run's own immutable plan
`ConfigMap`, beside `restore.yaml`, and `LOGWEIR_ARCHIVE_CA_FILE` /
`LOGWEIR_EVIDENCE_CA_FILE` point there. Mounting the destination's own
`ConfigMap` would mean a root rotated mid-run changes what an approved run
trusts. `restore.yaml` is still `spec.planBytes` verbatim.

### 7b.2 A destination edited after the freeze changes nothing for a running run

Every edit to a `BackupDestination` bumps its `metadata.generation`, and the
frozen block records the generation it resolved. A pass that RE-CREATES a
garbage-collected runner Job — a node lost, a controller restarted, the Job
deleted — therefore never re-resolves the live object: it reads the frozen
block, and the CA bytes beside it, back out of the run's own immutable plan
`ConfigMap` and renders from those. Rotating an `archiveWrite` Secret name or
adding an `evidenceRead` grant while a backup runs changes nothing about that
run; the next admission picks the edit up.

`status.execution` is what distinguishes the two passes: absent means nothing
is frozen yet and the destination is resolved; present means the plan is the
answer. The topic selection has had the same readback since D1 W5, for the
same reason.

### 7b.3 What destination-backed execution does not do in this build

- **A destination declaring `spec.transport.caBundle` is REFUSED for `Backup`
  and `Restore`** with `CaBundleUnsupportedByEngine`. Whether the pinned engine
  honours a custom CA through `SSL_CERT_FILE` has not been measured on a
  recorded engine digest, and the failure if it does not is a TLS handshake
  inside the engine child reported as an opaque operational error with no
  mention of certificates. Checks, verification and the controller's own
  evidence reads DO support the CA — no engine child is involved there. An
  administrator who accepts the risk for their installation sets
  `engine.allowUnverifiedCustomCa` in the installation policy `ConfigMap`; the
  compiled constant flips only after the measurement.
- **`ControllerIdentity` evidence reads and the engine-CA opt-in are
  administrator settings, and OFF until an administrator turns them on.** Both
  live in the `weirkeeper-policy` `ConfigMap` the chart renders (D2 W11) from
  two values:

  ```yaml
  # charts/logweir/values.yaml
  engine:
    allowUnverifiedCustomCa: false  # D2 §14's S2b sets this true, then back
  evidence:
    controllerIdentityLocations: [] # [{endpoint, region, bucket}] — all three
  ```

  The shipped defaults are the closed ones, so out of the box every
  `evidenceRead: ControllerIdentity` is refused `ControllerIdentityNotAllowlisted`
  and every destination declaring a `caBundle` is refused
  `CaBundleUnsupportedByEngine`. That is the intended posture: a namespace
  operator may ASK for the controller's own principal, and only a chart or
  cluster administrator may grant it.

  **When there is no policy document at all**, both refusals say so instead of
  blaming an administrator's allowlist — because "nobody listed this location"
  and "no policy exists, so nobody listed anything anywhere" send an operator to
  two different places, and the second one is an object that is not there. The
  refusal names which of the two states it is in: no
  `LOGWEIR_POLICY_CONFIGMAP` / `LOGWEIR_INSTALLATION_NAMESPACE` on the
  Deployment at all (a hand-wired controller; the chart and `logweir.yaml` both
  set the pair), or the named `ConfigMap` simply absent.
- **A `SecretKeys`, `WorkloadIdentity` or `ArchiveReadGrant` `evidenceRead` is
  read by an evidence-fetch check Job, never by the controller** (D2 §3.8
  option C, the default). The controller holds no verb on `secrets` and still
  gets none. After the terminal patch and the runner's TTL it creates
  `lwc-ev-<first 20 hex of sha256(<uid>:<attempt>)>` in the object's own
  namespace. The Job is owned by the `Backup` (or the `Restore`, for its
  scorecard and the EVIDENCE destination's grant). Its plan is the immutable
  `<job>-plan` `ConfigMap`, and its argv is `check run` for kind
  `evidenceFetch`. The kubelet projects exactly the `evidenceRead` grant into
  the pod:
  - `SecretKeys`: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optionally
    `AWS_SESSION_TOKEN` from that grant's Secret;
  - `ArchiveReadGrant`: the same variables from the `archiveRead` Secret;
  - `WorkloadIdentity`: that grant's ServiceAccount.

  Nothing else is projected: no signing key, no `archiveWrite` or
  `evidenceWrite`, no Kafka credential, and no token automount. The pod relays
  the two objects, at most 1 MiB for the document and 64 KiB for the sidecar.
  The controller reads the relay only from a pod whose controller owner is that
  Job's UID. It then verifies in its own process, in this order:
  1. the relayed receipt's sha256 against the RUNNER-reported
     `receiptSha256`. A disagreement is `Invalid` and projects no window,
     records or capture.
  2. the document's binding to the run: the receipt's `backup_id` against
     `status.backupId`, or the scorecard's `run_id` against its key.
  3. DSSE against the namespace's resolved trust, through the same
     `verify_fetched` the `ControllerIdentity` handle feeds.

  It then writes the same facts that path writes. While the Job runs,
  `status.evidence.verification.result` is `Pending` (additive, never green)
  and `status.evidence.observation` is `{mode, jobRef{name, uid}, attempt}`,
  plus `presence` once a relay is read. A Job that ends without a verified
  relay, a runner refusal, a denial, a missing object or a document over the
  cap is `NotAttempted` naming the cause, never `Valid`. A failed attempt is
  retried with a new Job at +1 m, +5 m and +15 m (`observation.retryAfter`),
  and the run itself is never re-run. Since PoC P12 that includes a relay whose
  store DENIED the grant (or answered with any other error that is not
  `NotFound`) and a relay whose own framing did not hold. Before, those were
  final after one Job. A missing document and one over the cap are still final.
  The controller's own read
  (`ControllerIdentity`, and the inline-archive handle) takes the same
  schedule for a transient failure, recorded with `observation.mode`
  `ControllerIdentity` or `ArchiveHandle` and no `jobRef` (§15.1b). At most
  `checks.maxEvidenceFetchActivePerNamespace` fetches are active per namespace,
  and a run over the limit waits `Pending`. The Job's TTL (10 minutes) is set
  only after the verdict commits. A Job holding the name that this object does
  not control is never observed. A run whose runner reported no receipt digest
  (legacy-unbound) gets no Job. What such a read never does is fall back to the
  controller's global handle, which holds a different principal over a
  different bucket. `evidenceRead: ControllerIdentity` at an allowlisted
  location is read through the bounded store cache and verified by the same
  verifier.
- **The frozen destination DOES reach `Backup.status` now**, as
  `status.destination` — four fields, `{name, uid, generation, locationDigest}`,
  written at the freeze from the same snapshot the plan is rendered from (§10).
  `Preflight`'s `RecoveryPointLocationMismatch` has a digest to compare at last
  (§21.8). What is still NOT published is the storage block: the plan carries
  `archiveStorage` as an internally tagged enum whose variants have
  incompatible required fields, which a structural CRD schema cannot hold
  without re-spelling it, so the bucket, endpoint, region and addressing stay in
  the run's frozen `execution-inputs.json`, which is where the Job is rendered
  from and where a reader that needs them will find them.
- **A `WorkloadIdentity` grant REPLACES the Job's `serviceAccountName`**, with
  any ServiceAccount the namespace operator names — bypassing the
  chart-managed runner ServiceAccount. That is how the grant is meant to work:
  the object store authenticates the pod's identity, and a cloud identity
  webhook projects its own token volume regardless of
  `automountServiceAccountToken: false`. It is inside the namespace's own trust
  boundary (an operator who can create a `BackupDestination` can already create
  a Job under that SA), but the chart's `identity.authorizedRunnerNamespaces`
  machinery governs the DEFAULT runner SA and not this override. **W11/W14 to
  confirm and record in `docs/install.md` §4.**
- **`status.records` is written only from a verified receipt** (§10,
  *`status.records` and `status.capture` come from the verified receipt*), so
  the `RECORDS` printer column is blank until the controller — or the
  evidence-fetch Job relaying the receipt — has reached a `Valid` verdict over
  it. A run whose verdict is `NotAttempted` keeps a blank column, which is the
  honest answer rather than a zero.
- **A destination-backed `BackupSchedule` gets no evaluated retention
  report.** The controller's one global archive handle is for objects without
  a `destinationRef`; a report computed through it would describe another
  bucket's catalogue while printing `aws s3 rm` commands naming keys in this
  one. Such a schedule's `status.retentionReport` carries only a `note` saying
  so, with every list empty, and a legacy schedule whose archive is not the
  handle's gets the same shape with its own note — §9, *A schedule on another
  bucket is now told so*. A `RetentionPolicy` (§7f) is the per-destination
  answer.

### 7b.4 Every object-store read has a size cap (FX-31)

The controller is one process for every namespace. Until FX-31 it read a
receipt, a scorecard, a sidecar and a manifest **whole**. A tenant could put a
multi-gigabyte object at its own receipt or sidecar key, and that object would
then take the controller's memory every time a reconcile read it. The pod's
memory limit (`controller.resources.limits.memory`, 512Mi in the chart) would
OOM-kill the controller for every namespace, and every restart read the
object again.

Every read now names a cap. Nothing in the tree can read an object whole:

- **The size the store reports is checked first.** That is the GET's
  `Content-Length` or `Content-Range`, or a filesystem's metadata. An object
  over the cap is refused there, and no body byte is read.
- **Then a running cap holds over the stream.** A store that reports a small
  size and then streams more is cut off at the cap.
- **An existence test reads no body.** The sidecar's presence is a `HEAD`, and
  the readiness probe's GET has a 0-byte cap.

| Document | Read by | Cap |
|---|---|---|
| Receipt, scorecard (the signed document) | the controller | **1 MiB**, the evidence relay's own cap |
| DSSE sidecar | everyone | **64 KiB**, the relay's sidecar cap |
| Engine manifest | the controller's retention report | **64 MiB**, parsed as a stream |
| Receipt, scorecard, catalog record | runner, CLI, check Jobs | 64 MiB (the catalog walk keeps its own 256 KiB) |
| Engine manifest | runner, CLI, check Jobs | 256 MiB |
| Archived segment | runner, CLI | 1 GiB |
| Consumer-groups snapshot, engine report | runner, CLI | 64 MiB |

The controller's caps are the evidence relay's. A document is therefore
verifiable through the controller's own handle (`ControllerIdentity`, or the
inline-archive handle) exactly when an evidence-fetch Job can relay it.

The 1 MiB cap is also what bounds the controller's parse. The controller reads
a receipt's window and a scorecard's outcome before any digest check, and a
document of tiny values parses into about 37 times its size. That figure was
measured by `crates/weirkeeper/tests/read_caps.rs`: 16 MiB of JSON held 621 MB.

A manifest is never parsed into a tree. The window is folded as the bytes
stream past, so the retention report holds at most the 64 MiB it read.

**Concurrent reads share one budget.** A cap bounds one read, and the
controller runs reconciles concurrently: every schedule reconcile evaluates
its retention report, and a controller start reconciles every schedule at
once. So every controller read reserves its worst case out of one
process-wide **128 MiB** budget (a quarter of the chart's 512Mi limit) before
it reads, and holds the reservation until its bytes and its parse are freed:

- a receipt or scorecard reserves 40 MiB, its 1 MiB cap plus the parse;
- a manifest reserves 64 MiB.

A read that does not fit waits. Measured in `crates/weirkeeper/tests/read_caps.rs`:
- eight retention evaluations of 60 MiB manifests at once add 126 MB of peak
  memory under the budget, and 504 MB without it;
- sixteen 1 MB scorecards add 122 MB under the budget, and 413 MB without it.

A degraded store makes evidence reads for every namespace wait on one another.
The store's own request timeout bounds that wait, and it is the trade the
controller's four-permit evidence-read pool already makes.

**What an operator sees.** An object over its cap is never a crash and never
a pass:

| Where | What it says |
|---|---|
| `Backup` and `Restore` `status.evidence.verification` | `NotAttempted`, with the detail `<key> is larger than the <cap>-byte cap weirkeeper reads (the store reports <n> bytes); nothing was verified`. The verdict is **final**: the object will not shrink, so it is not read again on the retry schedule. A new controller process reads it once more, and that read is refused on the size alone. |
| The relay path (`evidenceFetch`) | The Job reports the object `present` and `truncated` and relays **no** bytes. The controller records `<key> is larger than the <cap>-byte cap an evidence fetch relays; nothing was verified`, as before. |
| A `BackupSchedule`'s retention report (`status.retentionReport.skipped`) | The set is listed under `skipped`, and the reason names the cap. It is neither kept nor listed as removable. A `RetentionPolicy` works from the catalog view and reads no manifest here. |
| `Preflight` restore check (`archive.backupSet`) | Not ready. The message ends `…could not be read: <code>: it is larger than the 268435456-byte read cap for a manifest`. |
| `Preflight` restore check of a plan bound to a recovery point, the bound receipt (`archive.backupSet`) | Not ready, `PointBindingMismatch`: the answer of a receipt that is absent or has other bytes, because a preflight runs before any approval and must not say whether an object exists at a key the plan chose (§21.8). The message gives the three causes together, `…it is absent, its bytes do not hash to the bound digest, or it is larger than the 67108864-byte read cap for a receipt`, and never the object's size. The runner's binding, after approval, fails operationally (exit 1) and names the cap. |
| Drill, `backup run`, `catalog sync` | An operational failure (exit 1) or an `Unreadable` point. The message names the cap. |

**The limit this sets, measured.** A receipt is two-space pretty JSON. With
FX-4's configuration coverage, PROD-05.1's 14 semantic entries and PROD-03.0's
schema-dependency block per topic, a 1.5.0 receipt is about 3.4 KB per topic,
so 1 MiB holds about **250–300 topics**: about 300 with no overrides (a
300-topic receipt measured 1,034,994 bytes through the runner's own
serializer), and fewer with per-topic configuration overrides (about 250 with
five each). A run that
selects more topics writes a receipt that neither path can verify. It reads
`NotAttempted` naming the cap, and it is not a recovery point. This was
already true of every evidence-fetch relay before FX-31. It is new for the
controller's own handle, where such a receipt used to verify. Under OD-7's
third case this moves a verdict to the safer side only. Lifting it means
raising the relay and the controller caps together, with a parse that is
bounded without the cap. It is proposed as a follow-up row and is not done in
FX-31.

**A manifest has a limit too.** An engine manifest is about 540 bytes per
segment, so the retention report's 64 MiB holds about 124,000 segments. At
Logweir's default 10 MiB segment that is about 1.2 TB in one backup set, and
about 15 TB at the engine's 128 MiB default. A set whose manifest is larger is
listed under `skipped` on every report, naming the cap, and is never listed as
removable. Runner-side reads, such as a drill or a restore preflight, take a
manifest of up to 256 MiB.

### 7c. A `TopicDiscovery` is one observation, and `unknown` is its honest default

**In this build, end to end.** The reconciler resolves the connection, renders
the plan, creates the check Job, stores the chunks and writes the status, and
the runner's `logweir check run` prints the frames it reads. PLAT-09.1 is Done
on live docker-desktop runs (D2 W14 and lab-refresh-3: a 5,003-topic inventory
in three chunks, an empty cluster, an ACL-limited principal). Against a runner
image older than `check run` a discovery ends `Failed` with
`RunnerContractUnsupported` or `ResultUnreadable`; nothing is stored and nothing
is claimed.

**`attestedComplete` needs an administrator's attestation, and nothing else can
produce it.** The controller reads the installation policy through
`LOGWEIR_POLICY_CONFIGMAP` or `LOGWEIR_INSTALLATION_NAMESPACE`; the chart and
`logweir.yaml` both set them (§22.2), and the chart renders `weirkeeper-policy`
itself. With no attestation in that document every honest observation is
`unknown` or `limited`. [UNVERIFIED — an attestation matching a live broker has not been run on docker-desktop; the matching rules are unit-tested only.]

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
| `Failed` | Terminal, with a closed code in `reason`. A check Job that RAN and relayed a classified failure projects **that check's own code** — `BrokerUnreachable`, `AuthenticationFailed`, `TlsHandshakeFailed`, `MetadataTimeout`, `ClusterAuthorizationFailed`, … — and its message and remedy in `status.message`. Otherwise the code is the controller's own: `ConnectionNotFound`, `ConnectionInvalid`, `DeadlineExceeded`, `ResultUnreadable`, `CheckPlanConflict`, `ResultStorageConflict`, `Stalled`, or a pod-waiting code such as `CredentialSecretNotFound`. |
| `Cancelled` | `spec.cancelRequested` reached a non-terminal object. The Job's deadline was collapsed and **no chunks were written**. |

`ConnectionNotFound` and `ConnectionInvalid` are terminal rather than retried,
because `spec.request` is immutable: the object can never name a different
connection, so a later pass would ask the same question. Create the
`KafkaCluster`, then create a new `TopicDiscovery`.

**A relayed failure keeps its own code.** `logweir check run` does not exit 1 on
an unreachable broker: an operational failure is a RESULT, so the runner prints a
result document with no `inventory` block and one `connection.authenticated` row
carrying the classified code, its message and its remedy. The controller projects
that row — the first **blocking** check that is not `ready`, taking a `notReady`
one before an `unknown` or `skipped` one, exactly as D2 §6.4 aggregates — into
`status.reason` and `status.message`. So an unreachable bootstrap reads
`BrokerUnreachable` and a password the broker no longer accepts reads
`AuthenticationFailed`, and an operator can tell the two apart without reading a
pod log. An **advisory** row is a warning by definition and is never the terminal
reason.

`ResultUnreadable` is reserved for what the word says: the relay did not decode,
the result document did not verify, its counts or its `topicsSha256` are not the
ones the frames carry, no result document was relayed at all, or the document
carries neither an inventory nor a blocking check that is not `ready`. Before
2026-09-18 every classified failure read `ResultUnreadable` as well (defect
`D2-RESULTUNREADABLE`, found by the D2 live run at S11/S12), which made an
unreachable broker and a rejected credential indistinguishable in the console.

**The check Job's own condition stays `Complete` on that path**, and that is not
a contradiction: the runner performed the check, relayed the answer and exited 0,
so Kubernetes marks the Job complete while the `TopicDiscovery` is `Failed`. The
verdict lives on the `TopicDiscovery`; the Job condition says only whether the
pod ran to completion.

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
the controller holds no `delete` on **Jobs** — after verifying that the Job's
controller owner is this object, so a Job that merely shares the name is never
touched. A finished Job gets `ttlSecondsAfterFinished: 600`, patched **only
after** the status write returned 200 — on the failed path exactly as on the
successful one: the relay lives on the pod, and the TTL controller removes a Job
and its pods together, so a TTL set after a status write that answered 409 would
let garbage collection take the reason an operator still has to read. The plan and chunk `ConfigMap`s
carry an owner reference with `blockOwnerDeletion`, so deleting the
`TopicDiscovery` removes everything it owns by cascade.

**Retention IS automatic, since D2 W11.** D2 §5.8's 24-hour window and
keep-last-five per connection are enforced by the reconciler's own hourly pass
over a terminal object: it lists the namespace, collects what is past
`observedAt + retentionSeconds` or outside the newest `keepPerConnection` for
its connection UID, and deletes with a UID precondition, at most twenty per
pass. Both numbers come from the installation policy (§22.2), and §22.3 is the
full rule, its four bounds and what a truncated listing does.

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

**RBAC.** This kind adds three rules to the `weirkeeper` ClusterRole:
`list`/`watch` on `topicdiscoveries` (the controller's watch), `patch` on
`topicdiscoveries/status`, and `delete` on `topicdiscoveries` — shared with
`preflights` in one rule, and granted on nothing else in the cluster (§22.1).
No `get`: the reconciler never re-reads a discovery, and the collector lists
rather than naming one. No verb on `secrets`.

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

**Every sync is harvested, including the second one.** `status.lastSyncJob` is
the record of ONE Job and is **replaced** when a sync starts, not merged into:
the `/status` write is an RFC 7386 merge patch, so a record that named only the
new Job would keep the previous one's `finishedAt` — and `finishedAt` is the one
fact that says a Job's result has been read. Until 2026-09-18 it did keep it,
and the consequence was that every sync after the first ran to `Complete` and
was never looked at: `spec.syncRequest` was inert, an `intervalSeconds: 300`
catalog stopped refreshing past its first interval, and the only way to refresh
a view was to delete and re-create the `RecoveryCatalog`.

**`intervalSeconds` is a cadence, not an alarm clock.** A sync that finished
inside the current interval slot has served it, whatever started it, so a
`spec.syncRequest` that completes at 11:55 is not followed by the 12:00 slot's
own walk one reconcile later. That matters for what `Synced` means: **after a
view is published, `Synced` stays `True/Succeeded` until a new `syncRequest` or
the next slot starts a sync**, and it is usable as a completion signal.
`Synced=Unknown/SyncInProgress`, or a waiting code such as
`Unknown/PodNotStarted`, means a sync is running right now — while
`Ready=True/ViewReady` says the previous view is still usable, because `Ready`
is about the view and `Synced` is about the walk. `status.syncedAt` remains the
timestamp to compare when you want to know which walk a view came from.

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

**How many generations coexist, exactly.** At most one Job lives per slot (a
slot in which a sync finished has no Job of its own), so the number alive at
once is at most `ttl / intervalSeconds` — **three** at an hourly cadence, and
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
controller holds no delete capability against object storage at all (§15.1) and
no `delete` verb on a `ConfigMap`, a Job or a `RecoveryCatalog` — its only
`delete` is on the two transient check kinds (§22.1) — and `logweir catalog
list` still reads the durable catalog.

**Two axes, and nothing merges them.** Each entry carries an `availability` and a
`verification`, and a `selectable` flag that is their conjunction.

| `availability` | meaning |
|---|---|
| `Available` | receipt, sidecar and manifest readable; the manifest digest equals the receipt's, and — for a point whose receipt pins a manifest version (FX-7, versioned buckets) — the manifest's current version is the pinned one, or this bucket does not hold the pinned version at all (a copy of the archive, an unversioned bucket, a version that was expired or deleted): then the digest decided — which an identical manifest over rewritten segments passes — and the entry's `remedy` says the pin could not be checked in this bucket |
| `Missing` | a definite `NotFound` |
| `Unreadable` | any other storage error — 403, timeout, truncated, or a failed read of a pinned manifest version (FX-7; a 403 there is a principal without `s3:GetObjectVersion`, and the entry's remedy names it). **"Could not tell", never "is not there".** |
| `Deleted` | a completed retention tombstone exists |
| `Conflict` | two records disagree for one identity, a record's facts contradict the receipt, or this bucket holds the manifest version the receipt pins and it is no longer the current one — the set was written again in this bucket after the point was signed (FX-7; the entry's remedy says so) |
| `UnsupportedFormat` | the record's major version is above this build's |
| `Partial` | a sampled segment the manifest lists is missing |

**What the pin cannot see (FX-7).** The pin is checked only where the bucket
still holds the pinned version and serves it by id. A version that was expired
or DELETED, a copy synced after the set was written again, or a store that
cannot read by version leaves the digest alone, which an identical manifest
over rewritten segments passes
([the three routes](formats/backup-receipt.md#the-pinned-manifest-version-versioned-buckets)).
Object Lock retention covering a point's lifetime keeps its pinned version, and
when the signing bucket's catalog says `Conflict` while a copy's says
`Available`, believe the `Conflict`: it is evidence about the set, not about
the place. Where ONE catalog lists a point at two locations, the locations merge
best-of, so there the copy's `Available` is the entry's and the signing
bucket's verdict survives only as "<location> is Conflict" in its `remedy`.

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

**The trust source is the one bound to the catalog's namespace.** The controller
resolves it exactly as it resolves the trust an `Approval` in that namespace is
verified against (and the restore preflight re-judges a catalog point's signer
against): the `TrustPolicy` whose `spec.namespaces` names the namespace, else the
`default: true` policy, else `legacy-roster-v1` synthesised from
`TrustRoster/default`. Only the policy's `EvidenceSigning` keys are mounted and
consulted, and only those whose PEM parses and hashes to the declared `keyId`;
each point is judged BY `logweir_core::trust::decide` itself — the judge the
restore preflight and the runner apply to the same key — at the point's claimed
signing time, so the view is never the more permissive surface. `Valid/Current`
is `Verified` (an `Active` key, signed inside its window); `Valid/Historical` is
`VerifiedHistorical` (signed before `notAfter` has passed, at or before
`retiredAt`, or before a `Superseded`/`Unspecified` revocation's
`revocationEffectiveFrom`); a `KeyCompromise` revocation is `Revoked` whatever the
claim; and every other window refusal is `Invalid` — a claim before `notBefore`,
after the accepted bound, **in the future**, against a key whose window **has not
opened yet** (a staged successor), or with no instant at all. (The roster has no
lifecycle; a roster key is classified as it always was.)
A namespace two policies claim resolves to nothing: every point is `NotAttempted`
and `TrustAvailable=False/TrustPolicyConflict` names the policies. A roster-only
namespace mounts the same bundle and reaches the same verdicts as builds before
this. A `TrustPolicy` event wakes every catalog in the namespaces it could govern
(the same trigger the `Approval` controller carries, backed off like every
controller watch), so `TrustAvailable` and the NEXT sync's bundle and verdicts
follow a key change at once. **A published view is not re-classified in place:**
its rows keep the verdicts of the sync that wrote them until the next sync (or
`spec.syncRequest`), which is why a restore from a catalog point re-judges the
row's signer against the current trust in preflight and again in the runner.
Builds before this read only `TrustRoster/default`, so a point signed under a
`TrustPolicy` key was `UntrustedSigner` (or `NotAttempted` with no roster) and
never offered, and a policy's retirement or revocation never reached the view
(defect CATALOG-TRUST-ROSTER-ONLY). **Upgrade:** nothing to migrate; the next sync
of a policy-governed catalog mounts the policy's keys (a new trust `ConfigMap`,
named by the key set) and its points are re-classified then — request one with
`spec.syncRequest` to see it at once. **Rollback:** an older controller reads the
roster again and classifies as before.

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

**What the runner walks, and what each number covers.** `catalogSync` is the
sixth plan kind of the one check runner (`docs/stability.md`). An `Index` sync
walks day shards of `logweir/catalog/v1/log/` **newest first, always starting at
today**: the view is rebuilt from this body on every sync, so a walk that
resumed at an old cursor would publish a window of old points and call it the
catalog.

**Its floor is the archive's own oldest day, and one listing establishes it.**
The log key path sorts by day and then by millisecond, so the smallest key under
`logweir/catalog/v1/log/` IS the oldest point; the walk runs from today to that
day and reports `complete: true` when it gets there, whatever the archive's age.
`status.cursor.indexShard` is therefore **reported and not consumed** — it says
how far the last walk reached. A floor derived from the previous reach receded
one day per sync for ever, and once it passed the shard bound `complete` was
never true again: every sync then published `ScanIncomplete` on a healthy,
fully-walked archive. A floor read from the archive cannot recede. One sync
lists at most 3 660 day shards; a walk that hits that bound says
`catalogStoppedFor: shardBudget` rather than blaming the object budget.

A `Full` rescan pages `logweir/catalog/v1/points/` and *does* resume strictly
after `status.cursor.rescanStartAfter`. It reports no cursor once it finishes —
and it always reports one when it has not, whatever stopped it.

**The buckets sum to `catalog-counts.total`, on every walk, by construction.** A
point is begun only when all four of its objects — the record, the receipt, its
sidecar and the manifest — can be afforded, and it is COUNTED only when it is
examined. `spec.sync.viewLimit` bounds the entry LINES the body carries and
nothing else: a walk with `viewLimit: 2000` over a 20 000-point archive
manifest-checks all 20 000 and relays the newest 2 000. Nothing is ever counted
that was not examined, so "no conflicts in 50 000 points" cannot be a report
about 2 000 of them.

What a walk does NOT reach is said by the fence instead. `catalog-cursor`'s
`complete` means **the walk listed its whole range and examined every point it
counted**; `false` comes with the cursor to resume from, the `Synced` condition
reads `ScanIncomplete`, and the check result names which bound stopped it
(`catalogStoppedFor: objectBudget` or `shardBudget`). A point the budget never
reached is unexamined — **not** `Unreadable`, which is a fact about permissions
or transport and which the controller renders as `PartialScan`.

**A point whose record could not be read is counted and not listed.** An entry
line's required fields are the receipt-derived facts, and there is no honest
value for any of them when the record is `Missing` or `Unreadable`; the counts
carry the fact and a row of zeroes would carry a fiction.

**`Deleted` and `Partial` are in the table and this build's sync produces
neither.** `Partial` needs segment sampling, which does not exist. `Deleted`
needs the sync to read the retention tombstones §7f's worker writes under
`logweir/retention/`, and the sync does not read them yet: a point a
`RetentionPolicy` removed reads `Missing` (its manifest is deleted first, by
design), and the retention record is where its removal is attested. Both values
are listed because they are the vocabulary a reader of a page must be able to
interpret. Do not confuse this availability word with the retention worker's
own per-point outcome `Deleted` in `status.lastEnforcement` — that one is a
fact about one run, and on a versioned bucket it has a narrower meaning (§7f).

**`deepCheck: SegmentSample` is admitted and not implemented.** A plan naming it
is honoured as `ManifestDigest` and the check result says so
(`catalogSegmentSample: notImplemented`), rather than reporting a sample nobody
took.

**`deepCheck: None` weakens what `Available` means, and does so on purpose.**
The table above defines `Available` as receipt, sidecar and manifest readable
with the manifest digest equal to the receipt's; `None` is an operator's
explicit "existence only", and under it a point is `Available` once the record
and the receipt read. The check result publishes `catalogDeepCheck` so which
reading produced a row is never a guess. `ManifestDigest` is the default because
it is the check that distinguishes "the object is there" from "the object is the
one this receipt describes".

**`UntrustedSigner` cannot come from the Job, and does not need to.** That state
is "the signature verifies under a key this installation does not list", and a
Job holding only this installation's keys cannot verify under a key it does not
hold. A sidecar naming a key the pod does not have is reported `notAttempted`
with the **claimed** key id, which is what puts the stranger's key into
`catalog-signers` — and therefore into `status.counts.untrustedSigner` and
`status.signers[].trusted: false` — without any entry claiming a verdict nobody
could reach. The controller reaches `UntrustedSigner` when its trust source is
narrower than the bundle it mounted.

**A walk that never started relays no body at all.** A body that parses is a
body this controller publishes in place of the view it has, so a destination
whose handle would not build, or whose first listing was denied, relays the
failure in the check result's `destination.archiveListable` row and nothing in
`details`. The controller reads the absence as `ResultUnreadable`, keeps the
previous view, and the pod log carries the code.

**Conditions.** `Ready` (a usable view exists now), `Synced` (what the last sync
did — `Succeeded`, `PartialScan` when part of the archive could not be read,
`ScanIncomplete` when the object budget ran out and the cursor was recorded, or a
closed check code on a failure), `Stale` (the view is older than two intervals; a
`intervalSeconds: 0` catalog is manual-only and never stale by the clock) and
`TrustAvailable`.

**One catalog per destination per namespace.** Two `RecoveryCatalog`s whose
`spec.destinationRef` names the same `BackupDestination` in one namespace are
not two indexes: the one created first catalogs the destination, and every later
one reports `Ready=False/DuplicateCatalog`, naming the catalog that holds the
destination. "First" is `metadata.creationTimestamp`, then the name for a tie in
the same second, so every reconcile of either object reaches the same answer. A
duplicate resolves no destination and runs no sync Job (`Synced=False` with the
same reason), and it lists no view:
`status.pages`, `status.indexConfigMap`, `status.truncated` and
`status.viewExpiresAt` are withdrawn. The API's point listing and a
catalog-point restore read `status.pages` directly, and a `ProtectionPolicy`
reads `viewExpiresAt` first. A policy over a duplicate therefore reads
`CatalogStale`, never a fresh view with no available point. Its page
`ConfigMap`s age out with their sync Job's TTL. Use the elder catalog, or
delete the one you do not want. If you delete the elder, the next one in
creation order takes over within a minute and syncs at once, even inside an
interval slot its own earlier sync served. **Upgrade:** a namespace
that already holds duplicates (an older controller accepted them) keeps its
first-created catalog syncing, and every other catalog over the same destination
turns `Ready=False/DuplicateCatalog` on its first reconcile. A sync Job such a
catalog was running is not harvested. Two consequences to check before
upgrading over duplicates:

- The survivor is the OLDEST catalog, not the best. If a newer duplicate has
  better settings (a larger `viewLimit`, `mode: Full`), delete the older one
  with kubectl; the console holds no delete.
- A `RetentionPolicy.catalogRef`, a `RehearsalSchedule`'s point `catalogRef` or
  a `ProtectionPolicy.protects.catalogRef` that names a newer duplicate loses
  its input. Retention refuses ("published no view"), the rehearsal falls back
  to `Backup` candidates, and protection reads `CatalogStale`. All three fail
  closed. Point them at the survivor; the first two are immutable, so re-create
  them.

Rolling back to an older controller makes the duplicates sync again.

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

**Points from before the catalog are not in it until they are backfilled.**
Both sync modes read the durable catalog's signed **records** under
`logweir/catalog/v1/` — `Full` rescans every `points/<pointId>/record.json`,
`Index` reads the day shards — and the sync Job is read-only by design, so it
never writes the record a receipt is missing. A backup written by a release
before the catalog existed (`v0.1.5` and earlier) has its signed receipt and no
record, so a connected catalog neither counts nor offers it (no
`unreadable`/`unsupported` count either: there is nothing it read). Its
`Backup` object, where it still exists, is restorable as before
(`#/restore?ns=<ns>&backup=<name>`, §21.8). To make such points part of the
catalog — a disaster restore from an archive with no `Backup` objects needs
exactly that — run the operator backfill once, from a workstation with a
credential that may write under `logweir/catalog/v1/`:

```bash
logweir catalog sync --url s3://<bucket> [--endpoint <url> --region <r> --path-style --allow-http] \
  --signing-key <record-signing-key.pem> --public-key <receipt-key.pub.pem> [--public-key …]
```

It writes a signed record, create-only, for every receipt that verifies under
one of the `--public-key` values and nothing for any other; it is idempotent
(point ids are derived from the receipt bytes), resumable with
`--since <catalog-next>`, bounded by `--max`, and deletes nothing
([formats/catalog-point.md](formats/catalog-point.md)). Then set
`spec.syncRequest` on the `RecoveryCatalog` to a new value.

**This is a gap against the design, not a limit of it.** D3 §5.3 designs `Full`
as a read-only rescan of receipts and manifests ("the scanner backfills"): the
receipt is the verification root, a point's id is derived from the receipt's
bytes, and no write grant or signing key is needed. This build's `Full` walks
records only. Closing it is a change to the runner's `catalogSync` kind — a
second prefix under the resumable cursor, exactly-once counting of a receipt
that also has a record, the per-point object budget — and it is tracked for
PLAT-15.1 rather than made on the legacy-restore path. The operator backfill
above is the supported route until then.

### 7d.1 Disaster restore: an archive, and no `Backup` object at all (PLAT-15.2)

A recovery point is a signed receipt in object storage. A cluster that lost every
`Backup` object — rebuilt from nothing, restored from an etcd snapshot older than
the archive, or a fresh installation pointed at an archive another installation
wrote — still has every point, and this is the supported way to restore one.
**No `Backup`, `BackupSchedule` or source `KafkaCluster` is read at any step, and
no configuration is reconstructed by hand.**

1. **A read-only credential, bound to its destination.** On the console's
   *Destinations* page enter the archive's read-only key pair once for
   `archiveRead` (*new*): the console creates the Secret, owned by and bound to
   the destination, and never shows the key again. With `kubectl`, create the
   `BackupDestination` naming a Secret by NAME
   (`access.archiveRead: {mode: SecretKeys, secret: {name}}`), then create that
   Secret with the key pair and the destination's `status.credentialBinding`
   under `logweir-binding` (§20.10): a Secret without it is refused by every
   runner.
2. **Connect the archive.** Create a `RecoveryCatalog` with
   `spec.destinationRef` and `sync.mode: Full` (the console's *Catalog* page,
   *Connect an existing archive*, or `POST .../catalogs`). The sync Job walks
   the archive's `logweir/catalog/v1/` records and the controller publishes the
   view (§7d). Repeating the connect with the same `Idempotency-Key` returns the
   same object; a second sync of the same archive yields the same point ids.
3. **Establish trust explicitly.** A point signed by a key this namespace's
   trust does not list is `UntrustedSigner` and is never offered. The catalog's
   signer panel shows the key id; compare it out of band (`docs/keys.md`) and
   add the key to the `TrustPolicy` with `kubectl apply`. There is no one-click
   trust, and a key found beside the archive is a claim, not a trust decision.
   The catalog is judged by the `TrustPolicy` that governs its namespace (the
   roster only when none does; see the `RecoveryCatalog` view section above),
   and the policy change wakes it at once — but the published rows keep their
   verdicts until the next sync, so set `spec.syncRequest` to a new value to
   see the key's points offered now.
4. **Choose a catalog-verified point.** The wizard (`#/restore?ns=<ns>` lists
   *Recovery points from connected archives*; the catalog table links each
   restorable row) opens on `#/restore?ns=<ns>&catalog=<name>&point=<pointId>`
   and **re-reads the point from the product API**. It is offered only when the
   API publishes the row `selectable` — `Available` and `Verified` or
   `VerifiedHistorical`, joined server side with the namespace's `Backup`
   verdicts (`backupVerdict`) — the verdict join is complete
   (`backupVerdictsIncomplete` absent), and the row carries an unredacted
   backup set id and receipt key and both digests. (A catalog synced by a
   runner up to v0.2.0-rc.1 published every SCHEDULED run's set id and receipt
   key as `[redacted]`, so none of its scheduled points was offered; upgrade the
   runner image and sync the catalog again — FX-17.) The operator names the
   topics to restore, and the readiness check reads the manifest for exactly
   those names. Since PROD-05.1 the view lists an `Available` point's topics
   with their recorded partition count and replication factor
   (`PointView.topics[]`, for points whose receipt is format 1.3.0 or later); the wizard
   defaults the plan's replication factor from them, capped at the target's
   broker count, and says so. The operator still types the list: a listed topic
   set is not yet offered as a choice. Since PROD-03.0 each listed topic also
   carries its schema dependency (`PointView.topics[].schemaDependency`, receipt
   format 1.5.0): the recovery-point step and the review name every
   schema-dependent topic with the schema ids its keys or values reference,
   under **"Registry not captured: applications may not read these records
   after restore."** — Logweir never captures a schema registry, so an
   application reading the restored records needs the registry that issued
   those ids, reachable from where it runs. Nothing is blocked; a topic without
   the field, or one the backup could not judge, is said to be not assessed,
   never "no registry needed" ([the contract](formats/backup-receipt.md#schema_dependency--does-a-restore-need-a-schema-registry-format-150)).
5. **The plan is bound to the point.** It carries `source.backup: <backupId>`
   and `source.point {point_id, receipt_key, receipt_sha256, manifest_sha256}`;
   the restore point in time defaults to `coveredTo − 1 ms` (the catalog's end
   is exclusive), and a point in time is accepted in `[coveredFrom + 1 ms,
   coveredTo − 1 ms]`, the range the runner's `archive.coverage` accepts — the
   same rule the wizard applies to a Backup's `windowCovered`
   (WIZARD-DEFAULT-PIT-EXCLUSIVE). The approver signs those bytes, so the approval covers WHICH
   archive object is recovered.
6. **Readiness re-reads the row, and the archive.** Step 5 starts a `Preflight`
   with `spec.request.restore.catalogPointRef {catalogRef, pointId}` (at most
   one of it and `recoveryPointRef`, CEL rule P10). §21.8 lists what
   `recoveryPoint.state` answers from the catalog. The check Job then judges
   the archive itself as the runner will in step 7 (FX-14): `archive.backupSet`
   reads the bound receipt and the manifest with the receipt's pin, so a set
   that was written again after the point was signed is `ManifestSuperseded`
   here and not a `ready` preview of a run the runner refuses (§21.8).
7. **Approve and run.** The `Restore` is the ordinary one — `backupSetRef` is
   the point's set, `sourceDestinationRef`/`evidenceDestinationRef` the
   catalog's destination — and waits for its `Approval`. The runner, before it
   constructs any client (execution contract v2), re-reads the receipt the plan
   names, re-derives the point id from its bytes, compares both digests,
   verifies the receipt's signature against the evidence keyring the
   controller mounts in the approval bundle (`evidence-keys.json`, every key
   of the namespace's resolved trust with its lifecycle, digest-pinned) and
   reads the manifest back — and, when the receipt pins the manifest's version
   (FX-7, a versioned bucket), compares it with the current one, since the
   engine restores only the current one. When they differ it reads the pinned
   version by id: a version this bucket still holds is a rewrite here; one it
   does not hold — every copy of the archive, an unversioned bucket, a version
   that was expired or deleted — leaves the decision to the digest (which an
   identical manifest over rewritten segments passes), and the runner logs
   `PointPinUnchecked` and goes on. A digest mismatch, or a pinned version
   the bucket holds that is no longer current, is exit 3 `PointBindingMismatch`;
   a pinned version that cannot be read at all (a 403 without
   `s3:GetObjectVersion`, an outage) is exit 1;
   an unsigned receipt, a signature no trusted key verifies, or a signer the
   trust refuses (revoked for compromise, retired before the receipt was
   written, not an `EvidenceSigning` key) is exit 3 `PointUntrusted`. Either
   way no data moves and no target topic is created. The source cluster is
   never contacted. **The run then restores the point's own set or nothing
   (FX-16):** a plan whose `source.backup` is not the receipt's set
   (`latestCompleted` included), or under whose storage the engine would
   read another manifest than the one the receipt attests, is refused before
   any broker is contacted; the set is then selected by the point's manifest
   key, and once it is described the runner refuses one whose set id,
   manifest digest or manifest version is not the one the binding verified — both exit 3 `PointBindingSetMismatch`, before any target
   topic of the restore exists. The `Restore` this step creates names the
   point's set (`backupSetRef`), so it is never refused for that; the
   readiness check above refuses the same plan as
   `CatalogPointBindingMismatch`.

**A run the controller could not verify is restored the same way
(CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW).** A destination-backed `Backup` whose
own verdict is `NotAttempted` (no evidence grant, or a `ControllerIdentity`
location the administrator did not allowlist) has no `status.windowCovered`, so
it is not a recovery point on its own. The schedule detail and the wizard offer
it **from its catalog row** only when its own verdict is absent or
`NotAttempted` — never `Invalid`, `Untrusted` or a word this build does not
know, and not while it is still `Pending` (the console waits for the evidence
fetch's own verdict) — exactly one row answers its receipt digest (its set id when it
reported no digest), that row is offerable as above, and the catalog reads the
destination the run froze (same name, UID and location digest). The run's own
verdict is READ from its operation (a console `Backup` list does not publish
it), by the schedule detail and again by the wizard; a read that fails leaves
the run un-offered. The link opens the wizard on the catalog point, so the plan
is bound to that receipt.

**A restore started from a Backup is bound the same way (FX-35).** The wizard
opened on a `Backup` that has its own window (from the Backups, History or
Schedules page) looks up that run's point by its receipt digest in a catalog over
the run's own destination and builds the plan above for it. A run whose point no
catalog lists yet keeps a plan with `source.backup` only; the wizard says it is
not bound, and its scorecard reports each topic's timestamp type NOT RECORDED.

**Upgrade and rollback.** Everything here is additive. A plan without
`source.point` is byte-identical to before and restores as before;
`catalogPointRef` is one optional field and one CEL rule on the `Preflight`
kind, whose objects are immutable, so no existing object changes; the seven new
check codes are new members of a closed vocabulary. The runner's receipt
signature check is a runner-image change with its own migration note in
[`stability.md`](stability.md#a-bound-points-receipt-signature-is-verified-before-any-data-moves-d3-55-step-6):
a point-bound plan now needs the evidence keyring a controller at this version
renders. **Upgrade the controller and runner images together**: a runner at
this version handed a point-bound plan by an older controller refuses it
`PointUntrusted`, and an older runner does not know `--evidence-keys` and exits
1 before any work. A standalone `logweir restore run` of a point-bound plan now
needs `--evidence-keys`, and a point-bound `Restore` caught mid-upgrade (its
approval bundle created by the older controller, its Job not yet) ends
`ApprovalBundleConflict`: delete it and create it again. Rolling the controller back
leaves a `catalogPointRef` Preflight answering no `recoveryPoint.state` row (an
older controller ignores the field — the CRD prunes it once the older schema is
re-applied); rolling the console back removes the catalog-point route and its
links. Archives, catalog records and the runner's binding check (which predates
this change) are untouched in both directions, except that a runner at this
version refuses a point-bound plan from an older controller (`PointUntrusted`,
no keyring).

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
API response, so reading a stranger's is defect `SEC-PODLOG`. Only the ten
`notify-result=` values this build knows are read; nothing else from a pod log
can reach a status.

**Each sink's Secret is bound to this policy (FX-20).** Every route's
credential Secret must carry, under `logweir-binding`, the value
`status.credentialBindings` publishes for it — the policy's UID, the sink kind
and, for PagerDuty, the endpoint. The delivery Job projects that key beside the
credential, and `logweir notify deliver` refuses a sink whose Secret carries no
binding or another policy's, sink's or endpoint's **before composing a body or
dialling**: it prints `notify-result=<sink>:refused`, still delivers the other
sinks, and the controller records the attempt `Failed` with
`NotificationsDelivered=False/CredentialBindingMismatch`. Editing a PagerDuty
route's `endpoint` changes its binding, so the routing key is never sent to the
new endpoint until the Secret is bound again (§20.10).

**A sink must be `https://`, and the one way round it is the INSTALLATION's.**
`logweir notify deliver` refuses a non-`https://` webhook or Slack URL **before
it dials**, so an in-cluster echo sink on `http://` receives nothing and the
delivery is recorded `<sink>:failed`. The documented escape hatch,
`NOTIFY_ALLOW_INSECURE_SINKS`, was set by nothing — not by the spec and not by
the delivery Job — which made a laptop-cluster rehearsal of the notification
path impossible (`NOTIFY-INSECURE-SINK-UNEXPOSED`). It is now
`notify.allowInsecureSinks` in `charts/logweir/values.yaml`, default `false`:
when true the chart renders `LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS=1` on the
`weirkeeper` Deployment, the controller reads it **once at startup** (an
explicit `1`/`true`/`yes` only, so an empty `value:` is off) and forwards
`NOTIFY_ALLOW_INSECURE_SINKS=1` into every delivery Job's literal env. When it
is unset nothing at all is rendered — not the variable with a falsy value — so
the Job, the install file and the chart's rendered output are byte-identical to
what they were before the value existed. **A `ProtectionPolicy` cannot enable
it.** The spec is a namespaced object any namespace operator may write, and a
field there would let whoever creates a policy downgrade their own alerts'
transport to cleartext — carrying the event, the policy's name, its health and,
on Slack, a bearer credential in the URL. The switch is on the controller
Deployment, which is the cluster administrator's, and the answer is the same for
every policy in the cluster. The Job's command line still names no URL either
way: the sink URL reaches the pod as a `secretKeyRef` and the hatch is a literal
`1`. **Production leaves it `false`**; it is a local-development setting, and an
installation that turns it on logs one `WARN` at startup saying so.

**A protection event's `verificationScope` is `sampled`, `degraded` or `none` —
never `complete`.** It describes the policy's newest available POINT (`sampled`
when that point carries verified evidence), not a restore of it. A `Restore` and
a rehearsal can ask for complete coverage (PROD-08.1a, §12 *Complete coverage*),
and a complete run reports what it covered on the `Restore` itself
(`status.integrity.coverage` and `.complete`), in the product API's
`verificationScope.coverage`, in the runner's own notification body and in its
metrics — never in this field. The value reaches a PagerDuty incident title and
a Slack channel where someone decides, during an incident, whether an archive
can be trusted, so the type has three variants and `logweir notify deliver`
refuses a fourth at parse time. See
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

**A point whose receipt the controller could not read (`PointFactsUnread`).** On
a destination whose `evidenceRead` grant is `SecretKeys` or `WorkloadIdentity`
the controller holds no Secret verb by design, so it verifies nothing itself and
records `evidence.verification.result: NotAttempted` with a sentence naming the
grant. The runner now prints the SHA-256 of the exact persisted receipt bytes,
so `status.evidence.receiptSha256` is a captured public fact even when the
controller cannot read the destination. `Backup.status.capture` remains
receipt-derived and is still absent on `NotAttempted`; the digest therefore
names a joinable point but does not age it, verify it or make it selectable by
itself. That runner-reported digest is the required immutable anchor; if a
fetched receipt hashes differently, verification is `Invalid` rather than
validating the fetched bytes against their own hash, and no `windowCovered`,
records or capture is projected from those bytes. Older runners that omit
the digest remain observable, but publish no receipt digest and record
`NotAttempted` with `legacy-unbound`: fetched payload and sidecar bytes cannot
validate themselves or project records/capture, even when their signature is
otherwise valid. Malformed or duplicate digest lines are likewise ignored
rather than promoted to evidence. Until 2026-09-21 the policy read such a run
as *no point at all*:
`health: Unprotected`, which is D3 §3.2's "nothing to recover from", and which
**pages**, about archives whose own catalog entry for the same point read
`Available`/`Verified`. Three changes close it and an operator sees all three:

- The catalog join answers on the **archive set id** (`Backup.status.execution.id`
  / `status.backupId`, and `backupId` on the catalog row) when the point has no
  receipt-derived identity. `pointId` still decides where the controller has one.
- The **capture time and the identity are read off that row** —
  `recoveryPointAtMs` IS the receipt's `started_at` carried through the view — so
  `status.lastAvailablePoint` carries a real `recoveryPointAt` and `pointId`. The
  verification verdict is NOT rewritten: it still reads `NotAttempted`, because
  this controller still did not read that receipt. Facts are filled only from the
  ONE row the view holds for the point; an ambiguous archive-set join fills
  nothing.
- `objectives.requireVerifiedEvidence` is satisfied by the **entry's own
  verification axis** where the controller reached no verdict. Only
  `NotAttempted` defers this way — and a `Valid` on `trust.basis: Unverified`,
  which is the same "nothing compared yet". A `Valid` counts as verified only
  beside no `trust` block or a `Current`/`Historical` basis, the badge's own
  rule; on any other basis (`RecordedBeforeRevocation`, `None`, a block with no
  basis) it is a refusal like `Untrusted`. A verdict a verifier REACHED still decides:
  `Untrusted` is a signature this installation refuses and `Invalid` is a
  document that is not what it claims to be, and neither is overruled by a
  catalog row — otherwise `TrustPolicy` would be decorative and a tampered
  archive would read `Healthy` behind a view harvested before the tampering.
  Such a point is `Unprotected` and pages, with or without a capture time, **and at
  every setting of `requireVerifiedEvidence`**: that objective governs whether an
  UNVERIFIED point may count as protection, never whether a REFUSED one may, and
  turning it off does not ask Logweir to call a mismatched digest healthy. The
  catalog is consulted only for a verdict the controller never reached, so a refused
  point is not handed a capture time either and never becomes the newest available
  point.
  **The trust boundary this moves, and why it holds:** the answer now comes
  from the content of a namespaced `ConfigMap`, but those page ConfigMaps are
  created `immutable: true` and owned by the catalog sync Job, the writer never
  adopts a foreign-owned object (a 409 is routed through the page-acceptance
  check), and `RecoveryCatalog.status.pages` — which names them — is a status
  subresource a tenant does not write. Deleting a page yields
  `CatalogUnreadable` and therefore `Unknown`, never a forged pass.

Where nothing can place the point — no catalog, or no row for it — the policy
reports `Protected=Unknown` with reason **`PointFactsUnread`** and a sentence
saying a run succeeded and its point could not be placed in time. That is a new
member of the `Protected` condition's `reason` set; the field is a free string in
the CRD, so **no schema change and no conversion** is involved. **Upgrade:** a
policy that read `Unprotected`/`NoAvailablePoint` on this posture moves to
`Healthy`, `Stale` or `Unknown` on the first pass after the upgrade, and an
`ArchiveUnavailable` incident may open where the catalog calls the bytes
degraded. **An open `Staleness` incident resolves only where the new health is
`Healthy` or `AtRisk`** — D3 §3.3's resolve column, verbatim: "`health` back to
`Healthy`/`AtRisk`". A policy that lands on `Unknown`/`PointFactsUnread` keeps
its incident open and un-renotified until someone gives it a `catalogRef` or a
readable receipt: Logweir does not claim a condition cleared because it stopped
being able to look. **Rollback:** nothing is persisted that an older controller cannot
read — `PointFactsUnread` only ever appears in a `reason`/`message` string — and
the old build recomputes its own verdict on its next pass. Installations where
the controller does read receipts (`ControllerIdentity`) see no change at all:
`status.capture` is present, so none of the three paths is taken.

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
touch four files this worker does not own. **The NetworkPolicy half is still not
covered, and W13 did not take it either — the reason is below, and it is not a
missing selector.** `logweir-runner-egress` selects
`batch.kubernetes.io/job-name Exists`, so a delivery pod inherits egress to the
broker ports as well as 443, where D3 §9 wants a notifier reaching DNS and 443
only; on an enforcing CNI a compromised runner image in a delivery pod can reach
a broker.

**Why a selector is not enough, corrected at W13's review.** The labels are
there — `controllers/protection_policy.rs` puts
`logweir.dev/component: notification` on the delivery Job *and* on its pod
template, and `controllers/retention_policy.rs` does the same with
`logweir.dev/component: retention` — so for those two classes a narrow policy
really is one selector away. What is not one selector away is the OTHER half:
**NetworkPolicies are additive**, so adding a narrow policy for a delivery pod
does not take anything away from it. The broad `logweir-runner-egress` matches
that same pod and keeps granting the broker ports, and a narrow policy beside it
changes nothing at all. Narrowing therefore means editing the SHIPPED policy's
`podSelector` to exclude those components
(`matchExpressions: [{key: logweir.dev/component, operator: NotIn, values:
[notification, retention]}]`), which changes the egress of every runner Job in
every installation — and Docker Desktop, the only cluster this project has,
enforces no NetworkPolicy, so the change could be made but its effect could not
be observed. The third class, catalog sync, has no component label at all
(`controllers/recovery_catalog.rs` sets none), so it would need a controller
change first.

**Who owns it now.** The two policies plus the `NotIn` on the broad selector,
and the catalog-sync label, are a wave that can both edit
`crates/weirkeeper/src/controllers/recovery_catalog.rs` and test on a CNI that
enforces. It is **not** W13, which shipped the ServiceAccounts and the RBAC and
owns no controller source; recording it against a finished wave is how a gap
stops being looked for.
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

### 7f. A `RetentionPolicy` in `Enforce` is the one thing Logweir does that cannot be undone

Everything else in this product is additive. A backup writes objects, a restore
writes topics, a verification writes a verdict; a mistake costs storage or a
scratch cluster. Deleting an archived recovery point costs the point. That
asymmetry is why retention has its own kind, its own credential, its own binary
and its own gate, and why **`mode: Report` is the default and deletes nothing at
all**.

**Four gates stand between a rule and a removed object**, and every one of them
is observable on the object or in the cluster:

1. `spec.mode` must be `Enforce`. `Report` evaluates and publishes; nothing is
   created.
2. `spec.enforcement.approvedPlanSha256` must equal
   `status.lastEvaluation.planSha256`, and the plan must be younger than
   `planMaxAgeSeconds`.

   **The digest covers what would be deleted, and nothing that moves on its
   own.** It is over the policy's identity, the destination, the scope, the
   rules as applied and the exact lines — no instant, no `metadata.generation`,
   no counter. So **content invalidates an approval and counters do not**: a
   `rules` edit, a `holds` edit or a new backup changes the lines and therefore
   the digest, while an edit to `deadlineSeconds`, `schedule`,
   `credentialSecretRef` or `mode` leaves an approval standing, because none of
   them changes one key. That is deliberate and it is load-bearing:
   `approvedPlanSha256` is itself a `spec` field, so a digest over
   `metadata.generation` would be changed by the very act of approving it, and
   no approval could ever match. `planMaxAgeSeconds` is what bounds a stale
   approval; the digest is what bounds a wrong one.
3. The controller writes `status.lease` with a resourceVersion-preconditioned
   PATCH — **and it must land**; a 409 aborts the pass before any Job exists —
   and *then* performs a consistent, non-cached, cluster-wide list of
   `Restore`s. The order is the property: a restore that arrives after the lease
   is seen by the list. A `Restore` is matched to this destination **by
   identity**: `spec.sourceDestinationRef.name` equal to the policy's
   `spec.destinationRef.name` in the same namespace, and URL equality only for a
   legacy restore that names no destination at all.
4. The worker re-validates every key against `<scope.prefix>/<backupId>/` and
   refuses the whole plan, deleting nothing, on the first one outside it.

**The controller cannot delete, and that is a link-time fact.** It holds no
`delete` verb on any resource, no verb on `secrets`, and — the part a grep
cannot establish — it does not link the code that deletes.
`crates/logweir-reaper` is the only crate in the workspace that names an
object-store delete, `logweir-retention` is the only binary that links it, and
`scripts/check-no-archive-write.sh` check 3 proves that from `cargo metadata`
over every dependency kind, dev edges included. Adding the edge to `weirkeeper`,
`logweir-store`, `logweir` or `logweir-api` fails `just lint`.

**Two credentials, and neither alone is enough.**

| credential | where it comes from | what it may do |
|---|---|---|
| the delete grant | `spec.enforcement.credentialSecretRef`, projected by `secretKeyRef` into retention Jobs only | `s3:ListBucket` on the bucket with a prefix condition, `s3:GetObject`/`s3:DeleteObject` on `<prefix>/*` **excluding `logweir/*`** |
| the `evidenceWrite` grant | the `BackupDestination`'s own `spec.access.evidenceWrite` | create-only puts under `logweir/`, and no delete |

The first can remove a point and cannot write the document that attributes its
removal. The second can write that document and cannot remove anything.

**Both are bound (FX-20).** The delete grant's Secret must carry
`status.credentialBinding` — this policy's UID, the route of the destination
`destinationRef` resolves to, and `spec.scope.prefix` — under
`logweir-binding`, and the `evidenceWrite` Secret the destination's own
binding; the worker refuses either otherwise, before it builds a handle
(`retention-refusal=CredentialBindingMismatch`,
`Enforced=False/CredentialBindingMismatch`, nothing deleted). A destination
deleted and re-created under the same name at another route changes the
policy's binding, so the delete key never follows the name (§20.10).

**A run with no `evidenceWrite` credential exits 3 having deleted nothing**, and
is refused before any handle is built. A credential that exists but cannot
actually write under `logweir/` is caught one step later and by a different
mechanism: opening the sink configures a client and performs no round trip, so
the guarantee that holds there is the narrower true one — *the first point whose
intent tombstone cannot be written is not deleted, and neither is any point
after it*. Either way nothing is removed unattributably; only the exit code
differs (3 against 1).

**Which grant that is, exactly.** The controller resolves the destination's
`evidenceWrite` role for the record credential — its own role, from the same
read that resolves `archiveRead` for the location — and projects it as
`LOGWEIR_EVIDENCE_AWS_*`. **`evidenceWrite` absent still means `archiveWrite`**,
exactly as §7's absent-field rule and `docs/install.md`'s *absent grants do not
widen* say it does; an installation that never separated its principals is
unaffected by any of this. What is **never** used here is `archiveRead`: it is
the read-only principal §7a recommends, it is not a fall-back for these
variables, and until the fix for `RET-EVIDENCE-GRANT-IS-ARCHIVEREAD` the build
projected it anyway — so a destination that separated the two had every intent
tombstone refused `403 AccessDenied`, `state=Kept code=TombstoneRefused`,
`deleted=0 failed=1`, and could not enforce retention at all. That was measured
live before it was fixed.

**A role that resolves to nothing a Job can use is refused before anything is
spent.** Three cases: a malformed grant; a grant with no static keys (the worker
builds its record store from `LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID` and
`…_SECRET_ACCESS_KEY` and has no workload-identity path, so a `WorkloadIdentity`
grant renders a Job that can only exit 3); and a grant naming the same Secret as
`spec.enforcement.credentialSecretRef`, because one principal that both removes
a point and writes the record attributing its removal can forge that record.
Each gets `Enforced=False`, reason `EvidenceGrantUnusable`, with the field named
— `spec.access.evidenceWrite`, or `spec.access.archiveWrite` when the role
defaulted to it — and **no Job, no plan `ConfigMap`, no lease, no run record and
no retry-budget slot spent**. `status.enforcement` drops to `RecommendationOnly`
and `status.guarantees.ageExpiry` to `NotEnforced`, so no console sentence
claims a worker is deleting under a policy that will not create one until a
human edits the destination. `mode: Report` is untouched throughout, because a
policy that deletes nothing has no deletion to attribute. Adding or fixing the
grant is the whole remedy: enforcement resumes on the next reconcile, within
about a minute, with no spec edit and no restart.

**The same-Secret check compares NAMES, and that is all it can do.** Two
differently named Secrets may hold identical keys, and this controller reads
neither — it holds no verb on `secrets` at all. So Logweir can refuse the
obvious spelling of "one credential doing both jobs" and cannot verify the
thing that actually matters. **That the delete grant and the record grant are
genuinely distinct principals, with the IAM scopes in the table above, is an
operator obligation**; §7a's measured table is where to check them.

**The enforcement Job's image is the runner image, and it carries two
binaries.** The controller renders the Job from its own `LOGWEIR_RUNNER_IMAGE`
(the chart's `runnerImage`) and overrides only the container's command, which is
`logweir-retention` and never `logweir`. That binary ships in the runner image
beside the everyday one — `Dockerfile` builds `-p logweir -p logweir-retention`
and copies both to `/usr/local/bin/` — so there is no `retentionImage` value to
set and nothing extra to publish. It did **not** ship before 2026-09-18, and the
symptom was exact: the Job's pod terminated `ContainerCannotRun`, exitCode 127,
`exec: "logweir-retention": executable file not found in $PATH`, with the
policy's own status unchanged. Two checks now stand between that and an
operator: `./scripts/render-install.sh --check` refuses to render an install
file whose enforcement chain is broken, and `scripts/check-image.sh` check 7 —
run by `just smoke` and by the image workflow, on the local tag and again on the
digest the registry returns — runs `logweir-retention --version` out of the
image by bare name, so the container runtime resolves it through `PATH` exactly
as the kubelet does, and asserts that the binary names *itself*. **Shipping the
binary widens no boundary in the control plane**: `logweir` still links no
delete path and `scripts/check-no-archive-write.sh` check 3 still holds the set
of crates reaching `logweir-reaper` to exactly `{logweir-retention}`.

**Where the margin actually is, stated exactly.** `logweir-retention` reads its
archive credential from unprefixed `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` —
the same variable names an ordinary destination-backed Job is given, which is
why the controller filters those two out of the retention pod. The executable is
now present in every runner pod, so what stops a deletion from one is **not** the
absence of a binary and not the absence of a credential name: it is that the
destination's archive grant does not carry `s3:DeleteObject`. That is a
deployment property. **An operator who grants `DeleteObject` on the archive
prefix to the ordinary backup credential loses this margin**, and should either
not do that or run enforcement from a separately built image.

**The bounded retry, and how to read it.** Three consecutive failed runs set
`EnforcementDegraded=True/ConsecutiveFailures` and stop scheduling until the
spec changes — "the spec changed" being `metadata.generation !=
status.observedGeneration`, which is the only thing on the object that says so.
One successful run clears `status.consecutiveRunFailures` to 0, so *consecutive*
means consecutive and a policy that recovers is not degraded by history. Neither
time nor a controller restart releases a degraded policy; editing the spec does.

```bash
kubectl --context docker-desktop -n <namespace> get retentionpolicy primary \
  -o jsonpath='{.status.consecutiveRunFailures}{"\n"}{.status.lastEnforcement.finishedAt}{"\n"}'
```

**On a build before 2026-09-19 that counter is stuck and the bounded retry does
not exist** (defect RET-DEGRADED-UNREACHABLE). `status.lastEnforcement` is
written by a merge PATCH, and the patch that recorded a *starting* run left the
*previous* run's `finishedAt` in place; the controller reads exactly that field
to decide whether the run it just started still needs harvesting, so from the
second run onward it decided there was nothing to track. No run after the first
was ever harvested, no exit code was ever read, and `consecutiveRunFailures`
stopped at 1 while the policy kept creating deletion Jobs and reporting
`Enforced=True`, `EnforcementDegraded=False` — a healthy-looking policy whose
every run was failing. Observed on docker-desktop: five enforcement Jobs in
140 seconds, all failed, the counter at 1. **The tell is a `lastEnforcement`
whose `runId` is current but whose `finishedAt` is older than its `startedAt`.**
A started run now deletes all seven of the previous run's terminal fields with
explicit `null`s.

A second guard sits beside it: a `/status` write that has no
`metadata.resourceVersion` to precondition on is refused rather than sent as a
blind write, and the harvest that could not publish its exit code no longer sets
the Job's `ttlSecondsAfterFinished`. The pod therefore survives for the next
pass to read, instead of being collected with the exit code still unpublished.

**On a build before 2026-09-20 the counter moves but the condition is never
there.** `status.conditions` is an array, and an RFC 7386 merge PATCH replaces an
array whole — so a pass that named only its own conditions deleted every other
one. The harvest published `EnforcementDegraded` correctly and the very next
evaluation pass, whose four conditions do not include it, took it back off the
object. The symptom is exact: `consecutiveRunFailures 3`, scheduling correctly
stopped, and `EnforcementDegraded` **absent** — not `False`, not present at all —
so `Ready=True` and `Evaluated=True` were all a console could see and nothing
said the policy had stopped or why. It was never only that condition: a pass that
evaluated and was then refused published `Enforced` alone and removed `Ready`,
`Evaluated` and `ExternalLifecycleConflict` with it. Every status write now
upserts into the conditions it already carries. On the same builds the
generation bump that releases the stop did not clear
`consecutiveRunFailures`, so the policy got exactly one run before the next
failure re-degraded it; the count is now reset in the same patch that adopts the
new generation.

**Every writer that adopts the generation releases the budget with it.** The
controller writes `status.observedGeneration` from seven places, and
`evaluate()` returns through the refusal writers — an unreadable catalog view, a
refused plan, an unusable destination, a declared external lifecycle — before it
ever reaches the evaluation. Until 2026-09-20 only the evaluation performed the
reset, so a degraded policy whose view was *also* unreadable **consumed the
operator's spec edit without releasing anything**: `observedGeneration` moved,
`consecutiveRunFailures` stayed at 3, and every further edit was eaten the same
way. Since the runs were failing for a reason, that co-occurrence is the likely
case rather than an exotic one. The adoption and the release are now one
function and one preconditioned patch, so a conflict between them cannot leave
the generation new and the count at its ceiling.

**What the enforcement Job is NOT: the evidence.** A harvested retention Job gets
`ttlSecondsAfterFinished = 600`, so ten minutes later Kubernetes deletes it and
its pod. A census by `ownerReference` therefore undercounts — after three failed
runs it may see two Jobs, or none — and that is the design, not a discrepancy:
the pod is where the log lives, the log is read once at harvest, and keeping
finished Jobs forever would keep their pods forever.

The durable evidence is, in order: `status.consecutiveRunFailures` for how many
runs failed, `status.lastEnforcement` for what the last one did (`exitCode`,
`deleted[]`, `failed[]` with the closed per-point codes, `objectsDeleted`), and
`status.lastEnforcement.recordKey` for the **create-only, unsigned record in
object storage**, which outlives the Job, the pod, the controller and the policy.
`EnforcementDegraded`'s message names the count and the last run's exit code and
codes so that the common question is answered without following any of them.
**Do not use a Job census to decide whether a run happened**; use the record.

**Enforcement needs the retention ServiceAccount in the policy's namespace.**
Every Job this controller builds requests `logweir-retention`. The chart renders
it when `retention.enabled` is true, in the release namespace and every
`identity.authorizedRunnerNamespaces` entry; on the low-level path apply
`config/rbac/retention-serviceaccount.yaml` ([install.md](install.md) step 4).
Without it an `Enforce` policy produces a Job whose pod the API server will not
admit — fail-closed, and nothing is deleted.

The controller never reads either Secret — `config/rbac/role.yaml` grants it no
verb on `secrets` at all — and never inherits them: the reaper builds its handle
with `AmazonS3Builder::new()` and the destination's own frozen addressing, never
`from_env()`, so no ambient `AWS_ENDPOINT_URL` can relocate a deletion
(seam **S5**).

**What a run writes, and in what order.** Per point: the create-only intent
tombstone — **unsigned in this build**, see below — at
`logweir/retention/<policyUid>/<runId>/<pointId>.intent.json`, then
the **manifest**, then the segment objects, then the completion tombstone. The
manifest goes first so that a run interrupted halfway leaves a set the catalog
reports `Missing` rather than a plausible-looking `Partial` one, and the leftover
segment keys are exactly what the next plan names — completion is idempotent. The
run's own record lands at `logweir/retention/<policyUid>/<runId>.json` and
`status.lastEnforcement.recordKey` points at it.

```bash
kubectl --context docker-desktop -n <namespace> \
  get retentionpolicy primary -o jsonpath='{.status.lastEvaluation.planSha256}'
# sha256:…  — copy this into spec.enforcement.approvedPlanSha256 to authorise a run
```

**`status.lastEvaluation.planRef` names the plan THIS evaluation rendered**, and
it moves whenever `planSha256` beside it moves — including on an evaluation that
renders a new plan and starts no run, which is most of them. It was previously
written only by a pass that started a run, so the two fields of one block could
describe two different plans and the ref an administrator follows to preview a
deletion was the one belonging to a run that was already over. The name is
derived from the policy UID and the digest, so the ref is exactly as true as the
digest it sits next to; the `ConfigMap` itself is created by the pass that starts
the run, so the ref can name an object that does not exist yet, which is the
documented absent-object behaviour and not a fault.

**`status.lastEvaluation.at` is when the findings beside it were first
reached** (FX-29), not when the controller last looked. The controller
evaluates on every reconcile, and a later evaluation that finds the same counts,
candidates, protected and skipped points, plan and plan expiry keeps the instant;
the first one that finds something different moves it. It used to be the clock
of every evaluation, and since this controller's own status write wakes it, every
`Report` or `Enforce` policy whose catalog resolved rewrote its status on every
pass for as long as it existed. The `EVALUATED` printer column therefore reads
"how long these findings have stood"; whether the controller is alive is the
`Ready` and `Evaluated` conditions' business, and a new plan digest still moves
`at` the moment it is rendered.

**`status.lastEvaluation` counts every point once, and a point the per-run
ceiling held back is not "kept"** (FX-22). Four numbers add up:

| field | what it counts |
|---|---|
| `pointsEvaluated` | every point of this destination in the catalog view the evaluation read |
| `keptCount` | the points that stay: the rules keep them, or something protects them (those are in `protected` too) |
| `candidateCount` | the points **this plan** would remove, at most the ceiling |
| `truncatedByCap` | the points the rules would remove, that nothing protects, and that the ceiling left out of this plan |

`pointsEvaluated` = `keptCount` + `candidateCount` + `truncatedByCap` + the
points in `skipped`. `maxDeletionsPerRun` beside them is the ceiling the
evaluation applied: `spec.enforcement.maxDeletionsPerRun`, or the default of 50
for a policy with no `spec.enforcement`, because a `Report` preview is bounded
as a run would be. A held-back point is **due, not kept**. It is in no list
(`kept` names only what stays, `candidates` only this plan), and no run deletes
it until a later plan names it. A plan takes the due points newest first, so the
ones held back are usually the oldest, but not always: a backup set is planned
whole or not at all, so a set that does not fit the room left under the ceiling
is held back while older single points behind it are planned. The `Evaluated`
message says it in words, and `kubectl get retentionpolicy` prints a `HELD-BACK`
column between `CANDIDATES` and `EVALUATED` (a script that reads that output by
column position must skip one more column):

```text
371 point(s) evaluated at this destination: 10 kept, 50 candidate(s), 0 protected,
0 skipped. 311 more point(s) are due under the rules and held back by the per-run
ceiling (maxDeletionsPerRun 50): they are not kept and not in this plan, and they
stay due until a later plan names them. …
```

Until this build those points were listed under `kept` and nothing published
their number: with 371 points a policy read "321 kept, 50 candidate(s)" for
`keepLast: 300` and for `keepLast: 10` alike. `truncatedByCap: 0` means the plan
is everything the rules would remove. An **absent** `truncatedByCap` or
`keptCount` means an older controller wrote the block: its `kept` list may hold
held-back points, so the product API and the console publish no kept count and
no `kept` rows for it, and never derive a count from that list. The product API
says which case it is on every answer: `lastEvaluation.accounting` is
`Recorded` or `NotRecorded`, so an absent `kept` is never the only signal
([api.md](api.md)). The plan document, `planSha256` and what a run deletes are
unchanged, so an approved digest stays approved. To clear a backlog in fewer
runs, raise `spec.enforcement.maxDeletionsPerRun` (1–500).

**When no due point fits the ceiling, the plan is empty and the object says
why.** A backup set that more due points name than `maxDeletionsPerRun` is
never selected: a set goes whole or not at all ("Plans over re-run receipts
change", below). When every due point is in such a set, `candidateCount` is
`0`, `truncatedByCap` is every due point, and no later plan names them either:
the next evaluation finds the same sets over the same ceiling. The object does
not call that "nothing to remove":

```text
Evaluated: … 2 point(s) are due under the rules and held back by the per-run ceiling
(maxDeletionsPerRun 1): they are not kept, and this plan is empty because not one of
them fits. Each is in a backup set that more due points name than the ceiling (sets that
share objects count as one), a set is planned whole or not at all, and no plan names
them until maxDeletionsPerRun is raised.

Enforced=False/NothingFitsCeiling: 2 point(s) are due under the rules and the plan is
empty: not one of them fits the per-run ceiling (spec.enforcement.maxDeletionsPerRun 1).
… Raise spec.enforcement.maxDeletionsPerRun to at least the number of points that name
the smallest of those sets; 2 fits all of them.
```

`NothingFitsCeiling` is a closed `Enforced=False` reason, written only for an
`Enforce` policy whose plan is empty while `truncatedByCap` is above 0. An empty
plan with nothing due is still `Enforced=False/NothingToDo`. A `Report` policy
is `RecommendationOnly` either way and says it on `Evaluated` alone; its ceiling
is `spec.enforcement.maxDeletionsPerRun` when it carries that block and the
default of 50 when it does not. Neither condition changes what is planned: the
plan is empty in both cases, and its digest is the empty plan's.

**`status.lastEvaluation.viewIncomplete: true` says the evaluation did not see
the whole archive.** The evaluation reads the catalog's view (§7d), and the
catalog says when that view is not every point: `status.truncated` (the view is
a window of the newest `spec.sync.viewLimit` points), or
`status.cursor.complete: false` (the sync's object budget ran out and its pages
were published anyway, `Synced=False/ScanIncomplete`). Points outside the view
are not evaluated, are in none of the four counts, and are **never candidates
while they stay outside it**. In a window those are the oldest points, the ones
an age rule is for, so a policy over an archive larger than its catalog's
`viewLimit` does not expire them: raise `viewLimit` (100–5000), or let the sync
finish. **Raising `viewLimit` helps only when the limit is what cut the view.**
The catalog also sets `status.truncated` when it left entries out for page
space, when an entry was too large for one page, and when its walk counted rows
it then merged as duplicates; the `Evaluated` message says so beside the
catalog's own numbers, and the catalog's `Synced` message counts the entries it
refused as too large. `false` means the catalog said its walk finished and its
view holds every point it counted; absent means the catalog did not say. The
evaluation of the points that are in the view is unchanged, and so is the plan.

The product API and the console apply one rule to this member: **a warning is
never hidden, and completeness is never asserted when it is not recorded.**
`viewIncomplete: true` is published and shown whether or not the four counts
are recorded. `false` ("the whole archive, the catalog said") is published and
shown only beside counts that are recorded (`accounting: Recorded`). Absent, or
`false` beside counts that are not recorded, reads "not recorded".

**Upgrade and rollback of these four members.** They are additive `status`
fields: **apply the CRDs before rolling the controller**, as for every upgrade.
The first evaluation after the upgrade rewrites each policy's status once:
`kept` loses the held-back points and the four members appear
(`lastEvaluation.at` moves only when `kept` changed). A controller rolled out
ahead of its CRD has the four members pruned by the API server on every write;
it leaves `lastEvaluation.at` where it was and sends one patch per pass that
changes nothing, until the CRD is applied. While that lasts the product API
answers `accounting: NotRecorded` for every policy (the stored block has no
counts), and the console's panel reads "not recorded" in both cells.

After a rollback of the controller image alone, the older controller rewrites
`pointsEvaluated`, `candidateCount` and the lists and cannot remove the four
members (a merge patch leaves the keys it does not name), which then describe
the newer controller's last evaluation. What the product API and the console
show depends on whether the ceiling had cut that policy's plan:

- **`truncatedByCap` was above 0.** The older controller's first write puts the
  held-back points back under `kept`: 321 ids beside a `keptCount` of 10. From
  that write on, the API answers `accounting: NotRecorded` and publishes no
  `keptCount`, no `truncatedByCap`, no `maxDeletionsPerRun` and no `kept` rows
  for the policy, and the console prints "not recorded" for "kept" and "held
  back by the per-run ceiling". This holds **between the rollback and the next
  archive change too**, when the stale counts still add up: a `kept` list that
  is not `keptCount` long is refused before the sum is read. `candidateCount`,
  `candidates` and the plan are the older controller's own and are published as
  before.
- **`truncatedByCap` was 0.** The older controller's `kept` list is the same
  list, so it leaves `lastEvaluation` as it is, and the API and the console go
  on showing the newer controller's counts. They stay `Recorded` for as long
  as the older controller's `kept` list is `keptCount` long and the counts add
  up, which is while the policy keeps the same number of points and its plans
  stay under the ceiling; the counts shown are then still true. The first plan
  the ceiling cuts puts held-back points under `kept` again, the list is
  longer than `keptCount`, and the block reads `NotRecorded`, as above. **One
  block reads as recorded and is not**: if the number of points the policy
  keeps falls by exactly the number the ceiling newly holds back (a hold
  expires over a plan already at the ceiling), the list has its old length and
  the stored `truncatedByCap: 0` is stale. The check compares lengths and sums
  and cannot see that; pruning the members (below) removes the doubt.

`viewIncomplete: true`, if the newer controller's last evaluation wrote it,
stays visible on both surfaces until the members are pruned (a warning is never
withheld); a stale `false` is not shown once the counts read as not recorded.
`kubectl get` goes on printing the stale `HELD-BACK` value. With `kubectl`,
re-apply the older CRDs to prune the four members, or ignore them while the
older controller runs.

**A retention run is recorded on the object BEFORE its Job exists.** The order
is: the run record (`status.lastEnforcement.runId`, `startedAt`, `planSha256`,
with every terminal field of the previous run cleared), and only then
`POST …/jobs`. A record write that does not land therefore creates no Job at
all, and `Enforced=False/RunNotRecorded` says so — a Job created past a refused
write would be a deletion run nothing harvests, nothing counts against the retry
budget and nobody can account the deletions of, and this controller holds no
`delete` verb on `jobs` to withdraw it (Global Constraint 6). The pass reads the
Job at the run's deterministic name before it records anything, so a Job that
already stands there ends the pass instead of being re-recorded as a fresh run.

**Every `/status` write this controller makes is a merge PATCH preconditioned on
`metadata.resourceVersion`** — on every kind, not only the ones that said so.
A `409 Conflict` is the precondition working: the pass stops, nothing it computed
reaches the object, and the next pass reads what the other writer stored. A pass
that writes more than once preconditions each later write on the version the
previous one returned, so the sequences that must complete within one pass —
a `Backup`'s freeze then run, either kind's outcome then evidence verdict — are
not refused by their own first write.

**The approved plan names each set's key BOUND, not its objects — and no
surface pretends otherwise.** The catalog view carries a point's `manifestKey`
and no segment list, and this controller holds no archive credential for the
destination, so a plan line names the manifest, the set prefix
`<scope.prefix>/<backupId>/`, and `enumerate_set: true`. The worker lists that
bound and re-validates every key it gets back before deleting it — which is the
second half of the wrong-prefix rule ("listed from that point's own manifest or
set directory"). Three consequences an administrator is entitled to have in
front of them:

* `status.lastEvaluation.candidates[].objects` is **omitted**, not `1`. Absent
  means *not observed*, which is the rule everywhere else in this API; the
  `Evaluated` condition message says the view carries no segment keys and the
  plan does not enumerate.
* The real count comes from `logweir-retention run … --dry-run`, which holds the
  list grant the run needs anyway and prints `retention-point=<id> state=Kept
  objects=<n> code=DryRun` per point. Nothing deletes on that path: the deleter
  is never called, which a test asserts over a deleter that panics.
* **An object that appears under an approved bound between the preview and the
  run is removed**, without having been in the approved bytes. That is the
  honest cost of the bound, and it is what `spec.enforcement.planMaxAgeSeconds`
  is for.

**`status.enforcement` says what is actually happening**, which is not always
what `spec.mode` asked for: `RecommendationOnly` (nothing is deleted — every
`Report` policy, and an `Enforce` policy that cannot run, for example
`EvidenceGrantUnusable` above), `LogweirWorker` (the isolated worker deletes
under this policy) or `ExternalLifecycleDeclared` (the bucket's own rule does).
The console keys its retention sentence on this field and never on the mode.

**`status.guarantees` says who is enforcing what, and never flatters anyone.**
Each of `ageExpiry`, `minUsablePoints`, `activeRestoreProtection`,
`sharedSegments` and `legalHold` reads `LogweirEnforced`,
`ProviderEnforcedUnverified` or `NotEnforced`. Two of them are never
`LogweirEnforced` on a view this build can read, and the reasons are different:

* `sharedSegments` is **`NotEnforced`**, because the guarantee needs a point's
  segment keys and the catalog view entry has no segment field at all. What
  **is** enforced is the set half of it. Two receipts can name one backup set
  — before RECEIPT-DUP was fixed a runner Job re-created from its frozen inputs
  rewrote the same `<prefix>/<backupId>/` and signed a second receipt over it,
  and such sets, and sets whose first run was made by a build without the
  execution claim, are still in buckets — and every key a
  plan line may remove lies under its own set's directory. So the evaluation
  groups points that share a `backupId`, a manifest key or a segment key
  (transitively), and a group holding any retained point — kept, protected,
  skipped, or a row whose location the catalog could not establish — plans
  none of its candidates: each is protected `SharedSegment`. The
  `maxDeletionsPerRun` ceiling selects such a group whole or not at all, and
  the plan writer refuses outright (`Evaluated=False`, no plan) a line whose
  set a retained point still names. When EVERY receipt of a set is due, the
  set is removed by ONE plan line that names the others in `co_point_ids`:
  each point gets its own outcome and tombstones, and the objects are counted
  once. What stays unseen is a manifest that names a segment under *another*
  set's directory; the engine does not write that layout, but the guarantee as
  worded covers it, so the value stays `NotEnforced` and the `Evaluated`
  message says which half is in force. It becomes `LogweirEnforced` on its
  own, with no code change, the day a view entry carries its keys.
  A point whose set id names no single directory is never a candidate: an
  empty id, one carrying `/`, and — since FX-17 — one the catalog published
  as the redactor's output (`backupId` or `manifestKey` carrying
  `[redacted]`; a long set id someone chose for `logweir backup run`) is
  protected `Unknown`, so it neither shares a group with every other such
  point nor makes the plan writer refuse the whole plan. A scheduled run's
  set id (`<schedule uid>-<slot>[-r<k>]`) is published whole by a runner after
  v0.2.0-rc.1, and a set and its retry are two directories: the bound
  `<scope>/<backupId>/` ends in `/`, so removing one never enumerates the
  other.
* `legalHold` is `ProviderEnforcedUnverified` even in `Enforce`, because
  `object_store` 0.14 exposes no WORM readback. "Legal hold respected" means
  exactly *a provider refusal is authoritative, recorded, not retried, and
  excluded from the next plan* — never "Logweir knows the hold exists". The
  exclusion half is real: `status.lastEnforcement.failed[]` carries the closed
  code `Locked`, and the next evaluation protects that point as `LegalHold`.
* **A provider can only refuse a delete it is asked to make, and on a
  versioned bucket a delete by key asks for nothing.** Every S3 Object Lock
  bucket is versioned, and on a versioned bucket a `DELETE` with no version id
  is answered success and removes nothing: the provider writes a delete marker,
  the data stays as a noncurrent version, and a hold on that version is never
  consulted (measured on the lab MinIO by harness-rows-11's `object-lock` row).
  `object_store` 0.14 can neither delete a specific version nor read the
  marker header off the response, so the worker **refuses the combination**
  rather than record a marker as a deletion, from three signals:
  1. **The bucket, now.** Before any delete of a point the worker PUTs its
     create-only intent tombstone into the same bucket (under `logweir/`). A
     provider answers that PUT with a version id exactly when versioning is
     *Enabled* on the bucket (measured on MinIO: a version id from an Enabled
     bucket, none from a plain or a Suspended one). Then nothing of the point
     is deleted and it is `Kept` with code `VersionedBucket` — including
     objects written *before* versioning was turned on, whose own HEAD
     carries no version id and which a delete by key would still only hide.
  2. **The key.** Before every delete it HEADs the key; a current version
     carrying a version id (`x-amz-version-id`) is not deleted — the point is
     `Kept` (or `Orphaned`, if the manifest had already gone) with
     `VersionedBucket`. This catches objects stored under Enabled versioning
     in a bucket since *Suspended*, where the tombstone PUT carries no id.
  3. **The bucket, after the deletes.** Versioning can be switched on while
     a point's deletes run — minutes for a large set — and every delete after
     that is a marker the per-key HEAD cannot see (a null version answers no
     version id). So after a point's deletes the worker PUTs one more
     create-only check object (`logweir/retention/<policyUid>/<runId>/<pointId>.check.json`)
     into the same bucket. If that comes back with a version id, or cannot be
     written, the point is NOT recorded `Deleted`: it is `Orphaned` with
     `VersionedBucket` (or `VersionCheckRefused:…`), every planned key named as
     possibly remaining and no object counted as removed.

  The run exits 1, and three such runs set `EnforcementDegraded`, whose
  message names the remedy: enforce on an unversioned bucket, or declare the
  provider's own lifecycle (`NoncurrentVersionExpiration` honours holds) with
  `mode: ExternalLifecycle`. **It resumes on its own**: a policy degraded only
  by `VersionedBucket` or `VersionProbeRefused` starts one re-probe run 24 h
  after its last run — it deletes nothing unless the refusal has cleared —
  so no spec edit is needed once the bucket or the grant is fixed.
  `VersionedBucket` is not a hold verdict and does not protect the point as
  `LegalHold`; the `Locked` path remains for a provider that refuses a plain
  `DELETE` itself. The HEAD needs `s3:GetObject` on `<prefix>/*` — the scope
  D3 §6.5 always documented for the retention credential; without it every
  key is `Kept` with `VersionProbeRefused` and nothing is deleted.

  **`Deleted` means the CURRENT object at each key was removed — nothing more.**
  Noncurrent versions that bucket versioning keeps are the bucket's lifecycle
  responsibility; `object_store` 0.14 can neither list nor delete them, and
  Logweir does not see them. **Residual, accepted and tracked (it needs a
  version-aware store client):** a bucket whose versioning was Enabled, then
  *Suspended*, and whose keys were then written again — and re-run backups
  DID rewrite the same keys: before RECEIPT-DUP was fixed a runner Job
  re-created from its frozen inputs wrote the same `<prefix>/<backupId>/`
  objects, which is exactly how two receipts came to name one set (a build with
  the execution claim refuses that second engine run; sets written earlier
  remain). There the current version is a null version (no
  version id on the HEAD, none on a PUT to a Suspended bucket), the delete
  removes it, the point is recorded `Deleted`, and the Enabled-era version
  beneath it survives. No hold is possible on a suspended null version, and an
  Object Lock bucket cannot suspend versioning, so this is a false deletion
  record, not a hold bypass. **Do not enforce on a bucket whose versioning was
  ever enabled and later suspended** — use `mode: ExternalLifecycle` or a
  noncurrent-version lifecycle rule — **and do not change a bucket's
  versioning while a retention run is in flight.**

  **What else is not seen, stated so nothing claims it:**
  * whether AWS answers `x-amz-version-id: null` for a PUT or HEAD of a null
    version is unmeasured — the worker counts any version id, `null`
    included, as versioned, so the unmeasured answer can only refuse more;
  * a key whose latest version is *already* a marker (for instance one written
    by an earlier build) answers the HEAD `404` and is counted gone: nothing
    live remains, but its data may survive as noncurrent versions;
  * versioning switched on and then back to *Suspended* while one point's
    deletes run (two operator toggles inside one point): the post-delete check
    is written after the second toggle and answers no version id.

**`mode: ExternalLifecycle` is a declaration, not an enforcement.** It records
that a bucket lifecycle rule exists so a console can stop claiming retention is
unenforced. `ageExpiry` and `legalHold` become `ProviderEnforcedUnverified`;
`minUsablePoints`, `activeRestoreProtection` and `sharedSegments` become
`NotEnforced`, because a lifecycle rule cannot count usable points, cannot see an
in-flight restore and cannot reason about a segment two manifests share. Logweir
evaluates nothing in this mode and reads no provider configuration. When the
declared `expirationDays` is shorter than the policy's `keepDays`,
`ExternalLifecycleConflict=True` says so — **and the bucket wins**. A Logweir pin
cannot override a bucket lifecycle rule.

**The evaluation names the right destination, by construction.** Its input is the
`RecoveryCatalog`'s bounded view of *this* destination (§7d), read out of the page
`ConfigMap`s the catalog published and digest-checked against
`status.pages[].sha256`. A point whose `locations[]` does not name this
destination is not merely excluded from the candidate list — it is not counted in
`pointsEvaluated` either, so a console cannot read another tenant's total as its
own. Two `RetentionPolicy` objects covering one destination put **both** in
`Ready=False/Conflict` and neither evaluates: a plan an administrator could
approve is precisely what makes two contesting policies dangerous.

A point is a deletion candidate only if the catalog said `Available` **and**
`Verified`/`VerifiedHistorical`. Everything else — `Unreadable`, `Partial`,
`Conflict`, `UntrustedSigner`, `NotAttempted` — lands in
`status.lastEvaluation.skipped` and can never become a candidate. **A retention
pass that cannot read the archive proposes nothing**, which is the opposite of
what a timestamp-driven bucket rule does.

**The controller's own verdict outranks the row.** Before it counts a row the
evaluation lists the namespace's `Backup` objects and joins their
`status.evidence.verification.result` to the rows — by the full
`receiptSha256`, or by `backupId` for a `Backup` that carries no digest. A point
whose `Backup` the controller refused (`Invalid`, `Untrusted`, a result this
build does not recognise, or a `Valid` on a trust basis the badge refuses —
anything but no `trust` block, `trust: null`, `Current`, `Historical` or
`Unverified`) is skipped with reason `Unreadable` however
`Available`/`Verified` its row reads: a view is served until `viewExpiresAt`,
so the row may predate the refusal, and counted as usable it would take a
`keepLast` or `minUsablePoints` rank and push an older good point into the plan.
`NotAttempted`, `Pending` (the evidence fetch is still running), an absent
result, a `Valid` beside no `trust` block or a `Current`/`Historical` basis, and
a `Valid` on `Unverified` (nothing compared yet) leave the row in charge, and a
point no `Backup` in the namespace names is evaluated exactly as before. `Backup` objects are read
through a lenient projection of the three fields the rule needs: an object this
build cannot type does not stop the evaluation, and one whose verdict field is
present but unreadable refuses its own point like an unknown verdict. The
evaluation stays fail-closed where the join cannot be completed, and says so on
the object rather than retrying silently — `Evaluated=False`, reason
`BackupVerdictsIncomplete`, nothing planned, `Enforced=False` "nothing is
removed while the controller's Backup verdicts cannot all be read", with
`Ready=False` naming the cause:

| `Ready` reason | cause | remedy |
|---|---|---|
| `BackupHistoryTooLarge` | the namespace holds more than 10 000 `Backup`s (20 pages of 500) | prune the Backup history (PLAT-05.2); the catalog view is fine |
| `BackupVerdictsUnreadable` | the `Backup` list failed, or a refused/unreadable `Backup` names neither `receiptSha256` nor `backupId` | restore the controller's `list` on `backups`, or repair/remove the named objects |

The refusal the walk did not reach is exactly the one that would have enlarged
the plan. `Backup`s in other namespaces writing to the same archive are not
consulted. A point the evaluation skips is retained, so a segment it shares
with a candidate — or a backup set it names — protects that candidate as
`SharedSegment`.

And the newest `minUsablePoints` usable
points are kept whatever the rules say, reported as `MinUsablePoints` in
`status.lastEvaluation.protected` so an operator can see which points the rules
wanted and the floor saved.

**What protects a point being restored, today, and what does not.** D3 §6.5
calls for two guards. The one that exists is the controller's: before creating a
Job it lists every `Restore` in the cluster with a quorum read and **refuses the
run outright if any nonterminal one reads this destination** — not only if one
names a candidate's set. That is deliberately wider than the design asks, and it
is fail-closed: a run refused for a restore it would not have touched costs one
cadence slot, and the other way costs the set.

The restore-side half is only partly there. Rehearsal point selection holds
with reason `PointRetentionInProgress` while a matching `status.lease` exists
(§7g's table); `Restore` admission does **not** consult the lease in this build.
So the residual window is a `Restore` **created after** the
controller's consistent list and before the run's first delete. It is narrow; it
is real; and until the admission arm lands, an operator planning a large restore
during a retention window should suspend the policy (`mode: Report`) rather than
rely on the race.

**A retention failure never blocks a backup.** It is a different controller, a
different object and a different condition: an unreadable catalog view writes
`Evaluated=False/ViewUnreadable` and touches no `Backup`, no `BackupSchedule` and
no Job of any other kind.

**Upgrade and rollback.** The kind is additive and inert: an installation that
never creates a `RetentionPolicy` behaves exactly as before, and one that creates
a `Report` policy deletes nothing. An older controller ignores the kind, so the
objects sit with no status — visibly pending rather than silently wrong. **Do not
roll back with a retention Job in flight**: the old controller does not know
about leases, and the old restore admission does not hold on them. Set every
policy to `mode: Report`, wait for `status.lease` to clear and for any
enforcement Job to finish, and only then roll back. Objects already removed from
the archive are gone; the tombstones and the record under `logweir/retention/`
are what remains, and they are readable by any S3 client.

**Upgrading to a build with the versioned-bucket refusal and one line per shared
set** (defects OBJECT-LOCK-DELETE-MARKER and SHARED-SET-RETENTION), in order:

1. **Grant the retention delete credential `s3:GetObject` on
   `<bucket>/<prefix>/*` BEFORE upgrading the controller or the runner image.**
   Without it every enforcement run keeps every point (`VersionProbeRefused`)
   and deletes nothing. A policy that degraded on it anyway resumes by itself:
   one re-probe run is started 24 h after its last run. To resume at once,
   edit its spec (any change bumps `metadata.generation` and releases the
   budget).
2. **On a versioned or Object Lock bucket, `Enforce` now deletes nothing** and
   degrades with `VersionedBucket`. Enforcement records from earlier builds on
   such a bucket that say `Deleted` are FALSE: the data is still there as
   noncurrent versions behind delete markers. Move the policy to an
   unversioned bucket or to `mode: ExternalLifecycle`. **Do not change a
   bucket's versioning while a retention run is in flight, and do not enforce
   on a bucket whose versioning was ever enabled and later suspended** (use
   `mode: ExternalLifecycle` or a noncurrent-version lifecycle rule): re-run
   backups rewrite the same keys, and a deletion there removes only the
   newest copy while being recorded `Deleted` — the accepted residual above.
3. **Plans over re-run receipts change.** The older receipt of a shared set is
   now `protected: SharedSegment` until every receipt naming the set is due
   together, and then the set is removed by ONE plan line naming every point
   (`co_point_ids`). A digest approved over such a plan must be re-approved; a
   plan with no shared set is byte-identical and its approval holds. A shared
   set with more receipts than `maxDeletionsPerRun` is never selected — raise
   the ceiling to let it go.
4. **Rollback**: first set every policy on a versioned bucket to
   `mode: Report` — the older worker records delete markers as deletions
   there. An older worker also refuses a plan carrying `co_point_ids` (its
   plan type rejects unknown fields): the run exits 3 and deletes nothing.

**A residual the enforcer does not re-check** (review L3): a second receipt
signed over a set AFTER the run-start pass evaluated it — a runner Job
re-created from frozen inputs between the approval and the deletes — is not
seen by the worker, whose rails are the approved plan, the prefix and the
evidence root (D3 §6.5). It needs a re-run over a set old enough to be a
candidate; the next evaluation reads the new receipt, and a set it names is
never planned again. Since RECEIPT-DUP was fixed a runner with the execution
claim cannot sign that second receipt at all; the residual remains only for a
set whose first run was made by a build without the claim.
### 7g. A `RehearsalSchedule` proves recovery on a cron, under one signed authorization

A backup that has never been restored is a hypothesis. PLAT-14.3's
`RehearsalSchedule` is the object that tests it on a cadence: every slot it
picks a qualifying recovery point, restores it into an isolated scratch cluster
under a prefix nothing else uses, scores the result and tears the topics down —
and records a reason for every slot that produced no rehearsal at all.

**The spec is sealed except `suspend`.** The standing authorization binds a
`sha256` over the canonical JSON of `spec` minus `suspend`, so a template that
could be edited after approval would authorize work nobody approved. One CEL
rule enumerates every other field; a change means a NEW `RehearsalSchedule` and
a NEW authorization. `suspend` is outside the digest on purpose: pausing an
unattended rehearsal must not invalidate the document authorising it, or the
one control an operator reaches for in an incident would be the control that
breaks the schedule. The controller publishes the recomputed digest at
`status.templateDigest`, so minting an authorization is a copy and not an
arithmetic exercise.

**One standing approval, checked twice — and never minted here.** The
authorization is an ordinary immutable `Approval` with
`spec.subjectRef.kind: RehearsalSchedule` and `spec.planHash` equal to the
template digest. The Approval controller parses this arm as a
`StandingAuthorization`, not as the per-run approval document: after
the common DSSE, roster and key-lifecycle checks, it requires the exact
template digest and subject API version/kind/namespace/name/**UID**, a live
positive window of at most 90 days, and a complete scratch-only scope with
positive numeric bounds. These failures retain standing-specific reasons
(`TemplateDigestMismatch`, `SubjectMismatch`, `WindowInvalid`, `ScopeInvalid`
or `StandingDocumentInvalid`); they never masquerade as the Restore arm's
`PlanHashMismatch`. The standing format carries no approval-policy mode and
stays `GovernedApproval`-only whatever the namespace is bound to (§8, "Approval
policy"); PLAT-19.2's policy mode is carried end to end for per-run Restores
only, and `EvidenceSigning` is never authorization. Its
`spec.approvalBytes` is a signed `StandingAuthorization` document carrying the
subject (with its **UID**), the scope and `issuedAt`/`expiresAt`, and its DSSE payload type is
that document's own — so a genuinely signed drill approval replayed as a
standing authorization is refused by the signature layer rather than by a field
comparison. A verified standing condition is re-evaluated at the earlier of
the signed `expiresAt` and the matched policy key's `notAfter`; it cannot remain
green across either boundary. `weirkeeper` links no signer at all
(`tests/linkage.rs`), so the controller COPIES that envelope and its sidecar
into the run's bundle byte for byte; it cannot produce one.

Each slot the controller re-checks, in this order, and a failure is a recorded
skip with no `Restore`:

| It checks | Skip reason |
|---|---|
| `spec.suspend`, then topics a previous teardown could not remove | `LeftoverTopics` |
| the slot is inside `spec.bounds.startingDeadlineSeconds` | `ConcurrencyBlocked` |
| this schedule's own previous rehearsal finished | `ConcurrencyBlocked` |
| no other schedule is rehearsing against the same target cluster | `TargetBusy` |
| `spec.bounds.runnerResources` is one the controller applies: whole millicores and bytes, at most 4 CPUs and 8Gi, no zero limit, a memory limit of at least 32Mi, no request above its limit (FX-2, §12) | `AuthorizationInvalid` |
| the `Approval` is `Verified=True`, bound to **this object's UID**, its `planHash` is the recomputed digest, its key may still authorise and carries an approver usage | `AuthorizationInvalid` |
| the signed document has not expired and was not minted for more than 90 days | `AuthorizationExpired` |
| the target `KafkaCluster` reports `reachable: true` and a `clusterId`, and its saved connection resolves (the same `RestoreTarget` resolution the `Restore` admission makes — a SCRAM connection with no `secretRef`, say, is refused here by field) | `TargetUnavailable` |
| the signed scope's `templateDigest`, `targetClusterId` and `deadlineSeconds` agree with the sealed spec | `AuthorizationInvalid` |
| a point qualifies: covered by `spec.point.topics`, old enough, with a non-empty window, inside `maxPartitions`, not captured from the target cluster, not inside a retention lease | `NoQualifyingPoint`, `TargetUnavailable` or `PointRetentionInProgress` |
| the RENDERED plan falls inside the signed scope — including its coverage, which must be the scope's own (`coverage`, absent = sampled) and, for a complete plan, inside the scope's `completeMaxRecords` (PROD-08.1a) | `AuthorizationInvalid` |

**`spec.bounds.runnerResources` reaches the runner container.** It is copied
verbatim onto each child `Restore`'s `spec.runnerResources`, and from there
onto the runner container (§12, *The runner's requests and limits*). A block
outside the bounds can never run under the authorization that binds it, so
every slot is skipped as `AuthorizationInvalid` — the `Authorized` condition's
message names the field — and no child is created; the spec is sealed, so the
remedy is a new schedule under a new authorization. Before FX-2 the block was
copied onto the child and then dropped at the Job.

**A `Restore` carries its signed time basis (FX-8).** When the controller reads
a run's scorecard it copies `source.time_basis` (format 1.3.0) onto
`status.timeBasis` — `{plan, producerTime, notRecorded}`, beside `outcome` and
`integrity` and, like them, a claim until `status.evidence.verification` is
`Valid` — so the console and the product API can show which topics were
selected by the producers' clocks and which with an unrecorded timestamp type.
Absent means not recorded. A refused run (`exitReason:
PointInTimeByProducerTime`) signs nothing and carries none.

**`spec.point.timeBasis` lets a rehearsal of a `LogAppendTime` topic run
(FX-8).** Every slot's plan states `restore.point_in_time`, and the archive
holds each record's PRODUCER timestamp, so over a source topic recorded as
`LogAppendTime` — the archive manifest's topic override, or the bound receipt's
effective value, a broker default included — the runner refuses the slot with
`PointInTimeByProducerTime`, exit 3, before any target topic exists. The
schedule then records `lastFailed.reason: PointInTimeByProducerTime` and sets
`RehearsalHealthy=False`; it is never a silent failure. A schedule that states
`spec.point.timeBasis: producerTime` renders `restore.time_basis: producerTime`
into every slot's plan, exactly as the chosen point becomes
`restore.point_in_time`: the slot runs, by the producers' clocks, and its
signed scorecard lists the topic under `source.time_basis.producer_time`
([the plan field](formats/drill-spec.md#restoretime_basis-fx-8)). The field has
one value, is sealed like the rest of the spec, and is inside
`templateDigest`, so a schedule that states it needs an authorization signed
for that digest; an authorization signed without it refuses every slot as
`AuthorizationInvalid`. A schedule that states none serialises none, so every
existing schedule's digest and authorization are unchanged. To opt an
existing schedule in, create a new `RehearsalSchedule` with the field and sign
a new authorization for its `status.templateDigest`.

**`spec.bounds.coverage: complete` makes every slot verify every record
(PROD-08.1a).** Absent (or `sampled`), each slot runs today's sampled check. With
`complete`, the slot's plan states `sample.coverage: complete` and **no**
`sample.max_partitions` (a complete verification checks every partition, and
phase 0 refuses the pair; `maxPartitions` still bounds which point a slot may
select), plus `sample.complete_max_records` when `spec.bounds.completeMaxRecords`
is set (CEL: only beside `complete`), and the child `Restore` declares the same
on `spec.coverage`/`spec.completeMaxRecords`. **It costs more**: every slot
reads every archived record of the restored topics and the whole restored
output — about a minute per GiB of one-KiB records with an optimised build on a
laptop, against about five seconds for the sampled check
([the decision record](to-do/decisions/PROD-08.1-integrity-contract.md) §7) — so
size `deadlineSeconds` and the cron for it. A slot whose bound stops it, or
whose archive it cannot compare, signs `covered: false`: the run exits 2,
`fail-integrity`, the `Restore`'s badge is `CompleteNotCovered`, and the
schedule records `lastFailed` — never `lastSucceeded`.

**The coverage is signed twice.** The field is inside `templateDigest` (the
spec is sealed, so flipping sampled↔complete after signing changes the digest
and every slot is `AuthorizationInvalid`), **and** the standing authorization's
scope must itself say `coverage: complete` (document format 1.1.0, below):
`plan_within_scope` — the controller's each-slot check and the runner's own,
over the mounted bundle — requires the plan's coverage to equal the signed one.
A scope that states no `coverage`, which is every scope signed before this
field, authorises sampled rehearsals only, so a complete plan under it is
refused by name; a scope that says `complete` refuses a sampled plan too. A
scope that says `complete` signs `maxPartitions: 0`, and every reader of this
build refuses one with any other value: a runner or controller older than
format 1.1.0 ignores `coverage`, and under a partition bound of 0 it runs
nothing
([stability.md](stability.md#the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature)). When
the scope states `completeMaxRecords`, the plan's bound must be present and no
larger. The runner cannot see `templateDigest`; it can see the signed scope, so
an approver who signed sampled rehearsals never finds a complete one run in
their name, whatever a controller renders.

**A slot that came due before the `RehearsalSchedule` was created is not its
slot.** The controller never rehearses a slot whose due time is before the
object's `metadata.creationTimestamp`, even inside
`spec.bounds.startingDeadlineSeconds`; the pass is idle, and the slot is neither
named in `status.lastSkipped` nor written to `status.lastScheduledSlot`. The
first rehearsal is the first slot at or after creation, the same rule as a
`BackupSchedule` and a Kubernetes CronJob.

**A skipped slot is skipped, never deferred.** Whatever the reason — every row
above, and a namespace whose trust cannot verify any approver
(`AuthorizationInvalid`) — the skip names the DUE slot it refused in
`status.lastSkipped.slot` (not the instant the controller looked) and advances
`status.lastScheduledSlot` to that slot in the same resourceVersion-checked
status write. The slot is therefore decided: when the blocker clears inside
`startingDeadlineSeconds` — last week's rehearsal finishes, the target becomes
reachable, a Backup lands, the approval is re-verified — **that slot is not run
late**; the next rehearsal is the next slot. A rehearsal measures recovery AT a
cadence, and one fired forty minutes late because its predecessor overran would
record an RTO for a slot that never happened. D3 names each of these reasons as
a skip of the slot; for `TargetBusy` and `PointRetentionInProgress`, where it
names the skip but not the late-fire question, the same conservative rule
applies. A pass inside a slot already decided writes neither field — the `Ready`
message still carries the current refusal — so `lastSkipped` stays the record
of the last slot actually refused. Upgrading from a build that deferred: a slot
that build left undecided is decided by the first pass of this one.

A succeeded destination-backed `Backup` contributes its immutable named topic
list even when evidence reading was `NotAttempted`, but it is only a joinable,
non-selectable candidate until a catalog row supplies the receipt-derived
capture time, window and selectability. `metadata.creationTimestamp` is only a
sorting placeholder and never makes that candidate selectable. Because
`pointId` truncates the receipt digest, catalog enrichment additionally requires
the row's full `receiptSha256` to equal the Backup's captured digest; a colliding
prefix cannot supply facts for another receipt. The catalog decides only where
the controller could not look: when the Backup's own verification result is
`NotAttempted`, `Pending`, absent, or a `Valid` on `trust.basis: Unverified`
(nothing compared yet). A `Valid` beside no `trust` block or a
`Current`/`Historical` basis is a pass the catalog may still narrow. A verdict
the controller reached and refused — `Invalid`, `Untrusted`, a result this build
does not recognise, or a `Valid` on any other trust basis
(`RecordedBeforeRevocation`, `None`, a block with no basis) — is never made
selectable by a catalog row, which
may have been harvested before the receipt was replaced or its signer revoked.
The same holds for a catalog-only candidate — a row no `Backup` of the
schedule's `scheduleRefs` names, which is all a `catalogRef`-only schedule sees:
the controller walks every page of the namespace's `Backup`s (up to 20 pages of
500, read leniently so one object this build cannot type is simply no
candidate) for either kind of source, and a row whose receipt any listed
`Backup` refused is never selectable. A walk the bound cuts short admits no
catalog-only row at all, and the `NoQualifyingPoint` skip names the Backup
history as the reason. Of the schedule's own Backups the newest 200 by
recovery point are candidates, so a long history never hides the newest run. The catalog also decides selectability only together with a representable,
positive `recoveryPointAtMs`; a row without one leaves the candidate
non-selectable. The protection policy applies the same rule, and reads an
unrecognised verification result as `Untrusted`, never as `NotAttempted`. This
preserves D3 §4.2's `topics ⊆ point.topics` filter without upgrading the
Backup's verification or protection-health verdict.

The last row is the one that matters most, and it runs over the bytes that will
be frozen, before the reservation and before any `POST`: an out-of-scope plan
reaches no `Restore`, no `ConfigMap` and no Job. **That half is live today.** The
runner's half — proving the same thing again against the mounted bundle, through
the same predicate from the same projection, before it constructs any client —
is live too since PLAT-14.3b, and the `Restore` reconciler makes the same chain
a THIRD time at admission (see "How a rehearsal executes" below).

**The plan's target block is the saved connection's.** The controller renders
`target.bootstrap_servers` and `target.auth` (mode, username, TLS) from the
target `KafkaCluster` resolved for `RestoreTarget` — the resolution the
`Restore` admission repeats before it compares the plan with the connection, so
a rehearsal against a SCRAM or TLS target is admitted like any other restore.
Neither the password, its Secret's name nor the CA is ever in the plan; they
reach the runner through the connection's own projection. Builds before this
fix rendered `auth: {mode: plaintext}` whatever the target was, and every
rehearsal against an authenticated target ended `Failed/ConnectionPlanMismatch`
with no Job (defect REHEARSAL-PLAN-AUTH-PLAINTEXT). **Existing standing
Approvals are unaffected:** the template digest covers `RehearsalSchedule.spec`,
which names the target only by `clusterRef`, and the signed scope carries the
target cluster id and never its auth — so no authorization needs re-minting on
upgrade, and a rollback renders plaintext again (and is refused at admission
again) without touching one.

**What the bundle contains.** One immutable `ConfigMap` owned by the `Restore`:
the signed standing document at `standing-authorization.json` with its sidecar
derived at `standing-authorization.sig` (the paths the runner mounts), the
trusted public keys at `authorization-keys.json`, an allowlist holding **exactly**
the signed target cluster id, and the approver's public key — **five members,
and no per-run `approval.json` slot.** Every member is pinned by a sha256 in
the Job's immutable environment, and no private key material is ever written to
it. The sidecar keeps its own name: mounted at `approval.sig` the runner would
verify it under `PAYLOAD_TYPE_APPROVAL` and report a correctly signed rehearsal
as a substituted approval.

**The rendered prefix is unique per schedule object.**
`spec.target.topicPrefix` is rendered as `<prefix><schedule-uid-first-8>-`, for
example `rehearsal-3f2a91c7-`. Two schedules therefore can never map a source
topic to the same target name, and the runner's own prefix-scoped deletion
guard can be scoped to one run. A schedule deleted and recreated gets a new UID
and so a new prefix — correct, because it is a different object and its
authorization is a different document.

**The controller deletes no topic, ever.** Teardown is the runner's phase 9,
which deletes the exact names it created through a deleter that refuses any name
outside the prefix. Failures are read from the signed teardown attestation into
`Restore.status.teardown` and mirrored to
`RehearsalSchedule.status.cleanup.pendingTopics`; while that list is non-empty
the next slot is **skipped** with `LeftoverTopics`, because the run that would
otherwise collide is not allowed to adopt or delete topics it did not create.

**That guard covers an ATTESTED teardown failure and not a crash.** A run killed
before phase 9 — deadline, OOM, node loss, an evicted pod — writes no teardown
attestation at all, so `Restore.status.teardown` is absent, `pendingTopics` stays
empty and the next slot fires under the same per-schedule prefix over whatever
the dead run left behind. What happens then is the runner's phase 0 question (it
refuses a mapped name that already exists), not this controller's, and the
controller will not have told you why. If a rehearsal disappears without a
terminal status, list the target's `rehearsal-<uid8>-` topics before the next
slot is due. Making the prefix per-slot rather than per-schedule, or treating a
vanished non-terminal child as `LeftoverTopics`, would close it; neither has
landed.
Clear them with your own Kafka tooling and then clear the status field:

```
kubectl --context <ctx> -n <ns> get rehearsalschedule weekly-orders \
  -o jsonpath='{.status.cleanup.pendingTopics}'
# delete those exact topics with kafka-topics.sh, then:
kubectl --context <ctx> -n <ns> patch rehearsalschedule weekly-orders \
  --subresource=status --type=merge -p '{"status":{"cleanup":null}}'
```

**What the status says, and who reads it.** `lastSucceeded` carries the
`Restore`, the instant, the evidence key and the measured RTO; `lastFailed`
carries why the run did not pass; `lastSkipped` carries the slot and one of
the reasons above.

**A rehearsal is recorded on its REACHED evidence verdict, not when its
`Restore` turns terminal** (REHEARSAL-PASS-RECORDED-AS-FAILED). A
destination-backed `Restore` writes its exit code and evidence keys first and
its `outcome` and `evidence.verification` later, from the evidence-fetch Job.
While that verdict is owed — the keys are named and no verdict is written yet,
the verdict is `Pending`, or it is `NotAttempted` with a retry scheduled
(`observation.retryAfter`) — the schedule records nothing, keeps
`activeRestoreRef`, leaves `RehearsalHealthy` as it was, and a slot that comes
due meanwhile is skipped `ConcurrencyBlocked` (firing would lose the result
being waited for). Once the verdict is reached the run is recorded once:

- `lastSucceeded` only for exit `0`, `outcome: pass` **and** a green verdict
  (the `Restore` badge rule of §15.2 — `Valid` on the `Current` or
  `Historical` basis);
- otherwise `lastFailed`, whose `reason` is the verified signed `outcome`
  (`fail-integrity`, …) or the exit's own reason for a run that did not exit 0,
  and the badge's reason (`VerificationInvalid`, `VerificationUntrusted`,
  `VerificationNotAttempted` once the fetch's attempts are spent) for one that
  did;
- a verdict still owed an hour after the run finished (`VERDICT_WAIT_SECONDS`,
  longer than the whole fetch schedule's 35 minutes; the margin is for the
  per-namespace evidence-fetch slot a Job may queue behind and per-step requeue
  latency, and a namespace whose fetch slot stays saturated past it records a
  genuine pass as a failure, never a failure as a pass) is `lastFailed` with
  reason `EvidenceVerdictNotReached` — never a pass;
- a run that named its evidence and has **no** verification block at all five
  minutes after it finished (`UNRECORDED_VERDICT_GRACE_SECONDS`) is decided the
  same way: on the controller's own read handle that shape is permanent (a
  scorecard read that failed leaves no digest and so no verdict), and on the
  fetch path it lasts one reconcile;
- a rehearsal `Restore` deleted while `activeRestoreRef` still names it is
  `lastFailed` with reason `RestoreDeleted`, and the ref is released. The same
  record is written for a reservation (`pendingRestoreRef` with no matching
  `activeRestoreRef`) whose `Restore` no longer exists, or was never created.

**The reservation protocol, and what a lost write leaves.** A slot that fires
writes three things in order: `pendingRestoreRef` and `lastScheduledSlot` under
a resourceVersion-checked status patch, then the deterministic
`logweir-rehearsal-<schedule>-<slot>` `Restore`, then the pass's commit
(`activeRestoreRef`, `pendingRestoreRef: null`, the verdict and the three
conditions). The commit is preconditioned on the version the reservation left,
so it lands. If another writer changes the schedule in between, the commit is
refused and nothing of it is stored. The next pass then reads the reserved name
and adopts that `Restore` as `activeRestoreRef` only when it is this schedule's
own: controlled by the schedule's UID and standing-authorised by its name. A
`Restore` of that name owned by anything else is never adopted. The reservation
is released, and a create that meets such an object is recorded as a
`ConcurrencyBlocked` skip for its slot, with no bundle written.

One residual remains, and it is bounded. The controller runs as one replica
with the `Recreate` strategy, so two controllers overlap only when a pod is
force-deleted or partitioned during a rollout. If that happens, one of them can
read a reservation in the moment between the other's reservation write and its
create. It then records `RestoreDeleted` for a child that is created a moment
later, and that run is not tracked. The error fails loud:
`RehearsalHealthy=False`, never a false pass. A controller that stops between
its reservation and its create leaves the same record, and there it is correct,
because that child never existed.

`RehearsalHealthy` moves with the same decision (`True/Passed`, or
`False/Failed` naming the reason). *Upgrade:* nothing to migrate. Builds before REHEARSAL-FIRE-PASS-STATUS-LOST's
fix never stored a firing pass's commit: that write was preconditioned on the
version the reservation had already moved, and was refused. So a schedule
whose last-fired run is not yet recorded carries `pendingRestoreRef` and no
`activeRestoreRef`, and `Authorized` still reads `Unknown/NoResult` after
rehearsals fired. On upgrade the first pass adopts that pending `Restore`, or
records it `RestoreDeleted` if it is gone. `Authorized` is evaluated on the next
slot that fires. Rolling back leaves an older build reading `activeRestoreRef`
first and `pendingRestoreRef` second, as it always did. A schedule
whose `activeRestoreRef` names a finished run is simply re-read; a run an older
controller already recorded (including a pass it recorded as `lastFailed`
reason `ok`) is not revisited — the next rehearsal records correctly.
*Rollback:* an older controller records the terminal instant again, so a
destination-backed pass reads as failed until upgraded. The three conditions are `Ready` (this controller could act),
`Authorized` (the standing document currently admits a slot: `True` from a
pass that fired, `False` naming the refusal from a pass that refused the
authorization, and carried unchanged by a pass that did not evaluate it, such
as a concurrency skip) and `RehearsalHealthy` (the last finished rehearsal
passed). A `ProtectionPolicy`
reads `lastSucceeded.{at,restoreRef}` and `lastFailed.{at,reason}` — the four
fields its `RehearsalFailure` alert is computed from — and reads nothing else
here.

**Evidence is never deleted.** Rehearsals write ordinary scorecards, sidecars,
offset reports and teardown attestations under `logweir/drills/`. Their
`Restore` objects are owned by the schedule with `blockOwnerDeletion: false`, so
deleting the schedule collects the CRs and never the signed evidence, and a
rehearsal in flight never blocks the delete.

**RBAC.** This kind adds three rules to the `weirkeeper` ClusterRole:
`get`/`list`/`watch` on `rehearsalschedules` (the `get` has a caller — the
`Approval` reconciler reads the referent whose sealed spec the digest is
recomputed from), `patch` on `rehearsalschedules/status`, and `create` on
`restores`. No `delete` anywhere, no `update` on any kind, and no verb on
`secrets`: the envelope, the sidecar and the approver's PUBLIC key all come from
an `Approval` and from the namespace's resolved `TrustPolicy`.

**Upgrade and rollback.** The kind is additive and off by default: an
installation that never creates a `RehearsalSchedule` behaves exactly as before.
`Restore.spec.approvalRef` became optional in this group, and a
standing-authorized `Restore` carries `spec.authorization` instead; an OLDER
controller reading one sees an empty `approvalRef` and refuses terminally with
`ApprovalNotReceived` — fail closed, which is the required rollback behaviour.
Rolling the CRDs back deletes any `RehearsalSchedule` objects and, by owner
cascade, their `Restore` CRs and approval bundles; no archive object and no
signed evidence is affected.

**How a rehearsal executes.** The schedule fires, selects a point, proves the
plan is inside the signed scope, reserves, creates the `Restore` and writes its
bundle; the `Restore` reconciler then admits it and creates the Job. The
standing document REPLACES the per-run approval rather than sitting beside it
(PLAT-14.3b) — a per-run approval binds `sha256(plan bytes)` and only a human
with a signing key can produce one, while the controller links no signer at all
(Global Constraint 27), so requiring both is requiring a rehearsal never to run.

*The runner.* `logweir restore run` takes `--standing-authorization` and
`--authorization-keys` and no `--approval`. It verifies the DSSE signature over
the envelope's exact bytes under a key the bundle pins, judges THAT key's usage
(`GovernedApproval` only: the standing format carries no approval-policy
mode, so a `ConsoleConfirmation` key authorises no rehearsal, and
`EvidenceSigning` never authorizes), admits the document (kind, subject, the UID binding to this schedule,
and a validity window capped at ninety days), and proves `plan ∈ scope` — all
before any client is constructed. Each failure is refused by name with exit 3
and `no data operation was started`. `--approval` is REQUIRED for every other
shape and refused under `AUTHORIZATION_KIND=standing`; the contract's
`LOGWEIR_EXECUTION_APPROVAL_SHA256` / `…_SIDECAR_SHA256` are likewise required
under `approval` and refused under `standing`, so the approval slot's presence
cannot disagree with the authorization kind in either direction.

*`--triggered-by`.* A rehearsal's reason is a slot, not an approval a human
clicked for this run, so the value is `rehearsal/<schedule>/<slot>` and it is
copied verbatim into the signed scorecard. The runner checks its SHAPE before
anything is parsed and binds the `<schedule>` segment to the signed document's
own `subjectRef.name` once the signature verifies — a stronger binding than an
equality against an environment variable the controller set. The scorecard's
`approval.approver` reads `standing-authorization/<schedule>` and never a
person's name: what a human signed was a schedule.

*The controller.* `admit` dispatches on the object's own `spec.authorization` —
never on what `Approval` happens to exist — so a standing document cannot admit
an ordinary `Restore`, and an ordinary `Restore` still needs its own verified
`Approval`. For a rehearsal it re-makes the schedule's chain: the `Approval` is
`Verified=True` and is a `RehearsalSchedule` approval bound to the schedule the
spec names, the key it verified under is one this namespace's trust still lets
authorise with an approver's usage, the signed bytes are admissible against the
UID the `Approval` controller recorded, and `plan ∈ scope`. Running it again
here is not redundancy: a key can be withdrawn between the slot firing and this
reconcile, and a `Restore` carrying `spec.authorization` can be created by
anything with RBAC on the kind. A refusal is terminal with reason
**`StandingAuthorizationRefused`** — its own reason, because an operator told
`ApprovalSubjectMismatch` goes looking at a subject binding that is correct —
and creates no Job. An absent or unverified `Approval` stays a thirty-second
hold. This is the one kind for which trust is resolved BEFORE admission; the
ordinary path still resolves it only when a Job is going to exist.

*Minting the document.* `logweir drill approve --standing` signs it — the same
signer as a per-run approval, under the standing payload type:

```
logweir drill approve --standing \
  --key approver-private.pem \
  --schedule-namespace team-a --schedule-name weekly-orders \
  --schedule-uid $(kubectl --context <ctx> -n team-a get rehearsalschedule weekly-orders \
                     -o jsonpath='{.metadata.uid}') \
  --scope scope.json --valid-days 30 \
  --out standing-authorization.json
```

`scope.json` is D3 §4.3's scope in camelCase — `templateDigest`,
`targetClusterId`, `topicPrefix`, `topics`, `maxPartitions`,
`recordsPerPartition`, `deadlineSeconds`, `modes` — and, for a schedule whose
`spec.bounds.coverage` is `complete`, **`coverage: complete`** with
**`maxPartitions: 0`**, and optionally **`completeMaxRecords`** (PROD-08.1a): a
scope carrying either new field is minted at `formatVersion` 1.1.0, every other
one at 1.0.0 as before. A scope that states no `coverage` authorises sampled
rehearsals only; `completeMaxRecords` without `coverage: complete`, or of 0, and
`coverage: complete` beside any `maxPartitions` but 0, are refused before
anything is signed. The 0 is what makes a runner or controller older than
format 1.1.0 refuse every plan under the document instead of reading it as a
sampled scope; the schedule's own `spec.bounds.maxPartitions` still bounds the
point a slot selects, inside the signed `templateDigest`.
`--spec`, `--approver` and `--ticket` are refused: this document binds a SCOPE
and covers every slot, and its versions carry neither an approver nor a
ticket, so a value given for them would not be signed. The two files become the
`Approval`'s `spec.approvalBytes` and `spec.sidecarBytes`, with
`spec.subjectRef.kind: RehearsalSchedule` and `spec.planHash` set to the
schedule's `templateDigest`. The signing key's PUBLIC half must be on this
namespace's trust carrying `GovernedApproval`. `ConsoleConfirmation` is not
accepted by the standing format merely because such a key exists, and
`EvidenceSigning` never authorizes (D3 §7.3). The command refuses before signing anything the
cluster would refuse afterwards: the ninety-day cap, a blank schedule UID, a
mode this build does not implement, and every scope bound whose absence the
runner treats as a mismatch.

*Which `Approval` object, and not merely which name.*
`spec.authorization.approvalRef` is a `LocalRef` and carries a name only, so
the `Restore` reconciler resolves the standing `Approval` **by name**. To stop
a different object that later took that name being used, the schedule stamps
the UID of the `Approval` the slot was actually authorised against onto the
child `Restore` as `logweir.dev/approval-uid`, and admission requires the
resolved object to carry it — an `Approval` deleted and recreated under the
same name is refused. A standing `Restore` with no such annotation is refused
too: every one this build creates carries it, and one that predates it never
executed.

**The pin is metadata, and metadata is not frozen by the API server.**
`Restore.spec` is CEL-immutable; an annotation is not, so "the `Approval`
object a slot was authorised against cannot be re-pointed" rests on RBAC — no
shipped human role has `patch` or `update` on `restores` — rather than on
admission, unlike every other authorization input beside it in `spec`. Once the
bundle exists the window closes anyway: the bundle is immutable and carries the
resolved `Approval`'s UID, so any change is a terminal `ApprovalBundleConflict`.
The real fix is a `uid` field on `approvalRef` so the reference carries the
identity it means; that is a CRD change queued as a follow-up.

*On the schedule.* A rehearsal that fails — verification, preflight, or its
authorization — is recorded as the failure it is on `RehearsalHealthy` and
D3 §4's `rehearsalLast*`, with the `Restore`'s own terminal reason in the
message. The `StandingAuthorizationNotAdmitted` hold PLAT-14.3 carried while
this was unwired is gone. Owned-topic cleanup is unchanged: the controller
deletes no topic, teardown is the runner's phase 9 inside the prefix guard, and
an unrelated topic on the target is never touched.

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

**A `KeyCompromise` revocation applies to every namespace, and it outlives the
policy that recorded it** (TRUSTPOLICY-DELETE-DROPS-REVOCATION). Resolution
picks the answering source by the rules above, and then every key it lists
that ANY `TrustPolicy` in the cluster records as `Revoked` with
`revocationReason: KeyCompromise` — a policy with a `deletionTimestamp`
included — is revoked for compromise in the answer too, with the earliest
recorded `revocationEffectiveFrom`. That covers a namespace removed from the
recording policy's `spec.namespaces` (it falls to the roster, which cannot
express a revocation), a second policy that still lists the key `Active`, and
the `default: true` policy. Only a compromise is carried; a supersession stays
the recording policy's own. **The event that records it re-evaluates every
namespace.** When a policy's compromise records change (added, escalated from a
supersession, edited away by hand), when a policy carrying one is first seen, or
when one is being deleted, the policy watch of the `Backup`, `Restore`,
`Approval` and `RecoveryCatalog` reconcilers enqueues every object they hold,
not only the policy's own namespaces. The `TrustPolicy` reconciler also
re-evaluates every other policy. A terminal `Restore` in a namespace on the
roster therefore turns `RecordedBeforeRevocation` on that event, without
waiting for a restart. A status heartbeat of an unchanged record fans out
nothing. A refusal caused this way names the recording
policy ("recorded by TrustPolicy/…"), and each policy's `status.keys[]`
reports such a key `Revoked` even where its own spec says `Active`.

The record itself is kept by a finalizer. The controller places
`logweir.dev/compromise-revocation` on every policy that records a compromise,
and a deletion is released only when another policy without a
`deletionTimestamp` records the same revocation, or nothing in the cluster — no
other policy, not `TrustRoster/default` — lists the key. Until then `kubectl
delete` leaves the policy `Terminating`, still resolved through by this build
and by an older one after a rollback, and its `CompromiseGuard` condition reads
`DeletionBlocked` with every source still listing the key. The other readings
are `CompromiseRecorded`, `CompromiseInherited` (this policy lists a key
another policy revoked for compromise) and `NoCompromiseRecorded`. A roster
read that fails releases nothing. CEL rule G9 keeps a revoked key's
`KeyCompromise` reason from being edited to anything else. The replacement
procedure, the roster-only rollback and the residuals are in
[`keys.md`](keys.md), *A compromise revocation outlives the policy that
recorded it* and *Replacing a `TrustPolicy` safely*.


### The checks

Each `Approval` event resolves the roster and fetches the referent named by
`spec.subjectRef`. A Restore approval reads `spec.planBytes` and runs the
per-run checks below **in this order**. The order is load-bearing: the DSSE
verifier refuses a `payloadType`
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

A RehearsalSchedule approval shares checks 1–6, then parses the verified bytes
as `StandingAuthorization`. The allowed usage set for this document kind is
explicitly `GovernedApproval` only, whatever the namespace's approval policy,
and `EvidenceSigning` never authorizes. The per-run table above is the
`legacy-governed-v1` path; a namespace bound to an approval policy verifies
authorization document v2 instead ("Approval policy" below).
Its closed standing arm checks the exact canonical template digest and unsigned
`spec.planHash`, the full subject
identity including UID, the live at-most-90-day window, and the complete
scratch-only scope. The corresponding reasons are
`TemplateDigestMismatch`, `SubjectMismatch`, `WindowInvalid`, `ScopeInvalid`
and `StandingDocumentInvalid`; no standing failure is reported as the per-run
`PlanHashMismatch`. `Verified` remains valid only until the earlier of the
document's `expiresAt` and the matched key's `notAfter`, and reconciliation is
requeued at that boundary.

Two more `reason`s reach the same `Verified` condition without being checks on
a signature at all. They are properties of the **referent** — the object
`spec.subjectRef` points at — and are kept in their own vocabulary because
"your cluster is missing an object" is not a verdict about anybody's approval:

| — | Referent problem | `reason` |
|---|---|---|
| — | `spec.subjectRef` names an object that does not exist in this namespace | `ReferentNotFound` |
| — | The referent exists and its KIND carries no `spec.planBytes` for check 7 to recompute a hash from — in tag 1 that is `subjectRef.kind: Backup` | `ReferentHasNoPlanBytes` |

**So `Verified`'s `reason` is one of NINETEEN strings, and this is the one place
all nineteen are named**: `PayloadTypeMismatch`, `SignatureInvalid`,
`KeyIdNotInRoster`, `KeyIdExpired`, `KeyRetired`, `KeyRevoked`,
`KeyNotYetValid`, `TrustPolicyConflict`, `PlanHashMismatch`,
`SubjectKindMismatch`, `RosterNotFound` (install step 1, above),
`ReferentNotFound`, `ReferentHasNoPlanBytes`, `ReferentUidChanged`,
`TemplateDigestMismatch`, `SubjectMismatch`, `WindowInvalid`, `ScopeInvalid`
and `StandingDocumentInvalid`. A twentieth would be a compile error rather than a surprise: each reason is the
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
`selfAttestedRisk`, `approverKeyWindow`, and a `Verified` condition. On a
refusal: `verified: false` and a `Verified` condition whose `reason` is the name
from the table above and whose `message` says what was compared — and **no**
`approver`, **no** `matchedKeyId` and **no** `approverKeyWindow`, because a name
lifted out of bytes whose signature nobody authorised is an attacker-controlled
string on a field a UI renders. Those five — the three above plus `ticket` and
`selfAttestedRisk` — are sent as an explicit `null` on a refusal, not merely
left out: a JSON merge patch that omits a key leaves the old value, so an
`Approval` verified at 09:00 and refused at 09:05 used to keep all of them
(fixed 2026-09-21 with `approverKeyWindow`'s arrival, which made it load-bearing
rather than untidy).

`selfAttestedRisk` is `true` when the matched approver key id also appears in
`spec.signingKeys[].keyId`. It is **labelled, never refused**: `false` means
only "two different key ids", and one operator holding both keys satisfies it.

**`approverKeyWindow` is the matched approver key's declared validity window** —
`keyId`, `notBefore`, `notAfter`, as the `TrustPolicy` that governs the
namespace declares them (§19), re-derived on exactly the events that re-derive
the `Verified` condition, because it comes out of the same decision and is
written in the same preconditioned patch:

```bash
kubectl --context docker-desktop get approval a1 \
  -o jsonpath='{.status.approverKeyWindow.notAfter}'
```

It exists for a reader that cannot resolve the key itself. The restore
preflight's `approval.keyValidity` row is that reader (§21.6a): it compares the
restore's deadline against `notAfter` and caps both approval rows' re-check at
`min(10 m, notAfter)`. **It is not a second verdict** — whether the key may
authorise anything *now* is the `Verified` condition's answer, and a retired or
revoked key has an open window and authorises nothing.

**Absent means unknown, never valid.** No key matched, so there is no window;
a reader that finds none must say so rather than assume the key is good. A
controller image that predates this field publishes none either, which is the
same answer for the length of an upgrade.

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

### Approval policy: ordinary confirmation and governed approval (PLAT-19.2)

Everything above describes **`legacy-governed-v1`**, which is what every
namespace resolves to until the installation binds it to an approval policy:
a v1 approval document, signed out of band by a `GovernedApproval` key, is the
only thing that authorises a `Restore`. PLAT-19.2 (decision D0, "Ordinary
versus governed approval contract") adds two explicit modes for **per-run
`Restore`s**, chosen per namespace by the installation and frozen into every
run's execution inputs.

**The installation document.** One YAML document declares named policies and
binds namespaces to them. The chart renders it from `approvalPolicy.*` into an
immutable, content-addressed ConfigMap `<release>-approval-policy-<digest>` and
mounts the **same** object into the controller (`LOGWEIR_APPROVAL_POLICY_FILE`)
and into the console (`approvalPolicyFile`), so both consume the same bytes and
log the same `approval_policy_digest` at start:

```yaml
allowOrdinaryConfirmation: true          # D0's floor; default false
policies:
  - name: team-ordinary
    mode: Ordinary                       # the console's confirmation is the authorization
    maxAgeSeconds: 900                   # 60..604800; default 900 (Ordinary), 86400 (Governed)
  - name: prod-governed
    mode: Governed                       # console confirmation + an independent approver
    requireDistinctPrincipal: true       # must be true for Governed; false is refused
namespaces:
  team-a: team-ordinary
  prod: prod-governed                    # an unbound namespace stays legacy-governed-v1
```

The controller and the console **refuse to start** on a document that does not
validate — an unknown field, an `Ordinary` policy without
`allowOrdinaryConfirmation: true`, a Governed policy with
`requireDistinctPrincipal: false`, a binding to an undeclared policy, the
reserved names `legacy-governed-v1` and `default-confirm-v1` — rather than run
every namespace as legacy and silently accept v1 approvals where the
administrator bound Governed. An absent document on an install that was not
marked fresh is the legacy installation. Nothing synthesises `Ordinary` except
the fresh-install default below, which an upgrade can never reach.

### The three modes, and a fresh install's default (PROD-16.1)

The operator sees three modes; the internal names stay, because they are
inside signed documents and snapshots (`logweir_core::approval_policy::OperatorMode`
is the one mapping, `logweir.approvalPolicy.internalMode` the chart's half):

| mode | internal | who approves a `Restore` |
|---|---|---|
| **confirm** | `Ordinary` (v2) | the requester, one click in the console; no key handled by anyone |
| **two-person** | PROD-16.2, not in this build | a second person, one click in the console |
| **strict** | `Governed` (v2) or `legacy-governed-v1` (v1) | an approver's personal `GovernedApproval` key |

`approvalPolicy.policies[].mode` may be written `confirm` or `strict` (the
chart renders `Ordinary`/`Governed`, so an image-only rollback reads the same
document); `two-person` is refused by name.

**An unbound namespace** resolves, in this order: an explicit
`approvalPolicy.default` (`defaultMode` in the document) — `confirm` (which
needs `allowOrdinaryConfirmation: true`, D0's floor) or `strict`; else the
**fresh-install marker**, which makes it `confirm`; else `legacy-governed-v1`.
Under `confirm` an unbound namespace resolves to the concrete policy
`default-confirm-v1` (`Ordinary`, 900 s), checked at every boundary below
exactly like an explicit binding; an explicit binding always wins.

**The fresh-install marker, and why an upgrade never reaches it.** The identity
hook (`logweir identity bootstrap`) writes it only in the ONE run that
**generates** the installation identity and the console key — a first `helm
install` with a console, the managed identity and a bootstrap image that runs
the PROD-16.1 flags (`identity.bootstrapFeatures.consoleKey`). In that run it
first creates the installation's default `TrustPolicy` (below), then writes, on the public identity ConfigMap
(`logweir-signing-trust`), the annotation
`logweir.dev/approval-default: confirm;policy=<name>;uid=<uid>;signing=<keyId>;console=<keyId>`,
and annotates the policy `logweir.dev/approval-default: confirm` beside its
provenance `logweir.dev/created-by: identity-bootstrap`. The console (per
request) and the controller (every 15 s) read both objects and reach ONE
verdict, `logweir_core::approval_policy::verify_marker` through
`weirkeeper::approval_policy::marker_verdict`: the claim is honoured only when
the policy it names exists with that exact UID, the hook's provenance and the
confirm annotation, is still `default: true`, and declares exactly the claimed
installation signer (`EvidenceSigning`, principal `install:<release ns>/…`)
and console key (`ConsoleConfirmation`, principal `console:<release ns>/…`).
Anything else — no annotation, a bare `confirm`, a claim naming another
identity, a missing, re-created or hand-written policy, one no longer default,
one without the console key — is **unmarked**, logged as a WARN by both, and
the namespace stays `legacy-governed-v1`. An upgraded install's identity
already existed, so the hook neither creates the trust nor writes the claim;
an adopted external identity may predate this chart and is never marked; a
console key the hook ADOPTED (a hand-made Secret of the managed name present
before the first install) is never trusted automatically, so that install is
not marked either; a first install whose hook run was interrupted after
generating the identity is not finished by a later run and starts strict.

**The threat model, and the residual** (the PROD-16.1 security review).
Before PROD-16.1, turning a governed namespace into one-person confirmation
took two gates: an approval-policy rollout (installation administrator) and a
trust administrator trusting the console key. On an install that existed
before, that is still so: patching the marker into the ConfigMap changes
nothing (no hook-made policy carries it), and making one requires
`TrustPolicy` create — the trust administrator's gate. The explicit opt-in of
such an install is the same two gates (below). **On a fresh install**,
whoever can change the hook-made `TrustPolicy` (a trust administrator) or the
approval-policy document (an installation administrator) can change approval,
as before; and in any console mode, whoever controls the console pod, its key
Secret or the identity provider can approve alone, which `Ordinary` always
accepted (SECURITY.md).

**A pre-existing bound on both attackers** (the PROD-16.1 review's attacker
model; not changed by PROD-16.1, and owed to the trust/RBAC owner). The
controller's ServiceAccount holds cluster-wide `patch` on `trustpolicies` for
the compromise finalizer (`charts/logweir/templates/clusterrole.yaml`,
`controller-scope.yaml`; RBAC cannot narrow a finalizer patch below the
object), and no admission policy fences that patch to `metadata.finalizers`. So
whoever can run a pod as `weirkeeper` in the release namespace can append a key
to ANY EXISTING `TrustPolicy` — a trust administrator's power wherever a policy
exists. On a fresh install with a console that is from the first minute, because
`logweir-installation` exists; before PROD-16.1 it waited for the first policy.
A ValidatingAdmissionPolicy letting that account change only
`metadata.finalizers` on `trustpolicies` would close it.

**The install-only trust grant.** Creating that one `TrustPolicy` needs
`create` on the cluster-scoped kind, which RBAC cannot narrow by name. The
chart therefore renders the grant (`ClusterRole`/`ClusterRoleBinding`
`<release>-identity-trust`: `list`+`create` TrustPolicy, `get`
`TrustRoster/default`, `delete` its own binding) ONLY under one guard — a first
install (`.Release.IsInstall`) into a namespace with no established signing
Secret, with the managed console key (a console, the bootstrap feature, no
`identity.externalSecret`: an install that never uses confirm gets no grant) —
and ONLY as a `post-install` hook (never upgrade or rollback: a rollback replays
the stored revision's hooks, and this one is not a rollback hook), weighted
before the Job, with `hook-delete-policy:
before-hook-creation,hook-succeeded,hook-failed`.

*What removes it, and on which Helm.* On success, every Helm deletes it when
the install's post-install hooks have run. When the identity Job FAILS, Helm
3.19.0 and 4.x (`pkg/action/hooks.go` `execHook`,
`deleteHooksByPolicy(executingHooks[0:i], HookSucceeded, …)`; v4.0.1 sha256
`ebb121ed85cc27f7c2e249f49dede260e4b9aefa8b61606fd0844d39d600c58d`) also delete
the earlier `hook-succeeded` hooks — this grant — but Helm 3.12–3.18 delete only
the failed Job and would leave the grant bound. **The chart therefore refuses
to render the grant under Helm before 3.19** (`identity.installationTrust
needs Helm 3.19.0 or newer`; install with a newer Helm, or set
`identity.installationTrust.enabled=false` and trust the console key
yourself). The hook ALSO deletes the binding itself on every exit path it
controls — after the trust step, after a failed step, after a store it could
not open, after a panic, after a failed delete (on a fresh connection), and
after a usage error such as a flag from a newer chart — and every later run
deletes a binding still left. What neither covers is a hook that never runs
its code (an image that cannot be pulled, a pod never admitted, a SIGKILL), a
failed `Create` of a later hook, or a Helm client killed mid-install: then the
binding outlives the install until the next hook run, and the failed-install
recovery in `docs/install.md` §5f deletes it
(`kubectl delete clusterrolebinding,clusterrole <release>-identity-trust`).
`helm uninstall` does not delete hook objects.

**The window:** from the post-install phase to the end of the trust step,
whoever can run a pod as `<release>-identity-bootstrap` in the release
namespace could create a TrustPolicy; the installer holds cluster-admin for
that install anyway (CRDs, ClusterRoles). The hook itself creates only a policy
of exactly the signer and the console key it GENERATED, `default: true`, and
only when the cluster has **no TrustPolicy at all** (default or namespaced) and
no `TrustRoster/default` — a default policy beside administered namespaced
trust would turn every other unbound namespace into confirm.

**The console key at install.** With a console and the managed identity, the
hook generates the console's `ConsoleConfirmation` key (Ed25519) once into the
retained Secret `logweir-console-confirmation` and publishes its public half in
`logweir-console-trust` — the installation identity's lifecycle: never
regenerated, a published half without its private key stops the hook (key
loss), a hand-made Secret of that name (every PLAT-19.2 install) is adopted —
and, adopted, trusted by nobody automatically. The console mounts it `optional`
and reads it on first use (`confirmationKeyManaged: true` in its
configuration), because the hook runs after the console starts; until then a
`confirm` request is refused before anything is created. An operator-named key
file that is missing is still a refusal to start.

**Why installation configuration and not a namespaced object.** D0 puts the
binding and the `allowOrdinaryConfirmation` floor in installation
configuration and says "selecting a different policy is an explicit
installation-admin rollout and audit event, not a namespace operator edit". A
policy object a namespace could create would be a namespace naming its own
authority, which §7.1 forbids for trust. Immutability comes from the content
address: every signed document names the policy's **snapshot digest**, so any
edit to a policy is a different policy to every outstanding document. Changing
the document renames the ConfigMap and rolls both Deployments.

**Authorization document v2.** Both modes produce the same document,
`application/vnd.logweir.restore-authorization+json;version=2.0.0`, signed by
the console with its **own** `ConsoleConfirmation` key after the Restore exists:

```json
{"formatVersion":"2.0.0","kind":"RestoreAuthorization","authorizationMode":"Ordinary",
 "subject":{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore","namespace":"team-a",
            "name":"rst-…","uid":"…"},
 "planHash":"sha256:…","requester":{"issuer":"…","subject":"…"},
 "policy":{"name":"team-ordinary","digest":"sha256:…"},
 "issuedAt":"…","expiresAt":"…","ticket":"CHG-4711"}
```

`ticket` is the change ticket the requester gives when submitting (the create
request's `ticket`). D0: **required under `Governed`** — a governed document
without one is refused `AuthorizationDocumentInvalid` by the controller, at
admission and by the runner, and the console refuses the request before
anything exists — and optional under `Ordinary`. At most 128 printable
characters.

**`confirm` in the administrator console (PROD-16.1, amending D0's "does not
expose Ordinary").** The `localAdmin` console confirms too: the confirming
principal is `urn:logweir:local-admin#admin`. **Residual:** whoever can reach
that console — the Kubernetes permission to port-forward to
`deploy/<release>-api`, which is already full console administrator authority
— can confirm a restore alone; SECURITY.md says so. `two-person` (PROD-16.2)
is never served there: its one identity cannot be two people. A `strict`
namespace works in both modes: the console only attests the requester, and an
independent approver key decides. `GET .../approval-policy` answers
`ordinaryConfirmationAvailable: false` only while the console's key is not
there yet.

The console's signature attests **which authenticated principal asked** and
nothing else. Under `Ordinary` it is the whole authorization and the console
stores the document as the `Approval` the Restore's `spec.approvalRef` names.
Under `Governed` it is stored as `<approvalRef>-confirmation`, which authorises
nothing; an approver runs `logweir drill countersign` over the same bytes on
their own machine and submits the result (`POST
/api/v1/namespaces/{ns}/restores/{name}/approval`, Approver role), and only
then does the referenced `Approval` exist, carrying the console's signature
and the approver's. Unknown fields in the document are refused.

**Where it is enforced — four places, and the controller is the final gate.**

| Boundary | What it checks | Refusal |
|---|---|---|
| `Approval` controller | by the namespace's CURRENT binding: unbound → only a v1 document; bound → only a v2 document. v2: payload type; a `ConsoleConfirmation` signature by a usable key of the namespace's `TrustPolicy` (both modes); the exact subject **including UID**; the plan hash recomputed from the Restore; the policy name, snapshot digest and mode equal to the binding; a window no longer than `maxAgeSeconds`, issued no more than 60 s ahead, not expired. Governed: a second signature by a usable `GovernedApproval` key, and that key's `principal.id` ≠ the requester's `<issuer>#<subject>` | `ApprovalPolicyMismatch`, `AuthorizationDocumentInvalid`, `AuthorizationSubjectMismatch`, `PlanHashMismatch`, `AuthorizationWindowInvalid`, `AuthorizationExpired`, `GovernedApprovalRequired` (the pending state), `SelfApprovalRefused`, and the key refusals above |
| `Restore` admission | immediately before any ConfigMap or Job: the format matches the binding, the signed bytes still name this Restore, its plan and the current policy digest and mode, the window has not closed, and the Approval's own `status.authorization` agrees. The Approval's permanent refusals (`AuthorizationExpired`, `ApprovalPolicyMismatch`) end the Restore rather than hold it | terminal `ApprovalPolicyMismatch`, `AuthorizationExpired` |
| The mounted bundle | two more PUBLIC members: `approval-policy.json` (the frozen snapshot) and `confirmation.pub.pem`; `approver.pub.pem` is the key that authorised the run (the console's under Ordinary, the approver's under Governed). Each key must still be able to sign something new under its own usage when the bundle is written. The Job pins both through `LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256` / `…_CONFIRMATION_KEY_SHA256` | `ApprovalBundleMaterializationFailed` (a hold) |
| The runner | `--policy-snapshot`/`--confirmation-key`: re-verifies the console's signature, the snapshot's canonical bytes and digest, the subject from the execution contract, the plan hash and the window's shape; Ordinary requires the mounted authoriser to BE the console key, Governed requires a different key whose countersignature verifies | exit 3, `no data operation was started` |

The runner does **not** re-check expiry against its own clock: the controller
admitted the run inside the window, and an admitted run "continues under its
recorded policy snapshot" (D0) even if its pod waited in `Pending`.

**After admission the `Approval` is a record, not a gate.** Expiry, the policy
binding and the keys' windows bound the time to **admission**. Once the Restore
controller has recorded `Admitted=True` on the Restore that `spec.approvalRef`
names this `Approval` for (the pass that creates the runner Job), and that
Restore's own approval bundle (`<restore>-approval-bundle`,
`logweir.dev/approval-uid`) names **this `Approval` object's UID** — or, for a
Restore admitted before per-Restore bundles, the `Approval` existed at the
admission — the `Approval` controller adds `Consumed=True` (reason
`RestoreAdmitted`; its `lastTransitionTime` is the admission instant) beside the
verdict. From then on its `Verified=True` condition, `status.authorization`
(mode, policy, requester, confirmation key), `matchedKeyId`, `approver` and key
window stay as they were recorded, whatever the clock, a key's `notAfter`, a
retirement, a `Superseded`/`Unspecified` revocation or a policy edit does. The
record still binds only the Restore UID in `status.verifiedSubjectRef`; an
`Approval` deleted and re-created under the same name is another object and was
admitted under nothing.

**The one key event that still reaches it is a compromise** (D3 §7.4). A
consumed record reads the namespace's trust on each pass. When
`status.matchedKeyId` or `status.authorization.confirmationKeyId` is `Revoked`
with `KeyCompromise`, `status.verified` becomes `false` and the `Verified`
condition reads `RecordedBeforeRevocation` when the admission instant precedes
`revocationEffectiveFrom` (else `revokedAt`), and `KeyRevoked` otherwise —
**never green** either way, in the API and the console ("signer revoked for
compromise after use"). The authorization, key ids, approver and `Consumed` are
kept: they are what an incident responder lists runs by. The withdrawal is
sticky — a revocation is monotonic on a `TrustPolicy`, so a policy deleted or a
namespace re-bound does not restore the green (and since
TRUSTPOLICY-DELETE-DROPS-REVOCATION a compromise recorded on ANY policy reaches
the record, even one whose stored status is still clean). A consumed record whose recorded
key is still in the trust must also still carry a signature that verifies under
it; one that does not (a planted `Consumed`) is judged again from scratch. A
Restore that is only held (`Admitted=False`) or has no Job yet is not
admitted, and its `Approval` still expires `AuthorizationExpired` as before.
Defect P9 (the PoC install, 2026-09-24) was the absence of this rule: 900 s
after an Ordinary confirmation a succeeded Restore's `Approval` was rewritten
`Verified=False/AuthorizationExpired` with its provenance nulled. **Upgrade:**
an `Approval` an earlier build rewrote that way is re-verified at the admission
instant (the Restore's `Admitted` `lastTransitionTime`) on the first pass of this
build and, when it verifies there, is restored to `Verified=True` with
`Consumed=True`; a refusal message no longer names the current time, so an
unchanged refusal is never rewritten and no longer logs `approval refused` on
every pass. **Rollback:** an older controller ignores `Consumed` and resumes
re-judging consumed `Approval`s (the P9 behaviour); nothing it writes is
unreadable by this build.

**Separation of duties and key authority.** Three keys with three usages, one
usage each (CEL rule G8): the runner's `EvidenceSigning` key, which never
authorises; the console's `ConsoleConfirmation` key, which attests a requester
and — only under an `Ordinary` binding — authorises; and each human approver's
`GovernedApproval` key. The comparison that makes self-approval impossible is
between **principals**, never display names or key ids: a governed approver's
key must carry `principal.id: <issuer>#<subject>` — the approver's own OIDC
identity, the same string the console records as the requester — and the
console additionally refuses an approval submitted by the requester (403,
whatever roles the actor holds). An administrator is not a bypass. **The form is
enforced, failing closed:** under a Governed policy an approver key whose
`principal.id` is not `<non-empty issuer>#<non-empty subject>` (an email, a
display name, an `install:` id) is refused `SelfApprovalRefused`, because a
principal that cannot be compared with the requester cannot be shown to differ
from it.

**Keys to add to the namespace's `TrustPolicy`.** On a fresh install the hook's
installation policy already carries the signer and the console key. Otherwise:
the console's public key with usage `ConsoleConfirmation` (`logweir-console-trust`
publishes it; `GET /api/v1/namespaces/{ns}/approval-policy` prints its key id),
and, for a Governed namespace, each approver's public key with usage
`GovernedApproval` and their `principal.id`. The legacy roster never
yields a `ConsoleConfirmation` key (§7.3), so a namespace bound to a policy but
still on `legacy-roster-v1` refuses every v2 document `KeyIdNotInRoster` until
a `TrustPolicy` governs it.

**A direct write.** A `Restore` and an `Approval` written straight to the API
server in a bound namespace are kept, as they always were, and refused with the
reasons above; a v1 approval, a console-only document under Governed, a
document for another UID or plan, or a self-countersigned one creates **no**
ConfigMap and **no** Job. The legacy `kubectl proxy` page cannot produce a v2
document at all (D0: "Ordinary confirmation is unavailable through this legacy
direct-CR UI").

**What it does not cover.** A `RehearsalSchedule`'s standing authorization
keeps its own `GovernedApproval`-only format (§7g) whatever the namespace is
bound to; the ordinary form of a standing authorization (D3 §4.3) is not
implemented.

**Upgrade.** Existing installations keep their approval requirement: with no
document every namespace is `legacy-governed-v1` and every existing `Approval`
and signed archive verifies exactly as before. Binding a namespace applies to
Restores not yet admitted: a Restore created before the binding whose v1
Approval was not yet admitted is refused `ApprovalPolicyMismatch` and must be
submitted again through the console; a run already admitted continues.
**A policy change is a rollout, and it may need resubmits:** while the console
Deployment rolls, an old console pod can still sign under the old policy
digest, and the controller refuses those documents `ApprovalPolicyMismatch`
(terminal, by D0's rule). A Restore submitted during a policy rollout may
therefore have to be submitted again once the rollout completes. Order:
CRDs (the `Approval` status gains `authorization`) → controller and runner
image → console with its confirmation key → the `TrustPolicy` keys → the
binding. **PROD-16.1:** an upgrade changes no namespace's approval — no marker,
no trust step; the hook only generates (or adopts) the console key. To opt an
older install's unbound namespaces into `confirm`, two gates, as before: (1) a
trust administrator adds the console key from `logweir-console-trust` to the
`TrustPolicy` that governs them (usage `ConsoleConfirmation`); (2) the
installation administrator sets `approvalPolicy.allowOrdinaryConfirmation: true`
and `approvalPolicy.default: confirm` (or binds the namespaces to a `confirm`
policy) and rolls out ([install.md](install.md) §5f).

**Rollback.** Unbinding (or removing the document) returns the namespace to
`legacy-governed-v1`; not-yet-admitted v2 approvals are then refused
`ApprovalPolicyMismatch`, never admitted as v1. An older controller reached by
rollback refuses every v2 document as `PayloadTypeMismatch` — its payload type
is not the v1 approval's — and an older runner refuses the two new flags before
dispatch: both fail closed. Nothing deletes public key material in either
direction, and v1 documents and archives are untouched. **PROD-16.1:** an older
controller or console ignores the marker, so every unbound namespace is
`legacy-governed-v1` again: a pending `default-confirm-v1` confirmation is
refused `ApprovalPolicyMismatch` (never admitted as v1), and a document with
`defaultMode` makes an older binary refuse to start — remove
`approvalPolicy.default` before rolling back. The installation `TrustPolicy`
stays (an older controller reads it; its evidence keys keep verifying), and so
do both retained key Secrets.

## 9. Schedules, and a schedule's retention report (which deletes nothing)

A schedule's `spec.retention` only ever **reports**. The one component that can
delete archive objects is a `RetentionPolicy` in `mode: Enforce`, run by a
separately linked worker under its own credential and an administrator's
approved plan — §7f is that boundary, and nothing in this section changes it.

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

One operator-facing consequence, because it is reachable by an edit. The
schedule's retention report evaluates the **current** `archive.url`, so after a
move the old destination's sets stop being reported. The wrong-bucket report
this edit used to be able to produce (defect **RET-WRONGBUCKET**) is closed
since PLAT-16.1: a schedule whose archive is not the controller's handle now
gets a `note` and empty lists instead (*A schedule on another bucket is now
told so*, below).

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
| `False` | `LegacyOwnerReferencesRemain` | Terminal runs are still being detached; the next passes finish it. This is the ordinary state of a healthy upgrade, however many pages it spans. |
| `False` | `ActiveLegacyRunsOwned` | The only owned runs left are still running. A pre-upgrade run keeps its ownerReference until it is terminal, because its scheduled identity derives from that UID. |
| `False` | `MigrationBlocked` | Either one or more runs could not be detached — `status.history.migrationBlocked` names up to ten, sorted by name, and the total is in `legacyOwnedRuns` — or the inventory **did not finish** and nothing above explains why. See the table below. |

`kubectl --context docker-desktop delete backupschedule <name> --cascade=orphan`
is always safe, before or after the migration, and it is what to use while the
condition says `False`.

**`MigrationBlocked` has two causes, and the message says which.** Either one or
more runs could not be detached — then `status.history.migrationBlocked` names
them, and its `reason` is one of three values — or the **inventory itself did
not finish**, in which case `migrationBlocked` is empty and
`status.history.ownershipScanComplete` is `false`.

Every entry in `migrationBlocked` names a `Backup`, and its `reason` is a closed
vocabulary:

| `reason` | Clears itself? | What to do |
|---|---|---|
| `ApiForbidden` (the API server answered `403`) | yes | Fix the RoleBinding. The next hourly inventory sends the same patch. |
| `ApiInvalid` (the API server answered `422`) | yes | A pre-upgrade object that fails a newer schema. Fix it; the next inventory retries. |
| `NoScheduleReference` | **no, never** | The run's `spec.scheduleRef` does not name this schedule, so detaching it would leave it a member of nothing — and `Backup.spec` is sealed by CEL, so the reference cannot be added to an object that already exists. **This condition will not reach `True` while such a run exists.** Delete the schedule with `--cascade=orphan`, or record `status.backupId` and `status.evidence.*` and delete the run. |

A schedule can also have an unfinished inventory *while* one of the rows above
is the reported reason — a migration spanning several pages sets it on every
pass — and then the message carries "The inventory also did not finish" as a
suffix rather than replacing the reason. The reason stays the one that describes
what is happening; `ownershipScanComplete` stays the fact to act on.

**`status.history.ownershipScanComplete` is the machine-readable form of that,
and it is the field to gate an upgrade script on** — more precisely
than the condition, because it is `false` exactly when the controller has not
looked everywhere it would have to look. While it is `false` the schedule keeps
listing namespace-wide and never narrows to the `logweir.dev/schedule-uid`
label, which no pre-upgrade run carries.

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

No `Backup`, plan ConfigMap or Job is deleted by Logweir — see *A schedule's
retention report reports*, below, and the explicit rules under it. Retaining
history is therefore an etcd cost you choose:

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

**During the migration window the cost is higher, and bounded.** While a
schedule still has runs to detach it re-inventories on every reconcile (every
30 s) rather than hourly, because waiting an hour between batches would stretch
an upgrade over days. The detaching happens *inside* the paginated walk, and the
walk **stops as soon as that pass has spent its detach budget of 500** — so one
pass detaches at most one page's worth and never reads more than the page bound.

The walk restarts at page 0 each pass, though, and a page whose runs are already
detached does not stop it, so pass *k* reads *k* pages of already-migrated runs
before it reaches new work. **The whole migration is therefore quadratic in the
number of pages**, not linear. With `P` = ⌈*n* / 500⌉ pages of pre-upgrade runs:

| | LIST requests | objects read |
|---|---|---|
| one migrating pass | ≤ 20 (the page bound) | ≤ 10 000 |
| the whole migration of *n* pre-upgrade runs | `P(P+1)/2 + P − 1` | ≈ 500 × that |
| *n* = 500 (`P` = 1) | 1 | 500 |
| *n* = 2 000 (`P` = 4) | 13 | ≈ 6 500 |
| *n* = 10 000 (`P` = 20) | **229** | **≈ 114 500** |
| steady state, per hour | 1 | ≤ 10 000 |
| steady state, per 30 s reconcile | **0** | 0 |

The closed form is measured, not estimated: `a_multi_page_migration_costs_a_
quadratic_number_of_lists` drives a whole migration against a double that pages
properly and asserts the count against that formula.

For comparison, detaching after the whole walk — the shape this replaced — cost
one full 20-page walk per 200 runs, so the same 10 000 runs were about 1 000
LISTs and 500 000 objects read. Every walk, then and now, is `limit`ed and
page-bounded: none of them streams an unbounded response body. If the quadratic
term ever matters for your namespace, prune before upgrading (below) — halving
the run count quarters the cost.

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
overlap matters.

**After rolling forward, the migration picks those runs up**, and it does so by
construction rather than by luck. A run the older controller created carries
both the ownerReference *and* the `logweir.dev/schedule-uid` label, so even a
label-selected inventory sees it; the moment it does, `legacyOwnedRuns` goes
positive, the condition drops to `False`, and every later walk is namespace-wide
again until the detach is complete. The narrowing is never a one-way latch: it
is on only while the last **complete** walk concluded that nothing is owned, and
a walk that did not complete records `ownershipScanComplete: false` and turns it
straight back off.

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
| no exit code: `PodCreationForbidden` — the runner's pod, or a dynamic run's discovery pod, refused at creation (a `ResourceQuota`, a `LimitRange`, an admission webhook, a missing ServiceAccount) | no: the namespace has to change first, and the next slot runs normally |
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
Manual runs are bounded by a pool of their own instead (P10, *Manual runs may
queue* below), which scheduled runs never enter.

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

### Manual runs may queue (P10)

`concurrencyPolicy` never bounded manual runs, and nothing else did: on the PoC
install one operator's hundred accepted "Back up now" requests became a hundred
runner pods at once, the node hit its 110-pod limit and went `NotReady`. Manual
runs therefore have a pool of their own, **per namespace**, beside — never
inside — a schedule's policy:

| Run | Pool | Ceiling (installation policy, §22.2) |
|---|---|---|
| manual `Backup` (`spec.trigger.kind: Manual`, or no `trigger` and `triggeredBy` ≠ `schedule`) | manual backups | `runs.maxManualBackupsActivePerNamespace`, default `4` |
| admitted manual `Restore` (no `spec.authorization`) | manual restores | `runs.maxManualRestoresActivePerNamespace`, default `2` |
| `Scheduled`, `CatchUp`, `Retry` `Backup` | none — `concurrencyPolicy` and `maxActiveRuns` | unchanged |
| a `RehearsalSchedule`'s `Restore` | none — one active per schedule | unchanged |
| topic discovery, preflight, evidence fetch | the check pools (`checks.*`) | unchanged |

**The gate is the last check before anything is created.** A manual run is
refused, or held on a missing destination or approval, under its own reason
first; only then does the controller count. It admits the run when the manual
runs of its kind that already hold a slot in the namespace, **plus the older
manual runs still waiting**, are below the ceiling. A run holds a slot when its
own status says so — a recorded `status.execution` or `jobRef`, an
`Admitted=True` condition, a `Running`/`Resolving` phase, or any phase this build
does not know — **or when this controller admitted it and the watch has not
shown that yet.** The count reads the watch the controller already runs (no API
call), and a watch lags the controller's own writes; so the decision and a
**reservation** are taken together under one lock, and every later decision
counts a reserved run whatever the watch says. That is what holds the ceiling
when many runs decide in the same instant: a burst, restores released together
when their approvals verify, runs released together from a destination hold,
and every run re-enqueued at once by a controller restart or a `TrustPolicy`
event. The queue is FIFO by arrival — `metadata.creationTimestamp`, then the
UID; never a name, which a client chose. The timestamp has one-second
resolution, so within one second the order is the UID's: stable and the same
in every pass, but not the order of the clicks. Nothing is admitted until the
watch has finished its first list.

**Every admission is written before anything is created.** The admitting pass
first writes `Admitted=True` (and clears `status.queue`), and only then
discovers, freezes and creates. A reservation lives in the process; the record
does not, so a controller that restarts between the record and the Job counts
the run from its own status. A failed record creates nothing. Reservations are
per controller process: the chart runs one, with `Recreate`, so a rollout never
overlaps two; a force-deleted or partitioned pod whose replacement starts while
it still runs can, and then the excess is what both admit within one watch lag
— at most twice the ceiling, briefly — because each counts the other's
`Admitted=True` records as soon as its watch delivers them.

**A queued run has nothing.** `phase: Queued`, `Admitted=False` reason
`ConcurrencyLimited` (the word a queued `Preflight` uses), and
`status.queue.limit` — the ceiling, and nothing that moves, so a run that waits
an hour is written once. No plan `ConfigMap`, no `status.execution`, no Job and
therefore no execution claim: a queued run freezes its inputs on the pass that
admits it, exactly as it would have on its first pass (the frozen-inputs
contract is unchanged, and a run that WAS frozen or admitted is never
re-queued — its Job is re-created from the frozen inputs). A queued run is
looked at again on the 15 s requeue, so it starts within one requeue of a slot
freeing.

**A queued `Restore` keeps its approval, and the approval keeps its clock.** The
queue does **not** extend an approval's maximum age (`maxAgeSeconds`): every
pass re-runs the admission before the gate, so a restore still queued when its
authorization expires is refused `AuthorizationExpired`, exactly as any other
wait would end it. The deadline is on the object while it waits —
`status.queue.authorizationExpiresAt` and the `Admitted` message, and the
console's "Queued (limit N active; approval expires T)" — and the refusal says
"expired while this Restore was queued behind N manual restore(s)". Keep
`runs.maxManualRestoresActivePerNamespace` and restore bursts small, or
re-confirm.

**A known limit: the pool is for runs that DECLARE themselves manual.** A
subject who may create `Backup` objects directly can declare a scheduled kind
(`trigger.kind: Scheduled`, an existing schedule's `scheduleRef`, a valid slot
and the matching name); such a run is bounded by neither this pool nor the
schedule's `concurrencyPolicy`. RBAC on `create backups` is what governs direct
object creation; `logweir-operator` has it, as D1 §8.7 already records.

**Rollback.** An older controller has no pool: `Queued` is a phase it does not
know, so it reads the run as active and starts it — every queued run at once,
as before. Nothing is stranded.

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
| 5a | The latest due slot came due before the schedule's `metadata.creationTimestamp` | nothing; not counted in `missedSlots`, no `lastSlot` | `Scheduled` (True) |
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

Row 5a is the Kubernetes CronJob rule: a schedule never fires a slot whose due
time is before the schedule itself existed, even when that slot is still inside
`startingDeadlineSeconds`. A schedule created at 00:53 with a slot at 00:30
first fires at the next slot; the `Ready` message names the earlier slot and
the creation time. The bound is strictly before, at the one-second resolution
of `creationTimestamp`, so a slot at exactly the creation second is the
schedule's own. It applies to creation only: a slot that came due before an
**edit** is still decided by rows 17–21 (row 19 keeps catch-up from
back-filling a revision). Use "Run first backup now" (a manual `Backup`) for an
immediate first run.

Upgrading to a controller with this rule changes nothing already recorded. A
slot an older controller fired before the schedule's creation keeps its
`Backup`, `status.lastFireTime` and `lastSlot`; until the next slot the
`Ready` message says an earlier controller fired it, and this controller
neither fires nor retries it (a failed pre-creation run is not retried). A
pre-creation slot an older controller counted as missed stays in
`status.missedSlots.count` and `status.lastMissedSlot`; nothing re-evaluates
it. Because the bound reads `metadata.creationTimestamp`, deleting and
recreating a schedule — a GitOps prune and re-apply, a backup-tool restore —
resets it: the slot just before the recreation is not fired.

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

A slot that came due **before the schedule was created** is not a skipped slot
of that schedule at all: it is never fired, never counted in
`status.missedSlots` or `status.lastMissedSlot`, and the schedule reports
`Ready=True reason=Scheduled` until its first slot (row 5a above). Only slots
at or after `metadata.creationTimestamp` are fired, missed, caught up or
retried.

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

### A schedule's status moves when its content does (FX-29)

**What wakes the controller.** A schedule is reconciled when it is created,
when its spec changes (a new `metadata.generation`), and on the requeue every
pass returns — at most 30 seconds, sooner when a retry or a slot is due first.
A status write does not wake it: the controller is the only writer of a
schedule's status, so a status event is its own last write coming back. A
status edited by hand (clearing a stale reservation, say) is read at the next
requeue.

**What it writes.** Nothing, when the status it computes is the one the object
already carries. The three "when" fields move only with what they time:

- `status.lastSlot.decidedAt` is when the decision recorded beside it was
  reached. It moves when the slot, `dueAt`, the attempt, the disposition, the
  reason or the `backupRef` moves, and never on a reconcile that reaches the
  same decision again.
- `status.retentionReport.evaluatedAt` is when the report's findings were first
  reached; an evaluation that finds the same keeps it.
- `status.policy.evaluatedAt` is when the status last moved.

One write is not about a decision: `status.history.inventoriedAt` moves once
an hour, when the controller re-inventories the schedule's retained runs, and
`policy.evaluatedAt` moves with it. A schedule watched for a steady
`resourceVersion` therefore still writes about once an hour.

`status.lastSlot.backupRef` is present only when the decision names a `Backup`
(`Admitted`, `CaughtUp`, `Retried`, `Failed`, and `Exhausted` when an attempt exists). A
`Missed`, `Blocked`, `NameUnavailable` or `Released` slot carries none.

**Upgrading.** Before FX-29 a slot decided `Missed` (or `Blocked`, or
`NameUnavailable`) right after one that ran kept the earlier slot's
`backupRef`, because a merge patch cannot remove a key it does not send — and
the controller read that leftover as a different decision on every pass, wrote
a fresh `decidedAt` each time, and was woken by its own write: about 120 status
writes a second per schedule on the PoC, until the next slot. The first
reconcile after the upgrade removes the leftover reference with one status
write, which moves `decidedAt` and `policy.evaluatedAt` once, to the instant of
that write. Nothing else is rewritten. **Rolling back** restores the old
behaviour the next time a slot is skipped or blocked after one that ran; a
schedule that is spinning stops when it is suspended
(`kubectl patch backupschedule <name> --type merge -p '{"spec":{"suspend":true}}'`)
or when its next slot is decided.

### A schedule's retention report reports. It never deletes

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
deleted, nothing that computes it can delete, and **no Logweir component in
tag 1 holds any delete capability against object storage** (since Amendment H
the one exception is §7f's separately linked worker — the version-scoped form
below) — the controller's archive handle is built with the
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
attributable record.* The sentence above therefore becomes
version-scoped: it holds wherever `RetentionPolicy.mode != Enforce`.

**The worker now exists** (`crates/logweir-retention`, decision D3 §6.5), so the
sentence above is genuinely scoped rather than vacuously so. It is still true of
every installation that has not created a `RetentionPolicy`, of every policy in
`Report` — the default — and of every policy in `ExternalLifecycle`; and it is
still true unconditionally of `logweir-store`, of the control plane, of the
everyday `logweir` binary and of `logweir-api`, none of which links the deleting
crate. `scripts/check-no-archive-write.sh` check 3 proves that last claim from
`cargo metadata` rather than from source text, which is the only way to prove it.
**§7f is what an operator turning `Enforce` on should read**, and it is deliberate
that doing so requires an administrator to select the mode, provide a separate
delete-capable Secret, and copy a plan digest onto the spec.

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

#### A schedule on another bucket is now told so, instead of shown someone else's catalog

The controller holds **one** archive handle, built from `LOGWEIR_ARCHIVE_URL`,
and the report renders removal commands for the schedule's own
`spec.archive.url`. When those are two different locations the old report listed
one bucket's manifests under the other bucket's `aws s3 rm` lines — defect
**RET-WRONGBUCKET**, and since `destinationRef` became editable it is reachable
by an edit between runs, not only by creating a schedule elsewhere.

The report is now **replaced**, not corrected:

```bash
kubectl --context docker-desktop get backupschedule nightly \
  -o jsonpath='{.status.retentionReport.note}'
# the controller's archive handle points at a different destination; this
# schedule's retention is not evaluated here — create a RetentionPolicy
```

`setsKept`, `setsThatWouldBeRemoved`, `awsCli`, `mcCli` and `skipped` are all
**empty**, and the empty lists are the point: "not evaluated" and "nothing to
remove" are different claims, and only one of them is true here. The remedy is a
`RetentionPolicy` (§7f), which reads its own destination's catalog view and
therefore cannot make this mistake at all.

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

**Which locations count as the handle's, exactly.** The comparison is on the
bucket **and** the prefix after normalisation, so `s3://b/p` and `s3://b/p/` are
one location and `s3://b/p` and `s3://b/pp` are two
(`retention_plan::legacy_report_applies`). An earlier revision of this section
said a different prefix in the same bucket still reported and a mismatch was an
omitted report with an INFO line; neither is what this build does — the
mismatch gets the `note` above, and the controller logs it at `WARN`.

**A destination-backed schedule gets a note too, never an evaluation**, even
at the same bucket: the global handle's credential is not the destination's,
and a report produced with the wrong principal under-reports whatever that
principal cannot list. Its `status.retentionReport.note` says the schedule
writes to a saved `BackupDestination` and that a `RetentionPolicy` reports
retention there; every list is empty. That is the documented behaviour and not a
failure.

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
| **1** | `Failed` | `operational`, or `ExecutionAlreadyClaimed` off a backup runner's final `failure-reason=` line, or `TargetTopicAppeared` / `CreatedTopicsLeft` off a restore runner's (a stopped creation step: §12, "Restoring under the original topic names") | `Failed=True`, reason `Operational` | The run could not be attempted or continued. **No artifact was written.** `ExecutionAlreadyClaimed`: an earlier run of the same execution reached the engine — it holds the claim (RECEIPT-DUP), or, with no claim, its backup set already exists in the archive (FX-7) — so this one did not start it. With no reason: among others, a backup runner whose read of the archive to prove its set is new failed TRANSIENTLY (a transport error, a timeout, a 5xx the client had already retried; FX-7) — retryable, and a retry is a new execution id. |
| **2** | `Failed` | `drill-not-pass` | `Failed=True`, reason `DrillNotPass` | A result that is not a pass — **a document WAS written and signed.** Not produced by `backup run`; it is the drill path's code and the row is here because `exitReason`'s vocabulary is one vocabulary across both paths. |
| **3** | `Failed` | the terminal state off the log's `refusal-reason=` line, or `GuardRefusedUnknownReason` | `Failed=True`, reason `GuardRefused` | A guard refused before anything ran. The condition's message ends with the runner's own reason: see "What a refused run says about why" below. |
| **4** | `Failed` | `signing-or-lock`, `OrphanedScorecard`, or `ExecutionClaimUnproven` off a backup runner's final `failure-reason=` line | `Failed=True`, reason `SigningOrLock` | Signing or the lock proof failed and **nothing was uploaded**. `ExecutionClaimUnproven`: the evidence store refused the execution claim or does not enforce conditional create, or (FX-7) the archive could not be read to prove the backup set is new for a reason no retry changes (a 401/403, a wrong bucket, region or CA); the engine never started. |
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

There is **no `resources` block**: `Backup.spec` has no requests-or-limits
field, so a backup's runner states none and the namespace's `LimitRange`
supplies them or nothing does. A `ResourceQuota` that requires limits rejects
the pod, and the `Backup` reports `RunnerReady=False`, `PodCreationForbidden`.
(`Restore.spec.runnerResources` is the `Restore`'s field — §12.)

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
   `KafkaCluster.status.clusterId` — that field is the probe's record of its
   last successful look, rewritten by every reachable pass (the companion
   `reachable` is cleared by every probe verdict that cannot vouch for it,
   and never by the collection of a probe Job whose verdict is already on the
   status — the crashed-Job section below), so pinning
   it would turn ordinary probe churn during the discovery window into a
   refused run.
2. Creates `lwd-<backup uid>` — a `topicInventory` check Job, running
   `logweir check run --plan /check/check-plan.json --check-contract-version 1`
   as the runner ServiceAccount with **no** Kubernetes token, **no** signing key
   and **no** archive credential — plus its immutable plan ConfigMap
   `lwd-<backup uid>-plan`. Both are owned by the `Backup` with
   `controller: true`, so deleting the `Backup` collects them; the Job carries
   `logweir.dev/purpose=topic-discovery` and
   `activeDeadlineSeconds: min(300, spec.deadlineSeconds)`. It runs **the same
   image and pull policy as the run's own runner Job** — the controller's
   `LOGWEIR_RUNNER_IMAGE` and `LOGWEIR_RUNNER_PULL_POLICY`, or the compiled-in
   pin when neither is set (§14). The phase becomes
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
| `DiscoveryFailed` | The check Job did not produce a usable result: an unreachable broker, a rejected credential, a pod that never started, a deadline | yes |
| `PodCreationForbidden` | The discovery Job's pod was refused at creation — a `ResourceQuota`, a `LimitRange`, an admission webhook or a missing ServiceAccount — and its `FailedCreate` event says so. Decided 30 seconds after the Job, which is cancelled; the message quotes the admission. The same state a runner pod refused at creation ends a `Backup` with (FX-11) | yes, once the namespace admits the pod; a schedule's `spec.retry` does not retry it |
| `DiscoveryResultUnreadable` | It produced output that did not verify — frames that do not decode, a result document whose counts or digest the frames do not support, a missing plan ConfigMap, no result document at all, or a verified result carrying neither an inventory nor a blocking check that is not `ready` | no, not without fixing the runner |
| `DiscoveryIncomplete` | Visibility was not established and the policy is `Refuse` | only with more permission, or an attestation |
| `SelectionEmpty` | Nothing was left after internal topics, exclusions and the topics the broker would not describe | only if the cluster changes |
| `SelectionTooLarge` | Over 5,000 resolved names, or over 256 KiB of them | only with more exclusions |
| `SelectionTooLarge` (truncated listing) | The runner had to cut the listing at the plan's 20,000-topic `maxTopics` or at its relay budget, so the names are a **prefix** of what the principal can see | **not** with more exclusions — they are applied controller-side, after the listing. Name the topics explicitly, or split the cluster across schedules |
| `SourceChangedDuringResolution` | The broker's `clusterId` is not the one the `KafkaCluster` observed, or the saved connection changed while the discovery ran | yes |
| `JobNameConflict` | Something else owns `lwd-<backup uid>` | remove it first |

**A classified broker failure is `DiscoveryFailed`, and the message names the
check's own code.** When the runner reaches the broker and the broker refuses
it, the relayed result carries no inventory and one blocking
`connection.authenticated` row instead — `BrokerUnreachable`,
`AuthenticationFailed`, `MetadataTimeout`, `TlsHandshakeFailed`,
`ClusterAuthorizationFailed` and the rest of the check vocabulary (§7c). The
controller projects the first blocking check that is not `ready` (`notReady`
before `unknown` or `skipped`, advisory and execution-only rows excluded) and
writes that code, the check's id, the runner's sentence and its remedy into
`TopicsResolved`'s message, while the reason stays in this table's closed set.
So an unreachable bootstrap and a rotated password are told apart by the
message and are **both retryable** — before 2026-09-18 both read
`DiscoveryResultUnreadable`, which says the runner needs fixing and that a new
`Backup` cannot help. Neither the reason vocabulary nor the CRD changed;
rolling the controller back restores the older, flatter message.

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
closed. The chart and `logweir.yaml` wire the policy reference (§22.2), so
`coverage: AllUserTopicsAttested` is written exactly when such an attestation
matches; without one the two labels a run can earn are `VisibleUserTopicsOnly`
(with `incompleteDiscovery: BackUpVisibleTopics`) and, for a named allowlist,
`NamedTopics`. (Earlier revisions said no chart rendered the reference; that
stopped being true with D2 W11.)

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
**Allowed is not the same as running at once** (P10): when the namespace's
manual-run pool is full the run waits `phase: Queued` with nothing created and
starts in arrival order (§9, *Manual runs may queue*). A MANUAL-kind object
created with `kubectl` queues exactly as the console's does — the pool is the
controller's, not the API's. A hand-made scheduled-kind object does not (§9,
*A known limit*).

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

**One CR path, three doors, and the name is the same through all of them.** The
name above is not decoration: it is the idempotency record. D1 §8.2 derives it
as `logweir-manual-` followed by the first 26 characters of the lowercase,
unpadded RFC 4648 base32 (`a`–`z` then `2`–`7`) of `sha256` over a document that
opens with the literal line `logweir-api/idempotency-scope/v1\n` and then
carries five fields — **issuer, subject, namespace, route, key** — each prefixed
with its UTF-8 byte length as a 64-bit big-endian integer. `route` is the string
`POST /api/v1/namespaces/{ns}/backups`. The format line is inside the hashed
bytes on purpose: a format change is then a name change rather than a silent
change of question, and the length prefixes are what stop two fields being
re-cut into a different pair that hashes the same.

The rule is implemented twice — `crates/logweir-api/src/idempotency.rs`
(`identity`, `base32_lower`) and `ui/client.js` (`manualBackupName`) — and one
file pins both: `ui/tests/fixtures/manual-backup-names.json` records six scope
tuples and the name each one produces, and **each implementation is held to it
by its own unit test** —
`crates/logweir-api/tests/manual_backups.rs::the_manual_run_name_fixture_is_this_routes_own_rule`
and `ui/tests/d1.spec.js::the_manual_run_name_rule_is_one_rule_and_the_fixture_pins_both_sides`.
Neither pins only itself, and a drift in either fails a test with no cluster and
no browser. On top of that, `scripts/live/d1/run.py`'s `L-06-2-cli` re-derives
every recorded row with the page's own function on a real machine and then
requires the name the **real** `logweir-api` gave an object it created to equal
the name the page derives for that same live scope. What this buys an operator
is that the three doors produce **one object**:

| Door | Who derives the name | What else differs |
|---|---|---|
| `kubectl create -f config/samples/backup-manual.yaml` | you do, or you pick any free DNS-1123 name | no idempotency annotations |
| `POST /api/v1/namespaces/{ns}/backups` with `Idempotency-Key` | the API, from the authenticated `(issuer, subject)` | `api.logweir.dev/idempotency-scope-sha256`, `…/request-sha256`, `…/request-id`, `…/actor` |
| the console's "Back up now" behind `kubectl proxy` | the page, from the same rule with an **empty** issuer and subject | `logweir.dev/request-sha256` over the request the page built |

`spec` and the four labels are **identical** across all three; only the
annotations differ, and they differ because they are hashes of different
documents. The legacy page's issuer and subject are empty because a browser
behind `kubectl proxy` holds neither — the credential is attached by the proxy,
out of the page's sight — so in that mode the name is decided by
`(namespace, route, key)` alone. The key is 128 random bits minted per draft,
and a collision would be an `AlreadyExists` whose stored
`logweir.dev/request-sha256` decides replay versus conflict, which is what the
product API does with its own annotation.

**The legacy page copies the policy digest and never computes one.**
`spec.scheduleRef.runPolicySha256` is `weirkeeper::policy::run_policy_sha256`
over a canonical document, the reconciler recomputes it, and a mismatch is a
terminal refusal. So the browser reads `status.policy.runPolicySha256` off the
schedule, and only when `status.policy.generation` equals the
`metadata.generation` it is about to copy — the controller's own statement that
the two describe one revision. When the controller is behind, the page refuses
by name, says which number is behind, and points here. The same holds for the
in-cluster UI ServiceAccount's authority: it holds `get`, `list` and `create` on
`backups` and no `patch`, `update` or `delete`, because a run's inputs are
frozen and a second run is a second object.

### Consumer position evidence: `spec.consumerGroups`

A `Backup` or `BackupSchedule` may name the consumer groups whose committed
positions its runs record as evidence:

```yaml
spec:
  topics: [orders, payments]
  consumerGroups: [billing, invoicing]   # exact ids; at most 100
```

Each run reads the selected groups just before its engine starts and records
them as signed evidence (format 1.7.0): **exactly one outcome per group** —
`captured`, `excluded` with a reason (`GroupTypeNotCaptured` for a share or
streams group, or another protocol; `GroupNotFound` for an absent id), or
`failed` with a reason (for example `NotVisibleToPrincipal`, a group the run's
principal may not describe, or `GroupVanishedDuringCapture`, one deleted while
it was read). The receipt's `consumer_positions` carries each group's outcome
and, for a captured group, its position COUNTS; every position — every
partition of every backed-up topic accounted for, each judged against the
archive — is in the positions document beside the receipt
(`<run_id>.consumer-positions.json`), which the receipt binds by digest. **The
receipt's size depends on the number of groups, never on partitions**, so the
catalog reads a point that selects 100 groups over many partitions as
`Available`. A partition with no committed offset is counted, **never offset
0**, and nothing is ever dropped
([the field reference](formats/backup-receipt.md#consumer_positions--consumer-position-evidence-format-170)).

- **What it costs the source.** Read-only: the group listings, one
  DescribeConsumerGroups, one RequireStable OffsetFetch per captured group (each
  bounded at 15 s; a group with a pending transactional offset commit fails
  `PositionsUnstable` after that bound, rather than recording the stale
  position), and the partitions' watermarks. The run's principal needs
  **Describe on each selected group** and, for a complete listing, **Describe on
  the cluster**: without it the listing is filtered (T14), an unlisted id is
  classified by a targeted describe, and a group this principal may not see is
  `failed: NotVisibleToPrincipal`, never absent. Nothing is committed and no
  group is joined.
- **Not atomic with the records.** Positions are read while applications run;
  an `active` group may commit again a moment later, and the engine reads the
  records after that. The receipt says when the positions were observed and
  which groups were active; nothing claims one consistent cut. A topic whose
  marks REGRESSED during the run — its log start or high watermark read after
  the engine below the one read before — fails the groups holding a position
  on it (`GenerationChangedDuringCapture`). That is the only detection the
  position evidence makes: a topic recreated and refilled past its old marks
  before the second read is not seen by it, and a second read that failed
  decides nothing. The same receipt's `generations` block records each
  topic's ID before and after the engine, which is where such a recreation
  shows.
- **Kafka 3.7.x.** A broker below ListGroups v5 types no group, so on 3.7.x
  every selected group is `excluded: GroupTypeNotCaptured`: the run says so
  rather than guessing. Use 3.9 or 4.x to capture positions.
- **Where it shows.** Both receipt readers print one `consumer_positions` line
  per group, and — given the positions document (`--consumer-positions`) —
  verify it against the receipt and print each position; the catalog point
  record carries the summary bound by the block's digest; the catalog's view
  and the product API (`PointView.consumerPositions`) show the snapshot's
  freshness (`observedBeforeRecoveryPointMs`) and, per group, its outcome and
  its COUNTS — how many positions relate to archived data, and how many were
  never committed, beyond the end, failed or not observed. The per-position
  relation is the document's.
- **Refusals.** The CRD schema bounds the list (100 ids of 1 to 255
  characters). A repeated, blank or control-character id, one over 255 bytes,
  more than 100, or a selection whose receipt summary could exceed 80 KiB as
  the receipt encodes it (ids of `"` or `\` count double, so 84 such 255-byte
  ids fit), is refused before any Job by name (`ExecutionSpecInvalid`:
  `ConsumerGroupSelectedTwice`, `ConsumerGroupIdInvalid`,
  `ConsumerGroupSelectionTooLarge`), and a schedule carrying one is
  `Ready=False`.
- **Set only through the CRDs today.** The product API's create bodies refuse
  `consumerGroups` (an unknown field, never silently dropped) and the console
  neither sets nor shows a selection; a `kubectl`-set selection survives a
  console edit.
- **Applying positions is not this field.** Nothing here resets a group: a
  reviewed cutover (PROD-04.2) applies translated positions. With the source
  gone, read them from the evidence store as
  [the receipt format describes](formats/backup-receipt.md#recovering-positions-with-the-source-offline).

**Upgrade and rollback.** `spec.consumerGroups` is an additive CRD field (apply
the CRDs). Absent or empty, a run is the run it was: its plan, its frozen
execution inputs, its run-policy digest and its receipt are byte for byte what
they were. A schedule that names groups changes its `runPolicySha256` (the
selection is part of what a run does; the digest sorts it). An **older**
controller ignores the field on an unfrozen object (records no positions) and
refuses a run whose frozen inputs carry `consumerGroups`
(`PlanConfigMapConflict`), so let such runs finish, or remove the field from
the schedule, before rolling back. The console creates schedules without it;
its edit leaves a `kubectl`-set selection alone.

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
  | `destination` | **`v2`** | the resolved saved `BackupDestination` this run writes to: `{name, uid, generation, locationDigest, archiveStorage, evidenceStorage, transport, addressing, caSha256?, grant}`. Absent for a legacy inline-`archive` run. **The Job is rendered from THIS block and not from the live object**, so a credential or CA rotated between the freeze and a Job re-creation cannot change what an approved, half-written run addresses. It carries no credential value: the grant is a Secret name and data key names |
  | `archive` | `v1` | the archive URL, the resolved `storage` block and the object-store addressing variables this controller forwards. **For a destination-backed run the forwarded list is EMPTY and `storage` comes from the destination**; `url` is then the `logweir-destination://` sentinel, which an older controller refuses terminally as `ArchiveUrlUnreadable` rather than writing anywhere |
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

### `status.destination`: where this recovery point actually is

A destination-backed run publishes a second block in that **same** pre-Job
patch, from the **same** snapshot:

```bash
kubectl --context docker-desktop -n <ns> get backup <name> -o jsonpath='{.status.destination}'
# {"name":"prod","uid":"…","generation":3,"locationDigest":"sha256:…"}
```

Four fields and no more. `locationDigest` is the digest of the canonical
location — the value `BackupDestination.status.locationDigest` publishes for
the same object — and it is what a restore, a preflight and PLAT-15's catalog
identify a recovery point's location by. The storage settings, the transport,
the addressing, the CA digest and the grant stay in the `destination` block of
`execution-inputs.json` above, because that is what the **Job** is rendered
from and a second spelling of them here would be a second thing to keep true.
There is no credential value and no CA byte in either place.

**Written once, at the freeze, and never rewritten.** A later pass re-reads the
stored snapshot and renders the identical patch, so nothing is sent; a
destination edited afterwards — an access-key rotation, a CA rotation, a new
generation — moves neither the plan nor this block. That is the property a
recovery point needs: it says where the run *wrote*, not where the object
called `prod` points today.

**Absent is a real value.** A legacy inline-`archive` run carries no block at
all, and neither does any `Backup` a controller frozen before this field
reconciled — the key is omitted, never written as `null`. Absent means "this
recovery point publishes no frozen location", which is a different fact from
"its location disagrees", and §21.8 is where the two get different answers.

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

**The re-created Job never runs the engine a second time over one execution**
(tracker defect RECEIPT-DUP). Every runner claims its execution id with a
create-only `logweir/backups/<backupId>/execution.claim.json` immediately before
the engine starts ([the execution claim](formats/backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id)).
If the lost Job's pod got that far, the re-created Job finds the claim and exits
**1** naming `ExecutionAlreadyClaimed`, with no engine run and no receipt: a
second engine run would have overwritten the manifest the first run's signed
receipt attests. **The same holds for a Job whose lost pod was an OLDER runner
without the claim** (FX-7): the re-created Job wins a fresh claim, reads
`<prefix>/<backupId>/`, finds the older run's manifest or segments there and
stops with the same exit and reason — the engine would otherwise rewrite that
run's segments in place
([the format](formats/backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id)).
The `Backup` ends `Failed` with `status.exitReason:
ExecutionAlreadyClaimed` (the runner's final `failure-reason=` line, lifted by
the controller; `kubectl describe backup` shows it on `status.exitReason` and
the `Failed` condition's message, and the console in the run's exit reason and
message). Exit 1 is retryable, so a
schedule **with `spec.retry` configured** starts a **new** execution
`<uid>-<slot>-r<k>`; without `spec.retry` (the default) the slot is recorded
`RunFailed` and the next slot runs normally. A manual `Backup` is retried by
creating a new one. Whatever the lost pod already signed stays in the bucket
and in the catalog. A Job lost before its pod reached the claim is re-created
and runs normally. An evidence store that does not honour conditional create
(`If-None-Match: *`) makes every backup exit **4** with `exitReason:
ExecutionClaimUnproven` before the engine starts — and a destination whose
`writeProbe` is on reports that store `notReady / ConditionalCreateUnsupported`
before the first backup (§21.5).

**Rolling the runner back re-opens this window, and on an unversioned bucket
nothing reports it** (FX-7). A runner from before the claim (`v0.1.5` is the
measured one) ignores the claim and the set check: if a `Backup`'s Job is
re-created with it, it re-runs the engine over the set, exits 0 and signs a
second receipt. On a versioned bucket the first point is then reported
`Conflict` (its pinned manifest version was superseded), though the older
runner's own, unpinned point over the same set stays selectable. On an
unversioned bucket the first point keeps verifying — the manifest bytes can
come out identical — while the segments under it were rewritten, and no check
this build runs sees it. **Let in-flight `Backup`s finish before rolling the
runner back** ([release notes](release-notes.md), *Before a rollback*).

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

`logweir backup run` prints `receipt-sha256=sha256:<64-lowercase-hex>` for the
exact persisted receipt bytes, followed by its **final two stdout lines** in
this order: `receipt-key=<key>` then `sidecar-key=<key>`. The controller reads them
back through the **`pods/log` subresource** and writes them to
`status.evidence.receiptKey` and `status.evidence.sidecarKey`.

It matches them **by key name, not by position.** A log body with the two lines
reversed still puts each key in its own field, and a log body with neither
leaves **both keys unset**. No key is ever derived from the backup id: a
guessed key points at an object that may not exist, and a verifier would then
report `Invalid` for a run whose evidence was merely unread.

**The evidence fact is its own condition, and it exists only at exit 0** (and,
on a `Restore`, at exit 2 when the runner named its signed failure — §12).
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
absent, phase `Failed`; for a `KafkaCluster` probe, `Reachable=Unknown`,
`reachable` cleared and the Job replaced on the re-probe cadence). Picking the newest would be worse than picking at
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
`NoExitCode`, and for a probe `reachable` cleared and the Job replaced on the
re-probe cadence — which is also what happens when the pod was genuinely
garbage-collected BEFORE the run's verdict was recorded (one collected after
it is not re-judged: *A Job Kubernetes is collecting is not a crash*, below).
A Job with no
`metadata.uid` is not even listed for. Every candidate that was not read is
logged once, with code `ForeignPodIgnored` and the namespace, the Job and the
pod name; in the contested case **all** claimants are named, including the one
a newest-wins rule would have chosen. Nothing here is configurable and nothing
changes an object's schema, so there is no migration step; an operator sees the
change only as a run whose pod was never really its own no longer producing a
status.

### The crashed Job: when there is no exit code at all

A Job can finish having produced no terminated state for `runner` — the node
went away, the pod never scheduled, the pod was garbage-collected before the
run's verdict was read. There is no
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

`status.exitCode` is **absent** in all four rows.

**A probe whose Job crashed vouches for nothing, and is probed again**
(PoC batch 1, O-1). For a `KafkaCluster` every row above writes
`Reachable=Unknown`, **clears `status.reachable`** and keeps `clusterId` (the
identity last observed) — so no earlier `reachable: true` survives beside
`NoExitCode` to admit a `Restore` or a rehearsal — and then gives the finished
Job the usual five-minute TTL, which is the re-probe timer: the pass after the
TTL controller collects it creates a fresh probe Job. A `ProbeOutputUnreadable`
verdict and a `NameTooLong` refusal clear `reachable` the same way. Before this
build a crashed probe left `reachable` and the terminal Job in place, and the
connection was never probed again.

**A pod that was never created is not a crash** (FX-11). When the Job has no
pod at all and the Job controller's `FailedCreate` event on it says why — a
`ResourceQuota`, a `LimitRange`, an admission webhook or a missing
ServiceAccount — the reason is `PodCreationForbidden`, quoting that event,
rather than `NoExitCode`: on a `Backup` and a `Restore` through the
diagnostics' fail-fast (*The runner's requests and limits*, §12), and on a
`KafkaCluster` probe as `Reachable=Unknown` / `PodCreationForbidden` with
`reachable` cleared, `observedAt` left alone and the usual TTL, so the next
probe runs. So a probe pod the cluster refuses — under a pod quota, for
example — clears `status.reachable` on that connection until a probe runs
again, even where the connection answered minutes before, and a `Restore` that
names it as its target is refused `ClusterNotReachable` meanwhile. With no such
event the rows above stand. An absent `EXIT` column with
`PHASE=Failed` is therefore a real, distinct state and not a rendering gap.

**A Job Kubernetes is collecting is not a crash either** (FX-19). The TTL
controller deletes a finished Job with *foreground* propagation: the Job gains
a `metadata.deletionTimestamp`, its pod is deleted first, and the Job stays
readable — finished and pod-less — until the pod is gone. A controller
patches a Job's `ttlSecondsAfterFinished` on only AFTER the status write that
recorded the Job's verdict (the catalog sync Job alone carries its TTL from
creation, and that TTL is at least an hour — three sync intervals — so the
sync is harvested long before it), so that collection follows a recorded
verdict, and no controller reads it as a crash:

* `Backup` and `Restore` stop at their terminal status before any pod read;
  the catalog sync stops at the record of that Job's completion, retention at
  `lastEnforcement.finishedAt`, a `ProtectionPolicy` delivery once its ledger
  entry is no longer `Pending`, and `Preflight` and `TopicDiscovery` never
  read a terminal object's Job at all.
* The `KafkaCluster` probe re-reads its Job on every pass, so it states the
  rule itself. A probe Job with a `deletionTimestamp` is judged not at all —
  no pod read, no status write, no TTL patch — and the next probe is created
  once it is gone. A finished probe Job that carries **this controller's
  marker** has had its verdict recorded and is not re-judged when it has no
  terminated `runner` left: the annotation
  `logweir.dev/probe-verdict-recorded`, whose value is the Job's own UID, which
  the controller sends in the same patch as the five-minute TTL and only after
  the status write that recorded the verdict. **A TTL alone is never the
  marker:** a mutating admission policy or a defaulting webhook that gives
  every new Job a `ttlSecondsAfterFinished` does not make a crashed probe read
  as judged, and the controller overwrites that TTL with its own re-probe
  timer.
* **The last recorded verdict stands until the next probe answers, for at
  most 630 s** (`STALE_AFTER_SECS`, twice the 315 s re-probe interval — the
  console's own freshness budget). The probe Job's name is fixed, so while it
  is mid-deletion (a finished pod `Terminating` on a node that went away, a
  foreign finalizer) or judged and never collected by its TTL, no newer probe
  can run. Once the reading in `status.observedAt` is older than that bound,
  any pass that forms no new verdict — the deferred ones, a re-read of the old
  Job, a probe still running — clears `reachable` with reason `ProbeStale`
  (`Reachable=Unknown`, its message naming why no newer probe has answered)
  and leaves `observedAt` and `clusterId` as the record of the last real look.
  A `Restore` and a rehearsal admit a target on `reachable: true` alone, so a
  stale reading admits nothing; the next probe that answers sets `reachable`
  again. A healthy connection is re-read every 315 s plus the probe's run, so
  it never reaches the bound.
* `NotFound` and `Conflict` while a Job is collected are expected: the probe
  pod gone between the pod list and the `pods/log` read, the Job gone before
  its TTL patch, and a status write that lost its `resourceVersion`
  precondition to a newer copy of the object (the watch cache delivers the new
  probe Job's events before the status the creating pass wrote) are debug
  lines and ordinary outcomes, never a WARN and never a reconcile error. A
  verdict write that lost the precondition is never followed by its TTL; the
  newer copy's own watch event reconciles again. Only those calls are
  answered that way: any other error, a `404` or a `409` from another call
  included, reaches `error_policy` and is a WARN
  (`KafkaCluster probe reconcile failed; requeueing`).
* A real crash, an unreadable probe log, a refused probe pod and a reading
  cleared as `ProbeStale` are logged at WARN **once per Job**, on the pass
  whose status write first recorded them.

Before this build PoC batch 2 measured `reachable` cleared for about 17 s on a
healthy connection whenever its probe Job was collected — long enough for a
`Restore` against it to be refused `ClusterNotReachable` — and about twelve
WARN lines per five-minute cadence for twelve connections. Nothing about the
objects' schema changes, so there is no migration step: the first pass over a
probe Job finished by an older controller (which carries no marker) judges it
once more, as that controller would have, and gives it the marker. A rollback
brings back the flap and the WARN lines, and drops the 630 s bound.

The pod is found by `batch.kubernetes.io/job-name=<job>`, falling back to the
legacy unprefixed `job-name=<job>` when that returns nothing — both are set on
1.29 and only the prefixed one is current — and the container is selected **by
name**, never by index, because an init container or a logging sidecar would put
an unrelated `exitCode: 0` at index 0.

### One surprise worth knowing: `refusal-reason=` is the last line of *stdout*, not of the pod log

`logweir backup run` prints `refusal-reason=<TerminalState>` as its **final
stdout line** for exit 3 — that is the CLI's contract and it holds. But a pod
log is **stdout and stderr merged in nondeterministic order**, and `backup run`
also writes the guard's full explanation to stderr. The controller therefore scans the final sixteen non-empty log lines and
matches by key name (`KEY_SCAN_TAIL_LINES = 16`). Missing evidence keys remain
unset; a missing exit-3 discriminator becomes `GuardRefusedUnknownReason`.

The window is a **budget with a stated margin**, not a guess. A passing restore
under execution contract v2 prints seven trailing lines (the summary,
`topic-preflight=`, `teardown-key=`, `scorecard-key=`, `sidecar-key=`,
`offset-report-key=`, and the `drill finished` line `tracing` emits in
production), so nine are held in reserve. Because the scan matches by key name
and takes the **last** occurrence of each prefix, a wider window can only find
a key it would otherwise have missed — never a different one. Two tests keep
the two halves honest: `progress_channel.rs` measures what the runner actually
prints, and `backup_controller.rs` checks the window still fits.

### What a refused run says about why (FX-34)

A guard refusal used to leave one sentence on the object: "the runner exited 3
(guard-refused); the code was read from …". Which guard, and what to change,
was only in the pod log, and the pod goes with its Job. The terminal
condition's message now ends with the runner's own reason code and sentence.
Take a restore whose plan names a plain `http://` archive endpoint without
`allow_http: true`:

```
kubectl -n team-a get restore r1 -o jsonpath='{.status.conditions[?(@.type=="Failed")].message}'
the runner exited 3 (guard-refused); the code was read from status.containerStatuses[name=runner].state.terminated.exitCode; the runner's own reason, cleaned and bounded: GuardRefused: source.storage.endpoint is a plain http:// endpoint but source.storage.allow_http is false. The pinned engine (kafka-backup 0.22.0 and later) derives plaintext transport from an http:// endpoint whatever allow_http says, so it would dial the archive in the clear although the spec asked for no plaintext. Set allow_http: true to state plaintext explicitly, or use an https:// endpoint.
```

`status.progress.message` carries the same text, and the console's operation
page shows both. `status.exitReason` is unchanged: it stays the terminal state
off `refusal-reason=`. Both a `Restore` (a rehearsal's included) and a `Backup`
(a scheduled one included) do this.

**It is the runner's own words, and a pod log is untrusted text.** The runner
pod runs in the tenant's namespace, and its sentence repeats what the plan, the
broker and the archive said. So the controller takes one line, validates it,
and cleans it before it is stored:

- **One line, and only when it carries this Job's line token.** At every exit
  3 the runner's last two lines are
  `refusal-detail={"token":"…","code":"…","message":"…"}` and then
  `refusal-reason=`
  ([the line's contract](stability.md#refusal-detail-carries-a-guard-refusals-reason-code-and-sentence-fx-34)).
  The reason is shown only when that line carries the Job's own token. This
  is not caution for its own sake: whoever writes a plan can start a line in
  the runner pod's log. The runner escapes every line break in the error text
  it prints itself (PROD-15.1), but the Kafka client inside it writes its own
  lines to the same stderr, unescaped, and those can repeat a plan value that
  holds a line break (a bootstrap address, for one). A pod log is stdout and
  stderr merged into one stream, so nothing about where a line stands, or how
  well-formed it is, tells the runner's line from one the plan's author got
  into the log.
- **What the token is.** Each time the controller builds a `Restore`'s or a
  `Backup`'s Job it makes a fresh random value (160 bits from the operating
  system, written as 40 hex digits) and gives it to the runner as the last
  two arguments of the `runner` container, `--line-token <hex>`. It is made
  when the Job is built, from nothing the plan could know, and a plan is older
  than its Job, so text the plan chose cannot contain it. The controller reads
  it back off the Job's own pod template and compares it with the line's in
  constant time. A `refusal-detail=` line with no token, or with another
  one, is not read at all, wherever it stands; the last line that carries the
  Job's token is the runner's. It is an argument and not an environment
  variable because the engine the runner starts inherits the runner's
  environment and expands `${NAME}` in its configuration, and neither reaches
  an argument.
- **The token is not a credential, and it is still never shown.** Anyone who
  can read the Job can read it (`kubectl get job -o yaml`). It is in that
  pod template and in the runner's own log line and nowhere else: not in a
  status, a condition, an event, an annotation, the product API, the console,
  or a line the controller logs.
- **The code is one of a closed list, per kind.** It must be a code that kind
  of run can print; any other word is not shown, however code-shaped. For a
  `Restore`: `GuardRefused`, `CredentialNotRenderable`,
  `TargetTopicConfigRefused`, `PointInTimeByProducerTime`, `PlainWithoutTls`,
  `CredentialBindingMismatch`, `StorageRegionInvalid`, `AuthorizationInvalid`,
  `AuthorizationExpired`, `PointBindingMismatch`, `PointBindingSetMismatch`,
  `PointUntrusted`, `RehearsalScopeViolation`. For a `Backup`: `GuardRefused`,
  `CredentialNotRenderable`, `PlainWithoutTls`, `CredentialBindingMismatch`,
  `StorageRegionInvalid`, `ConsumerGroupSelectionTooLarge`,
  `ConsumerGroupIdInvalid`, `ConsumerGroupSelectedTwice`,
  `WorkloadIdentityNotInjected`. A refusal with no name of its own carries
  `GuardRefused`, and so does one whose sentence opens with a word that is
  not on its kind's list: the word stays in the sentence.
- **The two lines agree.** The state on the `refusal-reason=` line the runner
  wrote with the detail (the next one after it) is the detail's code or
  `GuardRefused`; a detail line whose state line says anything else is not
  shown.
- **The sentence** keeps printable ASCII and `§ – — … →`. A run of whitespace
  or control characters (a line break, a tab, an ANSI escape's `ESC`) becomes
  one space, and a run of anything else (a bidi override, a zero-width
  character, a letter of another script) becomes one `U+FFFD`. A URL loses its
  query string and its userinfo, and the credential shapes every relayed
  message is checked for are replaced by `[redacted]`. That check errs towards
  removing: a long unbroken name or path in the sentence can read `[redacted]`
  too. The sentence is then cut to 760 bytes, on a character boundary, and a
  cut sentence ends with `…`. That is the most that keeps the whole message
  inside the 1024 bytes `status.progress.message` and the product API allow
  it.
- **The read is bounded and made once.** For exit 3 the controller asks for
  the `runner` container's last 32 lines and at most 512 KiB, and stops reading
  at that many bytes whatever arrives. It reads on the pass that writes the
  terminal status; a terminal object's pod is never read again. No permission
  was added: it is the `pods/log` `get` the controller already holds.

**What you see when there is no reason to show:**

| The message ends with | It means |
|---|---|
| *(nothing after `…exitCode`)* | No line in the log carries this Job's line token. The Job has none (a controller from before this change built it, or the operating system gave the controller no random bytes when it built the Job, which the controller logs as one warning), or the runner printed its line without one, or printed none. Any `refusal-detail=` line that IS in the log was not written by this Job's runner with its token, and is not shown. The pod log, while it exists, has the sentence. |
| ``; the runner gave no readable reason: its `refusal-detail=` line did not validate, so nothing from it is shown`` | A line carried the Job's token, so the runner wrote it, and it was not something this controller can show: a code that is not on that kind's list (a runner newer than the controller), nothing printable in its sentence, or a `refusal-reason=` line after it naming a state it could not have been printed with. |
| `; the runner's reason could not be read because the pod is gone` | The pod was already collected when the controller read its log (a `404`). The exit code was read before that and is recorded. |
| `; the runner's reason could not be read: the pod log read answered HTTP 403` (or `500`, …) | The read was refused or failed. The controller logs one warning naming the pod and the status, and does not read again. |
| `; the runner's reason could not be read: the last 32 lines of the pod log are over the 512 KiB this controller reads` | Possible only on a runtime that stores log lines longer than CRI's default 16 KiB. Nothing is taken from a log that was cut, the terminal state included (`exitReason: GuardRefusedUnknownReason`). |

In the last three cases the object is still terminal with `exitCode: 3`: a log
that cannot be read no longer leaves a refused run in `Running` with the
reconcile failing, which is what a `403` on `pods/log` used to do. Only exit 3
changed. Every other exit code reads the log as it always did, and a failure
of that read is still a reconcile error.

`refusal-reason=`'s own value reaches `status.exitReason` only when it is
shaped like a state name (ASCII letters and digits, 64 bytes); anything else is
`GuardRefusedUnknownReason`. It is still read the way it always was, as the
last such line in the final sixteen non-empty lines, and its list of states is
not closed, so a newer runner's state arrives.

**What the sentence can still hold.** It is the runner's sentence, and a
refusal names what it refused: a topic, a field, a cluster id. Those are words
the plan's author chose, and they are in the message, cleaned and inside the
760 bytes. Treat the text after "the runner's own reason" as a description of
that one object written partly by whoever wrote its plan, not as a statement
by the platform.

**What the token does not cover.** It separates the runner's line from text
written before the Job existed, which is every plan. Anyone who can read the
Job can read its token, so text that is produced after the Job is built, and
that reaches the pod log with a line break intact, could in principle carry
it. The runner escapes line breaks in every error text it prints itself
(PROD-15.1), which leaves what it does not print: the Kafka client's own
lines on stderr, which can repeat what a broker sends. No plan can.

#### The runner image must be at least as new as the controller

This controller passes `--line-token` to every `Restore` and `Backup` Job it
creates, and a runner that does not know the flag stops while it parses its
arguments, before any work. That is **every runner image published before
this change**, the images published from `main` since `v0.2.0-rc.1` included:
not only an older release. (No tagged release's runner loses a working run to
this. Since release-notes item 35, a runner image published before PROD-00.2
declares no engine, the controller gives it none, and its runs already stop
at exit 1 before the engine starts.)

**One `helm upgrade` of the packaged chart cannot produce that pair.** The
chart renders both images into ONE Deployment: `controllerImage` is the
controller container's image, and `runnerImage` is the `LOGWEIR_RUNNER_IMAGE`
that controller gives every Job. The packaged chart pins both to one
publication, so one upgrade moves both in one rollout. A controller with this
change over a runner without it can still occur in three ways:

- **Two pinned tags, one moved.** `controllerImage` and `runnerImage` are
  each set to a tag (the `sha-<commit>` tags CI publishes, for example), and
  an upgrade moves the first and leaves the second.
- **The source chart's floating defaults.** `charts/logweir/values.yaml`
  names `…/weirkeeper:latest` and `…/logweir:latest`. They are two tags, moved
  one after the other and pulled at different moments: the controller's when
  its pod starts, the runner's when each Job's pod starts, from whatever
  registry or mirror that node pulls from and under `runnerImagePullPolicy`.
  CI moves the runner's tag before the controller's, so a direct pull under
  the default `Always` gets a runner at least as new. A mirror that copies
  the controller first, or a node that holds an older `logweir:latest` under
  `IfNotPresent`, gets the mixed pair.
- **`runnerImage` pinned apart from the controller:** a mirror, an air-gapped
  registry, a pin left from an earlier incident.

**What it looks like.** Every `Restore` and `Backup` the controller starts
ends `Failed`, with `exitCode: 1`, `exitReason: operational` and the message
every failed run carries ("the runner exited 1 (operational); the code was
read from …"). Nothing on the object names the cause, and a broker outage
writes the same status. The runner pod's log names it for as long as the pod
exists. Its first line is

```
error: unexpected argument '--line-token' found
```

and the command's usage line follows. A `BackupSchedule` with a retry policy
retries the slot, because exit 1 is retryable, and each attempt fails the same
way. Nothing ran, nothing was signed and nothing was written.

**What to do.** Set `runnerImage` to the image published from the same build
as `controllerImage`, in one `helm upgrade`. The next Job starts. The
controller does not look for that log line and has no state that names a
mixed pair.

**Rolling back: both together, or the controller first.** An older controller
passes no token and ignores the new line, so it runs over this runner as it
always did; a newer runner given no token prints its line without one. The
runner image first is the mixed pair above. A Job created before an upgrade
has no token and runs as it did.

### What a run says about itself while it is running — `status.progress`

Before PLAT-14.1 a run in flight said `phase: Running` and nothing else, which
is the same answer for a healthy capture and for a pod that has been unable to
pull its image for four minutes. `status.progress` is the difference, and it is
**additive**: it is absent on any object an older controller reconciled, which
is the documented absent-field behaviour and not a degraded state.

| Field | What it says |
|---|---|
| `stage` | `Admission`, `Queued`, `Preparing`, `Running`, `Verifying` or `Finished` |
| `reason`, `message` | the current `RunnerReady` reason, and a sanitized explanation |
| `lastTransitionTime` | moves only when `stage` or `reason` moves |
| `lastObservedTime` | "still being watched", rewritten at most once per 60 s, and cleared with an explicit `null` once the stage is `Finished` |
| `runner` | the one owned pod: its name, phase, whether it is scheduled, the container state, and the kubelet's own waiting reason **verbatim** |
| `runnerPhase` | the runner's own phase, from its `progress-phase=<n>:<name>` lines |
| `diagnostics[]` | at most eight, newest first, deduplicated on `(code, object.kind, object.name)` |

**`diagnostics[].count` counts minutes, not occurrences.** It moves with
`lastSeen`, and `lastSeen` moves at most once per 60 s — so `count: 7` means
"this has been true for about seven minutes", not "this happened seven times".
The alternative would be to increment it once per 15-second reconcile, which
would make every pass over a diagnosing object a status write and lose E11(d)'s
"zero patches between heartbeats" for exactly the objects an operator is
watching. The CRD field's own description still reads "How many times" and is
owed a correction by the shapes owner, together with `RunnerPhase.contract`.

Two timestamps and not one clock read per pass: a reconciler's own status patch
is what wakes it, so a field carrying a fresh instant every pass would make
every pass a write. A run whose state has not changed issues **zero** API
writes between heartbeats.

### `RunnerReady`, and the four states a run can reach with no exit code

The `RunnerReady` condition is `False` while the runner container cannot start,
with one of six reasons — `WaitingForPod`, `PodUnschedulable`,
`VolumeMountFailed`, `CredentialReferenceMissing`, `RunnerImageUnavailable`,
`PodCreationForbidden` — and `True` with reason `RunnerStarted` once the
container has been seen running or terminated.

The diagnostic code and the condition reason are **two vocabularies on
purpose**. `diagnostics[].code` is the most specific thing known
(`CredentialSecretNotFound` means create a Secret; `CredentialSecretKeyMissing`
means add a key to one that exists); the condition reason is its class, because
a `metav1` reason is a closed label other software matches on. The parameters —
which Secret, which volume — travel in the diagnostic's `object` and `message`.

Four of those reasons are also terminal states: `VolumeMountFailed`,
`CredentialReferenceMissing`, `RunnerImageUnavailable`, `PodCreationForbidden`
(and `PodUnschedulable` already was one). They replace `NoExitCode` **only**
when the matching diagnostic was recorded before the Job ended; otherwise the
table above is unchanged, and `exitCode` stays absent in all of them. A run
whose pod never started has no code to lift, and none is invented.

**A warning-class diagnostic that ended the run is its terminal reason.** When
a Job hits its deadline — its own, or the one fail-fast collapses — the Job
controller deletes its pod, so the pod-reading table above has nothing to read
and the recorded diagnostic is the only witness. An `Error`-class code
(`CredentialSecretNotFound`, `SigningKeyMissing`, …) is read whenever it was
recorded, because it does not resolve on its own. A `Warning`-class code
(`VolumeMountFailed` from a volume that never mounted, `PodUnschedulable`,
`RunnerImagePullFailed`) is read only when the runner **never started** and the
diagnostic was **still being observed when the Job ended** — its `lastSeen`
within three minutes of the Job's `Failed` transition — because a warning that
stopped being seen had resolved. So a projected ConfigMap that never mounts
ends `Failed/VolumeMountFailed` and a pod no node takes ends
`Failed/PodUnschedulable`, not `NoExitCode`; anything less certain stays
`NoExitCode`. `WaitingForPod` — what the pass after fail-fast deleted the pod
records — is never a terminal state.

### Failing fast, and what it costs

When a **non-transient** diagnostic has held continuously for
`LOGWEIR_FAIL_FAST_SECONDS` (default 300, floor 60) and the runner container has
**never** started, the controller collapses the Job's `activeDeadlineSeconds`
rather than waiting for it. The Job fails with `DeadlineExceeded`, the existing
crashed-Job path runs, and the terminal reason is the recorded diagnostic. No
data-plane process ever started, so no approval, plan or archive state was
consumed; a new attempt is a new `Backup` or `Restore`, never a mutation of the
old one.

`PodUnschedulable` is **never** failed fast, and that is a decision: a node can
join a cluster, a pod can be preempted, and a cluster autoscaler exists. It is
reported and left to the Job's own deadline. The same holds for
`RunnerImagePullFailed`.

**This behaviour is on by default, and here is how to change it.** Set
`LOGWEIR_FAIL_FAST_SECONDS` to a larger number of seconds to wait longer, or to
**`0` to switch fail-fast off entirely** — every Job then runs to its own
`activeDeadlineSeconds`, exactly as before this behaviour existed. `0` means
*never*, not *immediately*; a value between 1 and 59 is raised to the 60-second
floor. The case this lever is for is a cluster where an external controller
(external-secrets, a vault injector) materialises a Secret a few minutes behind
the Job, where an otherwise healthy run would be cancelled at the default.

> **Both are controller environment variables, and the chart sets them.**
> `controller.failFastSeconds` and `controller.jobTtlSeconds` in
> `charts/logweir/values.yaml` render `LOGWEIR_FAIL_FAST_SECONDS` and
> `LOGWEIR_JOB_TTL_SECONDS` on the `weirkeeper` Deployment when they are not
> empty ([chart reference](../charts/logweir/README.md)); on the low-level path
> set the variables on the Deployment directly.

### Diagnostics are derived from Events, which are best effort

Kubernetes Events are rotated and rate limited. An absent event yields a
**weaker** code (`WaitingForPod`) and never an invented cause. Events are listed
only while the pod is not running, by `involvedObject.uid` and never by name —
`FailedCreate` is a common event in a namespace with a `ResourceQuota`, and a
controller that took the first one it saw would cancel a healthy Job because of
somebody else's workload.

### `status.records` and `status.capture` come from the verified receipt

`Backup.status.records` is the sum of the signed receipt's per-topic counts, and
`status.capture.{startedAt,finishedAt}` are copied verbatim from the same
document. Both are written on the verification patch and **only** when the
verdict is `Valid`: a count on a `Backup` has to be one some key this
installation accepts attested to, not a number anybody who can write to the
bucket chose. The per-topic breakdown stays in the receipt, where it is
attested; a status is not a second copy of a signed document.

**What happens to them when trust is withdrawn, decided rather than inherited.**
A `TrustPolicy` change can re-evaluate a stored verdict from `Valid` to
`Untrusted` (§7.4) without re-fetching anything. `status.records` and
`status.capture` are **left in place** by that pass, for the same reason
`exitCode`, `outcome` and `windowCovered` are: they are a record of what was
observed and attested *at the time*, and a re-trust pass changes no run fact.
Rewriting history to match a policy that changed afterwards would destroy the
evidence an operator needs to work out what was relied upon and when.

The consequence is one a console must not get wrong: **a record count is only
ever as trustworthy as the verification badge beside it.** The RECORDS column
means "this many records, attested by a key that was accepted when the run
finished" — never "by a key this installation accepts now". Any surface that
renders the count without the badge is misrepresenting it, which is the same
rule §8 already applies to a `succeeded` result with `notAttempted` evidence.

### Completed Job cleanup, and its repair

The order is unchanged and load bearing: **terminal status first, TTL second**.
A status patch that did not return 200 leaves the reconcile before any TTL
exists, so pod garbage collection cannot start on a run whose exit code was
never recorded. The TTL value is `LOGWEIR_JOB_TTL_SECONDS` (default 604800,
floor 3600).

If that second patch failed, the run is already terminal and no later pass
re-reads its pod — so nothing would ever set the TTL and the finished Job would
sit in the namespace's quota for ever. A pass over a terminal object therefore
patches the TTL when, and only when, the Job is finished, carries exactly this
object's controller owner, and has none. It reads no pod and writes no status.

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
it runs these six checks, in this order:

| # | Check | `reason`, and what happens |
|---|---|---|
| 0 | The object's own name is at most 63 characters | `NameTooLong`, terminal. Nothing is created |
| 0b | `spec.runnerResources`, when set, is a block the controller applies (*The runner's requests and limits*, below) | `ExecutionSpecInvalid`, **terminal**, naming every refused field. Nothing is created, and the `Approval` is not read |
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
| `approval` | immutable ConfigMap `<restore>-approval-bundle` | `/approval` | `approval.json`, `approval.sig`, `approver.pub.pem`, `allowed-clusters.json` — a standing-authorized rehearsal projects five DIFFERENT members and neither `approval.*`; see §7g |
| `signing` | Secret `logweir-signing-key`, `0440` | `/signing` | the runner's own signing key, readable only because `fsGroup: 65532` is set |
| `plan` | ConfigMap `<name>-plan` | `/plan` | `spec.planBytes`, verbatim |
| `work` | `emptyDir` | `/work` | the scorecard, the offset report and the checkpoint state, on a pod whose root filesystem is read-only |

The Job template pins the SHA-256 of the plan and every approval-bundle member,
including `allowed-clusters.json`, plus the Restore and Approval identities.
New Jobs also pass `--execution-contract-version 2`, matching
`LOGWEIR_EXECUTION_CONTRACT_VERSION=2` in the immutable pod template (v1 is
accepted only for already-created legacy Restores — `docs/stability.md`,
*Execution contract v2*). The
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

**The hazard is live once anyone has minted a standing authorization.** The
rehearsal controller exists (§7g) and an operator creates such an `Approval` by
hand from `logweir drill approve --standing`. An installation that never created
one can apply these CRDs and roll the controller back unaffected; one that did
must follow the order below. (An earlier revision of this section said no
component created one and the rehearsal controller did not exist; that stopped
being true with PLAT-14.3.)

**The rollback order:**

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

### The runner's requests and limits: `spec.runnerResources` (FX-2)

`spec.runnerResources` is what the runner pod asks for and is capped at. It
reaches the one `runner` container's `resources` **exactly as written** —
requests as requests, limits as limits, in the spelling the object carries (the
API server stores the canonical form, so `0.5` reads back as `500m`). Absent,
or with no quantity in it, the container states no `resources` at all, which is
the Job every `Restore` had before FX-2, and the namespace's `LimitRange`
defaults apply unchanged. A `RehearsalSchedule`'s `spec.bounds.runnerResources`
is copied onto each child `Restore` verbatim and arrives the same way (§7g).
The console never sets the field; it is set with `kubectl` or by a
`RehearsalSchedule`. Neither the console nor the product API shows it yet, so an
approver does not see it beside the plan: read it with the `kubectl` line at the
end of this section. Showing it on the `Restore` view is owed to PROD-10.1,
which exposes the control.

Before FX-2 the field was accepted, documented and **dropped**: the container
carried no `resources` whatever the object said, so every runner pod was
`BestEffort`.

**The decision: apply it, and refuse rather than clamp.** D3 §4.5 designed the
field and the CRD documented it. Withdrawing it would have left every runner
pod `BestEffort` — the first pod the kubelet evicts under node pressure, and a
restore evicted mid-write leaves a half-written target — and unable to run at
all in a namespace whose `ResourceQuota` requires limits. A clamped value would
be a Job nobody asked for, and a run OOM-killed at a limit the controller chose
would read as a runner defect, so a value the controller will not apply is
refused, never adjusted. Every quantity is checked before anything else about
the object is read:

| Rule | Refused, for example |
|---|---|
| a Kubernetes quantity in the grammar the schema's pattern admits | `abc`; `1K` (the decimal kilo is `k`) |
| memory is a whole number of bytes, CPU a whole number of millicores | `memory: 100m` (a tenth of a byte — `Mi` was meant); `cpu: 100u` |
| nothing above the ceiling, requests included: **4** CPUs and **8Gi** of memory | `limits.memory: 16Gi`; `requests.cpu: "8"`; `cpu: 5Gi` |
| a limit is a cap: never zero, and a memory limit is at least **32Mi** | `limits.cpu: "0"` (a runtime reads zero as "no limit"); `limits.memory: "512"` (512 bytes) |
| a request is at most its limit, per resource | `requests.memory: 4Gi` beside `limits.memory: 2Gi` |

The ceilings are D3 §4.1's and are compiled in: no chart value configures them,
and the schema cannot state them because comparing quantities in CEL needs a
library the 1.29 floor cannot be relied on to have. The memory floor is not a
measured minimum for a working run (PROD-10.1 measures that); it stops a
missing unit before it becomes a pod the runtime cannot create. A `Restore`
that breaks any rule ends `phase: Failed` with `reason: ExecutionSpecInvalid`
(`Failed=True`, same reason) **before its approval is read, a manual-run pool
slot is taken, or anything is created**, and the message names every refused
field at once, because `spec` is immutable and the remedy is a new `Restore`. A
`RehearsalSchedule` skips each slot as `AuthorizationInvalid` instead and
creates no child (§7g).

**`LimitRange`, `ResourceQuota` and the scheduler decide the rest, and each
answer lands on the `Restore`.** The Job is created with the values above; what
happens next is the namespace's:

- a `LimitRange` fills in what the block leaves out. With only
  `requests.memory` set its default limit applies, and a default below the
  request gets the pod rejected;
- a `LimitRange` minimum or maximum, or a `ResourceQuota` (including one that
  requires every pod to state limits), rejects the pod at creation. The Job
  controller's only trace is a `FailedCreate` event on the Job, and the
  `Restore` reports it: `RunnerReady=False` with reason `PodCreationForbidden`,
  the same value in `status.reason` (the `REASON` column), and a
  `status.progress.diagnostics[]` entry `PodCreateRejected` carrying the
  admission's own words (`exceeded quota: …`). Once that has held for
  `failFastSeconds` (§10, *Failing fast, and what it costs*) the Job's deadline
  is collapsed and the run ends `PodCreationForbidden` — never a Job silently
  waiting out its deadline;
- requests no node can hold leave the pod `Pending`: `RunnerReady=False` with
  `PodUnschedulable`, left to `activeDeadlineSeconds` because a node can still
  join.

A namespace whose `ResourceQuota` covers compute and which has no `LimitRange`
defaults refuses every pod that states no limits, and `spec.runnerResources` is
how a `Restore` runs there. `Backup`, check, probe, delivery and retention Jobs
have no such field and state no resources; their requests and limits come from
the namespace's `LimitRange` or not at all. When such a Job's pod is rejected
at creation, every controller that owns the Job reads its `FailedCreate` event
(FX-11) and reports the rejection promptly, in the admission's own words: a
check kind with `PodCreateRejected`, a runner kind with `PodCreationForbidden`.
The same holds for a pod refused because its ServiceAccount does not exist
(the classifier's `RunnerServiceAccountMissing`):

| The Job | What the object reports for a pod rejected at creation |
|---|---|
| A `Backup`'s runner, and a `Restore`'s (with a block or without) | `RunnerReady=False` / `PodCreationForbidden`, a `PodCreateRejected` diagnostic quoting the admission, then terminal `PodCreationForbidden` after `failFastSeconds` |
| A `Preflight`, and a run's evidence-fetch Job | `PodCreateRejected` once the Job has had no pod for 30 seconds, and the Job is cancelled |
| A `TopicDiscovery` | `Failed` / `PodCreateRejected` once the Job has had no pod for 30 seconds, quoting the admission, and the Job is cancelled |
| A `RecoveryCatalog` sync | `Synced` names `PodCreateRejected` once the Job has had no pod for 30 seconds and the Job is cancelled; the harvest that follows is `Synced=False` / `PodCreateRejected` with `lastSyncJob.refusalReason: PodCreateRejected`, and the published view is kept |
| A dynamic `Backup`'s topic discovery | Once the discovery Job has had no pod for 30 seconds the Job is cancelled and the `Backup` ends `Failed` / `PodCreationForbidden` (also on `TopicsResolved`), quoting the admission; no runner Job is created |
| A `KafkaCluster` probe | `Reachable=Unknown` / `PodCreationForbidden` (also in `status.reason`) once the Job has had no pod for 30 seconds, quoting the admission; the Job is cancelled, `reachable` is cleared, `clusterId` and `observedAt` keep their last values (a refused probe observed nothing, so the last look's record ages and goes stale as usual), and the finished Job gets the usual five-minute TTL, so the next probe runs on the ordinary cadence and clears the reason once the namespace admits the pod |
| A `ProtectionPolicy` delivery | Once the delivery Job has had no pod for 30 seconds the Job is cancelled and the attempt is recorded `Failed`, `lastError` naming `PodCreationForbidden` and the admission, with `NotificationsDelivered=False` / `DeliveryFailed`; the ordinary backoff retries it, three attempts in all |
| A `RetentionPolicy` enforcement run | Once the Job has had no pod for 30 seconds the Job is cancelled and `Enforced=False` / `PodCreationForbidden` quotes the admission, "nothing was deleted". The run is recorded when the cancelled Job has finished, about a second later, and its lease is held until then. It counts as a failed run, so three in a row turn `EnforcementDegraded=True` |

**What it costs, and what it needs.** The read is one `list` of core `events`
with an `involvedObject.uid=<uid>` field selector and `limit=20`, made only
while the classifier could use an Event: no pod after the 30-second grace, or a
volume still mounting after 60 seconds. A Job whose pod exists and runs costs
nothing, and the probe, delivery and retention controllers do not even list
pods for a running Job until its own status shows it has none (`active`,
`succeeded` and `failed` all zero) past the grace. The grant is the `list` on
`events` the `weirkeeper` role already holds, bound in every namespace the
controller acts in under both binding modes (§`controller.watchNamespaces` in
the chart reference). The read is best effort: a list that fails leaves the
object on its previous path (`PodNotStarted`, `ProbeRunning`, `NoExitCode`, a
delivery with no exit code, `RunFailed`) rather than inventing a cause.

**What binds it.** `spec.runnerResources` is not part of `planBytes` and not a
member of the approval bundle, so a per-run approver signs the data operation
and not its container bounds — the same position as a per-run `Restore`'s
`deadlineSeconds`. `spec` is immutable, so the value cannot change after the
object is created, and a Job's pod template is immutable too: nothing changes it
after admission. For a rehearsal the value is inside the `RehearsalSchedule`'s
sealed spec, whose digest (`templateDigest`) the standing authorization signs,
so a different value is a different schedule under a different authorization.

**That binds the schedule, not every `Restore` the authorization admits.** The
signed `RehearsalScope` carries no `runnerResources`. The `Restore` controller's
standing admission checks a `Restore`'s plan and its `deadlineSeconds` against
the signed scope, and the schedule's digest is out of its reach. So a standing
`Restore` written by hand (anyone with `create` on `restores` may name the
authorization) is held only to the controller's compiled-in bounds above, not to
the schedule's sealed value; the `Restore`s the schedule creates carry that value
verbatim. Carrying `runnerResources` in the signed `RehearsalScope`, a versioned
change to a signed document, is owed to PROD-10.1.

**Upgrade and rollback.** The CRD change is descriptions only; the field and its
pattern have been served since the D3 W0 schemas. Before the upgrade, list the
objects it changes, those that already carry a block, with the inventory in
[the release notes](release-notes.md), item 21.

- A `Restore` that already has a Job keeps it: the pod template is immutable,
  and the upgraded controller observes the run and never refuses it
  mid-flight. That pod has no `resources`.
- A terminal `Restore` is untouched.
- A `Restore` with no Job yet (held for approval, queued, or created during
  the upgrade) is checked on its next pass. A valid block gives it a Job whose
  container carries it, which is a behaviour change: the pod now asks for, and
  is capped at, what the object said, so it can meet a quota or an OOM limit it
  never met before. A block outside the bounds ends it
  `Failed`/`ExecutionSpecInvalid`.
- A `RehearsalSchedule` with the block set applies it from its next slot, or
  skips every slot as `AuthorizationInvalid` when it is outside the bounds.
- **Rolling back** to a controller from before FX-2 ignores the field again:
  the Jobs it creates carry no `resources`. A `Restore` this build refused stays
  `Failed` (no controller acts on a terminal `Restore`); a schedule's next slot
  fires under the older controller and drops the block, as it always did. A
  schedule this build skipped for its block therefore runs again, uncapped:
  suspend it first (`spec.suspend`, its one mutable field) if it must not.
  Nothing has to be deleted in either direction.

What a runner Job actually carries:

```bash
kubectl --context "$LOGWEIR_CONTEXT" -n <namespace> get job <restore-name> \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="runner")].resources}'
```

A Job whose `Restore` states no `runnerResources` prints `{}`, not an empty
line: the API server stores the container's `resources` as an empty object, so
`{}` means no requests and no limits (measured on the PoC on 2026-10-08, for a
Job from this build and from the controller before FX-2 alike). A Job whose
`Restore` states the block prints it, for example
`{"limits":{"memory":"512Mi"},"requests":{"cpu":"250m"}}`.

### Complete coverage: `spec.coverage` and `spec.completeMaxRecords` (PROD-08.1a)

**The field.** `spec.coverage` is `sampled` (absent means this) or `complete`;
`spec.completeMaxRecords` (at least 1, CEL: only beside `complete`) bounds a
complete verification. They **declare** what the plan's `sample.coverage` and
`sample.complete_max_records` say, so a list, the `COVERAGE` printer column's
neighbours and the console can read the requested coverage without parsing the
plan. The plan is what the approver signs and what the runner executes; the
controller compares the two before an approval is waited for or anything is
created, and a `Restore` whose declaration the plan does not say — a complete
declaration over a sampled plan, a complete plan with no declaration, or a
different bound — ends `Failed` with reason **`ExecutionSpecInvalid`**, naming
both, with no Job. Absent and a sampled plan is every `Restore` written before
the fields existed, so they all agree, and their objects and plan hashes are
unchanged. `spec` is immutable and the approval binds the plan bytes, so the
coverage cannot change after approval: a different coverage is a different
plan, a different hash and a new approval. The console's restore wizard and
the product API's create route set both fields from the same choice
([api.md](api.md#the-restores-coverage-prod-081a)); a plan written with the CLI
needs `spec.coverage: complete` on the `Restore` too.

**What complete costs.** It reads every archived segment of every partition of
every restored topic, decodes every record, reads every restored record back
and compares each one with the archive by its source offset — exact
per-partition counts of missing, unexpected, duplicate, out-of-order and
different records. Measured on one laptop with an optimised build: about a
minute per GiB of one-KiB records, against about five seconds for the sampled
check; a real archive pays more for object-store transfer
([the decision record](to-do/decisions/PROD-08.1-integrity-contract.md) §7).
Size `deadlineSeconds` for it; `completeMaxRecords` is the control that stops
it early.

**What `covered: false` means.** A complete verification that the bound stopped,
or that met an archive it could not compare (records with no lineage header, a
segment that would not decode), compares only part of the restore. It signs
`integrity.verification.complete.covered: false` with the reason, its
`integrity.result` is never `pass` (scorecard arm IV-6), its outcome is
`fail-integrity` and the runner exits 2. **It is never a pass, anywhere:** the
`Verified` condition's reason is `CompleteNotCovered` (§15.2; the badge rule
reads `covered` before the outcome, so a real one is named by this reason and
not by `OutcomeNotPass`, and so is a status whose other fields said pass), the
product API's `verifiedSuccess` is `false`, the console's badge and list
verdict say so, a rehearsal over it is `lastFailed`, the runner's notification
body carries `integrity.covered: false` and its metrics
`logweir_drill_integrity_complete_covered 0`. Alert on either: the condition's
reason `CompleteNotCovered`, or the gauge
`logweir_drill_integrity_complete_covered == 0` in the runner's metrics
textfile ([metrics.md](metrics.md)).

**What the status carries.** Beside `integrity.level`/`result`/`partialReason`,
the controller copies from the signed scorecard: `integrity.coverage`
(`sampled` or `complete`; ABSENT means not recorded — a scorecard before format
1.4.0 — and is read as sampled, never as complete); `integrity.complete` —
`covered`, `incompleteReason`, `maxRecords`, the archive counts
(`segments`, `segmentsVerified`, `segmentsFailedCount`,
`segmentsUnverifiedCount`, `recordsDecoded`, `offsetHoles`), the totals under
`replay` and, up to 256 partitions, one row per partition with its own exact
counts and `compared` (past 256 the rows are omitted and `partitionCount` says
how many the scorecard holds — a truncated list would be a claim the signed
document does not make); and FX-23's `integrity.unsampledTopics` for a sampled
check (format 1.6.0). Each is all of it or nothing, like `timeBasis`.

**A narrowed restore says so (PROD-11.1b).** When the signed scorecard records
a replay selection — a partition subset (format 2.0.0) or a window from a
stated start (format 1.7.0) — the controller copies it to
`integrity.selection`: `scope: partial` (the marker; the `SELECTION` printer
column, appended after `AGE` so no column moves), `windowStartMs` (a stated
start only), `windowEndMs`, `narrowedTopics`, `partitions` (each narrowed
topic and its selected partitions; up to 256 topics and 1024 partitions in one,
past which the rows are omitted and `narrowedTopics` still says how many) and
`engineRuns`. Every verdict beside it is the selection's: `complete.covered:
true` over a subset means every record of every *selected* partition was
compared, and no other partition of a narrowed topic was restored. A block
the controller cannot read is still copied as `{scope: partial}` — never read
as a restore of everything. **Absent `selection` is an unnarrowed restore,
exactly as before.** The schema change is additive.

```bash
kubectl --context docker-desktop get restore r1 \
  -o jsonpath='{.status.integrity.selection.scope}{"  "}{.status.integrity.selection.partitions}'
# partial  [{"partitions":[0,2],"topic":"orders"}]
```

```bash
kubectl --context docker-desktop get restore r1 \
  -o jsonpath='{.spec.coverage}{"  "}{.status.integrity.coverage}{"  "}{.status.integrity.complete.covered}'
# complete  complete  false
```

The `COVERAGE` printer column reads `status.integrity.coverage`: empty is not
recorded, never complete.

**Upgrade and rollback.** The fields are additive. An older controller ignores
`spec.coverage` (the structural schema of an older CRD prunes it) and runs the
plan as written — a complete plan still verifies completely, because the
runner reads the plan — but it does not refuse a disagreeing declaration and
does not copy `status.integrity.coverage`/`complete`; the console then reads
"not recorded". Rolling the CRDs back prunes both spec fields from stored
objects; their plans are untouched.

### Restoring under the original topic names (PROD-15.1)

**What it is.** Every other restore writes a NEW topic beside the old one
(`restore-<instant>-orders`), so applications must move to a prefixed name. A
restore under the ORIGINAL topic names writes `orders` itself — the recovery
of a deleted topic, or of a lost cluster onto a replacement, without renaming
anything. The owner's decision OD-2 (2026-10-05) allows exactly one such path
and keeps the rest of `docs/stability.md`'s Never #1: Logweir never writes into
a LIVE topic. It writes only a topic that does not exist, which it creates
itself, exclusively, and the restored topic is a NEW generation of the name
(Kafka assigns a new topic id; none can be preserved), never the original
topic.

**The plan** (the approver signs it, the runner executes it):

```yaml
target:
  mode: newTopic                 # required: the identity ban stays in scratch mode
  topic_naming:
    prefix: ""                   # the identity mapping; an older runner refuses it
    original_name:               # the explicit opt-in; an empty prefix without it is refused
      owners: []                 # the approver's statement: no declarative owner of any restored name
      # owner_path: true         # restore although an owner is found (see below)
  topic_mapping_prefix: drill-   # still required: the LogAppendTime probe is created under it
sample:
  coverage: complete             # required: an original-name restore is never verified by sample
```

**The `Restore`** declares it: `spec.target.topicNaming: {prefix: "",
originalName: true}` in `newTopic` mode, with `spec.coverage: complete` (two
CEL rules refuse the declaration anywhere else, and without complete
coverage). The console and the product API show such a restore — and the
approval it needs — distinctly, and the controller refuses, before any Job,
an object whose declaration and plan disagree (`ExecutionSpecInvalid`, the
rule `spec.coverage` follows above).

**Its own approval subject.** The approval document carries a separate,
signed approval subject, `originalName` (v1: `approval_subject`, minted by
`logweir drill approve --approval-subject original-name`, which reads the plan
and refuses a subject that is not the plan's; v2: `approvalSubject`, which the
console signs only for a `Restore` that declares `originalName`). An ordinary
approval never authorises an original-name restore, and an `originalName`
approval authorises nothing else: the controller refuses the pair terminally
before any Job (`ApprovalSubjectMismatch`, "whose signed approval subject is
not the one this Restore's plan needs"), and the runner refuses it again
before it reads the archive or dials the target (exit 3). A standing rehearsal
authorization never authorises one.

**One person confirms only with the names typed — the owner's decision OD-10
(2026-10-09).** In a namespace confirmed by one person (`confirm`, internal
`Ordinary`; the fresh-install default), the requester may confirm an
original-name restore alone, but the console first asks them to RE-TYPE every
original topic name, exactly (one per line; case and spelling as the plan
names them), and signs what was typed into the authorization document
(`originalNameConfirmation.typedTopics`, beside `approvalSubject`). The
product API refuses the request before anything is created when the names
are missing (`typed_topics_required`) or are not exactly the plan's
(`typed_topics_mismatch`, naming what is missing, extra or repeated), and
typed names anywhere else (`not_accepted`). The controller refuses the
Approval at admission (`ApprovalSubjectMismatch`, the message naming
`OriginalNameConfirmationMissing` or `…Mismatch`) and the runner refuses it
again (exit 3). The signed scorecard records `confirmation: typedTopicNames`,
and the approvals page lists the typed names. **A `strict` namespace still
needs the second person:** the requester's submission stores only the
console's confirmation (`<approvalRef>-confirmation`), which authorises
nothing until an approver countersigns it; typed names are refused there and
replace nobody. A v1 approval (`logweir drill approve --approval-subject
original-name`) is an approver's personal key, a second person too.

**What the runner proves before anything is written** (exit 3,
`refusal-reason=GuardRefused`, the message opening with the condition's name;
nothing created, nothing deleted):

| condition | refused as |
|---|---|
| `newTopic` mode and `prefix: ""` beside the block | `OriginalNameNotNewTopic`, `OriginalNamePrefixNotEmpty` |
| the plan asks for complete verification, `sample.coverage: complete` (see "Complete verification is required" below) | `OriginalNameNeedsCompleteCoverage`, before any broker is asked anything |
| the plan restores whole topics: it states no `restore.partitions` (see "Whole topics are required" below) | `OriginalNameNeedsWholeTopics`, before any broker is asked anything |
| every restored name is absent on the target | "already exists" (the refusal every restore gets) |
| the target is not the source cluster — the source cluster id the bound recovery point's verified receipt measured at backup differs from the target's — OR every broker reports `auto.create.topics.enable=false` (read from every broker with DescribeConfigs) | `OriginalNameAutoCreateEnabled`; `OriginalNameAutoCreateUnknown` when a broker does not report it (a refused read is exit 1). The source id counted is the bound point's VERIFIED receipt only: the allowlist file's `source_cluster_id` is unsigned runner input and never makes the target "another cluster". Brokers are the ones the cluster metadata lists at phase 0; one offline then is not read (the exclusive create below still refuses a name it creates) |
| somewhere was looked for a declarative owner (a Strimzi `KafkaTopic`, GitOps, Terraform) of a restored name, and none was found unless the plan chose the owner path; nothing the `KafkaTopic` resources file holds is dropped | `OriginalNameOwnerNotChecked`, `OriginalNameOwnerPresent`, `OriginalNameOwnersInvalid`, `OriginalNameOwnerUnreadable` (a `KafkaTopic` whose topic cannot be read, one whose `namespace/name` is longer than the 256 characters an owner is recorded with, or a file holding no `KafkaTopic` at all unless it is the explicit empty `List`) |
| the `LogAppendTime` probe (the one write phase 0 makes, only on a `LogAppendTime` broker) is created as `<topic_mapping_prefix>logweir-probe-<plan hash>`, never under an original name, and that name is free | `OriginalNameProbeUnusable` |

**Declarative owners: where the runner looks, and why it never assumes none.**
An owner recreates a deleted name on its own and reverts the restored topic's
settings — the pinned `retention.ms=-1` included — which can delete the
restored records. The runner cannot read Kubernetes or a repository, so it
looks in three places and REFUSES when it looked in neither of the first
two: the approved plan's
`original_name.owners` (an empty list is the approver's signed statement that
no owner exists; `{topic, kind: strimzi|external, reference}` names one); the
target's `KafkaTopic` resources given to `logweir restore run
--kafka-topic-resources <file>` (`kubectl get kafkatopics -A -o yaml`; CLI
only); and, for a target that may be the source cluster, the owners the bound
point's verified receipt recorded at backup time (PROD-05.1). **The receipt
adds owners and never stands in for looking:** an owner it names blocks, but
"the receipt found none" is not enough on its own, because a backup records a
`KafkaTopic` whose reference it cannot record as no owner. **The controller
does not list `KafkaTopic` resources** (child row PROD-05.1a, which needs a
`kafka.strimzi.io` grant), so a `Restore` relies on the plan's statement,
with the receipt's owners beside it. **The owner path** (`owner_path: true`) restores although an
owner is found: the approver states that the owner's reconciliation is paused
for the restore (`strimzi.io/pause-reconciliation: "true"` on the
`KafkaTopic`, and any GitOps sync that would revert that annotation
suspended), so it neither creates the name first nor reverts the topic during
the restore, and that it adopts the topic afterwards. Logweir still creates
the topic itself, exclusively; before unpausing, make the owner's desired
state keep the restored data (its `retention.ms`).

**Creation is exclusive, and a race loses by name.** The topics are created
after phase 5, with `CreateTopics`, which fails on a name that exists. A name
that appears after phase 0 — a producer on a cluster that auto-creates, an
operator, an owner — is looked for once more right before the create, and a
`TOPIC_ALREADY_EXISTS` answer is the same refusal: exit 1 (phases 0–5 have
run), the message opening `TargetTopicAppeared`, and nothing is written into a
topic this run did not create (both modes). A `CreateTopics` answer that does
not name exactly the topics asked is refused too (exit 1). **The Restore says
so:** the runner's last line is `failure-reason=TargetTopicAppeared`, which
the controller lifts onto `status.exitReason`, and the line before it names
the topics, which it copies to `status.targetTopicsAppeared` and into the
`Failed` condition's message.

**What a stopped creation step may have left: three lists.** Every stop of
the step names what it knows, in the list that says exactly that much, on
`status.targetTopicsAppeared`:

| list | what the run knows | what every surface says |
|---|---|---|
| `appeared` | a mapped name someone ELSE created after phase 0: the look before the create showed it, or `CreateTopics` answered `TOPIC_ALREADY_EXISTS` for it | created by someone else; the restore wrote nothing into it |
| `left` | a topic THIS run created: its own `CreateTopics` call answered success for that name | "created by this restore and left empty; remove it yourself once you have checked nothing writes to it" |
| `unconfirmed` | a name this run ASKED for and got NO DEFINITE ANSWER about (the whole `CreateTopics` call failed, the name got no answer, or it got an error that is not "already exists"), which the cluster LISTED when the run looked again | "exists now; this restore asked the cluster to create it and got no definite answer, so it may be this restore's or someone else's: check what it holds and who writes to it before you remove it" |

- **`unconfirmed` is never `left` and never `appeared`.** A client that gives
  up before the broker's answer arrives has no answer, and the broker may
  have applied the request; Kafka also goes on creating a topic whose request
  it answered `REQUEST_TIMED_OUT`. The run cannot say whose such a topic is,
  so it claims nothing: it lists the cluster once more (a read) and names
  each asked name that is there. When it cannot list the cluster either,
  `unconfirmedSeen` is `false`, EVERY name without a definite answer is
  listed, and the sentence says "may exist now … look for it".
- **A count beside each list** (`appearedCount`, `leftCount`,
  `unconfirmedCount`): a list carries at most 100 names, and the condition's
  message, the product API and the console say "and N more" when the bound
  cut it. Each further name is one of the restore's mapped target topics, and
  the runner's log names every one.
- **The broker's own bound is below the client's.** The runner asks
  `CreateTopics` with an operation timeout of 15 s inside its 20 s request
  timeout (librdkafka's default would be 60 s), so a reachable broker answers
  per name before the client gives up. That does not guarantee an answer (a
  lost connection still fails the whole call), and a name answered
  `REQUEST_TIMED_OUT` may still be created afterwards: both are what
  `unconfirmed` is for. **One residual:** the look is one read at one moment,
  so a topic the broker finishes creating AFTER it is not named. After any
  `Failed` Restore whose creation step did not answer, list the plan's target
  names on the cluster yourself.
- **`status.newTopics` is exactly `left`** after a stopped creation step: what
  the run's own answers say it created, never a name someone else created or
  one it cannot account for. It is absent when the lists could not be read.

**Where the names come from, and what that is worth.** The controller reads
them from the runner's pod log, and only (a) from its LAST TWO lines, in the
order the runner prints them (`target-topics-appeared=`, then
`failure-reason=`, each a restore's own closed state beside exit 1), (b) from
a line no longer than a genuine one (76 KB; a longer line is refused before
it is parsed, and the read itself asks for the last 256 lines), and (c)
keeping only names this Restore's own plan maps, with each count held to the
number of names the plan maps. A block with no such name is dropped, and the
condition then says the list could not be read. This build's runner prints
every error text on one line, so no string of a plan, a broker or an object
store can start a line of its log. **The names are shown only as the
runner's log gives them: during an upgrade with an older runner image, check
the list against the cluster before acting on it.** A runner built before
that escape prints an error's text raw, and an error that ends with a plan's
own string can end the log with a pair of lines the runner did not mean; the
controller cannot tell those from the runner's own, so it shows them, held to
the plan's names as above. Logweir itself never deletes a topic on the
strength of the list.

**Logweir never deletes a topic under an original name. Not one it created,
and not after a lost race.** When the creation step stops after this run has
created a topic — another name lost the race, the broker refused another name,
or a created topic was not served in time — every topic the run KNOWS it
created is LEFT on the cluster, empty, and NAMED (`left`), and every topic it
asked for and cannot account for is left and named too (`unconfirmed`): on
the runner's `target-topics-appeared=` line, in
`status.targetTopicsAppeared`, in the `Failed` condition's message, in the
product API's Restore view (`targetTopicsAppeared`, with `leftInstruction`
and `unconfirmedInstruction`) and at the top of the Restore's page in the
console, each list with its own sentence. `status.exitReason` is
`TargetTopicAppeared` when a name lost the race and `CreatedTopicsLeft` when
creation stopped for another reason and left a topic of either kind.
The reason nothing is cleaned up: Kafka has no conditional delete, so a record
a producer wrote between any "it is empty" read and the delete would be lost
with the topic, under a production name. An empty topic left behind is
recoverable; that is not. A name that appeared is never touched either. To
retry, check that nothing writes to each `left` topic and what each
`unconfirmed` topic holds and who writes to it, delete it yourself
(`kafka-topics.sh --delete --topic <name>`), and create a new `Restore`.
Teardown runs in scratch mode only, phase 9 never hands an identity mapping to
the deleter, and the runner's deleter refuses every source topic's own name
whatever the scratch prefix.

**Complete verification is required.** An original-name restore runs only
with `sample.coverage: complete` (`spec.coverage: complete` on the `Restore`):
phase 7 reads every record of every restored partition back and compares it
with the archive by its `x-original-offset`. A sampled plan with the identity
mapping is refused by name, `OriginalNameNeedsCompleteCoverage`, at every
boundary: the console selects complete coverage when the original names are
chosen, locks the box and says why; the product API refuses the request
(`coverage`, `original_name_requires_complete`); a CEL rule refuses the
object; the controller refuses a plan that does not ask for it before any Job
(`ExecutionSpecInvalid`); `logweir drill approve --approval-subject
original-name` refuses to sign it; and the runner refuses it before it dials
anything (exit 3). The reason: under a production name another producer may
still be writing. A sampled check reads the first records of each partition
and holds the count to a bound that is loose whenever the window cuts a
segment, so a foreign record inside that bound can pass. The complete check
reports a record the archive does not hold as unexpected, by its target
offset. `sample.complete_max_records` may still bound the work; a run it stops
signs `covered: false`, which is never a pass.

**Whole topics are required.** An original-name restore never restores a
partition subset. A plan that carries the block and `restore.partitions` is
refused by name, `OriginalNameNeedsWholeTopics`: the controller refuses it
before any Job (`ExecutionSpecInvalid`, the condition opening with the token),
both readiness checks say so, `logweir drill approve` refuses to sign it, and
the runner refuses it before it dials anything (exit 3). There is no CEL rule
for this one: a `Restore` declares no partitions, so the plan bytes are the
only place a subset is written and the controller's plan check is the
Kubernetes boundary. The reason: the run creates each topic under its own
name with EVERY partition the archive lists and would fill only the selected
ones, and the partitions left out could never be restored under that name
afterwards (the name exists, and a restore into an existing topic is
refused). A window is allowed, a start or an end: it restores every
partition, bounded in time. Restore a partition subset under a prefix.

**What does NOT change.** The restore does not fence producers: stop every
producer of a restored name before the restore and repoint consumers after it
(consumer positions are not copied; PROD-04.2). **A producer still writing
while the restore runs is detected and named, not prevented:** its records
land in the topic beside the restored ones, the complete verification counts
each as `unexpected` and names it ("target offset N carries no
x-original-offset" in `integrity.verification.complete.partitions[].findings`),
and the run signs `fail-integrity` (exit 2). The topic then holds both writers'
records and is not deleted, so stop the producers first. `strip_offset_headers`
stays `false`, so the next capture of the name starts a new generation
(PROD-01.4).

**Evidence.** The signed scorecard is format 1.8.0 and carries
`target.original_name`: the approval subject and the approval document it was
verified in (`v1Approval`, `governed`, `ordinary` — the last with
`confirmation: typedTopicNames`), the cluster condition (`targetIsNotSource`
with the source cluster id, or `autoCreateDisabled`), where owners were looked
for and what was found (the `KafkaTopic` resources file by its sha256), and
whether the owner path was chosen. Both verifiers check it (arms ON-1 to
ON-14; ON-14 refuses the block beside a partition subset; ON-13 refuses the
block beside a sampled verification, and beside a
pass that records none) and print two `original name:` lines
([verify-a-scorecard.md](verify-a-scorecard.md)). The decisions and the review's
findings are recorded in
[PROD-15.1-original-name.md](to-do/decisions/PROD-15.1-original-name.md).

**Readiness.** The `Preflight` for such a `Restore` does not refuse the
identity mapping it asked for (`plan.names`), and refuses the block in a
shape phase 0 refuses, a sampled plan and a partition subset included
(`TopicMappingIdentity`, in the runner's words). It
does not evaluate the cluster and owner conditions, which need the verified
receipt and the target's brokers. The run's phase 0 does that, before
anything is written.

**Upgrade and rollback.** Additive. An older runner refuses an original-name
plan (it maps every topic onto itself, which its guard refuses, exit 3); an
older controller ignores `topicNaming.originalName` and its runner refuses the
plan. **The authorization document v2 that carries `approvalSubject` or
`originalNameConfirmation` is format 2.1.0**; every document without them
stays 2.0.0, byte for byte what it was. This build's controller and runner
accept 2.1.0 and refuse either field under 2.0.0
(`AuthorizationDocumentInvalid`, "defined from formatVersion 2.1.0"); a
controller or runner built before the fields refuses every document that
carries them (an unknown field) rather than read it as ordinary, so roll the
controller and the console forward together before anyone confirms an
original-name restore. An older controller reads a stopped creation step as a
plain exit 1, and ignores the `failure-reason=` line and
`status.targetTopicsAppeared`. Rolling the CRDs back prunes
`topicNaming.originalName` and `status.targetTopicsAppeared` from stored
objects. Their plans are untouched, and the next runner refuses them as above.

### The credential is validated by the RUNNER, and the controller checks nothing

`weirkeeper` holds **no `get` on Secrets anywhere** (§9), so it never sees the
projected value and has nothing to validate. The check happens in the runner,
at the moment it reads `LOGWEIR_SOURCE_PASSWORD` / `LOGWEIR_TARGET_PASSWORD`,
and it exits **3** with `refusal-reason=CredentialNotRenderable`. The
controller maps that refusal onto the terminal state of the same name and does
nothing else with it. Do not look for a controller-side check; "no `get` on
Secrets" forbids one.

### Exit 3: the discriminator is a KEY NAME in a bounded tail

`TargetTopicConfigRefused`, `CredentialNotRenderable` and (since FX-8)
`PointInTimeByProducerTime` — a point-in-time selection over a source topic
recorded as `LogAppendTime`, in a plan that does not state
`restore.time_basis: producerTime`
([the plan field](formats/drill-spec.md#restoretime_basis-fx-8)) — are all
exit 3, and the only thing that tells them apart is the runner's
`refusal-reason=` line. The recovery-point binding's refusals —
`PointBindingMismatch`, `PointUntrusted` and (since FX-16)
`PointBindingSetMismatch`, a point-bound plan or restored set that is not the
point's own set ([the plan field](formats/drill-spec.md#sourcepoint-execution-contract-v2))
— are exit 3 too, but they are not terminal states: the line reads
`refusal-reason=GuardRefused`, so the `Restore` records `exitReason:
GuardRefused`, and the name is the first token of the refusal message in the
pod log. Since FX-34 that name and its sentence are also the end of the
`Failed` condition's message (§10, "What a refused run says about why"), so
they outlive the pod.
**Read §10's note on `refusal-reason=` before writing any reader of it**
(plan erratum **E4**): the line is the last line of the runner's *stdout*, but
a pod log is stdout and stderr merged in nondeterministic order, and the pod
log API has no stream selector — so the position is not a rule in either
direction. This reconciler scans the final sixteen non-empty lines
(`KEY_SCAN_TAIL_LINES`, §10) and matches by key name, exactly as the `Backup`
path does and through the same shared function. **A log body with no such line at exit 3 yields
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

**Exit 2 names its signed failure (interface I8 as amended,
`docs/stability.md`).** A runner carrying the amendment prints the same key
lines at exit 2. The reconciler records them, raises `EvidenceRecorded=True` /
`EvidenceKeysRecorded`, and fetches and verifies the scorecard exactly as for a
pass, so `status.outcome` (`fail-objective`, `fail-integrity`,
`preflight-failed`) and `status.evidence.verification` are written for a failed
run too. The run is still `phase: Failed`, and `Verified` is never `True` over
a recorded non-zero `exitCode` (§15.2). An older runner prints no keys at exit
2: nothing about that status changes, and no `EvidenceKeysUnreadable` is raised.

### What the status carries, and what it copies

`exitCode` and the condition are decided from the pod. Everything else is
**copied verbatim** out of the signed scorecard, fetched with the controller's
read-only archive credential: `outcome`, `lastPhaseCompleted`, `objectives`
(`rtoSeconds`, `rpoSeconds`, `passRate`, `met`), `integrity`
(`level`, `result`, `partialReason`, and since PROD-08.1a `coverage`,
`complete` and `unsampledTopics` — *Complete coverage* above — and since
PROD-11.1b `selection`) and `measured`. The controller **never
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

`logweir-operator` grants `update` and `patch` on BackupSchedules. CEL in the
CRD, not RBAC, decides what may change: since PLAT-05.1 every field except
`spec.sourceRef` (§7, §9). The controller reads all fourteen kinds, patches
their status, creates Backups and Restores **and holds one `patch` on
Backups**, Job create/read/patch, Pod and `pods/log` reads, ConfigMap
create/get, and `delete` on exactly two transient kinds (§22.1). It has no
Secret read, pod exec or pod attach permission, and **no `update` on
anything**: every status write, the `Forbid` slot reservation included, is a
merge `PATCH` whose body carries `metadata.resourceVersion` as the
compare-and-set precondition (§10's three RBAC notes). Inspect
[config/rbac](../config/rbac/) for the authoritative grants.

### The five Amendment G kinds, grant by grant

The controller's rules for `ProtectionPolicy`, `RehearsalSchedule`,
`RecoveryCatalog`, `RetentionPolicy` and `TrustPolicy` are written the way every
other rule in that file is: a verb appears only where a reconciler calls it, and
`crates/logweir/tests/manifest_lint.rs` walks both directions over the source
manifest, the install file and every rendered chart copy.

| Kind | Controller's verbs | Why not more |
|---|---|---|
| `protectionpolicies` | `list`, `watch`; `patch` on `/status` | no `get`: the watcher hands the object over and the reconciler never re-reads one |
| `rehearsalschedules` | `get`, `list`, `watch`; `patch` on `/status` | `get` has a caller here where the others have none — an `Approval` naming a standing authorization is checked against the referent's own sealed spec, so the referent is read |
| `recoverycatalogs` | `get`, `list`, `watch`; `patch` on `/status` | `get` is the one catalog a `ProtectionPolicy` names, folded into the existing rule rather than given a second one on the same resource |
| `retentionpolicies` | `list`, `watch`; `patch` on `/status` | `list` is the namespace-wide conflict check (two policies over one destination); no `get`, no `update` |
| `trustpolicies` | `list`, `watch`, `patch`; `patch` on `/status` | a namespace never names its own trust, so there is no name to `get`; the write half is `logweir-trust-admin`'s. `patch` on the object is the `logweir.dev/compromise-revocation` finalizer and nothing else (*Trust resolution*, below §8): RBAC cannot grant less than the object, so the one call site's body — `metadata.{name, resourceVersion, finalizers}` — is pinned by a test instead |

Plus `create` on `restores` — one per due rehearsal slot, and nothing in the
crate updates, replaces or deletes a `Restore` — and `list` on core `events`,
which is how a Job pod that never started can say why: every Job-owning
reconciler reads its own Jobs' and pods' events, by `involvedObject.uid`
(FX-11, §12).

**The controller holds no `delete` on any of the five, and none on anything
except the two transient check kinds** (`TopicDiscovery`, `Preflight`), whose
collector deletes with a UID precondition and a per-pass cap. That includes the
kind whose Job can remove data: the `RetentionPolicy` reconciler creates the
enforcement Job and does not link the code that deletes —
`scripts/check-no-archive-write.sh` proves the deleting crate is not in this
process's dependency graph, and the deletion capability lives in the Job's own
prefix-scoped object-store credential.

**Every status write is `patch` on the `/status` subresource**, never `update`
and never a verb on the spec. Seam S7: each carries
`metadata.resourceVersion` as a compare-and-set precondition. The one write to
an object's main resource is the `TrustPolicy` finalizer patch, which carries
the same precondition and touches `metadata.finalizers` only.

### Who may write the five, from outside

| Kind | Create | Edit | Held by |
|---|---|---|---|
| `protectionpolicies` | operator | `update`/`patch` (spec not sealed) | `logweir-operator` |
| `recoverycatalogs` | operator | `patch` (`spec.syncRequest` only, by CEL) | `logweir-operator` |
| `rehearsalschedules` | operator | `patch` (`spec.suspend` only, by CEL) | `logweir-operator` |
| `retentionpolicies` | **retention admin** | `create`/`update`/`patch` | `logweir-retention-admin` |
| `trustpolicies` | **trust admin** | `create`/`update`/`patch` | `logweir-trust-admin` |

Neither admin role carries `delete`, and neither names the other's kind. The
two separations are the same idea applied to the two irreversible things in the
product: who decides whose keys may sign an approval, and who decides whether
an installation reports or deletes.

### The CRD apply/upgrade procedure for the five

Unchanged from the procedure this release already documents, and it applies to
the D3 kinds without an exception: `kubectl apply --server-side
--force-conflicts -f charts/logweir/crds/` (Helm owns the fields of the CRDs it
installed, so without the flag an existing CRD keeps its old schema),
`kubectl wait --for=condition=Established` on all
fourteen, and only then the controller. [install.md, "Upgrade CRDs before
upgrading the controller"](install.md#upgrade-crds-before-upgrading-the-controller)
carries the exact loop. Helm installs `crds/` on first release only and neither
upgrades nor rolls them back, which is why the procedure is by hand.

Five new CRDs are pure addition: nothing existing changes shape, no object is
converted, and a cluster that applies them and never creates one of the kinds
behaves exactly as it did. The one carve-out is
`Approval.spec.subjectRef.kind`, whose new `RehearsalSchedule` value is an enum
widening an older controller cannot decode — see [The one widening that is NOT
rollback-safe](#the-one-widening-that-is-not-rollback-safe-approvalspecsubjectrefkind).

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

Both values reach **every** Job this controller creates: the run's own runner
Job, the `KafkaCluster` probe, interactive checks, recovery-catalog and
retention Jobs, and a dynamic `Backup`'s per-run topic discovery Job. Before
2026-09-18 the discovery Job was the one exception — it named the compiled-in
pin under `imagePullPolicy: Never` while the runner Job of the same run used
the configured image, so a dynamic run failed `TopicsResolved=False` with
reason `DiscoveryFailed` after its pod reported `ErrImageNeverPull`. An
installation that sets neither variable is unaffected; one that sets either
needs no change, and a controller upgraded into place fixes existing dynamic
schedules at their next run. Rolling back restores the old split.

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
It cannot write or delete in any bucket, and — because a schedule's retention
only reports (guard **G-RET**) — **neither the controller nor any component it
links holds a delete capability against object storage.** The one component
that can delete archive objects is the separately linked `logweir-retention`
worker of a `RetentionPolicy` in `mode: Enforce`, under its own credential
(§7f, ADR 0008 Amendment H); an installation that never enforces has none.

That sentence is about **object storage**. The controller
does now hold a Kubernetes `delete` verb, on exactly two resources:
`topicdiscoveries` and `preflights`, the transient check requests, so their
retention windows can be enforced (§22.3). It is a different subject and a
different mechanism — `scripts/check-no-archive-write.sh` still refuses any
delete on a store-shaped receiver anywhere under `crates/weirkeeper/src/`, and
`Store` exposes no delete method for one to be written against.

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
environment is fixed at start. The restarted controller reads every
unverified run whose read failed once more (§15.1b), so points that were
`NotAttempted` for want of the credential become verified without any edit.

### 15.1a Where an inline-archive run's evidence is read

A run with no saved destination — a `Backup` with an inline `spec.archive`, a
`Restore` with no `evidenceDestinationRef`, and every run an older release
wrote — has exactly ONE evidence reader: the controller's archive handle, the
read-only store `main` builds from `LOGWEIR_ARCHIVE_URL` with the controller's
own credential (D2 §3.10: "the global handle serves legacy inline objects
only"). The runner prints only bucket-relative keys, so **the run is verified
exactly when its evidence is in that handle's bucket**:

| run | where its evidence is | verified when |
|---|---|---|
| inline-archive `Backup` | its archive's own bucket, under `logweir/` (the receipt is written with the archive credential) | the archive URL's bucket is the handle's |
| inline-archive `Restore` | wherever the approved plan's `evidence:` block says, written with the archive credential | the plan's `evidence.bucket` is the handle's |

Backend and bucket are compared; the prefix is not (a handle over
`s3://kafka-backups/logweir` reads `s3://kafka-backups/poc`'s receipts) and
neither is the endpoint, which for the handle comes from the controller's own
environment.

- **Evidence in another bucket is not read.** The verdict is `NotAttempted`
  with a detail naming the run's own evidence location, the handle BY ROLE and
  the `logweir drill verify` route, and a `Restore` gets no `completion` (it is
  written only beside `Valid`). The handle's URL is installation configuration:
  it is in the controller's log (`archive_handle=`), and never in a status, a
  `Preflight` message or an event a namespace operator — in shared mode, any
  tenant — can read. Before
  this build the key was looked up in the handle's bucket, where the document
  never was: a `Backup` read `NotAttempted` with a store "not found" that looked
  like a missing receipt, and a `Restore` published **no verification block and
  no completion at all** — the PoC's `v0.1.5` point restored after the upgrade
  with its evidence in `logweir-evidence` while the handle read `kafka-backups`.
- **A scorecard the handle read nothing for is `NotAttempted`, named.** When the
  runner printed both scorecard keys and the controller's read produced no
  document — no handle configured, the object not in that bucket, or a
  credential that cannot read it — the verdict names the key and the handle
  by role. It is never `Invalid`, and there is no completion. Since PoC P12
  the same holds for a destination-backed run whose evidence destination reads
  with `ControllerIdentity`: its failed read used to write no block at all
  (and so could never be read again), and now writes `NotAttempted` naming the
  destination. Both are read again on the schedule in §15.1b, so a rehearsal
  waits for that schedule — at most about 21 minutes of attempts, inside its
  hour — and then records the reached verdict, rather than
  `EvidenceVerdictNotReached` after its five-minute grace.
- **The console writes such a plan's evidence to the archive's own bucket** —
  the bucket the archive credential already wrote the point's receipt to, and on
  the chart's default install the handle's — and its restore readiness check
  publishes the advisory `destination.evidenceReadable` row when a plan names
  any other bucket (§21.8). A hand-written plan for `kubectl apply` should do the
  same.
- **Upgrade and rollback.** Nothing is converted. A `Restore` that already
  finished with its evidence in another bucket is not re-read — reading it
  would read the wrong bucket — so a run restored before the upgrade with its
  evidence elsewhere keeps its missing block; verify it with the printed
  commands (the Restore's detail page fetches the scorecard from the plan's
  evidence bucket). A run whose evidence IS in the handle's bucket and whose
  read merely failed is read again (§15.1b). An older controller reads the
  wrong bucket again after a rollback.

### 15.1b A failed controller read is read again (PoC P12)

The controller reads a run's evidence itself in two cases: an inline-archive
run, through its archive handle (§15.1a), and a run whose destination's
`evidenceRead` is `ControllerIdentity`, through that destination's handle.
Until PoC P12 that read happened once, on the pass that made the run terminal,
and a failure there was final. On the PoC, three inline-archive `Backup`s read
`NotAttempted` because the controller had no credential (the SDK fell back to
the instance metadata endpoint). They stayed that way after
`logweir-evidence-ro` was created and after two restarts, so they were never
recovery points: a point needs a verified receipt and its `windowCovered`.

A failed read's `NotAttempted` now has one of three classes, decided from its
`detail`:

| class | `detail` | what happens next |
|---|---|---|
| transient | `the evidence object could not be read: …` (a denial, a missing credential, a timeout), the trust resolution could not be read, the verification task did not complete, a scorecard read produced nothing, a receipt that verified while the read of its window did not answer | read again on the evidence-fetch Job's schedule: attempt 2 at +1 m, 3 at +5 m after that, 4 at +15 m after that |
| absent | `the evidence object <key> is not in the archive` (the store's own `NotFound`) | never read again: that is a fact about the archive |
| configuration | no archive handle, no signing key, two policies claiming the namespace, a destination with no `evidenceRead`, evidence outside the handle's bucket | not on the schedule; see "a new controller process" below |

**Absent means the store said `NotFound`, whatever the reason.** The object
store maps every 404 to `NotFound`, including a bucket that does not exist and
a path-style or endpoint setting that 404s. A run read through a misconfigured
handle can therefore be recorded as absent and is not read again after the
handle is fixed. Verify it with the printed `logweir drill verify` command. A
`Restore`'s scorecard read cannot tell an absent object from a refused one, so
an absent scorecard is transient: it costs four reads per controller process.

A scheduled attempt is recorded in the same fields a fetch Job uses:
`status.evidence.observation = {mode, attempt, retryAfter}`, with `mode`
`ArchiveHandle` or `ControllerIdentity` and no `jobRef`. The `detail` names
the next attempt (`…; attempt 2 starts at 2026-09-25T00:06:38Z`). A reconcile
before `retryAfter` reads nothing and writes nothing. A terminal `Backup`
reconciles every 15 seconds, so this is the rate bound: at most four reads per
run per controller process, over about 21 minutes. A terminal `Restore`
requeues for its next attempt rather than waiting for a change. Each attempt is
the terminal pass's own evidence step run again: the same handle, digest check
and verifier. It writes what that pass would have written: the verdict, the
`Verified` condition and `windowCovered`. For a `Backup` it also writes
`records` and `capture`, and for a `Restore` the scorecard's `outcome`,
`objectives`, `integrity` and `measured`. Those go only beside a passing
verdict for a `Backup`, and `completion` only beside `Valid` for a `Restore`.
Nothing here can write `Valid` without a verifier reading the document. For a
`Backup`, a receipt that verifies while the read that carries its window does
not answer is recorded as transient and read again, not written `Valid` with no
`windowCovered`. A reached verdict is never read again, so that write would lose
the recovery point for good. After the fourth attempt the `detail` says no
attempt remains in this process and `retryAfter` is absent.

**A scheduled attempt that can no longer happen is settled.** If the archive
handle is removed, or re-pointed at another bucket, while an inline-archive
run's attempt is scheduled, the next pass records `NotAttempted` with the
handle's own sentence. It keeps the attempt count and clears `retryAfter`. A
`Restore` then waits for a change, and a rehearsal over it stops waiting. The
run is read again when a controller process with a handle over its bucket
starts.

**At most four controller reads are in flight at once, per process.** A
restart re-reads every eligible unverified run together, so a reconcile that
owes a read waits for one of four permits. The rest follow as permits free.
Each read resolves the namespace's trust, issues two or three object `GET`s and
writes one status patch.

**A new controller process reads it once more.** The controller's credential
comes from its environment, which a running process never re-reads, so a
created or rotated `logweir-evidence-ro` arrives with a restart. Each
controller process therefore reads every eligible unverified run once. If that
read fails transiently it starts a new schedule; otherwise it is recorded. A
second pass in the same process never starts another schedule. The memory of
which runs a process has read is bounded at 100 000 objects; past that it reads
no new ones. A definite absence, a legacy-unbound run (no runner digest) and a
run whose evidence is outside the handle's bucket are never re-read.

"Once more" is per process, not once ever. On every start, an inline-archive
run whose last answer was a configuration refusal is read once, and one whose
last answer was transient is read up to four times. A destination-backed run is
re-read only when its last answer was a transient read failure: a destination
that refuses is not re-read on every start.

**Upgrade.** A `NotAttempted` written by an older controller has no
`observation`. The first process of this build reads each such inline-archive
run once, and each destination-backed one whose `detail` is a transient read
failure. With the credential in place the PoC's three points verify on the
first reconcile after the upgrade, and the console offers them for restore.
**Rollback:** an older controller ignores the observation and reads nothing
again. A verdict this build reached stays on the object.

### 15.2 The badge is two rules, one per kind

Spec §8's green badge is not one rule, because "passed" is a different field on
each kind:

| kind      | green when                                                       |
|-----------|------------------------------------------------------------------|
| `Backup`  | `status.evidence.verification.result == Valid` **and** `status.exitCode == 0` |
| `Restore` | `status.evidence.verification.result == Valid` **and** `status.outcome == pass` **and** no recorded `status.exitCode` other than `0` **and** no `status.integrity.complete.covered: false` (PROD-08.1a) |

There is **no `outcome` on the `Backup` path at all** — `Backup.status` carries
`exitCode` and no `outcome` — so a single shared rule would render every
`Backup` ungreen. The `Restore` rule's exit-code clause exists because an exit-2
run publishes and verifies its signed failure (interface I8 as amended): the
exit code stays authoritative, so a document that verifies `Valid` at a
non-zero exit is `ExitCodeNotZero`, never green. A status with no `exitCode` at
all (nothing terminal wrote one) is judged on `outcome` as before. Either badge is labelled

> verified by weirkeeper at `<verifiedAt>` against key `<matchedKeyId>`

and **never** "verified in your browser": the in-browser WASM verifier is cut
from tag 1, and rendering no browser-computed verdict is strictly more honest
than a green badge over a verification that did not happen there. Anything that
is not green renders the literal word **`unverified`**, never `pass`.

Both rules also appear on the object itself, as a `Verified` condition whose
`reason` is `Verified`, `VerificationInvalid`, `VerificationNotAttempted`,
`VerificationUntrusted`, `ExitCodeNotZero`, `OutcomeNotPass` or
`CompleteNotCovered` (a complete verification that did not cover the restore,
whatever else the status says, judged before the outcome — PROD-08.1a), and whose
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
| `Pending` | NOTHING YET: an evidence-fetch check Job is reading the document with the destination's `evidenceRead` grant, or is waiting for a slot (§7b.3). Never green, and replaced by one of the four above |

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
  basis: Current                        # Current | Historical | RecordedBeforeRevocation | Unverified | None
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
keeps this controller quiet), so its verdict does not change by itself. The one
exception is a run whose evidence read FAILED and so has no verdict about the
document yet (§15.1b); a reached verdict is never re-read. When it
is re-derived, it is re-derived from what is already on the status: the stored
`matchedKeyId`, `signedAt` and `verifiedAt` are the three facts the decision
takes, so there is **no storage read, no signature check and no Job** — with
the one bounded exception in §15.2c, which exists because a status written
before `signedAt` existed carries only two of those three facts. Such a pass
writes `evidence.verification.{result,trust,detail}` and the `Verified`
condition that says the same thing, and touches `phase`, `exitCode`, `outcome`,
the evidence keys, `matchedKeyId` and `verifiedAt` not at all.

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

### 15.2c Upgrading past `signedAt`: what happens to objects written before it

**This is the only case in which a terminal object is read from storage.**

`signedAt` and `trust` are additive status fields introduced with the trust
lifecycle. A `Backup` or `Restore` that finished on an older controller carries
neither: its verification block is `result`, `matchedKeyId`, `payloadType` and
`verifiedAt`, and nothing more. The new rule compares the key's validity window
against the document's claimed signing time — and on such an object there is no
recorded instant to compare.

Refusing it as though the *document* carried no signing time is wrong, and
measurably so: it reports this cluster's own upgrade history as a finding about
somebody's archive. So the controller distinguishes the two:

The question is not *which build wrote this block* — nothing on the status says
so — but **is this block evidence that a signing time was ever compared to the
key's validity window?**

| the stored block (no `signedAt`) | what it means | what the controller does |
|---|---|---|
| `trust.basis` is `Current` or `Historical` | a signing time was read from the document and compared to the key's validity window | `Untrusted`, `SignedOutsideValidity`, as before — the document itself carries none |
| `trust.signingTimeRead: absent` | a re-read completed and the document carried no signing time | the same, and no further read: the answer is on the record |
| `trust.signingTimeRead: overCap` (FX-31) | the store answered with a document larger than the controller's read cap (§7b.4), so it was not re-read | the stored verdict is kept on an unverified basis, the sentence says the document was not re-read and names the cap, and no further read: the next would answer the same |
| anything else — no `trust` block, a `trust` block with no `basis`, `None`, `Unverified`, or a spelling a later build invents | nothing has been compared to the window yet | one bounded re-read, then decide |

Only `Current` and `Historical` are reachable through the window comparison, and
the comparison is unreachable without a claim — so the first row is an
allow-list that stays correct under a build nobody has written yet. Enumerating
the other side instead is what left five objects marked `Untrusted` across three
upgrades: an intermediate build had re-derived them into `basis: None` with no
`signedAt`, which two earlier rules both read as "a document that claims
nothing".

On a reconcile of an object in the last row, and only if the status also records
the document's key and its `sha256`, the controller performs **one** `get` of
that document through the same evidence path the original verdict came from — the controller's own read-only handle for a legacy inline-`archive` run,
or the destination's handle for a destination-backed one. The bytes are checked
against the digest the run recorded before anything is read out of them, so a
signing time taken this way is exactly as trustworthy as the verdict being
repaired; the signature is not re-checked, because it was checked over these
same bytes when the run finished. `signedAt` then comes from the receipt's
`finished_at` (or the scorecard's last phase), exactly as a fresh run derives
it, and the verdict is re-derived with it.

Until that read succeeds the object is **neither withdrawn nor presented**:
`result` becomes `NotAttempted` — no verification *was* attempted under this
policy — with `trust.basis: Unverified` and a `detail` saying why. The original
observation is not lost: `matchedKeyId` and `verifiedAt` still record it.

```yaml
result: NotAttempted         # not Untrusted: nothing was read, so nothing is refused
matchedKeyId: 2c76e22f...    # unchanged - the original verification still happened
verifiedAt: 2026-09-14T…Z    # unchanged - and it is still the independent observation
trust:
  basis: Unverified          # nothing has been compared to the key's window yet
detail: "the signature over this document verified under key <id>, and the trust policy
         <name> has not been applied to it yet (Unverified): ..."
```

**`result` and not only the basis, and that is the whole safety argument.**
`ui/pages/backups.js`, the product API's status projection and the `SIGNED`
printer column all read `result` and nothing else, so a block that meant "not
verified" while leaving `Valid` there would render a green *"verified by
weirkeeper"* badge. `NotAttempted` is the value every one of those already
fails closed on.

`Unverified` is **never green**. The badge renders the literal word `unverified`
and the `Verified` condition reads `False` with reason
`VerificationNotAttempted` — not `VerificationUntrusted`, because nothing has
been refused. A destination whose `evidenceRead` grant only a pod may hold
(D2 §3.9) is never repaired by this controller at all, and its `detail` says so
— the printed `logweir drill verify` command is the answer there, as everywhere
else.

**What "bounded" means, precisely.** A terminal object is reconciled every
`REQUEUE_SECS` (15 seconds), not only when a `TrustPolicy` changes, so an
attempt that learns nothing records **`trust.retryAfter`** — fifteen minutes
ahead — and no further read is attempted until that instant. A reconcile inside
that window resolves no destination, issues no `get` and writes nothing; the
`Unverified` basis is still the mark that says "this status predates
`signedAt`", so the retry survives any number of failed passes and an archive
that comes back is picked up within one window. There is no retry loop inside a
reconcile and no queue.

```yaml
trust:
  basis: Unverified
  retryAfter: 2026-09-19T12:15:00Z   # no read before this instant
```

A **digest mismatch** — the bytes at the recorded key are not the bytes the run
reported writing — is reported through the same `Unverified` path, with a
`detail` beginning "digest mismatch at". It is a stronger signal than a
timeout and is worth escalating; the verdict is not weakened by it, because the
original verdict was reached over the *recorded* bytes and no claim is taken
from the substituted ones.

**Three things this does not do.** It does not touch `verifiedAt`, which stays
the independent observation it has always been — and a backoff never delays a
revocation: an unlisted signer, a usage mismatch and a `KeyCompromise`
revocation are decided before the row that defers, and none of them consults the
signing time. It does not delay a revocation:
an unlisted signer, a usage mismatch and a `KeyCompromise` revocation still
change a pre-`signedAt` object's verdict immediately, with no read, because none
of those rows consults the signing time — and a kube API failure while resolving
the evidence path is recorded as "no read was attempted" rather than failing the
reconcile, so it does not delay one either. And it does not repeat. A repaired object carries `signedAt` and is an ordinary
one; a document that genuinely carries no signing time — indistinguishable on
the status from the legacy re-stamp, so it is read once too — records
`trust.signingTimeRead: absent` when the read answers, and is never asked again.
Either way a further reconcile writes nothing unless the verdict actually
changes.

**What it costs.** One `get` and one destination resolution per pre-`signedAt`
object, then nothing: a successful read writes `signedAt`, a document that
carries none records `signingTimeRead`, as does one over the controller's read
cap (`overCap`), and an attempt that learned nothing is barred for fifteen
minutes by `retryAfter`. The destination handle is UID-cached
and the installation policy is cached, so the marginal cost is the `get` itself.
A cluster with many such objects and an unreachable archive pays one failed
`get` and one small patch per object per quarter hour until the archive answers
— not one per reconcile, which at `REQUEUE_SECS` would be four an hour times
sixty.

**Rollback.** An older controller reached by rollback ignores `signedAt` and
`trust` entirely and reports the `result` it finds, so a repaired object reads
as `Valid` there too. An object left on `Unverified` reads as its stored
`result` with a `trust` block the old controller does not parse — additive,
and no worse than the pre-upgrade state it came from.

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

**What the scoped install narrows, and what it does not (D0 stage 5).** The
chart's `controller.watchNamespaces` REPLACES the cluster-wide binding with one
RoleBinding per execution namespace (`charts/logweir/README.md`
§`controller.watchNamespaces`), and the controller then watches and creates Jobs
in those namespaces only (`LOGWEIR_WATCH_NAMESPACES`,
`crates/weirkeeper/src/scope.rs`). O1 then covers the execution namespaces and
nothing else: a namespace that is not listed — the release namespace, where the
shared console's session and cursor keys live — is outside the controller's
Job-create authority, and `kubectl auth can-i create jobs` there answers `no`
for `weirkeeper`. The signing key in each execution namespace is still inside
it, so O1 itself stands.

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

**This forwarding is LEGACY-ONLY.** It applies to a `Backup`, `Restore` or
`BackupSchedule` that names an inline `archive.url`. An object that names a
`BackupDestination` instead gets a complete `AWS_*` set rendered from that
destination and **none** of the controller's own — §7b is the rule and
`plan_addressing` is the test. The two paths never mix.

**And the same four values are published**, read-only, in the installation
policy `ConfigMap`'s `legacyArchiveAddressing` block (§22.2), from the same
chart values and behind the same "only when an endpoint is set" guard. That
block is reserved for `POST …/destinations:from-legacy` (D2 §3.12 branch (b)),
which is to derive a legacy object's location from configuration it has
actually READ and label the result `installationConfig`. **That route does not
read the block yet:** it cannot read a policy `ConfigMap` it does not own, so it
takes branch (c) and refuses. Today the block's only reader is the restore
readiness check over a `legacySourceArchive` point (§21.8), which takes
`endpoint` and `region` from it when the plan leaves them out; `allowHttp` and
`virtualHostedStyle` are published and read by nothing until the route lands
(the runner gets both from the controller's own environment, above). An
install that forwards no addressing publishes an empty block — not
`allowHttp: true` — because transport security is never derived (D-SEAMS S5).

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
patch over a `BackupSchedule` touching `spec.suspend` and nothing else. (The
CRD itself leaves every field but `spec.sourceRef` mutable since PLAT-05.1;
the page's other schedule edits are the console's `PUT` below, which a merge
patch against the API server cannot replace.)

Those are the page's limits, not the cluster's. The API server's limits are
whatever the kubeconfig that started the proxy carries, which is the subject of
"What that costs" above, and they are the ones that actually hold.

### Destinations, topic discovery and readiness are console-only

Three surfaces landed with D2 and none of them is reachable through
`kubectl proxy`:

| tab | routes it uses |
|---|---|
| `#/destinations` | `GET/POST .../destinations`, `.../destinations/{name}`, `:update-access`, `:test`, `:from-legacy`, `/usage` |
| `#/clusters` (a connection's detail) | `.../connections/{name}/topic-discoveries`, `.../topic-discoveries/{id}`, `/topics`, `:cancel` |
| `#/schedules` and `#/restore` step 5 | `POST .../preflights`, `.../preflights/{id}`, `/details`, `:cancel` |

Every one of those is a `/api/v1/...` route on `logweir-api`. The custom
resources behind them (`BackupDestination`, `TopicDiscovery`, `Preflight`) are
in the same API group the proxy already forwards, so a page COULD have read them
directly -- and deliberately does not. The legacy in-cluster UI ServiceAccount
has no binding for the three kinds, `api.js`'s writable allowlist above is
unchanged, and a page that asked anyway would render a 403 it did not cause.
Instead the client refuses each call by name, in the page, with a sentence
saying which API serves the flow. Run the console (`logweir-api`) to use them.

The `#/destinations` tab is in the navigation in both modes on purpose: the mode
is decided once at boot, after the first paint, so a tab that appeared only in
console mode would appear and vanish under a reader.

**What a readiness result does and does not claim.** A `Preflight` is a real
check Job with the destination's and the connection's own credentials. Its
verdict is that Job's recorded aggregate, and the console renders it and nothing
else: an empty `checks` array is not a pass, a `skipped` blocking check is
labelled as never a pass, a phase this build does not recognise reads `unknown`,
and a `ready` aggregate is shown beside the execution-only checks with the
sentence saying it never meant those passed. Execution-time guards remain the
authority whatever a preflight says.

**What a topic inventory does and does not claim.** An all-topics Kafka Metadata
request silently omits every topic the principal cannot `DESCRIBE`, so
`visibility.state` is `unknown` for a listing that succeeded, `unknown` is that
field's healthy default, and the console never renders the word "complete" for
one. `attestedComplete` is an administrator's claim from the installation policy
`ConfigMap`, and it is always rendered with its author, its instant and
"not verified by Logweir".

**With no controller for the three kinds**, which is every installation whose
`weirkeeper` image predates them, the API creates the objects and their statuses
stay empty. The console renders that honestly: a destination reads "not judged
yet" rather than valid or invalid, and a discovery or a preflight reads
`pending`. Nothing is rendered as ready or as failed on the strength of an
absent status.

**Where a schedule writes.** `BackupSchedule.spec.destinationRef` (PLAT-06.2)
is projected and the schedules table renders it, resolved by name against the
destinations in the namespace: a live one is shown with the location it writes
to, a schedule naming a destination that is not there any more is a refusal to
say where it writes rather than a guess at the object now holding that name,
and a schedule with no reference is labelled as carrying an inline archive.
The create form sends `destinationRef` on `POST .../schedules` (PLAT-10.1) and,
when nobody has chosen a location yet, preselects the namespace default
destination (PLAT-08.2); an EXISTING schedule is bound to one from the Future
policy panel below (`PUT .../schedules/{name}`) or with `kubectl`. Both forms
show what the schedule inherits from the chosen destination -- location,
endpoint, region, addressing, transport, CA, verdict -- as facts, pin the choice
by uid and refuse at submit a destination deleted or recreated under the same
name while the form was open (an access or CA edit keeps the uid and is not a
refusal). **Converting an inline schedule:** when the panel changes where a
schedule writes, it compares the old and new locations by bucket and prefix; the
same location is saved as is (recovery points before and after share the prefix,
and runs already created keep the inputs they froze), while a different location,
or one it cannot compare, needs an explicit "write new runs to the new location"
box. The engine's endpoint for an inline archive comes from the controller's
environment, which the console cannot read, so confirm the destination names the
endpoint those runs used.

### The schedule policy, its previews and manual runs are console-only too

D1 W7 added three surfaces to `#/schedules` and one column to `#/backups`, over
routes that exist only on `logweir-api`:

| surface | routes it uses |
|---|---|
| Future policy panel (per schedule) | `PUT /api/v1/namespaces/{ns}/schedules/{name}` |
| Preview next runs | `GET /api/v1/cadence-previews` |
| Back up now / Run first backup now | `POST /api/v1/namespaces/{ns}/backups` |

**The policy panel replaces, it does not patch.** `PUT .../schedules/{name}`
takes the whole future policy under an `expectedGeneration` precondition, and a
field omitted from the request is REMOVED from the schedule. The panel therefore
shows every field of the policy and sends every one of them, states that above
its button, and prints the documented default beside each input rather than
prefilling it -- a blank input means "do not set this field", and prefilling
`3600` would turn every save into a schedule that pins what it used to inherit.
`sourceRef` is never sent: the route carries it only to refuse it. A schedule
whose `generation` this build does not publish gets no form at all, because the
precondition IS that revision. A successful save RE-READS the schedule and
renders what the API server then holds, the way the suspend toggle does: the
revision the panel was opened at is superseded by the save itself, and a second
save from a stale screen would be refused `412` with nothing on screen saying
why.

**The browser evaluates no cron.** A preset is compiled to its canonical
expression by `GET /api/v1/cadence-previews`, against the same `chrono-tz`
database the controller schedules with, and that expression is what is saved;
the page holds the preset catalogue as parameter bounds and no cron template at
all. A saved schedule's `status.nextRuns` and a draft's preview render through
one renderer, because they are one shape. A fixed local time inside a repeated
hour shows BOTH firings with their offsets and the controller's own
`RepeatedLocalTimeFirst` / `RepeatedLocalTimeSecond` markers; a local time that
does not exist shows `NonexistentLocalTimeShifted` at the end of the gap. An
absent `nextRuns` is not an empty one, and the only staleness signal the page
reads is `nextRuns[0].at` in the past -- **not** `status.policy.evaluatedAt`,
which is when the status last moved and stands still on a healthy schedule.

**A manual run is one run per intent, and the intent is a field of the form's
draft.** Its value is `logweir-ui.manual.` plus 32 random hex characters, so a
double click, a retry after a timeout and a "Check status" all read the same
field, send the same key and return the same run with `replayed: true`. A
reload loses the draft and therefore the intent -- nothing in this console is
stored in the browser, and a random body cannot be reproduced by a later
session the way a counter could -- so a click after a reload is a deliberate
NEW run and creates a second one; the panel lists the manual runs that already
exist so the reader sees the first before deciding. **"Back up again"** is the
only control that mints a new intent, and it appears once a run exists or once
the API has refused with a `409`; that is PLAT-06.2's "a deliberate later
backup creates another". A `409 policy_changed` renders the revision in force
now, from the problem document's one extension member, and a `409
idempotency_conflict` says the intent was spent on a different request; both
offer that control, and no other refusal does. Nothing blocks a manual run: a suspended
schedule and an active run are notices, and a suspended schedule stays
suspended. A suspended schedule or a `notReady` preflight requires a second
explicit confirmation carrying the object's own recorded reason, and confirming
a red verdict sends `readinessAcknowledgement`, which the API records as the
annotation `logweir.dev/readiness-ack` -- the API reads no preflight and gates
on nothing.

**In `kubectl proxy` mode all three are refused by name.** There is no preview
route in front of the proxy and the browser will not evaluate cron to fill the
gap; the replace is built on the product API's `expectedGeneration`
precondition, which a JSON-merge patch against kube-apiserver would not have;
and a manual run's name is derived by the API from the authenticated subject
(issuer, subject, namespace, route, key), which the browser does not hold -- so
a browser-minted name would not be the name `kubectl create -f` and the API
produce. The in-cluster UI ServiceAccount also has no `create` on `backups`;
`charts/logweir/templates/ui/ui.yaml` grants `create` on approvals,
`kafkaclusters`, `backupschedules` and `restores` and nothing else, and adding
that verb is a chart change with its own review (D1 section 8.5).

**What the console can and cannot say about a run.** `Backup`'s projection
carries `trigger` and `scheduleRef`, so the Backups table renders the run kind
from `spec.trigger.kind` alone -- `Scheduled`, `CatchUp`, `Retry`, `Manual` --
and a run frozen before PLAT-05.1 says its trigger was not recorded rather than
being rendered as `Scheduled` from the coarser `triggeredBy`. The projection
carries **no `status.selection`**, so a run's coverage label cannot be rendered
in console mode; the page says so and names PLAT-09.2 for the projection, and
renders the label, the selection mode, the counts and the `TopicsResolved=False`
reason wherever the object does carry them, which in `kubectl proxy` mode is
every run. `ScheduleStatusView` likewise carries `policy`, `nextRuns` and
`activeRuns` and not `lastSlot`, `missedSlots`, `pendingRun` or `history`, and
the card names those four rather than leaving empty rows.

### The three D3 tabs, and what each of them refuses to say

D3 added `#/operations`, `#/protection` and `#/catalog`, extended `#/keys` and
changed one column on `#/backups`, `#/history` and `#/schedules`. The routes are:

| tab | what it reads |
|---|---|
| `#/operations?ns=&kind=&name=&uid=` | `GET .../operations/{kind}/{name}` and its SSE stream `.../events` in console mode; the `Backup`/`Restore` custom resource itself in legacy mode |
| `#/protection` | `GET .../protection-policies[/{name}]`, or the `ProtectionPolicy` custom resource |
| `#/catalog` | `GET .../catalogs[/{name}]`, `/points`, `/signers` and the one create, `POST .../catalogs`; the `RecoveryCatalog` custom resource carries the status half in legacy mode and the point list is console-only |
| `#/keys` | `GET /api/v1/trust-policies`, or the cluster-scoped `TrustPolicy`, with `TrustRoster/default` as the named fallback |

**The operation view follows a run and cancels nothing.** The stream is one
`EventSource` on the same origin, opened in `ui/api.js` beside the one `fetch`,
carrying no header and no token in its identifier; it reconnects with backoff
and falls back to polling after three failed connects, because a proxy that will
not carry `text/event-stream` never starts. Closing the view closes a
connection. There is no cancel route in v1 and the page offers no control that
pretends there is. In legacy mode there is no normalized state at all -- the ten
words are `logweir-api`'s, computed from a table this page does not implement a
second time -- so the view renders the controller's own `status.progress` and
says which fact it is showing.

**The read route and the stream send two different shapes, and that is the
server's decision.** `GET .../operations/{kind}/{name}` answers the envelope
`{item, requestId}`; the stream's `operation` and `reset` frames are the BARE
view, with no wrapper and no request id -- a request id belongs to a request,
and a stream that emitted one per frame would publish the same value twenty
times. The page decodes the two with two shapes.

**`end` carries a reason and no document, and only two of the three reasons
end anything.** `settled` means the run is terminal AND its verification
verdict is in, which is the same pair `logweir-api`'s own `is_settled` uses.
`vanished` means the object is gone: the last snapshot stays on screen, the
reason is printed beside it, and nothing is re-read, because an object created
later under the same name is a different run. `maxDuration` is the
**connection's** 300-second ceiling and not the end of anything -- a backup
longer than five minutes hits it while it is still running -- so the page
reconnects, and falls back to polling only after as many empty closes as
failed connects. A reason this build does not recognise is treated the same
way, which costs a connection rather than showing a running operation as a
finished one.

**A rehearsal is labelled a rehearsal before its scorecard exists.** A
`Restore`'s `targetMode` is on the view from the moment the object is created,
so `#/operations` says `scratch` or `newTopic` for a run that is pending,
running or refused -- not only for one that finished. That label is not the
completion guidance: the guidance says what a run PRODUCED and stays beside
the scorecard, while the label says what the run IS, and "its topics are
deleted by teardown" is worth reading before the teardown rather than after
it. A `Restore` that records no mode says so and no mode is guessed.

**Protection health and schedule health are two questions.** An enabled,
healthy, never-failing schedule can have no recoverable backup: its evidence may
not verify, the archive may have lost the object, or its runs may cover topics
the objective is not about. `#/protection` renders both healths in their own
columns and never collapses them, labels the capture-start instant and the
newest archived record as the two different instants they are, and keeps the
`Protected` condition at three statuses -- `Unknown` for an evaluation that
could not happen is never rounded to `False`, because "Logweir checked and you
are not protected" is a claim nothing made.

**The catalog keeps availability and verification apart.** Whether the archive
can still serve a point and whether its receipt verifies under a key this
installation accepts are different facts with different repairs. Whether a point
may be restored from is the catalog's own materialised `selectable` field as the
API publishes it — `false` as well when the point's own `Backup` carries a
refusal the controller reached, named in `backupVerdict` — read and never
recomputed. Nothing is hidden: a `Missing`, a `Conflict` and an
`UntrustedSigner` row are each listed with their state and their remedy
sentence. **There is no one-click trust anywhere.** A point signed by a key this
installation does not list shows the key id -- the SHA-256 of the DER SPKI, the
number `openssl` prints -- the out-of-band fingerprint command and a `TrustPolicy`
document the page renders and does not apply. A key arriving beside an archive is
never trusted by proximity.

**`unknown` is not `valid`.** The keys view reads `unknown` whenever the object
carries no status, its `observedGeneration` is behind its `generation`, or its
`evaluatedAt` is outside the freshness window -- and that window is measured
against the **server's** clock, the `Date` header of the answer that carried the
object, never the browser's. With no server instant at all the column reads
`unknown`, which is the fail-closed side. `valid` and `expired` are rendered only
for a fresh verdict. Retirement and revocation are explained apart: a retired key
verifies everything it signed before it was retired and authorises nothing new,
while a key revoked for compromise does not get that courtesy, because its own
claimed signing time is attacker-controlled.

**The retention panel says what is HAPPENING.** It reads
`RetentionPolicy.status.enforcement` and not `spec.mode`: a policy asking for
`Enforce` whose destination will not resolve reports `RecommendationOnly`, and
the panel says the thing that is true of it. "Logweir never deletes from your
archive" is kept verbatim for a schedule report and for `RecommendationOnly`, and
is replaced by the mode's own sentence otherwise -- printing it beside a policy
that deletes nightly would be the most consequential false sentence this console
could render. In `Enforce` the approved digest, the newest evaluation's digest
and the plan's expiry are three separate facts, because a plan approved and then
superseded is exactly what the two-step approval exists to catch, and the
irreversibility sentence is on the page before an administrator approves a
digest rather than after.

**`legalHold` is never enforced by Logweir, in any mode.** The panel renders
`ProviderEnforcedUnverified` as "declared by your provider; Logweir cannot verify
it", because `object_store` exposes no WORM readback and the guarantee is exactly
"a provider refusal is authoritative, recorded, not retried and excluded from the
next plan". On a versioned (and so on every Object Lock) bucket the worker never
asks the provider at all: it refuses the delete as `VersionedBucket`, because a
delete by key there writes a marker instead of being refused (§7f, the
versioned-bucket bullet, including the cases it does not see).

**The legacy in-cluster UI ServiceAccount reads none of the four D3 kinds.**
`charts/logweir/templates/ui/ui.yaml`'s ClusterRole is unchanged by this change,
so under the Helm UI the three D3 tabs are console flows and the keys view falls
back to the roster, by name, with the API server's own refusal rendered. Adding
`protectionpolicies`, `recoverycatalogs`, `retentionpolicies` and
`trustpolicies` to that role would let the legacy path read them too; it is a
chart change with its own review and its own exact-rule table in
`crates/logweir/tests/chart_lint.rs`.

### What the console publishes, and what it deliberately does not

The four D3 tabs read the product API's own **flat** views -- a
`ProtectionPolicyView` carries its identity and its facts at the top level, with
no `spec`/`status` pair -- and the custom resource in legacy mode. The console
projects one into the other so a reader sees the same page either way.

**Three facts the API does not publish, and what stands in each place.**

* **No Job name and no pod name.** `progress.runner` is infrastructure detail
  and the console contract does not carry it; the operation view says so and
  points at the diagnoses, each of which names the object it is about
  (`{kind, name}` IS published, because PLAT-14.1 asks for resource-scoped
  errors).
* **No ConfigMap names.** A catalog's materialised pages live in ConfigMaps the
  sync Job owns; the console's facts about the view are `viewPoints`,
  `truncated` and `viewExpired`.
* **No frozen `locationDigest` on a recovery point.** That digest is a fact
  about a Backup's own destination snapshot, not about a point read out of a
  bucket, so no document carries one for a point. "Restore this point" carries
  D3 §5.5's own plan binding instead: the point id, the receipt key and digest,
  the manifest digest where the catalog has one, and the catalog's destination.

**A word a controller writes stays an open string.** Eight D3 vocabularies are
published as typed enums; the rest -- a health, an availability, a diagnosis
code, an effective key state -- are `string`, because a closed enum over a
status field would turn a forward-compatible controller into a 500. The console
validates membership to pick a badge colour and renders an unrecognised word
**verbatim**, which is also what "unknown is not valid" requires.

**The trust evaluation is the API's decision.** `GET /api/v1/trust-policies`
publishes `evaluation {state, reason, serverTime, freshWithinSeconds,
ageSeconds}`, so the freshness verdict §7.7 asks for is made once, against the
API's own clock, with the arithmetic on screen. The console renders it and says
who decided; in legacy mode, where there is no such view, it decides for itself
against the `Date` header of the answer that carried the object.

**Reading a TrustPolicy needs an administrator binding.** `GET
/api/v1/trust-policies` is Administrator-only and serves a policy only when it
governs a namespace the actor administers, or is the installation default;
inside a served policy the namespace lists are filtered to that administered set
and `namespacesFiltered` says when they were. The keys page renders that flag,
because "these are the namespaces" is not a claim this reader can make.

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
carries a signature by a key the namespace's resolved trust lists — its
`TrustPolicy`, or `TrustRoster/default` when no policy governs it (§8, *Trust
resolution*) — over the exact bytes of that restore's plan. This section is the
`legacy-governed-v1` flow; a namespace bound to an approval policy uses the
console's confirmation and, under `Governed`, `logweir drill countersign`
instead (§8, *Approval policy*). Nothing in this product can produce that
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
id, never the first one listed), checks that id against the approver keys of
the namespace's resolved trust (§8), compares the `payloadType` in full, and
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

**Prefer a `TrustPolicy`.** `TrustRoster` is deprecated, and its `spec` is
sealed by CEL (§7, *The immutability seals*): a changed roster cannot be
applied over the old one, only created in its place, and every approval check
is interrupted while `TrustRoster/default` is absent. Key rotation with an
overlap is a `TrustPolicy` edit ([keys.md](keys.md), *The supported
procedure*). What follows is the roster's shape, for a namespace no policy
governs.

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
kubectl --context docker-desktop create -f roster.yml
```

`create`, not `apply`: on a cluster that already has `TrustRoster/default` the
API server refuses a changed spec, and replacing it means deleting it first
([install.md](install.md), *The cluster-scoped `TrustRoster`*).

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
proxy restricts paths to `/ui/` and the Logweir API; the chart renders no
Ingress for it (the console's `shared` mode is the one component that may have
one — [install.md](install.md) §5e).
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
| `auth.mode` | required: `plaintext`, `scramSha512`, `scramSha256`, `plain` or `mtls` (§20.8) | value |
| `auth.username` | required for the SASL modes (`scramSha512`, `scramSha256`, `plain`) | value (public identity) |
| `auth.secretRef.name` | required for the SASL modes | **reference** to a Secret in this namespace |
| `auth.secretRef.passwordKey` | `password` | key name, not a value |
| `auth.tls` | `false` | value (the only switch that turns TLS on; **required** `true` for `plain` and `mtls`) |
| `auth.tlsCa` | the runner image's `ca-certificates` and the engine's bundled roots | **reference** to a Secret or ConfigMap key in this namespace |
| `auth.clientCertificate` | required for `mtls`, refused for every other mode | **reference** to ONE Secret in this namespace: `name`, `certificateKey` (absent: `tls.crt`), `privateKeyKey` (absent: `tls.key`) |
| `status.credentialBinding` | written by the controller for a connection with a credential | the value the credential Secret's `logweir-binding` key must hold (§20.9) |
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
| `auth.mode: scramSha256` or `plain` with no `auth.username` or `auth.secretRef.name` | `CredentialNotRenderable` |
| `auth.mode: plain` with `auth.tls: false` (SASL/PLAIN in the clear) | **`PlainWithoutTls`** (also rejected at admission by CEL) |
| `auth.mode: mtls` with `auth.tls: false` | `ConnectionConfigInvalid` (also CEL) |
| `auth.mode: mtls` with no `auth.clientCertificate`, or `auth.clientCertificate` on any other mode | `CredentialNotRenderable` / `ConnectionConfigInvalid` (both also CEL) |
| a projected credential whose Secret carries no `logweir-binding`, or another connection's | **`CredentialBindingMismatch`** — refused by the RUNNER before any client exists (§20.9) |
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
- **No readback path exists.** `connection::credential` builds a credential
  Secret for the console's write-only entry flow (the password, or since
  PROD-01.3 an mTLS certificate and key) and can never read one; the caller
  keeps `metadata` from the create response and nothing else.
- **No client key is held by any Logweir process.** An `mtls` key reaches both
  clients as a projected FILE whose path is all the runner sees (§20.8); the
  e2e rows scan every output, receipt and scorecard for a line of the key's
  body (`e2e/tests/auth_modes.rs`).

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

### 20.8 Client authentication modes (PROD-01.3)

| `auth.mode` | `auth.tls` | Credential | Logweir's client (librdkafka) | The engine |
|---|---|---|---|---|
| `plaintext` | `false` only | none | `PLAINTEXT` | no `security:` block |
| `scramSha512` | either | password (`secretRef`) | `SASL_PLAINTEXT`/`SASL_SSL`, `SCRAM-SHA-512` | `SCRAM-SHA512` |
| `scramSha256` | either | password (`secretRef`) | `SASL_PLAINTEXT`/`SASL_SSL`, `SCRAM-SHA-256` | `SCRAM-SHA256` |
| `plain` | **`true` only** | password (`secretRef`) | `SASL_SSL`, `PLAIN` | `SASL_SSL`, `PLAIN` |
| `mtls` | **`true` only** | client certificate and key (`clientCertificate`) | `SSL`, `ssl.certificate.location` / `ssl.key.location` | `SSL`, `ssl_certificate_location` / `ssl_key_location` |

**SASL/PLAIN only over TLS.** PLAIN sends the password itself, so `plain`
without `tls: true` is refused — never dialled — with the named reason
**`PlainWithoutTls`**: by the CRD's admission rule, by the resolver before any
Job exists, by every runner entry point (`refusal-reason=PlainWithoutTls`,
exit 3) before any client exists, and by both clients' builders as a backstop.
Confluent Cloud API keys and Azure Event Hubs connection strings
(`username: $ConnectionString`) are presented in this mode. **Neither provider
has been run against**: both are `untested` in
[the compatibility contract](support-matrix.md#managed-kafka-providers), and
whether either serves the request versions the engine sends is not known. A
`Backup` `Preflight` answers that on first contact (§21.6c,
`connection.engineProtocol`).

**mTLS** presents the client certificate in the TLS handshake. Create the
Secret with `kubectl create secret tls <name> --cert=client.pem --key=client.key`
(the key **unencrypted**: PKCS#8, PKCS#1 or SEC1; neither client takes a
passphrase) and name it in `auth.clientCertificate`. The controller projects it
read-only (mode `0440`) as `tls.crt` and `tls.key` under
`/connection/source-client-cert` or `/connection/target-client-cert`, and names
the two **paths** in `LOGWEIR_{SOURCE,TARGET}_TLS_CERT_FILE` /
`..._TLS_KEY_FILE`; both clients load the files themselves, so no Logweir
process holds the key's bytes. An `mtls` connection has no username: the Kafka
principal is the certificate's subject, which the controller (holding no
Secret read) cannot know, so its `principal` is reported as
`mtls:secret/<name>` and a restore plan binds the mode and the transport, not
the principal. Any TLS mode takes `auth.tlsCa` (§20.2).

The signed documents name the mode: a backup receipt and a catalog point
record that name `scramSha256`, `plain` or `mtls` are format **1.4.0**, a
scorecard **1.5.0**; every `plaintext`/`scramSha512` run writes exactly the
document it always did ([stability.md](stability.md)).

Example (SASL/PLAIN in the shape Confluent Cloud documents, public CA). It
shows the mode, and it is not a tested row: no Confluent Cloud cluster has
been dialled
([the compatibility contract](support-matrix.md#managed-kafka-providers)).

```yaml
apiVersion: logweir.dev/v1alpha1
kind: KafkaCluster
metadata: {name: confluent-prod, namespace: team-a}
spec:
  bootstrapServers: ["pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"]
  role: source
  auth:
    mode: plain
    tls: true
    username: <API key>
    secretRef: {name: confluent-prod-credential}   # holds `password` and `logweir-binding`
```

### 20.9 The credential binding: a connection presents only its own credential

**The threat.** A connection names the Secret its credential is in. Without
more, anyone who may write a `KafkaCluster` — or ask the console to — could
name **another** connection's credential Secret, one they cannot read, point
`bootstrapServers` at a host they control, and have Logweir's probe and runner
present that credential there: Logweir would be the deputy that exfiltrates it
(SASL/PLAIN sends the password itself; SCRAM hands the host an exchange it can
guess against offline; a client certificate is presented to a server the
author chose).

**The binding.** A credential Secret is used only when its data key
`logweir-binding` equals the binding of the connection that projected it:

```
v1:<KafkaCluster UID>:sha256:<digest of the bootstrap set, mode, username, tls and CA reference>
```

Every Job built from a connection with a credential carries the EXPECTED value
as a literal (`LOGWEIR_{SOURCE,TARGET}_CREDENTIAL_BINDING_EXPECTED`) and the
Secret's own `logweir-binding` key as an **optional** `secretKeyRef`
(`LOGWEIR_{SOURCE,TARGET}_CREDENTIAL_BINDING`). Every runner — `backup run`,
`restore run`/`drill run`, `cluster-probe`, the discovery and preflight check
runner, `doctor` — compares the two **before any client exists and before the
credential is used**, and refuses an absent or different binding with
**`CredentialBindingMismatch`** (exit 3, `refusal-reason=CredentialBindingMismatch`;
on a `KafkaCluster`, the `Reachable` condition's reason; on a `Preflight`, an
`AuthenticationFailed` row whose message opens with it). No Secret is read by
the controller: the kubelet projects, the runner compares.

**Why a UID and an endpoint, and why that covers "edit the endpoint, keep the
password".** A UID exists only once the object does, so no Secret written for
one connection can name another. `spec` is CEL-immutable, so a connection's
endpoint cannot change under its UID: a new bootstrap address, CA or transport
is a **new** `KafkaCluster` with a new UID, which no stored credential names —
the credential must be entered again. The endpoint digest holds the same rule a
second time, for an object whose `spec` changed by a route that bypassed the
immutability rule. Tested: `crates/weirkeeper/tests/connection.rs::
the_credential_binding_names_this_connection_and_its_endpoint`.

**Where the binding comes from.** The **console** never names a Secret: the
product API takes the password, or the client certificate and key, ONCE
(write-only), creates the Secret itself — type
`logweir.dev/kafka-sasl-password` or `logweir.dev/kafka-client-certificate`,
labelled `app.kubernetes.io/managed-by: logweir` and `logweir.dev/connection`,
**owned by the `KafkaCluster`** (deleting the connection collects it) and
carrying the binding — and never reads it back ([api.md](api.md)). With
**`kubectl`**, create the `KafkaCluster`, read its binding, and write it into
the Secret:

```
kubectl --context <ctx> -n <ns> get kafkacluster <name> -o jsonpath='{.status.credentialBinding}'
kubectl --context <ctx> -n <ns> create secret generic <name>-credential \
  --from-literal=password='<password>' --from-literal=logweir-binding='<that value>'
```

**Upgrade: existing credentialed connections must be bound — one at a time,
by name, after an inventory.** A `scramSha512` connection created before this
release has a Secret with no `logweir-binding`, and every run of it is refused
(`CredentialBindingMismatch`) until the key is added — fail closed, never a
credential presented unbound. **The binding step can itself perform the theft
the binding prevents**, so it is done carefully: before this release nothing
stopped a `KafkaCluster` from naming another connection's Secret, so a "thief"
connection — someone else's Secret, an endpoint its author controls — may
already exist. Binding "each connection's Secret" in a loop would bind the
victim's Secret to whichever connection came last; "splitting" a shared Secret
would copy the victim's credential into a Secret bound to the thief.

1. **Before the roll, suspend the credentialed schedules** (`spec.suspend:
   true` on each `BackupSchedule` and `RehearsalSchedule` that uses such a
   connection). `status.credentialBinding` exists only once the new controller
   has probed the connection, and until the Secret is bound every run of it is
   refused; suspending turns those refusals into skipped slots.
2. **Inventory, per namespace, and stop on any Secret named twice:**

   ```
   kubectl --context <ctx> -n <ns> get kafkaclusters \
     -o custom-columns=CONNECTION:.metadata.name,SECRET:.spec.auth.secretRef.name,SERVERS:.spec.bootstrapServers
   ```

   A Secret that appears in more than one row is **an incident, not a split**:
   ask the credential's owner which connection is theirs, check the other's
   `bootstrapServers` and its creator (`api.logweir.dev/actor` when the console
   made it), delete the connection they do not recognise, and treat the
   credential as exposed if that connection ever ran. A second legitimate
   connection gets its own credential, entered again by its owner (the console
   creates the Secret) — never a copy of a Secret another connection names.
3. **Bind one connection at a time, by an explicit command that names both the
   connection and the Secret**, and only when the connection already named that
   Secret before the upgrade and its owner confirms the endpoint:

   ```
   c=<connection>; s=<secret>
   # the connection must name exactly this Secret ...
   test "$(kubectl --context <ctx> -n <ns> get kafkacluster "$c" -o jsonpath='{.spec.auth.secretRef.name}')" = "$s"
   # ... and this must be the endpoint the credential's owner expects
   kubectl --context <ctx> -n <ns> get kafkacluster "$c" -o jsonpath='{.spec.bootstrapServers}{"\n"}'
   b=$(kubectl --context <ctx> -n <ns> get kafkacluster "$c" -o jsonpath='{.status.credentialBinding}')
   kubectl --context <ctx> -n <ns> patch secret "$s" --type merge \
     -p "{\"stringData\":{\"logweir-binding\":\"$b\"}}"
   ```

   Never iterate over every connection, and never bind a Secret a connection
   started naming after the inventory.
4. **Resume the schedules.** A console-mode adopter needs a kubectl user with
   Secret `patch` for step 3: the console has no re-bind action, by design
   (it never names an existing Secret).

Rollback: an older controller and runner ignore the binding pair, so a bound
Secret keeps working with them; the extra key is inert. Connections created
through this release's console are bound when they are made and need none of
this.

**What it does not close.** Anyone who can **write** a Secret can bind a
credential they put there — that is their own credential. A namespace
administrator with Secret read can read every credential anyway. **For a
connection credential, Secret `patch` is equivalent to Secret `get`:** a
principal who may patch Secrets but not read them can set a victim Secret's
`logweir-binding` to a thief connection's `status.credentialBinding` (public)
and so re-bind a credential they never saw to an endpoint they chose. Grant
Secret `patch` in a namespace only to principals you would let read its
credentials. The kubelet
projects a foreign Secret's value into the refused pod's environment before
the runner refuses; it never leaves the pod (`automountServiceAccountToken:
false`, no network use before the check), and the pod exits at once
([SECURITY.md](../SECURITY.md)). A CA reference is not bound: a CA is public
and is only ever a local trust anchor, never sent anywhere — and the console
takes a CA from a ConfigMap only.

### 20.10 Every other credential reference is bound too (FX-20)

§20.9's confused deputy is not peculiar to Kafka connections. Wherever a
writable object names a credential Secret **beside an endpoint its author
chooses**, an author who cannot read Secrets could name somebody else's Secret,
point the endpoint at a host they control, and have Logweir present it there:

| Object | Credential | Presented to | Bound to |
|---|---|---|---|
| `ProtectionPolicy` route | PagerDuty routing key (sent in the body); webhook or Slack URL (a bearer token) | the route's `endpoint`; the URL in the Secret | the policy's UID, the sink kind, and the PagerDuty endpoint |
| `BackupDestination` `SecretKeys` grant | S3 key pair and session token | `spec.storage` | the destination's UID and its whole archive route (bucket, prefix, region, endpoint, addressing, transport) |
| `RetentionPolicy` `enforcement.credentialSecretRef` | a DELETE-capable S3 key pair | the destination it resolves to | the policy's UID, that destination's route, and `spec.scope.prefix` |
| inline archive `secretRef` (`Backup`, `BackupSchedule`, `Restore.sourceArchive`, a restore `Preflight`'s legacy source) | S3 key pair | the location the runner dials | the LOCATION: every field that shapes the URL — scheme, bucket, endpoint, region, addressing, `allowHttp` — never the prefix |

For S3, SigV4 never sends the secret key, but every request carries the access
key id, a signature an endpoint can replay within its window, and any session
token, in clear headers; for PagerDuty the routing key itself travels.

**The rule is §20.9's.** A credential Secret is used only when its
`logweir-binding` key equals the binding the controller computed for the object
and endpoint the Job was built for. Every Job carries the expectation as a
literal and the Secret's key as an **optional** `secretKeyRef`:

| Credential variables | Binding pair |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | `LOGWEIR_ARCHIVE_CREDENTIAL_BINDING[_EXPECTED]` |
| `LOGWEIR_EVIDENCE_AWS_*` | `LOGWEIR_EVIDENCE_CREDENTIAL_BINDING[_EXPECTED]` |
| `LOGWEIR_EVIDENCE_READ_AWS_*` (a check pod) | `LOGWEIR_EVIDENCE_READ_CREDENTIAL_BINDING[_EXPECTED]` |
| `PAGERDUTY_ROUTING_KEY`, `NOTIFY_WEBHOOK_URL`, `NOTIFY_SLACK_WEBHOOK_URL` | `NOTIFY_{PAGERDUTY,WEBHOOK,SLACK}_CREDENTIAL_BINDING[_EXPECTED]` |

Every runner compares the pairs **before it builds a store or composes a
request**, and refuses an absent or different binding with
**`CredentialBindingMismatch`**:

* `backup run`, `restore run`/`drill run`: exit 3,
  `refusal-reason=CredentialBindingMismatch`, the `Backup`'s or `Restore`'s
  terminal state;
* the check runner (`Preflight`, the catalog sync, evidence fetch): the
  `CredentialBindingMismatch` check code on the row, before any store exists —
  and, for a `Preflight`, on `destination.credentialBound` for every grant the
  run would present, probed or not (below);
* `logweir-retention`: exit 3, `retention-refusal=CredentialBindingMismatch`,
  `Enforced=False/CredentialBindingMismatch` on the `RetentionPolicy`; nothing
  is deleted. The refusal **stands** until a later run is harvested (FX-20c):
  the controller reads no Secret, so only the next run can see a rebound one,
  and the evaluation passes in between publish neither `Enforced=True` nor
  "enforced by Logweir" over it — `status.enforcement` reads
  `RecommendationOnly` and `status.guarantees.ageExpiry` `NotEnforced`, the two
  fields the console's retention panel reads, which also prints the
  `Enforced=False` reason;
* `logweir notify deliver`: `notify-result=<sink>:refused` for that sink only
  (the others are still attempted), and
  `NotificationsDelivered=False/CredentialBindingMismatch` (§7e).

A Job builder that projected a credential without its expectation would hand
the runner an unchecked credential; `job::build` gives such a credential the
never-satisfied `unbound:missing-expectation`, so the omission fails closed.

**Test access, and every readiness check, compares every grant's binding
(FX-20c).** A check compares a binding where it opens a store, and some grants
are never opened by a check: `archiveWrite` (a check may not write into the
archive prefix, so `destination.archivePrefixWritable` is execution-only), and
a separate `evidenceWrite` Secret on a check with no marker probe. Before
FX-20c a destination whose only grant named another destination's
`archiveWrite` Secret therefore tested **READY** — `destination.credentialProjected`
said `Projected` — while every backup of it was refused
`CredentialBindingMismatch` (PoC batch 4, FX-20 F6). Now:

* The controller lists, in the check plan, every `SecretKeys` grant the run
  would present — `destination.grantBindings: [{role, secretName}]`, references
  only:

  | `Preflight` operation | Grants listed |
  |---|---|
  | `DestinationAccess` (*Test access*) | every grant the destination **declares** (`archiveWrite`, and `archiveRead`, `evidenceWrite`, `evidenceRead` when present), whichever roles the test exercises |
  | `Backup` | `archiveWrite` and `evidenceWrite` (a missing `evidenceWrite` is `archiveWrite`'s Secret) |
  | `Restore` | the source destination's `archiveRead`, and the evidence destination's `evidenceWrite`, each held to **its own** destination's binding |
  | `SourceConnection` | none (no destination) |

  A workload-identity, `ControllerIdentity` or absent grant carries no binding
  and is not listed (FX-20b is the workload-identity follow-up).
* Each listed grant reaches the check pod as a **binding-only pair**: its
  Secret's `logweir-binding` as an optional `secretKeyRef`
  (`LOGWEIR_ARCHIVE_WRITE_GRANT_BINDING`, `LOGWEIR_ARCHIVE_READ_GRANT_BINDING`,
  `LOGWEIR_EVIDENCE_WRITE_GRANT_BINDING`, `LOGWEIR_EVIDENCE_READ_GRANT_BINDING`)
  and the destination's binding as a literal (`…_GRANT_BINDING_EXPECTED`). **No
  credential variable rides with it**: a grant the check does not exercise is
  compared and never used, so nothing is dialled with a foreign Secret to learn
  that it is foreign, and no Secret value reaches a status, a log or the API.
* The runner answers **`destination.credentialBound`** — **blocking**, from the
  check Job, expiring with the other credential rows (15 minutes) — by
  comparing each pair the way every run does (several bindings in one key are
  accepted, an absent expectation is refused). `ready`/`CredentialBound` when
  every listed grant is bound; otherwise `notReady`/`CredentialBindingMismatch`,
  and the message LEADS with one entry per refused grant — its `spec.access`
  field, its Secret, and `no binding` or `foreign binding` — so the per-grant
  answer survives the 512-character status cap; the destination is the row's
  scope (the refused grant's, on a restore spanning two destinations, where the
  entry names it too). The row's facts carry one
  `<grant>=bound|CredentialBindingMismatch` per listed grant (rendered into the
  message as `[archiveWrite=CredentialBindingMismatch; …]`). Neither binding
  value is ever written. **The remedy never tells anyone to bind the refused
  Secret to this destination** — on a thief's row that Secret is another
  destination's: give this destination its own Secret (enter the credential
  through the console, or bind a Secret only it names with
  `scripts/bind-credential.py`, which refuses a Secret another object names),
  and treat a Secret two objects name as an incident. Every
  `CredentialBindingMismatch` text the runners and the controller write says
  the same (FX-20c review).
* The controller expects the row whenever the plan lists a grant, so a runner
  that does not answer it leaves the verdict `unknown` — **readiness is never
  `ready` for a destination a run would refuse on a binding**. The product API
  returns the row unchanged in the preflight's `checks` (and the destination's
  `lastTest` follows the verdict); the console's *Test access* panel shows it
  with the other blocking rows.

```text
destination.credentialBound  notReady  blocking  CredentialBindingMismatch
  archiveWrite (Secret `lwd-primary-archive-write`: foreign binding): a run that
  presents it is refused before it builds a store; nothing was dialled
  [archiveWrite=CredentialBindingMismatch]
  scope: BackupDestination/fx20-thief
```

Upgrade and rollback: roll the controller and the runner together (the chart
does). A plan that lists a grant carries a new field, which an older runner
refuses at startup (`deny_unknown_fields`, exit 3): the `Preflight` lands
`phase: Failed`, reason `CheckContractMismatch`, naming `grantBindings`, and
nothing is dialled (§21.9). A plan with no Secret-backed grant is
byte-identical to before. An older controller lists nothing, so a newer runner
emits no binding row and the verdict is what it was. Nothing is stored: a
verdict recorded before the upgrade keeps its rows until its expiry, and
re-testing adds the row. Rolling back removes the row and the pairs; the
check reverts to the pre-FX-20c overclaim, and every run still refuses a
foreign Secret.

**Changing the endpoint never keeps the credential.** A destination's
`spec.storage` and `spec.transport.security` are immutable, so another endpoint
is another destination with another UID. A `RetentionPolicy` is bound to the
route of the destination its `destinationRef` resolves to NOW, so a destination
deleted and re-created under the same name elsewhere changes it. A
`ProtectionPolicy`'s spec is mutable, so its PagerDuty `endpoint` is in the
binding: edit it, and the routing key is refused until the Secret is bound
again. An inline archive is bound to its location — every field that shapes
the URL the runner dials, the region included — so a plan or URL naming
another bucket, endpoint, region, addressing style or transport is refused.

**A region that is not a region name is refused outright.** Without an
endpoint, the S3 client builds the host from the region
(`s3.<region>.amazonaws.com`), so a region is part of where a request goes. A
storage block whose `region` does not match `^[a-z0-9-]{1,32}$` is refused by
name, `StorageRegionInvalid` (`refusal-reason=GuardRefused`), by `restore
run`/`drill run` before the archive is read, by `backup run` at phase −1, by
every engine renderer and by every object-store client Logweir builds — a
second rule beside the binding, which a region spelled `x@attacker/` would
otherwise have kept the victim's (FX-20's review F1). The `BackupDestination`
CRD, the product API (`region_invalid`) and the chart's `archive.s3.region`
refuse the same spellings at admission.

**Where the binding comes from.**

* **The console** never names an existing Secret for a destination: a credential
  is entered once and becomes a Secret owned by and bound to the destination
  ([api.md](api.md)); a rotation writes the new value to a new bound Secret.
* **`kubectl`**: read the object's published binding —
  `BackupDestination.status.credentialBinding`,
  `RetentionPolicy.status.credentialBinding`,
  `ProtectionPolicy.status.credentialBindings[]` (one per route and channel) —
  and write it into the Secret under `logweir-binding`, as §20.9 shows.
* **An inline archive** has no object of its own (a `Backup` is one-shot and a
  schedule mints them), so its Secret is bound to the location:
  `v1:location:sha256:<hex>` over `scheme`, `bucket`, the endpoint
  (lower-cased, trailing `/` removed, `aws` for none — the installation's
  `AWS_ENDPOINT_URL` when the plan or URL names none), the region (the
  installation's `AWS_REGION` when the plan names none), the addressing style
  and `allowHttp`: every field that shapes the URL. Any object in the
  namespace may then use that Secret, but only at that location — what a
  `BackupDestination` in the namespace already allows. An inline `Restore`
  whose plan writes `evidence:` to another bucket or endpoint presents the
  credential at both, so it needs both bindings (below).
  `scripts/bind-credential.py --location` computes the value.

**One Secret, several bindings.** `logweir-binding` may hold several bindings
separated by whitespace or commas; one equal to the expectation is enough. Each
is an explicit authorization by whoever wrote the Secret — the same act as
copying the credential into a second Secret, and no wider. Add one by hand only
for a credential you mean to share (an inline restore's archive and evidence
buckets, say), never for a Secret another object also names (that is the
incident in the upgrade below).

**Upgrade: bind one Secret to one object, after an inventory.** A credential
Secret made by an earlier release carries no binding, and every use of it is
refused until it does. Binding is the theft §20.9 describes if it is done in a
loop, so it is done with `scripts/bind-credential.py`, one Secret and one
object at a time:

1. **Before the roll, suspend** every `BackupSchedule` and `RehearsalSchedule`
   whose destination, connection or inline archive uses a Secret, and expect
   in-flight runs re-created after the roll to be refused until step 3.
2. Apply the CRDs (additive: three `status` fields) and roll the controller,
   the runner and the console together. Wait for each object to publish its
   binding.
3. For each credentialed object, run the tool **dry**, read what it prints,
   have the credential's owner confirm the endpoint, then apply:

   ```
   python3 scripts/bind-credential.py --context <ctx> --namespace <ns> \
     --kind BackupDestination --name primary --secret lwd-primary-archive-write
   python3 scripts/bind-credential.py --context <ctx> --namespace <ns> \
     --kind BackupDestination --name primary --secret lwd-primary-archive-write \
     --apply --confirm-endpoint '<the endpoint it printed>'
   ```

   `--kind` is `BackupDestination`, `RetentionPolicy`, `ProtectionPolicy`
   (`--route` when one Secret serves routes with different bindings) or
   `KafkaCluster`; an inline archive is `--location s3://<bucket> --endpoint
   <url|aws> --region <region|none> --path-style true|false --allow-http
   true|false` — for a `Restore` its plan's `source.storage` (and `evidence`),
   for a `Backup` or a schedule the controller's `AWS_ENDPOINT_URL`,
   `AWS_REGION`, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` (path style is its
   negation) and `AWS_ALLOW_HTTP`. The binding it writes is the one it
   **computes** from the object's UID and the spec it prints (the same forms
   as the product's, checked against one fixture), so what the owner confirms
   is what is bound. The tool refuses — exit 3, nothing written — a Secret that
   any other object names (an **incident**: find out which object is the
   owner's, delete the other, and treat the credential as exposed if it ever
   ran), a Secret owned by or minted for another object, a Secret already bound
   to something else, an object that does not name the Secret or has not
   published its binding, an object whose published binding is not the one its
   spec gives (a status that lags an edit, or a `RetentionPolicy` whose
   destination was re-created: let the controller reconcile and run it again),
   and a region that is not a region name. It patches one key, preconditioned
   on the Secret's `resourceVersion`.
4. **Resume the schedules.**

**A destination created through the console before this release** also carries
an `api.logweir.dev/request-sha256` taken over the secret key it was created
with (PROD-01.3's F1): remove it (`kubectl annotate backupdestination <name>
api.logweir.dev/request-sha256-`), and rotate the key if it could be guessed —
the same hash is in that create's audit record, which only a rotation retires.

Rollback: an older controller and runner ignore every binding variable and the
new status fields; bound Secrets keep working, and the extra key is inert.

**What this does not close.**

* The kubelet projects a foreign Secret's value into the refused pod before the
  runner refuses it (§20.9's residual): the pod mounts no ServiceAccount token,
  sends nothing first, and exits.
* **Secret `patch` is equivalent to Secret `get`** for every credential here,
  as for a connection's (§20.9): grant it accordingly.
* **A workload-identity grant names a ServiceAccount, not a Secret**, and is not
  bound: a destination author who names another team's IRSA-annotated
  ServiceAccount beside an endpoint they control would have requests signed
  with that role's temporary credentials (FX-20 class sweep, owed).
* **A destination's CA reference is mutable and not bound.** The endpoint is
  immutable, so presenting the credential elsewhere still needs a principal who
  can both edit the CA reference and intercept traffic to that endpoint.
* **A webhook or Slack URL is the credential**: its host is whatever the
  Secret's writer put there, and only that writer can change it.
* **The product API cannot inspect an inline archive's `credentialRef`**: it
  names a Secret, as before, and the runner enforces the location binding.
* **A standing rehearsal authorization's scope does not name the source or
  evidence storage** (PLAT-14.3b): a `Restore` under one may name any inline
  archive and Secret without a per-run approver. The location binding and the
  region rule are what keep that Secret at its own location.

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

### 21.0 The four operations

`spec.request.operation` names what the check is about, and CEL rule P3 ties
exactly one block to it.

| `operation` | Block | What it needs | What it runs |
|---|---|---|---|
| `Backup` | `backup` | a source `KafkaCluster`, a destination or a legacy archive, 1–1000 **named** topics | the whole D2 §6.3 Backup catalogue, and the three capability rows of the source (§21.6c) |
| `Restore` | `restore` | a draft plan or an existing `Restore`, a target, the source and evidence destinations — or, for a point with no saved destination, `legacySourceArchive` (§21.8) — the recovery point | the target, plan, archive and approval rows, and the target's capability row (§21.6c) |
| `DestinationAccess` | `destinationAccess` | a `BackupDestination` and 1–4 roles | the `destination.*` rows for those roles, and `destination.credentialBound` over every `SecretKeys` grant the destination declares (§20.10) |
| `SourceConnection` | `sourceConnection` | one `connectionRef` — and nothing else | `connection.resolved`, `connection.credentialProjected`, `connection.authenticated`, `connection.clusterIdentity`, `runner.*`, `configuration.policy` and `configuration.egress` (execution-only) |

```yaml
apiVersion: logweir.dev/v1alpha1
kind: Preflight
metadata: {name: pf-conn-1, namespace: team-a}
spec:
  request:
    operation: SourceConnection
    sourceConnection:
      connectionRef: {name: source}
```

**Why `SourceConnection` is its own operation and not a narrower `Backup`.**
"Does this connection answer?" is the question a console's *Test connection*
control asks, and until this build nothing could answer it: a `Backup` check
reaches `connection.authenticated`, but its request cannot be rendered without
a destination and at least one named topic, so asking an operator to supply
those in order to test a connection would have been a different question
wearing the same label. The check plan it renders (`sourceConnection`) carries
one connection and has no field for anything else, so a plan that tried to
smuggle a destination into a connectivity test is refused by the runner before
it opens a socket.

**What it does not report.** `connection.topicsDescribable` is **absent**, and
deliberately. That row's whole vocabulary is about a topic the requester NAMED
— `TopicNotFound` is an existence fact about one name and `TopicNotAuthorized`
a visibility fact about one name — so over an empty selection `ready` would be
a green verdict about the empty set and `unknown` on a blocking row would pin
every connection test at `unknown` for ever. What this principal can see is a
`TopicDiscovery` question (§20), not this one.

**The pod is narrower too.** No destination credential is projected, no CA
bundle is mounted and **no signing key is mounted**: nothing would be signed by
a dial, and a check pod holding the installation's signing key to answer a
question about a broker is blast radius bought for nothing. `signer.rostered`
and `destination.*` are therefore not reported either — a row about a key or a
grant the pod was never given is not a verdict.

**`connection.clusterIdentity` asks a narrower question here.**
`ClusterIdentityChanged` — the broker naming a different cluster than
`KafkaCluster.status.clusterId` records — is blocking, because that is a fact
about the connection itself. `SourceIsAllowlistedTarget` is **not** reported:
whether a cluster may be backed up is a `Backup` question, guarded by the
runner's phase −1 rail, and reporting it here made a successful dial to a
legitimate restore target read `not ready` under a remedy advising a backup
nobody asked for.

**Upgrade and rollback.** The block and the enum value are ADDITIVE to the
CRD: every existing `Preflight` is byte-for-byte unaffected, no conversion is
written and nothing is re-reconciled. Apply the CRDs before rolling the
controller forward, as §"Upgrade, rollback and legacy Jobs" says for every
additive field, and roll the controller and runner images **together** — a
runner image without the `sourceConnection` plan kind refuses the plan in its
contract step and the check lands on `Failed / CheckContractMismatch`, visibly,
rather than doing something narrower.

Rolling BACK is the one direction that needs an action. A `SourceConnection`
value is a new member of a CLOSED enum, so a previous controller cannot decode
a `Preflight` carrying it — unlike an additive field, which it would ignore.
Before rolling the controller back, delete the connectivity checks:

```bash
kubectl --context docker-desktop get preflights -A \
  -o jsonpath='{range .items[?(@.spec.request.operation=="SourceConnection")]}{.metadata.namespace}{" "}{.metadata.name}{"\n"}{end}'
kubectl --context docker-desktop delete preflight -n <ns> <name>
```

They are transient by construction — the garbage collector removes terminal
ones after `policy.preflight.retentionSeconds` anyway — so there is nothing to
preserve. Their Jobs and `ConfigMap`s go with them through the owner cascade.

**If they are not deleted, the cost is not confined to them.** The reconciler
watches `Api::all`, so an object the previous controller cannot decode is
neither a crash nor a per-object refusal: the list/watch fails and retries, and
**no `Preflight` in any namespace is reconciled** until the undecodable ones are
gone. A rollback that skips the deletion looks like every readiness check in the
installation quietly stopping, with nothing in the object to say why.

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
| `Failed` | **no result could be produced** — `ResultUnreadable`, `RunnerContractUnsupported`, `CheckContractMismatch`, `DeadlineExceeded`, `Stalled`. Terminal for this object: it is not run again. Fix the cause and create a new `Preflight` |
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

`scope` names the object a row is ABOUT — `{kind, name, uid}` — and every row
carries one. A row that has a better referent uses it (the `KafkaCluster`
principal a connection dialled, the `BackupDestination` a grant belongs to, the
`TrustRoster` a signing key is or is not on, the topic a collision names);
anything left over takes the referent its authority implies, so a `podStatus`
row names the Pod the kubelet reported on, a `checkJob` row names the check Job
and a `controller` row names the `Preflight` itself. Before the Job has a pod a
pod-status row names the Job, and before a plan could be rendered at all every
row names the `Preflight`. A verdict stored by an older controller may carry
rows with no `scope`; the field is optional and nothing reads it as a
discriminator.

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
forms, PEM blocks, S3 XML bodies and unstructured base64 or hex runs of 40
characters or more are replaced, and the result is capped. That cap is why a
message quotes a SHORT digest (`sha256:1f4c2b8a9e07…`) while the whole hash is
in `status.binding.planHash`, which is a field and not prose.

**Redaction is by key name and by secret shape, and never by "any long
token".** A remedy has to name the thing it asks you to go and fix, so the
public identifiers survive whole: a `signerKeyId` (the SHA-256 of a
SubjectPublicKeyInfo DER — it is on the `TrustRoster`, which is how you roster
it), a content digest, `runner.image`'s `imageID`, a backup set's id (a manual
run's UUID, or a scheduled run's `<schedule uid>-<yyyymmdd>-<hhmmss>[-r<k>]`,
exactly as the controller mints it), an object key, a segment path, and the NAME of a Secret and of the data key inside
it. So does the kubelet's own `couldn't find key password in Secret
<namespace>/<name>`: D2 §6.5 says a Secret name is a public reference, and a
message that named neither the Secret nor the key was the one row an operator
could not act on. A credential VALUE is removed under a key name
(`password`, `token`, `aws_secret_access_key`, `sasl.password`) at ANY length,
and unkeyed at forty characters and up — an AWS secret access key's exact width
— **unless it is indistinguishable from a content digest**: a raw key printed as
64 or 128 lower-case hex characters reads exactly like a SHA-256 or SHA-512, and
no rule can refuse it while digests have to survive. That residual is stated in
full, with the probability argument, on `is_hex_digest` in
`crates/logweir-core/src/check_contract.rs`. Print secrets under a key name, not
bare.

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
execution-only with `WriteNotProbed`. The same rule holds for a `Backup`
readiness check and for a `DestinationAccess` check that requests
`EvidenceWrite` (the console's destination test requests it whenever the
destination configures `evidenceWrite`).

The opt-in is read from the object on every pass, for both operations. It used
to be hard-coded off, which gave an operator who had opted in a row whose message
said their destination configured no probe — a status contradicting the spec,
which is the defect this kind exists to close — and a `DestinationAccess` check
kept that behaviour until defect DESTINATIONACCESS-IGNORES-WRITEPROBE: it asked
about evidence writability and always answered `WriteNotProbed`, a check that
could not fail.

**The marker is written as the `evidenceWrite` principal.** When
`spec.access.evidenceWrite` names a different Secret or ServiceAccount than the
grant the check's destination credential was resolved for, the plan names that
grant and the pod is projected it (see §7a, *The write probe is the
`evidenceWrite` principal's*); the row's fact `grant=evidenceWrite` and its
message say which principal answered. Two workload identities on two different
ServiceAccounts cannot share one check pod: that is `destination.resolved` →
`ExecutionContextConflict`, and no Job is created — the same refusal a `Backup`
or `Restore` pod with those two grants meets. A `DestinationAccess` check that
requests `ArchiveRead` resolves its destination credential for `archiveRead`
even when `ArchiveWrite` is also requested (it used to take `archiveWrite`,
which is never probed), so `destination.archiveListable` is the `archiveRead`
principal's answer.

**The evidence read is the `evidenceRead` principal's, by the same rule.**
`destination.evidenceReadable` reads its absent probe key AS the destination's
`evidenceRead` grant. When that grant is a different Secret or ServiceAccount
than the checked grant, the plan names it (`evidenceRead: {credentials,
secretName | serviceAccountName}`) and the pod is projected its keys as
`LOGWEIR_EVIDENCE_READ_AWS_*` — a third set, beside `AWS_*` and
`LOGWEIR_EVIDENCE_AWS_*` — or runs as its ServiceAccount; the row's fact is
`grant=evidenceRead`. It used to be answered by the checked grant (for a
`Backup` check, the archive-WRITE grant — the one D2 rule R9 forbids ever
reading evidence back with). A grant no check pod holds — `ControllerIdentity`,
or none configured on a `DestinationAccess` check that asks for the role anyway
— is answered `unknown` / `EvidenceReadNotConfigured` (advisory) and nothing is
read. A grant the resolver refuses (e.g. `ControllerIdentity` on a location
the installation policy does not list yet, `ControllerIdentityNotAllowlisted`)
is answered by the controller: `destination.evidenceReadable` advisory
`unknown` / `EvidenceReadNotConfigured`, with the refusal in its message —
and ONLY that row: `destination.resolved` stays `DestinationValid` and the
archive and marker rows still run. A workload identity that cannot share the
check pod's ServiceAccount refuses a `DestinationAccess` check
(`destination.resolved` → `ExecutionContextConflict`, no Job), because the check
asked for that principal; on a `Backup` check, whose run never reads evidence in
its own pod, the controller answers the advisory row `unknown` with both
ServiceAccounts named, and the verdict is unchanged.

**The archive-write grant is never probed, and its row is never green.**
`destination.archivePrefixWritable` stays execution-only
(`ArchivePrefixWriteVerifiedOnlyAtExecution`) even when the destination opts in
to the marker and a `DestinationAccess` check requests `ArchiveWrite`: the marker
is under `logweir/readiness/`, which proves nothing about the archive prefix, and
a check may not write into the archive prefix to find out. The row's message
names the archive prefix and says it was not write-probed; the run's own guards
answer it. **Its binding is compared all the same** (FX-20c): the grant's
Secret's `logweir-binding` is compared with no request on
`destination.credentialBound`, a blocking row, so a destination whose
`archiveWrite` Secret was written for another one is `notReady` here and not
only when its backup runs (§20.10).

**The probe also proves the store enforces conditional create** (RECEIPT-DUP).
A backup runner claims each execution with a create-only put before its engine
starts, and that claim is a lock only on a store that honours
`If-None-Match: *`. So the probe creates a fresh marker a second time and
requires the second create to be refused. A store that reports conditional put
unsupported (the client falls back to HEAD-then-PUT), or that accepts the
second create, makes the row **`notReady / ConditionalCreateUnsupported`** —
the grant is there, but every backup to that store would exit 4
`ExecutionClaimUnproven`. The probe uses the same key, the same principal as the first create (the
`evidenceWrite` principal, above) and the same `s3:PutObject` on
`logweir/readiness/*`; nothing else is needed. **Without
`writeProbe` the requirement is proven at the first backup instead**, which
exits 4 before any data is written.

| Object store | Conditional create (`If-None-Match: *`) |
|---|---|
| MinIO `RELEASE.2025-09-07T16-13-09Z` (the compose and lab image; compose and the chart's demo MinIO now run the project's rebuild of it, `third_party/minio-mirror/`) | **Supported, measured** — a private container ran the claim and the probe; the rebuild answers the second create with the same `412` (smoke of 2026-09-24) |
| MinIO releases older than that | `[UNVERIFIED — needs a run against an older MinIO release]` |
| AWS S3 | `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]`; `object_store` sends `If-None-Match: *` by default |
| GCS, Azure Blob | `[UNVERIFIED — native conditional create in object_store, not run against either provider]` |
| any S3-compatible store with `AWS_CONDITIONAL_PUT=disabled`, or one that ignores the header | **Unsupported** — `ConditionalCreateUnsupported` at readiness, exit 4 at every backup |

### 21.6 A restore preflight writes nothing

No topic is created, altered or deleted. The collision answer comes from
targeted metadata per mapped name and from a **validate-only** `CreateTopics`
(`AdminOptions::validate_only(true)`) — both inside the check pod, with the
restore's own credential, and never from the controller. The controller patches
its own `/status`, its own Job's `ttlSecondsAfterFinished` (after the status
commit, so garbage collection cannot race the relay) and its own owned
`ConfigMap`s, and nothing else.

The archive rows read the source `BackupDestination`'s own key space. A backup
set's manifest is `<spec.storage.prefix>/<backupId>/manifest.json` and its
segments are `<prefix>/<backupId>/topics/<topic>/partition=<n>/segment-…` —
exactly where the runner writes them, because the manifest names its segments
RELATIVE to the prefix and the prefix is joined at the storage boundary. A
destination that sets `spec.storage.prefix` therefore gets the same answers as
one that does not; there is nothing to configure, and a preflight that could
only be green for a prefix-less destination was a bug (fixed 2026-09-18 — the
check read `<backupId>/manifest.json` at the bucket root and answered
`archive.backupSet notReady AccessDenied`). If you are re-running a preflight
that reported that against a prefixed destination, create a new one: `Preflight`
specs are immutable and the stored verdict is not revised in place.

### 21.6a The approval rows read the `Approval`, not the roster

`approval.state` is the `Approval`'s own verdict, relayed. The check reads
`status.verified`, the `Verified` condition's **reason** and its **message**,
`status.matchedKeyId` and `status.approverKeyWindow` — the last of which also
decides this row's own `expiresAt` — and it derives nothing of its own about the
approver key.

| what the `Approval` says | `approval.state` |
|---|---|
| `Verified=True` | `ready` `ApprovalVerified` |
| `Verified=False`, reason `KeyIdExpired` | **`notReady` `ApprovalExpired`** — blocking, so the restore is refused |
| `Verified=False`, any other reason (`KeyRetired`, `KeyRevoked`, `KeyNotYetValid`, `KeyIdNotInRoster`, `TrustPolicyConflict`, `SignatureInvalid`, …) | `notReady` `ApprovalNotVerified`, carrying the controller's own message |
| no `Verified` condition yet | `unknown` `ApprovalPending` — a wait, not a verdict |

The order is unchanged and it matters: **the plan hash first**, then expiry,
then verified. An approval that verified against a *different* plan is a
stronger and more actionable finding than "not verified yet", and reporting the
mismatch as a pending approval sends an operator to wait for something that has
already happened.

**Why it does not decide expiry itself (defect PREFLIGHT-APPROVAL-ROSTER).** It
used to resolve the approver key and its `notAfter` out of the cluster-scoped
`TrustRoster` named `default` and compare that with the clock. The `Approval`
controller resolves the same key through the **`TrustPolicy` that governs the
namespace** (§19), which may retire, revoke or narrow a key the roster still
shows as open. The two authorities disagreed in the lab: a `TrustPolicy`
carrying an expiring approver key moved the `Approval` to
`Verified=False, KeyIdExpired` while the preflight, reading the roster, still
reported `ready`. A preflight that authorises what the controller has already
refused is the one direction this kind may never fail in.

**A retirement and a revocation are not expiries**, and this table keeps them
apart for the reason §19 gives: a revocation reported as an expiry sends an
operator to extend a window when the remedy is an investigation, and a retired
key has no window to extend. The closed check vocabulary has one code for "did
not verify"; the reason and the message say which.

**`approval.keyValidity` compares the window the `Approval` publishes.** It is
advisory. It used to compare the restore's deadline with the *roster's*
`notAfter` — the same wrong authority the blocking row stopped reading — and
between 2026-09-19 and 2026-09-21 it compared nothing at all, because no window
was published anywhere (defect APPROVAL-KEY-WINDOW-UNPUBLISHED, recorded rather
than hidden). It now reads `status.approverKeyWindow`, which the `Approval`
controller resolved through the governing `TrustPolicy`:

| what the `Approval` publishes | `approval.keyValidity` |
|---|---|
| a window whose `notAfter` is **before** now + `spec.deadlineSeconds` | `notReady` `ApproverKeyExpiresBeforeDeadline` — **advisory, so it is a warning and not a refusal** |
| a window the restore's deadline falls inside | `ready` `ApproverKeyValid` |
| no window, or one naming a different key than `matchedKeyId` | `unknown` `ApproverKeyWindowUnknown` — never `ready` |

The warning is a forecast, not a verdict: a key that closes mid-run does not
invalidate an approval that verified while it was open, and the row that refuses
a *withdrawn* verdict is the blocking `approval.state` row above. An advisory
`notReady` appears as a warning beside the result and cannot by itself make the
aggregate `notReady` (D2 §6.4).

**Both approval rows re-check at `min(10 m, notAfter)`.** A verdict about a key
must not outlive the key: a preflight that stayed fresh for the full ten minutes
past a `notAfter` four minutes away would let a restore be admitted on a record
that says `ready` about a window that has closed. With no window published there
is nothing to cap at, and the ten-minute catalogue entry stands.

**Upgrading and rolling back.** `status.approverKeyWindow` is additive and
optional. A controller that predates it publishes none, and every reader treats
its absence as `unknown` — so an upgrade in progress shows
`ApproverKeyWindowUnknown` on an advisory row and refuses nothing, and a
rollback returns to exactly that. Existing `Approval` objects need no
conversion: the field appears on the next reconcile of each one, which the
five-minute heartbeat guarantees. Nothing reads the window to decide
authorisation, so a cluster that never publishes one keeps working.

### 21.6b A refused configuration read is `unknown`, never `ready` (FX-4)

`target.timestampBound` reads the target's broker configuration with the
restore's own credential. A credential without DescribeConfigs on the Cluster
resource (Describe does not imply it) now gets `unknown` with code
`BrokerConfigsNotReadable`. Builds before FX-4 answered `ready` with
`TimestampWithinBound`: rust-rdkafka returned the refused read as an EMPTY
configuration, and an empty configuration declares no bound (PROD-04.0 T13,
[the ruling](stability.md#an-empty-configuration-answer-is-a-refused-read-never-no-overrides-prod-040-t13-fx-4)).
The Restore itself needs the same grant: phase 0 of a run with that credential
exits 1 instead of assuming the broker is on `CreateTime`.

A `Backup`'s source credential needs DescribeConfigs on every backed-up topic
for the receipt to record the topic's configuration as `captured`. Without it
the backup still succeeds, the topic reads `captureDenied`, its configuration
model records no settings (only its partition count and replication factor,
[`topic_configuration`](formats/backup-receipt.md#topic_configuration--the-topic-configuration-model-format-130)),
and a later restore's configuration parity names that topic as not assessed
([the receipt field](formats/backup-receipt.md#config_coverage--topic-configuration-capture-coverage-format-110)).
A `Backup` does not look for declarative owners yet (no `KafkaTopic` listing,
no declared owners: PROD-05.1a), so its receipt records `owner_detection: []`,
both readers say each topic's owner was not checked, and the product API
publishes `applyRoute: unknown` for it, never the admin-API route.

Every `Backup` run by a runner from PROD-03.0 on also records, per topic,
whether the archived keys or values carry Confluent wire-format framing — the
receipt's [`schema_dependency`](formats/backup-receipt.md#schema_dependency--does-a-restore-need-a-schema-registry-format-150),
copied into the catalog point and published by the product API. The runner
judges a bounded sample of the segments it just wrote, through the archive
credential it already holds: **no schema registry is contacted, and none needs
to be reachable from the runner**. The sample is at most two segments per
partition for at most eight partitions per topic, streamed, with six bytes kept
per key and value. A segment stored over 16 MiB, a zstd frame declaring a
window over 8 MiB or a body decompressing past 256 MiB leaves its topic
`notAssessed (segmentTooLargeForDetection)`; detection stops after **120 s per
backup** (a hard stop: no read or scan continues past it) and leaves the rest
`notAssessed (detectionTimeBudgetExceeded)`; any other failure is
`segmentUnreadable`. None of these fails the backup. **Memory:** detection
adds at most about **17 MB** to the runner, measured — the worst case is a
16 MiB incompressible segment held while scanned (+16.6 MB); a 1 GiB zstd
bomb, a zstd frame declaring a 128 MiB window and a 250 MiB lz4 body each add
2.4 MB. **Time:** give a `Backup` whose `spec.deadlineSeconds` is tight up to
120 s more for detection after the engine, or detection may be cut short by
the Job's deadline before the receipt is signed. A topic with no record reads
`notAssessed (noRecords)`. Detection reads only Confluent's payload prefix:
schema ids in record headers, Apicurio's 8-byte ids and other registries'
framing read `notDetected`
([the stated limits](formats/backup-receipt.md#schema_dependency--does-a-restore-need-a-schema-registry-format-150)).

### 21.6c Capability rows: what the endpoint itself can do (PROD-01.2)

The rows above ask what this **principal** may do. Four more ask what this
**endpoint** can do. A Kafka-compatible endpoint is not always Apache Kafka,
and a connection test passing says nothing about a restore
([the compatibility contract](support-matrix.md#the-compatibility-contract)).

| Row | Operation | Gating | `notReady` means | What to do |
|---|---|---|---|---|
| `connection.engineProtocol` | `Backup` | blocking | `EngineProtocolUnsupported`: the source does not serve a request version the engine sends to read from it. The message names each request and the range the endpoint serves. | Back up from an endpoint that serves them. Every supported Apache Kafka line does. |
| `target.engineProtocol` | `Restore` | blocking | `EngineProtocolUnsupported`: the target does not serve a request version the engine sends to write to it. Redpanda v26.2.4 answers this way: the engine sends Produce v8 and it serves Produce v0–v7. | Restore the archive into a cluster that serves them. An endpoint that cannot be a target can still be a source. |
| `connection.topicConfigsReadable` | `Backup` | advisory | `TopicConfigsNotReadable`: this principal may not read the configuration of the topics the detail names. The backup still runs. | Grant DescribeConfigs on the topic, or accept a point whose configuration is `captureDenied` and whose timestamp type is not recorded (§21.6b). |
| `connection.groupTypes` | `Backup` | advisory | `GroupTypesNotListed`: the endpoint's group listing names no group type (it serves ListGroups below v5; Apache Kafka 3.7 and Redpanda v26.2.4 do). The backup of the topics is unaffected. | A backup that selects consumer groups records each as excluded (`GroupTypeNotCaptured`), never as captured. Back up from an endpoint that serves ListGroups v5, or select no group. |

**The engine sends fixed versions and never negotiates**, so an endpoint that
does not serve one closes the connection, and the run fails with only
`kafka-backup backup exited 1` or `restore exited 1`. The two blocking rows
say which request it would be, before the run. The SASL pair (SaslHandshake
v1, SaslAuthenticate v2) is asked for only on a SASL connection.

**Each row reads the endpoint's own answer**: the ApiVersions response on a
real connection made with the operation's credential, and for the
configuration row one DescribeConfigs per selected topic. When the answer
could not be read the row is `unknown` (`ApiVersionsNotObserved`, or the
read's own timeout code), never `ready`. When the connection did not
authenticate, or a selected topic is not describable, the rows are `unknown`
with `BlockedByPrerequisite`.

**The three ApiVersions rows answer for the cluster only when EVERY broker
answered.** The engine may be sent to any broker, so the check reads the
cluster's broker list from metadata and waits for an ApiVersions answer from
every broker on it, and from every bootstrap address the `KafkaCluster`
names, inside one budget of at most 10 s (less when little of
`timeoutSeconds` is left; with under 2 s left it does not dial and says so).

- `ready` or `notReady` carries the fact `brokersAnswered: 3 of 3`: distinct
  brokers of how many the cluster lists, never a count of connections. When
  the brokers' answers differ (a rolling upgrade), the row is judged on the
  versions every one of them serves, and its message says they differ.
- A broker that did not answer in time makes the row `unknown`
  (`ApiVersionsNotObserved`), with a message such as "2 of 3 broker(s) the
  cluster lists answered ApiVersions within the check's budget; no answer from
  broker 3 (kafka-3.example:9092)". So does a bootstrap address nobody
  answered at. Bring the broker back, or take a decommissioned address out of
  `spec.bootstrapServers`, and create a new `Preflight`.
- **A broker the cluster no longer lists is not asked.** A cluster that has
  dropped a stopped broker lists the others, and through bootstrap addresses
  that all answer the row says `2 of 2`. The row is about the brokers the
  cluster says it has.
- **What it opens.** For the length of the observation, one connection to each
  bootstrap address and one to each listed broker: six on a three-broker
  cluster with three bootstrap addresses. A cluster that caps connections per
  principal below that answers `unknown`. Builds before the fix round of
  PROD-01.2 read whichever one to three brokers a sparse client had dialled
  and called the result the endpoint's.

**An advisory row never changes the verdict.** On Apache Kafka 3.7 every
`Backup` check carries `connection.groupTypes` as a warning beside a `ready`
verdict. It matters only to a backup that selects consumer groups.

**The target's record-timestamp bound.** `target.timestampBound` is `unknown`
with `TimestampBoundNotReported` when the target's broker configuration
answers without either bound key (Redpanda keeps the bound per topic); under
an older controller the code is `BrokerConfigsNotReadable` (§21.9). Builds
before PROD-01.2 answered `ready`, "declares no record-timestamp bound", for
an endpoint that had declared nothing. The same builds published
`status.topicPreflight.timestampType: CreateTime` on a `Restore` whose target
had not reported its timestamp type; the field is now absent in that case.

**An unreachable advertised address.** When the bootstrap address answers and
the brokers the cluster advertises do not, `connection.authenticated` (or
`target.authenticated`) is `notReady` with `BrokerUnreachable`, and its
message says exactly that: the bootstrap answered and named the cluster, and
the advertised listeners are not reachable from the runner. Check
`advertised.listeners` for the listener the bootstrap address belongs to. A
`SourceConnection` check, and `logweir cluster-probe`, read only the cluster
id and pass against such a cluster.

### 21.7 Skipping a check is not answering it

`spec.request.skipChecks` leaves a row out of the run. The row is still
reported, with `state: skipped`, and **a skipped blocking check keeps the
overall verdict `unknown`**. The same applies to the one row a draft cannot
answer: a `Preflight` over `planBytes` has no `Restore` for an approver to sign,
so `approval.state` is `skipped` with `SubjectNotCreated` and the verdict is
`unknown` however green everything else is. The aggregate stays that way on
purpose. The console's restore wizard reads it as submittable only in exactly
that shape: every other blocking row `ready` and this one row skipped with this
one code. Creating the `Restore` is what makes the row answerable. Anything
else that is not `ready` still refuses the Create (`ui/README.md`, *The
readiness check holds the submit*).

### 21.8 What this build does not do

- **A backup readiness check over an inline `legacyArchive` is not run.** It
  would need the archive-WRITE principal a legacy `Backup` Job holds, which no
  check exercises. Such a request is reported `phase: Failed` with
  `ArchiveUrlUnreadable` and a message saying to create a `BackupDestination`
  and use `destinationRef`. It is never a verdict about the operation.
- **A restore readiness check over a recovery point with no saved destination
  (`legacySourceArchive`, e.g. a point written by `v0.1.5`) reads the archive as
  the restore Job will.** The check Job gets a `SecretKeys` grant over
  `legacySourceArchive.secretRef` with the two keys a legacy restore Job
  projects (`access-key-id`, `secret-access-key`), and the location of the
  approved plan's `source.storage` — a region or endpoint the plan leaves out is
  taken from the installation policy's `legacyArchiveAddressing`, the same
  values the controller forwards to every legacy runner Job; `path_style` and
  `allow_http` are the plan's. Transport is `InsecureHTTP` only for
  `allow_http: true` with an explicit `http://` endpoint, and a custom endpoint
  is path-style (§15.5's mapping). The `archive.*` rows are that Job's real
  reads, scoped `InlineArchive/<url>`. It fails closed: a URL that is not
  `s3://`, a request with no `secretRef`, a plan that does not parse or does not
  read S3, or a location the destination rules refuse (an `http://` endpoint
  with `allow_http: false`) is `destination.resolved notReady` and no Job runs.
  `plan.bindings` holds the plan to the named inline archive and to the
  recovery point's own `spec.archive.url` (`PlanDestinationMismatch`). A check
  of an EXISTING `Restore` (`restoreRef`) reads with that Restore's own
  `spec.sourceArchive` — URL and Secret — and a request naming another archive
  or another Secret is `destination.resolved notReady`,
  `PlanDestinationMismatch`, with both named and no Job. The console records
  the Secret a check was started with and refuses the Create when the Secret on
  screen differs (the Secret is not in the plan bytes, so the plan hash cannot
  see it change). The advisory evidence row names the handle by role, never its
  URL. And
  `recoveryPoint.state` does not compare the point with a location derived from
  its own plan. One advisory row, `destination.evidenceReadable`
  (`EvidenceReadNotConfigured`), is published when the plan's `evidence:`
  bucket is not the bucket of the controller's archive handle
  (`LOGWEIR_ARCHIVE_URL`), or no handle is configured: the restore would run
  and its verification would read `NotAttempted` with no completion (§15.1a).
  It is absent — never green
  — when the buckets match. A controller older than this build answers the same
  request `Failed`/`ArchiveUrlUnreadable`; nothing in the `Preflight` object
  changes shape.
- **`recoveryPoint.state` compares the two locations, and this is what it
  answers.** A recovery point is only restorable from the destination it was
  written to. The point's frozen `locationDigest` (`Backup.status.destination`,
  §10) is compared with the digest this check resolved for the source
  destination — never with a digest re-derived from the live
  `BackupDestination`, because an edit after the freeze must not be able to make
  a moved point look settled.

  | The point's block | This check's source destination | Answer |
  |---|---|---|
  | present, equal | present | `ready`, `RecoveryPointSucceeded` |
  | present, different | present | `notReady`, `RecoveryPointLocationMismatch`, with BOTH digests in the message |
  | **absent** | present | `unknown`, `RecoveryPointLocationUnknown` |
  | present or absent | absent | `ready` — this check makes no location claim, and `plan.bindings` is the row that holds a legacy plan's location to account |

  **A recovery point archived before this field existed publishes no location**,
  so the third row is the upgrade case and it is deliberately `unknown` rather
  than `ready`: a blocking row that answered `ready` would be reporting a
  comparison nobody made. As a blocking `unknown` it holds the whole verdict at
  `unknown` until somebody confirms by hand which destination that point is in,
  or restores from one archived through the destination. Nothing is refused
  that was not refused before — an `unknown` verdict authorises nothing, exactly
  as a `skipped` blocking row does.
- **A catalog point's `recoveryPoint.state` is read from the catalog row
  (PLAT-15.2).** With `catalogPointRef`, the controller reads the catalog, the
  page `ConfigMap`s its status names (by those names only; each must be
  `immutable` and hash to its recorded digest), and the namespace's `Backup`s in
  at most four pages of 500, and answers, in this order:

  | What it found | Answer |
  |---|---|
  | no such catalog, or the view does not list the point | `notReady`, `RecoveryPointNotFound` |
  | no view yet, an expired view, a page gone, mutable or altered | `unknown`, `CatalogPointViewUnavailable` |
  | a `Backup` of the same receipt digest (set id for a digest-less run) whose own verdict is anything but absent, `NotAttempted` or `Valid` | `notReady`, `CatalogPointRefusedByController` |
  | the row is not `selectable` | `notReady`, `CatalogPointNotSelectable`, both axes named |
  | the row's signer, judged again against the namespace's CURRENT trust (the `TrustPolicy` resolution the `Approval` controller makes) for `EvidenceSigning` at the row's recovery point, is refused — revoked or retired since the catalog synced, no longer listed, or its public key unusable | `notReady`, `CatalogPointSignerUntrusted`, the key and the reason named |
  | that trust cannot be resolved (two policies claim the namespace, none does, or the read failed), or the row names no signer | `unknown`, `CatalogPointSignerUnknown` |
  | more `Backup`s than the four pages read | `unknown`, `CatalogPointViewUnavailable` — asked after the refusal it could not rule out |
  | the plan has no `source.point`, or its binding or `source.backup` is not the row's | `notReady`, `CatalogPointBindingMismatch` |
  | otherwise | `ready`, `CatalogPointSelectable` |

  The catalog joins the check's referents. A `ready` here authorises nothing:
  the runner re-verifies the binding against the archive before any data moves.
  The signer row uses the recovery point as the claimed signing time, because
  the view carries no receipt `finished_at`: a key retired between the two
  instants passes here and is refused by the runner, which reads the real one.
- **A plan bound to a recovery point has its archive judged as the runner will
  judge it (FX-14).** A plan that carries `source.point` is restored only if
  the runner's binding proves the point against the archive, and before FX-14
  the check Job read the set's CURRENT manifest alone: it could answer `ready`
  over a set written again after the point was signed, which the runner then
  refused (exit 3). `archive.backupSet` now makes the binding's own
  comparisons, in its order, and `archive.coverage` and `archive.segments` are
  `unknown`, `BlockedByPrerequisite` behind every refusal, because they would
  describe a manifest the run will not accept.

  **What the plan may make the check read is decided first, from the plan
  alone.** A preflight runs before any approval, for whoever can create one,
  with the namespace's archive-read credential, and `source.point.receipt_key`
  is that person's text. So nothing is opened or read unless the binding is
  well-formed, the plan's `source.backup` is the set the check reads
  (`backupSetRef`; never `latestCompleted`), and the receipt key is the key a
  backup of that set writes — `logweir/backups/<backupId>/<run id>.receipt.json`,
  compared byte for byte, each id one path segment. A key under another
  prefix, of another set, with a relative or nested path, or naming any object
  that is not a receipt, is refused by name and not fetched. The receipt is
  then read only after that set's manifest was read under the destination's
  own prefix.

  **Which ids the check can confine.** Each of the two ids must be one path
  segment that the store addresses exactly as written: printable ASCII, and a
  space is allowed, so a set named `nightly 7` is checked like any other. An
  id that is empty, is `.` or `..`, or holds a `/`, a control character (a tab,
  a line break), a character that is not ASCII, or one of the
  characters an object-store path rewrites (`\ % ? # * ~ | ^ { } [ ] < > "` and
  the backtick) is read by the store at another key than its text says. A plan
  that names such an id is `notReady`, `PointBindingMismatch`, with nothing
  read. The comparison is of bytes: for the set `nightly 7`, neither
  `nightly%207` nor the id with a tab, a no-break space, or a space before or
  after it is that set.

  **The confinement is by set id, not by archive prefix.** The receipt
  namespace `logweir/backups/<set id>/` is bucket-wide: it does not sit under
  a destination's prefix, so two destinations in one bucket share it. What
  keeps them apart is the manifest-first read above (a set this destination's
  prefix does not hold is `BackupSetNotFound`, and its receipt is never
  fetched) and
  [the execution claim](formats/backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id),
  which admits one engine run per set id per bucket. The shape of the key
  alone does not.

  | What the check found | `archive.backupSet` |
  |---|---|
  | the binding is malformed, names another set than the one the check reads, or names a receipt key outside that set's receipts | `notReady`, `PointBindingMismatch`; nothing was read |
  | the receipt is not at its key, its bytes are not the bound digest, or the object at its key is larger than the read cap for a receipt (64 MiB, §7b.4) | `notReady`, `PointBindingMismatch` — one answer for all three where the store answers 404 for an absent key (measured on MinIO), so there it does not say whether an object exists at a key the plan chose. An object over the cap is refused on the size the store reports, by the one read an absent receipt costs, and its size is not repeated. On AWS S3 an absent receipt can differ from the other two: see *Where an absent receipt is not a 404* below the table |
  | the receipt does not derive the bound point id, is not a receipt, attests another manifest digest, or describes another set or manifest key than the one this restore reads | `notReady`, `PointBindingMismatch` |
  | the receipt pins a manifest version the bucket still holds, and it is not the current one | `notReady`, `ManifestSuperseded`: the set was written again after the point was signed; restore from another point |
  | the pinned version could not be read (a 403, an outage, a version larger than the 256 MiB read cap for a manifest) | the store's own code (`AccessDenied` is `notReady`, `Timeout` is `unknown`; a version over the cap is `notReady`, `StoreErrorUnclassified`), with the remedy that names `s3:GetObjectVersion` |
  | the receipt could not be read | the store's own code |
  | the receipt pins a version this bucket does not hold (a copy of the archive, an unversioned bucket, a version expired or deleted) and the manifest hashes to the bound digest | `ready`, `ManifestReadable`; the message says `PointPinUnchecked` and the remedy carries the note the catalog gives such a point |
  | the manifest does not hash to the bound digest | `notReady`, `PointBindingMismatch` |
  | otherwise | `ready`, `ManifestReadable`, naming the point |

  **Where an absent receipt is not a 404.** On AWS S3 a principal without a
  listing grant over a key gets 403, not 404, for an object that is not there.
  The documented minimal `archiveRead` grant (§7a) is such a principal for the
  receipt namespace: its `s3:ListBucket` is conditioned on the archive prefix
  and it holds only `s3:GetObject` on `logweir/backups/*`. With that grant an
  absent receipt reads as `AccessDenied` ("could not be read") and a receipt
  with other bytes as `PointBindingMismatch`, so the two are told apart. The
  difference is confined to the receipt keys of the plan's own set, and it has
  a cost for the operator: a deleted receipt can look like a missing grant.
  [UNVERIFIED — needs a real AWS S3 bucket and a credential source]

  No answer repeats anything read from the store — not a digest, not a version
  id, not a field of the receipt; a message names only what the plan and the
  request already state. The receipt's SIGNATURE is not checked here: a check
  Job is given no evidence keyring, `recoveryPoint.state` above judges the
  signer, and the runner verifies the signature before any data moves. A plan
  with no `source.point` is judged exactly as before. The grants these reads
  need are in §7a.
- **`gc.rs` IS wired, since D2 W11.** A terminal `Preflight` is collected an
  hour after `result.expiresAt` (or after `observedAt`, when it never produced
  a verdict with an expiry), by the reconciler's own hourly pass, with a UID
  precondition and at most twenty per pass. `preflight.retentionSeconds` in the
  installation policy is the window; §22.3 is the rule and its four bounds.
  `kubectl delete preflight` still works, and the owner cascade takes the Job
  and the `ConfigMap`s with each one either way. **To keep a verdict, copy it
  out** — there is no per-object retention override.
- **`signer.rostered` is EXECUTION-ONLY for a restore.** The verdict needs the
  runner's public `signerKeyId`, which rides on `signer.privateKeyUsable`; the
  landed `restorePreflight` check-plan contract carries no `signer_path`, so a
  restore check pod is never given a key to report. The row is still published,
  with `SignerKeyIdNotObserved` and a message naming the reason, and it does not
  gate — a blocking row nobody can answer would pin every restore preflight at
  `unknown`. The runner still validates its signer before it writes anything
  (PLAT-02.2). Closing it means adding `signer_path` to the restore request.
- **`signer.rostered` and the restore allowlist read the namespace's RESOLVED
  trust.** The preflight resolves the namespace exactly as the `Approval`
  controller does (`trust::resolve`). When a `TrustPolicy` governs it, the
  runner's reported key must be a usable `EvidenceSigning` key of THAT policy
  that may sign new evidence now (`Active`, inside `notBefore`/`notAfter`):
  `SignerRostered`, else `SignerNotRostered` (not carried) or `SignerKeyExpired`
  naming the refusal (`KeyRetired`, `KeyRevoked`, `KeyIdExpired`,
  `KeyNotYetValid`), scoped to the `TrustPolicy`. A namespace two policies claim
  is `TrustRosterNotLoaded` naming both; a policy list the controller could not
  read is `unknown`, `TrustUnknown` — never the roster's answer. A scratch
  restore's `target.clusterIdentity` compares against the governing policy's
  `allowedTargetClusterIds` (the list the runner's `allowed-clusters.json` is
  rendered from), and a backup's source check against the same list; with the
  policy list unreadable both are `unknown`, `TrustUnknown`, rather than the
  roster's allowlist, so the preflight cannot read `ready` on a guess (a row
  decided by the broker itself — identity changed, target is the source —
  keeps its answer). The governing policy is a binding referent, so editing it
  makes the verdict stale. With no policy governing the namespace every answer
  is the `TrustRoster`'s, unchanged. Builds before this judged both rows by
  `TrustRoster/default` alone, so a TrustPolicy-only namespace read
  `TrustRosterNotFound`/`SignerNotRostered` for a key its policy trusts. The
  code `TrustUnknown` is new and additive; a rolled-back controller never
  writes it.
- **`destination.archivePrefixWritable` is not requested.** A backup readiness
  plan asks for the `archiveRead`, `evidenceWrite` and — when the destination
  configures one — `evidenceRead` grants. The archive-WRITE grant is what the
  run itself exercises, and its row's whole content is "verified by the run", so
  requesting it would add a line and no information. Its BINDING is still
  compared, with the `evidenceWrite` grant's, on `destination.credentialBound`
  (FX-20c, §20.10): a foreign Secret on either is `notReady` here.
- **`destination.evidenceReadable` is only requested when the destination
  configures an `evidenceRead` grant.** Probing a role the object leaves
  unconfigured would report a refusal about a grant nobody asked for; when it is
  configured, the row is answered as that grant (§21.5), and a `ControllerIdentity`
  grant is reported `unknown` rather than read by the check pod. Absent means verification is `NotAttempted` (§7b), and the advisory row
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

**The evidence-write grant in a check plan (mixed versions).** A controller from
this build adds `request.{operationReadiness,destinationAccess}.evidenceWrite` to
a check plan only when the plan writes the marker AND the destination's
`evidenceWrite` grant differs from the checked grant; every other plan is
byte-identical to what earlier controllers rendered. An **older runner** handed a
plan that carries the field refuses it at startup (`deny_unknown_fields`,
`CheckContractMismatch`, exit 3) — the `Preflight` lands `phase: Failed` and
nothing is written as the wrong principal; upgrade the runner image with the
controller. A **newer runner** handed a plan from an older controller (no field)
writes the marker with the destination grant, exactly as before, which on a
separated destination is the old wrong-principal answer until the controller is
upgraded. A `DestinationAccess` `Preflight` against an opted-in destination
starts writing the marker after the upgrade; roll back the controller to stop
it, or set `writeProbe: Disabled`. **`evidenceRead` follows the same rules**:
the field is added only when the `EvidenceRead` role is carried and the grant
differs from the checked grant or is one no check pod holds; an older runner
refuses such a plan (exit 3); a newer runner handed an older plan reads with
the destination grant, as before. **A runner that refuses a plan** reports
`phase: Failed`, reason `CheckContractMismatch`, with a message saying the runner
image is older than the controller and should be upgraded, naming the plan field
it refused (`evidenceWrite`, `evidenceRead`) when its own log line says which.
**`grantBindings` (FX-20c) follows the same rules**: a destination plan carries
it only when the run would present a `SecretKeys` grant (§20.10); an older
runner refuses such a plan (exit 3, `grantBindings` named); a newer runner
handed an older plan answers no `destination.credentialBound` row, as before.

**A restore check over `legacySourceArchive` (this build).** The plan it renders
is an ordinary `restorePreflight` plan — no new field — whose source destination
is the inline archive as the restore Job reads it (§21.8), so a runner of any
build that knows `restorePreflight` runs it. A controller from before this build
answers the same request `Failed`/`ArchiveUrlUnreadable`, as it always did;
after a rollback, re-run the check or restore without it. The one Restore
controller row added, the advisory `destination.evidenceReadable`, never
changes the aggregate.

**A point-bound plan's archive rows (FX-14).** No plan field and no object
changes: the runner reads `source.point` from the plan bytes it already
hashes. A runner image from this build answers `archive.backupSet` for such a
plan as §21.8 says, with two codes that are new to the closed vocabulary,
`ManifestSuperseded` and `PointBindingMismatch`. A controller OLDER than this
build cannot parse a result that carries either, so the `Preflight` lands
`Failed`, `ResultUnreadable` — never `ready`; upgrade the controller with the
runner image, as for every check code. A runner older than this build reads
the current manifest alone, as before, and can still answer `ready` over a set
the runner's binding will refuse. A `Preflight` already stored is not revised;
create a new one after the upgrade.

**A refused broker-configuration read (FX-4).** A runner image from FX-4 on
answers `target.timestampBound` `unknown` (`BrokerConfigsNotReadable`) where an
older one answered `ready` (§21.6b). No code, field or plan shape is new, so
the controller and runner may be upgraded in either order. Rolling back the
runner brings back the old `ready` answer.

**Capability rows (PROD-01.2).** A controller from this build lists the
capability rows of §21.6c in every `Backup` and `Restore` check plan
(`request.{operationReadiness,restorePreflight}.capabilityChecks`). An **older
runner** refuses such a plan at startup (exit 3): the `Preflight` lands
`phase: Failed`, `CheckContractMismatch`, naming `capabilityChecks`. **Upgrade
the runner image with the controller**; unlike the conditional fields above,
this one is in every `Backup` and `Restore` check.

- **Which setting moves.** The chart has two image values, `controllerImage`
  and `runnerImage`. Its defaults move together, but a release installed with
  `runnerImage` pinned (a private registry, the ECR example of
  [install.md](install.md)) keeps the old runner through
  `helm upgrade --reuse-values` unless `--set runnerImage=…` moves it. An
  install from `logweir.yaml` has no chart value: the controller reads
  `LOGWEIR_RUNNER_IMAGE` from its own Deployment, so set it to the new runner
  image in the same change that rolls the controller.
- **A `Preflight` that failed this way stays `Failed`.** The phase is terminal
  (§21.2): the object is not run again when the runner image is corrected.
  Create a new `Preflight`.
- **A `Preflight` in flight across the upgrade.** By reading the controller,
  not by a run: the rows a result must hold are derived from the request as
  THIS controller renders it, so a check Job the old controller created and
  the new one sees finish is missing the capability rows, which are then
  reported `unknown` and blocking (`BlockedByPrerequisite`, "the check Job did
  not report this row"). That one object's verdict is `unknown`, never a
  `ready` it did not earn. Create a new `Preflight` after the upgrade.

A **newer runner** handed a
plan from an older controller (no field) emits no capability row, and the
older controller reads its result as before. The new answer of
`target.timestampBound` follows the same rule: a plan from this build's
controller gets the code `TimestampBoundNotReported`, and a plan from an older
controller gets the same `unknown`, message and remedy under
`BrokerConfigsNotReadable`, a code that controller already reads. Rolling the
runner back brings back the old `ready` answer.

## 22. The installation policy, the RBAC rows, and the console admission policy

Everything an **administrator** controls that a namespace operator cannot. The
three parts are separate on purpose: §22.1 is who may do what,
§22.2 is the one document that tunes the check framework, and §22.4 is an
optional admission rule that narrows a grant RBAC cannot narrow.

### 22.1 The roles, and the one verb that changed

`logweir.yaml` and the chart ship **six** ClusterRoles, all of them unbound
except the controller's ([install.md](install.md) step 5 binds the five human
ones).

| Role | What it is for |
|---|---|
| `weirkeeper` | The controller. Bound by one `ClusterRoleBinding` at install. |
| `logweir-viewer` | Read on all fourteen kinds. No `/status` resource is named — `get` already returns it — and no verb on `configmaps` or `secrets`. |
| `logweir-operator` | `create` on the operational kinds; `update`/`patch` on `backupschedules`; `create`/`patch` on `backupdestinations`, `topicdiscoveries` and `preflights`. |
| `logweir-approver` | `create` on `approvals`, `get`/`list` on `preflights`, nothing else. |
| `logweir-trust-admin` | Cluster-scoped read and write on `trustpolicies`. The only holder of a write verb on the kind. |
| `logweir-retention-admin` | Namespaced write on `retentionpolicies` — the only holder of one, because moving a policy to `Enforce` is what makes an installation delete archive objects (§7f). No `delete`. |

The console API's own principal is not in that list: the chart renders it under
`api.enabled` (`<release>-api`, `<release>-api-trustpolicies` and
`<release>-api-trustroster`), and [install.md](install.md) tables every grant.
Its two cluster-scoped trust reads are `get`/`list` on `trustpolicies` and `get`
on the ONE roster, `trustrosters` with `resourceNames: ["default"]`. The console
reads `TrustRoster/default` and the governing `TrustPolicy` only to compare them
with the referents a readiness check recorded. A refused read leaves the verdict
`unverifiable`, and so served stale.

**What an operator may change on the three check kinds is the CRD's CEL rule,
not RBAC.** A `BackupDestination`'s location and transport are sealed; its
access grants and CA reference are editable. A `TopicDiscovery` and a
`Preflight` have exactly one mutable field, `spec.cancelRequested`, and the CEL
rule permits only `false → true`. The grant is a plain `patch` because that is
what `kubectl edit`, `kubectl apply` and the console all send; `update` is
absent because nothing issues one.

**`logweir-approver` reads `preflights` and that is safe to state.** A readiness
verdict is redacted by construction (no credential value, no broker error body,
no URL carrying userinfo), references no Secret, and authorizes nothing on its
own — the execution-time guards are what decide (§21.1). Reading one cannot
approve anything; it lets an approver see whether the check for *this plan*
said `ready` before signing.

**`logweir-trust-admin` needs a `ClusterRoleBinding`**, and a `RoleBinding` of
it grants nothing at all, silently: `TrustPolicy` is cluster-scoped. Bind it to
somebody who does not hold `logweir-operator` — a trust policy decides whose
keys may sign an approval, so one person holding both could add their own key
and then approve their own restore. It carries **no `delete`**: deleting a
policy does not retire a key, it removes the binding that governs a namespace.

#### The legacy in-cluster UI proxy

The optional `ui.enabled` proxy (§16, §19) gains `get`/`list` on
`backupdestinations`, `topicdiscoveries` and `preflights` and nothing else. No
`create`, no `patch`: the page has no form for any of them and the new flows
are console-only. Without the read it would render a destination-backed
schedule as though its archive were unconfigured. It still holds no verb on
`configmaps`, so it shows an inventory's **counts** and never its pages.

#### `delete`: exactly two resources, and why the doctrine sentence moved

The controller's ClusterRole used to grant `delete` on nothing, and
`crates/logweir/tests/manifest_lint.rs` asserted that twice. It now grants it on
**`topicdiscoveries` and `preflights`**, in one rule, and on nothing else.

The reason is that nothing else can collect them. A `TopicDiscovery` is one
observation with an immutable spec and a `Preflight` is one verdict about one
plan; neither is owned by another object, so no ownerReference cascade reaches
them, and a custom resource has no `ttlSecondsAfterFinished`. A namespace that
refreshes an inventory every minute would fill etcd with objects a fresh check
has already replaced.

Everything else is unchanged and is asserted to be:

* **No `delete` on** `backups`, `restores`, `approvals`, `backupschedules`,
  `kafkaclusters`, `backupdestinations`, `trustrosters`, `trustpolicies`,
  `recoverycatalogs`, `jobs`, `configmaps`, `pods` or `events`. A finished Job
  still goes by the API server's TTL controller, and plan and result
  `ConfigMap`s still go by owner cascade — which is why collecting a check
  leaves nothing behind.
* **No delete capability against object storage** in the controller or in
  anything it links (§15.1). The only archive deleter is a `RetentionPolicy`'s
  enforcement worker, under its own credential (§7f). That is a different
  subject, guarded by a different gate, and this change did not touch it.

### 22.2 `weirkeeper-policy`: the one administrator-owned document

A `ConfigMap` named `weirkeeper-policy` in the **release** namespace, under the
key `policy.json`. The Deployment finds it through
`LOGWEIR_INSTALLATION_NAMESPACE` (the pod's own `metadata.namespace`, from the
downward API) or an explicit `LOGWEIR_POLICY_CONFIGMAP=<namespace>/<name>`.

**It is optional.** No document — or no `ConfigMap` — means the documented
defaults below, and `configuration.policy` reads **ready**. An install that
renders none is a supported install, not a degraded one.

```json
{"version": 1,
 "checks": {"maxActivePerNamespace": 4, "maxActiveTotal": 20,
            "maxActiveDiscoveriesPerConnection": 1,
            "maxEvidenceFetchActivePerNamespace": 4},
 "discovery": {"freshSeconds": 900, "retentionSeconds": 86400, "keepPerConnection": 5,
               "defaultMaxTopics": 20000, "hardMaxTopics": 50000,
               "visibilityAttestations": []},
 "preflight": {"defaultTimeoutSeconds": 120, "retentionSeconds": 3600},
 "engine": {"allowUnverifiedCustomCa": false},
 "evidence": {"controllerIdentityLocations": []},
 "legacyArchiveAddressing": {"endpoint": "", "region": "", "allowHttp": false,
                             "virtualHostedStyle": false},
 "runs": {"maxManualBackupsActivePerNamespace": 4,
          "maxManualRestoresActivePerNamespace": 2}}
```

| Block | What it decides |
|---|---|
| `checks` | How many check Jobs may run at once, per namespace and in total. Evidence fetches have their **own** pool, so verification cannot be starved by interactive checks. Over a ceiling a request is `Queued` with reason `ConcurrencyLimited` — not an error. |
| `discovery.freshSeconds` | After this an inventory reads **stale**, never wrong. |
| `discovery.retentionSeconds` / `keepPerConnection` | The collector's two rules (§22.3). |
| `discovery.hardMaxTopics` | The ceiling a request's `maxTopics` is clamped to. It only ever LOWERS a request. |
| `discovery.visibilityAttestations` | The **only** route to `visibility.state: attestedComplete` (§7c). |
| `preflight.retentionSeconds` | The collector's window. |
| `discovery.defaultMaxTopics`, `preflight.defaultTimeoutSeconds` | **Withdrawn (FX-10, 2026-10-05). Nothing reads them.** They were documented as the defaults for a request that names none, but no request ever names none: the CRDs default `spec.request.maxTopics` (20 000) and `spec.request.timeoutSeconds` (120) at admission, and the console writes both. The controller accepts a document with or without them and applies no range rule to them, only the type: any whole number from 0 to 4 294 967 295 is accepted, and `null`, a negative, a fraction, a quoted number or a larger number is refused like any malformed field, which fails the whole document closed (as it did before FX-10). The chart renders them at fixed values: `hardMaxTopics` if lower than 20 000, else 20 000, and 120. That keeps the document readable by a controller older than FX-10, which requires both keys and refuses a document without them. `charts/logweir/README.md`, *Withdrawn values*, has the upgrade notes. |
| `engine.allowUnverifiedCustomCa` | Whether a `BackupDestination` may carry a private CA the archive engine cannot verify. |
| `evidence.controllerIdentityLocations` | Where the controller's own identity may read evidence from. An unlisted location is refused with `ControllerIdentityNotAllowlisted`, so the empty default is the closed direction. |
| `runs` | P10: how many MANUAL runs one namespace may have holding a runner slot at once — "Back up now" `Backup`s and admitted manual `Restore`s. Over a ceiling a run is `phase: Queued` with `Admitted=False` reason `ConcurrencyLimited` and `status.queue.limit`, with nothing created, and starts in creation order as slots free (§ *Manual runs may queue*). Scheduled, catch-up and retry `Backup`s and a `RehearsalSchedule`'s `Restore`s are never counted or queued. Absent is the defaults (4 and 2), so a document written before the block keeps bounding manual runs — and the chart renders the block **only** when a value differs from those defaults. |
| `legacyArchiveAddressing` | The installation's inline-archive addressing, published read-only and reserved for `POST …/destinations:from-legacy` (D2 §3.12 branch (b)), which does not read it yet and refuses with branch (c) (§15.5). The restore readiness check over a `legacySourceArchive` point reads `endpoint` and `region` from it; `allowHttp` and `virtualHostedStyle` are read by nothing until that route lands. |

**Who may write it is the access-control statement.** `create`/`update` on a
ConfigMap in the release namespace is a chart or cluster administrator;
`logweir-operator` names no `configmaps` at all. That is what makes an
attestation an *administrator* statement, which is what
`attestedComplete` requires.

**An attestation is nine required fields**, and every one of them must be
non-blank:

```json
{"id": "att-orders-prod", "namespace": "team-a", "kafkaCluster": "source",
 "clusterId": "M29I2S7FQPyHBEX12Vx7XA", "principal": "User:backup",
 "attestedBy": "platform-admin@example.invalid",
 "attestedAt": "2026-09-15T00:00:00Z", "expiresAt": "2026-12-15T00:00:00Z",
 "statement": "User:backup has DESCRIBE on literal Topic:* with no DENY; reviewed ACL export 2026-09-14"}
```

It applies only on an **exact** match of namespace, `KafkaCluster` name, the
cluster id the runner read from the broker, and the principal Logweir
presented — and only before `expiresAt`, and only to a listing that was not
truncated. A blank `clusterId` or `principal` matches nothing while *looking*
like an attestation somebody can rely on, which is why
`charts/logweir/values.schema.json` refuses one at install time. **Logweir never
verifies the statement**: the UI renders "attested by *X* at *T*; not verified
by Logweir".

**A document the controller refuses fails closed.** It is parsed with unknown
fields rejected and ten range rules applied (P10 added the two `runs` floors;
FX-10 removed the two withdrawn fields' rules). A refusal produces empty
attestations and an empty evidence allowlist, plus one advisory
`configuration.policy notReady PolicyUnreadable` row on a `Preflight`. **It
also writes one `WARN` line naming the failing rule** —
`the installation policy ConfigMap was REFUSED` — so
`kubectl -n logweir-system logs deploy/weirkeeper | grep REFUSED` is the answer
to "why did my attestation not work?". That line is the only signal outside a
`Preflight`, and it is there because everything else about a refused policy
looks healthy.

**Three things validate this document, and they check different halves.**

| layer | what it can check | when |
|---|---|---|
| `charts/logweir/values.schema.json` | every **per-field** bound, with `hardMaxTopics` pinned to `check_contract::MAX_TOPICS_CEILING`. Required fields, the attestation's nine, `additionalProperties: false` (the two withdrawn keys stay allowed, unbounded and ignored). | `helm install` / `helm template`, before anything is applied |
| `charts/logweir/templates/policy.yaml` | the **one cross-field** rule JSON Schema draft-07 cannot express — `maxActiveTotal >= maxActivePerNamespace`. A named `fail`, quoting both values. | the same moment |
| `weirkeeper::check::policy::parse` | all of the above, plus `deny_unknown_fields`, and it is the **authority**. | every 30 s in the controller |

So a **chart** install cannot produce a document the controller then refuses;
that gap was real until fix round 1 (the schema admitted `hardMaxTopics` up to
200 000 while the parser refused anything above 50 000, and `helm install`
succeeded). For a **hand-written** file the schema is still the thing to
validate against, and the `WARN` line is the backstop.
`crates/weirkeeper/tests/chart_policy.rs` reads the schema and the constants
together so the two cannot drift apart again.

The digest of the policy that was actually in force is recorded on every check's
`status.binding.policyDigest`, so a verdict can be traced to the document it was
computed under.

### 22.3 Retention: the check kinds are collected, and nothing else is

`Backup`, `Restore` and every other kind are still never deleted by Logweir
(§9). The two transient check kinds are, by the reconciler that owns them, on
the hourly pass a terminal object already takes.

| Kind | Collected when |
|---|---|
| `TopicDiscovery` | `now > observedAt + discovery.retentionSeconds`, **or** it is outside the newest `discovery.keepPerConnection` terminal discoveries for the same connection UID. |
| `Preflight` | `now > result.expiresAt + preflight.retentionSeconds`, or `observedAt + …` when the check never produced a verdict with an expiry. |

Four bounds make that safe, and each has a test:

1. **Terminal only.** A `Queued` or `Running` check is never collected, however
   old: its pod holds the only copy of a relay nobody has read.
2. **UID preconditions.** Every delete carries the object's UID, so a
   same-named replacement created between the listing and the delete is
   refused with a 409 rather than removed.
3. **Twenty per pass.** A namespace holding thousands drains over many passes
   rather than in one burst against the API server.
4. **A truncated listing applies the age rule alone.** The API server returns
   items in name order, not by `observedAt`, so one page of a truncated listing
   is not "the newest" anything and the keep-last-N rule cannot be computed
   over it.

A failed delete is logged and the pass continues — garbage collection is never
the reason a check's own reconcile reports an error. The result `ConfigMap`s
and the check Job go with the object by owner cascade.

**Keep-last-five is per namespace and per connection, so it is a housekeeping
bound and also a small authority.** Anyone who can create a `TopicDiscovery` in
a namespace can retire that namespace's older observations of the same
connection by creating six more — `logweir-operator` holds `create` on the kind
and `delete` on nothing, so this is the one thing an operator can make the
controller remove. It is bounded and it is worth knowing:

* it never crosses a namespace — the collector lists through `Api::namespaced`,
  never `Api::all`;
* it never reaches a non-terminal check, so a running observation cannot be
  displaced;
* it is **not spoofable**. The cohort key is `status.binding.connectionUid`,
  which only the controller writes and only through the `/status` subresource,
  a grant no human role holds. A creator can flood their own connection's
  cohort; they cannot claim somebody else's.

**Why the cohort is not additionally keyed on the creator.** It would need a
trustworthy creator identity on the object, and there is none: `TopicDiscovery.spec`
is `request` and `cancelRequested`, and any `metadata` label or annotation is
written by whoever creates the object. Keying on a value the flooder chooses
makes the rule *weaker*, not stronger — vary the annotation and every
observation is its own cohort of one, so keep-last-five never fires and the
namespace fills for the full 24 hours instead. A real per-creator bound needs
either an admission-time identity stamp or a quota on the kind, both of which
are larger decisions than this rule; the retention window is what bounds the
worst case meanwhile.

**To keep a verdict or an inventory, copy it out.** There is no per-object
retention override; the windows are installation-wide, in the document only an
administrator can write.

### 22.4 Fencing the console's `create secrets` (Kubernetes 1.30+)

The console API (`logweir-api`) holds `create` on `secrets` and **no read
verb**, so a stored credential cannot be read back by any route, any projection
or any future refactor of one. `create` alone is still the widest grant it asks
for: in a namespace it could in principle mint a
`kubernetes.io/service-account-token` Secret for any ServiceAccount there, and
RBAC cannot express "only this shape of Secret".

Both credential builders stamp a distinct, immutable-after-create `type` —
`logweir.dev/object-store-credential` for a destination credential and
`logweir.dev/kafka-sasl-password` for a connection credential — and the
`app.kubernetes.io/managed-by: logweir` label. The shipped
`ValidatingAdmissionPolicy` requires both, for the console principals only:

```yaml
matchConditions:
  - name: console-service-account
    expression: request.userInfo.username in ["system:serviceaccount:logweir-system:logweir-api"]
validations:
  - expression: has(object.type) && (object.type == 'logweir.dev/object-store-credential' || object.type == 'logweir.dev/kafka-sasl-password')
  - expression: has(object.metadata.labels) && 'app.kubernetes.io/managed-by' in object.metadata.labels && object.metadata.labels['app.kubernetes.io/managed-by'] == 'logweir'
```

`failurePolicy: Fail` is safe **because** of that subject test: every other
principal in the cluster — an administrator, the controller, a CSI driver — is
skipped before a validation runs. The binding's action is `Deny`; `Warn` would
leave the grant exactly as wide as it is today while reading, on an audit
surface, as though it did not.

Turn it on with `admissionPolicy.enabled=true` (Helm) or apply
`config/samples/console-credential-admission-policy.yaml` after editing the
principal (kustomize). **It is off by default for one reason and it is not a
security opinion:** `admissionregistration.k8s.io/v1`
`ValidatingAdmissionPolicy` is Kubernetes 1.30+ and Logweir's floor is 1.29,
where the document is rejected with `no matches for kind`.

**It fences the console the chart renders.** Since D0 stage 7 the chart
renders the console's ServiceAccount, `<release>-api` (`logweir-api` for a
release named `logweir`), under `api.enabled`, with its `create
secrets` grant, and refuses to render an admission policy whose
`admissionPolicy.consoleServiceAccountName` is not that account — a name that
fenced nobody would install, look enabled and bound nothing. (Earlier revisions
of this section called the policy inert because the console had not landed;
that is no longer the case.)

**A `create`-only fence assumes there is nothing else to fence.** The policy
matches `CREATE`, because `create` is the only verb on `secrets` any Logweir
principal has ever held. `manifest_lint::no_shipped_role_may_write_or_read_a_secret_it_does_not_name`
is what keeps that true: no shipped role may carry `delete` on `secrets` at any
scope, and `update`/`patch`/`get`/`list`/`watch` only with a `resourceNames`
naming exactly which object (the identity bootstrap's signing key is the one
such grant). A console role that arrived with an unscoped `patch` would bypass
this policy completely, and that test fails instead.

**What it does not do.** A cluster administrator can delete the policy — it
raises the cost of a mistake and of a compromised console, not of a deliberate
administrator. It says nothing about what the console does with a credential it
legitimately creates, and it is not what keeps the value unreadable; the
missing read verb is.

**[UNVERIFIED — no API server has seen either document.]** What would verify it:
on a 1.30+ cluster, apply both, then as the console ServiceAccount create (a) a
Secret of type `logweir.dev/object-store-credential` carrying the managed-by
label, which must be **accepted**, and (b) one of type
`kubernetes.io/service-account-token`, which must be **rejected** with the
message above — then show (b) succeeding once the binding is deleted.

### 22.5 Upgrade, rollback and what an older controller does

**Order.** RBAC before the controller image, as always: the `delete` rule must
be in place before the image that calls it, or the collector 403s on every
terminal pass. The rule is additive, so applying it early costs nothing.

**The policy `ConfigMap` can be created at any time.** The controller caches it
for 30 s and an absent one is the defaults; creating it later turns
attestations on without a restart. Changing it changes the `policyDigest`
recorded on subsequent checks, and never one already written.

**Rolling the controller back** leaves the `delete` rule granted to an image
that never calls it — harmless, and removable. Terminal `TopicDiscovery` and
`Preflight` objects then accumulate again, exactly as they did before this
change; `kubectl delete topicdiscoveries,preflights --all -n <namespace>`
clears them and their Jobs and `ConfigMap`s by cascade. The policy `ConfigMap`
is simply ignored by an older image.

**An older controller with the new roles** reconciles nothing differently: it
does not know the three check kinds, does not call `delete`, and never reads the
policy document. The human roles are additive grants on kinds an older image
ignores.

**The `runs` block (P10) is the one addition an older controller refuses.** A
controller that predates it parses `policy.json` with unknown fields rejected,
so a document that carries `runs` makes an older image's policy **fail closed**
(no attestations, no evidence allowlist, the `WARN` line above). The chart
therefore renders the block **only when a value differs from the defaults** (4
and 2): a default install carries none, and an image-only rollback reads the
document it always did. An install that set `runs.*` rolls the controller image
back together with the chart (`helm rollback`), never alone. The other
direction is safe: this controller reads a document without the block as the
defaults.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
