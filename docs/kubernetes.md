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

## 7. The control plane: six kinds on `logweir.dev/v1alpha1`

**Minimum Kubernetes: 1.29.** That floor is not about the client library — it
is about **CEL validation rules** (`x-kubernetes-validations`), which reached GA
in 1.29 and are how every one of the six CRDs below makes its `.spec`
immutable. On an older API server the rules are dropped rather than rejected,
and a dropped immutability rule is worse than no rule: the object would accept
an edit after approval and nothing would say so.

The CRDs are checked in at [../config/crd/](../config/crd/) and regenerated
with `just crds`; CI re-renders them and diffs the result, so a schema change
arrives as a reviewable diff. Do not hand-edit those files.

| Kind | Scope | What it is |
|---|---|---|
| `KafkaCluster` | Namespaced | A cluster connection: bootstrap servers, `auth{mode, username, secretRef, tls}`, role, and the marker topic that proves a scratch target. `status.clusterId` is read from the broker, never from the spec. |
| `BackupSchedule` | Namespaced | A recurring backup of a **named** topic set (no wildcard, no glob metacharacter). `spec.concurrencyPolicy` is `Forbid` by default or explicitly `Allow`; `spec.suspend` is the only mutable field. `retention{keepLast, keepDays}` **reports** what it would remove and deletes nothing. |
| `Backup` | Namespaced | One archive run, as a Job. Its name and `status.backupId` are a pure function of the trigger, so a duplicate reconcile gets `AlreadyExists` rather than a second partial archive. |
| `Restore` | Namespaced | One restore run, as a Job. **A drill is a `Restore` with `spec.target.mode: scratch`** — there is no `Drill` kind. A `Restore` only ever writes a *new* topic, so it is non-destructive by construction. |
| `Approval` | Namespaced | A DSSE-signed authorisation for one `Restore` or `Backup`. **Four required spec fields**; `approvalBytes` and `sidecarBytes` are the UTF-8 document text, verbatim, never base64. |
| `TrustRoster` | **Cluster** | The keys that may authorise (`approverKeys`) and the keys that may attest (`signingKeys`) — **both carrying public key material** — plus `allowedClusterIds`. Cluster-scoped so a namespace tenant cannot widen its own allowlist. |

`Switchover` is tag 2 and ships in none of the above, not even as a value of
`Approval.spec.subjectRef.kind`. `MetadataSnapshot` is reserved and unbuilt.

**Every `kubectl` command line in this repository names its context
explicitly** — `kubectl --context docker-desktop …`, the `proxy` subcommand
included. That covers every copy-pasteable block and every quoted transcript
above, §1's two `bash` blocks and its verified transcripts included. A bare
`kubectl get pods` in prose or in a code comment names what the tool renders
rather than a line to run, and is not one of them.

### The immutability seals

Five kinds carry one rule on `.spec`: `self == oldSelf`, message *spec is
immutable; create a new object instead*. `BackupSchedule` carries one
**object-level** rule instead, naming every field except `suspend`.

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

**So `Verified`'s `reason` is one of NINE strings, and this is the one place
all nine are named**: `PayloadTypeMismatch`, `SignatureInvalid`,
`KeyIdNotInRoster`, `KeyIdExpired`, `PlanHashMismatch`, `SubjectKindMismatch`,
`RosterNotFound` (install step 1, above), `ReferentNotFound` and
`ReferentHasNoPlanBytes`. A tenth would be a compile error rather than a
surprise: each reason is the name of the enum variant that produced it, and
both `match`es are wildcard-free on purpose.

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

### Concurrency policy uses owned Backup state

`spec.concurrencyPolicy` controls whether different scheduled slots may run at
the same time. The field has two values: `Forbid` (recommended and default) and
`Allow` (explicit opt-in). An existing `BackupSchedule` stored before the field
was added behaves as `Forbid` without an object rewrite. The policy is sealed
with the other schedule inputs, so choosing `Allow` for an old omitted-field
schedule requires creating a replacement schedule. This changes no cron
expression, timezone, missed-slot horizon, catch-up, or retry behavior.

Until PLAT-05 decouples retained history from schedule ownership, replace an
immutable schedule policy only with this drain-and-retain procedure:

1. Set `spec.suspend: true` on the old schedule so it admits no new slots.
2. List every `Backup` whose **controller owner reference UID** equals the old
   schedule UID, and wait until all of them are terminal (`Succeeded`, `Failed`,
   or legacy `Refused`). Do not use the singular `activeBackupRef` as proof that
   the drain is complete.
3. Retain the old, suspended `BackupSchedule`. Its controller owner references
   still anchor the old `Backup` history; deleting it can let Kubernetes garbage
   collection delete that history.
4. Create the replacement under a **different name**, only after the drain.
   The replacement has a different UID and therefore cannot see an old active
   child as its own; starting it before the drain can overlap generations.

Do not delete and recreate a schedule under the same name. That is not a safe
policy migration: deletion can remove history, while the recreated object's new
UID neither adopts nor excludes work owned by the old generation. An old
schedule whose `concurrencyPolicy` field is omitted already behaves as
`Forbid`; suspend that same object and follow the procedure above.

Under `Forbid`, the controller lists Backups in the schedule's namespace and
accepts only children whose controller owner UID is the schedule's current UID.
An absent phase, an unknown phase, and a nonterminal Backup whose Job is missing
all remain active conservatively; the Backup controller may still create or
recreate that Job. Terminal `Succeeded`, `Failed`, or legacy `Refused` Backups
do not block. Completed and missing `activeBackupRef` values are cleared.

Admission of a new slot is a resource-version-checked status reservation, so
two controller replicas cannot admit different slots from the same schedule
state. `status.pendingBackupRef` identifies accepted work between reservation
and child creation; a restart resumes that deterministic child. Once the child
is observed or created, the controller clears the reservation and reports it
through `status.activeBackupRef`. This uses no separate Kubernetes Lease.

| Policy and observed state | Due-slot result | Schedule status |
|---|---|---|
| omitted or `Forbid`; no owned unfinished Backup | Atomically reserve, then create the deterministic slot child | `Scheduled`; pending ref becomes active ref |
| `Forbid`; owned Backup is nonterminal or unknown, including a missing Job | Do not create the new slot | `ConcurrencyBlocked`; slot recorded in `lastMissedSlot` with the blocking Backup named |
| `Forbid`; referenced Backup is terminal | Clear the completed active ref and admit the due slot | New child becomes active |
| `Forbid`; `activeBackupRef` names no owned Backup | Clear the stale ref and admit the due slot | New child becomes active |
| `Forbid`; `pendingBackupRef` has no child after restart | Resume the already accepted deterministic child | Pending ref becomes active ref; no second admission |
| explicit `Allow` | Create the deterministic due-slot child regardless of earlier slots; after a 409, fetch the winner and require the current schedule's complete controller identity | The owned winner is reported; foreign, ownerless, old-UID, or transiently missing winners are errors and are not adopted |

Apply the regenerated CRD before starting the new controller. For a strict
no-overlap upgrade, suspend schedules or stop the old controller before the
rollout: an old and new replica briefly running together do not share the new
reservation protocol. No schedule rewrite is needed, and existing Backup
children remain the run-state authority.

After the updated CRD is installed, an older controller ignores the additive
fields but does not enforce cross-slot `Forbid`. Suspend schedules and drain or
stop the new controller before rollback if overlap prevention must remain
guaranteed; remove neither accepted Backup children nor their pending
reservation during that handoff.

### A slot older than one hour is skipped, and the skip is recorded

The controller has no timer and no leader lease: it re-examines every schedule
every 30 seconds, works out which slot is due, and uses the status reservation
above only when admitting `Forbid` work. A slot that came due more
than **one hour** before the controller looked is **skipped** — a controller
restarted after a week must not fire six days of backlog, because a `Backup`
for a window nobody is waiting for costs the same broker read as one somebody
is.

A skip is a fact, not a silence. It lands in
`status.lastMissedSlot` with a `Ready` condition whose reason is `SlotMissed`,
and the field is never cleared afterwards — it is the audit trail of the skip:

```bash
kubectl --context docker-desktop get backupschedule nightly \
  -o jsonpath='{.status.lastMissedSlot}'
```

`kubectl explain backupschedule.status.lastMissedSlot` states the same
one-hour horizon, so the number is discoverable from the cluster and not only
from this page.

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

**The run identity is the control plane's, not the client's.**

| `spec.triggeredBy` | What must also be true | The run identity (`status.execution.id`, the archive `backup_id`) |
|---|---|---|
| `manual` | no `spec.slot` | this object's **API-server UID** |
| `schedule` | `spec.scheduleRef`, a `spec.slot` of `yyyymmdd-hhmmss`, a controller owner reference to that `BackupSchedule`, and the deterministic name `logweir-backup-<schedule>-<slot>` | `<BackupSchedule UID>-<slot>` |

Anything else — a `manual` Backup naming a slot, a `schedule` claim without the
owner reference the `BackupSchedule` controller writes, an unknown
`triggeredBy`, a non-positive `deadlineSeconds` — is terminal
`ExecutionSpecInvalid`, before anything is created. A hand-written object may
not claim `schedule`: its signed receipt would say `schedule` about a run no
schedule created.

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
  run executes: the identity and trigger above, the source `KafkaCluster`'s
  **UID**, bootstrap addresses, SCRAM username and TLS flag, the topic
  allowlist, the archive URL with the resolved `storage` block and the
  object-store addressing variables this controller forwards, and the runner
  argv, deadline and engine tunables. Its grammar is versioned
  (`logweir.dev/backup-execution-inputs/v1`).
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

One difference is informational and deliberate: a newly observed
`KafkaCluster.status.clusterId` changes the snapshot's bytes but not its
executable inputs, so a probe that lands between the freeze and the Job does
not invalidate the run. Everything else — a recreated source cluster (its UID
is pinned), different bootstrap addresses, a different topic list, a different
archive or endpoint — does.

**A Job that disappears from a nonterminal `Backup` is re-created from those
same frozen inputs** (the ConfigMap is read and verified, never re-rendered),
which is what makes deleting a running Job a retry of one run rather than the
start of another. The Job keeps the `Backup`'s name, so the pod carrying the
exit code is selected by the job-name label **and** by its owner Job's UID: a
deleted Job's pod is never read as the new Job's evidence.

The source connection is configured once on `KafkaCluster`. The probe and each
backup reuse that object's bootstrap servers, SCRAM username, TLS setting and
`auth.secretRef`. For SCRAM, both project the Secret's `password` key into
`LOGWEIR_SOURCE_PASSWORD` with `valueFrom.secretKeyRef`. The controller never
reads the password, and **no Secret name and no credential key is written into
the snapshot or the status**: the references are fixed by the pinned
`KafkaCluster` UID and the CEL-immutable `KafkaCluster.spec` and `Backup.spec`,
so the Job builder derives them from those objects and the ConfigMap — which
has no encryption at rest and a much wider read surface than a Secret — names
none of them. Archive credentials remain separate, under
`Backup.spec.archive.secretRef`.

These Jobs open separate connections: a completed probe leaves no running
client to share with a later backup or its engine subprocess. Each new pod
resolves the same Secret reference, so password rotation applies to new Jobs
without copying credentials into every Backup. A successful probe establishes
reachability at that time; backup permissions and the engine's credential
rendering restrictions are still checked by the backup runner.

A SCRAM source with a missing or blank `auth.secretRef.name` produces
`CredentialNotRenderable` before a plan or Job is created. An absent Secret or
missing `password` key is reported by Kubernetes when it starts the pod.

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

The Approval CRD change is additive and may remain installed during and after a
controller rollback; do not attempt to downgrade or delete the CRD as part of
rollback. Fence controller changes by scaling weirkeeper to zero, then let or
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
`KafkaCluster`'s own `auth.secretRef`, key `password`.

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
six kinds, status patches, Backup creation, Job create/read/patch, Pod and
`pods/log` reads, and ConfigMap create/get. It has no Secret read, pod exec,
pod attach or delete permission, and **no `update` on anything**: every status
write, the `Forbid` slot reservation included, is a merge `PATCH` whose body
carries `metadata.resourceVersion` as the compare-and-set precondition (§10's
three RBAC notes). Inspect [config/rbac](../config/rbac/) for the authoritative
grants.

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
`ExitCodeNotZero` or `OutcomeNotPass`, and whose `message` is the badge label.

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
4. **For each `TrustRoster.spec.signingKeys[]` entry**, build a verifying key
   from its `spkiPem` and check the DSSE sidecar. The first success is `Valid`
   carrying that entry's own `keyId`; otherwise `Invalid` with the last error.
   An **empty** list is `NotAttempted` with

   > the TrustRoster lists no signing key material; add the runner's public key to spec.signingKeys

   — it names itself rather than failing silently, and an `Invalid` there would
   blame every document in the cluster for one missing line in one
   cluster-scoped object.

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

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
