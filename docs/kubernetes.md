# Running Logweir on Kubernetes

v0.1 **does not require Kubernetes** — `logweir drill run` is a CLI that spawns
a local subprocess, and the demo runs on a laptop with `docker compose`. But a
scheduled drill is the point of the tool, and a `CronJob` is how most people
schedule one, so the facts below are part of the release rather than folklore.

Every fact on this page marked **verified** was checked on a live
`docker-desktop` cluster. Facts that were *not* checked live are marked
**unverified** and say so — please do not promote one to the other without
running it.

There is a worked manifest at [../examples/cronjob-drill.yaml](../examples/cronjob-drill.yaml).

---

## 1. The exit-code contract is nearly invisible in Kubernetes

This is the most important thing on the page, because it silently destroys the
signal Logweir exists to produce.

Logweir's exit codes are a published interface
([docs/stability.md](stability.md)):

| Code | Meaning |
|---|---|
| 0 | Pass. |
| 1 | Operational — the drill could not be attempted or continued. **No artifact was written.** |
| **2** | **A drill result that is not a pass. A scorecard WAS written and signed.** |
| 3 | Refused by a guard, before anything ran. |
| 4 | Signing or lock proof failed — and nothing was uploaded. |

**Exit 2 is the most valuable result the tool produces**: the drill ran, it was
measured, and your backup did not meet its objective. That is the finding you
scheduled the drill to get.

**Verified — and re-verified independently on 2026-09-05** with a job whose
container simply `exit 2`s: `kubectl --context docker-desktop get pods` showed
`Error` with no code, the Job's `.status` carried only `BackoffLimitExceeded`,
and the exit code `2` was readable at exactly one path. In Kubernetes the code
appears only at —

```
pod.status.containerStatuses[].state.terminated.exitCode
```

(or `.lastState.terminated.exitCode` after a restart). It is **not** in Job
status, and `kubectl --context docker-desktop get pods` renders every non-zero
exit as a generic `Error`. So to an operator glancing at the namespace, **exit 2 is
indistinguishable from exit 1** — "your backup failed its drill" looks exactly
like "the drill could not run".

**The mitigation, verified working:**

```yaml
spec:
  backoffLimit: 0          # do not retry
  template:
    spec:
      restartPolicy: Never # exactly one pod, one attempt
```

That yields **exactly one pod** (re-verified: `kubectl --context docker-desktop get
pods -l job-name=... | wc -l` returned 1), an immediate `Failed` Job condition, and a
cleanly readable
exit code at the path above:

```bash
kubectl --context docker-desktop get pod -l job-name=<job> \
  -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}'
```

**The alternative is actively harmful, and worse than "buries the code".**
`restartPolicy: OnFailure` with `backoffLimit > 0` **retries a drill that
legitimately did not pass** — it runs your restore again, against the same
archive, expecting a different answer.

Re-verified on this machine's `docker-desktop` cluster on 2026-09-05, with a
job whose container simply `exit 2`s (`backoffLimit: 2`, `restartPolicy:
OnFailure`). What actually happened, in order:

```
Warning  BackOff               Back-off restarting failed container probe in pod ...
Normal   SuccessfulDelete      Deleted pod: logweir-onfailure-probe-79czs
Warning  BackoffLimitExceeded  Job has reached the specified backoff limit
```

The container was restarted **in place**, and then the job controller
**DELETED THE POD**. `kubectl --context docker-desktop get pods` afterwards:
`No resources found`. Since
the exit code lives only on the pod object, **it is not buried in `lastState` —
it is gone**, and the Job records only `BackoffLimitExceeded`. A drill that
found a real problem leaves behind no evidence of which problem it was.

Do not use `OnFailure` for a drill.

**Partly verified on 2026-09-10, and the remaining half is still a mark.**
`podFailurePolicy` (GA) supports exit-code-specific Job actions and would
express "code 2 is a real result, do not retry; code 1 may be retried"
declaratively. Every Job the `Backup` reconciler creates now carries one (§10).

**Verified live** on `docker-desktop` (server v1.34.1), with a controller-built
Job whose runner exited **3**: the rule was **evaluated and matched**, and it
changes the Job's own failure reason. The Job's conditions came back as —

```
FailureTarget, Failed   reason: PodFailurePolicy
```

— and **not** `BackoffLimitExceeded`, which is what a `backoffLimit: 0` Job
without the field reports (§1's first transcript). So the field is **not inert
in the sense of unobservable**: it is what makes `kubectl describe job` say "a
policy rule matched this exit code" rather than "this Job ran out of retries",
which are different facts about the same failure.

**Still a mark:** the *no-retry* half. With `backoffLimit: 0` a single pod
failure already fails the Job, so nothing here demonstrates that the rule
prevents a retry. **The sentence that would verify it:** run a Job with
`backoffLimit: 3`, `restartPolicy: Never`, the same `onExitCodes` rule and a
container that `exit 2`s, and observe **exactly one** pod and no second attempt;
then repeat with the rule removed and observe four. Until someone runs that,
"code 1 may be retried" is a declaration and not a measured behaviour.

### 1b. The log line is how you correlate a drill, and it works at the shipped default

The mitigation above gets you the code off one pod. Tying that pod to the
archive it read, to the metrics textfile it wrote (§5) and to the scorecard in
the bucket is the log's job.

`drill run` writes structured JSON to **stdout**, one object per line. The
default level is **`info` even with `RUST_LOG` unset**, so a pod nobody
configured still emits a correlatable log. Three facts follow:

- **Every line Logweir emits at the default level carries the run id** — on the
  event as `fields.run_id`, or on the entered span as `span.run_id`. It is the
  same id as the scorecard's `run_id` and the same id the `--metrics-file`
  textfile carries as a leading `# logweir run_id=…` comment, so one grep joins
  all three. The scope is real: the default directive pins the dependencies
  that emit `tracing` (`h2`, `hyper_util`, `object_store`, the `quinn` crates)
  to `warn`, because they log from worker threads that never entered the run's
  span and so could not carry the id. Setting `RUST_LOG` yourself replaces that
  scoping — `RUST_LOG=debug` will show dependency lines with no run id.
- Every terminal path emits `drill finished` with `fields.exit_code` and a
  `fields.meaning` string. That line is the one place the §1 distinction —
  "could not run" versus "ran and did not pass" — survives into a log
  aggregator at all.
- `RUST_LOG` overrides the default whenever it is set to anything non-blank.
  `RUST_LOG=warn` keeps the error line and its run id and drops everything
  else. A blank `value:` is treated as unset, not as "log nothing".

`examples/cronjob-drill.yaml` pins `RUST_LOG: info` in the container's `env:`
anyway. That is belt-and-braces rather than the mechanism: it holds the level
where a cluster-wide policy or a base image might otherwise inject a quieter
one, and it makes the level visible in
`kubectl --context docker-desktop get cronjob -o yaml` without reading
Logweir's source.

Pulling the run id out of a failed pod, from your laptop:

```bash
kubectl --context docker-desktop logs job/<job> > drill.log
jq -r 'select(.level=="ERROR") | .fields.run_id' drill.log
```

`jq` is a laptop-side tool here — it is **not** in the runtime image (§2), and
the redirect is deliberate: `kubectl --context docker-desktop logs … | jq`
would work, but piping
`logweir` itself into `jq` replaces the drill's exit code with `jq`'s, and §1
is about not losing that code.

## 2. The engine image is linux/amd64 only

**Verified.** Upstream publishes `osodevops/kafka-backup` for **linux/amd64
only**; no arm64 manifest exists for any tag. On an arm64 node it runs under
emulation, but **only after a host-side pre-pull**:

```bash
docker pull --platform linux/amd64 osodevops/kafka-backup@sha256:8ff5be…
```

Kubelet-side pulling fails with `no matching manifest`. Two consequences:

- **Use `imagePullPolicy: Never`.** A missing pre-pull then fails legibly as
  `ErrImageNeverPull` rather than as an opaque pull error. This is the honest
  trade: the pod cannot fetch what it needs, so make it say so.
- **Never set `nodeSelector: kubernetes.io/arch: amd64`** on a single-arm64-node
  cluster. The pod stays `Pending` **forever**, with no event that names the
  real problem.

**Verified, and it will bite you later:** `docker image prune` evicts the
pre-pulled image, and drills silently start failing with **no code change and no
manifest change**. If drills stop working after a routine Docker cleanup, this
is why.

## 3. Registry, permissions and mounts

All **verified** on `docker-desktop`:

- **No registry is needed.** Docker Desktop's kubelet shares the local Docker
  image store, so a locally built `logweir:v0.1.0` is directly runnable.
- **The default ServiceAccount has zero access.** A drill pod needs its own
  ServiceAccount, Role and RoleBinding. `automountServiceAccountToken: false` is
  appropriate for v0.1, which makes no Kubernetes API calls at all.
- **The signing key needs `fsGroup`. Without it a non-root pod cannot read its
  own key, and this page used to claim the Secret "mounts cleanly".** It does
  not: kubelet writes Secret files owned by `root:root`, so a container running
  as `runAsUser: 65532` — which the image sets and the manifest repeats — reads
  nothing through the owner bits. `examples/cronjob-drill.yaml` shipped
  `runAsUser: 65532`, `defaultMode: 0400` and **no `fsGroup` anywhere**, so
  every scheduled drill would have died at phase 1 opening
  `/etc/logweir-keys/signing.pem`.

  **All four combinations run live on `docker-desktop`, 2026-09-05**, in one
  pod shape with the manifest's own uid/gid, `busybox` doing `ls -lL` then
  `cat`:

  | `defaultMode` | `fsGroup` | file as mounted | `cat` |
  |---|---|---|---|
  | `0400` | none | `-r-------- root root` | **Permission denied** |
  | `0440` | none | `-r--r----- root root` | **Permission denied** |
  | `0400` | `65532` | `-r--r----- root 65532` | reads |
  | `0440` | `65532` | `-r--r----- root 65532` | reads |

  **`fsGroup` is the load-bearing half, and it is sufficient on its own.**
  Two things happen when it is set: kubelet chowns the volume's group to that
  GID, *and* it ORs group-read into the file mode — which is why row 3 lands on
  disk as `0440` even though the manifest asked for `0400`. Widening the mode
  without `fsGroup` (row 2) fixes nothing, because the group is still root's.

  `examples/cronjob-drill.yaml` now sets `fsGroup: 65532` and writes
  `defaultMode: 0440`, so the manifest states the permission that actually
  lands rather than one kubelet silently widens.
- **The ConfigMap was never the failing half, and that was checked, not
  assumed.** The drill spec, the approval and the allowed-clusters list mount as
  a ConfigMap; the manifest sets no `defaultMode` there, so kubelet's `0644`
  applies. Verified live in the same pod shape, with **no `fsGroup`**:
  `-rw-r--r-- root root drill.yaml`, `cat` exit 0. The files are `root:root` for
  the same reason the Secret's are — the group bits simply do not matter when
  the world bits are set. That is also why the key is a Secret and not a
  ConfigMap.
- **`emptyDir` is verified** for the engine's scratch directory. **No PVC was
  exercised.** The only StorageClass on the test cluster was `hostpath`.
- **The metrics file must NOT be an `emptyDir`** — see §5.

## 4. What the pod still needs from you

The container image carries the engine; the environment does not carry itself:

| Variable | Why |
|---|---|
| `LOGWEIR_ENGINE_BIN` | Set by the image to `/usr/local/bin/kafka-backup`. Override only if you mount an engine elsewhere. |
| `LOGWEIR_ENGINE_VERSION`, `LOGWEIR_ENGINE_DIGEST` | **Mandatory.** An empty value is refused with exit 1: a signed scorecard must name the engine image that produced the restore. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`, or an IRSA / Pod Identity setup | `object_store`'s own credential chain — **not** the AWS SDK's. `~/.aws/credentials`, `AWS_PROFILE` and SSO are unsupported. See [stability.md](stability.md). |
| `TMPDIR` | Where the rendered `restore.yaml` and the restore checkpoint land. Point it at a writable volume; the checkpoint is pod-local and is never uploaded, so a crashed restore is not resumable in v0.1. |

## 5. The metrics file is the only metrics surface, so where you mount it decides whether you get any

v0.1 has **no HTTP surface** — no `/metrics`, no `/healthz`, no `/readyz`. The
Prometheus **textfile** written at `--metrics-file` is not one of two routes; it
is the route. So the volume behind it is load-bearing.

`examples/cronjob-drill.yaml` mounted it on an **`emptyDir`**, directly beneath
its own comment telling the reader to mount node_exporter's textfile directory.
An emptyDir is deleted with the pod. Every drill therefore wrote its metrics
into a filesystem nothing would ever scrape and then destroyed it, and
[../dashboards/logweir.json](../dashboards/logweir.json) stayed empty forever —
which looks exactly like Logweir never running.

**Verified live on `docker-desktop`, 2026-09-05.** One pod shape at
`runAsUser: 65532` wrote `logweir.prom`; the pod was then **deleted**; a second
pod mounted the same volume and read it back:

```
hostPath /var/lib/node_exporter/textfile   ->  logweir_drill_last_run_timestamp_seconds{cluster="unknown"} 1757000000
emptyDir                                   ->  total 0
                                               cat: can't open '/metrics/logweir.prom'
```

The sample above is emitted by `crates/logweir/src/metrics.rs` — `write_textfile`
on a drill result, `write_minimal_textfile` on exits 1, 3 and 4. The `cluster`
label is always present: it is the scorecard's `target.cluster_id`, or the
literal `unknown` on a terminal path that never learned it (see
[metrics.md](metrics.md)). The value is that run's own `Utc::now().timestamp()`
at emit time, never a constant.

One series in that file is about the cluster this CronJob runs against rather
than about the drill: `logweir_drill_teardown_topics_failed{cluster}` counts the
scratch topics phase 9 created on the target and could not delete, because the
broker refused. It is emitted on every scorecard-carrying path, `0` included, so
`0` means phase 9 ran and cleaned up while *absence of the series inside a
present file* means the run ended before a scorecard existed. A non-zero value
leaves real topics on a real cluster and does not change the pod's exit code —
phase 8 has already signed and uploaded by the time phase 9 runs — so this is
the one series worth an alert even though the drill "passed". The full metric
reference, including which names the failed topics are reported under, is
[metrics.md](metrics.md).

Three facts decide the shape, each of them run:

- **`type: Directory`, not `DirectoryOrCreate`.** `DirectoryOrCreate` makes
  kubelet create the path and it lands `drwxr-xr-x root root`, which uid 65532
  cannot write: `can't create /metrics/logweir.prom: Permission denied`, at
  phase 8, after the whole drill has already run. `Directory` fails at **mount**
  time instead, before the container starts, with an event that names the path:

  ```
  Warning  FailedMount  MountVolume.SetUp failed for volume "metrics":
                        hostPath type check failed: /var/lib/... is not a directory
  ```

  Loud beats silent.
- **`fsGroup` does not apply to `hostPath`.** Ownership is host-side setup, not
  manifest setup. Verified: the directory at `root:root 0755` refuses the write;
  at `0775` with group 65532 it succeeds.

  ```bash
  sudo install -d -m 0775 -g 65532 /var/lib/node_exporter/textfile
  ```

- **A `hostPath` volume is forbidden by Pod Security `baseline` AND
  `restricted`.** Verified by labelling the namespace and re-applying:

  ```
  Error from server (Forbidden): violates PodSecurity "baseline:latest":
    hostPath volumes (volume "metrics")
  ```

  Everything else in the worked manifest satisfies `restricted`; the metrics
  volume is the one thing that does not. In a namespace that enforces either
  profile, **delete both the `--metrics-file` argument and the `metrics`
  volume** rather than pointing the flag at an emptyDir. No metrics is an honest
  state; a dashboard fed by a deleted file is not.

  That is what the **`restricted` manifest variant** does: it ships without the
  `hostPath` volume, without its mount and without `--metrics-file`. It
  therefore emits **no textfile on any exit path** — not on a pass, not on a
  drill result, and not on the exits 1, 3 and 4 that now write a minimal record
  everywhere else. Nothing below rescues it: there is no PVC, no sidecar, no
  push route and no HTTP surface in v0.1, so a `restricted` namespace gets its
  drill results from the signed scorecard and the pod's exit code alone. This
  gap is a known regression, recorded rather than fixed, and it is written up
  with the hardened manifests; do not close it by adding a volume back.

### Is the drill still running at all?

Every terminal path now writes the textfile, so the file's *existence* says
nothing. Its **mtime** does:

```promql
time() - node_textfile_mtime_seconds{file=~".*logweir.*"} > 8d
```

This is mtime-based and not `time() - logweir_drill_last_run_timestamp_seconds{cluster=~".+"}`
on purpose. A metric Logweir writes cannot report that Logweir did not run, and
node_exporter's `node_textfile_mtime_seconds` carries **no Logweir label at
all** — so it still matches on the terminal paths where the cluster id was never
learned and every Logweir series is labelled `cluster="unknown"`. `8d` is one
day of slack over the weekly schedule in `examples/cronjob-drill.yaml`; the
dashboard's "Time since the last drill reported" panel thresholds at the same
`691200` seconds. Full metric reference: [metrics.md](metrics.md).

**Unverified:** a shared PVC read by a node_exporter sidecar, and any push-based
route (Pushgateway, OTLP). Neither was tested, and v0.1 emits nothing but the
textfile.

## 6. What is NOT here in v0.1

Stated so nobody goes looking:

- **No operator, no CRDs — in v0.1.** `weirkeeper` and `logweir.dev/v1alpha1`
  are not in the v0.1 tag; §7 below is the kind list they ship as, and
  `RestoreDrill` is not among them (Global Constraint 34 retires it for
  `Restore`, and `MetadataSnapshot` stays reserved and unbuilt).
- **No Kubernetes Job execution of the engine.** v0.1 spawns a local
  subprocess inside the drill pod; it does not create a Job of its own.
- **No namespace-label segregation proof.** v0.1 proves the target is a scratch
  cluster with a **marker topic**, because a guard that needs a kubeconfig
  cannot run in the unit suite.
- **No `SubjectAccessReview`-verified approval.** Approval is a DSSE-signed
  document checked against the approver's public key.
- **No HTTP surface.** No `/metrics`, `/healthz` or `/readyz`. Metrics are a
  Prometheus **textfile** written at `--metrics-file` and scraped through
  node_exporter's textfile collector — see §5 for where that file has to live,
  and [../dashboards/logweir.json](../dashboards/logweir.json) for the
  dashboard it feeds.

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
| `BackupSchedule` | Namespaced | A recurring backup of a **named** topic set (no wildcard, no glob metacharacter). `spec.suspend` is the only mutable field. `retention{keepLast, keepDays}` **reports** what it would remove and deletes nothing. |
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

## 8. The approval flow: five checks, in this order

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

### The five checks

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
| 3 | Each entry's declared `keyId` is the sha256 of its own `spkiPem` | `KeyIdNotInRoster`, naming both ids |
| 4 | Some `sidecarBytes.signatures[].keyid` is on `approverKeys` | `KeyIdNotInRoster`, naming the sidecar's key ids |
| 5 | The signature verifies, under the **matched** key | `SignatureInvalid` |
| 6 | The matched entry's `notAfter` is in the future | `KeyIdExpired` |
| 7 | The document's `plan_hash` equals the sha256 of the referent's `spec.planBytes`, **recomputed** | `PlanHashMismatch` |
| 8 | The document's `subject_kind` equals the referent's kind | `SubjectKindMismatch` |

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

Checks 1–6 are the "five checks" the roster and the signature answer; 7 and 8
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

### A slot older than one hour is skipped, and the skip is recorded

The controller has no timer and no leader lease: it re-examines every schedule
every 30 seconds and works out which slot is due. A slot that came due more
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

### The plan ConfigMap: the reconciler renders it, and it renders it first

The Job mounts a ConfigMap named `<backup name>-plan` at `/plan`, and the runner
argv points `--spec` at `/plan/backup.yaml` and `--allowed-clusters` at
`/plan/allowed-clusters.json`. **The `Backup` reconciler renders that ConfigMap
in the same pass that creates the Job, and the ConfigMap `POST` comes first.**
The order is the whole point: a Job created first is a pod stuck in
`ContainerCreating` on `MountVolume.SetUp failed for volume "plan": configmap
"<name>-plan" not found` until `activeDeadlineSeconds` fires, after which the
job controller deletes the pod and the exit code goes with it — a terminal
`NoExitCode` that explains nothing.

It has **exactly two keys**, owner-referenced to the `Backup` with
`controller: true` and `blockOwnerDeletion: true`, so deleting the `Backup`
collects the plan and a half-deleted `Backup` cannot orphan one:

- **`backup.yaml`** — the typed `BackupSpec` document `logweir backup run
  --spec` parses. `source.bootstrapServers` and `source.auth` come from the
  `KafkaCluster` that `spec.sourceRef` names, never from `Backup.spec`, which
  carries neither; `source.topics` is `spec.topics` **verbatim**; `storage` is
  `spec.archive.url` through the same parser the controller's own read-only
  archive handle is built with. The document is built as the Rust type and
  serialised, not assembled as text: `storage` is an internally tagged enum
  whose variants have incompatible required fields, and a stringly-typed
  renderer emits `backend: filesystem` beside a `bucket:` key, which fails the
  engine's config load with a hard missing-field error.
- **`allowed-clusters.json`** — the cluster allowlist, in the format the CLI's
  own reader parses.

Two refusals live on this path, both **terminal and never a requeue**, because
`Backup.spec` is CEL-immutable and the next pass would read the same spec:
`spec.sourceRef` naming a `KafkaCluster` that does not exist is
`ReferentNotFound`, and a topic carrying a glob metacharacter (`*`, `?`, `[`,
`]`, `{`, `}`) is `GuardRefused` **before anything is created** — topics are a
mandatory named allowlist, and the rail is the same one the runner uses.

A **409** on the ConfigMap `POST` is success only when the existing object
carries a controller owner reference with **this** `Backup`'s UID. That is the
ordinary case — a previous pass of this same reconcile, whose bytes are
identical because the plan is a pure function of an immutable spec. A 409 on
somebody else's object is `PlanConfigMapConflict`: writing the Job then would
mount a plan document a stranger wrote, at the mount path of the pod that holds
the signing key.

**Why the allowlist is a ConfigMap key here when the drill path keeps its own in
a Secret.** On the drill path `allowedClusterIds` authorises a restore
*target*, so a subject with `patch configmaps` could widen it, and it lives in
the `logweir-approval-bundle` Secret. On the backup path the direction is
reversed: **the allowlist is a consistency rail, not a boundary.** The address
the run dials comes from the CEL-immutable `sourceRef` and never from this file,
and the backup guard *refuses* a run whose broker-observed source cluster id
appears in `allowedClusterIds` — a cluster cannot be both the source of an
archive and a scratch cluster whose topics a drill deletes. So the rendered file
carries an **empty** `allowedClusterIds` and the observed cluster id in
`sourceClusterId`, which the backup path does not read; every id an attacker
could add makes the backup **refuse**, and none widens it.

Neither key carries a credential. The auth block has a username and no password
at any variant, and the object-store credential reaches the runner as
`secretKeyRef` environment — a ConfigMap has no encryption at rest and a much
wider read surface than a Secret, and nothing in it is secret.

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

Two RBAC notes, because both are easy to get wrong:

- **`pods/log` is a subresource and `pods` does not cover it.** A role granting
  `get` on `pods` reads every pod's spec and cannot read one line of any pod's
  stdout — and the failure is a 403 that looks like a transient API error.
- The controller requests **no verb on `secrets`, and no `pods/exec` or
  `pods/attach`**. The runner's key reaches its pod because kubelet projects it;
  the controller never reads it and cannot start a process inside the pod that
  holds it. `config/rbac/` carries the request; the install file carries the
  grant.

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

### Verified live, 2026-09-10, `docker-desktop` v1.34.1

A controller-built Job (the real output of `job::build`, applied into
`logweir-t17`) whose runner was handed a plan the phase-−1 admission guard
refuses. What the cluster actually showed:

```
$ kubectl --context docker-desktop -n logweir-t17 get pods -l batch.kubernetes.io/job-name=logweir-backup-t17-live
NAME                            READY   STATUS   RESTARTS   AGE
logweir-backup-t17-live-wf42j   0/1     Error    0          8s          # ONE pod; "Error", no code

$ kubectl --context docker-desktop -n logweir-t17 get pod logweir-backup-t17-live-wf42j \
    -o jsonpath='{range .status.containerStatuses[*]}{.name}={.state.terminated.exitCode}{"\n"}{end}'
runner=3                                                                # the code, at the one path

$ kubectl --context docker-desktop -n logweir-t17 get job logweir-backup-t17-live \
    -o jsonpath='{.status.conditions[*].type}={.status.conditions[*].reason}'
FailureTarget Failed=PodFailurePolicy PodFailurePolicy                  # not BackoffLimitExceeded
```

Four other things were confirmed on the same pod: `spec.ttlSecondsAfterFinished`
was **empty** at creation and took the 604800 patch afterwards; the pod's
`securityContext` landed as `{fsGroup: 65532, runAsUser: 65532, runAsGroup:
65532, runAsNonRoot: true, seccompProfile: RuntimeDefault}`; its volume list was
exactly `signing`, `plan`, `work` with **no `kube-api-access-*` projection**, so
`automountServiceAccountToken: false` really does keep a token out of the
key-holding pod; and **both** job-name labels were present —
`batch.kubernetes.io/job-name` and the legacy `job-name` — each selector
returning the one pod, which is what the reconciler's prefixed-then-legacy
fallback is written against.

### One surprise worth knowing: `refusal-reason=` is the last line of *stdout*, not of the pod log

`logweir backup run` prints `refusal-reason=<TerminalState>` as its **final
stdout line** for exit 3 — that is the CLI's contract and it holds. But a pod
log is **stdout and stderr merged in nondeterministic order**, and `backup run`
also writes the guard's full explanation to stderr. Measured **twice on
`docker-desktop` v1.34.1, from the same refusal**, `kubectl logs` returned the
discriminator in two different positions — second-to-last in one run:

```
refusal-reason=GuardRefused
guard: plan refused by the admission guard: source topic `orders*` contains a glob metacharacter …
```

and last in the other:

```
guard: plan refused by the admission guard: source topic `orders*` contains a glob metacharacter …
INFO backup finished
refusal-reason=GuardRefused
```

So the position is not a rule, in either direction: **no controller-side reader
may take "the last line of the pod log", and none may take the second-to-last
either.** This is the practical consequence of the fact §1 already states — the
pod log API has no stream selector — and it is why every reader in this
controller scans a **bounded tail** and matches **by key name**
(`controllers::backup::{evidence_keys, refusal_state}`, with
`KEY_SCAN_TAIL_LINES = 8`). A reader written against a POSITION would have
reported no terminal state at all on whichever of the two runs did not match
it.

### What a green `Backup` requires

`status.evidence.verification.result == Valid` **and** `status.exitCode == 0`.
Both, and nothing else. A `Backup` carries **no `outcome`** — that field is
`Restore`'s — so there is no second source of truth for the badge to disagree
with. Verification itself is not this reconciler's work; it records the two
keys and leaves `evidence.verification` alone.

## 11. Task 7 — the five-listener compose stack, and the one string CI overrides

`e2e/compose/docker-compose.yml` has exactly one editor (STANDING RULE 15), and
from Task 7 onward every task that brings the stack up gets a broker with
**five listeners** plus a `scram-setup` step that must have exited 0 before any
SASL client authenticates.

| listener | `listeners` | `advertised.listeners` | protocol | published | who reaches it |
|---|---|---|---|---|---|
| `PLAINTEXT` | `kafka-broker-1:9094` | `kafka-broker-1:9094` | PLAINTEXT | no | inter-broker; every in-network setup step |
| `EXTERNAL` | `kafka-broker-1:9092` | `localhost:9092` | PLAINTEXT | `9092:9092` | the host-side e2e harness |
| `CONTROLLER` | `kafka-broker-1:9093` | — | PLAINTEXT | no | the KRaft quorum |
| `SASL` | `kafka-broker-1:9096` | `kafka-broker-1:9096` | SASL_PLAINTEXT | no | in-network SCRAM |
| `SASLEXT` | `kafka-broker-1:9097` | `localhost:9097` | SASL_PLAINTEXT | `9097:9097` | host-side SCRAM |
| `K8S` | `kafka-broker-1:9095` | `${LOGWEIR_K8S_ADVERTISED_HOST:-host.docker.internal}:9095` | PLAINTEXT | `9095:9095` | **a pod** |

`SASL` and `SASLEXT` are **one credential store advertised twice**, not two
security configurations. A broker's metadata redirects a client to the
*advertised* name whatever address it bootstrapped against, so a single
`SASL://kafka-broker-1:9096` is unreachable from the host and a single
`SASLEXT://localhost:9097` is the container's own loopback. Both, or neither
client works.

### `host.docker.internal:9095` is the pod bootstrap, and it is a Docker Desktop behaviour

`KafkaCluster.spec.bootstrapServers` in Demo 1 is exactly
`host.docker.internal:9095`. `EXTERNAL://localhost:9092` cannot serve a pod at
all — the broker hands back `localhost:9092`, which inside a pod is the pod's
own loopback — so the K8S listener exists for the Kubernetes path and nothing
else.

**A published port is not the same claim as a resolvable name inside a pod.**
`host.docker.internal` resolution inside pods comes from Docker Desktop, not
from Kubernetes, so it is verified with a real pod:

```
kubectl --context docker-desktop create ns logweir-t7
kubectl --context docker-desktop -n logweir-t7 run probe --rm -i --restart=Never \
  --image=busybox -- sh -c 'nc -z host.docker.internal 9095'; echo "rc=$?"
kubectl --context docker-desktop delete ns logweir-t7 --ignore-not-found
```

Measured on `docker-desktop` v1.34.1, 2026-09-10: **rc=0**. The same probe
against the unpublished 9096 returns **rc=1**, which is what makes the 0 mean
something. If the NAME does not resolve on some host, the fallback
(`hostNetwork`, or the host's LAN address written into
`KafkaCluster.spec.bootstrapServers`) is a controller decision, not an
implementer's.

### `LOGWEIR_K8S_ADVERTISED_HOST` is the one string a CI runner overrides

`host.docker.internal` does not resolve inside a `kind` node on a Linux GitHub
runner. The advertised K8S name is therefore a compose **parameter** with
`host.docker.internal` as its default, which every local gate uses:

```
docker compose -f e2e/compose/docker-compose.yml config -q; echo "rc=$?"
# rc=0 → K8S://host.docker.internal:9095

LOGWEIR_K8S_ADVERTISED_HOST=172.18.0.1 \
  docker compose -f e2e/compose/docker-compose.yml config -q; echo "rc=$?"
# rc=0 → K8S://172.18.0.1:9095
```

Set it to the address the cluster's nodes can reach (on `kind`, the docker
bridge gateway) and change nothing else. Without a parameter here, a later task
would have to edit the compose file and break STANDING RULE 15 to get one.

### `scram-setup` runs third, and its exit code is load-bearing

In KRaft there is no ZooKeeper to pre-seed and no `--zookeeper` path: a SCRAM
credential is a user config record in the metadata log, so it has to be written
by a **client**, after the quorum is serving, **over the PLAINTEXT listener** —
the credential being created is the credential a SASL client would need, so
bootstrapping it over a SASL listener is circular and fails. `just e2e-up` runs
it as a third foreground step and `just` checks its status:

```
docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm scram-setup
# /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-broker-1:9094 --alter \
#   --add-config 'SCRAM-SHA-512=[password=…]' --entity-type users --entity-name logweir
# → Completed updating config for user logweir.
```

`--alter --add-config` is idempotent, so `just e2e-up` against a stack that is
already up still exits 0.

### The listener-scoped JAAS variable: the spelling, measured

Spec §15's sixth `[UNVERIFIED]` mark was the exact spelling of the
listener-scoped JAAS environment variable under the `apache/kafka` entrypoint.
**It is closed by execution.** `kafka.docker.KafkaDockerWrapper` strips the
`KAFKA_` prefix, lowercases, and then maps `___` → `-`, `__` → `_`, `_` → `.`,
so the two variables

```yaml
KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG:    'org.apache.kafka.common.security.scram.ScramLoginModule required;'
KAFKA_LISTENER_NAME_SASLEXT_SCRAM___SHA___512_SASL_JAAS_CONFIG: 'org.apache.kafka.common.security.scram.ScramLoginModule required;'
```

produce these properties. Read back off the RUNNING broker with
`docker compose -f e2e/compose/docker-compose.yml exec kafka-broker-1 cat /opt/kafka/config/server.properties`
on 2026-09-10, `apache/kafka:3.7.1`:

```
listener.name.saslext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
```

and as the exit code the e2e row reads directly:

```
docker compose -f e2e/compose/docker-compose.yml exec kafka-broker-1 \
  grep -q 'listener.name.sasl.scram-sha-512.sasl.jaas.config' /opt/kafka/config/server.properties; echo "rc=$?"
# rc=0   (and rc=0 for listener.name.saslext.…)
```

The alternative form the procedure allowed for — a single
`KAFKA_SASL_JAAS_CONFIG` plus `KAFKA_LISTENER_NAME_SASL_SASL_ENABLED_MECHANISMS`
— was **not needed** and is not shipped.

**Why this is asserted where it lands, and not by "the broker started".** A
misspelled `KAFKA_…` variable is not an error under that entrypoint: the
wrapper translates whatever it is given and the broker starts happily without
the property. The failure then surfaces as an authentication error in some
other test — the same silently-ignored-key shape as the rendered documents'
`security:` block. So the property is checked in `server.properties`.

### Both SCRAM clients, because they do not agree on the spelling

`e2e/tests/scram.rs` exercises the two independent implementations:

* **Logweir's** client is librdkafka — `sasl.mechanism=SCRAM-SHA-512`, two
  hyphens, configured through `AuthConfig::from_spec`;
* **the engine's** is a from-scratch RFC 5802 client —
  `sasl_mechanism: "SCRAM-SHA512"`, ONE hyphen, configured by the four keys
  under `security:`.

One enabled broker mechanism (`sasl.enabled.mechanisms=SCRAM-SHA-512`) serves
both. A wrong password fails as an **authentication** failure and never as
`No available brokers`, which is what a silent PLAINTEXT downgrade against a
SASL-only listener looks like:

```
librdkafka: Global error: Authentication (Local: Authentication failure):
sasl_plaintext://localhost:9097/bootstrap: SASL authentication error: Authentication
failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512
```

The compose SCRAM password is a **fixture constant**, not key material: it
authenticates to one throwaway broker on one developer machine. It appears in
the compose file, the harness and the e2e config on purpose. No signing key
appears anywhere in this repository.

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

**An unapproved plan creates nothing at all** — no ConfigMap, no Job, zero
`POST`s. So **exit 3 from a `Restore`'s pod now means only "a phase-0 admission
guard refused"**, and the exit-code table of §10 reads the same way for both
kinds.

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
| `approval` | Secret `logweir-approval-bundle` | `/approval` | `approval.json`, `approval.sig`, `approver.pub.pem`, `allowed-clusters.json` |
| `signing` | Secret `logweir-signing-key`, `0440` | `/signing` | the runner's own signing key, readable only because `fsGroup: 65532` is set |
| `plan` | ConfigMap `<name>-plan` | `/plan` | `spec.planBytes`, verbatim |
| `work` | `emptyDir` | `/work` | the scorecard, the offset report and the checkpoint state, on a pod whose root filesystem is read-only |

**`allowed-clusters.json` is in a SECRET here and a ConfigMap on the backup
path, and the direction is why.** On this path the file authorises a restore
*target*: a subject with `patch configmaps` who replaced it would WIDEN the set
of clusters a restore may write into. On the backup path the same file can only
make a run refuse (§10), so it stays a ConfigMap key there.

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
sidecar-key=logweir/drills/<run_id>.json.sig
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

## 13. Installing: one file, and the five Secrets you must create first

**Minimum Kubernetes: 1.29** (CEL validation rules are GA there, and the six
CRDs use them). The `ValidatingAdmissionPolicy` example in
[../config/samples/validatingadmissionpolicy.yaml](../config/samples/validatingadmissionpolicy.yaml)
is 1.30+ and ships **commented out** — see the end of this section.

The whole install is:

```bash
kubectl --context docker-desktop apply --server-side -f logweir.yaml
```

[../logweir.yaml](../logweir.yaml) is the checked-in `kubectl kustomize` output
of [../config](../config), regenerated by `just install-yaml` and never edited by
hand. It contains the Namespace, the six CustomResourceDefinitions, the RBAC and
the controller Deployment, and **no custom resource** — so the
CRD-not-yet-established ordering failure cannot happen. Samples are separate
files, applied second.

**It does not start a pod, and that is recorded rather than hidden.** The
Deployment references `ghcr.io/logweir/weirkeeper:v0.1.0` by tag, no such image
has been pushed, and until a git remote exists and `release.yml` has run against
it the install file's digest rows read `blocked: no remote`. The transcript at
the end of this section shows the pod in `ImagePullBackOff`, which is the
expected result. For an author-only local run:

```bash
just image && just image-weirkeeper
kubectl --context docker-desktop apply --server-side -k config/overlays/local-images
```

A locally built or locally loaded image is **author-only** and never satisfies
spec §16 clause 1.

### Step 1, before any custom resource

Everything in this step is a cluster-admin or namespace-admin action, and none of
it is done by the controller or by the UI.

#### 1a. The two keypairs

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem && openssl pkey -in signing.pem -pubout -out signing.pub.pem
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out approver.pem && openssl pkey -in approver.pem -pubout -out approver.pub.pem
```

P-256, because that is what `logweir-evidence` mints and verifies. Keep both
private halves off the cluster except as the one Secret named below; the
approver's private key never goes on the cluster at all.

> **WARNING — an absent signing key is MINTED, silently.**
> `SigningKey::load_or_generate` returns a **new** key when the path is absent
> (`crates/logweir-evidence/src/keys.rs:81-92` — `if path.exists()` at `:82`,
> then `let key = Self::generate_p256();` at `:85`). So a first run against an
> empty or mis-keyed `logweir-signing-key` Secret does not fail. It **succeeds**,
> and it signs its scorecard and its receipt with a key that is in no
> `TrustRoster`, that nothing attests, and that disappears with the pod. The
> evidence looks green and verifies against nothing. This is why
> `just check-secrets <namespace>` exists and why it checks
> `logweir-signing-key` first.

#### 1b. The cluster-scoped `TrustRoster`, whose name is fixed

```bash
kubectl --context docker-desktop apply -f config/samples/trustroster.yaml
```

**The name is `default` and nothing reads any other name.**
`weirkeeper::controllers::approval::ROSTER_NAME` is the literal `"default"` and
`load_roster` does `Api::<TrustRoster>::all(client).get(ROSTER_NAME)`. A roster
called anything else is stored, reconciled and listed — and consulted by no
approval check, so every `Approval` reports `RosterNotFound`.

`TrustRoster` is **cluster-scoped**, so this is a cluster-admin step. The UI
surfaces this snippet and does **not** submit it: a namespace-scoped adopter
cannot complete the approval flow without somebody with cluster-level write
running this once. Fill in `spkiPem` from `approver.pub.pem` and
`signing.pub.pem`, and each `keyId` from `shasum -a 256 <that file>`; a declared
id that does not match its own key is `KeyIdNotInRoster`, naming both ids.

#### 1c. The five Secrets

There are **five**, not three. Four live in the namespace your `Backup`,
`Restore` and `KafkaCluster` objects live in; the fifth is the controller's and
lives in `logweir-system`.

**1. `logweir-signing-key`** — the runner signs evidence with it. The data key is
`signing.pem`, **not** `key.pem`: `SIGNING_KEY_SECRET_KEY` is `"signing.pem"` and
the Job projects it to the *file* `key.pem` under `/signing`. A Secret keyed
`key.pem` mounts a directory without the file the runner was told to read.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-signing-key --from-file=signing.pem=signing.pem
```

**2. `logweir-approval-bundle`** — the signed approval, its detached sidecar, the
approver's **public** key, and the cluster allowlist. No private key reaches a
runner pod on this path.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-approval-bundle \
  --from-file=approval.json=approval.json \
  --from-file=approval.sig=approval.sig \
  --from-file=approver.pub.pem=approver.pub.pem \
  --from-file=allowed-clusters.json=allowed-clusters.json
```

**3. The per-cluster SCRAM credential** — its *name* is yours, whatever
`KafkaCluster.spec.auth.secretRef` says; its *data key* is fixed at `password`
(`TARGET_PASSWORD_SECRET_KEY`). The username is `spec.auth.username`, in the
object, not in the Secret.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  kafka-scram --from-literal=password="$KAFKA_PASSWORD"
```

**4. `logweir-s3`** — the archive credential the runner already needs
(`examples/cronjob-drill.yaml:126-131`). `object_store`'s own credential chain,
not the AWS SDK's: `~/.aws`, `AWS_PROFILE` and SSO are unsupported.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-s3 \
  --from-literal=access-key-id="$AWS_ACCESS_KEY_ID" \
  --from-literal=secret-access-key="$AWS_SECRET_ACCESS_KEY"
```

**5. `logweir-evidence-ro`** — the controller's **read-only** evidence-bucket
credential, and the only one of the five the controller's own pod consumes. A
**different principal** from the runner's archive credential, and read-only:
GC6 leaves no Logweir component any object-store delete capability. Created in
`logweir-system`.

```bash
kubectl --context docker-desktop -n logweir-system create secret generic \
  logweir-evidence-ro \
  --from-literal=access-key-id="$EVIDENCE_RO_ACCESS_KEY_ID" \
  --from-literal=secret-access-key="$EVIDENCE_RO_SECRET_ACCESS_KEY"
```

Then check, before creating anything:

```bash
just check-secrets <namespace>; echo "rc=$?"
```

It exits **1** naming the first absent Secret. It is deliberately **not** part of
`just apply-install`: X-APPLY is defined against a clean cluster with no Secrets
at all, and folding the check into the install would make the install refuse in
exactly the state it is specified to succeed in.

#### 1d. The runner ServiceAccount, in each namespace that runs jobs

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f config/rbac/backup-runner-serviceaccount.yaml
```

Runner Jobs run in the namespace of the `Backup`, `Restore` or `KafkaCluster`
object that produced them, and every one of them names the ServiceAccount
`logweir-runner` (`controllers::backup::RUNNER_SERVICE_ACCOUNT`). That account is
**not** in `logweir.yaml`, because `logweir.yaml` installs into `logweir-system`
and no runner ever runs there — creating it in that one namespace would be
creating it in the only namespace where it is useless. It is granted **no verb on
anything**, it sets `automountServiceAccountToken: false`, and so does every
PodSpec `job::build` renders: `logweir backup run` makes zero Kubernetes API
calls, and it is the process that holds the signing key.

A pod whose PodSpec names no ServiceAccount silently gets `default` — the one
account an operator is most likely to have granted something to. That is why the
name is set explicitly and why this step is not optional.

#### 1e. Binding the three human roles

`logweir.yaml` ships `logweir-viewer`, `logweir-operator` and `logweir-approver`
**unbound**. Who may approve a restore in which namespace is your decision, not
the install file's.

```bash
kubectl --context docker-desktop create rolebinding logweir-viewer \
  --clusterrole=logweir-viewer --user=<user> -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-operator \
  --clusterrole=logweir-operator --user=<user> -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-approver \
  --clusterrole=logweir-approver --user=<someone-else> -n <namespace>
```

`logweir-approver` is `create` on `approvals` and **nothing else**. Bind it to
somebody who is not the operator: `self_attested: false` means only "two
different keys", and one person holding both keypairs satisfies it.

### `update` on `backupschedules` is plain, and the `suspend` restriction is CEL

`logweir-operator` grants a **plain `update`** on `backupschedules`. It cannot
grant a restricted one: **an RBAC `rules[]` entry is
`apiGroups`/`resources`/`verbs`/`resourceNames` and nothing else — there is no
field in which a CEL expression could be written**, so "CEL-restricted `update`
of `suspend`" is not expressible in a ClusterRole and must not be attempted.

The restriction exists one layer up, in the CRD.
`config/crd/backupschedules.yaml` carries an object-level
`x-kubernetes-validations` rule over `.spec` that seals `schedule`, `sourceRef`,
`topics`, `archive` and `retention` — every field except `suspend` — with the
message `only spec.suspend is mutable; create a new BackupSchedule instead`. The
API server applies it to **every** subject, cluster-admin included, which is a
stronger statement than any ClusterRole could make about any one of them.

### What the controller can read, and what it cannot

The `weirkeeper` ClusterRole is written by one rule: **every granted verb has a
caller**. It carries `get/list/watch` on the six kinds; `create` on `backups`
(that is how a `BackupSchedule` produces a run); `patch` on the six `/status`
subresources and nothing else of the object; `create/get/list/watch/patch` on
`batch/v1` Jobs; `get/list/watch` on Pods; **one rule naming `pods/log`, with
`get`**; and `create/get` on ConfigMaps.

It has **no verb on `secrets`, anywhere**. The runner's signing key, approval
bundle and SCRAM credential reach its pod because the **kubelet** projects them
from references the controller writes into a PodSpec — writing a reference is not
reading a value. **This bounds reads and not capability:** Job create in a
namespace holding the signing key is equivalent to holding the key, because the
controller can create a pod that mounts it. That is O1/O0 default (a), accepted,
and it is said here rather than left to be inferred.

It has **no `delete` on anything**. A finished Job is removed by the API server's
TTL controller after the controller patches `ttlSecondsAfterFinished` — after the
status write, because the TTL controller deletes the Job *and its pods*, and the
exit code lives only on the pod. The plan ConfigMap and the probe Job are removed
by ownerReference garbage collection.

**`pods/log` is a subresource and `get` on `pods` does not grant it.** Without an
explicit `resources: ["pods/log"]` rule the API server answers 403 for every
`GET /api/v1/namespaces/<ns>/pods/<p>/log`, and the visible symptom is not a
crash: it is every `status.evidence.*` key staying empty, forever, behind an
error that looks transient. No rule anywhere names `pods/exec` or `pods/attach`.

### The NetworkPolicy

`logweir.yaml` ships `logweir-runner-egress`: default-deny egress on Job pods,
with explicit allowances for DNS to `kube-system`, the five broker listener ports
and the object store's 443/9000.

`[UNVERIFIED — docker-desktop runs no CNI that enforces NetworkPolicy, so a deny
is never observed here; only the kind+Calico probe would make this claim real,
and it is backlogged]` The same mark is in
[../config/manager/networkpolicy.yaml](../config/manager/networkpolicy.yaml),
with the sentence that would verify it: create a `kind` cluster with Calico,
install the policy, and show a runner pod's connection to a disallowed address
timing out while the broker connection succeeds. Neither half has been run.

Two honest caveats. A NetworkPolicy is **namespaced**, and `logweir.yaml`
installs this one into `logweir-system`, where no runner ever runs — apply it
into each runner namespace too:

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f config/manager/networkpolicy.yaml
```

And the selector is `batch.kubernetes.io/job-name: Exists`, because
`job::build` gives runner pods no label of their own, so the policy also covers
any other batch work in that namespace.

### The `ValidatingAdmissionPolicy` example

[../config/samples/validatingadmissionpolicy.yaml](../config/samples/validatingadmissionpolicy.yaml)
ships **commented out** and labelled 1.30+, carrying
`[UNVERIFIED — needs a 1.30+ cluster]` in place. It is not rendered into
`logweir.yaml` by any kustomization: a `ValidatingAdmissionPolicy` document
applied to a 1.29 cluster is rejected, and an install file carrying one would
fail on the stated floor. `optionalOldSelf` is 1.30+, which is why immutability
over an object with optional fields has to be an object-level rule there — the
same shape `config/crd/backupschedules.yaml` already uses, inside the CRD, where
it works on 1.29.

### Uninstall, and what it leaves behind

```bash
kubectl --context docker-desktop delete -f logweir.yaml
```

removes the control plane — the Namespace, the six CRDs and every custom resource
stored under them, the RBAC, the Deployment and the NetworkPolicy — and **deletes
nothing else**. Three things survive it, by design, and each is removed with its
own command:

```bash
# scratch topics a `mode: scratch` restore created (phase 9 normally removes them)
kafka-topics.sh --bootstrap-server <broker> --delete --topic 'logweir-scratch-<name>'

# archive objects — Logweir NEVER deletes these; retention only reports
aws s3 rm 's3://kafka-backups/logweir/<backup_id>/' --recursive

# evidence objects — likewise, and the controller's credential is read-only
aws s3 rm 's3://logweir-evidence/<prefix>/' --recursive
```

Two cluster-scoped objects are **not** in `logweir.yaml` and survive the delete:
the `TrustRoster` (if you applied the sample) and any RoleBindings you created in
step 1e.

```bash
kubectl --context docker-desktop delete trustroster default
```

### X-APPLY, recorded

Spec §16 clause 1. Run on `docker-desktop` (client v1.35.0 with Kustomize v5.7.1
built in, server v1.34.1) against a cluster that had never held a `logweir.dev`
CRD — the pre-flight below is what establishes that.

```bash
crds=$(kubectl --context docker-desktop get crd -o name)
echo "rc=$?"
# rc=0
printf '%s\n' "$crds" | grep -c 'logweir.dev'
# 0
```

```bash
kubectl --context docker-desktop apply --server-side -f logweir.yaml
# namespace/logweir-system serverside-applied
# customresourcedefinition.apiextensions.k8s.io/approvals.logweir.dev serverside-applied
# customresourcedefinition.apiextensions.k8s.io/backups.logweir.dev serverside-applied
# customresourcedefinition.apiextensions.k8s.io/backupschedules.logweir.dev serverside-applied
# customresourcedefinition.apiextensions.k8s.io/kafkaclusters.logweir.dev serverside-applied
# customresourcedefinition.apiextensions.k8s.io/restores.logweir.dev serverside-applied
# customresourcedefinition.apiextensions.k8s.io/trustrosters.logweir.dev serverside-applied
# serviceaccount/weirkeeper serverside-applied
# clusterrole.rbac.authorization.k8s.io/logweir-approver serverside-applied
# clusterrole.rbac.authorization.k8s.io/logweir-operator serverside-applied
# clusterrole.rbac.authorization.k8s.io/logweir-viewer serverside-applied
# clusterrole.rbac.authorization.k8s.io/weirkeeper serverside-applied
# clusterrolebinding.rbac.authorization.k8s.io/weirkeeper serverside-applied
# deployment.apps/weirkeeper serverside-applied
# networkpolicy.networking.k8s.io/logweir-runner-egress serverside-applied
echo "rc=$?"
# rc=0

kubectl --context docker-desktop apply --server-side -f logweir.yaml
# (the same fifteen lines, all `serverside-applied`)
echo "rc=$?"
# rc=0
```

Fifteen documents, twice, `rc=0` both times — and the second run reports
`serverside-applied` for all fifteen rather than erroring on a kind whose CRD is
still establishing, because the file carries no custom resource.

**The pod does not start, and X-APPLY does not claim it does:**

```bash
kubectl --context docker-desktop -n logweir-system get pods
# NAME                          READY   STATUS             RESTARTS   AGE
# weirkeeper-7ccb764bcd-bf8cc   0/1     ImagePullBackOff   0          22s
echo "rc=$?"
# rc=0
```

The waiting message is `Error response from daemon: error from registry:
denied`. That is the expected and recorded result of Global Constraint 37: the
image is referenced by tag, no such image has been pushed, and the install file's
digest rows read `blocked: no remote`. **X-APPLY proves `kubectl apply` exits 0;
it does not start a pod**, so on its own it can be ticked while the documented
install works for nobody but the author. What closes spec §16 clause 1 is pulling
the published digests back from a registry the author does not control, which is
Task 30b's.

The missing-Secret pre-flight, on a namespace holding none of the five:

```bash
just check-secrets logweir-t21; echo "rc=$?"
# check-secrets: logweir-signing-key is absent from namespace logweir-t21
# ...
# rc=1
```

And the cleanup, which is how this transcript ends:

```bash
kubectl --context docker-desktop delete -f logweir.yaml; echo "rc=$?"
kubectl --context docker-desktop delete ns logweir-system logweir-t21 --ignore-not-found; echo "rc=$?"
```


Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
