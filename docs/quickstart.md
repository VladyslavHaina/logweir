# Quickstart

This page is **the one supported setup-and-recovery guide**. A new operator
follows [*The supported path*](#the-supported-path-install-to-disaster-restore)
from top to bottom: install, a saved connection and destination, a schedule, the
first backup, a restore of a chosen point, verification, and a disaster restore
from a connected archive. Each step names who decides it and links to the
reference that owns the detail; this page does not restate those references.

The CLI and the local stack come after it, as
[*Demos and the standalone CLI*](#demos-and-the-standalone-cli): the product
backup/restore demo, the scratch-drill demo, a drill against your own archive
and the laptop Kubernetes walkthrough. To interpret evidence someone gives you,
use [verify-a-scorecard.md](verify-a-scorecard.md). What changed since the last
release, and what an upgrade requires, is [release-notes.md](release-notes.md).

---

## The supported path: install to disaster restore

**What this path is.** The Helm chart with its managed installation identity,
the `weirkeeper` controller, the product console (`logweir-api`, in `localAdmin`
or `shared` mode), and saved `KafkaCluster` connections and `BackupDestination`s.
The static page behind `kubectl proxy` is a supported **legacy** view of the same
objects, but the destination, discovery, readiness, catalog and schedule-policy
steps below are console-only ([kubernetes.md](kubernetes.md) §16). Each step
is a shipped task with its own docker-desktop evidence; what has not been run —
this page as one walk on a fresh install, among others — is named in the last
section.

**The PoC install profile is this path, made concrete.**
[deploy/poc/](../deploy/poc/README.md) installs it end to end with Helm, from the
published OCI chart and its `sha-` images, with no post-install patch: Traefik, a
cert-manager local CA for real TLS,
Dex for sign-in with one user per console role, the shared console, a scoped
controller, an approval-policy binding and a demo Kafka and MinIO to back up —
ordered commands, a check per step, first sign-in per role, the first backup
and restore, the two upgrade rehearsals and uninstall. Use it for a proof of
concept; the steps below are the same path for any installation.

### Who decides what

| Decision | Who | Where it is made |
|---|---|---|
| Install, upgrade, CRDs, controller scope | cluster administrator | Helm values ([install.md](install.md)) |
| Whose keys may approve and attest, per namespace | `logweir-trust-admin` (a `TrustPolicy`), or a cluster administrator (the legacy `TrustRoster/default`) | [keys.md](keys.md); never a namespace operator |
| How a namespace's restores are approved: `legacy-governed-v1`, `Ordinary` or `Governed` | installation administrator | `approvalPolicy.*` values, one rollout ([install.md](install.md) §5f); never a namespace object |
| Approving one restore | an approver who is not the requester | `logweir drill approve` / `logweir drill countersign`, on their own machine |
| Connections, destinations, schedules, backups, restore requests | `logweir-operator` (console role Operator) | the console |
| Whether archive objects are ever deleted | `logweir-retention-admin`, plus an administrator's approval of each plan digest | a `RetentionPolicy` ([kubernetes.md](kubernetes.md) §7f); the default deletes nothing |

Bind the operator, approver, trust-admin and retention-admin roles to
**different** people ([install.md](install.md) §5): one person holding two of
them can approve their own restore, or enforce a deletion plan they wrote.

### 1. Install

1. Check the floors: Kubernetes 1.29+, amd64-capable nodes for runner Jobs, and
   engine 0.21.0 ([install.md](install.md), top; [support-matrix.md](support-matrix.md)).
2. Choose images: the published digests of the exact CI run you deploy, not
   `latest` ([install.md](install.md), *Choose an image and installation path*).
3. Install with Helm and the managed identity — path (c) — declaring every
   namespace where backups and restores will run in
   `identity.authorizedRunnerNamespaces` (those namespaces must exist first).
   Add `retention.enabled=true` only if you will ever enforce retention.
4. Back up the installation identity at once ([install.md](install.md),
   *Back up and recover the installation identity*). Losing it loses the
   ability to sign; losing its public half loses the ability to verify old
   archives.

**Check:** the controller Deployment is ready, and
`logweir-signing-trust` carries a `key-id` ([install.md](install.md) §1).

### 2. Trust, keys and approval policy

1. Generate the approver's key pair on the approver's machine; its private half
   never enters the cluster ([install.md](install.md) §1).
2. Establish trust: the smallest first step is `TrustRoster/default` with the
   approver key and the installation's signing key from `logweir-signing-trust`
   ([install.md](install.md) §2). A `TrustPolicy` is the current mechanism —
   retirement, revocation and overlap rotation — and `logweir trust
   migrate-roster` writes one from the roster ([keys.md](keys.md)).
3. Decide each namespace's approval policy. Doing nothing leaves every namespace
   on `legacy-governed-v1`: an out-of-band signed approval per restore.
   `Ordinary` (the console's own confirmation) needs the **shared** console;
   `Governed` needs a change ticket and an independent approver
   ([install.md](install.md) §5f; [kubernetes.md](kubernetes.md) §8, *Approval
   policy*).

**Check:** `kubectl --context <ctx> get trustroster default` (or `get
trustpolicy`) reports its keys loaded.

### 3. Roles and the console

1. Bind the human roles ([install.md](install.md) §5): viewer, operator,
   approver and retention-admin with a `RoleBinding` per namespace, and
   `logweir-trust-admin` once, with a `ClusterRoleBinding` (it is
   cluster-scoped).
2. Turn the console on ([install.md](install.md) §5e). `localAdmin` is one
   administrator reached by `kubectl port-forward` — for a lab or break-glass;
   `shared` is the SSO console, needs `controller.watchNamespaces`, an OIDC
   client and a TLS Ingress, and is the only mode that can serve `Ordinary`
   confirmation.
3. If the cluster is 1.30+, fence the console's `create secrets` with the
   admission policy ([install.md](install.md) §5b).

**Check:** the console loads and `GET /api/v1/session` lists your namespaces.

### 4. A saved connection

Create the Kafka credential Secret with `kubectl`, then the connection in the
console's cluster page, naming that Secret — the console never shows or reads a
credential value. The console names the connection itself (`conn-` and 26
characters, shown once it exists); its role is how later steps tell a source
from a target. TLS is a checkbox on the form. **A private CA (`auth.tlsCa`) or a
non-default Secret key (`auth.secretRef.passwordKey`) cannot be set through the
console yet:** the product API's connection create has no field for either
(PLAT-07.2), so the console refuses such a connection rather than create one
without them. Create it with `kubectl` (or through `kubectl proxy`'s legacy
mode) — set the CA once and both of the runner's TLS clients use it
([kubernetes.md](kubernetes.md) §20) — and the console then reads it like any
other.

**Check:** *Test connection* creates a `SourceConnection` readiness check and
shows each row's state, code and remedy ([kubernetes.md](kubernetes.md) §21.0).
A `ready` connection check authorises nothing by itself.

### 5. A saved destination

Create a `BackupDestination` on the console's *Destinations* page with separate
grants per role — `archiveWrite`, `archiveRead`, `evidenceWrite`, `evidenceRead`
— scoped exactly as the measured table says ([install.md](install.md) §3.11).
**Give it an `evidenceRead` grant**: without one, every run's verification is
`NotAttempted` and no badge can turn green. No role is ever granted
`s3:DeleteObject`.

**Check:** the destination reads `Valid`, and *Test access* (a
`DestinationAccess` readiness check) is `ready` for the roles you configured
([kubernetes.md](kubernetes.md) §7a).

### 6. A schedule, and the first backup

1. On *Schedules*, create one: the source connection, **named topics** or **all
   user topics** (a discovery per run; say what happens when completeness
   cannot be established), the cadence with its preview in your time zone, and
   the destination ([ui/README.md](../ui/README.md), *Creating a schedule*).
   Everything but the source connection can be edited later, and an edit never
   reaches a run that already exists ([kubernetes.md](kubernetes.md) §9).
2. Press *Run first backup now* rather than waiting: a slot due before the
   schedule existed never fires.

**Check:** the run's operation page reaches `Succeeded` with a green badge —
*verified by weirkeeper … against key …* — and the run shows the window it
covered. A run that succeeded but reads `unverified: not attempted` is almost
always a destination with no `evidenceRead` grant.

### 7. Restore a chosen point

1. From the schedule's page, choose *Restore this point* on the exact row you
   want — an older point is its own link, not a highlighted row
   ([ui/README.md](../ui/README.md), *Restore, from here*).
2. In the wizard, choose the target connection (a `target` role), the topics and
   the new-topic mapping, and the point in time inside the covered window.
   Restores only ever write **new** topics.
3. The readiness check must pass before *Create the Restore* is enabled: every
   blocking row `ready`, except the approval row, which is `skipped` until the
   Restore exists ([kubernetes.md](kubernetes.md) §21.7; [ui/README.md](../ui/README.md),
   *The readiness check holds the submit*). A point with no saved destination —
   every point `v0.1.5` wrote — is checked the same way: the check reads its
   inline archive with the Secret its Backup named, as the restore will. Its
   evidence bucket starts as the archive's own; keep it there, because the
   controller verifies such a run only in its archive handle's bucket, and an
   advisory `destination.evidenceReadable` row says when a plan would not be
   verified ([kubernetes.md](kubernetes.md) §15.1a, §21.8).
4. Get it approved, by the namespace's policy:
   - `legacy-governed-v1`: download the plan bytes; the approver runs
     `logweir drill approve … --subject-kind Restore` on their machine and
     records the two files through the console (Approver role) or `kubectl`
     ([kubernetes.md](kubernetes.md) §17);
   - `Ordinary`: the shared console's confirmation is the authorization;
   - `Governed`: the console confirms the requester, and a different approver
     runs `logweir drill countersign` and submits it ([kubernetes.md](kubernetes.md)
     §8, *Approval policy*).

**Check:** the `Approval` reads `Verified=True`, the `Restore` is admitted,
and its operation page reaches `Succeeded` with a completion panel.

### 8. Verify what you got

- **The badge** is green only when the signed document verified under a key the
  namespace's trust accepts **and** the run succeeded (a Backup's `exitCode 0`,
  a Restore's `outcome pass`). *Signed before that key was retired* is still a
  pass; *recorded before revocation* never is ([kubernetes.md](kubernetes.md)
  §15.2).
- **The record check is a sample.** *Records verified in the sampled window* is
  the count read back from the new topics inside the sampled window, and *records
  sampled and matching* is how many of those matched byte for byte. Neither is
  the total the restore wrote, and no level in this version compares every
  record (*Terms* below).
- **Check it without Logweir.** Fetch the scorecard and its sidecar from the
  keys on the `Restore` (`status.evidence`) and run
  `python3 docs/verify_scorecard.py` over them ([verify-a-scorecard.md](verify-a-scorecard.md)).

### 9. Disaster restore from a connected archive

When the cluster that ran the backups is gone — or this is a fresh installation
pointed at an archive another installation wrote — no `Backup`, schedule or
source connection is needed ([kubernetes.md](kubernetes.md) §7d.1):

1. Create a read-only credential Secret with `kubectl`, and a destination whose
   `archiveRead` names it by name (*existing Secret name*). Widen `archiveRead`
   to the catalog row of [install.md](install.md) §3.11, or the catalog will not
   sync.
2. *Catalog* → *Connect an existing archive* (a `Full` sync). A sync reads the
   archive's catalog records; points written before the catalog existed
   (`v0.1.5` and earlier) have none until `logweir catalog sync` backfills them
   once ([kubernetes.md](kubernetes.md) §7d).
3. Establish trust for the archive's signing key out of band, then re-sync;
   there is no one-click trust ([keys.md](keys.md)).
4. Choose a point the catalog marks selectable (`Available` and `Verified` or
   `VerifiedHistorical`), name the topics, and restore it as in step 7 — the plan
   is bound to that exact receipt, and the runner re-verifies it before any data
   moves.

**Check:** the catalog reads `Synced`, the signer panel lists the key you
trusted, and the restore's completion panel appears.

### 10. Retention: reported by default, deleted only by decision

A schedule's retention settings only **report** what would be removed. A
`RetentionPolicy` starts in `mode: Report`. Moving it to `Enforce` is a
retention administrator's decision that also needs a separate delete-capable
credential with `s3:GetObject`, an unversioned bucket, the `logweir-retention`
ServiceAccount, and an administrator's approval of each plan digest
([kubernetes.md](kubernetes.md) §7f; [release-notes.md](release-notes.md), items
1–3). `mode: ExternalLifecycle` declares that the bucket's own rule deletes.

### 11. Upgrade and roll back

Read [release-notes.md](release-notes.md) before every upgrade: it lists the
required actions in order. The standing rule is **CRDs first, then the controller
and runner together, then the console** (on the Helm path these are successive
`helm upgrade`s of one release: first the image values, then any
approval-policy binding) ([install.md](install.md), *Upgrade CRDs
before upgrading the controller*), with the identity backed up beforehand.

### Terms: what the console says, and what it means

| The console says | The field behind it | What it means |
|---|---|---|
| *verified by weirkeeper at T against key K* | `status.evidence.verification.result: Valid` with `trust.basis` `Current` or `Historical` (or no `trust` block, on an object an older controller wrote), plus `exitCode 0` or `outcome pass` | the green badge ([kubernetes.md](kubernetes.md) §15.2) |
| *(signed before that key was retired)* | `trust.basis: Historical` | a pass; the key was valid when it signed |
| *unverified: invalid* | `result: Invalid` | a fact about the **document**: a signature or digest did not match |
| *unverified: not attempted* | `result: NotAttempted` | a fact about the **controller**: no credential, no object, or no trust material |
| *unverified: untrusted signer* | `result: Untrusted` | a fact about the **signer**: authentic bytes, a key this installation does not accept |
| *recorded before revocation* | `trust.basis: RecordedBeforeRevocation` | never green; a compromise-revoked key signed it |
| *pending* | `result: Pending` | the evidence fetch has not answered yet |
| *records verified in the sampled window* | `Restore.status.completion.recordsRestored` (`sample.records_restored`) | records read back from the new topics inside the sampled window — not the total written, and not the matching count |
| *records sampled and matching* | `completion.recordsSampledMatching` | how many sampled records matched byte for byte |
| verification scope `sampled` / `degraded` / `none` | `verificationScope.level` | how the records were compared; `complete` does not exist |
| *Visible user topics only — completeness not established* | `status.selection.coverage: VisibleUserTopicsOnly` | a dynamic run backed up what its principal could see; Kafka hides the rest silently |
| *All user topics (attested complete)* | `AllUserTopicsAttested` | only with an administrator's attestation ([kubernetes.md](kubernetes.md) §22.2) |
| catalog availability / verification | `Available`…`Partial` / `Verified`…`NotAttempted` | two separate axes; *selectable* needs both ([kubernetes.md](kubernetes.md) §7d) |
| protection health | `Healthy`, `AtRisk`, `Stale`, `Unprotected`, `Unknown` | `Unknown` is never healthy and never a failure ([kubernetes.md](kubernetes.md) §7e) |
| *Logweir reports what would be removed under this policy and removes nothing* | `RetentionPolicy.status.enforcement: RecommendationOnly` | nothing is deleted |
| *An isolated Logweir retention worker deletes archive objects under this policy* | `status.enforcement: LogweirWorker` | an approved plan is being enforced |
| *enforced by Logweir* / *declared by your provider; Logweir cannot verify it* / *not enforced* | `status.guarantees.*` | who, if anyone, enforces each retention guarantee |
| key evaluation `unknown` | a `TrustPolicy` with no fresh status | never read as valid |

### What this path has not yet been shown to do end to end

Every step above is on `main` and was exercised on docker-desktop by its own
task's journey; which of those tasks are Done and which still wait for a lab
run is in [release-handoff.md](release-handoff.md). This
page as one walk on a fresh install, and an upgrade from the last published
image that keeps identities, schedules and archive readability, are PLAT-20.2's
live half: the PoC profile ran them on docker-desktop on 2026-09-24 — a clean
install from the published chart, steps 4–9 through the shared console behind
Traefik (TLS) and Dex (OIDC), and upgrades from `v0.1.5` and `sha-f49849d…`
([deploy/poc/](../deploy/poc/README.md), *What the first live round showed*). It
found console defects on the way (among them: the names typed for a connection
and a schedule are replaced by minted ones, and *Test access* does not show its
result), which its report lists and which are being fixed.
[UNVERIFIED — a legacy inline-archive point's restore readiness check and evidence bucket, and Catalog → Connect, failed in the shared console on that round; the fixes are not yet live.]
A corporate identity provider bound by group, AWS S3, MSK and a
NetworkPolicy-enforcing CNI have not been run at all
([release-notes.md](release-notes.md), *Limitations*).

---

## Demos and the standalone CLI

The four paths below run the CLI and the local stack. They are demos and
drills, not an installation: nothing in them is the supported path above.

---

## Path 1: `just mvp-demo` — backup, point-in-time restore, signed receipt

```bash
just e2e-up      # Kafka (KRaft) + MinIO, in docker compose
just mvp-demo
just e2e-down
```

This is the one command that exercises the whole CLI path, in order:

| step | what runs | what it proves |
|---|---|---|
| 1 | preflight | the stack is up and healthy, and `orders` and `payments` hold **zero** records |
| 2 | produce | 1000 records into each topic, end offsets read back off the broker |
| 3 | `logweir backup run` | **the product** takes the backup, behind phase −1's admission guard, and signs a backup receipt |
| 4 | two readers | `logweir drill verify --payload-type backup-receipt` **and** `python3 docs/verify_scorecard.py --payload-type backup-receipt` |
| 5 | `logweir drill approve` | the plan is approved by hash, with a **different key** from the one that signs the result |
| 6 | `logweir restore run` | `target.mode: newTopic` at a `restore.point_in_time`: the records land in topics that did not exist, and nothing is torn down |
| 7 | two readers again | over the scorecard, plus the evidence object keys an auditor would fetch |
| 8 | summary | the new topics, their record count off the broker, the measured RTO and RPO, the receipt key and the scorecard key |

Use a fresh stack: the demo refuses existing records in `orders` or
`payments` before producing or backing up. Restart it with
`just e2e-down && just e2e-up` when repeating this disposable demo.

Requirements: `docker`, `cargo`, `openssl`, `awk`, and `python3` with
`cryptography`. Set `LOGWEIR_PYTHON` to override the interpreter. Output is in
gitignored `.demo/mvp/`; the script removes its own archive prefix on exit.

The demo generates separate approval and signing keys. They demonstrate
integrity, not organizational provenance; see [keys.md](keys.md).

---

## Path 2: the drill demo

```bash
./scripts/demo.sh
```

This demo uses a harness-created `kafka-backup` archive and restores it into a
marker-protected scratch cluster. Path 1 instead exercises Logweir's own
`backup run` and a `newTopic` point-in-time restore.

Needs `docker`, `cargo`, `openssl`, `shasum`, and `python3` with the
`cryptography` package. All five are checked before anything starts, so a missing one costs you
a second rather than four minutes. Override the interpreter with
`LOGWEIR_PYTHON=/path/to/python3`.

Tear down with `just e2e-down`.

**It leaves your working tree clean.** Everything the demo writes goes to
`.demo/` and `.engine/`, both gitignored, and the script checks `git status`
itself at the end and tells you the result.

The one thing worth knowing: `scripts/e2e-seed.sh` has a second job besides
seeding — by default it also refreshes two **checked-in** fixtures
(`e2e/fixtures/manifests/0.21.json` and
`e2e/fixtures/segments/upstream-0.21.0.kbak`) from the archive it just made.
Those bytes are not reproducible between runs, so refreshing them shows up as
two modified tracked files. That is a **maintainer** action — `just e2e-seed` —
and the demo opts out of it with `LOGWEIR_SEED_REFRESH_FIXTURES=0`. If you run
`scripts/e2e-seed.sh` directly and see two modified fixtures, that is why; set
the same variable to avoid it.

What it proves, in order: the engine is digest-pinned and extractable; a real
backup exists; the plan was approved by a **different key** than the one that
signs the result; the drill ran every phase against a real broker and a real
archive; and the scorecard verifies under **two independent verifiers**, one of
which shares no code with Logweir.

---

## Path 3: a scratch cluster you already have

### 0. What you need before you start

- An **existing** `kafka-backup` archive in an S3-compatible bucket, created
  by `logweir backup run` or another compatible producer. This drill path reads
  that archive; integrated `--from-cluster` capture remains deferred
  ([architecture](architecture.md#adr-0007-source-capture-scope)).
- A **scratch** Kafka cluster you are willing to have topics created in. Not
  your production cluster, and not a cluster anything else depends on.
- A **marker topic** on that scratch cluster. This is v0.1's segregation proof:
  if it is absent, phase 0 refuses the drill with exit 3 before anything runs.
  Create it with any name you like and put that name in the spec.
- The `kafka-backup` binary of the pinned digest on `$PATH`, or the container
  image, which carries it.

### 1. Write the drill spec

Start from [`examples/drill.yaml`](../examples/drill.yaml). Three blocks need
your attention:

**`source`** — where the archive is. `prefix` is the backup id's prefix in the
bucket, not the bucket root; getting this wrong produces "the archive holds no
backup set at the configured source storage location", which is the correct
refusal and a confusing first experience.

**`sample`** — **the window is deployment-specific and the example's dates are
illustrative.**

```yaml
sample:
  window_start: "2026-09-04T00:00:00Z"   # a range your archive actually covers
  window_end:   "2026-09-05T00:00:00Z"
  records_per_partition: 25
  anchor: head
```

A window that overlaps no segment is **refused** — "a drill over an empty window
would report a pass that means nothing" — rather than reported as a pass over
nothing. `anchor: head` is the only value v0.1 implements; `tail` and `random`
are refused at phase 0, for reasons [stability.md](stability.md) sets out in
full (they are unsound here, not merely unimplemented).

**`objectives`** — what you are actually testing.

```yaml
objectives:
  rto_seconds: 900
  rpo_seconds: 300
  pass_rate: 1.0
```

`rto_seconds` is compared against `measured.rto_excluding_preflight_seconds`,
not against the wall clock — see
[formats/drill-scorecard.md](formats/drill-scorecard.md) for why.

### 2. Check before you run

```bash
logweir doctor \
  --spec drill.yaml \
  --allowed-clusters allowed-clusters.json \
  --approver-key approver.pub.pem
```

`doctor` checks credentials, the engine **version** and glibc floor, target
reachability, the marker topic and the approver key — before a drill is
attempted. It compares the engine's own `--version` output against the pinned
`0.21.0`; it does **not** compute or compare an image digest
(`third_party/kafka-backup-binary.digest` is quoted in the failure message and
nowhere else), so a green `ok engine version` line says the right version ran,
not that the right binary did. Add `--strict` to treat a check it could not perform (for example
`storage`, with no live bucket to list against) as a failure rather than a skip.

`allowed-clusters.json` must name the target cluster's own id:

```json
{ "allowed_cluster_ids": ["<the scratch cluster's id>"], "source_cluster_id": null }
```

**Derive it from the running broker rather than typing it.** An allowlist that
does not name this cluster is refused at phase 0 with exit 3, which is the guard
doing its job and looks like a bug the first time.

### 3. The two-key approval flow

The approval is a separate signed document. In a real deployment it is produced
on the **approver's** machine, with the **approver's** key, and only
`approval.json` and `approval.sig` cross the boundary.

```bash
# On the approver's machine, with the approver's PRIVATE key:
logweir drill approve \
  --spec drill.yaml \
  --key approver.pem \
  --approver sre-oncall@example.com \
  --ticket CHG-40881 \
  --out approval.json
```

That writes `approval.json` **and** `approval.sig` beside it — the only path
`drill run` looks for the sidecar. It needs nothing but the `logweir` binary:
no clone, no Rust toolchain, no `jq`, no `shasum`. In the container image it is
`docker run --rm -v "$PWD:/w" -w /w logweir:v0.1.0 drill approve …`.

`plan_hash` binds the approval to the **exact bytes** of the spec that will run.
Edit the spec after approving — including moving the sample window — and phase 1
refuses with exit 3, which is the point: **re-run `drill approve` on every spec
edit.** `openssl dgst` cannot produce this sidecar: the signature covers
PAE(payloadType, payload), never the bare bytes.

**If the approver key equals the signing key**, Logweir does not refuse; it
labels the scorecard `approval.self_attested: true`, and both verifiers print
`SELF-ATTESTED` — **because they derived it**, by comparing `approval.key_id`
against the key that verified the signature, not because the document said so.
A document whose claim disagrees with that derivation is refused (`drill
verify` exits 4, `verify_scorecard.py` exits 1). A self-attested run is not a
forgery, but it is a materially weaker governance signal, and an auditor is
entitled to treat it as a reason to seek corroboration.

### 4. Run the drill

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1
# Optional engine override. Both doctor and drill run search LOGWEIR_ENGINE_BIN,
# ./.engine/kafka-backup, /usr/local/bin/kafka-backup, then PATH.
export LOGWEIR_ENGINE_BIN=/usr/local/bin/kafka-backup
export LOGWEIR_ENGINE_VERSION=0.21.0
export LOGWEIR_ENGINE_DIGEST=sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317

logweir drill run \
  --spec drill.yaml \
  --approval approval.json --approver-key approver.pub.pem \
  --allowed-clusters allowed-clusters.json \
  --signing-key signer.pem \
  --out scorecard.json \
  --metrics-file /var/lib/node_exporter/textfile/logweir.prom \
  --triggered-by "quarterly DR drill"
```

Credentials come from `object_store`'s **own** chain (static keys, then web
identity / IRSA, ECS, EKS Pod Identity, IMDS). That is **not** the AWS SDK
chain: `~/.aws/credentials`, `AWS_PROFILE` and SSO are unsupported.

`LOGWEIR_ENGINE_VERSION` and `LOGWEIR_ENGINE_DIGEST` are mandatory. An empty
value is refused with exit 1: a signed scorecard must name the engine image that
produced the restore.

`--out` writes `scorecard.json` and its DSSE sidecar beside it as
`scorecard.sig`.

### 5. Read the exit code — it is the result

| Code | Meaning | What you do |
|---|---|---|
| 0 | Pass. | Archive the scorecard. |
| 1 | Operational — the drill could not be attempted or continued. **No artifact.** | Fix the environment and re-run. |
| **2** | **A drill ran, was measured, and did not pass. A scorecard WAS written and signed.** | **Read the scorecard.** This is the finding you scheduled the drill for. |
| 3 | Refused by a guard, before anything ran. | Fix the plan. Nothing happened. |
| 4 | Signing or lock proof failed — and nothing was uploaded. | Fix keys or bucket permissions. |

**1 and 2 are completely different things** and are easy to confuse under a
scheduler. Under Kubernetes they are actively hard to tell apart unless the Job
is shaped correctly — read [kubernetes.md](kubernetes.md) before scheduling one.

### 6. Read the scorecard

```bash
logweir drill show scorecard.json
```

The table is a fixed-width summary of a signed document. Below the fourteen rows
it prints the objectives, whether they were met, `integrity.partial_reason` and
the engine sub-report's own caveat — the qualifiers that most change how much a
result is worth and that the frozen layout does not carry.

Verify it, twice:

```bash
logweir drill verify --scorecard scorecard.json \
  --signature scorecard.sig --public-key signer.pub.pem

python3 docs/verify_scorecard.py scorecard.json scorecard.sig signer.pub.pem
```

The second shares no code with Logweir. If they ever disagree, the format is
broken, not merely one of the tools.

### 7. What landed in the bucket

Under your evidence prefix — Logweir writes under `logweir/` and nowhere else,
and refuses a prefix that is not exactly that:

```
logweir/drills/<run_id>.json           the signed scorecard
logweir/drills/<run_id>.sig            its DSSE sidecar
logweir/drills/<run_id>.receipt.json   what the store answered AFTER the put
logweir/drills/<run_id>.receipt.sig
logweir/drills/<run_id>.teardown.json  what was torn down
logweir/drills/<run_id>.teardown.sig
```

The receipt exists because the scorecard's four `evidence` fields describe facts
that only exist after the upload, and the scorecard is signed before it. Verify
it with the same tool:

```bash
python3 docs/verify_scorecard.py --payload-type receipt \
    <run_id>.receipt.json <run_id>.receipt.sig signer.pub.pem
```

Then check the binding by hand — `scorecard_sha256` in the receipt is the sha256
of the scorecard's exact stored bytes:

```bash
shasum -a 256 scorecard.json
```

A **missing** receipt means "no storage evidence was published for this run" —
never "the upload was not create-only".

---

## Path 4: `just laptop-demo` — the whole product on docker-desktop Kubernetes

This walkthrough runs `weirkeeper`, custom resources and the local UI on a
disposable docker-desktop installation. Prepare the two local images using
[install.md](install.md)'s author-only path, then:

```bash
just e2e-up
LOGWEIR_DEMO_NONINTERACTIVE=1 just laptop-demo; echo "rc=$?"
```

The script refuses a different context. Its shared twelve-step implementation
is [scripts/demo-steps.sh](../scripts/demo-steps.sh): preflight, namespace and
install checks, keys and Secrets, roster, Kafka reachability, scheduled backup,
UI/API requests, Restore creation, out-of-band approval, terminal status and
both independent verifiers. The noninteractive run exercises the request
emitter; the browser half is a separate interactive pass:

```bash
LOGWEIR_DEMO_ONLY_STEP=10b ./scripts/laptop-demo.sh
```

The demo uses **author-only** images and a laptop MinIO endpoint. It validates
the product flow, not public image availability. It writes `.demo/laptop/` and
tears down its install, namespaces, archive prefix, temporary tags, proxy,
keypairs and compose stack. Use a disposable environment.

The recorded browser walkthrough is
[e2e/k8s/laptop-demo.md](../e2e/k8s/laptop-demo.md); CI behavior is documented
in [kubernetes.md](kubernetes.md) §18. A `logweir-ui` field manager alone cannot
prove a browser created an object; other API clients can set the same value.


---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
