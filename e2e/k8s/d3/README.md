# D3 live acceptance on docker-desktop

`d3_live.py` is the harness for decision D3 §15's catalog, retention and trust
scenarios — the live half of PLAT-15.1, PLAT-16.1 and PLAT-16.2, with evidence
toward PLAT-14.2 and PLAT-19.1. It is a *live* harness: it creates real
`Backup`s against a real Kafka, writes real objects into a real MinIO, and
reads the verdicts the controller wrote. A fixture-only run of it proves
nothing and it has no fixture mode.

## What it needs

- A docker-desktop cluster running the chart's controller. Every command it
  issues carries `--context docker-desktop`; it names no other context.
- The shared `logweir-scram-local` lab: it copies that namespace's
  `source-scram`, `logweir-s3`, `logweir-signing-key` and `minio-root` Secrets
  — and, for `old-archive`, `rehearsal` and `refused-point`, `target-scram`, the scratch broker's own SCRAM
  credential — into its own namespace and dials that namespace's Kafka and
  MinIO by service DNS. **Outside its own namespace it writes exactly two
  things**: the cluster-scoped `TrustPolicy` the trust phases, `rehearsal` and
  `refused-point` need (deleted only after an owner-label check), and, in
  `old-archive`, `rehearsal` and `refused-point`, topics on the shared
  `kafka-target` broker — `old-archive`'s restored topic under the prefix
  `<owner>-<stamp>-hist-`, a witness named for this run and names under this
  run's own rendered prefixes — which the phase deletes again after an
  ownership check (see below). `old-archive`'s row requires the Restore to
  finish `Succeeded`/`pass` with a Valid scorecard and the restored topic
  holding exactly the records the Backup archived; `Running` is not a restore. Its plan's RPO objective is the age of the subject Backup's own covered window plus one hour, because the shared lab's seed records are days old and a one-day constant made every such restore a signed `fail-objective` (lab-refresh-8). No phase of THIS harness changes the shared release; D2's `evf5`
  (lab-refresh-8's rows, below) scales its controller and restores it.
- For `refused-point`'s two `/points` rows: a `logweir-api` binary
  (`LOGWEIR_API_BIN`, else the newer of `target/{release,debug}/logweir-api`),
  started in `localAdmin` mode on a loopback port against this namespace only.
  Without one those two rows record NOT-RUN.
- `openssl` on the host, for the phases that mint a keypair (`old-archive`,
  `multiple-namespaces`, `rehearsal`).
- For `old-archive`: the lab roster's approver PRIVATE key, `approver.pem`,
  read from `$LOGWEIR_SCRAM_OUT`, else `$HOME/.logweir-lab/scram-e2e` (the lab's
  durable home since 2026-09-22), else the `/tmp/logweir-scram-e2e` symlink to
  it. It must be the private half of `TrustRoster/default.spec.approverKeys[0]`
  — after a lab rebuild that is the NEW approver key, and a private half left
  over from the previous lab is refused by the cluster.
- For `rehearsal`: a `logweir` binary with `drill approve --standing` —
  `LOGWEIR_BIN`, else the more recently built of `target/release/logweir` and
  `target/debug/logweir`. The phase asks the binary for `--standing` first and
  refuses, by name, one that lacks it.
- `busybox:latest` present on the node (`imagePullPolicy: Never`), and the
  MinIO client mirror `d3_live.py` pins by digest — present, or pullable
  (`imagePullPolicy: IfNotPresent`; upstream's `mc` images were withdrawn, see
  `third_party/minio-mirror/README.md`).
- **An image carrying `logweir-retention`**, for the five enforcement phases
  only. The product runner image now carries it, so the default is the
  `LOGWEIR_RUNNER_IMAGE` the shared controller itself names — see
  §"The enforcer's image". `--retention-image` overrides it to measure a build
  the lab is not running; a phase records **NOT-RUN** only when neither can be
  resolved.
- A python3 of version 3.12 or newer with no third-party packages (the shared
  venv `/tmp/logweir-roadmap-run/venv/bin/python3` on the lab host; the macOS
  `/usr/bin/python3` is 3.9 and fails at the first PEP 701 f-string).

## Running it

```bash
export LOGWEIR_D3_STAMP=$(date -u +%Y%m%dt%H%Mz)
export LOGWEIR_D3_OUT=/tmp/d3-live/$LOGWEIR_D3_STAMP
RET=logweir:scram-local            # optional; the default is the controller's
                                   # own LOGWEIR_RUNNER_IMAGE — see below

python3 e2e/k8s/d3/d3_live.py setup            # namespace, destinations, buckets
python3 e2e/k8s/d3/d3_live.py catalog          # PLAT-15.1: reconstruction after CR loss
python3 e2e/k8s/d3/d3_live.py catalog-cases    # duplicate identity, Missing, stale index, format
python3 e2e/k8s/d3/d3_live.py catalog-scale    # PLAT-15.1: 104 REAL points past `viewLimit`
python3 e2e/k8s/d3/d3_live.py catalog-access   # PLAT-15.1: partial access, corrupt, future major
python3 e2e/k8s/d3/d3_live.py retention        # PLAT-16.1: two destinations, honest reports
python3 e2e/k8s/d3/d3_live.py legal-hold       # PLAT-16.2: spec.holds[]
python3 e2e/k8s/d3/d3_live.py packaging        # proves the shipped image DOES carry the enforcer
python3 e2e/k8s/d3/d3_live.py preview                 --retention-image $RET
python3 e2e/k8s/d3/d3_live.py no-evidence-credential  --retention-image $RET
python3 e2e/k8s/d3/d3_live.py enforce                 --retention-image $RET
python3 e2e/k8s/d3/d3_live.py wrong-prefix            --retention-image $RET
python3 e2e/k8s/d3/d3_live.py denied-deletion         --retention-image $RET
python3 e2e/k8s/d3/d3_live.py trust            # CLUSTER LOCK (TrustPolicy is cluster-scoped)
python3 e2e/k8s/d3/d3_live.py signed-at-probe  # CLUSTER LOCK
python3 e2e/k8s/d3/d3_live.py notify           # PLAT-14.2: staleness, dedup, transport
python3 e2e/k8s/d3/d3_live.py protection-cases # PLAT-14.2: recovery, unavailable archive, scope
python3 e2e/k8s/d3/d3_live.py protection-verdicts  # PLAT-14.2: PointFactsUnread, a refused
                                               #   signature, D3 §3.3's resolve column
python3 e2e/k8s/d3/d3_live.py rehearsal       # D3 §15 L6 / PLAT-14.3: a rehearsal
                                               #   end to end. CLUSTER LOCK
python3 e2e/k8s/d3/d3_live.py refused-point   # lab-refresh-8: a REFUSED point under a
                                               #   stale view. CLUSTER LOCK
python3 e2e/k8s/d3/d3_live.py operation-states  # PLAT-14.1: mount failure (ConfigMap and
                                               #   Secret), unschedulable pod, engine crash
python3 e2e/k8s/d3/d3_live.py notify-transport  # PLAT-14.2: a webhook scaled to zero
python3 e2e/k8s/d3/d3_live.py rehearsal-faults  # PLAT-14.3: unavailable target, failed
                                               #   verification. CLUSTER LOCK
python3 e2e/k8s/d3/d3_live.py object-lock       # PLAT-16.2: a lock-enabled bucket with a legal
                                               #   hold, and a plain bucket then versioned:
                                               #   the controller's own Enforce Job refuses both
python3 e2e/k8s/d3/d3_live.py shared-set        # PLAT-16.2: two receipts over one backup set
python3 e2e/k8s/d3/d3_live.py control          # the negative control — it MUST fail
python3 e2e/k8s/d3/d3_live.py report
python3 e2e/k8s/d3/d3_live.py cleanup          # CLUSTER LOCK (deletes the TrustPolicy)
```

`catalog-scale` and `catalog-access` each create their OWN bucket and destination (`-c`, `-d`)
rather than reusing dest-b: `retention` asserts dest-b holds exactly its six points, and a
corrupt manifest planted there would fail a phase that never asked for one. `catalog-scale`
runs 104 real Backups, which is the only honest way past `viewLimit`'s CRD floor of 100 —
nothing here edits that floor. `protection-cases` needs `catalog` (for the view it refreshes)
and `notify` (for the namespace's first alert), and both are declared in
`PHASE_PRECONDITIONS`.

`notify` and `protection-cases` each create the point their policy measures, as a manual run
OF a `BackupSchedule` (`spec.scheduleRef.{name,uid}` — D3 §3.2: "the `spec.scheduleRef.uid`
field is the authority and the label is the index"). Without it `identity::is_run_of_schedule`
excludes a manual `Backup` from a policy naming `scheduleRefs`, the policy has an EMPTY
candidate set, and `Unprotected` is an answer about nothing — which `status.health` cannot
distinguish from the real verdict, so all three rows carry `selector_matched_a_run`. They name
DIFFERENT schedules (`keeps-running`, `recovery-runs`) so neither selects the other's point.
`notify` also ages its point past the objective, because `catalog` deletes every dest-a CR to
prove reconstruction and the `Stale` arm had nothing to be stale about; that makes the phase
take about six minutes.

`rehearsal` is D3 §15's **L6** and PLAT-14.3's live half: ten steps as ten rows, every
clause named after `claude/plat14-3b.review.md` §4's own words. It needs `catalog` (it
restores a point out of dest-a and writes the rehearsal's evidence back into it) and
`trust` — an ORDERING, because `trust`'s last row leaves the lab signing key **Revoked**
on this namespace's `TrustPolicy` and a rehearsal that inherited that would fail at the
point's evidence verdict for a reason belonging to the previous phase; `rehearsal_trust`
deletes and recreates the object (the CRD's `spec.keys` is append-only with no
Revoked->Active transition), which is only correct after that phase has run. It mints the
standing authorization with the SHIPPED signer, `logweir drill approve --standing`
(`docs/kubernetes.md` §7g) — never in python — so a `logweir` binary that has that flag
is required (see "What it needs"). It signs with an approver keypair it MINTS per run
rather than the lab roster's, so the phase does not depend on the lab's approver private
key at all (which macOS deleted once from `/tmp`, measured 2026-09-22). Both minted
private halves — the Active one and the Retired one step 10 needs — are 0600 inside a
0700 directory, are minted inside the phase's `try`, never enter an object or an
artifact, and are deleted in its `finally` even when the broker sweep before them raises
(`rehearsal-minted-private-keys-never-outlive-the-row`).

**On the shared scratch broker it touches only what it made.** The unrelated topic D3
§15 L6 names is `rehearsal-not-ours-<ownertag>-<stamp>` — the scenario's name as a
prefix, this run's identity as the rest — because one fixed name on a shared broker is
nobody's: two runs each deleted the other's witness. It is planted only if absent (a
pre-existing one stops the phase, and is neither adopted nor deleted), and deleted only
if this run planted it. Every topic under a rendered prefix `rehearsal-<uid[..8]>-` of a
`RehearsalSchedule` this run created is deleted at the end — after the schedule is
suspended and quiet, and after the live object's owner label and uid are checked
against the ones its create returned. `rehearsal-shared-broker-left-as-found` records
that sweep and fails if anything under those prefixes, or the witness, is left.

**The four arms run one after another, never together.** All four name the same
`rehearsal-target`, and the controller checks `TargetBusy` (another schedule's rehearsal
still running against the target) BEFORE the authorization, so an arm left firing
turns the next arm's row into a verdict about the harness. Each arm is suspended as
soon as its row is recorded, and the next starts only when the previous one has no
rehearsal running (`quiesce_arm`; a rehearsal held at admission with no Job is deleted,
because it would hold the target busy for ever). `l6-rehearsal` is suspended only after
step 5's read, because a suspended schedule records no `lastSucceeded`.

**Step 6 reads the scratch broker's own log** (lab-refresh-10). The mapped topic lives for under a second, shorter than one `kafka-topics --list` through `kubectl exec`, so "during" is what the background sampler saw OR what `kafka-target`'s own log (read only, from the Restore's creation on) says existed, and the row also requires the broker to record the creation AND the teardown deletion of every mapped topic and of nothing else under the prefix (`06-topics.json` `brokerLog`).

**Step 5 is decided on the reached verdict** (rehearsal-fix, lab-refresh-10). A `kubectl get -w`
on `l6-rehearsal` runs from the moment the rehearsal exists until step 5's read, and the row
requires: `lastSucceeded.evidence` equal to the rehearsal's signed scorecard KEY;
`lastSucceeded.at` not before the Restore's `Verified` transition; and no watched status write
that named the rehearsal in `lastFailed` (lab-refresh-9 recorded the pass as `lastFailed
{reason: ok}` 0.15 s after the terminal patch). The artifact `05-schedule-status.json` carries
the timeline (Restore `Complete`, `Verified`, `lastSucceeded.at`) and every watched write.
Since lab-refresh-11 (REHEARSAL-FIRE-PASS-STATUS-LOST) it also requires a watched write that
names the rehearsal in `activeRestoreRef` with `pendingRestoreRef` released (the fire pass's
commit landed) and `Authorized=True/Authorized` after the slot fired. The watch re-opens when
the API server ends it (30-60 min), so a row that waits an hour still sees every write.

**Steps 2-9 are a chain, and a step whose input never existed is recorded `NOT-REACHED`.**
That is a third verdict on purpose: on a controller image that predates PLAT-14.3b the
standing-authorized `Restore` holds terminally at `ApprovalNotReceived` — the documented
fail-closed rollback for an older controller — so step 2 FAILS, which is that defect
reproduced live, and steps 3-9 have no Job, no scorecard and no mapped topic to read.
Recording those as passes would credit the product for assertions that never ran and as
failures would blame it for a chain the first link broke. Step 2 requires ADMISSION — a
runner Job, or phase `Running`/`Succeeded` — not merely a reason other than
`ApprovalNotReceived`: a standing `Restore` refused `StandingAuthorizationRefused` or
`PlanHashMismatch` fails it.

The verdicts this phase can record, and what each means:

| verdict | meaning |
|---|---|
| `PASS` | every clause held |
| `FAIL` | a clause did not hold — a product fact, or a precondition the row names |
| `NOT-REACHED` | the row's input never existed (the chain broke earlier), or — step 7 only — the first rehearsal finished before the controller was obliged to evaluate the next slot against it (the next `* * * * *` boundary + `REQUEUE_SECONDS` (30) + 15 s). Never a pass. |
| `HARNESS-FAULT` | the harness's own ordering decided the row: a `TargetBusy` from another arm of this run, or (steps 5/6) a later ten-minute slot that fired before the arm was suspended. Names the busy `Restore`. Never a pass, and never blamed on the product. |
| `INCONCLUSIVE` | step 10 only: the refusal held, but the mechanism was not shown — the refused `Approval` did not say `KeyRetired`, or the passing arm's `Approval` was not `Verified=True` in the same run, so a build on which nothing verifies would look the same |

Step 7 FAILS on a second `Restore` listed while the first was still running, and on a
first rehearsal still running after that obligation with no skip recorded. Step 10, the
negative control, is independent of the chain and runs either way; it REQUIRES the
refusal it records, because zero Jobs is also what a build that never reconciles this
kind at all leaves behind — and it counts Jobs and bundle `ConfigMap`s by the child name
prefix `logweir-rehearsal-l6-refused-`, not only through `Restore`s, so a schedule-side
refusal does not make "zero" true by construction. Its `RehearsalHealthy` clause accepts
`False` or `Unknown/NoResult` — a spec correction of review §4 step 10's "`False`",
because a schedule refused before any rehearsal finished has no result to be false about
(`rehearsal_schedule.rs::REASON_NO_RESULT`).

The offline tests: `python3 -m pytest e2e/k8s/d3 e2e/k8s/d2`. `test_rows.py` drives every
row's predicate over planted-wrong fixtures; `test_rehearsal_sim.py` runs `rehearsal()`
itself, unmodified, against a fake cluster and controller on a fake clock, with mutants
for each defect the loops exist to catch. It is a model of the controller, not live
evidence.

`protection-verdicts` needs only `setup`: it creates its own three policies, its own two
catalogs, its own legacy destination and every Backup it measures, because its rows assert
exact alert ledgers and an incident another phase opened on the same policy would make
every count in them somebody else's. Three of its rows are written against the CORRECT —
fixed — behaviour of `PROTECTION-SECRETKEYS-UNPROTECTED`, so on a controller image that
predates the fix they FAIL, and that failure is the defect's live reproduction; each row's
detail says which of the two a reader is looking at, and `verdicts/controller.json` records
the image's `org.opencontainers.image.revision`. It writes no cluster-scoped object: the
refused signature is produced by SIGNING with a key the installation has never heard of
(this namespace's `logweir-signing-key`, restored afterwards) rather than by revoking a
known one, which would need a `TrustPolicy` and the cluster lock.

Who this run is, and where: `LOGWEIR_D3_OWNER` (default `d3w14`) is the
`logweir.dev/test-owner` label `cleanup` checks, the prefix of the buckets and of the shared
MinIO's minted user and policy — the shared MinIO has no namespaces, so two workers running
this harness at once must not share it. `LOGWEIR_D3_NS` names the namespace (default
`<owner>-<stamp>`).

`--retention-image <ref>` may also be given as `LOGWEIR_D3_RETENTION_IMAGE`.
Each phase appends to `$LOGWEIR_D3_OUT/state.json`, so they run one after
another in one namespace and `report` renders the whole table. The order above
matters in three places: `catalog` before `catalog-cases`, `retention` before
the enforcement phases (they plan against its points), and
`no-evidence-credential` before `enforce` (it asserts that nothing was deleted,
which is only meaningful while the plan's objects are still there). Run
`control` before `cleanup` so the negative control gets its live read.

## The cluster lock

`trust`, `signed-at-probe`, `rehearsal`, `refused-point`, `rehearsal-faults` and `cleanup` touch one
cluster-scoped
object (the `TrustPolicy`, which binds only this run's namespace). Take the
orchestration's lock around exactly those phases and release it immediately
afterwards. Every
other phase is namespaced and needs no lock, and **no phase changes the shared
release** — not its image, not its env, not its CRDs. `packaging` reads the
controller's `LOGWEIR_RUNNER_IMAGE`; it never writes it.

## lab-refresh-8's rows (D2 and D3)

The rows `claude/evidence-fetch`, `claude/verdict-precedence` and
`claude/fix-standing-verify` owe live proof for, and the D2/D3 rows whose
expectation those branches changed. Each is a named phase; each row judges a
pure predicate that has planted-wrong twins in `e2e/k8s/d2/test_evidence_fetch_rows.py`
or `e2e/k8s/d3/test_refused_point_rows.py` (`python3 -m pytest e2e/k8s/d2 e2e/k8s/d3`).

**Images and binaries.** Everything is built from main AFTER both
`claude/evidence-fetch` and `claude/verdict-precedence` have landed, with
`--label org.opencontainers.image.revision=$(git rev-parse HEAD)` (WORKER-RULES):

- the **controller** (`weirkeeper`) the lab release runs — the evidence-fetch
  Job, the `Pending` verdict, the Backup-verdict joins in retention and the
  rehearsal, and the slot-consuming skip all live there;
- the **runner** (`logweir`, `--platform linux/amd64`) the controller names in
  `LOGWEIR_RUNNER_IMAGE` — the evidence-fetch Job is a `check run` of it, and
  it prints the receipt digest the fetch is anchored on;
- **`logweir-api`** on the host, passed as `LOGWEIR_API_BIN` (else the newer
  of `target/{release,debug}/logweir-api`), for the two `/points` rows;
- the **`logweir` CLI** with `drill approve --standing` as `LOGWEIR_BIN`, for
  the rehearsal arms (as `rehearsal` already needs), and for D2's approvals.

D2 runs in its own namespaces (`d2_live.py setup` first; `D2W14_OUT`,
`D2W14_STAMP`); D3 runs after `setup` in `LOGWEIR_D3_NS` as above. Use
`LOGWEIR_PYTHON=/tmp/logweir-roadmap-run/venv/bin/python3` for both.

| row | phase | command | PASS requires |
|---|---|---|---|
| `S1.statusVerification` (swept) | `s1b` | `d2_live.py s1 s1b` | bk-a and bk-b (SecretKeys `evidenceRead`) read **`Valid`** within 240 s: `matchedKeyId`, `observation.jobRef.name` = `lwc-ev-<sha256(uid:attempt)[:20]>`, `mode: SecretKeys`, `presence: Complete`, no `retryAfter`, `windowCovered`, `records`, `capture`, `Verified=True`, runner `receiptSha256`, and no controller-identity location allowlisted. `NotAttempted` here is EVIDENCE-FETCH-JOB-UNBUILT again |
| `S1.notAttempted` | `s1c` | `d2_live.py s1c` | for dest-noread (no `evidenceRead`) AND dest-ci (`ControllerIdentity`, not allowlisted): `NotAttempted` with a detail, no `matchedKeyId`, no `observation`, never `Pending` on the watch, no `lwc-ev-` Job by `logweir.dev/check-owner-uid`, no `windowCovered` |
| `EVF-1` | `evf` | `d2_live.py evf` | dest-arg (`evidenceRead: ArchiveReadGrant`, `archiveRead` = its own read-only principal): the watch sees `Pending` naming attempt 1's Job, then `Valid` and nothing else; every `S1.statusVerification` clause holds |
| `EVF-2` | `evf` | (same run) | the Job: `logweir.dev/check-kind=evidenceFetch`; controlled by the Backup; `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` only, both from `a-archread` (never `a-writer`, never `logweir-signing-key`); no `LOGWEIR_EVIDENCE_AWS_*`, no `envFrom`; `automountServiceAccountToken: false`; `logweir-runner`; volumes exactly `check-plan` (= `<job>-plan`, immutable, controlled by the Backup) and `work`; the REAL pod controlled by the Job's UID; `ttlSecondsAfterFinished: 600` |
| `EVF-1.ttl` | `evf` | (same run) | the Job EVF-2 recorded (same uid, TTL 600, completed) is last seen within 30 s of `completionTime + 600` and gone by +120 s, while its owner Backup and a long-TTL Job in the namespace still exist |
| `EVF-4` | `evf4` | `d2_live.py evf4` | dest-evbroken's `evidenceRead` Secret lacks `secret-access-key`: attempt 1 `NotAttempted` naming `lwc-ev-…(uid:1)` and `CredentialSecretKeyMissing`, "attempt 2 starts at", `retryAfter` 45–90 s after `verifiedAt`; never `Valid`, no window; attempt 2's own Job `lwc-ev-…(uid:2)`, controlled by the Backup, and the status at attempt 2 |
| `EVF-5` | `evf5` | `d2_live.py evf5` — **CLUSTER LOCK** (scales the shared `weirkeeper` to 0 and back; the original replica count is restored in a `finally` and recorded in `objects/evf5/controller-restored.json`) | `Pending` seen, then the controller stopped; the Job finished with none running; after the restart exactly ONE `lwc-ev-` Job for the Backup (attempt 1's, created before the stop), `Valid` naming that Job's UID and written after the new pod started. `notRun` — never `pass` — when the fetch beat the stop |
| `EVF-6` | `evf6` | `d2_live.py evf6` (after `s1`, whose bk-a it restores) | a Restore whose evidence destination (dest-b) has a `SecretKeys` `evidenceRead`: `Pending` then `Valid`, `outcome` and `objectives` copied, `scorecardSha256`, `Verified=True` exactly when `outcome: pass`, the Job controlled by the Restore |
| `EVF-3` (console Restore) | — | `scripts/plat10-ui-e2e.mjs` (not this harness; its NotAttempted assumption is in "Class sweep owed" of `harness-rows-10.result.md`) | the PLAT-10 journey reaches the wizard for EVF-1's point |
| `protection-catalog-places-an-unverified-point`, `protection-unplaceable-point-is-unknown`, `protection-unknown-keeps-its-incident-open` (swept) | `protection-verdicts` | `d3_live.py protection-verdicts` | unchanged clauses, now on `dest-noread`/`dest-noread-stays` (bucket `-n`, no `evidenceRead`), the one shape still `NotAttempted` with no capture |
| `rehearsal-7b-a-skipped-slot-is-consumed-never-fired-late` | `rehearsal` | `d3_live.py rehearsal` — CLUSTER LOCK | on the one-minute concurrency arm, kept unsuspended through the first rehearsal's end: every `ConcurrencyBlocked` skip names a due slot (a minute boundary ≤ the read), `lastScheduledSlot` ≥ it, one record per slot, no Restore for a skipped slot, the next rehearsal for a later slot. `NOT-REACHED` if step 7 saw no skip |
| `refused-point-fixture-is-real` | `refused-point` | `d3_live.py refused-point` — CLUSTER LOCK, after `trust` and `rehearsal` | rp-1/rp-2 `Valid`; rp-4 `Valid` under the minted key; rp-3 (newest, via `dest-rp-slow`) `NotAttempted`/`CredentialSecretKeyMissing` with a retry owed; the manual-only view `rp-cat` offers all four |
| `standing-11-replaced-receipt-is-invalid-and-projects-nothing` | `refused-point` | (same run) | after the receipt object is replaced and the grant repaired, the retry makes the controller write `Invalid` on attempt ≥ 2; `receiptSha256` still the runner's; no `windowCovered`/`records`/`capture`/`matchedKeyId`; the stale row still `Available`/`Verified`/selectable. `HARNESS-FAULT` if no attempt was left |
| `retention-refused-newest-point-is-skipped-unreadable` | `refused-point` | (same run) | `keepLast 1, minUsablePoints 1`: control kept rp-3 and planned rp-4 `BeyondKeepLast`; now rp-3 skipped `Unreadable`, rp-4 kept and not a candidate, `planSha256` changed |
| `catalog-points-refused-point-is-not-selectable` | `refused-point` | (same run; needs `LOGWEIR_API_BIN`, else NOT-RUN) | control listed rp-3 selectable with no `backupVerdict`; now `selectable: false`, `backupVerdict: Invalid`, row still `Available`/`Verified`, omitted from `?selectable=true`, rp-1 still selectable, no `backupVerdictsIncomplete` |
| `protection-refused-point-under-a-stale-view-is-unprotected` | `refused-point` | (same run) | control: the catalog placed NotAttempted rp-3 (`Healthy`, basis `Catalog`); now `Unprotected`/`NoAvailablePoint`, `Staleness` Open, no point published, although the row could have placed it |
| `rehearsal-refused-point-is-never-selected` | `refused-point` | (same run) | a one-minute `RehearsalSchedule` whose only run is rp-3, with a `Verified=True` standing Approval: a slot after unsuspend skipped `NoQualifyingPoint`, zero Restores, zero Jobs. `INCONCLUSIVE` if the premise (authorization, selectable row) did not hold; `HARNESS-FAULT` on `TargetBusy` |
| `standing-12-revoked-signer-is-untrusted` | `refused-point` | (same run) | revoking the minted signer (`KeyCompromise`) turns rp-4 `Valid` → `Untrusted` about the same key, run still `Succeeded`, stale row still selectable |
| `retention-revoked-point-is-skipped-unreadable`, `catalog-points-revoked-point-is-not-selectable`, `protection-revoked-point-under-a-stale-view-is-unprotected`, `rehearsal-revoked-point-is-never-selected` | `refused-point` | (same run) | the four clauses above for rp-4 (`backupVerdict: Untrusted`; retention keeps rp-2 and still skips rp-3; protection's control is the `Valid` point `Healthy`) |
| `refused-point-minted-keys-never-outlive-the-row` | `refused-point` | (same run) | both minted private keys and the work directory are gone |

**Not built, and why.** verdict-precedence's fix round adds two more rows: a
`/points` page served `backupVerdictsIncomplete: Unavailable` after the API's
`list` on `backups` is removed, and a Backup whose verdict field is unreadable.
The loopback API uses the operator's kubeconfig, not the chart's ServiceAccount,
so the first needs a chart install with an edited Role; the second has no
product path at all (only a status patch writes a non-string verdict). Both are
covered by the branch's own crate tests and are listed as gaps rather than
built as fault injection.

## harness-rows-11's rows (PLAT-14.1, 14.2, 14.3, 16.2)

The tests lab-refresh-8 §5 found with NO committed row. Each is a named phase whose
row judges a pure predicate with planted-wrong twins in `test_rows.py` (the mount
and object-lock twins plant the exact shape the lab published on `f49849d`). Every
fault is induced from this run's own namespace, bucket and MinIO users; nothing
about the shared release changes. The console-view clauses read
`GET /api/v1/namespaces/{ns}/operations/{kind}/{name}` from a host `logweir-api`
(`LOGWEIR_API_BIN`, else the newer of `target/{release,debug}/logweir-api`); with
no binary those clauses read `None` and the row FAILS rather than passing on the
object alone.

| row | phase | PASS requires | negative control |
|---|---|---|---|
| `ops-mount-failure-is-volume-mount-failed` | `operation-states` | a Backup over a `KafkaCluster` whose `tlsCa.configMapKeyRef` names an absent ConfigMap: while waiting `progress.stage=Preparing`, `RunnerReady=False/VolumeMountFailed`, a `VolumeMountFailed` diagnostic about the runner Pod naming the ConfigMap, the operation view `preparing`/`VolumeMountFailed`; fail-fast collapses the Job deadline; terminal `Failed` with the `Failed` condition reason `VolumeMountFailed`, no exit code, view `failed`/`VolumeMountFailed`/`error` (D3 §13: "the terminal reason is the diagnostic") | twin: `f49849d`'s `NoExitCode` after the recorded diagnostic, no deadline patch, an unnamed object, a run that only said `WaitingForPod` |
| `ops-mount-failure-secret-is-volume-mount-failed` | `operation-states` | the same, for the projected `signing` Secret (this namespace's `logweir-signing-key` removed for one run, restored after): diagnostic `SigningKeyMissing`, `RunnerReady`/terminal reason `VolumeMountFailed` | twin: the Secret's shape judged for the ConfigMap's code |
| `ops-unschedulable-pod-is-pod-unschedulable` | `operation-states` | a namespace `LimitRange` defaulting 512Gi (deleted once the pod carries it): the pod asked for it and was `Unschedulable`; `RunnerReady=False/PodUnschedulable` with a Pod diagnostic; view `preparing`/`PodUnschedulable`; the Job keeps its own `activeDeadlineSeconds` (480, longer than fail-fast's 300 + 60 grace, so "no fail-fast" can fail); terminal `PodUnschedulable`, no exit code, view `failed`/`PodUnschedulable` | twin: a fail-fast patch, a terminal `NoExitCode`, a pod without the request |
| `ops-engine-crash-is-failed-operational` | `operation-states` | a destination whose `archiveWrite` user is denied `s3:PutObject` on SEGMENT keys only: the runner started (`RunnerReady=True/RunnerStarted`), the pod log carries `kafka-backup backup exited` and an `AccessDenied` on a `/topics/` key (the engine had read records), `Failed`, `exitCode: 1`, `exitReason: operational`, no receipt; view `failed`/`error`/`exitCode 1`/`noEvidence`; the fixture control (same source and topic, full grant) Succeeded | twin: exit 3, a manifest-only refusal, a runner that never started, a failed control |
| `notify-transport-failure-is-recorded-and-rewrites-nothing` | `notify-transport` | a policy whose only webhook is a Service with no endpoints, over a member point aged past the objective: one `Staleness` alert, `delivery.state: Failed` after exactly 3 attempts, `lastError` naming `webhook:failed`, `NotificationsDelivered=False/DeliveryFailed`; three delivery Jobs of one transition, each exit 1 with `notify-result=webhook:failed` and `transport error`, created ≥ 60 s then ≥ 300 s apart; no fourth; the health the alert opened on unchanged; every Backup's `resourceVersion` unchanged (`every_backup_unchanged`, the same comparison `notify-delivery-never-rewrites-a-backup` makes); the insecure-sink hatch open (else the failure is the https-only refusal) | twin: a rewritten Backup, a shut hatch, a fourth attempt, a rushed retry, a delivered alert, an https refusal, a relabelled verdict |
| `rehearsal-unavailable-target-skips-and-consumes-the-slot` | `rehearsal-faults` | a one-minute arm targeting `rehearsal-target-alias` (an ExternalName Service of this run's to the scratch broker, CUT to a non-resolving name after the standing Approval verified): a `TargetUnavailable` skip naming a due minute slot, each skipped slot seen while the cluster reported `reachable: false` (not the `Ready` message: the next pass rewrites it to "no slot is due"), `lastScheduledSlot` advanced to it, no Restore for a skipped slot, and — with the alias restored — a Restore for a LATER slot. `NOT-REACHED` if the cluster never reported `reachable: false`, or never `true` again | twin: a skipped slot fired late, a deferred `lastScheduledSlot`, no skip |
| `rehearsal-failed-verification-is-a-failed-rehearsal` | `rehearsal-faults` | the arm's own point with every `orders` segment re-sealed (KBAK reserved bytes changed, CRC32 recomputed: same records, different sha256): the rehearsal is `Failed`, exit 2, `outcome: fail-integrity`, its scorecard `Valid`, `status.evidence` naming the scorecard, sidecar and offset-report keys, `EvidenceRecorded=True`, `Verified` not `True`, and no `status.completion`; the schedule's `lastFailed` names it with reason `fail-integrity` (the verified signed outcome) and `lastSucceeded` does not; `RehearsalHealthy=False/Failed`; the view `failed`/`notPass`. `NOT-REACHED` when the Restore never ran a Job (on `f49849d`: `ConnectionPlanMismatch`, REHEARSAL-PLAN-AUTH-PLAINTEXT) | twin: counted as a pass, passed over the tamper, an untampered segment; lab-refresh-9's unpublished failure, no offset-report key, no `EvidenceRecorded`, `Verified=True`, a `completion`, the exit's reason over a `Valid` verdict |
| `rehearsal-restore-deleted-while-its-verdict-is-owed-is-a-failure` | `rehearsal-deleted` | (lab-refresh-10; rehearsal-fix review LOW-2) a one-minute arm whose rehearsal is deleted the moment it is terminal with its evidence verdict still owed (no verification block, or `Pending`); over EVERY status write of the schedule (a `kubectl get -w`): a write records it in `lastFailed` with reason `RestoreDeleted`, that write releases `activeRestoreRef` and carries `RehearsalHealthy=False/Failed`, no write records it as `lastSucceeded`, and (lab-refresh-11) no later slot is reserved or fired over it before the write that records it (the recording pass's own reservation of the next slot, in the one write before its commit, is the protocol and allowed). `NOT-REACHED` when every tried run's verdict landed before the delete (at most four slots) | twin: the pre-LOW-2 shape (never recorded, ref kept), recorded but the ref still naming it, another reason |
| `rehearsal-verdict-still-owed-after-the-wait-is-evidence-verdict-not-reached` | `rehearsal-verdict-not-reached` | (lab-refresh-11; own namespace, about 70 minutes) the namespace's evidence-fetch pool is saturated with `checks.maxEvidenceFetchActivePerNamespace` suspended Jobs wearing the check labels, so a one-minute arm's rehearsal finishes exit 0 with its verification `Pending`; over every schedule write: it is recorded `lastFailed {reason: EvidenceVerdictNotReached}` no earlier than `VERDICT_WAIT_SECONDS` (3600 s) after its `Complete` transition and within 300 s of it, with `RehearsalHealthy=False/Failed`; while owed `activeRestoreRef` named it, a due slot was skipped `ConcurrencyBlocked` and no later rehearsal was reserved or fired over it (the deciding pass's own reservation excepted); never `lastSucceeded`. A second row frees the pool and requires the late verdict to stay unpromoted. `NOT-REACHED` if the verdict landed | twins: early, unbounded, another reason, a later slot reserved while owed, no skip, promoted, never `activeRestoreRef`, never recorded; the constants are read from `rehearsal_schedule.rs` |
| `rehearsal-deleted-active-beside-an-owned-reservation-keeps-the-reservation` | `rehearsal-deleted-active` | (lab-refresh-11; reserve-commit review L1) a one-minute arm: once C1 is recorded and C2 is committed active and running, C1 is deleted and the status staged `activeRestoreRef=C1, pendingRestoreRef=C2`; the next write records C1 `RestoreDeleted` AND names C2 in `activeRestoreRef` with the reservation released, C2 is never unreferenced before its own verdict, which is recorded | twins: the a8f594ec shape (both refs cleared), promotion in a later write, nothing recorded |
| `preflight-check-plan-conflict-after-pending-is-recorded` | `reservation-writes` | (lab-refresh-11; reserve-commit class sweep) a DestinationAccess Preflight held `Queued` by suspended check-labelled Jobs while a foreign `<lwc-da-…>-plan` ConfigMap is placed; released: `Pending/PodNotStarted` then `Failed/CheckPlanConflict` within 2 s (one pass), terminal, no failed-reconcile line, no Job | twins: the unfixed build's error requeue, left Pending, a Job created |
| `backup-job-name-conflict-over-a-frozen-run-is-recorded` | `reservation-writes` | (lab-refresh-11) a ResourceQuota at the namespace's Job count lets the freeze pass record `status.execution` while the runner Job POST is refused; a suspended ownerless Job of the Backup's name is then placed: the Backup is `Failed/JobNameConflict` with `status.execution` kept, the stranger neither adopted nor run, no 409 in the controller log. The in-pass freeze→POST 409 stays kube-mock only | twins: adopted, execution lost, a 409 logged, the Backup ran |
| `retention-object-lock-provider-refusal-is-recorded` | `object-lock` | **FLIPPED at lab-refresh-9** (OBJECT-LOCK-DELETE-MARKER, `62ef1a1`/`fd17eb2`/`de14881`). A bucket made `--with-lock`, three points, a legal hold ON on every object of the oldest, a `mode: Enforce` policy (`keepLast 1`, `requireApprovedPlan: false`, a delete principal with `s3:GetObject`): the controller's own run finishes with exit 1, `lastEnforcement.deleted` EMPTY, `failed` naming BOTH the held and the control point `VersionedBucket`; no delete marker anywhere under either set and every object still its key's latest live version; the next evaluation does NOT protect the held point `LegalHold` (it stays a candidate); three runs degrade the policy (`EnforcementDegraded=True` naming `VersionedBucket` and the remedy "Enforce on an unversioned bucket"), `guarantees.ageExpiry: NotEnforced`, `legalHold: ProviderEnforcedUnverified`. The provider's own semantics are recorded beside it (`lock/00-provider.json`) | twin: `f49849d`'s delete-marker shape recorded `Deleted`; the pre-flip expectation (control deleted, held `Locked`, `LegalHold`); the held point alone; exit 0; a marker anywhere; never degraded; `ageExpiry` still `LogweirEnforced`; a bucket without lock |
| `retention-versioning-enabled-after-write-is-refused` | `object-lock` | ctl-batch-2's H1 shape: a PLAIN bucket, three points written into it (null version ids), THEN `mc version enable`, then the same policy: every clause of the row above, with the fixture clause "written unversioned, Enabled before the run" in place of the hold | the same twin (one rule) |
| `retention-shared-set-is-never-planned-under-a-retained-point` | `shared-set` | one Backup's Job run twice from its frozen inputs (two receipts, one backup set, `catalog-duplicate-identity`'s technique), a Report policy `keepLast 1`: no candidate shares its `backupId` with a retained point. `NOT-REACHED` unless both points are Available and Verified | twin: a candidate over a retained point's set, both planned together, one unusable |

`notify-transport` must run with no other phase creating, deleting or verifying Backups in
the namespace: its no-rewrite clause compares EVERY Backup's `resourceVersion` across
the window, so a concurrent phase's churn fails it for the harness's reason (measured:
run alongside `operation-states`, it failed on exactly the Backups that phase recreated).

`rehearsal-faults` runs after `rehearsal` and `refused-point`: it rebuilds the same
`TrustPolicy`. Both arms select from the phase's own point (`l6f-point`, made first);
a re-run deletes its two schedules, their Approvals and that point before starting,
because the schedules are sealed, the Approvals immutable and the point tampered with. Its broker sweep and key cleanup are recorded as
`rehearsal-faults-shared-broker-left-as-found` and
`rehearsal-faults-minted-private-keys-never-outlive-the-row`. `object-lock` releases
every legal hold it placed and removes every version and both buckets in its
`finally` (`lock/99-cleanup.json`, `ver/99-cleanup.json`); `cleanup`'s `rb --force` cannot remove a bucket
with held versions.

## The enforcer's image

`crates/weirkeeper/src/controllers/retention_policy.rs` renders every
enforcement Job's command as `logweir-retention` and takes that Job's image from
the controller's `LOGWEIR_RUNNER_IMAGE`.

**RET-NOIMAGE was that no image this repository built contained that binary** —
the product `Dockerfile` ran `cargo build … -p logweir` and copied
`/usr/local/bin/logweir` alone, so `mode: Enforce` produced a Job that died at
the kubelet with exit 127, `executable file not found`, which the `packaging`
phase proved live.

**That defect is closed.** `Dockerfile` builds `logweir-retention` beside
`logweir`, and `scripts/check-image.sh` check 7 refuses a runner image that does
not carry it. `packaging` now proves the inverse from the same probe: the
container starts and exits `3`, `logweir_retention::EXIT_REFUSED` — the
enforcer's own refusal when it is given no plan, which only a binary that exists
and resolves by bare name through `$PATH` can produce. The row is named
`retention-enforcer-ships-in-the-runner-image`; it was
`retention-enforcer-ships-in-no-image` and asserted `not found` here. So the
enforcement phases default to the controller's own `LOGWEIR_RUNNER_IMAGE` and
need no hand-built image.

`Dockerfile.retention` beside this file stays as the *test* recipe for an
enforcer image the lab is not running — a build under review, or one from
another commit — reached with `--retention-image`. It starts from the product
`Dockerfile`'s own `builder` stage rather than copying it, so there is no second
home for the cross-compile setup to drift in, and it changes neither the product
`Dockerfile` nor the chart. Nothing in `justfile` or CI references it:

```bash
docker build --platform linux/amd64 --target builder \
  -f Dockerfile -t logweir-builder:d3w14 .

docker build --platform linux/amd64 --load \
  -f e2e/k8s/d3/Dockerfile.retention \
  --build-arg BUILDER_IMAGE=logweir-builder:d3w14 \
  --build-arg RUNNER_IMAGE=logweir:scram-local \
  --label org.opencontainers.image.revision="$(git rev-parse HEAD)" \
  -t logweir:scram-local-d3w14 .
```

The resulting image is the shipped runner plus one binary, so the enforcement
rows are about the enforcer and not about a runtime that differs from the
product's. `--label …revision` is not decoration: a locally built image has no
other way to say which commit it came from, and `WORKER-RULES.md` requires it.

**The enforcement scenarios are operator-run by design**, not a swap of the
shared controller's image. `docs/kubernetes.md` §7f says the real per-candidate
object count comes from an operator running `logweir-retention run … --dry-run`;
this harness runs that, and the enforced pass beside it, as Jobs in its own
namespace against the plan it wrote. Those five phases record NOT-RUN only when
no image can be resolved at all — neither an override nor the controller's own
`LOGWEIR_RUNNER_IMAGE` — which is the honest verdict, and not a skip.

## Safety rules it enforces in code, not in prose

- Every object it creates carries `logweir.dev/test-owner=<owner>`; `cleanup`
  re-reads the namespace's label AND compares the UID it recorded at `setup`
  before deleting anything, and refuses a bucket whose name is not this run's.
  Every phase that deletes a cluster-scoped `TrustPolicy` checks that label
  first and refuses another run's policy of the same name
  (`delete_owned_trust_policy`).
- Retention enforcement only ever runs against the buckets this run created.
  The fixture's `kafka-backups` is written (the legacy-archive Backups the
  Backup-level evidence verdict needs live there) and never deleted from.
- The one credential the harness mints — the read-only MinIO user the
  denied-deletion case needs — is generated per run, registered in `MINTED`, and
  scrubbed out of every recorded argv, every captured stream and every artifact.
  `redact` runs over both before either is written.
- `report` re-reads every artifact and fails the run on anything
  credential-shaped **or on any minted value**, and `sweep_selftest` plants both
  shapes first and fails the run if the sweep misses either — so the clean sweep
  means something.
- `control` asserts something false about a **live** read of the catalog's
  pages, through the same digest-checked path the catalog rows use, and fails
  the run if it passes.
