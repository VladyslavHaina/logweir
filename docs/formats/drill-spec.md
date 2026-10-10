# The drill spec: `name`, `source.point`, `restore.time_basis`, `sample.coverage`, the replay selection, `target.topic_naming.original_name` and `notifications`

**This is not yet a complete drill-spec reference.** It documents exactly seven
things — the top-level `name` key, `source.point`, `restore.time_basis`,
`sample.coverage` with its bound, the replay selection (a window start,
`restore.point_in_time: "<start>/<end>"`), a restore under the original topic
names (`target.topic_naming.original_name`) and the `notifications` block —
because those are what Task 14, decision D3, FX-8, PROD-08.1, PROD-11.1 and
PROD-15.1 created and changed. Every other key of a drill spec is
described today only by the commented example at
[`examples/drill.yaml`](../../examples/drill.yaml) and by
[`crates/logweir-core/src/spec.rs`](../../crates/logweir-core/src/spec.rs). A
full reference is a separate piece of work; a partial document that says so is
more use than none, and less use than one that pretends to be complete.

Every key on this page is **snake_case**, matching the rest of the spec
(`pagerduty_routing_key`, `slack_webhook`, `engine_overrides`).

---

## `name`

```yaml
name: nightly-orders-drill
```

Optional. This drill's own stable identity.

It exists so that two drill specs pointed at **one** scratch cluster do not
share a PagerDuty incident. The alert dedup key is
`logweir-drill-{name}-{cluster_id}`; when `name` is absent it falls back to the
first 12 hex characters of the approval's `plan_hash` — a sha256 over the
approved plan bytes, distinct per spec and stable across re-runs of that spec.
Should that hash ever be too short to supply 12 characters, the key uses
`unnamed` rather than an empty identity, so it can never collapse back to
`logweir-drill-{cluster_id}` — one incident per cluster is the defect this key
exists to fix.

Set it. The fallback is correct but opaque, and the operational-failure route
(below) has no cluster id to fall back on at all: a spec with no `name` reports
every "logweir could not run this drill" under the single key
`logweir-drill-unnamed-preflight`, so every unnamed spec on the install shares
one incident.

`name` is **spec-side only**. It is never written to a scorecard. An
artifact-side drill identity is backlog **T1-8**, assigned to decision **O16**
with default *not funded*; see
[`docs/stability.md`](../stability.md#known-limitations-of-v01) for the
residual.

---

## `source.point` (execution contract v2)

```yaml
source:
  storage: {backend: s3, bucket: recovery, prefix: archive/}
  backup: nightly-7
  topics: [orders]
  point:
    point_id: lwp1-3f2a91c74b8e05d6a1f0c2b3948e7d15
    receipt_key: logweir/backups/nightly-7/01J….receipt.json
    receipt_sha256: sha256:…
    manifest_sha256: sha256:…
```

Optional. **The recovery point this plan is bound to** (decision D3 §5.5).
Absent means exactly what it meant before this block existed: the archive set
is chosen by `source.backup` (`latestCompleted` or a pinned backup id) and no
binding is checked. Every plan written before this block existed therefore
still loads, still verifies byte for byte, and still runs unchanged.

**Present, `source.backup` names the point's own set** — the receipt's
`backup_id`, `nightly-7` above (FX-16). A bound plan whose `source.backup` is
another set, or `latestCompleted` (which names whichever set is newest when the
run starts, so it can stop being the point's set at the next backup), is
refused; the console, the catalog route and every rehearsal render the point's
set.

**Why it is in the plan and not in the Job's environment.** The environment is
the controller's word for it; the plan is what the approver signed. Binding the
point into plan bytes means the approval covers *which archive object this
restore recovers from* — and the disaster path (PLAT-15.2: a fresh
installation, no `Backup` CR anywhere, only a bucket) has nothing else to bind
to.

**What the runner does with it, before any data-plane work.** Before a broker
client is constructed and before anything is written, the runner:

1. reads `receipt_key` from `source.storage` through a **read-only** handle;
2. checks `sha256(receipt bytes) == receipt_sha256`;
3. re-derives the point identity from those bytes — `lwp1-` plus the first 32
   lowercase hex characters of the same digest — and checks it equals
   `point_id`. The identity is content-derived, so it is never *believed*: a
   point id that had to be taken on trust would be a label anyone could
   relabel;
4. checks the receipt's own `archive.manifest_sha256` equals `manifest_sha256`,
   its `backup_id` equals `source.backup`, and its `archive.manifest_key` is the
   key the engine reads that set at under `source.storage`,
   `<prefix>/<backup_id>/manifest.json` (FX-16);
5. verifies the receipt's **signature** (its `.sig` sidecar, DSSE, payload
   type `application/vnd.logweir.backup-receipt+json;version=1.0.0`) against the
   evidence-signing keyring passed as `--evidence-keys`, and judges the key
   that verified with `logweir_core::trust::decide` for `EvidenceSigning` at
   the receipt's own `finished_at` (D3 §5.5 step 6);
6. reads the manifest the receipt names and checks its bytes hash to the same
   value. Steps 2–4 prove the plan and the receipt agree; step 5 proves an
   installation this one trusts wrote the receipt; this one proves the
   *archive* does.

The receipt, its signature and the manifest are read through the same archive
handle the restore uses (under the store contract, the controller-named
credential and CA). And once the set chosen by `source.backup` has been
described — after phase 0, before phase 2 — the runner checks it is the set the
receipt describes: the same set id, the digest of the manifest it just read
equal to `manifest_sha256`, and the same version id the binding's read
answered. That set is selected by the receipt's manifest key, not as the first
set the listing shows with the id, so the set described is the set the engine
restores. Everything the run takes from the receipt (FX-4's
capture coverage, FX-8's recorded timestamp types, FX-7's pin) is about that
set alone.

A digest or identity mismatch is **exit 3**, with `PointBindingMismatch` at the
start of the refusal message — the tampered-bundle case, moved to the archive.
A plan or a restored set that is not the point's set is **exit 3** with
`PointBindingSetMismatch`, before any target topic of the restore exists.
A signature fault is **exit 3** with `PointUntrusted`: no keyring, a keyring
holding no key, a receipt with no sidecar, a sidecar that does not parse, a
signature no key in the keyring verifies, or a key the keyring's lifecycle
refuses for this receipt. A receipt or manifest that is **missing or
unreadable** is **exit 1**: the archive did not answer, and that may be a
rotated credential or a briefly unavailable bucket, so telling an operator to
change an approved document would be the wrong repair. See
[`docs/stability.md`](../stability.md) for the whole stdout and exit contract.

**The evidence keyring (`--evidence-keys`).** Inside a cluster the controller
renders it into the Job's approval bundle as `evidence-keys.json` and pins its
digest (`LOGWEIR_EXECUTION_EVIDENCE_KEYS_SHA256`); it carries every key of the
namespace's resolved trust whose public half parses, WITH its lifecycle:

```json
{
  "formatVersion": "1.0.0",
  "keys": [{
    "publicKeyPem": "-----BEGIN PUBLIC KEY-----\n…\n-----END PUBLIC KEY-----\n",
    "trust": {
      "key_id": "f27c7f51…", "principal_id": "install:f27c7f51…",
      "usages": ["EvidenceSigning"],
      "not_before": "2026-01-01T00:00:00Z", "not_after": "2027-01-01T00:00:00Z",
      "state": "Active", "retired_at": null, "revoked_at": null,
      "revocation_reason": null, "revocation_effective_from": null
    }
  }]
}
```

The lifecycle is in the file because the runner, not the controller, is the one
that has read the receipt's claimed signing time. So a key **retired** after
the receipt was written still verifies it (D3 §7.4: a retired key keeps what it
signed before `retired_at`), a key **revoked for compromise** verifies nothing
(a restore Job holds no earlier independent observation of the receipt), a key
revoked as `Superseded`/`Unspecified` is a retirement at
`revocation_effective_from`, and a key without `EvidenceSigning` refuses as
`KeyUsageMismatch`. A standalone `logweir restore run` of a point-bound plan
(the disaster path, with no cluster) needs the same file, written by hand from
the installation's public evidence-signing keys.

A plan carrying this block requires **execution contract v2**, and that is
enforced: a v1 invocation carrying `source.point` is a post-rollout Restore
wearing an old version number, and it is refused by name with exit 3 before any
data-plane work. Let the in-flight legacy Restore finish (or delete it) and
create the new one; a legacy object is not upgraded in place.

---

## `restore.time_basis` (FX-8)

```yaml
restore:
  point_in_time: "2026-09-07T14:05:00Z"
  time_basis: producerTime
```

Optional, and **one value only**: `producerTime`. Any other spelling
(`ProducerTime`, `producer_time`, `appendTime`, an empty string) does not parse,
so a typo can never be read as consent. The key is snake_case like
`point_in_time`; the value is camelCase like `target.mode`'s `newTopic`.

**Why it exists.** The pinned engine archives each record's **producer**
timestamp: it reads a batch's first timestamp plus the record's delta and
discards the batch's max timestamp, the only place a broker on
`message.timestamp.type=LogAppendTime` writes its append time. Every selection
by time therefore reads producer time. For a `CreateTime` topic that is the
topic's own clock; for a `LogAppendTime` topic it is not — PROD-01.1 restored,
at a recovery point in 2001, six records the broker had appended in 2026, and
the drill passed, because phase 7 compares the restored topic with the archive
and both carry producer time
([`docs/stability.md`](../stability.md#a-point-in-time-over-a-logappendtime-source-is-refused-unless-the-plan-selects-by-producer-time)).

**What the runner does.** After the archive is described and before phase 2 —
so before any target topic of the restore is created and before the engine
starts — it decides, for each source topic the plan maps:

| the topic's recorded timestamp type | the plan selects it by time | `time_basis` absent | `time_basis: producerTime` |
|---|---|---|---|
| `LogAppendTime` | yes | **refused**, exit 3, `PointInTimeByProducerTime` | runs; listed in the scorecard's `source.time_basis.producer_time` |
| not recorded | yes | runs; listed in `source.time_basis.not_recorded` | the same |
| `CreateTime` | yes | runs, listed nowhere | the same |
| any | no | runs, listed nowhere | the same |

* **Recorded** means one of two records, and never a live read of the source:
  the archive manifest's `configurations["message.timestamp.type"]` (a topic
  OVERRIDE — the engine keeps explicit overrides only), or the effective value
  the bound, verified backup receipt recorded at backup time
  (`config_coverage[topic].timestamp_type`, receipt format 1.1.0 and later —
  the only record of a broker-wide default). `LogAppendTime` from either wins.
  With neither — an unbound plan, a receipt from before format 1.1.0, or a
  configuration read that failed — the type is **not recorded**, and it is
  never assumed to be `CreateTime`.
* **Selects by time** means: the plan states `restore.point_in_time` (always),
  or it states none and its `sample.window_end` — the window's end in that case
  — is earlier than the newest timestamp the archive manifest records for the
  topic. A restore with no point in time whose `sample.window_end` is at or after
  the archive's newest record takes the archive as written (a full restore) and
  is never refused by this rule.

The refusal names every refused topic and the record that made it
`LogAppendTime`, and the runner's final stdout line is
`refusal-reason=PointInTimeByProducerTime`, so a `Restore` object's
`status.exitReason` carries it.

**It is inside `plan_hash`.** The field is part of the plan bytes an approver
signs, so an approval minted over a plan without it does not authorise the same
plan with it, and the reverse. A `RehearsalSchedule` states it as
`spec.point.timeBasis: producerTime`, rendered into every slot's plan and
inside the standing authorization's `templateDigest`; a schedule that states
none renders none, and its slots over a `LogAppendTime` topic are refused
([kubernetes.md](../kubernetes.md) §7g).

**Absent field, old plans, old runners.** Every plan written before FX-8 lacks
the field and means "no time selection by producer time is accepted". A runner
built before FX-8 ignores the key (the grammar ignores unknown keys) and runs
the plan without refusing or labelling, which is the behaviour this field
exists to end; the signed scorecard of such a run carries no
`source.time_basis` block, and both readers print that the time basis was not
recorded.

---

## `sample.coverage` and `sample.complete_max_records` (PROD-08.1)

```yaml
sample:
  window_start: "2026-08-29T00:00:00Z"
  window_end: "2026-08-30T02:00:00Z"
  coverage: complete            # sampled (the default) | complete
  complete_max_records: 5000000 # optional: the most archived records it decodes
```

Optional. **How much of the restore phase 7 verifies.**

- **`sampled`** (the default, and every plan written before the field): the
  first `records_per_partition` records of each sampled partition, reconciled
  by a fingerprint that sorts headers; the sha256 of the segments those
  records came from; the archive manifest's count bound for the window, for
  the whole restore and (since FX-23) for every mapped partition on its own;
  and the engine's offset report, which must name every mapped partition the
  manifest proves holds records in the window. `sample.max_partitions`, when
  set, caps how many partitions are sampled: since FX-23 it keeps one
  partition of every restored topic first (round-robin, in manifest order) and
  a second of any only after that, and the scorecard names the topics it could
  not reach in `sample.unsampled_topics` (format 1.6.0). Before FX-23 it kept
  the first N partitions in manifest order
  ([what a sampled pass guarantees](../verify-a-scorecard.md#what-a-sampled-pass-guarantees-and-what-it-does-not)).
- **`complete`**: every archived segment of every partition of every restored
  topic is read, its sha256 checked against the manifest and its records
  decoded; the expected output is every archived record whose OWN timestamp
  is at or before the restore window's end (no lower bound: the window starts
  at the archive, unless the plan states a window start,
  `restore.point_in_time: "<start>/<end>"`, which is then the lower bound); every restored record is read back and compared with it by
  its `x-original-offset` — content with headers in order, exact counts,
  duplicates and order. The manifest's first/last-timestamp count bound is not
  consulted. The contract is
  [`PROD-08.1-integrity-contract.md`](../to-do/decisions/PROD-08.1-integrity-contract.md);
  the signed result is `integrity.verification`
  ([the scorecard format](drill-scorecard.md#integrityverification-format-140)).

`records_per_partition`, `anchor` and the sample window do not narrow a
complete verification; the window still names `sample.window_end`, the restore
window's end when the plan states no `restore.point_in_time`.

**`complete_max_records`** bounds a complete verification: the most archived
records it decodes, summed over the restored partitions (each partition's
count is taken from the manifest before any byte of it is read). When the next
partition would take the total past the bound, that partition and every later
one are NOT compared, and the signed block says `covered: false` with the
reason; the verdict is then never `pass`. **Complete coverage is never
silently replaced by sampling.** Absent means no bound.

**Refused at phase 0** (exit 3, before anything runs), because each asks for
two verifications at once:

- `coverage: complete` with `max_partitions`, which keeps only N partitions;
- `complete_max_records` with `coverage: sampled` (or no `coverage`);
- `complete_max_records: 0`.

**What it costs.** Complete verification reads the whole archive of the
restored partitions and the whole restored output; measured on the compose
stack in the decision record (about a minute per GiB of one-KiB records with
an optimised build on a laptop, several times the sampled check).
`logweir drill run` and `logweir restore run` read it from the plan. A
`Restore` declares the same value on `spec.coverage` (and
`spec.completeMaxRecords`), and the controller refuses one whose declaration
the plan does not say; a `RehearsalSchedule` asks for it with
`spec.bounds.coverage`, under a standing authorization whose signed scope says
`coverage: complete`; the console's restore wizard offers it as an advanced
choice with this cost beside it (PROD-08.1a,
[kubernetes.md](../kubernetes.md) §12 and §7g).

**It is inside `plan_hash`.** The fields are part of the plan bytes an approver
signs. Both are omitted from the serialised plan at their defaults, so a plan
that does not ask for complete coverage is byte-identical to one written before
the fields existed.

**Absent field, old plans, old runners.** A plan without the field is sampled,
exactly as before. A runner built before PROD-08.1 ignores both keys (the
grammar ignores unknown keys), runs a sampled verification and signs no
`integrity.verification` block, which both readers print as "coverage not
recorded" — never as complete.

---

## A window start: `restore.point_in_time: "<start>/<end>"` (PROD-11.1)

```yaml
source:
  topics: [orders, payments]          # a topic subset is just the topics named
restore:
  point_in_time: "2026-09-07T13:00:00Z/2026-09-07T14:05:00Z"   # <start>/<end>
```

Optional. A `point_in_time` holding ONE instant is what it always was: every
partition of every topic in `source.topics`, from the archive set's floor (its
earliest covered timestamp; guard G-WIN) to that instant. The INTERVAL form,
two RFC 3339 instants joined by `/`, also states the window's START. It is the
only way to state one: a `restore.window_start` key is refused when the plan
is parsed, naming the interval form. The contract is
[`PROD-11.1-replay-selection.md`](../to-do/decisions/PROD-11.1-replay-selection.md).

- **The start is INCLUSIVE**, like the end: a record whose timestamp equals it
  is restored. It is a time selection, so FX-8's `restore.time_basis` rule
  applies to it as to the end.
- **Every partition** of every topic in `source.topics` is restored from the
  start, unless the plan also states a partition subset (below). A topic
  subset is the topics `source.topics` names.

## A partition subset: `restore.partitions` (PROD-11.1b)

```yaml
source:
  topics: [orders, payments, audit]
restore:
  point_in_time: "../2026-09-07T14:05:00Z"   # from the archive's floor; or "<start>/<end>"
  partitions:
    orders: [0, 2]                            # only these partitions of orders
    payments: [1]                             # audit: every partition
```

Optional. Each topic named restores ONLY the partitions listed; a topic in
`source.topics` that is not named restores every partition. The owner decided
OD-9 (a) on 2026-10-09: a restore that states a subset signs scorecard format
**2.0.0**, which every verifier before it refuses as an unsupported major
instead of reading it as a restore of every partition
([the scorecard format](drill-scorecard.md#sourceselection-format-170-and-200);
[stability](../stability.md)).

**Only beside the interval form of `point_in_time`.** `restore.partitions` is
written beside `"<start>/<end>"` or, for a window from the archive's floor,
`"../<end>"` (an ISO 8601 interval with an open start). A runner built before
PROD-11.1 ignores an unknown `partitions` key and would restore every
partition of a plan an approver narrowed; it cannot parse either interval, so
it refuses the plan (`drill spec does not parse`, exit 1) before it touches
anything. A subset beside a plain instant, or with no `point_in_time`, does not
parse in this release; neither does `"../<end>"` without a subset, so a plan
has one spelling. `logweir drill approve` parses a `Restore` plan that states
`restore.partitions` before it signs anything, and refuses one that does not
parse (`SubsetPlanUnparseable`, exit 1, nothing written): an approval binds
bytes, and no approval this release mints carries a subset an older runner
would widen.

**One engine run per distinct subset.** The engine's partition filter applies
to every topic of one run, so topics with different subsets restore in
different runs (one more for the topics without one); phase 5 checks every
rendered run against the subsets the approved plan states, and the scorecard
says how many ran.

**What phase 7 judges.** Only the selection: samples are drawn only from the
selected partitions; the count bound and the per-partition presence check are
the selected partitions'; every other partition of a narrowed topic must be
EMPTY on the target (a record there fails the run, on both coverages); a
complete verification expects nothing from an unselected partition, so a
record there is `unexpected`.

**A selected partition with no record in the window** is signed
`preflight-failed` (exit 2) at phase 5, naming it, as below for a start.

**The restore preflight previews it** through the same selection function: its
`archive.coverage` row names how many partitions of how many topics the plan
selects and in how many engine runs, or reports `PartitionNotInBackupSet` for a
partition the archive does not list; `plan.parse` reports `SelectionInvalid`
for a subset of a topic the plan does not select, an empty subset, or a
repeated or negative partition.

**Refused, exit 3, before anything runs** — each names the value to fix, and
none is ever answered by restoring something wider than the plan states:

| what | when |
|---|---|
| a subset for a topic `source.topics` does not select; an empty subset; a repeated or negative partition | phase 0, before any broker or bucket is touched |
| a start at or after the window's end, or after `sample.window_end` | phase 0 |
| a start earlier than the archive set's floor — refused, **never moved to the floor** | as soon as the manifest is read, before any target topic is created |
| a subset naming a partition the archive set does not list | the same |
| a window no archived segment of a selected partition overlaps (an empty restore is never a pass) | the same |
| a start or a subset under a standing rehearsal authorization (`plan_within_scope`): a standing scope restores every partition from the floor and admits no narrowing nobody approved | before the run, with the scope's other checks |

**A partition with no record in the window** — every record of it before the
start, or after the end — is signed `preflight-failed` (exit 2) at phase 5,
naming it (`<topic>/<partition> empty: no records in the selected window for
this partition`), before any target topic is created. That is phase 5's
existing rule (the engine's header preflight reports the partition `empty`,
which is never a positive pass), as for a partition with nothing before the
window's end; a start makes it likelier. Choose a start every restored
partition has a record after, or restore fewer topics or partitions.

**What phase 7 judges for a start.** Only the window: samples are drawn from
the stated start, the count bound is every selected partition's over
`[start, end]`, and a complete verification computes its expected output by
each record's own timestamp in `[start, end]`.

**What the scorecard says.** A restore with a stated start only signs it in
`source.selection` (`window_start_ms`, `window_end_ms`; format 1.7.0,
[the scorecard format](drill-scorecard.md)), and the existing fields name it
too: `sample.window_start` is never earlier than the start,
`sample.coverage_note` opens with the selection, and a complete verification's
`window.start_ms` is the start. A restore with a subset signs format 2.0.0 with
`source.selection.partitions` and `engine_runs` (and `window_start_ms` when it
states a start).

**The restore preflight previews a start** through the same selection
function: its `archive.coverage` row reports `WindowStartBeforeCoverage` or
`SelectionEmpty`, its `archive.segments` row checks exactly the segments the
selection reads, and `plan.parse` reports `SelectionInvalid` for a start at or
after the end.

**It is inside `plan_hash`.** A plan without a selection serialises exactly as
one written before PROD-11.1 — a `RehearsalSchedule` slot's included.

**Old plans, old runners.** A plan without a selection restores the full
window, exactly as before. A runner built BEFORE PROD-11.1 reads
`point_in_time` as a single instant, so it cannot parse either interval form:
it refuses a plan with a start or a subset (`drill spec does not parse`, exit
1) before it reaches a broker or bucket, creates nothing and signs nothing. A
runner built after PROD-11.1 and before PROD-11.1b refuses a subset too: it
cannot parse `"../<end>"`, and it refuses `restore.partitions` beside
`"<start>/<end>"` by name (`PartitionSubsetsAwaitOwnerDecision`, exit 3). No
runner restores every partition of a plan that names a subset in the form this
release accepts. (A subset written beside a plain instant — which this release
does not parse — is ignored by a runner from before PROD-11.1, which restores
every partition: the decision record's §6 residual, now closed by the
grammar.)

---

## `target.topic_naming.original_name` (PROD-15.1)

```yaml
target:
  mode: newTopic                 # required
  topic_mapping_prefix: drill-   # still required: the LogAppendTime probe's namespace
  topic_naming:
    prefix: ""                   # the identity mapping: orders -> orders
    original_name:
      owners: []                 # the approver's statement: no declarative owner
      # owners: [{topic: orders, kind: strimzi, reference: kafka/orders}]
      # owner_path: true         # restore although an owner is found
```

Optional. **A restore under the ORIGINAL topic names, into topics that do not
exist** — the recovery of a deleted topic, or of a lost cluster onto a
replacement, without renaming anything. Every other restore maps each topic to
a new, prefixed name, and an empty `prefix` without this block is still
refused (`maps onto itself`). The owner's decision OD-2 narrowed
[`stability.md`](../stability.md)'s Never #1 to a LIVE topic: the restored
topic is created by this run, exclusively, and is a new generation of the name
(a new Kafka topic id), never the original topic.

- **`owners`** (optional list): an empty list is the approver's signed
  statement that no declarative owner (a Strimzi `KafkaTopic`, a GitOps
  repository, Terraform) manages any restored name; an entry
  `{topic, kind: strimzi|external, reference}` names one. Absent means the
  plan states nothing, and the runner must find another place it looked.
- **`owner_path`** (default `false`): restore although an owner is found. The
  approver states that the owner's reconciliation is paused for the restore
  and that it adopts the topic afterwards; Logweir still creates the topic
  itself.

The block is a strict object: an unknown key is a parse error (exit 1).

**It requires `sample.coverage: complete`.** A plan that carries the block
with a sampled coverage — stated, or left to its default — is refused by
name, `OriginalNameNeedsCompleteCoverage`, at exit 3 before the runner dials
anything (and `logweir drill approve --approval-subject original-name` refuses
to sign it). Under a production name another producer may still be writing: a
sampled check reads the first `records_per_partition` records of each
partition and a count bound, which such a record can pass, while the complete
check compares every restored record with the archive and reports one the
archive does not hold as unexpected, by its target offset
([`sample.coverage`](#samplecoverage-and-samplecomplete_max_records-prod-081)). `sample.complete_max_records`
may bound it; a run the bound stops signs `covered: false`, never a pass.

**It restores whole topics.** A plan that carries the block and
[`restore.partitions`](#a-partition-subset-restorepartitions-prod-111b) is
refused by name, `OriginalNameNeedsWholeTopics`, at exit 3 before the runner
dials anything (`logweir drill approve` refuses to sign it). The run would
create each topic under its own name with every partition the archive lists
and fill only the selected ones, and the partitions left out could never be
restored under that name afterwards, because a restore into an existing topic
is refused. A stated window, a start
(`point_in_time: "<start>/<end>"`) or an end, stays allowed: it restores every
partition, bounded in time. To restore a subset, restore it under a prefix.

**What the runner proves at phase 0** (exit 3, `refusal-reason=GuardRefused`,
each message opening with the condition's name; nothing created): the mode is
`newTopic` and `prefix` is `""` (`OriginalNameNotNewTopic`,
`OriginalNamePrefixNotEmpty`); the plan asks for complete verification
(`OriginalNameNeedsCompleteCoverage`) and states no partition subset
(`OriginalNameNeedsWholeTopics`); every restored name is absent on the
target;
the target is not the source cluster — the source cluster id the bound
point's VERIFIED receipt measured differs from the target's (the allowlist
file's `source_cluster_id` never counts: it is unsigned runner input) — or
every broker the cluster's metadata lists reports
`auto.create.topics.enable=false` (`OriginalNameAutoCreateEnabled`,
`OriginalNameAutoCreateUnknown`); an owner was looked for somewhere — the
plan's `owners`, the `KafkaTopic` resources given to `logweir restore run
--kafka-topic-resources <file>` (optionally narrowed by `--strimzi-cluster`;
a `KafkaTopic` the runner cannot read or whose reference it cannot record,
and a file holding no `KafkaTopic` unless it is the explicit empty `List`,
are refused `OriginalNameOwnerUnreadable` — never read as none) — and none
was found, there or, for a target that may be the source, among the owners
the point's receipt recorded at backup (which adds owners but never stands
in for looking), unless `owner_path` is `true`
(`OriginalNameOwnerNotChecked`, `OriginalNameOwnerPresent`,
`OriginalNameOwnersInvalid`); and the `LogAppendTime` probe's name,
`<topic_mapping_prefix>logweir-probe-<12 hex of the plan hash>`, is legal, is
no restored name and is free (`OriginalNameProbeUnusable`). The approval must
carry the separate approval subject `originalName`
(`ApprovalSubjectMismatch`; `logweir drill approve --approval-subject
original-name` mints one); a one-person confirmation (an authorization
document v2 under an `Ordinary` policy) must also carry every one of
`source.topics` re-typed, exactly (`OriginalNameConfirmationMissing`,
`OriginalNameConfirmationMismatch`; the owner's decision OD-10).

**Creation is exclusive, and nothing is ever deleted.** The names are looked
for once more right before `CreateTopics`, which itself fails on a name that
exists; either way the run stops before the engine starts, exit 1,
`failure-reason=TargetTopicAppeared`. A topic this run had already created
when the creation step stopped — for that race, or for any other reason
(`failure-reason=CreatedTopicsLeft`) — is LEFT in place, empty, and named on
the line before it, `target-topics-appeared={"appeared":[…],"left":[…]}`:
"created by this restore and left empty; remove it yourself once you have
checked nothing writes to it". No code path deletes a topic under an original
name: Kafka has no conditional delete, so a record a producer wrote between
any check and the delete would be lost with it.

**It is inside `plan_hash`.** A plan without the block serialises exactly as
before.

**Old plans, old runners.** A runner built before PROD-15.1 ignores the block
and sees the empty prefix, which maps every topic onto itself: it refuses the
plan at phase 0 (exit 3) before it writes anything. It never restores under
the original names without the conditions above. The full account, with the
`Restore` declaration and the approval subject, is
[kubernetes.md](../kubernetes.md#restoring-under-the-original-topic-names-prod-151).

---

## `notifications`

```yaml
notifications:
  webhooks:
    - https://example.internal/logweir-hook
  slack_webhook: https://hooks.slack.com/services/T0.../B0.../XXXXXXXX
  pagerduty_routing_key: R0...
  pagerduty_endpoint: https://events.eu.pagerduty.com/v2/enqueue
```

The whole block is optional, and so is every key in it. Absent means "notify
nobody"; it is never an error.

### `webhooks` (list of URLs, default empty) and `slack_webhook` (URL)

The **scorecard-summary** route. Each receives one JSON summary of a completed
drill — outcome, measured RTO and RPO, integrity level and result, whether the
approval was self-attested, and the list of redacted paths.

These fire **only when a scorecard exists** — a drill that ran to completion,
whether it passed or not. They are not used for the operational-failure route
below, because that route has no scorecard, and a scorecard-shaped body with no
scorecard behind it is how a dashboard starts reporting drills that never ran.

Every transport failure is logged and swallowed. A webhook being down never
changes a drill's exit code (Global Constraint 11).

### `pagerduty_routing_key` (string)

The PagerDuty **Events v2 integration routing key**. Present means the
PagerDuty route is on; absent means it is off. There is no other switch.

Two families of event are sent, under **two different dedup keys**:

| When | `event_action` | `dedup_key` | `severity` |
|---|---|---|---|
| The drill passed (exit 0) | `resolve` | `logweir-drill-{name}-{cluster_id}` | `warning` |
| The drill ran and did not pass (exit 2) | `trigger` | `logweir-drill-{name}-{cluster_id}` | `warning` |
| Operational failure, no artifact (exit 1) | `trigger` | `logweir-drill-{name}-preflight` | `critical` |
| A guard refused the plan (exit 3) | `trigger` | `logweir-drill-{name}-preflight` | `warning` |
| Result unattested, nothing uploaded (exit 4) | `trigger` | `logweir-drill-{name}-preflight` | `critical` |

The two keys are separate deliberately. "This drill ran and did not pass" and
"logweir could not run this drill at all" are different facts, and a later
passing run's `resolve` must not silently close an operational-failure incident
nobody has looked at.

### `pagerduty_endpoint` (URL, default US region)

Which PagerDuty **service region** the events go to.

Absent means `https://events.pagerduty.com/v2/enqueue` — the **US** region, the
behaviour of every earlier version. An account on the **EU** service region
must set:

```yaml
  pagerduty_endpoint: https://events.eu.pagerduty.com/v2/enqueue
```

Only `https://` is accepted. Anything else is **refused before a request is
made**, because the routing key travels in the request body and plaintext HTTP
would put a bearer credential on the wire. A refusal is not silent: it logs at
WARN with the message `pagerduty alert NOT delivered`, the `dedup_key` of the
incident that did **not** open, the run id, and the reason. The same line is
emitted when a request is attempted and fails, so "no page arrived" is always
greppable and never has to be inferred from the absence of anything.

A refused or failed enqueue never changes the exit code (Global Constraint 11),
and there is no retry.

---

## Credentials in this block

**A Slack incoming-webhook URL and a PagerDuty routing key are bearer
credentials, and today they live in the plaintext drill spec.** Whoever holds
one can post as that integration. When the spec is delivered to Kubernetes it
is delivered as a **ConfigMap**, which means every subject with `get
configmaps` in the drill namespace can read them; a ConfigMap is not a Secret
and is not encrypted at rest by default.

This is recorded, not fixed. Moving these three keys to a Secret reference is
decision **O17**, default *not funded*. Until it is funded:

- Treat the drill spec itself as sensitive, and scope RBAC on the drill
  namespace as you would for a Secret.
- Prefer a webhook or routing key scoped narrowly enough that its disclosure is
  a rotation and not an incident.

`pagerduty_endpoint` is **not** a credential — but it is free-form input, and
that is a different thing. Logweir reduces it to `scheme://host/…` wherever it
reaches a display surface: the WARN line above and `Debug` output both. The
host is what says *which region*, which is the whole reason the key exists, so
nothing diagnostic is lost; what is dropped is the userinfo, path and query,
which is where a token lives in a URL somebody pasted. The other three keys are
redacted more strongly still — presence is reported, values never are.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
