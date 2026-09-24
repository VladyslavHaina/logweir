# The backup receipt format, field by field

`application/vnd.logweir.backup-receipt+json;version=1.0.0`

The machine-readable schema is
[`schemas/logweir-backup-receipt-1.0.0.json`](../../schemas/logweir-backup-receipt-1.0.0.json)
and CI regenerates it from the Rust type and `diff -u`s it against the checked-in
file on every build, so this document and the schema cannot drift apart
silently. A signed worked example is
[`e2e/fixtures/signed/backup-receipt.json`](../../e2e/fixtures/signed/backup-receipt.json)
with its detached sidecar
[`backup-receipt.sig`](../../e2e/fixtures/signed/backup-receipt.sig) — read
[`e2e/fixtures/signed/README.md`](../../e2e/fixtures/signed/README.md) first,
which says what the throwaway key in that directory is and is not for.

If you are verifying a document rather than producing one, read
[../verify-a-scorecard.md](../verify-a-scorecard.md) for the mechanics of a
detached DSSE sidecar; everything it says about verifying-as-read applies here
unchanged, and this document is a field reference.

## Why this is its own document and not eight new scorecard fields

The drill scorecard is **frozen** at `format_version 1.0.0` with 21 top-level
properties and 17 required ones. A backup happens on a different cluster, at a
different time, under a different command, and the facts it establishes — the
source cluster id read from the broker, the rendered auth mode, the named topic
set, the pinned engine's digest, the manifest key and its sha256, the per-topic
record counts, the covered window — are top-level facts about that operation.
Bolting them onto the scorecard would have cost eight new top-level properties
in the one document that exists to stay still, and would have left every
scorecard ever written claiming, by the shape of its own schema, to say
something about a backup it never observed.

So the receipt has its own media type, its own schema, its own
`format_version: "1.0.0"` — **independent of the scorecard's** — and its own
five arms.

## Reading rules a consumer must honour

1. **`format_version` is semver, and its major is `1`.** Ignore unknown fields
   when the major matches what you support. **Refuse** a document whose major is
   higher rather than guessing at a shape you have never seen. The schema pins
   the major with a pattern (`^1\.[0-9]+\.[0-9]+$`) as well, so a schema-only
   validator refuses a `9.9.9` document too.
2. **Verify against the bytes as stored.** The signature covers the exact bytes,
   including the trailing newline. Never `jq .` a receipt, re-save it and then
   verify.
3. **`exit_code` is the ENGINE's exit status**, not `logweir`'s. The `logweir`
   process maps its own outcome through the exit-code contract in
   [../../README.md](../../README.md); this field is what the pinned engine
   subprocess returned.
4. **`covered` is in epoch milliseconds**, not RFC 3339. See
   [the covered window](#the-covered-window-and-why-it-is-not-rfc-3339) below.

---

## Identity and provenance

| Field | Type | Meaning |
|---|---|---|
| `format_version` | string | Semver of **this** format. `1.0.0`. Independent of the scorecard's. |
| `run_id` | string | ULID of the run that produced this receipt. Also the object key stem in the evidence bucket. |
| `backup_id` | string | The engine's identifier for the archive this run wrote — the **execution** id under Kubernetes. **Not** `run_id`: `archive.manifest_key` is keyed on this, and a set written by an older build can carry receipts from two runs. Since RECEIPT-DUP was fixed, at most one run per `backup_id` reaches the engine (see [the execution claim](#the-execution-claim-one-engine-run-per-backup_id)), so a new execution signs exactly one receipt. |
| `requested_at` | RFC 3339 | When the run was requested. |
| `started_at` | RFC 3339 | When the engine subprocess started. Logweir-measured. |
| `finished_at` | RFC 3339 | When the engine subprocess finished. Logweir-measured. |
| `exit_code` | integer | The engine's exit status. `0` **if and only if** `archive.manifest_key` names a manifest — invariant 2. |
| `triggered_by` | string | Free text from `--triggered-by`. Deliberately **not** a metric label: unbounded cardinality. |

`started_at`, `finished_at` and `exit_code` are Logweir-measured and never
engine-reported: the engine's `backup` subcommand has no `--format` and writes
no report file, so its start, finish and exit code are ours to time.

## `source` — the cluster the data came from

| Field | Type | Meaning |
|---|---|---|
| `source.cluster_id` | string | **Read from the broker**, never from the spec. The fourth rail of the backup guard records this value and re-asserts it is not the restore target; this is where the recorded value is attested. |
| `source.bootstrap_servers` | array of string | The bootstrap list the source client was given. |
| `source.auth.mode` | string | **A closed set of two: `plaintext` or `scramSha512`.** These are `AuthSpec`'s serde tag values, the `KafkaCluster` CRD's `auth.mode` enum byte for byte, and the only two strings `AuthSpec::mode_str()` returns — so the spec an adopter writes, the CRD they apply and this signed document all spell the mechanism the same way. **Any other value is refused by both readers** (arm 5). |
| `source.auth.username` | string \| null | The SASL username, when there is one. `null` under `plaintext` — which is not the same as an empty username. |
| `source.topics` | array of string | The named topic allowlist. A **named set with no glob metacharacter**, so this is the exact set of topics and not a pattern a reader would have to re-expand against a cluster it cannot see. |

**`auth` never carries a password, and has no field that could hold one.** The
secret reaches the engine through its own `${VAR}` environment expansion and is
never interpolated by Logweir — which is exactly what stops it being
interpolated into a document Logweir then signs and publishes.

## `engine` — what took the backup

| Field | Type | Meaning |
|---|---|---|
| `engine.id` | string | `oso-cli`. |
| `engine.version` | string | e.g. `v0.21.0`, at or above the supported floor. |
| `engine.digest` | string | `sha256:…`, from `third_party/kafka-backup-binary.digest`. |

`digest` is why this block is worth signing. Pinning is by digest and never by
tag; a receipt that named only a version would be satisfied by any binary
claiming that version.

## `archive` — what was written, and where

| Field | Type | Meaning |
|---|---|---|
| `archive.manifest_key` | string | The manifest's object key. Empty **if and only if** the backup did not exit 0 — invariant 2. |
| `archive.manifest_sha256` | string | `sha256:<hex>` over the manifest bytes **this run read back** — not over bytes Logweir remembers writing. |
| `archive.prefix` | string | The object-store prefix everything this run wrote lives under. Logweir writes only under its own `logweir/` prefix. |

## `records` — per-topic counts

An object whose keys are topic names and whose values are unsigned integers.
It has **exactly one entry per `source.topics` entry and no others** — invariant
3. Serialised from a sorted map, so two runs over the same topic set produce
byte-identical bytes here.

## The covered window, and why it is not RFC 3339

| Field | Type | Meaning |
|---|---|---|
| `covered.from_ms` | integer (int64) | **Inclusive** start of the covered range, **epoch milliseconds**. |
| `covered.to_ms` | integer (int64) | **EXCLUSIVE** end of the covered range, **epoch milliseconds**. Strictly `> from_ms` — invariant 4. |

The window is **half-open**: `[from_ms, to_ms)`. That is the convention
`config/crd/backups.yaml` already documents for
`Backup.status.windowCovered.toMs`, which Task 17 fills by copying these two
integers, so the two documents describe one range under one rule. Task 5 shipped
invariant 4 as `<=` and a test asserting that `from_ms == to_ms` was a legal
"instantaneous window"; that was the disagreement, and the receipt is the side
that moved (Task 5's review, F3).

A backup whose records all share one millisecond is still a window, and still
legal: `crates/logweir/src/backup/phase_run.rs` derives `to_ms` from the newest
segment's **inclusive** `end_timestamp` and adds one millisecond, so such a run
publishes `[t, t+1)` — a window containing exactly those records — rather than
the empty `[t, t]` invariant 4 now refuses. That conversion happens once, where
the window is measured, and nowhere else.

This is the shape the operator's `Backup.status.windowCovered{fromMs,toMs}`
mirrors, as two `int64`s. A Kubernetes status subresource has no date-time type
to mirror a string into, so two representations of one window — a string here
and an integer there — would need a conversion nobody owns, and the first
disagreement between them would be invisible because both would still be
well-formed. The receipt speaks the operator's units and the operator copies the
numbers.

---

## The five arms

`logweir_core::backup_receipt::BackupReceipt::validate_invariants` implements
these, and `docs/verify_scorecard.py::check_backup_receipt_invariants` mirrors
them ARM FOR ARM, IN ORDER. The messages below are the **exact** refusal text of
BOTH readers — compared byte-for-byte by
`crates/logweir-core/tests/backup_receipt.rs` (`backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message`
over arms 1–4, `arm_5_refuses_an_auth_mode_outside_the_closed_two` over arm 5, and
`validate_invariants_has_exactly_five_return_err_statements` over the total),
by `crates/logweir/tests/two_reader_parity_receipt.rs::two_reader_parity_over_the_backup_receipt_corpus`
over the eight documents in `e2e/fixtures/invariants/backup-receipt-index.json`,
and by `scripts/check-verifier-parity.sh`'s second loop — and they are not to be
reworded. `scripts/check-invariant-corpus.sh` additionally derives the arm list
from both readers' source and refuses to balance if they are not the same five
arms in the same order.

1. **`format_version` parses as semver and its major is `1`.** Checked first, so
   a document from a future major is refused before any other arm is evaluated
   against fields that build may have redefined. "Parses as semver" is strict:
   exactly three dot-separated non-negative integers, so `1`, `1.0`, `1.0.0.0`
   and `1.0.0-rc1` are all refused.

   > `format_version "2.0.0" is not a 1.x version this reader understands`

2. **`exit_code == 0` if and only if `archive.manifest_key` is non-empty.** A
   receipt for a failed backup names no manifest, and a receipt naming a
   manifest did not fail. A whitespace-only `manifest_key` counts as **absent**,
   not as a manifest made of spaces.

   > `exit_code 1 and manifest_key "logweir/…/manifest.json" disagree: a receipt names a manifest if and only if the backup exited 0`

   > `exit_code 0 and manifest_key absent disagree: a receipt names a manifest if and only if the backup exited 0`

3. **`records` covers exactly `source.topics`.** A receipt that counts a topic
   the run was never asked to back up, or omits one it was, is describing some
   other run — and either way the per-topic figures cannot be read against the
   topic list beside them. Both sides are rendered sorted, so the message is
   deterministic.

   > `records covers {"orders"} but the named topic set is {"orders", "payments"}`

4. **`covered.from_ms < covered.to_ms`, strictly.** The end is EXCLUSIVE, so a
   window that ends before it begins is meaningless and one that ends where it
   begins is empty — and an archive that captured a record cannot cover an
   empty range.

   > `covered.from_ms 2 is not before covered.to_ms 1: the covered window's end is EXCLUSIVE, so an empty range covers no record`

5. **`source.auth.mode` is `plaintext` or `scramSha512`, and nothing else.** The
   only arm that is not a claim the document makes against itself: the receipt
   does not contradict itself, it names a mechanism this format has no spelling
   for. It is last for that reason — a document that contradicts itself should
   be told so first. The two values are `AuthSpec`'s serde tags, so `logweir`
   itself has exactly one writer of this field and no way to reach a third
   value; the arm is here for the documents this tree did not write, and
   because `Backup.status.auth.mode` — which the operator copies FROM this
   field — promises the same two in its own CRD description.

   > `source.auth.mode "scram-sha-512" is not one of the two values this format defines: "plaintext" or "scramSha512"`

---

## Verifying a receipt

Two readers, one contract. Both check the detached DSSE sidecar against the
bytes as stored, and both take the document type by name:

```
logweir drill verify --payload-type backup-receipt \
  --scorecard e2e/fixtures/signed/backup-receipt.json \
  --signature e2e/fixtures/signed/backup-receipt.sig \
  --public-key e2e/fixtures/signed/public.pem
```

```
python3 docs/verify_scorecard.py --payload-type backup-receipt \
  e2e/fixtures/signed/backup-receipt.json \
  e2e/fixtures/signed/backup-receipt.sig \
  e2e/fixtures/signed/public.pem
```

`--payload-type` defaults to `scorecard` in both readers, so every invocation
that predates the receipt is unchanged. The accepted names are `scorecard`,
`backup-receipt`, `receipt` (the post-put storage readback of a scorecard) and
`teardown`; anything else is an **error** rather than a passthrough, because a
typo'd media type would otherwise surface as "unexpected payloadType" and read
like a bad artifact instead of a bad command line.

The `--scorecard` flag keeps its name even when it names a receipt. Renaming it
would break every existing invocation, every document and
`scripts/check-verifier-parity.sh` in exchange for a better word.

> **What each reader checks today, stated plainly rather than implied.** Both
> readers now run **all five arms above** over a `--payload-type
> backup-receipt` document, and both were given them in one commit (Task 5b) so
> that they could never disagree in between. `logweir drill verify` prints
> `checked:   the signature AND all five backup-receipt invariants …`;
> `docs/verify_scorecard.py` prints `verifier: verify_scorecard.py 1.10.0
> (backup-receipt invariant set: …)`. Both compare the sidecar's `payloadType`
> **in full**, so a genuinely-signed scorecard presented as a receipt is refused
> as a substitution rather than accepted — `drill verify` exits 4 and says
> `PAYLOAD TYPE MISMATCH`, which is deliberately not `SIGNATURE INVALID`: the
> signature may be perfectly valid over some other document.
>
> An exit 0 from either reader therefore means "these bytes are signed by this
> key under this media type **and** the document does not contradict itself".
> The weaker sentence — `checked:   the SIGNATURE only …` — is still printed for
> `--payload-type receipt` and `--payload-type teardown`, whose invariant
> readers are not in tag 1, and
> `crates/logweir/tests/cli_verify.rs::the_signature_only_verdict_is_still_reachable`
> keeps it honest.
>
> **Where else the five arms are enforced.** At the two places a receipt is
> WRITTEN: `crates/logweir-evidence/examples/mint_backup_receipt_fixture.rs`
> validates before it signs, and `logweir backup run` validates the exact
> document it is about to sign before it signs or uploads anything
> (`crates/logweir/src/backup/phase_run.rs::persist_receipt`, step 1) — a
> violating receipt is exit **4** with nothing in the bucket, asserted by
> `crates/logweir/tests/backup_run.rs::a_receipt_that_cannot_be_signed_is_exit_4_and_puts_nothing`.
> Before Task 5b this paragraph named `logweir backup run` while that command
> wrote no receipt at all; it is now true by execution.

## Where `logweir backup run` puts it

Two objects, both create-only, both under Global Constraint 6's `logweir/`
root:

```
logweir/backups/<backup_id>/<run_id>.receipt.json
logweir/backups/<backup_id>/<run_id>.receipt.sig
```

The prefix is an **assertion**, not a convention: `Store::put_create_only`
refuses any key outside `logweir/`, so a build that tried to write the receipt
elsewhere aborts rather than writing it. The evidence handle is derived from the
archive's own object-store location with the prefix replaced by `logweir/` —
`Backup.spec` carries one URL, the archive root, and `Store::from_url` refuses
any evidence prefix that is not exactly `logweir/`.

The **final two stdout lines** of a successful `logweir backup run` are, in this
order and with nothing after them:

```
receipt-key=logweir/backups/<backup_id>/<run_id>.receipt.json
sidecar-key=logweir/backups/<backup_id>/<run_id>.receipt.sig
```

That is a machine contract (interface **I7**): the Kubernetes pod log API has no
stream selector, so a controller reading a Job's output cannot separate stdout
from stderr and reads the last lines instead.

### The execution claim: one engine run per `backup_id`

A third object sits beside the receipts, and it is the only one there whose key
does not carry a run id:

```
logweir/backups/<backup_id>/execution.claim.json
```

**Why it exists (tracker defect RECEIPT-DUP).** The engine writes
`<prefix>/<backup_id>/manifest.json` with its own, unconditional store client. A
second engine run under the same `backup_id` — a Kubernetes Backup Job lost and
re-created from its frozen inputs, or a second `logweir backup run` with the
same spec — replaced the manifest the first run's receipt attests. When the
topic had advanced in between, the first receipt's `archive.manifest_sha256` no
longer matched the manifest in the bucket, and every verifier that reads the
archive back (a point-bound drill, the `catalogSync` deep check, an auditor with
`sha256sum`) reported the first receipt as describing an archive that is no
longer there. Logweir cannot make the engine's write conditional, so it makes
sure the second engine run never starts.

**What the runner does.** After every local and read-only check and
immediately before the engine starts, `logweir backup run` puts the claim with a
conditional create (`If-None-Match: *`) and then puts it a second time: the
second put must be refused as `AlreadyExists`. Only then does the engine start.

| What the store answers | Exit | The message names | What happened |
|---|---|---|---|
| first create succeeds, second is refused as already existing | — | — | the run holds the claim; the engine starts |
| the first create is refused because the claim **already exists** | **1** | `ExecutionAlreadyClaimed` | an earlier run of this `backup_id` reached the engine. **No engine run, no receipt.** Retry under a **new** `backup_id`: a new `Backup`, or — exit 1 being retryable — a schedule's next attempt `-r<k>` **when the schedule has `spec.retry`**; without it the slot is `RunFailed` |
| the first create is refused for any other reason (a missing `s3:PutObject` on `logweir/*`, a transport error) | **4** | `ExecutionClaimUnproven` | lock-proof failed, nothing uploaded — the engine never started |
| the backend reports conditional put unsupported (the store falls back to HEAD-then-PUT) | **4** | `ExecutionClaimUnproven` | a HEAD-then-PUT is not exclusive, so the claim is no lock |
| the **second** create succeeds | **4** | `ExecutionClaimUnproven` | the store accepts `If-None-Match: *` and overwrites anyway; a claim on it is no lock |

Both refusals end with a final stdout line `failure-reason=ExecutionAlreadyClaimed` (exit 1) or
`failure-reason=ExecutionClaimUnproven` (exit 4), the exit-1/4 twin of exit 3's `refusal-reason=`.
A Kubernetes controller lifts it into `Backup.status.exitReason` and the terminal condition's
message, and only beside the exit code it belongs to.

**Transient failures are safe, and say so imperfectly.** The object-store client retries a 5xx:
if the server committed the claim before answering 5xx, the retried create is refused and the
run's own claim is reported `ExecutionAlreadyClaimed` (exit 1). A transport failure on either
create is exit 4. In both cases no engine ran and nothing was signed; an operator who can read the
evidence root can tell the first case apart by comparing the claim's `run_id` with the run's own.
`claimed_at` is the instant the run was requested, not the instant of the put.

A claim also stays behind for every execution that reached it and then failed — a few hundred
bytes each, on the same never-deleted lifecycle as receipts under `logweir/`.

The claim is **unsigned and never read by the runner**: it is a lock, not
evidence, and its existence is learned from the conditional put's own answer.
So it needs no permission the runner did not already hold — `s3:PutObject` on
`logweir/*` (`evidenceWrite`) — and adds no `s3:GetObject` or `s3:ListBucket`
under `logweir/`. Its body names the run that holds it, for an operator who can
read the evidence root:

```json
{"backup_id":"<backup_id>","claimed_at":"<RFC 3339>","format_version":"1.0.0","run_id":"<run_id>"}
```

It is never deleted by Logweir: the retention worker refuses every key under
`logweir/`, and a deleted claim would let a later run of the same execution
overwrite an attested manifest again.

**What it does not cover.** A set whose first run was made by a build without
the claim has no claim, so a later run of that same `backup_id` by a new build
is not stopped. That is a window at upgrade (a Backup Job lost while the
controller is upgraded) and for a standalone `backup run` re-using a
`backup_id` an older build already wrote to; sets written before the fix may
therefore carry two receipts, and the catalog keeps both as two points (see
[`catalog-point.md`](catalog-point.md)).

`--receipt-out <path>` additionally writes the same bytes to `<path>` and the
DSSE sidecar to `<path>` with the extension replaced by `.sig` — the pairing
`drill run --out` already uses. `--out` is the same flag by another name;
naming two DIFFERENT paths is refused before anything runs, because this command
writes exactly one document.

## Regenerating the fixture

```
just fixtures-sign
```

`mint_backup_receipt_fixture` **reads** the pinned throwaway key at
`e2e/fixtures/signed/signing.pem` and never mints one — a fresh key would orphan
the fingerprint [`../verify-a-scorecard.md`](../verify-a-scorecard.md) teaches
auditors to pin. It writes the document and the signature over exactly those
bytes itself, in one process, after validating the document against all five
arms, so there is no window in which the tracked document and the tracked
signature over it disagree.

## Regenerating the schema

```
just schema
```

Regenerates both of tag 1's schemas from their Rust types. The CI drift arm
fails on any difference, so `just schema` is the only sanctioned way to change
`schemas/logweir-backup-receipt-1.0.0.json`.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
