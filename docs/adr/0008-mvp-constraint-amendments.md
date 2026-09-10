# ADR 0008: the MVP tag-1 constraint amendments (spec §3.1 A–D)

## Status

Accepted, 2026-09-09, as **amendments**. This is the O2 ADR the MVP tag-1 plan
(`docs/mvp/2026-09-09-mvp-tag1-plan.md`) requires: the record of every place
where tag 1 CHANGES a decision an earlier document made, written down as a
change rather than smuggled in as a reading of it.

## Context

Four constraints of spec §3.1 are revised by tag 1. Three of them are recorded
here because the earlier text says the opposite of what tag 1 does, and one is
recorded here because a reader would otherwise assume it had been revised and
it has not. An amendment that is only implied is an amendment nobody can review;
each section below therefore carries **the replacement text itself**, not a
description of it.

## Amendment A — the kind list

`docs/ROADMAP.md:294`'s kind list is replaced. The kinds are `KafkaCluster`,
`BackupSchedule`, `Backup`, `Restore`, `Approval` and `TrustRoster`, plus
`Switchover` (tag 2) and, reserved, `MetadataSnapshot`. `RestoreDrill` is
**retired** in favour of `Restore`. No kind may name a cluster, a fleet, a topic,
a connector or a backup. A new kind requires an ADR.

This is Global Constraint 34. Task 1 lands the `docs/ROADMAP.md` marker; the
kinds are implemented in code by Task 15b.

## Amendment B — GC14 is NOT amended

`gc.txt:18` (GC14) needs **no** amendment, and this section exists so that the
absence is on the record rather than inferred. Its naming exclusions stand
verbatim — never `kafkabackup.com`, never `oso.sh`, never the API groups
`kafka.oso.sh` or `kafkabackup.com`, never the `osodevops/` Docker Hub namespace
for Logweir's own artefacts, never OSO's crates.io names — its API group
`logweir.dev/v1alpha1` stands verbatim, and its ASF footer sentence stands
verbatim. The **only** change is that GC14's "deferred to SP5" clause is
**lifted**: `logweir.dev/v1alpha1` is Logweir's now, not later.

GC14 contains no kind list. The kind list is Amendment A, above, and Global
Constraint 34.

## Amendment C — Phase 5's adopter gate is overridden

`docs/ROADMAP.md:292`'s gate on Phase 5 — "Only if adopters ask by name.
Nothing here is scheduled; each item is unlocked by a request" — is
**overridden by owner decision**. The operator, the CRDs and the static UI are
scheduled work in tag 1 and are not waiting on an adopter request.

This is Global Constraint 26. It is recorded here, in writing, rather than
implied by the existence of the tasks that build them: a gate that is silently
walked past is a gate that was never really there.

## Amendment D — four runtime engine subcommands, not three

Global Constraint 3 (`gc.txt:7`) and `docs/adr/0002-shell-out.md`'s Decision
(`:21-25`) both now read:

> the `logweir` binary invokes **exactly four** engine subcommands at runtime:
> `backup`, `restore`, `validate-restore`, `validation run`.

`list`, `restore-status`, `offset`, `evidence-verify` and
`validation evidence-verify` stay denied.

**Why this is a change and not a reading.** ADR 0002's original Decision fixed
the count at "exactly three subcommands reachable from shipped code" and
contains **no `--from-cluster` clause anywhere in its 79 lines**. The sentence
"`backup` belongs to `--from-cluster`, which is deferred" is GC3's own, and it
**defers** the path rather than pre-authorising it. Meanwhile GC18 funds the
`--from-cluster` execution path, which renders a `backup.yaml` and runs it — so
GC3 and GC18 contradicted each other until this amendment resolved them. Spec §5
("Logweir drives the engine's `backup` command itself") and spec §16 item 10 are
the authority, and ADR 0007 (`docs/adr/0007-from-cluster-in-v0.1.md`) had already
revoked the `--from-cluster` deferral; this amendment is the count-side
consequence of that revocation.

**How it is enforced.** `scripts/check-no-oso.sh` carries two allowlists that are
deliberately not the same list:

- `ENGINE_RUNTIME_ALLOWLIST` — the contract: the four subcommand tokens above.
  This is what a refusal message quotes.
- `ENGINE_ARGV_ALLOWLIST` — what an argv array may legally contain: the same
  four plus `run` (the second word of the two-word `validation run`) and the
  argv furniture `--config`, `--format`, `json`.

The gate also changed **shape**, not just membership. Its primary check now
matches an **invocation**: a double-quoted literal that is an argv token and
that sits inside the balanced expression of `run_engine(` or of a direct
`Command::new` spawn of the engine binary. The old whole-file token grep is kept
as a secondary, minus `backup`, and its escape is now
`// engine-token-ok: <reason>` with a reason of at least ten characters — a bare
marker is not an escape. Neither check subsumes the other: the secondary owns
the two-word forms and tokens built outside an invocation, and its escape does
**not** excuse the primary.

## Amendment E — two crates extracted, on opposite sides of the pure layer

Tag 1 splits two crates out of existing ones. They are recorded together
because the pair is the point: one lands **outside** the pure layer and one
**inside** it, and each placement is a decision taken here rather than a
consequence of where the code happened to sit.

**`crates/logweir-store` — the object-store half of `logweir-engine-oso`,
outside the pure layer.** The whole of `logweir-engine-oso/src/storage.rs`
becomes `logweir-store/src/lib.rs`, and `logweir-engine-oso/src/lib.rs` reads
`pub use logweir_store as storage;` where it read `pub mod storage;`, so every
existing `…::storage::Store` call site resolves unchanged. **The reason:**
three tag-1 features need an object-store handle in a component that must not
link `logweir-engine-oso` — `weirkeeper` verifies evidence it fetches with its
own read-only credential (spec §8), lists manifests to compute a retention
report (spec §5, guard G-RET) and reads a manifest's covered window for a
schedule's status — and `logweir-engine-oso` exists to shell out to the OSO
engine, carrying `subprocess.rs`, `vendored/` and the engine binary path, all
of which linking it would drag into the control plane for the sake of
`object_store`. `02-k8s-transition-plan.md:963` already named the extraction;
this is it.

`logweir-store` is **deliberately outside the pure layer**, by this amendment
and not by any edit to a check. It takes `object_store` at Global Constraint 9's
`["aws","azure","gcp","http"]` feature set, which pulls `aws-*`, so it can never
satisfy the pure layer's zero-`aws-*` grep. Global Constraint 1 already names
`crates/weirkeeper` and `crates/logweir-store` as outside the pure layer "by
ADR, never by a quiet edit to the grep";
`.github/workflows/no-oso.yml`'s forbidden-dependency loop is therefore **not**
extended to it, and carries a comment saying so with a pointer here, so a
future reader does not mistake the omission for an oversight. What the crate
does depend on besides `object_store` is `logweir-core`, which is pure.

Both constraints that lived in the moved file move with it and are unchanged:
Global Constraint 6's `LOGWEIR_ROOT` is still the literal `"logweir/"` fixed in
code, guarded in `from_url` and asserted in `put_create_only`, and a handle
from `read_only_from_url` still physically cannot put — the `ReadOnly` check
runs before the `LOGWEIR_ROOT` assertion and before anything else. The one
piece of genuinely new code is `Store::manifest_facts`, returning
`ManifestFacts { backup_id, newest_record_ms, oldest_record_ms }`: the
retention reconciler needs a backup set's covered window, `list_manifests`
returns `BackupSetRef`, which carries no timestamp at all, and the only
structure that does carry one is produced by `OsoCliEngine::describe` — in the
crate this extraction exists to keep out of the control plane.

**`crates/logweir-verify` — the verifying half of `logweir-evidence`, INSIDE
the pure layer.** `pae.rs` and `verify.rs` move whole; from `keys.rs`, the
`VerifyingKey` enum and its **entire inherent `impl` block** —
`from_pem_file`, `key_id` **and `to_public_key_pem`**; from `lib.rs`, the three
payload-type constants, `Signature`, `Sidecar` and `Error`.
`logweir-evidence/src/lib.rs` gains `pub use logweir_verify::*;` and its
`keys.rs`, `pae.rs` and `verify.rs` become re-export shims, so every existing
`logweir_evidence::…` call site — including the `VerifyingKey::P256(_)`
patterns in `crates/logweir/tests/fixtures/mod.rs` — compiles unchanged.
`VerifyingKey::from_pem_str` is **added**: the same parse without the
`std::fs::read_to_string`, because `weirkeeper` reads a `TrustRoster` entry's
`spkiPem` out of an API object and has no file to hand.

**The reason:** spec §8 requires `weirkeeper` to perform the DSSE checks the UI
renders, and `scripts/check-one-signer.sh:46-49` had already ruled the remedy
in writing — "`weirkeeper`… does not go on the allowlist — the ruled remedy is
to extract a verify-only crate, so that a controller which VERIFIES a signature
does not thereby link the signer." **A feature flag cannot express this split.**
`logweir-evidence` had `[features] default = []` with `p256`, `ed25519-dalek`
and `rand_core` all non-optional, so `default-features = false` removed
nothing; and verification needs **both** primitives anyway, because
`VerifyingKey` is an enum over them and `verify_detached` matches both arms.
The one thing the verifying half does not need is
`rand_core = { version = "0.6", features = ["getrandom"] }` — the signer's
entropy source — and a crate that never declares that line is the whole content
of the split.

**`KeyAlg` stayed with the signer.** It is `SigningKey::alg`'s return type and
`impl VerifyingKey` never names it.

**Which crate keeps which dependency.** `logweir-verify` declares `base64`,
`ed25519-dalek` (`pkcs8` only, **no `rand_core` feature**), `hex`, `p256`
(`ecdsa`, `pkcs8`, `pem`), `serde`, `serde_json`, `sha2` and `thiserror`.
`logweir-evidence` keeps `base64`, `ed25519-dalek` (with `rand_core`), `p256`,
`rand_core` and `serde_json`, gains `logweir-verify`, and **drops** its now-dead
direct `hex`, `sha2`, `serde` and `thiserror` edges — those were
`VerifyingKey::key_id`'s, `Sidecar`/`Signature`'s derive and `Error`'s derive,
and all four moved with the code that used them. The resolved package count
goes 342 → **343**: one new workspace member and no new third-party package
(Global Constraint 38).

**What the transitive tree does NOT show, stated here so nobody re-derives it
as a defect.** `logweir-verify`'s resolved tree still contains `rand_core` and
`getrandom`: `p256` requires `elliptic-curve`, which declares `rand_core`
non-optionally, and `signature`'s own `rand_core` feature brings `getrandom`.
Measured on this tree, `cargo tree -p logweir-verify -e normal --prefix none |
grep -c '^rand_core '` is **5** against `logweir-evidence`'s **7**. The claim
this extraction makes is therefore about the **declared** manifest edge, which
is what `crates/logweir-verify/tests/deps.rs` asserts, in both directions at
once; a "zero transitive `rand_core`" claim would be false, and is not made
anywhere.

`logweir-verify` is **inside the pure layer** — it builds with
`--no-default-features` and carries no `aws-*`, `rusoto`, `kube`,
`k8s-openapi` or `kafka-backup` dependency — so
`.github/workflows/no-oso.yml`'s build list **and** its forbidden-dependency
loop are BOTH extended to it, in the same commit, making four crates in each.
That is the asymmetry this section exists to record: `logweir-store` is added
to neither, `logweir-verify` to both.

### G-SIGN's check 2 is re-scoped, and this is the written reason

`scripts/check-one-signer.sh`'s check 2 does not walk to `logweir-evidence`; it
walks to the **primitive crates**, `p256` and `ed25519-dalek`, over
`cargo tree --workspace --invert`, and fails for every workspace member on
those inverted trees that is not on `ALLOWED_PRIMITIVE`. Because
`logweir-verify` must depend on both primitives, the inverted tree gains it and
check 2 **cannot pass unamended**; at the end of Task 15 it also gains
`weirkeeper`.

Guard **G-SIGN** is `[EDIT-DERIVED]`, so STANDING RULE 21 binds: an implementer
must not quietly add two names to an allowlist, because that would retire the
check for `weirkeeper` for good. The re-scope is therefore **recorded**, in
three places that must agree — the script's own header, this section, and spec
§10's **G-SIGN** row (which carries amendment 2's text at v3.1). The narrowed
claim check 2 now makes is:

> These four crates and no others reach the primitive crates. The crates that
> reach the SIGNING half are checks 1 and 3.

`ALLOWED_PRIMITIVE` becomes `"logweir-evidence logweir logweir-verify
weirkeeper"` — four names, in that order, and no fifth. Check 2's heading and
its `ok:` line are rewritten to state that claim and to point at checks 1 and 3
for the signing half. **Checks 1 and 3 are byte-identical** to `fdc73a5`:
`ALLOWED_LINK="logweir e2e"` and
`ALLOWED_SOURCE="logweir-evidence logweir e2e"`, with `weirkeeper` absent from
both. A **fourth**, separate allowlist,
`ALLOWED_VERIFY_LINK="logweir-evidence logweir weirkeeper e2e"`, governs who may
link the verifying crate; it does not weaken checks 1 and 3, and check 4 is a
one-directional subset check because it names `weirkeeper` a task before
`weirkeeper` exists.

`crates/logweir/tests/one_signer_gate.rs::check_two_states_the_narrowed_claim`
fails if the header paragraph goes missing or a fifth name appears, and
`the_two_original_allowlists_are_unchanged` fails on the byte comparison of the
two original allowlist lines.

### G-SIGN's second half — the corpus grep

`scripts/check-withdrawn-claim.sh` walks every shipped surface — the five root
documents, `docs/`, `config/`, `examples/`, `ui/`, `.github/workflows/`,
`scripts/`, and every `*.rs` under `crates/` — for a fixed, case-insensitive
list of six phrases, and fails naming the file and line. It exempts exactly two
paths, as literals and never as a pattern:
`scripts/check-one-signer.sh` and `scripts/check-withdrawn-claim.sh`, which
exist to *forbid* the claim and therefore have to quote it. `scripts/` is walked
for that reason: a scan that skipped it would make the exemption decorative.

It exists because the stronger claim has already been made once, was red on the
tree it was asserted about, and was withdrawn;
`scripts/check-one-signer.sh:10-19` forbids restating it, in those words, and
binds "this script's output, its comments, or the CI step that runs it" by name.
That last clause is why this task also reworded `.github/workflows/ci.yml`'s
G2′ comment, which quoted the withdrawn sentence in order to deny it — a
negation is still a quotation to a fixed-string grep, and the gate reported it.
The residual is real and accepted (**O1**/O0 default (a)): `weirkeeper` has Job
CRUD in the runner namespace, so it can create a pod that mounts the signing key
and sign anything. "No `get` on Secrets" bounds *reads*, not *capability*
(Global Constraint 27). Nothing in this repository may say otherwise, on any
surface, and this gate is what keeps that true.

## Consequences

- Every later task that needs to cite an amendment cites **this file**, by
  section letter, rather than restating the reasoning.
- `docs/adr/0002-shell-out.md` carries its own dated **Amended** paragraph
  pointing here; `.superpowers/sdd/2026-09-05-stage2-tier0-phase1/gc.txt` item 3
  and `docs/ROADMAP.md` lines 292 and 294 carry `[REVISED …]` / `[AMENDED …]`
  markers naming this file. A constraint revised in one place and not the others
  is the failure this ADR exists to prevent.
- The retired kind is named exactly once in this document, in Amendment A, and
  `crates/logweir/tests/engine_allowlist.rs` asserts that count — so a later
  edit that reintroduces it by name has to argue with a test.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
