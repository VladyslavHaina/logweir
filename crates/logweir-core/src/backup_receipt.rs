//! The backup receipt: what an auditor can check about a backup that
//! Logweir took, signed on its own.
//!
//! # Why a NEW DOCUMENT and not new scorecard fields
//!
//! Global Constraint 12 freezes the drill scorecard at `format_version 1.0.0`
//! with **21 top-level properties and 17 required ones**, and permits tag 1 to
//! add NESTED OPTIONAL fields only. The backup's evidence is not a nested
//! detail of a restore drill: the source cluster id read from the broker, the
//! rendered auth mode, the named topic set, the engine id/version/digest, the
//! `backup_id`, the manifest key and its sha256, the per-topic record counts
//! and the covered time range are top-level facts about a DIFFERENT
//! operation, on a different cluster, at a different time. Bolting them onto
//! the scorecard would have cost eight new top-level properties in the one
//! document GC12 exists to keep still — and would have left every scorecard
//! ever written claiming, by the shape of its own schema, to say something
//! about a backup it never observed.
//!
//! So this is its own media type
//! (`logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT`), its own schema
//! (`schemas/logweir-backup-receipt-1.0.0.json`), its own
//! `format_version: "1.0.0"` and its own five arms — four
//! self-contradiction invariants and one closed value set. Spec §7: "new
//! payload types, not new scorecard fields."
//!
//! # A backup that produces no verifiable evidence is a backup an auditor has
//! # to take Logweir's word for
//!
//! That is the whole reason this file exists. `logweir backup run` measures
//! all of the above and, before this document, could only print it. A printed
//! line is not evidence: nothing binds it to the archive it describes and
//! nothing stops it being retyped. A signed receipt is checkable by a third
//! party who has the public key and neither the cluster nor the bucket.
//!
//! # Global Constraint 1
//!
//! No I/O, no clock, no network. `validate_invariants` is a pure function of
//! the document; every timestamp here is a value the caller measured and
//! handed in.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The signed record of one `logweir backup run`.
///
/// Field order is the document's own serialisation order (`serde_json` is
/// built with `preserve_order`, so declaration order IS byte order through
/// `crate::det_json::to_deterministic_json`). Do not reorder without
/// regenerating `schemas/logweir-backup-receipt-1.0.0.json` and re-minting
/// `e2e/fixtures/signed/backup-receipt.json`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackupReceipt {
    /// Semver of THIS format — `1.0.0`, and independent of the scorecard's.
    ///
    /// The schema PINS the major with a pattern rather than leaving the field
    /// an unconstrained string, for the reason
    /// `crate::scorecard::Scorecard::format_version` records: without it a
    /// document reading `"9.9.9"` validated cleanly against the file named
    /// `logweir-backup-receipt-1.0.0.json`, so a schema-only validator — the
    /// one route that does not go through `validate_invariants` — accepted
    /// exactly the document GC12 exists to refuse. Any `1.x.y` is allowed,
    /// because a MINOR bump adds optional fields only and a 1.0.0 reader must
    /// still read it.
    #[schemars(regex(pattern = r"^1\.[0-9]+\.[0-9]+$"))]
    pub format_version: String,
    /// ULID of the run that produced this receipt. Also the object key stem
    /// in the evidence bucket, exactly as `Scorecard::run_id` is.
    pub run_id: String,
    /// The engine's own identifier for the archive this run wrote. Distinct
    /// from `run_id`: two runs can be asked to append to one backup set, and
    /// `archive.manifest_key` is keyed on THIS.
    pub backup_id: String,
    /// When the run was requested — the same clock reading
    /// `Scorecard::requested_at` carries.
    pub requested_at: DateTime<Utc>,
    /// When the engine subprocess started. Logweir-measured, never
    /// engine-reported (`crate::engine::BackupFacts`): `backup` has no
    /// `--format` and writes no report file.
    pub started_at: DateTime<Utc>,
    /// When the engine subprocess finished.
    pub finished_at: DateTime<Utc>,
    /// The engine's exit status, as `crate::engine::BackupFacts` measured it —
    /// NOT the `logweir` process's own exit code, which maps through Global
    /// Constraint 11. Invariant 2 below is the biconditional that makes this
    /// field mean something: a receipt for a failed backup names no manifest.
    pub exit_code: i32,
    /// Free text from `--triggered-by`. Deliberately **not** a metric label:
    /// unbounded cardinality, for the reason `Scorecard::triggered_by`
    /// records.
    pub triggered_by: String,
    pub source: ReceiptSource,
    pub engine: ReceiptEngine,
    pub archive: ReceiptArchive,
    /// Records captured, per topic. Invariant 3 requires exactly one entry
    /// per `source.topics` entry and no others: a receipt that counts a topic
    /// the run was never asked to back up, or omits one it was, is describing
    /// some other run.
    ///
    /// A `BTreeMap`, so the key order is the topic names' own order and two
    /// runs over the same topic set produce byte-identical bytes here.
    pub records: BTreeMap<String, u64>,
    pub covered: ReceiptCovered,
}

/// The SOURCE cluster, as measured — never as a spec claimed it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptSource {
    /// Read from the broker at phase −1, never from the spec. GC18(c)'s
    /// fourth rail records the source `cluster_id` and re-asserts it is not
    /// the restore target; this is where the recorded value is attested.
    pub cluster_id: String,
    pub bootstrap_servers: Vec<String>,
    pub auth: ReceiptAuth,
    /// The named topic allowlist the run was given. GC18(c) rail 1: a named
    /// set with no glob metacharacter (**G-GLOB**), so this list is the exact
    /// set of topics, not a pattern that a reader would have to re-expand
    /// against a cluster it cannot see.
    pub topics: Vec<String>,
}

/// How the source client was told to authenticate. **Never a password, and
/// no field that could hold one** — the render-side twin of
/// `crate::engine::AuthRender`, whose doc comment states the same rule for
/// the same reason: the secret reaches the engine through its own `${VAR}`
/// environment expansion and is never interpolated by us, so it can never be
/// interpolated into a document we then sign and publish.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptAuth {
    /// **A CLOSED SET OF TWO: `"plaintext"` or `"scramSha512"`.** Any other
    /// value is refused by BOTH readers (arm 5 below, and
    /// `docs/verify_scorecard.py::check_backup_receipt_invariants`'s mirror),
    /// so a receipt naming a third spelling is never signed and never
    /// verifies.
    ///
    /// These are `logweir_core::spec::AuthSpec`'s serde tag values, which is
    /// what makes ONE spelling possible at all: they are the strings an
    /// adopter writes in a spec, `KafkaCluster.spec.auth.mode`'s CRD enum
    /// byte for byte, the only two values `AuthSpec::mode_str()` can return,
    /// and — since Task 17 copies this field — what
    /// `Backup.status.auth.mode`'s CRD description promises. A `String` and
    /// not an enum on the wire because a reader must be able to REPORT a
    /// value it refuses; the closed set is enforced by the arm, where both
    /// readers can state it in the same words.
    pub mode: String,
    /// The SASL username, when there is one. `null` under `plaintext` —
    /// which is not the same as an empty username.
    #[serde(default)]
    pub username: Option<String>,
}

/// The pinned engine that took the backup. `digest` is why this block is
/// worth signing: GC7 pins by digest and never by tag, and a receipt that
/// named only a version would be satisfied by any binary claiming it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptEngine {
    /// `"oso-cli"`.
    pub id: String,
    /// `"v0.21.0"` — at or above the GC8 floor.
    pub version: String,
    /// `"sha256:…"`, from `third_party/kafka-backup-binary.digest`.
    pub digest: String,
}

/// What was written, and where. The two fields an auditor needs in order to
/// go and look: the manifest's key, and a digest over the exact manifest
/// bytes THIS RUN READ BACK (not over bytes Logweir remembers writing).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptArchive {
    /// Empty **if and only if** the backup did not exit 0 — invariant 2.
    pub manifest_key: String,
    /// `"sha256:<hex>"` over the manifest bytes read back after the run.
    pub manifest_sha256: String,
    /// The object-store prefix everything this run wrote lives under. GC6:
    /// Logweir writes only under its own `logweir/` prefix.
    pub prefix: String,
}

/// The time range the archive covers, in **EPOCH MILLISECONDS**, as a
/// HALF-OPEN interval: `from_ms` is inclusive, `to_ms` is EXCLUSIVE.
///
/// # The end is exclusive, and both documents now say so (I22)
///
/// `config/crd/backups.yaml` documents `Backup.status.windowCovered.toMs` as
/// the exclusive end, and Task 17 copies these two numbers into it. Task 5
/// shipped invariant 4 as `<=` — accepting `from_ms == to_ms` as an
/// "instantaneous window" — which made the receipt and the CRD two
/// descriptions of one range under rules that disagreed. Invariant 4 is now
/// strict, and `crates/logweir/src/backup/phase_run.rs` converts the
/// manifest's inclusive newest `end_timestamp` to an exclusive bound in the
/// one place that measures it.
///
/// # Not RFC 3339, and this is the interface, not a preference (I22)
///
/// `Backup.status.windowCovered{fromMs,toMs}` mirrors this shape as two
/// `int64`s, and a Kubernetes status subresource has no date-time type to
/// mirror a string into. Two representations of one window — a string here
/// and an integer there — would need a conversion nobody owns, and the first
/// disagreement between them would be invisible: both would still be
/// well-formed. So the receipt speaks the operator's units, and the operator
/// copies the numbers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptCovered {
    pub from_ms: i64,
    pub to_ms: i64,
}

/// Three dot-separated non-negative integers, or `None`.
///
/// Hand-parsed on purpose: Global Constraint 38 closes the workspace graph,
/// so no `semver` crate is added for eleven lines. Stricter than
/// `crate::scorecard`'s `major_version`, which reads the leading component
/// alone — `"1"` and `"1.2.3.4"` are semver-shaped enough for that reader and
/// are refused here, because invariant 1 claims the whole string parses.
fn parse_semver(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

impl BackupReceipt {
    /// The five arms of a signed receipt: the four claims it cannot
    /// contradict, and the one field whose value set is closed.
    ///
    /// Called before signing and by `logweir drill verify --payload-type
    /// backup-receipt`, so no signed receipt can carry a self-contradicting
    /// claim. `Err` is the exact message, and the messages are **not to be
    /// reworded**: `docs/verify_scorecard.py`'s mirrored block compares
    /// byte-for-byte against them, and `crates/logweir-core/tests/
    /// backup_receipt.rs` asserts each one in full —
    /// `backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message` over the four
    /// self-contradiction arms, `arm_5_refuses_an_auth_mode_outside_the_closed_two`
    /// over arm 5, and
    /// `validate_invariants_has_exactly_five_return_err_statements` over the
    /// total, so neither the four nor the five can go stale on its own.
    ///
    /// `Result<(), String>` rather than a `thiserror` newtype, because the
    /// interface this task publishes is the STRING: the comparison the two
    /// readers make is on the text, and a wrapper type would put a prefix in
    /// front of it that the Python half has no way to reproduce.
    ///
    /// # The arms, in order
    ///
    /// 1. `format_version` parses as semver and its major is `1`. Checked
    ///    FIRST, like `Scorecard::refuse_unreadable_major`, so a document
    ///    from a future major is refused before any other arm is evaluated
    ///    against fields that build may have redefined.
    /// 2. `exit_code == 0` **iff** `archive.manifest_key` is non-empty.
    /// 3. `records` covers exactly `source.topics`.
    /// 4. `covered.from_ms < covered.to_ms` STRICTLY — the end is EXCLUSIVE
    ///    (I22, and Task 5's review finding F3).
    /// 5. `source.auth.mode` is one of the two values this format defines.
    ///    The first arm that is not a self-contradiction check: the document
    ///    does not disagree with itself, it names a mechanism the format has
    ///    no spelling for. LAST on purpose — the four consistency arms are
    ///    what an auditor reads first, and a receipt that contradicts itself
    ///    should say so before it is told its auth mode is unknown.
    pub fn validate_invariants(&self) -> Result<(), String> {
        // ARM 1. GC12 for this document: a reader refuses a major it has
        // never seen rather than guessing at a shape.
        if parse_semver(&self.format_version).map(|(major, _, _)| major) != Some(1) {
            return Err(format!(
                "format_version {:?} is not a 1.x version this reader understands",
                self.format_version
            ));
        }
        // ARM 2. A biconditional, both directions, one message. A receipt for
        // a failed backup names no manifest, and a receipt naming a manifest
        // did not fail.
        //
        // TRIMMED-EMPTY COUNTS AS ABSENT (ruling R-A). `.is_empty()` alone
        // accepted `"   "` as "a manifest was named", which is the exact
        // class of defect R-A was raised over on the scorecard's
        // `partial_reason`: a whitespace-only string that one reader treats
        // as present and the other as blank. Naming no manifest and naming a
        // manifest made of spaces are the same claim, and this is the strict
        // side of it — the accepted set narrows and no receipt Logweir writes
        // is affected, because `BackupOutcome::manifest_key` is either the
        // engine's key or `""`.
        let named = !self.archive.manifest_key.trim().is_empty();
        if (self.exit_code == 0) != named {
            let rendered = if named {
                format!("{:?}", self.archive.manifest_key)
            } else {
                "absent".to_string()
            };
            return Err(format!(
                "exit_code {} and manifest_key {} disagree: a receipt names a manifest \
                 if and only if the backup exited 0",
                self.exit_code, rendered
            ));
        }
        // ARM 3. The counted set and the named set are the same set. A
        // receipt that counts a topic the run was never asked to back up, or
        // omits one it was, is describing some other run — and either way the
        // per-topic figures cannot be read against the topic list beside
        // them.
        //
        // Both sides are rendered as SORTED, DEDUPLICATED lists so the
        // message is deterministic: `records` is a BTreeMap (already sorted)
        // and `source.topics` is a Vec whose order is the spec's.
        let counted: std::collections::BTreeSet<&str> =
            self.records.keys().map(String::as_str).collect();
        let named_topics: std::collections::BTreeSet<&str> =
            self.source.topics.iter().map(String::as_str).collect();
        if counted != named_topics {
            return Err(format!(
                "records covers {} but the named topic set is {}",
                render_set(&counted),
                render_set(&named_topics)
            ));
        }
        // ARM 4. THE END IS EXCLUSIVE, so the window is HALF-OPEN and
        // `from_ms == to_ms` is not an "instantaneous window" — it is an empty
        // one, and an archive that captured a record cannot cover an empty
        // range.
        //
        // STRICT `<` SINCE TASK 5b (Task 5's review, F3). Task 5 shipped `<=`
        // and a test asserting `from_ms == to_ms` was legal, while
        // `config/crd/backups.yaml` already documented
        // `status.windowCovered.toMs` as the *exclusive* end of the same
        // window (I22) — two documents describing one range by rules that
        // disagree, with Task 17 about to copy the numbers from one into the
        // other. One of the two had to move; the CRD's is the shape an
        // operator reads and the half-open convention every range API in this
        // tree uses, so the receipt's is the one that moved.
        //
        // The measurement side moved with it: `crates/logweir/src/backup/
        // phase_run.rs` derives `to_ms` from the newest segment's INCLUSIVE
        // `end_timestamp` and adds one millisecond, so a single-record topic
        // yields `[t, t+1)` — a window that contains exactly that record —
        // rather than the empty `[t, t]` this arm now refuses. That
        // conversion is stated once, there, and nothing else in the tree
        // converts between the two conventions.
        if self.covered.from_ms >= self.covered.to_ms {
            return Err(format!(
                "covered.from_ms {} is not before covered.to_ms {}: the covered window's end is EXCLUSIVE, so an empty range covers no record",
                self.covered.from_ms, self.covered.to_ms
            ));
        }
        // ARM 5. THE AUTH MODE'S VALUE SET IS CLOSED, and this is where it is
        // closed (controller ruling, Task 5b fix round 1; Task 6's review
        // Ruling 3).
        //
        // Before this arm the field was an unconstrained `String` that no
        // reader looked at: a receipt carrying
        // `"auth": {"mode": "totally-made-up"}` verified 0/0 at BOTH readers,
        // which was proved by execution in Task 5b's review. That is worse
        // than a documentation defect, because `Backup.status.auth.mode` is
        // copied FROM here by Task 17 and its CRD description promises the
        // two values below — so an unrefused third spelling propagates into
        // the control plane as an attested claim.
        //
        // The two values are `logweir_core::spec::AuthSpec`'s serde tags, the
        // only strings `AuthSpec::mode_str()` returns, and the
        // `KafkaCluster` CRD's `auth.mode` enum byte for byte. There is
        // therefore no legitimate writer of a third value anywhere in this
        // tree, and `crates/logweir/src/backup/phase_run.rs::receipt_auth` —
        // the ONE site that fills this field on a real run — cannot drift
        // back to `scram-sha-512` without `logweir backup run` refusing its
        // own receipt at `persist_receipt`'s step 1, before it signs or puts
        // anything.
        //
        // The mode IS interpolated, unlike the scorecard's two `target.auth`
        // arms: every arm of THIS document already echoes an adopter-supplied
        // string (`format_version` in arm 1, `manifest_key` in arm 2), a
        // reader that refuses a value without naming it makes the refusal
        // unactionable, and `{:?}` is the same rendering
        // `docs/verify_scorecard.py::_rust_debug_str` reproduces.
        if !matches!(self.source.auth.mode.as_str(), "plaintext" | "scramSha512") {
            return Err(format!(
                "source.auth.mode {:?} is not one of the two values this format defines: \
                 \"plaintext\" or \"scramSha512\"",
                self.source.auth.mode
            ));
        }
        Ok(())
    }
}

/// `{a, b}` — a set rendered the way arm 3's message spells it.
fn render_set(set: &std::collections::BTreeSet<&str>) -> String {
    let inner: Vec<String> = set.iter().map(|t| format!("{t:?}")).collect();
    format!("{{{}}}", inner.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_semver_refuses_anything_that_is_not_three_integers() {
        assert_eq!(parse_semver("1.0.0"), Some((1, 0, 0)));
        assert_eq!(parse_semver("1.12.3"), Some((1, 12, 3)));
        assert_eq!(parse_semver("1"), None);
        assert_eq!(parse_semver("1.0"), None);
        assert_eq!(parse_semver("1.0.0.0"), None);
        assert_eq!(parse_semver("1.0.0-rc1"), None);
        assert_eq!(parse_semver("v1.0.0"), None);
        assert_eq!(parse_semver(""), None);
    }

    #[test]
    fn render_set_is_sorted_and_quoted() {
        let set: std::collections::BTreeSet<&str> = ["b", "a"].into_iter().collect();
        assert_eq!(render_set(&set), "{\"a\", \"b\"}");
        let empty: std::collections::BTreeSet<&str> = Default::default();
        assert_eq!(render_set(&empty), "{}");
    }
}
