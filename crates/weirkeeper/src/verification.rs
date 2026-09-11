//! The controller's own verification of the evidence the UI renders — and the
//! two green-badge rules, one per kind (interface **I21**).
//!
//! # The whole point in one sentence
//!
//! `weirkeeper` renders a verdict only when a verdict actually happened, which
//! is why [`VerificationVerdict::NotAttempted`] exists as a value distinct
//! from [`VerificationVerdict::Invalid`]: `Invalid` is a claim about the
//! DOCUMENT, and an adopter who declines to give the controller a read-only
//! bucket credential has not produced a bad document — they have produced no
//! answer at all, and the honest badge for that is the literal word
//! `unverified` beside the printed `logweir drill verify` command.
//!
//! # The credential is a FIFTH Secret and a DIFFERENT principal
//!
//! Spec §9, Global Constraint 6 ("a separate bucket and a separate
//! principal"). `logweir-evidence-ro` is projected into the controller's own
//! pod as environment (`config/manager/deployment.yaml`, `optional: true`), so
//! a cluster without it STARTS and every verification here reads
//! `NotAttempted`. It cannot write or delete in any bucket — the handle is
//! built once, in `main`, through the read-only constructor — and since
//! retention only reports (Task 19, guard **G-RET**), **no Logweir component
//! holds any delete capability against object storage in tag 1.**
//!
//! The controller reads that credential from **its own environment and never
//! through the Kubernetes API**: the `weirkeeper` ClusterRole grants no verb
//! on `secrets` (Task 21), and `tests/linkage.rs::the_controller_never_reads_a_secret`
//! keeps it that way.
//!
//! # The rule is TWO rules, one per kind
//!
//! Spec §8 amendment 4, critique C **H1**/**H2**. "Passed" is a different
//! field on each kind, so there is no one badge rule:
//!
//! * **`Backup` is green ⟺ `verification.result == Valid` AND
//!   `status.exitCode == 0`** — [`backup_badge`].
//! * **`Restore` is green ⟺ `verification.result == Valid` AND
//!   `status.outcome == pass`** — [`restore_badge`].
//!
//! **There is no `outcome` on the `Backup` path at all** (spec §3.2, C95):
//! `BackupStatus` carries `exitCode` and no `outcome`, [`VerificationResult`]
//! carries none, and `BackupReceipt` carries `exit_code` and none. Adding one
//! is explicitly not taken, which is what makes the mutant "use one badge rule
//! for both kinds by reading `status.outcome` on a `Backup`" die at assertion
//! time: the field is ALWAYS absent there, so a single rule renders every
//! `Backup` ungreen and
//! `a_backup_is_green_only_on_valid_and_exit_code_zero`'s first fixture fails.
//!
//! Either badge is labelled **"verified by weirkeeper at `<verifiedAt>`
//! against key `<matchedKeyId>`"** and never "verified in your browser";
//! anything else is the literal word [`UNVERIFIED`], never `pass`
//! (`design-operator.md:135-139`). The in-browser WASM verifier is cut from
//! tag 1, and rendering no browser-computed verdict is strictly more honest
//! than a green badge over a verification that did not happen there.
//!
//! # The roster must carry KEY MATERIAL, and that is why `signingKeys[]` exists
//!
//! Critique B **H6**, spec amendment 3. [`verify_evidence`]'s step 4 resolves a
//! signing key from `TrustRoster.spec.signingKeys[].spkiPem`. With a
//! `signingKeyIds: Vec<String>` shape there would be nothing to verify
//! against: every call would return `NotAttempted`,
//! `status.evidence.verification.result` could never be `Valid`, and **this
//! task's own exit criterion would be unsatisfiable.** The shape was fixed at
//! slot 5 (`crds::trust_roster`) rather than retrofitted here.
//!
//! # Interface I13 — `Store` is blocking, so nothing here is `async`
//!
//! Every [`Store`] method drives its own current-thread runtime, and `kube`
//! drives every reconciler ON a runtime, so a direct call COMPILES CLEANLY and
//! panics with *Cannot start a runtime from within a runtime* at the first
//! reconcile. [`verify_evidence`] is therefore a plain synchronous function —
//! the body of a `tokio::task::spawn_blocking` closure, never an `async fn` —
//! over the shared `Arc<Store>` built once at controller start (Task 19).
//! `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` names
//! this file in its `I13_FILES` and fails if a `Store` call appears in a
//! function body outside such a closure.
//!
//! # The signing-oracle residual, stated rather than implied
//!
//! Global Constraint 27, O1/O0 default **(a)**, accepted: Job CRUD in a
//! namespace holding `logweir-signing-key` is equivalent to holding that key,
//! so `weirkeeper` is a signing oracle. The hardened layout — the signing
//! Secret in a namespace where `weirkeeper` has no Job CRUD — is **documented,
//! not mandated** (`docs/kubernetes.md` §15), because mandating it would split
//! the single-namespace install the "a stranger applies one file" decision
//! rests on.

use std::fmt;

use chrono::{DateTime, Utc};
use logweir_core::ids::sha256_prefixed;
use logweir_store::{Store, StoreError};
use logweir_verify::{verify_detached, Sidecar, VerifyingKey};
use serde_json::{json, Value};

use crate::conditions::{
    merge_condition, CONDITION_VERIFIED, REASON_EXIT_CODE_NOT_ZERO, REASON_OUTCOME_NOT_PASS,
    REASON_VERIFICATION_INVALID, REASON_VERIFICATION_NOT_ATTEMPTED, REASON_VERIFIED,
};
use crate::crds::trust_roster::TrustRosterSpec;
use crate::crds::Condition;

/// The `detail` on a `NotAttempted` produced because the controller holds no
/// evidence credential at all.
///
/// A SENTENCE THAT NAMES THE WAY OUT. Spec §9's "documented switch": an
/// adopter who will not give the control plane bucket access is not broken,
/// they are running with verification display off, and the CLI command that
/// answers the same question is the one printed here.
pub const NO_CREDENTIAL_DETAIL: &str =
    "no evidence credential is configured; run the printed logweir drill verify command instead";

/// The `detail` on a `NotAttempted` produced by an empty
/// `TrustRoster.spec.signingKeys`.
///
/// **VERBATIM, AND IT NAMES ITSELF.** This is the sentence
/// `an_empty_signing_key_list_names_itself` asserts. A roster with no signing
/// key material is the one configuration under which no evidence in the
/// cluster can ever verify, and a silent `Invalid` would blame the documents
/// for it.
pub const NO_SIGNING_KEYS_DETAIL: &str = "the TrustRoster lists no signing key material; add the \
                                          runner's public key to spec.signingKeys";

/// The word every badge that is not green renders.
///
/// **NEVER `pass`**, and never "verified in your browser"
/// (`design-operator.md:135-139`). `pass` is a `Restore`'s own `outcome`
/// value; reusing it for "we did not check" is how a UI comes to show the
/// happy word over an unchecked document.
pub const UNVERIFIED: &str = "unverified";

/// What the controller made of one piece of signed evidence.
///
/// `Valid` and `Invalid` are claims about the DOCUMENT; `NotAttempted` is a
/// claim about the CONTROLLER. Keeping them apart is the whole reason this
/// enum has three variants rather than a `bool` — see this module's header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationVerdict {
    /// The bytes matched the recorded digest and a roster signing key verified
    /// the detached DSSE sidecar over them.
    Valid,
    /// A claim about the document: the digest did not match, or no roster
    /// signing key verified the sidecar. **Never** produced by a storage
    /// failure.
    Invalid,
    /// No verdict was reached, and the `detail` says why: no evidence
    /// credential, an unreadable or absent object, or a roster with no signing
    /// key material.
    NotAttempted,
}

impl VerificationVerdict {
    /// The wire spelling, as it lands on `status.evidence.verification.result`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "Valid",
            Self::Invalid => "Invalid",
            Self::NotAttempted => "NotAttempted",
        }
    }
}

impl fmt::Display for VerificationVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One verification, as the controller performed it.
///
/// THE SHAPE IS THE BRIEF'S, VERBATIM. `detail` carries the `Error`'s
/// `Display` on `Invalid` and the reason on `NotAttempted`, and is `None` on
/// `Valid` — there is nothing to explain about an answer that came out yes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationResult {
    /// `Valid`, `Invalid` or `NotAttempted`.
    pub result: VerificationVerdict,
    /// The `keyId` of the `TrustRoster` signing key that verified the
    /// signature. **The ROSTER's declared id**, not the sidecar's: the roster
    /// is what an operator edits and what `status.matchedKeyId` has to be
    /// greppable against.
    pub matched_key_id: Option<String>,
    /// The DSSE `payloadType` that was verified, echoed from the caller.
    pub payload_type: String,
    /// When this verification happened.
    pub verified_at: DateTime<Utc>,
    /// The `Error`'s `Display` on `Invalid`; the reason on `NotAttempted`.
    pub detail: Option<String>,
}

impl VerificationResult {
    /// A `NotAttempted` carrying `detail`.
    ///
    /// `pub` because the reasons a verification is not attempted are not all
    /// inside [`verify_evidence`]: an unreadable `TrustRoster` and a blocking
    /// task that panicked are both decided by [`verify_oracle`], and both are
    /// `NotAttempted` for exactly the reason this module's header gives.
    #[must_use]
    pub fn not_attempted(payload_type: &str, detail: impl Into<String>) -> Self {
        Self {
            result: VerificationVerdict::NotAttempted,
            matched_key_id: None,
            payload_type: payload_type.to_string(),
            verified_at: Utc::now(),
            detail: Some(detail.into()),
        }
    }

    /// An `Invalid` carrying `detail`.
    fn invalid(payload_type: &str, detail: impl Into<String>) -> Self {
        Self {
            result: VerificationVerdict::Invalid,
            matched_key_id: None,
            payload_type: payload_type.to_string(),
            verified_at: Utc::now(),
            detail: Some(detail.into()),
        }
    }

    /// A `Valid` naming the roster key that verified it.
    fn valid(payload_type: &str, matched_key_id: String) -> Self {
        Self {
            result: VerificationVerdict::Valid,
            matched_key_id: Some(matched_key_id),
            payload_type: payload_type.to_string(),
            verified_at: Utc::now(),
            detail: None,
        }
    }

    /// This result as the `status.evidence.verification` object, **reusing
    /// `existing`'s `verifiedAt` when nothing but the clock has moved.**
    ///
    /// # Why the timestamp is not simply `self.verified_at`
    ///
    /// Plan erratum **E11(d)**, and it is the difference between a quiet
    /// controller and 12,000 reconciles in 90 s. A reconciler's own status
    /// patch is what wakes it, so a field that carries a fresh clock read on
    /// every pass makes every pass a write and every write a wake-up. Task
    /// 16b's contract (`conditions::status_unchanged`,
    /// `patch_status_if_changed`) skips a patch that would change nothing —
    /// and it can only do that if a verdict that has not changed renders the
    /// same bytes.
    ///
    /// So `verifiedAt` is **when this verdict was reached**, not when it was
    /// last re-confirmed. A change in `result`, `matchedKeyId`, `payloadType`
    /// or `detail` is a new verdict and takes the new clock; anything else
    /// keeps the stored one.
    #[must_use]
    pub fn to_status_value(&self, existing: Option<&Value>) -> Value {
        let mut block = serde_json::Map::new();
        block.insert("result".into(), json!(self.result.as_str()));
        if let Some(id) = &self.matched_key_id {
            block.insert("matchedKeyId".into(), json!(id));
        }
        block.insert("payloadType".into(), json!(self.payload_type));
        if let Some(d) = &self.detail {
            block.insert("detail".into(), json!(d));
        }
        let same_substance = existing.is_some_and(|e| {
            ["result", "matchedKeyId", "payloadType", "detail"]
                .iter()
                .all(|k| e.get(*k) == block.get(*k))
        });
        let at = match (same_substance, existing.and_then(|e| e.get("verifiedAt"))) {
            (true, Some(stored)) => stored.clone(),
            _ => json!(self
                .verified_at
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        };
        block.insert("verifiedAt".into(), at);
        Value::Object(block)
    }
}

/// Read the object with the read-only evidence handle, check the digest the
/// status recorded, then verify the DSSE sidecar against the roster's signing
/// key material.
///
/// Returns `NotAttempted` (never `Invalid`) when no evidence credential is
/// configured.
///
/// # The four steps, in order, and why each verdict is the one it is
///
/// 1. **No `store`** → `NotAttempted` with [`NO_CREDENTIAL_DETAIL`]. The
///    controller was not given a credential; that is a fact about the install,
///    not about the document.
/// 2. **`get` the payload and the sidecar** through the read-only handle.
///    `StoreError::NotFound` and **every other** `StoreError` map to
///    `NotAttempted`, the latter carrying the error's `Display`. A storage
///    failure is never `Invalid`, because `Invalid` is a claim about the
///    document and an object that could not be fetched has made no claim.
/// 3. **Recompute `sha256_prefixed(payload)`** and compare it against the
///    digest the status recorded. A mismatch **is** `Invalid`, and its
///    `detail` names both digests: the bytes in the bucket are not the bytes
///    this run reported writing, which is precisely the substitution a digest
///    on the status exists to catch. Skipping this and verifying the signature
///    alone would accept a genuinely-signed OLDER document in place of this
///    one.
/// 4. **For each `roster.signing_keys` entry**, build
///    [`VerifyingKey::from_pem_str`] over its `spkiPem` and call
///    [`verify_detached`]. The FIRST `Ok` gives `Valid` carrying that entry's
///    own `keyId`; otherwise `Invalid` with the LAST error's `Display`. An
///    **empty** list is `NotAttempted` with [`NO_SIGNING_KEYS_DETAIL`] — it
///    names itself rather than failing silently, and returning `Invalid` there
///    would blame every document in the cluster for a missing line in one
///    cluster-scoped object.
///
/// # Interface I13
///
/// Synchronous on purpose: this is the body of a `spawn_blocking` closure. See
/// this module's header.
#[must_use]
pub fn verify_evidence(
    store: Option<&Store>,
    roster: &TrustRosterSpec,
    payload_key: &str,
    payload_sha256: &str,
    sidecar_key: &str,
    payload_type: &str,
) -> VerificationResult {
    // STEP 1. No credential is not a bad document.
    let Some(store) = store else {
        return VerificationResult::not_attempted(payload_type, NO_CREDENTIAL_DETAIL);
    };

    // STEP 2. Both objects, through the read-only handle. EVERY StoreError is
    // NotAttempted — `NotFound` and the rest alike.
    let payload = match store.get(payload_key) {
        Ok((bytes, _version)) => bytes,
        Err(e) => return VerificationResult::not_attempted(payload_type, store_detail(&e)),
    };
    let sidecar_bytes = match store.get(sidecar_key) {
        Ok((bytes, _version)) => bytes,
        Err(e) => return VerificationResult::not_attempted(payload_type, store_detail(&e)),
    };

    // STEP 3. The digest the status recorded, against the bytes in the bucket
    // right now. A MISMATCH IS `Invalid`.
    let computed = sha256_prefixed(&payload);
    if computed != payload_sha256 {
        return VerificationResult::invalid(
            payload_type,
            format!(
                "digest mismatch at {payload_key}: the status records {payload_sha256} and the \
                 bytes in the archive are {computed}"
            ),
        );
    }

    // The sidecar has to parse before any key can be tried against it. A
    // sidecar that is not JSON is a fact about the OBJECT, so this is
    // `Invalid`: the bytes were fetched, they are the evidence this run named,
    // and they are not a DSSE sidecar.
    let sidecar: Sidecar = match serde_json::from_slice(&sidecar_bytes) {
        Ok(s) => s,
        Err(e) => {
            return VerificationResult::invalid(
                payload_type,
                format!("{sidecar_key} is not a DSSE sidecar: {e}"),
            )
        }
    };

    // STEP 4. The roster's signing key material — interface I17. An EMPTY list
    // names itself and is never `Invalid`.
    if roster.signing_keys.is_empty() {
        return VerificationResult::not_attempted(payload_type, NO_SIGNING_KEYS_DETAIL);
    }
    let mut last_error: Option<String> = None;
    for entry in &roster.signing_keys {
        let key = match VerifyingKey::from_pem_str(&entry.spki_pem) {
            Ok(k) => k,
            Err(e) => {
                last_error = Some(format!("{}: {e}", entry.key_id));
                continue;
            }
        };
        match verify_detached(&key, payload_type, &payload, &sidecar) {
            // THE ROSTER'S OWN `keyId`, not the one `verify_detached` returned
            // out of the sidecar. They are the same string whenever the roster
            // declares its ids correctly, and when they are not, the id an
            // operator can act on is the one written in the object they edit.
            Ok(_sidecar_key_id) => {
                return VerificationResult::valid(payload_type, entry.key_id.clone())
            }
            Err(e) => last_error = Some(format!("{}: {e}", entry.key_id)),
        }
    }
    VerificationResult::invalid(
        payload_type,
        last_error.unwrap_or_else(|| {
            "no signing key on the TrustRoster verified this sidecar".to_string()
        }),
    )
}

/// A `StoreError` as the `detail` of a `NotAttempted`.
///
/// `NotFound` gets its own sentence because "the object is not there" and "the
/// bucket would not answer" are different things for an operator to act on,
/// and both are `NotAttempted`.
fn store_detail(e: &StoreError) -> String {
    match e {
        StoreError::NotFound(key) => {
            format!("the evidence object {key} is not in the archive; nothing was verified")
        }
        other => format!("the evidence object could not be read: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Interface I21 — the two green-badge rules, one per kind
// ---------------------------------------------------------------------------

/// What a UI renders for one object.
///
/// `green` is the badge; `reason` is the CamelCase `reason` of the `Verified`
/// condition that says the same thing on the object itself; `label` is the
/// sentence beside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Badge {
    /// Whether the badge is green.
    pub green: bool,
    /// The `Verified` condition's `reason` — [`REASON_VERIFIED`] when green,
    /// and one of [`REASON_VERIFICATION_INVALID`],
    /// [`REASON_VERIFICATION_NOT_ATTEMPTED`], [`REASON_EXIT_CODE_NOT_ZERO`] or
    /// [`REASON_OUTCOME_NOT_PASS`] otherwise.
    pub reason: &'static str,
    /// "verified by weirkeeper at `<verifiedAt>` against key `<matchedKeyId>`"
    /// when green, and the literal [`UNVERIFIED`] in every other case.
    pub label: String,
}

impl Badge {
    fn green(verified_at: &str, matched_key_id: &str) -> Self {
        Self {
            green: true,
            reason: REASON_VERIFIED,
            label: format!("verified by weirkeeper at {verified_at} against key {matched_key_id}"),
        }
    }

    fn not_green(reason: &'static str) -> Self {
        Self {
            green: false,
            reason,
            label: UNVERIFIED.to_string(),
        }
    }
}

/// The verification half of both rules: `Ok(())` when the block is a `Valid`
/// this controller could have written, `Err(reason)` otherwise.
///
/// A `Valid` with no `matchedKeyId` or no `verifiedAt` is not a verdict
/// [`verify_evidence`] ever produces — both are set on every `Valid` — so a
/// block shaped like that is treated as no verdict at all rather than as a
/// green badge whose label cannot be rendered.
fn valid_verification(status: &Value) -> Result<(&str, &str), &'static str> {
    let v = status.pointer("/evidence/verification");
    let result = v
        .and_then(|v| v.get("result"))
        .and_then(Value::as_str)
        .unwrap_or("");
    match result {
        "Valid" => {}
        "Invalid" => return Err(REASON_VERIFICATION_INVALID),
        _ => return Err(REASON_VERIFICATION_NOT_ATTEMPTED),
    }
    let at = v
        .and_then(|v| v.get("verifiedAt"))
        .and_then(Value::as_str)
        .ok_or(REASON_VERIFICATION_NOT_ATTEMPTED)?;
    let key = v
        .and_then(|v| v.get("matchedKeyId"))
        .and_then(Value::as_str)
        .ok_or(REASON_VERIFICATION_NOT_ATTEMPTED)?;
    Ok((at, key))
}

/// **Interface I21, the `Backup` half.** Green ⟺ `verification.result ==
/// Valid` AND `status.exitCode == 0`.
///
/// IT NEVER READS `outcome`, AND THERE IS NONE TO READ. `BackupStatus` carries
/// `exitCode` and no `outcome` (spec §3.2, C95). A rule that read `outcome`
/// here would find the field absent on every `Backup` ever written and render
/// every one of them ungreen — which is what
/// `a_backup_is_green_only_on_valid_and_exit_code_zero` fails on.
#[must_use]
pub fn backup_badge(status: &Value) -> Badge {
    let (at, key) = match valid_verification(status) {
        Ok(pair) => pair,
        Err(reason) => return Badge::not_green(reason),
    };
    if status.get("exitCode").and_then(Value::as_i64) != Some(0) {
        return Badge::not_green(REASON_EXIT_CODE_NOT_ZERO);
    }
    Badge::green(at, key)
}

/// **Interface I21, the `Restore` half.** Green ⟺ `verification.result ==
/// Valid` AND `status.outcome == pass`.
///
/// `outcome` is the scorecard's own string, copied onto the status by Task
/// 20's reconciler and never re-derived — `pass`, `fail-objective`,
/// `fail-integrity` or `preflight-failed` in the frozen 1.0.0 enum. Only
/// `pass` is green: a `fail-integrity` run produced a perfectly valid signed
/// document SAYING THE RESTORE DID NOT RECONCILE, and a green badge over it
/// would invert the most valuable thing the tool reports.
#[must_use]
pub fn restore_badge(status: &Value) -> Badge {
    let (at, key) = match valid_verification(status) {
        Ok(pair) => pair,
        Err(reason) => return Badge::not_green(reason),
    };
    if status.get("outcome").and_then(Value::as_str) != Some("pass") {
        return Badge::not_green(REASON_OUTCOME_NOT_PASS);
    }
    Badge::green(at, key)
}

// ---------------------------------------------------------------------------
// The SECOND patch
// ---------------------------------------------------------------------------

/// The `Verified` condition for `badge`, with the `metav1.Condition`
/// `lastTransitionTime` contract applied.
///
/// `existing` is the object's current `Verified` condition, so a badge that
/// has not changed keeps the instant it changed at (Task 16b,
/// `conditions::merge_condition`).
#[must_use]
pub fn verified_condition(
    badge: &Badge,
    existing: Option<&Condition>,
    observed_generation: Option<i64>,
    now: DateTime<Utc>,
) -> Condition {
    merge_condition(
        existing,
        Condition {
            r#type: CONDITION_VERIFIED.to_string(),
            status: if badge.green { "True" } else { "False" }.to_string(),
            observed_generation,
            last_transition_time: Some(now),
            reason: Some(badge.reason.to_string()),
            message: Some(badge.label.clone()),
        },
    )
}

/// The SECOND `/status` merge patch: `status.evidence.verification` plus the
/// full condition list with `Verified` merged into it.
///
/// # Why it is a second, separate patch and not one more key on the first
///
/// So a verification failure can never prevent the exit code from being
/// recorded. The terminal patch lands first and on its own; this one follows.
/// A 500 on this patch leaves the exit code, the evidence keys and the
/// scorecard-derived fields exactly where the first patch put them — which is
/// what `verification_is_a_second_patch_after_the_status_patch` asserts by
/// answering the second route with a 500 and reading the object back.
/// Folding the two together makes the mutant's own failure mode the proof: the
/// 500 then loses the exit code too.
///
/// # Why it carries the WHOLE condition list
///
/// A JSON merge patch REPLACES arrays (RFC 7386;
/// `conditions::apply_merge_patch` is the API server's half written out). A
/// second patch carrying only `[Verified]` would therefore DELETE the terminal
/// `Complete`/`Failed` condition the first patch just wrote. `terminal` is the
/// condition list that first patch carried — taken from the patch value
/// itself, because the in-memory object this reconcile holds was read BEFORE
/// that patch and no longer says what the object says.
#[must_use]
pub fn second_patch(terminal: &[Condition], verified: Condition, verification: Value) -> Value {
    let mut conditions: Vec<Condition> = terminal
        .iter()
        .filter(|c| c.r#type != verified.r#type)
        .cloned()
        .collect();
    conditions.push(verified);
    json!({
        "status": {
            "evidence": { "verification": verification },
            "conditions": conditions,
        }
    })
}

/// The condition list a terminal `/status` merge patch carried, as
/// [`Condition`]s.
///
/// Reads the patch VALUE rather than the object, for the reason
/// [`second_patch`] documents: after the first PATCH the in-memory object is
/// stale, and the only exact record of what was written is the thing that was
/// written. An empty list is the truthful answer for a patch that carried no
/// `conditions` key at all.
#[must_use]
pub fn conditions_in(patch: &Value) -> Vec<Condition> {
    patch
        .pointer("/status/conditions")
        .and_then(|c| serde_json::from_value::<Vec<Condition>>(c.clone()).ok())
        .unwrap_or_default()
}

/// The `status.evidence.verification` block currently on the object, for
/// [`VerificationResult::to_status_value`]'s timestamp rule.
#[must_use]
pub fn stored_verification(status: Option<&Value>) -> Option<&Value> {
    status?.pointer("/evidence/verification")
}

// ---------------------------------------------------------------------------
// The oracle — interface I13's shape, as the two reconcilers take it
// ---------------------------------------------------------------------------

/// What one verification names: the two objects, the digest the status
/// recorded, and the media type that binds them.
///
/// BY VALUE, because the future outlives the call: everything the
/// `spawn_blocking` closure reads has to be owned by it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceRef {
    /// The object key of the signed payload — a backup receipt or a scorecard.
    pub payload_key: String,
    /// `sha256:<hex>` as the status recorded it, from the bytes the run wrote.
    pub payload_sha256: String,
    /// The object key of the detached DSSE sidecar.
    pub sidecar_key: String,
    /// `logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT` (interface **I3**) or
    /// `PAYLOAD_TYPE_SCORECARD` — never a string a reconcile arm invented.
    pub payload_type: &'static str,
}

/// The verification half of a reconciler, injected.
///
/// # Why an oracle and not a `Store` on the reconcile
///
/// The same argument `controllers::backup::ArchiveOracle` makes, for the same
/// interface. `Store` is blocking (**I13**) and `kube` drives reconcilers on a
/// runtime, so the real implementation is a `spawn_blocking` closure over the
/// shared `Arc<Store>`; writing the reconcile against an injected oracle is
/// what lets `verification_is_a_second_patch_after_the_status_patch` assert the
/// route ORDER over `mock_client` with no bucket anywhere.
///
/// `BoxFuture<'static, …>` and not `BoxFuture<'a, …>`: an `Fn`'s `Output` is an
/// associated type matched exactly, so a future borrowed for `'a` makes `'a`
/// the lifetime of both the future and the `&'a dyn`, and the borrow checker
/// then asks for a `'static` local. The future owns everything it reads.
pub type VerifyOracle<'a> = &'a (dyn Fn(EvidenceRef) -> futures::future::BoxFuture<'static, VerificationResult>
         + Send
         + Sync
         + 'a);

/// The oracle for a controller that holds no evidence credential: it attempts
/// nothing and says so.
///
/// This is what both reconcilers use when `Context::archive` is `None` — a
/// controller started without `logweir-evidence-ro` — and what every unit test
/// that is not about verification passes. The verdict is `NotAttempted` with
/// [`NO_CREDENTIAL_DETAIL`], **never** `Invalid`: see this module's header.
#[must_use]
pub fn unverified_evidence(
    r: EvidenceRef,
) -> futures::future::BoxFuture<'static, VerificationResult> {
    Box::pin(async move { VerificationResult::not_attempted(r.payload_type, NO_CREDENTIAL_DETAIL) })
}

/// The `detail` on a `NotAttempted` produced because the cluster-scoped
/// `TrustRoster` could not be read at all.
///
/// DISTINCT FROM [`NO_SIGNING_KEYS_DETAIL`], which is about a roster that IS
/// readable and lists nothing. An API failure is not an empty list, and an
/// operator who is told to "add the runner's public key" when the real problem
/// is a 503 will edit an object that was already correct.
pub const ROSTER_UNREADABLE_DETAIL: &str =
    "the cluster-scoped TrustRoster `default` could not be read; nothing was verified";

/// The real verification oracle: the controller's read-only evidence handle,
/// the cluster-scoped `TrustRoster`, and [`verify_evidence`] on a blocking
/// thread.
///
/// # Interface I13, and it is not decoration
///
/// `Store::get` drives its own current-thread runtime and `kube` drives every
/// reconciler ON a runtime, so calling [`verify_evidence`] straight from the
/// `async move` block below COMPILES CLEANLY and panics with *Cannot start a
/// runtime from within a runtime* at the first verification.
/// `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` names
/// `verify_evidence(` in its `STORE_CALL_TOKENS` for exactly that reason: this
/// call site holds a `Store::get` while naming no `Store` at all, which is the
/// same blindness the Task 20 review found in `observe_archive(` and
/// `observe_scorecard(`.
///
/// # THREE WAYS TO REACH `NotAttempted` BEFORE THE DOCUMENT IS EVEN FETCHED
///
/// No archive handle, an unreadable roster, and a blocking task that did not
/// come back. **None of them is `Invalid`**: each is a fact about this
/// controller, and `Invalid` is a claim about the document.
pub fn verify_oracle(
    archive: Option<std::sync::Arc<Store>>,
    client: kube::Client,
) -> impl Fn(EvidenceRef) -> futures::future::BoxFuture<'static, VerificationResult>
       + Send
       + Sync
       + 'static {
    move |r: EvidenceRef| {
        let handle = archive.clone();
        let client = client.clone();
        Box::pin(async move {
            let Some(handle) = handle else {
                return unverified_evidence(r).await;
            };
            let roster = match crate::controllers::roster_spec(&client).await {
                Ok(spec) => spec,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        roster = crate::ROSTER_NAME,
                        "the TrustRoster could not be read; this verification is NotAttempted \
                         rather than Invalid"
                    );
                    return VerificationResult::not_attempted(
                        r.payload_type,
                        ROSTER_UNREADABLE_DETAIL,
                    );
                }
            };
            let payload_type = r.payload_type;
            // ONE `spawn_blocking`, TWO `get`s — interface I13.
            match tokio::task::spawn_blocking(move || {
                verify_evidence(
                    Some(&handle),
                    &roster,
                    &r.payload_key,
                    &r.payload_sha256,
                    &r.sidecar_key,
                    r.payload_type,
                )
            })
            .await
            {
                Ok(result) => result,
                Err(e) => VerificationResult::not_attempted(
                    payload_type,
                    format!("the verification task did not complete: {e}"),
                ),
            }
        })
    }
}

/// `conditions` with the object's existing `Verified` condition carried
/// forward — **the fix for a hot loop, measured on a live cluster.**
///
/// # What went wrong, in one sentence
///
/// A JSON merge patch REPLACES arrays (RFC 7386), so a terminal `/status`
/// patch writing `conditions: [Complete, EvidenceRecorded]` DELETES the
/// `Verified` condition [`second_patch`] added on the previous pass — the
/// second patch then re-adds it, the write wakes the reconciler, and the two
/// patches fight forever.
///
/// **MEASURED** on the Phase B run that otherwise passed: 20 `Backup`
/// reconciles and 20 `Restore` reconciles **per second**, each one a real
/// write, in a controller whose whole condition contract (Task 16b, plan
/// erratum **E11(d)**) exists to make a steady object issue zero patches. The
/// verdict was correct on every pass; the object never stopped being rewritten.
///
/// So every terminal patch builder calls this. `Verified` is not the run's
/// fact — it is the CONTROLLER's fact about the run, computed after the
/// terminal write — and a builder that owns the array has to carry the parts
/// of it that are not its own. `a_verified_object_reconciles_without_a_patch`
/// is the regression, and it asserts the count is ZERO rather than small.
#[must_use]
pub fn carry_verified(existing: Option<&Vec<Condition>>, mut conditions: Vec<Value>) -> Vec<Value> {
    // `Vec<Value>` AND NOT `Vec<Condition>`, because the patch builders build
    // their arrays as `json!(merge_condition(…))` and a round trip through the
    // struct here would re-serialise every element — turning a comparison that
    // is currently byte-for-byte into one that depends on two serialisations
    // agreeing. See `conditions::status_unchanged`'s note on why every element
    // this crate writes comes from `Condition` and `serde` in one step.
    if conditions
        .iter()
        .any(|c| c.get("type") == Some(&json!(CONDITION_VERIFIED)))
    {
        return conditions;
    }
    if let Some(v) = crate::conditions::current_condition(existing, CONDITION_VERIFIED) {
        conditions.push(json!(v));
    }
    conditions
}
