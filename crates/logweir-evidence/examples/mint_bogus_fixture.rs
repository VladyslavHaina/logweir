// crates/logweir-evidence/examples/mint_bogus_fixture.rs
//!
//! Mints the DELIBERATELY BOGUS fixture pair under `e2e/fixtures/signed/`:
//! `scorecard-self-attested-bogus.json` and its `.sig`. Invoked by
//! `just fixtures-sign-bogus`, from the workspace root.
//!
//! The document's signature is GENUINE and its claim is FALSE. It says
//! `approval.self_attested: true` while `approval.key_id` is `"a"*64` — not
//! the key that signed it — so `drill verify` and `docs/verify_scorecard.py`
//! must both refuse it on the DERIVATION and never on the cryptography. It
//! exists to be refused; see `e2e/fixtures/signed/README.md`.
//!
//! Three hard differences from `mint_fixture.rs`, stated here so nobody
//! "unifies" the two:
//!
//! 1. It LOADS the pinned key with `SigningKey::from_pem_file` and never calls
//!    `generate_p256` or `load_or_generate`. An absent or malformed
//!    `signing.pem` must be a loud failure here, never a silent mint: a bogus
//!    fixture signed by a key nobody pinned would test nothing. It writes
//!    neither `public.pem` nor `signing.pem`.
//! 2. It is ADDITIVE. It writes EXACTLY TWO files, both of which are new, and
//!    re-mints nothing. Ruling R-G (`plan.md:60`) makes Task 2 the one and
//!    only re-mint of the five existing signed-fixture files; this program
//!    must never touch `scorecard.json`, `scorecard.sig`,
//!    `scorecard-self-attested.json`, `scorecard-self-attested.sig`,
//!    `public.pem` or `signing.pem`.
//! 3. It leaves `approval.key_id` alone. `mint_fixture.rs` sets the
//!    self-attested variant's `key_id` to the signing key's, which is what
//!    makes that variant genuinely self-attested. Here the point is the
//!    opposite: the claim and the key must DISAGREE.
//!
//! `validate_invariants()` still passes over the result, on purpose. The
//! approval claim is not a `logweir-core` invariant and this task does not
//! make it one — that layer has no key and cannot derive the finding. The
//! refusal lives in `crates/logweir/src/verify.rs` and its Python mirror.

use logweir_core::det_json::to_deterministic_json;
use logweir_core::scorecard::Scorecard;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::PAYLOAD_TYPE_SCORECARD;
use std::path::Path;

fn write_sidecar(path: &Path, sidecar: &logweir_evidence::Sidecar) {
    let mut json = serde_json::to_string_pretty(sidecar).expect("sidecar serialises");
    json.push('\n');
    std::fs::write(path, json).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

fn main() {
    let dir = Path::new("e2e/fixtures/signed");
    let source_path = dir.join("scorecard.json");

    let bytes = std::fs::read(&source_path).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}. Run via `just fixtures-sign-bogus`, from the \
             workspace root.",
            source_path.display()
        )
    });
    let mut sc: Scorecard = serde_json::from_slice(&bytes).expect("scorecard.json parses");

    // The CLAIM is flipped and `approval.key_id` is deliberately NOT touched:
    // it stays `"a"*64` (emit_fixture's approver key id), which is not the id
    // of the key below. That disagreement IS the fixture.
    sc.approval.self_attested = true;
    sc.validate_invariants().expect(
        "the bogus fixture must still satisfy every logweir-core invariant — \
                 its only defect is the approval claim, which is not an invariant",
    );

    // Loaded, never minted (see the module comment, difference 1).
    let signing_key_path = dir.join("signing.pem");
    let key = SigningKey::from_pem_file(&signing_key_path).unwrap_or_else(|e| {
        panic!(
            "cannot load the pinned signing key at {}: {e}. This program never mints \
             one: a bogus fixture is only useful when it is signed by the key the \
             tests pin.",
            signing_key_path.display()
        )
    });
    assert_ne!(
        sc.approval.key_id,
        key.key_id(),
        "this fixture is only bogus while the approval key id and the signing key id \
         DISAGREE; they now match, so it would verify cleanly and prove nothing"
    );

    // Sign the EXACT bytes that are written — never a re-serialisation of a
    // parsed value (spec §6 C3, verify-as-read).
    let bogus_bytes = to_deterministic_json(&sc).expect("the bogus scorecard serialises");
    std::fs::write(dir.join("scorecard-self-attested-bogus.json"), &bogus_bytes)
        .expect("write scorecard-self-attested-bogus.json");
    let sidecar =
        sign_detached(&key, PAYLOAD_TYPE_SCORECARD, &bogus_bytes).expect("sign the bogus fixture");
    write_sidecar(&dir.join("scorecard-self-attested-bogus.sig"), &sidecar);

    // Path and key ID only — never key material.
    eprintln!(
        "minted the bogus fixture pair under {} (signed by {}, claiming approval key {})",
        dir.display(),
        key.key_id(),
        sc.approval.key_id
    );
}
