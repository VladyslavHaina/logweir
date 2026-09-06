// crates/logweir-evidence/examples/mint_fixture.rs
//!
//! Mints the checked-in signed test fixtures under `e2e/fixtures/signed/`.
//! Invoked by `just fixtures-sign`, which first regenerates
//! `e2e/fixtures/signed/scorecard.json` via `logweir-core`'s `emit_fixture`
//! example (Task 3), then runs this binary from the workspace root.
//!
//! This program does NOT normally produce a key: it READS the checked-in
//! `e2e/fixtures/signed/signing.pem` (itself checked in per the `.gitignore`
//! un-ignore) and mints one only in a tree where that file is absent. Either
//! way the key is a THROWAWAY TEST FIXTURE — see
//! `e2e/fixtures/signed/README.md` — and the private half is NEVER printed to
//! stdout/stderr.

use logweir_core::det_json::to_deterministic_json;
use logweir_core::scorecard::Scorecard;
use logweir_evidence::keys::{KeyOrigin, SigningKey};
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
    let scorecard_path = dir.join("scorecard.json");

    let bytes = std::fs::read(&scorecard_path).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}. Run via `just fixtures-sign`, which writes it first \
             (from the workspace root).",
            scorecard_path.display()
        )
    });
    let sc: Scorecard = serde_json::from_slice(&bytes).expect("scorecard.json parses");
    sc.validate_invariants()
        .expect("scorecard.json must satisfy its own invariants before it is signed");

    // Pinned, not minted (Task 1 / backlog T0-5 / Phase 1 item 1a). Minting a
    // fresh key here orphaned the `917cf9a2…` fingerprint that
    // docs/verify-a-scorecard.md teaches auditors to pin and that both committed
    // .sig files carry. The key is now READ when e2e/fixtures/signed/signing.pem
    // is present and minted only when it is absent, so a re-mint can change the
    // DOCUMENT and never the KEY. Recipe: docs/keys.md.
    let signing_key_path = dir.join("signing.pem");
    let (key, origin) = SigningKey::load_or_generate(&signing_key_path)
        .unwrap_or_else(|e| panic!("resolve {}: {e}", signing_key_path.display()));
    if origin == KeyOrigin::Minted {
        // Path only, never key material (brief §10 item 11). stderr, so `just
        // fixtures-sign`'s stdout redirect at justfile:31 is untouched.
        eprintln!(
            "mint_fixture: no key at {}; minted a fresh P-256 keypair there",
            signing_key_path.display()
        );
    }

    // Public key: checked in, read by `drill verify` (this task) and by
    // later tasks' fixtures. Written unconditionally: it is derived from the
    // key in hand, so on the READ path these bytes are the committed bytes and
    // the write is a no-op in content.
    std::fs::write(
        dir.join("public.pem"),
        key.verifying_key()
            .to_public_key_pem()
            .expect("public key PEM-encodes"),
    )
    .expect("write public.pem");
    // The private key is NOT written here. `load_or_generate` owns that file:
    // it writes it on the mint path only, and leaves it untouched on the read
    // path — which is what keeps `signing.pem` byte-identical across a re-mint.

    // scorecard.sig: sign the EXACT bytes read from scorecard.json — never a
    // re-serialisation (spec §6 C3, verify-as-read).
    let sidecar = sign_detached(&key, PAYLOAD_TYPE_SCORECARD, &bytes).expect("sign scorecard.json");
    write_sidecar(&dir.join("scorecard.sig"), &sidecar);

    // scorecard-self-attested.json: parse, flip approval.self_attested,
    // re-serialise through the SAME deterministic-JSON path production code
    // uses, then sign THAT byte string — never sign one document and ship a
    // hand-edited copy (addendum ruling A3).
    let mut self_attested = sc;
    self_attested.approval.self_attested = true;
    // Task 2 / T0-5: a variant claiming self-attestation must BE self-attested —
    // the approval key id is the signing key's. Task 3 derives `self_attested`
    // from exactly this comparison instead of echoing the document's claim, so
    // scorecard.json (approval.key_id = "a"*64, emit_fixture.rs) stays
    // derivably-false and this variant stays derivably-true. `sign_detached`
    // stamps the same value into the sidecar's `keyid`, which is what
    // `fixture_regen.rs::self_attested_fixture_key_id_equals_the_signature_keyid`
    // compares.
    self_attested.approval.key_id = key.key_id();
    self_attested
        .validate_invariants()
        .expect("the self-attested variant must also satisfy every invariant");
    let self_attested_bytes =
        to_deterministic_json(&self_attested).expect("self-attested scorecard serialises");
    std::fs::write(
        dir.join("scorecard-self-attested.json"),
        &self_attested_bytes,
    )
    .expect("write scorecard-self-attested.json");

    let self_attested_sidecar = sign_detached(&key, PAYLOAD_TYPE_SCORECARD, &self_attested_bytes)
        .expect("sign scorecard-self-attested.json");
    write_sidecar(
        &dir.join("scorecard-self-attested.sig"),
        &self_attested_sidecar,
    );

    eprintln!("minted signed fixtures under {}", dir.display());
}
