// crates/logweir-evidence/examples/mint_fixture.rs
//!
//! Mints the checked-in signed test fixtures under `e2e/fixtures/signed/`.
//! Invoked by `just fixtures-sign`, which first regenerates
//! `e2e/fixtures/signed/scorecard.json` via `logweir-core`'s `emit_fixture`
//! example (Task 3), then runs this binary from the workspace root.
//!
//! ALL key material this program generates is a THROWAWAY TEST FIXTURE — see
//! `e2e/fixtures/signed/README.md`. The private key is written to disk
//! (`signing.pem`, checked in per Task 1's `.gitignore` un-ignore) but is
//! NEVER printed to stdout/stderr.

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

    let key = SigningKey::generate_p256();

    // Public key: checked in, read by `drill verify` (this task) and by
    // later tasks' fixtures.
    std::fs::write(
        dir.join("public.pem"),
        key.verifying_key()
            .to_public_key_pem()
            .expect("public key PEM-encodes"),
    )
    .expect("write public.pem");
    // Private key: ALSO checked in — Task 1's `.gitignore` un-ignores exactly
    // this directory's `*.pem` files because Task 14's `guard_cli.rs` passes
    // `--signing-key ../../e2e/fixtures/signed/signing.pem` and Task 22's
    // demo needs a worked example. A throwaway test key; never printed.
    std::fs::write(
        dir.join("signing.pem"),
        key.to_pkcs8_pem().expect("private key PEM-encodes"),
    )
    .expect("write signing.pem");

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
