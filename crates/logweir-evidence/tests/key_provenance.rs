//! The fixture signing key is PINNED, not minted.
//!
//! `mint_fixture` used to call `SigningKey::generate_p256()` unconditionally,
//! so every regeneration of the signed fixtures orphaned the `917cf9a2…`
//! fingerprint that `docs/verify-a-scorecard.md` teaches an auditor to pin and
//! that both committed `.sig` sidecars carry. These tests hold the pin from
//! four directions: the checked-in key is the pinned key, an absent key really
//! is minted (and really is a fresh one), a corrupt key file is an error rather
//! than a silent re-mint, and the recipe that produced the key is documented
//! and reachable from the two documents that reference the key.
//!
//! Nothing here asserts on private key material: the key is identified by
//! `key_id()` (the SHA-256 of its SPKI DER) and never by its PEM bytes.

use logweir_evidence::keys::{KeyAlg, KeyOrigin, SigningKey};
use logweir_evidence::Sidecar;

/// The workspace root, from this crate's manifest directory.
fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn mint_fixture_reads_the_pinned_key_when_present() {
    let root = repo_root();
    let (key, origin) =
        SigningKey::load_or_generate(&root.join("e2e/fixtures/signed/signing.pem")).unwrap();
    assert_eq!(origin, KeyOrigin::LoadedFromFile);

    let sidecar: Sidecar = serde_json::from_slice(
        &std::fs::read(root.join("e2e/fixtures/signed/scorecard.sig")).unwrap(),
    )
    .unwrap();
    // The committed pin, spelled out so a silent fixture re-mint cannot make
    // this test pass by moving both sides at once.
    assert_eq!(
        key.key_id(),
        "917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd"
    );
    assert_eq!(key.key_id(), sidecar.signatures[0].keyid);
    // And the public half round-trips to the committed bytes.
    assert_eq!(
        key.verifying_key().to_public_key_pem().unwrap(),
        std::fs::read_to_string(root.join("e2e/fixtures/signed/public.pem")).unwrap()
    );
}

#[test]
fn mint_fixture_mints_when_key_absent() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let pa = a.path().join("signing.pem");
    let pb = b.path().join("signing.pem");

    let (ka, oa) = SigningKey::load_or_generate(&pa).unwrap();
    let (kb, ob) = SigningKey::load_or_generate(&pb).unwrap();
    assert_eq!(oa, KeyOrigin::Minted);
    assert_eq!(ob, KeyOrigin::Minted);
    assert!(pa.exists(), "the mint path must write the key it minted");
    // A valid P-256 PKCS#8 PEM: it re-loads, and it re-loads as the SAME key.
    let (reloaded, o2) = SigningKey::load_or_generate(&pa).unwrap();
    assert_eq!(o2, KeyOrigin::LoadedFromFile);
    assert_eq!(reloaded.key_id(), ka.key_id());
    assert_eq!(reloaded.alg(), KeyAlg::EcdsaP256Sha256);
    // Two mints are two DIFFERENT keys — this is what kills the hard-coded-key mutant.
    assert_ne!(ka.key_id(), kb.key_id());
}

#[test]
fn loading_a_corrupt_key_is_an_error_not_a_silent_remint() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("signing.pem");
    std::fs::write(
        &p,
        "-----BEGIN PRIVATE KEY-----\nnot a key\n-----END PRIVATE KEY-----\n",
    )
    .unwrap();
    let before = std::fs::read(&p).unwrap();
    // `.map(…)` before `.unwrap_err()` only because `SigningKey` deliberately
    // does NOT implement `Debug` — a private key must never be printable, and
    // `unwrap_err()` needs `Debug` on the Ok type. Mapping to `KeyOrigin`
    // keeps the panic-on-Ok that kills the swallow-the-error mutant, and makes
    // its message name the origin the mutant returned instead of erroring.
    let err = SigningKey::load_or_generate(&p)
        .map(|(_, origin)| origin)
        .unwrap_err();
    assert!(
        matches!(err, logweir_evidence::Error::Key(_)),
        "got {err:?}"
    );
    assert_eq!(
        std::fs::read(&p).unwrap(),
        before,
        "a corrupt key file must never be overwritten"
    );
}

#[test]
fn keygen_recipe_is_documented_and_linked() {
    let root = repo_root();
    let keys = std::fs::read_to_string(root.join("docs/keys.md")).unwrap();
    for needle in [
        "openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem",
        "openssl pkey -in signing.pem -pubout -out public.pem",
        "debian:bookworm-slim",
        "917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd",
    ] {
        assert!(keys.contains(needle), "docs/keys.md is missing: {needle}");
    }
    // The pin document reaches the recipe document.
    let pinned = std::fs::read_to_string(root.join("docs/verify-a-scorecard.md")).unwrap();
    assert!(
        pinned.contains("keys.md"),
        "docs/verify-a-scorecard.md must link docs/keys.md"
    );
    let fixtures = std::fs::read_to_string(root.join("e2e/fixtures/signed/README.md")).unwrap();
    assert!(
        fixtures.contains("keys.md"),
        "the fixture README must link docs/keys.md"
    );
}

/// Kills the one mutant the four pinned tests leave to a reviewer's eye:
/// linking `docs/keys.md` from the fixture README while leaving that README's
/// two "the keypair is regenerated" claims standing. Both are FALSE the moment
/// `mint_fixture` reads the pinned key, and a document asserting behaviour the
/// code does not have is this project's recurring defect.
///
/// Asserted as ABSENCE, not presence, on purpose: the corrected sentences live
/// in a section a later task may delete wholesale, and a presence assertion
/// would go red on that deletion even though nothing was broken. Absence
/// survives it.
#[test]
fn the_fixture_readme_makes_no_stale_claim_that_the_keypair_is_regenerated() {
    let readme =
        std::fs::read_to_string(repo_root().join("e2e/fixtures/signed/README.md")).unwrap();
    // Collapse whitespace so a re-wrapped line cannot hide a claim from the check.
    let flat = readme.split_whitespace().collect::<Vec<_>>().join(" ");
    for stale in [
        "it is regenerated on demand by `just fixtures-sign`",
        "generates a NEW key pair on every run",
    ] {
        assert!(
            !flat.contains(stale),
            "e2e/fixtures/signed/README.md still claims the KEYPAIR is regenerated: {stale}"
        );
    }
}
