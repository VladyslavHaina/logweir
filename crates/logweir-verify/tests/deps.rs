//! The crate boundary the `logweir-verify` extraction exists to create,
//! asserted at ASSERTION time rather than at link time — the same shape, and
//! for the same reason, as `crates/logweir-store/tests/deps.rs`.
//!
//! `weirkeeper` must be able to VERIFY a DSSE signature without linking the
//! signer (`scripts/check-one-signer.sh:46-49` ruled that remedy in writing;
//! ADR 0008 §E). An edge back to `logweir-evidence` would be a dependency
//! CYCLE and would fail `cargo build` long before any test ran, which sounds
//! like enough and is not: a build failure is not a NAMED property, nothing
//! records why the build broke, and the moment someone makes the cycle
//! buildable (a `[dev-dependencies]` edge, a feature-gated edge) the boundary
//! is gone with no test to notice. So the property is read out of
//! `cargo metadata`'s DECLARED dependency set, which needs no successful
//! compile.
//!
//! `--no-deps` deliberately: it resolves nothing, downloads nothing and takes
//! no package-cache lock, so this stays a millisecond-scale unit test rather
//! than something that can hang behind a concurrent build (Global Constraint
//! 22's 15 s per-test bound).
//!
//! The plan's Files block names exactly one test file for this crate, so the
//! two API-surface tests that prove the moved `impl VerifyingKey` block is
//! reachable FROM here — `verifying_key_parses_a_pem_from_memory` and
//! `to_public_key_pem_round_trips` — live here too rather than in a path the
//! plan never named.

use std::collections::BTreeSet;

/// The workspace root: this crate's manifest directory is
/// `crates/logweir-verify`.
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/logweir-verify sits two levels under the workspace root")
        .to_path_buf()
}

/// Every dependency a workspace member's `Cargo.toml` DECLARES, of every kind
/// (normal, dev and build alike — a dev edge back to the signer would defeat
/// the extraction just as thoroughly as a normal one).
fn declared_dependencies(package: &str) -> BTreeSet<String> {
    let out = std::process::Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(workspace_root())
        .output()
        .expect("cargo metadata runs");
    assert!(
        out.status.success(),
        "cargo metadata --no-deps failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cargo metadata emits JSON");
    let pkg = meta
        .get("packages")
        .and_then(|p| p.as_array())
        .expect("metadata carries packages")
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some(package))
        .unwrap_or_else(|| panic!("{package} is a workspace member"));
    pkg.get("dependencies")
        .and_then(|d| d.as_array())
        .expect("the package carries a dependency array")
        .iter()
        .filter_map(|d| d.get("name").and_then(|n| n.as_str()))
        .map(|s| s.to_string())
        .collect()
}

/// TWO SEPARATE CLAIMS, deliberately.
///
/// (a) is the exact set, so a future dependency addition — legitimate or not —
/// fails loudly and gets read by a human. (b) is the property this crate
/// exists for, asserted on its own so that relaxing (a) for a legitimate
/// addition can never weaken it. One combined assertion would let a widened
/// set quietly carry the signer back in, which is exactly the mutant.
#[test]
fn logweir_verify_reaches_no_signer() {
    let deps = declared_dependencies("logweir-verify");

    // (a) the exact declared set.
    let expected: BTreeSet<String> = [
        "base64",
        "ed25519-dalek",
        "hex",
        "p256",
        "serde",
        "serde_json",
        "sha2",
        "thiserror",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(
        deps, expected,
        "logweir-verify's declared dependency set changed. `rand_core` is \
         deliberately ABSENT — it is the signer's entropy source and the whole \
         content of this split; `serde_json` is REQUIRED (this file reads \
         `cargo metadata`'s JSON). Adding a dependency here is a reviewable \
         event: change this list in the same commit and say why."
    );

    // (b) the boundary itself, on its own.
    assert!(
        !deps.contains("logweir-evidence"),
        "logweir-verify exists so a component can VERIFY a signature WITHOUT \
         linking the signer (ADR 0008 §E; scripts/check-one-signer.sh:46-49); \
         it declared {deps:?}"
    );
}

/// The H22 pair, as a DECLARATION test rather than a `cargo tree` occurrence
/// count — and the reason is measured, not preference.
///
/// `rand_core` cannot be absent from `logweir-verify`'s resolved tree: `p256`
/// requires `elliptic-curve`, which declares `rand_core` NON-optionally, and
/// `signature`'s own `rand_core` feature brings `getrandom` with it. Measured
/// on this tree, `cargo tree -p logweir-verify -e normal --prefix none |
/// grep -c '^rand_core '` is **5** and the same line for `logweir-evidence` is
/// **7** — so a "zero transitive `rand_core`" assertion would be red on a
/// correct split, and a test that cannot be green is not a gate.
///
/// What IS true, and what the split is actually about, is the DECLARATION:
/// `logweir-evidence` takes `rand_core = { version = "0.6", features =
/// ["getrandom"] }` because `SigningKey::generate_*` needs an entropy source,
/// and `logweir-verify` takes no `rand_core` line at all. BOTH HALVES ARE THE
/// ASSERTION: an absent `rand_core` here proves nothing on its own, because a
/// mis-read manifest name would make it absent everywhere.
#[test]
fn the_verifying_half_declares_no_entropy_source_and_the_signing_half_does() {
    let verify = declared_dependencies("logweir-verify");
    let evidence = declared_dependencies("logweir-evidence");
    assert!(
        !verify.contains("rand_core"),
        "logweir-verify must declare no entropy source; it declared {verify:?}"
    );
    assert!(
        evidence.contains("rand_core"),
        "logweir-evidence must still declare `rand_core` — if it does not, this \
         test's other half is proving nothing. It declared {evidence:?}"
    );
}

/// `KeyAlg` stays with the signer.
///
/// Asserted over SOURCE TEXT on purpose. Naming `logweir_evidence::keys::KeyAlg`
/// as a Rust path from here is impossible — it would be the very dependency
/// edge `logweir_verify_reaches_no_signer` forbids — and a compile error is
/// the wrong failure mode anyway: the mutant ("move `KeyAlg` into
/// `logweir-verify`") must fail at ASSERTION time, with a message that says
/// which file was supposed to declare it.
///
/// `KeyAlg` is `SigningKey::alg`'s return type and `impl VerifyingKey` never
/// names it.
#[test]
fn key_alg_stays_with_the_signer() {
    let root = workspace_root();
    let signer = std::fs::read_to_string(root.join("crates/logweir-evidence/src/keys.rs"))
        .expect("crates/logweir-evidence/src/keys.rs is readable");
    let verifier = std::fs::read_to_string(root.join("crates/logweir-verify/src/keys.rs"))
        .expect("crates/logweir-verify/src/keys.rs is readable");
    assert!(
        signer.contains("pub enum KeyAlg {"),
        // The signing-API token is spelled only in the `//` comments of this
        // file, never in a string literal: `check-one-signer.sh`'s check 3
        // greps every `*.rs` under `crates/` for it and `ALLOWED_SOURCE` is
        // byte-identical to what it was before this extraction, so
        // `logweir-verify` is not on it and must not name the API.
        "`KeyAlg` must still be DECLARED in crates/logweir-evidence/src/keys.rs — \
         it is the return type of the signing key's `alg` method"
    );
    assert!(
        !verifier.contains("enum KeyAlg"),
        "`KeyAlg` must not be declared in crates/logweir-verify/src/keys.rs: \
         `impl VerifyingKey` never names it, and moving it would drag the \
         signing-side vocabulary into the verify-only crate"
    );
}

/// The checked-in fixture public key, parsed from a `&str`.
///
/// This is what Task 16's `TrustRoster` reconciler has in hand instead of a
/// file: it reads an entry's `spkiPem` out of an API object. The key id is
/// compared against `from_pem_file` over the same bytes, so the two entry
/// points cannot drift.
const FIXTURE_PEM: &str = "e2e/fixtures/signed/public.pem";

#[test]
fn verifying_key_parses_a_pem_from_memory() {
    let path = workspace_root().join(FIXTURE_PEM);
    let pem = std::fs::read_to_string(&path).expect("the fixture public key is readable");
    let from_memory =
        logweir_verify::VerifyingKey::from_pem_str(&pem).expect("from_pem_str parses the fixture");
    let from_file = logweir_verify::keys::VerifyingKey::from_pem_file(&path)
        .expect("from_pem_file parses the fixture");
    assert_eq!(
        from_memory.key_id(),
        from_file.key_id(),
        "from_pem_str and from_pem_file must be the same parse over the same bytes"
    );
    assert!(
        matches!(from_memory, logweir_verify::VerifyingKey::P256(_)),
        "the checked-in fixture is a P-256 key"
    );
}

/// `to_public_key_pem` moved with its `impl` block and is reachable from here.
///
/// Rust coherence is why it had to move: `logweir-evidence` cannot add an
/// inherent method to a type it no longer defines. This asserts the method is
/// on `logweir_verify::VerifyingKey`, which is the half of the mutant ("leave
/// `to_public_key_pem` behind in `logweir-evidence`") that a reviewer can see
/// without a compiler.
#[test]
fn to_public_key_pem_round_trips() {
    let pem = std::fs::read_to_string(workspace_root().join(FIXTURE_PEM))
        .expect("the fixture public key is readable");
    let k = logweir_verify::VerifyingKey::from_pem_str(&pem).expect("the fixture parses");
    let minted = k
        .to_public_key_pem()
        .expect("to_public_key_pem encodes SPKI");
    let round_tripped =
        logweir_verify::VerifyingKey::from_pem_str(&minted).expect("the minted PEM re-parses");
    assert_eq!(
        round_tripped.key_id(),
        k.key_id(),
        "to_public_key_pem must re-encode the same SubjectPublicKeyInfo"
    );
}
