//! The committed signed fixtures and the generator that produces them must not
//! drift apart.
//!
//! Before Task 2 they had. `emit_fixture` emitted `last_phase_completed: 7` and
//! the checked-in documents said `9`; `emit_fixture` emitted
//! `evidence.create_only_enforced: true` and phase 8 emits `false`
//! unconditionally — so the project's own worked example falsified the sentence
//! `docs/verify_scorecard.py` printed on every successful verification, and
//! `drill verify` said `VALID` over it. Nothing compared the two, because the
//! only thing that had ever compared them was a person.
//!
//! `just fixtures-sign` is the ONLY sanctioned way to change the four files
//! under `e2e/fixtures/signed/` that this crate's generator feeds. These tests
//! are what makes a hand-edit, a half-applied regeneration, or a generator
//! change that was never re-minted fail at `cargo test` rather than at an
//! auditor's desk.
//!
//! Global Constraint 1: this is an integration-test target, not `logweir-core`
//! source. It uses `std` and the crate's own `serde_json`, adds no dependency,
//! and `scripts/check-pure-core.sh` (which scans `crates/logweir-core/src`)
//! is unaffected.

use logweir_core::scorecard::Scorecard;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The workspace root. `CARGO_MANIFEST_DIR` is `crates/logweir-core`.
fn workspace_root() -> PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../..")).to_path_buf()
}

fn signed_dir() -> PathBuf {
    workspace_root().join("e2e/fixtures/signed")
}

fn read_fixture(name: &str) -> Vec<u8> {
    let p = signed_dir().join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn parse_scorecard(name: &str) -> Scorecard {
    serde_json::from_slice(&read_fixture(name))
        .unwrap_or_else(|e| panic!("{name} must parse as a Scorecard: {e}"))
}

fn parse_json(name: &str) -> serde_json::Value {
    serde_json::from_slice(&read_fixture(name)).unwrap_or_else(|e| panic!("{name} parses: {e}"))
}

/// The committed `scorecard.json` is EXACTLY what the generator emits today.
///
/// This is the test that catches a hand-edit of a signed fixture, and the one
/// that catches a generator change nobody re-minted. It kills both directions
/// of the drift Task 2 found.
///
/// The separate `--target-dir` is deliberate: the outer `cargo test` run holds
/// the default target directory's build lock, so a child `cargo run` into it
/// would block until this test's own harness exited.
#[test]
fn emit_fixture_reproduces_the_committed_scorecard_bytes() {
    let out = Command::new(env!("CARGO"))
        .current_dir(workspace_root())
        .args([
            "run",
            "-q",
            "-p",
            "logweir-core",
            "--example",
            "emit_fixture",
            "--target-dir",
            "target/fixture-regen",
        ])
        .output()
        .expect("spawn cargo run -p logweir-core --example emit_fixture");

    assert!(
        out.status.success(),
        "emit_fixture must succeed — it validates its own invariants before printing. \
         status={:?}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );

    let committed = read_fixture("scorecard.json");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&committed),
        "e2e/fixtures/signed/scorecard.json is not what `emit_fixture` emits. \
         Re-run `just fixtures-sign` — never hand-edit a signed fixture."
    );
}

/// `last_phase_completed` is `7` in both signed documents: the generator's
/// value, settled in Task 2 over the checked-in `9`.
///
/// This is the PINNED VALUE, not the domain. Global Constraint 18 keeps the
/// `-1..=9` domain exactly as it was, and this test says nothing about it — a
/// signed document simply cannot read `9`, because `phase8_score::run` signs a
/// frozen clone before phases 8 and 9 are recorded.
#[test]
fn fixture_last_phase_matches_generator() {
    for name in ["scorecard.json", "scorecard-self-attested.json"] {
        let sc = parse_scorecard(name);
        assert_eq!(
            sc.last_phase_completed, 7,
            "{name} must pin the generator's last_phase_completed"
        );
    }
}

/// Both signed documents carry a fully zeroed `evidence` block, and both
/// satisfy every invariant.
///
/// The four fields describe an upload that has not happened when the bytes are
/// signed. `docs/verify_scorecard.py` printed that as a guarantee on every
/// successful verification while these two files falsified it.
#[test]
fn committed_fixtures_have_a_zeroed_evidence_block() {
    for name in ["scorecard.json", "scorecard-self-attested.json"] {
        let sc = parse_scorecard(name);
        assert!(
            sc.evidence.version_id.is_none(),
            "{name}: evidence.version_id must be null"
        );
        assert!(
            sc.evidence.retain_until.is_none(),
            "{name}: evidence.retain_until must be null"
        );
        assert!(
            !sc.evidence.immutable,
            "{name}: evidence.immutable must be false"
        );
        assert!(
            !sc.evidence.create_only_enforced,
            "{name}: evidence.create_only_enforced must be false"
        );
        sc.validate_invariants()
            .unwrap_or_else(|e| panic!("{name} must satisfy every invariant: {e}"));
    }
}

/// The self-attested variant IS self-attested, derivably — its
/// `approval.key_id` equals the key id its own DSSE sidecar was signed under —
/// and `scorecard.json` derivably is NOT.
///
/// Task 3 replaces the reader's trust in `approval.self_attested` (a claim the
/// document makes about itself) with exactly this comparison. Minting the pair
/// so both answers are already correct is what spares the stage a second
/// re-mint, and this test is what stops the property being dropped from
/// `mint_fixture` in between.
#[test]
fn self_attested_fixture_key_id_equals_the_signature_keyid() {
    let doc = parse_json("scorecard-self-attested.json");
    let sidecar = parse_json("scorecard-self-attested.sig");
    let doc_key_id = doc["approval"]["key_id"]
        .as_str()
        .expect("approval.key_id is a string");
    let sig_keyid = sidecar["signatures"][0]["keyid"]
        .as_str()
        .expect("signatures[0].keyid is a string");
    assert_eq!(
        doc_key_id, sig_keyid,
        "scorecard-self-attested.json claims self_attested: true, so its approval \
         key id must BE the signing key's — otherwise the claim is unearned"
    );
    assert_eq!(
        doc["approval"]["self_attested"],
        serde_json::json!(true),
        "the self-attested variant must carry the claim it is named for"
    );

    let plain = parse_json("scorecard.json");
    let plain_sidecar = parse_json("scorecard.sig");
    assert_ne!(
        plain["approval"]["key_id"].as_str().unwrap(),
        plain_sidecar["signatures"][0]["keyid"].as_str().unwrap(),
        "scorecard.json is the derivably-NOT-self-attested half of the pair; if its \
         approval key id ever equals the signing key's, the pair stops covering \
         both branches"
    );
    assert_eq!(
        plain["approval"]["self_attested"],
        serde_json::json!(false),
        "scorecard.json must not claim self-attestation"
    );
}

/// `just fixtures-sign` never redirects into a tracked fixture.
///
/// The shell truncates a redirect target BEFORE the program runs, and
/// `emit_fixture` ends with `validate_invariants().expect(...)`. The recipe
/// used to emit straight into the tracked, signed, fingerprint-pinned
/// `e2e/fixtures/signed/scorecard.json`, so a generator that refused its own
/// document zeroed the committed fixture first and panicked second. Emitting
/// into `target/` and `mv`-ing on success is the whole fix, and it is one
/// careless edit away from being undone.
#[test]
fn fixtures_recipe_is_non_destructive() {
    let justfile =
        std::fs::read_to_string(workspace_root().join("justfile")).expect("read justfile");

    let mut lines = justfile.lines();
    lines
        .by_ref()
        .find(|l| l.trim_end() == "fixtures-sign:")
        .expect("justfile has a `fixtures-sign` recipe");
    let body: Vec<&str> = lines
        .take_while(|l| l.starts_with(' ') || l.starts_with('\t') || l.trim().is_empty())
        .filter(|l| !l.trim().is_empty())
        .collect();
    assert!(
        !body.is_empty(),
        "the fixtures-sign recipe body must not be empty"
    );

    for line in &body {
        for (i, _) in line.match_indices('>') {
            // Everything the redirect could name, up to the next command
            // separator.
            let target: &str = line[i + 1..]
                .split("&&")
                .next()
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("");
            assert!(
                !target.contains("e2e/fixtures"),
                "fixtures-sign redirects into a tracked fixture: {line:?}. The shell \
                 truncates a redirect target before the program runs, so a failing \
                 generator would destroy the committed, signed fixture. Emit into \
                 target/ and `mv` on success."
            );
        }
    }

    let emit_line = body
        .iter()
        .find(|l| l.contains("emit_fixture"))
        .expect("fixtures-sign runs emit_fixture");
    assert!(
        emit_line.contains("&& mv"),
        "the emit step must `mv` into place only on success: {emit_line:?}"
    );
}
