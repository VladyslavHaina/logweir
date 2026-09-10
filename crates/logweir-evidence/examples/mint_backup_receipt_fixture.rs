// crates/logweir-evidence/examples/mint_backup_receipt_fixture.rs
//!
//! Mints the checked-in signed **backup receipt** fixture:
//! `e2e/fixtures/signed/backup-receipt.json` and `…/backup-receipt.sig`.
//! Invoked by `just fixtures-sign` from the workspace root, beside
//! `mint_fixture` (the scorecard's minter).
//!
//! # It writes BOTH files itself — nothing is shell-redirected
//!
//! `just fixtures-sign` stays non-destructive
//! (`crates/logweir-core/tests/fixture_regen.rs::
//! fixtures_recipe_is_non_destructive`). The shell truncates a redirect
//! target BEFORE the program runs, and this program validates its own
//! document before it signs it — so a redirect into the tracked, signed
//! fixture would zero the committed file first and refuse second. The
//! scorecard's line needs a `&& mv` for that reason; this one needs no
//! redirect at all, which is strictly better: the document and the signature
//! over it are written by one process, in one order, or not at all.
//!
//! # The key
//!
//! READ, never minted: the checked-in throwaway `e2e/fixtures/signed/
//! signing.pem`, whose key id is the same `917cf9a2…` fingerprint every other
//! fixture here carries and that `docs/verify-a-scorecard.md` teaches
//! auditors to pin. `SigningKey::from_pem_file` rather than
//! `load_or_generate`, so this program can never invent a key: a fresh key
//! here would orphan that fingerprint and re-sign the corpus under something
//! nobody documented. The key id is echoed to **stderr** on every run so the
//! provenance is in the transcript; the private half is never printed.
//!
//! # What the document describes
//!
//! A SUCCESSFUL two-topic backup: `exit_code: 0`, a named manifest,
//! per-topic counts over exactly the named topic set, and a covered window
//! that begins before it ends, and `source.auth.mode` inside the closed
//! two-value set — so it satisfies all five of
//! `BackupReceipt::validate_invariants`' arms, which are asserted here before
//! anything is signed. Every value is a plausible constant, not a measured
//! one: this is an example of the FORMAT, and `logweir backup run` writing a
//! real one is Task 5b's.
//!
//! # The one re-mint, and why the key id does not move
//!
//! Task 5b fix round 1 re-mints the pair, on controller ruling, because
//! `source.auth.mode` moved from `scram-sha-512` to **`scramSha512`** — the
//! product's one spelling, and now a value BOTH readers close (arm 5). The
//! key is still READ from the same checked-in throwaway PEM, so the
//! `917cf9a2…` fingerprint `docs/verify-a-scorecard.md` teaches auditors to
//! pin is unchanged, and the three other signed pairs under
//! `e2e/fixtures/signed/` are not touched by this program at all. ECDSA over
//! P-256 here is RFC 6979 deterministic, so re-running this program over an
//! unchanged document reproduces both files byte for byte.

use logweir_core::backup_receipt::{
    BackupReceipt, ReceiptArchive, ReceiptAuth, ReceiptCovered, ReceiptEngine, ReceiptSource,
};
use logweir_core::det_json::to_deterministic_json;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT;
use std::collections::BTreeMap;
use std::path::Path;

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    s.parse().expect("fixture timestamp is RFC 3339")
}

fn receipt() -> BackupReceipt {
    let mut records = BTreeMap::new();
    records.insert("orders".to_string(), 4_211_u64);
    records.insert("payments".to_string(), 917_u64);
    BackupReceipt {
        format_version: "1.0.0".to_string(),
        run_id: "01J8Z9QK7V6M3F2R5T8W1XB0CD".to_string(),
        backup_id: "logweir-backup-01J8Z9QK7V".to_string(),
        requested_at: ts("2026-09-09T11:02:14Z"),
        started_at: ts("2026-09-09T11:02:19Z"),
        finished_at: ts("2026-09-09T11:04:46Z"),
        exit_code: 0,
        triggered_by: "release-engineering".to_string(),
        source: ReceiptSource {
            cluster_id: "kRtQ7yZ1S0uPq9AeVxN2Lg".to_string(),
            bootstrap_servers: vec!["kafka-broker-1:9094".to_string()],
            auth: ReceiptAuth {
                mode: "scramSha512".to_string(),
                username: Some("logweir-backup".to_string()),
            },
            // Named, in the spec's order, with no glob metacharacter
            // (GC18(c) rail 1 / G-GLOB). `records` above covers exactly this
            // set, which is invariant 3.
            topics: vec!["orders".to_string(), "payments".to_string()],
        },
        engine: ReceiptEngine {
            id: "oso-cli".to_string(),
            version: "v0.21.0".to_string(),
            digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        },
        archive: ReceiptArchive {
            manifest_key: "logweir/backups/logweir-backup-01J8Z9QK7V/manifest.json".to_string(),
            manifest_sha256:
                "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
            prefix: "logweir/backups/logweir-backup-01J8Z9QK7V/".to_string(),
        },
        records,
        covered: ReceiptCovered {
            from_ms: 1_757_415_734_000,
            to_ms: 1_757_419_486_000,
        },
    }
}

fn main() {
    let dir = Path::new("e2e/fixtures/signed");
    if !dir.is_dir() {
        panic!(
            "{} does not exist; run this via `just fixtures-sign`, from the workspace root",
            dir.display()
        );
    }

    let receipt = receipt();
    // Refused BEFORE it is written, let alone signed. A generator that can
    // emit a document its own reader rejects is how `e2e/fixtures/signed/`
    // acquired a signed scorecard that falsified the guarantee
    // `docs/verify_scorecard.py` printed over it.
    receipt
        .validate_invariants()
        .expect("the fixture receipt must satisfy its own five arms before it is signed");

    let bytes = to_deterministic_json(&receipt).expect("receipt serialises");

    let signing_key_path = dir.join("signing.pem");
    let key = SigningKey::from_pem_file(&signing_key_path).unwrap_or_else(|e| {
        panic!(
            "cannot read the pinned fixture key at {}: {e}. This program never mints one — a \
             fresh key would orphan the fingerprint docs/verify-a-scorecard.md pins and \
             re-sign the corpus under something undocumented.",
            signing_key_path.display()
        )
    });
    // Path and key id, never key material.
    eprintln!(
        "mint_backup_receipt_fixture: signing with the pinned key at {} (key id {})",
        signing_key_path.display(),
        key.key_id()
    );

    // The document first, then the signature over the EXACT bytes just
    // written — never over a re-serialisation of them (spec §6 C3,
    // verify-as-read).
    let doc_path = dir.join("backup-receipt.json");
    std::fs::write(&doc_path, &bytes)
        .unwrap_or_else(|e| panic!("write {}: {e}", doc_path.display()));

    let sidecar =
        sign_detached(&key, PAYLOAD_TYPE_BACKUP_RECEIPT, &bytes).expect("sign backup-receipt.json");
    let mut sig_json = serde_json::to_string_pretty(&sidecar).expect("sidecar serialises");
    sig_json.push('\n');
    let sig_path = dir.join("backup-receipt.sig");
    std::fs::write(&sig_path, sig_json)
        .unwrap_or_else(|e| panic!("write {}: {e}", sig_path.display()));

    eprintln!("minted {} and {}", doc_path.display(), sig_path.display());
}
