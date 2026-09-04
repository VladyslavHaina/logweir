use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

#[test]
fn schema_scorecard_prints_the_schema() {
    let out = bin().args(["schema", "scorecard"]).output().unwrap();
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("logweir-drill-scorecard-1.0.0.json"));
}

#[test]
fn schema_plan_exits_1_naming_the_sub_project() {
    let out = bin().args(["schema", "plan"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("SP3"),
        "must name the sub-project that introduces it, got: {err}"
    );
}

#[test]
fn drill_verify_accepts_a_good_scorecard_and_rejects_a_tampered_one() {
    let dir = tempfile::tempdir().unwrap();
    let sc = dir.path().join("s.json");
    let sig = dir.path().join("s.sig");
    let pubk = dir.path().join("pub.pem");
    // fixtures/ carries a pre-signed scorecard + sidecar + public key, minted
    // once by `just fixtures-sign` (step 4).
    std::fs::copy("../../e2e/fixtures/signed/scorecard.json", &sc).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/scorecard.sig", &sig).unwrap();
    std::fs::copy("../../e2e/fixtures/signed/public.pem", &pubk).unwrap();

    let out = bin()
        .args(["drill", "verify", "--scorecard"])
        .arg(&sc)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubk)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut bytes = std::fs::read(&sc).unwrap();
    let pos = bytes.iter().position(|b| *b == b'1').unwrap();
    bytes[pos] = b'2';
    std::fs::write(&sc, bytes).unwrap();

    let out = bin()
        .args(["drill", "verify", "--scorecard"])
        .arg(&sc)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubk)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(4), "a tampered payload must exit 4");
}

#[test]
fn drill_verify_labels_a_self_attested_scorecard() {
    let out = bin()
        .args([
            "drill",
            "verify",
            "--scorecard",
            "../../e2e/fixtures/signed/scorecard-self-attested.json",
            "--signature",
            "../../e2e/fixtures/signed/scorecard-self-attested.sig",
            "--public-key",
            "../../e2e/fixtures/signed/public.pem",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(
        s.contains("SELF-ATTESTED"),
        "the label must be surfaced, got: {s}"
    );
}
