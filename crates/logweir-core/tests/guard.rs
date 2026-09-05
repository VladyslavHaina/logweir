use logweir_core::guard::{check_topic_mapping_coverage, scan_forbidden_keys};
use std::collections::BTreeMap;

/// The guard is on the KEY, not the value: `purge_topics: false` is refused
/// exactly like `true` (spec §9.3 phase 0, GT-9).
#[test]
fn purge_topics_is_refused_at_either_value() {
    for v in ["true", "false"] {
        let doc = format!("restore:\n  purge_topics: {v}\n");
        assert_eq!(
            scan_forbidden_keys(&doc).unwrap(),
            vec!["restore.purge_topics".to_string()],
            "value {v}"
        );
    }
}

#[test]
fn dry_run_is_refused_at_either_value() {
    for v in ["true", "false"] {
        let doc = format!("restore:\n  dry_run: {v}\n");
        assert_eq!(
            scan_forbidden_keys(&doc).unwrap(),
            vec!["restore.dry_run".to_string()]
        );
    }
}

#[test]
fn header_preflight_external_is_refused_but_header_preflight_is_not() {
    assert_eq!(
        scan_forbidden_keys("restore:\n  header_preflight_external: true\n").unwrap(),
        vec!["restore.header_preflight_external".to_string()]
    );
    assert!(scan_forbidden_keys("restore:\n  header_preflight: full\n")
        .unwrap()
        .is_empty());
}

#[test]
fn a_forbidden_key_at_any_nesting_depth_is_found() {
    let doc = "a:\n  b:\n    c:\n      purge_topics: true\n";
    assert_eq!(
        scan_forbidden_keys(doc).unwrap(),
        vec!["a.b.c.purge_topics".to_string()]
    );
}

#[test]
fn a_clean_document_passes() {
    assert!(
        scan_forbidden_keys("restore:\n  create_topics: true\n  header_preflight: full\n")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn every_selected_topic_must_have_a_mapping_whose_target_differs() {
    let mut m = BTreeMap::new();
    m.insert("orders".to_string(), "drill-orders".to_string());
    assert!(check_topic_mapping_coverage(&["orders".into()], &m).is_ok());
    assert!(check_topic_mapping_coverage(&["orders".into(), "payments".into()], &m).is_err());

    let mut same = BTreeMap::new();
    same.insert("orders".to_string(), "orders".to_string());
    assert!(
        check_topic_mapping_coverage(&["orders".into()], &same).is_err(),
        "a mapping onto the same name would restore over the source topic"
    );
}

/// THE SCANNER FAILS CLOSED. A document it cannot parse is a document it did
/// not scan, and it must say so rather than answering "I found nothing" — the
/// predecessor returned an empty list, which is indistinguishable from a clean
/// document to every caller. This is the same "reported a pass having examined
/// nothing" shape the integrity chokepoint exists to prevent, in the guard
/// layer.
#[test]
fn a_document_the_scanner_cannot_parse_is_refused_never_reported_clean() {
    let err = scan_forbidden_keys("restore:\n  a: [unclosed\n\t\tbad: : :\n")
        .expect_err("unparseable YAML must not answer `no forbidden keys`");
    let msg = err.to_string();
    assert!(
        msg.contains("did not scan it"),
        "the refusal must say the document was not scanned, not merely that it was bad: {msg}"
    );
}
