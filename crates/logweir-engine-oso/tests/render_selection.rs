//! **PROD-11.1 — the engine runs a partition selection needs.**
//!
//! The pinned engine's `restore.source_partitions` is a run-wide filter: it
//! applies to every topic of one run (`restore/engine.rs:1253-1263` in the
//! 0.23.3 source). So a plan whose topics carry DIFFERENT subsets renders one
//! document per distinct subset, plus one unfiltered document for the topics
//! without one — and a plan with no subset renders the one document it always
//! rendered (the goldens in `render.rs` are unchanged by construction).
use logweir_core::engine::{BackupSetRef, RestorePlan, StorageUrl, WindowFloorSource};
use logweir_engine_oso::render_backup::RenderError;
use logweir_engine_oso::render_restore::{self, RestoreRun};

fn plan(subsets: &[(&str, &[i32])]) -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "b".into(),
            manifest_key: "b/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/archive".into(),
        },
        target_bootstrap: vec!["k:9092".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: ["audit", "orders", "payments"]
            .iter()
            .map(|t| (t.to_string(), format!("r-{t}")))
            .collect(),
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        window_floor_source: WindowFloorSource::InheritedFromSpec,
        source_partitions: subsets
            .iter()
            .map(|(t, ps)| (t.to_string(), ps.to_vec()))
            .collect(),
        default_replication_factor: 1,
        checkpoint_state: "/w/run/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/w/run/offsets.json".into(),
    }
}

/// The value of a `restore:` key, with the renderer's double quotes removed.
fn line<'a>(doc: &'a str, key: &str) -> Option<&'a str> {
    doc.lines()
        .find_map(|l| l.strip_prefix(&format!("  {key}: ")))
        .map(|v| v.trim_matches('"'))
}

fn include(doc: &str) -> Vec<String> {
    let v: serde_yaml_like::Doc = serde_yaml_like::parse(doc);
    v.include
}

/// A tiny reader for the one list this file asserts, so the test does not
/// depend on the renderer's own helpers: the `      - <topic>` lines under
/// `target.topics.include`.
mod serde_yaml_like {
    pub struct Doc {
        pub include: Vec<String>,
    }
    pub fn parse(doc: &str) -> Doc {
        let mut include = Vec::new();
        let mut inside = false;
        for l in doc.lines() {
            if l == "    include:" {
                inside = true;
                continue;
            }
            if inside {
                match l.strip_prefix("      - ") {
                    Some(t) => include.push(t.trim_matches('"').to_string()),
                    None => break,
                }
            }
        }
        Doc { include }
    }
}

/// No subset: ONE run, `restore.yaml`, no `source_partitions` key, the plan's
/// own paths — the document `render` has always produced.
#[test]
fn a_plan_without_a_subset_is_one_unfiltered_run() {
    let p = plan(&[]);
    let all = render_restore::render_all(&p).unwrap();
    assert_eq!(all.len(), 1);
    let (run, doc) = &all[0];
    assert_eq!(run.config_file_name(), "restore.yaml");
    assert_eq!(doc, &render_restore::render(&p).unwrap());
    assert!(line(doc, "source_partitions").is_none(), "{doc}");
    assert_eq!(
        line(doc, "checkpoint_state"),
        Some("/w/run/checkpoint.json")
    );
    assert_eq!(line(doc, "offset_report"), Some("/w/run/offsets.json"));
    assert_eq!(include(doc), vec!["audit", "orders", "payments"]);
}

/// Every topic sharing one subset is still ONE run, filtered.
#[test]
fn one_shared_subset_is_one_filtered_run() {
    let p = plan(&[("audit", &[0]), ("orders", &[0]), ("payments", &[0])]);
    let doc = render_restore::render(&p).expect("one run renders through `render`");
    assert_eq!(line(&doc, "source_partitions"), Some("[0]"));
    assert_eq!(
        line(&doc, "checkpoint_state"),
        Some("/w/run/checkpoint.json")
    );
}

/// DIFFERENT subsets on two topics are two filtered runs, and the topic
/// without a subset is a third, unfiltered one, first. Each run names only
/// its own topics and has its own checkpoint and offset report. `render`
/// refuses the plan rather than hand back one run's document as the plan's.
/// KILLS: one document for every topic (the engine would filter the
/// unrestricted topic too), a shared checkpoint or report path, a topic in
/// two runs.
#[test]
fn different_subsets_are_different_runs_with_their_own_files() {
    let p = plan(&[("orders", &[0, 2]), ("payments", &[1])]);
    let all = render_restore::render_all(&p).unwrap();
    type Row<'a> = (
        String,
        Vec<String>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
    );
    let summary: Vec<Row> = all
        .iter()
        .map(|(run, doc)| {
            (
                run.config_file_name(),
                include(doc),
                line(doc, "source_partitions"),
                line(doc, "checkpoint_state"),
                line(doc, "offset_report"),
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            (
                "restore.run-0.yaml".to_string(),
                vec!["audit".to_string()],
                None,
                Some("/w/run/checkpoint.run-0.json"),
                Some("/w/run/offsets.run-0.json"),
            ),
            // Subsets in ascending order: [0, 2] sorts before [1].
            (
                "restore.run-1.yaml".to_string(),
                vec!["orders".to_string()],
                Some("[0, 2]"),
                Some("/w/run/checkpoint.run-1.json"),
                Some("/w/run/offsets.run-1.json"),
            ),
            (
                "restore.run-2.yaml".to_string(),
                vec!["payments".to_string()],
                Some("[1]"),
                Some("/w/run/checkpoint.run-2.json"),
                Some("/w/run/offsets.run-2.json"),
            ),
        ]
    );
    // The mapping block of each run is that run's slice of the plan's.
    assert!(all[2].1.contains("payments"), "{}", all[2].1);
    assert!(!all[2].1.contains("orders"), "{}", all[2].1);
    assert!(matches!(
        render_restore::render(&p),
        Err(RenderError::MultipleRuns(3))
    ));
    assert!(matches!(
        render_restore::render_and_digest(&p),
        Err(RenderError::MultipleRuns(3))
    ));
    let digests: Vec<String> = render_restore::render_all_and_digest(&p)
        .unwrap()
        .into_iter()
        .map(|(_, _, d)| d)
        .collect();
    assert_eq!(digests.len(), 3);
    assert!(digests[0] != digests[1] && digests[1] != digests[2]);
}

/// A subset keyed by a topic the plan does not map selects nothing and adds
/// no run (plan construction drops such keys; this is the renderer's own
/// belt).
#[test]
fn a_subset_for_an_unmapped_topic_adds_no_run() {
    let p = plan(&[("ghost", &[0])]);
    let runs = render_restore::runs(&p);
    assert_eq!(
        runs,
        vec![RestoreRun {
            index: 0,
            count: 1,
            topic_mapping: p.topic_mapping.clone(),
            source_partitions: None,
        }]
    );
}
