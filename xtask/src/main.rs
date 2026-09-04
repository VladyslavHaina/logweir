//! cargo xtask sync-upstream --tag v0.21.0 --upstream /path/to/kafka-backup
//!
//! For each vendored file, extract the `SOURCE:` header, read the named
//! upstream file, extract every `pub struct` / `pub enum` block named in the
//! vendored file, and compare the FIELD NAME SETS. A field added upstream is a
//! warning (serde(default) + the catch-all absorb it); a field REMOVED or
//! RETYPED upstream is an error, because our reader may be depending on it.
use std::collections::BTreeSet;

fn field_names(src: &str, item: &str) -> Option<BTreeSet<String>> {
    let start = src
        .find(&format!("pub struct {item} "))
        .or_else(|| src.find(&format!("pub enum {item} ")))?;
    let body = &src[start..];
    let open = body.find('{')?;
    let mut depth = 0usize;
    let mut end = open;
    for (i, c) in body[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = open + i;
                    break;
                }
            }
            _ => {}
        }
    }
    Some(
        body[open + 1..end]
            .lines()
            .filter_map(|l| l.trim().strip_prefix("pub "))
            .filter_map(|l| l.split(':').next())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let tag = args
        .iter()
        .position(|a| a == "--tag")
        .map(|i| args[i + 1].clone())
        .expect("--tag");
    let up = args
        .iter()
        .position(|a| a == "--upstream")
        .map(|i| args[i + 1].clone())
        .expect("--upstream");
    let mut failed = false;
    for (vend, upstream_rel, items) in [
        (
            "crates/logweir-engine-oso/src/vendored/manifest.rs",
            "crates/kafka-backup-core/src/manifest.rs",
            vec![
                "BackupManifest",
                "TopicBackup",
                "PartitionBackup",
                "SegmentMetadata",
                "OffsetGap",
                "PrunedRange",
                "DryRunReport",
                "DryRunTopicReport",
            ],
        ),
        (
            "crates/logweir-engine-oso/src/vendored/preflight.rs",
            "crates/kafka-backup-core/src/restore/preflight.rs",
            vec!["HeaderPreflightReport", "PartitionHeaderCoverage"],
        ),
    ] {
        let ours = std::fs::read_to_string(vend).unwrap();
        let theirs = std::fs::read_to_string(format!("{up}/{upstream_rel}")).unwrap();
        for item in items {
            let a = field_names(&ours, item).unwrap_or_default();
            let b = field_names(&theirs, item).unwrap_or_default();
            for gone in b.difference(&a) {
                println!("note   {tag} {item}: upstream has `{gone}`, we do not (added upstream)");
            }
            for extra in a.difference(&b) {
                if extra == "extra" {
                    continue;
                }
                println!("DRIFT  {tag} {item}: we read `{extra}`, upstream no longer declares it");
                failed = true;
            }
        }
    }
    if failed {
        eprintln!(
            "vendored structs have drifted from {tag}. Update them, bump the pin, \
                   and re-run the engine-matrix job before releasing."
        );
        std::process::exit(1);
    }
    println!("vendored structs agree with {tag}");
}
