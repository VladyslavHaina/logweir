//! Vendored serde shape of the engine's offset-mapping report, the file a
//! restore writes to `restore.offset_report` (FX-23).
//! SOURCE: U/kafka-backup/crates/kafka-backup-core/src/manifest.rs @ tag v0.23.3
//! (`OffsetMapping` :682-706, `OffsetMappingEntry` :1103-1129), written by
//! `restore/engine.rs:1390-1395` (`serde_json::to_string_pretty`).
//! Ported, not linked (Global Constraint 2).
//!
//! # Only the two fields Logweir reads, and NO catch-all (FX-23 review M1)
//!
//! The report carries a `detailed_mappings` section with one
//! `{source_offset, target_offset, timestamp}` object PER RESTORED RECORD
//! (`restore/engine.rs:1903` `add_detailed_batch`, unconditional in the
//! produce path; measured at about 116 bytes of file per record). The other
//! vendored shapes keep a `#[serde(flatten)] extra` map so an upstream
//! addition degrades instead of failing; here that map would MATERIALISE the
//! per-record section — serde buffers every unmatched key and rebuilds it as
//! `Value` — at about 750 bytes of memory per record, enough to OOM-kill a
//! memory-capped runner after a large restore that exited 0. So these structs
//! name exactly the two fields phase 7 needs and nothing else: serde skips
//! every other key (`detailed_mappings` included) with `IgnoredAny`, streaming,
//! which still degrades gracefully on an upstream addition. Read through
//! `serde_json::from_reader`, never into a whole-file buffer
//! (`crate::engine::read_engine_report`).
//! `crates/logweir-engine-oso/tests/offset_report_memory.rs` fails if either
//! the section is held in memory or a catch-all comes back.
use serde::Deserialize;
use std::collections::HashMap;

/// upstream manifest.rs:682-706, `entries` only. `entries` is keyed
/// `"<topic>/<partition>"`, and the topic is the TARGET topic: the restore adds
/// an entry per `(self.target_topic, self.target_partition)`, once for every
/// segment whose time-filtered records are not empty
/// (`restore/engine.rs:1794-1811`).
#[derive(Debug, Clone, Deserialize)]
pub struct OffsetMappingReport {
    #[serde(default)]
    pub entries: HashMap<String, OffsetMappingEntry>,
}

/// upstream manifest.rs:1103-1129, `topic` and `partition` only.
#[derive(Debug, Clone, Deserialize)]
pub struct OffsetMappingEntry {
    pub topic: String,
    pub partition: i32,
}
