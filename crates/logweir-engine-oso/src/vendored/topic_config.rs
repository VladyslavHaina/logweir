//! What the pinned engine CAPTURES of a topic's configuration — the filter a
//! backup's configuration coverage is checked against (FX-4).
//!
//! SOURCE: U/kafka-backup/crates/kafka-backup-core/src/backup/engine.rs @ tag
//! v0.21.0 (`third_party/kafka-backup-v0.21.0.tar.gz`, sha256
//! `0252a83735148331c16d7c4e737a41f099c0f52eda5d7a66db75b8848ddc405b`):
//! `capture_topic_configs` at `:686-721` keeps an entry when
//! `entry.is_topic_override() && !entry.read_only && !entry.is_sensitive &&
//! is_recovery_topic_config(&entry.name)` and its value is present
//! (`:703-716`); `is_recovery_topic_config` is the allowlist at `:1099-1127`;
//! `is_topic_override` is `config_source == 1`, Kafka's
//! `DYNAMIC_TOPIC_CONFIG` (`kafka/admin.rs:90-92`).
//!
//! Ported, not linked (Global Constraint 2), and NOT a serde shape, so the
//! xtask gate pairs it as a LIST (`LIST_CHECKS` in `xtask/src/main.rs`): the
//! allowlist below must name exactly the keys of the pinned tarball's
//! `is_recovery_topic_config`, on every `cargo test --workspace`, and
//! `cargo xtask sync-upstream --tag <new>` reports a key a new engine adds or
//! drops before the pin moves. The PREDICATE ([`engine_captures`]) is not
//! text the gate can compare: a pin bump re-reads `capture_topic_configs`.
//! `the_allowlist_is_the_pinned_engines_twenty_four_keys` pins the list as
//! data, so an edit here is a visible diff.
//!
//! # Why Logweir needs the engine's filter at all
//!
//! The engine's capture is non-fatal and all-or-nothing (`backup/engine.rs:
//! 382-393`; `kafka/admin.rs:476-487` fails the WHOLE DescribeConfigs call on
//! the first per-resource error), and its manifest spells "captured, no
//! overrides" and "not captured" identically: `configurations: {}`. So the
//! backup runner reads each topic's configuration ITSELF and compares what
//! the engine WOULD have kept with what the manifest DOES hold. Equal means
//! the manifest's record is complete; anything else means it is not.

/// The pinned engine's `is_recovery_topic_config`, verbatim and in its order.
///
/// It still names keys Kafka 4.0 removed (`message.format.version`,
/// `message.downconversion.enable`, `message.timestamp.difference.max.ms`) —
/// PROD-05.1's portability table is where that matters; for coverage it only
/// means those keys can never be present on a 4.x broker to disagree about.
pub const RECOVERY_TOPIC_CONFIG_KEYS: [&str; 24] = [
    "cleanup.policy",
    "compression.type",
    "delete.retention.ms",
    "file.delete.delay.ms",
    "flush.messages",
    "flush.ms",
    "index.interval.bytes",
    "max.compaction.lag.ms",
    "max.message.bytes",
    "message.downconversion.enable",
    "message.format.version",
    "message.timestamp.difference.max.ms",
    "message.timestamp.type",
    "min.cleanable.dirty.ratio",
    "min.compaction.lag.ms",
    "min.insync.replicas",
    "preallocate",
    "retention.bytes",
    "retention.ms",
    "segment.bytes",
    "segment.index.bytes",
    "segment.jitter.ms",
    "segment.ms",
    "unclean.leader.election.enable",
];

/// The engine's own predicate: would `capture_topic_configs` keep this entry?
///
/// `is_topic_override` is the entry's source being `DYNAMIC_TOPIC_CONFIG`;
/// the caller maps its client's source vocabulary to that one boolean, so this
/// crate stays free of any Kafka client type.
#[must_use]
pub fn engine_captures(
    name: &str,
    value_present: bool,
    is_topic_override: bool,
    read_only: bool,
    sensitive: bool,
) -> bool {
    value_present
        && is_topic_override
        && !read_only
        && !sensitive
        && RECOVERY_TOPIC_CONFIG_KEYS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The list as DATA: an edit is a diff a reviewer reads against the
    /// pinned source's `:1099-1127`.
    #[test]
    fn the_allowlist_is_the_pinned_engines_twenty_four_keys() {
        assert_eq!(RECOVERY_TOPIC_CONFIG_KEYS.len(), 24);
        let mut sorted = RECOVERY_TOPIC_CONFIG_KEYS.to_vec();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            RECOVERY_TOPIC_CONFIG_KEYS.to_vec(),
            "upstream lists them alphabetically; so does this copy"
        );
        assert!(RECOVERY_TOPIC_CONFIG_KEYS.contains(&"message.timestamp.type"));
        assert!(!RECOVERY_TOPIC_CONFIG_KEYS.contains(&"local.retention.ms"));
    }

    #[test]
    fn engine_captures_only_present_non_sensitive_writable_topic_overrides_it_allowlists() {
        assert!(engine_captures("retention.ms", true, true, false, false));
        assert!(
            !engine_captures("retention.ms", false, true, false, false),
            "no value"
        );
        assert!(
            !engine_captures("retention.ms", true, false, false, false),
            "a broker default"
        );
        assert!(
            !engine_captures("retention.ms", true, true, true, false),
            "read-only"
        );
        assert!(
            !engine_captures("retention.ms", true, true, false, true),
            "sensitive"
        );
        assert!(
            !engine_captures("local.retention.ms", true, true, false, false),
            "not allowlisted"
        );
    }
}
