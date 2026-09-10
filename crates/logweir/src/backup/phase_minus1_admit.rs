//! Phase −1's admission guard: the source-side twin of `phase0_admit`, with
//! the same structure and for the same reason — **all local checks first,
//! network second** (`phase0_admit.rs:28-43` local, `:94` network).
//!
//! A local refusal must not need a reachable broker: a plan this build will
//! never accept is knowable with no cluster, no bucket and no engine, and
//! Global Constraint 11 reserves exit 3 for exactly that. `local` and
//! `network` are therefore separate public functions rather than one
//! sequence, so `backup::run` can run every local check BEFORE it constructs
//! a client at all. On the backup path the cluster in question is the SOURCE
//! — the production one — which is the last cluster a refused plan should
//! open a socket to.
use crate::backup::BackupError;
use logweir_core::guard::{scan_forbidden_keys, GuardRefusal};
use logweir_core::spec::{AllowedClusters, BackupSpec};
use logweir_kafka::reader::ClusterReader;

#[derive(Debug)]
pub struct Admitted {
    /// Read from the BROKER (`reader.cluster_id()`), never from the spec.
    pub source_cluster_id: String,
}

/// Steps 1–3: everything refusable with no network round trip.
///
/// Returns `BackupError`, not a bare `GuardRefusal`, so the caller's `?` keeps
/// the GC11 mapping in one place.
pub fn local(spec: &BackupSpec, spec_text: &str) -> Result<(), BackupError> {
    // 1. `?` on purpose: the scan FAILS CLOSED. A spec text this scanner
    //    cannot parse is a spec it did not scan, and that is a refusal (exit
    //    3), never an empty result silently treated as clean. Same predicate
    //    (`guard.rs:30`) and same refusal wording as `phase0_admit.rs:31-43`
    //    — Global Constraint 4 extends to the rendered `backup.yaml`, and the
    //    guard refuses on the KEY, never on the value.
    let bad = scan_forbidden_keys(spec_text)?;
    if !bad.is_empty() {
        return Err(GuardRefusal(format!(
            "forbidden key(s) present in the backup spec, at any value: {}. \
             purge_topics is irreversible, absent from the engine's dry run, has no \
             confirmation gate and truncates EVERY partition of each target topic \
             regardless of partition or time-window filters; dry_run would make the \
             restore a no-op and the measured RTO meaningless; header_preflight_external \
             would silently disable the header scan the drill depends on.",
            bad.join(", ")
        ))
        .into());
    }

    // 2. GC18(c) rail 1 / **G-GLOB**, at the SPEC layer.
    //
    //    `render_backup::render` carries the identical call, and that is NOT
    //    where a plan gets refused: a renderer refusal is reached inside
    //    `DataEngine::backup` and is exit 1 with no `refusal-reason=` line
    //    (ruling R-E, which is not reopened here). A wildcard in a spec is an
    //    adopter-written string this build will never accept, knowable with
    //    no broker — so it is refused HERE, where the code is 3.
    //
    //    **G-EXP is checked BEFORE G-GLOB**, in the same order and for the
    //    same reason as the renderers
    //    (`logweir_engine_oso::yaml::reject_dollar_brace`): `${` contains `{`
    //    and `}`, two of `GLOB_METACHARACTERS`, so a glob-first order reports
    //    `orders${X}` as a pattern and sends the operator to escape a brace
    //    instead of to stop an expansion.
    if let Some(entry) = spec.source.topics.iter().find(|t| t.contains("${")) {
        return Err(GuardRefusal(format!(
            "source topic `{entry}` contains `${{`, which the engine expands textually BEFORE \
             the config is parsed [U:crates/kafka-backup-cli/src/commands/config.rs:1-34], \
             replacing an unset name with the empty string behind nothing but a warning. The \
             document logweir hashes would be a TEMPLATE and the document the engine executes \
             its expansion, so the archive would not hold the topic the plan named."
        ))
        .into());
    }
    if let Err(entry) = logweir_core::guard::reject_glob_metacharacters(&spec.source.topics) {
        return Err(GuardRefusal(format!(
            "source topic `{entry}` contains a glob metacharacter (one of {}); the engine's \
             TopicSelection treats include entries as GLOB PATTERNS \
             [U/kafka-backup/crates/kafka-backup-core/src/config.rs:334-343], so one named \
             entry would silently widen to a set and the backup would capture topics the plan \
             never named. GC18(c) rail 1 is a mandatory named-topic allowlist with no \
             wildcard; if the topic is genuinely called that, this build cannot back it up.",
            logweir_core::guard::GLOB_METACHARACTERS
                .iter()
                .collect::<String>()
        ))
        .into());
    }

    // 3. A mandatory allowlist whose absence means "all topics" is not an
    //    allowlist (`BackupSourceSpec::topics`' own note). `serde` makes the
    //    field required, so this refuses the EMPTY list — the shape a
    //    templating mistake produces, and the one shape the engine would read
    //    as "everything".
    if spec.source.topics.is_empty() {
        return Err(GuardRefusal(
            "a backup spec must name at least one topic; GC18(c) requires a mandatory \
             named-topic allowlist"
                .to_string(),
        )
        .into());
    }

    Ok(())
}

/// Steps 4–5: the network read, and GC18(c) rail 4.
///
/// FROM HERE ON every failure reaches the network. A `KafkaError` means the
/// guard could not OBSERVE the fact it needed — it is not a refusal, and `?`
/// converts it into `BackupError::Kafka` (exit 1), never
/// `BackupError::Guard` (exit 3).
pub fn network(
    allowed: &AllowedClusters,
    reader: &dyn ClusterReader,
) -> Result<Admitted, BackupError> {
    // 4. The SOURCE cluster id, read from the BROKER and never from the spec.
    //    `BackupPlan` deliberately carries no `source_cluster_id` field for
    //    this reason (see its doc comment): an adopter-supplied string
    //    standing where a measured fact belongs is the same class of defect
    //    as letting a spec widen its own cluster allowlist.
    let source_cluster_id = reader.cluster_id()?;

    // 5. **GC18(c) rail 4, stated once.**
    //
    //    `allowed.allowed_cluster_ids` (`logweir-core/src/spec.rs:333`) is the
    //    restore-TARGET allowlist: the set of scratch clusters a drill may
    //    restore INTO. A cluster cannot be both the source of an archive and
    //    a permitted scratch target — a scratch cluster is one whose topics a
    //    drill deletes in phase 9 — so an observed source cluster appearing
    //    in that list is refused.
    //
    //    **`allowed.source_cluster_id` (`spec.rs:336`) is NOT read by the
    //    backup path.** Do not look for it here. That field is the drill's:
    //    phase 0 uses it to refuse a TARGET that equals the source, and it is
    //    `Option<String>` with `#[serde(default)]`, so an allowlist file that
    //    omits it (the ordinary case) would make a rail written over it
    //    refuse nothing at all.
    if allowed.allowed_cluster_ids.contains(&source_cluster_id) {
        return Err(GuardRefusal(format!(
            "source cluster id {source_cluster_id} is also listed in allowedClusterIds, which \
             is the restore-TARGET allowlist; a cluster cannot be both the source of an \
             archive and a permitted scratch target"
        ))
        .into());
    }

    // 6. No topic creation, no offset commit — GC18(c) rail 2. Nothing to
    //    assert here, because the property is STRUCTURAL and lives in
    //    `backup::run`: no scratch namespace is ever configured on that
    //    reader, so the deleting method refuses every name it is handed
    //    (`crates/logweir-kafka/src/rdkafka_reader.rs:446-460`), while the
    //    consumer keeps `enable.auto.commit = false` (`:81-82`) and
    //    `allow.auto.create.topics = false` (`:45`). A `ClusterReader` cannot
    //    create or commit anything either: the trait has no method that
    //    could. (Identifiers omitted on purpose — see `backup`'s module doc,
    //    rail 2: `the_backup_path_never_scopes_a_deleter` reads this file's
    //    raw source text.)
    Ok(Admitted { source_cluster_id })
}

/// Local checks first, network second — the whole guard, in the order GC11's
/// exit-3 contract requires.
pub fn run(
    spec: &BackupSpec,
    spec_text: &str,
    allowed: &AllowedClusters,
    reader: &dyn ClusterReader,
) -> Result<Admitted, BackupError> {
    local(spec, spec_text)?;
    network(allowed, reader)
}
