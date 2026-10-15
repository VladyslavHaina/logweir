//! Phase −1's admission guard: the source-side twin of `phase0_admit`, with
//! the same structure and for the same reason — **all local checks first,
//! network second**.
//!
//! A local refusal must not need a reachable broker: a plan this build will
//! never accept is knowable with no cluster, no bucket and no engine, and
//! Global Constraint 11 reserves exit 3 for exactly that. `local` and
//! `network` are therefore separate public functions rather than one
//! sequence, so `backup::run` can run every local check BEFORE it constructs
//! a client at all. On the backup path the cluster in question is the SOURCE
//! — the production one — which is the last cluster a refused plan should
//! open a socket to.
use crate::backup::{BackupError, BackupRunArgs};
use logweir_core::guard::{scan_forbidden_keys, GuardRefusal};
use logweir_core::spec::{AllowedClusters, BackupSpec};
use logweir_kafka::reader::ClusterReader;

#[derive(Debug)]
pub struct Admitted {
    /// Read from the BROKER (`reader.cluster_id()`), never from the spec.
    pub source_cluster_id: String,
}

/// Steps 1–4: everything refusable with no network round trip.
///
/// Returns `BackupError`, not a bare `GuardRefusal`, so the caller's `?` keeps
/// the GC11 mapping in one place.
///
/// `args` is taken so step 4 — interface **I6**'s refusal — can be raised
/// here: it is a property of the FLAGS alone and needs no cluster, no bucket
/// and no engine.
pub fn local(args: &BackupRunArgs, spec: &BackupSpec, spec_text: &str) -> Result<(), BackupError> {
    // 1. `?` on purpose: the scan FAILS CLOSED. A spec text this scanner
    //    cannot parse is a spec it did not scan, and that is a refusal (exit
    //    3), never an empty result silently treated as clean. Same predicate
    //    and refusal wording as phase 0's: Global Constraint 4 extends to the
    //    rendered `backup.yaml`, and the guard refuses on the KEY, never on the
    //    value.
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

    // 1b. An `http://` archive endpoint with `allow_http: false` is refused
    //     here, exit 3, before any client exists. The pinned engine would
    //     otherwise derive plaintext from the scheme; `render_storage_block`'s
    //     refusal is the backstop (exit 1).
    logweir_core::guard::reject_plaintext_endpoint_without_allow_http("storage", &spec.storage)?;

    // 1b'. A region that is not a region name is refused here, exit 3
    //      (`StorageRegionInvalid`), before any store or client exists; without
    //      an endpoint it is the host.
    logweir_core::guard::reject_invalid_storage_region("storage", &spec.storage)?;

    // 1c. SASL/PLAIN without TLS (`refusal-reason=
    //     PlainWithoutTls`) and `mtls` without TLS are refused here, exit 3,
    //     before any client exists — so a PLAIN password never leaves this
    //     process for a source the plan would have dialled in the clear.
    logweir_core::guard::reject_auth_without_required_tls("source.auth", &spec.source.auth)?;
    // 1c'. A bootstrap server holding a control character or a line break is
    //      refused here, exit 3, before any client logs it (FX-43).
    logweir_core::guard::reject_line_breaking_bootstrap_servers(
        "source.bootstrap_servers",
        &spec.source.bootstrap_servers,
    )?;

    // 2. GC18(c) rail 1 / **G-GLOB**, at the SPEC layer.
    //
    //    `render_backup::render` carries the identical call, and that is NOT
    //    where a plan gets refused: a renderer refusal is reached inside
    //    `DataEngine::backup` and is exit 1 with no `refusal-reason=` line
    //    (ruling R-E). A wildcard in a spec is an
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

    // 3a. At most `MAX_BACKUP_TOPICS` names. A backup signs
    //     several blocks per topic in its receipt, and the catalog and the
    //     controller read a receipt under one bound
    //     (`logweir_core::topic_budget`); a selection over the count would
    //     sign evidence neither could read, after the archive exists. Refused
    //     here, exit 3, by name, with no client built.
    if let Some(why) = logweir_core::topic_budget::refuse_topic_count(spec.source.topics.len()) {
        return Err(GuardRefusal(format!(
            "{why} (source.topics). NO backup was taken: this is refused before any client \
             exists"
        ))
        .into());
    }

    // 3b. A declared owner names one of the plan's topics, a
    //     kind the receipt defines and a usable reference — or the plan is
    //     refused here, exit 3, rather than recording an owner the receipt's
    //     arm 18 would refuse after the archive exists, or silently dropping
    //     a declaration that would have routed a restore to desired-state
    //     export. A topic declared twice is refused too: which of the two
    //     the receipt would carry is not the operator's to guess.
    if let Some(why) = logweir_core::topic_configuration::refuse_declarations(
        spec.source.topic_owners.as_deref().unwrap_or(&[]),
        &spec.source.topics,
    ) {
        return Err(GuardRefusal(format!(
            "{why}; a declared owner routes the topic's restore to a desired-state export, \
             so a declaration this build cannot record is refused rather than dropped"
        ))
        .into());
    }

    // The consumer groups whose positions the receipt records —
    // the plan's `source.consumer_groups` and `--consumer-group` together. A
    // selection the receipt could not record one outcome per id for is
    // refused before anything runs, never trimmed.
    if let Some(why) = logweir_core::consumer_positions::refuse_selection(
        &crate::backup::selected_groups(args, spec),
    ) {
        return Err(GuardRefusal(format!(
            "{why} (source.consumer_groups and --consumer-group together); every selected \
             group gets exactly one outcome in the receipt, so a selection this build cannot \
             record is refused rather than trimmed"
        ))
        .into());
    }

    // 4. Interface **I6**'s refusal, LAST among the local checks: two
    //    DIFFERENT paths for the one document this command writes cannot be
    //    honoured. After steps 1–3, because a guard refusal wins over a bad
    //    flag combination (exit 3 on the key, not exit 1 on the flags); inside
    //    the local stage, because a property of the flags is knowable with
    //    zero I/O and must not cost a metadata timeout against the production
    //    source cluster.
    refuse_two_receipt_paths(args)
}

/// Interface **I6** writes ONE document. `--receipt-out` takes precedence over
/// `--out` (`crate::backup::receipt_out_path`), so two flags naming the SAME
/// path is fine and two flags naming DIFFERENT paths is a request this command
/// cannot satisfy: one of the two files would never appear, and an operator
/// would be left looking for evidence at a path nothing wrote.
///
/// Exit 1, not 3: nothing about the PLAN was found wanting, and GC11 reserves
/// 3 for a refused plan.
fn refuse_two_receipt_paths(args: &BackupRunArgs) -> Result<(), BackupError> {
    if let (Some(receipt_out), Some(out)) = (&args.receipt_out, &args.out) {
        if receipt_out != out {
            return Err(BackupError::Operational(format!(
                "--receipt-out {} and --out {} name DIFFERENT paths, and `logweir backup run` \
                 writes exactly one document: the signed backup receipt. --receipt-out would \
                 win and nothing would ever be written to --out. NO backup was taken: this is \
                 refused before the engine is spawned AND before any broker client exists. \
                 Pass one of the two.",
                receipt_out.display(),
                out.display()
            )));
        }
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
    //    `allowed.allowed_cluster_ids` is the
    //    restore-TARGET allowlist: the set of scratch clusters a drill may
    //    restore INTO. A cluster cannot be both the source of an archive and
    //    a permitted scratch target — a scratch cluster is one whose topics a
    //    drill deletes in phase 9 — so an observed source cluster appearing
    //    in that list is refused.
    //
    //    **`allowed.source_cluster_id` is NOT read by the
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

/// The most topic names a refusal names; the rest are counted.
const NAMED_IN_A_REFUSAL: usize = 10;

/// Step 7 (FX-45): **what this principal and this endpoint can do, asked
/// before the engine**, through the reader that admitted the run.
///
/// 0. **Every selected topic exists** (DUI-5). A name DescribeTopics answers
///    `UNKNOWN_TOPIC_OR_PART` for, from a principal DescribeCluster reports
///    Describe on the cluster for ([`logweir_kafka::access::TopicRead::Absent`]),
///    is refused here, exit 3: "topic `x` does not exist on the source".
///    Without the cluster's Describe it stays unknown and the engine answers.
/// 1. **READ on every selected topic.** One DescribeTopics request with the
///    caller's authorized operations (KIP-430). A topic the broker REPORTS
///    this principal may not READ (its operations exclude READ, or it refuses
///    even to describe the topic) is refused here, exit 3, by name and with
///    the grant, instead of failing in the engine with "kafka-backup backup
///    exited 1". A topic whose answer says nothing (a broker that computes no
///    authorized operations, a failed read) is not refused: the engine's own
///    fetch answers it, as before.
/// 2. **`connection.engineProtocol`**, PROD-01.2's capability row: an
///    endpoint SEEN not to serve a request version the engine sends is
///    refused by the row's name. Not observed is a warning, never a refusal.
/// 3. **`connection.groupTypes`**, when the run selects a consumer group: a
///    warning that the receipt will record each group as not captured.
///
/// `connection.topicConfigsReadable` is not asked again here: the run reads
/// every selected topic's configuration itself just before the engine
/// (`config_coverage::observe`), records a refused read as `captureDenied`
/// and warns, which is the row's whole question.
///
/// Before the execution claim, so a refused run consumes no execution id.
pub fn source_capabilities(
    reader: &dyn ClusterReader,
    spec: &BackupSpec,
    groups_selected: bool,
) -> Result<(), BackupError> {
    use crate::check::kinds::capability;
    use logweir_core::check_contract::{CheckId, CheckState};
    use logweir_kafka::access::{ReadDenied, TopicRead};

    let topics = &spec.source.topics;
    match reader.topic_read_access(topics) {
        Ok(reads) => {
            // DUI-5: a NAMED topic the source does not hold, refused by name
            // before the denials: a plan that cannot run at all. It used to
            // reach the engine and fail "exited 1 (operational)" with no cause.
            let absent: Vec<String> = reads
                .iter()
                .filter(|(_, r)| r.is_absent())
                .map(|(t, _)| t.clone())
                .collect();
            if !absent.is_empty() {
                return Err(GuardRefusal(absent_refusal(&absent, topics.len())).into());
            }
            let denied: Vec<&(String, TopicRead)> =
                reads.iter().filter(|(_, r)| r.is_denied()).collect();
            if !denied.is_empty() {
                let hidden = denied
                    .iter()
                    .filter(|(_, r)| *r == TopicRead::Denied(ReadDenied::NotDescribable))
                    .count();
                let mut named: Vec<String> = denied
                    .iter()
                    .take(NAMED_IN_A_REFUSAL)
                    .map(|(t, _)| format!("`{t}`"))
                    .collect();
                if denied.len() > NAMED_IN_A_REFUSAL {
                    named.push(format!("and {} more", denied.len() - NAMED_IN_A_REFUSAL));
                }
                return Err(GuardRefusal(format!(
                    "the backup principal may not READ {} of {} selected topic(s): {} (the \
                     broker's DescribeTopics answer lists this principal's operations on each \
                     topic, and READ is not among them{}), so the engine would fail on its \
                     first fetch. Grant READ on each named topic to the principal this backup \
                     authenticates as (READ implies DESCRIBE). NO backup was taken: this is \
                     refused before the engine is spawned and nothing was written",
                    denied.len(),
                    topics.len(),
                    named.join(", "),
                    if hidden > 0 {
                        format!("; {hidden} of them it may not even Describe")
                    } else {
                        String::new()
                    }
                ))
                .into());
            }
            let unknown = reads
                .iter()
                .filter(|(_, r)| matches!(r, TopicRead::Unknown(_)))
                .count();
            if unknown == 0 {
                tracing::info!(
                    topics = topics.len(),
                    "the broker reports READ for the backup principal on every selected topic"
                );
            } else if let Some((topic, TopicRead::Unknown(why))) = reads
                .iter()
                .find(|(_, r)| matches!(r, TopicRead::Unknown(_)))
            {
                tracing::info!(
                    topic = %topic,
                    unknown,
                    why = %why,
                    "whether the backup principal may READ every selected topic is not known \
                     before the engine; its own fetch will say"
                );
            }
        }
        Err(e) => tracing::warn!(
            error = %e,
            "the broker could not be asked whether the backup principal may READ the selected \
             topics; the engine's own fetch will say"
        ),
    }

    let observed = reader.api_versions(capability::STANDALONE_BUDGET);
    let now = chrono::Utc::now();
    let row = capability::engine_protocol(
        CheckId::ConnectionEngineProtocol,
        &observed,
        capability::spec_uses_sasl(&spec.source.auth),
        now,
    );
    if let Some(why) = capability::refusal_of(&row) {
        return Err(GuardRefusal(format!(
            "{why} NO backup was taken: this is refused before the engine is spawned and \
             nothing was written"
        ))
        .into());
    }
    match row.state {
        CheckState::Ready => tracing::info!(check = %row.id, "{}", row.message),
        _ => tracing::warn!(
            check = %row.id,
            code = %row.code,
            "{} (the backup runs; the engine's own requests will say)",
            row.message
        ),
    }
    if groups_selected {
        let groups = capability::group_types(&observed, now);
        if groups.state == CheckState::NotReady {
            tracing::warn!(check = %groups.id, code = %groups.code, "{}", groups.message);
        }
    }
    Ok(())
}

/// The refusal of selected topics the source does not hold (DUI-5): "topic
/// `x` does not exist on the source", each named (at most
/// [`NAMED_IN_A_REFUSAL`], the rest counted), with the remedy. The names stand
/// in the template's topic position (`topic `x``, `topics `a`, `b``), where
/// the free-text redactor keeps a long legal name whole (F5).
#[must_use]
pub fn absent_refusal(absent: &[String], selected: usize) -> String {
    let mut named: Vec<String> = absent
        .iter()
        .take(NAMED_IN_A_REFUSAL)
        .map(|t| format!("`{t}`"))
        .collect();
    if absent.len() > NAMED_IN_A_REFUSAL {
        named.push(format!("and {} more", absent.len() - NAMED_IN_A_REFUSAL));
    }
    let which = if absent.len() == 1 {
        format!("topic {} does not exist on the source", named.join(", "))
    } else {
        format!(
            "topics {} do not exist on the source ({} of the {selected} selected)",
            named.join(", "),
            absent.len()
        )
    };
    let each = if absent.len() == 1 { "it" } else { "each" };
    format!(
        "{which} (the broker answered UNKNOWN_TOPIC_OR_PARTITION for {each}, and this principal \
         may Describe the cluster). Correct the topic name in the selection, or create the topic \
         before the run. NO backup was taken: this is refused before the engine is spawned and \
         nothing was written"
    )
}

/// Local checks first, network second — the whole guard, in the order GC11's
/// exit-3 contract requires.
pub fn run(
    args: &BackupRunArgs,
    spec: &BackupSpec,
    spec_text: &str,
    allowed: &AllowedClusters,
    reader: &dyn ClusterReader,
) -> Result<Admitted, BackupError> {
    local(args, spec, spec_text)?;
    network(allowed, reader)
}
