# Kafka recovery product expansion

Research date: 2026-09-14. Logweir baseline: main revision 92e02097540c39ff8565283a38ee592499b95020.

Status: proposed backlog; none of these features is declared implemented by this document. This file owns market-informed expansion. The [platform improvements tracker](platform-improvements.md) owns the existing operator, UI, discovery, onboarding, authorization, catalog and retention corrections. Complete the relevant foundation there instead of implementing it twice.

Execution order confirmed by the user on 2026-09-14: complete the entire platform improvements tracker first, then begin this expansion. Astra is orchestration only; implementation, testing and technical reviews use GPT-5.6 Sol or Terra according to complexity. Spark may handle small edits where the worker tooling supports it. No expansion task is currently dispatched.

## Product direction

Make Logweir an approachable, independently verifiable Kafka recovery tool: select what to protect, see the latest usable recovery point, restore into a safe target, and know whether applications can resume. Start with self-hosted Kafka operators and application teams. Offer shared enterprise operation after authorization and recovery correctness are established; make hosted fleet management optional.

A mature product needs several distinct promises. Topic data recovery, application recovery, cross-cluster migration, and complete environment recovery are different capabilities. An all-user-topics selector must not imply that consumer state, schemas, ACLs, transactions or external databases were captured. Likewise, a signature authenticates evidence; it does not make sampled verification exhaustive.

The existing Rust core, Kubernetes operator, isolated execution Jobs and static frontend remain useful. Add bounded product APIs and a durable recovery catalog before considering a frontend framework migration or mandatory database. Continuous capture may need long-lived workers, but finite restores should retain isolated execution and immutable plans. Reuse saved connection definitions and credential references; do not attempt to share a TCP connection across unrelated pods.

## Research findings and limitations

The comparison uses official product documentation, release notes and maintainer repositories. Kannika documentation displayed version 0.18.0 when accessed. Other moving documentation is identified in the source register. These are documented capabilities, not results of deploying or benchmarking competitors. A missing feature in the reviewed pages means unverified, not necessarily absent. Marketing statements about near-zero loss, recovery speed, compliance or universal compatibility are not adopted as Logweir guarantees.

### Kannika: useful patterns and important distinctions

| Area | Documented observation | Decision for Logweir |
| --- | --- | --- |
| Protection model | The operator manages streaming backup workers, source/storage references, topic selectors and pause controls. Selectors can discover future topics. [Backup documentation](https://docs.kannika.io/user-guide/backup/) | First deliver the foundational discovery workflow; then investigate continuous protection in PROD-02. |
| Incident workflow | Restores support drafts, topic renaming, partition/offset ranges, time filtering and linkage to a backup. The documented preflight list is narrower than a complete recovery-readiness assessment. [Restore documentation](https://docs.kannika.io/user-guide/restore/) | Reuse the platform restore wizard; add advanced selection without exposing technical settings to every operator. |
| Consumer positions | The product page describes offset handling, but the technical FAQ says the Restore itself does not migrate consumer groups. It directs users to a companion tool. [Product](https://www.kannika.io/product/), [consumer-group FAQ](https://docs.kannika.io/faq/restored-consumer-groups/) | Integrate capture, mapping, review and application into one durable recovery workflow, rather than leaving an incident-time script gap. |
| Offset translation | The companion kbridge separates fetching committed positions, calculating target positions and applying them. Original-offset headers are required for its mapping path; it can also read a backed-up offsets topic restored under a normal name. [kbridge](https://github.com/kannika-io/kbridge) | PROD-04 needs an archived snapshot that survives source loss and an explicit, auditable cutover. Do not equate source and target offsets. |
| Schema recovery | Separate registry backup/restore resources are documented. Registry restore can change versions, repairs references, and supports an import mode with collision risk; the documented restore currently imports all stored schemas. [Registry backup](https://docs.kannika.io/user-guide/schema-registry-backup/), [registry restore](https://docs.kannika.io/user-guide/schema-registry-restore/) | PROD-03 should offer dependency-aware selection and collision preview without modifying the original archive. |
| Schema mapping | Mapping uses a lookup table; the documented SAME generator supports Avro. Missing mappings can leave record data unchanged. [Schema mapping](https://docs.kannika.io/user-guide/restore/schema-mapping/) | Validate every required mapping before replay; explicitly qualify supported serialization formats. |
| Interrupted recovery | Restore progress is persisted on a Kubernetes volume, with lifecycle tied to the Restore. [Restore report](https://docs.kannika.io/user-guide/restore/report/) | PROD-07 should define crash semantics and preserve durable progress beyond a disposable worker, while avoiding unsupported exactly-once claims. |
| Topic recreation | Snapshot support distinguishes topic generations through UUIDs; it is documented as experimental and has compatibility restrictions for existing backup layouts. [Snapshots](https://docs.kannika.io/user-guide/backup/snapshots/) | Treat topic identity and name reuse as a correctness requirement in continuous capture, not merely a UI version picker. |
| Retention | Keep/Delete policies and a running segment reaper are documented. A stopped backup does not physically reap expired segments; one-off console deletion is described as future work. [Data retention](https://docs.kannika.io/user-guide/backup/data-retention/) | Keep expiration eligibility, physical deletion, holds and active-restore protection distinct in the platform retention work. |
| Monitoring | A lag monitor queries source offsets to calculate backup progress. [Lag monitor](https://docs.kannika.io/user-guide/backup/lag-monitor/) | Measure durable archived progress separately from consumed progress. RPO must reflect data recoverable after worker loss. |
| Authentication | OIDC login and API token validation are documented. This security page does not establish a complete per-resource viewer/operator/approver authorization model. [Security](https://docs.kannika.io/installation/configuration/security/) | Follow the platform API/role work; do not infer authorization merely from SSO. |
| Recent correctness work | Release 0.18.0 documents connection-test/custom-CA alignment, discovery error reporting and segment rollover fixes affecting durability. [Release notes](https://docs.kannika.io/release-notes/0-18-0/) | Exercise credentials from the actual runner context and test steady low-volume streams as well as high throughput. |
| Packaging and adoption | The pricing page describes broker-based licensing, all storage targets and unrestricted restore counts; it does not provide a universal numeric price. [Pricing](https://www.kannika.io/pricing/) | Keep recovery accessible during incidents. Evaluate transparent operating costs and optional commercial support without introducing emergency restore gates. |

The strongest opportunity is a complete recovery journey with explicit coverage and evidence. Copying a long feature list would leave the same operational gaps. In particular, independently stored data must remain discoverable after loss of the original Kubernetes installation, and application recovery must report exactly which dependencies were recovered.

### Other recovery approaches

| Tool/approach | Documented strengths | Boundaries that matter for Logweir |
| --- | --- | --- |
| Confluent Cluster Linking | Live mirror topics, optional consumer-offset and ACL synchronization, and promote/failover operations. Registry availability needs Schema Linking or another compatible schema access/recovery strategy. [Disaster recovery guide](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.html) | Synchronization is asynchronous; applications still need endpoint/credential changes. Kafka Streams/ksqlDB state does not automatically fail over. This is useful continuity machinery, not evidence of an independently retained historical backup catalog. |
| Confluent scope restrictions | Supports external Kafka sources into supported Confluent Cloud destinations. [Features and limitations](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html) | Transactional/exactly-once mirror-topic workloads and share-group state synchronization are documented as unsupported. ACL synchronization is constrained by organization and prefixing. The documented lag caveat for unavailable/paused sources reinforces that unknown must not appear as zero lag. |
| Amazon MSK Replicator | Managed asynchronous record replication with scaling and selected topic configuration, ACL and group-offset synchronization; current documentation also covers self-managed Kafka sources into MSK Provisioned. [Overview](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator.html) | Both source and destination clusters are part of the replication setup. MSK-to-MSK replication requires the same AWS account. Do not describe this as a universal historical-backup mechanism or rely on older claims that external sources are unsupported. |
| MSK recovery metadata | Translates offsets, with enhanced bidirectional synchronization for eligible two-direction deployments. [Offset synchronization](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-bidirectional-offset-sync.html) | Translation can favor replay over skipping and does not overwrite active destination groups. ACL copying is selective: literal topic ACLs, not all prefixed/resource policies or IAM configuration. Schema Registry protection was not established in these pages. [Metadata and ACLs](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-metadata-acl.html) |
| MSK cutover | Documents planned exercises and unplanned failover/failback. [Planned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-planned-failover.html), [unplanned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-unplanned-failover.html) | Clients still require coordinated stopping/restarting and target configuration. Unplanned failover may lose unreplicated data; reverse replication must include writes made during the outage. Make cutover a distinct reviewed stage in Logweir. |
| Redpanda Whole Cluster Restore | An enterprise feature restores archived data and metadata into a new cluster, including topic definitions, users, ACLs, consumer offsets and schemas when the schema topic was archived. [Whole Cluster Restore](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/whole-cluster-restore/) | Documentation explicitly disclaims snapshot consistency and atomic committed transactions. Topics without archived data return empty; in-flight transactions are aborted and offsets may be adjusted to restored coverage. Some infrastructure configuration remains outside recovery. |
| Redpanda topic recovery / Tiered Storage | Individual-topic recovery uses object storage. [Topic recovery](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/topic-recovery/) | Recovery imposes bucket-writer and operational constraints. Tiered data remains subject to retention, so object-store location alone does not establish an independent historical copy. [Retention behavior](https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/disk-utilization/) |

**Inference from the comparison:** Logweir should complement replication with independent recovery history, make metadata coverage visible and prove application cutover. It should not compete by promising stronger transactional or zero-loss guarantees than its capture and replay protocols can support. The market-informed spikes below are Logweir design proposals, not claims that any comparator implements them in the same way.

**Open research questions:** no comparative throughput benchmark, commercial trial or independent security assessment was performed. Kannika's exact UI role granularity, all serializer combinations and end-to-end transaction semantics remain unverified. Provider-specific auth and administration differences require the capability tests in PROD-01; local Kafka cannot certify them. Pricing/edition constraints should be rechecked at procurement time. This backlog does not assume competitor archive formats are mutually readable.

## Roadmap and dispatch rules

P1 means essential to a credible application-recovery product after foundational safety fixes. P2 means valuable expansion with a bounded first release. P3 means optional strategic work. Priority is not permission to skip dependencies. Every spike and task starts **Proposed**, with no assigned owner and no completion evidence.

| Spike | Priority | Deliverable | Initial dispatch |
| --- | --- | --- | --- |
| PROD-01 | P1 | Tested recovery guarantees and provider compatibility | Research task can start now |
| PROD-02 | P1 | Continuous capture with honest coverage and topic generations | After semantic contract and foundational catalog/discovery |
| PROD-03 | P1 | Schema Registry recovery and validated mappings | After archive metadata contract |
| PROD-04 | P1 | Consumer position capture and safe cutover | After offset semantics contract |
| PROD-05 | P1 | Topic configuration, ACL and quota recovery | After compatibility contract |
| PROD-06 | P2 | Stateful application recovery profiles | After schemas, offsets and configuration |
| PROD-07 | P1 | Durable restore checkpoints and controlled resume | Research can start; implementation follows findings |
| PROD-08 | P1 | Deeper verification and application recovery exercises | After truthful baseline evidence and drill scheduling |
| PROD-09 | P2 | Independent protected archive copies and storage adapters | After saved destinations, catalog and retention |
| PROD-10 | P2 | Throughput controls, capacity guidance and cost visibility | Benchmark existing engine first |
| PROD-11 | P2 | Safe environment cloning and selected-data replay | After stable restore selection and schemas |
| PROD-12 | P2 | Reviewed migration and cutover orchestration | After continuous capture, offsets and application verification |
| PROD-13 | P3 | Optional fleet and customer-hosted execution model | Design only until local product is dependable |
| PROD-14 | P2 | Accessible distribution and public integration contracts | Documentation/design can start now |

### Foundation dependency map

The named PLAT tasks live in the [single platform tracker](platform-improvements.md). These mappings identify prerequisites without creating another implementation of the same feature.

| Expansion | Required platform capabilities |
| --- | --- |
| PROD-01 | PLAT-03 readiness and PLAT-20 verification/documentation baseline |
| PROD-02 | PLAT-07 connections, PLAT-09 discovery, PLAT-14 status, PLAT-15 catalog |
| PROD-03 | PLAT-07/08 saved connections/destinations, PLAT-11 selection, PLAT-15 catalog |
| PROD-04 | PLAT-01 approval binding, PLAT-03 preflight, PLAT-11/12 restore identity and submission, PLAT-19 policy |
| PROD-05 | PLAT-03 preflight, PLAT-07 connection contract, PLAT-11 restore review |
| PROD-06 | PLAT-09 explicit coverage and PLAT-15 catalog, plus the listed metadata expansions |
| PROD-07 | PLAT-12 execution identity, PLAT-14 progress, PLAT-16 cleanup/retention |
| PROD-08 | PLAT-14.3 recurring drills and PLAT-20 truthful verification baseline |
| PROD-09 | PLAT-02 signing lifecycle, PLAT-08 destinations, PLAT-15 catalog, PLAT-16 retention |
| PROD-10 | PLAT-14 metrics/status, PLAT-18 bounded UI lists, PLAT-20 performance baseline |
| PROD-11 | PLAT-11 selected recovery point, PLAT-13 draft safety, PLAT-17 authorization |
| PROD-12 | PLAT-01/19 approvals, PLAT-03 preflight, PLAT-12/14 durable operations |
| PROD-13 | PLAT-17 API/identity/audit and PLAT-19 policy |
| PROD-14 | PLAT-02/03 onboarding, PLAT-15 archive import, PLAT-17 API, PLAT-20 docs/releases |

The map lists capabilities that must exist before the associated feature is shipped. Research, fixtures and contract decisions can proceed sooner when the individual task explicitly permits it. Within a task, the more specific dependency list defines its start condition.

### Rules inherited by every task

1. Read the named spike, dependencies and the platform tracker. Recheck current implementation before coding; this is a dated baseline. Record owner, status, dependency evidence and the exact bounded scope in the task's completion record in this file.
2. A research task ends with a decision, measured evidence, limits and a concrete contract recorded under its task. It may conclude a capability is unsupported. It must not silently expand into a rewrite. A dependent implementation task remains blocked until that contract is resolved; split it into narrower child tasks here if findings require multiple independent changes.
3. Preserve existing archives and immutable plans. Version new manifest/API fields, define absent-field behavior and prove old readers either remain compatible or reject clearly. Never reinterpret old evidence as a stronger guarantee.
4. Keep credentials server-side, scope source/archive/target permissions separately, and record actor, selected recovery point and execution identity. Use platform authorization and approval policy. Archive signatures do not replace encryption or access control.
5. Every mutation task includes failure handling, visible UI/API states, idempotency and bounded cleanup. Default to new target names; do not overwrite production topics or change live consumer positions implicitly.
6. All Kubernetes deployments and E2E runs must explicitly target **docker-desktop**. Use disposable local fixtures. Do not deploy to the company EKS or another context. Local emulation does not certify a hosted provider: record cloud-specific validation as unverified until separately authorized evidence exists.
7. Apply the task-specific tests below plus relevant existing regressions. Store reproducible commands, versions, fixture sizes, actual outcomes and limitations in the completion record. Do not add redundant CI gates for every spike; extend the shared test suite and use focused/manual matrices for expensive scenarios.
8. A task is Done only when acceptance criteria pass, compatibility/migration notes are updated, focused review is resolved and operator-facing documentation matches the behavior. Planning documents themselves do not satisfy implementation tasks.

For an AI-agent handoff, name one task ID, include its parent spike and these rules, assign ownership of the affected logical components, and tell the agent not to revert concurrent work. The agent should implement only that task, run its tests, and update its completion record with changed areas, evidence, remaining limits and follow-up IDs. No task authorizes a production rollout.

## PROD-01 — Establish the recovery contract

**Priority/status:** P1 / Proposed. **Owner areas:** engine adapter, archive format, verification, support matrix. **Dependencies:** none for research; reuse the platform's truthful coverage presentation. **Boundary:** establish supported semantics, not a new backup engine. **Risk/migration:** historical archives may lack evidence; classify them explicitly instead of inventing fields.

### PROD-01.1 — Prove record and transaction behavior

- **Issue:** Current sampled fingerprints do not establish ordering, transaction boundaries, crash duplication or exhaustive recovery. Kafka transactions have explicit producer/consumer semantics that ordinary replay cannot automatically recreate. [Kafka design](https://kafka.apache.org/40/design/design/)
- **Approach:** Inspect the pinned engine and run a local semantic fixture covering partition order, keys, nulls, headers, timestamps, committed/aborted transactions, topic recreation and compaction. Define supported guarantees separately for capture, replay and verification; identify what the archive cannot represent.
- **Acceptance:** A capability contract states each guarantee, known counterexample, isolation setting and archive prerequisite. Unsupported transactional or exactly-once recovery is blocked from product claims. The decision identifies whether adapter changes suffice or engine work is required.
- **Tests/evidence:** Produce deterministic records across several partitions, interrupt capture and replay around acknowledgements, and compare committed input with observed output. Include equal/nonmonotonic timestamps, duplicate headers and tombstones. Record actual outcomes, not only engine exit status.
- **Dependencies:** None. **Handoff:** semantic matrix, fixtures, unresolved engine constraints and decisions for PROD-02, PROD-04 and PROD-07.

### PROD-01.2 — Publish a tested compatibility contract

- **Issue:** Kafka-compatible endpoints vary in authentication, metadata permissions and supported administrative operations; a connection test alone cannot certify recovery.
- **Approach:** Define capability detection and a support matrix by broker/version, auth mode, registry and archive backend. Start with local Apache Kafka and S3-compatible storage; add explicit entries for MSK, Confluent and Redpanda as unverified until tested. Classify supported, limited, untested and unsupported behavior.
- **Acceptance:** Preflight exposes missing capabilities with an actionable fallback; unsupported metadata cannot appear captured. Each supported entry has versioned evidence and a minimum-permission profile for probe, backup and restore.
- **Tests/evidence:** Local SCRAM, TLS/private CA, certificate rotation, ACL-denied listing/configuration and broker advertised-address failures; adapter contract tests for cloud auth expiry. Cloud-specific results remain separate from mocks.
- **Dependencies:** PROD-01.1. **Handoff:** capability identifiers, fixture matrix, supported release boundaries and provider validation gaps.

## PROD-02 — Continuous protection and recoverable history

**Priority/status:** P1 / Proposed. **Owner areas:** capture engine, worker lifecycle, archive catalog. **Dependencies:** PROD-01; platform discovery and durable catalog. **Boundary:** opt-in streaming mode alongside scheduled backups, not a cluster-consistent snapshot claim. **Risk/migration:** independent topic generations and append-only manifest versions must preserve older archives.

### PROD-02.1 — Define durable capture and generation boundaries

- **Issue:** Scheduled capture leaves intervals before newer data is durably archived; permanent gaps depend on source retention and capture semantics. Consumed data is not necessarily safely stored. A recreated topic can reuse names and offsets from an older topic.
- **Approach:** Define partition ownership/fencing, durable offset checkpoints, segment commit protocol, topic UUID/generation identity, low-volume rollover and source-retention gap detection. Model consumed, committed-to-storage and verified watermarks separately. Decide whether the pinned engine supports the required protocol before selecting a long-lived workload.
- **Acceptance:** The design has a crash table for every archive publication boundary, a bounded takeover process and an explicit response when UUIDs are unavailable. No committed catalog entry references an unfinished object; name reuse cannot merge unrelated histories.
- **Tests/evidence:** Fault-inject between upload and manifest publication; recreate a topic, expire source data, lose ownership, and send a steady trickle that never goes idle. Demonstrate recoverable data after each interruption.
- **Dependencies:** PROD-01.1 and foundational catalog contract. **Handoff:** versioned capture/checkpoint protocol and go/no-go decision for implementation.

### PROD-02.2 — Ship streaming protection with a coverage timeline

- **Issue:** A running worker can look healthy while archived data is stale or incomplete.
- **Approach:** Implement the approved protocol, per-partition progress, bounded backpressure, graceful pause/resume and coverage intervals in the existing catalog. Add an opt-in continuous mode, gap explanations and recovery-point selection restricted to committed coverage. Resolve future topics using the foundational selector policy.
- **Acceptance:** Worker loss does not lose acknowledged durable coverage. The UI differentiates unknown, lagging and protected states; selected historical points remain stable. An RPO target is configurable and compared with measured durable progress, without treating record event time as a reliable wall clock.
- **Tests/evidence:** Broker/storage outages, rolling worker restart, delayed partition, new topic, clock skew, topic-generation switch and upgrade from scheduled-only installations. Restore from committed history with source offline.
- **Dependencies:** PROD-02.1; platform discovery, readiness, catalog and status refresh. **Handoff:** operating limits, metrics, recovery evidence and rollback behavior.

## PROD-03 — Recover schemas with the data

**Priority/status:** P1 / Proposed. **Owner areas:** registry adapter, archive metadata, restore planner. **Dependencies:** PROD-01 and foundational saved connections/catalog. **Boundary:** begin with one explicitly supported Confluent-compatible API and Avro; other formats require separate compatibility evidence. **Risk/migration:** schema import is a target mutation; default to non-destructive mapping, never silently overwrite IDs.

### PROD-03.1 — Capture a usable registry dependency set

- **Issue:** Archived record bytes can be unreadable when referenced schemas or versions disappear.
- **Approach:** Add a saved registry connection with server-side credentials. Capture subjects, versions, IDs, references, compatibility settings and deletion state exposed by the selected API. Link an immutable registry snapshot and observation window to each recovery point. Report missing permissions or unresolved references as incomplete coverage.
- **Acceptance:** Required schemas and transitive dependencies can be discovered without the source registry; the backup lists unsupported registry metadata. Topic-to-subject association is explicit and does not assume every installation uses the default naming strategy.
- **Tests/evidence:** Multiple subjects sharing a schema, references, version changes during capture, deleted subjects, cyclic/invalid references, auth expiry and partial API failure. Verify archive integrity and old archives without registry metadata.
- **Dependencies:** PROD-01.2. **Handoff:** registry snapshot format, naming-strategy behavior and completeness contract.

### PROD-03.2 — Preview and execute schema-aware recovery

- **Issue:** Destination schema IDs and subject names can differ, causing restored applications to fail even when record counts match.
- **Approach:** Plan dependency-ordered selected schema import and key/value ID mapping before record replay. Preview conflicts and supported transformations; default to rejection when a required mapping is missing. Preserve nulls and unsupported raw payloads according to an explicit selected mode. Include transformation identity in evidence.
- **Acceptance:** A real application deserializes restored Avro records against the target registry with different IDs. Unmapped required schemas block before data writes. Subject subset recovery does not require deleting archive contents. Any privileged import mode requires separate collision review.
- **Tests/evidence:** Key and value schemas, null values, shared/reference schemas, incompatible existing subjects, retry after partial import and wrong serialization declaration. Compare semantic values and prove unchanged fields remain intact.
- **Dependencies:** PROD-03.1 and platform preflight/restore selection. **Handoff:** conflict policy, mapping evidence, target changes and format limitations.

## PROD-04 — Restore consumer positions and guide cutover

**Priority/status:** P1 / Proposed. **Owner areas:** Kafka metadata adapter, archive metadata, restore/cutover workflow. **Dependencies:** PROD-01; stable selected recovery points. **Boundary:** explicitly selected groups, not blind replay into Kafka's internal offsets topic. **Risk/migration:** advancing offsets can skip work; rewinding can duplicate effects. Preserve prior target positions and require authorized confirmation.

### PROD-04.1 — Archive consistent-enough consumer position evidence

- **Issue:** Logweir currently skips group restoration; an optional snapshot is not a guaranteed capture contract.
- **Approach:** Capture selected group/topic/partition next-to-consume positions, observation time, source generation and relevant data coverage. State that metadata observed while applications run may not be atomic with record capture. Bind snapshots to recovery points and retain a source-offline recovery path.
- **Acceptance:** Every selected group is captured, excluded with a reason, or failed; absence never means offset zero. The catalog shows snapshot freshness and whether every position can be related to archived data.
- **Tests/evidence:** Active and empty groups, rebalances, missing Describe permission, offset beyond coverage, expired offsets, partitions added during capture and source loss after backup.
- **Dependencies:** PROD-01.1. **Handoff:** versioned group snapshot and explicit completeness/consistency classifications.

### PROD-04.2 — Translate positions and perform reviewed cutover

- **Issue:** Replayed records receive new offsets; using original committed positions directly is unsafe.
- **Approach:** Produce a source-to-target position mapping using verified provenance and next-to-consume semantics. Preview exact, approximate and unavailable mappings; default to blocking unknown mappings. Check selected target groups are inactive, save original positions and apply through a separately authorized cutover stage after data verification.
- **Acceptance:** A paused fixture consumer resumes at the expected record after restore. A live group cannot be silently reset. Partial application is recoverable and visible; restoring data never automatically triggers a consumer reset.
- **Tests/evidence:** Compaction gaps, offset at partition end, filtered restore, empty target partition, duplicate provenance, recreated topics, concurrent group activation and failure halfway through applying groups. Verify rollback limits if consumers have already restarted.
- **Dependencies:** PROD-04.1 and baseline approval/preflight; use PROD-07 where resume is supported. **Handoff:** mapping report, applied-position audit and application cutover instructions.

## PROD-05 — Recover configuration and access metadata

**Priority/status:** P1 / Proposed. **Owner areas:** Kafka administrative adapters, recovery planner. **Dependencies:** PROD-01.2 and platform preflight. **Boundary:** recover supported topic settings, ACLs and quotas; do not export credentials or recreate broker infrastructure automatically. **Risk/migration:** provider-managed settings and identities differ, and restored retention can immediately remove old data.

### PROD-05.1 — Capture metadata with portability rules

- **Issue:** Current target defaults do not recreate source topic settings, access policy or quotas.
- **Approach:** Archive explicitly supported topic configuration, partition count, replication intent, ACL patterns and quotas with provenance. Separate explicit overrides from inherited defaults, provider-only settings and secret material. Report missing permissions and unsupported resource types individually.
- **Acceptance:** A recovery point lists metadata coverage and a portable desired-state model. Unsupported settings do not masquerade as defaults; credentials are absent from manifests and logs.
- **Tests/evidence:** Compacted and delete-retention topics, min-in-sync requirements, mixed literal/prefixed ACLs, wildcard principals, quotas, missing metadata permission and unknown provider settings.
- **Dependencies:** PROD-01.2. **Handoff:** versioned metadata model, portability table and capture permissions.

### PROD-05.2 — Apply a reviewed target configuration plan

- **Issue:** Blindly copying source settings can fail on a smaller target or grant inappropriate access; retaining infinite recovery retention indefinitely creates cost and policy problems.
- **Approach:** Preview target-specific differences and principal mappings. Apply safe creation settings, restore records, then offer an explicit post-verification retention/configuration transition. Keep ACL/quota changes separately selectable and fail closed on unresolved principals or unsupported settings.
- **Acceptance:** Recovery preserves chosen compaction semantics and partition layout, explains replication changes and leaves existing unrelated policies untouched. The operator can review before/after state and see any partial application.
- **Tests/evidence:** Fewer target brokers, incompatible configs, old timestamps, denied ACL writes, colliding principals and retry after partial metadata application. Demonstrate a readable restored topic before and after deliberate cutover.
- **Dependencies:** PROD-05.1, platform restore review and PROD-04.2 when group cutover is selected. **Handoff:** target-diff report, rollback limitations and remaining manual infrastructure steps.

## PROD-06 — Application recovery profiles

**Priority/status:** P2 / Proposed. **Owner areas:** recovery planner, application fixtures, catalog. **Dependencies:** PROD-03, PROD-04, PROD-05. **Boundary:** first one documented Kafka Streams profile; Kafka Connect/Flink and external systems remain explicit follow-ups. **Risk/migration:** internal topic names alone cannot establish application consistency.

### PROD-06.1 — Define a recoverable application dependency profile

- **Issue:** Selecting all user topics can omit state needed by a stateful application. Kannika's guidance distinguishes changelog state from rebuildable repartition topics and ties recovery to source data and group positions. [Kafka Streams guide](https://docs.kannika.io/faq/kafka-streams/)
- **Approach:** Model application identity/version, source topics, selected changelogs, schemas, consumer groups and declared external dependencies. Offer discovery suggestions that require review. Specify the quiescence or consistency boundary needed for a supported recovery.
- **Acceptance:** The profile reports which dependencies are protected and why others are excluded. It blocks an application-recovery claim when required source history, group positions or state are missing.
- **Tests/evidence:** A local stateful aggregation, compacted changelog, renamed application ID, topology evolution and an intentionally missing dependency. Record behavior under capture while the application is active.
- **Dependencies:** PROD-01.1 plus metadata contracts from PROD-03/04/05. **Handoff:** one supported profile and explicit unsupported scenarios.

### PROD-06.2 — Prove application restart after recovery

- **Issue:** A completed data replay does not establish that a stateful application can resume correctly.
- **Approach:** Orchestrate the approved profile into an isolated target, then run a version-pinned application fixture and compare expected state and subsequent outputs. Expose Data restored and Application validated as distinct outcomes. Do not execute arbitrary user scripts with controller credentials.
- **Acceptance:** The fixture resumes and processes new inputs with expected state; missing prerequisites yield a useful failure and no misleading success badge. A profile change requires a new immutable plan.
- **Tests/evidence:** Full source loss, consumer restart, stale local state, schema mismatch, changelog compaction and interrupted verification. Compare business-level aggregates as well as records.
- **Dependencies:** PROD-06.1 and implemented PROD-03/04/05. **Handoff:** reproducible recovery exercise, residual external dependencies and recovery-time measurement.

## PROD-07 — Durable restore resume

**Priority/status:** P1 / Proposed. **Owner areas:** engine adapter, checkpoint storage, operator execution. **Dependencies:** PROD-01.1 and baseline distinct execution identities. **Boundary:** same immutable plan and target generation; changing a plan starts a new operation. **Risk/migration:** acknowledgements and checkpoint writes may not be atomic; automatic replay could duplicate data.

### PROD-07.1 — Resolve checkpoint and delivery semantics

- **Issue:** Current restore checkpoints are pod-local, and a crash is documented as non-resumable.
- **Approach:** Inspect engine checkpoint semantics and choose durable storage with integrity, fencing, versioning and lifecycle independent of a worker. Define the replay ambiguity window, producer acknowledgement boundary and behavior after target mutation. If duplicate-free continuation cannot be established, require reconciliation or a fresh target instead of silently resuming.
- **Acceptance:** The decision covers process kill, lost node/storage and competing workers. Checkpoints bind plan, execution, archive generation and target identity; old ephemeral checkpoints cannot be treated as durable.
- **Tests/evidence:** Inject termination before/after acknowledgements and checkpoint commits; replay stale checkpoints; start two workers and mutate target topics. Measure actual duplicate/loss behavior.
- **Dependencies:** PROD-01.1. **Handoff:** resume capability decision, checkpoint contract and failure-state table.

### PROD-07.2 — Expose safe pause, cancel, resume and retry

- **Issue:** Operators need understandable control of a long recovery without manually deleting jobs or guessing whether partial output is safe.
- **Approach:** Implement the approved checkpoint protocol and durable state transitions. Distinguish cancellation with partial output retained, safe resume and fresh-target retry. Show last durable progress and any replay ambiguity; stop new writes before acknowledging pause completion.
- **Acceptance:** Restarting the controller/browser cannot duplicate an operation. Resume either continues within proven semantics or explains why it is blocked. Cleanup never deletes an unrelated target or the only checkpoint before the operator has a recovery path.
- **Tests/evidence:** Kill the runner, restart controller, expire credentials, disconnect browser, repeat commands, lose checkpoint access and race cancel with completion. Verify output integrity and target identity on every restart.
- **Dependencies:** PROD-07.1 and platform operation state/idempotency. **Handoff:** user state model, recovery evidence and retention/cleanup policy.

## PROD-08 — Verification that answers recovery questions

**Priority/status:** P1 / Proposed. **Owner areas:** verifier, drill execution, evidence UI. **Dependencies:** platform truthful evidence and recurring drills; PROD-01. **Boundary:** extend existing evidence, do not add another mandatory approval ritual. **Risk/migration:** full verification is expensive and must not be silently substituted with sampling.

### PROD-08.1 — Add selectable verification depth

- **Issue:** Current record verification is sampled; users need a clear choice and a stronger option for critical recoveries.
- **Approach:** Define metadata-only, sampled and full supported comparisons, including how transformations, compaction and filtered ranges alter expected output. Stream comparisons with bounded memory and record count/byte coverage, exclusions and verifier version. Interrupted verification remains incomplete.
- **Acceptance:** Evidence clearly separates authenticated report, archive integrity, replay comparison and application validation. Full mode either compares every selected supported record under the defined contract or explicitly fails/incompletely verifies.
- **Tests/evidence:** Corrupt an unsampled record, omit a segment, duplicate output, alter a header, apply a schema mapping and interrupt verification. Ensure only the appropriate levels catch or disclose each fault.
- **Dependencies:** PROD-01.1 and platform evidence labels. **Handoff:** verification contract, cost measurements and backward-compatible report fields.

### PROD-08.2 — Measure recovery objectives through exercises

- **Issue:** A healthy backup schedule does not prove a usable recovery time or that restored applications function.
- **Approach:** Extend foundational drill scheduling with selected application assertions, pinned recovery points, isolated target quotas and cleanup accounting. Measure provisioning, replay, verification and application-validation durations separately; show observed results against user-defined recovery objectives.
- **Acceptance:** A failed drill makes protection risk visible without marking the archive deleted or unusable. Results identify tested scope and environment; a local timing result is not advertised as a production SLA.
- **Tests/evidence:** Corruption, source loss, unavailable registry, restricted target, timeout, expired credentials and cleanup failure. Include at least one known failing application assertion and verify alert deduplication.
- **Dependencies:** PROD-08.1; PROD-06.2 only for the stateful profile. **Handoff:** repeatable exercise, measured RPO/RTO definitions and failed-drill recovery steps.

## PROD-09 — Independent protected archives and storage choice

**Priority/status:** P2 / Proposed. **Owner areas:** storage adapters, catalog, archive policy. **Dependencies:** platform destinations/catalog/retention. **Boundary:** first strengthen the current S3 path; add another backend only through an explicit compatibility task. **Risk/migration:** signatures do not stop deletion; encryption without surviving keys can make a backup unrecoverable.

### PROD-09.1 — Make archive protection independently verifiable

- **Issue:** A backup accessible to compromised source credentials may be deleted along with the source. Object locking is version-specific and distinct from ordinary retention configuration. [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- **Approach:** Detect and report storage versioning, lock mode, retain-until/hold state and encryption/key references for the exact archived versions. Separate writer, reader and deletion authorities; preserve catalog/manifests/public verification material. Do not enable irreversible retention modes or hold removal as an incidental install action.
- **Acceptance:** The product distinguishes configured policy from observed protection. A delete marker cannot hide a known recoverable locked version from catalog import. Recovery identifies missing decryption keys before engine work and never exports private keys in support bundles.
- **Tests/evidence:** Local compatible-store versioning/locks where supported, permission denial, delete markers, retained versions, encrypted-object access failure and key rotation. Record provider-specific guarantees that local tests cannot establish.
- **Dependencies:** Foundational destination, retention and signing lifecycle. **Handoff:** protection report, required permissions and explicit provider validation gaps.

### PROD-09.2 — Add a verified secondary-copy protocol

- **Issue:** One bucket/account or region can remain a shared failure domain; copying some objects does not produce a complete recovery point.
- **Approach:** Define an adapter contract for listing/versioned reads, conditional publication, integrity and credential renewal. First implement copy of a committed recovery point to an independent S3-compatible destination, then re-import and verify it. Treat GCS/Azure adapters as separately scoped follow-ups after contract evidence, not assumed equivalents.
- **Acceptance:** The secondary location becomes available only after all required data, manifests and public verification metadata validate. A failed copy is resumable or restartable without corrupting the source; retention understands references to each copy.
- **Tests/evidence:** Interrupted multipart copy, corrupted object, missing schema snapshot, revoked credentials, primary unavailable and cleanup of abandoned uploads. Restore using only the secondary copy.
- **Dependencies:** PROD-09.1 and foundational archive import. **Handoff:** adapter contract, complete-copy marker semantics and backend-specific follow-up tasks.

## PROD-10 — Predictable performance and operating cost

**Priority/status:** P2 / Proposed. **Owner areas:** engine performance, metrics, capacity UI. **Dependencies:** PROD-01; continuous capture and resume metrics when available. **Boundary:** measured controls and estimates, not unproven multi-terabyte performance claims. **Risk/migration:** aggressive concurrency can harm source traffic or exhaust target/storage quotas.

### PROD-10.1 — Benchmark and expose safe workload controls

- **Issue:** Users cannot choose backup/recovery resource consumption confidently without throughput and bottleneck evidence.
- **Approach:** Build a reproducible local benchmark across record sizes, partition skew, compression and storage latency. Expose bounded concurrency, bandwidth/resource limits and prioritization only where supported by the engine. Evaluate fairness between schedules and emergency restores.
- **Acceptance:** Publish fixture size, hardware, broker settings, throughput and resource use. Controls have documented bounds, preserve correctness and expose backpressure. Scaling conclusions identify where larger/provider tests remain necessary.
- **Tests/evidence:** Uneven partitions, large records, low throughput, throttled storage, CPU/memory pressure and competing jobs. Compare correctness and source impact before and after tuning.
- **Dependencies:** PROD-01.1. **Handoff:** benchmark baseline, safe defaults and bottleneck-specific follow-ups rather than a broad rewrite.

### PROD-10.2 — Show cost and recovery-time estimates with uncertainty

- **Issue:** Retention and recovery choices create storage, request and transfer costs that users cannot currently estimate.
- **Approach:** Estimate retained bytes, compression, object requests and transfer from observed usage and user-supplied rate assumptions. Estimate recovery duration from comparable measured jobs; show range, sample age and missing inputs. Never present provider list prices as universal customer rates.
- **Acceptance:** Estimates are reproducible from visible assumptions, distinguish logical bytes from billed storage and show unknown when evidence is insufficient. Operator-supplied rate changes update the estimate without changing retention or execution policy.
- **Tests/evidence:** Empty/new installations, changing compression, unknown rates, partial metrics, currency/unit changes and unusually slow archives. Verify arithmetic and UI explanation against fixed fixtures.
- **Dependencies:** PROD-10.1 and platform status/catalog. **Handoff:** estimation model, accuracy limits and examples of operator decisions it supports.

## PROD-11 — Safe cloning and granular replay

**Priority/status:** P2 / Proposed. **Owner areas:** recovery selection, transformation pipeline, UI. **Dependencies:** baseline contextual restore; PROD-01 and PROD-03 for schema-aware data. **Boundary:** new isolated target topics; no in-place editing of original archives. **Risk/migration:** cloning production data can expose sensitive fields and transformations change verification expectations.

### PROD-11.1 — Add advanced replay selection and clone previews

- **Issue:** Developers may need a bounded dataset or an incident window rather than a whole backup.
- **Approach:** Extend the immutable plan with partition selection and offset/time ranges, preserving the established endpoint semantics. Reuse ordinary topic subsets and target-name preview from PLAT-11.2; add range coverage, omitted partitions and estimated volume. Validate filters across topic generations and retain the chosen source identity while newer backups arrive.
- **Acceptance:** The basic restore flow stays short; advanced controls appear only when selected. Preview and execution select the same records. The system cannot silently widen a requested range when archive coverage is missing.
- **Tests/evidence:** Inclusive/exclusive boundaries, equal timestamps, nonmonotonic time, empty selections, compaction holes, topic subsets, new recovery points during editing and target-name collisions.
- **Dependencies:** PROD-01.1 and baseline recovery-point selection. **Handoff:** filter contract, coverage preview and truthful evidence for subsets.

### PROD-11.2 — Introduce constrained masking for test environments

- **Issue:** Production clones may carry sensitive values; arbitrary transformations are difficult to secure and verify.
- **Approach:** Start with a small reviewed set of schema-aware field redaction/tokenization operations, deterministic when key relationships require it. Keep policies versioned and immutable per execution; reject unsupported formats or failed transformations by default. Use isolated execution and deny unneeded network/secret access.
- **Acceptance:** A clone preserves declared schema compatibility and required join-key relationships without revealing protected values. Evidence describes transformed fields and compares against transformed expectations rather than raw source fingerprints.
- **Tests/evidence:** Nested fields, keys/values, nulls, schema evolution, malformed payloads, deterministic references, attempted secret leakage and resource exhaustion. Scan outputs and logs for original protected fixture values.
- **Dependencies:** PROD-11.1 and PROD-03.2. **Handoff:** supported transformation catalog, security boundary and irreversible-transformation limitations.

## PROD-12 — Migration with an explicit cutover

**Priority/status:** P2 / Proposed. **Owner areas:** recovery orchestration and cutover UI. **Dependencies:** PROD-02, PROD-03, PROD-04, PROD-05 and relevant verification. **Boundary:** orchestrate supported backup/replay; do not promise generic zero downtime or automatic DNS/application changes. **Risk/migration:** dual writers, stale offsets and external effects can make rollback unsafe.

### PROD-12.1 — Build a migration readiness and rehearsal plan

- **Issue:** Moving providers requires coordinated data, schemas, permissions and applications; a successful replay alone is insufficient.
- **Approach:** Compose an immutable plan with source/target capability differences, bulk copy, catch-up, verification, producer/consumer pause conditions, offset application and operator-managed traffic switch. Model exact prerequisites and rollback limits; integrate with existing resources rather than introducing a second execution engine.
- **Acceptance:** A dry run identifies incompatible metadata, insufficient history and unresolved consumer mappings before migration writes. Operators see the point beyond which rollback needs business reconciliation.
- **Tests/evidence:** Provider capability fixtures, partial schema coverage, unpaused producers, changing topic counts and target capacity shortfalls. Demonstrate a complete local rehearsal without modifying the original source data.
- **Dependencies:** Contracts from PROD-02/03/04/05. **Handoff:** migration state machine, permission boundaries and measured rehearsal result.

### PROD-12.2 — Execute a checkpointed, reviewed cutover

- **Issue:** Manual handoffs leave unclear ownership and can declare migration complete while consumers point to the wrong place.
- **Approach:** Add durable reviewed transitions for copy, catch-up, final source quiescence, target verification, consumer mapping and application confirmation. Record who acknowledged external steps, their evidence and expiry. Resume only safe stages and make abort behavior explicit.
- **Acceptance:** A local application moves to the target and processes the next expected input. Completion requires data and selected application checks. A stale confirmation or new source writes invalidates the relevant cutover stage.
- **Tests/evidence:** API/browser loss, operator restart, source writes after pause, failed consumer reset, duplicate submission and target outage at each transition. Exercise abort before and after target consumers begin.
- **Dependencies:** PROD-12.1 and implemented underlying capabilities; PROD-07 for resumable stages. **Handoff:** execution evidence and operational rollback runbook.

## PROD-13 — Optional fleet and hosted control plane

**Priority/status:** P3 / Proposed. **Owner areas:** product API, enrollment, tenancy, fleet UI. **Dependencies:** foundational authenticated API/authorization/audit and a dependable local recovery workflow. **Boundary:** optional extension; self-hosted recovery must continue without the hosted service. **Risk/migration:** central credentials, cross-tenant access and disconnected control planes increase the recovery blast radius.

### PROD-13.1 — Decide the fleet architecture from explicit constraints

- **Issue:** Teams may operate several isolated Kafka environments, but sending data and credentials to a central SaaS may be unacceptable.
- **Approach:** Compare self-hosted multi-cluster management with an outbound-connected customer execution agent. Define tenant identity, metadata sensitivity, enrollment, revocation, command signing, allowed operations and offline behavior. Keep Kafka/archive credentials and data in the customer environment by default. Choose storage/database technology only against measured query and durability needs.
- **Acceptance:** The decision names trust boundaries and threat cases, including replayed commands and central outage. It explains what still works locally and which metadata leaves the environment. Hosted operation is not enabled merely by completing the design.
- **Tests/evidence:** Protocol/tabletop tests for revoked enrollment, stolen token, wrong tenant, expired command, disconnected agent and unavailable central catalog.
- **Dependencies:** Platform API/role/audit contracts. **Handoff:** go/no-go decision, bounded enrollment protocol and infrastructure prerequisites for a pilot.

### PROD-13.2 — Implement an isolated two-environment pilot

- **Issue:** A design alone cannot prove tenant isolation or reliable operation through disconnection.
- **Approach:** Implement one approved enrollment and command path, minimal fleet health and links to durable local operations. Enforce authorization both centrally and at execution; exclude generic Kubernetes or shell proxying. Preserve local administrator recovery when the central service is unavailable.
- **Acceptance:** Two logical environments cannot view or control each other; replayed commands do not create duplicate restores. Metadata is minimal and credentials never appear in the central catalog. Revocation blocks new commands while existing authorized work follows documented policy.
- **Tests/evidence:** Two isolated namespaces on docker-desktop, deliberate cross-tenant requests, command replay, key rotation, central outage and reconnect. Treat this as a local protocol pilot, not production SaaS certification.
- **Dependencies:** PROD-13.1 and foundation API implementation. **Handoff:** isolation evidence, telemetry policy and remaining production-hosting requirements.

## PROD-14 — Distribution, integrations and adoption

**Priority/status:** P2 / Proposed. **Owner areas:** installation, public API/CLI, documentation, packaging. **Dependencies:** reuse foundational onboarding and CI; PROD-01 support contract. **Boundary:** consolidate existing guides and packaging, not a parallel documentation tree or a new mandatory deployment system. **Risk/migration:** hidden prerequisites and incompatible version combinations can make a working archive inaccessible during an incident.

### PROD-14.1 — Validate a simple install and emergency recovery kit

- **Issue:** Kafka users include small teams, restricted networks and installations whose original control plane has been lost.
- **Approach:** Reuse the installation/import/upgrade implementations and evidence from PLAT-02, PLAT-15 and PLAT-20. The new deliverable is a versioned offline recovery kit containing compatible binaries/images, manifest schemas, public verification material and instructions to obtain separately managed credentials, validated through a disconnected exercise. Assess a non-Kubernetes mode only if the existing CLI can support it coherently.
- **Acceptance:** A fresh operator reaches a verified sample backup and restore without generating signing keys manually. Another clean installation can import the archive and recover using the kit; no private keys or passwords are bundled. Version and architecture constraints are visible before installation.
- **Tests/evidence:** Clean docker-desktop install, upgrade/rollback, private CA, no public network after artifacts are prepared, lost original CRs and unsupported CPU/image combinations. Time the journey and record remaining manual steps.
- **Dependencies:** Baseline onboarding/catalog/signing and PROD-01.2. **Handoff:** one canonical guide, compatibility manifest and measured usability result.

### PROD-14.2 — Offer stable automation and support interfaces

- **Issue:** A platform needs repeatable integration with GitOps, incident tooling and support without duplicating business logic in scripts.
- **Approach:** Version the bounded product API, align CLI and declarative resources, and provide idempotent operation/status examples using the same authorization. Add opt-in event/webhook delivery and a redacted support bundle with explicit preview. Define upgrade/deprecation policy and maintain one documentation source per workflow.
- **Acceptance:** An automation client creates one operation, follows it across reconnects and handles documented errors. Webhooks are authenticated, retried with bounds and deduplicable. Support bundles omit credential values and record payloads by default; emergency recovery does not depend on telemetry or an external license service.
- **Tests/evidence:** Old client/new server contracts, duplicate requests/events, unavailable receiver, expired tokens, unauthorized namespace and seeded secrets/payloads in diagnostic input. Verify offline access to recovery instructions.
- **Dependencies:** Platform API, status and audit contracts; reuse existing notification work rather than a second delivery service. **Handoff:** public compatibility policy, examples, redaction evidence and support boundaries.

## Release outcomes

1. **Dependable core:** foundational safety, easy installation, stable schedule-to-restore flow, archive import and honest protection/evidence states. No expansion substitutes for these fixes.
2. **Application-ready recovery:** supported semantics, schemas, consumer positions and target configuration; source-offline local recovery with an application consuming correct restored data.
3. **Continuous, operable protection:** committed coverage, gap visibility, safe resume, measured resource limits and independent archive copies.
4. **Broader adoption:** tested provider capabilities, optional cloning/migration and integrations. Fleet/SaaS follows demonstrated demand and isolation evidence.

Do not attach blanket claims such as complete Kafka recovery, zero data loss, exactly-once replay, regulatory compliance or universal provider support to a milestone. Publish the measured scope and limitations for each release instead.

## Completion record format

For each claimed task, append beneath that task: status; owner; implementation revision/PR; dependency evidence; decision and changed responsibilities; migration/rollback behavior; tests run and actual results; artifacts; remaining limitations; and follow-up task IDs. Use Proposed, Ready, In progress, Blocked, Done or Deferred. A blocked task must name the missing contract or reproducible failure. Only mark Done after the relevant acceptance criteria have evidence.

## Sources

All sources below were accessed 2026-09-14. Dates are access dates, not inferred publication dates. Kannika user-guide pages displayed v0.18.0; kbridge and vendor documentation may evolve. Recheck version-sensitive behavior before implementation.

- Kannika — [Product overview](https://www.kannika.io/product/), [pricing and packaging](https://www.kannika.io/pricing/).
- Kannika v0.18.0 — [Backup](https://docs.kannika.io/user-guide/backup/), [Restore](https://docs.kannika.io/user-guide/restore/), [Storage](https://docs.kannika.io/user-guide/storage/).
- Kannika v0.18.0 — [Consumer-group recovery FAQ](https://docs.kannika.io/faq/restored-consumer-groups/), [original-offset FAQ](https://docs.kannika.io/faq/restored-offsets/); Kannika maintainers — [kbridge repository](https://github.com/kannika-io/kbridge), default-branch documentation.
- Kannika v0.18.0 — [SchemaRegistryBackup](https://docs.kannika.io/user-guide/schema-registry-backup/), [SchemaRegistryRestore](https://docs.kannika.io/user-guide/schema-registry-restore/), [schema mapping](https://docs.kannika.io/user-guide/restore/schema-mapping/).
- Kannika v0.18.0 — [Restore report](https://docs.kannika.io/user-guide/restore/report/), [snapshots](https://docs.kannika.io/user-guide/backup/snapshots/), [data retention](https://docs.kannika.io/user-guide/backup/data-retention/), [lag monitor](https://docs.kannika.io/user-guide/backup/lag-monitor/).
- Kannika v0.18.0 — [Security configuration](https://docs.kannika.io/installation/configuration/security/), [Kafka Streams guidance](https://docs.kannika.io/faq/kafka-streams/), [0.18.0 release notes](https://docs.kannika.io/release-notes/0-18-0/) (no publication date shown).
- Apache Software Foundation — [Kafka 4.0 design: delivery and transaction semantics](https://kafka.apache.org/40/design/design/).
- Amazon Web Services — [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html), current user guide.
- Confluent — [Cluster Linking disaster recovery](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.html) and [features/limitations](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html), current Confluent Cloud documentation; displayed last-publication date 2026-08-31.
- Amazon Web Services — [MSK Replicator overview](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator.html), [bidirectional offset synchronization](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-bidirectional-offset-sync.html), [metadata and ACLs](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-metadata-acl.html), [planned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-planned-failover.html), [unplanned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-unplanned-failover.html), current MSK developer guide; no publication date established.
- Redpanda — [Whole Cluster Restore](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/whole-cluster-restore/), [topic recovery](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/topic-recovery/), [retention behavior](https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/disk-utilization/), current documentation identified as v26.2 when reviewed.

---
Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
