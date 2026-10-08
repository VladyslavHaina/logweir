# Kafka recovery product expansion

Research date: 2026-09-14. Re-validated and re-prioritized on 2026-09-23 against main `df5333f7`, the pinned engine source (`third_party/kafka-backup-v0.21.0.tar.gz`, read, not run) and the vendor pages in the source register. Current-state claims cite that revision; recheck them before implementing.

Status: **in progress.** The owner gave the go-ahead on 2026-09-28 (OD-1 released under option (a): every PLAT task has been Done since 2026-09-25). The loop started the same day. This file owns market-informed expansion; the platform tracker owns the foundation, so complete foundation work there instead of implementing it twice. A loop runs this file through the [Loop contract](#loop-contract-claude-code) and the [Execution ledger](#execution-ledger). On 2026-10-07 the owner took OD-3 and the new OD-8 (approvals without a personal key), closed OD-5 for the platform (paperwork moved to [Business](#business-out-of-platform-scope)), and asked to finish the platform first and simplify it after.

## Product direction

Make Logweir an approachable, independently verifiable Kafka recovery tool: select what to protect, see the latest usable recovery point, restore into a safe target, and know whether applications can resume. Complement replication with independent, restorable history. Replication copies mistakes immediately, and managed platforms leave historical data recovery to the customer (Confluent Cloud: "When a topic is deleted, it cannot be restored").

- **Who it serves.** Platform and SRE teams who install and protect Kafka: self-hosted first, managed Kafka once PROD-01.3 can reach it. Application and data teams who restore, replay and seed test environments. Security, GRC and continuity staff who read the evidence. Paying demand concentrates in regulated enterprises.
- **Positioning.** Evidence a customer can re-check independently, with its limits stated: two independent verifiers, approvals bound to the plan, restores into new topics, sampled-versus-complete labelling. Signing alone does not differentiate: the engine vendor's evidence reports are signed too, and hard-code `checksums_valid: true`.
- **North-star metric.** Time from a fresh install to the first verified restore. Every milestone records it.

A mature product needs several distinct promises. Topic data recovery, application recovery, cutover and complete environment recovery are different capabilities. An all-user-topics selector must not imply that consumer state, schemas, ACLs, transactions or external databases were captured. A signature authenticates evidence; it does not make sampled verification exhaustive.

Extend the existing foundation instead of replacing it: the Rust core, Kubernetes operator, isolated runner Jobs, static console, product API (PLAT-17) and durable recovery catalog (PLAT-15). Continuous capture needs a long-lived worker, which is a new kind under ADR 0008 Amendment A; finite restores keep isolated execution and immutable plans. Reuse saved connection definitions and credential references; do not share a TCP connection across unrelated pods.

## Research findings

Sources are official documentation, release notes, maintainer repositories and the pinned engine's source; the register is at the end. These are documented capabilities, not results of deploying or benchmarking competitors, so a missing feature means unverified, not absent. Marketing statements about near-zero loss, speed, compliance or universal compatibility are not adopted as Logweir guarantees. No benchmark, commercial trial or independent security assessment was performed, and practitioner forums were only partly reachable. Recheck version-sensitive rows before implementation; no competitor archive format is assumed readable except OSO's (PROD-00.1).

### OSO kafka-backup: engine supplier and competitor

Logweir runs OSO's MIT-licensed `kafka-backup` engine, digest-pinned at v0.21.0, as a subprocess (ADR 0001/0002). OSO also sells the closest competing product.

| Area | Observation (2026-09-23) | Decision for Logweir |
| --- | --- | --- |
| Currency | Upstream released v0.22.0 on 2026-09-07; it fixes the `path_style` defect behind ENGINE-PATHSTYLE. Logweir has not evaluated it. The weekly `engine-matrix` workflow failed both scheduled runs (2026-09-14, 2026-09-21) and has no 0.22.0 row. One maintainer accounts for 157 of the repository's contributions. | PROD-00.1 |
| Build | The runner copies OSO's published, linux/amd64-only image (`Dockerfile` engine stage; ruling GR6 in `scripts/extract-engine.sh`). The vendored source is checksummed but never built, so the "vendored source is the hedge" claim is unexercised, arm64 is blocked and engine CVEs wait for an upstream release. | PROD-00.2 (OD-3) |
| Gaps read from source | READ_UNCOMMITTED fetch with no control-record filtering (`kafka/fetch.rs`). Archive records have no transaction or producer fields (`BackupRecord`). Restore produces non-transactionally and non-idempotently and retries on connection errors (`kafka/produce.rs`, `kafka/partition_router.rs`). The restore checkpoint is written once per topic and `restore.checkpoint_interval_secs` is unused. In offset-store mode the offset checkpoint is set before the sealed segment's upload completes. `{backup_id}/manifest.json` is rewritten in place. Segment time bounds are the first and last record timestamps, not min/max (`segment/writer.rs`). No topic IDs. Fixed protocol versions without ApiVersions negotiation (`kafka/client.rs`). OAUTHBEARER and MSK IAM only through a programmatic plugin. The per-record Keep/Drop/Tombstone filter is settable only by embedding code (`#[serde(skip)]`). | Each gap is a row of PROD-00.1's capability table; dependent tasks name the row. |
| Enterprise edition | kafkabackup.com/enterprise lists as available: Confluent Schema Registry and Apicurio backup and restore, Confluent RBAC (MDS) backup, CSFLE metadata backup and MSK ZooKeeper-to-KRaft migration. Planned: data masking, audit logging and WebAssembly plugins. 14-day trial, offline licence. | Overlaps PROD-03, 04, 09.3, 11.2 and 12 and the Never list (OD-2). Do not expect the open-source engine to gain these. |
| Contribution policy | `docs/OSO_Feature_Gate_PRD.md` in the v0.21.0 source lists as enterprise-only, among others: client-side encryption, RBAC/SSO, audit trail, compliance reporting, GDPR erasure and crypto-shredding, data masking, automatic offset reset and rollback, Schema Registry backup/restore/ID remapping and backup validation test runs. Its review checklist rejects open-source PRs that add a listed feature. | Upstream PRs are realistic for bug-class fixes only; listed features need a Logweir-native path or a fork (OD-3). |
| Operators and archives | `kafka-backup-operator` (MIT) and `strimzi-backup-operator`, whose default engine has been v0.22.0 since 2026-09-07 (`docs/support-matrix.md` and `docs/stability.md` still say v0.19.1). Their archives use the same format. | PROD-00.1 corrects the docs and records whether such archives import and verify (after FX-1). |
| Evidence | The engine emits signed evidence reports, but `evidence/emit.rs` sets `checksums_valid: true` unconditionally. | Differentiate on independently re-checkable evidence (PROD-08). |

### Kannika: useful patterns and important distinctions

Kannika Armory's documentation still showed v0.18.0 (released 2026-09-08) on 2026-09-23. The original rows were re-checked. Rows marked "(corrected)" or "(added)" changed on 2026-09-23.

| Area | Documented observation | Decision for Logweir |
| --- | --- | --- |
| Protection model | The operator manages streaming backup workers, source/storage references, glob/regex topic selectors and pause controls. Selectors can discover future topics. [Backup documentation](https://docs.kannika.io/user-guide/backup/) | Discovery is delivered (PLAT-09). Continuous protection is PROD-02.3/02.4 after the engine route (OD-3). |
| Incident workflow (corrected) | Restores support drafts, topic renaming, partition/offset ranges, time filtering and linkage to a backup. Target topics must exist. The user guide lists two preflight checks; the v0.18.0 API schema defines eight (reachability, backup paused, topic in backup, source data, target exists/empty/writable, partition count). None cover schemas, groups, ACLs, configuration or capacity. [Restore documentation](https://docs.kannika.io/user-guide/restore/) | Reuse the platform restore wizard; add advanced selection (PROD-11.1) without exposing technical settings to every operator. |
| Consumer positions | The product page describes offset handling, but the technical FAQ says the Restore itself does not migrate consumer groups and directs users to a companion tool. [Product](https://www.kannika.io/product/), [consumer-group FAQ](https://docs.kannika.io/faq/restored-consumer-groups/) | Integrate capture, mapping, review and application into one durable recovery workflow (PROD-04). |
| Offset translation (corrected) | The companion kbridge (Business Source License 1.1; commercial use needs a licence from Cymo NV) separates fetching committed positions, calculating target positions and applying them. Its mapping path requires original-offset headers. Reading a restored `__consumer_offsets` topic exists only in the pre-release v0.3.0-rc1; the stable release is v0.2.0. [kbridge](https://github.com/kannika-io/kbridge) | PROD-04 needs an archived snapshot that survives source loss and an explicit, auditable cutover. Do not equate source and target offsets. |
| Schema recovery | Separate registry backup/restore resources are documented. Registry restore can change versions, repairs references, and supports an import mode with collision risk; the documented restore imports all stored schemas. [Registry backup](https://docs.kannika.io/user-guide/schema-registry-backup/), [registry restore](https://docs.kannika.io/user-guide/schema-registry-restore/) | PROD-03 offers dependency-aware selection and collision preview without modifying the archive (after OD-2). |
| Schema mapping (corrected) | Mapping uses a lookup table. The SAME generator maps Avro and, since 0.6.0, JSON Schema; Kannika's docs still say Avro only. Unmapped IDs pass through unchanged. [Schema mapping](https://docs.kannika.io/user-guide/restore/schema-mapping/) | Validate every required mapping before replay; qualify supported serialization formats explicitly. |
| Interrupted recovery | Restore progress is persisted on a Kubernetes volume, with lifecycle tied to the Restore. [Restore report](https://docs.kannika.io/user-guide/restore/report/) | PROD-07 defines crash semantics and durable progress beyond a disposable worker, without exactly-once claims. |
| Topic recreation | Snapshot support distinguishes topic generations through UUIDs; it is experimental and has compatibility restrictions for existing backup layouts. [Snapshots](https://docs.kannika.io/user-guide/backup/snapshots/) | Topic identity is a correctness requirement for every consumer (PROD-01.4), not a UI version picker. |
| Retention | Keep/Delete policies and a running segment reaper are documented. A stopped backup does not physically reap expired segments; one-off console deletion is future work. [Data retention](https://docs.kannika.io/user-guide/backup/data-retention/) | Keep expiration eligibility, physical deletion, holds and active-restore protection distinct (PLAT-16). |
| Monitoring | A lag monitor queries source offsets to calculate backup progress. [Lag monitor](https://docs.kannika.io/user-guide/backup/lag-monitor/) | Measure durable archived progress separately from consumed progress (PROD-02.1). RPO must reflect data recoverable after worker loss. |
| Authentication | OIDC login and API token validation are documented; the security page does not establish a per-resource viewer/operator/approver model. [Security](https://docs.kannika.io/installation/configuration/security/) | Logweir implements viewer/operator/approver roles in the product API (PLAT-17.2). Do not infer authorization merely from SSO. |
| Recent correctness work | Release 0.18.0 documents connection-test/custom-CA alignment, discovery error reporting and segment rollover fixes affecting durability. [Release notes](https://docs.kannika.io/release-notes/0-18-0/) | Exercise credentials from the actual runner context and test steady low-volume streams as well as high throughput. |
| Packaging and adoption (corrected) | Broker-based licensing, all storage targets and unrestricted restore counts; no universal numeric price. Installation requires a valid licence key (free trial on request). Case studies cite Engie and regulated financial infrastructure. [Pricing](https://www.kannika.io/pricing/), [installation](https://docs.kannika.io/installation/) | Keep recovery free of any licence gate during incidents. The business model is OD-5. |
| Roadmap (added) | Planned for 1.0.0: Namespace Isolation and Custom Authorization Policies (built-in role rules). Upcoming, unversioned: Topic Configuration Backups, Restore Replay and console schema-mapping generation. [Roadmap](https://docs.kannika.io/roadmap/) | Logweir's current leads in roles and topic configuration recovery may not last; keep PROD-05.1 in M2. |
| Beyond this backlog (added) | Azure Blob, GCS and volume storage; PLAIN, SCRAM-SHA-256, OAUTHBEARER and mTLS client credentials; Azure Event Hubs; plugins. Marketing claims beyond the docs: "near-zero RPO", "immutable audit logs with regulator-ready evidence". [Storage](https://docs.kannika.io/user-guide/storage/) | Client auth is PROD-01.3; storage verification is PROD-09.2 (OD-4); the rest are non-goals until demand says otherwise. Do not copy marketing claims. |

The strongest opportunity is a complete recovery journey with explicit coverage and evidence. Independently stored data must remain discoverable after loss of the original Kubernetes installation, and application recovery must report exactly which dependencies were recovered.

### Other recovery approaches

| Tool/approach | Documented strengths | Boundaries that matter for Logweir |
| --- | --- | --- |
| Confluent Cluster Linking | Live mirror topics, optional consumer-offset and ACL synchronization, and promote/failover operations. Registry availability needs Schema Linking or another compatible schema strategy. [Disaster recovery guide](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.html) | Synchronization is asynchronous; applications still need endpoint/credential changes. Kafka Streams/ksqlDB state does not fail over automatically. Continuity machinery, not an independently retained historical catalog. |
| Confluent scope restrictions | Supports external Kafka sources into supported Confluent Cloud destinations. [Features and limitations](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html) | Transactional/exactly-once mirror-topic workloads and share-group state synchronization are unsupported. ACL synchronization is constrained by organization and prefixing. Unknown lag must not appear as zero lag. |
| Confluent KCP (added) | Open-source CLI that moves clusters to Confluent Cloud over Cluster Linking (byte-for-byte, offsets preserved) and maps ACLs, schemas and connectors; currently from MSK. [KCP](https://docs.confluent.io/cloud/current/clusters/migrate-kcp.html) | Moving clusters with offsets is served by free vendor tooling; PROD-12 verifies such cutovers instead of orchestrating them. |
| Amazon MSK Replicator (corrected) | Managed asynchronous replication with topic configuration, ACL and group-offset synchronization. External Apache Kafka clusters can replicate into MSK Express brokers (April 2026) and Standard brokers (2026-07-09) using SASL/SCRAM or mTLS. [Overview](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator.html), [external sources](https://aws.amazon.com/about-aws/whats-new/2026/07/amazon-msk-replicator-external-kafka-standard-broker-support/) | Both clusters are part of the setup; not a historical backup. Same conclusion for PROD-12 as KCP. |
| MSK recovery metadata | Translates offsets, with enhanced bidirectional synchronization for eligible deployments. [Offset synchronization](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-bidirectional-offset-sync.html) | Translation can favor replay over skipping and does not overwrite active destination groups. ACL copying is selective. Schema Registry protection is not established. [Metadata and ACLs](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-metadata-acl.html) |
| MSK cutover | Documents planned exercises and unplanned failover/failback. [Planned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-planned-failover.html), [unplanned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-unplanned-failover.html) | Clients still need coordinated stop/restart and target configuration; unplanned failover may lose unreplicated data. Cutover is a distinct reviewed stage. |
| MSK data delivery to S3 (added) | Since 2026-07-30, Express brokers deliver topic data to S3 for archival and replay (JSON, ByteArray or String; no backfill). No restore into Kafka is documented. [Data delivery](https://docs.aws.amazon.com/msk/latest/developerguide/msk-data-delivery-s3.html) | An export, not a verified recovery point. |
| Redpanda Shadowing (added) | Since 25.3, an asynchronous, offset-preserving, byte-for-byte replica of a whole cluster (topics, configs, group offsets, ACLs, schemas); Enterprise licences on both clusters. [Shadowing](https://docs.redpanda.com/streaming/25.3/manage/disaster-recovery/shadowing/overview/) | Live continuity that replicates mistakes; offset-preserving copies are becoming platform features. |
| Redpanda Whole Cluster Restore | Restores archived data and metadata into a new cluster, including topic definitions, users, ACLs, consumer offsets and schemas when the schema topic was archived. [Whole Cluster Restore](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/whole-cluster-restore/) | Disclaims snapshot consistency and atomic committed transactions; in-flight transactions are aborted and offsets may be adjusted to restored coverage. |
| Redpanda topic recovery / Tiered Storage | Individual-topic recovery from object storage. [Topic recovery](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/topic-recovery/) | Tiered data remains subject to retention, so object-store location alone is not an independent historical copy. [Retention behavior](https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/disk-utilization/) |
| Strimzi operators (added) | The Unidirectional Topic Operator makes `KafkaTopic` resources the source of truth and reverts out-of-band changes made through the Admin client; the User Operator deletes ACLs that do not match `KafkaUser` resources. [UTO](https://strimzi.io/blog/2023/11/02/unidirectional-topic-operator/), [User Operator](https://github.com/orgs/strimzi/discussions/8871) | PROD-05 and PROD-15 detect declarative owners and export desired state instead of applying through admin APIs. |
| Kafka versions (added) | Apache support for 3.7 has ended; supported lines are 3.9 and 4.0–4.3. MSK ended 3.7.x support on 2026-09-01; MSK 4.2 is Express-only and does not yet support queues. Kafka 4.2 (2026-02-17) made share groups production-ready and the Streams rebalance protocol GA. Every Logweir fixture pins 3.7.1. [Support dates](https://endoflife.date/apache-kafka), [MSK versions](https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html), [4.2](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/) | PROD-01.5 adds supported broker lines; PROD-04 and PROD-06 classify group types. |

### Demand and regulation

- **Jobs, most cited first:** topic loss caused by people or automation (including GitOps or operator reconciliation); rolling back bad data that replication has already copied; seeding test environments and replaying incidents; an independent copy for cluster, provider or account loss; long retention. Consumer positions and schemas are second-order needs that appear once a restore is attempted. Moving providers is served by free vendor tools. Ransomware is mostly a vendor and regulatory story; no Kafka-specific incident was found.
- **Signal:** practitioner discussion of Kafka backup is thin next to replication, and paid demand concentrates in regulated enterprises.
- **Regulation asks for tested restores with recorded results:** DORA Art. 11–12 and its RTS (Art. 12(3): restore into segregated systems), ISO/IEC 27001 A.8.13, SOC 2 A1.3, NIS2 implementing regulation 2024/2690 Annex §4.2, and HIPAA 164.308(a)(7). Logweir's segregated drills, reconciliation, measured RTO/RPO, approvals and independently verifiable scorecards fit that wording. Keep the ban on compliance claims and publish a clause-by-clause control-evidence mapping instead (PROD-08.4).
- **Erasure:** the EDPB's coordinated-enforcement report on the right to erasure (adopted 2026-02-10) expects organisations that keep personal data in back-ups to track erasure requests and honour them on restored systems as far as possible. Immutable archives, secondary copies, continuous capture and clones all extend the life of personal data (PROD-09.3).

**Inference:** Logweir should complement replication with independent recovery history, make metadata coverage visible and prove application cutover, differentiating on evidence a customer can re-check. It should not compete by promising stronger transactional or zero-loss guarantees than its capture and replay protocols support. It must plan around its engine supplier's paid tier rather than assume open-source parity.

## Owner decisions

The loop never decides these. A row gated on an open decision stays Blocked; the loop asks the owner once, with the options below, and continues with other Ready rows.

| ID | Decision | Options | Recommendation | Blocks | State |
| --- | --- | --- | --- | --- | --- |
| OD-1 | When expansion starts | (a) after every PLAT task is Done and the owner says go; (b) additionally allow Wave 0 rows that need no docker-desktop change before the platform tracker completes; (c) hand FX-1…FX-7 to the platform run's defect table now | (c) now, (a) for the rest; (b) if agent capacity allows | Every row | **Released 2026-09-28** under (a): all 41 PLAT tasks Done 2026-09-25; the owner said go |
| OD-2 | Product boundaries in `docs/stability.md` "Never" | Per entry: keep, narrow or reverse | Keep #1 (live topics) and allow PROD-15's original-name restore into an absent topic; keep #3 (PROD-12 is cutover verification) and #4 (PROD-13 deferred); decide #2 after PROD-03.0 shows how many protected topics depend on a registry, and if narrowed, allow schema resolution and selective import only (RBAC-MDS and CSFLE stay refused) | 03.1, 03.2, 12.1, 15.1 | **Decided 2026-10-05** for #1, #3 and #4, as recommended. #1 is narrowed: restore-in-place into a live topic stays refused, and PROD-15's original-name restore into an absent topic is allowed behind its own approval, so 15.1's gate is clear (rule 8: the Never-list edit lands with its doc_lint update in one commit). #3 is kept, and 12.1 is cutover verification. #4 is kept, and PROD-13 stays deferred. **#2 stays open** until PROD-03.0 reports, so 03.1 and 03.2 stay gated |
| OD-3 | Engine route per capability (proposed by PROD-00.1) | Upstream PR (bug-class fixes only); maintained MIT fork built from the vendored source (supersedes or scopes ruling GR6); Logweir-native path; declared unsupported | Build from source regardless (arm64, CVE patching, signed provenance); upstream PRs for bug-class fixes; native paths for offsets, ACLs and verification; fork only what upstream refuses | 00.2, 00.3, 02.3, 07.3, 11.2 | **Decided 2026-10-07**, the simpler form of `decisions/PROD-00-engine-route.md` §8. (1) Logweir builds the engine from the vendored source with its own patch folder. This supersedes GR6, amends ADR 0002 and brings engine CVEs into SECURITY.md's scope; `doctor` and the evidence name Logweir's build distinctly from OSO's release. (2) The carrying window is 0: a fix lands as a patch as soon as its oracle passes, with a one-line reason, and is dropped when an OSO release contains it. (3) Upstream PRs are optional and come after shipping, for bug-class fixes only (C1, C4, C5, C6, C12, C14 to `kafka-backup`; C13 to `kafka-protocol-rs`), never the filter-rule or SASL-plugin patches. (4) MSK IAM and OAUTHBEARER are deferred (X): MSK is reached with SCRAM-SHA-512 over TLS. 00.3f re-targets the newest OSO release (0.23.x, which rewrote `partition_router.rs`) instead of 0.22.0. 00.3a–e take the F route and depend on 00.2; 00.3g–i and 00.3o join the ledger; 00.3j–m wait for their triggers, and 00.3n is its own trust decision. Amendment D (04.0c) still needs the owner's yes under rule 8, asked when 04.0c is ready |
| OD-4 | Provider and storage evidence | Accounts and budget for AWS (S3, MSK), Confluent Cloud, Redpanda Cloud, Aiven, Azure Event Hubs, GCS and Azure Blob; whether rule 6 lets a local runner reach remote endpoints | AWS S3 and MSK first (largest managed population; real conditional create and lock readback strengthen the evidence), then Confluent Cloud after PROD-01.3 | Provider rows in 01.2 and 09.2 | **Decided 2026-10-05:** AWS S3 and MSK only; no other provider is funded yet. There is no standing budget: before creating any billable AWS resource, the orchestrator asks the owner with a cost estimate, and test resources are deleted after each session. Rule 6: a runner on the owner's machine may reach the funded provider only, with test-only credentials scoped to disposable resources. The AWS provider rows in 01.2 and 09.2 wait for the owner's account and credentials; every other provider stays not evidenced |
| OD-5 | Business and legal | Trademark clearance or rename (`TRADEMARKS.md` gates announcing); monetization before external contributions (no CLA, so relicensing closes after the first outside PR); a legal opinion on copyright in AI-assisted code for the chosen model; a contracting entity for regulated buyers | Decide monetization and the name before any public positioning | — (no platform row) | **Decided 2026-10-05:** open core. The Apache-2.0 core stays open and paid add-ons ship separately under a commercial license, so no relicensing is needed and the no-CLA rule stands. Name: the owner commissions the clearance act in `TRADEMARKS.md`; public positioning, publishing 14.3's outputs and outreach beyond NDA stay gated on that opinion. The copyright opinion on AI-assisted code and a contracting entity are owner actions due before the first sale of a paid add-on; no engineering row waits on them. **Closed for the platform 2026-10-07:** the owner set the paperwork aside. The trademark clearance, the copyright opinion on AI-assisted code, a contracting entity, add-on terms and pricing, public positioning and outreach move to [Business](#business-out-of-platform-scope), and PROD-14.3 is deferred there |
| OD-6 | A scoped `unsafe` policy for librdkafka calls the safe rdkafka API lacks (PROD-01.4's TI-OC1; it also covers PROD-04.0's group, offset and ACL calls, so `unsafe` is decided once) | (a2) one FFI crate for every such call, with `logweir-kafka` and every product crate keeping `forbid(unsafe_code)`; (a1) a private module in `logweir-kafka` (`deny` plus one `allow`); (b) wait for upstream rust-rdkafka (PR #721, stalled since 2024-09); (c) the engine route (PROD-00.3, and Amendment D for offsets); (d) a raw-protocol client; (e) no `unsafe`: the heuristic only, and PROD-04.0 limited to the safe consumer API | (a2), with (b) as the per-call exit and the PROD-01.4 heuristic as the fallback for `null` IDs and truncation (`decisions/PROD-01.4-topic-identity.md` §6.4) | 01.4a; PROD-04.0's FFI paths | **Decided 2026-10-05** as recommended: (a2), one FFI crate for every such librdkafka call, with `logweir-kafka` and every product crate keeping `forbid(unsafe_code)`. (b) is the per-call exit: a call is deleted once rust-rdkafka wraps it safely. The PROD-01.4 heuristic stays the fallback for `null` IDs and truncation. The gate on 01.4a and 04.0b is clear |
| OD-7 | Versioning of new `validate_invariants` arms that read ONLY a new optional block | (a) MINOR (format 1.1.0): a reader that predates the block ignores the arms, and nothing it verifies changes (the argument of FX-4 for its arms 6–11, and of PROD-01.4 §4); (b) MAJOR (format 2.0.0), the literal reading of `docs/stability.md`, which calls every `validate_invariants` change MAJOR and needs two maintainer approvals | (a), limited to arms that read only the new optional block; any arm that changes how an existing field is judged stays MAJOR | FX-4's merge; the topic-ID arms PROD-01.4 planned for PROD-02.1 | **Decided 2026-10-05** as recommended: (a) MINOR for arms that read ONLY a new optional block; an arm that changes how an existing field is judged stays MAJOR. A follow-up ruling the same day makes FX-4's review-M5 fix MINOR as well: it adds a `<topic>: configuration not assessed (<why>)` entry to the existing `unexpected_divergence`, which can only weaken an older reader's verdict. `docs/stability.md` records both rulings with FX-4. FX-4's merge is no longer gated on OD-7; its version order with FX-7 still applies. **Third case, 2026-10-07** (FX-7's review V3 and FX-3): a new cause for an existing value, or new content in an existing field, is MINOR when it can only move a reader's verdict to the safer side (not restorable, not trusted, not intended), and never when it can make a verdict stronger. FX-7's superseded pin (`availability: Conflict`, exit 3, `Unreadable`) and FX-3's move of the new-topic deviations to `unexpected_divergence` are this case. Because the project has one maintainer, a MAJOR bump now needs the owner's recorded decision (a row in this table) instead of two maintainer approvals (`docs/stability.md`, `MAINTAINERS.md`) |
| OD-8 | Approvals without a personal key (the owner: "I don't like that an approver needs to have a key") | (a) no key by default: the requester confirms in the console, and a namespace may opt in to two-person (a second person signs in and clicks Approve) or strict (today's personal-key countersign); (b) two-person by default, no keys; (c) remove personal keys entirely | (a), from the 2026-10-07 research and its security challenge | PROD-16.1–16.3; PROD-14.1's key-free acceptance | **Decided 2026-10-07** as (a). Default: one-person confirmation in the console, also in localAdmin, with the console key generated at install, so no operator handles a key. Two-person, per namespace: the second person signs in and clicks Approve. Its must-haves: the console verifies its own earlier signature on the stored request before approving; the approver's `<issuer>#<subject>` and `approvedAt` go into the signed bytes; the API, the controller and the runner each refuse requester == approver and local-admin principals; the mode is part of the policy snapshot's digest; the existing ConsoleConfirmation key usage is reused (no new `KeyUsage`, which would trap rollbacks); the chart allows it only in shared mode. Strict, per namespace: today's `drill countersign` path, unchanged. Residual, stated in SECURITY.md: in console modes, whoever controls the console pod, its key Secret or the identity provider can approve alone, as Ordinary already accepts. This amends D0 (`decisions/D0-product-api-and-identity.md`). Other simplifications wait until the platform is complete |

## Business (out of platform scope)

Set aside by the owner on 2026-10-07 ("leave aside all the bureaucracy part"). The loop never asks about these, and no row depends on them.

- **B-1** Trademark clearance before any public announcement (`TRADEMARKS.md`).
- **B-2** A legal opinion on copyright in AI-assisted code, before the first paid add-on sale.
- **B-3** A contracting entity for regulated buyers.
- **B-4** The add-on list, licence terms and pricing under open core (OD-5).
- **B-5** PROD-14.3's positioning and design-partner kit and its open questions. The drafts are kept privately in `$HOME/.logweir-roadmap-run/claude/artifacts/prod-14-3/` and never committed; its north-star definition moved to PROD-14.1.
- **B-6** Public positioning, a launch and outreach beyond NDA.
- **B-7** The DCO sign-off: required only on commits in outside pull requests. `CONTRIBUTING.md` is updated with the next docs change.

## Loop contract (Claude Code)

1. **Start.** Nothing starts while OD-1 is on hold; a loop run then only reconciles and reports. After the go-ahead, rows start in wave order.
2. **Each iteration reads,** in order: this file's [Execution ledger](#execution-ledger) and [Owner decisions](#owner-decisions); the platform tracker rows named in the [dependency map](#foundation-dependency-map); the run directory `$HOME/.logweir-roadmap-run` (symlinked from `/tmp/logweir-roadmap-run`): `coordinator-state.json`, `claude/WORKER-RULES.md`, `claude/REVIEW-RULES.md` and reports; `git status` and `git log` on main; live workers and `claude/k8s-lock.sh status`. Code, tests and runtime behaviour outrank reports.
3. **Ready** means: status Proposed; every row in "Depends on" is Done; the gate is clear; the dependency map's PLAT prerequisites are Done. Choose the lowest wave, then the row that unblocks the most others.
4. **Dispatch** one row per brief through the Agent tool with `model: "opus"`. At most four agents run at once, workers and reviewers together (owner rule), and no worker spawns agents. A brief quotes the row, the task text, the inherited rules, file ownership, the review tier, required tests and the owning decision record. Workers follow `WORKER-RULES.md` and never edit this file.
5. **Research rows** are time-boxed to two worker runs and one review, answer from source first, and end in `docs/to-do/decisions/PROD-<id>-<slug>.md`: the decision, evidence, limits and numbered acceptance rows (pass predicate, negative control, fixture) for each dependent task, plus any child rows to add to the ledger.
6. **Review tiers** (the platform run's lean loop). A: runner, engine adapter, controller, API, RBAC, signing, CRDs and chart security, which get a full Rust and security review and a second pass only on a HIGH finding. B: console, harnesses, CI and docs with behavioural claims, which get one checklist pass. C: tests or docs without behavioural claims, which get gates only while the orchestrator reads the diff. Controller, API, RBAC, signing and CRD code needs mutant evidence; console and harness code needs one negative control per behaviour.
7. **Integrate** after the reviewer accepts: rebase, run the full gates (`just lint` with `LOGWEIR_PYTHON`; `bash scripts/ci-check.sh` for code), merge, push through the repository's publication workflow and remove the worktree. Controller-image changes get their live proof at one batch lab refresh per merge batch.
8. **Record** only as the orchestrator: update the row's status and append the completion record under the task in the same `docs(to-do):` commit. A row is Done only with its acceptance evidence.
9. **Kubernetes:** docker-desktop only, explicit `--context docker-desktop` / `--kube-context docker-desktop`. The platform run leaves one Logweir release installed (the owner's 2026-09-23 decision: a Helm install from published images). Tests use their own labelled namespaces (`lw-<task>-<utc>`), change shared or cluster-scoped state only under the lock, and leave that release as found. Provider evidence only per OD-4.
10. **Stop** when every row is Done or Deferred, or only owner-gated rows remain: checkpoint the ledger and `coordinator-state.json`, then report. Also stop on an owner instruction; before session limits, checkpoint first.

Starting the loop, once OD-1 is released:

```text
/loop Run the Logweir product-expansion loop: follow the Loop contract in docs/to-do/product-expansion.md. Reconcile the Execution ledger with main and the run directory, dispatch Ready rows (at most four Opus subagents, none spawning agents), review, integrate, record evidence, and stop at owner decisions. Do nothing while OD-1 is on hold.
```

## Milestones and priorities

P1 is required for M1 or M2, the first release a self-hosted team would adopt. P2 is M3. P3 is M4 or optional. Priority is not permission to skip dependencies.

1. **M1 — Installable and reachable.** FX-1…FX-7 closed. A tagged GitHub Release is green, with CLI archives, the independent verifier and notices, and the chart and images are published with digests (14.0). One canonical guide takes a fresh operator on docker-desktop to a verified backup and restore, and the time is recorded (14.1). PLAIN over TLS, SCRAM-SHA-256 and mTLS work (01.3). The support matrix states tested broker lines, including a supported 4.x line (01.5), provider rows with their evidence status (01.2), and the engine version and route (00.1). An arm64 runner exists, or the limitation is stated with its cause (00.2).
2. **M2 — Get your topic back.** A deleted topic comes back under its original name behind its own approval (15.1), or as a time/partition slice into new topics (11.1). Restored topics keep partitions, bounded replication and recovery-relevant configuration, with a post-verification transition (05.1, 05.2). Selected consumer groups are captured and applied through a reviewed cutover, and a paused consumer resumes at the expected record (04.x). Complete archive integrity and exact counts exist (08.1). Record, transaction and identity semantics are decided and enforced (01.1, 01.4). Schema-dependent topics are flagged (03.0). Scheduled capture shows honest coverage and is incremental (02.1, 02.2). The control-evidence mapping exists (08.4).
3. **M3 — Operable at scale.** Engine capabilities by route (00.3); continuous protection (02.3, 02.4); interruption and resume (07.x); recovery objectives and full comparison (08.2, 08.3); protected archives and copies (09.x); benchmark and bounded controls (10.1); registry recovery per OD-2 (03.1, 03.2); ACL export (05.3); automation interfaces (14.2).
4. **M4 — Ecosystem and optional.** Application profiles (06), cost estimates (10.2), masking (11.2) and cutover verification (12.1). Fleet (13) is deferred.

Do not attach blanket claims such as complete Kafka recovery, zero data loss, exactly-once replay, regulatory compliance or universal provider support to a milestone. Publish the measured scope and limitations for each release instead.

## Execution ledger

The single source of task status. Waves give the earliest intended batch; "Depends on" governs. Every row also needs OD-1's go-ahead. Lab: `none`, `compose` (the `e2e/compose` fixtures), `k8s` (docker-desktop). Tier: review tier from the Loop contract.

| Wave | Row | Title | P | M | Kind | Depends on | Gate | Lab | Tier | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | FX-1 | Parse the engine's consumer-group snapshot | P1 | M1 | fix | — | — | compose | A | Done |
| 0 | FX-2 | Apply or refuse `runnerResources` | P1 | M1 | fix | — | — | k8s | A | Done |
| 0 | FX-3 | Stop labelling new-topic deviations "intended" | P1 | M1 | fix | — | — | compose | A | Done |
| 0 | FX-4 | Record topic-configuration capture coverage | P1 | M1 | fix | — | — | compose | A | Done |
| 0 | FX-5 | Console replication factor from the source | P1 | M1 | fix | — | — | k8s | B | Done |
| 0 | FX-6 | Disclose transaction and timestamp semantics | P1 | M1 | docs | — | — | none | B | Done |
| 0 | FX-7 | Keep earlier points valid after a manifest rewrite | P1 | M1 | fix | — | — | compose | A | Done |
| 0 | FX-8 | Refuse or label point-in-time selection over `LogAppendTime` sources | P1 | M1 | fix | 01.1; FX-4 for the broker-default arm | — | compose | A | Done |
| 0 | FX-9 | Pass `now` into the standing-authorization mint and countersign checks (time-bombed fixture) | P1 | M1 | fix | — | — | none | A | Done |
| 4 | FX-9a | A Linux shim for `just test-shifted-clock` (macOS only today; Linux refuses) | P3 | M3 | infra | FX-9 | — | none | C | Proposed |
| 0 | FX-10 | Apply or withdraw configured values that reach nothing (two inert chart defaults; the retention caps' parse) | P2 | M1 | fix | — | — | none | A | Done |
| 0 | FX-16 | Refuse a restore whose backup set is not the bound point's (manifest digest) | P1 | M1 | fix | — | — | compose | A | Proposed |
| 0 | FX-17 | The catalog view offers no point on its first page ("not offered: … receipt key as [redacted]") | P1 | M1 | fix | — | — | k8s | A | Proposed |
| 0 | FX-18 | Test and harness hygiene: the `logweir-api` connection-ceiling test flakes under host load; the PoC harnesses cannot name a browser executable | P3 | M1 | infra | — | — | none | C | Proposed |
| 0 | FX-19 | A probe Job Kubernetes is deleting reads as a crash: `reachable` flaps for seconds and each probe cycle logs about 12 WARN lines | P2 | M1 | fix | FX-11 | — | none | A | Proposed |
| 0 | FX-15 | FX-10's owed items: non-default rows for five configured values (evidence-fetch pool, restore manual-run gate, `api.console.*`, `identity.*`, a Backup's `visibilityAttestations`), the unread `legacyArchiveAddressing` fields, and a retention refusal's reason on its status | P3 | M3 | fix | — | — | none | A | Proposed |
| 0 | FX-11 | Report a rejected Job pod promptly, with its reason, in every Job-owning controller | P2 | M1 | fix | — | — | none | A | Done |
| 0 | FX-12 | Pass `now` into weirkeeper's evidence verification (trust-window clock reads) | P2 | M1 | fix | FX-9 | — | none | A | Proposed |
| 0 | FX-13 | Key the sign-in limiter on the trusted-proxy client address | P1 | M1 | fix | — | — | none | A | Done |
| 0 | FX-13a | Make the sign-in state single-use (one login cookie replays several callbacks within its 600 s, each a token request) | P3 | M1 | fix | FX-13 | — | none | A | Proposed |
| 0 | FX-14 | FX-7's and PROD-08.4's owed items: the restore preflight reads the pinned manifest version; minimal grants name `s3:GetObjectVersion`; the surviving fail-safe mutant MR-L2; two wrong field descriptions | P2 | M1 | fix | FX-7 | — | compose | A | Proposed |
| 0 | PROD-00.1 | Evaluate the engine; decide a route per capability | P1 | M1 | research | — | — | compose | B | Done |
| 0 | PROD-01.1 | Prove record and transaction behaviour | P1 | M2 | research | — | — | compose | B | Done |
| 0 | PROD-01.4 | Define topic identity and generations | P1 | M2 | research | — | — | none | B | Done |
| 0 | PROD-01.5 | Shared fixture profiles and broker versions | P1 | M1 | infra | — | — | compose | B | Done |
| 0 | PROD-04.0 | Decide the Kafka administrative path | P1 | M2 | research | — | — | compose | B | Done |
| 0 | PROD-08.4 | Publish a control-evidence mapping | P1 | M2 | docs | — | — | none | B | Done |
| 0 | PROD-14.0 | Ship a working release | P1 | M1 | infra | — | — | none | B | Done |
| — | PROD-14.3 | Positioning and design-partner kit | — | — | docs | — | Business (B-5) | none | C | Deferred |
| 1 | PROD-00.2 | Build the engine from the vendored source, with a patch folder (amd64 and arm64) | P1 | M1 | infra | 00.1 | — | compose | A | Proposed |
| 1 | PROD-01.3 | Reach managed and mTLS clusters | P1 | M1 | impl | 01.5 | — | k8s | A | Proposed |
| 1 | PROD-01.2 | Publish a tested compatibility contract | P1 | M1 | research | 00.1, 01.1, 01.3, 01.5 | OD-4 (provider rows) | compose | B | Proposed |
| 1 | PROD-14.1 | Simple install and emergency recovery kit | P1 | M1 | impl | 14.0, 01.2, 16.1 | — | k8s | A | Proposed |
| 1 | PROD-16.1 | No key by default: one-person console confirmation, console key generated at install | P1 | M1 | impl | — | — | k8s | A | Proposed |
| 1 | PROD-16.2 | Two-person approval in the console: the second person clicks Approve | P1 | M1 | impl | 16.1 | — | k8s | A | Proposed |
| 1 | PROD-16.4 | Auth follow-ups: IdP groups mapped to roles proven live; an audit-log retention recipe | P2 | M1 | impl | — | — | k8s | B | Proposed |
| 1 | PROD-08.1 | Complete archive integrity and exact counts | P1 | M2 | impl | 01.1 | — | compose | A | Proposed |
| 1 | PROD-05.1 | Capture topic configuration with coverage | P1 | M2 | impl | FX-4, 01.5 | — | compose | A | Proposed |
| 1 | PROD-04.1 | Archive consumer position evidence | P1 | M2 | impl | 04.0, 04.0a, 04.0d, 01.4, FX-1 | — | compose | A | Proposed |
| 1 | PROD-02.1 | Honest coverage for scheduled backups | P1 | M2 | impl | 01.4 | — | k8s | A | Proposed |
| 1 | PROD-07.1 | Resolve checkpoint and delivery semantics | P2 | M3 | research | 01.1 | — | none | B | Proposed |
| 1 | PROD-09.3 | Decide archive data protection | P2 | M3 | research | 00.1 | — | none | B | Proposed |
| 1 | PROD-01.1a | Detect transactional archives; refuse by default, label an approved override | P1 | M2 | impl | 01.1, FX-6 | — | compose | A | Proposed |
| 1 | PROD-01.1b | Make recovery-point selection safe for out-of-order timestamps | P1 | M2 | impl | 01.1 | — | compose | A | Proposed |
| 1 | PROD-01.4a | Topic IDs through DescribeTopics (the first wrapper under OD-6's policy) | P1 | M2 | impl | 01.4 | OD-6 | compose | A | Proposed |
| 1 | PROD-04.0a | Committed positions through the safe consumer API | P1 | M2 | impl | 04.0 | — | compose | A | Done |
| 1 | PROD-01.5a | Move the default broker line off 3.7.1 | P1 | M1 | infra | 01.5, 00.1 | — | compose | B | Proposed |
| 1 | PROD-01.5c | Run PROD-01.1's and 01.4's suites on the 3.9, 4.1 and 4.3 lines | P1 | M2 | infra | 01.5 | — | compose | C | Proposed |
| 3 | PROD-01.5b | Put the `crates/` e2e rows on the per-stack variables | P2 | M3 | infra | 01.5 | — | compose | B | Proposed |
| 1 | PROD-04.0b | Group and ACL calls inside OD-6's perimeter | P1 | M2 | impl | 04.0 | OD-6 | compose | A | Proposed |
| 1 | PROD-04.0c | Amendment D names the engine's group subcommands | P2 | M3 | docs | 04.0 | owner's yes on Amendment D (rule 8) | none | B | Proposed |
| 1 | PROD-04.0d | Fixtures for groups and ACLs (`acl` profile, `groups` helper, streams-protocol variant, share-state settings) | P1 | M2 | infra | 04.0, 01.5 | — | compose | B | Proposed |
| 3 | PROD-01.4b | Upstream DescribeTopics in rust-rdkafka (the exit for 01.4a) | P3 | M3 | impl | 01.4a | — | none | B | Proposed |
| 2 | PROD-02.2 | Capture incrementally | P1 | M2 | impl | 02.1, 00.1 | — | compose | A | Proposed |
| 2 | PROD-03.0 | Flag schema-dependent topics | P1 | M2 | impl | — | — | compose | A | Proposed |
| 2 | PROD-04.2 | Translate positions; reviewed cutover | P1 | M2 | impl | 04.1, 08.1 | — | k8s | A | Proposed |
| 2 | PROD-05.2 | Apply a reviewed target topic configuration | P1 | M2 | impl | 05.1, 08.1 | — | k8s | A | Proposed |
| 2 | PROD-11.1 | Replay selection and safe clones | P1 | M2 | impl | 01.1, 08.1 | — | k8s | A | Proposed |
| 2 | PROD-15.1 | Restore under the original name into an absent topic | P1 | M2 | impl | 01.4 | OD-2 | k8s | A | Proposed |
| 3 | PROD-00.3 | Engine capabilities by route (child rows) | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 3 | PROD-00.3a | Committed-only capture (control records and READ_COMMITTED) | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 3 | PROD-00.3b | Segment min/max record timestamps | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 3 | PROD-00.3c | Keep `LogAppendTime` through capture | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 3 | PROD-00.3d | Idempotent (or sequence-checked) restore produce | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 3 | PROD-00.3e | Keep repeated header keys through capture and replay | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 1 | PROD-00.3f | Move the pin to the newest OSO release (0.23.x), with the C15/C16 guards and `doctor`'s pin | P1 | M1 | infra | 00.1 | — | compose | A | In progress |
| 3 | PROD-00.3g | Engine restore checkpoint: honour `checkpoint_interval_secs`, hash without file paths (C4) | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 3 | PROD-00.3h | Enforce the byte-rate limit (C12) | P2 | M3 | impl | 00.1, 00.2 | — | compose | B | Proposed |
| 3 | PROD-00.3i | YAML record-filter rules: erasure, offset ranges, resume point (C10, C11) | P2 | M3 | impl | 00.1, 00.2 | — | compose | A | Proposed |
| 1 | PROD-00.3o | Weekly archive-compatibility rows for operator-written archives | P2 | M1 | infra | FX-1 | — | compose | B | Proposed |
| 3 | PROD-02.3 | Decide the continuous capture protocol | P2 | M3 | research | 02.2, 01.4, 00.3 | — | compose | B | Proposed |
| 3 | PROD-02.4 | Streaming protection with a coverage timeline | P2 | M3 | impl | 02.3 | — | k8s | A | Proposed |
| 3 | PROD-03.1 | Capture a usable registry dependency set | P2 | M3 | impl | 03.0, 01.2 | OD-2 | compose | A | Proposed |
| 3 | PROD-03.2 | Preview and execute schema-aware recovery | P2 | M3 | impl | 03.1, 08.1 | OD-2 | k8s | A | Proposed |
| 3 | PROD-05.3 | Export access policy for review | P2 | M3 | impl | 04.0, 04.0b, 04.0d, 05.1 | — | compose | A | Proposed |
| 3 | PROD-07.2 | Make interruption honest | P2 | M3 | impl | 07.1 | — | k8s | A | Proposed |
| 3 | PROD-07.3 | Resume within proven semantics | P2 | M3 | impl | 07.2, 00.3 | — | k8s | A | Proposed |
| 2 | PROD-16.3 | Standing rehearsal authorizations without a personal key | P2 | M2 | impl | 16.2 | — | k8s | A | Proposed |
| 3 | PROD-08.2 | Measure recovery objectives | P2 | M3 | impl | 08.1 | — | k8s | A | Proposed |
| 3 | PROD-08.3 | Full streamed replay comparison | P2 | M3 | impl | 08.1 | — | compose | A | Proposed |
| 3 | PROD-09.1 | Make archive protection observable | P2 | M3 | impl | 09.3, FX-7, 01.5 | — | compose | A | Proposed |
| 3 | PROD-09.2 | Verified secondary copy | P2 | M3 | impl | 09.1 | OD-4 (provider rows) | compose | A | Proposed |
| 3 | PROD-10.1 | Benchmark and expose bounded controls | P2 | M3 | impl | 01.5, 02.2, FX-2 | — | compose | A | Proposed |
| 3 | PROD-14.2 | Stable automation and support interfaces | P2 | M3 | impl | — | — | k8s | A | Proposed |
| 4 | PROD-06.1 | Define an application recovery profile | P3 | M4 | research | 01.1, 04.2, 05.2 | — | compose | B | Proposed |
| 4 | PROD-06.2 | Prove application restart after recovery | P3 | M4 | impl | 06.1 | — | compose | A | Proposed |
| 4 | PROD-10.2 | Cost and recovery-time estimates | P3 | M4 | impl | 10.1 | — | none | B | Proposed |
| 4 | PROD-11.2 | Constrained masking for test environments | P3 | M4 | impl | 11.1, 03.2, 00.3 | — | compose | A | Proposed |
| 4 | PROD-12.1 | Verify a replicated target against the archive | P3 | M4 | impl | 04.2, 08.1 | OD-2 | compose | A | Proposed |
| — | PROD-12.2 | Orchestrated cutover | — | — | — | — | — | — | — | Deferred |
| — | PROD-00.3j | OAUTHBEARER from YAML, and MSK IAM (C9) | — | — | — | 00.2 | OD-3: deferred until a buyer asks | — | — | Deferred |
| — | PROD-13.1 / 13.2 | Fleet and hosted control plane | — | — | — | — | — | — | — | Deferred |

Statuses: Proposed, Ready, In progress, Blocked, Done, Deferred. A Blocked row names the missing contract, owner decision or reproducible failure.

## Fix-now defects in shipped code

Found by the 2026-09-23 review. They do not depend on any expansion feature. Evidence is at `df5333f7` unless noted; engine paths are inside the pinned tarball.

| Row | Defect | Evidence | Required fix |
| --- | --- | --- | --- |
| FX-1 | The vendored consumer-group snapshot shape does not match what the engine writes, so a non-empty snapshot fails every drill and backup receipt of that archive. Logweir never enables the snapshot, but upstream kafka-backup archives with it enabled are a supported drill input (`docs/quickstart.md` Path 3). | `crates/logweir-engine-oso/src/vendored/consumer_groups.rs` expects `captured_at`, `state` and `offsets: Vec<Value>`; engine `backup/engine.rs` writes `snapshot_time` and `offsets` as topic → partition → offset. `describe()` maps the parse error to `EngineError::Operational`. The fixture `e2e/fixtures/consumer-groups-snapshot.json` is invented; the xtask drift gate checks only `manifest.rs` and `preflight.rs`. | Parse the engine's shape; replace the fixture with bytes from the engine's writer; add a non-empty regression test; add the file to the drift gate. |
| FX-2 | `Restore.spec.runnerResources` and `RehearsalSchedule` `bounds.runnerResources` are accepted but never reach the Job. | `crates/weirkeeper/src/crds/restore.rs` (documented as "what the runner pod asks for and is capped at"), `controllers/rehearsal_schedule.rs` copies it; `job.rs` builds the container without resources and `RunnerJobSpec` has no resources field. | Apply requests and limits to the runner container, or refuse the field with a condition; mutant evidence; live Job-spec check at the batch refresh. |
| FX-3 | In new-topic (production) restores, signed evidence calls lost compaction and replication factor 1 "intended". | `crates/logweir/src/drill/phase7_verify.rs` `INTENDED` (`cleanup.policy`, `retention.ms`) and the RF/partition divergences carry a scratch-only rationale but apply in every mode. | In new-topic mode record them as deviations "not reconstructed" until PROD-05 applies source settings; versioned field semantics in both verifiers. |
| FX-4 | A denied DescribeConfigs looks like "no overrides", so later parity checks report no divergence. | `render_backup.rs` keeps the engine default `require_topic_configs: false`; engine capture is non-fatal; manifests and receipts carry no coverage flag. | Record per-topic capture coverage (captured / not captured / capture denied) in receipt and catalog; parity is "not assessed" without coverage. |
| FX-5 | Every console restore creates replication-factor-1 topics. | `ui/pages/restore-wizard.js` hard-codes `replicationFactor: 1` (shown read-only in review); the plan grammar has `target.default_replication_factor` (default 1); nothing derives it from the source. | Default to min(source RF from the manifest, target broker count) with an input and the existing `ReplicationFactorExceedsBrokers` check; Playwright journey. |
| FX-6 | Two restore semantics are undisclosed. Transactional topics probably come back with aborted records and commit/abort markers as ordinary data. With non-monotonic timestamps, the point-in-time end can omit an in-window record without detection. Drills pass in both cases because phase 7 compares the target with the archive. | Engine `kafka/fetch.rs` READ_UNCOMMITTED with no control-record filter; `BackupRecord` has no transaction fields; `kafka/produce.rs` non-transactional; `segment/writer.rs` first/last timestamps used by every selector. Read from source, not run; no test produces transactionally. | Disclose both in `docs/verify-a-scorecard.md`, `docs/stability.md` and the restore review screen; PROD-01.1 decides the product rails. |
| FX-7 | A second run under an existing `backup_id` rewrites the manifest in place, so an earlier signed point no longer verifies. This is the residue of platform defect RECEIPT-DUP, which gives each receipt its own point but not its own manifest. | Engine rewrites `{backup_id}/manifest.json` (get-merge-put); the CLI takes `spec.backup_id`; `crates/logweir/src/backup/phase_run.rs` reads the manifest's version id and discards it. | On versioned buckets pin the manifest version id in receipt and catalog and read by version; otherwise refuse a second run under an existing `backup_id`. Skip if the platform run closed RECEIPT-DUP completely. |
| FX-8 | A point-in-time restore over a `LogAppendTime` source selects records by the producers' CreateTime (what the archive holds) and is signed `pass`. Found by PROD-01.1 on 2026-09-29. | PROD-01.1 `lat` row: a point in 2001 restored six records the broker appended in 2026, `pass` 6/6 (`decisions/PROD-01.1-record-semantics.md` §2.3); the engine archives CreateTime (S3); the manifest's `configurations` carries a topic override of `message.timestamp.type`. | Refuse point-in-time selection for `LogAppendTime` topics before any target is created (`PointInTimeByProducerTime`), unless the approved plan says `restore.time_basis: producerTime`, which labels the scorecard, receipt and console. The topic-override arm lands now; the broker-default arm lands with FX-4. Full specification: the record's §9. |
| FX-9 | `logweir drill approve --standing` checks a new authorization's expiry against the wall clock, and so does the countersign path. Global Constraint 1 says every clock-relative check takes `now` as an argument. The committed fixture was minted at a fixed instant (2026-09-01, 30 days), so `crates/logweir/tests/standing_approve.rs` has failed 3 of 4 rows since 2026-10-01: every code push to main is red. Found by the orchestrator on 2026-10-05, in PROD-00.1's integration gate. | `crates/logweir/src/approve.rs:373` (`let now = chrono::Utc::now()` before the already-expired refusal) and `:503` (the countersign expiry check); the test's fixed `issued_at()` (`standing_approve.rs:37-41`); the shared fixture `crates/logweir-core/tests/fixtures/standing-authorization.json`, also read by `crates/weirkeeper/tests/standing_restore.rs`. ci-check log: `claude/artifacts/prod-00-1/integrate/ci-check.log`. | Pass `now` into the mint and countersign checks (GC1): the CLI passes the wall clock, and tests pass a fixed instant, keeping the fixture byte-reproducible and the refusal of an already-expired document. Class sweep: every test whose outcome depends on the wall clock against a fixed instant, found by running the suites under a far-future clock and by grepping clock reads on signing, approval and expiry paths (for example the `2026-12-15` expiries in `check_contract.rs` and `check_framework.rs`). Guard test: the suites pass under a shifted clock. Mutant evidence. |
| FX-10 | Configured values that never reach their consumer. The chart's `checks.discovery.defaultMaxTopics` and `checks.preflight.defaultTimeoutSeconds` are rendered, parsed and range-checked, but the CRDs default the request fields at admission, so the controller never sees "the request named none" and an operator who sets either knob changes nothing. The retention worker's `LOGWEIR_RETENTION_MAX_DELETIONS` / `_MAX_OBJECTS` caps are proven only at their default values. Found by FX-2's class sweep on 2026-10-05. | `charts/logweir/values.yaml:78,82`, README `:97,100`; `crates/weirkeeper/src/check/policy.rs:145,173,370-372,388-390` (the only readers); `config/crd/topicdiscoveries.yaml:81` (`default: 20000`), `config/crd/preflights.yaml:313` (`default: 120`); `crates/logweir-retention/src/lib.rs:404-411` and its only test, `tests/worker.rs:102-103`, which sets exactly 50/20000. Evidence: `claude/fx-2.result.md` §7. | Apply the two knobs (drop the schema defaults so absent resolves to the installation default) or withdraw them (remove them from values, README and parser; FX-2 recommends withdraw), with a written rationale and the upgrade and rollback behaviour. Add a non-default row for the retention worker's caps. Sweep the class: every chart value or env-borne cap is proven at a non-default value to reach its consumer, or is refused or removed. |
| FX-11 | A Job pod that a `ResourceQuota` or `LimitRange` rejects is reported late or without its reason outside `Backup`, `Restore`, `Preflight` and evidence fetch. `TopicDiscovery`, the catalog sync and a dynamic `Backup`'s discovery read no Events, so they report `PodNotStarted` until the Job's deadline, then `DeadlineExceeded` / `DiscoveryFailed`, without the admission's words. The `KafkaCluster` probe ends `NoExitCode` after 120 s with `reachable` untouched; `ProtectionPolicy` delivery ends `Failed` "with no exit code" across three attempts; retention ends `RunFailed`. Their comments say the controller holds no `events` grant, but it holds `list`. Found by FX-2's fix round on 2026-10-05. | `controllers/topic_discovery.rs:1776`, `controllers/recovery_catalog.rs:958,1009`, `controllers/backup_selection.rs:1427` (`events: &[]`); `charts/logweir/templates/clusterrole.yaml:95-99` (`list` on events); `controllers/kafka_cluster.rs` STEP 4; `protection.rs:2179-2182`, `controllers/protection_policy.rs:1518-1590`; evidence: `claude/fx-2.result.md`, fix round, L5. | Read `FailedCreate` Events through the shared classifier (`check/waiting.rs`) in every Job-owning controller, and fail fast with the admission's message (`PodCreateRejected` or `PodCreationForbidden`), as `Preflight` does. Fix the stale comments, and add one kube-mock row per controller with a quota-rejected pod and its negative control. |
| FX-12 | weirkeeper's evidence verification judges a signing key's trust window against the wall clock below its entry point, against Global Constraint 1 (FX-9's class, in the controller). `VerificationResult::signed` reads `Utc::now()` and judges the verdict at it, gating `is_pass()` and the receipt facts copied onto a status. It is reached from five pub functions that take no `now` (`verify_evidence`, `verify_fetched`, `verify_resolved`, `verify_fetched_resolved`, `verify_relayed`) and from the `verify_oracle` closure, so a reconcile pass judges with two clock reads. The rows at `tests/verification.rs:2086,2208` fail from 2099-01-01. Found by FX-9's review on 2026-10-05. | `crates/weirkeeper/src/verification.rs:459-471` (and `:420-432`, `:767`, `:816`, `:942`, `:975`, `:1014`, `:1742`); `controllers/backup.rs:5818-5824,5876`; evidence: `claude/fx-9.review.md`, the classification section. | Thread `now` through every `verify_*` function and the oracle (the closure can take `(EvidenceRef, DateTime<Utc>)`), with `backup.rs` and `restore.rs` passing the pass's `now`. Add guard rows at fixed instants that fail if any verification path reads the clock; mutant evidence; `just test-shifted-clock` green. The same class, no time bomb today (FX-9's report §8.2): give `reconcile_policy` (`controllers/trust_policy.rs:830`), `reconcile_roster` (`trust_roster.rs:221`), `reconcile_destination` (`backup_destination.rs:135`), `reconcile_discovery` (`topic_discovery.rs:1298`) and `phase0_admit::run` (`crates/logweir/src/drill/phase0_admit.rs:672`) an `_at(.., now)` form, as `approval::decide_with_policy_at` already has. |
| FX-13 | The sign-in limiter is one global budget behind an ingress. `/auth/login` and `/auth/callback` are limited to 20 requests per 60 s per socket peer, and behind the shared console's ingress every user has the same peer: the proxy. About ten sign-ins a minute in total are possible, and one unauthenticated client can block every sign-in for a minute at a time. Found by the 2026-10-07 auth review. | `crates/logweir-api/src/auth/ratelimit.rs:11-19` ("behind one ingress the limit is a global limit"), `:32-33`; `crates/logweir-api/src/http.rs:143-149` keys `login_limiter.check(peer)`; `crates/logweir-api/src/trusted_proxy.rs:113` already decides whether a peer is a trusted proxy. | Key the limiter on the client address only when the immediate peer is a trusted proxy (the rightmost forwarded address that is not itself a trusted proxy); keep the peer otherwise, so an untrusted peer's forwarded header never chooses a bucket. Keep the bounded map. Update the module comment and D0's wording. Tests: two clients behind one trusted proxy get separate budgets; a forged header from an untrusted peer is ignored (negative control); mutant evidence. Live: the PoC's Traefik ingress, at the batch refresh. Must merge before the v0.2.0-rc.1 tag (2026-10-07 decision). |
| FX-14 | Owed by FX-7 and PROD-08.4 (2026-10-05 reviews). (1) The restore preflight reads the manifest without the pin, so it can pass a point the binding then refuses. (2) The documented minimal grants predate `s3:GetObjectVersion`, which a pinned read needs. (3) MR-L2: a conditional drop of a noted point's coverage in `check_v2_bindings` survives the `logweir-core` and `logweir` suites (fail-safe; guarded only at the runner seam). (4) The `RehearsalSchedule` CRD says `recordsPerPartition` is how many records the canary restores, and `docs/formats/drill-scorecard.md` says `sample.records_restored` is how many were restored; both are the count read back for the sample. | (1) `crates/logweir/src/check/kinds/restore.rs:285`; (2) `docs/kubernetes.md:470-480`; (3) `claude/fx-7.review.md` (merge review), `check_v2_bindings`; (4) `crates/weirkeeper/src/crds/rehearsal_schedule.rs:286` (copied into `config/crd/rehearsalschedules.yaml:104`, `charts/logweir/crds/` and the renders), `docs/formats/drill-scorecard.md:252`, `phase7_verify.rs:1056-1057`. | (1) Read the pinned version where the receipt carries one, with the same `Superseded`/unchecked outcomes as the binding, and a row; (2) correct the grants table and measure them; (3) a row that kills MR-L2; (4) correct both descriptions and re-render. |
| FX-15 | Owed by FX-10's class sweep (2026-10-07; all LOW, none an inert knob like FX-10's pair). Five configured values reach their consumer but are proven only at the default; two published fields are parsed and read by nothing; a retention worker's refusal never reaches the `RetentionPolicy` status. | `claude/fx-10.result.md` "Class sweep owed" items 1–7 at `50e945a7`: `controllers/backup.rs:1582` → `evidence_fetch.rs:666`; `controllers/restore.rs:5078-5098`; `check/policy.rs:306,309` (`legacyArchiveAddressing.allowHttp`/`.virtualHostedStyle`); `crates/logweir-api/tests/chart_config.rs:37,46,73`; `charts/logweir/templates/identity.yaml:188-199` → `crates/logweir/src/identity.rs:290-325`; `controllers/backup_selection.rs:1547`; `controllers/retention_policy.rs:1085,3595`. | A non-default row for each of the five values; for `legacyArchiveAddressing`, wire the `:from-legacy` route or withdraw the two fields; put an exit-3 refusal's code and variable on the `RetentionPolicy` status. Consider, for the next chart minor, accepting only the exact old defaults for FX-10's withdrawn keys and failing the render otherwise (NOTES is not seen in GitOps flows). |
| FX-19 | After FX-11, the KafkaCluster controller reads a probe Job that Kubernetes is deleting (its TTL) as a crashed probe, so it clears `reachable` until the next probe answers (observed live on one PoC connection for about 17 s), and each probe cycle logs about 12 WARN lines. Found by PoC batch 2 on `v0.2.0-rc.1`. | `crates/weirkeeper/src/controllers/kafka_cluster.rs:1341-1430`; `claude/poc-batch-2.result.md`. | A Job with a deletion timestamp, or whose verdict was already recorded, is neither a crash nor a reason to clear `reachable`; WARN only on a real crash, once per Job. Rows with negative controls; mutant evidence; a live check at the next PoC batch. |
| FX-16 | The CLI runner never ties the restored backup set to the bound recovery point. `pick_backup_set` chooses by `source.backup` (`latestCompleted` included), and `verify_point_binding` checks the receipt's own manifest, but nothing compares that manifest with the restored set's digest after `describe`, nor the receipt's `backup_id` with `source.backup`; the controller's readiness preflight does. Since FX-8, a refusal decision is taken from the bound receipt's coverage, so a plan binding point A while restoring set B applies A's recorded timestamp type to B's records. Reaching it needs an approver to sign a mismatched plan; rehearsals pin `backup: point.backup_id`. Found by FX-8's review (L-7). | `crates/logweir/src/drill/mod.rs:3315-3337` (`pick_backup_set`); `binding.rs:193,296`; `crates/weirkeeper/src/controllers/preflight.rs:2735` (the controller's comparison); `rehearsal.rs:813`. | When `source.point` is set, refuse with exit 3 before phase 2 if the restored set's manifest digest differs from the bound point's `manifest_sha256` or its `backup_id` from the receipt's; a row per mismatch with a negative control; mutant evidence. |
| FX-17 | The console's Catalog view lists points that it then calls "not offered": on the PoC, the first page (20 of 200 shown) all read "not offered: the catalog published this point's receipt key as [redacted].receipt.json …" while the counts read 368 total / 368 available. The point route a schedule page offers is unaffected. Pre-existing (console round 5's O7), observed live by poc-batch-1 at `ea9788cf`. | `claude/poc-batch-1.result.md` §6 O-2 and `claude/artifacts/poc-batch-1/post/catalog.txt`. | Find why the view's point rows carry a redacted receipt key, make the catalog view offer what the catalog publishes (or say exactly why a point is not offered), with a Playwright row on the PoC and a negative control. |
| FX-18 | (1) `crates/logweir-api` `local_admin` `the_connection_ceiling_holds_and_then_releases` failed under host load in the FX-3, FX-8 and FX-13 workers' full runs and passed on re-run. (2) `scripts/live/poc/*.mjs` call `chromium.launch()` bare, so they fail on a host whose Playwright browser cache lacks the pinned headless shell. (3) FX-2's L2b predicate says "an empty line" where the API server prints `{}`. | the workers' reports of 2026-10-07; `claude/poc-batch-1.result.md` §6 O-3, O-4. | (1) Make the test independent of scheduling latency, or bound its timing with a measured margin, and prove it under load; (2) an executable override (environment variable) or a documented `npx playwright install` step; (3) correct the predicate in FX-2's report and in PROD-10.1's acceptance. |

### Fix-now completion records

**FX-1 — Done (2026-09-29).**
- **Ownership:** worker `fx-1` (one run and one fix round), independent Tier-A review `claude/fx-1.review.md`. The review was ACCEPT-WITH-FIXES (1 MEDIUM, 5 LOW, no HIGH), with 11 mutants killed including 8 of the reviewer's own. The orchestrator read the fix round. Merged as `b4103221` (branch tip `54e8c892`); the full `scripts/ci-check.sh` passed with rc 0 on `b4103221`.
- **Fix:**
  - `crates/logweir-engine-oso/src/vendored/consumer_groups.rs` parses what the engine writes: `snapshot_time`, and `offsets` as topic → partition → offset. The writer is identical from engine 0.19.1 to 0.22.0.
  - Unknown fields are kept. Values the engine never writes are refused: a bad partition key, a negative offset, a repeated group, a repeated topic or partition key.
  - A snapshot Logweir cannot read is a value (`Unreadable`), never an `Operational` failure. `drill run`, `restore run` and `backup run` now tell the operator: a `warning:` line on stderr and a WARN log event (`ArchiveNotice`), with nothing signed changed.
- **Fixture:** `e2e/fixtures/consumer-groups-snapshot.json` is now the pinned engine's real bytes, with the broker's committed offsets as the oracle. Provenance is in `e2e/fixtures/README.md`.
- **Drift gate:** the xtask gate covers `consumer_groups.rs`, compares field types and serde wire attributes, refuses an ambiguous struct name, and runs in `cargo test`.
- **Evidence** (compose slot 1, and the reviewer's slot 4): before the fix, a drill or `backup run` over an archive with a non-empty snapshot exited 1 with no scorecard and no receipt. After it, snapshots from engines 0.19.1–0.22.0 parse exactly. Archives from 0.21 and 0.22 pass, with both verifiers VALID. Archives from 0.19 and 0.20 end signed `fail-integrity`/`partial`, because they carry no segment digests; that part belongs to PROD-00.1, not this row.
- **Migration:** none. Archives without a snapshot behave as before.
- **Left for PROD-04.1:** carrying the snapshot's state into signed evidence.
- **Artifacts:** `claude/artifacts/fx-1/`.

**FX-9 — Done (2026-10-07).**
- **Ownership:** worker `fx-9` (one run and one fix round), independent Tier-A review `claude/fx-9.review.md`: ACCEPT-WITH-FIXES, no HIGH; MEDIUM-1 (the shim leaked the shifted clock into cargo, which ran its global-cache GC "in 2100") and LOW-1..3 were fixed in the fix round and read by the orchestrator. The core fix landed first as `84230a3d` and `79c0d41d` (integration batch 1, `dce54bda`); the harness as merge `f5e81c9a` (batch 2, `79623859`).
- **Fix:** `crates/logweir/src/approve.rs`: `mint_standing`, `countersign` and `mint` take `now`; `run` and `run_countersign` read the wall clock once and hand it down. The already-expired refusals keep their text and exit 1. No flag or environment variable moves the clock in product code. The shared fixture `crates/logweir-core/tests/fixtures/standing-authorization.json` is byte-identical.
- **Class sweep:** every wall-clock read in `crates/*/src` and `xtask/src` classified (report §7), plus the whole workspace suite run at +366 days and at 2100-01-02 under a clock shim. It found four more time-bomb groups and fixed the three that were test-only (2026-12-15 discovery attestations, two of them vacuous rather than red, proven with mutants V1/V2; a 2050 OIDC test certificate; a 2099 approval row). The fourth, `crates/weirkeeper/src/verification.rs:460`, is a product change and became FX-12.
- **Guard:** in-suite rows at fixed instants (2001, 2999) that fail on every date if the mint or countersign reads a clock, and `just test-shifted-clock`, which runs the suite with the test binaries' clocks shifted while cargo, rustc and build scripts keep the real clock and cargo's cache GC is disabled under it, with a self-check.
- **Evidence:** 12/12 Tier-A mutants on `approve.rs` killed (report §4), plus 5 of the reviewer's own. At the fix-round tip `ed7f475e`: `just test-shifted-clock` 4150 passed, 0 failed, 15 ignored; `cargo test -p logweir` 1180/0/9; `just lint`, links, doc_lint and gate_lint green. Main CI green on both batches: run 37363593960 (`dce54bda`) and run 37377692658 (`79623859`).
- **Migration:** none. CLI flags, refusal texts, exit codes and documents are unchanged. `docs/gates.md` gains the recipe.
- **Follow-ups:** FX-12 (the controller's verification clock reads, and the five reconcile entry points of report §8.2, folded into its row); FX-9a (a Linux shim, P3); a weekly macOS CI job for the shifted-clock suite is possible now and is left to PROD-14.2's CI review.
- **Artifacts:** `claude/artifacts/fx-9/`.

**FX-7 — Done (2026-10-07).**
- **Ownership:** worker `fx-7` (a run, a fix round and the renumber to 1.2.0 after FX-4 merged first), independent Tier-A review `claude/fx-7.review.md`: ACCEPT-WITH-FIXES with H-1 (a byte-identical COPY of a pinned point read as `Conflict`), fixed and re-checked; merge review ACCEPT (MR-L1 fixed by the orchestrator in `202e5a07`, MR-L2 owed). Integrated in batch 3: merge `709df5ae` (branch tip `7ed3dd4e`, merged as `208b12da` on `claude/integrate-3`); the owner's V3 ruling (MINOR, OD-7's third case) in `fde0ed8b`. Gate: `scripts/ci-check.sh` rc 0 on `fde0ed8b` (`claude/artifacts/integrate-3/ci-check-2026-10-07.log`).
- **Fix:** after winning the execution claim and before the engine, `logweir backup run` refuses a set the archive already holds (exit 1 `ExecutionAlreadyClaimed`) on every bucket type, which closes RECEIPT-DUP-UPGRADE-WINDOW and a CLI re-use of a pre-claim `backup_id`. On versioned buckets the receipt and catalog record pin `archive.manifest_version_id` (receipt and catalog-point format 1.2.0; unversioned receipts stay FX-4's 1.1.0 byte for byte). The point binding refuses a superseded pin (exit 3 `PointBindingMismatch`), the deep check reports it `Conflict` with the superseded remedy, and a copy that cannot serve the pinned version falls back to the digest with a note.
- **Evidence:** live rows on compose (slot 3 at `0228b227`, slot 2 at the tip): the second run refused on MinIO and SeaweedFS (versioned, Object Lock) with the first receipt still verifying in both readers; a v0.1.5 rollback rewrite detected as `Conflict` on the versioned bucket; copies `Available` with the note; Object Lock retention kept the pinned version (`claude/artifacts/fx-7/live/`, `live/rn/results.txt`). 23/23 mutants killed at the tip, plus the fix round's. Gates at `7ed3dd4e`: workspace tests 4207/0/15, `just verify-py` 115, verifier parity and invariant corpus, schema, CRD and chart checks, `just lint`, links.
- **Migration:** nothing to migrate; older readers ignore the new field inside major 1; on rollback an older runner ignores the claim, the set check and the pin, and on a versioned bucket the points this build signed are then reported `Conflict`. Documented in `docs/stability.md` (FX-7 section), `docs/formats/{backup-receipt,catalog-point}.md`, `docs/kubernetes.md`, `docs/support-matrix.md` and release-notes item 11.
- **Limits:** an unversioned bucket detects a rewrite only through segment digests, which no reader checks yet; an older runner still RUNNING when its Job is re-created, having written nothing, is seen by neither check (let it finish before upgrading).
- **Follow-ups:** FX-14 (the restore preflight's pin, the `s3:GetObjectVersion` grant, MR-L2); PROD-09.2 gains the acceptance line "in a view that merges two buckets, a location whose `Available` rests on an unchecked pin never outvotes a sibling's superseded `Conflict`, and the merge keeps every location's remedy" (`crates/weirkeeper/src/catalog_view.rs` `merge_entries`); PROD-01.4/02.1's generations take receipt and catalog-point 1.3.0.
- **Artifacts:** `claude/artifacts/fx-7/`.

**PROD-08.4 — Done (2026-10-07).**
- **Ownership:** worker `prod-08-4` (a run and a fix round), Tier-B review `claude/prod-08-4.review.md`: ACCEPT-WITH-FIXES (no HIGH, four MEDIUM, seven LOW), all fixed in the fix round; merged in batch 3 (`709df5ae`, branch tip `550d0327` as `bd0c78f8`). Gate: `scripts/ci-check.sh` rc 0 on `fde0ed8b`.
- **Delivered:** `docs/control-evidence.md` maps Logweir's evidence fields to DORA Art. 11–12 and RTS 2024/1774, ISO/IEC 27001:2022 A.8.13, SOC 2 A1.3, NIS2 implementing regulation 2024/2690 Annex §4.2 and HIPAA 164.308(a)(7): 11 supported-statement tables, every supported statement citing an exact field and its format document, every limit linked to a named gap, and no compliance claim. `docs/verify-a-scorecard.md` and the README link it. `crates/logweir/tests/control_evidence.rs` (13 rows) guards the cited fields against the newest schema and refuses compliance wording; 29 mutants, 0 unexpected.
- **Limits:** the SOC 2 2022 points of focus and ISO's Annex A table were confirmed from secondary sources (paywalled). The page's gap sections must be refreshed in the same change as FX-3, FX-8, PROD-02.1, 03.0, 04.x, 08.1, 08.2 and 09.1.
- **Follow-ups:** FX-14 item (4).
- **Artifacts:** `claude/artifacts/prod-08-4/`.

**FX-10 — Done (2026-10-07).**
- **Ownership:** worker `fx-10` (a run and a fix round), independent Tier-A review `claude/fx-10.review.md`: ACCEPT-WITH-FIXES (MEDIUM M1: the retention ceilings were proven only below their defaults, so a worker clamping a raised ceiling passed; LOWs L1–L5), all fixed in the fix round and read by the orchestrator. Merged as `1ca0cf1c` (branch tip `50e945a7`). Gate: `scripts/ci-check.sh` rc 0 at `50e945a7` (`claude/artifacts/fx-10/integrate/ci-check-2026-10-07.log`).
- **Fix:** the chart's `checks.discovery.defaultMaxTopics` and `checks.preflight.defaultTimeoutSeconds` are WITHDRAWN: the CRDs default the request fields at admission, so the controller never saw "the request named none". The schema still accepts both keys so old values files upgrade; the policy document keeps fixed compatibility values so older controllers still read it; `NOTES.txt` prints `WITHDRAWN VALUES ARE SET AND IGNORED`, naming each key set. The retention worker refuses an absent or unreadable per-run ceiling, and the ceilings are proven above and below their defaults on both hops (worker 75/30 000 and 7/1234; controller 55/30 000 and 2/12 345).
- **Class sweep and guard:** all 153 chart values pass a per-value render probe; `crates/weirkeeper/tests/chart_policy.rs` lists every read site of every policy value, and an unlisted reader fails it.
- **Evidence:** 35 first-round mutants and 18 fix-round mutants killed, including the reviewer's M3 and M5; weirkeeper and logweir-retention 1775 passed, 0 failed; `just lint`, CRD, chart, render-install and schema checks green.
- **Migration:** an install that never set either key keeps a byte-identical policy document and digest. One that set another value gets the fixed value once: its digest changes and its still-valid `ready` Preflights read `unknown` (`policyChanged`) until run again (chart README, release-notes item 23). `helm rollback` restores the previous document.
- **Follow-ups:** FX-15 (the seven owed LOW items).
- **Artifacts:** `claude/artifacts/fx-10/`.

**FX-3 — Done (2026-10-07).**
- **Ownership:** worker `fx-3` (a run, a resume and a fix round), independent Tier-A review `claude/fx-3.review.md`: ACCEPT-WITH-FIXES (F1 MEDIUM: nothing refused a 1.2.0 `newTopic` scorecard claiming `not_reconstructed: []` beside intended settings; F2, F3 LOW), all fixed and read by the orchestrator. Merged into main after integration batch 3 (branch tip `895549fc`). Gate: `scripts/ci-check.sh` rc 0 at `895549fc` (`claude/artifacts/fx-3/integrate/ci-check-2026-10-07.log`).
- **Fix:** in `newTopic` mode, phase 7 (`crates/logweir/src/drill/phase7_verify.rs` `classify_parity`, mode passed from `drill/mod.rs`) records lost compaction, `retention.ms`, partition count and replication factor as `unexpected_divergence` plus the new optional `topic_parity.not_reconstructed`, never `intentionally_deviated`; scratch drills are unchanged. Scorecard format 1.2.0 (frozen 1.1.0 schema kept), verifier script 1.17.0. Arms NR-1 to NR-5 in both readers fire only on a document carrying the block (NR-4 and NR-5 only in `newTopic` mode) and can only refuse. New readers re-read a pre-1.2.0 `newTopic` document's `intentionally_deviated` as not reconstructed (weaker, never stronger). Two pre-existing two-reader splits (array shape, `format_version` parse) were closed.
- **Owner ruling:** the content change to the two existing arrays is MINOR (OD-7's third case, 2026-10-07), stated in `docs/stability.md`, with a guard that no doc calls it pending.
- **Evidence:** live row on compose slot 1 at `7c804375` (a compacted RF-3 source on `cluster3`, restored as `newTopic` and as a drill, the broker as the oracle; `claude/artifacts/fx-3/live/`); later changes were readers, docs and tests, and the review confirmed the live row still represents the tip. Mutants: 11/11 (first round) and 9/9 (fix round, including the reviewer's survivor R6). Workspace tests 4275 passed, 0 failed; `just verify-py` 122; parity and invariant corpus green.
- **Migration:** older readers accept 1.2.0 scorecards without misreading them (the old-reader table in the report); release-notes item 24.
- **Artifacts:** `claude/artifacts/fx-3/`.

**PoC batch 1 (2026-10-08) closes FX-2, FX-4, FX-5 and FX-6.** Worker `poc-batch-1` upgraded the PoC (Helm release `logweir`, rev 13 at `sha-fdb48cd8` → rev 15 at `sha-ea9788cf`) with `deploy/poc/README.md`'s procedure, after checking that the new chart left both demo brokers' pod templates unchanged (their pods kept their uids, so the emptyDir data survived; the README now carries that check as step 1b). The README's own checks passed: no ERROR or Forbidden from the controller, sign-in through Dex, 368 catalog points, the pre-upgrade backup's badge, a pre-upgrade point restored and verified, and all 370 receipts verifying independently. 37 rows PASS, 0 FAIL, 3 notRun (FX-2's L3/L4, below; the first scheduled slot on the new controller, due 2026-10-09 02:00Z). Report and artifacts: `claude/poc-batch-1.result.md`, `claude/artifacts/poc-batch-1/`.

**FX-2 — Done (2026-10-08).** Worker `fx-2` (a run and a fix round), Tier-A review `claude/fx-2.review.md` ACCEPT-WITH-FIXES (M1 and L1–L8 fixed). Merged as `dce54bda` (integration batch 1; main CI run 37363593960 green). `Restore.spec.runnerResources` and `RehearsalSchedule` `bounds.runnerResources` now reach the runner container, or the object is refused (`ExecutionSpecInvalid`); ceilings are never clamped; quota and LimitRange rejections are surfaced as `PodCreationForbidden`. Live on the PoC at `ea9788cf`: L-pre (the inventory before the upgrade), L0, L1, L1b, L2 and L2b PASS. L3 (the full apply path) and L4 (the quota interplay) need a governed live row and move to PROD-10.1, which PROD-16 makes runnable without lab key files.

**FX-4 — Done (2026-10-08).** Worker `fx-4` (a run and a fix round), Tier-A review `claude/fx-4.review.md` ACCEPT-WITH-FIXES (fixed; M5's entry ruled MINOR under OD-7). Merged as `fdef4d4d`. Receipts, catalog points and scorecards record per-topic configuration capture coverage (format 1.1.0); parity is "not assessed" without coverage; the reader-layer DescribeConfigs refusal is named. Live on the PoC: new receipts and catalog points carry `config_coverage`, and the false-ready case is closed against a broker that refuses the read (the old build said `ready`, the new one `unknown` / `BrokerConfigsNotReadable`).

**FX-5 — Done (2026-10-08).** Worker `fx-5` (a run and a fix round), Tier-B review `claude/fx-5.review.md` ACCEPT-WITH-FIXES (M1, M2, L1–L6 fixed). Merged as `79623859`. The restore wizard's replication factor is an input defaulted from the target's broker count (at most 3) until PROD-05.1 projects the source's, refused above the count before Create, with its source and storage effect disclosed. Live on the PoC through Traefik and Dex: P-1 to P-7 PASS at 1440 and 390 pixels wide, Create never clicked.

**FX-6 — Done (2026-10-08).** Worker `fx-6`, Tier-B review `claude/fx-6.review.md` ACCEPT-WITH-FIXES (fixed). Merged as `d378805b`. The measured transaction, timestamp and header semantics are disclosed in `docs/verify-a-scorecard.md`, `docs/stability.md` and the restore plan step. Live on the PoC: the step-6 sentence shows on the restore review screen at both widths.

**The nightly backups.** Both PoC schedules failed every 02:00Z slot from 2026-10-02 to 2026-10-05 ("the runner exited 1 (operational)"); their pod logs were lost with the node restart, so the exact error is not recoverable. The 2026-10-08 02:00Z slot of both SUCCEEDED (400 records, verified) after the 2026-10-07 re-seed of the source topics, consistent with the source having been emptied by retention. A manual run of each schedule after the upgrade also succeeded. A compose reproduction of a scheduled backup over an emptied source is part of PROD-02.1's honest-coverage work.

**Follow-ups found by the batch:** FX-17 (catalog view "not offered"), FX-18 (harness and flake hygiene); a stale `reachable: true` on connections whose probe pod was lost, never re-probed, folded into FX-11's fix round (same file).

**PoC batch 2 (2026-10-08) closes PROD-14.0, FX-8, FX-11 and FX-13.** Worker `poc-batch-2` upgraded the PoC in place to the PUBLISHED `v0.2.0-rc.1` (Helm rev 15 → 17) with `deploy/poc/README.md`'s steps 1, 1b, 2 and 3: step 1b showed no change under either broker StatefulSet's pod template, and both Kafka pods kept their uid and 0 restarts. 27 rows PASS, 0 FAIL, 4 notRun. `deploy/poc/versions.env` now pins the release (publication, tag, chart version and the four image digests) and `validate.sh` renders a release pin (merged from `claude/poc-batch-2`). Report and artifacts: `claude/poc-batch-2.result.md`, `claude/artifacts/poc-batch-2/`.

**PROD-14.0 — Done (2026-10-08).** Worker `prod-14-0` (a run and a fix round; Tier-B review ACCEPT-WITH-FIXES, fixed), merged as `ea9788cf`; the OpenAPI document moved to `1.0.0-alpha.2` (`e74f81b6`). A version tag promotes main CI's `sha-<commit>` publication unchanged, publishes the three CLI archives with notices, the independent verifier and the engine licence, a digest-pinned OCI chart and a GitHub Release; a dispatch is a credential-free dry run. Evidence: dry runs 37715507251 and 37730468445 (`release.sh verify`: 14 assets); the tag `v0.2.0-rc.1` (on `56a60ebe`, publication `2c277dc1`, pushed with the owner's standing approval of 2026-10-07) ran 37733173995, every job green including the release drill; verified as a user would (`claude/artifacts/prod-14-0/rc1-u1/`: SHA256SUMS 14/14, chart pulled byte-identical, the four version-tagged digests equal `release.json`'s, a tampered copy refused); the published chart installed on docker-desktop over the PoC (U0–U7 PASS: pinned digests, CRDs, the upgrade with and without the binding, identity, schedules and receipts surviving, 371/371 receipts verifying with the release's verifier, a 2026-10-01 point restored, the macOS archive's `--version` and `drill countersign --help`). The candidate record in `docs/release-notes.md` is filled (`56a60ebe`, `e305a2cc`). notRun: U8 rollback (optional) and the next scheduled slot under the new build (2026-10-09 02:00Z). Limits: `logweir --version` prints the workspace version `0.1.0`, not the tag (bump before the next candidate).

**FX-8 — Done (2026-10-08).** Worker `fx-8` (a run and a fix round), Tier-A review `claude/fx-8.review.md` ACCEPT-WITH-FIXES (M-1 and M-2 ruled fix-now by the orchestrator, fixed with L-1..L-6, X3b and X4). Merged as `bef9b1a7`. A point-in-time restore over a `LogAppendTime` source is refused (exit 3 `PointInTimeByProducerTime`) before any target exists, for the topic-override and the broker-default arms, unless the approved plan says `restore.time_basis: producerTime`; scorecard 1.3.0 and both readers label it; `RehearsalSchedule` `spec.point.timeBasis` opts a schedule in inside its standing authorization; the console shows the signed time basis and warns. Evidence: compose slot 2 (both arms refused, the opt-in labelled, `not_recorded` when nothing records the type), 28 + 16 mutants killed, and PoC batch 2 (the console rows at 1440 and 390 px, the RehearsalSchedule admission dry run).

**FX-11 — Done (2026-10-08).** Worker `fx-11` (a run and a fix round), Tier-A review `claude/fx-11.review.md` ACCEPT-WITH-FIXES (M1, L1–L4 fixed; PoC batch 1's stale-`reachable` finding added). Merged as `917f0241`. Every Job-owning controller reads `FailedCreate` Events through the shared classifier and fails fast with the admission's words; a probe with no verdict clears `reachable` and is probed again; a refused retention run keeps its lease until the cancelled Job is gone. Evidence: one kube-mock row per controller with its control, 31 + 10 mutants killed; PoC batch 2: the 12 stale PoC connections cleared and were re-probed within seconds. notRun live: the quota rows (they would need a quota in `logweir-poc` or the controller watching another namespace; the kube-mock rows carry them) and O-1's negative control (its fixture is gone). Follow-up: FX-19.

**FX-13 — Done (2026-10-08).** Worker `fx-13` (a run and a fix round after three automated security flags, decided by the orchestrator), Tier-A review `claude/fx-13.review.md` ACCEPT-WITH-FIXES (the design judged sound; the provider-side residual stated). Merged as `78e00ea8`. The sign-in limiter counts each client behind a trusted proxy (the rightmost untrusted `X-Forwarded-For` hop from a trusted peer, IPv6 folded to /64), with no global bucket and a bounded table that never refuses. Evidence: 17 mutants killed; PoC batch 2: two clients through Traefik got separate budgets on `v0.2.0-rc.1`, where the old build refused the second client 429. Follow-up: FX-13a.

## Foundation dependency map

PLAT prerequisites that must be Done before a task ships. Research and contract work may proceed sooner when the task allows. Within a task, its own dependency line defines its start condition.

| Expansion | Required platform capabilities |
| --- | --- |
| PROD-00 | None (CI and engine); GR6 is a recorded ruling that OD-3 may supersede |
| PROD-01 | PLAT-03 readiness; PLAT-14.1/14.2 evidence labels; PLAT-07.1 and PLAT-09.1 live evidence reused |
| PROD-02 | PLAT-03, PLAT-04/05 schedules, PLAT-07, PLAT-09, PLAT-14 status, PLAT-15 catalog, PLAT-16.2 shared segments |
| PROD-03 | PLAT-03, PLAT-07/08, PLAT-11, PLAT-15 |
| PROD-04 | PLAT-01 approval binding, PLAT-03, PLAT-11/12, PLAT-15, PLAT-19 |
| PROD-05 | PLAT-03, PLAT-07, PLAT-11; PLAT-17/19 authority for access policy |
| PROD-06 | PLAT-09 coverage, PLAT-15 |
| PROD-07 | PLAT-12 execution identity and fresh-target retry, PLAT-14 progress, PLAT-16 retention holds |
| PROD-08 | PLAT-14.1/14.2 evidence labels, PLAT-14.3 rehearsals, PLAT-15.1 `CatalogDeepCheck`, PLAT-20.1 journeys |
| PROD-09 | PLAT-08 destinations, PLAT-15 catalog and import, PLAT-16 retention (`VersionedBucket`), PLAT-19.1 signing lifecycle |
| PROD-10 | PLAT-18.2 bounded lists. No PLAT task provides engine metrics or throughput baselines. |
| PROD-11 | PLAT-11 selected point and preview, PLAT-13 drafts, PLAT-17 authorization, PLAT-19.2 governed approval (11.2) |
| PROD-12 | PLAT-01/19 approvals, PLAT-03, PLAT-12/14 |
| PROD-13 | PLAT-17 API, identity and audit (namespace isolation is PLAT-17.2's acceptance), PLAT-19 |
| PROD-14 | PLAT-02 bootstrap, PLAT-15.2 import, PLAT-17 API, PLAT-19.2 ordinary confirmation, PLAT-20.2 docs |
| PROD-15 | PLAT-01 approvals, PLAT-03 preflight, PLAT-11.2 preview, PLAT-19.2 |

## Rules inherited by every task

1. Read the task, its ledger row, the owner decisions it names and the platform sections in the dependency map. Recheck current implementation before coding; this is a dated baseline.
2. A research task ends with a decision record, measured evidence, limits and a concrete contract. It may conclude a capability is unsupported. It must not silently expand into a rewrite. Dependent tasks stay Blocked until the record lands; findings that need several independent changes become child rows in the ledger.
3. Preserve existing archives and immutable plans. Version new manifest, receipt, scorecard, catalog and API fields in both verifiers and the parity script; define absent-field behaviour; prove old readers stay compatible or reject clearly. Never reinterpret old evidence as a stronger guarantee.
4. Keep credentials server-side, scope source, archive and target permissions separately, and record actor, selected recovery point and execution identity. Use platform authorization and approval policy. Signatures do not replace encryption or access control.
5. Every mutation handles failure, shows UI/API states, is idempotent and cleans up within bounds. Default to new target names; never overwrite production topics or change live consumer positions implicitly. PROD-15 is the only original-name path, behind its own approval.
6. Use disposable fixtures from PROD-01.5's profiles. Local emulation does not certify a hosted provider; provider results follow OD-4.
7. Apply the task's tests plus the relevant regressions. Every new guarantee ships with a Logweir-owned oracle test that checks observed outcomes, not engine exit status, and every negative control must be able to fail. Record reproducible commands, versions, fixture sizes, outcomes and limits.
8. A change to a `docs/stability.md` Never entry or an ADR 0008 amendment lands only after its owner decision, together with the doc_lint update, in one commit.
9. Done means acceptance criteria pass, compatibility and migration notes are updated, review findings are resolved and operator documentation matches behaviour. Planning documents do not satisfy implementation tasks. No task authorizes a production rollout.

## PROD-00 — Engine route, currency and supplier risk

**Priority:** P1 (00.1, 00.2), P2 (00.3). **Owner areas:** engine adapter, runner image and CI, support matrix, ADR 0001/0002. **Boundary:** decide and deliver how each required engine behaviour is obtained. Keep Logweir's independent `.kbak` decoder and both scorecard verifiers apart from any engine change, so writer and verifier stay independent. **Risk:** the supplier sells the competing product and gates features several spikes need; one maintainer; the published image is amd64-only.

### PROD-00.1 — Evaluate the engine and decide a route per capability

- **Issue:** The pin (v0.21.0) trails upstream (v0.22.0). `engine-matrix` fails and lacks 0.22.0, and the support docs misstate `strimzi-backup-operator`'s default engine. The capability gaps in the OSO research table decide PROD-01.1, 02, 04, 07, 09.3, 10 and 11, but no task chooses between an upstream PR, a fork, a Logweir-native path and "unsupported".
- **Approach:** Answer from source first, then confirm on the compose drill and G-PITR. Evaluate 0.22.0 (manifest and config changes, `path_style`). Repair `engine-matrix` seeding and publish, add 0.22.0 and a latest-supported-broker row with PROD-01.5, and correct `docs/support-matrix.md` and `docs/stability.md`. Build a capability table covering: control records and READ_COMMITTED; offset-after-upload ordering; conditional manifest publication; restore checkpoint cadence and hash scope; idempotent produce; min/max segment timestamps; topic IDs; ApiVersions negotiation; OAUTHBEARER and MSK IAM; a YAML-exposed filter or transform action; offset-range restore; byte-rate limits. For each: the route, its cost, the supplier-policy constraint and the dependent tasks. Also record whether archives written by OSO's operators (engine 0.19–0.22) import and verify once FX-1 lands.
- **Acceptance:** `decisions/PROD-00-engine-route.md` lists every capability with a proposed route and evidence; `engine-matrix` is green on its declared rows; the support docs match evidence; OD-3 goes to the owner with the recommendation.
- **Tests/evidence:** Compose demo drill and `just pitr` on 0.22.0; an `engine-matrix` run; a source citation per capability.
- **Dependencies:** None. **Handoff:** OD-3 proposal, the capability table and the PROD-00.3 child rows.


**PROD-00.1 — Done (2026-10-07).**
- **Ownership:** worker `prod-00-1` (a run, a fix round and rounds 3–4 after run 36542777892's red v0.22.0 row), independent Tier-B review `claude/prod-00-1.review.md` (ACCEPT-WITH-FIXES, fixed). Merged as `e4145dbf` (branch tip `6c23fe36`) in integration batch 1 (`dce54bda`); main CI run 37363593960 green.
- **Decision record:** `docs/to-do/decisions/PROD-00-engine-route.md` routes 19 capabilities (the 12 the task names, C13–C17 found in source, C18 and C19), each with source citations in the pinned tarball and in v0.22.0, a route, a cost, the supplier constraint, its dependents and numbered acceptance rows; §4 evaluates 0.22.0 with runs, §7 records how operator-written archives (engine 0.19–0.22) import and verify, §8 gave OD-3's options (decided 2026-10-07), §9 the child rows.
- **engine-matrix:** seven causes of the red scheduled runs repaired (`d5d0be9b`, `ff9aa14a`); six declared rows, green only when each records what it declares. Evidence: run 36575640011 at the branch tip, all green; run 37363689044 on main `dce54bda`, attempt 2 (2026-10-07, after the GitHub incident cancelled two rows), all six rows and publish green.
- **Support docs:** `docs/support-matrix.md` and `docs/stability.md` engine and operator statements corrected to evidence (`9817252e`, `eaf391b0`).
- **Child rows:** 00.3a–e (routes F, behind 00.2), 00.3f–i and 00.3o on the ledger since 2026-10-07; 00.3j deferred with MSK IAM (OD-3); 00.3k (only if PROD-02.3 picks the engine's continuous mode), 00.3l (topic IDs went the OD-6 FFI route), 00.3m (only if a broker row fails) stay in §9 until their triggers fire; 00.3n needs its own trust-model decision.
- **Limits:** OSO shipped 0.23.0–0.23.3 after the record (the last on 2026-10-07); 00.3f re-evaluates on the newest release, including 0.23.0's rewritten `partition_router.rs`.
- **Artifacts:** `claude/artifacts/prod-00-1/`.
### PROD-00.2 — Build the engine from the vendored source

- **Issue:** The runner copies OSO's amd64-only binary and never builds the vendored source. That blocks arm64 nodes, forces emulation on the arm64 development host (distorting benchmarks), leaves engine CVEs waiting for upstream and makes every fork option theoretical. No image is signed or ships an SBOM or provenance, `cargo deny` never sees the engine's dependency tree, and `SECURITY.md` excludes engine vulnerabilities.
- **Approach:** Build `kafka-backup` from the vendored tarball (or the evaluated successor) in CI for linux/amd64 and linux/arm64. Compare behaviour with the pinned OSO binary on the compose drill and G-PITR. Run `cargo deny` over the engine lockfile, emit an SBOM and signed provenance, and sign the four images. Record an ADR amendment that supersedes GR6 or scopes it to "unmodified upstream until a recorded fork decision".
- **Acceptance:** A multi-arch runner built from source produces the same drill and G-PITR results as the pinned binary; arm64 installation is documented; images carry signatures and provenance; the engine digest pin names Logweir's own build.
- **Tests/evidence:** Both architectures on the compose drill; signature and provenance verification; a negative control where modified engine source fails the comparison.
- **Dependencies:** PROD-00.1 and OD-3. **Handoff:** build recipe, parity evidence and rollback to the upstream image.

### PROD-00.3 — Deliver engine capabilities by the recorded route

- **Issue:** Several spikes need behaviour the engine lacks (PROD-00.1's table).
- **Approach:** One child row per capability, added to the ledger when OD-3 records its routes (for example `PROD-00.3a` READ_COMMITTED capture). Each follows its route: an upstream PR with a pinned bump, a patch on the PROD-00.2 build, or a Logweir-native implementation. Each ships a Logweir-owned oracle test.
- **Acceptance and tests:** Per child row, taken from the decision record.
- **Dependencies:** PROD-00.1 and OD-3; PROD-00.2 for patch routes.

## PROD-01 — Establish the recovery contract

**Priority:** P1. **Owner areas:** engine adapter, archive format, verification, support matrix, compose fixtures. **Boundary:** establish supported semantics and reach; engine changes go through PROD-00. **Risk/migration:** historical archives may lack evidence; classify them explicitly instead of inventing fields.

### PROD-01.1 — Prove record and transaction behavior

- **Issue:** Current sampled fingerprints do not establish ordering, transaction boundaries, crash duplication or exhaustive recovery. Source reading indicates two hazards in shipped restores (FX-6), and the drill cannot see either because it compares target with archive. [Kafka design](https://kafka.apache.org/40/design/design/)
- **Approach:** Start from the source findings in PROD-00.1, then confirm with a compose fixture from PROD-01.5, or built here if 01.5 has not landed. It covers: a transactional producer that commits and aborts; non-monotonic CreateTime within one segment; a LogAppendTime source; keys, nulls, tombstones and duplicate headers; compaction; topic recreation. Define supported guarantees separately for capture, replay and verification, and what the archive cannot represent. Decide the product rail for transactional topics (refuse, label, or support through a PROD-00.3 capability) and a detection route (rdkafka 0.36 exposes no DescribeProducers).
- **Acceptance:** A capability contract states each guarantee, known counterexample, isolation setting and archive prerequisite. Unsupported transactional or exactly-once recovery is blocked from product claims, and the rail becomes a ledger row. Fault injection around acknowledgements is either run or marked blocked on subprocess cancellation (`docs/stability.md` Later #13).
- **Tests/evidence:** Deterministic records across several partitions; committed input (read with read_committed) compared with observed output; equal and non-monotonic timestamps, duplicate headers and tombstones. Record actual outcomes, not engine exit status.
- **Dependencies:** None. **Handoff:** `decisions/PROD-01.1-record-semantics.md`, fixtures, rail rows and constraints for PROD-02, 04, 07 and 08.

**Completion record — Done (2026-09-29), PROD-01.1.**
- **Ownership:** worker `prod-01-1` (one run and one fix round), independent Tier-B review `claude/prod-01-1.review.md`. The review was ACCEPT-WITH-FIXES (5 MEDIUM, 9 LOW, no HIGH), and the orchestrator read the fix round. Merged as `ac6aa00e` (branch tip `f47c675a`); the full `scripts/ci-check.sh` passed with rc 0 on `ac6aa00e`.
- **Decision record:** [`decisions/PROD-01.1-record-semantics.md`](decisions/PROD-01.1-record-semantics.md). It contains:
  - the capability contract for capture, replay and verification (§3);
  - what the archive cannot represent (§4);
  - fault injection (§5);
  - the transactional rail (§6);
  - acceptance rows 02-1…08-7 for PROD-02, 04, 07 and 08 (§7);
  - the FX-6 wording (§8);
  - the new rows (§9).
- **Evidence** comes from the compose fixture: Kafka 3.7.1 single-node KRaft and the pinned engine v0.21.0. Two full passes and the fix-round pass at `44838f31` agree row for row. Every "signed" verdict below is Logweir's own scorecard.
  - **Transactions:** 7 commit/abort markers and the aborted and open-transaction records are restored as ordinary data, and signed `pass` (19/19).
  - **`LogAppendTime`:** the source is archived with the producers' CreateTime. A point in 2001 restored six records appended in 2026, signed `pass`.
  - **Non-monotonic CreateTime:**
    - an in-window record is skipped, signed `pass` (16/16);
    - a correct point-in-time restore is signed `fail` (it misses the count bound);
    - a record below the window floor is dropped: a full restore fails, and a point-in-time restore passes (10/10).
  - **Headers:** a repeated header key keeps one copy, and a record's own `x-original-offset` is lost.
  - **Compaction and topic recreation** restore exactly.
  - **Fault injection:**
    - a lost produce acknowledgement resends batches: 3,000 and 2,000 duplicates, and the engine exited 0 over the second;
    - a broker outage leaves a partial target;
    - no fault run was ever signed `pass`.
  - **Termination injection** stays blocked on subprocess cancellation: a killed `logweir` leaves its engine running to completion (`docs/stability.md` Later #13).
- **Decisions:**
  - **No product claim** may say transactional, exactly-once, `read_committed` or source-faithful recovery; FX-6 publishes the limits.
  - **The transactional rail** is refuse by default, detect, and label an approved override (PROD-01.1a). Committed-only capture comes through PROD-00.3a.
  - **Detection without DescribeProducers** uses three measured signals: control-shaped archive records confirmed by the offset gap, the last-stable-offset gap at the probe, and a tail reconciliation after the engine. Only the third sees a transaction opened after the probe.
- **Tests:**
  - `e2e/tests/record_semantics_oracle.rs`: 22/22. The reviewer's mutants and 20 oracle mutants are all killed.
  - Live rows in `e2e/tests/record_semantics.rs` run under `--features e2e`; contract assertions are gated to engine 0.21.0, and other engine releases only record their outcomes.
  - Support is in `e2e/tests/record_semantics_support/`, with one address place at `kafka.rs:42-68` for PROD-01.5's sweep.
- **Migration:** none for this row, which is evidence and tests. Each new rail carries its own migration notes.
- **Artifacts:** `claude/artifacts/prod-01-1/`.
- **Limits:**
  - one broker line (3.7.1; PROD-01.5 re-runs the rows on 3.9, 4.1 and 4.3);
  - one producer library;
  - the deterministic PROD-07 fixtures are named, not built.
- **Rows added:** FX-8 (also in the fix-now table), PROD-01.1a, PROD-01.1b, and PROD-00.3a–e. The routes for 00.3a–e are PROD-00.1's to propose and OD-3's to decide.

### PROD-01.2 — Publish a tested compatibility contract

- **Issue:** Kafka-compatible endpoints vary in authentication, metadata permissions and administrative operations, and a connection test cannot certify recovery. Confluent Cloud and Azure Event Hubs are unreachable today (they need SASL/PLAIN or OAUTHBEARER), not merely untested.
- **Approach:** Define capability detection as new CheckIds in the closed check vocabulary, and a support matrix by broker version, auth mode, registry and archive backend. Reuse the live evidence PLAT-07.1 (SCRAM, TLS private CA, rotation) and PLAT-09.1 (ACL-denied listing) already hold. Add local Redpanda and Confluent Platform containers as reachable rows. Classify supported, limited, untested and unsupported; managed providers (MSK, Confluent Cloud, Redpanda Cloud, Aiven, Event Hubs) stay untested until OD-4 evidence exists.
- **Acceptance:** Preflight exposes missing capabilities with an actionable fallback; unsupported metadata cannot appear captured. Each supported row has versioned evidence and a minimum-permission profile for probe, backup and restore.
- **Tests/evidence:** Local rows from PROD-01.5; ACL-denied listing and configuration; advertised-address failures; cloud rows only under OD-4, recorded separately from local results.
- **Dependencies:** PROD-00.1, 01.1, 01.3, 01.5; OD-4 for provider rows. **Handoff:** capability identifiers, fixture matrix, supported release boundaries and provider validation gaps.

### PROD-01.3 — Reach managed and mTLS clusters

- **Issue:** Logweir accepts only `plaintext` and `scramSha512` (optionally over TLS). The closed set appears in the CLI, the engine renderer (`AuthRender`), the Kafka client, the `KafkaCluster` CRD, the signed receipt's validator and the Python verifier. The pinned engine already accepts SASL/PLAIN, SCRAM-SHA-256/512 and client certificates in YAML, and librdkafka supports all three.
- **Approach:** Add SASL/PLAIN over TLS, SCRAM-SHA-256 and mTLS client certificates on both clients, with write-only credential references (PLAT-07.1) and versioned `auth_mode` values in receipts, the catalog and both verifiers. OAUTHBEARER and MSK IAM follow the PROD-00.1 route; the engine's plugin seam is not reachable from its CLI.
- **Acceptance:** Each new mode backs up, restores and verifies against the compose listener; old receipts still verify; credentials never appear in status, logs or downloads; Confluent Cloud and Event Hubs move from "unsupported" to "untested".
- **Tests/evidence:** A compose listener per mode (PROD-01.5), wrong-credential and wrong-CA refusals, receipt parity in both verifiers, a live `KafkaCluster` journey on docker-desktop.
- **Dependencies:** PROD-01.5. **Handoff:** auth-mode contract and support-matrix rows.

### PROD-01.4 — Define topic identity and generations

- **Issue:** A recreated topic can reuse a name and offsets. Topic IDs are unavailable through the engine (none in the manifest) and through safe rdkafka (no DescribeTopics in 0.36.2; `logweir-kafka` forbids unsafe code). PROD-02, 04, 07, 11 and 15 consume generation identity without an owner.
- **Approach:** Decide a generation contract: an offset-regression heuristic usable now, a nullable `topic_id` field, and the route to real IDs (FFI exception, upstream wrapper, or engine through PROD-00.3). Define how each consumer reacts to a generation change.
- **Acceptance:** A decision record with the field, its absent-value behaviour, the detection rule with known false positives and negatives, and numbered acceptance rows for PROD-02.1, 04.1, 04.2, 07.1, 11.1 and 15.1.
- **Tests/evidence:** Recreate a topic between runs; delete records; compaction; the heuristic's outcome on each.
- **Dependencies:** None. **Handoff:** `decisions/PROD-01.4-topic-identity.md`.

**Completion record — Done (2026-09-29), PROD-01.4.**
- **Ownership:** worker `prod-01-4` (one run and one fix round), independent Tier-B review `claude/prod-01-4.review.md`. The review was ACCEPT-WITH-FIXES with 1 HIGH, 6 MEDIUM and 9 LOW; the HIGH re-check gave ACCEPT. Merged as `ac76cd0d` (branch tip `e6045f46`); the full `scripts/ci-check.sh` passed with rc 0 on `ac76cd0d`.
- **Decision record:** [`decisions/PROD-01.4-topic-identity.md`](decisions/PROD-01.4-topic-identity.md).
  - **Identity:** the identity of recoverable history is (source cluster ID, topic name, generation). A break is any pair of observations that cannot belong to one continuous history.
  - **Field:** a nullable `topic_id` (Kafka's URL-safe text form, built from the UUID's two 64-bit halves) in receipt 1.1.0, catalog point 1.1.0 and the API. It is captured before and after the engine (`topic_id`, `topic_id_after`), with arms for both verifiers and the parity script. `null` means unknown, never "same", and every existing archive reads unknown.
  - **Heuristic usable now:** five verdicts (`continuous`, `unverified`, `suspected`, `break`, `unknown`). It compares partition counts, READ_UNCOMMITTED log start and end, and the predecessor's archived tail against the same offsets in the current capture's own archive, never against a live read.
- **Evidence:** 19 live rows with broker topic-ID ground truth, identical on Kafka 3.7.1 and 4.3.1.
  - **8 recreations:** the heuristic detects 6 (5 `break`, 1 `suspected`), with 2 known misses; the ID path detects all 8.
  - **11 same-topic rows:** no false `break`. There is one known false positive, a `suspected` after partitions were added and every old tail deleted, and the ID path clears it.
  - **The rejected source-read variant** falsely breaks on `LogAppendTime` and repeated-header rows (c17, c18). That was review H1, fixed at the root; a mutant reverting to a live read fails exactly those rows.
  - **Oracle:** `e2e/tests/topic_identity.rs`, with 25/25 mutants killed and 52/52 at the reviewer's re-check. There is one address place for PROD-01.5's sweep.
- **Finding for every ID consumer:** librdkafka's `rd_kafka_Uuid_base64str` uses the standard base64 alphabet, and Kafka uses the URL-safe one. Build the text from the two halves.
- **Decisions:** each consumer's reaction is in §7, with numbered acceptance rows for PROD-02.1, 04.1, 04.2, 07.1, 11.1 and 15.1 in §8.
  - **Owner choice:** OD-6, one scoped-`unsafe` policy, presented and not taken.
  - **Carried to PROD-02.1's brief** (re-check): line ~414 must not let PROD-02.1 read only each archive's last segment, and a fixture must span segments. Four minor items go to the PROD-02.1 and PROD-00.3c/00.3e briefs.
- **Migration:** none for this row, which is evidence and tests. The receipt and catalog 1.1.0 field ships with PROD-02.1.
- **Artifacts:** `claude/artifacts/prod-01-4/`.
- **Limits:** two broker lines; the byte-identical replay miss (§4.7) stays until IDs exist.
- **Rows added:** PROD-01.4a (gated on OD-6), PROD-01.4b, and OD-6.

### PROD-01.5 — Shared fixture profiles and supported broker versions

- **Issue:** Every fixture pins Apache Kafka 3.7.1 on one combined broker (`e2e/compose/.env`). Apache support for that line has ended, and MSK stopped supporting it on 2026-09-01; no Kafka 4.x broker has been exercised. The compose project name and host ports are fixed, so fixtures cannot run in parallel. Tasks assume environments no task provides: several brokers, a second cluster, extra auth listeners, a lock-capable second bucket, a registry-compatible service, a Kafka Streams application and a transactional producer. MinIO, the only exercised store, is archived upstream.
- **Approach:** Versioned compose profiles with a parameterized project name and ports. Broker lines 3.9, 4.1 and 4.3 through `KAFKA_VERSION`. Optional profiles for: a 3-broker KRaft cluster; a second cluster; PLAIN, SCRAM-256 and mTLS listeners; lock-capable object storage with a second bucket; a registry-compatible service; a pinned minimal Streams app; a transactional producer. Run the demo drill, `just pitr` and the receipt path on each broker line and record support-matrix rows with a broker-version column. Later tasks extend these profiles instead of building private fixtures.
- **Acceptance:** Two fixture runs execute concurrently; the drill passes, or its failures are recorded, on 3.9, 4.1 and 4.3; 3.7.1 becomes a legacy row; the maintained object-store choice is recorded.
- **Tests/evidence:** Profile smoke runs, support-matrix rows, and the engine's unnegotiated protocol versions exercised on 4.x.
- **Dependencies:** None. **Handoff:** profile names and ownership, broker rows.

**Completion record — Done (2026-09-29), PROD-01.5.**
- **Ownership:** worker `prod-01-5` (one run, one fix round, and the reader sweep after PROD-01.1 and 01.4 merged), independent Tier-B review `claude/prod-01-5.review.md`. The review was ACCEPT-WITH-FIXES (2 MEDIUM, 8 LOW, no HIGH), and the orchestrator read the fix round. Merged as `632ea345` (branch tip `856219da`). The full `scripts/ci-check.sh` passed with rc 0 on `632ea345`, and CI run 36536046039 was green, including the `e2e` job on the default stack.
- **Decision record:** [`decisions/PROD-01.5-fixture-profiles.md`](decisions/PROD-01.5-fixture-profiles.md).
- **Parallel stacks:**
  - `e2e/compose/stack-env.sh --slot N` gives project `logweir-e2e-s<N>`, host ports +N×10000, and its own network, volumes and KRaft cluster ids.
  - The default render is byte-identical to before; the reviewer diffed it.
  - One variable list (`stack-lib.sh`) is checked at every entry point, including the Rust harness. Incoherent environments were refused 55/55 before any docker call.
  - Two slots ran drills side by side and tore down only their own projects.
- **Broker lines:** Kafka 3.9.2, 4.1.2 and 4.3.1 (pinned by digest), with 3.7.1 kept as the legacy default. Each passed the demo drill, `just pitr` and the receipt path, and the e2e package passed 75/0 on 4.3.1.
  - The engine's 15 sendable fixed request versions sit inside every line's supported range. Two sit at the 4.x minimum; that headroom is PROD-00.1's ApiVersions row.
  - `docs/support-matrix.md` gains the broker-version column.
- **Object store:** SeaweedFS 4.48 (Apache-2.0) is the maintained choice. It passed conditional create, versioning with version-id reads, Object Lock readback, path-style and SigV4. RustFS 1.0.0 is the runner-up. versitygw v1.8.0 was not chosen: its unknown-key `404 XAdminUserNotFound` misleads Logweir's classifier. MinIO in `charts/` and `deploy/poc/` is untouched; that is REPLACE-MINIO.
- **Profiles:** `auth` (PLAIN, SCRAM-SHA-256 and mTLS listeners), `cluster3`, `cluster2`, `objectstore` (lock-capable, with a second bucket), `registry` and `streams` (classic protocol). The smoke passed 29/29 on slot 2 (4.3.1), with a real negative control for Streams. The `txn` hook is reserved for PROD-01.1's fixture.
- **Guards:** `e2e/tests/stack_params.rs` walks every reader directory recursively and fails on a default-stack address. PROD-01.1's and 01.4's readers were swept onto `bootstrap()`/`s3_endpoint()` with five negative controls, and their live rows ran on slot 3.
- **Migration:** none for users. Workers use slots (the WORKER-RULES "Compose stacks are parallel" section).
- **Artifacts:** `claude/artifacts/prod-01-5/`.
- **Rows added:** PROD-01.5a (move the default broker line; after PROD-00.1), 01.5b (the `crates/` `--features e2e` readers and `just links` over `e2e/README.md`), and 01.5c (PROD-01.1's and 01.4's suites on 3.9, 4.1 and 4.3, which both handoffs owed).
- **Notes for other rows:**
  - C3, a RocksDB Streams variant: PROD-06 if it needs on-disk state.
  - C4, the object-store rows: PROD-01.2.
  - C5: PROD-00.1's ApiVersions row.
  - C6, `logweir-store`'s `StoreErrorClass::classify` mapping versitygw's `404 XAdminUserNotFound` to not-found: PROD-01.2's archive-backend rows.

## PROD-02 — Continuous protection and recoverable history

**Priority:** P1 (02.1, 02.2), P2 (02.3, 02.4). **Owner areas:** capture, receipts, catalog, worker lifecycle. **Boundary:** make scheduled capture honest and incremental first; a long-lived capture worker only after the engine route (OD-3); never a cluster-consistent snapshot claim. **Risk/migration:** generations and append-only, versioned manifests must preserve older archives.

### PROD-02.1 — Show honest coverage for scheduled backups

- **Issue:** A healthy schedule can hide stale or incomplete archives. Receipts record no source watermarks, capture gaps (data expired before capture), pruned ranges or topic generation; gaps live only in the free-text `sample.coverage_note`.
- **Approach:** Before and after each run, record per-partition source low/high watermarks (`fetch_watermarks`), archived ranges, gaps and the PROD-01.4 generation as versioned receipt and catalog fields. The console distinguishes unknown, lagging and protected. Recovery-point selection offers only committed coverage. Freshness reuses `ProtectionPolicy.maxRecoveryPointAgeSeconds` and never treats record event time as a wall clock.
- **Acceptance:** Gaps and regressions are visible per partition in the receipt, catalog, API and console; old receipts read as "coverage not recorded", never as complete.
- **Tests/evidence:** Source retention expiring before capture, topic recreation, a delayed partition, an idle trickle, both verifiers reading the new fields. Also a scheduled backup over a source whose every record retention already deleted (earliest == latest in every partition): the PoC's nightly schedules failed exit 1 "operational" on such a source from 2026-10-02 to 10-05 (PoC batch 1); reproduce it on compose and make it an honest, named outcome (an empty capture with its watermarks), never an operational failure.
- **Dependencies:** PROD-01.4. **Handoff:** coverage fields and UI states.

### PROD-02.2 — Capture incrementally

- **Issue:** Every scheduled run and retry re-reads the whole retained log. Logweir renders no `start_offset` or `offset_storage` and gives each slot a fresh `backup_id`, and the engine starts from the earliest offset. Run time, source load and stored bytes grow with retained data. Runs that overrun their slot skip `Forbid` slots, so RPO degrades as topics grow.
- **Approach:** Seed each run's `start_offset: specific` from the previous committed point's last archived offsets. Keep a fresh `backup_id` so manifests stay immutable. Chain restores across points. Retention treats chains as shared (PLAT-16.2 `SharedSegment`). An offset regression or new generation forces a full copy. Lifecycle rules are flagged on incremental destinations. Do not use the engine's offset-store or continuous modes, which checkpoint before upload.
- **Acceptance:** A restore across at least three chained points is byte-verified. Bytes read and stored per run fall to the new data, measured before and after. A regression forces a full copy under a new generation. Retention never deletes a segment a live chain needs.
- **Tests/evidence:** A growing topic across N runs; regression and recreation; retention over chains; a crash between runs.
- **Dependencies:** PROD-02.1, 00.1. **Handoff:** chain contract and measured savings.

### PROD-02.3 — Decide the continuous capture protocol

- **Issue:** Scheduled capture leaves intervals before newer data is durably archived, and consumed data is not necessarily stored. The engine's continuous and offset-store modes set the offset checkpoint before the segment upload completes and rewrite the manifest in place, so a crash can leave a silent hole.
- **Approach:** Define partition ownership and fencing, durable offset checkpoints, a segment commit protocol, generations (PROD-01.4), low-volume rollover and source-retention gap detection. Model consumed, committed-to-storage and verified watermarks separately. Choose the capture path through PROD-00.3 or a Logweir-native writer, and the long-lived worker kind through an Amendment A decision.
- **Acceptance:** A crash table covers every archive publication boundary, with a bounded takeover process and an explicit response when identity is unavailable. No committed catalog entry references an unfinished object; name reuse cannot merge unrelated histories.
- **Tests/evidence:** Fault injection between upload and manifest publication; topic recreation; expired source data; lost ownership; a steady trickle that never goes idle; recoverable data after each interruption.
- **Dependencies:** PROD-02.2, 01.4, 00.3 per OD-3. **Handoff:** versioned capture and checkpoint protocol, and a go/no-go decision.

### PROD-02.4 — Ship streaming protection with a coverage timeline

- **Issue:** A running worker can look healthy while archived data is stale or incomplete.
- **Approach:** Implement the approved protocol with per-partition progress, bounded backpressure, graceful pause and resume, and coverage intervals in the existing catalog. Add an opt-in continuous mode, gap explanations and recovery-point selection restricted to committed coverage. Resolve future topics with the platform's selector policy.
- **Acceptance:** Worker loss does not lose acknowledged durable coverage. The UI separates unknown, lagging and protected. Selected historical points stay stable. The RPO objective is compared with measured durable progress.
- **Tests/evidence:** Broker and storage outages, rolling worker restart, a delayed partition, a new topic, clock skew, a generation switch, and an upgrade from scheduled-only installations. Restore from committed history with the source offline.
- **Dependencies:** PROD-02.3; platform discovery, readiness, catalog and status. **Handoff:** operating limits, metrics, recovery evidence and rollback behaviour.

## PROD-03 — Recover schemas with the data

**Priority:** P1 (03.0), P2 (03.1, 03.2). **Owner areas:** archive decoding, evidence, registry adapter, restore planner. **Boundary:** 03.0 reads archived bytes only and never contacts a registry. Registry work waits for OD-2 because Never #2 refuses Schema Registry support; it starts with one Confluent-compatible API and Avro. **Risk/migration:** schema import mutates the target, so default to non-destructive mapping and never silently overwrite IDs. OSO sells registry backup, restore and ID remapping as enterprise-only. The open-source engine's restore-time ID rewrite passes unmapped IDs through and applies to every topic in a restore.

### PROD-03.0 — Flag schema-dependent topics

- **Issue:** A restore can succeed while applications cannot read the data because the registry holding its schemas was never captured, and nothing warns today.
- **Approach:** With the independent `.kbak` decoder, detect Confluent wire-format framing (magic byte and schema ID) per topic. Record "schema-dependent, registry not captured", with the IDs seen, in coverage, evidence and the catalog.
- **Acceptance:** Restore review and evidence name schema-dependent topics and the IDs they reference; no registry is contacted; old evidence reads as "not assessed".
- **Tests/evidence:** Avro-, JSON- and Protobuf-framed and unframed payloads in keys and values, nulls, and false-positive controls (raw data starting with byte 0).
- **Dependencies:** None. **Handoff:** detection contract and evidence field.

### PROD-03.1 — Capture a usable registry dependency set

- **Issue:** Archived record bytes can be unreadable when referenced schemas or versions disappear.
- **Approach:** Add a saved registry connection with server-side credentials. Capture subjects, versions, IDs, references, compatibility settings and deletion state exposed by the selected API. Link an immutable registry snapshot and observation window to each recovery point. Report missing permissions or unresolved references as incomplete coverage. A cheaper first slice to evaluate: restore the `_schemas` topic under a prefix with one partition and apply compaction after verification.
- **Acceptance:** Required schemas and transitive dependencies can be discovered without the source registry; the backup lists unsupported registry metadata. Topic-to-subject association is explicit and does not assume the default naming strategy.
- **Tests/evidence:** Multiple subjects sharing a schema, references, version changes during capture, deleted subjects, cyclic or invalid references, auth expiry and partial API failure. Verify archive integrity and old archives without registry metadata.
- **Dependencies:** PROD-03.0, 01.2, OD-2. **Handoff:** registry snapshot format, naming-strategy behaviour and completeness contract.

### PROD-03.2 — Preview and execute schema-aware recovery

- **Issue:** Destination schema IDs and subject names can differ, so restored applications fail even when record counts match. An ID rewrite changes value bytes, so today's byte fingerprints would report every rewritten record as a mismatch.
- **Approach:** Plan dependency-ordered selected schema import and key/value ID mapping before record replay. Preview conflicts and supported transformations, and reject by default when a required mapping is missing. Preserve nulls and unsupported raw payloads per an explicit mode. Verify against PROD-08.1's expected-output model and include the transformation identity in evidence.
- **Acceptance:** A real application deserializes restored Avro records against a target registry with different IDs. Unmapped required schemas block before data writes. Subject-subset recovery needs no archive deletion. Any privileged import mode requires separate collision review.
- **Tests/evidence:** Key and value schemas, null values, shared and referenced schemas, incompatible existing subjects, retry after partial import, wrong serialization declaration. Compare semantic values and prove unchanged fields stay intact.
- **Dependencies:** PROD-03.1, 08.1, OD-2. **Handoff:** conflict policy, mapping evidence, target changes and format limits.

## PROD-04 — Restore consumer positions and guide cutover

**Priority:** P1. **Owner areas:** Kafka metadata adapter, archive metadata, restore/cutover workflow. **Boundary:** explicitly selected groups; never blind replay into `__consumer_offsets`; applying positions is a separate, reviewed stage. **Risk/migration:** advancing offsets can skip work and rewinding can duplicate effects, so preserve prior target positions and require authorized confirmation. This is the main differentiator: Kannika leaves offsets to the separate kbridge tool, and OSO sells automatic offset reset as enterprise-only.

### PROD-04.0 — Decide the Kafka administrative path

- **Issue:** Group and ACL operations have no sanctioned path.
  - rdkafka 0.36.2's safe AdminClient has no group describe, offset list/alter or ACL calls. `Client::fetch_group_list` lists classic groups (state, protocol type, members) without type or generation.
  - librdkafka's C APIs need `unsafe` FFI, which `logweir-kafka` forbids, and librdkafka has no quota API.
  - The engine's `offset` subcommand is denied by ADR 0008 Amendment D, and ADR 0004 prefers existing client operations.
  - Kafka 4.x adds consumer, share and streams group types.
- **Approach:** For each operation — list and describe groups by type, fetch and commit offsets, describe and create ACLs — choose one of: the safe consumer API, an FFI crate with a scoped `unsafe` exception, an upstream rdkafka contribution, a raw-protocol client, or the engine behind an Amendment D change. Record the ADR 0004 and Amendment D amendments.
- **Acceptance:** A decision record per operation with evidence against the 4.x fixture; group-type handling defined (share and streams groups reported as not captured unless supported).
- **Tests/evidence:** Prototype calls against PROD-01.5's 4.x profile.
- **Dependencies:** None. **Handoff:** `decisions/PROD-04.0-admin-path.md` and ADR amendments.

**Completion record — Done (2026-09-29), PROD-04.0.**
- **Ownership:** worker `prod-04-0` (one run and one fix round), independent Tier-B review `claude/prod-04-0.review.md`. The review was ACCEPT-WITH-FIXES with 1 HIGH, 5 MEDIUM and 11 LOW; the HIGH re-check gave ACCEPT. Merged as `51aeefd2` (branch tip `00cf81fd`). The branch changes docs only; links, labels, `doc_lint` and `just lint` passed with rc 0.
- **Decision record:** [`decisions/PROD-04.0-admin-path.md`](decisions/PROD-04.0-admin-path.md). The route per operation:

  | Operation | Route |
  | --- | --- |
  | List groups by type | librdkafka ListConsumerGroups inside OD-6's perimeter, joined with a name listing |
  | Describe groups | DescribeConsumerGroups, for ids already classified |
  | Fetch offsets | the safe consumer API, with RequireStable |
  | Commit offsets | the safe consumer API, from a non-member; the broker refuses it while the group has members |
  | Describe ACLs | DescribeAcls, guarded by two positive probes that tell "no authorizer" from "denied" |
  | Create ACLs | no product route |
  | DescribeProducers, ListTransactions | unsupported; only a raw-protocol client reaches them |

  The engine route is recommended against for every operation, which feeds OD-3 and OD-6's option (c).
- **Group types:** classic and consumer groups are captured. Share, streams and other-protocol groups are `excluded: GroupTypeNotCaptured`. An id missing from every listing is `failed: NotVisibleToPrincipal` when a targeted lookup is refused, and `excluded: GroupNotFound` only when one answers. Absence never means offset 0.
- **Measured traps** (Kafka 4.3.1, and 3.9.2 where it differs):
  - rdkafka 0.36.2's safe `fetch_group_list` aborts on any member-less group (fixed in 0.37.0);
  - the classic describe reports every non-classic or absent group as `Dead`;
  - librdkafka's DescribeAcls reports "no authorizer" and "not authorised" alike as 0 bindings;
  - ListGroups silently shows a caller only the groups it may Describe;
  - an empty streams group accepts an outside commit.
- **Input to OD-6:** four operations need librdkafka calls the safe API lacks (the typed listing, describe, DescribeAcls, DescribeCluster). Measured cost: 117 shared lines, plus 30–45 lines per call. Fetch and commit need no `unsafe` under any option. §7 tabulates what each OD-6 option leaves PROD-04.1, 04.2 and 05.3.
- **Class sweep:** "denied reads as defaults" (T13) reaches five shipped consumers, including a restore readiness check that reports `ready` on a refused DescribeConfigs. They are handed to FX-4, which is in flight.
- **Proposed text** for the ADR 0004 amendment (perimeter-neutral) and Amendment D. Neither landed, per rule 8.
- **Upstream reports proposed (not filed):** librdkafka T9 and T3; rust-rdkafka PR #785's missing wrappers and T13. Filing them publicly needs the owner's OK.
- **Left for PROD-04.1's brief** (the reviewer's non-blocking LOWs): "no new third-party crate" wording at :135; §0's summary aligned with §5's missing-group rule; ten `examples/` targets that also lack `forbid(unsafe_code)`.
- **Artifacts:** `claude/artifacts/prod-04-0/`.

**Completion record — Done (2026-10-08), PROD-04.0a.**
- **Ownership:** worker `prod-04-0a` (a run and a fix round), independent Tier-A review `claude/prod-04-0a.review.md`: ACCEPT-WITH-FIXES (MEDIUM: no test bounded a fetch's time; LOWs), fixed and read by the orchestrator. Merged as `901dcde1` (branch tip `a02b4184`). Gate: `scripts/ci-check.sh` rc 0 at `a02b4184`.
- **Delivered:** `committed_positions(group, partitions, listing)` and `commit_positions(group, positions)` on `RdKafkaReader` (`crates/logweir-kafka`), through a private per-call handle that can never subscribe or join; RequireStable fetches; a commit from a non-member that the broker refuses while the group has members (`GroupActive`); every rdkafka code mapped as `decisions/PROD-04.0-admin-path.md` §4.3 says (`PositionsUnstable` per group, `NotVisibleOrUnreachable`, absence never offset 0, commits at leader epoch −1); bounded timeouts. No `unsafe`, no new crate; `forbid(unsafe_code)` intact. The third argument says whether the caller's listing shows the group; the review found a wrong value only swaps failure labels, never yields `Ok`.
- **Finding recorded in the decision record (§13):** the safe route CAN name a group hidden from the principal (`NotVisibleToPrincipal` / `NotAuthorized`); AP-04.1-6 now uses the reviewer's wording.
- **Evidence:** six e2e rows on compose (classic Empty, classic with a live member, consumer protocol, absent, unstable, invisible) pass at Kafka 4.3.1, 3.9.2 and 3.7.1 (the KIP-848 row only from 4.0); unit rows bound an unanswered fetch and commit in time. Mutants: 29 (first round) and 8 of 9 in the fix round; the survivor is a drain hardening that cannot be provoked. Not run live: a share-group refusal (it needs PROD-04.0d's fixture).
- **Artifacts:** `claude/artifacts/prod-04-0a/`.
- **Rows added:** PROD-04.0a–d. PROD-04.1 now also depends on 04.0a and 04.0d, and PROD-05.3 on 04.0b and 04.0d.

### PROD-04.1 — Archive consumer position evidence

- **Issue:** Logweir never restores groups. The engine's snapshot is off in Logweir, swallows per-group errors, drops groups without offsets and records no type, state or generation, and Logweir cannot parse its file yet (FX-1).
- **Approach:** Capture explicitly selected groups natively through the PROD-04.0 path, recording:
  - next-to-consume positions per partition and the observation time;
  - group type, state and generation (PROD-01.4), plus data coverage;
  - a per-group outcome: captured, excluded with a reason, or failed;
  - a digest bound into the receipt and catalog as versioned fields.

  State that positions observed while applications run may not be atomic with record capture, and keep a source-offline recovery path. Engine snapshots serve only as an import source for foreign archives.
- **Acceptance:** Every selected group is captured, excluded with a reason, or failed; absence never means offset zero. The catalog shows snapshot freshness and whether each position relates to archived data. Share and streams groups appear as not captured (or start offset only), never silently skipped.
- **Tests/evidence:** Active and empty groups, rebalances, missing Describe permission, offset beyond coverage, expired offsets, partitions added during capture, source loss after backup, Kafka 4.x group types.
- **Dependencies:** PROD-04.0, 01.4, FX-1. **Handoff:** versioned group snapshot and completeness/consistency classifications.

### PROD-04.2 — Translate positions and perform reviewed cutover

- **Issue:** Replayed records receive new offsets; using original committed positions directly is unsafe.
- **Approach:** Map each committed position to the first target record whose `x-original-offset` is at or above it; restored records keep these headers. Report exact, approximate (partition end proven within coverage) and unavailable, and block unknown mappings. The engine's offset report is only a cross-check. Check that target groups are inactive, save their original positions and apply through the reserved `Switchover` kind with its own approval subject, after data verification (PROD-08.1). The commit path follows PROD-04.0.
- **Acceptance:** A paused fixture consumer resumes at the expected record after restore. A live group cannot be reset silently. Partial application is recoverable and visible. Restoring data never triggers a consumer reset automatically.
- **Tests/evidence:** Compaction gaps, offset at partition end, empty target partition, duplicate provenance, recreated topics, concurrent group activation, failure halfway through applying groups. Verify rollback limits if consumers have already restarted.
- **Dependencies:** PROD-04.1, 08.1; PROD-07.3 where resume is supported. **Handoff:** mapping report, applied-position audit and application cutover instructions.

## PROD-05 — Recover configuration and access metadata

**Priority:** P1 (05.1, 05.2), P2 (05.3). **Owner areas:** Kafka administrative adapters, recovery planner, restore evidence. **Boundary:** topic configuration first. ACLs are exported and diffed, not applied. Quotas are deferred because librdkafka has no quota API. Never export credentials or recreate broker infrastructure. **Risk/migration:** provider-managed settings differ; restored retention can remove old data immediately; declarative owners (Strimzi `KafkaTopic`/`KafkaUser`, Terraform) revert changes made around them.

### PROD-05.1 — Capture topic configuration with coverage and portability

- **Issue:** Manifests already carry recovery-relevant topic overrides, source replication factor and partition count, but restores apply only the partition count. Capture is non-fatal, so a denied DescribeConfigs looks like "no overrides" (FX-4). The engine's allowlist still names keys removed in Kafka 4.0, such as `message.format.version`.
- **Approach:** Project captured configuration, source RF and partition counts into the receipt, catalog and API, with per-topic coverage (captured, not captured, capture denied). Build a version-aware portability table validated with `CreateTopics` `validate_only`, separating explicit overrides, inherited defaults, provider-only settings and secrets. Detect declarative owners (Strimzi labels or `KafkaTopic` resources, or externally declared) and mark those topics for desired-state export.
- **Acceptance:** A recovery point lists configuration coverage and a portable desired-state model. Unsupported settings never masquerade as defaults. Credentials are absent from manifests and logs.
- **Tests/evidence:** Compacted and delete-policy topics, min-in-sync settings, denied DescribeConfigs, Kafka 3.9 vs 4.x keys, a Strimzi-managed topic.
- **Dependencies:** FX-4, PROD-01.5. **Handoff:** versioned configuration model and portability table.

### PROD-05.2 — Apply a reviewed target topic configuration

- **Issue:** New-topic restores create topics with the plan's replication factor (the console always sends 1), infinite retention and the broker's cleanup policy. Blindly copying source settings can fail on a smaller target.
- **Approach:**
  1. Preview target differences.
  2. Create topics with the source partition count, replication bounded by the target's broker count, and safe recovery settings.
  3. Restore and verify.
  4. Offer the explicit post-verification transition for compaction and retention. Compacted targets fail verification if compaction starts earlier.

  Fail closed on unsupported settings. For owner-managed topics, emit reviewed `KafkaTopic` YAML (or Terraform) instead of applying.
- **Acceptance:** Recovery preserves the chosen compaction semantics and partition layout, explains replication changes, leaves unrelated policies untouched and shows before/after state, including partial application.
- **Tests/evidence:** Fewer target brokers, incompatible configs, old timestamps, retry after partial application, a readable topic before and after the transition, an owner-managed topic.
- **Dependencies:** PROD-05.1, 08.1. **Handoff:** target-diff report, rollback limits and remaining manual steps.

### PROD-05.3 — Export access policy for review

- **Issue:** No ACLs are captured. Applying them blindly can grant inappropriate access, and Strimzi's User Operator deletes ACLs that do not match `KafkaUser` resources.
- **Approach:** Capture literal and prefixed ACLs with provenance through the PROD-04.0 path. Produce a target diff with principal and name mapping, and emit `KafkaUser` YAML for operator-managed principals. This task applies nothing.
- **Acceptance:** A recovery point lists ACL coverage; the diff is reviewable and exportable; unresolved principals are explicit.
- **Tests/evidence:** Mixed literal and prefixed ACLs, wildcard principals, denied DescribeAcls, an operator-managed user.
- **Dependencies:** PROD-04.0, 05.1. **Handoff:** ACL model. An apply task is added only if demand appears.

## PROD-06 — Application recovery profiles

**Priority:** P3. **Owner areas:** recovery planner, application fixtures, catalog. **Boundary:** first one documented, non-EOS Kafka Streams profile (EOS waits for a PROD-00.3 transaction capability); Kafka Connect, Flink and external systems remain explicit follow-ups. **Risk/migration:** internal topic names alone cannot establish application consistency; Kafka 4.2 adds the streams group type.

### PROD-06.1 — Define a recoverable application dependency profile

- **Issue:** Selecting all user topics can omit state a stateful application needs. Kannika's guidance distinguishes changelog state from rebuildable repartition topics and ties recovery to source data and group positions. [Kafka Streams guide](https://docs.kannika.io/faq/kafka-streams/)
- **Approach:** Model application identity and version, source topics, selected changelogs, schemas, consumer or streams groups and declared external dependencies. Offer discovery suggestions that require review. Specify the quiescence or consistency boundary a supported recovery needs.
- **Acceptance:** The profile reports which dependencies are protected and why others are excluded. It blocks an application-recovery claim when required source history, group positions or state are missing.
- **Tests/evidence:** A local stateful aggregation (PROD-01.5 Streams profile), compacted changelog, renamed application ID, topology evolution, an intentionally missing dependency; behaviour under capture while the application runs.
- **Dependencies:** PROD-01.1, 04.2, 05.2. **Handoff:** one supported profile and explicit unsupported scenarios.

### PROD-06.2 — Prove application restart after recovery

- **Issue:** A completed data replay does not establish that a stateful application can resume correctly.
- **Approach:** Orchestrate the approved profile into an isolated target, run a version-pinned application fixture and compare expected state and subsequent outputs. Expose "data restored" and "application validated" as distinct outcomes. Never execute arbitrary user scripts with controller credentials.
- **Acceptance:** The fixture resumes and processes new inputs with the expected state. Missing prerequisites produce a failure naming the missing dependency, and no success badge. A profile change requires a new immutable plan.
- **Tests/evidence:** Full source loss, consumer restart, stale local state, schema mismatch, changelog compaction and interrupted verification; business-level aggregates as well as records.
- **Dependencies:** PROD-06.1. **Handoff:** reproducible recovery exercise, residual external dependencies and a recovery-time measurement.

## PROD-07 — Durable restore interruption and resume

**Priority:** P2. **Owner areas:** engine adapter, checkpoint storage, operator execution. **Boundary:** same immutable plan and target generation; changing a plan starts a new operation. **Risk/migration:** acknowledgements and checkpoint writes are not atomic; automatic replay could duplicate data.

### PROD-07.1 — Resolve checkpoint and delivery semantics

- **Issue:** Restore checkpoints are pod-local and a crash is not resumable. The engine's checkpoint has five problems:
  - it is saved only after each topic, and `restore.checkpoint_interval_secs` is unused;
  - it hashes the whole rendered options, including Logweir's run-specific checkpoint and offset-report paths, and restarts from the beginning on a mismatch;
  - the engine produces without idempotence and retries on connection errors;
  - it checks for shutdown only between topics;
  - skipped segments add nothing to the offset report.
- **Approach:** Record the decision from source first, time-boxed. The default contract is "resume means reconcile or use a fresh target", unless PROD-00.3 funds per-segment checkpoints, path-free hashing and a persisted mapping. Define the replay ambiguity window, the producer acknowledgement boundary and behaviour after target mutation.
- **Acceptance:** The decision covers process kill, lost node or storage and competing workers. Checkpoints bind plan, execution, archive generation and target identity; old ephemeral checkpoints are never treated as durable.
- **Tests/evidence:** Termination before and after acknowledgements and checkpoint commits; stale checkpoints; two workers; target mutation; measured duplicate and loss behaviour.
- **Dependencies:** PROD-01.1. **Handoff:** resume decision, checkpoint contract and failure-state table.

### PROD-07.2 — Make interruption honest

- **Issue:** An interrupted restore reads as a generic failure and its partial targets are not listed. Retention protects only non-terminal Restores, so the point a retry needs can be released.
- **Approach:** A distinct interrupted state with a signed list of partial targets. Retry through PLAT-12.2's fresh target and new approval. Hold the point until the interruption is resolved. Clean up only targets the same execution created. Propagate cancellation to the engine subprocess (`docs/stability.md` Later #13).
- **Acceptance:** Restarting the controller or browser cannot duplicate an operation. The operator sees the last durable progress. Cleanup never deletes an unrelated target or the only recovery path.
- **Tests/evidence:** Kill the runner, restart the controller, disconnect the browser, repeat commands, expire credentials, race cancel with completion.
- **Dependencies:** PROD-07.1. **Handoff:** user state model and retention/cleanup policy.

### PROD-07.3 — Resume within proven semantics

- **Issue:** Long restores need to continue after a crash without duplicating or losing records.
- **Approach:**
  - Resume each partition from the target tail's `x-original-offset`.
  - Keep rendered options byte-identical across attempts.
  - Rebuild the offset mapping from headers and prove it covers every restored record.
  - Label up to one segment of duplicates per partition as bounded.
  - Scope an exception to phase 0's existing-target refusal to targets the same execution created.
- **Acceptance:** Resume continues within proven semantics or explains why it is blocked; output integrity and target identity are verified after every restart.
- **Tests/evidence:** Kill before and after acknowledgements and checkpoint commits, stale checkpoints, two workers, target mutation.
- **Dependencies:** PROD-07.2, 00.3 per OD-3. **Handoff:** resume evidence and limits.

## PROD-08 — Verification that answers recovery questions

**Priority:** P1 (08.1, 08.4), P2 (08.2, 08.3). **Owner areas:** verifier, drill execution, evidence UI, documentation. **Boundary:** extend existing evidence; do not add another mandatory approval ritual. **Risk/migration:** full verification is expensive and must never be silently replaced by sampling.

### PROD-08.1 — Complete archive integrity and exact counts

- **Issue:** Today's verification has five gaps:
  - Only in-window segments of sampled partitions are hashed, and `max_partitions` keeps the first N.
  - The count check is an aggregate bound that includes whole straddling segments.
  - Duplicates collapse in an offset-keyed map, and order is never checked.
  - The archive side selects segments by first and last record timestamps, as the engine does, so it is not independent of the engine's selection.
  - Gaps are free text.
- **Approach:**
  - Hash every in-window segment of every restored partition.
  - Compute exact per-partition expected counts by decoding records and filtering on each record's own timestamp.
  - Check duplicates and order on `x-original-offset`.
  - Add structured, signed gap and pruned-range fields and an additive `coverage: sampled | complete` field, in both verifiers and the parity script.
  - Define the expected-output model for filters, partition subsets, compaction and transformations that PROD-03.2, 04.2 and 11.x consume.
- **Acceptance:** Evidence separates authenticated report, archive integrity, replay comparison and application validation; old scorecards verify unchanged; complete mode covers every selected record or reports incomplete.
- **Tests/evidence:** Corrupt an unsampled segment, omit a segment, duplicate output, reorder records, non-monotonic timestamps, compaction holes; each fault is caught or disclosed at the right level.
- **Dependencies:** PROD-01.1. **Handoff:** verification contract, cost measurements, backward-compatible report fields.

### PROD-08.2 — Measure recovery objectives through exercises

- **Issue:** PLAT-14.3 already schedules rehearsals with selection, isolated targets, bounds and `rtoSeconds`/`passRate` objectives, and scorecards already record per-phase durations. RPO objectives, trends and application assertions are missing.
- **Approach:** Add `rpoSeconds`, surface per-stage durations, keep trend history per schedule and add application assertions once PROD-06.2 exists.
- **Acceptance:** A failed exercise makes protection risk visible without marking the archive unusable; results identify tested scope and environment; local timings are never advertised as production SLAs.
- **Tests/evidence:** Corruption, source loss, unavailable registry, restricted target, timeout, expired credentials, cleanup failure, a known failing assertion, alert deduplication.
- **Dependencies:** PROD-08.1; PLAT-14.3 Done; PROD-06.2 for application assertions. **Handoff:** repeatable exercise and measured RPO/RTO definitions.

### PROD-08.3 — Full streamed replay comparison

- **Issue:** Critical recoveries need every selected record compared, not a sample.
- **Approach:** Stream a full comparison of every selected supported record with bounded memory. Record counts, bytes, exclusions and verifier version. An interrupted run leaves verification incomplete.
- **Acceptance:** Full mode compares every selected supported record under the PROD-08.1 contract or reports incomplete; its cost is measured and shown.
- **Tests/evidence:** Large fixture, interruption, memory ceiling, altered header, schema mapping once PROD-03.2 exists.
- **Dependencies:** PROD-08.1. **Handoff:** cost model and report fields.

### PROD-08.4 — Publish a control-evidence mapping

- **Issue:** Auditors and GRC teams ask what a restore test proves. Regulations require tested restores with recorded results and competitors market compliance, but Logweir publishes no mapping while it rightly refuses compliance claims.
- **Approach:** Add a page beside `docs/verify-a-scorecard.md`. It maps scorecard, receipt and rehearsal fields to DORA Art. 11–12 and RTS Art. 25, ISO/IEC 27001 A.8.13, SOC 2 A1.3, NIS2 implementing regulation 2024/2690 §4.2 and HIPAA 164.308(a)(7)(ii)(D). For each clause it states what the evidence supports and what it does not (sampling, configuration coverage, key custody, storage immutability, transaction semantics).
- **Acceptance:** Every supported statement cites a field, every gap is stated, no sentence claims compliance, and the verification guide links the page.
- **Tests/evidence:** doc_lint and link checks; the orchestrator checks each cited field exists.
- **Dependencies:** None. **Handoff:** the page, refreshed as fields change.

## PROD-09 — Independent protected archives and storage choice

**Priority:** P2. **Owner areas:** storage adapters, catalog, archive policy. **Boundary:** strengthen the S3 path first. GCS, Azure and filesystem code paths exist through legacy URLs but are untested; real-store rows need OD-4. **Risk/migration:** signatures do not stop deletion; encryption without surviving keys makes a backup unrecoverable; immutability without an erasure answer is a liability for EU buyers.

### PROD-09.1 — Make archive protection observable

- **Issue:**
  - Retention `Enforce` now refuses versioned buckets (`VersionedBucket`) and deletes nothing, and every Object Lock bucket is versioned, so lock-protected archives need `ExternalLifecycle`.
  - object_store 0.14 exposes no lock mode, retain-until, legal hold, SSE key or delete-by-version; `object_lock_readback` returns None.
  - Destinations default `archiveRead` and `evidenceWrite` to the write credential.
  - The engine overwrites its manifest unconditionally. [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- **Approach:**
  1. Decide the S3 control-plane client: an amendment to the fixed-dependency constraints, an upstream object_store contribution, or a minimal SigV4 client.
  2. Report versioning, lock mode, retain-until, holds and SSE key references for the exact archived versions.
  3. Score the grants actually in use and which prefixes can be overwritten.
  4. Distinguish configured from observed protection.

  Never enable irreversible retention as an incidental install step.
- **Acceptance:** The product distinguishes configured from observed protection. A delete marker cannot hide a known recoverable locked version from catalog import. Recovery identifies missing decryption keys before engine work. Support bundles never export private keys.
- **Tests/evidence:** Lock-capable store from PROD-01.5, delete markers, retained versions, encrypted-object access failure, key rotation, provider guarantees recorded as untested locally.
- **Dependencies:** PROD-09.3, FX-7, PROD-01.5. **Handoff:** protection report, required permissions and provider validation gaps.

### PROD-09.2 — Add a verified secondary copy

- **Issue:** One bucket, account or region can remain a shared failure domain, and copying some objects does not produce a complete recovery point.
- **Approach:** The first slice declares a secondary destination, imports it through PLAT-15.2 and verifies every segment digest before the copy becomes available. Copies preserve object keys because receipts bind `manifest_key` and the prefix. Provider replication needs versioning on both sides, which conflicts with `Enforce` retention; record the supported combination. Then add a Logweir copy of a committed point with an adapter contract for versioned reads, conditional publication, integrity and credential renewal.
- **Acceptance:** The secondary location becomes available only after all required data, manifests and public verification metadata validate. A failed copy is resumable or restartable without corrupting the source. Retention understands references to each copy. In a view that merges two buckets, a location whose `Available` rests on an unchecked pin never outvotes a sibling's superseded `Conflict`, and the merge keeps every location's remedy (FX-7's follow-up; `crates/weirkeeper/src/catalog_view.rs` `merge_entries`).
- **Tests/evidence:** Interrupted copy, corrupted object, missing schema snapshot, revoked credentials, primary unavailable, abandoned-upload cleanup, restore using only the secondary copy; real stores per OD-4.
- **Dependencies:** PROD-09.1; OD-4 for provider rows. **Handoff:** adapter contract, complete-copy marker semantics, backend follow-ups.

### PROD-09.3 — Decide archive data protection

- **Issue:** Nothing addresses encryption at rest or erasure:
  - Neither the engine nor Logweir configures SSE-KMS or client-side encryption.
  - Immutable archives, copies, continuous capture and clones extend the life of personal data.
  - Restores create targets with infinite retention.
  - A point-in-time restore can bring back data the source has erased.

  The EDPB expects erasure honoured on restored systems, and OSO sells encryption, GDPR erasure and crypto-shredding as enterprise-only.
- **Approach:** Decide:
  - SSE-KMS configuration on destinations;
  - an erasure ledger of signed, subject-keyed Drop/Tombstone rules applied at every restore and clone and recorded in the scorecard (needs a filter route from PROD-00.1);
  - per-key or per-topic envelope encryption for crypto-shredding;
  - retention defaults for restored and cloned targets;
  - a statement of what is and is not erased from immutable archives.
- **Acceptance:** A decision record with the chosen mechanisms, their limits and child rows.
- **Tests/evidence:** A decision matrix with cheap prototypes where they settle a question.
- **Dependencies:** PROD-00.1. **Handoff:** `decisions/PROD-09.3-data-protection.md`.

## PROD-10 — Predictable performance and operating cost

**Priority:** P2 (10.1), P3 (10.2). **Owner areas:** engine performance, metrics, capacity UI. **Boundary:** measured controls and estimates, not unproven multi-terabyte claims. **Risk/migration:** aggressive concurrency can harm source traffic or exhaust target and storage quotas.

### PROD-10.1 — Benchmark and expose bounded controls

- **Issue:** No throughput or bottleneck evidence exists.
  - Backups run the demo defaults (`BackupSettings::default()`: 1,000-record segments, three concurrent partitions).
  - The only engine throttle is a per-partition restore records/sec limit; `rate_limit_bytes_per_sec` is parsed but not enforced.
  - No metrics endpoint exists (`docs/metrics.md`), and PLAT-20.2 measures control-plane load, not engine throughput.
- **Approach:** Build a reproducible benchmark on native amd64 hardware, labelling emulated runs, across record sizes, partition skew, compression and storage latency. Measure bytes read and stored per run, manifest bytes, object counts, readback memory and restore throughput. Replace the demo defaults with measured ones. Expose the engine's existing knobs with CEL bounds. Add versioned receipt fields for compressed and uncompressed bytes, segment count and per-partition offsets.
- **Acceptance:** Published fixture size, hardware, broker settings, throughput and resource use. Controls have documented bounds, preserve correctness and expose backpressure. Scaling conclusions name where larger or provider tests remain necessary.
- **Tests/evidence:** Uneven partitions, large records, low throughput, throttled storage, CPU and memory pressure, competing jobs; correctness and source impact compared before and after tuning.
- **Dependencies:** PROD-01.5, 02.2, FX-2. **Handoff:** benchmark baseline, safe defaults and bottleneck-specific follow-ups.

### PROD-10.2 — Show cost and recovery-time estimates with uncertainty

- **Issue:** Retention and recovery choices create storage, request and transfer costs that users cannot estimate today.
- **Approach:** Estimate retained bytes, compression, object requests and transfer from observed usage and user-supplied rates. Include request and egress costs for captures from tiered-storage or diskless clusters. Estimate recovery duration from comparable measured jobs, showing the range, sample age and missing inputs. Never present list prices as universal rates.
- **Acceptance:** Estimates are reproducible from visible assumptions, distinguish logical from billed bytes and show unknown when evidence is insufficient. Rate changes update the estimate without changing retention or execution policy.
- **Tests/evidence:** Empty installations, changing compression, unknown rates, partial metrics, currency and unit changes, unusually slow archives.
- **Dependencies:** PROD-10.1. **Handoff:** estimation model and accuracy limits.

## PROD-11 — Safe cloning and granular replay

**Priority:** P1 (11.1), P3 (11.2). **Owner areas:** recovery selection, guards, verification, UI. **Boundary:** new isolated target topics; no in-place editing of original archives. **Risk/migration:** production clones can expose sensitive fields; transformations change verification expectations.

### PROD-11.1 — Add replay selection and safe clones

- **Issue:** Developers and incident responders need a bounded window or a partition subset. The engine accepts a time-window start and source partitions, but four things block it:
  - Guard G-WIN pins every restore window's start to the archive floor: plan building in `crates/logweir/src/drill/mod.rs`, phase 5's floor check, and `spec.rs`, which says no window start exists.
  - The reserved `InheritedFromSpec` source is unused.
  - Phases 4 and 7 do not know about partition subsets.
  - The engine's `source_partitions` applies to every topic in a run.

  New-topic clones also keep infinite retention and are never torn down.
- **Approach:** Add an optional inclusive start instant and per-topic partition subsets, using one engine run per distinct subset if needed. Do this through a recorded G-WIN amendment that uses `InheritedFromSpec`. Make phases 4 and 7 subset-aware, state the sub-window in the evidence and reuse PLAT-11.2's target-name preview. Clones get a TTL and cleanup policy, an `AllowedClusters` check and explicit header handling. Offset ranges wait for PROD-00.3.
- **Acceptance:** The basic flow stays short and advanced controls appear only when selected. Preview and execution select the same records. Missing coverage is never silently widened. Clones expire as declared.
- **Tests/evidence:** Inclusive and exclusive boundaries, equal and non-monotonic timestamps, empty selections, compaction holes, topic subsets, new points arriving during editing, name collisions, consumer-position mapping over a filtered restore (with PROD-04.2).
- **Dependencies:** PROD-01.1, 08.1. **Handoff:** filter contract, coverage preview and truthful evidence for subsets.

### PROD-11.2 — Introduce constrained masking for test environments

- **Issue:** Production clones may carry sensitive values. The engine's per-record hook can only keep, drop or tombstone and is settable only by embedding code, which ADR 0001 forbids. OSO lists masking as a planned enterprise feature.
- **Approach:** Start with a small reviewed set of schema-aware field redaction and tokenization operations, deterministic where key relationships require it. Take the route from PROD-00.1: a transform action in a fork, or a Logweir-native producer. Never restore to staging and transform afterwards. Exclude key transformation from the first slice. Version policies immutably per execution and reject unsupported formats. Require PLAT-19.2 governed approval for production-to-non-production copies.
- **Acceptance:** A clone preserves declared schema compatibility and required join keys without revealing protected values. Evidence describes transformed fields and compares against transformed expectations.
- **Tests/evidence:** Nested fields, keys and values, nulls, schema evolution, malformed payloads, deterministic references, attempted secret leakage, resource exhaustion; scan outputs and logs for protected fixture values. Network-deny acceptance needs an enforcing CNI; docker-desktop does not enforce NetworkPolicy, so record it as untested locally.
- **Dependencies:** PROD-11.1, 03.2, 00.3 per OD-3. **Handoff:** supported transformation catalog, security boundary and irreversibility limits.

## PROD-12 — Cutover verification

**Priority:** P3. **Owner areas:** verification, restore/cutover workflow. **Boundary:** verify and record a cutover someone else performed. There is no data-moving orchestration: free vendor tools (MSK Replicator, Cluster Linking and KCP, Redpanda Shadowing) do that with offsets. Product surfaces avoid the word "migration" (Never #3). **Risk:** dual writers, stale offsets and external effects.

### PROD-12.1 — Verify a replicated target against the archive

- **Issue:** Teams moving clusters with replicators lack independent proof that the target holds what the source held and that consumers resume correctly.
- **Approach:** Compare target topics written by any replicator against Logweir's archive: counts, fingerprints, and offset alignment where the replicator preserves offsets. Map group positions through PROD-04.2 where offsets changed. Record who confirmed external steps, and emit signed evidence.
- **Acceptance:** A local rehearsal detects a missing partition, an offset shift and an unpaused producer; evidence names what was and was not checked.
- **Tests/evidence:** MirrorMaker 2 between the two PROD-01.5 clusters, unpaused producers, changing topic counts.
- **Dependencies:** PROD-04.2, 08.1, OD-2. **Handoff:** verification report and operator guidance.

### PROD-12.2 — Orchestrated cutover

Deferred. Revisit only with demand evidence that replicators do not serve.

## PROD-13 — Optional fleet and hosted control plane

**Priority:** P3, deferred. Namespace isolation is PLAT-17.2's acceptance, Never #4 refuses fleet views and no adopter has asked. Revive only on a named adopter's request. Then start with a read-only evidence aggregator: it verifies signed points from several installations' buckets with the existing verifiers and holds no command path, so each installation executes only locally verified approvals. The earlier design questions still apply at that point: trust boundaries, enrollment, revocation, command replay and offline behaviour. A two-environment pilot needs release-scoped RBAC first, because the chart supports one full release per cluster.

## PROD-14 — Distribution, integrations and adoption

**Priority:** P1 (14.0, 14.1), P2 (14.2); 14.3 is deferred to [Business](#business-out-of-platform-scope) (2026-10-07). **Owner areas:** release pipeline, installation, public API/CLI, documentation, packaging. **Boundary:** consolidate existing guides and packaging, not a parallel documentation tree or a new mandatory deployment system. **Risk/migration:** hidden prerequisites and incompatible version combinations can make a working archive inaccessible during an incident.

### PROD-14.0 — Ship a working release

- **Issue:** Every release run for v0.1.1–v0.1.5 failed at the CLI build matrix, so no GitHub Release exists. `README.md` still says version tags publish versioned images and CLI archives. The chart installs from a checkout with `latest` images. No task in either tracker owns this.
- **Approach:** Repair `release.yml`. Publish a GitHub Release with CLI archives, the independent Python verifier, third-party notices and the engine licence. Publish the chart as an OCI artifact with digest-pinned images. Align with the platform run's published images (`docker.io/vladyslavhaina/*`, `sha-<commit>` tags) and the owner's countersigning step. Correct the README.
- **Acceptance:** A tag produces a green release run and a GitHub Release whose artifacts verify; the chart installs from the registry with pinned digests.
- **Tests/evidence:** A release-candidate tag, artifact verification, an install from the published chart on docker-desktop.
- **Dependencies:** None; coordinate with the platform run's final install. **Handoff:** release procedure and artifact list.
- **Owner decisions, 2026-10-07:**
  - **The tag.** The orchestrator pushes `v0.2.0-rc.1` without asking again once five things are green: `wt/integrate-3` (FX-7 + PROD-08.4) is merged; this branch is merged with main CI green, publish included; one dry run of `release.yml` is verified; the PoC is up and re-seeded; FX-13 is merged. In-flight FX rows ship in rc.2.
  - **The API document.** It is not frozen. rc.1 bumps it to `1.0.0-alpha.2` with a list of the changes since alpha.1, and `docs/stability.md` says the pre-release version moves once per release candidate. The freeze belongs to PROD-14.2.
  - **The leftover v0.1.x Docker Hub tags** (26 tags in `logweir`, `weirkeeper` and `logweir-ui`) stay until v0.2.0 final ships. Then the owner deletes them all in one step with their own login, and R1 (the upgrade rehearsal from v0.1.5) is retired. Nothing uses them: not the chart, the CI, the PoC or any signed evidence.

### PROD-14.1 — Validate a simple install and emergency recovery kit

- **Issue:** Kafka users include small teams, restricted networks and installations whose original control plane has been lost. Three things make installation harder today:
  - the runner is amd64-only;
  - the managed signing identity needs Helm `lookup`, which offline rendering and Argo CD do not support;
  - approvals need a manual approver-key step.
- **Approach:** Write one canonical install and recovery guide and measure the time to the first verified restore. Provide arm64 through PROD-00.2, or state the limitation. Add a lookup-free identity path and a compose sandbox that needs no Rust toolchain. Build the versioned offline recovery kit (compatible binaries and images, manifest schemas, public verification material, instructions for separately managed credentials) and validate it through a disconnected exercise.
- **Acceptance:**
  - A fresh operator reaches a verified sample backup and restore without a manually generated signing key, using ordinary confirmation (PLAT-19.2) where the namespace is not governed.
  - Another clean installation imports the archive and recovers with the kit, which bundles no private keys or passwords.
  - Version and architecture constraints show before installation.
  - The time to the first verified restore is recorded.
- **Tests/evidence:** Clean docker-desktop install, upgrade and rollback, private CA, no public network once artifacts are prepared, lost original CRs, unsupported CPU/image combinations; the timed journey and remaining manual steps.
- **North-star measurement (moved from PROD-14.3, 2026-10-07):** measure with the draft definition ns-def 0.1 (`north-star.md` §1–§9, kept with the 14.3 drafts). The clock stops at the first qualifying Restore's `status.evidence.verification.verifiedAt`, with the independent re-check's lag recorded beside it. Where to measure is decided when 14.1 is dispatched; re-run the draft's citation check at that commit.
- **Dependencies:** PROD-14.0, 01.2, 16.1 (no approver key by default); PLAT-15.2, 19.2, 20.2. **Handoff:** canonical guide, compatibility manifest and measured usability.

### PROD-14.2 — Offer stable automation and support interfaces

- **Issue:** A platform needs repeatable integration with GitOps, incident tooling and support without duplicating logic in scripts. The OpenAPI document is still `1.0.0-alpha.1`. Idempotency keys (PLAT-17.1) and notification delivery (PLAT-14.2) exist, but notification POSTs are unsigned.
- **Approach:** Freeze OpenAPI 1.0.0 with a deprecation policy. On the existing notify path, sign webhook payloads, bound retries and make events deduplicable. Add a redacted support bundle with an explicit preview. Align the CLI and declarative resources, and keep one documentation source per workflow.
- **Acceptance:** An automation client creates one operation, follows it across reconnects and handles documented errors. Webhooks are authenticated, retried within bounds and deduplicable. Support bundles omit credential values and record payloads by default. Emergency recovery never depends on telemetry or an external licence service.
- **Tests/evidence:** Old client against new server, duplicate requests and events, an unavailable receiver, expired tokens, an unauthorized namespace, seeded secrets in diagnostic input.
- **Freeze precondition (2026-10-07):** freeze OpenAPI 1.0.0 only after PROD-16.1–16.2 and the rows that change the API (15.1, 05.1, 05.2, 02.1, 04.2, 11.1, 08.1) have merged, and write the deprecation policy then. Until the freeze the document stays a pre-release that moves once per release candidate.
- **Dependencies:** PLAT-17.2 Done. **Handoff:** public compatibility policy, examples and redaction evidence.

### PROD-14.3 — Positioning and design-partner kit

- **Issue:** The roadmap named no persona, buyer or design partner, and Logweir is absent from market roundups. Public positioning waits for trademark clearance (`TRADEMARKS.md`).
- **Approach:** Draft a positioning page (evidence you can re-check; a complement to replication), a persona and segment note, and an interview guide for three to five regulated self-hosted teams (MSK, Confluent Cloud and Strimzi users). Define the north-star metric precisely. Keep everything internal until OD-5.
- **Deferred to Business (B-5), 2026-10-07.** The drafts (positioning, personas and segments, interview guide, north-star definition and its citation check) were written and kept privately in `$HOME/.logweir-roadmap-run/claude/artifacts/prod-14-3/`, never committed. Their open questions (the open-core commitments, a working name under NDA, the measurement host, the publication approver) wait for the owner's business track. The north-star definition moved to PROD-14.1.

## PROD-15 — Restore under the original topic name

**Priority:** P1. **Owner areas:** guard, product API, restore wizard, runner phase 0. **Boundary:** only into a topic that does not exist; Never #1 (restore into a live topic) is unchanged. **Risk:** automatic creation or a running producer can create or write the name during the restore, and an operator-managed `KafkaTopic` can recreate it.

### PROD-15.1 — Restore under the original name into an absent topic

- **Issue:** Identity mappings are refused in every mode, even into another cluster where the name does not exist:
  - the guard refuses them ("the target must differ from the source");
  - the API refuses them (`mapping_identity`, and a non-empty prefix is required);
  - so does the wizard.

  Phase 0 already refuses any existing target. The platform tracker left "advanced in-place recovery" to this file and no task took it up, so every deleted-topic or cluster-loss recovery forces applications onto prefixed names.
- **Approach:** Allow an identity mapping only under these conditions:
  - the target cluster differs from the source, or the name is verified absent and broker auto-creation is disabled (refuse otherwise);
  - creation is exclusive: `CreateTopics` fails if the topic exists;
  - the restore carries a separate approval subject;
  - a declarative owner (Strimzi `KafkaTopic`, GitOps) of the name blocks the restore unless the owner path is chosen.

  Keep the identity ban in scratch mode and for any existing target, and keep the probe topic and teardown rails away from original names. Record the ADR and the `docs/stability.md` update.
- **Acceptance:** A deleted topic is recovered under its original name both on a second cluster and on the same cluster with auto-creation disabled. Every unsafe condition refuses before data moves. Evidence records the approval subject.
- **Tests/evidence:** Auto-create race, a producer writing before cutover, an existing `KafkaTopic` owner, same versus different cluster, approval binding; phase 7 unaffected.
- **Dependencies:** PROD-01.4, OD-2. **Handoff:** conditions contract and operator guidance.

## PROD-16 — Approvals without a personal key

**Priority:** P1 (16.1, 16.2), P2 (16.3, 16.4). **Owner decision:** OD-8 (2026-10-07). **Owner areas:** approval policy (`crates/logweir-core/src/approval_policy.rs`), product API approval routes (`crates/logweir-api`), the controller's approval and restore admission (`crates/weirkeeper`), the runner's phase 1 (`crates/logweir/src/drill/phase1_approval.rs`), the approvals page (`ui/pages/approvals.js`), the chart and the identity bootstrap hook, and docs (D0, SECURITY.md, `docs/kubernetes.md` §8, quickstart, install). **Boundary:** additive. Existing Approval resources, v1/v2 documents, signed archives and both verifiers keep verifying; the personal-key path stays as the strict mode. **Risk:** in console modes, whoever controls the console pod, its key Secret or the identity provider can approve alone. That is stated in SECURITY.md, and the strict mode stays for namespaces that cannot accept it.

**Today (research of 2026-10-07):** a fresh install resolves every namespace to `legacy-governed-v1` (`charts/logweir/values.yaml:171-176`; `approval_policy.rs:327-333`), so every restore needs a document signed with a personal key: the approver copies two files out of the console (`ui/pages/approvals.js:1160-1163`), runs `logweir drill approve` or `drill countersign` with `--key` (`crates/logweir/src/approve.rs:145-228`, `:467-603`) and pastes the sidecar back. Ordinary confirmation needs `allowOrdinaryConfirmation`, an explicit binding and a hand-made console key Secret, and is refused in localAdmin (`crates/logweir-api/src/routes/restores.rs:578-589`). Standing rehearsal authorizations accept only `GovernedApproval` keys (`crates/weirkeeper/src/controllers/restore.rs:1159-1170`).

### PROD-16.1 — No key by default

- **Issue:** A first restore needs a personal signing key and a copy, sign and paste ritual, even for one operator on a laptop.
- **Approach:** Make one-person console confirmation the default for a fresh install and for every namespace with no policy, including localAdmin, where the confirming principal is the local admin. Generate the console confirmation key in the existing identity bootstrap hook (create-once, retained Secret, public half published) and render its TrustPolicy entry with usage `ConsoleConfirmation`, so no operator runs `openssl` or edits a TrustPolicy. An upgrade never weakens approval on its own (D0's rule): an existing install whose namespaces resolve to `legacy-governed-v1` keeps it until an administrator opts in, so only a fresh install starts in confirm. The worker designs how the chart tells the two apart (for example a create-once marker written at first install) and documents it. Collapse what the operator sees to three named modes: confirm (default), two-person (16.2) and strict (personal key).
- **Acceptance:** a fresh `helm install` on docker-desktop reaches a verified restore with one person and no key handled; the console key is generated exactly once and survives upgrade and reinstall like the installation identity; an existing install's governed namespaces behave as before; rollback is documented and fails closed.
- **Tests/evidence:** chart-lint rows for the defaults and the generated key; controller and API rows for the default mode and for localAdmin; the PoC journey with no key file; upgrade and rollback rows; mutant evidence on the mode resolution.
- **Dependencies:** PLAT-19.2 (Done). **Handoff:** the default mode and the key lifecycle, which PROD-14.1 measures.

### PROD-16.2 — Two-person approval in the console

- **Issue:** Where a second person must approve, that person needs a personal key today.
- **Approach:** A per-namespace policy setting (`approverSignature: Console`; absent keeps today's personal key) inside the policy snapshot's digest. The approver signs in to the shared console and clicks Approve. The API verifies its OWN earlier ConsoleConfirmation signature on the stored request before approving and copies requester, subject, UID, plan hash, policy and expiry only from those verified bytes. It then signs the v2 document with `approver {issuer, subject}` and `approvedAt` added as optional fields, using the existing ConsoleConfirmation key usage (no new `KeyUsage`). The API enforces the Approver role (Administrator cannot approve), requester != approver, CSRF and Origin, and refuses an expired request. The controller (Approval verdict and Restore admission) and the runner each re-check the signature, an approver in `<issuer>#<subject>` form, approver != requester, no `urn:logweir:local-admin` principal, and `approvedAt` within the request's window; the runner relaxes its "console key cannot be the approver" rule only when the snapshot says Console. The chart refuses the setting unless `api.console.mode=shared`. The scorecard names the approver's principal.
- **Acceptance:** two Dex users on the PoC: one requests, the other approves with a click, and the restore runs; the same user cannot approve their own request; a confirmation planted with `kubectl` in another user's name is refused; localAdmin refuses console approval; strict namespaces behave as before; old binaries fail closed on the new field.
- **Tests/evidence:** API, controller and runner rows with negative controls for each must-have; the PoC journey with both users and the planted-confirmation row; mutant evidence; an OpenAPI change entry (additive).
- **Dependencies:** 16.1. **Handoff:** D0 amended; SECURITY.md residual for console modes.

### PROD-16.3 — Standing rehearsal authorizations without a personal key

- **Issue:** Rehearsal schedules accept only a personal `GovernedApproval` key, and the API has no route to create one (`crates/logweir-api/src/routes/rehearsals.rs:38`).
- **Approach:** A console route to create a rehearsal schedule whose standing authorization is confirmed or approved under the namespace's mode (16.1/16.2), with the same must-haves.
- **Acceptance and tests:** a rehearsal schedule created and authorized in the console runs on the PoC without a key file; strict namespaces keep the personal key; negative controls as 16.2.
- **Dependencies:** 16.2.

### PROD-16.4 — Auth follow-ups

- **Issue:** Group-to-role mapping has not been proven against an identity provider that emits groups (groups are read only from the ID token), and the audit log is stdout only, with no retention recipe.
- **Approach:** A PoC row with a group-emitting connector (Dex LDAP or the static connector with groups) proving the group-to-role mapping and its fail-closed case; per-provider notes on groups in the ID token; a documented log-shipping example with retention guidance.
- **Dependencies:** none. Tier B.

## Completion record format

The orchestrator appends a record beneath each task it closes, in the same commit that updates the ledger row. The record gives: status; owner; implementation revision or PR; dependency evidence; the decision record and changed responsibilities; migration and rollback behaviour; tests run and actual results; artifacts; remaining limitations; and follow-up rows. A Blocked row names the missing contract, owner decision or reproducible failure. Mark Done only after the acceptance criteria have evidence.

## Sources

Accessed 2026-09-14 unless marked 2026-09-23. Dates are access dates, not inferred publication dates. Recheck version-sensitive behaviour before implementation.

- OSO (2026-09-23): [kafka-backup releases](https://github.com/osodevops/kafka-backup/releases), [Enterprise edition](https://kafkabackup.com/enterprise), [strimzi-backup-operator](https://github.com/osodevops/strimzi-backup-operator) (`src/engine.rs` default image), [kafka-backup-operator](https://github.com/osodevops/kafka-backup-operator). The pinned source `third_party/kafka-backup-v0.21.0.tar.gz`: `docs/OSO_Feature_Gate_PRD.md` and `crates/kafka-backup-core`.
- Kannika — [Product overview](https://www.kannika.io/product/), [pricing and packaging](https://www.kannika.io/pricing/).
- Kannika v0.18.0 — [Backup](https://docs.kannika.io/user-guide/backup/), [Restore](https://docs.kannika.io/user-guide/restore/), [Storage](https://docs.kannika.io/user-guide/storage/).
- Kannika v0.18.0 — [Consumer-group recovery FAQ](https://docs.kannika.io/faq/restored-consumer-groups/), [original-offset FAQ](https://docs.kannika.io/faq/restored-offsets/); Kannika maintainers — [kbridge repository](https://github.com/kannika-io/kbridge), default-branch documentation and licence (2026-09-23).
- Kannika v0.18.0 — [SchemaRegistryBackup](https://docs.kannika.io/user-guide/schema-registry-backup/), [SchemaRegistryRestore](https://docs.kannika.io/user-guide/schema-registry-restore/), [schema mapping](https://docs.kannika.io/user-guide/restore/schema-mapping/).
- Kannika v0.18.0 — [Restore report](https://docs.kannika.io/user-guide/restore/report/), [snapshots](https://docs.kannika.io/user-guide/backup/snapshots/), [data retention](https://docs.kannika.io/user-guide/backup/data-retention/), [lag monitor](https://docs.kannika.io/user-guide/backup/lag-monitor/).
- Kannika v0.18.0 — [Security configuration](https://docs.kannika.io/installation/configuration/security/), [Kafka Streams guidance](https://docs.kannika.io/faq/kafka-streams/), [0.18.0 release notes](https://docs.kannika.io/release-notes/0-18-0/).
- Kannika (2026-09-23) — [roadmap](https://docs.kannika.io/roadmap/), [installation](https://docs.kannika.io/installation/).
- Apache Software Foundation — [Kafka 4.0 design: delivery and transaction semantics](https://kafka.apache.org/40/design/design/); [Kafka 4.2.0 release announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/) (2026-09-23); [release support dates](https://endoflife.date/apache-kafka) (2026-09-23).
- Amazon Web Services — [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html), current user guide.
- Confluent — [Cluster Linking disaster recovery](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-failover.html) and [features/limitations](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html); [KCP](https://docs.confluent.io/cloud/current/clusters/migrate-kcp.html) and [topic management](https://docs.confluent.io/cloud/current/topics/overview.html) (2026-09-23).
- Amazon Web Services — [MSK Replicator overview](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator.html), [bidirectional offset synchronization](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-bidirectional-offset-sync.html), [metadata and ACLs](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-metadata-acl.html), [planned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-planned-failover.html), [unplanned failover](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-unplanned-failover.html). Checked on 2026-09-23: [external sources into Standard brokers](https://aws.amazon.com/about-aws/whats-new/2026/07/amazon-msk-replicator-external-kafka-standard-broker-support/), [into Express brokers](https://aws.amazon.com/about-aws/whats-new/2026/04/amazon-msk-replicator-external-kafka-cluster-support/), [data delivery to S3](https://docs.aws.amazon.com/msk/latest/developerguide/msk-data-delivery-s3.html) and [supported Kafka versions](https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html).
- Redpanda — [Whole Cluster Restore](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/whole-cluster-restore/), [topic recovery](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/topic-recovery/), [retention behavior](https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/disk-utilization/); [Shadowing](https://docs.redpanda.com/streaming/25.3/manage/disaster-recovery/shadowing/overview/) (2026-09-23).
- Strimzi — [Unidirectional Topic Operator](https://strimzi.io/blog/2023/11/02/unidirectional-topic-operator/), [User Operator ACL handling](https://github.com/orgs/strimzi/discussions/8871) (2026-09-23).
- Regulation (2026-09-23) — [DORA, Regulation (EU) 2022/2554](https://eur-lex.europa.eu/eli/reg/2022/2554/oj); [NIS2 implementing regulation (EU) 2024/2690](https://eur-lex.europa.eu/eli/reg_impl/2024/2690/oj). EUR-Lex returned empty pages, so the text was read through secondary sources. [EDPB coordinated enforcement report on the right to erasure](https://www.edpb.europa.eu/system/files/2026-02/edpb_cef-report_2025_right-to-erasure_en.pdf), adopted 2026-02-10.

---
Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
