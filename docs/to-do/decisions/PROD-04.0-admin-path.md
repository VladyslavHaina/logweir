# PROD-04.0 — The Kafka administrative path

Status: proposed for review (fix round 1). Research row, review tier B.

- **The `unsafe` question is OD-6's.** The ledger's owner decision OD-6 (`product-expansion.md`, Owner decisions), proposed by PROD-01.4 §6.4, decides once whether and how Logweir calls librdkafka functions the safe rdkafka API lacks. §7 is this row's **input to OD-6**: the per-operation needs and the measured cost. It takes no option.
- **The engine route belongs to OD-3.** OD-3 decides the engine route per capability. §2 and §8.2 feed it.
- **Nothing lands here.** No file in `crates/`, the ADRs or `docs/stability.md` changes in this row. The amendment texts in §8 land with the implementing rows, after their owner decisions (rule 8).

## 0. Decisions in one page

1. **Per operation** (§4). O1 is the safe consumer API. O2 is librdkafka's C admin API behind the FFI perimeter OD-6 chooses: an FFI crate (a2) or a module of `logweir-kafka` (a1). O4 is a raw-protocol client, and O5 the engine.

   | Operation | Route |
   | --- | --- |
   | List groups with their type | O2 ListConsumerGroups, joined with a name listing |
   | Describe groups | O2 DescribeConsumerGroups, only for ids already classified |
   | Fetch committed offsets | O1, with RequireStable |
   | Commit offsets | O1 from a non-member; the broker refuses it while the group has members |
   | Describe ACLs | O2 DescribeAcls, guarded by two positive probes |
   | Create ACLs | none in product; no row applies ACLs |
   | DescribeProducers, ListTransactions | unsupported; only O4 reaches them |

   **This row recommends against O5 for every operation** (§2, E1–E6). That recommendation feeds OD-3 (the engine route per capability, whose own recommendation already names native paths for offsets and ACLs) and OD-6's option (c). O4 is deferred until a row needs share- or streams-group detail.

2. **Group types** (§5).
   - Classic and consumer groups are captured.
   - Share groups, streams groups and classic groups of other protocols are `excluded: GroupTypeNotCaptured` (`groupType: other`).
   - An id missing from every listing is classified by a targeted call (T14). If it is refused (GROUP_AUTHORIZATION_FAILED), the group is `failed: NotVisibleToPrincipal`. Only if it is answered is the group `excluded: GroupNotFound`.
   
   No group is dropped, and absence never means offset 0.

3. **The safe route is not trap-free, and neither is the listing** (§3, §6).
   - rdkafka 0.36.2's `fetch_group_list` aborts (undefined behaviour) on any member-less group. rust-rdkafka 0.37.0 fixed it.
   - The classic describe it wraps reports every non-classic group, and every absent id, as `Dead`.
   - librdkafka's DescribeAcls, in every release through 2.15.1, reports both "no authorizer" and "not authorised" as "0 bindings".
   - ListGroups shows a caller without Describe on the cluster only the groups it may Describe, silently (T14, §3.9). An id missing from a listing is not "not found" until a targeted call says so.

4. **Input to OD-6** (§7), without a competing recommendation.
   - Four of this row's operations need librdkafka calls that the safe API lacks: the typed listing, describe, DescribeAcls and DescribeCluster.
   - Measured cost: 117 shared lines, plus 30–45 lines per call.
   - Fetch and commit need no `unsafe` under any OD-6 option.
   - What each OD-6 option leaves PROD-04.1, 04.2 and 05.3 is tabled in §7.

5. **ADR text** (§8). Neither text lands in this row.
   - An ADR 0004 amendment for whichever FFI perimeter OD-6 chooses, written perimeter-neutral.
   - An Amendment D amendment naming the engine's v0.21.0 group subcommands.

6. **Child rows** (§9):
   - 04.0a: positions through the safe API;
   - 04.0b: the group and ACL calls, gated on OD-6 (a1) or (a2);
   - 04.0c: Amendment D, owner sign-off;
   - 04.0d: fixtures (`acl`, `groups`, share-group state).

   **Acceptance rows** for PROD-04.1, 04.2 and 05.3 are in §10. PROD-00.1's A-C8-3 is answered in §3.7.

## 1. What exists today

Citation prefixes, used throughout:

- `L/`: Logweir at main `632ea345`. Every `L/` line cited here is unchanged since `ac6aa00e`, where the first round read it.
- `R/`: rust-rdkafka 0.36.2, the locked crate (checksum `1beea247…`).
- `S/`: rdkafka-sys 4.10.0+2.12.1 (checksum `e234cf31…`), so librdkafka 2.12.1
  under `S/librdkafka/src/`.
- `E/`: the pinned engine source, `third_party/kafka-backup-v0.21.0.tar.gz`
  (sha256 `0252a837…05b`), under `crates/`.
- `K/`: Apache Kafka tag `4.3.1` (commit `26b251a451ce941d3d7a55e6487bcb7f16b5ad48`),
  under `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/`.
- `P/`: kafka-protocol 0.18.0 (crates.io, 2026-08-20; the engine's protocol
  crate).
- `A/`: `/tmp/logweir-roadmap-run/claude/artifacts/prod-04-0/`.

### 1.1 Logweir

| ID | Finding | Evidence |
| --- | --- | --- |
| L1 | `logweir-kafka` is the only broker-dialling crate and carries `#![forbid(unsafe_code)]`. Eleven other crate roots carry it too. **Five do not**:<br>`crates/logweir/src/main.rs:1` (the `logweir` binary's own root);<br>`crates/logweir-engine-oso/src/lib.rs:1`;<br>`crates/logweir-store/src/lib.rs:1`;<br>`xtask/src/main.rs:1`;<br>`e2e/src/lib.rs:1`.<br>No `[lints]` table or `clippy.toml` supplies it, and no script checks the attribute. The workspace contains no `unsafe` code today: the word appears only in strings and comments. | `L/crates/logweir-kafka/src/lib.rs:3`; a scan of every member's `src/lib.rs` and `src/main.rs` (members `crates/*`, `xtask`, `e2e`); no `unsafe_code` under `L/scripts/` |
| L2 | Its admin client is used for DescribeConfigs, CreateTopics and a prefix-fenced DeleteTopics. Nothing reads or writes consumer groups or ACLs. | `L/crates/logweir-kafka/src/rdkafka_reader.rs:127-165`; `reader.rs:257-331` |
| L3 | Amendment D denies the engine token `offset`. The gate's primary check is an allowlist of argv tokens, so every group subcommand of v0.21.0 is already refused; the secondary scan's literal `"offset"` predates those subcommand names. | `L/docs/architecture.md` Amendment D; `L/scripts/check-no-oso.sh:84-98`, `:298` |

### 1.2 rust-rdkafka and librdkafka

| ID | Finding | Evidence |
| --- | --- | --- |
| C1 | rdkafka 0.36.2's `AdminClient` offers create/delete topics, delete groups, create partitions and describe/alter configs. It has no group listing or description, no offset list or alter, and no ACL call. | `R/src/admin.rs:51-332` |
| C2 | `Client::fetch_group_list` wraps the legacy `rd_kafka_list_groups`. That sends ListGroups **v0** to every broker, then DescribeGroups **v0**, and fills every field from the DescribeGroups reply: name, state, protocol type, protocol, members. ListGroups' own protocol type is discarded. `GroupInfo` exposes no per-group error. | `R/src/client.rs:473-501`; `R/src/groups.rs:73-120`; `S/…/rdkafka.c:5183` (v0), `:5099` (v0), `:4959-4962` |
| C3 | rust-rdkafka's `TopicPartitionList` has no leader-epoch accessor, so the safe API can neither read a committed leader epoch nor set one. | no `epoch` in `R/src/topic_partition_list.rs` |
| C4 | The safe consumer's `committed_offsets` sends OffsetFetch with RequireStable whenever `isolation.level` is `read_committed`, librdkafka's default, and retries a partition that answers UNSTABLE_OFFSET_COMMIT. The admin path (`ListConsumerGroupOffsets`) does not retry: it returns that code per partition. | `S/…/rdkafka.c:3622-3656`; `rdkafka_request.c:1316-1318`, `:1378-1382`; `rdkafka_admin.c:7114-7116` (`allow_retry` false) |
| C5 | librdkafka 2.12.1's C admin API has ListConsumerGroups (ListGroups up to v5, with a type filter), DescribeConsumerGroups (ConsumerGroupDescribe v0, else a classic DescribeGroups fallback capped at **v4**), List/AlterConsumerGroupOffsets (OffsetFetch/OffsetCommit up to v9), and Describe/Create/DeleteAcls (v0–1). It has no quota API, no DescribeProducers and no ListTransactions. | `S/…/rdkafka.h:5992-6121`, `:7344-7362`, `:8691-9400`, `:9799-10041`; `rdkafka_request.c:1501`, `:1816`, `:2474`, `:2542`, `:6135`, `:5717`, `:5836`, `:5941` |
| C6 | Its group **type** enum is Unknown, Consumer and Classic, and its **state** enum is Unknown, PreparingRebalance, CompletingRebalance, Stable, Dead and Empty. Names are mapped by string, and anything else, such as `share`, `streams`, `Assigning`, `Reconciling` or `NotReady`, becomes Unknown. | `S/…/rdkafka.h:5197-5216`; `rdkafka.c:4874-4914` |
| C7 | ListConsumerGroups keeps a listed group only when its protocol type is empty or `consumer`; every other group (share, streams, Connect) is dropped without an error. | `S/…/rdkafka_admin.c:7489-7526` |
| C8 | DescribeConsumerGroups reads ConsumerGroupDescribe's GroupEpoch and AssignmentEpoch and discards them: the description has no epoch field or accessor. | `S/…/rdkafka_admin.c:8463-8473`; `rdkafka_admin.h:538-563` |
| C9 | DescribeAcls clamps a resource type outside Unknown…TransactionalId (Kafka's DelegationToken = 6, User = 7) and an operation outside Unknown…IdempotentWrite (CreateTokens = 13, DescribeTokens = 14 and later) to **Unknown**, logging a warning. | `S/…/rdkafka.h:7390-7410`, `:7879-7904`; `rdkafka_admin.c:5587-5646` |
| C10 | rdkafka-sys declares `rd_kafka_resp_err_t` as a `#[repr(i32)]` Rust enum whose variants stop at 118, then 129 and 130. librdkafka passes broker error codes through, and a code the enum does not list is an invalid discriminant at the FFI return, which is undefined behaviour. The safe crate carries the same exposure wherever it converts a per-item code. | `S/src/bindings.rs:159-161`, `:331-334` |
| C11 | No rust-rdkafka release wraps any of C5's group, offset or ACL calls, through 0.39.0 (2026-01-25). 0.37.0 added only `delete_records`. PR #785 would add `describe_consumer_groups`, `list_consumer_group_offsets` and `list_offsets`, but not a typed list, alter offsets or ACLs. It is conflicted (`mergeable_state: dirty`) and has no review. PR #838 scaffolds a KIP-932 share **consumer**. | `…/prod-01-4/upstream/rdkafka-0.3{7,8,9}.0/src/admin.rs`; GitHub API, 2026-09-29: PR #785 (opened 2025-08-11, updated 2026-05-22), PR #838 (opened 2026-05-23) |
| C12 | librdkafka's latest release, v2.15.1 (2026-09-09), still has only the three group types. v2.15.0 adds a preview share *consumer*; no release adds share- or streams-group administration. v2.14.2 fixed duplicate groups in ListConsumerGroups when several brokers return one group, a bug present "since 1.x". | `A/librdkafka-v2.15.1-rdkafka.h:6321-6325` (sha256 `5c9810e9…`); `A/librdkafka-v2.15.1-CHANGELOG.md:68-118`, `:151-156`, `:218-222` (sha256 `24d257c8…`) |

### 1.3 The engine (kafka-backup v0.21.0)

| ID | Finding | Evidence |
| --- | --- | --- |
| E1 | The engine is itself a raw-protocol client on kafka-protocol 0.18. It never negotiates versions: ListGroups v2 (no state, no type), DescribeGroups **v0** (the `_ => 0` arm), OffsetFetch v5 (no RequireStable), OffsetCommit v5, and the ACL keys pinned at v1 but never sent. | `E/kafka-backup-core/src/kafka/client.rs:587-611` |
| E2 | `list_groups` records group id, protocol type and an always-empty state; `describe_groups` records no type or epoch; `commit_offsets` only logs per-partition errors. | `E/…/kafka/consumer_groups.rs:107-137`, `:141-188`, `:347-400` |
| E3 | `snapshot-groups` skips, with a `warn!` or `debug!`, every group whose offsets it cannot fetch or that has none on archived topics. | `E/kafka-backup-cli/src/commands/snapshot_groups.rs:81-123` |
| E4 | The group subcommands are `snapshot-groups`, `offset-reset` (plan, execute, script), `offset-reset-bulk`, `offset-rollback`, `three-phase-restore` and `show-offset-mapping`. No subcommand is named `offset`. The engine has no ACL operation at all. | `E/kafka-backup-cli/src/main.rs:188-278`, `:629-822` |
| E5 | The engine's reset relies on the broker refusing a generation-less commit to a group with members (UNKNOWN_MEMBER_ID); it does not check group activity itself. | `E/kafka-backup-core/src/restore/offset_reset.rs:647-662` |
| E6 | OSO's feature-gate PRD lists automatic offset reset, "Kafka API Integration (OffsetCommitRequest)", bulk reset and reset rollback as enterprise-gated. | `E/../docs/OSO_Feature_Gate_PRD.md:151-158`; PROD-00.1 §2 |

### 1.4 The broker (Kafka 4.3.1 source)

| ID | Finding | Evidence |
| --- | --- | --- |
| K1 | `listGroups` filters only by the request's state and type filters, never by request version. Every ListGroups version lists every group of every type. The listed protocol type is `consumer` for classic consumers and KIP-848 groups, `share` for share groups and `streams` for streams groups. | `K/GroupMetadataManager.java:636-665`; `modern/share/ShareGroup.java:45`; `streams/StreamsGroup.java:76`, `:282-288` |
| K2 | The classic `describeGroups` answers a group that is not classic (consumer, share, streams) with state `Dead`. Only from **v6** does it add GROUP_ID_NOT_FOUND; below v6 it adds no error, no members and no protocol type. | `K/GroupMetadataManager.java:769-821` |
| K3 | A non-member OffsetCommit (generation or member epoch −1, empty member id) is accepted by a classic group only in state Empty, otherwise UNKNOWN_MEMBER_ID. A consumer or streams group accepts it only with no members, otherwise it throws UnknownMemberId. To an absent group id, it creates a "simple" classic group. | `K/classic/ClassicGroup.java:831-878`; `modern/consumer/ConsumerGroup.java:649-700`; `streams/StreamsGroup.java:728-760`; `OffsetMetadataManager.java:458-495` |
| K4 | A share group refuses OffsetCommit with GROUP_ID_NOT_FOUND ("Group … is not a consumer group"). Its OffsetFetch validation throws the same exception, which K7 then swallows. Its positions are share-partition start offsets, read by DescribeShareGroupOffsets. | `K/modern/share/ShareGroup.java:216-233` |
| K5 | An admin OffsetFetch (no member id, epoch −1) is accepted for classic, consumer and streams groups. | `K/modern/consumer/ConsumerGroup.java:709-717`; `streams/StreamsGroup.java:776-786`; `classic/ClassicGroup.java:884-892` |
| K6 | The classic DescribeGroups response has no generation field in any version. No admin API returns a classic group's generation; ConsumerGroupDescribe, ShareGroupDescribe and StreamsGroupDescribe return group and assignment epochs. | `P/src/messages/describe_groups_response.rs:176-218`; `share_group_describe_response.rs:131-178` |
| K7 | OffsetFetch for a share group or an absent group id answers every partition with "no committed offset" and **no error**: `fetchOffsets` catches GroupIdNotFound and fails all partitions silently. | `K/OffsetMetadataManager.java:895-915` |

## 2. The five routes, costed once

Mapped to OD-6's options:

| This record | OD-6 |
| --- | --- |
| O1 | the safe API, available under every option; under (e) it is all there is |
| O2 | (a2), an FFI crate, or (a1), a module of `logweir-kafka` |
| O3 | (b) |
| O4 | (d) |
| O5 | (c), and OD-3's engine route |

| Route | What it reaches | Cost | Constraint |
| --- | --- | --- | --- |
| **O1** the safe consumer API (rdkafka 0.36.2, no `unsafe`) | OffsetFetch and OffsetCommit for one group, from a consumer that carries that `group.id` and never subscribes (`committed_offsets`, `commit`). ListGroups v0 plus DescribeGroups v0 through `fetch_group_list`. | Tens of lines behind the existing `client` feature. No new dependency and no ADR change. | No group type, epoch, leader epoch or ACL (C1, C3). `fetch_group_list` is unsound on 0.36.2 and misreports non-classic groups (§3.1). The handle carrying the target `group.id` must be unable to subscribe (§4.3). |
| **O2** librdkafka's C admin API behind OD-6's FFI perimeter: a crate (a2) or a module (a1) | Everything in C5: a typed listing of classic and consumer groups, describe with members and assignments, an all-partition offset listing with leader epochs and an explicit UNSTABLE code, alter offsets, DescribeCluster's authorized operations, and Describe/CreateAcls. | Measured prototype (`A/admin-probe/src/main.rs`, FFI leg of 381 lines): 117 lines of shared helpers (integer-typed error readers, string copy, an options/queue/event runner that destroys everything once), then 30–45 lines per call (§7). No new crate: `rdkafka::bindings` is `rdkafka_sys::bindings`, and librdkafka is already linked and attributed. | The traps of §6. Nothing at all for share or streams groups (C12). |
| **O3** upstream contributions | O2's calls without `unsafe`, once a rust-rdkafka release wraps them. Share and streams types need a **librdkafka** change as well. | Complete and rebase PR #785, add the typed list, alter offsets and ACL wrappers, then bump rdkafka (0.38 has a breaking API change; PROD-01.4 §6.2). Plus librdkafka reports for T3 and T9 (§6); T6 needs only a librdkafka at 2.14.2 or later. | Timeline outside Logweir's control; #785 has had no review in 13 months (C11). |
| **O4** a raw-protocol client (kafka-protocol 0.18.0) | Every API the broker serves: ListGroups v5 types, ShareGroupDescribe, DescribeShareGroupOffsets, DescribeGroups v6, DescribeAcls v3, DescribeProducers, ListTransactions. StreamsGroupDescribe needs a hand-written codec, since P has no StreamsGroup messages. | A second Kafka client: TCP, TLS with hostname verification and CA pinning, SASL (SCRAM now; PLAIN, mTLS and OAUTHBEARER as PROD-01.3 adds them), coordinator routing, version negotiation and retries. The engine's equivalent is about 4,800 lines (`E/kafka-backup-core/src/kafka/{client,scram,tls,connection_error,partition_router}.rs`, `sasl/`). One new dependency (MIT/Apache-2.0) with notices and `cargo deny`. | ADR 0004 forbids a custom protocol path "for operations an existing client provides", which covers everything in C5. A second TLS/SASL stack must stay in agreement with librdkafka's (PLAT-07.1). |
| **O5** the engine behind an Amendment D change | `snapshot-groups`: offsets of every group on archived topics. `offset-reset` and `offset-rollback`: non-member commits. No ACL operation exists. | An Amendment D change, a subprocess per operation, and FX-1's parser. | E1–E6: no type, state or epoch; errors swallowed; OffsetFetch without RequireStable (§3.4); fixed old versions; and the supplier gates automatic reset. |

## 3. Measured outcomes

Fixture: the default e2e compose stack at `ac6aa00e` (`e2e/compose/docker-compose.yml`, project `logweir-e2e`), broker service only. `KAFKA_VERSION=4.3.1` runs `apache/kafka:4.3.1`, image `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (PROD-01.5's pin). The legacy line is `apache/kafka:3.9.2`, `sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02`.

- **Nothing was enabled on 4.3.1.** The image's defaults already carry `group.coordinator.rebalance.protocols=classic,consumer,streams`, and the features `group.version`, `share.version` and `streams.version` are finalized at 1 (`A/live-4.3.1/java-reference.txt`, refs `features` and `broker-group-config`). On 3.9.2 the default protocol list is `classic`, and ShareGroup* and StreamsGroup* are unsupported.
- **ACL rows** add `A/acl-overlay.yml`: `StandardAuthorizer`, `super.users=User:ANONYMOUS` and `allow.everyone.if.no.acl.found=true`. The SCRAM principal `logweir` from `scram-setup` is the non-privileged client.
- **Groups**, one per row:
  - `pa-classic-empty` and `pa-classic-live`: console consumers with `group.protocol=classic`;
  - `pa-consumer-empty` and `pa-consumer-live`: `group.protocol=consumer`;
  - `pa-share-idle` and `pa-share-live`: `kafka-console-share-consumer.sh`;
  - `pa-streams`: `WordCountDemo` with `group.protocol=streams`;
  - `pa-simple`: created by a non-member commit;
  - `pa-txn-group`: the transaction row.
- **Driver and outputs.** `A/live-session.sh` and `A/run-all.sh` drove the main run inside one compose-lock hold, 2026-09-29 06:36:10–06:48:42 UTC. The supplement (06:50–06:58:44 UTC) and the third session (from 06:58:59 UTC) each held the lock separately. Every probe result is one JSON line in `A/live-<line>/evidence.jsonl`, and the Java CLIs' answers are in `java-reference.txt` beside it.

### 3.1 Listing and describing (4.3.1)

| Group | Broker (`kafka-groups.sh --list`, raw ListGroups v5) | safe `fetch_group_list` | FFI ListConsumerGroups | FFI DescribeConsumerGroups | raw DescribeGroups v0 and v5 (v0 is the engine's) |
| --- | --- | --- | --- | --- | --- |
| `pa-classic-empty` | Classic, Empty | **abort** | Classic, Empty | Classic, Empty, 0 members | Empty, protocol type `consumer` |
| `pa-classic-live` | Classic, Stable, 1 member | Stable, `consumer`, `range`, 1 member | Classic, Stable | Classic, Stable, 1 member holding 3 partitions, `range` | Stable, 1 member |
| `pa-consumer-empty` | Consumer, Empty | **abort** | Consumer, Empty | Consumer, Empty, `uniform` | **Dead**, 0 members, no error |
| `pa-consumer-live` | Consumer, Stable, 1 member | **abort** | Consumer, Stable | Consumer, Stable, 1 member holding 3 partitions | **Dead**, 0 members, no error |
| `pa-share-idle` | Share, Empty | **abort** | **not listed** | **simple, Classic, Dead, no error** | **Dead**, no error |
| `pa-share-live` | Share, Stable, 1 member | **abort** | **not listed** | **simple, Classic, Dead, no error** | **Dead**, no error |
| `pa-streams` | Streams, Stable, 1 member | **abort** | **not listed** | **simple, Classic, Dead, no error** | **Dead**, no error |
| `pa-simple`, before it existed | — | (0 groups) | not listed | **simple, Classic, Dead, no error** | **Dead**, no error |

- **abort**: `GroupInfo::members()` builds a slice from librdkafka's NULL member array for every member-less group. The debug build's precondition check aborts the process (exit 134, `A/live-4.3.1/session.log`); a release build has undefined behaviour instead. The defect is in `R/src/groups.rs:86-92`, and rust-rdkafka fixed it in commit `9be7eca2fdac` (2024-08-04, "Missing null pointer check in src/groups.rs"), released in 0.37.0. Logweir locks 0.36.2. **The all-groups call aborted too**, after reporting 7 groups; only a lookup of the one Stable classic group returned. Logweir calls none of this today (no `fetch_group_list` or `members()` in `L/crates`, `L/e2e`).
- Raw ListGroups at **v0, v2, v4 and v5** each listed all 7 groups (K1). v0 and v2 carry the protocol type (`consumer`, `share`, `streams`) and no state; v4 adds the state; v5 adds the type.
- Raw ShareGroupDescribe v1 described both share groups:
  - `pa-share-live`: Stable, group epoch 2, assignment epoch 2, assignor `simple`, one member subscribed to `pa-share-in`;
  - `pa-share-idle`: Empty, group epoch 3.
  
  For every other id it returned error 69, "Group … is not a share group.", and for the absent id "Group pa-simple not found."

Supplement run on a fresh 4.3.1 stack (`A/live-4.3.1-supplement/`):

- **DescribeGroups v6** is the one classic-describe version that tells the truth. It returned 69 with "Group … is not a classic group." for `pa-consumer-live`, `pa-share-idle`, `pa-share-live` and `pa-streams`, and "Group pa-absent not found." for an absent id. At **v4**, librdkafka's fallback version, the same ids came back `Dead` with no error.
- **`fetch_group_list`, read without `members()`**, returned `Dead`, protocol type empty and protocol empty for all four non-classic groups, and `Empty`/`consumer` for the classic one. That is K2 through the safe API, once the abort is avoided.

### 3.2 Fetching committed positions (4.3.1)

| Group | safe `committed_offsets` (read_committed, asked for `pa-orders` 0–2, `pa-share-in` 0, `streams-plaintext-input` 0) | FFI ListConsumerGroupOffsets (all partitions, require_stable) | raw OffsetFetch v5, all partitions (the engine's request) |
| --- | --- | --- | --- |
| `pa-classic-empty` | `pa-orders` 9, 0, 3; the rest Invalid | the same three; leader epochs 0, −1, −1 | the same |
| `pa-classic-live` | 9, 12, 9 | the same; leader epochs 0 | the same |
| `pa-consumer-empty`, `pa-consumer-live` | as their classic counterparts | the same | the same |
| `pa-streams` | `streams-plaintext-input` 0 → 3 | that, **and** `pa-streams-…-repartition` 0 → 9 | as FFI |
| `pa-share-idle`, `pa-share-live` | every partition Invalid, no error | no partitions, no error | no partitions, error 0 |
| `pa-simple`, before it existed | every partition Invalid, no error | no partitions, no error | no partitions, error 0 |

All three routes agree wherever a group has commits. A share group, an absent group and a group that never committed give the **same** answer (K7). The safe route asks only for named partitions, so it cannot discover a group's other committed topics. FFI and raw list them all, and carry the committed leader epoch that the safe route cannot read (C3).

### 3.3 Committing from a non-member (4.3.1)

| Target group | safe `commit` (sync) | FFI AlterConsumerGroupOffsets | Readback |
| --- | --- | --- | --- |
| classic, Empty | ok | ok | applied |
| classic, Stable with 1 member | `UnknownMemberId` | partition error 25 | unchanged |
| consumer, Empty | ok | ok | applied |
| consumer, Stable with 1 member | `UnknownMemberId` | 25 | unchanged |
| share, Empty or Stable | `GroupIdNotFound` | 69 | — |
| streams, Stable with 1 member | `UnknownMemberId` | 25 | unchanged |
| control: the classic member stopped (group Empty) | ok, the same commit | — | applied |
| an absent id (`pa-simple`) | ok; the id now lists as type classic, protocol type empty, Empty | — | applied |

The first run's control killed only the classic member: its `pkill -f "group pa-classic-live"` also matched its own `sh -c`, which died before the other two `pkill`s ran. The supplement redid the control for the other two types, with bracketed patterns and on a fresh stack (`A/live-4.3.1-supplement/`):

| Target group | Member live (06:53:48Z) | Member stopped (06:58:35Z) | Readback after the second commit |
| --- | --- | --- | --- |
| consumer (`pa-consumer-live`) | `UnknownMemberId` | FFI: Consumer, Empty, 0 members. Commit **ok**. | `pa-orders` 0 → 1, leader epoch −1 |
| streams (`pa-streams`) | `UnknownMemberId` | ListGroups v5: streams, Empty (FFI describe: "simple, Classic, Dead"). Commit **ok**. | `streams-plaintext-input` 0 → 1, leader epoch −1, metadata null: the probe's commit, not the application's. The application's own commits carry metadata; its repartition topic shows `AgAAAaDr7mxK` in the same readback (`A/live-4.3.1-supplement/evidence.jsonl:45`). |

**An Empty streams group accepts a non-member commit.** Nothing in the broker stops Logweir from overwriting a stopped Streams application's input positions without its state. That is what AP-04.2-2 guards.

The broker, not the client, decides (K3, E5): a non-member commit to a group with members is refused for the whole request, however long ago the caller checked. That is the atomic guard 04.2 relies on.

### 3.4 A pending transactional offset commit (4.3.1)

`pa-txn-group` committed 3 on `pa-txn` 0. A transactional producer then sent offset 7 into an open transaction (TxnOffsetCommit), held it for 45 s, and aborted.

| Reader | While 7 was pending | After the abort |
| --- | --- | --- |
| safe `committed_offsets`, `read_committed` (librdkafka's default) | **failed after 15,000 ms**: "Meta data fetch error: OperationTimedOut". librdkafka retried the UNSTABLE partition until the call's timeout, and the error's name says metadata. | — |
| safe, `read_uncommitted` | 3 | — |
| FFI, require_stable | partition error **88** (UNSTABLE_OFFSET_COMMIT), offset −1001 | 3 |
| FFI, without require_stable | 3 | — |
| raw OffsetFetch v5 (the engine's) | **3**, error 0 | 3 |

A reader without RequireStable returns the pre-transaction position with no sign that a newer one is pending. If the transaction commits, that position is behind by the transaction's records, so replay duplicates them. The engine's snapshot is such a reader (E1).

### 3.5 ACLs (4.3.1)

| Case | librdkafka 2.12.1 through FFI | Java `kafka-acls.sh` |
| --- | --- | --- |
| No authorizer (the default stack) | DescribeAcls: **0 bindings, no error**. CreateAcls: 54 SECURITY_DISABLED per binding. | SecurityDisabledException |
| StandardAuthorizer, no ACLs | 0 bindings | — |
| Six bindings created through FFI: literal topic, prefixed `pa-`, group, wildcard name `*`, wildcard principal `User:*` with DENY, transactional id | all created; read back with the right pattern and permission types | listed identically |
| CLUSTER resource | resource type 4, which librdkafka names `BROKER` | `CLUSTER` |
| USER resource `User:bob`, CreateTokens and DescribeTokens | **two identical bindings**: resource type 0, operation 0 (Unknown) | two distinct bindings |
| DELEGATION_TOKEN `tok1`, Describe | resource type 0 (Unknown), operation 8 | `DELEGATION_TOKEN` |
| TRANSACTIONAL_ID `pa-2pc`, TwoPhaseCommit | operation 0 (Unknown) | `TWO_PHASE_COMMIT` |
| SCRAM principal `User:logweir`, not a super user, once a cluster ACL exists | DescribeAcls: **0 bindings, no error**. CreateAcls: 31 CLUSTER_AUTHORIZATION_FAILED. | — |
| control: the same principal, allowed Describe on the cluster | 12 bindings | — |

**Both "0 bindings, no error" rows have one cause.** `rd_kafka_DescribeAclsResponse_parse` reads the response's top-level ErrorCode into a local, then returns NO_ERROR with the (empty) binding list whatever the code was (`S/…/rdkafka_admin.c:5553-5560`, `:5658`). In v2.15.1 its error handling is unchanged (`A/librdkafka-v2.15.1-rdkafka_admin.c`, sha256 `8ed0e410…`). The function is not byte-identical: 2.15.1 adds two `rd_kafka_buf_skip_tags` calls, but the lines that read and drop the error are the same. A GitHub search on 2026-09-29 found no upstream issue for it.

### 3.6 What differs on 3.9.2

Same driver, `KAFKA_VERSION=3.9.2` (`A/live-3.9.2/`):

- **KIP-848 groups cannot form.** The default `group.coordinator.rebalance.protocols=classic` answers a `group.protocol=consumer` client with UNSUPPORTED_VERSION (`session.log`). So only classic groups exist, and FFI ListConsumerGroups lists both, typed Classic: 3.9.2 serves ListGroups v5.
- **No share or streams APIs.** A raw ShareGroupDescribe or DescribeShareGroupOffsets makes the broker close the connection, so the probe read "failed to fill whole buffer". Any O4 client must negotiate ApiVersions first; the engine never does (E1).
- **No v6 answer.** DescribeGroups stops at v5, so no version reports an absent or non-classic id as an error. The absent ids answer `Dead` with no error, through FFI describe and raw v0 and v5 alike.
- **T1 is not a 4.x matter.** The safe listing aborted (exit 134) on the all-groups call and on `pa-classic-empty`. Any cluster with an Empty classic group, which is to say any stopped consumer application, trips it.
- **The API floors differ.** DescribeAcls, CreateAcls and DeleteAcls serve v0–3 on 3.9.2 and v1–3 on 4.3.1; librdkafka sends v1 on both (C5).
- **Fetch and commit behave as on 4.3.1** for classic groups: positions equal across the three legs, UnknownMemberId for the live group, ok for the Empty one.

### 3.7 PROD-00.1's A-C8-3, answered

- **What the engine sees.** Its ListGroups v2 sees every group of every type on 4.3.1, with the protocol type (`consumer`, `share`, `streams`) and no state or type (§3.1). Its DescribeGroups v0 answers every consumer-protocol, share and streams group, and any absent id, as `Dead` with no members and no error.
- **What its snapshot keeps**, since it iterates the listing and fetches offsets (E3). This is **inferred, not run**: it rests on the source (`E/kafka-backup-cli/src/commands/snapshot_groups.rs:81-123`, which has no protocol-type filter) and on the probe replaying the engine's two requests. `snapshot-groups` itself was not run; AP-04.1-7's fixture runs it.
  - share and absent groups are dropped silently (no offsets, K7);
  - a **streams group's offsets are kept with no type at all**, as though a consumer group's (`pa-streams` answered OffsetFetch v5 with its input and repartition positions, §3.2).
- **What follows for FX-1 and 04.1.** Engine snapshots import as `groupType: unknown` (§5, AP-04.1-7).

### 3.8 Share-group state, and T9's guard (4.3.1)

**Share groups on the default stack have no state.** On the default single-broker stack a share group gets members but never share-partition state:

- the broker could not create `__share_group_state`, whose defaults are replication factor 3 and min ISR 2; every auto-creation attempt logged INVALID_REPLICATION_FACTOR;
- `kafka-share-groups.sh` printed "has no offset information";
- DescribeShareGroupOffsets v0 answered with nothing for a null topic list, and **did not answer within 15 s** for an explicit one (`A/live-4.3.1-supplement/`).

**With state, the start offsets are readable.** `A/share-state-overlay.yml` sets `share.coordinator.state.topic.replication.factor=1` and `…min.isr=1`. With it, `share.auto.offset.reset=earliest` on both groups, and 60 records across two partitions (`A/live-4.3.1-aclguard/`):

| Group | ShareGroupDescribe v1 | DescribeShareGroupOffsets v0 (explicit topics, and a null list) | `kafka-share-groups.sh --describe` |
| --- | --- | --- | --- |
| `pa-share-live` | Stable, group epoch 3, assignment epoch 3, 1 member | p0 **22**, p1 **18** | 22, 18 |
| `pa-share-idle` | Empty, group epoch 4 | p0 **10**, p1 −1 | 10, "-" |
| `pa-absent` | — | explicit: −1 on each partition, **no error**; null: nothing | — |
| `pa-share-live` through the safe `committed_offsets` and FFI ListConsumerGroupOffsets | — | every partition Invalid, and no partitions, with no error (K7) | — |

**What O4 would give 04.1.** A share group's analogue of a position, the share-partition start offset, is readable through O4 only. A start offset of −1 means "no state for this partition" and also "no such group", so here too the listing must classify first.

**T9's guard, measured** (`A/live-4.3.1-aclguard-rerun/`). The first run of this session was invalid: its two probe commands were missing from the built binary and exited 101 (`A/live-4.3.1-aclguard/session.log`). The rerun used a rebuilt probe.

| Case | Safe DescribeConfigs, broker 1001: `authorizer.class.name` | DescribeCluster, the caller's authorized operations | DescribeAcls |
| --- | --- | --- | --- |
| No authorizer | `""`, default (read as `User:ANONYMOUS`) | Create, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite for both `User:ANONYMOUS` and `User:logweir` (every operation: `AuthHelper.scala:62-76` returns all of them when there is no authorizer) | 0 bindings, no error, for both |
| StandardAuthorizer, `User:ANONYMOUS` (a super user) | `org.apache.kafka.metadata.authorizer.StandardAuthorizer` | every operation | — |
| `User:logweir` before any cluster ACL exists (allow-everyone-if-no-ACL) | — | every operation | 1 binding |
| `User:logweir` once a cluster ACL names only `User:ops` | **the key is missing**: the resource came back with no entries and no error (T13) | **none** | **0 bindings, no error** |
| control: `User:logweir` allowed Describe on the cluster | — | Describe | 3 bindings |

**So the two probes separate the three states DescribeAcls conflates.**

- The broker configuration gives `""` for "no authorizer".
- DescribeCluster gives "no Describe" for "denied".
- A key that is missing from a refused DescribeConfigs means only "unverified", never "disabled".

### 3.9 Groups hidden from the caller, and the authorizer's settings (4.3.1, fix round)

The setup (`A/live-4.3.1-visibility-rerun/`, `A/live-visibility.sh`):

- slot 0 with `stack-env.sh --kafka 4.3`, plus `A/acl-overlay.yml`, under the compose lock;
- `pa-visible` and `pa-hidden`, two simple classic groups, each created by a non-member commit (5 and 7 on `pa-orders` 0);
- ACLs naming only `User:ops`: Describe and Read on group `pa-hidden`, and Describe on the cluster;
- `User:logweir`, over SCRAM, therefore has neither Describe on the cluster nor Describe on `pa-hidden`.

An earlier run is void (`A/live-4.3.1-visibility/`): its creating commits met `NotCoordinator` on the fresh broker. The rerun retries them until the coordinator is ready.

| Reader | `User:ANONYMOUS` (super user) | `User:logweir` | `User:logweir` after Describe on the cluster (control) |
| --- | --- | --- | --- |
| DescribeCluster, authorized operations | — | none | Describe |
| raw ListGroups v5 | `pa-hidden`, `pa-visible` | — | — |
| safe `fetch_group_list` (names, no `members()`) | — | **`pa-visible` only** | both; `pa-hidden` with state `""` |
| FFI ListConsumerGroups | — | **`pa-visible` only** (count 1) | both, Classic, Empty |
| targeted, `pa-hidden`: FFI DescribeConsumerGroups; FFI ListConsumerGroupOffsets | — | **30** GROUP_AUTHORIZATION_FAILED; **30** | — |
| targeted, `pa-hidden`: safe `committed_offsets` | — | **timed out after 15 s** ("Meta data fetch error: OperationTimedOut") | still timed out: cluster Describe grants no group access |
| targeted, `pa-absent` (never created, no ACL, so describable) | — | safe: Invalid on every partition, 86 ms, no error; FFI describe: "simple, Classic, Dead", no error | — |
| targeted, `pa-visible` | — | safe: 5 on partition 0; FFI describe: Classic, Empty | — |

What the rows show:

- **Filtered silently (T14).** A caller without Describe on the cluster is shown only the groups it may Describe, with no error (`KafkaApis.scala:1350-1374`). A group missing from both listings may therefore exist.
- **Targeted calls tell the difference.**
  - FFI DescribeConsumerGroups and ListConsumerGroupOffsets answer an undescribable id with 30 (the source is `KafkaApis.scala:1302-1315` and `:1018-1030`).
  - The safe `committed_offsets` gives only a timeout. FindCoordinator for that group is refused too (`KafkaApis.scala:1244-1247`), so the consumer never finds a coordinator. *PROD-04.0a's handle also reads the lookup's 30 off its queue after the bound (§13).*
- **An absent, describable id answers at once with no error** (K7), which is what lets it be called absent.
- **With Describe on the cluster, the listing is unfiltered.** The safe listing then shows the undescribable group with an empty state and no error: `GroupInfo` exposes no per-group error (C2).

**The authorizer's settings can be seen to be set, but not read.** DescribeConfigs on broker 1001, as `User:ANONYMOUS` (344 entries), returned `super.users` and `allow.everyone.if.no.acl.found` as present, not default, **`is_sensitive: true` and value null**.

- The source: `KafkaConfig.maybeSensitive` treats a key whose type it cannot determine as sensitive, "to be safe" (`kafka-src/4.3.1-KafkaConfig.scala:123-126`, sha256 `1fdcec9c…`), and the broker then returns a null value (`4.3.1-ConfigHelper.scala:246-249`, sha256 `58c22ccf…`). Both keys belong to the authorizer, not to the broker's typed configuration.
- For `User:logweir` the same read returned no entries at all (T13).

The first run of this session (`A/live-4.3.1-visibility/`) measured the same targeted and configuration rows; only its listings were void.

## 4. Decision per operation

| # | Operation | Recommended route | Why not the others | Cost | Under OD-6 (e): no `unsafe` |
| --- | --- | --- | --- | --- | --- |
| 1 | List groups with their type | **O2** `ListConsumerGroups` for the typed list of classic and consumer groups, joined by group id with a **name** listing (ListGroups). DescribeCluster says whether the listing is complete: it is unfiltered only with Describe on the cluster (T14). | O1's listing aborts on 0.36.2 and has no type. O2 alone drops share and streams groups silently (C7). O4 would give every type but brings a second client. O5 has no state or type. | ~45 lines inside OD-6's perimeter, plus the name listing: the safe `fetch_group_list` with `members()` never called (§4.1), or `rd_kafka_list_groups` inside the perimeter | Classic groups only, with a guard against `members()`. Consumer (KIP-848) groups cannot be told from streams groups, so they are not captured (§7.1). An undescribable id reads as a timeout (§5); PROD-04.0a's handle also reads the lookup's 30 off its queue (§13). |
| 2 | Describe groups | **O2** `DescribeConsumerGroups`, only for ids the typed list classifies as classic or consumer, and only when its type agrees with the list | O1: unsound and wrong. O2 on any other id returns "simple Classic Dead" with no error (§3.1). O4 would add epochs and share and streams detail that no row needs. | ~42 lines | Membership of classic groups only; consumer groups undescribed; no explicit 30 for an undescribable id (§3.9), though PROD-04.0a's handle reads the lookup's 30 off its queue (§13) |
| 3 | Fetch committed offsets | **O1** `committed_offsets` from a non-subscribing consumer, over the recovery point's partitions, `read_committed` (RequireStable) | O2 adds discovery of partitions outside the recovery point, leader epochs and an explicit code 88; useful, not required. O5 returns stale positions (§3.4). | ~30 lines, no `unsafe` | Unchanged; `PositionsUnstable` per group (§4.2) |
| 4 | Commit (alter) offsets | **O1** `commit` from a non-subscribing consumer; the broker's refusal is the atomic liveness guard (§3.3) | O2 has the same broker semantics with per-partition codes. O5 is gated upstream and logs only (E2, E6). | ~30 lines, no `unsafe` | The commit is unchanged. The pre-apply type check has only the safe describe, which cannot tell an Empty consumer group from an Empty streams group (§3.1, §3.3), so 04.2 must refuse every existing target group the describe does not prove classic, and Empty consumer groups become unsupported targets. |
| 5 | Describe ACLs | **O2** `DescribeAcls`, with guards against T9 and T10 (§6) | No safe, upstream or engine call exists. O4 is exact but brings a second client. | ~35 lines, plus T9's two probes: the broker's `authorizer.class.name` through the safe DescribeConfigs, and DescribeCluster's authorized operations (FFI, ~30 lines) | **No route**: PROD-05.3 stays Blocked, or funds O4 |
| 6 | Create ACLs | **None in product**: no row applies ACLs (PROD-05.3 exports only). Fixtures use `kafka-acls.sh`. O2's CreateAcls is measured for a future apply row. | — | — | — |
| 7 | DescribeProducers, ListTransactions (handed over by PROD-01.1 §6.2) | **Unsupported**: no row needs them, and only O4 reaches them (C5, C12; P has the messages, 4.3.1 serves them) | — | — | — |

### 4.1 The name listing without the abort

`fetch_group_list` stays usable for **names**, provided nothing calls `GroupInfo::members()` on 0.36.2. Two fences, either of which suffices:

- a workspace `clippy.toml` entry under `disallowed-methods` for `rdkafka::groups::GroupInfo::members`, which the existing `-D warnings` clippy run turns into an error;
- a bump to rdkafka 0.37.x, whose `rdkafka-sys` requirement (`4.8.0`, caret) keeps the locked 4.10.0+2.12.1.

The bump is a dependency decision for the child row: notices, `cargo deny`, and 0.37's own changes. The listing's state and member count **are never read for a group the typed list does not classify** (K2): they describe a "Dead" stand-in, not the group.

### 4.2 Fetch

- Positions are read per selected group, for exactly the recovery point's partitions.
- **What the fetch cannot say.** A partition the broker answers as Invalid (librdkafka −1001, wire −1) has **no committed position**, never offset 0. Because an absent group and a share group give the same answer (K7), the listing must classify the group before its positions mean anything.
- **Pending transactions, and the unit of `PositionsUnstable`.**
  - The safe route makes one RequireStable fetch per group, covering all its selected partitions. One pending transactional commit anywhere in it makes the whole call time out (§3.4: 15,000 ms, with no partition named).
  - So under O1 the unit is **the group**. Every selected partition of it is `failed: PositionsUnstable`, and the label says "pending transactional offsets, coordinator unavailable, or group not visible", because the timeout text is the same for all three (§3.4, §3.9).
  - Naming the affected partitions takes one fetch per partition under O1, or O2's per-partition code 88. The unit is then the partition.
- **Topics the caller may not Describe.** The explicit-partition fetch O1 makes answers them with TOPIC_AUTHORIZATION_FAILED per partition (`KafkaApis.scala:1148-1152`). The all-partitions form that O2 and the engine use drops them silently (`:1075-1085`). O2's discovery of a group's other topics is therefore itself limited by visibility.

### 4.3 Commit

- **The handle.** A `BaseConsumer` built with the target `group.id` and `enable.auto.commit=false`. It is wrapped in a type that exposes only `committed_positions` and `commit_positions`, so no caller can `subscribe` or `assign` with it and join the application's group.
- **Epoch and metadata.** Commits carry leader epoch −1, the safe API's only value (C3). A source leader epoch means nothing on a target cluster. The metadata string is Logweir's own marker.
- **Error mapping.**
  - UNKNOWN_MEMBER_ID (25) → `GroupActive`;
  - GROUP_ID_NOT_FOUND (69) → `NotAConsumerGroup`;
  - GROUP_AUTHORIZATION_FAILED (30) → `NotAuthorized`. OffsetCommit needs Read on the group (`KafkaApis.scala:273-282`).
  - A principal without Describe on the group never finds its coordinator (`:1244-1247`). The safe commit then times out rather than answering 30. This is inferred from §3.9's fetch, which does exactly that; the commit case was not run. *PROD-04.0a measured it (§13): the commit fails `_WAIT_COORD` after the handle's bound, nothing changes, and the coordinator lookup's 30 on the handle's queue names it `NotAuthorized`.*
  - anything else → `failed` with its integer code.
- **The target group is classified before any commit, by §5's rules.**
  - An id classified `GroupNotFound` is created as a simple classic group by the commit (§3.3), and the audit says `created`.
  - An id classified `NotVisibleToPrincipal` or `NotVisibleOrUnreachable` is refused `TargetNotVisible`. It may be a live group this principal cannot see.

### 4.4 ACL coverage, reconciled with D2 §5.4

D2 §5.4 (`decisions/D2-destinations-discovery-readiness.md`, "Visibility and completeness policy") rejects ACL-derived completeness for topic discovery, for four reasons. PROD-05.3 asks something narrower: whether an export holds the broker's ACL bindings, and what they mean. Each reason still applies, and is answered as follows.

1. **"It needs `DescribeAcls` or `DescribeCluster`, which are unavailable without unsafe FFI."** This goes away only if OD-6 takes (a1) or (a2), or (d). Under (e), and under (b) until a release, it stands, and 05.3 has no route (§4, #5).
2. **"It is authorizer-specific: `super.users` and `allow.everyone.if.no.acl.found` are broker config, not ACLs."** This stands in part.
   - The DescribeConfigs read that gives `authorizer.class.name` also shows whether each key is set (present, not default), but it withholds the value: the key is sensitive (§3.9).
   - So the export carries each key as `set, value unreadable`, `default`, or `not reported`.
   - An ACL set exported beside any `set` key is labelled `semanticsUnverified`. Its bindings are exact, but which requests they allow is not known.
   - An administrator attestation, D2 §5.4's own pattern, may supply the values. Logweir records it as attested, not verified.
3. **"It cannot represent non-ACL authorizers such as MSK IAM or RBAC."** This is answered by an allowlist of authorizer classes.
   - DescribeAcls is trusted only when `authorizer.class.name` is `org.apache.kafka.metadata.authorizer.StandardAuthorizer` (KRaft) or `kafka.security.authorizer.AclAuthorizer` (ZooKeeper-mode 3.x).
   - Any other class gives `unverified: authorizerNotAclBased`, whatever DescribeAcls answers.
4. **"A plausible but wrong 'complete' is the worst possible outcome."** The export never states "complete".
   - Coverage is `captured` only when every guard passes:
     - a known ACL class;
     - Describe on the cluster (DescribeCluster);
     - DescribeAcls answered;
     - every unrepresentable binding counted (T10).
   - Every other combination is `unverified`, `captureDenied` or `aclsNotApplicable`, each with its reason.
   - Even `captured` claims only "the bindings this broker returned", never "the access this cluster allows".

## 5. Group types: what is captured, and the labels

Every selected group gets exactly one of these outcomes in PROD-04.1's snapshot. Nothing is dropped, and absence never means offset 0.

| Classification (§4 #1) | Outcome | Label and reason | Notes |
| --- | --- | --- | --- |
| Typed list: Classic, with protocol type `consumer` or empty (a simple group) | `captured` | `groupType: classic` | positions per partition, state, member count |
| Typed list: Consumer | `captured` | `groupType: consumer` | a state librdkafka maps to Unknown (`Assigning`, `Reconciling`: C6) is recorded as `stateUnknownToClient` and treated as **active** by 04.2 |
| In the name listing, not in the typed list | `excluded` | `GroupTypeNotCaptured`, `groupType: other` ("a share group, a streams group, or a classic group of a non-consumer protocol such as Kafka Connect") | Under O1 and O2 no call names which. With O4 the label carries `share` or `streams`, and a share group can become `captured: startOffsetOnly` (DescribeShareGroupOffsets). |
| In neither listing, and the listing is complete (DescribeCluster shows Describe on the cluster) | `excluded` | `GroupNotFound` | an unfiltered listing omits only absent ids (T14) |
| In neither listing; the listing is filtered (no Describe on the cluster), and a targeted FFI DescribeConsumerGroups returns 30 | `failed` | `NotVisibleToPrincipal` | the group may exist, and this principal may not see it (§3.9). Never `GroupNotFound`. |
| In neither listing; the listing is filtered, and the targeted call answers without error ("simple, Classic, Dead") | `excluded` | `GroupNotFound` | the id is describable, so a filtered listing would have shown it |
| Under OD-6 (e), no DescribeCluster and no FFI: the targeted safe fetch answers at once without error | `excluded` | `GroupNotFound` | §3.9: 86 ms, every partition Invalid |
| Under OD-6 (e): the targeted safe fetch times out, and the handle's queue holds the coordinator lookup's GROUP_AUTHORIZATION_FAILED | `failed` | `NotVisibleToPrincipal` | measured by PROD-04.0a (§13): librdkafka posts the refusal there while the fetch waits out its bound |
| Under OD-6 (e): the targeted safe fetch times out with no such refusal | `failed` | `NotVisibleOrUnreachable` | the safe route cannot separate the rest (§3.9, §4.2) |
| Imported from an engine snapshot (FX-1) | as found in the archive | `groupType: unknown` | the engine records no type (§3.7); 04.2 applies such positions only after its own target-side classification |
| Listed, but its fetch refused, pending or failing | `failed` | `NotAuthorized` (30 on a listed id, e.g. one shown only through Describe on the cluster: §3.9), `PositionsUnstable` (per group under O1: §4.2), `Unreachable` | per group; other groups continue |

**Streams groups are `excluded` even though their positions are readable** (K5, §3.2). A Kafka Streams application resumes correctly only with its state stores, changelogs and repartition topics recovered consistently, and that is PROD-06's application profile. A share group has no committed offsets at all (K4). Its analogue, the share-partition start offset, is reachable only through O4.

## 6. Representation traps

PROD-01.4 found one (its C4: librdkafka's topic-ID text uses the wrong base64 alphabet). These are the group, offset and ACL counterparts, each with the guard its implementation owes.

| ID | Trap | Where | Guard |
| --- | --- | --- | --- |
| T1 | `GroupInfo::members()` on a member-less group: NULL slice, undefined behaviour | O1, rdkafka 0.36.2 (§3.1) | §4.1: `disallowed-methods`, or rdkafka ≥ 0.37 |
| T2 | The classic describe answers a group that is not classic, and an absent id, as `Dead` with no members and no error | O1, O5, and O2's fallback (K2, §3.1) | classify with the typed list first; never read state or members of an unclassified group |
| T3 | ListConsumerGroups drops every group whose protocol type is not empty or `consumer` | O2 (C7) | join with the name listing (§4 #1) |
| T4 | Group types and states outside librdkafka's enums become Unknown | O2 (C6) | Unknown state is active; Unknown type is `other` |
| T5 | Group and assignment epochs are parsed and discarded | O2 (C8) | none needed by any row; O4 if one ever is |
| T6 | Duplicate groups in ListConsumerGroups when several brokers answer for one group (fixed only in v2.14.2) | O2 on 2.12.1 (C12) | de-duplicate by group id; single-broker fixtures cannot show it, so a unit test over the joiner |
| T7 | "No committed offset" is −1 on the wire, −1001 in librdkafka, `Offset::Invalid` in rust-rdkafka; and a share or absent group answers the same | O1, O2, O4 (§3.2, K7) | an explicit `noCommittedPosition`; classification before interpretation |
| T8 | A fetch without RequireStable returns the pre-transaction position silently; the safe route's RequireStable failure reads as a metadata timeout | O5 (§3.4), and any OffsetFetch below v7; O1's error text | RequireStable always; the timeout label of §4.2 |
| T9 | DescribeAcls drops the response's top-level error: "authorizer disabled" and "not authorised" both read as "0 bindings" | O2, every librdkafka through 2.15.1 (§3.5) | trust "0 bindings" only after two positive probes: the broker's `authorizer.class.name` through the safe DescribeConfigs (empty: `authorizerDisabled`; outside §4.4's allowlist of ACL authorizers: `unverified: authorizerNotAclBased`), and DescribeCluster's authorized operations for the capturing principal (no Describe: `captureDenied`); a refused probe gives `unverified` (AP-05.3-1, -2, -5). Report the defect upstream (O3). |
| T10 | ACL resource types above TransactionalId (DelegationToken, User) and operations above IdempotentWrite (CreateTokens, DescribeTokens, TwoPhaseCommit) become Unknown; distinct bindings collapse into identical ones | O2 (C9, §3.5) | count them as `notRepresentable` with principal and name; never export an Unknown field |
| T11 | Kafka's CLUSTER resource is librdkafka's `BROKER` (4) | O2 (§3.5) | map by number, name it `CLUSTER` in the model |
| T12 | rdkafka-sys types broker error codes as a Rust enum that stops at 118, 129 and 130 | O2 and parts of O1 (C10) | read codes as C integers through module-local declarations (the prototype does) |
| T13 | rust-rdkafka's `DescribeConfigsFuture` never reads `rd_kafka_ConfigResource_error`: a refused or unknown resource comes back as `Ok` with no entries. Unfixed through 0.39.0. | O1: `R/src/admin.rs:1121-1159` (no call in 0.37–0.39 either); measured for a denied broker resource (§3.8) | T9's first probe reads a **missing** `authorizer.class.name` as `unverified`, and only a present, empty value as disabled. The same defect reaches `logweir-kafka`'s existing `topic_configs` and `broker_configs`: see §11's class sweep, which is FX-4's. |
| T14 | ListGroups filters silently. A caller without Describe on the cluster is shown only the groups it may Describe, with no error; the safe listing, the FFI listing and the engine's all inherit it. With Describe on the cluster the listing is complete, but the safe listing then shows an undescribable group with an empty state and no error (C2). | the broker: `kafka-src/4.3.1-KafkaApis.scala:1350-1374` (sha256 `f93449b5…`); measured, §3.9 | §5: an id missing from every listing is `GroupNotFound` only if the listing is complete (DescribeCluster shows Describe) or a targeted call answers without error. A targeted 30 means `NotVisibleToPrincipal`. D2 §5.4 records the topic analogue. |
| T15 | OffsetFetch for all partitions (a null topic list) silently drops the topics the caller may not Describe; the explicit form answers them with TOPIC_AUTHORIZATION_FAILED | the broker: `KafkaApis.scala:1075-1085`, `:1148-1152` (source, not run) | O1's explicit fetch shows the refusal; O2's all-partitions discovery and the engine's snapshot do not (§4.2) |
| T16 | A synchronous commit returns the LAST failing partition's code, even when other partitions of the request were applied | O1: `rd_kafka_commit` and `rdkafka_request.c:1733-1750` (source; found by PROD-04.0a) | a failed commit says `applied: Unknown` unless its code is one the broker returns for the whole request; read back before reporting it refused (§13) |
| T17 | librdkafka silently leaves a partition with a negative offset out of an OffsetCommit, so it reads as committed when nothing was sent | O1: `rdkafka_request.c:1847-1863` (source; found by PROD-04.0a) | refuse a negative offset before sending (§13) |
| T18 | rust-rdkafka's `TopicPartitionListElem::metadata()` panics on committed metadata that is not UTF-8 | O1: `rdkafka-0.36.2/src/topic_partition_list.rs:141-145` (source; found by PROD-04.0a) | withhold such metadata (`None`); an Apache Kafka broker always returns UTF-8 (§13) |

## 7. Input to OD-6: what PROD-04.0 needs from the `unsafe` policy

**OD-6 decides the policy once, for PROD-01.4 and this row.** It is the ledger's owner decision (`product-expansion.md`, Owner decisions, row OD-6) on how Logweir calls librdkafka functions the safe rdkafka API lacks. Its options are:

- **(a2)** one FFI crate for every such call, with `logweir-kafka` and every product crate keeping `forbid(unsafe_code)`;
- **(a1)** a private module in `logweir-kafka` (`deny` plus one `allow`);
- **(b)** wait for upstream rust-rdkafka;
- **(c)** the engine route;
- **(d)** a raw-protocol client;
- **(e)** no `unsafe`.

PROD-01.4 §6.4 proposed it and recommends (a2). This section adds this row's needs and measurements, and **takes no option**.

### 7.1 What this row needs, per operation

| Operation | librdkafka call outside the safe API | Needed by | Under (e), without it |
| --- | --- | --- | --- |
| List groups with their type | `rd_kafka_ListConsumerGroups` | 04.1, 04.2 | Classic groups only. KIP-848 groups read like share and streams groups (§3.1), so they are not captured. |
| Describe groups; the targeted visibility probe | `rd_kafka_DescribeConsumerGroups` | 04.1 (state, members), 04.2 (inactivity, target type), T14's explicit 30 | Membership of classic groups only. An undescribable id reads as a timeout (`NotVisibleOrUnreachable`, §5); PROD-04.0a's handle also reads the coordinator lookup's 30 off its queue, so it is named `NotVisibleToPrincipal` (§13). |
| Listing completeness; ACL guard | `rd_kafka_DescribeCluster` (authorized operations) | 04.1 (T14), 05.3 (T9) | completeness unknown |
| Describe ACLs | `rd_kafka_DescribeAcls` | 05.3 | no route: 05.3 Blocked |
| Fetch committed offsets | none: the safe consumer (O1) | 04.1 | unchanged |
| Commit offsets | none: the safe consumer (O1) | 04.2 | The commit is unchanged. The pre-apply type check is weaker (§4, #4). |
| Only when a row needs them | ListConsumerGroupOffsets (discovery, leader epochs, per-partition 88), AlterConsumerGroupOffsets, CreateAcls | no row today | — |

With PROD-01.4a's DescribeTopics, that makes **five calls needed now**, and three later.

### 7.2 The measured cost

This part is the same for any perimeter. The prototype's FFI leg (`A/admin-probe/src/main.rs`, `mod ffi`) is 381 lines:

- **117 lines of shared code:**
  - the integer-typed extern declarations (T12);
  - the string, error and partition-list readers;
  - one runner that creates options, queue and event, polls with a bound, and destroys each exactly once.
- **One block per call:**
  - ListConsumerGroups, 45 lines;
  - DescribeConsumerGroups, 42;
  - DescribeAcls, 35;
  - DescribeCluster, 30;
  - ListConsumerGroupOffsets, 35;
  - AlterConsumerGroupOffsets, 32;
  - CreateAcls, 45.

That is about 270 lines for this row's four calls and 381 for all seven. PROD-01.4's DescribeTopics adds a 56-line block (its §6.1). The shared code exists once per perimeter, whatever shape OD-6 picks.

**Obligations**, from both prototypes:

- Destroy each options object, queue, event and request object exactly once.
- Copy every string before its event is destroyed.
- Inputs carry no NUL.
- Every queue poll is bounded.
- Error codes and C enums are read as integers (T12).
- Unknown-clamped values are surfaced, never exported (T4, T10).
- Topic-ID text comes from the two halves (PROD-01.4 C4).
- Groups are de-duplicated by id (T6).

**Tests:**

- a soak test (thousands of calls with a bounded resident set);
- every answer compared with the Java CLIs on the 4.3 and 3.9 lines;
- one negative control per trap, for example: a share group comes back `other`, a USER-resource ACL `notRepresentable`, an error code outside the enum survives as its integer, and an undescribable id `NotVisibleToPrincipal`;
- a Tier A review with mutants.

### 7.3 How the needs sit with each OD-6 option

| OD-6 option | What PROD-04.1, 04.2 and 05.3 get | How the measured needs fit it |
| --- | --- | --- |
| (a2) one FFI crate | All four calls, behind one crate; `logweir-kafka` and every product crate carry `forbid`. | **Fits every call this row needs.** No FFI block in the prototype touches anything of `logweir-kafka`'s: each takes a native client handle and returns owned values, so it can sit behind a crate boundary with a safe API. The shared 117 lines and the eight wrappers (five now) are one perimeter with one gate. Cost: one more workspace package (PROD-01.4 §6.4); no new third-party crate. **Does not fit** share- and streams-group detail, for which librdkafka has no call at all (C12). It cannot remove T9 either, only guard it (§6). |
| (a1) a module of `logweir-kafka` | The same calls | Also fits, with no new package. But `logweir-kafka` drops to `deny` for as long as any wrapper lives there. This row's measured surface (~270 lines now, 381 for all seven) is about five to seven times DescribeTopics' block. PROD-01.4 §6.4 notes that (a1) "scales worse to PROD-04.0's larger call set". |
| (b) wait for upstream | Nothing until a rust-rdkafka release wraps the calls. PR #785 would wrap DescribeConsumerGroups and ListConsumerGroupOffsets; nothing wraps ListConsumerGroups, DescribeAcls or DescribeCluster, or is proposed (C11). | Even a released wrapper needs T9's guard, because the defect is in librdkafka itself. |
| (c) the engine route | Nothing usable: no type, state or ACL call, errors swallowed, reset gated upstream (E1–E6) | This row's recommendation against O5 feeds OD-3 as well (§0). |
| (d) a raw-protocol client | Every call, including share and streams detail, epochs and exact ACLs (no T9 or T10) | A second TLS/SASL client of about 5,000 lines (§2, O4), and a reversal of ADR 0004's central sentence |
| (e) no `unsafe` | Fetch and commit only (O1). Classic groups only. Undescribable ids as timeouts. 05.3 has no route. | — |

### 7.4 The fence, for whichever perimeter OD-6 names

1. **Add `#![forbid(unsafe_code)]` to the five crate roots that lack it** (L1): `crates/logweir/src/main.rs:1`, `crates/logweir-engine-oso/src/lib.rs:1`, `crates/logweir-store/src/lib.rs:1`, `xtask/src/main.rs:1` and `e2e/src/lib.rs:1`. This step is free, because no `unsafe` code exists (L1). The implementing child row takes it first.
2. **Draw the perimeter.**
   - Under (a2), the FFI crate's root does not carry `forbid`, and every other root does.
   - Under (a1), `logweir-kafka`'s root carries `#![deny(unsafe_code)]` with `#[allow(unsafe_code)]` on the one module, and every other root carries `forbid`.
3. **Add a gate to `just lint`**, for example `scripts/check-unsafe-scope.sh`. It checks that:
   - every crate root outside the perimeter carries `#![forbid(unsafe_code)]`;
   - the perimeter is exactly the one OD-6 named;
   - no code-shaped `unsafe` (`unsafe {`, `unsafe fn`, `unsafe impl`, `unsafe extern`) appears outside it.
   
   A bare-word scan would hit the API's "unsafe method" comments. Integration-test targets are crates of their own, which a root attribute does not reach. The code-shaped scan covers them, and so would a `[workspace.lints.rust] unsafe_code = "forbid"` table that every member but the perimeter inherits.
   
   Before step 1 lands, the gate's first run fails on the five roots, by design.
4. **Every `unsafe` block carries a `// SAFETY:` comment** naming its obligations.

### 7.5 Exit

This follows OD-6's recommendation text:

- **Per call:** a wrapper leaves the perimeter once a released rust-rdkafka offers it safely.
- **At the end:** when no wrapper is left, the perimeter goes. Under (a2) that is the crate; under (a1) it is the module, and `forbid` is restored.

## 8. Proposed ADR amendment text

Both texts are proposals. Neither lands in this row (rule 8).

- **ADR 0004's** lands with the first child row that ships a wrapper (PROD-01.4a or PROD-04.0b), once OD-6 has taken (a1) or (a2). It is written for either: the bracketed choices are filled from OD-6's answer.
- **Amendment D's** is an ADR 0008 amendment. It presumes that OD-3 takes native paths for offsets and ACLs, which OD-3's recommendation already names. It lands only after that owner decision, with the doc_lint update, in one commit.

### 8.1 ADR 0004

> **Amendment (PROD-01.4 and PROD-04.0; OD-6 [(a2) | (a1)]).** Logweir calls
> librdkafka functions the safe rdkafka API lacks, through `rdkafka::bindings`, in
> exactly one FFI perimeter: [(a2) the crate `<name>`, which alone does not
> carry `#![forbid(unsafe_code)]`, and which `logweir-kafka` depends on | (a1)
> one private module of `logweir-kafka`, compiled only with the `client`
> feature, whose crate root carries `#![deny(unsafe_code)]` with
> `#[allow(unsafe_code)]` on that module alone]. Every other crate root carries
> `#![forbid(unsafe_code)]`; `scripts/check-unsafe-scope.sh` enforces the
> perimeter and the attribute, and bars code-shaped `unsafe` outside it.
>
> The perimeter wraps DescribeTopics, ListConsumerGroups, DescribeConsumerGroups,
> DescribeCluster and DescribeAcls. It wraps ListConsumerGroupOffsets,
> AlterConsumerGroupOffsets and CreateAcls only once a row needs them. It reads
> error codes and C enums as integers, and reports every value librdkafka clamps
> to Unknown as not representable. Consumer positions are read and committed
> through the safe consumer API, from a consumer that never subscribes, which
> needs no exception.
>
> A wrapper leaves the perimeter once a released rust-rdkafka offers its call
> safely. When none is left, the perimeter is deleted [(a2) with its crate | (a1)
> and `forbid` restored].
>
> The rule against a custom protocol path for operations an existing client
> provides stands. Operations no linked client provides (share-group and
> streams-group description, share-group start offsets, DescribeProducers,
> ListTransactions) are unsupported until a row funds a protocol path through its
> own amendment.

### 8.2 ADR 0008 Amendment D

> **Amendment (PROD-04.0; after OD-3).** The engine's consumer-group subcommands
> of v0.21.0 are denied by name: `snapshot-groups`, `offset-reset`,
> `offset-reset-bulk`, `offset-rollback`, `three-phase-restore` and
> `show-offset-mapping`, beside the historical `offset`. Consumer positions and
> ACLs take Logweir-native paths (OD-3; ADR 0004). An engine consumer-group
> snapshot inside a foreign archive is read only as an import source (FX-1), with
> its group type unknown, and is never applied. The secondary scan of
> `scripts/check-no-oso.sh` names these tokens; the primary argv allowlist
> already refuses them.

## 9. Proposed child rows

- **PROD-04.0a — Positions through the safe consumer API** (impl, Tier A, no owner gate; lab: a PROD-01.5 slot, `stack-env.sh --kafka 4.3` and `--kafka 3.9`).
  - `logweir-kafka` gains `committed_positions(group, partitions)` and `commit_positions(group, positions)` on `RdKafkaReader`, behind a handle that cannot subscribe (§4.3).
  - It maps the codes of §4.3, and the timeouts of §4.2 and §5 (`PositionsUnstable` per group; `NotVisibleOrUnreachable`).
  - Mutants for the error mapping and for the leader epoch.
  - PROD-04.1 and 04.2 both consume it. It owes no FFI, so it proceeds under every OD-6 option.
  - **Landed:** §13.
- **PROD-04.0b — Group and ACL calls inside OD-6's perimeter** (impl, Tier A, gated on **OD-6 (a1) or (a2)**; lab: a slot).
  - ListConsumerGroups, DescribeConsumerGroups, DescribeCluster (authorized operations) and DescribeAcls.
  - They go in the FFI crate (a2) or module (a1) that the owner picks and PROD-01.4a creates. Whichever row lands first takes §7.4's steps: the five `forbid` roots, the perimeter and the gate. The other row extends the perimeter.
  - The traps' guards (T2–T4, T6, T9–T12, T14) and ADR 0004's amendment (§8.1) land in the same change.
- **PROD-04.0c — Amendment D names the engine's group subcommands** (docs and gate, Tier B; gate: OD-3, and owner sign-off on §8.2 under rule 8). The amendment text, `check-no-oso.sh`'s secondary token list and the doc_lint update go in one commit.
- **PROD-04.0d — Fixtures for groups and ACLs** (infra, Tier B). It extends PROD-01.5's profiles by `e2e/README.md`'s six "Extending the fixtures" steps, including a smoke function with a negative control in `compose/profile-smoke.sh` and a run of `cargo test -p e2e --test stack_params`. At `632ea345` no profile has an authorizer, the `streams` profile runs the classic protocol, and no profile sets share-state options.
  - **A new `acl` profile:**
    - §3's overlay: StandardAuthorizer, `super.users`, `allow.everyone.if.no.acl.found`;
    - a non-super SCRAM principal;
    - §3.9's visibility setup: a group whose only ACL names another principal, and a cluster ACL for another principal.
    
    Its smoke's negative control is the same principal granted Describe on the cluster (the listing is then unfiltered).
  - **A `groups` helper:** one group of each type on the 4.3 line and the classic groups on 3.9, with members that stop cleanly. Bracketed `pkill` patterns are needed: this row's first control step killed its own `sh -c`.
  - **A `group.protocol=streams` variant of the `streams` profile.** PROD-01.5's runs the classic protocol. The profile's owner is PROD-06.1, so this is a new service beside it or a request to its owner (README, "The owner changes a profile's services without asking").
  - **Share-state settings on single-broker 4.x stacks:** `share.coordinator.state.topic.replication.factor=1` and `…min.isr=1`. With the defaults (3 and 2), `__share_group_state` is never created (INVALID_REPLICATION_FACTOR on every attempt), so share groups there have members but no share-partition state (§3.8).
  - **As built (PROD-04.0d, 2026-10-08; `e2e/README.md`, "The groups fixture").**
    - The `acl` profile is FX-4's, extended rather than new. §3.9's visibility setup is opt-in, `e2e/compose/groups.sh visibility apply` and `remove`: its cluster ACL takes from `logweir` every cluster operation that FX-4's rows on the same profile expect it to have.
    - The `groups` helper is `e2e/compose/groups.sh`.
    - The streams-protocol variant is the profile `streams-protocol`. Its application is also the helper's streams group, `logweir-e2e-streams-protocol` (§10's annotation says why it is not `pa-streams`).
    - The share-state settings are on every single-node broker, whatever the line. **For PROD-05.3:** a 3.x broker does not use them, but its DescribeConfigs reports both keys as sensitive with a null value (measured on 3.7.1 and 3.9.2), the same "set, value withheld" form as AP-05.3-6's `super.users` and `allow.everyone.if.no.acl.found`, so a 3.x `acl` stack withholds four keys, not two.
- **Upstream reports** (no ledger row; any worker may file them):
  - librdkafka: DescribeAcls drops the response's top-level error (T9);
  - librdkafka: ListConsumerGroups drops non-consumer protocol types without telling the caller (T3);
  - rust-rdkafka: PR #785 lacks the typed list, alter and ACL wrappers;
  - rust-rdkafka: `DescribeConfigsFuture` ignores per-resource errors (T13).

## 10. Acceptance rows

Fixture names refer to §3's groups and to PROD-04.0d's profiles.

**Annotation (PROD-04.0d, 2026-10-08): §3's `pa-streams` is `logweir-e2e-streams-protocol` in the fixture.**
- It is the `streams-protocol` profile's application, WordCountProcessorDemo with `group.protocol=streams`.
- Why not WordCountDemo: it hard-codes its output topic, `streams-wordcount-output`, which PROD-06.1's `streams` profile writes on the same broker, so a second WordCountDemo would mix the two applications' counts. The processor demo writes its own topic.
- Why not the name `logweir-e2e-wordcount-streams`: its internal topics would match the `streams` smoke's `logweir-e2e-wordcount-` check.
- Its topology has no repartition topic, so §3.2's repartition position is not reproduced. AP-04.1-1 and -7 and AP-04.2-1 and -2 need only a Streams group with commits on its input topic.
- Every other group keeps §3's name.

- **"compose 4.3"** is a PROD-01.5 slot started with `eval "$(e2e/compose/stack-env.sh --slot <N> --kafka 4.3)"` (the 4.3.1 image pinned by digest). **"compose 3.9"** is the same with `--kafka 3.9`.
- Every negative control is a way the implementation could be wrong, and it must make the row fail.

These rows sit beside PROD-01.4's TI-04.1-1…4 and TI-04.2-1…3, and PROD-01.1's 04-1…04-5, and do not replace any of them:

- positions still bind to a topic generation;
- `PositionBeyondEnd` still excludes;
- translation never lands on a restored marker or aborted record (04-1);
- translation stays within one generation (04-2);
- the mapping report states the lineage depth it can prove (04-3);
- duplicate copies map to the first copy (04-4);
- time-based translation is refused for a `LogAppendTime` source (04-5).

### PROD-04.1

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| AP-04.1-1 | With one group of each kind selected (classic Empty, classic live, consumer Empty, consumer live, share idle, share live, streams) plus an absent id, the snapshot has exactly one entry per id. Classic and consumer groups are `captured`, with the type and state `kafka-groups.sh --list` shows. Share and streams groups are `excluded: GroupTypeNotCaptured` (`groupType: other`). The absent id is `excluded: GroupNotFound`. | Classifying from ListConsumerGroups alone omits the share and streams groups (4 of 7 listed, §3.1), failing "one entry per id". Classifying from DescribeConsumerGroups alone types them, and the absent id, as simple Classic Dead groups (§3.1), failing the type check. | compose 4.3 + PROD-04.0d `groups` |
| AP-04.1-2 | Every captured position equals the broker's (`kafka-consumer-groups.sh --describe --offsets`) on every recovery-point partition. A partition without a commit is `noCommittedPosition`. | Mapping `Offset::Invalid` (−1001) to 0 records position 0 where the CLI shows **no committed offset**. "No committed offset" in the CLI is **no row, or `-`**. For `pa-classic-empty`, `kafka-consumer-groups.sh --describe` printed rows only for its committed `pa-orders` partitions (partition 1 at 0) and none for its never-committed topics (`A/live-4.3.1/java-reference.txt:164-167`). A `-` is the CLI's mark for an assigned partition without a commit. Measured case: `pa-orders` 1 of a group that committed 0 there (CURRENT-OFFSET 0), against a partition that has no row. | compose 4.3 and 3.9 |
| AP-04.1-3 | While a transactional offset commit is pending for a selected group, the pre-transaction position is never recorded as captured. The unit is stated per route (§4.2). Under O1's single fetch, **every selected partition of the group** is `failed: PositionsUnstable`, because the bounded RequireStable call times out without naming a partition. With per-partition fetches, or O2's code 88, only the affected partitions are. | A fetch without RequireStable (the engine's OffsetFetch v5, or `isolation.level=read_uncommitted`) returns 3 while 7 is pending (§3.4) and records it; the row fails. | compose 4.3; §3.4's TxnOffsetCommit held open longer than the capture's bound |
| AP-04.1-4 | The listing never calls `GroupInfo::members()` on rdkafka 0.36.2. A workspace `clippy.toml` (none exists yet; the child row adds it) lists it under `disallowed-methods`, or the lock carries rdkafka ≥ 0.37. A capture on a cluster with an Empty group completes. | Two controls:<br>(1) With the entry present, a mutant that calls `GroupInfo::members()` fails `cargo clippy -D warnings` (`clippy::disallowed_methods`).<br>(2) A gate test reads `Cargo.lock` and `clippy.toml`, and fails when the entry is missing while rdkafka < 0.37 is locked. A mutant deleting the entry makes it red.<br>At run time the call aborts the debug build on an Empty group (§3.1, §3.6: exit 134 on both lines). | lint and the gate test; compose 3.9 (an Empty classic group) |
| AP-04.1-5 | A consumer group whose state librdkafka maps to Unknown is captured with `stateUnknownToClient` and marked active. | A classifier that reads Unknown as Empty marks it inactive; a unit test over the classifier with state code 0 catches it. | unit test over recorded listing rows |
| AP-04.1-6 | A selected group that the capture principal may not Describe, and that its filtered listing therefore omits, is `failed: NotVisibleToPrincipal`, never `GroupNotFound`. This holds whichever targeted call classifies it: DescribeConsumerGroups returning 30 (PROD-04.0b, OD-6 (a2)), or PROD-04.0a's fetch called with `GroupListing::NotListed`, whose bounded timeout carries the coordinator lookup's GROUP_AUTHORIZATION_FAILED from the handle's queue (§13). `NotVisibleOrUnreachable` is accepted only for a timeout that carries no such refusal. The other groups are captured, and the hidden group costs at most one position bound. | **Classifying from the listings alone reports it `GroupNotFound`**: measured, `pa-hidden` is missing from both the safe and the FFI listing for `User:logweir` (§3.9). The row fails. So does aborting the whole snapshot, or mapping the refusal to an empty result, which records it captured with no positions. Three more controls: (a) a reader that ignores the handle's queue labels the hidden group `NotVisibleOrUnreachable`, and the row fails (PROD-04.0a's mutant M26e); (b) a capture that passes `GroupListing::Listed` for an id its listing omitted labels it `NotAuthorized`, and the row fails; (c) in the same run, a describable absent id is `excluded: GroupNotFound` well inside the bound, and a listed group with a pending TxnOffsetCommit is `PositionsUnstable`, never `NotVisibleToPrincipal`. | PROD-04.0d `acl`'s visibility setup (§3.9): a principal without Describe on the cluster or on one group. Until 04.0d lands, the in-test setup of `e2e/tests/consumer_positions.rs` (`a_group_hidden_from_the_principal_is_never_absent`) |
| AP-04.1-7 | Positions imported from an engine consumer-group snapshot (FX-1) carry `groupType: unknown` and are never shown as a consumer group's. | The engine keeps a streams group's positions untyped (§3.7). Importing them as `groupType: consumer` fails the row. | an engine snapshot written by `snapshot-groups` against §3's fixture (FX-1's regression input) |

### PROD-04.2

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| AP-04.2-1 | Applying positions to a target group with a live member is refused `GroupActive` for the whole group, and the readback is unchanged. After the member stops, the same application succeeds. This holds for classic and consumer groups. | The measured control (§3.3): the same commit succeeds once the member stops, so a row that passes while the member lives has applied nothing. An implementation that commits with the member's own id would pass the broker check and fail the unchanged readback. | compose 4.3; PROD-04.0d `groups` with members that stop cleanly |
| AP-04.2-2 | A target group that exists but is not classic or consumer (share, streams, Connect) is refused `TargetGroupTypeUnsupported` before any commit. | An **Empty streams group accepts** a non-member commit (K3; §3.3's control). An implementation that relies on the broker's refusal alone overwrites the stopped application's positions, and the readback shows it. | compose 4.3; a stopped streams group |
| AP-04.2-3 | Prior target positions on the applied partitions are read and recorded before any commit. After a failure part-way through several groups, the audit lists applied, refused and untouched groups; a re-run applies only the untouched ones, under the same approval. | A mutant that skips the prior read leaves the audit's prior positions empty for the applied partitions, and the predicate ("recorded before any commit") fails. A re-run that repeats the refused groups fails the audit comparison. | compose 4.3; three groups, one of them live |
| AP-04.2-4 | Every applied position carries leader epoch −1, never a captured source epoch. | A mutant of the commit builder that copies the captured epoch fails a unit test over it. The safe route has no setter today; the row guards a future switch to O2. | unit test |
| AP-04.2-5 | A target consumer group in a state librdkafka maps to Unknown is refused `GroupActive` without a commit attempt. | Reading Unknown as inactive attempts the commit (visible in the audit); a unit test over the classifier catches it. | unit test |
| AP-04.2-6 | A target id that §5's rules classify `GroupNotFound` is created by the apply as a simple classic group, and the audit says `created`. | Reporting "applied to an existing group" contradicts the pre-apply classification, and the audit comparison fails. After the apply the id lists as classic, protocol type empty, Empty (§3.3). | compose 4.3 or 3.9 |
| AP-04.2-7 | A target id that the applying principal cannot see (a targeted call returns 30, §3.9, or PROD-04.0a's fetch carries the coordinator lookup's queued 30, §13) is refused `TargetNotVisible` before any commit, and the audit never says `created` for it. | Classifying the target from the listings alone takes the hidden group for absent, and records `created` for a group that exists. The row fails. | PROD-04.0d `acl`'s visibility setup, on the target |

### PROD-05.3

These rows answer D2 §5.4's objections (§4.4).

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| AP-05.3-1 | With no authorizer, coverage is `aclsNotApplicable: authorizerDisabled`, decided from the broker's `authorizer.class.name` being present and empty (safe DescribeConfigs), never from DescribeAcls. A missing key (a refused DescribeConfigs, T13) gives `unverified`. | Trusting DescribeAcls' "0 bindings, no error" (§3.5, §3.8) reports `captured, 0 bindings`. Reading a missing key as "disabled" misreports the denied principal of §3.8. Either fails the row. | compose 4.3, the default stack; `acl` with the principal denied |
| AP-05.3-2 | A principal without Describe on the cluster gets `captureDenied`, decided from DescribeCluster's authorized operations (Describe absent). After Describe is granted, the capture returns every binding. | Trusting DescribeAcls' "0 bindings, no error" for `User:logweir` (§3.5) reports `captured, 0 bindings`; the row fails. | PROD-04.0d `acl` with its SCRAM principal |
| AP-05.3-3 | Literal, prefixed, wildcard-name and wildcard-principal bindings, ALLOW and DENY, on TOPIC, GROUP, TRANSACTIONAL_ID and CLUSTER resources round-trip exactly against `kafka-acls.sh --list`, with resource type 4 named CLUSTER. | Dropping the pattern type turns the prefixed `pa-` binding into a literal one; naming resource type 4 `BROKER`. Either fails the round-trip. | `acl`, with §3.5's six bindings and one cluster binding |
| AP-05.3-4 | Bindings librdkafka cannot represent (USER resources with CreateTokens or DescribeTokens, DELEGATION_TOKEN resources, TwoPhaseCommit) are listed as `notRepresentable`, with principal and resource name, counted in coverage and never exported. | Exporting what librdkafka returns yields two identical `Unknown / User:bob / Unknown` bindings where the broker holds two distinct ones (§3.5); the round-trip against `kafka-acls.sh --list` fails. | `acl`, with §3.5's four Java-created bindings |
| AP-05.3-5 | With `authorizer.class.name` outside the allowlist of §4.4 item 3, coverage is `unverified: authorizerNotAclBased`, whatever DescribeAcls answers. | A non-ACL authorizer class, Describe allowed on the cluster, and 0 bindings must not report `captured`. A coverage function that checks only for a non-empty class reports `captured, 0 bindings` and fails. | unit test over the pure coverage function with recorded inputs (D2 §5.4's `check_contract::visibility` pattern); no fixture ships a non-ACL authorizer |
| AP-05.3-6 | When `super.users` or `allow.everyone.if.no.acl.found` is reported set but its value is withheld (sensitive, §3.9), the export records the key as `set, value unreadable` and labels the binding set `semanticsUnverified`. An administrator attestation, if given, is recorded as attested. | Reading the withheld value (null) as "not set" or as `false` exports the §3.9 fixture's bindings with no warning, although `allow.everyone.if.no.acl.found=true` opens every resource that has no binding; the row fails. | `acl` (its overlay sets both keys), read as a principal allowed DescribeConfigs |

## 11. Limits of this record

- **One broker.** Coordinator routing, and T6's duplicate groups across brokers, were not exercised. PROD-01.5's `cluster3` profile can.
- **Unmeasured traps.** No consumer group was caught in `Assigning` or `Reconciling` (T4 rests on source: C6). A broker error code outside rdkafka-sys's enum (T12) was not provoked.
- **Nothing on the consumer side after a commit.** Committing a source leader epoch to a target was not run. §4.3 avoids the question by committing −1, the only value the safe API has.
- **The raw leg is a prototype over PLAINTEXT only.** Its cost figure (§2, O4) is the engine's code, not a Logweir estimate. StreamsGroupDescribe was not sent: P has no codec for it.
- **Only local fixtures.** Managed providers (MSK, Confluent Cloud) were not tried; their group and ACL APIs follow OD-4.
- **Nothing ships.** The prototypes are throwaway, under `A/`. The measured values this record relies on are in its tables.
- **The engine's snapshot was not run.** §3.7 infers it from source and from the probe replaying its two requests. AP-04.1-7's fixture runs `snapshot-groups`.
- **Commits by an undescribable principal were not run.** §4.3's timeout for a principal that cannot Describe the target group is inferred from §3.9's fetch. *Measured since by PROD-04.0a (§13).*
- **A class sweep this row does not own (T13): FX-4's.**
  - `L/crates/logweir-kafka/src/rdkafka_reader.rs:293-325` (`topic_configs`) and `:343-386` (`broker_configs`) map a per-resource `Err` that rdkafka 0.36.2 never produces. A refused or unknown resource therefore reads as a configuration with no entries, rather than `NotAuthorized` or `TopicNotFound`.
  - The broker-resource case is measured (§3.8, §3.9); the topic case is read from source.
  - **The orchestrator has sent these five shipped consumers to FX-4** (ledger row FX-4, In progress):
    - `L/crates/logweir/src/drill/phase7_verify.rs:1209`: the restore target's parity reads "no overrides".
    - `L/crates/logweir/src/check/kinds/restore.rs:863`: its `Err` arm gives `Unknown`/`BrokerConfigsNotReadable` (`:864-874`), but a refused read arrives as an empty map. It falls through to `ready(TimestampWithinBound)`, "the target declares no record-timestamp bound" (`:883-888`): a false "ready".
    - `L/crates/logweir/src/drill/phase0_admit.rs:618`: the Guard G-TS preflight defaults a missing `log.message.timestamp.type` to CreateTime (`:620-628`), so its LogAppendTime arm never runs.
    - `L/crates/logweir/src/drill/phase0_admit.rs:715`: the probe topic's configuration readback gets an empty map, not an error.
    - `L/crates/logweir/src/drill/phase2_target.rs:59`: the target snapshot records empty configurations.
  - PROD-05.1 inherits the rule once FX-4 lands it.

## 12. Reproduction and artifacts

All paths are under `A/`, so `/tmp/logweir-roadmap-run/claude/artifacts/prod-04-0/`.

- **Probe.** `admin-probe/`, built with `cargo +1.89.0 build` into a temporary target directory (`admin-probe-build.log`; deleted afterwards).
  - Its `Cargo.toml` pins rdkafka `=0.36.2` with `logweir-kafka`'s feature list, and kafka-protocol `=0.18.0` with `client` only.
  - Its lockfile is the worktree's at `ac6aa00e`, plus kafka-protocol.
  - One leg, for example: `admin-probe ffi-list localhost:9092 all`, `admin-probe raw-describe localhost:9092 6 <group>…`, `admin-probe safe-commit localhost:9092 <group> <topic> <partition> <offset>`.
  - `PROBE_SASL_USER` and `PROBE_SASL_PASS` switch it to SCRAM on `localhost:9097`.
- **Drivers.**
  - `run-all.sh` runs one compose-lock hold, 06:36:10–06:48:42 UTC: 4.3.1 `groups`, 4.3.1 `acls`, `down -v`, 3.9.2 `legacy`, `down -v`. It calls `live-session.sh`.
  - `live-supplement.sh`, `live-aclguard.sh` (and its rerun) and `live-visibility.sh` each take and release the lock themselves.
  - The overlays are `acl-overlay.yml` and `share-state-overlay.yml`.
- **Outputs.** In `live-4.3.1/`, `live-4.3.1-acls/`, `live-3.9.2/`, `live-4.3.1-supplement/`, `live-4.3.1-aclguard/`, `live-4.3.1-aclguard-rerun/`, `live-4.3.1-visibility/` and `live-4.3.1-visibility-rerun/`:
  - `evidence.jsonl`: one JSON line per probe result;
  - `session.log`: probe stderr and exit codes;
  - `java-reference.txt`: the Kafka CLIs' answers.
- **Sources fetched** on 2026-09-29:
  - Kafka tag `4.3.1` (commit `26b251a4…`): the group-coordinator files and `core/…/AuthHelper.scala`, into `kafka-src/`;
  - librdkafka `v2.15.1`: `rdkafka.h`, `rdkafka_admin.c`, `CHANGELOG.md`;
  - rust-rdkafka 0.37–0.39, read from PROD-01.4's `artifacts/prod-01-4/upstream/`;
  - the GitHub API for rust-rdkafka PR #721, #785 and #838, commit `9be7eca2`, and librdkafka's releases.

- **Compose-lock holds**, all by `prod-04-0` and each released after `down -v`:
  - 06:36:10–06:48:42 (`run-all.sh`);
  - 06:50–06:58:44 (`live-supplement.sh`);
  - 06:58:59–07:00:29 (`live-aclguard.sh`, whose guard probes were invalid);
  - 07:01:10–07:02:38 (`live-aclguard-rerun.sh`);
  - fix round, on slot 0 (the brief names no slot), `stack-env.sh --slot 0 --kafka 4.3`: 07:35:01–07:36:25 (`live-visibility.sh`, void listings) and 07:37:36–07:38:33 (its rerun).
- **Fix-round sources**, fetched on 2026-09-29 from Kafka tag `4.3.1`, into `kafka-src/`:
  - `core/…/KafkaApis.scala` (sha256 `f93449b5…`);
  - `ConfigHelper.scala` (`58c22ccf…`);
  - `KafkaConfig.scala` (`1fdcec9c…`).

## 13. Landed: PROD-04.0a

Branch `claude/prod-04-0a`. `logweir-kafka` keeps `#![forbid(unsafe_code)]` and gains no dependency.

**The API.**
- `RdKafkaReader::committed_positions(group, partitions, listing)` and `RdKafkaReader::commit_positions(group, positions)`, with `with_position_bound` (default 15 s, 2–300 s).
- The contract and the code mapping are pure, in `crates/logweir-kafka/src/positions.rs`.
- The handle is `crates/logweir-kafka/src/rdkafka_positions.rs`. It is crate-private and opened per call, so no caller can subscribe, assign or poll it.
- Its configuration pins:
  - `group.protocol=classic`, because librdkafka says its default will change;
  - `isolation.level=read_committed` (RequireStable);
  - auto-commit and auto-store off;
  - `session.timeout.ms` and `socket.timeout.ms` at the bound.
- `listing` is the caller's `Listed` or `NotListed`, the one fact a safe fetch cannot supply. A timeout is `PositionsUnstable` for a listed group and `NotVisibleOrUnreachable` for an unlisted one (§4.2, §5).
- Reads carry `leader_epoch: None` (not exposed, C3). Commits carry −1 and the metadata marker `logweir:positions`.

**Measured** on a PROD-01.5 slot (slot 2, `--kafka 4.3` and `--kafka 3.9`, `--profiles acl`), by `e2e/tests/consumer_positions.rs`. Every row is checked against `kafka-consumer-groups.sh` inside the broker container. The two lines agree, except that the KIP-848 row needs 4.x.

| Row | Observed |
| --- | --- |
| classic, Empty | positions equal the CLI's; a committed 0 reads 0; a never-committed partition is `NoCommittedPosition` where the CLI prints no row; the non-member commit lands with the marker; the group stays Empty |
| classic and KIP-848, one live member | `GroupActive` in ~165 ms, readback unchanged; once the member leaves, the same commit applies |
| absent id | every partition `NoCommittedPosition` in ~165 ms; the read does not create the group; a commit creates it, Empty (K3) |
| pending TxnOffsetCommit | `PositionsUnstable` (listed) and `NotVisibleOrUnreachable` (unlisted) after the 6 s bound, while a `read_uncommitted` reader returns the stale 3; after the abort, 3 |
| group whose only ACL names another principal (`User:logweir`) | `NotVisibleToPrincipal` (unlisted) and `NotAuthorized` (listed); the commit is `NotAuthorized` after the bound and nothing changes; granting Read and Describe makes the same commit apply. A topic it may not Describe is `TopicNotAuthorized` for that partition (T15) |

**What this refines in the record.**
- **The safe route names the refusal.** When FindCoordinator answers GROUP_AUTHORIZATION_FAILED, librdkafka posts that error once on the consumer's own queue and keeps retrying (`rdkafka_cgrp.c:797-807` in rdkafka-sys 4.10.0+2.12.1). The fetch or commit still waits out its bound. The handle then reads the queue, which only errors can reach because it never subscribes, and counts GROUP_AUTHORIZATION_FAILED alone, never a transport, authentication or TLS error. §3.9 and §5 said the safe route could not separate a hidden group from an unreachable one. It can, when the broker refused the lookup. Three limits:
  - The refusal says "this principal may not Describe this id", not "the group exists". With Kafka's default `allow.everyone.if.no.acl.found=false`, an ABSENT id without an ACL is refused the same way. That is why the label is `NotVisibleToPrincipal` ("may exist"), never `GroupNotFound`.
  - The refusal is read only after the full bound, so a hidden group still costs one bound per call.
  - A lookup answered after the bound (a slow SASL or TLS handshake) falls back to `NotVisibleOrUnreachable`.
- **Three further traps, guarded** (also rows T16–T18 of §6):
  - **T16.** A synchronous commit returns the LAST failing partition's code even when other partitions were applied (`rdkafka_request.c:1733-1750`). So a failed commit says `applied: Unknown` unless its code is one the broker returns for the whole request.
  - **T17.** librdkafka silently leaves out a partition whose offset is negative (`rdkafka_request.c:1847-1863`). Such a request is refused before sending.
  - **T18.** rust-rdkafka's `TopicPartitionListElem::metadata()` panics on bytes that are not UTF-8. The reader withholds such metadata (`None`). An Apache Kafka broker always returns UTF-8.

**Bounds are guarded** (fix round, 2026-10-08). Broker-free unit rows time an unanswered fetch and commit against a 2 s bound: measured 2000.9–2003.3 ms and 2001.8–2007.1 ms on the development host, with margins of 1.5 s and 2.5 s. The e2e rows require every timed-out call to return within its 6 s bound plus 3 s (measured: at most 134 ms over). A fetch that waits longer than its bound fails both.

**AP-04.1-6 and AP-04.2-7 were reworded in the fix round** (review L1). The first landed wording (`e63b6d6d`) named two labels for ONE case: `NotVisibleOrUnreachable` "under OD-6 (e)" and `NotVisibleToPrincipal` "through PROD-04.0a's API". Under (e) the targeted fetch IS PROD-04.0a's API, so a PROD-04.1 returning either label would have passed. The rows now name one label per case: `NotVisibleToPrincipal` whenever a targeted call carries the refusal, and `NotVisibleOrUnreachable` only for a timeout without one.

**Not run here:**
- a share group's 69 (`NotAConsumerGroup`): unit only, for lack of PROD-04.0d's `groups` fixture;
- a per-partition 88 after librdkafka's retries: unit only;
- coordinator outages.

The ACL setup is built inside the row itself, on FX-4's `acl` profile, for PROD-04.0d to absorb.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
