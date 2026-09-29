# PROD-04.0 — The Kafka administrative path

Status: proposed for review. Research row, review tier B. One owner-level choice
(§7, **AP-OC1**) is presented with options and a recommendation, and is not taken
here. It merges PROD-01.4's TI-OC1 into a single scoped-`unsafe` policy for
`logweir-kafka`. Nothing in `crates/`, the ADRs or `docs/stability.md` changes in
this row: the amendment texts in §8 land with the implementing rows, after the
owner decision (rule 8).

## 0. Decisions in one page

1. **Per operation** (§4). O1 is the safe consumer API, O2 FFI under a scoped `unsafe` exception, O4 a raw-protocol client, O5 the engine.

   | Operation | Route |
   | --- | --- |
   | List groups with their type | O2 ListConsumerGroups, joined with a name listing |
   | Describe groups | O2 DescribeConsumerGroups, only for ids already classified |
   | Fetch committed offsets | O1, with RequireStable |
   | Commit offsets | O1 from a non-member; the broker refuses it while the group has members |
   | Describe ACLs | O2 DescribeAcls, guarded by two positive probes |
   | Create ACLs | none in product; no row applies ACLs |
   | DescribeProducers, ListTransactions | unsupported; only O4 reaches them |

   **O5 is rejected for every operation** (§2, E1–E6). O4 is deferred until a row needs share- or streams-group detail.

2. **Group types** (§5).
   - Classic and consumer groups are captured.
   - Share groups, streams groups and classic groups of other protocols are `excluded: GroupTypeNotCaptured` (`groupType: other`).
   - An absent id is `excluded: GroupNotFound`.
   
   No group is dropped, and absence never means offset 0.

3. **The safe route is not trap-free** (§3, §6).
   - rdkafka 0.36.2's `fetch_group_list` aborts (undefined behaviour) on any member-less group. rust-rdkafka 0.37.0 fixed it.
   - The classic describe it wraps reports every non-classic group, and every absent id, as `Dead`.
   - librdkafka's DescribeAcls, in every release through 2.15.1, reports both "no authorizer" and "not authorised" as "0 bindings".

4. **One owner choice, AP-OC1** (§7), merged with PROD-01.4's TI-OC1: one scoped-`unsafe` module in `logweir-kafka`. The recommendation is to take it now, with upstream wrappers as the exit. Fetch and commit need no exception either way.

5. **ADR text** (§8): an ADR 0004 amendment for the module, and an Amendment D amendment naming the engine's v0.21.0 group subcommands. Neither lands in this row.

6. **Child rows** (§9):
   - 04.0a: positions through the safe API;
   - 04.0b: the FFI calls, gated on AP-OC1(a);
   - 04.0c: Amendment D, owner sign-off;
   - 04.0d: fixtures (`acl`, `groups`, share-group state).

   **Acceptance rows** for PROD-04.1, 04.2 and 05.3 are in §10. PROD-00.1's A-C8-3 is answered in §3.7.

## 1. What exists today

Citation prefixes, used throughout:

- `L/`: Logweir at main `ac6aa00e`.
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
| L1 | `logweir-kafka` is the only broker-dialling crate and forbids `unsafe`. Every other crate root also carries `#![forbid(unsafe_code)]`, and no script checks the attribute. | `L/crates/logweir-kafka/src/lib.rs:3`; the same line in each `crates/*/src/{lib,main}.rs`; no hit for `unsafe_code` under `L/scripts/` |
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

| Route | What it reaches | Cost | Constraint |
| --- | --- | --- | --- |
| **O1** the safe consumer API (rdkafka 0.36.2, no `unsafe`) | OffsetFetch and OffsetCommit for one group, from a consumer that carries that `group.id` and never subscribes (`committed_offsets`, `commit`). ListGroups v0 plus DescribeGroups v0 through `fetch_group_list`. | Tens of lines behind the existing `client` feature. No new dependency and no ADR change. | No group type, epoch, leader epoch or ACL (C1, C3). `fetch_group_list` is unsound on 0.36.2 and misreports non-classic groups (§3.1). The handle carrying the target `group.id` must be unable to subscribe (§4.3). |
| **O2** FFI with a scoped `unsafe` exception | Everything in C5: a typed listing of classic and consumer groups, describe with members and assignments, an all-partition offset listing with leader epochs and an explicit UNSTABLE code, alter offsets, and Describe/CreateAcls. | Measured prototype (`A/admin-probe/src/main.rs`, FFI leg of 351 lines): about 120 lines of shared helpers (integer-typed error readers, string copy, an options/queue/event runner that destroys everything once), then 32–45 lines per call. No new crate: `rdkafka::bindings` is `rdkafka_sys::bindings`, and librdkafka is already linked and attributed. | The traps of §6. Nothing at all for share or streams groups (C12). |
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
| streams (`pa-streams`) | `UnknownMemberId` | ListGroups v5: streams, Empty (FFI describe: "simple, Classic, Dead"). Commit **ok**. | `streams-plaintext-input` 0 → 1 (was 3) |

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

**Both "0 bindings, no error" rows have one cause.** `rd_kafka_DescribeAclsResponse_parse` reads the response's top-level ErrorCode into a local, then returns NO_ERROR with the (empty) binding list whatever the code was (`S/…/rdkafka_admin.c:5553-5560`, `:5658`). The same function is unchanged in v2.15.1 (`A/librdkafka-v2.15.1-rdkafka_admin.c`, sha256 `8ed0e410…`). A GitHub search on 2026-09-29 found no upstream issue for it.

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
- **What its snapshot keeps**, since it iterates the listing and fetches offsets (E3):
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

## 4. Decision per operation

| # | Operation | Recommended route | Why not the others | Cost | Without AP-OC1(a) |
| --- | --- | --- | --- | --- | --- |
| 1 | List groups with their type | **O2** `ListConsumerGroups` for the typed list of classic and consumer groups, joined by group id with a complete **name** listing (ListGroups) | O1's listing aborts on 0.36.2 and has no type. O2 alone drops share and streams groups silently (C7). O4 would give every type but brings a second client. O5 has no state or type. | ~45 lines in the FFI module, plus the name listing: the safe `fetch_group_list` with `members()` never called (§4.1), or `rd_kafka_list_groups` in the FFI module | Classic groups only, with a guard against `members()`. Consumer (KIP-848) groups cannot be told from streams groups, so they are not captured (§7). |
| 2 | Describe groups | **O2** `DescribeConsumerGroups`, only for ids the typed list classifies as classic or consumer, and only when its type agrees with the list | O1: unsound and wrong. O2 on any other id returns "simple Classic Dead" with no error (§3.1). O4 would add epochs and share and streams detail that no row needs. | ~42 lines | Membership of classic groups only; consumer groups undescribed |
| 3 | Fetch committed offsets | **O1** `committed_offsets` from a non-subscribing consumer, over the recovery point's partitions, `read_committed` (RequireStable) | O2 adds discovery of partitions outside the recovery point, leader epochs and an explicit code 88; useful, not required. O5 returns stale positions (§3.4). | ~30 lines, no `unsafe` | Unchanged |
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
- **Pending transactions.** A RequireStable fetch that does not finish inside its bound is `failed: PositionsUnstable`. The safe route cannot tell that from an unreachable coordinator, whose error text is the same timeout, so the label says "pending transactional offsets or coordinator unavailable". O2's code 88 would separate the two.

### 4.3 Commit

- **The handle.** A `BaseConsumer` built with the target `group.id` and `enable.auto.commit=false`. It is wrapped in a type that exposes only `committed_positions` and `commit_positions`, so no caller can `subscribe` or `assign` with it and join the application's group.
- **Epoch and metadata.** Commits carry leader epoch −1, the safe API's only value (C3). A source leader epoch means nothing on a target cluster. The metadata string is Logweir's own marker.
- **Error mapping.**
  - UNKNOWN_MEMBER_ID (25) → `GroupActive`;
  - GROUP_ID_NOT_FOUND (69) → `NotAConsumerGroup`;
  - GROUP_AUTHORIZATION_FAILED → `NotAuthorized`;
  - anything else → `failed` with its integer code.
- **An absent target group** is created as a simple classic group by the commit (§3.3), and the audit says so.

## 5. Group types: what is captured, and the labels

Every selected group gets exactly one of these outcomes in PROD-04.1's snapshot. Nothing is dropped, and absence never means offset 0.

| Classification (§4 #1) | Outcome | Label and reason | Notes |
| --- | --- | --- | --- |
| Typed list: Classic, with protocol type `consumer` or empty (a simple group) | `captured` | `groupType: classic` | positions per partition, state, member count |
| Typed list: Consumer | `captured` | `groupType: consumer` | a state librdkafka maps to Unknown (`Assigning`, `Reconciling`: C6) is recorded as `stateUnknownToClient` and treated as **active** by 04.2 |
| In the name listing, not in the typed list | `excluded` | `GroupTypeNotCaptured`, `groupType: other` ("a share group, a streams group, or a classic group of a non-consumer protocol such as Kafka Connect") | Under O1 and O2 no call names which. With O4 the label carries `share` or `streams`, and a share group can become `captured: startOffsetOnly` (DescribeShareGroupOffsets). |
| In neither listing | `excluded` | `GroupNotFound` | an FFI describe of it would still answer "simple Classic Dead" (§3.1), so the listing decides |
| Imported from an engine snapshot (FX-1) | as found in the archive | `groupType: unknown` | the engine records no type (§3.7); 04.2 applies such positions only after its own target-side classification |
| Listing or fetch refused or failing | `failed` | `NotAuthorized`, `PositionsUnstable`, `Unreachable` | per group; other groups continue |

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
| T9 | DescribeAcls drops the response's top-level error: "authorizer disabled" and "not authorised" both read as "0 bindings" | O2, every librdkafka through 2.15.1 (§3.5) | trust "0 bindings" only after two positive probes: the broker's `authorizer.class.name` through the safe DescribeConfigs (empty: `authorizerDisabled`), and DescribeCluster's authorized operations for the capturing principal (no Describe: `captureDenied`); a refused probe gives `unverified` (AP-05.3-1, -2). Report the defect upstream (O3). |
| T10 | ACL resource types above TransactionalId (DelegationToken, User) and operations above IdempotentWrite (CreateTokens, DescribeTokens, TwoPhaseCommit) become Unknown; distinct bindings collapse into identical ones | O2 (C9, §3.5) | count them as `notRepresentable` with principal and name; never export an Unknown field |
| T11 | Kafka's CLUSTER resource is librdkafka's `BROKER` (4) | O2 (§3.5) | map by number, name it `CLUSTER` in the model |
| T12 | rdkafka-sys types broker error codes as a Rust enum that stops at 118, 129 and 130 | O2 and parts of O1 (C10) | read codes as C integers through module-local declarations (the prototype does) |
| T13 | rust-rdkafka's `DescribeConfigsFuture` never reads `rd_kafka_ConfigResource_error`: a refused or unknown resource comes back as `Ok` with no entries. Unfixed through 0.39.0. | O1: `R/src/admin.rs:1121-1159` (no call in 0.37–0.39 either); measured for a denied broker resource (§3.8) | T9's first probe reads a **missing** `authorizer.class.name` as `unverified`, and only a present, empty value as disabled. The same defect reaches `logweir-kafka`'s existing `topic_configs` and `broker_configs`: see the class sweep in §11. |

## 7. Owner choice AP-OC1: one scoped-`unsafe` policy for `logweir-kafka`

**The question, once for both rows:** may `logweir-kafka` call librdkafka's C API through `rdkafka::bindings` in one fenced module? PROD-01.4 asks it as TI-OC1 (DescribeTopics). This row asks it for the group and ACL calls. The answer should be one policy, one module and one amendment.

| Option | PROD-01.4 (topic IDs) | PROD-04.1 / 04.2 (groups) | PROD-05.3 (ACLs) | Cost and risk |
| --- | --- | --- | --- | --- |
| **(a) One scoped exception now (recommended)** | DescribeTopics | typed listing and describe (O2); fetch and commit stay O1 | DescribeAcls with the T9 and T10 guards | One reviewed module and one gate, no new crate. `forbid` becomes `deny` at the crate root until (c) ships. |
| (b) Safe API only | heuristic only (TI-OC1 d) | classic groups only; KIP-848 consumer groups `excluded: GroupTypeNotCaptured`; fetch and commit as recommended | **no route**; 05.3 Blocked | No `unsafe`; the listing still needs §4.1's fence |
| (c) Upstream first (rust-rdkafka PR #721, #785 and the missing wrappers; librdkafka reports for T3 and T9) | when released | when released | when released, still with T9 unless librdkafka fixes it | Unknown date; an rdkafka bump (0.38 is breaking); (b)'s limits meanwhile |
| (d) A raw-protocol client (O4) | Metadata v10+ | every type, epochs, share start offsets | exact ACLs, no T9 or T10 | A second TLS/SASL client of about 5,000 lines; reverses ADR 0004's central sentence for these operations |

**Scope of (a).**

- One private module, for example `crates/logweir-kafka/src/native.rs`, compiled only with the `client` feature, so the pure-layer build (`--no-default-features`) is unaffected.
- Safe methods on `RdKafkaReader`, and trait methods whose defaults return "not supported" for test fakes.
- The calls:
  - DescribeTopics (01.4);
  - ListConsumerGroups, DescribeConsumerGroups, DescribeAcls and DescribeCluster with authorized operations (this row; the last is T9's guard);
  - ListConsumerGroupOffsets, AlterConsumerGroupOffsets and CreateAcls only when a later row needs them.

**The unsafe surface, measured.**

- 01.4's prototype: one block of about 65 lines.
- This row's: 117 lines of shared code (the integer-typed extern declarations, the string, error and partition-list readers, and one runner that destroys everything once), then one block per call:
  - ListConsumerGroups, 45 lines;
  - DescribeConsumerGroups, 42;
  - DescribeAcls, 35;
  - DescribeCluster, 30;
  - ListConsumerGroupOffsets, 35;
  - AlterConsumerGroupOffsets, 32;
  - CreateAcls, 45.

  That is about 270 lines for the four this row recommends (the first four) and 381 for all seven.

**The fence.**

1. The crate root becomes `#![deny(unsafe_code)]`. `#![forbid]` cannot be relaxed inside a crate, and a module-level `#[allow(unsafe_code)]` applies to that one module only.
2. A new gate joins `just lint` (for example `scripts/check-unsafe-scope.sh`). It checks that:
   - every other crate root keeps `#![forbid(unsafe_code)]`;
   - `logweir-kafka` has exactly one `allow(unsafe_code)`, on that module;
   - no `unsafe` token appears elsewhere in the workspace's Rust sources.
   
   Today no script checks the attribute at all (L1).
3. Every `unsafe` block carries a `// SAFETY:` comment naming its obligations.

**The obligations, from both prototypes.**

- Destroy each options object, queue, event and request object exactly once.
- Copy every string before its event is destroyed.
- Inputs carry no NUL.
- Every queue poll is bounded.
- Error codes and C enums are read as integers (T12).
- Unknown-clamped values are surfaced, never exported (T4, T10).
- Topic-ID text comes from the two halves (01.4 C4).
- Groups are de-duplicated by id (T6).

**Tests.**

- A soak test: thousands of calls with a bounded resident set.
- Every FFI answer compared with the Java CLIs on 4.3.1 and on 3.9.2.
- One negative control per trap. For example: a share group must come back `other`; a USER-resource ACL must come back `notRepresentable`; an error code outside the enum must survive as its integer.
- A Tier A review with mutants.

**Exit.** Delete the module and restore `forbid` once a released rust-rdkafka wraps every call it makes (c).

**Recommendation:** take (a), with (c) as its exit. Keep O1 for fetch and commit whatever is chosen: they need no exception. Fund (d) only if a row needs share-group start offsets or streams-group description; none does today.

## 8. Proposed ADR amendment text

Both texts are proposals. Neither lands in this row (rule 8). ADR 0004's lands with the child row that ships the module, after AP-OC1(a). Amendment D's is an ADR 0008 amendment, which lands only after its owner decision, with the doc_lint update, in one commit.

### 8.1 ADR 0004

> **Amendment (PROD-01.4 and PROD-04.0).** `logweir-kafka` may call librdkafka's C
> API through `rdkafka::bindings` in exactly one private module, compiled only with
> the `client` feature. The crate root is `#![deny(unsafe_code)]`, that module
> alone carries `#[allow(unsafe_code)]`, every other crate keeps
> `#![forbid(unsafe_code)]`, and `scripts/check-unsafe-scope.sh` enforces all
> three. The module wraps DescribeTopics, ListConsumerGroups,
> DescribeConsumerGroups, DescribeAcls and DescribeCluster, and ListConsumerGroupOffsets,
> AlterConsumerGroupOffsets and CreateAcls only once a row needs them. It reads
> error codes and C enums as integers and reports every value librdkafka clamps
> to Unknown as not representable. Consumer positions are read and committed
> through the safe consumer API, from a consumer that never subscribes, which
> needs no exception. The exception ends when a released rust-rdkafka wraps
> every call the module makes: the module is then deleted and `forbid` restored.
>
> The rule against a custom protocol path for operations an existing client
> provides stands. Operations no linked client provides (share-group and
> streams-group description, share-group start offsets, DescribeProducers,
> ListTransactions) are unsupported until a row funds a protocol path through its
> own amendment.

### 8.2 ADR 0008 Amendment D

> **Amendment (PROD-04.0).** The engine's consumer-group subcommands of v0.21.0
> are denied by name: `snapshot-groups`, `offset-reset`, `offset-reset-bulk`,
> `offset-rollback`, `three-phase-restore` and `show-offset-mapping`, beside the
> historical `offset`. Consumer positions and ACLs are Logweir-native (ADR 0004).
> An engine consumer-group snapshot inside a foreign archive is read only as an
> import source (FX-1), with its group type unknown, and is never applied. The
> secondary scan of `scripts/check-no-oso.sh` names these tokens; the primary
> argv allowlist already refuses them.

## 9. Proposed child rows

- **PROD-04.0a — Positions through the safe consumer API** (impl, Tier A, no owner gate; lab compose).
  - `logweir-kafka` gains `committed_positions(group, partitions)` and `commit_positions(group, positions)` on `RdKafkaReader`, behind a handle that cannot subscribe (§4.3). It maps the codes of §4.3 and the timeout of §4.2.
  - Mutants for the error mapping and for the leader epoch.
  - PROD-04.1 and 04.2 both consume it. This row owes no FFI.
- **PROD-04.0b — Group and ACL calls in the scoped FFI module** (impl, Tier A, gated on **AP-OC1(a)**; lab compose).
  - ListConsumerGroups, DescribeConsumerGroups, DescribeAcls and DescribeCluster (authorized operations), in the module PROD-01.4a creates. Whichever row lands first creates it and the gate; the other extends it.
  - The traps' guards (T2–T4, T6, T9–T12), the gate script, and ADR 0004's amendment (§8.1) in the same change.
- **PROD-04.0c — Amendment D names the engine's group subcommands** (docs and gate, Tier B; gate: owner sign-off on §8.2, rule 8). The amendment text, `check-no-oso.sh`'s secondary token list, and the doc_lint update in one commit.
- **PROD-04.0d — Fixtures for groups and ACLs** (infra, Tier B; extends PROD-01.5's profiles).
  - An `acl` profile: §3's overlay plus the non-super SCRAM principal.
  - A `groups` helper that creates one group of each type on 4.3 and the classic groups on 3.9, with members that stop cleanly. Bracketed `pkill` patterns are needed: this row's first control step killed its own `sh -c`.
  - The `streams` profile's `group.protocol=streams` variant (PROD-01.5's runs the classic protocol).
  - `share.coordinator.state.topic.replication.factor=1` and `…min.isr=1` on single-broker 4.x profiles. With the defaults (3 and 2) `__share_group_state` is never created (INVALID_REPLICATION_FACTOR on every attempt), so share groups there have members but no share-partition state (§3.8).
- **Upstream reports** (no ledger row; any worker may file them):
  - librdkafka: DescribeAcls drops the response's top-level error (T9);
  - librdkafka: ListConsumerGroups drops non-consumer protocol types without telling the caller (T3);
  - rust-rdkafka: PR #785 lacks the typed list, alter and ACL wrappers;
  - rust-rdkafka: `DescribeConfigsFuture` ignores per-resource errors (T13).

## 10. Acceptance rows

Fixture names refer to §3's groups and to PROD-04.0d's profiles. "compose 4.3" is the default stack at `KAFKA_VERSION=4.3.1`. Every negative control is a way the implementation could be wrong, and it must make the row fail.

These rows sit beside PROD-01.4's TI-04.1-* and TI-04.2-* and PROD-01.1's 04-1…04-5, and do not replace them:

- positions still bind to a topic generation;
- `PositionBeyondEnd` still excludes;
- translation never lands on a restored marker or aborted record;
- translation stays within one generation.

### PROD-04.1

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| AP-04.1-1 | With one group of each kind selected (classic Empty, classic live, consumer Empty, consumer live, share idle, share live, streams) plus an absent id, the snapshot has exactly one entry per id. Classic and consumer groups are `captured`, with the type and state `kafka-groups.sh --list` shows. Share and streams groups are `excluded: GroupTypeNotCaptured` (`groupType: other`). The absent id is `excluded: GroupNotFound`. | Classifying from ListConsumerGroups alone omits the share and streams groups (4 of 7 listed, §3.1), failing "one entry per id". Classifying from DescribeConsumerGroups alone types them, and the absent id, as simple Classic Dead groups (§3.1), failing the type check. | compose 4.3 + PROD-04.0d `groups` |
| AP-04.1-2 | Every captured position equals the broker's (`kafka-consumer-groups.sh --describe --offsets`) on every recovery-point partition. A partition without a commit is `noCommittedPosition`. | Mapping `Offset::Invalid` (−1001) to 0 records position 0 where the CLI shows "-". Measured case: `pa-orders` 1 of a group that committed there 0 versus one that never committed. | compose 4.3 and 3.9 |
| AP-04.1-3 | While a transactional offset commit is pending for a selected group, the affected partitions are `failed: PositionsUnstable` (a bounded RequireStable fetch), and the pre-transaction position is never recorded as captured. | A fetch without RequireStable (the engine's OffsetFetch v5, or `isolation.level=read_uncommitted`) returns 3 while 7 is pending (§3.4) and records it; the row fails. | compose 4.3; §3.4's TxnOffsetCommit held open longer than the capture's bound |
| AP-04.1-4 | The listing never calls `GroupInfo::members()` on rdkafka 0.36.2: the workspace clippy configuration denies it, or the lock carries rdkafka ≥ 0.37. A capture on a cluster with an Empty group completes. | Deleting the `disallowed-methods` entry and calling `members()` fails `cargo clippy -D warnings`. At run time the same call aborts the debug build on an Empty group (§3.1, §3.6: exit 134 on both lines). | lint; compose 3.9 (an Empty classic group) |
| AP-04.1-5 | A consumer group whose state librdkafka maps to Unknown is captured with `stateUnknownToClient` and marked active. | A classifier that reads Unknown as Empty marks it inactive; a unit test over the classifier with state code 0 catches it. | unit test over recorded listing rows |
| AP-04.1-6 | A selected group the capture principal may not describe is `failed: NotAuthorized`; the other groups are captured. | Aborting the whole snapshot fails the row, and so does mapping the refusal to an empty result, which records the group as captured with no positions. | PROD-04.0d `acl`: a principal without Describe on one group |
| AP-04.1-7 | Positions imported from an engine consumer-group snapshot (FX-1) carry `groupType: unknown` and are never shown as a consumer group's. | The engine keeps a streams group's positions untyped (§3.7). Importing them as `groupType: consumer` fails the row. | an engine snapshot written by `snapshot-groups` against §3's fixture (FX-1's regression input) |

### PROD-04.2

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| AP-04.2-1 | Applying positions to a target group with a live member is refused `GroupActive` for the whole group, and the readback is unchanged. After the member stops, the same application succeeds. This holds for classic and consumer groups. | The measured control (§3.3): the same commit succeeds once the member stops, so a row that passes while the member lives has applied nothing. An implementation that commits with the member's own id would pass the broker check and fail the unchanged readback. | compose 4.3; PROD-04.0d `groups` with members that stop cleanly |
| AP-04.2-2 | A target group that exists but is not classic or consumer (share, streams, Connect) is refused `TargetGroupTypeUnsupported` before any commit. | An **Empty streams group accepts** a non-member commit (K3; §3.3's control). An implementation that relies on the broker's refusal alone overwrites the stopped application's positions, and the readback shows it. | compose 4.3; a stopped streams group |
| AP-04.2-3 | Prior target positions on the applied partitions are read and recorded before any commit. After a failure part-way through several groups, the audit lists applied, refused and untouched groups; a re-run applies only the untouched ones, under the same approval. | Without the prior read nothing can be restored. A re-run that repeats the refused groups fails the audit comparison. | compose 4.3; three groups, one of them live |
| AP-04.2-4 | Every applied position carries leader epoch −1, never a captured source epoch. | A mutant of the commit builder that copies the captured epoch fails a unit test over it. The safe route has no setter today; the row guards a future switch to O2. | unit test |
| AP-04.2-5 | A target consumer group in a state librdkafka maps to Unknown is refused `GroupActive` without a commit attempt. | Reading Unknown as inactive attempts the commit (visible in the audit); a unit test over the classifier catches it. | unit test |
| AP-04.2-6 | A group absent from the target is created by the apply as a simple classic group, and the audit says `created`. | Reporting "applied to an existing group" contradicts the pre-apply listing, and the audit comparison fails. After the apply the id lists as classic, protocol type empty, Empty (§3.3). | compose 4.3 or 3.9 |

### PROD-05.3

| # | Pass predicate | Negative control | Fixture |
| --- | --- | --- | --- |
| AP-05.3-1 | With no authorizer, coverage is `aclsNotApplicable: authorizerDisabled`, decided from the broker's `authorizer.class.name` being present and empty (safe DescribeConfigs), never from DescribeAcls. A missing key (a refused DescribeConfigs, T13) gives `unverified`. | Trusting DescribeAcls' "0 bindings, no error" (§3.5, §3.8) reports `captured, 0 bindings`. Reading a missing key as "disabled" misreports the denied principal of §3.8. Either fails the row. | compose 4.3, the default stack; `acl` with the principal denied |
| AP-05.3-2 | A principal without Describe on the cluster gets `captureDenied`, decided from DescribeCluster's authorized operations (Describe absent). After Describe is granted, the capture returns every binding. | Trusting DescribeAcls' "0 bindings, no error" for `User:logweir` (§3.5) reports `captured, 0 bindings`; the row fails. | PROD-04.0d `acl` with its SCRAM principal |
| AP-05.3-3 | Literal, prefixed, wildcard-name and wildcard-principal bindings, ALLOW and DENY, on TOPIC, GROUP, TRANSACTIONAL_ID and CLUSTER resources round-trip exactly against `kafka-acls.sh --list`, with resource type 4 named CLUSTER. | Dropping the pattern type turns the prefixed `pa-` binding into a literal one; naming resource type 4 `BROKER`. Either fails the round-trip. | `acl`, with §3.5's six bindings and one cluster binding |
| AP-05.3-4 | Bindings librdkafka cannot represent (USER resources with CreateTokens or DescribeTokens, DELEGATION_TOKEN resources, TwoPhaseCommit) are listed as `notRepresentable`, with principal and resource name, counted in coverage and never exported. | Exporting what librdkafka returns yields two identical `Unknown / User:bob / Unknown` bindings where the broker holds two distinct ones (§3.5); the round-trip against `kafka-acls.sh --list` fails. | `acl`, with §3.5's four Java-created bindings |

## 11. Limits of this record

- **One broker.** Coordinator routing, and T6's duplicate groups across brokers, were not exercised. PROD-01.5's `cluster3` profile can.
- **Unmeasured traps.** No consumer group was caught in `Assigning` or `Reconciling` (T4 rests on source: C6). A broker error code outside rdkafka-sys's enum (T12) was not provoked.
- **Nothing on the consumer side after a commit.** Committing a source leader epoch to a target was not run. §4.3 avoids the question by committing −1, the only value the safe API has.
- **The raw leg is a prototype over PLAINTEXT only.** Its cost figure (§2, O4) is the engine's code, not a Logweir estimate. StreamsGroupDescribe was not sent: P has no codec for it.
- **Only local fixtures.** Managed providers (MSK, Confluent Cloud) were not tried; their group and ACL APIs follow OD-4.
- **Nothing ships.** The prototypes are throwaway, under `A/`. The measured values this record relies on are in its tables.
- **A class sweep this row does not own (T13).** `L/crates/logweir-kafka/src/rdkafka_reader.rs:293-325` (`topic_configs`) and `:343-386` (`broker_configs`) map a per-resource `Err` that rdkafka 0.36.2 never produces.
  - A refused or unknown topic therefore reads as a configuration with no overrides, rather than `NotAuthorized` or `TopicNotFound`.
  - `L/crates/logweir/src/drill/phase7_verify.rs:1209` reads the restore target's configuration through it.
  - The broker-resource case is measured (§3.8); the topic case is read from source.
  - It belongs to FX-4 and PROD-05.1.

## 12. Reproduction and artifacts

All paths are under `A/`, so `/tmp/logweir-roadmap-run/claude/artifacts/prod-04-0/`.

- **Probe.** `admin-probe/`, built with `cargo +1.89.0 build` into a temporary target directory (`admin-probe-build.log`; deleted afterwards).
  - Its `Cargo.toml` pins rdkafka `=0.36.2` with `logweir-kafka`'s feature list, and kafka-protocol `=0.18.0` with `client` only.
  - Its lockfile is the worktree's at `ac6aa00e`, plus kafka-protocol.
  - One leg, for example: `admin-probe ffi-list localhost:9092 all`, `admin-probe raw-describe localhost:9092 6 <group>…`, `admin-probe safe-commit localhost:9092 <group> <topic> <partition> <offset>`.
  - `PROBE_SASL_USER` and `PROBE_SASL_PASS` switch it to SCRAM on `localhost:9097`.
- **Drivers.**
  - `run-all.sh` runs one compose-lock hold, 06:36:10–06:48:42 UTC: 4.3.1 `groups`, 4.3.1 `acls`, `down -v`, 3.9.2 `legacy`, `down -v`. It calls `live-session.sh`.
  - `live-supplement.sh` and `live-aclguard.sh` each take and release the lock themselves.
  - The overlays are `acl-overlay.yml` and `share-state-overlay.yml`.
- **Outputs.** In `live-4.3.1/`, `live-4.3.1-acls/`, `live-3.9.2/`, `live-4.3.1-supplement/` and `live-4.3.1-aclguard/`:
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
  - 07:01:10–07:02:38 (`live-aclguard-rerun.sh`).

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
