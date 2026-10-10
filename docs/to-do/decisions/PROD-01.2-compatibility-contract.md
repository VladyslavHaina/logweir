# PROD-01.2 — The compatibility contract

- Row: PROD-01.2 (research and implementation, Tier B, lab `compose`), [product-expansion tracker](../product-expansion.md).
- Date: 2026-10-10 (UTC). Branch `claude/prod-01-2`, from `8436f941`.
- Every row is local compose (slot 2). No cloud resource was created and no remote endpoint was dialled (OD-4).
- Engine in every run: `kafka-backup` `0.23.3+logweir.2` (build inputs `sha256:2bca49d7…7db8`), from the published linux/arm64 runner image built from main `739f17c5` (the route [PROD-01.5c](PROD-01.5-fixture-profiles.md) found is needed on a Mac for the shipped engine).
- The published contract is [`docs/support-matrix.md`](../../support-matrix.md#the-compatibility-contract). This record is the reasoning and the measurements behind it.

## 0. Decision summary

1. **"Supported" is a row in this repository, never a reading.** §2 defines supported, limited, untested and unsupported by the evidence each needs, and a test holds the published tables to it (§9).
2. **Confluent Platform 8.3.2 (`cp-kafka`) is supported.** It did not differ from Apache Kafka 4.3.1 in anything Logweir uses (§4.2).
3. **Redpanda v26.2.4 is limited: a backup source only.** It serves Produce v0–v7; the engine sends Produce v8 and never negotiates, so a restore into it cannot run (§4.3). A backup of it is whole.
4. **Capability detection is four new check ids** (§5): `connection.engineProtocol`, `target.engineProtocol`, `connection.topicConfigsReadable`, `connection.groupTypes`. Each reads the endpoint's own answer, names what is missing and names a fallback. The plan asks for them, so an older controller never receives a row it cannot read.
5. **Two shipped readers recorded an unreported value as a fact**, and both are fixed (§6): phase 0 wrote `CreateTime` for a broker that had not reported its timestamp type, and `target.timestampBound` answered `ready` ("declares no bound") for a broker that had not reported a bound. Every other capture path already said "not recorded" with a reason.
6. **The minimum ACLs are measured** (§3): none for the probe, two for a backup, six for a scratch restore. One finding: a restore principal without `Delete` still signs `pass` and leaves its scratch topic behind.
7. **A passing connection test certifies nothing** (§4.5): against a listener that advertises an unreachable address the probe answers `reachable=true` and a backup fails a minute later. The readiness check now says which half failed.
8. **Managed providers stay untested** (§8), each with what blocks it. Amazon MSK with IAM and MSK Serverless are unsupported: they need OAUTHBEARER.
9. **No signed format changed.** The fixes are to a status field and a check row.

## 1. What Logweir needs from a Kafka-compatible endpoint

Two clients dial an endpoint, and they behave differently.

- **The engine** (`kafka-backup`) sends each request at one fixed version and never sends ApiVersions. Its table is `get_api_version`, `crates/kafka-backup-core/src/kafka/client.rs:625-648` of the vendored 0.23.3 source; an API absent from the table goes out at version 0. Recorded by [PROD-00.1](PROD-00-engine-route.md) and [PROD-01.5](PROD-01.5-fixture-profiles.md) §2.3 and not re-measured here. Since this row the table is also in the tree, `crates/logweir-engine-oso/src/vendored/request_versions.rs:49`, held to the pinned source by `cargo xtask check-drift`.
- **Logweir's own client** (librdkafka 2.3.0 through rust-rdkafka 0.36.2, and the FFI crate) negotiates: it sends ApiVersions first and picks the highest version both sides have.

An endpoint that does not serve an engine version closes the connection. An endpoint that lacks something Logweir's own client asks for returns an error or an older answer.

| Operation | Client | Kafka API (key) and version | Permission | When it is missing today | Honest? |
|---|---|---|---|---|---|
| **Probe** (`cluster-probe`, Test connection) | Logweir | ApiVersions (18), Metadata (3), negotiated; one `cluster_id()` read and nothing else (`crates/logweir/src/probe.rs:234-240`) | Authentication only (§3) | `cluster-id=` empty, `reachable=false`, exit 1 (`probe.rs:160-166`) | Yes, and narrow: `reachable=true` says the bootstrap address answered. It does not say the advertised brokers can be reached (§4.5). |
| **Backup**, the capture | Engine | Metadata (3) v9, ListOffsets (2) v5, Fetch (1) v11, DescribeConfigs (32) v1; on SASL also SaslHandshake (17) v1 and SaslAuthenticate (36) v2 | `Read` on each topic (it implies `Describe`) | The endpoint closes the connection; the engine exits 1; Logweir exits 1 with `kafka-backup backup exited 1` (`crates/logweir-engine-oso/src/engine.rs:1104`) and signs nothing | Honest and not actionable. **New:** `connection.engineProtocol` names the request and the served range before the backup (§5). |
| **Backup**, source identity | Logweir | Metadata, negotiated (cluster id, the named topics) | `Describe` on each topic | A topic not listed is refused before the engine | Yes. |
| **Topic identity** | Logweir | Metadata through librdkafka's DescribeTopics; the topic ID field exists from Metadata v10 | `Describe` on the topic | `topic_id: null` with the reason: `noTopicId`, `notAuthorized`, `topicNotFound`, `readFailed` (`crates/logweir/src/backup/topic_ids.rs:47-112`) | Yes. No endpoint measured here lacks IDs, so no check row was added (§10, P3). |
| **Configuration capture** | Logweir | DescribeConfigs (32), negotiated, per topic | `DescribeConfigs` on the topic (`Describe` does not imply it) | Coverage `captureDenied`, no entries, no timestamp type; an empty answer is a refusal, never "no overrides" (`crates/logweir/src/backup/config_coverage.rs:96`, `:187`; PROD-04.0 T13) | Yes. **New:** `connection.topicConfigsReadable` says so before the backup. |
| **Consumer positions** | Logweir | ListGroups (16) **v5** for the group's type, then the describe and offset calls of [PROD-04.0](PROD-04.0-admin-path.md) §1.2 | `Describe` on each selected group | Below ListGroups v5 a group's type reads Unknown, and the group is `excluded: GroupTypeNotCaptured` (`crates/logweir-kafka/src/groups.rs:640-646`, `crates/logweir/src/backup/consumer_positions.rs:338`) | Yes: never captured, never offset 0. **New:** `connection.groupTypes` says so before the backup. |
| **Restore**, the replay | Engine | Metadata (3) v9, Produce (0) **v8**; the SASL pair on SASL. Logweir renders `create_topics: false` (`crates/logweir-engine-oso/src/render_restore.rs:256`), so the engine creates nothing | `Write` on each target topic | The endpoint closes the connection; the engine reports "early eof" and exits 1; Logweir exits 1 with `kafka-backup restore exited 1` (`engine.rs:437`) and signs nothing | Honest and not actionable. **New:** `target.engineProtocol` (§5). |
| **Restore**, admission (phase 0) | Logweir | Metadata (cluster id against the allowlist, the marker topic); DescribeConfigs on the broker resource (timestamp type, record-timestamp bound); CreateTopics (19) | `Describe` on the marker topic; `DescribeConfigs` on the cluster; `Create` on the target topics | Marker absent or not describable: exit 3, refused by the guard (`crates/logweir/src/drill/phase0_admit.rs:562`). Broker configuration refused: exit 1, naming the grant. Create refused: exit 1, nothing left | Yes since this row. Two findings fixed: §6 S1, S2 and S3. |
| **Restore**, verification (phase 7) | Logweir | Fetch and ListOffsets, negotiated, on the restored topics | `Read` on each target topic | Exit 1, `TopicAuthorizationFailed`; the created topic is left | Yes about the failure. The leftover topic is §10 P5. |
| **Restore**, teardown (phase 9) | Logweir | DeleteTopics (20), fenced to the scratch prefix | `Delete` on the target topics | **Exit 0, `pass`, and the scratch topic is left behind** | **No.** The scorecard is true; the teardown failure is not surfaced. §10 P5. |
| **Verify** | neither | None. `logweir drill verify` and `docs/verify_scorecard.py` read a document, its signature and a public key | None | Nothing on the endpoint can be missing | Yes. |
| **Discovery** | Logweir | Metadata for all topics, negotiated | `Describe` on a topic for it to be listed | A topic the principal may not describe is omitted with no error, so the inventory's `visibility.state` is `unknown` unless an administrator attests otherwise ([kubernetes.md](../../kubernetes.md), `TopicDiscovery`) | Yes. |

## 2. The classification

| Word | It claims | The evidence it needs |
|---|---|---|
| **supported** | The operations the row names work against this endpoint. | A versioned, repeatable row in this repository: a test, named in the row, that passes against an image pinned by digest. |
| **limited** | Part works and a named part does not, and Logweir says which before the operation starts. | The same kind of row, showing both halves. |
| **untested** | Nothing either way. | None; the row says what blocks a real one. A run made once by hand is named with its date and does not change the word. |
| **unsupported** | It does not work, or Logweir refuses it by name. | The refusal, or the measurement. |

Three consequences, each applied in the published tables:

- **A hand run is not "supported".** SCRAM-SHA-512 over TLS ran once on docker-desktop (PLAT-07.1) and RustFS and versitygw ran once in private containers (PROD-01.5). Nothing in the repository repeats them, so all three are untested.
- **A managed provider is untested until OD-4 evidence exists**, however well its mode works against a local listener.
- **"Supported" is a statement about the operations in the row.** The broker rows are about the whole path (probe, capability checks, backup, restore, both verifiers). What a backup records beyond the records themselves has its own rows, because it differs by endpoint: Apache Kafka 3.7.1 is supported for the whole path and unsupported for consumer positions.

## 3. The minimum-permission profile

Measured on the `acl` profile: Apache Kafka 4.3.1, `StandardAuthorizer`, `allow.everyone.if.no.acl.found=true`, the restricted principal `User:logweir` over SCRAM-SHA-512. Row: `e2e/tests/compat_contract.rs::the_minimum_acls_for_probe_backup_and_restore`.

**Method.** Every resource the row touches is first closed (given a binding for another principal), so nothing is open by the "no ACL found" rule. Then exactly the listed bindings are granted and the operation must succeed; then each binding is removed in turn and the result is required to be the one in the table. The row fails if a listed binding turns out not to be needed.

| Operation | Bindings (all `ALLOW`, for the Logweir principal) |
|---|---|
| Probe | None. |
| Backup | `Read` on each source topic; `DescribeConfigs` on each source topic. |
| Restore into new topics (a scratch drill) | On the target: `Create`, `Write`, `Read` and `Delete` on the drill's topic prefix; `DescribeConfigs` on the cluster; `Describe` on the scratch marker topic. |

| Removed | Exit | What happens | How it reads |
|---|---|---|---|
| Backup: `Read` on the topic | 1 | No receipt. The topic-ID read is refused first and logged as `notAuthorized`; the engine then fails. | `engine: operational: kafka-backup backup exited 1` |
| Backup: `DescribeConfigs` on the topic | 0 | A receipt, with coverage `captureDenied`, no entries and no timestamp type. The topic ID is still recorded. | A warning naming the grant; `connection.topicConfigsReadable` is `notReady` beforehand |
| Restore: `Create` on the prefix | 1 | Phase 0 fails; nothing is left on the target. | `target topic … could not be created with the pinned configuration …: TopicAuthorizationFailed` |
| Restore: `Write` on the prefix | 1 | Phase 6 fails; the created topic is left. | `engine: operational: kafka-backup restore exited 1` |
| Restore: `Read` on the prefix | 1 | Phase 7 fails; the created topic is left. | `Message consumption error: TopicAuthorizationFailed` |
| Restore: `Delete` on the prefix | **0** | **A signed `pass`; the scratch topic is left behind.** | Nothing on the output says the teardown failed. |
| Restore: `DescribeConfigs` on the cluster | 1 | Phase 0 fails before anything is created. | `… the principal lacks DescribeConfigs on the cluster` |
| Restore: `Describe` on the marker | 3 | Refused by the admission guard. | `marker topic … does not exist …, or this principal may not Describe it …` (it said only "does not exist … Create it" before this row; §6 S3) |

The restore row is a scratch drill of a topic whose backup recorded no configuration overrides, into a broker on `CreateTime`. Three cases need `DescribeConfigs` on the target topics as well, which FX-4 measured on 2026-10-05 and this row did not repeat: a topic whose backup recorded overrides (the engine's restore describes that topic), an existing mapped topic (phase 2), and a broker on `LogAppendTime` (phase 0's probe readback). The chart README's ACL list for a managed cluster was corrected from this table: it named neither `Read` on the target topics nor `Describe` on the marker. It also named `DescribeCluster` for a backup, which this measurement did not need; the README now says so and keeps the word, because no MSK cluster has been run against to say whether MSK's authorizer asks for it (`crates/logweir/tests/chart_lint.rs` pins it as an MSK fact).

Not in the table: consumer positions need `Describe` on each selected group (a hidden group is recorded `failed: NotVisibleToPrincipal`: `e2e/tests/position_evidence.rs::a_group_hidden_from_the_backup_principal_is_never_absent`), and archive permissions are the object store's.

## 4. Measured rows

All on 2026-10-10 (UTC), compose slot 2, one stack at a time. Evidence files are under `artifacts/prod-01-2/runs/` in the orchestration directory; each row also writes `compat/<row>.json` under the stack's scratch directory.

### 4.1 What each endpoint serves

By the endpoint's own answer to `kafka-broker-api-versions.sh` (the 4.3.1 image's tool, run in-network). The Apache Kafka columns for 3.7.1, 3.9.2 and 4.1.2 are PROD-01.5's.

| API | Engine sends | Apache Kafka 4.3.1 | Confluent Platform 8.3.2 | Redpanda v26.2.4 |
|---|---|---|---|---|
| Produce | v8 | v0–v13 | v0–v13 | **v0–v7** |
| Fetch | v11 | v4–v18 | v4–v18 | v4–v13 |
| ListOffsets | v5 | v1–v11 | v1–v11 | v0–v6 |
| Metadata | v9 | v0–v13 | v0–v13 | v0–v12 |
| DescribeConfigs | v1 | v1–v4 | v1–v4 | v0–v4 |
| SaslHandshake, SaslAuthenticate | v1, v2 | v0–v1, v0–v2 | v0–v1, v0–v2 | v0–v1, v0–v2 |
| ListGroups (Logweir needs v5) | v2, not sent by a backup or restore | v0–v5 | v0–v5 | **v0–v4** |
| DescribeTopicPartitions, ConsumerGroupDescribe | not sent | served | served | not served |

### 4.2 Confluent Platform 8.3.2

Image `confluentinc/cp-kafka:8.3.2@sha256:5e8f3ab5…3cec48`, the community image. The broker reports `8.3.2-ccs`, Confluent's build of Apache Kafka; the licence files in the image are Apache-2.0. It is not `cp-server`.

Row `confluent_platform_backs_up_restores_and_verifies`: probe `reachable=true`; all four capability checks `ready`; a backup with exit 0, receipt 1.7.0 verified by both readers, topic IDs from `describeTopics`, coverage `captured`, a classic group `captured`; a drill `pass`, the scorecard verified by both readers. **No difference from Apache Kafka 4.3.1.**

### 4.3 Redpanda v26.2.4

Image `redpandadata/redpanda:v26.2.4@sha256:c98c2f04…124f2e89`. Business Source License 1.1, read at the `v26.2.2` tag (the `v26.2.4` tag is not published on GitHub; the image's git reference is `9a85b6b72a…`). Its use grant excludes only offering Redpanda to third parties as a streaming service, so a local fixture is permitted use. The row enables no enterprise feature.

Row `redpanda_backs_up_and_refuses_a_restore_before_it_starts`:

| What | Result |
|---|---|
| Probe | exit 0, `reachable=true`. The cluster id is `redpanda.` and a UUID, recorded as reported. |
| `connection.engineProtocol` | `ready`: Redpanda serves every capture request. |
| `connection.groupTypes` | `notReady`: ListGroups v0–v4. |
| Backup | exit 0, receipt 1.7.0, both readers. Topic IDs recorded. Coverage `captured`, with the entries Redpanda reports (no `min.insync.replicas`, `unclean.leader.election.enable` or `remote.storage.enable`). The selected group: `excluded: GroupTypeNotCaptured`. |
| Broker configuration | Nine keys (`advertised.listeners`, `auto.create.topics.enable`, `default.replication.factor`, `listeners`, `log.dirs`, `log.retention.bytes`, `log.retention.ms`, `log.segment.bytes`, `num.partitions`). No timestamp type, no record-timestamp bound. |
| `target.engineProtocol` | `notReady`: "it sends Produce v8 and this endpoint serves Produce v0-v7". |
| A drill started anyway | exit 1, no scorecard. The engine: "Restore completed with 1 error(s): … Connection error during read response length (UnexpectedEof): early eof". The scratch topic it created is left behind. |

Row `redpanda_authenticates_both_scram_mechanisms`: SCRAM-SHA-256 and SCRAM-SHA-512 backups pass on Redpanda's SASL listener, on both clients; a wrong password and the other mechanism's user are refused.

**The difference is the engine's, and so is the route out.** An engine that negotiated, or sent Produce at v7 or lower to an endpoint that serves no more, would restore into Redpanda. That is PROD-00.3's capability row C8, and §10 P1.

### 4.4 The four Apache Kafka lines

The generic row (`the_default_broker_backs_up_restores_and_verifies`) and the capability row (`the_default_broker_answers_every_capability_check`), one line at a time (`stack-env.sh --slot 2 --kafka LINE --profiles auth`, the digest-pinned images, the version read back from the running broker). The capability row requires each check to agree with the broker's own tool, whichever way it points.

| Line | Capability checks | Backup | Restore |
|---|---|---|---|
| 3.7.1 | the two engine rows and the configuration row `ready`; `connection.groupTypes` **`notReady`** (ListGroups v0–v4) | exit 0, both readers; topic ID recorded; the selected group `excluded: GroupTypeNotCaptured` | `pass`, both readers |
| 3.9.2 | all four `ready` | exit 0, both readers; a classic group `captured` | `pass`, both readers |
| 4.1.2 | all four `ready` | the same | `pass`, both readers |
| 4.3.1 | all four `ready` | the same | `pass`, both readers |

The `broker-lines` job of `engine-matrix.yml` now runs both rows by name on 3.9, 4.1 and 4.3 every week; CI runs the capability row on 3.7.1 on every pull request.

### 4.5 An advertised address nobody can reach

The `confluent` profile's broker has a third listener, `OFFNET`, that answers a bootstrap connection and advertises `127.0.0.1:1`. Row `an_unreachable_advertised_address_is_not_a_reachable_cluster`:

| Step | Result |
|---|---|
| `logweir cluster-probe` | exit 0, `reachable=true`, the real cluster id |
| Readiness check | `connection.authenticated` `notReady`, `BrokerUnreachable`, after about 12 s. The capability rows are `unknown`, `BlockedByPrerequisite`. |
| `logweir backup run` | exit 1 after about 70 s, no receipt |

The row's message used to be the generic one about the bootstrap address, which is the one thing that works here. It now says the bootstrap answered and named the cluster and the advertised brokers did not answer, and the remedy names `advertised.listeners` (`crates/logweir/src/check/kinds/readiness.rs:61`, `:73`). The probe's own answer is unchanged: it is defined as one `cluster_id()` read. §10 P6.

## 5. Capability detection

### 5.1 The rows

| Check id | Operation | Gating | Ready | Not ready | Unknown |
|---|---|---|---|---|---|
| `connection.engineProtocol` | Backup | blocking | `EngineProtocolSupported` | `EngineProtocolUnsupported`: each request the endpoint does not serve, with the range it does | `ApiVersionsNotObserved` |
| `target.engineProtocol` | Restore | blocking | `EngineProtocolSupported` | `EngineProtocolUnsupported` | `ApiVersionsNotObserved` |
| `connection.topicConfigsReadable` | Backup | advisory | `TopicConfigsReadable` | `TopicConfigsNotReadable`, naming the topics | the read's own code (a timeout) |
| `connection.groupTypes` | Backup | advisory | `GroupTypesListed` | `GroupTypesNotListed` | `ApiVersionsNotObserved` |

One more code, on an existing row: `target.timestampBound` is `unknown` with `TimestampBoundNotReported` when the broker's answer carries neither bound key (§6 S2). That row is answered for every restore plan, so the code follows §5.3's rule too: a plan that lists no capability row came from an older controller, and gets the same `unknown`, message and remedy under `BrokerConfigsNotReadable`, which that controller already reads.

The engine rows block because the operation cannot run. The other two are advisory because the backup runs and is whole; what it cannot record is recorded as not recorded. Code: `crates/logweir/src/check/kinds/capability.rs:94`, `:155`, `:189`; the vocabulary in `crates/logweir-core/src/check_contract.rs:512`.

**The SASL pair is asked for only on a SASL connection** (`capability.rs:54`), because the engine sends SaslHandshake and SaslAuthenticate only then.

**Not observed is `unknown`, never `ready`.** A row whose connection did not authenticate, or whose topics are not describable, is `unknown` with `BlockedByPrerequisite` (`capability.rs:263`).

### 5.2 Where the served ranges come from

From the endpoint's own ApiVersions answer on a real connection, with the operation's credential.

rust-rdkafka 0.36.2 does not deliver librdkafka's log lines, and librdkafka has no public call that returns the negotiated ranges. It does log them: with `debug=feature` each broker connection writes one `ApiKey <name> (<key>) Versions <min>..<max>` line under the facility `APIVERSION` (`rdkafka_request.c:3194-3209` at 2.3.0). So the FFI crate of OD-6 gained one safe function, `drain_logs` (`crates/logweir-rdkafka-ffi/src/logs.rs:139`): a short-lived consumer handle with `log.queue=true`, its log queue forwarded to a private queue, polled until the lines stop. `logweir-kafka` parses and folds them (`crates/logweir-kafka/src/api_versions.rs:83`, `:164`; `inventory.rs:1492`): the range a request is served at is the intersection over every broker connection that answered, and a drain that did not go quiet yields no answer at all.

Rejected: a table of ranges by broker version string (that is reasoning, and Redpanda has no such string); a hand-written ApiVersions client over a raw socket (it would need its own SASL and TLS); reading the range from a failed request (it would have to fail first).

**Drift guards.** The parser runs against librdkafka's in-process mock cluster in the default test suite (`crates/logweir-kafka/tests/api_versions.rs`), so a librdkafka upgrade that rewords the line fails a unit row and not a customer's preflight. The live row requires agreement with the broker's own tool.

### 5.3 The plan asks, or the row is not emitted

The check id vocabulary is closed on the reading side too: a controller that receives an id it does not know refuses the whole result (`ResultUnreadable`). So a runner emits a capability row only for an id the plan lists in `capabilityChecks`, a new optional field of the backup and restore check requests, absent when empty.

| Controller | Runner | What happens |
|---|---|---|
| this build | this build | The plan lists the operation's capability rows (`crates/weirkeeper/src/controllers/preflight.rs:642`); the runner answers them. |
| this build | older | The older runner refuses the plan field (`deny_unknown_fields`); the `Preflight` is `Failed`, `CheckContractMismatch`, with the message naming `capabilityChecks`. **Upgrade the runner image with the controller.** This applies to every Backup and Restore `Preflight`, not only to some. |
| older | this build | The plan has no field; the runner emits no capability row; the older controller reads the result as before. |

A plan may list only its own operation's capability rows, once, and never one it also skips (`check_contract.rs:1629`). A skipped capability row is left out of the plan and reported `skipped`, like any other.

`logweir check run` answers the same plan a `Preflight` renders, so both surfaces carry the rows. The standalone `backup run` and `drill run` do not run them on their own (§10 P7).

## 6. Capture paths swept: "unsupported metadata never appears captured"

Every place a backup, a restore or a check writes down something it read from the endpoint, asked one question: when the endpoint did not answer, is the absence recorded as an absence?

| # | Path | Before this row | Now | Row |
|---|---|---|---|---|
| S1 | Phase 0's target preflight, published as `Restore.status.topicPreflight.timestampType` | **A broker answer without `log.message.timestamp.type` was recorded as `CreateTime`** | The field is absent, a warning says the type is not recorded, and the `LogAppendTime` refusal applies only to a reported value (`crates/logweir/src/drill/phase0_admit.rs:62`, `:725-729`) | `crates/logweir/tests/topic_preflight.rs::a_broker_that_does_not_report_its_timestamp_type_is_not_recorded_as_create_time`; live on Redpanda |
| S2 | `target.timestampBound` | **A broker answer with neither bound key was `ready`: "declares no record-timestamp bound"** | `unknown`, `TimestampBoundNotReported` (`crates/logweir/src/check/kinds/restore.rs:987`, `:1012`) | `crates/logweir/tests/check_cli.rs::a_broker_answer_without_the_timestamp_bound_is_unknown_never_no_bound`; live on Redpanda |
| S3 | Phase 0's marker check, and `doctor`'s | A marker topic the principal may not describe read as "does not exist … Create it" | "does not exist, or this principal may not Describe it", naming the grant (`phase0_admit.rs:562`, `crates/logweir/src/doctor.rs:563`) | `crates/logweir/tests/restore_mode.rs`; live on the `acl` profile |
| S4 | The store's error classifier | versitygw's `404 XAdminUserNotFound` read as a missing object | A refused credential, at every site that maps a 404 (`crates/logweir-store/src/lib.rs:2103`, `:2291`); a genuine `NoSuchKey` is still not-found (PROD-01.5 C6) | `a_404_that_refuses_the_credential_is_never_not_found`, in that file |
| — | Topic IDs (`generations`) | `null` with a reason | unchanged | `topic_ids.rs:47` |
| — | Topic configuration (`config_coverage`, `topic_configuration`) | `captureDenied` / `notCaptured`; an empty answer is a refusal | unchanged | `config_coverage.rs:96`, `:187` |
| — | The topic's timestamp type in the receipt | absent when the configuration was not read | unchanged | measured with `DescribeConfigs` removed (§3) |
| — | Consumer positions | one outcome per selected group; `excluded` with a reason | unchanged | `consumer_positions.rs:338` |
| — | Replication factors (FX-21) | an unrecorded factor is never read as matching | unchanged | `e2e/tests/replication_factor_parity.rs` |
| — | Schema dependency | `notAssessed` with a reason | unchanged | `e2e/tests/schema_dependency.rs` |
| — | Discovery | `visibility.state: unknown` unless attested | unchanged | [kubernetes.md](../../kubernetes.md) |

S1 and S2 are the same defect in two readers: a map lookup that fell back to a default when the key was absent. PROD-04.0's T13 fixed the refused read (an empty map); these are the read that succeeded and did not carry the key. Class sweep owed: §10 P4.

## 7. Archive backends

PROD-01.5's notes C4 and C6. What Logweir relies on: conditional create (`If-None-Match: *`), reads by version id on a versioned bucket, path-style SigV4. It does not read Object Lock retention back.

| Backend | Status | Conditional create | Versioned reads | Evidence |
|---|---|---|---|---|
| MinIO `RELEASE.2025-09-07T16-13-09Z` (the project's rebuild) | supported | Holds; every e2e row claims its execution on it | The fixture's buckets are unversioned: no pin | `e2e/tests/mvp_demo.rs`, `e2e/tests/backup_argv.rs::a_backup_over_a_set_an_earlier_engine_run_wrote_is_refused_before_the_engine` |
| SeaweedFS 4.48 (the `objectstore` profile) | supported | A second claim of one execution exits 1, `failure-reason=ExecutionAlreadyClaimed` | On the versioned bucket the receipt pins `archive.manifest_version_id`; on the unversioned one it carries none | `e2e/tests/compat_contract.rs::seaweedfs_takes_a_backup_a_restore_and_refuses_a_second_claim` |
| RustFS 1.0.0 | untested | Held once (PROD-01.5, 2026-09-29) | Held once | No fixture in the repository |
| versitygw v1.8.0 | untested | Held once | Held once | No fixture. Its `404 XAdminUserNotFound` is S4 above. **C6's acceptance is met:** `crates/logweir-store/tests/minio_options.rs` against `versity/versitygw:v1.8.0@sha256:30292fc2…a2499` is 6 passed, 0 failed (by hand, 2026-10-10, a private container on slot 2's object-store port, removed afterwards); PROD-01.5 recorded 5 passed, 1 failed on 2026-09-29 (`an_unknown_access_key_id_is_classified_as_a_credential_problem`) |
| AWS S3 | untested | `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` | the same | OD-4 |
| GCS, Azure Blob | untested | `[UNVERIFIED — native conditional create in object_store, not run against either provider]` | not run | OD-4 |
| A local directory | untested | In process only | none | No engine row |
| A store that ignores `If-None-Match` | unsupported | Refused: exit 4 `ExecutionClaimUnproven`; `ConditionalCreateUnsupported` beforehand | — | `crates/logweir/tests/check_marker_create_only.rs` |

**Lock readback is unsupported on every backend, by Logweir's own choice.** `Store::object_lock_readback` returns no proof (`crates/logweir-store/src/lib.rs:952`), so a scorecard says `immutable: false` and never guesses from a bucket's configuration. The four stores above answered the readback to the AWS CLI in PROD-01.5's protocol probe (GOVERNANCE mode). Reading it through Logweir is PROD-09.1's.

## 8. Managed providers

None was dialled. Each is untested or unsupported, with what blocks a row.

| Provider | Status | How Logweir would reach it | What blocks a row |
|---|---|---|---|
| Amazon MSK (provisioned), SASL/SCRAM | untested | `scramSha512` over TLS, port 9096, the Secrets Manager credential | No cluster until the owner funds a session (OD-4). The mode has no repeatable local row either: no compose listener serves SCRAM-SHA-512 over TLS (§10 P8). |
| Amazon MSK (provisioned), mutual TLS | untested | `mtls`, port 9094 | No cluster. |
| Amazon MSK with IAM; MSK Serverless | unsupported | Not reachable | Both need OAUTHBEARER with a SigV4 token, which no `auth.mode` names and the engine's command line cannot be given (PROD-00.1 C9; OD-3 defers it). |
| Confluent Cloud | untested | `plain` over TLS, the API key and secret, the public CA | No account. Not known: whether it serves the engine's fixed request versions. |
| Redpanda Cloud | untested | `scramSha256` or `scramSha512` over TLS | No account. If it serves what v26.2.4 serves, it is a backup source only. That is a projection. |
| Aiven for Apache Kafka | untested | `mtls` with the project CA, or SCRAM over TLS | No account. |
| Azure Event Hubs (Kafka endpoint) | untested | `plain` over TLS, `$ConnectionString` as the user name | No namespace. It is not a Kafka broker; whether it serves the engine's requests is not known. Its Entra ID sign-in is OAUTHBEARER. |

The first thing to run against any of them is a Backup `Preflight`: `connection.engineProtocol` answers whether the engine can read there before anything else is tried.

## 9. Guards and their mutants

| Guard | What it fails on | Mutant that fails it |
|---|---|---|
| `crates/logweir/tests/support_matrix.rs::the_compatibility_matrix_claims_only_what_a_row_shows` | A row with no status or two; a supported or limited row with no test reference; a referenced test or file that does not exist; a check id the vocabulary does not have; the tables gone | `every_rule_of_the_matrix_lint_has_a_mutant_that_fails_it` plants each one on a copy of the real page |
| `crates/logweir/tests/engine_matrix.rs` (the `broker-lines` job) | The compatibility rows not run by name, without `--include-ignored`, against another test binary, or outside the stack's lifetime | Four controls in the same test |
| `e2e/tests/stack_params.rs::the_other_endpoints_are_pinned_by_digest_and_opt_in` | Either endpoint unpinned, in the default profile set, or its digest missing from the support matrix | Run by hand: removing a digest, and moving a service out of its profile, each fail |
| `cargo xtask check-drift` (`VERSION_CHECKS`) | The in-tree request-version table differing from the pinned engine source | A unit row edits one version and requires the failure |
| Negative controls for each new check id (`crates/logweir/tests/check_cli.rs`) | The capability present: `ready`. Absent: the finding with its fallback text. Not observed: `unknown`. Not listed: no row | Each row has both arms |

## 10. Limits, handoff and proposed rows

**Limits.** One Redpanda version and one Confluent Platform version. Single-node brokers. No managed provider. The ACL profile was measured on 4.3.1 only. The scratch-drill restore is the measured restore; a restore into existing topics needs no `Create` and was not measured separately.

**Handoff, as numbered acceptance rows.**

For PROD-14.1 (the install guide and recovery kit):

1. The guide's compatibility statement is a link to the contract's tables and repeats no status of its own.
2. The guide's first check against a new cluster is a Backup `Preflight` (or `logweir check run`), and it names `connection.engineProtocol` as the row that says whether the engine can read there.
3. The guide's permission list for the backup and restore principals is §3's, and its restore list includes `Delete` with the reason.
4. A cluster that types no groups (Apache Kafka 3.7, Redpanda) is told so before a group is selected.

For PROD-03.1 (registry capture):

5. A registry row moves from unsupported to supported only with a test named in the contract's registry table, against a registry image pinned by digest.
6. A registry the capture cannot read is recorded as not captured with the reason, and a capability row of the plan says so before the backup (the pattern of §5.3).
7. Detection (`schema_dependency`) keeps meaning "Confluent framing seen in the bytes" and is never upgraded to "registry captured" by the new row.

**Proposed rows.**

| # | Row | Evidence | Acceptance |
|---|---|---|---|
| P1 | **The engine restores into an endpoint that serves Produce below v8** (PROD-00.3 C8: negotiate, or choose the version from the endpoint's range) | §4.3 | The Redpanda row's restore passes and its broker row moves from limited to supported |
| P2 | **Fixtures for RustFS and versitygw** | §7 | Each store as an opt-in profile or a weekly row, with a test the matrix can cite; the two rows move from untested to supported |
| P3 | **`connection.topicIds`** when an endpoint without topic IDs is measured | §1 | A row that is `notReady` on such an endpoint, with its negative control |
| P4 | **Class sweep: a configuration lookup that falls back to a default when the key is absent** (S1, S2) | Not fixed here: `crates/logweir/src/drill/phase3_diff.rs:151` reads a source setting the existing target topic does not report as "not differing" (`unwrap_or(false)`). Reviewed and left: `phase0_admit.rs:736` (an unreported retention is omitted from the status line) | Each `broker.get(` and `configs.get(` in `crates/logweir/src` either records the absence or has a row showing why the default is right; the phase 3 collision diff names a setting the target does not report as not assessed |
| P5 | **A failed or unpermitted restore leaves its scratch topics, and a missing `Delete` grant is a silent `pass`** (PROD-07.2's domain) | §3, §4.3 | The teardown's failure is on the output and in the scorecard's notes; a failed drill removes what it created or names what it left |
| P6 | **Test connection says more than "the bootstrap answered"** | §4.5 | The probe, or the console's Test connection, reports an unreachable advertised address as such |
| P7 | **The standalone CLI runs the capability rows before a backup or restore** (`backup run`, `drill run`, or `doctor`) | §5.3 | A restore into Redpanda from the CLI is refused by name before the engine starts |
| P8 | **A SCRAM-SHA-512 TLS listener in the `auth` profile**, and its row | §8 | `auth_modes.rs` backs up, restores and verifies over it; the contract's row moves to supported |
| P9 | **Capability rows on `SourceConnection`** (Test connection in the console) | §5.3 | A connection test against Redpanda says it can be a source and not a target |
| P10 | **A topic-level fallback for the timestamp bound and type** when the broker resource does not report them (Redpanda keeps them per topic) | §4.3 | `target.timestampBound` is answered from the created topic's own configuration |
| P11 | **A source topic the principal cannot read reaches the engine before it fails** | §3 | A Backup `Preflight` row, or the runner, names the missing `Read` before the engine starts |
| P12 | **Provider rows under OD-4**: AWS S3 and Amazon MSK first | §7, §8 | Each recorded separately from the local rows, with the session's date and cost |
| P13 | **A directory archive row** for the standalone CLI | §7 | A backup, a restore and both verifiers with `backend: filesystem` and the real engine |

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
