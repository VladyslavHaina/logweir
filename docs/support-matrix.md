# Support matrix

<!--
HAND-WRITTEN, except the section between the `engine-matrix:rows` markers,
which `.github/workflows/engine-matrix.yml` renders weekly and publishes as the
`support-matrix` artifact (a pull request only when the `ENGINE_MATRIX_OPEN_PR`
repository variable is `true`). Everything else carries exactly the rows that
were ACTUALLY EXERCISED, and says so for every other version rather than
projecting a result. A matrix that lists an untested version with a verdict is
worse than a short matrix.
-->

**Installing Logweir is [install.md](install.md)**, which leads with the engine
floor below. This file is the row-by-row evidence behind it: the compatibility
contract first, then the engine floors and each measurement in detail.

## The compatibility contract

What Logweir was shown to do, against which endpoint, in four words
(PROD-01.2). The per-operation capability list, the measurements and the
reasoning are in the
[decision record](to-do/decisions/PROD-01.2-compatibility-contract.md).

| Word | What it claims | The evidence it needs |
|---|---|---|
| supported | The operations the row names work against this endpoint. | A versioned, repeatable row in this repository: a test, named in the row, that passes against an image pinned by digest. |
| limited | Part works and a named part does not, and Logweir says which before the operation starts. | The same kind of row, showing both halves: what works, and the check or refusal that names what does not. |
| untested | Nothing either way. The mode is implemented, and nothing in this repository repeats a run against this endpoint. A run somebody made once by hand is named with its date, and the row stays untested. | None. The row says what blocks a real one. |
| unsupported | It does not work, or Logweir refuses it by name. | The refusal, or the measurement that shows it cannot work. |

Nothing is called supported on reasoning, and a managed provider is untested
until somebody runs against it (OD-4): a local container is not a hosted
service. `crates/logweir/tests/support_matrix.rs` holds this page to it: one
status per row between the two markers below; a named test behind every
supported and limited row, wherever on the page it stands; every reference of
that shape is to a function with `#[test]` above it; and every check id named
here exists.

**A passing connection test is none of these.** `logweir cluster-probe` and the
console's Test connection reach the bootstrap address and read the cluster id.
They do not show that the engine can read or write there
([Advertised addresses](#advertised-addresses), and the capability checks
below).

Every row ran the engine the images ship, `kafka-backup` `0.23.3+logweir.2`,
on a compose stack. **A cell says who runs its row.** "CI" is the e2e job of
every pull request, on Apache Kafka 3.7.1. "By hand" is a row that is
`#[ignore]`d because it needs a profile or a broker line CI's job does not
start: somebody ran it on the date given, on compose slot 2, with that engine
from the published linux/arm64 runner image, and **nothing runs it again
until somebody does** (tracker row PROD-01.2a asks for a weekly job). A
supported cell that rests on a hand-run row is true of the day it was run.

<!-- compatibility:begin -->

### Brokers

The status is for the whole path: the probe, the capability checks, a backup,
a restore into new topics, and both verifiers on the receipt and the scorecard.
Plaintext, one node, KRaft, MinIO. A cluster of several brokers has one row
of its own, under [Capability checks](#capability-checks).

| Endpoint | Status | What the row shows | Evidence |
|---|---|---|---|
| Apache Kafka 3.7.1, the default fixture and the line CI runs on every pull request | **supported** | The whole path. Its group listing does not type groups: see the next table. Apache no longer maintains this line (legacy, under [Broker versions](#broker-versions-apache-kafka)); supported here means Logweir's rows pass on it. | CI: `e2e/tests/full_drill.rs::a_full_drill_produces_a_signed_scorecard_with_real_numbers`, `e2e/tests/mvp_demo.rs::mvp_demo_backs_up_restores_at_a_point_in_time_and_verifies`, `e2e/tests/compat_contract.rs::the_default_broker_answers_every_capability_check`. The generic row on this line, `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies`: by hand, 2026-10-10; no workflow runs it |
| Apache Kafka 3.9.2 | **supported** | The whole path, and every capability check `ready`. | `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies` and `e2e/tests/compat_contract.rs::the_default_broker_answers_every_capability_check` with `--kafka 3.9`: by hand, 2026-10-10, and by the weekly `broker-lines` job of `engine-matrix.yml`, whose step for these two rows has not yet run on GitHub |
| Apache Kafka 4.1.2 | **supported** | The same. | `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies` and `e2e/tests/compat_contract.rs::the_default_broker_answers_every_capability_check` with `--kafka 4.1`: by hand, 2026-10-10, and by the weekly `broker-lines` job of `engine-matrix.yml`, whose step for these two rows has not yet run on GitHub |
| Apache Kafka 4.3.1 | **supported** | The same. | `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies` and `e2e/tests/compat_contract.rs::the_default_broker_answers_every_capability_check` with `--kafka 4.3`: by hand, 2026-10-10, and by the weekly `broker-lines` job of `engine-matrix.yml`, whose step for these two rows has not yet run on GitHub |
| Confluent Platform 8.3.2 (the `confluentinc/cp-kafka` image, which reports `8.3.2-ccs`) | **supported** | The whole path, with no difference from Apache Kafka 4.3.1: the same request ranges, the same capability answers, a classic group captured. | `e2e/tests/compat_contract.rs::confluent_platform_backs_up_restores_and_verifies` (by hand, 2026-10-10; no workflow runs it) |
| Redpanda v26.2.4 | **limited** | **A backup source only.** The probe, a backup and both verifiers pass, with topic IDs and the topic's configuration recorded. **A restore into it cannot run:** the engine sends Produce v8 and never negotiates, and Redpanda serves Produce v0–v7. `target.engineProtocol` is `notReady` and says so before a restore starts; a restore started anyway fails in the engine and signs nothing. Restore its archive into a cluster that serves Produce v8. | `e2e/tests/compat_contract.rs::redpanda_backs_up_and_refuses_a_restore_before_it_starts` (by hand, 2026-10-10; no workflow runs it) |
| Apache Kafka 4.0.x and 4.2.x | **untested** | No pinned image and no row. | |
| Apache Kafka on ZooKeeper (any 3.x) | **untested** | Every fixture is KRaft. | |
| Any other Kafka-compatible endpoint | **untested** | `connection.engineProtocol` and `target.engineProtocol` answer, before anything is read or written, whether it serves the request versions the engine sends. | |

### What a backup records, by endpoint

An endpoint that cannot answer is recorded as **not recorded, with the
reason**. It is never recorded as an empty answer.

| What | Endpoints | Status | What the receipt says | Evidence |
|---|---|---|---|---|
| Consumer-group positions (`--consumer-group`) | Apache Kafka 3.9.2, 4.1.2, 4.3.1; Confluent Platform 8.3.2 | **supported** | A classic or consumer group is `captured` with its positions. | The generic row on each endpoint, which requires its group: `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies`, `e2e/tests/compat_contract.rs::confluent_platform_backs_up_restores_and_verifies` (by hand, 2026-10-10; no workflow runs it; the Apache Kafka lines also weekly, as above). Every outcome a group can have, on a broker that types groups: `e2e/tests/position_evidence.rs::every_selected_group_gets_one_outcome_in_the_signed_receipt` (CI runs it on 3.7.1, where every group is excluded; its `captured` arms need a line that types groups) |
| Consumer-group positions | Apache Kafka 3.7.1; Redpanda v26.2.4 | **unsupported** | The endpoint serves ListGroups v0–v4, which names no group type, so Logweir cannot tell a classic consumer group from any other. Every selected group is `excluded: GroupTypeNotCaptured`: never captured, never offset 0. `connection.groupTypes` is `notReady` (advisory) before the backup. | Apache Kafka 3.7.1, CI: `e2e/tests/position_evidence.rs::every_selected_group_gets_one_outcome_in_the_signed_receipt`, `e2e/tests/compat_contract.rs::the_default_broker_answers_every_capability_check`. Redpanda: `e2e/tests/compat_contract.rs::redpanda_backs_up_and_refuses_a_restore_before_it_starts` (by hand, 2026-10-10; no workflow runs it) |
| Topic IDs (which generation of a topic the backup read) | All six endpoints above | **supported** | `topic_id` before and after the engine, source `describeTopics`. | On every endpoint, the generic row, which requires the ID and that it did not change during the capture: `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies` (the four Apache Kafka lines), `e2e/tests/compat_contract.rs::confluent_platform_backs_up_restores_and_verifies`, `e2e/tests/compat_contract.rs::redpanda_backs_up_and_refuses_a_restore_before_it_starts` (by hand, 2026-10-10; no workflow runs it; the Apache Kafka lines also weekly). A recreated topic is a new generation, CI on 3.7.1: `e2e/tests/topic_ids.rs::a_recreated_topic_is_a_new_generation_and_the_same_topic_continues` |
| Topic configuration | All six endpoints above | **supported** | Coverage `captured`, with the entries the endpoint reports. A principal without DescribeConfigs on the topic gets `captureDenied` and no entries, and `connection.topicConfigsReadable` is `notReady` (advisory) before the backup. | On every endpoint, the generic row, which requires coverage `captured`: `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies` (the four Apache Kafka lines), `e2e/tests/compat_contract.rs::confluent_platform_backs_up_restores_and_verifies`, `e2e/tests/compat_contract.rs::redpanda_backs_up_and_refuses_a_restore_before_it_starts`. The denied read, on Apache Kafka 4.3.1: `e2e/tests/compat_contract.rs::the_minimum_acls_for_probe_backup_and_restore`. All four: by hand, 2026-10-10; no workflow runs it; the Apache Kafka lines also weekly |
| The broker's timestamp type and record-timestamp bound | Apache Kafka (all four lines); Confluent Platform 8.3.2 | **supported** | The receipt records the topic's timestamp type with its source. Before a restore, a `Restore` Preflight in the shape the controller renders (`restorePreflight`) answers `target.timestampBound` `ready` and quotes the bound the broker's own tool reports; during it, phase 0 records the target's timestamp type as that tool reports it (the run's `topic-preflight=` line). The rows restore a recent window, so they show the bound read and compared, and not a window the bound refuses: that arm is in process, `crates/logweir/tests/check_cli.rs` | `e2e/tests/compat_contract.rs::the_default_broker_backs_up_restores_and_verifies`, `e2e/tests/compat_contract.rs::confluent_platform_backs_up_restores_and_verifies` (by hand, 2026-10-10; no workflow runs it; the Apache Kafka lines also weekly) |
| The broker's timestamp type and record-timestamp bound | Redpanda v26.2.4 | **unsupported** | Its broker resource reports neither (it keeps them per topic). Nothing is recorded as `CreateTime` and nothing as "no bound": the type is absent, and `target.timestampBound` is `unknown` (`TimestampBoundNotReported`). | Live, the nine keys, a `Restore` Preflight's `target.timestampBound` and phase 0's own account: `e2e/tests/compat_contract.rs::redpanda_backs_up_and_refuses_a_restore_before_it_starts` (by hand, 2026-10-10; no workflow runs it). Over those nine keys, in process, on every `cargo test`: `crates/logweir/tests/check_cli.rs::a_broker_answer_without_the_timestamp_bound_is_unknown_never_no_bound`, `crates/logweir/tests/topic_preflight.rs::a_broker_that_does_not_report_its_timestamp_type_is_not_recorded_as_create_time` |

### Authentication modes

Backup, restore and verify on the `auth` profile's listeners, which CI starts
(Apache Kafka 3.7.1), unless the row says otherwise.

| `auth.mode` | Status | What the row shows | Evidence |
|---|---|---|---|
| `plaintext` | **supported** | Every broker row above. | `e2e/tests/full_drill.rs::a_full_drill_produces_a_signed_scorecard_with_real_numbers` |
| `plain` with `tls: true` (SASL/PLAIN over TLS) | **supported** | A private CA; a wrong password and a wrong CA are refused. | `e2e/tests/auth_modes.rs::plain_over_tls_backs_up_restores_and_verifies` |
| `plain` without TLS | **unsupported** | Refused by name before a socket opens (`PlainWithoutTls`, exit 3): SASL/PLAIN sends the password itself. | `e2e/tests/auth_modes.rs::plain_without_tls_is_refused_by_name_before_dialling` |
| `scramSha256` | **supported** | A wrong password is refused. Against Redpanda's own SCRAM too (a backup; a restore is the broker row's limit). | CI: `e2e/tests/auth_modes.rs::scram_sha_256_backs_up_restores_and_verifies`. Redpanda: `e2e/tests/compat_contract.rs::redpanda_authenticates_both_scram_mechanisms` (by hand, 2026-10-10; no workflow runs it) |
| `scramSha256` with `tls: true` | **supported** | A private CA; a wrong CA is refused. | `e2e/tests/auth_modes.rs::scram_sha_256_over_tls_backs_up_restores_and_verifies` |
| `scramSha512` | **supported** | Both clients, a drill, and the capability checks over SASL (the engine's SaslHandshake v1 and SaslAuthenticate v2 are checked only on a SASL connection). Against Redpanda too. | CI: `e2e/tests/scram.rs::a_drill_passes_over_scram`, `e2e/tests/scram.rs::scram_authenticates_through_the_engines_own_client`, `e2e/tests/compat_contract.rs::the_default_broker_answers_every_capability_check`. Redpanda: `e2e/tests/compat_contract.rs::redpanda_authenticates_both_scram_mechanisms` (by hand, 2026-10-10; no workflow runs it) |
| `scramSha512` with `tls: true` | **untested** | No compose listener serves SCRAM-SHA-512 over TLS. Run once by hand (PLAT-07.1, 2026-09-16, a broker with a private CA on docker-desktop). This is the mode Amazon MSK's SCRAM endpoint needs. | |
| `mtls` | **supported** | A CA-signed client certificate; a wrong CA and an untrusted client certificate are refused. | `e2e/tests/auth_modes.rs::mtls_backs_up_restores_and_verifies` |
| OAUTHBEARER, in any form (MSK IAM, an OIDC token, Entra ID) | **unsupported** | No `auth.mode` names it, and the engine's command line has no token plugin. Deferred (OD-3). | |
| GSSAPI (Kerberos) | **unsupported** | No `auth.mode` names it and no fixture serves it. | |

### Schema registries

Logweir contacts no schema registry. What it does is read the archived bytes.

| Registry | Status | What the row shows | Evidence |
|---|---|---|---|
| Records in Confluent's wire format (a zero byte and a schema id), whoever wrote them | **supported** | Detection only: the receipt flags the topic `schemaDependent` with the schema ids seen, from the archived bytes. | `e2e/tests/schema_dependency.rs::a_backup_flags_raw_framed_records_with_their_ids` |
| Karapace 6.2.3 (the `registry` profile) | **supported** | Detection only: Avro, JSON Schema and Protobuf records produced through its REST proxy are flagged with the registry's ids, with the registry stopped during the backup. | `e2e/tests/schema_dependency.rs::a_backup_flags_the_schema_dependent_topics_from_their_bytes` (by hand, 2026-10-10; no workflow runs it) |
| Confluent Schema Registry, Apicurio Registry, AWS Glue Schema Registry | **untested** | No fixture. A framing that is not Confluent's reads `notDetected`, which means "no Confluent framing seen" and never "no registry needed". | |
| A registry's contents (subjects, schemas, compatibility settings), on any registry | **unsupported** | Never contacted and never captured ([stability.md](stability.md), Never #2). A restore brings back the bytes; `schemaDependent` on the receipt is the warning that the schemas are not in the archive. | |

### Archive backends

| Backend | Status | What the row shows | Evidence |
|---|---|---|---|
| MinIO `RELEASE.2025-09-07T16-13-09Z` (the project's rebuild, `third_party/minio-mirror/`) | **supported** | Every e2e row archives to it. Conditional create holds: a second run of one execution is refused before the engine starts. Its fixture buckets are unversioned, so no manifest version is pinned. A backup whose id, and so every archive key, spells an S3 credential code (`expiredtoken-…`) is a backup like any other: MinIO's `404 NoSuchKey` echoes the key, and Logweir reads the answer's own code (`e2e/tests/compat_contract.rs::a_backup_whose_id_spells_a_credential_code_is_a_backup`, by hand, 2026-10-10; on every `cargo test`, `crates/logweir-store/tests/options.rs::a_real_404_about_an_object_named_like_a_credential_code_is_not_found`). | `e2e/tests/mvp_demo.rs::mvp_demo_backs_up_restores_at_a_point_in_time_and_verifies`, `e2e/tests/backup_argv.rs::a_backup_over_a_set_an_earlier_engine_run_wrote_is_refused_before_the_engine` |
| SeaweedFS 4.48 (the `objectstore` profile), the maintained choice | **supported** | A backup, a restore and both verifiers with every object on it; a second claim of one execution is refused (`ExecutionAlreadyClaimed`); on a versioned bucket the receipt pins the manifest's version id, and on an unversioned one it carries no pin. | `e2e/tests/compat_contract.rs::seaweedfs_takes_a_backup_a_restore_and_refuses_a_second_claim` (by hand, 2026-10-10; no workflow runs it) |
| RustFS 1.0.0 | **untested** | No fixture here. Run once by hand (PROD-01.5, 2026-09-29, a private container): 19 of 19 protocol checks, Logweir's store layer 6 of 6, the receipt path `pass`. | |
| versitygw v1.8.0 | **untested** | No fixture here. Run once by hand, the same way: 19 of 19, the receipt path `pass`, the store layer 5 of 6. The sixth: it answers an unknown access key `404 XAdminUserNotFound`, which Logweir read as a missing object. Since PROD-01.2 Logweir reads it as a refused credential, by the answer's own `<Code>` and never by a word found in its text, and the store layer is 6 of 6 against the same image (by hand, 2026-10-10). | |
| AWS S3 | **untested** | `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` | |
| Google Cloud Storage, Azure Blob Storage | **untested** | `[UNVERIFIED — native conditional create in object_store, not run against either provider]` | |
| A local directory (`backend: filesystem`, the standalone CLI) | **untested** | In-process tests claim an execution on a temporary directory. No row runs the engine against a directory archive. | |
| Object Lock retention, read back through Logweir, on any backend | **unsupported** | Logweir reads no retention back, so a scorecard's evidence block says `immutable: false` on every backend and never guesses from a bucket's configuration. The stores answer the readback to the AWS CLI (PROD-01.5: all four above, GOVERNANCE mode). | `crates/logweir-store/tests/storage.rs::object_lock_readback_reports_no_proof_rather_than_guessing` |
| A store that ignores `If-None-Match`, or `AWS_CONDITIONAL_PUT=disabled` | **unsupported** | Refused, never silently accepted: every backup exits 4 `ExecutionClaimUnproven` before data is written, and a destination with `writeProbe` on reports `ConditionalCreateUnsupported` beforehand. | `crates/logweir/tests/check_marker_create_only.rs::a_store_without_conditional_put_is_not_ready`, `crates/logweir-store/tests/storage.rs::a_backend_without_conditional_put_reports_create_only_enforced_false` |

### Managed Kafka providers

**No managed provider has been run against.** Workers create no cloud resource
(OD-4); only AWS is funded, per session, with the owner's approval. Each row
says how Logweir would reach the provider and what blocks a real row.

| Provider | Status | How Logweir would reach it | What blocks a row |
|---|---|---|---|
| Amazon MSK (provisioned), SASL/SCRAM | **untested** | `scramSha512` with `tls: true`, port 9096, the Secrets Manager credential | No cluster until the owner funds a session. The mode itself has no repeatable row either (SCRAM-SHA-512 over TLS, above). |
| Amazon MSK (provisioned), mutual TLS | **untested** | `mtls`, port 9094 | No cluster. |
| Amazon MSK with IAM authentication, and MSK Serverless | **unsupported** | Not reachable: both need OAUTHBEARER with a SigV4 token, and MSK Serverless accepts nothing else. | Deferred (OD-3). |
| Confluent Cloud | **untested** | `plain` with `tls: true`: the API key as the user name, the API secret as the password, the public CA | No account. Not known: whether it serves the request versions the engine sends. The capability checks answer that on first contact. |
| Redpanda Cloud | **untested** | `scramSha256` or `scramSha512` with `tls: true` | No account. If it serves what Redpanda v26.2.4 serves, it is a backup source only; that is a projection from the local row, not a measurement. |
| Aiven for Apache Kafka | **untested** | `mtls` with the project CA, or `scramSha256` / `scramSha512` with `tls: true` | No account. |
| Azure Event Hubs (the Kafka endpoint, port 9093) | **untested** | `plain` with `tls: true`: the user name `$ConnectionString`, the connection string as the password, the public CA | No namespace. It is not a Kafka broker, and whether it serves the engine's requests is not known. Its Entra ID sign-in is OAUTHBEARER, which is unsupported. |

<!-- compatibility:end -->

### Capability checks

A backup or restore Preflight, and `logweir check run` on the same plan, ask
the endpoint itself what it can do before the operation starts. The controller
lists these rows in the plan (`capabilityChecks`); a runner emits exactly the
ones listed. Each names what is missing and what to do instead.

| Check | Operation | What it asks | When the capability is missing | The fallback it names |
|---|---|---|---|---|
| `connection.engineProtocol` | backup | Does the endpoint serve every request version the engine sends to read from it (Metadata v9, ListOffsets v5, Fetch v11, DescribeConfigs v1; on a SASL connection also SaslHandshake v1 and SaslAuthenticate v2)? The engine sends fixed versions and never negotiates. | `notReady`, **blocking**, `EngineProtocolUnsupported`: each request, with the range the endpoint serves. | Back up from an endpoint that serves them. |
| `target.engineProtocol` | restore | The same for what the engine sends to write (Metadata v9, Produce v8, and the SASL pair). | `notReady`, **blocking**, `EngineProtocolUnsupported`. | Restore the archive into a cluster that serves them; an endpoint that cannot be a target can still be a source. |
| `connection.topicConfigsReadable` | backup | Does DescribeConfigs answer for every selected topic, as this principal? | `notReady`, advisory, `TopicConfigsNotReadable`, naming the topics. The backup runs. | Grant DescribeConfigs on the topic, or accept a point whose configuration is `captureDenied` and whose timestamp type is not recorded. |
| `connection.groupTypes` | backup | Does the endpoint's group listing name each group's type (ListGroups v5)? | `notReady`, advisory, `GroupTypesNotListed`. The backup runs. | Back up from an endpoint that serves ListGroups v5, or select no group and export positions with the endpoint's own tooling. |

An answer that could not be read is `unknown` (`ApiVersionsNotObserved`), never
`ready`. The served ranges are read from each broker's own ApiVersions answer
on a real connection; the engine's versions come from a table held to the
pinned engine source by `cargo xtask check-drift`. Operator reference:
[kubernetes.md §21](kubernetes.md).

**The three rows that read ApiVersions answer for a cluster only when every
broker of it answered.** The engine may be sent to any broker, so the check
reads the cluster's broker list, connects to every broker on it and to every
bootstrap address the connection names, and waits for each one's answer
inside its budget (at most 10 s).

| The cluster | The row |
|---|---|
| Every listed broker and every bootstrap address answered | Its own verdict, with the fact `brokersAnswered: N of N` (distinct brokers, never connections). Its message says "all N brokers of this endpoint serve …". |
| The brokers' answers differ (a rolling upgrade) | Judged on what every one of them serves, and the message says they differ. One broker that does not serve Produce v8 makes `target.engineProtocol` `notReady`. |
| A broker the cluster lists did not answer in time | `unknown`, `ApiVersionsNotObserved`, never `ready`: "2 of 3 broker(s) the cluster lists answered …; no answer from broker 3 (host:port)". Re-run when the broker is back. |
| A bootstrap address did not answer | `unknown` the same way, naming the address, even when every listed broker answered: a stopped broker the cluster has dropped from its list is still a broker the connection names. |

Measured on the `cluster3` profile (three Apache Kafka 3.7.1 brokers) with one
broker's process frozen, and required to be `3 of 3` from three bootstrap
addresses and from one, five rounds each:
`e2e/tests/compat_contract.rs::a_three_broker_cluster_is_answered_by_every_broker_or_not_at_all` (by
hand, 2026-10-10; no workflow runs it). On every `cargo test`, against
librdkafka's in-process cluster of three:
`crates/logweir-kafka/tests/api_versions.rs::a_silent_broker_makes_the_view_partial_and_never_an_answer`
and
`crates/logweir-kafka/tests/api_versions.rs::every_broker_of_a_three_broker_cluster_answers_from_one_address_or_three`.

Two limits. **A broker the cluster no longer lists is not asked:** through
bootstrap addresses that all answer, a cluster that has dropped a stopped
broker lists two, both answer, and the row says `2 of 2`. And **the row is
about request versions, not about a restore into several brokers**: every
whole-path row above is one node.

### Minimum permissions

Measured on the `acl` profile (Apache Kafka 4.3.1, `StandardAuthorizer`, a
SCRAM-SHA-512 principal that is not a super user) by granting exactly the list
and then removing one grant at a time:
`e2e/tests/compat_contract.rs::the_minimum_acls_for_probe_backup_and_restore`
(by hand, 2026-10-10; no workflow runs it).

| Operation | The ACLs it needs | With one removed |
|---|---|---|
| Probe (`cluster-probe`, Test connection) | None beyond authenticating. | Nothing to remove: with no ACL at all for the principal, the probe reads the cluster id. |
| Backup | `Read` on each source topic (it implies `Describe`), and `DescribeConfigs` on each source topic. | Without `Read`: exit 1, the engine fails, no receipt. Without `DescribeConfigs`: exit 0, and the receipt says `captureDenied` with no timestamp type. |
| Restore into new topics (a scratch drill) | On the target: `Create`, `Write`, `Read` and `Delete` on the drill's topic prefix, `DescribeConfigs` on the cluster, and `Describe` on the scratch marker topic. | Without `Create`: exit 1, the topic is not created, nothing is left. Without `Write` or `Read`: exit 1, the created topic is left behind, **and the run says nothing about it**. Without `DescribeConfigs` on the cluster: exit 1, naming the grant. Without `Describe` on the marker: exit 3, refused by the guard. **Without `Delete`: exit 0 and a signed `pass`, the scratch topic is left behind, and the run says so**: a warning names the topic, the summary line repeats it beside `outcome pass`, and the signed teardown attestation records it. It does not say why: nothing names the missing grant. |

**Give the restore identity `Delete`, and read the summary line of a restore
that passed.** A leftover scratch topic blocks the next rehearsal that would
use its name. Two things are open (tracker row FX-44): a restore that FAILS
leaves its topic without a word, and the teardown warning of one that passes
does not name the grant. The row above requires both as measured, so this
paragraph is held to the product.

| Restore without | What the run prints about the topic it leaves |
|---|---|
| `Delete` (exit 0, `pass`) | `teardown left 1 scratch topic behind on the target cluster: <topic>` (WARN); `… — outcome pass — … — teardown left 1 scratch topic behind (<topic>)` (the summary line); `teardown-key=logweir/drills/<run>.teardown.json` (the signed attestation) |
| `Write` or `Read` (exit 1) | Nothing |

The restore row is a scratch drill of a topic whose backup recorded no
configuration overrides, into a broker on `CreateTime`. Three cases need
`DescribeConfigs` on the target topics as well (FX-4, measured 2026-10-05): a
topic whose backup recorded overrides, an existing mapped topic, and a broker
on `LogAppendTime`.

Consumer-group positions additionally need `Describe` on each selected group:
a group hidden from the principal is recorded as `failed: NotVisibleToPrincipal`,
never as absent
(`e2e/tests/position_evidence.rs::a_group_hidden_from_the_backup_principal_is_never_absent`).
Archive permissions are the object store's, and are not in this table.

### Advertised addresses

A Kafka client reaches the bootstrap address first and then the addresses the
cluster advertises. When the second kind cannot be reached from where Logweir
runs, the probe still passes and nothing else does. Measured on a Confluent
Platform listener that advertises a dead address
(`e2e/tests/compat_contract.rs::an_unreachable_advertised_address_is_not_a_reachable_cluster`):

| Step | Result |
|---|---|
| `logweir cluster-probe` | exit 0, `reachable=true`, the cluster id |
| The readiness check | `connection.authenticated` is `notReady` (`BrokerUnreachable`), and its message says the bootstrap address answered and the advertised brokers did not. The capability rows are `unknown`, blocked behind it. |
| `logweir backup run` | exit 1 after the metadata timeout, no receipt |

## The floors, stated first

| Floor | Version | What it gates |
|---|---|---|
| **Warning-mechanism floor** | `kafka-backup` **0.16.0** | The `Ignoring unknown config key ...` message Logweir parses off the engine's streams. Below it, a rendered key the engine dropped fails **silently** instead of surfacing in `engine.levers.unknown_key_warnings`. |
| **Full-drill floor** | `kafka-backup` **0.21.0** | The full drill as shipped. This is the version every vendored struct and CLI behaviour was first verified against. |
| **The pin** | `kafka-backup` **0.23.3+logweir.2** | Not a floor: **Logweir's build** of OSO's 0.23.3 source with Logweir's patch folder (PROD-00.2, OD-3; `third_party/kafka-backup-build.env`; build 2 adds FX-21's patch 0002, the manifest records every topic's replication factor), the engine the runner image carries for linux/amd64 and linux/arm64. `logweir doctor` accepts exactly this version, and names OSO's own 0.23.3 (pinned by `third_party/kafka-backup-binary.digest` since PROD-00.3f, and the one-release rollback) as OSO's release. The vendored structs are drift-gated against the same source. |

Anything below the full-drill floor is reported **`unsupported (lever-absent)`**
— an engine that predates a lever Logweir needs. **That is never a fault Logweir
raises against that engine or against an operator that defaults to it.**

## Architectures

| Image or binary | linux/amd64 | linux/arm64 |
|---|---|---|
| Runner image (Logweir's engine build, `logweir`, `logweir-retention`) | yes | **yes, since PROD-00.2** |
| Controller, console, UI images | yes | yes |
| OSO's released engine image (the rollback, the compose stack's seeding engine) | yes | no: OSO publishes none |
| Standalone CLI archives (`release.yml`) | `x86_64-unknown-linux-gnu` | `aarch64-unknown-linux-gnu`, and `aarch64-apple-darwin` |

Before PROD-00.2 the runner was amd64-only, because its engine was OSO's binary.
Evidence for the arm64 runner is in the rows below and in the
[decision record](to-do/decisions/PROD-00-engine-route.md) §13: `scripts/check-image.sh`
on both platforms, and the parity runs against OSO's binary.

## The five outcomes

| Outcome | Meaning |
|---|---|
| `pass` | The full drill ran and passed. |
| `pass-degraded` | The drill passed at a reduced integrity level (e.g. `consume-only`, because the KBAK decoder returned `Unsupported`). |
| `fail(reason)` | The drill ran and did not pass, for the stated reason. The weekly job also records these reasons, each derived from what ran (`scripts/engine-matrix-outcome.sh`): `fail(seed)` when the stack could not be seeded with that engine; `fail(build)` when `logweir` did not build; `fail(floor-not-enforced)` when a drill accepted an engine below the full-drill floor, or failed without Logweir's floor refusal; and `fail(setup)` when a step did not run, the tag did not resolve to a digest whose revision label is the tag's commit, or the broker the stack ran is not the one the row declares. |
| `fail(lever-not-honoured)` | The engine accepted a lever and did not act on it. The deleted-segment positive control catches this: a **non-oldest** segment is removed from the live archive, so `validate-restore` must report it unrestorable and the drill must block at phase 5 with `outcome: preflight-failed` and exit 2. A version where the drill sails past lands here. The control is the e2e test `a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard`. |
| `unsupported(lever-absent)` | The engine predates a lever Logweir needs. Reported, never treated as a fault. |

## Rows

| Engine version | Image digest | Outcome | Evidence |
|---|---|---|---|
| **0.21.0** | `sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317` | **`pass`** | **The engine floor: `0.21.0` is the minimum supported `kafka-backup` version and the version this row was run against; it was also the shipped pin until PROD-00.3f (2026-10-07). [install.md](install.md) leads with it.** Full drill, 2026-09-05, against the compose stack (Kafka 3.7.1 KRaft + MinIO) via `scripts/demo.sh`: `outcome: pass`, `integrity: byte-fingerprint/pass`, 150/150 records reconciled, `pass_rate_measured: 1.0`, `rto_excluding_preflight_seconds: 6`, `rpo_seconds: 9`, all three objectives met, signature VALID under both `logweir drill verify` and `docs/verify_scorecard.py`. `header_preflight: honoured`. |

| **0.23.3+logweir.2** | build inputs `sha256:2bca49d72b92fc9d96d69ff2a8b64faef2c837bfbff8ba92326723f549197db8` (not an image digest) | **`pass`** | **Logweir's build 2 of 0.23.3, the engine the images ship since FX-21: patches 0001 and 0002.** On 2026-10-08, on the compose stack (slot 1, Kafka 3.7.1 KRaft + MinIO), built natively for macOS arm64 with `scripts/engine-source.sh build` and run natively: the demo drill `outcome: pass`, `integrity: byte-fingerprint/pass` 150/150, both readers VALID; `full_drill` 15/15, G-PITR 1/1 and PROD-01.1's record-semantics rows 11 (3 ignored) with the contract asserted. The suites and record-semantics files compared SAME against build 1 run the same way, and the demo, suites and record-semantics files SAME against OSO's 0.23.3 binary (container route) in the same session. FX-21's three-topic `cluster3` row: the manifest records every topic's replication factor (3 of 3; OSO's 0.23.3 and build 1 record 1 of 3). Since then the runner image's linux/arm64 engine has run both suites on four broker lines (PROD-01.5c, "Record semantics and topic identity on each line" below) and the hand-run rows of the [compatibility contract](#the-compatibility-contract) (PROD-01.2), and CI's `e2e` job builds this engine from the vendored source for linux/amd64 and runs it on every pull request. Not yet run: the published linux/amd64 runner image as a container. |
| **0.23.3+logweir.1** | build inputs `sha256:6385b2d3aecb9d107010b14362bb60db756e6774b2181cd2273d7c6f92ed9af3` (not an image digest) | **`pass`** | **Logweir's build of 0.23.3, the engine the images shipped from PROD-00.2 until FX-21; `doctor` refuses it since.** On 2026-10-08, on the compose stack (Kafka 3.7.1 KRaft + MinIO), built for linux/arm64 (run natively) and linux/amd64 (run under emulation): the demo drill `outcome: pass`, `integrity: byte-fingerprint/pass` 150/150, `header_preflight: honoured`, objectives met, both readers VALID; `full_drill` 15/15, G-PITR (`pitr_boundary`) 1/1, and PROD-01.1's record-semantics rows 8/8 with the contract asserted. All of it compared SAME against OSO's 0.23.3 binary in the same session, and a deliberately modified build did not ([decision record](to-do/decisions/PROD-00-engine-route.md) §13.5). |
| **0.23.3** | `sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db` | **`pass`** | **OSO's release of the shipped source, and the one-release rollback since PROD-00.2; the pin from PROD-00.3f until then.** Full drill, 2026-10-08, against the compose stack (slot 4, Kafka 3.7.1 KRaft + MinIO, the `linux/amd64` image under emulation), `scripts/demo.sh` steps 4–6: `outcome: pass`, `integrity: byte-fingerprint/pass`, 150/150 records reconciled, `header_preflight: honoured`, objectives met, signature VALID under both `logweir drill verify` and `docs/verify_scorecard.py`. The same day CI's e2e command passed (177 tests, PROD-01.1's contract asserted on 0.23.3), and the record-semantics, G-PITR, FX-1, FX-7 and full-drill rows passed on Kafka 4.3.1. An archive 0.21.0 wrote drills `pass` with 0.23.3, and the reverse ([decision record](to-do/decisions/PROD-00-engine-route.md) §12.5). |

That is **one green row at the declared floor**, which is the release
requirement, and one at the pin. These two are the rows that have been run
by hand; the weekly job's rows are below.

## Versions with no row yet, and why

Listed so that "absent from the matrix" is never mistaken for "known bad", and
so nobody quotes a projected verdict as a tested one.

| Engine version | Status | Note |
|---|---|---|
| **0.23.0 – 0.23.2** | **not run; superseded within the minor** | Released 2026-09-29 and 2026-10-06. PROD-00.3f read them from source on the way to 0.23.3 ([decision record](to-do/decisions/PROD-00-engine-route.md) §12) and ran only 0.23.3. `doctor` refuses them. `kafka-backup-operator` 1.4.0 and 1.4.1 link 0.23.0 and 0.23.1. |
| **0.22.0** | **evaluated, not the shipped pin; a weekly row is declared** | Released 2026-09-07; the default of `strimzi-backup-operator` v0.3.0–v0.4.0. PROD-00.1 ran it against the compose stack (Kafka 3.7.1, the `linux/amd64` image `sha256:1c3432c9399dbdd59fea7b6cf386135656a73bb6dd60841212ddbfe5ba26b1fe` under emulation, 2026-09-29). The demo drill scored `outcome: pass` and `integrity: byte-fingerprint/pass` (150/150), and both verifiers returned VALID. `just pitr` passed (six of nine records, boundary included). The `.kbak` fixture tests passed on 0.22.0 bytes. `doctor` refuses it, because it accepts exactly the pin. The pin moved past it to 0.23.3 (PROD-00.3f; OD-3). Its `path_style` change does **not** make VirtualHosted addressing with a custom endpoint possible, so that refusal stays. It also newly treats an `http://` endpoint as `allow_http: true`, which Logweir refuses before an engine document is rendered. |
| 0.20.x | **unsupported by the full-drill floor** | Matrix compatibility probes do not override the 0.21.0 runtime floor. The weekly job runs v0.20.0 as a below-floor row. |
| 0.19.x | **unsupported by the full-drill floor** | Same floor as 0.20.x. v0.19.2 is the `kafka-backup-core` that `kafka-backup-operator` 1.3.0 links as a library (its `Cargo.lock`). v0.19.1 is the default of `strimzi-backup-operator` v0.2.22–v0.2.25; v0.2.21 defaulted to v0.19.0. Engines before 0.21 write no segment sha256, so a drill over an archive one of them wrote reports `integrity.result: partial`, never `pass`. Measured on 2026-09-29 over a v0.19.2 archive, drilled with the pinned engine: `outcome: fail-integrity`, `integrity: byte-fingerprint/partial`, exit 2, and both verifiers VALID. Logweir refuses to drive a below-floor engine itself: v0.19.2 as the restore engine exits 1 with "ignored the config key `restore.header_preflight` that logweir rendered; this tag is below the declared floor". The weekly job runs v0.19.2 and v0.19.1 as below-floor rows, which record `unsupported (lever-absent)` and are never a fault. |
| 0.16.0 – 0.18.x | **unsupported**, by floor | Below the full-drill floor; only the unknown-key warning mechanism works. |
| < 0.16.0 | **unsupported (lever-absent)**, by floor | The warning mechanism this project depends on does not exist. |

`strimzi-backup-operator` has defaulted to engine **v0.22.0** since its v0.3.0
(2026-09-07, `DEFAULT_BACKUP_IMAGE` in its `src/engine.rs`), through v0.4.0
(2026-10-06). An earlier revision of this page said its default was v0.19.1,
which is true only of its v0.2.22–v0.2.25 releases. `kafka-backup-operator`
1.3.0 links `kafka-backup-core` 0.19.2; its 1.4.0, 1.4.1 and 1.4.2 (2026-10-06
and 07) link 0.23.0, 0.23.1 and 0.23.3.

## Authentication modes, and what each one has actually been run against

The status of each mode is in the
[compatibility contract](#authentication-modes); this table is what each client
is configured with. The recorded engine rows above are **PLAINTEXT** drills.
Authentication test coverage is separate from those results: the Compose stack has SCRAM
listeners and `e2e/tests/scram.rs` exercises both clients with real brokers
when the `e2e` feature and its infrastructure are enabled, and the `auth`
compose profile (PROD-01.5, extended by PROD-01.3) adds SASL/PLAIN over TLS,
SCRAM-SHA-256 with and without TLS, and mTLS listeners behind a private CA,
which `e2e/tests/auth_modes.rs` drives end to end.

| `auth.mode` | Logweir's client | The engine's client | Exercised |
|---|---|---|---|
| `plaintext` (default) | `security.protocol: PLAINTEXT` | no `security:` block rendered — the engine's own default | **Yes**, by the 0.21.0 row above and by every e2e drill. |
| `scramSha512`, `tls: false` | `security.protocol: SASL_PLAINTEXT`, `sasl.mechanism: SCRAM-SHA-512` | `security_protocol: SASL_PLAINTEXT`, `sasl_mechanism: SCRAM-SHA512` | **Automated e2e coverage exists.** `e2e/tests/scram.rs` exercises Logweir's librdkafka client, engine-backed backups and a drill against the Compose SCRAM listeners in CI; its in-cluster pod row needs Kubernetes and is skipped there. PROD-01.2 added the capability checks over this listener, and Redpanda's own SCRAM-SHA-512 (`e2e/tests/compat_contract.rs`). |
| `scramSha512`, `tls: true` | `security.protocol: SASL_SSL` | `security_protocol: SASL_SSL` | **Untested: run once by hand, on docker-desktop, and no repeatable row.** The compose stack's TLS listeners (the `auth` profile) serve PLAIN and SCRAM-SHA-256, not SCRAM-SHA-512, so no automated e2e row covers this pair; PLAT-07.1's live run (2026-09-16, an in-namespace broker with a private CA) succeeded with `KafkaCluster.spec.auth.tlsCa` and failed at the handshake — never a plaintext dial — without the CA or with the wrong one. `auth.tlsCa` hands one CA file to both trust stores (Global Constraint 29; [kubernetes.md](kubernetes.md) §20.2). |
| `scramSha256`, `tls: false` or `true` | `SASL_PLAINTEXT` / `SASL_SSL`, `sasl.mechanism: SCRAM-SHA-256` | `security_protocol: SASL_PLAINTEXT` / `SASL_SSL`, `sasl_mechanism: SCRAM-SHA256` (one hyphen) | **Automated e2e coverage exists (PROD-01.3).** `e2e/tests/auth_modes.rs` backs up, restores and verifies over the `auth` profile's SCRAM256 listener and, over TLS with the private CA, its SASL_SSL listener; a wrong password and (TLS) a wrong CA are refused. Compose slot 4, Kafka 3.7.1, engine 0.23.3, 2026-10-08. Redpanda's own SCRAM-SHA-256 authenticates both clients for a backup (PROD-01.2). |
| `plain`, `tls: true` only | `SASL_SSL`, `sasl.mechanism: PLAIN` | `security_protocol: SASL_SSL`, `sasl_mechanism: PLAIN` | **Automated e2e coverage exists (PROD-01.3)**, the same file and run: backup, restore and verify over the SASL_SSL listener; a wrong password and a wrong CA refused. **`plain` without TLS is refused, never dialled** — `refusal-reason=PlainWithoutTls` (exit 3) at the runner, the same named reason at the controller, and the CRD's admission rule — because SASL/PLAIN sends the password itself. |
| `mtls`, `tls: true` only | `SSL`, `ssl.certificate.location` / `ssl.key.location` | `security_protocol: SSL`, `ssl_certificate_location` / `ssl_key_location` | **Automated e2e coverage exists (PROD-01.3)**, the same file and run, over the `auth` profile's MTLS listener (`ssl.client.auth=required`): backup, restore and verify with the CA-signed client certificate; a wrong CA and an untrusted (self-signed) client certificate refused. The key must be unencrypted PEM (PKCS#8, PKCS#1 or SEC1): the engine's loader takes no passphrase. |
| OAUTHBEARER / MSK IAM | `AuthConfig::Token` — constructing it returns an error | not rendered. The engine's YAML offers only PLAIN, SCRAM-SHA-256/512 and GSSAPI; other mechanisms need a programmatic plugin its CLI does not expose ([PROD-00.1](to-do/decisions/PROD-00-engine-route.md) C9) | **Deferred** (OD-3, 2026-10-07), until a buyer asks. |

Every credentialed mode also passes through the credential **binding**: a
runner refuses a projected password or client key whose Secret does not carry
the `logweir-binding` of the connection it was projected for
(`CredentialBindingMismatch`, before any client exists) —
[kubernetes.md](kubernetes.md), "Client authentication modes".

## Managed providers: what is not verified

**No managed provider has been run against.** The table, with how Logweir
would reach each provider and what blocks a real row, is in the
[compatibility contract](#managed-kafka-providers); every provider there is
untested or unsupported. Confluent Cloud and Azure Event Hubs were unsupported
before PROD-01.3, because both need SASL/PLAIN. Local emulation does not
certify a hosted provider.

**Nothing here is an MSK row.** `[UNVERIFIED — needs an MSK cluster]`: MSK
holds SCRAM credentials in AWS Secrets Manager and requires TLS on its
`:9096` SASL endpoint, so the sentence that would verify it is *"point
`auth.tls: true` and `bootstrap_servers` at an MSK cluster's
`*.kafka.<region>.amazonaws.com:9096` endpoint, project the Secrets Manager
value into `LOGWEIR_TARGET_PASSWORD`, and record that `drill run` reaches
phase 2"*. It needs a provisioned cluster, which Global Constraint 17 forbids,
so it is recorded as **blocked, never as closed** — and the two `tls: true`
claims above are deliberately about rendered bytes and not about a handshake.

## Broker versions (Apache Kafka)

The engine rows above were run against Apache Kafka **3.7.1**, a line Apache
no longer supports and MSK stopped supporting on 2026-09-01. Each row below
names its broker line. All were run on 2026-09-29 against the compose stack's
single-node KRaft broker, engine 0.21.0 (the pin on that date), plaintext, MinIO,
each line on its own compose slot (`e2e/compose/stack-env.sh --slot N --kafka
LINE`, [`e2e/README.md`](../e2e/README.md)). Three paths per line: the demo
drill (`scripts/demo.sh`), `just pitr` (the G-PITR boundary row) and the
receipt path (`just mvp-demo`: `logweir backup run`, the receipt verified by
both readers, a point-in-time `restore run` into new topics, the scorecard
verified by both readers).

| Broker (Apache Kafka) | Image digest | Engine | Demo drill | `just pitr` | Receipt path | Status |
|---|---|---|---|---|---|---|
| **3.7.1** | `sha256:ed74d7d115968d5e8b00ba6822ac6a384cbaaf54ca38991828647000d7089b68` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | **legacy** — still the default fixture (`e2e/compose/.env`) |
| **3.9.2** | `sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | supported line, measured |
| **4.1.2** | `sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | supported line, measured |
| **4.3.1** | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | supported line, measured |

The digests are the pins: `stack-env.sh --kafka LINE` runs exactly these
images (`KAFKA_IMAGE`), and a test fails if the two disagree.

No row yet: **4.0.x** and **4.2.x** (supported by Apache, not run). The rows
with authentication, another endpoint or another object store are in the
[compatibility contract](#the-compatibility-contract). Three brokers (the
`cluster3` profile, [`e2e/README.md`](../e2e/README.md)) have rows of their own
for replication factors and group listing, and no whole-path row.

**The engine does not negotiate protocol versions, and 4.x accepts what it
sends.** `kafka-backup` 0.21.0 sends every request at a fixed version and never
sends ApiVersions (`crates/kafka-backup-core/src/kafka/client.rs:588-611` in the
v0.21.0 source; any API not in that table goes out at version 0). The 0.23.3 pin
sends the same versions: the table is byte-identical at `client.rs:625-648` of
the tarball now vendored. Kafka
4.0 removed old versions (KIP-896). Measured on each line with
`kafka-broker-api-versions.sh`, every version the engine can send is inside the
broker's range; 4.x raised the floors below them:

| API | Engine sends | 3.7.1 | 3.9.2 | 4.1.2 | 4.3.1 |
|---|---|---|---|---|---|
| Produce | v8 | v0–v10 | v0–v11 | v0–v13 | v0–v13 |
| Fetch | v11 | v0–v16 | v0–v17 | v4–v18 | v4–v18 |
| ListOffsets | v5 | v0–v8 | v0–v9 | v1–v10 | v1–v11 |
| Metadata | v9 | v0–v12 | v0–v12 | v0–v13 | v0–v13 |
| OffsetCommit | v5 | v0–v9 | v0–v9 | v2–v9 | v2–v10 |
| OffsetFetch | v5 | v0–v9 | v0–v9 | v1–v9 | v1–v10 |
| FindCoordinator | v2 | v0–v4 | v0–v6 | v0–v6 | v0–v6 |
| DescribeGroups | **v0** (the `_ => 0` fallback) | v0–v5 | v0–v5 | v0–v6 | v0–v6 |
| ListGroups | v2 | v0–v4 | v0–v5 | v0–v5 | v0–v5 |
| CreateTopics | v5 | v0–v7 | v0–v7 | v2–v7 | v2–v7 |
| DeleteRecords | v1 | v0–v2 | v0–v2 | v0–v2 | v0–v2 |
| DescribeConfigs | **v1** (the 4.x floor) | v0–v4 | v0–v4 | v1–v4 | v1–v4 |
| IncrementalAlterConfigs | v1 | v0–v1 | v0–v1 | v0–v1 | v0–v1 |
| SaslHandshake / SaslAuthenticate | v1 / v2 | v0–v1 / v0–v2 | same | same | same |

On 4.3.1 the broker's own request log (`kafka.request.logger` at DEBUG) showed
the engine (client id `kafka-backup`) sending exactly Metadata v9, ListOffsets
v5, Fetch v11, DescribeConfigs v1 and Produce v8 on both the demo drill and the
receipt path, all accepted, and no ApiVersions; Logweir's own client negotiated
(ApiVersions v3, then Fetch v16, Metadata v13) and created the target topics
itself. The engine's CreateTopics, consumer-group APIs (FindCoordinator,
OffsetFetch, OffsetCommit, ListGroups, DescribeGroups), DeleteRecords,
IncrementalAlterConfigs and, on these plaintext runs, SASL requests were never
sent, so they are in range but unexercised. **The risk is structural:**
DescribeConfigs already sits on the 4.x floor and DescribeGroups on v0, and a
future release that raises either floor fails the engine with an unsupported
version instead of a downgrade. So do the table's DescribeAcls, CreateAcls and
DeleteAcls entries (v1, the 4.x floor), which 0.21.0 never sends. That route
(negotiate, or pin higher) belongs to PROD-00.1's capability table.

### Record semantics and topic identity on each line

The table above is the drill, G-PITR and the receipt path. Two suites state
finer contracts, and both were first measured on 3.7.1: PROD-01.1's record and
transaction semantics (`e2e/tests/record_semantics.rs`,
[decision record](to-do/decisions/PROD-01.1-record-semantics.md)) and
PROD-01.4's topic identity (`e2e/tests/topic_identity.rs`,
[decision record](to-do/decisions/PROD-01.4-topic-identity.md)). PROD-01.5c ran
both on every line on 2026-10-10 (UTC), with the suites and the product code as
at main `64b66a15` (branch commit `fa7a9b0b`), one line at a time on compose
slot 2 (`stack-env.sh --slot 2 --kafka LINE --profiles auth`: the digest-pinned
images above, each version read back from the running broker), single-node
KRaft, plaintext, MinIO.

The engine was the one the images ship, `0.23.3+logweir.2`, run from the
published linux/arm64 runner image
(`vladyslavhaina/logweir@sha256:4f082ae8a1ee22c89bbcc8518e9c95027e8ecdacd0c1b915e8aed2a6aec42844`,
built from main `739f17c5`; its `/etc/logweir/engine-identity` names build inputs
`sha256:2bca49d7…`). That is the record-semantics suite's `CONTRACT_ENGINE`, so
every row asserted its contract, and the outcome files name that engine. On any
other engine those rows record an outcome and assert nothing.

| Broker | Record semantics, default set | Topic identity, `--include-ignored` | Lost acknowledgement (`--ignored`, one sample) | Killed restore (`--ignored`, one sample) |
|---|---|---|---|---|
| 3.7.1 (the baseline, same session) | 12 passed, 0 failed | 53 passed, 0 failed | 2 batches resent: 2,000 duplicates, 1,000 missing; engine exit 1; Logweir exit 1, nothing signed | the engine outlived the kill and wrote all 60,000 |
| 3.9.2 | 12 passed, 0 failed | 53 passed, 0 failed | 3 batches resent: 3,000 duplicates, 4,000 missing; engine exit 1; Logweir exit 1, nothing signed | the engine outlived the kill and wrote all 60,000 |
| 4.1.2 | 12 passed, 0 failed | 53 passed, 0 failed | 3 batches resent: 3,000 duplicates, 10,000 missing; engine exit 1; Logweir exit 1, nothing signed | the engine outlived the kill and wrote all 60,000 |
| 4.3.1 | 12 passed, 0 failed | 53 passed, 0 failed | 3 batches resent: 3,000 duplicates, 8,000 missing; engine exit 1; Logweir exit 1, nothing signed | the engine outlived the kill and wrote all 60,000 |

**No line diverges from 3.7.1.**

- **Record semantics.** The 12 rows are PROD-01.1's eight (transactions, the
  three non-monotonic `CreateTime` rows, `LogAppendTime`, record shapes,
  compaction, topic recreation) and the four complete-coverage rows of PROD-08.1
  and 08.1a, which share the file. Each of the eight asserts its exact set of
  divergences and Logweir's own verdict, as PROD-01.1 stated them from 3.7.1,
  and proves its check can fail on a mutated output. The 13 outcome files also
  compare the same in semantics between 3.7.1 and each other line: record
  counts, the configurations the manifest recorded, every divergence class and
  count, and Logweir's exit, outcome and count bound.
- **Topic identity.** 33 pure tests, the 19 live rows (the retention row c10
  included) and the engine-deadline row. Every live row's four verdicts with
  their signals, its classification and its ground truth (whether the
  broker's own topic ID changed) are equal on all four lines and equal to
  PROD-01.4's measurement:
  6 of 8 recreations detected, c13 and c14 the known misses, c19 the known
  false positive, and no false `break`.
- **The two fault rows are samples, not deterministic fixtures** (PROD-01.1
  §5), so their numbers differ run to run by design. On every line the same
  thing happened: a 75 s broker freeze made produce requests time out at 60 s
  and be resent, which duplicated one 1,000-record batch each; the thawed
  broker answered `NOT_LEADER_FOR_PARTITION`, the engine gave up on leader −1
  and exited 1 over a partial target, and Logweir exited 1 and signed nothing.
  A killed `logweir` left its engine container running, and the engine
  completed the restore with nobody to verify it.

Not run: either suite with authentication or on more than one broker, and the
4.0 and 4.2 lines.

## Other Kafka-compatible endpoints

Two endpoints that are not the `apache/kafka` image run as opt-in compose
profiles (`redpanda`, `confluent`; [`e2e/README.md`](../e2e/README.md)), pinned
by digest and never part of the default set. Their statuses are in the
[compatibility contract](#brokers); this section is what was measured
(PROD-01.2, 2026-10-10 UTC, compose slot 2, engine `0.23.3+logweir.2`).

| Endpoint | Image | Licence of what the row runs |
|---|---|---|
| Redpanda v26.2.4 | `redpandadata/redpanda:v26.2.4@sha256:c98c2f04a751e6646012cc701bac5183252cee44c88ba1dfa412e4c7124f2e89` | Business Source License 1.1 (read at the `v26.2.2` tag; the `v26.2.4` tag is not published on GitHub). Its use grant excludes only offering Redpanda to third parties as a streaming service, so a local test fixture is permitted. The row enables no enterprise feature. |
| Confluent Platform 8.3.2 (`cp-kafka`, the community image) | `confluentinc/cp-kafka:8.3.2@sha256:5e8f3ab5b4977c9a8fd6137d26af2caad878aca316f24c55f08206217e3cec48` | The broker reports `8.3.2-ccs`, Confluent's build of Apache Kafka, Apache-2.0 by the licence files the image carries. |

What each serves, by its own answer to `kafka-broker-api-versions.sh`, beside
what the engine sends (the engine's column is the one under
[Broker versions](#broker-versions-apache-kafka)):

| API | Engine sends | Apache Kafka 4.3.1 | Confluent Platform 8.3.2 | Redpanda v26.2.4 |
|---|---|---|---|---|
| Produce | v8 | v0–v13 | v0–v13 | **v0–v7** |
| Fetch | v11 | v4–v18 | v4–v18 | v4–v13 |
| ListOffsets | v5 | v1–v11 | v1–v11 | v0–v6 |
| Metadata | v9 | v0–v13 | v0–v13 | v0–v12 |
| DescribeConfigs | v1 | v1–v4 | v1–v4 | v0–v4 |
| SaslHandshake / SaslAuthenticate | v1 / v2 | v0–v1 / v0–v2 | v0–v1 / v0–v2 | v0–v1 / v0–v2 |
| ListGroups (Logweir's own client needs v5 to type a group) | v2, never sent by a backup or restore | v0–v5 | v0–v5 | **v0–v4** |
| DescribeTopicPartitions, ConsumerGroupDescribe | never sent | served | served | not served |

**Confluent Platform 8.3.2 did not differ from Apache Kafka 4.3.1** in anything
the row reads: the same ranges, every capability check `ready`, a backup with
topic IDs, the topic's configuration and a classic group captured, a drill
`pass`, and both verifiers on both documents.

**Redpanda v26.2.4 differed in four ways**, each one a row above:

1. **A restore into it cannot run.** The engine sends Produce v8; Redpanda
   serves up to v7 and closes the connection on v8. `target.engineProtocol`
   says so before the restore (`notReady`: "it sends Produce v8 and this
   endpoint serves Produce v0-v7"). A drill started anyway exits 1 at the
   engine ("early eof" reading the response), signs no scorecard, and leaves
   the scratch topic it created.
2. **Its groups cannot be typed** (ListGroups v0–v4), so a selected group is
   `excluded: GroupTypeNotCaptured`, announced by `connection.groupTypes`.
3. **Its broker resource reports nine configuration keys**, and neither the
   broker's timestamp type nor a record-timestamp bound is among them. Logweir
   records the type as absent and the bound as `unknown`; before PROD-01.2 it
   recorded `CreateTime` and "no bound" for an endpoint that had said neither.
4. **Its cluster id has a prefix** (`redpanda.` and a UUID). It is recorded as
   the endpoint reports it.

A backup of it is whole: exit 0, a receipt both readers verify, the records,
the topic IDs and the topic's configuration with the entries Redpanda reports.
SCRAM-SHA-256 and SCRAM-SHA-512 against Redpanda's own SCRAM both authenticate
for a backup, and a wrong password or the other mechanism's user is refused.

## Object stores: conditional create is required

Since RECEIPT-DUP was fixed, `logweir backup run` claims each execution with a
create-only put (`If-None-Match: *`) under `logweir/backups/<backup_id>/`
before the engine starts, and proves the store refused a second create. A store
that does not enforce conditional create is **unsupported**: every backup to it
exits 4 `ExecutionClaimUnproven` before any data is written, and a destination
with `writeProbe` on reports it `notReady / ConditionalCreateUnsupported`
beforehand ([kubernetes.md §21.5](kubernetes.md)).

Which store is supported, untested or unsupported is in the
[compatibility contract](#archive-backends). What was measured about
conditional create:

| Object store | Conditional create |
|---|---|
| MinIO `RELEASE.2025-09-07T16-13-09Z` | Measured (private container: the claim, the refused second run, and the readiness probe's double create). The project's rebuild of this release (`third_party/minio-mirror/`), which the compose stack and the chart's demo MinIO now run, answers a second `If-None-Match: *` create with the same `412 PreconditionFailed` as the upstream image (smoke of 2026-09-24) |
| SeaweedFS 4.48 | Measured through Logweir (PROD-01.2, the `objectstore` profile): the second claim of one execution exits 1 with `failure-reason=ExecutionAlreadyClaimed` |
| RustFS 1.0.0, versitygw v1.8.0 | Measured once with the AWS CLI and the receipt path (PROD-01.5, 2026-09-29): the second create answered 412, and a second run under one `backup_id` was refused |
| older MinIO releases | `[UNVERIFIED — needs a run against an older MinIO release]` |
| AWS S3 | `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` |
| GCS, Azure Blob | `[UNVERIFIED — native conditional create in object_store, not run against either provider]` |
| local filesystem (standalone CLI) | Measured in process only: no row runs the engine against a directory archive |
| `AWS_CONDITIONAL_PUT=disabled`, or an S3-compatible store that ignores `If-None-Match` | Refused, never silently accepted |

**A store's own error codes are read for what they refuse.** versitygw answers
an unknown access key `404 XAdminUserNotFound`. Logweir read that 404 as a
missing object, which sent an operator to the wrong remedy; since PROD-01.2 it
is a refused credential (`InvalidCredentials`), and a genuine `NoSuchKey` is
still not-found.

**Versioned buckets pin the manifest (FX-7).** On a bucket with versioning
enabled, a backup receipt records the version id of the manifest it attests
(`archive.manifest_version_id`, receipt format `1.2.0`), and a point-bound
restore and the catalog compare it with the manifest's current version; when
they differ they read the pinned version by id, so a byte-for-byte copy of the
archive in another bucket — which carries the pin and not the version — is
checked by its digest and says the pin could not be checked there
([backup-receipt.md](formats/backup-receipt.md#the-pinned-manifest-version-versioned-buckets)).

| Object store | Manifest version pinned |
|---|---|
| SeaweedFS 4.48, bucket with versioning (Object Lock) enabled | **Yes, measured** (compose slot 3, 2026-09-29; again on slot 2, 2026-10-05, and at receipt format `1.2.0` after FX-4 merged): the receipt pins the version the read-back was answered with; after a `v0.1.5` runner rewrote the set (identical manifest bytes, a rewritten segment), the catalog's deep check reported the point `Conflict` by version, and a point-bound `restore run` of it exited 3 `PointBindingMismatch` (2026-10-05) |
| A byte-for-byte copy of a pinned point (`aws s3 cp`) into a MinIO unversioned bucket, and into a SeaweedFS 4.48 versioned bucket with its own version ids | **The same point, measured** (slot 2, 2026-10-05): the deep check reports it `Available` with the note that the pin could not be checked in that bucket, and a point-bound `restore run` gets past the binding and logs `PointPinUnchecked`. Each store answered the foreign version id `404 NoSuchVersion`, or MinIO `400 InvalidArgument` for an id that is not a UUID |
| MinIO `RELEASE.2025-09-07T16-13-09Z` and SeaweedFS 4.48, unversioned buckets | **No pin, measured**: the store answers no version id, and the receipt is the document without the pin (format `1.1.0` since FX-4) |
| AWS S3 with versioning | `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` |
| Versioning suspended (S3's `null` version) | no pin by design: a `null` version is replaced in place. A point pinned BEFORE versioning was suspended is still checked: a write after the suspension, over a pinned version the bucket still holds, is `Conflict` (in process) |

## The weekly engine-matrix job

`.github/workflows/engine-matrix.yml` runs every Monday, and on demand, over a
declared set of seven rows: the newest four engine minors (`v0.23.3`, the pin;
`v0.22.0`; `v0.21.0`, the full-drill floor; `v0.20.0`), `v0.19.2` (the library
version of `kafka-backup-operator` 1.3.0), `v0.19.1` (the default of
`strimzi-backup-operator` v0.2.22–v0.2.25), and the pin once more on the newest
supported Apache Kafka line (`KAFKA_VERSION`), where its fixed protocol versions
meet the highest floors. That last row ran `v0.21.0` until PROD-00.3f moved the
pin; the engine's protocol-version table is byte-identical in the two. Each row does the following:

- pins the tag to a digest whose revision label is the tag's commit;
- sets the stack up and runs the suite exactly as the CI e2e job does
  (`just e2e-up`, then `cargo test --locked -p e2e --features e2e`);
- reads back the version the running broker logged, which is what the Kafka
  broker column records;
- runs the deleted-segment positive control;
- reads back what the broker's time-retention check deleted during the run.
  A failed suite's evidence names it, because a test fixture stamped older
  than the broker's retention can lose its records to that check before the
  capture;
- records one of the outcomes above.

A row is green only when it records the outcome it declares, and a row whose
broker differs from its declaration fails. Rows below the full-drill floor seed
with segment digests optional, because those engines write none. They record
`unsupported(lever-absent)` only when Logweir is seen refusing the engine
("below the declared floor") in both the reduced row and the control.

**The `broker-lines` job (PROD-01.5c).** The same workflow also runs PROD-01.1's
record-semantics suite and PROD-01.4's topic-identity suite on every supported
broker line (3.9, 4.1 and 4.3; `stack-env.sh --kafka LINE`, the digest-pinned
images), one job per line, with Logweir's engine build compiled as the CI e2e
job compiles it. The rows above run OSO's releases, and on those the
record-semantics rows record an outcome and assert nothing; the CI e2e job
asserts their contract on the default broker only. This job asserts it on the
other lines. It runs each suite's default set, publishes no row (a red line is a
red job), and has not yet run on GitHub (added 2026-10-10); the runs made by
hand are under [Broker versions](#record-semantics-and-topic-identity-on-each-line).

The job renders its rows between the two markers below and changes nothing else
in this file. It publishes the page as the `support-matrix` artifact and in the
run summary, and opens a pull request only when the repository variable
`ENGINE_MATRIX_OPEN_PR` is `true`: GitHub Actions may not create pull requests
in this repository (checked 2026-09-28).

<!-- engine-matrix:rows:begin -->
No repaired run has been published here yet. The three scheduled runs before
the repair (2026-09-14, 2026-09-21 and 2026-09-28) produced no usable row; the
[engine route decision record](to-do/decisions/PROD-00-engine-route.md) says
why.
<!-- engine-matrix:rows:end -->

Filter-based checks use `scripts/run-named-tests.sh`, which resolves test
names against `--list` and fails if a requested test does not exist. A bare
`cargo test <filter>` can exit successfully after running zero tests.

Only recorded runs belong in the result table; workflow definitions and test
coverage alone do not establish a green version or authentication combination.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
