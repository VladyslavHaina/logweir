# The e2e fixtures: parallel stacks, broker lines and profiles

One compose file, [`compose/docker-compose.yml`](compose/docker-compose.yml),
provides every Kafka and object-store fixture the e2e suites, the demo scripts
and the expansion tasks run against. This guide says how to run more than one
copy at a time, how to pick the Apache Kafka line, which optional profiles
exist and who owns them, and how to extend them. The decision record behind it
is [`docs/to-do/decisions/PROD-01.5-fixture-profiles.md`](../docs/to-do/decisions/PROD-01.5-fixture-profiles.md).

## The default stack is unchanged

With no stack variable set, the stack is exactly the one CI, the quickstart and
every existing doc describe: project `logweir-e2e`, `kafka-broker-1` with its
five listeners on host ports 9092 (plaintext), 9095 (pods) and 9097 (SCRAM),
MinIO on 9000 and 9001, and `just e2e-up` / `just e2e` / `just e2e-down`.
`docker compose config` renders byte for byte what it rendered before these
parameters existed, except for two broker settings PROD-04.0d added for share
groups (`KAFKA_SHARE_COORDINATOR_STATE_TOPIC_*`, below). The 3.7 broker does
not use them: it starts and serves as before and never logs them, but its
DescribeConfigs now reports both as sensitive entries with no value (below).
The default stack is shared: on the
orchestrated run, hold `claude/compose-lock.sh` while you use it.

## Parallel stacks: slots

A **slot** is a complete, private copy of the stack. Slot N (1 to 4) is
compose project `logweir-e2e-sN`, every published host port is the default
plus N×10000, and every Kafka cluster in it has its own KRaft cluster id
([`compose/slots/<project>/`](compose/slots/)), so slots never share a project,
a port, a cluster id, a network or a volume and need no lock:

```sh
eval "$(e2e/compose/stack-env.sh --slot 2)"   # project logweir-e2e-s2, ports 29092, 29000, …
just e2e-up
./scripts/e2e-seed.sh && just e2e              # or: just demo / just pitr / just mvp-demo
just e2e-down
eval "$(e2e/compose/stack-env.sh --slot 0)"    # back to the default stack
```

`stack-env.sh` prints `export` and `unset` lines and validates every argument
before it prints anything. Every variable it can set is also UNSET when not
asked for, so moving to another slot — or back to slot 0 — never inherits the
last slot's broker line or profiles. The variables are the whole interface, and
their one list is [`compose/stack-lib.sh`](compose/stack-lib.sh):

| Variable | Default | What it moves |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `logweir-e2e` | the project (compose's own override of `name:`) |
| `LOGWEIR_E2E_KAFKA_PORT` | 9092 | `EXTERNAL` host port **and** its advertised `localhost:<port>` |
| `LOGWEIR_E2E_K8S_PORT` | 9095 | `K8S` host port and its advertisement |
| `LOGWEIR_E2E_SASL_PORT` | 9097 | `SASLEXT` host port and its advertisement |
| `LOGWEIR_E2E_S3_PORT` | 9000 | MinIO's S3 API |
| `LOGWEIR_E2E_S3_CONSOLE_PORT` | 9001 | MinIO's console |
| `LOGWEIR_E2E_<PROFILE>_PORT` ×9 | 9102-9141 | each optional profile's ports (below) |
| `KAFKA_VERSION` | `.env`'s pin (3.7.1) | the broker line (`--kafka`) |
| `KAFKA_IMAGE` | unset | the broker image, pinned by digest (`--kafka LINE`) |
| `COMPOSE_PROFILES` | none | the optional profiles (`--profiles`) |

The in-network addresses (`kafka-broker-1:9094`, `minio:9000`) never move: each
slot has its own network.

**Anything but one coherent stack is refused.** The project must be
`logweir-e2e` (or unset) or `logweir-e2e-sN`, and EVERY port — the five above
and the nine profile ports — must be exactly that slot's; an unknown
`LOGWEIR_E2E_*_PORT` is refused too. `just e2e-up`, `just e2e-down`,
`scripts/demo.sh`, `scripts/mvp-demo.sh`, `scripts/e2e-seed.sh` and
`compose/profile-smoke.sh` check first (`stack-env.sh --check`), and so does the
Rust harness before its first address, scratch path or `docker compose` call. A
single moved port on the default project would make `up` recreate the shared
default stack and `down` remove it; one slot's project with another's ports
would recreate that slot. Set everything at once with `stack-env.sh`.

**What reads the variables.** The scripts source
[`compose/stack-lib.sh`](compose/stack-lib.sh); the Rust harness compiles the
same file in ([`tests/harness/stack.rs`](tests/harness/stack.rs)). On a slot, the harness and
`scripts/demo.sh` / `scripts/mvp-demo.sh` rebind the checked-in examples'
`localhost:9092` and `http://localhost:9000` to the slot's ports, and write to
`.e2e/<project>/` and `.demo/<project>/` instead of `.e2e/` and `.demo/`, so two
slots can run from one checkout; `scripts/e2e-seed.sh` never refreshes the two
tracked fixtures on a slot (it refuses `LOGWEIR_SEED_REFRESH_FIXTURES=1` there).
`tests/stack_params.rs` fails if the compose file disagrees with the list, if
anything else keeps a copy of it, if the shell and the harness refuse different
environments, if a slot switch leaves a variable behind, if a published port or
a host-facing advertisement is a literal, if `just e2e-down` misses a profile,
if an e2e suite or stack script spells a default-stack address (any of the 14
ports) in code, or if a function runs `docker compose` without checking
coherence first (`stack::ensure_coherent()`).

**What does not move yet.** On a non-default slot `just e2e` runs only the `e2e`
package: the `--features e2e` rows under `crates/` still dial
`localhost:9092` / `localhost:9000` and would reach the default stack, so they
run on the default stack. `scripts/k8s-demo.sh` and the `laptop-demo.sh` /
`kind-demo.sh` walk (`demo-steps.sh`) address the default stack only and REFUSE
a slot before they take the stack (they also install Logweir into a cluster,
which the orchestrated run forbids besides).

**Cluster ids.** Each Kafka cluster loads its id from
`compose/slots/<project>/<cluster>.env`: `kafka-broker-1.env`,
`kafka-auth.env`, `kafka-c3.env` (one id for the three nodes) and
`kafka-cluster2.env`. The default stack's broker has NO file and keeps the
image's `5L6g3nShT-eMCtK--X86sw`, so the default render is unchanged;
`slots/logweir-e2e/` holds the profile clusters' ids they always had. The
profile clusters' files are `required`, so a project with no directory of its
own fails to start instead of borrowing another stack's ids.
`tests/stack_params.rs` fails if two clusters on any two stacks share an id, if
a file is missing or stray, or if a service puts `CLUSTER_ID` back in
`environment:`, which would override its stack's file.

## Broker lines

`--kafka LINE` sets `KAFKA_VERSION` to the line's patch and `KAFKA_IMAGE` to
`apache/kafka:<patch>@<digest>`, the digest recorded in the support matrix
(`stack_params.rs` checks the two agree); every broker service uses
`${KAFKA_IMAGE:-apache/kafka:${KAFKA_VERSION}}`. An exact version
(`--kafka 4.2.2`) is accepted too, by tag. `e2e/compose/.env` keeps the default at
3.7.1, which `scripts/extract-engine.sh` writes.

| Line | Image (`--kafka` pins its index digest) | Status |
|---|---|---|
| 3.7 | `apache/kafka:3.7.1@sha256:ed74d7d1…` | **legacy**: Apache support ended, MSK support ended 2026-09-01; still the default fixture (by tag, from `.env`) |
| 3.9 | `apache/kafka:3.9.2@sha256:05b4616e…` | supported line |
| 4.1 | `apache/kafka:4.1.2@sha256:5cc2a2fd…` | supported line |
| 4.3 | `apache/kafka:4.3.1@sha256:77e3df90…` | supported line |

What each line proved (the demo drill, `just pitr`, the receipt path, the
engine's fixed protocol versions, and PROD-01.1's record-semantics and
PROD-01.4's topic-identity suites) is in
[`docs/support-matrix.md`](../docs/support-matrix.md), "Broker versions".

```sh
eval "$(e2e/compose/stack-env.sh --slot 3 --kafka 4.3)"
just e2e-up && bash scripts/demo.sh && just pitr
just e2e-down && just e2e-up && just mvp-demo && just e2e-down
```

## Which engine runs

The compose stack's `kafka-backup` service is OSO's released image at the
pinned digest (`.env`'s `OSO_DIGEST`), and it seeds the drill archive. Every
engine run through `logweir` — drills, `logweir backup run`, G-PITR,
record semantics — is `harness::engine_bin()`: `.engine/kafka-backup` where it
executes (CI's e2e job builds Logweir's engine there with
`scripts/engine-source.sh build`), else `e2e/fixtures/engine-docker.sh`. The
shim runs OSO's pinned image under `linux/amd64` by default; set
`LOGWEIR_E2E_ENGINE_IMAGE` and `LOGWEIR_E2E_ENGINE_PLATFORM` together to run
another image's engine, such as the runner image's Logweir build natively on
an arm64 host (PROD-00.2):

```sh
eval "$(e2e/compose/stack-env.sh --slot 1)"
LOGWEIR_E2E_ENGINE_IMAGE=logweir:check LOGWEIR_E2E_ENGINE_PLATFORM=linux/arm64 \
  cargo test -p e2e --features e2e --test pitr_boundary -- --test-threads=1
```

`engine_version()` reads the engine's own `--version`, and `engine_digest()`
names Logweir's build-input digest for a `+logweir.` version, as the build
ledger `third_party/kafka-backup-builds.txt` records it for that version (so
an earlier build run at this checkout is named by its own digest, FX-21), and
OSO's image digest otherwise, so a signed document always names the engine
that ran. `scripts/demo.sh` and `scripts/mvp-demo.sh` read the ledger the
same way.

**A native engine can outrun the target's metadata.** Built natively on the
host (`scripts/engine-source.sh build` on macOS), the engine starts a restore
within a second of phase 0 creating the target topics, and it can fail every
topic with `Partition N not available for topic …` before the broker's
metadata names the new partitions' leaders: the drill exits 1 and signs
nothing. Measured on 2026-10-08, without a delay: 1 failed restore in 7
(FX-21's runs) and 1 in 17 (its review's), on a loaded host. The container route
starts slowly enough not to meet it. Since FX-18, phase 0 waits until the
cluster serves each topic it created: 0 failures in 18 native restores, then 0
in 30 (FX-25, 2026-10-10, `full_drill`'s
`a_restore_into_an_empty_scratch_target_succeeds` on slot 1).

## Optional profiles

`--profiles a,b` (or `COMPOSE_PROFILES=a,b`) adds profiles to the stack.
`just e2e-up` brings them up in the same `--wait` (each profile's setup feeds a
long-running service with a healthcheck); `just e2e-down` removes every profile.
`compose/profile-smoke.sh` smokes the active profiles, one negative control per
behaviour, from the host side (published ports) and in-network; name `groups`
to smoke the groups fixture as well (`profile-smoke.sh groups`). It bounds every
container with `timeout` or `gtimeout` when present (this host's
`/tmp/lwtimeout` otherwise), and warns once and runs unbounded with none.

| Profile | Services | What it provides | Ports (default) | Owner (first consumers) |
|---|---|---|---|---|
| `auth` | `kafka-auth` (+ certs, setup) | A separate single-node cluster: SASL_SSL/PLAIN on 9102, SASL_PLAINTEXT/SCRAM-SHA-256 on 9103, SSL with a required client certificate on 9104, all advertised as `localhost:<port>`. User `logweir`, password `logweir-e2e-not-a-secret`. A throwaway test CA, broker and client certificates and a wrong CA in `.e2e/auth/<project>/`. | 9102-9104 | PROD-01.3 |
| `cluster3` | `kafka-c3-1..3` | A three-node KRaft cluster, replication factor 3 and `min.insync.replicas` 2 by default | 9112-9114 | the orchestrator until PROD-10.1 starts (FX-5 and PROD-05.1 use it first; FX-3's `new_topic_parity.rs` and FX-21's `replication_factor_parity.rs`). PROD-01.2's `compat_contract.rs::a_three_broker_cluster_is_answered_by_every_broker_or_not_at_all` reads it and FREEZES one node that is not the active controller (`docker compose pause`) for about a minute, then unfreezes it and waits for three brokers again: do not run it beside another row on the same stack |
| `cluster2` | `kafka-cluster2` (+ setup) | A second single-node cluster with its own cluster id and the marker topic | 9122 | PROD-11.1 (PROD-04.2, PROD-12.1) |
| `autocreate` | `kafka-autocreate` | A single-node cluster that AUTO-CREATES topics (`auto.create.topics.enable=true`, Kafka's default, which every other broker here turns off), with its own cluster id and no marker topic. Harness: `bootstrap_autocreate()` | 9126 | PROD-15.1 (`e2e/tests/original_name.rs`) |
| `objectstore` | `objectstore` (+ setup) | SeaweedFS 4.48 beside MinIO: `kafka-backups`, `logweir-evidence`, `kafka-backups-locked` (Object Lock) and `kafka-backups-2`, credentials `minioadmin`/`minioadmin` | 9130 | PROD-09.1 (PROD-09.2, REPLACE-MINIO) |
| `registry` | `registry`, `registry-rest` | Karapace 6.2.3, Schema-Registry-compatible, schemas in `_schemas` on `kafka-broker-1`, BACKWARD compatibility; and its REST proxy (Confluent REST v2: `POST /topics/<t>` in `avro`, `jsonschema` or `protobuf` writes records in the Confluent wire format, registering their schemas in `registry`). Logweir never talks to either: PROD-03.0's row (`tests/schema_dependency.rs`) produces through the proxy, then stops both before the backup | 9141, 9142 | PROD-03.0 (PROD-03.1, 03.2) |
| `streams` | `streams-wordcount` (+ topics) | Apache Kafka's WordCountDemo from the broker line's own image, group `logweir-e2e-wordcount`, in-memory state stores | none | PROD-06.1 (PROD-04.x, 06.2) |
| `streams-protocol` | `streams-protocol-wordcount` (+ topics) | The same idea on the STREAMS rebalance protocol (KIP-1071, `group.protocol=streams`): Apache Kafka's WordCountProcessorDemo, group `logweir-e2e-streams-protocol`, a Streams group that `kafka-groups.sh --list` types `Streams` and the consumer-group tools do not show. It reads `streams-plaintext-input` and writes `streams-wordcount-processor-output`, so it runs beside `streams` (two WordCountDemos would share one output topic). In-memory store, changelog-backed, no repartition topic; counts are forwarded on stream-time punctuation, i.e. once a later record arrives. **4.x lines only** (measured on 4.3.1): on 3.x the application exits and `up --wait` fails. The groups fixture's streams member | none | PROD-04.0d (PROD-04.1, 04.2, 06.1, 06.x) |
| `acl` | `kafka-acl` (+ setup) | A single-node cluster that ENFORCES ACLs (KRaft's StandardAuthorizer, `super.users=User:ANONYMOUS`, `allow.everyone.if.no.acl.found=true`). Every PLAINTEXT client, in-network on `kafka-acl:9094` or host-side on 9150, is `User:ANONYMOUS`, a super user; the SCRAM-SHA-512 user `logweir` (password `logweir-e2e-not-a-secret`) on 9151 is the restricted principal a row's ACLs name. The marker topic exists. PROD-04.0 §3.9's visibility state is opt-in: `e2e/compose/groups.sh visibility apply` / `remove` (below). Harness: `bootstrap_acl()`, `bootstrap_acl_sasl()` | 9150-9151 | FX-4 (PROD-04.0d extends it; PROD-04.1's `position_evidence.rs`, 04.2, PROD-05.1's `topic_configuration.rs`, run on `--kafka 3.9` and `--kafka 4.3`; PROD-05.3; PROD-01.4a's `topic_ids.rs`, a DescribeTopics refused by name) |
| `redpanda` | `redpanda` (+ setup) | One Redpanda v26.2.4 node, pinned by digest: a Kafka-compatible endpoint that is not Apache Kafka. PLAINTEXT on 9124; SASL_PLAINTEXT on 9125, where user `logweir` authenticates with SCRAM-SHA-256 and `logweir512` with SCRAM-SHA-512 (Redpanda holds one mechanism per user), both with the fixture password. Authorization is off. `orders` (3 partitions) and the marker topic exist; topic auto-creation is off. Its cluster id is `redpanda.<uuid>`, new at every first start. Redpanda is under the Business Source License 1.1; the image is pulled, never redistributed. | 9124, 9125 | PROD-01.2 (`tests/compat_contract.rs`) |
| `confluent` | `kafka-cp` (+ setup) | One Confluent Platform 8.3.2 broker from the image `confluentinc/cp-kafka`, pinned by digest (Confluent's build of Apache Kafka, `8.3.2-ccs`; not `cp-server`; its Kafka jars carry the Apache-2.0 text, and the terms of the image as a whole are not stated in it: the decision record, §4.2). PLAINTEXT on 9128. Its own cluster id per stack, `orders` (3 partitions) and the marker topic. A third listener, OFFNET on 9129, is the advertised-address failure: it answers a bootstrap and advertises `127.0.0.1:1`, where nothing listens, so a connection test over it passes and no request to the advertised broker can. | 9128, 9129 (9126 is `autocreate`'s) | PROD-01.2 (`tests/compat_contract.rs`) |
| `txn` | reserved | The transactional producer PROD-01.1 builds | — | PROD-01.1 |

The **owner** changes a profile's services without asking; anyone else extends
it by adding a service, or asks the owner. A profile is torn down by
`just e2e-down` like the rest; the auth certificates stay in
`.e2e/auth/<project>/` (gitignored) and are reused by the next `up`.

Limits worth knowing: the `streams-protocol` profile and the groups fixture's
consumer, share and streams groups need a 4.x line (the groups fixture falls
back to classic groups by itself; the profile fails `up` on 3.x); the auth listeners are host-facing (an in-network client
uses `kafka-auth:9094`, plaintext); the Streams application keeps its state in
memory, because RocksDB's native library needs `libstdc++`, which the
Alpine-based `apache/kafka` image lacks; `objectstore` does not replace MinIO
(that is REPLACE-MINIO); on `acl`, a row grants or denies with `kafka-acls.sh`
inside `kafka-acl` as the super user and removes what it added, and the
restricted principal's `kafka-topics.sh --describe` needs DescribeConfigs
too, because it reads the topic's configuration (measured on 3.7.1); every
behaviour above was measured with Docker Compose v5.0.2. Neither `redpanda`
nor `confluent` is ever part of the default set: CI's e2e job does not start
them, and `tests/stack_params.rs` fails if `ci.yml`'s profile set names one
(it reads that one workflow, not every workflow).

## The groups fixture: one group of each type, and groups a principal cannot see

[`compose/groups.sh`](compose/groups.sh) (PROD-04.0d) builds the consumer-group
fixture PROD-04.1, 04.2 and 05.3's acceptance rows name (the record is
[`PROD-04.0-admin-path.md`](../docs/to-do/decisions/PROD-04.0-admin-path.md),
§3 and §10) on the stack the shell addresses. It is a helper, not a profile:
run it after `just e2e-up`, and `just e2e-down` removes everything it made.

```sh
eval "$(e2e/compose/stack-env.sh --slot 1 --kafka 4.3 --profiles acl)"
just e2e-up
e2e/compose/groups.sh up                    # the set below, on kafka-broker-1; idempotent
e2e/compose/groups.sh stop classic-live     # returns once pa-classic-live is Empty
e2e/compose/groups.sh start classic-live    # returns once it is Stable again
e2e/compose/groups.sh list                  # GROUP TYPE STATE, as the broker reports them
e2e/compose/groups.sh visibility apply      # §3.9 on kafka-acl; `remove` undoes it
e2e/compose/profile-smoke.sh groups acl     # the fixture's smoke
just e2e-down
```

**The set follows the broker's finalized features** (`kafka-features.sh
describe`): classic groups always, consumer groups with `group.version` ≥ 1,
share groups with `share.version` ≥ 1, streams groups with `streams.version`
≥ 1. That is every type on the 4.3 line and classic only on 3.9 (measured on
4.3.1 and 3.9.2). `list` prints the type the BROKER reports (`kafka-groups.sh`
on 4.x, `kafka-consumer-groups.sh --list --type` on 3.9); 3.7.1's tools cannot
report one, and `list` prints `-` there.

| Group | Type | State | How |
|---|---|---|---|
| `pa-classic-empty` | Classic | Empty | a console consumer read 12 records of `pa-orders` and left |
| `pa-classic-live` | Classic | Stable | member `classic-live` |
| `pa-consumer-empty` | Consumer (KIP-848) | Empty | as `pa-classic-empty`, `group.protocol=consumer` |
| `pa-consumer-live` | Consumer | Stable | member `consumer-live` |
| `pa-share-idle` | Share (KIP-932) | Empty | a share consumer read 10 records of `pa-share-in` and left; its start offsets are readable |
| `pa-share-live` | Share | Stable | member `share-live` |
| `logweir-e2e-streams-protocol` | Streams (KIP-1071) | Stable | member `streams`: the `streams-protocol` profile's application, which `up` starts |

Topics on `kafka-broker-1`: `pa-orders` (3 partitions, 30 keyed records) and
`pa-share-in` (2 partitions, 40 keyed records). Both share groups carry the
group configuration `share.auto.offset.reset=earliest`, set before their
first member joins. An id no one created (say `pa-absent`) is the absent
group of AP-04.1-1.

**Members stop cleanly.** The three live consumers run inside
`kafka-broker-1`, each bounded by `timeout` (`GROUPS_MEMBER_SECONDS`, default
7200). `stop` sends SIGTERM through a bracketed, anchored pattern
(`[g]roup NAME$`), so the consumer closes and LEAVES its group, which is
Empty at the first check after its process exits (`stop` says so, and the
smoke requires it; a member killed without leaving would hold the group for
`session.timeout.ms`, 45 s); the pattern never matches the shell that carries it (PROD-04.0
§3.3's first control killed its own `sh -c`) or a neighbouring member. The
streams member is stopped and started with `docker compose`. While a member
lives, a reset of its group is refused (the group is active); once `stop`
returns, the same reset commits and reads back — the control AP-04.2-1 relies
on.

**Share-group state on one broker.** Every single-node broker
(`kafka-broker-1`, `kafka-auth`, `kafka-cluster2`, `kafka-acl`) sets
`share.coordinator.state.topic.replication.factor=1` and `…min.isr=1`, as the
offsets and transaction topics already were. With the defaults (3 and 2)
`__share_group_state` is never created on one broker, so share groups get
members but no share-partition state (§3.8). A 3.x broker does not use either
key, but it does not hide them: DescribeConfigs on the broker reports both as
STATIC entries, `sensitive=true` with a null value, because a key the broker
cannot type is withheld (§3.9's mechanism, the same as `super.users` on `acl`).
Measured on 3.7.1 and 3.9.2, where the broker starts, serves as before and
never logs them. A row that reads a 3.x single-node broker's configuration
(PROD-05.3) therefore sees two more "set, value withheld" keys.

**Groups a principal cannot see (§3.9), on `acl`.** `visibility apply`, as the
super user on `kafka-acl`: topic `pa-orders`; groups `pa-visible` and
`pa-hidden`, each created by a non-member commit (offsets 5 and 7 on partition
0); and ACLs that name only `User:ops`: Describe and Read on group
`pa-hidden`, Describe on the cluster. The restricted `User:logweir` then has
neither Describe on the cluster nor on `pa-hidden`, so its group listing omits
`pa-hidden` with no error, a targeted describe of it is refused
(GroupAuthorizationException), and DescribeAcls is refused
(ClusterAuthorizationException). Granting it Describe on the cluster unfilters
the listing (the smoke's negative control). It is OPT-IN because its cluster
ACL takes every cluster operation from the restricted principal, which the
profile's other rows (FX-4's) expect it to have; `visibility remove` deletes
the ACLs, both groups and the topic.

Owner: PROD-04.0d; PROD-04.1, 04.2 and 05.3 are the first consumers. Extend it
as a profile is extended; a new member or group goes in `groups.sh`'s `SET`
and in `smoke_groups`.

## Extending the fixtures

Later tasks extend these profiles instead of building private fixtures:

1. Add the service to `compose/docker-compose.yml` under a profile (a new one or
   an existing one you own), on `kafka-net`.
2. Publish a port only as `"${LOGWEIR_E2E_<NAME>_PORT:-<default>}:<container>"`
   with an unused default below 9152, and advertise any host-facing listener
   with the same variable.
3. Register the port in the ONE list, `compose/stack-lib.sh`
   (`LW_E2E_PORT_TABLE`: variable, default, profile) — the harness and
   `stack-env.sh` read it from there — and a new profile in `stack-env.sh`'s
   `PROFILES_LIST` and on the `just e2e-down` line.
4. Make setup a dependency of, or itself, a long-running service with a
   healthcheck; a bare one-shot in the `up` set makes `--wait` fail.
5. A new Kafka cluster loads its id like the others (`env_file:
   ./slots/${COMPOSE_PROJECT_NAME:-logweir-e2e}/<cluster>.env`, `required:
   true`), with one fresh id per stack in `compose/slots/*/`.
6. Give the profile a smoke function with a negative control in
   `compose/profile-smoke.sh`, and run `cargo test -p e2e --test stack_params`.

---

Documentation is licensed [CC-BY-4.0](../docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
