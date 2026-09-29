# PROD-01.5 — Shared fixture profiles, supported broker lines and the object-store choice

**Type:** infrastructure decision with measured evidence and a contract later
rows build on.
**Covers:** PROD-01.5 (parallel-safe fixtures; broker lines 3.9, 4.1, 4.3 and
3.7.1 as legacy; the maintained object-store choice; the optional profiles).
**Date / base:** 2026-09-29, branch `claude/prod-01-5` from main `adee0a16`.
Every result below was measured on this host (Docker 29.2.1, Compose v5.0.2,
darwin/arm64, the engine's linux/amd64 image under emulation). Evidence paths
are under `/tmp/logweir-roadmap-run/claude/artifacts/prod-01-5/` (the run
directory); the orchestrator keeps them.
**Guide for users:** [`e2e/README.md`](../../../e2e/README.md).

---

## 0. Decision summary

1. **Parallel stacks by slot.** The compose project is `COMPOSE_PROJECT_NAME`
   (default `logweir-e2e`) and every published host port is a
   `LOGWEIR_E2E_*_PORT` variable (defaults 9092, 9095, 9097, 9000, 9001), with
   each host-facing advertised listener carrying its port's variable. A **slot**
   N (1–4) is project `logweir-e2e-sN` with every port plus N×10000 and its own
   broker cluster id; `e2e/compose/stack-env.sh` prints one. The variables have
   ONE list (`e2e/compose/stack-lib.sh`), and every entry point refuses an
   environment that is not exactly one slot. The default render is
   byte-identical to `adee0a16`'s.
2. **Broker lines.** 3.9.2, 4.1.2 and 4.3.1 pass the demo drill, `just pitr`
   and the receipt path with engine 0.21.0; 3.7.1 passes too and is recorded
   **legacy**. The default fixture stays 3.7.1 in this branch (child row C1).
3. **The engine's unnegotiated protocol versions work on 4.x, with no margin
   on two APIs.** It never sends ApiVersions; every fixed version is inside
   4.1.2's and 4.3.1's ranges, but DescribeConfigs v1 is on the 4.x floor and
   DescribeGroups goes out at v0. PROD-00.1's "ApiVersions negotiation"
   capability row owns the route.
4. **Object store: SeaweedFS 4.48** (Apache-2.0) is the maintained store for
   the fixtures and the recommendation for REPLACE-MINIO. RustFS 1.0.0 passes
   the same evidence and is the runner-up (it speaks MinIO's admin API). MinIO
   stays the default fixture store until REPLACE-MINIO.
5. **Profiles** `auth`, `cluster3`, `cluster2`, `objectstore`, `registry`,
   `streams` are delivered, each with a smoke run and negative controls; `txn`
   is reserved for PROD-01.1's transactional producer.

---

## 1. Parallel-safe fixtures

### 1.1 Contract

| Variable | Default | Meaning |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `logweir-e2e` (the file's `name:`) | compose project; compose itself reads it ahead of `name:` |
| `LOGWEIR_E2E_KAFKA_PORT` | 9092 | host port of `EXTERNAL`, and its advertisement `localhost:<port>` |
| `LOGWEIR_E2E_K8S_PORT` | 9095 | host port of `K8S`, and `${LOGWEIR_K8S_ADVERTISED_HOST}:<port>` |
| `LOGWEIR_E2E_SASL_PORT` | 9097 | host port of `SASLEXT`, and `localhost:<port>` |
| `LOGWEIR_E2E_S3_PORT` | 9000 | MinIO S3 API host port |
| `LOGWEIR_E2E_S3_CONSOLE_PORT` | 9001 | MinIO console host port |
| `LOGWEIR_E2E_{AUTH_PLAIN,AUTH_SCRAM256,AUTH_MTLS,C3_1,C3_2,C3_3,CLUSTER2,OBJSTORE,REGISTRY}_PORT` | 9102, 9103, 9104, 9112, 9113, 9114, 9122, 9130, 9141 | the optional profiles' host ports (§4) |
| `KAFKA_IMAGE` | unset (`apache/kafka:${KAFKA_VERSION}`) | the broker image; `--kafka LINE` pins it by digest |

- **One list.** `e2e/compose/stack-lib.sh` (`LW_E2E_PORT_TABLE`, the default
  project, the stride, the slot cap) is the only copy: the scripts and
  `stack-env.sh` source it, the harness compiles it in (`include_str!`), and
  `stack_params.rs` fails on any other file that spells a port variable with a
  number (the compose file's `${VAR:-N}` excepted, and checked against it).
- **Absent variable:** the default; the stack is the one CI and every doc
  describe.
- **Coherence rule:** the project is `logweir-e2e` (or unset) or
  `logweir-e2e-sN` (N = 1..4), and EVERY port in the list is exactly that
  slot's (unset counts as the default, so only on slot 0); no other
  `LOGWEIR_E2E_*_PORT` is set. The shell (`lw_e2e_check_coherent`, behind
  `stack-env.sh --check`) and the harness (`stack::incoherence_in`) implement
  it and are run over one matrix. Refusing: `just e2e-up`, `just e2e-down`,
  `just e2e`, `scripts/demo.sh`, `scripts/mvp-demo.sh`, `scripts/e2e-seed.sh`,
  `e2e/compose/profile-smoke.sh`, and the harness (`ensure_coherent()` before
  its first address, scratch path or `docker compose` call; every e2e file that
  runs compose itself calls it too). The Kubernetes demos (`k8s-demo.sh`,
  `demo-steps.sh`) additionally refuse any slot. Reason: the default project
  with any moved port makes `up` recreate, and `down -v` remove, the shared
  default stack; one slot's project with another's ports recreates that slot.
- **Switching slots** unsets everything the helper can export (project, the 14
  ports, `KAFKA_VERSION`, `KAFKA_IMAGE`, `COMPOSE_PROFILES`) unless asked for.
- **Cluster ids:** every Kafka cluster on every stack has its own
  `CLUSTER_ID`, loaded from `e2e/compose/slots/<project>/<cluster>.env`
  (`kafka-broker-1`, `kafka-auth`, `kafka-c3` for the three nodes,
  `kafka-cluster2`), and none is set in `environment:`, which would override
  the file. The default project has no `kafka-broker-1.env`
  (`required: false`), so the default broker keeps the image's
  `5L6g3nShT-eMCtK--X86sw` and the default render is unchanged;
  `slots/logweir-e2e/` holds the ids the profile clusters always had, so their
  slot-0 render is unchanged too. The profile clusters' files are
  `required: true`: a project with no directory of its own fails to start
  (measured: "env file …/slots/someone-else/kafka-auth.env not found") instead
  of borrowing another stack's ids. So the cluster-id allowlist tells any two
  clusters on any two stacks apart; slot isolation no longer rests on the
  address sweep alone.
- **Tracked fixtures:** `scripts/e2e-seed.sh` never refreshes
  `e2e/fixtures/manifests/0.21.json` / `segments/upstream-0.21.0.kbak` on a slot
  and refuses an explicit request there.
- **Scratch:** `.e2e/<project>/` and `.demo/<project>/` off the default stack
  (unchanged `.e2e/` and `.demo/` on it). The harness and the demo scripts
  rebind the examples' `localhost:9092` and `http://localhost:9000` to the
  slot's ports; in-network names never move.
- **Not moved:** the `crates/` `--features e2e` rows (they dial
  `localhost:9092` / `:9000`), so `just e2e` on a non-default slot runs the `e2e`
  package only and says so; and the Kubernetes demos (`k8s-demo.sh`,
  `kind-demo.sh`, `laptop-demo.sh`, `demo-steps.sh`), which refuse a slot.
  Child row C2.

### 1.2 Evidence

- **Byte-identical default.** `docker compose config` with no variable set,
  and with `--profile setup --profile tools`, before and after:
  `cmp` identical (`baseline-config-*.yaml` vs `after-config-*.yaml`, and again
  after the fix round's `KAFKA_IMAGE` and `env_file`: `fix-config-*.yaml`,
  `fix-round/config-*.final.yaml`). With EVERY profile active, slot 0's render
  after the cluster ids moved to `slots/` is identical to the branch tip's
  before the fix round (`fix-round/render-allprofiles-slot0.*.yaml`); slot 2's
  shows its four own ids (`render-allprofiles-slot2.after.yaml`).
- **Two stacks, two drills (2026-09-29T03:27–03:33Z).** Slots 1 and 2 up
  together (19092/19000 and 29092/29000; `concurrency/snapshot-both-up.txt`).
  `scripts/demo.sh` passed on both (runs `01M3NKA3AM6TEPRE71PTGXGAZR`,
  `01M3NKBP9ZP5JKKJTYESYDCPT1`, outcome `pass`, VALID under both readers). Then
  `just pitr` started on both in the same second (03:30:49) and passed (82 s,
  83 s) while another worker held the lock and ran the **default** stack from
  its own worktree — three stacks up (`concurrency/snapshot-three-stacks.txt`).
  `just e2e-down` per slot left no container, network or volume, and the default
  stack untouched (`concurrency/after-teardown.txt`).
- **Four broker lines at once** (§2) on slots 1–4, and the whole `e2e` package
  on a 4.3.1 slot (§2.3).
- **Refusal matrix** (fix round, `fix-round/refusal-matrix.sh` with a
  recording `docker` shim, so no stack was touched; `fix-round/matrix.txt`):
  55/55. `just e2e-up` and `just e2e-down` each refuse, before any docker call
  and for the stated reason, every one of the 14 ports set alone, a slot's
  project alone, a foreign project, an unknown port variable, slot 2's project
  with slot 1's ports, slot 1 missing a profile port, and slot 1 with slot 2's
  registry port; both pass a coherent slot 1 and the default. demo.sh,
  mvp-demo.sh, e2e-seed.sh and profile-smoke.sh refuse the same way; e2e-seed
  refuses a fixture refresh on a slot; the demos' default-only guard refuses a
  slot. The harness: `pitr_boundary`, `smoke` and `guards` under a lone
  `LOGWEIR_E2E_OBJSTORE_PORT=19130` fail with the coherence panic (22 panics)
  and ZERO docker calls; the only two `guards` rows that pass run
  `logweir schema` and touch no stack.
- **Guards.** `stack_params.rs`: 16 rows in the default test set, 1 Docker
  render row under `e2e`. Eleven fix-round mutants, each killed
  (`fix-round/mutants/summary.txt`): the shell rule back to the core ports,
  the harness rule skipping profile ports, the harness blind to cross-slot
  values, `unset KAFKA_VERSION` dropped, `ensure_coherent()` removed from a
  direct compose caller, two slots' brokers sharing a cluster id, two slots'
  `cluster3` sharing one, a `CLUSTER_ID` put back inline in `kafka-cluster2`,
  a line digest the support matrix does not record, a port table copied
  back into `stack-env.sh`, and a profile-port literal (`localhost:9130`)
  planted in a reader (the sweep's literal set covers all 14 ports).
  Fourteen earlier mutants, each killed (`mutants/summary.txt`):
  a literal published port, a literal advertisement, a drifted default in the
  compose file, in `stack-lib.sh` (assignment and coherence tuple), in
  `stack-env.sh`, a profile missing from `e2e-down` or from `PROFILES_LIST`, a
  default-stack address put back into `pitr_boundary.rs` and into `mvp-demo.sh`,
  a fifth slot reaching ephemeral ports, a literal `SASLEXT` advertisement in
  the render, a non-recursive walk (the planted-twin row fails) and a support
  module planted two directories down in `e2e/tests/` (the sweep fails).
- **PROD-01.1 and PROD-01.4 swept after they merged** (merge `23cf86ad`,
  sweep `8eb5ef93`). Their one address places —
  `record_semantics_support/kafka.rs` `bootstrap()`/`s3_endpoint()` and
  `topic_identity.rs` `broker_address(Host)` — delegate to the harness, and
  their own `docker compose` calls check coherence first. At the pure merge the
  guards named exactly those readers (the literal MinIO endpoint; both files'
  unchecked compose calls) and the `e2e` build refused the removed `BOOTSTRAP`
  constant. The compose-call guard is per function now: each code line naming
  the compose file needs a coherence call earlier in its function (a doc
  comment does not count). Five negative controls, each killed
  (`fix-round/merge/mutants/summary.txt`): each swept line reverted to main's,
  and each coherence call dropped. Live on slot 3 while the default stack was
  left alone: PROD-01.1's `keys_nulls_tombstones_and_duplicate_headers`
  (41.6 s) and its `#[ignore]`d `a_lost_produce_acknowledgement_during_restore`
  (177.9 s; `compose_broker` paused slot 3's broker, not the default one:
  `docker ps` showed `logweir-e2e-s3-kafka-broker-1-1 … (Paused)`), and
  PROD-01.4's `live::c01_recreate_same_partition_count_shorter` (19.7 s), all
  passed; every spec they wrote names `localhost:39092` / `:39000` and the
  allowlist names slot 3's own cluster id (`fix-round/live/slot3-*`). The
  default project had no container before or after, so a row that reached it
  would have failed. Under a lone `LOGWEIR_E2E_OBJSTORE_PORT=19130`, all 10
  `record_semantics` rows and all 20 `topic_identity` live rows fail with the
  coherence panic and ZERO docker calls; its 33 pure rows pass.

---

## 2. Broker lines

### 2.1 Pins

| Line | Image (index digest) | Status |
|---|---|---|
| 3.7 → 3.7.1 | `apache/kafka@sha256:ed74d7d115968d5e8b00ba6822ac6a384cbaaf54ca38991828647000d7089b68` | legacy (default fixture) |
| 3.9 → 3.9.2 | `apache/kafka@sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02` | supported |
| 4.1 → 4.1.2 | `apache/kafka@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e` | supported |
| 4.3 → 4.3.1 | `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` | supported |

The newest patch of each line on Docker Hub on 2026-09-28. The pins are
ENFORCED: `stack-env.sh --kafka LINE` exports
`KAFKA_IMAGE=apache/kafka:<patch>@<digest>`, which every broker service in the
compose file uses (`${KAFKA_IMAGE:-apache/kafka:${KAFKA_VERSION:-3.7.1}}`), and
`stack_params.rs` fails if a line's digest differs from the support matrix's.
The default fixture (no `--kafka`) stays tag-based on `.env`'s 3.7.1. 4.0 and
4.2 are supported upstream and not run here.

### 2.2 Results (2026-09-29T03:33–03:45Z, `lines/<line>/steps.txt`)

| Broker | Demo drill (run id) | `just pitr` | Receipt path (`just mvp-demo`) |
|---|---|---|---|
| 3.7.1 | pass, VALID ×2 (`01M3NKY724J8ZYBV3AS90NF1HD`) | 1 passed | pass, 2000 records, receipt and scorecard VALID ×2 |
| 3.9.2 | pass, VALID ×2 (`01M3NKPPSWE60T8DBAEQM2BBQY`) | 1 passed | pass, 2000 records, VALID ×2 |
| 4.1.2 | pass, VALID ×2 (`01M3NKTQFZ3H2CM2Y06D29WM71`) | 1 passed | pass, 2000 records, VALID ×2 |
| 4.3.1 | pass, VALID ×2 (`01M3NKX0M1H5CJW2KM5CKGQBXF`) | 1 passed | pass, 2000 records, VALID ×2 |

Recorded in `docs/support-matrix.md`, "Broker versions", with a broker-version
column. Limits: one broker, plaintext, MinIO, engine 0.21.0 only.

### 2.3 The engine's protocol versions on 4.x

- **Source.** `kafka-backup` v0.21.0 (`third_party/kafka-backup-v0.21.0.tar.gz`,
  sha256 `0252a83735148331c16d7c4e737a41f099c0f52eda5d7a66db75b8848ddc405b`),
  `crates/kafka-backup-core/src/kafka/client.rs:588-611`, `get_api_version`: a
  fixed table (Metadata 9, Fetch 11, Produce 8, SaslHandshake 1,
  SaslAuthenticate 2, ApiVersions 3, ListOffsets 5, CreateTopics 5,
  FindCoordinator 2, OffsetFetch 5, OffsetCommit 5, ListGroups 2, DeleteRecords 1,
  DescribeConfigs 1, IncrementalAlterConfigs 1, ACL APIs 1) and `_ => 0` for
  everything else. The only API the engine sends through that fallback is
  DescribeGroups (`consumer_groups.rs:153`). Nothing sends ApiVersions.
- **Ranges.** `kafka-broker-api-versions.sh` per line
  (`lines/api-versions-comparison.md`): all 15 engine versions inside every
  line's range. KIP-896 raised 4.x floors to Fetch v4, ListOffsets v1,
  OffsetCommit v2, OffsetFetch v1, CreateTopics v2 and DescribeConfigs v1 — all
  at or below the engine's pins.
- **What the broker received.** `kafka.request.logger` at DEBUG on 4.3.1
  (`lines/4.3/trace/`, `lines/4.3/trace-demo/`): on both the receipt path and
  the demo drill the engine (client id `kafka-backup`) sent Metadata v9,
  ListOffsets v5, Fetch v11, DescribeConfigs v1 and Produce v8 and no
  ApiVersions; Logweir's librdkafka client negotiated.
- **Whole suite on 4.3.1.** CI's e2e-package command (`--skip
  a_pod_really_reaches_the_k8s_listener`) on slot 3, 2026-09-29T05:14–05:31Z
  (`lines/4.3/e2e-package/steps.txt`): 75 passed, 0 failed across ten binaries
  (backup_argv 7, check_image 2 + 12 ignored, full_drill 15, guards 22,
  mvp_demo 3, offset_side 4, pitr_boundary 1, scram 9, smoke 1, stack_params 11).
  The scram rows are the SCRAM-SHA-512 evidence on 4.x, through both
  Logweir's client and the engine's: the engine's SaslHandshake v1 and
  SaslAuthenticate v2 (`client.rs:594-595`, not traced) were accepted, since its
  SCRAM backups succeeded.
- **Limit and risk.** Unexercised on these paths: the engine's CreateTopics,
  consumer-group APIs, DeleteRecords, IncrementalAlterConfigs. DescribeConfigs
  v1 and DescribeGroups v0 have no headroom: a release that raises either floor
  fails the engine with an unsupported version, not a downgrade. The same holds
  for the table's three ACL entries — DescribeAcls, CreateAcls and DeleteAcls
  pinned at v1, which is the 4.x floor (`DescribeAcls(29): 1 to 3` on 4.3.1,
  likewise Create/Delete) — although 0.21.0 has no call site for them. The
  table has 18 fixed entries; 15 are sendable (14 with call sites, DescribeGroups
  through the fallback).

---

## 3. The maintained object-store choice

### 3.1 What Logweir relies on, and what was run

Each candidate ran on its own private network and volume, then was removed.

1. **Protocol probe** (`objstore/probe.sh`, amazon/aws-cli 2.37.5, SigV4,
   forced path style): 19 checks — create/round trip, wrong secret and unknown
   key refused, `If-None-Match: *` first create 200 and second 412 with the
   first object intact, versioning enabled with distinct version ids, GET by
   version id, latest, listing, Object Lock bucket, lock configuration readback,
   GOVERNANCE retention put, `GetObjectRetention` and `HEAD` readback, deleting a
   retained version refused, legal hold readback, a second bucket.
2. **Logweir's own code** (`objstore/logweir-paths.sh`, on a 4.3.1 slot):
   `logweir-store`'s `minio_options.rs` (explicit credentials; wrong secret and
   unknown key classified as credential problems; missing bucket vs missing key;
   `allow_http=false`), and the receipt path with every S3 endpoint on the
   candidate: `logweir backup run` (execution claim, engine archive, receipt),
   the receipt verified by both readers, a second run under the same
   `backup_id` refused `ExecutionAlreadyClaimed`, a point-in-time `restore run`,
   the scorecard verified by both readers.
3. **Least privilege** (for REPLACE-MINIO): a non-root user limited to
   `kafka-backups/allowed/*`.

| Store | Licence, upkeep (GitHub, 2026-09-29) | Probe | Store layer | Receipt path | Least privilege |
|---|---|---|---|---|---|
| MinIO mirror `RELEASE.2025-09-07` (baseline) | AGPL-3.0; upstream **archived** | 19/19 | 6/6 | pass | (the chart's `mc admin` today) |
| **SeaweedFS 4.48** `sha256:4e61d15f…d7872d` | **Apache-2.0**; since 2014, 601 contributors, 30 releases since 2026-04-08 | **19/19** | **6/6** | **pass** (rto 11 s, rpo 12 s) | static `s3.json` identity scoped to a prefix: allowed write, other prefix and create-bucket AccessDenied; its IAM write API is disabled under a static config |
| RustFS 1.0.0 `sha256:8cc98017…58f4d1ff` | Apache-2.0; since 2023, 181 contributors, 1.0.0 GA 2026-09-16 | 19/19 | 6/6 | pass (9 s, 12 s) | `mc admin user add / policy create / attach` work; the prefix policy is enforced |
| versitygw v1.8.0 `sha256:30292fc2…a2499` | Apache-2.0; smaller community | 19/19 | **5/6** | pass (11 s, 11 s) | not run |

**versitygw is not chosen, as a preference — not an incompatibility.** An
unknown access key is answered `404 XAdminUserNotFound`, which Logweir's store
layer classifies as `ObjectNotFound`, not `InvalidCredentials`
(`minio_options.rs:158`), so `logweir check` and weirkeeper would send an
operator to the wrong remedy. The gap is Logweir's to close:
`StoreErrorClass::classify`'s credential tokens
(`crates/logweir-store/src/lib.rs:1844-1854`) do not include
`xadminusernotfound`, so the 404 falls through to `NOT_FOUND` (`:1884-1887`) —
a class of non-standard S3 error codes the classifier does not know (child row
C6). With that closed, versitygw would pass Logweir's store layer too; it stays
behind SeaweedFS on community size and on least privilege, which was not run
against it.

**Not run, with the reason:** Garage (AGPL-3.0, not permissive; no versioning
or Object Lock in its documentation); Ceph RGW (LGPL, and a cluster rather than
a fixture); Zenko CloudServer (its Docker Hub image last updated 2022; the
maintained one is on ghcr); MinIO forks (AGPL-3.0).

### 3.2 Decision

**SeaweedFS 4.48** is the maintained store: all five required properties
measured (conditional create, versioning with reads by version id, Object Lock
retention readback, path-style SigV4, a permissive licence with an active
upstream), Logweir's store layer and receipt path green against it, the longest
maintenance record of the candidates, and one container (`weed mini`) for
amd64 and arm64. It runs as the `objectstore` profile beside MinIO.

**RustFS 1.0.0** is the runner-up: the same evidence, plus MinIO's admin API,
which would let REPLACE-MINIO keep the chart's `mc admin` grant flow; against it
stands a two-week-old 1.0 line. REPLACE-MINIO should weigh that trade-off when
it starts: with SeaweedFS the chart renders identities into `s3.json` instead of
running `mc admin`.

**Limits:** SlowDown behaviour, scale and TLS on the store were not tested;
Object Lock enforcement was checked in GOVERNANCE mode only.

---

## 4. Profiles: names, ownership, evidence

Smoke at tip `a81a2c34` on 3.7.1 and 4.3.1 (27/27 each), and again in the fix
round on slot 2 with every profile, the digest-pinned 4.3.1 line and the
slot's own cluster ids (29/29, `fix-round/live/slot2-up-smoke.log`):

| Profile | Owner (first consumers) | Smoke |
|---|---|---|
| `auth` | PROD-01.3 | 8/8: PLAIN over TLS, SCRAM-SHA-256 and mTLS list topics from the host side; wrong password (PLAIN, SCRAM), wrong CA and missing client certificate refused |
| `cluster3` | the orchestrator until PROD-10.1 starts (FX-5 and PROD-05.1 use it first) | 5/5: three voters, RF 3 with ISR 3, `min.insync.replicas=2`, 30 records acked via three advertised ports; RF 4 refused |
| `cluster2` | PROD-11.1 (PROD-04.2, PROD-12.1) | 2/2: its own cluster id (on slot 2, `aWJU5TcuiI8b_OOhIWQL3Q` against the broker's `uCzUzs6QXUyPYC1O9M5csA`); serves its own topics from the host side |
| `objectstore` | PROD-09.1 (PROD-09.2, REPLACE-MINIO) | 6/6: four buckets via the published port; wrong secret refused; first create 200 and second 412; retention readback; retained version not deletable; read by version id |
| `registry` | PROD-03.0 (PROD-03.1, 03.2) | 4/4: register, read by id, incompatible version 409, the registration in `_schemas` |
| `streams` | PROD-06.1 (PROD-04.x, 06.2) | 4/4 (fix round; 2/2 at `a81a2c34`): two lines through the running app count exactly 2; the NEGATIVE CONTROL stops the app, produces a third line and requires the count to stay 2; restarted, the app counts the missed line (3); group Stable with changelog and repartition topics. (The earlier "never-produced word absent" check could not fail and is gone, review L5.) |
| `txn` | PROD-01.1 | reserved, not built here |

27/27 on each line (`profiles/tip-smoke-3.7.log`, `tip-smoke-4.3.log`); the
checks use a per-run nonce, so a second run on the same stack cannot pass on
leftovers. Two live mutants on 4.3.1 were killed and restored
(`mutants/live-*.log`): the mTLS listener without `ssl.client.auth=required`
(the missing-certificate check failed), and `kafka-cluster2` without its own
`CLUSTER_ID` (the distinct-id check failed). The smoke no longer needs this
host's helper (review L1): with a recording `timeout` first on `PATH`, every
container and request of the registry checks went through it (4 calls); with
no `timeout`, `gtimeout` or helper, it warned once and the same checks ran
unbounded and passed (`fix-round/live/l1-bound.log`).

**Owner** means the row that changes the profile's services next without
coordinating; any other row extends it with a new service or asks.

---

## 5. Acceptance rows for rows that use these fixtures

1. **PROD-01.3** — each new auth mode: fixture `auth` on a slot; pass = a
   backup and a restore through `localhost:<AUTH_*_PORT>` produce VALID
   evidence; negative control = the smoke's wrong password / wrong CA / missing
   certificate refusal, through Logweir's client.
2. **PROD-01.2** — broker rows: fixture `--kafka` per line; pass = the three
   paths of §2.2 and the protocol trace of §2.3 on every declared line;
   negative control = a line outside the declared set is reported untested.
3. **PROD-09.1** — lock readback: fixture `objectstore`, bucket
   `kafka-backups-locked`; pass = the product reports the provider's retention
   as read back; negative control = the same flow on `kafka-backups` (no lock)
   reports no provider retention.
4. **PROD-09.2** — secondary copy: fixture `objectstore`, `kafka-backups-2`;
   pass = the copy verifies against the primary archive; negative control = a
   deleted object in the copy is reported.
5. **PROD-10.1 / FX-5** — fixture `cluster3`; pass = the measured or proposed
   replication factor is 3 where the source is 3; negative control = RF 4 is
   refused on three brokers.
6. **PROD-03.0** — fixture `registry`; pass = a topic whose records carry a
   registered schema id is flagged; negative control = a plain-JSON topic is not.
7. **PROD-06.1** — fixture `streams`; pass = the application's group, changelog
   and repartition topics are listed as dependencies; negative control = a
   topic the application never touches is not.

---

## 6. Proposed child rows

- **C1 — Move the default broker line off 3.7.1.** Scope: `KAFKA_VERSION` in
  `scripts/extract-engine.sh`'s generated `.env` and in
  `.github/workflows/engine-matrix.yml` (neither is this row's), after PROD-00.1
  settles the engine matrix; plus the eleven
  `${KAFKA_IMAGE:-apache/kafka:${KAFKA_VERSION:-3.7.1}}` fallbacks in
  `e2e/compose/docker-compose.yml` and the `KV=${KV:-3.7.1}` fallback in
  `e2e/compose/profile-smoke.sh`. Acceptance: CI's e2e job green on the new
  default; 3.7.1 stays runnable with `--kafka 3.7`.
- **C2 — The rest of the stack readers.** Scope: the `crates/` `--features e2e`
  rows that dial `localhost:9092` / `localhost:9000`
  (`crates/logweir-kafka/tests/live.rs`, `crates/logweir-store/tests/minio_options.rs`,
  `crates/logweir/tests/{backup_run,catalog_minio,check_cli,doctor,restore_mode,topic_preflight,auth_binding,windowed_reconciliation}.rs`,
  `crates/logweir/src/doctor.rs`) read the same variables, so `just e2e` runs
  the whole workspace on a slot. Acceptance: `just e2e` on slot N passes while
  the default stack is down, and `stack_params.rs`'s sweep covers `crates/`.
- **C3 — A RocksDB Streams variant** if PROD-06 needs on-disk state: a
  glibc-based image carrying `libstdc++`. Acceptance: the smoke's counts after
  a restart come from the local store.
- **C4 — Object-store rows in `docs/support-matrix.md`** for SeaweedFS 4.48,
  RustFS 1.0.0 and versitygw v1.8.0 from §3 (that section is not this row's).
- **C5 — Engine protocol headroom** is PROD-00.1's "ApiVersions negotiation"
  capability row; §2.3 is its evidence. It covers DescribeConfigs v1 and
  DescribeGroups v0, and the three ACL entries (DescribeAcls, CreateAcls,
  DeleteAcls at v1, the 4.x floor), which have no call site in 0.21.0 but would
  fail the same way once one is added.
- **C6 — Classify non-standard S3 credential errors** (class sweep, product
  code): `StoreErrorClass::classify` (`crates/logweir-store/src/lib.rs:1844-1854`,
  `:1884-1887`) maps versitygw's `404 XAdminUserNotFound` to not-found.
  Acceptance: `minio_options.rs` 6/6 against versitygw v1.8.0; a store-layer
  unit row per known non-standard credential code, and a negative control where
  a genuine `NoSuchKey` stays not-found.
- **C7 — `just links` covers `e2e/README.md`** (class sweep, `justfile:280`):
  the new guide sits outside the standing link gate's path list. Acceptance:
  the recipe names it, and a broken link in it fails `just links`.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
