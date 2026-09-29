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
parameters existed. The default stack is shared: on the orchestrated run, hold
`claude/compose-lock.sh` while you use it.

## Parallel stacks: slots

A **slot** is a complete, private copy of the stack. Slot N (1 to 4) is
compose project `logweir-e2e-sN`, and every published host port is the default
plus N×10000, so slots never share a project, a port, a network or a volume and
need no lock:

```sh
eval "$(e2e/compose/stack-env.sh --slot 2)"   # project logweir-e2e-s2, ports 29092, 29000, …
just e2e-up
./scripts/e2e-seed.sh && just e2e              # or: just demo / just pitr / just mvp-demo
just e2e-down
eval "$(e2e/compose/stack-env.sh --slot 0)"    # back to the default stack
```

`stack-env.sh` prints `export` lines and validates every argument before it
prints anything. The variables it sets are the whole interface:

| Variable | Default | What it moves |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `logweir-e2e` | the project (compose's own override of `name:`) |
| `LOGWEIR_E2E_KAFKA_PORT` | 9092 | `EXTERNAL` host port **and** its advertised `localhost:<port>` |
| `LOGWEIR_E2E_K8S_PORT` | 9095 | `K8S` host port and its advertisement |
| `LOGWEIR_E2E_SASL_PORT` | 9097 | `SASLEXT` host port and its advertisement |
| `LOGWEIR_E2E_S3_PORT` | 9000 | MinIO's S3 API |
| `LOGWEIR_E2E_S3_CONSOLE_PORT` | 9001 | MinIO's console |
| `KAFKA_VERSION` | `.env`'s pin (3.7.1) | the broker line (`--kafka`) |
| `COMPOSE_PROFILES` | none | the optional profiles (`--profiles`) |

Profile ports (below) follow the same rule. The in-network addresses
(`kafka-broker-1:9094`, `minio:9000`) never move: each slot has its own network.

**A half-set environment is refused.** `just e2e-up`, `just e2e-down`, the
demo scripts and `scripts/e2e-seed.sh` run `stack-env.sh --check` first. The
default project with a moved port would make `up` recreate the default stack
under whoever is using it, and `down` remove it; a new project on a default port
collides with the default stack. Set everything at once with `stack-env.sh`.

**What reads the variables.** The scripts source
[`compose/stack-lib.sh`](compose/stack-lib.sh); the Rust harness reads
[`tests/harness/stack.rs`](tests/harness/stack.rs). On a slot, the harness and
`scripts/demo.sh` / `scripts/mvp-demo.sh` rebind the checked-in examples'
`localhost:9092` and `http://localhost:9000` to the slot's ports, and write to
`.e2e/<project>/` and `.demo/<project>/` instead of `.e2e/` and `.demo/`, so two
slots can run from one checkout. `tests/stack_params.rs` fails if the compose
file, `stack-lib.sh`, `stack-env.sh` and `stack.rs` disagree on any name,
default or port, if a published port or a host-facing advertisement is a
literal, if `just e2e-down` misses a profile, or if an e2e suite or stack script
spells a default-stack address in code.

**What does not move yet.** On a non-default slot `just e2e` runs only the `e2e`
package: the `--features e2e` rows under `crates/` still dial
`localhost:9092` / `localhost:9000` and would reach the default stack, so they
run on the default stack. `scripts/k8s-demo.sh`, `kind-demo.sh`,
`laptop-demo.sh` and `demo-steps.sh` also address the default stack only (they
install Logweir into a cluster, which the orchestrated run forbids besides).

## Broker lines

`--kafka LINE` sets `KAFKA_VERSION` to the line's pinned patch; an exact version
(`--kafka 4.2.2`) is accepted too. `e2e/compose/.env` keeps the default at
3.7.1, which `scripts/extract-engine.sh` writes.

| Line | Pinned image | Status |
|---|---|---|
| 3.7 | `apache/kafka:3.7.1` | **legacy**: Apache support ended, MSK support ended 2026-09-01; still the default fixture |
| 3.9 | `apache/kafka:3.9.2` | supported line |
| 4.1 | `apache/kafka:4.1.2` | supported line |
| 4.3 | `apache/kafka:4.3.1` | supported line |

What each line proved (the demo drill, `just pitr`, the receipt path and the
engine's fixed protocol versions) is in
[`docs/support-matrix.md`](../docs/support-matrix.md), "Broker versions".

```sh
eval "$(e2e/compose/stack-env.sh --slot 3 --kafka 4.3)"
just e2e-up && bash scripts/demo.sh && just pitr
just e2e-down && just e2e-up && just mvp-demo && just e2e-down
```

## Optional profiles

`--profiles a,b` (or `COMPOSE_PROFILES=a,b`) adds profiles to the stack.
`just e2e-up` brings them up in the same `--wait` (each profile's setup feeds a
long-running service with a healthcheck); `just e2e-down` removes every profile.
`compose/profile-smoke.sh` smokes the active profiles, one negative control per
behaviour, from the host side (published ports) and in-network.

| Profile | Services | What it provides | Ports (default) | Owner (first consumers) |
|---|---|---|---|---|
| `auth` | `kafka-auth` (+ certs, setup) | A separate single-node cluster: SASL_SSL/PLAIN on 9102, SASL_PLAINTEXT/SCRAM-SHA-256 on 9103, SSL with a required client certificate on 9104, all advertised as `localhost:<port>`. User `logweir`, password `logweir-e2e-not-a-secret`. A throwaway test CA, broker and client certificates and a wrong CA in `.e2e/auth/<project>/`. | 9102-9104 | PROD-01.3 |
| `cluster3` | `kafka-c3-1..3` | A three-node KRaft cluster, replication factor 3 and `min.insync.replicas` 2 by default | 9112-9114 | PROD-10.1 (FX-5, PROD-05.x) |
| `cluster2` | `kafka-cluster2` (+ setup) | A second single-node cluster with its own cluster id and the marker topic | 9122 | PROD-11.1 (PROD-04.2, PROD-12.1) |
| `objectstore` | `objectstore` (+ setup) | SeaweedFS 4.48 beside MinIO: `kafka-backups`, `logweir-evidence`, `kafka-backups-locked` (Object Lock) and `kafka-backups-2`, credentials `minioadmin`/`minioadmin` | 9130 | PROD-09.1 (PROD-09.2, REPLACE-MINIO) |
| `registry` | `registry` | Karapace 6.2.3, Schema-Registry-compatible, schemas in `_schemas` on `kafka-broker-1`, BACKWARD compatibility | 9141 | PROD-03.0 (PROD-03.1, 03.2) |
| `streams` | `streams-wordcount` (+ topics) | Apache Kafka's WordCountDemo from the broker line's own image, group `logweir-e2e-wordcount`, in-memory state stores | none | PROD-06.1 (PROD-04.x, 06.2) |
| `txn` | reserved | The transactional producer PROD-01.1 builds | — | PROD-01.1 |

The **owner** changes a profile's services without asking; anyone else extends
it by adding a service, or asks the owner. A profile is torn down by
`just e2e-down` like the rest; the auth certificates stay in
`.e2e/auth/<project>/` (gitignored) and are reused by the next `up`.

Limits worth knowing: the auth listeners are host-facing (an in-network client
uses `kafka-auth:9094`, plaintext); the Streams application keeps its state in
memory, because RocksDB's native library needs `libstdc++`, which the
Alpine-based `apache/kafka` image lacks; `objectstore` does not replace MinIO
(that is REPLACE-MINIO); every behaviour above was measured with Docker Compose
v5.0.2.

## Extending the fixtures

Later tasks extend these profiles instead of building private fixtures:

1. Add the service to `compose/docker-compose.yml` under a profile (a new one or
   an existing one you own), on `kafka-net`.
2. Publish a port only as `"${LOGWEIR_E2E_<NAME>_PORT:-<default>}:<container>"`
   with an unused default below 9152, and advertise any host-facing listener
   with the same variable.
3. Register the port in `tests/harness/stack.rs` (`PROFILE_PORTS`) and in
   `compose/stack-env.sh` (`PORTS`), and a new profile in `PROFILES_LIST` and on
   the `just e2e-down` line.
4. Make setup a dependency of, or itself, a long-running service with a
   healthcheck; a bare one-shot in the `up` set makes `--wait` fail.
5. Give the profile a smoke function with a negative control in
   `compose/profile-smoke.sh`, and run `cargo test -p e2e --test stack_params`.

---

Documentation is licensed [CC-BY-4.0](../docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
