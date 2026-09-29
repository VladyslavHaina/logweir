# `e2e/fixtures/`

Test fixtures. Nothing here is output the shipping code produced; each file is
either hand-authored to exercise a path, or captured from a real archive and
committed deliberately.

## `scorecard-pass.json` — the format example

This is the spec §6.1 example with its `//` annotations stripped. Four tests
depend on it byte-for-byte
(`crates/logweir-core/tests/scorecard_golden.rs`), the `logweir drill show`
golden renders it, and `crates/logweir/tests/fixtures/mod.rs` parses it into the
`scorecard_pass()` helper the unit suites share.

**One field in it still shows a value v0.1's code cannot emit, and one no longer
does. Both are stated here so nobody reads the example as a description of
output.**

- **`engine_subreport` is POPULATED. The shipping code always emits `null`.**
  `OsoCliEngine` does not override `DataEngine::validation_run`, so the engine's
  own `validation run` is never invoked and no report is retained. The block is
  kept populated because it is the only checked-in example of the format, and
  because `crates/logweir-engine-oso/tests/engine.rs` carries an `#[ignore]`d
  marker test — run on every CI build — that turns green the day the override
  lands. Reading `engine_subreport: null` in a real scorecard means "no engine
  sub-report was retained", **not** "the engine reported nothing wrong". See
  `docs/stability.md`.

- **`last_phase_completed` was `9` and is now `7`, matching the code.** No real
  drill can emit `9` in a SIGNED document: `phase8_score::run` is handed a
  frozen clone, so phase 8's own record and phase 9's teardown are both pushed
  after the bytes were signed, and a v0.1.0 signed scorecard ends at 5, 6 or 7.
  This file is unsigned, so it was corrected in place rather than documented as
  a divergence — the same treatment `create_only_enforced` got below, for the
  same reason. `drill run`'s stdout line quotes the artifact's value, so the
  console and the document cannot disagree about it. The signed fixtures under
  `signed/` were re-minted from the same generator and now read `7` too; the
  `-1..=9` DOMAIN is untouched (Global Constraint 18) — only the pinned value
  moved. See `signed/README.md`.

- **`evidence.create_only_enforced` was `true` and is now `false`, matching the
  code.** Task 20 made phase 8 zero all four `evidence` fields immediately
  before signing, because they describe an upload that has not happened yet, so
  `true` became a value the shipping code can never produce. It was left
  un-regenerated on the stated grounds that regenerating would invalidate a
  signature — **but this file is not signed**, so that reason did not apply to
  it, and Task 22 corrected the value rather than documenting a divergence that
  did not have to exist. The real post-put readback lives in the separately
  signed storage receipt.

  The signed fixtures under `signed/` used to read `true` and no longer do: the
  keypair is pinned, so re-minting them re-signs under the SAME key and the
  stated reason stopped applying. `Scorecard::validate_invariants` and
  `docs/verify_scorecard.py::check_invariants` now both REFUSE a `1.0.x`
  scorecard with any of the four `evidence` fields set, so no signed document —
  in this directory or anywhere — can carry the claim again. See
  `signed/README.md`.

## `signed/`

Throwaway P-256 key pair and the documents it signs. See `signed/README.md`.
The private key is checked in on purpose and signs nothing outside this
repository's test suite.

## `segments/`, `manifests/`

`segments/upstream-0.21.0.kbak` and `manifests/0.21.json` are captured from a
**real** archive produced by the digest-pinned engine, refreshed by
`scripts/e2e-seed.sh`. They are **not byte-reproducible** — each record carries
its own produce timestamp, so the zstd frames and the manifest timestamps differ
on every run. A CI job that seeds must never `git diff --exit-code` afterwards.
The committed pair is checked instead by the Docker-free default test set, in
`crates/logweir-engine-oso/tests/kbak.rs`.

The other segment files (`lz4.kbak`, `none.kbak`, `zstd.kbak`) are
codec-coverage fixtures.

## `consumer-groups-snapshot*.json` — the engine's own bytes (FX-1)

`consumer-groups-snapshot.json` and `consumer-groups-snapshot-empty.json` are
the object `<backup_id>/consumer-groups-snapshot.json` exactly as the
digest-pinned engine wrote it, copied out of the bucket unchanged (the engine
writes no trailing newline). They replace a hand-authored file whose shape no
engine writes (`captured_at`, a per-group `state`, `offsets` as a list).
`consumer-groups-snapshot.committed.json` is the oracle: the broker's own
account of what the groups had committed.

| File | sha256 | What it is |
| --- | --- | --- |
| `consumer-groups-snapshot.json` | `fbc05fcfb607e116e79c3ea82cffec9235be5938250f2b5f604da1f8d574231a` | four groups, ten positions |
| `consumer-groups-snapshot-empty.json` | `79ee9b577d2d9be42f5f5bc3799af5f1197d6070759d86c58a39bbaf281b0e31` | the snapshot the engine writes when no group has committed |
| `consumer-groups-snapshot.committed.json` | — | `kafka-consumer-groups --describe --all-groups --offsets`, distilled |

**Engine.** `kafka-backup 0.21.0` (`kafka-backup --version`), image
`osodevops/kafka-backup@sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317`,
the pin in `third_party/kafka-backup-binary.digest`. The writer is
`snapshot_consumer_groups` in `crates/kafka-backup-core/src/backup/engine.rs`
of `third_party/kafka-backup-v0.21.0.tar.gz` (lines 846-933).

**How they were made** (2026-09-29, on compose slot 1, `logweir-e2e-s1`:
`apache/kafka:3.7.1` in KRaft mode, the `confluentinc/cp-kafka:7.6.0` tools and
the MinIO mirror):

1. `just e2e-up` on a fresh stack, then 1000 records into each of `orders` and
   `payments` with `kafka-console-producer`, as `scripts/e2e-seed.sh` does.
2. With no consumer group on the cluster, one backup under
   `backup_id: fx1-empty`. That wrote `consumer-groups-snapshot-empty.json`.
3. Five groups committed, each a case the snapshot must get right:
   - `fx1-orders-reader`: a real consumer (`kafka-console-consumer --group
     fx1-orders-reader --from-beginning --max-messages 300`), committing on
     close;
   - `fx1-payments-set`: `kafka-consumer-groups --reset-offsets --execute` to
     11, 22 and 33 on `payments` partitions 0, 1 and 2;
   - `fx1-two-topics`: 42 on `orders` 0 and 1, and 9 on `payments` 2;
   - `fx1-unarchived-only`: 0 on `test-topic` 0 only, a topic the backup does
     not archive;
   - `fx1-mixed`: 17 on `orders` 2, and 0 on `test-topic` 1.
4. One backup under `backup_id: drill-demo`. That wrote
   `consumer-groups-snapshot.json`. The engine left out `fx1-unarchived-only`
   and `fx1-mixed`'s `test-topic` position: it keeps only committed offsets
   `>= 0` on archived topics.

Both backups used `e2e/compose/config/backup-drill.yaml` with one key added,
`backup.consumer_group_snapshot: true` (the prefix and `backup_id` set as
named above), and ran as
`docker run --rm --platform linux/amd64 --network logweir-e2e-s1_kafka-net -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -v <config dir>:/fx1:ro --entrypoint kafka-backup osodevops/kafka-backup@sha256:8ff5be71… backup --config /fx1/<backup_id>.yaml`.
The bytes were copied with
`mc cat local/kafka-backups/<prefix>/<backup_id>/consumer-groups-snapshot.json`.
`kafka-consumer-groups --describe --all-groups --offsets` was read before and
after step 4, and the two agree.

**Not byte-reproducible.** `snapshot_time` is the wall clock, and the writer
serialises `HashMap`s, so group and key order change between runs. The tests
therefore compare parsed values, never bytes:
`crates/logweir-engine-oso/tests/vendored_parse.rs` checks every position
against the oracle, and `crates/logweir-engine-oso/tests/engine.rs` reads both
files through `OsoCliEngine`.

## `fake-engine*.sh`, `engine-docker.sh`, `dryrun/`, `drill-*.yaml`

Harness scripts and specs that drive named failure paths — an engine that exits
non-zero, one that drops a rendered key, an archive with no backup set, an
unreachable target. `engine-docker.sh` is the container route the e2e suite and
`scripts/demo.sh` fall back to on a host that cannot exec a linux/amd64 ELF; it
invokes nothing itself and forwards argv verbatim.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
