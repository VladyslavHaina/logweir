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
  `signed/` still read `9` and say so in their own README.

- **`evidence.create_only_enforced` was `true` and is now `false`, matching the
  code.** Task 20 made phase 8 zero all four `evidence` fields immediately
  before signing, because they describe an upload that has not happened yet, so
  `true` became a value the shipping code can never produce. It was left
  un-regenerated on the stated grounds that regenerating would invalidate a
  signature — **but this file is not signed**, so that reason did not apply to
  it, and Task 22 corrected the value rather than documenting a divergence that
  did not have to exist. The real post-put readback lives in the separately
  signed storage receipt.

  The signed fixtures under `signed/` still read `true`, and there the stated
  reason **does** apply: regenerating them would invalidate the signatures those
  files exist to exercise. See `signed/README.md`.

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

## `fake-engine*.sh`, `engine-docker.sh`, `dryrun/`, `drill-*.yaml`

Harness scripts and specs that drive named failure paths — an engine that exits
non-zero, one that drops a rendered key, an archive with no backup set, an
unreachable target. `engine-docker.sh` is the container route the e2e suite and
`scripts/demo.sh` fall back to on a host that cannot exec a linux/amd64 ELF; it
invokes nothing itself and forwards argv verbatim.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
