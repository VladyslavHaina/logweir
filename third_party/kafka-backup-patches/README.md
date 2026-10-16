# third_party/kafka-backup-patches/

Logweir builds the `kafka-backup` engine from the vendored OSO source
(`third_party/kafka-backup-v<release>.tar.gz`), and this folder is the ordered
set of changes Logweir carries on top of that source. OD-3 (decided
2026-10-07, `docs/to-do/product-expansion.md`) is the decision; ADR 0002's
amendment in `docs/architecture.md` records it.

Without patches, Logweir's build is the OSO release's own source, built by
Logweir; it still carries Logweir's version identity (below), so nobody
mistakes it for OSO's binary.

| Patch | What it fixes | Oracle |
| --- | --- | --- |
| `0001-lockfile-rustls-h2-spin.patch` | The engine's Cargo.lock only: rustls 0.23.43 → 0.23.46 (RUSTSEC-2026-0285; since build 4 also RUSTSEC-2026-0346 to -0349, which the gate found in 0.23.45) with rustls-webpki 0.103.13 → 0.103.15, h2 0.4.15 → 0.4.19 (RUSTSEC-2026-0258), and the yanked spin 0.9.8 → 0.9.9. The first run of the engine's `cargo deny` gate (PROD-00.2) found all three in the shipped graph. Logweir's own graph carries the same rustls 0.23.46, rustls-webpki 0.103.15 and h2 0.4.19 (it has spin 0.10.1, not 0.9). | `scripts/ci-check.sh`'s engine `cargo deny` (fails on the unpatched lock), and PROD-00.2's parity suites |
| `0002-manifest-replication-factor.patch` | `merge_manifests` (`crates/kafka-backup-core/src/backup/engine.rs`) carries `source_replication_factor` from the current session into a topic the stored manifest already holds, as it already did `original_partition_count`. Without it the manifest records the factor for the FIRST topic a backup saves only: the configuration capture puts every topic into the manifest before the first save, so each later topic is already stored without a factor and the merge kept that. Found by PROD-05.1, fixed by FX-21 (Logweir's phase 7 had read the missing factor as matching). An upstream bug-class fix. | The patch's own engine unit tests (`test_merge_manifests_updates_replication_factor`, `…_preserves_replication_factor_when_none`, `test_manifest_persistence_keeps_every_topics_replication_factor`; the first and third fail on the unpatched source), and FX-21's e2e row `e2e/tests/replication_factor_parity.rs` (a three-topic backup on `cluster3` records every topic's factor) |
| `0003-restore-checkpoint-interval-and-hash.patch` | The restore checkpoint (`crates/kafka-backup-core/src/restore/`, C4). It was saved only after each topic, and `restore.checkpoint_interval_secs` was read by nothing: now a segment's completion also saves it once the interval has passed since the last save (`0` saves after every segment). Its hash covered `checkpoint_state` and `offset_report`, so the same restore run with other paths discarded its own checkpoint, and it serialised `topic_mapping`, a `HashMap`, in per-process order, so two processes could hash one two-topic document differently: now it leaves both paths out and hashes sorted keys. Found by PROD-07.1 (rows K1, H1, D1), fixed by PROD-00.3g. Logweir rendered a per-run checkpoint path and never read the file, so nothing Logweir did changed; since PROD-07.2 it renders no checkpoint at all. An upstream bug-class fix. A resumed restore still has no offset mapping for the segments it skips (07.1-G3); since this patch that can be part of a partition, and Phase 3's lookup then extrapolates those offsets, so offsets must not be applied after a resume (Logweir never resumes, and renders `reset_consumer_offsets: false` and `auto_consumer_groups: false`). | The patch's own engine unit tests (`restore_config_hash_ignores_the_checkpoint_and_report_paths` and `…_does_not_depend_on_map_iteration_order`, both failing on the unpatched source; `save_checkpoint_if_due_honours_the_interval`), and `e2e/tests/resume_semantics.rs`'s ignored PROD-00.3g rows (a checkpoint saved inside a topic; one restore recognised from another directory), which fail on build 2 |
| `0004-committed-only-capture.patch` | A backup's capture (`crates/kafka-backup-core/src/kafka/fetch.rs`, C1). The engine fetched and listed offsets `READ_UNCOMMITTED`, so aborted transactions' records, the records of a transaction still open, and every commit and abort marker were archived as ordinary records. Now the Fetch and both ListOffsets are `READ_COMMITTED`, so a capture ends at the last stable offset; control batches are skipped, and a transactional batch of a producer the fetch response lists as aborted is dropped, by the consumer's own algorithm; the next fetch offset still advances over every skipped batch. Always on. Found by PROD-01.1, fixed by PROD-00.3a (OD-23). An upstream bug-class fix. | The patch's own engine unit tests (`a_committed_transaction_is_kept_and_its_marker_skipped`, `an_aborted_transaction_and_its_marker_are_dropped_and_the_offset_advances`, `only_the_aborted_producers_batches_inside_its_transaction_are_dropped`, `an_abort_starting_later_does_not_drop_earlier_batches`), and `e2e/tests/record_semantics.rs`'s TXN row (EN-1: the archive is the committed input, no marker, and the restore needs no override), which fails on build 4 |
| `0005-idempotent-produce.patch` | A restore's produce (`kafka/produce.rs`, `kafka/partition_router.rs`, C5). A batch whose acknowledgement was lost was re-sent as a new batch and appended twice. Now the restore's router is one idempotent producer (one `InitProducerId` without a transactional id) and every batch carries its producer id, epoch and the partition's next sequence number, which advances only when the batch is acknowledged; every resend, by the client's reconnect or the router's retries, carries the same sequence, so the broker discards the copy. `OUT_OF_ORDER_SEQUENCE_NUMBER` or `UNKNOWN_PRODUCER_ID` fails the restore. The target principal needs `Write` on the topic (`IdempotentWrite` is implied since Kafka 3.0). Found by PROD-01.1 §5.1, fixed by PROD-00.3d (OD-23). An upstream bug-class fix. | The patch's own engine unit tests (`an_idempotent_batch_carries_the_producer_and_its_sequence`, `a_resend_before_the_acknowledgement_is_the_same_batch`, `the_sequence_advances_per_partition_only_when_acknowledged`), and `record_semantics.rs`'s `a_lost_produce_acknowledgement_during_restore` on the `faultproxy` profile (EN-2: a dropped acknowledgement leaves no duplicate), which fails on build 4 |
| `0006-header-list.patch` | Repeated header keys (`kafka/header_list.rs`, `kafka/fetch.rs`, `kafka/produce.rs`, C13). `kafka-protocol`'s `Record.headers` is an `IndexMap`, so a header key repeated in one record collapsed to one entry, at the first copy's position with the last copy's value, on capture and again on restore. Now both of the crate's compression hooks are used, without forking it: the decompressor keeps the decompressed records and a record-v2 header reader fills `BackupRecord.headers` as an ordered list; the compressor writes the engine's own encoding of the records, headers as a list, compressed with the batch's codec, and the crate fills the batch's size and CRC. Found by PROD-01.1 §2.4, fixed by PROD-00.3e (OD-23). An upstream bug-class fix (to `kafka-protocol-rs`). | The patch's own engine unit tests (`repeated_header_keys_survive_an_encode_and_a_decode` over every codec, with the stock decoder reading the same batch and collapsing it; `a_batch_without_repeated_keys_reads_as_the_crate_wrote_it`; `repeated_header_keys_are_kept_in_order`), and `record_semantics.rs`'s shapes row (EN-3: capture, replay and end to end exact, the verdict `pass`), which fails on build 4 |
| `0007-byte-rate-limit.patch` | `restore.rate_limit_bytes_per_sec` (`restore/helpers.rs`, `restore/engine.rs`, `restore/repartition.rs`, C12) was parsed and validated, and paced nothing. Now it limits the WHOLE restore: one next-allowed instant shared by every partition task, each batch reserving its key and value bytes over the limit before it is produced, at both produce sites beside the records limit. Absent, nothing waits. Logweir renders it from the plan's `restore.rate_limit_bytes_per_sec`. Fixed by PROD-00.3h (OD-23). An upstream bug-class fix. | The patch's own engine unit tests (`the_byte_rate_limit_paces_the_whole_restore`, two tasks sharing one limit; `no_byte_rate_limit_waits_for_nothing`), and `record_semantics.rs`'s `a_byte_rate_limit_paces_the_whole_restore` (EN-4: the limited restore's throughput is at most 1.2 times the limit), which fails on build 4 |

## The policy: patch first

1. **A fix lands here as soon as its oracle passes.** The carrying window is
   zero: a fix is not held back for an upstream release.
2. **One line of reason, in the patch itself.** The first line of every patch
   file is `Reason: <one line>`: what the patch fixes and which oracle shows
   it (for example the PROD-00.3 row and its acceptance id).
3. **Upstream is optional, and comes after shipping.** A patch may carry an
   `Upstream: <https URL>` line, the second line, naming the upstream pull
   request or issue. Upstream PRs are for bug-class fixes only (C1, C4, C5, C6,
   C12, C14 to `kafka-backup`; C13 to `kafka-protocol-rs`), never the
   filter-rule or SASL-plugin patches.
4. **A patch is dropped when an OSO release contains it.** Moving the pin
   (PROD-00.3f's refresh procedure) re-applies every patch to the new source;
   a fix the new release already contains no longer applies cleanly, the
   build refuses, and the patch is deleted in the same change that moves the
   pin.

## The format

- File names are `NNNN-<slug>.patch`: four digits, then lowercase letters,
  digits and hyphens. The digits order the set, they are unique, and the
  build applies the patches in that order. Nothing else lives here except
  this README.
- Line 1 is `Reason: <one line>`. Line 2 is either blank, or
  `Upstream: https://…` followed by a blank line 3. Then comes a unified diff
  against the extracted source root (`a/Cargo.lock`, `a/crates/…`), as
  `git diff` writes it.
- The build applies each patch with `git apply` (exact context, no fuzz), so
  a patch that does not apply cleanly fails the build instead of landing
  somewhere else.

`scripts/engine-source.sh check` enforces the format, and
`crates/logweir/tests/engine_build.rs` enforces it again in the default test
set, each with negative controls.

## The version identity, and what changes with the folder

`third_party/kafka-backup-build.env` names Logweir's build:

- `ENGINE_VERSION=<release>+logweir.<n>`: what `kafka-backup --version`
  prints for Logweir's build (`kafka-backup 0.23.3+logweir.5`). `<n>` counts
  Logweir's builds of one OSO release, from 1.
- `ENGINE_DIGEST=sha256:<hex>`: the build-input digest, over the tarball's
  sha256, every patch's name and sha256 in order, and `ENGINE_VERSION`.
  `scripts/engine-source.sh digest` prints it.

**Any change to a patch or to the tarball is a new build:** bump `<n>`,
re-record `ENGINE_DIGEST` (`scripts/engine-source.sh digest`), and APPEND the
pair to `third_party/kafka-backup-builds.txt`, the ledger of every build, and
to `SHIPPED_BUILDS` in `crates/logweir/tests/engine_pin.rs`. That is enforced,
not only asked:

- the build and the test set refuse a digest that does not describe the
  inputs;
- `scripts/engine-source.sh` refuses a ledger whose last line is not the build
  env's pair, a version or a digest recorded twice, and an `<n>` that does not
  rise within a release;
- `engine_pin.rs` refuses a ledger that does not begin with every shipped
  build, unchanged, so a recorded line rewritten in place to describe new
  inputs under an old version fails too.

So two different engines never share one version. (An edit to this README
changes no digest and needs no bump.)

**The stamp is the CLI's `--version` only.** Inside the engine,
`env!("CARGO_PKG_VERSION")` still reads the OSO release (`0.23.3`): for
example the `tool_version` of the engine's own `validation run` evidence, which
Logweir never runs, and the crate versions an SBOM of the image lists for
`kafka-backup-cli` and `kafka-backup-core`. Logweir's identity is the CLI's
`--version`, `/etc/logweir/engine-identity` and the build env, never those.
`logweir doctor`, the controller's Job environment, the runner image and every
signed scorecard and receipt name this identity, so they move together
(`crates/logweir/tests/engine_pin.rs`).

Upstream's MIT licence for the source is `third_party/LICENSE-MIT`. Logweir's
patches are Apache-2.0, as the rest of this repository.
