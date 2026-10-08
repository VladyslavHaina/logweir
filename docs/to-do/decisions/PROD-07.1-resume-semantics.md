# PROD-07.1 — Restore checkpoint and delivery semantics: the resume contract

- Row: PROD-07.1 (research, Tier B), [product-expansion tracker](../product-expansion.md#prod-071--resolve-checkpoint-and-delivery-semantics). Ledger row: `| 1 | PROD-07.1 | Resolve checkpoint and delivery semantics | P2 | M3 | research | 01.1 | — | none | B | Proposed |`.
- Date: 2026-10-08. Branch `claude/prod-07-1`, from main `740a4875`.
- Kind: research. Source first, then measured on compose slot 1. It ships no product code: the one file it adds outside this record is the `#[ignore]`d harness `e2e/tests/resume_semantics.rs`.
- Status: **proposed**. The default contract (§5.2) and the recommended resume route (§5.3) are recommendations for the orchestrator's review; the ledger changes in §8 are the orchestrator's to record.
- Inputs: [PROD-00 engine route](PROD-00-engine-route.md) §3.4 (C4), §3.5 (C5), §9 (00.3g, 00.3i), §12 (0.23.3); [PROD-01.1](PROD-01.1-record-semantics.md) §5 and §7 (rows 07-1 to 07-4); [PROD-01.4](PROD-01.4-topic-identity.md) §7 and §8 (rows TI-07.1-1 to -3); [PROD-08.1](PROD-08.1-integrity-contract.md) §2 (the oracle). OD-3 as decided on 2026-10-07: Logweir builds the engine with its own patch folder, patch first.

TODO-SUMMARY

## 1. Evidence base

| Item | Identity |
|---|---|
| Engine source | `third_party/kafka-backup-v0.23.3.tar.gz`, sha256 `bf5544bd521f0a0f343c402bbbde5d6dc0d9b45d70eb1a1efb447ca4f2fda0bd`, tag `v0.23.3` = commit `afb160e7f2c69b7c3c28e1b868dd952835a5b0af` ([PROD-00 §12.1](PROD-00-engine-route.md#121-the-target)) |
| Engine image (runs) | `osodevops/kafka-backup@sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db`, linux/amd64; `bash scripts/extract-engine.sh` on this branch verified the revision label and printed `kafka-backup 0.23.3` |
| Lab | TODO-LAB |
| Harness | `e2e/tests/resume_semantics.rs`, `the_pinned_engines_restore_checkpoint_under_interruption` |
| Oracle | PROD-08.1's complete verification: `logweir::drill::phase7_verify::run_with_coverage(…, Coverage::Complete, …)` over the slot's broker and MinIO |
| Artifacts | `/tmp/logweir-roadmap-run/claude/artifacts/prod-07-1/` on the worker host: one outcome file per row, the run log, the engine logs |

**Citation form.** `C23/<path>:<line>` is `crates/kafka-backup-core/src/<path>` and `CLI23/<path>:<line>` is `crates/kafka-backup-cli/src/<path>`, both in the vendored 0.23.3 tarball (the form [PROD-00 §12](PROD-00-engine-route.md#12-prod-003f-the-move-to-0233-2026-10-07) uses). `L/<path>:<line>` is this repository at `740a4875`.

## 2. The engine's restore checkpoint, from source (0.23.3)

### 2.1 What it is

A JSON file holding a `RestoreCheckpoint` (`C23/manifest.rs:482-508`): `backup_id`, `start_time`, `last_checkpoint_time`, `segments_completed` (segment storage keys), `segments_in_progress`, `records_restored`, `bytes_restored` and `config_hash`.

Only two fields do anything:

- **`segments_completed`** is read once per partition at the start of its restore, and every listed segment is skipped (`C23/restore/engine.rs:1714-1721`, `:1735-1739`).
- **`config_hash`** decides whether the loaded file is used at all (`:744-768`).

The rest is inert. `records_restored` and `bytes_restored` are never incremented (the "Loaded checkpoint: N segments completed, 0 records restored" line at `:1366-1369` always prints 0), `segments_in_progress` is written by nothing in the crate (`update_segment_progress`, `C23/manifest.rs:534-540`, has no caller), and `backup_id` is stored but never compared. A segment key carries the backup id (`{backup_id}/topics/{topic}/partition={p}/segment-{offset}.bin…`, `C23/backup/engine.rs:1494-1500`), so a checkpoint of another backup skips nothing; it does not detect a different generation written under the SAME backup id.

### 2.2 Where it lives

Wherever `restore.checkpoint_state` points; absent, the engine keeps no checkpoint (`C23/config.rs:940-941`, `C23/restore/engine.rs:744`). Logweir always renders it, under the attempt's run id: `checkpoint_state: <TMPDIR>/logweir-<run_id>/checkpoint.json` (`L/crates/logweir/src/drill/mod.rs:3455-3457`, `:3491`). The run id is minted per process (`L/crates/logweir/src/drill/mod.rs:1098`). Under Kubernetes `TMPDIR` is `/work`, an `emptyDir` (`L/crates/weirkeeper/src/job.rs:199`, `:405`, `:832`) of a Job with `backoffLimit: 0` and `restartPolicy: Never` (`:152`, `:160`): one pod per Job, and the file dies with the pod. Nothing uploads it ([`docs/stability.md`](../../stability.md#a-crashed-restore-is-not-resumable-in-v01): "pod-local and is never uploaded").

### 2.3 What it hashes

`restore_config_hash` is sha256 over `serde_json::to_vec(&RestoreOptions)`, plus the record-filter fingerprint when one is set (`C23/restore/engine.rs:2131-2143`). `RestoreOptions` (`C23/config.rs:850-1074`) holds every key under `restore:`: the topic mapping, the time window, the produce settings, `circuit_breaker` (new in 0.23.0, PROD-00 §12.2), and both of Logweir's per-run paths, `checkpoint_state` (`:941`) and `offset_report` (`:957`).

- **In the hash, and should not be:** the two paths. Every Logweir attempt renders a different run id into both, so every attempt's hash differs and a carried checkpoint is discarded (C4).
- **Not in the hash, and should be:** the target cluster (bootstrap servers, cluster id, credentials are under `target:`, not `restore:`), the target topics' identity, the archive's manifest digest, and any execution identity. A checkpoint is therefore accepted against a different cluster, a recreated topic of the same name, or a rewritten archive under the same backup id, as long as the `restore:` block is byte-identical.
- **On a mismatch** the engine logs `Restore configuration changed since the checkpoint was written (config hash mismatch); restarting from the beginning` at WARN and starts over (`:750-760`). It does not refuse.
- **A file that does not parse** fails the run before anything is produced: `load_checkpoint` reads and deserialises with `?` (`:1361-1373`), and that error ends `run_internal` (`:747`).

### 2.4 When it is saved

Only in `run_internal`'s per-topic loop, after each topic, whether the topic succeeded or failed (`C23/restore/engine.rs:905-937`); never per segment or per interval. `restore.checkpoint_interval_secs` is parsed (`C23/config.rs:944-945`, default 60 at `:1176-1178`) and read by nothing under `restore/` (the only readers of a `checkpoint_interval_secs` are the backup engine and the offset store). Logweir renders `checkpoint_interval_secs: 30` (`L/crates/logweir-engine-oso/src/render_restore.rs:147-148`), a key with no effect. Three paths skip the save:

- a topic error while the engine's health is `Unhealthy` returns at once (`:928-929`);
- an error before the loop (manifest, preflight, connection, topic creation) never reaches it;
- a kill.

`save_checkpoint` writes with `tokio::fs::write` (`:1376-1388`), which truncates and then writes: a kill during the save leaves a truncated file, which fails the next attempt (2.3, last item).

### 2.5 What "completed" means: the acknowledgement boundary

A segment is marked completed in memory only after every produce request for its records returned `Ok` (`C23/restore/engine.rs:1846-1915`, `:1949-1950`). A request returns `Ok` only after the broker's response was parsed without error (`C23/kafka/produce.rs:181-196`), with `acks` from `produce_acks`, default −1 (`C23/config.rs:928`, `:1158-1160`). A segment none of whose records fall in the window is marked completed without producing (`:1767-1771`). So:

- **The checkpoint never lists a segment with an unacknowledged record.** It cannot cause loss by itself, as long as the target it describes is the target the next attempt writes (2.3 says nothing checks that).
- **The target can hold more than the checkpoint says,** in three ways:
  1. records of a segment whose later requests were not yet acknowledged (the partial segment);
  2. acknowledged segments of the topic in progress, held only in memory until the topic ends (2.4);
  3. a request the broker appended whose response the engine never parsed: the replay ambiguity window (§5.5).

  A resume driven by the checkpoint re-produces all three: they are its duplicates.

### 2.6 How shutdown is checked

The CLI forwards SIGTERM and SIGINT into the engine's broadcast channel (`CLI23/commands/restore.rs:43-51`; `CLI23/commands/metrics_runtime.rs:58-68`, `:94-109`). The engine:

- **subscribes late.** It subscribes only after loading the manifest, the header preflight, connecting, creating topics and applying configurations (`C23/restore/engine.rs:897`). The channel's only receiver until then is dropped at construction (`:340`), so a signal before that point is sent to no one and lost: the restore runs to completion. This is from source and was not measured.
- **checks between topics only.** It checks with `try_recv` at the top of each topic (`:905-909`), so a signal during a topic is seen only when that topic is finished, and a signal during the last topic is never seen.
- **reports success.** On a seen signal it `break`s and builds an ordinary report, so `finalize_restore_report` returns `Ok` when no topic failed (`:2145-2156`), and the process exits **0 with the remaining topics not restored** (measured: T1, §4).

Nothing propagates a cancel into the engine from Logweir: `run_engine` waits on `Command::output()` with no timeout and no signal (`L/crates/logweir-engine-oso/src/subprocess.rs:209-236`), and a killed `logweir` leaves the engine running to completion (PROD-01.1 §5.2, five samples, the last on 0.23.3; [`docs/stability.md`](../../stability.md) Later #13).

### 2.7 What skipped segments add

Nothing. A skipped segment `continue`s before the offset-mapping update, the record and byte counters and `segments_processed` (`C23/restore/engine.rs:1735-1739`). The resumed attempt's report, its `records_restored` and its offset report (`offset_report`, written only when the run returns `Ok`, `:426-437`) describe only what that attempt produced (measured: K2's offset report has no entry for the skipped topic, §4). A mapping that must cover every restored record cannot be the engine's report after a resume. It has to be rebuilt from the target's lineage headers, as PROD-08.1 already does (its §2, "Consumer positions").

### 2.8 The producer acknowledgement boundary

- **No idempotence.** Restore produces with no producer id, epoch or real sequence (`C23/kafka/produce.rs:79-110`: `NO_PRODUCER_ID`, `NO_PRODUCER_EPOCH`, a batch-local `sequence`) and no `InitProducerId` anywhere (C5, unchanged since 0.21.0).
- **One request in flight per partition.** Within a partition the requests are sequential: the loop awaits each `produce` before the next (`C23/restore/engine.rs:1846-1915`). Partitions of one topic run concurrently, up to `max_concurrent_partitions` (default 4, `C23/config.rs:1150-1152`); Logweir renders neither key.
- **Re-sends.** A request whose response is lost is re-sent:
  - the client reconnects and re-sends once on a connection error, including a response timeout (60 s, `C23/kafka/client.rs:28`, `:505-529`);
  - the router retries a connection error up to 5 more times and NOT_LEADER up to 20 times (`C23/kafka/partition_router.rs:566-627`).

  One batch can therefore be appended more than once inside one attempt: PROD-01.1 §5.1 measured 3,000, 2,000 and 1,000 duplicates (three, two and one re-sent requests).
- **The window.** At any instant, the records the broker may hold that the engine has not seen acknowledged are at most one request per partition being restored: `min(partitions, max_concurrent_partitions) × produce_batch_size` records per topic (4 × 1,000 at the defaults). Measured: K3, §4.

### 2.9 Two further facts, from source only

- **A failed partition does not stop its siblings.** `restore_topic` awaits the partition tasks in order with `??` (`C23/restore/engine.rs:1338-1341`). The first error returns from the function and drops the remaining `JoinHandle`s, which detaches those tasks: they keep producing, and marking segments completed in the shared checkpoint, while `run_internal` records the error, saves the checkpoint and moves to the next topic (`:917-937`). The entries they add are still fully acknowledged segments (2.5), so this is not a false claim. It does mean a topic is "done" before its partitions are. Not measured.
- **Repartitioning** shares the checkpoint (`C23/restore/repartition.rs:307`). Logweir renders no repartitioning; out of scope.

## 3. What Logweir does today

- **Every attempt is a new run.** It renders a new per-run checkpoint path and a new per-run offset-report path (2.2), so the engine never resumes. Measured: H1 shows a carried checkpoint discarded on the hash.
- **The checkpoint is lost with the pod:** `emptyDir`, never uploaded, one pod per Job (2.2).
- **A retry of the same spec is refused before anything runs.** Phase 0 refuses any mapped target topic that already exists, in both modes, exit 3 (`L/crates/logweir/src/drill/phase0_admit.rs:579-600`). A retry therefore needs fresh target names, which is PLAT-12.2's "fresh-target retry".
- **Logweir's verdict never reads the engine's exit code as completeness.** Phase 7 compares the target with the archive, sampled or complete (PROD-08.1). That matters, because a SIGTERM-stopped engine exits 0 over a partial restore (T1).
- **A cancel does not reach the engine** (2.6, Later #13).

So a crashed restore is not resumable today, by three independent mechanisms (per-run paths, a pod-local file, the existing-target refusal). Two of them are also what keep it safe: §4 shows what the engine's checkpoint does when it IS carried with a stable path.

TODO-MEASURED

TODO-CONTRACT

TODO-TABLE

TODO-ROWS

TODO-LIMITS

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
