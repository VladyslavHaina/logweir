# PROD-07.1 — Restore checkpoint and delivery semantics: the resume contract

- Row: PROD-07.1 (research, Tier B), [product-expansion tracker](../product-expansion.md#prod-071--resolve-checkpoint-and-delivery-semantics). Ledger row: `| 1 | PROD-07.1 | Resolve checkpoint and delivery semantics | P2 | M3 | research | 01.1 | — | none | B | Proposed |`.
- Date: 2026-10-08. Branch `claude/prod-07-1`, from main `740a4875`.
- Kind: research. Source first, then measured on compose slot 1. It ships no product code: the one file it adds outside this record is the `#[ignore]`d harness `e2e/tests/resume_semantics.rs`.
- Status: **proposed**. The default contract (§5.2) and the recommended resume route (§5.3) are recommendations for the orchestrator's review; the ledger changes in §8 are the orchestrator's to record.
- Inputs: [PROD-00 engine route](PROD-00-engine-route.md) §3.4 (C4), §3.5 (C5), §9 (00.3g, 00.3i), §12 (0.23.3); [PROD-01.1](PROD-01.1-record-semantics.md) §5 and §7 (rows 07-1 to 07-4); [PROD-01.4](PROD-01.4-topic-identity.md) §7 and §8 (rows TI-07.1-1 to -3); [PROD-08.1](PROD-08.1-integrity-contract.md) §2 (the oracle). OD-3 as decided on 2026-10-07: Logweir builds the engine with its own patch folder, patch first.

## 0. Decision summary

1. **The engine's restore checkpoint (0.23.3) is not a durable record, and it does not describe the target.** From source (§2):
   - it is a pod-local JSON list of segment keys whose every produce request was acknowledged, plus one hash;
   - it is saved once per topic, and `checkpoint_interval_secs` is read by nothing;
   - the save truncates and then writes, so it is not atomic;
   - the hash covers every `restore:` key, Logweir's per-run paths included, and nothing about the target cluster, the target topics or the archive. For two or more topics it is not even deterministic across processes;
   - shutdown is seen only between topics, and a stopped restore exits 0;
   - skipped segments add nothing to the offset report;
   - restore produces without idempotence and re-sends a request whose response was lost.
2. **Measured on slot 1** (§4; 10 rows, 52 predictions held), running the engine's own next attempt the way a naive resume would:
   - a kill before the first commit duplicates every landed record (1,500), and after a commit the landed part of the next topic (1,200);
   - SIGTERM exits 0 with a whole topic unrestored. The complete lane fails that restore; the default sampled lane can sign `pass` over it (T2; review H1, filed as FX-23);
   - a stale file over a recreated target exits 0 having restored nothing;
   - a truncated file stops the engine (exit 1);
   - two writers duplicate the whole archive;
   - a kill with requests in flight leaves one unacknowledged request per partition in the target (100 records each).

   **New:** the hash is not deterministic for a document of two or more topics, because `topic_mapping` is a `HashMap`. One two-topic document gave 2 distinct hashes over 8 processes (D1), so the same document's checkpoint is accepted or discarded at random (K2 against T1). A tail-resume prototype, run after the in-flight kill, gave an exact target: 0 duplicates, 0 missing.
3. **Logweir today never resumes, and that is what keeps it safe** (§3). Per-run paths, a pod-local file and phase 0's existing-target refusal each prevent a resume. The per-run paths also make a stale file unreachable (F13 of §6), and the refusal stops a second execution from writing into the first one's target.
4. **Default contract, now (PROD-07.2): resume means reconcile, or a fresh target** (§5.2).
   - An interrupted restore is a state, never a generic failure.
   - Its partial targets are listed with PROD-08.1's complete-verification counts.
   - A retry is a new execution into fresh targets.
   - Logweir stops rendering `checkpoint_state` and `checkpoint_interval_secs`, so the engine keeps no file to be stale, corrupt or carried.
   - An engine exit 0 after a cancel is never completion. For the sampled lane this depends on FX-23.
   - A cancel is SIGTERM, then SIGKILL, and the attempt ends only when the writer is proved gone.
5. **Resume, for PROD-07.3: the target is the checkpoint** (§5.3).
   - Under six preconditions (same execution; the old writer gone and the target quiet; target identity unchanged per PROD-01.4; a clean prefix under PROD-08.1's model; one writer; selection by offset), a resume continues each partition after its last `x-original-offset`, through PROD-00.3i's per-partition offset floor (C11).
   - The interruption adds no duplicate, because the target shows every appended batch, acknowledged or not. The prototype measured this after a kill with requests in flight.
   - Complete verification after the resume decides the verdict, and the mapping is rebuilt from the target's lineage.
6. **Not recommended: the engine's checkpoint as the resume mechanism** (§5.6). Per-segment saves, path-free hashing, an atomic save, identity in the hash and a persisted file would cost about 6 days of engine and runner work. They would still leave up to a segment of duplicates per partition, and they still need the target scan to be safe. **PROD-00.3g leaves PROD-07.3's path**; PROD-07.3 depends on 00.3i and PROD-01.4's target identity instead.
7. **Rows** (§7): 07.1-I1 to I7 for PROD-07.2, 07.1-R1 to R10 for PROD-07.3, 07.1-G1 to G3 if 00.3g is kept, and 07.1-F1 for 00.3i. **Ledger proposals** (§8): PROD-07.3's dependencies, PROD-00.3g's priority, A-C4-1 to A-C4-3 re-homed, and a child row for the produce-response fault proxy (PROD-01.5d).

## 1. Evidence base

| Item | Identity |
|---|---|
| Engine source | `third_party/kafka-backup-v0.23.3.tar.gz`, sha256 `bf5544bd521f0a0f343c402bbbde5d6dc0d9b45d70eb1a1efb447ca4f2fda0bd`, tag `v0.23.3` = commit `afb160e7f2c69b7c3c28e1b868dd952835a5b0af` ([PROD-00 §12.1](PROD-00-engine-route.md#121-the-target)) |
| Engine image (runs) | `osodevops/kafka-backup@sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db`, linux/amd64; `bash scripts/extract-engine.sh` on this branch verified the revision label and printed `kafka-backup 0.23.3` |
| Lab | Compose slot 1 (`eval "$(e2e/compose/stack-env.sh --slot 1)"`, project `logweir-e2e-s1`), broker `apache/kafka:3.7.1` read back from the container, slot MinIO; `just e2e-up`, then `just e2e-down` (`down -v`) after each run, which left 0 containers, volumes and networks |
| Harness | `e2e/tests/resume_semantics.rs`, `the_pinned_engines_restore_checkpoint_under_interruption` |
| Oracle | PROD-08.1's complete verification: `logweir::drill::phase7_verify::run_with_coverage(…, Coverage::Complete, …)` over the slot's broker and MinIO |
| Artifacts | `/tmp/logweir-roadmap-run/claude/artifacts/prod-07-1/{r1,r2}/` on the worker host: `session.log`, `test.log`, `outcomes/<row>.json` and `outcomes/engine-logs/<container>.log` (every engine's full log) |

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
- **Not a function of the document.** `topic_mapping` is a `std::collections::HashMap<String, String>` (`C23/config.rs:869`; so are `partition_mapping` at `:865`, `repartitioning` at `:975` and `schema_id_mapping` at `:1027`). `serde_json` writes a `HashMap` in iteration order, and std seeds that order randomly per process. Logweir renders one mapping entry per restored topic, so for any restore of two or more topics two processes running the SAME document can compute different hashes. The second then discards a checkpoint that was written for exactly its document. Found by run r1 (T1's second attempt) and measured by D1 (§4).
- **On a mismatch** the engine logs `Restore configuration changed since the checkpoint was written (config hash mismatch); restarting from the beginning` at WARN and starts over (`:750-760`). It does not refuse.
- **A file that does not parse** fails the run before anything is produced: `load_checkpoint` reads and deserialises with `?` (`:1361-1373`), and that error ends `run_internal` (`:747`). Measured: C1, exit 1.

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
- **absorbs later signals.** After the first signal the forwarder task has ended (`CLI23/commands/metrics_runtime.rs:62-67`), and tokio never unregisters a signal handler it installed. So a second SIGTERM is absorbed too, and only SIGKILL stops a running restore. This is from source and tokio's documented behaviour, not measured.
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
- **Logweir never reads an engine exit 0 as completeness: phase 7 decides.** That matters, because a SIGTERM-stopped engine exits 0 over a partial restore (T1). **But only the complete lane (PROD-08.1) is certain to see a topic the engine never started.** The default sampled lane can sign `pass` over such a restore (review H1, filed as **FX-23**; measured live by T2, §4):
  - phase 6 accepts the target once any one partition is above 0 (`L/crates/logweir/src/drill/phase6_restore.rs:82-92`);
  - phase 4 keeps the first `max_partitions` candidates in manifest order, which is the engine's restore order, so a truncated sample can cover only the topics that finished (`L/crates/logweir/src/drill/phase4_sample.rs:180-186`);
  - phase 7's one aggregate count bound has slack whenever the missing topic's in-window segments straddle the point (`L/crates/logweir/src/drill/phase7_verify.rs:1638-1652`).

  Until FX-23 lands, "phase 7 decides" holds for the complete lane, and for the sampled lane only when every mapped partition is sampled or the bound has no slack. Logweir runs the engine with `RUST_LOG=warn`, so the INFO line "Shutdown signal received" never reaches it (`L/crates/logweir-engine-oso/src/subprocess.rs:220`). Today the signal can reach the engine alone only on a CLI host or in docker-engine mode; in-cluster, `logweir` is PID 1 with no handler. PROD-07.2's SIGTERM-first cancel would make the case routine, which is why its row 07.1-I3 depends on FX-23.
- **A cancel does not reach the engine** (2.6, Later #13).

So a crashed restore is not resumable today, by three independent mechanisms (per-run paths, a pod-local file, the existing-target refusal). Two of them are also what keep it safe: §4 shows what the engine's checkpoint does when it IS carried with a stable path.

## 4. Measured on compose slot 1

**How.** `e2e/tests/resume_semantics.rs`, run r2 at `6babf9f1`, 2026-10-08 12:58–13:10Z:

- the stack: project `logweir-e2e-s1`, broker `apache/kafka:3.7.1` (read back from the container: `kafka_2.13-3.7.1.jar`), engine image `sha256:cc7d5a8a…32db` with revision `afb160e7`;
- the fixture: two topics of three partitions with 1,200 records each (about 105-byte values), backed up by `logweir backup run` in 300-record segments, so each topic has 12 segments;
- the attempts: the engine runs Logweir's rendered document directly, with the two pacing keys of §9 (100-record batches, 300 records/s per partition);
- the oracle: every outcome below is PROD-08.1's complete verification of the targets against the archive, never the engine's exit code.

**Result:** one test, 10 rows, 52 predictions, all held (`test result: ok`, 664 s). `just e2e-down` then left 0 containers, volumes and networks. The outcome files and every engine's log are in the run artifacts (`r2/outcomes/`).

**Run r1** (at `676d990d`) gave the same outcomes for K1, K2, H1, K3, W1 and M1.

- **Its T1 prediction failed.** The prediction "after attempt 2 both topics are exact" was false, because attempt 2 discarded its own document's checkpoint on the hash. That is how §2.3's nondeterministic hash was found; D1 now measures it.
- **Its S1 row panicked.** It met phase 7's refusal of an empty target, which r2 records as an outcome.

In the table, "first" and "second" are the topics in the engine's restore order (manifest order).

| Row | What was done | Engine | Complete verification after the last attempt |
|---|---|---|---|
| **K1** kill before a checkpoint commit | SIGKILL with 1,500 of the first topic's 3,600 records landed (500 per partition). Every landed record was acknowledged (the acknowledged end equals the end offset on all three partitions), and no checkpoint file existed. Attempt 2: the same document and path | Attempt 1 exit 137; attempt 2 loads nothing, exit 0 | first: **1,500 duplicates**, 0 missing; second: exact |
| **K2** kill after a checkpoint commit | SIGKILL with the first topic done and its checkpoint saved (12 keys, all of the first topic), 1,200 of the second topic landed. Attempt 2: the same document and path | Attempt 2: "Loaded checkpoint: 12 segments completed", accepted, exit 0. Its offset report has entries for the second topic's three partitions only | first: exact (skipped); second: **1,200 duplicates** |
| **H1** the same, rendered as Logweir renders | As K2 (1,000 of the second topic landed), then attempt 2 with a new per-attempt path and attempt 1's file copied to it | "Loaded checkpoint: 12 …", then "config hash mismatch; restarting from the beginning", exit 0 | first: **3,600 duplicates**; second: 1,000 duplicates |
| **T1** SIGTERM | SIGTERM with 900 of the first topic landed | Finished the first topic, "Shutdown signal received, stopping restore", **exit 0** with the second topic never started; checkpoint of 12 | after attempt 1: first exact, second **3,600 missing**, verdict `fail`. Attempt 2 (same document, same path): loaded, then **"config hash mismatch"** and a restart: first 3,600 duplicates, second exact |
| **K3** kill with requests in flight | With 1,500 landed: froze the broker, slept 1.5 s, SIGKILL, thawed after 0.5 s | Acknowledged end 600 per partition; end offsets 700 per partition after the thaw: **100 records per partition appended unacknowledged** (one request each, 300 in all). r1: 800 acknowledged, 900 landed | after the tail-resume prototype (tail 699, then the remaining 500 per partition, and all of the second topic): **`pass`, 0 duplicates, 0 missing** |
| **W1** two writers | Two attempts at once, the same document, each with its own per-attempt path | Both exit 0 | **3,600 + 3,600 duplicates** (the whole archive twice), 0 missing, 0 out of order |
| **M1** foreign write | SIGKILL with 1,800 landed, then 5 records without lineage produced into the first target's partition 0 | The prototype **refused**: "…/0@600 carries no x-original-offset". The engine's re-run of the same document: exit 0 | after the re-run: first **5 unexpected**, 1,800 duplicates |
| **D1** hash determinism | One two-topic document run by 8 processes, one one-topic document by 4, with a window selecting nothing (nothing produced; every attempt exits 0 and saves its own hash) | Two topics: **2 distinct hashes** (`30c5de72…` ×5, `a347f134…` ×3). One topic: 1 hash (`0db8533f…` ×4) | n/a |
| **S1** stale checkpoint, new target | One topic restored completely (`pass`, 3,600), checkpoint of 12; the target topic deleted and recreated empty; attempt 2: the same document and path | "Loaded checkpoint: 12 segments completed", no mismatch, **exit 0, 0 records produced** | the target stays empty. Phase 7 refuses it: `Operational("no restored records found on any mapped target topic …")`, which is exit 1 with nothing signed |
| **C1** corrupt checkpoint | S1's file truncated to 839 bytes | **exit 1**, "Error: Serialization error: EOF while parsing a string at line 12 column 12"; 0 records produced | n/a |

**What the runs establish:**

- **The kill boundary is exactly where the source puts it.** With no checkpoint commit, a re-run duplicates every landed record (K1). After a commit it duplicates the landed part of the topic in progress (K2). The acknowledged prefix equals the target when the kill lands between requests (K1). The target exceeds it by exactly one request per partition when the kill lands with requests in flight (K3).
- **A checkpoint carried with a stable path works only by chance.** For a single topic the hash is deterministic, and the stale file is trusted against a recreated target: exit 0, nothing restored (S1). For two topics, the same document is accepted (K2) or discarded (T1) depending on the process (D1). Logweir's per-attempt paths discard it always (H1).
- **The engine's exit code says nothing about completeness.** Exit 0 came with a whole topic missing (T1), a target left empty (S1), a target duplicated twice over (W1) and 5 foreign records (M1).
- **The target is a better checkpoint than the checkpoint.** A tail scan after a kill with requests in flight saw the unacknowledged batch and resumed with 0 duplicates and 0 missing (K3). The same scan refused a target with a foreign record (M1).

## 5. The contract

### 5.1 Terms

- **Execution.** One approved restore of one plan, from one archive generation, into one set of targets.
  - Its id is minted at the first attempt: for a `Restore`, its UID and execution number (PLAT-12.2's retry identity); for the CLI, the first attempt's run id.
  - A changed plan, a changed archive generation or a fresh target makes a NEW execution (the tracker's PROD-07 boundary; PLAT-12.2).
- **Attempt.** One runner pod, one `logweir restore run` process and one engine process, identified by its run id.
- **Archive generation.** The catalog point id, `backup_id`, manifest sha256 and, where the store versions objects, `manifest_version_id`: the binding PROD-01.4 §7 gives PROD-07.1.
- **Target identity.** For each target topic:
  - the cluster id, the name and `topic_id` when available (PROD-01.4 §2; real ids need OD-6's FFI crate);
  - else its creation marks (partition count, log start 0);
  - and, at the end of every attempt, per partition, the end offset observed and the fingerprint of the record just below it (PROD-01.4 §7, "PROD-07.1").
- **Durable progress.** Per target partition, the longest prefix of the target that PROD-08.1's model proves to be an exact, in-order copy of the archive. It is computed over the target's records up to its last `x-original-offset` (the **tail**). It lives in the target topic itself; the attempt record (5.4) only binds it. Nothing else is durable progress: not the engine's checkpoint, the engine's offset report, the engine's exit code, or a count of acknowledged requests.

### 5.2 The default contract, now (PROD-07.2): an interrupted restore is reconciled, never resumed

1. **Interrupted is a state.** An attempt that ends without a signed verdict ends its execution as **Interrupted** (PROD-07.2's state), never as a generic failure, and is never retried into the same target automatically. That covers a killed runner, an engine exit other than 0, a lost pod or node, and a Logweir-side failure after the engine finished (PROD-01.1's 07-1b and 07-1c).
2. **Reconcile** means running Logweir's complete verification (PROD-08.1) over each partial target. It gives, per partition, the tail and the missing, duplicate, out-of-order and unexpected counts at or below it. These go into the signed list of partial targets that PROD-07.2 publishes, so the operator sees the last durable progress.
3. **Retry** is a new execution (PLAT-12.2) into fresh target names, behind the policy's approval. Phase 0's existing-target refusal (`L/crates/logweir/src/drill/phase0_admit.rs:579-600`) stays exactly as it is, so no execution can append into another's partial target. The partial targets stay until the operator resolves the interruption (PROD-07.2's hold and cleanup rules).
4. **The engine's checkpoint is never durable, never carried and never read.**
   - Logweir stops rendering `checkpoint_state` and `checkpoint_interval_secs`. With no `checkpoint_state` the engine keeps no checkpoint at all (2.2), so no file can be stale (S1), corrupt (C1) or carried (H1).
   - Within one attempt the file does nothing: the engine reads it only at start (2.1).
   - This supersedes PROD-00's A-C4-1 (stable per-execution paths) and absorbs A-C4-3 (§8).
   - Until it lands, per-attempt paths keep the same property (2.2, H1).
5. **An engine exit 0 is not completion.** A SIGTERM-stopped engine exits 0 with the remaining topics unrestored (T1). Once PROD-07.2 sends cancels, an attempt that was cancelled is Interrupted whatever the engine's exit, unless the complete verification passes. Phase 7 decides the verdict from the target, but only the complete lane is certain to see a never-started topic. The default sampled lane can sign `pass` over one (T2) until **FX-23** lands (§3), so this item and row 07.1-I3 depend on FX-23.
6. **A cancel stops the writer, then the attempt ends.**
   - SIGTERM reaches the engine only between topics (2.6). So PROD-07.2's cancel is SIGTERM, then SIGKILL after a bounded grace.
   - The attempt is not Interrupted, and its point not released for a retry, until the engine process or container is proved gone and the targets have stopped growing (PROD-01.1's 07-4).

### 5.3 Resume, when PROD-07.3 delivers it: continue each partition from its verified tail

**Recommended route: the target is the checkpoint.** A resume attempt reconciles the partial target, then restores, per partition, only the archived records after the tail. It selects them by source offset through PROD-00.3i's per-partition offset floor (C11). It does not use the engine's checkpoint.

Preconditions. All six must hold; any one that fails gives "resume blocked: <reason>", and the execution falls back to 5.2:

- **R1 Same execution.** The same plan hash and the same archive generation as the execution's earlier attempts, read from their attempt records (5.4), which must verify.
- **R2 The earlier writer is gone and the target is quiet.**
  - Every earlier attempt's engine is proved stopped: the process or container has exited, or the pod has terminated with its container's terminated state recorded.
  - A pod on an unreachable node (phase `Unknown`) is NOT proved stopped: such a pod can keep writing. The resume waits for the node's removal, or the execution falls back to a fresh target.
  - Then every target partition's end offset is unchanged over a quiet period at least the engine's `produce_timeout_ms` (30 s by default, `C23/config.rs:1162-1164`). That bounds a request a leader appended but had not yet committed when its writer died.
  - Without producer fencing (no `InitProducerId`, 2.8), this wait is the only bound. PROD-00.3d with a transactional id per execution would make it exact.
- **R3 Target identity unchanged since the last attempt record.** Equal `topic_id` when both are known. Otherwise:
  - equal creation marks;
  - no partition's end below its recorded end;
  - and the record just below each recorded end has the recorded fingerprint.

  Any difference refuses with `TargetGenerationChanged`, and the retry uses a fresh target (PROD-01.4 §7; rows TI-07.1-1 to -3). S1 shows what trusting a name alone does.
- **R4 A clean prefix.** Below the tail, the reconciliation finds no `unexpected` record, no `out_of_order`, no `mismatched` and no `missing`.
  - An unexpected record is one without lineage, as from a foreign writer (M1), or naming an offset the archive does not hold.
  - Duplicates below the tail come only from re-sent requests (C5, 2.8). They do not block. They are counted, disclosed as bounded duplicates in the attempt record, and the final verdict still fails on them, as PROD-08.1 fails any target that holds one.
- **R5 One writer.** One attempt at a time per execution, enforced by the controller (one Job per attempt, created only when R2 holds). W1 shows two writers duplicating everything. The CLI takes no lease, so the CLI offers no resume.
- **R6 Selection by offset, never by time.**
  - Per partition, the resume offset is the tail + 1, or the first archived offset in the window when the partition is empty (PROD-01.1's 07-2).
  - The tail may be a transaction marker's lineage: markers are archived and restored as records (PROD-01.1's 07-3).
  - The window, the mapping and every other key of the approved document are unchanged, except two things. The offset-floor rule block is added; it is derived mechanically from the reconciliation, and its digest goes into the attempt record. And `offset_report` names this attempt's own report path: each attempt writes its own report, which is never the mapping of record (2.7), and the attempt record holds its path and digest.

**After the resume:**

- complete verification over the whole target, against the PLAN's expected set (PROD-08.1 §2), is the execution's verdict;
- the offset floor is an execution detail, never a record filter in PROD-08.1's sense, so it never enters `complete.filter` or `excluded`;
- the offset mapping is rebuilt from the target's lineage headers (2.7; PROD-08.1 §2.1), never taken from the engine's report;
- the evidence records the attempts, each partition's resume offset and the prefix duplicates.

**Bounds:**

- **Duplicates a resume adds at the interruption:** 0, because the tail scan sees an appended batch whether or not it was acknowledged (measured with the prototype: K3, §4).
- **Duplicates inside any attempt:** C5's re-sends, at most the re-sent requests × `produce_batch_size` per partition (PROD-01.1's 07-1), until PROD-00.3d.
- **Loss:** none is assumed. The final complete verification proves the target or the verdict fails.

**Approval and the existing-target refusal.** A resume is the same execution:

- it writes only records the approved plan selects, into targets that execution created and that R3 re-identified;
- phase 0's refusal gets exactly one exception, for those targets. This is not a restore into a live topic (`docs/stability.md` Never #1), because nothing but this execution has written them, and R4 proves it;
- whether a resume needs a fresh approval is the policy's question under PLAT-12.2's retry identity, not this record's.

### 5.4 The attempt record: what binds a resume

The tracker's "checkpoints bind plan, execution, archive generation and target identity" becomes one create-only object per attempt in the evidence store (`logweir/restores/<execution>/attempt-<n>.json`), written by the runner at the end of every attempt that reached the engine, and when it can, on a cancel. It holds:

- the plan hash, the execution id, the attempt's run id and the attempt number;
- the archive generation (5.1);
- per target topic: cluster id, name, `topic_id` or null, partition count and creation marks;
- per partition at attempt end: end offset, tail, the fingerprint of the record below the end, and the reconciliation counts;
- whether a cancel was sent, the engine's exit, and the resume block's digest when the attempt was a resume.

The rules:

- **Absent means unknown, and unknown never resumes** (TI-07.1-2).
- **Records written before this contract are never read as resumable.** Neither are the engine's pod-local checkpoint files: none carries an identity block.
- **It is versioned and verified like every other evidence document** (inherited rule 3): both readers and the parity script, absent-field behaviour stated.
- **It lives in the evidence bucket,** so it survives the pod, the node and the controller. The controller may mirror the latest attempt in status for PLAT-14's progress.

### 5.5 The replay ambiguity window

**Definition.** The records the broker appended for an attempt that the attempt never saw acknowledged.

**Bound.** At the instant an attempt ends, at most one in-flight produce request per partition being restored: `min(partitions, max_concurrent_partitions) × produce_batch_size` records per topic, 4 × 1,000 at the defaults (2.8). Within the attempt, add the re-sent copies of any request whose response was lost (C5).

**Who can see it.**

- The engine's checkpoint cannot: it lists only fully acknowledged segments (2.5), so a checkpoint-driven resume re-produces the window.
- The target can, once R2's quiet period has passed: a tail scan reads every appended record, acknowledged or not.
- Measured (K3): the engine had seen 600 records per partition acknowledged (r1: 800). After the thaw the target held 700 per partition (r1: 900): exactly one 100-record request per partition, appended with no acknowledgement, 300 records in all. The tail scan saw them, and the prototype resumed after them with 0 duplicates.

### 5.6 What per-segment checkpoints, path-free hashing and a persisted mapping would buy (option B), and why it is not recommended

OD-3 now funds engine patches. So the alternative is real: make the engine's own checkpoint the resume mechanism.

**It would need five patches:**

- PROD-00.3g: save per segment or per `checkpoint_interval_secs`, and hash without the file paths and over a canonical (sorted) encoding, because D1 shows today's hash differs between processes for one document (~2 days, PROD-00 §9);
- an atomic save, write then rename (C1 shows the hazard; ~0.5 day);
- target cluster id, topic ids and the manifest digest in the hash (S1 shows the hazard; ~1 day);
- skipped segments' mappings re-added from the checkpoint (K2; ~0.5 day);
- a durable place for the file. That is a PVC, `docs/stability.md` Later #12, a new lifecycle to own; or a runner that uploads the file at every save and downloads it at start (~2 days, and a new object type anyway).

**It would buy** resume at segment granularity with no PROD-00.3i dependency.

**It would still leave:**

- **Duplicates up to one segment per partition**, plus the ambiguity window (2.5, 5.5). The checkpoint trails the target by construction, and per-topic saves today make it a topic (K1, K2).
- **A file that has to be checked against the target before it is trusted.** S1 shows a checkpoint applied to a recreated target exiting 0 with every record missing. TI-07.1-1 to -3 then require the same identity reads and fingerprints as route C. Once those reads are taken, the target already says where to resume.
- **Fencing and quiescence.** Two writers duplicate everything whatever the checkpoint says (W1).

So option B costs about 6 days of engine and runner work. It needs the target scan anyway, and it ends with a weaker bound (a segment per partition, against 0). Route C costs:

- PROD-00.3i's offset-range rule (+~1 day on C10's ~3 days, already on the ledger for PROD-09.3 and PROD-11.1);
- the reconciliation, which is PROD-08.1's complete lane, already shipped;
- the attempt record and the identity reads (PROD-01.4's, ~2 days);
- the controller's single-flight and quiet-period gate (~1–2 days).

**PROD-00.3g is therefore not on PROD-07.3's path.** Its fixes are bug-class (a documented key that does nothing, a non-atomic save, a hash that covers paths) and stay upstream-worthy. §7.3 gives its rows if the orchestrator keeps it; §8 proposes to keep it at lower priority.

### 5.7 Process kill, lost node or storage, and competing workers

Every event is a row of §6. In short:

- **Process kill** before or after acknowledgements, and before or after checkpoint commits: today it is Interrupted (5.2); under 5.3 it resumes exactly once from the tail, whatever the engine's checkpoint held.
- **Lost node:** a resume waits until the old pod is proved terminated (R2). A partitioned node's pod can still write.
- **Lost checkpoint storage** (the `emptyDir`): no effect, because nothing reads the engine's file.
- **Lost target storage** (topic deleted, recreated or truncated): R3 refuses, and the retry is a fresh target.
- **Lost archive** (the manifest or a segment unreadable or changed): R1 fails, or the reconciliation cannot compute the expected set (PROD-08.1 §2: "not compared"), and the resume is blocked.
- **Competing workers** (a second attempt, or the orphaned engine of a killed `logweir`): R2 and R5 refuse until the writer is gone; W1 measures what happens without them.

## 6. Failure-state table

"Engine next attempt" is what the pinned engine does when the next attempt runs the same document with the same checkpoint path. Logweir never does this today; it is the measured hazard. "Today" is Logweir as shipped; "5.2" and "5.3" are this contract.

| # | Event | Engine next attempt (0.23.3) | Today | 5.2 (PROD-07.2) | 5.3 (PROD-07.3) |
|---|---|---|---|---|---|
| F1 | Runner or engine killed (SIGKILL, OOM, pod deleted) inside the first topic, before any checkpoint commit | No file was saved, so it starts over. Duplicates = every record already landed (K1: 1,500) | Killed `logweir`: the engine finishes unrecorded (PROD-01.1 §5.2). Killed pod: no scorecard; a retry of the same spec is refused at phase 0 | Interrupted; reconcile; fresh-target retry | Resume after R2: 0 added duplicates, nothing missing |
| F2 | Killed after a topic's checkpoint commit, inside a later topic | If the hash matches, it skips the committed topic's segments: duplicates = the later topic's landed records (K2: 1,200), and its offset report omits the skipped topic. For two or more topics the hash matches only by chance (D1); otherwise as F3 (T1) | As F1 | As F1 | As F1 |
| F3 | As F2, with Logweir's per-attempt path and the file carried | Discards it on the hash ("config hash mismatch") and starts over; duplicates = everything landed (H1: 3,600 + 1,000) | Not carried: as F1 | As F1 | As F1 (the engine file is irrelevant) |
| F4 | Graceful stop (SIGTERM) mid-topic | Finishes the topic, saves, stops before the next and **exits 0** with later topics absent (T1) | No cancel reaches the engine (Later #13). A signal aimed at the engine alone (a CLI host, docker-engine mode) gives exit 0; the complete lane fails it, but the default sampled lane can sign `pass` (T2; **FX-23**) | SIGTERM then SIGKILL; Interrupted whatever the exit (5.2 items 5–6); depends on FX-23 for the sampled lane | As F1 |
| F5 | Killed with produce requests in flight (the ambiguity window) | Re-produces the in-flight batches with everything else (no file) | As F1 | As F1; the reconciliation counts what landed, acknowledged or not | Tail scan after the quiet period: 0 duplicates (K3: `pass`, 0 duplicates, 0 missing) |
| F6 | A response lost inside an attempt (C5) | Re-sends: one batch appended twice; can exit 0 (PROD-01.1 §5.1 samples 3, 5, 6) | Count bound or complete verification fails the run (PROD-01.1 §5.1 sample 6; PROD-08.1) | Same; the reconciliation names the duplicates | R4 lets them through, disclosed; the final verdict fails on them; 00.3d removes them |
| F7 | Broker outage past the retry budget: the engine exits 1 with a partial target | Errored topics are still checkpointed (2.4); the next attempt skips their completed segments | Exit 1, no scorecard; the partial target stays (PROD-01.1 §5.1 sample 3) | Interrupted, partial targets listed (07-1b) | Resume after R2–R4 |
| F8 | Engine exits 0, then Logweir's own read fails (07-1c) | n/a | Exit 1, no scorecard over a complete target (PROD-01.1 §5.1 samples 4, 5) | "Restore finished, verification not completed"; re-verify without re-restoring | Re-verify; nothing to resume |
| F9 | Controller restarted | n/a: the Job runs on | The Job is unaffected | Unaffected: state is in the attempt record and the Job | Unaffected |
| F10 | Node lost (runner pod `Unknown`) | n/a | The checkpoint is gone with the `emptyDir`; nothing to resume | Interrupted once the pod is proved terminated | Blocked until the pod is proved terminated (R2); then as F1 |
| F11 | Checkpoint storage lost | Starts over: the same as F1 | No effect (never read) | No effect: no file is rendered | No effect |
| F12 | Checkpoint file truncated (killed during the non-atomic save) | **Refuses to start**: exit 1, nothing produced (C1) | Unreachable: per-attempt paths | Unreachable: no file | Unreachable |
| F13 | A stale checkpoint and a target deleted and recreated under the same names | **Trusts the file whenever the hash matches (always for one topic): skips every segment, exits 0, and the target stays empty** (S1: 0 records; phase 7 refuses it, exit 1) | Unreachable: per-attempt paths and phase 0 | Unreachable: no file | R3 refuses: `TargetGenerationChanged`, fresh target (TI-07.1-1) |
| F14 | Target truncated (delete-records, a retention change) | Skips segments it already wrote; the truncated records stay missing | n/a: the retry is a fresh target | Reconciliation reports them missing | R3 (end below the recorded end) or R4 (missing below the tail) refuses (TI-07.1-3) |
| F15 | A foreign writer appends to the target | Re-produces around it; the foreign records stay (M1: 5 unexpected, 1,800 duplicates after the re-run) | n/a | Reconciliation reports them unexpected | R4 refuses (M1: the prototype refused) |
| F16 | Two writers for one execution: a second attempt, or the orphaned engine of a killed `logweir` | Both produce everything: duplicates = the whole archive (W1: 7,200) | Possible today only through the orphan (PROD-01.1 §5.2) into the same target | The cancel must stop the writer before Interrupted (5.2 item 6) | R2 and R5 refuse until one writer remains |
| F17 | Archive changed under the same backup id, or unreadable | Skips by key, so it trusts a rewritten archive (the hash does not cover the manifest, 2.3) | n/a | Reconciliation cannot compute the expected set: "not compared" (PROD-08.1 §2) | R1 refuses |
| F18 | Plan changed between attempts | The hash differs, so it starts over | n/a | A new execution (PLAT-12.2) | A new execution; never a resume |
| F19 | Cancel or SIGTERM before the engine subscribes to its shutdown channel | Lost: the restore runs to completion (2.6, from source; not measured) | n/a | SIGKILL after the grace (5.2 item 6) | As 5.2 |

## 7. Acceptance rows for the dependent tasks

Each row has a pass predicate, a negative control that must make it fail, and a fixture. "resume_semantics `<row>`" means that row of `e2e/tests/resume_semantics.rs`, whose steps the dependent task re-points at the product path (the runner, `logweir restore run`, or the controller) instead of the bare engine. PROD-01.1's and PROD-01.4's rows that already belong to these tasks are cited, not restated.

### 7.1 PROD-07.2 — Make interruption honest

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 07.1-I1 | A restore whose runner is killed inside the first topic ends Interrupted. The signed list names every partial target, and per partition its tail and its missing/duplicate/unexpected counts, equal to a complete verification of the same target. Extends 07-1b. | A state that reads as a generic failure, or a list without per-partition coverage, fails. So does a list whose counts differ from the complete verification's. | resume_semantics k1 steps through the runner; 07-1b's broker-stop fixture |
| 07.1-I2 | A cancel stops the writer: SIGTERM, then SIGKILL after the grace. After the grace no engine process or container of the attempt remains, the target's end offsets stop moving, and only then is the state Interrupted. | Today's `a_killed_restore_leaves_its_engine_writing` (PROD-01.1 07-4): the engine outlives the cancel and the target reaches the whole archive. That must fail the new "writer stopped" assertion. | the kill row, flipped |
| 07.1-I3 | A restore whose engine stopped inside the first of two topics is never Succeeded or `pass`, under **either** coverage and whatever the engine's exit code. That covers a PROD-07.2 cancel and a signal to the engine alone. **Depends on FX-23.** | T1: the engine exits 0 with the second topic absent. T2: the default sampled lane with `max_partitions` equal to one topic's partitions, over an archive whose in-window segments straddle the point, signs `pass` today (FX-23 open). An implementation that reads exit 0 as "restore finished", or that leaves the sampled lane as it is, fails. | resume_semantics t1 and t2 steps through the product path; FX-23's probe holes A and B (review H1) |
| 07.1-I4 | The rendered restore document carries neither `checkpoint_state` nor `checkpoint_interval_secs`. A test over the render fails if either key comes back (absorbs A-C4-3). | A mutant that renders a stable path. Its consequence is S1 (a stale file trusted: exit 0, every record missing), which the test cites. | render snapshot test; resume_semantics s1 |
| 07.1-I5 | A retry of an Interrupted execution is a new execution into fresh targets. A second execution naming the old partial target is still refused at phase 0. | A retry that reuses the old target name. | PLAT-12.2's retry journey |
| 07.1-I6 | A Logweir-side read failure after the engine exited 0 is "restore finished, verification not completed", and re-verifying needs no re-restore (PROD-01.1 07-1c). | As 07-1c. | 07-1c's pause-at-exit fixture |
| 07.1-I7 | While an execution is Interrupted, the recovery point it restores from is held. Cleanup deletes only targets whose identity matches the execution's attempt record. | A cleanup that deletes a same-name target recreated by someone else, or a retention run that releases the held point. | resume_semantics s1 steps (recreated target) |

### 7.2 PROD-07.3 — Resume within proven semantics

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 07.1-R1 | Killed with produce requests in flight (the broker frozen, the engine killed, the broker thawed), then resumed: complete verification is exact. No batch appended without acknowledgement appears twice. | The engine's own re-run of the same document: duplicates = every record landed (K1, K3's window included). | resume_semantics k3 (the prototype replaced by the product resume) |
| 07.1-R2 | Killed inside topic 1 of 2, and again after topic 1's checkpoint commit: exact after resume, either way. Replaces A-C4-2. | Per-attempt paths with the file carried re-produce topic 1 whole (H1: duplicates = 3,600). | resume_semantics k1, k2, h1 |
| 07.1-R3 | Resume selects by offset (PROD-01.1 07-2) and tolerates a marker at the tail (07-3). | As 07-2 and 07-3. | ts-pit row; TXN row |
| 07.1-R4 | A target deleted and recreated between attempts refuses with `TargetGenerationChanged` before anything is produced; so does c02's refilled recreation (TI-07.1-1). | S1: the engine's checkpoint trusts the recreated target (exit 0, every record missing); an offsets-only identity passes c02. | resume_semantics s1; PROD-01.4 c02 steps on a restore target |
| 07.1-R5 | A truncated target refuses (TI-07.1-3). An attempt record without an identity block is never resumed (TI-07.1-2). | As TI-07.1-2 and -3. | unit rows over recorded identities |
| 07.1-R6 | A target holding a record the execution did not write refuses, naming the partition and offset. | M1: a re-run leaves the 5 foreign records unexpected and exits 0. | resume_semantics m1 |
| 07.1-R7 | A resume is refused while the previous attempt's engine or pod is running or unknown, or while any target end moved within the quiet period. | W1: two writers duplicate the whole archive. The orphaned engine of the kill row keeps writing. | resume_semantics w1; the kill row |
| 07.1-R8 | After a resume, the mapping rebuilt from the target's lineage covers every restored record, equal to the complete verification's restored set. The engine's offset report is not used. | K2: the engine's report after a checkpoint resume has no entry for the skipped topic. | resume_semantics k2 |
| 07.1-R9 | Duplicates inside an attempt stay within the re-sent requests (PROD-01.1 07-1). R4 lets them through disclosed, and the final verdict fails on them. | 07-1's fault proxy. | the fault-proxy profile (§8) |
| 07.1-R10 | The resumed attempt's document differs from the approved one in exactly two places: the added offset-floor block, and `offset_report`, which names the attempt's own report path. The floor block's digest and the report's path and digest are in the attempt record. The final verification uses the plan's expected set, with no `excluded` and no `complete.filter`. | A resume that changes the window, the mapping or any other key, `target:` and `storage:` included, is refused. So is one whose floor block's digest differs from the attempt record's. A verification that treats the floor as a filter passes a target missing its prefix. A diff that also allowed any key besides these two would pass a changed window. | render diff over two attempts with different run ids (the allowed set is `{offset_report, the floor block}`); a complete verification with the prefix deleted |

### 7.3 PROD-00.3g, if the orchestrator keeps it

A-C4-4 stays. The rows below are added. None is a PROD-07.3 dependency.

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 07.1-G1 | A kill during a checkpoint save leaves the previous complete file (write to a temporary file, then rename). | C1: a truncated file stops the next attempt. | resume_semantics c1, with the kill injected during the save |
| 07.1-G2 | The hash is a function of the document: the same two-topic document hashes identically in every process. It excludes `checkpoint_state` and `offset_report`, and includes the target cluster id and the archive's manifest digest. | D1: 2 hashes over 8 processes today. H1: per-attempt paths discard the file. S1: a recreated target on the same cluster is not caught by the hash alone, and needs topic ids or the identity reads. | resume_semantics d1, h1, s1 |
| 07.1-G3 | Skipped segments' mappings are re-added from the checkpoint, so the report covers every restored record. | K2's report without topic a. | resume_semantics k2 |

### 7.4 PROD-00.3i (its offset-range rule, for PROD-07.3)

| # | Pass predicate | Negative control | Fixture |
|---|---|---|---|
| 07.1-F1 | A per-partition floor rule drops every source offset below the floor, produces the rest unchanged, and skips a segment wholly below the floor without producing from it. | A floor applied through the time window (07-2's ts-pit control). | A-C11-1 / A-C11-2 fixtures |

## 8. Ledger changes proposed (the orchestrator's to record)

1. **PROD-07.2** takes the default contract (5.2) and rows 07.1-I1 to I7. Its 5.2 item 5 and row I3 depend on **FX-23** (filed from this record's review), which closes the sampled lane's `pass` over an early-stopped restore. It also takes A-C4-3, as I4, and the `docs/kubernetes.md` `TMPDIR` row, which says the checkpoint lands there and must change with I4.
2. **PROD-07.3's dependency "00.3 per OD-3" becomes "00.3i"** (C11's offset floor, 07.1-F1), plus PROD-01.4's target identity:
   - `topic_id` needs OD-6's FFI crate; the creation-marks and fingerprint fallback is available now;
   - PROD-00.3d is recommended (it removes C5's duplicates inside an attempt), not required;
   - not 00.3g.

   **Proposed approach-text changes for PROD-07.3** (route C contradicts two sentences of today's text):
   - "Keep rendered options byte-identical across attempts" becomes "Keep rendered options identical across attempts except the attempt's own `offset_report` path and the offset-floor block (07.1-R10)".
   - "Label up to one segment of duplicates per partition as bounded" becomes "A resume adds no duplicate at the interruption (07.1-R1). Duplicates inside an attempt (C5's re-sends) are disclosed and fail the verdict until PROD-00.3d (07.1-R9)".
3. **PROD-00.3g** leaves PROD-07.3's path. Keep it as a bug-class patch at lower priority (A-C4-4 and 07.1-G1 to G3), or drop it; the orchestrator decides. PROD-00's A-C4-1 (stable per-execution paths) is superseded by 07.1-I4, and A-C4-2 by 07.1-R2.
4. **Proposed child row PROD-01.5d — a produce-response fault proxy profile** (compose, Tier B). It is a listener whose advertised address is a proxy that forwards produce requests and can drop or hold a response deterministically.
   - Two rows wait on it: PROD-01.1's 07-1 (here 07.1-R9) and PROD-00.3d's oracle A-C5-1. PROD-01.1 §5.1 names it and PROD-01.1 §7 assigns it to PROD-07, but neither task owns a compose profile.
   - K3's freeze-and-kill (§4) is deterministic for "kill with requests in flight", because the kill lands while the broker is frozen. It does not drop a single response, so it does not replace the proxy.

## 9. Limits of this record

- **One broker, no replication.** Slot 1 is one combined KRaft node, so an appended but uncommitted record (R2's quiet period) never occurs here; the bound comes from source and Kafka's acknowledgement semantics, not from a run.
- **The engine runs in a container under `--platform linux/amd64`** on an arm64 host, through a copy of `engine-docker.sh`'s invocation with a container name. The engine binary and image are the pin's.
- **Pacing keys.** Two keys Logweir does not render (`produce_batch_size: 100`, `rate_limit_records_per_sec: 300`) were appended to every document, identically across a scenario's attempts. They change timing and batch size, not the checkpoint or produce semantics. The defaults' window (4 × 1,000) is from source.
- **Few runs, small fixtures** (7,200 records). K1, K2, H1, T1, K3, W1 and M1 ran twice (r1, r2) with the same behaviour; the counts follow where each kill landed. The exception is T1's second attempt, which is D1's randomness. D1, S1 and C1 ran once. They establish the behaviours, not rates.
- **D1's split** (5 and 3 of 8 processes) is a property of std's `HashMap` seeding. How often a document of more than two topics keeps its hash was not measured.
- **The resume is a prototype.** It produces the remaining archived records through librdkafka with idempotence on, not through the engine with an offset floor, which does not exist until PROD-00.3i. It shows what a tail-driven resume yields. It does not test 00.3i.
- **Not run:**
  - a lost node or a Kubernetes runner (F10 is from source and Kubernetes semantics);
  - a signal before the engine subscribes (F19);
  - detached sibling partitions (2.9);
  - a broker outage past the retry budget (F7 cites PROD-01.1);
  - a rewritten archive under the same backup id (F17).
- **Routes and ledger changes are proposals.** OD-3 is decided; which 00.3 rows exist and their priority is the orchestrator's.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
