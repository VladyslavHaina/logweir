# OD-23 core: Schema Registry, four engine rows and an honest interrupted restore

- Rows: **PROD-03.1**, **PROD-03.2** (Schema Registry), **PROD-00.3a**, **00.3d**, **00.3e**, **00.3h** (engine), **PROD-07.2** (interruption), in the [product-expansion tracker](../product-expansion.md). Owner decisions: OD-23 (only what is required, the simplest form that works; OD-2 #2 narrowed), OD-3 (engine patch folder), OD-7 (format steps), OD-15 (built together, tested once), OD-11 (less code).
- Date: 2026-10-16. Base: main `56f4436d`. Branch `claude/od23-core-design`. A design record: it ships no product code.
- Companion record: [PROD-19.1](PROD-19.1-workspaces.md) (one namespace per install, workspaces inside it). Every object named here lives in the release namespace and belongs to one workspace, by that record's rules (§5).
- Citations are `file:line` at the base commit; `E:` is the vendored engine source (`third_party/kafka-backup-v0.23.3.tar.gz`, `crates/kafka-backup-core/src/`).

## 0. Decisions in one page

1. **Schemas are restored with the same IDs, so no record byte is ever rewritten** (§1). A backup whose source connection names a registry snapshots it through the REST API; a restore whose target connection names a registry imports, before any topic is created, exactly the subjects the restored records reference, keeping each schema's ID and version (Confluent's `IMPORT` mode). An ID or subject the target already holds with different content refuses the run before any write. Measured: works on Confluent Schema Registry 8.0.0 and Redpanda v26.2.4; Karapace 6.2.3 and Apicurio 3.0.9 cannot keep IDs and are refused by name.
2. **The registry is part of the Kafka connection**, not a new kind: an optional `KafkaCluster.spec.schemaRegistry` block (URL, username, a key of the connection's own Secret). The source connection's registry is captured; the target connection's registry is written. No new CRD, controller, route family or reference field.
3. **Which schemas a restore needs is read from the records**, not from a naming strategy: the pre-restore archive pass that already reads every selected segment (`crates/logweir/src/drill/archive_scan.rs:1-9`) also collects the Confluent-framed IDs that are in the snapshot. Topic-to-subject association therefore needs no declaration and no strategy.
4. **The four engine rows are four patches and one build bump** (`0.23.3+logweir.5`, §2). None changes the archive format or the evidence format. 00.3a is always on (no setting); 00.3d is always on; 00.3e needs no Logweir change (phase 7 already keys by the last `x-original-offset`, `crates/logweir/src/drill/phase7_verify.rs:159-169`); 00.3h adds one plan key, `restore.rate_limit_bytes_per_sec`, a limit on the whole restore.
5. **Interrupted is a phase, not a document** (§3). A run that ended without a signed scorecard after it created its targets is `Interrupted`. A reconcile Job runs the complete verification over its partial targets and signs an ordinary scorecard (`fail-integrity`, complete coverage, per-partition counts, `target.creation`), so no new document type, verifier arm or format version is needed. `Clean up` reuses `logweir restore cleanup`, which already deletes only what a signed scorecard records while each topic's ID is the recorded one.
6. **A cancel stops the writer before anything else happens**: the controller suspends the run Job, the kubelet sends SIGTERM, the runner forwards SIGTERM to the engine and SIGKILLs it after 10 s, and the run is Interrupted only once no pod of the Job is left.
7. **The point an Interrupted restore reads is held until it is cleaned up** (or the `Restore` is deleted). Retry is a new `Restore` into fresh names behind the workspace's approval mode; the console pre-fills it.

## 1. Schema Registry (PROD-03.1, PROD-03.2)

### 1.1 What was measured (prototype, 2026-10-16)

One `apache/kafka:4.3.1` broker on a private Docker network, a source Confluent Schema Registry, and as targets Confluent (two fresh), Karapace, Redpanda (two fresh) and Apicurio; scripts and logs in `/tmp/logweir-roadmap-run/scratchpad/od23-core/proto/` (`populate.sh`, `snap.py`, `imp.py`). The source held 7 subject-versions with IDs 4–10 (IDs 1–3 burnt and hard-deleted, so a fresh registry's counter cannot match by chance): an Avro value with a reference and two versions, one of them soft-deleted; an Avro key; a Protobuf message importing another; a JSON Schema. Records were produced with Confluent's console serializers by fixed schema ID.

| Target (digest) | `PUT /mode/{s}` `IMPORT` | ID kept on register | Deserializing consumer on the original bytes |
|---|---|---|---|
| Confluent `cp-schema-registry:8.0.0` (`sha256:9c1a706e…`) | yes (default config; `?force=true` on a non-empty subject) | yes | Avro (with reference), Protobuf (with import), JSON: all read |
| Redpanda `v26.2.4` (`sha256:c98c2f04…`) | yes, only on an empty subject unless `?force=true` | yes | all read |
| Karapace `6.2.3` (compose `registry` profile's pin) | **405** | **no**: IDs 1–7 assigned instead | not run: wrong IDs |
| Apicurio `3.0.9` ccompat (`sha256:51bd0274…`) | 204, but `GET /mode/{s}` does not answer `IMPORT` | **no** (one matched by coincidence) | not run |

Also measured: an import is idempotent (the second run classified all 7 as present and wrote nothing); a target ID held by a different schema was found by the preview and the run refused before writing (`imp.py` exit 3); re-applying a soft delete is unsafe: on Confluent, an imported version soft-deleted while it was its subject's only version stopped resolving by ID (`GET /schemas/ids/5` → 404, the consumer failed), so imported versions stay live. The portable snapshot calls (`/subjects?deleted=true`, `/subjects/{s}/versions[?deleted=true]`, `/subjects/{s}/versions/{v}?deleted=true`) answered identically on Confluent, Karapace and Redpanda.

### 1.2 Capture (03.1)

- **Where it runs:** the backup runner, after the engine finishes (so every ID a captured record carries was registered before the snapshot), when the plan's `source.schema_registry` is set. The controller renders that block from the source `KafkaCluster`; the password reaches the runner as the connection's other credentials do, under its binding (§4).
- **What it holds:** every subject (soft-deleted included), every version with its ID, type, schema text, references and a `deleted` flag. The whole registry, not a selection: the sampled detection of PROD-03.0 cannot say which IDs a large topic uses, and a registry is small. Bounded: at most 32 MiB of JSON and 100 000 versions, 10 s per request, 300 s in all.
- **Where it goes:** one create-only JSON object beside the backup's receipt, written before the receipt is signed; the receipt's new optional block `schema_registry` names its key and sha256 and the counts.
- **Completeness:** the block says `captured: false` with a reason (`registryUnreachable`, `registryUnauthorized`, `snapshotTooLarge`, `snapshotIncomplete`) or lists `unresolved_references` (a reference to a subject-version the snapshot lacks) and the IDs PROD-03.0 saw in the records that the snapshot lacks. Any of those is incomplete coverage on the backup and the catalog point. **Never a failed backup**, as 03.0's rule.
- **Not doing:** a separate `SchemaRegistry` kind, because a registry describes the topics of the cluster whose connection names it; compatibility settings, because reading needs only the schemas (stated as not captured); backing up `_schemas` (the tracker's cheaper slice), because Redpanda, Apicurio and hosted registries do not restore from a topic and replaying it into a live registry corrupts it; schema contexts, header-carried IDs, Apicurio's 8-byte IDs and AWS Glue framing, because the detection does not see them (03.0's stated limits); a periodic registry probe, because the connection test and every backup report it; a registry CA separate from the connection's `tlsCa`, because the connection's CA (when set) is added to the system roots.

### 1.3 Restore (03.2)

1. **Needed IDs.** `archive_scan::run` (`crates/logweir/src/drill/mod.rs:2292`) also reads each key's and value's first five bytes and keeps the framed IDs found in the snapshot, per topic side. An ID the snapshot lacks is never collected, so a `LongSerializer` key cannot flood the set (03.0's residual false positive is harmless here). Framed IDs not in the snapshot on a side where at least one in ten records resolves are counted and named in the scorecard.
2. **Closure.** The subjects holding a needed ID, and transitively the subjects their references name; each such subject is imported whole (all its versions), in reference order.
3. **Classify against the target**, before topic creation (`drill/mod.rs:2584`): an ID the target holds is `present` only when the target's own lookup of the snapshot schema under that subject answers the same ID and version (`POST /subjects/{s}?deleted=true`, so text normalisation does not matter); a subject the target holds may contain only versions identical to the snapshot's. Anything else refuses `SchemaConflict`, naming the subject, version and ID. No topic is created.
4. **Import:** per subject, `PUT /mode/{s}?force=true` `IMPORT`, then `GET /mode/{s}` must answer `IMPORT` (else `SchemaRegistryCannotKeepIds`, nothing written); register each missing version with its `id` and `version` and require the answer's ID to equal it (else stop, `SchemaRegistryCannotKeepIds`); `PUT /mode/{s}` `READWRITE`. A failure part-way leaves identical versions only, so a retry continues (rule 3 allows identical versions).
5. **Then the restore runs as today.** No byte changes, so every existing fingerprint, count and verdict holds unchanged.
6. **Evidence:** the scorecard's new optional block `schemas`: snapshot sha256, target registry URL, the imported subject-versions with IDs, the count already present, the referenced IDs missing from the snapshot, or `not_imported` with the reason (`noTargetRegistry`, `noSnapshot`). An archive with a snapshot restored to a target connection without a registry is not refused (a drill into a scratch cluster needs no registry); its review and scorecard say the schemas were not imported.
7. **Preview:** the restore readiness check (`Preflight`, operation Restore) gains `target.schemas`: the target registry answers with the credential, the snapshot verifies, and the classifier of step 3 over the IDs the receipt's `schema_dependency` lists for the selected topics. It says that the run recomputes the set from every record.
8. **Imported schemas are never removed**, by phase 9 or by cleanup: a registry delete is permanent and an imported schema is inert. The scorecard lists them.
- **Not doing (c), ID mapping with byte rewriting:** it would add restores into registries that cannot keep IDs (Karapace, Apicurio ccompat) or whose IDs are taken, at the cost of rewriting key/value bytes (and Protobuf indexes), a second reconciliation key for every verifier, and a mapping document. Not needed now: a recovery target registry is either empty or the source's own, and both work with (a). The engine's own `rewrite_schema_ids` (`E:restore/engine.rs:1824`) stays unrendered.
- **Not doing:** replaying soft deletes (measured unsafe, §1.1); importing compatibility settings or mode; a per-plan "skip schemas" switch (choose a target connection without a registry).
- **Tracker text to reword (03.2's acceptance):** "a real application deserializes restored Avro records against a target registry with different IDs" becomes "…against a target registry that held none of the source's schemas, with the source's IDs"; "unmapped required schemas block before data writes" becomes "a conflicting or unkeepable schema ID blocks before any topic is created; a referenced ID the snapshot lacks is named". 03.1's "compatibility settings" is cut.

### 1.4 Acceptance rows

Compose `registry` profile, extended by one service `registry-target`: a Redpanda v26.2.4 node used only for its Schema Registry, HTTP basic auth on (user `logweir`, the fixture password), port 9145. Karapace stays the source registry.

| # | Pass predicate | Negative control |
|---|---|---|
| SR-1 | A backup of topics framed as in §1.1 (shared reference, Protobuf import, JSON, two versions, one soft-deleted, IDs from 4) records `schema_registry.captured: true`, every subject-version and ID, the snapshot's sha256 equal to the object's; both verifiers and the parity script accept the receipt | Karapace stopped before the backup: the backup succeeds, `captured: false` `registryUnreachable`, the catalog point says registry not captured; a mutant that drops soft-deleted versions fails the row |
| SR-2 | A reference to a hard-deleted subject-version is listed in `unresolved_references` and the coverage is incomplete | A snapshot writer that ignores references reports complete: the row fails |
| SR-3 | Wrong registry password: `registryUnauthorized`, backup succeeds; a Secret without the connection's binding is refused `CredentialBindingMismatch` before any request | The binding check removed: the run presents the password and the row fails |
| SR-4 | Restore into `registry-target` (empty): `schemas.imported` lists the closure; the target answers each source ID with the same schema; a deserializing consumer (Avro, Protobuf, JSON) reads every restored record with the source's values; the restore's complete verification passes (no byte changed) | The same restore with a target connection without a registry: the consumer fails on an unknown ID |
| SR-5 | A subject using RecordNameStrategy (`com.ex.Order`) is imported and readable: no naming strategy is assumed | A strategy-based closure (`<topic>-value`) misses it: the row fails |
| SR-6 | The target holds one needed ID with a different schema: refused `SchemaConflict` before topic creation; no target topic exists, nothing registered | A mutant that skips step 3: a topic is created or a schema written |
| SR-7 | Target is Karapace: refused `SchemaRegistryCannotKeepIds`; no schema registered, no topic | A mutant without the `GET /mode` and returned-ID checks registers schemas under other IDs |
| SR-8 | The import is stopped after the first subject (registry paused); the retry skips what is present, imports the rest, and the consumer reads | A mutant that treats a partly imported subject as a conflict refuses the retry |
| SR-9 | Target registry is the source's own: everything present, nothing written, consumer reads | — (the idempotence half of SR-8) |
| SR-10 | Old documents: a receipt and a scorecard without the new blocks read "registry not captured" / no schemas line in both verifiers | A reader that treats an absent block as captured fails |

## 2. The engine rows (00.3a, 00.3d, 00.3e, 00.3h)

Each is one patch in `third_party/kafka-backup-patches/` under the folder's policy (`third_party/kafka-backup-patches/README.md`: `Reason:` line, `git apply` without fuzz, an oracle, dropped when an OSO release contains it). The four land together as **one** new build, `0.23.3+logweir.5`: bump `third_party/kafka-backup-build.env`, append to `kafka-backup-builds.txt` and to `SHIPPED_BUILDS` in `crates/logweir/tests/engine_pin.rs`. Existing patches 0001–0003 are carried unchanged. No row changes the archive format (records keep the engine's shape; `BackupRecord.headers` is already a list, `E:kafka/fetch.rs:200-218`) or a Logweir evidence format; the scorecard and receipt already name the engine build.

**00.3a, committed-only capture (C1).** Patch `0004`: fetch and both ListOffsets at `isolation_level` 1 (`E:kafka/fetch.rs:53`, `:263`, `:337`), so a capture ends at the last stable offset; skip control batches; drop the batches of aborted transactions with the fetch response's `aborted_transactions` and each batch's producer id (the consumer's algorithm). The next fetch offset still advances over skipped batches (`E:kafka/fetch.rs:191`). **Always on, no setting.** On a non-transactional topic the last stable offset is the high watermark, so nothing changes. Logweir change: none in code. PROD-01.1a's marker refusal (`crates/logweir-core/src/transaction_markers.rs`) stays as it is: a new archive holds no marker, so it never triggers; an archive written before build 5 still does. The documentation that forbids saying "transactional" (`decisions/PROD-01.1-record-semantics.md` §6.1) is updated with the row. Builder check: the receipt's `source_watermarks` arms 41–45 (`crates/logweir-core/src/backup_receipt.rs:463-470`) must not judge an archive that ends at the last stable offset below the high watermark as short. Not doing: a `capture: readCommitted` manifest field, because the engine build in the receipt already says it and the marker scan already passes such an archive; an option to capture uncommitted data, because nobody asked.

**00.3d, idempotent restore produce (C5).** Patch `0005`: one `InitProducerId` (no transactional id) per restore process; every produced batch carries that producer id and epoch and a per-partition base sequence that advances only on success, and a re-send reuses its sequence, so the broker discards the duplicate. The restore writes each target partition from one task at a time (`E:restore/engine.rs:1846-1860`; the repartition path drains one channel per target partition, `E:restore/repartition.rs:405-440`), so in-flight is 1 per partition. `OUT_OF_ORDER_SEQUENCE_NUMBER` or `UNKNOWN_PRODUCER_ID` fails the restore (it is then Interrupted, §3). Logweir change: none. A duplicate inside an attempt still fails the verdict, as today (`decisions/PROD-07.1-resume-semantics.md` §6 F6); 00.3d removes the cause, not the check. Needs the target principal's Write on the topic (IdempotentWrite is implied since Kafka 3.0): `docs/support-matrix.md` says so.

**00.3e, repeated header keys (C13).** Patch `0006`, without forking `kafka-protocol` (its `Record.headers` is an `IndexMap`, `kafka-protocol-0.18.0/src/records.rs:184`, filled by `insert`, `:919`): both of its public hooks are used. Decode: `RecordBatchDecoder::decode_with_custom_compression` (`records.rs:441`) gets a decompressor that keeps a clone of the decompressed records bytes, and a ~100-line record-v2 header reader fills `BackupRecord.headers` as an ordered list. Encode: `RecordBatchEncoder::encode_with_custom_compression` (`records.rs:212`) gets a compressor that writes the engine's own encoding of the records, headers as a list, compressed with the batch's codec; the crate fills the size and CRC after the hook (`records.rs:369-409`). Logweir change: none; the phase-7 half the tracker asks for is already done (`phase7_verify.rs:159-169`, `complete::lineage_offset`).

**00.3h, the byte-rate limit (C12).** Patch `0007`: `rate_limit_bytes_per_sec` (parsed and only validated, `E:config.rs:908`, `:1368-1371`) paces **the whole restore**: one shared next-allowed instant, each batch reserving its key-and-value bytes over the limit before it is produced, at both produce sites beside the records limit (`E:restore/engine.rs:1846-1851`, `E:restore/repartition.rs:419-423`). Logweir change: the plan's `restore.rate_limit_bytes_per_sec` (optional integer ≥ 1; absent means no limit, as today), rendered into the engine's restore block; the approver sees it in the plan bytes; the console's restore wizard has one optional field (MB/s). Not doing: a per-partition limit, because an operator sets a limit to protect the cluster; a backup-side limit, because no documented key exists and nobody asked; exposing `rate_limit_records_per_sec`, because one limit is enough; PROD-10.1's bounded controls, Deferred.

| # | Row | Pass predicate | Negative control |
|---|---|---|---|
| EN-1 | 00.3a (A-C1-1) | The TXN row (`e2e/tests/record_semantics.rs:1036`): capture, replay and end-to-end divergence sets empty; no control-shaped record in the archive; transaction D's records absent; and a restore of the new archive runs without `restore.allow_transaction_markers` | A build that fetches READ_COMMITTED but keeps control batches reports seven `extra:control-marker`; an archive from build 4 is still refused `TransactionMarkersInArchive` |
| EN-2 | 00.3d (A-C5-1) | The ack-fault row on the `faultproxy` profile (`arm drop N` on one produce response mid-restore): no `duplicate` divergence; the complete lane's `duplicates` is 0 | Build 4 under the same drop: at least one duplicate |
| EN-3 | 00.3e (A-C13-1) | The shapes row (`record_semantics.rs:2131`): capture and replay sets empty and Logweir's verdict passes | Build 4: the repeated key collapses |
| EN-4 | 00.3h (A-C12-1) | Measure build 4's restore throughput T; with L ≤ T/3, build 5's restore throughput is at most 1.2 L (a ratio, never a duration) over two partitions restored in parallel | Build 4 with the same key measures at least 3 L |

All four run through the published runner image (`LOGWEIR_E2E_ENGINE_IMAGE`, WORKER-RULES "On this Mac the default engine route makes contract rows vacuous"), with "contract not asserted" 0 times.

## 3. Honest interruption (PROD-07.2)

Stands alone: nothing here resumes, and nothing waits for PROD-07.3.

### 3.1 States

| Situation | Phase, reason |
|---|---|
| Cancel before a Job exists (`Pending`, `Queued`) | `Failed`, `Cancelled`; nothing ran |
| Cancel while running | `Running`, condition reason `Stopping` until no pod of the run Job is left (a pod on a lost node keeps it here, naming the pod; the operator force-deletes it) |
| The run ended with a signed scorecard (exit 0 or 2) | unchanged: the scorecard decides, even if a cancel arrived meanwhile |
| The run ended without one, before it created a target | `Failed`, as today |
| The run ended without one, after it created a target (killed, evicted, deadline, engine exit ≠ 0, cancel, a Logweir read failure after the engine) | **`Interrupted`**, `status.interruption.cause` (`Cancelled`, `RunnerStopped`, `RunFailed`) |

"Created a target" is the **targets record**: right after its creation step (`drill/mod.rs:2584`), before the engine starts, the runner puts one create-only object, `logweir/drills/<run_id>.targets.json`: run id, plan hash, target cluster id, and per created topic its name, topic ID and partition count (the `target.creation` facts the scorecard already records for clone cleanup). It is internal working state read only by Logweir's reconcile, not a verified evidence format; the signed scorecard of §3.2 is the evidence.

### 3.2 Reconcile: the signed list of partial targets

On `Interrupted` the controller starts `<restore>-reconcile`, a copy of the run Job's pod (the pattern of `crates/weirkeeper/src/controllers/restore_clone.rs:1-60`) with the argv of the new `logweir restore reconcile --run-id <id>`. It:

1. reads the targets record and keeps only topics whose cluster ID and topic ID are still the recorded ones (a recreated name is left out and named);
2. waits until the targets' end offsets are unchanged over 10 s (at most 5 min, else `TargetStillGrowing`);
3. runs PROD-08.1's complete verification of each kept target against the archive (phase 7's complete lane; the plan's `complete_max_records` if set) without restoring anything, admitting exactly the recorded targets past phase 0's existing-target refusal (`crates/logweir/src/drill/phase0_admit.rs:625-650`, which stays for every other run);
4. signs an ordinary scorecard for its own run id: `outcome: fail-integrity`, `complete` coverage with per-partition `expected`, `restored`, `matching`, `missing`, `duplicates`, `unexpected`; `target.creation` with the kept topics; `sample.coverage_note` "reconciliation of interrupted run <id> (<cause>); nothing was resumed"; no positions mapping.

If every partition is exact, the scorecard is a `pass` and the `Restore` becomes `Succeeded`, reason `ReconciledComplete` (07.1-I6: re-verified without re-restoring). The controller copies the counts into `status.integrity` as for any run. A failed reconcile is retried at most 3 times, 5 minutes apart (restore_clone's constants), then `status.interruption.verification: Failed` with the reason; the point stays held and Clean up is not offered (it needs the signed record).

### 3.3 Cancel, cleanup, hold, retry

- **Cancel:** `POST …/restores/{name}/cancel` sets the annotation `logweir.dev/cancel-requested-by: <subject>` (metadata, so the sealed spec is untouched; a second POST is 200 with nothing written). The controller sets `spec.suspend: true` on the run Job: the Job controller deletes its pod, the kubelet sends SIGTERM and SIGKILLs at the pod's grace (30 s, set explicitly on run Jobs).
- **The runner:** the engine is spawned instead of `Command::output()` (`crates/logweir-engine-oso/src/subprocess.rs:213-230`); `logweir` installs SIGTERM and SIGINT handlers (as PID 1 it has none today), forwards SIGTERM to the engine, SIGKILLs it after 10 s, reaps it, signs nothing and exits 1 with `failure-reason=Cancelled`. On Linux the engine gets `PR_SET_PDEATHSIG(SIGKILL)`, so it never outlives a killed `logweir` (flips `a_killed_restore_leaves_its_engine_writing`, `record_semantics.rs:2833`). In docker-engine mode (development hosts) the engine container is named per run and killed on SIGTERM; a SIGKILLed `logweir` there can leave it running, stated.
- **No checkpoint is rendered** (07.1-I4): the two lines at `crates/logweir-engine-oso/src/render_restore.rs:273-279` and the `RestorePlan` fields (`crates/logweir-core/src/engine.rs:477-478`) go; the `docs/kubernetes.md:2931` `TMPDIR` row drops "and the checkpoint".
- **Clean up:** `POST …/restores/{name}/cleanup` (only on `Interrupted` with a signed reconcile) sets `logweir.dev/cleanup-requested-by`. The controller runs `<restore>-cleanup`: `logweir restore cleanup` (`crates/logweir/src/restore_cleanup.rs:1-30`) against the reconcile's scorecard; its deleter is scoped to the plan's mapped target names instead of the clone prefix, so an interrupted original-name restore can be cleaned too. Its report goes to `status.interruption.cleanup`; topics it leaves (an ID changed) are listed, never deleted.
- **Hold:** `TERMINAL_RESTORE_PHASES` (`crates/weirkeeper/src/controllers/retention_policy.rs:4176`) does not list `Interrupted`, so the point is held; the hold ends when `status.interruption.cleanup` succeeded or the `Restore` is deleted. The run pool's terminal set (`crates/weirkeeper/src/run_pool.rs:217-219`) does list `Interrupted`, so it frees its slot.
- **Retry:** a new `Restore` with fresh target names and its own approval under the workspace's approval mode; the console's "Retry into new topics" opens the restore wizard pre-filled from this one with a new prefix. Phase 0's refusal keeps a second execution out of the first one's targets (07.1-I5).
- **Idempotence:** the reconcile and cleanup Jobs have deterministic names (a controller restart finds them); cancel and cleanup are annotations (a repeated click writes nothing); the controller never starts a retry. A browser holds no state.
- **Console** (`console/src/pages/restores/RestoreDetail.tsx`): `Cancel` while Pending, Queued or Running (with a confirm); "Stopping…" while the pod goes; on `Interrupted` an amber state "Stopped before it finished; nothing was resumed", a table per target topic and partition (matching of expected, missing, duplicates) from the signed reconcile, and two actions: **Retry into new topics** and **Clean up partial topics** (a confirm listing the topics); after cleanup, what was removed and what was left.
- **Not doing:** a separate interruption document format (the scorecard carries it); a tail offset per partition (that is PROD-07.3's resume input); a "verify again" action (three automatic attempts cover a transient fault); deleting imported schemas; a second approval for cleanup (open point 3); an automatic retry.

### 3.4 Acceptance rows (07.1 §7.1's rows on the product path)

| # | Pass predicate | Negative control |
|---|---|---|
| IR-1 (I1) | Runner pod deleted inside the first topic (compose: the runner process killed; PoC: the pod): `Interrupted`; the reconcile scorecard lists every created target with per-partition counts equal to a separate complete verification of the same targets | A generic `Failed`, or counts that differ, fail the row |
| IR-2 (I2) | Cancel mid-topic: after the grace no engine process or container of the run remains, end offsets stop moving, and only then `Interrupted` | Build without the handler: `a_killed_restore_leaves_its_engine_writing` reaches the whole archive |
| IR-3 (I3) | Engine stopped inside the first of two topics, by cancel or by a signal to the engine alone: never `Succeeded`/`pass` under either coverage | T1/T2 of `e2e/tests/stopped_restore.rs:324`, kept green (FX-23) |
| IR-4 (I4) | The rendered restore document has neither `checkpoint_state` nor `checkpoint_interval_secs` | A mutant rendering a stable path (consequence S1) |
| IR-5 (I5) | Retry into fresh names succeeds; a second execution naming the old partial target is refused at phase 0 | A retry reusing the old name |
| IR-6 (I6) | Engine finished, then Logweir's read failed (07-1c's pause-at-exit): `Interrupted`, then `Succeeded` `ReconciledComplete`, with no second restore | A reconcile that re-produces |
| IR-7 (I7) | While `Interrupted`, retention reports the point `ActiveRestore` and releases it after Clean up; Clean up deletes only the recorded topics with their recorded IDs | A target deleted and recreated by someone else under the same name is left in place and named; a retention run while Interrupted keeps the point |
| IR-8 | Cancel racing completion: a cancel that lands after the run signed leaves its outcome; one that lands before leaves `Interrupted`; a controller restart during reconcile starts no second reconcile Job; two cancel POSTs write one annotation | — |
| IR-9 | Target credential revoked during reconcile: three attempts, then `verification: Failed`, point still held, Clean up not offered | — |

## 4. Interfaces, decided once

| Surface | Name and shape | Owner row |
|---|---|---|
| CRD field | `KafkaCluster.spec.schemaRegistry: {url, username?, secretRef?: {name, passwordKey (default "registry-password")}}`, immutable with the rest of `spec`; https or http URL, no userinfo | 03.1 |
| Binding | `connection::credential_binding` (`crates/logweir-core/src/connection.rs:299-317`) appends `registry=<url>\nregistryUsername=<u>\n` only when the block is set (existing bindings unchanged); every Secret the connection names carries the one binding | 03.1 |
| Runner env | `LOGWEIR_SOURCE_REGISTRY_PASSWORD`, `LOGWEIR_TARGET_REGISTRY_PASSWORD` (optional `secretKeyRef`s, checked against the binding like the Kafka credential) | 03.1, 03.2 |
| Backup plan | `source.schema_registry: {url, username?}` (rendered by the controller) | 03.1 |
| Restore plan | `target.schema_registry: {url, username?}` (from the target connection; the controller checks it equals the connection's, as for the bootstrap servers); `restore.rate_limit_bytes_per_sec: <int ≥ 1>` | 03.2; 00.3h |
| Engine render | `rate_limit_bytes_per_sec` in the restore block; `checkpoint_state`, `checkpoint_interval_secs` removed | 00.3h; 07.2 |
| Receipt | optional `schema_registry: {captured, reason?, snapshot_key, snapshot_sha256, subjects, versions, unresolved_references[≤32], ids_not_in_snapshot[≤32]}`; next receipt MINOR, written only with the block (OD-7 (a)) | 03.1 |
| Snapshot object | `{format: 1, captured_at, registry_url, versions: [{subject, version, id, schemaType, schema, references, deleted}]}`, create-only beside the receipt | 03.1 |
| Scorecard | optional `schemas: {snapshot_sha256, target_registry_url, imported[{subject, version, id}], already_present, ids_not_in_snapshot[≤32]} \| {not_imported: noTargetRegistry\|noSnapshot}`; next scorecard MINOR, only with the block. 07.2 adds no field | 03.2 |
| Targets record | `logweir/drills/<run_id>.targets.json` `{run_id, plan_hash, cluster_id, topics[{name, topic_id, partitions}]}`, create-only, internal | 07.2 |
| Refusals | `SchemaConflict`, `SchemaRegistryCannotKeepIds`, `SchemaSnapshotInvalid` (sha256 or parse), `SchemaRegistryUnreachable` (restore only; exit 3, before topic creation); backup coverage reasons `registryUnreachable`, `registryUnauthorized`, `snapshotTooLarge`, `snapshotIncomplete` | 03.1, 03.2 |
| Check ids | `registry.reachable` (connection test and backup readiness), `target.schemas` (restore readiness) | 03.1, 03.2 |
| Restore phase | `Interrupted`; reasons `Cancelled`, `Stopping`, `ReconciledComplete`; `failure-reason=Cancelled` runner line | 07.2 |
| Restore status | `interruption: {cause, requestedBy?, writerGoneAt, verification: Running\|Done\|Failed, verificationReason?, reconcileRunId, cleanup?: <CloneCleanupReport>}`; `schemas: {imported, alreadyPresent, notImported?}` copied from the scorecard | 07.2; 03.2 |
| Backup / point status | `schemaRegistry: {captured, subjects, versions, incompleteReason?}` from the receipt | 03.1 |
| Annotations | `logweir.dev/cancel-requested-by`, `logweir.dev/cleanup-requested-by` on `Restore` | 07.2 |
| API | connection create body: `schemaRegistry {url, username?, password?}` (the password goes into the connection's own Secret under `registry-password`); `POST …/restores/{name}/cancel`, `POST …/restores/{name}/cleanup` (409 in a phase that does not allow it); restore view: `interruption`, `schemas` | 03.1; 07.2 |
| CLI | `logweir restore reconcile --run-id <id>` (Job argv; same flags as `restore cleanup`); the registry passwords through the env names above | 07.2; 03.x |
| Chart | the API's Role gains `patch` on `restores` (annotations only; `spec` stays sealed by CEL). No new value | 07.2 |
| Engine build | `0.23.3+logweir.5`, patches `0004-committed-only-capture`, `0005-idempotent-produce`, `0006-header-list`, `0007-byte-rate-limit` | 00.3a/d/e/h |
| Docs | `docs/stability.md` Never #2 narrowed (below), Later #13 delivered; `docs/kubernetes.md` registry, Interrupted, TMPDIR; `docs/support-matrix.md` registries proven and refused | all |

**Never #2, a builder task (rule 8, one commit with `crates/logweir/tests/doc_lint.rs:1440-1445`, the `NEVER` list):** the entry becomes "**RBAC-MDS and CSFLE; Schema Registry beyond backup, resolution and selective import**", its reason saying that Logweir captures a Confluent-compatible registry's schemas and imports the ones a restore needs, keeping their IDs (OD-23), and does not evaluate a registry's RBAC, hold field-level encryption keys, rewrite schema IDs or manage registry configuration.

## 5. Interfaces with the other record (PROD-19.1)

What this design needs from it, and assumes:
- `KafkaCluster` is a workspace object; its Secret (holding `registry-password` too) is in the release namespace and follows PROD-19.1's naming and its cross-workspace refusal. A restore's target connection and its source point must be in the same workspace; nothing here references across workspaces.
- The API route prefix is PROD-19.1's; this record writes `…/restores/{name}/cancel` relative to it.
- Roles: creating a connection (with a registry) is whoever may create connections in the workspace; **cancel and cleanup** are the role that may create restores in the workspace (no approval); a retry follows the workspace's approval mode.
- The reconcile and cleanup Jobs carry the run Job's workspace label, so per-workspace views and the run pool see them.
- Builders can start before PROD-19.1 lands, on the current namespace routes; the prefix change is its mechanical edit.

## 6. Work split and file ownership

| Builder | Owns | Needs from 19.1 |
|---|---|---|
| **B1 engine** (00.3a/d/e/h, and the render half of 07.2-I4) | `third_party/kafka-backup-patches/0004`–`0007` and its README table, `kafka-backup-build.env`, `kafka-backup-builds.txt`, `crates/logweir/tests/engine_pin.rs`, `crates/logweir-engine-oso/src/render_restore.rs`, `RestorePlan` in `crates/logweir-core/src/engine.rs`, `RestoreSpecBlock` in `crates/logweir-core/src/spec.rs` (`rate_limit_bytes_per_sec`), `drill/mod.rs:3347-3348` and `:2466-2471` (checkpoint path), `e2e/tests/record_semantics.rs` rows EN-1–EN-4, `docs/support-matrix.md` engine lines | nothing |
| **B2 schema** (03.1, 03.2) | new `crates/logweir-core/src/schema_registry.rs` (snapshot model, closure, classifier: pure), new `crates/logweir/src/backup/schema_registry.rs`, new `crates/logweir/src/drill/schema_import.rs`, `archive_scan.rs` (ID collection), `backup_receipt.rs` and `scorecard.rs` blocks, `docs/verify_scorecard.py`, `scripts/check-verifier-parity.sh`, `TargetSpec`/`BackupSourceSpec` in `spec.rs`, `crds/kafka_cluster.rs`, `connection.rs` binding, the controller's env projection and plan rendering for registries, `routes/connections.rs` and its contract types, the two check ids, `docs/stability.md` Never #2 + `doc_lint.rs`, `e2e/compose` `registry-target` + `e2e/README.md` profile row, new `e2e/tests/schema_registry.rs` (SR-1–SR-10) | workspace on `KafkaCluster` and its Secret naming |
| **B3 interruption** (07.2) | `crates/logweir-engine-oso/src/subprocess.rs`, the runner's signal handling (`crates/logweir/src/main.rs`/`cli.rs`), the targets record in `drill/mod.rs` after `:2584`, new `crates/logweir/src/drill/reconcile.rs`, `restore_cleanup.rs` (scope by mapped names), `controllers/restore.rs` Interrupted path, new `controllers/restore_interruption.rs`, `crds/restore.rs` `status.interruption`, `retention_policy.rs`, `run_pool.rs`, `routes/restores.rs` cancel/cleanup, `app.rs`, `kube.rs`, chart Role, `docs/kubernetes.md`, `docs/stability.md` Later #13, new `e2e/tests/interrupted_restore.rs` (IR-1–IR-9) | role names for cancel/cleanup; route prefix |
| **B4 console** (all three rows' screens) | `console/src/pages/connections/AddConnection.tsx`, `ConnectionDetail.tsx` (registry section and its test line), `console/src/pages/backups/*` and `catalog/*` (registry captured line), `console/src/pages/restores/*` (rate limit field, schema preview, `schemas` line, Cancel, Interrupted state, Retry, Clean up), `console/src/api/*` types | the workspace switcher and routes |

Shared files, distinct hunks: `spec.rs` (B1 `RestoreSpecBlock`, B2 `TargetSpec`/`BackupSourceSpec`), `crds/restore.rs` (B2 `status.schemas`, B3 `status.interruption`), `contract.rs` (B2 connection request, B3 restore view), `drill/mod.rs` (B1 checkpoint lines, B2 the import call between `:2292` and `:2584`, B3 the targets record after `:2584`), `cli.rs` (B3). B4 builds against §4's shapes from the start and rebases once B2 and B3 merge.

## 7. The combined acceptance pass

**Compose (each builder once on its own slot, then the orchestrator's one run):** EN-1–EN-4 through the published runner image of the integration branch; SR-1–SR-10 on `--profiles registry`; IR-1–IR-9 (IR-1, IR-2, IR-6 on the CLI path; IR-7–IR-9 through the controller rows the suite already drives); the shapes, TXN, `stopped_restore` and `resume_semantics` rows that exist stay green.

**PoC (one fresh install in one namespace, workspaces `dev` and `prd`; fixtures the orchestrator adds: a Karapace beside the source Kafka, a Redpanda Schema Registry with basic auth beside the target, a producer Job writing Avro-with-reference, Protobuf and JSON records, and a transactional producer Job; all pinned by digest).** Every step by visible clicks (OD-19), with the pass predicate:

1. In `dev`, **add a connection** to the source Kafka with its registry (URL, no auth) and **test it**: Kafka and `registry.reachable` pass. Add the target connection with the target registry URL and credential; test passes. Edit nothing; a second target connection with a wrong registry password: the test shows the registry failed and Kafka passed.
2. **Back up** the schema-dependent topics: the backup and the catalog point show "Registry captured: N subjects, M versions" and no incomplete note.
3. **Restore** that point into the target connection (new topics): the readiness check shows `target.schemas` "will import K subjects, none conflict"; approve (workspace mode); the restore succeeds and its page shows the imported subjects. A consumer pod (`kafka-avro-console-consumer` and the Protobuf and JSON ones, against the target registry) prints every restored record with the source's values.
4. Restore the same point again into a second prefix: "already present: K", nothing imported, consumer reads.
5. **Transactional topic:** the producer Job commits two transactions and aborts one; back up; restore with no override: the restored topic's record count equals the committed records; a `read_uncommitted` consumer sees no aborted record and no marker.
6. **Interrupted restore:** start a restore of a topic large enough to run a minute (a byte-rate limit in the wizard makes it so, which is also 00.3h's live check: the run's restore time is at least the bytes over the limit); click **Cancel**: "Stopping…", then **Interrupted** with the partial targets table; the retention view shows the point held (`ActiveRestore`). Click **Clean up partial topics**: the topics are gone from the target (the topic list), the page shows them removed, the hold is released. Then **Retry into new topics**: the wizard opens pre-filled with a new prefix.
7. Start another restore and delete its runner pod with `kubectl delete pod` (the one non-click step): **Interrupted**, cause `RunnerStopped`, same table.
8. In `prd`, the `dev` connections are not listed and cannot be chosen (PROD-19.1's row, run here because the registry rides on the connection).

## 8. Open points for the owner

**Decided 2026-10-16 by the owner: all five as recommended.**

1. **Registries that cannot keep IDs are refused** (Karapace 6.2.3, Apicurio 3.0.9's Confluent API, and any registry without IMPORT mode, §1.1). ID mapping with byte rewriting (c) would serve them at the cost of a second reconciliation model. *Recommendation:* accept the refusal now; (c) comes back only on a real need.
2. **The registry is set when the connection is created.** Connections are immutable (`crates/weirkeeper/src/crds/kafka_cluster.rs:339`), so adding a registry to an existing connection means a new connection. *Recommendation:* accept; a fresh install (OD-23) starts with it.
3. **Clean up needs no second approval.** It deletes only topics the interrupted run created, by recorded topic ID, as phase 9 and clone cleanup already do. *Recommendation:* the role that may create restores in the workspace may clean up.
4. **Committed-only capture is always on.** A topic with a transaction left open is captured up to the last stable offset until the broker aborts it (`transaction.max.timeout.ms`, 15 minutes by default). *Recommendation:* accept, no setting.
5. **The test target registry is Redpanda's** (BSL, pulled, never redistributed, as the `redpanda` profile already is), because the repository does not use Confluent's image (`e2e/compose/docker-compose.yml:901-902`, Community License). Confluent 8.0.0 is proven by this record's prototype only. *Recommendation:* accept; no Confluent row in CI.

## 9. Corrections to the tracker found here

- `decisions/PROD-00-engine-route.md` A-C13-1 and PROD-01.1 §9 say phase 7 keys the target by the FIRST `x-original-offset` (`phase7_verify.rs:288-293`); it keys by the last since PROD-08.1 (`phase7_verify.rs:159-169`), so 00.3e's Logweir half is done.
- `decisions/PROD-07.1-resume-semantics.md` cites phase 0's existing-target refusal at `phase0_admit.rs:579-600`; it is at `:625-650` now.
- PROD-03.2's issue and acceptance assume different IDs on the target and a mapping; with §1.3 the IDs are the source's (reword as §1.3 says). PROD-03's boundary "starts with … Avro": the REST import is format-agnostic, and Avro, Protobuf and JSON Schema were all read back.
- `e2e/README.md`'s `registry` row ("Logweir never talks to either") changes with SR-1.

## 10. What was run

The prototype of §1.1 only: containers `od23c-*` on the network `od23core-proto` (no compose slot, no cluster, no PoC), images pulled with the anonymous Docker config; all removed by name afterwards (`docker rm -f` of the nine recorded names, `docker network rm`). Engine and `kafka-protocol` facts are read from source (the vendored tarball extracted under the scratchpad, and `~/.cargo/registry/src/*/kafka-protocol-0.18.0`).

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
