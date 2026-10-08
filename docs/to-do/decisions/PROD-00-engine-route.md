# PROD-00.1 — Engine route per capability

- Row: PROD-00.1 (research, Tier B, lab `compose`), [product-expansion tracker](../product-expansion.md).
- Status: **proposed**. The routes below are a recommendation. [OD-3](../product-expansion.md#owner-decisions) is the owner's, and nothing here decides it.
- Date: 2026-09-28. Branch `claude/prod-00-1`, from main `adee0a16`.
- Kind: research. Source first, then runs on the e2e compose stack. The row repaired `engine-matrix` and corrected the support docs; it changed no product code, no engine pin and nothing in `third_party/`.
- **Addendum, 2026-10-07 (PROD-00.3f, branch `claude/prod-00-3f`):** the pin moved from 0.21.0 to **0.23.3**, the newest OSO release, not to 0.22.0 as §0 item 1 and §9 proposed (OD-3, decided 2026-10-07). §12 is the evaluation of 0.23.3 and the record of the move. Sections 0–11 are kept as written on 2026-09-28; where §12 changes one of their statements, §12 says so.

## 0. Decision summary

1. **Stay on the 0.21.0 pin until OD-3 is recorded, then move to 0.22.0 (proposed child row PROD-00.3f).** 0.22.0 is one squash commit over 0.21.0. It does not change the segment format, the three subcommands Logweir runs (`backup`, `restore`, `validate-restore`), or any key Logweir renders. On the compose stack it passes the demo drill, G-PITR, the receipt path and the full CI e2e command (64 of 64), and both verifiers accept its evidence (§4, §5.4). Run 36542777892 recorded one red row: 0.22.0's full row, which since the merge also runs PROD-01.1's record-semantics suite. The broker's time retention deleted one of those rows' source records before the capture. The pin does the same under the same conditions, so this is a fixture race, not the engine (§4.4). 00.3f carries it as precondition P-3f-1. The fixture fix is on this branch (§4.5). Two of 0.22.0's changes need Logweir work before a bump:
   - its `path_style` fix does **not** lift ENGINE-PATHSTYLE, because a custom endpoint still forces path-style;
   - it newly derives plaintext HTTP from an `http://` endpoint even when `allow_http: false` is rendered.
2. **Every capability gap has a route** (§3). The engine's Kafka and archive correctness defects go **upstream first**; this is bug-class work that `docs/OSO_Feature_Gate_PRD.md` does not gate. Each one is carried as a patch on the PROD-00.2 source build when upstream declines or stalls. The engine's gated seams go to a **patch or a Logweir-native path**, never to an upstream feature PR: the programmatic record filter, and SASL plugins for OAUTHBEARER and MSK IAM. Offsets, ACLs, verification and transport safety stay **Logweir-native**. MSK IAM, the engine's own evidence reports and its continuous/offset-store modes are **declared unsupported** for now.
3. **One patch serves three dependents.** Exposing the engine's existing Keep/Drop/Tombstone record filter in YAML, with rules keyed by partition, offset range and record key, gives:
   - PROD-09.3 its erasure ledger;
   - PROD-11.1 offset-range restore;
   - PROD-07.3 a resume point.

   That makes it the cheapest route to all three (C10/C11).
4. **`engine-matrix` is repaired** (commits `d5d0be9b` and `ff9aa14a`). It failed all three scheduled runs for seven independent reasons (§5.1). It now declares six rows, each with an expected outcome, and is green only when every row records what it declares. Each row's outcome is derived from what its steps did, and the broker is read back from the running container. Run 36531786341 was green on all six rows at `7e4cd0b1`. Run 36542777892 ran at `4861e03a`, after the merge of PROD-01.5 (§5.5). It recorded five rows as declared, and the v0.22.0 row as `fail(e2e suite)` from §4.4's fixture race. A failed suite's row now names what the broker's time retention deleted (§5.6).
5. **The support docs were wrong about the operators.** `strimzi-backup-operator` has defaulted to engine v0.22.0 since its v0.3.0 (2026-09-07). `kafka-backup-operator` 1.3.0 links `kafka-backup-core` 0.19.2 as a library. [support-matrix.md](../../support-matrix.md) and [stability.md](../../stability.md) now say so (§6).
6. **OSO-operator archives:**
   - 0.21 and 0.22 archives drill fully;
   - 0.19.x and 0.20.x archives drill as `outcome: fail-integrity` with `integrity.result: partial`, because they carry no segment digests. This was run on a 0.19.2 archive, and both readers accept the signed result. For 0.19.0, 0.19.1 and 0.20.x it is inferred: their manifests carry no digest (CI run 36531786341's seeds, for v0.19.1 and v0.20.0), and the phase-7 lane treats a missing digest as unverified;
   - any archive with a non-empty consumer-group snapshot fails the drill until FX-1;
   - none can be imported into the catalog, because none carries a Logweir receipt. This is a reading of Logweir's source; A-OSO-3 is the row that will measure it.

   §7 states exactly what remains after FX-1, as acceptance rows.
7. **Two defects sit in the protocol crate the engine uses** (`kafka-protocol` 0.18), below anything Logweir's drill can see:
   - a repeated header key keeps only its last value (C13);
   - a LogAppendTime batch is archived with the producer's timestamps rather than the append times consumers read (C14).

   PROD-01.1 has since measured both (its record §2.3 and §2.4; FX-8 rails the second), and the ledger carries them as PROD-00.3c and 00.3e. C14 is a small engine fix; C13 needs a `kafka-protocol-rs` change.

## 1. Evidence base

| Item | Identity |
|---|---|
| Pinned engine source | `third_party/kafka-backup-v0.21.0.tar.gz`, sha256 `0252a83735148331c16d7c4e737a41f099c0f52eda5d7a66db75b8848ddc405b`, tag `v0.21.0` = commit `ae5a102f93b5270927d95d4ccec184b577febb10` |
| Upstream current | tag `v0.22.0` = commit `cc10aa4ada2ab11fcd8679c01aac13d7b5139949` (2026-09-07), tarball `https://github.com/osodevops/kafka-backup/archive/refs/tags/v0.22.0.tar.gz` sha256 `f62e8bc44538635834aa372f20ec8b313aee6c10e8c53e95118ea13425bd2ae1`; no newer tag on 2026-09-28 |
| Delta | one squash commit, `release: 0.22.0 — skip-missing-topics, describe/validate --config, storage config fixes, offset-storage key cleanup (#186)`, 27 files, +1272/−130 |
| Images (linux/amd64, pulled 2026-09-28) | v0.22.0 `sha256:1c3432c9399dbdd59fea7b6cf386135656a73bb6dd60841212ddbfe5ba26b1fe`; v0.21.0 `sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317` (the pin); v0.20.0 `sha256:a5676ded4a899a8e2a8a8dad250036623a1d6fbc04a9f712dcb8268aec2ba2ac`; v0.19.2 `sha256:fd333b6cdeca3a77da6478f4b278ae5779f68c80078196865354263610f45786`; v0.19.1 `sha256:0761485d1e0f8aeb9cc74bb35b4506128d5d2ec403cc8cede6a8d1ecc52579b9`. Every image's `org.opencontainers.image.revision` equals its tag's commit |
| Protocol crate | `kafka-protocol` 0.18.0 (the engine's `Cargo.lock`), crate sha256 `099d5c2f1b40cd830cbf18ca4d2a0805f2875b811ca18f0372b2433e68fe2dda` |
| Broker probe | `apache/kafka:4.3.1` (the `latest` tag on 2026-09-28), `kafka-broker-api-versions.sh` against a private container |
| Operators | `strimzi-backup-operator` HEAD `cf5b1ecf` (2026-09-07), tags v0.2.21–v0.3.1; `kafka-backup-operator` HEAD `b287418f` = v1.3.0 (2026-08-30) |

**Citation form.** `C/<path>:<line>` is `crates/kafka-backup-core/src/<path>` in the pinned tarball of this record's date, v0.21.0 (since PROD-00.3f the tree vendors v0.23.3 instead; §12 cites that tarball as `C23/`). A file 0.22.0 did not touch has the same line in v0.22.0. For the four touched files that are cited below (`backup/engine.rs`, `config.rs`, `manifest.rs`, `storage/s3.rs`), the v0.22.0 line follows `→`. `KP/records.rs` is `src/records.rs` in `kafka-protocol` 0.18.0. `L/<path>` is this repository at `adee0a16`.

The run artifacts are under `/tmp/logweir-roadmap-run/claude/artifacts/prod-00-1/` on the worker host: the upstream diff, image digests, the 4.3.1 probe, the engine-matrix run logs and every compose cycle.

## 2. The supplier-policy constraint, stated once

`docs/OSO_Feature_Gate_PRD.md` is byte-identical in v0.21.0 and v0.22.0. Its review checklist (Part 4) rejects an open-source PR only when the PR adds a feature listed in Part 2. That list includes:

- client-side encryption, key management, RBAC, SSO/OIDC and secrets managers;
- audit trails and compliance reporting;
- GDPR erasure, **data masking / PII redaction** and crypto-shredding;
- **automatic offset reset** and rollback;
- Schema Registry backup, restore and ID remapping;
- advanced metrics and **backup validation test runs**;
- versioned plugins.

A bug fix is not a feature under that checklist. Two seams are the exception. The source says the record filter is "Set by code — e.g. an embedding application or a commercial distribution — never from YAML" (`C/config.rs:956-961` → `:986-991`). The SASL plugin factory is "Not YAML-configurable" (`C/config.rs:243-258` → `:249-264`). Those are the supplier's commercial extension points, so a PR exposing either from YAML is treated here as likely refused, even where the listed feature is not named.

The gate is not applied mechanically, which cuts both ways. Outside PR #149, "automatically commit consumer group offset resets at the end of restore", was merged into the open-source engine on 2026-08-18, although Part 2 lists automatic offset reset. Rule used below: **upstream PR only for defects; a gated seam goes to a patch or a native path.**

## 3. The capability table

Routes: **U** upstream PR (bug-class only), carried as a patch on the PROD-00.2 build until released; **F** a maintained patch on the PROD-00.2 source build (the MIT fork); **N** Logweir-native; **X** declared unsupported. "Same in 0.22.0" means the cited code is unchanged in v0.22.0.

| ID | Capability | Current behaviour (0.21.0; same in 0.22.0 unless noted) | Proposed route | Cost | Policy constraint | Dependents |
|---|---|---|---|---|---|---|
| C1 | Control records and READ_COMMITTED | Fetch and ListOffsets use READ_UNCOMMITTED; commit/abort markers and aborted records are archived as ordinary records; no transaction fields archived | **U** (drop control batches; READ_COMMITTED with aborted-transaction filtering and an LSO end), N rail until released | ~2–4 days + PROD-01.1 fixture | none (defect) | PROD-00.3a, FX-6, PROD-01.1, 01.1a, 08.1, 02.2, 02.3, 12.1 |
| C2 | Offset-after-upload ordering | In offset-store mode the offset checkpoint covers records still buffered or in an in-flight upload | **U**; **X** for Logweir until released (Logweir renders neither mode) | ~1–2 days | none (defect) | PROD-02.2, 02.3 |
| C3 | Conditional manifest publication | `{backup_id}/manifest.json` is get-merge-put with an unconditional PUT; an unparseable manifest is overwritten | **N** now (FX-7), **U** when PROD-02.3 needs the engine writer | ~1–2 days upstream; FX-7 as scoped | none (defect) | FX-7, PROD-02.3, 09.1, 09.2 |
| C4 | Restore checkpoint cadence and hash scope | Checkpoint saved once per topic; `restore.checkpoint_interval_secs` parsed, never read; hash covers every option, including Logweir's per-run paths; shutdown seen only between topics | **N** (stable per-execution paths) + **U** (per-segment cadence honouring the key; path-free hash) | N ~1 day; U ~2 days | none (defect: a documented key does nothing) | PROD-07.1, 07.2, 07.3 |
| C5 | Idempotent produce | Restore produces with no producer id and retries connection errors (up to 5 + 1) with no idempotence, so a lost acknowledgement duplicates a batch | **U** (InitProducerId + per-partition sequences), N duplicate detection meanwhile | ~3–5 days | none (defect) | PROD-00.3d, 07.1, 07.3, 01.1, 08.1 |
| C6 | Min/max segment timestamps | Segment bounds are the first and last record timestamps, used as min/max by every selector | **U** (additive `min_timestamp`/`max_timestamp`, selectors prefer them), N exact filter in PROD-08.1 | ~1–2 days | none (defect) | PROD-00.3b, FX-6, PROD-01.1, 01.1b, 08.1, 11.1 |
| C7 | Topic IDs | None anywhere; Metadata is sent at v9, which predates topic IDs (v10+) | **N** first (PROD-01.4 heuristic + nullable field); **F** capture via Metadata v10+ only if PROD-01.4 picks the engine | F ~2 days | a feature, so not an upstream PR under this row's rule | PROD-01.4, 02.1, 04.1, 04.2, 07.1, 11.1, 15.1 |
| C8 | ApiVersions negotiation | Never sends ApiVersions; fixed versions per key, `_ => 0` for the rest. Every version it sends lies inside Kafka 4.3.1's ranges; DescribeConfigs v1 and DescribeGroups v0 sit at 4.3.1's floor | **X** today (not needed on 4.3); **U** when a broker line raises a floor; the matrix broker row is the tripwire | U ~2 days | none (robustness defect) | PROD-01.5, 01.2, 04.0 |
| C9 | OAUTHBEARER and MSK IAM | YAML offers PLAIN, SCRAM-SHA-256/512 and GSSAPI; other mechanisms only through a programmatic plugin factory | **F** for OAUTHBEARER (static token file and OIDC client credentials); **X** for MSK IAM until OD-4 | F ~3–4 days; MSK IAM +3 days and a SigV4 dependency | seam marked "not YAML-configurable"; SSO/OIDC and secrets managers are Part 2 | PROD-01.3, 01.2 |
| C10 | YAML filter or transform action | Keep/Drop/Tombstone filter exists; settable only by embedding code (`#[serde(skip)]`); no transform action | **F** filter rules in YAML (Keep/Drop/Tombstone by partition, offset range, key); **N** for any transform (masking) | F ~3 days; N producer path is PROD-11.2's | masking and GDPR erasure are Part 2; the seam is "for a commercial distribution" | PROD-09.3, 11.2, 11.1, 07.3 |
| C11 | Offset-range restore | Time window and source partitions only | **F** through C10's rule set (Drop outside `[start, end)` per partition) | +~1 day on C10 | none directly; rides the C10 seam | PROD-11.1, 07.3, 04.2 |
| C12 | Byte-rate limits | `rate_limit_bytes_per_sec` parsed and checked to be non-zero, never enforced; per-partition records/sec enforced on restore; no backup-side limit | **U** (enforce the documented key); N exposes only records/sec until then | ~1 day | none (defect) | PROD-10.1 |
| C13 | Duplicate header keys (found here) | `kafka-protocol` 0.18 decodes headers into an `IndexMap`, so a repeated key keeps only its last value, on capture and again on produce | **U** to `kafka-protocol-rs` (header list) + engine bump; **X** disclosed until then | ~2–3 days upstream, crate API change | none | PROD-00.3e, PROD-01.1, 08.1, 08.3 |
| C14 | LogAppendTime timestamps and producer metadata (found here) | For a LogAppendTime batch the decoder ignores the batch's max timestamp, so the archive keeps the producer's timestamps, not the append times consumers read; timestamp type, producer id/epoch, sequence and the transactional flag are dropped; restore always produces CreateTime | **U** to `kafka-backup` for LogAppendTime (the engine parses batch headers itself); **X** disclosed for producer metadata | ~1 day | none (defect) | PROD-00.3c, PROD-01.1, FX-6, FX-8, 08.1, 11.1 |
| C15 | Transport derived from the endpoint (0.22.0) | 0.22.0 treats an `http://` endpoint as `allow_http: true` even when `false` is rendered | **N**: refuse in `render_storage_block` (all three engine documents) before the bump (PROD-00.3f) | ~0.5 day | none | PROD-00.3f, PLAT-08 seam S5 |
| C16 | `path_style` (ENGINE-PATHSTYLE) | 0.21.0 ignores `path_style`; 0.22.0 honours `path_style: true`, but any custom endpoint still forces path-style | **N** keep the refusal of VirtualHosted plus endpoint; U only if demanded | — | none | PLAT-08.1, PROD-09.2 |
| C17 | Engine evidence reports | `checksums_valid: true` is set unconditionally | **X** never consumed; verification stays N (PROD-08) | — | validation runs are Part 2 | PROD-08.x |
| C18 | Consumer-group snapshot | Written as `snapshot_time` + topic → partition → offset; drops groups without offsets on archived topics; Logweir's vendored shape differs (FX-1) | **N** (FX-1 parses it as an import source; PROD-04.1 captures natively) | FX-1 as scoped | automatic offset reset is Part 2 | FX-1, PROD-04.1, 04.2 |
| C19 | Build and architecture | Logweir copies OSO's amd64-only image binary (GR6) | **F** build from the vendored source (PROD-00.2) | PROD-00.2 as scoped | MIT permits it; GR6 amendment | PROD-00.2, every F row |
| C20 | Capture of a topic that retention emptied (found by run 36542777892) | Each partition starts at its earliest offset. A partition whose earliest offset has reached its high watermark is skipped with a debug-level line and no segment. With every partition empty, the engine writes no segment and exits 0. The code is identical in 0.21.0 and 0.22.0 | **None upstream** (intended: an empty source is not an engine error); **N** exists: Logweir refuses the run, and nothing is signed | — | none | PROD-01.1 and G-PITR fixtures (A-C20-2), engine-matrix (§5.6) |

### 3.1 C1 — Control records and READ_COMMITTED

**Behaviour.**

- Fetch requests set `isolation_level(0)`, READ_UNCOMMITTED (`C/kafka/fetch.rs:53`). So do the two ListOffsets requests (`:263`, `:337`), so a capture ends at the high watermark, not at the last stable offset.
- `decode_fetch_data` pushes every decoded record at or above the fetch offset (`:148-194`, push at `:182`). It never reads the batch's control or transactional flags. The FetchResponse `aborted_transactions` list is never read (no reference in the crate).
- `kafka-protocol` 0.18 keeps control records and flags them (`KP/records.rs:155-184`, `control` at `:160`), so commit and abort markers reach the archive.
- `convert_record` keeps only key, value, headers, timestamp and offset (`C/kafka/fetch.rs:200-218`). `BackupRecord` has no transaction fields (`C/manifest.rs:406` → `:413`).

**Effect** (read from source, then measured by PROD-01.1, whose record, `docs/to-do/decisions/PROD-01.1-record-semantics.md` §2.1, found aborted records, open transactions and every marker archived as ordinary records). A transactional source restores with its aborted records, and with its markers as ordinary records. A marker has a 4-byte key and a 6-byte value. The drill compares target with archive, so it cannot see either. The archive keeps no flag, so detection from the archive alone is a shape heuristic: a key of `00 00 00 00` or `00 00 00 01` with a 6-byte value. A user record can have that shape, so the heuristic has false positives. PROD-01.1 owns the rail.

**Route.** U: the correction is small and uncontroversial.

1. Skip control batches.
2. Offer `isolation_level: read_committed` on fetch and ListOffsets.
3. Drop aborted records using the response's `aborted_transactions` and the batch producer ids, the standard consumer algorithm.

Until it is released, PROD-01.1's rail applies (refuse or label transactional topics), with a fork patch if upstream declines. A fully native capture path is not proposed: it re-implements the engine's writer.

**Acceptance rows.** The oracle is PROD-01.1's (`docs/to-do/decisions/PROD-01.1-record-semantics.md` §9); this record adds no second one.

- **A-C1-1 (PROD-00.3a; the ledger row's oracle).** PROD-01.1 §9, "PROD-00.3a — Committed-only capture", run by `transactional_topic_committed_input_versus_restored_output` (the TXN row) in `e2e/tests/record_semantics.rs`.
  - Pass: capture, replay and end-to-end divergence sets all empty; the archive holds no control-shaped record; the records of the transaction open at capture (D) are absent.
  - Negative control: a build that fetches `READ_COMMITTED` but keeps control batches still reports seven `extra:control-marker` divergences.
  - Fixture: the TXN row (transactions A–D).
  - Additional pass predicate, not an oracle: an archive written before the change still reads as transactional to PROD-01.1a's detection, never as committed-only, and both verifiers accept the manifest's new capture field or reject it clearly (rule 3).
- **Until 00.3a lands,** the rail is PROD-01.1a's (refuse by default, detect, label an approved override). Its acceptance rows are in PROD-01.1 §9.
- **Dependent tasks** take their rows from PROD-01.1 §7: 02-1 and 02-5 (capture), 04-1 (positions), 07-3 (resume) and 08-5 (evidence).

### 3.2 C2 — Offset-after-upload ordering

**Behaviour.**

- In the per-partition loop, a sealed segment is uploaded by a spawned task (`C/backup/engine.rs:1311-1329` → `:1367-1385`).
- The offset store is then set to the end of the fetched batch (`:1336-1343` → `:1392-1399`), which includes records still buffered in the writer and records in the in-flight upload.
- `sync_if_due` then publishes `offsets.db`.
- The store exists only in continuous mode or with `offset_storage` configured (`:1802` → `:1881`).

**Effect.** A crash after the sync and before the upload completes leaves a checkpoint past the durable data. The next run resumes after a hole it cannot see. Logweir renders `continuous: false` and no `offset_storage` (`L/crates/logweir-engine-oso/src/render_backup.rs:210-213`), so shipped backups are not exposed. PROD-02.2 already refuses the mode.

**Route.** U (move the `set_offset` after the flush that covers it). **X** for Logweir until released: Logweir never renders the mode. PROD-02.3 decides whether continuous capture uses the engine at all.

**Acceptance rows.**

- **A-C2-1 (PROD-02.2).** `render_backup` never emits `continuous: true` or an `offset_storage` block. A unit test pins it, and a mutant emitting either turns it red.
- **A-C2-2 (PROD-02.3).** If the engine's continuous mode is chosen: kill the engine between the offset sync and the segment upload (fault injection on the storage PUT). Pass: the next run re-reads the unarchived range, and the archive has no gap versus the source. Negative control: the unpatched engine leaves the gap.

### 3.3 C3 — Conditional manifest publication

**Behaviour.**

- `save_manifest_snapshot` GETs the manifest, merges and PUTs it unconditionally (`C/backup/engine.rs:1594-1617` → `:1650-1673`).
- When the existing manifest does not parse, it logs and overwrites it (`:1606` → `:1662`).
- The storage trait has no conditional put (`C/storage/backend.rs:21-29`), although `object_store` 0.14.1 supports `PutMode::Create` and `PutMode::Update`.

**Effect.** Two writers under one `backup_id` lose updates. A rewrite invalidates earlier signed points (FX-7). A damaged manifest is replaced silently.

**Route.** **N** now: FX-7 pins the manifest version, or refuses a second run under an existing `backup_id`, and PLAT-16's retention treats manifests as immutable per point. **U** when PROD-02.3 needs the engine's writer: an ETag-conditioned PUT with a bounded retry, and refusal instead of overwrite on a parse error.

**Acceptance rows.**

- **A-C3-1 (FX-7).** A second `logweir backup run` under an existing `backup_id` either pins and reads the first point's manifest by version, so the first point's receipt still verifies, or is refused before the engine starts. Negative control: without the fix, the first receipt's `manifest_sha256` no longer matches the object.
- **A-C3-2 (PROD-02.3).** Two engine writers race on one `backup_id` against a store with conditional writes. Pass: exactly one PUT per generation wins and the loser re-merges, so no segment entry is lost. Negative control: the unpatched engine loses one writer's entries.
- **A-C3-3 (PROD-09.1).** A manifest that fails to parse is reported and never overwritten. Pass: the object's version id is unchanged after a backup attempt. Negative control: the unpatched engine rewrites it.

### 3.4 C4 — Restore checkpoint cadence and hash scope

**Behaviour.**

- The checkpoint is loaded or seeded against a hash (`C/restore/engine.rs:736-759`) and saved only after each topic (`:926-929`).
- Shutdown is checked only between topics (`:899`).
- `restore.checkpoint_interval_secs` is parsed (`C/config.rs:845-846` → `:875-876`) and never read under `restore/`.
- The hash is sha256 over the serde encoding of the whole `RestoreOptions` (`C/restore/engine.rs:2093-2102`), including `checkpoint_state` and `offset_report` (`C/config.rs:842`, `:858` → `:872`, `:888`).
- Logweir renders both paths under the run id: `/var/lib/logweir/<run>/checkpoint.json` and `…/offsets.json` (`L/crates/logweir-engine-oso/tests/snapshots/render__restore_yaml.snap`). So every attempt's hash differs and a restore never resumes.
- Logweir also renders `checkpoint_interval_secs: 30` (`L/crates/logweir-engine-oso/src/render_restore.rs:147`), a key the engine never reads.
- Segments skipped as already completed add nothing to the offset report (`C/restore/engine.rs:1727-1731`).

**Route.** **N**: render per-execution paths, identical across attempts of one execution, and carry the checkpoint file between attempts (it is pod-local today; PROD-07.1 decides where it lives). With both, the engine's own per-topic checkpoint resumes a restarted execution, and PROD-07.3 reconciles the partial topic from `x-original-offset`. **U**: honour `checkpoint_interval_secs` (per-segment saves), hash the options without file paths, and re-add skipped segments' mappings from the checkpoint. **X**: the shutdown granularity until Logweir propagates cancellation (Later #13).

**Acceptance rows.**

- **A-C4-1 (PROD-07.1).** Two attempts of one execution render byte-identical restore documents. A test diffs the two renders, and a mutant that re-inserts the run id turns it red.
- **A-C4-2 (PROD-07.3).** Kill the runner after topic 1 of 2 completes. Pass: the resumed attempt skips topic 1 (the loaded checkpoint lists its segments), and the target holds each source record of topic 1 exactly once. Negative control: with per-attempt paths the resumed attempt re-produces topic 1, and the duplicate count equals topic 1's record count.
- **A-C4-3 (PROD-07.1).** The rendered document never carries a key the engine does not read. `checkpoint_interval_secs` is removed from the render until C4's U part is released, or kept and labelled as having no effect.
- **A-C4-4 (PROD-00.3g).** With the released engine, killing mid-topic loses at most one segment of progress per partition. A fault-injected kill after the N-th segment PUT shows the checkpoint listing N segments. The 0.21.0 pin lists 0.

### 3.5 C5 — Idempotent produce

**Behaviour.**

- Records are built with `transactional: false`, `producer_id: NO_PRODUCER_ID` and a batch-local sequence (`C/kafka/produce.rs:79-110`).
- `produce` sends Produce v8 with the configured acks, default −1 (`:118-190`; `C/config.rs:1052-1054`).
- The router retries connection errors up to five times with linear back-off (`C/kafka/partition_router.rs:500-552`). The client reconnects and retries once more (`C/kafka/client.rs:459-492`).
- There is no InitProducerId anywhere.

**Effect** (measured by PROD-01.1 §5.1: 3,000 duplicates after three resent requests, with the engine exiting 0). A produce whose acknowledgement is lost after the broker appended it is sent again and appended twice. The drill compares by `x-original-offset` and collapses duplicates (PROD-08.1's gap), so it cannot see this.

**Route.** **U**: InitProducerId, then per-partition sequence numbers and epoch handling. The protocol crate already models both, and 4.3.1 serves InitProducerId v0–v5. Meanwhile **N**: PROD-08.1 detects duplicates on `x-original-offset`, and PROD-07.1's contract labels the replay ambiguity window. A native producer is not proposed for this alone.

**Acceptance rows.** The oracle is PROD-01.1's.

- **A-C5-1 (PROD-00.3d; the ledger row's oracle).** PROD-01.1 §9, "PROD-00.3d — Idempotent restore produce", run by its ack-fault row.
  - Pass: no `duplicate` divergence, with the broker frozen past the engine's response timeout.
  - Negative control: the pinned engine's §5.1 result (duplicates after resent requests).
  - Fixture: the ack-fault row. PROD-01.1 §7's 07-1 describes the deterministic fault proxy that makes it repeatable; the row reproduced duplicates in one of five samples.
- **Dependent tasks** take their rows from PROD-01.1 §7: 04-4 (positions over duplicates), 07-1 (the resume bound) and 08-4 (duplicate detection).

### 3.6 C6 — Min/max segment timestamps

**Behaviour.** The writer sets `start_timestamp` from the first record and `end_timestamp` from the last (`C/segment/writer.rs:236-242`). `SegmentMetadata::overlaps_time_window` treats them as min and max (`C/manifest.rs:391-401` → `:398-408`). Every restore selection uses it: restore and dry run (`C/restore/engine.rs:547`, `:1961`), the header preflight (`C/restore/preflight.rs:263`) and repartitioning (`C/restore/repartition.rs:300`).

**Effect** (measured by PROD-01.1 §2.2). With non-monotonic CreateTime, a record inside the window can sit in a segment whose first and last timestamps are both outside it. That segment is skipped silently, which is FX-6's second hazard.

**Route.** **U**: record `min_timestamp` and `max_timestamp` additively, and have the selectors prefer them when present. Existing archives keep first/last and stay correct to read. **N**: PROD-08.1 already plans exact per-record filtering on the archive side. **X**: archives written before the fix remain first/last-bounded, and their evidence must say so.

**Acceptance rows.** The oracle is PROD-01.1's.

- **A-C6-1 (PROD-00.3b; the ledger row's oracle).** PROD-01.1 §9, "PROD-00.3b — Segment min/max record timestamps", run by `non_monotonic_create_time_skipped_at_the_point_in_time` (ts-pit).
  - Pass: ts-pit's replay set becomes empty (p0@5 restored); ts-floor and ts-bound are unchanged until PROD-01.1b.
  - Negative control: the pinned engine's archive.
  - Additional pass predicate, not an oracle: a manifest written before the change, with no min/max fields, selects exactly as today (first/last), and both verifiers accept both shapes.
- **A-C6-2 (PROD-11.1).** Preview and execution select the same records for an inclusive window over non-monotonic timestamps, and the preview names the rule it used (first/last or min/max).
  - Negative control: on ts-pit, a preview computed from record timestamps disagrees with an engine that selects by first/last.
  - Fixture: ts-pit.
- **Other dependent tasks** take their rows from PROD-01.1 §7: 02-2 (coverage bounds), 07-2 (resume by offset) and 08-1 to 08-3 (complete mode). PROD-01.1b rails selection until 00.3b lands.

### 3.7 C7 — Topic IDs

**Behaviour.** No engine code reads or records a topic ID. Metadata is requested at v9 (`C/kafka/client.rs:591`), which carries none. The manifest has no field for one.

**Route.** **N** first. PROD-01.4 decides the generation contract: an offset-regression heuristic now, and a nullable `topic_id`. **F** only if PROD-01.4 takes the engine route: raise Metadata to v10+ (4.3.1 serves 0–13) and write `topic_id` into the manifest additively. An upstream PR is not proposed, because it is a feature under this row's rule. Kafka 4.3.1 also serves `DescribeTopicPartitions` v0.

**Acceptance rows.**

- **A-C7-1 (PROD-01.4).** Delete and recreate a topic between two backups. Pass: the generation contract flags the second archive as a new generation with the heuristic, and with the engine-recorded `topic_id` if F is taken. Negative control: a plain continued write is not flagged.
- **A-C7-2 (PROD-02.1, 04.1, 04.2, 07.1, 11.1, 15.1).** Each consumer's reaction to a generation change is the one PROD-01.4 records. Each gets one negative control where the change is absent.

### 3.8 C8 — ApiVersions negotiation

**Behaviour.**

- `get_api_version` returns a fixed version per key, and `_ => 0` for any key not listed (`C/kafka/client.rs:587-611`).
- ApiVersions is listed at v3 but never sent.
- DescribeGroups, used by the consumer-group snapshot (`C/kafka/consumer_groups.rs:153`), goes out as v0.
- Measured against `apache/kafka:4.3.1` (artifact `kafka-4.3.1-api-versions.txt`), every version the engine sends is inside the broker's range:
  - Metadata 9 (0–13), Fetch 11 (4–18), Produce 8 (0–13), ListOffsets 5 (1–11), CreateTopics 5 (2–7);
  - DescribeConfigs 1 (1–4), IncrementalAlterConfigs 1 (0–1), DeleteRecords 1 (0–2);
  - FindCoordinator 2 (0–6), OffsetFetch 5 (1–10), OffsetCommit 5 (2–10), ListGroups 2 (0–5), DescribeGroups 0 (0–6);
  - SaslHandshake 1 (0–1), SaslAuthenticate 2 (0–2).
- DescribeAcls, CreateAcls and DeleteAcls appear only in the version table (`C/kafka/client.rs:606-608`). No `send_request` sends them anywhere in the crate, so a broker's ACL floor cannot affect the engine.

**Effect.** No failure on 4.3.1 is predicted from source. Two sent APIs sit exactly at 4.3.1's floor, so a floor raise breaks them:

- DescribeConfigs v1 (`C/kafka/admin.rs:472`, range 1–4), used to capture topic configurations;
- DescribeGroups v0 (range 0–6), used only by the consumer-group snapshot.

SaslAuthenticate v2 and IncrementalAlterConfigs v1 sit at the ceiling, which a floor raise does not touch. The compose runs on 4.3.1 are in §4.3.

**Route.** **X** now (no gap on supported lines). **U** when a supported broker line raises a floor above a fixed version. The engine-matrix row `v0.21.0 × Kafka 4.3.1` is the weekly tripwire.

**Acceptance rows.**

- **A-C8-1 (PROD-01.5).** The drill, G-PITR and the receipt path pass on each broker line PROD-01.5 adds. Where one fails, the recorded error names the API key and version.
- **A-C8-2 (engine-matrix).** The row `v0.21.0 × 4.3.1` records `pass`. Negative control: a row on a broker image with an API floor above the engine's version records `fail(e2e suite)`, not `pass`.
- **A-C8-3 (PROD-04.0).** Group operations on 4.x record which group types (classic, consumer, share, streams) the engine's ListGroups v2 and DescribeGroups v0 can see. Share and streams groups are reported as not captured.

### 3.9 C9 — OAUTHBEARER and MSK IAM

**Behaviour.**

- The YAML mechanism enum is PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and GSSAPI (`C/config.rs:321-331` → `:327-337`).
- Anything else needs `sasl_mechanism_plugin_factory`, which is `#[serde(skip)]` and documented as "e.g. OAUTHBEARER for MSK IAM … Not YAML-configurable" (`C/config.rs:243-258` → `:249-264`).
- The CLI wires only GSSAPI, behind a cargo feature (`crates/kafka-backup-cli/src/commands/sasl_plugin.rs:1-45`).
- The plugin trait already supports multi-round handshakes and KIP-368 re-authentication (`C/kafka/sasl/plugin.rs:1-60`).

**Route.** **F**: a small CLI patch that builds an OAUTHBEARER plugin from YAML, for a static token file and OIDC client credentials. Logweir renders the token path and never the secret. **X** for MSK IAM until OD-4 funds an MSK account: it needs SigV4 signing (a new dependency) and cannot be verified locally. It is not an upstream PR, because the seam is the supplier's commercial extension point (§2).

**Acceptance rows.**

- **A-C9-1 (PROD-01.3).** A compose OAUTHBEARER listener (PROD-01.5 profile) with an unsecured-JWT validator. Pass: backup, restore and verify pass, both clients authenticate, and the receipt's `auth_mode` names the mode in both verifiers. Negative controls: a wrong token fails with an authentication error, never a PLAINTEXT dial; the token never appears in status, logs or downloads.
- **A-C9-2 (PROD-01.2).** Until OD-4 evidence exists, the support matrix lists MSK IAM as `unsupported` and Confluent Cloud / Event Hubs OAUTHBEARER as `untested`.

### 3.10 C10 — A YAML-exposed filter or transform action

**Behaviour.**

- A per-record hook returns Keep, Drop or Tombstone and runs on every restore path after time-window filtering (`C/restore/filter.rs`).
- It is set only by code (`C/config.rs:956-961` → `:986-991`), and its fingerprint joins the checkpoint hash (`:963-968` → `:993-998`).
- There is no transform action, and manifests count dropped and tombstoned records since 0.21.

**Route.**

- **F**: a CLI patch that builds the existing hook from a YAML rule list. A rule matches on topic, partition, offset range and exact key bytes, and returns Keep, Drop or Tombstone. Logweir renders the rules and binds their digest into the approved plan. This keeps the patch inside the engine's existing semantics, which minimises fork divergence.
- **N**: any transform (masking), through a Logweir-native producer that decodes `.kbak` with Logweir's own decoder (PROD-11.2), because a transform action would be a deep fork of a Part-2 feature.
- Not an upstream PR (§2).

**Acceptance rows.**

- **A-C10-1 (PROD-09.3).** An erasure-ledger rule set drops (or tombstones) every record of subject key K across two topics during restore. Pass: a full scan of the target finds no K record, and the scorecard records the rule digest and the dropped count. Negative control: the same restore without the rules restores K.
- **A-C10-2 (PROD-11.2).** The masking path never uses the engine hook for value changes. A test proves the engine's rendered document carries no transform rule. The native producer's output passes the declared-schema check.

### 3.11 C11 — Offset-range restore

**Behaviour.** `RestoreOptions` selects by `time_window_start`/`time_window_end` and `source_partitions` only (`C/config.rs:751-762` → `:781-792`). No offset bound exists.

**Route.** **F** through C10: per-partition `Drop` for offsets outside `[start, end)`. Selection efficiency (skipping whole segments by offset) is a follow-up inside the same patch. It also gives PROD-07.3 a resume point: Drop below the target tail's last `x-original-offset`.

**Acceptance rows.**

- **A-C11-1 (PROD-11.1).** Restoring partition 1 of `orders` over offsets [100, 200) yields exactly those 100 records, in order, with their original offsets in `x-original-offset`, and preview and execution agree. Negative control: an off-by-one bound, [100, 201), is caught by the count check.
- **A-C11-2 (PROD-07.3).** A resumed restore with a Drop-below-tail rule leaves each source record on the target exactly once. Negative control: without the rule, the duplicates are counted.

### 3.12 C12 — Byte-rate limits

**Behaviour.**

- `rate_limit_bytes_per_sec` is parsed (`C/config.rs:807-809` → `:837-839`). Only `validate()` reads it, to reject 0 (`:1224-1230` → `:1290-1296`); nothing enforces it.
- `rate_limit_records_per_sec` is enforced per partition on restore (`C/restore/engine.rs:1816`, `C/restore/repartition.rs:420`).
- The backup path has no limit.

**Route.** **U**: enforce the documented key, a defect because a documented control does nothing. **N**: PROD-10.1 exposes only records/sec, with CEL bounds, until the byte limit is released.

**Acceptance rows.**

- **A-C12-1 (PROD-00.3h, PROD-10.1).** The claim rests on a ratio measured in one environment (emulated or native), never on a duration, because §10 says durations here are not evidence.
  - Setup: first measure the pinned engine's restore throughput T on the host. Choose a limit L at most T/3.
  - Pass: with `rate_limit_bytes_per_sec: L`, the restore's measured throughput (bytes produced over the restore-only time the scorecard records) is at most 1.2 L.
  - Negative control: the same restore on the 0.21.0 pin measures at least 3 L, which proves the key is ignored today.
  - Logweir renders the key only when the engine version honours it.

### 3.13 C13 — Duplicate header keys (found here)

**Behaviour.**

- `kafka-protocol` 0.18 stores record headers as `IndexMap<StrBytes, Option<Bytes>>` (`KP/records.rs:184`) and fills it with `insert` (`:896-919`).
- A repeated key keeps its first position and its last value, so `[(a,1),(b,2),(a,3)]` decodes as `[(a,3),(b,2)]`.
- The engine's own `BackupRecord.headers` is a `Vec` and could hold duplicates, but the decoder has already collapsed them. The produce path rebuilds an `IndexMap` too (`C/kafka/produce.rs:84-90`).

**Effect** (measured by PROD-01.1, §2.4 of its record).

- **Capture side, at p0@4:** a header multiset is not preserved. The repeated key kept its first position and its last value. The drill cannot see this, because target and archive are both collapsed.
- **Replay side, at p0@6:** a record that already carried `x-original-offset` loses it on restore. The drill does see this: exit 2, `fail-integrity`, 14 of 15 sampled records matching.

**Route.** **U** to `kafka-protocol-rs` (a header list in `Record`, a breaking change for that crate), then an engine bump. **X** disclosed until then (PROD-01.1's contract). Logweir's own reader (librdkafka) preserves duplicates, so a source-versus-target comparison can detect the loss.

**Acceptance rows.** The oracle is PROD-01.1's.

- **A-C13-1 (PROD-00.3e; the ledger row's oracle).** PROD-01.1 §9, "PROD-00.3e — Keep repeated header keys", run by `keys_nulls_tombstones_and_duplicate_headers` (the shapes row).
  - Pass: the shapes row's capture and replay sets become empty, AND Logweir's verdict on it passes. The verdict needs phase 7 to key the target by the LAST `x-original-offset`; `crates/logweir/src/drill/phase7_verify.rs:288-293` keys by the first today.
  - Negative control: the pinned engine.
- **Dependent tasks** take their rows from PROD-01.1 §7: 04-3 (lineage), 08-6 (header comparison) and 08-7 (full comparison).

### 3.14 C14 — LogAppendTime timestamps and producer metadata (found here)

**Behaviour** (read from source, then measured by PROD-01.1 §2.3: a point in 2001 restored six records the broker appended in 2026, signed `pass`; FX-8 rails it).

- `kafka-protocol` 0.18 reads the batch's timestamp type (`KP/records.rs:572-573`) but decodes the batch's max timestamp into a discarded `_max_timestamp` (`:585`).
- It stamps every record `base + delta` (`:861-862`), whatever the type.
- For a LogAppendTime batch the broker sets only the batch's max timestamp and the type bit; the per-record deltas keep what the producer sent. The source is Apache Kafka 4.3.1 (tag commit `26b251a451ce941d3d7a55e6487bcb7f16b5ad48`), `storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java:248-250` and `:377-381`.
- A Java consumer reports the batch's max timestamp for every record of such a batch (`clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecordBatch.java:580`, `clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecord.java:321-322`).
- So the engine archives the **producer's** timestamps, not the append times consumers read.
- `convert_record` also drops the timestamp type, the producer id/epoch, the sequence and the transactional flag (`C/kafka/fetch.rs:200-218`).
- Restore always produces CreateTime (`C/kafka/produce.rs:101`).

**Effect.** A point-in-time restore of a LogAppendTime topic selects by producer time. With an unsynchronised or deliberately back-dated producer, that can differ from the append time the application and its consumers saw.

**Route.**

- **U** to `kafka-backup`. The engine already parses each batch header by hand (`C/kafka/fetch.rs:156-191`), and the max timestamp (bytes 35–43) and the timestamp-type bit (attributes, bytes 21–23) sit in the same header. It can therefore stamp a LogAppendTime batch's records with the batch max timestamp, as the Kafka client does, without any `kafka-protocol` change (PROD-01.1's review L2). A matching fix to `kafka-protocol-rs`'s decoder is optional hygiene, not a prerequisite.
- **X** disclosed for the producer metadata, revisited with C1 if transactional support needs producer identity.

**Acceptance row.** The oracle is PROD-01.1's.

- **A-C14-1 (PROD-00.3c; the ledger row's oracle).** PROD-01.1 §9, "PROD-00.3c — Keep `LogAppendTime` through capture", run by `log_append_time_source_versus_restored_output` (the LAT row).
  - Pass: the LAT row's capture set becomes empty; the restored timestamps equal the source's append times (end to end keeps only `timestamp-type-changed`); FX-8's refusal no longer triggers for archives written this way.
  - Negative control: the pinned engine.
  - Additional pass predicate, not an oracle: an archive written before the change still triggers FX-8's refusal, because its timestamps are still producer times, and the per-segment timestamp type is additive in both verifiers.
- **Dependent tasks** take their rows from PROD-01.1 §7 (04-5, time-based translation) and from FX-8.

### 3.15 C15 — Transport derived from the endpoint in 0.22.0

**Behaviour.**

- **Only the YAML path derives it.** In 0.22.0, YAML's `allow_http` is a plain `bool` (`storage/config.rs:43-44`), and `storage/mod.rs:58-64,73` ORs it with the endpoint's scheme through `implied_allow_http` (`storage/config.rs:112-118`). YAML therefore cannot tell `allow_http: false` from absent. The URL path honours an explicit `allow_http=false` (`storage/config.rs:159-163`, upstream test at `:308-316`). Logweir renders YAML.
- **Measured by run** (2026-09-29, artifact `c15-allow-http/`). Both engines ran with `--network none` and a restore document with `endpoint: "http://127.0.0.1:1"` and `allow_http: false`:
  - 0.21.0 stops with "Error performing GET http://127.0.0.1:1/… in 14.5 ms - HTTP error: builder error", before any connection attempt.
  - 0.22.0 logs "storage.endpoint uses http://; enabling allow_http (set storage.allow_http: true to make this explicit)", then makes ten "transport error of kind Connect" retries: real plaintext attempts.
- **Every engine document goes through one function.** Logweir renders `allow_http` from the plan's transport alone (`L/crates/logweir-core/src/destination.rs:529-560`), in one function, `render_storage_block` (`L/crates/logweir-engine-oso/src/render_restore.rs:263`). That function renders the storage block of all three engine documents: restore (`:87`), backup (`render_backup.rs:203`) and validate-restore (`render_validation.rs:65`).
- **Today's shielding is ordering, not a rule.** For destinations, rule R3 makes the combination unrepresentable (`L/crates/logweir-core/src/destination.rs:337-357`). A CLI spec has no such rule. `backup run`'s create-only execution claim (`L/crates/logweir/src/backup/mod.rs:550-557`) and the drill's early archive reads go through Logweir's own store first, which refuses plaintext (`L/crates/logweir-store/src/lib.rs:385-411`), so today the engine never starts.

**Route.** **N**, before a bump:

- `render_storage_block` refuses a storage block whose `allow_http` is false and whose endpoint scheme is `http`, with a `RenderError`. No engine document of any kind can then carry the combination.
- A phase-0 spec rule, the same as R3, keeps a clear exit 3 before any work.

**Acceptance row.**

- **A-C15-1 (PROD-00.3f).**
  - Pass: each of the three renderers (restore, backup, validate-restore) returns a `RenderError` for a storage block with `endpoint: http://…` and `allow_http: false`, with one unit test per document. A drill or backup spec with that combination exits 3 at phase 0, before any engine start.
  - Negative control 1: the mutant that deletes the check in `render_storage_block` turns all three render tests red.
  - Negative control 2 (the differential above, repeated on the new pin): with the render check bypassed, the same document makes 0.22.0 log "enabling allow_http" and attempt plaintext connections, where 0.21.0 fails with "builder error". This shows the guard is load-bearing on 0.22.0.
  - Fixture: `c15-allow-http/restore.yaml`.

### 3.16 C16 — `path_style` and ENGINE-PATHSTYLE

**Behaviour.**

- In 0.21.0, a custom endpoint forces `with_virtual_hosted_style_request(false)` and `path_style` is ignored (`C/storage/s3.rs:64-67`).
- In 0.22.0, `use_path_style(endpoint, path_style) = path_style || endpoint.is_some()` (`storage/s3.rs:52-57`, applied at `:78-80`).
- `object_store` 0.14.1 defaults to path-style (`aws/builder.rs:951-953`).
- So in 0.22.0 `path_style: true` without an endpoint is honoured, and it also overrides `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`. Virtual-hosted addressing with a custom endpoint remains impossible.

**Route.** **N**: keep Logweir's refusal (`L/crates/logweir-core/src/destination.rs:462-477`, `L/crates/weirkeeper/src/destination.rs:531-535`, `L/crates/logweir-api/src/routes/destinations.rs:756-763`). On a bump, change its message from "engine 0.21.0" to a version-neutral statement. The research table's "0.22.0 fixes the `path_style` defect behind ENGINE-PATHSTYLE" is wrong for Logweir's case.

**Acceptance row.**

- **A-C16-1 (PROD-00.3f).** On the bumped pin, a destination with `addressing: VirtualHosted` and a custom endpoint is still refused with `addressing_unsupported_by_engine`, and the message names no engine version.

### 3.17 C17 — Engine evidence reports

`C/evidence/emit.rs:109` sets `checksums_valid: true` unconditionally. Logweir never runs `validation run` (Later #4) and never consumes engine evidence. **X**: it stays unconsumed. PROD-08's verification remains Logweir's own, with two independent verifiers.

### 3.18 C18 — Consumer-group snapshot

**Behaviour.**

- `snapshot_consumer_groups` (`C/backup/engine.rs:846-932` → `:902-988`) writes `{backup_id}/consumer-groups-snapshot.json` as `snapshot_time` plus `groups[].offsets` (topic → partition → offset).
- It keeps only groups with offsets on archived topics (`:889-901`).
- It never overwrites an existing snapshot with an empty one.
- Logweir's vendored shape expects `captured_at`, `state` and a list (FX-1).

**Route.** **N**: FX-1 parses the engine's real shape as an import source only. PROD-04.1 captures positions natively. Automatic offset application is Part 2 upstream, and is Logweir's reviewed cutover (PROD-04.2).

**Acceptance rows.** See §7 (A-OSO-2).

### 3.19 C19 — Build and architecture

The runner copies the binary out of OSO's amd64-only image (ruling GR6, `L/scripts/extract-engine.sh`). Every F route above presupposes PROD-00.2: building the vendored source for amd64 and arm64, with the patch queue applied, SBOM and provenance. **Its acceptance is PROD-00.2's own.** One addition: the build applies F patches in a fixed order from a checked-in directory, and each patch has the upstream PR link or the refusal that justifies carrying it.

### 3.20 C20 — Capture of a topic that retention emptied (found by run 36542777892)

- **Behaviour, both versions.** Logweir renders neither `start_offset` nor `stop_at_current_offsets`, so each partition starts at its earliest offset:
  - `StartOffset::Earliest` is the default (`config.rs:680-681` in 0.21.0, `:710-711` in 0.22.0);
  - `stop_at_current_offsets` defaults to `false` (`:476-477` and `:505-506`).

  A partition whose earliest offset has reached its high watermark is skipped: `if start_offset >= end_offset { debug!("… no new data to back up"); return Ok(()) }` (`backup/engine.rs:1191-1199` in 0.21.0, `:1247-1255` in 0.22.0). From offset resolution to that skip, the path is byte-identical in the two versions (61 lines: `:1147-1207` and `:1203-1263`). With every partition skipped, the engine writes no segment and exits 0.
- **What Logweir does.** The backup's readback refuses a set that names no segment for any named topic (`L/crates/logweir/src/backup/phase_run.rs:290-300`). It exits 1 with no receipt, and nothing is signed. Run 36542777892 printed exactly that refusal.
- **Route.** Nothing is needed upstream: skipping an empty range is intended, and backing up an empty topic is not an engine error. A per-topic "captured nothing" warning would be a feature rather than a bug-class fix, so it is not a PR candidate under §2. Logweir's guard is the N route, and it already exists.
- **Acceptance.**
  - **A-C20-1.** A topic that the broker's time retention emptied before the capture is refused by `logweir backup run` with the "captured nothing" message, on the pin and on 0.22.0 alike. Measured in §4.4: three topics, two engines.
  - **A-C20-2 (the fixtures; applied on this branch in `d795bae4`, round 4).** A source topic that holds records stamped older than the broker's retention is created with `retention.ms=-1`, as Logweir creates its restore targets (`L/crates/logweir-kafka/src/reader.rs:349-355`). Pass: the topic survives a retention check, and its capture archives every record (§4.4, the fix verification; §4.5, live). Negative control: the same topic on cluster-default retention is emptied by the check and refused (A-C20-1).

## 4. The 0.22.0 evaluation

### 4.1 What changed, and what it means for Logweir

| 0.22.0 change | Source | Effect on Logweir |
|---|---|---|
| `path_style` honoured; a custom endpoint still implies path-style | `storage/s3.rs:52-57,78-80` | ENGINE-PATHSTYLE stays (C16); only the refusal's wording changes |
| An `http://` endpoint implies `allow_http` | `storage/config.rs:111-118` | New transport derivation; guard before a bump (C15) |
| `access_key_id`/`secret_access_key` accepted as aliases | `storage/config.rs:29-35` | None: Logweir renders credentials by environment, never these keys |
| Azure Workload Identity ids from YAML win | `storage/azure.rs` | None: Azure is untested in Logweir |
| `backup.on_missing_topic: fail \| warn`, default `fail`; manifest `missing_topics` (skipped when empty) | `config.rs:456-462`, `manifest.rs:28-34` | Logweir renders neither, so behaviour and manifest bytes are unchanged. The vendored manifest struct would keep the field in `extra` (`L/crates/logweir-engine-oso/src/vendored/manifest.rs:10-25`) |
| `offset_storage.sync_interval_secs` optional; deprecation warnings from `validate()` | `config.rs:1139-1172` | None: Logweir renders no `offset_storage` and no `backup.checkpoint_interval_secs`, and the warning text does not match the `Ignoring unknown config key` needle (`L/crates/logweir-engine-oso/src/subprocess.rs:164`) |
| `describe` and `validate` accept `--config` | `crates/kafka-backup-cli/src/main.rs` | None: Logweir runs `backup`, `restore` and `validate-restore` only (`L/crates/logweir-engine-oso/src/engine.rs:268-273,422-423,593-594`), whose arguments are unchanged |
| Segment container | `segment/format.rs`, `segment/reader.rs` unchanged since 0.18.0; `writer.rs` last changed in 0.21.0 (segment sha256) | `.kbak` decoding is unaffected (§4.2, cycle c3) |
| `doctor` | `L/crates/logweir/src/doctor.rs:194-230` accepts exactly `0.21.0` | A bump must move this pin with the digest (PROD-00.3f) |

### 4.2 Runs: 0.22.0 against the pin

Every cycle ran on the shared e2e compose stack (project `logweir-e2e`, Kafka `apache/kafka:3.7.1` unless stated), under `claude/compose-lock.sh`, from `up` to `down -v`. The engine digest was overridden in this worktree only (`third_party/kafka-backup-binary.digest` and `e2e/compose/.env`) and restored at the end of each cycle, before the lock was released. The demo steps are `scripts/demo.sh` steps 2–6 with two changes:

- step 1 (`extract-engine.sh`) is skipped, because it refuses any digest but the pin;
- `target/debug/logweir` replaces `cargo run --release`.

`doctor`'s exit code is recorded, and the drill runs regardless. The logs are under `runs/<cycle>/` in the artifacts.

| Cycle | Engine | What ran | Result |
|---|---|---|---|
| c3 | v0.22.0 | demo drill | `outcome: pass`, `integrity: byte-fingerprint/pass` 150/150, `matrix_verdict: pass`, `header_preflight: honoured`, `unknown_key_warnings: []`; `logweir drill verify` and `docs/verify_scorecard.py` 1.14.0 both VALID |
| c3 | v0.22.0 | `doctor` | `FAIL engine version: version mismatch: expected 0.21.0, engine reports kafka-backup 0.22.0` |
| c3 | v0.22.0 | `.kbak` decoder | the seed refreshed the fixture pair from the 0.22.0 archive (6 segments, 2000 records, every sha256 present); `cargo test -p logweir-engine-oso --test kbak` 20/20, including `the_upstream_fixture_pair_describes_itself`; fixtures restored afterwards |
| c3 | v0.22.0 | `just pitr` | 1/1: six of nine records restored, boundary included, bound [0, 9], both readers accept the signed result |
| c3 | v0.22.0 | manifest | the same top-level keys as a 0.21.0 manifest; no `missing_topics` |
| c1 | v0.21.0 (pin) | demo drill | identical verdicts: `pass`, `byte-fingerprint/pass` 150/150, `matrix_verdict: pass`, same levers, both verifiers VALID |
| c1 | v0.21.0 (pin) | `just pitr` | 1/1: six of nine records restored, bound [0, 9] |
| c4 | v0.22.0 | `just mvp-demo` (the receipt path) | `logweir backup run` through engine 0.22.0 wrote a receipt (2000 records, 2 topics) that `logweir drill verify --payload-type backup-receipt` and `verify_scorecard.py --payload-type backup-receipt` both accept (VALID, all five receipt invariants). The bound new-topic point-in-time restore scored `pass`, `byte-fingerprint/pass` 150/150, VALID in both readers |
| — | v0.21.0 (pin) | receipt path | main's CI run 36194593644 (`e52d41cb`, 2026-09-25), e2e job green; its suite runs `e2e/tests/mvp_demo.rs`, which shells `scripts/mvp-demo.sh` and asserts its artefacts |

### 4.3 The pin on Kafka 4.3.1

The engine's fixed protocol versions (C8) were exercised on the newest supported Apache Kafka line by setting `KAFKA_VERSION=4.3.1`, the variable PROD-01.5 parameterizes.

| Cycle | What ran | Result |
|---|---|---|
| c7 | demo drill on `apache/kafka:4.3.1`, engine v0.21.0 | `outcome: pass`, `integrity: byte-fingerprint/pass` 150/150, `matrix_verdict: pass`, `header_preflight: honoured`, both verifiers VALID. The engine's backup (the seed), its `validate-restore` and its restore all completed against the 4.3.1 broker with the fixed versions listed in §3.8 |
| c7 | `just pitr` on 4.3.1 | 1/1: six of nine records restored, bound [0, 9] |
| c8 | CI's full e2e command on 4.3.1, engine v0.21.0 | exit 0 in 831 s, 64 passed and 0 failed (including `mvp_demo.rs`, the receipt path, and nine SCRAM tests). The deleted-segment control passed. A read-only probe of the running broker container read `apache/kafka:4.3.1` (image `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`) and `Kafka version: 4.3.1` |

c7 set `KAFKA_VERSION=4.3.1` exactly as c8 did, but recorded no broker probe. c8's probe is the direct evidence that this mechanism selects `apache/kafka:4.3.1`, and c5's probe on 3.7.1 read back `apache/kafka:3.7.1` and "Kafka version: 3.7.1". Nothing in the engine's fixed protocol versions failed on 4.3.1, which confirms §3.8's reading from source.

### 4.4 Run 36542777892: the red v0.22.0 row is a fixture race, not the engine

Run 36542777892, at `4861e03a`, recorded five rows as declared and `v0.22.0 | 3.7.1 | full` as `fail(e2e suite)`. One test failed: PROD-01.1's `keys_nulls_tombstones_and_duplicate_headers` (`e2e/tests/record_semantics.rs:1609`). Its backup `recsem-1463674564-shapes-b` exited 1 with "declares no segment for any of the named topics … the engine exited 0 having captured nothing". The other seven record-semantics rows passed on 0.22.0.

**What happened.** This is the job's broker log, uploaded as `compose-logs-v0.22.0-kafka-3.7.1` (artifact `round3/ci-36542777892/compose.log`):

| Time (UTC) | Event |
|---|---|
| 08:39:53.2 | The source topic is created with `message.timestamp.type=CreateTime` and cluster-default retention |
| 08:39:54.3 | The fixture lands: 15 records, stamped around `T` = 2025-10-09 |
| 08:39:55.755–.760 | The broker's retention check deletes the only segment of all three partitions "due to log retention time 604800000ms breach based on the largest record timestamp" (`largestRecordTimestamp=1760000000070`). The log start reaches the log end (8, 4 and 3) |
| 08:39:57.32–.35 | Engine 0.22.0 runs for 27 ms, finds every partition empty and exits 0. Logweir refuses |

The check is periodic. The broker started at 08:29:25.7, and it checks 30 s after starting and then every 300 s: 08:29:55.7, 08:34:55.7 and 08:39:55.7. The fixture's records are about 355 days old, far past the default 7-day retention, so the first check after the produce deletes them. The row fails when that check falls between the produce and the engine's capture.

**Reproduced with both engines.** The runs used compose slot 2 and Kafka 3.7.1, from a scratch copy of the worktree with one scratch test that was never committed (artifacts `round3/`). The test creates three topics the way PROD-01.1 creates its sources:

- A: the shapes fixture at `T`;
- B: one plain record at `T`, with no nulls, tombstones or headers;
- C: the shapes fixture, stamped with the current time.

It waits for the broker's retention check, then backs each topic up.

| Engine | Wait for the check | A (shapes at `T`) | B (plain record at `T`) | C (shapes, stamped now) |
|---|---|---|---|---|
| v0.22.0 | 208 s | emptied (log start 8/4/3 = end); exit 1, "captured nothing" | emptied; exit 1, "captured nothing" | untouched; exit 0, captured |
| v0.21.0 (pin) | 116 s | the same | the same | the same |

The broker deleted A and B at its checks at 09:10:38.7 and 09:15:38.6, 300 s apart, and nothing else (`round3/broker-retention-deletions.txt`). Without the forced wait, `keys_nulls_tombstones_and_duplicate_headers` passed on both engines in the same session: 74.7 s on 0.22.0 and 65.0 s on 0.21.0. It also passed in c10 (§5.5).

**The fix, verified.** A second scratch test used the same two old-stamped topics, created with `retention.ms=-1`, plus a canary on cluster-default retention. The canary was emptied at the check at 09:25:13.4, 244 s in, which proves the check ran. A and B were untouched, and engine 0.22.0 archived 15 of 15 and 1 of 1 records (`round3/fix-summary.txt`).

**So:**

- **No record shape triggers it.** A single plain record at `T` is lost the same way, and both engines capture the full shapes fixture when it is stamped now.
- **It is not the engine.** Both versions handle an emptied topic identically, by the same code (C20).
- **It is a latent race in the test fixtures, on every full row.** The source topics that hold `T`-stamped records are created without `retention.ms`. In run 36542777892 there were seven such lifetimes, totalling about 100 s; the recreate row creates its topic twice:
  - `pitr-src`: 16.9 s;
  - `shapes`: 6.1 s;
  - `ts-floor`: 21.4 s;
  - `ts-bound`: 13.4 s;
  - `ts-pit`: 13.5 s;
  - `recreate`: 6.0 s, then 22.5 s for its second generation.

  With a check every 300 s, a check lands inside one of those lifetimes about one run in three. It lands inside the narrower window between a produce and its capture less often. The pin rows and main's CI e2e job carry the same exposure.
- **The fix is in the fixtures** (A-C20-2). Round 4 applied it on this branch (§4.5).

### 4.5 Round 4: the fixture fix on this branch, and the race gone live

The orchestrator authorized the fix on this branch as an out-of-ownership edit (`d795bae4`).

- **The helper.** `harness::create_topic_for_fixed_timestamps` (`e2e/tests/harness/mod.rs`) is `create_topic_with_configs` with `retention.ms=-1` added. It refuses a caller that states `retention.ms` itself.
- **The class, swept once** (`e2e/tests/**` and `crates/*/tests`). Three sites stamp a fixed past instant into a topic they create, and all three now use the helper:
  - `record_semantics.rs`'s `Row::source_topic`, which every record-semantics row uses;
  - the recreate row's second generation, a direct re-creation in `record_semantics.rs`;
  - `pitr_boundary.rs`'s `pitr-src`.

  The support module creates no topics. These are outside the class:
  - `topic_identity.rs` (PROD-01.4) stamps a minute ago, and already starts every row on `retention.ms=-1`. Its row c10 lowers retention on purpose and keeps it.
  - `mvp_demo.rs` stamps at the live seed's newest instant.
  - No `crates/*/tests` live row sets a record timestamp; `check_cli.rs`'s console producer stamps now.
- **The guard.** `e2e/tests/fixture_retention.rs` is a text scan in the default test set, so CI's `cargo test --workspace` runs it. A file under `e2e/tests/` that holds a fixed epoch-milliseconds literal older than seven days must create its topics through the helper.
  - It asserts that the walk descends, that it sees the two known suites, and that the helper adds the override.
  - Negative controls: a planted configs-path creation and a raw `--create`, in memory and on disk in a nested directory; relative, recent and comment-only stamps stay outside.
  - Seven mutants are caught (`round3/fixture-guard-mutants.log`).
- **Live, on slot 2 with Kafka 3.7.1** (`round4/`). The broker's `kafka.log.LogManager` logger was set to DEBUG at run time, so every retention check logs "Beginning log cleanup". `keys_nulls_tombstones_and_duplicate_headers` then ran ten times with each engine:

  | Measure | Result |
  |---|---|
  | Shapes row, v0.22.0 | 10 of 10 passed |
  | Shapes row, v0.21.0 | 10 of 10 passed |
  | Retention checks logged | 6, every 300 s (09:53:23 to 10:18:23) |
  | Fixed-stamp source topics | 22 lifetimes (20 shapes, 2 `pitr-src`), every one created with `retention.ms=-1` |
  | Checks inside a lifetime | 5. Three fell after the source was created and before its first restore target existed, the window in which run 36542777892 lost its records: 09:58:23 and 10:03:23 on 0.22.0, 10:18:23 on 0.21.0 |
  | Segments deleted "due to log retention time" | 0 |

  `record_semantics` and `pitr_boundary` then ran in full on a fresh stack, per engine, with the second verifier's interpreter set (`LOGWEIR_E2E_PYTHON`). The first attempt had stopped earlier, before any retention question arose, because this host's `python3` has no `cryptography`.
  - **Both engines:** `pitr_boundary` 1 passed, and `record_semantics` 8 passed with 2 ignored.
  - **That session:** 4 checks, and 14 fixed-stamp lifetimes, all on `retention.ms=-1`. They cover every source of every row, both generations of the recreated topic, and both `pitr-src`.
  - **Checks inside a lifetime:** two. At 10:27:20 a check fell inside `ts-bound`'s produce-to-restore window on 0.22.0; at 10:32:20 one fell inside `pitr-src`'s lifetime on 0.21.0. No segment was deleted by retention.

  Across both sessions that makes 10 checks and 36 lifetimes. Seven lifetimes had a check inside, four of them in the window that failed CI, and nothing was deleted.

## 5. engine-matrix

### 5.1 Why every scheduled run failed

The three scheduled runs (34830737064 on 2026-09-14, 35586616823 on 2026-09-21, 36413265594 on 2026-09-28) failed identically. Seven defects, each independent of the others:

| # | Defect | Evidence (run 36413265594) | Effect |
|---|---|---|---|
| 1 | `scripts/e2e-seed.sh` demanded a sha256 on every segment | rows v0.20.0, v0.19.2, v0.19.1, v0.18.0: `segments with an empty sha256 (not a v0.21.0 manifest)`, exit 1 | Every row below the pin failed before a test ran; engines below 0.21 write no digest |
| 2 | The full-drill step had drifted from the CI e2e job | row v0.21.0: four `e2e/tests/scram.rs` tests failed with `invalid credentials`, because the matrix never ran `scram-setup`. A fifth, `a_pod_really_reaches_the_k8s_listener`, failed with `context "docker-desktop" does not exist` | The pinned engine could never be `pass` |
| 3 | `scripts/run-named-tests.sh` reported an existing test missing | row v0.21.0: `printf: write error: Broken pipe`, then `no test named a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard exists … 4042 test(s) were available`; the test is at `e2e/tests/full_drill.rs:129` | Under `pipefail`, `grep -q` exits at the first match and `printf` takes EPIPE once the listing outgrows the pipe buffer. The pin recorded `fail(lever-not-honoured)` |
| 4 | Rows were written to a hidden directory | every row: `No files were found with the provided path: .matrix/` (`include-hidden-files: false`, the `upload-artifact@v4` default) | `publish` found no rows: `no matrix rows were produced; refusing to blank the table` |
| 5 | A skipped control was classified as a failed one | row v0.20.0: control `skipped` after the seed failed, recorded as `fail(lever-not-honoured)` | A wrong verdict in the one outcome that detects an ignored lever. Fully fixed only in the fix round (`ff9aa14a`), which derives every outcome from what ran |
| 6 | `publish` needed a pull request Actions may not open | `gh api repos/VladyslavHaina/logweir/actions/permissions/workflow` → `can_approve_pull_request_reviews: false` (2026-09-28) | `publish` would have failed at `create-pull-request` even with rows |
| 7 | The declared rows contradicted the documented floor, and a failing row was green | v0.20.0 and v0.19.2 were declared full-drill rows although the docs put them below 0.21.0. Row v0.21.0 showed ✓ with five failing tests, because every test step was `continue-on-error` and nothing compared the verdict with an expectation | A green matrix said nothing about the rows. The verdict step closes it, and since the fix round a test executes that step's own text |

`publish` also rewrote the whole hand-written `## Rows` table of `docs/support-matrix.md`. That would have erased the recorded evidence, and PROD-01.5's broker rows, on its first green run.

### 5.2 The repair (commits `d5d0be9b`, and `ff9aa14a` from the review)

**Steps.**

- The matrix job sets the stack up and runs the suite exactly as the CI e2e job does: `just e2e-up`, then `cargo test --locked -p e2e --features e2e -- --test-threads=1 --skip a_pod_really_reaches_the_k8s_listener`.
- `KAFKA_VERSION` comes from the row, and only through the job's environment. PROD-01.5 parameterizes that variable, and `e2e/compose/docker-compose.yml` reads it: `${KAFKA_IMAGE:-apache/kafka:${KAFKA_VERSION:-3.7.1}}` since PROD-01.5. The generated `.env` names the engine (`OSO_DIGEST`) and nothing else, and the matrix never sets `KAFKA_IMAGE`, which would win over the row (§5.5).
- Each tag resolves to a digest whose `org.opencontainers.image.revision` must equal the tag's commit (`git ls-remote`), the digest-to-commit binding `extract-engine.sh` asserts for the pin.

**Seed and lookup.**

- The seed runs with `LOGWEIR_SEED_REFRESH_FIXTURES=0`.
- For rows below the floor it runs with `LOGWEIR_SEED_SEGMENT_SHA256=optional`. The new mode relaxes only the absence of a digest (`scripts/seed-manifest-check.py`): counts and every present digest are still checked, and the mode is refused with a fixture refresh.
- `run-named-tests.sh` matches against the listing with here-strings.

**Recording and publishing.**

- **The outcome is derived from what every step did** (`scripts/engine-matrix-outcome.sh`, since `ff9aa14a`; review M1):
  - `fail(setup)`: a step did not run, the tag did not resolve, the stack did not come up, or the broker differs from the declaration;
  - `fail(seed)`, and `fail(build)`;
  - `fail(lever-not-honoured)` only when the control failed;
  - `pass` only when the suite and the control passed.

  Below the floor, `unsupported(lever-absent)` needs Logweir's floor refusal ("below the declared floor") in the kept transcripts of both the reduced row and the control. A drill that accepted a below-floor engine, or failed without that refusal, records `fail(floor-not-enforced)`. The first round recorded `unsupported(lever-absent)` for every below row whatever ran, and `fail(lever-not-honoured)` for a skipped control.
- **The broker is read back** from the running container (`scripts/engine-matrix-broker.sh`: its image, image id and logged "Kafka version"; review L6). The Kafka column records that measured version, or `unmeasured`.
- **The row line** carries that broker, the digest, the outcome, the evidence and a run link.
- **A final verdict step** fails the job unless the recorded outcome equals the row's `expect`.
- Rows go to `matrix-rows/`, and the upload fails if the directory is empty.
- `publish` is read-only. `scripts/engine-matrix-rows.py` renders the rows between `<!-- engine-matrix:rows:begin -->` and `<!-- engine-matrix:rows:end -->` and touches nothing else. It refuses missing or repeated markers, zero rows, a malformed row, a repeated (tag, broker) pair, or fewer rows than `--expect 6`. The page goes out as the `support-matrix` artifact and in the run summary.
- `open-pr` opens the pull request only on `main` and only when the repository variable `ENGINE_MATRIX_OPEN_PR` is `true`.

**Guards.** `crates/logweir/tests/engine_matrix.rs` holds 28 tests: 24 from the fix round, two from the merge of PROD-01.5 (§5.5) and two from run 36542777892 (§5.6). Beyond the first round's sixteen, the fix round's tests:

- run the outcome script over a table of step outcomes, including the reviewer's cases A (a build failure), B (a failed below-floor seed) and C (a below-floor engine the drills accepted);
- execute the Record step's and the verdict step's own `run:` text, with the Record step's wiring asserted;
- pin the seed's digest mode per floor and `open-pr`'s main-only gate;
- check that the drill steps keep their transcript and exit status;
- check the broker readback against a fake `docker`.

The reviewer planted eight regressions, and four survived the first round's tests. All eight are now caught, with seventeen further mutants (artifact `fix-round-mutants.log`; the first round's log is `engine-matrix-guard-mutants.log`). One test sweeps every workflow for `upload-artifact` from a hidden path. `actionlint` 1.7.12, which runs shellcheck on every `run:` block, is clean.

### 5.3 Declared rows

| Engine | Kafka | Floor | Declared outcome | Why this row |
|---|---|---|---|---|
| v0.22.0 | 3.7.1 | full | `pass` | Upstream's current release; the proposed pin (PROD-00.3f) |
| v0.21.0 | 3.7.1 | full | `pass` | The pin, on the compose stack's default broker |
| v0.21.0 | 4.3.1 | full | `pass` | The pin on the newest supported Apache Kafka line (the `latest` image on 2026-09-28): the C8 tripwire |
| v0.20.0 | 3.7.1 | below | `unsupported(lever-absent)` | Newest four minors |
| v0.19.2 | 3.7.1 | below | `unsupported(lever-absent)` | `kafka-backup-operator` 1.3.0's library version |
| v0.19.1 | 3.7.1 | below | `unsupported(lever-absent)` | `strimzi-backup-operator` v0.2.22–v0.2.25 default |

v0.18.0 left the window: it is the fifth-newest minor, and no operator defaults to it. PROD-01.5 has since pinned its 4.3 line to 4.3.1 (`e2e/compose/stack-env.sh`), the release this row declares.

The full rows run the whole CI e2e command. Since the merge of main (`632ea345`), that includes PROD-01.1's `e2e/tests/record_semantics.rs`, PROD-01.4's `e2e/tests/topic_identity.rs` live rows and PROD-01.5's `e2e/tests/stack_params.rs`. The record-semantics contract is asserted only on engine 0.21.0 (`CONTRACT_ENGINE`): the v0.22.0 row records those outcomes without asserting them, and both pin rows assert them. §5.5 runs these rows locally on the three matrix rows that no earlier run had combined them with. A failure on GitHub makes a row record `fail(e2e suite)`. That is real evidence, for PROD-01.5 when the broker is the cause and for PROD-00.3 when the engine is, and the row's declaration then follows the evidence.

### 5.4 Local validation and the dispatch command

**On GitHub.** Run 36531786341, dispatched on this branch at `7e4cd0b1`, was green on all six rows.

- The reviewer read each job's log. The three below rows seeded with `optional` digests ("6 of 6 segments carry no sha256"), and failed their reduced row and control on Logweir's floor refusal ("ignored the config key `restore.header_preflight`", or `restore.dry_run_check_segments` for v0.20.0, "… below the declared floor").
- `publish` rendered six rows, and `open-pr` was skipped.
- The fix round (`ff9aa14a`) changes how a row is recorded, so its CI evidence is the re-dispatch at the final tip.

**Locally.** The workflow's matrix steps were run under the compose lock, with the same commands and environment, as cycles c5 (v0.22.0 × 3.7.1, full row), c6 (v0.19.2 × 3.7.1, below-floor row) and c8 (v0.21.0 × 4.3.1, full row). The `publish` path was run over the `Record this row` step extracted verbatim from the workflow, for six step-outcome combinations (artifact `publish-simulation/`). Each combination recorded its intended outcome (`pass`, `unsupported(lever-absent)`, `fail(seed)`, `fail(setup)`, `fail(e2e suite)`, `fail(lever-not-honoured)`), and `engine-matrix-rows.py` rendered all six, leaving the page byte-identical outside the markers.

| Cycle | Row | Steps as the workflow runs them | Recorded outcome |
|---|---|---|---|
| c6 | v0.19.2 × 3.7.1 (below) | `just e2e-up`; seed with `LOGWEIR_SEED_SEGMENT_SHA256=optional`, exit 0 (6 segments, 2000 records, 0 digests: the step every earlier run failed); `cargo build`; reduced row, exit 101; control, exit 101 | `unsupported(lever-absent)`, as declared. Both test failures are the floor at work: Logweir refused the engine with "engine 0.19.2 ignored the config key `restore.header_preflight` that logweir rendered; this tag is below the declared floor" (exit 1) |
| c5 | v0.22.0 × 3.7.1 (full) | `just e2e-up`; seed (`required`), exit 0; `cargo build --locked -p logweir`; `cargo test --locked -p e2e --features e2e -- --test-threads=1 --skip a_pod_really_reaches_the_k8s_listener`, exit 0 in 1006 s: `backup_argv` 7, `check_image` 2 (12 ignored), `full_drill` 15, `guards` 22, `mvp_demo` 3, `offset_side` 4, `pitr_boundary` 1, `scram` 9 (1 filtered), `smoke` 1. That is 64 passed, 0 failed. The deleted-segment control passed | `pass`, as declared |
| c8 | v0.21.0 × 4.3.1 (full) | the same steps on `apache/kafka:4.3.1` (probed). Seed exit 0; the CI e2e command exit 0 in 831 s: `backup_argv` 7, `check_image` 2 (12 ignored), `full_drill` 15, `guards` 22, `mvp_demo` 3, `offset_side` 4, `pitr_boundary` 1, `scram` 9 (1 filtered), `smoke` 1. That is 64 passed, 0 failed. The control passed | `pass`, as declared |

The orchestrator dispatches the workflow on this branch. `workflow_dispatch` takes no inputs:

```sh
gh workflow run engine-matrix.yml --repo VladyslavHaina/logweir --ref claude/prod-00-1
gh run list --repo VladyslavHaina/logweir --workflow engine-matrix.yml --branch claude/prod-00-1 --limit 1
gh run watch <run-id> --repo VladyslavHaina/logweir
gh run view <run-id> --repo VladyslavHaina/logweir --log-failed
```

Expected result:

- six `matrix` jobs green, each ending with `recorded '<outcome>' as declared`, with the Kafka column holding the broker version each job read back;
- `publish` green, with a `support-matrix` artifact whose generated section holds six rows;
- `open-pr` skipped, because the run is not on `main`.

A red row names what it recorded against what it declared.

### 5.5 After the merge of PROD-01.5 (main `632ea345`)

This branch merged main at `632ea345` with a merge commit (`2af7f62e`, no rebase). The merge brought in PROD-01.1, PROD-01.4 and PROD-01.5, and it changed three things for the matrix.

**The seed keeps both behaviours (review L5; `5818feea`).**

- `scripts/e2e-seed.sh` conflicted, because both branches added a check before any work.
- PROD-01.5's check comes first. It computes `REFRESH_FIXTURES`, which is 1 on the default stack and 0 on a slot, and it refuses 1 on a slot.
- The digest-mode guard follows and tests that computed value. It no longer reads the raw `${LOGWEIR_SEED_REFRESH_FIXTURES:-1}`, which refused `optional` on every slot.
- `optional_digests_work_on_a_slot_and_on_the_default_stack` runs the seed in a clean environment built from `e2e/compose/stack-env.sh --slot 2`, with a `docker` that records whether it was called.
  - Three cases pass the checks and reach the stack: slot 2 with the refresh unset, slot 2 with it at 0, and the default stack with it at 0.
  - Two cases are refused before docker runs, each by its own stack's rule: the default stack with the refresh unset, and slot 2 with it at 1.
- Two mutants of the merge turn the test red: a guard that reads the raw variable, and PROD-01.5's slot refusal dropped (artifact `fix-round-l5-mutants.log`).
- The matrix still seeds on the default stack with the refresh at 0.

**The generated `.env` no longer names the broker (`545bc8bb`).**

- The full drill now runs PROD-01.5's `a_slot_moves_every_host_port_and_the_default_render_does_not`. It renders the stack with every stack variable removed, and it requires the compose file's own default broker, `apache/kafka:3.7.1` (`e2e/tests/stack_params.rs:1246`).
- The matrix wrote the row's `KAFKA_VERSION` into `e2e/compose/.env`, and that render reads `.env`. The 4.3.1 row would therefore have recorded `fail(e2e suite)`.
- This was reproduced with `docker compose config` alone (artifact `merge-env-render.log`). A `.env` holding `KAFKA_VERSION=4.3.1` fails at `:1246`; CI's shape (`KAFKA_VERSION=3.7.1`) and a digest-only `.env` both pass.
- The generated `.env` now holds `OSO_DIGEST` alone. The row's broker is the job's `KAFKA_VERSION`, which compose prefers over `.env`.
- `the_generated_env_names_the_engine_and_leaves_the_broker_to_the_row` pins this. In cycle c9 (the table below), the render row passed while the stack ran 4.3.1.

**The broker line.**

- `kafka-broker-1`'s image is now `${KAFKA_IMAGE:-apache/kafka:${KAFKA_VERSION:-3.7.1}}`.
- `the_declared_rows_follow_the_documented_floor` reads that image as YAML and accepts both this form and the unwrapped one. Before, it split the whole file on a substring.
- The test also asserts that the job sets `KAFKA_VERSION` from the row. Nothing in the job, `.env` included, may set `KAFKA_IMAGE`, which would win over every row.
- The matrix asks for 4.3.1 by tag, and PROD-01.5's `--kafka 4.3` pins the same release by digest. On slot 2 the running broker's image id read back as that digest (`sha256:77e3df90…`); 3.7.1's read back as `sha256:ed74d7d1…`.
- Six mutants are caught:
  - `KAFKA_VERSION` dropped from the image line;
  - `KAFKA_IMAGE` set in the job;
  - `KAFKA_IMAGE` written into `.env`;
  - `KAFKA_VERSION` not taken from the row;
  - the old `.env` line restored;
  - the `.env` write removed.
- The pre-01.5 form of the image line is still read. The compose mutants ran in a scratch copy with its own target directory, and `e2e/compose` was not edited (artifacts `fix-round-parser-mutants.log` and `merge-env-render.log`).

**The merged suite on the rows at risk.** No earlier run had combined the new rows with three of the matrix's rows:

- PROD-01.1 measured its record-semantics rows on 3.7.1 only;
- PROD-01.4 ran its live rows with the pin, on 3.7.1 and 4.3.1;
- PROD-01.5 ran one row of each on 4.3.1.

Cycles c9 to c11 ran on compose slot 2 (PROD-01.5; a slot needs no lock). Each was set up exactly as the workflow now sets up a row: a digest-only `.env`, `KAFKA_VERSION` in the environment, and the broker read back by `scripts/engine-matrix-broker.sh`. The engine digest was overridden in this worktree only, and restored afterwards.

| Cycle | Row | Steps | Result |
|---|---|---|---|
| c9 | v0.21.0 × 4.3.1 | `just e2e-up` (broker read back as `apache/kafka:4.3.1`, Kafka 4.3.1); seed (`required`); `cargo build`; `cargo test --locked -p e2e --features e2e --test record_semantics --test topic_identity --test stack_params -- --test-threads=1` | Exit 0 in 905 s. `record_semantics`: 8 passed, 2 ignored, with the contract asserted (engine 0.21.0). `stack_params`: 17 passed, including the default render. `topic_identity`: 52 passed, 1 ignored (its retention row) |
| c10 | v0.22.0 × 3.7.1 | the same (broker `apache/kafka:3.7.1`, Kafka 3.7.1; engine `kafka-backup 0.22.0`) | Exit 0 in 980 s. `record_semantics`: 8 passed, 2 ignored; on 0.22.0 the contract is not asserted, by design. `stack_params`: 17 passed. `topic_identity`: 52 passed, 1 ignored. Its rows capture with the engine under test, so the rule's asserted known misses (c13, c14) and false positive (c19) hold on 0.22.0 |
| c11 | v0.19.2 × 3.7.1 (below) | seed (`optional`), exit 0; `cargo build`; reduced row, exit 101; control, exit 101 | Both refused with "below the declared floor", so the row records `unsupported(lever-absent)`, as declared |

The rest of the full drill, 64 tests, passed on the three full rows at `7e4cd0b1` (run 36531786341). On main, the CI e2e job runs the pin on 3.7.1, and it passed at `632ea345` (run 36536046039).

### 5.6 After run 36542777892: the declaration, and the retention readback (`f3eb390a`)

**The v0.22.0 row still declares `pass`.** That is what the engine earns. The row passed in c10, and in §4.4's unforced runs, and the red was a fixture race that hits the pin too.

An expected-failure declaration naming a 0.22.0 defect would be false, because no such defect exists. It would also turn the row red whenever the race misses, which is most runs.

Nothing is hidden. PROD-01.1's assertion is untouched, a failed suite still records `fail(e2e suite)`, and the verdict step still fails the job. A-C20-2 has landed (§4.5). The readback stays, so a future retention loss names itself.

**What changed: the red now names its cause.**

- A step after the drills, "Read back what the broker's time retention deleted" (`if: always()` once the stack is up; `continue-on-error`), runs `scripts/engine-matrix-broker.sh --retention`.
- The script counts the segments the broker deleted "due to log retention time" and names their topics. It names five and counts the rest. It ignores the other deletion lines: the log start moving, a topic deleted by a test's cleanup, and the files removed later.
- `scripts/engine-matrix-outcome.sh` adds them to a failed suite's reason, for example: "the full e2e suite failed; during the run the broker's time-retention check deleted 3 segment(s) of recsem-1463674564-shapes …". It never changes an outcome.

**Guards.**

- `the_brokers_retention_deletions_are_read_back` covers:
  - CI's three lines, among the other deletion lines, giving `retention_deletions=3` and `retention_topics=recsem-1463674564-shapes`;
  - the five-topic cap;
  - zero deletions;
  - a refused option;
  - the step's place and flags.
- `a_failed_suite_names_what_the_brokers_retention_deleted` checks that the reason names deletions only for a failed suite, and never changes a `pass`, a below-floor refusal or a control failure.
- Ten mutants are caught (artifact `round3/retention-attribution-mutants.log`):
  - counting every deletion line;
  - keeping partition suffixes;
  - no cap;
  - zero deletions made an error;
  - never naming the deletions;
  - zero counted as a deletion;
  - deletions changing the outcome;
  - the readback skipped after a failed drill;
  - the readback made fatal;
  - the Record step not wired to the readback.

## 6. Support documents corrected

Only engine-version and operator-default statements changed. The broker-version rows are PROD-01.5's.

- **[support-matrix.md](../../support-matrix.md).**
  - The opening note now says that the workflow renders only the section between its markers.
  - `fail(reason)` names `fail(seed)` and `fail(setup)`.
  - "Versions with no row yet":
    - adds 0.22.0 with its evaluation;
    - states which operator release runs which engine;
    - replaces the wrong claim that v0.19.1 is `strimzi-backup-operator`'s hard-coded default, together with its projected verdict ("restore succeeds, `pass-degraded`, `consume-only`"), which no run supported.
  - The auth table's engine column states what the engine offers for OAUTHBEARER and MSK IAM.
  - "What the weekly job will add" becomes "The weekly engine-matrix job", describing the repaired job and carrying the generated section.
- **[stability.md](../../stability.md).**
  - The "Supported engine pin" bullet: 0.22.0's status, the operators' engines, and `partial` for archives written before 0.21.
  - Later item 2, "Strimzi as a source": v0.22.0 since operator v0.3.0; what the drill and the catalog do with operator archives.
- **[install.md](../../install.md)** (outside this row's ownership, one sentence). The same wrong default claim.

## 7. Archives written by OSO's operators (engine 0.19–0.22)

### 7.1 Which engines write them

| Operator release | Engine it writes with | Source |
|---|---|---|
| `strimzi-backup-operator` v0.3.0, v0.3.1 (2026-09-07), HEAD `cf5b1ecf` | v0.22.0 | `DEFAULT_BACKUP_IMAGE = "osodevops/kafka-backup:v0.22.0"` (`src/engine.rs:17`, commit `74f4e17`, "default engine v0.22.0"); the chart's `image: ""` means the compiled default |
| `strimzi-backup-operator` v0.2.22–v0.2.25 (2026-08-29/30) | v0.19.1 | the image reference in `src/` at each tag |
| `strimzi-backup-operator` v0.2.21 | v0.19.0 | same |
| `strimzi-backup-operator` v0.2.20 and earlier | v0.16.0 and older | same |
| `kafka-backup-operator` v1.3.0 (HEAD `b287418f`, 2026-08-30) | `kafka-backup-core` 0.19.2, linked as a library | `Cargo.toml` `kafka-backup-core = "0.19.2"`, `Cargo.lock` 0.19.2 |

`strimzi-backup-operator` also accepts a per-resource `spec.image`, so any engine is possible there. Both operators default `consumer_group_snapshot` to off (the engine's own default is `false`, `C/config.rs:505-508`), but both expose it.

### 7.2 What happens to such an archive today

- **The format is the same.** The segment container (`C/segment/format.rs`, `reader.rs`) is unchanged from 0.18.0 to 0.22.0, and only the manifest grew:
  - 0.20.0 added `source_replication_factor` and `configurations`;
  - 0.21.0 added the segment `sha256`, `uploaded_at` and `pruned`;
  - 0.22.0 added `missing_topics`.

  Logweir's vendored manifest defaults every added field and keeps unknown ones (`L/crates/logweir-engine-oso/src/vendored/manifest.rs:1-25`).
- **The CLI drill (quickstart Path 3) reads it** and restores it with Logweir's pinned engine. Only the archive is foreign, so the lever floor applies to the pin, not to the operator's engine.
  - 0.21 and 0.22 archives carry segment digests, so the segment lane can verify.
  - 0.19 and 0.20 archives carry none. The lane is `Unverified` (`L/crates/logweir/src/drill/phase7_verify.rs:739-742,791-799`), the integrity result is `partial` and the drill never reports `pass`.

  Measured on 2026-09-29 (cycle c6): the compose stack was seeded by engine v0.19.2 (the `kafka-backup-operator` library version). Its manifest has no `source_replication_factor`, no `configurations` and no segment `sha256` or `uploaded_at` (artifact `runs/c6-v0.19.2-k3.7.1-below/archive-manifest.json`). The pinned engine then drilled it. The signed scorecard reads `outcome: fail-integrity`, `integrity: byte-fingerprint/partial`, and its `partial_reason` names all six segments as "carry no sha256 (written before 0.21) and could not be verified". The drill exits 2, and `logweir drill verify` and `verify_scorecard.py` both return VALID. The records themselves were restored and fingerprinted; only the segment lane is unverifiable.
- **Topic parity on 0.19.x archives compares partition counts only.** The real 0.19.2 manifest carries `original_partition_count`, so partition-count parity is compared. It has no `source_replication_factor`, so the target's own replication factor stands in for the source's (`phase7_verify.rs:1175-1182,1212-1213`). It has no `configurations`, so no configuration is compared. The evidence reports "no divergence" for the two quantities the archive never recorded, which is FX-4's coverage gap. The c6 scorecard over the 0.19.2 archive signs `topic_parity: {"intentionally_deviated": [], "unexpected_divergence": []}`, the same text as a fully measured archive.
- **A non-empty `consumer-groups-snapshot.json` fails the drill** before a scorecard exists: the vendored shape does not match what the engine writes (FX-1). This is not re-run here; the tracker's FX-1 row is the evidence.
- **Nothing imports into the catalog.** `logweir catalog sync` and the console's "Connect an existing archive" read only `logweir/backups/**/*.receipt.json`, verified against a trusted key (`L/crates/logweir/src/catalog/cli.rs:368-393`, prefix at `catalog/record.rs:37`). An operator-written archive has no receipt, so a sync finds zero points. It is restorable only through the CLI drill path, never from the console. This is read from source; A-OSO-3 is the row that will measure it.

### 7.3 What remains after FX-1

FX-1 makes the snapshot parse. After it:

1. 0.19.x and 0.20.x archives still drill as `fail-integrity` with `integrity.result: partial`: upstream wrote no segment digests before 0.21, so Logweir can decode and count their segments but cannot authenticate the bytes. PROD-08.1's complete mode could hash segments itself, but with no reference digest it states "decoded, not authenticated".
2. Foreign archives still cannot enter the catalog. That needs an "unattested foreign point" import (proposed row §9, `PROD-00.3n`), not an engine change.
3. The snapshot remains import-only evidence: it has no group type, state or generation, it omits groups without offsets on archived topics (C18), and PROD-04.1's native capture supersedes it.
4. 0.19.x archives keep FX-4's parity gap until capture coverage is recorded.

### 7.4 Acceptance rows (the procedure)

- **A-OSO-1 (FX-1, PROD-01.2).** For each engine E in {v0.19.1, v0.19.2, v0.20.0, v0.21.0, v0.22.0}:
  1. Bring the compose stack up with the engine digest set to E, and seed it (`LOGWEIR_SEED_SEGMENT_SHA256=optional` below 0.21).
  2. Point `third_party/kafka-backup-binary.digest` back at the pin.
  3. Run the demo drill steps 4–6 over that archive (the `foreign-drill` cycle, §4.2).

  Pass: for E ≥ 0.21, `outcome: pass` with `integrity: byte-fingerprint/pass`; for E < 0.21, a signed `outcome: fail-integrity` scorecard (exit 2) whose `integrity.result` is `partial` with a `partial_reason` naming the segments without sha256. This is what v0.19.2 produced on 2026-09-29. Every scorecard verifies with both readers, and no run exits 1. Negative control: deleting a non-oldest segment of the E < 0.21 archive yields `preflight-failed`, exit 2.
- **A-OSO-2 (FX-1).** As A-OSO-1, with `consumer_group_snapshot: true` in the backup and one committed consumer group on `orders`. Before FX-1 the drill exits 1 with no scorecard. After it the drill completes as in A-OSO-1, and the snapshot parses to exactly the committed group. Negative control: a snapshot with no groups parses to zero groups, not an error.
- **A-OSO-3 (PLAT-15.2 / PROD-00.3n).** A bucket holding only an operator-written archive: `logweir catalog sync` reports zero points and says why (no Logweir receipt), never a phantom point and never an error. Negative control: a Logweir-written point in the same bucket syncs as one point.
- **A-OSO-4 (engine-matrix, after FX-1).** Add an "archive" row kind, seeded with E and drilled with the pin, for the operator engines. A-OSO-1 then runs weekly.

## 8. OD-3 — options and recommendation for the owner

OD-3's four options per capability are:

- **U**, an upstream PR (bug-class only);
- **F**, a maintained MIT patch queue on the PROD-00.2 source build, which supersedes or scopes GR6;
- **N**, a Logweir-native path;
- **X**, declared unsupported.

| ID | U | F | N | X | Recommendation |
|---|---|---|---|---|---|
| C1 control records, READ_COMMITTED | viable: a defect, small | carry if refused | a native capture = rewriting the engine | the interim rail | **U**, F fallback; N rail now |
| C2 offset-after-upload | viable | carry if refused | — | Logweir never uses the mode | **X** for Logweir now; **U** when PROD-02.3 wants the mode |
| C3 conditional manifest | viable | carry if refused | FX-7 covers Logweir's own runs | — | **N** now; **U** with PROD-02.3 |
| C4 checkpoint cadence and hash | viable | carry if refused | stable per-execution paths | shutdown granularity | **N** + **U** |
| C5 idempotent produce | viable, moderate | carry if refused | native producer (large) | — | **U**, F fallback; N duplicate detection now |
| C6 min/max timestamps | viable | carry if refused | exact archive-side filter (PROD-08.1) | old archives | **U** + **N** |
| C7 topic IDs | a feature (not U here) | Metadata v10+ capture | PROD-01.4 heuristic + nullable field | — | **N**; **F** only if PROD-01.4 chooses it |
| C8 ApiVersions | viable | carry if refused | — | nothing fails on 4.3.1 | **X** now; **U** on a floor raise |
| C9 OAUTHBEARER / MSK IAM | likely refused (commercial seam) | OAUTHBEARER plugin from YAML | — | MSK IAM until OD-4 | **F** OAUTHBEARER; **X** MSK IAM |
| C10 filter / transform | likely refused (Part 2, commercial seam) | filter rules from YAML | masking through a native producer | — | **F** filter; **N** transform |
| C11 offset-range restore | a feature | via C10's rules | — | — | **F** via C10 |
| C12 byte-rate limit | viable | carry if refused | expose records/sec only | — | **U** |
| C13 duplicate headers | viable in `kafka-protocol-rs` | carry if refused | source-side detection | disclosed meanwhile | **U** + **X** disclosed |
| C14 LogAppendTime timestamps, producer metadata | viable in `kafka-backup` (a defect; the engine parses batch headers itself) | carry if refused | FX-8's rail | producer metadata disclosed | **U** for LogAppendTime; **X** disclosed for producer metadata |
| C15 transport derivation | — | — | refusal in `render_storage_block`, plus a phase-0 rule like R3 | — | **N**, before the bump |
| C16 ENGINE-PATHSTYLE | — | — | keep the refusal | — | **N** |
| C17 engine evidence | — | — | Logweir verification | unconsumed | **X** |
| C18 group snapshot | — | — | FX-1 + PROD-04.1 | — | **N** |
| C19 build | — | PROD-00.2 | — | — | **F** (the precondition of every F row) |

**Recommendation to the owner.**

1. **Build from source (PROD-00.2) and allow a patch queue.** Amend GR6 from "unmodified upstream image" to "upstream source plus a recorded patch queue". Each patch cites its upstream PR, or the refusal that justifies carrying it. This agrees with OD-3's current recommendation.
2. **Upstream first for defects.**
   - Open PRs to `kafka-backup` for C1, C4 (cadence and hash), C5, C6, C12 and C14; to `kafka-protocol-rs` for C13; for C2 and C3 when PROD-02.3 needs them; and for C8 on its trigger.
   - Carry a patch when a PR is declined, or not released within **30 days**.
   - Evidence that this is realistic: an outside contributor's three bug-fix PRs (#145, #147, #149) were merged the day after they were opened (2026-08-17 → 2026-08-18). The maintainer's own issues #161 and #166–#168 (filed 2026-08-30) shipped eight days later in 0.22.0. Upstream tagged seven releases between 2026-08-29 and 2026-09-07.
   - Its limits: outside PR #172, which adds `--config` to `describe` and `validate`, is still open, while the maintainer shipped the same change himself in 0.22.0 (#186). Outside PRs #198 and #199 (2026-09-24) are open. One maintainer accounts for 157 of the repository's contributions (GitHub contributors API, 2026-09-28).
3. **Patches, not upstream PRs, for the commercial seams:** C9's OAUTHBEARER, C10's filter rules (which also give C11) and, only if PROD-01.4 asks, C7.
4. **Native** for offsets and groups (C18, PROD-04), verification (C17, PROD-08), transport and addressing (C15, C16), resume paths (C4) and masking (C10's transform).
5. **Declared unsupported** until evidence or demand: MSK IAM (OD-4), the engine's continuous and offset-store modes (C2/C3), the engine's evidence reports (C17), producer metadata (C14), and LogAppendTime timestamps and duplicate header keys until C14 and C13 are released.
6. **Move the pin to 0.22.0** as PROD-00.3f, after its two guards (C15, C16) and the `doctor` pin. It does not wait for PROD-00.2.

**What only the owner can decide:**

- whether a patch queue on OSO's MIT source is acceptable at all (it supersedes GR6);
- the carrying window;
- whether Logweir sends fixes to a supplier that sells the competing product (recommended: every accepted fix removes a patch Logweir would otherwise carry);
- MSK IAM's place under OD-4.

## 9. PROD-00.3 child rows: routes for the ledger's rows, and proposed rows

PROD-01.1 (Done, main `0cd7cca0`) added **PROD-00.3a–e** to the ledger. Their oracles are in `docs/to-do/decisions/PROD-01.1-record-semantics.md` §9, which measured each defect on the compose stack. This record gives each one a route, a cost and the supplier constraint. The route is a proposal, and OD-3 decides it. Their ledger dependency is "00.1; 00.2 for a patch route".

| Row (ledger) | Capability | Proposed route | Cost | Supplier constraint | Acceptance |
|---|---|---|---|---|---|
| PROD-00.3a Committed-only capture | C1 | **U** to `kafka-backup`: skip control batches, `isolation_level=1` on fetch and ListOffsets, drop aborted transactions; **F** if declined or not released within 30 days | ~2–4 days | none: a defect, not a Part-2 feature | the oracle, PROD-01.1 §9's TXN row (A-C1-1), with one additional compatibility predicate |
| PROD-00.3b Segment min/max record timestamps | C6 | **U**: additive `min_timestamp`/`max_timestamp`, selectors prefer them; F fallback | ~1–2 days | none (defect) | the oracle, PROD-01.1 §9's ts-pit (A-C6-1), with one additional compatibility predicate |
| PROD-00.3c Keep `LogAppendTime` through capture | C14 | **U** to `kafka-backup`: the engine already parses each batch header by hand (`C/kafka/fetch.rs:156-191`), so it can take the max timestamp and the type bit from the same header, with no `kafka-protocol` change (PROD-01.1's review L2). Upstream to `kafka-protocol-rs` is an alternative, not a prerequisite. F fallback | ~1 day | none (defect) | the oracle, PROD-01.1 §9's LAT row, under which FX-8 stops triggering (A-C14-1), with one additional compatibility predicate |
| PROD-00.3d Idempotent (or sequence-checked) restore produce | C5 | **U**: InitProducerId, per-partition sequences and epochs; F fallback | ~3–5 days | none (defect) | the oracle, PROD-01.1 §9's ack-fault row (A-C5-1) |
| PROD-00.3e Keep repeated header keys through capture and replay | C13 | **U** to `kafka-protocol-rs` (`Record.headers` becomes a list, a breaking change for that crate), then an engine bump; F (a vendored `kafka-protocol` patch) if declined. Also a Logweir change: phase 7 keys by the LAST `x-original-offset` (PROD-01.1 §9) | ~2–3 days + the crate's release | none (defect) | the oracle, PROD-01.1 §9's shapes row with Logweir's verdict (A-C13-1) |

Proposed new rows, lettered from **f**:

| Row | Title | Capability | Route | Cost | Supplier constraint | Depends on | Gate | Lab | Tier | Acceptance |
|---|---|---|---|---|---|---|---|---|---|---|
| PROD-00.3f | Move the pin to 0.22.0 | C15, C16, §4 | N (+ refresh): `OSO_REFRESH=1 OSO_TAG=v0.22.0` with `EXPECTED_REVISION` `cc10aa4a…` (digest, tarball, `.env`, Dockerfile); `doctor`'s pin; PROD-01.1's `CONTRACT_ENGINE`; the refusal of `http://` with `allow_http: false` in `render_storage_block` (every engine document), plus the phase-0 rule; a version-neutral ENGINE-PATHSTYLE message; the matrix pin rows | ~1–2 days | none | 00.1 | OD-3; P-3f-1 | compose | A | A-C15-1, A-C16-1, A-3f-1, P-3f-1; §4.2's runs on the new pin |
| PROD-00.3g | Engine-side restore checkpoint: honour `checkpoint_interval_secs`, hash without file paths, keep skipped segments' mappings | C4 | U (F fallback). The Logweir-side N part is not this row: per-execution paths and dropping the unread key are PROD-07.1's (A-C4-1, A-C4-3), and carrying the checkpoint between attempts is PROD-07.3's (A-C4-2) | ~2 days | none (defect) | 00.1; 00.2 for a patch route | OD-3 | compose | A | A-C4-4 |
| PROD-00.3h | Enforce the byte-rate limit | C12 | U | ~1 day | none (defect) | 00.1 | OD-3 | compose | B | A-C12-1 |
| PROD-00.3i | YAML record-filter rules (erasure, offset ranges, resume point) | C10, C11 | F | ~3 days + ~1 day | the seam is "for a commercial distribution"; masking and erasure are Part 2 | 00.1, 00.2 | OD-3 | compose | A | A-C10-1, A-C11-1, A-C11-2 |
| PROD-00.3j | OAUTHBEARER from YAML | C9 | F; MSK IAM X until OD-4 | ~3–4 days | plugin seam "not YAML-configurable"; SSO/OIDC is Part 2 | 00.2, 01.5 (listener) | OD-3 | compose | A | A-C9-1, A-C9-2 |
| PROD-00.3k | Engine-side manifest and offset ordering for continuous capture | C2, C3 | U | ~2–4 days | none (defect) | 02.3 choosing the engine | OD-3 | compose | A | A-C2-2, A-C3-2, A-C3-3 |
| PROD-00.3l | Topic ID capture in the manifest | C7 | F | ~2 days | a feature under this row's rule | 01.4 choosing the engine route, 00.2 | OD-3 | compose | A | A-C7-1 |
| PROD-00.3m | ApiVersions negotiation | C8 | U | ~2 days | none (robustness defect) | a matrix broker row failing on a floor | OD-3 | compose | B | A-C8-1, A-C8-2 |
| PROD-00.3n | Unattested import of operator-written archives | §7 | N (catalog). A catalog trust-model change, not an engine capability: the catalog admits only receipts verified against a trusted key. It needs its own owner decision, and it belongs with the PLAT-15.2 import and the PROD-09 lineage; it is listed here only because §7 found it | ~3 days | none | FX-1, PLAT-15.2 | a trust-model decision (owner) | compose | A | A-OSO-3; points marked unattested, never `Verified` |
| PROD-00.3o | Weekly archive-compatibility rows | §7 | N (CI) | ~1 day | none | FX-1 | — | compose | B | A-OSO-1, A-OSO-2, A-OSO-4 |

- **A-3f-1 (PROD-00.3f: the contract moves with the pin).** PROD-01.1 asserts its record-semantics contract only when the engine is `CONTRACT_ENGINE`. That constant is `e2e/tests/record_semantics.rs:413` on main at `632ea345`. On any other engine a row prints "outcome recorded, contract not asserted" (`contract_applies`, `:415-424`) and stays green.
  - Pass: the bump moves `CONTRACT_ENGINE` to 0.22.0, and a guard test asserts it equals the pinned engine version (the tag `scripts/extract-engine.sh` pins, which is also `doctor`'s pin). PROD-01.1's rows re-run on 0.22.0 with the contract asserted. Any difference from their 0.21.0 outcomes is recorded as a contract change in PROD-01.1's record, not absorbed.
  - Negative control: the old constant (`"0.21.0"`) on the new pin fails the guard test. Without the guard, every contract row on 0.22.0 would record its outcome, assert nothing, and stay green.
  - Fixture: `e2e/tests/record_semantics.rs` and its rows.
- **P-3f-1 (PROD-00.3f precondition, from run 36542777892).** The bump waits for two things:
  - the fixture race is closed: A-C20-2 has landed in PROD-01.1's and G-PITR's source topics. This is met on this branch (`d795bae4`, §4.5);
  - the matrix's `v0.22.0 | 3.7.1 | full` row has recorded `pass` on GitHub with PROD-01.1's rows included. This is pending the re-dispatch at this branch's tip.

  The red row is not an engine finding (§4.4, C20), so this does not block 0.22.0 on a defect. It makes the bump's CI evidence attributable: until the race is closed, a red 0.22.0 row cannot be told apart from an engine regression without the broker log. Pass: both conditions hold. Negative control: a 0.22.0 row that is red on a record-semantics row without deletions named in its reason is an engine finding, and it blocks the bump until it is explained.

## 10. Limits of this record

- **Local runs used amd64 emulation.** Every local run was on an arm64 host, with the engine running as the linux/amd64 image under emulation (`e2e/fixtures/engine-docker.sh`). Durations are not performance evidence.
- **One broker, and no transactional fixture here.** The compose stack is one combined KRaft broker, and no run in this record produces transactionally, duplicates headers or writes non-monotonic timestamps. PROD-01.1 has since measured C1, C5, C6, C13 and C14 (its record §2 and §5).
- **The fix round and the merge have not yet run on GitHub.** Run 36531786341 was green on all six rows at `7e4cd0b1`. The fix round changes how rows are recorded (`ff9aa14a`). The merge adds three suites to the full rows and changes the generated `.env` (§5.5). Their CI evidence is therefore the re-dispatch at the final tip. `crates/logweir/tests/engine_matrix.rs` pins the changes by executing the recording and verdict steps' own text, and §5.5's cycles ran the new rows locally.
- **This record's 4.3.1 evidence uses the pinned engine only.** Kafka 4.3.1 was probed (ApiVersions) and run with the pin: the demo drill, G-PITR and the full CI e2e command before the merge (§4.3), and the merged suite's new rows after it (§5.5, c9). All of it ran on one combined broker, and 0.22.0 ran on 3.7.1 only. PROD-01.5 owns the 3.9, 4.1 and 4.3 lines and has measured them with the pin (`docs/support-matrix.md`, "Broker versions").
- **§4.4's reproduction ran the engine through the docker route under emulation; CI runs it natively.** The mechanism is the broker's, not the engine's, and the CI broker log shows the same deletion before the engine ran. The run-level exposure before the fix (about one run in three) is estimated from one CI run's topic lifetimes, not measured over many runs.
- **The upstream forecasts are forecasts.** Whether upstream accepts a given PR, and the cost estimates, are forecasts from the code and the release history, not measurements.
- **Operator archives were reproduced, not collected.** They were reproduced by seeding with the same engine images the operators use; no archive was taken from a running operator. FX-1's snapshot failure is cited from the tracker, not re-run.

## 11. Class sweep owed (outside this row's ownership)

- `e2e/fixtures/manifests/0.19.2.json` carries `source_replication_factor`, `configurations` and `pruned`. Engine 0.19.2 never writes those: they were added in 0.20.0 and 0.21.0 (`git diff v0.19.2 v0.20.0` and `v0.20.0 v0.21.0 -- crates/kafka-backup-core/src/manifest.rs`). The fixture is therefore not writer bytes, the same provenance defect FX-1 names for the snapshot fixture. The real 0.19.2 manifest from cycle c6 confirms it: its topics carry only `name`, `original_partition_count` and `partitions`, and its segments no `sha256`. It is saved at `runs/c6-v0.19.2-k3.7.1-below/archive-manifest.json` to replace the fixture.
- `L/crates/logweir-core/src/destination.rs:462-477`, `L/crates/weirkeeper/src/destination.rs:531-535` and `L/crates/logweir-api/src/routes/destinations.rs:756-763` name "engine 0.21.0" in the ENGINE-PATHSTYLE refusal. On a bump, the message should not name a version (PROD-00.3f).
- `L/crates/logweir-engine-oso/src/render_restore.rs:147` renders `checkpoint_interval_secs`, a key the engine never reads (C4, A-C4-3).
- FX-6's disclosure (in `docs/verify-a-scorecard.md`, `docs/stability.md` and the restore review screen) should add C13 (duplicate header keys collapse) and C14 (LogAppendTime topics are archived with producer timestamps) beside the transaction and non-monotonic-timestamp hazards it already names.
- **The source-fixture retention race (§4.4, A-C20-2) is done on this branch** (`d795bae4`), as an out-of-ownership edit the orchestrator authorized in round 4 (§4.5). It stays listed because the files belong to PROD-01.1 and G-PITR: `e2e/tests/harness/mod.rs` (the helper), `e2e/tests/record_semantics.rs` (`Row::source_topic` and the recreate row), `e2e/tests/pitr_boundary.rs`, and the new guard `e2e/tests/fixture_retention.rs`. PROD-01.1's precondition assertion is not engine-gated; that was the orchestrator's ruling.
- `docs/quickstart.md` Path 3 invited any compatible producer's archive without saying that a pre-0.21 archive drills as `fail-integrity`/`partial` (exit 2). The fix round added one sentence there and a link to `support-matrix.md`, an out-of-ownership edit (review L8).

## 12. PROD-00.3f: the move to 0.23.3 (2026-10-07)

OD-3 (decided 2026-10-07) re-targeted PROD-00.3f from 0.22.0 to the newest OSO release. This section is that row's evaluation, from source first and then on compose slot 4, and the record of what the bump changed in Logweir. Branch `claude/prod-00-3f`, from main `fcaae178`.

**Citation form here.** `C23/<path>:<line>` is `crates/kafka-backup-core/src/<path>` in `third_party/kafka-backup-v0.23.3.tar.gz`, the tarball the tree now vendors. `L/<path>` is this repository at the branch tip.

### 12.1 The target

| Release | Tag commit | Published (GitHub release) | Image (`linux/amd64`, pulled 2026-10-07) | Revision label |
|---|---|---|---|---|
| v0.23.3 | `afb160e7f2c69b7c3c28e1b868dd952835a5b0af` | 2026-10-07T12:25:31Z | `sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db` | equals the tag commit |
| v0.23.2 | `7708fc0a56ece8d5ac8b44e77004be75ec1b1749` | 2026-10-06T15:12:42Z | `sha256:5e5532f65b32a45cbec8efc28c03e8f23556f0dd64ff7fe4f3384d27da7f26e5` | equals the tag commit |
| v0.23.1 | `3746e993693d4f9dce3994ab41b01c57f582e0fc` | 2026-10-06T11:05:14Z | `sha256:a4f5ba4d93149f4ba86f92e94258cad716f9816659ca0662fd7a6bdde6de68cf` | equals the tag commit |
| v0.23.0 | `ea573a3e161f964c7abc160f724e97149ffb4d14` | 2026-09-29T09:28:28Z | `sha256:828c62e970464b8c9a449293c78276488bb4a9c2a5f5f014c19fe4edaeb4ac9c` | equals the tag commit |
| v0.22.0 | `cc10aa4ada2ab11fcd8679c01aac13d7b5139949` | 2026-09-07 | §1 | §1 |

- **The choice is v0.23.3**, the newest release on 2026-10-07 (`git ls-remote --tags`, the releases API). No newer tag appeared while this row ran.
- Its image is still `linux/amd64` only (an OCI index with one platform and one attestation manifest), so C19 is unchanged. `kafka-backup --version` prints `kafka-backup 0.23.3`.
- The vendored source is GitHub's tag archive, sha256 `bf5544bd521f0a0f343c402bbbde5d6dc0d9b45d70eb1a1efb447ca4f2fda0bd`, byte-identical in content to a clone of the tag (`diff -r`, no difference).
- `kafka-protocol` is still 0.18.0 in the engine's `Cargo.lock`; `object_store` moved from 0.14.1 to 0.14.2.
- **The operators moved too.** `kafka-backup-operator` v1.4.0, v1.4.1 and v1.4.2 (2026-10-06/07) link `kafka-backup-core` 0.23.0, 0.23.1 and 0.23.3. `strimzi-backup-operator` v0.4.0 (2026-10-06, tag commit `e804c9ff`) still defaults to `osodevops/kafka-backup:v0.22.0`. §7's table is otherwise unchanged.

### 12.2 What changed between 0.21.0 and 0.23.3, and what it means for Logweir

0.21.0 → 0.22.0 is §4.1. 0.22.0 → 0.23.3 is four releases (`git diff --stat v0.22.0 v0.23.3`: 55 files, +5810/−652; the diff is the artifact `upstream-v0.22.0..v0.23.3.diff`). The files that hold every behaviour Logweir depends on are byte-identical across the whole span 0.21.0 → 0.23.3: `segment/` (format, reader, writer), `kafka/fetch.rs`, `kafka/produce.rs`, `restore/preflight.rs`, `restore/filter.rs`, `restore/repartition.rs`, the CLI's `commands/{config,backup,restore,validate_restore}.rs` and the upstream `Dockerfile`.

| Change (release) | Source at v0.23.3 | Effect on Logweir |
|---|---|---|
| `backup.circuit_breaker` / `restore.circuit_breaker` YAML keys, advisory only: the engines record successes and failures and never gate a request on the breaker (0.23.0, #197) | `C23/config.rs:424-484`, `:637`, `:1073`; `C23/circuit_breaker.rs:1-12` | Logweir renders neither key, so the defaults (5 / 30 s / 2) apply, as before. The field is serialised in `RestoreOptions`, so the restore checkpoint hash (`restore_config_hash`, `C23/restore/engine.rs:2134-2143`) differs from a 0.22.0 hash over the same document. Logweir never resumes a restore (C4: per-run paths), so nothing reads that hash across versions |
| The restore-stall fix: the offset mapping is updated once per segment and once per produce batch instead of once per record under a shared mutex; detailed mappings stay sorted (0.23.0, #197) | `C23/manifest.rs:834-894`; `C23/restore/engine.rs:1774-1813`, `:1872-1907` | The offset report (`offsets.json`) is stored and hashed by Logweir, never parsed (`L/crates/logweir/src/drill/phase8_score.rs:351-371`). One byte-level difference is possible: for records whose timestamps are not monotonic within the first segment, `first_timestamp` is now the segment's minimum rather than the first record's. The pairs and their order are unchanged for an unfiltered restore |
| `partition_router.rs`: a connection error evicts only the pool of the broker the failed request went to; NOT_LEADER refreshes metadata and keeps every pool; a concurrent rebuild keeps an installed full pool (0.23.0, #197). Group-coordinator routing moved to `consumer_groups.rs` (0.23.3, #224) | `C23/kafka/partition_router.rs:259-365` (`get_broker_connection`, `evict_failed_broker`, `route`), `:566-642` (produce: `MAX_CONNECTION_RETRIES = 5`, `MAX_LEADER_RETRIES = 20`, unchanged), `:1018-1047` | Fewer reconnects on a multi-broker target; one broker on compose sees none of it. A produce that fails with a connection error is still re-sent without a producer id or sequence (`C23/kafka/produce.rs:79-110` unchanged), so **C5 is unchanged** |
| OffsetFetch and OffsetCommit go to the group's coordinator (FindCoordinator on NOT_COORDINATOR, 12 attempts); `fetch_offsets` fails on a group-level error instead of returning "no offsets"; `snapshot-groups` refuses to save an incomplete snapshot; validation lists groups on every broker (0.23.3, #224) | `C23/kafka/consumer_groups.rs:202-254`, `:457-555`; `C23/kafka/client.rs:62-66`, `:121-145` | The backup engine's consumer-group snapshot keeps its shape: `snapshot_time` plus `groups[].offsets` as topic → partition → offset (`C23/backup/engine.rs:903-988`, structs at `:926-936`). A group whose offsets cannot be read is skipped with a `warn` (`C23/kafka/partition_router.rs:824`; it was a `debug`). So **FX-1's parser and its drift gate are unchanged**, and on a multi-broker cluster the snapshot is now complete where 0.22.0 silently dropped groups coordinated elsewhere. Logweir renders `consumer_group_snapshot` off and never runs `snapshot-groups` |
| Phase 3 offset reset translates explicit `consumer_groups` through `topic_mapping` (0.23.1, #214) | `C23/restore/offset_reset.rs`, `C23/restore/three_phase.rs:372-376` | None: Logweir renders `reset_consumer_offsets: false`, `consumer_group_strategy: skip` and `auto_consumer_groups: false` (`L/crates/logweir-engine-oso/tests/snapshots/render__restore_yaml.snap`) |
| `offset-rollback`, `offset-reset`, `show-offset-mapping`, `status` honour storage URLs in `--path` (0.23.2, #174); `--help` text | `crates/kafka-backup-cli/src/commands/storage_path.rs`, `main.rs` | None: Logweir runs `backup`, `restore` and `validate-restore` only, whose arguments and handlers are unchanged |
| "Created S3/Azure/GCS backend" moves from `info` to `debug` and names the effective endpoint (0.23.2) | `C23/storage/s3.rs:94-112` (the log line at `:109-112`) | None: the only engine line Logweir parses is "Ignoring unknown config key" (`L/crates/logweir-engine-oso/src/subprocess.rs:160-190`), still emitted by `crates/kafka-backup-cli/src/commands/config.rs:46` |
| `p256` 0.14 and a routine dependency refresh (0.23.0) | `C23/evidence/signing.rs`, `envelope.rs` (test code only) | None: Logweir never consumes engine evidence (C17) |
| `storage/config.rs` and `storage/mod.rs` | unchanged since 0.22.0 (`C23/storage/config.rs:116-118`, `C23/storage/mod.rs:56-73`) | **C15 still applies on the new pin**, so its guard lands with the bump (12.4) |
| `use_path_style(endpoint, path_style) = path_style \|\| endpoint.is_some()` | unchanged since 0.22.0 (`C23/storage/s3.rs:55-57`, applied at `:78-80`) | **C16 still applies**: VirtualHosted addressing with a custom endpoint stays refused, with a version-neutral message (12.4) |
| The manifest | `BackupManifest`, `TopicBackup`, `PartitionBackup` and `SegmentMetadata` unchanged since 0.22.0 (`missing_topics`, skipped when empty) | The vendored shapes still read it; the drift gate passes against the new tarball (12.4) |

### 12.3 The capability rows on 0.23.3

**No row moves to "fixed upstream".** In particular, none of C1, C4, C5, C6, C12, C13 or C14 is fixed by 0.23.3, so none of 00.3a–e, 00.3g or 00.3h gets smaller:

| Row | Evidence at v0.23.3 |
|---|---|
| C1 control records, READ_COMMITTED | `kafka/fetch.rs` is byte-identical to 0.21.0: `with_isolation_level(0)` at `C23/kafka/fetch.rs:53`, `:263`, `:337`; no reference to `aborted_transactions` in the crate |
| C4 restore checkpoint | Saved once per topic (`C23/restore/engine.rs:936`), shutdown seen between topics (`:907`); `checkpoint_interval_secs` is parsed (`C23/config.rs:944-945`) and still read nowhere under `restore/`; the hash covers the whole `RestoreOptions` (`:2134-2143`), now including `circuit_breaker` |
| C5 idempotent produce | No InitProducerId; `NO_PRODUCER_ID` at `C23/kafka/produce.rs:99`; the router still re-sends on a connection error (`C23/kafka/partition_router.rs:566-627`). PROD-01.1's ack-fault row ran on 0.23.3 (12.5) |
| C6 min/max segment timestamps | `segment/writer.rs` byte-identical to 0.21.0 (first/last record, `:236-242`); `overlaps_time_window` at `C23/manifest.rs:398` unchanged |
| C12 byte-rate limit | `rate_limit_bytes_per_sec` parsed (`C23/config.rs:908`) and only checked for zero (`:1368-1373`) |
| C13 duplicate header keys | `kafka-protocol` 0.18.0 unchanged; the produce path still rebuilds an `IndexMap` (`C23/kafka/produce.rs:84`) |
| C14 LogAppendTime | `kafka/fetch.rs` byte-identical: no batch max timestamp, `TimestampType::Creation` on produce |

The other rows: C2 (`C23/backup/engine.rs:1396-1399`), C3 (`:1663`, `:1671`), C7 and C8 (`get_api_version` is byte-identical, `C23/kafka/client.rs:625-648`), C9 (`C23/config.rs:264`, `:328-336`), C10/C11 (`C23/config.rs:1059-1066`, `#[serde(skip)]`; 0.23.0 only keeps the filter's detailed mappings sorted), C17 (`C23/evidence/emit.rs:109`, `checksums_valid: true`) and C20 (`C23/backup/engine.rs:1249-1250`) are unchanged. C18 is unchanged in shape and better in completeness (12.2). C15 and C16 are unchanged upstream and closed in Logweir by this row.

### 12.4 What the bump changed in Logweir

- **The pin, by the refresh procedure.** `scripts/extract-engine.sh` defaults to `TAG=v0.23.3` with `EXPECTED_REVISION=afb160e7…`; `OSO_REFRESH=1` resolved the digest, verified the revision label, and rewrote `third_party/kafka-backup-binary.digest`, the `Dockerfile` engine stage and `e2e/compose/.env`. It re-fetched `third_party/LICENSE-MIT` (unchanged) and vendored the v0.23.3 tarball and its `.sha256`. The v0.21.0 tarball is removed, because xtask's drift gate requires exactly one.
- **`doctor`'s pin** is `logweir::doctor::ENGINE_PIN = "0.23.3"`, matched as a whole whitespace-delimited token. The adjacency test it replaces still accepted `0.21.0+anything`. The controller's `job::ENGINE_VERSION` and `ENGINE_DIGEST` moved with it.
- **A-3f-1.** `CONTRACT_ENGINE` is `0.23.3`. It stays a literal, because the contract is what was measured on one engine. `crates/logweir/tests/engine_pin.rs`, in CI's workspace run and never in the `e2e` package engine-matrix runs with other engines (fix round, review H1), compares every place that names the pin with `ENGINE_PIN`: the script's tag, the single tarball and its checksum, the controller's two constants, the Dockerfile, `CONTRACT_ENGINE`, `doctor`'s accepting fixture, the CronJob example and the quickstart's `export` lines. Each check is also run over a copy lagging to 0.21.0, which it must refuse.
- **C15 (A-C15-1).** `StorageUrl::plaintext_endpoint_without_allow_http` is the one predicate. `render_storage_block`, which the backup, restore and validate-restore documents all go through, returns `RenderError::PlaintextEndpointWithoutAllowHttp`; drill phase 0 and backup phase −1 refuse the same spec with exit 3 (`guard::reject_plaintext_endpoint_without_allow_http`). Neither message echoes the endpoint.
- **C16 (A-C16-1).** The ENGINE-PATHSTYLE refusal is the version-neutral `destination::ENGINE_PATHSTYLE_MESSAGE`, asserted digit-free in the core, controller and API tests.
- **The drift gate** (`cargo test -p xtask`) passes against the v0.23.3 tarball with no new divergence.

### 12.5 Runs on 0.23.3

Compose slot 4 (`logweir-e2e-s4`, PROD-01.5), Kafka 3.7.1 read back from the running container (`apache/kafka:3.7.1`, image `sha256:ed74d7d1…`, "Kafka version: 3.7.1"), the engine image above through `e2e/fixtures/engine-docker.sh` under `linux/amd64` emulation. Artifacts under `claude/artifacts/prod-00-3f/runs/`.

| Cycle | What ran | Result |
|---|---|---|
| c1 | `just e2e-up`; `scripts/e2e-seed.sh` (digests `required`); `cargo build --locked -p logweir`; CI's e2e command, `cargo test --locked -p e2e --features e2e -- --test-threads=1 --skip a_pod_really_reaches_the_k8s_listener` | Exit 0; the cycle took 25.8 min from the build to the last suite: **177 passed, 0 failed, 19 ignored**. `record_semantics` 8 passed, 2 ignored, **with the contract asserted on 0.23.3**: every outcome file names engine 0.23.3, which equals `CONTRACT_ENGINE`, so `contract_applies` held and the assertions ran (the "contract not asserted" line is a passing test's captured stderr, so its absence from a log is not the evidence); `pitr_boundary` 1 (G-PITR: six of nine, both readers VALID); `full_drill` 15, the deleted-segment control included; `consumer_group_snapshot` 2 (FX-1: a snapshot the 0.23.3 engine wrote parses and the drill passes); `backup_argv` 8 (FX-7: a backup over a set an earlier run wrote is refused before the engine); `mvp_demo` 3 (the receipt path); `topic_identity` 52 (1 ignored); `scram` 9; `stack_params` 17; `guards` 22; `offset_side` 4. The broker's time retention deleted no segment (`engine-matrix-broker.sh --retention`) |
| c2 | PROD-01.1's two `#[ignore]`d rows, each alone | Ack fault: one produce request timed out after 60 s and was resent; 1,000 duplicates (p0@19000–19999); Logweir signed `fail-integrity`, exit 2, on the count bound (61,000 against 60,000). Kill: the engine container outlived `logweir` and wrote all 60,000 records. PROD-01.1 §5.1 sample 6 and §5.2 sample 5 |
| d1 | A fresh stack, seeded by the pin; `scripts/demo.sh` steps 4–6 with `target/debug/logweir` | `doctor` all ok (`engine version kafka-backup 0.23.3`); `outcome: pass`, `integrity: byte-fingerprint/pass` 150/150, `header_preflight: honoured`, objectives met (RTO 8 s, RPO 14 s); `logweir drill verify` and `docs/verify_scorecard.py` VALID. The 0.23.3 manifest has the same top-level keys as a 0.21.0 one (no `missing_topics`), six segments, every one with a sha256 |
| d2 (upgrade) | A fresh stack seeded by **0.21.0** (the shell's `OSO_DIGEST` overrides `.env` for compose only), then drilled by the pin | `pass`, 150/150, VALID in both readers. That 0.21.0 wrote this archive shows in its seed log: the INFO line "Created S3 backend", which 0.23.x logs only at debug (`C23/storage/s3.rs:109`) and the two pin-seeded cycles do not print |
| d3 (rollback) | A fresh stack seeded by the pin, then drilled by a **0.21.0** engine (a scratch copy of `engine-docker.sh` pinned to the old digest) | `doctor` FAIL "version mismatch: expected 0.23.3, engine reports `kafka-backup 0.21.0`", as it must; the drill itself `pass`, 150/150, VALID in both readers. An archive the new pin writes is restorable by the old engine |
| C15 | Negative control 2 of A-C15-1 on the pin: `docker run --network none` with §3.15's `c15-allow-http/restore.yaml` (`endpoint: "http://127.0.0.1:1"`, `allow_http: false`) | 0.23.3 logs "storage.endpoint uses http://; enabling allow_http" and makes ten plaintext "transport error of kind Connect" attempts; 0.21.0 stops with "HTTP error: builder error" before any connection. The guard is load-bearing on the pin (artifact `c15-allow-http/`) |
| c3 | The same compose slot on **Kafka 4.3.1** (`stack-env.sh --kafka 4.3`, read back as `apache/kafka:4.3.1`, image `sha256:77e3df90…`): seed, then `record_semantics`, `pitr_boundary`, `consumer_group_snapshot`, `backup_argv` and `full_drill` | Exit 0: 34 passed, 0 failed, 2 ignored. `record_semantics` 8 passed with the contract asserted, G-PITR 1, FX-1 2, FX-7 8, `full_drill` 15. Retention deleted nothing. This is the local twin of the declared `v0.23.3 × 4.3.1` row |


### 12.6 The acceptance rows of PROD-00.3f

| Row | Status | Evidence |
|---|---|---|
| A-C15-1 | **pass** | One `RenderError::PlaintextEndpointWithoutAllowHttp` row per engine document (`L/crates/logweir-engine-oso/tests/transport_c15.rs`); drill phase 0 exits 3 with `refusal-reason=GuardRefused` (`L/crates/logweir/tests/guard_cli.rs`) and backup phase −1 refuses before any client exists (`L/crates/logweir/tests/backup_run.rs`). Negative control 1: the mutant that deletes the check in `render_storage_block` turns the renderer rows red (artifact `mutants.log`). Negative control 2: the differential in 12.5, row C15 |
| A-C16-1 | **pass** | On the 0.23.3 pin `VirtualHosted` with a custom endpoint is still refused as `addressing_unsupported_by_engine` / `AddressingUnsupportedByEngine`, and the message holds no digit (core, controller and API tests) |
| A-3f-1 | **pass** | `CONTRACT_ENGINE = "0.23.3"`; `crates/logweir/tests/engine_pin.rs` asserts it equals `logweir::doctor::ENGINE_PIN`, and refuses the old constant on the new pin (in-file control and mutant). PROD-01.1's rows re-ran on 0.23.3 with the contract asserted on two broker lines (12.5, c1 and c3); no difference from their 0.21.0 outcomes, recorded in PROD-01.1's record §11 |
| P-3f-1 | **met** (fix round, 2026-10-08) | The fixture race is closed (A-C20-2, §4.5), and no retention deletion occurred in any run here. engine-matrix run 37728540932, dispatched at `aa46f18d`, recorded `pass` for `v0.23.3 × 3.7.1` and `v0.23.3 × 4.3.1` with PROD-01.1's rows included (record_semantics 8 passed, 2 ignored; each row's extract step printed `kafka-backup 0.23.3`, which equals `CONTRACT_ENGINE`, so the contract was asserted). The same run's `v0.21.0` and `v0.22.0` rows recorded `fail(e2e suite)`: this row's pin guard then lived in `e2e/tests/` and failed on the row's own digest, and cargo stopped before the drill suites (review H1). Not an engine finding: the guard moved to `crates/logweir/tests/engine_pin.rs`, `no_e2e_test_compares_the_engine_with_a_committed_pin` keeps pin statements out of the package the matrix runs, and the floor row's suite ran locally the way the matrix runs it (12.9). The run must be re-dispatched green at the fix tip (12.7) |

The ENGINE-PATHSTYLE item of §11's class sweep is done by this row (12.4).

### 12.7 engine-matrix after the bump

| Engine | Kafka | Floor | Declared outcome | Why this row |
|---|---|---|---|---|
| v0.23.3 | 3.7.1 | full | `pass` | The pin, on the compose stack's default broker |
| v0.23.3 | 4.3.1 | full | `pass` | The pin on the newest supported Apache Kafka line: the C8 tripwire |
| v0.22.0 | 3.7.1 | full | `pass` | The default of `strimzi-backup-operator` v0.3.0–v0.4.0 |
| v0.21.0 | 3.7.1 | full | `pass` | The full-drill floor, and the previous pin |
| v0.20.0 | 3.7.1 | below | `unsupported(lever-absent)` | The fourth-newest minor |
| v0.19.2 | 3.7.1 | below | `unsupported(lever-absent)` | `kafka-backup-operator` 1.3.0's library version |
| v0.19.1 | 3.7.1 | below | `unsupported(lever-absent)` | `strimzi-backup-operator` v0.2.22–v0.2.25 default |

- **Retired:** `v0.21.0 × 4.3.1`. It was the pin's tripwire on the newest broker. The engine's protocol-version table is byte-identical in 0.21.0 and 0.23.3 (`C23/kafka/client.rs:625-648`), so the pin's own 4.3.1 row watches exactly what it watched, and 0.21.0 is no longer what Logweir ships. `the_declared_rows_follow_the_documented_floor` now requires the pin itself on a broker newer than the default, so the tripwire cannot be left behind on an old engine again.
- **Kept:** every other row. v0.22.0 stays as an operator default, and v0.21.0 × 3.7.1 as the floor.
- `publish` expects 7 rows (`--expect 7`). PROD-01.1's contract rows assert on the v0.23.3 rows and record outcomes on the others (`CONTRACT_ENGINE`).
- **The dispatch** is the orchestrator's, after the branch is pushed. `workflow_dispatch` takes no inputs:

  ```sh
  gh workflow run engine-matrix.yml --repo VladyslavHaina/logweir --ref claude/prod-00-3f
  gh run list --repo VladyslavHaina/logweir --workflow engine-matrix.yml --branch claude/prod-00-3f --limit 1
  gh run watch <run-id> --repo VladyslavHaina/logweir
  gh run view <run-id> --repo VladyslavHaina/logweir --log-failed
  ```

  Expected: seven `matrix` jobs, each ending `recorded '<outcome>' as declared`; `publish` green with seven rows; `open-pr` skipped (not `main`). P-3f-1's second condition (a recorded `pass` for the new pin's row on GitHub with PROD-01.1's rows included) is met by this run's two v0.23.3 rows and by nothing earlier.

### 12.8 Limits of this section

- **Local runs used amd64 emulation** on an arm64 host, as in §10. Durations are not evidence.
- **One broker per run.** The compose stack is one combined KRaft broker. 0.23.0's router changes (scoped pool eviction, NOT_LEADER without eviction) and 0.23.3's coordinator routing act on multi-broker clusters; none of them was exercised by a run here. The `cluster3` profile can, and the ack-fault fixture PROD-07 owns is where a router difference would show.
- **0.23.0–0.23.2 were read, not run.** Only 0.23.3 ran.
- **A race in an existing e2e row can still redden a matrix row:** `guards::a_logappendtime_broker_accepts_or_refuses_a_per_topic_override` misread a just-created topic's empty DescribeConfigs answer as a refusal once in two runs (12.9). It is engine-independent and outside this row.
- **engine-matrix ran once on GitHub** (run 37728540932 at `aa46f18d`: both v0.23.3 rows and the three below-floor rows as declared, the v0.21.0 and v0.22.0 rows red on the misplaced pin guard). The re-dispatch at the fix tip is pending.

### 12.9 Fix round (2026-10-08): the matrix's non-pin rows run their suites

Review H1 found the pin guard (then `e2e/tests/engine_pin.rs`) inside the package `engine-matrix` runs on every row after writing the ROW's digest into `third_party/kafka-backup-binary.digest`. Run 37728540932 recorded its v0.21.0 and v0.22.0 rows `fail(e2e suite)` that way, and cargo stopped before their drill suites. The guard now lives in `crates/logweir/tests/engine_pin.rs`, and `crates/logweir/tests/engine_matrix.rs`'s `no_e2e_test_compares_the_engine_with_a_committed_pin` keeps every pin statement out of `e2e/tests/`.

**The proof, run the way a matrix row runs** (compose slot 4, Kafka 3.7.1 read back; artifacts `runs/f1a-*` and `runs/f1b-*`). The workflow's "Extract this engine and point the stack at it" step was run verbatim for v0.21.0: the 0.21.0 binary into `.engine/`, `OSO_DIGEST` alone into `e2e/compose/.env`, and the 0.21.0 digest into `third_party/kafka-backup-binary.digest`. Then `just e2e-up`, the seed (`required`, no fixture refresh), `cargo build --locked -p logweir` and CI's e2e command. The pin was restored afterwards with `git checkout` and `scripts/extract-engine.sh`.

| Attempt | Result |
|---|---|
| f1a | No pin guard ran (the package has none). The suite stopped at `guards::a_logappendtime_broker_accepts_or_refuses_a_per_topic_override`: "DescribeConfigs answered this visible topic with no configuration … the principal lacks DescribeConfigs on it", read straight after the probe topic was created. The engine plays no part in that row, which passed on the next attempt and in c1 and c3. It is a race in an existing row (a topic described before the broker's config view holds it is read as a refusal), not an engine or pin finding; §12.8 and the report list it |
| f1b | **Exit 0: 169 passed, 0 failed, 20 ignored** across 17 test targets (c1's 177 less the 8 pin-guard tests that left the package), `guards` 22, `full_drill` 15, `record_semantics` 8 with every outcome file naming engine 0.21.0 (recorded, not asserted, by design), `pitr_boundary` 1, `consumer_group_snapshot` 2, `backup_argv` 8, `topic_identity` 52. Retention deleted nothing |

So the floor row's suites run and pass with the row's own engine in the digest file; the v0.22.0 row differs only in the digest. The re-dispatch at the fix tip is the orchestrator's (the report names the commit).

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
