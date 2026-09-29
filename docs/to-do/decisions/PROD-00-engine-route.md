# PROD-00.1 — Engine route per capability

- Row: PROD-00.1 (research, Tier B, lab `compose`), [product-expansion tracker](../product-expansion.md).
- Status: **proposed**. The routes below are a recommendation. [OD-3](../product-expansion.md#owner-decisions) is the owner's, and nothing here decides it.
- Date: 2026-09-28. Branch `claude/prod-00-1`, from main `adee0a16`.
- Kind: research. Source first, then runs on the e2e compose stack. The row repaired `engine-matrix` and corrected the support docs; it changed no product code, no engine pin and nothing in `third_party/`.

## 0. Decision summary

1. **Stay on the 0.21.0 pin until OD-3 is recorded, then move to 0.22.0 (proposed child row PROD-00.3f).** 0.22.0 is one squash commit over 0.21.0. It does not change the segment format, the three subcommands Logweir runs (`backup`, `restore`, `validate-restore`), or any key Logweir renders. On the compose stack it passes the demo drill, G-PITR, the receipt path and the full CI e2e command (64 of 64), and both verifiers accept its evidence (§4, §5.4). Two of its changes need Logweir work before a bump:
   - its `path_style` fix does **not** lift ENGINE-PATHSTYLE, because a custom endpoint still forces path-style;
   - it newly derives plaintext HTTP from an `http://` endpoint even when `allow_http: false` is rendered.
2. **Every capability gap has a route** (§3). The engine's Kafka and archive correctness defects go **upstream first**; this is bug-class work that `docs/OSO_Feature_Gate_PRD.md` does not gate. Each one is carried as a patch on the PROD-00.2 source build when upstream declines or stalls. The engine's gated seams go to a **patch or a Logweir-native path**, never to an upstream feature PR: the programmatic record filter, and SASL plugins for OAUTHBEARER and MSK IAM. Offsets, ACLs, verification and transport safety stay **Logweir-native**. MSK IAM, the engine's own evidence reports and its continuous/offset-store modes are **declared unsupported** for now.
3. **One patch serves three dependents.** Exposing the engine's existing Keep/Drop/Tombstone record filter in YAML, with rules keyed by partition, offset range and record key, gives:
   - PROD-09.3 its erasure ledger;
   - PROD-11.1 offset-range restore;
   - PROD-07.3 a resume point.

   That makes it the cheapest route to all three (C10/C11).
4. **`engine-matrix` is repaired** (commit `d5d0be9b`). It failed all three scheduled runs for seven independent reasons (§5.1). It now declares six rows, each with an expected outcome, and is green only when every row records what it declares. The orchestrator dispatches it on this branch (§5.4).
5. **The support docs were wrong about the operators.** `strimzi-backup-operator` has defaulted to engine v0.22.0 since its v0.3.0 (2026-09-07). `kafka-backup-operator` 1.3.0 links `kafka-backup-core` 0.19.2 as a library. [support-matrix.md](../../support-matrix.md) and [stability.md](../../stability.md) now say so (§6).
6. **OSO-operator archives:**
   - 0.21 and 0.22 archives drill fully;
   - 0.19.x and 0.20.x archives drill as `outcome: fail-integrity` with `integrity.result: partial`, because they carry no segment digests (measured on a 0.19.2 archive; both readers accept the signed result);
   - any archive with a non-empty consumer-group snapshot fails the drill until FX-1;
   - none can be imported into the catalog, because none carries a Logweir receipt.

   §7 states exactly what remains after FX-1, as acceptance rows.
7. **Two defects sit in the protocol crate the engine uses** (`kafka-protocol` 0.18), below anything Logweir's drill can see:
   - a repeated header key keeps only its last value (C13);
   - a LogAppendTime batch is archived with the producer's timestamps rather than the append times consumers read (C14).

   PROD-01.1 has since measured the second (FX-8), and the ledger carries both as PROD-00.3c and 00.3e. C14 is a small engine fix; C13 needs a `kafka-protocol-rs` change.

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

**Citation form.** `C/<path>:<line>` is `crates/kafka-backup-core/src/<path>` in the pinned tarball. A file 0.22.0 did not touch has the same line in v0.22.0. For the four touched files that are cited below (`backup/engine.rs`, `config.rs`, `manifest.rs`, `storage/s3.rs`), the v0.22.0 line follows `→`. `KP/records.rs` is `src/records.rs` in `kafka-protocol` 0.18.0. `L/<path>` is this repository at `adee0a16`.

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
| C8 | ApiVersions negotiation | Never sends ApiVersions; fixed versions per key, `_ => 0` for the rest. Every version it sends lies inside Kafka 4.3.1's ranges; the three ACL APIs sit at the 4.x floor (v1) | **X** today (not needed on 4.3); **U** when a broker line raises a floor; the matrix broker row is the tripwire | U ~2 days | none (robustness defect) | PROD-01.5, 01.2, 04.0 |
| C9 | OAUTHBEARER and MSK IAM | YAML offers PLAIN, SCRAM-SHA-256/512 and GSSAPI; other mechanisms only through a programmatic plugin factory | **F** for OAUTHBEARER (static token file and OIDC client credentials); **X** for MSK IAM until OD-4 | F ~3–4 days; MSK IAM +3 days and a SigV4 dependency | seam marked "not YAML-configurable"; SSO/OIDC and secrets managers are Part 2 | PROD-01.3, 01.2 |
| C10 | YAML filter or transform action | Keep/Drop/Tombstone filter exists; settable only by embedding code (`#[serde(skip)]`); no transform action | **F** filter rules in YAML (Keep/Drop/Tombstone by partition, offset range, key); **N** for any transform (masking) | F ~3 days; N producer path is PROD-11.2's | masking and GDPR erasure are Part 2; the seam is "for a commercial distribution" | PROD-09.3, 11.2, 11.1, 07.3 |
| C11 | Offset-range restore | Time window and source partitions only | **F** through C10's rule set (Drop outside `[start, end)` per partition) | +~1 day on C10 | none directly; rides the C10 seam | PROD-11.1, 07.3, 04.2 |
| C12 | Byte-rate limits | `rate_limit_bytes_per_sec` parsed, never read; per-partition records/sec enforced on restore; no backup-side limit | **U** (enforce the documented key); N exposes only records/sec until then | ~1 day | none (defect) | PROD-10.1 |
| C13 | Duplicate header keys (found here) | `kafka-protocol` 0.18 decodes headers into an `IndexMap`, so a repeated key keeps only its last value, on capture and again on produce | **U** to `kafka-protocol-rs` (header list) + engine bump; **X** disclosed until then | ~2–3 days upstream, crate API change | none | PROD-00.3e, PROD-01.1, 08.1, 08.3 |
| C14 | LogAppendTime timestamps and producer metadata (found here) | For a LogAppendTime batch the decoder ignores the batch's max timestamp, so the archive keeps the producer's timestamps, not the append times consumers read; timestamp type, producer id/epoch, sequence and the transactional flag are dropped; restore always produces CreateTime | **U** to `kafka-backup` for LogAppendTime (the engine parses batch headers itself); **X** disclosed for producer metadata | ~1 day | none (defect) | PROD-00.3c, PROD-01.1, FX-6, FX-8, 08.1, 11.1 |
| C15 | Transport derived from the endpoint (0.22.0) | 0.22.0 treats an `http://` endpoint as `allow_http: true` even when `false` is rendered | **N** guard before the bump (PROD-00.3f) | ~0.5 day | none | PROD-00.3f, PLAT-08 seam S5 |
| C16 | `path_style` (ENGINE-PATHSTYLE) | 0.21.0 ignores `path_style`; 0.22.0 honours `path_style: true`, but any custom endpoint still forces path-style | **N** keep the refusal of VirtualHosted plus endpoint; U only if demanded | — | none | PLAT-08.1, PROD-09.2 |
| C17 | Engine evidence reports | `checksums_valid: true` is set unconditionally | **X** never consumed; verification stays N (PROD-08) | — | validation runs are Part 2 | PROD-08.x |
| C18 | Consumer-group snapshot | Written as `snapshot_time` + topic → partition → offset; drops groups without offsets on archived topics; Logweir's vendored shape differs (FX-1) | **N** (FX-1 parses it as an import source; PROD-04.1 captures natively) | FX-1 as scoped | automatic offset reset is Part 2 | FX-1, PROD-04.1, 04.2 |
| C19 | Build and architecture | Logweir copies OSO's amd64-only image binary (GR6) | **F** build from the vendored source (PROD-00.2) | PROD-00.2 as scoped | MIT permits it; GR6 amendment | PROD-00.2, every F row |

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

**Acceptance rows.**

- **A-C1-1 (PROD-01.1, fixture).** Fixture: PROD-01.5's transactional-producer profile (PROD-01.1 builds the producer). On a 3-partition topic it writes 300 committed and 90 aborted records, then back-to-back commit and abort markers, with a final transaction left open. Pass for the "support" rail: after backup and a new-topic restore, the multiset of (partition, key, value, headers) on the target equals the source read with `isolation.level=read_committed`, and no target record has the control-marker shape. Negative control: the same comparison against the 0.21.0 pin fails, with the 90 aborted records and the markers present. That run is also FX-6's missing measurement.
- **A-C1-2 (PROD-01.1, rail).** Until C1 is released, a restore plan whose archive contains control-marker-shaped records is refused or labelled "transactional semantics not preserved", as PROD-01.1 decides. It happens before the approval is minted and in the signed evidence. Negative control: the non-transactional demo archive carries no such label.
- **A-C1-3 (PROD-08.1).** Complete-mode verification over the A-C1-1 archive reports the aborted and control records as excluded or not reconstructed, never as a silent pass. Negative control: switch the exclusion off and complete mode must report the mismatch.
- **A-C1-4 (PROD-02.2 / 02.3).** An incremental or continuous capture chained over a topic with an open transaction never records a point beyond the LSO while READ_COMMITTED is in force. Negative control: under READ_UNCOMMITTED the recorded end offset exceeds the LSO.
- **A-C1-5 (PROD-00.3a).** The released engine, or the carried patch, passes A-C1-1, and the 0.21.0 pin still fails it (the same fixture, both ways).

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

**Acceptance rows.**

- **A-C5-1 (PROD-08.1).** Complete-mode verification reports duplicated `x-original-offset` values per partition. Fixture: a restore target with one batch produced twice, made with the e2e harness's producer. Negative control: the offset-keyed map of today reports none.
- **A-C5-2 (PROD-07.1 / 01.1).** Fault fixture: a TCP proxy that drops the broker's Produce response once. Pass with the released engine: no duplicate on the target. Negative control: with the 0.21.0 pin the dropped response yields exactly one duplicated batch.

### 3.6 C6 — Min/max segment timestamps

**Behaviour.** The writer sets `start_timestamp` from the first record and `end_timestamp` from the last (`C/segment/writer.rs:236-242`). `SegmentMetadata::overlaps_time_window` treats them as min and max (`C/manifest.rs:391-401` → `:398-408`). Every restore selection uses it: restore and dry run (`C/restore/engine.rs:547`, `:1961`), the header preflight (`C/restore/preflight.rs:263`) and repartitioning (`C/restore/repartition.rs:300`).

**Effect** (measured by PROD-01.1 §2.2). With non-monotonic CreateTime, a record inside the window can sit in a segment whose first and last timestamps are both outside it. That segment is skipped silently, which is FX-6's second hazard.

**Route.** **U**: record `min_timestamp` and `max_timestamp` additively, and have the selectors prefer them when present. Existing archives keep first/last and stay correct to read. **N**: PROD-08.1 already plans exact per-record filtering on the archive side. **X**: archives written before the fix remain first/last-bounded, and their evidence must say so.

**Acceptance rows.**

- **A-C6-1 (PROD-01.1 / FX-6).** Fixture: one partition, one segment, records with CreateTime 10, 5, 20. A window of [0, 8] must return the record stamped 5. Pass with the released engine. Negative control: the 0.21.0 pin returns nothing, and today's drill still passes, which is the disclosure FX-6 states.
- **A-C6-2 (PROD-08.1).** Complete-mode verification decodes the in-window segment and counts the record stamped 5 as expected, so a restore that omits it fails verification. Negative control: the first/last selector reports nothing expected.
- **A-C6-3 (PROD-11.1).** Preview and execution select the same records for an inclusive window over non-monotonic timestamps, and the preview names the rule it used.

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
  - SaslHandshake 1 (0–1), SaslAuthenticate 2 (0–2);
  - DescribeAcls, CreateAcls and DeleteAcls 1 (each 1–3).

**Effect.** No failure on 4.3.1 is predicted from source. The ACL APIs and SaslAuthenticate sit exactly at the 4.x floor or ceiling, so the next floor raise breaks them. The compose runs on 4.3.1 are in §4.3.

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

- `rate_limit_bytes_per_sec` is parsed (`C/config.rs:807-809` → `:837-839`) and never read anywhere else.
- `rate_limit_records_per_sec` is enforced per partition on restore (`C/restore/engine.rs:1816`, `C/restore/repartition.rs:420`).
- The backup path has no limit.

**Route.** **U**: enforce the documented key, a defect because a documented control does nothing. **N**: PROD-10.1 exposes only records/sec, with CEL bounds, until the byte limit is released.

**Acceptance rows.**

- **A-C12-1 (PROD-10.1).** A 50 MB restore with `rate_limit_bytes_per_sec: 1048576` takes ≥ 45 s. Negative control: the 0.21.0 pin finishes in a fraction of that, which proves the key is ignored today. Logweir renders the key only when the engine version honours it.

### 3.13 C13 — Duplicate header keys (found here)

**Behaviour.**

- `kafka-protocol` 0.18 stores record headers as `IndexMap<StrBytes, Option<Bytes>>` (`KP/records.rs:184`) and fills it with `insert` (`:896-919`).
- A repeated key keeps its first position and its last value, so `[(a,1),(b,2),(a,3)]` decodes as `[(a,3),(b,2)]`.
- The engine's own `BackupRecord.headers` is a `Vec` and could hold duplicates, but the decoder has already collapsed them. The produce path rebuilds an `IndexMap` too (`C/kafka/produce.rs:84-90`).

**Effect.** A header multiset is not preserved. The drill fingerprints target against archive, where both are collapsed, so it cannot see this.

**Route.** **U** to `kafka-protocol-rs` (a header list in `Record`, a breaking change for that crate), then an engine bump. **X** disclosed until then (PROD-01.1's contract). Logweir's own reader (librdkafka) preserves duplicates, so a source-versus-target comparison can detect the loss.

**Acceptance rows.**

- **A-C13-1 (PROD-01.1).** Fixture: records with headers `[(a,1),(b,2),(a,3)]`, produced with librdkafka. Pass for a support claim: the target, read with librdkafka, returns all three headers in order. Negative control: the 0.21.0 pin returns `[(a,3),(b,2)]`, so the contract states "duplicate header keys are not preserved".
- **A-C13-2 (PROD-08.3).** A full streamed comparison reads the SOURCE (when available) or the archive with a duplicate-preserving decoder, and reports a header-multiset mismatch. Negative control: an identical multiset reports none.

### 3.14 C14 — LogAppendTime timestamps and producer metadata (found here)

**Behaviour** (read from source, then measured by PROD-01.1 §2.3: a point in 2001 restored six records the broker appended in 2026, signed `pass`; FX-8 rails it).

- `kafka-protocol` 0.18 reads the batch's timestamp type (`KP/records.rs:572-573`) but decodes the batch's max timestamp into a discarded `_max_timestamp` (`:585`).
- It stamps every record `base + delta` (`:861-862`), whatever the type.
- For a LogAppendTime batch the broker sets only the batch's max timestamp and the type bit; the per-record deltas keep what the producer sent (Apache Kafka 4.3.1, `storage/.../log/LogValidator.java:248-250` and `:377-381`).
- A Java consumer reports the batch's max timestamp for every record of such a batch (`clients/.../record/internal/DefaultRecordBatch.java:580`, `DefaultRecord.java:321-322`).
- So the engine archives the **producer's** timestamps, not the append times consumers read.
- `convert_record` also drops the timestamp type, the producer id/epoch, the sequence and the transactional flag (`C/kafka/fetch.rs:200-218`).
- Restore always produces CreateTime (`C/kafka/produce.rs:101`).

**Effect.** A point-in-time restore of a LogAppendTime topic selects by producer time. With an unsynchronised or deliberately back-dated producer, that can differ from the append time the application and its consumers saw.

**Route.**

- **U** to `kafka-backup`. The engine already parses each batch header by hand (`C/kafka/fetch.rs:156-191`), and the max timestamp (bytes 35–43) and the timestamp-type bit (attributes, bytes 21–23) sit in the same header. It can therefore stamp a LogAppendTime batch's records with the batch max timestamp, as the Kafka client does, without any `kafka-protocol` change (PROD-01.1's review L2). A matching fix to `kafka-protocol-rs`'s decoder is optional hygiene, not a prerequisite.
- **X** disclosed for the producer metadata, revisited with C1 if transactional support needs producer identity.

**Acceptance row.**

- **A-C14-1 (PROD-01.1, 08.1, 11.1).** Fixture: a topic with `message.timestamp.type=LogAppendTime`, produced with CreateTime values one day in the past. After backup, pass for a support claim: the archive's record timestamps equal the append times a librdkafka consumer of the source reads. Negative control: the 0.21.0 pin archives the producer's back-dated values, so PROD-01.1's contract states that PITR on LogAppendTime topics selects by producer time until C14's U route is released.

### 3.15 C15 — Transport derived from the endpoint in 0.22.0

**Behaviour.**

- 0.22.0 adds `implied_allow_http(endpoint, explicit) = explicit || endpoint starts with "http://"` for both the YAML and the URL paths (`storage/config.rs:111-118` in v0.22.0).
- Logweir renders `allow_http` from the plan's transport alone (`L/crates/logweir-core/src/destination.rs:529-560`).
- Under 0.22.0, a rendered `allow_http: false` with an `http://` endpoint becomes plaintext in the engine, while Logweir's own store client still refuses it (`L/crates/logweir-store/src/lib.rs:385-411`).
- For destinations, rule R3 already makes that combination unrepresentable (`L/crates/logweir-core/src/destination.rs:337-357`). The CLI drill spec has no such rule.

**Route.** **N**, before a bump: the CLI's spec validation refuses `endpoint: http://…` with `allow_http: false`, the same rule as R3, so the engine is never handed a combination it now reinterprets.

**Acceptance row.**

- **A-C15-1 (PROD-00.3f).** A drill spec with `endpoint: http://…` and `allow_http: false` is refused at phase 0, exit 3, before any engine start. A unit test covers it, and the mutant that deletes the rule turns it red.

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

## 5. engine-matrix

### 5.1 Why every scheduled run failed

The three scheduled runs (34830737064 on 2026-09-14, 35586616823 on 2026-09-21, 36413265594 on 2026-09-28) failed identically. Seven defects, each independent of the others:

| # | Defect | Evidence (run 36413265594) | Effect |
|---|---|---|---|
| 1 | `scripts/e2e-seed.sh` demanded a sha256 on every segment | rows v0.20.0, v0.19.2, v0.19.1, v0.18.0: `segments with an empty sha256 (not a v0.21.0 manifest)`, exit 1 | Every row below the pin failed before a test ran; engines below 0.21 write no digest |
| 2 | The full-drill step had drifted from the CI e2e job | row v0.21.0: four `e2e/tests/scram.rs` tests failed with `invalid credentials`, because the matrix never ran `scram-setup`. A fifth, `a_pod_really_reaches_the_k8s_listener`, failed with `context "docker-desktop" does not exist` | The pinned engine could never be `pass` |
| 3 | `scripts/run-named-tests.sh` reported an existing test missing | row v0.21.0: `printf: write error: Broken pipe`, then `no test named a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard exists … 4042 test(s) were available`; the test is at `e2e/tests/full_drill.rs:129` | Under `pipefail`, `grep -q` exits at the first match and `printf` takes EPIPE once the listing outgrows the pipe buffer. The pin recorded `fail(lever-not-honoured)` |
| 4 | Rows were written to a hidden directory | every row: `No files were found with the provided path: .matrix/` (`include-hidden-files: false`, the `upload-artifact@v4` default) | `publish` found no rows: `no matrix rows were produced; refusing to blank the table` |
| 5 | A skipped control was classified as a failed one | row v0.20.0: control `skipped` after the seed failed, recorded as `fail(lever-not-honoured)` | A wrong verdict in the one outcome that detects an ignored lever |
| 6 | `publish` needed a pull request Actions may not open | `gh api repos/VladyslavHaina/logweir/actions/permissions/workflow` → `can_approve_pull_request_reviews: false` (2026-09-28) | `publish` would have failed at `create-pull-request` even with rows |
| 7 | The declared rows contradicted the documented floor, and a failing row was green | v0.20.0 and v0.19.2 were declared full-drill rows although the docs put them below 0.21.0. Row v0.21.0 showed ✓ with five failing tests, because every test step was `continue-on-error` and nothing compared the verdict with an expectation | A green matrix said nothing about the rows |

`publish` also rewrote the whole hand-written `## Rows` table of `docs/support-matrix.md`. That would have erased the recorded evidence, and PROD-01.5's broker rows, on its first green run.

### 5.2 The repair (commit `d5d0be9b`)

**Steps.**

- The matrix job sets the stack up and runs the suite exactly as the CI e2e job does: `just e2e-up`, then `cargo test --locked -p e2e --features e2e -- --test-threads=1 --skip a_pod_really_reaches_the_k8s_listener`.
- `KAFKA_VERSION` comes from the row, in the environment and in the generated `.env`. That is the variable `e2e/compose/docker-compose.yml` reads (`apache/kafka:${KAFKA_VERSION:-3.7.1}`) and PROD-01.5 parameterizes.
- Each tag resolves to a digest whose `org.opencontainers.image.revision` must equal the tag's commit (`git ls-remote`), the digest-to-commit binding `extract-engine.sh` asserts for the pin.

**Seed and lookup.**

- The seed runs with `LOGWEIR_SEED_REFRESH_FIXTURES=0`.
- For rows below the floor it runs with `LOGWEIR_SEED_SEGMENT_SHA256=optional`. The new mode relaxes only the absence of a digest (`scripts/seed-manifest-check.py`): counts and every present digest are still checked, and the mode is refused with a fixture refresh.
- `run-named-tests.sh` matches against the listing with here-strings.

**Recording and publishing.**

- The row line carries the broker, the digest, the outcome and a run link. `fail(seed)` and `fail(setup)` are distinct from `fail(lever-not-honoured)`.
- A final step fails the job unless the recorded outcome equals the row's `expect`.
- Rows go to `matrix-rows/`, and the upload fails if the directory is empty.
- `publish` is read-only. `scripts/engine-matrix-rows.py` renders the rows between `<!-- engine-matrix:rows:begin -->` and `<!-- engine-matrix:rows:end -->` and touches nothing else. It refuses missing or repeated markers, zero rows, a malformed row, a repeated (tag, broker) pair, or fewer rows than `--expect 6`. The page goes out as the `support-matrix` artifact and in the run summary.
- `open-pr` opens the pull request only on `main` and only when the repository variable `ENGINE_MATRIX_OPEN_PR` is `true`.

**Guards.** `crates/logweir/tests/engine_matrix.rs` holds 16 tests, and twelve mutants each turn a named test red (artifact `engine-matrix-guard-mutants.log`). One test sweeps every workflow for `upload-artifact` from a hidden path. `actionlint` 1.7.12, which runs shellcheck on every `run:` block, is clean.

### 5.3 Declared rows

| Engine | Kafka | Floor | Declared outcome | Why this row |
|---|---|---|---|---|
| v0.22.0 | 3.7.1 | full | `pass` | Upstream's current release; the proposed pin (PROD-00.3f) |
| v0.21.0 | 3.7.1 | full | `pass` | The pin, on the compose stack's default broker |
| v0.21.0 | 4.3.1 | full | `pass` | The pin on the newest supported Apache Kafka line (the `latest` image on 2026-09-28): the C8 tripwire |
| v0.20.0 | 3.7.1 | below | `unsupported(lever-absent)` | Newest four minors |
| v0.19.2 | 3.7.1 | below | `unsupported(lever-absent)` | `kafka-backup-operator` 1.3.0's library version |
| v0.19.1 | 3.7.1 | below | `unsupported(lever-absent)` | `strimzi-backup-operator` v0.2.22–v0.2.25 default |

v0.18.0 left the window: it is the fifth-newest minor, and no operator defaults to it. When PROD-01.5 settles its 4.3 patch release, the broker row should name the same one.

After integration onto main (`0cd7cca0` and later), the full rows also run PROD-01.1's `e2e/tests/record_semantics.rs`. Its contract assertions are gated on engine 0.21.0 (`CONTRACT_ENGINE`), so the v0.22.0 row records outcomes from it without red cells, while both pin rows assert the contract. On Kafka 4.3.1 those assertions have not run: this branch predates them, and PROD-01.1 measured on 3.7.1. If they fail there, the 4.3.1 row records `fail(e2e suite)`. That is real broker-line evidence for PROD-01.5, and the row's declaration should then follow the evidence.

### 5.4 Local validation and the dispatch command

The workflow's matrix steps were run locally, under the compose lock, with the same commands and environment, as cycles c5 (v0.22.0 × 3.7.1, full row), c6 (v0.19.2 × 3.7.1, below-floor row) and c8 (v0.21.0 × 4.3.1, full row). The `publish` path was run over the `Record this row` step extracted verbatim from the workflow, for six step-outcome combinations (artifact `publish-simulation/`). Each combination recorded its intended outcome (`pass`, `unsupported(lever-absent)`, `fail(seed)`, `fail(setup)`, `fail(e2e suite)`, `fail(lever-not-honoured)`), and `engine-matrix-rows.py` rendered all six, leaving the page byte-identical outside the markers.

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

- six `matrix` jobs green, each ending with `recorded '<outcome>' as declared`;
- `publish` green, with a `support-matrix` artifact whose generated section holds six rows;
- `open-pr` skipped, because the run is not on `main`.

A red row names what it recorded against what it declared.

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
- **Topic parity on 0.19.x archives compares nothing it can see.** Without `source_replication_factor` and `original_partition_count`, the target's own values stand in for the source's (`phase7_verify.rs:1175-1182,1212-1213`). Without `configurations`, no configuration is compared. The evidence reports "no divergence" for quantities the archive never recorded, which is FX-4's coverage gap. The c6 scorecard over the 0.19.2 archive signs `topic_parity: {"intentionally_deviated": [], "unexpected_divergence": []}`, the same text as a fully measured archive.
- **A non-empty `consumer-groups-snapshot.json` fails the drill** before a scorecard exists: the vendored shape does not match what the engine writes (FX-1). This is not re-run here; the tracker's FX-1 row is the evidence.
- **Nothing imports into the catalog.** `logweir catalog sync` and the console's "Connect an existing archive" read only `logweir/backups/**/*.receipt.json`, verified against a trusted key (`L/crates/logweir/src/catalog/cli.rs:368-393`, prefix at `catalog/record.rs:37`). An operator-written archive has no receipt, so a sync finds zero points. It is restorable only through the CLI drill path, never from the console.

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
| C15 transport derivation | — | — | CLI rule R3 | — | **N**, before the bump |
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
| PROD-00.3a Committed-only capture | C1 | **U** to `kafka-backup`: skip control batches, `isolation_level=1` on fetch and ListOffsets, drop aborted transactions; **F** if declined or not released within 30 days | ~2–4 days | none: a defect, not a Part-2 feature | PROD-01.1 §9 (the TXN row); A-C1-1…A-C1-5 |
| PROD-00.3b Segment min/max record timestamps | C6 | **U**: additive `min_timestamp`/`max_timestamp`, selectors prefer them; F fallback | ~1–2 days | none (defect) | PROD-01.1 §9 (ts-pit); A-C6-1…A-C6-3 |
| PROD-00.3c Keep `LogAppendTime` through capture | C14 | **U** to `kafka-backup`: the engine already parses each batch header by hand (`C/kafka/fetch.rs:156-191`), so it can take the max timestamp and the type bit from the same header, with no `kafka-protocol` change (PROD-01.1's review L2). Upstream to `kafka-protocol-rs` is an alternative, not a prerequisite. F fallback | ~1 day | none (defect) | PROD-01.1 §9 (the LAT row; FX-8 stops triggering); A-C14-1 |
| PROD-00.3d Idempotent (or sequence-checked) restore produce | C5 | **U**: InitProducerId, per-partition sequences and epochs; F fallback | ~3–5 days | none (defect) | PROD-01.1 §9 (the ack-fault row); A-C5-1, A-C5-2 |
| PROD-00.3e Keep repeated header keys through capture and replay | C13 | **U** to `kafka-protocol-rs` (`Record.headers` becomes a list, a breaking change for that crate), then an engine bump; F (a vendored `kafka-protocol` patch) if declined. Also a Logweir change: phase 7 keys by the LAST `x-original-offset` (PROD-01.1 §9) | ~2–3 days + the crate's release | none (defect) | PROD-01.1 §9 (the shapes row); A-C13-1, A-C13-2 |

Proposed new rows, lettered from **f**:

| Row | Title | Capability | Route | Cost | Supplier constraint | Depends on | Gate | Lab | Tier | Acceptance |
|---|---|---|---|---|---|---|---|---|---|---|
| PROD-00.3f | Move the pin to 0.22.0 | C15, C16, §4 | N (+ refresh): `OSO_REFRESH=1 OSO_TAG=v0.22.0` with `EXPECTED_REVISION` `cc10aa4a…` (digest, tarball, `.env`, Dockerfile); `doctor`'s pin; the CLI rule refusing `http://` with `allow_http: false`; a version-neutral ENGINE-PATHSTYLE message; the matrix pin rows | ~1–2 days | none | 00.1 | OD-3 | compose | A | A-C15-1, A-C16-1; §4.2's runs on the new pin |
| PROD-00.3g | Resumable restore checkpoint | C4 | N (per-execution paths, the checkpoint carried between attempts) + U (cadence, path-free hash) | N ~1 day; U ~2 days | none (defect) | 00.1, 07.1 | OD-3 | compose | A | A-C4-1…A-C4-4 |
| PROD-00.3h | Enforce the byte-rate limit | C12 | U | ~1 day | none (defect) | 00.1 | OD-3 | compose | B | A-C12-1 |
| PROD-00.3i | YAML record-filter rules (erasure, offset ranges, resume point) | C10, C11 | F | ~3 days + ~1 day | the seam is "for a commercial distribution"; masking and erasure are Part 2 | 00.1, 00.2 | OD-3 | compose | A | A-C10-1, A-C11-1, A-C11-2 |
| PROD-00.3j | OAUTHBEARER from YAML | C9 | F; MSK IAM X until OD-4 | ~3–4 days | plugin seam "not YAML-configurable"; SSO/OIDC is Part 2 | 00.2, 01.5 (listener) | OD-3 | compose | A | A-C9-1, A-C9-2 |
| PROD-00.3k | Engine-side manifest and offset ordering for continuous capture | C2, C3 | U | ~2–4 days | none (defect) | 02.3 choosing the engine | OD-3 | compose | A | A-C2-2, A-C3-2, A-C3-3 |
| PROD-00.3l | Topic ID capture in the manifest | C7 | F | ~2 days | a feature under this row's rule | 01.4 choosing the engine route, 00.2 | OD-3 | compose | A | A-C7-1 |
| PROD-00.3m | ApiVersions negotiation | C8 | U | ~2 days | none (robustness defect) | a matrix broker row failing on a floor | — | compose | B | A-C8-1, A-C8-2 |
| PROD-00.3n | Unattested import of operator-written archives | §7 | N (catalog) | ~3 days | none | FX-1, PLAT-15.2 | — | compose | A | A-OSO-3; points marked unattested, never `Verified` |
| PROD-00.3o | Weekly archive-compatibility rows | §7 | N (CI) | ~1 day | none | FX-1 | — | compose | B | A-OSO-1, A-OSO-2, A-OSO-4 |

## 10. Limits of this record

- **Local runs used amd64 emulation.** Every local run was on an arm64 host, with the engine running as the linux/amd64 image under emulation (`e2e/fixtures/engine-docker.sh`). Durations are not performance evidence.
- **One broker, and no transactional fixture here.** The compose stack is one combined KRaft broker, and no run in this record produces transactionally, duplicates headers or writes non-monotonic timestamps. PROD-01.1 has since measured C1, C5, C6 and C14 (its record §2 and §5). C13's duplicate-collapse half remains a source reading in both records.
- **The CI rows are not yet run on GitHub.** The engine-matrix repair was validated by running the workflow's steps locally (§5.4) and with `actionlint`. The first dispatched run on this branch is the CI evidence.
- **The 4.3.1 evidence is one broker line.** Kafka 4.3.1 was probed (ApiVersions) and run (demo drill, G-PITR, the full CI e2e command) with the pinned engine only, on one combined broker, before PROD-01.1's `record_semantics.rs` existed on this branch. PROD-01.5 owns the 3.9, 4.1 and 4.3 lines and their profiles.
- **The upstream forecasts are forecasts.** Whether upstream accepts a given PR, and the cost estimates, are forecasts from the code and the release history, not measurements.
- **Operator archives were reproduced, not collected.** They were reproduced by seeding with the same engine images the operators use; no archive was taken from a running operator. FX-1's snapshot failure is cited from the tracker, not re-run.

## 11. Class sweep owed (outside this row's ownership)

- `e2e/fixtures/manifests/0.19.2.json` carries `source_replication_factor`, `configurations` and `pruned`. Engine 0.19.2 never writes those: they were added in 0.20.0 and 0.21.0 (`git diff v0.19.2 v0.20.0` and `v0.20.0 v0.21.0 -- crates/kafka-backup-core/src/manifest.rs`). The fixture is therefore not writer bytes, the same provenance defect FX-1 names for the snapshot fixture. The real 0.19.2 manifest from cycle c6 confirms it: its topics carry only `name`, `original_partition_count` and `partitions`, and its segments no `sha256`. It is saved at `runs/c6-v0.19.2-k3.7.1-below/archive-manifest.json` to replace the fixture.
- `L/crates/logweir-core/src/destination.rs:462-477`, `L/crates/weirkeeper/src/destination.rs:531-535` and `L/crates/logweir-api/src/routes/destinations.rs:756-763` name "engine 0.21.0" in the ENGINE-PATHSTYLE refusal. On a bump, the message should not name a version (PROD-00.3f).
- `L/crates/logweir-engine-oso/src/render_restore.rs:147` renders `checkpoint_interval_secs`, a key the engine never reads (C4, A-C4-3).
- FX-6's disclosure (in `docs/verify-a-scorecard.md`, `docs/stability.md` and the restore review screen) should add C13 (duplicate header keys collapse) and C14 (LogAppendTime topics are archived with producer timestamps) beside the transaction and non-monotonic-timestamp hazards it already names.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
