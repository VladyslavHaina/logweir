# D2 — Saved destinations, bounded topic discovery, operation readiness and restore preflight

**Type:** implementation decision (spike outcome) with runnable acceptance criteria.
**Covers:** PLAT-08.1, PLAT-08.2, PLAT-09.1, PLAT-03.1, PLAT-03.2, the shared check-Job
framework, and the kind/ADR decision those require. Records seams for PLAT-09.2,
PLAT-11.2, PLAT-15 and PLAT-16 without implementing them.
**Date / base:** 2026-09-15, main `4956785`. Read-only investigation: no repository,
tracker, cluster or deployment change was made. This is not implementation,
deployment or acceptance evidence.
**Inputs:** `WORKER-RULES.md`; `docs/to-do/platform-improvements.md` (boundaries,
PLAT-03/07/08/09/11.2/15/16); `docs/architecture.md` (ADR 0008 A and E);
`docs/kubernetes.md`; `docs/install.md`; `charts/logweir/values.yaml`; the adopted
API decision `/tmp/plat17-contract-decision.md`; the sources listed in §1; the
pinned upstream engine source `third_party/kafka-backup-v0.21.0.tar.gz`; the
resolved `object_store 0.14.1`, `rdkafka 0.36.2` and `rdkafka-sys 4.10.0+2.12.1`
sources; the interrupted PLAT-06 patch
`/tmp/logweir-roadmap-run/claude/plat06-codex-interrupted.patch` (direction only,
not authoritative); read-only `kubectl --context docker-desktop get` of the
`logweir-scram-local` lab fixture.

---

## 0. Decision summary

1. **Three new namespaced kinds**, recorded as ADR 0008 Amendment F (§2.3):
   `BackupDestination` (durable saved destination), `TopicDiscovery` and
   `Preflight` (transient, cancellable check requests). No generic `Check`
   kind, no API-created Jobs, no API-held operation state.
2. **Destination model.** Immutable *location* (`provider: S3`, bucket, prefix,
   region, endpoint, addressing) and immutable *transport security*
   (`TLS` | `InsecureHTTP`, validated against the endpoint scheme); mutable CA
   bundle reference and four access grants: `archiveWrite`, `archiveRead`,
   `evidenceWrite`, `evidenceRead`. **Addressing never implies transport.**
   `InsecureHTTP` requires an explicit `http://` endpoint, and transport cannot be
   downgraded by an edit.
3. **Existing kinds keep inline `ArchiveRef`** (legacy, unchanged) and gain optional
   `destinationRef` / `sourceDestinationRef` / `evidenceDestinationRef`. When a
   ref is set, `archive.url` must be the sentinel `logweir-destination://<name>`,
   so an older controller *refuses* such objects instead of writing to the wrong
   place.
4. **Per-operation resolution.** Destination-backed runner Jobs receive a
   complete, explicitly rendered `AWS_*` environment and explicit plan storage
   blocks. The controller's own environment is never forwarded to them. The
   global `LOGWEIR_ARCHIVE_URL` handle serves legacy inline objects only.
5. **Evidence reads without Secret access.** By default, an evidence-fetch check
   Job runs in the object's namespace with the destination's `evidenceRead`
   credential and relays bounded bytes through its pod log. The controller
   recomputes digests and verifies DSSE itself. Admin-allowlisted
   `ControllerIdentity` is opt-in. Scoped Secret `get` is **rejected** (§3.8).
6. **PLAT-03.1 choice: a credential-consuming check Job.** The Job mirrors the
   execution pod: same image, ServiceAccount, Secret projections, CA and signing
   mounts, and egress. Only credential-free checks run in the controller.
   Pod-status and event classification produce exact codes for a missing
   Secret, key, image or ServiceAccount.
7. **Discovery.** A `TopicDiscovery` runs an isolated Job through the PLAT-07.1
   resolver and relays framed stdout. The controller stores owned, immutable,
   digest-indexed TSV `ConfigMap` chunks (≤ 2,500 entries / ≤ 768 KiB each).
   Visibility is `limited` only from *observed* authorization failures and
   `attestedComplete` only from an administrator attestation. Otherwise it is
   `unknown`.
8. **Restore preflight** binds to the exact `planHash`, an `inputsDigest` over
   referent UID/generation, and `expiresAt`. It performs **no writes**: it uses
   targeted metadata and validate-only `CreateTopics`. **Nothing on an execution
   path reads a `Preflight`.**
9. **One framework.** `logweir_core::check_contract` holds the pure contract,
   `logweir check run` is the runner, and `weirkeeper::check` is the controller
   side. Discovery, preflight and evidence fetch use it, and the `KafkaCluster`
   probe reuses its pod-lookup, classification and TTL helpers.
10. **Sixteen bounded worker tasks** (§13, W1–W14 including W6a/W6b, W13a and
    W14a). W1, W2, W4 and W5 can start now
    without touching files owned by PLAT-06.1, PLAT-07.1 or PLAT-17.1.

---

## 1. Grounding: what the code does today, and what that forces

| # | Observation (file:line) | Consequence for this decision |
|---|---|---|
| G1 | `ArchiveRef{url, secretRef}` is inline on `BackupSchedule.spec.archive` (`crates/weirkeeper/src/crds/backup_schedule.rs:291`), `Backup.spec.archive` (`crds/backup.rs:111`) and `Restore.spec.sourceArchive` (`crds/restore.rs:222`); type at `crds/mod.rs:113-123`. | Destinations must be additive refs. Every existing object stays valid, and specs are CEL-immutable (`crds/mod.rs:242-277`). |
| G2 | The controller builds **one** read-only store from `LOGWEIR_ARCHIVE_URL` (`crates/weirkeeper/src/main.rs:145-191`; env name `retention.rs:91`). Region, endpoint and credentials come from its own environment (`retention.rs:818-835`, `logweir-store/src/lib.rs:192` `AmazonS3Builder::from_env()`). | Evidence for a second destination is read from the wrong bucket or with the wrong principal. That is the tracker's "global configuration leakage". |
| G3 | The controller **forwards** `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` from its own environment to every runner Job (`controllers/backup.rs:192-218`, used at `:1004-1008` and `controllers/restore.rs:1297`). | The engine calls `AmazonS3Builder::from_env()`, which reads every `AWS_*` variable (object_store `aws/builder.rs:606-617`). A forwarded `AWS_ALLOW_HTTP=true` therefore enables HTTP **even when the approved plan says `allow_http: false`**. This is a transport downgrade through global configuration. Destination-backed Jobs must control their complete `AWS_*` set. |
| G4 | The pinned engine ignores `path_style` (`kafka-backup-core/src/storage/mod.rs:55` `path_style: _`). It forces path-style whenever an endpoint is set (`storage/s3.rs:66`) and otherwise uses the environment default (`s3.rs:57`). | `VirtualHosted` addressing with a custom endpoint cannot be honoured by engine 0.21.0 and is refused as `AddressingUnsupportedByEngine` (§3.3). |
| G5 | The restore wizard sets `allowHttp` from the path-style checkbox (`ui/pages/restore-wizard.js:1142-1149`: `block.allowHttp = pathStyle.checked === true;`). It also applies one endpoint, region and path-style value to **both** source and evidence (`:1135`). | Confirmed PLAT-08.2 defect ("path-style can accidentally enable HTTP") and a missing archive/evidence separation. |
| G6 | A Restore Job gets **one** object-store credential, from `sourceArchive.secretRef` (`controllers/restore.rs:1224-1235`). The runner builds both the evidence and archive stores from the same environment (`crates/logweir/src/drill/mod.rs:1114-1120`). | Archive-read and evidence-write principals are not separable today. Runner changes are needed (§3.5). |
| G7 | Backup evidence is written to the **archive bucket** under `logweir/` with the archive credential (`crates/logweir/src/backup/mod.rs:783-790`, `evidence_location` at `:809-847`). | For backups, the evidence location is the destination bucket root `logweir/`. Separation applies to the *evidence-read* principal and to restore evidence destinations. |
| G8 | The controller holds no Secret verb; `tests/linkage.rs:461-478` (`the_controller_never_reads_a_secret`) scans for `Api::<Secret>` and `"secrets"`. It has cluster-wide ConfigMap `create/get`, Job `create/get/list/watch/patch`, pod `list` and `pods/log get` (`config/rbac/role.yaml`). | Any design that reads credentials in the controller breaks a tested, documented invariant (`docs/kubernetes.md` §8, §12, §15.1). |
| G9 | The probe is already a credential-consuming Job pipeline (`controllers/kafka_cluster.rs:433-485`, TTL patch after verdict `:1146-1154`, key-name tail scan `:279-298`). The backup and restore paths duplicate the Job/pod/log handling. | A framework already exists implicitly. Extract it rather than write a third and fourth copy. |
| G10 | Pod lookup uses **labels only** and takes the first match (`controllers/backup.rs:1683-1704`, `restore.rs:2519-2540`, `kafka_cluster.rs:832-853`). | A tenant who can create pods could label one `batch.kubernetes.io/job-name=<job>` and have its log read. New check code must verify the pod's controller owner is the Job UID. This is a pre-existing gap on three paths. |
| G11 | Kafka listing: `ClusterReader::list_topics` (`logweir-kafka/src/reader.rs:217`, `TopicMeta` `:31-47`) makes an all-topics metadata call with a 20 s budget (`rdkafka_reader.rs:17,171-202`) and pins `allow.auto.create.topics=false` (`:46`). rdkafka 0.36.2 `MetadataTopic` has no internal flag. `DescribeTopics`, `DescribeCluster` and `DescribeAcls` exist only as unsafe `rdkafka-sys` bindings, and `logweir-kafka` is `#![forbid(unsafe_code)]`. | Internal-topic classification is by name. ACL-derived completeness is not implementable safely or soundly (§5.4). |
| G12 | Runner phase 0 lists target topics and refuses collisions (`crates/logweir/src/drill/phase0_admit.rs:393-394,521-547`). Its LogAppendTime check **creates and deletes a probe topic** (`:607-704`). | Preflight cannot reuse phase 0 wholesale, because preflight must be side-effect-free. Execution keeps phase 0 as the authoritative guard. |
| G13 | The wizard's step 5, "Target-topic preflight", shows only the target `KafkaCluster`'s cached `status.reachable` (`ui/pages/restore-wizard.js:424-443`). Restore admission gates on the same cached value (`controllers/restore.rs:638-650`). | This is exactly PLAT-03.2's "cached reachability looks like completed preflight". |
| G14 | Retention lists manifests through the global handle while rendering commands for the schedule's own URL (`controllers/backup_schedule.rs:1417-1450`). | A schedule on another bucket gets a report about the wrong catalog. For destination-backed schedules this path is disabled (§3.10); PLAT-16.1 supplies the replacement. |
| G15 | In-flight PLAT-06.1 (worktree `wt/plat06`; direction visible in the interrupted patch) freezes `ResolvedBackupInputs{…, archive: ArchiveRef, …}` into `<backup>-plan` as `execution-inputs.json` and adds `status.execution{id, inputsRef, inputsSha256}`. | Destination snapshots attach to that frozen boundary (§3.7). They are not a second freeze. |
| G16 | `object_store 0.14.1` supports `ClientOptions::with_root_certificate` (`client/mod.rs:519`) and by default verifies against the platform store (`:580-590`). Credential order is static keys, then web identity, then container credentials, then **instance metadata** (`aws/builder.rs:1156-1233`). `AWS_METADATA_ENDPOINT` is honoured by `from_env` (`:524`). | Logweir's own stores can trust a destination CA. Every destination-backed Job pins `AWS_METADATA_ENDPOINT` to a dead loopback so a missing workload identity cannot fall back to the node role. |
| G17 | The lab Kafka (`logweir-scram-local/kafka-source`, apache/kafka 3.7.1) has **no authorizer**. The lab controller reconciles **all namespaces** (`Api::all` in every `controller()`, e.g. `kafka_cluster.rs:1220`). | ACL scenarios need an owned Kafka with `StandardAuthorizer`. Live runs need the cluster lock and a scaled-down lab controller (§14). |

---

## 2. Kinds and the recorded architectural decision

### 2.1 Options considered

| Option | Verdict | Reason |
|---|---|---|
| Extend existing kinds only (fields on `KafkaCluster` or `BackupSchedule`) | Rejected | A destination is shared by schedules, manual backups, restores and catalog import. Duplicating it is the defect PLAT-08 removes. |
| Destination as a `ConfigMap` convention | Rejected | No structural schema, CEL, status, printer columns or RBAC separation. |
| One generic `Check` kind with a discriminated union | Rejected | Topic inventories reveal topic names, which some organizations treat as sensitive. Separate kinds allow granting readiness without inventory read. CEL on one union spec is error-prone. The API decision already exposes `discovery` and `preflight` as distinct operation kinds. |
| No check kinds (the API runs checks itself or holds state) | Rejected | The API decision forbids the API from creating Jobs or dialling Kafka or object storage, and restart durability requires Kubernetes state. |
| **`BackupDestination` + `TopicDiscovery` + `Preflight`** | **Chosen** | Clear RBAC, clear `kubectl get` output, and a 1:1 API mapping. The shared framework lives in code, not in a super-kind. |
| Separate result kind for inventory pages (`TopicDiscoveryPage`) | Fallback only | Adopted only if security review rejects `get configmaps` for the API ServiceAccount (§7.3). |

### 2.2 Kind name reading of Amendment A

Amendment A forbids names that encode *a particular* cluster, fleet, topic,
connector or backup:

- `BackupDestination` names a class of object, a destination for backups, not a
  particular backup.
- `TopicDiscovery` names an operation over topics, not a particular topic.
- `Preflight` names an operation.
- None contains `Kafka`, so `crd_shape.rs`'s "only `KafkaCluster`" assertion still
  holds.

### 2.3 ADR text to add to `docs/architecture.md` (new section after Amendment E)

> ## Amendment F — saved destinations and transient check requests
>
> Accepted from <merge date>. Adds three namespaced kinds to Amendment A's list:
> `BackupDestination`, `TopicDiscovery` and `Preflight`. `Switchover` remains tag 2
> and `MetadataSnapshot` remains reserved.
>
> `BackupDestination` is durable configuration: where archives live (immutable
> location and transport security) and which namespace-local credential references
> each role uses. It holds no credential value. Executions freeze a resolved
> snapshot of it, so later edits never change an existing run or recovery point.
>
> `TopicDiscovery` and `Preflight` are requests for bounded, time-limited
> observations. `weirkeeper` turns each into one isolated runner-image Job in the
> request's namespace. That Job has no Kubernetes token and uses the same credential
> projection as execution. The controller stores results in status and in owned,
> immutable `ConfigMap` chunks. It may cancel the Job and may delete an expired
> terminal check request; it deletes nothing else. Results are advisory. No
> reconciler or runner treats a check result as authorization or as a substitute
> for an execution-time guard.
>
> Rejected alternatives: controller Secret reads, scoped by name or otherwise;
> API-side checks; a single discriminated `Check` kind; destination settings as
> `ConfigMap` conventions. The controller still holds no verb on `secrets`, and the
> signing-oracle residual of Amendment E is unchanged: check Jobs are created under
> the same Job-create authority.

### 2.4 Mechanical consequences (owned by W6a, §13)

These tests, files and documents must change together:

- `crds/mod.rs:66-73`: `KINDS` becomes 9 and `render_all` emits three more files.
- `seal_spec` must accept additional rules. Kinds have more than one rule entry,
  and the new kinds use a sealed `spec.request` plus `spec.cancelRequested`.
- `crates/weirkeeper/tests/crd_shape.rs:238-306`: the six-kind test becomes a
  nine-kind test and `FILES` gains three entries.
- `tests/linkage.rs:952`: `"controllers":6` becomes 9.
- `crates/logweir/tests/manifest_lint.rs:523-564`: `ty_of` needs mappings for the
  three new plurals and for `events`.
- `docs/kubernetes.md` §7 table and "six kinds" prose.
- `docs/install.md:8,511-515`: CRD count and wait loop.
- `config/crd/kustomization.yaml`, `charts/logweir/crds/*` copies,
  `charts/logweir/rendered/*`.

---

## 3. `BackupDestination` (PLAT-08.1 / PLAT-08.2)

### 3.1 Resource shape

```yaml
apiVersion: logweir.dev/v1alpha1
kind: BackupDestination
metadata:
  name: primary            # <= 63 chars (root CEL rule R0)
  namespace: team-a
  annotations:
    logweir.dev/default-destination: "true"   # optional; at most one per namespace is honoured by the API
spec:
  description: "Production archive (MinIO, private CA)"        # optional, maxLength 256, mutable
  storage:                                                     # IMMUTABLE (R1)
    provider: S3                                               # required, enum [S3]
    bucket: kafka-backups                                      # required, pattern ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$
    prefix: team-a/prod                                        # default "", maxLength 512, R6
    region: us-east-1                                          # optional, pattern ^[a-z0-9-]{1,32}$
    endpoint: https://minio.storage.svc:9000                   # optional, maxLength 2048, R5; absent = AWS S3
    addressing: PathStyle                                      # required, enum [PathStyle, VirtualHosted]; no default
  transport:
    security: TLS                                              # required, enum [TLS, InsecureHTTP]; IMMUTABLE (R2); R3
    caBundle:                                                  # optional, TLS only (R4); mutable
      configMapName: minio-ca                                  # same namespace; CAs are public, never a Secret
      key: ca.crt                                              # default ca.crt
  access:                                                      # mutable (rotation)
    archiveWrite:                                              # Backup Jobs: engine segments + receipt under logweir/
      mode: SecretKeys                                         # enum [SecretKeys, WorkloadIdentity]
      secret: {name: logweir-s3, accessKeyIdKey: access-key-id, secretAccessKeyKey: secret-access-key}
    archiveRead:                                               # optional; absent => uses archiveWrite
      mode: SecretKeys
      secret: {name: archive-reader}
    evidenceWrite:                                             # optional; absent => uses archiveWrite
      mode: WorkloadIdentity
      workloadIdentity: {serviceAccountName: logweir-runner}   # default logweir-runner
    evidenceRead:                                              # optional; absent => verification NotAttempted
      mode: SecretKeys                                         # enum [SecretKeys, WorkloadIdentity, ControllerIdentity, ArchiveReadGrant]
      secret: {name: evidence-ro}
  readiness:
    writeProbe: Disabled                                       # enum [Disabled, CreateOnlyMarker], default Disabled; mutable
status:
  observedGeneration: 3
  reason: Valid
  canonicalUrl: s3://kafka-backups/team-a/prod
  locationDigest: sha256:…      # §3.4
  caBundleSha256: sha256:…      # over the CA bytes read from the ConfigMap
  conditions: [{type: Valid, status: "True", reason: Valid, …}]
```

Field details:

- **`S3SecretKeysRef`**
  - `name`: required, DNS-1123 subdomain.
  - `accessKeyIdKey`: default `access-key-id`.
  - `secretAccessKeyKey`: default `secret-access-key`.
  - `sessionTokenKey`: optional, no default.
  - Every key name matches `^[-._a-zA-Z0-9]{1,253}$`.
  - The defaults match today's `ARCHIVE_ACCESS_KEY` / `ARCHIVE_SECRET_KEY`
    (`controllers/backup.rs:161-164`).
- **Printer columns:** `BUCKET` (`.spec.storage.bucket`), `ENDPOINT`
  (`.spec.storage.endpoint`), `TRANSPORT` (`.spec.transport.security`), `VALID`
  (`.status.conditions[?(@.type=="Valid")].status`), `AGE`.
- **No periodic health probe.** A destination is exercised only by operations and
  explicit tests (`Preflight` with `operation: DestinationAccess`). Cached health
  masquerading as readiness is the very defect PLAT-03 names.

### 3.2 CEL rules (exact; regex and core functions only, valid at the 1.29 floor)

| Id | Placement | Rule | Message |
|---|---|---|---|
| R0 | root | `size(self.metadata.name) <= 63` | `BackupDestination names are at most 63 characters` |
| R1 | `.spec` | `self.storage == oldSelf.storage` | `spec.storage is immutable: a different location is a different BackupDestination` |
| R2 | `.spec` | `self.transport.security == oldSelf.transport.security` | `spec.transport.security is immutable: transport can never be changed in place` |
| R3 | `.spec` | `self.transport.security == 'InsecureHTTP' ? (has(self.storage.endpoint) && self.storage.endpoint.startsWith('http://')) : (!has(self.storage.endpoint) \|\| self.storage.endpoint.startsWith('https://'))` | `transport.security must match the endpoint scheme: TLS needs an https:// endpoint or none; InsecureHTTP needs an explicit http:// endpoint. storage.addressing never changes transport` |
| R4 | `.spec` | `!has(self.transport.caBundle) \|\| self.transport.security == 'TLS'` | `transport.caBundle requires transport.security TLS` |
| R5 | `.spec.storage` | `!has(self.endpoint) \|\| self.endpoint.matches('^https?://([A-Za-z0-9] ([A-Za-z0-9-]*[A-Za-z0-9])?(\\.[A-Za-z0-9] ([A-Za-z0-9-]*[A-Za-z0-9])?)*\|\\[[0-9A-Fa-f:.]+\\])(:[0-9]{1,5})?/?$')` | `storage.endpoint must be an http(s) origin: scheme, host and optional port only (no userinfo, path, query or fragment)` |  (a space follows each character class so the repository link gate does not read the regex as a Markdown link; the exact expression is the one in `crds/destination.rs`)
| R6 | `.spec.storage` | `self.prefix == '' \|\| (self.prefix.matches("^[A-Za-z0-9!_.*'()-]+(/[A-Za-z0-9!_.*'()-]+)*$") && !self.prefix.matches('(^\|/)[.]{1,2}(/\|$)') && !self.prefix.matches('^logweir(/\|$)'))` | `storage.prefix is relative with no empty, '.' or '..' segment, and may not be the reserved evidence root logweir/` |
| R7 | each of `.spec.access.{archiveWrite,archiveRead,evidenceWrite}` | `self.mode == 'SecretKeys' ? (has(self.secret) && !has(self.workloadIdentity)) : !has(self.secret)` | `SecretKeys needs secret and no workloadIdentity; WorkloadIdentity takes no secret` |
| R8 | `.spec.access.evidenceRead` | `self.mode == 'SecretKeys' ? (has(self.secret) && !has(self.workloadIdentity)) : (!has(self.secret) && (self.mode == 'WorkloadIdentity' \|\| !has(self.workloadIdentity)))` | `evidenceRead fields must match its mode` |
| R9 | `.spec.access` | `!has(self.evidenceRead) \|\| self.evidenceRead.mode != 'ArchiveReadGrant' \|\| has(self.archiveRead)` | `evidenceRead ArchiveReadGrant requires an explicit read-only archiveRead grant; a write grant is never reused for verification` |

The endpoint rule is regex-only on purpose. It avoids a dependency on the CEL URL
library, which cannot be proved on the 1.29 floor from docker-desktop 1.34.

### 3.3 Controller validation and status (not CEL, because the engine version can change it)

`controllers/backup_destination.rs` sets one condition, `Valid`, and copies its
reason into `status.reason`. It requeues every 300 s so that CA `ConfigMap`
rotation is observed.

| Condition | `Valid` | Reason |
|---|---|---|
| All checks pass | True | `Valid` |
| `addressing == VirtualHosted` with an `endpoint` set (G4) | False | `AddressingUnsupportedByEngine` |
| `caBundle` `ConfigMap` absent | False | `CaBundleNotFound` |
| `caBundle` key missing | False | `CaBundleKeyMissing` |
| `caBundle` over 64 KiB | False | `CaBundleTooLarge` |
| `caBundle` has no parseable PEM certificate | False | `CaBundleInvalid` |

- **Status writes:** `canonicalUrl`, `locationDigest`, `caBundleSha256` and
  `observedGeneration` are written on every verdict.
- **Loop avoidance:** the status write is skipped when unchanged
  (`conditions::status_unchanged`), as the probe does.
- **Transport and addressing:** the rules in §3.2 and above are the only ones the
  controller enforces. Nothing else maps addressing to transport, in either
  direction.

### 3.4 Resolver (`crates/weirkeeper/src/destination.rs`) and pure core (`crates/logweir-core/src/destination.rs`)

**Pure core (`logweir_core::destination`, no I/O):**

- `DestinationLocation {provider, bucket, prefix, region, endpoint, addressing, transport}`.
- `validate(&DestinationLocation) -> Result<(), Vec<FieldError>>` duplicates
  R3–R6 for API 422 messages and for the controller.
- `engine_compatible(&DestinationLocation) -> Result<(), &'static str>` covers G4.
- `canonical_url()` returns `s3://<bucket>[/<prefix>]`.
- `location_digest()` is `sha256_prefixed` over the UTF-8 of
  `"s3\n" + host_identity + "\n" + bucket + "\n" + prefix + "\n"`, where:
  - `host_identity` is `endpoint` lower-cased without scheme or trailing `/` (for
    example `minio.storage.svc:9000`), or `aws/<region or "">` when there is no
    endpoint.
  - Transport and addressing are excluded, because they describe how the data is
    reached, not where it is.
- `archive_storage_url()` returns
  `StorageUrl::S3{bucket, prefix, region, endpoint, path_style: addressing==PathStyle, allow_http: transport==InsecureHTTP}`.
- `evidence_storage_url()` is the same with `prefix: "logweir/"`, matching
  `crates/logweir/src/backup/mod.rs:809-847` and the `Store::from_url` guard.

**Controller resolver (`weirkeeper::destination::resolve`):**

```rust
pub enum DestinationRole { ArchiveWrite, ArchiveRead, EvidenceWrite, EvidenceRead }
pub struct ResolvedDestination {
    pub name: String, pub uid: String, pub generation: i64,
    pub location: DestinationLocation, pub location_digest: String,
    pub ca_pem: Option<Vec<u8>>, pub ca_sha256: Option<String>,
    pub grant: ResolvedGrant,           // for the requested role, after defaulting
}
pub enum ResolvedGrant {
    SecretKeys { secret: String, access_key_id_key: String, secret_access_key_key: String, session_token_key: Option<String> },
    WorkloadIdentity { service_account_name: String },
    ControllerIdentity,                 // EvidenceRead only; requires policy allowlist (§4.4)
    NotConfigured,                      // EvidenceRead absent
}
pub fn resolve(dest: &BackupDestination, role: DestinationRole, policy: &Policy) -> Result<ResolvedDestination, DestinationRefusal>;
```

**Role defaulting:**

- `ArchiveRead` falls back to `archiveWrite`.
- `EvidenceWrite` falls back to `archiveWrite`.
- `EvidenceRead` resolves `ArchiveReadGrant` to the explicit `archiveRead`.
- `EvidenceRead` absent resolves to `NotConfigured`.
- `ArchiveWrite` absent refuses with `DestinationRoleNotConfigured`.

**Refusals:**

| Refusal | Handling |
|---|---|
| `DestinationNotFound` | Hold (§3.6) |
| `DestinationNotValid(reason)` | Hold (§3.6) |
| `DestinationRoleNotConfigured(role)` | Terminal |
| `ControllerIdentityNotAllowlisted(canonicalUrl)` | Verification `NotAttempted` |
| `ExecutionContextConflict{a, b}` | Terminal: two roles in one pod need different ServiceAccounts, or a role conflicts with the PLAT-07.1 connection execution context |

### 3.5 Rendering into execution Jobs

**Version-skew handshake.** Destination-backed Backup and Restore Jobs add argv
`--store-contract-version 1` and env `LOGWEIR_STORE_CONTRACT_VERSION=1`. This is
the same technique as `--execution-contract-version` (`docs/kubernetes.md:1011-1016`):
a runner without the contract rejects the unknown flag before dispatch, so a new
controller can never drive an old runner that would silently use ambient
credentials for the evidence store.

**Backup Job (destination-backed): complete env excerpt.**
`job::build` still adds `LOGWEIR_ENGINE_*` and `TMPDIR`.

```yaml
- {name: RUST_LOG, value: info}
- {name: LOGWEIR_STORE_CONTRACT_VERSION, value: "1"}
- {name: LOGWEIR_ARCHIVE_CREDENTIALS, value: static}          # static | workloadIdentity
- {name: AWS_ALLOW_HTTP, value: "false"}                      # "true" ONLY when transport.security == InsecureHTTP
- {name: AWS_VIRTUAL_HOSTED_STYLE_REQUEST, value: "false"}    # "true" ONLY for VirtualHosted (no endpoint)
- {name: AWS_METADATA_ENDPOINT, value: "http://127.0.0.1:1"}  # no instance-metadata (node role) fallback (G16)
- {name: AWS_REGION, value: us-east-1}                        # only when storage.region is set
- {name: LOGWEIR_ARCHIVE_CA_FILE, value: /plan/archive-ca.pem} # only with caBundle; bytes frozen in the plan ConfigMap
- {name: LOGWEIR_SOURCE_PASSWORD, valueFrom: {secretKeyRef: {name: <from PLAT-07.1 resolver>, key: <explicit key>}}}
- {name: AWS_ACCESS_KEY_ID,     valueFrom: {secretKeyRef: {name: logweir-s3, key: access-key-id}}}
- {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: logweir-s3, key: secret-access-key}}}
- {name: AWS_SESSION_TOKEN,     valueFrom: {secretKeyRef: {name: logweir-s3, key: <sessionTokenKey>}}}  # only if configured
# ABSENT BY CONSTRUCTION: AWS_ENDPOINT_URL and every value from archive_addressing_env().
```

- **Plan storage block.** `plan_backup_spec` takes `storage` from
  `ResolvedDestination.location.archive_storage_url()` instead of
  `storage_url_for(archive.url)` (`controllers/backup.rs:394`).
- **Credential projection.** `WorkloadIdentity` omits the three `AWS_*` key
  variables, sets `LOGWEIR_ARCHIVE_CREDENTIALS=workloadIdentity`, and sets the
  Job `serviceAccountName` to the grant's ServiceAccount.
- **Fail closed without an injected identity.** The runner refuses with exit 3 and
  `refusal-reason=WorkloadIdentityNotInjected` unless `AWS_WEB_IDENTITY_TOKEN_FILE`
  plus `AWS_ROLE_ARN`, or `AWS_CONTAINER_CREDENTIALS_FULL_URI` plus its token file,
  is present.
- **No node roles.** Node instance roles are not a supported destination mode.
  They remain available only on the legacy inline path.

**Restore Job (destination-backed):**

- **Archive-read credential.** Rendered exactly as above from the source
  destination's `archiveRead` (the engine reads the archive through `AWS_*`).
- **Evidence-write credential (`evidenceWrite` of the evidence destination):**
  - Same resolved grant as archive read (same Secret name and keys, or the same
    ServiceAccount): `LOGWEIR_EVIDENCE_CREDENTIALS=archive`.
  - Different Secret: `LOGWEIR_EVIDENCE_CREDENTIALS=static` plus
    `LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID`, `LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY`
    and optionally `LOGWEIR_EVIDENCE_AWS_SESSION_TOKEN`.
  - Workload identity with a static archive grant:
    `LOGWEIR_EVIDENCE_CREDENTIALS=workloadIdentity`. The runner then builds the
    evidence store **without** `from_env`, copying only the web-identity and
    container-credential variables. Otherwise the static archive keys would take
    precedence (G16).
  - Two different workload-identity ServiceAccounts in one pod refuse as
    `ExecutionContextConflict`.
- **CA files.** `LOGWEIR_ARCHIVE_CA_FILE` and `LOGWEIR_EVIDENCE_CA_FILE` point at
  keys copied into the immutable `<restore>-plan` `ConfigMap`, next to
  `restore.yaml`. Plan bytes stay verbatim.

**Runner-side changes (W10, using the W2 store API):**

- **Store construction.** `backup/mod.rs:758-790` and `drill/mod.rs:1114-1120` build
  their stores with `StoreOptions` (§4.1, W2):
  - explicit `allow_http`, virtual-hosted and endpoint values from the plan block
    (overriding the environment);
  - credentials according to `LOGWEIR_*_CREDENTIALS`;
  - `with_root_certificate` for the relevant CA file.
- **Engine trust bundle.** For the engine child process the runner writes
  `/work/trust/archive-bundle.pem` (system bundle `/etc/ssl/certs/ca-certificates.crt`
  followed by the destination CA) and sets `SSL_CERT_FILE` on that subprocess only.
  The change is in `crates/logweir-engine-oso/src/subprocess.rs:75-80`.
- **[UNVERIFIED — U1: whether the engine honours a custom CA file]** The engine honours `SSL_CERT_FILE` through
  rustls-platform-verifier on Linux. Until it is proved, the controller refuses a
  destination with a `caBundle` for Backup and Restore admission with
  `CaBundleUnsupportedByEngine`, while Logweir-only paths (checks and verification)
  support the CA. The refusal is gated by the compiled constant
  `ENGINE_CUSTOM_CA_VERIFIED` (false until the evidence exists) and by the
  administrator-governed policy key `engine.allowUnverifiedCustomCa` (default
  false). W14 sets that key to run S2b; the constant flips only after S2b passes on
  a recorded engine digest, and the key returns to false.

### 3.6 References from existing kinds, the sentinel, and admission

**Additive, optional fields (W6b):**

- `BackupSchedule.spec.destinationRef: LocalRef`; also add its two halves to
  `SUSPEND_ONLY_RULE` (`crds/backup_schedule.rs:65`). *(Superseded 2026-09-17 at D1 W2's integration: D1 §5.2 R1 replaces `SUSPEND_ONLY_RULE` and D1 §5.1 makes `destinationRef` mutable, as amended there.)*
- `Backup.spec.destinationRef: LocalRef`.
- `Restore.spec.sourceDestinationRef: LocalRef` and
  `Restore.spec.evidenceDestinationRef: LocalRef`.

**Additional CEL on `.spec` (the existing seal stays):**

| Kind | Rule | Message |
|---|---|---|
| Backup, BackupSchedule | `has(self.destinationRef) ? (self.archive.url == 'logweir-destination://' + self.destinationRef.name && !has(self.archive.secretRef)) : !self.archive.url.startsWith('logweir-destination://')` | `with destinationRef, archive.url is exactly logweir-destination://<destinationRef.name> and archive.secretRef is absent; the logweir-destination scheme is otherwise reserved` |
| Restore | `has(self.sourceDestinationRef) == has(self.evidenceDestinationRef)` | `sourceDestinationRef and evidenceDestinationRef are set together` |
| Restore | `has(self.sourceDestinationRef) ? (self.sourceArchive.url == 'logweir-destination://' + self.sourceDestinationRef.name && !has(self.sourceArchive.secretRef)) : !self.sourceArchive.url.startsWith('logweir-destination://')` | as above |

**Why a sentinel rather than making `archive` optional.**

- An older controller deserializing an object without `archive` would fail the
  whole `Backup` list or watch (kube-runtime reflector decode error). Every
  `Backup` reconcile would stall after a rollback.
- With the sentinel, old objects decode, and the old `storage_url_for`
  (unknown-scheme arm, `retention.rs:877`) returns an error. The old `Backup` controller therefore
  writes the terminal `ArchiveUrlUnreadable` before any POST.
- An old `Restore` controller ignores `sourceArchive.url`. It projects no archive
  credential (the sentinel has no `secretRef`), so the runner fails reading the
  plan-pinned location. The approved plan bytes pin the location, so nothing is
  misrouted.

**Backup reconcile additions**, before PLAT-06.1's freeze:

1. **Destination resolution** (`get`, namespace-local):
   - `DestinationNotFound` or `DestinationNotValid`: `phase: Pending`, condition
     `Admitted=False` with that reason, requeue 30 s.
   - After `min(spec.deadlineSeconds, 600)` seconds from `creationTimestamp` the
     hold becomes terminal `Failed` with the same reason.
   - Nothing is created while holding.
2. **Role and context:** `DestinationRoleNotConfigured` and
   `ExecutionContextConflict` are terminal.
3. **Freeze:** the resolved destination is frozen into PLAT-06.1's execution
   inputs before any Job exists (§3.7).

**Restore admission additions** run after existing checks 0–4
(`controllers/restore.rs:558-650`), so today's reason precedence is unchanged:

| # | Check | Outcome on failure |
|---|---|---|
| 5 | Both destinations resolvable and `Valid` | `DestinationNotFound` / `DestinationNotValid`: hold and requeue 30 s, like `ApprovalNotVerified` (an approver may take an afternoon) |
| 6 | Parse `planBytes` as `logweir_core::spec::RestoreSpec` (read only, never re-emitted) | `PlanUnparseable`, terminal |
| 7 | `plan.source.storage == source.archive_storage_url()`, exact on all six fields | `PlanDestinationMismatch`, terminal; the message names fields, not credentials |
| 8 | `plan.evidence == evidence.evidence_storage_url()` | `PlanEvidenceDestinationMismatch`, terminal |
| 9 | `archiveRead` (source) and `evidenceWrite` (evidence) grants resolvable; ServiceAccounts compatible | `DestinationRoleNotConfigured` / `ExecutionContextConflict`, terminal |

A destination edit never invalidates an approval. Location and transport are
immutable, so the plan bytes are unchanged; only preflights go stale (§6.6).

### 3.7 Freezing the resolved destination (PLAT-06.1 boundary)

- **Where.** PLAT-06.1's `ResolvedBackupInputs` gains
  `destination: Option<ResolvedDestinationSnapshot>` containing
  `{name, uid, generation, locationDigest, archiveStorage: StorageUrl, evidenceStorage: StorageUrl, transport, addressing, caSha256, grant: {mode, secretName?, keys?, serviceAccountName?}}`.
- **CA bytes.** They are copied into the same immutable plan `ConfigMap` as key
  `archive-ca.pem`, and their digest is part of the canonical snapshot.
- **Effect.** The Job is rendered only from the snapshot. Later destination edits
  (access rotation, CA rotation) cannot affect a created run, and the recovery
  point keeps its `locationDigest` for restore selection (§3.12) and PLAT-15
  catalog indexing.
- **Legacy objects.** `destination` stays absent and `archive` remains the legacy
  input, unchanged.

### 3.8 Per-destination evidence reads: options evaluated honestly

| Option | How the credential reaches the read | Controller RBAC | Blast radius / aggregation | Namespace isolation | Verification independence | Cost | Verdict |
|---|---|---|---|---|---|---|---|
| A. Global controller store (today) | One principal from controller env | none | One principal must read all evidence; a second destination is read with the wrong bucket or credential (G2) | none | high | none | **Legacy inline objects only** |
| B. Scoped Secret `get` by `resourceNames` | Controller GETs the destination's Secret | `get secrets` per name, maintained by an administrator per destination, or by the controller with `bind`/`escalate` | Controller memory aggregates every tenant's evidence credentials; one compromise exposes all; credentials risk reaching logs; breaks `the_controller_never_reads_a_secret` and docs §15.1 | only as good as generated RoleBindings | high | low | **Rejected** |
| B'. Fixed-name per-namespace Secret (`logweir-evidence-read`) | Static Role with `resourceNames` | `get secrets` in every managed namespace | Same aggregation; cannot distinguish two destinations in one namespace | ok | high | low | **Rejected** |
| C. Evidence-fetch check Job, controller verifies | kubelet projects `evidenceRead` into a no-token pod in the object's namespace; bounded bytes relayed by pod log | none new | per-operation pod; no controller-held credential | kubelet resolves `secretKeyRef` in the pod's namespace only | Digest and DSSE computed **in the controller** over relayed bytes. A forged relay cannot yield `Valid` without a roster signing key (bounded by residual O1). Binding checks (payload `backup_id` / run identity / subject) block substitution of another validly signed document. | +1 pod per verified operation (10–60 s), counts against namespace quotas | **Chosen default** |
| D. Runner self-report of its evidence bytes | Writer's own log | none | n/a | n/a | low: verifies the writer's claim, not bucket contents | none | Rejected |
| E. `ControllerIdentity` per destination, admin allowlist | Controller ambient chain (IRSA/Pod Identity, or today's `logweir-evidence-ro` env) plus explicit per-destination addressing and CA | none | One principal over admin-allowlisted locations only (§4.4); an operator cannot point it elsewhere | allowlist | high | low | **Opt-in** |
| F. Mount per-destination credentials into the controller pod | CSI/projected volumes | none | Aggregation; rotation needs restarts; tenant-namespace Secrets cannot be projected into another namespace's pod | impossible across namespaces | — | — | Rejected |
| G. API performs reads | — | — | The API decision forbids object-store dialling | — | — | — | Rejected |

**Decision: C by default, E opt-in, A for legacy objects only.**

### 3.9 Evidence observation flow for destination-backed runs

**Backup** (`controllers/backup.rs`, W10):

1. As today, write the terminal status patch (exit code, keys, refusal) and then
   the runner Job TTL patch. For destination-backed runs the terminal patch **no
   longer** carries `receiptSha256`, `windowCovered` or orphan presence, because
   no store read has happened yet.
2. If both keys are present, choose by `evidenceRead` mode:
   - `ControllerIdentity`: `evidence_store::StoreCache` (§3.10) fetches inside
     `spawn_blocking`.
   - `SecretKeys` / `WorkloadIdentity`: create an evidence-fetch Job
     `lwc-ev-<20 hex of sha256(backup uid + ":" + attempt)>` owned by the `Backup`,
     with plan `ConfigMap` `<job>-plan` (§4.2).
   - `NotConfigured`: write
     `verification = {result: NotAttempted, detail: "BackupDestination <name> has no spec.access.evidenceRead; run the printed logweir drill verify command instead"}`.
3. While the fetch runs: `status.evidence.verification.result: Pending`, an
   additive value that the UI badge renders `unverified` because it is not
   `Valid`. `status.evidence.observation = {mode, jobRef{name,uid}, attempt}`.
4. On relay completion the controller:
   - checks sha256 of the relayed receipt;
   - checks the receipt JSON's `backup_id == status.backupId` (binding);
   - verifies DSSE against `TrustRoster.spec.signingKeys` using
     `verification::verify_fetched(bytes, sidecar, roster, payload_type)`,
     refactored from `verify_evidence` (`verification.rs:315`);
   - takes `windowCovered` from the verified receipt's `covered`;
   - takes presence (`Complete` | `PayloadWithoutSidecar` | `Absent` | `Unknown`)
     from the relay; `Absent` requires a `NotFound`, which S3 returns only when
     the reader has `s3:ListBucket`, otherwise `Unknown`.
   - Then it patches `evidence.{receiptSha256, verification, observation.presence}`,
     `windowCovered` and the `Verified` condition.
5. **Failure** (exit 1, relay unreadable, pod not started): `NotAttempted` with a
   redacted code in `detail`. Retry up to 3 attempts at +1 m, +5 m and +15 m, then
   stop. Retries never re-run the backup.

**Restore:** the same flow on `scorecard-key` / `sidecar-key`. Scorecard fields
(`outcome`, `objectives`, `integrity`, `measured`, `evidence.offsetReportSha256`)
are copied **from the relayed bytes** in the verification patch, by JSON pointer
as today (`controllers/restore.rs:1485-1510`). The `evidenceRead` grant comes from
the evidence destination.

**Legacy inline objects:** unchanged. They use the global handle through
`observe_archive` (`backup.rs:693`), `observe_scorecard` (`restore.rs:1570`) and
`verify_oracle` (`verification.rs:703`).

### 3.10 Scope of the global handle and the `ControllerIdentity` store cache

- **Global handle limits.** `Context::archive` (`controllers/mod.rs`, the `archive`
  field) is used only for objects without destination refs.
- **Retention guard.** `BackupSchedule` retention with the global handle is
  evaluated only when the schedule's `archive.url` bucket equals the global
  `LOGWEIR_ARCHIVE_URL` bucket. Otherwise the report is omitted and one INFO line
  names both buckets. This stops reports describing another destination's catalog
  (G14).
- **Destination-backed schedules** get no retention report until PLAT-16.1 adds
  the per-destination archive-inventory check kind.
- **`evidence_store::StoreCache`** (new, W7):
  - LRU of at most 32 read-only stores keyed by
    `(destination uid, generation, caSha256)`.
  - Built **inside** `spawn_blocking` with
    `Store::read_only_with(location.evidence_storage_url(), StoreOptions{credentials: Ambient, root_certificates, …})`.
  - Only for locations in the policy allowlist.
- **I13 test amendment.** `crates/weirkeeper/tests/retention.rs:967`
  (`no_store_call_is_made_outside_spawn_blocking`) and the "main is the only
  constructor" assertion gain exactly one sanctioned site,
  `evidence_store.rs::StoreCache::get_or_build`. A mutant that constructs outside
  `spawn_blocking` must fail.

### 3.11 Required object-store permissions per role (S3 actions; documented in `docs/install.md`)

| Role | Actions | Resources | Used by |
|---|---|---|---|
| `archiveWrite` | `s3:ListBucket` (condition `s3:prefix` in `<prefix>/*`, `logweir/*`); `s3:GetObject`, `s3:PutObject`, `s3:AbortMultipartUpload` | `arn:aws:s3:::<bucket>/<prefix>/*`, `arn:aws:s3:::<bucket>/logweir/*` | Backup Job (engine and receipt) |
| `archiveRead` | `s3:ListBucket` (prefix `<prefix>/*`); `s3:GetObject` | `<bucket>/<prefix>/*` | Restore Job source; preflight archive checks |
| `evidenceWrite` | `s3:PutObject` (conditional create); `s3:GetObject` | `<bucket>/logweir/*` | Restore Job scorecard / offsets |
| `evidenceRead` | `s3:GetObject`; optional `s3:ListBucket` (prefix `logweir/*`) so absence is distinguishable from denial | `<bucket>/logweir/*` | Evidence-fetch Job or `ControllerIdentity` |
| write probe (optional) | `s3:PutObject` | `<bucket>/logweir/readiness/*` | Preflight with `writeProbe: CreateOnlyMarker` |

- **No role is ever granted `s3:DeleteObject`.** Guard G-RET is unchanged.
- **[MEASURE U6]** The minimal action set the engine needs for writes (multipart,
  listing, checkpoint reads) is measured in S1 with the MinIO policies in §14.3.
  The table above is recorded from the measured set, not assumed.

### 3.12 Converting inline `ArchiveRef` without changing plans or locations

1. **Existing objects are never mutated.** Specs are immutable, and legacy
   `Backup`/`Restore`/`BackupSchedule` objects keep the legacy path until
   terminal or suspended.
2. **Derive the destination from facts, not guesses.**
   `POST /api/v1/namespaces/{ns}/destinations:from-legacy` with
   `{sourceSchedule | sourceBackup, name, access}`, with kubectl equivalent recipes
   in `docs/kubernetes.md`, derives the storage block in this order:
   - (a) The PLAT-06.1 frozen `execution-inputs.json` of the newest `Succeeded`
     `Backup` gives the exact bucket, prefix, endpoint, region, `path_style` and
     `allow_http` the runner used. `addressingSource: frozenExecution`.
   - (b) Otherwise, the `archive.url` plus the installation's legacy addressing
     published in the policy `ConfigMap`, rendered from the same chart values as
     the Deployment env. `addressingSource: installationConfig`; the user must
     confirm.
     *(Amended 2026-09-17 at W12's integration: until W11 publishes the legacy
     addressing in the policy `ConfigMap`, the route reads no installation config
     and therefore never emits `installationConfig` — it takes (c). A provenance
     label must never name a source that was not read; a hard-coded AWS/TLS guess
     was reviewed and removed.)*
   - (c) Otherwise refuse with `legacy_location_unknown`.
3. **Mapping rules.**
   - Legacy `allow_http=true` becomes `InsecureHTTP` **only** with an `http://`
     endpoint. With `https://` it becomes `TLS`, with a note that `allow_http` was a
     no-op there.
   - Legacy `path_style=true` becomes `PathStyle`.
   - Virtual-hosted with an endpoint becomes `PathStyle`, with a note that engine
     0.21.0 used path-style anyway (G4).
   - `archive.secretRef.name` becomes `archiveWrite.secret.name` with default keys.
   - `evidenceRead` becomes `ControllerIdentity` only if the location is
     allowlisted; otherwise it is left absent and the UI prompts.
4. **Adopt without moving data.** Use the drain-and-retain procedure
   (`docs/kubernetes.md` §9) until PLAT-05.1: suspend the old schedule, wait for
   terminal children, then create a **differently named** schedule with
   `destinationRef`.
   - The API refuses the replacement when `locationDigest(destination)` differs
     from `locationDigest(legacy derived)`, unless `allowLocationChange: true` is
     sent explicitly.
   - Recovery points from both generations share the same prefix.
5. **Restores from legacy recovery points.** The API offers destinations whose
   `locationDigest` matches the point. Plan bytes rendered from the destination can
   differ textually from a legacy plan (endpoint normalization), which means a new
   plan hash and a new approval. **Old approvals are never reused** for different
   bytes.

### 3.13 Two-destination operation (acceptance contract)

Two destinations in one namespace with different endpoints, transports, CAs and
credentials:

- Each Job's env and plan storage come only from its own destination.
- The controller environment is unchanged and unused.
- Evidence verification for each uses its own `evidenceRead`.
- No object is written to the other bucket.
- No status, event, log line or API response contains a credential value.

The exact live assertions are S1 and S5 (§14).

---

## 4. Shared check framework (probe, discovery, preflight, evidence fetch)

### 4.1 Pure contract: `crates/logweir-core/src/check_contract.rs` (W1)

**Types:** `CheckPlan` (JSON, `deny_unknown_fields`), `CheckResult`, `CheckId`,
`CheckCode` (CamelCase names that satisfy the `metav1.Condition` reason regex),
`CheckState {Ready, NotReady, Unknown, Skipped}`,
`Gating {Blocking, Advisory, ExecutionOnly}`, and
`aggregate(&[CheckOutcome]) -> OverallState`.

**Frame format:**

- Frame writer `frames::write_*` and incremental decoder `frames::Decoder`, using
  `base64` and `hex`, which `logweir-core` already has. No new dependency.
- Every frame line is at most 4,096 bytes including the newline. This stays under
  the CRI 16 KiB partial-line split.

| Line | Meaning |
|---|---|
| `logweir-check-topic=<name>\t<partitions>\t<flags>` | Inventory entry. Byte-order sorted. Flags `-` or a comma list of `internal`, `expected`, `error:<Code>`. |
| `logweir-check-part=<stream>:<seq>/<total>:<base64>` | Streams `result`, `details`, `evidence.payload`, `evidence.sidecar`; ≤ 3,000 base64 characters per part. |
| `logweir-check-end=<compact JSON>` | Last stdout line: `{"contract":"logweir.dev/check-result/v1","planSha256":…,"subjectUid":…,"streams":{"<stream>":{"parts":N,"sha256":"sha256:…"}},"topicLines":{"count":N,"sha256":"sha256:…"}}` |

**Other pure functions:**

- **Redaction** `redact(&str) -> String`, applied to every `message`/`remedy`
  (≤ 512 characters) and to every relayed status message. It removes:
  - URL userinfo;
  - `(AKIA|ASIA)[A-Z0-9]{16}`;
  - `aws_secret_access_key|secret[_-]?access[_-]?key|password|sasl.password|token` value forms;
  - PEM blocks;
  - S3 XML bodies (only the `<Code>` element is kept);
  - base64 or hex runs ≥ 40 characters.
  - Every pattern has a mutant test.
- **Visibility policy** `visibility(signals, attestation, now) -> Visibility` (§5.4).
- **Binding digest** `inputs_digest(&BindingInputs) -> String` (§6.6). The API
  and controller share it.

### 4.2 Runner: `logweir check run` (W4)

**Invocation:** argv
`["check","run","--plan","/check/check-plan.json","--check-contract-version","1"]`,
with env `LOGWEIR_CHECK_CONTRACT_VERSION=1`,
`LOGWEIR_CHECK_PLAN_SHA256=sha256:<hex>`, `LOGWEIR_CHECK_SUBJECT_UID=<uid>`,
`RUST_LOG=warn` and `TMPDIR=/work`.

**Startup order (no network before step 5):**

1. Parse argv.
2. Read plan bytes once.
3. Check the SHA-256 against the env value.
4. Parse strictly; kind supported; subject UID equals env.
5. Build clients.

A failure in steps 1–4 exits 3 with `refusal-reason=CheckContractMismatch` and
prints no frames. An old runner rejects `check` as an unknown subcommand, which the
controller maps to `RunnerContractUnsupported` (§4.3).

**Plan kinds:**

| Kind | Inputs | Actions |
|---|---|---|
| `topicInventory` | connection, `includeInternal`, `expectedTopics` ≤ 500, `maxTopics`, relay budget | §5.2 |
| `operationReadiness` | operation `backup`; source connection; destination (`archiveRead`, optional `evidenceWrite` probe); topics ≤ 1,000; signer path | Checks in §6.3 |
| `restorePreflight` | Plan bytes at `/check/plan.yaml` plus sha; target connection; source and evidence destinations; `backupId`; `manifestKey`; checks | §6.7 |
| `destinationAccess` | destination; roles | archive list, evidence get of a nonexistent key (classifies denial vs not-found), optional marker |
| `evidenceFetch` | destination role `evidenceRead`; ≤ 3 objects `{role, key, maxBytes}` (payload ≤ 1 MiB, sidecar ≤ 64 KiB) | get and relay |
| `sourceConnection` | connection | One metadata read: `connection.authenticated`, beside the `runner.contract` row every kind emits. No destination, no signer, no plan, no topic. (Amended 2026-09-18, d2-source-check.) |

**Exit codes:**

- 0: an end line was printed, whatever the per-check states.
- 1: an operational failure before a result existed; no end line.
- 3: contract refusal.
- 2 and 4 are never used.

**Hard restrictions:**

- Never invokes the engine binary, so `scripts/check-no-oso.sh` passes unchanged.
- Never writes, except the optional create-only marker
  `logweir/readiness/<destinationUid>.json` through `Store::put_create_only`.
  "Already exists" counts as write-authorized, because S3 and MinIO authorize
  before evaluating the precondition **[VERIFY U7]**.
- Never prints a credential.
- Stderr carries JSON tracing at `warn`.

**Error classification** (W2 for the store, W3 for Kafka). Raw errors are never
printed, only codes plus a redacted message:

| Source | Codes |
|---|---|
| Kafka | `BrokerUnreachable`, `AuthenticationFailed`, `TlsHandshakeFailed`, `TlsTrustFailed`, `MetadataTimeout`, `ClusterAuthorizationFailed`, `TopicAuthorizationFailed`, `UnknownTopicOrPartition`. W3 installs a capturing `ClientContext::error` so that SASL and TLS failures are not reported as plain timeouts **[VERIFY U5]**. |
| Store | `AccessDenied`, `InvalidCredentials` (`InvalidAccessKeyId`, `SignatureDoesNotMatch`, `ExpiredToken`), `BucketNotFound`, `ObjectNotFound`, `EndpointUnreachable`, `TlsTrustFailed`, `RegionMismatch`, `Timeout`, `StoreErrorUnclassified`. |

### 4.3 Controller: `crates/weirkeeper/src/check/` (W5)

| Module | Responsibility |
|---|---|
| `job.rs` | `CheckJobSpec -> job::RunnerJobSpec`. **Name** `lwc-<k>-<first 20 hex of sha256(owner uid)>`, where `k` ∈ `td` (inventory), `rd` (readiness), `rp` (restore preflight), `da` (destination access), `ev` (evidence, uid+attempt), `cs` (catalog sync — D3's `catalogSync`, which never reached this table) or `sc` (source connection; amended 2026-09-18). The name is always ≤ 63 characters and independent of the owner name, so there is no `NameTooLong` path. **Labels** on the Job and pod template: `app.kubernetes.io/managed-by: weirkeeper`, `app.kubernetes.io/component: check`, `logweir.dev/check-kind: <kind>`, `logweir.dev/check-owner-uid: <uid>`. **Deadline** `activeDeadlineSeconds = request.timeoutSeconds + 90`. `backoffLimit: 0`, `restartPolicy: Never`, the existing `failure_policy()`, no ServiceAccount token, ServiceAccount from the PLAT-07.1 execution context or destination grant (default `logweir-runner`), and no TTL at creation. `job.rs` gains `labels` and `template_labels` fields only. |
| `plan.rs` | Renders `<job>-plan` as an **immutable** `ConfigMap` owned by the check CR (or the `Backup`/`Restore` for `ev`). Keys: `check-plan.json`, trust files (`source-ca.pem`, `target-ca.pem`, `archive-ca.pem`, `evidence-ca.pem`) and `plan.yaml` (verbatim restore bytes). The digest is pinned in the Job env. A 409 is accepted only if the existing object has a matching owner UID, digest and `immutable: true`; otherwise `CheckPlanConflict` (terminal). |
| `pod.rs` | `find_owned_pod(ns, &job)` lists by `batch.kubernetes.io/job-name` and keeps only pods whose `ownerReferences` contain `{kind: Job, uid: job.uid, controller: true}`. It ignores and logs `ForeignPodIgnored`. No legacy-label fallback for check Jobs. Fixes G10 for new code. W10 switches the backup, restore and probe callers. |
| `waiting.rs` | Pure classification of pod, Job and event state (table below). |
| `relay.rs` | `LogParams{container: Some("runner"), limit_bytes: Some(8 MiB)}`. Decodes frames and requires the end line, `planSha256`, `subjectUid`, per-stream part counts and digests, and the topic-line count and digest. Any mismatch is `ResultUnreadable`, reported with the exit code but without log content. |
| `chunks.rs` | Writes immutable result `ConfigMap`s `<job>-r<3-digit index>` or `<job>-details`, owned by the check CR. Annotations: `logweir.dev/result-format`, `logweir.dev/result-sha256`, `logweir.dev/chunk: "i/n"`. The 409 rule is the same as for plans; otherwise `ResultStorageConflict`. **Commit point:** all chunks are written first; the status patch that indexes them is the commit. |
| `cancel.rs` | For an owned, unfinished Job (UID verified), patch `spec.activeDeadlineSeconds: 1`. The Job fails with `DeadlineExceeded`, pods terminate, and the Job becomes finished so the TTL applies **[VERIFY U3]**. Fallback if the API server refuses: `spec.suspend: true`, which deletes active pods; the CR's owner cascade removes the Job. Foreign or unowned Jobs are never touched. |
| `limits.rs` | Policy limits (§4.4): per-namespace and total active check Jobs, counted by listing Jobs with label `app.kubernetes.io/component=check` whose status has no `Complete`/`Failed` condition, plus one active discovery per connection UID. Over the limit: status `phase: Queued`, reason `ConcurrencyLimited`, requeue 10 s. A small overshoot under concurrent reconciles is accepted and documented. Evidence fetches use a separate per-namespace pool so verification cannot be starved by interactive checks. |
| `gc.rs` | Deletes a terminal `TopicDiscovery` when `now > observedAt + retentionSeconds` or when it is outside the newest `keepPerConnection` terminal discoveries for the same connection UID. Deletes a terminal `Preflight` when `now > expiresAt + retentionSeconds` (or `observedAt + …` if never ready). Always uses `DeleteParams{preconditions: {uid}}`. Stuck non-terminal objects (no Job, older than 2 × timeout plus 5 m) become `Failed/Stalled` first. `ConfigMap`s and Jobs go through owner cascade. |
| `policy.rs` | Loads and validates the policy `ConfigMap` (§4.4) with a 30 s cache. Parse failure fails closed: empty attestations and allowlist, plus the readiness check `configuration.policy notReady PolicyUnreadable`. |

**Waiting classification (`waiting.rs`):**

| Observation | Code |
|---|---|
| Container `waiting.reason=CreateContainerConfigError`, message `secret "X" not found` | `CredentialSecretNotFound{secret: X}` |
| Same reason, message `couldn't find key K in Secret NS/X` | `CredentialSecretKeyMissing{secret, key}` |
| `configmap "X" not found` | `TrustBundleNotFound{configMap}` |
| `ErrImagePull` / `ImagePullBackOff` | `RunnerImagePullFailed` |
| `ErrImageNeverPull` | `RunnerImageNotPresent` (remedy names architecture and pull policy; docs §2) |
| `InvalidImageName` | `RunnerImageInvalid` |
| `PodScheduled=False` reason `Unschedulable` for > 60 s | `PodUnschedulable` |
| Pod `ContainerCreating` > 60 s with Event `FailedMount` naming volume `signing` | `SigningKeyMissing`; other volumes give `VolumeMountFailed{volume}` |
| Job has no pod after 30 s, Event `FailedCreate`, message `serviceaccount "…" not found` | `RunnerServiceAccountMissing` |
| Other `FailedCreate` (quota, PodSecurity) | `PodCreateRejected` (redacted) |
| Pod `DisruptionTarget=True` | `DisruptedMidCheck` |

- **Mapping to check IDs.** A code is attributed to a check by matching the Secret
  or volume name against the plan's projections (connection, destination role or
  signer). An unmatched code blocks the whole pod: every Job-sourced check becomes
  `unknown` with `BlockedByPrerequisite`, and the named cause is reported.
- **Early cancel.** Blocking waiting states cancel the Job immediately through
  `cancel.rs`, so users do not wait for `activeDeadlineSeconds`.
- **TTL.** `ttlSecondsAfterFinished: 600` is patched only after the status commit
  (probe ordering, `kafka_cluster.rs:1146-1154`).
- **Restart behaviour.** Everything is derivable from CR status, the Job and the
  `ConfigMap`s. A restart between chunk writes and the commit re-decodes the relay
  (the Job still exists because no TTL is set) and accepts identical 409s.

### 4.4 Installation policy `ConfigMap` (W11 renders, W5 reads)

`weirkeeper-policy` lives in the release namespace. The Deployment points at it
with `LOGWEIR_POLICY_CONFIGMAP=<ns>/<name>` and adds
`LOGWEIR_INSTALLATION_NAMESPACE` through a `fieldRef` on `metadata.namespace`. The
key is `policy.json`:

```json
{"version": 1,
 "checks": {"maxActivePerNamespace": 4, "maxActiveTotal": 20, "maxActiveDiscoveriesPerConnection": 1,
            "maxEvidenceFetchActivePerNamespace": 4},
 "discovery": {"freshSeconds": 900, "retentionSeconds": 86400, "keepPerConnection": 5,
               "defaultMaxTopics": 20000, "hardMaxTopics": 50000,
               "visibilityAttestations": [
                 {"id": "att-orders-prod", "namespace": "team-a", "kafkaCluster": "source",
                  "clusterId": "M29I2S7FQPyHBEX12Vx7XA", "principal": "User:backup",
                  "attestedBy": "platform-admin@example.invalid",
                  "attestedAt": "2026-09-15T00:00:00Z", "expiresAt": "2026-12-15T00:00:00Z",
                  "statement": "User:backup has DESCRIBE on literal Topic:* with no DENY; reviewed ACL export 2026-09-14"}]},
 "preflight": {"defaultTimeoutSeconds": 120, "retentionSeconds": 3600},
 "engine": {"allowUnverifiedCustomCa": false},
 "evidence": {"controllerIdentityLocations": [{"endpoint": "https://minio-b.ns.svc:9000", "region": "", "bucket": "lw-b"}]},
 "legacyArchiveAddressing": {"endpoint": "", "region": "", "allowHttp": false, "virtualHostedStyle": false}}
```

- **Who can write it.** Only principals who can write that `ConfigMap` in the
  release namespace: chart and cluster administrators. Namespace operators cannot.
  This is the "explicit administrator-governed capability" the API decision
  requires for `attestedComplete`.
- **Provenance.** The policy digest is recorded in every check binding.

### 4.5 `KafkaCluster` probe reuse

- **Phase 1 (with W10).** `controllers/kafka_cluster.rs` uses
  `check::pod::find_owned_pod`, `check::waiting::classify` and the TTL helper. Probe
  reasons gain `CredentialSecretNotFound`, `CredentialSecretKeyMissing`,
  `RunnerImagePullFailed` and `RunnerImageNotPresent` (added to
  `PROBE_CONDITION_REASONS`, `kafka_cluster.rs:175`) instead of timing out as
  `NoExitCode`. The `cluster-probe` argv and the two I14 lines are unchanged, so a
  runner image without `check` keeps working.
- **Phase 2 (owned by PLAT-07.2; one release after the `check` contract is the
  minimum runner).** The probe becomes `check run` kind `connectionProbe`, which
  prints the I14 lines **and** frames. The I14 parser remains as a fallback.

---

## 5. `TopicDiscovery` (PLAT-09.1)

### 5.1 Resource shape

```yaml
apiVersion: logweir.dev/v1alpha1
kind: TopicDiscovery
metadata: {name: td-5f0c2a9b7e1d4c38, namespace: team-a}   # API-minted per idempotency scope; kubectl users choose any name
spec:
  request:                        # CEL: self == oldSelf  (required object, so always evaluated)
    connectionRef: {name: source} # required LocalRef -> KafkaCluster
    includeInternal: false        # default false
    expectedTopics: [orders, payments]  # optional, maxItems 500, items pattern ^[a-zA-Z0-9._-]{1,249}$, not '.'/'..'
    maxTopics: 20000              # default 20000, minimum 1, maximum 50000 (policy hardMaxTopics may lower)
    timeoutSeconds: 60            # default 60, minimum 10, maximum 300 (in-Job Kafka budget)
  cancelRequested: false          # CEL on .spec: (!has(oldSelf.cancelRequested) || !oldSelf.cancelRequested) || (has(self.cancelRequested) && self.cancelRequested)
status:
  phase: Succeeded                # Pending | Queued | Running | Succeeded | Failed | Cancelled
  reason: Succeeded               # CamelCase; failure codes in §4.2/§4.3; ConnectionNotFound, ConnectionInvalid, ResultUnreadable, RunnerContractUnsupported, DeadlineExceeded, CancelRequested, Stalled
  message: "…"                    # redacted, <= 1024
  binding: {connectionName: source, connectionUid: …, connectionGeneration: 1, principal: "User:backup",
            authMode: scramSha512, bootstrapSha256: "sha256:…", policyDigest: "sha256:…"}
  jobRef: {name: lwc-td-…, uid: …}
  queuedAt: …; startedAt: …; observedAt: …   # observedAt = runner container finishedAt (probe rule, kafka_cluster.rs:513-542)
  freshUntil: …                   # observedAt + discovery.freshSeconds
  result:
    format: logweir.dev/topic-inventory/v1
    clusterId: M29I2S7FQPyHBEX12Vx7XA
    brokerCount: 1
    counts: {listed: 5004, returned: 5003, internalExcluded: 1, errored: 0}
    truncated: false
    truncationReason: null        # MaxTopics | RelayLimit
    visibility: {state: limited, basis: [expectedTopicNotAuthorized], attestation: null}
    expected: {requested: 2, visible: 1, notAuthorized: 1, notFound: 0, unknown: 0}
    topicsSha256: "sha256:…"      # over the canonical TSV of the returned entries
    chunks:                       # maxItems 64
      - {name: lwc-td-…-r000, sha256: "sha256:…", count: 2500, firstName: "bulk-00000", lastName: "bulk-02499"}
  conditions: [{type: Complete, status: "True", reason: Succeeded}]
```

**Printer columns:** `CONNECTION`, `PHASE`, `VISIBILITY` (`.status.result.visibility.state`),
`TOPICS` (`.status.result.counts.returned`), `OBSERVED`, `AGE`.

**CEL rules.** The `spec.request` / `spec.cancelRequested` split exists so that one
transition rule on a required sub-object seals everything else, avoiding the
absent-to-present hole documented for `BackupSchedule`
(`crds/backup_schedule.rs:1-22`).

| Id | Placement | Rule | Message |
|---|---|---|---|
| T1 | `.spec.request` | `self == oldSelf` | `spec.request is immutable; create a new TopicDiscovery` |
| T2 | `.spec` | `(!has(oldSelf.cancelRequested) \|\| !oldSelf.cancelRequested) \|\| (has(self.cancelRequested) && self.cancelRequested)` | `spec.cancelRequested may only change from false to true` |

**Name length is unconstrained**, because the Job name is derived from the object
UID and never from the name (§4.3), so no `NameTooLong` path exists.

### 5.2 Algorithm

**Controller (W8):**

1. Handle a terminal object through GC, and a cancel request through `cancel.rs`.
2. Resolve the connection via the PLAT-07.1 resolver (§13 seam). A missing
   connection is `Failed/ConnectionNotFound`, terminal because the request is
   immutable. A resolver refusal is `Failed/<code>`.
3. Patch `binding`, then apply limits (`Queued` if over).
4. Render the plan: `topicInventory`, `maxTopics = min(request, policy.hardMaxTopics)`,
   relay budget 6 MiB of topic lines, deadline. Create the plan `ConfigMap`, then the
   Job, then set `phase: Running`.
5. While running, classify waiting states and cancel blocking ones (§4.3).
6. When finished: owned pod, then relay decode, then build TSV chunks
   (≤ 2,500 entries **and** ≤ 768 KiB each), then write chunks, then compute
   visibility (§5.4) with the policy attestation, then commit status, then TTL.

**Runner (W4, using W3):**

1. Connect with resolver auth, TLS and CA, `allow.auto.create.topics=false` and
   `client.id=logweir-check`.
2. `cluster_id()`.
3. `list_topics()` (all-topics metadata) and the broker count.
4. Record per-entry errors as flags.
5. For each expected topic **absent** from the listing, send targeted metadata one
   name at a time with a 2 s timeout, within a budget of 40 % of `timeoutSeconds`.
   Record `visible|notAuthorized|notFound|unknown`.
6. Sort bytewise. Exclude internal topics unless `includeInternal`. Truncate at
   `maxTopics` or at the relay budget.
7. Emit topic lines, then the `result` part (counts, `clusterId`, `brokerCount`,
   signals, expected summary and per-name results), then the end line.

### 5.3 Internal-topic exclusion

- **Rule.** `internal = true` iff the name starts with `__`. That covers
  `__consumer_offsets`, `__transaction_state` and `__share_group_state`: the Kafka
  reserved-name convention.
- **Why a name rule.** No flag is available through the safe client (G11).
- **No other heuristics.** `_schemas`, `_confluent-*` and Connect internal topics
  have configurable names and are **not** guessed. Users filter them by search.
- **Defaults.** Internal topics are excluded by default and counted in
  `counts.internalExcluded`. `includeInternal: true` returns them with the
  `internal` flag.

### 5.4 Visibility and completeness policy

**What Kafka does.** An all-topics Metadata request silently omits topics the
principal may not `DESCRIBE`. A successful listing therefore never proves full
visibility.

**What is detected:**

- (i) listing entries carrying `TopicAuthorizationFailed`. This is rare, because the
  broker filters unauthorized topics rather than reporting them.
- (ii) expected topics that return `TOPIC_AUTHORIZATION_FAILED` on a targeted
  request. The broker returns this for a principal without `DESCRIBE` whether or
  not the topic exists.

**What is not detected:**

- Topics nobody named.
- ACL-derived completeness, rejected for four reasons:
  - It needs `DescribeAcls` or `DescribeCluster`, which are unavailable without
    unsafe FFI.
  - It is authorizer-specific: `super.users` and
    `allow.everyone.if.no.acl.found` are broker config, not ACLs.
  - It cannot represent non-ACL authorizers such as MSK IAM or RBAC.
  - A plausible but wrong "complete" is the worst possible outcome.

**State algorithm** (pure `check_contract::visibility`):

```
limited          if signals.topicAuthorizationErrorInListing || expected.notAuthorized > 0
attestedComplete else if an attestation matches (namespace, kafkaCluster name, observed clusterId,
                        binding.principal) && now < attestation.expiresAt && !truncated
unknown          otherwise
basis ⊆ [listingOnly, topicAuthorizationErrorInListing, expectedTopicNotAuthorized, expectedTopicsAllVisible,
         truncated, administratorAttestation, attestationExpired, attestationPrincipalMismatch,
         attestationClusterIdMismatch]
```

**Principal string:** `User:<username>` for SCRAM and `User:ANONYMOUS` for
plaintext. The UI labels attestation as "attested by <attestedBy> at <attestedAt>;
not verified by Logweir".

**Distinguishable outcomes (PLAT-09.1 acceptance):**

| Outcome | How it shows |
|---|---|
| Empty | `phase=Succeeded`, `counts.returned=0`, visibility `unknown`/`limited`. UI: "No visible topics. Kafka hides topics this principal cannot describe; this is not proof the cluster is empty." |
| Failed | `phase=Failed` plus `reason` |
| Stale | API `stale=true` (§5.7) |
| Permission-limited | `visibility.state=limited` |

### 5.5 Result storage and size math

- **Format.** TSV line = `name \t partitions \t flags \n`, where Kafka-legal names
  contain no tab or newline.
- **Worst-case line.** A 249-character name, 6 digits, ~24 characters of flags and
  separators: ≤ 283 bytes. A typical 40-character name gives ~74 bytes.
- **Chunk.** ≤ 2,500 lines and ≤ 768 KiB of data, which is under the 1 MiB
  `ConfigMap` data limit with metadata headroom. Worst case per chunk is
  2,500 × 283 B ≈ 675 KiB.

| `maxTopics` | Typical (74 B) total / chunks | Worst (283 B) total / chunks |
|---|---|---|
| 5,000 | 353 KiB / 2 | 1.35 MiB / 2 |
| 20,000 (default) | 1.41 MiB / 8 | 5.4 MiB / 8 |
| 50,000 (hard max) | 3.5 MiB stored / 20 chunks (relay ≈ 4.5 MiB) | relay-capped: 6 MiB / 303 B ≈ 20,700 entries, then `truncated: RelayLimit` |

**Relay budget.** 6 MiB of topic lines. A relayed line is the TSV line plus the
20-byte `logweir-check-topic=` prefix: about 94 B typical and 303 B worst case. So
the budget carries roughly 66,900 typical or 20,700 worst-case entries, and the
default `maxTopics` of 20,000 fits even at worst-case names (5.8 MiB). It stays
under the controller's 8 MiB read limit and the default kubelet
`containerLogMaxSize` of 10 MiB **[VERIFY U4]**. Anything above it is
`truncated: RelayLimit`.

**etcd bound per discovery:** ≤ 5.7 MiB of chunk data (20,000 worst-case entries),
and ≤ 3.5 MiB at the 50,000 typical shape. Across a namespace it is bounded by
`keepPerConnection` (5) × connections, plus 24 h retention.

### 5.6 Pagination and search (API, §8)

- **Order.** Items are served in stored order (bytewise name).
- **Page size.** `limit` default 50, maximum 200.
- **Cursor.** Opaque, authenticated with the API cursor key (API decision). It
  binds `{actor, namespace, discoveryUid, topicsSha256, filtersHash, chunkIndex, offset, expiresAt(15 m)}`.
- **Filters.**
  - `q`: case-insensitive substring.
  - `prefix`: uses `firstName`/`lastName` to skip chunks.
  - `internal`: `exclude|include`, only when stored.
  - `errored`: `include|exclude|only`.
- **Scan budget.** ≤ 8 chunks per request. A sparse `q` returns a short page with
  `scan.complete=false` and a `nextCursor`.
- **Integrity checks per chunk:**
  - `ownerReferences[0].uid == discovery uid`;
  - `immutable == true`;
  - the annotation digest equals `status.result.chunks[i].sha256` and equals
    sha256 of the data.
  - Any mismatch returns 409 `result_integrity_failed`.
- **Collected objects.** A deleted discovery or `ConfigMap` returns 410
  `cursor_expired` or 404.
- **Never** list all `ConfigMap`s, and never collect an unbounded list.

### 5.7 Freshness and staleness

The API computes `stale = true` when any of these holds:

- `now > freshUntil`;
- the connection binding changed (UID, generation, username, auth mode or
  bootstrap digest);
- a newer `Succeeded` discovery exists for the same connection and parameters
  (`superseded`).

Failed discoveries do not hide the last successful one. The API returns
`latestAttempt` and `lastSuccessful` separately.

### 5.8 Cancellation, TTL, cleanup

- **Cancel.** `spec.cancelRequested: true`, or `POST …:cancel`, calls `cancel.rs`
  and ends in `phase: Cancelled`, reason `CancelRequested`. Frames that still arrive
  are discarded and no chunks are written.
- **Cancel is idempotent.** Cancel after a terminal phase is a no-op, and the API
  returns 200 `alreadyTerminal`.
- **Timeout.** The Job deadline is `timeoutSeconds + 90`.
- **TTL and GC.** The Job TTL is 600 s after commit. CR retention is 24 h, with
  keep-last 5 per connection (§4.3). `ConfigMap`s go by owner cascade.

### 5.9 Credential rotation

- **New pods see new credentials.** Each Job projects the Secret at pod start.
- **Invisible content rotation.** The controller cannot see Secret content changes
  (no Secret verb). Results therefore state "reflects credentials in effect at
  observedAt".
- **Visible reference changes.** If PLAT-07.1 makes credential references mutable,
  a generation bump or username change marks old results stale (§5.7).
- **Rotation that breaks auth.** The next discovery is
  `Failed/AuthenticationFailed`, while the previous success stays readable and
  stale.

### 5.10 API and UI delivery

- **API routes:** §8.
- **Legacy UI.** The direct-CR proxy UI (`charts/logweir/templates/ui/ui.yaml`)
  gets read-only summaries only: counts, visibility, phase. Browsing topics needs
  the console API, because chunk `ConfigMap`s are outside the proxy path regex (§9).
- **Expected-topic seeding.** The API fills `expectedTopics` from the union of
  topics named by `BackupSchedule`s and the latest 20 `Backup`s whose `sourceRef`
  is the connection (≤ 500), plus user-supplied names. The controller never lists
  schedules for this.

### 5.11 Seam with PLAT-09.2 (not implemented here)

- **Freshness per run.** Dynamic "all user topics" runs must discover **afresh
  inside or immediately before the run** and freeze the names in the `Backup`
  snapshot. A `TopicDiscovery` result is never an execution input.
- **Shared code, stricter policy.** PLAT-09.2 may reuse
  `crates/logweir/src/check/topics.rs`. Its completeness policy for "whole-cluster"
  claims must reuse §5.4 unchanged: never better than `unknown` without an
  attestation.

---

## 6. `Preflight`: operation readiness and restore preflight (PLAT-03.1 / PLAT-03.2)

### 6.1 Decision: credential-consuming check Job, not scoped Secret access

**Chosen: the check Job**, because:

- (a) The controller already reads no Secret, and that invariant is tested (G8).
- (b) Only the real execution pod shape proves the real prerequisites: image pull
  on a schedulable node, ServiceAccount existence, Secret and key projection,
  kubelet namespace resolution, CA trust, egress, SASL, S3 signature and signer
  loadability. A controller reading a Secret would prove only that bytes exist.
- (c) Credential exposure equals the execution Job's, and ends with the pod.

**Costs accepted:**

- One pod per preflight, typically 10–40 s.
- Namespace quota consumption.
- A missing Secret blocks the whole pod; dependent checks are reported
  `BlockedByPrerequisite` with the exact cause.

**Rejected: narrowly scoped Secret access**, for the aggregation, rotation and RBAC
maintenance reasons in §3.8 B and B'. It would also give only "Secret present",
never "credential works".

### 6.2 Resource shape

```yaml
apiVersion: logweir.dev/v1alpha1
kind: Preflight
metadata: {name: pf-9a1b…, namespace: team-a}
spec:
  request:                                   # CEL: self == oldSelf
    operation: Restore                       # enum [Backup, Restore, DestinationAccess, SourceConnection]
    sourceConnection:                        # SourceConnection only (amended 2026-09-18)
      connectionRef: {name: source}          # the KafkaCluster to dial
    # exactly one of backup|restore|destinationAccess, matching operation (CEL)
    backup:
      sourceRef: {name: source}
      destinationRef: {name: primary}        # xor legacyArchive
      legacyArchive: {url: "s3://…", secretRef: {name: logweir-s3}}
      topics: [orders]                       # 1..1000, named (G-GLOB pattern)
      scheduleRef: {name: nightly}           # optional context
    restore:
      planBytes: "…"                         # <= 262144; xor restoreRef
      planHash: "sha256:<64 hex>"            # required with planBytes; controller recomputes
      restoreRef: {name: restore-1a2b3c4d, uid: …}   # existing Restore (e.g. awaiting approval)
      targetRef: {name: target}              # required with planBytes; derived from Restore otherwise
      sourceDestinationRef: {name: primary}  # xor legacySourceArchive; with evidenceDestinationRef
      evidenceDestinationRef: {name: evidence}
      legacySourceArchive: {url: "s3://…", secretRef: {name: logweir-s3}}
      recoveryPointRef: {name: logweir-backup-nightly-20260915-030000, uid: …}  # PLAT-11.1 identity
    destinationAccess:
      destinationRef: {name: primary}
      roles: [ArchiveRead, EvidenceRead]     # 1..4
    skipChecks: [archive.segments]           # optional, maxItems 32; skipped blocking checks keep overall unknown
    timeoutSeconds: 120                      # default policy.preflight.defaultTimeoutSeconds, 30..600
  cancelRequested: false                     # false -> true only
status:
  phase: Completed                           # Pending | Queued | Running | Completed | Failed | Cancelled
  reason: …
  binding:
    operation: Restore
    planHash: "sha256:…"
    inputsDigest: "sha256:…"                 # §6.6
    referents:                               # maxItems 16
      - {kind: KafkaCluster, name: target, uid: …, generation: 1}
      - {kind: BackupDestination, name: primary, uid: …, generation: 3}
      - {kind: Backup, name: …, uid: …}
    policyDigest: "sha256:…"
  jobRef: {name: lwc-rp-…, uid: …}
  observedAt: …
  result:
    state: notReady                          # ready | notReady | unknown
    expiresAt: …                             # min over non-skipped checks
    checks: [ … ]                            # maxItems 64, schema §6.4
    detailsRef: {name: lwc-rp-…-details, sha256: "sha256:…"}
  conditions: [{type: Complete, …}, {type: Ready, status: "False", reason: NotReady}]
```

**Printer columns:** `OPERATION`, `PHASE`, `RESULT` (`.status.result.state`),
`EXPIRES`, `AGE`.

**CEL rules:**

| Id | Placement | Rule | Message |
|---|---|---|---|
| P1 | `.spec.request` | `self == oldSelf` | `spec.request is immutable; create a new Preflight` |
| P2 | `.spec` | `(!has(oldSelf.cancelRequested) \|\| !oldSelf.cancelRequested) \|\| (has(self.cancelRequested) && self.cancelRequested)` | `spec.cancelRequested may only change from false to true` |
| P3 | `.spec.request` | `self.operation == 'Backup' ? (has(self.backup) && !has(self.restore) && !has(self.destinationAccess) && !has(self.sourceConnection)) : self.operation == 'Restore' ? (has(self.restore) && !has(self.backup) && !has(self.destinationAccess) && !has(self.sourceConnection)) : self.operation == 'DestinationAccess' ? (has(self.destinationAccess) && !has(self.backup) && !has(self.restore) && !has(self.sourceConnection)) : (has(self.sourceConnection) && !has(self.backup) && !has(self.restore) && !has(self.destinationAccess))` | `exactly the block matching spec.request.operation may be set` |
| P4 | `.spec.request.backup` | `has(self.destinationRef) != has(self.legacyArchive)` | `set exactly one of destinationRef or legacyArchive` |
| P5 | `.spec.request.restore` | `has(self.planBytes) != has(self.restoreRef)` | `set exactly one of planBytes (a draft) or restoreRef (an existing Restore)` |
| P6 | `.spec.request.restore` | `!has(self.planBytes) \|\| (has(self.planHash) && has(self.targetRef))` | `a draft needs planHash and targetRef; the controller recomputes the hash` |
| P7 | `.spec.request.restore` | `has(self.sourceDestinationRef) == has(self.evidenceDestinationRef)` | `source and evidence destinations are set together` |
| P8 | `.spec.request.restore` | `has(self.sourceDestinationRef) != has(self.legacySourceArchive)` | `set exactly one of the destination refs or legacySourceArchive` |
| P9 | `.spec.request.restore` | `!has(self.planHash) \|\| self.planHash.matches('^sha256:[0-9a-f]{64}$')` | `planHash is sha256:<64 lowercase hex>, the form logweir_core::ids::sha256_prefixed produces` |

**Failure vocabulary:** `phase: Failed` means the check could not produce a
result, for example `ResultUnreadable`, `RunnerContractUnsupported` or `Stalled`.
It is distinct from `Completed` with `state: notReady`.

### 6.3 Check catalogue

**Legend.** Authority: **C** controller (credential-free), **J** check Job,
**P** pod status or events. Gating: **B** blocking, **A** advisory,
**E** execution-only. E checks are always reported `unknown` with a note and are
excluded from aggregation.

Operation `Backup` (the "Back up now" and schedule readiness contract, PLAT-06.2):

| id | Auth | Gate | Ready code | NotReady codes | Unknown codes | Expiry |
|---|---|---|---|---|---|---|
| `connection.resolved` | C | B | `Resolved` | `ConnectionNotFound`, `ConnectionInvalid`, `CredentialReferenceMissing` | — | 15 m |
| `connection.credentialProjected` | P | B | `Projected` | `CredentialSecretNotFound`, `CredentialSecretKeyMissing` | `PodNotStarted` | 15 m |
| `connection.authenticated` | J | B | `Authenticated` (facts: `clusterId`, `brokerCount`) | `BrokerUnreachable`, `AuthenticationFailed`, `TlsHandshakeFailed`, `TlsTrustFailed` | `MetadataTimeout`, `BlockedByPrerequisite` | 15 m |
| `connection.clusterIdentity` | J+C | B | `ClusterIdentityMatches` | `ClusterIdentityChanged` (≠ `KafkaCluster.status.clusterId`), `SourceIsAllowlistedTarget` (id ∈ `TrustRoster.allowedClusterIds`, which the phase −1 rail refuses) | `ClusterIdentityNotObserved` | 15 m |
| `connection.topicsDescribable` | J | B | `TopicsDescribable` | `TopicNotFound` (detail), `TopicNotAuthorized` (detail) | `TopicVisibilityUnknown` | 10 m |
| `connection.topicsReadable` | — | E | — | — | `ReadVerifiedOnlyAtExecution` | — |
| `destination.resolved` | C | B | `DestinationValid` | `DestinationNotFound`, `DestinationNotValid`, `DestinationRoleNotConfigured`, `ExecutionContextConflict`, `CaBundleUnsupportedByEngine` | — | 15 m |
| `destination.credentialProjected` | P | B | `Projected` | `CredentialSecretNotFound`, `CredentialSecretKeyMissing`, `WorkloadIdentityNotInjected` | `PodNotStarted` | 15 m |
| `destination.archiveListable` | J | B | `ArchiveListable` | `AccessDenied`, `InvalidCredentials`, `BucketNotFound`, `EndpointUnreachable`, `TlsTrustFailed`, `RegionMismatch` | `Timeout` | 15 m |
| `destination.evidenceWritable` | J | B if `writeProbe: CreateOnlyMarker`, else E | `MarkerWritten` / `MarkerAlreadyPresent` | as above | `WriteNotProbed` | 15 m |
| `destination.archivePrefixWritable` | — | E | — | — | `ArchivePrefixWriteVerifiedOnlyAtExecution` | — |
| `destination.evidenceReadable` | J or C | A | `EvidenceReadable` | as above | `EvidenceReadNotConfigured` | 15 m |
| `signer.privateKeyUsable` | J+P | B | `SignerUsable` (fact: public `signerKeyId`) | `SigningKeyMissing`, `SigningKeyUnreadable`, `SigningKeyInvalid` (`ValidatedSigner::load`, `crates/logweir/src/signer.rs:13-80`) | `PodNotStarted` | 15 m |
| `signer.rostered` | C | B | `SignerRostered` | `TrustRosterNotFound`, `TrustRosterNotLoaded`, `SignerNotRostered`, `SignerKeyExpired` | `SignerKeyIdNotObserved` | min(15 m, key `notAfter`) |
| `runner.image` | P | B | `ImageAvailable` (fact: `imageID`) | `RunnerImagePullFailed`, `RunnerImageNotPresent`, `RunnerImageInvalid` | `PodNotStarted` | 15 m |
| `runner.pod` | P | B | `PodStarted` | `RunnerServiceAccountMissing`, `PodCreateRejected`, `PodUnschedulable`, `VolumeMountFailed` | — | 15 m |
| `runner.contract` | J | B | `ContractSupported` | `RunnerContractUnsupported` | — | 15 m |
| `configuration.policy` | C | A | `PolicyLoaded` | `PolicyUnreadable` | — | 15 m |
| `configuration.egress` | — | E | — | — | `NetworkPolicyEnforcementNotObservable` (remedy lists broker and object-store ports, e.g. a non-443/9000 endpoint port) | — |

Operation `DestinationAccess` runs `destination.*`, `runner.*` and
`configuration.policy` for the requested roles.

Operation `SourceConnection` runs `connection.resolved`, `connection.credentialProjected`,
`connection.authenticated`, `connection.clusterIdentity`, `runner.*`, `configuration.policy`
and `configuration.egress` (execution-only), and nothing else. `connection.topicsDescribable`
is **not** reported: the request names no topic, and that row's vocabulary is about a topic
the requester named. On `connection.clusterIdentity`, `ClusterIdentityChanged` is blocking as
everywhere else, but `SourceIsAllowlistedTarget` is a `Backup` verdict and is not reported
here: whether a cluster may be backed up is a different question, guarded by the runner's
phase −1 rail. (Amended 2026-09-18, d2-source-check.)

Operation `Restore` runs the target equivalents (`target.resolved`,
`target.credentialProjected`, `target.authenticated`) and destination roles
`archiveRead` (source) and `evidenceWritable` (evidence), plus `signer.*`,
`runner.*` and:

| id | Auth | Gate | Ready | NotReady | Unknown | Expiry |
|---|---|---|---|---|---|---|
| `plan.parse` | C | B | `PlanParsed` | `PlanUnparseable`, `PlanHashMismatch` | — | until bytes change |
| `plan.bindings` | C | B | `PlanMatchesReferences` | `PlanDestinationMismatch`, `PlanEvidenceDestinationMismatch`, `PlanTargetMismatch` (bootstrap/auth ≠ target `KafkaCluster`), `PlanTopicsNotInRecoveryPoint` | — | 15 m |
| `plan.names` | C (pure phase-0 rules shared from `logweir_core::guard`) | B | `MappedNamesLegal` | `MappedTopicNameIllegal`, `TopicMappingIdentity`, `GlobInTopic`, `ExpansionInTopic` | — | until bytes change |
| `recoveryPoint.state` | C | B | `RecoveryPointSucceeded` | `RecoveryPointNotFound`, `RecoveryPointNotSucceeded`, `RecoveryPointUidChanged`, `RecoveryPointLocationMismatch` (frozen `locationDigest` ≠ source destination) | `RecoveryPointLocationUnknown` (the point predates `Backup.status.destination`, so no frozen location can be compared — never `ready`; amended 2026-09-18 at d2-status-destination's review) | 15 m |
| `archive.backupSet` | J | B | `ManifestReadable` | `BackupSetNotFound`, `ManifestUnreadable`, store codes | `Timeout` | 30 m |
| `archive.coverage` | J | B | `PointInTimeCovered` | `PointInTimeBeforeCoverage`, `PointInTimeAfterCoverage` (inclusive rules as in `logweir_core::spec::RestoreSpecBlock`), `TopicNotInBackupSet` | — | 30 m |
| `archive.segments` | J | B (skippable) | `SegmentsPresent` | `SegmentMissing` (count + ≤ 10 sample keys; full list in details) | `SegmentListTooLarge` (> 200,000 keys) | 30 m |
| `target.clusterIdentity` | J+C | B | `TargetAllowed` | `TargetNotAllowlisted` (scratch), `TargetEqualsSource`, `ClusterIdentityChanged` | — | 15 m |
| `target.scratchMarker` | J | B (scratch only) | `MarkerHealthy` | `MarkerTopicMissing`, `MarkerTopicErrored` | — | 15 m |
| `target.mappedTopics` | J | B | `MappedTopicsAbsent` | `MappedTopicExists` (detail) | `MappedTopicVisibilityUnknown` (targeted metadata `TopicAuthorizationFailed`) | **5 m** |
| `target.topicCreate` | J (validate-only `CreateTopics`) | B | `TopicCreateValidated` | `TopicCreateNotAuthorized`, `TopicConfigRejected`, `ReplicationFactorExceedsBrokers`, `MappedTopicExists` | `TopicCreateValidationUnsupported` | 5 m |
| `target.timestampBound` | J | B | `TimestampWithinBound` | `TimestampBoundExceeded` (same arithmetic as `phase0_admit.rs:629-680`) | `BrokerConfigsNotReadable` | 15 m |
| `target.logAppendTime` | — | E | — | — | `LogAppendTimeOverrideVerifiedOnlyAtExecution` (the execution probe creates a topic; preflight never does, G12) | — |
| `approval.state` | C | B when `restoreRef` is set; `skipped` (`SubjectNotCreated`) for drafts | `ApprovalVerified` | `ApprovalNotVerified` (carries the Approval's reason), `ApprovalExpired` (`KeyIdExpired` or v2 `expiresAt` passed), `ApprovalPlanMismatch`, `ApprovalSubjectMismatch` | `ApprovalPending` | min(10 m, key `notAfter`) |
| `approval.keyValidity` | C | A | `ApproverKeyValid` | `ApproverKeyExpiresBeforeDeadline` (`notAfter` < now + `deadlineSeconds`) | `ApproverKeyWindowUnknown` (no window is published for `matchedKeyId`, so nothing can be compared — never `ready`; added 2026-09-21, see below) | min(10 m, `notAfter`) (Amended 2026-09-19 at PREFLIGHT-APPROVAL-ROSTER's fix: the approval rows relay the Approval's own verdict and no longer read the roster, so `ApproverKeyExpiresBeforeDeadline` and the `min(10 m, notAfter)` re-check cap were SUSPENDED — defect APPROVAL-KEY-WINDOW-UNPUBLISHED; `KeyIdExpired` on the Approval reads `ApprovalExpired` here. **RESTORED 2026-09-21** at that defect's fix: `controllers::approval` publishes the matched key's window on `Approval.status.approverKeyWindow` (`keyId`, `notBefore`, `notAfter`, as the resolved `TrustPolicy` declares them) and both rows read it back — the window is the trust policy's, never the roster's. The warning is ADVISORY per this row's `Gate` column and §6.4's "Advisory `notReady` appears as warnings": it warns ahead of time and never refuses, because a key that closes mid-run does not invalidate an approval that verified while it was open. An absent or key-mismatched window is `unknown` and never `ready` — PLAT-19.1's acceptance that unevaluated or stale expiry information is unknown, not valid.) |

### 6.4 Result model and aggregation

**Per-check entry** (≤ 64 entries; the record is about 1.2 KiB):

```json
{"id": "target.mappedTopics", "category": "target",
 "scope": {"kind": "KafkaCluster", "name": "target", "uid": "…"},
 "state": "notReady", "gating": "blocking", "authority": "checkJob",
 "code": "MappedTopicExists",
 "message": "2 mapped target topics already exist on cluster BQ_nM8…",
 "remedy": "Choose a topic prefix nothing has used, or delete the topics on the target before restoring.",
 "observedAt": "…", "expiresAt": "…",
 "detail": {"count": 2, "sample": ["restore-20260915T030000Z-orders", "restore-20260915T030000Z-payments"]}}
```

**Aggregate:**

- `notReady` if any blocking check is `notReady`;
- otherwise `unknown` if any blocking check is `unknown` or `skipped`;
- otherwise `ready`.
- Advisory `notReady` appears as warnings. Execution-only checks never affect
  state.
- `expiresAt` is the minimum over non-skipped checks.

**UI wording:** "Ready: every pre-execution check passed. N permissions are
verified only when the run executes."

### 6.5 Redaction

- **What is stored.** Every message and remedy passes
  `check_contract::redact` and the 512-character cap. The status stores codes, not
  raw errors.
- **What is never stored:** Secret values, S3 error bodies, SASL error strings, or
  URLs with userinfo.
- **What may appear:** Secret and `ConfigMap` names and key names, which are public
  references.
- **Tests:**
  - W1 mutants for each pattern.
  - A controller-double body recorder test asserting no fixture secret string in
    any PATCH or POST.
  - Live `grep` over `kubectl get … -o yaml` and controller logs (S14).

### 6.6 Binding to the exact plan and invalidation

`inputs_digest(BindingInputs)` (pure, shared) is the SHA-256 of canonical JSON:

```
{version: 1, operation, planHash?, topics? (sorted, Backup),
 referents: sorted by (kind,namespace,name) of {kind, namespace, name, uid, generation},
 caBundles: sorted [{destinationUid, sha256}],
 roster: {uid, generation}, approval?: {uid, resourceVersion}, policyDigest}
```

- **Recording.** The controller records `binding` **before** creating the Job, from
  the objects it resolved.
- **Applicability.** The API returns a result as applicable only when all of these
  hold:
  - `phase=Completed`;
  - `now < result.expiresAt`;
  - `binding.planHash == draft.planHash`;
  - `binding.inputsDigest ==` the API's recomputation from current objects.
- **Otherwise** `applicable=false, stale=true` with
  `staleReasons ⊆ [expired, planHashChanged, referentChanged:<Kind>/<name>, caBundleChanged, policyChanged]`.

**Consequences:**

- Editing the target, recovery point, point in time, topic subset or mapping
  prefix changes the plan bytes and therefore the hash, so the result is stale.
- Choosing another target or destination, or a recreated one, changes a referent
  UID, so the result is stale.
- A destination access edit changes the generation, so the result is stale, while
  the approval is unaffected (§3.6).
- Deleting and recreating a recovery point (UID) makes the result stale.
- The approval resource version changes when verification status moves, so the
  result is stale.

### 6.7 Restore-specific checks: implementation notes

- **Archive checks** use `archiveRead`: manifest `get` of
  `<prefix>/<backupId>/manifest.json`, coverage from the manifest segments of the
  named topics, and a segment list under `<prefix>/<backupId>/` compared with
  `Store::segment_keys_for_set` (`logweir-store/src/lib.rs:665`).
- **Collision** needs two answers:
  - (a) targeted metadata per mapped name:
    - `UnknownTopicOrPartition` means absent;
    - present means `MappedTopicExists`;
    - `TopicAuthorizationFailed` means `unknown`.
  - (b) validate-only `CreateTopics` (`AdminOptions::validate_only(true)`, which
    the safe rdkafka API supports) with the pinned configs and replication factor.
    It also yields `TOPIC_ALREADY_EXISTS` for a topic hidden by `DESCRIBE` when
    `CREATE` is authorized **[VERIFY U2]**.
- **No writes.** No topic is created, altered or deleted by preflight.
- **Detail storage.** Details (missing segments, collisions) go to one immutable
  `<job>-details` `ConfigMap` (JSON lines, ≤ 768 KiB, truncated with count).

### 6.8 Execution-time guards remain authoritative

Unchanged guards:

- Backup controller: glob rail, destination admission (§3.6), PLAT-06.1 freeze and
  `CredentialNotRenderable`.
- Runner phase −1 source rails.
- Restore admission checks 0–9.
- PLAT-01 bundle revalidation.
- Runner phase 0: forbidden keys, mapping and names, allowlist and equality,
  marker, **collision** (`phase0_admit.rs:521-547`) and the LogAppendTime probe.
- G-WIN in phase 5.
- PLAT-02.2 signer validation.

The preflight never replaces any of them. Enforcement:

- **(a)** Source-scan test `no_execution_path_reads_preflight_or_discovery`
  (W9): `controllers/{backup,backup_schedule,restore,approval}.rs` and
  `crates/logweir/src/{backup,drill}/**` must not name `Preflight`,
  `TopicDiscovery`, `preflights` or `topicdiscoveries`.
- **(b)** Live race S15b: a green preflight, then a colliding topic is created,
  then the Restore is refused at execution with `exitCode=3` /
  `exitReason=GuardRefused`.
- **(c)** Clients (API or UI) may *require* an applicable ready preflight as
  friction for "Back up now" or restore submission (PLAT-06.2 / PLAT-12.1). They
  may never bypass execution guards, and absence of a preflight is never a server
  refusal.

### 6.9 Alignment with PLAT-11.2

Subset, mapping and prefix are plan bytes, so the preflight result is valid only
for the exact preview that is submitted. "No existing target topic is overwritten
by the ordinary path" stays enforced by phase 0. A failed restore's fresh-target
retry is a new plan, which means a new preflight and a new approval.

---

## 7. RBAC

### 7.1 Controller `ClusterRole` (`config/rbac/role.yaml` and `charts/logweir/templates/clusterrole.yaml`, rule for rule)

Added rules:

```yaml
  - apiGroups: ["logweir.dev"]
    resources: ["backupdestinations", "topicdiscoveries", "preflights"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["logweir.dev"]
    resources: ["backupdestinations/status", "topicdiscoveries/status", "preflights/status"]
    verbs: ["patch"]
  - apiGroups: ["logweir.dev"]
    resources: ["topicdiscoveries", "preflights"]
    verbs: ["delete"]        # expired terminal check requests only; UID preconditions (§4.3 gc.rs)
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["list"]          # FailedCreate/FailedMount of owned check Jobs/pods (field selector involvedObject.uid)
```

- **Unchanged:** Job `create/get/list/watch/patch`, which covers cancellation and
  TTL; `ConfigMap` `create/get`, which covers plan, chunk and details writes, policy
  reads and CA reads; pod `list`; `pods/log get`.
- **Still absent:** any `secrets` verb; `delete` on Jobs, `ConfigMap`s, `Backup`s,
  `Restore`s or `Approval`s; `pods/exec`; `pods/attach`.
- **Doctrine update.** `controllers/mod.rs:13-21` "it never DELETES" gains one
  sentence: the check reconcilers may delete their own expired terminal
  `TopicDiscovery`/`Preflight` objects with UID preconditions, and nothing else.
- **Gate compatibility.** Receivers named `discoveries.delete(`/`preflights.delete(`
  do not match `scripts/check-no-archive-write.sh`, whose delete token is anchored to
  store-shaped receivers.

### 7.2 Human roles (`config/rbac/*_role.yaml`, `charts/logweir/templates/human-roles.yaml`)

| Role | Added grants |
|---|---|
| `logweir-viewer` | `get/list/watch` on `backupdestinations`, `topicdiscoveries`, `preflights` |
| `logweir-operator` | `create` on those three; `patch` on `backupdestinations` (access and CA edits; CEL keeps location and transport immutable); `patch` on `topicdiscoveries`, `preflights` (CEL permits only `cancelRequested` false → true) |
| `logweir-approver` | `get/list` on `preflights` (approval context) |

- **No human role gains `configmaps` or `secrets`.**
- **Browsing inventories** requires the console API. Kubectl users see summaries in
  status.

### 7.3 Console API ServiceAccount (PLAT-17.1 per-namespace `RoleBinding`s)

- **Grants:** `get/list/watch/create` on the three kinds; `patch` on the three
  kinds (access edits and cancel); `get` on `configmaps`, used only for chunk and
  details reads verified by owner UID, immutability and digest (§5.6).
- **Credential creation.** Write-only credential creation reuses PLAT-07.1's
  credential-Secret builder and its narrow create rule. No Secret read.
- **Fallback.** If security review rejects `get configmaps`, adopt a
  `TopicDiscoveryResult` kind holding chunks in an immutable `spec`. That requires
  its own amendment and has the same size math.

### 7.4 Legacy in-cluster UI proxy (`charts/logweir/templates/ui/ui.yaml`)

- Add `get/list` on the three kinds only (summaries).
- No create. `ui/api.js` `WRITABLE_PLURALS` is unchanged, so new flows are
  console-only.

### 7.5 Runner ServiceAccount

Unchanged: no verbs, and `automountServiceAccountToken: false`.

---

## 8. API routes and DTOs (`crates/logweir-api/src/routes/`, API decision conventions)

**Conventions.** All routes sit under `/api/v1/namespaces/{ns}`. They use
`application/problem+json`, `Idempotency-Key` on durable POSTs (names `bd-`, `td-`,
`pf-` plus the hash per the API decision), `deny_unknown_fields` on inputs,
`limit`/`cursor` paging, and capabilities `destinations`, `topicDiscovery` and
`preflight` in `/session`.

### 8.1 Destinations (PLAT-08)

| Method | Route | Role | Notes |
|---|---|---|---|
| GET | `/destinations` | viewer | `{items: [DestinationSummary], page}` |
| GET | `/destinations/{name}` | viewer | `Destination` |
| POST | `/destinations` | operator | `DestinationCreate` → 201 `Destination` |
| POST | `/destinations/{name}:update-access` | operator | `{expectedGeneration, access, transport?: {caBundle}}` → 200, or 412 on generation mismatch |
| POST | `/destinations/{name}:test` | operator | Creates a `Preflight` `DestinationAccess` → 202 `Preflight` |
| POST | `/destinations:from-legacy` | operator | §3.12 |
| GET | `/destinations/{name}/usage` | viewer | Bounded list of schedules and recent backups by label `logweir.dev/destination=<name>` (set by the API on objects it creates) |

```json
// DestinationCreate
{"name": "primary", "description": "…",
 "storage": {"provider": "s3", "bucket": "kafka-backups", "prefix": "team-a/prod", "region": "us-east-1",
             "endpoint": "https://minio.storage.svc:9000", "addressing": "pathStyle"},
 "transport": {"security": "tls", "caBundle": {"configMapName": "minio-ca", "key": "ca.crt"}},
 "access": {
   "archiveWrite":  {"mode": "secretKeys", "secret": {"existing": {"name": "logweir-s3"}}},
   "archiveRead":   {"mode": "secretKeys", "secret": {"new": {"accessKeyId": "…", "secretAccessKey": "…"}}},
   "evidenceWrite": null,
   "evidenceRead":  {"mode": "archiveReadGrant"}},
 "readiness": {"writeProbe": "createOnlyMarker"}}
```

- **Write-only credentials.** `secret.new` values are written once through the
  PLAT-07.1 builder into `lwd-<destination>-<role>` with default keys, labelled
  `logweir.dev/credential-for`, owned by the destination, and never echoed.
- **Default `writeProbe` differs by client.** The API DTO defaults it to
  `createOnlyMarker`, disclosed in the form; the CRD default is `Disabled`.

```json
// Destination (response): never a credential value
{"name": "primary", "uid": "…", "generation": 3, "description": "…",
 "storage": {…}, "transport": {"security": "tls", "caBundle": {"configMapName": "minio-ca", "key": "ca.crt", "sha256": "sha256:…"}},
 "access": {"archiveWrite": {"mode": "secretKeys", "secretName": "logweir-s3", "keys": ["access-key-id", "secret-access-key"]},
            "archiveRead": {…}, "evidenceWrite": {"mode": "inheritsArchiveWrite"}, "evidenceRead": {"mode": "archiveReadGrant"}},
 "canonicalUrl": "s3://kafka-backups/team-a/prod", "locationDigest": "sha256:…",
 "status": {"valid": true, "reason": "Valid", "message": null},
 "default": true, "lastTest": {"preflightId": "pf-…", "state": "ready", "observedAt": "…", "stale": false}}
```

**Problem codes:**

- 422 `destination_invalid`, with field codes `transport_scheme_mismatch`,
  `insecure_http_requires_http_endpoint`, `ca_bundle_requires_tls`,
  `addressing_unsupported_by_engine`, `prefix_reserved`, `endpoint_not_origin`,
  `bucket_invalid`, `grant_mode_fields`.
- 409 `destination_location_immutable` and 409 `transport_downgrade_forbidden`, for
  attempts through `:update-access`.
- 409 `legacy_location_mismatch` (§3.12).
- 404 `legacy_location_unknown`.

### 8.2 Topic discoveries (PLAT-09.1)

| Method | Route | Role | Body / response |
|---|---|---|---|
| POST | `/connections/{name}/topic-discoveries` | operator | `{includeInternal?: false, expectedTopics?: [≤500], maxTopics?: int, reuseFresh?: true}` → 202 `TopicDiscovery`; 200 with `reused: true` when a fresh `Succeeded` discovery with the same parameter hash and connection binding exists and `reuseFresh` |
| GET | `/topic-discoveries/{id}` | viewer | `TopicDiscovery` |
| GET | `/topic-discoveries/{id}/topics?limit&cursor&q&prefix&internal&errored` | viewer | `{items: [{name, partitions, internal, expected?, errorCode?}], page: {limit, nextCursor, snapshot: "<uid>@<topicsSha256>"}, scan: {complete}}` |
| POST | `/topic-discoveries/{id}:cancel` | operator | 200 `{state}`; `alreadyTerminal` when terminal |
| GET | `/connections/{name}/topic-discoveries?latest=true` | viewer | `{latestAttempt, lastSuccessful}` |

```json
// TopicDiscovery
{"id": "td-…", "connection": {"name": "source", "uid": "…"}, "state": "succeeded",
 "reason": "Succeeded", "observedAt": "…", "freshUntil": "…", "stale": false, "staleReasons": [],
 "clusterId": "…", "counts": {"listed": 5004, "returned": 5003, "internalExcluded": 1, "errored": 0},
 "truncated": false, "visibility": {"state": "limited", "basis": ["expectedTopicNotAuthorized"], "attestation": null},
 "expected": {"requested": 2, "visible": 1, "notAuthorized": 1, "notFound": 0, "unknown": 0},
 "error": null}
```

- **States:** `pending|queued|running|succeeded|failed|cancelled`.
- **`error`:** `{code, message, remedy}`, redacted.
- **Rate limit** per (actor, namespace): 6 creates per minute, burst 3, then 429
  with `Retry-After`.

### 8.3 Preflights (PLAT-03)

| Method | Route | Role | Body / response |
|---|---|---|---|
| POST | `/preflights` | operator | `PreflightCreate` → 202 `Preflight` |
| GET | `/preflights/{id}` | viewer; approver for restore | `Preflight` |
| GET | `/preflights/{id}/details?check=<id>&cursor` | viewer | Paged detail entries from `<job>-details` |
| POST | `/preflights/{id}:cancel` | operator | as §8.2 |
| GET | `/operations/preflight/{id}` and `/operations/discovery/{id}` | viewer | API decision normalized operation |

```json
// PreflightCreate
{"operation": "restore",
 "restore": {"planBytes": "…", "planHash": "sha256:…", "target": "target",
             "sourceDestination": "primary", "evidenceDestination": "evidence",
             "recoveryPoint": {"backupName": "logweir-backup-nightly-20260915-030000", "backupUid": "…"},
             "restoreName": null},
 "skipChecks": [], "timeoutSeconds": 120}
// or {"operation": "backup", "backup": {"sourceConnection": "source", "destination": "primary", "topics": ["orders"], "schedule": null}}
```

The fourth operation's spelling (amended 2026-09-18):

```json
{"operation": "sourceConnection", "sourceConnection": {"connectionRef": "source"}}
```

```json
// Preflight
{"id": "pf-…", "operation": "restore", "state": "notReady",
 "binding": {"planHash": "sha256:…", "inputsDigest": "sha256:…"},
 "applicable": false, "stale": true, "staleReasons": ["planHashChanged"],
 "observedAt": "…", "expiresAt": "…", "checks": [/* §6.4 entries */], "warnings": [/* advisory */],
 "executionOnly": [{"id": "target.logAppendTime", "note": "…"}]}
```

- **Applicability.** `applicable` and `stale` are recomputed per GET (§6.6); the API
  sends the current draft `planHash` as the `?planHash=` query parameter.
- **States:** `pending|queued|running|ready|notReady|unknown|failed|cancelled`.
- **Rate limit:** 20 creates per minute, burst 5.
- **Plan bytes** are forwarded verbatim; never re-serialized, as in the API decision.

---

## 9. UI touchpoints (after PLAT-13.2, PLAT-17.1 client migration, W12)

| Surface | Change |
|---|---|
| New `ui/pages/destinations.js`; route in `ui/app.js:26-35` | List and create (location, independent **transport** radio TLS/InsecureHTTP, **addressing** radio PathStyle/VirtualHosted, CA `ConfigMap`, four grants with existing-Secret or write-only new-credential entry). "Test access" shows the `Preflight` checks with observed time and scope. "Rotate access" uses `:update-access`. Credential inputs are never persisted in drafts (PLAT-13.2). |
| `ui/pages/schedules.js:192-243` | Replace the archive URL and Secret inputs with a destination selector, defaulting to the namespace default destination. Legacy inline fields move behind "Advanced (legacy inline archive)". Readiness panel from `Preflight` `Backup`. Topic picker from the latest fresh discovery, with manual entry retained and stale, failed and limited states labelled. |
| `ui/pages/clusters.js` (detail) | "Discover topics" panel: start, cancel, progress, visibility banner (`unknown`/`limited`/`attestedComplete` with basis), searchable paged table, stale badge, last successful versus latest attempt. Connection health (`status.reachable`) is labelled "connection probe", never "ready". "Test connection" creates a `SourceConnection` `Preflight` and renders its rows; the list's per-row control is relabelled "Re-read probe" (amended 2026-09-18). |
| `ui/pages/restore-wizard.js` | Step 1 takes the source destination from the recovery point's frozen destination (`locationDigest` match). Remove the per-restore endpoint, region and path-style fields (`:160-190`) for destination-backed points. Add an evidence destination selector. **Fix G5 now:** delete the `allowHttp` derivation at `:1142-1149`, or for legacy drafts replace it with an explicit "Allow insecure HTTP (explicit)" control defaulting off and independent of path-style. Step 5 becomes a real `Preflight` bound to the plan hash, with a stale banner on any edit and a re-run button. Replace the `preflightSentence` copy (`ui/render.js:223-230`). |
| `ui/plan.js:262-276` | Unchanged grammar; storage fields are fed from the destination, never from the addressing checkbox. |
| Legacy direct-CR mode | Read-only summaries only (§7.4). |

**Browser tests:**

- `restore_wizard_path_style_does_not_enable_http`: a unit behaviour gate in
  `scripts/check-ui-behaviour.sh`, plus a Playwright journey.
- Draft stale on target edit.
- Discovery paging and search.
- Destination create with write-only credential never echoed (network capture).

---

## 10. Helm values and documentation

**`charts/logweir/values.yaml` additions** (with `values.schema.json`):

```yaml
checks:
  maxActivePerNamespace: 4
  maxActiveTotal: 20
  maxEvidenceFetchActivePerNamespace: 4
  discovery: {freshSeconds: 900, retentionSeconds: 86400, keepPerConnection: 5, defaultMaxTopics: 20000,
              hardMaxTopics: 50000, visibilityAttestations: []}
  preflight: {defaultTimeoutSeconds: 120, retentionSeconds: 3600}
evidence:
  controllerIdentityLocations: []   # [{endpoint, region, bucket}] for ControllerIdentity evidence reads
destinations: []                    # optional BackupDestination objects to render (like kafka.enabled renders KafkaCluster)
```

- **New template.** `templates/policy.yaml` renders `weirkeeper-policy`; the
  `legacyArchiveAddressing` block uses the same `archive.s3.*` values that
  `deployment.yaml:81-92` already renders.
- **Deployment env.** Add `LOGWEIR_POLICY_CONFIGMAP` and
  `LOGWEIR_INSTALLATION_NAMESPACE`.
- **Demo destination.** `minio.enabled` renders
  `BackupDestination/minio` with **explicit** `transport.security: InsecureHTTP`,
  an `http://` endpoint and `PathStyle`. It replaces the implicit
  `ternary .Values.archive.s3.allowHttp true $explicit` coupling for new objects;
  the legacy env stays for inline objects.

**Documentation (W11):**

- `docs/kubernetes.md`:
  - new §20 "Backup destinations" (schema, CEL, env rendering, CA, permissions,
    sentinel, legacy conversion, rollback);
  - §21 "Topic discovery" (visibility policy, attestation, size limits, paging,
    GC);
  - §22 "Operation readiness and restore preflight" (catalogue, gating,
    execution-only, binding, authority);
  - §7 table (nine kinds);
  - §9 retention scope;
  - §15.1/15.5 (global handle and env forwarding are legacy-only).
- `docs/install.md`: CRD wait list (nine), permissions matrix §3.11, policy
  `ConfigMap`.
- `charts/logweir/README.md`: new values.
- `docs/stability.md`: `check` exit-code contract; engine custom-CA limitation
  (U1); node-role credentials unsupported for destinations; inventory visibility
  limits.
- `ui/README.md`: new pages.

---

## 11. Compatibility, upgrade and rollback

### 11.1 Absent-field behaviour

- Objects without destination refs behave exactly as today, including global handle
  use and env forwarding. The retention bucket-mismatch guard (§3.10) is the one
  intended change.
- Absent `evidenceRead` means `NotAttempted`, with a detail naming the field.
- Absent `readiness.writeProbe` means `Disabled`.
- Absent `TopicDiscovery` request fields take the documented defaults.

### 11.2 Upgrade order

1. Apply CRDs: three new files plus additive fields and CEL on
   `backups`/`backupschedules`/`restores`. Wait for `Established`; verify the
   field paths exist, as in docs §12 "Upgrade, rollback and legacy Jobs".
2. Apply RBAC (the controller `ClusterRole` must precede the controller).
3. Roll out the controller and runner images **together**. The runner must support
   `--store-contract-version 1` and `check run --check-contract-version 1`. An old
   runner is refused visibly (§3.5, §4.2).
4. Render the policy `ConfigMap`.
5. Enable console routes (capability flags) after the API deploys.

### 11.3 Existing objects

- No conversion writes, and no in-flight object is touched.
- Legacy Jobs keep their Pod templates.
- Old signed archives verify unchanged; verification code paths for bytes are
  refactored, not reinterpreted, with fixture tests on existing signed receipts and
  scorecards (`e2e/fixtures/signed/*`).

### 11.4 Rollback

1. Suspend schedules that use `destinationRef`.
2. Wait until no non-terminal destination-backed `Backup` or `Restore` exists.
3. Wait until no `TopicDiscovery` or `Preflight` is `Running`, or cancel them.
4. Roll back the controller and runner.

**What the old controller does afterwards:**

- It refuses new sentinel `Backup`s with `ArchiveUrlUnreadable`, terminal and
  before any POST.
- It ignores the three new kinds.
- It does not re-verify terminal destination-backed objects; their stored
  verification stays.
- Check Jobs already finished without TTL remain until
  `kubectl --context <ctx> delete topicdiscoveries,preflights --all -n <ns>`
  cascades them.

**Do not create destination-backed objects after a rollback.** An old controller
would create a Restore Job with no archive credential and without the
`AWS_METADATA_ENDPOINT` pin, so on a cloud node its runner could fall back to the
node instance role. Disable the console destination capability during the rollback
window, and keep destination-backed schedules suspended.

**Keep, do not delete:** the CRDs (additive), `BackupDestination` objects
(re-adoption after roll-forward), and credential Secrets. Deleting CRDs deletes
their objects.

### 11.5 Old controller with new CRDs

Additive fields are ignored. CEL still rejects mixed shapes at admission.

---

## 12. Test matrix (tracker-required tests → layers)

**Layers:**

| Layer | Meaning |
|---|---|
| Unit | Pure (`logweir-core`, `waiting.rs`, `relay.rs`, …) |
| Double | Controller over `weirkeeper::testing` route table with body recorder (panics on unrecorded routes) |
| Runner | CLI or subprocess with fake reader/store |
| Live | docker-desktop scenario in §14 |

**Mutant rule.** "A guard without a mutant is not a guard": each W-task names at
least one planted regression per new guard in its report.

### PLAT-08.1

| Required test | Unit | Double | Runner/store | Live |
|---|---|---|---|---|
| Distinct endpoints/credentials | `destination::two_locations_render_distinct_storage_urls_and_digests` | `backup_controller::destination_backed_jobs_project_only_their_destination_and_no_controller_env` (mutant: re-add `archive_addressing_env()`, which must fail) | `store_options::explicit_static_credentials_ignore_ambient_env` | S1 |
| Denied location | `store_errors::access_denied_is_classified_without_body` | `preflight_controller::destination_access_denied_maps_to_blocking_not_ready` | `check_cli::destination_access_denied_code` | S3 |
| Malformed URL | `destination::validation_table` (http+TLS, https+InsecureHTTP, userinfo, path, query, prefix `logweir/`, `..`, bucket) | `destination_controller::virtual_hosted_with_endpoint_is_invalid`; `crd_shape::destination_cel_rules_are_emitted_verbatim` | — | S2 (exact 422 strings) |
| Namespace separation | — (`LocalRef` has no namespace) | `backup_controller::destination_ref_resolves_in_backup_namespace_only` (routes answer only ns A; a ns B GET panics) | — | S4 |
| Evidence/archive destination differences | `destination::evidence_url_is_bucket_root_logweir` | `restore_controller::plan_evidence_must_equal_evidence_destination`; `restore_job_projects_archive_read_and_evidence_write_separately` | `drill_store::evidence_store_uses_evidence_credentials_not_aws_env` | S5 |
| Secret values not returned | `redact::*` mutants | `no_status_or_configmap_body_contains_fixture_secret` | `check_cli::stdout_never_contains_projected_secret` | S1/S14 grep |

### PLAT-08.2

| Required test | Unit | Double | Runner | Live |
|---|---|---|---|---|
| HTTPS with path-style | `https_path_style_renders_allow_http_false` | Job env `AWS_ALLOW_HTTP=false` | MinIO TLS e2e (W2, gated) | S1 dest-a |
| Explicitly configured local HTTP | `insecure_http_requires_explicit_http_endpoint` | Job env `AWS_ALLOW_HTTP=true` only for dest-b | — | S1 dest-b |
| Destination edit during a draft | `binding::destination_generation_change_changes_digest` | API `preflight_applicability_goes_stale_on_destination_edit`; plan hash unchanged | — | S21 |
| Custom endpoint | `custom_endpoint_requires_path_style_for_engine` | — | W2 MinIO | S1, S2 |
| Archive/evidence separation | as 08.1 | as 08.1 | as 08.1 | S5 |
| Addressing never downgrades transport | CEL R2/R3 rule-text tests | — | UI `restore_wizard_path_style_does_not_enable_http` (mutant: re-add the `:1148` line) | S2 patch → 422; S22 browser |

### PLAT-09.1

| Required test | Unit | Double | Runner | Live |
|---|---|---|---|---|
| Large catalog | `frames::topic_lines_roundtrip_50000`; `chunks::size_bounds_worst_case_names` | `topic_discovery_controller::writes_immutable_owned_chunks_then_commits_index` (5,003 fixture → 3 chunks; restart between chunk 2 and commit accepts identical 409s) | `check_cli::topics_truncate_at_relay_budget` | S7 |
| Empty cluster | `visibility::empty_listing_is_unknown` | 0 chunks, counts 0 | fake reader empty | S10 |
| ACL-limited principal | `visibility::expected_not_authorized_is_limited`; `attestation_match_expiry_principal_cluster` | policy `ConfigMap` attestation applied only on exact match | fake reader: targeted `NotAuthorized` | S9 |
| Timeout | — | Job `DeadlineExceeded` → `Failed/DeadlineExceeded`; Kafka timeout → `Failed/BrokerUnreachable` | `check_cli::metadata_timeout_code` | S11 |
| Internal topics | `inventory::double_underscore_is_internal` | — | excluded count and include flag | S8 |
| Refresh | `gc::keeps_last_five_per_connection_and_uses_uid_preconditions` | API `reuse_fresh_and_force_new` | — | S12 |
| Credential rotation | `binding::principal_or_generation_change_marks_stale` | `waiting::credential_secret_key_missing_maps_to_code` | — | S12 |
| Cancellation (API decision) | — | `cancel::patches_deadline_only_on_owned_job` (foreign Job with same name untouched) | — | S13 |
| Spoofed pod (G10) | — | `pod::ignores_label_matching_pod_without_job_controller_owner` | — | S7 variant (optional) |

### PLAT-03.1

| Required test | Unit | Double | Runner | Live |
|---|---|---|---|---|
| Missing Secret/key | `waiting::secret_not_found_and_key_missing_messages` | Maps to `connection.credentialProjected` vs `destination.credentialProjected` by Secret name | — | S14a, S14b |
| Wrong credentials | `kafka_errors::sasl_failure_via_error_callback` | — | fake reader auth error | S14c |
| Storage denial | `store_errors::*` | mapping | fake store 403 | S14d, S3 |
| Image failure | `waiting::err_image_never_pull` | early cancel, `runner.image notReady` | — | S14e |
| Timeout | — | deadline mapping, dependents `BlockedByPrerequisite` | runner budget | S14f |
| Redaction | `redact::*` mutants (AKIA, secret key, userinfo, PEM, S3 XML, long base64) | body recorder scan | stdout scan | S14 grep |

### PLAT-03.2

| Required test | Unit | Double | Runner | Live |
|---|---|---|---|---|
| Stale inventory | — | `preflight_controller_never_reads_topicdiscovery` (source scan) | `restore_preflight::uses_fresh_listing` | S19 |
| Plan edits | `binding::plan_hash_change_invalidates` | API applicability per GET | — | S15a |
| New target conflict after preview | — | `no_execution_path_reads_preflight_or_discovery` (source scan, mutant: import `Preflight` in `restore.rs`) | existing phase-0 collision tests | S15b |
| Missing segment | `restore_preflight::missing_segment_detected_with_sample` | details `ConfigMap` written | fake store | S16 |
| Denied access | as above | as above | as above | S17 |
| Expired approval | `approval_check::expired_key_is_not_ready` | reads `Approval.status` and `TrustRoster` | — | S18 |

### Framework / security additions

- **Relay:** `relay::missing_end_line_is_result_unreadable`,
  `relay::wrong_plan_sha_is_refused`, `relay::part_digest_mismatch`.
- **Plan and chunks:** `plan::foreign_owner_409_is_conflict`,
  `chunks::immutable_false_is_conflict`.
- **Limits:** `limits::queues_over_namespace_cap`.
- **I13:** `retention::no_store_call_is_made_outside_spawn_blocking` amended with
  the single `StoreCache` site.
- **Gates:** `linkage::the_controller_never_reads_a_secret` unchanged and passing;
  `manifest_lint::every_granted_verb_has_a_caller` with `events` and the three
  kinds; `chart_lint` parity.
- **Existing suites that must stay green:** `crd_shape`, `plan_addressing`
  (legacy path), `scripts/check-no-oso.sh`, `scripts/check-no-archive-write.sh`,
  `scripts/check-one-signer.sh`, `scripts/check-pure-core.sh` (W1 adds no I/O,
  clock or entropy to `logweir-core`), `no_network_in_unit_tests` (allow-list only
  the dialling runner files).

---

## 13. Implementation plan: bounded worker tasks

### 13.1 Consumed seams from in-flight work (agreed fake boundaries)

**PLAT-07.1** (worktree `wt/plat07`, not merged). Names are provisional and adapt
to what lands.

```rust
// crates/weirkeeper/src/connection.rs (PLAT-07.1)
pub enum ConnectionUse { Probe, BackupSource, RestoreTarget, Discovery, Preflight }
pub struct ResolvedConnection {
    pub bootstrap_servers: Vec<String>, pub auth: logweir_core::spec::AuthSpec,
    pub password_env: Option<job::EnvFromSecret>,          // explicit Secret key
    pub ca: Option<(String /*configMap*/, String /*key*/)>, pub ca_pem: Option<Vec<u8>>,
    pub execution: ExecutionContext /* serviceAccountName, placement */,
    pub principal: String /* "User:<name>" | "User:ANONYMOUS" */,
    pub generation: i64, pub uid: String, pub bootstrap_sha256: String,
}
pub fn resolve(cluster: &KafkaCluster, use_: ConnectionUse) -> Result<ResolvedConnection, ConnectionRefusal>;
```

**PLAT-06.1** (worktree `wt/plat06`). `ResolvedBackupInputs` gains
`destination: Option<ResolvedDestinationSnapshot>` (§3.6); plan key
`archive-ca.pem` is present only with a CA. Either a v1 optional field or a `v2`
version: the PLAT-06.1 owner decides, and W10 adapts.

**PLAT-17.1** (worktree `wt/plat17-api`). Routes in
`crates/logweir-api/src/routes/`, DTO modules, cursor and idempotency helpers, and
capability flags.

### 13.2 Tasks

"Now" means the task can start on main `4956785` without editing files owned by
PLAT-06.1, PLAT-07.1 or PLAT-17.1. Class: **C** complex/security (strong model plus
Rust and security review), **B** bounded.

| ID | Class | Scope / tracker | Owned files | Depends on | Start |
|---|---|---|---|---|---|
| W1 | B | Pure contracts: check plan/result/codes/frames/redaction/visibility/binding digest; destination validation/location digest/storage URLs (08.1, 09.1, 03.x) | `crates/logweir-core/src/check_contract.rs` (new), `crates/logweir-core/src/destination.rs` (new), `crates/logweir-core/src/lib.rs` (2 lines), `crates/logweir-core/tests/{check_contract,destination}.rs` (new) | — | **Now** |
| W2 | C | Store options: explicit credentials (static, workload-identity-only, ambient), root certificates, explicit http/addressing overriding env, `AWS_METADATA_ENDPOINT` pin, error classification (08.1) | `crates/logweir-store/src/lib.rs` (additive `StoreOptions`, `read_only_with`, `from_url_with`, `StoreErrorClass`), `crates/logweir-store/tests/{options.rs, minio_options.rs (e2e-gated)}` | — | **Now** |
| W3 | B | Kafka inventory and validate-only probes: targeted describe, broker count, validate-only `CreateTopics`, error-callback classification (09.1, 03.2) | `crates/logweir-kafka/src/inventory.rs` (new), `crates/logweir-kafka/src/lib.rs` (1 line), `crates/logweir-kafka/tests/inventory.rs`; later a `pub(crate)` accessor edit in `rdkafka_reader.rs` | W1; accessor edit after PLAT-07.1 merges its TLS changes to `rdkafka_reader.rs` | **Now** (new files) |
| W4 | C | Runner `logweir check run` (all plan kinds; frames; redaction; handshake; marker probe) | `crates/logweir/src/check/**` (new), `crates/logweir/src/cli.rs` (Check block), `crates/logweir/src/main.rs` (dispatch), `crates/logweir/src/lib.rs`, `crates/logweir/tests/check_cli.rs` (new), `crates/logweir/tests/no_network_in_unit_tests.rs` (allow-list), `docs/stability.md` (check exit contract) | W1; W2/W3 through traits, stubbed until merged | **Now** (rebase `cli.rs` after PLAT-07.1) |
| W5 | C | Controller check framework (§4.3) plus policy loader | `crates/weirkeeper/src/check/**` (new), `crates/weirkeeper/src/lib.rs` (1 line), `crates/weirkeeper/tests/check_framework.rs` (new); last commit: additive `labels`/`template_labels` in `crates/weirkeeper/src/job.rs` | W1; `job.rs` edit after PLAT-07.1 merge | **Now** |
| W6a | B | New CRDs + Amendment F + kind-count gates | `crates/weirkeeper/src/crds/{backup_destination,topic_discovery,preflight}.rs` (new), `crates/weirkeeper/src/crds/mod.rs`, `config/crd/*` (new + kustomization), `charts/logweir/crds/*`, `charts/logweir/rendered/*`, `crates/weirkeeper/tests/crd_shape.rs`, `docs/architecture.md` (Amendment F), `docs/kubernetes.md` §7, `docs/install.md` CRD list | W1 (enum names) | After **PLAT-07.1** CRD regeneration merges (`crds/mod.rs` and rendered files conflict) |
| W6b | B | Refs + sentinel CEL on existing kinds | `crates/weirkeeper/src/crds/{backup,backup_schedule,restore}.rs`, regenerated CRDs/chart copies | W6a, **PLAT-06.1** merged (`crds/backup.rs`) | After PLAT-06.1 |
| W7 | C | Destination resolver + controller + `ControllerIdentity` store cache | `crates/weirkeeper/src/destination.rs` (new), `crates/weirkeeper/src/controllers/backup_destination.rs` (new), `crates/weirkeeper/src/evidence_store.rs` (new), `crates/weirkeeper/src/controllers/mod.rs` (module line, doctrine sentence), `crates/weirkeeper/src/main.rs` (one push line), `crates/weirkeeper/tests/destination_controller.rs` (new), `crates/weirkeeper/tests/retention.rs` (I13 site) | W1, W2, W5 (policy), W6a | After W6a |
| W8 | C | `TopicDiscovery` controller | `crates/weirkeeper/src/controllers/topic_discovery.rs` (new), `main.rs` (one line), `crates/weirkeeper/tests/topic_discovery_controller.rs` (new) | W5, W6a, W4 (topics), W3, **PLAT-07.1** resolver | After PLAT-07.1 + W6a |
| W9 | C | `Preflight` controller (Backup, DestinationAccess, Restore) + execution-path source scans | `crates/weirkeeper/src/controllers/preflight.rs` (new), `main.rs` (one line), `crates/weirkeeper/tests/preflight_controller.rs` (new) | W5, W6a, W7, W4 (readiness/restore), W3, PLAT-07.1; restore recovery-point identity per PLAT-11.1 contract (a Backup ref suffices) | After W7 |
| W10 | C | Execution integration: destination-backed Backup/Schedule/Restore, evidence-fetch flow, verification refactor, runner store/CA wiring, pod-owner lookup on legacy paths, probe phase 1 | `crates/weirkeeper/src/controllers/{backup,backup_schedule,restore,kafka_cluster}.rs`, `crates/weirkeeper/src/verification.rs`, `crates/weirkeeper/tests/{backup_controller,restore_controller,schedule_controller,verification,plan_addressing,kafka_cluster_controller}.rs`, `crates/logweir/src/backup/mod.rs`, `crates/logweir/src/drill/mod.rs`, `crates/logweir-engine-oso/src/subprocess.rs` | **PLAT-06.1 + PLAT-07.1 merged**, W2, W4 (evidence), W5, W6b, W7 | Last backend task |
| W11 | B | RBAC, chart, policy `ConfigMap`, docs | `config/rbac/{role,operator_role,viewer_role,approver_role}.yaml`, `logweir.yaml` (regen), `charts/logweir/templates/{clusterrole,human-roles,deployment}.yaml`, `charts/logweir/templates/policy.yaml` (new), `charts/logweir/templates/ui/ui.yaml` (read grants), `charts/logweir/values.yaml`, `values.schema.json`, `charts/logweir/README.md`, `crates/logweir/tests/{manifest_lint,chart_lint}.rs`, `docs/kubernetes.md` §15/§20–22, `docs/install.md` | W6a; grants land with or after the controllers that call them (W7–W9, verb/caller lint) | With W7–W9 |
| W12 | B | API routes/DTOs (§8) | `crates/logweir-api/src/routes/{destinations,topic_discoveries,preflights}.rs` (new), DTO/schema additions coordinated with the API owner, tests | PLAT-17.1 stages 1 and 3 merged; W1; W6a | After PLAT-17.1 skeleton |
| W13a | B | UI G5 fix only (path-style never enables HTTP; explicit insecure-HTTP control) | `ui/pages/restore-wizard.js` (lines ~1135–1149 and field render), `ui/tests/*` behaviour case | ui-correct worker (PLAT-13.2/12.x) merged | Right after ui-correct |
| W13 | B | UI pages (§9) | `ui/pages/destinations.js` (new), `ui/pages/{schedules,clusters,restore-wizard}.js`, `ui/app.js`, `ui/render.js`, `ui/tests/*`, `ui/README.md` | W12, PLAT-17.1 UI client stage, W13a | After W12 |
| W14a | B | Controller watch scope `LOGWEIR_WATCH_NAMESPACES` (comma list; empty = all namespaces, today's behaviour), so a test controller can run beside the shared lab controller without reconciling lab objects | `crates/weirkeeper/src/watch_scope.rs` (new), `crates/weirkeeper/src/main.rs`, the `controller()` constructors in `crates/weirkeeper/src/controllers/*.rs`, `crates/weirkeeper/tests/linkage.rs` | **Preferred owner is PLAT-17.2 stage 5** ("controller authority isolation"). Only if that has not landed: run after W10 merges, so the controller set is stable | Before W14 |
| W14 | C | Live docker-desktop acceptance (§14) + evidence | `e2e/k8s/d2/**` (new harness and fixtures), `/tmp/logweir-roadmap-run/claude/artifacts/d2/` | W7–W11 integrated on one branch; cluster lock | Last |

### 13.3 Sequencing

```
now:            W1 ─┬─> W3 ──────────────┐
                W2 ─┤                     │
                    ├─> W4 ───────────────┤
                    └─> W5 ───────────────┤
PLAT-07.1 merge ───> W6a ─> W7 ─> W9 ─────┤
                           └─> W8 ────────┤
PLAT-06.1 merge ───> W6b ─────────────────┼─> W10 ─> W11(final) ─> W14
PLAT-17.1 skeleton ─> W12 ─> W13 ─────────┘
ui-correct merge ──> W13a
PLAT-17.2 stage 5 (or W14a after W10) ───> prerequisite of W14
```

### 13.4 Conflict notes

- **`main.rs`.** W7, W8 and W9 each append one `controllers.push(...)` line, in
  that order, and `tests/linkage.rs:952` moves from 6 to 7, 8 and 9 in the same
  commits.
- **`job.rs` and `crds/mod.rs`** are the only shared-file hot spots with PLAT-07.1.
  Both edits are additive and scheduled after that merge.
- **PLAT-06.1 files are not edited before its merge:** `controllers/backup.rs`,
  `crds/backup.rs` and `backup_schedule.rs`.
- **`cli.rs`** may conflict with PLAT-07.1 probe TLS flags. W4 keeps its edit to a
  self-contained enum variant and rebases.

---

## 14. Live docker-desktop acceptance (W14)

### 14.1 Rules and lock

- **Context.** Every command passes `--context docker-desktop` (kubectl) or
  `--kube-context docker-desktop` (helm). The kubeconfig is never changed.
- **Namespaces.** `lw-d2-<utcstamp>` and `lw-d2-<utcstamp>-b`, both labelled
  `logweir.dev/test-owner=d2`.
- **Lock.** Acquire `/tmp/logweir-roadmap-run/claude/k8s-lock.sh acquire d2-live`
  before any of: CRD apply, `TrustRoster/default` replacement, or scaling the lab
  controller.
- **Record originals** to `artifacts/d2/originals/`:
  - `kubectl get deploy weirkeeper -n logweir-scram-local -o yaml`
  - `kubectl get trustroster default -o yaml`
  - `kubectl get crd backups.logweir.dev backupschedules.logweir.dev restores.logweir.dev -o yaml`
- **Why scale the lab controller to 0.** It watches all namespaces (G17) and would
  reconcile D2 `Backup`s and `Restore`s, including sentinel refusals.
- **The D2 controller.** Run it in `lw-d2-<ts>` with a watch scope limited to the two
  D2 namespaces (`LOGWEIR_WATCH_NAMESPACES`, from PLAT-17.2 stage 5 or W14a). If
  neither has landed, **stop** and record every scenario as unrun rather than
  reconciling lab objects with an unreleased controller.
- **Images.** The runner and controller are built from the integrated branch.
  Record image IDs (`docker image inspect … --format '{{.Id}}'`) and the pod
  `imageID`.

### 14.2 Fixtures (all in the owned namespace)

**`kafka-acl`** (apache/kafka 3.7.1 KRaft combined, 1 GiB):

- env as the lab broker, plus
  `KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer`,
  `KAFKA_SUPER_USERS=User:ANONYMOUS;User:admin`,
  `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`,
  `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`;
- SASL listener 9096 SCRAM-SHA-512;
- PLAINTEXT 9092 advertised `localhost:9092`, for in-pod admin tools only.

**SCRAM users and ACLs:**

- Users are created with
  `kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'SCRAM-SHA-512=[password=…]' --entity-type users --entity-name <u>`
  for `admin` and `limited`.
- `limited` gets one ACL:
  `kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:limited --operation Describe --operation Read --topic orders`.
- `rotating` (S12 only, so no other scenario is disturbed) gets
  `--add --allow-principal User:rotating --operation Describe --topic '*'`, with
  Secret `kafka-rotating` and `KafkaCluster/source-rotating`.

**Topics:**

- `orders` and `payments` (3 partitions each) and `audit`, seeded with 200 records
  each.
- `bulk-00000` … `bulk-04999`, 1 partition each, created by the host helper
  `e2e/k8s/d2/bulk-topics` (rdkafka AdminClient, 500 per batch) through
  `kubectl --context docker-desktop port-forward pod/kafka-acl 9092`.
- One consumer group commit (admin) creates `__consumer_offsets`.

**`kafka-empty`:** plaintext, no user topics.

**`minio-a`** (TLS, private CA):

- Generate the CA and server certificate with openssl on the host (SAN
  `minio-a.<ns>.svc`, `minio-a.<ns>.svc.cluster.local`).
- Secret `minio-a-tls` mounted as `/certs/{public.crt,private.key}`; `ConfigMap`
  `minio-a-ca` holds `ca.crt`.
- Bucket `lw-a`.
- Users and policies (§14.3): `a-writer`, `a-reader`, `a-evidence-ro`, `a-denied`.

**`minio-b`** (plain HTTP):

- Bucket `lw-b`.
- Users: `b-writer`, `b-evidence-ro`.
- Controller identity for `ControllerIdentity` tests: `b-controller-ro`
  (`s3:GetObject` and `s3:ListBucket` on `lw-b/*`, covering both `logweir/*` and the
  S6 legacy prefix), projected into the D2 controller env as
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, with the policy allowlist entry for
  `minio-b` / `lw-b`. It is the same principal the legacy global handle uses in S6,
  which is deliberate: S6 proves the legacy path and the per-destination path do not
  contaminate each other's addressing.

**Cluster-scoped and namespace objects:**

- `TrustRoster/default` replaced, under lock, with the **union** of the original
  entries, a D2 test approver key and a D2 signing key.
- Namespace Secret `logweir-signing-key` with the D2 signing key.
- `logweir-runner` ServiceAccount.

**D2 resources:**

- `KafkaCluster/source-admin`, `source-limited`, `source-rotating`, `target`
  (points at `kafka-acl`, admin) and `empty`.
- `BackupDestination/dest-a`: TLS, `caBundle` `minio-a-ca`, `PathStyle`;
  `archiveWrite` a-writer, `archiveRead` a-reader, `evidenceRead` a-evidence-ro.
- `BackupDestination/dest-b`: `InsecureHTTP` `http://minio-b.<ns>.svc:9000`,
  `PathStyle`; `archiveWrite`/`evidenceWrite` b-writer, `evidenceRead`
  `ControllerIdentity`.
- `BackupDestination/dest-denied`: as dest-a with a-denied for every grant.

### 14.3 MinIO policies

Starting points; S1 records the measured minimum (U6):

```json
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::lw-a"],
  "Condition":{"StringLike":{"s3:prefix":["team/prod/*","logweir/*"]}}},
 {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:AbortMultipartUpload"],
  "Resource":["arn:aws:s3:::lw-a/team/prod/*","arn:aws:s3:::lw-a/logweir/*"]}]}
```

- `a-reader`: ListBucket (prefix `team/prod/*`) plus GetObject on
  `lw-a/team/prod/*`.
- `a-evidence-ro`: GetObject on `lw-a/logweir/*`, plus ListBucket on prefix
  `logweir/*`.
- `a-denied`: no policy.

### 14.4 Scenarios and exact pass criteria

Jsonpath shorthand below omits `kubectl --context docker-desktop -n $NS get`.

| # | Scenario | Pass criteria (all must hold) |
|---|---|---|
| S1 | Two destinations, manual Backup `bk-a` (dest-a, source-admin, topic `orders`) and `bk-b` (dest-b) | Both `backup/<n> -o jsonpath={.status.phase}` = `Succeeded`, `{.status.exitCode}` = `0`, `{.status.evidence.verification.result}` = `Valid`. `mc ls --recursive a/lw-a/team/prod/` lists `<backupIdA>/manifest.json`; `mc ls b/lw-b/` has no `<backupIdA>`; the converse holds for B. `job/bk-a -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="AWS_ALLOW_HTTP")].value}'` = `false`, and for `bk-b` = `true`. Neither Job has `AWS_ENDPOINT_URL`. `bk-a`'s `AWS_ACCESS_KEY_ID.valueFrom.secretKeyRef.name` = `a-writer`. Evidence Job `lwc-ev-*` for `bk-a` has `ownerReferences[0].uid` = `bk-a` UID; `bk-b` has no evidence Job (`{.status.evidence.observation.mode}` = `ControllerIdentity`). `kubectl auth can-i get secrets --as=system:serviceaccount:$NS:weirkeeper -n $NS` prints `no`. `grep -E 'AKIA\|<a-writer secret>\|<b-writer secret>'` over controller logs, both Backup YAMLs and all `lwc-*` `ConfigMap`s: 0 matches. Measured minimal MinIO actions recorded. |
| S2 | Transport and addressing validation | Apply dest-bad1 (`TLS` + `http://` endpoint): exit ≠ 0 and stderr contains `transport.security must match the endpoint scheme`. dest-bad2 (`InsecureHTTP` + `https://`): same message. dest-bad3 (`PathStyle`, no endpoint, `InsecureHTTP`): same message. dest-vh (`VirtualHosted` + endpoint): applied, `{.status.conditions[?(@.type=="Valid")].reason}` = `AddressingUnsupportedByEngine`. `kubectl patch backupdestination dest-a --type merge -p '{"spec":{"transport":{"security":"InsecureHTTP"}}}'`: rejected with `transport can never be changed in place`. |
| S2b | Private-CA HTTPS through the engine (U1) | Run with policy `engine.allowUnverifiedCustomCa: true`. `bk-a` Succeeded (S1) then proves the engine trusted `minio-a` through `SSL_CERT_FILE`; record the engine digest and flip `ENGINE_CUSTOM_CA_VERIFIED`. If it fails with a TLS trust error, U1 is recorded failed, the constant stays false, the policy key returns to false, and S1 is rerun against dest-b (explicit HTTP) so the rest of the matrix still executes. |
| S3 | Denied location | `Preflight` `DestinationAccess` on dest-denied (roles `ArchiveRead`): `{.status.result.state}` = `notReady`; check `destination.archiveListable` code `AccessDenied`; message contains no secret. `Backup` on dest-denied: `exitCode` = 1 (execution authoritative). |
| S4 | Namespace separation | In `$NS-b`, a `Backup` with `destinationRef: dest-a`: `{.status.reason}` = `DestinationNotFound` within 60 s; `kubectl get jobs,configmaps -n $NS-b -l app.kubernetes.io/managed-by=weirkeeper` empty; after `min(deadlineSeconds, 600)` s, `{.status.phase}` = `Failed`. |
| S5 | Evidence/archive separation | Restore of `bk-a` into `target` (newTopic) with `sourceDestinationRef: dest-a`, `evidenceDestinationRef: dest-b`, approved with the D2 approver key: `{.status.exitCode}` = `0`, `{.status.evidence.verification.result}` = `Valid`; `mc ls b/lw-b/logweir/drills/` contains `<run>.json`; `mc ls a/lw-a/logweir/drills/` does not. Job env: `AWS_ACCESS_KEY_ID` from `a-reader`, `LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID` from `b-writer`. `mc cp` as `a-reader` to `lw-a/team/prod/x` fails `Access Denied`. |
| S6 | Legacy compatibility and no leakage | A legacy inline Backup (`archive.url: s3://lw-b/legacy`, `secretRef: b-writer-legacy`) with controller env `LOGWEIR_ARCHIVE_URL=s3://lw-b/legacy`, `AWS_ENDPOINT_URL=http://minio-b…`, `AWS_ALLOW_HTTP=true`: Succeeded and Valid via the global handle. `bk-a2` (dest-a) created while that env is set: Job env `AWS_ALLOW_HTTP=false`, no `AWS_ENDPOINT_URL`. |
| S7 | Large catalog and paging | `TopicDiscovery td-full` (source-admin, `maxTopics` 20000): `{.status.phase}` = `Succeeded` within 180 s; `{.status.result.counts.returned}` = 5003 (5,000 bulk + orders, payments, audit); `{.status.result.chunks[*].name}` has 3 names; each chunk `-o jsonpath={.immutable}` = `true`, owner UID = td UID; `sha256sum` of `data["topics.tsv"]` equals the annotation and `status.result.chunks[i].sha256`. If the console API is available: 26 pages at `limit=200` cover exactly the names from `kafka-topics.sh --list` minus `__*` (set equality, no duplicates), and `q=bulk-0049` returns 10 items. Otherwise the TSV union equals that set. |
| S8 | Internal topics | `td-full`: `{.status.result.counts.internalExcluded}` ≥ 1 and `__consumer_offsets` absent from all chunks. `td-internal` (`includeInternal: true`) contains it with flag `internal`. |
| S9 | ACL-limited principal | `td-limited` (source-limited, `expectedTopics [orders, payments]`): chunks contain exactly `orders`; `{.status.result.visibility.state}` = `limited`; basis contains `expectedTopicNotAuthorized`; `{.status.result.expected.notAuthorized}` = `1`. `td-limited-noexpect`: state `unknown`, basis `[listingOnly]`. With a policy attestation for (`$NS`, `source-admin`, observed clusterId, `User:admin`, expires +10 m), `td-full2` state is `attestedComplete`. After the attestation expires, `td-full3` is `unknown` with basis containing `attestationExpired`. |
| S10 | Empty cluster | `td-empty`: `Succeeded`, `counts.returned` = 0, visibility `unknown`, zero chunks. |
| S11 | Timeout | `KafkaCluster/blackhole` (bootstrap `10.255.255.1:9096`), `td-timeout` `timeoutSeconds: 10`: `{.status.phase}` = `Failed` with reason ∈ {`BrokerUnreachable`, `MetadataTimeout`} within 150 s; no chunk `ConfigMap`s; the check Job is `Complete` — the runner exits 0 after relaying its `notReady` result and the failure is carried in the projected reason (amended 2026-09-18 at D2 W14 / lab-refresh-3: the earlier "Job `Failed`" criterion described the pre-W9 runner). |
| S12 | Refresh and credential rotation | Uses the dedicated `rotating` principal, so no other scenario is affected. Change its SCRAM password on the broker only: `td-rot1` on `source-rotating` is `Failed/AuthenticationFailed`; its previous success stays readable with API `stale=true` after `freshUntil`. Update Secret `kafka-rotating` `password`: `td-rot2` `Succeeded`. GC: after 6 terminal discoveries for `source-rotating`, at most 5 remain (`kubectl get topicdiscoveries`) and the removed one's chunks are gone. |
| S13 | Cancellation | `td-cancel` on blackhole with `timeoutSeconds: 300`; after it reaches `Running`, `kubectl patch topicdiscovery td-cancel --type merge -p '{"spec":{"cancelRequested":true}}'`: `{.status.phase}` = `Cancelled` within 30 s; the Job condition `Failed` has reason `DeadlineExceeded` (or the job is `suspended` under the U3 fallback); no pod remains after 60 s. A patch back to `false` is rejected by CEL. |
| S14 | Readiness failures | (a) `Preflight` Backup on a `KafkaCluster` whose Secret `missing-secret` is absent: check `connection.credentialProjected` = `notReady`, code `CredentialSecretNotFound`, message contains `missing-secret`, result within 90 s (early cancel). (b) Missing key: `CredentialSecretKeyMissing`. (c) Wrong password: `connection.authenticated` `AuthenticationFailed`. (d) dest-denied: `destination.archiveListable` `AccessDenied`. (e) Controller `LOGWEIR_RUNNER_IMAGE=logweir:d2-missing` with `Never`: `runner.image` `RunnerImageNotPresent`. (f) Blackhole: `MetadataTimeout`/`BrokerUnreachable`, dependents `BlockedByPrerequisite`. Redaction: `kubectl get preflights -o yaml` plus controller logs contain none of the fixture secret strings. |
| S15 | Restore binding and race | (a) Draft `Preflight` for plan P1 → `ready`. Recompute the plan with a changed topic prefix (P2): API `applicable=false`, `staleReasons` contains `planHashChanged` (without the API: the recomputed `inputs_digest`/`planHash` via `logweir-core` test binary differs, recorded). (b) As admin create topic `<P1 prefix>orders` on `kafka-acl`; submit `Restore` P1 with a verified Approval: `{.status.exitCode}` = `3`, `{.status.exitReason}` = `GuardRefused`, pod log contains `already exists on cluster`. Re-run the preflight: `target.mappedTopics` `notReady` `MappedTopicExists`, detail sample contains that name. |
| S16 | Missing segment | As MinIO admin remove one segment object of `bk-a`; restore `Preflight`: `archive.segments` `notReady`, `detail.count` = 1, `sample[0]` = removed key. |
| S17 | Denied access (restore) | Restore preflight with a source destination using a-denied for `archiveRead`: `archive.backupSet` `notReady` `AccessDenied`. |
| S18 | Expired approval | Roster approver key with `notAfter` 2 minutes ahead; after expiry an Approval for `Restore` R shows `Verified=False` `KeyIdExpired`; `Preflight` with `restoreRef` R: `approval.state` `notReady` `ApprovalExpired`. |
| S19 | Stale inventory | `td-full` lists `audit`; as admin delete `audit`; Backup `Preflight` with topics `[audit]`: `connection.topicsDescribable` `notReady` `TopicNotFound`, while `td-full` chunks still contain `audit` (preflight did not use the inventory). |
| S20 | Pod-owner spoof (optional, G10) | Create a bare pod labelled `batch.kubernetes.io/job-name=<a running lwc-td job>` printing forged frames; the discovery result equals the genuine run's and the controller log contains `ForeignPodIgnored`. |
| S21 | Destination edit during draft | Draft restore `Preflight` `ready`; `kubectl patch backupdestination dest-a` `access.archiveRead.secret.name` → `a-reader2`: generation increments; API `staleReasons` contains `referentChanged:BackupDestination/dest-a`; plan bytes and hash unchanged, so the existing Approval still `Verified`. |
| S22 | Browser (only when console and UI are available) | Playwright: restore wizard path-style toggle leaves the rendered plan's `allow_http: false`; destination create with a new credential shows no credential in DOM or network responses; discovery paging and search work. Otherwise recorded as **unrun**. |

### 14.5 Cleanup proof (in `artifacts/d2/cleanup.md`)

1. Delete the D2 namespaces after verifying the label and UID. Wait for `NotFound`.
2. Delete the three new CRDs only after
   `kubectl get backupdestinations,topicdiscoveries,preflights -A` is empty.
3. Re-apply the original `backups`/`backupschedules`/`restores` CRDs from
   `originals/`.
4. Restore `TrustRoster/default` from `originals/` (delete, then create), and verify
   `{.status.loaded}` = `true` and the key IDs equal the original.
5. Scale the lab controller back and verify `Ready`, with the image and env equal to
   the original.
6. Release the lock.

---

## 15. Unverified assumptions, risks and open questions

| Id | Item | Resolution |
|---|---|---|
| U1 | The engine subprocess trusts a private CA through `SSL_CERT_FILE` (rustls-platform-verifier / native-certs on Linux) | S2b. On failure keep admission refusal `CaBundleUnsupportedByEngine`. |
| U2 | KRaft 3.7 validate-only `CreateTopics` returns `TOPIC_ALREADY_EXISTS` for existing topics and authorizes before existence | S15 |
| U3 | Patching a running Job's `activeDeadlineSeconds` terminates it as `DeadlineExceeded` | S13; fallback `suspend: true` |
| U4 | Framed stdout ≤ 4 KiB lines survive CRI logging intact within the default `containerLogMaxSize` 10 MiB and an 8 MiB `limitBytes` read | S7 |
| U5 | librdkafka SASL/TLS failures are classifiable through the error callback rather than timeouts | S14c |
| U6 | Minimal S3 actions for engine writes | S1 measurement |
| U7 | MinIO/S3 authorize before `If-None-Match` evaluation (412 means authorized) | S14d with marker probe |
| U8 | StandardAuthorizer filters unauthorized topics from all-topics metadata | S9 |
| U9 | CEL `matches`/`startsWith` rules behave identically at the 1.29 floor | docker-desktop is 1.34, so 1.29 is unproven locally. Rules deliberately avoid newer libraries. Record as a floor risk. |
| U10 | EKS IRSA/Pod Identity injection with `automountServiceAccountToken: false` | Production-only; docker-desktop cannot prove it. Document as unverified. |

**Risks:**

- **Relay trust.** Evidence and inventory bytes come from a pod in a tenant
  namespace. Crypto stays in the controller, and forgery of a `Valid` verdict is
  bounded by residual O1 plus binding checks. Inventory relays have no signature:
  a tenant with pod-exec in their own namespace can falsify their own inventory,
  which is accepted because it affects only that tenant's advisory view.
- **etcd and pod pressure.** Bounded by limits, keep-last, TTL and chunk caps
  (§5.5). Large installations should lower `hardMaxTopics`.
- **Events RBAC.** `list events` exposes event messages cluster-wide to the
  controller. They carry no Secret values, and the grant is used only with an
  `involvedObject.uid` field selector. Security review must accept it; otherwise
  classification degrades to `PodNotStarted` for mount and create failures.
- **API `get configmaps`** (§7.3): fallback kind recorded.
- **Engine addressing** (G4) constrains destinations until an engine upgrade. The
  controller check is keyed to the pinned version so it can be relaxed without a
  CRD change.
- **Operational cost of evidence-fetch Jobs.** Mitigated by an opt-in
  `ControllerIdentity` for allowlisted locations.

**Out of scope here** (seams recorded):

- PLAT-09.2 per-run dynamic selection (§5.11).
- PLAT-15 catalog: `locationDigest` is the partition key, and archive-inventory is a
  future check kind on this framework.
- PLAT-16 retention: destination-backed schedules report nothing until an
  inventory check exists; no delete authority is added anywhere.
- GCS/Azure destination providers (PROD-09).
- NetworkPolicy port automation.
- Node-role (IMDS) credentials for destinations.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.

## Amendment at integration (2026-09-21, PLAT-08.1)

§15 U6 and U7 are resolved by measurement, not by the recorded MinIO policies: the
per-role minimal object-storage permission table lives in `docs/kubernetes.md` §7a, each
row citing the `e2e/k8s/d2` bisection row that measured it on the worker's own MinIO
(seven roles, thirty-five rows; the two `s3:ListBucket` prefix legs are separate units,
and `archiveWrite` lists only under `<prefix>/*` while the catalogSync reader lists only
under `logweir/*`). Where §3.11 or §14.3 name a grant a role carries, the measured table
is authoritative — no role is granted `s3:DeleteObject` except the retention enforcer's
delete principal. The retention enforcement Job's evidence credential is `evidenceWrite`
(D3 §6.5); the controller projecting `archiveRead` there is the defect
RET-EVIDENCE-GRANT-IS-ARCHIVEREAD, not a change to this contract.

## Amendment at integration (2026-09-23, PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL, DESTINATIONACCESS-IGNORES-WRITEPROBE, RECEIPT-DUP)

Landed as `claude/readiness-principal` (Tier-A review ACCEPT-with-LOWs, LOW round done) and `claude/receipt-dup` (probe half), merged on main together.

1. **§6.3: each destination row is answered by the principal its role names.** A check plan's destination credential is the grant the check resolved:
   - for `Backup`: `archiveWrite`. A Backup run lists with it, so `destination.archiveListable` is its answer. This amends §4.2's input list, which said `archiveRead`.
   - for `DestinationAccess`: `archiveRead` whenever requested, else the first of `archiveWrite`, `evidenceWrite`, `evidenceRead`.

   When the `evidenceWrite` or `evidenceRead` grant differs from it, the plan names that grant by reference: `evidenceWrite` / `evidenceRead: {credentials: static|workloadIdentity|controllerIdentity|notConfigured, secretName|serviceAccountName}`, never a value. The pod is projected the grant: `LOGWEIR_EVIDENCE_AWS_*` for `evidenceWrite`, `LOGWEIR_EVIDENCE_READ_AWS_*` for `evidenceRead`, or the grant's ServiceAccount. The runner never falls back to other keys.

   Each probed row carries the fact `grant` = `destination` | `evidenceWrite` | `evidenceRead`.
   - **A grant no check pod holds** (`controllerIdentity`, `notConfigured`) answers `destination.evidenceReadable` `unknown`/`EvidenceReadNotConfigured`, and nothing is read.
   - **A grant the resolver refuses** is answered by the controller as that one advisory row `unknown`, with the refusal in its message; `destination.resolved` stays valid and the other rows run.
   - **Two workload identities on different ServiceAccounts** give `destination.resolved` → `ExecutionContextConflict` and no Job for a `DestinationAccess` check. For a `Backup` check's advisory `EvidenceRead`, the controller answers the row `unknown`, naming both ServiceAccounts, and the verdict is unchanged.

   The marker probe asks the evidence-write principal for create-only PUTs of the one marker key. The read probe asks the evidence-read principal for one GET of an absent key. Nothing else.
2. **§6.3: `DestinationAccess` reads `spec.readiness.writeProbe` exactly as `Backup` does.** A requested `EvidenceWrite` is probed and blocking under `CreateOnlyMarker`, and is execution-only `WriteNotProbed` under `Disabled`.
   - **A requested `ArchiveWrite` is never probed** (accepted limit). `destination.archivePrefixWritable` stays execution-only `ArchivePrefixWriteVerifiedOnlyAtExecution`, and its message names the archive prefix and says it was not write-probed: the one key a check may write lies under the evidence root and proves nothing about the archive prefix.
3. **§6.3/§4.2: the marker probe proves conditional create** (RECEIPT-DUP). A backup run claims its execution with a create-only put, which is a lock only on a store that enforces `If-None-Match: *`. After a fresh marker, the probe creates the same key again, as the same principal, and requires `AlreadyExists`. A second create that succeeds, or a store that reports conditional put unsupported (HEAD-then-PUT fallback), makes the row `notReady`/`ConditionalCreateUnsupported`. `AlreadyExists` on the first create stays proof. An error on the second create is classified, never proof.
4. **Mixed version.** A runner older than this contract refuses a plan carrying `evidenceWrite`/`evidenceRead` (exit 3, `CheckContractMismatch`), and the Preflight says to upgrade the runner image, naming the refused field. Roll the controller and the runner together.
5. **Residual.** `evidenceReadable` for a `Backup` check whose evidence-read identity is a ServiceAccount other than the check pod's is advisory `unknown`, not probed. An evidence-read Job per principal would close it.

## Amendment (2026-09-24, legacy-point-restore: PoC defects P3 and P5)

Found by the PoC upgrade round: a `v0.1.5` recovery point (no `BackupDestination`) could not pass the restore readiness check, and its restore finished with no verification and no completion.

1. **§6.2 / §6.8: `legacySourceArchive` is checked, as the restore Job will read it.** The check plan's source is the approved plan's `source.storage` (region and endpoint the plan omits come from the policy's `legacyArchiveAddressing`, §4.4 — the values the controller forwards to legacy runner Jobs; `path_style` and `allow_http` are the plan's), with a `SecretKeys` grant over `legacySourceArchive.secretRef` and the keys a legacy restore Job projects (`access-key-id`, `secret-access-key`). Transport follows §3.12's mapping (`InsecureHTTP` only for `allow_http` with an explicit `http://` endpoint; a custom endpoint is path-style). Fail closed: a non-`s3://` URL, no `secretRef`, an unparseable or non-S3 plan, or a location R3–R6 refuse is `destination.resolved notReady` and no Job. `plan.bindings` compares scheme, bucket and prefix of the plan, the request URL and the point's `spec.archive.url`; `recoveryPoint.state` does not compare a legacy point with a location derived from its own plan. Relayed rows are scoped `InlineArchive/<url>`. A **Backup** check's `legacyArchive` stays `Failed/ArchiveUrlUnreadable` (it would need the archive-write principal). *Option not taken:* an advisory `unknown` archive row that does not block Create — rejected because a real read with the restore's own principal was available and cheaper than a new gating exception in the console.
2. **§3.9 / §3.10: the global handle reads a legacy run's evidence only in its own bucket.** A legacy `Restore`'s scorecard is wherever its plan's `evidence:` block says (written with the archive credential, G6); a legacy `Backup`'s receipt is under its archive's bucket (G7). The controller compares backend and bucket with `LOGWEIR_ARCHIVE_URL`'s (`destination::legacy_evidence_scope`) and, when they differ, publishes `NotAttempted` naming both instead of reading the wrong bucket. A run whose runner printed both scorecard keys and whose controller read produced no digest publishes `NotAttempted` naming the key (it used to publish nothing). The console renders a legacy point's evidence in the archive's own bucket, and the restore check adds an advisory controller row `destination.evidenceReadable` (`EvidenceReadNotConfigured`) when the plan's evidence bucket is not the handle's; absent, never `ready`, when it is.
3. **Unchanged:** option A's scope (legacy objects only), no controller-held Secret, no new referent kind in the binding (the URL and Secret name are in the immutable request, the location in the plan bytes, the addressing in the policy digest).
4. **Review round (same day).** A `restoreRef` check takes the archive and Secret from the `Restore`'s own `spec.sourceArchive`; a request naming another is `PlanDestinationMismatch`, no Job. The console binds a legacy verdict to the Secret the check read with (it is not in the plan bytes). Tenant-visible text names the archive handle by role; its URL is logged by the controller only. The unread-scorecard `NotAttempted` is limited to the global handle (inline-archive runs), amending for that source only the decision recorded at `rehearsal_schedule::UNRECORDED_VERDICT_GRACE_SECONDS`.
