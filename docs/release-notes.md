# Release notes

One entry per release candidate, newest first. Each entry says what an operator
must know before running it: the **supported behaviour** that changed, the
**operator actions** an upgrade requires, the **verification scope** behind
each claim, who holds **retention authority**, and **migration and rollback**.
[The release checklist](tag1-checklist.md) row 9 points here, and its *What to
record* list is filled in this file's candidate record, not in a pull request.

A claim with no evidence behind it yet carries an `[UNVERIFIED]` mark and the
sentence that would close it (`scripts/check-unverified-labels.sh` refuses a
mark without one). The supported path these notes assume is
[quickstart.md, *The supported path*](quickstart.md); the measured limits are in
[stability.md, *Measured scale limits*](stability.md#measured-scale-limits-plat-202).

---

## Unreleased — `main` after `v0.2.0-rc.1`

The last tag is `v0.2.0-rc.1` (candidate `56a60ebe`, publication `2c277dc1`);
its record is in the next entry, whose twenty-seven items are what that
candidate shipped. This entry collects what lands on `main` after that
publication: items 28 (PROD-00.3f, the engine pin), 29 (PROD-16.1, no approver
key by default), 30 (PROD-08.1), 31 (FX-17, scheduled points in the catalog),
32 (PROD-05.1), 33 (PROD-01.3, client authentication modes and the credential
binding), 34 (FX-16, a point-bound restore restores its point's set), 35
(PROD-00.2, the engine built from the vendored source), 36 (FX-23, an
early-stopped restore is never signed `pass`), 37 (PROD-04.0b, the
one crate that may hold `unsafe` code, and the consumer-group and ACL reads
behind it), 38 (FX-20, the binding for every other credential reference),
39 (FX-18, a topic phase 0 creates is used only once the cluster serves
it), 40 (FX-24, a silent connection meets the console's header deadline),
41 (FX-21, a replication factor the archive does not record is never
read as matching; the engine's first patch), 42 (PROD-11.1, a restore
can select a window start) and 43 (PROD-08.1a, complete coverage requested
and shown through the CRDs, the API and the console), 44 (FX-24b, a
client that stops reading or sending meets a stall deadline), 45 (FX-29, a
controller no longer rewrites a status whose content has not changed), 46
(PROD-03.0, schema-dependent topics flagged from the archived bytes), 47
(FX-28, a sign-in whose identity provider stalls is answered at the provider
deadline), 48 (FX-20c, a destination's Test access compares every grant's
binding), 49 (PROD-01.4a, each topic's ID in the receipt and the catalog
point), 50 (PROD-04.1, consumer position evidence for selected groups), 51
(PROD-11.1b, a restore can select a partition subset, signed as scorecard
format 2.0.0 and named on every surface), 52 (FX-31, every object-store
read has a size cap), 53 (FX-24c, one peer's share of the console's
connections, and a rate floor on request bodies), 54 (FX-19, a probe Job
Kubernetes is collecting no longer clears `reachable`) and 55 (FX-13a and
FX-32, a sign-in state is single-use on each replica, and a refused callback
really clears the login cookie) so far. Items continue the next entry's
numbering. No candidate is cut from this entry yet, so it carries no candidate
record; when one is, its record follows [the release checklist](tag1-checklist.md)
as the next entry's does.

### The operator-facing changes after `v0.2.0-rc.1`

Each item names what changed, what to do, what the claim rests on (its
verification scope), and how to roll it back. Item 28 is PROD-00.3f, the engine
pin, proven on a compose stack; the PoC upgrade that carries it runs its
controller and runner rows. Item 29 is PROD-16.1 (owner decision OD-8), proven
by unit, mock-cluster and chart rows and a host console journey; its controller
and hook rows run at the PoC refresh that carries it. Item 30 is row PROD-08.1, proven
on the compose stack; it changes the runner's signed scorecard and adds a plan
value no controller renders yet, so the PoC upgrade that carries it runs the
sampled rows unchanged. Item 31 is fix-now row FX-17, proven on the compose
stack with the console on the host; it changes the runner (the catalog sync)
and the console's text, and the PoC upgrade that carries it re-syncs the PoC's
catalog and checks its first page and the nightly schedule's page.
Item 32 is row PROD-05.1, proven on the compose stack on the 3.9 and 4.3 broker
lines; it changes the runner's signed receipt and catalog record, the catalog's
view (runner and controller), the product API and the console, so the PoC
upgrade that carries it runs its catalog and console rows.
Item 33 is PROD-01.3 and its security follow-up, proven on a compose stack
(both clients, every mode); it changes the controller, the runner, the
console and the `KafkaCluster` CRD, and the PoC upgrade that carries it runs
the live `KafkaCluster` rows.
Item 34 is fix-now row FX-16, proven by unit, phase-sequence and real-binary
rows and on the compose stack; it changes the runner only, and the PoC upgrade
that carries it runs a console catalog-point restore (the binding's reads under
the store contract).
Item 35 is PROD-00.2 (owner decision OD-3), proven on a compose stack and by
image checks on both platforms; the PoC refresh that carries it runs the
runner's signed engine identity, and the first `main` publication after it
runs the keyless signing.
Item 36 is fix-now row FX-23, proven on a compose stack by a SIGTERM to the
engine mid-restore; it changes the runner's sampled verification and its
signed scorecard, and the PoC upgrade that carries it runs its sampled
rehearsal and restore rows unchanged.
Item 38 is fix-now row FX-20, proven by unit, controller, API, console and
real-binary rows and on the compose stack (a foreign archive binding writes no
object to MinIO; a bound one backs up); it changes the controller, every
runner, the retention worker, the product API, the console and three CRDs'
status, and the PoC upgrade that carries it binds the `primary` destination's
Secrets and runs the refusal rows against a sentinel.
Item 39 is fix-now row FX-18, proven by unit and phase rows, a guard over
every e2e helper that creates a topic, and the compose e2e suite (the
`NotLeaderForPartition` race reproduced with the wait removed, 3 of 13 runs,
and gone with it); it changes the runner's phase 0 only, and the PoC upgrade
that carries it runs its restore rows unchanged.
Item 40 is fix-now row FX-24, proven by rows on the built console binary and
live on the host in localAdmin mode; it changes the console only, and the PoC
upgrade that carries it repeats the silent-socket probe against the shared-mode
console.
Item 41 is fix-now row FX-21, proven by unit, phase and reader rows and on
the compose stack (`cluster3`) with three engines; it changes the runner's
phase 7 and the engine (patch 0002, build `0.23.3+logweir.2`), so the PoC
refresh that carries it runs a multi-topic backup and restore and reads the
new runner's engine identity.
Item 44 is fix-now row FX-24b, proven the same way; it changes the console
only, and the PoC upgrade that carries it repeats the slow-reader probe and
the event-stream row against the shared-mode console.
Item 45 is fix-now row FX-29, proven by controller rows over a fake API that
applies each status patch as the API server does and counts the writes; it
changes the controller only, and the PoC upgrade that carries it resumes the
two suspended schedules and watches their `resourceVersion`.
Item 46 is row PROD-03.0, proven by unit, reader, corpus and memory rows and
on the compose stack (`registry`: Karapace and its REST proxy, stopped before
the backup); it changes the runner's receipts and records, the catalog's view
(runner and controller), the product API and the console, so the PoC upgrade
that carries it runs a backup, reads its receipt's `schema_dependency` and the
catalog's `topics[].schemaDependency`, and opens the restore review.
Item 47 is fix-now row FX-28, proven by rows over a loopback identity
provider that stalls and on the built console binary; it changes the console
only, and the PoC upgrade that carries it signs in through Dex (a stall cannot
be simulated on the live Dex).
Item 48 is fix-now row FX-20c, proven by contract, runner, controller, API and
console rows over one shared fixture; it changes the controller and the
runner (the check plan, a new check row, and a `RetentionPolicy`'s `Enforced`
after a binding refusal), and the PoC upgrade that carries
it re-creates PoC batch 4's F6 thief destination, tests it, and deletes it.
Item 49 is PROD-01.4a, proven on a compose stack on the 3.7.1, 3.9.2 and 4.3.1
broker lines against the brokers' own tools; it changes the runner's signed
receipt and catalog record only, and the PoC upgrade that carries it runs one
scheduled backup and checks its receipt's `generations` against the source's
topic IDs.
Item 50 is row PROD-04.1, proven by unit, seam, corpus and parity rows and on
the compose stack (Kafka 4.3.1 with the `acl` and `streams-protocol` profiles,
and the default 3.7.1 line); it changes the runner's signed receipt and
catalog record, the catalog's view (runner and controller), the product API
and the `Backup` and `BackupSchedule` CRDs, and the PoC upgrade that carries it
runs a schedule selecting a group and reads its point.
Item 51 is row PROD-11.1b (the owner's decision OD-9 (a)), proven by unit,
phase, preview and reader rows, the parity script and the invariant corpus,
and on the compose stack (subset restores, a faulty engine, older runners and
older readers); it changes the runner (the plan grammar, phase 7's evidence
and the signed scorecard, whose first MAJOR it introduces), the restore
preview, both verifiers, the controller's `Restore` status, the product API,
the console and the runner's notification, and the PoC upgrade that carries
it runs a subset `Restore`, reads its scorecard with both readers, and reads
the selection on its status, the API, the console and the notification.
Item 52 is fix-now row FX-31, proven by store, controller and check rows, by a
child-process peak-RSS measurement, and on the compose stack against MinIO; it
changes the controller, the runner and the check Jobs, and the PoC upgrade that
carries it plants an oversized object at a receipt key in a scratch namespace's
bucket and reads the controller's verdict and memory.
Item 53 is fix-now row FX-24c, proven by rows on the built console binary in
both modes, by chart rows, and live on the host; it changes the console and
the chart (a shared console behind the chart's Ingress must name its trusted
proxy), and the PoC upgrade that carries it runs the per-peer probe against
one shared-mode console pod and a burst through the ingress (the PoC profile
already names Traefik's Service).
Item 54 is fix-now row FX-19, proven by controller rows over a fake API with
the controller's own log captured; it changes the controller only, and the PoC
upgrade that carries it watches every connection's `reachable` and the
controller's WARN lines across fifteen minutes of probe cycles.
Item 55 is fix-now rows FX-13a and FX-32, proven by router rows over one and
two console processes; it changes the console only, and the PoC upgrade that
carries it signs in through Dex and replays the callback URL against both
replicas.

#### 28. The engine is `kafka-backup` 0.23.3; an `http://` archive endpoint needs `allow_http: true` (PROD-00.3f)

**Changed.** The runner image carries `kafka-backup` **0.23.3** (image digest
`sha256:cc7d5a8a…`, upstream commit `afb160e7`), OSO's newest release on
2026-10-07, in place of 0.21.0. The segment format, the three engine commands
Logweir runs and every key it renders are unchanged, and archives written by
either engine read and restore with the other. `logweir doctor` accepts exactly
0.23.3, as a whole token: 0.21.0 is now a version mismatch, and so is a suffixed
`0.23.3+build`. Receipts and scorecards name the engine that ran, so new ones
say 0.23.3. Two engine behaviours since 0.22.0 are refused instead of
inherited. A storage location with a plain `http://` endpoint and
`allow_http: false` is refused at phase 0 with exit 3 (`refusal-reason=GuardRefused`),
by `drill run`, `restore run` and `backup run` alike, and no engine document
is rendered with it: the engine now derives plaintext from the scheme and
would dial the archive in the clear. `VirtualHosted` addressing with a custom
endpoint stays refused (`AddressingUnsupportedByEngine` /
`addressing_unsupported_by_engine`); its message no longer names an engine
version. The full-drill floor stays 0.21.0.
**Do:** nothing with the chart: the controller stamps the new version into
every runner Job, so roll the controller **and** runner image together (the
existing upgrade order). A standalone CLI install replaces its engine binary
with 0.23.3 (the digest in `third_party/kafka-backup-binary.digest`) and its
`LOGWEIR_ENGINE_VERSION` / `LOGWEIR_ENGINE_DIGEST` with the new pair
([quickstart.md](quickstart.md), step 4; `examples/cronjob-drill.yaml`). A spec
that names an `http://` endpoint must say `allow_http: true`; a saved
destination already cannot combine the two (rule R3).
**Scope:** source evaluation of every change from 0.21.0 to 0.23.3
([decision record](to-do/decisions/PROD-00-engine-route.md) §12: no capability gap
it lists is fixed, and nothing Logweir reads or renders changed shape). On a
compose stack (slot 4, Kafka 3.7.1, the engine under `linux/amd64` emulation)
CI's e2e command passed 177 tests, PROD-01.1's record-semantics contract
asserted on 0.23.3 included; the demo drill passed with both readers VALID; an
archive 0.21.0 wrote drilled with 0.23.3, and an archive 0.23.3 wrote drilled
with 0.21.0, both `pass`. On Kafka 4.3.1 the record-semantics, G-PITR, FX-1,
FX-7 and full-drill rows passed with the pin as well (34 tests). Unit rows refuse the `http://` combination in each
of the three engine documents and at phase 0 for drill and backup specs, with
mutants on those guards and on `doctor`'s pin, and `crates/logweir/tests/engine_pin.rs`
(CI's workspace run) holds every place that names the pin to one version.
`engine-matrix` run 37728540932 recorded `pass` for both 0.23.3 rows (Kafka
3.7.1 and 4.3.1) with PROD-01.1's rows asserted. Its 0.21.0 and 0.22.0 rows
recorded `fail(e2e suite)` because the pin guard then ran in the package the
matrix runs with each row's own engine, and stopped those rows before their
drill suites; it is not an engine finding, and the guard has moved. The
re-dispatch at the fix tip, run 37736333362, recorded every one of the seven
declared rows as declared: 0.21.0, 0.22.0 and both 0.23.3 rows `pass`, the
three below-floor rows `unsupported(lever-absent)`. The controller's
`LOGWEIR_ENGINE_VERSION` reaches a live runner Job only at the next PoC
upgrade.
**Rollback:** an older runner and controller run 0.21.0 again; `doctor` from
that build refuses 0.23.3. Measured: an archive 0.23.3 wrote restores and
verifies `pass` when this build drives the 0.21.0 engine. Not run, and reasoned
from source only: an OLDER Logweir build reading a 0.23.3 archive or receipt.
The manifest has the keys a 0.21.0 manifest has (`missing_topics` is omitted
when empty), the segment container is unchanged since 0.18.0, and the vendored
structs ignore unknown keys.

#### 29. A fresh install needs no approver key; an upgrade changes no namespace's approval (PROD-16.1)

**Changed.** Three approval modes by name: **confirm** (one person clicks
Create in the console, no key; internal `Ordinary`), **two-person** (PROD-16.2,
refused by name) and **strict** (an approver's personal key; `Governed` or
`legacy-governed-v1`). On a FIRST `helm install` (Helm 3.19+ or 4.x) with a
console, the managed identity and `identity.bootstrapFeatures.consoleKey: true`,
the identity hook generates the console's `ConsoleConfirmation` key
(`logweir-console-confirmation`, retained, never regenerated, key loss stops
the hook) and — when the cluster has no trust of its own (no `TrustPolicy` at
all, no `TrustRoster/default`) — creates one default `TrustPolicy`
`logweir-installation` for exactly the installation signer and that generated
console key, and marks the install (`logweir.dev/approval-default` on
`logweir-signing-trust`: a claim naming that policy's UID and both key ids,
honoured by the console and the controller only beside that exact hook-made
policy). Every namespace without its own policy is then confirm
(`default-confirm-v1`); an explicit binding always wins. An install without a
console, with `identity.externalSecret`, or whose console key was adopted from
a hand-made Secret gets no policy, no marker and no grant: no approver key by
default only where the console's one click needs it. The `localAdmin` console
confirms too, as `urn:logweir:local-admin#admin`. `approvalPolicy.default`
(`confirm` with `allowOrdinaryConfirmation`, or `strict`) overrides the marker;
`policies[].mode` accepts `confirm`/`strict`. The policy name
`default-confirm-v1` is now RESERVED beside `legacy-governed-v1`: an
approval-policy document that declares a policy of that name is refused by the
chart at render and by the controller and the console at start (which stops
the controller), so rename such a policy before upgrading; `mode: two-person`
and a `defaultMode` other than `confirm`/`strict` are refused the same way. The console's configuration gains
`confirmationKeyManaged` (the managed key may arrive after the console starts;
an operator-named key file that is missing still refuses to start). The API's
policy view gains `operatorMode` and `basis`, the create's `authorization` gains
`operatorMode` (OpenAPI `1.0.0-alpha.2`, additive). Item 29 lands after the
`v0.2.0-rc.1` publication commit `2c277dc1` and is not in that candidate.
**The install-only grant.** The `ClusterRole`/`ClusterRoleBinding`
`<release>-identity-trust` that lets the hook create a cluster-scoped
`TrustPolicy` is a `post-install` hook of that first install alone, deleted by
Helm when the install's hooks finish — succeeded or failed on Helm 3.19+ and
4.x; Helm 3.12–3.18 keeps it after a failed hook, so the chart refuses to render
it there (`identity.installationTrust needs Helm 3.19.0 or newer`) — and the
hook deletes it on every exit path it controls, a usage error included.
**An upgrade changes no namespace's approval mode:** no marker, no trust step,
every unbound namespace stays `legacy-governed-v1`; the hook only generates the
console key, or ADOPTS the one a PLAT-19.2 install made by hand under that name.
One thing does change on upgrade: a `localAdmin` console now confirms
namespaces ALREADY bound to an `Ordinary` policy (it refused them before), so
whoever can port-forward to it can authorise a restore there alone.
**Do:** nothing, to keep today's approval (first rename any policy named `default-confirm-v1`). To opt an older install into
confirm: a trust administrator adds the key from `logweir-console-trust` to the
`TrustPolicy` governing those namespaces (`ConsoleConfirmation`), then set
`approvalPolicy.allowOrdinaryConfirmation: true` and `approvalPolicy.default:
confirm` and upgrade ([install.md](install.md) §5f). **After a failed first
install:** `kubectl delete clusterrolebinding,clusterrole
<release>-identity-trust --ignore-not-found`, then check it is NotFound
([install.md](install.md) §5f). **Release coordinator, two steps (both done):** the merge
was inert (`identity.bootstrapFeatures.consoleKey` `false`); after CI published
the merge's runner, one commit re-pinned `identity.bootstrapImage` to runner rev
`2fe8d907`, refreshed `crates/logweir/tests/fixtures/bootstrap-image-help.txt`
from its `identity bootstrap --help` and set the value `true` by default
([install.md](install.md), *Release coordinator: re-pin bootstrap bytes*). An
operator who pins an older bootstrap image sets it `false`.
**Scope:** core rows (the unbound default, the bound marker with every binding
broken once, an older reader refusing `defaultMode`), identity hook rows (fresh
install creates trust and a claim the readers honour; no console, an adopted
console key and any existing TrustPolicy create nothing; upgraded,
hand-provisioned and adopted identities get neither; a patched marker is
ignored; the console key generated once, adopted, key loss; the trust grant
revoked on every exit path the binary controls; the policy body guard),
controller rows (a refused marker is unmarked, a failed read keeps the last
verdict), console rows (seven attacks on an upgraded install read legacy and
sign nothing; a fresh install confirms in one request; only the managed key may
be missing at start), controller and runner rows over an out-of-tree-signed
`default-confirm-v1` confirmation, chart rows (the transient grant absent from
an upgrade render and from installs without a console; the Helm floor, run with
Helm 3.18.6; every render passes only flags the pinned image's `--help` lists),
and a host journey (localAdmin console, Playwright, compose slot 3; a real
ClusterRoleBinding deleted by a failing hook). The PoC refresh that carries it
runs the live controller rows; the fresh-install rows wait for PROD-14.1's
clean-install exercise.
**Rollback:** remove `approvalPolicy.default` first (an older binary refuses a
document carrying `defaultMode`), and `confirmationKeyManaged` goes with the
chart (an older console refuses the unknown field). An older controller and
console ignore the marker: every unbound namespace is `legacy-governed-v1` again
and a pending confirmation under `default-confirm-v1` is refused
`ApprovalPolicyMismatch` — fail closed. The installation `TrustPolicy`, both key
Secrets and their public ConfigMaps stay.

#### 30. A plan can ask phase 7 to verify every record, and the scorecard says what its verdict covered (PROD-08.1)

**Added.** `sample.coverage: complete` in a drill or restore plan makes phase 7
read every archived segment of every partition of every restored topic, check
its sha256 against the manifest and decode it, compute the expected output from
each archived record's OWN timestamp, read every restored record back and
compare it with that output by `x-original-offset`, headers in order: exact
per-partition counts of missing, unexpected, duplicate, out-of-order and
different records, every faulty segment named. The manifest's first/last
count bound is not consulted, so PROD-01.1's out-of-order cases read right
there: a skipped segment and a record below the window floor FAIL, and a
correct point-in-time restore PASSES. `sample.complete_max_records` bounds it;
a bound that stops it signs `covered: false` and never a pass. Every 1.4.0
scorecard — sampled or complete — carries the new optional
`integrity.verification` block: the coverage, what it compared with (the
archive), whether header order was verified, the verified partitions' capture
gaps and pruned ranges as structured ranges, and a complete run's archive
integrity and replay comparison. The scorecard is format **1.4.0**; both
readers check seven new arms, IV-1 to IV-7, and print `integrity coverage:`
lines; `verify_scorecard.py` is 1.19.0
([drill-spec.md](formats/drill-spec.md#samplecoverage-and-samplecomplete_max_records-prod-081),
[the scorecard format](formats/drill-scorecard.md#integrityverification-format-140),
[stability.md](stability.md#scorecard-format-140-integrityverification-prod-081),
[the contract](to-do/decisions/PROD-08.1-integrity-contract.md)).

What changes on the upgrade:

- **A sampled drill fails a restored head that repeats or reorders source
  offsets** (`x-original-offset`); it used to key that head in a map, where a
  duplicate collapsed and order was invisible. Only a target the engine wrote
  wrongly is affected.
- **Phase 0 refuses**, exit 3, a plan with `coverage: complete` and
  `max_partitions`, or `complete_max_records` without complete coverage, or a
  bound of `0`.
- **A scorecard written by the new runner is format 1.4.0.** Readers built
  before PROD-08.1 accept it and ignore the block.

**Do:** nothing for existing plans, which stay sampled and byte-identical. To
verify every record, add `coverage: complete` to the plan's `sample` block and
re-approve it; budget for a read of the whole archive of the restored
partitions and of the whole output (measured in the decision record: about a
minute per GiB of one-KiB records with an optimised build on a laptop, against
about five seconds for the sampled check), and set `complete_max_records` if a
run must stop. A `Restore`, a `RehearsalSchedule` and the console cannot ask for
it yet. **Scope:**
`crates/logweir/tests/complete_verify.rs` (the fault matrix over real KBAK
segments: a corrupt unsampled segment, an omitted segment in the store and in
the target, a duplicate, a reorder, reordered headers, compaction holes,
non-monotonic timestamps, the bound), `crates/logweir/tests/orchestrator.rs`
(a complete plan through every phase, and a changed record past the canary),
the seven arms in both readers with the invariant corpus and the parity gate,
and planted mutants, each killed. Live, on the compose stack: the complete
restores added to
`e2e/tests/record_semantics.rs`'s timestamp, shapes and compaction rows, and
`complete_coverage_hashes_every_segment_outside_the_window` and
`complete_coverage_over_faulted_targets_on_the_real_broker_and_archive`.
**Rollback:** an older runner ignores `sample.coverage`, runs a sampled check
and signs format 1.3.0 again with no block; the 1.4.0 scorecards already written
stay valid under both readers.

#### 31. Every scheduled run's recovery point is offered from the Catalog view (FX-17)

**Changed.** The catalog sync publishes each point's backup set id, receipt key
and manifest key through the product's redactor, which read a manual run's
set id (a UUID) as an identity but not a scheduled run's
`<schedule uid>-<yyyymmdd>-<hhmmss>`, or `…-r<k>` for a retry: 52 to 55
lower-case characters, over the length at which the redactor treats an
unrecognised run as material. A catalog synced by a runner up to
`v0.2.0-rc.1` therefore published every SCHEDULED point as
`backupId: "[redacted]"`, `receiptKey: "[redacted].receipt.json"` and
`manifestKey: "[redacted].json"`, and the console's Catalog view offered none of
them ("not offered: … `[redacted].receipt.json` …"): on the PoC, 84 of 370
points, the whole first page. The redactor now reads exactly the shape the
controller mints — a lower-case UID, a slot that is a real instant, at most a
one-digit retry suffix — as an identity, as it does a UUID, so those three
fields are published whole and the points are offered. Nothing else is
exempted: a near miss (an impossible date, an upper-case UID, a two-digit
retry, anything after the slot) is still withheld. The same set id now also
survives in check details and remedies, where a scheduled set's segment key
used to read `[redacted]`. When a set id or receipt key does come back
redacted, the console says so in ONE reason naming the field and its cause
(an older runner, or a set id chosen for `logweir backup run` that is not a
public form), instead of the old "keeps a ULID run id" advice or "no usable
backup set id".

What changes on the upgrade:

- **A catalog's published view does not change until its next sync** with the
  new runner image. Until then its scheduled points stay "not offered", now
  with the reason above.
- **The schedule page shows the catalog's verdict for scheduled runs.** It
  joins a run to its catalog row on the set id, so every scheduled run there
  read `not in the catalog` in both verdict columns while the catalog held it,
  and a scheduled set the catalog marks not selectable kept its restore link.
  After the re-sync the columns carry the catalog's own words, and such a set
  reads "not restorable: the catalog marks this set not selectable" instead of
  a link. A `ProtectionPolicy` that judges a scheduled run with no receipt
  digest joins its catalog row the same way, on the set id.
- **A set id chosen for `logweir backup run`** (`--backup-id-override`, or a
  plan's `backup_id`) still has its receipt key withheld unless it is a public
  form — for example a UUID, or lower-case letters, digits, `.`, `-`, `_` and
  `=` under 40 characters: the run id already spends the key's one free
  component. Its points are listed and not offered, and the row says why; a
  retention pass keeps such a point as `protected: Unknown`, and a rehearsal
  never selects one.
- **A `RetentionPolicy` starts weighing scheduled sets one by one.** Every
  scheduled point used to share the set id `[redacted]` and the manifest key
  `[redacted].json`, so retention treated them as ONE set and no scheduled set
  was ever expired: while any one was retained, every scheduled candidate was
  `protected: SharedSegment`; when all were due, the group was held back over
  `maxDeletionsPerRun` (`truncatedByCap`) or, when it fit, its manifest key
  under no set bound refused the WHOLE plan, UUID sets included. An active
  `Restore` of a scheduled set did not protect its point either, because the
  join never matched; the shared group hid that. After the re-sync each
  scheduled set is its own set: those outside `keepLast` / `keepDays` become
  candidates, an active `Restore` protects its own, and an `Enforce` policy
  with `requireApprovedPlan: false` deletes the due ones at its next run. A
  scheduled set and its retry (`…-r1`) are separate sets, and deleting one
  never touches the other.
- **A catalog-point readiness check (`Preflight`)** compared a plan's set id
  and receipt key against the redacted row and refused it; it now compares
  against the whole values. A rehearsal that drew a scheduled point bound
  nothing redacted: a candidate whose only receipt key is the redactor's output
  is not selected.

**Do:** before the runner image rolls, read the plan preview of every
`Enforce` `RetentionPolicy` with `requireApprovedPlan: false`, or set it to
`true` until you have read the first plan after the re-sync. After the runner
image rolls, sync each `RecoveryCatalog` once — set `spec.syncRequest` to a new
value — rather than wait for its interval.
**Scope:** `crates/logweir-core/tests/check_contract.rs` (the PoC's set ids
kept bare and in receipt, manifest and segment keys; thirteen near misses
withheld; F1's credential probes with the scheduled id as the anchor; the
fixture `ui/tests/fixtures/set-ids.json`, edges included, that the console's
rows read too), `crates/logweir/tests/check_cli.rs` (a real catalog sync
publishes a scheduled point whole and withholds a forged one; the resume
cursor survives), `crates/weirkeeper/tests/cadence.rs` (every set id the
schedule controller mints survives), `retention_policy_controller.rs` (a
redacted set is `Unknown` and refuses no plan; a scheduled set and its retry
are weighed one by one), `rehearsal_controller.rs` (a redacted binding never
qualifies), `crates/logweir-reaper/tests/reaper.rs` (deleting a scheduled set
leaves its retry), the console rows in `ui/tests/restore-catalog.spec.js`,
`ui/tests/schedules-detail.spec.js` and `ui/tests/d3.spec.js`, and planted
mutants, each killed but one equivalent (FX-17). Live, on compose slot 3 with
the console on the host: seven real backups (four scheduled set ids, one a
retry) synced by this build's runner were all offered and each opened the
restore wizard on its point; the same archive synced by the `v0.2.0-rc.1`
runner image reproduced the PoC's "not offered" rows. And a second archive of
five real backups (scheduled `X`, `Y`, `X-r1`, `Z` and a manual run): the
controller's retention evaluation over this build's view planned `X` and `Y`
as two separate lines, over the rc.1 view kept every scheduled point
`Unknown`, and the `logweir-retention` worker deleted `X` and `Y` and left every
key of `X-r1`. Not yet proven on the PoC: the upgrade that carries FX-17
re-syncs its catalog and checks the Catalog view and the nightly schedule's
page.
**Rollback:** an older runner withholds the scheduled set ids again at the
catalog's next sync, the views return to "not offered" and `not in the catalog`
for those points, and retention treats every scheduled set as one shared set
again. Nothing in the archive changes on the rollback itself; sets an
`Enforce` policy deleted in between stay deleted.

#### 32. A recovery point records each topic's configuration, its portability and its owner, and the console defaults the replication factor from the source's (PROD-05.1)

**Added.** Every backup receipt `logweir backup run` signs now records, per
named topic, the source's partition count and replication factor, the topic's
explicit overrides and the effective value of each semantic key (retention,
compaction, timestamps, min in-sync and the rest) — each with its source and a
portability class from a table measured on the 3.9 and 4.3 broker lines — and
the topic's declarative owner: a Strimzi `KafkaTopic`
(`--kafka-topic-resources <file> [--strimzi-cluster <name>]`) or the plan's own
`source.topic_owners` — and where the run looked for one
(`owner_detection`), so an owner nobody looked for reads "owner not checked",
never "applied through the admin API". A topic whose configuration read was
denied records NO entries, never "no overrides". Keys Kafka 4.0 removed are
recorded and marked `removedInKafka4`; a sensitive entry is recorded by key,
never by value. The receipt and the catalog point record are format **1.3.0**;
both readers check ten new arms, 12 to 21, and print one `topic_configuration`
line per topic; `verify_scorecard.py` is 1.20.0. The catalog's view lists an `Available`
point's topics with their recorded layout, the product API publishes them as
`PointView.topics[]` with each topic's `applyRoute` (`unknown` where the run
did not look for owners, beside `PointView.ownerDetection`), and the console's
restore wizard defaults the
replication factor to the largest selected topic's source factor, capped at
the target's broker count, saying which catalog and point it came from
([backup-receipt.md](formats/backup-receipt.md#topic_configuration--the-topic-configuration-model-format-130),
[stability.md](stability.md#receipt-and-catalog-point-format-130-topic_configuration-prod-051),
[the model and the table](to-do/decisions/PROD-05.1-configuration-model.md),
[ui/README.md](../ui/README.md)).

What changes on the upgrade:

- **Every receipt and catalog record a new runner writes is 1.3.0**, pinned or
  not. Readers built before PROD-05.1 accept them and ignore the new fields.
- **Phase −1 refuses**, exit 3, a backup plan whose `source.topic_owners`
  names an unplanned topic, a kind other than `strimzi` or `external`, a
  reference that is blank, over 256 characters or carries a control character,
  or one topic twice.
- **A `Backup` or `BackupSchedule` records `owner_detection: []`**: the
  controller passes neither a declaration nor `KafkaTopic` resources yet, so
  its receipts say each un-owned topic's owner was not checked.
- **The receipt's replication factor is read from the source's metadata**: the
  pinned engine's manifest keeps it for the first topic it saves only (measured;
  the decision record names the upstream lines).

**Do:** nothing for existing plans. Pass `--kafka-topic-resources` (or declare
`source.topic_owners`) for topics an operator such as Strimzi or Terraform
manages; a restore of such a topic is meant to export desired state for its
owner rather than change it behind the owner's back (PROD-05.2). A `Backup`
cannot declare owners yet (child row PROD-05.1a). **Scope:**
`crates/logweir-core/tests/backup_receipt.rs` (one row per arm),
`crates/logweir/src/backup/config_coverage.rs` (the projection, the secret, the
factor's source), the corpus and the parity gate over both readers, the catalog
and view rows (`crates/logweir/tests/catalog.rs`,
`crates/logweir/tests/check_cli.rs`, `crates/weirkeeper/tests/catalog_controller.rs`,
`crates/logweir-api/tests/d3_reads.rs`), the console rows
(`ui/tests/replication-factor.spec.js`), and live, on the compose stack on 3.9.2
and 4.3.1: `e2e/tests/topic_configuration.rs`'s three rows (the table against
the broker, the model end to end with its owners, a denied DescribeConfigs).
**Rollback:** an older runner writes 1.1.0 or 1.2.0 receipts again and records
no model; the 1.3.0 documents already written stay valid under both readers. A
catalog synced by an older runner lists no topics, and the console then
defaults as before and says why.

#### 33. SASL/PLAIN over TLS, SCRAM-SHA-256 and mTLS; a connection presents only its own credential (PROD-01.3) — required action

**Changed.** A connection (`KafkaCluster`, a spec's `auth:` block, the console
form, `logweir cluster-probe --auth-mode`) may use **`scramSha256`**,
**`plain`** (SASL/PLAIN — Confluent Cloud API keys, Azure Event Hubs
connection strings) or **`mtls`** (a TLS client certificate, from
`auth.clientCertificate`, a `kubernetes.io/tls`-shaped Secret) beside
`plaintext` and `scramSha512`, on both clients. **`plain` without `tls: true` is
refused, never dialled** (`PlainWithoutTls`: the CRD's rule, the controller,
`refusal-reason=PlainWithoutTls` at exit 3, and both client builders). Signed
documents name the mode: a receipt or catalog point naming a new mode is
format **1.4.0**, a scorecard **1.5.0** (MINOR; [stability.md](stability.md));
`verify_scorecard.py` is 1.21.0. **Security follow-up:** a connection presents
only a credential bound to it. Every runner refuses a projected password or
client key whose Secret lacks the connection's `logweir-binding` (UID and
endpoint digest) — `CredentialBindingMismatch`, before any client exists — so
a `KafkaCluster` that names another connection's Secret cannot make Logweir
present that credential to a broker of its author's choosing. The console API
takes the credential ONCE (`auth.credential`), creates the bound Secret itself
(owned by the connection) and refuses `auth.credentialRef`
(`422 existing_credential_refused`); a console CA comes from a ConfigMap.
**Do:** suspend the schedules that use a credentialed connection, apply the
CRDs, roll controller, runner and console together, then bind each existing
credentialed connection's Secret **one at a time, by a command that names both
the connection and the Secret, after an inventory** ([kubernetes.md](kubernetes.md)
§20.9 has the procedure), and resume. Before this release a `KafkaCluster`
could name another connection's Secret, so a connection naming a Secret that
another connection also names is an incident to investigate, not a Secret to
split or to bind in a loop: either would hand the credential to the second
connection's endpoint. Binding needs Secret `patch`, which for a connection
credential is as strong as Secret `get` — grant it accordingly. A client of
the product API that named `credentialRef` sends the password in
`auth.credential` instead; a credential value is no longer part of a create's
idempotency identity, so a same-key retry that changes only the value replays
the first create ([api.md](api.md)). A `BackupDestination` created with
`secret.new` by an earlier build carries a request hash taken over its secret
key: remove its `api.logweir.dev/request-sha256` annotation, or rotate the key. Admission-policy users: the
chart's policy now also admits `logweir.dev/kafka-client-certificate`.
**Scope:** on compose slot 4 (Kafka 3.7.1, engine 0.23.3 under emulation), the
`auth` profile's four listener shapes each passed a real backup (Logweir's
client at phase −1, the engine's client for the archive) and a real drill
restoring it into the same cluster, with the receipt and scorecard VALID in both
readers and a seeded-secret scan clean; a wrong password, a wrong CA, an
untrusted client certificate and PLAIN without TLS were each refused
(`e2e/tests/auth_modes.rs`, 8/8). The binding's refusal is proven on the shipped
binary for `backup run`, `drill run`, `cluster-probe` and the check runner
against a loopback sentinel that is never dialled; the API's write-only entry,
its refusals and a seeded-value scan by `crates/logweir-api/tests/connection_credentials.rs`.
No managed provider was dialled (OD-4): Confluent Cloud and Event Hubs move
from unsupported to **untested**, MSK through SCRAM-SHA-512/TLS stays untested
([support-matrix.md](support-matrix.md)). The live `KafkaCluster` journey on
docker-desktop is the next PoC upgrade's; none of the PoC's twelve connections
has a credential, so none needs binding there.
**Rollback:** an older controller and runner ignore the binding pair and the
new fields; a bound Secret keeps working with them. An older build cannot parse
a spec naming a new mode, and an older reader refuses a 1.4.0/1.5.0 document
that names one (the safe direction). An older console would send
`credentialRef` again; roll the console with the controller.

#### 34. A point-bound restore restores its point's own set, or nothing (FX-16)

**Changed.** A plan bound to a recovery point (`source.point`) had the point's
receipt, signature and manifest verified, and then restored whichever set
`source.backup` named — `latestCompleted` included — while everything the
runner takes from that receipt (the configuration capture coverage phases 3
and 7 compare against, the timestamp types the time basis is decided from,
the manifest pin) was applied to that set's records. The runner now refuses
such a run, exit 3, the message opening `PointBindingSetMismatch`
(`refusal-reason=GuardRefused`, so a `Restore` reads `exitReason:
GuardRefused`):

- **before any broker is contacted**, when `source.backup` is not the
  receipt's set (`backup: latestCompleted` beside a bound point is always
  refused: it names whichever set is newest when the run starts), or when,
  under the plan's storage, the engine would read the set's manifest anywhere
  but where the receipt attests it: the engine loads
  `<prefix>/<backup_id>/manifest.json`, so a plan pointed at a copy under
  another prefix, or at a parent of the point's prefix with another same-id
  set there, is refused, naming the prefix the set was written under;
- **after the set is described and before any target topic exists**, when the
  set about to be restored is not the one the binding verified: another set
  id, another manifest digest, or another manifest version (the manifest
  written again during the run). The set is chosen by the point's manifest
  key, never as the first listed set with the id, so a same-id copy elsewhere
  under the prefix is neither restored nor a reason to refuse.

The binding also reads the receipt, its signature and the manifest through the
restore's own archive handle: under the store contract, the controller-named
credential and CA, where it used the environment-driven client before.

What changes on the upgrade:

- **Console, catalog-route and rehearsal restores** name the point's set
  already and run as before.
- **A hand-written point-bound plan that says `backup: latestCompleted`** (the
  standalone disaster path) is refused. Name the point's set instead
  (`source.backup: <the receipt's backup_id>`, which `logweir catalog list`
  shows) and approve the new plan.
- **A standalone point-bound plan whose `source.storage` prefix is not the one
  the point's set was written under** (a copy of the archive under another
  prefix, or a parent prefix) is refused; the refusal names the prefix. A copy
  that keeps the original keys, in another bucket, restores as before.

**Do:** before the runner image rolls, change every standalone point-bound
plan that says `backup: latestCompleted` to name its point's set, and approve
it again. Nothing else.
**Scope:** `crates/logweir/src/drill/binding.rs` (the plan half: another set
and `latestCompleted` refused although the point verifies; each of the four
restored-set disagreements refused alone, naming itself),
`crates/logweir/tests/orchestrator.rs` (each disagreement refused with no target
topic, no fingerprint and no scorecard; the matching set runs; the set check
comes before FX-8's time basis), `crates/logweir/tests/restore_phase.rs` (the
real binary refuses both plan shapes and a nested point before phase 0, the
bootstrap never dialled), `crates/logweir-store/tests/storage.rs` (the engine's
manifest key), source guards in `crates/logweir/src/drill/mod.rs` (where the
check sits; the verified set travels with the coverage; one archive-handle
constructor), and planted mutants, each killed (FX-16 and its fix round). Live,
on compose slot 4 with engine 0.23.3 (`e2e/tests/point_set_binding.rs`): the
plan bound to point A and naming A restored A's 30 records although a same-id
copy of A was listed first; naming a later set B or `latestCompleted`, a copy
of A under another prefix, an edited copy, and a point whose set is nested
under the plan's prefix with another same-id set where the engine reads were
all refused before phase 0, with no target topic. The build before FX-16
restored each of them under the point's receipt (B's 45 records, a copy, or
the other same-id set's 45 under a 60-record point, signed `fail-integrity`);
the build at FX-16's first round still restored that last one, and refused
the truthful plan because of the copy listed first. Not yet proven on the PoC: a catalog-point restore after
the upgrade that carries FX-16.
**Rollback:** an older runner restores whatever `source.backup` names again,
`latestCompleted` included, and reads the binding through the environment's
client. No archive, catalog or evidence object changes in either direction.

#### 35. The engine is Logweir's build of `kafka-backup` 0.23.3, for amd64 and arm64; the images are signed (PROD-00.2)

**Changed.** The runner image's engine is no longer OSO's released binary. It
is **Logweir's build of the vendored OSO 0.23.3 source** (OD-3): the tarball is
checked against its checksum, Logweir's ordered patch folder
(`third_party/kafka-backup-patches/`) is applied and the engine is compiled
`--locked` for **linux/amd64 and linux/arm64**, so the runner image, like the
other three, now has an arm64 variant. `kafka-backup --version` prints
`kafka-backup 0.23.3+logweir.1`, which `logweir doctor` accepts exactly; OSO's
own `0.23.3` is named as OSO's release and passes only as the declared
rollback. The first patch is a lockfile-only bump of three dependencies the
engine's new `cargo deny` gate found in its shipped graph: rustls 0.23.45
(RUSTSEC-2026-0285), h2 0.4.19 (RUSTSEC-2026-0258) and spin 0.9.9 (0.9.8 was
yanked). Engine vulnerabilities are now in `SECURITY.md`'s scope, and the
engine's `cargo deny` also refuses any crate from outside crates.io. One
version names one build: a change to a patch or the tarball bumps `<n>` and is
appended to `third_party/kafka-backup-builds.txt`, and a reused version is
refused. The image declares its engine in `/etc/logweir/engine-identity`, and
every scorecard and receipt signs that declaration: new documents say
`engine.version: 0.23.3+logweir.1` and an `engine.digest` that is Logweir's
build-input digest (`third_party/kafka-backup-build.env`), not an image
digest. **The controller no longer puts `LOGWEIR_ENGINE_VERSION`/
`LOGWEIR_ENGINE_DIGEST` in any Job**, and a drill or backup asks its engine
for `--version` before it signs anything and refuses a version the binary
does not print. From the first
`main` publication after the merge, CI signs all four images keylessly,
attests the runner's SBOM and records SLSA provenance.
**Do:** roll the controller and the runner image together, as before.
**Never pair this controller with a runner image published before this
change:** that image declares no engine, the controller now gives it none, and
its runs are refused (exit 1, before the engine spawns, nothing signed)
instead of signing an engine that did not run. Verify a digest before
deploying it with the pinned commands in [install.md](install.md#verify-the-images),
never with an identity regular expression. A standalone CLI install replaces
its engine with Logweir's build (copy it out of the runner image, or
`scripts/engine-source.sh build`) and exports the new
`LOGWEIR_ENGINE_VERSION=0.23.3+logweir.1` and digest
([quickstart.md](quickstart.md), step 4). On arm64 nodes, runner Jobs now run
natively. The identity bootstrap image pinned in the chart predates this
change and stays amd64-only until it is re-pinned.
**Scope:** on one compose stack (Kafka 3.7.1), the demo drill, the full-drill
suite, G-PITR and PROD-01.1's record-semantics rows ran with OSO's 0.23.3 binary
(linux/amd64, emulated) and with Logweir's build on linux/arm64 (native) and
linux/amd64 (emulated). Both builds compared SAME against OSO's binary: the
signed scorecard minus identity and timing, 24 test verdicts, and the nine
record-semantics outcome files, with the contract asserted on Logweir's build.
A negative control, the same inputs plus a scratch patch that flips one byte
of every restored value, failed the comparison: the drill `fail-integrity`,
13 of 15 full-drill rows, G-PITR and all eight record-semantics rows
([decision record](to-do/decisions/PROD-00-engine-route.md) §13.5).
`scripts/check-image.sh`, including the new check 8 (the declared engine is
the engine), passed on both platforms' runner images and on the rollback
image. The engine's `cargo deny` fails on the unpatched lockfile and passes
with patch 0001. Guard tests (`engine_build.rs`, `engine_pin.rs`, the engine
identity and `doctor` rows, `scripts/test-check-cosign-verify.py`) carry
negative controls, and fourteen mutants were killed. Not run: keyless signing,
the SBOM attestation and the provenance, which happen only in `images.yml` on
`main` (the first publication is their first run). The runner's identity file
reaches a live Job at the next PoC refresh. Fix round (the row's review): the
build ledger, the `sources` gate and the version probe each carry negative
controls (a reused version, a planted `git+file://` source, and a binary that
prints another version, refused by a live drill and a live backup), and the
e2e suites (`guards`, `backup_argv`, `full_drill`, G-PITR, record semantics)
passed with Logweir's arm64 engine and the probe in place.
**Rollback:** for one release, OSO's released binary stays buildable:
`docker build --platform linux/amd64 --build-arg ENGINE_SOURCE=oso` produces a
runner that carries it and declares OSO's identity, so its scorecards and
receipts name OSO's 0.23.3 and its image digest
([install.md](install.md#rolling-the-engine-back)); set `runnerImage` to it.
It is amd64-only. An older controller and runner pair runs OSO's 0.23.3 again
and its `doctor` refuses `0.23.3+logweir.1`. Archives are unaffected in either
direction: patch 0001 changes no source file, so the segment format and the
manifest are OSO 0.23.3's. Measured: every parity drill restored, with
Logweir's build, an archive OSO's binary wrote. The reverse (an archive
Logweir's build wrote, restored by OSO's binary) is reasoned from that
identity of source, not run.

#### 36. An early-stopped restore is never signed `pass`; `max_partitions` samples every topic first (FX-23)

**Changed.** A restore the engine stopped early — a SIGTERM to the engine
(`pkill kafka-backup` on a CLI host, `docker stop` of its container) finishes
the topic it is on, exits 0 and never starts the rest — could be signed `pass`
by the default sampled verification when `sample.max_partitions` was below the
number of partitions with records in the window: the cap kept the first
partitions in manifest order, which is the order the engine restores in, and
the one count bound over all topics had slack. The sampled verification now
(a) holds every mapped partition to its own count bound — a segment the
point in time cuts across proves the one record whose timestamp opens or
closes it inside the window — so an empty partition the archive proves holds
records in the window fails, by name; (b) keeps one partition of every topic
before a second of any under `max_partitions`, and names the topics it could
not reach in the scorecard's new optional `sample.unsampled_topics`. Every
sampled scorecard this build signs is format **1.6.0** (MINOR), named topics
or not, so the version marks the fixed build (
[stability.md](stability.md#scorecard-format-160-sampleunsampled_topics-and-a-stricter-sampled-check-fx-23));
and (c) fails a restore whose engine offset report has no entry for such a
partition — read by streaming past the report's per-record section, so the
read holds a few kilobytes however large the restore (8.5 KB of heap for a
235 MB, 2,000,000-record report). All three are new causes for the existing `fail-integrity`, exit 2.
The in-cluster runner was not exposed (`logweir` is PID 1 there and no
pod signal reaches the engine), and a scheduled rehearsal never truncated when
the catalog knew the point's partition count (it drops points larger than its
cap; a point of unknown size is kept, and its sample now reaches every topic
first). `verify_scorecard.py` is 1.22.0; both readers say, for every sampled
`pass`, whether its version proves these checks ran: only 1.6.0 or later does,
because a 1.4.0 or 1.5.0 document is the same bytes whichever build signed it.
**Do:** a consumer that matches a scorecard's exact `format_version` must
accept `1.6.0`: every sampled scorecard is 1.6.0 from this build on (the
major is unchanged, so both readers and every older reader accept it).
Otherwise nothing. A correct restore of an archive whose timestamps do not run
backwards within a segment cannot fail the new checks (one whose timestamps do
can now fail the per-partition bound where the sum absorbed a record every
restore drops — [the limitation](stability.md#recovery-point-selection-uses-segment-first-and-last-timestamps));
a sampled `pass` from an earlier build over a plan with `max_partitions` below the
partitions in the window is worth re-checking
([verify-a-scorecard.md](verify-a-scorecard.md#what-a-sampled-pass-guarantees-and-what-it-does-not)).
One fixture moved: a target holding exactly the wholly-inside count when a
straddling segment opens inside the window was a pass and is a fail, because
that segment's first record is missing.
**Scope:** on compose slot 1 (Kafka 3.7.1, engine 0.23.3 under emulation), a
two-topic `logweir restore run` whose engine was sent SIGTERM while the first
topic was landing: the engine finished the first topic, exited 0 and wrote
nothing to the second. The build before FX-23 signed `pass` (exit 0) at
`max_partitions` 3 and 1; this build signs `fail-integrity` (exit 2) at both,
naming every partition of the second topic and the engine report's finding,
with `sample.unsampled_topics` at `max_partitions: 1`, and both readers accept
the signed documents (`e2e/tests/stopped_restore.rs`). The review's probe
holes and controls, one row per fix deciding alone, and 25 mutants (all
killed) are unit rows (`crates/logweir/tests/stopped_restore.rs`).
**Rollback:** an older runner samples the first N partitions again, judges one
aggregate bound and ignores the engine report; the 1.6.0 scorecards already
written stay valid under both readers, and an older reader ignores the field.

#### 37. One crate holds all `unsafe` code; consumer-group and ACL reads land behind it (PROD-04.0b)

**Added, inside the build; no command uses it yet.** Logweir now calls the
librdkafka functions that the safe `rdkafka` API lacks (OD-6 (a2); ADR 0004's
amendment in [architecture](architecture.md)). The calls are the typed and
the all-type consumer-group listings, the group description,
DescribeCluster's authorized operations, and DescribeAcls. They go through ONE
crate, `logweir-rdkafka-ffi`, which is the only place in the workspace where
`unsafe` may appear.

Every other package is compiled with `unsafe_code` forbidden in every target:
library, binaries, examples, tests, benches and build scripts (the root
`Cargo.toml`'s `[workspace.lints.rust]`). `scripts/check-unsafe-scope.sh`, in
`just lint`, checks that and scans the tree as a second layer.

On top of the calls, `logweir-kafka` decides which consumer groups a capture
may take, and what an ACL read is worth:

- **Groups.** Classic and KIP-848 consumer groups are capturable. Share,
  streams and other-protocol groups are excluded `GroupTypeNotCaptured`. An
  id no listing shows is `GroupNotFound` only on a complete listing, or when
  a targeted describe answers it under this principal's filtering. It is
  `NotVisibleToPrincipal` when that describe is refused, and failed otherwise.
- **ACLs.** "0 bindings" is trusted only after two positive probes: the
  broker's `authorizer.class.name`, and the principal's Describe on the
  cluster. Bindings librdkafka cannot name are counted, never exported.

PROD-04.1 (capturing positions) and PROD-05.3 (exporting access policy) build
on this. No CLI, controller, API, console or archive behaviour changes in this
item.

**Known librdkafka behaviour, measured and recorded**
(`docs/to-do/decisions/PROD-04.0-admin-path.md` §14):

- a describe refused because the principal may not see the group leaks
  224 bytes inside librdkafka, per call;
- the legacy group listing reports only the last broker's error;
- on a broker below Kafka 3.8 (ListGroups below v5, such as the 3.7.1 compose
  default) no group has a type, so every group is excluded
  `GroupTypeNotCaptured`.

**Do:** nothing.
**Scope:**
- `crates/logweir-rdkafka-ffi`, with unit rows: bounded calls with no broker,
  input refusals, and a 100,000-call soak;
- `crates/logweir-kafka/src/{groups,acls,access,rdkafka_admin}.rs`, with unit
  rows per trap and planted mutants, each killed;
- the gate and its negative control `crates/logweir/tests/unsafe_scope_gate.rs`;
- `e2e/tests/group_admin.rs`, against the brokers' own tools on 4.3.1, 3.9.2
  and the 3.7.1 default (the `acl`, `streams-protocol` and `cluster3`
  profiles, and a 7,000-call soak);
- memory checks: macOS `leaks`, Guard Malloc, and (in the review) ASan on
  Linux.

**Rollback:** an older build has no FFI crate and no workspace lint table. No
archive, catalog, evidence or API object changes in either direction.

#### 38. Every other credential reference presents only a credential bound to it (FX-20) — required action

**Changed.** PROD-01.3's binding (item 33) now guards every place a writable
object names a credential Secret beside an endpoint its author chooses. A
`BackupDestination`'s `SecretKeys` grants are bound to the destination (its UID
and its archive route), a `RetentionPolicy`'s delete-capable key to the policy,
its destination's route and its scope, each `ProtectionPolicy` route's Secret
to the policy, the sink kind and the PagerDuty endpoint, and an inline
`archive.secretRef` (a `Backup`, a schedule, a `Restore`'s `sourceArchive`) to
the location the runner dials — every field that shapes its URL: scheme,
bucket, endpoint, region, addressing style and `allowHttp`. An S3 region that
is not a region name (`^[a-z0-9-]{1,32}$`) is refused by name,
`StorageRegionInvalid` (`refusal-reason=GuardRefused`), by every runner before
any client exists, by the engine renderers and by every object-store client:
without an endpoint the region is part of the host. Every runner
(`backup run`, `restore run`, the check runner, the catalog sync and evidence
fetch, `logweir-retention`, `logweir notify deliver`) refuses an unbound or
foreign Secret with `CredentialBindingMismatch` **before it builds a store or
composes a request**: exit 3 and the terminal state on a `Backup` or `Restore`,
the check code on a `Preflight` row, `Enforced=False/CredentialBindingMismatch`
on a `RetentionPolicy` (nothing is deleted), and
`notify-result=<sink>:refused` with `NotificationsDelivered=False/
CredentialBindingMismatch` on a `ProtectionPolicy` (the other sinks are still
delivered). Editing a PagerDuty route's endpoint changes its binding, so the
routing key never follows an edit. The values are published on
`BackupDestination.status.credentialBinding`,
`RetentionPolicy.status.credentialBinding` and
`ProtectionPolicy.status.credentialBindings` (additive CRD fields). A Secret
may carry several bindings (whitespace- or comma-separated); one that matches
is enough. The product API no longer lets a destination name an existing
Secret: a create (and `:from-legacy`) refuses `secret.existing`
(`existing_credential_refused`), `:update-access` accepts only a Secret the
destination already names, and a rotation's `secret.new` is written to a new
bound Secret `lwd-<destination>-<role>-<suffix>`. The console's create form
offers no existing Secret and starts on a new credential. The chart's
`archive.s3.region` and `evidence.controllerIdentityLocations[].region` refuse a non-region
spelling, and the API's `region_invalid` message no longer repeats the value.
**Do:** suspend the schedules that use a credential Secret, apply the CRDs,
roll the controller, the runner and the console together, then bind every
existing credential Secret **one at a time with `scripts/bind-credential.py`**
— dry first, then `--apply --confirm-endpoint` once the credential's owner has
confirmed the endpoint it prints ([install.md](install.md), *Bind every
existing credential Secret after the upgrade*). The tool computes the binding
from the object's UID and the spec it prints, and refuses when the published
status says otherwise (a status that lags an edit); an `s3://` location states
its region, path style and `allowHttp`. It refuses a Secret any other object
also names (an incident), a Secret owned by or minted for another object, and
a Secret already bound elsewhere, and writes one key under a
`resourceVersion` precondition. Remove the pre-PROD-01.3
`api.logweir.dev/request-sha256` from a console-made destination, and rotate a
guessable key (its create's audit record keeps the hash). Resume the
schedules. Until a Secret is bound its runs are refused, closed.
**Scope:** unit and mock-cluster rows per site, each with its negative control
(`crates/weirkeeper/tests/{protection_controller,destination_controller,retention_policy_controller,backup_controller,restore_controller,preflight_controller,credential_backstop}.rs`,
`crates/logweir-api/tests/destinations.rs`), the shipped binaries against a
loopback sentinel that is never dialled (`notify deliver`, `backup run`,
`drill run`, `catalog list`; `crates/logweir/tests/{notify_deliver,credential_binding,guard_cli}.rs`),
the review's region-injection probe refused twice beside a store that does
dial its control (`crates/weirkeeper/tests/fx20_region_binding.rs`,
`crates/logweir-store/tests/region_backstop.rs`), one binding fixture the
product and the upgrade tool both check
(`e2e/fixtures/credential-binding/bindings.json`),
the real runner driven by the controller's rendered Job
(`schedule_controller.rs`), the retention worker binary
(`crates/logweir-retention/tests/worker.rs`), and the upgrade tool's offline
rows (`scripts/test_bind_credential_rows.py`). The live journey on
docker-desktop (the PoC's `primary` destination, a thief destination and policy
against a sentinel) is the next PoC upgrade's.
**Rollback:** an older controller and runner ignore every binding variable and
the new status fields; bound Secrets keep working with them. An older console
offers `existing` again, which this API refuses — roll the console with the
controller.
#### 39. A topic phase 0 creates is read, or handed to the engine, only once the cluster serves it (FX-18)

**Changed.** Phase 0 creates the target topics (and, on a `LogAppendTime`
broker, the one probe topic whose `message.timestamp.type` it reads back).
Kafka answers a create before every broker serves the new topic, so a read or
an engine produce that follows at once could meet `UnknownTopicOrPartition`,
`LeaderNotAvailable` or `NotLeaderForPartition`. CI met the last one. Phase 0
now waits, bounded, until every partition of each topic it created has a
leader that answers. A topic still not served at the bound is exit 1 naming
it, never a pass and never a guessed value. The probe's configuration read
after the create retries the same propagation answers, plus the empty
DescribeConfigs answer a topic gives while it propagates, and returns every
other error at once.

**Do:** nothing.
**Scope:**
- `crates/logweir-kafka` (the wait and the read classifications, each a
  pure function with a unit row);
- `crates/logweir/src/drill/phase0_admit.rs`, with phase rows, including a
  created topic that is never served (exit 1);
- the guard `e2e/tests/created_topics.rs`: every e2e helper that creates a
  topic waits until it is served;
- the compose e2e suite. With the wait removed, CI's `NotLeaderForPartition`
  came back in 3 of 13 runs under load; with it, none did.

A run whose target cluster never serves a created topic leaves those topics
for an operator to remove, as a failed create in the same batch already did.

**Rollback:** an older runner creates and uses the topics at once again. No
document, archive or API object changes in either direction.

#### 40. A connection that sends nothing is closed at the console's header deadline; the console speaks HTTP/1.1 only (FX-24)

**Changed.** `logweir-api` (the console) closed a connection that sent part of
a request head at its ten-second header deadline (R4), but never timed out a
connection that sent no byte at all: the listener first read the leading bytes
to choose HTTP/1 or HTTP/2, with no timer, and the deadline began only after
them. 256 silent sockets, the console's connection ceiling, therefore held every
connection slot for as long as their client liked, and the console answered
nobody; in shared mode anything that can reach the pod could do it. The console
now serves HTTP/1.1 only, and the deadline starts when a connection is
accepted: a silent socket is closed at ten seconds like a partial one, and its
slot is free again. A client that opens with the HTTP/2 preface (prior
knowledge, `h2c`) is closed at its first line instead of being served HTTP/2,
which had no header deadline either. Browsers, an ingress controller dialling
an HTTP backend and kubelet probes all speak HTTP/1.1 to the console, and
nothing Logweir ships spoke HTTP/2 to it. The deadline covers the request head
only; a body a handler is reading and an answer a client has stopped reading
are bounded by item 44 (FX-24b) ([api.md](api.md#conventions)).
**Do:** nothing, unless an ingress controller was configured to dial the
console's Service with HTTP/2 (an `h2c` or gRPC backend, never the chart's
setting): return it to HTTP/1.1, or the console is unreachable through it
([api.md](api.md#conventions)).
**Scope:** rows on the built binary (`crates/logweir-api/tests/local_admin.rs`):
a silent connection is closed no sooner than ten seconds and within fifteen, while
one opened beside it that sends a complete request after three seconds is
answered; 256 silent connections hold a queued request and then the deadline
releases it, every silent socket closed by the server; an HTTP/2 preface is
closed at once; the R4 partial-head row is unchanged; the deadline the
connection builder is handed is pinned to the documented ten seconds. Twelve
mutants are each killed: the deadline removed two ways, raised to 300 s,
doubled or set to twenty seconds at the call site, started only once the
first byte is readable, the old version sniff restored, cut to one second (the
three-second client fails), the connection ceiling raised, removed or never
released, and the controller's health listener without its deadline. Live, the built binary on the host in localAdmin mode: a silent
socket closed at 10.0 s (still open at 16 s before), and a request behind 256
silent sockets answered at 10.0 s (unanswered at 25 s before). The
controller's in-pod health listener already dropped a silent connection at its
two-second deadline; a row now pins that. The shared-mode console's probe runs
at the PoC upgrade that carries this item.
**Rollback:** an older console serves HTTP/2 prior knowledge again and leaves a
silent connection open; nothing is stored, so nothing needs converting.

#### 41. A replication factor the archive does not record is never read as matching; the engine records every topic's (FX-21)

**Changed.** Engine 0.23.3 records a topic's source replication factor in the
archive's manifest for the FIRST topic a backup saves only, and phase 7 used
the restored topic's own factor in place of a missing one, so every other
topic's replication-factor difference was signed as no divergence. Phase 7 now
compares the factor only where the source's is recorded: the manifest's, else
the bound recovery point's receipt (`topic_configuration`, format 1.3.0,
Logweir's own read of the source). Where neither records it, the scorecard
names it in `topic_parity.not_assessed` as `"<target topic>:
replication_factor (notRecorded)"`, with the twin `"<target topic>:
replication_factor not assessed (notRecorded)"` in `unexpected_divergence`, and
both readers print it in their `configuration parity: NOT ASSESSED for …`
line. A partition count the manifest does not record (an archive before engine
0.17) is the same case. The engine itself is fixed too: Logweir's first real
engine patch, `0002-manifest-replication-factor.patch`, makes the manifest
record every topic's factor, so the engine is now `kafka-backup
0.23.3+logweir.2` (build-input digest
`sha256:2bca49d72b92fc9d96d69ff2a8b64faef2c837bfbff8ba92326723f549197db8`,
appended to `third_party/kafka-backup-builds.txt`); `logweir doctor` accepts
exactly that version. No document format moves: the new entries are content
in two existing lists that can only move a verdict to the safer side (MINOR
under OD-7's third case, [stability](stability.md#a-source-replication-factor-the-archive-does-not-record-is-not-assessed-fx-21)).
**Do:** roll the controller and the runner image together, as for item 35. A
standalone CLI install replaces its engine with build 2 and exports
`LOGWEIR_ENGINE_VERSION=0.23.3+logweir.2` and its digest
([quickstart.md](quickstart.md), step 4). No field marks FX-21's documents:
a reader tells them apart by the writer identity every scorecard carries,
`engine.version` (`0.23.3+logweir.2`, or a later `+logweir.<n>`) with
`engine.digest` (`sha256:2bca49d7…`), which a runner image declares and ships
beside the `logweir` that wrote the document, plus the archive's manifest
(`source.backup_id`, `source.manifest_sha256`). For a scorecard with any other
`engine.version`, or one a standalone CLI signed, a topic whose
`topics[].source_replication_factor` the manifest lacks has no
replication-factor finding, whatever the scorecard lists.
**Scope:** unit rows over the rule (not recorded, recorded as 0 or less, only
the factor missing) in both modes, phase-7 rows through `run` (no record, the
receipt's factor where the manifest has none, the manifest's first where both
do, the partition count, a `newTopic` document phase 8 signs), the arm NR-5
row in both readers and the parity script's new case, and ten mutants, all
killed (`claude/artifacts/fx-21/mutants/`). Live, on compose slot 1 with `cluster3`
(Kafka 3.7.1): three topics at factors 3, 2 and 3 backed up in one run and
restored by `newTopic` restores, unbound and bound to the point, on three
engines. OSO's 0.23.3 (the container route) and Logweir's `0.23.3+logweir.1`
recorded one factor of three; this build named the other two `notRecorded`
unbound and compared all three from the receipt bound, and a `logweir` built
before FX-21 signed the same two topics with no replication-factor entry at
all. `0.23.3+logweir.2` recorded all three, each the broker's. The engine's
own unit rows for the patch fail without it. Parity on one stack (Kafka 3.7.1),
the engines run natively: `full_drill` 15/15, G-PITR and the record-semantics
rows (contract asserted on build 2) compared SAME between builds 1 and 2, and
the demo drill, the suites and the record-semantics files compared SAME
between OSO's 0.23.3 (container route) and build 2; the replication factor
the patch now records appears in none of those outputs. On a loaded host a
natively built engine can fail a restore it starts within a second of phase
0 creating the target topics (`Partition N not available`; measured 1 restore
in 7 and, in the review, 1 in 17), so the parity runs waited 5 s before a
restore (a test shim; [e2e/README.md](../e2e/README.md)); the race is its own
tracker row.
**Rollback:** an older `logweir` writes the old silence again; the documents
this build wrote stay valid for every reader. An engine rollback (to
`+logweir.1`, or OSO's 0.23.3 with `ENGINE_SOURCE=oso`) records one factor per
backup again, which this build then names `notRecorded` unless a 1.3.0 receipt
is bound. Archives are unaffected in either direction: the patch changes what
the manifest records, never the segment format, and a manifest with every
factor is read by every engine that reads one with the first.

#### 42. A restore can select a window start, and the scorecard signs it (PROD-11.1)

**Added.** A drill or restore plan may state an inclusive window START, as the
interval form of its point in time:
`restore.point_in_time: "<start>/<end>"`
([drill-spec.md](formats/drill-spec.md#a-window-start-restorepoint_in_time-startend-prod-111)).
A single instant is what it was. Every partition of every topic the plan names
is restored from the start; a start before the archive's coverage (never moved
to it), a start at or after the end and a window no archived segment overlaps
are refused (exit 3) before any target topic is created. Phases 4 and 7 judge
the window only. The restore preflight previews it through the same function
execution uses (`WindowStartBeforeCoverage`, `SelectionEmpty`,
`SelectionInvalid`). The scorecard is format **1.7.0** and carries
`source.selection {window_start_ms, window_end_ms}`; its existing fields
(`sample.window_start`, `sample.coverage_note`, a complete block's window) name
the start too
([stability.md](stability.md#scorecard-format-170-sourceselection-a-restore-from-a-stated-window-start-prod-111)).
`verify_scorecard.py` 1.23.0 and `logweir drill verify` check it (arms SEL-1
to SEL-3), print a `replay selection:` line, and qualify a sampled `pass` by
its window. Each says no record before the start was restored only over a
complete verification that passed; for a sampled document it says the
sampled check does not prove it.
**Refused:** `restore.partitions` (a partition subset), by name,
`PartitionSubsetsAwaitOwnerDecision`, until the owner decides how a
subset-narrowed scorecard is versioned (OD-9; decided and implemented by
item 51); and a plan stating a start under a standing rehearsal
authorization, which restores every partition from the floor.
**Do:** nothing for a plan without a start. A runner older than this release
refuses a plan with a start (`drill spec does not parse`, exit 1) before it
touches anything, so roll the runner forward before submitting one. The
`Restore` CRD and the console are unchanged (the console's selection is
PROD-11.1a); a `Restore` carries the plan bytes as they are.
**Scope:** compose rows (slot 2 with `COMPOSE_PROFILES=auth`, Kafka 3.7.1,
engine `0.23.3+logweir.1`) with an oracle of their own: inclusive vs exclusive
at the start with equal and non-monotonic timestamps; a segment whose last
record is before the start hides an in-window record, which complete coverage
fails (the engine's limit, PROD-01.1b); a topic subset from a start under both
coverages, read by both verifiers; the refusals (a partition subset among
them) with a control; a compaction hole inside a sub-window; a newer backup
set arriving after approval; a partition whose records all precede the start,
signed `preflight-failed` naming it (phase 5's existing rule for a partition
with nothing in the window), never `pass`; and main's runner from before this release
refusing a plan with a start, with the same plan without it restoring.
**Rollback:** an older runner refuses plans with a start (above) and ignores a
`restore.partitions` key (which no Logweir writer emits). 1.7.0 scorecards
already written stay valid under the older readers, which ignore the block;
the document's `sample.window_start` and `sample.coverage_note` still name the
start.

#### 43. A `Restore`, a rehearsal and the console can ask for complete coverage, and every surface says sampled or complete (PROD-08.1a)

**Added.** Item 30's complete verification can now be requested and read
outside a plan file. A `Restore` declares `spec.coverage: complete` (and
`spec.completeMaxRecords`) beside a plan that says the same — the controller
refuses, `ExecutionSpecInvalid` before any approval is waited for, a
declaration the plan bytes do not say, in either direction. A
`RehearsalSchedule` asks with `spec.bounds.coverage`/`completeMaxRecords`
(inside `templateDigest`; each slot's plan then states no `max_partitions`).
The product API's create route carries both fields; every restore read carries
`coverage {requested, recorded, covered, incompleteReason}`; the operation
view's `verificationScope` adds `coverage`, a complete block with every
partition's exact counts, and FX-23's `unsampledTopics`; the rehearsal view's
`bounds` carry the coverage. The controller copies the signed coverage, the
complete block and the unsampled topics onto `Restore.status.integrity`, and a
`COVERAGE` printer column reads it. The console's restore wizard offers complete
coverage as a closed advanced choice with its cost stated beside it, never as
the default; the History list, the Restore detail and the operation view say
sampled or complete and show a complete run's per-partition counts. The
runner's notification body adds `integrity.coverage`/`covered` and its metrics
`logweir_drill_integrity_coverage` and `logweir_drill_integrity_complete_covered`
([kubernetes.md](kubernetes.md) §12 *Complete coverage* and §7g,
[api.md](api.md#the-restores-coverage-prod-081a), [metrics.md](metrics.md)).

**`covered: false` is never a pass, anywhere.** The `Restore` badge's
`Verified` condition is `CompleteNotCovered` beside it: for a real one
(`fail-integrity`, exit 2), because the rule reads `covered` before the
outcome, and even over a status whose other fields say pass. The API's
`verifiedSuccess`, the console's badge and list verdict and a rehearsal's
`lastSucceeded` all refuse it; the notification and the metrics label carry
`fail-integrity`, and `logweir_drill_integrity_complete_covered` is 0.

**The standing authorization signs the coverage (format 1.1.0).** A rehearsal
scope gains optional `coverage` and `completeMaxRecords`; the plan's coverage
must EQUAL the signed one (absent = sampled), and a signed record bound must be
met. A scope that authorises complete coverage signs `maxPartitions: 0`, and
every reader of this build refuses one with any other value: a runner or
controller older than 1.1.0 ignores `coverage`, and under a partition bound of
0 it runs nothing (its `Approval` controller refuses the scope, its
plan-in-scope check every real plan). `logweir drill approve --standing` mints
1.1.0 only when the scope carries one of the new fields. A scope signed before
this field authorises sampled rehearsals only, so no existing authorization
admits a complete plan
([stability.md](stability.md#the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature)).

**Do:** apply the CRDs (both new fields are additive and absent means sampled,
so every existing `Restore`, `RehearsalSchedule`, template digest and plan hash
is unchanged). To rehearse with complete coverage, create a NEW
`RehearsalSchedule` with `spec.bounds.coverage: complete`, write `"coverage":
"complete"` and `"maxPartitions": 0` in its `scope.json` (the minter refuses
any other `maxPartitions` beside `complete`), and sign a new authorization for
its `status.templateDigest`. Budget for the cost: complete coverage reads every
archived record of the restored topics and the whole restored output — about a
minute per GiB of one-KiB records with an optimised build on a laptop, against
about five seconds for the sampled check. A plan written with the CLI that asks
for complete coverage needs `spec.coverage: complete` on its `Restore`, or the
controller refuses it.
**Scope:** unit and mock-cluster rows in `logweir-core` (the scope predicate
and the 1.1.0 admission, each arm with a control), `weirkeeper`
(`restore_controller.rs`: the declaration refused both ways before the
approval is read, admitted and reaching the plan ConfigMap byte for byte, the
signed block copied all-or-nothing, `CompleteNotCovered`;
`rehearsal_controller.rs`: the coverage inside `templateDigest`, a complete
schedule firing only under a scope that signed complete, a sampled schedule's
plan unchanged, a `covered: false` slot never a pass), `logweir-api`
(`complete_coverage.rs`, over the console fixtures it writes), the runner
(the standing document minted at 1.1.0 and admitted through the real binary,
a complete scope minted and read only with `maxPartitions: 0`, a `covered:
false` run never a pass in the notification or the metrics), the `Approval`
controller (the zero bound only beside complete), and
`ui/tests/complete-coverage.spec.js`; planted mutants, each killed. An older
build's runner binary and `Approval` controller (`main` before this item) were
run against a complete-only scope this build minted, and ran nothing under it
(`execution_contract_v2.rs`, an `#[ignore]`d row taking `LOGWEIR_OLDER_RUNNER`). Live: a
plan built by the console's own emitter, run with complete coverage on the
compose stack over real records and through the controller's and the API's
projections. The k8s rows (the CRDs, a `Restore` and a rehearsal through the
real controller, the console in a browser) run at the next PoC upgrade.
**Rollback:** an older controller ignores the spec fields (an older CRD prunes
them) and runs the plan as written; it does not copy the status fields, and
the console then reads "not recorded". An older runner or controller ignores a
1.1.0 scope's fields and reads a complete-only scope as a sampled scope with a
partition bound of 0: its `Approval` controller refuses it, and its
plan-in-scope check refuses every real plan, so nothing runs under a
complete-only authorization after a rollback (fail closed). An older minter
refuses a `scope.json` with `maxPartitions: 0`
([stability.md](stability.md#the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature)).

#### 44. A client that stops reading an answer, or stops sending a body, meets the console's stall deadline (FX-24b)

**Changed.** After FX-24 the console (`logweir-api`) timed out a connection
that never finished its request head, and nothing after the head: a client
that sent requests and stopped reading the answers left the console waiting
on a write the kernel would not take, and a signed-in client that sent a head
and stopped sending its body left a handler waiting on the body, each with no
deadline. 256 such clients, the console's connection ceiling, held every slot,
and the console answered nobody: measured on the built binary, a real request
behind them was unanswered at 30 s. Now a connection may wait on its client
for **thirty seconds with no progress** (`IO_STALL_TIMEOUT`): an answer's
pending write, or a request body's pending read, that moves no byte for that
long fails, and the connection and its slot are released. It is a stall
deadline, not a total: it restarts on every byte, so a slow but steady reader
keeps its connection, and an operation event stream that is being read is
never cut by it (between heartbeats it has nothing to write); a stream whose
client stops reading is held to its own 300-second ceiling instead, up to
310 s with the idle deadline after it. A JSON
mutation body must also arrive whole within **sixty seconds**
(`JSON_BODY_DEADLINE`), so one that trickles a byte at a time is ended too.
A body that stops, or misses the total, is answered `400 malformed_request`
("The request body stopped arriving before it was complete." — item 53 adds
"or arrived too slowly" — or "The request body was not received within 60
seconds.") and the connection is closed after the answer. This item said these
bounds end abandoned and stalled clients but not a client that reads, or sends,
one byte every thirty seconds; item 53 re-measured that: the stall ends such a
reader at 35.1 s, a body sent that way is now ended at thirty seconds, and what
remains — a client that reads fast enough to keep the kernel taking its answer
— is held to one peer's share of the connections
([api.md](api.md#conventions)).
**Do:** nothing is required. A client that pauses mid-transfer for more than
thirty seconds, or sends a mutation body over more than sixty, sees its
connection closed and retries; the console's own browser client does neither.
In shared mode on a cluster whose network plugin enforces NetworkPolicy,
consider `api.console.networkPolicy.enabled: true`, with the ingress
controller's selectors and the identity provider's egress (`oidcCIDRs` or
`oidcPeers`; [chart README](../charts/logweir/README.md)), so that only the
ingress controller can reach the API pod: beside item 53's per-peer share, it
is the bound on slow-rate clients from many addresses.
**Scope:** rows on the built binary (`crates/logweir-api/tests/local_admin.rs`):
256 clients that pipeline requests for a 156 KiB asset and read nothing hold
every connection slot, then a request queued behind them is answered no
sooner than thirty seconds after the first and within forty-five of the last,
every one of them ended by the server before its answers were all sent, and a
single such client on its own is ended the same way; a
reader taking 8 KiB every 150 ms receives fourteen answers over more than
thirty-five seconds, uncut; the operation event stream of a running backup,
served by a fake API server, is still open with a heartbeat after the
deadline; a signed-in body that stops is answered and closed at thirty seconds
(not at the sixty-second total) while one sent three seconds late is read
normally; one that trickles a byte every four seconds is answered and closed at
sixty; the deadlines are pinned to their documented values at their call
sites. Unit rows over the two guards in `src/transport.rs` on loopback
sockets and stub IO. The connection's own reads are deliberately not timed:
the server keeps one pending for the whole of every answer to notice a client
leaving, and a timer there would cut every event stream at thirty seconds (a
mutant shows it). Live, the built binary on the host in localAdmin mode: a
request behind 256 non-reading clients answered 35.1 s after the first of them
connected, every one of them ended by the server (before: unanswered at 48 s,
none ended), a stopped body answered and closed at 30.0 s (still open at 75 s
before), a trickling one at 60.0 s (nothing at 90 s before), while a steady
reader took 42 s and an event stream heartbeated past 48 s on both. The controller's in-pod health listener already bounds the
whole exchange, its write included, at two seconds. The shared-mode console's
probe runs at the PoC upgrade that carries this item.
**Rollback:** an older console leaves a connection whose client stopped
reading or sending open again; nothing is stored, so nothing needs converting.

#### 45. A controller no longer rewrites a status whose content has not changed (FX-29)

**Changed.** A `BackupSchedule` whose latest slot was skipped (`Missed`) right
after a slot that ran rewrote its status on every reconcile, and every write
woke the next reconcile: on the PoC both schedules wrote about 120 times a
second each from 2026-10-09 02:00Z, with 323 MB of controller log in 67 minutes
and API-server `429`s on the shared cluster. The cause was a merge patch: the
skipped slot's record names no `Backup`, an absent key in a merge patch leaves
the stored one, so the previous slot's `status.lastSlot.backupRef` stayed, and
the controller read that leftover as a new decision on every pass and stamped
`lastSlot.decidedAt` and `policy.evaluatedAt` with the clock. Now:

- every status block the schedule controller writes replaces the stored one,
  so a field that became absent is removed, and a "when" field
  (`lastSlot.decidedAt`, `retentionReport.evaluatedAt`) is kept unless the
  block it times changed — compared as the API server will store it;
- the schedule controller is woken by a new schedule or a spec change, and
  otherwise by its own requeue (at most 30 s); its own status writes no longer
  wake it.

The sweep of every controller that writes status found the same loop in three
more: a `BackupDestination` whose CA `ConfigMap` went away (`observedAt`
restamped beside a stale `caBundleSha256`), a `ProtectionPolicy` whose
notification routes were removed or whose newest point lost an optional field
(`evaluatedAt` restamped beside the stale field), and every `RetentionPolicy`
in `Report` or `Enforce` mode whose catalog resolved
(`lastEvaluation.at` was the clock of every evaluation). Each now writes once
and settles. `RetentionPolicy` `status.lastEvaluation.at` and `BackupSchedule`
`status.retentionReport.evaluatedAt` mean "when these findings were first
reached", and their CRD descriptions say so ([kubernetes.md](kubernetes.md)
§9 and §7f).
**Do:** nothing is required. If a schedule was suspended to stop the loop,
resume it after the controller rolls. The first reconcile after the upgrade
writes each affected object once — a schedule's stale `backupRef` is removed
and its `decidedAt` and `policy.evaluatedAt` move to that instant; a
destination's stale `caBundleSha256` is removed.
**Scope:** controller rows over `testing::ObjectStore`, which applies each
`/status` merge patch as RFC 7386 says, enforces the `resourceVersion`
precondition and counts writes (`crates/weirkeeper/tests/schedule_status_churn.rs`):
the PoC schedule object itself, reconciled forty times in 0.4 s, is written
once (forty times before); a fresh `Admitted` then `Missed` sequence is written
once over twenty passes (twenty before) and its record names no `Backup`; a new
slot still fires, writes and decides; a `suspend` edit still writes once; the
watch filter triggers on a new object or a new spec revision and on none of four
status writes. The class-sweep rows: a destination whose CA went away, twenty
passes, one write (twenty before); a protection policy whose routes were
removed settles after one write; a retention policy writes nothing once settled
(every pass before). The requeue that is now each schedule's only clock is
pinned too: every decision returns a timed requeue of at most 30 s, never
`await_change()`, and a schedule driven only by that requeue, with no watch
event, fires its next slot within one poll; a destination re-reads a rotated
CA, a protection policy turns `Stale` as its point ages, and a retention
policy starts its enforcement slot, each on the pass its own timed requeue
runs. Each guard has a mutant that fails a row. Not proven live in this
branch: the PoC upgrade that carries it resumes the two schedules outside
02:00–03:00Z, expects each `resourceVersion` to stand for five minutes while
no slot is due (one hourly history-inventory write, moving only
`history.inventoriedAt` and `policy.evaluatedAt`, is allowed), and then
watches the next slot fire and be decided once.
**Rollback:** an older controller restores the old behaviour the next time a
schedule's slot is skipped after one that ran, or a destination loses its CA;
suspending the schedule, or restoring the `ConfigMap`, stops it. Nothing stored
needs converting: the objects this build rewrote read the same to an older one.

#### 46. Topics whose records need a schema registry are flagged from the archived bytes: "registry not captured" (PROD-03.0)

**Changed.** A record a Confluent serializer wrote is a zero byte, a 4-byte
schema id and a payload only that schema decodes, and Logweir captures no
schema registry, so a restore could succeed while no application could read
what it restored, and nothing said so. Now every backup judges, per topic,
whether its archived keys or values carry that framing — from the segments it
just wrote, with Logweir's own decoder; **no registry is contacted** — and
records it in the signed receipt (format **1.5.0**, `schema_dependency`: a
verdict, the basis, each side's framed share and the schema ids seen, the 16
smallest and a count). A side is dependent when at least one in ten of its
non-null records is framed (magic byte 0, an id from 1 to 2^24 − 1, a payload
after it); nulls and tombstones never count. The catalog point copies it
(format 1.5.0), the catalog's view and the product API publish it per topic
(`PointView.topics[].schemaDependency`), and the console says it where a
restore is reviewed, on a catalog point's recovery-point step and on the
catalog page: **"Registry not captured: applications may not read these
records after restore."**, with each schema-dependent topic, its sides and its
ids. Nothing is blocked: the bytes are restored unchanged, and what an
application needs is the registry that issued those ids. A receipt before
1.5.0, or a topic the backup could not judge, reads **not assessed**, never "no
registry needed". Detection is bounded and never fails a backup: it streams at
most two segments per partition for at most eight partitions per topic,
keeps six bytes per key and value, and stops at 16 MiB stored, an 8 MiB zstd
window or 256 MiB decompressed per segment and at 120 s per backup (a hard
stop) — those topics read `notAssessed` (`segmentTooLargeForDetection`,
`detectionTimeBudgetExceeded`, `segmentUnreadable`). It adds at most about
**17 MB** to the runner's memory, measured: a 16 MiB incompressible segment
held while scanned (+16.6 MB) is the worst case; a 1 GiB zstd bomb, a frame
declaring a 128 MiB window and a 250 MiB lz4 body each add 2.4 MB. Both verifiers read the block: `verify_scorecard.py`
**1.24.0** and `logweir drill verify` check its eight arms (22 to 29) and
print one `schema_dependency` line per topic. Stated limit: a binary key that
is a big-endian 64-bit integer from 2^24 to 2^56 (an epoch-millisecond
timestamp, a large database id) looks framed and is flagged with the "ids" its
bytes hold; schema ids in record headers, Apicurio's 8-byte ids and other
registries' framing are not detected ([the contract](formats/backup-receipt.md#schema_dependency--does-a-restore-need-a-schema-registry-format-150)).
**Do:** nothing is required. A `Backup` whose `spec.deadlineSeconds` is tight
should allow up to 120 s more for detection after the engine. To see a point's
flags, open the restore review or the catalog page, or verify its receipt with
either reader. Points recorded before this item read not assessed until a
backup by this runner records them.
**Scope:** unit rows over the detector (Avro, JSON Schema and Protobuf framing
in keys and values, unframed payloads, nulls and tombstones, short records and
random ids after a zero byte, the one-in-ten boundary, a mixed topic, an empty
topic, the 16-id cap); the sampler measured with a counting segment source;
the caps, the time budget, a panicking source and the head's early stop; a
child-process memory row (a ~190 MiB segment, a 1 GiB zstd bomb, a frame
declaring a 128 MiB window and a 250 MiB lz4 body each add 2.4 MB; a 16 MiB
incompressible segment +16.6 MB; decoding a segment whole adds 397 MB); a row
CI runs on its default stack (raw Confluent framing, the real engine and
store, the receipt's ids asserted); arms
22–29 with their exact text in both readers, the corpus and the parity gate
(which also derives each reader's lines from the document); the catalog's
cross-check and reconcile; one fixture read by the runner's, the API's and the
console's rows; and a live compose row: Avro, JSON Schema and Protobuf records
produced through Karapace's REST proxy, the registry STOPPED, then a backup
whose 1.5.0 receipt names exactly the four dependent topics with the
registry's own ids, the plain and zero-byte controls `notDetected` and the
empty topic `notAssessed`; both readers accept it and print the same lines;
the catalog point copies it; the console's own render over the point says the
sentence with the live ids. Mutants on the detector, the threshold, the
readers and the bounds are killed. Older readers (`verify_scorecard.py` 1.23.0
and earlier) accept a 1.5.0 receipt and print no schema line.
**Rollback:** an older runner writes 1.3.0/1.4.0 receipts and records with no
block (their topics read not assessed); the 1.5.0 documents already written
stay valid for every reader. An older controller or console ignores the field.

#### 47. A sign-in whose identity provider stalls is answered at the provider deadline (FX-28)

**Changed.** The shared-mode console's OIDC client put its ten-second provider
deadline (`PROVIDER_DEADLINE`) around the request up to the response head only,
and read the body after it with no timer. A provider, or a path to it, that
answered a head and then stopped sending held the sign-in open for as long as
the socket stayed open: `/auth/login`, which is unauthenticated, whenever its
hour-long discovery cache is stale, and `/auth/callback` at the token endpoint
or the key set. Each such request held one of the console's 256 connection
slots. Now one deadline covers the connection, the request, the head and the
whole body of every provider request (discovery, the key set and the token
exchange), and a document over 512 KiB is still refused, now by name. A
stalled request fails at ten seconds and the console drops its connection to
the provider: `/auth/login` answers `503 kubernetes_unavailable` ("The identity
provider could not be reached. Try again shortly."), the callback
`401 unauthenticated` ("The sign-in could not be completed."). The audit
failure is a new code, `provider_timeout`, and the console's warning says "did
not complete a request within the 10-second provider deadline"; the login
warning now carries that detail too, as the callback's and readiness's already
did. The deadline is per request: `/auth/login` makes at most one, and a
callback at most four, so a provider that answers each just inside the bound
can take a callback to forty seconds, while one that stalls ends it at once.
A dial (TCP connect and TLS handshake) also has a twelve-second bound of its
own, for a dial that outlives its sign-in. Provider connections now carry TCP
keepalive (30 s idle, then three probes ten seconds apart), so a pooled
connection to a provider that has gone silently is dropped after about a
minute of idleness; a sign-in inside that minute can still meet the deadline
once. What the deadline leaves: a failed discovery is not cached and
concurrent sign-ins do not share a fetch, so while a provider stalls, about 77
client addresses at the sign-in limit can keep every console connection busy
([api.md](api.md#sign-in)).
**Do:** nothing is required. Alert on `provider_timeout` beside
`provider_unreachable` and `code_exchange_failed`.
**Scope:** rows over a loopback HTTP provider that this repository's tests
bind, reached through the console's production client
(`crates/logweir-api/tests/oidc_provider_deadline.rs`): a discovery document,
a key set and a token response that each send a head and stall, a provider
that never sends a head, and a TLS handshake that is never answered are each
answered after 10.0 s with `provider_timeout` (or the client's deadline
error), and the provider sees the console hang up at 10.0 s; under the pre-fix
code each body-stall row is still pending at fifteen seconds. A provider that
trickles its head over seven seconds and then stalls the body is answered at
10.0 s, not seventeen, so the request and the body share one deadline. A
provider that takes six seconds per document still signs in (login 6.1 s,
callback 12.1 s). An oversized document is refused by name. On the built
binary in shared mode, a raw-socket `GET /auth/login` against a stalled
provider gets its `503` status line at 10.1 s and the server closes the
connection. Unit rows read keepalive back off a socket the client's own
connector dialled, and end a stalled dial at the connector's bound. Mutants,
all killed: the pre-fix timer, a timer around the body alone, one timer per
phase, the body uncapped, keepalive off in the connector or in the client's
wiring, the deadline doubled, halved or moved, the dial unbounded, and the
timeout reported under another name.
Live: the PoC upgrade that carries this item signs in through Dex; a stall
cannot be simulated on the live Dex.
**Rollback:** an older console reads a stalled provider's body with no
deadline again; nothing is stored, so nothing needs converting.
#### 48. A destination's *Test access* compares every grant's binding, and is never READY for a destination a backup would refuse (FX-20c)

**Changed.** A readiness check compared a credential's binding (item 38) only
where it opened a store, and a check never opens one for some grants: a
destination's `archiveWrite` (a check may not write into the archive prefix)
and a separate `evidenceWrite` Secret when no marker probe runs. So a
destination whose only grant named another destination's `archiveWrite`
Secret tested **ready** on the PoC (`destination.credentialProjected:
Projected`) while its backup was refused `CredentialBindingMismatch`. Now a
`Preflight` lists, in its check plan, every `SecretKeys` grant the run would
present — on *Test access* every grant the destination declares, whichever
roles are exercised; on a backup readiness check `archiveWrite` and
`evidenceWrite`; on a restore preflight the source's `archiveRead` and the
evidence destination's `evidenceWrite` — and projects each one's
`logweir-binding` key, and nothing else of it, beside its destination's
expected binding. The new blocking row **`destination.credentialBound`**
compares them in the check pod with no request: `ready`/`CredentialBound`, or
`notReady`/`CredentialBindingMismatch`, leading with each refused grant's
`spec.access` field, its Secret, and whether its binding was absent or
foreign, with one `<grant>=bound|CredentialBindingMismatch` fact per grant;
its remedy gives the destination its own Secret and never suggests binding the
refused one to it (every binding refusal's text now says the same). The controller
expects the row whenever a grant is listed, so a check that does not answer it
is `unknown`, never `ready`. No Secret value and no binding value reaches a
status, the API or the console, and nothing is dialled with a foreign
credential. The product API returns the row in the preflight's `checks` and
the destination's `lastTest` follows the verdict; the console's *Test access*
panel shows it among the blocking rows. A workload-identity grant carries no
binding and is not listed (FX-20b). The same sweep fixed one more surface: a
`RetentionPolicy` whose run was refused `CredentialBindingMismatch` read
`Enforced=True` again on the next evaluation pass (`UnattendedDeletionEnabled`
or `RunInProgress`), and its console panel said "enforced by Logweir"
throughout; the refusal now stands until a later run is harvested, with
`status.enforcement: RecommendationOnly` and `guarantees.ageExpiry:
NotEnforced`, and the panel prints the `Enforced=False` reason.
**Do:** roll the controller and the runner image together (the chart does):
an older runner refuses a plan that lists a grant (`phase: Failed`,
`CheckContractMismatch`, naming `grantBindings`). Re-run *Test access* on each
destination after the upgrade; a grant that now reads
`CredentialBindingMismatch` would have been refused by the next run that
presents it (a backup for `archiveWrite`, a restore or a verification for the
read grants) — bind its own Secret, or stop naming another destination's
([kubernetes.md](kubernetes.md) §20.10).
**Scope:** the plan contract's rows (`crates/logweir-core/tests/check_contract.rs`,
`crates/logweir-core/src/credential_binding.rs`); the runner's rows over each
kind with a map for the pod's environment — the F6 thief `notReady` by name
and its bound control `ready`, one foreign grant among bound ones named alone,
absent, foreign and lost-expectation refusals, a backup readiness check's
unprobed `evidenceWrite`, a restore preflight's two destinations, and no store
handle built for any of it (`crates/logweir/tests/check_grant_binding.rs`);
the controller's rendered plans and pods for every operation, a
`DestinationAccess` that exercises one grant and compares four while
projecting only one credential, and the verdict through `assemble` (`notReady`
with the row, `unknown` without it, `ready` only when it is ready)
(`crates/weirkeeper/tests/preflight_controller.rs`); the product API
(`crates/logweir-api/tests/destinations.rs`) and the console
(`ui/tests/credential-binding.spec.js`) over one fixture, which the runner's
and the controller's rows hold their output to; the retention hold over real
passes, with a generic refusal and a later successful run as its controls
(`crates/weirkeeper/tests/retention_policy_controller.rs`) and the console's
retention panel over the fields it writes (`ui/tests/credential-binding.spec.js`);
a thief's Test access through the controller's real reconcile, the plan and
Job it POSTs (`preflight_controller.rs`); and every binding refusal's text
(`crates/logweir-core/src/credential_binding.rs`). The live row — PoC batch 4's
F6 thief re-created, tested and deleted — is the next PoC upgrade's.
**Rollback:** an older controller lists no grant and an older runner emits no
binding row: *Test access* reverts to the overclaim this item fixes (and a
refused retention run's `Enforced` flips back to `True` on the next pass), and
every run still refuses a foreign Secret. Nothing is stored, so nothing needs
converting.

#### 49. A backup receipt records each topic's ID before and after the engine; a recreated topic is a new generation (PROD-01.4a)

**Added.** A topic deleted and created again under the same name is a new
topic: its offsets restart at zero and mean other records. Until now no
signed document could tell it from the same topic — the engine reads no topic
IDs, and a recreation refilled past the old end can look continuous. `logweir
backup run` now reads each named topic's ID (KIP-516) itself, through
DescribeTopics, as the last read before the engine and again the moment the
engine exits, and the receipt records both, per topic, in Kafka's own text
(the `TopicId` `kafka-topics.sh --describe` prints), or `null` with the reason:
`noTopicId` (a cluster below inter-broker protocol 2.8 has none),
`notAuthorized` (refused by name, never read as absent), `topicNotFound`,
`readFailed`, `notRead` or `reservedTopicId` (Kafka's reserved
`AAAAAAAAAAAAAAAAAAAAAQ`, a sentinel no topic is given). Receipt and catalog
point are format **1.6.0** (`generations`, `topics[].identity`); every receipt
this build signs is at least 1.6.0 (item 50's consumer selection makes it
1.7.0). Two points' IDs decide their generation: different
IDs are a new generation, never a continuation; equal IDs the same one only
when the later capture's read after the engine recorded the same ID too; a
topic whose ID changed during its own capture is flagged; and an unknown ID is
"not established", never "the same" (`docs/formats/backup-receipt.md`). Both
verifiers check five new arms (36–40), refuse Kafka's reserved IDs in a
receipt and in a catalog point's copy, and print one `generations` line per
topic; `verify_scorecard.py` is 1.25.0. DescribeTopics is the third call family in the one crate that may hold
`unsafe` code (item 37); every other crate still forbids it. No command,
console page or API field compares two points yet: PROD-02.1's coverage view
is the first consumer.
**Do:** nothing. The read needs `Describe` on each topic, which the engine's
own read already needs; a refused or failed read is recorded and never fails
the backup.
**Scope:**
- `crates/logweir-rdkafka-ffi/src/topics.rs`: unit rows for every input
  refusal, a bounded call with no broker, librdkafka's two immediate answers,
  and a 100,000-call soak (+0 KiB); Guard Malloc over the unit rows and
  macOS `leaks` over the live rows;
- `crates/logweir-kafka/src/topic_ids.rs` and
  `crates/logweir-core/src/topic_identity.rs`: the answer mapping (a transport
  failure is `Unreachable`, never "not found"), the canonical text from the
  ID's two halves, and the generation rule, each with unit rows and mutants;
- receipt arms 36–40 in both readers, twelve corpus cases, the parity gate;
- `e2e/tests/topic_ids.rs` on the compose stack, on Kafka 3.7.1, 3.9.2 and
  4.3.1: the product's ID equals the broker CLI's (IDs with `-` and `_`
  included); two real backups around a delete-and-recreate give a new
  generation for that topic and the same generation for the control topic;
  the `acl` profile's restricted principal is refused by name; and
  `e2e/tests/topic_identity.rs`'s oracle now requires the product's read to
  equal the CLI's on every one of its rows.
**Rollback:** an older runner writes 1.3.0, 1.4.0 or 1.5.0 receipts again,
with no IDs, and its points' generations read "not established" against newer
ones. The 1.6.0 receipts and records already written stay valid under every
major-1 reader; readers before 1.25.0 ignore the IDs.
#### 50. A backup can record the committed positions of the consumer groups it names, as signed evidence (PROD-04.1)

**Added.** A backup now records, for each consumer group it is asked about,
where that group would resume — read through Logweir's own client just before
the engine starts, and signed: the receipt's new `consumer_positions` block
(receipt and catalog point format **1.7.0**) carries each group's outcome and
position counts, and binds by digest a positions document put beside the
receipt (`<run_id>.consumer-positions.json`, format 1.0.0) that carries every
position. Name the groups in the plan (`source.consumer_groups`), on the
command line (`logweir backup run --consumer-group <id>`, repeatable) or on a
`Backup`/`BackupSchedule` (`spec.consumerGroups`): **at most 100 exact ids**,
each at most 255 bytes, and a summary at most 80 KiB as the receipt encodes it
(ids of `"` or `\` count double: 84 such 255-byte ids fit), anything else
refused by name before anything runs (`ConsumerGroupSelectionTooLarge`,
`ConsumerGroupIdInvalid`, `ConsumerGroupSelectedTwice`). Every selected group gets exactly one outcome:
`captured` — its type and state, whether it was active, and every partition of
every backed-up topic accounted for, each committed position judged against the
partition's marks and the archive (`withinArchive`, `atArchiveEnd`,
`beforeArchive`, `beyondArchive`, `beforeLogStart`, `noArchivedData`;
`PositionBeyondEnd` above the partition's end) — `excluded` with a reason
(`GroupTypeNotCaptured` for a share or streams group; `GroupNotFound`), or
`failed` with a reason (for example `NotVisibleToPrincipal` for a group the
backup principal may not describe, `PositionsUnstable` for a pending
transactional offset commit, `GroupVanishedDuringCapture` for a group deleted
while it was read). A partition with no committed offset is counted, never
offset 0. **The receipt's size depends on the selection, never on
partitions** — 9 KB for 10 groups over 20 topics of 12 partitions and 44 KB for
100 groups over 10 of 11 through the runner's own builder (52 KB and 66 KB live
on Kafka 4.3.1, most of it the topics' configuration model), where inline
positions would have been 482 KB and 1.9 MB (513 KB and 2.1 MB live), over the
catalog's 256 KiB read — so the catalog reads such a point `Available` and the
console offers it. Both readers check six new receipt arms
(30 to 35) and, given the positions document (`--consumer-positions <file>`),
fourteen more over it (CP-1 to CP-14), refusing a document changed after
signing; they print one `consumer_positions` line per group and, with the
document, one per position. `verify_scorecard.py` is 1.26.0. The catalog point
binds the block by its digest, and the catalog's view and the product API
(`PointView.consumerPositions`) show the snapshot's freshness and, per group,
its counts
([backup-receipt.md](formats/backup-receipt.md#consumer_positions--consumer-position-evidence-format-170),
[kubernetes.md](kubernetes.md#consumer-position-evidence-specconsumergroups),
[stability.md](stability.md#receipt-and-catalog-point-format-170-consumer_positions-prod-041)).

What it does not do: positions read while applications run are **not atomic**
with the records the engine reads (the receipt says when and which groups were
active); on Kafka 3.7.x, which types no group, every selected group is
`excluded: GroupTypeNotCaptured`; a topic recreated and refilled past its old
marks during the run is not detected by the position evidence itself (the same
receipt's `generations`, item 49, records each topic's ID before and after
the engine);
the product API and the console neither set nor show `spec.consumerGroups`
(set it with `kubectl`); nothing resets a group — applying positions is
PROD-04.2's reviewed cutover. With the source gone, the positions are read from
the evidence store with a reader from 1.26.0 on
([the recovery path](formats/backup-receipt.md#recovering-positions-with-the-source-offline)):
an older reader says `VALID` over a 1.7.0 receipt and checks nothing in the
block. An engine consumer-group snapshot beside a foreign archive is only an
import source, every group typed `unknown`.

**Do:** nothing for existing plans and objects: a backup that selects no group
writes the receipt it wrote before, and no positions document, and its plan and
run-policy digest are unchanged. Apply the CRDs before setting
`spec.consumerGroups`, and give the backup principal Describe on each selected
group (and on the cluster, for a complete listing). **Scope:** the core rules,
the bound and the arms (`crates/logweir-core/src/consumer_positions.rs`,
`crates/logweir-core/tests/backup_receipt.rs`, one row per arm), the builder
(`crates/logweir/src/backup/consumer_positions.rs`), the seam
(`crates/logweir/tests/consumer_positions_seam.rs`), the corpus
(`scripts/fixtures/consumer_positions_corpus.py`) and the parity gates over
both readers, the review's two sizes through the runner's own builder and the
catalog (`crates/logweir/tests/check_cli.rs`), the catalog, view and API rows
(`crates/logweir/tests/catalog.rs`, `crates/weirkeeper/tests/catalog_controller.rs`,
`crates/logweir-api/tests/d3_reads.rs`), the console rows
(`ui/tests/restore-catalog.spec.js`), the controller rows
(`crates/weirkeeper/tests/backup_controller.rs`,
`crates/weirkeeper/tests/schedule_controller.rs`), and live on the compose stack
through the shipped binary, `e2e/tests/position_evidence.rs`: one outcome per
group of each type on 4.3.1 and the 3.7.1 rule on the default line, a group
hidden from the backup principal, a rebalance during the capture, a position
beyond the end, expired records and deleted offsets, a partition added during
the capture, the capture read after the source topic and group are gone, and
the review's two sizes, each verified by both readers with its positions
document and refused with one byte of it changed.
**Rollback:** an older runner ignores `source.consumer_groups`, refuses
`--consumer-group`, and records no positions; the 1.7.0 receipts, positions
documents and records already written stay valid under every major-1 reader. An
older controller refuses a run whose frozen inputs carry `consumerGroups`
(`PlanConfigMapConflict`): let those runs finish, or remove
`spec.consumerGroups` from the schedule, first.

#### 51. A restore can select a partition subset, and its scorecard is format 2.0.0 (PROD-11.1b)

**Added.** A drill or restore plan may name per-topic partition subsets,
`restore.partitions: {orders: [0, 2], payments: [1]}`, written beside the
interval form of `restore.point_in_time`: `"<start>/<end>"`, or `"../<end>"`
for a window from the archive's floor
([drill-spec.md](formats/drill-spec.md#a-partition-subset-restorepartitions-prod-111b)).
A named topic restores only the listed partitions; the others restore whole.
Topics with different subsets restore in different engine runs (the engine's
filter applies to every topic of a run), which phase 5 checks against the
approved plan. Phases 4 and 7 judge the selection only: every other partition
of a narrowed topic must be empty on the target, and a record there fails the
run under both coverages. A subset the archive cannot satisfy (a topic the
plan does not select, an empty, repeated or negative partition, a partition
the archive does not list) is refused, exit 3, before anything is created; a
selected partition with no record in the window is signed `preflight-failed`.
The restore preflight previews it through the same function
(`PartitionNotInBackupSet`, `SelectionInvalid`).
**The scorecard is format 2.0.0** — the format's first MAJOR, by the owner's
decision OD-9 (a) of 2026-10-09 — written ONLY for a restore that states a
subset: `source.selection` names the subsets and the engine runs, and a
complete block's `partitions[]` and the sampled lane's fields name the
selected partitions
([stability.md](stability.md#scorecard-format-200-a-partition-subset-restore-prod-111b-the-first-major)).
Every other scorecard is the 1.x document it was. `verify_scorecard.py`
1.27.0 and `logweir drill verify` read it (arms PS-1 to PS-5) and print the
subset; every older verifier refuses it instead of reading it as a full
restore. Schema: `schemas/logweir-drill-scorecard-2.0.0.json`; 1.7.0 is
frozen beside it.
**Refused, still:** a selection under a standing rehearsal authorization; a
subset beside a plain instant, which does not parse.
**Do:** upgrade every verifier that will read a subset restore's scorecard
(older ones refuse it), and roll the runner forward before submitting a
subset plan: an older runner refuses one (`drill spec does not parse`, or
`PartitionSubsetsAwaitOwnerDecision`) before it touches anything. Nothing
for a plan without a subset.
**Every surface says partial.** The controller copies the signed
`source.selection` to the `Restore` status' `integrity.selection` —
`scope: partial`, the window's ends, how many topics were narrowed and, up to
256 topics and 1024 partitions in one, each topic's selected partitions, and
the engine runs — shown by a new `SELECTION` printer column appended after
`AGE` ([kubernetes.md](kubernetes.md)); the CRD's schema change is additive.
The product API serves it as `selection` on both restore reads and the
operation view's `verificationScope` ([api.md](api.md)); the console's History
list, detail and operation view say `partial: partitions 0, 2 of topic
orders`, and a covered complete check reads "every record of every SELECTED
partition"; the runner's notification body carries `selection` (`scope:
"partial"`) and `format_version`, and its PagerDuty title appends `(partial:
…)`. A restore without a selection shows none of it, exactly as before.
`logweir drill approve` refuses to mint an approval over a `Restore` plan that
states `restore.partitions` and does not parse (`SubsetPlanUnparseable`, exit
1, nothing signed). Choosing a subset in the console is PROD-11.1a.
**Scope:** unit, phase, preview, reader, parity and corpus rows (a document
with one topic narrowed and one restored whole among them); controller, API,
console and notification rows, each beside an unnarrowed control, and an
approval row; compose rows
(the default stack with `COMPOSE_PROFILES=auth`, Kafka 3.7.1, engine
`0.23.3+logweir.2`) with an oracle of their own: two topics with different
subsets (two engine runs) from the floor under both coverages and from a
start, beside a third topic restored whole (three engine runs), every
unselected partition empty and signed 2.0.0, read alike by both readers; an engine that ignores the filter, failed under both coverages; a
selected partition with nothing in the window, `preflight-failed`; the
refusals; a runner from before PROD-11.1 and main's runner from before this
release refusing subset plans, with the unnarrowed and start-only documents
of the two builds the same shape; and every `verify_scorecard.py` from 1.16.0
to 1.24.0 and both older `logweir` readers refusing the signed 2.0.0
documents.
**Rollback:** an older runner refuses subset plans and an older reader
refuses 2.0.0 scorecards, so roll the verifiers back last; 2.0.0 scorecards
already written stay verifiable with this release's readers. Start-only and
unnarrowed documents are unchanged in both directions. An older CRD, API and
console do not carry `integrity.selection`, so after a rollback they show a
subset restore without its selection again: read the subset restores' signed
scorecards (2.0.0) with this release's verifiers.

#### 52. Every object-store read has a size cap; the controller reads evidence under the relay's 1 MiB (FX-31)

**Changed.** A read from an object store took the whole object, so the memory
it used was whatever the bucket held. In shared mode a namespace whose bucket
held a multi-gigabyte object at its receipt or sidecar key could OOM-kill the
controller that serves every namespace, and each restart read it again. The
check Job's evidence fetch read the whole object before it truncated. Now
every read names a cap ([kubernetes.md](kubernetes.md) §7b.4). It refuses on
the size the store reports before a body byte is read, then holds a running
cap over the stream, so a store that reports a small size and streams more is
cut off. Existence tests read no body: the sidecar's presence is a `HEAD`, and
the readiness probe and the backup set check GET with a 0-byte cap. The caps:

- The controller reads a receipt or scorecard under **1 MiB** and a sidecar
  under **64 KiB**. These are the evidence relay's own caps, so a document is
  verifiable through the controller's handle exactly when a relay can carry
  it.
- The controller's retention report reads a manifest under 64 MiB, folded as
  it streams, never as a tree.
- Runner, CLI and check Jobs read documents under 64 MiB, manifests under
  256 MiB and segments under 1 GiB. The catalog walk keeps its 256 KiB.
- The evidence-fetch Job relays nothing for an object over `maxBytes` (before,
  it relayed a prefix the controller refused anyway).
- Concurrent controller reads share ONE 128 MiB budget, a quarter of the
  chart's 512Mi limit. Each read reserves its worst case before it reads
  (a document 40 MiB with its parse, a manifest 64 MiB), and a read that does
  not fit waits. Eight concurrent retention evaluations of 60 MiB manifests add
  126 MB of peak memory with the budget, against 504 MB without it.

Over a cap, the controller writes `NotAttempted`, naming the key and the cap.
That verdict is final, never re-read on the schedule; a signing-time re-read
of such a document settles as `trust.signingTimeRead: overCap` (a CRD
description gains the value). The retention report
lists the set under `skipped`, and the CLI and runner fail operationally,
naming the cap. Never a crash, never a pass.

**Do:** nothing is required. Know the limit it makes visible. A 1.5.0 receipt
is about 3.4 KB per topic, so a run that selects more than about **250–300 topics**
(fewer with per-topic configuration overrides)
writes a receipt over 1 MiB. No path verifies such a receipt now, and the run
is not a recovery point. Before this item, the controller's own handle verified
it, and an evidence-fetch relay did not. This moves a verdict only to the safer
side (OD-7's third case), and no signed format changes.
**Scope:** store rows (`crates/logweir-store/tests/capped.rs`):
- the two fences, including a store whose meter shows no body byte was taken;
- the version read;
- `head`;
- the streaming manifest fold, which answers what the old `Value` walk answered
  over 39 bodies.

Controller rows (`crates/weirkeeper/tests/read_caps.rs`):
- an oversized receipt, sidecar, scorecard, signing-time re-read and manifest
  each name the cap, while the signed fixture verifies;
- in a child process, the five controller read paths over 512 MiB objects add
  0 B of peak RSS, while a read under a cap far too large adds hundreds of MiB. Over a 16 MiB
  manifest of tiny values, the streaming fold adds the bytes, while a
  `serde_json::Value` of them adds 37 times their size.

Check rows (`crates/logweir/tests/check_cli.rs`): the fetch, the probe, the
catalog walk and the restore preflight read under their caps.

The live row on compose slot 3 (MinIO) is recorded in
`claude/fx-31.result.md` §5. The PoC upgrade repeats it on the controller
image.
**Rollback:** an older build reads whole objects again; nothing is stored
differently. A final `NotAttempted` this build wrote for an oversized document
is treated by an older controller as it treats any final `NotAttempted`.

#### 53. One peer outside the trusted proxy holds at most 32 of the console's connections, and a request body must keep up 32 KiB a window (FX-24c) — required action

**Changed.** Item 44 said that a client reading, or sending, one byte every
thirty seconds keeps a console connection, so 256 of them could hold every
connection (the FX-24b review measured "still open at 100 s" for one byte
every twenty seconds). Re-measured on the built binary, that does not hold for
a reader: the stall deadline ends one that reads one byte, or 16 KiB, every
twenty seconds at 35.1 s, because the kernel stops taking an answer for a
client that takes almost nothing. The review's probe read a byte at a time out
of the half megabyte the kernel had buffered and never reached the
end-of-stream behind it; re-run against item 44's binary it still prints
"still open" while that server logs both connections ended at 35.1 s. What does
keep a connection is a client that reads fast enough to keep the kernel taking
its answer — one reading a steady 16 KiB/s kept its connection to the end of
82 s of pipelined answers — and a signed-in client that sends its request body
a byte at a time, until the sixty-second total. So:
- **One peer's share.** In shared mode, once `trustedProxyService` or
  `trustedProxyCidrs` names a proxy, any other peer — a pod dialling the
  console's pod IP, a kubelet probe, a `kubectl port-forward` — may hold at
  most **32** of the console's 256 connections at once
  (`MAX_CONNECTIONS_PER_PEER`). Its next connection is closed as soon as it is
  accepted, before anything is read, and the console logs `closed a connection
  at once` (at most once every ten seconds). Holding all 256 now takes eight
  addresses. The trusted proxy is never capped, because every browser behind
  the ingress arrives from its address; with no trusted proxy configured — the
  chart's default — nobody is capped, because the console cannot then tell its
  ingress from any other peer; localAdmin mode has no cap.
- **A body's floor.** A request body the console is reading must bring at
  least **32 KiB in every thirty-second window** while it is still arriving
  (`BODY_MIN_PROGRESS`). One that trickles a byte every twenty seconds is
  answered `400 malformed_request` at thirty seconds, not sixty, with the
  detail "The request body stopped arriving, or arrived too slowly, before it
  was complete."; a body that ends inside its window is never cut, however
  small. The `malformed_request` description in the OpenAPI document says so.
- **The answer keeps item 44's stall.** A 32 KiB window on answers was built and
  measured: the server sees a reader's progress only when the kernel lets it
  write again, in bursts the size of a send buffer, and that window cut a steady
  16 KiB/s reader at 32.5 s which the stall serves to the end.
- **The chart requires the trusted proxy of a console it publishes.** With
  `api.console.mode: shared` and `api.console.ingress.enabled: true`, the chart
  refuses to render unless `api.console.trustedProxyService` (the ingress
  controller's Service) or `api.console.trustedProxyCidrs` names the proxy, or
  the new `api.console.trustedProxy: none` opts out by name — "api.console.
  ingress.enabled in shared mode needs the trusted proxy named". `none` beside
  a named proxy, any other value, and the key in localAdmin mode are refused.
- **The start line says whether the cap is on.** `logweir-api started`
  carries `per_peer_cap` (32, or 0 when off), and a shared console with no
  trusted proxy warns at start that the cap is off.
**Do:** before `helm upgrade`, a shared console the chart publishes through
its Ingress names its trusted proxy — `api.console.trustedProxyService:
{namespace, name}` of the ingress controller's Service (it also makes
`/readyz` depend on reading that Service's EndpointSlices: [api.md](api.md),
*The ingress controller by its Service*) — or opts out with
`api.console.trustedProxy: none`; otherwise the upgrade's render fails, naming
the value (Required operator actions, below). A shared console without the
chart's Ingress is not refused, but set `trustedProxyService` there too:
without a trusted proxy the per-peer share is off. If another proxy (an L7
load balancer, a second ingress) carries many clients to the console's pods,
name it too, or those clients share 32 connections; behind a service-mesh
sidecar that re-originates every connection (Istio from `127.0.0.6`, Linkerd
from `127.0.0.1`) every client is one peer, so name the sidecar's address or
opt out. A wide `trustedProxyCidrs` range never caps anything inside it. An
enforcing NetworkPolicy (`api.console.networkPolicy.enabled`) remains the bound
on clients from many addresses ([api.md](api.md#conventions); [chart
README](../charts/logweir/README.md)).
**Scope:** rows on the built binary (`crates/logweir-api/tests/local_admin.rs`).
In shared mode: one peer outside the trusted set holds 32 connections and its
33rd is closed within two seconds, unanswered and logged, and a connection it
drops gives a place back before its other connections' own ten-second
deadline; a trusted proxy holds 40, and with no trusted proxy configured a
peer holds 40, neither refused; and, at CI scale, 256 clients of one address
of which exactly 32 are held and 224 closed (where the host can dial from
`127.0.0.2`, Linux, a second address is answered at once). In localAdmin mode:
a client reading one byte every twenty seconds is ended at the stall deadline
(drained after the bound, short of its answers), and 256 of them, at CI scale,
let a queued request through between 29 s and 45 s; a signed-in body sent a
byte every twenty seconds is answered "arrived too slowly" and closed at
thirty seconds; one that keeps 8 KiB every four seconds still runs to the
sixty-second total. The floor and the cap are pinned at their call sites.
Unit rows over the clock, the guards and the cap in `src/transport.rs`. The
256-socket rows ran at eight and forty clients on the worker's host (the flood
rule) and run at 256 in CI. Mutants, all killed: the body floor off (at the
call site and in the clock), the window at 300 s, the cap off (served, and
never reached), the trusted proxy capped, a cap with no trusted proxy, places
that never come back, the cap off by one, IPv4-mapped peers counted apart, a
window that outlives its operation, the 32 KiB floor on answers, a body floor
of 1 MiB, and an output clock that restarts on every poll. The review's fix
round added: a row that holds 32 connections each with its own
`X-Forwarded-For` (four claiming the trusted range) and requires the 33rd,
claiming the trusted proxy, to be closed unanswered (killing a cap re-keyed on
the forwarded client); 240 sequential refusals after the first, each closed at
once, before the place-back (killing a refusal that keeps its permit, which
leaves the 224th unaccepted); the start line's `per_peer_cap` and warning, read
by the cap rows; and the chart's refusal, opt-out, contradiction, localAdmin
and enum rows in `scripts/check-chart.sh`, the FX-10 values row, and a
`chart_lint` row over every values file that publishes a shared console (the
PoC profile names Traefik's Service), each with its mutant killed. Live, the built
binary on the host, before and after: in shared mode one peer outside the
trusted proxy kept 32 of 40 connections and its 41st request was reset at
once, with one warning logged (before: all 40 kept and the 41st answered); as
the trusted proxy it kept all 40 on both builds; a body sent a byte every
twenty seconds was answered at 30.0 s, "arrived too slowly" (before: at 60.0
s, "not received within 60 seconds"); a reader of one byte, or of 16 KiB,
every twenty seconds was ended at 35.1 s on both builds, and a steady 16 KiB/s
reader completed in 82.0 s on both. The PoC upgrade that carries this item
runs the per-peer probe against one console pod (it passed against this build
on the host and failed against the one before it).
**Rollback:** an older console caps no peer and reads a body a byte at a time
until its sixty-second total; nothing is stored, so nothing needs converting.
Remove `api.console.trustedProxy` from the values before rolling the chart back:
an older chart's schema refuses the key it does not know.

#### 54. A probe Job Kubernetes is collecting no longer clears `reachable`, and the probe's WARN lines stop (FX-19)

**Changed.** The `KafkaCluster` probe re-reads its probe Job on every
reconcile, and the TTL controller deletes a finished probe Job five minutes
after it finished with foreground propagation: the Job gains a
`metadata.deletionTimestamp`, its pod goes first, and the Job stays readable,
finished and pod-less, until the pod is gone. The controller read that Job as
a crashed probe, wrote `Reachable=Unknown` / `NoExitCode` and cleared
`status.reachable` until the next probe answered. PoC batch 2 measured about
17 s on one healthy connection, a window in which a `Restore` against it is
refused `ClusterNotReachable` (item 26). The races around it — the pod gone by
the log read, the Job gone by its TTL patch, and a status write preconditioned
on an older copy of the object than the API server held — logged about twelve
`KafkaCluster probe reconcile failed; requeueing` WARN lines per cadence for
twelve connections, about 3,000 a day. Now:

- a probe Job with a `deletionTimestamp` is not read at all and nothing is
  written; the next probe is created once it is gone;
- a finished probe Job that carries the controller's own marker — the
  annotation `logweir.dev/probe-verdict-recorded` = the Job's UID, sent in the
  same patch as its TTL and only after the status write that recorded its
  verdict — is not re-judged when its pod is gone. A TTL alone is never the
  marker: a Job that a mutating admission policy gave a TTL at creation is
  still judged, and its TTL is overwritten with the five-minute re-probe timer;
- `NotFound` on the pod log or the TTL patch and `Conflict` on a status write
  are debug lines and ordinary outcomes, not reconcile errors, and a verdict
  write that lost its precondition is never followed by its TTL; every other
  error, a `404` or `409` from another call included, is still a WARN;
- a crash, an unreadable probe log and a refused probe pod are logged at WARN
  once per Job, on the pass whose write first recorded them.

The last recorded verdict therefore stands until the next probe answers — **for
at most 630 s** (twice the 315 s re-probe interval, the console's freshness
budget). A probe Job stuck mid-deletion (a pod `Terminating` on a node that
went away, a foreign finalizer) or judged and never collected keeps its fixed
name, so no newer probe can run; once the reading is older than the bound,
`reachable` is cleared with the new reason `ProbeStale` (`Reachable=Unknown`,
`observedAt` and `clusterId` kept) and logged at WARN once, so a `Restore` or a
rehearsal is refused `ClusterNotReachable` rather than admitted on a reading
nobody repeated. The sweep of every other Job-owning controller (`Backup`,
`Restore`, the catalog sync, retention, a `ProtectionPolicy` delivery,
`Preflight` and `TopicDiscovery`; a `RehearsalSchedule` owns `Restore`s, not
Jobs) found none that reads a collected Job: each records a Job's verdict
before it gives the Job a TTL (the catalog sync's TTL, from creation, is at
least an hour) and stops at that record before it reads a pod
([kubernetes.md](kubernetes.md), *The crashed Job*).
**Do:** nothing is required. A `KafkaCluster probe reconcile failed` WARN line
now means a failure other than the collection of a probe Job; an alert that
ignored the line for its noise can use it again. A connection that reads
`ProbeStale` has a probe Job Kubernetes has not collected: look for a pod stuck
`Terminating` or a finalizer on `logweir-probe-<name>`.
**Scope:** controller rows over the fake API with the controller's log
captured (`crates/weirkeeper/tests/kafka_cluster_controller.rs`, `fx19_*`): a
probe Job being deleted, in four shapes (recorded with its pod gone or still
terminating, deleted before any verdict, still running), costs one Job read,
writes nothing and logs no WARN; a recorded verdict whose pod was collected
writes nothing; a crashed Job created with a foreign TTL is judged and gets
the TTL and the marker, a marker naming another UID is not this Job's, and a
reading under a foreign day-long TTL keeps the five-minute cadence; a
deletion stalled past the bound clears `reachable` (`ProbeStale`, one WARN
over three passes) while one inside it is deferred and silent; a judged Job
its TTL never collects is bounded the same way and is looked at again before
the bound; a reading older than the bound is not written as a verdict, and a
running probe clears a stale `reachable`; a `Restore` over the target the
probe left is admitted inside the bound and refused `ClusterNotReachable`
past it; the pod gone by the log read and the Job gone by its TTL patch are
outcomes with no WARN, and a `500` on the log read is still an error; a `409`
on a status write is an outcome on every write path, and a verdict's TTL is
not patched after it (and is after a `200`); an `AlreadyExists` on the probe
Job's `POST` is a reconcile error and a WARN; one whole probe cycle — the read
Job re-read, its deletion with the pod terminating and then gone, a stale read
of it, the next probe's creation, a `409` from a stale copy, the new probe
running and then read — keeps `reachable: true` after every pass and logs no
WARN. Negative controls: a real crash clears `reachable` and logs exactly one
WARN over three passes; a refused probe pod, an unreadable log and a crash
whose TTL patch failed log one WARN each over their passes. The class-sweep
rows (`backup_controller.rs`, `restore_controller.rs`,
`recovery_catalog_controller.rs`, `retention_policy_controller.rs`,
`protection_controller.rs`, `fx19_*`) hand each other controller a Job in its
being-collected shape and assert no write and no WARN, each with a control
showing that the read its gate prevents is reachable. Forty-two mutants of the controller change, all
killed: among them deletion read as a crash, a WARN on `NotFound` or
`Conflict`, a `404` or a `409` propagated as an error, clearing on an
already-recorded verdict, the marker read as "has a TTL", the staleness bound
removed at the deferrals, at a re-read and at a running probe, a requeue that
sleeps past it, a superseded write ignored on each of its ten paths, a WARN
on every pass at each of five sites, and `error_policy` demoting what it is
handed (three survived a first run and got their rows).
Not proven live in this branch: the PoC upgrade that carries it watches every
connection's `reachable` across fifteen minutes of probe cycles (it must never
leave `true` on a healthy connection), checks that new probe Jobs carry no
TTL before their verdict, records the longest any probe Job stays mid-deletion,
and counts the controller's `KafkaCluster` WARN lines (about zero, against
about twelve a cadence before).
**Rollback:** an older controller brings the flap and the WARN lines back and
drops the 630 s bound; the marker annotation it does not read is harmless,
and nothing stored needs converting.

#### 55. A sign-in state is single-use on each replica; a refused callback really clears the login cookie (FX-13a, FX-32)

**Changed.** The login cookie is sealed and stateless and opens for 600
seconds, and nothing recorded that its `state` had been used: anyone who kept
a copy could drive `/auth/callback` again and again, and each callback was a
token request to the provider authenticated as this console's client. Now,
once the cookie has opened and its `state` matched and **before the code is
exchanged**, the callback redeems the state in the console process's own
record, in memory. A replay on the same replica — later, with another code,
or racing the first — is refused `401 unauthenticated` ("This sign-in was
already used. Start again at /auth/login.") with audit failure
`login_state_replayed`, a warning, the login cookie cleared and **no token
request**. Nothing is written to Kubernetes and the console's RBAC is
unchanged. Across replicas the provider is the backstop: an authorization
code is single-use (RFC 6749 §4.1.2), so a replay cannot obtain a second
sign-in from a code the provider has exchanged. It costs one token request
per replica, and again after that replica restarts or evicts the entry: the
provider refuses it (`code_exchange_failed`), and that replica's record
refuses it from then on. A code the provider has not consumed is not covered:
when the first callback's exchange fails without the provider consuming the
code, the same cookie and URL still sign in on another replica, exactly as
before this change. The record holds 65,536 states per process; an entry is
forgotten once its login state could no longer open and the entries redeemed
before it have gone, or earlier when the record is full: then the oldest is
forgotten, never a sign-in refused (audit note `usedSignInStates: full`, one
warning a minute), and a replay of a forgotten state meets the same backstop.
Separately (FX-32), the problem
rendering kept only `Allow` from a handler's headers, so the callback's
`Set-Cookie` clearing `__Host-logweir_login` on a refusal never reached the
browser; every header now survives except those describing the replaced body
and its caching ([api.md](api.md#sign-in)).
**Do:** nothing is required. Alert on `login_state_replayed` (someone
driving callbacks with a copied cookie) beside `code_exchange_failed`.
**Scope:** rows through the whole router (`crates/logweir-api/tests/sign_in_state.rs`):
a replay on one replica — the same code, another code, and 599 s later — is
refused by name with no token request while the first callback signs in,
against a provider double that would have accepted the code twice; two
callbacks with one state at once on one replica, the provider taking 150 ms
per token request, give one sign-in and one token request; with the provider
double's codes single-use, a replay on a second console process reaches the
token request once, is refused by the provider with no session, and is then
refused by that process's record, and two callbacks at once on two processes
give exactly one sign-in; a record shrunk to two forgets its oldest entries
and still signs in every new login, announces it once, and a replay of a
forgotten state meets the provider's refusal. Each row also asserts the
callback made no Kubernetes write, and the replay and refused-exchange rows
find no `state`, authorization code, nonce or sealed cookie in the console's
logs. `tests/oidc_login.rs` drives seven
callback refusals and reads the clearing `Set-Cookie` on the response the
browser gets; unit rows hold what the rendering keeps and drops (the deny
list's fifteen names are written out and pinned), the record's expiry,
eviction and bound, and that of eight threads redeeming one state at once
exactly one is first, 300 times over. Mutants, all killed: the record not
consulted, the record written after the exchange, a check-then-write race, a
check and a mark under two lock acquisitions, a
full record that refuses, no eviction, eviction of the newest, no expiry, the
eviction announced every time or not reported, a record keyed on the code or
on nothing, one record shared by both processes, a replay warning that logs
the authorization code, a refusal without the
clear, the deny list losing `Cache-Control` or `Content-Length`, and the
rendering dropping `Set-Cookie` again, keeping a short list,
carrying the old body's headers or doubling its own.
[UNVERIFIED — no PoC sign-in has replayed a callback URL against the deployed console yet; the PoC upgrade that carries this item does.]
**Rollback:** an older console keeps no record (a copied cookie replays
within its 600 s again, each replay a token request) and
drops a refusal's `Set-Cookie` again; nothing is stored, so nothing needs
converting.

### Required operator actions after `v0.2.0-rc.1`

In addition to the next entry's six, in its order:

- **Before `helm upgrade`, name the trusted proxy of a shared console the
  chart publishes** (item 53): with `api.console.mode: shared` and
  `api.console.ingress.enabled: true`, set `api.console.trustedProxyService`
  to the ingress controller's Service (or `api.console.trustedProxyCidrs` to
  its pods' range), or `api.console.trustedProxy: none` to run without one
  deliberately. Otherwise the upgrade's render fails, naming the value.
- **Before the runner image rolls, read the plan preview of every `Enforce`
  `RetentionPolicy` with `requireApprovedPlan: false`**, or set it to `true`
  until you have read its first plan after the re-sync below: scheduled sets
  it never expired become candidates (item 31).

- **Back up the console key too, once it exists** (item 29):
  `logweir-console-confirmation` and `logweir-console-trust`, beside the
  installation identity the next entry's step 1 names
  ([install.md](install.md), *Back up and recover the installation identity*).
- **Roll the controller and the runner image together** onto the 0.23.3
  engine, as the next entry's step 6 already orders; a standalone CLI install
  replaces its engine binary and its `LOGWEIR_ENGINE_VERSION` /
  `LOGWEIR_ENGINE_DIGEST`, and every spec that names an `http://` archive
  endpoint says `allow_http: true` (item 28).
- **After the runner image rolls, set each `RecoveryCatalog`'s
  `spec.syncRequest` to a new value** so its view is published again by the
  new runner (item 31).
- **Bind each credentialed connection's Secret, one at a time, after an
  inventory** (item 33). Suspend the schedules that use such a connection
  before the roll; apply the CRDs and roll the controller, the runner and the
  console together; list which `KafkaCluster` names which Secret and stop on
  any Secret named twice (an incident, not a split); then, per connection, by a
  command that names both, copy its `status.credentialBinding` into the Secret
  it already named, once its owner confirms the endpoint
  ([kubernetes.md](kubernetes.md) §20.9); resume the schedules. Until a Secret
  is bound its runs are refused (`CredentialBindingMismatch`), closed. Never
  bind in a loop over every connection. A connection with no credential
  (`plaintext`, or TLS with no client certificate) needs nothing.
- **Remove `api.logweir.dev/request-sha256` from each `BackupDestination` an
  earlier build created with `secret.new`** (item 33), or rotate its key: that
  hash was taken over the secret access key, and the create's audit record
  keeps it, so only a rotation retires that copy.
- **Before the runner image rolls, name the point's set in every standalone
  point-bound plan that says `backup: latestCompleted`**, and approve it again
  (item 34).
- **Verify an image digest before you deploy it** with the pinned commands in
  [install.md](install.md#verify-the-images) (item 35); a standalone CLI
  install replaces its engine with Logweir's build and exports
  `LOGWEIR_ENGINE_VERSION=0.23.3+logweir.2` and its digest (build 2 since
  item 41).
- **Bind every destination, retention, notification and inline-archive
  credential Secret, one at a time, with `scripts/bind-credential.py`**
  (item 38), after suspending the schedules that use them and rolling the
  controller, the runner and the console together; never in a loop. The tool's
  refusal of a Secret another object also names is an incident to investigate
  ([install.md](install.md); [kubernetes.md](kubernetes.md) §20.10).

### Verification scope after `v0.2.0-rc.1`

- **Complete coverage can be asked for by a plan, a `Restore`, a rehearsal and
  the console** (items 30 and 41). A `Restore` or a rehearsal that does not ask
  still verifies a sample. The signed scorecard says which in
  `integrity.verification.coverage`; the `Restore`'s status, the product API
  (`Restore.coverage`, `verificationScope.coverage`) and the console repeat
  it. A protection event's `verification_scope` describes a policy's newest
  POINT and still never says `complete`. A complete verification that did not
  cover the restore (`covered: false`) is never a pass.

### Migration and rollback after `v0.2.0-rc.1`

An upgrade from `v0.2.0-rc.1` (publication `2c277dc1`) crosses items 28, 29, 30,
31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54 and 55, in the order of the next entry's upgrade path. Item 28 moves the engine in
the controller and runner images together; item 29 adds console and chart
values (`identity.bootstrapFeatures.consoleKey`, `approvalPolicy.default`) that
change nothing until set; items 30 and 31 change the runner (item 31 also the
console's text), and item 31 takes effect at each catalog's next sync; item 32
changes the runner's receipts and records, the catalog's view (runner and
controller), the product API and the console; item 33 changes the controller,
the runner, the console and the `KafkaCluster` CRD, and needs each credentialed
connection's Secret bound; item 34 changes the runner only; item 35 changes the
runner image (its engine and its platforms) and the controller's Job
environment together; item 36 changes the runner's sampled verification and
needs nothing; item 37 changes no behaviour any binary shows (it adds
library calls no command uses yet) and needs nothing; item 38 changes the
controller, every runner, the retention worker, the product API, the
console and the status of three CRDs, and needs each destination,
retention, notification and inline-archive credential Secret bound; item 39
changes the runner's phase 0 only and needs nothing; item 40 changes the
console only and needs nothing unless an ingress dials the console with
HTTP/2; item 41 changes the runner (phase 7 and the engine's build)
only; item 42 changes the runner, the restore preview and the
controller's standing-scope check, and needs nothing for a plan without a
window start (an older runner refuses a plan with one); item 43 changes the
`Restore` and `RehearsalSchedule` CRDs, the controller, the runner's
notification and metrics, the standing authorization's scope (format
1.1.0), the product API and the console, and needs nothing unless a
rehearsal is to verify every record (a new schedule and a new
authorization); item 44 changes the console only and needs nothing; item 45
changes the controller only (and two CRD descriptions) and needs nothing; item 46
changes the runner's receipts and catalog records, the catalog's view (runner
and controller), the product API and the console, and needs nothing; item 47
changes the console only and needs nothing; item 48
changes the controller and the runner together (the check plan and a check
row, and the retention controller's `Enforced` after a binding refusal) and
needs nothing beyond rolling them together; item 49 changes the runner's
receipts and catalog records and needs nothing; item 50 changes the runner's
receipts and records, the catalog's view, the product API and two CRDs (apply
the CRDs), and needs nothing until a plan or object selects consumer groups;
item 51 changes the runner, the restore preview, both verifiers, the
controller's `Restore` status (and the CRD's schema, additively), the product
API, the console and the runner's notification, and needs nothing for a plan
without a partition subset (an older runner refuses a subset plan, and an
older verifier a subset scorecard); item 52 changes the controller
(and two CRD descriptions), the runner and the check Jobs and needs nothing; item 53 changes the console and
the chart, and needs a shared console the chart publishes through its Ingress
to name its trusted proxy (or set `api.console.trustedProxy: none`) before
the upgrade renders; item 54 changes the controller
only and needs nothing; item 55 changes the console
only and needs nothing. To roll back to
`v0.2.0-rc.1`, in this order, on top of the next entry's rollback steps:

1. **Remove `approvalPolicy.default`** (item 29): an older binary refuses a
   document carrying `defaultMode` at start. Expect unbound namespaces of a
   fresh install to be `legacy-governed-v1` again under the older build.
2. Roll the controller and the runner back together (item 28): they run the
   0.21.0 engine again, and that build's `doctor` refuses 0.23.3. The runner
   signs format 1.3.0 again, sampled (item 30); the 1.4.0 scorecards already
   written stay valid under both readers. It writes 1.1.0 or 1.2.0 receipts
   again with no `topic_configuration` (item 32) and no `schema_dependency`
   (item 46); the 1.3.0 and 1.5.0 receipts and records already written stay
   valid.
3. Roll the console back with the controller and the runner (item 33): an older
   console names an existing Secret again, which this API refuses. The bound
   Secrets keep working with the older controller and runner, which ignore the
   binding pair; a connection using a new mode stops working (an older build
   cannot parse it), so delete or re-create those first. The 1.4.0 receipts
   and 1.5.0 scorecards already written stay valid for every reader from
   PROD-01.3 on, and older readers refuse them (the safe direction).
4. To roll back only the engine (item 35), use the one-release rollback image
   (`ENGINE_SOURCE=oso`, linux/amd64; [install.md](install.md#rolling-the-engine-back))
   rather than an older runner; it declares OSO's 0.23.3, so its documents say
   which engine ran.
5. Item 38 needs no rollback step of its own (nor does item 34, a runner
   change): an older controller and runner ignore the binding variables and
   the new status fields, and the bound Secrets keep working. Roll the console
   back with them (an older console offers `existing` on a create again,
   which only this API refuses).
6. Before rolling the controller back past item 50, let every run whose frozen
   inputs carry `consumerGroups` finish, or remove `spec.consumerGroups` from
   its schedule: an older controller refuses such a frozen plan
   (`PlanConfigMapConflict`). The 1.7.0 receipts, their positions documents
   and the records already written stay valid.

---

## Unreleased — `main` after `v0.1.5`

The last tag is `v0.1.5` (`9cc78a3`). This entry covers `main` through
`fdb48cd8` (2026-09-25): the platform tracker's shipped tasks, the operator
actions collected for PLAT-20.2 and after it, and the upgrade from the last
published image. Items 21 (FX-2), 22 (FX-5), 23 (FX-10), 24 (FX-3), 25
(FX-13), 26 (FX-11) and 27 (FX-8), from the product-expansion tracker's fix-now
rows, land after `fdb48cd8`, and so do FX-7's additions to item 11 (the
execution-claim set check, receipt and catalog format 1.2.0, the pin's read
by version id) and FX-4's format 1.1.0, which has no item of its own. No tag is cut at `fdb48cd8`, so the candidate
record below stays empty. The shipped task list, the six publications the PoC ran, the
tested environments and the results are in
[release-handoff.md](release-handoff.md).

### Candidate record

Fill every row for the exact candidate; a row left as `—` is an unrecorded
fact, not a pass. A previous run does not validate new bytes. A tag does not
rebuild the images — it gives the `sha-<commit>` publication `main` CI already
made the version tag, unchanged — so the release dry run's `release.json`
(workflow artifact `release-assets`) gives the publication commit and the four
image digests **before** the tag. Nothing else carries over: the tag run
packages the chart and builds the three CLI archives again. The chart package
is not byte-reproducible (`helm package` records each file's modification
time, which is its checkout time), and nothing shows the archives to be. The
chart and archive rows therefore come from the **tag run's** `release.json`,
the GitHub Release asset, after the tag, together with the run rows
([the release checklist](tag1-checklist.md), *Cutting a release candidate*).

| What | Value |
|---|---|
| Candidate commit (the tagged commit) | `56a60ebe509f2449c649fc1899756100d961832f` (the pre-tag record; it differs from the publication commit only under `docs/`) |
| Version tag | `v0.2.0-rc.1` (pushed 2026-10-08 with the owner's standing approval of 2026-10-07) |
| Publication commit (`release.json` `.images.publication`: the `sha-<commit>` images and chart the tag promotes) | `2c277dc11521c337748fbf9059cbaff76c36e82c` |
| CI run (`ci.yml`) for the publication commit, its `publish` job green | [37725467323](https://github.com/VladyslavHaina/logweir/actions/runs/37725467323): `check`, `e2e` and `publish` (build amd64, build arm64, promote) green |
| Release dry run (`release.yml` dispatched on the candidate) | [37730468445](https://github.com/VladyslavHaina/logweir/actions/runs/37730468445) on `2c277dc1`, every job green; `release.sh verify` on its `release-assets`: 14 assets verified, `SHA256SUMS` 14/14 |
| Release run (`release.yml` on the tag) and release drill | [37733173995](https://github.com/VladyslavHaina/logweir/actions/runs/37733173995): every job green, including `drill / drill-from-artifact` (job 113167927725), `publish-images` and `github-release`; the GitHub Release is a pre-release |
| Runner image digest (`linux/amd64`) | `docker.io/vladyslavhaina/logweir@sha256:affa8075492614ff0964bf29ca79a0caa61aa2f8aa47d022460f855e92024054` |
| Controller image digest (manifest list; amd64 and arm64) | `docker.io/vladyslavhaina/weirkeeper@sha256:fac312ba31df831e4cf96dd4546266ee47766528752f97a931eb125974dd56c6` |
| Console image digest (`logweir-console`) | `docker.io/vladyslavhaina/logweir-console@sha256:b7402cb143aee14356707ee10fb644e5f5c25a4993ff285315adfccf5b13b0a7` |
| UI image digest (`logweir-ui`) | `docker.io/vladyslavhaina/logweir-ui@sha256:2a43b5c0a7ec42688076986c96f63d34449060981fc746a9c8e4ac397e305daf` |
| Chart (`logweir-chart` version and package sha256 from the tag run's `release.json` `.chart`; OCI digest from the release's notes, as an anonymous `helm pull` reports it) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart` `0.2.0-rc.1`, package sha256 `515be23262193931c1c4028654c24a2e2b72a96f9473232724e2cd12e5ce561b`, OCI digest `sha256:d10e9772159da2934fd27e86daa822623e82366021acbbaa09fc21871ca6b727`; an anonymous `helm pull` is byte-identical to the release asset |
| CLI archives (three; the tag run's `release.json` `.archives`, each with its sha256 and run-time needs) | `logweir-x86_64-unknown-linux-gnu.tar.xz` `6b45d0787a0cae4edd952c50b2218eeb5730148a498a7323bd799ee18d3ce02d` (glibc 2.34+; libssl3, libsasl2-2, zlib1g); `logweir-aarch64-unknown-linux-gnu.tar.xz` `32e07061a67f1b29554d5a336118fcf3bfee97f0d635af9a0de8fbf273e70839` (the same); `logweir-aarch64-apple-darwin.tar.xz` `a2ddd05df432c0b7fcb09f749ee8f4b75297dcc802a6679fb0741c9990792a66` (macOS 11+, Homebrew `openssl@3`) |
| `ui/` bundle, file by file | the output of the command below, which the release asset `ui-files.sha256` also carries |
| Kubernetes exercises run on this candidate (context, auth mode, storage, limits) | — |
| Checks deliberately deferred, each with its reason | — |

**The `ui/` bundle is listed by digest** because the page runs with the
viewer's authority (README, *Threat model*): the bytes a browser executed must
be comparable with the bytes that were released. List exactly the files the
images ship — the selection `scripts/check-image-api.sh` check 1 pins at
twenty-six:

```bash
find ui -type f ! -name '*.md' ! -path 'ui/tests/*' | LC_ALL=C sort | xargs shasum -a 256
```

### Supported behaviour in this release

- **One supported path** for a new installation: the Helm chart with the
  managed installation identity, saved connections and destinations, schedules,
  the product console (`logweir-api`, `localAdmin` or `shared`), restore of a
  chosen point with an approval, verification, and a disaster restore from a
  connected archive with no `Backup` objects at all
  ([quickstart.md](quickstart.md)). The static page behind `kubectl proxy`
  remains a supported legacy view; the destination, discovery, readiness,
  catalog and schedule-policy flows are console-only.
- **A versioned PoC install profile**, [deploy/poc/](../deploy/poc/README.md):
  Traefik, cert-manager with a local CA, Dex and the shared console, all by
  Helm from the published OCI chart and its `sha-` images, with no post-install
  patch: the six chart gaps it first had to stand in for (G1–G6) are chart
  values and behaviours now — the issuer CA bundle, host aliases, the
  connection objects' namespace, the published chart, controller probes and the
  ingress controller trusted by its Service. Installed at `86a554e6`, signed
  into, upgraded from `v0.1.5` and from `sha-f49849d…`, and rolled back on
  docker-desktop on 2026-09-24 ([deploy/poc/](../deploy/poc/README.md), *What
  the first live round showed*). The running install was then upgraded in place
  five times by the profile's *Upgrade to a newer publication*, the last to
  `fdb48cd8` on 2026-09-25 ([release-handoff.md](release-handoff.md)). The
  fixes those rounds needed are in the profile.
- **The chart is published** as `oci://registry-1.docker.io/vladyslavhaina/logweir-chart`,
  beside the images and versioned with them ([install.md](install.md), *(c) The
  Helm chart*). The first publication the PoC ran, `0.1.0-sha-86a554e6…`
  (digest `sha256:90b4d41b…`), and the last, `0.1.0-sha-fdb48cd8…` (digest
  `sha256:36e65b2a…`, CI run 36183088296), pull anonymously and name their own
  commit's four images.
  **On first publication, `vladyslavhaina/logweir-chart` must be Public in Docker Hub**, or `main` CI's
  chart step fails closed (its anonymous pull-back is refused) until the repository is made Public and
  the job is re-run.
- **Fourteen kinds** on `logweir.dev/v1alpha1`, all additive over `v0.1.5`
  ([install.md](install.md), *Upgrade CRDs before upgrading the controller*).
- **Trust has a lifecycle.** A `TrustPolicy` governs a namespace's keys with
  retirement and revocation; `TrustRoster/default` is the deprecated fallback
  and still works unmigrated ([keys.md](keys.md)).
- **Approval policy is per namespace.** Unbound namespaces keep
  `legacy-governed-v1` (an out-of-band signed approval); a namespace may be bound
  to `Ordinary` (the console's own confirmation) or `Governed` (the console's
  confirmation plus an independent approver) ([kubernetes.md](kubernetes.md)
  §8, *Approval policy*).
- **Retention can delete, and only when an administrator says so** — see
  *Retention authority* below.

### Required operator actions, in upgrade order

Do these before rolling the controller, in this order. Each is also listed
under its item below.

1. **Back up the installation identity** (`logweir-signing-key` and
   `logweir-signing-trust`) and export the trust material
   (`logweir trust export`) — [install.md](install.md), *Back up and recover
   the installation identity*.
2. **Grant every retention delete credential `s3:GetObject` on
   `<bucket>/<prefix>/*`** (item 1).
3. **Move every `RetentionPolicy` on a versioned or Object Lock bucket to
   `mode: Report` or `mode: ExternalLifecycle`** (item 2).
4. **Review alert rules that match `NoExitCode`** (item 4).
5. **Bring a shared-console values file in line** (`controller.watchNamespaces`
   and the proxy CIDR bound) before `helm upgrade`, or the render stops
   (item 6).
6. **Apply the CRDs** (`kubectl apply --server-side --force-conflicts`), **wait
   for all fourteen to be established**, then roll the
   controller **and the runner image together** (item 5), with no Backup in
   flight (item 11) and no Restore running (item 12), then the console,
   then any approval-policy binding (item 7). On the Helm path these are one
   release and successive `helm upgrade`s: first the image values (controller,
   runner and console move together), then the `approvalPolicy.*` values.

**Pre-upgrade check: which compromise revocations become installation-wide**
(item 16). This build applies every `KeyCompromise` revocation a `TrustPolicy`
records to every namespace, not only the ones that policy governs, and holds that
policy on deletion while anything else lists the key. Before deploying it, list
the records:

```bash
kubectl --context <ctx> get trustpolicies -o json \
  | jq -r '.items[] | .metadata.name as $p | .spec.keys[]
      | select(.state == "Revoked" and .revocationReason == "KeyCompromise")
      | "\($p)\t\(.keyId)"'
```

Each line is a policy and a key id that will read as compromised in EVERY
namespace after the upgrade. No output means nothing changes. For each line,
confirm that the key really is compromised for the whole installation. A key
revoked this way only to test a namespace's trust (a shared installation signer
revoked in a test policy, for example) turns every document it signed, in every
namespace, `Untrusted`. It also keeps the policy from being deleted while
`TrustRoster/default` or another policy still lists the key. Compare with the
roster's key ids:

```bash
kubectl --context <ctx> get trustroster default -o json \
  | jq -r '.spec.signingKeys[].keyId, .spec.approverKeys[].keyId'
```

A key that must not be compromised everywhere cannot be un-revoked (G3, and G9
in this build). Either delete that policy before the upgrade, or re-issue the key.
Do not deploy until every listed record is one you mean installation-wide.

**Pre-upgrade check: which `Restore`s and `RehearsalSchedule`s carry a
`runnerResources` block** (item 21). This build applies the block to the runner
container, or refuses the object, where earlier builds ignored it. Run item 21's
inventory before the controller rolls; no output means the upgrade changes
nothing there.

### The twenty-seven operator-facing changes

Each item names what changed, what to do, what the claim rests on (its
verification scope), and how to roll it back. Items 1–20 were collected for
PLAT-20.2 from merged changes; the defect names are the platform tracker's.
Items 17–20 were found by the PoC rounds and landed after its first
publication (`86a554e6`); each was proven on the running install by the
in-place upgrade that carried it. Items 21 and 22 are the product-expansion
tracker's fix-now rows FX-2 and FX-5 and are not proven live yet: the PoC
upgrade that carries each runs its rows. Item 23 is fix-now row FX-10, proven
offline; the PoC upgrade that carries it checks that the PoC's policy
document and its digest are unchanged (the PoC sets neither withdrawn key).
Item 24 is fix-now row FX-3, proven on a compose stack (it changes the
runner's signed scorecard, not the controller).
Item 25 is fix-now row FX-13 and is not proven live yet: the PoC upgrade that
carries it runs two clients through Traefik. Item 26 is fix-now row
FX-11 and is not proven live yet: the PoC upgrade that carries it runs its
rows.
Item 27 is fix-now row FX-8, proven on the compose stack; the PoC upgrade that
carries it runs its refusal and opt-in rows.

#### 1. Retention needs `s3:GetObject` — required action

**Changed.** The retention worker HEADs every key before deleting it, to refuse
a versioned bucket where a delete by key would only write a delete marker
(OBJECT-LOCK-DELETE-MARKER). **Do:** grant the retention delete credential
`s3:GetObject` on `<bucket>/<prefix>/*` **before** upgrading. Without it the
enforcer deletes nothing and records every point `Kept` with
`VersionProbeRefused`. A policy degraded for that reason re-probes 24 h after
its last run, or at once on any spec edit. **Scope:** unit and controller tests
on `main` `b57753b`; the grant is documented in
[install.md](install.md) §3.11 and [kubernetes.md](kubernetes.md) §7a. Live on
lab-refresh-9 (`306cebf`, 2026-09-23): the enforcer's grant set measured 9/9
(U6), and `VersionProbeRefused` without `s3:GetObject` (PLAT-16.2's completion
record).
**Rollback:** the extra grant is harmless to an older worker.

#### 2. `Enforce` refuses versioned and Object Lock buckets

**Changed.** `Enforce` on a versioned or Object Lock bucket now deletes nothing
and degrades with `VersionedBucket`. **Records written by earlier builds on such
buckets that say `Deleted` are false**: the data is still there as noncurrent
versions behind delete markers. **Do:** use an unversioned bucket or
`mode: ExternalLifecycle` (a provider rule such as
`NoncurrentVersionExpiration`). Do not change a bucket's versioning while a
retention run is in flight, and do not enforce on a bucket whose versioning was
ever enabled and later suspended — there a deletion removes only the newest
copy while being recorded `Deleted` (accepted residual
RET-VERSIONED-SUSPENDED-REWRITE; `Deleted` means the current object at each key
was removed). **Scope:** found live by harness-rows-11 on the lab MinIO; fixed
on `main` `b57753b` with two review rounds. Live on lab-refresh-9 (`306cebf`):
on a versioned or Object Lock bucket `Enforce` deleted nothing, named every
point `VersionedBucket` and wrote 0 delete markers, including a
plain-then-versioned arm (PLAT-16.2's completion record).
**Rollback:** set such policies to `mode: Report` **before** rolling back; the
older worker records delete markers as deletions.

#### 3. Re-run receipts over one backup set are protected together

**Changed.** Two receipts can name one backup set (a runner Job re-created from
its frozen inputs signs a second receipt). The older one is now protected as
`SharedSegment` until every receipt naming the set is due, and then the set is
removed by one plan line naming the others in `co_point_ids`
(SHARED-SET-RETENTION). **Do:** re-approve any plan digest approved over such a
plan — it changed. Plans with no shared set are byte-identical and their
approvals hold. A shared set with more receipts than `maxDeletionsPerRun` is
never selected; raise the ceiling to let it go. **Scope:** found live by
harness-rows-11 (it was data loss); fixed on `main` `b57753b`, Tier-A review
with two fix rounds. Live on lab-refresh-9 (`306cebf`): two receipts over one
set were both kept `SharedSegment`, with no plan line (PLAT-16.2's completion
record).
**Rollback:** set every `RetentionPolicy` to `mode: Report` **before** rolling
back: an older controller plans without this protection again and could plan
a set a retained receipt still names (a deletion would still need a fresh
`approvedPlanSha256`). An older worker refuses a plan carrying `co_point_ids`
(exit 3) and deletes nothing.

#### 4. Runs that end without an exit code are named

**Changed.** A run whose pod never started may now end `Failed/VolumeMountFailed`,
`Failed/PodUnschedulable` or `Failed/RunnerImageUnavailable` instead of
`NoExitCode`, when the matching diagnostic was still being observed as the Job
ended (WARNING-DIAGNOSTICS-NOEXITCODE; [kubernetes.md](kubernetes.md) §10,
*`RunnerReady`, and the four states a run can reach with no exit code*).
`exitCode` stays absent. **Do:** review alert rules that match `NoExitCode`.
**Scope:** controller tests on `main` `b57753b` and `c892650` (a crash-looping
runner's start is read from `lastState`). Live on lab-refresh-9 (`306cebf`,
2026-09-23): two mount failures ended `VolumeMountFailed` and an unschedulable
pod `PodUnschedulable` (PLAT-14.1's completion record).
**Rollback:** an older controller writes `NoExitCode` again; nothing stored
needs converting.

#### 5. Disaster restore upgrades the controller and runner together (PLAT-15.2)

**Changed.** A restore bound to a catalog point verifies the point's receipt
signature in the runner before any data moves, against an evidence keyring the
controller renders into the approval bundle. **Do:** upgrade the controller and
runner images **together**. A standalone `logweir restore run` of a point-bound
plan now needs `--evidence-keys`. A point-bound `Restore` whose bundle an older
controller created ends `ApprovalBundleConflict`: delete it and create it again.
**Scope:** PLAT-15.2 is on `main` since `ac00819` ([kubernetes.md](kubernetes.md)
§7d.1), Done 2026-09-23 on lab-refresh-9 (`306cebf`): a restore after the loss
of every custom resource (0 CRs, 100 records restored), and an untrusted signer
refused by the Preflight (`CatalogPointSignerUntrusted`) and by the runner
(exit 3, `PointUntrusted`). On the PoC (2026-09-24, `86a554e6`) a
catalog-verified point restored `Valid` 150/150 through the console.
**Rollback:** an older runner refuses `--evidence-keys` and exits 1 before any
work; a runner at this version refuses a point-bound plan from an older
controller (`PointUntrusted`). Archives and catalog records are untouched.

#### 6. Shared console values must name the controller's namespaces (PLAT-17.2)

**Changed.** A values file with `api.console.mode: shared` stops rendering until
`controller.watchNamespaces` is set, excludes the release namespace, and lists
every `roles.bindings` namespace; `ui.enabled` must be off beside it; and
`trustedProxyCidrs` wider than `/16` (IPv4) or `/48` (IPv6) is refused when
`requireTrustedProxy` is on. The refused `helm upgrade` changes nothing in the
cluster. **Do:** follow [install.md](install.md) §5e's migration list, then
upgrade once. **Scope:** chart lint and render tests; PLAT-17.2 is on `main`
since `ac00819`. Shared mode ran behind a TLS ingress (Traefik, cert-manager) and
an OIDC provider (Dex) on docker-desktop on 2026-09-24 — the PoC profile: sign-in
per role, the role matrix, CSRF, forged headers, unauthenticated API and stream
requests, the trusted-entry `421` and the console journey held. Dex with static
users is the provider that ran; a corporate IdP bound by group has not.
**Rollback:** reinstall the previous chart version with the previous values;
the scoping objects go and the cluster-wide binding returns.

#### 7. Approval policy: Ordinary and Governed (PLAT-19.2)

**Changed.** `Ordinary` confirmation requires `allowOrdinaryConfirmation: true`
plus an explicit namespace binding, and is refused by the `localAdmin` console;
`Governed` requires a change ticket. The policy reference a document signs is
`{name, digest}`, so any edit to a policy is a different policy (D0 amendment).
**Do:** keys first (a `ConsoleConfirmation` key and, for `Governed`, each
approver's `GovernedApproval` key with `principal.id`), then the binding
([install.md](install.md) §5f). A `Restore` submitted during a policy rollout
may need resubmitting once the rollout completes. **Scope:** PLAT-19.2 is on
`main` since `ac00819`; controller, runner and API tests. Done 2026-09-23 on
lab-refresh-9 (`306cebf`): Ordinary admitted with the frozen policy and the
console key, Governed needing both signatures, self-approval refused `403`, an
unbound namespace keeping `legacy-governed-v1`. On the PoC: a Governed restore
approved by a second person (2026-09-24), and Ordinary restores in every round.
**Rollback:** unbinding returns the namespace to `legacy-governed-v1`;
not-yet-admitted v2 approvals are then refused, never admitted as v1. An older
controller refuses every v2 document as `PayloadTypeMismatch`.

#### 8. Restore completion is written only from a valid scorecard

**Changed.** `Restore.status.completion` appears only once the scorecard's
verification is `Valid` (RESTORE-COMPLETION-UNWRITTEN). Its `recordsRestored`
is the count read back from the target **in the sampled window** —
`sample.records_restored`, which the console labels *records verified in the
sampled window* — never the total the restore wrote. **Do:** nothing; read
absence as "not yet verified", never as zero. **Scope:** `main` `fa3384e`.
Seen live on 2026-09-24 (PoC install): restores of pre-upgrade points after both
upgrade rehearsals and the console journey's restore wrote `completion` from a `Valid`
scorecard (150 of 150 sampled records matching), and a restore whose scorecard the
controller could not read wrote none.
**Rollback:** an older controller writes no `completion` at all.

#### 9. A schedule never fires a slot due before it was created

**Changed.** A slot whose due time is before the schedule's
`metadata.creationTimestamp` never fires, for `BackupSchedule` and
`RehearsalSchedule` alike (D1 §4.7 row 5a; SCHEDULE-FIRES-SLOT-BEFORE-CREATION).
Use *Run first backup now* for an immediate first run. **Do:** nothing;
deleting and recreating a schedule (a GitOps prune and re-apply) resets the
bound. **Scope:** `main` `fa3384e`, both kinds. Live on lab-refresh-9
(`306cebf`, SCHEDULE-FIRES-SLOT-BEFORE-CREATION), and on the PoC at
`86a554e6` (2026-09-25): a schedule created at 00:01:48Z did not fire its 00:00
slot. `v0.1.5` and `sha-f49849d…` each fired such a slot in the rehearsals.
**Rollback:** an older controller may fire that slot once.

#### 10. The API's `trust.state` never calls a revoked-key observation green

**Changed.** `trust.state` for `result: Valid` beside
`trust.basis: RecordedBeforeRevocation`, `None`, an absent basis inside a
`trust` block, or a basis word the server does not know is now `untrusted`,
where it read `verified`; `Valid` beside `Unverified` (nothing compared yet) is
`notAttempted` (TRUST-STATE-RBR-VERIFIED). `Current` stays `verified` and
`Historical` stays `verifiedHistorical`. **Do:** a client that must also read older servers
checks `trust.basis` as well ([api.md](api.md)). **Scope:** `main` `178cc1c`,
Tier-A review; the console reads the same word in both modes. **Rollback:** an
older API server says `verified` again for that pairing.

#### 11. One backup id is one engine run; the evidence store must enforce conditional create

**Changed.** A Backup Job lost and re-created from its frozen inputs used to run
the engine again under the same execution id, rewrite the manifest the first
run's signed receipt attests, and leave that receipt describing an archive that
is no longer there (RECEIPT-DUP). `logweir backup run` now claims its execution
with a create-only `logweir/backups/<backupId>/execution.claim.json` before the
engine starts. A second run of one execution exits 1 with `status.exitReason:
ExecutionAlreadyClaimed` and writes nothing; a schedule retries it under a new
execution id only when it has `spec.retry`. An evidence store that does not
enforce `If-None-Match: *` makes every backup exit 4 `ExecutionClaimUnproven`
before any data is written, and a destination with `writeProbe` on reports it
`notReady / ConditionalCreateUnsupported` first. The claim adds no permission and
changes no signed format; FX-7 below adds receipt and catalog format `1.2.0`, the MINOR after FX-4's `1.1.0` ([stability.md](stability.md#the-first-post-tag-addition-format-110-fx-4)).
**Do:** confirm the evidence store honours conditional create — turn on `writeProbe: CreateOnlyMarker` for one run of the destination
check, and never set `AWS_CONDITIONAL_PUT=disabled` — see the store table in
[support-matrix.md](support-matrix.md). A standalone `logweir backup run` that
reused a fixed `backup_id` must pass a fresh `--backup-id-override` per run.
**The upgrade window is closed since FX-7:** an execution whose first run was
made by the older runner has no claim, so if its Job is lost and re-created
after the upgrade the new runner wins a claim — and then finds the older run's
manifest or segments under `<prefix>/<backup_id>/` and stops, exit 1
`ExecutionAlreadyClaimed`, before the engine. Only an older runner that is still RUNNING when its Job is
re-created, and has written nothing yet, escapes both checks; let such a Job
finish before upgrading. A read of the archive that fails while proving the set
new is exit 1 when it is transient (a transport error, a timeout, a 5xx), so a
schedule with `spec.retry` retries it under a new execution id, and exit 4
`ExecutionClaimUnproven` otherwise (a 403, a wrong bucket). On a versioned bucket a receipt also pins its
manifest's version (`archive.manifest_version_id`, receipt format `1.2.0`), so a
set written again in that bucket after the point was signed — by an older
runner after a rollback, say — is refused by a point-bound restore
(`PointBindingMismatch`) and reported `Conflict` by the catalog even when the
manifest bytes came out identical. That detection covers the points this build
signed; the older runner's own receipt over the rewritten set pins nothing and
stays selectable. A version id belongs to one bucket, so a byte-for-byte COPY
of the archive (`aws s3 sync`, `mc mirror`, an unversioned destination) is the
same point, checked by its manifest digest: the restore runs and logs
`PointPinUnchecked`, and the catalog entry's remedy says the pin could not be
checked in that bucket. **The pin is checked only where the bucket still holds
the pinned version and serves it by id:** a rewrite whose pinned version was
since expired or DELETED, a copy synced after the set was written again, or a
store that cannot read by version reads the same way, and there the digest
cannot see segments rewritten under an identical manifest. Object Lock
retention covering a point's lifetime keeps its pinned version; when the
signing bucket's catalog says `Conflict` and a copy's says `Available`, believe
the `Conflict`. The pin's read by id needs
`s3:GetObjectVersion` on the archive prefix
([backup-receipt.md](formats/backup-receipt.md#the-pinned-manifest-version-versioned-buckets)).
**Scope:** in-process rows, a private MinIO `RELEASE.2025-09-07T16-13-09Z`
container, and four planted mutants plus the review's two. Live on
lab-refresh-10 (2026-09-24): PLAT-06.1's case e (a lost Job re-created), both
arms, and the receipt-dup rows 2–5 (RECEIPT-DUP). FX-7 (2026-09-29): compose
slot 3, MinIO unversioned and SeaweedFS versioned buckets, a `v0.1.5` runner
for the older build's run; its fix round (2026-10-05): slot 2, the same matrix
plus byte-for-byte copies of a pinned point into both stores and a point-bound
`restore run` against each bucket.
**Rollback:** an older runner ignores the claims, the set check and the pin,
and returns to re-running the engine over a re-created Job; the claims stay in
the bucket, harmless, and are honoured again after a re-upgrade. **Before
rolling the runner back to a build without the execution claim, let in-flight
`Backup`s finish:** on an unversioned bucket a re-created Job's older runner
rewrites the set's segments under an unchanged manifest, and no check reports
it — the first, signed point keeps verifying. On a versioned bucket that
point is reported `Conflict`.

#### 12. A failed restore's signed scorecard: roll the controller out before the runner

**Changed.** A restore runner at this version names its signed failure at exit 2
(the scorecard of a run whose data did not reconcile is published and verified
`Valid`), and this controller writes `Restore.status.completion` — the console's
completion panel with its cutover guidance — only for a run that PASSED
(`exitCode 0`, `outcome: pass`, a green verdict). An **older controller** gates
completion on the verdict alone, so it would write a completion panel over a
restore that FAILED. **Do:** roll the controller out before (or with) the
runner, never the runner first, and roll the runner back before the controller;
let running `Restore`s finish before either. On the Helm path both move in one
`helm upgrade`, which is safe once nothing is running. **Scope:**
`claude/rehearsal-fix` (`6b2704d`, Tier-A review), [stability.md](stability.md),
*Mixed versions*. Live on lab-refresh-10 (2026-09-24): the failed Restore got no
`status.completion`, and the passing one's panel was present
(CONSOLE-COMPLETION-ON-FAILED-RESTORE). **Rollback:** runner first, then
controller; nothing stored needs converting.

#### 13. Readiness rows are answered by the principal they name; an older runner says "upgrade"

**Changed.** A destination readiness check (`DestinationAccess`, and the
destination rows of a Backup/Restore check) now answers `evidenceWritable` with
the `evidenceWrite` grant and `evidenceReadable` with the `evidenceRead` grant
when they differ from the checked one — the row's sentence is about the right
principal (PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL) — and a `DestinationAccess`
check on a `writeProbe: CreateOnlyMarker` destination now writes its one
create-only marker under `logweir/readiness/` (DESTINATIONACCESS-IGNORES-WRITEPROBE).
**Do:** upgrade the runner image **with** the controller: an older runner handed
such a plan refuses it at startup — the `Preflight` ends `Failed` /
`CheckContractMismatch` with a message saying the runner is older than the
controller — and nothing is written as the wrong principal. Set
`writeProbe: Disabled` on a destination that must never receive the marker.
**Scope:** `claude/readiness-principal` (merged `540e3ea`), closed live at
lab-refresh-10 (rows RP-L1…L15); [kubernetes.md](kubernetes.md) §21,
*The evidence-write grant in a check plan (mixed versions)*. **Rollback:** an
older controller renders the old plans again, which a newer runner still
accepts (the old wrong-principal answer returns until you re-upgrade).

#### 14. A recovery point with no saved destination is checked, verified and completed

**Changed.** Three behaviours for a point written without a `BackupDestination`
— every point `v0.1.5` wrote — found by the PoC upgrade round (P3, P5, P6):
**readiness** — a restore readiness check over such a point
(`legacySourceArchive`) reads the archive with the restore Job's own principal
(the Backup's Secret, keys `access-key-id` / `secret-access-key`) at the
approved plan's location, instead of ending `Failed/ArchiveUrlUnreadable` and
leaving the wizard's Create disabled; no `secretRef`, a non-`s3://` archive or
an unreachable plan location is `notReady` and named. **Evidence** — the
console writes such a restore's evidence to the archive's own bucket (it wrote
`logweir-evidence`), and the controller reads an inline-archive run's evidence
only in the bucket of its archive handle (`LOGWEIR_ARCHIVE_URL`): a run whose
evidence is elsewhere is `NotAttempted` naming its own evidence bucket (the
handle is named by role; its URL is only in the controller log) and is never
read in the wrong one, and an inline-archive scorecard the handle read nothing
for is `NotAttempted` naming the key — it used to publish no verification and
no completion at all. A rehearsal over such a point records
`VerificationNotAttempted` at once instead of `EvidenceVerdictNotReached` after
five minutes; destination-backed runs are unchanged. The readiness verdict is
bound to the archive Secret: editing it after a green check refuses the Create,
and a check of an existing `Restore` must name that Restore's own archive and
Secret.
**Catalog** — a `Full` or `Index` sync reads catalog records only, so a
pre-catalog point is not in a connected catalog until its record is backfilled
([kubernetes.md](kubernetes.md) §7d, §15.1a, §21.8). **Do:** if your legacy
schedules write to a bucket other than `LOGWEIR_ARCHIVE_URL`'s (chart
`archive.url`; with the bundled MinIO `s3://kafka-backups/<release>`), their
runs now read `NotAttempted` rather than a misleading store error — verify them
with the printed commands or move them to a `BackupDestination`; write a
hand-written legacy restore plan's `evidence:` to the handle's bucket; run
`logweir catalog sync` once per archive to list `v0.1.5` points in a catalog
(D3 designs `Full` as a read-only receipt walk that would make this unnecessary;
this build's `Full` reads records only, a gap tracked for PLAT-15.1);
re-run any readiness check made before the upgrade. **Scope:** in-process rows
over the preflight, restore and backup controllers and the console suite.
**Live** (PoC upgrade round, 2026-09-25, `claude/poc-upgrade-1`): on the PoC upgraded to `sha-02dc44b6…`, a point of a `v0.1.5`-shaped inline-archive schedule (`s3://kafka-backups/poc`, Secret `logweir-s3`) passed its readiness check with the restore Job's own principal, restored `Valid` with a 150/150 completion, refused a changed archive Secret, and a plan naming `logweir-evidence` read `NotAttempted` with the handle named by role only. The controller's handle needs its read credential (`logweir-evidence-ro`, [install.md](install.md) §3): without it every inline-archive run reads `NotAttempted` and is not offered as a recovery point until the controller reads it again ([kubernetes.md](kubernetes.md) §15.1b: at +1, +5 and +15 minutes, and once per controller process). [UNVERIFIED — a point written by the v0.1.5 runner itself was not available on the upgraded install to run this path.]
**Live** (`claude/poc-upgrade-2`, the upgrade to `sha-b748fd5f…`): the three inline-archive points that round 1 left `NotAttempted` verified `Valid`, with their covered window, 3 s after the new controller started, and one of them restored `Valid` with a 150/150 completion through the console.
**Rollback:** an older controller answers a legacy restore readiness check
`Failed/ArchiveUrlUnreadable` again and reads a legacy restore's evidence in the
handle's bucket whatever the plan names; a plan the new console rendered still
verifies under it when the archive's bucket is the handle's.

#### 15. Manual runs may queue; "Back up now" is rate limited (P10)

**Changed.** Nothing used to bound manual runs: on the PoC install one
operator's hundred accepted `POST …/backups` (all `201` within 2.4 s) became a
hundred simultaneous runner pods, the node hit its 110-pod limit and went
`NotReady`, and MinIO answered `503 SlowDown`. Now:

- **The controller bounds manual runs per namespace.** At most
  `runs.maxManualBackupsActivePerNamespace` (default `4`) manual `Backup`s and
  `runs.maxManualRestoresActivePerNamespace` (default `2`) admitted manual
  `Restore`s hold a runner slot at once in one namespace (one installation
  value each, applied in every namespace). The rest wait with
  `phase: Queued`, `Admitted=False / ConcurrencyLimited` and
  `status.queue.limit`, **with nothing created** — no plan, no Job, no
  execution claim — and start in arrival order as slots free (within one
  requeue, 15 s). Each admission is reserved in the controller and recorded on
  the run (`Admitted=True`) before anything is created, so runs released
  together — restores whose approvals verify at once, runs held on a
  destination, everything after a controller restart — never pass the
  ceiling. The console shows "Queued (limit N active)". Scheduled, catch-up
  and retry `Backup`s and a `RehearsalSchedule`'s `Restore`s are neither
  counted nor queued; `concurrencyPolicy` is unchanged.
- **A queued restore keeps its approval's deadline.** The queue does not
  extend an approval's maximum age: the deadline is on the object
  (`status.queue.authorizationExpiresAt`, "approval expires T" in the
  console), and a restore still queued when it passes is refused
  `AuthorizationExpired` "while queued behind N"; confirm again and create a
  new one.
- **The console limits how fast one person can start runs:** `10`
  "Back up now" and `5` manual restores per person (`issuer#subject`), per
  namespace, per minute (`api.console.rateLimits.*`), then `429 rate_limited`
  with `Retry-After`. A malformed request does not spend the window; a
  replayed idempotency key does.
- **Known limit:** a subject allowed to create `Backup` objects directly can
  declare a scheduled kind for an existing schedule and escape both the pool
  and `concurrencyPolicy`; RBAC on `create backups` governs that path.

**Do:** nothing is required. An automation that starts more than the limits
above must pace itself, or read `429` and `Retry-After`; one that expects a
manual run to be `Running` right after `201` must also accept `Queued`. Raise
`runs.*` where every namespace's nodes can carry more runner pods at once.
**Scope:** pure and route-table rows (`crates/weirkeeper/tests/manual_run_pool.rs`,
`restore_controller.rs`, `restore_policy.rs`,
`crates/logweir-api/tests/manual_run_limits.rs`), chart rows, and twelve
planted mutants (eight first round, four in the review round, including the
reviewer's two survivors), each killed. **Live** (PoC upgrade round, 2026-09-25, `claude/poc-upgrade-1`): twenty manual runs from two people at once held at most four runner pods, the rest `Queued` with nothing created, through a controller restart, while a scheduled slot started at once; three approved restores at once ran two and queued one with its approval deadline shown. The per-person `429` came at the 21st "Back up now" and the 11th restore request, because the window is per console process and the PoC runs two replicas.
**Rollback:** in this order, roll the **console** and the **controller** back
**with the chart** (`helm rollback`). A default install carries neither new
block — the chart renders `runs` in `weirkeeper-policy` and `rateLimits` in the
console configuration **only** when a value differs from the defaults — so an
image-only rollback of a default install keeps working. With a non-default
value, an older controller refuses the whole `weirkeeper-policy` ConfigMap and
fails closed (no attestations, no evidence allowlist), and an older console
refuses its configuration file and does not start. An older controller has no
pool: it reads `Queued` as an active phase and starts every queued run at
once. An older console shows a queued run as `unknown` (`UnrecognizedPhase`).

#### 16. A compromise revocation outlives its `TrustPolicy`; deleting one may wait

**Changed.** On the PoC install (rehearsal R2) a `TrustPolicy` revoked the
installation signer for `KeyCompromise`, every backup it had signed turned
`Untrusted`, and deleting the policy sent the namespace back to
`legacy-roster-v1`, whose roster still listed the key: all six backups
re-verified `Valid` (TRUSTPOLICY-DELETE-DROPS-REVOCATION). A compromise is now
a fact about the key, not the policy:

- **Every namespace.** A key ANY `TrustPolicy` records as `Revoked` /
  `KeyCompromise` is revoked for compromise wherever it is listed — the
  recording policy's namespaces, a namespace re-bound away from it, one that
  falls to the roster, and one governed by another policy that still lists the
  key `Active`. Evidence verification and re-trust, the catalog view, the
  runner's evidence keyring, fresh and consumed approvals, `signer.rostered`,
  the API's `trust.state` and the keys view all read it; the refusal names the
  recording policy. A `Superseded` revocation is not carried.
- **Deleting a recording policy is held.** The controller places the finalizer
  `logweir.dev/compromise-revocation` on it and releases a `kubectl delete` only
  when another live policy records the same revocation, or nothing (no other
  policy, not `TrustRoster/default`) lists the key. Until then the policy stays
  `Terminating` and keeps governing; `CompromiseGuard=True/DeletionBlocked`
  names what still lists the key.
- **CEL rule G9:** a revoked key's `KeyCompromise` reason can no longer be
  edited to `Superseded` or removed.
- **The controller now holds `patch` on `trustpolicies`** (the object, beside
  `/status`) for that finalizer and nothing else; the body is pinned by a test.

**Do:** before this upgrade, run the *Pre-upgrade check* above (the
`kubectl get trustpolicies -o json | jq …` listing) and confirm every
`KeyCompromise` record it prints is one you mean installation-wide. After the
upgrade, replace a policy that records a compromise by applying a successor
under a new name first
([keys.md](keys.md), *Replacing a `TrustPolicy` safely*); the delete-and-re-create
repair for a mistaken `notBefore` still works unchanged for a policy that
records no compromise. Wait until a newly revoked policy lists the finalizer
before deleting it: a policy revoked and deleted before the controller saw it is
not guarded. **Scope:** `crates/weirkeeper/tests/trust_revocation_consumers.rs`
(one row per consumer; eight of ten fail on the previous commit),
`trust_revocation_durable.rs` (the guard table and the finalizer over a route
table enforcing seam S7), `crd_shape.rs` (G9), `approval_policy.rs`,
`crates/logweir-api/tests/trust_revocation_durable.rs`, `ui/tests/d3.spec.js`,
and planted mutants, each killed.
**Live** (PoC upgrade round, 2026-09-25, `claude/poc-upgrade-2`, on the PoC upgraded to `sha-b748fd5f…`, with a MINTED key listed only on two test policies): the pre-upgrade check printed nothing; recording the compromise on one policy placed the finalizer and `CompromiseGuard=CompromiseRecorded` within 2 s, and the other policy, still declaring the key `Active`, read it `Revoked`/`CompromiseInherited` in the same 2 s (the API's `effectiveState` and the console's keys page agreed); a `kubectl delete` of the recorder stayed `Terminating` with `DeletionBlocked` naming the other policy until a successor recorded the same revocation (released in 2 s); G9 refused `KeyCompromise`→`Superseded` on the API server and accepted `Superseded`→`KeyCompromise`; cleanup released every policy at once, and the PoC's own policy and keys were unchanged. Not shown live: a re-derived `Backup`/`Restore` verdict, which needs evidence the minted key signed in a namespace the controller watches.
**Rollback:** an older controller never removes the finalizer, so a deletion
made while it runs is held until this build returns; an older
`TrustPolicy`-aware controller applies a compromise only in the recording
policy's own namespaces, and `v0.1.5` reads only the roster and trusts every key
it lists. So before rolling back, re-create `TrustRoster/default` without every
compromise-revoked key and record each compromise on every policy that still
lists the key (rollback step 9 below). G9 stays with the CRDs, which a rollback
leaves in place.

#### 17. An `Approval` its Restore was admitted under is kept as a record (P9)

**Changed.** On the PoC install (2026-09-24, `86a554e6`), 900 s after an
Ordinary confirmation the controller rewrote a succeeded Restore's `Approval`
to `Verified=False/AuthorizationExpired`, dropped its recorded authorization,
and rewrote it again every pass: about 42 `approval refused` lines a second,
with the API server above 100% CPU. Expiry, the policy binding and the key
windows now bound only the time to admission. Once the Restore it names is
`Admitted=True` and that Restore's approval bundle names this `Approval`'s UID,
the controller adds `Consumed=True` (reason
`RestoreAdmitted`; its `lastTransitionTime` is the admission instant) and keeps
`Verified=True`, `status.authorization`, the key id and the approver as
recorded. A compromise revocation of the recorded key still withdraws the green
(`RecordedBeforeRevocation` or `KeyRevoked`); an `Approval` deleted and
re-created is never `Consumed`; a refusal message no longer names the current
time ([kubernetes.md](kubernetes.md) §8, *After admission the `Approval` is a
record, not a gate*). `Consumed` can land at the `Approval`'s next pass — at
the latest its expiry, 15 minutes on the PoC — backdated to the admission; the
API reads `verified: true` meanwhile. **Do:** nothing is required. Drop any
workaround that deleted `Approval`s after their restore. On the upgrade, an
`Approval` an earlier build withdrew this way is re-checked at the admission
instant and, when it verifies there, returns to `Verified=True` with
`Consumed`; with the approval-policy binding off (the PoC's upgrade step 2) it
reads `ApprovalPolicyMismatch` until the binding returns. **Scope:**
`crates/weirkeeper/tests/approval_policy.rs` and `approval_controller.rs`, five
planted mutants killed, a review round (`claude/poc-fixes-2`, merged
`7974b43a`). **Live** (`claude/poc-upgrade-1`, the upgrade to `02dc44b6`,
2026-09-25): an `Approval` the old controller had withdrawn was restored at
upgrade step 3 with `Consumed` at its admission instant; its resourceVersion
did not move for ten minutes, across a controller restart, with no refusal
line; three new Ordinary restores kept `Verified` and `Consumed` past their
expiry; a deleted and re-applied `Approval` was never `Consumed`. All eight
`Approval`s of the second round and both of the third ended `Verified` and
`Consumed`. **Rollback:** an older controller ignores `Consumed` and judges
consumed `Approval`s again; on `86a554e6`, where P9 was found, that is the
loop above.

#### 18. One catalog per destination: a second `RecoveryCatalog` over it is refused (P11)

**Changed.** Several `RecoveryCatalog`s whose `spec.destinationRef` names one
`BackupDestination` in one namespace used to be accepted, each with its own
sync Job: on the PoC at `02dc44b6`, five over `primary`, all `Ready=True`. The
one created first now catalogs the destination. Every later one reports
`Ready=False/DuplicateCatalog` and `Synced=False/DuplicateCatalog`, naming the
catalog that holds it; it runs no sync Job and withdraws its view
(`status.pages`, `status.viewExpiresAt`), so a `ProtectionPolicy` over it reads
`CatalogStale`. If the elder is deleted, the next one in creation order takes
over within a minute ([kubernetes.md](kubernetes.md) §7d, *One catalog per
destination per namespace*). **Do:** before upgrading, list the catalogs per
destination:

```bash
kubectl --context <ctx> get recoverycatalogs -A -o json \
  | jq -r '.items[] | select(.spec.destinationRef != null)
      | "\(.metadata.namespace)\t\(.spec.destinationRef.name)\t\(.metadata.creationTimestamp)\t\(.metadata.name)"' \
  | sort
```

Lines with the same namespace and destination are duplicates; the oldest
survives, not the best. If a newer duplicate has the settings you want (a
larger `viewLimit`, `mode: Full`), delete the older one first. A
`RetentionPolicy.catalogRef`, a `RehearsalSchedule`'s point `catalogRef` or a
`ProtectionPolicy.protects.catalogRef` that names a duplicate loses its input
and fails closed: point it at the survivor (the first two are immutable, so
re-create them). **Scope:** `crates/weirkeeper/tests/recovery_catalog_controller.rs`
and `catalog_controller.rs`, five planted mutants killed, a review round
(`claude/poc-fixes-3`, merged `56205b1`). **Live** (`claude/poc-upgrade-2`, the
upgrade to `b748fd5f`, 2026-09-25): a duplicate the old controller had synced
turned `DuplicateCatalog` on the new controller's first pass; duplicates made in
the console and with kubectl were refused, with no sync Job; a
`ProtectionPolicy` over one read `CatalogStale`; after the elder was deleted the
younger's sync Job started in 28 s and it was `Ready` in 42 s. **Rollback:** an
older controller syncs the duplicates again.

#### 19. A failed controller evidence read is read again (P12)

**Changed.** The controller reads a run's evidence itself for an inline-archive
run (through its archive handle) and for a destination whose `evidenceRead` is
`ControllerIdentity`. A failed read there used to be final: on the PoC three
inline-archive `Backup`s stayed `NotAttempted`, and so were never recovery
points, after `logweir-evidence-ro` was created and after two controller
restarts. Now a transient failure (a denial, a missing credential, a timeout)
is read again three more times, 1, 5 and 15 minutes apart, recorded in
`status.evidence.observation` with its `retryAfter`: at most four reads per run
per controller process, and at most four in flight at once. Each new controller
process reads every eligible unverified run once more. A store `NotFound` is
final, and a `Backup` is never written `Valid` without its `windowCovered`
([kubernetes.md](kubernetes.md) §15.1b). **Do:** a created or rotated
`logweir-evidence-ro` takes effect only when the controller restarts. A run
recorded absent through a misconfigured handle is not read again once the
handle is fixed; check it with its printed `logweir drill verify` command. On
the upgrade, the first controller process of this build reads once each
`NotAttempted` inline-archive run an older controller wrote. **Scope:**
`crates/weirkeeper/tests/backup_controller.rs`, `restore_controller.rs` and
`verification.rs`, eight planted mutants and the review's, each killed
(`claude/poc-fixes-3`, merged `56205b1`). **Live** (`claude/poc-upgrade-2`, the
upgrade to `b748fd5f`, 2026-09-25): the three stuck points turned `Valid` with
their window 3 s after the new controller started, and one restored `Valid`
150/150 through the console; a denied read turned `Valid` on its second attempt
once the denial was lifted; a read denied for 25 minutes was attempted three
more times, 1, 5 and 15 minutes apart, and then stopped; a controller restart
read it once more, and it turned `Valid`. **Rollback:** an older controller ignores the observation
and never reads a failed run again; a verdict this build reached stays.

#### 20. A readiness replay names its own expiry, and the console asks again (P14)

**Changed.** One idempotency key names one `Preflight` for ever, so a key built
from the question alone replayed that check after its validity had passed. On
the PoC at `b748fd5f`, the schedule form's *Check readiness* with unchanged
inputs answered `200 replayed` with an expired check, and no click could make a
fresh one. The API's replay, cancel and operation projections of a finished
check now carry `expired` in `staleReasons` once its `expiresAt` has passed,
beside `unverifiable`, with `staleBasis: ["expiry"]`: the same object, the same
UID, `200` ([api.md](api.md), *A readiness key replays only while its check
can still be the answer*). The console keeps one intent token per form in the
key and renews it once the check is spent (expired, inapplicable, `failed` or
`cancelled`) on the schedule form, the Schedules list's readiness panel and
*Discover topics*; a retry after a lost response, or a second click inside the
validity, still replays. **Do:** a client that builds its own readiness keys
sends a new key once a replay reads `expired`. Nothing else: the response shape
is unchanged, and `expired` was already in the closed vocabulary. **Scope:**
`crates/logweir-api/tests/preflights.rs`
(`a_replay_names_its_own_expiry_and_a_new_key_asks_afresh`), three API and three
console mutants killed, `ui/tests/check-intent.spec.js` (`claude/poc-fixes-4`,
merged `a54fb823`). **Live** (`claude/poc-upgrade-3`, on `a54fb823`,
2026-09-25): after expiry one click on the schedule form gave a `200` replay
reading `staleReasons [expired, unverifiable]` and `staleBasis [expiry]`, then a
`202` under a new key whose check applied; a click inside the window replayed
the same check; the list panel did the same in two dedicated runs; Cancel then
Check made a new check. *Discover topics* after its inventory went stale could
not be staged inside the PoC's 15-minute session. **Rollback:** `helm rollback`
moves the API and the console together, and the older pair replays a spent
check again; nothing stored changes.

#### 21. A `Restore`'s `runnerResources` is applied, or the object is refused (FX-2)

**Changed.** `Restore.spec.runnerResources`, and a `RehearsalSchedule`'s
`spec.bounds.runnerResources` through the child `Restore` it creates, were
accepted, documented as what the runner pod asks for and is capped at, and
dropped: no runner container carried `resources`, so every runner pod was
`BestEffort`. The block now reaches the `runner` container exactly as written.
A block the controller will not apply is refused, never clamped: a quantity
outside the schema's grammar, memory that is not whole bytes, CPU finer than
`1m`, anything above 4 CPUs or `8Gi` (requests included), a zero limit, a memory
limit below `32Mi`, or a request above its limit. A `Restore` with such a block
ends `Failed` with reason `ExecutionSpecInvalid` before its approval is read or
anything is created, and the product API serves it `refused`. A
`RehearsalSchedule` skips every slot as `AuthorizationInvalid` and creates no
child ([kubernetes.md](kubernetes.md) §12, *The runner's requests and limits*).
An object without the block gets exactly the Job it got before; the console
never sets the field.

What changes on the upgrade, for objects that already carry a block:

- **A `Restore` with no Job yet** (held for its approval, queued, or created
  during the upgrade) is judged on its next pass. A valid block now caps its
  pod, so the pod can be rejected by a `ResourceQuota` or `LimitRange` it never
  met before (reported as `RunnerReady=False` with `PodCreationForbidden`, then
  ending `PodCreationForbidden`), or be OOM-killed at its memory limit. A block
  outside the bounds ends it `Failed`/`ExecutionSpecInvalid`, and the remedy is
  a new `Restore`, because `spec` is immutable.
- **A `Restore` whose Job already exists** keeps that Job, which carries no
  `resources`. A terminal `Restore` is untouched.
- **A `RehearsalSchedule`** caps every rehearsal from its next slot, or, when its
  block is outside the bounds, skips every slot as `AuthorizationInvalid` for
  good. The spec is sealed, so the fix is a new schedule and a new signed
  standing authorization.

**Do:** before the upgrade, list every object that carries a block:

```bash
kubectl --context <ctx> get restores,rehearsalschedules -A -o json \
  | jq -r '.items[] | select(.spec.runnerResources // .spec.bounds.runnerResources)
      | "\(.kind) \(.metadata.namespace)/\(.metadata.name)"'
```

No output means nothing changes. Check each listed block against §12's rules
and against its namespace's `ResourceQuota` and `LimitRange`. Replace an
out-of-bounds schedule (and its authorization) before the upgrade, or expect its
slots to be skipped. Expect an out-of-bounds `Restore` that has no Job yet to
end `ExecutionSpecInvalid`. **Scope:** `crates/weirkeeper/tests/runner_resources.rs`
(the rules, and the exact quantity arithmetic in every suffix),
`restore_controller.rs` and `rehearsal_controller.rs` (the container carries
the block exactly, one-sided blocks included; the refusal comes before the
approval is read, with no `POST`; a quota rejection is reported and failed
fast), `crates/logweir-api/tests/status_mapping.rs` (`refused`), and planted
mutants, each killed (FX-2, its review and its fix round). Not yet proven live:
the PoC upgrade that carries FX-2 runs its CRD, refusal and Job-bytes rows. An
admitted `Restore`'s real Job and the `RehearsalSchedule` side wait for
PROD-10.1, which exposes the control in the console.
**Rollback:** an older controller ignores the field again. Its Jobs carry no
`resources`, and a `Restore` this build refused stays `Failed`; an older
product API serves that refusal `failed` again. A
`RehearsalSchedule` this build skipped for its block fires again under the
older controller, uncapped: suspend it (`spec.suspend: true`, the one mutable
field) before rolling back if it must not run. Nothing has to be deleted.

#### 22. A console restore asks for a replication factor it can explain, and keeps its topic subset (FX-5)

**Changed.** The restore wizard wrote `replicationFactor: 1` into every plan
and showed it read-only, so every console restore created topics with
replication factor 1, on any cluster. Step 4 now has a **replication factor**
input. Its default is the target connection's broker count, at most 3, read
from that connection's newest successful topic discovery: a fresh one, or one
whose only stale reason is `expired`, while the controller keeps it
(`checks.discovery.retentionSeconds`, a day by default). With no such
discovery the default stays 1, and step 4 says why and links *Discover
topics* on the target. The source's own factor is not read: it is recorded
only in the archive manifest, and projecting it is PROD-05.1's. Step 4 and the
review step print the factor with where it came from, such as `2 (the target's
2 brokers; ...)` or `3 (set by you; the target has 2 brokers)`. They also say
it can differ from the source's: a topic the source kept at replication factor
1, restored at 3, takes three times the storage it took there. A factor above
a fresh discovery's count is refused before anything is sent, with
`ReplicationFactorExceedsBrokers`. A factor above an older count is not
refused; the readiness check's `target.topicCreate` row stays the check
against the target as it is. The topic-discovery DTO of the product API gains
an optional, additive `brokerCount` ([api.md](api.md), *Bounded, honest topic
inventory*; [ui/README.md](../ui/README.md), *The replication factor: a
default with its basis, an input, and a refusal before Create*).

Also fixed: **a resumed restore draft lost its topic subset.** The console's
draft store keeps strings and booleans only, and the wizard handed it the
topic subset, and a catalog point's typed topic list, as arrays, which it
dropped without a word. After leaving the wizard and coming back in the same
page, every topic of the point was selected again while the page said "your
unsubmitted edits ... are back", so a Restore created from a resumed draft may
have restored more topics than were chosen. Both lists are now kept.

What an operator sees after the console image is upgraded:

- **No discovery of the target in the last day** (the PoC's state): the factor
  is still 1, now with a warning beside the input and a link to run *Discover
  topics* on the target connection.
- **After a *Discover topics* of the target:** a restore into a multi-broker
  target asks for 2 or 3 replicas where it asked for 1, so it stores up to
  three times as much on the target as the same restore did before the
  upgrade, plus replication traffic.
- **A `Restore` created before the upgrade** keeps its plan bytes
  (`Restore.spec` is immutable) and so its factor of 1. A retry builds a new
  plan with the new default.

**Do:** before restoring into a target with little free disk, check the factor
on step 4 and set it yourself if the default is not what you want. Run the
readiness check (step 5) before Create: with no check, a factor the target's
brokers cannot hold fails the approved run when it creates the topics
(`exitCode 1`, `operational`, nothing restored; [quickstart.md](quickstart.md)
§7). For console Restores created before this build from a resumed draft,
compare the topics each one restored with the ones you meant: they are the
`source.topics` list of the plan
(`kubectl --context <ctx> -n <ns> get restore <name> -o jsonpath='{.spec.planBytes}'`).
**Scope:** `ui/tests/replication-factor.spec.js` (the default rule and its
4-broker boundary, the count's freshness rule, the refusal before Create, the
review row, the sentence about the source's factor in the page and in both
documents, the readiness warning, both mounts, and the draft class), with a
negative control for each behaviour, each killed. Also
`crates/logweir-api/tests/topic_discoveries.rs`
(`a_discovery_publishes_the_broker_count_its_result_recorded`, over the fixture
the console rows read), with four API mutants killed, and a Chromium journey
over the real console modules at 1440 and 390 px (FX-5, its review and its fix
round). Not yet proven live: the PoC upgrade that carries FX-5 runs its rows,
stopping before Create. **Rollback:** rolling the console image back restores
the fixed, read-only 1 and the draft that drops the subset, and an older
product API omits `brokerCount`. Nothing stored changes: a `Restore` created
with a factor above 1 keeps it.

#### 23. Two policy values that changed nothing are withdrawn (FX-10)

**Changed.** `checks.discovery.defaultMaxTopics` and
`checks.preflight.defaultTimeoutSeconds` were documented as the default a
request that names none gets. They never reached anything. Both CRDs default
the request field at admission (`maxTopics` 20 000, `timeoutSeconds` 120) and
the console writes both, so an operator who set either changed nothing.

- They are gone from `values.yaml` and the chart README.
- The controller's policy parser accepts both keys, or neither, and reads
  neither. It applies no range rule, only the type: any whole number from 0
  to 4 294 967 295 is accepted. A hand-written document that puts `null`, a
  negative, a fraction, a quoted number or a larger number there is refused
  whole, as before.
- The chart renders both at fixed values: 20 000 (or `hardMaxTopics`, if that
  is lower) and 120. A controller older than this one requires both keys, and
  this keeps the document readable to it.
- Nothing that runs changes: every install has always used the request's own
  values.

**Also changed: the retention worker refuses a cap it cannot read.**
`logweir-retention` now refuses a run whose `LOGWEIR_RETENTION_MAX_DELETIONS`
or `LOGWEIR_RETENTION_MAX_OBJECTS` is absent, or is not a whole number of at
least 1. It exits 3 and deletes nothing. Before, it silently used 50 and
20 000. Every controller that creates an enforcement Job sets both from
`spec.enforcement`, so a supported controller and runner never hit the
refusal.

**Do:**

- Remove either key from your values file.
- A `helm upgrade` that still carries one succeeds, because the schema still
  accepts both. It renders exactly what it would render without them, and its
  notes print `WITHDRAWN VALUES ARE SET AND IGNORED`.
- An upgrade with `--reuse-values` carries an older chart's defaults forward
  and prints the same warning. Upgrade once with `--reset-then-reuse-values`.
- To bound a discovery, name `maxTopics` on it; `checks.discovery.hardMaxTopics`
  still caps it. To give a slow cluster longer, name `timeoutSeconds` (30–600)
  on the `Preflight`.

**Scope:**

- `crates/weirkeeper/tests/chart_policy.rs`;
- `crates/logweir/tests/chart_lint.rs`;
- `scripts/check-chart.sh`, its withdrawn-values arm;
- `scripts/check-chart-values.sh`, one render per chart value;
- `crates/logweir-retention/tests/worker.rs`, the caps below the old
  defaults (7 and 1234), above them (75 and 30 000, through execution), and
  the refusals;
- `crates/weirkeeper/tests/retention_policy_controller.rs`, raised ceilings
  (55 and 30 000) reaching the plan and the Job;
- the mutants in the FX-10 report.

**What an operator sees after the upgrade.** An install that never set either
key keeps a byte-identical policy document, so its policy digest does not move
and no retained `Preflight` reads `policyChanged`. An install that had set
either key to a value other than the one the chart now renders (20 000, or
`hardMaxTopics` if lower, and 120) gets a changed document once: the chart
renders the fixed value in place of its own. Its policy digest changes, and
every retained `Preflight` whose `ready` verdict has not expired yet reads
`unknown`, its message naming `policyChanged`. Run the check again. Nothing
else changes, because the controller never read either value.

**Rollback:** `helm rollback` restores the previous chart's values and
document. Rolling back only the controller image is also safe, because the
document still carries both keys at values an older controller accepts.

#### 24. A `newTopic` restore's scorecard names the source settings it did not reconstruct (FX-3)

**Changed.** A restore creates its target topics at the plan's replication
factor, with `retention.ms=-1` and the target broker's `cleanup.policy`. The
signed scorecard labelled those deviations from the source
`intentionally_deviated` in every mode — right for a scratch drill, wrong for a
`newTopic` restore, whose scorecard therefore signed lost compaction and
replication factor 1 as intended. Scorecard format `1.2.0` names them in the new
`topic_parity.not_reconstructed` and also in `unexpected_divergence`, never as
intended, and `logweir drill verify`, `docs/verify_scorecard.py` 1.17.0 and
`logweir drill show` say so in words; for a `newTopic` scorecard signed before
1.2.0 they say its intended entries were not reconstructed. A scratch drill's
scorecard is unchanged apart from `format_version` and `not_reconstructed: []`.
No exit code or `outcome` of a scorecard this build signs changes: `topic_parity`
decides neither. A 1.2.0 scorecard whose lists contradict `not_reconstructed`,
such as a `newTopic` one that labels these settings intended beside
`not_reconstructed: []`, is refused by both readers (`drill verify` exit 4, the
script exit 1), and phase 8 never signs one.
**Do:** nothing on the upgrade. After a `newTopic` restore, read
`not_reconstructed` and apply the source's settings once the restore is
verified ([stability.md](stability.md#a-newtopic-restore-does-not-reconstruct-the-sources-topic-settings));
Logweir does not apply them yet (PROD-05). Re-read any `newTopic` scorecard
signed by an earlier build with the current verifier. Automation that parses
`intentionally_deviated` or `unexpected_divergence` should expect these entries
in the second list for `newTopic` runs; how the format change is classified is
in [stability.md](stability.md#format-120-fx-3-what-a-newtopic-restore-did-not-reconstruct).
**Scope:** unit rows for each of the four settings in both modes and through
the whole phase sequence (`crates/logweir/tests/verify_phase.rs`,
`orchestrator.rs`), the five arms in both readers with the invariant corpus
and the verifier-parity gate, and a live row on compose
(`e2e/tests/new_topic_parity.rs`: a compacted, replication-factor-3 source on
the `cluster3` profile restored as `newTopic` and as a drill, the broker's own
configuration as the oracle, and a pre-FX-3 binary's restore of the same point
for contrast). Readers built before FX-3 accept the 1.2.0 scorecards: `logweir`
and script 1.15.0 at main `b8b9263f`, FX-7's script 1.16.0, and `v0.1.5`
(measured).
**Rollback:** an older runner writes 1.1.0 scorecards with the old labels again.
The 1.2.0 scorecards already written stay valid under older and newer readers.

#### 25. The sign-in limit counts each client behind the trusted ingress, not the ingress (FX-13)

**Changed.** `/auth/login` and `/auth/callback` allow 20 requests a minute.
The count was kept per **socket peer**, and behind the shared console's
ingress every request has the ingress as its peer, so the limit was one budget
for everyone: about ten sign-ins a minute across all users, and one
unauthenticated client could block every sign-in for a minute at a time. The
count is now kept per **client**: when the socket peer is a trusted proxy
(`api.console.trustedProxyService` or `trustedProxyCidrs`, the same check the
`requireTrustedProxy` gate makes), the client is the rightmost
`X-Forwarded-For` hop that is not itself a trusted proxy; from any other peer,
or with no usable hop, it is the peer, as before. Only `X-Forwarded-For` is
read, never `Forwarded`. An IPv6 client is counted by its `/64`. There is no
budget over all clients, on purpose — a global one, at any height, is a
lockout for whoever holds enough addresses — and when 65,536 clients hold live
windows in one console process, a new client is served without a window
instead of refused. The forwarded address chooses a counter only; it is never
an identity, a grant or an audit actor ([api.md](api.md), *Rate limits*;
D0's amendment of 2026-10-07).

What an operator sees after the console image is upgraded:

- **Through the ingress, with a trusted proxy configured** (the PoC profile:
  `trustedProxyService: {namespace: traefik, name: traefik}`), **and the
  client's own address reaching the ingress**: one person's failed or repeated
  sign-ins no longer refuse anyone else's. If kube-proxy or a load balancer
  replaces client addresses before Traefik (the Traefik chart's default
  `externalTrafficPolicy: Cluster` on a multi-node cluster, a load balancer in
  instance mode), clients through one node still share its budget: keep the
  address with `externalTrafficPolicy: Local`, the PROXY protocol, or an L7 hop
  trusted at both Traefik and the console.
- **New audit notes** on sign-in requests: `loginRateKey` (`forwardedClient`
  or `peer`, which counter the request spent) and, only when the table is
  full, `loginRateUntracked: tableFull`, with at most one warning a minute in
  the console log.
- **No trusted proxy configured:** nothing changes; the ingress is the peer,
  and its one budget is still everyone's.

**Do:** if sign-ins are refused `429` behind the ingress after the upgrade,
check that the ingress is a trusted proxy (`trustedProxyService` naming the
ingress controller's Service) — the audit's `loginRateKey: peer` on a request
that came through it says it is not. Keep the ingress from passing a client's
own `X-Forwarded-For` through: Traefik's `forwardedHeaders.trustedIPs: []`
with `insecure: false` (its default, and `deploy/poc/traefik.values.yaml`)
deletes it. Check `trustedProxyCidrs`: without `requireTrustedProxy` a range
has no width floor, and a range wider than the ingress's pods now lets a
client in it choose its sign-in budget, including another client's. And plan
for the residual this design leaves at the identity provider: sign-in
requests reach it authenticated as this console, so a many-address attack can
spend the provider's per-client quota for it. Set that quota generously,
alert on `loginRateUntracked` and on the warning `a sign-in was refused` that
names `the provider answered HTTP 429`, and keep the in-cluster administrator
mode (`kubectl port-forward`) as the break-glass path ([api.md](api.md),
*Rate limits*).
**Scope:** `crates/logweir-api/src/http.rs` (`login_rate_key`, six unit rows),
`crates/logweir-api/src/auth/ratelimit.rs` (the `/64` fold and the NAT64
prefix, no global budget, the 65,536-key bound with its memory, the sweep rate
and the once-a-window warning), and ten rows through the real router in
`crates/logweir-api/tests/entry_point.rs` (two clients, one client through two
proxies, a forged header from an untrusted peer, an unread and a stale proxy
Service, a chain with no client hop, an IPv6 `/64`, NAT64 clients, a spray
past the table, 40 clients × 20 requests all served, and a refused exchange
naming the provider's status), with the mutants of the FX-13 report and its
review killed. Not
yet proven live: the PoC upgrade that carries FX-13 runs two clients through
Traefik. **Rollback:** an older console image keys the limit on the socket
peer again — one budget behind the ingress — and writes neither audit note.
Nothing is stored; the counters are in memory.

#### 26. A Job pod the namespace refuses is reported at once, in the admission's words, by every kind (FX-11)

**Changed.** A pod a `ResourceQuota`, a `LimitRange`, an admission webhook or a
missing ServiceAccount refuses at creation leaves one trace: the Job
controller's `FailedCreate` event on the Job. Only a `Backup`'s and a
`Restore`'s runner, a `Preflight` and an evidence fetch read it. The other
Job-owning controllers read no events, so their objects waited out the Job's
own deadline and then named only that: `PodNotStarted` and `DeadlineExceeded`
on a `TopicDiscovery` and a catalog sync, `Resolving` and then
`DiscoveryFailed` on a dynamic `Backup`, `ProbeRunning` for two minutes and
then `NoExitCode` on a `KafkaCluster`, "the delivery Job finished with no exit
code" three times on a `ProtectionPolicy`, and `RunFailed` "produced no exit
code" on a `RetentionPolicy`. Every one of them now reads the event through the
shared waiting classifier. Once the Job has had no pod for 30 seconds (the
`Preflight` grace) and the event says why, the Job is cancelled and the object
names the refusal with the admission's own words: `PodCreateRejected` for the
check kinds, and `PodCreationForbidden` for the runner kinds. That is the
`KafkaCluster`'s `Reachable` condition and `status.reason`, a delivery attempt's
`lastError`, a retention run's `Enforced` condition, and a dynamic `Backup`'s
terminal state ([kubernetes.md](kubernetes.md) §12, *The runner's requests
and limits*, lists each kind).

What changes on the upgrade:

- **New values where alert rules and dashboards match.** A `KafkaCluster` can
  carry `status.reason: PodCreationForbidden` where it carried `NoExitCode`, a
  `RetentionPolicy` can carry `Enforced=False/PodCreationForbidden` where it
  carried `RunFailed`, and a dynamic `Backup` whose discovery pod is refused
  ends `PodCreationForbidden` where it ended `DiscoveryFailed`.
- **A schedule no longer retries a refused discovery.** `DiscoveryFailed` is
  retried by a `BackupSchedule`'s `spec.retry`; `PodCreationForbidden` is
  not, exactly as for a `Backup` whose runner pod is refused. The next slot
  runs normally.
- **A probe that produced no verdict clears `reachable` and is probed again**
  (PoC batch 1, O-1). A probe Job that crashed, lost its pod, was refused, or
  printed no contract line used to leave an earlier `reachable: true` beside
  `Reachable=Unknown`, and a crashed or refused one was never replaced, so the
  connection was never probed again (all twelve PoC connections, for a week).
  Now `reachable` is cleared (`clusterId` is kept, as the identity last
  observed), the finished Job gets the usual five-minute TTL, and the next
  probe runs on the ordinary cadence. A refusal never writes `observedAt`.
  **A `Restore` or a rehearsal against a cluster whose newest probe could not
  vouch for it is now refused `ClusterNotReachable`** where a stale `true`
  used to admit it; the next successful probe, within about five and a half
  minutes, admits it again. On the upgrade, every connection stuck behind a
  terminal probe Job reads `reachable` cleared on its first pass and is
  re-probed within one cadence.
- **A `RetentionPolicy` that declares a provider rule clears an earlier
  evaluation.** `mode: ExternalLifecycle` writes `Evaluated=Unknown`; a policy
  that was `Report` or `Enforce` before kept its `lastEvaluation` beside it,
  so the console showed a plan preview this mode never makes. It is now
  cleared, and the next evaluation (in `Report` or `Enforce`) writes a fresh one.
- **Retries stay bounded.** A refused delivery is a failed attempt, so it is
  retried by the ordinary backoff, three attempts in all. A refused retention
  run counts toward `EnforcementDegraded`, so three in a row stop scheduling.
- **The cost.** One `list` of core `events`, by `involvedObject.uid` with
  `limit=20`, per pass of a Job that has had no pod for 30 seconds. A Job
  whose pod exists costs nothing. The `list` on `events` is the grant the
  `weirkeeper` role has carried since `Preflight`, bound in every namespace
  the controller acts in under both binding modes.

**Do:** see which namespaces refuse Logweir pods today. Each line is a Job the
Job controller could not give a pod:

```bash
kubectl --context <ctx> get events -A --field-selector reason=FailedCreate \
  -o custom-columns=NS:.metadata.namespace,JOB:.involvedObject.name,WHY:.message
```

Before the upgrade, list the connections whose `reachable` rests on an
earlier probe than their newest verdict: `kubectl --context <ctx> get
kafkaclusters -A` and look for a `REACHABLE` value beside a reason other than
`Reachable` or `ProbeReportedUnreachable`. Each one is cleared on the
upgrade and re-probed; a `Restore` waiting to be admitted against it waits
for that probe. Give each namespace a `LimitRange` default, or room in its `ResourceQuota`,
for Jobs that state no resources. Add `PodCreationForbidden` wherever an alert
rule or dashboard matches `NoExitCode`, `DiscoveryFailed` or `RunFailed` on
these kinds. A schedule that relied on `spec.retry` to ride out quota
contention needs the quota fixed instead. **Scope:**
`crates/weirkeeper/tests/topic_discovery_controller.rs` (a quota and a
`LimitRange` refusal, the Job cancelled), `recovery_catalog_controller.rs` (the
running pass and the harvest of the cancelled Job), `backup_selection.rs` (the
`Backup` ends `PodCreationForbidden` and creates no runner Job),
`kafka_cluster_controller.rs` (`reachable` cleared, `observedAt` untouched,
the status before the TTL, a crashed Job replaced), `protection_controller.rs` (cancel, status, TTL, and the third attempt is
the last) and `retention_policy_controller.rs` (named at once, harvested with its lease released only after the cancelled Job has finished, counted, and degraded
on the third). Each row has a negative control with no event, or another Job's,
which keeps the path from before, and `check_framework.rs` pins both grace
boundaries and the no-pod pre-filter. Planted mutants were each killed (FX-11).
Not yet proven live: the PoC upgrade that carries FX-11 runs its rows.
**Rollback:** an older controller reads no events in these kinds again, and
refused pods wait out their Job's deadline as before. An object this build
ended (a `TopicDiscovery`, a `Backup`) keeps its `PodCreateRejected` or
`PodCreationForbidden`. A `KafkaCluster`, a delivery and a retention policy
are rewritten by the older controller's next verdict; an older controller
leaves a crashed probe's Job in place again, and an ExternalLifecycle policy's
cleared `lastEvaluation` stays absent until its next evaluation. Nothing has to be
deleted, and the role is unchanged.

#### 27. A point-in-time restore of a `LogAppendTime` topic is refused unless its plan selects by producer time (FX-8)

**Changed.** The archive holds each record's PRODUCER timestamp, so a
point-in-time restore of a topic on `message.timestamp.type=LogAppendTime`
selected records by the producers' clocks and was signed `pass`: PROD-01.1
restored, at a recovery point in 2001, six records the broker had appended in
2026. The runner now refuses a plan that selects such a topic by time — it
states `restore.point_in_time`, or its `sample.window_end` cuts the archive —
with exit 3 and `refusal-reason=PointInTimeByProducerTime`, after the archive
is described and before any target topic is created. The topic counts as
`LogAppendTime` when the archive manifest records that topic override, or when
the bound, verified backup receipt recorded it as the topic's effective value
(a broker-wide default; receipts since FX-4). A plan that states
`restore.time_basis: producerTime`, approved with it, runs, and its signed
scorecard — format **1.3.0** — lists the topic under
`source.time_basis.producer_time`. A topic selected by time whose type nothing
recorded runs and is listed under `source.time_basis.not_recorded`. Full
restores still run. `logweir drill verify`, `verify_scorecard.py` 1.18.0 and
`logweir drill show` print the label; both verifiers also say, for a backup
receipt, which topics it records as `LogAppendTime`. The controller copies the
signed label onto `Restore.status.timeBasis` (a new optional status field) and
the product API serves it as `timeBasis`; the console's Restore detail shows the
signed lists and warns about a topic whose type was not recorded. The console's
restore wizard offers the opt-in, shows it on the review step and warns when a
plan takes it
([stability.md](stability.md#a-point-in-time-over-a-logappendtime-source-is-refused-unless-the-plan-selects-by-producer-time),
[drill-spec.md](formats/drill-spec.md#restoretime_basis-fx-8),
[the scorecard format](formats/drill-scorecard.md#sourcetime_basis-format-130)).

What changes on the upgrade:

- **A point-in-time `Restore` of a `LogAppendTime` topic** that ran before is
  refused after the runner image is upgraded: `Failed`, exit 3, `exitReason:
  PointInTimeByProducerTime`, nothing created. The remedy is a new plan with
  `restore.time_basis: producerTime` and a new approval, when restoring by the
  producers' clocks is what you want.
- **A `RehearsalSchedule` over such a topic** that states no
  `spec.point.timeBasis` has every slot refused the same way, and records
  `lastFailed.reason: PointInTimeByProducerTime` with `RehearsalHealthy=False`.
  The new optional `spec.point.timeBasis: producerTime` renders the opt-in into
  every slot's plan; it is inside `templateDigest`, so it takes a new schedule
  and a new standing authorization ([kubernetes.md](kubernetes.md) §7g). The CRD
  gains the field: apply the CRDs before the controller rolls.
- **A scorecard written by the new runner is format 1.3.0.** Readers built
  before FX-8 accept it and ignore the block.

**Do:** before the runner image rolls, find the source topics your
point-in-time restores and rehearsals name that are `LogAppendTime`, by topic
override or by the broker's default. With `--all` the describe prints every
topic's EFFECTIVE value, a broker default included (its synonym reads
`DYNAMIC_DEFAULT_BROKER_CONFIG`), so this one command names both kinds:

```bash
kafka-configs.sh --bootstrap-server <source> --describe --entity-type topics --all \
  | awk '/configs for topic/ {t=$5} /^ *message.timestamp.type=LogAppendTime/ {print t}'
```

For each, decide whether a restore by the producers' clocks is acceptable; if
it is, add `restore.time_basis: producerTime` to the plan and re-approve it.
For a `RehearsalSchedule` over such a topic, create a new schedule with
`spec.point.timeBasis: producerTime` and sign its authorization, or suspend the
old one. **Scope:**
`crates/logweir-core/src/time_basis.rs` (both arms, the opt-in, the unknown
case and the `CreateTime` control), `crates/logweir/tests/orchestrator.rs`
(refused before any target topic, the label, the opt-in inside the approved
bytes, the broker-default arm), the four scorecard arms in both readers with
the invariant corpus and the parity gate, the console rows, and planted
mutants, each killed (FX-8). Live, on the compose stack:
`e2e/tests/record_semantics.rs::log_append_time_source_versus_restored_output`
and `e2e/tests/config_coverage.rs::fx8_a_broker_default_log_append_time_is_refused_from_the_bound_receipt`.
Not yet proven on the PoC: the upgrade that carries FX-8 runs those rows.
**Rollback:** an older runner ignores `restore.time_basis` and runs the
selections this build refuses, unlabelled, signing format 1.1.0 again. The 1.3.0
scorecards already written stay valid under both readers.

### Verification scope: what "verified" means in this release

- **A green badge** means the signed document's signature verified under a key
  the namespace's trust accepts **and** the run's own success field
  (`exitCode == 0` for a Backup, `outcome == pass` and a zero `exitCode` for a Restore; a failed
  restore's signed scorecard is published and verified, and is never green and never gets the
  completion panel — upgrade the controller before the runner). `Historical`
  (signed before the key was retired) is a pass; `RecordedBeforeRevocation` never
  is ([kubernetes.md](kubernetes.md) §15.2–15.2a).
- **Record checks are samples.** `verificationScope` is `sampled`, `degraded` or
  `none`, never `complete`; no level in this version compares every record
  (`recordsSampled`, `recordsSampledMatching` and the window beside them are the
  exact claim).
- **The independent verifier** (`docs/verify_scorecard.py`) checks the same
  signed documents with no Logweir code ([verify-a-scorecard.md](verify-a-scorecard.md)).
- **Tested environments** are named in [release-handoff.md](release-handoff.md):
  docker-desktop Kubernetes with a SCRAM (and private-CA TLS) Kafka and MinIO;
  the PoC profile on docker-desktop (Traefik, cert-manager, Dex, the shared
  console) from the published chart and images; and the GitHub Actions Compose
  suite. Nothing here was run against AWS S3, MSK, EKS, a corporate identity
  provider or a NetworkPolicy-enforcing CNI.

### Retention authority

- **A schedule's `spec.retention` only reports.** It prints the commands an
  operator would run and deletes nothing.
- **A `RetentionPolicy` is created in `mode: Report`** and deletes nothing.
  Only `logweir-retention-admin` — namespaced, and bound to somebody who does not
  hold `logweir-operator` — may move it to `Enforce`.
- **`Enforce` deletes only when all four gates hold**: the mode, an
  administrator's `approvedPlanSha256` equal to the current plan digest and
  younger than `planMaxAgeSeconds`, a lease written before a cluster-wide
  `Restore` check, and a worker that re-validates every key against the
  policy's prefix. The worker runs under its own delete-capable credential,
  never under `logweir/`, and writes create-only tombstones and a record
  (unsigned in this build) under `logweir/retention/`
  ([kubernetes.md](kubernetes.md) §7f).
- **`ExternalLifecycle` is a declaration**: the bucket's rule deletes, the bucket
  wins, and Logweir verifies nothing about it.
- **A `Restore` created after a run's cluster-wide check and before its first
  delete is not held by the lease** in this build; suspend enforcement
  (`mode: Report`) around a large restore.

### Migration and rollback

**Upgrade order:** identity backup → retention grants and modes (items 1–2) →
the `runnerResources` inventory (item 21) → CRDs (all fourteen established) →
controller **and** runner image together → console image → approval-policy
binding. Every CRD change is additive; nothing
is converted and no stored object is rewritten
([install.md](install.md), *Upgrade CRDs before upgrading the controller*).

**Before a rollback**, in this order:

1. Set every `RetentionPolicy` to `mode: Report`, wait for `status.lease` to
   clear and any enforcement Job to finish (item 2; [kubernetes.md](kubernetes.md)
   §7f, *Upgrade and rollback*).
2. Delete `Preflight` objects with `operation: SourceConnection` — an older
   controller cannot decode them and stops reconciling every `Preflight`
   ([kubernetes.md](kubernetes.md) §21.0).
3. If rolling back past Amendment G, delete every `Approval` whose
   `spec.subjectRef.kind` is `RehearsalSchedule` first ([kubernetes.md](kubernetes.md)
   §12, *The one widening that is NOT rollback-safe*).
4. Let destination-backed and `v2`-frozen `Backup`s finish; an older controller
   refuses them terminally rather than running them ([kubernetes.md](kubernetes.md)
   §10, *Backups created under the previous execution contract*). Rolling the
   runner back to a build without the execution claim, let EVERY in-flight
   `Backup` finish first (item 11): on an unversioned bucket a re-created Job's
   older runner rewrites the set's segments under an unchanged manifest, and no
   check reports it.
5. Let point-bound `Restore`s that have no Job yet reach one, or recreate them
   after the rollback: an older controller re-renders their approval bundle
   without the evidence keyring and ends them `ApprovalBundleConflict` (item 5;
   fail-closed, nothing is restored).
6. Unbind approval policies, or expect not-yet-admitted v2 approvals to be
   refused (item 7).
7. Roll the controller and runner back together (the runner not after the
   controller: item 12), with no `Restore` running, and leave the CRDs in place.
8. **Rolling back to a chart that did not render an object this one adopted
   deletes it.** `helm rollback` to `v0.1.5` removes the runner ServiceAccount
   and the `logweir-s3` Secret the upgrade adopted in each runner namespace
   (they were hand-provisioned at `v0.1.5`); re-create them before the next run,
   or every run fails `serviceaccount "logweir-runner" not found` (found by the
   PoC install's upgrade rehearsal R1, 2026-09-24).
9. **Take every compromised key out of the roster first** (item 16). An older
   controller cannot carry a `KeyCompromise` revocation across policies, and
   `v0.1.5` cannot express one at all: re-create `TrustRoster/default` without
   every key any `TrustPolicy` revoked for `KeyCompromise` (a policy's
   `CompromiseGuard` message says "TrustRoster/default still lists …" while
   one is there), and record each such revocation on every policy that still
   lists the key (`CompromiseInherited`). Do not delete a policy to "go back to
   the roster" before that: it is held until nothing lists the key.
10. **Suspend every `RehearsalSchedule` this build skips for its
    `runnerResources`** (item 21) that must not run uncapped: an older
    controller drops the block and fires its next slot. A `Restore` this build
    refused `ExecutionSpecInvalid` stays `Failed`.

**How this upgrade is rehearsed.** From `v0.1.5` (the last version tag: 6 →
14 CRDs, the managed identity adopting a hand-provisioned signer, the console
arriving) and from `sha-f49849d…` (the last build before `ac00819`), each to
the first PoC publication `86a554e6`, which crosses items 1–13, and each rolled
back. The running install was then upgraded in place five times: to
`02dc44b6` (items 14, 15 and 17), to `b748fd5f` (16, 18 and 19), to
`a54fb823` (20), to `815249cb` (no item: a console-only fix, P15, and no
CRD change) and to `fdb48cd8` (no item: console-only fixes, P16 and O2, and
no CRD change). [release-handoff.md](release-handoff.md) names the chart and
image digests, the state each rehearsal set up first, and what each round
showed. An upgrade from `sha-7b0277b…` crosses items 1–4 and 11–20. An upgrade
from `fdb48cd8` crosses items 21, 22, 23, 24, 25, 26 and 27, and item 11's FX-7 additions:
grant `s3:GetObjectVersion` before the upgrade, or a pinned point whose current
version differs fails closed at the binding, and let in-flight Backups finish
before rolling the runner back.

**The chart and the images move together.** This chart's controller probes run
`weirkeeper --probe`, and its console configuration can carry
`oidc.caBundleFile` and `trustedProxyService`: a controller image older than the
chart fails its liveness probe (and is restarted), and an older console refuses
the configuration (exit 2, `unknown field`). The published chart names its own
commit's images, and `helm rollback` restores the previous chart and images
together; pin all four images to one build whenever you override them.

Archives, evidence and catalog records are untouched in both directions; old
signed archives keep verifying as long as their public keys stay in the trust
policy or roster ([keys.md](keys.md)).

### Limitations and open items

- **The live half of PLAT-20.2 ran on 2026-09-24 and 2026-09-25** with the PoC
  profile on docker-desktop, from published charts and images only: a clean
  install at `86a554e6`; upgrades to it from `v0.1.5` and from `sha-f49849d…`,
  each with a rollback, that kept installation identities, schedules and
  archive readability, with a restore of a pre-upgrade point after each; then
  five in-place upgrades of the running install, to `02dc44b6`, `b748fd5f`,
  `a54fb823`, `815249cb` and `fdb48cd8`, after which all 356 of its backup
  receipts still passed the independent verifier
  ([release-handoff.md](release-handoff.md)).
  [UNVERIFIED — R2's pre-upgrade retention, mount-failure and point-bound-restore states were not set up.]
- **The large catalog was measured live at 258 real points, not 1,000.** The
  host could not run more runner pods: the `amd64` runner runs under
  emulation there, and manual runs had no bound before item 15. The timings at
  258 points, and the offline rows at 1,000 and 5,000 rows, are in
  [stability.md](stability.md#measured-scale-limits-plat-202); the console
  refuses a list longer than 5,000 rows rather than showing a prefix.
  [UNVERIFIED — a 1,000-point archive was not reached live; 258 real points were measured on docker-desktop.]
- **P15, fixed in `815249cb` and proven live:** a readiness check slower than
  the console's old follow (40 s on the Schedules page, 30 s for *Test
  connection*, 60 s for *Test access*, 90 s at restore step 5; *Discover
  topics* had none) was left "not finished" for good. Every follower now reads
  its check until the longest time a check may take (12 minutes; a discovery
  7), backing off from 2 s to 10 s between reads, and says "did not finish" —
  "not cancelled", with *Run the check again*, which starts a new check — if
  that passes ([ui/README.md](../ui/README.md)). On the PoC
  (`claude/poc-upgrade-4`), checks of 100–170 s were read to their verdicts
  without a reload on the five readiness followers, *Discover topics* now
  settles on the page, and the product API's request log showed the cadence. The 12-minute "did not finish" state was left to the
  offline rows (`ui/tests/check-deadline.spec.js`).
- **P16, fixed in `fdb48cd8` and proven live:** at restore step 5 the first
  repaint of a running check scrolled the page to its top, so on a 390 px
  screen the focused status line was off screen until the verdict landed.
  A repaint of the view on screen now keeps the reader's place (scroll and
  focus), the status is kept above the Back/Next bar, a reader who scrolled
  away is left where they are, and Next, Back and a `&step=` link open a step
  at its heading ([ui/README.md](../ui/README.md)). On the PoC
  (`claude/poc-upgrade-5`, 390×844) the page and the focused status held
  still through every repaint of a running check, where the same rows on
  `815249cb` had seen it jump to the top; *Test connection* and *Discover
  topics* kept the page, and *Test access* and the schedule form kept focus.
  The same publication prints `logweir catalog list` on the Catalog as code,
  not between backticks (O2).
- **A followed check stops at the ingress's own `503` (P17, console, open).**
  A follow tries past five failed reads of the API, which covers a restart,
  but when no console pod is ready the ingress answers `503` with a plain-text
  body, which the page reads as a contract failure and stops at once: the
  check shows "could not be read again" with *Run the check again*, though it
  goes on to finish in the cluster. Run the check again, or reload once the
  console answers. Found by the fifth PoC round on `fdb48cd8`, when a loaded
  host failed both console pods' readiness probes.
- **The demo MinIO is a rebuilt mirror.** MinIO withdrew its public images
  (Docker Hub on 2026-09-11; `quay.io` refuses anonymous pulls since
  2026-09-24). The chart's demo MinIO, the e2e stack and the PoC run the same
  MinIO and `mc` releases rebuilt from the archived upstream source,
  `docker.io/vladyslavhaina/minio-mirror` and `mc-mirror` (AGPL-3.0). Replacing
  MinIO with a maintained, permissively licensed S3 server is an open task
  (REPLACE-MINIO), not started.
- **A console restore's replication factor does not start from the
  source's** (item 22). The source's factor is recorded only in the archive
  manifest; the default is the target's broker count, at most 3, until
  PROD-05.1 projects the source's factor to the console.
- **The `v0.1.1`–`v0.1.5` image tags on Docker Hub are leftovers of failed
  runs, not releases.** Those tag runs pushed version-tagged images before
  they failed — `logweir:v0.1.1`–`v0.1.5`, `weirkeeper:v0.1.2`–`v0.1.5` and
  `logweir-ui:v0.1.3`–`v0.1.5`; no console image and no chart — and none of
  them published a chart, a GitHub Release or a release drill. Every run
  failed in the CLI build matrix; `v0.1.1`'s image job also failed its own
  repository-digest check after pushing, and the pull-back jobs of `v0.1.2`
  and `v0.1.3` failed with "cannot overwrite digest". Do not install or pin
  them. The owner decided on 2026-10-07 to delete them all once v0.2.0 ships. The repaired pipeline
  never builds an image under a version tag: it tags main CI's `sha-<commit>`
  images.
- **The CLI archives' run-time needs.** The Linux archives are built in the
  runner image's builder base (`rust:1.89-bookworm`), so they need what the
  runner image installs: a glibc at least as new as the version measured on
  each binary — 2.34 on the Linux arm64 archive built locally on 2026-10-05;
  each release's notes give its own — which is never above the Debian 12
  glibc (2.36) they are built against, so Debian 12's glibc or newer always
  suffices; and `libssl.so.3`, `libcrypto.so.3`, `libsasl2.so.2` and
  `libz.so.1` (Debian and Ubuntu: `libssl3`, `libsasl2-2`, `zlib1g`). A
  distribution whose SASL library has another soname
  (`libsasl2.so.3` on RHEL and Fedora) builds from the checkout. The macOS
  archive needs Homebrew's `openssl@3`. `logweir --version` prints the
  workspace version (`0.1.0`), not the tag; `release.json` ties each archive
  to its tag and commit.
- **The product API's OpenAPI document is `1.0.0-alpha.2`, still a
  pre-release**, although the console image and the chart consume it; ship and
  upgrade the console and the API together until PROD-14.2 freezes it at
  `1.0.0` ([stability.md](stability.md)). Since `1.0.0-alpha.1` (2026-09-16,
  never published) it gained 38 operations and removed none: destinations
  (list, create, read, usage, test, update access, adopt from legacy), catalogs
  with their points and signers, topic discoveries, preflights, the operation
  event stream, schedule updates, manual backups, the restore approval
  submission, read-only protection, rehearsal, retention and trust policies,
  the namespace's approval policy, cadence previews, and the shared console's
  sign-in routes (`/auth/login`, `/auth/callback`, session logout). Its
  component schemas grew from 66 to 258 (FX-8 added `RestoreTimeBasisView`,
  served as a restore's optional `timeBasis`); none was removed.
- **No in-place runner signing-key cutover** ([keys.md](keys.md), step 2 of
  *The supported procedure*).
- **Restore admission does not hold on a retention lease** (above).
- The standing limitations of [stability.md](stability.md), *Known limitations*,
  and the `[UNVERIFIED]` marks it carries (AWS S3 create-only puts, MSK, the
  NetworkPolicy, the 1.30+ admission policy) are unchanged.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
