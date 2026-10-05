# Mapping Logweir's evidence to backup and restore-testing controls

Auditors, GRC teams and control owners ask what a Logweir restore test proves.
This page answers clause by clause for six texts that ask for backups that are
tested by restoring them, with the results recorded:

| Text | Clauses mapped here |
|---|---|
| [DORA](#dora-regulation-eu-20222554), Regulation (EU) 2022/2554 | Article 11(6); Article 12(1), (2), (3), (6) and (7) |
| [DORA's RTS on ICT risk management](#the-dora-rts-commission-delegated-regulation-eu-20241774), Commission Delegated Regulation (EU) 2024/1774 | Article 25, and Article 40 for the simplified framework |
| [ISO/IEC 27001:2022](#isoiec-270012022-annex-a-control-813) | Annex A, control 8.13 |
| [SOC 2](#soc-2-trust-services-criterion-a13), the AICPA Trust Services Criteria | A1.3 |
| [NIS2's implementing regulation](#nis2-commission-implementing-regulation-eu-20242690), Commission Implementing Regulation (EU) 2024/2690 | Annex, point 4.2 |
| [The HIPAA Security Rule](#hipaa-45-cfr-164308a7iid) | 45 CFR 164.308(a)(7)(ii)(D) |

For each clause it states what Logweir's evidence supports, citing the exact
field, and what the evidence does not show, naming the gap.

**This page makes no compliance claim.** It does not say that Logweir, an
installation of it, or an organisation that runs it answers any clause. That is
for the organisation, its auditor and its regulator to judge, against the
organisation's own policies, scope and risk assessment. Logweir's evidence is
one input to that judgement: it can support a control owner's statement that a
restore was tested and what the test found. Every clause below also asks for
things no restore tool records, such as policies, impact analyses, management
review and corrective action, and the page says where.

**Verify a document before you rely on a field in it.** A field read from a
document whose signature you have not checked, under a key you have not
authenticated independently, is a claim and not evidence.
[Verifying a Logweir drill scorecard](verify-a-scorecard.md) shows how, and its
section [What the scorecard does **not** claim](verify-a-scorecard.md#what-the-scorecard-does-not-claim)
is the companion to this page.

## How to read this page

- Every clause section has three parts: the clause, quoted from the official
  text (ISO/IEC 27001 is cited by number and title only, because its text is
  not public); a table of **what the evidence supports**, in which every row
  cites the fields it rests on; and **what it does not show**, a list in which
  every item links to a gap under
  [What the evidence does not show](#what-the-evidence-does-not-show).
- A field is cited as the document, linked to the page that defines its
  fields, then the field's JSON path in that document, for example
  [Scorecard](formats/drill-scorecard.md): `integrity.result`. The six
  documents are described in [the next section](#the-evidence-this-page-cites).
- `crates/logweir/tests/control_evidence.rs` checks every cited field against
  the definition of the document it names, refuses a supported row that cites
  no field, and refuses wording that would turn evidence into a verdict about an
  organisation. When a field is renamed or removed, that test fails until this
  page is updated.
- The clause texts were read on 2026-10-05. [Sources](#sources) gives the URLs,
  and records where a citation had to be made precise.

## The evidence this page cites

| Document | Signed by | What it records | Fields are defined in |
|---|---|---|---|
| Scorecard | The runner's evidence-signing key (under Kubernetes, the installation identity), in a DSSE sidecar | One restore drill or scheduled rehearsal: the archive, the target, the approval, the measured RTO and RPO, and a sampled integrity check | [Scorecard format](formats/drill-scorecard.md) |
| Put receipt | The same key | What the store reported when the scorecard was uploaded | [The storage receipt](verify-a-scorecard.md#the-storage-receipt-a-second-signed-document) |
| Backup receipt | The same key | One backup run: the source cluster, the topics, per-topic record counts, the covered window and the manifest digest | [Backup receipt format](formats/backup-receipt.md) |
| Catalog point | The key named in its `signing.key_id`; the backup receipt it points at is the verification root | One recovery point: when its capture started, its window and where its archive is | [Catalog point format](formats/catalog-point.md) |
| Standing authorization | A human approver's key with usage `GovernedApproval` | The scope inside which one rehearsal schedule may run, for at most 90 days | [The standing rehearsal authorization](stability.md#the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature); [Kubernetes §7g](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization) |
| RehearsalSchedule | Nothing: Kubernetes status written by the controller | The cadence and objectives of scheduled rehearsals, and the last pass, failure and skipped slot | [Kubernetes §7g](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization); [CRD schema](../config/crd/rehearsalschedules.yaml) |

Three reading notes:

- **A rehearsal writes an ordinary scorecard.** In it, `approval.approver`
  reads `standing-authorization/<schedule>` rather than a person's name,
  because what a human signed was the schedule's scope, and `triggered_by`
  reads `rehearsal/<schedule>/<slot>`.
- **RehearsalSchedule status is an index, not evidence.** It is unsigned and
  keeps only the latest pass, failure and skip. `status.lastSucceeded.evidence`
  names the signed scorecard, which outlives the status and the `Restore`.
- **A backup receipt or a catalog point says what was captured. Only a
  scorecard says that something was restored and checked.**

## DORA: Regulation (EU) 2022/2554

DORA applies to the financial entities listed in its Article 2. Articles 11 and
12 belong to its ICT risk management chapter. For the entities listed in its
Article 16(1), Articles 5 to 15 do not apply and a simplified framework does;
for them, read [Article 40 of the RTS](#article-40-the-simplified-framework).

### Article 11(6): test the plans at least yearly

> As part of their comprehensive ICT risk management, financial entities shall:
> (a) test the ICT business continuity plans and the ICT response and recovery
> plans in relation to ICT systems supporting all functions at least yearly, as
> well as in the event of any substantive changes to ICT systems supporting
> critical or important functions;

The second subparagraph asks entities other than microenterprises to include in
the testing plans "scenarios of cyber-attacks and switchovers between the
primary ICT infrastructure and the redundant capacity, backups and redundant
facilities necessary to meet the obligations set out in Article 12". The third
asks for a regular review of the policy and plans, "taking into account the
results of tests".

| What the evidence supports | Fields |
|---|---|
| A restore from a named archive was tested at a recorded time, and the test recorded its own outcome. | [Scorecard](formats/drill-scorecard.md): `run_id`, `requested_at`, `outcome`, `source.backup_id`, `source.manifest_sha256` |
| The test ran a plan approved before it ran: a per-run approval signs the exact plan bytes, and a rehearsal's plan was proven to fall inside a scope a human signed for that one schedule. | [Scorecard](formats/drill-scorecard.md): `approval.approver`, `approval.ticket`, `approval.approved_at`, `approval.plan_hash`, `approval.key_id`; [Standing authorization](stability.md#the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature): `subjectRef.uid`, `scope.templateDigest`, `issuedAt`, `expiresAt` |
| Scheduled rehearsals recur on a configured cadence; the last pass is recorded, and so are the last failure and the last refused slot, with their reasons. | [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `spec.schedule`, `status.lastSucceeded.at`, `status.lastFailed.reason`, `status.lastSkipped.slot`, `status.lastSkipped.reason` |
| The restore software is identified by version and image digest, so tests before and after a change to it can be told apart. | [Scorecard](formats/drill-scorecard.md): `engine.version`, `engine.digest` |

**What it does not show**

- That the tests covered the systems supporting all functions, ran at the
  cadence the plans require, or fed a review of the plans:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).
- That a business continuity plan or a response and recovery plan was
  exercised. A Logweir test restores Kafka topics from an archive and checks
  them, which is one step such a plan may contain:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).
- A switchover of applications to the restored data:
  [consumer positions and application recovery](#consumer-positions-and-application-recovery).
- Who held the approving and signing keys, and when they signed:
  [key custody](#key-custody).

### Article 12(1) and (2): backup policies, and periodic tests of backup and restoration

> 1. \[…\] financial entities shall develop and document:
> (a) backup policies and procedures specifying the scope of the data that is
> subject to the backup and the minimum frequency of the backup, based on the
> criticality of information or the confidentiality level of the data;
> (b) restoration and recovery procedures and methods.
>
> 2. \[…\] Testing of the backup procedures and restoration and recovery
> procedures and methods shall be undertaken periodically.

| What the evidence supports | Fields |
|---|---|
| Each backup run names the topics it was asked to capture, counts the records it captured per topic, and states the window it covers. | [Backup receipt](formats/backup-receipt.md): `source.topics`, `records`, `covered.from_ms`, `covered.to_ms` |
| How often backups actually ran can be read from the recovery points, each dated by the start of its capture. | [Catalog point](formats/catalog-point.md): `point_id`, `capture.started_at`, `capture.finished_at` |
| A backup names its archive manifest only when the engine exited 0, and the manifest digest is taken over the bytes read back from the store. | [Backup receipt](formats/backup-receipt.md): `exit_code`, `archive.manifest_key`, `archive.manifest_sha256` |
| A restore from such a backup was tested, and its result recorded. | [Scorecard](formats/drill-scorecard.md): `source.backup_id`, `source.manifest_sha256`, `outcome`, `integrity.result` |

**What it does not show**

- The backup policy itself: which data, how often, and why. A schedule is the
  operator's configuration, and the receipts show what ran:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).
- That a backup captured everything the source held:
  [archive-relative, not source-relative](#archive-relative-not-source-relative).
- That topic configuration, consumer positions or schemas were backed up:
  [configuration coverage](#configuration-coverage),
  [consumer positions and application recovery](#consumer-positions-and-application-recovery),
  [schema registries](#schema-registries).

### Article 12(3): restore into segregated systems

> When restoring backup data using own systems, financial entities shall use ICT
> systems that are physically and logically segregated from the source ICT
> system. The ICT systems shall be securely protected from any unauthorised
> access or ICT corruption and allow for the timely restoration of services
> making use of data and system backups as necessary.

| What the evidence supports | Fields |
|---|---|
| The restore went to a cluster identified by the id that cluster reports, in a recorded target mode. | [Scorecard](formats/drill-scorecard.md): `target.cluster_id`, `target.mode` |
| In scratch mode, the target was proven to be a designated scratch cluster before anything ran: its id was on the allowlist and its marker topic existed. | [Scorecard](formats/drill-scorecard.md): `target.marker_topic` |
| The cluster the archive came from was read from that cluster at backup time and is recorded separately, so the two ids can be compared. | [Backup receipt](formats/backup-receipt.md): `source.cluster_id`; [Scorecard](formats/drill-scorecard.md): `target.cluster_id` |
| Restored data went to prefixed topic names under an attested mapping; the topics the restore set out to create, and any mapped name that already existed, are listed. | [Scorecard](formats/drill-scorecard.md): `target.topic_mapping_prefix`, `target.topic_mapping_sha256`, `target_diff.would_create`, `target_diff.collisions` |
| A scheduled rehearsal may run only in scratch mode, against one target cluster id and under one topic prefix, both fixed in its signed scope. | [Standing authorization](stability.md#the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature): `scope.modes`, `scope.targetClusterId`, `scope.topicPrefix` |

**What it does not show**

- Physical segregation, or protection of the target against unauthorised
  access; and in `newTopic` mode, segregation of any kind:
  [segregation of the restore target](#segregation-of-the-restore-target).

### Article 12(6): recovery time and recovery point objectives

> In determining the recovery time and recovery point objectives for each
> function, financial entities shall take into account whether it is a critical
> or important function and the potential overall impact on market efficiency.
> Such time objectives shall ensure that, in extreme scenarios, the agreed
> service levels are met.

| What the evidence supports | Fields |
|---|---|
| The objectives the approved plan set for the test, and whether the test met them, are recorded; no objective, or an unmeasurable one, reads `null` and never `true`. | [Scorecard](formats/drill-scorecard.md): `objectives.rto_seconds`, `objectives.rpo_seconds`, `objectives.pass_rate`, `objectives.met` |
| The test's recovery time is measured four ways, so the figure compared with the objective can be read against the other three. | [Scorecard](formats/drill-scorecard.md): `measured.rto_seconds`, `measured.rto_requested_to_verified_seconds`, `measured.rto_restore_only_seconds`, `measured.rto_excluding_preflight_seconds` |
| The archive's coverage gap at the requested recovery point is measured. | [Scorecard](formats/drill-scorecard.md): `measured.rpo_seconds` |
| A rehearsal schedule carries a recovery-time objective and records the recovery time its last passing rehearsal measured. | [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `spec.objectives.rtoSeconds`, `status.lastSucceeded.rtoSeconds` |

**What it does not show**

- That a production recovery would take that long, or lose no more than that:
  [RTO and RPO are measured, not guaranteed](#rto-and-rpo-are-measured-not-guaranteed).
- How the objectives were chosen for each function. The objectives are inputs
  to the test, set by the control owner:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).

### Article 12(7): checks and reconciliations when recovering

> When recovering from an ICT-related incident, financial entities shall perform
> necessary checks, including any multiple checks and reconciliations, in order
> to ensure that the highest level of data integrity is maintained. These checks
> shall also be performed when reconstructing data from external stakeholders,
> in order to ensure that all data is consistent between systems.

| What the evidence supports | Fields |
|---|---|
| Restored records were reconciled byte for byte with the archive, on a sample whose size and result are recorded. | [Scorecard](formats/drill-scorecard.md): `integrity.level`, `integrity.result`, `integrity.records_sampled`, `integrity.records_sampled_matching`, `integrity.mismatches`, `sample.records_expected` |
| The archive manifest the restore read is identified by digest, which an auditor can re-derive from the store by hand. | [Scorecard](formats/drill-scorecard.md): `source.manifest_sha256`, `source.manifest_version_id` |
| A check that could not finish says why, and both verifiers refuse a `pass` beside such a reason. | [Scorecard](formats/drill-scorecard.md): `integrity.partial_reason`, `outcome` |
| The target's topic configuration was compared with the configuration the archive recorded. | [Scorecard](formats/drill-scorecard.md): `topic_parity.unexpected_divergence`, `topic_parity.intentionally_deviated` |

**What it does not show**

- That every record was checked:
  [sampling versus complete verification](#sampling-versus-complete-verification).
- Consistency with the source cluster or with other systems, because the
  comparison is with the archive:
  [archive-relative, not source-relative](#archive-relative-not-source-relative).
- Transaction boundaries, the source's timestamps and repeated headers:
  [transaction and timestamp semantics](#transaction-and-timestamp-semantics).
- In a scorecard of format 1.0.0, that an empty
  `topic_parity.unexpected_divergence` means configuration was compared at all:
  [configuration coverage](#configuration-coverage).

## The DORA RTS: Commission Delegated Regulation (EU) 2024/1774

This is the regulatory technical standard on ICT risk management tools,
methods, processes and policies that DORA's Articles 15 and 16(3) call for. Its
Article 25 governs the testing of ICT business continuity plans for entities on
the full framework; its Article 40 is the counterpart for entities on DORA's
simplified framework.

### Article 25: testing of the ICT business continuity plans

Article 25(2) asks the testing to assess whether the entity can ensure the
continuity of its critical or important functions. Point (c) asks entities other
than microenterprises to include "scenarios of switchover from primary ICT
infrastructure to the redundant capacity, backups and redundant facilities", and
for that point:

> the testing shall verify whether at least critical or important functions can
> be operated appropriately for a sufficient period of time, and whether the
> normal functioning may be restored.

Article 25(5):

> Financial entities shall document the results of the testing referred to in
> paragraph 1. Any identified deficiencies resulting from that testing shall be
> analysed, addressed, and reported to the management body.

| What the evidence supports | Fields |
|---|---|
| The result of each restore test is documented in a signed record, which also says why a test did not pass. | [Scorecard](formats/drill-scorecard.md): `outcome`, `integrity.result`, `integrity.partial_reason`, `phases[].notes` |
| A failed rehearsal is recorded with its reason, and the schedule reports itself unhealthy until a rehearsal passes. | [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `status.lastFailed.at`, `status.lastFailed.reason`, `status.conditions` |
| The restored sample was compared with the archive, and the test records how much of it read back exactly as archived. | [Scorecard](formats/drill-scorecard.md): `integrity.level`, `integrity.records_sampled_matching`, `sample.records_expected` |
| The stored record of a test is bound to its exact signed bytes, and the store's answer to its create-only upload is recorded. | [Put receipt](verify-a-scorecard.md#the-storage-receipt-a-second-signed-document): `scorecard_sha256`, `scorecard_key`, `create_only_enforced`, `version_id` |

**What it does not show**

- The scenarios, the impact analysis, and whether critical or important
  functions can be operated on the restored data:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up),
  [consumer positions and application recovery](#consumer-positions-and-application-recovery).
- The analysis and remediation of deficiencies, and their reporting to the
  management body: [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).
- That the restored data is complete:
  [sampling versus complete verification](#sampling-versus-complete-verification).
- Who held the key that signed the documented results:
  [key custody](#key-custody).
- That the stored records cannot be changed or deleted later:
  [storage immutability, location and access](#storage-immutability-location-and-access).

### Article 40: the simplified framework

> The financial entities referred to in Article 16(1) of Regulation (EU)
> 2022/2554 shall test their business continuity plans referred to in Article 39
> of this Regulation, including the scenarios referred to in that Article, at
> least once every year for the back-up and restore procedures, or upon every
> major change of the business continuity plan.

Article 40(3) asks these entities, too, to document the results and to analyse,
address and report deficiencies to the management body.

| What the evidence supports | Fields |
|---|---|
| A restore test of a backup ran on a recorded date and left a signed result. | [Scorecard](formats/drill-scorecard.md): `requested_at`, `source.backup_id`, `outcome` |
| Restore tests recur on a configured cadence, and the last passing one names its signed scorecard. | [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `spec.schedule`, `status.lastSucceeded.evidence` |

**What it does not show**

- That a test happened at least once every year, or after every major change of
  the plan. Logweir records the tests that ran, not the tests that were due:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).

## ISO/IEC 27001:2022, Annex A control 8.13

**Control 8.13, *Information backup*.** It is listed in ISO/IEC 27001:2022,
Annex A, and the same number and title head clause 8.13 of ISO/IEC 27002:2022.

ISO/IEC 27001 and ISO/IEC 27002 are sold by ISO and its national members, and
their text is copyrighted. This page cites the control by number and title
only. ISO/IEC 27002:2022 gives every control a statement of purpose, but the one
for 8.13 is not in the standard's public preview, so this page neither quotes
nor paraphrases it. Read the control, and your statement of applicability, in
your licensed copy.

The rows below say what Logweir's evidence records about backups and restore
tests. Which of them an implementation of 8.13 relies on is for the
organisation's information security management system to record.

| What the evidence supports | Fields |
|---|---|
| Backups were made: each run, the topics it covered, its per-topic record counts and its window. | [Backup receipt](formats/backup-receipt.md): `run_id`, `source.topics`, `records`, `covered.from_ms`, `covered.to_ms` |
| Each backup is identified by the digest of its manifest, and each recovery point by the digest of its receipt, so a later reader can tell whether either has changed. | [Backup receipt](formats/backup-receipt.md): `archive.manifest_sha256`; [Catalog point](formats/catalog-point.md): `point_id`, `receipt.sha256` |
| Backups were restored in a test, and the restored data was compared with the backup on a sample. | [Scorecard](formats/drill-scorecard.md): `source.manifest_sha256`, `integrity.level`, `integrity.result`, `sample.records_expected` |
| The restore test ran under an approved, recorded plan. | [Scorecard](formats/drill-scorecard.md): `approval.plan_hash`, `approval.approver`, `approval.key_id` |

**What it does not show**

- The organisation's backup policy and what it requires to be backed up:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).
- Backups of anything but topic data:
  [configuration coverage](#configuration-coverage),
  [schema registries](#schema-registries),
  [consumer positions and application recovery](#consumer-positions-and-application-recovery).
- That every record was checked:
  [sampling versus complete verification](#sampling-versus-complete-verification).
- How the backups are stored and protected:
  [storage immutability, location and access](#storage-immutability-location-and-access).
- Who held the approving and signing keys:
  [key custody](#key-custody).

## SOC 2: Trust Services Criterion A1.3

A1.3 is one of the AICPA's additional criteria for availability:

> The entity tests recovery plan procedures supporting system recovery to meet
> its objectives.

Its points of focus name periodic testing of the business continuity plan, and
periodic testing of the integrity and completeness of backup data. The criteria
describe points of focus as important characteristics of a criterion, and say
that using the criteria does not require an assessment of whether each one is
addressed.

| What the evidence supports | Fields |
|---|---|
| A recovery procedure was tested: a restore ran, recorded each phase it went through, and checked its result. | [Scorecard](formats/drill-scorecard.md): `outcome`, `phases[].name`, `phases[].outcome`, `integrity.result` |
| The integrity of backup data was tested on a sample, byte for byte against the archive whose manifest digest is recorded. | [Scorecard](formats/drill-scorecard.md): `integrity.level`, `integrity.records_sampled_matching`, `integrity.mismatches`, `source.manifest_sha256` |
| The objectives set for the test, and whether they were met, are recorded. | [Scorecard](formats/drill-scorecard.md): `objectives.rto_seconds`, `objectives.pass_rate`, `objectives.met`, `measured.rto_excluding_preflight_seconds` |
| Tests recur on a schedule, with the last result of each kind recorded. | [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `spec.schedule`, `status.lastSucceeded.at`, `status.lastFailed.at` |

**What it does not show**

- The completeness of backup data against the source:
  [archive-relative, not source-relative](#archive-relative-not-source-relative).
- The integrity of every record:
  [sampling versus complete verification](#sampling-versus-complete-verification).
- The revision of continuity plans after a test:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).
- The recovery of a system as a whole, beyond its Kafka topic data:
  [consumer positions and application recovery](#consumer-positions-and-application-recovery).
- That a production recovery would take as long as the test did:
  [RTO and RPO are measured, not guaranteed](#rto-and-rpo-are-measured-not-guaranteed).

## NIS2: Commission Implementing Regulation (EU) 2024/2690

This regulation applies only to the entities its Article 1 lists: DNS service
providers, TLD name registries, cloud computing service providers, data centre
service providers, content delivery network providers, managed service
providers, managed security service providers, providers of online market
places, of online search engines and of social networking services platforms,
and trust service providers. Its Annex sets out the measures of Article 21(2)
of Directive (EU) 2022/2555 for them; other NIS2 entities apply Article 21(2) as
their Member State transposed it. Point 4.2, *Backup and redundancy management*,
implements Article 21(2), point (c). Its parts that concern tested restores:

> 4.2.2. \[…\] the relevant entities shall lay down backup plans which include
> the following: (a) recovery times; (b) assurance that backup copies are
> complete and accurate, including configuration data and data stored in cloud
> computing service environment; \[…\] (e) restoring data from backup copies;
> \[…\]
>
> 4.2.3. The relevant entities shall perform regular integrity checks on the
> backup copies.
>
> 4.2.6. The relevant entities shall carry out regular testing of the recovery
> of backup copies and redundancies to ensure that, in recovery conditions, they
> can be relied upon and cover the copies, processes and knowledge to perform an
> effective recovery. The relevant entities shall document the results of the
> tests and, where needed, take corrective action.

Points 4.2.2(c), (d) and (f) ask for a safe storage location, access controls
and retention periods for the copies.

| What the evidence supports | Fields |
|---|---|
| 4.2.2(a): the recovery time of a tested restore is measured, and compared with the objective the plan set. | [Scorecard](formats/drill-scorecard.md): `measured.rto_seconds`, `measured.rto_restore_only_seconds`, `measured.rto_excluding_preflight_seconds`, `objectives.rto_seconds`, `objectives.met` |
| 4.2.2(b): each backup run's per-topic record counts and covered window are signed, and a restore's sampled records are compared byte for byte with the archive. | [Backup receipt](formats/backup-receipt.md): `records`, `covered.from_ms`, `covered.to_ms`; [Scorecard](formats/drill-scorecard.md): `integrity.records_sampled`, `integrity.records_sampled_matching` |
| 4.2.2(e): data was restored from a recorded archive into a recorded target. | [Scorecard](formats/drill-scorecard.md): `source.backup_id`, `target.cluster_id`, `target_diff.would_create` |
| 4.2.3: the archive manifest's digest is signed and can be re-derived by hand, and each restore test checks a sample of the archive. | [Scorecard](formats/drill-scorecard.md): `source.manifest_sha256`, `integrity.level`, `integrity.result` |
| 4.2.6: each recovery test leaves a signed record of its result, and of why it did not pass. | [Scorecard](formats/drill-scorecard.md): `outcome`, `integrity.partial_reason`; [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `status.lastFailed.reason` |

**What it does not show**

- 4.2.2(a), the recovery time of a real recovery:
  [RTO and RPO are measured, not guaranteed](#rto-and-rpo-are-measured-not-guaranteed).
- 4.2.2(b), configuration data:
  [configuration coverage](#configuration-coverage); completeness against the
  source: [archive-relative, not source-relative](#archive-relative-not-source-relative).
- 4.2.2(c), (d) and (f), where the copies are stored, who can reach them and how
  long they are kept:
  [storage immutability, location and access](#storage-immutability-location-and-access).
- 4.2.3, an integrity check of every copy and every record:
  [sampling versus complete verification](#sampling-versus-complete-verification).
- 4.2.6, corrective action, and the processes and knowledge an effective
  recovery needs beyond restoring topic data:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up),
  [consumer positions and application recovery](#consumer-positions-and-application-recovery).

## HIPAA: 45 CFR 164.308(a)(7)(ii)(D)

The contingency plan standard, 45 CFR 164.308(a)(7)(i), has five implementation
specifications. This one reads:

> (D) Testing and revision procedures (Addressable). Implement procedures for
> periodic testing and revision of contingency plans.

Beside it, (A) *Data backup plan* and (B) *Disaster recovery plan* are
Required: "Establish and implement procedures to create and maintain
retrievable exact copies of electronic protected health information", and
"Establish (and implement as needed) procedures to restore any loss of data".
For an addressable specification, 45 CFR 164.306(d)(3) has the covered entity or
business associate assess whether it is reasonable and appropriate, and either
implement it, or document why not and implement an equivalent alternative
measure where that is reasonable and appropriate. A proposed rule
([90 FR 898](https://www.federalregister.gov/documents/2025/01/06/2024-30983/hipaa-security-rule-to-strengthen-the-cybersecurity-of-electronic-protected-health-information),
2025-01-06) would restructure this section; on 2026-10-05 it was still a
proposal.

| What the evidence supports | Fields |
|---|---|
| Periodic testing: restore tests recur on a schedule, and each one leaves a dated, signed result. | [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `spec.schedule`, `status.lastSucceeded.at`; [Scorecard](formats/drill-scorecard.md): `requested_at`, `outcome` |
| What a test found, including why it did not pass, is recorded as an input to the revision step. | [Scorecard](formats/drill-scorecard.md): `outcome`, `integrity.partial_reason`, `phases[].notes`; [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `status.lastFailed.reason` |
| For (A) and (B), a backup was restored in a test, and a sample of the restored records was compared byte for byte with it. | [Scorecard](formats/drill-scorecard.md): `integrity.level`, `integrity.records_sampled_matching`, `sample.records_expected` |

**What it does not show**

- The revision of contingency plans, and whether the tested topics hold
  electronic protected health information:
  [test scope, cadence and follow-up](#test-scope-cadence-and-follow-up).
- Exact copies. The comparison is sampled, and some record properties are not
  preserved: [sampling versus complete verification](#sampling-versus-complete-verification),
  [transaction and timestamp semantics](#transaction-and-timestamp-semantics).
- Encryption of the copies and control of access to them:
  [storage immutability, location and access](#storage-immutability-location-and-access).
- Who held the key that signed the test records:
  [key custody](#key-custody).

## What the evidence does not show

Each gap below is a limit of the evidence as the code on `main` produces it
today, with the section that documents it. Where tracked work would narrow a
gap, the [product-expansion tracker](to-do/product-expansion.md) row is named,
so that it can be followed. Until that work merges, the gap stands as written.

### Sampling versus complete verification

A restore test checks a sample. Per sampled partition it compares the first
records of the requested window (`sample.anchor` is always `head`), as many as
the plan's records-per-partition setting allows, up to the plan's partition
cap; `sample.records_expected` is that canary size, and `sample.partitions` and
`sample.topics` count what was selected. A `pass` establishes agreement inside
the sample. It does not establish that unsampled records would match
([the sample window is not a claim about the whole archive](verify-a-scorecard.md#the-sample-window-is-not-a-claim-about-the-whole-archive)),
and `sample.coverage_note` is the test's own statement of how representative
the sample is.

`sample.records_restored` counts the records the check read back from the
target, not every record the restore wrote: the scorecard records no total of
restored records. The record count check is a bound computed from the archive
manifest, not an exact count per partition. Duplicates and record order are not
checked, and only the in-window segments of the sampled partitions are hashed.
[PROD-08.1](to-do/product-expansion.md#prod-081--complete-archive-integrity-and-exact-counts)
lists these limits and plans complete per-partition counts, duplicate and order
checks, and a signed field that says whether a check was sampled or complete.
The engine's own validation report is not retained (`engine_subreport` is
`null`) and would corroborate nothing if it were
([why](verify-a-scorecard.md#engine_subreport-corroborates-nothing-about-logweirs-integrity-claim)).

### Archive-relative, not source-relative

A restore test compares the restored topics with the archive and never contacts
the source cluster: `source.captured_by_logweir` is `false`,
`measured.rpo_source_relative_seconds` is `null`, and
`measured.rpo_source_relative_unmeasured_reason` reads
`source cluster never contacted`. A loss that happened when the archive was
written is in both copies, so the comparison cannot see it
([a pass compares the restored topic with the archive](verify-a-scorecard.md#a-pass-compares-the-restored-topic-with-the-archive-not-with-the-source)).

A backup receipt counts what the engine captured, per topic, but records no
source watermarks and no capture gaps, such as records that expired before
capture. It therefore does not show that a backup holds everything the source
held. [PROD-02.1](to-do/product-expansion.md#prod-021--show-honest-coverage-for-scheduled-backups)
plans signed watermarks and gaps.

### Configuration coverage

As of `main`, a restore rebuilds topic data and the partition count. It creates
each target topic with the archive's partition count, the plan's replication
factor and two fixed settings (`CreateTime` timestamps and unlimited retention);
the source topic's other configuration, such as `cleanup.policy` or
`min.insync.replicas`, is not applied.

`topic_parity.unexpected_divergence` compares the target with the topic
configuration the archive recorded. A backup receipt and a scorecard of format
1.0.0 carry no coverage for that recording: when the source refused
DescribeConfigs, the capture reads as "no overrides", and the parity check
reports no divergence without having compared anything, so an empty list in a
1.0.0 scorecard does not show that configuration was compared. Tracker row FX-4
narrows this in format 1.1.0: the backup receipt carries a per-topic
`config_coverage` (`captured`, `notCaptured` or `captureDenied`), the catalog
point copies it, and the scorecard's `topic_parity` and `target_diff` gain a
`not_assessed` list that names each topic whose configuration was not compared,
and why. Absent coverage, as in every 1.0.0 document, means unknown and never
`captured`. `topic_parity.intentionally_deviated` labels differences in
`cleanup.policy`, `retention.ms`, partition count and replication factor as
intended in every target mode, including `newTopic`, where they are not (FX-3,
proposed). Both rows are in the tracker's
[fix-now table](to-do/product-expansion.md#fix-now-defects-in-shipped-code).

Nothing beyond topic data and that captured configuration is in an archive:
not access control lists, quotas, users, consumer group positions, schemas,
connectors or stream-processing state
([PROD-05](to-do/product-expansion.md#prod-05--recover-configuration-and-access-metadata),
[consumer positions](#consumer-positions-and-application-recovery),
[schema registries](#schema-registries)).

### Key custody

The evidence identifies keys, not people. Scorecard `approval.key_id` names the
approver's key, the scorecard's DSSE sidecar names the signing key, and
`approval.self_attested` says whether they are the same key, which both
verifiers derive from the key that actually verified rather than trust
([reading `approval.self_attested`](verify-a-scorecard.md#reading-approvalself_attested)).
A catalog point names its signer in `signing.key_id`.

Who holds those keys is outside the documents:

- **The evidence-signing key** is the installation identity, generated once by
  the chart's bootstrap Job into the Secret `logweir-signing-key` and mounted by
  runner Jobs ([managed installation identity](keys.md#managed-installation-identity)).
  Anyone who can create pods in a runner namespace, and the controller, which
  creates Jobs there, can sign with it
  ([the residual, stated plainly](kubernetes.md#154-the-residual-stated-plainly);
  [what the control plane does not stop](stability.md#kubernetes-what-the-control-plane-does-not-stop-and-what-it-was-built-against)).
  Logweir has no KMS or PKCS#11 signing
  ([deferred item 10](stability.md#later-named--original-scope-with-current-status)).
- **Approver keys** are generated and held by the approvers; a namespace's
  trust policy holds only their public halves, with usage `GovernedApproval`
  ([key usage separation](keys.md#key-usage-separation)).
- **The publisher's public key** must reach the auditor through a channel
  independent of the evidence
  ([where the public key comes from](verify-a-scorecard.md#where-the-public-key-comes-from)).

Every timestamp in these documents was read from the clock of a machine that
produced it: the runner's, or for an approval, the approver's. Logweir uses no
trusted timestamping service, so a signature shows which key signed and not
when.

### Storage immutability, location and access

Object Lock and other write-once storage are the operator's to configure on the
bucket. Logweir does not configure them, and on `main` it cannot read them back:
`Store::object_lock_readback` returns nothing on every backend, so the put
receipt's `immutable` is always `false` and its `retain_until` always `null`.
That is an absence of proof, not proof that the object is mutable
([the storage receipt](verify-a-scorecard.md#the-storage-receipt-a-second-signed-document)).
The scorecard's own `evidence.create_only_enforced`, `evidence.immutable`,
`evidence.retain_until` and `evidence.version_id` are zeroed before signing and
say nothing about storage
([the `evidence` block](formats/drill-scorecard.md#evidence--makes-no-claim-about-the-upload)).
What the put receipt does record is whether the store performed a conditional
put of the scorecard (`create_only_enforced`) and the version id it returned
(`version_id`).
[PROD-09.1](to-do/product-expansion.md#prod-091--make-archive-protection-observable)
plans observed lock mode, retention and grants.

The archive is written by the engine's own store client. Logweir starts at most
one engine run per `backup_id`
([the execution claim](formats/backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id)),
and records the manifest's version id when the store returns one
(`source.manifest_version_id`), but sets written by older builds can carry two
receipts and a rewritten manifest. Re-deriving `archive.manifest_sha256` from
the store is how a reader checks that a manifest is still the one attested.

`archive.location_id` names a bucket and prefix only, never a region, an
endpoint or a network, so the evidence does not show where a copy is or whether
it is held apart from the source. It does not show who can read or delete the
archive. Neither the engine nor Logweir configures encryption of the archive at
rest. Retention periods are the operator's: a `RetentionPolicy` in `Enforce`
refuses to delete from versioned buckets, including every Object Lock bucket,
and Logweir cannot verify a legal hold
([§7f](kubernetes.md#7f-a-retentionpolicy-in-enforce-is-the-one-thing-logweir-does-that-cannot-be-undone)).
Logweir never deletes evidence under `logweir/`.

### Transaction and timestamp semantics

With the pinned engine, a restore can differ from the source in ways the test
does not detect. In each measured case below, the test was signed `pass`
([the measured cases](verify-a-scorecard.md#a-pass-compares-the-restored-topic-with-the-archive-not-with-the-source)):

- Aborted records, records of open transactions, and commit and abort markers
  are restored as ordinary records
  ([transactional topics](stability.md#transactional-topics-are-restored-with-aborted-records-and-markers-as-data)).
- A topic on `LogAppendTime` is restored with its producers' timestamps, and a
  point-in-time restore selects by those clocks
  ([`LogAppendTime` sources](stability.md#logappendtime-sources-are-restored-with-the-producers-timestamps)).
  Tracker row FX-8 (proposed) refuses that selection unless the approved plan
  asks for it, and labels the result.
- Out-of-order timestamps within a segment can make a point-in-time restore
  omit a record at or before the requested point
  ([recovery-point selection](stability.md#recovery-point-selection-uses-segment-first-and-last-timestamps)).
- A repeated header key keeps one copy
  ([repeated header keys](stability.md#a-repeated-header-key-keeps-one-copy)).

A broker outage or a lost acknowledgement during a restore can leave duplicates
and a partial target. In every measured case Logweir exited 1 and signed
nothing, so what a completed test makes of duplicates was not measured
([broker outages](stability.md#a-broker-outage-during-a-restore-can-leave-a-partial-target-with-duplicates)).

So a `pass` shows that the restored sample matches the archive; it does not
show transactional, exactly-once or source-faithful recovery.
[PROD-01.1](to-do/product-expansion.md#prod-011--prove-record-and-transaction-behavior)
measured each case, and FX-6 states them on the verification guide, the
stability page and the console's restore review step.

### Schema registries

Logweir does not back up, restore or check a schema registry: Confluent Schema
Registry, Apicurio, RBAC-MDS and CSFLE are a product boundary
([Never #2](stability.md#never--four-entries)). Records are restored as bytes,
and no field shows that the schemas needed to read them exist where they are
restored. [PROD-03.0](to-do/product-expansion.md#prod-030--flag-schema-dependent-topics)
plans to flag topics that depend on a registry.

### Consumer positions and application recovery

Logweir does not capture or restore consumer group positions, and restored
records receive new offsets on the target, each carrying its source offset in an
`x-original-offset` header. No field shows that an application resumed on the
restored data, or from where; Logweir performs no switchover of applications.
[PROD-04](to-do/product-expansion.md#prod-04--restore-consumer-positions-and-guide-cutover)
plans the capture, translation and reviewed application of positions.

### RTO and RPO are measured, not guaranteed

The four RTO figures are durations of one test, on that test's target, at that
test's size; `measured.rto_excluding_preflight_seconds` is the one compared with
`objectives.rto_seconds`, because the preflight reads every segment in a way no
incident response does ([the four RTO definitions](formats/drill-scorecard.md#measured--the-four-rto-definitions-verbatim)).
The scorecard does not record how many records the restore wrote, so a test's
RTO does not predict how long a production restore would take.

`measured.rpo_seconds` is the archive's coverage gap at the requested point:
how far the newest restored record falls short of the point asked for. It is
not data lost from the source, which no restore test measures
([`rpo_seconds`](formats/drill-scorecard.md#rpo_seconds)). A rehearsal schedule
carries a recovery-time objective in `spec.objectives.rtoSeconds`, and no
recovery-point objective;
[PROD-08.2](to-do/product-expansion.md#prod-082--measure-recovery-objectives-through-exercises)
plans one. Logweir records the objectives a plan sets; choosing them for each
function is the control owner's work.

### Segregation of the restore target

In scratch mode the target is a designated scratch cluster: its id is on the
allowlist and its marker topic exists, both checked before anything runs, and
the scorecard records the marker in `target.marker_topic`
([`source` and `target`](formats/drill-scorecard.md#source-and-target)). The
runner also refuses a scratch target whose id equals the archive's source
cluster id when the allowlist file names that id; the scorecard does not record
whether it did. These are checks of a cluster's identity, not of the
infrastructure under it: the evidence does not show that the target shares no
hosts, network or account with the source, or that it is protected from
unauthorised access. In `newTopic` mode the
allowlist and marker checks are skipped, `target.marker_topic` is absent, and
only the new topic names separate restored data from the cluster's existing
topics; that cluster may be the source cluster itself. Restoring into a live
topic is refused in every mode ([Never #1](stability.md#never--four-entries)).
Scheduled rehearsals run in scratch mode only (Standing authorization
`scope.modes`).

### Test scope, cadence and follow-up

A test's scope is its plan: which archive, which topics, which window and which
target. `approval.plan_hash` is the digest of the exact plan bytes that ran; the
scorecard itself carries counts (`sample.topics`, `sample.partitions`) and the
target names it set out to create (`target_diff.would_create`), so naming the
source topics a test covered takes the plan document, checked against that
digest. Whether a test's scope covers what a clause names, such as all
functions, the critical or important ones, or the systems holding protected
health information, is the control owner's mapping.

Cadence is configuration: `spec.schedule` says when rehearsals should fire. The
record of tests that happened is the set of signed scorecards under
`logweir/drills/`, which Logweir never deletes. RehearsalSchedule status keeps
only the latest pass, failure and skip; a skipped slot is not run late, and its
reason survives in `status.lastSkipped.reason` only until the next skip.

The evidence records results. The impact analysis, test scenarios, review of
the results, corrective action, plan revision and reporting to management that
these clauses also ask for happen outside Logweir, and no field records them.

## Sources

Every text was read on 2026-10-05.

| Clause as cited here | Official text | Notes |
|---|---|---|
| DORA, Articles 11, 12 and 16 | [Regulation (EU) 2022/2554](https://eur-lex.europa.eu/eli/reg/2022/2554/oj/eng), OJ L 333, 27.12.2022, p. 1 | EUR-Lex lists corrigenda to other language versions only, and no amending act. |
| DORA RTS, Articles 25 and 40 | [Commission Delegated Regulation (EU) 2024/1774](https://eur-lex.europa.eu/eli/reg_del/2024/1774/oj/eng), OJ L, 2024/1774, 25.6.2024 | "The RTS" means this regulation. Its English [corrigendum](https://eur-lex.europa.eu/eli/reg_del/2024/1774/corrigendum/2025-05-15/oj/eng) of 15.5.2025 changes Article 22(d) only. |
| NIS2 implementing regulation, Annex point 4.2 | [Commission Implementing Regulation (EU) 2024/2690](https://eur-lex.europa.eu/eli/reg_impl/2024/2690/oj/eng), OJ L, 2024/2690, 18.10.2024 | The section is point 4.2 of the Annex. EUR-Lex lists corrigenda to the Polish, Swedish and Dutch versions only. |
| HIPAA, 45 CFR 164.308(a)(7)(ii)(D) | [45 CFR 164.308 on the eCFR](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164/subpart-C/section-164.308) | The eCFR was current to 2026-10-01, with no amendment to §164.308 in its history since 2016-12-30. |
| SOC 2, A1.3 | [AICPA, 2017 Trust Services Criteria (With Revised Points of Focus — 2022)](https://www.aicpa-cima.com/resources/download/2017-trust-services-criteria-with-revised-points-of-focus-2022) | The download needs a free AICPA account. A1.3 and its points of focus were read from the AICPA's own PDF of the 2017 criteria with the March 2020 updates, through an [archived copy](https://web.archive.org/web/20220901034236/https://us.aicpa.org/content/dam/aicpa/interestareas/frc/assuranceadvisoryservices/downloadabledocuments/trust-services-criteria.pdf) of its former public address; the 2022 revision of the points of focus was not read. |
| ISO/IEC 27001:2022, Annex A control 8.13 | [ISO/IEC 27001:2022 in ISO's catalogue](https://www.iso.org/standard/27001) | Sold, not public. The number and title were checked in the contents of ISO/IEC 27002:2022 in the [preview published by SIS](https://www.sis.se/en/produkter/information-technology-office-machines/it-security/isoiec-270022022/), Sweden's ISO member. |

## Keeping this page true

The page describes the evidence as `main` produces it. When a field it cites is
renamed or removed, `crates/logweir/tests/control_evidence.rs` fails. When a
tracker row named above merges, the gap it narrows is rewritten in the same
change, and any new field the row adds is cited here only once it is on `main`.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
