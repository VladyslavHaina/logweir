# The tag-1 checklist — one row per spec §16 clause

**"Publishable" for tag 1 means all of** the ten clauses of the specification's
tag plan (`docs/mvp/03-spec.md` §16, in the parent corpus). This file is the
ledger of those ten, and it exists because a checklist whose rows can read
`closed` without a closer is worse than no checklist: a clause nobody can close
gets ticked by whoever is tired, and the tag ships with a documented guarantee
behind which there is nothing.

**The row grammar admits exactly three statuses and no fourth.**

| status | meaning |
|---|---|
| `closed` | a command exits 0, or a recorded transcript exists, and the row names it. The named path or command **exists in this repository** — `scripts/check-dod.sh` asserts that on every run. |
| `blocked: <reason>` | the clause cannot be closed from this tree today, and the reason says why. A blocked row is **recorded as blocked, never as closed** (tag-1 STANDING RULE 22), is echoed under `check-dod.sh`'s "NOT CHECKED HERE" heading, and is **never counted as a pass**. |
| `open` | neither: work that could be done here and has not been. No row is `open` today. |

**A row never carries two statuses**, and a clause with two halves states both
halves in its note rather than averaging them into one word. Where a row's
closer is a test or a script that a **later** task will add, the row names the
artefacts that exist **today** and says in its note which task adds what — a
row that cites a file nobody has written is the same defect as a tick with no
closer.

## The ten rows

| # | Spec §16 clause | Status | Closer or recorded evidence |
|---|---|---|---|
| 1 | `kubectl apply --server-side -f logweir.yaml` works twice on a clean docker-desktop, **and** the image digests that file carries have been pulled back from a registry the author does not control | `blocked: images not published` | `docs/kubernetes.md` §13 (the X-APPLY transcript) · `docs/install.md` (the two digest rows, both blocked) · `logweir.yaml` · `crates/logweir/tests/manifest_lint.rs` |
| 2 | Demo 1 runs end to end **in CI on a `kind` cluster created by the workflow**; X-APPLY is additionally recorded once on docker-desktop; Demo 2 is documented and labelled | `blocked: no remote` | `e2e/k8s/laptop-demo.md` (the recorded docker-desktop walkthrough, X-APPLY included) · `crates/logweir/tests/laptop_demo_lint.rs` · `docs/kubernetes.md` §13 |
| 3 | Every shipped image is referenced by `@sha256:` and asserted before push; the engine digest stays in `third_party/kafka-backup-binary.digest`; the extraction script's tag pull is fixed | `closed` | `docs/kubernetes.md` §14 (the X-DIGEST transcript) · `crates/logweir/tests/extract_engine.rs` · `crates/logweir/tests/manifest_lint.rs` · `scripts/check-image.sh` · `scripts/check-image-weirkeeper.sh` · `third_party/kafka-backup-binary.digest` · `scripts/check-dod.sh` |
| 4 | Task 9's F1–F4 are closed before the first real push, and `release.yml` then runs for real, once | `blocked: no remote` | `crates/logweir/tests/workflow_lint.rs` · `.github/workflows/release.yml` |
| 5 | LICENSE, NOTICE, README, SECURITY.md and CONTRIBUTING with DCO are present; every doc footer carries the ASF sentence; both images `COPY` LICENSE and NOTICE | `closed` | `crates/logweir/tests/doc_lint.rs` · `scripts/check-dod.sh` · `scripts/check-image.sh` · `scripts/check-image-weirkeeper.sh` · `THIRD_PARTY_NOTICES.md` |
| 6 | `docs/verify_scorecard.py` ships **inside the release artefact**, not only in the repository | `blocked: no remote` | `docs/verify_scorecard.py` · `.github/workflows/release.yml` |
| 7 | The drift gate covers the scorecard **and** the backup receipt, and both readers agree on each | `closed` | `crates/logweir-core/tests/schema_drift.rs` · `scripts/check-verifier-parity.sh` · `crates/logweir/tests/two_reader_parity.rs` · `crates/logweir/tests/two_reader_parity_receipt.rs` · `.github/workflows/ci.yml` |
| 8 | The trademark position is cleared by counsel, or the name is explicitly a placeholder, with the registry namespace already fixed | `blocked: owner action` | `TRADEMARKS.md` · `crates/logweir/tests/doc_lint.rs` |
| 9 | Every mark in the shipped docs is labelled in place, with what would verify it | `closed` | `bash scripts/check-unverified-labels.sh` · `crates/logweir/tests/label_gate.rs` |
| 10 | The ADR-0002 amendment is landed: the ADR's Decision and GC3 both read four runtime engine commands | `closed` | `docs/adr/0002-shell-out.md` · `docs/adr/0008-mvp-constraint-amendments.md` · `crates/logweir/tests/engine_allowlist.rs` |

## The notes, clause by clause

### 1 — the install file, and the digests it carries

**`blocked: images not published`.** Install gate X-APPLY (Task 21) records
`kubectl apply --server-side -f logweir.yaml` exiting **0 twice in a row** on a
clean docker-desktop, reporting `serverside-applied` for all fifteen documents
on both runs. That is a real result and it is **not this clause**: `kubectl
apply` exiting 0 does not start a pod — the transcript records the Deployment
settling into `ImagePullBackOff` — and the digests `logweir.yaml` carries were
built on one laptop and have never been pulled back from a registry the author
does not control (Global Constraint 37, spec §11 amendment 5). A locally built
digest is worse than unproven: it **changes on every build**, including a
cached no-op rebuild, because BuildKit regenerates the provenance attestation,
so it is the measurement of one build rather than a reproducible pin.

What moves this row to `closed` is Task 30b's pull-back: a git remote, one
`release.yml` run against it, and a pull of the published digests **on a host
that did not build them**. Nothing in this tree can do it, and no test here
asserts otherwise.

### 2 — Demo 1, in CI, on a workflow-created cluster

**`blocked: no remote`.** The `kind` job is Task 31's and needs a git remote and
GitHub Actions; it does not exist today, so nothing in this row names it as a
closer. What **is** recorded is the whole walkthrough run on docker-desktop end
to end and checked in at `e2e/k8s/laptop-demo.md` — preflight, the install gate,
the keypairs, the Secrets, the roster, the cluster, a schedule and the backup it
fires, the approval minted out of band, both readers over the scorecard and the
teardown — with `crates/logweir/tests/laptop_demo_lint.rs` refusing a transcript
that has been forged rather than run. The docker-desktop half of the clause is
therefore evidenced; the CI half is not, and the row reads `blocked` for the
whole clause because a row carries one status.

Demo 2 is documented and labelled: its marks are the MSK rows, each carrying the
sentence that would verify it, and `bash scripts/check-unverified-labels.sh`
keeps them labelled (row 9).

### 3 — every image by digest

**`closed`.** This clause is about the **shape** being right, and every part of
it is machine-checked with no network: install gate X-DIGEST (Task 23) answered
whether a digest reference starts a pod — it does — and its transcript is
`docs/kubernetes.md` §14; `crates/logweir/tests/extract_engine.rs`'s
`extract_engine_pulls_by_digest_in_the_default_mode` holds the extraction
script's default to a digest pull; `crates/logweir/tests/manifest_lint.rs`'s
`manifest_lint_every_image_reference_is_a_digest` parses every shipped manifest;
`scripts/check-dod.sh` carries the coarse backstop that runs with no cargo at
all; and the engine pin itself is one `sha256:` line in
`third_party/kafka-backup-binary.digest`.

Closed here does **not** mean the images are published — that is row 1, and it
is blocked. Task 30b adds `workflow_lint_every_platform_is_asserted_before_the_manifest_push`
for the multi-architecture push; that test does not exist today and this row
does not name it.

### 4 — Task 9's F1–F4, and one real run

**`blocked: no remote`**, and the clause has two halves that are stated
separately rather than averaged.

The **F1–F4 half is closed**, by five named lints in
`crates/logweir/tests/workflow_lint.rs`, every one of which parses the shipped
workflow and runs in the ordinary `cargo test --workspace` set:
`workflow_lint_exactly_one_job_pushes` (F1 — the set of pushing jobs has
cardinality one), `workflow_lint_the_push_step_names_no_build_command` (F2 — no
`build` token in a push step's shell), `workflow_lint_login_and_push_are_tag_gated`
(F3 — both gated on a tag ref, in every job),
`workflow_lint_version_tag_is_pushed_before_latest` (F4 — the version reference
is pushed before the moving one), and
`workflow_lint_release_builds_the_image_once`, which is the property the other
four exist to protect: the bytes the image checker interrogated are the bytes
that reach the registry.

The **"runs for real, once" half is blocked**: `.github/workflows/release.yml`
has never executed on any commit, and cannot until a remote exists. Task 30b is
what runs it.

### 5 — the licence and attribution files

**`closed`.** `crates/logweir/tests/doc_lint.rs` holds four of the five parts in
the default test set —
`every_doc_footer_carries_the_asf_sentence_and_the_docs_licence` (the ASF
sentence in every Markdown file under `docs/` and at the repository root, with a
CC-BY-4.0 link that has to **resolve** on disk),
`contributing_requires_a_dco_signoff_and_no_cla`,
`both_images_copy_the_licence_and_the_notice` (the `COPY` instructions in both
Dockerfiles), and
`the_notice_names_every_librdkafka_component_in_licenses_txt`.
`scripts/check-dod.sh` is the fifth: it requires each release file to be present
and non-empty, and its attribution arm walks **every** Markdown file in the tree
rather than only the ones under `docs/` — `e2e/k8s/laptop-demo.md` is covered by
that arm and by nothing else. The image half is proved at runtime by
`scripts/check-image-weirkeeper.sh` check 3, which refuses an image whose
`/usr/share/licenses/logweir/LICENSE` or `NOTICE` is absent or empty.

### 6 — the auditor's reader, inside the release artefact

**`blocked: no remote`.** `docs/verify_scorecard.py` exists, is the auditor's
independent reader, and is exercised on every signed artefact by
`scripts/check-verifier-parity.sh` and by the e2e suite. What is missing is a
**published release artefact that contains it**: an auditor's independence is
not real if the script lives only in a repository they have to clone.
`the_release_artefact_ships_the_auditors_reader` is Task 30b's test and does not
exist today, so this row does not name it.

### 7 — the drift gate and the parity gate

**`closed`.** Two drift arms, one per document. `.github/workflows/ci.yml`
carries them as the steps named *schema drift* (the scorecard) and *backup
receipt schema drift* (Task 5, deliberately a separate step so a failure names
which schema drifted). Because `ci.yml` has never executed, the **enforcing**
copies are the two tests in `crates/logweir-core/tests/schema_drift.rs` —
`checked_in_schema_matches_the_types` for the scorecard and
`backup_receipt_schema_has_no_drift` for the receipt — both in the default
`cargo test --workspace` set.

The parity half is `scripts/check-verifier-parity.sh`, a commit precondition
under Global Constraint 23 and a member of `just lint`, with
`crates/logweir/tests/two_reader_parity.rs` and
`crates/logweir/tests/two_reader_parity_receipt.rs` running both readers over
the signed fixture corpus in the same default set. `ci.yml` carries a **third**
drift arm, for the CRDs (Task 15b); it is a format gate of the same shape and is
outside this clause.

### 8 — the trademark position

**`blocked: owner action`, and no task closes this one.** What would close it is
a formal **UKIPO + EUIPO + USPTO search for LOGWEIR and WEIRKEEPER in classes 9
and 42, on the official registries** — an act by the owner, or by counsel
instructed by the owner. The research record is explicit that the compounds
return zero hits while "weir" alone returns 2,824 records dominated by one
engineering group across classes that include 9 and 42, that the UK, EU and US
registrations were never individually inspected, and that **counsel must assess
likelihood-of-confusion** for a compound in software classes. No test, script or
transcript in this repository can substitute for that.

The cost of the answer is already bounded, which is why the clause is a blocker
and not a stop-work: the registry namespace is the literal `ghcr.io/logweir/…`
(Global Constraint 24), fixed now under the working name, so a trademark answer
changes one string and not the install path. `TRADEMARKS.md` states the
clearance act and the announcement gate, and
`crates/logweir/tests/doc_lint.rs`'s
`trademarks_states_the_clearance_act_and_the_announcement_gate` keeps that
statement from being quietly softened.

### 9 — every mark labelled in place

**`closed`, by this task.** `bash scripts/check-unverified-labels.sh` exits 0 on
this tree. It anchors on the bracket token rather than the bare word, requires
every mark to carry a dash and a description of at least three words and at
least twelve characters, refuses a mark on a line that also asserts the thing is
true, prints every accepted mark with its description, prints quotations of the
form `[UNVERIFIED]` in backticks under their own heading, and prints the
bare-word mentions without ever failing on them. It is a member of `just lint`,
which is what makes it a gate: `ci.yml` has never executed, so a workflow step
would be documentation. `crates/logweir/tests/label_gate.rs` keeps the
membership honest, keeps the two formerly defective marks closed, and keeps this
file's own rows from drifting.

What the gate does **not** check is whether a description is accurate. It checks
that one exists, that it is long enough to say something, and that it is not
contradicted on its own line.

### 10 — the ADR-0002 amendment

**`closed`.** `docs/adr/0002-shell-out.md`'s Decision reads four runtime engine
subcommands and carries its own dated **Amended** paragraph;
`docs/adr/0008-mvp-constraint-amendments.md` records the amendment as
Amendment D, with the reasoning stated once rather than restated at each site.
`crates/logweir/tests/engine_allowlist.rs`'s `adr_0002_decision_reads_four_commands`
and `adr_0008_records_all_four_amendments` hold both in the default test set.

The matching `[REVISED …]` / `[AMENDED …]` markers on the constraints file and
on the roadmap live in the **parent corpus**, not in this repository — ADR-0008's
Consequences names them by file and line — so this row names the two ADRs and
the two tests, which are what a reader of this repository can check.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
