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
| 1 | `kubectl apply --server-side -f logweir.yaml` works twice on a clean docker-desktop, **and** the image digests that file carries have been pulled back from a registry the author does not control | `blocked: images not published` | `docs/kubernetes.md` §13 (the X-APPLY transcript) · `docs/install.md` (the two digest rows, both blocked) · `logweir.yaml` · `crates/logweir/tests/manifest_lint.rs` · **2026-09-11, still blocked. What would close it: the pullback job's summary from one tagged run — the two pulled digests and the pull transcript, written on a runner that did not build the bytes. Nothing else does.** |
| 2 | Demo 1 runs end to end **in CI on a `kind` cluster created by the workflow**; X-APPLY is additionally recorded once on docker-desktop; Demo 2 is documented and labelled | `closed` | `e2e/k8s/laptop-demo.md` (the recorded docker-desktop walkthrough, X-APPLY included) · `crates/logweir/tests/laptop_demo_lint.rs` · `.github/workflows/kind-demo.yml` · `scripts/kind-demo.sh` · `scripts/demo-steps.sh` · `e2e/k8s/kind-config.yaml` · `crates/logweir/tests/workflow_lint.rs` · `docs/kubernetes.md` §13, §18 · **2026-09-12, closed by the ninth run: https://github.com/VladyslavHaina/logweir/actions/runs/34700987743 — one green run of `.github/workflows/kind-demo.yml` on commit a113dd2, on a kind cluster that run created, all twelve steps. It closes THIS clause and nothing else; the images it ran were built by that run and loaded, never pulled, so clause 1 is untouched.** · **2026-09-11, still blocked at the time. What would close it: the URL of one green run of `.github/workflows/kind-demo.yml`, on a kind cluster that run created. Nothing else does.** · **2026-09-12: the fourth CI run reached step 3 and stopped at rollout status deploy/weirkeeper, because X-APPLY's unedited `logweir.yaml` points the Deployment (and, through a compiled-in constant, every runner Job) at digests that runner's own build does not carry — Task 33 makes the controller honour LOGWEIR_RUNNER_IMAGE and has step 3 hand over the images the cluster loaded; still blocked then.** |
| 3 | Every shipped image is referenced by `@sha256:` and asserted before push; the engine digest stays in `third_party/kafka-backup-binary.digest`; the extraction script's tag pull is fixed | `closed` | `docs/kubernetes.md` §14 (the X-DIGEST transcript) · `crates/logweir/tests/extract_engine.rs` · `crates/logweir/tests/manifest_lint.rs` · `scripts/check-image.sh` · `scripts/check-image-weirkeeper.sh` · `third_party/kafka-backup-binary.digest` · `scripts/check-dod.sh` |
| 4 | Task 9's F1–F4 are closed before the first real push, and `release.yml` then runs for real, once | `blocked: no tag pushed` | `crates/logweir/tests/workflow_lint.rs` · `.github/workflows/release.yml` · **2026-09-11, still blocked. What would close it: the URL of one tagged run of that workflow, green.** |
| 5 | LICENSE, NOTICE, README, SECURITY.md and CONTRIBUTING with DCO are present; every doc footer carries the ASF sentence; both images `COPY` LICENSE and NOTICE | `closed` | `crates/logweir/tests/doc_lint.rs` · `scripts/check-dod.sh` · `scripts/check-image.sh` · `scripts/check-image-weirkeeper.sh` · `THIRD_PARTY_NOTICES.md` |
| 6 | `docs/verify_scorecard.py` ships **inside the release artefact**, not only in the repository | `blocked: no release run` | `docs/verify_scorecard.py` · `.github/workflows/release.yml` · `crates/logweir/tests/workflow_lint.rs` · **2026-09-11, still blocked. What would close it: the download URL of the published install bundle asset, with the reader inside it.** |
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

**2026-09-11 — Task 30b ran and stopped here.** The mechanism now exists and the
clause is unchanged. `.github/workflows/release.yml` carries a `pullback` job:
`needs: [image]`, a fresh `ubuntu-24.04` runner with an empty daemon and no
build cache, which pulls the runner digest and pulls the controller manifest
digest **twice, once per `--platform`**, re-runs `scripts/check-image.sh` and
`scripts/check-image-weirkeeper.sh` against what came back, and writes both
digests and the pull transcript into the run's job summary.
`crates/logweir/tests/workflow_lint.rs`'s
`workflow_lint_the_pullback_job_builds_nothing` holds its shape. **That job has
never run**, because no tag has been pushed (the remote exists since 2026-09-12), so the
transcript that would close this row does not exist. The digests in
`logweir.yaml` are still one laptop's — and Task 30b moved the controller one
again (the fourth time in this plan), which is the same point restated: a local
digest is the measurement of one build.

### 2 — Demo 1, in CI, on a workflow-created cluster

**`closed`, 2026-09-12, by run
<https://github.com/VladyslavHaina/logweir/actions/runs/34700987743>** — one
green run of `.github/workflows/kind-demo.yml` on commit `a113dd2`, on a `kind`
cluster that run created. What **is** recorded is the whole walkthrough run on
docker-desktop end to end and checked in at `e2e/k8s/laptop-demo.md` — preflight,
the install gate, the keypairs, the Secrets, the roster, the cluster, a schedule
and the backup it fires, the approval minted out of band, both readers over the
scorecard and the teardown — with
`crates/logweir/tests/laptop_demo_lint.rs` refusing a transcript that has been
forged rather than run. The docker-desktop half of the clause is therefore
evidenced; the CI half was not until 2026-09-12 — the row read `blocked` for the whole clause
because a row carries one status, and reads `closed` since the run recorded below.

**2026-09-11 — Task 31 built the CI half and stopped here.** The workflow now
exists: `.github/workflows/kind-demo.yml` creates a `kind` cluster from the
digest-pinned `e2e/k8s/kind-config.yaml` (Kubernetes 1.29, Global Constraint
25's floor), brings the five-listener compose stack up, builds both images with
their named producers, installs, and runs `scripts/kind-demo.sh` — which reads
the kind network's IPv4 gateway, adds a CoreDNS `hosts` block so the advertised
listener `host.docker.internal:9095` resolves in every pod, probes the broker
from inside the cluster, and then runs the twelve steps of
`scripts/demo-steps.sh`, the same twelve the laptop walkthrough recorded.
`crates/logweir/tests/workflow_lint.rs` and
`crates/logweir/tests/laptop_demo_lint.rs` hold its shape.

**2026-09-12 — that workflow ran, and this row closed.** The repository was
pushed to <https://github.com/VladyslavHaina/logweir> on 2026-09-12 and
`kind-demo.yml` was red on its first eight runs — seven on environment facts the
laptop had hidden, and the eighth on the demo's step 3 meeting the laptop's pins,
which is what Task 33 fixed. The ninth run, run id **34700987743** on commit
`a113dd2`, is green end to end: the five-listener compose stack from quay, both
images built natively on the amd64 runner, a `kind` cluster from the pinned
`e2e/k8s/kind-config.yaml`, the **author-only** install branch, the CoreDNS
`hosts` patch, an in-cluster probe reporting `reachable=true`, the preflight on
context `kind-logweir`, and then all twelve steps — `Backup` reaching
`phase: Succeeded`, X-UIWRITE (a) returning `HTTP 201` through `kubectl proxy`,
the approval minted with the shipped CLI, `Restore` reaching `phase: Succeeded`
with `status.outcome: pass`, both readers over the scorecard (`VALID … outcome=pass`,
verifier 1.13.0), `PHASE C EXIT CRITERION MET`, and a clean teardown.

**It closes this clause and no other.** The run installed the **author-only**
branch: both images were built by that run and `kind load`ed onto the node, never
pulled from a registry. Global Constraint 37 is untouched by it, clause 1 stays
`blocked: images not published`, and nothing here may be read as evidence that
an image was published.

Before that run, two things had been proven instead, and neither was this clause: the script was proven **dry** (a stubbed
`kubectl` and `docker`, the twelve steps invoked in order after the CoreDNS patch
and the probe — **with tracers standing in for the step bodies** — the real
`step_01` then walked through under the `kind` driver by execution, and the
gateway-resolution failure path exiting 1), and one local
`kind` proving run was made under STANDING RULE 16's single authorised
exception, deleting its cluster in the same session. Its transcript is
`docs/kubernetes.md` §18.5 and it is labelled there for what it is — author-only
images on an arm64 host, which is neither evidence for this clause nor for
clause 1. That run proved the name resolution and the listener from a pod and
stopped before the twelve steps: an arm64 `kind` node cannot start the
amd64-only runner image, so the steps have not yet run on `kind` anywhere. Both
install branches are written now, so the day a remote exists this row needs a
run and not a rewrite.

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
is blocked. Task 30b added
`workflow_lint_every_platform_is_asserted_before_the_manifest_push` for the
multi-architecture controller push: every platform the manifest list carries has
a `docker load` and an assert step before the push, in the pushing job, and each
was compiled on a runner of its own architecture. That test now exists, in
`crates/logweir/tests/workflow_lint.rs`, which row 4 names.

### 4 — Task 9's F1–F4, and one real run

**`blocked: no tag pushed`**, and the clause has two halves that are stated
separately rather than averaged. The reason changed on 2026-09-12 and the state
did not: a remote now exists — <https://github.com/VladyslavHaina/logweir> — and
`release.yml` still has not run, because it fires on a pushed tag and no tag has
been pushed.

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

Task 30b added four more, and renamed one. New:
`workflow_lint_every_platform_is_asserted_before_the_manifest_push` (every
platform inside the pushed manifest list was loaded and asserted first, in the
pushing job, and was compiled on a native runner of its own architecture),
`workflow_lint_the_pullback_job_builds_nothing` (the job that pulls the
published digests back compiles nothing and pushes nothing) and
`the_release_artefact_ships_the_auditors_reader` (clause 6's subject, below).
Renamed: Task 30's `workflow_lint_the_asserted_image_is_the_pushed_image` is now
`workflow_lint_every_image_is_asserted_before_push`, because there are three
images and the singular had gone stale.

The **"runs for real, once" half is blocked**: `.github/workflows/release.yml`
has never executed on any commit — no tag has been pushed.
**2026-09-11 — Task 30b built the release, and stopped at the release step.**
**2026-09-12 — the repository was pushed and three workflows ran; this was not
one of them.** `ci.yml` (run 34700987730), `no-oso.yml` (run 34700987811) and
`kind-demo.yml` (run 34700987743) are all green on commit `a113dd2`;
`release.yml`, `release-drill.yml` and `engine-matrix.yml` have zero runs,
because the first needs a pushed tag and the other two need a schedule that has
not fired. `git tag` lists `v0.1.0` at its old commit in the local tree and the
remote carries no tags at all. What would close this half is one thing and it is
not in this tree: the URL of a green tagged run.

### 5 — the licence and attribution files

**`closed`.** `crates/logweir/tests/doc_lint.rs` holds four of the five parts in
the default test set —
`every_doc_footer_carries_the_asf_sentence_and_the_docs_licence` (the ASF
sentence in every Markdown file under `docs/` and at the repository root, with a
CC-BY-4.0 link that has to **resolve** on disk),
`contributing_requires_a_dco_signoff_and_no_cla`,
`every_image_copies_the_licence_and_the_notice` (the `COPY` instructions in all
three Dockerfiles), and
`the_notice_names_every_librdkafka_component_in_licenses_txt`.
`scripts/check-dod.sh` is the fifth: it requires each release file to be present
and non-empty, and its attribution arm walks **every** Markdown file in the tree
rather than only the ones under `docs/` — `e2e/k8s/laptop-demo.md` is covered by
that arm and by nothing else. The image half is proved at runtime by
`scripts/check-image-weirkeeper.sh` check 3, which refuses an image whose
`/usr/share/licenses/logweir/LICENSE` or `NOTICE` is absent or empty.

### 6 — the auditor's reader, inside the release artefact

**`blocked: no release run`.** `docs/verify_scorecard.py` exists, is the auditor's
independent reader, and is exercised on every signed artefact by
`scripts/check-verifier-parity.sh` and by the e2e suite. What is missing is a
**published release artefact that contains it**: an auditor's independence is
not real if the script lives only in a repository they have to clone.

**2026-09-11 — Task 30b built the artefact and could not publish it.**
`.github/workflows/release.yml`'s `publish` job now assembles
`logweir-<tag>-install.tar.gz` out of `logweir.yaml`, `LICENSE`, `NOTICE`,
`THIRD_PARTY_NOTICES.md`, `docs/verify_scorecard.py` and `ui/` — with `ui/tests/`
excluded by a stated pattern, so the bundle carries no fixture naming a
developer's compose stack — and publishes a sha256 listing of every file it
ships under `ui/` in the release notes.
`crates/logweir/tests/workflow_lint.rs`'s
`the_release_artefact_ships_the_auditors_reader` asserts all of that, and now
exists, which is why this row names that file. What does **not** exist is a
published artefact. **2026-09-12: the remote exists now and the reason narrowed
to one thing** — `release.yml` has never run, because no tag has been pushed, so
there is still no release and nothing to download. The row stays blocked,
because the clause is about a file an auditor can fetch and not about a workflow
that says it would produce one.

### 7 — the drift gate and the parity gate

**`closed`.** Two drift arms, one per document. `.github/workflows/ci.yml`
carries them as the steps named *schema drift* (the scorecard) and *backup
receipt schema drift* (Task 5, deliberately a separate step so a failure names
which schema drifted). `ci.yml` executes on every push since 2026-09-12 and mirrors them; the **enforcing**
copies on a laptop are the two tests in `crates/logweir-core/tests/schema_drift.rs` —
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
and not a stop-work: the registry namespace is the literal `docker.io/vladyslavhaina/…`
on Docker Hub
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
which is what makes it a gate on a laptop: `ci.yml` mirrors `just gate` (green since
2026-09-12) and runs on a push, the recipe before one. `crates/logweir/tests/label_gate.rs` keeps the
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
