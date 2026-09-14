# The default recipe is the release gate (Task 22 step 8), so it must be able to
# FAIL on formatting: it never runs the rewriting `fmt` recipe. Run `just fmt`
# yourself to fix formatting; `lint` checks it, and also runs the guard scripts
# listed in the `lint` recipe below — read the recipe for the current set. This
# comment deliberately does not enumerate them, so it cannot go stale.
# check-verifier-parity.sh and check-invariant-corpus.sh need a `python3` with
# the `cryptography` package (Tasks 3 and 4) — they FAIL rather than skipping
# when the package is missing, because the two-reader parity claim is not
# checkable without the second reader.
# check-invariant-corpus.sh is the auditor-side half: it walks
# e2e/fixtures/invariants/index.json with the Python reader alone, so the
# corpus is still checked where there is no Rust toolchain.
default: lint test

fmt:
    cargo fmt --all

# The `phase9_teardown.rs` grep in `lint` is T0-11's, and it is a grep rather
# than a script because what it guards is a DOC COMMENT — the one kind of claim
# no unit test reads. `phase9_teardown::persist` promised "a teardown that
# cannot be attested is exit 4 rather than a silent success" one sentence away
# from the sentence that contradicts it, and the call site proved the promise
# false. The claim is deleted; this keeps it deleted. `test -f` runs first so a
# renamed file fails here rather than passing on grep's exit 2, and
# `crates/logweir/tests/teardown.rs::the_false_exit_4_guarantee_is_gone_and_the_gate_keeps_it_gone`
# keeps this line's membership in the recipe honest.
#
# Task 14 appends `check-withdrawn-claim.sh` after it — G-SIGN's second half,
# the corpus grep that keeps the withdrawn stronger claim about signing off
# every shipped surface. `ci.yml` mirrors `just gate` (green since 2026-09-12), and
# membership in THIS recipe is what makes it enforced on a laptop before a push;
# `crates/logweir/tests/withdrawn_claim.rs::the_withdrawn_claim_gate_is_in_just_lint`
# keeps it here.
#
# Task 19 (chain J, slot 9) appends `check-no-archive-write.sh` last — G-RET,
# the capability gate for the retention path. It greps `crates/weirkeeper/src`
# for `Store::from_url`, the put methods and a RECEIVER-ANCHORED `.delete(`,
# with comment and doc-comment lines stripped first, and never for the bare
# word delete: that is a Kubernetes verb the controller legitimately holds on
# Jobs and an ordinary English word in the doc comments this design requires.
# The question a guard has to answer about a deletion is not "did it?" but
# "CAN it?", which is why this is a grep beside `check-one-signer.sh` and not
# a behavioural test.
# `crates/weirkeeper/tests/retention.rs::the_no_archive_write_gate_is_in_just_lint`
# keeps it here.
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    ./scripts/check-no-oso.sh
    ./scripts/check-pure-core.sh
    ./scripts/check-verifier-parity.sh
    ./scripts/check-invariant-corpus.sh
    ./scripts/check-deps-count.sh
    ./scripts/time-unit-suite.sh
    ./scripts/check-one-signer.sh
    test -f crates/logweir/src/drill/phase9_teardown.rs && ! grep -q 'exit 4 rather than a silent success' crates/logweir/src/drill/phase9_teardown.rs
    ./scripts/check-withdrawn-claim.sh
    ./scripts/check-no-archive-write.sh
    ./scripts/check-ui-offline.sh
    ./scripts/check-ui-behaviour.sh
    ./scripts/check-unverified-labels.sh

# Task 7 (Phase 1 line item 1c). G2′: the set of workspace crates from which
# the signing API is reachable is exactly {logweir, e2e}, computed from the
# dependency graph rather than a text search.
#
# A LINK-TIME PROPERTY AND NOTHING MORE. It does NOT prove the control plane
# cannot sign: the signing key is a Kubernetes Secret and `create pods` in its
# namespace is equivalent to holding it. The stronger claim was withdrawn once
# already and must not be restated here or in the script's output.
#
# Membership in `lint` above is what makes this a gate: ci.yml has never
# executed on any commit, so the workflow step is documentation.
# `crates/logweir/tests/one_signer_gate.rs::just_lint_runs_the_one_signer_gate`
# keeps the membership honest.
check-one-signer:
    ./scripts/check-one-signer.sh

# Task 5b. The artifact-directory ceiling. `target/debug/deps` reached 873,349
# files / 43.5 GiB across five tasks of mutation rounds that nobody cleaned up
# after, and at that size cargo spends ~30 s PER TEST BINARY fingerprinting the
# directory: `cargo test --workspace` took twenty minutes at 0% CPU and was
# twice mistaken for a hang. Fails over 50,000 files and PRINTS THE COUNT on
# every run, so the trend is readable long before it fails. Part of `lint`.
deps-count:
    ./scripts/check-deps-count.sh

# Task 5b. THE RULE mutation rounds inherit.
#
# FIRST: a mutation round does not run in this working tree at all. It runs in
# an isolated worktree or clone, because a round leaves mutated source behind
# whenever it is interrupted, and "there were backups" is not a property anyone
# can check afterwards.
#
# SECOND, wherever it does run: build it under a throwaway target dir, so it
# never pollutes a shared artifact set —
#
#     just mutant "test --workspace --lib doctor"
#     just mutant-clean
#
# — because five tasks of rounds that did not is what put 873,349 files in
# target/debug/deps and turned a 13-second suite into a twenty-minute one.
# `just deps-count` above is the detection for the round that forgot. See
# docs/stability.md, "The unit suite dials nothing; the e2e suite dials".
mutant ARGS:
    CARGO_TARGET_DIR=target/mutants cargo {{ARGS}}

mutant-clean:
    rm -rf target/mutants

# Task 5b, wired into `lint` in fix round 1 (review F3). The suite's OWN clock,
# and the check that keeps it honest. Bounds the whole default suite
# (LOGWEIR_UNIT_SUITE_BUDGET_SECS, default 120) AND every individual test
# (LOGWEIR_UNIT_TEST_BUDGET_SECS, default 5).
#
# THE PER-TEST BOUND IS THE ONLY THING THAT CATCHES A RE-ADDED DIALER. The
# reviewer verified it: deleting the `#[cfg(feature = "e2e")]` from
# `check_7_…` puts a 20 s broker wait back in the default suite, and the grep
# audit does not see it (its address is `127.0.0.1:1`, and by ruling B1(a) no
# address could usefully be a token, since every address costs the same 20 s).
# This harness caught it — `FAIL 20.31s check_7_…`, exit 1 — while the 120 s
# SUITE budget stayed green at 44 s. So the catcher has to run automatically,
# and that means here.
#
# IT REFUSES TO RUN, EXIT 1, WHILE 9092 OR 9000 ANSWERS. A timing number taken
# against a live stack is about a different machine than the one this bound is
# for, and a gate that reports PASS without having measured anything is the
# defect this whole task exists to remove. The consequence, stated plainly:
# **`just lint` fails while the compose stack is up.** Run `just e2e-down`
# first, or run `just e2e` (which wants the stack up) as the separate phase it
# is. Exiting 0 with a "skipped" line was considered and rejected — that is a
# check that cannot fail.
time-unit-suite:
    ./scripts/time-unit-suite.sh

test:
    cargo test --workspace

golden:
    INSTA_UPDATE=always cargo test --workspace

# Regenerates BOTH checked-in schemas. Tag 1 ships two (Global Constraint 13
# as revised): the drill scorecard and the backup receipt. The CI drift arms at
# .github/workflows/ci.yml regenerate each one into /tmp and `diff -u` it
# against the file here, so a schema that stopped describing its type is a diff
# a reviewer sees rather than a surprise at validation time.
#
# `just` runs each recipe line in its own shell, so the two redirects cannot
# interfere. Neither target is a SIGNED artefact — unlike `fixtures-sign`'s,
# these files carry no signature and a truncated redirect target costs a
# `just schema`, not a re-mint (Task 5).
schema:
    cargo run -p logweir-core --example emit_schema > schemas/logweir-drill-scorecard-1.0.0.json
    cargo run -p logweir-core --example emit_backup_receipt_schema > schemas/logweir-backup-receipt-1.0.0.json

# Task 32, chain J slot 25. THE CHECKING HALF of `schema` above — the recipe
# `just gate` runs, because `schema` itself WRITES the tracked files and a gate
# that rewrites the thing it is checking cannot fail on its own.
#
# THE DETECTION IS THE POINT: regenerate, then require `git status --porcelain
# -- schemas/` to come back EMPTY. `schemas/` is regenerated by the recipe and
# must return byte-identical; anything else is drift, and the porcelain listing
# names which file drifted. `.github/workflows/ci.yml` does the same job with
# two `diff -u`s into /tmp — that one executes on every push since 2026-09-12, and
# this is the copy a laptop enforces before one.
#
# ONE RECIPE, COVERING BOTH SCHEMAS, because `just schema` regenerates both in
# one go: splitting the check would mean claiming two checks where the tree has
# one command. `receipt-schema-check` below is a SECOND NAME for this same
# recipe (the plan's gate list names both), and `just gate` runs both names — so
# a reader who types either gets the whole check rather than half of one.
#
# `just schema`'s status is read on the next line (STANDING RULE 20); nothing
# here is piped.
schema-check:
    #!/usr/bin/env bash
    set -uo pipefail
    cd "{{justfile_directory()}}"
    just schema
    rc=$?
    if [ "$rc" -ne 0 ]; then
      echo "schema-check: \`just schema\` exited $rc — the two schemas could not be regenerated," >&2
      echo "  so nothing was compared. That is a build failure, not a drift verdict." >&2
      exit "$rc"
    fi
    drift="$(git status --porcelain -- schemas/)"
    if [ -n "$drift" ]; then
      echo "schema-check: FAIL — regenerating the schemas CHANGED schemas/." >&2
      echo "  A checked-in schema has stopped describing its type. Global Constraint 12 makes" >&2
      echo "  this a format change: land the regenerated file in the same commit as the type," >&2
      echo "  with a SCRIPT_VERSION bump and a corpus case if a reader is affected." >&2
      printf '%s\n' "$drift" >&2
      exit 1
    fi
    echo "schema-check: ok — schemas/ came back byte-identical (both documents)"

# The plan's gate list names the backup-receipt drift check separately, and the
# tree has one regenerator for both documents. This is that same recipe under
# the second name rather than a second, thinner check: a name that ran a
# narrower check than the one people think it runs is worse than a duplicate.
receipt-schema-check: schema-check
    @echo 'receipt-schema-check: the same recipe as just schema-check — just schema regenerates BOTH schemas, so one drift check covers both documents'

# The Python auditor verifier. Task 7 — needs `pip install cryptography pytest`.
#
# ONE INTERPRETER-RESOLUTION ORDER IN THE REPOSITORY, and this is the second
# copy of it: $LOGWEIR_PYTHON, then $LOGWEIR_E2E_PYTHON, then the repo-local
# .e2e/venv, then a bare python3 — token for token the order
# `scripts/check-verifier-parity.sh` resolves (its if/elif chain), asserted
# equal by `crates/logweir/tests/extract_engine.rs`'s
# `verify_py_resolves_the_interpreter_like_the_parity_gate`. It used to be a
# hardcoded system `python3`, which is the one interpreter on a contributor's
# machine that is guaranteed NOT to be the venv the rest of the repository
# built for `cryptography` — so this recipe failed on exactly the trees where
# the parity gate passed. It is ONE recipe line because `just` runs each line
# in its own shell, and no new script is added for it (Global Constraint 38:
# nothing new enters the tree that an existing file can carry).
verify-py:
    "${LOGWEIR_PYTHON:-${LOGWEIR_E2E_PYTHON:-$([ -x .e2e/venv/bin/python3 ] && echo .e2e/venv/bin/python3 || echo python3)}}" -m pytest docs/test_verify_scorecard.py -q

# Mints the checked-in signed fixtures under e2e/fixtures/signed/. Task 6.
#
# NON-DESTRUCTIVE, deliberately. This used to redirect straight into the
# tracked, signed, fingerprint-pinned e2e/fixtures/signed/scorecard.json — and
# the shell truncates a redirect target BEFORE the program runs, while
# `emit_fixture` itself ends with `sc.validate_invariants().expect(...)`. So a
# generator that refuses its own document zeroed the committed fixture first
# and panicked second. Emitting into target/ and `mv`-ing on success means a
# failing generator leaves the tracked fixture byte-identical. `just` runs each
# recipe line in its own shell, so the mkdir must be its own line and the
# redirect and the mv must share one.
# `crates/logweir-core/tests/fixture_regen.rs::fixtures_recipe_is_non_destructive`
# keeps it that way.
#
# Task 5's `mint_backup_receipt_fixture` needs no redirect and no `mv` at all:
# it writes e2e/fixtures/signed/backup-receipt.json AND the .sig over exactly
# those bytes itself, in one process, after validating the document against its
# own five arms — so there is no window in which the tracked document and
# the tracked signature over it disagree.
fixtures-sign:
    mkdir -p target/fixtures-tmp
    cargo run -p logweir-core --example emit_fixture > target/fixtures-tmp/scorecard.json && mv target/fixtures-tmp/scorecard.json e2e/fixtures/signed/scorecard.json
    cargo run -p logweir-evidence --example mint_fixture
    cargo run -p logweir-evidence --example mint_backup_receipt_fixture

# The DELIBERATELY BOGUS fixture: a validly signed scorecard whose only defect
# is its own self_attested claim. Additive — writes ONLY the two -bogus files
# and re-mints nothing (ruling R-G, plan.md:60). Task 3.
fixtures-sign-bogus:
    cargo run -p logweir-evidence --example mint_bogus_fixture

# Bring the compose stack up, seed topics, produce, back up. See Task 21.
# `--wait` blocks until every non-profiled service reports HEALTHY, so this
# recipe cannot hand back a broker that is merely `Running` and not yet
# serving. The two one-shot setup services sit behind the `setup` profile
# precisely so they are not in that wait set (`--wait` exits 1 the moment a
# container it is waiting on exits, even with status 0); they run here, in
# order, as foreground commands whose exit codes `just` checks.
#
# Task 7 (chain J, slot 10) adds the THIRD setup step, `scram-setup`, and its
# exit code is load-bearing: in KRaft the SCRAM credential is a user config
# record that has to be written by a client after the quorum is serving, so
# every SASL client in this suite depends on this line having exited 0. `just`
# checks each recipe line's status, and `docker compose run --rm` returns the
# container's own exit code, so a broker that refused the alter fails
# `just e2e-up` here rather than surfacing as an authentication error in a test
# twenty minutes later. It runs LAST of the three because it is the only one
# that needs the broker to be answering client requests, and `topic-setup`
# already proves that.
e2e-up:
    docker compose -f e2e/compose/docker-compose.yml up -d --wait
    docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm minio-setup
    docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm topic-setup
    docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm scram-setup

# Both profiles are named on purpose. `down` only removes containers for
# services in the ACTIVE profile set, so a plain `down -v` walks past any
# `topic-setup` / `minio-setup` / `kafka-backup` container left over from an
# earlier `up`, and the next `down` walks past it again — verified: it survived
# a full `down -v` on this machine. `--remove-orphans` additionally clears
# containers for services this file no longer defines.
e2e-down:
    docker compose -f e2e/compose/docker-compose.yml --profile setup --profile tools down -v --remove-orphans

# AWS_EC2_METADATA_DISABLED (fix round 1, review F7): with no AWS credentials
# in the environment, `AmazonS3Builder::from_env()` falls through to the EC2
# instance-metadata credential provider and object_store spends ten retries on
# the link-local 169.254.169.254 before the configured endpoint is contacted at
# all. MEASURED on `check_storage_skips_a_genuinely_unreachable_endpoint`:
# 9.86 s without this variable, 4.21 s with it. It is off-loopback traffic from
# a test (GC17) and it buys nothing — no test in this repository runs on an EC2
# instance, and the MinIO credentials the e2e harness needs come from the
# environment, never from IMDS. Set for the whole run rather than per test:
# nothing here should ever probe it.
#
# `--no-fail-fast` (Task 11): without it cargo stops at the FIRST test binary
# that reports a failure and never runs the later ones, so one red suite hides
# every suite after it in the alphabet — a reviewer reading the output sees one
# failure and no information at all about `pitr_boundary`, `scram` or `smoke`.
# The e2e suites are independent (they share the stack, not state), and
# `--test-threads=1` still serialises them, so running all of them and
# reporting every failure is both safe and the only way the run is diagnostic.
# cargo's own exit code is unchanged: non-zero if any binary failed.
e2e:
    AWS_EC2_METADATA_DISABLED=true cargo test --workspace --features e2e --no-fail-fast -- --test-threads=1 --nocapture

# Produce, back up with the pinned engine, and refresh the two fixtures that
# must come from a REAL archive. Needs a FRESH stack (`e2e-down` then `e2e-up`);
# it refuses a dirty one, because a second backup into the same backup_id does
# not accumulate and would leave a partial archive.
#
# A SUCCESSFUL RUN DIRTIES THE TREE and is not byte-reproducible: record
# timestamps differ per run, so the zstd frames and the manifest timestamps do
# too. Any CI job that calls this MUST NOT `git diff --exit-code` afterwards.
# The committed fixtures are checked instead by the default (Docker-free) test
# set — see crates/logweir-engine-oso/tests/kbak.rs.
# `scripts/demo.sh` seeds with LOGWEIR_SEED_REFRESH_FIXTURES=0, which does
# everything except the fixture refresh and leaves the tree clean — the
# quickstart must not hand a stranger two modified tracked files. THIS recipe
# is the maintainer form and refreshes them on purpose.
e2e-seed:
    ./scripts/e2e-seed.sh

demo:
    ./scripts/demo.sh

# Task 19 fix round 2 (review FIX 6): runs every `#[ignore]`d marker test in
# the workspace — currently just `oso_cli_engine_must_override_validation_run_once_docker_is_available`
# (crates/logweir-engine-oso/tests/engine.rs), which tracks the
# `DataEngine::validation_run` override still owed on `OsoCliEngine`, blocked
# on Docker / the extracted kafka-backup binary in this environment. Expected
# to FAIL today — that is the point: CI actually runs this (see ci.yml,
# continue-on-error so it stays informational rather than blocking merges)
# so the obligation cannot go unnoticed the way a stub nobody runs would let
# it. The day the real override lands, this recipe (and the CI step) turns
# green on its own.
check-todo-markers:
    cargo test --workspace -- --ignored

# Resolve the pinned OSO image by digest and extract the engine binary.
# Run this once after a fresh clone; `cargo test --workspace` needs .engine/.
engine:
    ./scripts/extract-engine.sh

# Task 22. Broken RELATIVE markdown links across the docs and the root files.
# http/https are never fetched (Global Constraint 17); this is a repository
# integrity check, not a network check.
links:
    ./scripts/check-links.sh docs/ README.md SECURITY.md MAINTAINERS.md CONTRIBUTING.md TRADEMARKS.md THIRD_PARTY_NOTICES.md third_party/ e2e/fixtures/ ui/ charts/

# Task 22. The v0.1.0 definition of done, as far as it is mechanically
# checkable without GitHub Actions or a live stack. Prints what it could NOT
# check as UNVERIFIED rather than skipping it silently.
dod:
    ./scripts/check-dod.sh

# Task 8. Build the runtime image into the LOCAL daemon, linux/amd64 explicitly:
# the engine layer (Dockerfile:164) has no arm64 manifest, and `imagePullPolicy:
# Never` (Tasks 16/17) needs the image in the daemon, not in a registry.
#
# Task 8b: THE RUST COMPILE IS NO LONGER EMULATED. The builder stage runs on
# `$BUILDPLATFORM` and cross-compiles to x86_64, so on an arm64 host this costs
# minutes rather than the 3044 s Task 8 measured — see the wall-clocks in
# docs/stability.md, under "Known limitations of v0.1".
#
# THE NAMED PRODUCER of the local `logweir:check` tag. Tasks 16, 17 and 19 need
# a locally built image and must call this recipe rather than open-code a
# `docker build`, so there is one place where the platform is stated.
image:
    docker build --platform linux/amd64 -t logweir:check .

# Task 8 / Phase 1 line item 1f. THE gate for the image, replacing the release
# workflow, which has never run (no tag has been pushed). One
# implementation (scripts/check-image.sh), two callers: this recipe and
# release.yml (Task 9).
#
# EVERY EXIT CODE BELOW IS READ DIRECTLY. `just` runs each line in its own
# shell and aborts on the first non-zero status, so nothing here is piped and
# no status is swallowed — which is the failure this whole extraction is about.
#
# DELIBERATELY NOT PART OF `lint`, `test`, `default` OR `e2e`: an amd64 image
# build must never become a precondition of the Docker-free test run. That was
# true when the build was emulated and stays true now that it is cross-compiled
# — it is still a whole-workspace release compile plus a docker daemon. The
# image tests are `#[ignore]`d for the same reason and are run here,
# explicitly, with `--ignored`. Task 22 decides where `smoke` sits in `just gate`.
smoke: image
    bash scripts/check-image.sh logweir:check
    cargo test -p e2e --features e2e --test check_image -- --ignored --test-threads=1

# Task 15b (chain J, slot 5). Regenerate the six checked-in CRDs.
#
# THE SIBLING OF `schema`, ABOVE, AND FOR THE SAME REASON. A CRD change is a
# FORMAT change: the six files under config/crd/ are what `kubectl apply`
# consumes, what the UI's forms are written against and what Tasks 16-24 read,
# so a change to one has to appear as a diff in a pull request rather than as a
# surprise at apply time. `.github/workflows/ci.yml`'s third drift arm renders
# into a temporary directory and `diff -u`s these files against it, beside the
# scorecard-schema arm that has done the same job since Phase 1.
#
# DELIBERATELY NOT PART OF `lint`. This recipe WRITES the tracked files, and a
# gate that rewrites the thing it is checking cannot fail. The checking half is
# `crates/weirkeeper/tests/crd_shape.rs::the_checked_in_crds_are_what_the_emitter_renders`,
# which is in the default `cargo test` set and compares the checked-in bytes
# against the same renderer IN-PROCESS — no subprocess, no shell, so it holds
# on a laptop as well as in CI (ci.yml mirrors it; green since 2026-09-12).
#
# `crds` and `schema` stay independent: two formats, two gates, no dependency
# edge between them.
crds:
    cargo run -p weirkeeper --example emit_crds -- --out config/crd

# Task 32, chain J slot 25. THE CHECKING HALF of `crds` above, and the CRD drift
# check this plan has run by hand at every landing: regenerate, then require
# `git status --porcelain -- config/crd` to come back EMPTY.
#
# `crds` itself WRITES the six tracked files, so it is not a gate; this is.
# `crates/weirkeeper/tests/crd_shape.rs::the_checked_in_crds_are_what_the_emitter_renders`
# compares the same bytes IN-PROCESS and is the cheaper, always-on half — this
# recipe is the one that also catches a renderer whose output depends on
# something the in-process comparison does not see (a file `--out` writes that
# the test does not read, an emitter that drops a kind).
crds-check:
    #!/usr/bin/env bash
    set -uo pipefail
    cd "{{justfile_directory()}}"
    just crds
    rc=$?
    if [ "$rc" -ne 0 ]; then
      echo "crds-check: \`just crds\` exited $rc — the CRDs could not be regenerated, so nothing" >&2
      echo "  was compared. That is a build failure, not a drift verdict." >&2
      exit "$rc"
    fi
    drift="$(git status --porcelain -- config/crd)"
    if [ -n "$drift" ]; then
      echo "crds-check: FAIL — regenerating the CRDs CHANGED config/crd." >&2
      echo "  A CRD change is a FORMAT change: config/crd/ is what \`kubectl apply\` consumes and" >&2
      echo "  what the UI's forms are written against. Land the regenerated files in the same" >&2
      echo "  commit as the type change, so it is a diff a reviewer reads." >&2
      printf '%s\n' "$drift" >&2
      exit 1
    fi
    echo "crds-check: ok — config/crd came back byte-identical (six kinds)"

# Task 35: THE CHART GATE. `charts/logweir` is derived from `config/` and `ui/`,
# and this is what keeps it derived: the six CRDs and the fourteen UI files
# byte-identical to the tree (`cmp`), `helm lint` for the defaults and every
# example, `helm template` regenerated into `charts/logweir/rendered/` with the
# `crds-check` drift idiom (porcelain must be empty), every rendered image a
# digest (the author-only render exempt by name), the values schema refusing a
# non-boolean flag, and `values.yaml` carrying the tree's own pins. Refuses
# without helm >= 4, naming it. In `just gate`, right after `crds-check`;
# `crates/logweir/tests/gate_lint.rs` keeps it there.
chart-check:
    bash scripts/check-chart.sh

# Task 11, chain J slot 12. **G-PITR on its own**: the inclusive point-in-time
# boundary, across three partitions, proved against the real stack — without
# waiting for the whole e2e suite. One test, named exactly, so the reviewer who
# wants to see the guard (or watch a mutant kill it) pays for one restore
# instead of twenty.
#
# THIS RECIPE ASSUMES THE STACK IS ALREADY UP. It does not run `e2e-up` and it
# does not run `e2e-down`: the compose stack is a shared resource with one
# owner at a time (STANDING RULE 3), and a recipe that tore it down would pull
# it out from under whoever was using it. Run it as:
#
#     just e2e-up
#     just pitr; echo "rc=$?"
#     just e2e-down
#
# `just e2e-up` ALONE is enough, and that is measured: this row needs nothing
# from `scripts/e2e-seed.sh`. It creates its own topic, produces its own nine
# records with explicit CreateTime, takes its own backup under its own
# `backup_id`, restores in `newTopic` mode (which requires no marker topic),
# and sweeps that archive out of the shared bucket at both ends. All it wants
# from the stack is a broker and the two buckets `e2e-up` creates.
#
# `cargo build -p logweir` FIRST, and it is not optional (plan erratum E9): the
# harness runs `target/debug/logweir`, and `cargo test -p e2e` does not rebuild
# the BINARY — only the library it links. Without this line the row tests
# whatever binary was left over from an earlier build, which is how a mutant
# survives a run that looks green.
pitr:
    cargo build -p logweir
    AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e --test pitr_boundary -- --exact pitr_boundary_includes_the_record_whose_timestamp_equals_point_in_time --test-threads=1 --nocapture

# Task 12, chain J slot 13. **PHASE A'S EXIT CRITERION**, on the local stack:
# one recipe from a source topic to a verified point-in-time restore into a NEW
# topic and a signed receipt.
#
#     produce -> logweir backup run -> receipt verified by BOTH readers
#             -> logweir drill approve -> logweir restore run
#                (target.mode: newTopic, restore.point_in_time)
#             -> scorecard verified by BOTH readers -> one summary line
#
# IT IS NOT `just demo`. `scripts/demo.sh` takes its archive from the HARNESS
# (`scripts/e2e-seed.sh`, whose backup step is the pinned engine invoked
# directly) and restores into a SCRATCH cluster at no point in time. This
# recipe drives the PRODUCT's own `backup run` and its `newTopic` restore, and
# it is the single command Phases B, C and D all cite as "the CLI path works".
#
# THIS RECIPE ASSUMES THE STACK IS ALREADY UP, exactly as `pitr` above does and
# for the same reason: the compose stack is a shared resource with one owner at
# a time (STANDING RULE 3), and a recipe that brought it down would pull it out
# from under whoever was using it. Run it as:
#
#     just e2e-down && just e2e-up
#     just mvp-demo; echo "rc=$?"
#     just e2e-down
#
# `just e2e-up` ALONE is enough and that is measured: the demo needs nothing
# from `scripts/e2e-seed.sh`. It produces its own records into `orders` and
# `payments`, takes its own backup under its own `backup_id`, restores in
# `newTopic` mode (which requires no marker topic), and sweeps that archive out
# of the shared bucket at both ends — from a `trap … EXIT`, so a run that dies
# mid-flight sweeps too. It REFUSES a stack whose source topics already hold
# records, which is what makes a second run exit 1 at step 1 rather than write
# a partial archive.
#
# `cargo build -p logweir` FIRST, and it is not optional (plan erratum E9): the
# script runs `target/debug/logweir` through a one-entry shim directory on
# `$PATH`, so the demo and the e2e suite test the same bytes. Without this line
# the demo runs whatever binary was left over from an earlier build, which is
# how a mutant survives a run that looks green.
mvp-demo:
    cargo build -p logweir
    ./scripts/mvp-demo.sh
# Task 21, chain J slot 14. The three install recipes — interface **I26**.
#
# `just apply-check` WAS TWO DIFFERENT JOBS UNDER ONE NAME, and it is split
# into three (critique B M15). X-APPLY is "apply the file twice and read both
# exit codes"; the missing-Secret check is "inspect a namespace and refuse";
# and the second would make the first FAIL on a clean cluster — no Secrets yet
# — which is precisely the state X-APPLY is defined against. They must never be
# chained. `crates/logweir/tests/manifest_lint.rs`'s
# `check_secrets_refuses_a_missing_secret` asserts they are not.

# Regenerate `logweir.yaml` from `config/`.
#
# `scripts/render-install.sh` is the ONLY producer of that file. The checking
# half is `./scripts/render-install.sh --check` (which re-renders into a temp
# file and `diff -u`s) and, without a subprocess,
# `crates/logweir/tests/manifest_lint.rs::install_yaml_has_no_drift`, which
# compares the rendered documents against the source manifests value by value.
# This recipe WRITES the tracked file and is therefore deliberately not part of
# `lint` — the same arrangement `crds` has, and for the same reason: a gate that
# rewrites the thing it is checking cannot fail.
install-yaml:
    ./scripts/render-install.sh

# X-APPLY (spec §16 clause 1): apply the install file twice, reading both exit
# codes DIRECTLY.
#
# TWICE IS THE TEST, NOT A RETRY. The first apply creates the CRDs; the second
# is the one that would fail if the file contained a custom resource of a kind
# the same file is still establishing, or if server-side apply disagreed with
# itself about field ownership. A single green apply proves much less.
#
# `--server-side` IS NOT OPTIONAL: the six CRDs are large enough that a
# client-side apply stores a `last-applied-configuration` annotation over the
# 262144-byte metadata limit on some of them.
#
# WHAT THIS DOES AND DOES NOT PROVE. It proves `kubectl apply` exits 0. It does
# NOT start a pod: the images are referenced by tag, no such image has been
# pushed, and the Deployment's pod is expected to sit in `ImagePullBackOff`
# until `release.yml` has run on a pushed tag (Global Constraint 37 —
# `blocked: images not published`; no tag has been pushed, so that workflow has
# never run). For an author-only local run use the overlay:
#
#     kubectl --context docker-desktop apply --server-side -k config/overlays/local-images
#
# NO PIPE ANYWHERE (STANDING RULE 20): `cmd | grep` reports grep's status, and
# both of these exit codes are load-bearing.
# THE CONTEXT IS THE DRIVER'S. `scripts/demo-steps.sh` exports
# `LOGWEIR_KUBE_CONTEXT` (`kind-demo.sh` sets `kind-logweir`, `laptop-demo.sh`
# sets `docker-desktop`); run by hand, unset, it is the laptop's cluster. STANDING
# RULE 12 is satisfied by the context ALWAYS being passed. The literal here was
# what failed the first CI run of the demo (2026-09-12): step 3 on kind asked
# for a `docker-desktop` context the runner does not have.
apply-install:
    kubectl --context "${LOGWEIR_KUBE_CONTEXT:-docker-desktop}" apply --server-side -f logweir.yaml
    kubectl --context "${LOGWEIR_KUBE_CONTEXT:-docker-desktop}" apply --server-side -f logweir.yaml

# The pre-flight: refuse a namespace that is missing any of the five Secrets,
# naming the FIRST absent one.
#
# RUN IT BEFORE THE FIRST CUSTOM RESOURCE, AND NEVER AS PART OF THE INSTALL.
# There are FIVE Secrets, not three (spec §9, critique B H14), and until this
# task nothing in the repository told a stranger to create any of them. The one
# that matters most is `logweir-signing-key`, which is why it is checked first:
# `SigningKey::load_or_generate` MINTS A NEW KEY when the path is absent
# (`crates/logweir-evidence/src/keys.rs:81-92`), so a first run against an empty
# Secret produces evidence signed by a key nothing attests — silently, and with
# a green scorecard.
#
# FOUR OF THE FIVE LIVE IN THE RUNNER'S NAMESPACE; the fifth,
# `logweir-evidence-ro`, is the CONTROLLER's read-only evidence credential and
# lives in `logweir-system` (spec §9's table: the kubelet reads it, so "no `get`
# on Secrets" holds in the letter). The per-cluster SCRAM credential's NAME is
# the adopter's — it is whatever `KafkaCluster.spec.auth.secretRef` says — so it
# is the second argument, defaulting to `kafka-scram`; what is fixed is its data
# key, `password`.
#
#     just check-secrets logweir-t21
#     just check-secrets my-namespace my-scram-secret
#
# EVERY EXIT CODE IS READ DIRECTLY (STANDING RULE 20). `kubectl get` writes to
# a discarded stream and its status is read from `$?` on the next line; nothing
# is piped.
check-secrets ns scram="kafka-scram":
    #!/usr/bin/env bash
    # NOT `set -e`: a missing Secret is the ANSWER, not an accident, and the
    # loop has to reach the end to name the first one that is absent.
    set -uo pipefail
    missing=""
    for pair in "logweir-signing-key:{{ns}}" \
                "logweir-approval-bundle:{{ns}}" \
                "{{scram}}:{{ns}}" \
                "logweir-s3:{{ns}}" \
                "logweir-evidence-ro:logweir-system"; do
      name="${pair%%:*}"
      space="${pair##*:}"
      kubectl --context "${LOGWEIR_KUBE_CONTEXT:-docker-desktop}" -n "$space" get secret "$name" -o name >/dev/null 2>&1
      rc=$?
      if [ "$rc" -ne 0 ] && [ -z "$missing" ]; then
        missing="$name"
        missing_ns="$space"
      fi
    done
    if [ -n "$missing" ]; then
      echo "check-secrets: $missing is absent from namespace $missing_ns"
      echo ""
      echo "Create it before any custom resource — docs/kubernetes.md §13 step 1 carries the"
      echo "exact command, and the two openssl commands that mint the keypairs. An absent"
      echo "logweir-signing-key is the worst of the five: SigningKey::load_or_generate mints a"
      echo "new key when the path is absent (crates/logweir-evidence/src/keys.rs:81-92), so the"
      echo "run would succeed and sign its evidence with a key nothing attests."
      exit 1
    fi
    echo "check-secrets: all five Secrets are present ({{ns}}, and logweir-evidence-ro in logweir-system)."

# Task 23, chain J slot 16. The two image recipes for the CONTROLLER image and
# the org-root anchor — appended at the END of this file, as STANDING RULE 17
# requires of every editor of it.

# THE NAMED PRODUCER of the local `weirkeeper:check` tag — the sibling of
# `image` above, and deliberately not a flag on it. Two images, two Dockerfiles,
# two producers, two gates: `scripts/check-image.sh` asserts the runner image
# and `scripts/check-image-weirkeeper.sh` asserts this one, because all six of
# the former's checks are written around the runner's two binaries, its MIT
# notice and its x86-64 ELF (critique B H15).
#
# SINGLE-PLATFORM, AND LOADED INTO THE LOCAL DAEMON. `docker build
# --platform linux/amd64,linux/arm64` CANNOT `--load`: the local image store
# holds single-platform images only, so a multi-platform build must `--push` to
# a registry — and nothing in this tree pushes: publication happens in
# `release.yml` on a pushed tag, which has never run (Global Constraint 37,
# `blocked: images not published`). Either way
# `weirkeeper:check` would not exist as a local tag,
# `scripts/check-image-weirkeeper.sh weirkeeper:check` would have nothing to
# inspect, `just check-org-root` could not run, and `imagePullPolicy: Never`
# would find no image on the node. **Multi-arch is Task 30b's `release.yml` and
# nothing else**, and there is deliberately NO `image-weirkeeper-release`
# recipe here: Task 30b is not a member of chain J and may not edit this file,
# and a recipe whose body is `docker buildx build --push` cannot be executed on
# a laptop with no registry — which is a recipe that has never run (critique B
# H16).
#
# `${LOGWEIR_IMAGE_PLATFORM:-linux/arm64}` IS THE WHOLE POINT OF THE VARIABLE.
# The developer default is this host's own architecture, so the builder stage
# (`FROM --platform=$BUILDPLATFORM`) and the target agree and nothing is
# emulated. Task 31's GitHub-hosted amd64 runner sets
# `LOGWEIR_IMAGE_PLATFORM=linux/amd64` and gets the same native build, instead
# of compiling this workspace under QEMU — STANDING RULE 10's forbidden case,
# measured at 33x in docs/stability.md. Hard-coding either value breaks the
# other host, which is the mutant
# `the_weirkeeper_image_recipe_takes_its_platform_from_the_environment` kills.
#
# DELIBERATELY NOT PART OF `lint`, `test`, `default` OR `e2e`, exactly as
# `image` is not: an image build must never become a precondition of the
# Docker-free test run. The two `#[test]`s that assert the anchor read only
# checked-in files for the same reason; the image-inspecting half is
# `check-org-root` below, a recipe and not a test (critique B M14).
image-weirkeeper:
    docker build --platform "${LOGWEIR_IMAGE_PLATFORM:-linux/arm64}" --load -f Dockerfile.weirkeeper -t weirkeeper:check .

# Task 32, chain J slot 25. THE CONTROLLER IMAGE'S GATE — `just smoke`'s sibling,
# and the recipe that gives `scripts/check-image-weirkeeper.sh` (interface I25,
# Task 23) a caller. Task 23 shipped the script and no recipe, so the only thing
# invoking it was `release.yml`, which has never executed: the check existed and
# nothing ran it.
#
# DELIBERATELY NOT IN `just gate`, for the same reason `smoke` is not: it builds
# an image and needs a Docker daemon, and `just gate` must stay runnable on a
# machine that has neither. It is one of the seven recipes named in
# `docs/gates.md`'s stack/cluster table, and
# `crates/logweir/tests/gate_lint.rs::gate_lint_every_check_is_in_the_gate`
# asserts that every `scripts/check-*.sh` is reached by the gate OR by a recipe
# in that table, that the two sets are disjoint, and that together they are
# exhaustive — so this recipe is what keeps `check-image-weirkeeper.sh` inside
# the enumeration rather than orphaned.
#
#     just smoke-weirkeeper; echo "rc=$?"
smoke-weirkeeper: image-weirkeeper
    bash scripts/check-image-weirkeeper.sh weirkeeper:check

# THE IMAGE-INSPECTING HALF OF THE ORG-ROOT ASSERTION — stage-2 Task 16's T1,
# Global Constraint 7's byte-identity parity shape.
#
# A RECIPE AND NOT A `#[test]`, ON PURPOSE (critique B M14). A `#[test]` that
# shells `docker run` twice violates Global Constraint 22's 15 s per-test bound
# and STANDING RULE 7 ("a lint or unit gate never reaches the network... no
# image pull"), and would make `just lint` require both images to exist. The
# unit half is two tests over checked-in files —
# `crates/logweir/tests/manifest_lint.rs`'s
# `org_root_fingerprint_is_a_single_sha256_line` and
# `both_dockerfiles_copy_the_fingerprint` — and this is the half that needs a
# daemon.
#
# BOTH IMAGES, BECAUSE BOTH CARRY THE ANCHOR. The runner image is what a drill
# pod runs and the controller image is what the cluster owner is handed; the
# whole point of baking the fingerprint is that neither can be changed without
# producing a DIFFERENT image, so both are checked against the one checked-in
# file and against each other by construction.
#
#     just image && just image-weirkeeper
#     just check-org-root; echo "rc=$?"
#
# It takes both references as parameters so a pushed digest can be checked the
# same way the local tags are:
#
#     just check-org-root docker.io/vladyslavhaina/logweir@sha256:... docker.io/vladyslavhaina/weirkeeper@sha256:...
#
# EVERY EXIT CODE IS READ ON ITS OWN LINE (STANDING RULE 20). `docker run`'s
# status is read from `$?` on the next line and `diff`'s from `$?` on the line
# after that; nothing whose status is load-bearing is piped. `set -e` is NOT
# used, for the same reason `check-secrets` above does not use it: a mismatch
# is the ANSWER, and the loop has to reach the end so that a failure on the
# first image does not hide the state of the second.
check-org-root runner="logweir:check" controller="weirkeeper:check":
    #!/usr/bin/env bash
    set -uo pipefail
    cd "{{justfile_directory()}}"
    if [ ! -s third_party/org-root.fingerprint ]; then
      echo "check-org-root: third_party/org-root.fingerprint is missing or empty."
      echo "That file is the checked-in half of the anchor and this gate's expected value;"
      echo "without it the comparison below would compare the images against nothing."
      exit 1
    fi
    bad=0
    for ref in "{{runner}}" "{{controller}}"; do
      tmp="$(mktemp -t logweir-org-root.XXXXXX)"
      docker run --rm --entrypoint cat "$ref" /etc/logweir/org-root.fingerprint > "$tmp" 2>/dev/null
      rc=$?
      if [ "$rc" -ne 0 ]; then
        echo "check-org-root: could not read /etc/logweir/org-root.fingerprint from $ref (docker rc=$rc)"
        echo "  Build it first: \`just image\` for the runner, \`just image-weirkeeper\` for the"
        echo "  controller. Both Dockerfiles must carry"
        echo "  \`COPY third_party/org-root.fingerprint /etc/logweir/\`."
        bad=1
        rm -f "$tmp"
        continue
      fi
      diff -u third_party/org-root.fingerprint "$tmp"
      rc=$?
      if [ "$rc" -ne 0 ]; then
        echo "check-org-root: $ref's baked anchor is NOT the checked-in one (diff above:"
        echo "  - is third_party/org-root.fingerprint, + is the image). Rebuild the image;"
        echo "  never edit the file to match a stale image."
        bad=1
      else
        echo "check-org-root: $ref carries the checked-in org-root fingerprint, byte for byte."
      fi
      rm -f "$tmp"
    done
    if [ "$bad" -ne 0 ]; then
      exit 1
    fi
    echo "check-org-root: both images carry third_party/org-root.fingerprint at /etc/logweir/org-root.fingerprint."

# Task 24, chain J slot 17. **PHASE B'S EXIT CRITERION**, scripted.
#
# One command from an empty docker-desktop cluster to a `Restore` object
# carrying `phase: Succeeded`, `exitCode: 0`, `outcome: pass` and
# `status.evidence.verification.result: Valid` with a `matchedKeyId` — plus a
# `Backup` carrying `exitCode: 0` and the same `Valid`. Both verdicts are the
# CONTROLLER's, computed in-cluster with the read-only `logweir-evidence-ro`
# credential against the runner's public key on the cluster-scoped
# `TrustRoster` named `default`.
#
# IT IS AUTHOR-ONLY (Global Constraint 37) and says so in its own header. Two
# steps work only on the machine the images were built on: the `docker tag`
# that makes the SHIPPED `docker.io/vladyslavhaina/<name>@sha256:…` references resolve
# on this node (the kubelet keys on the WHOLE reference — plan erratum E19b),
# and `config/overlays/k8s-demo/deployment-env-patch.yaml`, which points the
# controller at this laptop's compose stack. "Published" still means a PULL
# from a registry the author does not control, and the install file's digest
# rows still read `blocked: images not published`.
#
# THE ORDER OF THE FIRST THREE STEPS IS NOT A STYLE CHOICE.
# `scripts/time-unit-suite.sh` refuses to run, exit 1, while 9092 or 9000
# answers (Global Constraint 22), so `just lint` runs at step 2, with the stack
# DOWN, and the stack comes up at step 3 — never the other way round.
# `crates/weirkeeper/tests/verification.rs::the_demo_runs_lint_before_the_stack_is_up`
# asserts it over the script text, in the default test suite.
#
# THE OBJECT STORE IS ADDRESSED AS `http://host.docker.internal:9000`
# (critique B H20): the compose stack's MinIO already publishes `9000:9000`, so
# no compose change is needed and STANDING RULE 15's one-owner rule is not
# touched. The archive is `s3://kafka-backups/k8s-demo` and the bucket is
# created with `mc` inside the compose network first, because `just e2e-down`
# runs `down -v` and EMPTIES the MinIO volume (plan erratum E12g).
#
# `cargo build -p logweir` FIRST, and it is not optional (plan erratum E9): the
# script shells `logweir drill approve` from `target/debug/logweir`, and
# nothing else in this recipe builds it.
#
#     just image && just image-weirkeeper     # once, if the images are absent
#     just k8s-demo; echo "rc=$?"
#
# It cleans up after itself from a `trap`: `kubectl delete -f logweir.yaml`,
# `kubectl delete ns logweir-system logweir-t24`, the two author-only image
# tags, the archive prefix, and `just e2e-down` — each with its rc printed.
k8s-demo:
    cargo build -p logweir
    ./scripts/k8s-demo.sh

# Task 25 (slot 18). SERVE THE UI.
#
# `kubectl proxy` serves the static files AND proxies the Kubernetes API on the
# SAME ORIGIN, attaching the viewer's own kubeconfig credential to every
# request it forwards, server side. That is the whole serving story: tag 1
# ships no server-side UI component, no image, no sidecar and no HTTP surface
# of its own, so this is the one zero-config path that gives the page an
# authenticated, same-origin API to talk to.
#
# `kubectl port-forward` cannot do it: it forwards a port to a pod, gives the
# browser no credential, and leaves every API call a cross-origin request to a
# server that sends no CORS headers unless it was started with
# `--cors-allowed-origins`, which no adopter has set.
#
# THE CONTEXT IS NAMED, AND THAT IS NOT COSMETIC (STANDING RULE 12). Without
# `--context docker-desktop` this recipe proxies whatever context happens to be
# current, with whatever credential that context carries -- on the one command
# in this product that hands a browser a cluster credential.
#
# THE TWO FLAGS THAT MUST NEVER CHANGE are `--address=127.0.0.1` and
# `--disable-filter` (never pass it; the default `false` keeps the
# `--accept-hosts` filter on). Changing either turns a local page holding your
# cluster authority into a network service holding it.
ui:
    @echo "Serving the Logweir UI at http://127.0.0.1:8001/ui/"
    @echo ""
    @echo "WHAT THIS COSTS, said plainly: kubectl proxy forwards every API path"
    @echo "except pod exec and attach, on the same origin as the page, under your"
    @echo "kubeconfig. The page therefore runs with YOUR ENTIRE CLUSTER AUTHORITY,"
    @echo "not with the four ClusterRoles logweir.yaml ships -- those bind the user,"
    @echo "and under this serving path they bind nothing about the page. Run this"
    @echo "from a cluster-admin kubeconfig and you have given the page cluster-admin."
    @echo "No bearer token, key or credential of any kind is ever placed in the page."
    @echo ""
    @echo "Two flags must never change: --address=127.0.0.1 and --disable-filter"
    @echo "(never pass it). Changing either turns a local page holding your cluster"
    @echo "authority into a network service holding it."
    @echo ""
    @echo "To narrow it, run the proxy under a kubeconfig bound to logweir-viewer"
    @echo "and logweir-operator and nothing else -- ui/README.md has the four"
    @echo "kubectl config commands."
    @echo ""
    kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1

# Task 28 (slot 21). PHASE C's EXIT CRITERION, and install gate X-UIWRITE.
#
# The walk spec §1 calls "the whole acceptance surface": a stranger clones,
# applies ONE file to docker-desktop Kubernetes and gets CRDs, RBAC and the
# controller; the UI is static files served by `kubectl proxy --www=`; against
# a plain broker from the compose stack they create a `BackupSchedule`, watch a
# `Backup` produce a signed receipt, run a drill (a `Restore` with a `newTopic`
# target) and read a signed scorecard with measured RTO and RPO -- NEVER TYPING
# A PRIVATE KEY INTO A BROWSER. Twelve numbered steps; the transcript of the
# run that proved it is `e2e/k8s/laptop-demo.md`.
#
# THE COMPOSE STACK IS A PRECONDITION, not a step: spec §2's Demo 1 opens with
# `docker compose … up`, so bring it up first and let the demo's teardown take
# it down (STANDING RULE 3).
#
#     just e2e-up
#     LOGWEIR_DEMO_NONINTERACTIVE=1 just laptop-demo; echo "rc=$?"
#
# X-UIWRITE HAS TWO HALVES AND THE SECOND ONE IS THE GATE. Spec §10 asks for a
# `create` of a `Restore` FROM THE PAGE returning 201, and `curl` is not the
# page. Step 10(a) is the scripted half; step 10(b) is performed by hand from
# the wizard's final step, and the created object's `metadata.managedFields`
# names `logweir-ui` (`ui/api.js`'s `?fieldManager=logweir-ui`, interface
# register I23) -- which is the evidence, and which neither `kubectl` nor a
# browser User-Agent can produce. With the proxy still up from the first pass:
#
#     LOGWEIR_DEMO_ONLY_STEP=10b ./scripts/laptop-demo.sh
#
# `cargo build -p logweir` FIRST, and it is not optional (plan erratum E9): the
# script shells `logweir drill approve` and `logweir drill verify` from
# `target/debug/logweir`, and nothing else in this recipe builds it.
#
# It cleans up after itself from a `trap`: `kubectl delete -f logweir.yaml`,
# `kubectl delete ns logweir-system logweir-t28`, the two author-only image
# tags, the archive prefix, the proxy, the keypairs it minted, and `just
# e2e-down` -- each with its rc printed.
laptop-demo:
    cargo build -p logweir
    ./scripts/laptop-demo.sh

# Task 35: THE CHART, WALKED END TO END ON A REAL CLUSTER. The release must be
# installed first with all three optional components on (the README's
# five-minute path); the walk then mints the keypairs, creates the five
# Secrets, probes two KafkaClusters, fires a BackupSchedule, restores from its
# Backup with an approval minted on the host, verifies the scorecard with both
# readers, fetches the in-cluster UI through a port-forward and tears
# everything down. Parameterised by LOGWEIR_KUBE_CONTEXT (default
# docker-desktop), LOGWEIR_HELM_RELEASE (logweir) and LOGWEIR_HELM_NAMESPACE
# (logweir-system). `.github/workflows/helm-demo.yml` runs the same script on
# a kind cluster. Outside `just gate` — it needs a cluster — and in
# docs/gates.md's stack/cluster table.
helm-demo:
    cargo build -p logweir
    bash scripts/helm-demo.sh

# ===========================================================================
# Task 32, chain J slot 25. THE GATE — the one local command that runs every
# check this repository has, in an order chosen so the total is what it is.
#
# WHY IT EXISTS. Every check in this tree must be reachable by one local
# command or it is not enforced. "Wired into ci.yml" is still not the gate here,
# even now that `ci.yml` HAS executed (run 34700987730, commit a113dd2,
# 2026-09-12, green): a workflow reports after a push, this recipe reports
# before one, and `docs/tag1-checklist.md` clause 4 still reads
# `blocked: no tag pushed` because `release.yml` has never run. This recipe is
# the enforcement point and `ci.yml` is its mirror.
#
# THE ORDERING IS THE BUDGET. `./scripts/time-unit-suite.sh` sits IMMEDIATELY
# after `cargo test --workspace` and BEFORE the `--release` line, because its
# SUITE_CMD is that same `cargo test --workspace`, in the same profile and the
# same target directory: at that position it is a second RUN of artefacts the
# previous line already built, not a third build. So this recipe contains
# exactly TWO workspace compilations — one debug (shared by clippy's target
# set, the suite and the timing harness) and one release. Move the timing line
# above `cargo test --workspace` and the recipe still exits 0 while the timing
# line silently absorbs a whole debug compile; the per-line seconds in
# docs/gates.md are what catch that, and that file says so rather than implying
# a guard exists.
#
# MEASURED — 2026-09-11, quiet 10-core Apple-silicon host, CARGO_BUILD_JOBS=4,
# compose stack down, warm debug target directory and a COLD release profile:
# **420.1 s (7 min 0 s)**, of which 315.7 s is line 5, the release compile.
# Re-run immediately with everything cached: **118.9 s**. `docs/gates.md`
# carries the per-line seconds and what each line does and does not prove.
#
# THE STANDING THRESHOLD IS 2x THE RECORDED TOTAL — **840 s**. Over that, stop
# and report; `cargo clean` is NOT the remedy (STANDING RULE 6's triggers are
# 50,000 files in target/debug/deps or a single workspace RUN over five minutes,
# and a slow gate is neither).
#
# NO STATUS THROUGH A PIPE (STANDING RULE 20, Global Constraint 11). `just`
# runs each line in its own shell and aborts on the first non-zero status, so
# every line's exit code is read by `just` itself. The one `&&` line is the
# third-party-notices diff, where both halves' statuses are load-bearing and
# neither is piped.
#
# WHAT IT DELIBERATELY DOES NOT RUN: `just e2e`, `just smoke`,
# `just smoke-weirkeeper`, `just mvp-demo`, `just k8s-demo`, `just laptop-demo`
# and `just pitr`. Each needs the compose stack, a Kubernetes cluster or a
# Docker build — and `./scripts/time-unit-suite.sh` REFUSES to run, exit 1,
# while 9092 or 9000 answers, so a gate that brought the stack up would fail
# itself. Those seven are in `docs/gates.md`'s stack/cluster table with what
# each proves and which `check-*.sh` each invokes, and
# `crates/logweir/tests/gate_lint.rs` asserts the two sets are disjoint and
# together exhaustive over every `scripts/check-*.sh` in the tree.
#
# THE COMPOSE STACK MUST BE DOWN: `just e2e-down` first, or the timing line
# refuses.
gate:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    ./scripts/time-unit-suite.sh
    cargo test --workspace --release
    cargo deny --offline check licenses
    ./scripts/check-no-oso.sh
    ./scripts/check-pure-core.sh
    ./scripts/check-one-signer.sh
    ./scripts/check-withdrawn-claim.sh
    ./scripts/check-no-archive-write.sh
    ./scripts/check-ui-offline.sh
    ./scripts/check-ui-behaviour.sh
    ./scripts/check-unverified-labels.sh
    ./scripts/check-verifier-parity.sh
    ./scripts/check-invariant-corpus.sh
    ./scripts/check-deps-count.sh
    ./scripts/check-dod.sh
    ./scripts/check-links.sh docs/ README.md SECURITY.md MAINTAINERS.md CONTRIBUTING.md TRADEMARKS.md THIRD_PARTY_NOTICES.md third_party/ e2e/fixtures/ ui/ charts/
    ./scripts/render-install.sh --check
    just schema-check
    just crds-check
    just chart-check
    just receipt-schema-check
    bash scripts/gen-third-party-notices.sh > target/tpn.check && diff -u THIRD_PARTY_NOTICES.md target/tpn.check
    just verify-py

# Task 39, appended at the END of this file as STANDING RULE 17 requires of
# every editor of it. THE UI IMAGE — the third image this project publishes,
# and the one that replaced the chart's ConfigMap copy of the page.

# THE NAMED PRODUCER of the local `logweir-ui:check` tag — the sibling of
# `image` and `image-weirkeeper` above. Three images, three Dockerfiles, three
# producers, three gates: `scripts/check-image.sh` asserts the runner,
# `scripts/check-image-weirkeeper.sh` the controller and
# `scripts/check-image-ui.sh` this one, because each of the other two is
# written around binaries this image does not carry.
#
# SECONDS, NOT MINUTES, AND THAT IS WHY THIS RECIPE IS DIFFERENT FROM ITS TWO
# SIBLINGS. `Dockerfile.ui` compiles nothing: it is two `COPY`s of ~90 KB over
# a kubectl the Kubernetes project publishes. There is no builder stage, no
# `aws-lc-sys`, no cross-compile refusal — so unlike `image-weirkeeper` this
# recipe has no architecture it cannot build, and unlike `image` it is not
# pinned to linux/amd64 (no engine ELF is involved).
#
# `${LOGWEIR_IMAGE_PLATFORM:-linux/arm64}` IS THE SAME VARIABLE `image-weirkeeper`
# READS, and for the same reason: the developer default is this host's own
# architecture, and a CI runner sets its own. The base
# (`registry.k8s.io/kubectl@sha256:59bafa07…`) is a MANIFEST LIST carrying
# linux/amd64 and linux/arm64, so either value resolves to that architecture's
# own variant — measured 2026-09-14.
#
# SINGLE-PLATFORM AND LOADED, exactly as its two siblings: the local image
# store holds single-platform images only, so a multi-platform build could not
# `--load` and would have to `--push`. Nothing in this tree pushes (Global
# Constraint 17); multi-arch is `release.yml`'s image job and nothing else, and
# there is deliberately no `image-ui-release` recipe here.
#
# DELIBERATELY NOT PART OF `lint`, `test`, `default`, `e2e` OR `gate`: it needs
# a Docker daemon, and `just gate` must stay runnable on a machine that has
# none. It is a row in `docs/gates.md`'s stack/cluster table instead.
image-ui:
    docker build --platform "${LOGWEIR_IMAGE_PLATFORM:-linux/arm64}" --load -f Dockerfile.ui -t logweir-ui:check .

# THE UI IMAGE'S GATE — `just smoke`'s and `just smoke-weirkeeper`'s sibling,
# and the recipe that gives `scripts/check-image-ui.sh` a caller.
#
# IT IS WHAT REPLACED `scripts/check-chart.sh`'s byte-copy arm. That arm
# compared two directories in this repository; this one computes the sha256 of
# every file the image will serve and of every file under `ui/`, and compares
# them — a statement about the bytes a browser receives.
#
# DELIBERATELY NOT IN `just gate`, for the same reason `smoke` and
# `smoke-weirkeeper` are not: it builds an image and needs a Docker daemon.
# `crates/logweir/tests/gate_lint.rs::gate_lint_every_check_is_in_the_gate`
# asserts that every `scripts/check-*.sh` is reached by `just gate` OR by a
# recipe named in `docs/gates.md`'s stack/cluster table, that the two sets are
# disjoint and that together they are exhaustive — so this recipe and that
# table row are what keep `check-image-ui.sh` inside the enumeration rather
# than orphaned.
#
#     just smoke-ui; echo "rc=$?"
smoke-ui: image-ui
    bash scripts/check-image-ui.sh logweir-ui:check
