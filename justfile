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
# every shipped surface. `ci.yml` has never executed on any commit, so
# membership in THIS recipe is what makes it enforced rather than asserted;
# `crates/logweir/tests/withdrawn_claim.rs::the_withdrawn_claim_gate_is_in_just_lint`
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

schema:
    cargo run -p logweir-core --example emit_schema > schemas/logweir-drill-scorecard-1.0.0.json

# The Python auditor verifier. Task 7 — needs `pip install cryptography pytest`.
verify-py:
    python3 -m pytest docs/test_verify_scorecard.py -q

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
fixtures-sign:
    mkdir -p target/fixtures-tmp
    cargo run -p logweir-core --example emit_fixture > target/fixtures-tmp/scorecard.json && mv target/fixtures-tmp/scorecard.json e2e/fixtures/signed/scorecard.json
    cargo run -p logweir-evidence --example mint_fixture

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
e2e-up:
    docker compose -f e2e/compose/docker-compose.yml up -d --wait
    docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm minio-setup
    docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm topic-setup

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
e2e:
    AWS_EC2_METADATA_DISABLED=true cargo test --workspace --features e2e -- --test-threads=1 --nocapture

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
    ./scripts/check-links.sh docs/ README.md SECURITY.md MAINTAINERS.md CONTRIBUTING.md TRADEMARKS.md third_party/ e2e/fixtures/

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
# workflow that has never executed on any commit including the tag. One
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
