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
# on a laptop as well as in CI (ci.yml has never executed on any commit).
#
# `crds` and `schema` stay independent: two formats, two gates, no dependency
# edge between them.
crds:
    cargo run -p weirkeeper --example emit_crds -- --out config/crd

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
# until `release.yml` has run against a git remote (Global Constraint 37 —
# `blocked: no remote`). For an author-only local run use the overlay:
#
#     kubectl --context docker-desktop apply --server-side -k config/overlays/local-images
#
# NO PIPE ANYWHERE (STANDING RULE 20): `cmd | grep` reports grep's status, and
# both of these exit codes are load-bearing.
apply-install:
    kubectl --context docker-desktop apply --server-side -f logweir.yaml
    kubectl --context docker-desktop apply --server-side -f logweir.yaml

# The pre-flight: refuse a namespace that is missing any of the five Secrets,
# naming the FIRST absent one.
#
# RUN IT BEFORE THE FIRST CUSTOM RESOURCE, AND NEVER AS PART OF THE INSTALL.
# There are FIVE Secrets, not three (spec §9, critique B H14), and until this
# task nothing in the repository told a stranger to create any of them. The one
# that matters most is `logweir-signing-key`, which is why it is checked first:
# `SigningKey::load_or_generate` MINTS A NEW KEY when the path is absent
# (`crates/logweir-evidence/src/keys.rs:77-88`), so a first run against an empty
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
      kubectl --context docker-desktop -n "$space" get secret "$name" -o name >/dev/null 2>&1
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
      echo "new key when the path is absent (crates/logweir-evidence/src/keys.rs:77-88), so the"
      echo "run would succeed and sign its evidence with a key nothing attests."
      exit 1
    fi
    echo "check-secrets: all five Secrets are present ({{ns}}, and logweir-evidence-ro in logweir-system)."
