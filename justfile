# The default recipe is the release gate (Task 22 step 8), so it must be able to
# FAIL on formatting: it never runs the rewriting `fmt` recipe. Run `just fmt`
# yourself to fix formatting; `lint` checks it, and also runs check-no-oso.sh
# and check-pure-core.sh.
default: lint test

fmt:
    cargo fmt --all

lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    ./scripts/check-no-oso.sh
    ./scripts/check-pure-core.sh

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
fixtures-sign:
    cargo run -p logweir-core --example emit_fixture > e2e/fixtures/signed/scorecard.json
    cargo run -p logweir-evidence --example mint_fixture

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

e2e:
    cargo test --workspace --features e2e -- --test-threads=1 --nocapture

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
