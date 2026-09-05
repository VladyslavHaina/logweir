default: fmt lint test

fmt:
    cargo fmt --all

lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    ./scripts/check-no-oso.sh

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
e2e-up:
    docker compose -f e2e/compose/docker-compose.yml up -d
    docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm topic-setup

e2e-down:
    docker compose -f e2e/compose/docker-compose.yml down -v

e2e:
    cargo test --workspace --features e2e -- --test-threads=1 --nocapture

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
