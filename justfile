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
