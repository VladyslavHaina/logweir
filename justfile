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
