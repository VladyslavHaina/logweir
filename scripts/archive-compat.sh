#!/usr/bin/env bash
# PROD-00.3o: archives that operator-shaped setups wrote must still restore
# and verify. Run weekly by .github/workflows/compat-weekly.yml, and on any
# compose slot by hand (it addresses the stack the environment names):
#
#   scripts/archive-compat.sh fixtures   each published version's archive fixtures
#   scripts/archive-compat.sh engines    an archive each published version's engine writes
#
# The PUBLISHED VERSIONS are the `v*` tags (a clone with its tags).
#
# fixtures: a tag's archive fixtures are its e2e/fixtures/segments/ and
#   e2e/fixtures/manifests/, bytes a real engine wrote (FX-1). Each must still
#   be in the tree byte for byte, under any name, where the default test suite
#   decodes it on every pull request (crates/logweir-engine-oso/tests/kbak.rs,
#   kbak_scan.rs, vendored_parse.rs; crates/logweir/tests/engine_matrix.rs). A
#   refresh that replaced one would otherwise leave that release's bytes
#   unread. Exit 1, naming each one that is not.
#
# engines: a tag's engine is third_party/kafka-backup-binary.digest at that
#   tag, what an operator running that release wrote archives with. For each
#   such digest other than the current pin, on a FRESH stack (`just e2e-down`,
#   `just e2e-up`): seed `drill-demo` with it (scripts/e2e-seed.sh through the
#   compose `kafka-backup` service), put the pin back, and drill that archive
#   with the engine this checkout runs (harness::engine_bin(); CI builds the
#   shipped engine into .engine/): a signed `pass` both readers verify, and
#   the corrupted-segment control, a signed `preflight-failed` with exit 2
#   (decisions/PROD-00-engine-route.md §7, A-OSO-1 and A-OSO-4).
#
# Nothing needs a secret. Every subprocess that can hang is bounded by the
# tool it runs (the seed, `docker`, cargo's own test harness).
set -euo pipefail
cd "$(dirname "$0")/.."

die() { echo "archive-compat: $*" >&2; exit 1; }

tags=$(git tag -l 'v*')
[ -n "$tags" ] || die "no published version (no v* tag) in this clone: fetch the tags first"

fixtures() {
  local t f have bad=0 n=0
  # The tree's fixtures as git blob ids, under any name: a refresh may keep
  # a release's bytes beside the new ones under another name.
  have=$(find e2e/fixtures/segments e2e/fixtures/manifests -type f -exec git hash-object -- {} +)
  for t in $tags; do
    for f in $(git ls-tree -r --name-only "$t" -- e2e/fixtures/segments e2e/fixtures/manifests); do
      n=$((n + 1))
      if ! grep -qxF "$(git rev-parse "$t:$f")" <<<"$have"; then
        echo "FAIL  $t's $f is in the tree under no name: keep the published bytes (under another name, read by the default suite) beside any refresh"
        bad=1
      fi
    done
  done
  [ "$n" -gt 0 ] || die "no published version carries an archive fixture"
  [ "$bad" -eq 0 ] || exit 1
  echo "ok: $n archive fixture(s) of $(printf '%s\n' "$tags" | wc -l | tr -d ' ') published version(s) are in the tree byte for byte"
}

engines() {
  local pin digests d version tmp
  # shellcheck source=e2e/compose/stack-lib.sh
  . e2e/compose/stack-lib.sh
  lw_e2e_check_coherent
  pin=$(tr -d '[:space:]' < third_party/kafka-backup-binary.digest)
  digests=$(for t in $tags; do git show "$t:third_party/kafka-backup-binary.digest" 2>/dev/null | tr -d '[:space:]'; echo; done \
    | sed '/^$/d' | sort -u | grep -vxF "$pin" || true)
  if [ -z "$digests" ]; then
    echo "ok: every published version pins the current engine ($pin); no other archive to seed"
    return 0
  fi
  tmp=$(mktemp -d)
  cp e2e/compose/.env "$tmp/env"
  cp third_party/kafka-backup-binary.digest "$tmp/digest"
  # The pin goes back on every exit path, so a failed seed never leaves the
  # checkout pointing at another engine.
  # shellcheck disable=SC2064
  trap "cp '$tmp/env' e2e/compose/.env; cp '$tmp/digest' third_party/kafka-backup-binary.digest; rm -rf '$tmp'" EXIT
  cargo build --locked -p logweir
  for d in $digests; do
    version=$(docker run --rm --platform linux/amd64 --entrypoint kafka-backup "osodevops/kafka-backup@$d" --version)
    echo "== an archive written by $version ($d), drilled with this checkout's engine"
    just e2e-down
    just e2e-up
    sed "s|^OSO_DIGEST=.*|OSO_DIGEST=$d|" "$tmp/env" > e2e/compose/.env
    echo "$d" > third_party/kafka-backup-binary.digest
    LOGWEIR_SEED_REFRESH_FIXTURES=0 ./scripts/e2e-seed.sh
    cp "$tmp/env" e2e/compose/.env
    cp "$tmp/digest" third_party/kafka-backup-binary.digest
    CARGO_TEST_ARGS="--locked -p e2e --features e2e --test full_drill" \
      ./scripts/run-named-tests.sh \
      a_full_drill_produces_a_signed_scorecard_with_real_numbers \
      a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard
    echo "ok: the archive $version wrote drills pass and verifies, and its corrupted twin is refused"
  done
}

case "${1:-}" in
  fixtures) fixtures ;;
  engines) engines ;;
  *) die "usage: $0 fixtures|engines" ;;
esac
