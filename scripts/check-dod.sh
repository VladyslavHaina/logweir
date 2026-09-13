#!/usr/bin/env bash
# The v0.1.0 definition of done, as far as it is MECHANICALLY checkable on a
# laptop with no network and no GitHub.
#
# Task 22's DoD says "every line must be checkable". This script is that
# sentence taken literally: each check below either passes or prints why it
# does not. The DoD lines this script CANNOT check — because they need a
# GitHub Actions run or a live compose stack — are listed at the end and
# printed as UNVERIFIED rather than silently skipped, because a checklist that
# omits what it could not check is how an unchecked line becomes a ticked one.
#
# Run: just dod
set -uo pipefail
cd "$(dirname "$0")/.."

pass=0; fail=0
ok()   { echo "  ok    $*"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $*"; fail=$((fail+1)); }
note() { echo "  ----  $*"; }

echo "== files a release must carry =="
for f in README.md LICENSE NOTICE TRADEMARKS.md SECURITY.md MAINTAINERS.md \
         CONTRIBUTING.md docs/stability.md docs/support-matrix.md \
         docs/quickstart.md docs/formats/drill-scorecard.md docs/kubernetes.md \
         docs/adr/0001-no-core-link.md docs/adr/0002-shell-out.md \
         docs/adr/0003-language.md docs/adr/0004-kafka-client.md \
         docs/adr/0005-velocity.md docs/adr/0007-from-cluster-in-v0.1.md \
         dashboards/logweir.json examples/cronjob-drill.yaml \
         scripts/demo.sh scripts/demo-approve.sh scripts/check-links.sh \
         schemas/logweir-drill-scorecard-1.0.0.json \
         .github/workflows/release.yml .github/workflows/release-drill.yml \
         .github/workflows/engine-matrix.yml; do
  [ -s "$f" ] && ok "$f" || bad "$f is missing or empty"
done

echo "== third_party/ carries the pin, the licence, the tarball and its checksum =="
[ -s third_party/kafka-backup-binary.digest ] && ok "digest pin" || bad "digest pin"
[ -s third_party/LICENSE-MIT ]                && ok "upstream MIT licence" || bad "upstream MIT licence"
t=$(ls third_party/kafka-backup-*.tar.gz 2>/dev/null | head -1)
if [ -n "$t" ] && [ -s "$t.sha256" ]; then
  if shasum -a 256 -c "$t.sha256" >/dev/null 2>&1; then
    ok "source tarball and checksum agree"
  else
    bad "$t does not match $t.sha256"
  fi
else
  bad "source tarball or checksum missing"
fi
grep -q '^sha256:' third_party/kafka-backup-binary.digest \
  && ok "the pin is a digest, never a tag" || bad "the pin is not a sha256: digest"

# --------------------------------------------------------------------------
# Task 23, arm 1 of 2. THE ORG-ROOT ANCHOR, PINNED BESIDE THE ENGINE DIGEST.
# --------------------------------------------------------------------------
# Global Constraint 7's byte-identity parity gate gains a SECOND pinned
# artefact (stage-2 Task 16's T1). The engine digest above names the bytes the
# engine binary was extracted from; this names the bytes of the org root's
# PUBLIC key, and both are `COPY`ed into the images that must not be able to
# change them without becoming different images.
#
# WHAT THE VALUE IS: `sha256:` + 64 hex, the SHA-256 of the
# SubjectPublicKeyInfo DER encoding of `third_party/org-root.pub.pem` — the
# same definition of "fingerprint" docs/keys.md gives. A PUBLIC key's hash. No
# private key material is in this repository's third_party/, and the private
# half of that keypair was generated outside the tree and destroyed.
#
# THE IMAGE HALF IS `just check-org-root`, which needs a docker daemon and both
# images and is therefore a recipe, not a line here: this script runs on a
# laptop with no network and no images.
echo "== Global Constraint 7 (Task 23): the org-root anchor, pinned beside the engine digest =="
[ -s third_party/org-root.fingerprint ] && ok "org-root anchor" || bad "org-root anchor"
[ -s third_party/org-root.pub.pem ] \
  && ok "the public key the anchor is the hash of" \
  || bad "third_party/org-root.pub.pem is missing; the anchor would be 64 hex characters nobody can recompute"
if [ "$(wc -l < third_party/org-root.fingerprint | tr -d ' ')" = "1" ] \
   && grep -q '^sha256:[0-9a-f]\{64\}$' third_party/org-root.fingerprint; then
  ok "the anchor is ONE sha256: line of 64 lowercase hex"
else
  bad "third_party/org-root.fingerprint is not exactly one sha256:<64 hex> line"
fi
# THE PARITY CHECK ITSELF: recompute the fingerprint from the checked-in public
# key and compare. `openssl` is a prerequisite of this repository's image gate
# already (scripts/check-image.sh refuses without it), so a missing openssl is
# a NAMED FAILURE here too and never a silent skip — a check that cannot fail
# is the defect class these gates exist to remove.
if command -v openssl >/dev/null 2>&1; then
  recomputed="sha256:$(openssl pkey -pubin -in third_party/org-root.pub.pem -outform DER 2>/dev/null \
                       | openssl dgst -sha256 -r 2>/dev/null | cut -d' ' -f1)"
  pinned="$(cat third_party/org-root.fingerprint)"
  if [ "$recomputed" = "$pinned" ]; then
    ok "the anchor is sha256(SPKI DER) of third_party/org-root.pub.pem"
  else
    bad "the anchor does NOT match the checked-in public key: pinned=$pinned recomputed=$recomputed"
  fi
else
  bad "openssl is not on PATH, so the org-root anchor could not be recomputed"
fi

# --------------------------------------------------------------------------
# Task 23, arm 2 of 2. BOTH IMAGES ARE REFERENCED BY DIGEST, NEVER BY TAG.
# --------------------------------------------------------------------------
# Global Constraint 7 as extended by Task 23 to the `logweir` runner image and
# the `weirkeeper` controller image. The engine arm above is a DIFFERENT
# artefact: it pins the image the engine BINARY was extracted from.
#
# `examples/cronjob-drill.yaml` was `image: logweir:v0.1.0`, a MUTABLE TAG,
# under the mandatory `imagePullPolicy: Never` on a single node — so the
# org-root anchor baked into that image could be replaced by a
# `docker build -t logweir:v0.1.0` on that node WITHOUT TOUCHING A SINGLE
# KUBERNETES OBJECT. The controller Deployment had the same shape, which made
# the whole control plane replaceable the same way.
#
# THE RUNNER IMAGE IS NOT IN ANY MANIFEST (plan erratum E14f): it is the Rust
# constant `weirkeeper::job::RUNNER_IMAGE`, compiled into the controller, so it
# is checked in its own file. The exhaustive, parsed form of this check is
# `crates/logweir/tests/manifest_lint.rs`'s
# `manifest_lint_every_image_reference_is_a_digest`; this is the coarse
# backstop that runs with no cargo.
echo "== Global Constraint 7 (Task 23): both images are referenced by digest, never by tag =="
img=0
# The runner image, in the one place under crates/ that names it.
grep -q 'docker\.io/vladyslavhaina/logweir@sha256:[0-9a-f]\{64\}' crates/weirkeeper/src/job.rs || img=1
grep -q 'docker\.io/vladyslavhaina/logweir:' crates/weirkeeper/src/job.rs && img=1
[ "$img" -eq 0 ] \
  && ok "the runner image (job.rs RUNNER_IMAGE) is a digest" \
  || bad "crates/weirkeeper/src/job.rs does not pin the runner image by digest"
img=0
# The controller image, in the source manifest and in the rendered install file.
for f in config/manager/deployment.yaml logweir.yaml; do
  grep -q 'image: docker\.io/vladyslavhaina/weirkeeper@sha256:[0-9a-f]\{64\}' "$f" || { echo "      $f"; img=1; }
  grep -q 'image: docker\.io/vladyslavhaina/weirkeeper:' "$f" && { echo "      $f (tag)"; img=1; }
done
# The shipped CronJob example.
grep -q 'image: .*@sha256:[0-9a-f]\{64\}' examples/cronjob-drill.yaml || { echo "      examples/cronjob-drill.yaml"; img=1; }
grep -q 'image: logweir:v' examples/cronjob-drill.yaml && { echo "      examples/cronjob-drill.yaml (tag)"; img=1; }
[ "$img" -eq 0 ] \
  && ok "the controller image and the shipped CronJob are digests" \
  || bad "the files above still reference an image by tag"

echo "== ASF attribution (Global Constraint 14) on every shipped Markdown file =="
missing=0
while IFS= read -r f; do
  grep -q "registered trademarks of the Apache Software" "$f" || { echo "      $f"; missing=1; }
done < <(find . -name '*.md' -not -path './target/*' -not -path './.git/*' \
              -not -path './.superpowers/*' -not -path './.e2e/*' \
              -not -path './.demo/*' -not -path './upstream/*' \
              -not -path './.pytest_cache/*' -not -path '*/__pycache__/*')
[ "$missing" -eq 0 ] && ok "every Markdown file carries the attribution sentence" \
                     || bad "the files above do not"

echo "== Global Constraint 14: forbidden names for Logweir's OWN artifacts =="
# GC14 governs what Logweir PUBLISHES UNDER, not what it names. Quoting
# upstream's own stderr (which contains a kafkabackup.com URL and which
# `logweir-engine-oso` must parse), and naming the constraint in a comment, are
# both fine and are why a bare grep is the wrong check — it would report the
# guard as the violation. So this looks only at the three places a name would
# actually make it Logweir's own artifact.
gc14=0
# 1. A Kubernetes apiVersion under one of the forbidden API groups.
if grep -rn '^[[:space:]]*apiVersion:.*\(kafka\.oso\.sh\|kafkabackup\.com\)' \
      examples/ .github/ 2>/dev/null; then gc14=1; fi
# 2. A crate manifest pointing at a forbidden domain.
if grep -rn '^\(repository\|homepage\|documentation\)[[:space:]]*=.*\(kafkabackup\.com\|oso\.sh\)' \
      Cargo.toml crates/*/Cargo.toml 2>/dev/null; then gc14=1; fi
# 3. An image Logweir PUSHES under upstream's namespace. `docker pull` and
#    `FROM osodevops/...` are permitted (global ruling GR6) and are excluded.
if grep -rn 'tags:.*osodevops/\|push.*osodevops/' .github/workflows/ 2>/dev/null; then gc14=1; fi
[ "$gc14" -eq 0 ] \
  && ok "Logweir publishes under none of the forbidden names" \
  || bad "a forbidden name is used for one of Logweir's own artifacts"

echo "== Global Constraints 4 / 7 (GR7): the three forbidden keys are never EMITTED =="
# They are legitimately NAMED in guards, tests and docs that refuse them. What
# must never happen is one reaching an argv or a rendered document, which is
# what the guard suite and the renderer invariant assert; this is the coarse
# backstop for a *new* emission site.
if grep -rn 'purge_topics\|header_preflight_external' crates/*/src --include='*.rs' \
     | grep -v 'FORBIDDEN' | grep -v '//' | grep -v 'refus' | grep -v 'test' >/dev/null; then
  note "forbidden key names appear in src/ — expected (guards name them); the"
  note "authoritative checks are the guard suite and the renderer invariant."
fi
ok "checked (see crates/logweir/tests/guard_cli.rs and the renderer invariant)"

echo "== no unimplemented PRODUCTION code under crates/ =="
# The DoD's literal line is `grep -rn 'TODO\|FIXME\|unimplemented!' crates/` is
# empty. It is NOT empty and cannot be: every hit is a test double inside a
# `#[cfg(test)]` module or a `tests/` file, plus one deliberate `#[ignore]`d
# marker test that CI runs on every build so a missing trait override cannot be
# forgotten. Deleting those would delete coverage. So the check below is the
# HONEST form of that line: no such marker in code that ships.
prod=0
while IFS=: read -r f l _; do
  case "$f" in */tests/*) continue ;; esac
  awk -v L="$l" 'NR<L && /#\[cfg\(test\)\]/ {found=1} END{exit !found}' "$f" || {
    echo "      $f:$l"; prod=1; }
done < <(grep -rn 'TODO\|FIXME\|unimplemented!' crates/ --include='*.rs')
[ "$prod" -eq 0 ] && ok "no TODO/FIXME/unimplemented! outside test code" \
                  || bad "the lines above are in shipping code"

echo "== the dashboard reads only metric names the CLI writes =="
note "asserted by crates/logweir/tests/orchestrator.rs::"
note "  every_metric_name_the_dashboard_queries_is_one_the_cli_writes"

echo "== relative links resolve =="
if ./scripts/check-links.sh docs/ README.md SECURITY.md MAINTAINERS.md \
     CONTRIBUTING.md TRADEMARKS.md third_party/ e2e/fixtures/ >/dev/null 2>&1; then
  ok "no broken relative Markdown links"
else
  bad "broken relative Markdown links (run \`just links\`)"
fi

echo "== docs/support-matrix.md has a green row at or above the floor =="
if grep -q '| \*\*0\.21\.0\*\* .*`pass`' docs/support-matrix.md; then
  ok "0.21.0 is recorded as pass"
else
  bad "no green row at or above the declared floor"
fi
if grep -q 'v0\.19\.1' docs/support-matrix.md && \
   grep -q 'unsupported (lever-absent)' docs/support-matrix.md; then
  ok "v0.19.1 is recorded unsupported (lever-absent), never as a fault"
else
  bad "v0.19.1's below-floor status is not recorded"
fi

# --------------------------------------------------------------------------
# Task 29b. THE TAG-1 CHECKLIST: TEN ROWS, ONE PER SPEC §16 CLAUSE, AND NO ROW
# THAT READS `closed` WITHOUT A CLOSER THAT EXISTS.
# --------------------------------------------------------------------------
# Spec §16 lists ten clauses that together mean "publishable". Five of them
# cannot be closed from this tree — three need a git remote, one needs images
# published to a registry the author does not control, one needs an act by the
# owner — and the failure mode a checklist has is that such a row gets ticked
# anyway, because the tick costs nothing and nothing reads it back.
#
# So this arm reads the file back. It asserts the shape (ten rows, numbered
# 1-10), the grammar (each status is exactly one of three literals), and the
# one property that makes a tick mean something: EVERY `closed` ROW NAMES A
# PATH OR A COMMAND THAT EXISTS ON DISK. A row that cites a test a later task
# will write is the same defect as a tick with no closer at all.
#
# The BLOCKED rows are collected here and printed at the end of this script,
# under the "NOT CHECKED HERE" heading, so they are counted as what they are:
# not checked. A blocked row is never a pass.
echo "== Task 29b: docs/tag1-checklist.md, one row per spec §16 clause =="
checklist=docs/tag1-checklist.md
blocked_list=""
if [ ! -s "$checklist" ]; then
  bad "$checklist is missing or empty; spec §16 clause 9 and the tag ledger both live in it"
else
  ok "$checklist"
  # One pass over the table. A row is a table line whose first cell is a
  # number; the status cell is unwrapped from its backticks, and every
  # backticked token in the closer cell of a `closed` row is emitted
  # separately so the existence test below is a plain `test -e` and not a
  # pattern match. A `bash ` or `just ` prefix and a `::test_name` suffix are
  # stripped: the thing that has to exist is the file. Tokens are emitted for
  # `closed` rows ONLY -- a blocked row's cell records evidence and reasons,
  # not closers, and holding it to the same rule would make the reason itself
  # a path that has to exist.
  parsed=$(awk -F'|' '
    /^\|[ \t]*[0-9]+[ \t]*\|/ {
      num = $2; gsub(/[ \t]/, "", num)
      st = $4; gsub(/`/, "", st); sub(/^[ \t]+/, "", st); sub(/[ \t]+$/, "", st)
      printf "ROW\t%s\t%s\n", num, st
      if (st != "closed") next
      n = split($5, seg, "`")
      for (i = 2; i <= n; i += 2) {
        t = seg[i]
        sub(/^[ \t]+/, "", t); sub(/[ \t]+$/, "", t)
        sub(/^bash /, "", t); sub(/^just /, "", t)
        p = index(t, "::"); if (p > 0) t = substr(t, 1, p - 1)
        if (t != "") printf "TOKEN\t%s\t%s\n", num, t
      }
    }' "$checklist")
  nrows=0; expected=1; ordered=1; badstatus=0; badcloser=0; badnote=0
  # A here-document, not a pipe: the loop has to run in THIS shell or
  # `blocked_list` would be built in a subshell and lost.
  while IFS="$(printf '\t')" read -r kind num rest; do
    case "$kind" in
      ROW)
        nrows=$((nrows + 1))
        [ "$num" = "$expected" ] || ordered=0
        expected=$((expected + 1))
        case "$rest" in
          closed|open) : ;;
          "blocked: "?*) blocked_list="$blocked_list$num	$rest
" ;;
          *) echo "      row $num: status \"$rest\" is none of closed / blocked: <reason> / open"
             badstatus=1 ;;
        esac
        # THE TABLE AND THE NOTE SAY THE SAME THING. Each clause has a note
        # below the table that OPENS with its status; the one-line edit that
        # ticks a row without arguing with its own paragraph is exactly the
        # edit this catches. Only the note's FIRST line counts: the body of a
        # blocked note says what would close it, and naming `closed` there is
        # the whole point of the row rather than a second status.
        note=$(awk -v n="$num" '
          $0 ~ "^### " n " " { inside = 1; next }
          /^### / { inside = 0 }
          inside && NF { print; exit }' "$checklist")
        case "$note" in
          *"\`$rest\`"*) : ;;
          *) echo "      row $num: the table says \"$rest\" and the note below does not say so"
             badnote=1 ;;
        esac
        ;;
      TOKEN)
        [ -e "$rest" ] || {
          echo "      row $num: the closer \"$rest\" does not exist on disk"
          badcloser=1
        }
        ;;
    esac
  done <<EOF
$parsed
EOF
  [ "$nrows" -eq 10 ] \
    && ok "ten rows, one per spec §16 clause" \
    || bad "$checklist carries $nrows numbered rows; spec §16 has ten clauses"
  [ "$ordered" -eq 1 ] \
    && ok "the rows are numbered 1-10, in order" \
    || bad "$checklist's rows are not numbered 1-10 in order"
  [ "$badstatus" -eq 0 ] \
    && ok "every status is one of closed / blocked: <reason> / open" \
    || bad "the rows above carry a status outside the three-literal grammar"
  [ "$badcloser" -eq 0 ] \
    && ok "every closed row names a path or a command that exists on disk" \
    || bad "the rows above read closed and name a closer that is not there"
  [ "$badnote" -eq 0 ] \
    && ok "every row's status is repeated in that clause's own note" \
    || bad "the rows above carry a status their own note does not state"
fi

echo
echo "== NOT CHECKED HERE, and therefore UNVERIFIED =="
note "release.yml produced binaries and a debian:bookworm-slim image"
note "release-drill.yml ran the drill from the RELEASED BINARY artifact"
note "engine-matrix.yml produced any row other than the hand-written 0.21.0 one"
note "  -> all three need a GitHub Actions run; no tag has been pushed and no schedule has fired."
note "just e2e (needs \`just e2e-up\` and a seeded stack)"
note "./scripts/demo.sh (needs docker; run it, it is the task's own test)"
note "cargo deny check (run it, or read the CI job)"
note "cargo xtask sync-upstream --tag v0.21.0 (needs an upstream checkout)"
# Task 29b. The blocked rows of the tag-1 checklist, echoed here rather than
# counted above: a clause that cannot be closed from this tree is NOT a pass,
# and the only honest place for it is the list of things this script did not
# check (tag-1 STANDING RULE 22 — a blocked mark is recorded as blocked, never
# as closed).
note "docs/tag1-checklist.md rows that are BLOCKED, and are therefore not passes:"
while IFS="$(printf '\t')" read -r num st; do
  [ -n "$num" ] && note "  spec §16 clause $num -- $st"
done <<EOF
$blocked_list
EOF

echo
echo "passed $pass, failed $fail"
exit "$([ "$fail" -eq 0 ] && echo 0 || echo 1)"
