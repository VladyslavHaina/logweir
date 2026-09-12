#!/usr/bin/env bash
# G2′ — THE LINK-TIME SINGLE-SIGNER GATE. Phase 1 line item 1c.
#
# WHAT THIS PROVES, exactly and only: no Logweir crate outside a named
# allowlist LINKS the signing API. The set of workspace crates from which
# `logweir-evidence` is reachable over the dependency graph is exactly
# {logweir, e2e}, and that set is computed from the graph, not from a text
# search.
#
# WHAT THIS DOES NOT PROVE: that the control plane cannot sign. It cannot. The
# scorecard signing key is a Kubernetes Secret, and `create pods` in its
# namespace is equivalent to holding it — a pod the control plane creates can
# mount the Secret and sign whatever it likes with no Logweir crate involved.
# The stronger claim ("the control plane provably cannot sign") was made once,
# was RED on the tree it was asserted about, and was withdrawn. Nothing in this
# script's output, its comments, or the CI step that runs it may restate it.
# A documented guarantee the code does not deliver is the defect class this
# gate exists to remove, not to repeat.
#
# FOUR CHECKS, because each alone proves the wrong thing — the same
# proved-several-ways shape as `scripts/check-no-oso.sh`:
#
#   1. the reverse-dependency walk over `cargo metadata` (the property
#      carrier: linkage);
#   2. the two primitive crates, `p256` and `ed25519-dalek`, over
#      `cargo tree --invert` (a second, independent walk from the primitive
#      end — RE-SCOPED, see below);
#   3. a narrow source grep for `sign_detached` / `SigningKey` (which catches a
#      crate that NAMES the API before its manifest edit lands);
#   4. a second reverse-dependency walk, for `logweir-verify`, against its own
#      separate allowlist (added with the verify-only extraction).
#
# FOUR WALKS, DELIBERATELY, AND THEY HAVE DIFFERENT ALLOWLISTS.
# Check 1 counts EVERY dependency kind — normal, build AND dev — because a
# backdoor added under `[dev-dependencies]` links the signer just as hard as
# one under `[dependencies]`; over that graph the reaching set is {logweir,
# e2e}, since `e2e` takes `logweir-evidence` as a dev-dependency. Check 2 keeps
# `-e normal`, which drops dev edges. Check 4 counts every kind again, like
# check 1, but walks a different target. Each statement is true of its own
# walk; none is a typo.
#
# CHECK 2 IS RE-SCOPED, AND THE REASON IS HERE RATHER THAN IN A COMMIT
# MESSAGE. Verification and signing SHARE their primitives: `VerifyingKey` is
# an enum over `p256` and `ed25519-dalek` and `verify_detached` matches both
# arms, so `crates/logweir-verify` — the verify-only crate the ruling below
# demanded — must depend on both. Once it does, "reaches a signing primitive"
# is no longer a proxy for "can sign": `p256` and `ed25519-dalek` are reached
# by everything that merely CHECKS a signature. Check 2 therefore no longer
# claims that only the signer reaches them. It claims the narrower thing it can
# still prove — THESE FOUR CRATES AND NO OTHERS REACH THE PRIMITIVE CRATES —
# and the crates that reach the SIGNING half are checks 1 and 3.
#
# This widening is RECORDED, not silent, and that is the whole point: an
# implementer who hit check 2 could have added two names to `ALLOWED_PRIMITIVE`
# and retired the check for `weirkeeper` for good. It is written here, in
# `docs/adr/0008-mvp-constraint-amendments.md` §E, and in `docs/mvp/03-spec.md`
# §10's G-SIGN row, and
# `crates/logweir/tests/one_signer_gate.rs`'s
# `check_two_states_the_narrowed_claim` fails if this paragraph goes missing or
# if a fifth name appears on that allowlist.
#
# THE FOURTH ALLOWLIST DOES NOT WEAKEN CHECKS 1 AND 3. `ALLOWED_VERIFY_LINK`
# governs who may link the VERIFYING crate, which holds no `SigningKey`, no
# `sign_detached` and no entropy source; `ALLOWED_LINK` and `ALLOWED_SOURCE`
# are byte-identical to what they were before the extraction, and `weirkeeper`
# is absent from both.
#
# `ring`, `rustls` AND `aws-lc-rs` ARE DELIBERATELY NOT CHECKED. They are TLS
# primitives, reachable from `object_store` / `reqwest` / `ureq`, and have
# nothing to do with signing. Blacklisting them is exactly what made the first
# version of this script RED on an unmodified tree — and the governing rule for
# this gate is that if it is not green against the unmodified tree, the script
# is wrong, not the tree. Do not add them back.
#
# `weirkeeper`, the controller, and every cluster object are OUT OF SCOPE for
# checks 1 and 3: it does not go on either allowlist — the ruled remedy was to
# extract a verify-only crate, so that a controller which VERIFIES a signature
# does not thereby link the signer. THAT REMEDY HAS BEEN CARRIED OUT:
# `crates/logweir-verify` holds `VerifyingKey`, `verify_detached` and `pae`,
# `crates/logweir-evidence` keeps `SigningKey` and `sign_detached` and
# re-exports the verifying half, and check 4 below is the allowlist for the
# verifying crate. `weirkeeper` appears on `ALLOWED_VERIFY_LINK` and on
# `ALLOWED_PRIMITIVE`, and on neither `ALLOWED_LINK` nor `ALLOWED_SOURCE`.
#
# COSTS NOTHING AND REACHES NOTHING: no network, no Docker, no `.engine/`, and
# it builds not one object file. `cargo metadata --no-deps` performs no
# resolution at all, and `cargo tree` reads the committed `Cargo.lock`.
set -euo pipefail

# `LOGWEIR_ROOT` exists so the tests can point this at a temp workspace overlay;
# it defaults to the repository root, exactly like `scripts/check-pure-core.sh`.
ROOT="${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

# The toolchain pin, per the standing rule that no lint gate resolves a
# toolchain over the network. `cd`-ing into a tree that carries
# `rust-toolchain.toml` is normally enough, but a `cargo` reached through
# rustup's shim with no override in scope resolves rustup's DEFAULT channel and
# SYNCS IT FROM THE NETWORK — measured, in another gate, from inside `just
# lint`. Exporting the pin costs one `sed` and removes the possibility.
if [ -z "${RUSTUP_TOOLCHAIN:-}" ] && [ -f rust-toolchain.toml ]; then
  pinned="$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' rust-toolchain.toml | head -1)"
  if [ -n "$pinned" ]; then
    export RUSTUP_TOOLCHAIN="$pinned"
  fi
fi

# The gate builds nothing, but it does need two tools. REFUSE, naming the
# missing one, rather than falling through: a guard that skips a check and
# still exits 0 is a check that cannot fail.
for tool in cargo python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "check-one-signer: REFUSING to run — \`$tool\` is not on PATH." >&2
    echo "  This gate needs \`cargo\` (for the two dependency walks) and \`python3\`" >&2
    echo "  (for the reverse-dependency walk over cargo's JSON). It builds nothing" >&2
    echo "  and reaches no network. Install the missing tool and re-run." >&2
    exit 1
  fi
done

fail=0

SIGNER="logweir-evidence"
# Crates permitted to LINK the signer (any dependency kind).
ALLOWED_LINK="logweir e2e"
# Crates permitted to NAME the signing API in source.
ALLOWED_SOURCE="logweir-evidence logweir e2e"
# Crates permitted to reach a signing PRIMITIVE over normal edges only. This is
# the same claim as ALLOWED_LINK seen from the other end of a narrower walk:
# `e2e` is absent because its edge is a dev-dependency and `-e normal` drops it.
# RE-SCOPED (see the header): these are the crates permitted to reach a
# PRIMITIVE crate over normal edges. It is no longer the same claim as
# ALLOWED_LINK seen from the other end, because verification shares the
# primitives with signing. `logweir-verify` is here because a verifier needs
# both primitives; `weirkeeper` is here because it links `logweir-verify`
# (spec §8). FOUR NAMES, IN THIS ORDER, AND NO FIFTH — a fifth is the mutant
# `check_two_states_the_narrowed_claim` exists to kill.
ALLOWED_PRIMITIVE="logweir-evidence logweir logweir-verify weirkeeper"
# Crates permitted to LINK the VERIFYING crate (any dependency kind). A FOURTH,
# SEPARATE allowlist: it governs `logweir-verify`, which holds no `SigningKey`,
# no `sign_detached` and no entropy source, and it leaves ALLOWED_LINK and
# ALLOWED_SOURCE untouched.
ALLOWED_VERIFY_LINK="logweir-evidence logweir weirkeeper e2e"
# The primitive crates check 2 walks are NOT a literal here any more (Task 32,
# stage-2 carried item). `PRIMITIVES="p256 ed25519-dalek"` was a fixed list, so
# a THIRD signing primitive added to `logweir-evidence` passed this gate in
# silence — and the verify-only split is exactly the kind of change during which
# a primitive moves without anyone noticing. The set is now DERIVED, below check
# 1, from `logweir-evidence`'s direct dependencies in the `cargo metadata
# --no-deps` this script already runs, minus the crates the checked-in
# classification calls non-primitive. See $PRIMITIVES_CLASSIFY.
PRIMITIVES_CLASSIFY="scripts/logweir-evidence-primitives.classify"
# The verify-only crate check 4 walks.
VERIFIER="logweir-verify"

meta_file="$(mktemp)"
trap 'rm -f "$meta_file"' EXIT

# ---------------------------------------------------------------- check 1
echo "== cargo metadata: which workspace crates reach $SIGNER, over EVERY dependency kind =="
# Deliberate: the exit status is not read through a pipe, and a `cargo
# metadata` that fails aborts the script under `set -e` rather than being
# swallowed into an empty reaching set — an empty set would otherwise be
# reported as "the allowlist is stale" and read like a code change.
cargo metadata --no-deps --format-version 1 > "$meta_file"

reaching="$(python3 - "$meta_file" "$SIGNER" <<'PYEOF'
import json, sys

meta = json.load(open(sys.argv[1]))
target = sys.argv[2]

# `--no-deps` lists the workspace members and nothing else, so this set is
# exactly the intra-workspace vocabulary. It performs no resolution: it never
# consults Cargo.lock, never hits the network and never builds.
names = {p["name"] for p in meta["packages"]}

# Reverse edges, counting EVERY dependency kind: `kind` is null for a normal
# dependency, "dev" for a dev-dependency and "build" for a build-dependency.
# Dropping the dev edges here is exactly the mutation this walk must not
# survive: it is the only walk that sees a backdoor added under
# `[dev-dependencies]`.
rev = {}
for p in meta["packages"]:
    for d in p.get("dependencies", []):
        if d["name"] in names:
            rev.setdefault(d["name"], set()).add(p["name"])

seen = set()
frontier = [target]
while frontier:
    node = frontier.pop()
    for parent in rev.get(node, ()):
        if parent not in seen:
            seen.add(parent)
            frontier.append(parent)
seen.discard(target)
for n in sorted(seen):
    print(n)
PYEOF
)" || { echo "FAIL: the reverse-dependency walk over cargo metadata failed" >&2; exit 1; }

# One line per crate becomes one space-separated list, so both directions below
# can be tested the same way.
reaching="$(echo $reaching)"

# Equality against the allowlist, both directions. An EXTRA member is a crate
# that gained the signer; a MISSING member is an allowlist that has outlived
# the edge it was written for, and is just as much a defect — a stale allowlist
# is how this check quietly stops meaning anything.
for c in $reaching; do
  case " $ALLOWED_LINK " in
    *" $c "*) ;;
    *)
      echo "FAIL: $c links the signer and is not on the allowlist" >&2
      fail=1
      ;;
  esac
done
for c in $ALLOWED_LINK; do
  case " $reaching " in
    *" $c "*) ;;
    *)
      echo "FAIL: allowlist names $c, which no longer reaches the signer (the allowlist is stale)" >&2
      fail=1
      ;;
  esac
done
if [ "$fail" -eq 0 ]; then
  echo "ok: the crates reaching $SIGNER are exactly {$(echo $reaching | tr ' ' ',')}"
fi

# ------------------------------------------------- the primitive set, DERIVED
# Task 32, stage-2 carried item. `PRIMITIVES` comes from the graph, not from a
# literal: every DIRECT dependency of $SIGNER in the `cargo metadata --no-deps`
# check 1 already produced, minus the crates `$PRIMITIVES_CLASSIFY` classifies
# `non-primitive`.
#
# FAIL-CLOSED. A direct dependency nobody has classified is treated as a signing
# primitive and this exits 1 naming it — so a third signing crate is a diff a
# reviewer reads rather than a walk that silently keeps covering two. The file
# is checked for staleness in the other direction too (an entry for a crate that
# is no longer a direct dependency), for the same reason check 1's allowlist is:
# a classification that has outlived its edge stops meaning anything. And an
# entry without a `reason:` line is refused — the reason is the reviewable part.
#
# NO SECOND `cargo metadata`: $meta_file is reused, so this costs one python3
# pass over JSON that is already on disk, reaches no network and builds nothing.
# The program is run with its status read on its OWN LINE and its output
# captured to a file — never `VAR="$(python3 … <<HEREDOC …)"`. A heredoc whose
# body contains backticks inside a command substitution is mis-parsed by bash:
# measured here, the closing `)` was found 60 lines further down the file and
# the whole of check 2 ran inside the substitution, exiting 0 with nothing
# printed. A gate that reports success having run nothing is this repository's
# signature defect, so the shape that cannot do it is used instead.
prim_file="$(mktemp)"
trap 'rm -f "$meta_file" "$prim_file"' EXIT
set +e
python3 - "$meta_file" "$SIGNER" "$PRIMITIVES_CLASSIFY" > "$prim_file" <<'PYEOF'
import json, sys

meta_path, target, classify_path = sys.argv[1], sys.argv[2], sys.argv[3]
meta = json.load(open(meta_path))

pkg = next((p for p in meta["packages"] if p["name"] == target), None)
if pkg is None:
    print(f"FAIL: {target} is not a workspace member — check 2 has nothing to derive its "
          f"primitive set from", file=sys.stderr)
    sys.exit(1)

# EVERY dependency kind, exactly as check 1's walk counts every kind: a signing
# crate added under [dev-dependencies] is a primitive that reached this crate.
direct = sorted({d["name"] for d in pkg.get("dependencies", [])})

try:
    raw = open(classify_path).read()
except OSError as e:
    print(f"FAIL: cannot read the primitive classification {classify_path}: {e}", file=sys.stderr)
    print("  Check 2 derives its primitive set from the dependency graph MINUS the crates this",
          file=sys.stderr)
    print("  file classifies as non-primitive. Without it every direct dependency is unclassified,",
          file=sys.stderr)
    print("  and the fail-closed polarity would name all of them. Restore the file.", file=sys.stderr)
    sys.exit(1)

VALID = ("primitive", "non-primitive")
klass, reason = {}, {}
pending = None          # the crate whose `reason:` line has not arrived yet
for n, line in enumerate(raw.splitlines(), 1):
    text = line.strip()
    if not text or text.startswith("#"):
        continue
    if text.startswith("reason:"):
        if pending is None:
            print(f"FAIL: {classify_path}:{n}: a `reason:` line with no entry above it",
                  file=sys.stderr)
            sys.exit(1)
        body = text[len("reason:"):].strip()
        if not body:
            print(f"FAIL: {classify_path}:{n}: `{pending}`'s reason line is empty — the reason "
                  f"is the reviewable part of a classification", file=sys.stderr)
            sys.exit(1)
        reason[pending] = body
        pending = None
        continue
    if pending is not None:
        print(f"FAIL: {classify_path}:{n}: `{pending}` has no `reason:` line. Every entry is two "
              f"lines:\n    <crate> <primitive|non-primitive>\n    reason: <why>", file=sys.stderr)
        sys.exit(1)
    parts = text.split()
    if len(parts) != 2 or parts[1] not in VALID:
        print(f"FAIL: {classify_path}:{n}: expected `<crate> <primitive|non-primitive>`, got: {text}",
              file=sys.stderr)
        sys.exit(1)
    name, verdict = parts
    if name in klass:
        print(f"FAIL: {classify_path}:{n}: `{name}` is classified twice", file=sys.stderr)
        sys.exit(1)
    klass[name] = verdict
    pending = name
if pending is not None:
    print(f"FAIL: {classify_path}: `{pending}` has no `reason:` line (end of file)", file=sys.stderr)
    sys.exit(1)

bad = 0
for c in direct:
    if c not in klass:
        print(f"FAIL: {c} is a direct dependency of {target} and is NOT classified in "
              f"{classify_path}.", file=sys.stderr)
        print(f"  Unclassified means PRIMITIVE here (fail-closed): until someone writes down what "
              f"{c} is,", file=sys.stderr)
        print(f"  this gate assumes a third signing crate just arrived. Add two lines to "
              f"{classify_path}:", file=sys.stderr)
        print(f"      {c} primitive        (or non-primitive)", file=sys.stderr)
        print(f"      reason: <why>", file=sys.stderr)
        bad = 1
for c in sorted(klass):
    if c not in direct:
        print(f"FAIL: {classify_path} classifies `{c}`, which is no longer a direct dependency of "
              f"{target} (the classification is stale)", file=sys.stderr)
        bad = 1
if bad:
    sys.exit(1)

prims = [c for c in direct if klass[c] == "primitive"]
if not prims:
    print(f"FAIL: no direct dependency of {target} is classified `primitive` — check 2 would walk "
          f"nothing, and a walk over an empty set is a check that cannot fail", file=sys.stderr)
    sys.exit(1)
print(" ".join(prims))
PYEOF
derive_rc=$?
set -e
if [ "$derive_rc" -ne 0 ]; then
  echo "FAIL: could not derive the signing primitives from the dependency graph (exit $derive_rc)" >&2
  exit 1
fi
PRIMITIVES="$(cat "$prim_file")"
echo "ok: the signing primitives derived from $SIGNER's direct dependencies are {$(echo $PRIMITIVES | tr ' ' ',')} (classified in $PRIMITIVES_CLASSIFY)"

# ---------------------------------------------------------------- check 2
echo "== cargo tree: only {$ALLOWED_PRIMITIVE} reach the primitive crates ($PRIMITIVES) — reaching a primitive is NOT reaching the signer; for that, see checks 1 and 3 =="
# Captured into a variable on its own line, status handled on its own line:
# `cargo tree | grep` would hide cargo's exit code behind grep's.
#
# ONE INVOCATION FOR BOTH PRIMITIVES, and the reason is measured, not tidiness.
# This gate runs inside `just lint`, whose timing arm bounds every individual
# test at 5 s, and two of the tests that exercise this script run it end to
# end. A `cargo tree` costs ~0.4 s of work and, whenever any other cargo on the
# machine holds the package-cache lock, 1.5–2.5 s of "Blocking waiting for file
# lock" on top — measured at 4.06 s for the whole script with two invocations
# and ~1.9 s with one. `--no-dedupe` keeps each root's tree COMPLETE, so
# splitting the output back into one section per primitive below yields exactly
# what a separate invocation per primitive would have printed; `--prefix depth`
# makes the root lines (depth 0) and the package names readable with a POSIX
# regex, instead of one that has to step over the box-drawing characters of the
# default tree prefix.
#
# NOT `--locked`: a probe overlay makes the lock stale, and `--locked` would
# then fail for the wrong reason and name no crate at all. And never `cargo
# update` — the workspace manifest records six precise pins that a bare update
# would float in one uncontrolled step.
invert_args=""
for prim in $PRIMITIVES; do
  invert_args="$invert_args --invert $prim"
done
tree="$(cargo tree --workspace $invert_args -e normal --prefix depth --no-dedupe 2>&1)" || {
  echo "FAIL: cargo tree could not walk the signing primitives ($PRIMITIVES):" >&2
  printf '%s\n' "$tree" >&2
  echo "  If a primitive named in PRIMITIVES is no longer in the graph, the signing crate" >&2
  echo "  changed its primitives: update PRIMITIVES here in the same commit as" >&2
  echo "  crates/logweir-evidence/Cargo.toml, so this walk keeps tracking the real ones." >&2
  exit 1
}

for prim in $PRIMITIVES; do
  # The root line of this primitive's section. Its absence means the walk
  # silently stopped covering a primitive it claims to cover.
  if [ -z "$(printf '%s\n' "$tree" | sed -n "/^0$prim v/p")" ]; then
    echo "FAIL: $prim is not a root of the cargo tree walk — this check is no longer covering it" >&2
    fail=1
    continue
  fi
  # Workspace members are the lines carrying an absolute path in parentheses;
  # registry crates carry none.
  names="$(printf '%s\n' "$tree" \
           | awk -v p="$prim" '
               /^0/ { split(substr($0, 2), a, " "); on = (a[1] == p); next }
               on { print }
             ' \
           | sed -n 's/^[0-9][0-9]*\([A-Za-z0-9_.+-][A-Za-z0-9_.+-]*\) v[0-9][^ ]* (\/.*/\1/p' \
           | sort -u)"
  # Per-primitive, so an `ok:` line is never printed for a primitive that just
  # FAILED: a run that says both at once reads as green to anyone skimming
  # stdout, and check 1 above already guards its `ok:` the same way.
  prim_fail=0
  for c in $names; do
    case " $ALLOWED_PRIMITIVE " in
      *" $c "*) ;;
      *)
        echo "FAIL: $c reaches the signing primitive $prim over a normal edge and is not on the allowlist" >&2
        prim_fail=1
        fail=1
        ;;
    esac
  done
  if [ "$prim_fail" -eq 0 ]; then
    echo "ok: $prim reaches {$(echo $names | tr ' ' ',')}, all on the primitive allowlist — the crates that reach the SIGNING half are checks 1 and 3"
  fi
done

# ---------------------------------------------------------------- check 3
echo "== source grep: only {$ALLOWED_SOURCE} names sign_detached / SigningKey =="
# Absent roots are skipped rather than erroring, so the gate runs against an
# overlay that carries only some of them.
#
# `VerifyingKey` is NOT a token here and must not be added: verification is
# permitted everywhere, and the whole point of the ruled verify-only extraction
# is that a crate may verify without linking the signer.
roots=""
for d in crates/*/src crates/*/tests crates/*/examples e2e/src e2e/tests xtask/src; do
  if [ -d "$d" ]; then
    roots="$roots $d"
  fi
done

# POSIX character classes only. BSD grep on macOS silently matches NOTHING for
# the GNU `\s` / `\b` extensions, which would turn a genuine violation into a
# clean run — the failure mode `scripts/check-pure-core.sh` records.
hits=""
if [ -n "$roots" ]; then
  hits="$(grep -rnE 'sign_detached|SigningKey' $roots --include='*.rs' \
          | grep -v '^[^:]*:[0-9]*:[[:space:]]*//' \
          | grep -v '^[^:]*:[0-9]*:[[:space:]]*///' || true)"
fi

# The owning crate of a hit is read from its manifest's `name`, not guessed
# from the directory: a crate whose directory and package name differ must
# still be reported by the name the allowlist is written in.
crate_of() {
  case "$1" in
    crates/*)
      rest="${1#crates/}"
      d="crates/${rest%%/*}"
      ;;
    e2e/*) d="e2e" ;;
    xtask/*) d="xtask" ;;
    *) d="" ;;
  esac
  if [ -n "$d" ] && [ -f "$d/Cargo.toml" ]; then
    sed -n 's/^name[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$d/Cargo.toml" | head -1
  else
    printf '%s' "$1"
  fi
}

named=""
if [ -n "$hits" ]; then
  paths="$(printf '%s\n' "$hits" | sed 's/:.*//' | sort -u)"
  for p in $paths; do
    c="$(crate_of "$p")"
    case " $named " in
      *" $c "*) ;;
      *) named="$named $c" ;;
    esac
  done
fi
src_fail=0
for c in $named; do
  case " $ALLOWED_SOURCE " in
    *" $c "*) ;;
    *)
      echo "FAIL: $c names the signing API in source and is not on the allowlist:" >&2
      for p in $paths; do
        if [ "$(crate_of "$p")" = "$c" ]; then
          printf '  %s\n' "$p" >&2
        fi
      done
      src_fail=1
      fail=1
      ;;
  esac
done
# Same guard as checks 1 and 2: no `ok:` line for a check that just failed.
if [ "$src_fail" -eq 0 ]; then
  echo "ok: the crates naming the signing API are {$(echo $named | tr ' ' ',')}"
fi

# ---------------------------------------------------------------- check 4
echo "== cargo metadata: which workspace crates reach $VERIFIER, over EVERY dependency kind =="
# THE WALKER IS DUPLICATED FROM CHECK 1 ON PURPOSE. Checks 1 and 3 are
# byte-identical to what they were before the verify-only extraction — that is
# the property spec §10's G-SIGN row asserts and
# `the_two_original_allowlists_are_unchanged` reads — so factoring the walk
# into a shell function would have rewritten check 1. `$meta_file` is reused:
# no second `cargo metadata`.
#
# ONE DIRECTION ONLY, unlike check 1, and the reason is written down rather
# than assumed. Check 1 also fails on a STALE allowlist (a name that no longer
# reaches the signer), because its allowlist describes a graph that exists.
# `ALLOWED_VERIFY_LINK` names `weirkeeper`, which spec §8 pre-authorises and
# which arrives one task later; a staleness arm here would make this gate red
# for that whole interval, and a gate that is red for a scheduled reason is a
# gate people learn to ignore. EXTRA members still fail: an unlisted crate
# linking the verifying crate is exactly what this check is for.
reaching_verify="$(python3 - "$meta_file" "$VERIFIER" <<'PYEOF'
import json, sys

meta = json.load(open(sys.argv[1]))
target = sys.argv[2]

# `--no-deps` lists the workspace members and nothing else, so this set is
# exactly the intra-workspace vocabulary.
names = {p["name"] for p in meta["packages"]}

# Reverse edges, counting EVERY dependency kind — a dev edge onto the verifying
# crate is a link like any other.
rev = {}
for p in meta["packages"]:
    for d in p.get("dependencies", []):
        if d["name"] in names:
            rev.setdefault(d["name"], set()).add(p["name"])

seen = set()
frontier = [target]
while frontier:
    node = frontier.pop()
    for parent in rev.get(node, ()):
        if parent not in seen:
            seen.add(parent)
            frontier.append(parent)
seen.discard(target)
for n in sorted(seen):
    print(n)
PYEOF
)" || { echo "FAIL: the reverse-dependency walk over cargo metadata failed for $VERIFIER" >&2; exit 1; }

reaching_verify="$(echo $reaching_verify)"

verify_fail=0
for c in $reaching_verify; do
  case " $ALLOWED_VERIFY_LINK " in
    *" $c "*) ;;
    *)
      echo "FAIL: $c links the verifying crate $VERIFIER and is not on the verify allowlist" >&2
      verify_fail=1
      fail=1
      ;;
  esac
done
# Same guard as checks 1, 2 and 3: no `ok:` line for a check that just failed.
if [ "$verify_fail" -eq 0 ]; then
  echo "ok: the crates reaching $VERIFIER are {$(echo $reaching_verify | tr ' ' ',')}, all on the verify allowlist — linking the verifier is NOT linking the signer"
fi

exit "$fail"
