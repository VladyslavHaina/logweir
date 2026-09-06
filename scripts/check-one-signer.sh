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
# THREE CHECKS, because each alone proves the wrong thing — the same
# three-ways-proved shape as `scripts/check-no-oso.sh`:
#
#   1. the reverse-dependency walk over `cargo metadata` (the property
#      carrier: linkage);
#   2. the two signing primitives, `p256` and `ed25519-dalek`, over
#      `cargo tree --invert` (a second, independent walk of the same claim
#      from the primitive end);
#   3. a narrow source grep for `sign_detached` / `SigningKey` (which catches a
#      crate that NAMES the API before its manifest edit lands).
#
# TWO DIFFERENT WALKS, DELIBERATELY, AND THEY HAVE DIFFERENT ALLOWLISTS.
# Check 1 counts EVERY dependency kind — normal, build AND dev — because a
# backdoor added under `[dev-dependencies]` links the signer just as hard as
# one under `[dependencies]`; over that graph the reaching set is {logweir,
# e2e}, since `e2e` takes `logweir-evidence` as a dev-dependency. Check 2 keeps
# `-e normal`, which drops dev edges, so its allowlist is {logweir-evidence,
# logweir}. Both statements are true of different walks; neither is a typo.
#
# `ring`, `rustls` AND `aws-lc-rs` ARE DELIBERATELY NOT CHECKED. They are TLS
# primitives, reachable from `object_store` / `reqwest` / `ureq`, and have
# nothing to do with signing. Blacklisting them is exactly what made the first
# version of this script RED on an unmodified tree — and the governing rule for
# this gate is that if it is not green against the unmodified tree, the script
# is wrong, not the tree. Do not add them back.
#
# `weirkeeper`, the controller, and every cluster object are OUT OF SCOPE here:
# none exists yet. When one does, it does not go on the allowlist — the ruled
# remedy is to extract a verify-only crate, so that a controller which VERIFIES
# a signature does not thereby link the signer.
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
ALLOWED_PRIMITIVE="logweir-evidence logweir"
# The signing primitives themselves, from crates/logweir-evidence/Cargo.toml.
PRIMITIVES="p256 ed25519-dalek"

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

# ---------------------------------------------------------------- check 2
echo "== cargo tree: the signing primitives ($PRIMITIVES) reach no other workspace crate =="
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
  for c in $names; do
    case " $ALLOWED_PRIMITIVE " in
      *" $c "*) ;;
      *)
        echo "FAIL: $c reaches the signing primitive $prim over a normal edge and is not on the allowlist" >&2
        fail=1
        ;;
    esac
  done
  echo "ok: $prim reaches {$(echo $names | tr ' ' ',')}"
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
      fail=1
      ;;
  esac
done
echo "ok: the crates naming the signing API are {$(echo $named | tr ' ' ',')}"

exit "$fail"
