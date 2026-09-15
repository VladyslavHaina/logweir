#!/usr/bin/env bash
# Global Constraint 2 (spec §7.1). A name grep proves the wrong thing on its
# own — a text match on a type name says nothing about linkage — so linkage is
# proved by cargo, and the grep is narrowed to the two forms that actually
# create a link. The vendored structs, xtask and docs legitimately NAME the
# upstream types (SegmentMetadata, DryRunReport) and are excluded.
#
# THE TWO CARGO CHECKS MUST NEVER RUN INSIDE `cargo test`: they take the
# package-cache lock, and a unit test that takes it is a unit test that can
# hang behind a concurrent build — which is why every test that exercises this
# script sets LOGWEIR_ROOT and thereby skips them (GC22's 15 s per-test bound).
set -euo pipefail

# LOGWEIR_ROOT exists so the tests can point this at a temp workspace overlay;
# it defaults to the repository root, exactly like scripts/check-one-signer.sh:58.
ROOT="${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

# The invocation-shape check (check A) is a Python pass over the source, for the
# same reason check-one-signer.sh walks cargo's JSON in Python: paren matching
# is not a regular language and a regex that pretends otherwise is a gate that
# reports "ok" on a violation. REFUSE, naming the missing tool, rather than
# falling through — a guard that skips a check and still exits 0 is a check that
# cannot fail.
if ! command -v python3 >/dev/null 2>&1; then
  echo "check-no-oso: REFUSING to run — \`python3\` is not on PATH." >&2
  echo "  This gate needs \`python3\` for the engine-invocation shape check." >&2
  echo "  It builds nothing and reaches no network. Install it and re-run." >&2
  exit 1
fi

fail=0

if [ -z "${LOGWEIR_ROOT:-}" ]; then

echo "== cargo tree: no consumer of kafka-backup-core =="
tree_err=$(cargo tree --workspace --invert kafka-backup-core 2>&1 >/dev/null) && tree_rc=0 || tree_rc=$?
if [ "$tree_rc" -eq 0 ]; then
  echo "FAIL: kafka-backup-core is present in the workspace dependency graph"
  cargo tree --workspace --invert kafka-backup-core || true
  fail=1
elif printf '%s' "$tree_err" | grep -q 'did not match any packages'; then
  echo "ok: kafka-backup-core not in the graph"
else
  echo "FAIL: cargo tree errored for a reason other than the package being absent:"
  printf '%s\n' "$tree_err"
  fail=1
fi

echo "== cargo metadata: no workspace crate declares it, under any feature/target =="
if ! meta=$(cargo metadata --format-version 1 --all-features 2>&1); then
  echo "FAIL: cargo metadata errored (the workspace does not load):"
  printf '%s\n' "$meta"
  fail=1
elif printf '%s' "$meta" | grep -q '"name":"kafka-backup-core"'; then
  echo "FAIL: kafka-backup-core appears in cargo metadata --all-features"
  fail=1
else
  echo "ok: absent from metadata"
fi

echo "== narrow linkage grep =="
# POSIX character classes, not the GNU `\s` extension: this repository is
# developed on macOS, whose BSD ERE grep silently matches nothing for `\s`
# and would report `ok: no linkage` on a genuine violation.
if grep -rnE '^[[:space:]]*(use|extern crate)[[:space:]]+kafka_backup_core' crates/ \
     --include='*.rs' \
     | grep -v '^crates/logweir-engine-oso/src/vendored/' ; then
  echo "FAIL: a source file links kafka_backup_core"
  fail=1
else
  echo "ok: no linkage"
fi

else
  echo "check-no-oso: LOGWEIR_ROOT set — running the GC3 block only"
fi

# ---------------------------------------------------------------------------
# Global Constraint 3, REVISED 2026-09-09 by docs/mvp/03-spec.md §5 and
# docs/architecture.md#adr-0008-mvp-constraint-amendments §D: FOUR engine subcommands are
# reachable from shipped code, not three. `backup` joins the contract because
# GC18's `--from-cluster` path renders and runs a backup; `list`,
# `restore-status`, `offset`, `evidence-verify` and `validation evidence-verify`
# stay denied.
#
# TWO ALLOWLISTS, DELIBERATELY, AND THEY ARE NOT THE SAME LIST.
# ENGINE_RUNTIME_ALLOWLIST is the CONTRACT: the four subcommand tokens GC3
# states. ENGINE_ARGV_ALLOWLIST is what an argv array may legally contain: the
# same four plus `run` (the second word of the two-word `validation run`) and
# the three pieces of argv furniture every invocation carries — `--config`,
# `--format`, `json`. The failure text quotes the CONTRACT, never the furniture.
# Subcommand *tokens* only — never the binary name and never a flag. Per global
# ruling GR8, `.engine/kafka-backup --version` is permitted and must not trip
# this check.
ENGINE_RUNTIME_ALLOWLIST="backup restore validate-restore validation"
ENGINE_ARGV_ALLOWLIST="backup restore validate-restore validation run --config --format json"
export ENGINE_RUNTIME_ALLOWLIST ENGINE_ARGV_ALLOWLIST

# CHECK A — the invocation shape. This is the gate. A violation is a
# double-quoted string literal that is not in ENGINE_ARGV_ALLOWLIST and that
# appears in the same balanced expression as `run_engine(` or as the engine
# binary path. Scanning the whole file instead would report the ordinary
# English word "offset" in a test assertion; scanning only the balanced
# expression is what makes the difference between prose and an invocation.
check_invocation_shape() {
  python3 - <<'PY'
import os, pathlib, re, sys

# The CONTRACT (four subcommands) and the legal argv vocabulary. See the shell
# comment above: these are two different lists on purpose.
ENGINE_RUNTIME_ALLOWLIST = os.environ["ENGINE_RUNTIME_ALLOWLIST"].split()
ENGINE_ARGV_ALLOWLIST = frozenset(os.environ["ENGINE_ARGV_ALLOWLIST"].split())

# An argv TOKEN is a bare word or a flag: letters, digits and hyphens, with an
# optional leading `-` or `--`. A path (`/dev/null`), a format string
# (`{}: not valid UTF-8`) and a env-var name (`RUST_LOG`) are not subcommand
# tokens and are not candidates; a literal carrying a SPACE is handed to check
# B, which owns the two-word forms (`validation evidence-verify`).
ARGV_TOKEN = re.compile(r"^(--|-)?[A-Za-z][A-Za-z0-9-]*$")

# The invocation markers. `run_engine(` is the crate's own spawn helper; the two
# `Command::new` forms are a direct spawn of the engine binary.
MARK_CALL = ("run_engine(",)
MARK_BIN = ("Command::new(&self.binary)", "Command::new(binary)")


def contract(runtime_allowlist):
    """Render the CONTRACT for a human. `validation` is the token; the command
    itself is the two-word `validation run`, which is how GC3 writes it."""
    return ", ".join(
        "validation run" if c == "validation" else c for c in runtime_allowlist
    )


def lex(src):
    """One pass: a mask marking the bytes that are CODE (not a comment, not
    inside a literal) and the list of double-quoted literals. Paren matching
    that counts a paren inside a comment or a string is paren matching that
    reports the wrong span."""
    n = len(src)
    mask = bytearray(n)
    lits = []
    i = 0
    while i < n:
        c = src[i]
        if c == "/" and i + 1 < n and src[i + 1] == "/":
            j = src.find("\n", i)
            i = n if j < 0 else j
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "*":
            depth, i = 1, i + 2          # Rust block comments nest.
            while i < n and depth:
                if src.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif src.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
            continue
        if c in "rb":                     # r"", b"", br#""#, r#""# …
            k = i
            while k < n and src[k] in "rb":
                k += 1
            if k < n and src[k] == "#":
                h = 0
                while k + h < n and src[k + h] == "#":
                    h += 1
                if k + h < n and src[k + h] == '"':
                    body = k + h + 1
                    close = '"' + "#" * h
                    e = src.find(close, body)
                    e = n if e < 0 else e
                    lits.append((i, src[body:e]))
                    i = e + len(close)
                    continue
            if k > i and k < n and src[k] == '"':
                j = k + 1
                while j < n and src[j] != '"':
                    j += 2 if src[j] == "\\" and src[i] != "r" else 1
                lits.append((i, src[k + 1 : j]))
                i = j + 1
                continue
        if c == '"':
            j = i + 1
            while j < n and src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            lits.append((i, src[i + 1 : j]))
            i = j + 1
            continue
        if c == "'":                      # char literal, or a lifetime.
            if i + 2 < n and src[i + 1] == "\\":
                j = i + 2
                while j < n and src[j] != "'":
                    j += 1
                i = j + 1
                continue
            if i + 2 < n and src[i + 2] == "'":
                i += 3
                continue
        mask[i] = 1
        i += 1
    return mask, lits


def match_paren(src, mask, open_idx):
    depth, i, n = 0, open_idx, len(src)
    while i < n:
        if mask[i]:
            if src[i] == "(":
                depth += 1
            elif src[i] == ")":
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return n


def stmt_end(src, mask, start):
    """End of the builder statement: the first `;` at depth 0, or the close of
    the expression that encloses it."""
    depth, i, n = 0, start, len(src)
    while i < n:
        if mask[i]:
            ch = src[i]
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth -= 1
                if depth < 0:
                    return i
            elif ch == ";" and depth <= 0:
                return i
        i += 1
    return n


def find_code(src, mask, needle, frm=0, to=None):
    to = len(src) if to is None else to
    out, i = [], src.find(needle, frm, to)
    while i >= 0:
        if mask[i]:                        # a marker named in a doc comment is prose
            out.append(i)
        i = src.find(needle, i + 1, to)
    return out


def spans(src, mask):
    """The balanced expressions whose literals are argv."""
    out = []
    for m in MARK_CALL:
        for i in find_code(src, mask, m):
            o = i + len(m) - 1             # the marker's own `(`
            out.append((o, match_paren(src, mask, o)))
    for m in MARK_BIN:
        # A `Command` builder's argv arrives only through `.arg`/`.args`; its
        # `.env`/`.current_dir` values are not argv, so the span for these two
        # markers is each `.arg(`/`.args(` call in the same statement rather
        # than `Command::new`'s own parens (which enclose the binary path and
        # never a subcommand, and would make both markers dead).
        for i in find_code(src, mask, m):
            end = stmt_end(src, mask, i + len(m))
            for sub in (".args(", ".arg("):
                for j in find_code(src, mask, sub, i + len(m), end):
                    o = j + len(sub) - 1
                    out.append((o, match_paren(src, mask, o)))
    return out


violations = 0
for path in sorted(pathlib.Path("crates").glob("**/*.rs")):
    src = path.read_text(encoding="utf-8", errors="replace")
    mask, lits = lex(src)
    for (o, c) in spans(src, mask):
        for (start, text) in lits:
            if not (o < start < c):
                continue
            if not text or not ARGV_TOKEN.match(text):
                continue
            if text in ENGINE_ARGV_ALLOWLIST:
                continue
            line = src.count("\n", 0, start) + 1
            print(f"check-no-oso: {path}:{line}: engine invocation names `{text}`, which is outside {{{contract(ENGINE_RUNTIME_ALLOWLIST)}}}")
            violations += 1

sys.exit(1 if violations else 0)
PY
}

# CHECK B — the token scan. The secondary, and it owns the forms check A cannot
# see: a two-word subcommand, a token built in prose, a token in a fixture. A
# hit is permitted only by a JUSTIFIED escape on the same physical line —
# `// engine-token-ok: <reason>` with a reason of at least ten characters. A
# bare marker is not an escape: an unexplained exemption is how a denylist
# rots into a comment.
check_token_secondary() {
  hits=$(grep -rnE '"(list|restore-status|offset|evidence-verify|validation evidence-verify)"' crates/ \
           --include='*.rs' || true)
  bad=0
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    if printf '%s\n' "$hit" | grep -qE '// engine-token-ok:[[:space:]]*[^[:space:]].{9,}'; then
      continue
    fi
    printf '%s\n' "$hit"
    bad=1
  done <<EOF
$hits
EOF
  return "$bad"
}

echo "== engine invocation shape (GC3's gate) =="
if check_invocation_shape; then
  echo "ok: every engine invocation names only contracted subcommands"
else
  echo "FAIL: an engine invocation names a subcommand outside the GC3 contract"
  fail=1
fi

echo "== engine subcommand tokens under crates/ (the secondary) =="
if check_token_secondary; then
  echo "ok: no unjustified denied engine subcommand token under crates/"
else
  echo "FAIL: a denied kafka-backup subcommand token is named under crates/ with no justified \`// engine-token-ok: <reason>\` escape"
  fail=1
fi

exit "$fail"
