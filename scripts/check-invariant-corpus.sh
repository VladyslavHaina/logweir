#!/usr/bin/env bash
# The auditor-side half of the two-reader parity gate (Task 4, T0-6 / T0-3).
#
# `crates/logweir/tests/two_reader_parity.rs` walks
# `e2e/fixtures/invariants/index.json` with BOTH readers and needs cargo. This
# script walks the same index with the SECOND reader alone, so the corpus is
# still checked on a machine that has an auditor's python3 and no Rust
# toolchain — which is exactly the machine `docs/verify-a-scorecard.md` is
# written for. Task 5 inherits it unchanged: it adds cases to `index.json`, not
# to this file.
#
# Task 5c: it also walks `shape-index.json`, whose documents neither reader
# reaches an invariant on (a whole required block removed, or a mistyped
# `sample.records_expected` — Rust refuses those at DESERIALISATION). The
# TWO-READER claim on those is the cargo test's; the half checkable with the
# second reader alone is this script's, and leaving it out would mean the
# sentence above — "the corpus is still checked" — covered only part of the
# corpus. Both files reduce to the same two assertions, `python_exit` and the
# `INVALID: `-stripped reason, so they are walked by one loop; `shape-index`
# spells the fields `python_reason` because its Rust half is recorded
# separately and is not this script's business.
#
# Task 5d: it also checks that the shape corpus ACCOUNTS FOR EVERY REQUIRED
# BLOCK of `logweir_core::scorecard::Scorecard` before it walks anything. Task
# 5c's review deleted a shape check, its corpus case and its pytest in one edit
# and this gate stayed green, reporting one fewer case and no complaint. The
# arithmetic below is what makes that fail: the block list comes from the Rust
# struct, which the deletion does not touch.
#
# Task 5e: the same arithmetic now closes over TWO more lists that had none —
# the required NON-block fields of `Scorecard` (`REQUIRED_FIELDS`) and every
# field it types as `u64`, with its optionality (`U64_FIELDS`). Task 5d's review
# measured the second gap the same way: deleting one `U64_FIELDS` entry, its
# pytest case and `test_the_u64_field_list_matches_the_rust_struct` — all three
# inside `docs/` — left this gate at 0 and put `integrity.mismatches: 2**64`
# back to `drill verify` exit 1 against `VALID` from the script.
#
# Task 5b: it also walks `backup-receipt-index.json` — the BACKUP RECEIPT's own
# corpus — and closes the same kind of arithmetic over its four invariant arms.
# The fixed point there is `crates/logweir-core/src/backup_receipt.rs`'s
# `validate_invariants`: the gate re-derives each arm's message SKELETON (the
# format string with every `{...}` placeholder normalised) from that function's
# body, re-derives the same list from `docs/verify_scorecard.py::
# check_backup_receipt_invariants`, requires the two lists to be equal in
# order, and requires every case in the index to match exactly one skeleton and
# every skeleton to be matched by at least one case. Deleting an arm from ONE
# reader together with its corpus case and its pytest therefore cannot balance:
# the other reader still has four arms. Deleting it from BOTH is what
# `crates/logweir-core/tests/backup_receipt.rs::
# backup_receipt_invariants_have_exactly_four_arms` is for — the per-arm Rust
# unit test the corpus README already names as the only thing that survives a
# fully coordinated deletion.
#
# The corpus cases are UNSIGNED on disk (see e2e/fixtures/invariants/README.md).
# `verify_scorecard.py` checks the signature before it evaluates any invariant,
# so each case is signed here, at test time, into a temp dir with the checked-in
# throwaway fixture key. Nothing under e2e/fixtures/signed/ is written: ruling
# R-G reserves the single fixture re-mint to Task 2.
#
# Every exit code is captured into a variable on its own line. NEVER through a
# pipe: `cmd | grep` reports grep's status, and a verifier whose failure is
# invisible is worse than no verifier.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
CORPUS="$ROOT/e2e/fixtures/invariants"
VERIFIER="$ROOT/docs/verify_scorecard.py"

# Interpreter resolution: $LOGWEIR_PYTHON, then $LOGWEIR_E2E_PYTHON, then
# .e2e/venv/bin/python3, then python3 — the same names in the same order as
# scripts/check-verifier-parity.sh, e2e/tests/harness/mod.rs::python and
# crates/logweir/tests/two_reader_parity.rs::python. All four agree, so no two
# gates can check the two-reader claim against different second readers.
if [ -n "${LOGWEIR_PYTHON:-}" ]; then
    PY="$LOGWEIR_PYTHON"
elif [ -n "${LOGWEIR_E2E_PYTHON:-}" ]; then
    PY="$LOGWEIR_E2E_PYTHON"
elif [ -x "$ROOT/.e2e/venv/bin/python3" ]; then
    PY="$ROOT/.e2e/venv/bin/python3"
else
    PY="python3"
fi

fail() {
    echo "check-invariant-corpus: $*" >&2
    exit 1
}

# NOT skipped when `cryptography` is missing, for the same reason
# check-verifier-parity.sh does not skip: an unchecked parity claim with
# nothing saying it went unchecked is the defect this work exists to remove.
set +e
"$PY" -c 'import cryptography' >/dev/null 2>&1
probe_rc=$?
set -e
if [ "$probe_rc" -ne 0 ]; then
    fail "python3 with the 'cryptography' package is required (pip install cryptography); the invariant corpus cannot be signed without it. tried $PY — set \$LOGWEIR_PYTHON (or \$LOGWEIR_E2E_PYTHON, or create .e2e/venv) to point at an interpreter that has it"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# CLOSED ARITHMETIC FOR THE SHAPE CORPUS (Task 5d, from Task 5c's review finding
# F2), run BEFORE anything is signed, because a corpus that does not account for
# every required block is not worth walking.
#
# The same three lists the Rust walker `every_required_block_has_a_shape_corpus_
# case` compares, and for the same reason: the FIXED POINT is
# `crates/logweir-core/src/scorecard.rs`, so deleting a check from
# verify_scorecard.py together with its corpus case and its pytest — the
# coordinated deletion Task 5c's review got past every gate — no longer balances.
# Re-derived here from the source text rather than imported, exactly as PAE is
# below: a gate that asks the thing it is checking cannot fail.
set +e
"$PY" - "$ROOT" <<'PYEOF'
import json, pathlib, re, sys

root = pathlib.Path(sys.argv[1])
rust = (root / "crates/logweir-core/src/scorecard.rs").read_text()
py = (root / "docs/verify_scorecard.py").read_text()
corpus = root / "e2e/fixtures/invariants"

# Every type the file declares. A `Scorecard` field whose type is EXACTLY one of
# them is a block; a String, an i8, a Vec<..> or an Option<..> is not.
declared = set(re.findall(r"^pub struct ([A-Za-z0-9_]+)", rust, re.M))
body = rust.split("pub struct Scorecard {", 1)[1].split("\n}", 1)[0]

# `check_invariants`'s body with whole-line `#` comments removed, for the same
# reason `code_only` strips `//` from the Rust in the walker: a fragment that
# survives only because a comment quotes it is not a check that exists.
ci = py.split("def check_invariants(", 1)[1].split("\ndef ", 1)[0]
ci_code = "\n".join(l for l in ci.splitlines() if not l.strip().startswith("#"))

# EVERY REQUIRED FIELD of `Scorecard`, ATTRIBUTE-AWARE: a field is REQUIRED when
# it carries no `#[serde(default)]`. Attribute lines are read, not just field
# lines, so `#[schemars(...)]` is not mistaken for a default and the flag is
# cleared at every field. Hoisted above the block list by Task 5f — see the
# cross-check immediately below.
required, defaulted = [], False
for line in body.splitlines():
    code = line.strip()
    if not code or code.startswith("//"):
        continue
    if code.startswith("#"):
        if "serde(default" in code:
            defaulted = True
        continue
    m = re.match(r"^pub ([a-z0-9_]+): (.+),$", code)
    if m and not defaulted:
        required.append((m.group(1), m.group(2)))
    defaulted = False

# --- Task 5f (f): the block list is `#[serde(default)]`-aware ----------------
# Task 5e's review, finding F3: this gate's block list was a one-line rule with
# no attribute awareness, while the `required` loop immediately below it WAS
# attribute-aware — the two lists in one file reading one struct by rules that
# could disagree. Task 5e closed the Rust side (`required_non_block_fields`
# asserts every name `required_blocks` calls a block is also a required field);
# the shell gate had no equivalent, so marking a block `#[serde(default)]` failed
# the walker at 101 and passed here at 0.
#
# `blocks` is now derived from the attribute-aware list, and the one-line rule is
# kept as an INDEPENDENT second parser that must agree — the same shape as the
# Rust guard, and for the reason stated there: two parsers reading one struct by
# different rules must not disagree silently, or both lists weaken at once.
blocks = [n for n, t in required if t in declared]
block_lines = [m.group(1) for m in
               re.finditer(r"^    pub ([a-z0-9_]+): ([A-Za-z0-9_]+),$", body, re.M)
               if m.group(2) in declared]
if block_lines != blocks:
    defaulted_blocks = [n for n in block_lines if n not in blocks]
    raise SystemExit(
        "this gate's two block parsers disagree about Scorecard.\n"
        f"  by field type only            ({len(block_lines)}): {block_lines}\n"
        f"  by field type AND no default  ({len(blocks)}): {blocks}\n"
        f"  block field(s) carrying #[serde(default)]: {defaulted_blocks}\n"
        "A block that gained a `#[serde(default)]` is no longer required: serde "
        "synthesises it, a document may omit it, and REQUIRED_BLOCKS must not "
        "demand it. crates/logweir/tests/two_reader_parity.rs fails such an edit "
        "at `required_non_block_fields`; since Task 5f this gate fails it too.")

loop = re.search(r"^REQUIRED_BLOCKS = \(\n(.*?)^\)$", py, re.M | re.S)
if loop is None:
    raise SystemExit("docs/verify_scorecard.py has no REQUIRED_BLOCKS tuple")
named = re.findall(r'^    "([a-z0-9_]+)",$', loop.group(1), re.M)

if named != blocks:
    raise SystemExit(
        "docs/verify_scorecard.py's REQUIRED_BLOCKS and Scorecard disagree.\n"
        f"  python ({len(named)}): {named}\n  rust   ({len(blocks)}): {blocks}\n"
        "Every non-optional block field of the struct must be named by the "
        "block-presence loop, in the struct's own declaration order.")

shape = json.loads((corpus / "shape-index.json").read_text())
covered = [e["check"].split(":", 1)[1] for e in shape
           if e.get("check", "").startswith("block:")]
if sorted(covered) != sorted(blocks):
    raise SystemExit(
        "the shape corpus does not account for Scorecard's required blocks.\n"
        f"  required ({len(blocks)}): {blocks}\n  covered  ({len(covered)}): {covered}\n"
        "Every required block needs a shape-index.json case whose `check` is "
        '"block:<name>", and every such case needs a block.')

# --- Task 5e (a): the required NON-BLOCK fields, with their TYPES (Task 5f) ---
# A field is REQUIRED when it carries no `#[serde(default)]` (derived above); it
# is a BLOCK when its type is one of the structs declared in the same file. The
# complement is what `REQUIRED_FIELDS` must name, in the same order, and what the
# shape corpus must carry one `field:` case (absent) and one `type:` case (present
# but wrongly typed) for.
#
# THE TYPE IS DERIVED FROM THE RUST TYPE, not chosen. Byte-for-byte the same
# mapping as `crates/logweir/tests/two_reader_parity.rs::json_type_of`, keyed on
# the struct's own type text, so a required non-block field of an unmapped type
# fails loudly here rather than reaching the Python reader untyped. 1.7.0 checked
# presence only, and Task 5e's review measured what that cost at `78bf570`:
# `run_id: 42`, `phases: "x"` and `requested_at: 5` were each `drill verify`
# exit 1 against `VALID` from the script.
def json_type_of(name, ty):
    if ty in ("String", "DateTime<Utc>", "Outcome"):
        return "string"       # chrono writes RFC 3339; Outcome is a unit enum
    if ty in ("i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64"):
        return "integer"
    if ty == "bool":
        return "boolean"
    if ty.startswith("Vec<"):
        return "array"
    raise SystemExit(
        f"Scorecard's required non-block field {name!r} has Rust type {ty!r}, which "
        "this gate does not map to a JSON type. Add the mapping here AND in "
        "crates/logweir/tests/two_reader_parity.rs::json_type_of, and give "
        "docs/verify_scorecard.py's REQUIRED_FIELDS the matching entry — a field "
        "with no mapping is type-checked by guesswork or not at all.")

non_block = [[n, json_type_of(n, t)] for n, t in required if t not in declared]
if not non_block:
    raise SystemExit("Scorecard has no required non-block field; the parsing rule "
                     "in check-invariant-corpus.sh no longer matches the struct")
non_block_names = [n for n, _ in non_block]

fields = re.search(r"^REQUIRED_FIELDS = \(\n(.*?)^\)$", py, re.M | re.S)
if fields is None:
    raise SystemExit("docs/verify_scorecard.py has no REQUIRED_FIELDS tuple")
named_fields = [[n, t] for n, t in
                re.findall(r'^    \("([a-z0-9_]+)", "([a-z]+)"\),$',
                           fields.group(1), re.M)]
if named_fields != non_block:
    raise SystemExit(
        "docs/verify_scorecard.py's REQUIRED_FIELDS and Scorecard disagree.\n"
        f"  python ({len(named_fields)}): {named_fields}\n"
        f"  rust   ({len(non_block)}): {non_block}\n"
        "Every required field of the struct that is NOT a block must be named by "
        "the field loop, in the struct's own declaration order, with the JSON "
        "type its Rust type implies.")

# The loop must actually READ the type. Asserted on the comment-stripped body,
# so a fragment quoted in prose does not count as a check that exists.
for fragment in ("for name, want in REQUIRED_FIELDS:", "_JSON_TYPES[want]",
                 "_JSON_TYPE_WORDS[want]"):
    if fragment not in ci_code:
        raise SystemExit(
            f"docs/verify_scorecard.py's check_invariants no longer contains "
            f"{fragment!r}, so the required non-block fields are no longer "
            "TYPE-checked; every type: case below is a document it prints VALID "
            "over while drill verify exits 1.")

field_cases = [e["check"].split(":", 1)[1] for e in shape
               if e.get("check", "").startswith("field:")]
if sorted(field_cases) != sorted(non_block_names):
    raise SystemExit(
        "the shape corpus does not account for Scorecard's required non-block "
        f"fields.\n  required ({len(non_block_names)}): {non_block_names}\n"
        f"  covered  ({len(field_cases)}): {field_cases}\n"
        "Every required non-block field needs a shape-index.json case whose "
        '`check` is "field:<name>".')

type_cases = [e["check"].split(":", 1)[1] for e in shape
              if e.get("check", "").startswith("type:")]
if sorted(type_cases) != sorted(non_block_names):
    raise SystemExit(
        "the shape corpus does not account for the TYPE of Scorecard's required "
        f"non-block fields.\n  required ({len(non_block_names)}): {non_block_names}\n"
        f"  covered  ({len(type_cases)}): {type_cases}\n"
        "Every required non-block field needs a shape-index.json case whose "
        '`check` is "type:<name>": one document carrying that field at a JSON '
        "type its Rust type refuses.")

# --- Task 5e (b): the u64 fields, with their optionality ---------------------
# Walk Scorecard's own fields in declaration order; a field whose type is a
# struct declared here contributes that struct's u64 fields under the field's
# name, a Vec<T> of one under `<field>[]` and an Option<T> of one under
# `<field>`. Every u64 line in the file must be reached that way, counted a
# second and independent way off the raw text.
u64_of, current = {}, None
for line in rust.splitlines():
    m = re.match(r"^pub struct ([A-Za-z0-9_]+)", line)
    if m:
        current = m.group(1)
        u64_of.setdefault(current, [])
        continue
    if line == "}":
        current = None
        continue
    if current is None:
        continue
    m = re.match(r"^    pub ([a-z0-9_]+): (u64|Option<u64>),$", line)
    if m:
        u64_of[current].append((m.group(1), m.group(2) == "Option<u64>"))

rust_u64, reached = [], set()
for name, ty in [(n, t) for n, t in
                 re.findall(r"^    pub ([a-z0-9_]+): (.+),$", body, re.M)]:
    inner = re.fullmatch(r"(?:Vec|Option)<([A-Za-z0-9_]+)>", ty)
    if ty in declared:
        owner, prefix = ty, name
    elif inner and inner.group(1) in declared:
        owner = inner.group(1)
        prefix = f"{name}[]" if ty.startswith("Vec<") else name
    else:
        continue
    reached.add(owner)
    for field, optional in u64_of.get(owner, []):
        rust_u64.append([f"{prefix}.{field}", optional])

flat = len(re.findall(r"^    pub [a-z0-9_]+: (?:u64|Option<u64>),$", rust, re.M))
if len(rust_u64) != flat:
    unreached = sorted(s for s, f in u64_of.items() if f and s not in reached)
    raise SystemExit(
        f"crates/logweir-core/src/scorecard.rs declares {flat} u64 document field(s) "
        f"but only {len(rust_u64)} are reachable from Scorecard by a struct field, a "
        f"Vec<T> or an Option<T>. Unreached struct(s): {unreached}.")

u64_tuple = re.search(r"^U64_FIELDS = \(\n(.*?)^\)$", py, re.M | re.S)
if u64_tuple is None:
    raise SystemExit("docs/verify_scorecard.py has no U64_FIELDS tuple")
py_u64 = [[n, o == "True"] for n, o in
          re.findall(r'^    \("([A-Za-z0-9_.\[\]]+)", (True|False)\),$',
                     u64_tuple.group(1), re.M)]
if py_u64 != rust_u64:
    raise SystemExit(
        "docs/verify_scorecard.py's U64_FIELDS and Scorecard disagree.\n"
        f"  python ({len(py_u64)}): {py_u64}\n  rust   ({len(rust_u64)}): {rust_u64}\n"
        "Every u64 field of the document needs an entry, in the struct's "
        "declaration order, and the second element must be True exactly for "
        "Option<u64>. Python's int is unbounded, so a field with no entry has no "
        "domain check at all.")

# --- Task 5f (d): closed arithmetic for the optionality flag's USE -----------
# Task 5e gave U64_FIELDS closed arithmetic on its LIST. Its USE had none, and
# its review measured that in one edit: revert `if optional and value is None:`
# to `if value is None:` — every flag left present and correct — and delete the
# three null corpus cases and both null pytests, and the walker, this gate and
# pytest were all still 0, with `sample.records_restored: null` back to `drill
# verify` exit 1 against VALID from the script. The null cases are now COUNTED,
# against the struct's own plain-`u64` set, and the guard line is asserted.
plain_u64 = [n for n, o in rust_u64 if not o]
null_cases = [e["check"].split(":", 1)[1] for e in shape
              if e.get("check", "").startswith("null:")]
if sorted(null_cases) != sorted(plain_u64):
    raise SystemExit(
        "the shape corpus does not account for null on Scorecard's plain u64 "
        f"fields.\n  plain u64 ({len(plain_u64)}): {plain_u64}\n"
        f"  covered   ({len(null_cases)}): {null_cases}\n"
        "Every non-Option u64 field needs a shape-index.json case whose `check` "
        'is "null:<dotted name>": one document setting it to null, which '
        "serde_json refuses with `invalid type: null, expected u64`.")
accepting = [e["id"] for e in shape if e.get("check", "").startswith("null:")
             and (e["rust_exit"] == 0 or e["python_exit"] == 0)]
if accepting:
    raise SystemExit(
        f"null: case(s) recorded as an ACCEPT: {accepting}. A plain u64 set to "
        "null must be REFUSED by both readers; an accept closes the arithmetic "
        "while asserting the opposite of the claim.")
if "if optional and value is None:" not in ci_code:
    raise SystemExit(
        "docs/verify_scorecard.py's check_invariants no longer contains `if "
        "optional and value is None:`, so the Option<u64> flag is derived, "
        "compared and never read. Every null: case above is then a document it "
        "prints VALID over while drill verify exits 1.")

print(f"check-invariant-corpus: {len(blocks)} required blocks, "
      f"{len(blocks)} block-presence checks, {len(covered)} shape cases — closed")
print(f"check-invariant-corpus: {len(non_block)} required non-block fields, "
      f"{len(named_fields)} field-presence checks, {len(field_cases)} absent-field "
      f"shape cases, {len(type_cases)} wrong-type shape cases — closed")
print(f"check-invariant-corpus: {len(rust_u64)} u64 document fields "
      f"({sum(1 for _, o in rust_u64 if o)} Option<u64>), "
      f"{len(py_u64)} domain checks, {len(null_cases)} null shape cases for the "
      f"{len(plain_u64)} plain u64 — closed")
PYEOF
arith_rc=$?
set -e
if [ "$arith_rc" -ne 0 ]; then
    fail "the shape corpus arithmetic does not close (see above)"
fi

# CLOSED ARITHMETIC FOR THE BACKUP-RECEIPT CORPUS (Task 5b). Same shape as the
# block above and for the same reason, with one difference that decides the
# mechanism: the receipt's four arm messages INTERPOLATE, so an arm cannot be
# joined to a corpus case by literal substring the way `index.json`'s `arm`
# field is joined to `Scorecard::validate_invariants`. The join is the message
# SKELETON instead — the format string with every placeholder normalised to
# `{}` — read out of both readers' source text, which is what lets the gate
# compare the two readers' arm SETS as well as the corpus's coverage of them.
set +e
"$PY" - "$ROOT" <<'PYEOF'
import json, pathlib, re, sys

root = pathlib.Path(sys.argv[1])
corpus = root / "e2e/fixtures/invariants"
rust_src = (root / "crates/logweir-core/src/backup_receipt.rs").read_text()
py_src = (root / "docs/verify_scorecard.py").read_text()


def normalise(literal):
    """A message literal as a SKELETON: every `{...}` placeholder to `{}`.

    Rust's `{:?}`/`{}` and Python's `{value!r}`/`{name}` are the same hole in
    the same sentence, and the sentence is what the two readers must agree on.
    What the hole is FILLED with is compared separately and exactly, by the
    corpus walk below and by `crates/logweir/tests/two_reader_parity_receipt.rs`.
    """
    return re.sub(r"\{[^{}]*\}", "{}", literal)


def rust_body(text, signature):
    """From the line carrying `signature` to the first line that is exactly
    four spaces and a closing brace — the same slice
    `crates/logweir/tests/two_reader_parity.rs::validate_invariants_body` takes,
    and for the same reason: a `trim()` would close the body at the first inner
    brace."""
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if signature in l)
    end = next(i for i, l in enumerate(lines[start + 1:], start + 1) if l == "    }")
    return "\n".join(lines[start:end + 1])


def read_rust_literal(text, i):
    """The Rust string literal starting at `text[i] == '"'`, with `\`-newline
    CONTINUATIONS applied — rustc drops the newline and the following leading
    whitespace, so a gate that did not would compare a string with spaces in it
    against a refusal that never had them."""
    i += 1
    out = []
    escapes = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "'": "'"}
    while True:
        c = text[i]
        if c == '"':
            return "".join(out), i + 1
        if c == "\\":
            nxt = text[i + 1]
            if nxt == "\n":
                i += 2
                while i < len(text) and text[i] in " \t":
                    i += 1
                continue
            out.append(escapes.get(nxt, nxt))
            i += 2
            continue
        out.append(c)
        i += 1


def rust_arms(body):
    """One skeleton per `return Err(format!(` STATEMENT, in order."""
    arms, i = [], 0
    while True:
        j = body.find("return Err(format!(", i)
        if j < 0:
            return arms
        lit, i = read_rust_literal(body, body.index('"', j))
        arms.append(normalise(lit))


PY_LIT = re.compile(r'(?:f|r|rf|fr)?"((?:[^"\\]|\\.)*)"')


def py_arms(body):
    """One skeleton per `return` statement, in order, with Python's implicit
    concatenation applied and `return ""` (the ACCEPT) dropped."""
    lines = body.splitlines()
    arms, i = [], 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith("return ") and stripped != "return":
            chunk, depth = [], 0
            while i < len(lines):
                chunk.append(lines[i])
                depth += lines[i].count("(") - lines[i].count(")")
                i += 1
                if depth <= 0:
                    break
            pieces = [m.group(1) for m in PY_LIT.finditer("\n".join(chunk))]
            joined = normalise("".join(pieces).replace('\\"', '"'))
            if joined:
                arms.append(joined)
            continue
        i += 1
    return arms


rust = rust_arms(rust_body(rust_src, "pub fn validate_invariants"))
python = py_arms(
    py_src.split("def check_backup_receipt_invariants(", 1)[1].split("\ndef ", 1)[0])

if not rust:
    raise SystemExit(
        "crates/logweir-core/src/backup_receipt.rs's validate_invariants has no "
        "`return Err(format!(` statement; this gate is reading the wrong function or "
        "the arms no longer report a message, and either way the corpus below is "
        "walking documents nothing accounts for.")
if rust != python:
    raise SystemExit(
        "the two readers do not implement the same backup-receipt arms.\n"
        f"  rust   ({len(rust)}): {json.dumps(rust, indent=4)}\n"
        f"  python ({len(python)}): {json.dumps(python, indent=4)}\n"
        "Every arm of BackupReceipt::validate_invariants must have a mirrored arm in "
        "docs/verify_scorecard.py::check_backup_receipt_invariants, in the same order, "
        "with the same message and the same placeholders. This is the assertion that "
        "fails when an arm is deleted from ONE reader together with its corpus case "
        "and its pytest.")

entries = json.loads((corpus / "backup-receipt-index.json").read_text())
if not entries:
    raise SystemExit("backup-receipt-index.json is empty; a walk over nothing proves nothing")

SIX = {"id", "file", "rust_exit", "python_exit", "reason", "arm"}
for e in entries:
    if set(e) != SIX:
        raise SystemExit(
            f"backup-receipt-index.json entry {e.get('id')!r} has fields {sorted(e)}; "
            f"interface I31 fixes the shape at exactly {sorted(SIX)}")
    if e["arm"] != e["reason"]:
        raise SystemExit(
            f"{e['id']}: `arm` and `reason` differ. For this corpus the arm IS the "
            "refusal text the arm returns (the messages interpolate, so there is no "
            "shorter fragment to name), and the join below is on it.")


def matches(skeleton, message):
    """`message` is this skeleton's sentence with the holes filled."""
    parts = [re.escape(p) for p in skeleton.split("{}")]
    return re.fullmatch(".*?".join(parts), message, re.S) is not None


covered = {s: 0 for s in rust}
for e in entries:
    if not e["reason"]:
        continue  # the accept-control
    hits = [s for s in rust if matches(s, e["reason"])]
    if len(hits) != 1:
        raise SystemExit(
            f"{e['id']}: its recorded reason matches {len(hits)} of the {len(rust)} arms "
            "of BackupReceipt::validate_invariants; every refusing case must name exactly "
            f"one.\n  reason: {e['reason']!r}\n  arms:   {json.dumps(rust, indent=4)}")
    covered[hits[0]] += 1

missing = [s for s, n in covered.items() if n == 0]
if missing:
    raise SystemExit(
        "the backup-receipt corpus does not account for every invariant arm.\n"
        f"  arms ({len(rust)}): {json.dumps(rust, indent=4)}\n"
        f"  uncovered ({len(missing)}): {json.dumps(missing, indent=4)}\n"
        "Every arm needs at least one backup-receipt-index.json case whose `reason` is "
        "the message that arm returns. The arm list comes from "
        "crates/logweir-core/src/backup_receipt.rs, which a deletion inside docs/ and "
        "e2e/fixtures/ does not touch.")

if not any(not e["reason"] for e in entries):
    raise SystemExit(
        "backup-receipt-index.json has no ACCEPT case; without one, a reader that "
        "refused every receipt would walk this corpus green.")

# Ids key the temp files every gate signs into, so a duplicate across ANY of the
# three indexes makes one gate compare the wrong document against another's
# expectations. Checked across all three here, which is what makes adding a
# fourth safe.
ids = []
for name in ("index.json", "shape-index.json", "backup-receipt-index.json"):
    ids += [e["id"] for e in json.loads((corpus / name).read_text())]
dupes = sorted({i for i in ids if ids.count(i) > 1})
if dupes:
    raise SystemExit(f"the corpus indexes have duplicate id(s): {dupes}")

print(f"check-invariant-corpus: {len(rust)} backup-receipt invariant arms in both "
      f"readers, {len(entries) - 1} refusing corpus case(s) covering all of them, "
      f"{len(ids)} unique case ids across three indexes — closed")
PYEOF
receipt_arith_rc=$?
set -e
if [ "$receipt_arith_rc" -ne 0 ]; then
    fail "the backup-receipt corpus arithmetic does not close (see above)"
fi

# Sign every case into $tmp and emit one TAB-separated line per case:
#   id <TAB> python_exit <TAB> reason
# PAE is re-derived from the spec here rather than imported from the verifier,
# so a bug in the verifier's own `pae()` cannot make this check agree with
# itself.
"$PY" - "$CORPUS" "$tmp" > "$tmp/cases.tsv" <<'PYEOF'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

corpus, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
signing_pem = corpus.parent / "signed" / "signing.pem"
PT = "application/vnd.logweir.drill-scorecard+json;version=1.0.0"

key = serialization.load_pem_private_key(signing_pem.read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
keyid = hashlib.sha256(der).hexdigest()

entries = json.loads((corpus / "index.json").read_text())
if not entries:
    raise SystemExit("index.json is empty; a walk over nothing proves nothing")
# The shape cases, normalised onto the same two fields. `python_reason` rather
# than `reason` because those entries also record the RUST refusal, which is a
# different reader's text and no business of this script's.
shape = json.loads((corpus / "shape-index.json").read_text())
if not shape:
    raise SystemExit("shape-index.json is empty; a walk over nothing proves nothing")
for e in shape:
    entries.append({"id": e["id"], "file": e["file"],
                    "python_exit": e["python_exit"], "reason": e["python_reason"]})
# Every case is signed into ONE shared temp dir keyed by `id`, so a duplicate
# id would silently overwrite an earlier case's files and then compare the
# WRONG document against that earlier case's expected reason — a green walk
# over a case that was never run. Checked ACROSS both index files, which is
# what makes adding a third one safe. Task 5 adds cases, so this fails loudly
# rather than staying latent.
ids = [e["id"] for e in entries]
dupes = sorted({i for i in ids if ids.count(i) > 1})
if dupes:
    raise SystemExit(f"the corpus indexes have duplicate id(s): {dupes}; every id must be unique")
for e in entries:
    # The signed payload is the file's bytes EXACTLY as written. Nothing is
    # re-serialised, or the reader would verify different bytes.
    payload = (corpus / e["file"]).read_bytes()
    t = PT.encode()
    msg = (b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" "
           + str(len(payload)).encode() + b" " + payload)
    sig = key.sign(msg, ec.ECDSA(hashes.SHA256()))
    (out / f"{e['id']}.json").write_bytes(payload)
    (out / f"{e['id']}.sig").write_text(json.dumps(
        {"payloadType": PT,
         "signatures": [{"keyid": keyid, "sig": base64.b64encode(sig).decode()}]}))
    if "\t" in e["reason"] or "\n" in e["reason"]:
        raise SystemExit(f"{e['id']}: `reason` must be a single TAB-free line")
    print(f"{e['id']}\t{e['python_exit']}\t{e['reason']}")
PYEOF

count=0
while IFS=$'\t' read -r id want_py reason; do
    [ -n "$id" ] || continue
    count=$((count + 1))

    set +e
    "$PY" "$VERIFIER" "$tmp/$id.json" "$tmp/$id.sig" "$ROOT/e2e/fixtures/signed/public.pem" \
        >"$tmp/out" 2>"$tmp/err"
    py_rc=$?
    set -e

    if [ "$py_rc" -ne "$want_py" ]; then
        cat "$tmp/err" >&2
        fail "$id: verify_scorecard.py exited $py_rc, index.json expects $want_py"
    fi

    # The refusal text, with this reader's own `INVALID: ` prefix stripped.
    got=""
    while IFS= read -r line; do
        case "$line" in
            "INVALID: "*) got="${line#INVALID: }"; break ;;
        esac
    done < "$tmp/err"

    if [ "$got" != "$reason" ]; then
        fail "$id: the refusal text is not the one index.json records.
  got:  $got
  want: $reason"
    fi
    echo "check-invariant-corpus: $id  python=$py_rc  ok"
# A here-string, NOT `... | while`: a pipeline runs the loop body in a
# subshell, where `fail`'s `exit 1` aborts only that subshell and `just lint`
# goes green over a real mismatch.
done <<< "$(cat "$tmp/cases.tsv")"

if [ "$count" -eq 0 ]; then
    fail "walked zero cases; e2e/fixtures/invariants/index.json is empty or unreadable"
fi
echo "check-invariant-corpus: the auditor's verifier agrees with index.json + shape-index.json on all $count cases"

# ---------------------------------------------------------------------------
# THE BACKUP-RECEIPT CORPUS (Task 5b), with the second reader alone.
#
# A separate signing block and a separate loop rather than a widened one,
# because the payload type is different: a receipt signed under the SCORECARD
# media type would be refused by `--payload-type backup-receipt` at the
# payloadType comparison and this walk would pass for the wrong reason on every
# case. The two-reader claim on these documents is
# `crates/logweir/tests/two_reader_parity_receipt.rs`'s; this is the half an
# auditor with python3 and no Rust toolchain can run.
# ---------------------------------------------------------------------------
mkdir -p "$tmp/receipt"
"$PY" - "$CORPUS" "$tmp/receipt" > "$tmp/receipt-cases.tsv" <<'PYEOF'
import base64, hashlib, json, pathlib, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

corpus, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
signing_pem = corpus.parent / "signed" / "signing.pem"
# The receipt's own media type. Written out rather than imported from the
# verifier, exactly as PAE is: a gate that asks the thing it is checking cannot
# fail. `docs/test_verify_scorecard.py::
# test_the_four_payload_types_match_the_rust_constants` is what keeps this
# literal and `crates/logweir-verify/src/lib.rs` in step.
PT = "application/vnd.logweir.backup-receipt+json;version=1.0.0"

key = serialization.load_pem_private_key(signing_pem.read_bytes(), password=None)
der = key.public_key().public_bytes(
    serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
keyid = hashlib.sha256(der).hexdigest()

entries = json.loads((corpus / "backup-receipt-index.json").read_text())
if not entries:
    raise SystemExit("backup-receipt-index.json is empty; a walk over nothing proves nothing")
for e in entries:
    payload = (corpus / e["file"]).read_bytes()
    t = PT.encode()
    msg = (b"DSSEv1 " + str(len(t)).encode() + b" " + t + b" "
           + str(len(payload)).encode() + b" " + payload)
    sig = key.sign(msg, ec.ECDSA(hashes.SHA256()))
    (out / f"{e['id']}.json").write_bytes(payload)
    (out / f"{e['id']}.sig").write_text(json.dumps(
        {"payloadType": PT,
         "signatures": [{"keyid": keyid, "sig": base64.b64encode(sig).decode()}]}))
    if "\t" in e["reason"] or "\n" in e["reason"]:
        raise SystemExit(f"{e['id']}: `reason` must be a single TAB-free line")
    print(f"{e['id']}\t{e['python_exit']}\t{e['reason']}")
PYEOF

receipt_count=0
while IFS=$'\t' read -r id want_py reason; do
    [ -n "$id" ] || continue
    receipt_count=$((receipt_count + 1))

    set +e
    "$PY" "$VERIFIER" --payload-type backup-receipt \
        "$tmp/receipt/$id.json" "$tmp/receipt/$id.sig" \
        "$ROOT/e2e/fixtures/signed/public.pem" >"$tmp/out" 2>"$tmp/err"
    py_rc=$?
    set -e

    if [ "$py_rc" -ne "$want_py" ]; then
        cat "$tmp/err" >&2
        fail "$id: verify_scorecard.py exited $py_rc, backup-receipt-index.json expects $want_py"
    fi

    got=""
    while IFS= read -r line; do
        case "$line" in
            "INVALID: "*) got="${line#INVALID: }"; break ;;
        esac
    done < "$tmp/err"

    if [ "$got" != "$reason" ]; then
        fail "$id: the refusal text is not the one backup-receipt-index.json records.
  got:  $got
  want: $reason"
    fi
    echo "check-invariant-corpus: $id  python=$py_rc  ok  (backup receipt)"
done <<< "$(cat "$tmp/receipt-cases.tsv")"

if [ "$receipt_count" -eq 0 ]; then
    fail "walked zero backup-receipt cases; e2e/fixtures/invariants/backup-receipt-index.json is empty or unreadable"
fi
echo "check-invariant-corpus: the auditor's verifier agrees with backup-receipt-index.json on all $receipt_count cases"
