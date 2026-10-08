#!/usr/bin/env bash
# The `unsafe` perimeter (OD-6, decided 2026-10-05 as (a2); ADR 0004's
# amendment in docs/architecture.md; PROD-04.0 §7.4, the fence).
#
# Logweir calls the librdkafka functions the safe rdkafka API lacks through
# EXACTLY ONE crate, `crates/logweir-rdkafka-ffi`. It is the only place in the
# workspace where `unsafe` may appear, and only in its library source
# (`src/`). Every other crate keeps `#![forbid(unsafe_code)]`. This gate makes
# that sentence mechanical. RUSTC is the first layer: the root Cargo.toml's
# `[workspace.lints.rust] unsafe_code = "forbid"`, which every member but the
# perimeter takes with `[lints] workspace = true`, makes rustc refuse `unsafe`
# (and `no_mangle`, `export_name`, `link_section`, `global_asm!`) in EVERY
# target: lib, bin, example, integration test, bench, build script. This
# script checks that layer is in place and adds a text scan as the second:
#
#   1. The perimeter exists, is a workspace member, and is the package this
#      script names; nothing but `logweir-kafka` depends on it, and it depends
#      on nothing but `rdkafka` (so no second binding layer enters through it).
#   2. The lint layer: the root manifest carries `[workspace.lints.rust]` with
#      `unsafe_code = "forbid"`; every member but the perimeter carries
#      `[lints]` with `workspace = true`; the perimeter does not. And every lib,
#      bin and example target root of every OTHER package carries
#      `#![forbid(unsafe_code)]` as code (a commented-out one does not count).
#   3. The scan, over every `*.rs` file of the tree (pruned only at the root:
#      `target`, `.git`, `.engine`, `.e2e`, `.demo`, `node_modules`, `upstream`,
#      `third_party`), outside the perimeter's `src/`, after comments and
#      string, byte-string, raw-string and char literals are stripped (so the
#      API's "unsafe method" prose and a test's probe text do not count): any
#      `unsafe` token (a block, fn, impl, trait, extern, attribute, or one a
#      macro expands to); a foreign `extern { }` block; `no_mangle`,
#      `export_name` or `link_section` anywhere (inside `cfg_attr` too);
#      `global_asm!` or `naked_asm!`; an `allow`, `warn` or `expect` of
#      `unsafe_code`; any `include!` (it would pull text this scan never
#      reads); and a `#[path]` that points into the perimeter or out of the
#      tree (it would compile the perimeter's exempt source into another crate).
#   4. The perimeter's root keeps the lints that make its `unsafe` reviewable:
#      `deny(unsafe_op_in_unsafe_fn)` and `deny(clippy::undocumented_unsafe_blocks)`
#      (so `cargo clippy -D warnings` refuses an `unsafe` block without a
#      `// SAFETY:` comment), and it does NOT forbid `unsafe` (a perimeter that
#      forbids it is empty: delete the crate, ADR 0004's exit, and this line).
#   5. Every code-shaped `unsafe` inside the perimeter has a `// SAFETY:`
#      comment directly above it (attributes may sit between), and every
#      `unsafe fn` a `# Safety` doc section, at least SAFETY_MIN_CHARS long, so
#      it can name its obligations. Clippy enforces the comments' presence;
#      this check also runs where clippy does not.
#
# `crates/logweir/tests/unsafe_scope_gate.rs` is the negative control: it runs
# this script over throwaway overlays of the workspace with one planted
# violation each and requires red, naming the planted file.
#
# Usage: bash scripts/check-unsafe-scope.sh
# `LOGWEIR_ROOT` overrides the tree it walks (the overlay tests set it).
set -euo pipefail

ROOT="${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

PERIMETER_PACKAGE="logweir-rdkafka-ffi"
PERIMETER_DIR="crates/logweir-rdkafka-ffi"
# The ONLY packages that may depend on the perimeter.
PERIMETER_USERS="logweir-kafka"
# The ONLY packages the perimeter may depend on.
PERIMETER_DEPENDENCIES="rdkafka"
SAFETY_MIN_CHARS=40

for tool in cargo python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "FAIL: check-unsafe-scope.sh needs \`$tool\`" >&2
    exit 1
  fi
done

meta_file="$(mktemp)"
trap 'rm -f "$meta_file"' EXIT
# `--no-deps` resolves nothing and reaches no registry: it lists the members.
if ! cargo metadata --no-deps --format-version 1 >"$meta_file"; then
  echo "FAIL: cargo metadata failed over $ROOT" >&2
  exit 1
fi

python3 - "$meta_file" "$ROOT" "$PERIMETER_PACKAGE" "$PERIMETER_DIR" \
  "$PERIMETER_USERS" "$PERIMETER_DEPENDENCIES" "$SAFETY_MIN_CHARS" <<'PYEOF'
import json
import os
import re
import sys

meta_path, root, perim_pkg, perim_dir, users, perim_deps, safety_min = sys.argv[1:8]
root = os.path.realpath(root)
perim_abs = os.path.realpath(os.path.join(root, perim_dir))
perim_src = os.path.join(perim_abs, "src")
allowed_users = set(users.split())
allowed_perim_deps = set(perim_deps.split())
safety_min = int(safety_min)
failures = []


def fail(msg):
    failures.append(msg)
    print("FAIL: " + msg)


def rel(p):
    return os.path.relpath(os.path.realpath(p), root)


RAW_STRING = re.compile(r'(?:b|c)?r(#*)"')


def strip(src):
    """The source with every comment and the CONTENT of every string, byte
    string, raw string and char literal replaced by spaces. Newlines are kept,
    so line numbers survive; quote characters are kept, so `extern "C" {`
    still reads as `extern " " {`."""
    out = []
    i, n = 0, len(src)

    def blank(s):
        return "".join("\n" if c == "\n" else " " for c in s)

    while i < n:
        c = src[i]
        nxt = src[i + 1] if i + 1 < n else ""
        if c == "/" and nxt == "/":
            j = src.find("\n", i)
            j = n if j < 0 else j
            out.append(blank(src[i:j]))
            i = j
            continue
        if c == "/" and nxt == "*":
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif src.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            out.append(blank(src[i:j]))
            i = j
            continue
        # Raw strings: r"..", r#".."#, br"..", cr"..".
        m = RAW_STRING.match(src, i) if c in "bcr" else None
        if m and (i == 0 or not (src[i - 1].isalnum() or src[i - 1] == "_")):
            hashes = m.group(1)
            start = m.end()
            end = src.find('"' + hashes, start)
            end = n if end < 0 else end
            out.append(src[i:start] + blank(src[start:end]))
            i = end
            if i < n:
                out.append('"' + hashes)
                i += 1 + len(hashes)
            continue
        if c == '"' or (c in "bc" and nxt == '"' and (i == 0 or not (src[i - 1].isalnum() or src[i - 1] == "_"))):
            q = i if c == '"' else i + 1
            out.append(src[i:q + 1])
            j = q + 1
            while j < n and src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            out.append(blank(src[q + 1:j]))
            if j < n:
                out.append('"')
            i = j + 1
            continue
        if c == "'":
            # A char literal ('x', '\n', '\u{1F600}', '\''), else a lifetime or label.
            if nxt == "\\":
                j = i + 2
                while j < n and src[j] != "'":
                    j += 2 if src[j] == "\\" else 1
                out.append("'" + blank(src[i + 1:j]) + ("'" if j < n else ""))
                i = j + 1
                continue
            if i + 2 < n and src[i + 2] == "'":
                out.append("' '")
                i += 3
                continue
            out.append(c)
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


CODE_SHAPED = [
    (re.compile(r"\bunsafe\b"), "an `unsafe` token"),
    (re.compile(r'\bextern\s*(?:"\s*"\s*)?\{'), "a foreign `extern` block"),
    (re.compile(r"\b(no_mangle|export_name|link_section)\b"), "an `unsafe_code` attribute"),
    (re.compile(r"\b(global_asm|naked_asm)\s*!"), "module-level assembly"),
    (re.compile(r"\b(allow|warn|expect)\s*\(\s*(?:[^)]*,\s*)?unsafe_code\b"), "a relaxed `unsafe_code` lint"),
    (re.compile(r"\binclude\s*!"), "an `include!` (text this scan never reads)"),
]
PATH_ATTR = re.compile(r'#\s*\[\s*path\s*=\s*"')
FORBID = re.compile(r"#!\[\s*forbid\s*\(\s*(?:[\w:]+\s*,\s*)*unsafe_code\s*(?:,\s*[\w:]+\s*)*\)\s*\]")
DENY_OP = re.compile(r"#!\[\s*deny\s*\(\s*(?:[\w:]+\s*,\s*)*unsafe_op_in_unsafe_fn\b")
DENY_DOC = re.compile(r"#!\[\s*deny\s*\(\s*(?:[\w:]+\s*,\s*)*clippy::undocumented_unsafe_blocks\b")
ROOT_KINDS = {"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro", "bin", "example"}


def read(p):
    with open(p, encoding="utf-8", errors="replace") as f:
        return f.read()


def line_of(text, pos):
    return text.count("\n", 0, pos) + 1


meta = json.load(open(meta_path))
members = set(meta["workspace_members"])
packages = [p for p in meta["packages"] if p["id"] in members]

# ---- check 1: the perimeter, its users and its own dependencies
print("== 1. the perimeter is exactly one crate, reached only through logweir-kafka ==")
perim = [p for p in packages if p["name"] == perim_pkg]
if len(perim) != 1:
    fail(f"the perimeter package `{perim_pkg}` is not a workspace member ({len(perim)} found)")
elif os.path.realpath(os.path.dirname(perim[0]["manifest_path"])) != perim_abs:
    fail(f"`{perim_pkg}` does not live at {perim_dir}")
else:
    print(f"ok: `{perim_pkg}` is the workspace member at {perim_dir}")
    own = {d["name"] for d in perim[0]["dependencies"]}
    extra = sorted(own - allowed_perim_deps)
    if extra:
        fail(f"the perimeter depends on {extra}; it may depend on {sorted(allowed_perim_deps)} only")
    else:
        print(f"ok: the perimeter depends on {sorted(own)} only")
for p in packages:
    if p["name"] == perim_pkg:
        continue
    if any(d["name"] == perim_pkg for d in p["dependencies"]) and p["name"] not in allowed_users:
        fail(f"`{p['name']}` depends on the perimeter; only {sorted(allowed_users)} may")
print(f"ok: checked who depends on `{perim_pkg}`")

# ---- check 2 and 4: crate roots
print("== 2a. rustc forbids unsafe in every target: the workspace lint table and each member's opt-in ==")


def tables(text):
    """TOML tables of a manifest, as {header: [body lines]}, comments dropped.
    A line parser, not a TOML one: enough for `[x]` headers and `k = v` keys."""
    out, cur = {}, None
    for raw_line in text.split("\n"):
        line = raw_line.split("#", 1)[0].strip()
        m = re.fullmatch(r"\[\s*([^\[\]]+?)\s*\]", line)
        if m:
            cur = m.group(1).replace(" ", "")
            out.setdefault(cur, [])
        elif cur is not None and line:
            out[cur].append(line.replace(" ", ""))
    return out


root_tables = tables(read(os.path.join(root, "Cargo.toml")))
if 'unsafe_code="forbid"' not in root_tables.get("workspace.lints.rust", []):
    fail('Cargo.toml: no `[workspace.lints.rust]` table with `unsafe_code = "forbid"`')
else:
    print('ok: Cargo.toml forbids unsafe_code for the workspace')
for p in packages:
    t = tables(read(p["manifest_path"]))
    opted = "workspace=true" in t.get("lints", [])
    if p["name"] == perim_pkg:
        if opted:
            fail(f"{rel(p['manifest_path'])}: the perimeter takes the workspace lints, which forbid "
                 f"the `unsafe` it exists for")
    elif not opted:
        fail(f"{rel(p['manifest_path'])}: `{p['name']}` lacks `[lints] workspace = true`, so rustc "
             f"does not forbid unsafe in its tests, benches and build scripts")
print(f"ok: read {len(packages)} member manifests")

print("== 2. every root outside the perimeter carries #![forbid(unsafe_code)] ==")
roots = 0
for p in packages:
    for t in p["targets"]:
        if not ROOT_KINDS.intersection(t["kind"]):
            continue
        path = t["src_path"]
        code = strip(read(path))
        if p["name"] == perim_pkg:
            if FORBID.search(code):
                fail(f"{rel(path)}: the perimeter forbids `unsafe`, so it is empty: delete the crate "
                     f"(ADR 0004's exit) and this gate's PERIMETER lines")
            if "lib" in t["kind"]:
                if not DENY_OP.search(code):
                    fail(f"{rel(path)}: the perimeter's root lacks #![deny(unsafe_op_in_unsafe_fn)]")
                if not DENY_DOC.search(code):
                    fail(f"{rel(path)}: the perimeter's root lacks "
                         f"#![deny(clippy::undocumented_unsafe_blocks)]")
            continue
        roots += 1
        if not FORBID.search(code):
            fail(f"{rel(path)}: {'/'.join(t['kind'])} target root of `{p['name']}` without "
                 f"#![forbid(unsafe_code)]")
print(f"ok: read {roots} target roots outside the perimeter")
print("== 4. the perimeter's root keeps its unsafe lints (reported above when missing) ==")

# ---- check 3 and 5: the code-shaped scan
print("== 3. no code-shaped unsafe outside the perimeter's src/; 5. SAFETY above each inside ==")
PRUNE = {"target", ".git", ".engine", ".e2e", ".demo", "node_modules", "upstream", "third_party"}
scanned = inside = 0
for dirpath, dirnames, filenames in os.walk(root):
    # Pruned at the ROOT only: a `tests/upstream/mod.rs` is compiled like any
    # other module, so a directory's name never hides it.
    at_root = os.path.realpath(dirpath) == root
    dirnames[:] = sorted(d for d in dirnames if not (at_root and d in PRUNE))
    for name in sorted(filenames):
        if not name.endswith(".rs"):
            continue
        path = os.path.join(dirpath, name)
        real = os.path.realpath(path)
        text = read(path)
        code = strip(text)
        in_perimeter = real.startswith(perim_src + os.sep)
        if not in_perimeter:
            scanned += 1
            for rx, what in CODE_SHAPED:
                for m in rx.finditer(code):
                    fail(f"{rel(path)}:{line_of(code, m.start())}: {what} outside the perimeter "
                         f"({perim_dir}/src)")
            # `#[path = "..."]`: the literal is read from the ORIGINAL text at
            # the stripped match's offsets (stripping keeps every offset).
            for m in PATH_ATTR.finditer(code):
                start = m.end()
                end = text.find('"', start)
                target = os.path.realpath(os.path.join(os.path.dirname(path), text[start:end]))
                if target.startswith(perim_abs + os.sep) or not target.startswith(root + os.sep):
                    fail(f"{rel(path)}:{line_of(code, m.start())}: a `#[path]` into the perimeter or "
                         f"out of the tree ({text[start:end]})")
            continue
        inside += 1
        lines = text.split("\n")
        for m in re.finditer(r"\bunsafe\b\s*(\{|impl\b|fn\b|trait\b|extern\b)", code):
            ln = line_of(code, m.start())
            k = ln - 2  # the line above, 0-based
            while k >= 0 and lines[k].strip().startswith("#["):
                k -= 1
            block = []
            while k >= 0 and lines[k].strip().startswith("//"):
                block.insert(0, lines[k].strip())
                k -= 1
            joined = " ".join(block)
            # An `unsafe fn` states its CALLER's obligations in a `# Safety`
            # doc section (clippy's `missing_safety_doc` convention); every
            # other `unsafe` states why its own obligations hold.
            marker = "# Safety" if m.group(1) == "fn" else "SAFETY:"
            at = joined.find(marker)
            if at < 0:
                fail(f"{rel(path)}:{ln}: `unsafe {m.group(1)}` without a `{marker}` comment "
                     f"directly above it")
            elif len(joined) - at - len(marker) < safety_min:
                fail(f"{rel(path)}:{ln}: the `{marker}` comment is too short to name its "
                     f"obligations (< {safety_min} characters)")
print(f"ok: scanned {scanned} files outside the perimeter's src/ and {inside} inside it")

if failures:
    print(f"check-unsafe-scope: {len(failures)} violation(s)")
    sys.exit(1)
print("check-unsafe-scope: the perimeter holds")
PYEOF
