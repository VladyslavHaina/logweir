#!/usr/bin/env bash
# THE THIRD-PARTY INVENTORY GENERATOR — Task 29, interface I30.
#
# WHAT IT PRODUCES: `THIRD_PARTY_NOTICES.md`, one entry per package in the
# resolved dependency graph, each carrying the package's name, its version, the
# SPDX expression its own manifest declares, and a copyright line resolved by
# the three-arm rule below with the arm it used NAMED in the entry.
#
# WHY IT EXISTS AT ALL. SPDX cleanliness is a PERMISSION check and `deny.toml`
# does that half: every package in this graph offers a permissive licence and
# nothing blocks Apache-2.0. It is not the whole obligation. MIT, BSD-2-Clause,
# BSD-3-Clause and Apache-2.0 each additionally require the COPYRIGHT NOTICE to
# travel with the redistributed binary, and Logweir redistributes a statically
# linked binary in two container images and in release tarballs
# (`docs/research/R10-naming-trademark-license.md:141-142` states the
# obligation for exactly this case and names this mechanism). Global Constraint
# 15 binds it. So this file is not decoration: it is the artefact that
# discharges the attribution half.
#
# RUN IT:
#
#     bash scripts/gen-third-party-notices.sh > /tmp/tpn.md; echo "rc=$?"
#     diff -u THIRD_PARTY_NOTICES.md /tmp/tpn.md; echo "rc=$?"
#
# or, to rewrite the checked-in file in place:
#
#     bash scripts/gen-third-party-notices.sh --write; echo "rc=$?"
#
# THE CHECKED-IN FILE IS THIS SCRIPT'S OUTPUT AND NOTHING ELSE.
# `crates/logweir/tests/doc_lint.rs::third_party_notices_covers_every_resolved_package`
# re-derives the expected package set from `Cargo.lock` and fails naming any
# `name@version` the file has lost, so a hand edit is caught by a test that
# reads the lockfile rather than the document.
#
# ---------------------------------------------------------------------------
# `--offline` IS LOAD-BEARING — STANDING RULE 7, GLOBAL CONSTRAINT 17
# ---------------------------------------------------------------------------
# This script is run by a developer and by Task 30b's release job. It reaches
# no network: `cargo metadata --format-version 1 --offline` resolves against
# the committed `Cargo.lock` and the local registry cache and FAILS rather
# than fetching. A licence inventory that could silently fetch is an inventory
# that could describe a graph other than the one that was built.
#
# ---------------------------------------------------------------------------
# NO CRATE AND NO MANIFEST ENTRY IS ADDED ANYWHERE — GLOBAL CONSTRAINT 38
# ---------------------------------------------------------------------------
# `cargo-about` and `cargo-deny`'s own inventory mode would each be a new tool
# in the graph. This is `cargo metadata` piped into a `python3` reader, the
# idiom `scripts/check-one-signer.sh` already establishes for walking cargo's
# JSON; `xtask`'s `[dependencies]` block is empty today and stays empty.
#
# ---------------------------------------------------------------------------
# THE INTERPRETER IS RESOLVED THE WAY THE PARITY GATE RESOLVES IT
# ---------------------------------------------------------------------------
# `$LOGWEIR_PYTHON`, then `$LOGWEIR_E2E_PYTHON`, then the repository-local
# `.e2e/venv/bin/python3`, then whatever `python3` is on PATH — the same four,
# in the same order, as `scripts/check-verifier-parity.sh:51-56`. Nothing here
# needs `cryptography`; the order is kept identical so that a worktree
# configured for one gate is configured for all of them.
#
# COSTS NOTHING AND REACHES NOTHING: no network, no Docker, no `.engine/`, and
# it builds not one object file.
set -euo pipefail

ROOT="${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

WRITE=0
case "${1:-}" in
  --write) WRITE=1 ;;
  "") ;;
  *)
    echo "usage: scripts/gen-third-party-notices.sh [--write]" >&2
    echo "  no argument: write the inventory to stdout" >&2
    echo "  --write:     overwrite THIRD_PARTY_NOTICES.md in place" >&2
    exit 2
    ;;
esac

# The pinned toolchain, exported rather than assumed — `scripts/check-one-signer.sh`
# gives the reason: a `cargo` reached through a different default toolchain can
# resolve a different graph.
if [ -z "${RUSTUP_TOOLCHAIN:-}" ] && [ -f rust-toolchain.toml ]; then
  pinned="$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' rust-toolchain.toml | head -1)"
  if [ -n "$pinned" ]; then
    export RUSTUP_TOOLCHAIN="$pinned"
  fi
fi

if [ -n "${LOGWEIR_PYTHON:-}" ]; then
  PY="$LOGWEIR_PYTHON"
elif [ -n "${LOGWEIR_E2E_PYTHON:-}" ]; then
  PY="$LOGWEIR_E2E_PYTHON"
elif [ -x "$ROOT/.e2e/venv/bin/python3" ]; then
  PY="$ROOT/.e2e/venv/bin/python3"
else
  PY="python3"
fi

# REFUSE, naming the missing tool, rather than falling through: a generator
# that skips and still exits 0 produces an empty inventory that looks complete.
if ! command -v cargo >/dev/null 2>&1; then
  echo "gen-third-party-notices: REFUSING to run — \`cargo\` is not on PATH." >&2
  exit 1
fi
if ! command -v "$PY" >/dev/null 2>&1 && [ ! -x "$PY" ]; then
  echo "gen-third-party-notices: REFUSING to run — no python3 at \`$PY\`." >&2
  echo "  Set \$LOGWEIR_PYTHON (or \$LOGWEIR_E2E_PYTHON) to an interpreter." >&2
  exit 1
fi

meta_file="$(mktemp)"
out_file="$(mktemp)"
trap 'rm -f "$meta_file" "$out_file"' EXIT

# The exit status is read on its own line and never through a pipe (STANDING
# RULE 20): a `cargo metadata` that failed would otherwise become an empty
# graph and an inventory naming nothing.
cargo metadata --format-version 1 --offline > "$meta_file"

"$PY" - "$meta_file" > "$out_file" <<'PYEOF'
"""Render THIRD_PARTY_NOTICES.md from `cargo metadata --offline` JSON.

THE THREE-ARM COPYRIGHT RULE, and why it has three arms.

The copyright line a redistributed binary owes is not a field cargo carries.
It was MEASURED across this graph rather than assumed, and three sources
between them cover it:

  1. the licence files beside the crate's own `Cargo.toml` — `LICENSE*`,
     `LICENCE*`, `COPYRIGHT*` or `NOTICE*` — and EVERY holder notice they
     carry, not the first one. These are the real notices and they are always
     preferred.
  2. failing that, the `authors` field of the crate's manifest. It is not a
     copyright statement and is not presented as one: the entry says which
     arm it came from.
  3. failing both, the STATED FACT that the published crate carries neither.

ARM 1 IS PLURAL, AND THAT IS THE POINT. The obligation is to reproduce the
notices, and a file that carries four holders owes four lines. An earlier
draft returned the first matching line of the first matching file, which
attributed `ring` to "The Go Authors" (its `LICENSE-BoringSSL` sorts before
`LICENSE-other-bits`, where Brian Smith is) and dropped nine of the eleven
notices `aws-lc-sys` ships. Forty-one of the arm-1 entries come from a file
with more than one holder in it. So: every licence-family file, in sorted
order; every matching line, in file order; exact duplicates removed after
whitespace normalisation; one `- Copyright:` line per notice.

A crate that VENDORS code carries the vendored code's notices too, and they
appear here beside the crate author's own — `unicode-ident` ships the Unicode
data licence, `atomic-waker` and `parking` ship a `LICENSE-THIRD-PARTY`. That
is not noise: those are notices the binary redistributes.

Arm 3 exists because the alternative was measured and rejected: a generator
that emits an empty copyright field for the packages that reach it produces a
file that LOOKS complete and is not, and the reader of a licence inventory has
no way to tell "nothing is owed here" from "the tool found nothing". So the
third arm writes what is true — `no copyright statement in the published
crate; SPDX <expr> applies` — and
`doc_lint.rs::third_party_notices_covers_every_resolved_package` asserts every
entry's copyright field is non-empty, which is exactly the assertion an empty
third arm fails.

Workspace members are their own arm and take `NOTICE:2` byte for byte.

DETERMINISM. Entries are sorted by `(name, version)`; licence files beside a
manifest are read in sorted order and their matching lines in file order; no
timestamp, no host path and no absolute path is ever emitted. Two runs on two
machines with the same `Cargo.lock` produce the same bytes, which is what
makes the `diff -u` acceptance line mean something.
"""

import json
import os
import re
import sys

# A COPYRIGHT LINE, AND NOT A SENTENCE OF LICENCE BODY TEXT.
#
# `^\s*Copyright` alone is not enough, and the first draft of this script
# proved it by producing a file full of false attributions: matched
# case-insensitively it hit Apache-2.0 section 4(c)'s own wrapped body line
# ("copyright notice that is included in or attached to the work"), so `serde`,
# `ryu` and every `windows-*` crate were attributed to a fragment of the
# licence they ship. A licence inventory that attributes a crate to a sentence
# out of a licence file is worse than one that says nothing, because it reads
# as an answer.
#
# So: case-SENSITIVE, and either of two shapes.
#
# SHAPE 1 — the marked notice. A `(c)` / `(C)` / `©` marker or a four-digit
# year immediately after the word, with only punctuation between. This branch
# is byte-for-byte the rule that fixed the false attributions above, and it is
# unchanged: whatever it accepted before, it accepts now, and whatever it
# rejected before — Apache-2.0's body text, its `Copyright [yyyy] [name of
# copyright owner]` appendix placeholder — it still rejects.
MARKED_RE = re.compile(r"^\s*Copyright\b[^A-Za-z0-9]*(?:\(c\)|\(C\)|©|\d{4})")

# SHAPE 2 — the UNMARKED notice, which is still a notice. `aws-lc-sys`'s
# `LICENSE:9` is `Copyright Amazon.com, Inc. or its affiliates. All Rights
# Reserved.` and `utf8_iter`'s `COPYRIGHT:1` is `Copyright Mozilla Foundation`
# — no `(c)`, no year, and both are the crate's real and only notice. Shape 1
# alone attributed `aws-lc-sys` to Google and told the reader `aws-lc-rs` and
# `utf8_iter` had no licence-file notice at all.
#
# The risk this branch creates is the one shape 1 exists to prevent — matching
# a sentence of licence body text — so it is deliberately narrow. What follows
# `Copyright` must LOOK LIKE A HOLDER:
#
#   * it starts with an upper-case letter. A template placeholder opens with
#     `[`, `{` or `<` (`[yyyy]`, `{yyyy}`, `<year>`, `<copyright holders>`,
#     `<owner>`) and body text opens lower-case, so both are out on this
#     clause alone. Note `jsonpath-rust` and `simd-adler32` ship a FILLED-IN
#     bracket form (`Copyright (c) [2021] [Boris Zhguchev]`) — those carry a
#     marker and never reach this branch, which is why brackets are not
#     banned outright.
#   * its first word is not one of the words a notice never opens with but a
#     heading or a sentence does — `Notice`, `Holder`, `Year`, `Information`…
#   * it contains no placeholder token anywhere, in any of the three bracket
#     spellings.
#   * it is not Apache-2.0 section 4(c)'s sentence, matched on its own words.
#   * it is not a paragraph. A holder line is a holder line; 200 characters is
#     already four times the longest real one in this graph.
#
# MEASURED, not assumed: across all 392 packages there are exactly five
# distinct lines matching `^\s*Copyright` that shape 1 rejects — the two
# Apache placeholder spellings, which stay rejected, and the three real
# notices above, which this branch admits. Nothing else changes hands.
UNMARKED_RE = re.compile(r"^\s*Copyright\s+(?P<rest>\S.*)$")
PLACEHOLDER_RE = re.compile(
    r"[\[{<]\s*(?:yyyy|year|name\b|copyright|owner|holder|fullname|author|your\b)",
    re.IGNORECASE,
)
APACHE_BODY_RE = re.compile(r"notice[s]? that (?:is|are) included in or attached to", re.IGNORECASE)
NOT_A_HOLDER_FIRST_WORD = frozenset(
    (
        "notice", "notices", "holder", "holders", "owner", "owners",
        "year", "years", "date", "dates", "statement", "statements",
        "header", "headers", "line", "lines", "information", "notation",
        "and", "or", "shall", "may", "must", "is", "are", "as", "by",
        "for", "in", "to", "of", "that", "this", "these", "those", "it",
        "its", "you", "your", "we", "our", "if", "when", "where", "which",
    )
)


def is_copyright_notice(line):
    """True for a real holder notice; false for licence body text and templates."""
    if MARKED_RE.match(line):
        return True
    match = UNMARKED_RE.match(line)
    if not match:
        return False
    rest = match.group("rest").strip()
    if len(rest) > 200:
        return False
    if not rest[:1].isupper():
        return False
    if PLACEHOLDER_RE.search(rest):
        return False
    if APACHE_BODY_RE.search(rest):
        return False
    first = re.sub(r"[^A-Za-z]", "", rest.split()[0]).lower()
    return first not in NOT_A_HOLDER_FIRST_WORD


# A NOTICE THAT NAMES NO HOLDER IS STILL THE NOTICE. `either`'s LICENSE-MIT is
# literally `Copyright (c) 2015` and `indexmap`'s is `Copyright (c) 2016--2017`
# — no holder, in the published crate, upstream's own text. An earlier draft
# required a name to follow and dropped both to arm 3, which then asserted
# "no copyright statement in the published crate" about crates that carry one.
# The obligation is to reproduce the notice, not to improve it.
LICENCE_FILE_PREFIXES = ("license", "licence", "copyright", "notice")

# Byte-identical to NOTICE:2. Not a coincidence and not to be "improved" in one
# place only: `doc_lint.rs` reads the line out of NOTICE and compares.
WORKSPACE_COPYRIGHT = "Copyright 2026 The Logweir Authors"

ARM_LICENCE_FILE = "licence file"
ARM_AUTHORS = "`authors` field"
ARM_NONE = "neither; the fact is stated"
ARM_WORKSPACE = "this workspace"


def clean(text):
    """One line, no control characters, safe inside a Markdown list item."""
    text = text.replace("\t", " ").strip()
    text = re.sub(r"\s+", " ", text)
    # Markdown emphasis and code spans would otherwise reflow a copyright line
    # that happens to contain them. Backslash-escaping is enough: the reader
    # sees the original characters.
    return text.replace("\\", "\\\\").replace("`", "\\`").replace("*", "\\*").replace("_", "\\_")


def copyrights_from_licence_files(manifest_path):
    """EVERY holder notice beside the manifest, in a stable order.

    Files in sorted order, lines in file order, exact duplicates removed after
    whitespace normalisation — a crate that ships the same notice in
    `LICENSE-MIT` and `LICENSE-APACHE` owes one line, not two. Returns a list,
    empty when the crate carries no notice at all, which is arm 2's cue.
    """
    directory = os.path.dirname(manifest_path)
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return []
    notices = []
    seen = set()
    for name in names:
        if not name.lower().startswith(LICENCE_FILE_PREFIXES):
            continue
        candidate = os.path.join(directory, name)
        if not os.path.isfile(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if not is_copyright_notice(line):
                        continue
                    normalised = re.sub(r"\s+", " ", line.strip())
                    if normalised in seen:
                        continue
                    seen.add(normalised)
                    notices.append(normalised)
        except OSError:
            continue
    return notices


def main():
    meta = json.load(open(sys.argv[1], encoding="utf-8"))
    workspace = set(meta.get("workspace_members", []))

    entries = []
    tally = {ARM_LICENCE_FILE: 0, ARM_AUTHORS: 0, ARM_NONE: 0, ARM_WORKSPACE: 0}

    for package in meta["packages"]:
        name = package["name"]
        version = package["version"]
        spdx = package.get("license")
        if not spdx:
            licence_file = package.get("license_file")
            if licence_file:
                spdx = "NOT-SPDX: the crate ships `%s` instead of a licence expression" % licence_file
            else:
                spdx = "NOT-SPDX: the published crate declares neither `license` nor `license_file`"

        if package["id"] in workspace:
            lines, arm = [WORKSPACE_COPYRIGHT], ARM_WORKSPACE
        else:
            lines = copyrights_from_licence_files(package["manifest_path"])
            if lines:
                arm = ARM_LICENCE_FILE
            else:
                authors = [a for a in package.get("authors") or [] if a.strip()]
                if authors:
                    lines, arm = [", ".join(authors)], ARM_AUTHORS
                else:
                    lines = [
                        "no copyright statement in the published crate; SPDX %s applies" % spdx
                    ]
                    arm = ARM_NONE

        tally[arm] += 1
        entries.append((name, version, spdx, lines, arm))

    entries.sort(key=lambda e: (e[0], e[1]))

    out = sys.stdout.write
    out("# Third-party notices\n\n")
    out(
        "**Generated. Regenerated, never edited.** Every entry below is produced by\n"
        "`scripts/gen-third-party-notices.sh`, which runs\n"
        "`cargo metadata --format-version 1 --offline` against the committed `Cargo.lock`\n"
        "and reaches no network. To refresh it:\n\n"
    )
    out("```bash\nbash scripts/gen-third-party-notices.sh --write\n```\n\n")
    out(
        "`crates/logweir/tests/doc_lint.rs::third_party_notices_covers_every_resolved_package`\n"
        "re-derives the expected set from `Cargo.lock` and fails naming any `name@version`\n"
        "this file has lost, so a hand edit is caught by a test that reads the lockfile\n"
        "rather than this document.\n\n"
    )
    out("**Packages in the resolved graph: %d.**\n\n" % len(entries))
    out(
        "## What this file is, and what `deny.toml` is\n\n"
        "SPDX cleanliness is a **permission** check — may Logweir ship under Apache-2.0 at\n"
        "all — and `cargo deny check licenses --offline` answers it. This file answers the\n"
        "other half: MIT, BSD-2-Clause, BSD-3-Clause and Apache-2.0 each require the\n"
        "**copyright notice to travel with the redistributed binary**, and Logweir\n"
        "redistributes a statically linked binary in two container images and in release\n"
        "tarballs. Global Constraint 15; `docs/research/R10-naming-trademark-license.md`\n"
        "states the obligation for this exact case.\n\n"
        "It is **not** the whole of the attribution Logweir owes. The C code statically\n"
        "linked through `rdkafka-sys`'s `cmake-build` feature — librdkafka, its fourteen\n"
        "vendored components, and OpenSSL — is invisible to `cargo metadata` and is\n"
        "attributed in [NOTICE](NOTICE) instead.\n\n"
        "## The copyright line has three sources, and each entry names the one it used\n\n"
        "1. **%s** — the `LICENSE*`, `LICENCE*`, `COPYRIGHT*` and `NOTICE*` files beside\n"
        "   the crate's own `Cargo.toml`, and **every** holder notice they carry, in\n"
        "   file-name order. An entry with four holders prints four `Copyright` lines:\n"
        "   the obligation is plural, and a generator that kept only the first\n"
        "   attributed `ring` to \"The Go Authors\" and lost Brian Smith altogether.\n"
        "   Where a crate vendors code, the vendored code's holders appear here beside\n"
        "   the crate author's own \u2014 they are notices the binary redistributes.\n"
        "   A line counts as a notice when it begins `Copyright` (case-SENSITIVELY) and\n"
        "   **either** carries a `(c)`, `(C)`, `\u00a9` or four-digit year, **or** is\n"
        "   followed directly by something shaped like a holder. Both halves earn their\n"
        "   keep. Without the first, the match hits Apache-2.0's own body text and its\n"
        "   `Copyright [yyyy] [name of copyright owner]` placeholder, and every crate\n"
        "   shipping `LICENSE-APACHE` is attributed to a fragment of the licence it\n"
        "   ships. Without the second, `Copyright Amazon.com, Inc. or its affiliates.`\n"
        "   is not a copyright notice, `aws-lc-sys` is attributed to Google, and\n"
        "   `aws-lc-rs` and `utf8_iter` are reported as carrying no notice at all.\n"
        "2. **%s** — the manifest's `authors`, used only when arm 1 finds nothing. An\n"
        "   author is not a copyright holder; the entry says which arm it used so that\n"
        "   the difference is visible rather than implied.\n"
        "3. **%s** — the published crate carries neither, so the entry says so in\n"
        "   words, with the SPDX expression that governs it regardless. A generator that\n"
        "   emitted an empty field here would produce a file that looks complete and is\n"
        "   not: a reader could not tell \"nothing is owed\" from \"the tool found\n"
        "   nothing\".\n"
        "4. **%s** — Logweir's own crates, taking [NOTICE](NOTICE) line 2 byte for byte.\n\n"
        % (ARM_LICENCE_FILE, ARM_AUTHORS, ARM_NONE, ARM_WORKSPACE)
    )
    out("| arm | entries |\n|---|---|\n")
    for arm in (ARM_LICENCE_FILE, ARM_AUTHORS, ARM_NONE, ARM_WORKSPACE):
        out("| %s | %d |\n" % (arm, tally[arm]))
    # NOT A CARGO PACKAGE, AND STILL OWED (PLAT-18.2). The console's design
    # tokens are VMware Clarity's names and values, copied into `ui/style.css`.
    # `cargo metadata` cannot see a stylesheet, so this section is written
    # here by hand, once, and regenerated with everything else.
    out(
        "\n## Assets copied into the console (not Cargo packages)\n\n"
        "**VMware Clarity design tokens (vmware-clarity/core v6.17.0).** "
        "`ui/style.css` section 1 copies the **names and values** of VMware Clarity's\n"
        "design tokens: the global space, typography, animation and colour-palette\n"
        "entries the console uses, and the light and dark alias mappings. Source:\n"
        "`https://github.com/vmware-clarity/core`, tag `v6.17.0`, commit\n"
        "`24e56ba2a337f8735873c379496eae2692679ed5` (2026-02-02), files\n"
        "`projects/core/build/tokens.ts` (global tokens and light aliases) and\n"
        "`projects/core/src/styles/theme.dark.scss` (dark aliases). No Clarity code,\n"
        "font or icon is copied; the page loads nothing from Clarity at run time.\n"
        "Licence: MIT (the repository's `LICENSE`); Clarity's fonts are OFL-1.1 and\n"
        "are **not** used.\n\n"
        "```text\n"
        "MIT License\n\n"
        "Copyright (c) 2016-2021 VMware, Inc.  All rights reserved.\n\n"
        "Permission is hereby granted, free of charge, to any person obtaining a copy\n"
        "of this software and associated documentation files (the \"Software\"), to deal\n"
        "in the Software without restriction, including without limitation the rights\n"
        "to use, copy, modify, merge, publish, distribute, sublicense, and/or sell\n"
        "copies of the Software, and to permit persons to whom the Software is\n"
        "furnished to do so, subject to the following conditions:\n\n"
        "The above copyright notice and this permission notice shall be included in all\n"
        "copies or substantial portions of the Software.\n\n"
        "THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR\n"
        "IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,\n"
        "FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE\n"
        "AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER\n"
        "LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,\n"
        "OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE\n"
        "SOFTWARE.\n"
        "```\n"
    )
    # NOT A CARGO PACKAGE, AND NOT IN ANY LOGWEIR IMAGE. The MinIO mirror is a
    # separate image the project publishes for its compose stack, the chart's
    # demo MinIO and the live harnesses; it is listed because the project
    # redistributes it. Written here by hand, like the section above.
    out(
        "\n## Images the project redistributes for its test and demo stacks (not in Logweir's images)\n\n"
        "**MinIO server and client, rebuilt from upstream source.** MinIO withdrew its public\n"
        "images (Docker Hub on 2026-09-11, anonymous quay.io pulls on 2026-09-24), so the compose\n"
        "stack, the chart's optional demo MinIO (`minio.enabled`), the PoC grants Job and the live\n"
        "harnesses run the project's unmodified rebuilds of the releases they were measured on,\n"
        "`docker.io/vladyslavhaina/minio-mirror:RELEASE.2025-09-07T16-13-09Z` and\n"
        "`docker.io/vladyslavhaina/mc-mirror:RELEASE.2025-08-13T08-35-41Z`, pinned by digest.\n"
        "Corresponding source: `https://github.com/minio/minio` at commit\n"
        "`07c3a429bfed433e49018cb0f78a52145d4bedeb` and `https://github.com/minio/mc` at commit\n"
        "`7394ce0dd2a80935aded936b09fa12cbb3cb8096`; the recipe is `third_party/minio-mirror/`.\n"
        "Licence: GNU Affero General Public License v3 (`AGPL-3.0-only` on the image label; most\n"
        "source files grant \"version 3 or any later version\"), Copyright MinIO, Inc.; the images\n"
        "carry upstream's `LICENSE` and `CREDITS` under `/licenses/`, and the server image's static\n"
        "curl carries its own notices under `/licenses/curl/`. The base image is Red Hat UBI 9\n"
        "micro, freely redistributable under the UBI EULA. None of this code is linked into, or\n"
        "shipped in, any Logweir image or binary. A stopgap until MinIO is replaced by a maintained\n"
        "S3 server.\n"
    )
    out("\n## The inventory\n\n")

    for name, version, spdx, lines, arm in entries:
        out("### %s@%s\n\n" % (name, version))
        out("- SPDX: `%s`\n" % spdx)
        # ONE LINE PER NOTICE. An entry with four holders prints four
        # `- Copyright:` lines and then the single source line;
        # `doc_lint.rs::inventory_entries` parses one-or-more per entry.
        for line in lines:
            out("- Copyright: %s\n" % clean(line))
        out("- Copyright source: %s\n\n" % arm)

    out("---\n\n")
    out("Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).\n\n")
    out(
        "Apache Kafka\u00ae and Kafka\u00ae are registered trademarks of the Apache Software\n"
        "Foundation. Logweir is not affiliated with or endorsed by the ASF.\n"
    )


main()
PYEOF

if [ "$WRITE" -eq 1 ]; then
  cp "$out_file" "$ROOT/THIRD_PARTY_NOTICES.md"
  echo "gen-third-party-notices: wrote THIRD_PARTY_NOTICES.md" >&2
else
  cat "$out_file"
fi
