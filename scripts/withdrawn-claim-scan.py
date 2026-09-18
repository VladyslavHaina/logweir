#!/usr/bin/env python3
"""The wrap-insensitive corpus scan `scripts/check-withdrawn-claim.sh` runs.

WHY THIS EXISTS (review `d3w9` R2). The gate was `grep -rniF`, which is
LINE-BASED, and this corpus hard-wraps prose at a median of 76 columns. A
phrase that happened to wrap — `"… an attributable\\nsigned record"` — was
invisible to it. That is not a gap in one phrase: **every one of the fourteen
phrases was escapable the same way, including the original withdrawn signing
claim the gate was built for**, and escaping it took nothing more deliberate
than a paragraph reflow.

So the match runs over a WHITESPACE-NORMALISED join of each file, and the line
number it reports is the line the phrase STARTS on, which is what a reader
needs.

WHAT "NORMALISED" MEANS HERE, EXACTLY. Each line is stripped, a leading comment
marker is removed (`//!`, `///`, `//`, `#`, `*`, and a Markdown blockquote `>`),
the rest is stripped again, and the lines are joined with ONE space. So for a
two-word phrase `alpha beta`, all four of these match:

    ... an alpha beta here.             (one line)
    ... an alpha                        (a prose wrap)
    beta here.
    /// ... an alpha                    (a Rust doc comment wrap)
    /// beta here.
    # ... an alpha                      (a shell comment wrap)
    # beta here.

The comment-marker strip is the part that makes the Rust and shell cases work;
without it the join would read `alpha /// beta`.

**THIS FILE QUOTES NO PHRASE FROM THE LIST, AND THAT IS DELIBERATE.** The gate
exempts exactly two paths, as literals, and
`crates/logweir/tests/withdrawn_claim.rs::the_exemption_list_is_exactly_two_paths`
asserts there is no third. A scan that had to be exempted from itself would be
the first crack in that.

WHAT IT STILL CANNOT CATCH, stated so nobody mistakes it for a proof: a phrase
split by punctuation or rewritten in other words. The phrase list has always
been "the sentence and its near paraphrases, not every possible rewording" and
that is unchanged — this only stops a LINE BREAK from being an exemption.

Output is `path:lineno:text`, byte for byte the shape `grep -rn` produced, so
the gate's reporting loop is untouched. A file that is not valid UTF-8 is
skipped: `grep -r` would have reported it as a binary match with no usable
text, which is not a surface anyone reads.
"""

import sys
from pathlib import Path

# The comment markers a continuation line may carry, longest first so `///`
# is stripped before `//`.
MARKERS = ("//!", "///", "//", "#", "*", ">")


def normalise(text: str) -> tuple[str, list[int]]:
    """The file as one whitespace-normalised line, plus a line number per char.

    The second element is parallel to the first: `lines[i]` is the 1-based line
    the character at `joined[i]` came from, so a match offset can be reported at
    the line it starts on.
    """
    joined: list[str] = []
    lines: list[int] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        piece = raw.strip()
        for marker in MARKERS:
            if piece.startswith(marker):
                piece = piece[len(marker) :].strip()
                break
        if not piece:
            continue
        if joined:
            joined.append(" ")
            lines.append(number)
        joined.append(piece)
        lines.extend([number] * len(piece))
    return "".join(joined), lines


def main() -> int:
    if len(sys.argv) < 3:
        print(
            "usage: withdrawn-claim-scan.py <phrase-file> <path> [<path> ...]",
            file=sys.stderr,
        )
        return 2
    phrase_path = Path(sys.argv[1])
    try:
        phrases = [
            p for p in phrase_path.read_text(encoding="utf-8").splitlines() if p.strip()
        ]
    except OSError as exc:
        print(f"FAIL: cannot read the phrase list {phrase_path}: {exc}", file=sys.stderr)
        return 1
    if not phrases:
        # FAIL-CLOSED. An empty phrase list would report a clean corpus forever,
        # which is exactly how a grep-shaped gate stops meaning anything.
        print(
            "FAIL: the phrase list is empty, so this scan asserted nothing",
            file=sys.stderr,
        )
        return 1
    needles = [p.lower() for p in phrases]

    scanned = 0
    for raw_path in sys.argv[2:]:
        path = Path(raw_path)
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        scanned += 1
        joined, lines = normalise(text)
        haystack = joined.lower()
        reported: set[int] = set()
        for needle in needles:
            start = 0
            while True:
                at = haystack.find(needle, start)
                if at == -1:
                    break
                start = at + 1
                lineno = lines[at] if at < len(lines) else 1
                if lineno in reported:
                    continue
                reported.add(lineno)
                # The ORIGINAL line, not the normalised join: a reader fixing
                # this needs the text as it sits in the file.
                original = text.splitlines()[lineno - 1].strip()
                print(f"{path}:{lineno}:{original}")
    if scanned == 0:
        print(
            "FAIL: the scan read no file at all, so it asserted nothing",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
