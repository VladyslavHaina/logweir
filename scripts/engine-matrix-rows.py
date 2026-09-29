#!/usr/bin/env python3
"""Render engine-matrix rows into docs/support-matrix.md, between two markers.

    engine-matrix-rows.py DOC ROWS_DIR [--note TEXT]

`.github/workflows/engine-matrix.yml`'s `publish` job runs this over the
`*.row` files its matrix jobs uploaded. Each row file holds exactly one line:

    | v0.22.0 | 3.7.1 | `sha256:<64 hex>` | `pass` | evidence text |

(engine tag, Kafka broker version, image digest, outcome, evidence).

It rewrites ONLY the text between `<!-- engine-matrix:rows:begin -->` and
`<!-- engine-matrix:rows:end -->`. The job it replaces rewrote the whole
hand-written `## Rows` table, which would have erased the recorded evidence of
every row a person wrote there (and the broker rows PROD-01.5 adds) the first
time it ran green. It refuses, and writes nothing, when the markers are missing
or repeated, when there are no rows, or when a row is malformed: a blank table
published as a result is the failure this file exists to prevent.
"""
import argparse
import pathlib
import re
import sys

BEGIN = "<!-- engine-matrix:rows:begin -->"
END = "<!-- engine-matrix:rows:end -->"
HEADER = (
    "| Engine version | Kafka broker | Image digest | Outcome | Evidence |\n"
    "|---|---|---|---|---|\n"
)
OUTCOMES = {
    "pass",
    "pass-degraded",
    "fail(lever-not-honoured)",
    "unsupported(lever-absent)",
}
ROW = re.compile(
    r"^\| (?P<tag>v\d+\.\d+\.\d+) \| (?P<kafka>\d+\.\d+\.\d+) \| `(?P<digest>sha256:[0-9a-f]{64}|unresolved)` "
    r"\| `(?P<outcome>[a-z-]+(?:\([a-z0-9 -]+\))?)` \| (?P<evidence>[^|\n]*) \|$"
)


def version_key(text):
    return tuple(int(part) for part in text.lstrip("v").split("."))


def parse_row(line, origin):
    match = ROW.match(line)
    if not match:
        raise SystemExit(f"{origin}: not a matrix row: {line!r}")
    outcome = match["outcome"]
    if outcome not in OUTCOMES and not outcome.startswith("fail("):
        raise SystemExit(f"{origin}: unknown outcome {outcome!r}")
    return match


def render(doc_text, rows, note=None, expect=None):
    if doc_text.count(BEGIN) != 1 or doc_text.count(END) != 1:
        raise SystemExit("could not locate exactly one engine-matrix marker pair; refusing to guess")
    head, rest = doc_text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    if not rows:
        raise SystemExit("no matrix rows were produced; refusing to blank the table")
    if expect is not None and len(rows) != expect:
        raise SystemExit(
            f"{len(rows)} row(s) were produced but the workflow declares {expect}; "
            "refusing to publish a table with a row missing"
        )
    keys = [(m["tag"], m["kafka"]) for m in rows]
    if len(set(keys)) != len(keys):
        raise SystemExit(f"a (tag, broker) pair was recorded twice: {sorted(keys)}")
    ordered = sorted(
        rows,
        key=lambda m: (version_key(m["tag"]), version_key(m["kafka"])),
        reverse=True,
    )
    body = HEADER + "".join(m.group(0) + "\n" for m in ordered)
    if note:
        body = note.strip() + "\n\n" + body
    return head + BEGIN + "\n" + body + END + tail


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("doc")
    parser.add_argument("rows_dir")
    parser.add_argument("--note", default=None)
    parser.add_argument("--expect", type=int, default=None)
    args = parser.parse_args(argv)
    rows = []
    for path in sorted(pathlib.Path(args.rows_dir).rglob("*.row")):
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(lines) != 1:
            raise SystemExit(f"{path}: expected exactly one row, found {len(lines)}")
        rows.append(parse_row(lines[0].rstrip(), str(path)))
    doc = pathlib.Path(args.doc)
    doc.write_text(
        render(doc.read_text(encoding="utf-8"), rows, args.note, args.expect), encoding="utf-8"
    )
    print(f"rendered {len(rows)} row(s) into {doc}")


if __name__ == "__main__":
    main(sys.argv[1:])
