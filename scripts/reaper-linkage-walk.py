#!/usr/bin/env python3
"""The reverse-dependency walk `scripts/check-no-archive-write.sh` check 3 runs.

WHY THIS IS A FILE AND NOT A HEREDOC. A heredoc body containing backticks inside
a command substitution is mis-parsed by bash — measured, in
`scripts/check-one-signer.sh`, where the closing `)` was found sixty lines
further down and a whole check ran inside the substitution, exiting 0 with
nothing printed. A gate that reports success having run nothing is this
repository's signature defect, so the walk lives in a file the shell invokes by
path and whose exit status is read on its own line.

WHAT IT PRINTS. One workspace crate name per line, sorted: every crate from
which `<target>` is reachable over the workspace dependency graph, counting
EVERY dependency kind — normal, build and dev. Dev edges are counted on purpose:
a `logweir-reaper` added under `[dev-dependencies]` links the deleter just as
hard as one under `[dependencies]`, and dropping those edges is exactly the
mutation this walk must not survive.

`cargo metadata --no-deps` lists the workspace members and nothing else, so the
set is exactly the intra-workspace vocabulary. It performs no resolution: it
never consults `Cargo.lock`, never reaches a network and never builds.
"""

import json
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "usage: reaper-linkage-walk.py <cargo-metadata.json> <target-crate>",
            file=sys.stderr,
        )
        return 2
    meta_path, target = sys.argv[1], sys.argv[2]
    try:
        with open(meta_path, encoding="utf-8") as handle:
            meta = json.load(handle)
    except (OSError, ValueError) as exc:
        print(f"FAIL: cannot read {meta_path}: {exc}", file=sys.stderr)
        return 1

    names = {p["name"] for p in meta.get("packages", [])}
    if target not in names:
        # FAIL-CLOSED. A walk towards a crate that is not in the workspace has
        # nothing to walk towards and would report an empty reaching set
        # forever, which reads exactly like "nobody links the deleter".
        print(
            f"FAIL: {target} is not a workspace member, so this walk asserted nothing. "
            f"If the crate was renamed or removed, move the name in the same commit.",
            file=sys.stderr,
        )
        return 1

    reverse: dict[str, set[str]] = {}
    for package in meta["packages"]:
        for dependency in package.get("dependencies", []):
            if dependency["name"] in names:
                reverse.setdefault(dependency["name"], set()).add(package["name"])

    seen: set[str] = set()
    frontier = [target]
    while frontier:
        node = frontier.pop()
        for parent in reverse.get(node, ()):
            if parent not in seen:
                seen.add(parent)
                frontier.append(parent)
    seen.discard(target)
    for name in sorted(seen):
        print(name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
