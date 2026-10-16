#!/usr/bin/env python3
"""FX-53 §2.6, the traced run: which tracked files did a gate really read?

ci-trace.yml runs one gate's own command under

    python3 scripts/ci-trace.py run --out <file> -- <command>...

which is `strace -f --seccomp-bpf -qq -y -s 4096 -e trace=<TRACED> -o <file>
<command>` and exits with the command's status, and then

    python3 scripts/ci-trace.py check --root <checkout> --inputs <file> <trace>...

where --inputs is `scripts/ci-gate.sh files <gate>`, taken before the command
ran (the hashed inputs: global, definition, crate closures, paths, readers,
walks, tests, and every unclassified file). `check` prints every TRACKED file
the command opened, stat'ed, probed or executed that is not among them, and
exits 1 if there is one: the gate would be skipped when that file changes.

A path counts when it resolves inside the checkout to a file `git ls-files`
lists. Paths under target/ or .git/, outside the checkout, untracked, or
directories are ignored. A relative path is resolved against the directory
strace decorated it with (`-y`: `AT_FDCWD</dir>`, `3</dir>`, or the returned
`= 3</abs/path>`), else against the process's working directory, which this
parser follows through chdir/fchdir and inherits through clone, clone3, fork
and vfork (a CLONE_FS child shares its parent's). A string strace truncated
(`"…"...`) cannot be resolved: it is an error, not a pass.

    python3 scripts/ci-trace.py reads --root <checkout> <trace>...

prints the tracked files read, one per line (the rows use it), and

    python3 scripts/ci-trace.py canary --root <checkout>

traces a known command on this machine (a shell that changes directory, a
child that opens by relative path, a builtin's stat, a python open) and fails
unless the parser finds exactly its three files: a change in strace's output
format then fails the traced run instead of passing it.
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile

TRACED = ("openat,open,creat,stat,lstat,newfstatat,statx,access,faccessat,faccessat2,"
          "execve,execveat,chdir,fchdir,clone,clone3,fork,vfork")
# syscall -> index of the path argument; the *at forms take a dirfd before it.
PATH_ARG = {
    "open": 0, "creat": 0, "stat": 0, "lstat": 0, "access": 0, "execve": 0, "chdir": 0,
    "openat": 1, "newfstatat": 1, "fstatat64": 1, "statx": 1, "faccessat": 1,
    "faccessat2": 1, "execveat": 1,
}
DIRFD = {"openat", "newfstatat", "fstatat64", "statx", "faccessat", "faccessat2", "execveat"}
OPENS = {"open", "openat", "creat"}
# Calls that look at a file without reading it: a file the gate declares by
# presence only (ci/gates.toml `exists`) may be named by these and no other.
STATS = {"stat", "lstat", "newfstatat", "fstatat64", "statx", "access", "faccessat", "faccessat2"}
FORKS = {"clone", "clone3", "fork", "vfork"}
LINE = re.compile(r"^(?:\[pid\s+)?(\d+)\]?\s+(.*)$")
START = re.compile(r"^([a-z_0-9]+)\((.*)$")
RESUMED = re.compile(r"^<\.\.\. ([a-z_0-9]+) resumed>(.*)$")
RET = re.compile(r"\)\s+=\s+(-?\d+|\?)(?:<([^>]*)>)?")
DECORATED = re.compile(r"^(AT_FDCWD|-?\d+)(?:<(.*)>)?$")


class TraceError(Exception):
    """The trace cannot be read soundly."""


def c_string(text, at):
    """Parse the C string literal starting at text[at] == '"'.
    Returns (value, end index after the literal, truncated)."""
    assert text[at] == '"'
    out, i = bytearray(), at + 1
    while i < len(text):
        c = text[i]
        if c == '"':
            truncated = text.startswith("...", i + 1)
            return out.decode("utf-8", "surrogateescape"), i + 1 + (3 if truncated else 0), truncated
        if c == "\\":
            n = text[i + 1]
            simple = {"n": 10, "t": 9, "r": 13, "v": 11, "f": 12, "\\": 92, '"': 34, "'": 39}
            if n in simple:
                out.append(simple[n])
                i += 2
            elif n == "x":
                out.append(int(text[i + 2:i + 4], 16))
                i += 4
            elif n.isdigit():
                j = i + 1
                while j < len(text) and j < i + 4 and text[j] in "01234567":
                    j += 1
                out.append(int(text[i + 1:j], 8))
                i = j
            else:
                out.extend(n.encode())
                i += 2
            continue
        out.extend(c.encode("utf-8", "surrogateescape"))
        i += 1
    raise TraceError(f"unterminated string: {text[at:at + 80]}")


def split_args(body):
    """Top-level comma-separated arguments of a syscall's text, strings kept
    whole; the text may end before the closing paren (unfinished)."""
    args, depth, cur, i = [], 0, [], 0
    while i < len(body):
        c = body[i]
        if c == '"':
            _, end, _ = c_string(body, i)
            cur.append(body[i:end])
            i = end
            continue
        if c in "{[(<":
            depth += 1
        elif c in "}])>":
            if depth == 0:
                break
            depth -= 1
        elif c == "," and depth == 0:
            args.append("".join(cur).strip())
            cur = []
            i += 1
            continue
        cur.append(c)
        i += 1
    if cur and "".join(cur).strip():
        args.append("".join(cur).strip())
    return args


def string_arg(arg):
    if not arg.startswith('"'):
        return None, False
    value, _, truncated = c_string(arg, 0)
    return value, truncated


def events(lines):
    """Completed syscalls as dicts {start, end, pid, name, text}: `start` and
    `end` are line numbers, the same unless an `<unfinished ...>` waited for
    its `<... resumed>`."""
    pending, out = {}, []
    for n, raw in enumerate(lines):
        raw = raw.rstrip("\n")
        m = LINE.match(raw)
        if not m:
            continue
        pid, rest = int(m.group(1)), m.group(2)
        if rest.startswith(("---", "+++")):
            continue
        r = RESUMED.match(rest)
        if r:
            if pid not in pending:
                continue
            name, head, start = pending.pop(pid)
            out.append({"start": start, "end": n, "pid": pid, "name": name, "text": head + r.group(2)})
            continue
        s = START.match(rest)
        if not s:
            continue
        name, text = s.group(1), s.group(2)
        if text.endswith("<unfinished ...>"):
            pending[pid] = (name, text[: -len("<unfinished ...>")].rstrip(), n)
            continue
        out.append({"start": n, "end": n, "pid": pid, "name": name, "text": text})
    return out


def returned(text):
    """(value, decoration) of the last `) = value<decoration>` in the text."""
    found = None
    for found in RET.finditer(text):
        pass
    return (found.group(1), found.group(2)) if found else (None, None)


def reads(lines, root, start_cwd=None):
    """Every absolute path the trace names (opened, stat'ed, probed, executed),
    resolved, as (path, syscall, the reading process's executable or None);
    raises TraceError for a truncated or unresolvable path.

    Working directories: the first process starts in start_cwd (default: the
    root). A child is created at its parent's clone/fork START, with a copy
    of the parent's directory then (or the parent's own, shared, under
    CLONE_FS), so its first syscalls, which strace may print before the
    parent's clone returns, resolve against the right directory.
    """
    evs = events(lines)
    timeline = []
    for e in evs:
        value, _ = returned(e["text"])
        if e["name"] in FORKS and value and value.isdigit() and int(value) > 0:
            shared = e["name"] in ("clone", "clone3") and "CLONE_FS" in e["text"]
            timeline.append((e["start"], 0, "spawn", e, int(value), shared))
        timeline.append((e["end"], 1, "done", e, None, None))
    timeline.sort(key=lambda t: (t[0], t[1]))
    fs_of, exe_of, found = {}, {}, []
    start_cwd = start_cwd or root

    def fs(pid):
        if pid not in fs_of:
            fs_of[pid] = [start_cwd]
        return fs_of[pid]

    for _, _, kind, e, child, shared in timeline:
        parent = fs(e["pid"])
        if kind == "spawn":
            fs_of[child] = parent if shared else [parent[0]]
            exe_of[child] = exe_of.get(e["pid"])
            continue
        name, text = e["name"], e["text"]
        value, decoration = returned(text)
        args = split_args(text)
        if name == "fchdir":
            if value == "0" and args:
                d = DECORATED.match(args[0])
                if not (d and d.group(2)):
                    raise TraceError(f"fchdir({args[0]}) without its path (strace -y)")
                parent[0] = d.group(2)
            continue
        if name not in PATH_ARG:
            continue
        idx = PATH_ARG[name]
        if idx >= len(args):
            continue
        path, truncated = string_arg(args[idx])
        if path is None:
            continue
        if truncated:
            raise TraceError(f"{name} names a truncated path ({path}...): raise strace -s")
        if name in OPENS and decoration and value and not value.startswith("-") and value != "?":
            resolved = decoration                         # `= 3</abs/path>`
        elif path.startswith("/"):
            resolved = path
        else:
            base = parent[0]
            if name in DIRFD:
                d = DECORATED.match(args[0])
                if not d:
                    raise TraceError(f"{name}: unreadable dirfd {args[0]!r}")
                if d.group(2):
                    base = d.group(2)
                elif d.group(1) != "AT_FDCWD":
                    raise TraceError(f"{name}: dirfd {args[0]} without its path (strace -y)")
            resolved = os.path.join(base, path)
        resolved = os.path.normpath(resolved)
        if name == "chdir":
            if value == "0":
                parent[0] = resolved
            continue
        found.append((resolved, name, exe_of.get(e["pid"])))
        if name in ("execve", "execveat") and value == "0":
            exe_of[e["pid"]] = resolved
    return found


def tracked_files(root):
    out = subprocess.run(["git", "-C", root, "ls-files", "-z"], check=True,
                         capture_output=True).stdout
    return {p.decode() for p in out.split(b"\0") if p}


def to_tracked(found, root, tracked):
    roots = {os.path.normpath(root), os.path.realpath(root)}
    hits = {}
    for path, name, exe in found:
        for r in roots:
            if path.startswith(r + "/"):
                rel = path[len(r) + 1:]
                break
        else:
            continue
        if rel.startswith(("target/", ".git/")) or rel in ("target", ".git"):
            continue
        if rel in tracked:
            hit = hits.setdefault(rel, {"calls": set(), "exes": set()})
            hit["calls"].add(name)
            hit["exes"].add(os.path.basename(exe) if exe else "?")
    return hits


def read_traces(paths):
    for path in paths:
        with open(path, encoding="utf-8", errors="surrogateescape") as fh:
            yield from fh


def strace_argv(out, command, seccomp=True):
    return ["strace", "-f", *(["--seccomp-bpf"] if seccomp else []), "-qq", "-y", "-s", "4096",
            "-e", f"trace={TRACED}", "-o", out, *command]


def failed_marker(trace):
    return trace + ".tracer-failed"


def run(out, command, seccomp=True):
    """Run the command under strace. strace's own diagnostics ("strace: …"
    on stderr, e.g. a PTRACE_LISTEN error that stops it following a process)
    mean the trace is incomplete: the run fails and leaves a marker that
    `check` refuses, so an incomplete trace can never pass."""
    marker = failed_marker(out)
    if os.path.exists(marker):
        os.unlink(marker)
    proc = subprocess.Popen(strace_argv(out, command, seccomp), stderr=subprocess.PIPE)
    diagnostics = []
    for raw in proc.stderr:
        line = raw.decode("utf-8", "replace")
        sys.stderr.write(line)
        if line.startswith("strace: "):
            diagnostics.append(line.strip())
    rc = proc.wait()
    if diagnostics:
        with open(marker, "w") as fh:
            fh.write("\n".join(diagnostics) + "\n")
        print(f"ci-trace: strace reported {len(diagnostics)} error(s); the trace is incomplete",
              file=sys.stderr)
        return rc or 3
    return rc


CANARY = ("cd ci && cat gates.toml > /dev/null && cd .. && test -e justfile "
          "&& python3 -c 'open(\"README.md\").read()'")
CANARY_READS = {"ci/gates.toml", "justfile", "README.md"}


def canary(root):
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "canary.txt")
        rc = subprocess.run(strace_argv(out, ["bash", "-c", CANARY]), cwd=root).returncode
        if rc != 0:
            print(f"ci-trace: the canary command exited {rc}", file=sys.stderr)
            return 2
        with open(out, encoding="utf-8", errors="surrogateescape") as fh:
            lines = fh.readlines()
    hits = to_tracked(reads(lines, root), root, tracked_files(root))
    missing = CANARY_READS - set(hits)
    print(f"ci-trace: canary: {len(lines)} trace lines; read {sorted(set(hits) & CANARY_READS)}")
    if missing:
        print(f"ci-trace: the canary's reads were not found: {sorted(missing)}. The parser does not "
              "read this strace's output; nothing traced on this runner can be trusted.", file=sys.stderr)
        return 1
    return 0


def main(argv):
    if argv[:1] == ["canary"]:
        if len(argv) != 3 or argv[1] != "--root":
            print("usage: ci-trace.py canary --root <checkout>", file=sys.stderr)
            return 2
        try:
            return canary(os.path.normpath(os.path.abspath(argv[2])))
        except TraceError as err:
            print(f"ci-trace: canary: {err}", file=sys.stderr)
            return 1
    if argv[:1] == ["run"]:
        rest, dry, seccomp = argv[1:], False, True
        while rest[:1] in (["--dry-run"], ["--no-seccomp-bpf"]):
            dry = dry or rest[0] == "--dry-run"
            seccomp = seccomp and rest[0] != "--no-seccomp-bpf"
            rest = rest[1:]
        if len(rest) < 4 or rest[0] != "--out" or rest[2] != "--":
            print("usage: ci-trace.py run [--dry-run] [--no-seccomp-bpf] --out <file> -- <command>...",
                  file=sys.stderr)
            return 2
        if dry:
            print("\0".join(strace_argv(rest[1], rest[3:], seccomp)))
            return 0
        return run(rest[1], rest[3:], seccomp)
    ap = argparse.ArgumentParser(prog="ci-trace.py")
    ap.add_argument("command", choices=("check", "reads"))
    ap.add_argument("--root", required=True, help="the checkout")
    ap.add_argument("--inputs", help="check: the gate's inputs, one tracked path per line")
    ap.add_argument("--exists", help="check: the files the gate declares by presence only "
                    "(`ci-gate.sh exists`); a stat or probe of one is declared, an open is not")
    ap.add_argument("--tracked", help="the tracked files, one per line (default: git ls-files)")
    ap.add_argument("--cwd", help="the directory strace started in (default: --root)")
    ap.add_argument("--reads-out", help="check: also write every tracked file read, with its "
                    "syscalls and readers, one per line")
    ap.add_argument("traces", nargs="+")
    a = ap.parse_args(argv)
    root = os.path.normpath(os.path.abspath(a.root))
    for trace in a.traces:
        if os.path.exists(failed_marker(trace)):
            print(f"ci-trace: {trace} is incomplete (strace failed: "
                  f"{open(failed_marker(trace)).read().strip()}); nothing in it can pass", file=sys.stderr)
            return 2
    try:
        tracked = (set(open(a.tracked).read().split("\n")) - {""}) if a.tracked else tracked_files(root)
        hits = to_tracked(reads(read_traces(a.traces), root, a.cwd), root, tracked)
    except TraceError as err:
        print(f"ci-trace: {err}", file=sys.stderr)
        return 2
    if a.command == "reads":
        print("\n".join(sorted(hits)))
        return 0
    if not a.inputs:
        ap.error("check needs --inputs")
    declared = set(open(a.inputs).read().split("\n")) - {""}
    if not declared:
        print("ci-trace: the gate declares no inputs (did `ci-gate.sh files` fail?)", file=sys.stderr)
        return 2
    present = (set(open(a.exists).read().split("\n")) - {""}) if a.exists else set()
    undeclared = sorted(rel for rel in hits if rel not in declared
                        and not (rel in present and hits[rel]["calls"] <= STATS))
    if a.reads_out:
        with open(a.reads_out, "w") as fh:
            for rel in sorted(hits):
                mark = "UNDECLARED" if rel in undeclared else "declared"
                fh.write(f"{mark}\t{rel}\t{','.join(sorted(hits[rel]['calls']))}\t"
                         f"{','.join(sorted(hits[rel]['exes']))}\n")
    print(f"ci-trace: {len(hits)} tracked files read, {len(declared)} declared, "
          f"{len(undeclared)} undeclared")
    for rel in undeclared:
        print(f"UNDECLARED {rel} ({', '.join(sorted(hits[rel]['calls']))}) "
              f"by {', '.join(sorted(hits[rel]['exes']))}")
    return 1 if undeclared else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
