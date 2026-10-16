#!/usr/bin/env bash
# Content-addressed gate results (FX-53 §2.1, §2.2). A CI gate (a job) is
# skipped only when a MARKER proves that the same gate passed on the same
# inputs in a same-repository run of ci.yml; otherwise it runs and leaves one.
#
#   ci-gate.sh hash <gate>                      the gate's input hash (hex)
#   ci-gate.sh files <gate>                     the tracked files whose contents it hashes
#   ci-gate.sh exists <gate>                    the tracked files whose presence only
#                                               it hashes (`exists` patterns)
#   ci-gate.sh lookup <gate> <hash>             0 = a run proves it (prints
#                                               proof-run=<url>); anything else = run
#   ci-gate.sh record <gate> <hash> <dir>       write the marker file into <dir>;
#                                               prints name=… and path=… (nothing
#                                               for a fork's pull request)
#   ci-gate.sh decide <gate> [force]            hash, then lookup unless forced;
#                                               writes hit, proof-run and hash to
#                                               $GITHUB_OUTPUT. Always exits 0.
#   ci-gate.sh filter <e2e gate>                the nextest filterset the shard runs
#   ci-gate.sh gates | unclaimed | describe     the map, the unclassified files,
#                                               and the whole map as JSON (lint)
#   ci-gate.sh match <pattern>…                 the tracked files each pattern names (JSON)
#   ci-gate.sh explain <gate> <hash> <run> [--job <name>]
#                                               diagnostics: each proof check against
#                                               one run; never used to skip
#
# THE HASH is sha256 over a scheme line, the gate's name, and `<mode> <blob id>
# <path>` of every tracked input (`git ls-files -s`, sorted). It reads the
# index, which must equal HEAD with no tracked change in the worktree (a dirty
# checkout gives no hash, so the gate runs), and `cargo metadata --no-deps`; it
# builds nothing. The inputs are ci/gates.toml's expansion of the gate:
# [global] (the lockfile, toolchain, root manifest, the map, this script, the
# local actions, and every workflow a proving run executes), its definition,
# crate closures, paths, readers, walks and tests, plus every tracked file no
# gate classifies; and, by PATH ONLY, the files its `exists` patterns name
# (what the gate only looks for: cargo's target discovery, a walker's
# is_dir). The runner is not hashed: a gate's job is one ci.yml job,
# and its `runs-on` is in ci.yml, a global input; the nightly catches image drift.
#
# THE PROOF. A marker artifact `gate-<gate>-<hash>` is only an INDEX: any code
# in a same-repository job could upload one. `lookup` accepts a marker only
# when the run it names
#   1. is this repository's, not a fork's, and the marker is not expired;
#   2. is a PROVERS run: ci.yml pushed to main or dispatched on main;
#      release.yml calling ci.yml from a v* tag push (the tag, read from the
#      refs API, peels to the run's commit) or a dispatch on main; or
#      ci-nightly.yml calling ci.yml from its schedule or a dispatch on main.
#      A main run's commit must be on main (the compare API). Pull requests,
#      pull_request_target, workflow_run and everything else prove nothing;
#   3. has exactly one job, per attempt, named as the gate's job, and that job
#      concluded `success` in some attempt (the jobs API);
#   4. has a head commit whose tree (the git trees API, not truncated) gives
#      the requested hash under this map.
# A forged marker can then only point at what is already true: the gate's own
# job passed on exactly these inputs, run by these workflow files.
#
# FAIL SAFE. Exit 0 from `lookup` is the only "skip". A miss is 1; a refused
# marker is a miss; an API error, a map error, a dirty checkout, a pattern that
# matches no tracked file (a stale definition), or an unknown gate exits 2 or
# 3. The gate action runs the gate on anything but 0. The lookup uses `gh api`
# (CI_GATE_GH overrides it) with $GH_TOKEN, which needs `actions: read`; cargo
# is CI_GATE_CARGO, CARGO, or `cargo`.
set -euo pipefail

self="${BASH_SOURCE[0]}"
root="$(cd "$(dirname "$self")/.." && pwd)"

decide() {
  local gate="${1:?usage: ci-gate.sh decide <gate> [force]}" force="${2:-false}"
  local out="${GITHUB_OUTPUT:-/dev/stdout}" env_file="${GITHUB_ENV:-/dev/null}"
  local hit=false proof="" hash="" answer="" line=""
  if hash="$(bash "$self" hash "$gate")" && [[ "$hash" =~ ^[0-9a-f]{64}$ ]]; then
    if [ "$force" = "true" ]; then
      echo "ci-gate: $gate: forced; markers are ignored and the gate runs" >&2
    elif answer="$(bash "$self" lookup "$gate" "$hash")"; then
      while IFS= read -r line; do
        case "$line" in proof-run=*) proof="${line#proof-run=}" ;; esac
      done <<< "$answer"
      if [ -n "$proof" ]; then
        hit=true
      else
        echo "ci-gate: $gate: the lookup named no proving run; the gate runs" >&2
      fi
    else
      echo "ci-gate: $gate: no marker proves inputs $hash; the gate runs" >&2
    fi
  else
    hash=""
    echo "ci-gate: $gate: no input hash; the gate runs and records no marker" >&2
  fi
  if [ "$hit" = true ]; then
    echo "::notice title=gate $gate skipped::inputs passed in $proof"
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
      echo "Gate \`$gate\`: inputs \`$hash\` passed in $proof; skipped." >> "$GITHUB_STEP_SUMMARY"
    fi
  fi
  {
    echo "hit=$hit"
    echo "proof-run=$proof"
    echo "hash=$hash"
  } >> "$out"
  {
    echo "CI_GATE_NAME=$gate"
    echo "CI_GATE_HASH=$hash"
    echo "CI_GATE_HIT=$hit"
  } >> "$env_file"
}

case "${1:-}" in
  decide)
    shift
    decide "$@"
    exit 0
    ;;
esac

exec python3 -I - "$root" "$@" <<'PY'
import datetime
import hashlib
import json
import os
import re
import subprocess
import sys

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11: no TOML reader, so no hash
    print("ci-gate: python3 3.11 or later is needed (tomllib)", file=sys.stderr)
    sys.exit(4)

SCHEME = "ci-gate/3"
MAP = "ci/gates.toml"
GATE_KEYS = {"covers", "job", "definition", "crates", "paths", "readers", "walks", "tests", "rows", "except", "exists", "always"}
# An e2e shard runs whole binaries (`tests`), single tests of another binary
# (`rows`, `<binary>::<test>`), and leaves out of its own binaries the tests
# another shard runs as rows (`except`, the same spelling). FX-53: the
# console-reading row of record_semantics runs in a shard of its own.
ROW = re.compile(r"^[a-z0-9_]+::[A-Za-z0-9_:]+$")
# Pattern kinds that CLASSIFY a path. `walks` and `global` are inputs but say
# nothing about who owns a file.
CLASSIFYING = ("definition", "crates", "paths", "readers", "tests")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
GATE_NAME = re.compile(r"^[a-z0-9][a-z0-9-]*$")
EXIT_HIT = EXIT_OK = 0
EXIT_MISS, EXIT_ERROR, EXIT_MAP, EXIT_ENV = 1, 2, 3, 4


class MapError(Exception):
    """The map or the tree cannot give a sound hash: run the gate."""


class LookupFailed(Exception):
    """The API gave no answer that can be trusted: run the gate."""


ROOT = sys.argv[1]
ARGS = sys.argv[2:]


def die(code, message):
    print(f"ci-gate: {message}", file=sys.stderr)
    sys.exit(code)


def git(*args):
    proc = subprocess.run(["git", "-C", ROOT, *args], capture_output=True)
    if proc.returncode != 0:
        raise MapError(f"git {' '.join(args[:2])}: {proc.stderr.decode(errors='replace').strip()}")
    return proc.stdout


def load_map():
    try:
        with open(os.path.join(ROOT, MAP), "rb") as fh:
            gmap = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as err:
        raise MapError(f"{MAP}: {err}")
    if gmap.get("schema") != 1:
        raise MapError(f"{MAP}: schema {gmap.get('schema')!r}, this script reads 1")
    gates = gmap.get("gates")
    if not isinstance(gates, dict) or not gates:
        raise MapError(f"{MAP}: no [gates]")
    for name, gate in gates.items():
        if not GATE_NAME.match(name):
            raise MapError(f"{MAP}: gate name {name!r}")
        unknown = set(gate) - GATE_KEYS
        if unknown:
            # A misspelt key would silently drop inputs.
            raise MapError(f"{MAP}: gate {name}: unknown key(s) {sorted(unknown)}")
        if not isinstance(gate.get("job", name), str) or not gate.get("job", name):
            raise MapError(f"{MAP}: gate {name}: job must be a job name")
        for key in GATE_KEYS - {"always", "job"}:
            value = gate.get(key, [])
            if not isinstance(value, list) or not all(isinstance(v, str) and v for v in value):
                raise MapError(f"{MAP}: gate {name}: {key} must be a list of strings")
        if not isinstance(gate.get("always", False), bool):
            raise MapError(f"{MAP}: gate {name}: always must be true or false")
        for key in ("rows", "except"):
            for row in gate.get(key, []):
                if not ROW.match(row):
                    raise MapError(f"{MAP}: gate {name}: {key} entry {row!r} is not <binary>::<test>")
    if not isinstance(gmap.get("global", {}).get("paths"), list):
        raise MapError(f"{MAP}: [global].paths is missing")
    present = gmap["global"].get("exists", [])
    if not isinstance(present, list) or not all(isinstance(v, str) and v for v in present):
        raise MapError(f"{MAP}: [global].exists must be a list of strings")
    return gmap


def compile_glob(pattern):
    """git's `:(glob)` pathspec, for the subset this map may use.

    `*` and `?` stop at `/`; `**` is a whole path segment and crosses `/`; a
    pattern without a wildcard names a file or a whole directory, as git's
    pathspec does. Anything else is refused rather than guessed at.
    scripts/test-ci-gate.py holds this to `git ls-files ':(glob)…'` for every
    pattern in the map.
    """
    if not pattern or pattern.startswith("/") or pattern.endswith("/") or "//" in pattern:
        raise MapError(f"pattern {pattern!r}: not a relative path")
    if any(c in pattern for c in "[]{}\\!"):
        raise MapError(f"pattern {pattern!r}: only *, ? and ** are understood")
    if not any(c in pattern for c in "*?"):
        return re.compile(re.escape(pattern) + "(?:/.+)?")
    segments = pattern.split("/")
    regex = ""
    for i, segment in enumerate(segments):
        last = i == len(segments) - 1
        if segment == "**":
            regex += ".+" if last else "(?:[^/]+/)*"
            continue
        if "**" in segment:
            raise MapError(f"pattern {pattern!r}: ** must be a whole path segment")
        regex += "".join("[^/]*" if c == "*" else "[^/]" if c == "?" else re.escape(c)
                         for c in segment)
        if not last:
            regex += "/"
    return re.compile(regex)


class Tree:
    """Tracked files as {path: (mode, blob id)}: this checkout's index
    (`git ls-files -s`, which a checkout makes equal to its commit), or a
    commit's tree from the git trees API. Matched by compile_glob."""

    def __init__(self, entries=None):
        self.cache = {}
        if entries is not None:
            self.all = entries
            return
        self.all = {}
        for record in git("ls-files", "-s", "-z").split(b"\0"):
            if not record:
                continue
            meta, _, path = record.partition(b"\t")
            mode, blob, stage = meta.decode().split(" ")
            if stage != "0":
                raise MapError(f"{path.decode()}: unmerged in the index")
            self.all[path.decode()] = (mode, blob)

    def match(self, pattern):
        if pattern not in self.cache:
            regex = compile_glob(pattern)
            self.cache[pattern] = {p for p in self.all if regex.fullmatch(p)}
        return self.cache[pattern]


def require_clean():
    """The hash reads the index and cargo reads the worktree's manifests, so
    both must be the commit: no staged and no unstaged tracked change."""
    for args in (("diff", "--cached", "--quiet", "HEAD", "--"), ("diff", "--quiet", "--")):
        proc = subprocess.run(["git", "-C", ROOT, *args], capture_output=True)
        if proc.returncode == 1:
            raise MapError("the checkout differs from HEAD (" + " ".join(args[:2])
                           + "): a gate hashes a clean commit only")
        if proc.returncode != 0:
            raise MapError("git " + " ".join(args) + ": "
                           + proc.stderr.decode(errors="replace").strip())


class Workspace:
    """Workspace packages and their path dependencies, from cargo metadata."""

    def __init__(self):
        self._members = None

    def members(self):
        if self._members is None:
            cargo = os.environ.get("CI_GATE_CARGO") or os.environ.get("CARGO") or "cargo"
            proc = subprocess.run(
                [cargo, "metadata", "--locked", "--offline", "--no-deps", "--format-version", "1"],
                cwd=ROOT, capture_output=True)
            if proc.returncode != 0:
                raise MapError("cargo metadata: " + proc.stderr.decode(errors="replace").strip())
            try:
                meta = json.loads(proc.stdout)
                ws_root = meta["workspace_root"]
                members = {}
                for pkg in meta["packages"]:
                    directory = os.path.relpath(os.path.dirname(pkg["manifest_path"]), ws_root)
                    if directory == "." or directory.startswith(".."):
                        raise MapError(f"package {pkg['name']} at {directory!r}")
                    members[pkg["name"]] = {
                        "dir": directory.replace(os.sep, "/"),
                        "deps": [(d["name"], d.get("kind") or "normal")
                                 for d in pkg["dependencies"] if d.get("path")],
                    }
            except (ValueError, KeyError, TypeError) as err:
                raise MapError(f"cargo metadata: unreadable output ({err})")
            self._members = members
        return self._members

    def closure(self, names):
        members = self.members()
        wanted = sorted(members) if names == ["*"] else names
        seen, stack = set(), [(n, True) for n in wanted]
        while stack:
            name, top = stack.pop()
            if name not in members:
                raise MapError(f"crate {name!r} is not a workspace package")
            if (name, top) in seen:
                continue
            seen.add((name, top))
            for dep, kind in members[name]["deps"]:
                if dep in members and (top or kind in ("normal", "build")):
                    stack.append((dep, False))
        return sorted({members[n]["dir"] for n, _ in seen})


def patterns(gmap, ws, name):
    gate = gmap["gates"][name]
    kinds = {
        "global": list(gmap["global"]["paths"]),
        "definition": list(gate.get("definition", [])),
        "crates": [d + "/**" for d in ws.closure(gate["crates"])] if gate.get("crates") else [],
        "paths": list(gate.get("paths", [])),
        "readers": list(gate.get("readers", [])),
        "walks": list(gate.get("walks", [])),
        "tests": [f"e2e/tests/{t}.rs" for t in gate.get("tests", [])]
        + [f"e2e/tests/{r.split('::', 1)[0]}.rs" for r in gate.get("rows", [])],
        # PRESENCE only: the gate's commands look whether these exist (cargo's
        # target discovery, a walker's is_dir), not what they hold. The hash
        # covers their paths, not their contents.
        "exists": list(gmap["global"].get("exists", [])) + list(gate.get("exists", [])),
    }
    return kinds


def unclaimed(gmap, ws, tree):
    claimed = set()
    for pattern in gmap["global"]["paths"]:
        claimed |= tree.match(pattern)
    for name in gmap["gates"]:
        kinds = patterns(gmap, ws, name)
        for kind in CLASSIFYING:
            for pattern in kinds[kind]:
                claimed |= tree.match(pattern)
    return set(tree.all) - claimed


def inputs(gmap, ws, tree, name, strict=True):
    if name not in gmap["gates"]:
        raise MapError(f"unknown gate {name!r}")
    files, empty = set(), []
    for kind, pats in patterns(gmap, ws, name).items():
        for pattern in pats:
            matched = tree.match(pattern)
            if not matched:
                empty.append(f"{kind}: {pattern}")
            if kind != "exists":
                files |= matched
    if strict and empty:
        # A definition or input that names nothing is stale: no sound hash.
        raise MapError(f"gate {name}: no tracked file matches " + "; ".join(empty))
    if not gmap["gates"][name].get("definition"):
        raise MapError(f"gate {name}: no definition")
    return files | unclaimed(gmap, ws, tree), empty


def presence(gmap, ws, tree, name, files):
    """The files the gate only checks for (its `exists` patterns), minus those
    it reads: their paths are hashed, not their contents."""
    present = set()
    for pattern in patterns(gmap, ws, name)["exists"]:
        present |= tree.match(pattern)
    return present - files


def gate_hash(gmap, ws, tree, name):
    files, _ = inputs(gmap, ws, tree, name)
    digest = hashlib.sha256()
    digest.update(f"{SCHEME}\ngate {name}\n".encode())
    for path in sorted(files):
        mode, blob = tree.all[path]
        digest.update(f"{mode} {blob} {path}\0".encode())
    for path in sorted(presence(gmap, ws, tree, name, files)):
        digest.update(f"exists {path}\0".encode())
    return digest.hexdigest()


# ------------------------------------------------------------------ markers

def gh_api(path, params=None):
    gh = os.environ.get("CI_GATE_GH") or "gh"
    cmd = [gh, "api", "-X", "GET", path]
    for key, value in (params or {}).items():
        cmd += ["-f", f"{key}={value}"]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as err:
        raise LookupFailed(f"{path}: {err}")
    if proc.returncode != 0:
        raise LookupFailed(f"{path}: gh exited {proc.returncode}: "
                           + proc.stderr.decode(errors="replace").strip()[:300])
    try:
        return json.loads(proc.stdout)
    except ValueError as err:
        raise LookupFailed(f"{path}: not JSON ({err})")


def parse_time(value):
    if not isinstance(value, str):
        raise LookupFailed(f"time {value!r}")
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def refuse_artifact(artifact, name, repo_id, now):
    if artifact.get("name") != name:
        return f"named {artifact.get('name')!r}"
    if artifact.get("expired") is not False:
        return "expired"
    if parse_time(artifact.get("expires_at")) <= now:
        return "past its expiry"
    run = artifact.get("workflow_run")
    if not isinstance(run, dict):
        return "no workflow run"
    if run.get("repository_id") != repo_id:
        return f"from repository {run.get('repository_id')}"
    if run.get("head_repository_id") != repo_id:
        return f"a fork's run (head repository {run.get('head_repository_id')})"
    if not isinstance(run.get("id"), int):
        return "no run id"
    return None


# WHO MAY PROVE A GATE. A marker artifact is only an index: anything that runs
# in a same-repository job can upload one. The proof is the RUN it points at:
# one of these workflows, on one of these events and refs, in which the gate's
# OWN job concluded success, on a commit whose tree gives the requested hash.
# `ref` is "main" (the run's head branch is main and its commit is on main) or
# "tag" (a v* tag push: the tag exists and peels to the run's commit). `prefix`
# is how a caller names ci.yml's jobs: `<caller job> / <job>`. Every workflow
# here, and every workflow they call, is a [global] input of every gate
# (gate_map_lint holds that), so a proving run executed the same workflow
# text as the requester's.
#
# NOT pull_request. A pull request run tests and executes the MERGE commit of
# its head and the base branch's tip at that moment, but the API names only
# the head (`head_sha`); `pull_requests[].base.sha` is the pull request's
# recorded base, not the base that was merged (PR #60's run 38808363036
# merged 65ba93fa while it records fb29ee72), and the base branch can have
# been retargeted since. No API answer binds the tested tree, so a pull
# request's run proves nothing. It still USES markers from these runs.
PROVERS = {
    ".github/workflows/ci.yml": {
        "events": {"push": "main", "workflow_dispatch": "main"},
        "prefix": ""},
    ".github/workflows/release.yml": {
        "events": {"push": "tag", "workflow_dispatch": "main"},
        "prefix": "tests / "},
    ".github/workflows/ci-nightly.yml": {
        "events": {"schedule": "main", "workflow_dispatch": "main"},
        "prefix": "ci / "},
}
CI = ".github/workflows/ci.yml"
TAG = re.compile(r"^v[0-9][0-9A-Za-z.+-]*$")


def refuse_run(run, repo, repo_id):
    """Why this run cannot prove anything (reason, None), or (None, prover).
    Reads only the run object; refuse_ref asks the API about its ref."""
    if run.get("repository", {}).get("id") != repo_id:
        return "a run of another repository", None
    if run.get("head_repository", {}).get("id") != repo_id:
        return "a fork's run", None
    path = str(run.get("path", "")).split("@")[0]
    prover = PROVERS.get(path)
    if prover is None:
        return f"a run of {path or 'an unknown workflow'}, not {', '.join(PROVERS)}", None
    if path != CI:
        called = [str(r.get("path", "")).split("@")[0].lower()
                  for r in run.get("referenced_workflows") or []]
        if f"{repo}/{CI}".lower() not in called:
            return f"a run of {path} that did not call {CI}", None
    event, branch = run.get("event"), run.get("head_branch")
    if event not in prover["events"]:
        return f"a {event} run of {path}; only {', '.join(prover['events'])} count", None
    want = prover["events"][event]
    if want == "tag" and not (isinstance(branch, str) and TAG.match(branch)):
        return f"a {event} run of {path} on {branch!r}, not a v<digit> tag", None
    if want == "main" and branch != "main":
        return f"a {event} run of {path} on {branch!r}, not main", None
    if not isinstance(run.get("head_sha"), str) or not re.match(r"^[0-9a-f]{40}$", run["head_sha"]):
        return "a run with no head commit", None
    url = run.get("html_url")
    if not isinstance(url, str) or not url.startswith("https://"):
        return "a run with no URL", None
    return None, prover


def refuse_ref(repo, run, prover):
    """The run's commit is where its ref says: on main, or the v* tag's commit."""
    sha, branch = run["head_sha"], run["head_branch"]
    if prover["events"][run["event"]] == "main":
        # main only moves forward (assumption: it is never force-pushed), so a
        # commit that was main's tip is an ancestor of, or equal to, main now.
        cmp = gh_api(f"repos/{repo}/compare/{sha}...main", {"per_page": 1})
        if cmp.get("status") not in ("ahead", "identical") or cmp.get("behind_by") != 0:
            return f"its commit {sha} is not on main ({cmp.get('status')})"
        return None
    # A branch named like a tag is not a tag: the tag must exist and peel to
    # the run's commit. (A 404 for a missing tag is an API error: the gate runs.)
    obj = gh_api(f"repos/{repo}/git/ref/tags/{branch}")
    if not isinstance(obj, dict) or obj.get("ref") != f"refs/tags/{branch}":
        return f"refs/tags/{branch} is not a tag"
    target = obj["object"]
    for _ in range(4):
        if target.get("type") != "tag":
            break
        target = gh_api(f"repos/{repo}/git/tags/{target['sha']}")["object"]
    if target.get("type") != "commit" or target.get("sha") != sha:
        return f"tag {branch} peels to {target.get('type')} {target.get('sha')}, not {sha}"
    return None


def run_jobs(repo, run_id):
    jobs, page = [], 1
    while True:
        answer = gh_api(f"repos/{repo}/actions/runs/{run_id}/jobs",
                        {"filter": "all", "per_page": 100, "page": page})
        batch, total = answer["jobs"], answer["total_count"]
        if not isinstance(batch, list) or not isinstance(total, int):
            raise TypeError("jobs is not a list")
        jobs += batch
        if len(jobs) >= total or not batch:
            break
        page += 1
        if page > 10:
            raise LookupFailed(f"run {run_id}: more than 1000 jobs")
    if len(jobs) < total:
        raise LookupFailed(f"run {run_id}: {len(jobs)} of {total} jobs listed")
    return jobs


def refuse_job(jobs, job_name):
    named = [j for j in jobs if j.get("name") == job_name]
    if not named:
        return f"the run has no job {job_name!r}"
    attempts = [j.get("run_attempt") for j in named]
    if len(set(attempts)) != len(attempts):
        # A second job with the gate's name (a `name:` can collide) is not
        # the gate's job; the name proves nothing.
        return f"{len(named)} jobs named {job_name!r} in one attempt"
    if any(j.get("status") == "completed" and j.get("conclusion") == "success" for j in named):
        return None
    seen = ", ".join(sorted({str(j.get("conclusion") or j.get("status")) for j in named}))
    return f"job {job_name!r} did not pass ({seen})"


def refuse_tree(gmap, ws, repo, name, value, sha):
    answer = gh_api(f"repos/{repo}/git/trees/{sha}", {"recursive": 1})
    if answer.get("truncated") is not False:
        return f"the tree of {sha} is truncated"
    entries = {}
    for entry in answer["tree"]:
        if entry["type"] in ("blob", "commit"):
            entries[entry["path"]] = (entry["mode"], entry["sha"])
    try:
        recomputed = gate_hash(gmap, ws, Tree(entries), name)
    except MapError as err:
        return f"the gate has no hash at {sha} ({err})"
    if recomputed != value:
        return f"its inputs at {sha} hash to {recomputed}"
    return None


def lookup(gmap, ws, name, value):
    if name not in gmap["gates"]:
        raise MapError(f"unknown gate {name!r}")
    if not HEX64.match(value):
        raise MapError(f"hash {value!r} is not 64 hex digits")
    if gmap["gates"][name].get("always"):
        print(f"ci-gate: {name}: always = true; it never skips", file=sys.stderr)
        return EXIT_MISS
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    try:
        repo_id = int(os.environ.get("GITHUB_REPOSITORY_ID", ""))
    except ValueError:
        raise LookupFailed("GITHUB_REPOSITORY_ID is not set")
    if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", repo):
        raise LookupFailed("GITHUB_REPOSITORY is not set")
    job = gmap["gates"][name].get("job", name)
    marker = f"gate-{name}-{value}"
    now = datetime.datetime.now(datetime.timezone.utc)
    try:
        listing = gh_api(f"repos/{repo}/actions/artifacts", {"name": marker, "per_page": 100})
        artifacts = listing["artifacts"]
        if not isinstance(artifacts, list):
            raise TypeError("artifacts is not a list")
        artifacts = sorted(artifacts, key=lambda a: str(a.get("created_at", "")), reverse=True)
        for artifact in artifacts:
            def refused(reason):
                print(f"ci-gate: marker {artifact.get('id')} refused: {reason}", file=sys.stderr)
            reason = refuse_artifact(artifact, marker, repo_id, now)
            if reason:
                refused(reason)
                continue
            run_id = artifact["workflow_run"]["id"]
            run = gh_api(f"repos/{repo}/actions/runs/{run_id}")
            reason, prover = refuse_run(run, repo, repo_id)
            if reason:
                refused(reason)
                continue
            reason = refuse_job(run_jobs(repo, run_id), prover["prefix"] + job)
            if reason:
                refused(reason)
                continue
            reason = refuse_ref(repo, run, prover)
            if reason:
                refused(reason)
                continue
            reason = refuse_tree(gmap, ws, repo, name, value, run["head_sha"])
            if reason:
                refused(reason)
                continue
            print(f"proof-run={run['html_url']}")
            print(f"proof-commit={run['head_sha']}")
            print(f"artifact={artifact['id']}")
            return EXIT_HIT
    except (KeyError, TypeError, AttributeError, ValueError) as err:
        raise LookupFailed(f"unexpected API answer ({err!r})")
    print(f"ci-gate: {name}: no marker {marker} proves it", file=sys.stderr)
    return EXIT_MISS


def explain(gmap, ws, name, value, run_id, job):
    """Diagnostics only (ci-trace.yml): each proof check against one run,
    whatever the run is. `decide` never calls it; it proves nothing."""
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    repo_id = int(os.environ.get("GITHUB_REPOSITORY_ID", "0") or 0)
    try:
        run = gh_api(f"repos/{repo}/actions/runs/{run_id}")
        reason, prover = refuse_run(run, repo, repo_id)
        print(f"run: {reason or 'ok'}")
        name_ = job or (prover["prefix"] if prover else "") + gmap["gates"][name].get("job", name)
        print(f"job {name_!r}: {refuse_job(run_jobs(repo, run_id), name_) or 'ok'}")
        if prover:
            print(f"ref {run.get('head_branch')}: {refuse_ref(repo, run, prover) or 'ok'}")
        print(f"tree {run.get('head_sha')}: "
              f"{refuse_tree(gmap, ws, repo, name, value, run['head_sha']) or 'ok'}")
    except (KeyError, TypeError, AttributeError, ValueError) as err:
        raise LookupFailed(f"unexpected API answer ({err!r})")
    return EXIT_OK


def record(gmap, name, value, directory):
    if name not in gmap["gates"]:
        raise MapError(f"unknown gate {name!r}")
    if not HEX64.match(value):
        raise MapError(f"hash {value!r} is not 64 hex digits")
    repo_id = os.environ.get("GITHUB_REPOSITORY_ID", "")
    if os.environ.get("GITHUB_EVENT_NAME", "").startswith("pull_request"):
        try:
            with open(os.environ["GITHUB_EVENT_PATH"]) as fh:
                head = json.load(fh)["pull_request"]["head"]["repo"]["id"]
        except (KeyError, OSError, ValueError, TypeError):
            head = None
        if str(head) != repo_id:
            print(f"ci-gate: {name}: a fork's pull request proves nothing; no marker", file=sys.stderr)
            return EXIT_OK
    marker = f"gate-{name}-{value}"
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, marker + ".json")
    body = {
        "scheme": SCHEME,
        "gate": name,
        "hash": value,
        "commit": os.environ.get("GITHUB_SHA", ""),
        "repository": os.environ.get("GITHUB_REPOSITORY", ""),
        "repository_id": repo_id,
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        "workflow_ref": os.environ.get("GITHUB_WORKFLOW_REF", ""),
        "event": os.environ.get("GITHUB_EVENT_NAME", ""),
        "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    with open(path, "w") as fh:
        json.dump(body, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"name={marker}")
    print(f"path={path}")
    return EXIT_OK


def describe(gmap, ws, tree):
    out = {"global": gmap["global"]["paths"], "provers": sorted(PROVERS),
           "not_gates": sorted(gmap.get("not_gates", {})), "gates": {}}
    for name, gate in gmap["gates"].items():
        files, empty = inputs(gmap, ws, tree, name, strict=False)
        out["gates"][name] = {
            "job": gate.get("job", name),
            "covers": gate.get("covers", []),
            "always": gate.get("always", False),
            "crates": gate.get("crates", []),
            "tests": gate.get("tests", []),
            "rows": gate.get("rows", []),
            "except": gate.get("except", []),
            "patterns": patterns(gmap, ws, name),
            "empty": empty,
            "files": sorted(files),
            "exists": sorted(presence(gmap, ws, tree, name, files)),
        }
    out["unclaimed"] = sorted(unclaimed(gmap, ws, tree))
    return out


def shard_filter(gmap, name):
    """The nextest filterset an e2e shard runs: its whole binaries less their
    `except` tests, and its `rows` (FX-53). ci.yml's shards and the traced run
    both take it from here, so the map's lists are what runs."""
    if name not in gmap["gates"]:
        raise MapError(f"unknown gate {name!r}")
    gate = gmap["gates"][name]
    if not gate.get("tests") and not gate.get("rows"):
        raise MapError(f"gate {name} names no e2e binary or row")
    def row(r):
        binary, test = r.split("::", 1)
        return f"(binary(={binary}) & test(={test}))"
    parts = []
    if gate.get("tests"):
        whole = " | ".join(f"binary(={t})" for t in gate["tests"])
        excepted = gate.get("except", [])
        if excepted:
            whole = f"({whole}) & not ({' | '.join(row(x) for x in excepted)})"
        parts.append(f"({whole})")
    parts.extend(row(r) for r in gate.get("rows", []))
    return " | ".join(parts)


def main():
    if not ARGS:
        die(EXIT_ERROR, "usage: ci-gate.sh hash|files|lookup|record|decide|gates|unclaimed|describe …")
    command, rest = ARGS[0], ARGS[1:]
    try:
        gmap = load_map()
        if command == "gates":
            print("\n".join(gmap["gates"]))
            return EXIT_OK
        if command == "filter" and len(rest) == 1:
            print(shard_filter(gmap, rest[0]))
            return EXIT_OK
        if command == "record" and len(rest) == 3:
            return record(gmap, rest[0], rest[1], rest[2])
        if command == "explain" and len(rest) >= 3:
            job = rest[rest.index("--job") + 1] if "--job" in rest else ""
            require_clean()
            return explain(gmap, Workspace(), rest[0], rest[1], rest[2], job)
        if command == "lookup" and len(rest) == 2:
            require_clean()
            return lookup(gmap, Workspace(), rest[0], rest[1])
        ws, tree = Workspace(), Tree()
        if command == "hash" and len(rest) == 1:
            require_clean()
            print(gate_hash(gmap, ws, tree, rest[0]))
            return EXIT_OK
        if command == "exists" and len(rest) == 1:
            require_clean()
            files = inputs(gmap, ws, tree, rest[0])[0]
            print("\n".join(sorted(presence(gmap, ws, tree, rest[0], files))))
            return EXIT_OK
        if command == "files" and len(rest) == 1:
            require_clean()
            print("\n".join(sorted(inputs(gmap, ws, tree, rest[0])[0])))
            return EXIT_OK
        if command == "match" and rest:
            json.dump({p: sorted(tree.match(p)) for p in rest}, sys.stdout, indent=1)
            print()
            return EXIT_OK
        if command == "unclaimed" and not rest:
            print("\n".join(sorted(unclaimed(gmap, ws, tree))))
            return EXIT_OK
        if command == "describe" and not rest:
            json.dump(describe(gmap, ws, tree), sys.stdout, indent=1)
            print()
            return EXIT_OK
    except MapError as err:
        die(EXIT_MAP, str(err))
    except LookupFailed as err:
        die(EXIT_ERROR, f"lookup failed, so the gate runs: {err}")
    die(EXIT_ERROR, f"usage: unknown command or arguments: {' '.join(ARGS)}")


sys.exit(main())
PY
