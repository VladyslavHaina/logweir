#!/usr/bin/env bash
# Content-addressed gate results (FX-53 §2.1, §2.2). A CI gate (a job) is
# skipped only when a MARKER proves that the same gate passed on the same
# inputs in a same-repository run of ci.yml; otherwise it runs and leaves one.
#
#   ci-gate.sh hash <gate> [--runner <label>]   the gate's input hash (hex)
#   ci-gate.sh files <gate>                     the tracked files that hash covers
#   ci-gate.sh lookup <gate> <hash>             0 = a marker proves it (prints
#                                               proof-run=<url>); anything else = run
#   ci-gate.sh record <gate> <hash> <dir>       write the marker file into <dir>;
#                                               prints name=… and path=… (nothing
#                                               for a fork's pull request)
#   ci-gate.sh decide <gate> [force]            hash, then lookup unless forced;
#                                               writes hit, proof-run and hash to
#                                               $GITHUB_OUTPUT. Always exits 0.
#   ci-gate.sh gates | unclaimed | describe     the map, the unclassified files,
#                                               and the whole map as JSON (lint)
#   ci-gate.sh match <pattern>…                 the tracked files each pattern names (JSON)
#
# THE HASH is sha256 over a scheme line, the gate's name, the runner label, and
# `<mode> <blob id> <path>` of every tracked input (`git ls-files -s`, sorted).
# It reads the index and `cargo metadata --no-deps`; it builds nothing. The
# inputs are ci/gates.toml's expansion of the gate: [global] (the lockfile,
# toolchain, root manifest, the map, this script, the gate action, ci.yml),
# its definition, crate closures, paths, readers, walks and tests, plus every
# tracked file no gate classifies.
#
# FAIL SAFE. Exit 0 from `lookup` is the only "skip". A miss is 1; a refused
# marker (a fork's run, another workflow, expired) is a miss; an API error, a
# map error, a pattern that matches no tracked file (a stale definition), or
# an unknown gate exits 2 or 3. The gate action runs the gate on anything but 0.
# The runner label is $CI_GATE_RUNNER, or RUNNER_OS-RUNNER_ARCH-ImageOS on a
# GitHub runner, or `local`. The lookup uses `gh api` (CI_GATE_GH overrides
# it) with $GH_TOKEN, which needs `actions: read`; cargo is CI_GATE_CARGO,
# CARGO, or `cargo`.
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

SCHEME = "ci-gate/1"
MAP = "ci/gates.toml"
GATE_KEYS = {"covers", "definition", "crates", "paths", "readers", "walks", "tests", "always"}
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
        for key in GATE_KEYS - {"always"}:
            value = gate.get(key, [])
            if not isinstance(value, list) or not all(isinstance(v, str) and v for v in value):
                raise MapError(f"{MAP}: gate {name}: {key} must be a list of strings")
        if not isinstance(gate.get("always", False), bool):
            raise MapError(f"{MAP}: gate {name}: always must be true or false")
    if not isinstance(gmap.get("global", {}).get("paths"), list):
        raise MapError(f"{MAP}: [global].paths is missing")
    workflows = gmap.get("marker", {}).get("workflows")
    if not isinstance(workflows, list) or not workflows:
        raise MapError(f"{MAP}: [marker].workflows is missing")
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
    """The tracked files (`git ls-files -s`: the index, which a checkout
    makes equal to the commit), matched by compile_glob."""

    def __init__(self):
        self.cache = {}
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
        "tests": [f"e2e/tests/{t}.rs" for t in gate.get("tests", [])],
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
            files |= matched
    if strict and empty:
        # A definition or input that names nothing is stale: no sound hash.
        raise MapError(f"gate {name}: no tracked file matches " + "; ".join(empty))
    if not gmap["gates"][name].get("definition"):
        raise MapError(f"gate {name}: no definition")
    return files | unclaimed(gmap, ws, tree), empty


def runner_label(args):
    if "--runner" in args:
        i = args.index("--runner")
        if i + 1 >= len(args) or not args[i + 1]:
            die(EXIT_ERROR, "--runner needs a label")
        return args[i + 1]
    if os.environ.get("CI_GATE_RUNNER"):
        return os.environ["CI_GATE_RUNNER"]
    if os.environ.get("RUNNER_OS"):
        return "-".join(os.environ.get(k, "") for k in ("RUNNER_OS", "RUNNER_ARCH", "ImageOS"))
    return "local"


def gate_hash(gmap, ws, tree, name, runner):
    files, _ = inputs(gmap, ws, tree, name)
    digest = hashlib.sha256()
    digest.update(f"{SCHEME}\ngate {name}\nrunner {runner}\n".encode())
    for path in sorted(files):
        mode, blob = tree.all[path]
        digest.update(f"{mode} {blob} {path}\0".encode())
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


def refuse_run(run, repo, repo_id, workflows):
    if run.get("repository", {}).get("id") != repo_id:
        return "run of another repository"
    if run.get("head_repository", {}).get("id") != repo_id:
        return "a fork's run"
    path = str(run.get("path", "")).split("@")[0]
    if path in workflows:
        return None
    prefixes = [f"{repo}/{w}".lower() for w in workflows]
    for ref in run.get("referenced_workflows") or []:
        if str(ref.get("path", "")).split("@")[0].lower() in prefixes:
            return None
    return f"run of {path or 'an unknown workflow'}, not {', '.join(workflows)}"


def lookup(gmap, name, value):
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
    workflows = gmap["marker"]["workflows"]
    marker = f"gate-{name}-{value}"
    now = datetime.datetime.now(datetime.timezone.utc)
    try:
        listing = gh_api(f"repos/{repo}/actions/artifacts", {"name": marker, "per_page": 100})
        artifacts = listing["artifacts"]
        if not isinstance(artifacts, list):
            raise TypeError("artifacts is not a list")
        artifacts = sorted(artifacts, key=lambda a: str(a.get("created_at", "")), reverse=True)
        for artifact in artifacts:
            reason = refuse_artifact(artifact, marker, repo_id, now)
            if reason:
                print(f"ci-gate: marker {artifact.get('id')} refused: {reason}", file=sys.stderr)
                continue
            run = gh_api(f"repos/{repo}/actions/runs/{artifact['workflow_run']['id']}")
            reason = refuse_run(run, repo, repo_id, workflows)
            if reason:
                print(f"ci-gate: marker {artifact.get('id')} refused: {reason}", file=sys.stderr)
                continue
            url = run.get("html_url")
            if not isinstance(url, str) or not url.startswith("https://"):
                raise LookupFailed(f"run {run.get('id')} has no URL")
            print(f"proof-run={url}")
            print(f"artifact={artifact['id']}")
            return EXIT_HIT
    except (KeyError, TypeError, AttributeError, ValueError) as err:
        raise LookupFailed(f"unexpected API answer ({err!r})")
    print(f"ci-gate: {name}: no marker {marker}", file=sys.stderr)
    return EXIT_MISS


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
    out = {"global": gmap["global"]["paths"], "workflows": gmap["marker"]["workflows"],
           "not_gates": sorted(gmap.get("not_gates", {})), "gates": {}}
    for name, gate in gmap["gates"].items():
        files, empty = inputs(gmap, ws, tree, name, strict=False)
        out["gates"][name] = {
            "covers": gate.get("covers", []),
            "always": gate.get("always", False),
            "crates": gate.get("crates", []),
            "tests": gate.get("tests", []),
            "patterns": patterns(gmap, ws, name),
            "empty": empty,
            "files": sorted(files),
        }
    out["unclaimed"] = sorted(unclaimed(gmap, ws, tree))
    return out


def main():
    if not ARGS:
        die(EXIT_ERROR, "usage: ci-gate.sh hash|files|lookup|record|decide|gates|unclaimed|describe …")
    command, rest = ARGS[0], ARGS[1:]
    try:
        gmap = load_map()
        if command == "gates":
            print("\n".join(gmap["gates"]))
            return EXIT_OK
        if command == "lookup" and len(rest) == 2:
            return lookup(gmap, rest[0], rest[1])
        if command == "record" and len(rest) == 3:
            return record(gmap, rest[0], rest[1], rest[2])
        ws, tree = Workspace(), Tree()
        if command == "hash" and rest and not rest[0].startswith("-"):
            print(gate_hash(gmap, ws, tree, rest[0], runner_label(rest[1:])))
            return EXIT_OK
        if command == "files" and len(rest) == 1:
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
