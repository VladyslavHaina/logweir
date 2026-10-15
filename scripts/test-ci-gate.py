#!/usr/bin/env python3
"""Unit rows for scripts/ci-gate.sh (FX-53 §2.1, §2.2, §4 item 2).

Each row builds a throwaway git repository with a small gate map, a fake
`cargo` (its `metadata`) and a fake `gh` (the artifacts and runs API), so no
toolchain, network or token is used. The last row holds the script's glob
matcher to git's own `:(glob)` pathspec for every pattern in the real map.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "ci-gate.sh"
REPO_ID = 1359519835
REPO = "VladyslavHaina/logweir"
FORK_ID = 42
FUTURE = "2999-01-01T00:00:00Z"
PAST = "2001-01-01T00:00:00Z"

GATES = """\
schema = 1

[global]
paths = ["Cargo.lock", "Cargo.toml", "rust-toolchain.toml", ".cargo/**", "ci/gates.toml",
         "scripts/ci-gate.sh", ".github/actions/**", ".github/workflows/ci.yml",
         ".github/workflows/release.yml", ".github/workflows/ci-nightly.yml"]

[not_gates]
changes = "plumbing"

[gates.alpha]
definition = ["scripts/alpha.sh"]
crates = ["alpha"]
readers = ["docs/**"]

[gates.beta]
definition = ["scripts/beta.sh"]
crates = ["beta"]
paths = ["web/**"]

[gates.walker]
definition = ["scripts/walker.sh"]
walks = ["**"]
paths = ["examples/*.yaml"]

[gates.clock]
always = true
definition = ["scripts/clock.sh"]
paths = ["web/package.json"]
"""

FILES = {
    "Cargo.toml": "[workspace]\nmembers = ['crates/*']\n",
    "Cargo.lock": "version = 4\n",
    "rust-toolchain.toml": "[toolchain]\nchannel = '1.89.0'\n",
    ".cargo/config.toml": "[alias]\n",
    ".github/workflows/ci.yml": "name: ci\n",
    ".github/workflows/release.yml": "name: release\n",
    ".github/workflows/ci-nightly.yml": "name: ci-nightly\n",
    ".github/actions/gate/action.yml": "name: gate\n",
    "crates/alpha/Cargo.toml": "[package]\nname = 'alpha'\n",
    "crates/alpha/src/lib.rs": "pub fn a() {}\n",
    "crates/beta/Cargo.toml": "[package]\nname = 'beta'\n",
    "crates/beta/src/lib.rs": "pub fn b() {}\n",
    "crates/beta/tests/t.rs": "#[test] fn t() {}\n",
    "docs/guide.md": "# guide\n",
    "web/index.html": "<html></html>\n",
    "web/package.json": "{}\n",
    "examples/drill.yaml": "a: 1\n",
    "examples/nested/deep.yaml": "b: 2\n",
    "scripts/alpha.sh": "true\n",
    "scripts/beta.sh": "true\n",
    "scripts/walker.sh": "true\n",
    "scripts/clock.sh": "true\n",
}

# `cargo metadata --no-deps` for the two crates: beta depends on alpha.
FAKE_CARGO = r'''#!/usr/bin/env python3
import json, os, sys
root = os.getcwd()
def pkg(name, deps):
    return {"name": name, "manifest_path": f"{root}/crates/{name}/Cargo.toml",
            "dependencies": [{"name": d, "kind": k, "path": f"{root}/crates/{d}"} for d, k in deps]
                            + [{"name": "serde", "kind": None}]}
if os.environ.get("FAKE_CARGO_FAIL"):
    print("error: failed to parse manifest", file=sys.stderr)
    sys.exit(101)
assert sys.argv[1] == "metadata" and "--no-deps" in sys.argv, sys.argv
print(json.dumps({"workspace_root": root,
                  "packages": [pkg("alpha", []), pkg("beta", [("alpha", None)])]}))
'''

# The API, from a JSON file of {path: answer}; every call is logged.
FAKE_GH = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ["FAKE_GH_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if os.environ.get("FAKE_GH_FAIL"):
    print("HTTP 502: Bad Gateway", file=sys.stderr)
    sys.exit(1)
assert args[:3] == ["api", "-X", "GET"], args
answers = json.load(open(os.environ["FAKE_GH_ANSWERS"]))
answer = answers.get(args[3])
if answer is None:
    print("HTTP 404: Not Found", file=sys.stderr)
    sys.exit(1)
print(answer if isinstance(answer, str) else json.dumps(answer))
'''


CI = ".github/workflows/ci.yml"
RELEASE = ".github/workflows/release.yml"
NIGHTLY = ".github/workflows/ci-nightly.yml"
CALLS_CI = (f"{REPO}/{CI}@refs/heads/main",)


def run_of(run_id, sha, path=CI, head_id=REPO_ID, referenced=(), event="push", branch="main"):
    return {"id": run_id, "path": path, "html_url": f"https://github.com/{REPO}/actions/runs/{run_id}",
            "repository": {"id": REPO_ID}, "head_repository": {"id": head_id},
            "referenced_workflows": [{"path": p} for p in referenced],
            "event": event, "head_branch": branch, "head_sha": sha}


def job(name, conclusion="success", status="completed", attempt=1):
    return {"name": name, "status": status, "conclusion": conclusion, "run_attempt": attempt}


def artifact(name, run_id, art_id=7, head_id=REPO_ID, expired=False, expires=FUTURE):
    return {"id": art_id, "name": name, "expired": expired, "expires_at": expires,
            "created_at": "2026-10-15T00:00:00Z",
            "workflow_run": {"id": run_id, "repository_id": REPO_ID, "head_repository_id": head_id}}


class Repo:
    def __init__(self):
        self.dir = Path(tempfile.mkdtemp(prefix="ci-gate-"))
        self.tools = Path(tempfile.mkdtemp(prefix="ci-gate-tools-"))
        for rel, text in FILES.items():
            self.write(rel, text)
        self.write("ci/gates.toml", GATES)
        (self.dir / "scripts").mkdir(exist_ok=True)
        shutil.copy(SCRIPT, self.dir / "scripts" / "ci-gate.sh")
        for name, text in (("cargo", FAKE_CARGO), ("gh", FAKE_GH)):
            path = self.tools / name
            path.write_text(text)
            path.chmod(0o755)
        self.answers = {}
        self.git("init", "-q")
        self.commit("tree")

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        shutil.rmtree(self.tools, ignore_errors=True)

    def git(self, *args):
        subprocess.run(["git", "-C", str(self.dir), *args], check=True, capture_output=True,
                       env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"})

    def write(self, rel, text):
        path = self.dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", message)

    def env(self, **extra):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("GITHUB_", "RUNNER_", "CI_GATE_", "FAKE_")) and k not in ("ImageOS", "CARGO")}
        answers = self.tools / "answers.json"
        answers.write_text(json.dumps(self.answers))
        env.update(CI_GATE_CARGO=str(self.tools / "cargo"), CI_GATE_GH=str(self.tools / "gh"),
                   FAKE_GH_ANSWERS=str(answers), FAKE_GH_LOG=str(self.tools / "gh.log"),
                   GITHUB_REPOSITORY=REPO, GITHUB_REPOSITORY_ID=str(REPO_ID))
        env.update(extra)
        return env

    def run(self, *args, **extra):
        return subprocess.run(["bash", str(self.dir / "scripts" / "ci-gate.sh"), *args],
                              cwd=self.dir, capture_output=True, text=True, env=self.env(**extra))

    def hash(self, gate):
        proc = self.run("hash", gate)
        if proc.returncode != 0:
            raise AssertionError(f"hash {gate}: {proc.returncode} {proc.stderr}")
        return proc.stdout.strip()

    def hashes(self):
        return {g: self.hash(g) for g in ("alpha", "beta", "walker", "clock")}

    def head(self, rev="HEAD"):
        return subprocess.run(["git", "-C", str(self.dir), "rev-parse", rev], check=True,
                              capture_output=True, text=True).stdout.strip()

    def tree(self, sha):
        """The git trees API's answer for a commit, recursive, directories included."""
        out = subprocess.run(["git", "-C", str(self.dir), "ls-tree", "-r", "-t", "--full-tree", sha],
                             check=True, capture_output=True, text=True).stdout
        entries = []
        for line in out.splitlines():
            meta, path = line.split("\t", 1)
            mode, kind, blob = meta.split(" ")
            entries.append({"path": path, "mode": mode, "type": kind, "sha": blob})
        return {"sha": sha, "truncated": False, "tree": entries}

    def gh_calls(self):
        log = self.tools / "gh.log"
        return [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []


class Hash(unittest.TestCase):
    def setUp(self):
        self.repo = Repo()
        self.before = self.repo.hashes()

    def tearDown(self):
        self.repo.cleanup()

    def changed(self):
        after = self.repo.hashes()
        return {g for g in after if after[g] != self.before[g]}

    def test_the_same_inputs_give_the_same_hash(self):
        self.assertEqual(self.repo.hashes(), self.before)
        for value in self.before.values():
            self.assertRegex(value, r"^[0-9a-f]{64}$")
        self.assertEqual(len(set(self.before.values())), 4, "each gate hashes its own name")
        clone = Path(tempfile.mkdtemp(prefix="ci-gate-clone-"))
        try:
            subprocess.run(["git", "clone", "-q", str(self.repo.dir), str(clone / "r")], check=True)
            proc = subprocess.run(["bash", "scripts/ci-gate.sh", "hash", "alpha"], cwd=clone / "r",
                                  capture_output=True, text=True, env=self.repo.env())
            self.assertEqual(proc.stdout.strip(), self.before["alpha"], proc.stderr)
        finally:
            shutil.rmtree(clone, ignore_errors=True)

    def test_an_unknown_path_changes_every_hash(self):
        self.repo.write("newdir/thing.txt", "x\n")
        self.repo.commit("an unclassified path")
        self.assertEqual(self.changed(), {"alpha", "beta", "walker", "clock"})
        # examples/nested/ is read only by a walk, which classifies nothing.
        self.assertEqual(self.repo.run("unclaimed").stdout.split(),
                         ["examples/nested/deep.yaml", "newdir/thing.txt"])

    def test_an_edit_to_an_unknown_path_changes_every_hash(self):
        self.repo.write("newdir/thing.txt", "x\n")
        self.repo.commit("an unclassified path")
        self.before = self.repo.hashes()
        self.repo.write("newdir/thing.txt", "y\n")
        self.repo.commit("edit it")
        self.assertEqual(self.changed(), {"alpha", "beta", "walker", "clock"})

    def test_a_ci_definition_change_changes_every_hash(self):
        for rel in (".github/workflows/ci.yml", ".github/workflows/release.yml",
                    ".github/workflows/ci-nightly.yml", "ci/gates.toml", "scripts/ci-gate.sh",
                    ".github/actions/gate/action.yml", "Cargo.lock", "rust-toolchain.toml",
                    "Cargo.toml", ".cargo/config.toml"):
            with self.subTest(rel=rel):
                self.before = self.repo.hashes()
                path = self.repo.dir / rel
                path.write_text(path.read_text() + "\n# edited\n")
                self.repo.commit(f"edit {rel}")
                self.assertEqual(self.changed(), {"alpha", "beta", "walker", "clock"})

    def test_one_reader_file_changes_exactly_the_gates_that_declare_it(self):
        self.repo.write("docs/guide.md", "# guide, edited\n")
        self.repo.commit("docs")
        # alpha declares docs/** as a reader; walker walks the tree.
        self.assertEqual(self.changed(), {"alpha", "walker"})

    def test_a_new_file_under_a_declared_glob_changes_only_its_gates(self):
        self.repo.write("web/new.js", "1\n")
        self.repo.commit("web")
        self.assertEqual(self.changed(), {"beta", "walker"})

    def test_a_crate_change_reaches_its_reverse_dependencies(self):
        self.repo.write("crates/alpha/src/lib.rs", "pub fn a() { }\n")
        self.repo.commit("alpha")
        self.assertEqual(self.changed(), {"alpha", "beta", "walker"})
        self.before = self.repo.hashes()
        self.repo.write("crates/beta/tests/t.rs", "#[test] fn t() { }\n")
        self.repo.commit("beta")
        self.assertEqual(self.changed(), {"beta", "walker"})

    def test_a_star_stops_at_a_slash(self):
        self.repo.write("examples/nested/deep.yaml", "b: 3\n")
        self.repo.commit("nested")
        # examples/*.yaml does not name examples/nested/; only the walk reads it,
        # and an unclassified file is every gate's input.
        self.assertEqual(self.changed(), {"alpha", "beta", "walker", "clock"})

    def test_the_runner_is_not_hashed(self):
        # ci.yml's runs-on (a global input) binds a gate's runner.
        proc = self.repo.run("hash", "alpha", RUNNER_OS="Linux", RUNNER_ARCH="ARM64", ImageOS="ubuntu24")
        self.assertEqual(proc.stdout.strip(), self.before["alpha"])

    def test_an_untracked_file_is_not_an_input(self):
        (self.repo.dir / "lookup.out").write_text("scratch\n")
        self.assertEqual(self.repo.hashes(), self.before)


class FailSafe(unittest.TestCase):
    def setUp(self):
        self.repo = Repo()

    def tearDown(self):
        self.repo.cleanup()

    def assert_refused(self, proc, code=3):
        self.assertEqual(proc.returncode, code, proc.stdout + proc.stderr)
        self.assertEqual(proc.stdout, "")

    def test_a_stale_definition_gives_no_hash(self):
        (self.repo.dir / "scripts" / "beta.sh").unlink()
        self.repo.commit("drop beta's script")
        proc = self.repo.run("hash", "beta")
        self.assert_refused(proc)
        self.assertIn("definition: scripts/beta.sh", proc.stderr)
        # The other gates still hash.
        self.repo.hash("alpha")

    def test_a_declared_path_that_matches_nothing_gives_no_hash(self):
        shutil.rmtree(self.repo.dir / "web")
        self.repo.commit("drop web")
        self.assert_refused(self.repo.run("hash", "beta"))
        self.assert_refused(self.repo.run("hash", "clock"))

    def test_a_map_error_gives_no_hash(self):
        cases = {
            "an unknown key": GATES.replace("readers = [\"docs/**\"]", "reader = [\"docs/**\"]"),
            "an unknown crate": GATES.replace("crates = [\"beta\"]", "crates = [\"gamma\"]"),
            "an unknown pattern syntax": GATES.replace("web/**", "web/[ab]*"),
            "another schema": GATES.replace("schema = 1", "schema = 2"),
            "no definition": GATES.replace("definition = [\"scripts/alpha.sh\"]\n", ""),
            "unparseable": GATES + "[gates.x\n",
        }
        for label, text in cases.items():
            with self.subTest(label):
                self.repo.write("ci/gates.toml", text)
                self.repo.commit(label)
                self.assert_refused(self.repo.run("hash", "alpha"))

    def test_a_dirty_checkout_gives_no_hash(self):
        guide = self.repo.dir / "docs" / "guide.md"
        cases = {"a tracked change in the worktree": ("rewritten by a job\n", False),
                 "a staged change": ("staged\n", True)}
        for label, (text, stage) in cases.items():
            with self.subTest(label):
                guide.write_text(text)
                if stage:
                    self.repo.git("add", "docs/guide.md")
                for args in (("hash", "alpha"), ("files", "alpha"), ("lookup", "alpha", "0" * 64)):
                    proc = self.repo.run(*args)
                    self.assert_refused(proc)
                    self.assertIn("differs from HEAD", proc.stderr)
                out = self.repo.tools / "out"
                out.write_text("")
                self.repo.run("decide", "alpha", GITHUB_OUTPUT=str(out))
                self.assertEqual(out.read_text(), "hit=false\nproof-run=\nhash=\n")
                self.repo.git("reset", "-q", "--hard")

    def test_an_unknown_gate_gives_no_hash(self):
        self.assert_refused(self.repo.run("hash", "nope"))

    def test_cargo_metadata_failing_gives_no_hash(self):
        self.assert_refused(self.repo.run("hash", "alpha", FAKE_CARGO_FAIL="1"))

    def test_decide_without_a_hash_runs_the_gate(self):
        (self.repo.dir / "scripts" / "beta.sh").unlink()
        self.repo.commit("stale")
        out = self.repo.tools / "out"
        proc = self.repo.run("decide", "beta", GITHUB_OUTPUT=str(out))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(out.read_text(), "hit=false\nproof-run=\nhash=\n")
        self.assertEqual(self.repo.gh_calls(), [], "no hash, so no lookup")


class Lookup(unittest.TestCase):
    """A marker is an index; the run it names is the proof (FX-53 §2.2)."""

    def setUp(self):
        self.repo = Repo()
        self.hash = self.repo.hash("alpha")
        self.name = f"gate-alpha-{self.hash}"
        self.sha = self.repo.head()
        self.listing = f"repos/{REPO}/actions/artifacts"

    def tearDown(self):
        self.repo.cleanup()

    def serve(self, artifacts, runs, jobs=None, trees=None):
        """The API: artifacts, runs, each run's jobs (default: alpha passed),
        and each run's head tree (default: the commit's own)."""
        self.repo.answers = {self.listing: {"total_count": len(artifacts), "artifacts": artifacts}}
        for run in runs:
            self.repo.answers[f"repos/{REPO}/actions/runs/{run['id']}"] = run
            listed = (jobs or {}).get(run["id"], [job("alpha")])
            self.repo.answers[f"repos/{REPO}/actions/runs/{run['id']}/jobs"] = {
                "total_count": len(listed), "jobs": listed}
            tree = (trees or {}).get(run["head_sha"]) or self.repo.tree(run["head_sha"])
            self.repo.answers[f"repos/{REPO}/git/trees/{run['head_sha']}"] = tree
            self.repo.answers.setdefault(f"repos/{REPO}/compare/{run['head_sha']}...main",
                                         {"status": "identical", "behind_by": 0})

    def tag(self, name, sha, annotated=True):
        """refs/tags/<name>, annotated (a tag object) or lightweight, at sha."""
        if annotated:
            self.repo.answers[f"repos/{REPO}/git/ref/tags/{name}"] = {
                "ref": f"refs/tags/{name}", "object": {"type": "tag", "sha": "f" * 40}}
            self.repo.answers[f"repos/{REPO}/git/tags/{'f' * 40}"] = {"object": {"type": "commit", "sha": sha}}
        else:
            self.repo.answers[f"repos/{REPO}/git/ref/tags/{name}"] = {
                "ref": f"refs/tags/{name}", "object": {"type": "commit", "sha": sha}}

    def lookup(self, **extra):
        return self.repo.run("lookup", "alpha", self.hash, **extra)

    def assert_hit(self, proc, run_id):
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"proof-run=https://github.com/{REPO}/actions/runs/{run_id}", proc.stdout)

    def assert_miss(self, proc, why):
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertNotIn("proof-run=", proc.stdout)
        self.assertIn(why, proc.stderr)

    def test_a_true_marker_proves_the_gate(self):
        self.serve([artifact(self.name, 11)], [run_of(11, self.sha)])
        proc = self.lookup()
        self.assert_hit(proc, 11)
        self.assertIn(f"proof-commit={self.sha}", proc.stdout)
        calls = [c[3] for c in self.repo.gh_calls()]
        self.assertEqual(calls, [self.listing, f"repos/{REPO}/actions/runs/11",
                                 f"repos/{REPO}/actions/runs/11/jobs",
                                 f"repos/{REPO}/compare/{self.sha}...main",
                                 f"repos/{REPO}/git/trees/{self.sha}"])
        self.assertIn(f"name={self.name}", self.repo.gh_calls()[0])
        self.assertIn("filter=all", self.repo.gh_calls()[2], "every attempt's jobs")
        self.assertIn("recursive=1", self.repo.gh_calls()[4])

    def test_a_forged_marker_whose_gate_job_did_not_pass_runs_the_gate(self):
        for conclusion in ("failure", "cancelled", "skipped", None):
            with self.subTest(conclusion=conclusion):
                status = "completed" if conclusion else "in_progress"
                self.serve([artifact(self.name, 12)], [run_of(12, self.sha)],
                           jobs={12: [job("alpha", conclusion, status), job("beta")]})
                self.assert_miss(self.lookup(), "job 'alpha' did not pass")

    def test_a_marker_from_a_run_without_the_gate_s_job_runs_the_gate(self):
        # Uploaded by another job (a test, a package's install script).
        self.serve([artifact(self.name, 13)], [run_of(13, self.sha)], jobs={13: [job("beta")]})
        self.assert_miss(self.lookup(), "the run has no job 'alpha'")

    def test_a_pass_in_any_attempt_proves_the_gate(self):
        self.serve([artifact(self.name, 14)], [run_of(14, self.sha)],
                   jobs={14: [job("alpha", "failure"), job("alpha", attempt=2)]})
        self.assert_hit(self.lookup(), 14)

    def test_a_duplicated_job_name_proves_nothing(self):
        # A second job named like the gate's (a `name:` can collide) runs `true`.
        self.serve([artifact(self.name, 9)], [run_of(9, self.sha)],
                   jobs={9: [job("alpha", "failure"), job("alpha")]})
        self.assert_miss(self.lookup(), "2 jobs named 'alpha' in one attempt")

    def test_a_main_run_whose_commit_is_not_on_main_proves_nothing(self):
        self.serve([artifact(self.name, 8)], [run_of(8, self.sha)])
        self.repo.answers[f"repos/{REPO}/compare/{self.sha}...main"] = {"status": "diverged", "behind_by": 2}
        self.assert_miss(self.lookup(), "is not on main (diverged)")

    def test_a_marker_whose_hash_does_not_recompute_at_its_commit_runs_the_gate(self):
        old = self.sha
        self.repo.write("docs/guide.md", "# guide, later\n")
        self.repo.commit("docs")
        # The marker claims today's hash, but its run tested the older tree.
        self.hash = self.repo.hash("alpha")
        self.name = f"gate-alpha-{self.hash}"
        self.serve([artifact(self.name, 15)], [run_of(15, old)])
        self.assert_miss(self.lookup(), f"its inputs at {old} hash to")
        # A change outside the gate's inputs leaves the proof standing.
        self.repo.write("web/index.html", "<html>later</html>\n")
        self.repo.commit("web")
        self.serve([artifact(self.name, 16)], [run_of(16, self.repo.head("HEAD~1"))])
        self.assert_hit(self.lookup(), 16)

    def test_a_truncated_or_unreadable_tree_runs_the_gate(self):
        tree = self.repo.tree(self.sha)
        tree["truncated"] = True
        self.serve([artifact(self.name, 17)], [run_of(17, self.sha)], trees={self.sha: tree})
        self.assert_miss(self.lookup(), "is truncated")
        self.serve([artifact(self.name, 18)], [run_of(18, self.sha)])
        del self.repo.answers[f"repos/{REPO}/git/trees/{self.sha}"]
        self.assertEqual(self.lookup().returncode, 2)
        self.serve([artifact(self.name, 19)], [run_of(19, self.sha)])
        del self.repo.answers[f"repos/{REPO}/actions/runs/19/jobs"]
        self.assertEqual(self.lookup().returncode, 2)

    def test_only_the_events_ci_produces_count(self):
        pr = {"base": {"ref": "main", "sha": self.sha}, "head": {"ref": "x", "sha": self.sha}}
        refused = {
            # A pull request's run tests a merge commit the API does not name.
            "a pull request": dict(run_of(20, self.sha, event="pull_request", branch="x"), pull_requests=[pr]),
            "a pull request with no pull_requests": run_of(29, self.sha, event="pull_request", branch="x"),
            "a pull request with two": dict(run_of(19, self.sha, event="pull_request", branch="x"),
                                            pull_requests=[pr, pr]),
            "pull_request_target": run_of(21, self.sha, event="pull_request_target", branch="x"),
            "workflow_run": run_of(22, self.sha, event="workflow_run"),
            "a push to another branch": run_of(23, self.sha, branch="feature"),
            "a dispatch off main": run_of(24, self.sha, event="workflow_dispatch", branch="feature"),
            "a schedule of ci.yml": run_of(25, self.sha, event="schedule"),
            "release.yml pushed to main": run_of(26, self.sha, RELEASE, referenced=CALLS_CI),
            "release.yml on a v-named branch": run_of(27, self.sha, RELEASE, referenced=CALLS_CI, branch="vfoo"),
            "release.yml not calling ci.yml": run_of(28, self.sha, RELEASE, branch="v0.4.1"),
            "the nightly on a push": run_of(18, self.sha, NIGHTLY, referenced=CALLS_CI),
            "another workflow": run_of(17, self.sha, ".github/workflows/ci-trace.yml", event="pull_request"),
        }
        for label, run in refused.items():
            with self.subTest(label):
                self.serve([artifact(self.name, run["id"])], [run],
                           jobs={run["id"]: [job("alpha"), job("tests / alpha"), job("ci / alpha")]})
                self.assert_miss(self.lookup(), "refused")
        accepted = {
            "a push to main": (run_of(30, self.sha), "alpha"),
            "a dispatch on main": (run_of(31, self.sha, event="workflow_dispatch"), "alpha"),
            "a release tag": (run_of(32, self.sha, RELEASE, referenced=(f"{REPO}/{CI}@refs/tags/v0.4.1",),
                                     branch="v0.4.1"), "tests / alpha"),
            "a release dry run": (run_of(33, self.sha, RELEASE, referenced=CALLS_CI,
                                         event="workflow_dispatch"), "tests / alpha"),
            "the nightly": (run_of(34, self.sha, NIGHTLY, referenced=CALLS_CI, event="schedule"), "ci / alpha"),
        }
        for label, (run, job_name) in accepted.items():
            with self.subTest(label):
                self.serve([artifact(self.name, run["id"])], [run], jobs={run["id"]: [job(job_name)]})
                self.tag("v0.4.1", self.sha)
                self.assert_hit(self.lookup(), run["id"])
                # A called ci.yml's job carries its caller's prefix, and only that one.
                if job_name != "alpha":
                    self.serve([artifact(self.name, run["id"])], [run], jobs={run["id"]: [job("alpha")]})
                    self.tag("v0.4.1", self.sha)
                    self.assert_miss(self.lookup(), f"no job '{job_name}'")

    def test_a_release_run_proves_only_through_its_tag(self):
        run = run_of(35, self.sha, RELEASE, referenced=CALLS_CI, branch="v0.4.1")
        for label, setup, code in (
                ("a lightweight tag at the commit", lambda: self.tag("v0.4.1", self.sha, annotated=False), 0),
                ("a tag at another commit", lambda: self.tag("v0.4.1", "e" * 40), 1),
                ("a lightweight tag at another commit",
                 lambda: self.tag("v0.4.1", "e" * 40, annotated=False), 1),
                # a branch named v0.4.1 with no such tag: the refs API answers 404
                ("no tag", lambda: None, 2)):
            with self.subTest(label):
                self.serve([artifact(self.name, 35)], [run], jobs={35: [job("tests / alpha")]})
                setup()
                proc = self.lookup()
                self.assertEqual(proc.returncode, code, proc.stdout + proc.stderr)
                if code:
                    self.assertNotIn("proof-run=", proc.stdout)
                if code == 1:
                    self.assertIn("peels to commit eeee", proc.stderr)

    def test_no_marker_is_a_miss(self):
        self.serve([], [])
        self.assert_miss(self.lookup(), "no marker")

    def test_a_fork_marker_is_refused(self):
        self.serve([artifact(self.name, 40, head_id=FORK_ID)], [run_of(40, self.sha, head_id=FORK_ID)])
        self.assert_miss(self.lookup(), "a fork's run")
        self.assertEqual(len(self.repo.gh_calls()), 1, "a fork's marker is refused before its run is read")

    def test_a_fork_run_behind_a_same_repository_artifact_is_refused(self):
        self.serve([artifact(self.name, 41)], [run_of(41, self.sha, head_id=FORK_ID, event="pull_request")])
        self.assert_miss(self.lookup(), "a fork's run")

    def test_an_expired_marker_is_refused(self):
        self.serve([artifact(self.name, 42, expired=True), artifact(self.name, 43, art_id=8, expires=PAST)],
                   [run_of(42, self.sha), run_of(43, self.sha)])
        proc = self.lookup()
        self.assert_miss(proc, "expired")
        self.assertIn("past its expiry", proc.stderr)

    def test_a_marker_with_another_name_is_refused(self):
        self.serve([artifact(self.name.replace("alpha", "beta"), 44)], [run_of(44, self.sha)])
        self.assert_miss(self.lookup(), "named")

    def test_one_true_marker_among_refused_ones_proves_the_gate(self):
        self.serve([artifact(self.name, 45, art_id=1, head_id=FORK_ID), artifact(self.name, 46, art_id=2),
                    artifact(self.name, 47, art_id=3)],
                   [run_of(46, self.sha), run_of(47, self.sha)],
                   jobs={46: [job("alpha", "failure")], 47: [job("alpha")]})
        self.assert_hit(self.lookup(), 47)

    def test_a_lookup_error_runs_the_gate(self):
        self.serve([artifact(self.name, 48)], [run_of(48, self.sha)])
        cases = {
            "an API error": {"FAKE_GH_FAIL": "1"},
            "no repository id": {"GITHUB_REPOSITORY_ID": ""},
            "no repository": {"GITHUB_REPOSITORY": ""},
            "no gh": {"CI_GATE_GH": str(self.repo.tools / "absent")},
        }
        for label, extra in cases.items():
            with self.subTest(label):
                proc = self.lookup(**extra)
                self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
                self.assertNotIn("proof-run=", proc.stdout)

    def test_an_unexpected_answer_runs_the_gate(self):
        for label, listing in {"not JSON": "<html>", "no artifacts": {"total_count": 0},
                               "a wrong shape": {"artifacts": "x"}}.items():
            with self.subTest(label):
                self.repo.answers = {self.listing: listing}
                proc = self.lookup()
                self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        # The run cannot be read: an error, not a proof.
        self.serve([artifact(self.name, 49)], [])
        self.assertEqual(self.lookup().returncode, 2)

    def test_an_always_gate_never_skips(self):
        value = self.repo.hash("clock")
        self.serve([artifact(f"gate-clock-{value}", 50)], [run_of(50, self.sha)])
        proc = self.repo.run("lookup", "clock", value)
        self.assert_miss(proc, "always = true")
        self.assertEqual(self.repo.gh_calls(), [])

    def test_a_malformed_hash_is_refused(self):
        self.assertEqual(self.repo.run("lookup", "alpha", "abc").returncode, 3)

    def test_decide_hits_only_on_a_proof(self):
        out = self.repo.tools / "out"
        env_file = self.repo.tools / "env"
        self.serve([artifact(self.name, 51)], [run_of(51, self.sha)])
        proc = self.repo.run("decide", "alpha", "false", GITHUB_OUTPUT=str(out), GITHUB_ENV=str(env_file))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        url = f"https://github.com/{REPO}/actions/runs/51"
        self.assertEqual(out.read_text(), f"hit=true\nproof-run={url}\nhash={self.hash}\n")
        self.assertEqual(env_file.read_text(),
                         f"CI_GATE_NAME=alpha\nCI_GATE_HASH={self.hash}\nCI_GATE_HIT=true\n")
        for label, extra, args in (("an API error", {"FAKE_GH_FAIL": "1"}, ["false"]),
                                   ("forged", {}, ["false"]),
                                   ("forced", {}, ["true"])):
            with self.subTest(label):
                if label == "forged":
                    self.serve([artifact(self.name, 52)], [run_of(52, self.sha)],
                               jobs={52: [job("alpha", "failure")]})
                out.write_text("")
                calls = len(self.repo.gh_calls())
                proc = self.repo.run("decide", "alpha", *args, GITHUB_OUTPUT=str(out), **extra)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(out.read_text(), f"hit=false\nproof-run=\nhash={self.hash}\n")
                if label == "forced":
                    self.assertEqual(len(self.repo.gh_calls()), calls, "a forced gate does not look up")


class Record(unittest.TestCase):
    def setUp(self):
        self.repo = Repo()
        self.hash = self.repo.hash("alpha")
        self.out = self.repo.tools / "markers"

    def tearDown(self):
        self.repo.cleanup()

    def test_a_marker_names_the_gate_and_its_hash(self):
        proc = self.repo.run("record", "alpha", self.hash, str(self.out), GITHUB_SHA="a" * 40,
                             GITHUB_RUN_ID="99", GITHUB_EVENT_NAME="push")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        name = f"gate-alpha-{self.hash}"
        path = self.out / f"{name}.json"
        self.assertEqual(proc.stdout, f"name={name}\npath={path}\n")
        body = json.loads(path.read_text())
        self.assertEqual((body["gate"], body["hash"], body["commit"], body["run_id"]),
                         ("alpha", self.hash, "a" * 40, "99"))

    def test_a_fork_pull_request_records_nothing(self):
        for label, head in (("a fork", FORK_ID), ("an unreadable event", None)):
            with self.subTest(label):
                event = self.repo.tools / "event.json"
                event.write_text(json.dumps({"pull_request": {"head": {"repo": {"id": head}}}}
                                            if head else {}))
                proc = self.repo.run("record", "alpha", self.hash, str(self.out),
                                     GITHUB_EVENT_NAME="pull_request", GITHUB_EVENT_PATH=str(event))
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(proc.stdout, "")
        self.assertFalse(self.out.exists())

    def test_a_same_repository_pull_request_records(self):
        event = self.repo.tools / "event.json"
        event.write_text(json.dumps({"pull_request": {"head": {"repo": {"id": REPO_ID}}}}))
        proc = self.repo.run("record", "alpha", self.hash, str(self.out),
                             GITHUB_EVENT_NAME="pull_request", GITHUB_EVENT_PATH=str(event))
        self.assertIn("name=gate-alpha-", proc.stdout)


class TheRealMap(unittest.TestCase):
    def test_the_matcher_is_git_s_glob_pathspec_for_every_pattern_in_the_map(self):
        proc = subprocess.run(["bash", str(SCRIPT), "describe"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        described = json.loads(proc.stdout)
        found = set(described["global"])
        for gate in described["gates"].values():
            for pats in gate["patterns"].values():
                found.update(pats)
        found.update(["**", "e2e/tests", "*.md", "crates/**/src/*.rs", "**/Cargo.toml", "docs/**/*.md"])
        ours = json.loads(subprocess.run(["bash", str(SCRIPT), "match", *sorted(found)], cwd=ROOT,
                                         capture_output=True, text=True, check=True).stdout)
        for pattern in sorted(found):
            with self.subTest(pattern=pattern):
                gits = subprocess.run(["git", "-C", str(ROOT), "ls-files", "--", f":(glob){pattern}"],
                                      capture_output=True, text=True, check=True).stdout.split("\n")
                self.assertEqual(ours[pattern], sorted(filter(None, gits)))


if __name__ == "__main__":
    unittest.main()
