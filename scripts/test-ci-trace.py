#!/usr/bin/env python3
"""Unit rows for scripts/ci-trace.py (FX-53 §2.6, the traced run).

Two kinds of sample:
- RECORDED (scripts/fixtures/ci-trace/): real strace 6.1 output of the canary
  and of `cargo test` on a small crate whose test reads by absolute and
  relative path, from a thread, through a script that changes directory, and
  through a child with its own current_dir. They were recorded in a throwaway
  rust:1.89-bookworm container exactly as `ci-trace.py run` records, then
  trimmed of system and target/ paths (every fork, chdir, exec and resumed
  line kept; the reads are the same before and after).
- WRITTEN in the same shape, for the cases a recording rarely shows: threads
  under CLONE_FS, vfork, a fork whose parent changes directory after it,
  truncation, undecorated dirfds, escapes.
The runner's own strace is checked live by ci-trace.yml's canary on every
traced run.
"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
TRACE = ROOT / "scripts" / "ci-trace.py"
CO = "/home/runner/work/logweir/logweir"
TRACKED = ["ci/gates.toml", "README.md", "justfile", "docs/install.md", "docs/a\"b.md",
           "crates/logweir/src/lib.rs", "crates/logweir/tests/doc_lint.rs", "charts/logweir/values.yaml",
           "scripts/ci-check.sh", "e2e/fixtures/x.json", "console/src/main.tsx", "Cargo.toml",
           "docs/café.md"]

# A cargo test run: cargo starts in the checkout, spawns the test binary in
# its package directory (posix_spawn: clone3 CLONE_VM|CLONE_VFORK, then the
# child's chdir and execve), whose threads (CLONE_FS) read files by relative
# path; a forked shell changes directory after its child was cloned.
SAMPLE = f"""\
100 execve("/home/runner/.cargo/bin/cargo", ["cargo", "test"], 0x7ffd /* 60 vars */) = 0
100 openat(AT_FDCWD</home/runner/work/logweir/logweir>, "Cargo.toml", O_RDONLY|O_CLOEXEC) = 3<{CO}/Cargo.toml>
100 newfstatat(3<{CO}/Cargo.toml>, "", {{st_mode=S_IFREG|0644, st_size=9000, ...}}, AT_EMPTY_PATH) = 0
100 openat(AT_FDCWD, "/home/runner/.cargo/registry/index/x", O_RDONLY) = -1 ENOENT (No such file or directory)
100 statx(AT_FDCWD</home/runner/work/logweir/logweir>, "target/debug/deps/doc_lint-1", AT_STATX_SYNC_AS_STAT, STATX_ALL, {{stx_mask=STATX_ALL, ...}}) = 0
100 clone3({{flags=CLONE_VM|CLONE_VFORK|CLONE_CLEAR_SIGHAND, exit_signal=SIGCHLD, stack=0x7f, stack_size=0x9000}}, 88 <unfinished ...>
101 chdir("{CO}/crates/logweir") = 0
101 execve("{CO}/target/debug/deps/doc_lint-1", ["doc_lint-1"], 0x7f /* 61 vars */ <unfinished ...>
100 <... clone3 resumed>) = 101
101 <... execve resumed>) = 0
101 clone3({{flags=CLONE_VM|CLONE_FS|CLONE_FILES|CLONE_SIGHAND|CLONE_THREAD|CLONE_SYSVSEM, child_tid=0x7f, parent_tid=0x7f, exit_signal=0, stack=0x7f, stack_size=0x200000, tls=0x7f}} => {{parent_tid=[102]}}, 88) = 102
102 openat(AT_FDCWD, "../../docs/install.md", O_RDONLY|O_CLOEXEC) = 4</home/runner/work/logweir/logweir/docs/install.md>
102 openat(AT_FDCWD, "src/lib.rs", O_RDONLY|O_CLOEXEC <unfinished ...>
101 access("../../justfile", R_OK) = 0
102 <... openat resumed>) = 5<{CO}/crates/logweir/src/lib.rs>
102 newfstatat(AT_FDCWD, "../../charts/logweir/values.yaml", 0x7f, 0) = -1 ENOENT (No such file or directory)
101 openat(AT_FDCWD, "../../docs/a\\"b.md", O_RDONLY) = -1 ENOENT (No such file or directory)
101 openat(AT_FDCWD, "../../docs/caf\\303\\251.md", O_RDONLY) = -1 ENOENT (No such file or directory)
101 clone(child_stack=NULL, flags=CLONE_CHILD_CLEARTID|CLONE_CHILD_SETTID|SIGCHLD, child_tidptr=0x7f) = 103
101 chdir("/tmp") = 0
103 openat(AT_FDCWD, "../../scripts/ci-check.sh", O_RDONLY) = 3<{CO}/scripts/ci-check.sh>
103 stat("../../e2e/fixtures/x.json", {{st_mode=S_IFREG|0644, st_size=2, ...}}) = 0
103 open("{CO}/.git/index", O_RDONLY) = 4<{CO}/.git/index>
103 open("{CO}/target/debug/build/x", O_RDONLY) = -1 ENOENT (No such file or directory)
103 openat(AT_FDCWD, "{CO}/crates/logweir", O_RDONLY|O_NONBLOCK|O_CLOEXEC|O_DIRECTORY) = 5<{CO}/crates/logweir>
103 openat(AT_FDCWD, "/etc/passwd", O_RDONLY) = 5</etc/passwd>
103 +++ exited with 0 +++
101 openat(AT_FDCWD, "ci/gates.toml", O_RDONLY) = -1 ENOENT (No such file or directory)
100 --- SIGCHLD {{si_signo=SIGCHLD, si_code=CLD_EXITED, si_pid=101, si_uid=1001, si_status=0, ...}} ---
"""


FIXTURES = ROOT / "scripts" / "fixtures" / "ci-trace"
RECORDED_ROOT = "/work/co"


class Recorded(unittest.TestCase):
    def run_trace(self, *args):
        return subprocess.run([sys.executable, str(TRACE), *args], capture_output=True, text=True)

    def reads(self, name):
        proc = self.run_trace("reads", "--root", RECORDED_ROOT, "--tracked",
                              str(FIXTURES / "tracked.txt"), str(FIXTURES / name))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return [line for line in proc.stdout.split("\n") if line]

    def test_the_recorded_canary_reads_its_three_files(self):
        self.assertEqual(self.reads("canary.strace"), ["README.md", "ci/gates.toml", "justfile"])

    def test_the_recorded_cargo_test_reads_every_file_it_touched(self):
        self.assertEqual(self.reads("cargo-test.strace"), [
            "README.md",                  # "../README.md" from the package dir
            "ci/gates.toml",              # `cat gates.toml` with current_dir(ci)
            "docs/caf\u00e9.md",          # the script's `[ -e caf\u00e9.md ]` after its cd
            "docs/declared.md",           # an absolute path through CARGO_MANIFEST_DIR
            "docs/install.md",            # the script's `cat install.md` after its cd
            "docs/undeclared.md",         # a thread's metadata(), relative
            "sample/Cargo.toml",          # cargo
            "sample/src/lib.rs",          # cargo's freshness check
            "sample/tests/reads.rs",
            "scripts/read-docs.sh",       # bash opens the script it runs
        ])

    def test_the_recorded_cargo_test_fails_on_its_undeclared_read(self):
        declared = [r for r in self.reads("cargo-test.strace") if r != "docs/undeclared.md"]
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            fh.write("\n".join(declared) + "\n")
        try:
            proc = self.run_trace("check", "--root", RECORDED_ROOT, "--tracked", str(FIXTURES / "tracked.txt"),
                                  "--inputs", fh.name, str(FIXTURES / "cargo-test.strace"))
        finally:
            os.unlink(fh.name)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertEqual([l for l in proc.stdout.split("\n") if l.startswith("UNDECLARED")],
                         ["UNDECLARED docs/undeclared.md (statx)"])


class Trace(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="ci-trace-"))
        (self.dir / "tracked").write_text("\n".join(TRACKED) + "\n")

    def run_trace(self, command, text, *extra):
        trace = self.dir / "trace.txt"
        trace.write_text(text)
        return subprocess.run([sys.executable, str(TRACE), command, "--root", CO, "--tracked",
                               str(self.dir / "tracked"), *extra, str(trace)],
                              capture_output=True, text=True)

    def reads(self, text, *extra):
        proc = self.run_trace("reads", text, *extra)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return [line for line in proc.stdout.split("\n") if line]

    def check(self, text, declared):
        inputs = self.dir / "inputs"
        inputs.write_text("\n".join(declared) + "\n")
        return self.run_trace("check", text, "--inputs", str(inputs))

    def test_the_sample_maps_to_the_tracked_files_read(self):
        self.assertEqual(self.reads(SAMPLE), sorted([
            "Cargo.toml",                         # an openat's returned path
            "docs/install.md",                    # a thread's relative path from its package dir
            "crates/logweir/src/lib.rs",          # unfinished, then resumed
            "justfile",                           # access(), relative to the chdir
            "charts/logweir/values.yaml",         # a failed stat still names the file
            'docs/a"b.md',                        # escapes
            "docs/café.md",                  # octal UTF-8
            "scripts/ci-check.sh",                # a fork's cwd is its parent's at the clone
            "e2e/fixtures/x.json",                # stat()
        ]))

    def test_target_git_untracked_directories_and_outside_paths_are_ignored(self):
        reads = self.reads(SAMPLE)
        for ignored in ("target/debug/deps/doc_lint-1", ".git/index", "crates/logweir", "/etc/passwd"):
            self.assertNotIn(ignored, reads)
        # The parent's chdir("/tmp") came after the clone: ci/gates.toml,
        # opened relative to /tmp, is not the checkout's.
        self.assertNotIn("ci/gates.toml", reads)

    def test_check_fails_on_an_undeclared_tracked_file(self):
        declared = [p for p in self.reads(SAMPLE) if p != "charts/logweir/values.yaml"]
        proc = self.check(SAMPLE, declared)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("UNDECLARED charts/logweir/values.yaml (newfstatat)", proc.stdout)
        self.assertEqual(proc.stdout.count("UNDECLARED"), 1)

    def test_check_passes_when_every_read_is_declared(self):
        declared = self.reads(SAMPLE) + ["console/src/main.tsx"]
        proc = self.check(SAMPLE, declared)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("9 tracked files read", proc.stdout)

    def test_no_declared_inputs_is_an_error(self):
        self.assertEqual(self.check(SAMPLE, []).returncode, 2)

    def test_a_truncated_path_is_an_error_not_a_pass(self):
        text = f'200 openat(AT_FDCWD, "{CO}/docs/ins"..., O_RDONLY) = -1 ENOENT (No such file)\n'
        proc = self.check(text, ["docs/install.md"])
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("truncated", proc.stderr)

    def test_a_dirfd_without_its_path_is_an_error(self):
        proc = self.run_trace("reads", '200 openat(7, "docs/install.md", O_RDONLY) = -1 ENOENT\n')
        self.assertEqual(proc.returncode, 2)
        self.assertIn("strace -y", proc.stderr)

    def test_a_decorated_dirfd_and_fchdir_resolve(self):
        text = (f'300 openat(7<{CO}/docs>, "install.md", O_RDONLY) = -1 ENOENT\n'
                f'300 fchdir(4<{CO}/ci>) = 0\n'
                '300 newfstatat(AT_FDCWD, "gates.toml", 0x7f, 0) = 0\n')
        self.assertEqual(self.reads(text), ["ci/gates.toml", "docs/install.md"])

    def test_the_start_directory_is_the_root_unless_told(self):
        text = '400 openat(AT_FDCWD, "README.md", O_RDONLY) = -1 ENOENT\n'
        self.assertEqual(self.reads(text), ["README.md"])
        self.assertEqual(self.reads(text, "--cwd", f"{CO}/docs"), [])

    def test_a_vfork_child_does_not_share_its_parent_s_chdir(self):
        text = ('500 vfork( <unfinished ...>\n'
                '501 chdir("/tmp") = 0\n'
                '500 <... vfork resumed>) = 501\n'
                '500 openat(AT_FDCWD, "README.md", O_RDONLY) = -1 ENOENT\n'
                '501 openat(AT_FDCWD, "justfile", O_RDONLY) = -1 ENOENT\n')
        self.assertEqual(self.reads(text), ["README.md"])

    def test_a_clone_fs_thread_shares_its_parent_s_chdir(self):
        text = ('600 clone3({flags=CLONE_VM|CLONE_FS|CLONE_THREAD, exit_signal=0}, 88) = 601\n'
                f'601 chdir("{CO}/docs") = 0\n'
                '600 openat(AT_FDCWD, "install.md", O_RDONLY) = -1 ENOENT\n')
        self.assertEqual(self.reads(text), ["docs/install.md"])

    def test_a_symlinked_root_is_the_same_checkout(self):
        with tempfile.TemporaryDirectory() as real:
            link = Path(tempfile.mkdtemp(prefix="ci-trace-link-")) / "co"
            os.symlink(real, link)
            (self.dir / "tracked").write_text("README.md\n")
            trace = self.dir / "trace.txt"
            trace.write_text(f'700 openat(AT_FDCWD, "{os.path.realpath(real)}/README.md", O_RDONLY) = -1 ENOENT\n')
            proc = subprocess.run([sys.executable, str(TRACE), "reads", "--root", str(link), "--tracked",
                                   str(self.dir / "tracked"), str(trace)], capture_output=True, text=True)
            self.assertEqual(proc.stdout.split(), ["README.md"], proc.stderr)

    def test_the_canary_needs_its_three_reads(self):
        # A stand-in strace on PATH: it runs the command and writes the lines a
        # real one would for it, or one fewer.
        tools = self.dir / "bin"
        tools.mkdir()
        fake = tools / "strace"
        fake.write_text(f"""#!{sys.executable}
import os, subprocess, sys
args = sys.argv[1:]
out = args[args.index("-o") + 1]
command = args[args.index("-o") + 2:]
rc = subprocess.run(command).returncode
root = os.getcwd()
lines = ['1 clone(child_stack=NULL, flags=SIGCHLD) = 2',
         f'1 chdir("{{root}}/ci") = 0',
         '2 openat(AT_FDCWD, "gates.toml", O_RDONLY) = 3<' + root + '/ci/gates.toml>',
         f'1 chdir("{{root}}") = 0',
         '1 newfstatat(AT_FDCWD, "justfile", {{st_mode=S_IFREG|0644, ...}}, 0) = 0']
if not os.environ.get("FAKE_DROP"):
    lines.append('1 openat(AT_FDCWD, "README.md", O_RDONLY|O_CLOEXEC) = 3<' + root + '/README.md>')
open(out, "w").write("\\n".join(lines) + "\\n")
sys.exit(rc)
""")
        fake.chmod(0o755)
        env = {**os.environ, "PATH": f"{tools}:{os.environ['PATH']}"}
        canary = [sys.executable, str(TRACE), "canary", "--root", str(ROOT)]
        proc = subprocess.run(canary, capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("read ['README.md', 'ci/gates.toml', 'justfile']", proc.stdout)
        proc = subprocess.run(canary, capture_output=True, text=True, env={**env, "FAKE_DROP": "1"})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("['README.md']", proc.stderr)

    def test_the_traced_syscalls_are_the_ones_parsed(self):
        sys.path.insert(0, str(TRACE.parent))
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("ci_trace", TRACE)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        finally:
            sys.path.pop(0)
        traced = set(mod.TRACED.split(","))
        for required in ("openat", "open", "stat", "access"):
            self.assertIn(required, traced)
        self.assertEqual(traced, set(mod.PATH_ARG) - {"fstatat64"} | mod.FORKS | {"fchdir"})
        proc = subprocess.run([sys.executable, str(TRACE), "run", "--dry-run", "--out", "t.txt", "--",
                               "bash", "scripts/ci-check.sh", "unit-core"], capture_output=True, text=True)
        self.assertEqual(proc.stdout.rstrip("\n").split("\0"),
                         ["strace", "-f", "--seccomp-bpf", "-qq", "-y", "-s", "4096",
                          "-e", f"trace={mod.TRACED}", "-o", "t.txt", "bash", "scripts/ci-check.sh", "unit-core"])

if __name__ == "__main__":
    unittest.main()
