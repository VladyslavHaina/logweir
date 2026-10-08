#!/usr/bin/env python3
"""Every signature verification in this repository pins WHO signed (PROD-00.2).

The four images are signed keylessly by `.github/workflows/images.yml`, a
REUSABLE workflow. A keyless certificate's identity (its SAN) for a reusable
workflow is the CALLED file's ref, whoever called it, and a public repository's
reusable workflow can be called from any repository. So an identity alone — or
an identity regular expression, worse — accepts a signature another
repository's workflow made by calling ours. Every `cosign verify`,
`cosign verify-attestation` and `gh attestation verify` in scripts, workflows
and docs must therefore name, literally:

  cosign:  --certificate-identity            https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main
           --certificate-oidc-issuer         https://token.actions.githubusercontent.com
           --certificate-github-workflow-repository  VladyslavHaina/logweir
           --certificate-github-workflow-ref         refs/heads/main
           --certificate-github-workflow-trigger     push
           and no `-regexp` variant of either certificate flag;
  gh:      --cert-identity <the same exact identity>, --repo
           VladyslavHaina/logweir (or -R), --source-ref refs/heads/main,
           --cert-oidc-issuer (the same issuer) and --deny-self-hosted-runners,
           and no --cert-identity-regex (-i) or --signer-workflow (whose
           matching of a ref differs between gh releases).

And no flag twice: cosign (cobra/pflag) and gh take the LAST value of a
repeated flag, so a second `--certificate-identity` after the pinned one would
verify someone else (review L1). Nor any switch that weakens what is checked:
`--insecure-ignore-tlog`, `--insecure-ignore-sct`, `--allow-insecure-registry`,
`--key`, `--certificate`, `--certificate-chain`.

Where an invocation is: a code line (not a comment) of a script, workflow or
source file, or a line inside a fenced code block of a Markdown file; a line
ending in `\\` continues onto the next. Prose that names a command is not one.

With no `--no-sites`, the places that must carry verifications are checked to
carry them (`scripts/ci-images.sh`, `docs/install.md`), so the gate cannot pass
by finding nothing. Exit 0 when every invocation pins, 1 otherwise.
"""

import argparse
import os
import shlex
import sys
from pathlib import Path

IDENTITY = "https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main"
ISSUER = "https://token.actions.githubusercontent.com"
REPOSITORY = "VladyslavHaina/logweir"
REF = "refs/heads/main"
TRIGGER = "push"

COSIGN_REQUIRED = {
    "--certificate-identity": IDENTITY,
    "--certificate-oidc-issuer": ISSUER,
    "--certificate-github-workflow-repository": REPOSITORY,
    "--certificate-github-workflow-ref": REF,
    "--certificate-github-workflow-trigger": TRIGGER,
}
COSIGN_FORBIDDEN = {
    "--certificate-identity-regexp": "a regular expression is not a pin",
    "--certificate-oidc-issuer-regexp": "a regular expression is not a pin",
    "--insecure-ignore-tlog": "skips the transparency log",
    "--insecure-ignore-sct": "skips the certificate timestamp",
    "--allow-insecure-registry": "allows an insecure registry",
    "--key": "a key replaces the keyless identity",
    "--certificate": "a supplied certificate replaces the keyless identity",
    "--certificate-chain": "a supplied chain replaces Fulcio's",
}
GH_REQUIRED = {
    "--cert-identity": IDENTITY,
    "--repo": REPOSITORY,
    "--source-ref": REF,
    "--cert-oidc-issuer": ISSUER,
    "--deny-self-hosted-runners": "",
}
GH_FORBIDDEN = {
    "--cert-identity-regex": "a regular expression is not a pin",
    "--signer-workflow": "its ref matching differs between gh releases; pin --cert-identity",
}
# Flags that take no value: the token after one is never its value.
BOOLEAN = {"--deny-self-hosted-runners", "--insecure-ignore-sct", "--allow-insecure-registry"}

SKIP_DIRS = {".git", "target", "node_modules", ".engine", ".e2e", ".demo", "upstream"}
SUFFIXES = {".sh", ".yml", ".yaml", ".md", ".py", ".mjs", ".js", ".rs", ".toml", ".bash"}
# This gate and its test name the commands in their own text on purpose.
SELF = {"scripts/check-cosign-verify.py", "scripts/test-check-cosign-verify.py"}


def candidate_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            path = Path(dirpath) / name
            rel = path.relative_to(root).as_posix()
            if rel in SELF:
                continue
            if path.suffix in SUFFIXES or name in ("justfile",) or name.startswith("Dockerfile"):
                yield path, rel


def code_lines(path):
    """(line number, text) of the lines that can carry an invocation."""
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return
    markdown = path.suffix == ".md"
    in_fence = False
    for number, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if markdown:
            if stripped.startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                yield number, line
            continue
        if stripped.startswith(("#", "//")):
            continue
        yield number, line


def invocations(path):
    """Each verification command, with its continuation lines joined."""
    lines = list(code_lines(path))
    i = 0
    while i < len(lines):
        number, line = lines[i]
        found = None
        for marker in ("cosign verify", "gh attestation verify"):
            at = line.find(marker)
            if at >= 0:
                found = (marker, at)
                break
        if not found:
            i += 1
            continue
        marker, at = found
        command = line[at:]
        while command.rstrip().endswith("\\") and i + 1 < len(lines):
            i += 1
            command = command.rstrip()[:-1] + " " + lines[i][1].strip()
        yield number, marker, command
        i += 1


def flags(command):
    """{flag: value} for `--flag value` and `--flag=value`; -R is --repo."""
    try:
        tokens = shlex.split(command, comments=False, posix=True)
    except ValueError:
        tokens = command.split()
    out = {}
    repeated = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "-R":
            tok = "--repo"
        elif tok == "-i":
            tok = "--cert-identity-regex"
        if tok.startswith("--"):
            if "=" in tok:
                name, value = tok.split("=", 1)
            elif tok in BOOLEAN or i + 1 >= len(tokens) or tokens[i + 1].startswith("--"):
                name, value = tok, ""
            else:
                name, value = tok, tokens[i + 1]
                i += 1
            if name in out:
                repeated.append(name)
            out[name] = value
        i += 1
    return out, repeated


def problems_of(marker, command):
    got, repeated = flags(command)
    problems = [
        f"{flag} is given twice; the command keeps the LAST value, so a pin can be overridden"
        for flag in sorted(set(repeated))
    ]
    cosign = marker == "cosign verify"
    required = COSIGN_REQUIRED if cosign else GH_REQUIRED
    forbidden = COSIGN_FORBIDDEN if cosign else GH_FORBIDDEN
    for flag, want in required.items():
        if flag not in got:
            problems.append(f"no {flag} {want}".rstrip())
        elif got[flag] != want:
            problems.append(f"{flag} is `{got[flag]}`, must be exactly `{want}`")
    for flag, why in forbidden.items():
        if flag in got:
            problems.append(f"{flag}: {why}")
    return problems


def check(root, require_sites=True):
    root = Path(root).resolve()
    failures = []
    counts = {}
    for path, rel in candidate_files(root):
        for number, marker, command in invocations(path):
            kind = command.split()[1] if marker == "cosign verify" else "gh-attestation"
            counts[(rel, kind)] = counts.get((rel, kind), 0) + 1
            for problem in problems_of(marker, command):
                failures.append(f"{rel}:{number}: {problem}\n    {command.strip()[:160]}")
    if require_sites:
        for rel, kind, least in (
            ("scripts/ci-images.sh", "verify", 1),
            ("scripts/ci-images.sh", "verify-attestation", 1),
            ("docs/install.md", "verify", 1),
            ("docs/install.md", "verify-attestation", 1),
            ("docs/install.md", "gh-attestation", 1),
        ):
            if counts.get((rel, kind), 0) < least:
                failures.append(
                    f"{rel}: carries no `{kind}` verification; the gate would pass by finding nothing"
                )
    return failures, sum(counts.values())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--no-sites", action="store_true")
    args = parser.parse_args(argv)
    failures, found = check(args.root, require_sites=not args.no_sites)
    if failures:
        print("check-cosign-verify: a signature verification that does not pin who signed:", file=sys.stderr)
        for f in failures:
            print("  " + f, file=sys.stderr)
        return 1
    print(f"check-cosign-verify: {found} verification(s), every one pinning identity, caller and issuer")
    return 0


if __name__ == "__main__":
    sys.exit(main())
