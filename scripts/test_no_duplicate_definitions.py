"""No Python script in scripts/ binds a top-level name twice.

    python3 scripts/test_no_duplicate_definitions.py   (or: python3 -m pytest scripts/test_no_duplicate_definitions.py)

Python binds a module's definitions in file order, so the LAST one wins for every caller,
including callers written above it: no error, no warning. That is how a live harness's
`patch_policy` (RetentionPolicy) was silently replaced by a second `patch_policy`
(ProtectionPolicy) during lab-refresh-7. Moved from the D3 harness's offline rows when the
shared-lab harnesses were removed after v0.2.0.
"""
from __future__ import annotations

import ast
import collections
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def duplicated_top_level_names(source: str) -> dict[str, list[int]]:
    seen: dict[str, list[int]] = collections.defaultdict(list)
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            seen[node.name].append(node.lineno)
    return {name: lines for name, lines in seen.items() if len(lines) > 1}


def modules() -> list[pathlib.Path]:
    return sorted(set(ROOT.glob("scripts/*.py")) | set(ROOT.glob("scripts/fixtures/*.py")))


def test_no_script_binds_a_top_level_name_twice():
    found = modules()
    # NOT VACUOUS: the live harnesses `just lint` imports are read.
    names = {p.name for p in found}
    assert {"test-k8s-scram.py", "test-plat06-live.py", "bind-credential.py"} <= names, sorted(names)
    for path in found:
        dups = duplicated_top_level_names(path.read_text())
        assert not dups, (str(path.relative_to(ROOT)), dups)


def test_the_sweep_names_a_module_that_defines_a_function_twice():
    """NEGATIVE CONTROL: the exact shape of the lab-refresh-7 defect."""
    planted = ("def patch_policy(name, spec_patch):\n    return 'retentionpolicy'\n\n"
               "def other():\n    return patch_policy('keep-b', {})\n\n"
               "def patch_policy(name, patch):\n    return 'protectionpolicy'\n")
    assert duplicated_top_level_names(planted) == {"patch_policy": [1, 7]}


if __name__ == "__main__":
    rows = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for row in rows:
        row()
        print("ok:", row.__name__)
    print(f"test_no_duplicate_definitions: {len(rows)} rows passed over {len(modules())} modules")
