#!/usr/bin/env python3
"""Offline rows for `scripts/test-plat02-chart-live.py`'s lab record and restore.

No cluster. The live harness scales the lab controller to zero and lets each
test release adopt the chart's cluster-scoped RBAC with `--take-ownership`;
uninstalling a test release deletes what it adopted, and `lab-restore`
re-creates the lab's copies from the record `lab_prepare` took. These rows pin
the three judges that record decides with, and each NEGATIVE CONTROL requires
a refusal:

* `chart_cluster_objects` — which rendered objects are cluster-scoped. The
  pre-fix harness carried a fixed list of five; the chart at 71edaaa renders
  seven, so the lab lost `logweir-trust-admin` and `logweir-retention-admin`.
* `lab_owned` — a lab object is Helm-annotated `scram-local`, or (applied by a
  lab refresh with `kubectl apply`) unannotated with the chart's instance label.
* `restore_action` — what `lab-restore` does with each record; anything that is
  neither the lab's nor a test release's is refused, never deleted.

    /usr/bin/python3 scripts/test_plat02_chart_live_rows.py   # rows, no pytest
    python3 -m pytest scripts/test_plat02_chart_live_rows.py  # same rows
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import shutil
import sys
import tempfile
import types

ROOT = pathlib.Path(__file__).resolve().parents[1]

try:  # the rendered-chart row needs PyYAML; every other row is pure
    import yaml  # noqa: F401

    HAVE_YAML = True
except ImportError:  # the shared pytest venv has no PyYAML
    sys.modules["yaml"] = types.ModuleType("yaml")
    HAVE_YAML = False

# The harness makes its output directory and state file at import.
os.environ["LOGWEIR_CHART_LIVE_OUT"] = tempfile.mkdtemp(prefix="plat02-chart-rows-")
os.environ["LOGWEIR_CHART_LIVE_TS"] = "20260101t0000z"
_spec = importlib.util.spec_from_file_location("plat02chart", ROOT / "scripts" / "test-plat02-chart-live.py")
chart = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(chart)

PRE_FIX_FIXED_LIST = [
    ("clusterrole", "weirkeeper"),
    ("clusterrolebinding", "weirkeeper"),
    ("clusterrole", "logweir-viewer"),
    ("clusterrole", "logweir-operator"),
    ("clusterrole", "logweir-approver"),
]


def obj(name: str, *, annotation: str | None = None, instance: str | None = None, uid: str = "u1") -> dict:
    metadata: dict = {"name": name, "uid": uid}
    if annotation is not None:
        metadata["annotations"] = {"meta.helm.sh/release-name": annotation}
    if instance is not None:
        metadata["labels"] = {"app.kubernetes.io/instance": instance}
    return {"metadata": metadata}


def raises(fn) -> bool:
    try:
        fn()
    except RuntimeError:
        return True
    return False


TEST_RELEASE = chart.NAMES["primary"][1]


# ------------------------------------------------------ chart_cluster_objects


def test_cluster_objects_are_the_non_hook_cluster_scoped_documents():
    docs = [
        {"kind": "ClusterRole", "metadata": {"name": "weirkeeper"}},
        {"kind": "ClusterRoleBinding", "metadata": {"name": "weirkeeper"}},
        {"kind": "Role", "metadata": {"name": "r", "namespace": "ns"}},
        {"kind": "ClusterRole", "metadata": {"name": chart.SINGLETON, "annotations": {"helm.sh/hook": "pre-install"}}},
        None,
    ]
    assert chart.chart_cluster_objects(docs) == [("clusterrole", "weirkeeper"), ("clusterrolebinding", "weirkeeper")]


def test_negative_control_unknown_cluster_scoped_kind_is_refused():
    docs = [{"kind": "PriorityClass", "metadata": {"name": "logweir-critical"}}]
    assert raises(lambda: chart.chart_cluster_objects(docs))


def test_rendered_chart_record_covers_every_cluster_scoped_object():
    if not HAVE_YAML or shutil.which("helm") is None:
        try:
            import pytest
        except ImportError:
            raise RuntimeError("the rendered-chart row needs PyYAML and helm")
        pytest.skip("needs PyYAML and helm")
    rendered = chart.render_cluster_objects()
    assert ("clusterrole", "logweir-trust-admin") in rendered
    assert ("clusterrole", "logweir-retention-admin") in rendered
    assert set(PRE_FIX_FIXED_LIST) <= set(rendered)
    assert ("clusterrole", chart.SINGLETON) not in rendered
    # NEGATIVE CONTROL: the pre-fix fixed list does not cover this chart — the
    # drift that left the lab without two ClusterRoles after lab-restore.
    assert set(rendered) - set(PRE_FIX_FIXED_LIST)


# ------------------------------------------------------------------ lab_owned


def test_lab_owned_helm_annotated_and_kubectl_applied():
    assert chart.lab_owned(obj("weirkeeper", annotation="scram-local", instance="scram-local"))
    assert chart.lab_owned(obj("logweir-trust-admin", instance="scram-local"))


def test_negative_control_lab_owned_refuses_other_owners():
    # A test release's adoption rewrites the annotation; the annotation wins.
    assert not chart.lab_owned(obj("weirkeeper", annotation=TEST_RELEASE, instance="scram-local"))
    assert not chart.lab_owned(obj("weirkeeper", annotation="someone-else"))
    assert not chart.lab_owned(obj("weirkeeper"))
    assert not chart.lab_owned(obj("weirkeeper", instance="another-install"))


# ------------------------------------------------------------- restore_action


def test_restore_action_for_recorded_present_objects():
    recorded = {"key": "clusterrole/weirkeeper", "uid": "u1", "comparable": {}}
    assert chart.restore_action(recorded, None) == "recreate"
    assert chart.restore_action(recorded, obj("weirkeeper", annotation="scram-local")) == "present"
    assert chart.restore_action(recorded, obj("logweir-trust-admin", instance="scram-local")) == "present"
    assert chart.restore_action(recorded, obj("weirkeeper", annotation=TEST_RELEASE)) == "reclaim"


def test_restore_action_for_recorded_absent_objects():
    recorded = {"key": "clusterrole/new-role", "absent": True}
    assert chart.restore_action(recorded, None) == "absent"
    assert chart.restore_action(recorded, obj("new-role", annotation=TEST_RELEASE)) == "delete"


def test_negative_control_restore_refuses_foreign_objects():
    present = {"key": "clusterrole/weirkeeper", "uid": "u1", "comparable": {}}
    absent = {"key": "clusterrole/new-role", "absent": True}
    assert raises(lambda: chart.restore_action(present, obj("weirkeeper", annotation="someone-else")))
    assert raises(lambda: chart.restore_action(present, obj("weirkeeper")))
    # Absent before the run, present now and not a test release's: never deleted.
    assert raises(lambda: chart.restore_action(absent, obj("new-role", instance="scram-local")))
    assert raises(lambda: chart.restore_action(absent, obj("new-role", annotation="someone-else")))


def main() -> int:
    failed = []
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except Exception as exc:  # noqa: BLE001 - every row is reported
                failed.append(name)
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
