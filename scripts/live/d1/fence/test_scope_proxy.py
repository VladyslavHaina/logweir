#!/usr/bin/env python3
"""Unit test for the scope proxy's rewrite table, request shapes and classifier.

Run it directly (`python3 scripts/live/d1/fence/test_scope_proxy.py`) or under
pytest; it imports `scripts/fixtures/plat04_scope_proxy.py` and touches no
cluster, no network and no file outside `config/crd`.

THE ROW THAT MATTERS MOST IS `table_matches_the_generated_crds`. The fence puts
a real controller behind this proxy: a namespaced kind missing from the table
is forwarded cluster-wide, so the fenced controller reconciles the shared lab
release the fence exists to stay out of, and a cluster-scoped kind wrongly IN
the table is rewritten into a namespace that cannot hold it and 404s. Both
failures are silent at runtime and both are caught here, against the generated
CRDs rather than against a second hand-written list.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "scripts" / "fixtures"))

import plat04_scope_proxy as proxy  # noqa: E402

NS = "d1fence-test"


def crd_scopes() -> dict[str, str]:
    """`plural -> scope` read out of the generated CRDs, without a YAML parser.

    `just crds` writes these files; the two lines this needs are emitted at a
    fixed indentation by the generator, so a five-line reader is honest here
    and keeps the test dependency-free.
    """
    scopes: dict[str, str] = {}
    for path in sorted((ROOT / "config" / "crd").glob("*.yaml")):
        if path.name == "kustomization.yaml":
            continue
        text = path.read_text()
        plural = re.search(r"^\s*plural:\s*(\S+)\s*$", text, re.M)
        scope = re.search(r"^\s*scope:\s*(\S+)\s*$", text, re.M)
        group = re.search(r"^\s*group:\s*(\S+)\s*$", text, re.M)
        assert plural and scope and group, f"{path} has no plural/scope/group"
        if group.group(1) != "logweir.dev":
            continue
        scopes[plural.group(1)] = scope.group(1)
    return scopes


def test_table_matches_the_generated_crds() -> None:
    scopes = crd_scopes()
    assert scopes, "no logweir CRDs found under config/crd"
    namespaced = {p for p, s in scopes.items() if s == "Namespaced"}
    cluster = {p for p, s in scopes.items() if s == "Cluster"}
    assert proxy.NAMESPACED_LOGWEIR == namespaced, (
        "the proxy's namespaced table and config/crd disagree: "
        f"only in proxy={sorted(proxy.NAMESPACED_LOGWEIR - namespaced)}, "
        f"only in CRDs={sorted(namespaced - proxy.NAMESPACED_LOGWEIR)}"
    )
    assert proxy.CLUSTER_SCOPED_LOGWEIR == cluster, (
        "the proxy's cluster-scoped table and config/crd disagree: "
        f"only in proxy={sorted(proxy.CLUSTER_SCOPED_LOGWEIR - cluster)}, "
        f"only in CRDs={sorted(cluster - proxy.CLUSTER_SCOPED_LOGWEIR)}"
    )
    assert not (proxy.NAMESPACED_LOGWEIR & proxy.CLUSTER_SCOPED_LOGWEIR)


def test_every_namespaced_logweir_collection_is_scoped() -> None:
    for plural in sorted(proxy.NAMESPACED_LOGWEIR):
        got = proxy.rewrite(f"/apis/logweir.dev/v1alpha1/{plural}", NS)
        assert got == f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/{plural}", (plural, got)


def test_cluster_scoped_logweir_collections_are_never_scoped() -> None:
    for plural in sorted(proxy.CLUSTER_SCOPED_LOGWEIR):
        path = f"/apis/logweir.dev/v1alpha1/{plural}"
        assert proxy.rewrite(path, NS) == path, plural
        named = f"{path}/default"
        assert proxy.rewrite(named, NS) == named, plural
        status = f"{named}/status"
        assert proxy.rewrite(status, NS) == status, plural


def test_a_cluster_wide_named_object_url_is_not_invented_into_a_namespace() -> None:
    """The proxy scopes COLLECTIONS. It never invents a namespace for a name.

    A rewrite that fired on a named URL would turn a caller's own addressing
    mistake into a silent read of a different object in the fenced namespace,
    which is exactly the confusion this proxy exists to prevent.
    """
    for path in (
        "/apis/logweir.dev/v1alpha1/backups/logweir-backup-ed-20260918-120000",
        "/apis/logweir.dev/v1alpha1/backupschedules/ed/status",
        "/apis/batch/v1/jobs/logweir-run-1",
        "/api/v1/pods/kafka-0",
        "/api/v1/pods/kafka-0/log",
    ):
        assert proxy.rewrite(path, NS) == path, path


def test_core_and_batch_collections_are_scoped() -> None:
    assert proxy.rewrite("/apis/batch/v1/jobs", NS) == f"/apis/batch/v1/namespaces/{NS}/jobs"
    assert proxy.rewrite("/api/v1/pods", NS) == f"/api/v1/namespaces/{NS}/pods"
    assert proxy.rewrite("/api/v1/configmaps", NS) == f"/api/v1/namespaces/{NS}/configmaps"
    assert proxy.rewrite("/api/v1/events", NS) == f"/api/v1/namespaces/{NS}/events"


def test_an_already_namespaced_url_is_untouched() -> None:
    for path in (
        f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups",
        f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups/run-1",
        f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backupschedules/nightly/status",
        f"/api/v1/namespaces/{NS}/configmaps/plan",
        "/apis/logweir.dev/v1alpha1/namespaces/other/backups",
    ):
        assert proxy.rewrite(path, NS) == path, path


def test_unknown_resources_are_forwarded_unchanged_and_recorded() -> None:
    proxy.UNKNOWN.clear()
    path = "/apis/logweir.dev/v1alpha1/futurekinds"
    assert proxy.rewrite(path, NS) == path
    assert proxy.UNKNOWN.get("futurekinds") == 1
    # A non-Logweir group the table does not name is forwarded and NOT recorded
    # as a Logweir gap.
    other = "/apis/networking.k8s.io/v1/networkpolicies"
    assert proxy.rewrite(other, NS) == other
    assert "networkpolicies" not in proxy.UNKNOWN
    proxy.UNKNOWN.clear()


def test_query_strings_survive_the_rewrite() -> None:
    got = proxy.rewrite(
        "/apis/logweir.dev/v1alpha1/backups?watch=true&resourceVersion=42&allowWatchBookmarks=true",
        NS,
    )
    assert got == (
        f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups"
        "?watch=true&resourceVersion=42&allowWatchBookmarks=true"
    )
    paged = proxy.rewrite(
        "/apis/logweir.dev/v1alpha1/backups?limit=500&continue=abc&labelSelector=a%3Db", NS
    )
    assert paged.endswith("?limit=500&continue=abc&labelSelector=a%3Db")
    assert f"/namespaces/{NS}/backups" in paged


def test_the_deprecated_watch_prefix_is_scoped_too() -> None:
    got = proxy.rewrite("/apis/logweir.dev/v1alpha1/watch/backups", NS)
    assert got == f"/apis/logweir.dev/v1alpha1/watch/namespaces/{NS}/backups"
    kept = "/apis/logweir.dev/v1alpha1/watch/trustrosters"
    assert proxy.rewrite(kept, NS) == kept


def test_an_empty_namespace_disables_the_rewrite() -> None:
    path = "/apis/logweir.dev/v1alpha1/backups"
    assert proxy.rewrite(path, "") == path


def test_parse_path_shapes() -> None:
    info = proxy.parse_path(f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backupschedules/n/status")
    assert info["group"] == "logweir.dev"
    assert info["namespace"] == NS
    assert info["resource"] == "backupschedules"
    assert info["name"] == "n"
    assert info["subresource"] == "status"
    assert proxy.parse_path("/api/v1/namespaces/x")["resource"] == "namespaces"
    assert proxy.parse_path("/healthz")["resource"] == ""
    assert proxy.parse_path("/version")["resource"] == ""


def test_shape_separates_list_get_and_watch() -> None:
    assert proxy.shape("GET", "/apis/logweir.dev/v1alpha1/backups") == "LIST backups"
    assert proxy.shape("GET", "/apis/logweir.dev/v1alpha1/backups?limit=500") == "LIST backups"
    assert proxy.shape("GET", "/apis/logweir.dev/v1alpha1/backups?watch=true") == "WATCH backups"
    assert (
        proxy.shape("GET", f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups/r1")
        == "GET backups"
    )
    assert (
        proxy.shape("PATCH", f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups/r1/status")
        == "PATCH backups/status"
    )


def test_classify_names_the_requests_the_scenarios_hold() -> None:
    base = f"/apis/logweir.dev/v1alpha1/namespaces/{NS}"
    reservation = json.dumps(
        {
            "status": {
                "pendingBackupRef": {"name": "logweir-backup-ed-20260918-120000"},
                "pendingRun": {"generation": 2},
            }
        }
    ).encode()
    assert proxy.classify("PATCH", f"{base}/backupschedules/ed/status", reservation) == (
        "reservation",
        "ed",
        "ed",
    )
    # The pre-PATCH controller sent the same decision as a PUT; both are the
    # reservation, so neither harness silently stops matching.
    assert proxy.classify("PUT", f"{base}/backupschedules/ed/status", reservation)[0] == (
        "reservation"
    )
    final = json.dumps({"status": {"lastSlot": {"slot": "20260918-120000"}}}).encode()
    assert proxy.classify("PATCH", f"{base}/backupschedules/ed/status", final) == (
        "schedule_final",
        "ed",
        "ed",
    )
    create = json.dumps(
        {
            "metadata": {"name": "logweir-backup-ed-20260918-120000"},
            "spec": {"scheduleRef": {"name": "ed"}},
        }
    ).encode()
    assert proxy.classify("POST", f"{base}/backups", create) == (
        "backup_create",
        "logweir-backup-ed-20260918-120000",
        "ed",
    )
    migration = json.dumps(
        {
            "metadata": {
                "ownerReferences": [],
                "resourceVersion": "4242",
                "labels": {"logweir.dev/schedule-uid": "u"},
            }
        }
    ).encode()
    assert proxy.classify("PATCH", f"{base}/backups/old-run", migration) == (
        "migration_patch",
        "old-run",
        "",
    )
    status = json.dumps({"status": {"phase": "Running"}}).encode()
    assert proxy.classify("PATCH", f"{base}/backups/old-run/status", status)[0] == "backup_status"
    plan = json.dumps({"metadata": {"name": "logweir-plan-abc"}}).encode()
    assert proxy.classify("POST", f"{base}/configmaps", plan) == (
        "configmap_create",
        "logweir-plan-abc",
        "",
    )
    job = json.dumps({"metadata": {"name": "logweir-run-abc"}}).encode()
    assert proxy.classify("POST", f"{base}/jobs", job) == ("job_create", "logweir-run-abc", "")
    assert proxy.classify("GET", f"{base}/backups/r1", b"") == ("backup_get", "r1", "")
    assert proxy.classify("GET", "/apis/logweir.dev/v1alpha1/backups", b"") == (
        "backup_list",
        "",
        "",
    )


def main() -> int:
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_")]
    failures = []
    for name, fn in tests:
        try:
            fn()
            print(f"ok   {name}")
        except AssertionError as error:
            failures.append((name, error))
            print(f"FAIL {name}: {error}")
    print(f"{len(tests) - len(failures)} passed, {len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
