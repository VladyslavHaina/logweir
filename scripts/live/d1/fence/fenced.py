#!/usr/bin/env python3
"""The fenced controller: manifests, lifecycle and the proof that it is fenced.

D1 §13.1 asks for "a source-matched image fenced to the test namespace ... a
namespace-rewriting API proxy plus a `ValidatingAdmissionPolicy`". This module
is that, and the reason it exists is that eight of D1 §13.2's scenarios are
statements about a controller — stop it, run two of it, hold one of its
requests, count its reads, swap it for an older build — and none of those can
be said about a controller other work shares.

# What "fenced" means here, exactly

1. **Inward**: the fenced controller reaches the API server only through
   `plat04_scope_proxy.py`, which rewrites every cluster-wide collection URL to
   this namespace. Its kubeconfig names `http://127.0.0.1:8080` and its
   in-cluster environment variables are blanked, so there is no second route.
   Its RBAC is a namespaced Role plus a cluster-scoped **read-only** grant for
   the two cluster-scoped kinds (`trustrosters`, `trustpolicies`) that have no
   namespace to be scoped into: it can read them and it can write nothing
   outside this namespace even if the proxy were removed.
2. **Outward**: a `ValidatingAdmissionPolicy` bound to this namespace alone
   denies every CREATE/UPDATE/DELETE from the SHARED release's ServiceAccount.
   The shared controller keeps running, keeps watching, and cannot write here.

The VAP is cluster-scoped, so it is created under the cluster lock and deleted
with it. Nothing in `logweir-scram-local` is modified: the fence is built
around it, not on it.
"""

from __future__ import annotations

import json
import time
from typing import Any

SHARED_NS = "logweir-scram-local"
SHARED_CONTROLLER_USER = f"system:serviceaccount:{SHARED_NS}:weirkeeper"
CONTROLLER = "d1fence-controller"
SA = "d1fence-controller"
PROXY_CONFIGMAP = "d1fence-proxy"
KUBECONFIG_CONFIGMAP = "d1fence-kubeconfig"
POLICY_CONFIGMAP = "weirkeeper-policy"
FENCE_LABEL = "d1fence.logweir.dev/fenced"

# The two source-matched controller image PAIRS this lab holds. The second
# element is the commit the pair must have been built from, or `None` when that
# commit is discovered rather than written down.
#
# `old` is pinned, and stays pinned: `weirkeeper:plat0102-4956785` is a frozen
# build kept solely so the downgrade rows have something older to swap to. It is
# never rebuilt, so a constant describes it exactly.
#
# `new` is NOT pinned, and the reason is a failure this file caused. Its tags
# (`weirkeeper:scram-reviewed`, `logweir:scram-local`) are mutable and every lab
# refresh moves them; a constant written beside them is a claim about a tag that
# has since changed. lab-refresh-3 refreshed the lab to `c6422a7` and the
# constant here still said `e7d0e79`, so `assert_source_matched` refused and D1
# L-05.1-3 could not run at all (lab-refresh-3 §9.1) — the refusal was right and
# the expectation was stale. The expectation is now taken from the running
# image's own `org.opencontainers.image.revision` label, and then required to
# name the commit THIS CHECKOUT is testing: a discovered expectation that agreed
# with nothing would be no expectation at all. `--fence-revision <sha>` on
# `run.py` overrides it for a deliberate measurement of some other build.
IMAGES = {
    "new": ("weirkeeper:scram-reviewed", None),
    "old": ("weirkeeper:plat0102-4956785", "4956785d00d74fe960c84d396d2eff852c68ebd8"),
}
RUNNER_IMAGES = {
    "new": ("logweir:scram-local", None),
    "old": ("logweir:plat0102-4956785", "4956785d00d74fe960c84d396d2eff852c68ebd8"),
}

# Set from `run.py --fence-revision <sha>` / `--fence-old-revision <sha>` before
# any phase runs. An explicit pin still has to match the image's label: the flag
# names an expectation, it does not suspend the check.
REVISION_OVERRIDE: dict[str, str] = {}

# `docker inspect --format '{{index .Config.Labels "..."}}'` prints this when the
# label is absent. An unlabelled image can never be source-matched.
NO_LABEL = "<no value>"


def policy_json(ns: str) -> str:
    """This namespace's installation policy, with its OWN archive endpoint.

    The shared release's `weirkeeper-policy` names the shared MinIO, which is
    why the W8 run could only reach its own store through a
    `BackupDestination`. The fenced controller is ours, so its installation
    setting points at this namespace's MinIO and a plain `s3://` archive URL
    resolves here — which the main@4956785 build needs, because
    `BackupDestination` did not exist when it was built.
    """
    return json.dumps(
        {
            "checks": {
                "maxActiveDiscoveriesPerConnection": 1,
                "maxActivePerNamespace": 40,
                "maxActiveTotal": 60,
                "maxEvidenceFetchActivePerNamespace": 4,
            },
            "discovery": {
                "defaultMaxTopics": 20000,
                "freshSeconds": 900,
                "hardMaxTopics": 50000,
                "keepPerConnection": 5,
                "retentionSeconds": 86400,
                "visibilityAttestations": [],
            },
            "engine": {"allowUnverifiedCustomCa": False},
            "evidence": {"controllerIdentityLocations": []},
            "legacyArchiveAddressing": {
                "allowHttp": True,
                "endpoint": f"http://minio.{ns}.svc.cluster.local:9000",
                "region": "us-east-1",
                "virtualHostedStyle": False,
            },
            "preflight": {"defaultTimeoutSeconds": 120, "retentionSeconds": 3600},
            "version": 1,
        },
        indent=2,
        sort_keys=True,
    )


def kubeconfig(ns: str) -> str:
    return f"""apiVersion: v1
kind: Config
clusters:
- name: scoped-real-api
  cluster:
    server: http://127.0.0.1:8080
contexts:
- name: scoped
  context:
    cluster: scoped-real-api
    user: anonymous-to-local-proxy
    namespace: {ns}
current-context: scoped
users:
- name: anonymous-to-local-proxy
  user: {{}}
"""


def namespaced_role(ns: str, labels: dict[str, str]) -> dict[str, Any]:
    """The shipped ClusterRole's namespaced rules, as a Role in one namespace.

    ONE DELTA FROM `config/rbac/role.yaml`, AND IT IS LISTED HERE. An earlier
    version of this function claimed to be "copied rule for rule" and was wider
    than a shipped install in SEVEN places (review R-12): `+get` on `secrets`,
    `+patch` on `restores`, `+get` on `pods`, `+watch` on `events`, `+list` on
    `configmaps`, `+get` on `preflights`/`topicdiscoveries`/
    `protectionpolicies`/`retentionpolicies`, and `+update` on the status
    subresources. Six of those seven are now gone; the `secrets` one was the
    worst of them, because `config/rbac/role.yaml` carries a banner — "NO VERB
    ON `secrets`, AT ALL, ANYWHERE IN THIS FILE" — and a fence that quietly
    broke a named product invariant is a fence that can launder a 403 into a
    pass.

    THE ONE THAT REMAINS is `update` alongside `patch` on the status
    subresources, and it is there for the pre-upgrade image alone: see
    [`trust_cluster_role`]'s neighbour comment and the report's RBAC finding.
    The current controller never uses it — `patch` is the verb every one of its
    status writes takes — so a row that passes here passes under the grants a
    shipped install has, which is the only way a row's verdict transfers.
    """
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": SA, "namespace": ns, "labels": dict(labels)},
        "rules": [
            {
                "apiGroups": ["logweir.dev"],
                "resources": [
                    "approvals",
                    "backupdestinations",
                    "backups",
                    "backupschedules",
                    "kafkaclusters",
                    "recoverycatalogs",
                    "rehearsalschedules",
                    "restores",
                ],
                "verbs": ["get", "list", "watch"],
            },
            {
                # `list, watch` and NOT `get`, exactly as shipped.
                "apiGroups": ["logweir.dev"],
                "resources": [
                    "preflights",
                    "protectionpolicies",
                    "retentionpolicies",
                    "topicdiscoveries",
                ],
                "verbs": ["list", "watch"],
            },
            {"apiGroups": ["logweir.dev"], "resources": ["backups"],
             "verbs": ["create", "patch"]},
            {"apiGroups": ["logweir.dev"], "resources": ["restores"], "verbs": ["create"]},
            {
                "apiGroups": ["logweir.dev"],
                "resources": [
                    "approvals/status",
                    "backupdestinations/status",
                    "backups/status",
                    "backupschedules/status",
                    "kafkaclusters/status",
                    "preflights/status",
                    "protectionpolicies/status",
                    "recoverycatalogs/status",
                    "rehearsalschedules/status",
                    "restores/status",
                    "retentionpolicies/status",
                    "topicdiscoveries/status",
                ],
                # THE ONE DECLARED DELTA. The shipped role grants `patch` and
                # deliberately grants no `update` anywhere: `reservation_patch`
                # says so and calls a replace "403 on every shipped install".
                # The main@4956785 build sends `Api::replace_status`, a PUT,
                # which the API server authorises as `update`, so without this
                # the conversion and rollback rows cannot make that build write
                # a single status — which this fence measured, and which is the
                # report's RBAC finding. D1 §13.2's L-05.1-3 allows it: "(W0 fix
                # applied only if required for it to fire)".
                "verbs": ["patch", "update"],
            },
            {
                "apiGroups": ["logweir.dev"],
                "resources": ["topicdiscoveries", "preflights"],
                "verbs": ["delete"],
            },
            {
                "apiGroups": ["batch"],
                "resources": ["jobs"],
                "verbs": ["create", "get", "list", "watch", "patch"],
            },
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["list"]},
            {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]},
            {"apiGroups": [""], "resources": ["events"], "verbs": ["list"]},
            {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["create", "get"]},
        ],
    }


# The ONLY verb the fenced Role may hold that the shipped ClusterRole does not.
# Checked against the live `weirkeeper` ClusterRole before the controller is
# deployed, so this cannot drift back to the seven-way divergence review R-12
# found. `*` matches any resource whose name ends `/status`.
DECLARED_ROLE_DELTAS: dict[str, frozenset[str]] = {"*/status": frozenset({"update"})}


def rules_to_grants(rules: list[dict[str, Any]]) -> dict[tuple[str, str], set[str]]:
    """`(apiGroup, resource) -> {verb}`, flattened out of a Role's rule list."""
    grants: dict[tuple[str, str], set[str]] = {}
    for rule in rules:
        for group in rule.get("apiGroups") or [""]:
            for resource in rule.get("resources") or []:
                grants.setdefault((group, resource), set()).update(rule.get("verbs") or [])
    return grants


def role_delta(
    fenced_rules: list[dict[str, Any]],
    shipped_rules: list[dict[str, Any]],
    *,
    declared: dict[str, frozenset[str]] | None = None,
) -> dict[str, list[str]]:
    """Every verb the fenced Role holds that the shipped one does not, minus the declared ones.

    A row that passes behind a wider-than-shipped grant does not prove the
    shipped grant suffices for the behaviour it measured — and the RBAC finding
    in this run's report is the proof that this can bite. So the fence states
    its one deliberate delta and refuses to carry any other.
    """
    declared = DECLARED_ROLE_DELTAS if declared is None else declared
    fenced_grants = rules_to_grants(fenced_rules)
    shipped_grants = rules_to_grants(shipped_rules)
    out: dict[str, list[str]] = {}
    for (group, resource), verbs in sorted(fenced_grants.items()):
        allowed = set(shipped_grants.get((group, resource), set()))
        if resource.endswith("/status"):
            allowed |= set(declared.get("*/status", frozenset()))
        allowed |= set(declared.get(f"{group}/{resource}", frozenset()))
        extra = verbs - allowed
        if extra:
            out[f"{group or 'core'}/{resource}"] = sorted(extra)
    return out


def check_role_is_not_wider_than_shipped(H: Any) -> dict[str, Any]:
    """Refuse to deploy behind a Role wider than the shipped one, bar the declared delta."""
    shipped = json.loads(H.run(H.K + ["get", "clusterrole", "weirkeeper", "-o", "json"]).stdout)
    fenced_rules = namespaced_role(H.NS, dict(H.LABELS))["rules"]
    delta = role_delta(fenced_rules, shipped["rules"])
    if delta:
        raise H.Failure(
            "the fenced Role grants verbs the shipped `weirkeeper` ClusterRole does not, "
            f"beyond the declared {sorted(DECLARED_ROLE_DELTAS)}: {delta}. A row that passes "
            "behind a grant a shipped install lacks does not prove the shipped grant suffices."
        )
    return {
        "shippedClusterRoleUid": shipped["metadata"]["uid"],
        "declaredDeltas": {k: sorted(v) for k, v in DECLARED_ROLE_DELTAS.items()},
        "undeclaredDeltas": delta,
    }


def trust_cluster_role(name: str, labels: dict[str, str]) -> dict[str, Any]:
    """READ-ONLY, and that is the whole point of a cluster-scoped grant here.

    `trustrosters` and `trustpolicies` are cluster-scoped, so their collection
    URL carries no namespace and the proxy cannot scope it: whatever the fenced
    controller is allowed to do to them, it does to the ones the shared release
    also reads. `get/list/watch` and nothing else is therefore the only grant
    that keeps "the shared release is not modified" true. The consequence is
    declared rather than hidden: the fenced controller's trust reconcilers
    cannot patch `trustrosters/status` and will log a 403 for it. No D1 §13.2
    scenario asserts on trust status, and a fence that could write cluster-wide
    would be worth less than the scenario it enabled.
    """
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": name, "labels": dict(labels)},
        "rules": [
            {
                "apiGroups": ["logweir.dev"],
                "resources": ["trustrosters", "trustpolicies"],
                "verbs": ["get", "list", "watch"],
            }
        ],
    }


def trust_cluster_role_binding(name: str, ns: str, labels: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": name, "labels": dict(labels)},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name},
        "subjects": [{"kind": "ServiceAccount", "name": SA, "namespace": ns}],
    }


def admission_policy(name: str, stamp: str, labels: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": name, "labels": dict(labels)},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [
                    {
                        "apiGroups": ["logweir.dev"],
                        "apiVersions": ["*"],
                        "operations": ["CREATE", "UPDATE", "DELETE"],
                        "resources": ["*/*"],
                    },
                    {
                        "apiGroups": ["batch"],
                        "apiVersions": ["v1"],
                        "operations": ["CREATE", "UPDATE", "DELETE"],
                        "resources": ["jobs", "jobs/*"],
                    },
                    {
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "operations": ["CREATE", "UPDATE", "DELETE"],
                        "resources": ["configmaps"],
                    },
                ]
            },
            "validations": [
                {
                    "expression": f"request.userInfo.username != '{SHARED_CONTROLLER_USER}'",
                    "message": (
                        f"D1 fence {stamp}: this namespace is reconciled by its own "
                        "controller; the shared release may not write here"
                    ),
                }
            ],
        },
    }


def admission_binding(name: str, stamp: str, labels: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": name, "labels": dict(labels)},
        "spec": {
            "policyName": name,
            "validationActions": ["Deny"],
            "matchResources": {"namespaceSelector": {"matchLabels": {FENCE_LABEL: stamp}}},
        },
    }


def deployment(
    ns: str,
    labels: dict[str, str],
    *,
    image: str,
    runner_image: str,
    replicas: int = 1,
    log_level: str = "weirkeeper=debug,info",
) -> dict[str, Any]:
    pod_labels = dict(labels)
    pod_labels["app"] = CONTROLLER
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": CONTROLLER, "namespace": ns, "labels": pod_labels},
        "spec": {
            "replicas": replicas,
            # `Recreate`, so "scaled to 0" means no controller is running at
            # all. A rolling update would keep the old pod reconciling while a
            # scenario believes the controller is down.
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": {"app": CONTROLLER}},
            "template": {
                "metadata": {"labels": pod_labels},
                "spec": {
                    "serviceAccountName": SA,
                    "terminationGracePeriodSeconds": 5,
                    "containers": [
                        {
                            "name": "weirkeeper",
                            "image": image,
                            "imagePullPolicy": "Never",
                            "env": [
                                # BLANKED ON PURPOSE: with these set, kube-rs
                                # would pick the in-cluster config and talk to
                                # the API server directly, straight past the
                                # proxy and straight out of the fence.
                                {"name": "KUBERNETES_SERVICE_HOST", "value": ""},
                                {"name": "KUBERNETES_SERVICE_PORT", "value": ""},
                                {"name": "KUBERNETES_SERVICE_PORT_HTTPS", "value": ""},
                                {"name": "KUBECONFIG", "value": "/kube/config"},
                                {"name": "RUST_LOG", "value": log_level},
                                {"name": "LOGWEIR_RUNNER_IMAGE", "value": runner_image},
                                {"name": "LOGWEIR_RUNNER_PULL_POLICY", "value": "Never"},
                                {
                                    "name": "LOGWEIR_ARCHIVE_URL",
                                    "value": "s3://d1-archive/fence",
                                },
                                {"name": "LOGWEIR_POLICY_CONFIGMAP", "value": POLICY_CONFIGMAP},
                                {
                                    "name": "LOGWEIR_INSTALLATION_NAMESPACE",
                                    "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}},
                                },
                                {
                                    "name": "AWS_ENDPOINT_URL",
                                    "value": f"http://minio.{ns}.svc.cluster.local:9000",
                                },
                                {"name": "AWS_ALLOW_HTTP", "value": "true"},
                                {"name": "AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "value": "false"},
                                {"name": "AWS_REGION", "value": "us-east-1"},
                                {
                                    "name": "AWS_ACCESS_KEY_ID",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "logweir-s3",
                                            "key": "access-key-id",
                                            "optional": True,
                                        }
                                    },
                                },
                                {
                                    "name": "AWS_SECRET_ACCESS_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "logweir-s3",
                                            "key": "secret-access-key",
                                            "optional": True,
                                        }
                                    },
                                },
                            ],
                            "volumeMounts": [
                                {"name": "kubeconfig", "mountPath": "/kube", "readOnly": True}
                            ],
                        },
                        {
                            "name": "scope-proxy",
                            "image": "python:3.12-alpine",
                            "imagePullPolicy": "Never",
                            "command": ["python3", "/proxy/plat04_scope_proxy.py"],
                            "env": [{"name": "PLAT04_NAMESPACE", "value": ns}],
                            "ports": [{"name": "proxy", "containerPort": 8080}],
                            "volumeMounts": [
                                {"name": "proxy", "mountPath": "/proxy", "readOnly": True}
                            ],
                        },
                    ],
                    "volumes": [
                        {"name": "kubeconfig", "configMap": {"name": KUBECONFIG_CONFIGMAP}},
                        {"name": "proxy", "configMap": {"name": PROXY_CONFIGMAP}},
                    ],
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# Lifecycle, driven through the harness module `H` (scripts/live/d1/run.py)
# ---------------------------------------------------------------------------


def image_revision(H: Any, image: str) -> str:
    result = H.run(
        [
            "docker",
            "--context",
            "desktop-linux",
            "inspect",
            image,
            "--format",
            '{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}',
        ],
        timeout=60,
    )
    return result.stdout.strip()


# Paths that cannot change what an image contains: the live harnesses
# themselves, the fixtures they mount into their own pods, and prose. Anything
# else is a product build input, and a change to one means the lab is running
# different code from the code this checkout describes.
#
# DELIBERATELY AN ALLOWLIST. A product directory this list has never heard of
# is refused rather than silently tolerated, which is the direction a guard
# should fail in.
NON_IMAGE_PATHS = ("scripts/live/", "scripts/fixtures/", "e2e/", "docs/")


def assert_checkout_contains(H: Any, revision: str) -> dict[str, Any]:
    """Refuse unless the product code in this checkout IS the build under test.

    NOT `origin/main`: that ref moves the moment any worker's branch lands, and
    a lab refreshed an hour earlier would then be "wrong" although nothing
    about it had changed — observed mid-run on 2026-09-18, when origin/main
    advanced past the commit the lab was built from and
    `d2_live.py::revision_guard` refused a run that was measuring exactly the
    right thing. A guard that a third party can break by pushing is the same
    stale-pin failure this file already had, with the constant moved into a ref.

    NOT `HEAD` either: a harness branch carries test-only commits over the
    build the lab runs, and requiring equality would make every harness change
    un-runnable against a lab that is otherwise exactly right.

    What has to be true is narrower, and checkable: this checkout KNOWS the
    commit the image was built from, CONTAINS it, and every TRACKED thing that
    has moved since — committed or not — is a file no image contains. Untracked
    files are ignored and recorded: a file git does not track cannot have been
    in the build, and one of them is the orchestrator's own brief at the
    repository root, which no worker may delete (lab-refresh-4 §9.1).
    """
    head = H.run(["git", "rev-parse", "HEAD"], timeout=60, record=False).stdout.strip()
    if H.run(["git", "cat-file", "-e", f"{revision}^{{commit}}"],
             timeout=60, check=False, record=False).returncode:
        raise H.Failure(
            f"the image names revision {revision!r}, which this checkout does not have; it "
            "cannot say what code that image contains. Fetch the commit, or rebuild the "
            "image from one this checkout has"
        )
    if H.run(["git", "merge-base", "--is-ancestor", revision, "HEAD"],
             timeout=60, check=False, record=False).returncode:
        raise H.Failure(
            f"the image was built from {revision!r}, which is not an ancestor of this "
            f"checkout's HEAD {head!r}: the lab is running a build this branch does not "
            "contain, so nothing measured here describes this code"
        )
    changed = [
        line.strip()
        for line in H.run(["git", "diff", "--name-only", revision, "HEAD"],
                          timeout=120, record=False).stdout.splitlines()
        if line.strip()
    ]
    # TRACKED CONTENT ONLY, and this is the rule stated correctly rather than a
    # relaxation of it. `git status --porcelain` lists untracked files too, and
    # the repository root carries one that the orchestrator owns — `prompt`, the
    # brief itself — which no image contains, which no worker may delete under
    # WORKER-RULES, and which the non-image allowlist cannot cover because it is
    # a root-level file rather than a directory. Refusing on it made a lab that
    # matched the checkout exactly un-runnable, and pushed lab-refresh-4 into
    # `--fence-revision`, a flag that SUSPENDS this very check (§9.1). An
    # untracked file cannot change what an image was built from; only tracked
    # content can. What is ignored is recorded, so the evidence names it.
    untracked = [
        line[3:].strip()
        for line in H.run(["git", "status", "--porcelain", "--untracked-files=all"],
                          timeout=120, record=False).stdout.splitlines()
        if line.startswith("?? ")
    ]
    dirty = [
        line[3:].strip()
        for line in H.run(["git", "status", "--porcelain", "--untracked-files=no"],
                          timeout=120, record=False).stdout.splitlines()
        if line.strip()
    ]
    product = sorted({p for p in changed + dirty if not p.startswith(NON_IMAGE_PATHS)})
    if product:
        raise H.Failure(
            f"the lab runs {revision[:12]} and this checkout has moved product files since: "
            f"{product[:8]}{' …' if len(product) > 8 else ''}. A run that measures one build "
            "and asserts against another is not evidence. REBUILD THE LAB IMAGES. "
            f"`--fence-revision {revision[:12]}…` also clears this, and it clears it by "
            "SUSPENDING the comparison rather than satisfying it — take that door only when "
            "the difference is deliberate and you want the older build measured"
        )
    return {
        "checkoutHead": head,
        "imageRevisionInHistory": True,
        "changedSinceImage": changed,
        "uncommitted": dirty,
        "untrackedIgnored": untracked,
    }


def expected_revision(H: Any, which: str, observed: str) -> tuple[str, str]:
    """The revision this run requires, and where that requirement came from.

    Three sources, in order: an explicit `--fence-revision`, the constant beside
    a frozen image, and — for the mutable `new` tags — the running image's own
    OCI label. Only the discovered case is cross-checked against the checkout,
    because it is the only one whose expectation the image itself supplied.
    """
    if which in REVISION_OVERRIDE:
        return REVISION_OVERRIDE[which], "flag"
    pinned = IMAGES[which][1]
    if pinned:
        return pinned, "pinned"
    if not observed or observed == NO_LABEL:
        raise H.Failure(
            f"{IMAGES[which][0]} carries no org.opencontainers.image.revision label, so the "
            "revision it was built from cannot be discovered; rebuild it with "
            "`--label org.opencontainers.image.revision=$(git rev-parse HEAD)` or pass "
            "`--fence-revision <sha>`"
        )
    return observed, "label"


def assert_source_matched(H: Any, which: str) -> dict[str, str]:
    """Refuse to measure a build this run cannot name.

    TWO CHECKS, AND ONLY ONE OF THEM IS UNCONDITIONAL.

    1. **The label match** always runs: both images of a pair must carry the
       revision this run expects, whatever the expectation came from. A flag
       cannot make an image that carries something else pass.
    2. **The drift check** (`assert_checkout_contains`) runs only when the
       expectation was DISCOVERED from the image's own label. A pinned
       expectation — `IMAGES["old"]`, or `--fence-revision` — stands it down,
       and that is what the pin is for: `weirkeeper:plat0102-4956785` is
       deliberately an older build than this checkout, and an operator whose
       checkout is deliberately ahead of the lab needs the same door.

    So `--fence-revision` SUSPENDS DRIFT DETECTION. It is not a way to silence
    a refusal you did not expect — a run taken through it measures one build
    and asserts against another, and the only thing that makes that evidence is
    an operator who meant it. `revisionSource` and `driftCheck` go into the
    row's provenance either way, so the record says which door the run came
    through (review L-4).
    """
    observed: dict[str, tuple[str, str, str]] = {}
    for label, ref in (("controller", IMAGES[which][0]), ("runner", RUNNER_IMAGES[which][0])):
        image_id, _, got = image_revision(H, ref).partition(" ")
        observed[label] = (ref, image_id, got.strip())
    want, source = expected_revision(H, which, observed["controller"][2])
    out: dict[str, str] = {"revisionSource": source}
    for label, (ref, image_id, got) in observed.items():
        if got != want:
            raise H.Failure(
                f"{label} image {ref} is revision {got!r}, not {want!r} (expected from the "
                f"{source}); a run that cannot name the revision it measured is not evidence"
            )
        out[f"{label}Image"] = ref
        out[f"{label}ImageId"] = image_id
        out[f"{label}Revision"] = got
    if source == "label":
        out["driftCheck"] = "enforced"
        out.update(assert_checkout_contains(H, want))
    else:
        # STOOD DOWN, AND SAID SO IN THE EVIDENCE. A reader of `results.json`
        # can see that this run's checkout was never compared with the build it
        # measured, and why.
        out["driftCheck"] = f"suspended: the expected revision came from the {source}"
        out["checkoutHead"] = H.run(["git", "rev-parse", "HEAD"], timeout=60,
                                    record=False).stdout.strip()
    return out


def create_fence(H: Any) -> dict[str, Any]:
    """The cluster-scoped half. Requires the cluster lock; created BEFORE the
    namespace exists, so there is no window in which the shared controller can
    write into it."""
    name = f"d1fence-isolate-{H.STAMP}"
    labels = dict(H.LABELS)
    policy = H.apply(admission_policy(name, H.STAMP, labels))
    binding = H.apply(admission_binding(name, H.STAMP, labels))
    return {
        "policy": name,
        "policyUid": policy["metadata"]["uid"],
        "bindingUid": binding["metadata"]["uid"],
        "fenceLabel": f"{FENCE_LABEL}={H.STAMP}",
        "deniedUser": SHARED_CONTROLLER_USER,
    }


def check_table_covers_the_cluster(H: Any) -> dict[str, Any]:
    """Refuse to deploy behind a proxy whose rewrite table misses a served CRD.

    The unit test compares the table with `config/crd`; this compares it with
    what the API server is actually serving right now, which is the thing the
    fenced controller will watch.
    """
    import sys

    sys.path.insert(0, str(H.ROOT / "scripts" / "fixtures"))
    import plat04_scope_proxy as proxy  # noqa: PLC0415

    served = json.loads(H.run(H.K + ["get", "crd", "-o", "json"]).stdout)["items"]
    namespaced = {
        c["spec"]["names"]["plural"]
        for c in served
        if c["spec"]["group"] == "logweir.dev" and c["spec"]["scope"] == "Namespaced"
    }
    cluster = {
        c["spec"]["names"]["plural"]
        for c in served
        if c["spec"]["group"] == "logweir.dev" and c["spec"]["scope"] == "Cluster"
    }
    if namespaced != set(proxy.NAMESPACED_LOGWEIR) or cluster != set(proxy.CLUSTER_SCOPED_LOGWEIR):
        raise H.Failure(
            "the proxy rewrite table does not match the CRDs this cluster serves: "
            f"namespaced only-in-cluster={sorted(namespaced - set(proxy.NAMESPACED_LOGWEIR))}, "
            f"only-in-proxy={sorted(set(proxy.NAMESPACED_LOGWEIR) - namespaced)}, "
            f"cluster-scoped only-in-cluster={sorted(cluster - set(proxy.CLUSTER_SCOPED_LOGWEIR))}, "
            f"only-in-proxy={sorted(set(proxy.CLUSTER_SCOPED_LOGWEIR) - cluster)}"
        )
    return {
        "servedNamespaced": sorted(namespaced),
        "servedClusterScoped": sorted(cluster),
        "tableMatches": True,
    }


def deploy(H: Any, which: str = "new", *, replicas: int = 1) -> dict[str, Any]:
    labels = dict(H.LABELS)
    provenance = assert_source_matched(H, which)
    table = check_table_covers_the_cluster(H)
    rbac = check_role_is_not_wider_than_shipped(H)
    proxy_source = (H.ROOT / "scripts/fixtures/plat04_scope_proxy.py").read_text()
    for manifest in (
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": SA, "namespace": H.NS, "labels": labels},
        },
        namespaced_role(H.NS, labels),
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": SA, "namespace": H.NS, "labels": labels},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": SA},
            "subjects": [{"kind": "ServiceAccount", "name": SA, "namespace": H.NS}],
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": PROXY_CONFIGMAP, "namespace": H.NS, "labels": labels},
            "data": {"plat04_scope_proxy.py": proxy_source},
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": KUBECONFIG_CONFIGMAP, "namespace": H.NS, "labels": labels},
            "data": {"config": kubeconfig(H.NS)},
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": POLICY_CONFIGMAP, "namespace": H.NS, "labels": labels},
            "data": {"policy.json": policy_json(H.NS)},
        },
    ):
        H.apply(manifest)
    cluster_name = f"d1fence-trust-{H.STAMP}"
    H.apply(trust_cluster_role(cluster_name, labels))
    H.apply(trust_cluster_role_binding(cluster_name, H.NS, labels))
    obj = H.apply(
        deployment(
            H.NS,
            labels,
            image=provenance["controllerImage"],
            runner_image=provenance["runnerImage"],
            replicas=replicas,
        )
    )
    H.kn("rollout", "status", f"deployment/{CONTROLLER}", "--timeout=180s", timeout=200)
    record = {
        "which": which,
        "deploymentUid": obj["metadata"]["uid"],
        "clusterRole": cluster_name,
        "replicas": replicas,
        "rewriteTable": table,
        "rbac": rbac,
        **provenance,
        **pod_provenance(H),
    }
    return record


def pods(H: Any) -> list[dict[str, Any]]:
    return [
        p
        for p in H.lst("pods", f"app={CONTROLLER}")
        if (p.get("status") or {}).get("phase") == "Running"
        and all(c.get("ready") for c in (p["status"].get("containerStatuses") or []))
        and not p["metadata"].get("deletionTimestamp")
    ]


def pod_provenance(H: Any) -> dict[str, Any]:
    return {
        "pods": [
            {
                "name": p["metadata"]["name"],
                "uid": p["metadata"]["uid"],
                "imageID": next(
                    (
                        c.get("imageID")
                        for c in p["status"]["containerStatuses"]
                        if c["name"] == "weirkeeper"
                    ),
                    None,
                ),
                "startedAt": p["status"].get("startTime"),
            }
            for p in pods(H)
        ]
    }


def set_initial_arm(H: Any, query: str = "") -> None:
    """Pre-arm the proxy for the pod's whole life, before any request arrives.

    The proxy and the controller start together, so an arming sent over the
    control API after a restart is always racing the controller's first
    requests. L-05.2-3's "the proxy answers the FIRST migration PATCH after
    restart with 409" is not satisfiable any other way.
    """
    H.kn(
        "set",
        "env",
        f"deployment/{CONTROLLER}",
        f"PLAT04_ARM={query}" if query else "PLAT04_ARM-",
        "-c",
        "scope-proxy",
    )


def controller_started_at(H: Any) -> list[str]:
    """When the weirkeeper CONTAINER started, which is when the controller is back.

    `kubectl scale` returning, and even `rollout status` returning, are both
    earlier than this, and L-04-2's 70–110 s restart window is measured against
    the controller being back — not against the harness having asked for it.
    """
    out = []
    for pod in pods(H):
        for status in pod["status"].get("containerStatuses") or []:
            if status["name"] == "weirkeeper":
                running = (status.get("state") or {}).get("running") or {}
                if running.get("startedAt"):
                    out.append(running["startedAt"])
    return sorted(out)


def scale(H: Any, replicas: int, *, timeout: int = 200) -> dict[str, Any]:
    H.kn("scale", "deployment", CONTROLLER, f"--replicas={replicas}")
    if replicas:
        H.kn("rollout", "status", f"deployment/{CONTROLLER}", f"--timeout={timeout - 20}s",
             timeout=timeout)
        H.wait_until(lambda: len(pods(H)) == replicas, timeout=timeout,
                     what=f"{replicas} fenced controller pods")
        H.wait_until(lambda: proxy_state(H) is not None, timeout=60, what="the proxy to answer")
    else:
        H.wait_until(
            lambda: not H.lst("pods", f"app={CONTROLLER}"),
            timeout=timeout,
            what="the fenced controller to be gone",
        )
    return {
        "replicas": replicas,
        "at": H.now(),
        "controllerStartedAt": controller_started_at(H) if replicas else [],
        **pod_provenance(H),
    }


def swap_image(H: Any, which: str) -> dict[str, Any]:
    provenance = assert_source_matched(H, which)
    H.kn(
        "set",
        "image",
        f"deployment/{CONTROLLER}",
        f"weirkeeper={provenance['controllerImage']}",
    )
    H.kn(
        "set",
        "env",
        f"deployment/{CONTROLLER}",
        f"LOGWEIR_RUNNER_IMAGE={provenance['runnerImage']}",
        "-c",
        "weirkeeper",
    )
    H.kn("rollout", "status", f"deployment/{CONTROLLER}", "--timeout=180s", timeout=200)
    H.wait_until(lambda: proxy_state(H) is not None, timeout=60, what="the proxy to answer")
    return {"which": which, "swappedAt": H.now(), **provenance, **pod_provenance(H)}


# ---------------------------------------------------------------------------
# Proxy control
# ---------------------------------------------------------------------------


def proxy_call(H: Any, path: str, *, pod: str | None = None) -> dict[str, Any] | None:
    running = pods(H)
    if not running:
        return None
    name = pod or running[0]["metadata"]["name"]
    result = H.run(
        H.KN + ["exec", name, "-c", "scope-proxy", "--", "wget", "-qO-",
                f"http://127.0.0.1:8080{path}"],
        timeout=60,
        check=False,
        record=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return json.loads(result.stdout)


def proxy_state(H: Any, *, pod: str | None = None) -> dict[str, Any] | None:
    return proxy_call(H, "/__state", pod=pod)


def proxy_reset(H: Any) -> None:
    for pod in pods(H):
        proxy_call(H, "/__reset", pod=pod["metadata"]["name"])


def proxy_requests(H: Any, since: float = 0.0, *, pod: str | None = None) -> dict[str, Any] | None:
    return proxy_call(H, f"/__requests?since={since}", pod=pod)


def arm(
    H: Any,
    kind: str,
    name: str = "",
    mode: str = "pause",
    *,
    code: int = 503,
    after: int = 0,
    repeat: bool = False,
    pod: str | None = None,
) -> dict[str, Any] | None:
    query = (
        f"/__arm?kind={kind}&name={name}&mode={mode}&code={code}&after={after}"
        f"&repeat={'true' if repeat else 'false'}"
    )
    return proxy_call(H, query, pod=pod)


def release(H: Any, *, pod: str | None = None) -> dict[str, Any] | None:
    return proxy_call(H, "/__release", pod=pod)


def disarm(H: Any, *, pod: str | None = None) -> dict[str, Any] | None:
    return proxy_call(H, "/__disarm", pod=pod)


def captures(H: Any, kind: str | None = None, *, pod: str | None = None) -> list[dict[str, Any]]:
    state = proxy_state(H, pod=pod)
    if not state:
        return []
    items = state.get("captures", [])
    if kind is None:
        return list(items)
    return [i for i in items if i.get("kind") == kind]


def controller_logs(H: Any, *, since: str = "10m", tail: int = 4000) -> str:
    out: list[str] = []
    for pod in pods(H):
        result = H.run(
            H.KN + ["logs", pod["metadata"]["name"], "-c", "weirkeeper",
                    f"--since={since}", f"--tail={tail}"],
            check=False,
            timeout=120,
            record=False,
        )
        out.append(f"### {pod['metadata']['name']}\n{result.stdout}")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# The fence proof
# ---------------------------------------------------------------------------


def prove(H: Any) -> dict[str, Any]:
    """Three readings, because one of them alone would not settle it.

    1. **The shared controller's identity is refused, live.** A CREATE
       impersonating its ServiceAccount is answered by the API server's
       admission chain with this policy's message. A CREATE as the harness
       itself is accepted at the same instant, so the denial is the policy and
       not a broken namespace.
    2. **The real shared controller pod is refused too.** Its own log is read
       (read-only; the release is never modified) for the denial message naming
       this namespace — impersonation proves the rule, this proves the rule is
       reaching the process it was written for.
    3. **The fenced controller's own writes land**, and the proxy's counters
       show the requests arriving through it, which is what says the fenced
       controller is inside the fence rather than beside it.
    """
    proof: dict[str, Any] = {"checkedAt": H.now(), "deniedUser": SHARED_CONTROLLER_USER}
    probe = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"d1fence-probe-{int(time.time())}",
            "namespace": H.NS,
            "labels": dict(H.LABELS),
        },
        "data": {"probe": "the shared controller must not be able to write this"},
    }
    denied = H.run(
        H.K + ["--as", SHARED_CONTROLLER_USER, "-n", H.NS, "apply", "-f", "-"],
        data=json.dumps(probe),
        check=False,
        timeout=60,
    )
    proof["impersonatedCreate"] = {
        "returnCode": denied.returncode,
        "stderr": denied.stderr.strip()[:600],
        "denied": denied.returncode != 0 and "d1fence" in denied.stderr,
    }
    allowed = H.run(H.K + ["-n", H.NS, "apply", "-f", "-"], data=json.dumps(probe), check=False,
                    timeout=60)
    proof["harnessCreate"] = {"returnCode": allowed.returncode, "created": allowed.returncode == 0}
    H.run(H.KN + ["delete", "configmap", probe["metadata"]["name"], "--ignore-not-found"],
          check=False, timeout=60)

    shared_pods = json.loads(
        H.run(
            H.K + ["-n", SHARED_NS, "get", "pods", "-l",
                   "app.kubernetes.io/component=control-plane", "-o", "json"]
        ).stdout
    )["items"]
    lines: list[str] = []
    for pod in shared_pods:
        result = H.run(
            H.K + ["-n", SHARED_NS, "logs", pod["metadata"]["name"], "--since=15m", "--tail=4000"],
            check=False,
            timeout=120,
            record=False,
        )
        lines += [ln for ln in result.stdout.splitlines() if H.NS in ln and "d1fence" in ln]
    proof["sharedControllerDenials"] = {
        "matchedLines": len(lines),
        "sample": lines[-5:],
    }

    state = proxy_state(H)
    proof["proxy"] = {
        "reachable": state is not None,
        "namespace": (state or {}).get("namespace"),
        "counts": (state or {}).get("counts", {}),
        "unknownResources": (state or {}).get("unknownResources", {}),
    }
    # WHICH READINGS GATE, SPELLED OUT (review R-1). Reading 2 — the real shared
    # controller's own denials — is RECORDED and NOT GATED, because at `prove`
    # time the namespace is seconds old and the shared controller has not tried
    # to write into it yet; gating on it here would fail every run. It becomes
    # true a minute later and the harness re-reads it into
    # `logs/shared-controller-denials.log`. Saying "all three gate" would be
    # false, so the proof says which is which.
    proof["gatedReadings"] = [
        "impersonatedCreate.denied",
        "harnessCreate.created",
        "proxy.reachable and proxy.namespace == this namespace",
        "proxy.unknownResources is empty",
    ]
    proof["recordedNotGatedReadings"] = [
        "sharedControllerDenials.matchedLines (0 at setup time is expected and not a failure)"
    ]
    proof["fenced"] = bool(
        proof["impersonatedCreate"]["denied"]
        and proof["harnessCreate"]["created"]
        and proof["proxy"]["reachable"]
        and proof["proxy"]["namespace"] == H.NS
        and not proof["proxy"]["unknownResources"]
    )
    return proof


def teardown(H: Any) -> dict[str, Any]:
    """Delete the cluster-scoped half. The namespaced half goes with the namespace."""
    removed: dict[str, Any] = {"at": H.now(), "deleted": [], "remaining": []}
    name = f"d1fence-isolate-{H.STAMP}"
    cluster_name = f"d1fence-trust-{H.STAMP}"
    for kind, target in (
        ("validatingadmissionpolicybinding", name),
        ("validatingadmissionpolicy", name),
        ("clusterrolebinding", cluster_name),
        ("clusterrole", cluster_name),
    ):
        obj = H.get_opt(kind, target, namespace=None)
        if obj is None:
            removed["deleted"].append(f"{kind}/{target} (already absent)")
            continue
        labels = obj["metadata"].get("labels") or {}
        if labels.get("logweir.dev/test-owner") != H.OWNER:
            removed["remaining"].append(f"{kind}/{target} REFUSED: not ours ({labels})")
            continue
        H.run(H.K + ["delete", kind, target, "--ignore-not-found"], check=False, timeout=120)
        removed["deleted"].append(f"{kind}/{target} uid={obj['metadata']['uid']}")
    for kind, target in (
        ("validatingadmissionpolicybinding", name),
        ("validatingadmissionpolicy", name),
        ("clusterrolebinding", cluster_name),
        ("clusterrole", cluster_name),
    ):
        if H.get_opt(kind, target, namespace=None) is not None:
            removed["remaining"].append(f"{kind}/{target} STILL PRESENT")
    removed["clean"] = not removed["remaining"]
    return removed
