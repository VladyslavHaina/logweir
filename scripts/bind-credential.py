#!/usr/bin/env python3
"""Bind ONE credential Secret to ONE Logweir object or inline-archive location
(FX-20) — the upgrade step for credentials created before the binding existed.

Since FX-20 (and PROD-01.3 for Kafka connections) every runner refuses a
credential whose Secret does not carry, under its `logweir-binding` key, the
binding of the object (or, for an inline archive, the location) it is used
for: `CredentialBindingMismatch`, before any request is signed. A Secret made
by an earlier release carries none, so its object's runs are refused until it
is bound — fail closed.

THE BINDING STEP CAN ITSELF PERFORM THE THEFT THE BINDING PREVENTS. Before
this release nothing stopped an object from naming somebody else's Secret
beside an endpoint its author controls, so such a "thief" object may already
exist. Binding "every Secret its object names" in a loop would bind a victim's
Secret to whichever object came last. This tool therefore binds exactly one
Secret to exactly one object, named by the operator, and REFUSES (exit 3,
nothing written) when:

* the object does not name that Secret;
* ANY other Logweir object in the namespace names that Secret (an object-bound
  credential) — or, for an inline-archive location, any object-bound kind
  names it, or an inline archive names it at another bucket: that is an
  INCIDENT to investigate, never a Secret to split or copy;
* the Secret is owned by another object, or labelled as minted for another
  destination or connection;
* the Secret already carries a binding that is not this one (re-binding is
  never automatic; see docs/kubernetes.md §20.10 for adding a second one by
  hand, deliberately);
* the object has not published its binding yet (upgrade the controller first).

It is a DRY RUN unless `--apply` is given, and `--apply` also needs
`--confirm-endpoint` set to the endpoint the dry run printed — the credential's
owner confirms WHERE the credential will be presented before anything is
written (PROD-01.3 review P2). The write is one merge patch of the Secret's
`stringData.logweir-binding`, preconditioned on the resourceVersion read.

Usage:

    python3 scripts/bind-credential.py --context CTX --namespace NS \\
        --kind BackupDestination|RetentionPolicy|ProtectionPolicy|KafkaCluster \\
        --name OBJECT [--route ROUTE] --secret SECRET [--apply --confirm-endpoint E]

    python3 scripts/bind-credential.py --context CTX --namespace NS \\
        --location s3://BUCKET[/PREFIX] --endpoint URL|aws --secret SECRET \\
        [--apply --confirm-endpoint E]

`--context` is required: this tool never uses a kubeconfig's current context.
It needs `kubectl`, read on the Logweir kinds, `get` on the one Secret (its
metadata and its current binding — never printed) and `patch` on it.

Exit codes: 0 bound, already bound, or dry run; 3 refused (nothing written);
2 usage; 1 kubectl failed.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass, field

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_REFUSED = 3

BINDING_KEY = "logweir-binding"
KUBECTL_TIMEOUT_SECONDS = 60

OBJECT_KINDS = {
    "KafkaCluster": "kafkaclusters",
    "BackupDestination": "backupdestinations",
    "RetentionPolicy": "retentionpolicies",
    "ProtectionPolicy": "protectionpolicies",
}

# Every Logweir kind that can name a credential Secret, and the paths it names
# one at. `object` kinds bind to the object's UID; `inline` kinds bind to the
# archive LOCATION (their URL is the second path).
INVENTORY = [
    ("kafkaclusters", "KafkaCluster", "object",
     [("spec", "auth", "secretRef", "name"), ("spec", "auth", "clientCertificate", "name")], None),
    ("backupdestinations", "BackupDestination", "object",
     [("spec", "access", role, "secret", "name")
      for role in ("archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead")], None),
    ("retentionpolicies", "RetentionPolicy", "object",
     [("spec", "enforcement", "credentialSecretRef", "name")], None),
    ("protectionpolicies", "ProtectionPolicy", "object", "routes", None),
    ("backups", "Backup", "inline",
     [("spec", "archive", "secretRef", "name")], ("spec", "archive", "url")),
    ("backupschedules", "BackupSchedule", "inline",
     [("spec", "archive", "secretRef", "name")], ("spec", "archive", "url")),
    ("restores", "Restore", "inline",
     [("spec", "sourceArchive", "secretRef", "name")], ("spec", "sourceArchive", "url")),
    ("recoverycatalogs", "RecoveryCatalog", "inline",
     [("spec", "legacyArchive", "secretRef", "name")], ("spec", "legacyArchive", "url")),
    ("preflights", "Preflight", "inline",
     [("spec", "request", "backup", "legacyArchive", "secretRef", "name"),
      ("spec", "request", "restore", "legacySourceArchive", "secretRef", "name")],
     None),
]

ROUTE_REFS = (
    ("pagerduty", ("pagerDuty", "routingKeySecretRef", "name")),
    ("webhook", ("webhook", "urlSecretRef", "name")),
    ("slack", ("slack", "webhookUrlSecretRef", "name")),
)


class Refused(Exception):
    """Nothing was written, and the message says why."""


class KubectlFailed(Exception):
    """kubectl did not answer."""


@dataclass
class Reference:
    kind: str
    name: str
    basis: str  # "object" or "inline"
    url: str | None = None

    def label(self) -> str:
        return f"{self.kind}/{self.name}"


@dataclass
class Target:
    """What the Secret is to be bound to, and what an owner must confirm."""

    kind: str
    name: str
    uid: str
    binding: str
    endpoint: str
    names_secret: bool
    detail: list[str] = field(default_factory=list)


def dig(obj, path):
    for part in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def location_binding(url: str, endpoint: str) -> str:
    """The binding of an inline archive's Secret: the scheme, the bucket (or
    account and container) and the endpoint, never the prefix — the same
    canonical form as `logweir_core::credential_binding::archive_location_binding`,
    which `scripts/test_bind_credential_rows.py` and the Rust crate both check
    against `e2e/fixtures/credential-binding/location.json`."""
    if "://" not in url:
        raise Refused(f"`{url}` is not an object-store URL")
    scheme, rest = url.split("://", 1)
    rest = rest.strip("/")
    first, _, tail = rest.partition("/")
    if scheme == "s3":
        e = (endpoint or "").strip()
        normalized = "aws" if e in ("", "aws") else e.rstrip("/").lower()
        lines = [("scheme", "s3"), ("bucket", first), ("endpoint", normalized)]
    elif scheme == "gs":
        lines = [("scheme", "gs"), ("bucket", first)]
    elif scheme == "az":
        container = tail.split("/", 1)[0]
        lines = [("scheme", "az"), ("account", first), ("container", container)]
    else:
        raise Refused(f"`{scheme}://` is not a location this tool can bind")
    canonical = "logweir-credential-binding/v1\nkind=ArchiveLocation\nsubject=location\n"
    canonical += "".join(f"{k}={v}\n" for k, v in lines)
    return "v1:location:sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


def bucket_of(url: str | None) -> str | None:
    if not url or "://" not in url:
        return None
    return url.split("://", 1)[0] + "://" + url.split("://", 1)[1].strip("/").split("/", 1)[0]


class Kubectl:
    """The one seam to the cluster: `get_json` and `patch_secret`."""

    def __init__(self, context: str, namespace: str, runner=None):
        self.context = context
        self.namespace = namespace
        self.runner = runner or self._run

    @staticmethod
    def _run(argv: list[str]) -> tuple[int, str, str]:
        try:
            done = subprocess.run(argv, capture_output=True, text=True,
                                  timeout=KUBECTL_TIMEOUT_SECONDS, check=False)
        except subprocess.TimeoutExpired:
            return 124, "", f"kubectl did not answer within {KUBECTL_TIMEOUT_SECONDS}s"
        except FileNotFoundError:
            return 127, "", "kubectl is not on PATH"
        return done.returncode, done.stdout, done.stderr

    def _argv(self, *rest: str) -> list[str]:
        return ["kubectl", "--context", self.context, "--namespace", self.namespace,
                "--request-timeout=30s", *rest]

    def get_json(self, resource: str, name: str | None = None, missing_ok: bool = False):
        argv = self._argv("get", resource, *([name] if name else []), "-o", "json")
        rc, out, err = self.runner(argv)
        if rc != 0:
            if missing_ok and ("NotFound" in err or "the server doesn't have a resource" in err):
                return None
            raise KubectlFailed(f"{' '.join(argv[:6])} … failed ({rc}): {err.strip()[:300]}")
        try:
            return json.loads(out)
        except json.JSONDecodeError as e:
            raise KubectlFailed(f"kubectl answered no JSON for {resource}: {e}") from e

    def patch_secret(self, name: str, resource_version: str, binding: str) -> None:
        body = json.dumps({"metadata": {"resourceVersion": resource_version},
                           "stringData": {BINDING_KEY: binding}})
        argv = self._argv("patch", "secret", name, "--type", "merge", "-p", body)
        rc, _, err = self.runner(argv)
        if rc != 0:
            raise KubectlFailed(f"the patch of Secret {name} failed ({rc}): {err.strip()[:300]}")


def inventory(kube: Kubectl, secret: str) -> list[Reference]:
    """Every Logweir object in the namespace that names `secret`."""
    found: list[Reference] = []
    for plural, kind, basis, paths, url_path in INVENTORY:
        listing = kube.get_json(plural, missing_ok=True)
        for item in (listing or {}).get("items", []):
            name = dig(item, ("metadata", "name")) or "?"
            names: set[str] = set()
            if paths == "routes":
                for route in dig(item, ("spec", "notifications", "routes")) or []:
                    for _, path in ROUTE_REFS:
                        value = dig(route, path)
                        if value:
                            names.add(value)
            else:
                for path in paths:
                    value = dig(item, path)
                    if value:
                        names.add(value)
            if secret in names:
                url = dig(item, url_path) if url_path else None
                found.append(Reference(kind, name, basis, url))
    return found


def object_target(kube: Kubectl, kind: str, name: str, secret: str, route: str | None) -> Target:
    obj = kube.get_json(OBJECT_KINDS[kind], name, missing_ok=True)
    if obj is None:
        raise Refused(f"{kind} {name} does not exist in this namespace")
    uid = dig(obj, ("metadata", "uid")) or ""
    status = obj.get("status") or {}
    spec = obj.get("spec") or {}
    detail: list[str] = []
    if kind == "KafkaCluster":
        names = {dig(spec, ("auth", "secretRef", "name")),
                 dig(spec, ("auth", "clientCertificate", "name"))}
        binding = status.get("credentialBinding") or ""
        endpoint = ",".join(spec.get("bootstrapServers") or [])
    elif kind == "BackupDestination":
        names = {dig(spec, ("access", r, "secret", "name"))
                 for r in ("archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead")}
        binding = status.get("credentialBinding") or ""
        storage = spec.get("storage") or {}
        endpoint = f"{storage.get('endpoint') or 'aws'} bucket={storage.get('bucket', '')}"
    elif kind == "RetentionPolicy":
        names = {dig(spec, ("enforcement", "credentialSecretRef", "name"))}
        binding = status.get("credentialBinding") or ""
        dest_name = dig(spec, ("destinationRef", "name")) or ""
        dest = kube.get_json("backupdestinations", dest_name, missing_ok=True) or {}
        storage = dig(dest, ("spec", "storage")) or {}
        endpoint = (f"{storage.get('endpoint') or 'aws'} bucket={storage.get('bucket', '')} "
                    f"scope={dig(spec, ('scope', 'prefix')) or ''}")
        detail.append(f"destination {dest_name} (uid {dig(dest, ('metadata', 'uid')) or '?'})")
    else:  # ProtectionPolicy
        names = set()
        endpoints = {}
        for r in dig(spec, ("notifications", "routes")) or []:
            if route and r.get("name") != route:
                continue
            for sink, path in ROUTE_REFS:
                value = dig(r, path)
                if value:
                    names.add(value)
                    if value == secret:
                        endpoints[f"{r.get('name')}/{sink}"] = (
                            dig(r, ("pagerDuty", "endpoint")) or "the PagerDuty default"
                            if sink == "pagerduty" else "the URL inside the Secret")
        entries = [b for b in status.get("credentialBindings") or []
                   if b.get("secretName") == secret and (not route or b.get("route") == route)]
        distinct = {b.get("binding") for b in entries}
        if len(distinct) > 1:
            raise Refused(f"the Secret is named by routes with different bindings "
                          f"({', '.join(sorted(e.get('route', '?') for e in entries))}); "
                          "name one with --route")
        binding = next(iter(distinct), "") or ""
        endpoint = "; ".join(f"{k}: {v}" for k, v in sorted(endpoints.items()))
    names.discard(None)
    return Target(kind, name, uid, binding, endpoint, secret in names, detail)


def check_secret(kube: Kubectl, secret: str, target: Target | None) -> tuple[str, str | None]:
    """The Secret's provenance and current binding. Returns `(resourceVersion,
    existing binding or None)`; raises Refused for a Secret minted for, or
    owned by, another object."""
    obj = kube.get_json("secrets", secret, missing_ok=True)
    if obj is None:
        raise Refused(f"Secret {secret} does not exist in this namespace")
    meta = obj.get("metadata") or {}
    owners = meta.get("ownerReferences") or []
    if owners:
        if target is None:
            raise Refused(f"Secret {secret} is owned by {owners[0].get('kind')}/"
                          f"{owners[0].get('name')}: it was made for that object, not for an "
                          "inline archive location")
        if not any(o.get("uid") == target.uid for o in owners):
            o = owners[0]
            raise Refused(f"Secret {secret} is owned by {o.get('kind')}/{o.get('name')} "
                          f"(uid {o.get('uid')}), not by {target.kind}/{target.name}: it was "
                          "made for another object. That is an incident, not a binding")
    labels = meta.get("labels") or {}
    for label, kind in (("logweir.dev/credential-for", "BackupDestination"),
                        ("logweir.dev/connection", "KafkaCluster")):
        minted_for = labels.get(label)
        if minted_for is None:
            continue
        if target is None or target.kind != kind or target.name != minted_for:
            raise Refused(f"Secret {secret} is labelled {label}={minted_for}: it was minted for "
                          f"{kind} {minted_for}, and is not bound to anything else")
    raw = (obj.get("data") or {}).get(BINDING_KEY)
    existing = base64.b64decode(raw).decode("utf-8", "replace").strip() if raw else None
    return meta.get("resourceVersion") or "", existing or None


def plan(kube: Kubectl, args) -> tuple[str, str, str, list[str]]:
    """Decide, read-only. Returns `(binding, endpoint, resourceVersion, lines)`
    or raises Refused. `lines` is what the dry run prints."""
    refs = inventory(kube, args.secret)
    lines: list[str] = []
    if args.location:
        binding = location_binding(args.location, args.endpoint)
        endpoint = f"{args.endpoint or 'aws'} bucket={bucket_of(args.location)}"
        object_bound = [r for r in refs if r.basis == "object"]
        if object_bound:
            raise Refused(f"Secret {args.secret} is also named by "
                          f"{', '.join(r.label() for r in object_bound)}, whose credentials are "
                          "bound to the object, never to a location. Investigate: a Secret named "
                          "by an object and by an inline archive is shared across trust "
                          "boundaries")
        elsewhere = [r for r in refs if bucket_of(r.url) not in (None, bucket_of(args.location))]
        if elsewhere:
            raise Refused(f"Secret {args.secret} is named by inline archives at other buckets "
                          f"({', '.join(f'{r.label()} {r.url}' for r in elsewhere)}). One "
                          "binding covers one location; investigate each before binding")
        lines.append(f"inline archive location: {bucket_of(args.location)} via "
                     f"{args.endpoint or 'aws'}")
        lines.extend(f"  named by {r.label()} ({r.url or 'no url'})" for r in refs)
        target = None
    else:
        target = object_target(kube, args.kind, args.name, args.secret, args.route)
        if not target.names_secret:
            raise Refused(f"{args.kind} {args.name} does not name Secret {args.secret}"
                          + (f" on route {args.route}" if args.route else ""))
        others = [r for r in refs if not (r.kind == args.kind and r.name == args.name)]
        if others:
            raise Refused(f"Secret {args.secret} is ALSO named by "
                          f"{', '.join(r.label() for r in others)}. A Secret named by two "
                          "objects is an incident, not a Secret to bind: ask the credential's "
                          "owner which object is theirs, check the other's endpoint and "
                          "creator, delete the one they do not recognise, and treat the "
                          "credential as exposed if it ever ran. Each legitimate object gets "
                          "its own credential, entered by its owner")
        if not target.binding.startswith("v1:"):
            raise Refused(f"{args.kind} {args.name} has published no binding yet "
                          f"(status is `{target.binding or 'absent'}`): upgrade the controller "
                          "and let it reconcile first")
        binding = target.binding
        endpoint = target.endpoint
        lines.append(f"{args.kind}/{args.name} (uid {target.uid})")
        lines.extend(f"  {d}" for d in target.detail)
    resource_version, existing = check_secret(kube, args.secret, target)
    if existing is not None:
        tokens = existing.replace(",", " ").split()
        if binding in tokens:
            raise AlreadyBound(binding)
        raise Refused(f"Secret {args.secret} already carries a `{BINDING_KEY}` that is not this "
                      "one. This tool never re-binds or appends: a Secret bound to something "
                      "else is either another object's credential or a deliberate shared one — "
                      "see docs/kubernetes.md §20.10 before adding a second binding by hand")
    lines.append(f"Secret {args.secret} -> {BINDING_KEY}={binding}")
    lines.append(f"ENDPOINT (confirm with the credential's owner): {endpoint}")
    return binding, endpoint, resource_version, lines


class AlreadyBound(Exception):
    """The Secret already carries this binding; nothing to do."""


def main(argv: list[str] | None = None, runner=None, out=sys.stdout, err=sys.stderr) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--kind", choices=sorted(OBJECT_KINDS))
    parser.add_argument("--name")
    parser.add_argument("--route")
    parser.add_argument("--location")
    parser.add_argument("--endpoint")
    parser.add_argument("--secret", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-endpoint")
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        return EXIT_USAGE
    if bool(args.location) == bool(args.kind) or (args.kind and not args.name) or (
            args.location and args.endpoint is None):
        print("bind-credential: name exactly one of --kind/--name or --location/--endpoint",
              file=err)
        return EXIT_USAGE
    kube = Kubectl(args.context, args.namespace, runner)
    try:
        binding, endpoint, resource_version, lines = plan(kube, args)
    except AlreadyBound as bound:
        print(f"bind-credential: Secret {args.secret} already carries {bound}; nothing to do",
              file=out)
        return EXIT_OK
    except Refused as refusal:
        print(f"bind-credential: REFUSED, nothing written: {refusal}", file=err)
        return EXIT_REFUSED
    except KubectlFailed as failure:
        print(f"bind-credential: {failure}", file=err)
        return EXIT_FAILED
    for line in lines:
        print(line, file=out)
    if not args.apply:
        print("DRY RUN: nothing was written. Re-run with --apply --confirm-endpoint "
              f"'{endpoint}' once the credential's owner has confirmed that endpoint.", file=out)
        return EXIT_OK
    if args.confirm_endpoint != endpoint:
        print("bind-credential: REFUSED, nothing written: --confirm-endpoint must be exactly "
              f"the endpoint printed above ('{endpoint}')", file=err)
        return EXIT_REFUSED
    try:
        kube.patch_secret(args.secret, resource_version, binding)
    except KubectlFailed as failure:
        print(f"bind-credential: {failure}", file=err)
        return EXIT_FAILED
    print(f"bound: Secret {args.secret} now carries {BINDING_KEY}={binding}", file=out)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
