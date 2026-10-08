#!/usr/bin/env python3
"""Offline rows for `scripts/bind-credential.py` (FX-20's upgrade tool).

No cluster: the tool's one seam to Kubernetes is a `runner(argv)` function, and
these rows hand it a fake that serves a namespace from a dict and records every
call. The point of the tool is what it REFUSES — binding a Secret another
object also names (the PROD-01.3 F3 theft), a Secret minted for or owned by
another object, a Secret already bound elsewhere — so every refusal row also
asserts that NOTHING was patched, and the control rows show the same tool does
write when the facts are clean.

    python3 scripts/test_bind_credential_rows.py
"""

from __future__ import annotations

import base64
import copy
import importlib.util
import io
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("bind_credential", ROOT / "scripts" / "bind-credential.py")
bc = importlib.util.module_from_spec(_spec)
sys.modules["bind_credential"] = bc
assert _spec.loader is not None
_spec.loader.exec_module(bc)

UID = "11111111-0000-4000-8000-000000000001"
THIEF_UID = "22222222-0000-4000-8000-000000000002"
BINDING = "v1:" + UID + ":sha256:" + "ab" * 32


def destination(name, uid, secret, endpoint="https://minio.storage.svc:9000", binding=BINDING):
    status = {"credentialBinding": binding} if binding else {}
    return {
        "metadata": {"name": name, "uid": uid},
        "spec": {
            "storage": {"bucket": "kafka-backups", "endpoint": endpoint},
            "access": {"archiveWrite": {"secret": {"name": secret}}},
        },
        "status": status,
    }


def secret(name, owner_uid=None, labels=None, binding=None, rv="42"):
    meta = {"name": name, "resourceVersion": rv}
    if owner_uid:
        meta["ownerReferences"] = [{"kind": "BackupDestination", "name": "x", "uid": owner_uid}]
    if labels:
        meta["labels"] = labels
    data = {"secret-access-key": base64.b64encode(b"never-printed").decode()}
    if binding is not None:
        data["logweir-binding"] = base64.b64encode(binding.encode()).decode()
    return {"metadata": meta, "data": data}


class Fake:
    def __init__(self, objects):
        self.objects = objects  # {plural: [items]}
        self.calls = []
        self.patches = []
        self.fail_on = None

    def __call__(self, argv):
        self.calls.append(argv)
        assert argv[:6] == ["kubectl", "--context", "ctx", "--namespace", "ns",
                            "--request-timeout=30s"], argv
        verb = argv[6]
        if self.fail_on and self.fail_on in argv:
            return 1, "", "Error from server (Forbidden): nope"
        if verb == "patch":
            self.patches.append(json.loads(argv[-1]) | {"_name": argv[8]})
            return 0, "{}", ""
        plural = argv[7]
        if argv[8] == "-o":
            return 0, json.dumps({"items": self.objects.get(plural, [])}), ""
        name = argv[8]
        for item in self.objects.get(plural, []):
            if item["metadata"]["name"] == name:
                return 0, json.dumps(item), ""
        return 1, "", f'Error from server (NotFound): {plural} "{name}" not found'


def run(fake, *argv):
    out, err = io.StringIO(), io.StringIO()
    code = bc.main(["--context", "ctx", "--namespace", "ns", *argv], runner=fake, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def clean():
    return {
        "backupdestinations": [destination("primary", UID, "lwd-primary-archive-write")],
        "secrets": [secret("lwd-primary-archive-write", owner_uid=UID,
                           labels={"logweir.dev/credential-for": "primary"})],
    }


DEST = ["--kind", "BackupDestination", "--name", "primary", "--secret", "lwd-primary-archive-write"]
ENDPOINT = "https://minio.storage.svc:9000 bucket=kafka-backups"


def row_dry_run_names_both_and_writes_nothing():
    fake = Fake(clean())
    code, out, err = run(fake, *DEST)
    assert code == 0, err
    assert BINDING in out and ENDPOINT in out and "DRY RUN" in out, out
    assert fake.patches == [], "a dry run patched"
    assert "never-printed" not in out + err


def row_apply_needs_the_confirmed_endpoint_then_patches_once():
    fake = Fake(clean())
    code, _, err = run(fake, *DEST, "--apply")
    assert code == 3 and "--confirm-endpoint" in err, (code, err)
    code, _, err = run(fake, *DEST, "--apply", "--confirm-endpoint", "https://attacker:9000")
    assert code == 3, (code, err)
    assert fake.patches == [], "an unconfirmed endpoint was written"
    code, out, err = run(fake, *DEST, "--apply", "--confirm-endpoint", ENDPOINT)
    assert code == 0, err
    assert fake.patches == [{"metadata": {"resourceVersion": "42"},
                             "stringData": {"logweir-binding": BINDING},
                             "_name": "lwd-primary-archive-write"}], fake.patches


def row_a_secret_another_object_names_is_an_incident_not_a_binding():
    """THE F3 THEFT: a thief destination names the victim's Secret beside an
    endpoint it controls. Binding the Secret to either object is refused."""
    objects = clean()
    objects["backupdestinations"].append(
        destination("thief", THIEF_UID, "lwd-primary-archive-write",
                    endpoint="https://attacker.example:9000",
                    binding="v1:" + THIEF_UID + ":sha256:" + "cd" * 32))
    for name in ("primary", "thief"):
        fake = Fake(copy.deepcopy(objects))
        code, _, err = run(fake, "--kind", "BackupDestination", "--name", name,
                           "--secret", "lwd-primary-archive-write", "--apply",
                           "--confirm-endpoint", ENDPOINT)
        assert code == 3, (name, code, err)
        assert "ALSO named by" in err and "incident" in err, err
        assert fake.patches == [], f"{name}: the shared Secret was bound"
    # …and across kinds: a KafkaCluster or a retention policy naming it.
    objects = clean()
    objects["retentionpolicies"] = [{
        "metadata": {"name": "r", "uid": "r-uid"},
        "spec": {"enforcement": {"credentialSecretRef": {"name": "lwd-primary-archive-write"}}},
    }]
    fake = Fake(objects)
    code, _, err = run(fake, *DEST)
    assert code == 3 and "RetentionPolicy/r" in err, err


def row_a_secret_owned_or_labelled_for_another_object_is_refused():
    objects = clean()
    objects["secrets"] = [secret("lwd-primary-archive-write", owner_uid=THIEF_UID)]
    fake = Fake(objects)
    code, _, err = run(fake, *DEST)
    assert code == 3 and "owned by" in err, err
    objects = clean()
    objects["secrets"] = [secret("lwd-primary-archive-write",
                                 labels={"logweir.dev/credential-for": "other"})]
    fake = Fake(objects)
    code, _, err = run(fake, *DEST)
    assert code == 3 and "minted for" in err, err
    assert fake.patches == []


def row_an_existing_binding_is_never_replaced_and_its_own_is_a_no_op():
    objects = clean()
    objects["secrets"] = [secret("lwd-primary-archive-write", owner_uid=UID,
                                 binding="v1:someone-else:sha256:00")]
    fake = Fake(objects)
    code, _, err = run(fake, *DEST, "--apply", "--confirm-endpoint", ENDPOINT)
    assert code == 3 and "never re-binds" in err, err
    assert fake.patches == []
    objects["secrets"] = [secret("lwd-primary-archive-write", owner_uid=UID,
                                 binding="v1:other\n" + BINDING)]
    fake = Fake(objects)
    code, out, _ = run(fake, *DEST, "--apply", "--confirm-endpoint", ENDPOINT)
    assert code == 0 and "already carries" in out, out
    assert fake.patches == []


def row_the_object_must_name_the_secret_and_have_published_its_binding():
    objects = clean()
    objects["secrets"].append(secret("unrelated"))
    fake = Fake(objects)
    code, _, err = run(fake, "--kind", "BackupDestination", "--name", "primary",
                       "--secret", "unrelated")
    assert code == 3 and "does not name Secret unrelated" in err, err
    objects = clean()
    objects["backupdestinations"] = [destination("primary", UID, "lwd-primary-archive-write",
                                                 binding=None)]
    fake = Fake(objects)
    code, _, err = run(fake, *DEST)
    assert code == 3 and "published no binding" in err, err


def row_an_inline_archive_secret_binds_to_its_location_only():
    fixture = json.loads((ROOT / "e2e/fixtures/credential-binding/location.json").read_text())
    for case in fixture["cases"]:
        assert bc.location_binding(case["url"], case["endpoint"]) == case["binding"], case
    backups = [{"metadata": {"name": f"b{i}"},
                "spec": {"archive": {"url": f"s3://kafka-backups/p{i}",
                                     "secretRef": {"name": "logweir-s3"}}}} for i in range(2)]
    objects = {"backups": backups, "secrets": [secret("logweir-s3")]}
    args = ["--location", "s3://kafka-backups/p0", "--endpoint", "https://minio.storage.svc:9000",
            "--secret", "logweir-s3"]
    fake = Fake(copy.deepcopy(objects))
    code, out, err = run(fake, *args)
    assert code == 0, err
    assert fixture["cases"][1]["binding"] in out, out
    assert "Backup/b0" in out and "Backup/b1" in out, out
    # A destination naming the same Secret: object-bound, refused.
    shared = copy.deepcopy(objects)
    shared["backupdestinations"] = [destination("primary", UID, "logweir-s3")]
    fake = Fake(shared)
    code, _, err = run(fake, *args)
    assert code == 3 and "BackupDestination/primary" in err, err
    # An inline archive naming it at ANOTHER bucket: refused.
    other = copy.deepcopy(objects)
    other["restores"] = [{"metadata": {"name": "r"},
                          "spec": {"sourceArchive": {"url": "s3://attacker-bucket/x",
                                                     "secretRef": {"name": "logweir-s3"}}}}]
    fake = Fake(other)
    code, _, err = run(fake, *args)
    assert code == 3 and "other buckets" in err, err
    # An owned Secret was made for an object: refused.
    owned = copy.deepcopy(objects)
    owned["secrets"] = [secret("logweir-s3", owner_uid=UID)]
    fake = Fake(owned)
    code, _, err = run(fake, *args)
    assert code == 3 and "owned by" in err, err
    assert fake.patches == []


def row_a_protection_policy_route_is_bound_to_its_own_entry():
    policy = {
        "metadata": {"name": "p", "uid": "p-uid"},
        "spec": {"notifications": {"routes": [
            {"name": "oncall", "pagerDuty": {"routingKeySecretRef": {"name": "pd"},
                                             "endpoint": "https://events.eu.pagerduty.com/v2/enqueue"}},
            {"name": "backup", "pagerDuty": {"routingKeySecretRef": {"name": "pd"}}},
        ]}},
        "status": {"credentialBindings": [
            {"route": "oncall", "sink": "pagerduty", "secretName": "pd", "binding": "v1:p-uid:sha256:01"},
            {"route": "backup", "sink": "pagerduty", "secretName": "pd", "binding": "v1:p-uid:sha256:02"},
        ]},
    }
    objects = {"protectionpolicies": [policy], "secrets": [secret("pd")]}
    fake = Fake(copy.deepcopy(objects))
    code, _, err = run(fake, "--kind", "ProtectionPolicy", "--name", "p", "--secret", "pd")
    assert code == 3 and "--route" in err, err
    fake = Fake(copy.deepcopy(objects))
    code, out, err = run(fake, "--kind", "ProtectionPolicy", "--name", "p", "--route", "oncall",
                         "--secret", "pd")
    assert code == 0 and "v1:p-uid:sha256:01" in out, (out, err)
    assert "events.eu.pagerduty.com" in out, out


def row_a_kubectl_failure_is_exit_one_and_usage_is_exit_two():
    fake = Fake(clean())
    fake.fail_on = "backupdestinations"
    code, _, err = run(fake, *DEST)
    assert code == 1 and "Forbidden" in err, err
    assert bc.main(["--context", "ctx", "--namespace", "ns", "--secret", "s"],
                   runner=Fake({}), out=io.StringIO(), err=io.StringIO()) == 2
    # No --context, no run: the tool never falls back to a current context.
    assert bc.main(["--namespace", "ns", "--kind", "KafkaCluster", "--name", "c", "--secret", "s"],
                   runner=Fake({}), out=io.StringIO(), err=io.StringIO()) == 2


ROWS = [v for k, v in sorted(globals().items()) if k.startswith("row_")]

if __name__ == "__main__":
    failed = 0
    for row in ROWS:
        try:
            row()
            print(f"ok   {row.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {row.__name__}: {e}")
    print(f"{len(ROWS) - failed}/{len(ROWS)} rows passed")
    sys.exit(1 if failed else 0)
