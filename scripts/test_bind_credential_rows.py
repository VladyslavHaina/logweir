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
FIXTURE = json.loads((ROOT / "e2e/fixtures/credential-binding/bindings.json").read_text())
PUBLISHED = object()  # the status binding the controller computes from this spec


def destination_spec(secret, endpoint="https://minio.storage.svc:9000", region="us-east-1"):
    return {
        "storage": {"provider": "S3", "bucket": "kafka-backups", "endpoint": endpoint,
                    "region": region, "addressing": "PathStyle"},
        "transport": {"security": "TLS"},
        "access": {"archiveWrite": {"secret": {"name": secret}}},
    }


def destination(name, uid, secret, endpoint="https://minio.storage.svc:9000", binding=PUBLISHED):
    spec = destination_spec(secret, endpoint)
    if binding is PUBLISHED:
        binding = bc.destination_binding(uid, spec)
    status = {"credentialBinding": binding} if binding else {}
    return {"metadata": {"name": name, "uid": uid}, "spec": spec, "status": status}


BINDING = bc.destination_binding(UID, destination_spec("lwd-primary-archive-write"))


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
    """Serves `{plural: [items]}`. Every Logweir kind must be asked for by its
    FULLY QUALIFIED name (`backups.logweir.dev`): a bare plural may resolve to
    another API group's kind (review S2), so the fake refuses one."""

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
        resource = argv[7]
        if resource != "secrets":
            assert resource.endswith(".logweir.dev"), f"a bare plural reached kubectl: {argv}"
        plural = resource.removesuffix(".logweir.dev")
        if argv[8] == "-o":
            return 0, json.dumps({"items": self.objects.get(plural, [])}), ""
        name = argv[8]
        for item in self.objects.get(plural, []):
            if item["metadata"]["name"] == name:
                return 0, json.dumps(item), ""
        return 1, "", f'Error from server (NotFound): {plural} "{name}" not found'


def retention_policy(secret, uid="r-uid", destination_name="primary", scope="poc/old",
                     published=None):
    status = {"credentialBinding": published} if published else {}
    return {
        "metadata": {"name": "r", "uid": uid},
        "spec": {"destinationRef": {"name": destination_name}, "scope": {"prefix": scope},
                 "enforcement": {"credentialSecretRef": {"name": secret}}},
        "status": status,
    }


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
ENDPOINT = "https://minio.storage.svc:9000 bucket=kafka-backups region=us-east-1"


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
    objects["retentionpolicies"] = [retention_policy("lwd-primary-archive-write")]
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


def row_every_form_agrees_with_the_products_fixture():
    """The tool's ports and the Rust crate read one fixture
    (`crates/logweir-core/tests/binding_fixture.rs`)."""
    for case in FIXTURE["location"]:
        assert bc.location_binding(case["url"], case["endpoint"], case["region"],
                                   case["pathStyle"], case["allowHttp"]) == case["binding"], case
    for case in FIXTURE["destination"]:
        assert bc.destination_binding(case["uid"], case["spec"]) == case["binding"], case
    for case in FIXTURE["retention"]:
        assert bc.retention_binding(case["uid"], case["destinationSpec"],
                                    case["scope"]) == case["binding"], case
    for case in FIXTURE["notification"]:
        assert bc.notification_binding(case["uid"], case["sink"],
                                       case["endpoint"]) == case["binding"], case
    for case in FIXTURE["kafka"]:
        assert bc.kafka_binding(case["uid"], case["spec"]) == case["binding"], case


LOCATION = ["--location", "s3://kafka-backups/p0", "--endpoint", "https://minio.storage.svc:9000",
            "--region", "us-east-1", "--path-style", "true", "--allow-http", "false",
            "--secret", "logweir-s3"]


def row_an_inline_archive_secret_binds_to_its_location_only():
    backups = [{"metadata": {"name": f"b{i}"},
                "spec": {"archive": {"url": f"s3://kafka-backups/p{i}",
                                     "secretRef": {"name": "logweir-s3"}}}} for i in range(2)]
    objects = {"backups": backups, "secrets": [secret("logweir-s3")]}
    args = LOCATION
    fake = Fake(copy.deepcopy(objects))
    code, out, err = run(fake, *args)
    assert code == 0, err
    assert FIXTURE["location"][1]["binding"] in out, out
    assert ("ENDPOINT (confirm with the credential's owner): https://minio.storage.svc:9000 "
            "bucket=kafka-backups region=us-east-1 pathStyle=true allowHttp=false") in out, out
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


EU = bc.notification_binding("p-uid", "pagerduty", "https://events.eu.pagerduty.com/v2/enqueue")
DEFAULT = bc.notification_binding("p-uid", "pagerduty", None)


def row_a_protection_policy_route_is_bound_to_its_own_entry():
    policy = {
        "metadata": {"name": "p", "uid": "p-uid"},
        "spec": {"notifications": {"routes": [
            {"name": "oncall", "pagerDuty": {"routingKeySecretRef": {"name": "pd"},
                                             "endpoint": "https://events.eu.pagerduty.com/v2/enqueue"}},
            {"name": "backup", "pagerDuty": {"routingKeySecretRef": {"name": "pd"}}},
        ]}},
        "status": {"credentialBindings": [
            {"route": "oncall", "sink": "pagerduty", "secretName": "pd", "binding": EU},
            {"route": "backup", "sink": "pagerduty", "secretName": "pd", "binding": DEFAULT},
        ]},
    }
    objects = {"protectionpolicies": [policy], "secrets": [secret("pd")]}
    fake = Fake(copy.deepcopy(objects))
    code, _, err = run(fake, "--kind", "ProtectionPolicy", "--name", "p", "--secret", "pd")
    assert code == 3 and "--route" in err, err
    fake = Fake(copy.deepcopy(objects))
    code, out, err = run(fake, "--kind", "ProtectionPolicy", "--name", "p", "--route", "oncall",
                         "--secret", "pd")
    assert code == 0 and EU in out, (out, err)
    assert "events.eu.pagerduty.com" in out, out
    # F2: the route's endpoint was edited a moment ago and the status still
    # names the old one — the tool binds neither.
    edited = copy.deepcopy(objects)
    edited["protectionpolicies"][0]["spec"]["notifications"]["routes"][0]["pagerDuty"][
        "endpoint"] = "https://attacker.example/v2/enqueue"
    fake = Fake(edited)
    code, _, err = run(fake, "--kind", "ProtectionPolicy", "--name", "p", "--route", "oncall",
                       "--secret", "pd", "--apply", "--confirm-endpoint",
                       "oncall/pagerduty: https://attacker.example/v2/enqueue")
    assert code == 3 and "lags an edit" in err, err
    assert fake.patches == []


def row_a_kubectl_failure_is_exit_one_and_usage_is_exit_two():
    fake = Fake(clean())
    fake.fail_on = "backupdestinations.logweir.dev"
    code, _, err = run(fake, *DEST)
    assert code == 1 and "Forbidden" in err, err
    assert bc.main(["--context", "ctx", "--namespace", "ns", "--secret", "s"],
                   runner=Fake({}), out=io.StringIO(), err=io.StringIO()) == 2
    # No --context, no run: the tool never falls back to a current context.
    assert bc.main(["--namespace", "ns", "--kind", "KafkaCluster", "--name", "c", "--secret", "s"],
                   runner=Fake({}), out=io.StringIO(), err=io.StringIO()) == 2


def row_the_binding_is_the_specs_and_a_status_that_disagrees_is_refused():
    """Review F2: the tool binds what it COMPUTES from the spec it prints, and
    refuses when the published status says anything else."""
    # A destination whose status was computed for another route.
    objects = clean()
    stale = bc.destination_binding(UID, destination_spec("x", endpoint="https://evil:9000"))
    objects["backupdestinations"] = [destination("primary", UID, "lwd-primary-archive-write",
                                                 binding=stale)]
    fake = Fake(objects)
    code, _, err = run(fake, *DEST, "--apply", "--confirm-endpoint", ENDPOINT)
    assert code == 3 and "lags an edit" in err, (code, err)
    assert fake.patches == []
    # CONTROL: the same object with the status its spec gives binds.
    fake = Fake(clean())
    code, _, err = run(fake, *DEST, "--apply", "--confirm-endpoint", ENDPOINT)
    assert code == 0 and len(fake.patches) == 1, (code, err)


def row_a_retention_key_is_never_bound_to_a_route_its_status_lags():
    """Review F2's race: the policy's destination was re-created at an evil
    route, evaluated once, and re-created at the legitimate route. The status
    still holds B(policy, evil route); the operator sees and confirms the
    legitimate one. Nothing is written."""
    secret_name = "retention-delete-key"
    legit = destination("primary", UID, "lwd-primary-archive-write")
    evil_spec = destination_spec("lwd-primary-archive-write", endpoint="https://evil.example:9000")
    evil_binding = bc.retention_binding("r-uid", evil_spec, "poc/old")
    good_binding = bc.retention_binding("r-uid", legit["spec"], "poc/old")
    confirm = "https://minio.storage.svc:9000 bucket=kafka-backups region=us-east-1 scope=poc/old"
    argv = ["--kind", "RetentionPolicy", "--name", "r", "--secret", secret_name, "--apply",
            "--confirm-endpoint", confirm]
    objects = {"backupdestinations": [legit],
               "retentionpolicies": [retention_policy(secret_name, published=evil_binding)],
               "secrets": [secret(secret_name)]}
    fake = Fake(copy.deepcopy(objects))
    code, _, err = run(fake, *argv)
    assert code == 3 and "lags an edit" in err, (code, err)
    assert fake.patches == []
    # CONTROL: once the controller has re-evaluated, the same command binds
    # the key to the route the operator confirmed.
    objects["retentionpolicies"] = [retention_policy(secret_name, published=good_binding)]
    fake = Fake(copy.deepcopy(objects))
    code, out, err = run(fake, *argv)
    assert code == 0, err
    assert fake.patches[0]["stringData"]["logweir-binding"] == good_binding
    assert "destination primary (uid " + UID + ")" in out, out


def row_a_kafka_cluster_binds_what_its_spec_gives():
    case = FIXTURE["kafka"][0]
    cluster = {"metadata": {"name": "c", "uid": case["uid"]}, "spec": case["spec"],
               "status": {"credentialBinding": case["binding"]}}
    objects = {"kafkaclusters": [cluster], "secrets": [secret("kafka-pw")]}
    fake = Fake(copy.deepcopy(objects))
    code, out, err = run(fake, "--kind", "KafkaCluster", "--name", "c", "--secret", "kafka-pw")
    assert code == 0 and case["binding"] in out, (out, err)
    objects["kafkaclusters"][0]["status"]["credentialBinding"] = FIXTURE["kafka"][1]["binding"]
    fake = Fake(objects)
    code, _, err = run(fake, "--kind", "KafkaCluster", "--name", "c", "--secret", "kafka-pw")
    assert code == 3 and "lags an edit" in err, err


def row_an_s3_location_states_its_region_and_never_an_injected_one():
    """Review F1, at the tool: an s3:// location is bound over its region,
    addressing style and allowHttp, so they are stated, and a region that is
    not a region name is refused."""
    objects = {"secrets": [secret("logweir-s3")]}
    no_region = [a for a in LOCATION if a not in ("--region", "us-east-1")]
    assert bc.main(["--context", "ctx", "--namespace", "ns", *no_region], runner=Fake(objects),
                   out=io.StringIO(), err=io.StringIO()) == 2
    injected = [("x@127.0.0.1:9/" if a == "us-east-1" else a) for a in LOCATION]
    fake = Fake(copy.deepcopy(objects))
    code, _, err = run(fake, *injected)
    assert code == 3 and "not an S3 region name" in err, (code, err)
    assert "127.0.0.1" not in err, err
    # CONTROL, and the F1 shape: the victim's bucket on the AWS default with
    # two regions are two locations.
    aws = ["--location", "s3://victim-backups/team-a", "--endpoint", "aws", "--path-style",
           "false", "--allow-http", "false", "--secret", "logweir-s3"]
    outs = []
    for region in ("us-east-1", "eu-west-1"):
        code, out, err = run(Fake(copy.deepcopy(objects)), *aws, "--region", region)
        assert code == 0, err
        outs.append([line for line in out.splitlines() if "logweir-binding=" in line][0])
    assert outs[0] != outs[1], outs
    assert FIXTURE["location"][2]["binding"] in outs[0], outs


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
