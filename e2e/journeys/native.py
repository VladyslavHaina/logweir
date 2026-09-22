"""The journeys no existing harness proves, implemented once, here.

The audit (`README.md`, "Audit map") found these gaps and nothing else:

1. No row drives a restore of an OLDER recovery point to completion and reads
   the restored records back; the nearest rows stop at admission (d3
   `trust-old-archive-still-restores`) or render the wizard's text (plat11-2,
   plat12-13). No row reads the selected point back off the created Restore.
2. No row backs up from an unreachable source. The only source-offline rows are
   a discovery (d2 S11), a preflight (d2 S14f), and a restore in the lab's own
   INSTALLER (`scripts/test-k8s-scram.py`), which swaps the shared broker's
   Service selector and is not re-runnable.
3. The product API's restore create has no live replay row. The UI rows prove
   a double click; nothing proves that two submissions with one
   `Idempotency-Key` leave one durable object.

Every row asserts on archive data, evidence or a durable resource, and every
row REQUIRES what it records: the restored-record row needs the source topic
to hold MORE records than the old point, so that "exactly the old point's
records" discriminates between the two points; the offline row's "nothing was
written" is checked with the same listing that finds the restore journey's
objects, so an empty listing is not "empty because the listing is blind".

Nothing here writes to the shared release. On the shared brokers it creates
exactly two topics, both named with this run's stamp, and deletes both. In the
shared MinIO it writes under `kafka-backups/lw-plat20-<stamp>/` only, and
removes that prefix and the evidence objects its own runs' statuses name.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from typing import Any

from core import ARCHIVE, EVIDENCE, FAIL, PASS, RESOURCE
from lab import FIXTURE_NS, Lab, condition

SOURCE_BOOTSTRAP = f"kafka-source.{FIXTURE_NS}.svc.cluster.local:9096"
TARGET_BOOTSTRAP = f"kafka-target.{FIXTURE_NS}.svc.cluster.local:9096"
MINIO = f"http://minio.{FIXTURE_NS}.svc.cluster.local:9000"
BUCKET = "kafka-backups"
KAFKA_BIN = "/opt/kafka/bin"
OLD_RECORDS = 20
NEW_RECORDS = 15
TERMINAL = {"Succeeded", "Failed", "Refused"}

# Row name -> what its assertion reads. `suites.py` cites these rows by the
# line their `record` call sits on; `test_catalogue.py` checks the cite.
ROWS = {
    "backup-from-an-offline-source-fails-and-writes-nothing": (RESOURCE, ARCHIVE),
    "restore-cr-carries-the-selected-older-point": (RESOURCE,),
    "api-restore-replay-is-one-object": (RESOURCE,),
    "older-point-restore-restores-exactly-its-records": (ARCHIVE, RESOURCE),
    "restore-evidence-verifies-with-two-verifiers": (EVIDENCE,),
    "restore-progress-is-durable-and-projected": (RESOURCE,),
}


class Native:
    def __init__(self, lab: Lab, stamp: str, *, api_bin: pathlib.Path, cli: pathlib.Path,
                 python: str, approver_key: pathlib.Path, private: pathlib.Path):
        self.lab = lab
        self.stamp = stamp
        self.tag = stamp.replace("t", "").replace("z", "")[-10:]
        self.ns = f"lw-plat20-n-{stamp}"
        self.api_bin = api_bin
        self.cli = cli
        self.python = python
        self.approver_key = approver_key
        self.private = private  # 0700, outside the swept artifact tree, removed at the end
        self.prefix = f"lw-plat20-{stamp}"
        self.topic = f"plat20-{self.tag}-orders"
        self.restored_prefix = f"plat20r-{self.tag}-"
        self.rows: dict[str, str] = {}
        self.details: dict[str, Any] = {}
        self.evidence_keys: list[str] = []
        self.created_topics: list[tuple[str, str]] = []  # (which broker, topic)
        self.api: subprocess.Popen | None = None
        self.api_port = 0

    # -- bookkeeping ----------------------------------------------------------

    def record(self, row: str, ok: bool, detail: str, facts: dict[str, Any]) -> bool:
        if row not in ROWS:
            raise RuntimeError(f"unknown native row {row}")
        self.rows[row] = PASS if ok else FAIL
        self.details[row] = {"verdict": self.rows[row], "detail": detail, "facts": facts,
                             "verifies": list(ROWS[row])}
        self.lab.write(f"native/rows/{row}.json", self.details[row])
        self.lab.log(f"[{self.rows[row]}] {row} — {detail}")
        return ok

    def result(self) -> dict[str, Any]:
        return {"namespace": self.ns, "rows": self.rows, "details": self.details}

    # -- fixture --------------------------------------------------------------

    def setup(self) -> None:
        lab, ns = self.lab, self.ns
        lab.create_namespace(ns)
        lab.create({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": lab.meta("logweir-runner", ns),
                    "automountServiceAccountToken": False})
        for name in ("source-scram", "target-scram", "logweir-s3", "logweir-signing-key", "minio-root"):
            lab.copy_secret(name, ns)
        scram = lambda secret: {"mode": "scramSha512", "username": "scram-user",  # noqa: E731
                                "secretRef": {"name": secret}, "tls": False}
        lab.create(self._obj("KafkaCluster", "source", {"bootstrapServers": [SOURCE_BOOTSTRAP],
                                                        "auth": scram("source-scram"), "role": "source"}))
        lab.create(self._obj("KafkaCluster", "target", {"bootstrapServers": [TARGET_BOOTSTRAP],
                                                        "auth": scram("target-scram"), "role": "target"}))
        # AN OWNED SERVICE WITH NO ENDPOINTS: the source is offline because
        # nothing answers, not because the shared broker was touched.
        lab.create({"apiVersion": "v1", "kind": "Service", "metadata": lab.meta("offline-broker", ns),
                    "spec": {"ports": [{"port": 9096, "targetPort": 9096}]}})
        lab.create(self._obj("KafkaCluster", "offline", {
            "bootstrapServers": [f"offline-broker.{ns}.svc.cluster.local:9096"],
            "auth": scram("source-scram"), "role": "source"}))
        lab.create(self._client_pod())
        lab.create(self._mc_pod())
        for pod in ("kafka-client", "mc"):
            lab.kubectl("wait", "--for=condition=Ready", f"pod/{pod}", "--timeout=180s", ns=ns, timeout=200)
        for name in ("source", "target"):
            lab.wait_for("kafkacluster", name, ns, lambda o: (o.get("status") or {}).get("reachable") is True,
                         seconds=240, what="the controller to reach the lab broker")

    def _obj(self, kind: str, name: str, spec: dict[str, Any]) -> dict[str, Any]:
        return {"apiVersion": "logweir.dev/v1alpha1", "kind": kind, "metadata": self.lab.meta(name, self.ns),
                "spec": spec}

    def _client_pod(self) -> dict[str, Any]:
        # The password reaches the pod as an env var from the copied Secret and
        # is written into a client config INSIDE the pod. It is never in argv,
        # never in this process, never in an artifact.
        props = ("security.protocol=SASL_PLAINTEXT\\nsasl.mechanism=SCRAM-SHA-512\\n"
                 "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required "
                 "username=\\\"scram-user\\\" password=\\\"$PW\\\";\\n")
        script = (f'PW="$SRC_PW"; printf "{props}" > /tmp/src.properties; '
                  f'PW="$TGT_PW"; printf "{props}" > /tmp/tgt.properties; '
                  "unset SRC_PW TGT_PW PW; touch /tmp/ready; sleep 10800")
        env = lambda var, secret: {"name": var, "valueFrom": {"secretKeyRef": {"name": secret, "key": "password"}}}  # noqa: E731
        return {"apiVersion": "v1", "kind": "Pod", "metadata": self.lab.meta("kafka-client", self.ns),
                "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                         "containers": [{"name": "kafka", "image": "apache/kafka:3.7.1",
                                         "imagePullPolicy": "IfNotPresent",
                                         "command": ["/bin/bash", "-c", script],
                                         "env": [env("SRC_PW", "source-scram"), env("TGT_PW", "target-scram")],
                                         "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/ready"]},
                                                            "periodSeconds": 1}}]}}

    def _mc_pod(self) -> dict[str, Any]:
        env = lambda var, key: {"name": var, "valueFrom": {"secretKeyRef": {"name": "logweir-s3", "key": key}}}  # noqa: E731
        script = (f'mc alias set local {MINIO} "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null '
                  "&& touch /tmp/ready && sleep 10800")
        return {"apiVersion": "v1", "kind": "Pod", "metadata": self.lab.meta("mc", self.ns),
                "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                         "containers": [{"name": "mc", "image": "minio/mc:latest", "imagePullPolicy": "Never",
                                         "command": ["/bin/sh", "-c", script],
                                         "env": [env("AWS_ACCESS_KEY_ID", "access-key-id"),
                                                 env("AWS_SECRET_ACCESS_KEY", "secret-access-key")],
                                         "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/ready"]},
                                                            "periodSeconds": 1}}]}}

    def kafka(self, broker: str, tool: str, *args: str, data: str | None = None,
              check: bool = True, timeout: int = 120) -> str:
        bootstrap = SOURCE_BOOTSTRAP if broker == "source" else TARGET_BOOTSTRAP
        config_flag = {"kafka-topics.sh": "--command-config", "kafka-get-offsets.sh": "--command-config",
                       "kafka-console-producer.sh": "--producer.config",
                       "kafka-console-consumer.sh": "--consumer.config"}[tool]
        conf = "/tmp/src.properties" if broker == "source" else "/tmp/tgt.properties"
        argv = ["exec"] + (["-i"] if data is not None else []) + [
            "kafka-client", "--", f"{KAFKA_BIN}/{tool}", "--bootstrap-server", bootstrap,
            config_flag, conf, *args]
        return self.lab.kubectl(*argv, ns=self.ns, data=data, check=check, timeout=timeout).stdout

    def objects(self, path: str) -> list[str]:
        out = self.lab.kubectl("exec", "mc", "--", "mc", "ls", "--recursive", "--json",
                               f"local/{BUCKET}/{path}", ns=self.ns, check=False).stdout
        keys = []
        for line in out.splitlines():
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("status") == "success" and entry.get("key"):
                keys.append(path.rstrip("/") + "/" + entry["key"])
        return keys

    def backup(self, name: str, source: str, path: str, *, deadline: int = 600) -> dict[str, Any]:
        self.lab.create(self._obj("Backup", name, {
            "sourceRef": {"name": source}, "topics": [self.topic],
            "archive": {"url": f"s3://{BUCKET}/{self.prefix}/{path}", "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual", "deadlineSeconds": deadline}))
        done = self.lab.wait_for("backup", name, self.ns, lambda o: (o.get("status") or {}).get("phase") in TERMINAL,
                                 seconds=deadline + 120, what="a terminal phase")
        # The runner also writes a signed catalog record into the SHARED
        # evidence root and names it only in its log (`catalog-key=`); it is
        # recorded here so cleanup removes it with the receipts.
        logs = self.lab.kubectl("logs", f"job/{name}", "--tail=400", ns=self.ns, check=False).stdout
        for line in logs.splitlines():
            if line.startswith("catalog-key=") and line.endswith("/record.json"):
                key = line.split("=", 1)[1].strip()
                self.evidence_keys += [key, key[: -len("record.json")] + "record.sig"]
        return done

    def verified(self, kind: str, name: str) -> dict[str, Any]:
        return self.lab.wait_for(
            kind, name, self.ns,
            lambda o: bool((((o.get("status") or {}).get("evidence") or {}).get("verification") or {}).get("result")),
            seconds=240, what="an evidence verdict")

    # -- the product API ------------------------------------------------------

    def start_api(self) -> None:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.api_port = s.getsockname()[1]
        key = self.private / "cursor.key"
        key.write_bytes(os.urandom(32))
        key.chmod(0o600)
        origin = f"http://127.0.0.1:{self.api_port}"
        config = "\n".join([
            "mode: localAdmin", f'listen: "127.0.0.1:{self.api_port}"', f'publicOrigin: "{origin}"',
            f"uiDirectory: {self.lab.root / 'ui'}", "localAdmin:", "  subject: admin",
            "  displayName: Local administrator", f"namespaces: [{self.ns}]", "kubernetes:",
            "  source: kubeconfig", "  context: docker-desktop", f"cursorKeyFile: {key}", ""])
        (self.private / "api.yaml").write_text(config)
        self.lab.write("native/api-config.yaml", config)
        log = (self.lab.out / "native" / "api.log").open("w")
        self.api = subprocess.Popen([str(self.api_bin), "--config", str(self.private / "api.yaml")],
                                    stdout=log, stderr=subprocess.STDOUT)
        for _ in range(60):
            try:
                with urllib.request.urlopen(origin + "/healthz", timeout=2) as r:
                    if r.status == 200:
                        return
            except OSError:
                time.sleep(0.5)
        raise RuntimeError("logweir-api never answered /healthz within 30 s")

    def stop_api(self) -> None:
        if self.api is not None and self.api.poll() is None:
            self.api.terminate()
            try:
                self.api.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.api.kill()

    def call(self, method: str, path: str, body: Any = None, key: str | None = None) -> tuple[int, Any]:
        origin = f"http://127.0.0.1:{self.api_port}"
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers.update({"Content-Type": "application/json", "Origin": origin})
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(origin + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as r:
                return r.status, json.loads(r.read() or b"null")
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, raw.decode(errors="replace")[:2000]

    # -- journeys -------------------------------------------------------------

    def source_offline(self) -> None:
        name = "offline-run"
        done = self.backup(name, "offline", "offline", deadline=180)
        status = done.get("status") or {}
        cluster = self.lab.get("kafkacluster", "offline", self.ns)
        written = self.objects(f"{self.prefix}/offline/")
        # The listing's own control: the SAME listing over the prefix the
        # restore journey's runs wrote finds objects. Without it, zero objects
        # is also what a blind listing returns.
        control = self.objects(f"{self.prefix}/points/")
        self.lab.write("native/offline-backup.json", done)
        failed = status.get("phase") in {"Failed", "Refused"}
        named = bool(status.get("reason") or condition(done, "Failed").get("reason")
                     or condition(done, "Complete").get("reason"))
        clauses = {
            "the Backup ended Failed or Refused, never Succeeded": failed,
            "and says why by name": named,
            "the controller never marked the offline source reachable":
                (cluster.get("status") or {}).get("reachable") is not True,
            "no archive object was written under its prefix": written == [],
            "no receipt is claimed": not ((status.get("evidence") or {}).get("receiptKey")),
            "the listing is not blind: it finds the restore journey's objects": len(control) > 0,
        }
        self.record("backup-from-an-offline-source-fails-and-writes-nothing", all(clauses.values()),
                    f"phase {status.get('phase')!r} reason {status.get('reason')!r}; "
                    f"{len(written)} object(s) under the offline prefix, {len(control)} under the control "
                    "prefix; " + "; ".join(f"{k}={v}" for k, v in clauses.items()),
                    {"clauses": clauses, "phase": status.get("phase"), "reason": status.get("reason"),
                     "uid": done["metadata"]["uid"], "reachable": (cluster.get("status") or {}).get("reachable")})

    def old_point_restore(self) -> None:
        lab, ns = self.lab, self.ns
        # --- two recovery points whose contents differ ---------------------
        self.kafka("source", "kafka-topics.sh", "--create", "--topic", self.topic,
                   "--partitions", "1", "--replication-factor", "1")
        self.created_topics.append(("source", self.topic))
        old_values = [f"old-{n:03}" for n in range(1, OLD_RECORDS + 1)]
        new_values = [f"new-{n:03}" for n in range(1, NEW_RECORDS + 1)]
        self.kafka("source", "kafka-console-producer.sh", "--topic", self.topic, data="\n".join(old_values) + "\n")
        old = self.verified("backup", self.backup("pt-old", "source", "points")["metadata"]["name"])
        self.kafka("source", "kafka-console-producer.sh", "--topic", self.topic, data="\n".join(new_values) + "\n")
        new = self.verified("backup", self.backup("pt-new", "source", "points")["metadata"]["name"])
        for b in (old, new):
            lab.write(f"native/backup-{b['metadata']['name']}.json", b)
            ev = (b.get("status") or {}).get("evidence") or {}
            self.evidence_keys += [ev[k] for k in ("receiptKey", "sidecarKey") if ev.get(k)]
        old_id, new_id = old["status"].get("backupId"), new["status"].get("backupId")
        source_end = self._end_offset("source", self.topic)
        if not (old["status"]["phase"] == new["status"]["phase"] == "Succeeded" and old_id and new_id
                and old_id != new_id):
            raise RuntimeError(f"the two points did not both succeed with distinct ids: {old_id} {new_id}")

        # --- the API lists both, and the OLDER one is chosen ---------------
        self.start_api()
        status, listed = self.call("GET", f"/api/v1/namespaces/{ns}/backups")
        lab.write("native/api-backups.json", listed)
        listing = json.dumps(listed)
        # --- the plan, signed by the lab approver with the shipped signer ---
        storage = {"backend": "s3", "bucket": BUCKET, "prefix": f"{self.prefix}/points", "region": "us-east-1",
                   "endpoint": MINIO, "path_style": True, "allow_http": True}
        pit = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 5))
        plan = {
            "source": {"storage": storage, "backup": old_id, "topics": [self.topic]},
            "target": {"bootstrap_servers": [TARGET_BOOTSTRAP],
                       "auth": {"mode": "scramSha512", "username": "scram-user", "tls": False},
                       "mode": "newTopic", "topic_naming": {"prefix": self.restored_prefix},
                       "topic_mapping_prefix": "logweir-scratch-", "marker_topic": "logweir.scratch",
                       "default_replication_factor": 1, "teardown": "delete"},
            "restore": {"point_in_time": pit},
            # REQUIRED by the runner's plan schema ("drill spec does not parse:
            # missing field `sample`", measured live 2026-09-22). The API and
            # the controller forward plan bytes unparsed, so a plan without it
            # is admitted and fails in the runner's first phase.
            "sample": {"window_start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400)),
                       "window_end": pit, "records_per_partition": 25, "anchor": "head"},
            "objectives": {"rto_seconds": 3600, "rpo_seconds": 86400, "pass_rate": 1.0},
            "evidence": {**storage, "prefix": "logweir/"},
            "notifications": {"webhooks": []},
        }
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        plan_hash = "sha256:" + hashlib.sha256(plan_bytes.encode()).hexdigest()
        approval = f"approval-{plan_hash[7:15]}"
        work = pathlib.Path(tempfile.mkdtemp(prefix="sign-", dir=self.private))
        try:
            (work / "plan.json").write_text(plan_bytes)
            lab.run([str(self.cli), "drill", "approve", "--spec", str(work / "plan.json"),
                     "--key", str(self.approver_key), "--approver", "plat20-1", "--ticket", "PLAT-20.1",
                     "--subject-kind", "Restore", "--out", str(work / "approval.json")], timeout=120)
            approval_bytes = (work / "approval.json").read_text()
            sidecar_bytes = (work / "approval.sig").read_text()
        finally:
            shutil.rmtree(work, ignore_errors=True)
        body = {"planBytes": plan_bytes, "planHash": plan_hash, "approvalRef": {"name": approval},
                "sourceArchive": {"url": f"s3://{BUCKET}/{self.prefix}/points",
                                  "credentialRef": {"name": "logweir-s3"}},
                "backupSetRef": old_id, "pointInTime": pit,
                "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                           "topicNaming": {"prefix": self.restored_prefix}},
                "deadlineSeconds": 900,
                "topicMapping": [{"source": self.topic, "target": self.restored_prefix + self.topic}]}
        key = f"plat20-{self.stamp}-restore"
        first_status, first = self.call("POST", f"/api/v1/namespaces/{ns}/restores", body, key)
        second_status, second = self.call("POST", f"/api/v1/namespaces/{ns}/restores", body, key)
        lab.write("native/api-restore-create.json", {"first": [first_status, first], "second": [second_status, second]})
        if first_status not in (200, 201) or not isinstance(first, dict):
            raise RuntimeError(f"the API refused the restore create: {first_status} {str(first)[:600]}")
        item = first.get("item") or {}
        name, uid = item.get("name"), item.get("uid")
        restores = lab.items("restore", ns)
        same = [r for r in restores if r["metadata"]["name"] == name]
        created = same[0] if same else {}
        self.record(
            "api-restore-replay-is-one-object",
            second_status in (200, 201) and isinstance(second, dict)
            and (second.get("item") or {}).get("uid") == uid and bool(second.get("replayed"))
            and len(restores) == 1 and created.get("metadata", {}).get("uid") == uid,
            f"two POSTs with one Idempotency-Key answered {first_status} then {second_status} "
            f"(replayed={second.get('replayed') if isinstance(second, dict) else None}); the namespace holds "
            f"{len(restores)} Restore(s), uid {uid}",
            {"first": first_status, "second": second_status, "uid": uid, "restores": len(restores)})
        spec = created.get("spec") or {}
        self.record(
            "restore-cr-carries-the-selected-older-point",
            spec.get("backupSetRef") == old_id and old_id != new_id and old_id in listing and new_id in listing
            and spec.get("planBytes") == plan_bytes and status == 200,
            f"the API listed both points ({old_id[:12]}…, {new_id[:12]}…) and the durable Restore it created "
            f"names backupSetRef {str(spec.get('backupSetRef'))[:12]}… — the OLDER one — with the signed plan "
            "byte for byte",
            {"old": old_id, "new": new_id, "restoreBackupSetRef": spec.get("backupSetRef"), "restore": name,
             "uid": uid})
        # Recorded BEFORE the restore runs, so a partial restore's topic is
        # removed too.
        self.created_topics.append(("target", self.restored_prefix + self.topic))
        # --- the approval, through the ceremony the product ships ----------
        lab.create(self._obj("Approval", approval, {
            "approvalBytes": approval_bytes, "sidecarBytes": sidecar_bytes, "planHash": plan_hash,
            "subjectRef": {"kind": "Restore", "name": name}}))
        # --- progress, sampled from the durable object while it runs -------
        seen: list[dict[str, Any]] = []

        def sample(o: dict[str, Any]) -> None:
            st = o.get("status") or {}
            point = {"phase": st.get("phase"), "stage": (st.get("progress") or {}).get("stage"),
                     "runnerPhase": (st.get("progress") or {}).get("runnerPhase"),
                     "lastPhaseCompleted": st.get("lastPhaseCompleted")}
            if not seen or seen[-1] != point:
                seen.append(point)

        done = lab.wait_for("restore", name, ns, lambda o: (o.get("status") or {}).get("phase") in TERMINAL,
                            seconds=1100, what="a terminal phase", every=sample)
        if (done.get("status") or {}).get("phase") == "Succeeded":
            # Only a run that finished has a verdict to wait for. A failed one
            # falls through, so every row below is RECORDED as a FAIL with its
            # clauses, instead of the journey dying on a wait for evidence
            # that can never come.
            done = self.verified("restore", name)
        lab.write("native/restore.json", done)
        lab.write("native/restore-progress.json", seen)
        st = done.get("status") or {}
        ev = st.get("evidence") or {}
        self.evidence_keys += [ev[k] for k in ("scorecardKey", "sidecarKey") if ev.get(k)]
        # --- the restored records -----------------------------------------
        restored_topic = self.restored_prefix + self.topic
        restored_end = self._end_offset("target", restored_topic) if st.get("phase") == "Succeeded" else None
        values = []
        if restored_end:
            out = self.kafka("target", "kafka-console-consumer.sh", "--topic", restored_topic, "--from-beginning",
                             "--max-messages", str(OLD_RECORDS + NEW_RECORDS), "--timeout-ms", "20000",
                             check=False, timeout=90)
            values = [v for v in out.splitlines() if v.startswith(("old-", "new-"))]
        lab.write("native/restored-values.json", {"topic": restored_topic, "endOffset": restored_end,
                                                    "values": values})
        clauses = {
            "the restore Succeeded with outcome pass": st.get("phase") == "Succeeded" and st.get("outcome") == "pass",
            "the source now holds BOTH points' records (the discrimination)": source_end == OLD_RECORDS + NEW_RECORDS,
            f"the restored topic ends at offset {OLD_RECORDS}": restored_end == OLD_RECORDS,
            "and holds exactly the older point's records, in order": values == old_values,
            "and none of the newer point's": not any(v.startswith("new-") for v in values),
        }
        self.record("older-point-restore-restores-exactly-its-records", all(clauses.values()),
                    f"source end offset {source_end}, restored end offset {restored_end}, "
                    f"{len(values)} value(s) read back; " + "; ".join(f"{k}={v}" for k, v in clauses.items()),
                    {"clauses": clauses, "sourceEnd": source_end, "restoredEnd": restored_end,
                     "restoredTopic": restored_topic})
        # --- the evidence, verified twice ----------------------------------
        self._verify_scorecard(ev, old_id, new_id, st)
        # --- durable progress, and the API's projection of it --------------
        op_status, op = self.call("GET", f"/api/v1/namespaces/{ns}/operations/restore/{name}")
        lab.write("native/api-operation.json", op)
        op_item = (op or {}).get("item") or {} if isinstance(op, dict) else {}
        stages = [p for p in seen if p.get("stage")]
        clauses = {
            "the controller published progress on the object while it ran": len(stages) >= 1,
            "more than one distinct progress state was observed": len(seen) >= 2,
            "the terminal object records the last phase completed": st.get("lastPhaseCompleted") is not None,
            "the API reads the same object back (uid)": op_item.get("uid") == uid,
            "and projects it terminal and verified": op_item.get("terminal") is True
                                                     and op_item.get("verifiedSuccess") is True,
        }
        self.record("restore-progress-is-durable-and-projected", op_status == 200 and all(clauses.values()),
                    f"{len(seen)} distinct progress state(s) sampled from the CR "
                    f"({[p.get('stage') for p in seen]}); lastPhaseCompleted {st.get('lastPhaseCompleted')!r}; "
                    f"API operation state {op_item.get('state')!r} terminal {op_item.get('terminal')} "
                    f"verifiedSuccess {op_item.get('verifiedSuccess')}; "
                    + "; ".join(f"{k}={v}" for k, v in clauses.items()),
                    {"clauses": clauses, "progress": seen, "operationState": op_item.get("state")})

    def _end_offset(self, broker: str, topic: str) -> int | None:
        out = self.kafka(broker, "kafka-get-offsets.sh", "--topic", topic, "--time", "-1", check=False)
        total = None
        for line in out.splitlines():
            parts = line.strip().split(":")
            if len(parts) == 3 and parts[0] == topic and parts[2].isdigit():
                total = (total or 0) + int(parts[2])
        return total

    def _verify_scorecard(self, ev: dict[str, Any], old_id: str, new_id: str, st: dict[str, Any]) -> None:
        roster = self.lab.get("trustroster", "default")
        work = pathlib.Path(tempfile.mkdtemp(prefix="verify-", dir=self.private))
        try:
            scorecard, sig = work / "scorecard.json", work / "scorecard.sig"
            ok_fetch = bool(ev.get("scorecardKey") and ev.get("sidecarKey"))
            if ok_fetch:
                for key, path in ((ev["scorecardKey"], scorecard), (ev["sidecarKey"], sig)):
                    path.write_text(self.lab.kubectl("exec", "mc", "--", "mc", "cat", f"local/{BUCKET}/{key}",
                                                     ns=self.ns).stdout)
            pub = work / "signing.pub.pem"
            pub.write_text(roster["spec"]["signingKeys"][0]["spkiPem"])
            rust = self.lab.run([str(self.cli), "drill", "verify", "--scorecard", str(scorecard), "--signature",
                                 str(sig), "--public-key", str(pub)], check=False, timeout=60) if ok_fetch else None
            py = self.lab.run([self.python, str(self.lab.root / "docs/verify_scorecard.py"), str(scorecard),
                               str(sig), str(pub)], check=False, timeout=60) if ok_fetch else None
            text = scorecard.read_text() if ok_fetch else ""
            self.lab.write("native/scorecard.json", text or {})
            clauses = {
                "the controller's verdict is Valid": (ev.get("verification") or {}).get("result") == "Valid",
                "the signed scorecard was fetched from the archive": ok_fetch,
                "`logweir drill verify` accepts it": rust is not None and rust.returncode == 0,
                "the independent python verifier accepts it": py is not None and py.returncode == 0,
                "the signed bytes name the older point": old_id in text,
                "and not the newer one": new_id not in text,
            }
            self.record("restore-evidence-verifies-with-two-verifiers", all(clauses.values()),
                        f"verification {(ev.get('verification') or {}).get('result')!r}, scorecard "
                        f"{ev.get('scorecardKey')!r}; " + "; ".join(f"{k}={v}" for k, v in clauses.items()),
                        {"clauses": clauses, "scorecardKey": ev.get("scorecardKey"),
                         "rustRc": rust.returncode if rust else None, "pythonRc": py.returncode if py else None})
        finally:
            shutil.rmtree(work, ignore_errors=True)

    # -- cleanup --------------------------------------------------------------

    def cleanup(self, extra_prefixes: list[str]) -> dict[str, Any]:
        proof: dict[str, Any] = {"topics": [], "prefixes": [], "evidence": []}
        self.stop_api()
        if self.lab.get_opt("pod", "kafka-client", self.ns) is not None:
            for broker, topic in self.created_topics:
                if self.tag not in topic:  # only names this run minted
                    continue
                self.kafka(broker, "kafka-topics.sh", "--delete", "--topic", topic, check=False)
                proof["topics"].append({"broker": broker, "topic": topic})
        if self.lab.get_opt("pod", "mc", self.ns) is not None:
            for prefix in [self.prefix] + extra_prefixes:
                if self.stamp not in prefix:
                    continue
                self.lab.kubectl("exec", "mc", "--", "mc", "rm", "--recursive", "--force",
                                 f"local/{BUCKET}/{prefix}/", ns=self.ns, check=False, timeout=300)
                proof["prefixes"].append({"prefix": f"{BUCKET}/{prefix}/",
                                          "leftAfter": len(self.objects(prefix + "/"))})
            for key in self.evidence_keys:
                self.lab.kubectl("exec", "mc", "--", "mc", "rm", f"local/{BUCKET}/{key}", ns=self.ns, check=False)
                proof["evidence"].append(key)
        proof["namespace"] = self.lab.delete_owned_namespace(self.ns)
        shutil.rmtree(self.private, ignore_errors=True)
        proof["privateDirRemoved"] = not self.private.exists()
        return proof

