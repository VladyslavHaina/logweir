# Phase B's exit criterion — `just k8s-demo`, run and recorded

> **2026-09-12 — THE REGISTRY NAMESPACE MOVED AFTER THIS TRANSCRIPT WAS RECORDED, AND NOTHING
> BELOW IS EDITED.** A record is not a config file. The image references below are the ones that
> actually ran on the day; the SHIPPED references are now `docker.io/vladyslavhaina/logweir`
> (runner) and `docker.io/vladyslavhaina/weirkeeper` (controller), on Docker Hub. Why they moved,
> and what a node has to hold before either resolves: `../docs/kubernetes.md` §14.


**Task 24.** `weirkeeper` verifies the evidence the UI renders, in a real
cluster, with a read-only credential it was given as environment — and the two
objects it verified carry the verdict on their own status.

- **Run:** 2026-09-11, `just k8s-demo; echo "rc=$?"` → **rc=0**.
- **Cluster:** `docker-desktop`. **Control plane:** `logweir-system`. **Custom
  resources:** `logweir-t24` (STANDING RULE 13's T21–T24 exception).
- **Object store:** the compose stack's MinIO, addressed from inside the
  cluster as `http://host.docker.internal:9000` with `AWS_REGION=us-east-1`
  and `AWS_ALLOW_HTTP=true`. Archive: `s3://kafka-backups/k8s-demo`.
- **Broker:** the compose stack's `K8S` listener, `host.docker.internal:9095`
  (Task 7).
- **Images:** `ghcr.io/logweir/logweir@sha256:6440a4a0…` (runner) and
  `ghcr.io/logweir/weirkeeper@sha256:a5aa6dc1…` (controller), both by DIGEST,
  both resolved on this node by the **author-only** `docker tag` step of
  `../docs/kubernetes.md` §14.3. **Global Constraint 37 is not relaxed by this
  run:** a locally built image is author-only, "published" means a PULL from a
  registry the author does not control, and the install file's digest rows
  still read `blocked: no remote`.
- **Every field below is read with `kubectl -o jsonpath` and every exit code on
  its own line** (STANDING RULE 20). Nothing whose status is load-bearing is
  piped; `crates/logweir/tests/k8s_demo_lint.rs` runs Task 12's I29 tokeniser
  over the script in the default test suite.

## 1. Pre-flight — the CRD list first

```
    rc=0  (kubectl get crd -o name)
    logweir.dev CRDs already installed: 0
    rc=0  (docker image inspect logweir:check)
    rc=0  (docker image inspect weirkeeper:check)
```

STANDING RULE 13 permits **0** (a clean cluster) or exactly **6** (this plan's
own install) and nothing between: a partial set means another agent owns the
cluster, or a previous run died halfway. The run refuses rather than starting.

**Verdict: PASS** — a clean cluster, both images present locally.

## 2. `just lint`, with the compose stack DOWN

```
    rc=0  (just lint)
```

`scripts/time-unit-suite.sh` refuses to run, exit 1, while 9092 or 9000
answers (Global Constraint 22), so this is the only point in the run at which
it can happen: it is step **2** and the stack comes up at step **3**.
`the_demo_runs_lint_before_the_stack_is_up` asserts that ordering over the
script text.

**Verdict: PASS** — lint green with the stack down.

## 3. Check-then-take, then the compose stack

```
    rc=0  (docker ps --filter name=logweir-)
    no logweir-* containers: the stack is free to take
    rc=0  (just e2e-up)
```

STANDING RULE 3: the stack has one owner at a time, so the run confirms no
`logweir-*` container is running before taking it, and `just e2e-down` gives it
back at the end.

**Verdict: PASS** — the stack was free and was taken.

## 4. The bucket, inside the compose network, before any custom resource

```
    rc=0  (mc mb local/kafka-backups)
```

`just e2e-down` runs `down -v` and EMPTIES the MinIO volume (plan erratum
E12g), so every owner of the stack makes its own bucket — and it has to happen
before a `Backup` exists to write into it. The archive prefix `k8s-demo` is
swept at both ends.

**Verdict: PASS** — `kafka-backups` exists inside the compose network.

## 5. Records, and the recovery point

```
    rc=0  (kafka-topics --create k8sdemo)
    rc=0  (kafka-console-producer -> k8sdemo)
    rc=0  (kafka-get-offsets k8sdemo)
    k8sdemo holds 200 records
    recovery point: 2026-09-11T11:44:28Z   sample window from: 2026-09-11T10:44:28Z
```

Plan errata E6/E7: every epoch instant in this project is COMPUTED, never
typed. `point_in_time` is five seconds after the last record — the boundary is
inclusive (guard **G-PITR**) — and strictly above the archive floor.

**Verdict: PASS** — 200 records on the broker, read back off it rather than
assumed from the producer's exit code.

## 6. Two keys, and only public halves leave the directory

```
    signing key id:  06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c
    approver key id: 2bb90872219f1f22e0be5dc060cac7fb50c505a8ca7326433124d8151f891ead
    WARNING: both were minted on this machine, minutes ago, and are attested by nothing.
```

The `keyId` is `VerifyingKey::key_id()`: the sha256 of the SPKI **DER**,
computed here with `openssl` so this script and `logweir-verify` cannot
disagree about it. The signing key's PUBLIC half goes on the `TrustRoster`;
`signing.pem` goes into a Secret and nowhere else.

**Verdict: PASS** — two different keys, and the warning is the honest one: a
signature over a key nobody has published proves integrity, not provenance.

## 7. The author-only image step

```
    rc=0  (docker tag logweir:check ghcr.io/logweir/logweir:v0.1.0)
    rc=0  (docker tag weirkeeper:check ghcr.io/logweir/weirkeeper:v0.1.0)
    rc=0  (docker inspect RepoDigests ghcr.io/logweir/logweir:v0.1.0)
["logweir@sha256:6440a4a06d6f4a0ecbef71fa7d8ad11b5a87f3670298c585d5cd0073ae2e1229","ghcr.io/logweir/logweir@sha256:6440a4a06d6f4a0ecbef71fa7d8ad11b5a87f3670298c585d5cd0073ae2e1229"]
```

**The kubelet keys on the WHOLE reference, not on the digest** (plan erratum
E19b, `../docs/kubernetes.md` §14.3): `ghcr.io/logweir/logweir@sha256:…` is
`ErrImageNeverPull` on a node that holds the same digest under the local name
`logweir:check`, until one `docker tag` makes the repository name resolve. Both
tags are removed by the cleanup at step 12.

**Verdict: PASS, and AUTHOR-ONLY** — the shipped reference resolves here and
proves nothing about a stranger's cluster.

## 8. `logweir.yaml`, then the demo's own env patch

```
    rc=0  (kubectl apply --server-side -f logweir.yaml)
    rc=0  (kubectl patch deployment weirkeeper --patch-file config/overlays/k8s-demo/deployment-env-patch.yaml)
    rc=0  (kubectl rollout status deploy/weirkeeper)
```

The shipped file is applied **verbatim**. `AWS_ENDPOINT_URL=http://host.docker.internal:9000`,
`AWS_REGION=us-east-1` and `AWS_ALLOW_HTTP=true` are a strategic-merge patch on
top (`../config/overlays/k8s-demo/deployment-env-patch.yaml`), because a
manifest naming one laptop's hostname is not a file a stranger can apply
(Global Constraint 37). The controller FORWARDS those values to every runner
Job it creates, so the endpoint is configured once rather than in two halves
that can disagree.

**Verdict: PASS** — the control plane rolled out from the digest reference.

## 9. The namespace, the five Secrets and the `TrustRoster`

```
    rc=0  (kubectl create namespace logweir-t24)
    rc=0  (kubectl apply -f config/rbac/backup-runner-serviceaccount.yaml)
    rc=0  (secret/logweir-signing-key)
    rc=0  (secret/logweir-s3)
    rc=0  (secret/logweir-evidence-ro in logweir-system)
    rc=0  (kubectl rollout restart deploy/weirkeeper)
    rc=0  (kubectl rollout status deploy/weirkeeper, after the evidence Secret)
    rc=0  (kubectl apply -f trustroster.yaml)
```

`logweir-evidence-ro` is the **fifth Secret** and the one this task is about: a
read-only credential, a **different principal** from the runner's `logweir-s3`,
created in `logweir-system`. The controller reads it from **its own
environment** and never through the API — the `weirkeeper` ClusterRole grants
no verb on `secrets` — which is why the Deployment is restarted after it is
created: container environment is fixed at start.

The roster is the cluster-scoped `default` (interface **I16**) and its
`signingKeys[]` carries the runner's public key (interface **I17**). With a
`signingKeyIds: [string]` shape there would be nothing to verify against and
everything below would read `NotAttempted`.

**Verdict: PASS** — five Secrets, `just check-secrets logweir-t24` green, and a
roster carrying key material.

## 10. `KafkaCluster` → `reachable: true`

```
    rc=0  (kubectl apply -f kafkacluster.yaml)
    rc=0  (kubectl get kafkacluster demo -o jsonpath={.status.reachable})
    reachable: true
```

Interface **I14**: the reconciler runs `logweir cluster-probe` as a
short-lived Job and reads its two stdout lines by key name. It never dials a
broker itself and never reads a Secret.

**Verdict: PASS** — `reachable: true` off the probe's own exit.

## 11. A `Backup`, then an approved `Restore`

```
    rc=0  (kubectl apply -f backup.yaml)
    rc=0  (kubectl get backup demo-backup -o jsonpath={.status.exitCode})
    exitCode: 0
    rc=0  (kubectl get backup demo-backup -o jsonpath={.status.evidence.verification.result})
    verification.result: Valid
approved .demo/k8s/restore-plan.yaml
  plan_hash  sha256:f27c42d6398358e4169e470215b5bcfaab86b820c6faaa2e80608171cb548304
  approver   k8s-demo
  ticket     DEMO-24
  subject    Restore
  key_id     2bb90872219f1f22e0be5dc060cac7fb50c505a8ca7326433124d8151f891ead
  wrote      .demo/k8s/approval.json
  wrote      .demo/k8s/approval.sig

This approval binds the EXACT bytes of .demo/k8s/restore-plan.yaml. Edit the spec — including its
sample window — and `logweir drill run` refuses with exit 3 until you re-run
this command.
    rc=0  (logweir drill approve --subject-kind Restore)
    rc=0  (secret/logweir-approval-bundle)
    rc=0  (secret/kafka-scram)
    rc=0  (just check-secrets logweir-t24)
    rc=0  (kubectl apply -f restore.yaml)
    rc=0  (kubectl get restore demo-restore -o jsonpath={.status.phase})
    phase: Succeeded
    rc=0  (kubectl get restore demo-restore -o jsonpath={.status.evidence.verification.result})
    verification.result: Valid
```

The approval binds the EXACT bytes of the plan; the `Restore`'s `spec.planBytes`
carries the same document, and the reconciler recomputes the hash at
Job-creation time and compares it against the hash inside the signed approval —
never against `Approval.status`, which is a cache.

**Verdict: PASS** — `exitCode: 0` and `Valid` on the `Backup`;
`phase: Succeeded` on the `Restore`.

## 12. The exit criterion, field by field

```
  Backup demo-backup:
    rc=0  status.exitCode: 0
    rc=0  verification.result: Valid
    rc=0  verification.matchedKeyId: 06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c
    rc=0  verification.verifiedAt: 2026-09-11T11:44:50Z
    rc=0  evidence.receiptKey: logweir/backups/c96f08dd-068b-4b1d-af13-20975ef4354d/01M284HKVWJ3321WZ8ZJK6EMYV.receipt.json
  Restore demo-restore:
    rc=0  status.phase: Succeeded
    rc=0  status.exitCode: 0
    rc=0  status.outcome: pass
    rc=0  verification.result: Valid
    rc=0  verification.matchedKeyId: 06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c
    rc=0  evidence.scorecardKey: logweir/drills/01M284HYD3XPGVC7FK31EPWZBV.json
    rc=0  topicPreflight.timestampType: CreateTime
    rc=0  (backup .status.evidence)
{"receiptKey":"logweir/backups/c96f08dd-068b-4b1d-af13-20975ef4354d/01M284HKVWJ3321WZ8ZJK6EMYV.receipt.json","receiptSha256":"sha256:4014e101db762a923bcd8a78d6db5fb375d4d05a73b2738f0bd7f9280851f267","sidecarKey":"logweir/backups/c96f08dd-068b-4b1d-af13-20975ef4354d/01M284HKVWJ3321WZ8ZJK6EMYV.receipt.sig","verification":{"matchedKeyId":"06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c","payloadType":"application/vnd.logweir.backup-receipt+json;version=1.0.0","result":"Valid","verifiedAt":"2026-09-11T11:44:50Z"}}
    rc=0  (restore .status.evidence)
{"offsetReportKey":"logweir/drills/01M284HYD3XPGVC7FK31EPWZBV.offsets.json","offsetReportSha256":"sha256:73160740c766bbf2d99e61d29935a8511f96369e0c787524a1cd67668fac21b7","scorecardKey":"logweir/drills/01M284HYD3XPGVC7FK31EPWZBV.json","scorecardSha256":"sha256:79cea5dfc46ba52b50c1ddc8e4a2a6bf3dad414b90e9f33c8cfcc6a5514d95dd","sidecarKey":"logweir/drills/01M284HYD3XPGVC7FK31EPWZBV.sig","verification":{"matchedKeyId":"06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c","payloadType":"application/vnd.logweir.drill-scorecard+json;version=1.0.0","result":"Valid","verifiedAt":"2026-09-11T11:45:02Z"}}
    rc=0  (kubectl logs deploy/weirkeeper --tail=80)
```

The reads above are these twelve commands, each one on its own line with its
status on the next (STANDING RULE 20) — `scripts/k8s-demo.sh`'s `read_field`
runs exactly these:

```bash
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.exitCode}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.evidence.verification.result}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.evidence.verification.matchedKeyId}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.evidence.verification.verifiedAt}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.evidence.receiptKey}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.phase}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.exitCode}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.outcome}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.evidence.verification.result}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.evidence.verification.matchedKeyId}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.evidence.scorecardKey}'; echo "rc=$?"
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.topicPreflight.timestampType}'; echo "rc=$?"
```

Both `matchedKeyId`s are the roster entry's own `keyId` — the string an
operator can grep for in the object they edit — and both `verification` blocks
carry the `payloadType` that was verified:
`application/vnd.logweir.backup-receipt+json;version=1.0.0` for the receipt and
`application/vnd.logweir.drill-scorecard+json;version=1.0.0` for the scorecard.

`receiptSha256` and `scorecardSha256` are the digests the controller computed
over the bytes it fetched. They are what step 3 of `verify_evidence` compares
against on a LATER pass: without them a verification could only check the
signature, and a genuinely-signed OLDER document put in this one's place would
verify.

`topicPreflight.timestampType: CreateTime` is plan erratum **E10(c)** closed on
both sides in one run — the runner printed `topic-preflight=…` and the
reconciler scanned it by key name.

**Verdict: PASS — PHASE B'S EXIT CRITERION IS MET.**

## 13. The controller's own log

```
{"timestamp": "2026-09-11T11:44:31Z", "level": "INFO", "fields": {"message": "built the controller's one archive handle; it cannot write (Global Constraint 6, guard G-RET)", "archive_url": "s3://kafka-backups/k8s-demo", "read_only": true}}
{"timestamp": "2026-09-11T11:44:31Z", "level": "INFO", "fields": {"message": "weirkeeper started", "controllers": 6, "default_namespace": "logweir-system"}}
{"timestamp": "2026-09-11T11:44:50Z", "level": "INFO", "fields": {"message": "weirkeeper verified this Backup's signed receipt with its read-only evidence credential", "backup": "demo-backup", "namespace": "logweir-t24", "verification": "Valid", "matched_key_id": "06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c", "green": true, "badge": "verified by weirkeeper at 2026-09-11T11:44:50Z against key 06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c"}}
{"timestamp": "2026-09-11T11:45:01Z", "level": "INFO", "fields": {"message": "weirkeeper verified this Restore's signed scorecard with its read-only evidence credential", "restore": "demo-restore", "namespace": "logweir-t24", "verification": "Valid", "matched_key_id": "06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c", "green": true, "badge": "verified by weirkeeper at 2026-09-11T11:45:01Z against key 06512f32b7d211ee0efb6f1381b60937fd28678d0c4abcccfbd63143c8903c7c"}}
```

Three things this says that nothing else does:

1. **The verdict is the CONTROLLER's.** `green: true` and the badge string are
   computed in-cluster by `verification::{backup_badge, restore_badge}`, not
   by this script and not in a browser.
2. **The handle cannot write** — `read_only: true`, Global Constraint 6, guard
   **G-RET**.
3. **The last `Backup` line carries `verifiedAt: 11:44:50` at 11:45:05.** The
   verdict was not re-dated on a later reconcile, which is what makes the
   second status patch a no-op and keeps the reconciler quiet (plan erratum
   **E11(d)**).

**MEASURED, AND IT WAS A DEFECT FIRST.** An earlier run of this same demo — the
one that first met every field above — logged **20 `Backup` and 20 `Restore`
reconciles per second**, each a real write: the terminal `/status` patch
replaced the `conditions` array and deleted the `Verified` condition the second
patch had just added, which re-added it, which woke the reconciler again. The
fix is `verification::carry_verified`, and the run above is 27 log lines over
34 seconds — three reconciles per object, which is exactly the 15-second
requeue.

**Verdict: PASS** — the control plane is quiet on a settled object.

## 14. Cleanup

```
    rc=0  (kubectl delete -f logweir.yaml)
    rc=0  (kubectl delete ns logweir-system logweir-t24)
    rc=0  (docker rmi the two author-only tags)
    rc=0  (just e2e-down)
k8s-demo rc=0
```

The cluster and the registry are left as they were found: the install file
deleted, both namespaces deleted, the two author-only image tags removed, the
archive prefix swept, and the compose stack down. Every one with its own `rc`.

**Verdict: PASS** — `just k8s-demo; echo "rc=$?"` → **rc=0**.


Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
