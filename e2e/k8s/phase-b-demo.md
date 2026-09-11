# Phase B's exit criterion — `just k8s-demo`, run and recorded

**Task 24.** `weirkeeper` verifies the evidence the UI renders, in a real
cluster, with a read-only credential it was given as environment — and the two
objects it verified carry the verdict on their own status.

- **Cluster:** `docker-desktop`. **Control plane:** `logweir-system`. **Custom
  resources:** `logweir-t24` (STANDING RULE 13's T21–T24 exception).
- **Object store:** the compose stack's MinIO, addressed from inside the
  cluster as `http://host.docker.internal:9000` with `AWS_REGION=us-east-1`
  and `AWS_ALLOW_HTTP=true`. Archive: `s3://kafka-backups/k8s-demo`.
- **Broker:** the compose stack's `K8S` listener, `host.docker.internal:9095`
  (Task 7).
- **Images:** `ghcr.io/logweir/logweir@sha256:3e9828d4…` (runner) and
  `ghcr.io/logweir/weirkeeper@sha256:767e3af2…` (controller), both by DIGEST,
  both resolved on this node by the **author-only** `docker tag` step of
  `docs/kubernetes.md` §14.3. **Global Constraint 37 is not relaxed by this
  run:** a locally built image is author-only, "published" means a PULL from a
  registry the author does not control, and the install file's digest rows
  still read `blocked: no remote`.
- **Every field below is read with `kubectl -o jsonpath` and every exit code on
  its own line** (STANDING RULE 20). Nothing whose status is load-bearing is
  piped.

## 1. Pre-flight — the CRD list first

```bash
kubectl --context docker-desktop get crd -o name; echo "rc=$?"
# rc=0
# logweir.dev CRDs already installed: 0
docker image inspect logweir:check; echo "rc=$?"      # rc=0
docker image inspect weirkeeper:check; echo "rc=$?"   # rc=0
```

STANDING RULE 13 permits **0** (a clean cluster) or exactly **6** (this plan's
own install) and nothing between: a partial set means another agent owns the
cluster, or a previous run died halfway.

**Verdict: PENDING — rerun and paste.**

## 2. `just lint`, with the compose stack DOWN

```bash
just lint; echo "rc=$?"
# rc=0
```

`scripts/time-unit-suite.sh` refuses to run, exit 1, while 9092 or 9000
answers (Global Constraint 22), so this is the only point in the run at which
it can happen. It is step 2 and the stack comes up at step 3.

**Verdict: PENDING — rerun and paste.**

## 3. Check-then-take, then the compose stack

```bash
docker ps --filter name=logweir- --format "{{.Names}}"; echo "rc=$?"
# rc=0   (no logweir-* containers: the stack is free to take)
just e2e-up; echo "rc=$?"
# rc=0
```

**Verdict: PENDING — rerun and paste.**

## 4. The bucket, inside the compose network

```bash
docker compose -f e2e/compose/docker-compose.yml run --rm -T --entrypoint mc \
  minio-setup mb --ignore-existing local/kafka-backups; echo "rc=$?"
# rc=0
```

`just e2e-down` runs `down -v` and EMPTIES the MinIO volume (plan erratum
E12g), so every owner of the stack makes its own bucket — **before** any custom
resource exists to write into it. The archive prefix `k8s-demo` is swept at
both ends.

**Verdict: PENDING — rerun and paste.**

## 5. Records, and the recovery point

```bash
# kafka-topics --create k8sdemo; echo "rc=$?"          rc=0
# kafka-console-producer -> k8sdemo; echo "rc=$?"      rc=0
# kafka-get-offsets k8sdemo; echo "rc=$?"              rc=0
```

**Verdict: PENDING — rerun and paste.**

## 6. Two keys, and only public halves leave the directory

```bash
# signing key id:  PENDING
# approver key id: PENDING
```

Both were minted on this machine, minutes ago, and are attested by nothing: a
signature over a key nobody has published proves integrity, not provenance.

**Verdict: PENDING — rerun and paste.**

## 7. The author-only image step

```bash
docker tag logweir:check ghcr.io/logweir/logweir:v0.1.0; echo "rc=$?"
# rc=0
docker tag weirkeeper:check ghcr.io/logweir/weirkeeper:v0.1.0; echo "rc=$?"
# rc=0
docker inspect --format '{{json .RepoDigests}}' ghcr.io/logweir/logweir:v0.1.0; echo "rc=$?"
# rc=0
```

**The kubelet keys on the WHOLE reference, not on the digest** (plan erratum
E19b, `docs/kubernetes.md` §14.3): `ghcr.io/logweir/logweir@sha256:…` is
`ErrImageNeverPull` on a node that holds the same digest under the local name
`logweir:check`, until one `docker tag` makes the repository name resolve.
This is **author-only** and the tags are removed at the end.

**Verdict: PENDING — rerun and paste.**

## 8. `logweir.yaml`, then the demo's own env patch

```bash
kubectl --context docker-desktop apply --server-side -f logweir.yaml; echo "rc=$?"
# rc=0
kubectl --context docker-desktop -n logweir-system patch deployment weirkeeper \
  --patch-file config/overlays/k8s-demo/deployment-env-patch.yaml; echo "rc=$?"
# rc=0
kubectl --context docker-desktop -n logweir-system rollout status deploy/weirkeeper --timeout=180s; echo "rc=$?"
# rc=0
```

The shipped file is applied **verbatim**: `AWS_ENDPOINT_URL=http://host.docker.internal:9000`,
`AWS_REGION=us-east-1` and `AWS_ALLOW_HTTP=true` are a patch on top, so a
stranger can apply `logweir.yaml` unedited (Global Constraint 37).

**Verdict: PENDING — rerun and paste.**

## 9. The namespace, the five Secrets and the `TrustRoster`

```bash
kubectl --context docker-desktop create namespace logweir-t24; echo "rc=$?"   # rc=0
# secret/logweir-signing-key       rc=0
# secret/logweir-s3                rc=0
# secret/logweir-evidence-ro       rc=0   (in logweir-system — a DIFFERENT principal)
just check-secrets logweir-t24; echo "rc=$?"
```

`logweir-evidence-ro` is the fifth Secret and the one this task is about. The
controller reads it from **its own environment** and never through the API —
the `weirkeeper` ClusterRole grants no verb on `secrets` — so the Deployment is
restarted after it is created.

**Verdict: PENDING — rerun and paste.**

## 10. `KafkaCluster` → `reachable: true`

```bash
kubectl --context docker-desktop -n logweir-t24 get kafkacluster demo \
  -o jsonpath='{.status.reachable}'; echo "rc=$?"
# reachable: true
# rc=0
```

Interface **I14**: the reconciler runs `logweir cluster-probe` as a short-lived
Job and reads its two stdout lines by key name. It never dials a broker itself.

**Verdict: PENDING — rerun and paste.**

## 11. A `Backup`, then an approved `Restore`

```bash
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup \
  -o jsonpath='{.status.exitCode}'; echo "rc=$?"
# exitCode: 0
# rc=0
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore \
  -o jsonpath='{.status.phase}'; echo "rc=$?"
# phase: Succeeded
# rc=0
```

**Verdict: PENDING — rerun and paste.**

## 12. The exit criterion, field by field

```bash
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.exitCode}'; echo "rc=$?"
# exitCode: 0
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.evidence.verification.result}'; echo "rc=$?"
# result: Valid
kubectl --context docker-desktop -n logweir-t24 get backup demo-backup -o jsonpath='{.status.evidence.verification.matchedKeyId}'; echo "rc=$?"
# matchedKeyId: PENDING
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.phase}'; echo "rc=$?"
# phase: Succeeded
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.exitCode}'; echo "rc=$?"
# exitCode: 0
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.outcome}'; echo "rc=$?"
# outcome: pass
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.evidence.verification.result}'; echo "rc=$?"
# result: Valid
kubectl --context docker-desktop -n logweir-t24 get restore demo-restore -o jsonpath='{.status.evidence.verification.matchedKeyId}'; echo "rc=$?"
# matchedKeyId: PENDING
```

**Verdict: PENDING — rerun and paste.**

## 13. The controller's own log

```bash
kubectl --context docker-desktop -n logweir-system logs deploy/weirkeeper --tail=80; echo "rc=$?"
# rc=0
```

**Verdict: PENDING — rerun and paste.**

## 14. Cleanup

```bash
kubectl --context docker-desktop delete -f logweir.yaml; echo "rc=$?"
kubectl --context docker-desktop delete ns logweir-system logweir-t24 --ignore-not-found; echo "rc=$?"
docker rmi ghcr.io/logweir/logweir:v0.1.0 ghcr.io/logweir/weirkeeper:v0.1.0; echo "rc=$?"
just e2e-down; echo "rc=$?"
```

**Verdict: PENDING — rerun and paste.**

