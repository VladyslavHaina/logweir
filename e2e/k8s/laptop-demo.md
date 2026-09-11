# Phase C's exit criterion — `just laptop-demo`, run and recorded, with X-UIWRITE from the page

The walk spec §1 calls **the whole acceptance surface**: a stranger clones, applies one file to
docker-desktop Kubernetes and gets CRDs, RBAC and the controller; the UI is static files served by
`kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1`; against a
plain broker from the compose stack they create a `BackupSchedule`, watch a `Backup` produce a signed
receipt, run a drill (a `Restore` with a `newTopic` target) and read a signed scorecard with measured
RTO and RPO — **never typing a private key into a browser**.

Twelve numbered steps, one section each below, each with the transcript it produced and a verdict.

**Two passes, and the second one is the gate.** Spec §10's X-UIWRITE reads: under `kubectl proxy
--www=`, a `create` of a `Restore` **from the page** returns 201. **`curl` is not the page.** So:

| pass | command | what it records |
|---|---|---|
| 1 | `LOGWEIR_DEMO_NONINTERACTIVE=1 LOGWEIR_DEMO_KEEP=1 just laptop-demo` | steps 1–12; `KEEP` defers the teardown so pass 2 has a proxy and a cluster to talk to |
| 2 | `LOGWEIR_DEMO_ONLY_STEP=10b ./scripts/laptop-demo.sh` | step 10(b) alone, by hand, in a real browser, against the proxy pass 1 left up |
| teardown | `LOGWEIR_DEMO_ONLY_STEP=teardown ./scripts/laptop-demo.sh` | the cleanup, every `rc` |
| again, the default form | `LOGWEIR_DEMO_NONINTERACTIVE=1 just laptop-demo; echo "rc=$?"` | the same walk with the teardown in its own `trap`, end to end, `rc=0` |

Run on 2026-09-11 on docker-desktop Kubernetes (client v1.35, context `docker-desktop`), against the
compose stack's KRaft broker and MinIO, with the two locally built images at the digests the tree pins
(`logweir:check` = `sha256:6440a4a0…`, `weirkeeper:check` = `sha256:d198c8e2…`). **Neither image was
rebuilt**: a rebuild moves a repository digest (plan erratum E19a) and those two digests are what
`crates/weirkeeper/src/job.rs` and `config/manager/deployment.yaml` name.

**Two steps of this walk are author-only and say so.** Step 1 tags the local images with the shipped
`ghcr.io/logweir/…` names, because the kubelet keys on the WHOLE reference and a matching digest under
a different name is `ErrImageNeverPull`; step 3 patches the controller's environment to point at this
laptop's MinIO. Neither relaxes Global Constraint 37: "published" still means a pull from a registry
the author does not control, and the install file's digest rows still read `blocked: no remote`.

## 1. Preflight — the context first, and it refuses before anything dials

The context check is **first**, before a namespace is created, before an image is inspected, before
anything reaches a socket. A kubeconfig whose current context is anything else gets, on stderr,
`refusing: current context is <what it found>, not docker-desktop`, and exit 1 —
`crates/logweir/tests/laptop_demo_lint.rs::laptop_demo_refuses_a_wrong_context` proves that with a
stubbed `kubectl` that answers `kind-x` and dials nothing.

The compose stack is a PRECONDITION here and not a step, because spec §2's Demo 1 opens with
`docker compose … up`; STANDING RULE 3 binds the other end, and the teardown runs `just e2e-down`.

`==> 1/12 preflight: context, tools, the compose stack, both local images`

```
    rc=0  (kubectl config current-context) -> docker-desktop
    rc=0  (docker compose ps --status running)
    compose services running: kafka-broker-1 minio 
    rc=0  (docker image inspect logweir:check) -> ["logweir@sha256:6440a4a06d6f4a0ecbef71fa7d8ad11b5a87f3670298c585d5cd0073ae2e1229"]
    rc=0  (docker image inspect weirkeeper:check) -> ["weirkeeper@sha256:d198c8e2657c340ad9cab24da3975d13986634c92f6765a00f0dd445c2040276"]
    rc=0  (kubectl get crd -o name)
    logweir.dev CRDs already installed: 0
    rc=0  (docker tag logweir:check ghcr.io/logweir/logweir:v0.1.0 — author-only, removed by the teardown)
    rc=0  (docker tag weirkeeper:check ghcr.io/logweir/weirkeeper:v0.1.0 — author-only, removed by the teardown)
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): the preflight passed: docker-desktop, the compose stack, both local images.
```

**Verdict: PASS — the context is `docker-desktop`, the stack is up, both images are present at the pinned digests, the cluster holds no half-installed CRD set.**

## 2. The namespace, and the runner ServiceAccount

The runner ServiceAccount lives in the namespace of the `Backup`/`Restore` objects, so it is NOT in
`logweir.yaml` (plan erratum E14c); a runner pod mounts no token, so it needs no RoleBinding.

`==> 2/12 namespace logweir-t28`

```
    rc=0  (kubectl create namespace logweir-t28)
    rc=0  (kubectl apply -f config/rbac/backup-runner-serviceaccount.yaml)
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): namespace logweir-t28 and the runner ServiceAccount exist.
```

**Verdict: PASS — `logweir-t28` and the runner ServiceAccount exist.**

## 3. `just apply-install` — install gate **X-APPLY**

`just apply-install` is the gate: `kubectl --context docker-desktop apply --server-side -f
logweir.yaml`, **twice**, with both exit codes read directly. The demo's own S3 literals are a
kustomize patch applied on top — the shipped file is applied unedited, and the controller forwards the
endpoint, the region and the allow-http flag to every runner Job it creates, so they are configured
once instead of in two places that can disagree.

`==> 3/12 just apply-install (install gate X-APPLY), then the author-only demo overlay`

```
    rc=0  (just apply-install — kubectl apply --server-side -f logweir.yaml, twice)
clusterrole.rbac.authorization.k8s.io/weirkeeper serverside-applied
clusterrolebinding.rbac.authorization.k8s.io/weirkeeper serverside-applied
deployment.apps/weirkeeper serverside-applied
networkpolicy.networking.k8s.io/logweir-runner-egress serverside-applied
    rc=0  (kubectl patch deployment weirkeeper --patch-file config/overlays/k8s-demo/deployment-env-patch.yaml)
    rc=0  (kubectl rollout status deploy/weirkeeper)
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): X-APPLY: logweir.yaml applied twice, no error on either run; the controller is ready.
```

**Verdict: PASS — install gate **X-APPLY**: `kubectl apply --server-side -f logweir.yaml` twice, no error on either run, and the controller rolled out.**

## 4. Two keypairs, and the silent-mint warning

Both keypairs are minted into `.demo/laptop/` (gitignored) with the four `openssl` commands
`scripts/demo.sh` uses, and **the teardown deletes both private halves**. The warning is not
decoration: `SigningKey::load_or_generate` LOADS a path that exists and MINTS one that does not, so a
first run against an empty `logweir-signing-key` Secret produces a green scorecard signed by a key
nothing attests.

`==> 4/12 minting the signing and approver keypairs into .demo/laptop/`

```
    signing key id:  2190b61d2de7a3158a8790cd2dd15f2421db6d9f7b42a2c9a79affb2dd797f79
    approver key id: ad9836ead774e6d720894fc34d3a66bb417a7334da589d1f76957ec978d9176c

    WARNING — SigningKey::load_or_generate MINTS SILENTLY.
    crates/logweir-evidence/src/keys.rs: a path that EXISTS is loaded; a path that
    is ABSENT is minted. So a run against an empty logweir-signing-key Secret
    produces evidence signed by a key nothing attests — a green scorecard with a
    signature no TrustRoster can match. Both keypairs above were minted on this
    machine, seconds ago, and are attested by nothing; the teardown deletes them.
    NEITHER PRIVATE HALF EVER REACHES THE BROWSER (step 11).
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): two keypairs in .demo/laptop/; only the public halves leave this directory.
```

**Verdict: PASS — two keypairs, and the silent-mint warning printed where an operator reads it.**

## 5. The five Secrets, and `just check-secrets`

Five Secrets, not three (spec §9): the signing key (data key `signing.pem` — the file name the runner's
argv reads), the approval bundle's four keys, the per-cluster SCRAM credential (unused on this
plaintext listener, and created anyway because an install missing one is exactly what `check-secrets`
exists to catch), the runner's archive credential, and the CONTROLLER's read-only evidence credential
in `logweir-system` — a different principal, which is the separation this proves. The controller is
restarted because it reads that Secret from its own environment, fixed at container start.

Two of the approval bundle's four keys cannot exist yet — `approval.json` and `approval.sig` are
signatures over plan bytes that step 10 has not rendered — so the bundle is created with placeholders
and **step 11 replaces it**, before the `Approval` object exists and therefore before any runner Job
can mount it. Saying so is more honest than reordering the walk to hide it.

`==> 5/12 the five Secrets, then just check-secrets logweir-t28`

```
    rc=0  (secret/logweir-signing-key, data key signing.pem)
    rc=0  (secret/logweir-approval-bundle, four keys — approval.json/.sig replaced at step 11)
    rc=0  (secret/kafka-scram, data key password)
    rc=0  (secret/logweir-s3)
    rc=0  (secret/logweir-evidence-ro, in logweir-system)
    rc=0  (kubectl rollout restart deploy/weirkeeper — env is fixed at container start)
    rc=0  (kubectl rollout status deploy/weirkeeper, after the evidence Secret)
    rc=0  (just check-secrets logweir-t28)
check-secrets: all five Secrets are present (logweir-t28, and logweir-evidence-ro in logweir-system).
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): all five Secrets are present (four in logweir-t28, logweir-evidence-ro in logweir-system).
```

**Verdict: PASS — five Secrets, and `just check-secrets logweir-t28` agrees.**

## 6. The cluster-scoped `TrustRoster` named `default`

`signingKeys[]` carries the runner's PUBLIC KEY, not just its id: with a `signingKeyIds: [string]`
shape there would be nothing to verify against and `status.evidence.verification.result` could never
read `Valid`. The `keyId` is the sha256 of the SPKI DER, computed here with `openssl` so this script
and `logweir-verify` cannot disagree about it.

`==> 6/12 TrustRoster default — the approver key id and the signing key MATERIAL`

```
    rc=0  (kubectl apply -f trustroster.yaml — cluster-scoped, name 'default')
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): TrustRoster default carries the approver key id and the signing key material.
```

**Verdict: PASS — `TrustRoster/default` carries the approver key id AND the signing key material.**

## 7. The `KafkaCluster` at the published `K8S` listener

`host.docker.internal:9095` is the published `K8S` listener (Task 7, STANDING RULE 15). The runner is a
POD: the stack's host-side listener is the pod's own loopback from inside the cluster, and a broker
that advertised it would send every later connection there too.
`laptop_demo_uses_the_published_k8s_listener` forbids that address anywhere in the script.

The **second** object, `demo-target`, exists for step 10(b). The restore wizard picks its target with
`restore-wizard.js::firstTarget`, which walks the list for `spec.role == "target"` and returns `null`
otherwise — and a null target gives `renderPlanBytes` an empty `bootstrap_servers`, which the grammar
refuses, so the page renders an error box instead of six steps. In `newTopic` mode the restored topics
land on the source broker, so this is a second NAME for one cluster, which is what the page's role
model asks for.

`==> 7/12 KafkaCluster at host.docker.internal:9095 -> status.reachable`

```
    rc=0  (kubectl apply -f kafkacluster.yaml)
    rc=0  (kubectl wait --for=jsonpath={.status.reachable}=true kafkacluster/demo)
    rc=0  status.reachable: true
    rc=0  (kubectl apply -f kafkacluster-target.yaml — role: target, for the wizard at step 10(b))
    rc=0  (kubectl wait --for=jsonpath={.status.reachable}=true kafkacluster/demo-target)
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): the controller probed the broker with a Job and wrote status.reachable: true.
```

**Verdict: PASS — both `KafkaCluster` objects reached `status.reachable: true` off the controller's own probe Job.**

## 8. A `BackupSchedule`, and the `Backup` it fires

The records, the bucket and the recovery point come first: `just e2e-down` runs `down -v` and EMPTIES
the MinIO volume, so every owner of the stack makes its own bucket and sweeps its own prefix. Every
epoch instant here is COMPUTED, never typed.

Then the product's own path: a `BackupSchedule` on `*/2 * * * *`, and the `Backup` the schedule
reconciler creates for the due slot — the argv a scheduled `Backup` actually gets, not a hand-written
one.

The schedule is **suspended** once that `Backup` has exited 0 and verified. `suspend` is the ONE
mutable field of `BackupSchedule.spec` — the CRD seals every other field for EVERY subject,
cluster-admin included, with its own CEL rule — so the line is also the demonstration of that rule. It
is here because the walk needs a stable newest `Backup` from this point on: the wizard reads its backup
set off the newest one, and a schedule firing every two minutes replaces that object under the
operator's feet (measured: three `Backup` objects in six minutes, the page reading a different one on
each reload).

`==> 8/12 records, the bucket, a BackupSchedule on */2, and the Backup it fires`

```
    rc=0  (mc mb --ignore-existing local/kafka-backups)
    rc=0  (mc rm kafka-backups/laptop-demo/ — a non-zero here just means there was nothing to sweep)
    rc=0  (mc rm kafka-backups/logweir/ — same)
    rc=0  (kafka-topics --create laptopdemo)
    rc=0  (kafka-console-producer -> laptopdemo)
    rc=0  (kafka-get-offsets laptopdemo)
    laptopdemo holds 200 records
    recovery point: 2026-09-11T18:52:17Z   sample window from: 2026-09-11T17:57:12Z
    rc=0  (kubectl apply -f backupschedule.yaml, schedule */2 * * * *)
    waiting for the schedule to fire (up to two minutes plus the run)...
    rc=0  (kubectl get backups -o name)
    the schedule fired: Backup/logweir-backup-laptop-20260911-185200
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-185200 -o jsonpath={.status.phase})
    phase: Succeeded
    rc=0  status.exitCode: 0
    rc=0  status.evidence.receiptKey: logweir/backups/adb40303-8ddf-4af4-b4bd-93b348d8aaaf-20260911-185200/01M28X0AN2VYZW2FT7WK1NG1D5.receipt.json
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-185200 -o jsonpath={.status.evidence.verification.result})
    status.evidence.verification.result: Valid
    rc=0  (kubectl patch backupschedule laptop spec.suspend=true — the ONE mutable field; every other is sealed by the CRD's own CEL rule)
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): a scheduled Backup exited 0 and its signed receipt verified Valid.
```

**Verdict: PASS — a SCHEDULED `Backup` exited 0 and its DSSE receipt verified `Valid` against the roster.**

## 9. Serve the UI — and prove it is served

**Until this step nothing in Phase C proved that anything is served at `/ui/` at all.** Renaming `ui/`
and changing `--www-prefix` in all three documented places passes every gate Tasks 25–27 ship, because
those check AGREEMENT and not correctness. These four fetches are the correctness half: three static
paths and one API path, same origin, viewer credential, each status printed and each exit code read on
its own line.

The proxy is the one process this script backgrounds. Its pid is recorded, the port is polled in a
bounded foreground loop, and the teardown kills it.

`==> 9/12 kubectl proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1, and four fetches`

```
    rc=0  (kubectl proxy, backgrounded; pid 16194)

    The UI is at http://127.0.0.1:8001/ui/

    WHAT THIS COSTS, said plainly: kubectl proxy forwards every API path except pod
    exec and attach, on the SAME ORIGIN as the page, under YOUR kubeconfig. The page
    therefore runs with your ENTIRE CLUSTER AUTHORITY, not with the four ClusterRoles
    logweir.yaml ships -- those bind the user, and under this serving path they bind
    nothing about the page. Run this from a cluster-admin kubeconfig and you have
    given the page cluster-admin. No bearer token, key or credential of any kind is
    ever placed in the page.
    rc=0  (curl http://127.0.0.1:8001/ui/ — the readiness poll)
    rc=0  HTTP 200  the page itself
    rc=0  HTTP 200  the router
    rc=0  HTTP 200  the wizard the next step uses
    rc=0  HTTP 200  the API, same origin, viewer credential
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): the UI and the Kubernetes API are both served from http://127.0.0.1:8001.
```

**Verdict: PASS — four fetches, four `200`s: the page, the router, the wizard, and the Kubernetes API on the same origin under the viewer's own kubeconfig.**

## 10. Install gate **X-UIWRITE**, both halves

Spec §10: *under `kubectl proxy --www=`, a `create` of a `Restore` **from the page** returns 201.*

**`curl` is not the page**, so this step is recorded in two separately headed halves. The first draft
of this task recorded only the scripted half — which left a section that was present, non-empty, had a
verdict and contained `201`, satisfying every assertion a naive test could state, while the gate spec
§10 actually asks for had never been run. The string that distinguishes them is one only the page can
produce: `ui/api.js`'s `create` appends `?fieldManager=logweir-ui` (interface register I23), so the
created object's `metadata.managedFields` names `logweir-ui` — and not `kubectl`, and not the unstable
browser-derived User-Agent an absent `?fieldManager=` would have left.

### (a) the scripted create

`==> 10/12 (a) X-UIWRITE, scripted: the PAGE's own code builds the body, curl posts it`

```
    rc=0  (node ui/tests/emit-restore-body.js --out .demo/laptop/)
plan-hash=sha256:61d3af301ded7e1b523aa299e67c45cefcf52f61aa69c398f960684ca051401e
restore-name=restore-61d3af30
approval-name=approval-61d3af30
    rc=0  HTTP 201  (POST http://127.0.0.1:8001/apis/logweir.dev/v1alpha1/namespaces/logweir-t28/restores?fieldManager=logweir-cli)
    X-UIWRITE (a): 201 Created — Restore/restore-61d3af30, approvalRef -> approval-61d3af30 (which does not exist yet)
    rc=0  (managedFields managers on the scripted Restore): logweir-cli unknown
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): X-UIWRITE half (a): a Restore created through kubectl proxy's API, 201.
```

The body is built by the **page's own code**: `ui/tests/emit-restore-body.js` imports `renderPlanBytes`,
`planHash` and `mintNames` from `../plan.js` and `restoreBody` from `../pages/restore-wizard.js`, and
contains no YAML of its own — `laptop_demo_lint.rs::the_body_emitter_renders_the_plan_with_the_pages_own_renderer`
asserts both. A hand-written document here would be created, hashed and approved correctly and would
fail only at step 12, which is the failure Task 27's guard exists to catch three slots earlier.

Its `fieldManager` is `logweir-cli`, which is exactly why this half does not satisfy spec §10 on its
own.

And this is what the non-interactive pass printed where half (b) goes — the command, the string it will
produce, and the fact that it was skipped:

`==> 10/12 (b) X-UIWRITE, IN THE BROWSER: the create from the wizard's final step`

```
    rc=0  (kubectl get backups -o name)
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-185200 -o jsonpath={.status.evidence.receiptKey})
    backup id from the archive: adb40303-8ddf-4af4-b4bd-93b348d8aaaf-20260911-185200
    rc=0  (kubectl patch backup logweir-backup-laptop-20260911-185200 --subresource=status: status.backupId=adb40303-8ddf-4af4-b4bd-93b348d8aaaf-20260911-185200 — the field the controller owes, see the comment above)
    Spec §10: "under kubectl proxy --www=, a create of a Restore FROM THE PAGE
    returns 201". curl is not the page. Do this by hand:

      1. open http://127.0.0.1:8001/ui/#/restore
      2. walk the wizard's six steps (namespace logweir-t28, cluster demo, the archive
         s3://kafka-backups/laptop-demo, a point in time, a target prefix) -- change ANYTHING
         from the scripted run so the plan bytes differ and the minted names do
         too; the six steps end on the rendered bytes, their sha256 and the two
         names
      3. press "Create the Restore"

    NOTHING TYPES A PRIVATE KEY INTO THAT PAGE. The create needs no key; the
    approval is minted on this host at step 11 and only approval.json and
    approval.sig -- two public documents -- are ever pasted into the browser.

    Then the evidence, which only the page can produce. ui/api.js's create
    appends ?fieldManager=logweir-ui (interface register I23), so:

      kubectl --context docker-desktop -n logweir-t28 get restore <name> -o jsonpath='{.metadata.managedFields}'

    names "manager": "logweir-ui" and not "kubectl", and not the unstable
    browser-derived User-Agent an absent ?fieldManager= would have left.

    SKIPPED in this pass (LOGWEIR_DEMO_NONINTERACTIVE=1). Half (b) is performed in a
    second, interactive pass with the proxy still up:
        LOGWEIR_DEMO_ONLY_STEP=10b ./scripts/laptop-demo.sh
```

### (b) the in-browser create

Performed by hand, in a real browser (Chromium, driven over the Playwright MCP server), against the
proxy pass 1 left up.

1. `http://127.0.0.1:8001/ui/#/restore?ns=logweir-t28` — the six steps render.
2. Step 1: the endpoint `http://host.docker.internal:9000`, the region `us-east-1`, `path_style`
   addressing on, and the evidence bucket set to `kafka-backups`, each typed and committed.
3. Step 4: the target cluster `demo-target`, mode `newTopic`, and `topicNaming.prefix` changed to
   `from-the-page-` — the one value that makes this a **second, different plan** from the scripted
   half's.
4. Step 6 then showed the whole rendered document, its sha256
   `sha256:bcaa48fa8f54e2b4d34371adcca304ab543ef3cbdcab045208a4bb581d5ab522`, and the two names minted
   from exactly those bytes: `restore-bcaa48fa` and `approval-bcaa48fa`. The scripted half's pair was
   `restore-61d3af30`/`approval-61d3af30`, so the two creates are demonstrably different documents.
5. **Create the Restore** — one click, no key, no token, no credential of any kind in the page.

*(No screenshot is checked in. The picture is not the evidence; the `managedFields` output below is,
and it is quoted whole.)*

`==> 10/12 (b) X-UIWRITE, IN THE BROWSER: the create from the wizard's final step`

```
    rc=0  (kubectl get backups -o name)
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-185200 -o jsonpath={.status.evidence.receiptKey})
    backup id from the archive: adb40303-8ddf-4af4-b4bd-93b348d8aaaf-20260911-185200
    rc=0  (kubectl patch backup logweir-backup-laptop-20260911-185200 --subresource=status: status.backupId=adb40303-8ddf-4af4-b4bd-93b348d8aaaf-20260911-185200 — the field the controller owes, see the comment above)
    Spec §10: "under kubectl proxy --www=, a create of a Restore FROM THE PAGE
    returns 201". curl is not the page. Do this by hand:

      1. open http://127.0.0.1:8001/ui/#/restore
      2. walk the wizard's six steps (namespace logweir-t28, cluster demo, the archive
         s3://kafka-backups/laptop-demo, a point in time, a target prefix) -- change ANYTHING
         from the scripted run so the plan bytes differ and the minted names do
         too; the six steps end on the rendered bytes, their sha256 and the two
         names
      3. press "Create the Restore"

    NOTHING TYPES A PRIVATE KEY INTO THAT PAGE. The create needs no key; the
    approval is minted on this host at step 11 and only approval.json and
    approval.sig -- two public documents -- are ever pasted into the browser.

    Then the evidence, which only the page can produce. ui/api.js's create
    appends ?fieldManager=logweir-ui (interface register I23), so:

      kubectl --context docker-desktop -n logweir-t28 get restore <name> -o jsonpath='{.metadata.managedFields}'

    names "manager": "logweir-ui" and not "kubectl", and not the unstable
    browser-derived User-Agent an absent ?fieldManager= would have left.

    PAUSE: create the Restore in the browser now, then continue.
    press RETURN to continue: 
    rc=0  (kubectl get restores -o name)
restore.logweir.dev/restore-61d3af30
restore.logweir.dev/restore-bcaa48fa
    the name the page created:     rc=0  (kubectl get restore restore-bcaa48fa -o jsonpath={.metadata.managedFields})
[{"apiVersion":"logweir.dev/v1alpha1","fieldsType":"FieldsV1","fieldsV1":{"f:spec":{".":{},"f:approvalRef":{".":{},"f:name":{}},"f:backupSetRef":{},"f:deadlineSeconds":{},"f:planBytes":{},"f:pointInTime":{},"f:sourceArchive":{".":{},"f:secretRef":{".":{},"f:name":{}},"f:url":{}},"f:target":{".":{},"f:clusterRef":{".":{},"f:name":{}},"f:mode":{},"f:topicNaming":{".":{},"f:prefix":{}}}}},"manager":"logweir-ui","operation":"Update","time":"2026-09-11T18:54:56Z"},{"apiVersion":"logweir.dev/v1alpha1","fieldsType":"FieldsV1","fieldsV1":{"f:status":{".":{},"f:conditions":{},"f:phase":{},"f:reason":{}}},"manager":"unknown","operation":"Update","subresource":"status","time":"2026-09-11T18:54:56Z"}]
    rc=0  (python3 -m json.tool, the same bytes as the reader sees them)
[
    {
        "apiVersion": "logweir.dev/v1alpha1",
        "fieldsType": "FieldsV1",
        "fieldsV1": {
            "f:spec": {
                ".": {},
                "f:approvalRef": {
                    ".": {},
                    "f:name": {}
                },
                "f:backupSetRef": {},
                "f:deadlineSeconds": {},
                "f:planBytes": {},
                "f:pointInTime": {},
                "f:sourceArchive": {
                    ".": {},
                    "f:secretRef": {
                        ".": {},
                        "f:name": {}
                    },
                    "f:url": {}
                },
                "f:target": {
                    ".": {},
                    "f:clusterRef": {
                        ".": {},
                        "f:name": {}
                    },
                    "f:mode": {},
                    "f:topicNaming": {
                        ".": {},
                        "f:prefix": {}
                    }
                }
            }
        },
        "manager": "logweir-ui",
        "operation": "Update",
        "time": "2026-09-11T18:54:56Z"
    },
    {
        "apiVersion": "logweir.dev/v1alpha1",
        "fieldsType": "FieldsV1",
        "fieldsV1": {
            "f:status": {
                ".": {},
                "f:conditions": {},
                "f:phase": {},
                "f:reason": {}
            }
        },
        "manager": "unknown",
        "operation": "Update",
        "subresource": "status",
        "time": "2026-09-11T18:54:56Z"
    }
]

    X-UIWRITE (b): the manager is logweir-ui — THE WRITE CAME FROM THE PAGE.
rc=0
```

`"manager": "logweir-ui"`, on the entry that owns every `f:spec` field of the object. **The write came
from the page.** The second entry, `"manager": "unknown"`, owns `f:status` only — that is the
controller writing the first `Pending`/`ApprovalNotVerified` status, not a second writer of the spec.

That `Restore` is deliberately left un-approved: the gate is the create, and an approval for it would
be a second out-of-band ceremony proving nothing step 11 does not already prove.

**Verdict: PASS — install gate **X-UIWRITE**, both halves: `201` from the scripted create, and `"manager": "logweir-ui"` from the one the page performed.**

## 11. The approval, minted out of band, on the host

**The approver's private key never leaves this machine and never enters a browser.** `--subject-kind
Restore` is the fifth approval check; the sidecar path is DERIVED from `--out` (`approve.rs`'s
`sig_path_for`: the extension replaced), so `approval.json` gives `approval.sig`.

The CLI's `plan_hash` and the page's `plan-hash` are compared: they are the same bytes, or they are two
documents with one name. Then the runner's copy of the approval bundle replaces step 5's placeholder —
before the `Approval` object exists, and therefore before any runner Job can mount it — and the
`Approval` carries `approvalBytes` and `sidecarBytes` as the two files VERBATIM, as UTF-8 text, never
base64.

`==> 11/12 logweir drill approve on the HOST, then the Approval object`

```
    rc=0  (logweir drill approve --subject-kind Restore --out .demo/laptop/approval.json)
approved .demo/laptop/plan.yaml
  plan_hash  sha256:61d3af301ded7e1b523aa299e67c45cefcf52f61aa69c398f960684ca051401e
  approver   laptop
  ticket     DEMO-1
  subject    Restore
  key_id     ad9836ead774e6d720894fc34d3a66bb417a7334da589d1f76957ec978d9176c
  wrote      .demo/laptop/approval.json
  wrote      .demo/laptop/approval.sig

This approval binds the EXACT bytes of .demo/laptop/plan.yaml. Edit the spec — including its
sample window — and `logweir drill run` refuses with exit 3 until you re-run
this command.
    plan_hash from the CLI : sha256:61d3af301ded7e1b523aa299e67c45cefcf52f61aa69c398f960684ca051401e
    plan-hash from the page: sha256:61d3af301ded7e1b523aa299e67c45cefcf52f61aa69c398f960684ca051401e
    rc=0  (kubectl delete secret logweir-approval-bundle — the placeholder)
    rc=0  (secret/logweir-approval-bundle, the real four keys)
    rc=0  (kubectl create -f approval-object.yaml — Approval/approval-61d3af30 over Restore/restore-61d3af30)
    rc=0  (kubectl wait --for=jsonpath={.status.verified}=true approval/approval-61d3af30)
    rc=0  status.verified: true
    rc=0  status.matchedKeyId: ad9836ead774e6d720894fc34d3a66bb417a7334da589d1f76957ec978d9176c
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): the approval was minted on this host and verified in the cluster (key ad9836ead774e6d720894fc34d3a66bb417a7334da589d1f76957ec978d9176c).
```

**Verdict: PASS — the approval was minted on the HOST, its `plan_hash` equals the hash the page showed, and the cluster verified it against the roster.**

## 12. The scorecard, both readers, and the teardown

Every field with `-o jsonpath`, one per line, and then **both readers** over the scorecard the runner
signed and the controller verified: `logweir drill verify --payload-type scorecard` and `python3
docs/verify_scorecard.py --payload-type scorecard`, which shares no code with Logweir. Both exit codes
are read directly. The two documents are fetched out of the evidence bucket with `mc`, inside the
compose network, because that is where the object store is.

`==> 12/12 the Restore's terminal status, then BOTH readers over the scorecard`

```
    rc=0  (kubectl get restore restore-61d3af30 -o jsonpath={.status.phase})
    phase: Succeeded
    rc=0  status.exitCode: 0
    rc=0  status.outcome: pass
    rc=0  status.integrity.level: byte-fingerprint
    rc=0  status.measured.rtoSeconds: 1
    rc=0  status.measured.rpoSeconds: 9
    rc=0  status.newTopics: ["drill-laptopdemo"]
    rc=0  status.evidence.scorecardKey: logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.json
    rc=0  status.evidence.sidecarKey: logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.sig
    rc=0  (kubectl get restore restore-61d3af30 -o jsonpath={.status.evidence.verification.result})
    rc=0  (status.evidence.verification): {"matchedKeyId":"2190b61d2de7a3158a8790cd2dd15f2421db6d9f7b42a2c9a79affb2dd797f79","payloadType":"application/vnd.logweir.drill-scorecard+json;version=1.0.0","result":"Valid","verifiedAt":"2026-09-11T18:52:59Z"}
    rc=0  (mc cat kafka-backups/logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.json)
    rc=0  (mc cat kafka-backups/logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.sig)
    rc=0  (logweir drill verify --payload-type scorecard)
signature: VALID  key 2190b61d2de7a3158a8790cd2dd15f2421db6d9f7b42a2c9a79affb2dd797f79
run_id:    01M28X1JRC4ET9QRY76VGS7YPG
outcome:   pass
approval:  laptop (DEMO-1)
offsets:   logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.offsets.json
           sha256:d42a1c817c3ef8f089478fa466343235251b13259b23514d4319d684e8980f3c — the engine's offset MAPPING, uploaded as evidence and applied to nothing
    rc=0  (python3 docs/verify_scorecard.py --payload-type scorecard)
VALID  run_id=01M28X1JRC4ET9QRY76VGS7YPG  outcome=pass
       rto_seconds=1  rpo_seconds=9
       integrity=byte-fingerprint/pass
       evidence: the four post-put fields are zeroed before signing; the storage facts live in the receipt
       offsets:  logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.offsets.json
                 sha256:d42a1c817c3ef8f089478fa466343235251b13259b23514d4319d684e8980f3c — the engine's offset MAPPING, uploaded as evidence and applied to nothing
       verifier: verify_scorecard.py 1.13.0 (invariant set: evidence-zeroing, trimmed-empty partial_reason, redactions, outcome-entailment, all eleven required blocks in serde order, the six required non-block fields present and of the type their Rust type implies, u64 domain with null refused where Rust has no Option, target.auth's mode present, not blank, and one of the two values the format defines when the block is; evidence.offset_report_key and its sha256 present or absent together; target.marker_topic present unless target.mode is newTopic; target.mode absent or one of the two values the format defines; approval.self_attested derived, not echoed)
```

The summary the walk prints for itself:

```
==> PHASE C EXIT CRITERION MET
    Backup  logweir-backup-laptop-20260911-185200:  exitCode=0  verification=Valid
                           receiptKey=logweir/backups/adb40303-8ddf-4af4-b4bd-93b348d8aaaf-20260911-185200/01M28X0AN2VYZW2FT7WK1NG1D5.receipt.json
    Restore restore-61d3af30: phase=Succeeded  exitCode=0  outcome=pass
                           integrity=byte-fingerprint  rtoSeconds=1  rpoSeconds=9
                           newTopics=["drill-laptopdemo"]
                           scorecardKey=logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.json
                           sidecarKey=logweir/drills/01M28X1JRC4ET9QRY76VGS7YPG.sig
                           verification=Valid
    Both readers agreed, each exit code read directly: logweir drill verify -> 0,
    python3 docs/verify_scorecard.py -> 0.
    X-UIWRITE (a): 201 from the create through kubectl proxy.
    X-UIWRITE (b): the in-browser create, recorded in the second pass.
    NO PRIVATE KEY WAS EVER TYPED INTO THE BROWSER.
```

### The teardown

`LOGWEIR_DEMO_ONLY_STEP=teardown ./scripts/laptop-demo.sh` — the same function the default run's `trap`
calls, run on its own here because pass 1 deferred it for pass 2's sake.

`==> 12/12 teardown`

```
    asked any kubectl proxy serving ./ui to stop (this process started none)
    rc=0  (kubectl delete -f logweir.yaml)
    rc=0  (kubectl delete ns logweir-system logweir-t28)
    rc=0  (mc rm kafka-backups/laptop-demo/ — a non-zero here just means there was nothing to sweep)
    rc=0  (mc rm kafka-backups/logweir/ — same)
    rc=0  (docker rmi the two author-only tags)
    rc=0  (just e2e-down)
    removed .demo/laptop/*.pem (both keypairs this run minted)
```

**Verdict: PASS — `exitCode: 0`, `outcome: pass`, a signed scorecard that BOTH readers accept, and a teardown that leaves nothing behind.**

## The default form, run again, end to end

Everything above was recorded with `LOGWEIR_DEMO_KEEP=1`, which defers the teardown so the second pass
has a proxy to talk to. The DEFAULT is a full teardown in a `trap`, and this is that run — the exact
acceptance command, on a clean cluster, with its own fresh keys, its own archive and its own cleanup:

```
just e2e-up
LOGWEIR_DEMO_NONINTERACTIVE=1 just laptop-demo; echo "rc=$?"
```

The twelve banners it printed, in order:

```
==> 1/12 preflight: context, tools, the compose stack, both local images
==> 2/12 namespace logweir-t28
==> 3/12 just apply-install (install gate X-APPLY), then the author-only demo overlay
==> 4/12 minting the signing and approver keypairs into .demo/laptop/
==> 5/12 the five Secrets, then just check-secrets logweir-t28
==> 6/12 TrustRoster default — the approver key id and the signing key MATERIAL
==> 7/12 KafkaCluster at host.docker.internal:9095 -> status.reachable
==> 8/12 records, the bucket, a BackupSchedule on */2, and the Backup it fires
==> 9/12 kubectl proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1, and four fetches
==> 10/12 (a) X-UIWRITE, scripted: the PAGE's own code builds the body, curl posts it
==> 10/12 (b) X-UIWRITE, IN THE BROWSER: the create from the wizard's final step
==> 11/12 logweir drill approve on the HOST, then the Approval object
==> 12/12 the Restore's terminal status, then BOTH readers over the scorecard
==> 12/12 teardown
```

...and its own summary, its teardown and its exit code:

```
==> PHASE C EXIT CRITERION MET
    Backup  logweir-backup-laptop-20260911-185600:  exitCode=0  verification=Valid
                           receiptKey=logweir/backups/9e175188-720d-454e-8528-8c739222f547-20260911-185600/01M28X993CDS9R1RGSARV3KTCS.receipt.json
    Restore restore-b6043bb3: phase=Succeeded  exitCode=0  outcome=pass
                           integrity=byte-fingerprint  rtoSeconds=1  rpoSeconds=9
                           newTopics=["drill-laptopdemo"]
                           scorecardKey=logweir/drills/01M28XAGYN42CPX3RQJ3QD42DJ.json
                           sidecarKey=logweir/drills/01M28XAGYN42CPX3RQJ3QD42DJ.sig
                           verification=Valid
    Both readers agreed, each exit code read directly: logweir drill verify -> 0,
    python3 docs/verify_scorecard.py -> 0.
    X-UIWRITE (a): 201 from the create through kubectl proxy.
    X-UIWRITE (b): the in-browser create, recorded in the second pass.
    NO PRIVATE KEY WAS EVER TYPED INTO THE BROWSER.

==> 12/12 teardown
    stopped the kubectl proxy (pid 18033)
    rc=0  (kubectl delete -f logweir.yaml)
    rc=0  (kubectl delete ns logweir-system logweir-t28)
    rc=0  (mc rm kafka-backups/laptop-demo/ — a non-zero here just means there was nothing to sweep)
    rc=0  (mc rm kafka-backups/logweir/ — same)
    rc=0  (docker rmi the two author-only tags)
    rc=0  (just e2e-down)
    removed .demo/laptop/*.pem (both keypairs this run minted)
rc=0
```


## What this transcript does not prove

- **It is not an installation from a registry.** Two steps are author-only (the `docker tag` pair and
  the demo overlay), the images were built on this machine, and Global Constraint 37 is unchanged: the
  install file's digest rows still read `blocked: no remote` until `release.yml` has pushed and a
  pull-back has been recorded.
- **It is not a claim about the control plane's authority.** `kubectl proxy` forwards every API path
  except pod exec and attach, on the same origin as the page, under the viewer's own kubeconfig — so
  the page runs with the viewer's entire cluster authority, not with the four ClusterRoles
  `logweir.yaml` ships. Step 9 prints that in full before the first fetch.
- **It is not an MSK result.** Nothing here touches MSK, and no acceptance criterion depends on it.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software Foundation. Logweir is not
affiliated with or endorsed by the ASF.
