# Phase C's exit criterion — `just laptop-demo`, run and recorded, with X-UIWRITE from the page

> **2026-09-12 — THE REGISTRY NAMESPACE MOVED AFTER THIS TRANSCRIPT WAS RECORDED, AND NOTHING
> BELOW IS EDITED.** A record is not a config file. The image references in the steps below are
> the ones that actually ran on the day; the SHIPPED references are now
> `docker.io/vladyslavhaina/logweir` (runner) and `docker.io/vladyslavhaina/weirkeeper`
> (controller), on Docker Hub. Why they moved, and what a node has to hold before either resolves:
> `../docs/kubernetes.md` §14.


The walk spec §1 calls **the whole acceptance surface**: a stranger clones, applies one file to
docker-desktop Kubernetes and gets CRDs, RBAC and the controller; the UI is static files served by
`kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1`; against a
plain broker from the compose stack they create a `BackupSchedule`, watch a `Backup` produce a signed
receipt, run a drill (a `Restore` with a `newTopic` target) and read a signed scorecard with measured
RTO and RPO — **never typing a private key into a browser**.

Twelve numbered steps, one section each below, each with the transcript it produced and a verdict.

**Re-recorded for Task 28a, with the two workarounds gone.** The first recording of this walk had to
work around two defects it had itself discovered, and said so in the open. Task 28a fixed both, and
this is the walk without them:

| defect | the workaround this recording no longer needs |
|---|---|
| `Backup.status.backupId` was declared on the CRD and **nothing wrote it**, so the restore wizard read `undefined` into `fields.backupSetRef`, the plan grammar refused the document, and the page was an error box before step 1 rendered | step 10(b) used to `kubectl patch --subresource=status` the field in by hand. **Closed by Task 28a**: `controllers/backup.rs::finished_status_patch` writes it, and step 10(b) now READS it back off the object and checks it against the archive's own prefix. |
| the wizard listed only `KafkaCluster`s with `spec.role == "target"` — a requirement the runner does not have, since `drill/phase0_admit.rs`'s `TargetMode::NewTopic` arm is empty and the CRD says `role` is a label rather than an authorisation — so the one-cluster namespace this walk builds rendered an empty target select | step 7 used to apply a **second** `KafkaCluster`, `demo-target`, at the same address. **Closed by Task 28a**: step 4 lists every cluster with its role beside it and lets the runner's guard decide, so step 7 applies ONE object again. |

A third finding was **not** a defect of the page and is not worked around: a `BackupSchedule` on
`*/2 * * * *` completes a newer `Backup` while an operator reads the wizard. Task 28a made the page
say which run it chose — the newest `Succeeded` one, by its `Complete` condition — and print
`a running schedule may complete a newer backup while you read this; reload to pick it up, or suspend
the schedule first.` Step 8 still suspends the schedule, because a walkthrough wants a fixed target;
the page now tells an operator who does not.

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
(`logweir:check` = `sha256:6440a4a0…`, `weirkeeper:check` = `sha256:e6e3384e…`). **The runner image was
not rebuilt.** The CONTROLLER image was, once, because the `backupId` fix above lives in it: 240 s,
native `arm64`, and the new digest is pinned in `config/manager/deployment.yaml` and re-rendered into
`logweir.yaml`. A rebuild moves a repository digest (plan erratum E19a) and `docs/kubernetes.md` §14.7
records the move.

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
    rc=0  (docker image inspect weirkeeper:check) -> ["weirkeeper@sha256:e6e3384eaf37321366fe36ee3a5d23dc7eb21868859cf32737300194587488ae"]
    rc=0  (kubectl get crd -o name)
    logweir.dev CRDs already installed: 0
    rc=0  (docker tag logweir:check ghcr.io/logweir/logweir:v0.1.0 — author-only, removed by the teardown)
    rc=0  (docker tag weirkeeper:check ghcr.io/logweir/weirkeeper:v0.1.0 — author-only, removed by the teardown)
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): the preflight passed: docker-desktop, the compose stack, both local images.
```

**Verdict: PASS — the context is `docker-desktop`, the stack is up, both images are present at the pinned digests, the cluster holds no half-installed CRD set.**

## 2. The namespace, and the runner ServiceAccount

The runner Job's ServiceAccount is a namespaced object and is applied here rather than in
`logweir.yaml`, which is cluster-scoped plus `logweir-system`.

`==> 2/12 namespace logweir-t28`

```
    rc=0  (kubectl create namespace logweir-t28)
    rc=0  (kubectl apply -f config/rbac/backup-runner-serviceaccount.yaml)
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): namespace logweir-t28 and the runner ServiceAccount exist.
```

**Verdict: PASS — `logweir-t28` and the runner ServiceAccount exist.**

## 3. `just apply-install` — install gate **X-APPLY**

X-APPLY is `kubectl --context docker-desktop apply --server-side -f logweir.yaml`, **twice**, with
both exit codes read directly: a second server-side apply of the same file is the one that catches a
field-ownership conflict, and an install file that cannot be re-applied is an install file no adopter
can upgrade with.

The overlay patch after it is the author-only half: it points the controller at this laptop's compose
MinIO. The SHIPPED `logweir.yaml` is applied unedited.

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

`SigningKey::load_or_generate` (`crates/logweir-evidence/src/keys.rs`) loads a path that exists and
MINTS one that does not. A run against an empty `logweir-signing-key` Secret therefore produces a
green scorecard signed by a key no `TrustRoster` attests, and nothing says so at the time. The walk
prints the warning where an operator reads it.

`==> 4/12 minting the signing and approver keypairs into .demo/laptop/`

```
    signing key id:  889fd1c00c029029e0dd10c2ba090faa71178b3bed245117c7d2284d32d83288
    approver key id: 70f4f569167fe05fe31a733a4c7dbaa9cf1c5a0f19674d1e4f3c83ea09a8b0b7

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

Spec §9's five: `logweir-signing-key`, `logweir-approval-bundle` (its own Secret, spec §7 amendment
4c, never folded into the signing key's), the per-cluster SCRAM credential, `logweir-s3`, and
`logweir-evidence-ro` — the last in `logweir-system`, because it is the CONTROLLER's read-only
evidence credential and a different principal from the runner's.

The approval bundle is created here with a placeholder and REPLACED at step 11 with the real
approval, before the `Approval` object exists and therefore before any runner Job can mount it.

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

`weirkeeper::ROSTER_NAME` is `default` and the object is cluster-scoped: one roster for the cluster,
carrying the approver's key id in `spec.approverKeys[]` and the signing key's MATERIAL in
`spec.signingKeys[]` — `keyId`, `spkiPem`, `subject`, `notAfter`.

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

**ONE object, and Task 28a is why.** The first recording of this walk applied a second
`KafkaCluster` here — `demo-target`, `role: target`, the same address — because the restore wizard
listed only `role: target` clusters and rendered an error box without one. That requirement was the
page's invention: `crates/weirkeeper/src/crds/kafka_cluster.rs` documents `role` as a label the
adopter picks, with `TrustRoster.allowedClusterIds` as the thing that authorises a target, and
`crates/logweir/src/drill/phase0_admit.rs` puts every cluster check inside the `Scratch` arm — its
`TargetMode::NewTopic` arm is empty, with a comment saying the source cluster is exactly where a
point-in-time recovery belongs. `scripts/k8s-demo.sh` had been proving that by running, green, against
one `role: source` cluster all along. Task 28a made step 4 of the wizard list every cluster with its
role beside it and let the runner's guard decide, so this walk applies one object again.

`==> 7/12 KafkaCluster at host.docker.internal:9095 -> status.reachable`

```
    rc=0  (kubectl apply -f kafkacluster.yaml)
    rc=0  (kubectl wait --for=jsonpath={.status.reachable}=true kafkacluster/demo)
    rc=0  status.reachable: true
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): the controller probed the broker with a Job and wrote status.reachable: true.
```

**Verdict: PASS — the single `KafkaCluster` reached `status.reachable: true` off the controller's own probe Job.**

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
is here because the walk wants a fixed chosen `Backup` from this point on. Since Task 28a the wizard
restores from the newest SUCCEEDED run and NAMES it on screen, so an operator who does not suspend can
at least see the choice move; measured on the first recording, a two-minute schedule produced three
`Backup` objects in six minutes, and with the chosen object go the plan bytes, the plan hash and both
minted names.

`==> 8/12 records, the bucket, a BackupSchedule on */2, and the Backup it fires`

```
    rc=0  (mc mb --ignore-existing local/kafka-backups)
    rc=0  (mc rm kafka-backups/laptop-demo/ — a non-zero here just means there was nothing to sweep)
    rc=0  (mc rm kafka-backups/logweir/ — same)
    rc=0  (kafka-topics --create laptopdemo)
    rc=0  (kafka-console-producer -> laptopdemo)
    rc=0  (kafka-get-offsets laptopdemo)
    laptopdemo holds 200 records
    recovery point: 2026-09-11T20:20:12Z   sample window from: 2026-09-11T19:25:07Z
    rc=0  (kubectl apply -f backupschedule.yaml, schedule */2 * * * *)
    waiting for the schedule to fire (up to two minutes plus the run)...
    rc=0  (kubectl get backups -o name)
    the schedule fired: Backup/logweir-backup-laptop-20260911-202000
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-202000 -o jsonpath={.status.phase})
    phase: Succeeded
    rc=0  status.exitCode: 0
    rc=0  status.evidence.receiptKey: logweir/backups/6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000/01M29219NNNSH9JVTW4AC6M5QD.receipt.json
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-202000 -o jsonpath={.status.evidence.verification.result})
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
    rc=0  (kubectl proxy, backgrounded; pid 90397)

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
plan-hash=sha256:7423c51b462e9ad7fb66855bcded9bbb7ad8e934196920e70905fcead07e1b17
restore-name=restore-7423c51b
approval-name=approval-7423c51b
    rc=0  HTTP 201  (POST http://127.0.0.1:8001/apis/logweir.dev/v1alpha1/namespaces/logweir-t28/restores?fieldManager=logweir-cli)
    X-UIWRITE (a): 201 Created — Restore/restore-7423c51b, approvalRef -> approval-7423c51b (which does not exist yet)
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

And this is what the non-interactive pass printed where half (b) goes — the backup id **read off the
object the controller wrote it on**, the command, the string it will produce, and the fact that the
create itself was skipped:

`==> 10/12 (b) X-UIWRITE, IN THE BROWSER: the create from the wizard's final step`

```
    rc=0  (kubectl get backups -o name)
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-202000 -o jsonpath={.status.backupId})
    status.backupId, written by the controller: 6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-202000 -o jsonpath={.status.evidence.receiptKey})
    the id in the archive key: 6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000
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

The four lines at the top of that block are the `backupId` fix, checked on the cluster rather than argued about:
`status.backupId` is what the controller wrote on the terminal status patch, and the id in
`status.evidence.receiptKey` is the prefix the runner actually wrote the archive under
(`logweir/backups/<backup_id>/<run_id>.receipt.json`). The script `die`s if they differ, and it `die`s
if `status.backupId` is empty — which is what a controller image predating the fix would give it. The
first recording had a `kubectl patch --subresource=status` here instead.

### (b) the in-browser create

Performed by hand, in a real browser (Chromium, driven over the Playwright MCP server), against the
proxy pass 1 left up.

1. `http://127.0.0.1:8001/ui/#/restore?ns=logweir-t28` — **all six steps rendered, with no status
   patch of any kind having been applied to the `Backup`.** That is the first defect closed, observed
   rather than reasoned about: the page read `status.backupId` off the object because the controller
   had written it.
2. Step 2 showed one row, `Succeeded`, marked `(chosen)`, under the sentence
   `chosen: logweir-backup-laptop-20260911-202000, backup set
   6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000. a running schedule may complete a newer
   backup while you read this; reload to pick it up, or suspend the schedule first.`
3. Step 4 offered exactly one option — `demo (role: source)`, **selected** — above the sentence
   `no cluster is labelled role: target; the source cluster is preselected. The role is a label, not
   an authorisation: for mode newTopic the runner accepts any reachable target, the source cluster
   included; mode scratch is refused by the runner unless the target differs from the source and
   proves it is scratch with its marker topic.` That is the second defect closed: there is no
   `demo-target` object in this namespace and the wizard does not want one.
4. `topicNaming.prefix` was typed to a distinct value and blurred — the one change that makes this a
   **second, different plan** from the scripted half's. The plan re-rendered to
   `sha256:db9b280adaa8f5da529b749f4c9d0839b3f71f5d84d799450910b78b62a33baa`, minting
   `restore-db9b280a` and `approval-db9b280a`; the scripted half's pair was
   `restore-7423c51b`/`approval-7423c51b`, so the two creates are demonstrably different documents.
5. **Create the Restore** — one click, no key, no token, no credential of any kind in the page. The
   page's console carried **0 errors and 0 warnings**.

The object the page created names the source cluster as its target, which is the whole of defect 2 in
one line:

```
$ kubectl --context docker-desktop -n logweir-t28 get restore restore-db9b280a \
    -o jsonpath='{.spec.target.clusterRef.name}{" mode="}{.spec.target.mode}{" backupSetRef="}{.spec.backupSetRef}'
demo mode=newTopic backupSetRef=6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000
rc=0
```

*(No screenshot is checked in. The picture is not the evidence; the `managedFields` output below is,
and it is quoted whole.)*

`==> 10/12 (b) X-UIWRITE, IN THE BROWSER: the create from the wizard's final step`

```
    rc=0  (kubectl get backups -o name)
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-202000 -o jsonpath={.status.backupId})
    status.backupId, written by the controller: 6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000
    rc=0  (kubectl get backup logweir-backup-laptop-20260911-202000 -o jsonpath={.status.evidence.receiptKey})
    the id in the archive key: 6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000
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
    rc=0  (kubectl get restore restore-db9b280a -o jsonpath={.metadata.managedFields})
[{"apiVersion":"logweir.dev/v1alpha1","fieldsType":"FieldsV1","fieldsV1":{"f:spec":{".":{},"f:approvalRef":{".":{},"f:name":{}},"f:backupSetRef":{},"f:deadlineSeconds":{},"f:planBytes":{},"f:pointInTime":{},"f:sourceArchive":{".":{},"f:secretRef":{".":{},"f:name":{}},"f:url":{}},"f:target":{".":{},"f:clusterRef":{".":{},"f:name":{}},"f:mode":{},"f:topicNaming":{".":{},"f:prefix":{}}}}},"manager":"logweir-ui","operation":"Update","time":"2026-09-11T20:21:38Z"},{"apiVersion":"logweir.dev/v1alpha1","fieldsType":"FieldsV1","fieldsV1":{"f:status":{".":{},"f:conditions":{},"f:phase":{},"f:reason":{}}},"manager":"unknown","operation":"Update","subresource":"status","time":"2026-09-11T20:21:38Z"}]
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
        "time": "2026-09-11T20:21:38Z"
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
        "time": "2026-09-11T20:21:38Z"
    }
]

    X-UIWRITE (b): the manager is logweir-ui — THE WRITE CAME FROM THE PAGE.
```

`"manager": "logweir-ui"`, on the entry that owns every `f:spec` field of the object — `f:backupSetRef`
and `f:target.f:clusterRef.f:name` among them. **The write came from the page.** The second entry,
`"manager": "unknown"`, owns `f:status` only — that is the controller writing the first
`Pending`/`ApprovalNotVerified` status, not a second writer of the spec.

That `Restore` is deliberately left un-approved: the gate is the create, and an approval for it would
be a second out-of-band ceremony proving nothing step 11 does not already prove.

**Verdict: PASS — install gate **X-UIWRITE**, both halves: `201` from the scripted create, and `"manager": "logweir-ui"` from the one the page performed — this time with no status patch and one `KafkaCluster`.**

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
  plan_hash  sha256:7423c51b462e9ad7fb66855bcded9bbb7ad8e934196920e70905fcead07e1b17
  approver   laptop
  ticket     DEMO-1
  subject    Restore
  key_id     70f4f569167fe05fe31a733a4c7dbaa9cf1c5a0f19674d1e4f3c83ea09a8b0b7
  wrote      .demo/laptop/approval.json
  wrote      .demo/laptop/approval.sig

This approval binds the EXACT bytes of .demo/laptop/plan.yaml. Edit the spec — including its
sample window — and `logweir drill run` refuses with exit 3 until you re-run
this command.
    plan_hash from the CLI : sha256:7423c51b462e9ad7fb66855bcded9bbb7ad8e934196920e70905fcead07e1b17
    plan-hash from the page: sha256:7423c51b462e9ad7fb66855bcded9bbb7ad8e934196920e70905fcead07e1b17
    rc=0  (kubectl delete secret logweir-approval-bundle — the placeholder)
    rc=0  (secret/logweir-approval-bundle, the real four keys)
    rc=0  (kubectl create -f approval-object.yaml — Approval/approval-7423c51b over Restore/restore-7423c51b)
    rc=0  (kubectl wait --for=jsonpath={.status.verified}=true approval/approval-7423c51b)
    rc=0  status.verified: true
    rc=0  status.matchedKeyId: 70f4f569167fe05fe31a733a4c7dbaa9cf1c5a0f19674d1e4f3c83ea09a8b0b7
    PAUSE (suppressed by LOGWEIR_DEMO_NONINTERACTIVE=1): the approval was minted on this host and verified in the cluster (key 70f4f569167fe05fe31a733a4c7dbaa9cf1c5a0f19674d1e4f3c83ea09a8b0b7).
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
    rc=0  (kubectl get restore restore-7423c51b -o jsonpath={.status.phase})
    phase: Succeeded
    rc=0  status.exitCode: 0
    rc=0  status.outcome: pass
    rc=0  status.integrity.level: byte-fingerprint
    rc=0  status.measured.rtoSeconds: 1
    rc=0  status.measured.rpoSeconds: 9
    rc=0  status.newTopics: ["drill-laptopdemo"]
    rc=0  status.evidence.scorecardKey: logweir/drills/01M2922J53Z6CZ8M7406W9G218.json
    rc=0  status.evidence.sidecarKey: logweir/drills/01M2922J53Z6CZ8M7406W9G218.sig
    rc=0  (kubectl get restore restore-7423c51b -o jsonpath={.status.evidence.verification.result})
    rc=0  (status.evidence.verification): {"matchedKeyId":"889fd1c00c029029e0dd10c2ba090faa71178b3bed245117c7d2284d32d83288","payloadType":"application/vnd.logweir.drill-scorecard+json;version=1.0.0","result":"Valid","verifiedAt":"2026-09-11T20:20:54Z"}
    rc=0  (mc cat kafka-backups/logweir/drills/01M2922J53Z6CZ8M7406W9G218.json)
    rc=0  (mc cat kafka-backups/logweir/drills/01M2922J53Z6CZ8M7406W9G218.sig)
    rc=0  (logweir drill verify --payload-type scorecard)
signature: VALID  key 889fd1c00c029029e0dd10c2ba090faa71178b3bed245117c7d2284d32d83288
run_id:    01M2922J53Z6CZ8M7406W9G218
outcome:   pass
approval:  laptop (DEMO-1)
offsets:   logweir/drills/01M2922J53Z6CZ8M7406W9G218.offsets.json
           sha256:16649760c94a1ab94a436616f08e16dbebe73d5ebcba27acc6bb6db6dff13747 — the engine's offset MAPPING, uploaded as evidence and applied to nothing
    rc=0  (python3 docs/verify_scorecard.py --payload-type scorecard)
VALID  run_id=01M2922J53Z6CZ8M7406W9G218  outcome=pass
       rto_seconds=1  rpo_seconds=9
       integrity=byte-fingerprint/pass
       evidence: the four post-put fields are zeroed before signing; the storage facts live in the receipt
       offsets:  logweir/drills/01M2922J53Z6CZ8M7406W9G218.offsets.json
                 sha256:16649760c94a1ab94a436616f08e16dbebe73d5ebcba27acc6bb6db6dff13747 — the engine's offset MAPPING, uploaded as evidence and applied to nothing
       verifier: verify_scorecard.py 1.13.0 (invariant set: evidence-zeroing, trimmed-empty partial_reason, redactions, outcome-entailment, all eleven required blocks in serde order, the six required non-block fields present and of the type their Rust type implies, u64 domain with null refused where Rust has no Option, target.auth's mode present, not blank, and one of the two values the format defines when the block is; evidence.offset_report_key and its sha256 present or absent together; target.marker_topic present unless target.mode is newTopic; target.mode absent or one of the two values the format defines; approval.self_attested derived, not echoed)
```

The summary the walk prints for itself:

```
==> PHASE C EXIT CRITERION MET

    Backup  logweir-backup-laptop-20260911-202000:  exitCode=0  verification=Valid
                           receiptKey=logweir/backups/6ed3dbba-4d06-4f71-9d44-1ce14a269d59-20260911-202000/01M29219NNNSH9JVTW4AC6M5QD.receipt.json
    Restore restore-7423c51b: phase=Succeeded  exitCode=0  outcome=pass
                           integrity=byte-fingerprint  rtoSeconds=1  rpoSeconds=9
                           newTopics=["drill-laptopdemo"]
                           scorecardKey=logweir/drills/01M2922J53Z6CZ8M7406W9G218.json
                           sidecarKey=logweir/drills/01M2922J53Z6CZ8M7406W9G218.sig
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
    Backup  logweir-backup-laptop-20260911-202200:  exitCode=0  verification=Valid
                           receiptKey=logweir/backups/5eb64236-3bee-4092-bf56-59437a01a32c-20260911-202200/01M2927M3060NWCGW51C13Q07J.receipt.json
    Restore restore-a9ea9f9c: phase=Succeeded  exitCode=0  outcome=pass
                           integrity=byte-fingerprint  rtoSeconds=1  rpoSeconds=9
                           newTopics=["drill-laptopdemo"]
                           scorecardKey=logweir/drills/01M2928WBA72NZ660YB1MMRGKT.json
                           sidecarKey=logweir/drills/01M2928WBA72NZ660YB1MMRGKT.sig
                           verification=Valid
    Both readers agreed, each exit code read directly: logweir drill verify -> 0,
    python3 docs/verify_scorecard.py -> 0.
    X-UIWRITE (a): 201 from the create through kubectl proxy.
    X-UIWRITE (b): the in-browser create, recorded in the second pass.
    NO PRIVATE KEY WAS EVER TYPED INTO THE BROWSER.

==> 12/12 teardown
    stopped the kubectl proxy (pid 1207)
    rc=0  (kubectl delete -f logweir.yaml)
    rc=0  (kubectl delete ns logweir-system logweir-t28)
    rc=0  (mc rm kafka-backups/laptop-demo/ — a non-zero here just means there was nothing to sweep)
    rc=0  (mc rm kafka-backups/logweir/ — same)
    rc=0  (docker rmi the two author-only tags)
    rc=0  (just e2e-down)
    removed .demo/laptop/*.pem (both keypairs this run minted)
rc=0
```

`grep -E 'rc=[1-9]'` over that whole log finds **nothing**. Wall clock 104 s; pass 1 was 90 s.

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
- **It does not prove the wizard renders for every namespace shape.** It renders here, on a namespace
  with one `role: source` `KafkaCluster` and one `Succeeded` `Backup`, and the two defects that made
  it fail on exactly that shape are closed and guarded by rows in `ui/tests/pages.spec.js`. A
  namespace in which NO run has reached `Succeeded` still has no covered window to default a point in
  time from: step 2 says which row it fell back to and why, and there is no plan to render.
- **A locally pinned digest is a measurement, not a reproducible pin.** The controller image was
  rebuilt for this recording and its digest moved again — the fourth time in this plan (plan erratum
  E19a, `docs/kubernetes.md` §14.7).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software Foundation. Logweir is not
affiliated with or endorsed by the ASF.
