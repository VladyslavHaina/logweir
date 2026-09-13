# Installing Logweir

This is the **single install document**. `README.md`, `docs/quickstart.md` and
`docs/kubernetes.md` all point here; nothing else in this tree carries install
steps, so there is one path to keep correct rather than two to keep in
agreement.

**Minimum Kubernetes: 1.29.** The six CRDs use CEL validation rules, which are
GA at 1.29. The `ValidatingAdmissionPolicy` example is 1.30+ and ships
commented out.

**Minimum `kafka-backup` engine: 0.21.0.** That is the floor for the drill as
shipped, and it is the version the pinned digest in
[../third_party/kafka-backup-binary.digest](../third_party/kafka-backup-binary.digest)
names. `v0.19.1` — `strimzi-backup-operator`'s hard-coded default — is *below*
the floor and is reported `unsupported (lever-absent)`, never as a fault.
[support-matrix.md](support-matrix.md) is the row-by-row version.

---

## Two supported paths, and the caveat that decides which one you are on

### (a) Published digests

```bash
kubectl --context docker-desktop apply --server-side -f logweir.yaml
```

using the `@sha256:` references the file carries.

**This path requires that those digests have been pulled back from `docker.io`
on a host that did not build them.** Until the release workflow has run, every
digest row in this document, in `logweir.yaml`'s own header comment and in
`docs/tag1-checklist.md` reads **`blocked: images not published`**, and this path is
**documented but not yet exercised**.

| image | reference lives in | status |
|---|---|---|
| `docker.io/vladyslavhaina/weirkeeper` (controller) | `config/manager/deployment.yaml`, rendered into `logweir.yaml` | `blocked: images not published` |
| `docker.io/vladyslavhaina/logweir` (runner) | `crates/weirkeeper/src/job.rs`, the constant `RUNNER_IMAGE` | `blocked: images not published` |

**No digest value is written into this document, on purpose.** A locally built
image's digest changes on **every build** — three builds of the same source
produced three digests, the third a cached no-op — so a digest copied into
prose is a measurement of one build that stops being true the next time
anybody runs `just image`. Read the two references out of the checkout, from
the two files named in the table above; those are the only places they live.

Applying this file on a cluster with no such image is not an error you have to
guess at: `kubectl apply` exits 0 without starting a pod, and the controller
Deployment's pod sits in `ImagePullBackOff`. That is the recorded, expected
result — [kubernetes.md](kubernetes.md) §13 carries the transcript.

### (b) Local build — **author-only**

```bash
just image && just image-weirkeeper
```

The runner image is `linux/amd64` (the engine binary is dynamically linked and
amd64-only) and the controller image `linux/arm64` — built natively, never
cross-compiled, and never emulated. Both are **loaded** into the local image
store, which is why the overlay below sets `imagePullPolicy: Never`.

Then apply through the checked-in
[../config/overlays/local-images](../config/overlays/local-images)
kustomization, which rewrites the controller image reference to the locally
loaded tag and sets `imagePullPolicy: Never`:

```bash
kubectl --context docker-desktop apply --server-side -k config/overlays/local-images
```

**One more author-only step, and it is not optional.** The kubelet keys images
on the **whole reference**, not on the digest alone: a matching digest under a
different repository name is `ErrImageNeverPull`. The runner image is a Rust
constant compiled into the controller, so kustomize cannot rewrite it — tag
the locally built image with the shipped name so the reference resolves:

```bash
docker tag logweir:check docker.io/vladyslavhaina/logweir:v0.1.0
docker inspect --format '{{json .RepoDigests}}' docker.io/vladyslavhaina/logweir:v0.1.0
```

That was measured, both ways, in [kubernetes.md](kubernetes.md) §14.

**This path is `author-only` everywhere it appears and never satisfies spec
§16 clause 1.** "Published" means a pull from a registry the author does not
control. A locally built or locally loaded image is not a published one — the
`registry:2` fallback included: it proves a pod starts from a repository
digest, and it proves nothing about publication. An install proven this way
proves the manifests are right and the binaries run, and says nothing at all
about whether a stranger can install Logweir.

### (c) The Helm chart

```bash
helm install logweir charts/logweir -n logweir-system --create-namespace
```

[`charts/logweir`](../charts/logweir/README.md) installs the same objects
`logweir.yaml` carries — the six CRDs (from `crds/`, once; Helm never upgrades
them), the RBAC, the controller Deployment, the NetworkPolicy — into the
release namespace, with `values.yaml` documenting every knob and three optional
components behind three flags: `minio.enabled`
(an in-cluster archive with the two buckets and the `logweir-s3` Secret),
`demoKafka.enabled` (two throwaway KRaft brokers, `orders` and `payments`
seeded on the source, the marker topic on the target) and `ui.enabled` (the
page served in-cluster by `kubectl proxy` with its **own** ServiceAccount's
authority — the chart's README says exactly whose). `scripts/check-chart.sh`
(`just chart-check`, in `just gate`) holds the chart's CRDs and UI files
byte-identical to this tree and its rendered control plane to `logweir.yaml`.

**The chart's two Logweir images are named by the `latest` TAG, and only in
this chart.** That is the owner's decision of 2026-09-12: `controllerImage` and
`runnerImage` default to the repository half of the tree's own two pins followed
by `:latest`, while `config/manager/deployment.yaml`, `logweir.yaml` and the
operator's compiled-in `weirkeeper::job::RUNNER_IMAGE` are untouched and still
pin **digests** (Global Constraint 7), as do the chart's four third-party images.
`charts/logweir/values.yaml`'s header carries the whole trade-off — what a
mutable tag gives up, and the `docker buildx imagetools inspect` recipe that
pins the two back to digests together with `--set imagePullPolicy=IfNotPresent
--set runnerImagePullPolicy=IfNotPresent`. Because the reference is a tag, the
chart's pull policies default to `Always` (Kubernetes' own default for
`:latest`), and `runnerImagePullPolicy` is a second value that reaches the runner
Jobs through the controller.

**The same caveat as (a) and (b) still decides which world you are in.** Nothing
has been pushed to `docker.io/vladyslavhaina/…` — `blocked: images not published` until
`release.yml` has run on a pushed tag, and it never has — so `:latest` resolves
in no registry and on a cluster with no access to that namespace the controller
pod sits in `ImagePullBackOff` exactly as path (a) records. The tag changed the
reference, not the fact: **the default path has never been exercised on any
cluster, and nothing here claims it has.**
`charts/logweir/examples/author-only.values.yaml` is path (b) as values —
`weirkeeper:check`, `logweir:check`, `imagePullPolicy: Never`,
`runnerImagePullPolicy: Never` — and it is **author-only** everywhere it
appears: images you built and loaded yourself, **never** evidence for spec §16
clause 1. The chart was walked end to end on the author's docker-desktop with
those images on 2026-09-12 ([kubernetes.md](kubernetes.md) §19), which proves
the chart's objects work together and proves nothing about publication.

The five Secrets, the two keypairs, the `TrustRoster` and the per-namespace
runner ServiceAccount below are the same on this path; the chart creates none
of them except, with `minio.enabled`, the demo-only `logweir-s3`.

---

## Before any custom resource

Run the two preflights. Both read a cluster or the checkout and change
neither.

```bash
./scripts/render-install.sh --check; echo "rc=$?"
just check-secrets logweir-system; echo "rc=$?"
```

`render-install.sh --check` re-renders `logweir.yaml` from `config/` and
`diff -u`s the checked-in file against it, so a hand edit to the one file a
stranger applies is a red exit rather than a surprise.

`just check-secrets <namespace>` exits **1 naming the first absent Secret**. It
is deliberately **not** part of `just apply-install`: the apply is specified
against a clean cluster with no Secrets at all, and folding the check in would
make the install refuse in exactly the state it is specified to succeed in.

**Everything in this section happens before the first custom resource is
created.** A `Backup` or a `Restore` created against a namespace missing its
Secrets does not fail cleanly — see the silent-mint warning below.

### 1. The two keypairs

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem && openssl pkey -in signing.pem -pubout -out signing.pub.pem
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out approver.pem && openssl pkey -in approver.pem -pubout -out approver.pub.pem
```

P-256, because that is what `logweir-evidence` mints and verifies. Keep both
private halves off the cluster except as the one Secret named below; the
approver's private key never goes on the cluster at all.

> **WARNING — an absent signing key is MINTED, silently.**
> `SigningKey::load_or_generate` returns a **new** key when the path is absent
> (`crates/logweir-evidence/src/keys.rs:81-92` — `if path.exists()` at `:82`,
> then `let key = Self::generate_p256();` at `:85`). So a first run against an
> empty or mis-keyed `logweir-signing-key` Secret does not fail. It
> **succeeds**, and it signs its scorecard and its receipt with a key that is
> in no `TrustRoster`, that nothing attests, and that disappears with the pod.
> The evidence looks green and verifies against nothing. This is why
> `just check-secrets <namespace>` exists and why it checks
> `logweir-signing-key` first.

### 2. The cluster-scoped `TrustRoster`, whose name is fixed

```bash
kubectl --context docker-desktop apply -f config/samples/trustroster.yaml
```

```yaml
apiVersion: logweir.dev/v1alpha1
kind: TrustRoster
metadata:
  # FIXED. `ROSTER_NAME` is the literal "default" and nothing reads any other.
  name: default
spec:
  allowedClusterIds: []
  approverKeys:
    - keyId: REPLACE-ME-sha256-of-approver.pub.pem
      subject: approver@example.invalid
      notAfter: "2027-01-01T00:00:00Z"
      spkiPem: |
        -----BEGIN PUBLIC KEY-----
        REPLACE-ME paste the contents of approver.pub.pem here
        -----END PUBLIC KEY-----
  signingKeys:
    - keyId: REPLACE-ME-sha256-of-signing.pub.pem
      subject: logweir-runner@example.invalid
      notAfter: "2027-01-01T00:00:00Z"
      spkiPem: |
        -----BEGIN PUBLIC KEY-----
        REPLACE-ME paste the contents of signing.pub.pem here
        -----END PUBLIC KEY-----
```

**The name is `default` and nothing reads any other name.**
`weirkeeper::controllers::approval::ROSTER_NAME` is the literal `"default"` and
`load_roster` does `Api::<TrustRoster>::all(client).get(ROSTER_NAME)`. A roster
called anything else is stored, reconciled and listed — and consulted by no
approval check, so every `Approval` reports `RosterNotFound`.

`TrustRoster` is **cluster-scoped**, so this is a cluster-admin step. The UI
surfaces this snippet and does **not** submit it. Fill in `spkiPem` from
`approver.pub.pem` and `signing.pub.pem`, and each `keyId` from
`shasum -a 256 <that file>`; a declared id that does not match its own key is
`KeyIdNotInRoster`, naming both ids.

### 3. The five Secrets

There are **five**, not three. Four live in the namespace your `Backup`,
`Restore` and `KafkaCluster` objects live in; the fifth is the controller's and
lives in `logweir-system`. The names and the **data keys** below are the ones
the code reads — not approximations of them.

**1. `logweir-signing-key`** — the runner signs evidence with it. The data key
is `signing.pem`, **not** `key.pem`: `SIGNING_KEY_SECRET_KEY` is
`"signing.pem"` and the Job projects it to the *file* `key.pem` under
`/signing`. A Secret keyed `key.pem` mounts a directory without the file the
runner was told to read.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-signing-key --from-file=signing.pem=signing.pem
```

**2. `logweir-approval-bundle`** — its **own** Secret, and not part of
`logweir-signing-key`: the signed approval, its detached sidecar, the
approver's **public** key, and the cluster allowlist. No private key reaches a
runner pod on this path.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-approval-bundle \
  --from-file=approval.json=approval.json \
  --from-file=approval.sig=approval.sig \
  --from-file=approver.pub.pem=approver.pub.pem \
  --from-file=allowed-clusters.json=allowed-clusters.json
```

**3. The per-cluster SCRAM credential** — its *name* is yours, whatever
`KafkaCluster.spec.auth.secretRef` says; its *data key* is fixed at `password`
(`TARGET_PASSWORD_SECRET_KEY`). The username is `spec.auth.username`, in the
object, not in the Secret.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  kafka-scram --from-literal=password="$KAFKA_PASSWORD"
```

**4. `logweir-s3`** — the archive credential the runner needs.
`object_store`'s own credential chain, not the AWS SDK's: `~/.aws`,
`AWS_PROFILE` and SSO are unsupported.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-s3 \
  --from-literal=access-key-id="$AWS_ACCESS_KEY_ID" \
  --from-literal=secret-access-key="$AWS_SECRET_ACCESS_KEY"
```

**5. `logweir-evidence-ro`** — the controller's **read-only** evidence-bucket
credential, and the only one of the five the controller's own pod consumes. A
**different principal** from the runner's archive credential, and read-only: no
Logweir component holds any object-store delete capability. Created in
`logweir-system`.

```bash
kubectl --context docker-desktop -n logweir-system create secret generic \
  logweir-evidence-ro \
  --from-literal=access-key-id="$EVIDENCE_RO_ACCESS_KEY_ID" \
  --from-literal=secret-access-key="$EVIDENCE_RO_SECRET_ACCESS_KEY"
```

[../config/samples/secrets.yaml](../config/samples/secrets.yaml) carries the
same five, beside these commands.

### 4. The runner ServiceAccount, in each namespace that runs jobs

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f config/rbac/backup-runner-serviceaccount.yaml
```

Runner Jobs run in the namespace of the `Backup`, `Restore` or `KafkaCluster`
object that produced them, and every one of them names the ServiceAccount
`logweir-runner`. That account is **not** in `logweir.yaml`, because
`logweir.yaml` installs into `logweir-system` and no runner ever runs there —
creating it in that one namespace would be creating it in the only namespace
where it is useless. **Apply it once per namespace that will run jobs.** It is
granted no verb on anything, it sets `automountServiceAccountToken: false`, and
so does every runner PodSpec: `logweir backup run` makes zero Kubernetes API
calls, and it is the process that holds the signing key.

A pod whose PodSpec names no ServiceAccount silently gets `default` — the one
account an operator is most likely to have granted something to. That is why
the name is set explicitly and why this step is not optional.

### 5. Binding the three human roles

`logweir.yaml` ships `logweir-viewer`, `logweir-operator` and
`logweir-approver` **unbound**. Who may approve a restore in which namespace is
your decision, not the install file's.

```bash
kubectl --context docker-desktop create rolebinding logweir-viewer \
  --clusterrole=logweir-viewer --user=<user> -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-operator \
  --clusterrole=logweir-operator --user=<user> -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-approver \
  --clusterrole=logweir-approver --user=<someone-else> -n <namespace>
```

`logweir-approver` is `create` on `approvals` and **nothing else**. Bind it to
somebody who is not the operator: `self_attested: false` means only "two
different keys", and one person holding both keypairs satisfies it.

---

## The install itself

Path (a) or path (b) above. The namespace is **not** created by hand:
`config/manager/namespace.yaml` is the first resource in
`config/kustomization.yaml`, so `logweir-system` arrives with the install.

`logweir.yaml` is the checked-in `kubectl kustomize` output of `config/`,
regenerated by `just install-yaml` and never edited by hand. It contains the
Namespace, the six CustomResourceDefinitions, the RBAC, the controller
Deployment and the runner NetworkPolicy, and **no custom resource** — so the
CRD-not-yet-established ordering failure cannot happen.

## The samples, applied second

Only now, after the preflight above has exited 0:

```bash
kubectl --context docker-desktop -n <namespace> apply -f config/samples/kafkacluster.yaml
kubectl --context docker-desktop -n <namespace> apply -f config/samples/backupschedule.yaml
kubectl --context docker-desktop -n <namespace> apply -f config/samples/restore.yaml
```

The NetworkPolicy is namespaced and `logweir.yaml` installs it into
`logweir-system`, where no runner ever runs. Apply it into each runner
namespace too — and read its `[UNVERIFIED]` mark in
[kubernetes.md](kubernetes.md) before relying on it. The source manifest
carries `namespace: logweir-system` (that is how it lands in `logweir.yaml`),
and `kubectl apply -n <namespace>` refuses a file whose own namespace
disagrees — so drop that one line on the way in:

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f <(sed '/^  namespace: logweir-system$/d' config/manager/networkpolicy.yaml)
```

---

## Serving the UI

The UI is a directory of static files — `ui/` in this repository — and a
Kubernetes API client. Nothing is installed onto the cluster: tag 1 ships no
server-side UI component, no image, no sidecar and no HTTP surface of its own.

```bash
kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1
```

Then open `http://127.0.0.1:8001/ui/`.

`kubectl proxy` forwards every API path except pod exec and attach, on the same
origin as the page, under the viewer's kubeconfig. So the page runs with the
**viewer's entire cluster authority**, not with the roles `logweir.yaml` ships:
`logweir-viewer`, `logweir-operator` and `logweir-approver` bind the **user**,
and under this serving path they bind nothing at all about the page. Anyone who
runs the UI from a cluster-admin kubeconfig gives the shipped bundle
cluster-admin. That residual is why there is no telemetry in this bundle, why
nothing in it is fetched from anywhere else, and why its contents are listed by
digest in the release notes.

**Two flags are the one-line escalation of exactly that residual, and neither
may change:** `--address=127.0.0.1` binds loopback only, and `--disable-filter`
must **never** be passed — the default keeps the cross-site request filter on.
**Changing either turns a local page holding your cluster authority into a network service holding it.**

**The hardened alternative is a kubeconfig that holds less**: bind a subject to
`logweir-viewer` (and `logweir-operator` if the page should write) and nothing
else, build a throwaway kubeconfig carrying that subject, and serve from it —
the context in it is named `docker-desktop` on purpose, so the command above is
unchanged. [kubernetes.md](kubernetes.md) §16, *Serving the UI*, carries that
section in full, with the exact `kubectl config` lines, and is the authority
for it; this document does not restate it.

---

## Two clients, two trust stores

Logweir speaks to Kafka through **two different TLS stacks**, and a private-CA
adopter must configure **both**:

- **The engine** (`kafka-backup`, invoked by the runner) falls back to its
  **bundled `webpki-roots`** unless `ssl_ca_location` is set. A private CA that
  is installed on the node is **not** enough — the engine never looks there.
- **Logweir's own rdkafka path** (the verification consumer) uses the
  **image's `ca-certificates`** bundle, at the OpenSSL default location.

Configuring one and not the other produces a working backup and a failing
verification, or the reverse, and neither failure names the trust store as the
cause. Set `ssl_ca_location` for the engine **and** ensure the CA is in the
image's bundle.

---

## Uninstall, and what it leaves behind

```bash
kubectl --context docker-desktop delete -f logweir.yaml
```

removes the control plane — the Namespace, the six CRDs and every custom
resource stored under them, the RBAC, the Deployment and the NetworkPolicy —
and **deletes nothing else**. `kubectl delete -f logweir.yaml` removes only
that first thing. Three more survive it, by design, and each is removed with
its own command:

```bash
# scratch topics a `mode: scratch` restore created (phase 9 normally removes them)
kafka-topics.sh --bootstrap-server <broker> --delete --topic 'logweir-scratch-<name>'

# archive objects — Logweir NEVER deletes these; retention only reports
aws s3 rm 's3://kafka-backups/logweir/<backup_id>/' --recursive

# evidence objects — likewise, and the controller's credential is read-only
aws s3 rm 's3://logweir-evidence/<prefix>/' --recursive
```

Two cluster-scoped objects are **not** in `logweir.yaml` and survive the
delete: the `TrustRoster` and any RoleBindings you created above.

```bash
kubectl --context docker-desktop delete trustroster default
kubectl --context docker-desktop delete rolebinding logweir-viewer logweir-operator logweir-approver -n <namespace>
```

The per-namespace runner ServiceAccount survives too, one per namespace you
applied it into:

```bash
kubectl --context docker-desktop -n <namespace> delete serviceaccount logweir-runner
```

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
