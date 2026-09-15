# Installing Logweir

Use this guide for image selection, cluster prerequisites, installation and
uninstall. See [kubernetes.md](kubernetes.md) for operation and troubleshooting,
and [the chart reference](../charts/logweir/README.md) for Helm values.
Commands use `docker-desktop`; substitute your intended context explicitly.

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

## Choose an image and installation path

### (a) Published digests

```bash
kubectl --context docker-desktop apply --server-side -f logweir.yaml
```

using the `@sha256:` references the file carries.

Release verification is still recorded as **`blocked: images not published`**
in [tag1-checklist.md](tag1-checklist.md). Treat this as the checkout's release
status, not a live registry check. A published installation requires pulling
both image digests on a host that did not build them.

| Image | Current reference |
|---|---|
| Controller | `config/manager/deployment.yaml`, rendered into `logweir.yaml` |
| Runner | `crates/weirkeeper/src/job.rs`, constant `RUNNER_IMAGE` |

Read pins from those files rather than copying historical build digests.
Local BuildKit provenance can change the digest even on a cached rebuild.
`kubectl apply` can succeed while a pod remains in `ImagePullBackOff`; verify
Deployment readiness separately.

### (b) Local build — **author-only**

```bash
just image && just image-weirkeeper
```

The runner is `linux/amd64`. The controller recipe defaults to `linux/arm64`;
on a native amd64 builder use `LOGWEIR_IMAGE_PLATFORM=linux/amd64 just image-weirkeeper`.
The controller cannot be cross-compiled by the shipped Dockerfile. Images are
loaded into the local Docker store; the cluster must be able to use that store.

Apply the controller overlay, then give the freshly built controller the local
runner image explicitly. The overlay cannot rewrite a Rust constant:

```bash
kubectl --context docker-desktop apply --server-side -k config/overlays/local-images
kubectl --context docker-desktop -n logweir-system set env deployment/weirkeeper \
  LOGWEIR_RUNNER_IMAGE=logweir:check LOGWEIR_RUNNER_PULL_POLICY=Never
```

The overlay sets `imagePullPolicy: Never`. On kind, load both local tags into
the nodes before using them. The amd64 runner requires compatible nodes.

Older controllers without the environment override need the exact compiled
runner digest present under its shipped repository name. The historical step
was:

```bash
docker tag logweir:check docker.io/vladyslavhaina/logweir:v0.1.0
docker inspect --format '{{json .RepoDigests}}' docker.io/vladyslavhaina/logweir:v0.1.0
```

Retagging cannot make newly built bytes match an older compiled digest. Prefer
a controller built from the checkout and the override above. See
[kubernetes.md](kubernetes.md) §14 for the recorded image-resolution findings.

A local build is **author-only** and **never satisfies spec §16 clause 1**,
including a local `registry:2` fallback: it does not prove public pullability.

### (c) The Helm chart

```bash
helm install logweir charts/logweir -n logweir-system --create-namespace
```

The [chart reference](../charts/logweir/README.md) documents all values and
optional components, including MinIO, demo brokers, existing Kafka clusters
and the UI. The chart's two Logweir images default to `latest`, with `Always`
pull policies; base manifests remain digest-pinned. For reproducibility,
override both images with published digests and set both pull policies
explicitly. `just chart-check` checks its rendered resources and copied files.

The release caveat in path (a) also applies to the chart. For local images use
`charts/logweir/examples/author-only.values.yaml`; recorded local and CI walks
are **author-only**, not evidence of publication.

The chart installs a runner ServiceAccount in its release namespace. Additional
runner namespaces still need their own account, Secrets and policy. The chart
creates no signing or approval keys; optional MinIO creates only demo archive
credentials. Helm installs CRDs once and does not upgrade them automatically.

### (d) Bring your own registry

Build both images and publish them to a registry your nodes can pull from.
Set `ACCOUNT` and `REGION` for this ECR example. The runner remains amd64;
choose compatible runner nodes and build the controller natively for its nodes.
For an amd64 controller, set `LOGWEIR_IMAGE_PLATFORM=linux/amd64` before its build.

```bash
aws ecr create-repository --repository-name logweir/weirkeeper   # once, per image
aws ecr create-repository --repository-name logweir/logweir
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
just image && just image-weirkeeper                              # on a builder of the cluster's arch
docker tag weirkeeper:check "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/weirkeeper:v0.1.0"
docker tag logweir:check    "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir:v0.1.0"
docker push "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/weirkeeper:v0.1.0"
docker push "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir:v0.1.0"
helm upgrade --install logweir charts/logweir -n logweir-system --create-namespace \
  --set controllerImage="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/weirkeeper:v0.1.0" \
  --set runnerImage="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir:v0.1.0" \
  --set imagePullPolicy=IfNotPresent \
  --set runnerImagePullPolicy=IfNotPresent \
  --set 'imagePullSecrets[0].name=my-regcred'
```

`imagePullSecrets` is rendered onto the controller ServiceAccount **and the
runner ServiceAccount**, so the runner Jobs the operator creates inherit it
without an operator change. On ECR with the node role already granted
`ecr:GetAuthorizationToken` you can leave it off entirely.

The release workflow targets Docker Hub. Ensure repositories are accessible
to the cluster, and supply a registry Secret through `imagePullSecrets` when
required. Repository visibility and pull quotas depend on the registry account.

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

**Everything in this section happens before workload custom resources are
created. On a fresh namespace the first preflight is expected to fail; repeat
it after provisioning Secrets and before applying the workload samples.** Missing Secrets prevent successful execution. The preflight checks presence;
it does not establish key provenance or validate a signed approval.

### 1. The two keypairs

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem && openssl pkey -in signing.pem -pubout -out signing.pub.pem
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out approver.pem && openssl pkey -in approver.pem -pubout -out approver.pub.pem
```

P-256, because that is what `logweir-evidence` mints and verifies. Keep both
private halves off the cluster except as the one Secret named below; the
approver's private key never goes on the cluster at all.

> **Signing-key prerequisite.** `SigningKey::load_or_generate`
> (`crates/logweir-evidence/src/keys.rs:81-92`) creates and saves a new key when
> the requested path is absent and writable. That key is not automatically
> trusted by a roster. An unreadable, malformed or unwritable path fails;
> a missing required Kubernetes Secret or key can prevent the pod from starting.
> Provision the expected `signing.pem` and run `just check-secrets` before jobs.

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
    - keyId: REPLACE-ME-sha256-of-approver-DER-SPKI
      subject: approver@example.invalid
      notAfter: "2027-01-01T00:00:00Z"
      spkiPem: |
        -----BEGIN PUBLIC KEY-----
        REPLACE-ME paste the contents of approver.pub.pem here
        -----END PUBLIC KEY-----
  signingKeys:
    - keyId: REPLACE-ME-sha256-of-signing-DER-SPKI
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
the SHA-256 of its DER SubjectPublicKeyInfo, not the PEM file bytes:

```bash
openssl pkey -pubin -in approver.pub.pem -outform DER | openssl dgst -sha256
openssl pkey -pubin -in signing.pub.pem -outform DER | openssl dgst -sha256
```

Use the lowercase hex digest as `keyId`. A declared id that does not match
its key produces `KeyIdNotInRoster`. Fill the sample before applying it;
`TrustRoster.spec` is immutable, so changing keys requires replacing the roster
and temporarily interrupts approval checks.

### 3. The five Secrets

There are **five**, not three. Four live in the namespace your `Backup`,
`Restore` and `KafkaCluster` objects live in; the fifth is the controller's and
lives in `logweir-system`. The names and the **data keys** below are the ones
the code reads — not approximations of them.

**1. `logweir-signing-key`** — the runner signs evidence with it. The data key
is `signing.pem`, **not** `key.pem`: `SIGNING_KEY_SECRET_KEY` is
`"signing.pem"` and the Job projects it to the *file* `key.pem` under
`/signing`. A Secret missing the required data key prevents the volume
projection from satisfying the Job.

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
`logweir.yaml` installs into `logweir-system` and runner objects may live
elsewhere. **Apply it once per namespace that will run jobs.** It is
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

Use the selected installation path above. The namespace is **not** created by hand:
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
`logweir-system`; runners in other namespaces need a policy there too. Apply it into each runner
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

The local UI serves `ui/` through a Kubernetes API proxy. The chart also
offers an optional in-cluster UI; see its reference for that account's authority.

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

The four cleanup scopes are the control plane, scratch topics, archive objects
and evidence objects. `kubectl delete -f logweir.yaml` removes only that first thing:
the Namespace and everything inside it, the six CRDs and all their custom
resources across namespaces, RBAC, Deployment and NetworkPolicy. Deleting those
resources can also collect their owned Jobs and ConfigMaps. Export records you
need before uninstalling.

Kafka topics and object-store data remain. Remove them separately only when
intended, using the actual names and prefixes from the evidence:

```bash
# scratch topics a `mode: scratch` restore created (phase 9 normally removes them)
kafka-topics.sh --bootstrap-server <broker> --delete --topic 'logweir-scratch-<name>'

# archive objects — Logweir NEVER deletes these; retention only reports
aws s3 rm 's3://kafka-backups/logweir/<backup_id>/' --recursive

# evidence objects — likewise, and the controller's credential is read-only
aws s3 rm 's3://logweir-evidence/<prefix>/' --recursive
```

Deleting the CRDs also deletes all their custom resources, including the
cluster-scoped `TrustRoster`. Export any records you need before uninstalling.
RoleBindings are namespaced; bindings outside the removed `logweir-system`
namespace survive and can be removed separately:

```bash
kubectl --context docker-desktop delete rolebinding logweir-viewer logweir-operator logweir-approver -n <namespace>
```

Runner ServiceAccounts in surviving namespaces also remain:

```bash
kubectl --context docker-desktop -n <namespace> delete serviceaccount logweir-runner
```

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
