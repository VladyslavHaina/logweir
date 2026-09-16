# Installing Logweir

Use this guide for image selection, cluster prerequisites, installation and
uninstall. See [kubernetes.md](kubernetes.md) for operation and troubleshooting,
and [the chart reference](../charts/logweir/README.md) for Helm values.
Commands use `docker-desktop`; substitute your intended context explicitly.

**Minimum Kubernetes: 1.29.** The fourteen CRDs use CEL validation rules, which
are GA at 1.29. The `ValidatingAdmissionPolicy` example is 1.30+ and ships
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

Check the [CI and release runs](https://github.com/VladyslavHaina/logweir/actions)
for published digests. Main publishes all three images after tests and registry
verification; version releases also run a drill with the packaged binary.
The checked-in base-manifest pins are not automatically updated by publication.
Use the desired run's digests when deploying a new version.

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

`identity.bootstrapImage` is pinned in `charts/logweir/values.yaml` to a
reviewed runner digest that contains the identity CLI, so the clean default
command needs neither a local key nor an image hash:

```bash
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m
```

That pinned runner, like every runner image, is amd64-only. On an arm64 node
without amd64 emulation the bootstrap hook fails with `exec format error` and
writes no identity; schedule the hooks on amd64-capable nodes with
`kubernetes.nodeSelector` (runner Jobs need such nodes anyway).

Development on Docker Desktop with locally built images uses the explicit
local-image exception; it is not a user installation or publication claim:

```bash
just image
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m \
  -f charts/logweir/examples/author-only.values.yaml
```

Managed identity installation is a **connected Helm operation**. `helm install`,
`helm upgrade`, and `helm rollback` must be able to perform the chart's live
`lookup` calls for the retained Secret, public ConfigMap, authorized-namespace
copies, singleton owner, and Kubernetes API endpoints. Applying output from
offline `helm template`, a disconnected GitOps renderer, or any renderer whose
credentials cannot perform those lookups is unsupported with
`identity.enabled=true`: the output cannot establish retained-resource or
one-installation-per-cluster invariants and can contain fresh empty
placeholders. Do not apply such output. A GitOps system must invoke connected
Helm with lookup-capable credentials, or use the existing low-level/manual
identity lifecycle with `identity.enabled=false` and provision/adopt the signer
before workloads. The chart's `identity.externalSecret` adoption mode is still
a connected managed-identity install; it is not an offline exception.

The [chart reference](../charts/logweir/README.md) documents all values and
optional components, including MinIO, demo brokers, existing Kafka clusters
and the UI. The controller, runner and optional UI images default to `latest`,
with `Always` pull policies for all three (`ui.imagePullPolicy` controls the UI). Base manifests remain digest-pinned. For reproducibility,
override enabled images with published digests and set controller and runner
pull policies explicitly. `just chart-check` checks rendered resources and
copied CRDs. The UI ships in `logweir-ui`, built by `Dockerfile.ui`, rather
than a chart ConfigMap; `just smoke-ui` compares its assets with `ui/`.

A running pod does not restart when a tag changes; roll out the deployment or upgrade its image reference. For local images use
`charts/logweir/examples/author-only.values.yaml`; recorded local and CI walks
are **author-only**, not evidence of publication.

The chart installs the runner prerequisites in its release namespace and every
existing namespace explicitly listed in
`identity.authorizedRunnerNamespaces`. A short-lived distributor copies the
same established identity to each authorized namespace; it never generates a
namespace-local signer. On every install, upgrade and supported rollback, a
short-lived hook initializes or validates the retained
installation signing identity described below; Helm never renders private key
bytes. The chart does not create an approver key. Optional MinIO creates only
demo archive credentials. Helm installs CRDs once and does not upgrade them
automatically.

With that digest pinned, this Helm command is the supported clean-install
path for PLAT-02.1. The
digest-pinned `logweir.yaml` path remains a low-level/base-manifest path and
does not run Helm hooks; when using it, provision an existing
`logweir-signing-key` explicitly before creating workloads.

### (d) Bring your own registry

Build the controller and runner images, plus `logweir-ui` when enabling the
UI, and publish them to a registry your nodes can pull from.
Set `ACCOUNT` and `REGION` for this ECR example. The runner remains amd64;
choose compatible runner nodes and build the controller natively for its nodes.
For an amd64 controller, set `LOGWEIR_IMAGE_PLATFORM=linux/amd64` before its build.

```bash
aws ecr create-repository --repository-name logweir/weirkeeper   # once, per image
aws ecr create-repository --repository-name logweir/logweir
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
just image && just image-weirkeeper                              # on a builder of the cluster's arch
# Include these UI steps only when enabling ui.enabled:
aws ecr create-repository --repository-name logweir/logweir-ui
just image-ui
docker tag weirkeeper:check "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/weirkeeper:v0.1.0"
docker tag logweir:check    "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir:v0.1.0"
docker push "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/weirkeeper:v0.1.0"
docker push "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir:v0.1.0"
BOOTSTRAP_DIGEST=$(docker buildx imagetools inspect \
  "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir:v0.1.0" \
  --format '{{json .Manifest}}' | jq -er .digest)
docker run --rm \
  "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir@$BOOTSTRAP_DIGEST" \
  identity bootstrap --help
docker tag logweir-ui:check "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir-ui:v0.1.0"
docker push "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir-ui:v0.1.0"
helm upgrade --install logweir charts/logweir -n logweir-system --create-namespace \
  --set controllerImage="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/weirkeeper:v0.1.0" \
  --set runnerImage="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir:v0.1.0" \
  --set-string identity.bootstrapImage="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir@$BOOTSTRAP_DIGEST" \
  --set identity.bootstrapImagePullPolicy=IfNotPresent \
  --set ui.image="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir/logweir-ui:v0.1.0" \
  --set imagePullPolicy=IfNotPresent \
  --set runnerImagePullPolicy=IfNotPresent \
  --set 'imagePullSecrets[0].name=my-regcred'
```

`Dockerfile.ui` compiles nothing, so the UI image can be built for another
architecture with `LOGWEIR_IMAGE_PLATFORM=linux/amd64 just image-ui`.
The UI image includes its pinned kubectl base; the cluster pulls `logweir-ui`
and does not need a separate kubectl image.

`imagePullSecrets` is rendered onto the controller ServiceAccount **and the
runner ServiceAccount**, so the runner Jobs the operator creates inherit it
without an operator change. On ECR with the node role already granted
`ecr:GetAuthorizationToken` you can leave it off entirely.

The release workflow targets Docker Hub. Ensure repositories are accessible
to the cluster, and supply a registry Secret through `imagePullSecrets` when
required. Repository visibility and pull quotas depend on the registry account.

### Release coordinator: re-pin bootstrap bytes

This is a release integration step, not end-user ceremony. The image
publication path runs `scripts/check-image.sh` against the pulled candidate
digest, including `identity bootstrap --help`, before promotion. Whenever the
identity CLI or the chart's hook arguments change, copy the exact
`runner_digest` emitted by the images job into `identity.bootstrapImage` in
`charts/logweir/values.yaml` as `docker.io/<namespace>/logweir@sha256:<digest>`,
leave `allowMutableBootstrapImageForDevelopment: false`, and rerun
`just chart-check`, which requires the tree's runner repository by digest and
refuses an emptied value. Never pin a runner older than the identity CLI: it
has no `identity` subcommand. Then pull the newly pinned exact reference and
run:

```bash
docker run --rm --platform linux/amd64 docker.io/<namespace>/logweir@sha256:<reviewed-digest> \
  identity bootstrap --help
```

End users supply neither a key nor an image hash.

---

## Images and restricted registries

With both demo flags disabled, the chart uses only Logweir’s controller,
runner and optional UI images. The optional MinIO and demo Kafka components
pull upstream MinIO, mc and Apache Kafka images pinned by digest. Logweir does
not mirror these images into its own namespace.

For a cluster without public registry access, mirror the enabled images into
your registry and configure their references. The demo images use
`minio.image`, `minio.mcImage` and `demoKafka.image`; the three Logweir image
values are shown above. Keep the upstream licenses and notices with any
redistributed images.

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
created.** On the supported Helm path, wait for identity bootstrap before
running `just check-secrets`; the signing Secret is then present automatically.
Other missing credentials still prevent successful execution. The preflight
checks presence; it does not establish key provenance or validate an approval.

### 1. Installation signer and approver identity

The supported Helm install generates a P-256 installation signer inside a
short-lived bootstrap Job. No local OpenSSL command and no local private-key
file is required. The Job atomically initializes the retained Secret
`logweir-signing-key` and publishes only the key id, algorithm and SPKI public
key in retained ConfigMap `logweir-signing-trust`:

```bash
kubectl --context docker-desktop -n logweir-system get configmap logweir-signing-trust \
  -o jsonpath='{.data.key-id}{"\n"}{.data.algorithm}{"\n"}{.data.trust-reference}{"\n"}'
```

Helm normally deletes the successful hook Job, so `NotFound` after a successful
`helm ... --wait` is expected. Do not inspect the signing Secret to obtain the
public half; that needlessly requests private material.

Only the bootstrap/distributor **processes and their projected API tokens** are
short-lived. Their ServiceAccounts, resource-name-scoped Roles, and
RoleBindings are ordinary persistent release resources so a later install,
upgrade, rollback, or hook retry can run without expanding authority. Service
Accounts default to `automountServiceAccountToken:false`; only the hook Pods
opt in. Helm does not automatically revoke these RBAC grants after success.

To adopt an organization-managed P-256 or Ed25519 PKCS#8 PEM key, create its
source Secret before the first install and configure it explicitly:

```bash
kubectl --context docker-desktop create namespace logweir-system \
  --dry-run=client -o yaml | kubectl --context docker-desktop apply -f -
kubectl --context docker-desktop -n logweir-system create secret generic \
  company-logweir-signer --from-file=identity.pem=/secure/path/identity.pem
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m \
  --set identity.externalSecret.name=company-logweir-signer \
  --set identity.externalSecret.key=identity.pem
```

The source is get-only to bootstrap; only the fixed managed Secret and public
ConfigMap are patchable. If the source is absent, unreadable or malformed, the
install fails and does not fall back to generation. If an identity already
exists, external configuration must resolve to the same public key or bootstrap
refuses it as an attempted rotation. Keep the external source reachable on
subsequent upgrades while those values remain configured, or clear both
external values after verifying the published key id.

The approver identity remains independent and operator-managed. Generate it in
your approved key system; this OpenSSL example is only for the approver, whose
private half never enters the cluster:

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out approver.pem && openssl pkey -in approver.pem -pubout -out approver.pub.pem
```

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
    - keyId: REPLACE-ME-from-logweir-signing-trust
      subject: logweir-runner@example.invalid
      notAfter: "2027-01-01T00:00:00Z"
      spkiPem: |
        -----BEGIN PUBLIC KEY-----
        REPLACE-ME copy signing.pub.pem from logweir-signing-trust
        -----END PUBLIC KEY-----
```

**The name is `default` and nothing reads any other name.**
`weirkeeper::controllers::approval::ROSTER_NAME` is the literal `"default"` and
`load_roster` does `Api::<TrustRoster>::all(client).get(ROSTER_NAME)`. A roster
called anything else is stored, reconciled and listed — and consulted by no
approval check, so every `Approval` reports `RosterNotFound`.

`TrustRoster` is **cluster-scoped**, so this is a cluster-admin step. The UI
surfaces this snippet and does **not** submit it. Copy the installation signer's
`key-id` and `signing.pub.pem` from the public ConfigMap; do not derive them by
reading the Secret. Fill the approver fields from `approver.pub.pem`, with its
id calculated over DER SubjectPublicKeyInfo rather than PEM bytes:

```bash
kubectl --context docker-desktop -n logweir-system get configmap logweir-signing-trust \
  -o jsonpath='{.data.key-id}{"\n"}'
kubectl --context docker-desktop -n logweir-system get configmap logweir-signing-trust \
  -o jsonpath='{.data.signing\.pub\.pem}' > signing.pub.pem
openssl pkey -pubin -in approver.pub.pem -outform DER | openssl dgst -sha256
```

Use the lowercase hex digest as `keyId`. A declared id that does not match
its key produces `KeyIdNotInRoster`. Fill the sample before applying it;
`TrustRoster.spec` is immutable, so changing keys requires replacing the roster
and temporarily interrupts approval checks.

### 3. The four current Secrets

There are four for current controller workflows. Three live in the namespace your `Backup`,
`Restore` and `KafkaCluster` objects live in; the fourth is the controller's and
lives in `logweir-system`. The names and the **data keys** below are the ones
the code reads — not approximations of them.

**1. `logweir-signing-key`** — the Helm bootstrap creates this automatically in
the release namespace. The runner projects its `signing.pem` data key to
`/signing/key.pem`. Do not create or replace it on the managed path. Declare
every additional runner namespace up front; it must already exist:

```bash
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m \
  --set-string 'identity.authorizedRunnerNamespaces[0]=recoveries'
```

The chart creates `logweir-runner`, the retained empty target Secret, scoped
distribution RBAC and both NetworkPolicies there. A short-lived hook may read
only the primary Secret and get/patch only that fixed target Secret. It copies
the exact established bytes. If the namespace is absent, Helm names it as a
missing prerequisite; if it already holds a different signer, the hook fails
closed with restore guidance. Never mint a per-namespace signer. If the UI
should operate there, also list the namespace under `ui.namespaces`; UI access
and signing authorization remain separate explicit grants.

**2. The per-cluster SCRAM credential** — its *name* is yours, whatever
`KafkaCluster.spec.auth.secretRef` says; its *data key* is fixed at `password`
(`TARGET_PASSWORD_SECRET_KEY`). The username is `spec.auth.username`, in the
object, not in the Secret.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  kafka-scram --from-literal=password="$KAFKA_PASSWORD"
```

**3. `logweir-s3`** — the archive credential the runner needs.
`object_store`'s own credential chain, not the AWS SDK's: `~/.aws`,
`AWS_PROFILE` and SSO are unsupported.

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-s3 \
  --from-literal=access-key-id="$AWS_ACCESS_KEY_ID" \
  --from-literal=secret-access-key="$AWS_SECRET_ACCESS_KEY"
```

**4. `logweir-evidence-ro`** — the controller's **read-only** evidence-bucket
credential, and the only one of the four the controller's own pod consumes. A
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
same four, beside these commands.

New controller-managed Restores need no namespace-wide approval Secret.
Weirkeeper creates an immutable `<restore>-approval-bundle` ConfigMap from the
verified Approval and TrustRoster. Keep the legacy
`logweir-approval-bundle` only for a Job already created by an older controller
or a standalone manifest that explicitly mounts it. Do not delete it until
those Jobs finish; then remove it after confirming no pod template still
references the name.

### 4. Runner namespace prerequisites: Helm-managed or low-level manifest

On the supported Helm path, `identity.authorizedRunnerNamespaces` manages the
runner ServiceAccount, protected signer distribution and NetworkPolicies; do
not apply the following low-level files over those Helm-owned objects.

Only when using `logweir.yaml` without Helm hooks, apply the runner account
manually and provision the already-established signer through your protected
secret-distribution system:

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f config/rbac/backup-runner-serviceaccount.yaml
```

Runner Jobs run in the namespace of the `Backup`, `Restore` or `KafkaCluster`
object that produced them, and every one of them names the ServiceAccount
`logweir-runner`. That account is **not** in `logweir.yaml`, because
`logweir.yaml` installs into `logweir-system` and runner objects may live
elsewhere. **For the low-level path, apply it once per namespace that will run
jobs.** It is
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

## Upgrade CRDs before upgrading the controller

Helm installs `crds/` only on first install; it neither upgrades nor rolls CRDs
back. Apply the new additive schemas, wait for all fourteen Logweir definitions
to be established, and only then upgrade the release:

```bash
kubectl --context docker-desktop apply --server-side -f charts/logweir/crds/
for crd in approvals backupdestinations backups backupschedules kafkaclusters \
           preflights protectionpolicies recoverycatalogs rehearsalschedules \
           restores retentionpolicies topicdiscoveries trustpolicies trustrosters; do
  kubectl --context docker-desktop wait --for=condition=Established \
    "crd/${crd}.logweir.dev" --timeout=60s
done
helm upgrade logweir charts/logweir -n logweir-system --wait --timeout 10m
```

Stop before Helm if any apply/wait fails. **Every change in this release is
additive**: eight new kinds — `BackupDestination`, `TopicDiscovery`,
`Preflight`, `TrustPolicy`, `ProtectionPolicy`, `RehearsalSchedule`,
`RecoveryCatalog` and `RetentionPolicy` — plus optional fields and optional
status blocks on `Backup`, `BackupSchedule` and `Restore`, and one additive
enum value on `Approval`. **No kind is removed**: `TrustRoster` is deprecated in
its description and still served, and with no `TrustPolicy` present the
controller synthesises `legacy-roster-v1` from it, so nothing has to be migrated.
No existing object is converted, nothing is rewritten, and an object that names
none of the new fields resolves exactly as it did. The one field that stopped
being required — `Restore.spec.approvalRef` — is still required in effect: CEL
demands exactly one of it and `spec.authorization`, so an unauthorised `Restore`
remains unrepresentable. An older controller reading a
newer object ignores the fields it does not declare; the one visible consequence
is that a destination-backed `Backup` is refused terminally with
`ArchiveUrlUnreadable` before any Job is created, which is the intended
fail-closed behaviour after a rollback. **Apply the CRDs before rolling the
controller**, never the other way round.

Schema-specific field verification,
controller rollout checks, and the safe additive rollback boundary are in
[kubernetes.md, “Upgrade, rollback and legacy Jobs”](kubernetes.md#upgrade-rollback-and-legacy-jobs).

---

## The install itself

Use the selected installation path above. The namespace is **not** created by hand:
`config/manager/namespace.yaml` is the first resource in
`config/kustomization.yaml`, so `logweir-system` arrives with the install.

`logweir.yaml` is the checked-in `kubectl kustomize` output of `config/`,
regenerated by `just install-yaml` and never edited by hand. It contains the
Namespace, the fourteen CustomResourceDefinitions, the RBAC, the controller
Deployment and the runner NetworkPolicy, and **no custom resource** — so the
CRD-not-yet-established ordering failure cannot happen.

## The samples, applied second

Only now, after the preflight above has exited 0:

```bash
kubectl --context docker-desktop -n <namespace> apply -f config/samples/kafkacluster.yaml
kubectl --context docker-desktop -n <namespace> apply -f config/samples/backupschedule.yaml
kubectl --context docker-desktop -n <namespace> apply -f config/samples/restore.yaml
```

The Helm chart renders both the ordinary runner policy and the narrower
bootstrap Kubernetes-API policy into every authorized runner namespace. On the
low-level `logweir.yaml` path, its one NetworkPolicy lands only in
`logweir-system`; apply that source policy into each other runner namespace —
and read its `[UNVERIFIED]` mark in [kubernetes.md](kubernetes.md) before
relying on it. The source manifest
carries `namespace: logweir-system` (that is how it lands in `logweir.yaml`),
and `kubectl apply -n <namespace>` refuses a file whose own namespace
disagrees — so drop that one line on the way in:

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f <(sed '/^  namespace: logweir-system$/d' config/manager/networkpolicy.yaml)
```

NetworkPolicy enforcement and Service DNAT ordering are CNI-specific. Connected
Helm discovers `kubernetes.default`'s ClusterIP and visible endpoint IPs. For a
managed/external control plane, supply every actual API destination as an exact
IPv4 `/32` or IPv6 `/128` in `identity.kubernetesApiCIDRs`; the schema rejects
broad CIDRs. The bootstrap policy permits those destinations only on TCP 443
and 6443. Other API ports are unsupported. The default
`kube-system/component=kube-apiserver` selector generally cannot reach a
provider-hosted control plane, and offline rendering discovers no Service or
Endpoint addresses. These are structural restrictions, not proof that Docker
Desktop enforces them. Validate DNS/API allow and arbitrary-443 deny with the
production NetworkPolicy-enforcing CNI.

---

## Serving the UI

The local UI serves `ui/` through a Kubernetes API proxy. The chart also
offers an optional in-cluster UI served from the `logweir-ui` image, built
from the same files. Its proxy uses a ServiceAccount; see the
[chart reference](../charts/logweir/README.md#the-uis-authority) for its authority.

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

### Back up and recover the installation identity

Back up both retained objects before the first real backup and after any
authorized trust change. The first command writes private material directly to
a mode-0600 file; it never prints it to the terminal. Encrypt and move that file
to your organization's secret backup system, then remove the local copy.

```bash
install -d -m 0700 identity-backup
umask 077
kubectl --context docker-desktop -n logweir-system get secret logweir-signing-key \
  -o yaml > identity-backup/logweir-signing-key.yaml
kubectl --context docker-desktop -n logweir-system get configmap logweir-signing-trust \
  -o yaml > identity-backup/logweir-signing-trust.yaml
```

Recovery is restore-first: stop workloads that could start a signing Job,
restore the original Secret and public ConfigMap into the same namespace, and
only then run Helm. Remove stale `resourceVersion`, `uid`, `creationTimestamp`
and `managedFields` metadata from the protected backup before applying it.

```bash
kubectl --context docker-desktop -n logweir-system apply \
  -f identity-backup/logweir-signing-key.yaml \
  -f identity-backup/logweir-signing-trust.yaml
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m
```

If `logweir-signing-trust` says `established` but the Secret/key is missing,
bootstrap exits nonzero with recovery guidance. It will not generate a new key.
If the public ConfigMap differs from the private key, it likewise refuses to
overwrite either side. Preserve the old public material and old roster entry:
old archive signatures remain verifiable only while their original public key
and policy history remain available.

Routine Helm 3 and Helm 4 upgrades, hook retries, and rollbacks to a chart
version carrying the `post-rollback` identity hook validate and reuse the same
private bytes. Empty retained objects are creation-only
`pre-install,pre-upgrade` hooks, not ordinary release-manifest resources and
not `pre-rollback` hooks; an old revision therefore cannot reconcile an empty
placeholder over live key material. Helm stores only the empty hook definition,
never the patched private bytes. A rollback to a pre-bootstrap chart has no
such hook, so retention—not active validation—is its only identity guarantee;
validate again before resuming workloads. A failed hook leaves the retained
objects and failed Job for diagnosis; rolling the chart back does not roll the
identity back. After
`helm uninstall`, Helm's `keep` policy leaves the primary Secret, public
ConfigMap, each authorized namespace copy, and the authority-free
`ClusterRole/logweir-identity-singleton` marker. On a same-name reinstall,
connected live lookup omits existing identity creation hooks and the bootstrap
hook validates them in place:

```bash
kubectl --context docker-desktop -n logweir-system get \
  secret/logweir-signing-key configmap/logweir-signing-trust
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m
```

Do not use `--take-ownership` or `--force-replace` on identity resources. If
both retained objects were intentionally destroyed, restore them from backup.
Creating a different key is a trust rotation, not reinstall recovery.

The fixed singleton marker makes the supported v0.1 contract one Logweir
installation identity per cluster. A second release in another namespace fails
before claiming the global `TrustRoster/default` contract. Do not delete the
marker to force a second independent signer; remove it only as part of an
intentional full-cluster retirement after exporting old trust material. PLAT-19.1
is responsible for any future multi-installation trust-reference model.

Upgrading an installation that already has a manually provisioned
`logweir-signing-key` is supported: the live lookup leaves that Secret
unmodified and bootstrap derives the missing public ConfigMap from it. Back up
the Secret first. If a `logweir-signing-trust` ConfigMap already exists, it must
match exactly; bootstrap never repairs a mismatch by overwriting trust.

### Trust reference and rotation contract

The public ConfigMap's `trust-reference` is
`logweir.dev/v1alpha1/TrustRoster/default#spec.signingKeys`, matching the actual
v0.1 verifier. Bootstrap publishes material but does not silently authorize it:
a cluster administrator explicitly copies it into that roster. The current CRD
makes `TrustRoster.spec` immutable and the current controller reads the global
name `default`; changing that is PLAT-19.1 work, not behavior this bootstrap
pretends already exists.

During a planned rotation, retain the retiring public key alongside the new key
for old-archive verification, stop new signing with the retired private key,
and record the policy-effective time. Routine retirement preserves historical
verification; revocation is a separate policy decision and may deliberately
make historical evidence untrusted. Never infer trust from a public key stored
beside an archive. Until explicit overlapping trust-policy references land,
rotation of the immutable default roster is an administrator-coordinated
maintenance window and rollback must restore the prior roster plus matching
private/public identity backup together.

```bash
# Managed Helm path:
helm uninstall logweir -n logweir-system

# Low-level base-manifest path:
kubectl --context docker-desktop delete -f logweir.yaml
```

The Helm uninstall intentionally retains the primary private/public identity,
the signer copy in every authorized runner namespace, and
`ClusterRole/logweir-identity-singleton`. It removes distribution Jobs/RBAC,
runner ServiceAccounts and chart-managed NetworkPolicies. Preserve the retained
objects for same-installation recovery; retire them only after exporting the
private/public identity and old trust policy.

The four cleanup scopes are the control plane, scratch topics, archive objects
and evidence objects. `kubectl delete -f logweir.yaml` removes only that first thing:
the Namespace and everything inside it, the fourteen CRDs and all their custom
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
cluster-scoped `TrustRoster` and `TrustPolicy` — the trust anchors of every
archive this installation signed. `logweir trust export` writes the public
material out; export it before uninstalling.
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
