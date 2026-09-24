# Installing Logweir

Use this guide for image selection, cluster prerequisites, installation and
uninstall. See [kubernetes.md](kubernetes.md) for operation and troubleshooting,
and [the chart reference](../charts/logweir/README.md) for Helm values.
Commands use `docker-desktop`; substitute your intended context explicitly.
**A new operator starts at [quickstart.md](quickstart.md), *The supported
path***, which walks this guide's supported steps in order and then carries on
to the first backup, a restore and a disaster restore.

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

The runner image carries **two** binaries. `logweir` is its entrypoint and is
what every backup, restore, verify and check Job runs; `logweir-retention` sits
beside it on `PATH` and is the command the controller gives a `RetentionPolicy`
enforcement Job (`docs/kubernetes.md` §7f). There is no separate retention image
and no separate chart value: the image `runnerImage` — or `LOGWEIR_RUNNER_IMAGE`
below — names is the image an `Enforce` run will pull. A runner image built from
a tree whose `Dockerfile` does not build `-p logweir-retention` leaves every
other Job working and makes that one exit 127, `executable file not found in
$PATH`; `./scripts/render-install.sh --check` refuses such a tree and the image
workflow runs the binary out of the built image before publishing it.

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

**The published chart.** Every publication of the images publishes the chart
beside them, as an OCI artifact on Docker Hub, from the same run and after the
images are public (`scripts/ci-images.sh chart`, in `images.yml`'s promote job):

| images published as | chart | `appVersion` |
|---|---|---|
| `sha-<commit>` (every push to `main`) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version 0.1.0-sha-<commit>` | `sha-<commit>` |
| `v<X.Y.Z>` (a release tag) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version <X.Y.Z>` | `v<X.Y.Z>` |

The packaged chart's four Logweir image defaults (`controllerImage`,
`runnerImage`, `api.console.image`, `ui.image`) are that same tag, so installing
it installs exactly the images of that commit — no `--set` for images and no
checkout of the repository:

```bash
helm upgrade --install logweir oci://registry-1.docker.io/vladyslavhaina/logweir-chart \
  --version 0.1.0-sha-<commit> -n logweir-system --create-namespace --wait --timeout 10m
helm show values oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version 0.1.0-sha-<commit>
```

A `main` version is a SemVer pre-release, so always pass `--version`. The
package's name is `logweir-chart` (Docker Hub names a chart's repository after
the chart, and `vladyslavhaina/logweir` is the runner image); everything it
installs is named exactly as from the source chart. The publication step
compares the bytes the registry serves back with the bytes it pushed. [UNVERIFIED — no chart has been pushed yet: the first publication is the first main push after this change merges.]

**On first publication, `vladyslavhaina/logweir-chart` must be Public in Docker Hub.** The publication step pulls the chart back anonymously; if Docker Hub creates the repository private (the namespace's default visibility decides), `main` CI's chart step fails closed until the repository is made Public (Repository → Settings → Visibility) and the job is re-run — the re-push overwrites the same version and is compared again.

**From a checkout.** `identity.bootstrapImage` is pinned in
`charts/logweir/values.yaml` to a reviewed runner digest that contains the
identity CLI, so the clean default command needs neither a local key nor an
image hash:

```bash
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m
```

From a checkout the four image defaults are `:latest`; the published chart
above is the way to install one commit's images by construction.

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

**A `TrustPolicy` is the current way to say whose keys a namespace trusts**
(PLAT-19.1: lifecycle, overlap rotation, one usage per key — [keys.md](keys.md)).
The roster below is the legacy fallback every namespace no policy governs
resolves to; it is still the smallest first-install step, and nothing has to be
migrated off it. A namespace a policy governs never consults the roster
([kubernetes.md](kubernetes.md) §8, *Trust resolution*).

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
**different principal** from the runner's archive credential, and read-only:
neither the controller nor the runner holds any object-store delete capability.
The one deleter is a `RetentionPolicy`'s enforcement worker, under its own
separately granted credential (§3.11 below). Created in `logweir-system`.

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

### 3.11 Object-store permissions, per destination role

A `BackupDestination` names up to four grants, and they are separable on
purpose: the principal that WRITES an archive should not be the principal that
reads evidence back to verify it. It is written in S3 action names; the
equivalent MinIO policy actions have the same spellings.

**The table below WAS measured**, on 2026-09-21, against a deny-by-default
MinIO: each role's own operation was run once with the intended set, once for
every single action withdrawn from it, and once more with exactly the actions
whose removal broke it. The method, one bisection row per action naming the
object whose status carries the refusal, and the two rows that are about a
different principal than their name suggests, are in
[`docs/kubernetes.md`](kubernetes.md) §7a, *The object-storage permission each
grant actually needs, measured*. This is a set to lock down to.

| Role | Actions | Resources |
|---|---|---|
| `archiveWrite` | `s3:ListBucket` (condition `s3:prefix` in `<prefix>/*`), `s3:GetObject`, `s3:PutObject` | the bucket for the listing; `arn:aws:s3:::<bucket>/<prefix>/*` for both object actions, plus `s3:PutObject` on `arn:aws:s3:::<bucket>/logweir/*` for the execution claim, the receipt and the catalog record — the claim is a conditional create (`If-None-Match: *`), so the store must honour it or every backup exits 4 `ExecutionClaimUnproven` ([why](formats/backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id)) |
| `archiveRead` | `s3:ListBucket` (condition `s3:prefix` in `<prefix>/*`), `s3:GetObject` | the bucket; `arn:aws:s3:::<bucket>/<prefix>/*` |
| `evidenceWrite` | `s3:PutObject` (conditional create) | `arn:aws:s3:::<bucket>/logweir/*` |
| `evidenceRead` | `s3:GetObject` | `arn:aws:s3:::<bucket>/logweir/*` |
| write probe (opt-in; run as the `evidenceWrite` grant, already inside its `logweir/*`) | `s3:PutObject` | `arn:aws:s3:::<bucket>/logweir/readiness/*` |
| `RecoveryCatalog` sync | `s3:ListBucket` (condition `s3:prefix` in `logweir/*`), `s3:GetObject` | the bucket; `arn:aws:s3:::<bucket>/<prefix>/*` AND `arn:aws:s3:::<bucket>/logweir/*` |

**`s3:AbortMultipartUpload` and `s3:GetBucketLocation` are in no row**, because
the measurement removed each of them and every operation still succeeded: the
engine is given an explicit region and never asks the bucket for one, and no
upload in the acceptance was large enough to abort. Grant them if your own
sizes differ; nothing here needs them.

**The `RecoveryCatalog` sync row is not `archiveRead`**, though the sync uses
that grant: it lists only under `logweir/*` and reads under BOTH roots, where
`archiveRead` lists under the archive prefix and reads only there. A
destination whose catalog you sync needs its `archiveRead` grant widened to
this row, or the catalog publishes no view.

**No role is ever granted `s3:DeleteObject`.** Logweir prints the removal
commands and an operator runs them; the only component that deletes is
`logweir-retention`, under its own separate `spec.enforcement.credentialSecretRef`
(`s3:ListBucket` with `s3:prefix` in `<prefix>/*`, and `s3:GetObject` and
`s3:DeleteObject` on `arn:aws:s3:::<bucket>/<prefix>/*`). `s3:GetObject` is
for a HEAD before every delete: on a versioned bucket — every S3 Object Lock
bucket is one — a delete by key only writes a delete marker and never
consults a legal hold, so the worker refuses to delete there (code
`VersionedBucket`, also read from the version id its own intent tombstone
gets back), and without the grant it cannot tell and deletes nothing (code
`VersionProbeRefused`). **Grant it before upgrading**; a policy that degraded
without it re-probes 24 h after its last run, or at once on a spec edit
(`docs/kubernetes.md` §7f, "Upgrade and rollback"). **`Deleted` means the
current object at each key was removed**; noncurrent versions a bucket keeps
are its lifecycle's responsibility and Logweir cannot see them. So **do not
enforce on a bucket whose versioning was ever enabled and later suspended**
(re-run backups rewrite the same keys, and there a deletion removes only the
newest copy while being recorded `Deleted`), and **do not change a bucket's
versioning while a retention run is in flight** — use `mode: ExternalLifecycle`
or a noncurrent-version lifecycle rule instead. The measured minimum before that check was
`s3:ListBucket` and `s3:DeleteObject` alone. [UNVERIFIED — the grant with s3:GetObject is re-measured by U6/retention-enforcer at the next lab refresh.]

**Grant `evidenceRead` its `s3:ListBucket` if you want "absent" to mean absent.**
It is not in the measured minimum — the `destination.evidenceReadable` probe
reads an absent key and `ObjectNotFound` is its passing answer — but without it,
S3 answers `AccessDenied` for a key that is not there, so a missing receipt is
indistinguishable from a denied read and verification reports `Unknown`
presence rather than `Absent`.

**`archiveRead` is what an `evidenceRead: ArchiveReadGrant` reuses**, and the CRD
refuses that mode unless an explicit `spec.access.archiveRead` exists: a write
grant is never reused to verify what it wrote.

**How a `SecretKeys`, `WorkloadIdentity` or `ArchiveReadGrant` evidence read
happens.** The controller holds no verb on Secrets, so it never reads this
credential itself. After a run finishes, it creates a short evidence-fetch check
Job, `lwc-ev-<20 hex>`, in the run's own namespace. The Job is owned by the
`Backup` or `Restore` and its plan `ConfigMap` is immutable. The kubelet
projects exactly the `evidenceRead` grant into the Job: that grant's Secret
keys, or for `WorkloadIdentity` its ServiceAccount (default `logweir-runner`).
For `ArchiveReadGrant` that grant is the `archiveRead` Secret. The Job gets no
signing key, no `archiveWrite` or `evidenceWrite` grant, and no
ServiceAccount token.

The Job relays the receipt (or scorecard) and its sidecar, at most 1 MiB and
64 KiB. The controller then does all the verifying itself:

- it hashes the relayed receipt and compares the digest with the one the runner
  reported;
- it checks that the document names this run;
- it checks the DSSE signature against the namespace's trust.

While the Job runs, `status.evidence.verification.result` is `Pending` and
`status.evidence.observation` names the Job, its UID and the attempt. `Pending`
is never green. It becomes the reached verdict (`Valid` also brings
`windowCovered`, which is what makes the console offer *Restore this point*).
If the Job cannot finish, it becomes `NotAttempted` with the cause named. A
failed attempt is retried after 1, 5 and 15 minutes, each time with a new Job;
the backup itself is never re-run. At most
`checks.maxEvidenceFetchActivePerNamespace` (default 4) such Jobs run in one
namespace at a time, and a run waits `Pending` for a free slot. The Job gets a
10-minute TTL once its verdict is recorded, and owner garbage collection removes
it with its run.

**Absent grants do not widen.** `archiveRead` and `evidenceWrite` absent mean
`archiveWrite` is used; `evidenceRead` absent means verification is
`NotAttempted`, with a detail naming the field.

`evidenceRead: ControllerIdentity` is the one mode whose credential is the
controller's own ambient chain rather than a Secret in your namespace, and it is
**opt-in and administrator-gated**: the location has to appear in
`evidence.controllerIdentityLocations` in the installation policy `ConfigMap` in
the release namespace, matched on endpoint, region AND bucket. A namespace
operator cannot add one. An unlisted location is refused with
`ControllerIdentityNotAllowlisted` and verification is `NotAttempted`.

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

**And, in every namespace that will run a retention enforcement Job, the second
account:**

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f config/rbac/retention-serviceaccount.yaml
```

Enforcement Jobs run in the namespace of the `RetentionPolicy` that produced
them, and every one names `logweir-retention` — a separate account from
`logweir-runner`, granted no verb either, because it is the one pod in the
installation that mounts a delete-capable storage credential and "what has run
as the deleter" should have an answer. **On the Helm path this is
`retention.enabled`**, which renders the account in the release namespace and in
every `identity.authorizedRunnerNamespaces` entry; do not apply this file over
those Helm-owned objects.

Without it, enforcement fails closed and undiagnosably: the Job is created, its
pod is never admitted because the account does not exist, and nothing in the
`RetentionPolicy`'s status says which of the four gates was shut.

### 5. Binding the five human roles

`logweir.yaml` ships `logweir-viewer`, `logweir-operator`, `logweir-approver`,
`logweir-trust-admin` and `logweir-retention-admin` **unbound**. Who may approve
a restore in which namespace is your decision, not the install file's.

```bash
kubectl --context docker-desktop create rolebinding logweir-viewer \
  --clusterrole=logweir-viewer --user=<user> -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-operator \
  --clusterrole=logweir-operator --user=<user> -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-approver \
  --clusterrole=logweir-approver --user=<someone-else> -n <namespace>
```

`logweir-approver` is `create` on `approvals` plus **read** on `preflights`
(approval context — a readiness verdict is redacted by construction and
authorizes nothing on its own) and **nothing else**. Bind it to somebody who is
not the operator: `self_attested: false` means only "two different keys", and
one person holding both keypairs satisfies it.

**`logweir-trust-admin` is cluster-scoped and needs a `ClusterRoleBinding`.**
`TrustPolicy` is a cluster-scoped kind — a namespace never names its own trust
— so a `RoleBinding` of this role grants nothing at all, silently:

```bash
kubectl --context docker-desktop create clusterrolebinding logweir-trust-admin \
  --clusterrole=logweir-trust-admin --user=<security-owner>
```

It is the **only** holder of a write verb on `trustpolicies`, and that is the
point: a trust policy decides whose keys may sign an approval, so an operator
who could edit one could add their own key and then approve their own restore.
It carries no `delete` — deleting a policy does not retire a key, it removes
the binding that governs a namespace and sends every namespace it bound back to
the legacy roster, which is a widening dressed as a cleanup. Withdrawing trust
is an edit.

**`logweir-retention-admin` is namespaced and needs a `RoleBinding` per
namespace.** It is the only holder of a write verb on `retentionpolicies`:

```bash
kubectl --context docker-desktop create rolebinding logweir-retention-admin \
  --clusterrole=logweir-retention-admin --user=<storage-owner> -n <namespace>
```

The reason it is not on `logweir-operator` is `spec.mode`. A `RetentionPolicy`
is created in `Report`, and moving it to `Enforce` is what makes this
installation delete data; that is "a separate, later, administrator decision
with its own credential", and RBAC is the only layer that can say whose
decision. Bind it to somebody who does not hold `logweir-operator`, or an
operator can set `Enforce` on a policy they also authored.

It carries no `delete` either, for a different reason than the trust admin's:
turning enforcement off is `mode: Report`, and deleting the object while an
enforcement Job holds its lease takes the `status.lease` record the
restore-side hold reads with it.

**Holding it deletes nothing by itself.** An enforcement run passes three more
gates, and all three are somewhere else: the `logweir-retention` ServiceAccount
must exist in the namespace (chart value `retention.enabled`, which renders it
in the release namespace and every `identity.authorizedRunnerNamespaces` entry;
or `config/rbac/retention-serviceaccount.yaml` applied by hand — step 4), the run needs an object-store credential whose scope is the
policy's own prefix and never `logweir/`, and every run needs an
administrator's `approve-plan` carrying the current `planSha256`.

### 5d. The console/API principal

The product API is a service, not a page: it authenticates every request,
resolves the actor's roles from its own binding table and refuses an ungranted
namespace before it makes any Kubernetes call. So its ServiceAccount's grants
are the union of what every route may need, and the per-actor narrowing happens
above them — which is why they are wider than the local proxy's
(§"Serving the UI") and why the two are separate identities that must not be
conflated.

The chart renders the identity and its roles under `api.enabled` (`<release>-api`
in the release namespace, bound in `api.namespaces`). `logweir.yaml` does not:
a console is optional, and an install file with no conditionals cannot ship an
identity only some installations want.

What it holds, and the seal it comes from — `crates/logweir-api/src/kube.rs` is
the one adapter every route goes through, it is sealed so no other module can
add a kind, and it spends exactly four Kubernetes verbs:

| Grant | Why |
|---|---|
| `get`/`list` on the eight product kinds | every projection the API serves |
| `create` on all eight | the collection routes with a POST, and `approvals` for PLAT-19.2: the console's signed ordinary confirmation, a governed request's confirmation object, and the approver's countersigned Approval (`POST .../restores/{name}/approval`) |
| `patch` on `backupschedules`, `backupdestinations`, `topicdiscoveries`, `preflights` | suspension and policy edits, access rotation, and the two checks whose `spec.cancelRequested` may be raised |
| `get`/`list` on `protectionpolicies`, `recoverycatalogs`, `rehearsalschedules`, `retentionpolicies` | D3's read surfaces |
| `create` on `recoverycatalogs` | "connect existing archive", D3's one write |
| `get`/`list` on `trustpolicies` (cluster-scoped, its own `ClusterRoleBinding`) | the keys view, and comparing a readiness check's governing `TrustPolicy` referent |
| `get` on `trustrosters` with `resourceNames: ["default"]` (cluster-scoped, `<release>-api-trustroster`) | comparing a readiness check's `TrustRoster/default` referent; without it every readiness verdict on a cluster with a roster is `unverifiable` and served stale. No `list` and no other roster |
| `get` on `configmaps` | a check's stored result and a catalog view's pages, each verified by owner UID, immutability and digest before a byte is served |
| `create` on `secrets` | the write-only credential entry |

And what it does **not** hold, each for a reason:

* **no `watch`**, anywhere. The adapter has no watch method; the operation
  event stream is server-sent events over this service's own reads.
* **no `delete`**, anywhere.
* **no read verb on `secrets`** — that missing verb is what makes a
  console-written credential write-only. `create` cannot name a
  `resourceNames`, so the *shape* of that create is fenced by a
  ValidatingAdmissionPolicy instead (§5b); set
  `admissionPolicy.consoleServiceAccountName` to this account.
* **no `list` on `configmaps`**: a console that could page every ConfigMap in a
  namespace is an inventory of somebody else's configuration.
* **no write verb on `trustpolicies`**. Trust administration is not an API
  operation in v1; the supported path is `kubectl apply` under
  `logweir-trust-admin` (see [keys.md](keys.md)).

**The per-actor narrowing of the trust read is the service's.** `TrustPolicy`
is cluster-scoped, so its read grant is installation-wide and no RBAC rule can
narrow it to a namespace. `GET /api/v1/trust-policies` does: it is
Administrator-only, serves a policy only when it governs a namespace the actor
administers (or is the installation default), and filters the namespace lists
inside it to that administered set, saying so with `namespacesFiltered`
([kubernetes.md](kubernetes.md) §16, *Reading a TrustPolicy needs an
administrator binding*). An earlier revision of this paragraph said the route
served every governed namespace; that defect is closed.

`./scripts/render-install.sh --check` answers the `kubectl auth can-i` question
for every pair above, in both directions, against every checked-in render — so a
route added without its grant, or a grant added without its route, is a red
build rather than a 403 in production. It finds the console's roles through
the BINDINGS that name its ServiceAccount, not by role name: a new role under
any name bound to the account is audited like the three above, and a binding
that names the account must name nobody else beside it (a second subject would
hold every grant the console holds). `chart_lint` holds the same property from
Rust for the console and for the controller account, per scope.

**What an operator may write on the D3 kinds.** `logweir-operator` gains
`create` on `protectionpolicies`, `recoverycatalogs` and `rehearsalschedules`,
and then exactly the edit each CRD permits: `update`/`patch` on a
`ProtectionPolicy` (its spec is not sealed — an objective is policy you tune),
and `patch` alone on the other two, whose CRDs leave one mutable field each
(`spec.suspend` on a rehearsal, `spec.syncRequest` on a catalog). What may
change is the CRD's own CEL rule and not the verb, exactly as it is for
`backupschedules`. `retentionpolicies` is not among them — see
`logweir-retention-admin` above.

**What the viewer can and cannot see.** `logweir-viewer` reads all fourteen
kinds, including `backupdestinations`, `topicdiscoveries` and `preflights`. It
holds no verb on `configmaps`, so a kubectl viewer sees a topic inventory's
**summary** on `status.result.counts` and never its pages: the chunk documents
are ConfigMaps, and paging them is the console API's job, with its own
owner-UID, immutability and digest checks. `trustpolicies` is cluster-scoped,
so a namespace `RoleBinding` of `logweir-viewer` does not convey it; a viewer
who should also read trust needs a `ClusterRoleBinding`, which is a separate
and visible decision.

### 5e. Running the console in the cluster (`api.console.enabled`)

§5d renders the identity and starts nothing. This is the pod that runs as it,
and the order is deliberate: install the principal, satisfy yourself with
`kubectl auth can-i` that it can do what the console does and no more, then run
the console.

**The image.** `logweir-console`, built by `Dockerfile.console`: the
`logweir-api` binary plus the same twenty-six static page files the
`logweir-ui` image carries, copied from the same one `ui/` directory in the
source tree. `scripts/check-image-api.sh` hashes what the image will serve
against that directory, so the console and the legacy proxy cannot drift apart,
and it refuses an image carrying any key-shaped path. It is published beside the
other three; §"Bring your own registry" applies to it unchanged
(`docker tag logweir-console:check …`, and `--set api.console.image=…`).

**Two switches.** `api.console.enabled` requires `api.enabled` and the render
refuses the pair rather than producing nothing.

**The key Secret first — the chart does not generate one.** These are
persistent keys: one that changes on every render would be unrecoverable on
upgrade and would make the checked-in rendered files unstable.

```console
$ head -c 32 /dev/urandom > cursor.key                 # localAdmin: raw bytes
$ kubectl --context docker-desktop -n logweir-system \
    create secret generic logweir-console-keys --from-file=cursor.key
$ rm cursor.key
```

**Where this Secret sits relative to the controller.** It lives in the release
namespace. Residual **O1** (`docs/kubernetes.md` §15.4, which states it for the
installation signing key: Job CRUD in a namespace that holds that key is
equivalent to holding it, because a Job the controller creates can mount it)
applies to any Secret in a namespace where `weirkeeper` may create Jobs, and the
default chart binds `weirkeeper` cluster-wide. For the `localAdmin` shape that is
accepted — its authority is the port-forward permission. For `shared` mode it is
not, and the chart refuses to render shared mode until the controller is scoped
away from the release namespace (D0 stage 5, below).

In `shared` mode that Secret carries two files instead, each two lines, and
`api.console.keyVersion` must equal the `version:` they declare:

```console
$ printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > session.key
$ printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > cursor.key
$ kubectl --context docker-desktop -n logweir-system \
    create secret generic logweir-console-keys \
    --from-file=session.key --from-file=cursor.key
```

**`api.console.mode` has no default and must be named.** The binary refuses a
configuration file that forgets to say which mode it wants rather than reading
it as the more permissive one, and the chart refuses the same way, naming the
field. The two modes are two shapes:

**`localAdmin` — the in-cluster administrator mode.** One configured
administrator, no identity provider, and a listener `logweir-api` refuses to
bind anywhere but `127.0.0.1`. The chart renders **no Service**, no Ingress and
no ingress rule in the NetworkPolicy, so there is nothing in the cluster that a
monitor or another pod could dial; the identity is the `<release>-api`
ServiceAccount of §5d, narrowly bound and never a kubeconfig; and readiness is
not gated on OIDC because there is none. Unlike the in-cluster proxy of
§"Serving the UI", turning this on cannot make a console that anyone who reaches
a Service can drive.

It exists for isolated labs and break-glass administration; it does not expose
Ordinary confirmation and it is **not** a shared console.

**`shared` is the only mode that may be exposed through a Service or an
Ingress.**

**Before shared mode: scope the controller (D0 stage 5).** Shared mode renders
only with `controller.watchNamespaces` — the execution namespaces where Backups,
Restores and checks run — and never with the release namespace in that list.
The chart then binds the controller with one RoleBinding per listed namespace
instead of its cluster-wide ClusterRoleBinding, grants the two cluster-scoped
trust kinds through `weirkeeper-cluster-scope`, and sets
`LOGWEIR_WATCH_NAMESPACES` so the controller watches exactly those namespaces.
The migration, in order:

1. List every namespace that holds Logweir objects:
   `kubectl --context docker-desktop get backups,restores,backupschedules,kafkaclusters -A`.
   Any that run in the release namespace move to an execution namespace first.
2. Prepare each execution namespace as step 4 describes (the runner
   ServiceAccount and the signing identity — `identity.authorizedRunnerNamespaces`).
3. Upgrade with `--set 'controller.watchNamespaces={team-a,team-b}'` and confirm
   with `kubectl auth can-i create jobs -n <release-namespace> --as
   system:serviceaccount:<release-namespace>:weirkeeper` that the answer is
   `no` (`charts/logweir/README.md` §`controller.watchNamespaces` has the
   whole matrix). Objects in a namespace left off the list stop being
   reconciled — nothing is deleted — until it is added.
4. Disable the in-cluster legacy proxy (`ui.enabled=false`): the chart refuses
   it beside a shared console, because anyone who reaches that Service acts as
   its ServiceAccount without signing in. The laptop
   `kubectl proxy --address=127.0.0.1` path stays.
5. Enable `api.console` in `shared` mode. Every `roles.bindings` namespace must
   be in `controller.watchNamespaces`. Set `trustedProxyCidrs` to the ingress
   controller's OWN pod range — no wider than `/16`, containing no other pod —
   and `requireTrustedProxy: true` to refuse any request that did not come
   through it over HTTPS. A wider range would contain the pods it exists to
   refuse, and is refused.

Rollback is the same list emptied: `watchNamespaces: []` restores the
cluster-wide binding (the chart then refuses shared mode again, so disable the
console first).

**Upgrading a shared console installed before this release.** A values file
with `api.console.mode: shared` that rendered on the release before this one
(D0 stage 7, 2026-09-21) **no longer renders** until it is brought in line —
`helm upgrade` stops at render time, names the field, and changes nothing in the
cluster. That is deliberate: until the controller is scoped, the console's
session and cursor keys sit in a namespace where the controller may create a Job
that mounts them (residual O1), and D0 says shared mode must not be run as
secure in that state. The refusals such a file meets, in the order to fix them:

| refusal (the render names it) | fix |
|---|---|
| `api.console.mode=shared requires controller.watchNamespaces` | list the execution namespaces (steps 1–3 above) |
| `controller.watchNamespaces includes the release namespace` | move Backups/Restores out of the release namespace and drop it from the list |
| `api.console.mode=shared with ui.enabled=true is refused` | set `ui.enabled=false` (the laptop `kubectl proxy` path stays) |
| `api.console.roles.bindings names namespace … which is not in controller.watchNamespaces` | add that namespace to the list, or remove the binding |
| `api.console.trustedProxyCidrs names … wider than /16` (only with `requireTrustedProxy`) | narrow it to the ingress controller's own pod range |

Then upgrade once with all of them fixed. The running console keeps serving
through a refused upgrade, because nothing is applied. To roll back, re-install
the previous chart version with the previous values — the scoping objects
(`weirkeeper-cluster-scope`, the per-namespace `weirkeeper` RoleBindings) are
removed and the cluster-wide binding returns with it. Installs without a shared
console render unchanged.

```console
$ helm upgrade logweir charts/logweir -n logweir-system --reuse-values \
    --set api.enabled=true --set api.console.enabled=true \
    --set api.console.mode=localAdmin \
    --set api.console.keySecret=logweir-console-keys
$ kubectl --context docker-desktop -n logweir-system \
    port-forward deploy/logweir-api 8484:8484
$ open http://127.0.0.1:8484/ui/
```

`kubectl port-forward` takes a Deployment directly, which is why this mode needs
no Service. The local port must be `8484`: the rendered `publicOrigin` carries
the listen port and the service refuses a mismatch, because an origin that does
not match the one the browser sends is a CSRF check that cannot pass.

**`create pods/portforward` in this namespace is equivalent to full console
administrator authority over every bound namespace — grant it as you would grant
that.** There is no identity provider and no product role check in this mode:
every request is the one configured administrator, so that Kubernetes verb *is*
the authorization boundary.

**This mode has no probes**, because a kubelet probe addresses the Pod IP and
this listener will not answer there. A configuration the binary refuses is
therefore a `CrashLoopBackOff` with exit code 2, not a NotReady endpoint; the
container's last log line names the field. `shared` mode binds the Pod IP and
gets `/healthz` and `/readyz`.

**Shared mode is the SSO console and needs four more things:** an OIDC issuer
and client, the client secret in its own Secret under the key `clientSecret`, an
exact `https://` public base URL, and a TLS certificate for the Ingress.
`charts/logweir/examples/console-shared.values.yaml` is the complete shape and
`charts/logweir/README.md` lists every configuration the chart refuses at render
time — a non-HTTPS base URL, an Ingress with no TLS Secret, an Ingress in front
of `localAdmin` mode, a role binding for an unbound namespace, a wildcard in a
binding. Each is refused with the field named, before anything installs.

**No credential reaches the ConfigMap.** The rendered configuration carries
paths into read-only Secret mounts under `/var/run/logweir/` and never a value;
a lint row walks every rendered console ConfigMap to keep it that way.

**Uninstalling the console leaves everything else.** Setting
`api.console.enabled=false` removes the ConfigMap and the Deployment, and with
them the Service, Ingress and NetworkPolicy wherever those were rendered; the principal, its grants and
every Logweir custom resource are untouched. The console creates and reads
objects and executes nothing, so removing it stops no backup, cancels no restore
and loses no evidence. Existing installations that never set the flag see no
change at all.

### 5f. Ordinary confirmation and governed approval (`approvalPolicy.*`, optional)

Without this step every namespace keeps today's governed approval
(`legacy-governed-v1`) and nothing below applies. To bind namespaces to an
approval policy (PLAT-19.2; the contract, the enforcement points and
upgrade/rollback are in `docs/kubernetes.md` §8, *Approval policy*):

1. Put the keys on the `TrustPolicy` that governs each namespace you will bind
   (`docs/keys.md`, *Key usage separation*): the console's
   `ConsoleConfirmation` public key, and for a Governed namespace each
   approver's `GovernedApproval` key with `principal.id` = the approver's
   `<issuer>#<subject>`.
2. Create the console's key Secret in the release namespace:

   ```bash
   openssl genpkey -algorithm ed25519 -out confirmation.key
   kubectl --context <ctx> -n <release-ns> create secret generic \
     logweir-console-confirmation --from-file=confirmation.key
   openssl pkey -in confirmation.key -pubout -out confirmation.pub.pem
   rm confirmation.key
   ```

3. Set `approvalPolicy.policies`, `approvalPolicy.namespaces`,
   `approvalPolicy.confirmationKeySecret`, and — only if a policy is Ordinary —
   `approvalPolicy.allowOrdinaryConfirmation: true`
   (`charts/logweir/examples/approval-policy.values.yaml`), and upgrade. The
   chart renders one immutable ConfigMap and mounts it into the controller and
   the console; `helm lint` refuses an Ordinary policy without the floor, an
   undeclared policy, and a console-served bound namespace without the key.

Upgrade CRDs first (the `Approval` status gains `authorization`), then the
controller and runner image, then the console, then set the binding. Changing
a policy later is a rollout of both Deployments; Restores confirmed under the
old policy and not yet admitted must be submitted again.

### 5a. The installation policy `ConfigMap` (optional, and what it unlocks)

The controller reads one administrator-owned document, `weirkeeper-policy`, in
the **release** namespace, under the key `policy.json`. **It is optional**: an
install that renders none runs on the documented defaults and reports
`configuration.policy` as *ready*, never as an error. `docs/kubernetes.md` §22
is the field reference.

**The Helm chart renders it** from `values.yaml` and needs nothing here.
**`logweir.yaml` does not**, because a kustomize install has no values file to
render it from; the Deployment it ships points at
`weirkeeper-policy` in its own namespace through
`LOGWEIR_INSTALLATION_NAMESPACE` (the downward API) and finds nothing until you
create it. The consequence is worth stating plainly: until that document
exists, **`visibility.state: attestedComplete` is unreachable** on a kustomize
install, because the only thing that can produce it is an administrator
attestation inside this ConfigMap.

```bash
kubectl --context docker-desktop -n logweir-system create configmap weirkeeper-policy \
  --from-file=policy.json=./policy.json
```

**Who may write it is the access-control decision.** `create`/`update` on a
ConfigMap in the release namespace is a chart or cluster administrator;
`logweir-operator` names no `configmaps` at all and cannot. That is what makes
an attestation an administrator statement rather than a self-assessment.

**A document the controller refuses fails closed.** It is parsed with unknown
fields rejected and ten range rules applied; a refusal yields empty attestations
and an empty evidence allowlist plus one advisory
`configuration.policy notReady PolicyUnreadable` row on a `Preflight`. Nothing
else goes red — but it **does** log, once per 30-second cache miss:

```bash
kubectl --context docker-desktop -n logweir-system logs deploy/weirkeeper | grep REFUSED
```

That line names the failing rule, and it is the answer to "my attestation is
configured and the discovery still says `unknown`".

**A Helm install cannot produce a document the controller then refuses.**
`values.schema.json` carries every per-field bound, pinned to the same
constants the parser uses; `templates/policy.yaml` refuses the two cross-field
rules JSON Schema cannot express (`maxActiveTotal >= maxActivePerNamespace`,
`defaultMaxTopics <= hardMaxTopics`) with a named `fail` at render time. If you
**hand-write** the file, validate it against
`charts/logweir/values.schema.json`'s `checks`/`engine`/`evidence` blocks *and*
check those two pairs yourself — or render one with `helm template` and copy
the result, which is the shortest safe path.

### 5b. Fencing the console's `create secrets` (Kubernetes 1.30+)

`logweir-api` holds `create` on `secrets` and **no read verb**, so a stored
credential cannot be read back by any route. `create` alone is still the widest
grant the service asks for: in a namespace it could in principle mint a
`kubernetes.io/service-account-token` Secret for any ServiceAccount there.

Both credential builders stamp a distinct `type` —
`logweir.dev/object-store-credential` and `logweir.dev/kafka-sasl-password` —
and the `app.kubernetes.io/managed-by: logweir` label, so a
`ValidatingAdmissionPolicy` can require both:

```bash
# Helm
helm upgrade --kube-context docker-desktop logweir charts/logweir \
  --set admissionPolicy.enabled=true \
  --set admissionPolicy.consoleServiceAccountName=logweir-api

# kustomize: edit the principal first, then apply by hand
kubectl --context docker-desktop apply \
  -f config/samples/console-credential-admission-policy.yaml
```

**It is off by default for one reason**, and it is not a security opinion:
`admissionregistration.k8s.io/v1` `ValidatingAdmissionPolicy` is Kubernetes
**1.30+**, and Logweir's floor is 1.29, where the document is rejected with
`no matches for kind` and the whole apply fails. Turn it on wherever the API
server has the kind.

**It fences the console this chart renders.** With `api.enabled` the chart
renders the console's ServiceAccount, `<release>-api` (`logweir-api` for a
release named `logweir`), and its `create secrets` grant (§5d),
and it refuses to render the policy when
`admissionPolicy.consoleServiceAccountName` is not that account, because a
policy naming nobody would install, look enabled and fence nothing.

**What it does not do.** A cluster administrator can delete the policy; it
raises the cost of a mistake and of a compromised console, not of a deliberate
administrator. It says nothing about what the console does with a credential it
legitimately creates, and it is not what keeps the value unreadable — that is
the missing read verb. The policy matches `CREATE` only, which is safe exactly
as long as no role holds another verb on `secrets`; that is pinned by
`manifest_lint::no_shipped_role_may_write_or_read_a_secret_it_does_not_name`
rather than left as an assumption. **[UNVERIFIED — neither document has been
applied to a live API server]**. What would verify it: on a 1.30+ cluster, as the console
ServiceAccount, create a Secret of type
`logweir.dev/object-store-credential` carrying the managed-by label (must be
accepted) and one of type `kubernetes.io/service-account-token` (must be
rejected, naming the policy's message), then show the second succeeding once
the binding is deleted.

### 5c. Notification sinks on a laptop cluster (`notify.allowInsecureSinks`)

`logweir notify deliver` **refuses a non-`https://` webhook or Slack URL before
it dials**. A scratch receiver on `http://echo.<ns>.svc:8080` therefore receives
nothing on a default install, and the alert is recorded as
`<sink>:failed` — the sink never saw a request.

The escape hatch is an **installation** setting and is off:

```bash
helm upgrade --install logweir charts/logweir -n logweir-system \
  --set notify.allowInsecureSinks=true      # LOCAL DEVELOPMENT ONLY
```

It renders `LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS=1` on the `weirkeeper`
Deployment and the controller forwards `NOTIFY_ALLOW_INSECURE_SINKS=1` into
every delivery Job it creates. Left at its default, the chart renders **no**
such variable and the delivery Job is byte-identical to the one it rendered
before the value existed.

**A `ProtectionPolicy` cannot turn it on**, and that is the point: the spec is a
namespaced object any namespace operator may write, and a field there would let
whoever creates a policy downgrade their own alerts' transport to cleartext —
the protection event, the policy's name, its health, and on Slack a bearer
credential carried in the URL itself. The only place to set it is the
controller Deployment, which is the cluster administrator's.

**Production leaves it `false`.** Turning it on does not weaken TLS for an
`https://` sink and changes nothing else about the Job; it removes one refusal,
and that refusal is the only thing standing between an alert and a cleartext
POST.

---

## Upgrade CRDs before upgrading the controller

Helm installs `crds/` only on first install; it neither upgrades nor rolls CRDs
back. Apply the new additive schemas, wait for all fourteen Logweir definitions
to be established, and only then upgrade the release:

```bash
kubectl --context docker-desktop apply --server-side --force-conflicts -f charts/logweir/crds/
for crd in approvals backupdestinations backups backupschedules kafkaclusters \
           preflights protectionpolicies recoverycatalogs rehearsalschedules \
           restores retentionpolicies topicdiscoveries trustpolicies trustrosters; do
  kubectl --context docker-desktop wait --for=condition=Established \
    "crd/${crd}.logweir.dev" --timeout=60s
done
helm upgrade logweir charts/logweir -n logweir-system --wait --timeout 10m
```

The loop names all fourteen kinds, the five D3 ones included, and the order
matters in one direction only: **CRDs first, controller second**. A controller
that starts before its CRDs exist logs a reflector error per missing kind and
reconciles nothing of that kind; CRDs applied ahead of a controller that does
not know them are inert, which is the safe half.

**`--force-conflicts` is required, not a convenience.** Helm created these
CRDs from `crds/` and owns their fields, so a server-side apply without it is
refused field by field (`Apply failed with 3 conflicts: conflicts with
"helm"`) for every CRD that already exists — and the `Established` wait still
passes, on the OLD schemas (the PoC install's upgrade rehearsal R1 found the six
`v0.1.5` CRDs unchanged that way). Afterwards
`kubectl --context docker-desktop diff --server-side --force-conflicts -f charts/logweir/crds/`
prints nothing when the live definitions are the chart's.

Stop before Helm if any apply/wait/diff fails. **Every change in this release is
additive**: eight new kinds — `BackupDestination`, `TopicDiscovery`,
`Preflight`, `TrustPolicy`, `ProtectionPolicy`, `RehearsalSchedule`,
`RecoveryCatalog` and `RetentionPolicy` — plus optional fields and optional
status blocks on `Backup`, `BackupSchedule` and `Restore`, and one additive
enum value on `Approval`. **No kind is removed**: `TrustRoster` is deprecated in
its description and still served, and with no `TrustPolicy` present the
controller synthesises `legacy-roster-v1` from it, so nothing has to be migrated.
`Backup` additionally gains the D1 run contract — `spec.trigger`, three fields
inside `spec.scheduleRef`, `spec.allUserTopics` and `status.selection` — all
optional, and a `Backup` with none of them is read exactly as the controller
that created it read it.
No existing object is converted, nothing is rewritten, and an object that names
none of the new fields resolves exactly as it did.

**One widening is not rollback-safe once you use it.**
`Approval.spec.subjectRef.kind` gained `RehearsalSchedule`. A controller image
that predates this change cannot decode such an object, and that is a reflector
decode error which stalls **every** `Approval` reconcile — not one object. Only
a standing rehearsal authorization creates one (`logweir drill approve
--standing`, [kubernetes.md](kubernetes.md) §7g); an installation that never
minted one can apply these CRDs and roll the controller back safely, and one
that did must remove those `Approval`s first — see
[kubernetes.md, "The one widening that is NOT rollback-safe"](kubernetes.md#the-one-widening-that-is-not-rollback-safe-approvalspecsubjectrefkind). The one field that stopped
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
Namespace, the fourteen CustomResourceDefinitions, the RBAC — one
ServiceAccount, **six** ClusterRoles (`weirkeeper`, the three human roles,
`logweir-trust-admin` and `logweir-retention-admin`) and one ClusterRoleBinding
— the controller Deployment
and the runner NetworkPolicy, and **no custom resource** — so the
CRD-not-yet-established ordering failure cannot happen. It carries no
`ValidatingAdmissionPolicy` either, and cannot: that kind is 1.30+ and this
file has to apply unedited on the stated 1.29 floor (§5b).

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

`kubectl proxy` runs the page with the **viewer's entire cluster authority**,
not with the roles `logweir.yaml` ships, and two flags (`--address=127.0.0.1`,
and never `--disable-filter`) are all that keep it a local page.
[kubernetes.md](kubernetes.md) §16, *Serving the UI*, is the authority for that
residual, the two flags and the hardened kubeconfig that holds less; this
document does not restate it. This page is the **legacy** serving path: the
supported console is `logweir-api` (§5e), whose identity and roles are its own
([quickstart.md](quickstart.md), *The supported path*).

---

## Two clients, two trust stores

Logweir speaks to Kafka through **two different TLS stacks**: the engine
(`kafka-backup`) falls back to its **bundled `webpki-roots`**, and Logweir's own
rdkafka path uses the **image's `ca-certificates`**. A private CA installed on
the node reaches neither.

**On Kubernetes, name the CA once on the saved connection:**
`KafkaCluster.spec.auth.tlsCa` (with `auth.tls: true`). The runner hands that
one file to **both** clients — librdkafka's `ssl.ca.location` and the engine's
`ssl_ca_location` — so they cannot disagree ([kubernetes.md](kubernetes.md)
§20.2). **A standalone CLI run** gets the same through
`LOGWEIR_SOURCE_TLS_CA_FILE` / `LOGWEIR_TARGET_TLS_CA_FILE`. Configuring a CA
for only one of the two clients by hand (an image bundle edit, an engine config
edit) produces a working backup and a failing verification, or the reverse, and
neither failure names the trust store as the cause.

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
`logweir.dev/v1alpha1/TrustRoster/default#spec.signingKeys`, the legacy roster
the bootstrap was written against. Bootstrap publishes material but does not
silently authorize it: a cluster administrator explicitly copies it into the
trust that governs each namespace — a `TrustPolicy` key with usage
`EvidenceSigning` (PLAT-19.1), or `TrustRoster/default`'s `signingKeys` where no
policy governs ([kubernetes.md](kubernetes.md) §8, *Trust resolution*). The
reference string itself is unchanged, so readers that parse it keep working.

During a planned rotation, retain the retiring public key alongside the new key
for old-archive verification, stop new signing with the retired private key,
and record the policy-effective time. Routine retirement preserves historical
verification; revocation is a separate policy decision and may deliberately
make historical evidence untrusted. Never infer trust from a public key stored
beside an archive. The overlap is a `TrustPolicy` edit (PLAT-19.1,
[keys.md](keys.md), *The supported procedure*); on a roster-only installation,
rotation of the immutable default roster is still an administrator-coordinated
maintenance window, and rollback must restore the prior roster plus the matching
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

# archive objects — uninstall never deletes these; only a RetentionPolicy in
# mode Enforce ever did, under its own credential (docs/kubernetes.md §7f)
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
