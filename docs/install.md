# Installing Logweir

This is **the** guide for one team's install: install the chart, take the first
backup, restore it and verify the result, choose how restores are approved,
recover onto a clean installation with the kit, then upgrade and roll back.
[kubernetes.md](kubernetes.md) is the operator guide by task,
[the chart reference](../charts/logweir/README.md) lists every Helm value, and
[quickstart.md](quickstart.md) runs a local demo with no cluster. Commands use
the `docker-desktop` context; name your own explicitly.

**Before you install.**

- **Minimum Kubernetes: 1.29.** The thirteen CRDs use CEL validation rules, GA
  at 1.29, and the chart's `kubeVersion: >=1.29.0-0` makes Helm refuse an older
  cluster. The `ValidatingAdmissionPolicy` of *5b* needs 1.30.
- **Helm 3.19 or newer, or 4.x.** A first install with the console needs it:
  older Helm keeps the install-only trust grant after a failed hook, so the
  chart refuses an older Helm by name (*5f*). The chart's gates render with
  Helm 4.0.1.
- **Nodes: linux/amd64 or linux/arm64.** The controller, runner and console
  images, the identity hook's included, are published for both
  ([support-matrix.md, *Architectures*](support-matrix.md#architectures)).
- **Engine: Logweir's build of OSO 0.23.3**, `kafka-backup 0.23.3+logweir.3`
  (build 3, PROD-00.3g's patch 0003;
  [../third_party/kafka-backup-build.env](../third_party/kafka-backup-build.env)),
  inside the runner image. The floor is 0.21.0; an engine below it, such as
  `v0.19.1`, is `unsupported (lever-absent)`, never a fault. A standalone CLI
  needs that binary (`logweir doctor` accepts exactly it): copy it out of the
  runner image or build it with `scripts/engine-source.sh build`.
- **Brokers:** Apache Kafka 3.7.1, 3.9.2, 4.1.2 and 4.3.1 (KRaft) and Confluent
  Platform 8.3.2 are supported; Redpanda is a backup source only; anything else
  is untested ([support-matrix.md, *Brokers*](support-matrix.md#brokers)).

Code, messages and older documents cite this guide by its numbered sections;
*Section numbers before v1* at the end maps them.

---

## Choose an image and installation path

### (a) Published digests

Install the chart ([(c)](#c-the-helm-chart)) with `controllerImage` and
`runnerImage` set to the `@sha256:` references of the run you want. The static
`kubectl apply` bundle `logweir.yaml` was withdrawn in v1
([release notes](release-notes.md), item 76). The
[CI and release runs](https://github.com/VladyslavHaina/logweir/actions) list
the published digests; read the checked-in pins from
`charts/logweir/values.yaml` (`controllerImage`, `:latest` from a checkout) and
`crates/weirkeeper/src/job.rs` (`RUNNER_IMAGE`) rather than copying a build's
digest. `helm install` can succeed while a pod stays in `ImagePullBackOff`, so
check Deployment readiness separately. All published images are
**linux/amd64 and linux/arm64**; an arm64 node pulls its own variant.

#### Verify the images

Publications from PROD-00.2 on are signed keylessly by CI: `images.yml`, run by
`ci.yml`'s `publish` job on a push to `main`, signs each image index and every
platform manifest with cosign, attests the runner's SBOM (SPDX, one per
platform) and records SLSA build provenance. No private key exists. Verify a
digest before you deploy it, with **every** pin below:

```bash
DIGEST=sha256:...           # the published index digest, e.g. the run's runner_digest
PLATFORM_DIGEST=sha256:...  # one platform's manifest in it (docker buildx imagetools inspect)
cosign verify \
  --certificate-identity "https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  --certificate-github-workflow-repository "VladyslavHaina/logweir" \
  --certificate-github-workflow-ref "refs/heads/main" \
  --certificate-github-workflow-trigger "push" \
  "docker.io/vladyslavhaina/logweir@$DIGEST"

# The runner's SBOM, for one platform's manifest digest:
cosign verify-attestation --type spdxjson \
  --certificate-identity "https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  --certificate-github-workflow-repository "VladyslavHaina/logweir" \
  --certificate-github-workflow-ref "refs/heads/main" \
  --certificate-github-workflow-trigger "push" \
  "docker.io/vladyslavhaina/logweir@$PLATFORM_DIGEST"

# SLSA provenance:
gh attestation verify "oci://docker.io/vladyslavhaina/logweir@$DIGEST" \
  --cert-identity "https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main" \
  --repo VladyslavHaina/logweir \
  --source-ref refs/heads/main \
  --cert-oidc-issuer https://token.actions.githubusercontent.com \
  --deny-self-hosted-runners
```

Never use `--certificate-identity-regexp` or give a flag twice: `images.yml` is
a reusable workflow, so only the repository, ref and trigger pins name this
repository's `main` (`scripts/check-cosign-verify.py` refuses a command here
that drops one). A release tags main's publication without rebuilding, so it
carries these signatures. Publications before PROD-00.2 are unsigned.

#### Rolling the engine back

For one release, OSO's own binary is the engine rollback. Build the runner with
the build argument, for linux/amd64 only (OSO publishes nothing else), and point
`runnerImage` at it:

```bash
docker build --platform linux/amd64 --build-arg ENGINE_SOURCE=oso \
  -t <registry>/logweir:<version>-oso .
bash scripts/check-image.sh <registry>/logweir:<version>-oso   # check 8 names OSO's release
docker push <registry>/logweir:<version>-oso
helm upgrade logweir <chart> --reuse-values \
  --set runnerImage=<registry>/logweir:<version>-oso@sha256:<pushed digest>
```

It declares OSO's identity in `/etc/logweir/engine-identity` (`version=0.23.3`),
which every receipt and scorecard signs, and its Jobs need amd64 nodes. Set
`runnerImage` back to return. A runner published before PROD-00.2 declares no
engine and its runs are refused (exit 1, nothing signed): roll the controller
and runner together.

### (b) Local build — **author-only**

```bash
just image && just image-weirkeeper
helm install logweir charts/logweir -n logweir-system --create-namespace \
  -f charts/logweir/examples/author-only.values.yaml
```

Both recipes default to `linux/arm64`; set `LOGWEIR_IMAGE_PLATFORM=linux/amd64`
on an amd64 builder. The values name the local tags and set
`imagePullPolicy: Never`; on kind, load both tags into the nodes first. The
runner image carries `logweir` (its entrypoint) and `logweir-retention`, which
the controller runs for `RetentionPolicy` enforcement; a runner built without
`-p logweir-retention` makes that one Job exit 127, and
`bash scripts/check-chart.sh` refuses such a tree.

Older controllers without the environment override need the exact compiled
runner digest under its shipped repository name. The historical step was:

```bash
docker tag logweir:check docker.io/vladyslavhaina/logweir:v0.1.0
docker inspect --format '{{json .RepoDigests}}' docker.io/vladyslavhaina/logweir:v0.1.0
```

Local BuildKit provenance can change a digest even on a cached rebuild, and
retagging cannot make new bytes match an older compiled digest
([kubernetes.md](kubernetes.md#images) says what `ErrImageNeverPull` means). A
local build is **author-only** and **never satisfies spec §16 clause 1**,
including a local `registry:2` fallback: it does not prove public pullability.

### (c) The Helm chart

**The published chart.** Every publication of the images publishes the chart
beside them as an OCI artifact on Docker Hub
([gates.md](gates.md#versioned-releases)):

| images published as | chart | `appVersion` |
|---|---|---|
| `sha-<commit>` (every push to `main`) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version 0.1.0-sha-<commit>` | `sha-<commit>` |
| `v<X.Y.Z>` or a pre-release such as `v0.2.0-rc.1` (the tagged commit's `sha-` digests) | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version <X.Y.Z>` (`0.2.0-rc.1`) | `v<X.Y.Z>` |

Its `controllerImage`, `runnerImage` and `api.console.image` are that tag (a
release's pinned by digest), so it installs exactly that commit's images:

```bash
helm upgrade --install logweir oci://registry-1.docker.io/vladyslavhaina/logweir-chart \
  --version 0.1.0-sha-<commit> -n logweir-system --create-namespace --wait --timeout 10m
helm show values oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version 0.1.0-sha-<commit>
```

Always pass `--version` (a `main` version is a pre-release). The package is
`logweir-chart` because `vladyslavhaina/logweir` is the runner image; a
release's package is also a GitHub Release asset. **On first publication,
`vladyslavhaina/logweir-chart` must be Public in Docker Hub**: the publication
step pulls it back anonymously and fails closed until it is Public and re-run.

**From a checkout** the image defaults are `:latest`:

```bash
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m
```

`identity.bootstrapImage` is pinned in `charts/logweir/values.yaml` to a
reviewed runner digest that carries the identity CLI, so neither path needs a
local key or an image hash.
That pinned runner (rev `d4e32956`) is an index of linux/amd64 and
linux/arm64, like every image published since PROD-00.2: the hook runs natively
on either node architecture.

On every install, upgrade and supported rollback a short-lived hook
initializes or validates the retained signing identity; Helm never renders private key
bytes. The chart creates no approver key, and Helm installs CRDs once and never
upgrades them.

#### Argo CD and other offline renders

Managed identity installation is a **connected Helm operation**. `helm install`,
`helm upgrade` and `helm rollback` look up the retained Secret, public
ConfigMap, authorized-namespace copies, singleton owner and Kubernetes API
endpoints, and render the empty retained objects and the fresh install's trust
grant only when the objects are absent. A render that cannot look up
(`helm template`, `helm lint`, `--dry-run=client`, and Argo CD, which renders
with `helm template`) is **refused by name**, because it would render them on
every apply: Argo CD runs the empty objects as PreSync hooks and deletes and
re-creates each one on every sync, so the filled signing Secret would be
replaced by an empty one and the next hook would generate a new key. Preview
against the cluster with `--dry-run=server`.

For Argo CD or `helm template | kubectl apply`, set
`identity.renderedOffline: true`. The chart then renders no retained object and
no trust grant; the hook Jobs still run and fill the objects you create once,
before the first sync. For the default names (the console pair only with
`api.console.enabled`, and one Secret in each
`identity.authorizedRunnerNamespaces` entry):

```bash
kubectl --context <ctx> -n <release-ns> create secret generic logweir-signing-key
kubectl --context <ctx> -n <release-ns> create configmap logweir-signing-trust
kubectl --context <ctx> -n <release-ns> create secret generic logweir-console-confirmation
kubectl --context <ctx> -n <release-ns> create configmap logweir-console-trust
kubectl --context <ctx> -n <runner-ns> create secret generic logweir-signing-key
```

The renderer never manages these objects, so no sync, re-apply or prune
touches them; back them up like any identity
([Back up and recover the installation identity](#back-up-and-recover-the-installation-identity)).
That holds only for objects made with the `kubectl create` commands above: one
an earlier `kubectl apply` made keeps
`kubectl.kubernetes.io/last-applied-configuration`, and `kubectl apply --prune`
deletes it once the render drops it, so remove that annotation first.
With `identity.externalSecret`, the hook adopts that key into the
`logweir-signing-key` you created. Such an install starts **strict**: no
`TrustPolicy` is created and no namespace defaults to confirm, so trust the
console key yourself ([§5f](#5f-approval-modes-confirm-two-person-strict-approvalpolicy)).
An offline render discovers no Kubernetes API address either: list them in
`identity.kubernetesApiCIDRs`. Under `kubectl apply`, delete the finished
`<release>-identity-bootstrap` Job before applying a newer render, because a
Job's template is immutable; Argo CD re-runs the hooks on every sync.

`identity.renderedOffline: false` renders what `helm install` would, without
asking the cluster. It is for previews (this repository's chart gates), never
for output you apply.

### (d) Bring your own registry

Build the controller and runner images, plus `logweir-console` for the console,
and push them where your nodes can pull. Build the runner for the nodes'
architecture (`LOGWEIR_IMAGE_PLATFORM=linux/amd64` or `linux/arm64`); the
controller builds natively, so build an amd64 controller on amd64. This ECR
example needs `ACCOUNT` and `REGION`:

```bash
aws ecr create-repository --repository-name logweir/weirkeeper   # once, per image
aws ecr create-repository --repository-name logweir/logweir
aws ecr create-repository --repository-name logweir/logweir-console
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
just image && just image-weirkeeper && just image-console
R="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/logweir"
for i in weirkeeper logweir logweir-console; do docker tag "$i:check" "$R/$i:v0.1.0"; docker push "$R/$i:v0.1.0"; done
BOOTSTRAP_DIGEST=$(docker buildx imagetools inspect "$R/logweir:v0.1.0" \
  --format '{{json .Manifest}}' | jq -er .digest)
docker run --rm "$R/logweir@$BOOTSTRAP_DIGEST" identity bootstrap --help
helm upgrade --install logweir charts/logweir -n logweir-system --create-namespace \
  --set controllerImage="$R/weirkeeper:v0.1.0" \
  --set runnerImage="$R/logweir:v0.1.0" \
  --set-string identity.bootstrapImage="$R/logweir@$BOOTSTRAP_DIGEST" \
  --set identity.bootstrapImagePullPolicy=IfNotPresent \
  --set api.console.image="$R/logweir-console:v0.1.0" \
  --set imagePullPolicy=IfNotPresent \
  --set runnerImagePullPolicy=IfNotPresent \
  --set 'imagePullSecrets[0].name=my-regcred'
```

`imagePullSecrets` reaches the controller and runner ServiceAccounts, so runner
Jobs inherit it; with an ECR node role holding `ecr:GetAuthorizationToken`,
leave it off. Visibility and pull quotas are the registry account's.

### Release coordinator: re-pin bootstrap bytes

Not an operator step. When the identity CLI or the hook's arguments change,
copy the images job's `runner_digest` into `identity.bootstrapImage`
(`docker.io/<namespace>/logweir@sha256:<digest>`), keep
`allowMutableBootstrapImageForDevelopment: false`, and rerun
`just chart-check`, which requires the runner repository by digest and
refuses an emptied value. Never pin a runner older than the identity CLI. Save
the output of

```bash
docker run --rm --platform linux/amd64 docker.io/<namespace>/logweir@sha256:<reviewed-digest> \
  identity bootstrap --help
```

with a `# digest:` line as `crates/logweir/tests/fixtures/bootstrap-image-help.txt`,
which `chart_lint` holds every render to. New hook flags land in two steps: the
merge passes them only while `identity.bootstrapFeatures.consoleKey` is `true`,
and after CI publishes `sha-<merge>` ONE commit re-pins, refreshes the fixture
and flips it (done for PROD-16.1: rev `2fe8d907`, value `true`). PROD-14.1
re-pinned it to rev `d4e32956`, an index of linux/amd64 and linux/arm64.

### Images and restricted registries

With both demo flags off, the chart pulls only the controller, runner, console
and identity-hook images; the demos add Apache Kafka (`demoKafka.image`) and
unmodified rebuilds of MinIO and mc (`minio.image`, `minio.mcImage`;
[third_party/minio-mirror/README.md](../third_party/minio-mirror/README.md)).
Without public registry access, mirror them, or use the recovery kit's
`docker save` archives (*Recover onto a clean installation with the kit*); keep
the upstream licences and notices with any image you redistribute.

---

## Install

### The install itself

Turn the console on in this same first install, so the install starts in
**confirm** and its first restore needs no key (*5f*). Create the release
namespace and the console's key Secret first (*5e*), then install the published
chart ([(c)](#c-the-helm-chart)) with a console example as your values:
`charts/logweir/examples/console.values.yaml` (`localAdmin`, one administrator
over `kubectl port-forward`) or `charts/logweir/examples/console-shared.values.yaml`
(`shared`, sign-in through your OIDC provider):

```bash
helm upgrade --install logweir oci://registry-1.docker.io/vladyslavhaina/logweir-chart \
  --version <version> -n logweir-system --create-namespace --wait --timeout 10m \
  -f <your values>
```

In the values, list every namespace backups and restores run in under
`identity.authorizedRunnerNamespaces` (each must exist), the clusters a restore
may write into under `identity.installationTrust.allowedTargetClusterIds`, and
`retention.enabled: true` only if you will ever enforce retention.

**Check:** the controller Deployment is ready and `logweir-signing-trust`
carries a `key-id` (*1*). Back up the identity at once (*Back up and recover the
installation identity*): without it nothing can sign, and without its public
half old archives cannot be verified.

### Before any custom resource

Once the identity hook has finished, run the preflight. It reads the cluster,
changes nothing, checks presence only, and exits **1 naming the first absent
Secret** (*3*):

```bash
just check-secrets logweir-system; echo "rc=$?"
```

### 1. Installation signer and approver identity

The install generates a P-256 installation signer inside a
short-lived bootstrap Job, with no local OpenSSL command or key file. The Job
fills the retained Secret `logweir-signing-key` and publishes only the key id,
algorithm and public key in the retained ConfigMap `logweir-signing-trust`
(read the public half there, never from the Secret):

```bash
kubectl --context docker-desktop -n logweir-system get configmap logweir-signing-trust \
  -o jsonpath='{.data.key-id}{"\n"}{.data.algorithm}{"\n"}{.data.trust-reference}{"\n"}'
```

Only the bootstrap/distributor **processes and their projected API tokens** are
short-lived. Their ServiceAccounts, name-scoped Roles and RoleBindings are
ordinary persistent release resources, so a later upgrade or hook retry runs
without widening authority; Helm does not automatically revoke these RBAC grants.

To adopt an organization's P-256 or Ed25519 PKCS#8 PEM key, create its source
Secret before the first install; a missing or malformed source fails the
install, and an established identity must match it. Keep the source reachable
while the values stay set, or clear both after checking the published key id:

```bash
kubectl --context docker-desktop -n logweir-system create secret generic \
  company-logweir-signer --from-file=identity.pem=/secure/path/identity.pem
helm upgrade --install logweir charts/logweir -n logweir-system --create-namespace \
  --set identity.externalSecret.name=company-logweir-signer \
  --set identity.externalSecret.key=identity.pem
```

With a console, the same hook generates the console's `ConsoleConfirmation` key
into the retained Secret `logweir-console-confirmation` (public half in
`logweir-console-trust`), under the same rules. An approver key is needed only
for a **strict** namespace; its private half never enters the cluster:

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out approver.pem && openssl pkey -in approver.pem -pubout -out approver.pub.pem
```

### 2. The cluster-scoped `TrustRoster`, whose name is fixed

A fresh install with the console needs no roster: its hook creates the
`TrustPolicy` `logweir-installation` (*5f*), and a `TrustPolicy` is the current
way to say whose keys a namespace trusts ([keys.md](keys.md)). The roster is the
legacy fallback for a namespace no policy governs; a roster that exists before
the first install makes the hook create nothing. Fill
`config/samples/trustroster.yaml` (the approver's key from `approver.pub.pem`,
the signer's `key-id` and `signing.pub.pem` from `logweir-signing-trust`) and
apply it as a cluster administrator:

```bash
kubectl --context docker-desktop apply -f config/samples/trustroster.yaml
openssl pkey -pubin -in approver.pub.pem -outform DER | openssl dgst -sha256   # the approver's keyId
```

```yaml
kind: TrustRoster
metadata:
  name: default   # FIXED: `ROSTER_NAME` is the literal "default"
```

**The name is `default` and nothing reads any other name**: a roster called
anything else is consulted by no approval check (`RosterNotFound`). A declared
id that does not match its key is `KeyIdNotInRoster`. `spec` is immutable, so
changing keys replaces the roster and briefly interrupts approval checks.

### 3. The four current Secrets

Three live in the namespace of your `Backup`, `Restore` and `KafkaCluster`
objects; the fourth is the controller's. Names and data keys are the code's.

**1. `logweir-signing-key`**: the bootstrap creates it in the release namespace
and the runner projects its `signing.pem`; never create or replace it. Declare
every other runner namespace (it must exist), for example
`--set-string 'identity.authorizedRunnerNamespaces[0]=recoveries'`: the chart
renders `logweir-runner`, scoped distribution RBAC and the NetworkPolicies
there, and a short-lived hook copies the exact established bytes. A namespace
that already holds a different signer fails closed.
Never mint a per-namespace signer. List it under `api.namespaces` too for the
console.

**2. The per-cluster SCRAM credential**: named by
`KafkaCluster.spec.auth.secretRef`, data key `password`
(`TARGET_PASSWORD_SECRET_KEY`), username in `spec.auth.username`, bound to its
connection ([kubernetes.md](kubernetes.md#credential-binding); the console does
this for you):

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  kafka-scram --from-literal=password="$KAFKA_PASSWORD"
```

**3. `logweir-s3`**: the runner's archive credential, read by `object_store`'s
own chain (`~/.aws`, `AWS_PROFILE` and SSO are unsupported):

```bash
kubectl --context docker-desktop -n <namespace> create secret generic \
  logweir-s3 \
  --from-literal=access-key-id="$AWS_ACCESS_KEY_ID" \
  --from-literal=secret-access-key="$AWS_SECRET_ACCESS_KEY"
```

**4. `logweir-evidence-ro`**: the controller's **read-only** evidence
credential, another principal. Neither it nor the runner can delete:

```bash
kubectl --context docker-desktop -n logweir-system create secret generic \
  logweir-evidence-ro \
  --from-literal=access-key-id="$EVIDENCE_RO_ACCESS_KEY_ID" \
  --from-literal=secret-access-key="$EVIDENCE_RO_SECRET_ACCESS_KEY"
```

The controller reads it at start: after creating or rotating it,
`kubectl --context docker-desktop -n logweir-system rollout restart deploy/weirkeeper`,
and runs it could not verify are read again. `config/samples/secrets.yaml`
carries the same four. Each Restore gets its own immutable
`<restore>-approval-bundle`; keep the legacy `logweir-approval-bundle` only
while a Job an older controller created still references it.

### 3.11 Object-store permissions, per destination role

A `BackupDestination`'s four grants are separable on purpose: the principal
that writes an archive is not the one that reads evidence back. Measured on
2026-09-21 against a deny-by-default MinIO ([kubernetes.md](kubernetes.md) §7a
has the method; MinIO's action names are S3's):

| Role | Actions | Resources |
|---|---|---|
| `archiveWrite` | `s3:ListBucket` (condition `s3:prefix` in `<prefix>/*`), `s3:GetObject`, `s3:PutObject` | the bucket for the listing; `arn:aws:s3:::<bucket>/<prefix>/*` for both object actions, plus `s3:PutObject` on `arn:aws:s3:::<bucket>/logweir/*` for the execution claim, the receipt and the catalog record — the claim is a conditional create (`If-None-Match: *`), so the store must honour it or every backup exits 4 `ExecutionClaimUnproven` ([why](formats/backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id)) |
| `archiveRead` | `s3:ListBucket` (condition `s3:prefix` in `<prefix>/*`), `s3:GetObject`; to restore a catalog point, also `s3:GetObject` on its receipt and, on a versioned bucket, `s3:GetObjectVersion` | the bucket; `arn:aws:s3:::<bucket>/<prefix>/*`; the receipt is under `arn:aws:s3:::<bucket>/logweir/backups/*`; `s3:GetObjectVersion` on `arn:aws:s3:::<bucket>/<prefix>/*` |
| `evidenceWrite` | `s3:PutObject` (conditional create) | `arn:aws:s3:::<bucket>/logweir/*` |
| `evidenceRead` | `s3:GetObject` | `arn:aws:s3:::<bucket>/logweir/*` |
| write probe (opt-in; run as the `evidenceWrite` grant, already inside its `logweir/*`) | `s3:PutObject` | `arn:aws:s3:::<bucket>/logweir/readiness/*` |
| `RecoveryCatalog` sync | `s3:ListBucket` (condition `s3:prefix` in `logweir/*`), `s3:GetObject`; on a versioned bucket, `s3:GetObjectVersion` | the bucket; `arn:aws:s3:::<bucket>/<prefix>/*` AND `arn:aws:s3:::<bucket>/logweir/*`; `s3:GetObjectVersion` on `arn:aws:s3:::<bucket>/<prefix>/*` |

- `s3:AbortMultipartUpload` and `s3:GetBucketLocation` are in no row: removing
  each broke nothing measured. Grant them if your object sizes differ.
- `s3:GetObjectVersion` reads a pinned manifest version on a versioned bucket
  [UNVERIFIED — needs a real AWS S3 bucket and a credential source]; without it
  those points read `Unreadable`. MinIO serves it under `s3:GetObject` (measured
  2026-10-09).
- **The catalog sync row is not `archiveRead`**: widen `archiveRead` to it, or
  the catalog publishes no view.
- **No role is ever granted `s3:DeleteObject`.** Only `logweir-retention`
  deletes, under `spec.enforcement.credentialSecretRef` (`s3:ListBucket` with
  `s3:prefix` in `<prefix>/*`, `s3:GetObject` and `s3:DeleteObject` on
  `arn:aws:s3:::<bucket>/<prefix>/*`); its `s3:GetObject` is the HEAD that
  refuses a versioned bucket (`VersionedBucket`; `VersionProbeRefused` without
  it). [UNVERIFIED — the grant with s3:GetObject is re-measured by U6/retention-enforcer at the next lab refresh.]
- Give `evidenceRead` `s3:ListBucket` if "absent" must mean absent; without it
  a missing key reads as `AccessDenied` and presence is `Unknown`.
- **Absent grants do not widen**: `archiveRead` and `evidenceWrite` absent mean
  `archiveWrite` is used; `evidenceRead` absent means verification is
  `NotAttempted`. `evidenceRead: ArchiveReadGrant` needs an explicit
  `archiveRead`; `ControllerIdentity` must be listed in
  `evidence.controllerIdentityLocations` (else
  `ControllerIdentityNotAllowlisted`). The `lwc-ev-<20 hex>` Job that reads
  evidence is in [kubernetes.md](kubernetes.md#evidence-fetch-how-a-pod-only-grant-is-read).

### 4. Runner namespace prerequisites: Helm-managed or low-level manifest

On the Helm path `identity.authorizedRunnerNamespaces` manages the runner
account, the signer copy and the NetworkPolicies; never apply the files below
over them. Only for a namespace the chart does not manage, with
`identity.enabled=false` (the low-level path), apply the account and provision
the signer yourself:

```bash
kubectl --context docker-desktop -n <namespace> \
  apply -f config/samples/backup-runner-serviceaccount.yaml
```

Every runner Job names `logweir-runner`, granted no verb and given no token (a
PodSpec naming none would get `default`).
For the low-level path, apply it once per namespace that will run jobs. Retention
enforcement also needs `logweir-retention` (`retention.enabled`, or
`config/samples/retention-serviceaccount.yaml`); without it the Job's pod is
never admitted and the policy cannot say why.

### 5. Binding the five human roles

The chart ships `logweir-viewer`, `logweir-operator`, `logweir-approver`,
`logweir-trust-admin` and `logweir-retention-admin` **unbound**:

```bash
kubectl --context docker-desktop create rolebinding logweir-operator \
  --clusterrole=logweir-operator --user=<user> -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-approver \
  --clusterrole=logweir-approver --user=<someone-else> -n <namespace>
kubectl --context docker-desktop create clusterrolebinding logweir-trust-admin \
  --clusterrole=logweir-trust-admin --user=<security-owner>
kubectl --context docker-desktop create rolebinding logweir-retention-admin \
  --clusterrole=logweir-retention-admin --user=<storage-owner> -n <namespace>
```

| Decision | Who |
|---|---|
| Install, upgrade, CRDs, controller scope, approval modes | the installation administrator, in Helm values |
| Whose keys a namespace trusts | `logweir-trust-admin`: cluster-scoped, the only write verb on `trustpolicies`, no `delete` |
| Connections, destinations, schedules, backups, restore requests | `logweir-operator` (console role Operator) |
| Approving one restore | the requester (confirm), another person (two-person), or an approver with a key (strict) |
| Whether archive objects are deleted | `logweir-retention-admin` (the only write verb on `retentionpolicies`, no `delete`), plus an administrator's approval of each plan digest |

Bind them to **different** people: an operator who could edit trust could
approve their own restore. `logweir-approver` is `create` on `approvals` and
read on `preflights`, nothing else. `logweir-viewer` reads all thirteen kinds
but no ConfigMap. Withdrawing trust is an edit ([keys.md](keys.md), *Replacing a
`TrustPolicy` safely*), and turning enforcement off is `mode: Report`.

### 5d. The console/API principal

`api.enabled` renders `<release>-api`, bound in `api.namespaces`. The API
authenticates every request and narrows each actor itself, through one sealed
adapter (`crates/logweir-api/src/kube.rs`):

| Grant | Why |
|---|---|
| `get`/`list`/`create` on the eight product kinds | projections and collection POSTs, including confirmations and Approvals |
| `patch` on `backupschedules`, `backupdestinations`, `topicdiscoveries`, `preflights` | suspension, policy edits, access rotation, cancellation |
| `get`/`list` on `protectionpolicies`, `recoverycatalogs`, `retentionpolicies`; `create` on `recoverycatalogs` | D3's read surfaces; "connect existing archive" |
| `get`/`list` on `trustpolicies`; `get` on `trustrosters` named `default` | the keys view and a readiness check's trust referent |
| `get` on `configmaps` | a check's result and a catalog's pages, verified by owner UID, immutability and digest |
| `create` on `secrets` | the write-only credential entry |

No `watch`, no `delete`, no read on `secrets`, no `list` on `configmaps`, no
write on `trustpolicies`. `bash scripts/check-chart.sh` checks
`kubectl auth can-i` for every pair, both ways, against every render.

### 5e. Running the console in the cluster (`api.console.enabled`)

`logweir-console` is the `logweir-api` binary and the page of `ui/`
(`scripts/check-image-api.sh` compares them). It needs `api.enabled`, and
`api.console.mode` has no default. **Create the key Secret first; the chart
does not generate it.** `localAdmin` needs `cursor.key` (32 raw bytes):

```console
$ head -c 32 /dev/urandom > cursor.key
$ kubectl --context docker-desktop -n logweir-system \
    create secret generic logweir-console-keys --from-file=cursor.key
$ rm cursor.key
```

`shared` needs two files of two lines, whose `version:` is `api.console.keyVersion`:

```console
$ printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > session.key
$ printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > cursor.key
$ kubectl --context docker-desktop -n logweir-system \
    create secret generic logweir-console-keys \
    --from-file=session.key --from-file=cursor.key
```

**`localAdmin`: one administrator, for a lab or break-glass.** It binds only
`127.0.0.1`, renders no Service, Ingress or probes (a refused configuration is
a `CrashLoopBackOff`, exit 2, naming the field; `shared` binds the Pod IP and
has `/healthz` and `/readyz`), and confirms as
`urn:logweir:local-admin#admin`; it cannot do two-person. **`create
pods/portforward` in this namespace is equivalent to full console administrator
authority over every bound namespace.** The local port must be `8484`, which
the rendered `publicOrigin` carries:

```console
$ helm upgrade logweir charts/logweir -n logweir-system --reuse-values \
    --set api.enabled=true --set api.console.enabled=true \
    --set api.console.mode=localAdmin \
    --set api.console.keySecret=logweir-console-keys
$ kubectl --context docker-desktop -n logweir-system \
    port-forward deploy/logweir-api 8484:8484
$ open http://127.0.0.1:8484/ui/
```

**`shared`: the SSO console**, the only mode a Service or Ingress may expose.
`charts/logweir/examples/console-shared.values.yaml` is the whole shape: the
OIDC issuer and client (`api.console.oidc.*`, the client secret's Secret key
`clientSecret`), an exact `https://` `publicBaseUrl`, an Ingress with a TLS
Secret, and `api.console.roles.bindings` (`role`, `namespace`, `groups`). It
needs `controller.watchNamespaces` without the release namespace, so the
controller cannot start a Job that mounts the console's keys (residual O1):
move objects out of the release namespace, prepare each namespace as in step 4,
and upgrade with the list (`[]` rolls back). With an Ingress set
`trustedProxyService` (preferred) or `trustedProxyCidrs` (the ingress pods'
range, at most `/16`), or `trustedProxy: none` by name, and
`requireTrustedProxy: true`. Disable the console before emptying the list.
[The chart reference](../charts/logweir/README.md) lists every render-time
refusal; a refused render changes nothing:

| refusal (the render names it) | fix |
|---|---|
| `api.console.mode=shared requires controller.watchNamespaces` | list the execution namespaces |
| `controller.watchNamespaces includes the release namespace` | move Backups/Restores out of it and drop it |
| `ui.* is withdrawn` | remove the `ui` block (release-notes item 75) |
| `api.console.roles.bindings names namespace … which is not in controller.watchNamespaces` | add the namespace, or remove the binding |
| `api.console.trustedProxyCidrs names … wider than /16` (only with `requireTrustedProxy`) | narrow it to the ingress controller's own pod range |

**Roles come from the ID token's `groups` claim alone** (scopes `openid profile
groups`). A user with no bound group sees no namespace (`404`). Per provider:

- **Dex:** the connector supplies groups (LDAP `groupSearch`, GitHub
  `orgs`/`teams`, an OIDC upstream with `insecureEnableGroups: true`);
  `staticPasswords` users have none.
- **Keycloak:** a client scope `groups` with a *Group Membership* mapper (claim
  `groups`, *Full group path* off, *Add to ID token* on), a default scope.
- **Microsoft Entra ID:** *Add groups claim* for the ID token; bind group object
  IDs, and emit only *Groups assigned to the application* (over 200 yields none).
- **Okta:** a `groups` claim in the ID token, value type *Groups*, filtered such
  as *Matches regex* `^logweir-`.

`[UNVERIFIED — none of the four lines was run against this console; each follows its provider's own documentation]`

Each request writes one audit line to stdout ([audit-log.md](audit-log.md)
ships them). The rendered ConfigMap carries Secret paths, never a value.
Setting `api.console.enabled=false` removes the console and stops nothing else.

### 5f. Approval modes: confirm, two-person, strict (`approvalPolicy.*`)

**Confirm**: one person clicks Create in the console, no key (internal
`Ordinary`). **Two-person**: a second person approves in the shared console, no
key (`Governed`, `approverSignature: Console`). **Strict**: an approver's
personal key (`Governed`, or `legacy-governed-v1`).
[kubernetes.md](kubernetes.md#approvals) has the mechanics.

**A fresh install with the console needs no step here.** On a first `helm
install` with Helm 3.19+ or 4.x, a console, the managed identity and
`identity.bootstrapFeatures.consoleKey: true` (the default), the hook generates
the console key, creates ONE default `TrustPolicy`
(`identity.installationTrust.policyName`, default `logweir-installation`) with
exactly the signer (`EvidenceSigning`) and the console key
(`ConsoleConfirmation`), and marks the install so every namespace without its
own policy is **confirm**. A trust administrator edits the target clusters
later: `kubectl patch trustpolicy logweir-installation --type merge -p
'{"spec":{"allowedTargetClusterIds":["<cluster id>"]}}'`.

```bash
kubectl --context <ctx> get trustpolicy logweir-installation        # DEFAULT true, KEYS 2
kubectl --context <ctx> -n <release-ns> get configmap logweir-signing-trust \
  -o jsonpath='{.metadata.annotations.logweir\.dev/approval-default}{"\n"}'
# confirm;policy=logweir-installation;uid=…;signing=…;console=…
```

The hook does this only when the cluster has **no `TrustPolicy` and no
`TrustRoster/default`**, and only for a console key it generated (a hand-made
one is adopted and trusted nowhere). Otherwise it logs `installation-trust
skipped:…` with the key ids to add, and the install starts strict, as does one
without a console or with `identity.externalSecret`, and one whose first hook
run failed after generating the identity (a later run does not finish it; opt in
as below). Creating the
policy needs `create` on `trustpolicies`, so `<release>-identity-trust` exists
only as a `post-install` hook Helm 3.19+ deletes when the hooks finish, which
the hook also revokes itself; under older Helm the chart refuses
(`identity.installationTrust needs Helm 3.19.0 or newer`), or set
`identity.installationTrust.enabled=false`. **After a failed first install**,
remove it at once (`helm uninstall` does not):

```bash
kubectl --context <ctx> delete clusterrolebinding,clusterrole <release>-identity-trust --ignore-not-found
kubectl --context <ctx> get clusterrolebinding <release>-identity-trust   # NotFound
```

**Choosing a mode.** `approvalPolicy.default: strict` keeps unbound namespaces
on `legacy-governed-v1`; `approvalPolicy.default: confirm` (with
`allowOrdinaryConfirmation: true`) makes them confirm. `approvalPolicy.policies`
(`mode: confirm`, `two-person` or `strict`) and `approvalPolicy.namespaces` bind
namespaces, and an explicit binding wins
(`charts/logweir/examples/approval-policy.values.yaml`). **Two-person** needs
the shared console:

```yaml
approvalPolicy:
  policies:
    - name: prod-pair
      mode: two-person
      maxAgeSeconds: 3600      # how long a request waits for its approver
  namespaces:
    prod: prod-pair
```

Bind the requester Operator and the approver **Approver**
(`api.console.roles.bindings`; an Administrator is not an Approver unless bound
as one); nobody approves their own request, and it cannot be the default. Whoever controls the console, its key or the identity provider
can produce both halves (SECURITY.md); bind such a namespace `strict`. For
**strict**, put each approver's `GovernedApproval` key, `principal.id` = their
`<issuer>#<subject>`, on the governing policy (`docs/keys.md`, *Key usage
separation*).

**An install from before this release keeps its approval.** To opt it into
confirm: upgrade (CRDs, controller and runner, then the console) and read the
console key from `logweir-console-trust`; as `logweir-trust-admin`, add it to
the governing `TrustPolicy` (`usages: [ConsoleConfirmation]`, `state: Active`,
`principal.id` such as `console:<release-ns>/logweir-console-confirmation`);
then set `approvalPolicy.allowOrdinaryConfirmation: true` and
`approvalPolicy.default: confirm`. A console naming its own key Secret
(`approvalPolicy.confirmationKeySecret`) manages it by hand.
`identity.bootstrapFeatures.consoleKey` moves with the bootstrap image's re-pin:
`true` with an older `identity.bootstrapImage` fails the hook, and with `false`
the chart manages no console key.

### 5a. The installation policy `ConfigMap`

The optional `weirkeeper-policy` (key `policy.json`, release namespace) is
rendered from `values.yaml` ([kubernetes.md](kubernetes.md#weirkeeper-policy)).
Only an attestation there makes `visibility.state: attestedComplete`. A document
the controller refuses fails closed (`configuration.policy notReady
PolicyUnreadable`) and logs the rule:
`kubectl --context docker-desktop -n logweir-system logs deploy/weirkeeper | grep REFUSED`.

### 5b. Fence the console's `create secrets` (Kubernetes 1.30+)

The console stamps its Secrets with a `logweir.dev/…` type and
`app.kubernetes.io/managed-by: logweir`, so a `ValidatingAdmissionPolicy` can
require both (or edit and apply
`config/samples/console-credential-admission-policy.yaml`):

```bash
helm upgrade --kube-context docker-desktop logweir charts/logweir \
  --set admissionPolicy.enabled=true \
  --set admissionPolicy.consoleServiceAccountName=logweir-api
```

It is off by default because the floor is 1.29.
[UNVERIFIED — neither document has been applied to a live API server]

### 5c. Notification sinks

Withdrawn in v1 (release-notes item 74): a values file that sets `notify` is
refused at render.

### The samples, applied second

Only after the preflight exits 0:

```bash
kubectl --context docker-desktop -n <namespace> apply -f config/samples/kafkacluster.yaml
kubectl --context docker-desktop -n <namespace> apply -f config/samples/backupschedule.yaml
```

NetworkPolicy enforcement is CNI-specific, and Docker Desktop does not enforce
it. Connected Helm discovers `kubernetes.default`'s addresses; offline rendering
discovers none, and the default `kube-system/component=kube-apiserver` selector
rarely reaches a hosted control plane. There, supply every API destination as an exact
IPv4 `/32` or IPv6 `/128` in `identity.kubernetesApiCIDRs`; the bootstrap
policy allows them only on TCP 443
and 6443. Other API ports are unsupported.

---

## The first backup

In the console, as an Operator:

1. **A connection.** Create the Kafka credential Secret (or enter it once in the
   form), then the connection. A private CA (`auth.tlsCa`) or a non-default
   Secret key needs a `kubectl`-made connection (*Two clients, two trust
   stores*). *Test connection* authorises nothing by itself.
2. **A destination** with the grants of *3.11*. **Give it an `evidenceRead`
   grant**, or every run is `unverified: not attempted`. *Test access* checks
   each role.
3. **A schedule**: the source, named topics or all user topics, the cadence and
   the destination. An edit never reaches a run that exists.
4. **Run first backup now**; a slot due before the schedule existed never fires.

**Check:** the run reaches `Succeeded` with *verified by weirkeeper … against
key …* and the window it covered.

## The first restore, and verifying it

1. On the schedule's page, *Restore this point* on the row you want.
2. Choose the target connection, the topics, the new names and a point in time
   inside the covered window; restores only ever write **new** topics. The
   **replication factor** defaults to the source's (capped at the target's
   brokers) when a catalog records it (receipts of format 1.3.0 or later), else
   to the target's broker count (at most 3) from a *Discover topics* the
   controller has not yet collected (a day by default; past its 15-minute
   freshness the count refuses no factor), else to 1. This factor can differ
   from the source's, and the target's storage follows it: a topic the source
   kept at replication factor 1, restored at 3, takes three times the storage it
   took there, with the replication traffic on top; a factor below the source's
   keeps fewer copies than the source had.
3. Run the readiness check. Once one ran, Create waits for it; it is advisory,
   so with no check at all Create stays enabled, beside a warning. A factor the
   brokers cannot hold is then refused when the runner creates the topics: the
   Restore ends `Failed` with `exitCode 1` / `operational`, no signed result and
   nothing restored, and `kubectl logs job/<Restore name>` names
   `InvalidReplicationFactor`. A point with no saved destination (every point
   v0.1.5 wrote) is checked against its inline archive the same way; keep its
   evidence in the archive's bucket, where the controller verifies it
   ([kubernetes.md](kubernetes.md) §15.1a, §21.8).
4. Approve it: in **confirm**, *Create the Restore* is the approval; in
   **two-person**, an Approver clicks *Approve this Restore*; in **strict**, the
   approver runs `logweir drill approve … --subject-kind Restore` (or `logweir
   drill countersign`) with the release's CLI
   ([kubernetes.md](kubernetes.md#strict-approving-out-of-band)).

**Check:** the `Approval` reads `Verified=True` and the `Restore` reaches
`Succeeded`. The badge is green only when the signed scorecard verified under a
trusted key **and** the outcome is `pass`; *signed before that key was retired*
is a pass, *recorded before revocation* never is
([kubernetes.md](kubernetes.md#the-verdict)). The record counts describe a
sample, not the total written. Check it without Logweir: fetch the scorecard and
its `.sig` from the `Restore`'s `status.evidence` keys and run
`python3 docs/verify_scorecard.py` with the public key from
`logweir-signing-trust` ([verify-a-scorecard.md](verify-a-scorecard.md)).

| The console says | The field behind it | What it means |
|---|---|---|
| *verified by weirkeeper at T against key K* | `status.evidence.verification.result: Valid`, `trust.basis` `Current` or `Historical` (or no `trust` block), plus `exitCode 0` or `outcome pass` | the green badge |
| *(signed before that key was retired)* | `trust.basis: Historical` | a pass; the key was valid when it signed |
| *unverified: invalid* / *not attempted* / *untrusted signer* | `result: Invalid` / `NotAttempted` / `Untrusted` | a fact about the **document** / the **controller** (no credential, object or trust material) / the **signer** |
| *recorded before revocation* | `trust.basis: RecordedBeforeRevocation` | never green; a compromise-revoked key signed it |
| *pending* | `result: Pending` | the evidence fetch has not answered yet |
| *records verified in the sampled window* | `Restore.status.completion.recordsRestored` (`sample.records_restored`) | read back from the new topics inside the sampled window: not the total written, not the matching count |
| *records sampled and matching* | `completion.recordsSampledMatching` | how many sampled records matched byte for byte |
| verification scope `sampled` / `degraded` / `none` | `verificationScope.level` | how records were compared; `complete` is not a level |
| *coverage: sampled* / *complete, covered* / *complete, NOT covered -- not a pass* | `Restore.status.integrity.coverage`, `.complete.covered` | how MUCH was compared; absent is never complete, and `covered: false` is never a pass |
| *Visible user topics only — completeness not established* / *All user topics (attested complete)* | `status.selection.coverage` `VisibleUserTopicsOnly` / `AllUserTopicsAttested` | a dynamic run saw only what its principal could see / an administrator attested completeness |
| catalog availability / verification | `Available`…`Partial` / `Verified`…`NotAttempted` | two axes; *selectable* needs both |
| protection health | `Healthy`, `AtRisk`, `Stale`, `Unprotected`, `Unknown` | `Unknown` is never healthy and never a failure |
| retention *removes nothing* / *an isolated worker deletes* | `RetentionPolicy.status.enforcement` `RecommendationOnly` / `LogweirWorker`; `status.guarantees.*` | nothing is deleted / an approved plan is enforced; who enforces each guarantee |
| key evaluation `unknown` | a `TrustPolicy` with no fresh status | never read as valid |

**Time to the first verified restore: PLACEHOLDER, to be measured by PROD-14.1's
clean install** (from `helm install` to the first `Restore`'s
`status.evidence.verification.verifiedAt`, with the independent re-check's lag).

Retention starts in `mode: Report` and deletes nothing until a retention
administrator enforces an approved plan ([kubernetes.md](kubernetes.md#retention)).

## Recover onto a clean installation with the kit

The cluster that ran Logweir is gone with all its objects; the archive survives
in object storage. A clean installation recovers from the archive alone, with
no public network once the kit is built.

**Build the kit while you can, and at every upgrade.** As the trust
administrator, export the public trust material:

```bash
mkdir trust-export
kubectl --context <ctx> get trustpolicy <name> -o json \
  | logweir trust export --policy <name> --stdin > trust-export/trustpolicy-<name>.yaml
kubectl --context <ctx> -n logweir-system get configmap logweir-signing-trust \
  -o jsonpath='{.data.signing\.pub\.pem}' > trust-export/signing.pub.pem
kubectl --context <ctx> -n logweir-system get configmap logweir-signing-trust \
  -o jsonpath='{.data.key-id}{"\n"}' > trust-export/signing-key-id.txt
```

Then, connected, in a checkout of the release:

```bash
gh release download v<X.Y.Z> -R VladyslavHaina/logweir -D release-assets
just recovery-kit v<X.Y.Z> --release-assets release-assets \
  --trust trust-export --save-images linux/<arch>
```

`--save-images` saves each image for the nodes' platform (the four pinned
images, the identity hook's included, carry linux/amd64 and linux/arm64); a
single-platform image is saved as it is, with a warning. `--local-build` instead
of `--release-assets` is a dry run with this host's `target/release/logweir`,
and `kit.json` says `"binaries": "local-build"`; its `sha-<commit>` may be
short. The kit is `target/recovery-kit/logweir-recovery-kit-<version>/`, a
`.tar.gz` of it and its `.sha256`, and holds `bin/` (the CLI for linux-amd64,
linux-arm64 and darwin-arm64), `chart/`, `images/images.txt` and
`values-offline.yaml` (the controller, runner, console and identity-hook images
by digest), `schemas/`, `verify/verify_scorecard.py`, `trust/`, this section as
`RECOVERY.md`, the guides it cites (`docs/install.md`, `docs/keys.md`),
`kit.json` and `MANIFEST.sha256` (every file's sha256). The demo MinIO, Kafka
and `mc` images are not in it.

**The kit holds no private key and no password.** It refuses to pack a
private-key PEM (plain or base64), a Kubernetes Secret, or a file named like a
key, a password or a credential: exit 3, `REFUSED`, nothing written. Keep these
in your secret store:
- the archive's destination credentials (*3.11*): `archiveWrite`, without
  which a destination is refused; `archiveRead`, widened to the catalog row;
  and `evidenceRead`, without which no run is verified;
- the target cluster's credential;
- the console's key Secret (and, in shared mode, the OIDC client secret and the
  TLS certificate);
- the old identity backup, if you recover that identity.

**Recover.**

1. **Check the kit:** `sha256sum -c MANIFEST.sha256` (or `shasum -a 256 -c`)
   prints `OK` for every file.
2. **Load the images:** `docker load -i images/<role>-<os>-<arch>.tar` for each,
   or push them to your registry; `docker image inspect <reference>` succeeds
   for every line of `images/images.txt`.
3. **Create the console's key Secret, then install the chart from the kit.**
   The chart does not generate that Secret (*5e*; this is `localAdmin`'s):

   ```bash
   kubectl --context <ctx> create namespace logweir-system
   head -c 32 /dev/urandom > cursor.key
   kubectl --context <ctx> -n logweir-system \
     create secret generic logweir-console-keys --from-file=cursor.key
   rm cursor.key
   helm install logweir chart/<package>.tgz -n logweir-system \
     -f values-offline.yaml -f <your values> --wait --timeout 10m
   ```

   Your values turn the console on (*5e*). With images loaded on the node and
   no registry, set `imagePullPolicy`, `runnerImagePullPolicy`,
   `identity.bootstrapImagePullPolicy` and `api.console.imagePullPolicy` to
   `Never`. The install generates its own identity and console key and the
   `logweir-installation` policy (*5f*); name the restore target's cluster id
   in `identity.installationTrust.allowedTargetClusterIds`.
4. **Trust the archive's signer.** Compare `trust/signing-key-id.txt` with your
   out-of-band record (`docs/keys.md`), then, as `logweir-trust-admin`, copy that
   key's entry (usage `EvidenceSigning`) from `trust/trustpolicy-<name>.yaml`
   into `spec.keys` of `logweir-installation` (`kubectl edit trustpolicy
   logweir-installation`). Never apply the exported policy beside it: two
   `default: true` policies are `TrustPolicyConflict` everywhere.
5. **Reach the archive.** If its bucket survives, the destination names it. If
   only a copy does (*Keep the demo archive across an uninstall*), create the
   bucket and the *3.11* users in the new store and copy it in, bucket by
   bucket: `mc mirror ./archive/<bucket> <alias>/<bucket>`.
6. **Connect the existing archive:** on *Destinations*, a destination with
   `archiveWrite`, `archiveRead` and `evidenceRead` grants (an absent
   `evidenceWrite` uses `archiveWrite`); one with only `archiveRead` is
   refused. Then on *Catalog*, *Connect an existing archive* (a `Full` sync).
   Points from v0.1.5 or earlier need one `logweir catalog sync` backfill
   first.
7. **Connect the target:** on *Clusters*, the target cluster with its
   credential (*The first backup*, step 1), the cluster id step 3 allowed.
8. **Choose a point** the catalog marks selectable (`Available`, and `Verified`
   or `VerifiedHistorical`), name the topics and target, and create the
   restore; in confirm, Create is the approval.
9. **Verify:** the completion panel shows the verdict. To check it yourself,
   fetch the documents with the `evidenceRead` credential (`mc cat
   <alias>/<bucket>/<key> > <file>`, or `aws s3 cp s3://<bucket>/<key> <file>`):
   the scorecard and its signature at the `Restore`'s
   `status.evidence.scorecardKey` and `sidecarKey`, and the old backup's
   receipt at the catalog point's `receiptKey`
   (`logweir/backups/<backupId>/<run id>.receipt.json`) with its `.receipt.sig`.
   Then verify with the kit's CLI (`tar -xJf bin/logweir-<target>.tar.xz` gives
   `logweir-<target>/logweir`; a dry-run kit has `bin/<target>/logweir`):

   ```bash
   kubectl --context <ctx> -n logweir-system get configmap logweir-signing-trust \
     -o jsonpath='{.data.signing\.pub\.pem}' > new-signing.pub.pem
   logweir drill verify --scorecard scorecard.json --signature scorecard.sig \
     --public-key new-signing.pub.pem
   logweir drill verify --payload-type backup-receipt --scorecard receipt.json \
     --signature receipt.sig --public-key trust/signing.pub.pem
   ```

   The CLI is the offline verifier because the kit already carries it.
   `python3 verify/verify_scorecard.py <document> <signature> <key>` (with
   `--payload-type backup-receipt` for the receipt) is the independent second
   reader; it needs Python's `cryptography`, which the kit does not carry.

---

## Upgrade CRDs before upgrading the controller

Read [release-notes.md](release-notes.md) first; it lists each release's
required actions. Back up the identity, apply the new CRDs, wait for all
thirteen, then upgrade (controller and runner together, then the console):

```bash
kubectl --context docker-desktop apply --server-side --force-conflicts -f charts/logweir/crds/
for crd in approvals backupdestinations backups backupschedules kafkaclusters \
           preflights protectionpolicies recoverycatalogs \
           restores retentionpolicies topicdiscoveries trustpolicies trustrosters; do
  kubectl --context docker-desktop wait --for=condition=Established \
    "crd/${crd}.logweir.dev" --timeout=60s
done
helm upgrade logweir charts/logweir -n logweir-system --wait --timeout 10m
```

**`--force-conflicts` is required**: Helm owns the fields of the CRDs it made,
so without it the apply is refused while the wait still passes on the OLD
schemas. `kubectl --context docker-desktop diff --server-side --force-conflicts -f charts/logweir/crds/`
then prints nothing. Every change is additive (the `rehearsalschedules` CRD of
an earlier build stays until you delete it, release-notes item 73).
[kubernetes.md](kubernetes.md#upgrade) has what to do before, during and after.

**Bind every existing credential Secret after the upgrade (FX-20).** A Secret
from an earlier release has no `logweir-binding`, so its runs are refused
`CredentialBindingMismatch`: suspend those schedules, roll, bind each Secret to
its one object with `scripts/bind-credential.py` (a dry run, the owner confirms
the endpoint, then `--apply`), and resume
([kubernetes.md](kubernetes.md#credential-binding)).

**Roll back** the controller and runner together, CRDs left in place, after
draining restores and setting every `RetentionPolicy` to `Report`
([kubernetes.md](kubernetes.md#rolling-back)). Before rolling back past
PROD-16.1 or PROD-16.2, remove `approvalPolicy.default` and any two-person
policy, which older binaries refuse at start; unbound namespaces are then
`legacy-governed-v1`, pending confirmations are refused
`ApprovalPolicyMismatch`, and the policy, keys and ConfigMaps are retained.

## Serving the UI

The console, `logweir-api` (*5e*), serves it; [kubernetes.md](kubernetes.md)
§16, *Serving the UI*, says how. The legacy `kubectl proxy` page and the
`logweir-ui` image were withdrawn in v1 (release-notes item 75).

## Two clients, two trust stores

The engine (`kafka-backup`) falls back to its **bundled `webpki-roots`**, and
Logweir's rdkafka path uses the **image's `ca-certificates`**; a CA installed
on the node reaches neither. Name the CA once, `KafkaCluster.spec.auth.tlsCa`
(with `auth.tls: true`), and the runner hands it to librdkafka's
`ssl.ca.location` and the engine's `ssl_ca_location`. A standalone CLI uses
`LOGWEIR_SOURCE_TLS_CA_FILE` / `LOGWEIR_TARGET_TLS_CA_FILE`. A CA given to one
client only yields a working backup and a failing verification, or the reverse.

---

## Uninstall, and what it leaves behind

### Back up and recover the installation identity

Back up both retained objects before the first real backup and after any trust
change, straight to mode-0600 files (never the terminal); encrypt them into your
secret backup, remove the local copies, and do the same for
`logweir-console-confirmation` and `logweir-console-trust`:

```bash
install -d -m 0700 identity-backup
umask 077
kubectl --context docker-desktop -n logweir-system get secret logweir-signing-key \
  -o yaml > identity-backup/logweir-signing-key.yaml
kubectl --context docker-desktop -n logweir-system get configmap logweir-signing-trust \
  -o yaml > identity-backup/logweir-signing-trust.yaml
```

Recovery is restore-first: stop what could start a signing Job, strip
`resourceVersion`, `uid`, `creationTimestamp` and `managedFields`, apply both,
then run Helm:

```bash
kubectl --context docker-desktop -n logweir-system apply \
  -f identity-backup/logweir-signing-key.yaml \
  -f identity-backup/logweir-signing-trust.yaml
helm upgrade --install logweir charts/logweir -n logweir-system \
  --create-namespace --wait --timeout 10m
```

A published key without its Secret is key loss: bootstrap exits nonzero and
never generates a new key, and a mismatched pair is refused too.
Routine Helm 3 and Helm 4 upgrades, hook retries and rollbacks to a chart
carrying the `post-rollback` identity hook validate and reuse the same bytes.
Empty retained objects are creation-only `pre-install,pre-upgrade` hooks,
not `pre-rollback` hooks, so no revision re-creates a filled one, and Helm
stores only the empty definition, never the patched private bytes.
A rollback to a pre-bootstrap chart has no such hook, so
retention—not active validation—is its only identity guarantee.
`helm uninstall` keeps the Secret, the ConfigMap, each namespace copy and the
authority-free `ClusterRole/logweir-identity-singleton`, which makes one Logweir
installation identity per cluster the contract (a second release fails); a
same-name reinstall validates them in place; remove the marker only in a
full-cluster retirement, after exporting old trust. Never use `--take-ownership`
or `--force-replace` on them; a different key is a rotation, not a recovery. A
failed hook leaves the objects and its Job for diagnosis, and a chart rollback
does not roll the identity back. An install with a hand-made
`logweir-signing-key` upgrades unchanged: bootstrap derives the missing public
ConfigMap, and an existing one must match exactly.

### Trust reference and rotation contract

The ConfigMap's `trust-reference` is
`logweir.dev/v1alpha1/TrustRoster/default#spec.signingKeys`. Bootstrap publishes
a key; an administrator decides to trust it. Never infer trust from a public
key stored beside an archive. A rotation keeps the retiring public key for old
archives ([keys.md](keys.md), *The supported procedure*); on a roster-only
install it is a maintenance window, and a rollback restores the prior roster and
the identity backup together.

**A `TrustPolicy` that records a `KeyCompromise` revocation carries the
`logweir.dev/compromise-revocation` finalizer, which only a running controller
releases.** Before removing Logweir entirely, export each policy
([keys.md](keys.md), *Backing the policy up*) and remove that finalizer:

```bash
kubectl --context docker-desktop get trustpolicies -o json \
  | jq -r '.items[] | select((.metadata.finalizers // []) | index("logweir.dev/compromise-revocation")) | .metadata.name'
kubectl --context docker-desktop edit trustpolicy <name>   # delete the logweir.dev/compromise-revocation line
```

```bash
helm uninstall logweir -n logweir-system

# Helm never deletes crds/. Only when removing Logweir entirely:
kubectl --context docker-desktop delete -f charts/logweir/crds/
```

The four cleanup scopes are the control plane, scratch topics, archive objects
and evidence objects. Uninstalling removes only
that first thing: the release's RBAC, Deployments, runner ServiceAccounts and
NetworkPolicies go, and the identity, its copies and the marker stay. Deleting
the CRDs deletes every custom resource and the Jobs and ConfigMaps they own, the
trust anchors included, so export what you need first. The rest goes only on purpose:

```bash
kafka-topics.sh --bootstrap-server <broker> --delete --topic 'logweir-scratch-<name>'
aws s3 rm 's3://kafka-backups/logweir/<backup_id>/' --recursive    # archive objects
aws s3 rm 's3://logweir-evidence/<prefix>/' --recursive            # evidence objects
kubectl --context docker-desktop delete rolebinding logweir-viewer logweir-operator logweir-approver -n <namespace>
kubectl --context docker-desktop -n <namespace> delete serviceaccount logweir-runner
```

### Keep the demo archive across an uninstall

The chart's MinIO (`minio.enabled`) holds its archive on the PVC
`<release>-minio`, and `helm uninstall` deletes it, with every backup on it
when the volume's reclaim policy is `Delete` (docker-desktop's is). Helm keeps
an object only when the manifest it **stored** says `helm.sh/resource-policy:
keep`; an annotation set by hand on the live PVC is ignored. Set it through
Helm, at install or before the uninstall:

```bash
helm upgrade logweir <chart> -n logweir-system --reuse-values \
  --set minio.persistence.keep=true --wait
```

The PVC then outlives the release until you delete it (`kubectl delete pvc
<release>-minio`) or its namespace. A later `helm install` of the same release
into the same namespace, with the same `minio.*` values, takes the PVC over and
serves the old archive.
[UNVERIFIED — Helm's adoption of a kept PVC is not yet run on this chart; the next PoC reinstall measures it]
The PVC is one copy on one node, so mirror every bucket as well:

```bash
kubectl --context docker-desktop -n logweir-system port-forward svc/<release>-minio 9000:9000 &
mc alias set old http://127.0.0.1:9000          # prompts for minio.rootUser and its password
mc ls old                                       # the buckets
mc mirror old/<bucket> ./archive/<bucket>       # for each
```

`mc mirror ./archive/<bucket> <alias>/<bucket>` copies one back into a new
installation's store (*Recover onto a clean installation with the kit*).

---

## Section numbers before v1

| Before v1 | Now |
|---|---|
| §1 | [1. Installation signer and approver identity](#1-installation-signer-and-approver-identity) |
| §2, step 2 | [2. The cluster-scoped `TrustRoster`](#2-the-cluster-scoped-trustroster-whose-name-is-fixed) |
| §3, step 3 | [3. The four current Secrets](#3-the-four-current-secrets) |
| §3.11 | [3.11 Object-store permissions](#311-object-store-permissions-per-destination-role) |
| §4, step 4 | [4. Runner namespace prerequisites](#4-runner-namespace-prerequisites-helm-managed-or-low-level-manifest) |
| §5 | [5. Binding the five human roles](#5-binding-the-five-human-roles) |
| §5a, §5b, §5c | [5a](#5a-the-installation-policy-configmap), [5b](#5b-fence-the-consoles-create-secrets-kubernetes-130), [5c](#5c-notification-sinks) |
| §5d, §5e, §5f | [5d](#5d-the-consoleapi-principal), [5e](#5e-running-the-console-in-the-cluster-apiconsoleenabled), [5f](#5f-approval-modes-confirm-two-person-strict-approvalpolicy) |
| path (a), (b), (c), (d) | [Choose an image and installation path](#choose-an-image-and-installation-path) |

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
