# The Logweir Helm chart

## Copy this directory and install it

`charts/logweir/` is self-contained. Copy the whole directory into your own
repository, write a short values file, and install:

```bash
helm upgrade --install logweir . -n <namespace> --create-namespace -f my-values.yaml
```

That is the whole command. Nothing is fetched at install time: the chart's
`crds/` and `ui/` are byte copies carried inside it, and there is no
subchart and no dependency lock. The only things you must supply are

1. **the two images** — `controllerImage` and `runnerImage`, because nothing
   is published yet (see *Bring your own registry* in
   [`docs/install.md`](../../docs/install.md));
2. **an archive** — `archive.url` and, for anything S3-compatible,
   `archive.s3.endpoint` and `archive.s3.region`; or `minio.enabled: true` to
   get one in the cluster;
3. **the `kafka:` block** — the cluster Logweir backs up, and optionally the
   scratch cluster it restores into.

Everything else has a default.
[`examples/msk.values.yaml`](examples/msk.values.yaml) is a complete one for a
real cluster: Amazon MSK over SASL/SCRAM, a tainted nodepool, a private
registry. [`values.yaml`](values.yaml) lists **every** option with its default,
one line each — it is deliberately short, and every explanation lives in this
file.

One chart that installs Logweir's control plane — the same objects
[`logweir.yaml`](../../logweir.yaml) ships — and, optionally, its own
object-store backend, two throwaway Kafka clusters and the UI. It is
**derived from `config/`**, never the other way round: `scripts/check-chart.sh`
holds the chart's CRDs and UI files byte-identical to the tree, and
`crates/logweir/tests/chart_lint.rs` holds the rendered control plane to the
install file. [`docs/install.md`](../../docs/install.md) is still the single
install document; this README is the chart's own.

## What it installs

| object | when | why |
|---|---|---|
| the six `CustomResourceDefinition`s under `logweir.dev/v1alpha1` | always — from `crds/`, **once**, on `helm install` | Helm never upgrades or deletes the contents of `crds/`; see *Upgrading the CRDs* below |
| `ServiceAccount`, `ClusterRole`, `ClusterRoleBinding` `weirkeeper` | always | the one API client in the design; every granted verb has a caller, no verb on `secrets`, no `delete` on anything |
| `Deployment` `weirkeeper` | always | the control plane. Image `controllerImage`, pull policy `imagePullPolicy`, `LOGWEIR_RUNNER_IMAGE` from `runnerImage`, `LOGWEIR_RUNNER_PULL_POLICY` from `runnerImagePullPolicy`, the archive env from `archive.*` |
| `ClusterRole`s `logweir-viewer`, `logweir-operator`, `logweir-approver` | always, **unbound** | the three human roles; who may act where is your decision |
| `NetworkPolicy` `logweir-runner-egress`, `ServiceAccount` `logweir-runner` | always, in the release namespace | apply both into every other namespace that runs Jobs (`docs/install.md` steps 4 and 6) |
| `Deployment` + `Service` `<release>-minio`, a PVC, `Secret` `<release>-minio-root`, `Secret` `logweir-s3`, `Job` `<release>-minio-seed` | `minio.enabled` | an in-cluster archive with the buckets `kafka-backups` and `logweir-evidence` |
| `StatefulSet` + two `Service`s `<release>-kafka-source` and `-target`, `Job` `<release>-kafka-seed` | `demoKafka.enabled` | two single-broker KRaft clusters; `orders` and `payments` seeded on the source, the marker topic `logweir.scratch` on the target |
| `Deployment`, `Service`, `ConfigMap`, `ServiceAccount`, `ClusterRole`s, `RoleBinding` `<release>-ui` | `ui.enabled` | `kubectl proxy` serving the fourteen UI files and the API on one origin, with its own authority (below) |

Nothing optional is on by default, and the defaults are the shipped install:
`helm template charts/logweir` with nothing overridden renders the same
controller, env, security context and RBAC rules as `logweir.yaml`
(`chart_lint_default_render_agrees_with_the_install_file`).

## The three flags, in plain words

* **`minio.enabled`** — *bring a backend.* A MinIO with the two buckets the
  runner writes and the controller reads, and the `logweir-s3` Secret minted
  from its root credential. Demo-only: the root user is not a read-only
  principal, and a production install brings its own archive and its own
  Secrets and leaves this off. With it on and `archive.*` empty, the
  controller is pointed at it automatically.
* **`demoKafka.enabled`** — *bring two clusters to back up from and restore
  into.* PLAINTEXT, emptyDir, one broker each. The demo's transport, not a
  recommendation.
* **`ui.enabled`** — *serve the page from the cluster.* Read *The UI's
  authority* before turning it on.

## The five-minute path

```bash
helm install logweir charts/logweir -n logweir-system --create-namespace \
  -f charts/logweir/examples/demo.values.yaml --wait --timeout 10m
cargo build -p logweir                # the walk mints the approval with the shipped CLI
bash scripts/helm-demo.sh             # or: just helm-demo
```

`--wait` returns when both brokers, MinIO, the UI and the controller are
Ready and the two seed Jobs have succeeded (they are Helm hooks; a succeeded
one is deleted, a failed one stays for `kubectl logs`). The walk then mints two
keypairs, creates the five Secrets in a demo namespace, applies the
`TrustRoster`, probes two `KafkaCluster`s to `reachable: true`, fires a
`BackupSchedule`, restores from its `Backup` with an approval minted on the
host, verifies the scorecard with both readers, fetches the page through a
port-forward, and tears everything down — the cluster ends with no
`logweir-*` namespace. Every step prints its exit codes.

On a cluster that holds images you built yourself (`just image && just
image-weirkeeper`), add `examples/author-only.values.yaml` — and read its
header: **an author-only install is not evidence of publication.**

## Pointing a real install at a real archive

[`examples/minimal.values.yaml`](examples/minimal.values.yaml) is the operator
alone at the chart's shipped image defaults with the three values a stranger sets:
`archive.url`, `archive.s3.endpoint` (empty for Amazon S3 proper) and
`archive.s3.region`. Then, **before any custom resource**, the five Secrets of
`docs/install.md` step 3 — the chart creates none of them on this path — the
two keypairs, the cluster-scoped `TrustRoster` named `default`, and the runner
ServiceAccount in every namespace that runs Jobs. `just check-secrets
<namespace>` says whether the five are there.

The controller's read-only evidence credential is the `logweir-evidence-ro`
Secret in the release namespace. It is `optional: true` on the Deployment: the
controller starts without it and every verification reads `NotAttempted`,
which is a choice and not a bad document.

## The two Logweir images, and why they name a tag

**The chart's defaults name `controllerImage` and `runnerImage` by the `latest`
TAG, not by a digest. That is the owner's decision of 2026-09-12**, taken after
the trade-off below was put to them, and it is this chart's ruling alone:
`config/manager/deployment.yaml`, `logweir.yaml` and
`weirkeeper::job::RUNNER_IMAGE` still pin digests under Global Constraint 7, and
so do the four third-party images this chart can bring (MinIO, `mc`,
`apache/kafka`, `kubectl`). The two repositories are the tree's own — the gate
derives them from those two files rather than spelling them — so a namespace
change propagates here on its own.

**What a mutable tag does not promise.** The bytes behind `:latest` can change
under you: the same reference can resolve to different content tomorrow, on a
different node, or mid-rollout. A digest named bytes, and those bytes carried
the org-root anchor baked into the image (`/etc/logweir/org-root.fingerprint`);
under a tag, an image can be replaced upstream without a single Kubernetes
object changing. That is the guarantee this default trades away, and it is why
the two pull policies below are what they are.

**The pull policies follow the tag.** `imagePullPolicy` defaults to `Always` —
Kubernetes' own default for a `:latest` reference; `IfNotPresent` under a
mutable tag is a pod running whatever bytes its node happened to cache first.
`runnerImagePullPolicy` is a separate value, rendered as
`LOGWEIR_RUNNER_PULL_POLICY` on the Deployment and read once at startup by the
controller, and it defaults to `Always` for the same reason. The compiled-in
default the controller falls back to when that variable is unset is still
`Never`, which is right for an image LOADED onto a node — the laptop path,
`kind`, and `examples/author-only.values.yaml`, which sets
`runnerImagePullPolicy: Never` explicitly. A value outside `Never` /
`IfNotPresent` / `Always` makes the controller refuse to start, because the API
server would otherwise reject every runner Job it created.

**`latest` exists in no registry today.** `blocked: images not published`
(Global Constraint 37) is unchanged by this: `release.yml` has never run,
nothing has been pushed, and `docker.io/vladyslavhaina/…:latest` resolves nowhere. On a
cluster with no access to that namespace the controller pod sits in
`ImagePullBackOff`, exactly as `docs/install.md` path (a) records — the same
outcome the digests produced, for the same reason. So the DEFAULT path cannot
be exercised on any cluster today, and nothing here claims it has been.

**How to pin them back.** Resolve each tag to the bytes it names — a manifest
read, not a pull:

```bash
docker buildx imagetools inspect <repository>:latest
```

then install with all four values together, because a digest names bytes that
cannot change and re-pulling them buys nothing:

```bash
helm install logweir charts/logweir -n logweir-system --create-namespace \
  --set controllerImage=<repository>@sha256:… \
  --set runnerImage=<repository>@sha256:… \
  --set imagePullPolicy=IfNotPresent \
  --set runnerImagePullPolicy=IfNotPresent
```

## The four third-party images, and where their digests came from

None of them is part of the `latest` ruling: MinIO, `mc`, `apache/kafka` and
`kubectl` are pinned by digest under Global Constraint 7, and this is the
provenance of each, so nobody has to trust a bare hash.

* **`minio.image` and `minio.mcImage`** are the references
  `e2e/compose/docker-compose.yml` pins, copied byte for byte and never
  resolved again. They are **quay.io**, not Docker Hub, because Docker Hub
  refuses anonymous pulls of `minio/minio` (plan erratum E30(a)).
* **`demoKafka.image`** — `apache/kafka:3.7.1`, the compose stack's broker,
  pinned by its **manifest-list** digest so the same reference resolves on an
  amd64 CI runner and on an arm64 development host. Resolved once, on
  **2026-09-12**, with the `kindest/node` provenance idiom:

  ```bash
  docker buildx imagetools inspect apache/kafka:3.7.1
  ```

* **`ui.image`** — a kubectl image at the cluster's minor version, also a
  manifest-list digest, resolved once on 2026-09-12 with:

  ```bash
  docker buildx imagetools inspect registry.k8s.io/kubectl:v1.34.1
  ```

  `registry.k8s.io` is the Kubernetes project's own registry. The brief named
  `bitnami/kubectl`; on 2026-09-12 `docker buildx imagetools inspect
  bitnami/kubectl:1.34.1` (and `:1.34`) answered `not found` — Bitnami's Docker
  Hub catalogue no longer publishes versioned tags, and `:latest` is a tag,
  which Global Constraint 7 forbids. `kubectl proxy` is a reverse proxy and a
  file server, so the client/server skew rules do not touch it; that is why the
  same digest serves a 1.29 `kind` node in CI.

`demoKafka.clusterIds` are not images but were minted the same way and on the
same day: the image's own `kafka-storage.sh random-uuid`, twice, because the
image's built-in default is **one fixed id** and two brokers reporting the same
id are one cluster to phase 0 — measured on 2026-09-12, on the second walk of
`scripts/helm-demo.sh`. Keep them different from each other; the template
refuses equal ids.

## `kafka:` — pointing the chart at a real cluster

A Kafka connection is not a chart value in Logweir's design: it is a
`KafkaCluster` custom resource the controller reconciles, probes, and reports
`status.clusterId` for. `kafka.enabled: true` makes the chart render those
objects from a flat block, so a stranger writes addresses and a Secret name
rather than a custom resource:

```yaml
kafka:
  enabled: true
  bootstrapServers: "b-1.example…:9096,b-2.example…:9096"   # or a YAML list
  security: { protocol: SASL_SSL, mechanism: SCRAM-SHA-512 }
  username: kafbat
  secretRef: my-scram-secret
  target:                       # optional — a restore needs one, a backup does not
    bootstrapServers: "b-1.scratch…:9096"
    security: { protocol: SASL_SSL, mechanism: SCRAM-SHA-512 }
    username: kafbat
    secretRef: my-scratch-secret
```

**The protocol/mechanism mapping is the chart's job, and it refuses what it
cannot speak.** The CRD's `auth.mode` enum is `plaintext | scramSha512` and the
client speaks SCRAM-SHA-512 only, so:

| `security.protocol` | `security.mechanism` | renders |
|---|---|---|
| `PLAINTEXT` | (any) | `mode: plaintext`, `tls: false` |
| `SASL_SSL` | `SCRAM-SHA-512` | `mode: scramSha512`, `tls: true` |
| `SASL_PLAINTEXT` | `SCRAM-SHA-512` | `mode: scramSha512`, `tls: false` |
| anything else | | **`helm` fails at render time**, naming the supported set |

A silently rendered `PLAIN` or `SCRAM-SHA-256` would be a chart that installs
and cannot authenticate, which is why it is a refusal and not a warning.
`kafka.enabled` and `demoKafka.enabled` together are a refusal too — `demoKafka`
brings its own two brokers and its own cluster objects — with a message saying
which one to turn off.

**The Secret is yours, and Logweir never reads it.** `secretRef` names a Secret
**in the release namespace** holding the SASL password under the key
`secretKey`. That key is `password` and can be nothing else: the operator
projects exactly one name into the probe
(`weirkeeper::controllers::restore::TARGET_PASSWORD_SECRET_KEY`), so the chart
refuses any other value rather than render a `KafkaCluster` whose probe cannot
read its credential. The password reaches the probe pod as a
`valueFrom.secretKeyRef` and never enters a status field, a log line or a
rendered document.

The rendered objects go in the release namespace with the chart's labels, and
the probe Job runs under the `logweir-runner` ServiceAccount the chart already
creates there.

## Node placement, and where it does not reach

`kubernetes.nodeSelector`, `kubernetes.tolerations` and `kubernetes.affinity`
apply to **every pod this chart renders** — the controller, MinIO and its seed
Job, both demo brokers and their seed Job, and the UI. Each optional component
may override all three with its own block (`minio.nodeSelector`,
`demoKafka.tolerations`, `controller.affinity`, …); an override replaces the
top-level value for that component's pods rather than merging with it.

**Runner Jobs are the operator's objects, not the chart's**, and the gap is
named rather than silent:

* `imagePullSecrets` **do** reach them — through the `logweir-runner`
  ServiceAccount, because a pod inherits its ServiceAccount's pull secrets.
* Node placement **does not**. There is no `nodeSelector`, toleration or
  affinity path to a runner Job today. On a cluster whose only Kafka-adjacent
  nodes are tainted, a runner Job will not schedule there. Implementing it is
  an operator change and is out of this chart's scope.

`kubernetes.namespace` does **not** move the install. `helm -n` / `--namespace`
decides that, and every object carries `Release.Namespace`; the key exists for
the one place the chart needs a namespace *name* it cannot derive — the UI's
`RoleBinding` list, where it supplies `ui.namespaces` when that is empty.

## `imagePullSecrets`, and what Docker Hub does differently

`imagePullSecrets: [{name: regcred}]` is rendered on the controller
ServiceAccount, on the **runner** ServiceAccount (so every runner Job inherits
it) and on the optional components' pods. It is how a private registry is
pulled from — an ECR, or a Docker Hub repository that is not public.

Three Docker Hub properties decide whether a pull works, and two of them are
the opposite of GHCR's:

* A Docker Hub repository **created by a push is public by default**; a GHCR
  package starts **private**.
* That default is the account's setting, and an auto-created repository takes
  it. A private one answers an anonymous pull with `failed to fetch anonymous
  token: … 403 Forbidden`. Make it public, or set `imagePullSecrets`.
* Anonymous Docker Hub pulls are **rate-limited per source IP**, shared by
  every node behind one NAT — so a pull secret carrying a Docker Hub login is
  useful on a *public* image too.

## `environment:`

A label, and nothing else: `logweir.dev/environment: <value>` on every object
the chart renders (the six CRDs excepted — Helm copies `crds/` verbatim and
never templates it). It switches no behaviour, changes no name and gates
nothing. A key that silently did something would be worse than no key.

## Amazon MSK

[`examples/msk.values.yaml`](examples/msk.values.yaml) is the shape; these are
the facts it does not have room for, measured 2026-09-12:

* SASL/SCRAM on MSK is port **9096** (not 9092), `SASL_SSL` +
  `SCRAM-SHA-512` — which is exactly the one SASL combination Logweir speaks.
* The SCRAM credential lives in **AWS Secrets Manager**, associated with the
  cluster. Kubernetes cannot read it directly: sync it into a Kubernetes Secret
  in the release namespace (External Secrets Operator, the Secrets Store CSI
  driver, or by hand) under the key `password`, and name that Secret in
  `kafka.secretRef`.
* **A password containing `"`, `'`, `$`, CR or LF is refused by the runner** —
  `logweir_core::guard::UNRENDERABLE_CREDENTIAL_CHARACTERS`. Mint one without
  them; a refusal at drill time is worse than a refusal at creation time.
* The IAM/ACL principal needs **Describe and Read on the source topics plus
  DescribeCluster**, and **Describe, Create and Write on a target**. Logweir
  never deletes (Global Constraint 6), so no delete action is required.
* **MSK IAM authentication is not implemented.** `AuthConfig::Token` is a named
  refusal in the operator, not an oversight — SASL/SCRAM is the path.

## The UI's authority

With `ui.enabled`, one pod runs `kubectl proxy --www=/ui --www-prefix=/ui/
--address=0.0.0.0 --port=8001 --accept-hosts='.*'
--accept-paths='^/(ui/|apis/logweir\.dev/v1alpha1/)'` from a kubectl image
pinned by digest, with the fourteen UI files mounted from a ConfigMap. The
proxy attaches the pod's ServiceAccount credential — `<release>-ui` — to every
request it forwards, so **anyone who can reach that Service acts with that
ServiceAccount's authority.** The page holds no credential and asks for none
(Global Constraint 28: no key material in the page, none in the ConfigMap).
The ServiceAccount is bound to a ClusterRole carrying exactly the verbs the
page issues, measured from `ui/api.js` and `ui/pages/*.js`: `get`/`list` on
the five namespaced kinds, `create` on approvals, kafkaclusters,
backupschedules and restores, `patch` on backupschedules, and `list` on the
cluster-scoped trustrosters — never `watch`, never `delete`, never the shipped
`logweir-operator`'s `update`. The path filter admits only `/ui/` and
`/apis/logweir.dev/v1alpha1/`; the core API, pod exec and attach are refused
by the proxy before RBAC is consulted. There is no Ingress. Reach it with

```bash
kubectl port-forward -n logweir-system svc/logweir-ui 8001:8001
```

and open `http://127.0.0.1:8001/ui/`. The chart binds the page's role in the
release namespace and in each namespace listed under `ui.namespaces`; for any
other namespace, one command:

```bash
kubectl create rolebinding logweir-ui --clusterrole=logweir-ui \
  --serviceaccount=logweir-system:logweir-ui -n <namespace>
```

The laptop path — `kubectl proxy --www=./ui` under your own kubeconfig, with
the page holding *your* authority — is unchanged and documented in
`docs/install.md`, *Serving the UI*.

## Upgrading the CRDs by hand

Helm installs `crds/` once and, by its own rule, never upgrades or deletes it.
When a release changes a CRD, apply the new definitions yourself before
`helm upgrade`:

```bash
kubectl apply --server-side -f charts/logweir/crds/
helm upgrade logweir charts/logweir -n logweir-system
```

`charts/logweir/crds/*.yaml` is byte-identical to `config/crd/*.yaml`
(`scripts/check-chart.sh` compares them with `cmp`), so applying either
directory is the same act.

## Uninstall, and what it leaves behind

```bash
helm uninstall logweir -n logweir-system
```

removes everything the release created **except**: the six CRDs (Helm never
deletes `crds/`; `kubectl delete crd <name>` removes each and every custom
resource stored under it), the MinIO `PersistentVolumeClaim` when
`minio.persistence.enabled` (delete it yourself, or keep the archive), the
namespace `--create-namespace` made, the cluster-scoped `TrustRoster`, and any
RoleBinding you created by hand. And, as with `kubectl delete -f
logweir.yaml`: no archive object and no evidence object is ever deleted by
Logweir — Global Constraint 6.

## The checks that hold the chart to the tree

* `scripts/check-chart.sh` (`just chart-check`, in `just gate`): CRDs and UI
  byte-identical to the tree; `helm lint` for the defaults and every example;
  `helm template` regenerated into `rendered/` with no drift; every rendered
  image a digest EXCEPT the two Logweir images, which must be exactly
  `<repository>:latest` (and `:latest` on any other image is still refused),
  with the author-only render exempt by name — its whole premise is a locally
  built tag; the schema refusing `--set demoKafka.enabled=yes`; `values.yaml`
  naming the tree's own repositories at `:latest`.
* `crates/logweir/tests/chart_lint.rs`: the rendered defaults agree with
  `logweir.yaml`; nothing optional renders under defaults; each flag renders
  its named objects; the UI ConfigMap is the tree's bytes and no key material.
* `scripts/helm-demo.sh` (`just helm-demo`; `.github/workflows/helm-demo.yml`
  on `kind`): the walk above, on a real cluster.

Documentation is licensed [CC-BY-4.0](../../docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
