# Logweir PoC install profile: Helm, ingress, TLS and SSO

This directory is a **versioned proof-of-concept installation** of the supported
path in [docs/quickstart.md](../../docs/quickstart.md): everything installed with
Helm — Logweir itself from its **published OCI chart**, which names the four
published Docker Hub images of the same commit (`docker.io/vladyslavhaina/weirkeeper`,
`logweir`, `logweir-console`, `logweir-ui`, by their immutable `sha-<commit>`
tags) — the console in `shared` mode behind **Traefik**, with real TLS from a
**cert-manager** local CA and sign-in through **Dex**. No image is built
locally, no binary is installed by hand, and **no installed object is patched
after the install**: every setting is a Helm value. The host needs `helm`,
`kubectl`, `openssl`, `htpasswd` and this directory (a checkout of the
repository at any commit that carries it).

It runs on docker-desktop Kubernetes as a PoC, and every setting is the one a
production install keeps unless the table in
[*What production keeps*](#what-production-keeps-and-what-the-poc-stands-in-for)
says the PoC stands something in for it.

| File | What it is |
|---|---|
| [versions.env](versions.env) | every version: the Logweir commit, its `sha-` tag and chart version, the two upgrade-rehearsal baselines, the three upstream chart versions, namespaces, hostnames and Traefik's fixed ClusterIP |
| [traefik.values.yaml](traefik.values.yaml) | the ingress controller (chart 41.6.0, Traefik v3.7.13, by digest): HTTPS redirect, HSTS, a fixed ClusterIP |
| [cert-manager.values.yaml](cert-manager.values.yaml) and [issuers.yaml](issuers.yaml) | cert-manager v1.21.2, images pinned by digest, and the local CA `ClusterIssuer` |
| [dex.values.yaml](dex.values.yaml) | Dex 0.24.1 (v2.44.0): one static user per Logweir role, secrets from a Secret |
| [logweir.values.yaml](logweir.values.yaml) | Logweir: shared console, scoped controller, Ingress, NetworkPolicy, approval policy, demo Kafka and MinIO — and the six chart-gap values that replaced the patch |
| [minio-grants.yaml](minio-grants.yaml) | three least-privilege MinIO users for the destination, so the demo MinIO's root credential is never given to Logweir |
| [trustpolicy.sh](trustpolicy.sh) | prints the `TrustPolicy` for `logweir-poc` from the cluster's public signing key and the console's confirmation key |
| [rehearsals/](rehearsals/) | the starting values of the two upgrade rehearsals, each for its own version's chart |
| [validate.sh](validate.sh) | renders every chart with the pinned versions and these values, without a cluster (see its header) |

**Why `deploy/poc/` and not `charts/logweir/examples/`.** Everything under
`charts/logweir/examples/` ships inside the Logweir chart package and is
rendered by `just chart-check` against the chart's own defaults. This profile
pins one publication and configures three other charts, so it lives beside the
chart, and [validate.sh](validate.sh) is its render gate.

**Which Logweir it installs.** [versions.env](versions.env) names one `main`
publication: `LOGWEIR_TAG` (`sha-<commit>`) for the images and
`LOGWEIR_CHART_VERSION` (`0.1.0-sha-<commit>`) for the chart, published
together by `images.yml`. The profile needs a build that carries the chart-gap
fixes below — the first `main` publication after they merged. Until
`LOGWEIR_COMMIT` names that publication it is an all-zero placeholder, and
nothing here can be installed. [UNVERIFIED — LOGWEIR_COMMIT is set when the first main publication with these chart fixes exists.]

Hostnames: `logweir.localtest.me` (the console) and `dex.localtest.me` (Dex).
`localtest.me` and all its subdomains resolve to `127.0.0.1` in public DNS, and
docker-desktop publishes Traefik's `LoadBalancer` Service on the host's
`127.0.0.1:80/443`.

## Prerequisites

- docker-desktop with Kubernetes enabled (1.30 or newer; the admission policy
  needs 1.30), ports 80 and 443 free on the host, and — on Apple silicon —
  amd64 emulation (the runner image is `linux/amd64`).
- `helm` 3.15+ or 4 (OCI charts), `kubectl`, `openssl`, `htpasswd` (macOS ships
  it), `git`.

```bash
git clone https://github.com/VladyslavHaina/logweir && cd logweir
. deploy/poc/versions.env
CTX=docker-desktop     # every command below names its context explicitly
```

Every secret below is generated on your machine into `./poc-secrets/`
(mode 0700) and loaded into the cluster as a Secret. None is in a values file,
a rendered manifest or Helm's release history — with **one** exception, the
demo MinIO's root credential (*The demo archive*, step 7).

```bash
install -d -m 0700 poc-secrets
```

## 1. Namespaces

```bash
for ns in "$LOGWEIR_NAMESPACE" "$POC_NAMESPACE" "$DEX_NAMESPACE"; do
  kubectl --context "$CTX" create namespace "$ns" --dry-run=client -o yaml \
    | kubectl --context "$CTX" apply -f -
done
```

`logweir-poc` must exist before Logweir installs: the chart copies the signing
identity and the runner account into it, and the demo archive credential lands
there (`kubernetes.connectionsNamespace`).

## 2. Ingress controller: Traefik

```bash
helm upgrade --install traefik traefik --repo "$TRAEFIK_REPO" \
  --version "$TRAEFIK_CHART_VERSION" --kube-context "$CTX" \
  -n "$INGRESS_NAMESPACE" --create-namespace \
  -f deploy/poc/traefik.values.yaml --wait --timeout 10m
```

Why Traefik: the Kubernetes project retired ingress-nginx in March 2026 (no
releases or security fixes since), so a production-ready profile cannot use it.
[traefik.values.yaml](traefik.values.yaml) lists every ingress-nginx setting
the profile used and its Traefik spelling (HSTS as a `headers` Middleware on the
`websecure` entry point, the HTTP→HTTPS redirect, forwarded headers never
believed from a client). Logweir only uses a standard `Ingress` with
`ingressClassName: traefik`, so Gateway API on the same controller is an option
later with no Logweir change.

**Check:** `kubectl --context "$CTX" -n traefik get svc traefik` shows
`CLUSTER-IP 10.96.0.80` and `EXTERNAL-IP localhost`, and
`curl -sk https://logweir.localtest.me` answers `404` (nothing routes there
yet). If the Service is refused because `10.96.0.80` is outside your cluster's
Service range, pick a free address inside it and set it in `traefik.values.yaml`,
`logweir.values.yaml` (`hostAliases`) and `versions.env` (`TRAEFIK_CLUSTER_IP`);
`validate.sh` checks that the three agree.

## 3. Certificates: cert-manager and the local CA

```bash
helm upgrade --install cert-manager cert-manager --repo "$CERT_MANAGER_REPO" \
  --version "$CERT_MANAGER_CHART_VERSION" --kube-context "$CTX" \
  -n "$CERT_MANAGER_NAMESPACE" --create-namespace \
  -f deploy/poc/cert-manager.values.yaml --wait --timeout 10m
kubectl --context "$CTX" apply -f deploy/poc/issuers.yaml
kubectl --context "$CTX" wait --for=condition=Ready clusterissuer/logweir-poc-ca --timeout=120s
kubectl --context "$CTX" -n cert-manager get secret logweir-poc-ca \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > poc-secrets/ca.crt
```

`poc-secrets/ca.crt` is the CA's **public** certificate. Trust it in your
browser for the PoC (macOS:
`sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain poc-secrets/ca.crt`),
or accept the warning; remove it again when the PoC ends.

## 4. Dex

One client secret, shared by Dex and the console, and a password per user:

```bash
openssl rand -hex 32 | tr -d '\n' > poc-secrets/client-secret   # no newline: both sides read it verbatim
for u in viewer operator approver admin; do openssl rand -base64 18 > "poc-secrets/$u.password"; done
hash() { htpasswd -bnBC 10 "" "$(cat "poc-secrets/$1.password")" | tr -d ':\n'; }
kubectl --context "$CTX" -n "$DEX_NAMESPACE" create secret generic dex-poc-secrets \
  --from-file=LOGWEIR_CONSOLE_CLIENT_SECRET=poc-secrets/client-secret \
  --from-literal=DEX_HASH_VIEWER="$(hash viewer)" \
  --from-literal=DEX_HASH_OPERATOR="$(hash operator)" \
  --from-literal=DEX_HASH_APPROVER="$(hash approver)" \
  --from-literal=DEX_HASH_ADMIN="$(hash admin)"
helm upgrade --install dex dex --repo "$DEX_REPO" --version "$DEX_CHART_VERSION" \
  --kube-context "$CTX" -n "$DEX_NAMESPACE" -f deploy/poc/dex.values.yaml --wait --timeout 10m
```

**Check:** `curl --cacert poc-secrets/ca.crt https://dex.localtest.me/.well-known/openid-configuration`
returns a document whose `issuer` is exactly `https://dex.localtest.me`, and the
response carries `strict-transport-security: max-age=31536000`.

## 5. Logweir's own Secrets and the CA reference

All of these live in `logweir-system`, which the controller cannot create Jobs
in (`controller.watchNamespaces`):

```bash
# The console's session and cursor keys (two lines each; keyVersion 1).
printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > poc-secrets/session.key
printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > poc-secrets/cursor.key
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" create secret generic logweir-console-keys \
  --from-file=session.key=poc-secrets/session.key --from-file=cursor.key=poc-secrets/cursor.key
# The OIDC client secret, the same value Dex holds, under the key clientSecret.
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" create secret generic logweir-console-oidc \
  --from-file=clientSecret=poc-secrets/client-secret
# The console's ConsoleConfirmation key (Ordinary approval), and its public half.
openssl genpkey -algorithm ed25519 -out poc-secrets/confirmation.key
openssl pkey -in poc-secrets/confirmation.key -pubout -out poc-secrets/confirmation.pub.pem
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" create secret generic logweir-console-confirmation \
  --from-file=confirmation.key=poc-secrets/confirmation.key
# The local CA's PUBLIC certificate — `api.console.oidc.caBundle` names this
# ConfigMap, and the console trusts it beside the system roots (chart gap G1).
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" create configmap logweir-poc-ca \
  --from-file=ca.crt=poc-secrets/ca.crt
# The three least-privilege MinIO users' secret keys (step 7).
for u in writer reader evidence; do openssl rand -hex 20 | tr -d '\n' > "poc-secrets/minio-$u"; done
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" create secret generic logweir-poc-minio-users \
  --from-file=writer=poc-secrets/minio-writer --from-file=reader=poc-secrets/minio-reader \
  --from-file=evidence=poc-secrets/minio-evidence
```

## 6. Logweir, from the published chart

```bash
helm upgrade --install logweir "$LOGWEIR_CHART" --version "$LOGWEIR_CHART_VERSION" \
  --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE" \
  -f deploy/poc/logweir.values.yaml --wait --timeout 15m
```

That is the whole install: the chart carries the four images of its own commit
(`helm show values "$LOGWEIR_CHART" --version "$LOGWEIR_CHART_VERSION" | grep image:`),
and nothing is read from the cluster or patched afterwards. What used to need a
`kubectl patch` or an address read at install time is now a value in
[logweir.values.yaml](logweir.values.yaml) — see [*The chart gaps, closed*](#the-chart-gaps-closed).

**From a checkout, for development.** The same values with this repository's
chart; `--set` gives it the images the published chart would have carried:

```bash
helm upgrade --install logweir ./charts/logweir --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE" \
  -f deploy/poc/logweir.values.yaml \
  --set "controllerImage=docker.io/vladyslavhaina/weirkeeper:$LOGWEIR_TAG" \
  --set "runnerImage=docker.io/vladyslavhaina/logweir:$LOGWEIR_TAG" \
  --set "api.console.image=docker.io/vladyslavhaina/logweir-console:$LOGWEIR_TAG" \
  --set "ui.image=docker.io/vladyslavhaina/logweir-ui:$LOGWEIR_TAG" \
  --wait --timeout 15m
```

Check out `$LOGWEIR_COMMIT` first when you want exactly the published objects;
`validate.sh` proves the two commands render the same objects at a given
commit.

**Check:**
- `kubectl --context "$CTX" -n logweir-system get deploy` shows `weirkeeper`
  (1/1, **Ready** — its readiness is `weirkeeper --probe ready` against its own
  loopback health listener) and `logweir-api` (2/2), and
  `get pdb,networkpolicy,ingress` lists the console's budget, policies and
  Ingress;
- `kubectl --context "$CTX" -n traefik get role,rolebinding logweir-api-trusted-proxy`
  exists — the console's one read outside its namespaces, `list endpointslices`;
- the console's log names the trusted proxy set:
  `kubectl --context "$CTX" -n logweir-system logs deploy/logweir-api | grep 'trusted proxy set changed'`
  shows the Traefik pod's address;
- `kubectl --context "$CTX" -n logweir-system get configmap logweir-signing-trust -o jsonpath='{.data.key-id}'`
  prints the installation signer's key id (back up the identity now:
  [docs/install.md](../../docs/install.md), *Back up and recover the installation identity*);
- `curl --cacert poc-secrets/ca.crt https://logweir.localtest.me/readyz` answers `200`.

**Restart Traefik and check again** — the gap this profile used to re-run a
step for: `kubectl --context "$CTX" -n traefik rollout restart deploy/traefik`,
wait for it, and `/readyz` still answers `200` within about five seconds of the
new pod serving, with no step re-run.

## 7. The demo archive, and the credentials Logweir gets for it

The chart's demo MinIO takes its **root** credential from chart values
(`minio.rootUser` / `minio.rootPassword`, the public default `minioadmin`). That
one credential is therefore in a rendered Secret and in Helm's release history —
this profile's single exception to "nothing secret in values" — and it can
delete. It is never handed to Logweir. A one-shot Job reads it inside the
cluster and creates three users with the policies
[docs/install.md](../../docs/install.md) §3.11 measured, none of which holds
`s3:DeleteObject`:

```bash
kubectl --context "$CTX" apply -f deploy/poc/minio-grants.yaml
kubectl --context "$CTX" -n logweir-system wait --for=condition=complete \
  job/logweir-poc-minio-grants --timeout=180s
```

| MinIO user | secret key | the destination grant it fills |
|---|---|---|
| `logweir-poc-writer` | `poc-secrets/minio-writer` | `archiveWrite` (and `evidenceWrite`, which falls back to it) |
| `logweir-poc-reader` | `poc-secrets/minio-reader` | `archiveRead` (widened to the catalog-sync row) |
| `logweir-poc-evidence` | `poc-secrets/minio-evidence` | `evidenceRead` (with the `s3:ListBucket` that makes "absent" mean absent) |

## 8. Trust

`logweir-trust-admin` is a **cluster-scoped** role, bound once with a
`ClusterRoleBinding` — not per namespace — because a `TrustPolicy` is
cluster-scoped and decides whose keys may sign for the namespaces it names. As a
holder of it (the PoC's kubeconfig is a cluster administrator), put the two
public keys on the `TrustPolicy` that governs `logweir-poc`:

```bash
bash deploy/poc/trustpolicy.sh poc-secrets/confirmation.pub.pem > poc-secrets/trustpolicy.yaml
kubectl --context "$CTX" apply -f poc-secrets/trustpolicy.yaml
```

**Check:** `kubectl --context "$CTX" get trustpolicy logweir-poc` reads `LOADED
True` and `BOUND logweir-poc`.

## 9. First sign-in, per role

Open `https://logweir.localtest.me/auth/login` (the console's sign-in route,
which redirects to Dex), sign in as `<role>@logweir.localtest.me` with the
password in `poc-secrets/<role>.password`, and land on
`https://logweir.localtest.me/ui/`:

| User | Console role in `logweir-poc` | What it can do |
|---|---|---|
| `viewer` | viewer | read everything; no button acts |
| `operator` | operator | connections, destinations, schedules, backups, restore requests — and, under this PoC's Ordinary policy, confirm its own restore request |
| `approver` | approver | the approvals view; countersigns under a `Governed` namespace (none in this PoC) |
| `admin` | administrator | everything above, plus the keys view |

**Check:** each user's session shows `logweir-poc` with exactly its role, and
the audit line for the sign-in names `https://dex.localtest.me#<subject>`.

## 10. First backup and restore, in the console

Signed in as `operator`, follow [the supported path](../../docs/quickstart.md#the-supported-path-install-to-disaster-restore)
steps 4–8 with these PoC values:

| Step | Value |
|---|---|
| Source connection | `source`, role `source`, `logweir-kafka-source.logweir-system.svc.cluster.local:9092`, plaintext |
| Target connection | `target`, role `target`, `logweir-kafka-target.logweir-system.svc.cluster.local:9092`, plaintext |
| Destination | `primary`: endpoint `http://logweir-minio.logweir-system.svc:9000` (transport `InsecureHTTP`, the demo MinIO speaks no TLS), region `us-east-1`, path-style, bucket `kafka-backups`, prefix `poc`; each grant the MinIO user in step 7's table, entered once as a new credential (access key = the user name, secret key = its file) |
| Schedule | `orders-nightly`, source `source`, topics `orders` and `payments` (the demo seeds both), daily, destination `primary`; then *Run first backup now* |
| Restore | from the schedule's page, *Restore this point*; target `target`, new-topic prefix `restored-`; the readiness check; *Create the Restore*; confirm it (Ordinary) |

**Check:** the backup's operation page reads `Succeeded` with *verified by
weirkeeper … against key …*; the restore reaches `Succeeded` with its completion
panel, and the `restored-` topics exist on the target broker.

## Upgrade to a newer publication

Set `LOGWEIR_COMMIT` in `versions.env` to the newer publication, read
[docs/release-notes.md](../../docs/release-notes.md) (its required actions come
first), then:

```bash
. deploy/poc/versions.env
# 1. The CRDs, from the NEW chart (Helm never upgrades crds/ itself).
helm pull "$LOGWEIR_CHART" --version "$LOGWEIR_CHART_VERSION" --untar -d poc-secrets/chart
kubectl --context "$CTX" apply --server-side -f poc-secrets/chart/logweir-chart/crds/
for crd in $(ls poc-secrets/chart/logweir-chart/crds | sed 's/\.yaml$//'); do
  kubectl --context "$CTX" wait --for=condition=Established "crd/$crd.logweir.dev" --timeout=60s
done
# 2. Controller, runner and console images TOGETHER, approval bindings unchanged.
helm upgrade logweir "$LOGWEIR_CHART" --version "$LOGWEIR_CHART_VERSION" --kube-context "$CTX" \
  -n "$LOGWEIR_NAMESPACE" -f deploy/poc/logweir.values.yaml \
  --set-json 'approvalPolicy.namespaces={}' --wait --timeout 15m
# 3. Then the approval-policy binding.
helm upgrade logweir "$LOGWEIR_CHART" --version "$LOGWEIR_CHART_VERSION" --kube-context "$CTX" \
  -n "$LOGWEIR_NAMESPACE" -f deploy/poc/logweir.values.yaml --wait --timeout 15m
```

**How "controller, then console, then binding" is staged inside ONE Helm
release.** Every Logweir component is in the one release, so the release notes'
order is successive `helm upgrade`s of it. Step 2 moves the controller, the
runner image the controller hands its Jobs and the console **together** — they
are one chart version naming one commit's images, and the probes and console
configuration of this chart need images of the same build — while leaving
`approvalPolicy.namespaces` empty, so no namespace changes approval semantics
under a Restore in flight. Step 3 adds the binding once step 2 is Ready. An
installation with no binding to add stops after step 2 without the `--set-json`
override.

**What must survive**, and how to check it:
- the installation identity: `logweir-signing-trust`'s `key-id` is the one
  recorded before;
- every schedule: same UID and `metadata.generation`, its history intact, its
  next slot fired by the new controller;
- the archive: the pre-upgrade backup still shows its green badge, and a
  pre-upgrade point still restores.

Rollback is `helm rollback logweir <revision>` to the previous revision — which
restores the previous chart AND its images together — after the checklist in
[docs/release-notes.md](../../docs/release-notes.md), *Migration and rollback*;
the CRDs stay.

## Upgrade rehearsals

[docs/release-handoff.md](../../docs/release-handoff.md) plans two rehearsals
from published builds to the release in `versions.env`. Neither older build can
install with this profile's values, so each has its own starting values file
under [rehearsals/](rehearsals/), installed with **that version's own chart**
from this repository. [validate.sh](validate.sh) renders both against their own
charts, and checks that the candidate refuses R2's starting values until they
are migrated. Both start from steps 1–5 above (namespaces, Traefik,
cert-manager, Dex, Secrets); both end with the three steps of *Upgrade to a
newer publication*, using this profile's `logweir.values.yaml`.

### R1 — from `v0.1.5`

`v0.1.5` has six CRDs, no managed identity, no console (there is no
`logweir-console:v0.1.5`) and no controller scoping. Its runs execute in
`logweir-poc`, the namespace the release keeps after the upgrade.

```bash
git archive "$R1_COMMIT" charts/logweir | (mkdir -p poc-baselines/r1 && tar -x -C poc-baselines/r1)
git show "$R1_COMMIT:config/rbac/backup-runner-serviceaccount.yaml" > poc-baselines/r1/runner-sa.yaml
# v0.1.5's hand-provisioned identity (its docs/install.md "Before any custom resource"):
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out poc-secrets/r1-signing.pem
openssl pkey -in poc-secrets/r1-signing.pem -pubout -out poc-secrets/r1-signing.pub.pem
# The runner namespace's copy, which v0.1.5 reads, AND the release namespace's,
# which the upgraded chart's identity bootstrap finds, keeps and distributes
# (same key both places, so the distributor reports it Existing):
for ns in "$POC_NAMESPACE" "$LOGWEIR_NAMESPACE"; do
  kubectl --context "$CTX" -n "$ns" create secret generic logweir-signing-key \
    --from-file=signing.pem=poc-secrets/r1-signing.pem
done
kubectl --context "$CTX" -n "$POC_NAMESPACE" apply -f poc-baselines/r1/runner-sa.yaml
# The TrustRoster `default` with that signing key and an approver key, as
# v0.1.5's docs/install.md step 2 describes; then:
helm install logweir poc-baselines/r1/charts/logweir --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE" \
  -f deploy/poc/rehearsals/r1-v0.1.5.values.yaml --wait --timeout 15m
```

**Pre-upgrade state** (v0.1.5's own procedures): the two `KafkaCluster`s in
`logweir-poc`, a `BackupSchedule` there with at least one `Backup` that
succeeded and verified, and a `logweir-s3` credential in `logweir-poc` (v0.1.5
has no per-grant destinations: its demo flow is the chart's root pair, the
baseline's own shape — after the upgrade, new destinations use step 7's users).
Record the signing key id (`sha256` of `r1-signing.pub.pem`'s DER SPKI), each
schedule's UID and `metadata.generation`, and the backups' receipts.

**Before the upgrade, let Helm adopt the two objects v0.1.5 needed by hand in
`logweir-poc`** and this release now renders there — the runner account and the
demo archive credential. Helm refuses to take over an object it did not create
unless the object says which release owns it:

```bash
for obj in serviceaccount/logweir-runner secret/logweir-s3; do
  kubectl --context "$CTX" -n "$POC_NAMESPACE" annotate "$obj" --overwrite \
    meta.helm.sh/release-name=logweir meta.helm.sh/release-namespace="$LOGWEIR_NAMESPACE"
  kubectl --context "$CTX" -n "$POC_NAMESPACE" label "$obj" --overwrite app.kubernetes.io/managed-by=Helm
done
```

Then the three upgrade steps. **The live round must show, after R1:**
1. all fourteen CRDs `Established`; the six old kinds' objects unchanged;
2. **identity** — `logweir-signing-trust`'s `key-id` equals the recorded
   `r1-signing` key id, `logweir-signing-key` in both namespaces still holds
   that key, and the bootstrap and distributor logged `source=existing`;
3. `TrustRoster/default` resolving as `legacy-roster-v1` for `logweir-poc`
   until the `TrustPolicy` of step 8 governs it;
4. **schedules** — same UID, `metadata.generation` and history; the next slot
   fired by the new controller (release-note item 9);
5. **archive readability** — every pre-upgrade receipt verifies (the console's
   badge and `docs/verify_scorecard.py`), and the first post-upgrade restore of a
   pre-upgrade point completes only from a valid scorecard (item 8);
6. the console arrives Ready through Traefik, with sign-in per role (step 9);
7. the controller Ready on its exec probes.

### R2 — from `sha-f49849d…`

The last publication before PLAT-15.2/17.2/19.2: managed identity and a shared
console, but no `controller.watchNamespaces`, no `approvalPolicy.*` and none of
the chart-gap values. Its console cannot trust the local CA (that value did not
exist) and stays NotReady before the upgrade, which is why the install waits on
the controller only.

```bash
git archive "$R2_COMMIT" charts/logweir | (mkdir -p poc-baselines/r2 && tar -x -C poc-baselines/r2)
helm install logweir poc-baselines/r2/charts/logweir --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE" \
  -f deploy/poc/rehearsals/r2-f49849d.values.yaml --timeout 15m
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" rollout status deploy/weirkeeper --timeout=10m
```

**Pre-upgrade state:** everything the handoff's item table lists for R2 —
the `Enforce` retention policies without `s3:GetObject` and on a versioned
bucket, the shared backup set, the run whose ConfigMap never mounts, the
point-bound `Restore` without a Job, and a `Backup` verified under a key then
revoked for compromise — plus the identity key id, the schedules and the
receipts recorded as for R1.

Then: first run step 2 of the upgrade with **R2's own values** against the
candidate chart and see it refused, naming `controller.watchNamespaces`
(release-note item 6; `validate.sh` shows the same refusal offline); then the
three upgrade steps with this profile's values. **The live round must show,
after R2:** the R1 list's items 1, 2 (identity from the managed bootstrap:
`key-id` unchanged), 4, 5, 6 and 7, and release-note items 1–10 as the
handoff's table states them — each with the pre-upgrade state it names.

## Uninstall

```bash
helm uninstall logweir --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE"
helm uninstall dex --kube-context "$CTX" -n "$DEX_NAMESPACE"
kubectl --context "$CTX" delete -f deploy/poc/issuers.yaml
kubectl --context "$CTX" delete -f deploy/poc/minio-grants.yaml --ignore-not-found
helm uninstall cert-manager --kube-context "$CTX" -n "$CERT_MANAGER_NAMESPACE"
helm uninstall traefik --kube-context "$CTX" -n "$INGRESS_NAMESPACE"
```

What remains on purpose, and how to remove it, is
[docs/install.md](../../docs/install.md), *Uninstall, and what it leaves behind*:
the retained signing identity, the fourteen CRDs (with every custom resource,
including the `TrustPolicy`), the MinIO volume, and cert-manager's and Traefik's
CRDs. Delete the namespaces last, and remove `poc-secrets/` and the CA from
your keychain.

## What production keeps, and what the PoC stands in for

| Setting | In this profile | Production |
|---|---|---|
| Images and chart | one publication: the OCI chart and its four `sha-` images; upstream by digest | the same, or a release tag's chart (`--version <X.Y.Z>`); pin Logweir by digest if your registry policy requires it |
| Ingress controller | Traefik, 1 replica, fixed ClusterIP | **a maintained ingress controller** (Traefik, or another that is maintained), ≥ 2 replicas across nodes with a PDB; Gateway API with the same controller is an option later. ingress-nginx is retired and is not one |
| TLS | cert-manager local CA; HSTS at the entry point | your CA or ACME issuer — only `issuers.yaml` and the Ingress annotation change |
| Identity provider | Dex static users, bound by subject | your IdP (below), bound by group |
| Console's trust of the IdP | the local CA by `oidc.caBundle`; `dex.localtest.me` mapped to Traefik by `hostAliases` | a publicly trusted issuer needs neither; a corporate PKI keeps `oidc.caBundle`; split-horizon DNS keeps `hostAliases` |
| Console | `shared`, 2 replicas, PDB, liveness and readiness probes, non-root, read-only root filesystem, `requireTrustedProxy` | the same |
| Trusted proxy | the Traefik Service's serving pods (`trustedProxyService`) | the same, naming your ingress controller's Service |
| Controller | scoped to `logweir-poc` (`watchNamespaces`), non-root, read-only root filesystem, resources set, **exec liveness and readiness probes** | the same, one namespace per team |
| NetworkPolicies | rendered for the console, the runners and the controller; console egress to Dex by **pod selector** (`oidcPeers`, Traefik's pods on 8443) | the same — **enforced**. docker-desktop does not enforce NetworkPolicy, so here they are objects, not boundaries. An IdP outside the cluster is `oidcCIDRs` (its own addresses) instead |
| Dex | 2 replicas, PDB, Kubernetes storage | the same, or your IdP directly |
| Archive and Kafka | the chart's demo MinIO (root credential in chart values — the one exception; Logweir gets three least-privilege users) and brokers | your object store (per-role grants, [docs/install.md](../../docs/install.md) §3.11) and clusters; the connection objects in a watched namespace (`kubernetes.connectionsNamespace`) |
| Approval policy | `Ordinary` in `logweir-poc` | `Governed` for production namespaces, with approver keys on the `TrustPolicy` |

## Swapping in a real identity provider

Replace `staticPasswords` in `dex.values.yaml` with a Dex connector — LDAP,
GitHub, Microsoft or an upstream OIDC provider ([Dex connectors](https://dexidp.io/docs/connectors/)) —
whose groups reach the ID token's `groups` claim (the console asks for the
`groups` scope). Then bind the console's roles by group in
`logweir.values.yaml`'s `api.console.roles.bindings[].groups`, drop the
`subjects` entries, and bump `roles.revision`. Pointing the console at your IdP
directly instead of through Dex changes `api.console.oidc.issuer`, the client
id, the `logweir-console-oidc` Secret, `networkPolicy.oidcCIDRs` (the IdP's own
addresses, replacing `oidcPeers`), and drops `hostAliases` and — for a publicly
trusted IdP — `oidc.caBundle`. The redirect URI registered with the IdP is
`https://<console host>/auth/callback`.

## The chart gaps, closed

The first version of this profile needed a `kubectl patch` after every install
and upgrade, and two addresses read from the cluster at install time. Each gap
is now a chart value or a chart behaviour; [charts/logweir/README.md](../../charts/logweir/README.md)
documents each one.

| Gap | Was | Now |
|---|---|---|
| G1 — the console could not trust a private CA for its issuer | a patch mounting the CA and setting `SSL_CERT_FILE` (replacing the system roots) | `api.console.oidc.caBundle: {configMap \| secret, key}`: the bundle is added to the system roots (`oidc.systemRoots: false` to drop them); an unreadable or empty bundle refuses to start |
| G2 — `dex.localtest.me` resolved to the console pod itself | a patch adding `hostAliases` with an address read at install | `api.console.hostAliases`, with Traefik's fixed ClusterIP. Chosen over a separate back-channel URL: it changes only where the name resolves, so the URL, the TLS name check and the exact `iss` comparison all stay on the one configured issuer |
| G3 — the chart's own connection objects landed in the release namespace, which a scoped controller cannot watch | the PoC created them through the console instead | `kubernetes.connectionsNamespace`; the render fails when it is not watched |
| G4 — the chart was not published | installed from a checkout of the image commit | `oci://registry-1.docker.io/vladyslavhaina/logweir-chart`, published by `images.yml` beside the images, versioned with them |
| G5 — the controller had no probes | a wedged controller was never restarted | exec liveness and readiness (`weirkeeper --probe`) against a loopback health listener |
| G6 — `trustedProxyCidrs` needed the ingress pod's `/32` | re-read and reinstalled after every ingress pod restart | `api.console.trustedProxyService`: the ingress Service's serving pods, re-read every five seconds |

## What this profile has not been run to show

It was written and rendered without a cluster: `validate.sh` renders all four
charts at their pinned versions (Logweir both as published and from a
checkout, and the two rehearsal baselines with their own charts), checks every
image is pinned, the cross-file addresses agree and the rendered Dex
configuration carries no secret.
[UNVERIFIED — this profile has not been installed, signed into or upgraded on docker-desktop yet; that is PLAT-20.2's live round.]
The live round must show, on docker-desktop, with `LOGWEIR_COMMIT` set to the
first publication carrying these chart fixes:

1. **The install is Helm only**: steps 1–8 run as written, from the OCI chart,
   with no `kubectl patch` and no address read from the cluster; the Traefik,
   Dex and Logweir releases reach `--wait` Ready.
2. **G1** — the console reaches `Ready` against Dex's locally issued
   certificate with `oidc.caBundle`; with the ConfigMap's key renamed the pod
   stays in `ContainerCreating`, and with an empty `ca.crt` it exits 2 naming
   the bundle.
3. **G2** — inside a console pod, `dex.localtest.me` resolves to `10.96.0.80`
   (`kubectl exec deploy/logweir-api -- getent hosts dex.localtest.me`), and
   sign-in completes.
4. **G6** — `/readyz` is `200` and sign-in works; `kubectl rollout restart
   deploy/traefik`, and within seconds of the new pod serving, requests succeed
   again with no step re-run; the console log shows the trusted set moving to
   the new address; a request sent to the console Service from another pod with
   `X-Forwarded-Proto: https` is answered `421`.
5. **G5** — the controller Deployment is Ready through `weirkeeper --probe
   ready`; `kubectl exec deploy/weirkeeper -- weirkeeper --probe live` exits 0.
6. **G3** — `logweir-s3` is in `logweir-poc`, not `logweir-system`.
7. **G4** — `helm show chart "$LOGWEIR_CHART" --version "$LOGWEIR_CHART_VERSION"`
   reads `appVersion: $LOGWEIR_TAG`, and every Logweir pod's image is
   `…:$LOGWEIR_TAG`.
8. **The MinIO grants** — the destination's four grants are the three
   least-privilege users; the backup verifies green; a `DeleteObject` with the
   writer's key is refused by MinIO.
9. **Sign-in per role and the first backup and restore** (steps 9–10), and
   HSTS on both hosts.
10. **The two upgrade rehearsals** with the checks listed under each, and a
    rollback to each starting point with the release notes' rollback list.

---

Documentation is licensed [CC-BY-4.0](../../docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
