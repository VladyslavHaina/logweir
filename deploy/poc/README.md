# Logweir PoC install profile: Helm, ingress, TLS and SSO

This directory is a **versioned proof-of-concept installation** of the supported
path in [docs/quickstart.md](../../docs/quickstart.md): everything installed with
Helm and the **published Docker Hub images** (`docker.io/vladyslavhaina/weirkeeper`,
`logweir`, `logweir-console`, `logweir-ui`, by their immutable `sha-<commit>` tags),
the console in `shared` mode behind **ingress-nginx**, with real TLS from a
**cert-manager** local CA and sign-in through **Dex**. No image is built locally
and no binary is installed by hand; the host needs `helm`, `kubectl`, `openssl`,
`htpasswd` and a checkout of this repository at the release commit.

It runs on docker-desktop Kubernetes as a PoC, and every setting is the one a
production install keeps unless the table in
[*What production keeps*](#what-production-keeps-and-what-the-poc-stands-in-for)
says the PoC stands something in for it.

| File | What it is |
|---|---|
| [versions.env](versions.env) | every version: the Logweir commit and `sha-` tag, the previous tag for the upgrade, the three upstream chart versions, namespaces and hostnames |
| [ingress-nginx.values.yaml](ingress-nginx.values.yaml) | the ingress controller (chart 4.15.1) |
| [cert-manager.values.yaml](cert-manager.values.yaml) and [issuers.yaml](issuers.yaml) | cert-manager v1.21.2, images pinned by digest, and the local CA `ClusterIssuer` |
| [dex.values.yaml](dex.values.yaml) | Dex 0.24.1 (v2.44.0): one static user per Logweir role, secrets from a Secret |
| [logweir.values.yaml](logweir.values.yaml) | Logweir: shared console, scoped controller, Ingress, NetworkPolicy, approval policy, demo Kafka and MinIO |
| [trustpolicy.sh](trustpolicy.sh) | prints the `TrustPolicy` for `logweir-poc` from the cluster's public signing key and the console's confirmation key |
| [console-oidc-trust.patch.yaml](console-oidc-trust.patch.yaml) | the PoC's stand-in for two chart values that do not exist yet (*Chart gaps* G1, G2) |
| [validate.sh](validate.sh) | renders all four charts with the pinned versions and these values, without a cluster, and checks every image is pinned |

**Why `deploy/poc/` and not `charts/logweir/examples/`.** Everything under
`charts/logweir/examples/` ships inside the Logweir chart package and is
rendered by `just chart-check`, which holds the four Logweir images at the
chart's default `:latest` tag and knows nothing of other charts. This profile
pins `sha-` tags and configures three other charts, so it lives beside the
chart rather than inside it, and [validate.sh](validate.sh) is its render gate.

Hostnames: `logweir.localtest.me` (the console) and `dex.localtest.me` (Dex).
`localtest.me` and all its subdomains resolve to `127.0.0.1` in public DNS, and
docker-desktop publishes the ingress controller's `LoadBalancer` Service on the
host's `127.0.0.1:80/443`.

## Prerequisites

- docker-desktop with Kubernetes enabled (1.30 or newer; the admission policy
  needs 1.30), ports 80 and 443 free on the host, and — on Apple silicon —
  amd64 emulation (the runner image is `linux/amd64`).
- `helm` 3.15+ or 4, `kubectl`, `openssl`, `htpasswd` (macOS ships it), `git`.
- A checkout of this repository at the release commit, so the chart matches the
  images:

```bash
git clone https://github.com/VladyslavHaina/logweir && cd logweir
. deploy/poc/versions.env
git checkout "$LOGWEIR_COMMIT"
CTX=docker-desktop     # every command below names its context explicitly
```

Every secret below is generated on your machine into `./poc-secrets/`
(mode 0700) and loaded into the cluster as a Secret; nothing secret is in a
values file, a rendered manifest or Helm's release history.

```bash
install -d -m 0700 poc-secrets
```

## 1. Ingress controller

```bash
helm upgrade --install ingress-nginx ingress-nginx --repo "$INGRESS_NGINX_REPO" \
  --version "$INGRESS_NGINX_CHART_VERSION" --kube-context "$CTX" \
  -n "$INGRESS_NAMESPACE" --create-namespace \
  -f deploy/poc/ingress-nginx.values.yaml --wait --timeout 10m
```

**Check:** `kubectl --context "$CTX" -n ingress-nginx get svc ingress-nginx-controller`
shows `EXTERNAL-IP localhost`, and `curl -sk https://logweir.localtest.me` answers
`404` from nginx (nothing routes there yet).

## 2. Certificates: cert-manager and the local CA

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

## 3. Namespaces

```bash
for ns in "$LOGWEIR_NAMESPACE" "$POC_NAMESPACE" "$DEX_NAMESPACE"; do
  kubectl --context "$CTX" create namespace "$ns" --dry-run=client -o yaml \
    | kubectl --context "$CTX" apply -f -
done
```

`logweir-poc` must exist before Logweir installs: the chart copies the signing
identity and the runner account into it.

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
returns a document whose `issuer` is exactly `https://dex.localtest.me`.

## 5. Logweir's own Secrets

All three live in `logweir-system`, which the controller cannot create Jobs in
(`controller.watchNamespaces`):

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
# The local CA's PUBLIC certificate, for the console's trust of Dex (G1).
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" create configmap logweir-poc-ca \
  --from-file=ca.crt=poc-secrets/ca.crt
```

## 6. Logweir

The console trusts exactly one peer to have terminated TLS — the ingress
controller pod — and reaches Dex through the ingress controller Service. Both
addresses are the cluster's, so they are read and passed at install:

```bash
INGRESS_POD_IP="$(kubectl --context "$CTX" -n ingress-nginx get pod \
  -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].status.podIP}')"
INGRESS_SVC_IP="$(kubectl --context "$CTX" -n ingress-nginx get svc ingress-nginx-controller \
  -o jsonpath='{.spec.clusterIP}')"
helm upgrade --install logweir ./charts/logweir --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE" \
  -f deploy/poc/logweir.values.yaml \
  --set-json "api.console.trustedProxyCidrs=[\"$INGRESS_POD_IP/32\"]" \
  --set-json "api.console.networkPolicy.oidcCIDRs=[\"$INGRESS_SVC_IP/32\"]" \
  --wait --timeout 15m
# G1 + G2 stand-in, after EVERY install or upgrade of this release:
sed "s/INGRESS_SVC_IP/$INGRESS_SVC_IP/" deploy/poc/console-oidc-trust.patch.yaml > poc-secrets/patch.yaml
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" patch deployment logweir-api \
  --type strategic --patch-file poc-secrets/patch.yaml
kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" rollout status deployment/logweir-api --timeout=5m
```

The ingress controller's pod IP changes if that pod is recreated; re-run this
step when it does (the console then answers `421` to everything, which is the
safe failure).

**Check:**
- `kubectl --context "$CTX" -n logweir-system get deploy` shows `weirkeeper` and
  `logweir-api` (2/2) ready, and `get pdb,networkpolicy,ingress` lists the
  console's budget, policies and Ingress;
- `kubectl --context "$CTX" -n logweir-system get configmap logweir-signing-trust -o jsonpath='{.data.key-id}'`
  prints the installation signer's key id (back up the identity now:
  [docs/install.md](../../docs/install.md), *Back up and recover the installation identity*);
- `curl --cacert poc-secrets/ca.crt https://logweir.localtest.me/readyz` answers `200`.

## 7. Trust

As a cluster administrator (the PoC's kubeconfig is one), put the two public
keys on a `TrustPolicy` that governs `logweir-poc`:

```bash
bash deploy/poc/trustpolicy.sh poc-secrets/confirmation.pub.pem > poc-secrets/trustpolicy.yaml
kubectl --context "$CTX" apply -f poc-secrets/trustpolicy.yaml
```

**Check:** `kubectl --context "$CTX" get trustpolicy logweir-poc` reads `LOADED
True` and `BOUND logweir-poc`.

## 8. First sign-in, per role

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

## 9. First backup and restore, in the console

Signed in as `operator`, follow [the supported path](../../docs/quickstart.md#the-supported-path-install-to-disaster-restore)
steps 4–8 with these PoC values:

| Step | Value |
|---|---|
| Source connection | `source`, role `source`, `logweir-kafka-source.logweir-system.svc.cluster.local:9092`, plaintext |
| Target connection | `target`, role `target`, `logweir-kafka-target.logweir-system.svc.cluster.local:9092`, plaintext |
| Destination | `primary`: endpoint `http://logweir-minio.logweir-system.svc:9000` (transport `InsecureHTTP`, the demo MinIO speaks no TLS), region `us-east-1`, path-style, bucket `kafka-backups`, prefix `poc`; every grant (including `evidenceRead`) the demo root credential `minioadmin` / `minioadmin`, entered once as a new credential |
| Schedule | `orders-nightly`, source `source`, topics `orders` and `payments` (the demo seeds both), daily, destination `primary`; then *Run first backup now* |
| Restore | from the schedule's page, *Restore this point*; target `target`, new-topic prefix `restored-`; the readiness check; *Create the Restore*; confirm it (Ordinary) |

**Check:** the backup's operation page reads `Succeeded` with *verified by
weirkeeper … against key …*; the restore reaches `Succeeded` with its completion
panel, and the `restored-` topics exist on the target broker.

## Upgrade

The rehearsal is an upgrade from the previous published tag to this one. Install
the previous release first — the same steps with `versions.env`'s
`LOGWEIR_PREVIOUS_COMMIT` checked out and every `sha-` tag in
`logweir.values.yaml` set to `LOGWEIR_PREVIOUS_TAG` — take a backup, then:

```bash
git checkout "$LOGWEIR_COMMIT"
# Read docs/release-notes.md first: it lists the required actions in order.
kubectl --context "$CTX" apply --server-side -f charts/logweir/crds/
for crd in $(ls charts/logweir/crds | sed 's/\.yaml$//'); do
  kubectl --context "$CTX" wait --for=condition=Established "crd/$crd.logweir.dev" --timeout=60s
done
# then step 6's helm upgrade --install and the patch, unchanged
```

**What must survive**, and how to check it:
- the installation identity: `logweir-signing-trust`'s `key-id` is the one
  recorded before;
- every schedule: same UID and `metadata.generation`, its history intact, its
  next slot fired by the new controller;
- the archive: the pre-upgrade backup still shows its green badge, and a
  pre-upgrade point still restores.

Rollback is the previous tag with the previous commit's chart, after the
checklist in [docs/release-notes.md](../../docs/release-notes.md), *Migration and
rollback*; the CRDs stay.

## Uninstall

```bash
helm uninstall logweir --kube-context "$CTX" -n "$LOGWEIR_NAMESPACE"
helm uninstall dex --kube-context "$CTX" -n "$DEX_NAMESPACE"
kubectl --context "$CTX" delete -f deploy/poc/issuers.yaml
helm uninstall cert-manager --kube-context "$CTX" -n "$CERT_MANAGER_NAMESPACE"
helm uninstall ingress-nginx --kube-context "$CTX" -n "$INGRESS_NAMESPACE"
```

What remains on purpose, and how to remove it, is
[docs/install.md](../../docs/install.md), *Uninstall, and what it leaves behind*:
the retained signing identity, the fourteen CRDs (with every custom resource,
including the `TrustPolicy`), the MinIO volume, and cert-manager's CRDs
(`crds.keep`). Delete the namespaces last, and remove `poc-secrets/` and the CA
from your keychain.

## What production keeps, and what the PoC stands in for

| Setting | In this profile | Production |
|---|---|---|
| Images | Logweir by immutable `sha-` tag; upstream by digest | the same; pin Logweir by digest if your registry policy requires it |
| TLS | cert-manager local CA | your CA or ACME issuer — only `issuers.yaml` and the Ingress annotations change |
| Identity provider | Dex static users, bound by subject | your IdP (below), bound by group |
| Console | `shared`, 2 replicas, PDB, liveness and readiness probes, non-root, read-only root filesystem, `requireTrustedProxy` | the same |
| Controller | scoped to `logweir-poc` (`watchNamespaces`), non-root, read-only root filesystem, resources set; **no probes** (G5) | the same, one namespace per team |
| NetworkPolicies | rendered for the console, the runners and ingress-nginx | the same — **enforced**. docker-desktop does not enforce NetworkPolicy, so here they are objects, not boundaries |
| `trustedProxyCidrs` | the ingress controller pod's `/32`, re-read after a restart | the ingress node pool's pod range (no wider than `/16`, containing no other pod) |
| Ingress controller | 1 replica | ≥ 2 replicas across nodes, with the PDB this values file already asks for |
| Dex | 2 replicas, PDB, Kubernetes storage | the same, or your IdP directly |
| Archive and Kafka | the chart's demo MinIO and brokers | your object store (per-role grants, [docs/install.md](../../docs/install.md) §3.11) and clusters |
| Approval policy | `Ordinary` in `logweir-poc` | `Governed` for production namespaces, with approver keys on the `TrustPolicy` |
| Console trust of the IdP | `console-oidc-trust.patch.yaml` (G1, G2) | unnecessary with a publicly trusted IdP |

## Swapping in a real identity provider

Replace `staticPasswords` in `dex.values.yaml` with a Dex connector — LDAP,
GitHub, Microsoft or an upstream OIDC provider ([Dex connectors](https://dexidp.io/docs/connectors/)) —
whose groups reach the ID token's `groups` claim (the console asks for the
`groups` scope). Then bind the console's roles by group in
`logweir.values.yaml`'s `api.console.roles.bindings[].groups` and drop the
`subjects` entries, and bump `roles.revision`. Pointing the console at your IdP
directly instead of through Dex changes `api.console.oidc.issuer`, the client
id, the `logweir-console-oidc` Secret and `networkPolicy.oidcCIDRs` (the IdP's
own addresses), and the redirect URI registered with the IdP is
`https://<console host>/auth/callback`.

## Chart gaps

What the profile needs that the Logweir chart cannot express today. None was
changed here; each is the chart owner's.

- **G1 — the console cannot be told to trust a private CA for its OIDC issuer.**
  `charts/logweir/templates/ui/api-deployment.yaml:186` (the `api` container)
  offers no env, extra volume or CA-bundle value; `logweir-api` reads system
  roots through rustls-native-certs, so only a mounted bundle plus
  `SSL_CERT_FILE` works. Stand-in: `console-oidc-trust.patch.yaml`. Wanted: an
  `api.console.oidc.caBundle` (ConfigMap reference) the chart mounts and points
  `SSL_CERT_FILE` at.
- **G2 — no way to resolve the issuer's public name to an in-cluster address.**
  Same file, the pod spec at `:160`: no `hostAliases` (or `dnsConfig`) value, so
  on a laptop cluster `dex.localtest.me` resolves to the console pod itself.
  Stand-in: the same patch. Wanted: `api.console.hostAliases`.
- **G3 — the chart's own connection objects land where a shared console cannot
  use them.** `charts/logweir/templates/kafka/kafkacluster.yaml:26` renders
  `kafka.enabled`'s `KafkaCluster`s, and `templates/minio/minio.yaml:37` the
  demo's `logweir-s3` credential, in the **release** namespace — which shared
  mode forbids `controller.watchNamespaces` to include. The PoC creates its
  connection and destination in `logweir-poc` through the console instead.
- **G4 — the chart is not published.** The images are on Docker Hub by `sha-`
  tag, but the chart is installed from a checkout of the same commit; no workflow
  packages or pushes it (for example as an OCI artifact beside the images).
- **G5 — the controller Deployment has no probes.**
  `charts/logweir/templates/deployment.yaml:76` renders no liveness or readiness
  probe for `weirkeeper`, and the controller serves no health endpoint; a wedged
  controller is not restarted by the kubelet.
- **G6 — `trustedProxyCidrs` is an address list, not a selector.**
  `templates/ui/api-config.yaml:172-179` takes CIDRs only, so trusting "the
  ingress controller pods" means a `/32` that changes when the pod does, or a
  range as wide as the pods' node pool.
- Wording owed with them: `templates/ui/api-config.yaml:101` and
  `values.yaml:227` still say "twenty-two" page files (twenty-six ship).

## What this profile has not been run to show

It was written and rendered without a cluster: `validate.sh` renders all four
charts at their pinned versions with these values and checks that every image
is pinned and that the rendered Dex configuration carries no secret.
[UNVERIFIED — this profile has not been installed, signed into or upgraded on docker-desktop yet; that is PLAT-20.2's live round.]
In particular the stand-in patch surviving a `helm upgrade`, Dex's static-user
subjects matching the console's bindings, and the console accepting the ingress
controller as its trusted proxy are exactly what that round must show.

---

Documentation is licensed [CC-BY-4.0](../../docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
