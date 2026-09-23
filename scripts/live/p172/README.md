# PLAT-17.2 live harness: the shared console, its RBAC, and an expired session

This is the live proof for PLAT-17.2. It runs against the docker-desktop lab. A
source-built `logweir-api` runs in `mode: shared` behind a TLS stand-in ingress.
People sign in through a local OpenID provider. The chart's two ServiceAccounts
are emulated in namespaces this run creates. Every row asserts the answer it
records, and a row that does not hold fails the run.

| file | what it is |
|---|---|
| `run.sh` | the driver: `run.sh all` or `run.sh expired` |
| `env.sh` | names, the owner label, binaries and the timeout wrapper; every script sources it |
| `setup.sh` | namespaces A, B, CTL and Z; the chart's API and scoped-controller RBAC as namespaced copies; the cluster-scoped halves as renamed, owner-labelled copies; ServiceAccount kubeconfigs with 2 h tokens |
| `cani.sh` | the `kubectl auth can-i` matrix for both ServiceAccounts, each row with its expected answer |
| `summarize_controller.py` | the scoped controller's start lines and every refused call |
| `live_p172.py` | the shared-console rows: sign-in, the role matrix, forged headers, the unbound namespace, CSRF, unauthenticated API and stream, the trusted-entry-point 421, attribution and audit |
| `live_p172_expired.py` | the expired-session rows |
| `mock_idp.py` | the local ES256 OpenID provider on 127.0.0.1 |
| `tls_proxy.py` | the TLS stand-in ingress |
| `cleanup.sh` | owner-checked deletion, and removal of the run's credential files |

## Running it

```bash
export LOGWEIR_PYTHON=/tmp/logweir-roadmap-run/venv/bin/python3   # needs `cryptography`
cargo build --locked -p logweir-api -p weirkeeper                 # target/debug/{logweir-api,weirkeeper}

# The expired-session rows. Namespaced only, so no cluster lock is needed. About 2 minutes.
P172_OWNER=my-task P172_PREFIX=lw-mytask-p172- bash scripts/live/p172/run.sh expired

# The full shared-console proof. It creates ClusterRoles and ClusterRoleBindings,
# so hold the cluster lock.
/tmp/logweir-roadmap-run/claude/k8s-lock.sh acquire my-task
P172_OWNER=my-task P172_PREFIX=lw-mytask-p172- bash scripts/live/p172/run.sh all
/tmp/logweir-roadmap-run/claude/k8s-lock.sh release my-task
```

The output goes to `$OUT`. It defaults to
`/tmp/logweir-roadmap-run/claude/artifacts/plat17-2-live/<TS>`, created with mode 0700.
- `run.log` holds the whole run.
- `rows.json` holds the rows from `live_p172.py` or `live_p172_expired.py`.
- `all` also writes `cani.txt`, `weirkeeper-scoped.log` and its summary, `local.txt` and
  the logs of the console, the provider and the proxy.

`run.sh` exits non-zero when any row, any can-i expectation or the localAdmin check
fails.

Environment: see `env.sh` (`TS`, `OUT`, `P172_OWNER`, `P172_PREFIX`, `P172_KEEP`,
`LOGWEIR_REPO`, `LOGWEIR_API_BIN`, `LOGWEIR_WEIRKEEPER_BIN`, `LOGWEIR_PYTHON`,
`LWTIMEOUT`).

## What it starts, on the loopback interface only

- **The local OIDC mock** (`mock_idp.py`). It serves discovery, JWKS, `/authorize`
  and `/token`, and signs ES256 ID tokens with a key generated at start.
  - `/token` checks `client_secret_basic` and PKCE S256.
  - The harness chooses who signs in next with `POST /control/next-user`.
  - The client secret is minted for each run with `openssl rand` or `os.urandom`,
    written to `$OUT/client-secret` with mode 0600, and passed to the mock and to
    the console **by path**. It is never on a command line or in this source.
- **The TLS terminator** (`tls_proxy.py`). It uses a self-signed certificate
  minted for each run. It forwards to the console over HTTP and overwrites
  `X-Forwarded-For` and `X-Forwarded-Proto`, as an ingress controller does.
  Bodies stream, so the SSE route works through it.
- **The console** (`logweir-api`, shared mode). The two modes differ as follows:

  | | `all` | `expired` |
  |---|---|---|
  | Kubernetes credentials | the emulated `logweir-api` ServiceAccount's kubeconfig | the `docker-desktop` context |
  | `sessionMaxAgeSeconds` | 900 | 60, the minimum the config accepts |

- **`all` only: a scoped `weirkeeper` for 40 s**, running as the emulated
  controller ServiceAccount, and then a localAdmin `logweir-api` on 127.0.0.1:18486.

The ports are fixed:

| mode | console | ingress | OIDC mock |
|---|---|---|---|
| `all` | 18484 | 18443 | 18555 |
| `expired` | 18494 | 18453 | 18565 |

## The expired-session rows (`run.sh expired`)

Alice signs in as an operator through the real code and PKCE redirects. This client
then replays the same cookie value. A browser would drop it at `Max-Age`, but the
server must not depend on that.
1. **Control.** Inside the session's life, `GET /api/v1/session` and the connections
   list answer 200, the operation-events stream opens (200, `text/event-stream`, a
   first frame), and a create with the CSRF token answers 201.
2. **Past the signed `exp`** (60 s plus 3), the same GET and the same list are each
   refused with 401 `session_expired`.
3. The stream is refused with 401 `session_expired`, as `application/problem+json`
   and never an event stream.
4. The same create, with the same CSRF token, is refused with 401 `session_expired`
   and creates nothing.

A stream that is already open was authorised when it subscribed. It outlives `exp`
until the 300 s connection ceiling. That is the product's documented design
(`crates/logweir-api/src/routes/operations.rs`, `events`), and it is not asserted
here.

## Cleanup and the owner label

- **Labels.** Every namespace, and every cluster-scoped `ClusterRole` or
  `ClusterRoleBinding`, carries `logweir.dev/test-owner=$P172_OWNER`. Names start
  with `$P172_PREFIX`.
- **`all`.** `cleanup.sh` runs from an `EXIT` trap unless `P172_KEEP=1`. It
  deletes a namespace, or one of the three cluster RBAC objects, only when that
  object's owner label reads `$P172_OWNER`, and prints the UID it deleted.
- **`expired`.** `live_p172_expired.py` deletes its one namespace only when both
  the label and the UID it recorded at creation match. It does the same when setup
  fails partway.
- **Files.** Both paths remove the run's credential files from `$OUT`: the
  ServiceAccount kubeconfigs, the session and cursor keys, the client secret and
  the TLS private key.
- **Shared objects.** The lab's own `logweir-scram-local` release is only read.

Every `kubectl` names `--context docker-desktop`. Docker Desktop does not enforce
NetworkPolicy, so no row claims a network deny path.
