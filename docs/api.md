# The product API (`logweir-api`)

`logweir-api` is a small HTTP service that serves the static UI at `/ui/` and a
typed JSON API at `/api/v1` on one origin, and that reads and creates the
existing `logweir.dev/v1alpha1` resources through one Kubernetes adapter.

**It is not a Kubernetes proxy.** There is no generic
`{group}/{version}/{resource}` route, no request-supplied Kubernetes path, no
Secret, Pod, log, exec or Job access, no delete, and no arbitrary patch. It
creates custom resources; `weirkeeper` remains the only execution authority and
isolated runner Jobs remain the data-plane boundary. The service dials no Kafka
broker and no object store.

## What ships today, and what does not

**Two modes, and the configuration file must name one.** There is no default:
a file that forgets to say which mode it wants is refused rather than read as
the more permissive one.

* `mode: localAdmin` — a loopback-only listener, the configured administrator
  as the actor, and namespaces from configuration alone. Not SSO, not a shared
  console.
* `mode: shared` — the SSO console: OpenID Connect identity, a short-lived
  encrypted session cookie, a synchronizer CSRF token on every unsafe method,
  exact role and namespace bindings, and one audit record per request. It
  refuses a non-HTTPS `publicBaseUrl` before it binds anything.

`logweir-api` is **not packaged or deployed**. No image builds it, the Helm
chart has no `console` template or value, and `publish = false` keeps it out of
the release archives. It runs from a local build against a kubeconfig context.
Nothing about an existing installation changes when this crate is present, so
there is nothing to upgrade, migrate or roll back: removing the crate removes
the feature. The chart, image, ingress and NetworkPolicy work is a later stage.

**Shared mode is therefore not a supported deployment yet.** It is implemented
and tested, and it runs from a local build behind a TLS terminator, but the
console image, the chart's `console.*` templates, the ingress, the
NetworkPolicy, the API's own ServiceAccount and its per-namespace RoleBindings
do not exist. Until they do, the API runs with whatever Kubernetes identity its
kubeconfig carries, which may be cluster-admin: the closed adapter is a
**source-level** bound on what it can reach, not an RBAC one.

A domain whose routes do not exist yet has **no route at all** — no stub and no
`501`. `GET /api/v1/session` reports each one as `false` under `capabilities`,
so a client learns what is unavailable instead of discovering it from an error.
Today that is: connection tests, write-only credential input, topic discovery,
preflight checks, saved destinations, manual backup creation, approval
submission and operation event streams.

## Running it

```yaml
# config.yaml
mode: localAdmin                       # the only accepted value
listen: "127.0.0.1:8484"               # a loopback IP literal; a hostname is refused
publicOrigin: "http://127.0.0.1:8484"  # the exact Origin unsafe requests must carry
uiDirectory: ./ui
localAdmin:
  subject: admin
  displayName: Local administrator     # never used for authorization
namespaces: [team-a]                   # explicit grants; core Namespaces are never listed
kubernetes:
  source: kubeconfig                   # or `inCluster`
  context: docker-desktop              # required; `current-context` is never used
cursorKeyFile: ./cursor.key            # at least 32 bytes
```

```console
$ head -c 32 /dev/urandom > cursor.key && chmod 600 cursor.key
$ cargo run -p logweir-api -- --config config.yaml
```

**Paths in this file are taken literally.** A relative one — `./ui`,
`./cursor.key` above — resolves against the **configuration file's own
directory**, not the working directory, so moving the file moves what it names.
A leading `~` is **not** expanded: `kubeconfig: ~/.kube/config` would look for a
directory actually named `~`, and the service would exit 2 saying it cannot read
that file. Write an absolute path, or omit `kubernetes.kubeconfig` entirely as
the example does — with the key absent the usual `KUBECONFIG`-then-`~/.kube/config`
lookup applies, performed by the Kubernetes client library and by the shell
conventions it implements, which is the only place that expansion belongs.
`kubernetes.context` is required either way.

Exit codes: `0` after a clean `SIGTERM`/`SIGINT`, `2` when the configuration is
refused — before any Kubernetes client or socket exists — and `1` for any other
startup or runtime failure.

The configuration is validated first, and that order is the security property.
`listen` must parse as a loopback IP literal: `localhost` is refused even though
it usually resolves to loopback, because resolution is an input this process
does not control. `0.0.0.0`, `[::]` and any routable address are refused by
name. So is a kubeconfig context whose user impersonates another identity, a
`mode` this release does not implement, and a cursor key shorter than 32 bytes.

`/healthz` answers whenever the process is alive and consults nothing.
`/readyz` additionally requires that Kubernetes answers for this service's
identity; its body never says which endpoint or why.

## Routes

All product routes are under `/api/v1`. The route table is the boundary:
anything not listed is `404`.

| Route | What it does |
|---|---|
| `GET /api/v1/session` | The actor, the explicit namespace grants and the capability flags. |
| `GET /api/v1/namespaces` | The configured grants. It never lists core `Namespace` objects. |
| `GET /api/v1/namespaces/{ns}/connections[/{name}]` | `KafkaCluster` projections: role, bootstrap addresses, auth mode, username, TLS, the credential Secret's **name**, and the controller's reachability observation. |
| `POST /api/v1/namespaces/{ns}/connections` | Create a `KafkaCluster` that references an existing credential Secret by name. |
| `GET /api/v1/namespaces/{ns}/schedules[/{name}]` | `BackupSchedule` projections. |
| `POST /api/v1/namespaces/{ns}/schedules` | Create a `BackupSchedule`. |
| `POST /api/v1/namespaces/{ns}/schedules/{name}:set-suspension` | The one permitted update. |
| `GET /api/v1/namespaces/{ns}/backups[/{name}]` | `Backup` projections. |
| `GET /api/v1/namespaces/{ns}/restores[/{name}]` | `Restore` projections. |
| `POST /api/v1/namespaces/{ns}/restores` | Create a `Restore`, preserving the plan bytes exactly. |
| `GET /api/v1/namespaces/{ns}/approvals[/{name}]` | Approval metadata and status. |
| `GET /api/v1/namespaces/{ns}/approvals/{name}/packet` | The raw approval document, only through this explicit route. |
| `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}` | The normalized operation status; `kind` is the closed set `backup\|restore`. |

`schemas/logweir-api-v1.openapi.json` is the generated contract. `just schema`
rewrites it and `just schema-check` fails on drift, as does
`crates/logweir-api/tests/contract.rs`, which compares the checked-in bytes with
the generator in-process.

There is no cancel and no delete for `Backup` or `Restore`: their external side
effects and cleanup semantics are not defined yet. Aborting a read cancels only
that HTTP and Kubernetes read; it never retracts an accepted mutation.

## The Kubernetes boundary

Every Kubernetes call is a method on one adapter, typed over a **sealed** set of
five resources — `KafkaCluster`, `BackupSchedule`, `Backup`, `Restore` and
`Approval`. No method takes a group, a version, a plural or a path, so no
request can name a sixth kind. The only update is
`BackupSchedule.spec.suspend`, sent as a merge patch whose body is built from a
boolean and a `resourceVersion` here, never from a request.

Every call carries a **10-second deadline**, and the client's own connect, read
and write timeouts are set to the same bound. A timeout is `504
upstream_timeout`. No Kubernetes message is returned verbatim: the status code
and reason class decide the problem, and the message is redacted — bearer
tokens, URL userinfo and anything past 512 bytes removed — before it reaches the
log. Nothing from an inbound request reaches an outbound Kubernetes request but
validated names.

## Conventions

**Errors are `application/problem+json`**, always, with a stable `code` and the
`requestId`:

```json
{
  "type": "https://logweir.dev/problems/validation-failed",
  "title": "Request validation failed",
  "status": 422,
  "code": "validation_failed",
  "detail": "One or more fields are invalid.",
  "requestId": "01M2N5SHWR0N2E7SDCW2H22Z8W",
  "retryable": false,
  "errors": [
    {"field": "bootstrapServers[0]", "code": "invalid_port",
     "message": "must be host:port with no scheme or userinfo"}
  ]
}
```

Mutation bodies are strict: an unknown field is `422 validation_failed` naming
the field, not a silently ignored key. So is an unknown or repeated query
parameter (`400 malformed_request`). Bodies are capped at 1 MiB.

**Every response** carries `X-Request-ID` — freshly minted; a client-supplied ID
is ignored — a Content-Security-Policy, `X-Content-Type-Options: nosniff`,
`Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`, a restrictive
Permissions-Policy, both Cross-Origin-*-Policy headers, and `Cache-Control:
no-store` unless a static asset set its own. One log line per request records
the method, path, status and latency — never a query string, a header value or a
body.

**Three guards run before any handler.** Any `Impersonate-*` header is refused
with `400 header_not_allowed` naming it; this service never impersonates, under
any configuration. A `Host` this listener does not serve is `421`, which is what
stops DNS rebinding against a loopback listener. And an unsafe method must carry
an `Origin` exactly equal to `publicOrigin` (`403`) and `Content-Type:
application/json` (`415`). The identity stage adds its CSRF check at the same
seam.

**Durable `POST`s require `Idempotency-Key`**, 8–128 visible ASCII characters,
and Kubernetes is the only store. The object's name is a per-route prefix plus
130 bits of `SHA-256(issuer, subject, namespace, route, key)`, so a lost
response, a double click, a restart or a second replica all target the same
name and the API server's `AlreadyExists` is what makes a second object
impossible. The object records the scope hash and the canonical request hash:
the same key with the same request replays as `200` with the same UID, the same
key with a different request is `409 idempotency_conflict`, and an object that
this scope did not create is `409 state_conflict` and is never adopted. The raw
key is never stored or logged — only hashes derived from it. A deliberately new
operation uses a new key.

`:set-suspension` is the exception: it takes no `Idempotency-Key` and instead
requires `expectedResourceVersion`, which is the value last read. That
precondition is on the **whole object**, so a controller status write between
your read and your write makes it `412 precondition_failed`: read the schedule
again and resend. That is the contract working, not a fault.

**Lists** default to `limit=50`, maximum 200, and answer `{items, page:{limit,
nextCursor, snapshot}}`. A cursor is opaque and authenticated with the
persistent key from `cursorKeyFile`; it binds the actor, route, namespace,
filters, a fifteen-minute expiry and the Kubernetes continue token. The MAC is
checked first and in constant time, so a tampered cursor is `400 cursor_invalid`
whatever else it claims, and a valid cursor replayed on another route or
namespace is `cursor_invalid` too. Expiry, and a Kubernetes `410`, are `410
cursor_expired` with an instruction to restart the list. Filters are exact name
and label selectors; there is no substring search, because implementing one
would mean collecting an unbounded list.

**Nothing credential-shaped is in a response.** No Secret data, no
service-account token, no kubeconfig, no raw pod log and no unredacted upstream
error. A connection projection carries the credential Secret's *name* and
nothing from inside it.

## The static UI

`/ui/` serves an allowlist built once at startup: the top-level `*.html`,
`*.js` and `*.css` files of the configured directory and the same three
extensions directly inside `pages/` — the selection `Dockerfile.ui` makes, and
nothing else. `README.md`, the `tests/` directory and its throwaway keypair, any
other subdirectory, any other extension, dotfiles and symbolic links are never
in the map. A request path is looked up **exactly**, with no decoding,
normalisation or directory index, so `/ui/../Cargo.toml` is a `404` rather than
a traversal to defeat. The bytes a browser receives are the bytes the process
read at startup; no request performs file-system I/O.

## Shared mode

### Configuration

```yaml
# console.yaml
mode: shared
listen: "0.0.0.0:8484"                       # any address; TLS terminates in front
publicBaseUrl: "https://console.example.com" # EXACT, HTTPS, no path, no trailing slash
uiDirectory: /srv/ui
oidc:
  issuer: https://idp.example.com/realms/logweir   # EXACT, as the discovery document states it
  clientId: logweir-console                        # EXACT; also the expected audience
  clientSecretFile: /var/run/secrets/oidc/clientSecret
  allowedAlgorithms: [RS256, ES256]                # the two this service verifies
  scopes: [openid, profile, groups]                # must contain `openid`
  groupsClaim: groups                              # the EXACT claim name
  displayNameClaim: name                           # presentation only
  tokenAuthMethod: clientSecretBasic               # or clientSecretPost
roles:
  revision: "2026-09-16.1"                         # recorded in every audit line
  bindings:
    - role: viewer                                 # viewer|operator|approver|administrator
      namespace: team-a                            # must appear in `namespaces` below
      groups: ["logweir-team-a-viewers"]           # EXACT strings; no wildcard, no regex
    - role: operator
      namespace: team-a
      groups: ["logweir-team-a-operators"]
    - role: approver
      namespace: team-a
      subjects: ["https://idp.example.com/realms/logweir#3f0c…"]
sessionKey:
  file: /var/run/secrets/session/key               # versioned key file
  expectedVersion: 1
cursorKey:
  file: /var/run/secrets/cursor/key
  expectedVersion: 1
sessionMaxAgeSeconds: 900                          # 60…900
trustedProxyCidrs: ["10.0.0.0/8"]                  # transport LOGGING only
namespaces: [team-a, team-b]
kubernetes:
  source: inCluster
```

A key file is two lines:

```console
$ printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > session.key.yaml
```

### What startup refuses, before it binds a socket

Every one of these is exit 2 with the field named. A console that comes up on
plain HTTP, or with a key someone rewrote underneath it, is worse than one that
does not come up at all, because the first two look like they are working.

| refusal | why |
|---|---|
| `publicBaseUrl` is not `https://` | TLS at the shared entry point is required, not recommended |
| `publicBaseUrl` has a path, a trailing slash, userinfo or no host | the redirect URI is this value plus `/auth/callback`; a mismatch is a login that cannot complete |
| `publicOrigin` is present | shared mode derives the origin from `publicBaseUrl`, so the origin checked and the redirect URI registered cannot disagree |
| a key file is missing, malformed, or under 32 bytes | there is no default key and no generated one |
| a key file's `version` is not `expectedVersion` | an **unexpected rotation**. Bumping both is a deliberate act that ends every live session; bumping neither means the file changed behind the service's back |
| the client-secret file is missing or empty | |
| an `allowedAlgorithms` entry is not `RS256` or `ES256` | `none` cannot be on the list, so an `alg: none` token has no matching entry |
| `oidc.issuer` ends in `/`, or carries a query, fragment or userinfo | the issuer is compared for exact equality with the token's `iss` |
| a plain-HTTP issuer without `oidc.insecureLoopbackIssuer` | and that flag is accepted only for a loopback host, for a local mock provider in development |
| a role binding names a role that is not one of the four | |
| a role binding names a namespace outside `namespaces` | |
| a role binding has neither `groups` nor `subjects` | |
| a binding string contains `*` or `?` | bindings are EXACT: a `*` would match nothing, so it is refused by name rather than silently granting nothing |
| `roles.revision` is empty | it is the provenance of every decision in the audit log |
| `sessionMaxAgeSeconds` outside 60…900 | a stateless session cannot be revoked before it expires |

### Sign-in

`GET /auth/login` starts an Authorization Code flow with PKCE S256, `state` and
`nonce`, and answers `303` to the provider's authorization endpoint. The three
per-login secrets travel in a sealed, `HttpOnly`, ten-minute
`__Host-logweir_login` cookie rather than in process memory, so a login begun on
one replica finishes on another.

`GET /auth/callback` compares `state` in constant time, exchanges the code with
the verifier, and validates the ID token: allowed `alg`, JWKS key by exact
`kid`, signature, exact `iss`, exact audience (`azp` required when there is more
than one), `exp`, `iat` (bounded skew, bounded age) and this login's `nonce`.
**The browser never receives a provider token**: the token response is
deserialised into a struct with one field, `id_token`, so no access or refresh
token exists in the process to leak.

JWKS are cached. An unknown `kid` provokes at most one refetch per minute —
that is what makes a provider's key rotation work without a restart, and what
stops an attacker-chosen `kid` from becoming a request amplifier. While the
provider is unreachable the cached keys keep working for a day, then validation
fails closed.

### The session and the CSRF token

| | |
|---|---|
| cookie | `__Host-logweir_session`, `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, **no `Domain`** |
| contents | session id, issuer, subject, display claim, group claims, issued/expiry/auth times, key version — **never** a provider token |
| protection | ChaCha20-Poly1305, with the cookie's own name and the key version as associated data |
| lifetime | at most 15 minutes; there is no refresh token and no server-side session table |
| CSRF token | `HMAC-SHA-256(session key, session id)`, returned by `GET /api/v1/session`, required in `X-CSRF-Token` on every unsafe method |
| logout | `POST /api/v1/session/logout` — an unsafe method, so it needs the exact `Origin`, `application/json` and the token like any other mutation |

Because the session is stateless, a restart or a second replica does not log
anyone out and nothing has to be replicated. What that costs is that revocation
before expiry is bounded by the expiry: removing an identity at the provider
takes effect within fifteen minutes. **Roles are not in the cookie** — they are
re-derived per request from the claims plus the current binding table — so
removing a role binding takes effect on the *next request*.

### What can never be an identity

`X-Remote-User`, `X-Remote-Group(s)`, `X-Remote-Extra-*`, `X-Forwarded-User`,
`X-Forwarded-Email`, `X-Forwarded-Groups`, `X-Forwarded-Preferred-Username` and
`X-Auth-Request-*` are **stripped from the request before routing** and recorded
in the audit line by name only. Stripping rather than ignoring is deliberate:
ignoring is a property of every reader and a future route can lose it, removing
is a property of the request. `Impersonate-*` is not ignored — it is refused
outright with `400 header_not_allowed` naming the header.

No callback URL and no authorization decision derives from `Host`, `Forwarded`
or `X-Forwarded-*`. A forwarded client address reaches exactly one audit field,
`forwardedFor`, and only when the immediate socket peer falls inside a
`trustedProxyCidrs` range.

There is no CORS layer: no response carries `Access-Control-Allow-Origin` or
`Access-Control-Allow-Credentials`.

### Roles

Actor identity is exactly `(issuer, sub)`. Group claims and subjects map by
**exact string** to bindings; multiple bindings union.

| action | viewer | operator | approver | administrator |
|---|:--:|:--:|:--:|:--:|
| read connections / schedules | ✓ | ✓ | | ✓ |
| create connection, test, credentials, discovery, preflight, destinations | | ✓ | | ✓ |
| create schedule, set suspension | | ✓ | | ✓ |
| read backups / restores / approvals / operations | ✓ | ✓ | ✓ | ✓ |
| create manual backup, create restore | | ✓ | | ✓ |
| read the approval packet | | ✓ | ✓ | ✓ |
| submit a governed approval | | | ✓ | |

**Administrator is deliberately absent from the last row.** An administrator who
must approve is bound as an Approver as well, and the separation-of-duties check
then still compares `(issuer, sub)` — not display names, not email claims, not
key ids. Administrator is not a self-approval bypass.

The governed-approval **route** is PLAT-19.2 and does not exist yet;
`capabilities.approvalSubmit` is `false` and no path serves it. The
**decision** exists and is tested now.

### Enumeration resistance

In shared mode an ungranted namespace answers exactly what a nonexistent object
answers — `404 not_found`, byte for byte apart from the request id — and the
namespace is checked **before** any Kubernetes call, so the answer never depends
on cluster state the actor may not see. In localAdmin mode the more informative
`403 namespace_forbidden` is kept: there is one actor, it is the administrator,
and there is nothing to enumerate.

The audit record carries the real reason either way. Hiding a namespace from a
caller must not also hide an authorization problem from the operator reading the
log.

### The audit record

One JSON object per request on the `logweir_api::audit` tracing target, emitted
by the middleware so no handler can forget one:

`auditId` (= `X-Request-ID`), `method`, `path`, `authenticationMode`,
`actorId`, `displayClaim` (kept separate because it is never an authorization
input), `sessionIdHash`, `bindingRevision`, `roles`, `namespace`, `action`,
`resource`, `decision`, `policyDigest`, `idempotencyKeyHash`, `requestHash`,
`planHash`, `objectName`, `objectUid`, `objectResourceVersion`, `httpStatus`,
`latencyMs`, `failureCode`, `peer`, `forwardedFor`, `ignoredIdentityHeaders`.

A record that reaches no decision point defaults to `deny`. Never logged:
cookies, bearer/authorization-code/refresh tokens, CSRF tokens, the raw
`Idempotency-Key`, Secret values, kubeconfig, approval or sidecar bytes, plan
bytes, raw pod logs, and object-store URLs carrying userinfo.

This is **attribution, not proof.** Annotations on a created object and lines on
stdout are correlation; the tamper-evident requester/approver record is the DSSE
document PLAT-19 owns, and Kubernetes audit supplies the complementary fact that
the `logweir-api` ServiceAccount made the API call. Retaining these lines is a
deployment responsibility: stdout alone is not durable evidence.

Dependency log targets (`kube_client`, `kube`, `hyper`, `hyper_util`, `rustls`,
`tower`, `h2`) are pinned at `warn` **regardless of `RUST_LOG`**, because
`kube_client` logs the upstream error body verbatim at `debug` — the exact text
this service redacts before logging it.

### Rate limits

`/auth/login` and `/auth/callback` are the only routes an unauthenticated caller
can reach that do work, so they carry a per-peer limit of 20 requests a minute
(`429` with `Retry-After`). The key is the **immediate socket peer**, never a
forwarded header: behind one ingress that makes it a global limit, which is the
correct conservative behaviour for a service whose per-user limits live behind
authentication.

There is no event stream yet: `capabilities.operationEvents` is `false` and no
path serves one, authenticated or not. The per-actor, per-namespace connection
slots it will need are implemented and tested.

## Local administrator mode is not a shared console

This mode uses the selected kubeconfig identity and may be cluster-admin. It is
an explicit administrator mode, not SSO and not per-user authorization: the
namespace grants come from the configuration file, and every actor of this
process is the same actor. It must not bind a routable address, must not get an
Ingress, and adding a login in front of it would not create per-user
authorization. Shared operation uses `mode: shared`, which is a different
listener, a different authenticator and a different authorizer — never this one
with a login bolted in front. Ordinary confirmation is unavailable through this
mode; it keeps the legacy governed approval behaviour.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
