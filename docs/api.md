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

This is the first stage. **Only `mode: localAdmin` exists**: a loopback-only
listener, the configured administrator as the actor, and namespaces from
configuration alone. There is no OIDC, no session cookie, no CSRF token and no
role matrix yet — those are the identity stage, and they plug into the
`Authenticator` and `Authorizer` seams without changing a route.

`logweir-api` is **not packaged or deployed**. No image builds it, the Helm
chart has no `console` template or value, and `publish = false` keeps it out of
the release archives. It runs from a local build against a kubeconfig context.
Nothing about an existing installation changes when this crate is present, so
there is nothing to upgrade, migrate or roll back: removing the crate removes
the feature. The chart, image, ingress and NetworkPolicy work is a later stage.

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
  kubeconfig: ~/.kube/config
  context: docker-desktop              # required; `current-context` is never used
cursorKeyFile: ./cursor.key            # at least 32 bytes
```

```console
$ head -c 32 /dev/urandom > cursor.key && chmod 600 cursor.key
$ cargo run -p logweir-api -- --config config.yaml
```

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

## Local administrator mode is not a shared console

This mode uses the selected kubeconfig identity and may be cluster-admin. It is
an explicit administrator mode, not SSO and not per-user authorization: the
namespace grants come from the configuration file, and every actor of this
process is the same actor. It must not bind a routable address, must not get an
Ingress, and adding a login in front of it would not create per-user
authorization. Shared operation requires the identity stage — OIDC, validated
sessions, the role matrix, audit attribution and a TLS ingress — and until then
the supported shared posture is no shared console at all.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
