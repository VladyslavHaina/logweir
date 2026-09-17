# PLAT-17.1 / PLAT-17.2 contract decision, including the PLAT-19.2 approval seam

Date: 2026-09-14  
Repository examined: `/Users/admin/Desktop/Repos/logweir`  
Reviewed base revision: `92e02097540c39ff8565283a38ee592499b95020`  
Worktree state: intentionally dirty with another release worker's candidate; this report made no repository, cluster, deployment, tracker, staging, or commit changes.

## DECISION

Build one new, bounded Rust shared-console service named `logweir-api`. It serves the existing static UI and `/api/v1`, authenticates users directly with OIDC, applies application-managed role and namespace authorization, and uses a narrowly bound Kubernetes ServiceAccount to create/read the existing Logweir resources. It is not a Kubernetes reverse proxy, does not expose arbitrary group/version/resource paths, does not impersonate users, does not dial Kafka or object storage, and does not execute backup/restore logic. `weirkeeper` remains the only execution controller and isolated runner Jobs remain the data-plane boundary.

Kubernetes remains the durable store. Durable create idempotency is represented by deterministic object names plus immutable specs and request hashes, not an API database. Status comes from the existing CRs and controller. Topic discovery and readiness are added later as typed, Kubernetes-backed check resources whose controller creates isolated, time-bounded Jobs; the HTTP service does not absorb that work. Static files remain plain HTML/CSS/ES modules copied byte-for-byte into an image; no frontend framework or bundler is introduced.

Choose API-managed authorization. Do **not** grant the console ServiceAccount Kubernetes `impersonate`, and reject inbound `Impersonate-*` headers. Kubernetes impersonation is cluster-scoped for users/groups and would couple the product policy to arbitrary cluster RBAC; Kubernetes documents that an impersonator acts as the target identity and that user/group impersonation requires a ClusterRole and ClusterRoleBinding. The shared service instead maps verified OIDC issuer/subject and exact group claims to Logweir roles and namespaces from administrator-owned configuration. Kubernetes audit will correctly show the console ServiceAccount; Logweir's audit record and correlation ID carry the end-user attribution. See [Kubernetes user impersonation](https://kubernetes.io/docs/reference/access-authn-authz/user-impersonation/) and [Kubernetes auditing](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/).

For PLAT-19.2, keep the controller—not the API—as the final authorization gate. Both ordinary confirmation and governed approval produce a DSSE authorization document bound to the exact Restore kind, namespace, name, UID, plan hash, policy identity/version, requester identity, issue time, and expiry. The console signature attests the verified requester in both modes. Governed mode additionally requires an independent human approver signature whose policy principal differs from the requester. The existing immutable `Approval` resource remains the transport and PLAT-01's per-Restore immutable bundle remains the runner input. A direct CR writer cannot mint the console signature or a governed approver signature, so a direct write may persist as refused/pending but cannot create a bundle or Job.

Missing policy means today's governed behavior. Ordinary mode cannot be enabled merely by upgrading: it requires an explicit installation-level `allowOrdinaryConfirmation=true`, an explicit namespace policy binding, and a confirmation issuer key in the future PLAT-19.1 trust model. That key is never placed in the legacy `TrustRoster/default.spec.approverKeys`. Consequently an old controller reached by rollback cannot mistake a console-only ordinary confirmation for a legacy governed approval; it fails closed. Existing v1 approval documents and signed archives continue to verify.

## Evidence from the current source

- The tracker explicitly preserves Rust execution, Kubernetes reconciliation and durable state, isolated runner Jobs, static asset packaging, and archive compatibility, and describes PLAT-17 as a small product API rather than an unrestricted proxy: `docs/to-do/platform-improvements.md`.
- The browser currently calls Kubernetes directly. `ui/api.js` has the only network call, builds `/apis/logweir.dev/v1alpha1/...`, allows five resource plurals for create, and permits only the schedule suspension patch. It intentionally has no credential or storage.
- The in-cluster UI is currently a `kubectl proxy` in `charts/logweir/templates/ui/ui.yaml`. Anyone reaching its ClusterIP Service acts as the `<release>-ui` ServiceAccount. The path regex limits paths but all callers share that identity. `charts/logweir/values.yaml`, `charts/logweir/README.md`, `ui/README.md`, and `docs/kubernetes.md` state the same boundary.
- Static bytes are already a first-class release artifact. `Dockerfile.ui` copies the sixteen source assets from `ui/`; `scripts/check-image-ui.sh`, `scripts/check-ui-offline.sh`, `crates/logweir/tests/chart_lint.rs`, and `crates/logweir/tests/ui_lint.rs` verify content, offline operation, and the single request site. Preserve those properties.
- The reusable Kubernetes domain model is in `crates/weirkeeper/src/crds/`. `LocalRef` deliberately forbids cross-namespace references. `KafkaCluster` separates public connection settings from same-namespace Secret references. `Backup`, `BackupSchedule`, `Restore`, `Approval`, and `TrustRoster` have structural schemas and immutable specs (only `BackupSchedule.spec.suspend` is mutable).
- `Restore.spec.planBytes` is intentionally opaque and byte-preserving in `crates/weirkeeper/src/crds/restore.rs`; the controller copies it verbatim and hashes it. `ui/plan.js` is the present canonical emitter and `ui/tests/fixtures/plan.golden.yaml` is parsed by Rust. The API migration must keep old bytes valid and must not parse/re-emit a submitted approved document.
- Current approval verification in `crates/weirkeeper/src/controllers/approval.rs` checks DSSE payload type, every roster key, key ID/material agreement, signature, key expiry, recomputed plan hash, and signed subject kind. `Approval.status.verifiedSubjectRef` sticks the successful verification to the referent UID. `crates/weirkeeper/src/controllers/restore.rs` rechecks the approval and plan before any Job or ConfigMap POST.
- The immutable controller-to-runner bundle handshake is named in `crates/logweir-core/src/execution_contract.rs`; current bundle construction and substitution checks live in `crates/weirkeeper/src/controllers/restore.rs`. Extend this versioned contract rather than create a second runner path.
- `crates/weirkeeper/src/job.rs` and the backup/restore controllers retain exit codes, evidence keys, owner references, Job deadlines, and isolated runner ServiceAccounts. Runner pods do not mount Kubernetes tokens. The API must create CRs only, never Jobs.
- Kafka discovery can reuse `logweir_kafka::reader::ClusterReader::list_topics` and `TopicMeta` in `crates/logweir-kafka/src/reader.rs`, but the broker-dialling code must remain in the isolated runner/check image. `KafkaCluster` probing already follows this pattern in `crates/weirkeeper/src/controllers/kafka_cluster.rs` and `crates/logweir/src/probe.rs`.
- Current human roles are unbound in `config/rbac/{viewer_role,operator_role,approver_role}.yaml` and `charts/logweir/templates/human-roles.yaml`. They are Kubernetes/local-admin roles, not shared-console application roles. The controller's broad binding and Job-create signing-oracle residual are documented in `config/rbac/role.yaml` and `docs/kubernetes.md`; this matters when isolating the console session and confirmation keys.
- `charts/logweir/templates/networkpolicy.yaml` already warns that Docker Desktop may accept NetworkPolicy without enforcing it. Kubernetes likewise states that NetworkPolicy has no effect without an enforcing network plugin and that Service/source-IP rewriting is implementation-dependent: [Kubernetes NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/).

## Placement and ownership boundaries

Add a workspace crate, not a controller subcommand:

```
crates/logweir-api/
  src/main.rs             process startup, config, graceful shutdown
  src/lib.rs              testable router construction
  src/contract.rs         request/response/problem types; schema generation
  src/auth/oidc.rs        authorization-code flow and ID-token validation
  src/auth/session.rs     authenticated stateless session cookie
  src/auth/csrf.rs        Origin and synchronizer-token enforcement
  src/authz.rs            role/namespace/action decision, pure and table-tested
  src/kube.rs             only Kubernetes adapter
  src/idempotency.rs      key validation, deterministic names, replay comparison
  src/status.rs           CR status to product Operation mapping
  src/audit.rs            structured attribution and redaction
  src/routes/             one module per bounded product resource
```

`logweir-api` may depend on `weirkeeper` for CRD Rust types and on `logweir-core` for hashes/spec/domain values. Do not move those libraries during PLAT-17. A later cleanup may extract CRDs into a neutral crate only after both binaries are stable; moving them now would enlarge the release diff for no contract benefit. Reuse `logweir_core::ids::sha256_prefixed`, the CRD structs in `weirkeeper::crds`, current status reason/evidence types, and the existing plan golden. The HTTP contract types are separate DTOs with `serde(deny_unknown_fields)` on mutation input, because exposing CRDs directly would leak Kubernetes metadata and freeze infrastructure details as product API.

Use a small Rust HTTP stack (`axum` over the already-resolved Tokio/Hyper/Tower family is the recommended implementation) in this new crate. This is a server implementation dependency, not a UI or execution framework rewrite. Generate JSON Schema/OpenAPI from the Rust DTOs and drift-test it. Keep the UI as shipped JavaScript; use JSDoc types plus `// @ts-check` and a no-emit typecheck if PLAT-18.1 wants compile-time checking without adding a browser build.

Build `Dockerfile.console` with the `logweir-api` binary and the exact existing `ui/` assets at `/ui`. Serve `/ui/` and `/api/v1/` from that binary on one origin. Retain `Dockerfile.ui` and the manual `kubectl proxy --www=./ui` path for explicit localhost administrator mode during the compatibility window.

## HTTP conventions and bounded contracts

All product routes are under `/api/v1`. There is no generic `{group}/{version}/{resource}` route, no request-supplied Kubernetes path, no arbitrary patch, no delete for Backup/Restore, and no pod/log/Secret endpoint. JSON mutation bodies are limited to 1 MiB; approval packet limits are separately explicit and no larger than 2 MiB. CRUD Kubernetes calls have a 10-second server deadline. Responses include `requestId`; no response includes Secret data, service-account tokens, OIDC tokens, kubeconfig data, raw pod logs, or unredacted upstream errors.

Durable POSTs require `Idempotency-Key`, 8–128 visible ASCII characters. Its scope is `(OIDC issuer, subject, namespace, route, key)`. The Kubernetes name is a DNS-safe prefix plus a sufficiently long base32/hex SHA-256 truncation of that scope; the full key is never stored or logged, only its SHA-256. The canonical validated request hash is recorded on the object. First creation returns 201; replay by the same still-authorized actor with the same request hash returns 200 and the same resource UID; same key/different hash returns 409 `idempotency_conflict`. A lost response or API restart therefore cannot create a second CR. A later deliberate operation uses a new key and gets a new immutable object. API process memory is never the source of truth.

Use `application/problem+json` for every error:

```json
{
  "type": "https://logweir.dev/problems/validation-failed",
  "title": "Request validation failed",
  "status": 422,
  "code": "validation_failed",
  "detail": "One or more fields are invalid.",
  "requestId": "01...",
  "retryable": false,
  "errors": [{"field": "topicSelection.topics[0]", "code": "topic_name_invalid", "message": "..."}]
}
```

Stable codes include `unauthenticated` (401), `session_expired` (401), `forbidden` and `namespace_forbidden` (403), `not_found` (404), `idempotency_conflict`, `state_conflict`, `approval_required`, and `policy_mismatch` (409), `precondition_failed` (412), `validation_failed` (422), `rate_limited` (429 plus `Retry-After`), `kubernetes_unavailable` (503), and `upstream_timeout` (504). Kubernetes `Status.message` is logged after redaction but is not returned verbatim.

Lists use `limit` default 50, maximum 200. The response is `{items, page:{limit,nextCursor,snapshot}}`. A cursor is opaque and authenticated with a persistent cursor key; it binds actor, route, namespace, filters, expiry, and the Kubernetes continue token or discovery result generation. It expires after 15 minutes. Tamper is 400 `cursor_invalid`; expiry or Kubernetes 410 is 410 `cursor_expired` with an instruction to restart the list. Native CR lists initially support exact name and label selectors, not pretend substring search. Topic-result pages are sorted and indexed by the discovery producer and may support `q`. Never collect an unbounded Kubernetes list merely to implement UI search.

### Initial routes that can ship against current resources

- `GET /api/v1/session` → stable actor ID (`issuer + sub`), display claims, session expiry, a CSRF token, explicit namespace grants, and capability flags. Authorization never uses email/display name.
- `GET /api/v1/namespaces` → only configured grants; it never lists core `Namespace` objects.
- `GET /api/v1/namespaces/{ns}/connections` and `GET .../connections/{name}` → `KafkaCluster` product projections with auth mode, username, TLS, Secret **reference name**, role/capability, reachability reason, cluster ID, and observed time. No Secret read.
- `POST /api/v1/namespaces/{ns}/connections` → operator-only typed create of the current immutable `KafkaCluster`. Initial delivery accepts an existing credential reference only. Write-only credential creation remains PLAT-07.1 work and must not be smuggled in with broad Secret permissions.
- `GET /api/v1/namespaces/{ns}/schedules`, `GET .../schedules/{name}`, `POST .../schedules`, and `POST .../schedules/{name}:set-suspension` → current schedule contract. The command route accepts only `{suspended:boolean, expectedResourceVersion}` and maps to the one permitted spec mutation.
- `GET /api/v1/namespaces/{ns}/backups`, `GET .../backups/{name}`, `GET .../restores`, `GET .../restores/{name}`, and the corresponding normalized operation/status routes.
- `GET /api/v1/namespaces/{ns}/approvals` and `GET .../approvals/{name}` → public approval metadata/status; raw approval documents are returned only through the explicit approval-packet route to authorized operator/approver roles, not in list rows.
- `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}` and `GET .../events` where `kind` is the closed set `backup|restore|discovery|preflight`. No arbitrary plural.

### Incremental routes and their owning platform tasks

- PLAT-06.1/06.2 owns `POST .../backups`: `{sourceRef, topicSelection, destinationRef|legacyArchive, scheduleRef?, deadlineSeconds}`. The API sets `triggeredBy=manual`, a server run identity, and copies the immutable schedule revision when requested. It does not emit internal runner-argv annotations.
- PLAT-07.1 owns write-only credential input and a narrow credential-create admission rule; until that lands only existing Secret refs are supported.
- PLAT-08.1/08.2 owns `destinations` DTOs/routes and the new domain resource. Minimum DTO: name, storage URL, endpoint, region, path-style boolean, `allowHttp` boolean, archive credential ref, and separate evidence-access ref. Addressing never implies transport downgrade. The API does not reinterpret old inline `ArchiveRef` while an operation is in flight.
- PLAT-09.1 owns `POST .../connections/{name}/topic-discoveries`, `GET .../topic-discoveries/{id}`, `GET .../{id}/topics`, and `POST .../{id}:cancel`. Response items are `{name,partitions,internal,errorCode?}` plus `observedAt` and `visibility:{state,basis}`. States are `attestedComplete|limited|unknown`; a successful Kafka list alone is `unknown`, observed authorization omissions are `limited`, and `attestedComplete` requires an explicit administrator-governed capability. Internal topics are excluded by default. Results are bounded, paged, and stored outside CR status (owned immutable ConfigMap chunks or another task-approved Kubernetes object), with only a summary/result reference in status.
- PLAT-03 owns the equivalent asynchronous `preflights` routes. Inputs bind operation kind, saved refs, exact plan hash, and requested checks. Results carry `ready|notReady|unknown`, per-check code/remedy, observed time, scope, and expiry. They never replace execution-time guards.
- PLAT-14.1 owns the final normalized state mapping and reconnect behavior. Product states are `pending|queued|preparing|running|verifying|succeeded|failed|refused|cancelled|unknown`; operation result and evidence verification remain separate fields. For example, Restore `outcome=pass` with verification `NotAttempted` is not “verified success.” Existing reason, message, exit code, last phase, timestamps, and evidence references remain visible in bounded form.
- PLAT-18.1 owns the JSDoc/no-emit typed client, runtime response decoders, explicit mutation state machine, and shared fixtures. Plan rendering moves to a Rust endpoint only after byte-for-byte fixtures prove the API emitter matches the runner grammar. During migration, `ui/plan.js` continues producing the exact bytes; submission sends those bytes and hash, and the API validates without reserializing.

Routes not yet implemented are absent and advertised as `false` in `/session.capabilities`; they must not return a fake successful stub.

### Read, stream, and cancellation semantics

Aborting a list/get or leaving a UI route cancels only that HTTP/Kubernetes read. It never retracts an accepted mutation. Backup and Restore have no v1 cancel/delete endpoint because external side effects and cleanup semantics are not yet defined.

Discovery and preflight are the only cancellable server operations. Their future CRD permits one transition `spec.cancelRequested: false -> true`; the controller verifies the exact owned check Job and UID before stopping it. Repeated cancel is 200 with the same state. Cancel after terminal is 200 `alreadyTerminal`. Cancellation never deletes archive data, Kafka topics, durable Backup/Restore CRs, or signed evidence. Check Jobs have an active deadline and TTL so API death cannot leak them forever.

The events route is authenticated server-sent events, not an authorization shortcut. Event IDs are Kubernetes `resourceVersion`. `Last-Event-ID` resumes a watch; a compacted/expired version emits one `reset` event containing the current authorized snapshot and new resourceVersion. Connections last at most five minutes, send non-sensitive heartbeats, have per-actor/namespace limits, use `Cache-Control: no-store`, and close promptly on browser disconnect. Polling the ordinary GET remains supported. No token appears in a URL.

## Identity, session, proxy, and browser trust boundary

Shared mode uses OIDC Authorization Code flow with PKCE S256, state, nonce, exact issuer, exact client ID/audience, exact configured redirect URI, signature/JWKS validation, expiration, and allowed-algorithm checks. OIDC documents that the code flow keeps tokens out of the user agent and requires ID-token validation; OAuth security BCP recommends PKCE for confidential clients as well: [OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html), [RFC 9700](https://datatracker.ietf.org/doc/html/rfc9700), and [RFC 7636](https://datatracker.ietf.org/doc/html/rfc7636).

The browser receives no ID/access/refresh token. After validation, the API issues a short-lived authenticated/encrypted stateless session cookie containing session ID, issuer, subject, allowed identity claims, issued/expiry/auth times, and key version—never provider tokens. Maximum session age is 15 minutes initially; no refresh token is retained. Group-to-role mapping is reevaluated from claims and current authorization config on every request, but IdP membership removal is bounded by session expiry. OIDC login fails closed if issuer/JWKS/token validation fails; an already valid console session continues only to its signed expiry. Session and cursor keys are persistent, versioned Kubernetes Secrets created/adopted explicitly; startup refuses missing, malformed, or unexpectedly rotated keys. Use projected, time-bound ServiceAccount tokens for Kubernetes access; Kubernetes recommends TokenRequest-backed projected tokens over long-lived token Secrets: [Managing ServiceAccounts](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/).

Cookie: `__Host-logweir_session`, `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, no `Domain`. The API returns a synchronizer CSRF token from `/session`; every unsafe method requires it in `X-CSRF-Token`, requires JSON content type, and verifies the `Origin` equals the single configured public origin. Do not enable credentialed CORS. RFC 6265 defines Secure/HttpOnly behavior, and OWASP recommends a synchronizer token in a custom header for AJAX APIs: [RFC 6265](https://datatracker.ietf.org/doc/html/rfc6265), [OWASP CSRF prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html).

The application does not support identity from `X-Remote-User`, `X-Forwarded-User`, `X-Auth-Request-*`, or similar headers in the first shared release. It ignores and strips them from downstream logs. It also does not derive callback URLs or authorization decisions from `Host`, `Forwarded`, or `X-Forwarded-*`; `publicBaseUrl` is an exact administrator value. Only transport logging may use forwarded client IP/scheme, and only when the immediate peer falls within explicit trusted proxy CIDRs. Forged identity headers from a client therefore have no effect.

TLS is mandatory at the shared ingress. Startup rejects a non-HTTPS `publicBaseUrl` in shared mode. TLS may terminate at the supported ingress controller, with ClusterIP HTTP inside the isolated network boundary; NetworkPolicy is defense in depth, not TLS. Set HSTS at ingress and CSP (`default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'`), `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and a restrictive Permissions Policy on static/API responses. The API never serves directory listings or test fixtures.

Localhost administrator mode remains explicit and separate: `kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1`. It uses the selected kubeconfig identity and may be cluster-admin; it is not SSO, not a shared service, and not evidence of per-user API authorization. It must not bind `0.0.0.0`, get an Ingress, or be described as shared-console mode. Ordinary confirmation is unavailable through this legacy direct-CR UI; it retains governed/legacy approval behavior.

## Application role and namespace matrix

Actor identity is exactly `(OIDC issuer, sub)`. Group claims map by exact string to bindings in administrator-owned config. No regex, email-domain inference, default namespace, or wildcard is implicit. Multiple bindings union permissions, but separation-of-duties rules still compare principals and cannot be overridden by also holding `admin`.

| Product action/resource | Viewer | Operator | Approver | Administrator |
|---|---:|---:|---:|---:|
| Session and explicitly granted namespaces | read | read | read | read |
| Connections/destinations summaries and health | read in bound namespaces | read/create/test in bound namespaces | read only when separately granted viewer | all bound namespaces |
| Topic discovery and readiness | read | start/read/cancel own checks | read only when needed for the approval packet | all bound namespaces, including cancelling any check in an administered namespace (amended 2026-09-17 at D2 W12's integration: an operator's cancel is exact-owner; an administrator's is namespace-wide, because a cancel is transient, idempotent and leaves the object and its evidence in place) |
| Schedules | read | create, set suspension, and edit future policy (amended 2026-09-17 at D1 W6's integration: editing a schedule's future policy — PLAT-05.1's `PUT …/schedules/{name}` under `expectedGeneration` — carries the same authority as creating one; every run keeps its own frozen snapshot, so an edit never reaches a running Backup) | read only with viewer | all bound namespaces |
| Backups/Restores/status/evidence metadata | read | create manual backup/restore and read | read exact governed approval subject/plan | all bound namespaces |
| Ordinary confirmation | none | confirm own operation where bound policy is Ordinary | none | may operate, but is still the requester |
| Governed approval submission | none | none | submit only in approver-bound namespaces | permitted only if separately bound; cannot approve own request when policy requires independence |
| Namespace approval policy/trust binding | none | none | read effective policy | administer explicitly; installation floor still applies |
| Installation trust, issuer/session config, role bindings | none | none | none | installation-admin only, cluster scope |

Viewer never mutates. Operator never submits a governed approval or changes trust/policy. Approver cannot create an execution. Admin is not a magic self-approval bypass. Namespace is checked before resource lookup so unauthorized and nonexistent resources both return the same 404/403 policy chosen for enumeration resistance; audit retains the real reason.

These are product roles. The console ServiceAccount receives only the union of Kubernetes verbs needed for enabled routes, bound with a RoleBinding in each configured namespace. It receives no Secret read, Job create, pod/log, exec, attach, delete, RBAC bind/escalate, TokenRequest-for-other-accounts, SubjectAccessReview, or impersonate permission. Cluster-scoped trust writes, when PLAT-19.1 implements them, use a separately reviewed admin path rather than silently widening the everyday ServiceAccount.

## Ordinary versus governed approval contract

PLAT-19.1 must introduce key usage separation before Ordinary can ship: `confirmationIssuerKeys` are distinct from human `governedApproverKeys` and runner `signingKeys`. A key has a stable `principalId`, algorithm/material, validity interval, lifecycle state, and usage. Current `TrustRoster` remains the legacy governed/evidence source until migrated. No private approval or confirmation key appears in a CR, response, UI asset, log, or runner bundle.

PLAT-19.2 adds an immutable namespaced `ApprovalPolicy` (or the exact PLAT-19.1 policy resource if that task adopts the fields directly):

- `mode: Governed | Ordinary`
- `trustPolicyRef`
- `maxAgeSeconds` with a bounded installation maximum
- `requireDistinctPrincipal` (must be true for Governed in the supported baseline)
- optional approved OIDC assurance/context requirements

The installation configuration binds each managed namespace to one exact policy name/UID and carries `allowOrdinaryConfirmation`, default false. API and controller consume the same content hash and expose it in readiness. Missing binding synthesizes `legacy-governed-v1`; it never synthesizes Ordinary. The CRD spec is immutable. Selecting a different policy is an explicit installation-admin rollout and audit event, not a namespace operator edit.

Authorization document v2 is canonical JSON and contains at least:

```
formatVersion, authorizationMode,
subject {apiVersion, kind, namespace, name, uid},
planHash,
requester {issuer, subject},
policy {apiVersion, kind, name, uid, specDigest, bindingDigest},
issuedAt, expiresAt,
ticket (required in Governed, optional in Ordinary)
```

The DSSE sidecar signs those exact bytes. The API's confirmation issuer signature is required in both modes and attests the requester it authenticated. Governed mode requires a second signature over the same bytes from a currently valid governed-approver key. Its `principalId` must differ from the requester's `(issuer,sub)` when `requireDistinctPrincipal` is true. Comparing display names, key IDs alone, or “approver key differs from evidence signing key” is not separation of duties.

Flow: operator submits Restore with an idempotency key; API creates the immutable Restore first, obtains its UID, builds and console-signs the exact approval packet. Ordinary mode creates the deterministic Approval immediately. Governed mode returns `awaitingApproval`; an approver fetches the packet, verifies the plan/policy/requester with an updated CLI, countersigns the same bytes, and submits it. Replays complete an interrupted create sequence by reading Restore/Approval; they do not create a second subject.

The Approval controller verifies payload type, console signature and key usage, optional human signature and key usage, exact subject including UID, recomputed plan hash, current policy binding/digests, issue/expiry, and distinct principal. It writes the matched identities and policy provenance to status. The Restore controller repeats the authorization verdict immediately before materializing the bundle and before Job creation. Bundle contract v2 includes document, sidecar, both required public keys, policy snapshot/digest, subject UID, and existing allowed-cluster material; the runner revalidates it before any data-plane work. Existing bundle v1 remains accepted only for already-created legacy governed Restores under the documented transition.

Direct create behavior is intentional: Kubernetes schema validates shape and immutability; cross-resource/policy/crypto checks occur in the controller. A direct malformed, expired, self-approved, console-only-under-Governed, human-only-v2, wrong-policy, wrong-UID, or changed-plan object may exist for audit but receives a refusal condition and causes zero ConfigMap/Job/data-plane POSTs. Do not make an admission webhook a first-release availability dependency. A later webhook may reject earlier, but it is defense in depth and cannot replace controller/runner checks.

Policy changes affect not-yet-admitted requests: binding/policy mismatch requires re-confirmation/re-approval. Once the controller has atomically selected and materialized an immutable v2 bundle and created the Job, that run continues under its recorded policy snapshot; changing policy does not mutate an in-flight plan. Revocation semantics for keys remain PLAT-19.1 work and must be explicit about already-running versus future execution.

## Audit attribution

Every request gets a cryptographically random request/audit ID, returned as `X-Request-ID` and in JSON. Ignore client-supplied IDs except as a separately named trace hint. Structured audit records include timestamp, audit ID, stable actor ID, display claim separately, session ID hash, evaluated role binding revision, namespace, action, product resource, allow/deny, policy identity/digest, idempotency-key hash, canonical request/plan hash, Kubernetes object name/UID/resourceVersion when known, HTTP result, latency, and sanitized failure code. Approval events add requester principal, approver principal/key ID, mode, expiry, and separation decision.

Never log cookies, bearer/code/refresh tokens, CSRF tokens, Secret values, kubeconfig, approval/sidecar bodies, plan bytes, raw pod logs, Kafka records, or object-store credentials/URLs containing userinfo. Apply the current redaction discipline before serializing errors.

Created CRs carry the audit ID, request hash, idempotency hash, and non-secret actor reference in reserved metadata annotations for correlation, but annotations are not the authorization proof. The v2 DSSE document and signed execution evidence are the tamper-evident requester/approver record. Kubernetes audit supplies the complementary fact that the `logweir-api` ServiceAccount made the API call; Kubernetes describes Metadata-level audit as recording requesting user, timestamp, resource, and verb. Retention/export of application audit logs is a deployment responsibility that must be documented and tested; stdout alone is not durable evidence unless the platform retains it.

## Helm, RBAC, ingress, and network changes

Add `console.*` values and templates rather than mutating `ui.enabled` in place:

- `console.enabled` default false; `console.image` must support digest pinning; replicas, resources, PDB, Service port, OIDC issuer/client ID, exact HTTPS public URL, Secret refs, role bindings, managed namespaces, policy bindings, and capability flags.
- `console.ingress.enabled`, `className`, exact host, TLS Secret, and explicit ingress-controller namespace/pod selectors. An Ingress object has no effect without a controller and controller behavior varies, as Kubernetes documents: [Kubernetes Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/).
- A ClusterIP Service exposes only the console port. No NodePort/LoadBalancer by default.
- Dedicated ServiceAccount with projected token; per-managed-namespace RoleBindings. No ClusterRoleBinding for namespaced console work.
- Ingress NetworkPolicy allows only the configured ingress-controller pods to the console port. Egress allows DNS, Kubernetes API endpoints on 443/6443, and configured OIDC/JWKS/token endpoint CIDRs on 443. It does not allow broker or object-store ports. Because FQDNs and pre/post-DNAT behavior are not portable NetworkPolicy concepts, dynamic IdP endpoints require documented CIDRs or an approved egress proxy and production-CNI validation.
- Put the console session/confirmation private keys in a control namespace outside `weirkeeper`'s Job-create authority. This requires scoping the current controller's namespaced permissions away from a cluster-wide ClusterRoleBinding and instantiating namespaced watches/RoleBindings for the configured execution namespaces, while retaining only necessary cluster-scoped TrustPolicy reads/status. Until that is done, the documented Job-create signing-oracle residual would extend to the console's keys and shared mode must not be declared secure.
- The API starts NotReady if OIDC discovery/JWKS cannot initialize, session/confirmation key material is missing or inconsistent, policy/API config digests disagree, or Kubernetes access is absent. `/healthz` reports process liveness only; `/readyz` returns no sensitive detail.

Static migration:

1. Keep `/ui/` and hash routing so deep-link behavior is unchanged. Replace only the request base from `/apis/...` to `/api/v1/...`; namespace choices come from `/session`, not the runtime ConfigMap.
2. Extend the existing asset/image gates to compare `/ui` in `logweir-console` against the source directory. Preserve “no test fixtures/PEM/external resources,” single request site, secure-context plan hashing, and relative assets.
3. New installs use `console.enabled` for shared access. `ui.enabled=true` continues rendering the current ClusterIP `kubectl proxy` unchanged for a compatibility release and is explicitly labeled `legacyLocalAdminProxy`; it gets no Ingress.
4. An upgrade never converts `ui.enabled` into shared mode or exposes it publicly. Administrators explicitly configure OIDC/TLS/roles/namespaces and enable console, verify it, then disable the legacy proxy. The manual localhost command remains.

## Implementation stages and delegable file ownership

No stage begins until the release owner unfreezes the source candidate.

1. **Contract and server skeleton — API worker.** Own `crates/logweir-api/**` and generated contract fixtures only. Implement strict DTOs, problem schema, router, health/readiness, static serving, list/get projections, status normalization, deterministic idempotency helpers, and mock Kubernetes tests. Do not edit controllers or UI.
2. **OIDC/session/authz — security worker.** Own `crates/logweir-api/src/auth/**`, `authz.rs`, `audit.rs`, and their tests. Implement code+PKCE/state/nonce, validated stateless sessions, CSRF/Origin, exact group mapping, rate/stream limits, redaction, and no-impersonation assertions. Independent security review is mandatory.
3. **Current-resource adapters — Kubernetes API worker.** Own `crates/logweir-api/src/kube.rs`, `routes/{connections,schedules,backups,restores,approvals,status}.rs`, and adapter tests. Implement only routes backed by current/landed domain behavior; preserve opaque plan bytes and resourceVersion preconditions. PLAT-06 gates manual Backup mutation.
4. **Approval policy/domain — controller/Rust worker.** Own the new policy CRD/type, `crates/weirkeeper/src/controllers/{approval,restore}.rs`, `crates/logweir-core/src/execution_contract.rs`, approval document types/CLI, CRD generation, and focused controller/runner tests. Introduce v2 dual-signature/key-usage checks, default legacy-Governed behavior, policy binding digest, expiry, exact UID, and bundle v2. This stage coordinates with PLAT-01 and PLAT-19.1 and cannot claim them complete.
5. **Controller authority isolation — Kubernetes controller worker.** Own `crates/weirkeeper/src/main.rs`, controller registration/watches, controller RBAC manifests/templates, and namespace-scope tests. Replace the namespaced cluster-wide binding with explicit execution namespaces so console keys are outside Job-create authority. Preserve cluster-scoped trust reads only.
6. **Static UI client — UI worker.** Own `ui/api.js`, `ui/lifecycle.js`, `ui/runtime.js`, page adapters, JSDoc types/decoders, and UI tests. Keep one network site, no browser token storage, no framework/bundler, exact plan bytes, navigation cancellation, and separate mutation acknowledgement. Do not change backend contracts.
7. **Image/Helm/network — deployment worker.** Own `Dockerfile.console`, chart `console` templates/values/schema, NetworkPolicies, ingress, image/asset gates, and rendered chart fixtures. Preserve `Dockerfile.ui`/legacy behavior. All Kubernetes commands in tests explicitly name `--context docker-desktop`.
8. **Cross-layer acceptance — E2E worker.** Own additions to existing test suites and one docker-desktop harness, not a new mandatory workflow. Exercise the real browser/API/controller/runner resource chain with a local mock OIDC provider and source-matched images. A code reviewer, Rust reviewer, and security reviewer independently close findings before integration.

Workers are not alone in the checkout: each must limit edits to its ownership, rebase/adjust around concurrent changes, and never revert another worker's candidate.

## Required tests and release failure criteria

Contract/unit:

- Schema/OpenAPI drift; unknown/missing/oversize fields; every error code/status; no secret fields; current CR fixture projections; plan/hash golden round-trip without reserialization.
- Idempotency create, response loss, same/different body, actor/namespace/route scope, API restart, concurrent replicas, Kubernetes 409/422, and deliberate new key.
- Pagination limit, exact filters, signed cursor tamper/cross-actor/cross-namespace/filter replay, expiry, Kubernetes 410 reset, large catalogs, and bounded memory.
- Status mapping for every current Backup/Restore phase, missing/unknown fields, evidence Valid/Invalid/NotAttempted, and disconnect/reconnect/resourceVersion reset.
- Read abort does not cancel mutations; durable operations survive browser/API/controller restart; transient cancellation is idempotent and exact-owner only.

Security/identity:

- OIDC wrong issuer/audience/algorithm/signature/nonce/state/PKCE/redirect, expired token/session, JWKS rotation/outage, missing group, changed role config, and session-key rotation/restart.
- Forged identity/forwarded/impersonation headers, Host poisoning, cross-origin form/JSON, missing/wrong CSRF token, credentialed CORS, cookie flags, unauthenticated SSE, stream enumeration, and rate/connection limits.
- Complete viewer/operator/approver/admin matrix in two namespaces, including role union and admin self-approval denial. API ServiceAccount `kubectl auth can-i` negatives for Secrets, Jobs, pods/log, exec/attach, delete, RBAC bind/escalate, token creation, SAR, and impersonation.
- Audit allow/deny/correlation/replay records; redaction mutants for token, password, Secret, approval bytes, plan bytes, raw Kubernetes error, and URL userinfo.

Approval/controller/runner:

- Legacy policy absent remains Governed; explicit Ordinary success; console signature absent/invalid/wrong usage; Governed human signature absent/invalid/wrong usage; same principal under two keys; self-approval; expiry/not-yet-valid; wrong namespace/name/UID/kind/plan/policy/binding digest; policy change before Job; direct CR submissions for every failure.
- Assert **zero** bundle/Job/data-plane calls on any failed authorization. Verify controller restart and duplicate reconcile preserve one exact bundle/Job. Tamper/missing bundle and old runner reject v2 before network/data work.
- Existing v1 Approval/Restore continues under legacy Governed. Old controller rejects ordinary console-only Approval. Existing signed scorecard/receipt/archive fixtures still verify byte-for-byte.

Static/browser/live docker-desktop:

- Console image serves exactly the source assets and no tests/keys; CSP/security headers; `/apis`, `/api/v1/raw`, core API, Secrets, pod logs, exec/attach, directory listing, and legacy proxy paths are unreachable.
- Real TLS ingress login/logout/session expiry, refresh of a running operation, double-click/lost response, cross-namespace delayed response after navigation, approval by a second browser identity, viewer denied mutation, operator denied approval, forged header denied, and SSE reconnect.
- Network allow from configured ingress and deny from an unrelated pod; API egress reaches only DNS/Kubernetes/IdP and cannot reach Kafka/object storage. Docker Desktop without enforcing CNI is explicitly **not** acceptable proof of deny behavior; structural checks may pass locally, but production-CNI enforcement remains required before a production shared-console claim.
- Localhost administrator mode still works only at loopback with an explicit docker-desktop kubeconfig/context and does not expose Ordinary.

Release fails if any of the following occurs: arbitrary Kubernetes paths are reachable; a viewer mutates; an operator submits a governed approval; an actor crosses an unbound namespace; an identity header affects auth; unsafe method succeeds without exact Origin+CSRF; two CRs result from one idempotency key; an API/browser disconnect cancels accepted durable work; policy absence or upgrade selects Ordinary; a direct invalid/self-approved CR creates a bundle or Job; old controller accepts console-only Ordinary; console/session keys are mountable by controller-created Jobs; credentials/approval bodies/tokens enter responses or logs; status labels unverified evidence as verified; asset bytes drift; or an old signed archive no longer verifies.

## Migration, rollback, and compatibility

Upgrade order: publish/pin compatible runner and controller images; install CRD additions first; deploy the new controller with legacy-Governed default and bundle v1+v2 read compatibility; scope controller namespaces and verify runner operation; deploy console disabled; configure persistent session and confirmation identity, OIDC, exact role/namespace bindings, TLS ingress, and policy digests; enable read-only console; enable current safe mutations; only after PLAT-19.1/19.2 tests pass explicitly bind a namespace to Ordinary; then disable the in-cluster legacy proxy. Never combine these into one implicit `ui.enabled` transition.

Inline `ArchiveRef`, current KafkaCluster refs, v1 Restores/Approvals, current object names, immutable plan bytes, existing Jobs/bundles, scorecards, receipts, and archives remain readable. New product DTOs adapt them; no conversion mutates an in-flight object. New saved destinations and check resources are additive. Unknown newer fields are tolerated on responses by older clients, while mutation inputs reject unknown fields.

Rollback: first stop new console mutations, leave accepted CRs and Jobs untouched, and retain their bundles/evidence. Rebind all namespaces to Governed and confirm no pending Ordinary request remains before rolling the controller back. The old controller cannot verify confirmation-only keys because those keys were never in legacy `TrustRoster.approverKeys`; pending Ordinary Restores therefore halt rather than downgrade. Governed v1 work continues. Roll back the console/Ingress independently; the manual loopback UI remains available. Retain session/confirmation public material and old policy snapshots for audit even after private-key rotation. Never delete old verification keys needed by archives.

If controller namespace scoping, persistent key retention, OIDC validation, or policy digest agreement cannot be demonstrated, shared mutations remain disabled and the supported state is read-only console plus explicit localhost administrator mode. That is a release blocker, not a reason to restore the shared ServiceAccount proxy.

## Scoped risks and future boundaries

- PLAT-17 establishes transport, identity, product authorization, idempotency, and status contracts. It does not complete PLAT-03, 06, 07, 08, 09, 14, 18, or 19; their routes stay capability-gated until their domain/controller work lands.
- PLAT-19.1 owns trust rotation, retirement/revocation, and key-usage resources. This report fixes the seam needed by PLAT-19.2 but does not pretend the trust lifecycle exists.
- NetworkPolicy cannot prove TLS, identity, FQDN restrictions, or enforcement on a non-enforcing CNI. Production support requires the chosen ingress/CNI evidence; local work remains docker-desktop only and never EKS.
- Cluster-admin can change CRDs, policies, RBAC, webhook/config, Secrets, or controller images and remains outside the product-role threat boundary. The design prevents ordinary shared-console users and direct namespaced CR writers from bypassing authorization; it does not claim protection from a hostile cluster administrator.
- The API has no database, global search index, catalog, retention executor, Kafka socket pool, or object-store client. Add none until the owning platform task and measured requirement justify it.

This subtask is complete when downstream workers accept these authorization semantics and file boundaries; it is not implementation, deployment, or tracker completion evidence.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
