// api.js -- the ONLY module in this tree that issues a network request.
//
// WHAT THIS MODULE IS. A Kubernetes API client, and nothing else. There is no
// API server of its own between this page and kube-apiserver; there is no
// database, no cache and no server-side component of any kind. The page is
// served by `kubectl proxy --www=./ui --www-prefix=/ui/`, which serves these
// files AND proxies the Kubernetes API on the SAME ORIGIN, attaching the
// viewer's own kubeconfig credential to every request it forwards, server
// side. That is the whole authentication story, and it is why nothing below
// builds a header carrying a credential: the page has none to carry.
//
// RULE 1 -- NO EXTERNAL RESOURCE. Every identifier this module builds is
// relative to the serving origin. `path(...)` is the only place a URL is
// built, and it refuses a segment that could turn a relative identifier into
// an absolute one. `scripts/check-ui-offline.sh` enforces the same rule over
// the whole directory, naming file and line.
//
// RULE 2 -- NO CREDENTIAL IN THE PAGE. No bearer token, key or credential of
// any kind is ever placed in the page, and the page stores nothing: no browser
// storage, no cookie written here, no header carrying a secret.
//
// RULE 3 -- `create` ONLY, PLUS ONE UPDATE. This module exports no delete and
// no generic patch. `patchSuspend` is the single write beyond `create`, and it
// sends a JSON-merge patch touching only `spec.suspend`.
//
// TWO MODES, ONE MODULE, ONE CALL SITE (PLAT-18.1, decision D0 stage 6). The
// first half above is the LEGACY mode: `kubectl proxy` in front of
// kube-apiserver, `/apis/logweir.dev/v1alpha1/...`, and the viewer's own
// kubeconfig credential attached server side. The second half below is the
// CONSOLE mode: `logweir-api` serving this page and a bounded product API at
// `/api/v1/...` on the SAME ORIGIN, answering `application/problem+json`,
// requiring `Idempotency-Key` on a durable create and paging with opaque
// cursors. `ui/client.js` decides which one is in front of the page, ONCE, and
// nothing in this file decides anything: it builds identifiers and reads
// responses.
//
// BOTH HALVES GO THROUGH THE SAME ONE `fetch`. That is the property
// `crates/logweir/tests/ui_lint.rs::every_api_path_is_relative` holds -- one
// call site in all of `ui/`, on one line, whose argument every caller built
// with `path(...)`. A second mode that opened a second call site would have
// put the same-origin claim back out of reach of a grep, which is exactly what
// this file exists to prevent.
//
// STILL NO CREDENTIAL IN THE PAGE, IN EITHER MODE. Legacy mode has none to
// carry. Console mode uses a session COOKIE the browser attaches by itself to
// a same-origin request; this module never reads it, never writes one, and
// stores nothing. The only request-scoped value a caller may hand a write is
// the synchroniser token the product API's own `/session` response carries,
// which `ui/client.js` holds in memory for the life of the loaded page and
// never anywhere else.
//
// THE ONE NETWORK CALL SITE. There is exactly one call to the browser's fetch
// API in all of `ui/`, on one line inside `request` below, and every caller
// hands it an identifier that `path(...)` produced. A reviewer can check both
// properties with a grep, which is the point: the same-origin claim is
// mechanically checkable rather than asserted.

/** The API group the six Logweir kinds live in. */
export const GROUP = "logweir.dev";

/** The served version of that group. */
export const VERSION = "v1alpha1";

/** The five plurals a page may WRITE. Frozen; `create` and `patchSuspend`
 *  throw a RangeError on anything else -- `trustrosters` included, which is
 *  cluster-scoped and admin-only. `path(...)` bounds the SHAPE of an
 *  identifier; this list bounds the SET OF KINDS the page may write, and
 *  without it the only thing stopping a page creating a trust roster would be
 *  a per-page source review. */
export const WRITABLE_PLURALS = Object.freeze([
  "kafkaclusters",
  "backupschedules",
  "backups",
  "restores",
  "approvals",
]);

// The field manager every write from this page carries, so `kubectl get -o
// yaml` shows who wrote each field. It is appended by `create` and by nothing
// else, and it appears exactly once in this file.
const FIELD_MANAGER = "?fieldManager=logweir-ui";

// The one plural `patchSuspend` may address. It is checked against
// WRITABLE_PLURALS on every call like any other plural, so removing that name
// from the allowlist disarms this write too rather than leaving it behind.
const SUSPENDABLE_PLURAL = "backupschedules";

// The scheme separator -- a colon followed by two slashes -- spelled as a
// concatenation rather than as a literal. `every_api_path_is_relative` asserts
// this file contains no occurrence of that three-character sequence at all, so
// that a reviewer grepping the module for one gets an empty result and learns
// something true. Building it here keeps that assertion honest and keeps this
// file plain ASCII: an earlier draft of the plan reached for an invisible
// character to the same end, and invisible characters are what
// `the_ui_sources_are_ascii_only` exists to forbid.
const SCHEME_SEPARATOR = ":" + "//";

/** Builds a same-origin, RELATIVE identifier from path segments:
 *  `path("apis", GROUP, VERSION)` -> `"/apis/logweir.dev/v1alpha1"`.
 *
 *  Throws a TypeError naming the offending segment if a segment is not a
 *  non-empty string, carries a scheme separator, starts with a slash, or
 *  contains a parent-directory hop. Never an absolute URL, never a credential,
 *  never a custom header beyond Content-Type on a write. */
export function path(...segments) {
  for (const segment of segments) {
    if (typeof segment !== "string" || segment.length === 0) {
      throw new TypeError(
        "path(): every segment must be a non-empty string; got " + describe(segment),
      );
    }
    if (segment.indexOf(SCHEME_SEPARATOR) !== -1) {
      throw new TypeError(
        "path(): segment " +
          describe(segment) +
          " carries a scheme separator; every identifier this module builds is relative to the serving origin",
      );
    }
    if (segment.charAt(0) === "/") {
      throw new TypeError(
        "path(): segment " + describe(segment) + " starts with a slash",
      );
    }
    if (segment.indexOf("..") !== -1) {
      throw new TypeError(
        "path(): segment " + describe(segment) + " contains a parent-directory hop",
      );
    }
  }
  return "/" + segments.join("/");
}

/** Lists a namespaced kind. */
export async function list(ns, plural, options) {
  const response = await request(
    path("apis", GROUP, VERSION, "namespaces", ns, plural),
    readInit(options),
  );
  return body(response);
}

/** Reads one namespaced object by name. */
export async function get(ns, plural, name, options) {
  const response = await request(
    path("apis", GROUP, VERSION, "namespaces", ns, plural, name),
    readInit(options),
  );
  return body(response);
}

/** Creates one namespaced object. POST, 201 expected, carrying FIELD_MANAGER
 *  and nothing else. Refuses a plural outside WRITABLE_PLURALS before it
 *  reaches the network. */
export async function create(ns, plural, object) {
  assertWritable("create", plural);
  const response = await request(
    path("apis", GROUP, VERSION, "namespaces", ns, plural) + FIELD_MANAGER,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(object),
    },
  );
  return body(response);
}

/** The one permitted update: a JSON-merge patch over a BackupSchedule that
 *  touches `spec.suspend` and no other field. The CRD's own
 *  `x-kubernetes-validations` rule seals every other field of `.spec` for every
 *  subject, cluster-admin included; this is the page's half of that. */
export async function patchSuspend(ns, name, value) {
  assertWritable("patchSuspend", SUSPENDABLE_PLURAL);
  const response = await request(
    path("apis", GROUP, VERSION, "namespaces", ns, SUSPENDABLE_PLURAL, name),
    {
      method: "PATCH",
      headers: { "Content-Type": "application/merge-patch+json" },
      body: JSON.stringify({ spec: { suspend: value } }),
    },
  );
  return body(response);
}

/** Lists a cluster-scoped kind -- `trustrosters`, which no page may write. */
export async function listCluster(plural, options) {
  const response = await request(
    path("apis", GROUP, VERSION, plural),
    readInit(options),
  );
  return body(response);
}

/** Turns a non-2xx response into an Error carrying `{status, reason, message}`
 *  -- the API server's OWN `reason` and `message`, verbatim -- and, when the
 *  Status names them, its `details` (the object's `name`, `kind` and the
 *  per-field `causes[]` a 422 carries), so a form can put each cause beside the
 *  field it is about without rewording it.
 *
 *  A 403 must read as the API server's 403, because the page's whole
 *  authorisation story is "the API server evaluated the viewer's RBAC". A
 *  message this module invented would be a claim about a decision it did not
 *  make. */
export function apiError(response, text) {
  let reason = "";
  let message = "";
  let details = null;
  try {
    const status = JSON.parse(text);
    if (status !== null && typeof status === "object") {
      if (typeof status.reason === "string") {
        reason = status.reason;
      }
      if (typeof status.message === "string") {
        message = status.message;
      }
      if (status.details !== null && typeof status.details === "object") {
        details = status.details;
      }
    }
  } catch (notJson) {
    // A body that is not a Kubernetes Status is reported as it arrived, below.
    reason = "";
  }
  if (message.length === 0) {
    message = text;
  }
  const error = new Error(message);
  error.status = response.status;
  error.reason = reason;
  if (details !== null) {
    error.details = details;
  }
  return error;
}

/** The console mode's own writable set: the collection routes the product API
 *  has a POST for today. `backups` has none until PLAT-06 and `approvals` none
 *  until PLAT-19.2, and a page that reached for either gets a RangeError here
 *  rather than a 404 from a route that does not exist.
 *
 *  `destinations` and `preflights` joined it with D2 W12. `topic-discoveries`
 *  did NOT: a discovery is created under the connection it is about
 *  (`/connections/{name}/topic-discoveries`), which is a sub-collection and is
 *  reached through [`consoleAction`] and its own bounded table -- not by
 *  POSTing to a top-level plural that has no create route at all. */
export const CONSOLE_WRITABLE_PLURALS = Object.freeze([
  "connections",
  "schedules",
  "restores",
  "destinations",
  "preflights",
  // D1 W7 (PLAT-06.2). `POST .../backups` is "Back up now": a durable create
  // with a required `Idempotency-Key`, which is the whole of what makes a
  // double click, a lost response and a reload one run.
  "backups",
  // D3 W12 (PLAT-15.2). `POST .../catalogs` is "connect an existing archive":
  // the operator names a destination they already hold a read-only credential
  // for, and the controller discovers what is in it. It is a DURABLE CREATE
  // with a required `Idempotency-Key` (D3 section 5.5 step 1: "repeating it returns
  // the same object"), which is exactly why it is here and not an action: a
  // lost response, a double click and a reload must all resolve to the one
  // RecoveryCatalog the first request made, and two catalogs for one
  // destination are refused by the server as a duplicate.
  //
  // THE LEGACY WRITABLE SET IS UNCHANGED. `WRITABLE_PLURALS` above still names
  // five kinds and `recoverycatalogs` is not one of them: this page creates no
  // custom resource against kube-apiserver for any D3 kind, in either mode.
  "catalogs",
]);

/** The product API's session document: the actor, the namespace grants and the
 *  capability flags. THE ONE REQUEST THAT DECIDES THE MODE, issued once by
 *  `ui/client.js` and never again.
 *
 *  It resolves `{ok, status, body}` instead of throwing, because the answer
 *  this page is most interested in is the REFUSAL: a plain `kubectl proxy`
 *  with its path filter answers this identifier with a refusal and a body that
 *  is not JSON, and that refusal is what legacy mode looks like from here. */
export async function session(options) {
  const response = await request(path("api", "v1", "session"), readInit(options));
  const text = await response.text();
  let body = null;
  try {
    body = text.length === 0 ? null : JSON.parse(text);
  } catch (notJson) {
    body = null;
  }
  return { ok: response.ok, status: response.status, body: body };
}

/** Lists one product kind in `ns`. `options.limit` and `options.cursor` are
 *  the product API's own paging parameters; a cursor is opaque and is echoed
 *  back exactly as it arrived. */
export async function consoleList(ns, plural, options) {
  const o = options || {};
  const response = await request(
    path("api", "v1", "namespaces", ns, plural) + listQuery(o),
    readInit(o),
  );
  return problemBody(response);
}

/** Reads one product object by name.
 *
 *  `options.planHash` is the ONE query parameter an item read takes: `GET
 *  .../preflights/{id}?planHash=` asks the product API to recompute
 *  applicability against the plan the caller is looking at NOW, rather than
 *  against the plan the check was bound to when it ran. Every other read
 *  sends no query at all. */
export async function consoleGet(ns, plural, name, options) {
  const o = options || {};
  const response = await request(
    path("api", "v1", "namespaces", ns, plural, name) + listQuery(o),
    readInit(o),
  );
  return problemBody(response);
}

/** Reads one explicitly named sub-resource of a product object: the approval
 *  packet, a destination's `/usage`, a discovery's stored `/topics` page, a
 *  preflight's `/details` page, and a connection's `/topic-discoveries` list.
 *  Each is reached only by naming it.
 *
 *  `options` may carry the product API's paging parameters and the bounded
 *  FILTERS the paged sub-resources declare. The filter names are a frozen
 *  list, so a caller cannot smuggle an arbitrary query parameter through this
 *  seam, and the values are encoded here exactly as `limit` and `cursor` are. */
export async function consoleSub(ns, plural, name, sub, options) {
  const o = options || {};
  const response = await request(
    path("api", "v1", "namespaces", ns, plural, name, sub) + listQuery(o),
    readInit(o),
  );
  return problemBody(response);
}

/** Reads one operation's normalized status. `kind` is the closed set the
 *  product API declares; anything else is refused here, before the network.
 *
 *  FOUR KINDS, TWO SHAPES. `backup` and `restore` answer `OperationResponse`;
 *  `discovery` and `preflight` answer `CheckOperationResponse`, which carries
 *  no result, no evidence and no verification -- a transient check has none of
 *  those facts. This module only builds the identifier; `ui/contract.js`
 *  decides which shape the answer is read as, and refuses to read one as the
 *  other. */
export async function consoleOperation(ns, kind, name, options) {
  if (CONSOLE_OPERATION_KINDS.indexOf(kind) === -1) {
    throw new RangeError(
      "consoleOperation(): kind is one of " + CONSOLE_OPERATION_KINDS.join(", ") +
        "; got " + String(kind),
    );
  }
  const response = await request(
    path("api", "v1", "namespaces", ns, "operations", kind, name),
    readInit(options),
  );
  return problemBody(response);
}

/** Lists one CLUSTER-SCOPED product kind. The product API serves exactly one
 *  (`GET /api/v1/trust-policies`, D3 section 10, installation-admin read), and the
 *  frozen list below is what bounds that: a page that reached for a second
 *  cluster-scoped plural is refused here, before the network.
 *
 *  A CLUSTER-SCOPED READ AND NOTHING MORE. D3 section 10 keeps trust WRITES off the
 *  API in v1 (`capabilities.trustAdministration: false`); the supported path
 *  is `kubectl apply` plus the `logweir trust` helpers. There is no cluster
 *  plural in [`CONSOLE_WRITABLE_PLURALS`] and this module offers no writer
 *  that takes one. */
export async function consoleClusterList(plural, options) {
  assertClusterReadable("consoleClusterList", plural);
  const response = await request(
    path("api", "v1", plural) + listQuery(options || {}),
    readInit(options),
  );
  return problemBody(response);
}

/** Reads one cluster-scoped product object by name. */
export async function consoleClusterGet(plural, name, options) {
  assertClusterReadable("consoleClusterGet", plural);
  const response = await request(
    path("api", "v1", plural, name) + listQuery(options || {}),
    readInit(options),
  );
  return problemBody(response);
}

/** THE ONE STREAM THIS PAGE OPENS, AND THE SECOND REQUEST SITE IN ALL OF `ui/`
 *  (D3 section 2.6).
 *
 *  `EventSource` IS A REQUEST, SO IT LIVES HERE. Every rule the one `fetch`
 *  obeys, this obeys: the identifier is built by `path(...)` on the line of
 *  the construction, so it is relative to the origin that served the page; no
 *  header is set, because `EventSource` cannot set one and this page has no
 *  credential to put in it either way; and nothing is stored. The browser
 *  attaches the session cookie itself in console mode, exactly as it does to
 *  the `fetch`.
 *
 *  `withCredentials` IS DELIBERATELY NOT SET. It only means anything for a
 *  CROSS-ORIGIN stream, and a cross-origin stream is the thing this whole
 *  module is built to make impossible: the identifier below cannot carry a
 *  scheme, a leading slash or a parent-directory hop, so the stream is always
 *  same-origin and same-origin requests carry the cookie by default.
 *
 *  NO TOKEN IN THE URL. A stream identifier lands in proxy logs, in browser
 *  history and in a `Referer`; the product API authenticates this stream the
 *  same way it authenticates every other read, from the cookie the browser
 *  attaches. There is no query parameter here at all.
 *
 *  It returns the stream object. Closing it is the caller's -- `watchOperation`
 *  in `ui/operation-watch.js` closes it on `lifecycle.signal` and on a
 *  terminal operation, and aborting a watch never cancels the run. */
export function openOperationStream(ns, kind, name, EventSourceClass) {
  if (CONSOLE_OPERATION_KINDS.indexOf(kind) === -1) {
    throw new RangeError(
      "openOperationStream(): kind is one of " + CONSOLE_OPERATION_KINDS.join(", ") +
        "; got " + String(kind),
    );
  }
  const Source = EventSourceClass || globalThis.EventSource;
  if (typeof Source !== "function") {
    throw new TypeError(
      "openOperationStream(): this browser has no EventSource, so this page cannot follow an " +
        "operation as a stream; the caller falls back to polling.",
    );
  }
  return new Source(path("api", "v1", "namespaces", ns, "operations", kind, name, "events"));
}

/** THE SIX NAMED ACTION ROUTES, AND NOTHING ELSE. The product API spells an
 *  action as a verb suffix on an object's own identifier
 *  (`destinations/primary:test`) or as a sub-collection create
 *  (`connections/source/topic-discoveries`). Neither shape is a plural this
 *  page could POST to, so neither could go through `consoleCreate`.
 *
 *  THIS IS NOT A GENERIC POST HELPER. `action` is looked up in a FROZEN table
 *  below, so the set of routes any page in this tree can reach is a list a
 *  reviewer reads in one place -- exactly as `CONSOLE_WRITABLE_PLURALS` bounds
 *  the creates. An action the table does not name is a RangeError before the
 *  network, with the permitted set in the message.
 *
 *  THE IDEMPOTENCY KEY IS THE ROUTE'S, NOT THE CALLER'S. Three of these six
 *  are durable creates and the product API REQUIRES a key on each; the other
 *  three refuse one with a 400 (a rotation's replay guard is
 *  `expectedGeneration`, and a cancel's is that cancelling twice is the same
 *  wish). The table says which, and a caller that hands a key to a route that
 *  refuses one -- or omits it on a route that requires one -- is refused here
 *  rather than by the server. */
export async function consoleAction(ns, action, name, body, options) {
  const route = CONSOLE_ACTIONS[action];
  if (route === undefined) {
    throw new RangeError(
      "consoleAction(): " + String(action) + " is not an action this page may take; the set " +
        "is " + Object.keys(CONSOLE_ACTIONS).join(", ") + ".",
    );
  }
  const o = options || {};
  const key = o.idempotencyKey === undefined ? null : o.idempotencyKey;
  if (route.key === true && (key === null || key === undefined)) {
    throw new RangeError(
      "consoleAction(): " + String(action) + " is a durable create and the route requires an " +
        "Idempotency-Key; none was given.",
    );
  }
  if (route.key === false && key !== null && key !== undefined) {
    throw new RangeError(
      "consoleAction(): " + String(action) + " refuses an Idempotency-Key (the product API " +
        "answers 400); its replay guard is the request's own precondition.",
    );
  }
  // TWO CALLS, NOT ONE CALL OVER A VARIABLE. `every_api_path_is_relative`
  // requires the first argument of every send in this module to BE a
  // `path(...)` call, so a reviewer grepping for the one network seam reads
  // the identifier beside it rather than following a binding. Building the
  // identifier above and passing it here would have been outside that
  // guarantee -- not because this particular expression is wrong, but because
  // the property stops being checkable by reading.
  const init = writeInit(
    body,
    Object.assign({}, o, { idempotencyKey: route.key === true ? key : null }),
  );
  const response = route.named === true
    ? await request(
      path("api", "v1", "namespaces", ns, route.plural, String(name) + route.suffix),
      init,
    )
    : await request(path("api", "v1", "namespaces", ns, route.plural + route.suffix), init);
  return problemBody(response);
}

/** Creates one product object. `options.idempotencyKey` is REQUIRED by the
 *  route and is what makes a lost response, a double click and a restart all
 *  target the same object; this module refuses to send a durable create
 *  without one rather than let the server say so.
 *
 *  NO ROUTE SIGNAL IS ACCEPTED, for the reason `readInit` gives: navigation
 *  after a click must not cancel an operation the server has already taken. */
export async function consoleCreate(ns, plural, body, options) {
  assertConsoleWritable("consoleCreate", plural);
  const response = await request(
    path("api", "v1", "namespaces", ns, plural),
    writeInit(body, options),
  );
  return problemBody(response);
}

/** The one permitted product update: a schedule's suspension, guarded by the
 *  `expectedResourceVersion` the caller last read. It takes no idempotency
 *  key -- that precondition is its replay guard -- and the route is named by a
 *  verb suffix on the object's own identifier. */
export async function consoleSetSuspension(ns, name, body, options) {
  const response = await request(
    path("api", "v1", "namespaces", ns, "schedules", name + SUSPENSION_VERB),
    writeInit(body, Object.assign({}, options || {}, { idempotencyKey: null })),
  );
  return problemBody(response);
}

/** Previews a DRAFT cadence: `GET /api/v1/cadence-previews`.
 *
 *  NO NAMESPACE, NO KUBERNETES OBJECT, NO WRITE. The product API compiles a
 *  preset to its canonical cron expression, resolves the zone against the tz
 *  database it was built with, and returns the next firings. THE BROWSER NEVER
 *  EVALUATES CRON -- a second implementation in a page is a second opinion
 *  about when a backup runs -- so this route is the schedule form's only
 *  source for both the canonical expression it saves and the instants it shows.
 *
 *  The parameters are a FROZEN ALLOWLIST, as `consoleSub`'s filters are: a
 *  caller hands `{preset: "daily", hour: 2, minute: 0, timeZone: "Europe/Berlin"}`
 *  and gets exactly those, and an invented parameter is dropped here rather
 *  than sent to a route that answers `400 malformed_request` for it. */
export async function cadencePreview(query, options) {
  const response = await request(
    path("api", "v1", "cadence-previews") + previewQuery(query || {}),
    readInit(options),
  );
  return problemBody(response);
}

/** Replaces one schedule's FUTURE POLICY: the product API's one replace.
 *
 *  A REPLACE, NOT A PATCH, AND THE DIFFERENCE IS THE WHOLE CONTRACT. A field
 *  omitted from `body` is REMOVED from the schedule, so the caller sends the
 *  complete policy it means -- which is why the form that owns this route owns
 *  every field of that policy. The replay guard is `body.expectedGeneration`
 *  and not an idempotency key: the route answers `400
 *  idempotency_key_invalid` for a key, and `412 precondition_failed` for a
 *  generation that has moved, so a second submission of a stale form is
 *  refused by the revision it was built from rather than silently applied.
 *
 *  It is the ONE identifier in this module reached with a method other than
 *  GET or POST, and `crates/logweir/tests/ui_lint.rs` holds it to exactly
 *  that: the replace method occurs once in this file, as `REPLACE_METHOD`'s
 *  value, and the removal verb occurs not at all. (Neither word is spelled in
 *  this paragraph, because that gate matches both tokens BARE and a comment is
 *  bytes like any other.) */
export async function consoleSchedulePolicy(ns, name, body, options) {
  const init = writeInit(body, Object.assign({}, options || {}, { idempotencyKey: null }));
  init.method = REPLACE_METHOD;
  const response = await request(
    path("api", "v1", "namespaces", ns, "schedules", name),
    init,
  );
  return problemBody(response);
}

/** THE ONE CLOCK THIS PAGE IS ALLOWED TO JUDGE FRESHNESS BY (D3 section 7.7).
 *
 *  The instant of the most recent answer, as the SERVER dated it -- the
 *  `logweir-api` process in console mode, the Kubernetes API server (through
 *  `kubectl proxy`) in legacy mode. Both are HTTP's own `Date` header, which
 *  every one of them sets, and both are a clock the cluster actually saw.
 *
 *  WHY NOT `Date.now()`. A keys view that compared `status.evaluatedAt` with
 *  the browser's clock would call a healthy evaluation stale on a laptop five
 *  minutes fast, and call a stale one fresh on one five minutes slow -- and it
 *  would disagree with the controller's own refusal by exactly that skew. The
 *  existing rule in this tree is "a page renders no verdict from a clock the
 *  cluster never saw", and this is what keeps it true for the one column that
 *  needs an elapsed time at all.
 *
 *  `null` UNTIL AN ANSWER HAS ARRIVED, and `null` if a proxy strips the
 *  header. A caller with no server instant has not established freshness, and
 *  D3 section 7.7's answer to "not established" is `unknown` -- never `valid`.
 *
 *  AND `null` ONCE IT IS TOO OLD TO BE "NOW". A recorded instant that never
 *  expired would be a clock that stops: a route whose answers carry no `Date`
 *  would leave the keys view comparing an evaluation against an instant from
 *  minutes ago, and an instant in the past SHRINKS the measured age -- which
 *  is the direction that reads FRESH. Section 7.7's rule is fail-closed, so
 *  the instant is carried forward by the elapsed time since it was recorded
 *  and dropped altogether past [`SERVER_TIME_MAX_AGE_MS`].
 *
 *  THE ELAPSED TIME IS A DURATION AND NEVER AN ABSOLUTE READING. It comes from
 *  the monotonic clock where there is one (`performance.now()`), and from
 *  `Date.now()` only as a difference of two of its readings. A browser five
 *  minutes fast measures a ten-second gap as ten seconds; that is the whole
 *  reason this is allowed to exist beside "a page renders no verdict from a
 *  clock the cluster never saw". */
export function serverTime() {
  if (lastServerTime === null) {
    return null;
  }
  const mark = elapsedMark();
  if (mark === null || lastServerMark === null) {
    return lastServerTime;
  }
  const elapsed = mark - lastServerMark;
  if (elapsed < 0 || elapsed > SERVER_TIME_MAX_AGE_MS) {
    return null;
  }
  return lastServerTime + elapsed;
}

/** How long a recorded server instant may be carried forward before a caller
 *  is told there is none. Five minutes: long enough that an idle tab keeps
 *  answering, short enough that no verdict rests on an instant from a
 *  different session of work. */
export const SERVER_TIME_MAX_AGE_MS = 300000;

// The value [`serverTime`] is built from, and the monotonic mark it was
// recorded at. Both are numbers in memory for the life of the loaded page and
// nothing else: not a cookie, not browser storage, not a header this module
// sends anywhere.
let lastServerTime = null;
let lastServerMark = null;

// A monotonic reading, for DURATIONS only. `performance.now()` where there is
// one; `Date.now()` otherwise, and only ever as the difference of two of its
// readings.
function elapsedMark() {
  const clock = globalThis.performance;
  if (clock !== undefined && clock !== null && typeof clock.now === "function") {
    return clock.now();
  }
  return Date.now();
}

// Records the `Date` of one answer. A header that is absent or unparseable
// leaves the previous value alone rather than clearing it: one proxy that
// strips the header on one route must not make every other verdict unknown --
// the age bound above is what stops that leniency becoming a stopped clock.
function noteServerTime(response) {
  const headers = response === null || response === undefined ? null : response.headers;
  if (headers === null || headers === undefined || typeof headers.get !== "function") {
    return;
  }
  const dated = headers.get("Date");
  if (typeof dated !== "string" || dated.length === 0) {
    return;
  }
  const at = Date.parse(dated);
  if (!isNaN(at)) {
    lastServerTime = at;
    lastServerMark = elapsedMark();
  }
}

/** Turns a non-2xx product-API response into an Error carrying the problem
 *  document's own `code`, `detail`, `requestId` and field `errors[]`.
 *
 *  `reason` is the problem CODE, not prose: `render.js`'s `errorBox` shows
 *  `status reason`, and a stable code is what a reader can search for and what
 *  `docs/api.md` documents. The decoded document is attached as `problem` so
 *  `ui/client.js` can translate its field paths without parsing anything
 *  twice.
 *
 *  A body that is NOT a problem document is itself a contract violation -- the
 *  product API answers every error that way -- so the error says so, and
 *  carries the bytes verbatim rather than inventing a sentence. */
export function problemError(response, text) {
  let document = null;
  try {
    const parsed = JSON.parse(text);
    if (parsed !== null && typeof parsed === "object" && typeof parsed.code === "string") {
      document = parsed;
    }
  } catch (notJson) {
    document = null;
  }
  if (document === null) {
    const error = new Error(
      "the product API answered " + String(response.status) + " with a body that is not a " +
        "problem document, which every error of this API is: " + text,
    );
    error.status = response.status;
    error.reason = "ContractViolation";
    error.kind = "contract";
    return error;
  }
  const error = new Error(
    typeof document.detail === "string" && document.detail.length > 0 ? document.detail : text,
  );
  error.status = response.status;
  error.reason = document.code;
  error.code = document.code;
  error.problem = document;
  // THE PER-FIELD CAUSES A 422 CARRIES, IN THE SHAPE A FORM ALREADY READS.
  //
  // THE LIVE RUN FOUND THIS ONE. `Problem.errors[]` is `{field, code,
  // message}` and `lifecycle.js::fieldErrors` reads `details.causes[]` as
  // `{field, message, reason}`; only four call sites in `ui/client.js` bridged
  // the two, and the D3 creates do not go through any of them. So a real 422
  // from `POST .../catalogs` -- "a catalog name is a DNS-1123 subdomain",
  // against the field `name` -- reached the connect form with NO field
  // errors at all, and the form still printed "fix the fields marked below"
  // with nothing marked. A page that names a repair it does not point at is
  // worse than one that says only what went wrong.
  //
  // Bridged HERE, at the one place every product-API error is built, rather
  // than at each caller: the per-caller `withCauses` still runs afterwards
  // where it exists and still wins, because it canonicalises the path into the
  // custom resource's vocabulary and this cannot -- it does not know the
  // plural. The messages are the server's own either way, verbatim.
  if (Array.isArray(document.errors) && document.errors.length > 0) {
    error.details = {
      causes: document.errors.map((cause) => {
        const c = cause || {};
        return {
          field: typeof c.field === "string" ? c.field : "",
          message: typeof c.message === "string" ? c.message : "",
          reason: typeof c.code === "string" ? c.code : "invalid",
        };
      }),
    };
  }
  if (typeof document.requestId === "string") {
    error.requestId = document.requestId;
  }
  return error;
}

// --------------------------------------------------------------- private half

// THE ONE NETWORK CALL SITE IN ALL OF `ui/`. Every caller above passes an
// identifier `path(...)` returned, so every request this page makes is
// relative to the origin that served the page -- which is the origin
// `kubectl proxy` is already authenticating to the cluster on the viewer's
// behalf.
function request(u, init) {
  return fetch(u, init);
}

// GET requests may carry a route-owned AbortSignal. Writes intentionally do
// not accept one: a route leaving after a click must not cancel an operation
// the API server has already accepted.
function readInit(options) {
  const init = { method: "GET" };
  if (options && options.signal !== undefined) {
    init.signal = options.signal;
  }
  return init;
}

// The verb suffix the product API puts on a schedule's identifier for its one
// update. Spelled as a constant so the colon in it is visible in exactly one
// place, beside the paragraph that says what it is.
const SUSPENSION_VERB = ":set-suspension";

// The HTTP method `consoleSchedulePolicy` sends, and the ONE place this module
// spells it. `ui_lint.rs::the_api_module_offers_no_delete_and_no_put` asserts
// that the token appears exactly once in this file and that it is the value of
// this constant -- so widening the module to a second replace, or to a delete,
// is a change a reviewer reads here rather than one that hides in a call.
const REPLACE_METHOD = "PUT";

// The parameters `GET /api/v1/cadence-previews` declares, frozen. `schedule`
// and `preset` are alternatives -- exactly one -- and the four integers belong
// to whichever preset was named; the route answers `422` for a parameter of a
// DIFFERENT preset, which is a refusal this page shows rather than pre-empts.
const PREVIEW_PARAMETERS = Object.freeze([
  "schedule", "preset", "timeZone", "after",
]);

const PREVIEW_NUMBERS = Object.freeze([
  "minute", "hour", "dayOfWeek", "dayOfMonth", "n", "count",
]);

// The preview's query string, built from the allowlist above and nothing else.
function previewQuery(query) {
  const parts = [];
  for (const name of PREVIEW_PARAMETERS) {
    const value = query[name];
    if (typeof value === "string" && value.length > 0) {
      parts.push(name + "=" + encodeURIComponent(value));
    }
  }
  for (const name of PREVIEW_NUMBERS) {
    const value = query[name];
    if (typeof value === "number" && isFinite(value)) {
      parts.push(name + "=" + encodeURIComponent(String(Math.floor(value))));
    }
  }
  return parts.length === 0 ? "" : "?" + parts.join("&");
}

// The four kinds `GET .../operations/{kind}/{name}` serves. `backup` and
// `restore` are durable runs; `discovery` and `preflight` are transient checks
// and answer a DIFFERENT document. The list is frozen here so a page cannot
// address a fifth kind that no route serves.
const CONSOLE_OPERATION_KINDS = Object.freeze([
  "backup",
  "restore",
  "discovery",
  "preflight",
]);

// THE SIX ACTION ROUTES, WRITTEN OUT. `plural` is the collection the route
// hangs off, `named` says whether it addresses one object, `suffix` is the
// verb or sub-collection, and `key` says whether the product API requires an
// `Idempotency-Key` (`true`), refuses one (`false`).
//
// The three `key: false` routes are not "less safe": a rotation's replay guard
// is `expectedGeneration` and a cancel's is that cancelling a cancelled check
// is the same wish, so a key there would be a second answer to a question that
// already has one -- which is why the product API answers 400 for it.
const CONSOLE_ACTIONS = Object.freeze({
  "destinations:from-legacy": Object.freeze({
    plural: "destinations", named: false, suffix: ":from-legacy", key: true,
  }),
  "destinations:update-access": Object.freeze({
    plural: "destinations", named: true, suffix: ":update-access", key: false,
  }),
  "destinations:test": Object.freeze({
    plural: "destinations", named: true, suffix: ":test", key: true,
  }),
  "connections:topic-discoveries": Object.freeze({
    plural: "connections", named: true, suffix: "/topic-discoveries", key: true,
  }),
  "topic-discoveries:cancel": Object.freeze({
    plural: "topic-discoveries", named: true, suffix: ":cancel", key: false,
  }),
  "preflights:cancel": Object.freeze({
    plural: "preflights", named: true, suffix: ":cancel", key: false,
  }),
});

// The bounded FILTER parameters the paged sub-resources declare, by the name
// the route publishes. An allowlist and never a pass-through bag: a caller
// hands `{q: "orders"}` and gets `?q=orders`, and a caller that invented a
// parameter gets nothing rather than a query string this page cannot account
// for.
const SUB_FILTERS = Object.freeze([
  // GET .../topic-discoveries/{id}/topics
  "q", "prefix", "internal", "errored",
  // GET .../preflights/{id}/details
  "check",
  // GET .../connections/{name}/topic-discoveries
  "latest",
  // GET .../catalogs/{name}/points. ONE FILTER, which is the materialised
  // judgement and not either axis: the catalog decides `selectable`, the route
  // filters on it, and no page in this tree recomputes
  // `Available AND (Verified|VerifiedHistorical)`. The published route takes
  // `limit`, `cursor` and this; an `availability=` or `verification=` this
  // client invented would be a query parameter the API does not declare.
  "selectable",
]);

// The header name that carries the product API's synchroniser token on an
// unsafe request. PLAT-17.2 owns the final spelling -- today's localAdmin mode
// returns `null` for the token and relies on the loopback listener and an
// exact Origin check, so nothing is sent and this line is not yet exercised
// against a server. It is ONE line on purpose: when the identity stage names
// the header, this is the only place that changes.
const TOKEN_HEADER = "X-CSRF-Token";

// The product API's paging parameters, as a query string. A cursor is opaque:
// it is echoed back exactly as it arrived, and never parsed, shortened or
// re-signed here.
function listQuery(options) {
  const parts = [];
  if (typeof options.limit === "number" && options.limit > 0) {
    parts.push("limit=" + encodeURIComponent(String(Math.floor(options.limit))));
  }
  if (typeof options.cursor === "string" && options.cursor.length > 0) {
    parts.push("cursor=" + encodeURIComponent(options.cursor));
  }
  for (const filter of SUB_FILTERS) {
    const value = options[filter];
    if (typeof value === "string" && value.length > 0) {
      parts.push(filter + "=" + encodeURIComponent(value));
    } else if (value === true) {
      parts.push(filter + "=true");
    }
  }
  // THE PLAN HASH A PREFLIGHT READ IS COMPARED AGAINST. `GET
  // .../preflights/{id}?planHash=` is how the product API recomputes
  // applicability against the plan the caller is LOOKING AT rather than the
  // one the check was bound to; sending nothing means "no plan of my own",
  // which is a different question and not a weaker one.
  if (typeof options.planHash === "string" && options.planHash.length > 0) {
    parts.push("planHash=" + encodeURIComponent(options.planHash));
  }
  return parts.length === 0 ? "" : "?" + parts.join("&");
}

// A product-API write: JSON, the idempotency key the route requires, and the
// synchroniser token when the session carries one. No signal, ever.
function writeInit(body, options) {
  const o = options || {};
  const headers = { "Content-Type": "application/json" };
  if (o.idempotencyKey !== null && o.idempotencyKey !== undefined) {
    if (typeof o.idempotencyKey !== "string" || o.idempotencyKey.length < 8 ||
      o.idempotencyKey.length > 128) {
      throw new RangeError(
        "a durable create carries an Idempotency-Key of 8 to 128 visible characters; got " +
          describe(o.idempotencyKey),
      );
    }
    headers["Idempotency-Key"] = o.idempotencyKey;
  }
  if (typeof o.token === "string" && o.token.length > 0) {
    headers[TOKEN_HEADER] = o.token;
  }
  return { method: "POST", headers: headers, body: JSON.stringify(body) };
}

// Reads a product-API response, raising the problem document on a non-2xx and
// a NAMED contract violation on a 2xx whose body is not JSON.
//
// The second half is not hypothetical politeness: something between this page
// and the product API -- a captive portal, a proxy's own error page -- answers
// 200 with HTML, and a bare `JSON.parse` puts `Unexpected token '<'` in the
// error box of a page whose whole contract story is that a body which is not
// what it promised is said so by name. This file already builds that failure
// for a non-2xx; it builds the same one here.
async function problemBody(response) {
  noteServerTime(response);
  const text = await response.text();
  if (!response.ok) {
    throw problemError(response, text);
  }
  if (text.length === 0) {
    return null;
  }
  return parsed(response, text);
}

// The same reading for the legacy half: a 2xx that is not JSON is named
// rather than reported as a parser's complaint about a byte.
function parsed(response, text) {
  try {
    return JSON.parse(text);
  } catch (notJson) {
    const error = new Error(
      "the server answered " + String(response.status) + " with a body that is not JSON, and " +
        "every successful answer of this API is: " + text.slice(0, 200),
    );
    error.status = response.status;
    error.reason = "ContractViolation";
    error.kind = "contract";
    throw error;
  }
}

// The cluster-scoped product plurals this page may READ. One entry, and the
// list exists so that a second one is a line a reviewer reads rather than a
// call that quietly appeared: a cluster-scoped read in a shared console is a
// read of something no namespace grant bounds.
const CONSOLE_CLUSTER_PLURALS = Object.freeze(["trust-policies"]);

// Refuses a cluster-scoped plural outside that list, by name, before anything
// is sent.
function assertClusterReadable(caller, plural) {
  if (CONSOLE_CLUSTER_PLURALS.indexOf(plural) === -1) {
    throw new RangeError(
      caller + "(): " + String(plural) + " is not a cluster-scoped kind this page may read; " +
        "the set is " + CONSOLE_CLUSTER_PLURALS.join(", ") + ".",
    );
  }
}

// Refuses a product kind this page may not create, by name, before anything is
// sent. The legacy half's `assertWritable` bounds the kinds a Kubernetes write
// may name; this bounds the kinds a product write may name, and the two lists
// are separate because the two APIs expose different sets.
function assertConsoleWritable(caller, plural) {
  if (CONSOLE_WRITABLE_PLURALS.indexOf(plural) === -1) {
    throw new RangeError(
      caller + "(): " + String(plural) + " has no create route in the product API; the set " +
        "with one is " + CONSOLE_WRITABLE_PLURALS.join(", ") + ".",
    );
  }
}

// Reads a response, raising the API server's own error on a non-2xx.
async function body(response) {
  noteServerTime(response);
  const text = await response.text();
  if (!response.ok) {
    throw apiError(response, text);
  }
  if (text.length === 0) {
    return null;
  }
  return parsed(response, text);
}

// Refuses a plural the page may not write, by name, before anything is sent.
function assertWritable(caller, plural) {
  if (WRITABLE_PLURALS.indexOf(plural) === -1) {
    throw new RangeError(
      caller +
        "(): " +
        String(plural) +
        " is not a kind this page may write; the writable set is " +
        WRITABLE_PLURALS.join(", ") +
        ". trustrosters is cluster-scoped and admin-only.",
    );
  }
}

// Names a value in an error message without depending on its type.
function describe(value) {
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  return String(value) + " (" + typeof value + ")";
}
