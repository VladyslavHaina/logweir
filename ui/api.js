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

/** The console mode's own writable set. `connections`, `schedules` and
 *  `restores` are the three the product API has a create route for today;
 *  `backups` has none until PLAT-06 and `approvals` none until PLAT-19.2, and
 *  a page that reached for either gets a RangeError here rather than a 404
 *  from a route that does not exist. */
export const CONSOLE_WRITABLE_PLURALS = Object.freeze([
  "connections",
  "schedules",
  "restores",
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

/** Reads one product object by name. */
export async function consoleGet(ns, plural, name, options) {
  const response = await request(
    path("api", "v1", "namespaces", ns, plural, name),
    readInit(options),
  );
  return problemBody(response);
}

/** Reads one explicitly named sub-resource of a product object -- today the
 *  approval packet, which is the ONLY route that returns approval document
 *  bytes and is reached only by naming it. */
export async function consoleSub(ns, plural, name, sub, options) {
  const response = await request(
    path("api", "v1", "namespaces", ns, plural, name, sub),
    readInit(options),
  );
  return problemBody(response);
}

/** Reads one operation's normalized status. `kind` is the closed set the
 *  product API declares; anything else is refused here, before the network. */
export async function consoleOperation(ns, kind, name, options) {
  if (kind !== "backup" && kind !== "restore") {
    throw new RangeError(
      "consoleOperation(): kind is backup or restore; got " + String(kind),
    );
  }
  const response = await request(
    path("api", "v1", "namespaces", ns, "operations", kind, name),
    readInit(options),
  );
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
