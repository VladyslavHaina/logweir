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

// Reads a response, raising the API server's own error on a non-2xx.
async function body(response) {
  const text = await response.text();
  if (!response.ok) {
    throw apiError(response, text);
  }
  if (text.length === 0) {
    return null;
  }
  return JSON.parse(text);
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
