// validate.js -- ONE set of checks, ONE vocabulary of field paths, for both
// modes.
//
// THE PROBLEM THIS SOLVES. A form's own checks speak the form's field names
// (`servers`, `secret`). A Kubernetes 422 speaks JSON paths into the custom
// resource (`spec.bootstrapServers[0]`). The product API's problem+json speaks
// JSON paths into ITS request body (`bootstrapServers[0]`, `auth.credentialRef`).
// Three vocabularies for one disagreement, and a client that does not translate
// between them has to choose which server it renders beside the field -- so
// before PLAT-18.1 there was only one server it could.
//
// THE CANONICAL VOCABULARY IS THE CUSTOM RESOURCE'S. Not because it is nicer,
// but because it is the one the pages already speak: every page exports a
// `*_FIELD_PATHS` table mapping `spec.auth.username` onto its own input, and
// `lifecycle.js`'s `fieldErrors` already resolves the longest matching prefix.
// So the console path is translated INTO that vocabulary here, and every page
// renders a product-API field error beside the right input with no change of
// its own. [`CONSOLE_FIELD_PATHS`] is the whole translation, stated once.
//
// AND THE CHECKS THEMSELVES RUN IN BOTH MODES. [`validateRequest`] is applied
// by `client.js` to the body about to be sent, whichever mode is selected, and
// it produces the SAME `FieldError[]` shape the product API produces -- so a
// refusal this client makes and a refusal the server makes reach a form
// through one path and render the same way. It checks structure, required
// fields, closed enumerations and the two formats the product has always
// enforced (an object name, a host and port); it does not re-implement the
// CRD's schema or the controller's probe, because the client-side check is a
// convenience and the server is the gate.
//
// THIS MODULE ISSUES NO REQUEST, READS NO CLOCK AND TOUCHES NO DOM.

import { CONNECTION_AUTH_MODES, RESTORE_MODES } from "./contract.js";

/** A lowercase RFC 1123 subdomain: what Kubernetes accepts as an object name. */
const DNS_SUBDOMAIN = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/;

/** A host and a port, the shape a Kafka client dials. */
const HOST_PORT = /^(\[[0-9A-Fa-f:.]+\]|[^\s:[\]]+):([0-9]{1,5})$/;

/** `sha256:` and sixty-four lowercase hex digits -- the form
 *  `logweir_core::ids::sha256_prefixed` produces and an Approval binds. */
const PLAN_HASH = /^sha256:[0-9a-f]{64}$/;

/** True when `value` is a name the API server will accept for an object. */
export function isObjectName(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 253 &&
    DNS_SUBDOMAIN.test(value);
}

/** True when `value` is `host:port` with a port in range. */
export function isHostPort(value) {
  if (typeof value !== "string") {
    return false;
  }
  const match = HOST_PORT.exec(value);
  return match !== null && Number(match[2]) >= 1 && Number(match[2]) <= 65535;
}

/** True when `value` is an instant both sides can read. */
export function isInstant(value) {
  return typeof value === "string" && value.length > 0 && !isNaN(new Date(value).getTime());
}

/** True when `value` is the prefixed sha256 an approval binds. */
export function isPlanHash(value) {
  return typeof value === "string" && PLAN_HASH.test(value);
}

// ===========================================================================
// the translation
// ===========================================================================

/** The product API's field paths, by plural, mapped onto the custom
 *  resource's. The longest matching prefix wins, and an index or a deeper
 *  segment is carried across unchanged: `bootstrapServers[0]` becomes
 *  `spec.bootstrapServers[0]`.
 *
 *  A path with no entry here is left ALONE rather than guessed at. It then
 *  matches no page's table and `fieldErrors` returns it in `unmatched`, which
 *  a form renders beside itself -- visible, attributed, and not silently
 *  dropped. That is the right answer for `Idempotency-Key` and for a query
 *  parameter, neither of which is a field on any form. */
export const CONSOLE_FIELD_PATHS = Object.freeze({
  connections: Object.freeze([
    ["role", "spec.role"],
    ["bootstrapServers", "spec.bootstrapServers"],
    ["auth.mode", "spec.auth.mode"],
    ["auth.tls", "spec.auth.tls"],
    ["auth.username", "spec.auth.username"],
    ["auth.credentialRef", "spec.auth.secretRef"],
    ["auth", "spec.auth"],
    ["markerTopic", "spec.markerTopic"],
  ]),
  schedules: Object.freeze([
    ["schedule", "spec.schedule"],
    ["sourceRef", "spec.sourceRef"],
    ["topics", "spec.topics"],
    ["archive.credentialRef", "spec.archive.secretRef"],
    ["archive.url", "spec.archive.url"],
    ["archive", "spec.archive"],
    ["retention", "spec.retention"],
    ["concurrencyPolicy", "spec.concurrencyPolicy"],
    ["suspended", "spec.suspend"],
    ["expectedResourceVersion", "metadata.resourceVersion"],
  ]),
  restores: Object.freeze([
    ["planBytes", "spec.planBytes"],
    ["planHash", "spec.planBytes"],
    ["approvalRef", "spec.approvalRef"],
    ["sourceArchive.credentialRef", "spec.sourceArchive.secretRef"],
    ["sourceArchive.url", "spec.sourceArchive.url"],
    ["sourceArchive", "spec.sourceArchive"],
    ["backupSetRef", "spec.backupSetRef"],
    ["pointInTime", "spec.pointInTime"],
    ["target.clusterRef", "spec.target.clusterRef"],
    ["target.mode", "spec.target.mode"],
    ["target.topicNaming", "spec.target.topicNaming"],
    ["target", "spec.target"],
    ["deadlineSeconds", "spec.deadlineSeconds"],
  ]),
});

/** One product-API field path in the custom resource's vocabulary. */
export function canonicalFieldPath(plural, path) {
  const table = CONSOLE_FIELD_PATHS[plural];
  if (!Array.isArray(table) || typeof path !== "string" || path.length === 0) {
    return typeof path === "string" ? path : "";
  }
  let best = null;
  let length = -1;
  for (const pair of table) {
    const prefix = pair[0];
    const matches = path === prefix ||
      path.indexOf(prefix + ".") === 0 ||
      path.indexOf(prefix + "[") === 0;
    if (matches && prefix.length > length) {
      best = pair;
      length = prefix.length;
    }
  }
  return best === null ? path : best[1] + path.slice(length);
}

/** The product API's `errors[]`, translated into the Kubernetes `causes[]`
 *  shape `lifecycle.js`'s `fieldErrors` already reads. The MESSAGE IS PASSED
 *  ON VERBATIM: this client never rewords a decision another component made. */
export function causesFrom(plural, errors) {
  const out = [];
  for (const entry of Array.isArray(errors) ? errors : []) {
    const e = entry || {};
    out.push({
      field: canonicalFieldPath(plural, typeof e.field === "string" ? e.field : ""),
      message: typeof e.message === "string" ? e.message : "",
      reason: typeof e.code === "string" ? e.code : "invalid",
    });
  }
  return out;
}

// ===========================================================================
// the checks
// ===========================================================================

function fail(errors, field, code, message) {
  errors.push({ field: field, code: code, message: message });
}

function nameRef(errors, value, path, what) {
  const name = ((value || {}).name);
  if (!isObjectName(name)) {
    fail(errors, path + ".name", "invalid_name", what);
  }
}

function nonEmptyList(errors, value, path, what) {
  if (!Array.isArray(value) || value.length === 0) {
    fail(errors, path, "required", what);
    return false;
  }
  return true;
}

function text(errors, value, path, what) {
  if (typeof value !== "string" || value.length === 0) {
    fail(errors, path, "required", what);
  }
}

// THE TWO CLOSED SETS THIS MODULE ENFORCES, imported rather than copied.
// `ui/contract.js` holds them against the published schema, so a set that
// moved on the server fails a test here instead of refusing an operator's
// input for a reason nobody wrote down.
const AUTH_MODES = CONNECTION_AUTH_MODES;
const TARGET_MODES = RESTORE_MODES;

/** The checks for one kind, over the custom-resource-shaped body both modes
 *  build. Returns `FieldError[]` in the product API's own shape, with field
 *  paths in the canonical vocabulary. An empty array is "nothing this client
 *  can see is wrong", never "this will be accepted". */
export function validateRequest(plural, body) {
  const errors = [];
  const b = body || {};
  const meta = b.metadata || {};
  const spec = b.spec || {};
  if (!isObjectName(meta.name)) {
    fail(
      errors,
      "metadata.name",
      "invalid_name",
      "a name is lowercase letters, digits, '-' and '.', starting and ending with a letter or digit",
    );
  }
  if (plural === "kafkaclusters") {
    if (nonEmptyList(errors, spec.bootstrapServers, "spec.bootstrapServers",
      "name at least one bootstrap server, as host:port")) {
      spec.bootstrapServers.forEach((server, i) => {
        if (!isHostPort(server)) {
          fail(errors, "spec.bootstrapServers[" + String(i) + "]", "invalid_port",
            "must be host:port with no scheme and a port between 1 and 65535");
        }
      });
    }
    const auth = spec.auth || {};
    if (AUTH_MODES.indexOf(auth.mode) === -1) {
      fail(errors, "spec.auth.mode", "invalid_enum", "auth mode is one of " + AUTH_MODES.join(", "));
    }
    if (auth.mode === "scramSha512") {
      text(errors, auth.username, "spec.auth.username", "scramSha512 needs the SASL username");
      nameRef(errors, auth.secretRef, "spec.auth.secretRef",
        "scramSha512 needs the name of the Secret that holds the password");
    } else if (auth.secretRef !== undefined && auth.secretRef !== null) {
      nameRef(errors, auth.secretRef, "spec.auth.secretRef",
        "a Secret name is lowercase letters, digits, '-' and '.'");
    }
  } else if (plural === "backupschedules") {
    text(errors, spec.schedule, "spec.schedule", "a schedule expression is required");
    nameRef(errors, spec.sourceRef, "spec.sourceRef", "name the KafkaCluster this schedule reads");
    nonEmptyList(errors, spec.topics, "spec.topics", "name at least one topic");
    text(errors, (spec.archive || {}).url, "spec.archive.url", "the archive location is required");
  } else if (plural === "restores") {
    text(errors, spec.planBytes, "spec.planBytes", "the plan document is required");
    nameRef(errors, spec.approvalRef, "spec.approvalRef", "the Approval this Restore waits for");
    text(errors, spec.backupSetRef, "spec.backupSetRef", "the backup set to restore from");
    if (!isInstant(spec.pointInTime)) {
      fail(errors, "spec.pointInTime", "invalid_instant",
        "an RFC 3339 instant, such as 2026-09-07T14:05:00Z");
    }
    text(errors, (spec.sourceArchive || {}).url, "spec.sourceArchive.url",
      "the archive location is required");
    const target = spec.target || {};
    nameRef(errors, target.clusterRef, "spec.target.clusterRef",
      "name the KafkaCluster the restore writes to");
    if (TARGET_MODES.indexOf(target.mode) === -1) {
      fail(errors, "spec.target.mode", "invalid_enum",
        "target mode is one of " + TARGET_MODES.join(", "));
    }
    text(errors, (target.topicNaming || {}).prefix, "spec.target.topicNaming.prefix",
      "the prefix every restored topic's name starts with");
  } else if (plural === "approvals") {
    const subject = spec.subjectRef || {};
    text(errors, subject.kind, "spec.subjectRef.kind", "the kind this approval is about");
    nameRef(errors, subject, "spec.subjectRef", "the object this approval is about");
    if (!isPlanHash(spec.planHash)) {
      fail(errors, "spec.planHash", "invalid_hash",
        "a plan hash is sha256: followed by sixty-four lowercase hex digits");
    }
    text(errors, spec.approvalBytes, "spec.approvalBytes", "the approval document is required");
    text(errors, spec.sidecarBytes, "spec.sidecarBytes", "the signature sidecar is required");
  }
  return errors;
}

/** True when this client's own checks found nothing. */
export function accepts(plural, body) {
  return validateRequest(plural, body).length === 0;
}
