// pages/clusters.js -- the KafkaCluster list, one cluster's detail, and the
// create form.
//
// THE RENDER HALF IS PURE. Every `render*` function below is a function from a
// JSON object -- one the API server could have returned, which is what the
// checked-in fixtures are -- to an HTML string. No DOM, no network, no clock.
// `ui/tests/pages.spec.js` asserts the rules on those strings and
// `scripts/check-ui-behaviour.sh` runs it on every `just lint`.
//
// THE MOUNT HALF IS GLUE. `mountClusters` reads through `api.js` -- the only
// module in this tree that issues a network request -- and hands the string to
// the shell, which parses it. It holds no rule of its own.
//
// WHAT THE `AUTH` COLUMN MAY SAY. The mode, the username, and the NAME of the
// Secret that carries the credential. A Secret's name is not a secret; its
// value never appears in a status field, a log line, a rendered document or
// this page, and `no_secret_value_is_rendered` asserts the rendered output for
// a SCRAM cluster names the Secret and carries no credential word at all.
//
// THE CREATE FORM KEEPS WHAT WAS TYPED (PLAT-13.2). Its values are an
// in-memory draft (`../lifecycle.js`), so a refused field, an API refusal, a
// lost response or a trip to another route leaves them in place. The create is
// idempotent by name: submitting the same draft again after an unknown outcome
// resolves to the object the first request made, and a different object under
// that name is reported as a conflict and never touched.

import { list, get, create } from "../api.js";
import {
  active,
  cancelled,
  createOnce,
  dropDraft,
  fieldErrors,
  formKey,
  invalidInput,
  keepDraft,
  listen,
  mutationFor,
  readDraft,
  readOptions,
  watchMutation,
} from "../lifecycle.js";
import {
  badge,
  cell,
  detailLink,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  listFooter,
  mutationStatus,
  replace,
  table,
} from "../render.js";

const PLURAL = "kafkaclusters";

const API = { list: list, get: get, create: create };

/** The form's identity in the draft and mutation registries. */
export const CLUSTER_FORM = "cluster-form";

/** The fields a draft of this form keeps. All of them are public connection
 *  settings or the NAME of a Secret; the form has no field a credential could
 *  be typed into, and a field added later is not kept unless it is named here. */
export const CLUSTER_DRAFT_FIELDS = Object.freeze([
  "name", "servers", "role", "mode", "username", "secret", "tls",
]);

/** The API server's field paths, mapped to this form's inputs, so a 422's
 *  `causes[]` lands beside the field it is about. */
export const CLUSTER_FIELD_PATHS = Object.freeze([
  ["metadata.name", "name"],
  ["spec.bootstrapServers", "servers"],
  ["spec.role", "role"],
  ["spec.auth.mode", "mode"],
  ["spec.auth.username", "username"],
  ["spec.auth.secretRef", "secret"],
  ["spec.auth.tls", "tls"],
]);

/** What the API server fills in when a create omits it: the CRD defaults
 *  `auth.tls` to false. Nothing else of a KafkaCluster's spec may change. */
const CLUSTER_SPEC_RULES = Object.freeze({ defaults: { "auth.tls": false } });

/** The objects a renderer was handed, whatever shape they arrived in: a
 *  `KafkaClusterList` from the API server, a bare array, or one object (which
 *  is what a checked-in fixture is). */
export function itemsOf(input) {
  if (input === null || input === undefined) {
    return [];
  }
  if (Array.isArray(input)) {
    return input;
  }
  if (Array.isArray(input.items)) {
    return input.items;
  }
  return [input];
}

/** A `metadata.name`, or the absent marker. */
function nameOf(object) {
  const meta = (object && object.metadata) || {};
  return cell(meta.name);
}

/** The name as a link to this object's detail view, when it has a name. */
function nameCell(object, ns) {
  const meta = (object && object.metadata) || {};
  if (typeof meta.name !== "string" || meta.name.length === 0) {
    return cell(null);
  }
  return detailLink("clusters", ns || (meta.namespace || "default"), meta.name);
}

/** `spec.auth` as one cell: the mode, the username, and the Secret's NAME.
 *  Never a credential value -- there is none on this object to render, in any
 *  mode, by construction (`crates/weirkeeper/src/crds/kafka_cluster.rs`). */
export function authCell(spec) {
  const auth = (spec && spec.auth) || {};
  const parts = [cell(auth.mode)];
  if (typeof auth.username === "string" && auth.username.length > 0) {
    parts.push("as " + esc(auth.username));
  }
  const ref = auth.secretRef || {};
  if (typeof ref.name === "string" && ref.name.length > 0) {
    parts.push("via Secret " + esc(ref.name));
  }
  parts.push(auth.tls === true ? "TLS" : "no TLS");
  return parts.join(" ");
}

/** `status.reachable` as a badge whose WORDS carry the state as well as its
 *  colour. An unset field is the absent marker, not a claim either way: the
 *  controller has not probed this cluster yet. */
export function reachableBadge(status) {
  const reachable = (status || {}).reachable;
  if (reachable === true) {
    return badge("ok", "reachable");
  }
  if (reachable === false) {
    return badge("flat", "not reachable");
  }
  return cell(null);
}

/** The sentence the clusters table carries when the namespace holds none. */
export const NO_CLUSTER_SENTENCE =
  "No KafkaCluster in this namespace yet. Create one with the form below, or pick another " +
  "namespace above.";

/** The clusters table. NAME, ROLE, REACHABLE, CLUSTER-ID, OBSERVED, AUTH. */
export function renderClusterList(input, ns) {
  const rows = itemsOf(input).map((object) => {
    const spec = object.spec || {};
    const status = object.status || {};
    return [
      nameCell(object, ns),
      cell(spec.role),
      reachableBadge(status),
      cell(status.clusterId),
      cell(status.observedAt),
      authCell(spec),
    ];
  });
  return (
    "<h2>Clusters</h2>" +
    "<p class=\"blurb\">Every KafkaCluster in this namespace. " +
    "<code>clusterId</code> is read from the broker and never from a spec.</p>" +
    table(["NAME", "ROLE", "REACHABLE", "CLUSTER-ID", "OBSERVED", "AUTH"], rows, NO_CLUSTER_SENTENCE) +
    listFooter()
  );
}

/** One cluster, in full. */
export function renderClusterDetail(object) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const servers = Array.isArray(spec.bootstrapServers) ? spec.bootstrapServers : [];
  return (
    "<h2>Cluster " + nameOf(object) + "</h2>" +
    reachableBadge(status) +
    facts([
      ["role", cell(spec.role)],
      ["bootstrap servers", servers.length === 0 ? cell(null) : esc(servers.join(", "))],
      ["marker topic", cell(spec.markerTopic)],
      ["auth", authCell(spec)],
      ["cluster id", cell(status.clusterId)],
      ["observed at", cell(status.observedAt)],
      ["reason", cell(status.reason)],
    ])
  );
}

/** The values an empty form starts from. */
const CLUSTER_DEFAULTS = Object.freeze({
  name: "", servers: "", role: "source", mode: "plaintext", username: "", secret: "", tls: false,
});

/** A lowercase RFC 1123 subdomain: what Kubernetes accepts as an object name,
 *  and what a Secret's name must be. */
const DNS_SUBDOMAIN = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/;

/** True when `value` is a name the API server will accept for an object. */
export function isObjectName(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 253 && DNS_SUBDOMAIN.test(value);
}

const HOST_PORT = /^(\[[0-9A-Fa-f:.]+\]|[^\s:[\]]+):([0-9]{1,5})$/;

/** The page's own checks, by field: the message for each field that is not
 *  acceptable, and nothing for a field that is. A CONVENIENCE -- the API
 *  server's schema and the controller's probe are the gate -- that turns a
 *  round trip and a generic 422 into a message beside the field. */
export function validateCluster(values) {
  const v = values || {};
  const problems = {};
  if (!isObjectName(v.name)) {
    problems.name = "a KafkaCluster name is lowercase letters, digits, '-' and '.', starting and " +
      "ending with a letter or digit";
  }
  const servers = String(v.servers || "").split(",").map((s) => s.trim()).filter((s) => s.length > 0);
  if (servers.length === 0) {
    problems.servers = "name at least one bootstrap server, as host:port";
  } else {
    const bad = servers.filter((s) => {
      const match = HOST_PORT.exec(s);
      return match === null || Number(match[2]) < 1 || Number(match[2]) > 65535;
    });
    if (bad.length > 0) {
      problems.servers = "not host:port: " + bad.join(", ");
    }
  }
  if (typeof v.role !== "string" || v.role.trim().length === 0) {
    problems.role = "a role is required; source or target is what the controller reports";
  }
  if (v.mode !== "plaintext" && v.mode !== "scramSha512") {
    problems.mode = "auth mode is plaintext or scramSha512";
  }
  if (v.mode === "scramSha512" && (typeof v.username !== "string" || v.username.length === 0)) {
    problems.username = "scramSha512 needs the SASL username";
  }
  if (v.mode === "scramSha512" && (typeof v.secret !== "string" || v.secret.length === 0)) {
    problems.secret = "scramSha512 needs the name of the Secret that holds the password";
  } else if (typeof v.secret === "string" && v.secret.length > 0 && !isObjectName(v.secret)) {
    problems.secret = "a Secret name is lowercase letters, digits, '-' and '.'";
  }
  return problems;
}

/** The create form, rendered from a draft, the field messages and the form's
 *  mutation record. Called with nothing it is the empty form. */
export function renderClusterForm(view) {
  const v = view || {};
  const d = Object.assign({}, CLUSTER_DEFAULTS, v.draft || {});
  const errors = ((v.errors || {}).fields) || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const field = (id, name) => invalidAttributes(id, errors[name]);
  const line = (id, name) => fieldErrorLine(id, errors[name]);
  return (
    "<section class=\"create\" id=\"cluster-create\"><h3>Create a KafkaCluster</h3>" +
    "<p class=\"note\">A KafkaCluster names a set of brokers and how to reach them. The " +
    "controller probes it and records the cluster id it reads from the broker.</p>" +
    "<form id=\"cluster-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    "<div class=\"field\"><label for=\"cluster-name\">name</label>" +
    "<input id=\"cluster-name\" name=\"name\" required value=\"" + esc(d.name) + "\"" +
    field("cluster-name", "name") + ">" +
    "<p class=\"help\">A Kubernetes object name: lowercase, digits and dashes.</p>" +
    line("cluster-name", "name") + "</div>" +
    "<div class=\"field\"><label for=\"cluster-servers\">bootstrap servers, comma separated</label>" +
    "<input id=\"cluster-servers\" name=\"servers\" required value=\"" + esc(d.servers) + "\"" +
    field("cluster-servers", "servers") + ">" +
    "<p class=\"help\">host:port pairs, as a client would dial them from inside the cluster.</p>" +
    line("cluster-servers", "servers") + "</div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-role\">role</label>" +
    "<input id=\"cluster-role\" name=\"role\" required value=\"" + esc(d.role) + "\"" +
    field("cluster-role", "role") + ">" +
    "<p class=\"help\">A label the controller reports: source or target. It authorises nothing.</p>" +
    line("cluster-role", "role") + "</div>" +
    "<div class=\"field\"><label for=\"cluster-mode\">auth mode</label>" +
    "<select id=\"cluster-mode\" name=\"mode\"" + field("cluster-mode", "mode") + ">" +
    "<option value=\"plaintext\"" + (d.mode === "plaintext" ? " selected" : "") + ">plaintext</option>" +
    "<option value=\"scramSha512\"" + (d.mode === "scramSha512" ? " selected" : "") + ">scramSha512</option>" +
    "</select>" + line("cluster-mode", "mode") + "</div>" +
    "</div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-username\">auth username</label>" +
    "<input id=\"cluster-username\" name=\"username\" value=\"" + esc(d.username) + "\"" +
    field("cluster-username", "username") + ">" + line("cluster-username", "username") + "</div>" +
    "<div class=\"field\"><label for=\"cluster-secret\">auth Secret name</label>" +
    "<input id=\"cluster-secret\" name=\"secret\" value=\"" + esc(d.secret) + "\"" +
    field("cluster-secret", "secret") + ">" +
    "<p class=\"help\">The NAME of the Secret holding the credential. The value is never read " +
    "by this page.</p>" + line("cluster-secret", "secret") + "</div>" +
    "</div>" +
    "<label class=\"inline\"><input id=\"cluster-tls\" name=\"tls\" type=\"checkbox\"" +
    (d.tls === true ? " checked" : "") + "> TLS</label>" +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Create</button></div>" +
    "</fieldset>" +
    "<div class=\"form-status\" id=\"cluster-form-status\" tabindex=\"-1\">" +
    mutationStatus(state, { kind: "KafkaCluster", name: d.name }, ((v.errors || {}).unmatched)) +
    "</div>" +
    "</form>" +
    "<p class=\"note\">The credential itself lives in the Secret named above and " +
    "is never read by this page, by a status field or by a rendered document. What you type " +
    "here is kept in this page's memory until the cluster exists -- through an error, a lost " +
    "response or a visit to another page -- and never written to browser storage; a reload " +
    "starts empty.</p>" +
    "</section>"
  );
}

/** The request body a filled-in form produces. Pure: it reads a plain object
 *  of field values, not the DOM. */
export function clusterBody(values) {
  const auth = { mode: values.mode || "plaintext", tls: values.tls === true };
  if (values.username) {
    auth.username = values.username;
  }
  if (values.secret) {
    auth.secretRef = { name: values.secret };
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "KafkaCluster",
    metadata: { name: values.name },
    spec: {
      bootstrapServers: String(values.servers || "")
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0),
      auth: auth,
      role: values.role || "source",
    },
  };
}

/** Checks the values, then creates the cluster idempotently by name. Throws an
 *  `invalid` error carrying the field messages, without a request, when the
 *  page's own checks refuse; see `createOnce` for what a retry resolves to. */
export async function submitCluster(ns, values, deps) {
  const problems = validateCluster(values);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  return createOnce(deps || API, ns, PLURAL, clusterBody(values), CLUSTER_SPEC_RULES);
}

/** What the form renders from in namespace `ns`: its draft, its record and the
 *  messages the record's failure carries. A record that already succeeded has
 *  consumed its draft, even if no view was current to see it happen. */
export function clusterFormView(ns) {
  const key = formKey(ns, CLUSTER_FORM);
  const state = mutationFor(key).state;
  if (state.phase === "succeeded") {
    dropDraft(key);
  }
  return {
    draft: readDraft(key),
    state: state,
    errors: state.phase === "failed" ? fieldErrors(state.error, CLUSTER_FIELD_PATHS) : null,
  };
}

// --------------------------------------------------------------- mount half

/** Reads the namespace's clusters and renders them. On any API error -- a 403
 *  from the viewer's own RBAC included -- the API server's OWN reason and
 *  message are rendered verbatim, because the page made no authorisation
 *  decision and must not narrate one. */
export async function mountClusters(node, ns, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    const collection = await api.list(ns, PLURAL, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    replace(
      node,
      parse(
        renderClusterList(collection, ns) +
          "<div class=\"form-slot\" id=\"cluster-form-slot\">" +
          renderClusterForm(clusterFormView(ns)) +
          "</div>",
      ),
    );
    wireForm(node, ns, parse, lifecycle, api);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** Reads one cluster and renders its detail. */
export async function mountClusterDetail(node, ns, name, parse, lifecycle) {
  try {
    const object = await get(ns, PLURAL, name, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    replace(node, parse(renderClusterDetail(object)));
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** The form's values, read from the DOM and trimmed where a name is involved. */
export function readClusterValues(form) {
  const e = form.elements;
  return {
    name: String(e.name.value).trim(),
    servers: String(e.servers.value),
    role: String(e.role.value).trim(),
    mode: String(e.mode.value),
    username: String(e.username.value).trim(),
    secret: String(e.secret.value).trim(),
    tls: e.tls.checked === true,
  };
}

function wireForm(node, ns, parse, lifecycle, api) {
  const form = node.querySelector("#cluster-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, CLUSTER_FORM);
  const mutation = mutationFor(key);

  // EVERY KEYSTROKE IS KEPT, so whatever happens to this view next -- an
  // answer that re-renders the form, or a route change -- the draft is intact.
  const remember = () => {
    if (!active(lifecycle)) {
      return;
    }
    keepDraft(key, readClusterValues(form), CLUSTER_DRAFT_FIELDS);
    if (mutation.state.phase === "succeeded") {
      mutation.clear();
      const status = node.querySelector("#cluster-form-status");
      if (status !== null) {
        replace(status, []);
      }
    }
  };
  listen(form, "input", remember, lifecycle);
  listen(form, "change", remember, lifecycle);

  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      dropDraft(key);
      mountClusters(node, ns, parse, lifecycle, api);
      return;
    }
    const slot = node.querySelector("#cluster-form-slot");
    if (slot === null) {
      return;
    }
    replace(slot, parse(renderClusterForm(clusterFormView(ns))));
    wireForm(node, ns, parse, lifecycle, api);
    if (state.phase === "failed") {
      focusFirstProblem(node, "#cluster-form-status");
    }
  }, lifecycle);

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readClusterValues(form);
    keepDraft(key, values, CLUSTER_DRAFT_FIELDS);
    mutation.run(() => submitCluster(ns, values, api));
  }, lifecycle);
}

/** Moves focus to the first field marked invalid, or to the status region, so
 *  a keyboard or screen-reader user lands on what needs doing. */
export function focusFirstProblem(node, statusSelector) {
  const target = node.querySelector("[aria-invalid=\"true\"]") || node.querySelector(statusSelector);
  if (target !== null && typeof target.focus === "function") {
    target.focus();
  }
}
