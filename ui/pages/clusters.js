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
//
// THE FORM TAKES CONTRACT v1's REFERENCES AND NEVER A VALUE (PLAT-07.1,
// PLAT-07.2). `spec.auth.secretRef.passwordKey` names the data key inside the
// Secret that holds the SASL password -- absent means `password`, which is what
// every earlier release projected -- and `spec.auth.tlsCa` names exactly one
// key of a Secret or a ConfigMap holding the private CA that signs the brokers'
// certificates. Both are NAMES. There is no field on this form a password could
// be typed into, `CLUSTER_DRAFT_FIELDS` names none, and
// `the_cluster_form_has_no_password_input` asserts both against the rendered
// bytes so a field added later cannot quietly become one.
//
// AND `status.reachable` IS A CONNECTION PROBE, NEVER "READY" (PLAT-07.2, D2
// section 9). The badge, the stale label and the controller's own refusal
// reason all come from `../select.js`, which is also what the schedule form and
// the restore wizard select a saved connection with -- so the three surfaces
// agree about what an observation means and about what is too old to present as
// current.

import { apiClient } from "../client.js";
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
import {
  PROBE_SENTENCE,
  TEST_CONNECTION_SENTENCE,
  clusterUid,
  probeBadge,
  probeLine,
  probeState,
  renderProbePanel,
  renderTestConnection,
  staleBadge,
} from "../select.js";

const PLURAL = "kafkaclusters";

const API = apiClient();

/** The form's identity in the draft and mutation registries. */
export const CLUSTER_FORM = "cluster-form";

/** The fields a draft of this form keeps. All of them are public connection
 *  settings or the NAME of a Secret; the form has no field a credential could
 *  be typed into, and a field added later is not kept unless it is named here. */
export const CLUSTER_DRAFT_FIELDS = Object.freeze([
  "name", "servers", "role", "mode", "username", "secret", "passwordKey",
  "tls", "tlsCaKind", "tlsCaName", "tlsCaKey",
]);

/** The credential-shaped field names this form must NEVER have, held as data
 *  so `the_cluster_form_has_no_password_input` can assert the rendered bytes
 *  and `CLUSTER_DRAFT_FIELDS` against the same list rather than against a
 *  spelling inside one test. `passwordKey` is the NAME OF A DATA KEY and is
 *  deliberately not on it; a key's name is not a secret, and contract v1 needs
 *  it to project the right entry of the Secret. */
export const FORBIDDEN_CLUSTER_FIELDS = Object.freeze([
  "password", "secretValue", "credential", "passphrase", "token",
]);

/** The API server's field paths, mapped to this form's inputs, so a 422's
 *  `causes[]` lands beside the field it is about. */
export const CLUSTER_FIELD_PATHS = Object.freeze([
  ["metadata.name", "name"],
  ["spec.bootstrapServers", "servers"],
  ["spec.role", "role"],
  ["spec.auth.mode", "mode"],
  ["spec.auth.username", "username"],
  ["spec.auth.secretRef.passwordKey", "passwordKey"],
  ["spec.auth.secretRef", "secret"],
  ["spec.auth.tlsCa.secretKeyRef.key", "tlsCaKey"],
  ["spec.auth.tlsCa.configMapKeyRef.key", "tlsCaKey"],
  ["spec.auth.tlsCa.secretKeyRef.name", "tlsCaName"],
  ["spec.auth.tlsCa.configMapKeyRef.name", "tlsCaName"],
  ["spec.auth.tlsCa", "tlsCaName"],
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
    // CONTRACT v1's KEY, and only when it was set. An absent key means the
    // legacy entry and rendering a default here would claim the object says
    // something it does not. The key's NAME is not a secret; its value is
    // never read by this page, by a status field or by a rendered document.
    if (typeof ref.passwordKey === "string" && ref.passwordKey.length > 0) {
      parts.push("key " + esc(ref.passwordKey));
    }
  }
  parts.push(auth.tls === true ? "TLS" : "no TLS");
  parts.push(caWords(auth.tlsCa));
  return parts.filter((part) => part.length > 0).join(" ");
}

/** Contract v1's `spec.auth.tlsCa` as words: which kind of object holds the
 *  private CA, its name and its key. The empty string when none is named,
 *  which is a connection that trusts the runner image's own store. */
export function caWords(tlsCa) {
  const ca = tlsCa || {};
  const secret = ca.secretKeyRef || null;
  const configMap = ca.configMapKeyRef || null;
  const source = secret !== null ? secret : configMap;
  if (source === null) {
    return "";
  }
  return (
    "CA from " + (secret !== null ? "Secret " : "ConfigMap ") + cell(source.name) +
    " key " + cell(source.key)
  );
}

/** `status.reachable` AS A CONNECTION PROBE, in one badge whose words carry
 *  the state as well as its colour.
 *
 *  It takes a `status` rather than a whole object because that is what its
 *  callers had before PLAT-07.2 and what the design suite exercises it with;
 *  the judgement itself lives in `../select.js`, so this page, the schedule
 *  form and both wizard sides say the same words about the same field. An
 *  unset `reachable` with no reason at all is `never probed` -- not the absent
 *  marker and not a claim either way, because "the controller has not written
 *  anything yet" is itself worth saying. */
export function reachableBadge(status, now, freshSeconds) {
  return probeBadge(probeState({ status: status || {} }, now, freshSeconds));
}

/** The probe as a table cell: the verdict badge, the stale badge beside it
 *  when the observation is older than the freshness budget, and nothing else
 *  -- the reason and the observed instant have their own columns. */
export function probeCell(object, now, freshSeconds) {
  const state = probeState(object, now, freshSeconds);
  const stale = staleBadge(state);
  return probeBadge(state) + (stale.length > 0 ? " " + stale : "");
}

/** The sentence the clusters table carries when the namespace holds none. */
export const NO_CLUSTER_SENTENCE =
  "No KafkaCluster in this namespace yet. Create one with the form below, or pick another " +
  "namespace above.";

/** The clusters table. NAME, ROLE, CONNECTION PROBE, OBSERVED, CLUSTER-ID,
 *  AUTH, and one Test connection control per row.
 *
 *  THE PROBE COLUMN IS CALLED WHAT IT IS. It was headed REACHABLE and rendered
 *  a bare `reachable` badge with the observed instant two columns away, so a
 *  five-hour-old success and a five-second-old one looked identical at a
 *  glance. The column is now the probe's verdict, the stale label when the
 *  observation is past the freshness budget, and the controller's own reason
 *  beside it -- and the word `ready` appears nowhere, because nothing on this
 *  page has decided that anything is.
 *
 *  `now` is epoch milliseconds, defaulted to the caller's clock by
 *  `probeState`; a test passes one so a freshness verdict is reproducible. */
export function renderClusterList(input, ns, now, freshSeconds) {
  const rows = itemsOf(input).map((object) => {
    const spec = object.spec || {};
    const status = object.status || {};
    const state = probeState(object, now, freshSeconds);
    return [
      nameCell(object, ns),
      cell(spec.role),
      probeCell(object, now, freshSeconds),
      cell(status.observedAt),
      state.reason.length === 0 ? cell(null) : "<code>" + esc(state.reason) + "</code>",
      cell(status.clusterId),
      authCell(spec),
      renderTestConnection(object, false),
    ];
  });
  const attributes = itemsOf(input).map(
    (object) => "data-cluster-uid=\"" + esc(clusterUid(object)) + "\"",
  );
  return (
    "<h2>Clusters</h2>" +
    "<p class=\"blurb\">Every KafkaCluster in this namespace. " +
    "<code>clusterId</code> is read from the broker and never from a spec.</p>" +
    "<p class=\"note\">" + PROBE_SENTENCE + "</p>" +
    table(
      ["NAME", "ROLE", "CONNECTION PROBE", "OBSERVED", "REASON", "CLUSTER-ID", "AUTH", ""],
      rows,
      NO_CLUSTER_SENTENCE,
      attributes,
    ) +
    "<p class=\"note\">" + TEST_CONNECTION_SENTENCE + "</p>" +
    listFooter()
  );
}

/** One cluster, in full: its probe panel with the Test connection control, and
 *  then the saved connection contract v1 carries -- every reference by name,
 *  no value of anything. */
export function renderClusterDetail(object, now, freshSeconds, pending) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const servers = Array.isArray(spec.bootstrapServers) ? spec.bootstrapServers : [];
  const auth = spec.auth || {};
  const ref = auth.secretRef || {};
  const state = probeState(object, now, freshSeconds);
  return (
    "<h2>Cluster " + nameOf(object) + "</h2>" +
    renderProbePanel(object, { now: now, freshSeconds: freshSeconds, pending: pending === true }) +
    facts([
      ["role", cell(spec.role)],
      ["bootstrap servers", servers.length === 0 ? cell(null) : esc(servers.join(", "))],
      ["marker topic", cell(spec.markerTopic)],
      ["auth", authCell(spec)],
      ["credential Secret", cell(ref.name)],
      ["credential key", typeof ref.passwordKey === "string" && ref.passwordKey.length > 0
        ? esc(ref.passwordKey)
        : (typeof ref.name === "string" && ref.name.length > 0
          ? "- (absent means the key every earlier release projected)"
          : cell(null))],
      ["TLS", auth.tls === true ? "on" : "off"],
      ["private CA", caWords(auth.tlsCa).length === 0 ? cell(null) : caWords(auth.tlsCa)],
      ["cluster id", cell(status.clusterId)],
      ["probe observed at", cell(status.observedAt)],
      ["probe reason", cell(status.reason)],
      ["probe freshness", esc(String(state.freshSeconds)) + "s budget"],
      ["uid", "<code id=\"cluster-uid\">" + esc(clusterUid(object)) + "</code>"],
    ])
  );
}

/** The values an empty form starts from. */
const CLUSTER_DEFAULTS = Object.freeze({
  name: "", servers: "", role: "source", mode: "plaintext", username: "", secret: "",
  passwordKey: "", tls: false, tlsCaKind: "none", tlsCaName: "", tlsCaKey: "",
});

/** The three answers the private-CA control takes. `none` is the default and
 *  means the runner image's own trust store, which is what every connection to
 *  a publicly signed broker uses. */
export const TLS_CA_KINDS = Object.freeze(["none", "secret", "configMap"]);

/** A Kubernetes data key: the `^[-._a-zA-Z0-9]+$` the CRD spells for
 *  `secretRef.passwordKey` and for both halves of `tlsCa`. */
const DATA_KEY = /^[-._a-zA-Z0-9]+$/;

/** True when `value` is a data key the API server will accept. */
export function isDataKey(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 253 && DATA_KEY.test(value);
}

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
  const problems = Object.create(null);
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
  // CONTRACT v1, THE SAME FOUR RULES `weirkeeper::connection::resolve` AND THE
  // CRD's CEL APPLY. A CONVENIENCE and never the gate: the API server refuses
  // three of these at admission and the resolver refuses the fourth before a
  // Job exists. Saying so here turns a round trip into a message beside the
  // field, and each message names the rule rather than restating the value.
  const key = String(v.passwordKey || "").trim();
  if (key.length > 0 && !isDataKey(key)) {
    problems.passwordKey = "a Secret data key is letters, digits, '-', '_' and '.'";
  }
  if (key.length > 0 && (typeof v.secret !== "string" || v.secret.length === 0)) {
    problems.passwordKey = "a data key belongs to a Secret; name the Secret first, or leave " +
      "this blank for the key every earlier release projected";
  }
  if (v.mode === "plaintext" && v.tls === true) {
    problems.tls = "auth mode plaintext with TLS on -- TLS without SASL -- is not supported by " +
      "saved-connection contract v1 and is refused rather than dialled without TLS. Choose " +
      "scramSha512 over TLS, or leave TLS off for a plaintext listener";
  }
  const kind = TLS_CA_KINDS.indexOf(v.tlsCaKind) === -1 ? "none" : v.tlsCaKind;
  if (kind !== "none") {
    if (v.tls !== true) {
      problems.tlsCaName = "a CA verifies a TLS transport, so naming one requires TLS on";
    }
    if (!isObjectName(String(v.tlsCaName || "").trim())) {
      problems.tlsCaName = "the name of the " + (kind === "secret" ? "Secret" : "ConfigMap") +
        " in this namespace that holds the PEM CA certificate(s)";
    }
    if (!isDataKey(String(v.tlsCaKey || "").trim())) {
      problems.tlsCaKey = "the data key inside it, letters, digits, '-', '_' and '.'";
    }
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
    "<div class=\"field\"><label for=\"cluster-password-key\">data key in that Secret " +
    "(spec.auth.secretRef.passwordKey)</label>" +
    "<input id=\"cluster-password-key\" name=\"passwordKey\" value=\"" + esc(d.passwordKey) +
    "\"" + field("cluster-password-key", "passwordKey") + ">" +
    "<p class=\"help\">Which entry of that Secret the controller projects. Leave it blank for " +
    "the entry every earlier release used, which is what a KafkaCluster created before " +
    "connection contract v1 means. This is the KEY's name, not its value: nothing on this page " +
    "reads the Secret.</p>" + line("cluster-password-key", "passwordKey") + "</div>" +
    "<label class=\"inline\"><input id=\"cluster-tls\" name=\"tls\" type=\"checkbox\"" +
    (d.tls === true ? " checked" : "") + "> TLS</label>" +
    line("cluster-tls", "tls") +
    "<p class=\"help\">The only switch that turns TLS on, and it is independent of the auth " +
    "mode: contract v1 supports scramSha512 over TLS (SASL_SSL). plaintext with TLS on is " +
    "refused rather than dialled in the clear.</p>" +
    "<fieldset class=\"ca\"><legend>private certificate authority " +
    "(spec.auth.tlsCa)</legend>" +
    "<p class=\"help\">Only when the brokers' certificates are signed by an authority the " +
    "runner image does not already trust. Exactly one key of a Secret or a ConfigMap in this " +
    "namespace, holding PEM certificate(s); a CA certificate is public, so a ConfigMap is an " +
    "ordinary home for it. It replaces the default trust store for this connection and requires " +
    "TLS on.</p>" +
    "<div class=\"field\"><label for=\"cluster-tls-ca-kind\">CA source</label>" +
    "<select id=\"cluster-tls-ca-kind\" name=\"tlsCaKind\">" +
    TLS_CA_KINDS.map(
      (kind) =>
        "<option value=\"" + esc(kind) + "\"" + (d.tlsCaKind === kind ? " selected" : "") + ">" +
        esc(kind === "none" ? "none (trust the runner image's own store)" : kind) + "</option>",
    ).join("") +
    "</select></div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-tls-ca-name\">CA object name</label>" +
    "<input id=\"cluster-tls-ca-name\" name=\"tlsCaName\" value=\"" + esc(d.tlsCaName) + "\"" +
    field("cluster-tls-ca-name", "tlsCaName") + ">" +
    line("cluster-tls-ca-name", "tlsCaName") + "</div>" +
    "<div class=\"field\"><label for=\"cluster-tls-ca-key\">CA data key</label>" +
    "<input id=\"cluster-tls-ca-key\" name=\"tlsCaKey\" value=\"" + esc(d.tlsCaKey) + "\"" +
    field("cluster-tls-ca-key", "tlsCaKey") + ">" +
    line("cluster-tls-ca-key", "tlsCaKey") + "</div>" +
    "</div></fieldset>" +
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
    // ABSENT IS A MEANING, so a blank key is left out rather than sent as the
    // default: an object that omits `passwordKey` is byte-for-byte what every
    // release before contract v1 wrote, and one that spells the default is a
    // different object with the same behaviour. The difference matters because
    // `spec` is immutable and the frozen execution inputs record what is here.
    const key = String(values.passwordKey || "").trim();
    if (key.length > 0) {
      auth.secretRef.passwordKey = key;
    }
  }
  const kind = TLS_CA_KINDS.indexOf(values.tlsCaKind) === -1 ? "none" : values.tlsCaKind;
  if (kind !== "none") {
    const reference = {
      name: String(values.tlsCaName || "").trim(),
      key: String(values.tlsCaKey || "").trim(),
    };
    auth.tlsCa = kind === "secret"
      ? { secretKeyRef: reference }
      : { configMapKeyRef: reference };
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
    wireProbeTests(node, ns, parse, lifecycle, api);
    wireForm(node, ns, parse, lifecycle, api);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** Reads one cluster and renders its detail, with the Test connection control
 *  wired to a re-read of the same object. */
export async function mountClusterDetail(node, ns, name, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    const object = await api.get(ns, PLURAL, name, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    replace(node, parse(renderClusterDetail(object)));
    wireDetailProbe(node, ns, name, parse, lifecycle, api);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** THE "TEST CONNECTION" CONTROL, ON THE DETAIL VIEW.
 *
 *  It is a READ, and the panel says so: the page re-reads this KafkaCluster and
 *  renders whatever the controller has recorded since. It cannot make the
 *  controller dial -- `spec` is immutable, this page's whole write surface is
 *  five creates and one suspend patch, and the re-probe cadence is the probe
 *  Job's own TTL -- so a control that claimed to force a dial would be lying in
 *  the same way a stale probe rendered as current lies.
 *
 *  IT IS BOUND TO THE ROUTE'S LIFETIME LIKE EVERY OTHER READ (PLAT-13.1): the
 *  answer is dropped when the view is gone, so a slow re-read cannot paint a
 *  probe from namespace A over namespace B. */
function wireDetailProbe(node, ns, name, parse, lifecycle, api) {
  const form = node.querySelector("form.probe-test");
  if (form === null) {
    return;
  }
  let reading = false;
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || reading) {
      return;
    }
    reading = true;
    const button = form.querySelector("button");
    if (button !== null) {
      button.disabled = true;
    }
    api.get(ns, PLURAL, name, readOptions(lifecycle)).then(
      (object) => {
        reading = false;
        if (!active(lifecycle)) {
          return;
        }
        replace(node, parse(renderClusterDetail(object)));
        wireDetailProbe(node, ns, name, parse, lifecycle, api);
      },
      (error) => {
        reading = false;
        if (cancelled(error, lifecycle) || !active(lifecycle)) {
          return;
        }
        const panel = node.querySelector("#cluster-probe-line");
        if (panel !== null) {
          replace(panel, parse(renderProbeReadFailure(error)));
        }
        if (button !== null) {
          button.disabled = false;
        }
      },
    );
  }, lifecycle);
}

/** What a failed re-read says. THE API SERVER'S OWN reason and message,
 *  verbatim, exactly as `errorBox` renders them elsewhere: the page made no
 *  authorisation decision and must not narrate one, and a re-read that was
 *  refused is not an observation about the broker. */
export function renderProbeReadFailure(error) {
  const e = error || {};
  const reason = typeof e.reason === "string" && e.reason.length > 0 ? e.reason : "";
  const message = typeof e.message === "string" ? e.message : String(e);
  return (
    "<span class=\"refusal\">Test connection could not re-read this KafkaCluster, so the " +
    "observation above is unchanged and is not a statement about right now" +
    (reason.length === 0 ? "" : " (" + esc(reason) + ")") + ": " + esc(message) + "</span>"
  );
}

/** The per-row Test connection controls on the LIST view. Each one re-reads
 *  its own cluster by name and replaces THAT row's probe cells; the rest of the
 *  page -- the create form's draft included -- is untouched, which is why this
 *  is not a re-mount. */
function wireProbeTests(node, ns, parse, lifecycle, api) {
  for (const form of node.querySelectorAll("form.probe-test")) {
    wireRowProbe(node, ns, parse, lifecycle, api, form);
  }
}

function wireRowProbe(node, ns, parse, lifecycle, api, form) {
  const name = form.getAttribute("data-probe-name");
  const uid = form.getAttribute("data-probe-uid");
  let reading = false;
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || reading || typeof name !== "string" || name.length === 0) {
      return;
    }
    reading = true;
    const button = form.querySelector("button");
    if (button !== null) {
      button.disabled = true;
    }
    api.get(ns, PLURAL, name, readOptions(lifecycle)).then(
      (object) => {
        reading = false;
        if (button !== null) {
          button.disabled = false;
        }
        if (!active(lifecycle)) {
          return;
        }
        // THE UID IS CHECKED BEFORE ANYTHING IS PAINTED. A cluster deleted and
        // recreated under the same name between the list read and this one is
        // a DIFFERENT connection, and writing its probe into the old row would
        // be the exact substitution this task forbids everywhere else.
        if (clusterUid(object) !== uid) {
          paintRow(node, parse, uid, renderRecreatedRow(name, uid, clusterUid(object)));
          return;
        }
        paintRow(node, parse, uid, probeLine(probeState(object)));
      },
      (error) => {
        reading = false;
        if (button !== null) {
          button.disabled = false;
        }
        if (cancelled(error, lifecycle) || !active(lifecycle)) {
          return;
        }
        paintRow(node, parse, uid, renderProbeReadFailure(error));
      },
    );
  }, lifecycle);
}

/** The sentence a row gets when the name it re-read answers to a different
 *  object than the one the row is about. */
export function renderRecreatedRow(name, was, now) {
  return (
    "<span class=\"refusal\">The KafkaCluster named <code>" + esc(name) + "</code> is no longer " +
    "the object this row is about: this row is uid <code>" + esc(was) + "</code> and that name " +
    "now answers to uid <code>" + esc(now) + "</code>. Nothing was painted over: reload the " +
    "list to see what this namespace holds.</span>"
  );
}

/** Replaces the probe cell of the row carrying `uid`. Finds the row by its
 *  recorded identity rather than by counting, so a list that changed under the
 *  read cannot be edited in the wrong place. */
function paintRow(node, parse, uid, html) {
  for (const row of node.querySelectorAll("tr[data-cluster-uid]")) {
    if (row.getAttribute("data-cluster-uid") !== uid) {
      continue;
    }
    const cells = row.querySelectorAll("td");
    if (cells.length > 2) {
      replace(cells[2], parse(html));
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
    passwordKey: String(e.passwordKey.value).trim(),
    tls: e.tls.checked === true,
    tlsCaKind: String(e.tlsCaKind.value),
    tlsCaName: String(e.tlsCaName.value).trim(),
    tlsCaKey: String(e.tlsCaKey.value).trim(),
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
