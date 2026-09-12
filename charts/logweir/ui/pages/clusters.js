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

import { list, get, create } from "../api.js";
import {
  badge,
  cell,
  detailLink,
  clear,
  errorBox,
  esc,
  facts,
  listFooter,
  replace,
  table,
} from "../render.js";

const PLURAL = "kafkaclusters";

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

/** The create form. It carries no value read from any cluster, so it is a
 *  constant string. */
export function renderClusterForm() {
  return (
    "<section class=\"create\"><h3>Create a KafkaCluster</h3>" +
    "<p class=\"note\">A KafkaCluster names a set of brokers and how to reach them. The " +
    "controller probes it and records the cluster id it reads from the broker.</p>" +
    "<form id=\"cluster-form\">" +
    "<div class=\"field\"><label for=\"cluster-name\">name</label>" +
    "<input id=\"cluster-name\" name=\"name\" required>" +
    "<p class=\"help\">A Kubernetes object name: lowercase, digits and dashes.</p></div>" +
    "<div class=\"field\"><label for=\"cluster-servers\">bootstrap servers, comma separated</label>" +
    "<input id=\"cluster-servers\" name=\"servers\" required>" +
    "<p class=\"help\">host:port pairs, as a client would dial them from inside the cluster.</p></div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-role\">role</label>" +
    "<input id=\"cluster-role\" name=\"role\" value=\"source\" required>" +
    "<p class=\"help\">A label the controller reports: source or target. It authorises nothing.</p></div>" +
    "<div class=\"field\"><label for=\"cluster-mode\">auth mode</label>" +
    "<select id=\"cluster-mode\" name=\"mode\">" +
    "<option value=\"plaintext\">plaintext</option>" +
    "<option value=\"scramSha512\">scramSha512</option>" +
    "</select></div>" +
    "</div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"cluster-username\">auth username</label>" +
    "<input id=\"cluster-username\" name=\"username\"></div>" +
    "<div class=\"field\"><label for=\"cluster-secret\">auth Secret name</label>" +
    "<input id=\"cluster-secret\" name=\"secret\">" +
    "<p class=\"help\">The NAME of the Secret holding the credential. The value is never read " +
    "by this page.</p></div>" +
    "</div>" +
    "<label class=\"inline\"><input id=\"cluster-tls\" name=\"tls\" type=\"checkbox\"> TLS</label>" +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Create</button></div>" +
    "</form>" +
    "<p class=\"note\">The credential itself lives in the Secret named above and " +
    "is never read by this page, by a status field or by a rendered document.</p>" +
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

// --------------------------------------------------------------- mount half

/** Reads the namespace's clusters and renders them. On any API error -- a 403
 *  from the viewer's own RBAC included -- the API server's OWN reason and
 *  message are rendered verbatim, because the page made no authorisation
 *  decision and must not narrate one. */
export async function mountClusters(node, ns, parse) {
  try {
    const collection = await list(ns, PLURAL);
    replace(node, parse(renderClusterList(collection, ns) + renderClusterForm()));
    wireForm(node, ns, parse);
  } catch (error) {
    replace(node, errorBox(error));
  }
}

/** Reads one cluster and renders its detail. */
export async function mountClusterDetail(node, ns, name, parse) {
  try {
    const object = await get(ns, PLURAL, name);
    replace(node, parse(renderClusterDetail(object)));
  } catch (error) {
    replace(node, errorBox(error));
  }
}

function wireForm(node, ns, parse) {
  const form = node.querySelector("#cluster-form");
  if (form === null) {
    return;
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const values = {
      name: form.elements.name.value.trim(),
      servers: form.elements.servers.value,
      role: form.elements.role.value.trim(),
      mode: form.elements.mode.value,
      username: form.elements.username.value.trim(),
      secret: form.elements.secret.value.trim(),
      tls: form.elements.tls.checked,
    };
    try {
      await create(ns, PLURAL, clusterBody(values));
      await mountClusters(node, ns, parse);
    } catch (error) {
      const slot = node.querySelector(".create");
      if (slot !== null) {
        clear(slot);
        slot.appendChild(errorBox(error));
      }
    }
  });
}
