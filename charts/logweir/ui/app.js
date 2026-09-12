// app.js -- the shell: a hash router and the frame around it.
//
// A HASH ROUTER, ON PURPOSE. Every route lives after the `#`, so the browser
// never asks the server for a path other than `/ui/` and its assets. That
// matters because the static half of `kubectl proxy` is Go's
// `http.StripPrefix(prefix, http.FileServer(http.Dir(base)))`: a plain file
// server with no rewrite rule, no custom 404 hook, and no way to be told that
// `/ui/backups` should return `index.html`. A path-based router would need one.
// This one needs nothing, which is why there is no 404 page in this tree: no
// route can produce a path-level 404.
//
// THIS MODULE MAKES NO NETWORK REQUEST. `api.js` is the only module that does.
// The shell shows the shape of the application and the API prefix it will
// address; the seven views arrive with their own tests.

import { GROUP, VERSION, path } from "./api.js";
import { el, replace } from "./render.js";
import { mountClusterDetail, mountClusters } from "./pages/clusters.js";
import { mountSchedules } from "./pages/schedules.js";
import { mountBackupDetail, mountBackups } from "./pages/backups.js";
import { mountHistory, mountRestoreDetail } from "./pages/history.js";
import { mountRestoreWizard } from "./pages/restore-wizard.js";
import { approvalRouteParams, mountApprovals } from "./pages/approvals.js";
import { mountKeys } from "./pages/keys.js";

// The seven routes, in navigation order. The hash is the whole route.
const ROUTES = [
  { hash: "#/clusters", title: "Clusters", blurb: "The KafkaCluster objects this namespace can reach.", mount: mountClusters, detail: mountClusterDetail },
  { hash: "#/schedules", title: "Schedules", blurb: "BackupSchedule objects, their next slot and their suspend state.", mount: mountSchedules },
  { hash: "#/backups", title: "Backups", blurb: "Backup runs, each with the evidence weirkeeper recorded for it.", mount: mountBackups, detail: mountBackupDetail },
  { hash: "#/history", title: "History", blurb: "Completed runs over time, newest first.", mount: mountHistory, detail: mountRestoreDetail },
  { hash: "#/restore", title: "Restore", blurb: "The restore wizard: archive, set, point in time, target, preflight, plan.", mount: mountRestoreWizard },
  { hash: "#/approvals", title: "Approvals", blurb: "Approval objects, and which key signed each one.", mount: mountApprovals, route: true },
  { hash: "#/keys", title: "Keys", blurb: "The TrustRoster, read-only: it is cluster-scoped and admin-only.", mount: mountKeys, cluster: true },
];

const DEFAULT_HASH = ROUTES[0].hash;

// The namespace every read and every write is scoped to. It travels IN THE
// HASH -- `#/backups?ns=logweir-t26` -- and nowhere else. Not in browser
// storage, not in a cookie, not in a header: the page stores nothing, which is
// rule 2 of `scripts/check-ui-offline.sh` and is asserted over every file under
// `ui/` by `no_credential_appears_in_the_page`. A hash is also the only place
// that survives a reload without the file server being asked for a path it
// cannot serve.
const DEFAULT_NAMESPACE = "default";

/** The `#/route?ns=name` a hash carries: the route half, the namespace, and
 *  every other parameter the hash names.
 *
 *  `params` exists for the approvals route, which the restore wizard navigates
 *  to as `#/approvals?subject=<restore>&hash=<planHash>&name=<approval>`. Those
 *  three values are where `Approval.spec.subjectRef` and `planHash` come from:
 *  the page is forbidden from parsing the two approval documents, so the hash
 *  cannot be lifted out of `approval.json` either, and the create form reads
 *  the ROUTE and never the bytes.
 *
 *  THAT HAND-OFF IS NOT MADE HERE. `pages/approvals.js` exports
 *  `approvalRouteParams(hash)` and the approvals branch below calls it, because
 *  this module cannot be imported under `node --test` -- it touches `window` at
 *  module scope -- and the three values above are worth a test that can
 *  actually run. This function keeps `params` as the generic bag every route
 *  can read. */
export function parseHash(hash) {
  const text = typeof hash === "string" ? hash : "";
  const question = text.indexOf("?");
  const route = question === -1 ? text : text.slice(0, question);
  let ns = DEFAULT_NAMESPACE;
  let name = "";
  const params = {};
  if (question !== -1) {
    for (const pair of text.slice(question + 1).split("&")) {
      const equals = pair.indexOf("=");
      if (equals === -1) {
        continue;
      }
      const key = pair.slice(0, equals);
      const value = decodeURIComponent(pair.slice(equals + 1)).trim();
      params[key] = value;
      if (key === "ns" && value.length > 0) {
        ns = value;
      }
      if (key === "name" && value.length > 0) {
        name = value;
      }
    }
  }
  return { route: route, ns: ns, name: name, params: params };
}

function routeFor(hash) {
  for (const route of ROUTES) {
    if (route.hash === hash) {
      return route;
    }
  }
  return null;
}

// PARSING A PAGE'S STRING, WITHOUT `innerHTML`.
//
// The page modules are pure functions from a JSON object to an HTML string --
// that is what makes their rules assertable under `node --test`, which has no
// DOM, in a tree with no bundler and no DOM shim. Turning that string into
// nodes is done by `DOMParser`, which parses markup WITHOUT executing any
// script content it finds, and never by assigning `innerHTML`. Every value
// that came from the cluster was escaped by `render.js`'s `esc` before it
// reached the string, so a topic name or an API server message is text in the
// output and cannot become markup; this is the second lock on the same door.
function parseFragment(html) {
  const parsed = new DOMParser().parseFromString(
    "<!doctype html><body>" + html,
    "text/html",
  );
  const nodes = [];
  for (const child of Array.from(parsed.body.childNodes)) {
    nodes.push(document.importNode(child, true));
  }
  return nodes;
}

function nav(current, ns) {
  const links = [];
  const suffix = ns === DEFAULT_NAMESPACE ? "" : "?ns=" + encodeURIComponent(ns);
  for (const route of ROUTES) {
    const attrs = { href: route.hash + suffix, class: "nav-link" };
    if (route.hash === current.hash) {
      attrs["aria-current"] = "page";
      attrs.class = "nav-link nav-link-current";
    }
    links.push(el("a", attrs, route.title));
  }
  return el("div", { class: "nav-bar" }, [
    el("nav", { class: "nav", "aria-label": "Sections" }, links),
    namespaceForm(current, ns),
  ]);
}

// The namespace picker. It changes the hash and nothing else -- no request is
// issued here, and no value is stored anywhere.
function namespaceForm(current, ns) {
  const input = el("input", { id: "ns-input", name: "ns", value: ns });
  const form = el("form", { class: "ns-form" }, [
    el("label", { for: "ns-input" }, "namespace"),
    input,
    el("button", { type: "submit" }, "Go"),
  ]);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const value = input.value.trim() || DEFAULT_NAMESPACE;
    window.location.hash =
      current.hash + (value === DEFAULT_NAMESPACE ? "" : "?ns=" + encodeURIComponent(value));
  });
  return form;
}

// The shell's one piece of content: the route, and the same-origin API prefix
// this page will address once its views land. `path(...)` builds it, so the
// prefix on screen is the prefix the client would send -- and it is relative,
// which is the property the whole serving story rests on.
function view(route) {
  return [
    el("h2", null, route.title),
    el("p", { class: "blurb" }, route.blurb),
    el("p", { class: "pending" }, "This view is not built yet. The shell ships first."),
    el("dl", { class: "facts" }, [
      el("dt", null, "API prefix"),
      el("dd", null, el("code", null, path("apis", GROUP, VERSION))),
      el("dt", null, "Credential"),
      el("dd", null, "the viewer's own, attached by the proxy; none is held here"),
    ]),
  ];
}

function render() {
  const hash = window.location.hash;
  const here = parseHash(hash);
  const current = routeFor(here.route) || routeFor(DEFAULT_HASH);
  const header = document.getElementById("nav-slot");
  const main = document.getElementById("view-slot");
  if (header !== null) {
    replace(header, nav(current, here.ns));
  }
  if (main !== null) {
    if (here.name !== "" && typeof current.detail === "function") {
      replace(main, el("p", { class: "pending" }, "Reading " + here.name + "..."));
      current.detail(main, here.ns, here.name, parseFragment);
    } else if (current.cluster === true && typeof current.mount === "function") {
      // The one CLUSTER-SCOPED read in this application. It takes no
      // namespace, because the TrustRoster has none.
      replace(main, el("p", { class: "pending" }, "Reading " + current.title + "..."));
      current.mount(main, parseFragment);
    } else if (current.route === true && typeof current.mount === "function") {
      // The approvals page reads `subject`, `hash` and `name` off the hash the
      // wizard navigated to. They are route parameters and never values parsed
      // out of the two approval documents.
      //
      // THE EXTRACTION IS THE PAGE MODULE'S OWN, and deliberately not written
      // out here. This file touches `window` at module scope and so cannot be
      // imported under `node --test`; an extraction written inline would be a
      // hop no test in either language could reach, and swapping two of these
      // three values left the whole suite green. `approvalRouteParams` is a
      // pure function of the hash string, and the suite calls it.
      replace(main, el("p", { class: "pending" }, "Reading " + current.title + "..."));
      current.mount(main, here.ns, approvalRouteParams(hash), parseFragment);
    } else if (typeof current.mount === "function") {
      replace(main, el("p", { class: "pending" }, "Reading " + current.title + "..."));
      current.mount(main, here.ns, parseFragment);
    } else {
      replace(main, view(current));
    }
  }
  document.title = "Logweir -- " + current.title;
}

window.addEventListener("hashchange", render);

if (window.location.hash === "") {
  window.location.hash = DEFAULT_HASH;
}

render();
