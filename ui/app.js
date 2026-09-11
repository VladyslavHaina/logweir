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
import { mountClusters } from "./pages/clusters.js";
import { mountSchedules } from "./pages/schedules.js";
import { mountBackups } from "./pages/backups.js";
import { mountHistory } from "./pages/history.js";

// The seven routes, in navigation order. The hash is the whole route.
const ROUTES = [
  { hash: "#/clusters", title: "Clusters", blurb: "The KafkaCluster objects this namespace can reach.", mount: mountClusters },
  { hash: "#/schedules", title: "Schedules", blurb: "BackupSchedule objects, their next slot and their suspend state.", mount: mountSchedules },
  { hash: "#/backups", title: "Backups", blurb: "Backup runs, each with the evidence weirkeeper recorded for it.", mount: mountBackups },
  { hash: "#/history", title: "History", blurb: "Completed runs over time, newest first.", mount: mountHistory },
  { hash: "#/restore", title: "Restore", blurb: "Restore runs and the preflight the runner reported." },
  { hash: "#/approvals", title: "Approvals", blurb: "Approval objects, and which key signed each one." },
  { hash: "#/keys", title: "Keys", blurb: "The TrustRoster, read-only: it is cluster-scoped and admin-only." },
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

/** The `#/route?ns=name` a hash carries: the route half and the namespace. */
export function parseHash(hash) {
  const text = typeof hash === "string" ? hash : "";
  const question = text.indexOf("?");
  const route = question === -1 ? text : text.slice(0, question);
  let ns = DEFAULT_NAMESPACE;
  if (question !== -1) {
    for (const pair of text.slice(question + 1).split("&")) {
      const equals = pair.indexOf("=");
      if (equals !== -1 && pair.slice(0, equals) === "ns") {
        const value = decodeURIComponent(pair.slice(equals + 1)).trim();
        if (value.length > 0) {
          ns = value;
        }
      }
    }
  }
  return { route: route, ns: ns };
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
  const here = parseHash(window.location.hash);
  const current = routeFor(here.route) || routeFor(DEFAULT_HASH);
  const header = document.getElementById("nav-slot");
  const main = document.getElementById("view-slot");
  if (header !== null) {
    replace(header, nav(current, here.ns));
  }
  if (main !== null) {
    if (typeof current.mount === "function") {
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
