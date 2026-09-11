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

// The seven routes, in navigation order. The hash is the whole route.
const ROUTES = [
  { hash: "#/clusters", title: "Clusters", blurb: "The KafkaCluster objects this namespace can reach." },
  { hash: "#/schedules", title: "Schedules", blurb: "BackupSchedule objects, their next slot and their suspend state." },
  { hash: "#/backups", title: "Backups", blurb: "Backup runs, each with the evidence weirkeeper recorded for it." },
  { hash: "#/history", title: "History", blurb: "Completed runs over time, newest first." },
  { hash: "#/restore", title: "Restore", blurb: "Restore runs and the preflight the runner reported." },
  { hash: "#/approvals", title: "Approvals", blurb: "Approval objects, and which key signed each one." },
  { hash: "#/keys", title: "Keys", blurb: "The TrustRoster, read-only: it is cluster-scoped and admin-only." },
];

const DEFAULT_HASH = ROUTES[0].hash;

function routeFor(hash) {
  for (const route of ROUTES) {
    if (route.hash === hash) {
      return route;
    }
  }
  return null;
}

function nav(current) {
  const links = [];
  for (const route of ROUTES) {
    const attrs = { href: route.hash, class: "nav-link" };
    if (route.hash === current.hash) {
      attrs["aria-current"] = "page";
      attrs.class = "nav-link nav-link-current";
    }
    links.push(el("a", attrs, route.title));
  }
  return el("nav", { class: "nav", "aria-label": "Sections" }, links);
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
  const current = routeFor(window.location.hash) || routeFor(DEFAULT_HASH);
  const header = document.getElementById("nav-slot");
  const main = document.getElementById("view-slot");
  if (header !== null) {
    replace(header, nav(current));
  }
  if (main !== null) {
    replace(main, view(current));
  }
  document.title = "Logweir -- " + current.title;
}

window.addEventListener("hashchange", render);

if (window.location.hash === "") {
  window.location.hash = DEFAULT_HASH;
}

render();
