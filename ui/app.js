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
import { CONSOLE, LEGACY, applyGrants, grantedNamespaces, selectMode } from "./client.js";
import { el, enhanceDatagrids, markScrollRegions, replace } from "./render.js";
import { mountClusterDetail, mountClusters } from "./pages/clusters.js";
import { mountDestinationDetail, mountDestinations } from "./pages/destinations.js";
import { mountScheduleDetail, mountSchedules } from "./pages/schedules.js";
import { mountBackupDetail, mountBackups } from "./pages/backups.js";
import { mountHistory, mountRestoreDetail } from "./pages/history.js";
import { mountRestoreWizard, restoreRouteParams } from "./pages/restore-wizard.js";
import { approvalRouteParams, mountApprovals } from "./pages/approvals.js";
import { mountKeys } from "./pages/keys.js";
// D3 (PLAT-12.1, PLAT-14.1, PLAT-14.2, PLAT-15.1): the durable operation view
// and the two D3 surfaces that have a list and a detail of their own.
import { mountOperation, operationRouteParams } from "./pages/operation.js";
import { mountProtection, mountProtectionDetail } from "./pages/protection.js";
import { mountCatalog, mountCatalogDetail } from "./pages/catalog.js";

// The eleven routes, in navigation order. The hash is the whole route.
//
// `#/destinations` IS CONSOLE-ONLY, AND IT IS STILL IN THIS LIST. The product
// API serves saved destinations and `kubectl proxy` does not; the legacy UI
// ServiceAccount has no binding for the kind either (D2 section 7.4), so a page that
// tried would render a 403 it did not cause. Hiding the tab in legacy mode
// would be worse than showing it: the route decides the mode ONCE at boot and
// the tab would appear and vanish under a reader. So the route is always
// there, and in legacy mode `ui/client.js` refuses each call BY NAME, with a
// sentence saying which API serves the flow.
const ROUTES = [
  { hash: "#/clusters", title: "Clusters", blurb: "The KafkaCluster objects this namespace can reach.", mount: mountClusters, detail: mountClusterDetail },
  { hash: "#/destinations", title: "Destinations", blurb: "Saved archive locations: one location, written down once, referenced by name.", mount: mountDestinations, detail: mountDestinationDetail },
  // PLAT-10.2 gave this route a DETAIL, and the hash it is reached by is the
  // one every other list/detail pair here uses: `?name=` on the list's own
  // route. That is why the migration note is "existing deep links keep
  // working" rather than "are redirected" -- `#/schedules?ns=<ns>` was the
  // only schedules link there had ever been, and a hash with no `name`
  // reaches `mountSchedules` exactly as it always did.
  { hash: "#/schedules", title: "Schedules", blurb: "BackupSchedule objects, their next slot and their suspend state.", mount: mountSchedules, detail: mountScheduleDetail },
  { hash: "#/backups", title: "Backups", blurb: "Backup runs, each with the evidence weirkeeper recorded for it.", mount: mountBackups, detail: mountBackupDetail },
  { hash: "#/history", title: "History", blurb: "Completed runs over time, newest first.", mount: mountHistory, detail: mountRestoreDetail },
  // `#/operations` CARRIES AN IDENTITY IN THE HASH and has no list: an
  // operation is always reached FROM a run -- a row, or the outcome of a
  // submission -- and a list of operations would be the backups and history
  // tables a second time.
  { hash: "#/operations", title: "Operations", blurb: "One durable run: where it is, why, what it produced and whether the evidence verified.", mount: mountOperation, route: true, params: operationRouteParams },
  { hash: "#/protection", title: "Protection", blurb: "Whether a recoverable backup exists, how old it is, and what was alerted about it.", mount: mountProtection, detail: mountProtectionDetail },
  { hash: "#/catalog", title: "Catalog", blurb: "The durable recovery catalog: what is in the archive, whether it is available and whether it verifies.", mount: mountCatalog, detail: mountCatalogDetail },
  { hash: "#/restore", title: "Restore", blurb: "The restore wizard: a chosen recovery point, archive, point in time, target, preflight, plan.", mount: mountRestoreWizard, route: true, params: restoreRouteParams },
  { hash: "#/approvals", title: "Approvals", blurb: "Approval objects, and which key signed each one.", mount: mountApprovals, route: true, params: approvalRouteParams },
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
const DEFAULT_NAMESPACE = "";

/** Owns the short-lived reads and callbacks for one route render. Starting a
 *  new route aborts only its predecessor's reads; mutation calls receive no
 *  signal and therefore remain durable after navigation. */
export function createRouteLifecycle(AbortControllerClass) {
  const Controller = AbortControllerClass || globalThis.AbortController;
  let current = null;
  let generation = 0;
  return {
    begin(routeHash) {
      if (current !== null) {
        current.controller.abort();
      }
      const controller = new Controller();
      const token = {
        controller: controller,
        generation: generation + 1,
        routeHash: typeof routeHash === "string" ? routeHash : null,
      };
      generation = token.generation;
      current = token;
      return {
        generation: token.generation,
        signal: controller.signal,
        isCurrent() {
          // `location.hash` changes synchronously, but `hashchange` is queued.
          // Compare the captured route as well as the controller so a short
          // client-side await cannot submit or render in that gap.
          return current === token &&
            !controller.signal.aborted &&
            (token.routeHash === null || typeof window === "undefined" || window.location.hash === token.routeHash);
        },
      };
    },
    dispose() {
      if (current !== null) {
        current.controller.abort();
        current = null;
      }
    },
  };
}

/** The `#/route?ns=name` a hash carries: the route half, the namespace, and
 *  every other parameter the hash names.
 *
 *  `params` exists for the two routes that carry an identity. The restore
 *  wizard is entered as `#/restore?ns=<ns>&backup=<name>&uid=<uid>`: `uid` is
 *  the recovery point's identity and `backup` is its name, and the wizard
 *  refuses rather than substituting another point when nothing answers to the
 *  uid. The approvals route, which the restore wizard's guided
 *  submit navigates to as
 *  `#/approvals?subject=<restore>&hash=<planHash>&name=<approval>`. `subject`
 *  names WHICH Restore; the approvals page reads that Restore and derives the
 *  subject, UID, plan hash and Approval name it would submit from the object
 *  itself, and treats `hash` and `name` only as the identity the wizard
 *  reviewed -- a route that disagrees with the Restore is refused, never
 *  submitted. A visit with no `subject` is the standalone page.
 *
 *  THAT HAND-OFF IS NOT MADE HERE. `pages/approvals.js` exports
 *  `approvalRouteParams(hash)` and the approvals branch below calls it, because
 *  this module cannot be imported under `node --test` -- it touches `window` at
 *  module scope -- and the three values above are worth a test that can
 *  actually run. This function keeps `params` as the generic bag every route
 *  can read. */
export function parseHash(hash, initialNamespace) {
  const text = typeof hash === "string" ? hash : "";
  const question = text.indexOf("?");
  const route = question === -1 ? text : text.slice(0, question);
  let ns = typeof initialNamespace === "string" ? initialNamespace : DEFAULT_NAMESPACE;
  let name = "";
  // NO PROTOTYPE: the keys are whatever the address bar spells, and a bag read
  // by an outside name must not answer `constructor` with a function.
  const params = Object.create(null);
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
  labelTableCells(parsed);
  const nodes = [];
  for (const child of Array.from(parsed.body.childNodes)) {
    nodes.push(document.importNode(child, true));
  }
  // THE DATAGRIDS, ENHANCED HERE AND BEFORE ANY PAGE WIRES ITS CONTROLS
  // (PLAT-18.2). A table a page declared a datagrid gains Clarity's filter,
  // sort and pagination around the same rows (`render.js`'s
  // `enhanceDatagrids`). It runs on the ADOPTED nodes, because `importNode`
  // copies no listener, and before the page's own `querySelectorAll` calls,
  // so a grid whose rows hold controls keeps every row findable.
  for (const node of nodes) {
    enhanceDatagrids(node);
  }
  return nodes;
}

// THE STACKED-CARD LABELS, SET HERE AND NOT IN THE STRING. Below 768 px (Clarity's `sm` width) the
// stylesheet turns every `table.grid` row into a card and shows each cell's
// column caption beside its value, read from the cell's `data-label`. The
// caption is copied from the header row at adoption time, with `setAttribute`
// and never `innerHTML`, so the page modules stay pure functions from a JSON
// object to a string -- which is what keeps the string the behaviour suite
// asserts on identical to the string a browser receives -- and the header row
// remains the one place a column's caption is written.
function labelTableCells(parsed) {
  for (const table of Array.from(parsed.querySelectorAll("table.grid"))) {
    const captions = Array.from(table.querySelectorAll("thead th")).map(
      (th) => th.textContent,
    );
    for (const row of Array.from(table.querySelectorAll("tbody tr"))) {
      const cells = Array.from(row.children);
      for (let i = 0; i < cells.length; i += 1) {
        if (cells[i].hasAttribute("colspan")) {
          continue;
        }
        if (typeof captions[i] === "string" && captions[i].length > 0) {
          cells[i].setAttribute("data-label", captions[i]);
        }
      }
    }
  }
}

function nav(current, ns, allowedNamespaces) {
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
    namespaceForm(current, ns, allowedNamespaces),
  ]);
}

// The namespace picker. It changes the hash and nothing else -- no request is
// issued here, and no value is stored anywhere.
function namespaceForm(current, ns, allowedNamespaces) {
  const allowed = Array.isArray(allowedNamespaces) ? allowedNamespaces : [];
  const input = allowed.length === 0
    ? el("input", { id: "ns-input", name: "ns", value: ns, required: "" })
    : el(
      "select",
      { id: "ns-input", name: "ns" },
      [el("option", { value: "" }, "Choose a namespace")].concat(
        allowed.map((name) => el("option", { value: name, selected: name === ns ? "" : null }, name)),
      ),
    );
  const form = el("form", { class: "ns-form" }, [
    el("label", { for: "ns-input" }, "namespace"),
    input,
    el("button", { type: "submit" }, "Go"),
  ]);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const value = input.value.trim();
    if (value.length === 0) {
      return;
    }
    window.location.hash =
      current.hash + "?ns=" + encodeURIComponent(value);
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

/** WHAT THE MASTHEAD AND THE COLOPHON SAY ABOUT WHOSE AUTHORITY THIS PAGE
 *  CARRIES, per mode (POC-P1).
 *
 *  The same files are served two ways (`ui/client.js`), and the sentence that
 *  is true of one is false of the other. Behind `kubectl proxy` (legacy) every
 *  request carries the kubeconfig that started the proxy, which is the
 *  residual `ui/README.md` documents and this page must keep saying. Behind
 *  `logweir-api` (the shared console) no kubeconfig is involved at all: the
 *  product API authorises every request for the session's own identity and
 *  namespace grants. The static page used to print the legacy sentence in
 *  BOTH, so a shared console behind an identity provider told every signed-in
 *  user that it ran with a proxy's kubeconfig. `index.html` now carries copy
 *  true of both, and [`applyModeCopy`] writes the decided mode's own.
 *
 *  Each value is a list of segments: a string is text, and `["code", text]`
 *  is a `<code>` element. Nothing here is markup, so nothing is parsed. */
export const MODE_COPY = Object.freeze({
  [LEGACY]: Object.freeze({
    tagline: Object.freeze([
      "A Kubernetes API client. It runs with the authority of the kubeconfig that started " +
        "the proxy serving it, and it holds no credential of its own.",
    ]),
    colophon: Object.freeze([
      "Served by ", Object.freeze(["code", "kubectl proxy"]),
      ", which attaches the viewer's own kubeconfig credential to every request it forwards. " +
        "See ",
      Object.freeze(["code", "ui/README.md"]), " for what that costs and how to narrow it.",
    ]),
  }),
  [CONSOLE]: Object.freeze({
    tagline: Object.freeze([
      "The Logweir console. Every request goes to the Logweir product API, which authorises " +
        "it for this session's identity and namespace grants; this page holds no credential " +
        "of its own.",
    ]),
    colophon: Object.freeze([
      "Served by ", Object.freeze(["code", "logweir-api"]),
      ", which authorises every request against this session's grants in the namespace it " +
        "names. The session is a cookie this page never reads. See ",
      Object.freeze(["code", "ui/README.md"]), " for the two ways this page is served.",
    ]),
  }),
});

/** Writes the decided mode's [`MODE_COPY`] into the masthead tagline and the
 *  colophon's serving line. A mode it does not know, or a document without the
 *  two elements, changes nothing -- the neutral copy `index.html` ships is
 *  still true. Returns whether anything was written. */
export function applyModeCopy(doc, decidedMode) {
  const copy = Object.prototype.hasOwnProperty.call(MODE_COPY, decidedMode)
    ? MODE_COPY[decidedMode]
    : null;
  if (copy === null || doc === null || doc === undefined ||
    typeof doc.getElementById !== "function") {
    return false;
  }
  let wrote = false;
  for (const [id, segments] of [["masthead-tagline", copy.tagline],
    ["colophon-serving", copy.colophon]]) {
    const node = doc.getElementById(id);
    if (node === null || node === undefined) {
      continue;
    }
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
    for (const segment of segments) {
      if (typeof segment === "string") {
        node.appendChild(doc.createTextNode(segment));
      } else {
        const code = doc.createElement(segment[0]);
        code.appendChild(doc.createTextNode(segment[1]));
        node.appendChild(code);
      }
    }
    wrote = true;
  }
  return wrote;
}

/** WHAT THE SHELL DOES ONCE THE MODE IS DECIDED, as a function the suite can
 *  call (`boot` cannot run under node). The authority sentence follows the
 *  decided mode in both directions (POC-P1) -- the legacy page keeps saying it
 *  carries a kubeconfig and the shared console stops saying so -- and in the
 *  shared console the session's grants replace the runtime namespace list,
 *  with one more render when that changed anything. */
export function modeDecided(record, doc, context, rerender) {
  const decided = (record || {}).mode;
  applyModeCopy(doc, decided);
  if (decided === CONSOLE && applyGrants(context, grantedNamespaces())) {
    rerender();
  }
}

export function namespaceContext(root) {
  const runtime = globalThis.LOGWEIR_NAMESPACE_CONTEXT || {};
  const raw = root.getAttribute("data-logweir-namespaces") || "";
  const configured = Array.isArray(runtime.allowed) ? runtime.allowed : raw.split(",");
  const allowed = configured
    .filter((name) => typeof name === "string")
    .map((name) => name.trim())
    .filter((name, index, all) => name.length > 0 && all.indexOf(name) === index);
  const selectedAttribute = root.getAttribute("data-logweir-namespace") || "";
  const selectedRuntime = typeof runtime.selected === "string" ? runtime.selected : "";
  const selected = (selectedAttribute || selectedRuntime).trim();
  return {
    allowed: allowed,
    selected: selected.length > 0 && (allowed.length === 0 || allowed.indexOf(selected) !== -1)
      ? selected
      : (allowed.length === 1 ? allowed[0] : ""),
  };
}

function namespacePrompt(allowed) {
  const sentence = allowed.length === 0
    ? "Choose a namespace above before Logweir reads cluster resources. The page does not list namespaces."
    : "Choose one of the namespaces this installation explicitly authorizes above.";
  return el("p", { class: "pending", role: "status" }, sentence);
}

/** FOCUS FOLLOWS A NAVIGATION (PLAT-18.2). A route change replaces the
 *  navigation and the view, so the link a keyboard reader activated is gone
 *  and their focus is on the document body; the next Tab would start at the
 *  skip link again. Once the new view has rendered, focus moves to the view
 *  slot -- the pattern a screen reader expects of a page that changed -- unless
 *  the reader has already moved it somewhere themselves. The first render of
 *  a page load is not a navigation and leaves focus alone. */
function focusView(result) {
  Promise.resolve(result).catch(() => null).then(() => {
    const main = document.getElementById("view-slot");
    const active = document.activeElement;
    if (main !== null && (active === null || active === document.body)) {
      main.focus({ preventScroll: true });
    }
  });
}

function render(lifecycle, context, navigated) {
  const hash = window.location.hash;
  const here = parseHash(hash, context.selected);
  const current = routeFor(here.route) || routeFor(DEFAULT_HASH);
  const header = document.getElementById("nav-slot");
  const main = document.getElementById("view-slot");
  if (header !== null) {
    replace(header, nav(current, here.ns, context.allowed));
  }
  let mounted = null;
  if (main !== null) {
    if (!current.cluster && (here.ns.length === 0 || (context.allowed.length > 0 && context.allowed.indexOf(here.ns) === -1))) {
      replace(main, namespacePrompt(context.allowed));
    } else if (here.name !== "" && typeof current.detail === "function") {
      replace(main, el("p", { class: "pending", role: "status" }, "Reading " + here.name + "..."));
      mounted = current.detail(main, here.ns, here.name, parseFragment, lifecycle);
    } else if (current.cluster === true && typeof current.mount === "function") {
      // The one CLUSTER-SCOPED read in this application. It takes no
      // namespace, because the TrustRoster has none.
      replace(main, el("p", { class: "pending", role: "status" }, "Reading " + current.title + "..."));
      mounted = current.mount(main, parseFragment, undefined, lifecycle);
    } else if (current.route === true && typeof current.mount === "function") {
      // THREE ROUTES CARRY AN IDENTITY IN THE HASH, and each names its own
      // extractor. The approvals page reads `subject`, `hash` and `name`; the
      // restore wizard reads `backup` and `uid`, the recovery point it is
      // bound to; the operation view reads `kind`, `name` and `uid`, the run a
      // link was about. In all three they are route parameters and never values
      // parsed out of a document, and in both cases the page checks them
      // against the object it reads before it offers anything.
      //
      // THE EXTRACTION IS THE PAGE MODULE'S OWN, and deliberately not written
      // out here. This file touches `window` at module scope and so cannot be
      // imported under `node --test`; an extraction written inline would be a
      // hop no test in either language could reach, and swapping two of these
      // values left the whole suite green. `approvalRouteParams` and
      // `restoreRouteParams` are pure functions of the hash string, and the
      // suite calls both.
      replace(main, el("p", { class: "pending", role: "status" }, "Reading " + current.title + "..."));
      mounted = current.mount(main, here.ns, current.params(hash), parseFragment, undefined, lifecycle);
    } else if (typeof current.mount === "function") {
      replace(main, el("p", { class: "pending", role: "status" }, "Reading " + current.title + "..."));
      mounted = current.mount(main, here.ns, parseFragment, lifecycle);
    } else {
      replace(main, view(current));
    }
  }
  document.title = "Logweir -- " + current.title;
  if (navigated === true) {
    focusView(mounted);
  }
}

function boot() {
  const context = namespaceContext(document.documentElement);
  const routes = createRouteLifecycle();
  const renderCurrent = () => render(routes.begin(window.location.hash), context);
  window.addEventListener("hashchange", () =>
    render(routes.begin(window.location.hash), context, true));
  window.addEventListener("pagehide", () => routes.dispose());
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) {
      renderCurrent();
    }
  });

  // THE SKIP LINK IS A BUTTON, NOT AN ANCHOR. An `<a href="#view-slot">` would
// set the hash, and every hash on this page is a route: the router would
// read `#view-slot`, find no such route, and render the default one. So the
// control moves focus with a script and touches the hash not at all.
  const skip = document.getElementById("skip-link");
  if (skip !== null) {
    skip.addEventListener("click", () => {
      const main = document.getElementById("view-slot");
      if (main !== null) {
        main.focus();
      }
    });
  }

  // A REGION THAT SCROLLS IS A REGION A KEYBOARD CAN REACH (PLAT-18.2).
  // Whether a table or a plan block overflows is a fact of layout, so it is
  // read after every change to the view and on every resize, once per frame.
  const view = document.getElementById("view-slot");
  if (view !== null) {
    let queued = false;
    const mark = () => {
      if (queued) {
        return;
      }
      queued = true;
      window.requestAnimationFrame(() => {
        queued = false;
        markScrollRegions(view);
      });
    };
    new MutationObserver(mark).observe(view, { childList: true, subtree: true });
    window.addEventListener("resize", mark);
  }

  if (window.location.hash === "") {
    window.location.hash = DEFAULT_HASH;
  }

  renderCurrent();

  // WHICH API IS IN FRONT OF THIS PAGE, DECIDED ONCE (PLAT-18.1, decision D0
  // stage 6). `ui/client.js` asks `GET /api/v1/session` exactly once, records
  // the answer for the life of the loaded page, and every page read and write
  // goes through that record. The first render above does not wait for it:
  // in the legacy deployment the answer is a refusal and only the masthead's
  // authority sentence changes (`modeDecided`), so the page paints exactly as
  // fast as it did before.
  //
  // IN CONSOLE MODE THE GRANTS REPLACE THE RUNTIME LIST. The namespaces come
  // from the session document -- the server knows what this actor may reach --
  // rather than from the ConfigMap `runtime.js` carries, which is the
  // installation's list and not the viewer's. A change is one more render;
  // no change is none.
  selectMode().then((record) => modeDecided(record, document, context, renderCurrent), () => {
    // `selectMode` resolves for every answer including a refusal, so this arm
    // is for a throw inside `renderCurrent` above -- which would otherwise be
    // an unhandled rejection with nothing on screen to say a render failed.
    // The first render has already happened; there is nothing further to do
    // here but not disappear silently.
  });
}

if (typeof window !== "undefined" && typeof document !== "undefined") {
  boot();
}
