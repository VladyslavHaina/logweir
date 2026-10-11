// app.js -- the shell: a hash router and the frame around it.
//
// A HASH ROUTER, ON PURPOSE. Every route lives after the `#`, so the browser
// never asks the server for a path other than `/ui/` and its assets, which
// `logweir-api` serves from an exact allowlist. A path-based router would need
// a rewrite rule, and no route here can produce a path-level 404.
//
// THIS MODULE MAKES NO NETWORK REQUEST; `api.js` is the only module that does.

import { path } from "./api.js";
import {
  CONSOLE,
  SIGNED_OUT,
  UNAVAILABLE,
  unavailableReason,
  applyGrants,
  granted,
  grantedAnywhere,
  grantedNamespaces,
  holdsNoRole,
  mode,
  selectMode,
  sessionIdentity,
  signOut,
  signedOutReason,
} from "./client.js";
import {
  el,
  enhanceDatagrids,
  errorBox,
  esc,
  markScrollRegions,
  replace,
  signInHref,
} from "./render.js";
import { mountClusterDetail, mountClusters } from "./pages/clusters.js";
import { mountDestinationDetail, mountDestinations } from "./pages/destinations.js";
import { mountScheduleDetail, mountSchedules } from "./pages/schedules.js";
import { mountBackupDetail, mountBackups } from "./pages/backups.js";
import { mountHistory, mountRestoreDetail } from "./pages/history.js";
import { mountRestoreWizard, restoreRouteParams } from "./pages/restore-wizard.js";
import { approvalRouteParams, mountApprovals } from "./pages/approvals.js";
import { mountKeys } from "./pages/keys.js";
// The durable operation view and the two surfaces with a list and a detail
// of their own.
import { mountOperation, operationRouteParams } from "./pages/operation.js";
import { mountProtection, mountProtectionDetail } from "./pages/protection.js";
import { mountCatalog, mountCatalogDetail } from "./pages/catalog.js";

// The eleven routes, in navigation order. The hash is the whole route.
//
const ROUTES = [
  { hash: "#/clusters", title: "Clusters", blurb: "The KafkaCluster objects this namespace can reach.", mount: mountClusters, detail: mountClusterDetail, can: "see its connections", takes: "any role", needs: ["connectionsRead"] },
  { hash: "#/destinations", title: "Destinations", blurb: "Saved archive locations: one location, written down once, referenced by name.", mount: mountDestinations, detail: mountDestinationDetail, can: "see its saved destinations", takes: "any role", needs: ["destinations"] },
  // A detail is reached by `?name=` on the list's own route, as every
  // list/detail pair here is, so `#/schedules?ns=<ns>` with no `name` still
  // reaches `mountSchedules` and existing deep links keep working.
  { hash: "#/schedules", title: "Schedules", blurb: "BackupSchedule objects, their next slot and their suspend state.", mount: mountSchedules, detail: mountScheduleDetail, can: "see its schedules", takes: "any role", needs: ["schedulesRead"] },
  { hash: "#/backups", title: "Backups", blurb: "Backup runs, each with the evidence weirkeeper recorded for it.", mount: mountBackups, detail: mountBackupDetail, can: "see its backups", takes: "any role", needs: ["backupsRead"] },
  { hash: "#/history", title: "History", blurb: "Completed runs over time, newest first.", mount: mountHistory, detail: mountRestoreDetail, can: "see its run history", takes: "any role", needs: ["backupsRead", "restoresRead"] },
  // `#/operations` CARRIES AN IDENTITY IN THE HASH and has no list: an
  // operation is always reached FROM a run, and a list would be the backups
  // and history tables a second time. So it is not a tab (one that opens on
  // "the address bar named none" is a dead end); a visit with no run names
  // the two lists it is reached from.
  { hash: "#/operations", title: "Operations", blurb: "One durable run: where it is, why, what it produced and whether the evidence verified.", mount: mountOperation, route: true, params: operationRouteParams, nav: false, can: "follow its operations", takes: "any role", needs: ["operationsRead"] },
  { hash: "#/protection", title: "Protection", blurb: "Whether a recoverable backup exists, how old it is, and what was alerted about it.", mount: mountProtection, detail: mountProtectionDetail, can: "see its protection", takes: "any role", needs: ["protection"] },
  { hash: "#/catalog", title: "Catalog", blurb: "The durable recovery catalog: what is in the archive, whether it is available and whether it verifies.", mount: mountCatalog, detail: mountCatalogDetail, can: "see its recovery catalog", takes: "any role", needs: ["catalogs"] },
  { hash: "#/restore", title: "Restore", blurb: "The restore wizard: a chosen recovery point, archive, point in time, target, preflight, plan.", mount: mountRestoreWizard, route: true, params: restoreRouteParams, can: "start restores", takes: "the operator or administrator role", needs: ["restoreCreate"] },
  { hash: "#/approvals", title: "Approvals", blurb: "Approval objects, and which key signed each one.", mount: mountApprovals, route: true, params: approvalRouteParams, can: "see its approvals", takes: "any role", needs: ["approvalsRead"] },
  { hash: "#/keys", title: "Keys", blurb: "The TrustRoster, read-only: it is cluster-scoped and admin-only.", mount: mountKeys, cluster: true, can: "view trust material", takes: "the administrator role", needs: ["trustPoliciesRead"] },
];

/** Which tabs this session sees, as a pure function of what the session
 *  publishes: a route is a tab when it is a navigation route at all (`nav !==
 *  false`) and the session holds ANY of the capability flags it `needs` -- in
 *  the chosen namespace when one is chosen and granted, in any namespace
 *  otherwise (always the latter for the cluster-scoped keys view). A hidden
 *  tab is a convenience and never a control: every route still refuses by
 *  name when an address reaches it. */
export function visibleRoutes(routes, ns, has) {
  const ask = typeof has === "function" ? has : () => true;
  return routes.filter((route) => {
    if (route.nav === false) {
      return false;
    }
    const needs = Array.isArray(route.needs) ? route.needs : [];
    if (needs.length === 0) {
      return true;
    }
    return needs.some((flag) => ask(flag, route.cluster === true ? "" : ns));
  });
}

// The session's answer for one flag: the chosen namespace's grant when there
// is one, the union across every granted namespace otherwise.
function sessionHas(flag, ns) {
  return typeof ns === "string" && ns.length > 0 && granted(ns, flag) ? true : grantedAnywhere(flag);
}

/** Whether this session may use a route at all, by the rule the tabs use: it
 *  holds ANY flag the route `needs`. Hiding a tab is not enough -- the address
 *  still reaches the page, whose every control the API would then refuse one
 *  click at a time -- so a route this answers `false` for renders
 *  `renderRoleRefusal` instead. Pure. */
export function routeAllowed(route, ns, has) {
  const ask = typeof has === "function" ? has : () => true;
  const needs = Array.isArray((route || {}).needs) ? route.needs : [];
  return needs.length === 0 || needs.some((flag) => ask(flag, route.cluster === true ? "" : ns));
}

/** The route gate's question, per namespace: the chosen namespace's own
 *  grant, never the union across namespaces the tabs use, so a viewer in
 *  team-a is refused the restore route there whatever they hold in team-b, as
 *  the API would refuse every click. A cluster-scoped route (no namespace)
 *  asks "anywhere". */
export function routeGateHas(flag, ns) {
  return typeof ns === "string" && ns.length > 0 ? granted(ns, flag) : grantedAnywhere(flag);
}

/** The state a route renders for a session whose role cannot use it: what the
 *  page is for, who is signed in with which role, and whom to ask. Pure. */
export function renderRoleRefusal(route, identity) {
  const r = route || {};
  const who = identity || {};
  const roles = Array.isArray(who.roles) ? who.roles : [];
  const where = typeof who.namespace === "string" && who.namespace.length > 0
    ? who.namespace : "";
  const held = roles.length === 0
    ? "no role" + (where.length > 0 ? " in " + where : "")
    : roles.join(", ") + (where.length > 0 ? " in " + where : "");
  return (
    "<section class=\"card role-refusal\" id=\"role-refusal\">" +
    "<h2>" + esc(r.title || "") + "</h2>" +
    "<p class=\"blurb\" id=\"role-refusal-sentence\">Your role " +
    (where.length > 0 ? "in " + esc(where) + " " : "") + "can't " + esc(r.can || "use this page") +
    ".</p>" +
    "<p class=\"note\">You are signed in as " + esc(who.displayName || "") + " (" + esc(held) +
    "). This page needs " + esc(r.takes || "a role that includes it") + ". Nothing was read " +
    "or offered here; ask your Logweir administrator if you need it, or choose another " +
    "namespace above.</p>" +
    "</section>"
  );
}

/** The landing a signed-in user with no role anywhere sees, rather than an
 *  empty navigation that says nothing about why. Pure. */
export function renderNoRole(identity) {
  const who = identity || {};
  return (
    "<section class=\"card role-refusal\" id=\"no-role\">" +
    "<h2>You have no role in any namespace yet</h2>" +
    "<p class=\"blurb\">You are signed in as " + esc(who.displayName || "") + ", and your " +
    "administrator has not granted you a role in any namespace, so there is nothing this " +
    "console can show you.</p>" +
    "<p class=\"note\">Ask your Logweir administrator for a role in the namespaces you work " +
    "in: viewer to look, operator to back up and restore, approver to approve restores. " +
    "Reload this page once it is granted.</p>" +
    "</section>"
  );
}

/** Where sign out leaves the browser: this page with no route. Keeping the
 *  hash would make the previous user's deep link the next user's landing
 *  page. Pure. */
export function signedOutAddress(location) {
  const l = location || {};
  return String(l.pathname || "/") + String(l.search || "");
}

/** The route list the navigation renders, exported for the suite. */
export const NAV_ROUTES = ROUTES;

const DEFAULT_HASH = ROUTES[0].hash;

// The namespace every read and write is scoped to travels IN THE HASH
// (`#/backups?ns=team-a`) and nowhere else -- not in browser storage, a cookie
// or a header: the page stores nothing (rule 2 of
// `scripts/check-ui-offline.sh`, asserted over `ui/` by
// `no_credential_appears_in_the_page`), and a hash survives a reload without
// asking the file server for a path it cannot serve.
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
        /** The same route at another address. The restore wizard writes the step on
 *  screen into the address with `replaceState`, which is not a navigation;
 *  this tells the route which address it is now on, so `isCurrent` keeps
 *  answering for it. Only the route's own path may be kept (another route's
 *  address is a navigation, and is refused), and only while this route is
 *  the current one. */
        retarget(next) {
          if (current !== token || controller.signal.aborted || typeof next !== "string") {
            return false;
          }
          const path = (h) => (h.indexOf("?") === -1 ? h : h.slice(0, h.indexOf("?")));
          if (token.routeHash !== null && path(next) !== path(token.routeHash)) {
            return false;
          }
          token.routeHash = next;
          return true;
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
 *  every other parameter the hash names, as a generic bag every route can
 *  read.
 *
 *  The routes that carry an identity extract it with their page module's own
 *  pure function (`approvalRouteParams`, `restoreRouteParams`,
 *  `operationRouteParams`), because this module touches `window` at module
 *  scope and cannot be imported under `node --test`. */
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

// PARSING A PAGE'S STRING, WITHOUT `innerHTML`. The page modules are pure
// functions from JSON to an HTML string (which is what makes their rules
// assertable under `node --test`, with no DOM). The string becomes nodes
// through `DOMParser`, which executes no script it finds; every cluster
// value was already escaped by `render.js`'s `esc`, so this is the second
// lock on the same door.
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
  // (`render.js`'s `enhanceDatagrids`): on the ADOPTED nodes, because
  // `importNode` copies no listener, and before the page's own
  // `querySelectorAll` calls, so a grid whose rows hold controls keeps every
  // row findable.
  for (const node of nodes) {
    enhanceDatagrids(node);
  }
  return nodes;
}

// THE STACKED-CARD LABELS, SET HERE AND NOT IN THE STRING. Below 768 px
// (Clarity's `sm` width) every `table.grid` row is a card showing each
// cell's column caption from its `data-label`, copied from the header row at
// adoption time with `setAttribute`. The page modules stay pure, so the
// string the suite asserts on is the string a browser receives, and the
// header row stays the one place a caption is written.
function labelTableCells(parsed) {
  for (const table of Array.from(parsed.querySelectorAll("table.grid"))) {
    markLongTokens(table);
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
  for (const route of visibleRoutes(ROUTES, ns, sessionHas)) {
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

/** The tokens in a table that may break anywhere: a code chip or a label
 *  holding a word longer than [`LONG_TOKEN_CHARS`] (a digest, a uid, a key
 *  id) gains the class `long`, and the stylesheet lets only those break
 *  mid-word. Every other chip wraps at a space and nowhere else. */
export const LONG_TOKEN_CHARS = 24;

export function markLongTokens(root) {
  for (const chip of Array.from(root.querySelectorAll("td code, td .badge"))) {
    const text = String(chip.textContent || "");
    const words = text.split(/\s+/);
    // A URL IS READ WHOLE: up to `URL_CHIP_CHARS` it stays on one line; a longer
    // one may break like any long token, so it still cannot force a sideways
    // scroll.
    const url = text.indexOf(":" + "//") !== -1 && text.length <= URL_CHIP_CHARS;
    if (!url && words.some((word) => word.length > LONG_TOKEN_CHARS)) {
      chip.setAttribute("class", (String(chip.getAttribute("class") || "") + " long").trim());
    } else if (text.trim().length <= SHORT_CHIP_CHARS &&
      String(chip.getAttribute("class") || "").split(/\s+/).indexOf("badge") !== -1) {
      // A short label never breaks at its space ("not / suspended"); a longer one
      // still wraps between its words.
      chip.setAttribute("class", (String(chip.getAttribute("class") || "") + " short").trim());
    }
  }
}

/** The longest URL chip kept on one line, and the longest label that never
 *  wraps. */
export const URL_CHIP_CHARS = 40;
export const SHORT_CHIP_CHARS = 16;

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

// The shell's one piece of content: the route, and the same-origin API
// prefix, built by `path(...)` so it is the prefix the client would send --
// relative, which is the property the whole serving story rests on.
function view(route) {
  return [
    el("h2", null, route.title),
    el("p", { class: "blurb" }, route.blurb),
    el("p", { class: "pending" }, "This view is not built yet. The shell ships first."),
    el("dl", { class: "facts" }, [
      el("dt", null, "API prefix"),
      el("dd", null, el("code", null, path("api", "v1"))),
      el("dt", null, "Credential"),
      el("dd", null, "the session cookie, which this page never reads; none is held here"),
    ]),
  ];
}

/** What the masthead and the colophon say about whose authority this page
 *  carries: behind `logweir-api` no kubeconfig is involved, and the product
 *  API authorises every request for the session's own identity and namespace
 *  grants. `index.html` carries neutral copy, and [`applyModeCopy`] writes
 *  the console's own once the probe has answered. Each value is a list of
 *  segments: a string is text and `["code", text]` a `<code>` element, so
 *  nothing is parsed. */
export const MODE_COPY = Object.freeze({
  [CONSOLE]: Object.freeze({
    tagline: Object.freeze([
      "The Logweir console. Every request goes to the Logweir product API, which authorises " +
        "it for this session's identity and namespace grants; this page holds no credential " +
        "of its own.",
    ]),
    colophon: Object.freeze([
      "Served by ", Object.freeze(["code", "logweir-api"]),
      // No repository path: a signed-in user has no checkout to open
      // `ui/README.md` in.
      ", which authorises every request against this session's grants in the namespace it " +
        "names. The session is a cookie this page never reads.",
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

/** What the shell does once the mode is decided, as a function the suite can
 *  call (`boot` cannot run under node): the console's authority sentence is
 *  written, and the session's grants replace the runtime namespace list, with
 *  one more render when that changed anything. */
export function modeDecided(record, doc, context, rerender) {
  const decided = (record || {}).mode;
  // A signed-out visitor, and a console that could not be reached, are IN the
  // shared console: its copy is the console's.
  applyModeCopy(doc, decided === SIGNED_OUT || decided === UNAVAILABLE ? CONSOLE : decided);
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

// An instruction, not a loading state: nothing is being read while the page
// waits for a namespace, so the prompt carries no spinner.
export function namespacePrompt(allowed) {
  const sentence = allowed.length === 0
    ? "Choose a namespace above before Logweir reads cluster resources. The page does not list namespaces."
    : "Choose one of the namespaces this installation explicitly authorizes above.";
  return el("p", { class: "prompt", role: "status" }, sentence);
}

/** The page a signed-out visitor gets, on every route: one action, Sign in,
 *  which returns to the address they asked for, and no namespace box, tabs or
 *  read -- this page may read nothing for someone the product API does not
 *  know. `reason` is the probe's problem code, so an expired session is told
 *  so rather than told it never signed in. Pure. */
export function renderSignIn(reason, hash) {
  const expired = reason === "session_expired";
  return (
    "<section class=\"card signin\" id=\"sign-in\">" +
    "<h2>" + (expired ? "Your session has ended" : "Sign in to Logweir") + "</h2>" +
    "<p class=\"blurb\">" +
    (expired
      ? "Console sessions are short-lived and this one has expired. Sign in again to carry on " +
        "where you were."
      : "This is the Logweir console for Kafka backup and restore. Sign in with your " +
        "organisation's identity provider to see the namespaces you have been granted.") +
    "</p>" +
    "<p class=\"actions\"><a class=\"button primary\" id=\"sign-in-link\" href=\"" +
    esc(signInHref(hash)) + "\">Sign in</a></p>" +
    "<p class=\"note\">You come back to this page once you are signed in. What you may see " +
    "and do depends on the roles your administrator granted you in each namespace.</p>" +
    "</section>"
  );
}

/** The page a console that could not be reached renders: what the probe got,
 *  in words, and Retry. Pure. */
export function renderUnavailable(reason) {
  return (
    "<section class=\"card signin\" id=\"console-unavailable\" role=\"alert\">" +
    "<h2>Can't reach the Logweir service</h2>" +
    "<p class=\"blurb\">This is the Logweir console, and its API did not answer the page's " +
    "first request, so nothing has been read and nothing is shown in its place.</p>" +
    "<p class=\"note\" id=\"console-unavailable-reason\">" + esc(String(reason || "")) + "</p>" +
    "<p class=\"actions\"><button type=\"button\" class=\"primary\" id=\"console-retry\">" +
    "Retry</button></p>" +
    "</section>"
  );
}

/** The header's session indicator: who is signed in, in which role for the
 *  chosen namespace, and Sign out. `identity` is `ui/client.js`'s
 *  `sessionIdentity(ns)`, and `null` renders nothing. A localAdmin console
 *  names its one actor and offers no Sign out, because it has no sign-in.
 *  Pure. */
export function renderIdentity(identity) {
  if (identity === null || identity === undefined) {
    return "";
  }
  const local = identity.authenticationMode === "localAdmin";
  const roles = Array.isArray(identity.roles) ? identity.roles : [];
  const where = typeof identity.namespace === "string" && identity.namespace.length > 0
    ? identity.namespace
    : "";
  const roleText = local
    ? "local administrator"
    : (roles.length === 0
      ? (identity.noRole === true
        ? "no role yet"
        : (where.length > 0 ? "no role in " + where : "choose a namespace to see your role"))
      : roles.join(", ") + (where.length > 0 ? " in " + where : ""));
  return (
    "<div class=\"session-identity\" id=\"session-identity\">" +
    "<span class=\"session-who\"><span class=\"visually-hidden\">Signed in as </span>" +
    "<span class=\"session-name\" title=\"" + esc(identity.subject) + "\">" +
    esc(local ? "Local administrator" : identity.displayName) + "</span>" +
    "<span class=\"session-role\" id=\"session-role\">" + esc(roleText) + "</span></span>" +
    (identity.canSignOut === true
      ? "<button type=\"button\" class=\"session-signout\" id=\"sign-out\">Sign out</button>"
      : "") +
    "</div>"
  );
}

// Writes the identity into the masthead and wires Sign out. A failed sign-out
// is shown beside the button and changes nothing else; a successful one
// reloads, so the next probe answers 401 and the sign-in page is what renders.
function renderSession(ns) {
  const slot = document.getElementById("session-slot");
  if (slot === null) {
    return;
  }
  replace(slot, parseFragment(renderIdentity(sessionIdentity(ns))));
  const button = slot.querySelector("#sign-out");
  if (button === null) {
    return;
  }
  button.addEventListener("click", () => {
    button.disabled = true;
    signOut().then(() => {
      // A navigation without the hash, not a reload: the next user signs in to the
      // console's first page, not to this user's last one.
      window.location.replace(signedOutAddress(window.location));
    }, (error) => {
      button.disabled = false;
      const failed = errorBox(error);
      failed.classList.add("session-error");
      slot.appendChild(failed);
    });
  });
}

/** Focus follows a navigation. A route change replaces the navigation and
 *  the view, so the activated link is gone and focus falls to the body; once
 *  the new view has rendered, focus moves to the view slot (what a screen
 *  reader expects of a changed page) unless the reader has moved it
 *  themselves. The first render of a page load is not a navigation and leaves
 *  focus alone. */
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
  const decided = mode();
  // SIGNED OUT: no tabs, no namespace box and no page. Every route is the
  // sign-in page, and its link returns to the address that was asked for.
  if (decided === SIGNED_OUT) {
    if (header !== null) {
      replace(header, []);
    }
    renderSession(here.ns);
    if (main !== null) {
      replace(main, parseFragment(renderSignIn(signedOutReason(), hash)));
    }
    document.title = "Logweir -- Sign in";
    return;
  }
  // THE CONSOLE COULD NOT BE REACHED: no tabs, no page, and a Retry that asks
  // again from a clean page.
  if (decided === UNAVAILABLE) {
    if (header !== null) {
      replace(header, []);
    }
    renderSession(here.ns);
    if (main !== null) {
      replace(main, parseFragment(renderUnavailable(unavailableReason())));
      const retry = main.querySelector("#console-retry");
      if (retry !== null) {
        retry.addEventListener("click", () => window.location.reload());
      }
    }
    document.title = "Logweir -- unavailable";
    return;
  }
  if (header !== null) {
    replace(header, nav(current, here.ns, context.allowed));
  }
  renderSession(here.ns);
  let mounted = null;
  if (main !== null && decided === null) {
    // BEFORE THE ONE PROBE HAS ANSWERED, NOTHING IS MOUNTED. Every page's first
    // read waits for the mode anyway (`client.js` `ensure`), so this costs no
    // time and prevents a page painted for the wrong state, such as a namespace
    // prompt shown to a visitor about to be asked to sign in. The probe is
    // bounded at five seconds.
    replace(main, el("p", { class: "pending", role: "status" }, "Connecting to Logweir..."));
  } else if (main !== null) {
    if (decided === CONSOLE && holdsNoRole()) {
      replace(main, parseFragment(renderNoRole(sessionIdentity(here.ns))));
    } else if (!current.cluster && (here.ns.length === 0 || (context.allowed.length > 0 && context.allowed.indexOf(here.ns) === -1))) {
      replace(main, namespacePrompt(context.allowed));
    } else if (decided === CONSOLE && !routeAllowed(current, here.ns, routeGateHas)) {
      replace(main, parseFragment(renderRoleRefusal(current, sessionIdentity(here.ns))));
    } else if (here.name !== "" && typeof current.detail === "function") {
      replace(main, el("p", { class: "pending", role: "status" }, "Reading " + here.name + "..."));
      mounted = current.detail(main, here.ns, here.name, parseFragment, lifecycle);
    } else if (current.cluster === true && typeof current.mount === "function") {
      // The one CLUSTER-SCOPED read in this application. It takes no
      // namespace, because the TrustRoster has none.
      replace(main, el("p", { class: "pending", role: "status" }, "Reading " + current.title + "..."));
      mounted = current.mount(main, parseFragment, undefined, lifecycle);
    } else if (current.route === true && typeof current.mount === "function") {
      // THREE ROUTES CARRY AN IDENTITY IN THE HASH, each with its own extractor:
      // the approvals page reads `subject`, `hash` and `name`; the restore wizard
      // `backup` and `uid`, its recovery point; the operation view `kind`, `name`
      // and `uid`, the run a link was about. They are route parameters, never
      // values parsed out of a document, and each page checks them against the
      // object it reads before it offers anything. The extraction is the page
      // module's own pure function, because this file cannot be imported under
      // `node --test` and an inline one would be a hop no test could reach.
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

  // A region that scrolls is a region a keyboard can reach. Whether a table or
  // a plan block overflows is a fact of layout, so it is read after every
  // change to the view and on every resize, once per frame.
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

  // THE SESSION, DECIDED ONCE. `ui/client.js` asks `GET /api/v1/session`
  // exactly once and records the answer for the life of the loaded page; the
  // first render above does not wait for it. In console mode the session's
  // grants replace the runtime namespace list (`runtime.js` carries the
  // installation's list, not the viewer's); a change is one more render.
  selectMode().then((record) => {
    // The first render above painted the frame and a "connecting" line; the
    // decided mode paints the page -- once, whether or not the grants moved.
    let rendered = false;
    modeDecided(record, document, context, () => {
      rendered = true;
      renderCurrent();
    });
    if (!rendered) {
      renderCurrent();
    }
  }, () => {
    // `selectMode` resolves for every answer including a refusal, so this arm is
    // for a throw inside `renderCurrent` above, which would otherwise be an
    // unhandled rejection with nothing on screen to say a render failed.
  });
}

if (typeof window !== "undefined" && typeof document !== "undefined") {
  boot();
}
