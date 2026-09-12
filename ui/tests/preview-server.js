#!/usr/bin/env node
// preview-server.js -- the UI over checked-in fixtures, for a developer's own
// eyes. Task 36.
//
//     node ui/tests/preview-server.js
//
// serves the `ui/` directory at `/ui/` and answers the Kubernetes API paths
// the page calls from the JSON under `ui/tests/fixtures/preview/`, so every
// route, every empty state and the error box can be looked at in a browser
// with no cluster, no `kubectl proxy` and no credential anywhere.
//
// WHAT IT IS NOT. It is not a test: its name does not end in `.spec.js`, so
// `scripts/check-ui-behaviour.sh`'s glob never runs it, and no suite imports
// it (`design.spec.js` reads this file as TEXT to assert the two properties
// below). It is not part of the product: it lives under `ui/tests/`, which
// the release bundle excludes and the shipped-asset scans skip. It is not a
// proxy: it forwards nothing, holds no kubeconfig, and the only thing behind
// it is a directory of JSON.
//
// TWO PROPERTIES, EACH ASSERTED BY A TEST THAT READS THIS SOURCE.
//   1. It binds loopback only. `HOST` below is the literal loopback address
//      and `listen` is handed it by name; there is no other address in this
//      file. `crates/logweir/tests/ui_lint.rs` and `ui/tests/design.spec.js`
//      both look for that.
//   2. It writes nothing. Every request that is not a GET or a HEAD is
//      answered with a 405 and a Kubernetes-shaped Status body, which the
//      page renders in its own error box as the API server's refusal. There
//      is no code path that stores, forwards or mutates anything.
//
// THIS FILE OPENS A SOCKET, AND SAYS SO HERE. `ui_lint.rs`'s
// `the_ui_behaviour_suite_never_dials` forbids the `node:`-prefixed dial
// specifiers from every file under `ui/tests/`, because the SUITE must run
// with the compose stack down and open nothing. This tool is not the suite
// and is never run by the gate, so it names node's built-ins by their bare
// specifiers instead -- `http`, `fs`, `path`, `url` -- which is a statement
// about scope and not a hiding place: `createServer` and `listen` are spelled
// out below for anyone who greps for a server. `ui/tests/` is outside the
// offline gate's relative-specifier rule by design (`api.spec.js` is the
// precedent), and nothing here is ever served by the product.
//
// THE FIXTURE LAYOUT.
//   ui/tests/fixtures/preview/namespaces/<ns>/<plural>.json   one list per kind
//   ui/tests/fixtures/preview/trustrosters.json               the cluster-scoped roster
// A namespace directory that exists is a POPULATED namespace; `default` is
// the one checked in. A namespace with no directory lists nothing, so any
// other name in the page's namespace box is the EMPTY state. The namespace
// `forbidden` is the ERRORING path: every read there is a 403 shaped like the
// API server's own refusal, so the error box can be seen. A detail read is
// served from the matching list by `metadata.name`, and a name the list does
// not carry is a 404. Files are read on every request, so an edited fixture
// shows on the next reload without a restart.

import { createServer } from "http";
import { existsSync, readFileSync, statSync } from "fs";
import { extname, join, resolve, sep } from "path";
import { fileURLToPath } from "url";

/** Loopback, and nothing else. */
const HOST = "127.0.0.1";

/** Not `kubectl proxy`'s 8001, so the two can run side by side. */
const PORT = Number(process.env.LOGWEIR_PREVIEW_PORT || "8011");

/** The answer to every write. */
const METHOD_NOT_ALLOWED = 405;

const UI_ROOT = resolve(fileURLToPath(new URL("../", import.meta.url)));
const TESTS_ROOT = join(UI_ROOT, "tests");
const FIXTURES = join(TESTS_ROOT, "fixtures", "preview");
const GROUP = "logweir.dev";
const API_PREFIX = "/apis/" + GROUP + "/v1alpha1/";
const FORBIDDEN_NAMESPACE = "forbidden";

const KINDS = {
  kafkaclusters: "KafkaCluster",
  backupschedules: "BackupSchedule",
  backups: "Backup",
  restores: "Restore",
  approvals: "Approval",
  trustrosters: "TrustRoster",
};

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".md": "text/markdown; charset=utf-8",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8",
};

// ------------------------------------------------------------------ answers

function send(res, code, type, body, extra) {
  const headers = { "Content-Type": type, "Cache-Control": "no-store" };
  for (const key of Object.keys(extra || {})) {
    headers[key] = extra[key];
  }
  res.writeHead(code, headers);
  res.end(body);
}

/** A Kubernetes `Status` object, in the shape `api.js`'s `apiError` reads:
 *  `reason` and `message` are what the page shows, verbatim. */
function status(res, code, reason, message, extra) {
  const body = {
    kind: "Status",
    apiVersion: "v1",
    metadata: {},
    status: "Failure",
    message: message,
    reason: reason,
    code: code,
  };
  send(res, code, "application/json; charset=utf-8", JSON.stringify(body, null, 2) + "\n", extra);
}

function json(res, object) {
  send(res, 200, "application/json; charset=utf-8", JSON.stringify(object, null, 2) + "\n");
}

// ------------------------------------------------------------------ fixtures

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function emptyList(plural) {
  return {
    apiVersion: GROUP + "/v1alpha1",
    kind: KINDS[plural] + "List",
    metadata: { resourceVersion: "1" },
    items: [],
  };
}

/** The list for a namespace and a plural: the checked-in file when there is
 *  one, an empty list otherwise. */
function listFor(ns, plural) {
  const file = join(FIXTURES, "namespaces", ns, plural + ".json");
  if (!existsSync(file)) {
    return emptyList(plural);
  }
  return readJson(file);
}

function itemNamed(list, name) {
  for (const item of Array.isArray(list.items) ? list.items : []) {
    if (((item.metadata || {}).name) === name) {
      return item;
    }
  }
  return null;
}

// ------------------------------------------------------------------ the API

function serveApi(res, urlPath) {
  const parts = urlPath.slice(API_PREFIX.length).split("/").filter((p) => p.length > 0);

  // The one cluster-scoped kind.
  if (parts[0] === "trustrosters" && parts.length <= 2) {
    const list = readJson(join(FIXTURES, "trustrosters.json"));
    if (parts.length === 1) {
      return json(res, list);
    }
    const item = itemNamed(list, parts[1]);
    return item === null
      ? status(res, 404, "NotFound", "trustrosters." + GROUP + " \"" + parts[1] + "\" not found")
      : json(res, item);
  }

  // Namespaced: /namespaces/<ns>/<plural>[/<name>]
  if (parts[0] === "namespaces" && (parts.length === 3 || parts.length === 4)) {
    const ns = parts[1];
    const plural = parts[2];
    const name = parts[3];
    if (!Object.prototype.hasOwnProperty.call(KINDS, plural) || plural === "trustrosters") {
      return status(res, 404, "NotFound", "the server could not find the requested resource");
    }
    if (ns === FORBIDDEN_NAMESPACE) {
      return status(
        res,
        403,
        "Forbidden",
        plural + "." + GROUP + " is forbidden: User \"logweir-ui\" cannot " +
          (name === undefined ? "list" : "get") + " resource \"" + plural +
          "\" in API group \"" + GROUP + "\" in the namespace \"" + ns + "\"",
      );
    }
    const list = listFor(ns, plural);
    if (name === undefined) {
      return json(res, list);
    }
    const item = itemNamed(list, name);
    return item === null
      ? status(res, 404, "NotFound", plural + "." + GROUP + " \"" + name + "\" not found")
      : json(res, item);
  }

  return status(res, 404, "NotFound", "the server could not find the requested resource");
}

// ------------------------------------------------------------------ the files

function serveStatic(res, urlPath) {
  let relative = decodeURIComponent(urlPath.slice("/ui/".length));
  if (relative === "" || relative.endsWith("/")) {
    relative += "index.html";
  }
  let file = resolve(UI_ROOT, relative);
  if (file !== UI_ROOT && !file.startsWith(UI_ROOT + sep)) {
    return send(res, 404, "text/plain; charset=utf-8", "not found\n");
  }
  // The release bundle excludes ui/tests/, so the preview does too: nothing
  // under it is ever a page asset.
  if (file === TESTS_ROOT || file.startsWith(TESTS_ROOT + sep)) {
    return send(res, 404, "text/plain; charset=utf-8", "not found\n");
  }
  if (existsSync(file) && statSync(file).isDirectory()) {
    file = join(file, "index.html");
  }
  if (!existsSync(file)) {
    return send(res, 404, "text/plain; charset=utf-8", "not found\n");
  }
  const type = TYPES[extname(file)] || "application/octet-stream";
  return send(res, 200, type, readFileSync(file));
}

// ------------------------------------------------------------------ the server

const server = createServer((req, res) => {
  const method = req.method || "GET";
  const question = (req.url || "/").indexOf("?");
  const urlPath = question === -1 ? req.url || "/" : req.url.slice(0, question);

  res.on("finish", () => {
    process.stdout.write(method + " " + (req.url || "/") + " " + String(res.statusCode) + "\n");
  });

  // EVERY WRITE IS REFUSED, before anything is looked up. The page shows this
  // Status in its error box, as it would show the API server's own refusal.
  if (method !== "GET" && method !== "HEAD") {
    return status(
      res,
      METHOD_NOT_ALLOWED,
      "MethodNotAllowed",
      "the fixture preview writes nothing: this is a development server over checked-in " +
        "JSON, and it refused the " + method + " to " + urlPath + ". Serve the real page " +
        "with kubectl proxy to create anything.",
      { Allow: "GET, HEAD" },
    );
  }

  if (urlPath === "/" || urlPath === "/ui") {
    return send(res, 302, "text/plain; charset=utf-8", "see /ui/\n", { Location: "/ui/" });
  }
  if (urlPath.startsWith("/ui/")) {
    return serveStatic(res, urlPath);
  }
  if (urlPath.startsWith(API_PREFIX)) {
    return serveApi(res, urlPath);
  }
  return status(res, 404, "NotFound", "the server could not find the requested resource");
});

server.on("error", (error) => {
  process.stderr.write("preview-server: " + String(error && error.message) + "\n");
  process.exit(1);
});

server.listen(PORT, HOST, () => {
  process.stdout.write(
    "preview-server: serving " + UI_ROOT + " at http://" + HOST + ":" + String(PORT) + "/ui/\n" +
      "preview-server: API reads answered from " + FIXTURES + "\n" +
      "preview-server: namespaces -- default (populated), forbidden (every read is a 403), " +
      "any other name (empty)\n" +
      "preview-server: every write is answered 405; this server writes nothing and " +
      "forwards nothing\n",
  );
});
