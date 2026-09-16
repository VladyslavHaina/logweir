// lifecycle.spec.js -- route ownership under delayed navigation.
//
// These are DOM-free integration tests of a real page mount. The fake node is
// only the tiny append/remove surface `render.replace` needs; the request and
// generation behavior is the same code the browser uses.

import { test } from "node:test";
import assert from "node:assert/strict";

import { createRouteLifecycle, namespaceContext, parseHash } from "../app.js";
import { listen } from "../lifecycle.js";
import { mountBackups } from "../pages/backups.js";
import { mountClusters } from "../pages/clusters.js";
import {
  initialState,
  mountRestoreWizard,
  recoveryPoints,
  submitRestore,
} from "../pages/restore-wizard.js";

/** The route identity for a fixture's newest recovery point. */
function newestPoint(list) {
  const point = recoveryPoints(list)[0];
  return { uid: point.metadata.uid, backup: point.metadata.name };
}
import { readFileSync } from "node:fs";

const fixture = (name) => JSON.parse(readFileSync(new URL("./fixtures/" + name, import.meta.url)));

function node() {
  return {
    children: [],
    get firstChild() {
      return this.children.length === 0 ? null : this.children[0];
    },
    removeChild(child) {
      assert.equal(child, this.children[0]);
      return this.children.shift();
    },
    appendChild(child) {
      this.children.push(child);
      return child;
    },
  };
}

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise: promise, resolve: resolve };
}

function response(items) {
  return { ok: true, status: 200, text: () => Promise.resolve(JSON.stringify({ items: items })) };
}

test("a delayed namespace A response cannot overwrite namespace B", async () => {
  const original = globalThis.fetch;
  const requests = [];
  globalThis.fetch = (u, init) => {
    const next = deferred();
    requests.push({ u: u, init: init, next: next });
    return next.promise;
  };
  try {
    const routes = createRouteLifecycle();
    const target = node();
    const parse = (html) => [{ html: html }];
    const a = routes.begin();
    const aMount = mountBackups(target, "team-a", parse, a);
    const b = routes.begin();
    const bMount = mountBackups(target, "team-b", parse, b);

    assert.equal(requests[0].init.signal.aborted, true, "B aborts A's view-owned GET");
    requests[1].next.resolve(response([{ metadata: { name: "from-b" }, status: {} }]));
    await bMount;
    requests[0].next.resolve(response([{ metadata: { name: "from-a" }, status: {} }]));
    await aMount;

    assert.equal(target.children.length, 1);
    assert.match(target.children[0].html, /from-b/);
    assert.doesNotMatch(target.children[0].html, /from-a/);
  } finally {
    globalThis.fetch = original;
  }
});

test("rapid routes, back-forward, and unmount invalidate every old callback", () => {
  const routes = createRouteLifecycle();
  const first = routes.begin();
  const second = routes.begin();
  const third = routes.begin(); // the browser's back/forward hashchange
  assert.equal(first.isCurrent(), false);
  assert.equal(second.isCurrent(), false);
  assert.equal(third.isCurrent(), true);
  assert.equal(first.signal.aborted, true);
  assert.equal(second.signal.aborted, true);
  routes.dispose(); // pagehide/unmount
  assert.equal(third.signal.aborted, true);
  assert.equal(third.isCurrent(), false);
});

test("a hash changes route ownership before its hashchange event runs", () => {
  const originalWindow = globalThis.window;
  globalThis.window = { location: { hash: "#/restore?ns=team-old" } };
  try {
    const routes = createRouteLifecycle();
    const restore = routes.begin(window.location.hash);
    window.location.hash = "#/backups?ns=team-new";
    assert.equal(restore.isCurrent(), false, "the synchronous URL change disarms pending client preparation");
  } finally {
    globalThis.window = originalWindow;
  }
});

test("route exit removes DOM subscriptions as well as aborting reads", () => {
  const routes = createRouteLifecycle();
  const oldView = routes.begin();
  const target = new EventTarget();
  let calls = 0;
  listen(target, "change", () => { calls += 1; }, oldView);
  routes.begin();
  target.dispatchEvent(new Event("change"));
  assert.equal(calls, 0, "the old listener was removed by its aborted signal");
});

test("a stale form callback cannot submit into its former namespace", async () => {
  const original = globalThis.fetch;
  const seen = [];
  globalThis.fetch = (u, init) => {
    seen.push({ u: u, init: init });
    return Promise.resolve(response([]));
  };
  try {
    let submit;
    const form = {
      elements: {
        name: { value: "old-cluster" },
        servers: { value: "broker:9092" },
        role: { value: "source" },
        mode: { value: "plaintext" },
        username: { value: "" },
        secret: { value: "" },
        tls: { checked: false },
      },
      addEventListener(type, handler) {
        if (type === "submit") {
          submit = handler;
        }
      },
    };
    const target = node();
    target.querySelector = (selector) => selector === "#cluster-form" ? form : null;
    const routes = createRouteLifecycle();
    const oldView = routes.begin();
    await mountClusters(target, "old-ns", (html) => [{ html: html }], oldView);
    routes.begin();
    await submit({ preventDefault() {} });
    assert.equal(seen.length, 1, "only the old view's GET ran; no stale POST was sent");
    assert.match(seen[0].u, /namespaces\/old-ns\/kafkaclusters$/);
  } finally {
    globalThis.fetch = original;
  }
});

test("restore preparation never renders or posts after its route has left", async () => {
  const target = node();
  let checks = 0;
  const renderThenLeave = {
    signal: new AbortController().signal,
    isCurrent() {
      checks += 1;
      return checks === 1;
    },
  };
  const api = {
    list: async (ns, plural) => plural === "kafkaclusters"
      ? fixture("wizard-clusters.json")
      : fixture("wizard-backups.json"),
    create: async () => assert.fail("a stale preparation must not reach create"),
  };
  await mountRestoreWizard(
    target,
    "team-old",
    newestPoint(fixture("wizard-backups.json")),
    (html) => [{ html }],
    api,
    renderThenLeave,
  );
  assert.equal(target.children.length, 0, "an async digest cannot replace a newer route");

  const state = initialState(
    "team-old",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups.json"),
    newestPoint(fixture("wizard-backups.json")),
  );
  let posts = 0;
  const result = await submitRestore(
    state,
    { create: async () => { posts += 1; } },
    { isCurrent: () => false },
  );
  assert.equal(result, null, "a route that left during client preparation has no submission result");
  assert.equal(posts, 0, "a route that left during client preparation posts to no old namespace");
});

test("a namespace exists only when supplied by an explicit route or installation context", () => {
  assert.equal(parseHash("#/backups").ns, "");
  assert.equal(parseHash("#/backups", "installed-ns").ns, "installed-ns");
  assert.equal(parseHash("#/backups?ns=chosen-ns", "installed-ns").ns, "chosen-ns");

  const configured = namespaceContext({
    getAttribute(name) {
      return name === "data-logweir-namespaces" ? "team-a, team-b" : "team-b";
    },
  });
  assert.deepEqual(configured, { allowed: ["team-a", "team-b"], selected: "team-b" });
  assert.deepEqual(
    namespaceContext({ getAttribute: (name) => name === "data-logweir-namespaces" ? "team-a" : "" }),
    { allowed: ["team-a"], selected: "team-a" },
    "a one-namespace installation needs no cluster-wide namespace list",
  );
});
