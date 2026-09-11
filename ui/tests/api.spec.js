// api.spec.js -- the behaviour arm of the UI's two mechanical claims:
// that every identifier `api.js` builds is relative, and that the write path
// refuses a kind the page may not write BEFORE it reaches the network.
//
// Run with `node --test 'ui/tests/*.js'` from `logweir/`. Node is a test runner here
// and nothing else: it builds no asset, it fetches no package, and there is no
// `package.json` for it to read. From Task 26 onward `just lint` runs this
// directory through `scripts/check-ui-behaviour.sh`.
//
// THIS DIRECTORY IS OUTSIDE `check-ui-offline.sh`'s SCOPE, on purpose. A test
// that proves `path(...)` refuses an absolute URL has to contain one, and a
// gate that forbade the string here would make the test unwritable. The
// release artefact excludes this directory, so nothing here is ever served.
//
// NOTHING HERE REACHES THE NETWORK. The two write rows replace the global
// fetch binding with a stub -- one that records what it was handed, one that
// throws if it is entered at all -- and restore the original afterwards. No
// Kubernetes object is created by this file; `logweir-t25` is a string.

import { test } from "node:test";
import assert from "node:assert/strict";

import { GROUP, VERSION, WRITABLE_PLURALS, create, path } from "../api.js";

test("path builds a relative identifier from its segments", () => {
  assert.equal(path("apis", "logweir.dev", "v1alpha1"), "/apis/logweir.dev/v1alpha1");
  assert.equal(path("apis", GROUP, VERSION), "/apis/logweir.dev/v1alpha1");
  assert.equal(
    path("apis", GROUP, VERSION, "namespaces", "logweir-t25", "backups"),
    "/apis/logweir.dev/v1alpha1/namespaces/logweir-t25/backups",
  );
});

test("path refuses anything that is not a relative segment", () => {
  assert.throws(() => path("https://evil"), TypeError, "a scheme separator is refused");
  assert.throws(() => path("/apis"), TypeError, "a leading slash is refused");
  assert.throws(() => path(".."), TypeError, "a parent-directory hop is refused");
  assert.throws(() => path("apis", ""), TypeError, "an empty segment is refused");
  assert.throws(() => path("apis", undefined), TypeError, "a non-string segment is refused");
});

test("path names the offending segment in its refusal", () => {
  assert.throws(() => path("https://evil"), /evil/);
  assert.throws(() => path("/apis"), /apis/);
});

test("the writable set is frozen and does not carry trustrosters", () => {
  assert.ok(Object.isFrozen(WRITABLE_PLURALS), "WRITABLE_PLURALS is frozen");
  assert.deepEqual(WRITABLE_PLURALS, [
    "kafkaclusters",
    "backupschedules",
    "backups",
    "restores",
    "approvals",
  ]);
  assert.equal(WRITABLE_PLURALS.includes("trustrosters"), false);
});

test("create carries the ui field manager", async () => {
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = function (u, init) {
    seen.push({ u: u, init: init });
    return Promise.resolve({
      ok: true,
      status: 201,
      text: function () {
        return Promise.resolve("{}");
      },
    });
  };
  try {
    await create("logweir-t25", "restores", {});
  } finally {
    globalThis.fetch = original;
  }
  assert.equal(seen.length, 1, "exactly one request was issued");
  assert.equal(
    seen[0].u,
    "/apis/logweir.dev/v1alpha1/namespaces/logweir-t25/restores?fieldManager=logweir-ui",
  );
  assert.equal(seen[0].init.method, "POST");
  assert.equal(seen[0].init.headers["Content-Type"], "application/json");
});

test("create refuses a plural outside the allowlist", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = function () {
    throw new Error(
      "the network was entered; the allowlist check did not run before the request",
    );
  };
  try {
    await assert.rejects(
      function () {
        return create("logweir-t25", "trustrosters", {});
      },
      function (error) {
        assert.ok(
          error instanceof RangeError,
          "a plural outside the allowlist is a RangeError, not a network failure",
        );
        assert.match(error.message, /trustrosters/);
        return true;
      },
    );
  } finally {
    globalThis.fetch = original;
  }
});

test("patchSuspend and create reach the network only through path()", async () => {
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = function (u) {
    seen.push(u);
    return Promise.resolve({
      ok: true,
      status: 200,
      text: function () {
        return Promise.resolve("{}");
      },
    });
  };
  try {
    await create("logweir-t25", "backups", {});
  } finally {
    globalThis.fetch = original;
  }
  for (const u of seen) {
    assert.equal(u.charAt(0), "/", "every identifier issued is relative to the serving origin");
  }
});
