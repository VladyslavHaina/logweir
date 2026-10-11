// api.spec.js -- the behaviour arm of the UI's mechanical claim that every
// identifier `api.js` builds is relative.
//
// Run with `node --test 'ui/tests/*.spec.js'` from `logweir/` (as `just lint`
// does through `scripts/check-ui-behaviour.sh`). Node is a test runner here
// and nothing else: it builds no asset, fetches no package, and there is no
// `package.json` for it to read.
//
// THIS DIRECTORY IS OUTSIDE `check-ui-offline.sh`'s SCOPE, on purpose. A test
// that proves `path(...)` refuses an absolute URL has to contain one, and a
// gate that forbade the string here would make the test unwritable. The
// release artefact excludes this directory, so nothing here is ever served.
//
// NOTHING HERE REACHES THE NETWORK.

import { test } from "node:test";
import assert from "node:assert/strict";

import { path } from "../api.js";

test("path builds a relative identifier from its segments", () => {
  assert.equal(path("apis", "logweir.dev", "v1alpha1"), "/apis/logweir.dev/v1alpha1");
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
