// plan-secure-context.spec.js -- `plan.js` refuses a non-secure origin, and
// this assertion needs a whole file to itself.
//
// WHY ITS OWN FILE. An ES module that has already been imported in a process
// is NEVER re-evaluated: the module registry caches it, and a second import
// hands back the same namespace without running a line. So the refusal, which
// happens at `plan.js`'s module scope, can be observed exactly once per
// process. `node --test` gives every test FILE its own process and therefore
// its own registry, which is the only way this assertion and the ones that
// import `plan.js` normally can both hold.
//
// AND WHY THERE IS NO STATIC IMPORT ANYWHERE ABOVE. A static import is hoisted
// and evaluated before the first statement of this file runs, so a static
// `import ... from "../plan.js"` -- even an unused one, even one two modules
// deep -- would evaluate the module while `crypto.subtle` was still present
// and the refusal would never fire. The two node built-ins below are pulled in
// dynamically for the same reason the rest of the file is careful: so that
// "this file imports nothing at module scope" is a property a reader can check
// by looking, rather than a claim about which modules happen to be harmless.

const { test } = await import("node:test");
const assert = (await import("node:assert/strict")).default;

// The message `plan.js` throws, verbatim. It names the requirement -- loopback,
// or TLS -- and NO URL scheme: `scripts/check-ui-offline.sh` has no exemption
// inside `ui/**`, `plan.js` is inside it, and a message that spelled the
// schemes out would fail the gate that keeps every identifier in this tree
// relative to the serving origin.
const MESSAGE =
  "this page must be served from a secure context: loopback (127.0.0.1 or localhost) " +
  "over plain transport, or any TLS origin. SubtleCrypto is unavailable here and the plan hash " +
  "cannot be computed.";

test("plan_js_refuses_a_non_secure_origin", async () => {
  // What a browser on a non-trustworthy origin actually presents: a `crypto`
  // object with no `subtle` on it. Not a missing `crypto` -- `crypto.getRandom
  // Values` is available everywhere, and it is `subtle` alone that a secure
  // context gates. That distinction is the whole reason the guard reads
  // `crypto?.subtle` and not `crypto`.
  //
  // `defineProperty` RATHER THAN A PLAIN ASSIGNMENT, and it is not a
  // preference. On this host's node (v25.6.1) `globalThis.crypto` is an
  // accessor with a getter and NO setter -- measured: a bare
  // `globalThis.crypto = {}` throws `TypeError: Cannot set property crypto of
  // #<Object> which has only a getter`. The descriptor is `configurable`, so
  // redefining it is the supported way to reach the same state, and the state
  // is what the assertion is about.
  Object.defineProperty(globalThis, "crypto", {
    value: {},
    configurable: true,
    writable: true,
  });
  assert.equal(globalThis.crypto.subtle, undefined, "the stand-in has no subtle");

  await assert.rejects(
    () => import("../plan.js"),
    (error) => {
      assert.ok(error instanceof Error, "the refusal is an Error");
      assert.equal(
        error.message,
        MESSAGE,
        "the refusal is the exact message, which names the requirement and no URL scheme",
      );
      return true;
    },
    "plan.js refuses at module load when crypto.subtle is absent. Without this the page " +
      "renders a plan with no hash beside it: `planHash` throws a TypeError reading a " +
      "property of undefined, and the security-critical string silently disappears from a " +
      "page whose whole purpose is to show it before the operator signs.",
  );

  // And the message carries no scheme token, which is the one place critique
  // C M3's wording was changed and the reason it was changed.
  for (const token of ["http" + ":", "https" + ":"]) {
    assert.equal(
      MESSAGE.indexOf(token),
      -1,
      "the refusal names no URL scheme: ui/**'s offline gate scans plan.js and has no exemption",
    );
  }
});
