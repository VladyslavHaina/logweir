// check-follow.spec.js -- defect P8 (poc-install, 2026-09-24): a page that
// starts a `Preflight` must READ it back before anything on screen is a
// verdict.
//
// WHAT THE LIVE ROUND SAW. Destinations -> Test access created
// `pf-nunjq5gx6ndxowp4kuklihjkgs`, which recorded `ready` in about three
// seconds; the page kept "pending / does not apply to your current inputs /
// compared: nothing" for four minutes, because it rendered the CREATE answer
// and never asked again. The schedule form's "Backup readiness" re-click showed
// a REPLAYED check as "ready / does not apply ... could not be checked: this
// response did not recompute staleness", because its follower stopped at a
// create answer that was already terminal. Both are one rule: the product API
// projects a create (and a replay) without recomputing staleness, so the first
// read is owed whatever the create said.
//
// Every row carries its NEGATIVE CONTROL: the assertion that fails on the code
// before this fix (no read at all, or a follower that stopped at a terminal
// create answer).

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { decodeConsoleItem } from "../contract.js";
import { owesRead } from "../lifecycle.js";
import {
  DESTINATION_TEST_POLLS,
  DESTINATION_TEST_STOPPED_SENTENCE,
  mountDestinationDetail,
  renderDestinationDetail,
} from "../pages/destinations.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

const console_ = (name) =>
  JSON.parse(readFileSync(new URL("./fixtures/console/" + name, import.meta.url), "utf8"));
const destination = () => decodeConsoleItem("destinations", console_("destination.json")).value.item;

const settled = async (turns) => {
  for (let i = 0; i < (turns || 10); i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
};

/** A check as the product API answers a CREATE: not recomputed. */
function created(id, state, terminal) {
  return {
    id: id, namespace: "team-a", uid: "uid-" + id, resourceVersion: "1",
    operation: "destinationAccess", state: state, terminal: terminal, reason: null,
    binding: { referents: [] }, applicable: false, stale: terminal,
    staleReasons: terminal
      ? [{ reason: "unverifiable",
        basis: "this response did not recompute staleness; read the preflight itself for a " +
          "current verdict" }]
      : [],
    staleBasis: [], checks: [], warnings: [], executionOnly: [], detailsAvailable: false,
    conditions: [],
  };
}

/** The same check as a READ answers it: recomputed, and terminal. */
function read(id) {
  return Object.assign(created(id, "ready", true), {
    applicable: true, stale: false, staleReasons: [], staleBasis: ["expiry", "referents"],
    observedAt: "2026-09-24T15:12:02Z",
    checks: [{ id: "destination.archiveWrite", state: "ready", gating: "blocking",
      code: "Ready", message: "put and delete of a marker object succeeded" }],
  });
}

test("owes_read_is_owed_once_after_any_create_answer_and_then_until_a_terminal_read", () => {
  // THE ONE RULE, stated on the helper every follower uses.
  const replay = created("pf-r", "ready", true);
  assert.equal(owesRead(replay, 0), true,
    "a TERMINAL create answer (a replay) still owes its first read: it was not recomputed");
  assert.equal(owesRead(replay, 1), false, "and a terminal READ ends the follow");
  assert.equal(owesRead(created("pf-p", "pending", false), 0), true);
  assert.equal(owesRead(created("pf-p", "running", false), 7), true,
    "a check still running after a read is read again");
  assert.equal(owesRead(null, 0), false, "no check, nothing to read");
  assert.equal(owesRead({ id: "" }, 0), false, "a check with no id cannot be read");
});

function destinationApi(options) {
  const o = options || {};
  const calls = { tests: [], reads: 0 };
  return {
    calls: calls,
    destination: async () => ({ item: o.item || destination(), unknown: [] }),
    destinationUsage: async () => ({ schedules: [], backups: [], truncated: false, basis: "labels" }),
    testDestination: async (ns, name, request, extra) => {
      calls.tests.push({ ns: ns, name: name, request: request, attempt: (extra || {}).attempt });
      return { item: o.started(calls.tests.length), replayed: false };
    },
    preflight: async (ns, id) => {
      calls.reads += 1;
      return { item: o.read(id, calls.reads) };
    },
    wait: async () => {},
  };
}

test("destination_test_access_follows_the_started_check_until_a_read_is_terminal", async () => {
  const api = destinationApi({
    started: () => created("pf-t", "pending", false),
    read: (id, n) => (n < 2 ? created(id, "running", false) : read(id)),
  });
  const view = fakeView();
  await mountDestinationDetail(view.root, "team-a", "primary", parse, LIFE(), api);
  const form = view.find("#destination-test-form");
  assert.ok(form !== null, "the page offers Test access");
  await form.dispatch("submit");
  await settled(20);

  assert.equal(api.calls.tests.length, 1, "one click, one test");
  assert.equal(api.calls.reads, 2,
    "NEGATIVE CONTROL: the page read the started check back until it was terminal (the code " +
      "before P8 read it zero times and painted the create answer for ever)");
  const html = view.html();
  assert.match(html, /pf-t/);
  assert.match(html, /badge-green">ready/, "the verdict the check recorded, not `pending`");
  assert.match(html, /applies to your current inputs/);
  assert.doesNotMatch(html, /did not recompute staleness/);
  assert.doesNotMatch(html, /id="destination-no-test"/,
    "the pre-test summary ('No access test has been recorded') stands down once the page holds " +
      "a test of its own");
  assert.doesNotMatch(html, /id="destination-test-stopped"/);
});

test("a_replayed_destination_test_is_read_before_it_is_believed", async () => {
  const api = destinationApi({
    started: () => created("pf-replayed", "ready", true),
    read: (id) => read(id),
  });
  const view = fakeView();
  await mountDestinationDetail(view.root, "team-a", "primary", parse, LIFE(), api);
  await view.find("#destination-test-form").dispatch("submit");
  await settled(20);
  assert.equal(api.calls.reads, 1,
    "NEGATIVE CONTROL: a terminal create answer is read ONCE, then believed");
  assert.doesNotMatch(view.html(), /did not recompute staleness/,
    "the recomputed read replaced the create answer's 'could not be checked'");
});

test("two_deliberate_destination_tests_are_two_checks_not_a_replay", async () => {
  // REVIEW F1's rule, for this control: the key was the destination and its
  // roles alone, so a second press after fixing a Secret REPLAYED the first
  // test's verdict. Each accepted click now carries its own attempt token.
  const api = destinationApi({
    started: (n) => created("pf-" + n, "ready", true),
    read: (id) => read(id),
  });
  const view = fakeView();
  await mountDestinationDetail(view.root, "team-a", "primary", parse, LIFE(), api);
  await view.find("#destination-test-form").dispatch("submit");
  await settled(20);
  await view.find("#destination-test-form").dispatch("submit");
  await settled(20);
  assert.equal(api.calls.tests.length, 2);
  const [first, second] = api.calls.tests.map((t) => t.attempt);
  assert.ok(typeof first === "string" && first.length > 0, "a click carries a token");
  assert.notEqual(first, second, "and a second click carries another");
  assert.match(view.html(), /pf-2/, "the newer test is the one on screen");
});

test("a_destination_test_that_never_settles_is_left_alone_and_the_panel_says_so", async () => {
  const api = destinationApi({
    started: () => created("pf-slow", "pending", false),
    read: (id) => created(id, "running", false),
  });
  const view = fakeView();
  await mountDestinationDetail(view.root, "team-a", "primary", parse, LIFE(), api);
  await view.find("#destination-test-form").dispatch("submit");
  await settled(DESTINATION_TEST_POLLS * 4 + 20);
  assert.equal(api.calls.reads, DESTINATION_TEST_POLLS, "the budget, and not one read more");
  assert.match(view.html(), /id="destination-test-stopped"/);
  assert.ok(view.html().includes("was not cancelled"));
  assert.match(DESTINATION_TEST_STOPPED_SENTENCE, /reload this page/);
});

test("a_reload_renders_the_last_test_the_destination_read_carries", () => {
  // THE RELOAD HALF OF P8: `GET .../destinations/{name}` answers `lastTest`,
  // and the detail must render it when this page holds no test of its own.
  const doc = console_("destination.json");
  doc.item.lastTest = {
    preflightId: "pf-nunjq5gx6ndxowp4kuklihjkgs", state: "ready",
    observedAt: "2026-09-24T15:12:02.585883510Z", stale: false, truncated: false,
  };
  const item = decodeConsoleItem("destinations", doc).value.item;
  const html = renderDestinationDetail(item, { mayOperate: true });
  assert.match(html, /id="destination-last-test"/);
  assert.match(html, /pf-nunjq5gx6ndxowp4kuklihjkgs/);
  assert.match(html, /badge-green">ready/);
  assert.doesNotMatch(html, /id="destination-no-test"/,
    "NEGATIVE CONTROL: a recorded test is never reported as none");
});
