// check-deadline.spec.js -- poc-upgrade-3's P15 and its class: a page that
// follows a check keeps reading it until the check's OWN deadline, backs off
// while it does, and -- when that deadline passes without a result -- says so
// and offers "Run the check again". It never leaves a check on "this page
// reads it again until then" with nothing reading it.
//
// WHAT THE LIVE ROUND SAW. Schedules -> Backup readiness renewed an expired
// check (`pf-gjepy6av...`) and read it twenty times, two seconds apart, all
// `200` -- its whole forty-second budget -- and stopped. The check completed
// sixty-four seconds after it was created. The panel kept "The check has not
// finished ... this page reads it again until then" for as long as it stayed
// open. Every follower in the console had the same shape: Test connection 30 s,
// the schedule form 40 s, Test access 60 s, restore step 5 90 s -- under a
// `Preflight` whose own budget is `timeoutSeconds` (120 by default, up to 600)
// plus the 90 s its Job is given to start. Topic discovery had no follower at
// all: "Discover topics" painted `pending` and left it there until a reload.
//
// THE CLOCK IS node:test's MOCK TIMERS (`setTimeout` and `Date`). No page here
// is handed a `wait`: each follow runs on the timer a browser gives it, and a
// check that settles at 100 s is 100 s of mocked time. `setImmediate` stays
// real, and it is what the rows flush promise continuations with.
//
// Every row carries its NEGATIVE CONTROL: an assertion that fails on the code
// before this fix (a 30-90 s budget, a follow that stops without a word, a
// "Run the check again" that replays the check that did not finish).

import { mock, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { resetMode } from "../client.js";
import { createRouteLifecycle } from "../app.js";
import {
  CHECK_JOB_MARGIN_SECONDS,
  DISCOVERY_FOLLOW_MS,
  DISCOVERY_TIMEOUT_CEILING_SECONDS,
  FOLLOW_DEADLINE,
  FOLLOW_FAILURES_TOLERATED,
  FOLLOW_MAX_GAP_MS,
  FOLLOW_UNREADABLE,
  PREFLIGHT_FOLLOW_MS,
  PREFLIGHT_TIMEOUT_CEILING_SECONDS,
  discoverySpent,
  dropDraft,
  followCheck,
  followGap,
  formKey,
  isFollowed,
  keepDraft,
  keepStopMark,
  preflightSpent,
  resetCheckIntents,
} from "../lifecycle.js";
import {
  CHECKING_SENTENCE,
  CHECK_UNFINISHED_SENTENCE,
  CHECK_UNREADABLE_SENTENCE,
  checkStoppedBlock,
} from "../render.js";
import { SCHEDULE_DRAFT_FIELDS, SCHEDULE_FORM, mountSchedules } from "../pages/schedules.js";
import { mountClusterDetail } from "../pages/clusters.js";
import { mountDestinationDetail } from "../pages/destinations.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

const REPO = fileURLToPath(new URL("../../", import.meta.url));
const source = (path) => readFileSync(REPO + path, "utf8");

// ------------------------------------------------------------------ the clock

/** Runs `body` with `setTimeout` and `Date` mocked from zero, always undone. */
function clocked(body) {
  return async () => {
    mock.timers.enable({ apis: ["setTimeout", "Date"], now: 0 });
    try {
      await body();
    } finally {
      mock.timers.reset();
      resetMode();
      resetCheckIntents();
    }
  };
}

/** Lets every promise continuation that is ready run (real `setImmediate`). */
async function flush(turns) {
  for (let i = 0; i < (turns || 12); i += 1) {
    await new Promise((resolve) => { setImmediate(resolve); });
  }
}

/** Moves the mocked clock `ms` forward in small steps, flushing between them,
 *  so each follow's read and repaint happens at the time it was due. */
async function advance(ms) {
  const step = 250;
  for (let moved = 0; moved < ms; moved += step) {
    mock.timers.tick(Math.min(step, ms - moved));
    await flush();
  }
}

const SETTLES_AT_MS = 100 * 1000;

// ---------------------------------------------------------------- the objects

/** A `Preflight` as the product API answers it. */
function preflight(id, state, terminal, over) {
  const ready = terminal === true && state === "ready";
  return Object.assign({
    id: id, namespace: "team-a", uid: "uid-" + id, resourceVersion: "1", operation: "backup",
    state: state, terminal: terminal, binding: { referents: [] }, applicable: ready,
    stale: false, staleReasons: [], staleBasis: ready ? ["expiry", "referents"] : [],
    checks: [], warnings: [], executionOnly: [], detailsAvailable: false, conditions: [],
  }, over || {});
}

/** The same check read at the mocked instant now: running until
 *  `SETTLES_AT_MS`, then ready -- or never, when `never` is set. */
function readAt(id, never) {
  return never !== true && Date.now() >= SETTLES_AT_MS
    ? preflight(id, "ready", true, { observedAt: "2026-09-25T13:02:34Z" })
    : preflight(id, "running", false);
}

/** A `TopicDiscovery` as the product API answers it. */
function discovery(id, state, terminal) {
  return {
    id: id, namespace: "team-a", uid: "uid-" + id, resourceVersion: "1",
    connection: { name: "orders-prod", principal: "logweir" },
    state: state, terminal: terminal, stale: false, staleReasons: [], truncated: false,
    chunkCount: terminal ? 1 : 0, conditions: [], visibility: { state: "unknown" },
    counts: terminal ? { listed: 3, returned: 3, internalExcluded: 0, errored: 0 } : undefined,
  };
}

/** Reads the newest paint of the element whose markup contains `marker`. */
function newest(view, marker) {
  const chunks = view.chunks.filter((c) => c.html.includes(marker));
  return chunks.length === 0 ? "" : chunks[chunks.length - 1].html;
}

// ===========================================================================
// The deadline is the product's own, and the follow is polite
// ===========================================================================

test("the_follow_deadline_covers_the_longest_check_the_product_accepts", () => {
  // THE BUDGET IS PINNED TO THE PRODUCT'S NUMBERS, READ FROM THE PRODUCT. A
  // console constant that drifted below them is the defect this file is about.
  const api = source("crates/logweir-api/src/routes/preflights.rs");
  const accepted = /if !\((\d+)\.\.=(\d+)\)\.contains\(&timeout\)/.exec(api);
  assert.ok(accepted !== null, "the product API's timeoutSeconds range is where this row reads it");
  const crd = source("crates/weirkeeper/src/crds/preflight.rs");
  const schema = /#\[schemars\(range\(min = (\d+), max = (\d+)\)\)\]\s*pub timeout_seconds/.exec(crd);
  assert.ok(schema !== null, "the CRD's timeoutSeconds range is where this row reads it");
  const byDefault = /pub const DEFAULT_TIMEOUT_SECONDS: i32 = (\d+);/.exec(crd);
  const job = source("crates/weirkeeper/src/check/job.rs");
  const margin = /pub const DEADLINE_MARGIN_SECONDS: i64 = (\d+);/.exec(job);
  const td = source("crates/logweir-api/src/routes/topic_discoveries.rs");
  const tdMax = /pub const MAX_TIMEOUT_SECONDS: i32 = (\d+);/.exec(td);
  assert.ok(byDefault !== null && margin !== null && tdMax !== null);

  const most = Math.max(Number(accepted[2]), Number(schema[2]));
  assert.equal(PREFLIGHT_TIMEOUT_CEILING_SECONDS, most, "the ceiling IS the product's largest");
  assert.equal(CHECK_JOB_MARGIN_SECONDS, Number(margin[1]), "the margin IS the Job's");
  assert.equal(DISCOVERY_TIMEOUT_CEILING_SECONDS, Number(tdMax[1]));
  assert.ok(PREFLIGHT_FOLLOW_MS >= (most + Number(margin[1])) * 1000,
    "NEGATIVE CONTROL: a follow shorter than the longest check the product runs");
  assert.ok(PREFLIGHT_FOLLOW_MS > (Number(byDefault[1]) + Number(margin[1])) * 1000,
    "and certainly longer than a default check's own Job deadline");
  assert.ok(DISCOVERY_FOLLOW_MS >= (Number(tdMax[1]) + Number(margin[1])) * 1000);
  // THE LIVE CASE, by name: 64 s, and the 100 s the rows below settle at.
  assert.ok(PREFLIGHT_FOLLOW_MS > 64000 && PREFLIGHT_FOLLOW_MS > SETTLES_AT_MS);
});

test("the_follow_backs_off_and_never_hot_loops", () => {
  // THE OLD CADENCE FOR THE FIRST TWENTY SECONDS (review L8): ten reads two
  // seconds apart, so a check settling in that time is seen as fast as before;
  // then 3, 4.5, 6.75 and 10 s.
  const gaps = [0, 1, 5, 9, 10, 11, 12, 13, 50].map(followGap);
  assert.deepEqual(gaps, [2000, 2000, 2000, 2000, 3000, 4500, 6750, 10000, 10000]);
  let steady = 0;
  for (let n = 0; followGap(n) === 2000; n += 1) {
    steady += 2000;
  }
  assert.equal(steady, 20000, "NEGATIVE CONTROL: two-second reads for the first twenty seconds");
  let reads = 0;
  for (let waited = 0; waited < PREFLIGHT_FOLLOW_MS; reads += 1) {
    const gap = followGap(reads);
    assert.ok(gap >= 2000 && gap <= FOLLOW_MAX_GAP_MS, "every gap is 2-10 s: " + gap);
    waited += gap;
  }
  assert.ok(reads < 100, "NEGATIVE CONTROL: twelve minutes cost " + reads + " reads, not hundreds");
});

test("the_stop_reasons_agree_between_the_follow_and_the_renderer", () => {
  // `render.js` imports nothing, so it spells the two reasons itself.
  const block = (why) => checkStoppedBlock({ id: "pf-x", followStopped: why });
  assert.match(block(FOLLOW_DEADLINE), /data-check-stopped="deadline"/);
  assert.ok(block(FOLLOW_DEADLINE).includes(CHECK_UNFINISHED_SENTENCE));
  assert.match(block(FOLLOW_UNREADABLE), /data-check-stopped="unreadable"/);
  assert.ok(block(FOLLOW_UNREADABLE).includes(CHECK_UNREADABLE_SENTENCE));
  assert.match(block(FOLLOW_DEADLINE), /<button type="button" class="check-retry" data-check="pf-x">Run the check again<\/button>/);
  assert.equal(checkStoppedBlock({ id: "pf-x" }), "", "a check still followed carries no block");
  assert.match(checkStoppedBlock({ id: "td-x", followStopped: FOLLOW_DEADLINE }, "discovery"),
    /The discovery did not finish in the longest time a discovery may take.*Run the discovery again<\/button>/);
});

// ===========================================================================
// The follow itself, on the mocked clock
// ===========================================================================

test("a_check_settling_at_100_s_is_followed_until_it_settles", clocked(async () => {
  const reads = [];
  const shown = [];
  const ended = followCheck({
    first: preflight("pf-slow", "pending", false),
    read: async (current) => { reads.push(Date.now()); return readAt(current.id); },
    show: (check) => { shown.push(check); },
  });
  await advance(90 * 1000);
  assert.ok(reads.length > 0 && shown[shown.length - 1].terminal === false,
    "still following at 90 s: the old budgets (30-90 s) had all given up by now");
  await advance(20 * 1000);
  const result = await ended;
  assert.equal(result.state, "ready", "NEGATIVE CONTROL: the verdict arrived and was held");
  assert.equal(shown[shown.length - 1].state, "ready", "and it was shown");
  const settledAt = reads[reads.length - 1];
  assert.ok(settledAt >= SETTLES_AT_MS && settledAt < SETTLES_AT_MS + FOLLOW_MAX_GAP_MS + 1,
    "read within one gap of settling: " + settledAt);
  const count = reads.length;
  await advance(60 * 1000);
  assert.equal(reads.length, count, "and never read again after a terminal read");
}));

test("a_check_with_no_result_by_its_deadline_is_marked_and_read_no_more", clocked(async () => {
  let reads = 0;
  const shown = [];
  const ended = followCheck({
    first: preflight("pf-never", "pending", false),
    read: async (current) => { reads += 1; return readAt(current.id, true); },
    show: (check) => { shown.push(check); },
  });
  await advance(PREFLIGHT_FOLLOW_MS - 5000);
  assert.equal(shown.some((c) => c.followStopped !== undefined), false,
    "not a second before the deadline: the follow is still reading");
  await advance(10 * 1000);
  const result = await ended;
  assert.equal(result.followStopped, FOLLOW_DEADLINE,
    "NEGATIVE CONTROL: the deadline ends the follow in words, not in silence");
  assert.equal(shown[shown.length - 1].followStopped, FOLLOW_DEADLINE, "and that is what is shown");
  assert.ok(reads < 100, "backed off: " + reads + " reads over twelve minutes");
  const count = reads;
  await advance(60 * 1000);
  assert.equal(reads, count, "nothing reads it after the deadline");
  assert.equal(preflightSpent(result), true,
    "a check the page stopped following is spent, so asking again is a NEW check");
  assert.equal(discoverySpent(Object.assign({}, discovery("td-x", "running", false),
    { followStopped: FOLLOW_DEADLINE })), true);
  assert.equal(keepStopMark(result, preflight("pf-never", "running", false)).followStopped,
    FOLLOW_DEADLINE, "a later read with no result keeps the mark");
  assert.equal(keepStopMark(result, readAt("pf-never")).followStopped, undefined,
    "a result replaces it");
}));

test("a_passing_read_failure_is_asked_again_and_a_refusal_stops_the_follow", clocked(async () => {
  const unavailable = Object.assign(new Error("service unavailable"), { status: 503 });
  let reads = 0;
  const shown = [];
  const blip = followCheck({
    first: preflight("pf-blip", "pending", false),
    read: async (current) => {
      reads += 1;
      if (reads <= 2) {
        throw unavailable;
      }
      return readAt(current.id);
    },
    show: (check) => { shown.push(check); },
  });
  await advance(SETTLES_AT_MS + 20 * 1000);
  assert.equal((await blip).state, "ready", "two 503s in a row are a blip, not the end");

  const gone = Object.assign(new Error("preflight pf-gone not found"), { status: 404 });
  const refused = await (async () => {
    const ended = followCheck({
      first: preflight("pf-gone", "running", false),
      read: async () => { throw gone; },
      show: () => {},
    });
    await advance(5000);
    return ended;
  })();
  assert.equal(refused.followStopped, FOLLOW_UNREADABLE, "a 404 will not change by asking again");
  assert.equal(refused.followError, "preflight pf-gone not found");
  assert.match(checkStoppedBlock(refused), /The read answered: preflight pf-gone not found/);

  let failing = 0;
  const outage = followCheck({
    first: preflight("pf-down", "running", false),
    read: async () => { failing += 1; throw unavailable; },
    show: () => {},
  });
  await advance(120 * 1000);
  const down = await outage;
  assert.equal(down.followStopped, FOLLOW_UNREADABLE, "an outage is reported, not retried for ever");
  assert.equal(failing, FOLLOW_FAILURES_TOLERATED + 1);
}));

// ===========================================================================
// Every page that follows a check
// ===========================================================================

/** The schedules page's API: the panel and the form start checks that settle
 *  at 100 s, or never. */
function schedulesApi(never) {
  const api = {
    sent: [],
    reads: 0,
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters"
        ? { items: [{ metadata: { name: "orders-prod", uid: "uid-A" },
          spec: { role: "source", bootstrapServers: ["kafka:9093"] }, status: {} }] }
        : { items: [] });
    },
    destinations() {
      return Promise.resolve({ items: [{ name: "primary", uid: "d-1", default: true,
        canonicalUrl: "s3://kafka-backups/poc" }] });
    },
    latestDiscoveries() { return Promise.resolve({ latestAttempt: null, lastSuccessful: null }); },
    startPreflight(namespace, request, options) {
      api.sent.push({ request: request, options: options || {} });
      return Promise.resolve({ item: preflight("pf-" + api.sent.length, "pending", false),
        replayed: false });
    },
    preflight(namespace, id) {
      api.reads += 1;
      return Promise.resolve({ item: readAt(id, never) });
    },
  };
  return api;
}

test("the_readiness_panel_follows_a_100_s_check_until_it_settles", clocked(async () => {
  const api = schedulesApi(false);
  const view = fakeView();
  await mountSchedules(view.root, "p15-panel", parse, LIFE(), api);
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  assert.equal(api.sent.length, 1);
  await advance(64 * 1000);
  assert.match(newest(view, "id=\"backup-readiness\""), /this page reads it again until then/,
    "at 64 s the check is still running and still followed");
  await advance(SETTLES_AT_MS - 64 * 1000 + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"backup-readiness\"");
  assert.match(panel, /pf-1/);
  assert.match(panel, /badge-green">ready/,
    "NEGATIVE CONTROL: the verdict is on screen (a 40 s budget left 'not finished' for good)");
  assert.match(panel, /applies to your current inputs/);
  assert.doesNotMatch(panel, /this page reads it again until then/);
}));

test("the_readiness_panel_says_when_a_check_outlived_its_deadline_and_asks_anew", clocked(async () => {
  const api = schedulesApi(true);
  const view = fakeView();
  await mountSchedules(view.root, "p15-panel-late", parse, LIFE(), api);
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  await advance(PREFLIGHT_FOLLOW_MS + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"backup-readiness\"");
  assert.match(panel, /data-check-stopped="deadline"/);
  assert.ok(panel.includes(CHECK_UNFINISHED_SENTENCE));
  assert.doesNotMatch(panel, /this page reads it again until then/,
    "NEGATIVE CONTROL: no promise to read again that nothing keeps");
  const reads = api.reads;
  await advance(60 * 1000);
  assert.equal(api.reads, reads, "and nothing reads it after the deadline");

  // RUN THE CHECK AGAIN: the same inputs, a NEW check under a renewed key.
  await view.find(".check-retry").dispatch("click");
  await flush();
  assert.equal(api.sent.length, 2, "the retry started a check");
  assert.notEqual(api.sent[1].options.attempt, api.sent[0].options.attempt,
    "NEGATIVE CONTROL: a new key -- a replay would hand back the check that did not finish");
}));

/** A filled-in guided draft for the schedule form. */
function scheduleDraft() {
  return {
    name: "nightly", source: "orders-prod", sourceUid: "uid-A", mode: "daily", cron: "",
    hour: "2", minute: "30", dayOfWeek: "1", dayOfMonth: "1", n: "6",
    timeZone: "Europe/Berlin", selection: "named", topics: "orders, payments",
    incompleteDiscovery: "", excludeTopics: "", excludePrefixes: "", destination: "primary",
    archive: "", archiveSecret: "", concurrencyPolicy: "", startingDeadlineSeconds: "",
    catchUpPolicy: "", maxRetries: "", retryDelaySeconds: "", activeDeadlineSeconds: "",
    keepLast: "", keepDays: "", suspended: "false",
  };
}

async function scheduleForm(ns, api) {
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  keepDraft(key, scheduleDraft(), SCHEDULE_DRAFT_FIELDS);
  const view = fakeView();
  await mountSchedules(view.root, ns, parse, LIFE(), api);
  const button = view.find("#schedule-check-readiness");
  assert.ok(button !== null, "the form offers Check readiness");
  await button.dispatch("click");
  await flush();
  return view;
}

test("the_schedule_form_follows_a_100_s_check_until_it_settles", clocked(async () => {
  const api = schedulesApi(false);
  const view = await scheduleForm("p15-form", api);
  assert.equal(api.sent.length, 1);
  await advance(SETTLES_AT_MS + FOLLOW_MAX_GAP_MS);
  const form = newest(view, "id=\"schedule-readiness-verdict\"");
  assert.match(form, /badge-green">ready/,
    "NEGATIVE CONTROL: the verdict is on the form (a 40 s budget left 'not finished')");
  assert.doesNotMatch(form, /this page reads it again until then/);
  dropDraft(formKey("p15-form", SCHEDULE_FORM));
}));

test("the_schedule_form_says_when_a_check_outlived_its_deadline_and_asks_anew", clocked(async () => {
  const api = schedulesApi(true);
  const view = await scheduleForm("p15-form-late", api);
  await advance(PREFLIGHT_FOLLOW_MS + FOLLOW_MAX_GAP_MS);
  const form = newest(view, "id=\"schedule-readiness-verdict\"");
  assert.match(form, /data-check-stopped="deadline"/);
  assert.match(form, /did not finish/);
  assert.doesNotMatch(form, /this page reads it again until then/);
  await view.find(".check-retry").dispatch("click");
  await flush();
  assert.equal(api.sent.length, 2, "Run the check again is Check readiness again");
  assert.notEqual(api.sent[1].options.attempt, api.sent[0].options.attempt,
    "NEGATIVE CONTROL: under a new key, so a new check");
  dropDraft(formKey("p15-form-late", SCHEDULE_FORM));
}));

const CONNECTION = Object.freeze({
  metadata: { name: "orders-prod", namespace: "team-a", uid: "uid-A", generation: 1 },
  spec: { role: "source", bootstrapServers: ["kafka-a:9093"] },
  status: {},
});

/** The connection detail's API: a connection check and a discovery that each
 *  settle at 100 s, or never. */
function clusterApi(never) {
  const api = {
    checks: [],
    discoveries: [],
    reads: 0,
    slotReads: 0,
    get() { return Promise.resolve(CONNECTION); },
    latestDiscoveries() {
      api.slotReads += 1;
      const done = api.discoveries.length > 0 && never !== true && Date.now() >= SETTLES_AT_MS;
      const last = done ? discovery("td-" + api.discoveries.length, "succeeded", true) : null;
      return Promise.resolve({ latestAttempt: last, lastSuccessful: last });
    },
    startPreflight(namespace, request, options) {
      api.checks.push(options || {});
      return Promise.resolve({ item: preflight("pf-c" + api.checks.length, "pending", false,
        { operation: "sourceConnection" }), replayed: false });
    },
    preflight(namespace, id) {
      api.reads += 1;
      return Promise.resolve({ item: Object.assign(readAt(id, never),
        { operation: "sourceConnection" }) });
    },
    startDiscovery(namespace, connection, request, options) {
      api.discoveries.push(options || {});
      return Promise.resolve({ item: discovery("td-" + api.discoveries.length, "pending", false),
        replayed: false, reused: false });
    },
    discovery(namespace, id) {
      api.reads += 1;
      return Promise.resolve({ item: never !== true && Date.now() >= SETTLES_AT_MS
        ? discovery(id, "succeeded", true)
        : discovery(id, "running", false) });
    },
    discoveryTopics() {
      return Promise.resolve({ items: [], page: {}, scan: { complete: true, chunksScanned: 1 } });
    },
  };
  return api;
}

test("test_connection_follows_a_100_s_check_until_it_settles", clocked(async () => {
  const api = clusterApi(false);
  const view = fakeView();
  await mountClusterDetail(view.root, "p15-c", "orders-prod", parse, LIFE(), api);
  await view.find("#connection-check-form").dispatch("submit");
  await flush();
  await advance(SETTLES_AT_MS + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"connection-check\"");
  assert.match(panel, /pf-c1/);
  assert.match(panel, /badge-green">ready/,
    "NEGATIVE CONTROL: the verdict is on screen (a 30 s budget said it stopped at 30 s)");
  assert.doesNotMatch(panel, /data-check-stopped=/);
}));

test("test_connection_says_when_a_check_outlived_its_deadline_and_runs_a_new_one", clocked(async () => {
  const api = clusterApi(true);
  const view = fakeView();
  await mountClusterDetail(view.root, "p15-c-late", "orders-prod", parse, LIFE(), api);
  await view.find("#connection-check-form").dispatch("submit");
  await flush();
  await advance(PREFLIGHT_FOLLOW_MS + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"connection-check\"");
  assert.match(panel, /data-check-stopped="deadline"/);
  assert.doesNotMatch(panel, /this page reads it again until then/);
  await view.find(".check-retry").dispatch("click");
  await flush();
  assert.equal(api.checks.length, 2, "Run the check again is Test connection again");
  assert.notEqual(api.checks[1].attempt, api.checks[0].attempt);
}));

test("a_connection_check_left_running_is_followed_again_when_the_view_returns", clocked(async () => {
  // THE SECOND FACE OF THE CLASS: the panel remembers its check across visits,
  // and the follow ended with the route that started it -- so a reader who
  // came back while it ran found "this page reads it again until then" and
  // nothing reading it.
  const api = clusterApi(false);
  const routes = createRouteLifecycle();
  const first = fakeView();
  await mountClusterDetail(first.root, "p15-c-back", "orders-prod", parse, routes.begin(), api);
  await first.find("#connection-check-form").dispatch("submit");
  await flush();
  await advance(10 * 1000);
  const left = routes.begin();
  const second = fakeView();
  await mountClusterDetail(second.root, "p15-c-back", "orders-prod", parse, left, api);
  await flush();
  assert.match(newest(second, "id=\"connection-check\""), /pf-c1/,
    "the check this page started is still the one it shows");
  await advance(SETTLES_AT_MS);
  assert.match(newest(second, "id=\"connection-check\""), /badge-green">ready/,
    "NEGATIVE CONTROL: the returning view follows it again and shows its verdict");
  assert.equal(api.checks.length, 1, "no second check was started to find it");
}));

test("discover_topics_follows_its_discovery_until_it_settles_and_rereads_the_slots", clocked(async () => {
  const api = clusterApi(false);
  const view = fakeView();
  await mountClusterDetail(view.root, "p15-td", "orders-prod", parse, LIFE(), api);
  await view.find("#discovery-form").dispatch("submit");
  await flush();
  assert.equal(api.discoveries.length, 1);
  await advance(30 * 1000);
  assert.ok(api.reads > 0,
    "NEGATIVE CONTROL: the started discovery is read again (the panel never read it before)");
  const slotsBefore = api.slotReads;
  await advance(SETTLES_AT_MS - 30 * 1000 + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"cluster-discovery\"");
  assert.match(panel, /td-1/);
  assert.match(panel, /succeeded/, "the finished attempt is on screen");
  assert.ok(api.slotReads > slotsBefore, "and the two slots were read again once it finished");
  assert.doesNotMatch(panel, /id="no-successful"/,
    "the new inventory is the last successful one now");
}));

test("discover_topics_says_when_a_discovery_outlived_its_deadline_and_asks_anew", clocked(async () => {
  const api = clusterApi(true);
  const view = fakeView();
  await mountClusterDetail(view.root, "p15-td-late", "orders-prod", parse, LIFE(), api);
  await view.find("#discovery-form").dispatch("submit");
  await flush();
  await advance(DISCOVERY_FOLLOW_MS + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"cluster-discovery\"");
  assert.match(panel, /data-check-stopped="deadline"/);
  assert.match(panel, /The discovery did not finish/);
  await view.find(".check-retry").dispatch("click");
  await flush();
  assert.equal(api.discoveries.length, 2, "Run the discovery again started one");
  assert.notEqual(api.discoveries[1].attempt, api.discoveries[0].attempt,
    "NEGATIVE CONTROL: under a renewed key -- the stopped attempt is spent");
}));

const DESTINATION = Object.freeze({
  name: "primary", uid: "d-1", generation: 1, canonicalUrl: "s3://kafka-backups/poc",
  transport: "insecureHttp", access: {}, conditions: [],
});

function destinationApi(never) {
  const api = {
    tests: [],
    destination() { return Promise.resolve({ item: DESTINATION }); },
    destinationUsage() { return Promise.resolve({ schedules: [], backups: [] }); },
    testDestination(namespace, name, request, options) {
      api.tests.push(options || {});
      return Promise.resolve({ item: preflight("pf-t" + api.tests.length, "pending", false,
        { operation: "destinationAccess" }), replayed: false });
    },
    preflight(namespace, id) {
      return Promise.resolve({ item: Object.assign(readAt(id, never),
        { operation: "destinationAccess" }) });
    },
  };
  return api;
}

test("test_access_follows_a_100_s_check_until_it_settles", clocked(async () => {
  const api = destinationApi(false);
  const view = fakeView();
  await mountDestinationDetail(view.root, "p15-d", "primary", parse, LIFE(), api);
  await view.find("#destination-test-form").dispatch("submit");
  await flush();
  await advance(SETTLES_AT_MS + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"destination-test\"");
  assert.match(panel, /pf-t1/);
  assert.match(panel, /badge-green">ready/,
    "NEGATIVE CONTROL: the verdict is on screen (a 60 s budget said it stopped at 60 s)");
}));

test("test_access_says_when_a_check_outlived_its_deadline_and_tests_again", clocked(async () => {
  const api = destinationApi(true);
  const view = fakeView();
  await mountDestinationDetail(view.root, "p15-d-late", "primary", parse, LIFE(), api);
  await view.find("#destination-test-form").dispatch("submit");
  await flush();
  await advance(PREFLIGHT_FOLLOW_MS + FOLLOW_MAX_GAP_MS);
  const panel = newest(view, "id=\"destination-test\"");
  assert.match(panel, /data-check-stopped="deadline"/);
  assert.ok(!panel.includes(CHECKING_SENTENCE), "NEGATIVE CONTROL: no checking sentence left");
  await view.find(".check-retry").dispatch("click");
  await flush();
  assert.equal(api.tests.length, 2, "Run the check again is Test access again");
}));

// ===========================================================================
// Fix round (review of poc-fixes-5): one follow per check, the route ends it,
// and the refusals that are not blips
// ===========================================================================

/** An API whose creates REPLAY by attempt token, as the product API does (D0:
 *  one key, one object): a second ask with the same token answers the same
 *  check. Its checks never settle, so a follow reads at its steady cadence.
 *
 *  ITS IDS CARRY `prefix`, one per row. A follow is registered by its check's
 *  id for as long as it lives, and `LIFE()` never leaves, so a follow an
 *  earlier row left running would BE the follow of a later row's check of the
 *  same id (`followCheck` never starts a second one). */
function replayingApi(prefix, settles) {
  const byToken = new Map();
  const api = {
    creates: [],
    reads: [],
    slotReads: 0,
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters"
        ? { items: [{ metadata: { name: "orders-prod", uid: "uid-A" },
          spec: { role: "source", bootstrapServers: ["kafka:9093"] }, status: {} }] }
        : { items: [] });
    },
    destinations() {
      return Promise.resolve({ items: [{ name: "primary", uid: "d-1", default: true,
        canonicalUrl: "s3://kafka-backups/poc" }] });
    },
    get() { return Promise.resolve(CONNECTION); },
    latestDiscoveries() {
      api.slotReads += 1;
      return Promise.resolve({ latestAttempt: null, lastSuccessful: null });
    },
    discoveryTopics() {
      return Promise.resolve({ items: [], page: {}, scan: { complete: true, chunksScanned: 1 } });
    },
    startPreflight(namespace, request, options) {
      const token = (options || {}).attempt;
      api.creates.push(token);
      const replayed = byToken.has(token);
      if (!replayed) {
        byToken.set(token, "pf-" + prefix + "-" + String(byToken.size + 1));
      }
      return Promise.resolve({ item: preflight(byToken.get(token), "running", false), replayed });
    },
    preflight(namespace, id) {
      api.reads.push(Date.now());
      return Promise.resolve({ item: preflight(id, "running", false) });
    },
    startDiscovery(namespace, connection, request, options) {
      const token = (options || {}).attempt;
      api.creates.push(token);
      const replayed = byToken.has(token);
      if (!replayed) {
        byToken.set(token, "td-" + prefix + "-" + String(byToken.size + 1));
      }
      return Promise.resolve({ item: discovery(byToken.get(token), "running", false), replayed,
        reused: false });
    },
    discovery(namespace, id) {
      api.reads.push(Date.now());
      return Promise.resolve({ item: settles === true && Date.now() >= SETTLES_AT_MS
        ? discovery(id, "succeeded", true)
        : discovery(id, "running", false) });
    },
  };
  return api;
}

const ASK_TWICE = [
  ["the readiness panel", async (view) => view.find("#readiness-form").dispatch("submit"),
    async (ns, api) => {
      const view = fakeView();
      await mountSchedules(view.root, ns, parse, LIFE(), api);
      return view;
    }],
  ["the schedule form", async (view) => view.find("#schedule-check-readiness").dispatch("click"),
    async (ns, api) => {
      const key = formKey(ns, SCHEDULE_FORM);
      dropDraft(key);
      keepDraft(key, scheduleDraft(), SCHEDULE_DRAFT_FIELDS);
      const view = fakeView();
      await mountSchedules(view.root, ns, parse, LIFE(), api);
      return view;
    }],
  ["Discover topics", async (view) => view.find("#discovery-form").dispatch("submit"),
    async (ns, api) => {
      const view = fakeView();
      await mountClusterDetail(view.root, ns, "orders-prod", parse, LIFE(), api);
      return view;
    }],
];

for (const [surface, ask, mount] of ASK_TWICE) {
  test("asking again while the check runs does not start a second follow: " + surface,
    clocked(async () => {
      // REVIEW M1. The ask is re-enabled once its create answers, and asking
      // again while the check runs REPLAYS it (the same intent token, the same
      // id). Each answer started a follow of its own, and two follows of one
      // check both stayed "mine": 6 reads a minute became 22 after two more
      // clicks, for up to twelve minutes.
      const ns = "m1-" + surface.replace(/[^a-z]/gi, "").toLowerCase();
      const api = replayingApi(ns);
      const view = await mount(ns, api);
      await ask(view);
      await flush();
      await advance(60 * 1000);
      const at = api.reads.length;
      await advance(60 * 1000);
      const before = api.reads.length - at;
      assert.equal(before, 6, "one follow reads six times a minute at its steady ten seconds");
      await ask(view);
      await flush();
      await ask(view);
      await flush();
      assert.equal(new Set(api.creates).size, 1, "the asks replayed the same check (one token)");
      assert.equal(api.creates.length, 3);
      const since = api.reads.length;
      await advance(60 * 1000);
      const after = api.reads.length - since;
      assert.ok(after <= before + 1,
        "NEGATIVE CONTROL: the cadence is unchanged after two replayed asks: " + before +
          " reads a minute before, " + after + " after");
      dropDraft(formKey(ns, SCHEDULE_FORM));
    }));
}

test("a_discovery_asked_again_while_it_runs_rereads_its_slots_once_when_it_finishes", clocked(async () => {
  // THE PAGE HALF OF M1 for Discover topics: its follow re-reads the two slots
  // when the discovery finishes, and a second ask that replayed the running
  // discovery must not chain a second re-read onto the one follow.
  const api = replayingApi("m1-slots", true);
  const view = fakeView();
  await mountClusterDetail(view.root, "m1-slots", "orders-prod", parse, LIFE(), api);
  await view.find("#discovery-form").dispatch("submit");
  await flush();
  await advance(30 * 1000);
  await view.find("#discovery-form").dispatch("submit");
  await flush();
  assert.equal(new Set(api.creates).size, 1, "the second ask replayed the running discovery");
  const slots = api.slotReads;
  await advance(SETTLES_AT_MS);
  assert.equal(api.slotReads - slots, 1,
    "NEGATIVE CONTROL: the finished discovery's slots are read once, not once per ask");
}));

test("a_follow_asked_for_a_check_already_followed_is_that_follow", clocked(async () => {
  let reads = 0;
  const first = { first: preflight("pf-once", "running", false),
    read: async (current) => { reads += 1; return readAt(current.id, true); } };
  const one = followCheck(first);
  const two = followCheck(Object.assign({}, first));
  assert.equal(one, two, "the second ask is handed the live follow's promise");
  assert.equal(isFollowed("pf-once"), true);
  await advance(20 * 1000);
  assert.equal(reads, 10, "one follow's ten reads in twenty seconds, not twenty");
}));

// THE ROUTE ENDS EVERY FOLLOW (review M2). The fake APIs below IGNORE `signal`,
// so the only thing that can stop a read after the route left is the
// follower's own `keep` -- `active(lifecycle)` in each of them. The route
// leaves during the first wait; after it, nothing is read and nothing painted.
const LEAVERS = [
  ["the readiness panel", async (ns, parse_, life, api) => {
    await mountSchedules(parse_.view.root, ns, parse_, life, api);
    await parse_.view.find("#readiness-form").dispatch("submit");
  }],
  ["the schedule form", async (ns, parse_, life, api) => {
    const key = formKey(ns, SCHEDULE_FORM);
    dropDraft(key);
    keepDraft(key, scheduleDraft(), SCHEDULE_DRAFT_FIELDS);
    await mountSchedules(parse_.view.root, ns, parse_, life, api);
    await parse_.view.find("#schedule-check-readiness").dispatch("click");
  }],
  ["Test access", async (ns, parse_, life, api) => {
    await mountDestinationDetail(parse_.view.root, ns, "primary", parse_, life, api);
    await parse_.view.find("#destination-test-form").dispatch("submit");
  }],
  ["Discover topics", async (ns, parse_, life, api) => {
    await mountClusterDetail(parse_.view.root, ns, "orders-prod", parse_, life, api);
    await parse_.view.find("#discovery-form").dispatch("submit");
  }],
];

for (const [surface, start] of LEAVERS) {
  test("a_follow_whose_route_left_reads_nothing_and_paints_nothing: " + surface, async () => {
    const routes = createRouteLifecycle();
    const life = routes.begin();
    const counted = { reads: 0, paintsAfterLeave: 0, left: false, waits: 0 };
    const ns = "m2-" + surface.replace(/[^a-z]/gi, "").toLowerCase();
    const api = Object.assign(replayingApi(ns), destinationApi(false), {
      testDestination() {
        return Promise.resolve({ item: preflight("pf-" + ns, "pending", false,
          { operation: "destinationAccess" }), replayed: false });
      },
      preflight(namespace, id) {
        counted.reads += 1;
        return Promise.resolve({ item: preflight(id, "running", false) });
      },
      discovery(namespace, id) {
        counted.reads += 1;
        return Promise.resolve({ item: discovery(id, "running", false) });
      },
      // THE ROUTE LEAVES DURING THE FOLLOW'S FIRST WAIT.
      wait: async () => {
        counted.waits += 1;
        if (!counted.left) {
          counted.left = true;
          routes.begin();
        }
      },
    });
    const view = fakeView();
    const parse_ = (html) => {
      if (counted.left) {
        counted.paintsAfterLeave += 1;
      }
      return [{ html: html }];
    };
    parse_.view = view;
    try {
      await start(ns, parse_, life, api);
      await flush(40);
      assert.equal(counted.waits, 1, "the follow reached its first wait, where the route left");
      assert.equal(counted.reads, 0,
        "NEGATIVE CONTROL: no read after the route left (the fake ignores the signal)");
      assert.equal(counted.paintsAfterLeave, 0, "and nothing painted on the view that left");
    } finally {
      dropDraft(formKey(ns, SCHEDULE_FORM));
      resetMode();
      resetCheckIntents();
    }
  });
}

// THE REFUSALS THAT ARE NOT BLIPS (review M3, L3): the session ended (401),
// the session may not read the check (403), this page refused to ask, and an
// answer this page cannot read. Each is read ONCE and then said.
for (const [label, error] of [
  ["401", Object.assign(new Error("the session ended; sign in again"), { status: 401 })],
  ["403", Object.assign(new Error("this role may not read preflights here"), { status: 403 })],
  ["refused", Object.assign(new Error("this page will not ask"), { kind: "refused", status: 0 })],
  ["contract", Object.assign(new Error("the answer is not a Preflight this page reads"),
    { kind: "contract" })],
]) {
  test("a_read_refused_with_" + label + "_is_read_once_and_stops_the_follow", clocked(async () => {
    let reads = 0;
    const ended = followCheck({
      first: preflight("pf-" + label, "running", false),
      read: async () => { reads += 1; throw error; },
    });
    await advance(60 * 1000);
    const result = await ended;
    assert.equal(reads, 1, "NEGATIVE CONTROL: not retried as a blip");
    assert.equal(result.followStopped, FOLLOW_UNREADABLE);
    assert.equal(result.followError, error.message);
  }));
}

test("a_follow_whose_route_aborts_ends_at_once_and_leaves_no_timer", clocked(async () => {
  // REVIEW L1: the wait between reads was a plain timer, left running for up
  // to ten seconds after the route had gone.
  const route = new AbortController();
  let reads = 0;
  let settled = false;
  const ended = followCheck({
    first: preflight("pf-leave", "running", false),
    signal: route.signal,
    keep: () => !route.signal.aborted,
    read: async (current) => { reads += 1; return readAt(current.id, true); },
  });
  ended.then(() => { settled = true; });
  await advance(30 * 1000);
  const count = reads;
  route.abort();
  await flush();
  assert.equal(settled, true,
    "NEGATIVE CONTROL: the follow ended on the abort, without its timer firing");
  assert.equal(await ended, null);
  assert.equal(isFollowed("pf-leave"), false, "and it is no longer registered");
  await advance(60 * 1000);
  assert.equal(reads, count);
}));

test("a_read_the_server_never_answers_ends_at_the_deadline_as_unreadable", clocked(async () => {
  // REVIEW L2: the deadline was checked only between reads, and a GET the
  // server accepted and never answered held "checking..." past it.
  const signals = [];
  let settled = false;
  const ended = followCheck({
    first: preflight("pf-hang", "running", false),
    read: (current, options) => { signals.push((options || {}).signal); return new Promise(() => {}); },
  });
  ended.then(() => { settled = true; });
  await advance(PREFLIGHT_FOLLOW_MS + 2 * FOLLOW_MAX_GAP_MS);
  assert.equal(settled, true, "NEGATIVE CONTROL: the follow ended although the read never answered");
  const result = await ended;
  assert.equal(result.followStopped, FOLLOW_UNREADABLE,
    "the page cannot say the check did not finish: it never heard back");
  assert.match(result.followError, /not answered before the check's deadline/);
  assert.equal(signals.length, 1);
  assert.equal(signals[0].aborted, true, "and the read itself was aborted");
}));

test("a_wall_clock_step_neither_ends_a_follow_nor_stretches_it", clocked(async () => {
  // REVIEW L4: the deadline ran on Date.now(); an NTP step or a laptop waking
  // an hour later ended the follow after one read with "did not finish".
  let reads = 0;
  const shown = [];
  const ended = followCheck({
    first: preflight("pf-clock", "running", false),
    read: async (current) => { reads += 1; return preflight(current.id, "running", false); },
    show: (check) => { shown.push(check); },
  });
  await advance(30 * 1000);
  mock.timers.setTime(Date.now() + 3600 * 1000);
  await advance(30 * 1000);
  assert.equal(shown.some((c) => c.followStopped !== undefined), false,
    "NEGATIVE CONTROL: an hour's step on the wall clock did not end the follow");
  assert.ok(reads > 10, "it is still reading: " + reads);
  await advance(PREFLIGHT_FOLLOW_MS);
  assert.equal((await ended).followStopped, FOLLOW_DEADLINE, "and it still ends at its deadline");
}));

test("a_failed_last_read_says_the_check_could_not_be_read_not_that_it_did_not_finish", clocked(async () => {
  // REVIEW L5: the read at the deadline failed with a 503, the loop went on,
  // found the deadline passed, and said "did not finish" -- which the page
  // did not know: the check may have settled.
  const unavailable = Object.assign(new Error("service unavailable"), { status: 503 });
  let reads = 0;
  const ended = followCheck({
    first: preflight("pf-last", "running", false),
    read: async (current) => {
      reads += 1;
      if (Date.now() >= PREFLIGHT_FOLLOW_MS - 3000) {
        throw unavailable;
      }
      return preflight(current.id, "running", false);
    },
  });
  await advance(PREFLIGHT_FOLLOW_MS + 2 * FOLLOW_MAX_GAP_MS);
  const result = await ended;
  assert.equal(result.followStopped, FOLLOW_UNREADABLE,
    "NEGATIVE CONTROL: not 'deadline' -- the last read failed");
  assert.equal(result.followError, "service unavailable");
}));

test("a_check_settling_within_twenty_seconds_is_seen_within_two", clocked(async () => {
  // REVIEW L8: backing off from the first read delayed the common case -- a
  // check settling at 17-26 s was shown at 26 s. The first twenty seconds keep
  // the old two-second cadence.
  const shown = [];
  followCheck({
    first: preflight("pf-17", "pending", false),
    read: async (current) => (Date.now() >= 17000
      ? preflight(current.id, "ready", true)
      : preflight(current.id, "running", false)),
    show: (check) => { shown.push([Date.now(), check.terminal]); },
  });
  await advance(19 * 1000);
  const verdict = shown.find((s) => s[1] === true);
  assert.ok(verdict !== undefined && verdict[0] <= 18000,
    "NEGATIVE CONTROL: shown by 18 s, not at 26 s: " + JSON.stringify(verdict));
}));
