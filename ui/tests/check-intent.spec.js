// check-intent.spec.js -- P14 (poc-upgrade-2): asking a check again is not
// replaying its last answer.
//
// THE DEFECT. The schedule form's "Check readiness", clicked with unchanged
// inputs after the earlier check's validity had passed, sent the same
// content-derived Idempotency-Key; the product API (D0: one key, one object)
// answered `200 replayed: true` with the EXPIRED check, the page said "does
// not apply to your current inputs", and no click could make a fresh one.
//
// THE FIX, on both sides. The API's replay answer names `expired` itself
// (`crates/logweir-api/tests/preflights.rs`,
// `a_replay_names_its_own_expiry_and_a_new_key_asks_afresh`); the console
// carries an intent token in the key, kept while the check it named can
// still be the answer and renewed once it is spent (`lifecycle.js`'s
// `askCheck`). A genuine retry inside the window still replays.
//
// THE SERVER BELOW IS D0's RULE, modelled: an object per (body, key); the
// same key answers the same object, a replay projects "not recomputed" and
// -- as the API now does -- `expired` once the validity has passed. On the
// code before the fix the console sent no token, so the second click's key
// was the first's and every row's negative control below fails.

import { test } from "node:test";
import assert from "node:assert/strict";

import { resetMode } from "../client.js";
import {
  askCheck,
  checkIntent,
  discoverySpent,
  dropDraft,
  formKey,
  keepDraft,
  preflightSpent,
  resetCheckIntents,
} from "../lifecycle.js";
import { LIFE, fakeView, parse } from "./fake-view.js";
import { SCHEDULE_DRAFT_FIELDS, SCHEDULE_FORM, mountSchedules } from "../pages/schedules.js";
import { mountClusterDetail } from "../pages/clusters.js";

const flush = () => new Promise((done) => setTimeout(done, 20));

const NOT_RECOMPUTED = { reason: "unverifiable",
  basis: "this response did not recompute staleness; read the preflight itself for a current verdict" };

// ------------------------------------------------------------- the rule itself

test("a_preflight_is_spent_only_when_it_can_no_longer_answer_the_same_question", () => {
  const base = { id: "pf-1", terminal: true, state: "ready", applicable: true, stale: false,
    staleReasons: [] };
  assert.equal(preflightSpent(Object.assign({}, base, { terminal: false, state: "running" })), false,
    "a running check is the answer being waited for");
  assert.equal(preflightSpent(base), false, "a current verdict is still the answer");
  assert.equal(preflightSpent(Object.assign({}, base, { applicable: false, stale: true,
    staleReasons: [NOT_RECOMPUTED] })), false,
  "a replay's 'not recomputed' is not a verdict about the check: the follow's read decides");
  assert.equal(preflightSpent(Object.assign({}, base, { applicable: false, stale: true,
    staleReasons: [{ reason: "expired" }, NOT_RECOMPUTED] })), true,
  "NEGATIVE CONTROL: a replay that names its own expiry is spent");
  assert.equal(preflightSpent(Object.assign({}, base, { applicable: false, stale: true,
    staleReasons: [{ reason: "referentChanged", kind: "BackupDestination", name: "primary" }] })),
  true, "a read that no longer applies is spent");
  for (const state of ["failed", "cancelled"]) {
    assert.equal(preflightSpent(Object.assign({}, base, { state: state, applicable: false })), true,
      "a " + state + " check produced no verdict to replay");
  }
});

test("a_discovery_is_spent_when_it_failed_or_its_inventory_is_stale", () => {
  assert.equal(discoverySpent({ terminal: false, state: "running" }), false);
  assert.equal(discoverySpent({ terminal: true, state: "succeeded", stale: false }), false);
  assert.equal(discoverySpent({ terminal: true, state: "succeeded", stale: true }), true);
  assert.equal(discoverySpent({ terminal: true, state: "failed", stale: false }), true);
});

test("ask_check_replays_a_retry_and_renews_only_a_spent_check", async () => {
  resetCheckIntents();
  const tokens = [];
  let answer = null;
  const start = (token) => { tokens.push(token); return Promise.resolve(answer); };
  const ask = (held) => askCheck({ intent: "ns/form", question: "q", held: held,
    spent: preflightSpent, start: start });
  // First ask, then a retry while it runs: ONE key (the retry is idempotent).
  answer = { item: { id: "pf-1", terminal: false, state: "pending" }, replayed: false };
  await ask(null);
  answer = { item: { id: "pf-1", terminal: false, state: "running" }, replayed: true };
  await ask({ id: "pf-1", terminal: false, state: "running" });
  assert.equal(tokens[0], tokens[1], "a retry while the check runs is the same key");
  // Current and terminal: still the same key.
  answer = { item: { id: "pf-1", terminal: true, state: "ready", staleReasons: [NOT_RECOMPUTED] },
    replayed: true };
  await ask({ id: "pf-1", terminal: true, state: "ready", applicable: true, staleReasons: [] });
  assert.equal(tokens[2], tokens[0], "a second click inside the validity replays");
  // The replay answers already expired: renewed, and asked once more.
  const expired = { item: { id: "pf-1", terminal: true, state: "ready", applicable: false,
    staleReasons: [{ reason: "expired" }, NOT_RECOMPUTED] }, replayed: true };
  const fresh = { item: { id: "pf-2", terminal: false, state: "pending" }, replayed: false };
  let turn = 0;
  const renewing = (token) => { tokens.push(token); turn += 1; return Promise.resolve(turn === 1 ? expired : fresh); };
  const got = await askCheck({ intent: "ns/form", question: "q", held: null, spent: preflightSpent,
    start: renewing });
  assert.equal(tokens[3], tokens[0], "the ask went out under the held key first");
  assert.notEqual(tokens[4], tokens[3], "NEGATIVE CONTROL: the spent check's key is renewed");
  assert.equal(got.item.id, "pf-2", "and the answer is the new check");
  assert.equal(got.renewedFrom, "pf-1", "which says what it replaced");
  // A held check the page READ as spent renews before the first create.
  const before = checkIntent("ns/form", "q", null, null).token;
  assert.equal(before, tokens[4], "the renewed key is now the form's");
  const read = { id: "pf-2", terminal: true, state: "cancelled", applicable: false };
  const after = checkIntent("ns/form", "q", read, preflightSpent).token;
  assert.notEqual(after, before, "a cancelled check of this intent's is not asked again");
  // A spent check this intent did NOT make does not move its key.
  const foreign = checkIntent("ns/form", "q", { id: "pf-other", terminal: true, state: "failed" },
    preflightSpent).token;
  assert.equal(foreign, after, "a check some other key made is not this intent's to renew");
  // A different question is a different key.
  assert.notEqual(checkIntent("ns/form", "q2", null, null).token, after);
});

// ------------------------------------------------------- the server, modelled

/** D0's replay rule over Preflights, with a clock. */
function preflightServer() {
  const s = { now: 0, objects: new Map(), creates: [], VALID: 600 };
  const project = (o, replay) => {
    const done = o.terminal === true;
    const expired = done && s.now >= o.created + s.VALID;
    const reasons = replay
      ? (expired ? [{ reason: "expired" }] : []).concat(done ? [NOT_RECOMPUTED] : [])
      : (expired ? [{ reason: "expired" }] : []);
    return {
      id: o.id, namespace: "ns", uid: "uid-" + o.id, state: done ? "ready" : "pending",
      terminal: done, binding: { referents: [] }, applicable: done && reasons.length === 0,
      stale: reasons.length > 0, staleReasons: reasons, staleBasis: ["expiry"], checks: [],
      warnings: [], executionOnly: [], detailsAvailable: false, conditions: [],
      expiresAt: done ? new Date((o.created + s.VALID) * 1000).toISOString() : undefined,
    };
  };
  s.api = {
    startPreflight(namespace, request, options) {
      const attempt = ((options || {}).attempt) || "";
      s.creates.push({ request: request, attempt: attempt });
      const key = JSON.stringify(request) + "|" + attempt;
      const held = s.objects.get(key);
      if (held !== undefined) {
        return Promise.resolve({ item: project(held, true), replayed: true });
      }
      const made = { id: "pf-" + String(s.objects.size + 1), created: s.now, terminal: false };
      s.objects.set(key, made);
      return Promise.resolve({ item: project(made, false), replayed: false });
    },
    preflight(namespace, id) {
      const o = Array.from(s.objects.values()).find((x) => x.id === id);
      o.terminal = true; // the controller has finished it by the time it is read
      return Promise.resolve({ item: project(o, false) });
    },
    wait: async () => {},
  };
  return s;
}

const CLUSTERS = Object.freeze({ items: [
  { metadata: { name: "orders-prod", uid: "uid-A" },
    spec: { role: "source", bootstrapServers: ["kafka:9093"] }, status: {} },
] });

const DESTINATIONS = Object.freeze([
  { name: "primary", uid: "d-1", default: true, canonicalUrl: "s3://kafka-backups/poc",
    transport: "insecureHttp" },
]);

function schedulesApi(server) {
  return Object.assign({
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters" ? CLUSTERS : { items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS }); },
    latestDiscoveries() { return Promise.resolve({ latestAttempt: null, lastSuccessful: null }); },
  }, server.api);
}

const DRAFT = Object.freeze({
  name: "", source: "orders-prod", sourceUid: "uid-A", mode: "daily", cron: "", hour: "2",
  minute: "0", dayOfWeek: "1", dayOfMonth: "1", n: "6", timeZone: "", selection: "named",
  topics: "orders, payments", incompleteDiscovery: "", excludeTopics: "", excludePrefixes: "",
  destination: "primary", archive: "", archiveSecret: "", suspended: "false",
});

test("the_schedule_forms_check_asked_again_after_it_expired_is_a_new_check", async () => {
  // R8.3 on the PoC: both clicks came back `replayed: true` of the expired check.
  resetMode();
  resetCheckIntents();
  const ns = "p14-create";
  const key = formKey(ns, SCHEDULE_FORM);
  keepDraft(key, DRAFT, SCHEDULE_DRAFT_FIELDS);
  const server = preflightServer();
  const view = fakeView();
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), schedulesApi(server));
    await view.find("#schedule-check-readiness").dispatch("click");
    await flush();
    assert.equal(server.objects.size, 1);
    // A second click inside the validity is the same check.
    server.now = 300;
    await view.find("#schedule-check-readiness").dispatch("click");
    await flush();
    assert.equal(server.objects.size, 1, "a click inside the window replays: one object");
    assert.equal(server.creates[1].attempt, server.creates[0].attempt);
    // Past the validity: a NEW check, and it is the one on screen.
    server.now = 700;
    await view.find("#schedule-check-readiness").dispatch("click");
    await flush();
    assert.equal(server.objects.size, 2,
      "NEGATIVE CONTROL: the click after expiry made a new check (it replayed the old one)");
    const form = view.html().split("id=\"schedule-form\"").pop();
    assert.match(form, /pf-2/, "the new check is the verdict on screen");
    assert.doesNotMatch(form, /does not apply to your current inputs/,
      "and the page does not show the expired one as the answer");
  } finally {
    dropDraft(key);
  }
});

test("the_list_panels_check_asked_again_after_it_expired_is_a_new_check", async () => {
  // R8.5's panel, the same class: its key was the request's content alone.
  resetMode();
  resetCheckIntents();
  const server = preflightServer();
  const view = fakeView();
  await mountSchedules(view.root, "p14-panel", parse, LIFE(), schedulesApi(server));
  view.find("#readiness-topics").value = "orders";
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  server.now = 120;
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  assert.equal(server.objects.size, 1, "a resubmit inside the window replays");
  server.now = 900;
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  assert.equal(server.objects.size, 2,
    "NEGATIVE CONTROL: the submit after expiry made a new check (it replayed the old one)");
  const panel = view.html().split("id=\"backup-readiness\"").pop();
  assert.match(panel, /pf-2/);
});

// ------------------------------------------------------------ discovery, the class

const CONNECTION = Object.freeze({
  metadata: { name: "orders-prod", uid: "uid-A", generation: 1 },
  spec: { role: "source", bootstrapServers: ["kafka:9093"] }, status: {},
});

test("a_topic_discovery_asked_again_once_stale_is_a_new_discovery", async () => {
  // P14's CLASS on Clusters -> Discover topics: the key was the parameters
  // alone, and the server's `reuseFresh` covers only a FRESH identical
  // inventory, so a stale one was replayed for as long as it was kept (24 h).
  resetMode();
  resetCheckIntents();
  const s = { now: 0, objects: new Map(), FRESH: 900 };
  const project = (o) => ({
    id: o.id, state: "succeeded", terminal: true, stale: s.now >= o.created + s.FRESH,
    staleReasons: s.now >= o.created + s.FRESH ? ["expired"] : [], observedAt: "x",
    visibility: { state: "unknown" }, counts: { topics: 1 },
  });
  const api = {
    get() { return Promise.resolve(CONNECTION); },
    latestDiscoveries() {
      const all = Array.from(s.objects.values());
      const newest = all.length === 0 ? null : project(all[all.length - 1]);
      return Promise.resolve({ latestAttempt: newest, lastSuccessful: newest });
    },
    startDiscovery(namespace, connection, request, options) {
      const fresh = Array.from(s.objects.values()).find((o) => s.now < o.created + s.FRESH);
      if (fresh !== undefined) {
        return Promise.resolve({ item: project(fresh), replayed: false, reused: true });
      }
      const key = JSON.stringify(request) + "|" + (((options || {}).attempt) || "");
      const held = s.objects.get(key);
      if (held !== undefined) {
        return Promise.resolve({ item: project(held), replayed: true, reused: false });
      }
      const made = { id: "td-" + String(s.objects.size + 1), created: s.now };
      s.objects.set(key, made);
      return Promise.resolve({ item: project(made), replayed: false, reused: false });
    },
    wait: async () => {},
  };
  const view = fakeView();
  await mountClusterDetail(view.root, "p14-discovery", "orders-prod", parse, LIFE(), api);
  await view.find("#discovery-form").dispatch("submit");
  await flush();
  assert.equal(s.objects.size, 1);
  s.now = 1000;
  await view.find("#discovery-form").dispatch("submit");
  await flush();
  assert.equal(s.objects.size, 2,
    "NEGATIVE CONTROL: a stale inventory is asked again, not replayed");
  assert.match(view.html(), /td-2/, "and the new discovery is the one on screen");
});

// ------------------------------------------------ review L4: overlapping asks

test("two_overlapping_asks_of_a_spent_check_renew_once", async () => {
  // The renewal was not compare-and-swap: two asks that both saw the replay
  // come back spent each deleted the intent and created a check of their own.
  resetCheckIntents();
  const objects = new Map();
  const spentOld = { id: "pf-old", terminal: true, state: "ready", applicable: false, stale: true,
    staleReasons: [{ reason: "expired" }, NOT_RECOMPUTED] };
  let firstToken = null;
  const start = async (token) => {
    await flush();
    if (firstToken === null) {
      firstToken = token;
    }
    if (token === firstToken) {
      return { item: spentOld, replayed: true };
    }
    if (objects.has(token)) {
      return { item: objects.get(token), replayed: true };
    }
    const made = { id: "pf-" + String(objects.size + 1), terminal: false, state: "pending" };
    objects.set(token, made);
    return { item: made, replayed: false };
  };
  const ask = () => askCheck({ intent: "ns/overlap", question: "q", held: null,
    spent: preflightSpent, start: start });
  const both = await Promise.all([ask(), ask()]);
  assert.equal(objects.size, 1, "NEGATIVE CONTROL: one renewal, one new check (it made two)");
  assert.equal(both[0].item.id, both[1].item.id);
});

test("the_schedule_forms_check_is_one_request_at_a_time_even_across_a_repaint", async () => {
  resetMode();
  resetCheckIntents();
  const ns = "p14-inflight";
  const key = formKey(ns, SCHEDULE_FORM);
  keepDraft(key, DRAFT, SCHEDULE_DRAFT_FIELDS);
  let open;
  const gate = new Promise((resolve) => { open = resolve; });
  let starts = 0;
  const server = preflightServer();
  const api = schedulesApi(server);
  const inner = api.startPreflight;
  api.startPreflight = async (...args) => { starts += 1; await gate; return inner(...args); };
  api.previewCadence = async () => ({ schedule: "0 2 * * *", timeZone: "UTC", next: [] });
  const view = fakeView();
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), api);
    await view.find("#schedule-check-readiness").dispatch("click");
    // THE FORM REPAINTS while the check is in flight (a cadence change here;
    // a preview answer takes the same `repaint`).
    const before = view.chunks.length;
    await view.find("select[name=\"mode\"]").dispatch("change");
    await flush();
    assert.ok(view.chunks.length > before, "the form was repainted");
    const button = view.find("#schedule-check-readiness");
    assert.ok("disabled" in button.attributes,
      "NEGATIVE CONTROL: the repaint re-enabled the button (the markup has it disabled)");
    await button.dispatch("click");
    assert.equal(starts, 1, "a second click while the first is in flight sends nothing");
    open();
    await flush();
    assert.ok(!("disabled" in view.find("#schedule-check-readiness").attributes),
      "and the answer enables it again");
  } finally {
    dropDraft(key);
  }
});
