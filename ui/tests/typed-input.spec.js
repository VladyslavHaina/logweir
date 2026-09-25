// typed-input.spec.js -- P13 (poc-upgrade-2) and its class: a form that
// repaints when an ASYNC READ lands keeps what the reader typed.
//
// THE DEFECT. Schedules -> Backup readiness repainted the whole panel when the
// discovery read a source change starts came back, and rendered it from the
// mount's view alone: the topics went empty, the source went back to the
// preferred connection and the destination back to the default. Typing the
// topics first and then choosing the source sent `topics: []`, refused 422.
// The same panel sent the DEFAULT destination whatever was chosen, because
// its submit read a hidden input written at render time and never again.
//
// EVERY ROW HERE TYPES FIRST AND THEN LETS THE READ LAND, and each carries its
// NEGATIVE CONTROL: the assertion that fails on the code before the fix.

import { test } from "node:test";
import assert from "node:assert/strict";

import { resetMode } from "../client.js";
import { resetCheckIntents } from "../lifecycle.js";
import { LIFE, fakeView, parse } from "./fake-view.js";
import { mountSchedules } from "../pages/schedules.js";
import { mountClusterDetail } from "../pages/clusters.js";
import { mountDestinationDetail } from "../pages/destinations.js";

const CLUSTERS = Object.freeze({
  items: [
    { metadata: { name: "orders-prod", uid: "uid-A" },
      spec: { role: "source", bootstrapServers: ["kafka-a:9093"] }, status: {} },
    { metadata: { name: "payments-prod", uid: "uid-B" },
      spec: { role: "source", bootstrapServers: ["kafka-b:9093"] }, status: {} },
  ],
});

const DESTINATIONS = Object.freeze([
  { name: "primary", uid: "d-1", default: true, canonicalUrl: "s3://kafka-backups/poc",
    transport: "insecureHttp" },
  { name: "offsite", uid: "d-2", default: false, canonicalUrl: "s3://kafka-offsite/poc",
    transport: "insecureHttp" },
]);

/** A promise and the two functions that settle it. */
function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

const flush = () => new Promise((done) => setTimeout(done, 15));

const DISCOVERY = (id) => ({
  id: id, state: "succeeded", terminal: true, stale: false, observedAt: "2026-09-25T08:00:00Z",
  visibility: { state: "unknown" },
});

/** The schedules page's API, with the discovery reads held open per
 *  connection so a row decides when each one lands. */
function schedulesApi(extra) {
  const discoveries = {};
  const api = {
    sent: [],
    discoveries: discoveries,
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters" ? CLUSTERS : { items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS }); },
    latestDiscoveries(namespace, connection) {
      const held = deferred();
      (discoveries[connection] = discoveries[connection] || []).push(held);
      return held.promise;
    },
    discoveryTopics(namespace, id) {
      return Promise.resolve({ items: [{ name: id + "-topic" }] });
    },
    startPreflight(namespace, request, options) {
      api.sent.push({ request: request, options: options || {} });
      return Promise.resolve({ item: {
        id: "pf-" + String(api.sent.length), namespace: namespace, uid: "u", state: "pending",
        terminal: false, binding: { referents: [] }, applicable: false, stale: false,
        staleReasons: [], staleBasis: [], checks: [], warnings: [], executionOnly: [],
        detailsAvailable: false, conditions: [],
      }, replayed: false });
    },
    preflight(namespace, id) {
      return Promise.resolve({ item: { id: id, state: "running", terminal: false,
        binding: { referents: [] }, applicable: false, stale: false, staleReasons: [],
        staleBasis: [], checks: [], warnings: [], executionOnly: [], conditions: [] } });
    },
    wait: async () => {},
  };
  return Object.assign(api, extra || {});
}

/** Lands the newest discovery read for `connection`. */
async function land(api, connection) {
  const held = (api.discoveries[connection] || []).pop();
  assert.ok(held !== undefined, "a discovery read was started for " + connection);
  held.resolve({ latestAttempt: DISCOVERY("td-" + connection), lastSuccessful: DISCOVERY("td-" + connection) });
  await flush();
}

/** Chooses a source the way a browser does: the select's value, then its
 *  `change`. */
async function chooseSource(view, uid) {
  view.find("#readiness-source").value = uid;
  await view.find("#readiness-source").dispatch("change");
}

async function mounted(ns, api) {
  resetMode();
  resetCheckIntents();
  const view = fakeView();
  await mountSchedules(view.root, ns, parse, LIFE(), api);
  assert.ok(view.find("#readiness-form") !== null, "the list page offers the readiness panel");
  return view;
}

test("readiness_topics_typed_before_the_source_survive_the_discovery_repaint", async () => {
  // P13's measured order A: topics first, then the source.
  const api = schedulesApi();
  const view = await mounted("typed-a", api);
  view.find("#readiness-topics").value = "orders, payments";
  view.find("#readiness-source-search").value = "payments";
  await chooseSource(view, "uid-B");
  await land(api, "payments-prod");
  assert.equal(view.find("#readiness-topics").value, "orders, payments",
    "NEGATIVE CONTROL: the typed topics survive the repaint the discovery answer made ('' before)");
  assert.equal(view.find("#readiness-source").value, "uid-B",
    "NEGATIVE CONTROL: the chosen source stays chosen (it went back to the preferred uid-A)");
  assert.equal(view.find("#readiness-source-search").value, "payments",
    "the search text survives too");
  assert.match(view.html().split("id=\"backup-readiness\"").pop(), /td-payments-prod-topic/,
    "and the offer beside it is the chosen connection's");
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  assert.equal(api.sent.length, 1);
  assert.deepEqual(api.sent[0].request.backup.topics, ["orders", "payments"],
    "NEGATIVE CONTROL: the POST carries the typed topics (it carried [] and was refused 422)");
  assert.equal(api.sent[0].request.backup.sourceConnection, "payments-prod");
});

test("readiness_topics_typed_while_the_discovery_read_is_in_flight_survive_it", async () => {
  // P13's order C: the source first, then the topics at once, then the read lands.
  const api = schedulesApi();
  const view = await mounted("typed-c", api);
  await chooseSource(view, "uid-B");
  view.find("#readiness-topics").value = "orders";
  await land(api, "payments-prod");
  assert.equal(view.find("#readiness-topics").value, "orders",
    "NEGATIVE CONTROL: typed after the change and before the answer, and kept");
});

test("a_chosen_destination_survives_a_repaint_and_is_the_one_sent", async () => {
  const api = schedulesApi();
  const view = await mounted("typed-d", api);
  view.find("#readiness-destination").value = "d-2";
  await view.find("#readiness-destination").dispatch("change");
  view.find("#readiness-topics").value = "orders";
  await chooseSource(view, "uid-A");
  await land(api, "orders-prod");
  assert.equal(view.find("#readiness-destination").value, "d-2",
    "NEGATIVE CONTROL: the destination stays the one chosen (the default came back)");
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  assert.equal(api.sent[0].request.backup.destination, "offsite",
    "NEGATIVE CONTROL: the chosen destination is the one checked (the default was sent)");
  // And leaving the default alone still sends the default, and still says it was preselected.
  const plain = schedulesApi();
  const other = await mounted("typed-d2", plain);
  other.find("#readiness-topics").value = "orders";
  await chooseSource(other, "uid-A");
  await land(plain, "orders-prod");
  assert.match(other.html().split("id=\"backup-readiness\"").pop(), /readiness-destination-default/);
  await other.find("#readiness-form").dispatch("submit");
  await flush();
  assert.equal(plain.sent[0].request.backup.destination, "primary");
});

test("a_followed_checks_next_read_keeps_what_was_typed_since_the_check_started", async () => {
  const read = deferred();
  const api = schedulesApi({
    preflight(namespace, id) {
      return read.promise.then(() => ({ item: { id: id, state: "ready", terminal: true,
        binding: { referents: [] }, applicable: true, stale: false, staleReasons: [],
        staleBasis: ["expiry"], checks: [], warnings: [], executionOnly: [], conditions: [] } }));
    },
  });
  const view = await mounted("typed-f", api);
  view.find("#readiness-topics").value = "orders";
  await view.find("#readiness-form").dispatch("submit");
  await flush();
  // The check is running; the reader edits the topics for the next one.
  view.find("#readiness-topics").value = "orders, refunds";
  read.resolve();
  await flush();
  assert.match(view.html().split("id=\"backup-readiness\"").pop(), /badge-green">ready/,
    "the follow's read landed");
  assert.equal(view.find("#readiness-topics").value, "orders, refunds",
    "NEGATIVE CONTROL: the edit made while the check ran survives its read ('' before)");
});

test("a_late_discovery_answer_for_an_earlier_connection_is_not_offered_beside_the_next", async () => {
  const api = schedulesApi();
  const view = await mounted("typed-late", api);
  await chooseSource(view, "uid-B");
  await chooseSource(view, "uid-A");
  await land(api, "orders-prod");
  await land(api, "payments-prod");
  const panel = view.html().split("id=\"backup-readiness\"").pop();
  assert.match(panel, /td-orders-prod-topic/, "the selected connection's offer");
  assert.doesNotMatch(panel.slice(panel.lastIndexOf("topic-picker")), /td-payments-prod-topic/,
    "NEGATIVE CONTROL: the earlier choice's slower answer is dropped, not painted beside uid-A");
  assert.equal(view.find("#readiness-source").value, "uid-A");
});

// ------------------------------------------------------------ the class sweep

const CONNECTION = Object.freeze({
  metadata: { name: "orders-prod", uid: "uid-A", generation: 1 },
  spec: { role: "source", bootstrapServers: ["kafka-a:9093"] },
  status: {},
});

test("the_connection_detail_keeps_typed_discovery_and_filter_input_across_a_check_read", async () => {
  // THE CLASS on Clusters -> a connection: "Test connection" follows its check
  // and every read repaints the whole detail, the discovery form and the
  // topic filters with it.
  resetMode();
  resetCheckIntents();
  const read = deferred();
  const api = {
    get() { return Promise.resolve(CONNECTION); },
    latestDiscoveries() {
      return Promise.resolve({ latestAttempt: DISCOVERY("td-1"), lastSuccessful: DISCOVERY("td-1") });
    },
    startPreflight() {
      return Promise.resolve({ item: { id: "pf-c", state: "pending", terminal: false,
        binding: { referents: [] }, applicable: false, stale: false, staleReasons: [],
        staleBasis: [], checks: [], warnings: [], executionOnly: [], conditions: [] },
      replayed: false });
    },
    preflight(namespace, id) {
      return read.promise.then(() => ({ item: { id: id, state: "ready", terminal: true,
        binding: { referents: [] }, applicable: true, stale: false, staleReasons: [],
        staleBasis: ["expiry"], checks: [], warnings: [], executionOnly: [], conditions: [] } }));
    },
    wait: async () => {},
  };
  const view = fakeView();
  await mountClusterDetail(view.root, "sweep-c", "orders-prod", parse, LIFE(), api);
  const check = view.find("#connection-check-form");
  assert.ok(check !== null, "the detail offers Test connection");
  await check.dispatch("submit");
  await flush();
  view.find("#discovery-expected").value = "orders, payments";
  view.find("#topic-q").value = "ord";
  view.find("#topic-prefix").value = "or";
  read.resolve();
  await flush();
  assert.match(view.html(), /pf-c/, "the check's read landed and repainted the detail");
  assert.equal(view.find("#discovery-expected").value, "orders, payments",
    "NEGATIVE CONTROL: the expected topics typed while the check ran survive its read");
  assert.equal(view.find("#topic-q").value, "ord", "NEGATIVE CONTROL: the typed filter survives");
  assert.equal(view.find("#topic-prefix").value, "or");
});

const DESTINATION = Object.freeze({
  name: "primary", uid: "d-1", generation: 1, canonicalUrl: "s3://kafka-backups/poc",
  transport: "insecureHttp", access: {}, conditions: [],
});

test("the_destination_detail_keeps_a_typed_rotation_across_a_test_read", async () => {
  // THE CLASS on Destinations -> a destination: "Test access" follows its
  // check and every read repaints the detail, the rotation form with it --
  // whose credential inputs no draft may keep.
  resetMode();
  resetCheckIntents();
  const read = deferred();
  const api = {
    destination() { return Promise.resolve({ item: DESTINATION }); },
    destinationUsage() { return Promise.resolve({ schedules: [], backups: [] }); },
    testDestination() {
      return Promise.resolve({ item: { id: "pf-t", state: "pending", terminal: false,
        binding: { referents: [] }, applicable: false, stale: false, staleReasons: [],
        staleBasis: [], checks: [], warnings: [], executionOnly: [], conditions: [] },
      replayed: false });
    },
    preflight(namespace, id) {
      return read.promise.then(() => ({ item: { id: id, state: "ready", terminal: true,
        binding: { referents: [] }, applicable: true, stale: false, staleReasons: [],
        staleBasis: ["expiry"], checks: [], warnings: [], executionOnly: [], conditions: [] } }));
    },
    wait: async () => {},
  };
  const view = fakeView();
  await mountDestinationDetail(view.root, "sweep-d", "primary", parse, LIFE(), api);
  const form = view.find("#destination-test-form");
  assert.ok(form !== null, "the detail offers Test access");
  await form.dispatch("submit");
  await flush();
  // The rotation form's inputs, credential halves included (no value
  // attribute, no draft: only an un-re-rendered input can keep them).
  const typedInputs = view.findAll("input").filter((i) =>
    String(i.attributes.id || "").startsWith("rotate-") &&
    /(AccessKeyId|SecretAccessKey|Secret|ServiceAccount)$/.test(String(i.attributes.name || "")));
  assert.ok(typedInputs.filter((i) => /SecretAccessKey$/.test(i.attributes.name)).length === 4,
    "the rotation form has a secret-key input per grant to type into");
  const typed = typedInputs.map((i) => [i.attributes.id, "typed-" + String(i.attributes.name)]);
  for (const [id, value] of typed) {
    view.find("#" + id).value = value;
  }
  read.resolve();
  await flush();
  assert.match(view.html(), /pf-t[\s\S]*badge-green">ready/, "the test's read landed and was painted");
  for (const [id, value] of typed) {
    assert.equal(view.find("#" + id).value, value,
      "NEGATIVE CONTROL: " + id + " typed while the test ran survives its read");
  }
});
