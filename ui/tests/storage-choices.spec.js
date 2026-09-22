// storage-choices.spec.js -- PLAT-08.2's behaviour arm: destination defaults
// are inherited, storage choices are validated, the archive and the evidence
// store are separate choices, and transport security and path-style addressing
// are two independent explicit controls.
//
// EVERY ROW CARRIES ITS NEGATIVE CONTROL: an assertion that fails when the
// behaviour it is about is absent. The planted mutants that prove it are
// listed in the task's result document, with the row each one turns red.
//
// Pure functions from a JSON object to an HTML string, as everything in this
// directory is: no DOM, no network, no clock.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  applyWizardDraft,
  confirmFrozenDestination,
  evidenceDestinationOptions,
  evidenceStoreOf,
  initialState,
  mergeReadiness,
  preparePlan,
  readinessRefusal,
  recoveryPoints,
  renderStoreFields,
  restoreBody,
  selectEvidenceDestination,
  validateRestore,
  wizardDraftValues,
} from "../pages/restore-wizard.js";
import { CONSOLE, resetMode, selectMode } from "../client.js";
import { destinationBody, validateDestination } from "../pages/destinations.js";
import { dropDraft, formKey, readDraft } from "../lifecycle.js";
import { LIFE, fakeView, parse } from "./fake-view.js";
import {
  LOCATION_MOVE_REFUSAL,
  SCHEDULE_FORM,
  mountSchedules,
  chosenDestination,
  confirmDestination,
  confirmThenCreate,
  locationChange,
  locationOf,
  policyFormView,
  policyInputValue,
  policyValuesOf,
  renderPolicyForm,
  renderScheduleForm,
  submitPolicy,
} from "../pages/schedules.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

const PRIMARY = () => fixture("console/destination.json").item;

const EVIDENCE_B = () => Object.assign(clone(PRIMARY()), {
  name: "evidence-b",
  uid: "uid-evidence-b",
  default: false,
  canonicalUrl: "s3://lw-evidence-b",
  locationDigest: "sha256:" + "b".repeat(64),
  storage: { provider: "s3", bucket: "lw-evidence-b", prefix: "", region: "eu-west-1",
    endpoint: "http" + "://minio-b.local:9000", addressing: "pathStyle" },
  transport: { security: "insecureHttp" },
});

/** A destination-backed recovery point, built the way the page builds it. */
function savedState(destinations) {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  const destination = PRIMARY();
  point.spec.destinationRef = { name: "primary", uid: destination.uid };
  point.spec.archive = { url: "logweir-destination://primary" };
  point.status.locationDigest = destination.locationDigest;
  return initialState(
    "team-a", fixture("wizard-clusters.json"), backups,
    { uid: point.metadata.uid, backup: point.metadata.name }, destination, destinations,
  );
}

// ------------------------------------------------ the evidence destination

test("evidence_store_of_a_destination_is_its_own_bucket_under_logweir_with_its_own_transport", () => {
  // `BackupDestination::evidence_storage_url`, field for field: the same
  // bucket, Global Constraint 6's prefix, and the destination's OWN endpoint,
  // region, addressing and transport.
  assert.deepEqual(evidenceStoreOf(EVIDENCE_B()), {
    bucket: "lw-evidence-b", prefix: "logweir/", region: "eu-west-1",
    endpoint: "http" + "://minio-b.local:9000", pathStyle: true, allowHttp: true,
  });
  assert.equal(evidenceStoreOf(PRIMARY()).allowHttp, false,
    "a TLS destination's evidence store is TLS; the flag is read, never assumed");
  assert.equal(evidenceStoreOf({ name: "x" }), null, "no storage, no store");
});

test("the_evidence_selector_offers_the_points_own_destination_first_and_each_other_once", () => {
  const state = savedState([EVIDENCE_B(), PRIMARY(), EVIDENCE_B()]);
  assert.deepEqual(evidenceDestinationOptions(state).map((d) => d.name), ["primary", "evidence-b"]);
  assert.equal(state.evidenceDestination.uid, PRIMARY().uid, "inherited: the point's own");
  const html = renderStoreFields(state);
  assert.match(html, /id="evidence-destination"/);
  assert.match(html, /this recovery point&#39;s destination/);
  // NEGATIVE CONTROL: a legacy point has no evidence DESTINATION, only a store.
  const legacy = initialState("team-a", fixture("wizard-clusters.json"),
    fixture("wizard-backups.json"), (() => {
      const p = recoveryPoints(fixture("wizard-backups.json"))[0];
      return { uid: p.metadata.uid, backup: p.metadata.name };
    })());
  assert.doesNotMatch(renderStoreFields(legacy), /id="evidence-destination"/);
  assert.match(renderStoreFields(legacy), /id="evidence-same-store"/);
});

test("choosing_an_evidence_destination_moves_the_evidence_block_and_nothing_else", async () => {
  const state = savedState([PRIMARY(), EVIDENCE_B()]);
  const source = clone(state.fields.source);
  selectEvidenceDestination(state, "uid-evidence-b", "evidence-b");
  assert.deepEqual(state.fields.source, source, "the archive block is untouched");
  assert.deepEqual(state.fields.evidence, evidenceStoreOf(EVIDENCE_B()));
  const plan = await preparePlan(state);
  const body = restoreBody(state, Object.assign({}, plan, { restoreName: "r", approvalName: "a" }));
  assert.deepEqual(body.spec.sourceDestinationRef, { name: "primary" });
  assert.deepEqual(body.spec.evidenceDestinationRef, { name: "evidence-b" },
    "MUTANT: the body named the source destination twice");
  assert.equal(validateRestore(state).evidenceDestination, undefined);
});

test("a_recreated_evidence_destination_is_refused_and_signs_nothing", async () => {
  const state = savedState([PRIMARY(), EVIDENCE_B()]);
  const kept = Object.assign(wizardDraftValues(state), {
    evidenceDestination: "evidence-b", evidenceDestinationUid: "uid-that-is-gone",
  });
  assert.equal(applyWizardDraft(state, kept), true);
  assert.match(validateRestore(state).evidenceDestination, /not in this namespace any more/);
  await assert.rejects(() => preparePlan(state), /bucket is required/,
    "no plan can be built for an evidence store nobody can name");
  assert.match(renderStoreFields(state), /id="evidence-destination-refusal"/);
  // NEGATIVE CONTROL: the same draft with a uid that answers applies cleanly.
  const good = savedState([PRIMARY(), EVIDENCE_B()]);
  applyWizardDraft(good, Object.assign(kept, { evidenceDestinationUid: "uid-evidence-b" }));
  assert.equal(validateRestore(good).evidenceDestination, undefined);
  assert.equal(good.fields.evidence.bucket, "lw-evidence-b");
});

test("the_pre_submit_read_confirms_the_evidence_destination_by_uid_and_digest", async () => {
  const state = savedState([PRIMARY(), EVIDENCE_B()]);
  selectEvidenceDestination(state, "uid-evidence-b", "evidence-b");
  const live = { primary: PRIMARY(), "evidence-b": EVIDENCE_B() };
  const api = { destination: async (_ns, name) => ({ item: clone(live[name]) }) };
  assert.equal(await confirmFrozenDestination(state, api), true, "unchanged: confirmed");
  live["evidence-b"].uid = "uid-recreated";
  await assert.rejects(() => confirmFrozenDestination(state, api),
    (e) => /evidence destination evidence-b was recreated/.test((e.fields || {}).evidenceDestination));
  live["evidence-b"] = EVIDENCE_B();
  live["evidence-b"].locationDigest = "sha256:" + "c".repeat(64);
  await assert.rejects(() => confirmFrozenDestination(state, api),
    (e) => /moved/.test((e.fields || {}).evidenceDestination));
  live["evidence-b"] = null;
  await assert.rejects(() => confirmFrozenDestination(state, api),
    (e) => /is absent/.test((e.fields || {}).evidenceDestination));
});

// -------------------------------------------- readiness through a draft edit

test("staleness_is_one_way_on_the_page_until_a_new_check_is_started", () => {
  const held = { id: "pf-1", state: "ready", applicable: false, stale: true,
    staleReasons: [{ reason: "referentChanged", kind: "KafkaCluster", name: "t2" }] };
  const fresh = { id: "pf-1", state: "ready", applicable: true, stale: false, staleReasons: [] };
  const merged = mergeReadiness(held, fresh);
  assert.equal(merged.stale, true,
    "a mark about a choice the server cannot see survives a fresher read");
  assert.equal(merged.applicable, false);
  assert.deepEqual(merged.staleReasons, held.staleReasons);
  // NEGATIVE CONTROL: a held verdict that was NOT stale is replaced wholesale,
  // and a different check is never merged with the held one.
  assert.equal(mergeReadiness(fresh, held).stale, true, "a server stale answer wins too");
  assert.equal(mergeReadiness(Object.assign({}, held, { id: "pf-0" }), fresh).stale, false);
  const prepared = { hash: "sha256:" + "a".repeat(64) };
  const state = { readiness: { preflight: Object.assign({}, merged, {
    binding: { planHash: prepared.hash }, terminal: true }) } };
  assert.match(readinessRefusal(state, prepared), /no longer applies/);
});

// ------------------------------------------ the schedule form's destination

const DESTINATIONS = () => [
  Object.assign(PRIMARY(), { default: true }),
  EVIDENCE_B(),
];

const INLINE_SCHEDULE = () => ({
  metadata: { name: "nightly", generation: 3 },
  spec: {
    schedule: "0 2 * * *", sourceRef: { name: "orders" }, topics: ["orders"],
    archive: { url: "s3://kafka-backups/team-a/prod", secretRef: { name: "logweir-s3" } },
  },
});

test("a_new_schedule_inherits_the_namespace_default_destination_and_shows_what_it_hands_down", () => {
  const html = renderScheduleForm({ draft: null, clusters: { items: [] },
    destinations: DESTINATIONS(), mayOperate: true });
  assert.match(html, /<option value="primary" selected>/, "the default is preselected");
  assert.match(html, new RegExp("name=\"destinationUid\" value=\"" + PRIMARY().uid + "\""),
    "and pinned by uid");
  assert.match(html, /id="policy-create-destination-default"/);
  const inherited = /id="policy-create-destination-inherited"[\s\S]*?<\/div>/.exec(html);
  assert.ok(inherited !== null, "the inherited settings are shown beside the choice");
  assert.match(inherited[0], /https:\/\/minio\.storage\.svc:9000/);
  assert.match(inherited[0], /data-inherited="addressing">pathStyle/);
  assert.match(inherited[0], /data-inherited="transport">[^<]*<span[^>]*>tls/);
  assert.doesNotMatch(inherited[0], /<input/, "facts, never inputs: nothing is re-entered");

  // NEGATIVE CONTROLS. An operator who chose the inline archive keeps it; two
  // defaults are none; and no default means nothing is chosen for them.
  const inline = renderScheduleForm({ draft: { destination: "" }, clusters: { items: [] },
    destinations: DESTINATIONS(), mayOperate: true });
  assert.match(inline, /<option value="" selected>/);
  assert.doesNotMatch(inline, /id="policy-create-destination-inherited"/);
  const two = DESTINATIONS().map((d) => Object.assign(d, { default: true }));
  assert.match(renderScheduleForm({ draft: null, clusters: { items: [] }, destinations: two,
    mayOperate: true }), /<option value="" selected>/);
  const none = DESTINATIONS().map((d) => Object.assign(d, { default: false }));
  assert.doesNotMatch(renderScheduleForm({ draft: null, clusters: { items: [] },
    destinations: none, mayOperate: true }), /id="policy-create-destination-default"/);
});

test("a_destination_recreated_during_a_schedule_draft_is_refused_and_an_edited_one_is_not", async () => {
  const values = { destination: "primary", destinationUid: PRIMARY().uid };
  const recreated = DESTINATIONS();
  recreated[0].uid = "uid-recreated";
  assert.equal(chosenDestination(recreated, values).state, "recreated");
  const html = renderScheduleForm({ draft: Object.assign({}, values), clusters: { items: [] },
    destinations: recreated, mayOperate: true });
  assert.match(html, /id="policy-create-destination-refusal"/,
    "the form says the chosen destination is gone rather than following the name");
  await assert.rejects(() => confirmDestination("team-a", values,
    { destinations: async () => ({ items: recreated }) }),
  (e) => /different object now answers/.test((e.fields || {}).destination));
  await assert.rejects(() => confirmDestination("team-a", values,
    { destinations: async () => ({ items: [] }) }),
  (e) => /not in this namespace any more/.test((e.fields || {}).destination));
  await assert.rejects(() => confirmDestination("team-a", values,
    { destinations: async () => { throw new Error("gateway timeout"); } }),
  (e) => /could not be read again.*gateway timeout/.test((e.fields || {}).destination));

  // A KEPT UID THAT BELONGS TO ANOTHER DESTINATION is not followed onto it:
  // Kubernetes names are immutable, so the uid answering under a different
  // name means the pin and the name disagree, and the page refuses.
  assert.equal(chosenDestination(DESTINATIONS(),
    { destination: "evidence-b", destinationUid: PRIMARY().uid }).state, "recreated");
  // NEGATIVE CONTROL: an EDIT -- an access rotation, a new generation -- keeps
  // the uid and the location, and is not a refusal.
  const edited = DESTINATIONS();
  edited[0].generation = 7;
  const confirmed = await confirmDestination("team-a", values,
    { destinations: async () => ({ items: edited }) });
  assert.equal(confirmed.destination, "primary");
  assert.equal(confirmed.destinationUid, PRIMARY().uid);
  assert.match(renderScheduleForm({ draft: Object.assign({}, values), clusters: { items: [] },
    destinations: edited, mayOperate: true }), /at revision <code>g7<\/code>/,
  "and the form shows the revision it read");
});

test("the_create_submit_confirms_the_destination_before_anything_is_sent", async () => {
  const created = [];
  const recreated = DESTINATIONS();
  recreated[0].uid = "uid-recreated";
  const api = {
    list: async () => ({ items: [{ metadata: { name: "orders", uid: "uid-orders" },
      spec: { role: "source", bootstrapServers: ["k:9092"] } }] }),
    destinations: async () => ({ items: recreated }),
    create: async (_ns, _plural, body) => { created.push(body); return body; },
    get: async () => { throw Object.assign(new Error("not found"), { status: 404 }); },
  };
  const values = { name: "nightly", source: "orders", sourceUid: "uid-orders", mode: "advanced",
    cron: "0 2 * * *", selection: "named", topics: "orders", destination: "primary",
    destinationUid: PRIMARY().uid };
  await assert.rejects(() => confirmThenCreate("team-a", values, api, null),
    (e) => /different object now answers/.test((e.fields || {}).destination));
  assert.equal(created.length, 0, "a recreated destination sends nothing");
  // NEGATIVE CONTROL: the same draft against the destination it chose is sent.
  api.destinations = async () => ({ items: DESTINATIONS() });
  await confirmThenCreate("team-a", values, api, null);
  assert.equal(created.length, 1);
  assert.deepEqual(created[0].spec.destinationRef, { name: "primary" });
});

test("converting_an_inline_schedule_keeps_its_location_or_says_it_moves", async () => {
  const current = { destination: "", archive: "s3://kafka-backups/team-a/prod" };
  const list = DESTINATIONS();
  assert.equal(locationOf({ destination: "primary" }, list), "s3://kafka-backups/team-a/prod");
  assert.equal(locationChange(current, { destination: "primary" }, list).state, "same",
    "inline s3://kafka-backups/team-a/prod to a destination at that location does not move it");
  assert.equal(locationChange(current, { destination: "evidence-b" }, list).state, "moves");
  assert.equal(locationChange(current, { destination: "not-read" }, list).state, "unknown",
    "a comparison the page cannot make is never reported as unchanged");
  assert.equal(locationChange(current, { destination: "", archive: current.archive }, list).state,
    "unchanged");
  assert.equal(locationChange({ destination: "primary" }, { destination: "evidence-b" }, list).state,
    "moves", "one destination to another is a move too");

  const view = policyFormView("team-a", INLINE_SCHEDULE(), {}, list, true);
  const same = renderPolicyForm(Object.assign(view, {
    values: Object.assign(view.values, { destination: "primary" }) }));
  assert.match(same, /id="policy-nightly-location-same"/);
  assert.doesNotMatch(same, /name="moveLocation"/);

  const sent = [];
  const api = { editSchedulePolicy: async (_ns, _name, body) => { sent.push(body); return body; } };
  const values = Object.assign(policyValuesOf(INLINE_SCHEDULE()), { destination: "evidence-b" });
  await assert.rejects(() => submitPolicy("team-a", INLINE_SCHEDULE(), values, null, api, list),
    (e) => (e.fields || {}).moveLocation === LOCATION_MOVE_REFUSAL);
  assert.equal(sent.length, 0,
    "NEGATIVE CONTROL: a move nobody acknowledged is not sent");
  await submitPolicy("team-a", INLINE_SCHEDULE(), Object.assign({}, values, { moveLocation: "true" }),
    null, api, list);
  assert.equal(sent.length, 1, "the explicit box lets the move through");
  assert.deepEqual(sent[0].destinationRef, { name: "evidence-b" });
  await submitPolicy("team-a", INLINE_SCHEDULE(),
    Object.assign(policyValuesOf(INLINE_SCHEDULE()), { destination: "primary" }), null, api, list);
  assert.equal(sent.length, 2, "the location-preserving conversion needs no box");
  assert.deepEqual(sent[1].destinationRef, { name: "primary" });
  assert.equal(sent[1].archive, undefined, "one location, never both");
});

test("a_checkbox_policy_input_is_its_checked_state_not_its_value", () => {
  const box = { type: "checkbox", value: "true", checked: false,
    getAttribute: (n) => (n === "type" ? "checkbox" : null) };
  assert.equal(policyInputValue(box), "", "an unticked box is no acknowledgement");
  box.checked = true;
  assert.equal(policyInputValue(box), "true");
  assert.equal(policyInputValue({ value: "x", getAttribute: () => null }), "x");
});

test("choosing_a_destination_on_the_mounted_forms_pins_its_uid_and_guards_a_move", async () => {
  selectMode(CONSOLE);
  const ns = "storage-choices-mount";
  dropDraft(formKey(ns, SCHEDULE_FORM));
  const view = fakeView();
  const sent = [];
  const schedule = Object.assign(INLINE_SCHEDULE(), {});
  schedule.metadata.namespace = ns;
  const api = {
    list(_namespace, plural) {
      if (plural === "backupschedules") {
        return Promise.resolve({ items: [clone(schedule)] });
      }
      return Promise.resolve({ items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS() }); },
    editSchedulePolicy(_ns, _name, body) { sent.push(body); return Promise.resolve(body); },
  };
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), api);
    // THE CREATE FORM: the default is inherited, and a new choice re-pins.
    const create = view.find("#policy-create-destination");
    assert.equal(create.value, "primary", "the namespace default is preselected on mount");
    create.value = "evidence-b";
    await create.dispatch("change");
    assert.equal(readDraft(formKey(ns, SCHEDULE_FORM)).destinationUid, "uid-evidence-b",
      "MUTANT: a changed choice kept the previous destination's uid");
    assert.match(view.html(), /data-destination-uid="uid-evidence-b"/,
      "the inherited settings followed the choice");

    // THE POLICY PANEL: an inline schedule pointed at another location is
    // refused until the move is acknowledged.
    const select = view.find("#policy-nightly-destination");
    select.value = "evidence-b";
    await select.dispatch("change");
    assert.match(view.html(), /id="policy-nightly-location-move"/);
    const form = view.find("form.policy-form[data-name=\"nightly\"]");
    await form.dispatch("submit");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(sent.length, 0, "NEGATIVE CONTROL: the unacknowledged move was not sent");
  } finally {
    dropDraft(formKey(ns, SCHEDULE_FORM));
    resetMode();
  }
});

// ------------------------------------------------- the destination form

test("a_custom_endpoint_needs_path_style_and_the_page_says_so_without_changing_either", () => {
  const base = {
    name: "p", bucket: "kafka-backups", archiveWriteSource: "existing", archiveWriteSecret: "s3",
  };
  const https = "https" + ":" + "//" + "minio.storage.svc:9000";
  const refused = validateDestination(Object.assign({}, base,
    { addressing: "virtualHosted", security: "tls", endpoint: https }));
  assert.match(refused.addressing, /virtualHosted addressing with a custom endpoint is refused/);
  assert.equal(refused.security, undefined, "the transport is not what is wrong");
  // NEGATIVE CONTROLS: path-style against the same endpoint is accepted, and so
  // is virtual-hosted against AWS S3 (no endpoint), in both transports' TLS.
  assert.deepEqual(Object.keys(validateDestination(Object.assign({}, base,
    { addressing: "pathStyle", security: "tls", endpoint: https }))), []);
  assert.deepEqual(Object.keys(validateDestination(Object.assign({}, base,
    { addressing: "virtualHosted", security: "tls" }))), []);
  // HTTPS WITH PATH-STYLE and EXPLICIT LOCAL HTTP are each one body, and each
  // control lands in its own field.
  const httpsPath = destinationBody(Object.assign({}, base,
    { addressing: "pathStyle", security: "tls", endpoint: https }));
  assert.deepEqual([httpsPath.storage.addressing, httpsPath.transport.security],
    ["pathStyle", "tls"]);
  const localHttp = destinationBody(Object.assign({}, base, { addressing: "pathStyle",
    security: "insecureHttp", endpoint: "http" + ":" + "//" + "minio.local:9000" }));
  assert.deepEqual([localHttp.storage.addressing, localHttp.transport.security],
    ["pathStyle", "insecureHttp"]);
});

// ------------------------------------ the list is summaries (found live, run 3)

const summaryOf = (d) => ({
  name: d.name, uid: d.uid, generation: d.generation, canonicalUrl: d.canonicalUrl,
  endpoint: (d.storage || {}).endpoint, transport: (d.transport || {}).security,
  addressing: (d.storage || {}).addressing, status: d.status, default: d.default === true,
});

test("the_schedule_form_reads_the_inherited_endpoint_and_transport_off_a_destination_summary", () => {
  const html = renderScheduleForm({ draft: null, clusters: { items: [] },
    destinations: DESTINATIONS().map(summaryOf), mayOperate: true });
  const inherited = /id="policy-create-destination-inherited"[\s\S]*?<\/div>/.exec(html)[0];
  assert.match(inherited, /https:\/\/minio\.storage\.svc:9000/,
    "NEGATIVE CONTROL: the first live run showed '- (AWS S3)' for a summary with an endpoint");
  assert.doesNotMatch(inherited, /AWS S3/);
  assert.match(inherited, /data-inherited="addressing">pathStyle/);
  assert.match(inherited, /data-inherited="transport">[^<]*<span[^>]*>tls/);
  assert.match(inherited, /on the destination's own page/,
    "a fact the summary does not publish is said to be elsewhere, never defaulted");
});

test("an_evidence_destination_offered_from_a_summary_is_signed_only_from_its_full_read", async () => {
  const state = savedState([PRIMARY(), EVIDENCE_B()].map(summaryOf));
  assert.deepEqual(evidenceDestinationOptions(state).map((d) => d.name), ["primary", "evidence-b"],
    "a summary is offered");
  const before = clone(state.fields.evidence);
  selectEvidenceDestination(state, "uid-evidence-b", "evidence-b");
  assert.match(state.evidenceDestinationProblem, /could not be read in full/,
    "a summary alone signs nothing");
  assert.equal(state.fields.evidence.bucket, "");
  selectEvidenceDestination(state, "uid-evidence-b", "evidence-b", EVIDENCE_B());
  assert.equal(state.evidenceDestinationProblem, null);
  assert.deepEqual(state.fields.evidence, evidenceStoreOf(EVIDENCE_B()));
  assert.notDeepEqual(state.fields.evidence, before);
  // A READ THAT ANSWERS WITH ANOTHER UID is a recreated destination.
  const other = savedState([PRIMARY(), EVIDENCE_B()].map(summaryOf));
  selectEvidenceDestination(other, "uid-evidence-b", "evidence-b",
    Object.assign(EVIDENCE_B(), { uid: "uid-recreated" }));
  assert.match(other.evidenceDestinationProblem, /could not be read in full/);
  assert.equal(other.fields.evidence.bucket, "");
});
