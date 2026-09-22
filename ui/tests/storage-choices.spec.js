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
