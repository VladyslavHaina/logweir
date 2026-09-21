// d1.spec.js -- D1 W7: the cadence policy, its previews, a run's trigger and
// revision, coverage labels, and Back up now.
//
// EVERY FIXTURE HERE IS A REAL OBJECT OR A REAL RESPONSE. The legacy custom
// resources come from D1 W8's live docker-desktop run
// (`artifacts/d1-live/20260918t0330z` and `.../20260918t1209z`): a schedule
// with a time zone, previews, a last slot and a missed-slot count; a retry and
// its `retryOf`; a catch-up; and a dynamic run whose discovery failed, with
// the `TopicsResolved=False` condition the controller wrote. The console
// answers come from D1 W6's live smoke (`artifacts/d1w6`): two cadence
// previews across a real DST transition, a manual run, its replay, and a
// `409 policy_changed`. `ui/tests/fixtures/README.md` records where each one
// came from, and `contract.spec.js` holds every console fixture to
// `schemas/logweir-api-v1.openapi.json`.
//
// THE ONE FIXTURE THAT IS NOT A CAPTURE is `backup-visible-only.json`, and it
// says so in its own row below: no live run reached `VisibleUserTopicsOnly`
// (D1's L-09-5 is the scenario that produces one, and W8's dynamic runs ended
// at their discovery deadline instead), so that object is a live run's SHAPE
// with the CRD's own declared selection block filled in. It is the only place
// in this file where a value was not observed.
//
// FIVE SENTENCES THIS PAGE REFUSES TO WRITE, and every row below is one of
// them held down:
//
//   1. A cron expression this browser computed. There is none: a preset is
//      compiled by the API and the expression it returns is what gets saved.
//   2. A firing count that hides the second occurrence of a repeated local
//      hour. Both are shown, with their offsets and their markers.
//   3. "revision 0" for a run that recorded none.
//   4. "all topics" for a coverage that is not `AllUserTopicsAttested`.
//   5. A second run from one intent.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  ADJUSTMENT_WORDS,
  NO_NEXT_RUNS_SENTENCE,
  NO_REVISION_SENTENCE,
  NO_TRIGGER_SENTENCE,
  STALE_NEXT_RUNS_SENTENCE,
  UTC_FALLBACK_NOTE,
  nextRunsPanel,
  revisionLine,
  triggerBadge,
  triggerLabel,
} from "../render.js";
import {
  ADVANCED_CRON,
  CADENCE_PRESETS,
  COVERAGE_LABELS,
  LAST_SLOT_NOT_PUBLISHED,
  NO_GENERATION_SENTENCE,
  POLICY_DRAFT_FIELDS,
  PREVIEW_BEFORE_SAVE,
  PREVIEW_COUNT,
  SELECTION_MODES,
  WHOLE_POLICY_SENTENCE,
  cadenceModeOf,
  coverageCell,
  discoveryFailure,
  RUN_AGAIN_SENTENCE,
  RUN_NOW_DRAFT_FIELDS,
  RUN_NOW_FORM,
  intentFor,
  manualRunsOf,
  mintIntent,
  newIntent,
  policyBody,
  policyValuesOf,
  presetOf,
  previewMatches,
  previewQueryFor,
  renderActiveRuns,
  renderCoverageLine,
  renderLastSlot,
  renderPolicyForm,
  renderRunNowPanel,
  renderRunNowResult,
  renderRunNowConflict,
  renderScheduleRevision,
  offersAnotherRun,
  submitPolicy,
  submitRunNow,
  validatePolicy,
} from "../pages/schedules.js";
import { mountSchedules } from "../pages/schedules.js";
import { renderBackupDetail, renderBackupList } from "../pages/backups.js";
import {
  MANUAL_BACKUP_ROUTE,
  apiClient,
  manualBackupName,
  resetMode,
  selectMode,
} from "../client.js";
import { dropDraft, formKey, mutationFor, readDraft } from "../lifecycle.js";
import { decodeCadencePreview, decodeManualBackup, decodePolicyChanged } from "../contract.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

function console_(name) {
  return fixture("console/" + name);
}

const SCHEDULE = fixture("schedule-policy.json");
const PRESETS = fixture("cadence-presets.json");

// ===========================================================================
// 1. PLAT-04.2 -- the presets, and the catalogue they are a display of
// ===========================================================================

test("the_forms_preset_catalogue_is_the_one_rust_emits", () => {
  // THE DRIFT ARM. `weirkeeper::cadence::presets` is the single catalogue and
  // `crates/weirkeeper/tests/cadence.rs` writes it to
  // `ui/tests/fixtures/cadence-presets.json`. The form holds its own copy --
  // there is no import from Rust into a browser -- so a preset that gained a
  // parameter, changed a bound or was renamed on that side must go red here
  // rather than become a 422 in front of an operator.
  assert.deepEqual(
    CADENCE_PRESETS.map((preset) => preset.kind),
    PRESETS.presets.map((preset) => preset.kind),
    "the five preset kinds, in the catalogue's own order",
  );
  for (const published of PRESETS.presets) {
    const held = presetOf(published.kind);
    assert.ok(held !== undefined, published.kind + " is offered by the form");
    assert.deepEqual(
      held.parameters.map((p) => [p.name, p.min, p.max]),
      published.parameters.map((p) => [p.name, p.min, p.max]),
      published.kind + ": the parameters and their bounds",
    );
    for (let i = 0; i < published.parameters.length; i += 1) {
      const there = published.parameters[i].values;
      const here = held.parameters[i].values;
      assert.deepEqual(
        here === undefined ? undefined : here.slice(),
        there === undefined ? undefined : there.slice(),
        published.kind + "." + published.parameters[i].name + ": the permitted values",
      );
    }
  }
});

test("the_form_holds_no_cron_template_and_compiles_nothing", () => {
  // THE CATALOGUE'S `cronTemplate` IS DELIBERATELY NOT COPIED INTO THE PAGE.
  // Filling it in would BE a browser-side cron compiler, which D1 section 4.2
  // forbids in as many words, and the preview route exists so that it does not
  // have to be. The fixture keeps the templates as evidence of what the server
  // produces; the form has no such field at all.
  for (const published of PRESETS.presets) {
    assert.ok(typeof published.cronTemplate === "string",
      "the emitted catalogue still carries the templates");
    const held = presetOf(published.kind);
    assert.equal(held.cronTemplate, undefined,
      published.kind + ": the form holds no template, so it can compile no expression");
  }
  const source = readFileSync(fileURLToPath(new URL("../pages/schedules.js", import.meta.url)),
    "utf8");
  for (const placeholder of ["{minute}", "{hour}", "{dayOfWeek}", "{dayOfMonth}", "{n}"]) {
    assert.equal(source.indexOf(placeholder), -1,
      "the page carries no cron template placeholder " + placeholder + ", so it has nothing " +
        "to substitute into and no expression it could build");
  }
});

test("a_preset_is_previewed_as_a_preset_and_never_as_an_expression", () => {
  const values = Object.assign(policyValuesOf(SCHEDULE), {
    mode: "daily", hour: "2", minute: "0", timeZone: "Europe/Berlin", cron: "0 2 * * *",
  });
  const query = previewQueryFor(values);
  assert.deepEqual(query, {
    count: PREVIEW_COUNT, timeZone: "Europe/Berlin", preset: "daily", hour: 2, minute: 0,
  });
  assert.equal(query.schedule, undefined,
    "the route takes EXACTLY ONE of schedule or preset, and a form that sent both would be " +
      "a 400 the operator would have to decode; the typed cron line is not sent with a preset");
});

test("an_advanced_cron_is_previewed_as_an_expression_and_never_as_a_preset", () => {
  const values = Object.assign(policyValuesOf(SCHEDULE), {
    mode: ADVANCED_CRON, cron: "*/15 * * * *", timeZone: "",
  });
  assert.deepEqual(previewQueryFor(values), { count: PREVIEW_COUNT, schedule: "*/15 * * * *" });
});

test("a_preset_with_a_parameter_missing_produces_no_query_at_all", () => {
  const values = Object.assign(policyValuesOf(SCHEDULE), { mode: "weekly", dayOfWeek: "", hour: "2", minute: "0" });
  assert.equal(previewQueryFor(values), null,
    "an incomplete preset asks nothing rather than asking a question with a blank in it");
});

test("the_preset_a_saved_expression_is_comes_from_the_server_and_not_from_this_page", () => {
  const matched = console_("schedule-preset.json").item;
  assert.equal(cadenceModeOf({ __preset: matched.preset }), "daily");
  // AND AN UNMATCHED EXPRESSION IS ADVANCED CRON, which is what every schedule
  // in legacy mode is: there is no preview route in front of kubectl proxy, so
  // nothing matched the expression, and the form opens on the expression it
  // actually stores rather than on a preset this page inferred.
  assert.equal(cadenceModeOf(SCHEDULE), ADVANCED_CRON);
  assert.equal(cadenceModeOf({ __preset: { kind: "fortnightly", minute: 0 } }), ADVANCED_CRON,
    "a preset kind this build does not offer opens on Advanced cron, not on a blank selector");
});

// ===========================================================================
// 2. PLAT-04.2 -- the previews, and the DST instants they must not hide
// ===========================================================================

test("a_repeated_local_hour_shows_both_firings_with_their_offsets", () => {
  // THE FIXTURE IS THE LIVE RESPONSE. Europe/Berlin, 2026-10-25, `30 2 * * *`:
  // the local time 02:30 happens twice, the schedule fires at BOTH instants,
  // and D1 section 4.3's table says so instant for instant.
  const answer = decodeCadencePreview(console_("cadence-preview-repeated.json")).value;
  const html = nextRunsPanel({
    runs: answer.runs, timeZone: answer.timeZone, tzdb: answer.tzdb,
  });
  assert.match(html, /2026-10-25T00:30:00Z/);
  assert.match(html, /2026-10-25T01:30:00Z/);
  assert.match(html, /2026-10-25T02:30:00\+02:00/);
  assert.match(html, /2026-10-25T02:30:00\+01:00/);
  // THE MARKER IS IN THE ROW AND THE SENTENCE IS UNDER THE TABLE, and both are
  // asserted. The first cut of this row checked only the sentence, and a
  // mutant that emptied the DST cell survived it: a reader scanning the table
  // would have seen two identical-looking rows and no reason for the second.
  assert.match(html, /<span class="badge badge-pending">RepeatedLocalTimeFirst<\/span>/,
    "the DST cell of the first row carries the marker");
  assert.match(html, /<span class="badge badge-pending">RepeatedLocalTimeSecond<\/span>/,
    "and the second row carries the other one");
  assert.ok(html.indexOf(ADJUSTMENT_WORDS.RepeatedLocalTimeFirst) !== -1,
    "and each marker carries the sentence that says what it means for that instant");
  assert.ok(html.indexOf(ADJUSTMENT_WORDS.RepeatedLocalTimeSecond) !== -1);
  assert.match(html, /chrono-tz 0\.10\.4/, "with the tz database the answer was computed against");
});

test("a_nonexistent_local_time_says_the_run_is_at_the_end_of_the_gap", () => {
  const answer = decodeCadencePreview(console_("cadence-preview-gap.json")).value;
  const html = nextRunsPanel({ runs: answer.runs, timeZone: answer.timeZone });
  assert.match(html, /2027-03-28T01:00:00Z/);
  assert.match(html, /2027-03-28T03:00:00\+02:00/);
  assert.match(html, /<span class="badge badge-pending">NonexistentLocalTimeShifted<\/span>/);
  assert.ok(html.indexOf(ADJUSTMENT_WORDS.NonexistentLocalTimeShifted) !== -1);
});

test("a_zone_that_was_never_named_says_so_instead_of_reading_as_a_choice", () => {
  const html = nextRunsPanel({ runs: [], timeZone: "" });
  assert.ok(html.indexOf(UTC_FALLBACK_NOTE) !== -1);
  assert.match(html, /data-utc-fallback="1"/);
  const named = nextRunsPanel({ runs: [], timeZone: "Asia/Kathmandu" });
  assert.equal(named.indexOf("data-utc-fallback"), -1,
    "a schedule that names a zone gets no fallback note");
});

test("an_empty_list_of_firings_is_an_answer_and_an_absent_one_is_not", () => {
  const empty = nextRunsPanel({ runs: [], timeZone: "UTC" });
  assert.match(empty, /data-next-runs="0"/);
  assert.ok(empty.indexOf(NO_NEXT_RUNS_SENTENCE) !== -1);
  const absent = nextRunsPanel({ runs: null });
  assert.match(absent, /data-next-runs="absent"/);
  assert.ok(absent.indexOf(NO_NEXT_RUNS_SENTENCE) === -1,
    "an absent list is not rendered as 'no further firing': nothing has computed one");
  assert.match(absent, /will not compute them either/);
});

test("staleness_is_the_first_firing_in_the_past_and_never_the_evaluation_instant", () => {
  const runs = SCHEDULE.status.nextRuns;
  const after = Date.parse(runs[0].at) + 1000;
  const before = Date.parse(runs[0].at) - 1000;
  const stale = nextRunsPanel({ runs: runs, timeZone: "Asia/Kathmandu", now: after });
  assert.match(stale, /data-stale="1"/);
  assert.ok(stale.indexOf(STALE_NEXT_RUNS_SENTENCE) !== -1);
  const fresh = nextRunsPanel({ runs: runs, timeZone: "Asia/Kathmandu", now: before });
  assert.equal(fresh.indexOf("data-stale"), -1);
  // AND WITH NO CLOCK AT ALL THERE IS NO VERDICT. Every other view in this
  // tree renders without reading one, and this one keeps that property: a
  // caller that supplies no `now` gets the table and no staleness claim.
  const clockless = nextRunsPanel({ runs: runs, timeZone: "Asia/Kathmandu" });
  assert.equal(clockless.indexOf("data-stale"), -1);
  assert.ok(STALE_NEXT_RUNS_SENTENCE.indexOf("evaluatedAt is NOT one") !== -1,
    "and the sentence says which field is NOT the staleness signal, because reading " +
      "status.policy.evaluatedAt as a heartbeat would label every healthy schedule stale");
});

// ===========================================================================
// 3. PLAT-05.1 -- the revision, and what a running run keeps showing
// ===========================================================================

test("the_card_prints_the_revision_in_force_and_the_digest_beside_it", () => {
  const html = renderScheduleRevision(SCHEDULE);
  assert.match(html, /revision g1/);
  assert.match(html, new RegExp(SCHEDULE.status.policy.runPolicySha256.slice(0, 20)));
  assert.match(html, /Asia\/Kathmandu/);
  assert.match(html, /chrono-tz 0\.10\.4/);
});

test("a_generation_the_controller_has_not_evaluated_is_named_as_that", () => {
  const ahead = JSON.parse(JSON.stringify(SCHEDULE));
  ahead.metadata.generation = 8;
  ahead.status.observedGeneration = 7;
  const html = renderScheduleRevision(ahead);
  assert.match(html, /not yet evaluated/);
  assert.match(html, /evaluated revision g7/);
  assert.match(html, /is not yet the policy that schedules/);
  const level = JSON.parse(JSON.stringify(ahead));
  level.status.observedGeneration = 8;
  assert.equal(renderScheduleRevision(level).indexOf("not yet evaluated"), -1);
});

test("a_run_that_recorded_no_revision_is_never_revision_zero", () => {
  assert.ok(revisionLine(undefined).indexOf(NO_REVISION_SENTENCE) !== -1);
  assert.ok(revisionLine({ name: "nightly" }).indexOf(NO_REVISION_SENTENCE) !== -1,
    "a scheduleRef with only a name recorded no revision");
  assert.equal(revisionLine({ name: "n", generation: 0 }).indexOf(NO_REVISION_SENTENCE), -1);
  assert.match(revisionLine({ name: "n", generation: 0 }), /revision g0/,
    "and revision g0 IS a revision -- it is the absence of the field, not the number, " +
      "that means 'not recorded'");
});

test("a_running_run_keeps_the_revision_it_froze_while_the_schedule_moves_on", () => {
  // THE PLAT-05.1 INVARIANT, ON SCREEN. The schedule is at g9; the run in
  // flight froze g7 and is not touched by the edit. The panel reads the RUNS
  // and not the schedule, so the two numbers being different is the invariant
  // rather than a rendering mistake.
  const object = JSON.parse(JSON.stringify(SCHEDULE));
  object.metadata.generation = 9;
  object.status.activeRuns = [{ name: "logweir-backup-tz-20260918-032200", kind: "Scheduled", attempt: 0 }];
  const backups = {
    items: [{
      metadata: { name: "logweir-backup-tz-20260918-032200" },
      spec: {
        trigger: { kind: "Scheduled", attempt: 0 },
        scheduleRef: { name: "tz", generation: 7, runPolicySha256: "sha256:0bc972dc" },
      },
    }],
  };
  const html = renderActiveRuns("d1w8", object, backups);
  assert.match(html, /data-active-runs="1"/);
  assert.match(html, /revision g7/, "the run's frozen revision, not the schedule's current one");
  assert.ok(html.indexOf("revision g9") === -1);
  assert.match(html, /keeps the revision it froze/);
});

test("an_absent_active_run_list_is_not_an_empty_one", () => {
  const object = JSON.parse(JSON.stringify(SCHEDULE));
  delete object.status.activeRuns;
  object.__contract = { mode: "console", absent: ["status.activeRuns"], unknown: [] };
  const html = renderActiveRuns("ns", object, { items: [] });
  assert.match(html, /data-active-runs="absent"/);
  assert.match(html, /an absent list is not an empty one/);
});

// ===========================================================================
// 4. PLAT-05.1 -- the policy form, and the replace it is built around
// ===========================================================================

test("the_panel_says_a_blank_field_is_a_field_being_removed", () => {
  const html = renderPolicyForm({
    name: "tz", generation: 1, values: policyValuesOf(SCHEDULE), mayOperate: true,
  });
  assert.ok(html.indexOf(WHOLE_POLICY_SENTENCE) !== -1);
  assert.match(html, /data-editing-generation="1"/);
  assert.match(WHOLE_POLICY_SENTENCE, /a field being REMOVED/);
  assert.match(WHOLE_POLICY_SENTENCE, /Runs already created are untouched/);
});

test("every_field_of_the_policy_is_on_screen_because_every_one_is_sent", () => {
  // THE FORM AND THE BODY ARE ONE LIST. A field in the draft that the body
  // never reads is a field a person edits and the API never hears about; a
  // field in the body that has no input is a field the form cannot express.
  // The two sets are compared mechanically, rather than by two people reading
  // two functions.
  const values = policyValuesOf(SCHEDULE);
  const html = renderPolicyForm({ name: "tz", generation: 1, values: values, mayOperate: true });
  for (const field of POLICY_DRAFT_FIELDS) {
    if (field === "cron" || field === "topics") {
      continue; // one of two shapes; covered by the two rows below
    }
    if (["minute", "hour", "dayOfWeek", "dayOfMonth", "n", "incompleteDiscovery",
      "excludeTopics", "excludePrefixes"].indexOf(field) !== -1) {
      continue; // present only in the shape that uses them
    }
    assert.ok(html.indexOf("name=\"" + field + "\"") !== -1,
      field + " has an input in the panel");
  }
  const body = policyBody(values, 1, "7 9 * * *");
  assert.equal(body.expectedGeneration, 1);
  assert.equal(body.schedule, "7 9 * * *");
  assert.equal(body.timeZone, "Asia/Kathmandu");
  assert.equal(body.activeDeadlineSeconds, 600);
  assert.deepEqual(body.topicSelection, { topics: ["t1"] });
  assert.equal(body.sourceRef, undefined,
    "sourceRef is on the DTO only to be refused, so this page never asks for that refusal");
});

test("a_chosen_destination_is_sent_instead_of_the_inline_archive_and_never_beside_it", () => {
  const values = Object.assign(policyValuesOf(SCHEDULE), {
    destination: "dest", archive: "s3://kafka/logweir", archiveSecret: "logweir-s3",
  });
  const body = policyBody(values, 1, "7 9 * * *");
  assert.deepEqual(body.destinationRef, { name: "dest" });
  assert.equal(body.archive, undefined,
    "the two are two spellings of one location; the CRD's sentinel rule refuses a real URL " +
      "beside a destination, and the API writes the sentinel itself");
  const inline = policyBody(
    Object.assign({}, values, { destination: "" }), 1, "7 9 * * *",
  );
  assert.deepEqual(inline.archive, {
    url: "s3://kafka/logweir", credentialRef: { name: "logweir-s3" },
  });
  assert.equal(inline.destinationRef, undefined);
});

test("a_dynamic_selection_sends_an_empty_topic_list_and_its_required_policy", () => {
  const values = Object.assign(policyValuesOf(SCHEDULE), {
    selection: "dynamic", incompleteDiscovery: "Refuse",
    excludeTopics: "skip-me", excludePrefixes: "pfx-",
  });
  const body = policyBody(values, 3, "7 9 * * *");
  assert.deepEqual(body.topicSelection, {
    topics: [],
    allUserTopics: {
      incompleteDiscovery: "Refuse",
      exclude: { topics: ["skip-me"], prefixes: ["pfx-"] },
    },
  });
});

test("a_dynamic_selection_with_no_incomplete_discovery_policy_is_refused_here", () => {
  const problems = validatePolicy(Object.assign(policyValuesOf(SCHEDULE), {
    selection: "dynamic", incompleteDiscovery: "",
  }));
  assert.match(problems.incompleteDiscovery, /There is no default/);
  assert.match(problems.incompleteDiscovery, /Refuse or BackUpVisibleTopics/);
});

test("a_blank_deadline_is_sent_as_nothing_and_never_as_the_documented_default", () => {
  // THE ROUTE REMOVES WHAT THE BODY OMITS, so "not set" and "set to 3600" are
  // different requests with different consequences: the first inherits the
  // default for ever, the second pins it. Prefilling the input with 3600 would
  // have turned every save into the second one.
  const values = Object.assign(policyValuesOf(SCHEDULE), {
    startingDeadlineSeconds: "", activeDeadlineSeconds: "", maxRetries: "",
  });
  const body = policyBody(values, 1, "7 9 * * *");
  assert.equal(body.startingDeadlineSeconds, undefined);
  assert.equal(body.activeDeadlineSeconds, undefined);
  assert.equal(body.retry, undefined);
  const html = renderPolicyForm({ name: "tz", generation: 1, values: values, mayOperate: true });
  assert.match(html, /Blank means the default, 3600 \(one hour\)/);
  assert.match(html, /Blank means no retries/);
});

test("a_preset_cannot_be_saved_until_the_server_has_compiled_it", async () => {
  const values = Object.assign(policyValuesOf(SCHEDULE), {
    mode: "daily", hour: "2", minute: "0",
  });
  const object = { metadata: { name: "tz", generation: 1 } };
  await assert.rejects(
    () => submitPolicy("ns", object, values, null, { editSchedulePolicy: () => assert.fail("sent") }),
    (error) => {
      assert.equal(error.kind, "invalid");
      assert.match(error.fields.mode, /Preview this cadence before saving it/);
      return true;
    },
  );
  // A PREVIEW OF A DIFFERENT QUESTION DOES NOT LICENSE THIS SAVE. The identity
  // is the QUERY, so a preview taken in Europe/Berlin is not a preview of the
  // same preset with no zone set -- the compiled expression is the same string
  // and the instants are not, and the panel says the preview is out of date
  // rather than saving under an answer to another question.
  const berlin = {
    query: previewQueryFor(Object.assign({}, values, { timeZone: "Europe/Berlin" })),
    answer: decodeCadencePreview(console_("cadence-preview-repeated.json")).value,
  };
  assert.ok(previewMatches(berlin, values) === false,
    "a preview taken for a different question does not license this save");
  // AND THE EXPRESSION SAVED IS THE ONE THE SERVER RETURNED, byte for byte.
  const matching = {
    query: previewQueryFor(values),
    answer: { schedule: "0 2 * * *", runs: [], timeZone: "UTC", tzdb: "chrono-tz 0.10.4" },
  };
  let sent = null;
  await submitPolicy("ns", object, values, matching, {
    editSchedulePolicy: (ns, name, body) => {
      sent = body;
      return Promise.resolve({});
    },
  });
  assert.equal(sent.schedule, "0 2 * * *");
  assert.ok(PREVIEW_BEFORE_SAVE.indexOf("this page does not compile one") !== -1);
});

test("a_schedule_with_no_published_revision_gets_no_form_at_all", () => {
  const html = renderPolicyForm({ name: "tz", generation: undefined, mayOperate: true });
  assert.match(html, /data-no-generation="1"/);
  assert.match(html, /the edit route takes the revision the form was opened at as a precondition/);
  assert.match(NO_GENERATION_SENTENCE, /lost update the precondition exists to prevent/);
  assert.equal(html.indexOf("<form"), -1,
    "no form at all, because the precondition IS the revision and a request without one " +
      "would ask the API to replace whatever revision happens to be current");
  assert.match(NO_GENERATION_SENTENCE, /lost update/);
});

test("a_login_that_may_not_operate_gets_words_and_not_a_disabled_control", () => {
  const html = renderPolicyForm({ name: "tz", generation: 1, mayOperate: false });
  assert.equal(html.indexOf("<form"), -1);
  assert.match(html, /may read this schedule and not edit its policy/);
});

// ===========================================================================
// 5. PLAT-06.2 -- Back up now
// ===========================================================================

test("one_intent_is_one_key_however_many_times_it_is_sent", () => {
  const key = formKey("intent-ns", RUN_NOW_FORM, "nightly");
  dropDraft(key);
  const first = intentFor(key);
  const again = intentFor(key);
  assert.equal(first, again,
    "a double click, a retry after a timeout and a Check status all resend ONE key");
  assert.ok(first.length >= 8 && first.length <= 128,
    "the product API takes 8 to 128 visible characters");
  const other = intentFor(formKey("intent-ns", RUN_NOW_FORM, "weekly"));
  assert.notEqual(first, other, "a different schedule is a different intent");
  const deliberate = newIntent(key);
  assert.notEqual(first, deliberate,
    "and a DELIBERATE later backup is a new intent and therefore a new run");
  dropDraft(key);
  dropDraft(formKey("intent-ns", RUN_NOW_FORM, "weekly"));
});

test("the_intent_is_a_field_of_the_draft_and_a_reload_genuinely_ends_it", () => {
  // REVIEW F3. The first cut composed the key from a module-level counter that
  // reset on every page load, so the FIRST intent minted after a reload
  // reproduced the first intent minted before it, byte for byte -- and the
  // page, `ui/README.md` and `docs/kubernetes.md` all said the opposite. The
  // row that "held" that sentence asserted only that it was ON SCREEN.
  //
  // A RELOAD IS THE DRAFT REGISTRY GOING AWAY, which is what `dropDraft` is
  // here: there is no browser storage in this tree, so the draft and the
  // intent are lost together, and the intent is what the draft holds.
  const key = formKey("reload-ns", RUN_NOW_FORM, "nightly");
  dropDraft(key);
  const before = intentFor(key);
  assert.deepEqual(Object.keys(readDraft(key)), RUN_NOW_DRAFT_FIELDS.slice(),
    "the intent IS the draft, and the draft holds nothing else");
  assert.equal(readDraft(key).intent, before);
  assert.equal(intentFor(key), before, "the same draft keeps its key");

  dropDraft(key); // the reload
  const after = intentFor(key);
  assert.notEqual(after, before,
    "a new draft after a reload is a new intent, so a click after a reload is a NEW run");

  // AND IT IS NOT ORDER-DEPENDENT. The defect was that the key's body was a
  // counter, so `mint()` twice in two sessions collided. Sixty-four mints are
  // sixty-four distinct keys.
  const minted = new Set();
  for (let i = 0; i < 64; i += 1) {
    const one = mintIntent();
    assert.ok(one.length >= 8 && one.length <= 128, "inside the API's budget");
    assert.match(one, /^logweir-ui\.manual\.[0-9a-f]{32}$/,
      "random hex, not a counter and not a clock");
    minted.add(one);
  }
  assert.equal(minted.size, 64, "no two mints collide");
  dropDraft(key);
});

test("two_page_loads_never_mint_the_same_first_intent", async () => {
  // THE ARM THAT CAN ACTUALLY SEE REVIEW F3, and the reason it is separate.
  // Within ONE page load a counter is unique too -- a zero-padded counter is
  // even 32 hex-legal characters -- so the shape check and the no-duplicates
  // check above both pass for the defect. What the defect IS is that a SECOND
  // page load starts the counter again and reproduces the first key.
  //
  // A SECOND MODULE INSTANCE IS A SECOND PAGE LOAD. Importing the page module
  // under a distinct specifier gives it its own module state -- its own
  // counter, if it had one -- and dropping the draft between the two is the
  // other half of a reload, because the draft registry does not survive one
  // either. Nothing here touches the network or a DOM.
  const loadA = await import("../pages/schedules.js?pageLoad=A");
  const loadB = await import("../pages/schedules.js?pageLoad=B");
  assert.notEqual(loadA, loadB, "two distinct module instances");
  const key = formKey("two-loads-ns", RUN_NOW_FORM, "nightly");
  const first = loadA.intentFor(key);
  dropDraft(key); // the reload
  const second = loadB.intentFor(key);
  assert.notEqual(second, first,
    "the FIRST intent of a new page load is not the first intent of the old one. A counter " +
      "that resets on load makes those two equal, which is what made 'a click after a reload " +
      "is a new run' false in every place this product says it");
  assert.notEqual(loadA.mintIntent(), loadB.mintIntent(),
    "and the mint itself is not order-dependent: the nth mint of one load is not the nth mint " +
      "of another");
  dropDraft(key);
});

test("the_body_is_the_schedule_and_the_revision_and_no_policy_at_all", async () => {
  let sent = null;
  let key = null;
  await submitRunNow("ns", { metadata: { name: "nightly", generation: 7 } }, {}, {
    runBackupNow: (ns, body, k) => {
      sent = body;
      key = k;
      return Promise.resolve({});
    },
  }, "logweir-ui.manual.1.x");
  assert.deepEqual(sent, { scheduleRef: { name: "nightly", expectedGeneration: 7 } });
  assert.equal(key, "logweir-ui.manual.1.x");
  assert.equal(sent.topicSelection, undefined,
    "a policy field in this body is a 422 by design: the API copies the schedule's own " +
      "revision, which is what makes 'the copied schedule revision' a fact and not a guess");
});

test("a_not_ready_verdict_is_acknowledged_on_the_run_and_never_gated_on", async () => {
  let sent = null;
  await submitRunNow("ns", { metadata: { name: "nightly", generation: 7 } },
    { readiness: { id: "pf-abc", state: "notReady" } }, {
      runBackupNow: (ns, body) => {
        sent = body;
        return Promise.resolve({});
      },
    }, "k".repeat(10));
  assert.deepEqual(sent.readinessAcknowledgement, { preflight: "pf-abc", state: "notReady" });
});

test("a_suspended_schedule_disables_the_button_until_it_is_confirmed_and_is_not_a_refusal", () => {
  const object = JSON.parse(JSON.stringify(SCHEDULE));
  object.spec.suspend = true;
  const view = {
    ns: "d1w8", name: "tz", object: object, mayOperate: true, runs: [],
    state: { phase: "idle" },
  };
  const unconfirmed = renderRunNowPanel(view);
  assert.match(unconfirmed, /data-suspended-notice="tz"/);
  assert.match(unconfirmed, /nothing about a schedule blocks one/);
  assert.match(unconfirmed, /Run anyway/);
  assert.match(unconfirmed, /<button type="submit" disabled>/);
  const confirmed = renderRunNowPanel(Object.assign({}, view, { acknowledged: true }));
  assert.match(confirmed, /<button type="submit">/);
  assert.match(confirmed, /will stay suspended/);
});

test("an_active_run_is_a_notice_and_never_a_disabled_button", () => {
  const object = JSON.parse(JSON.stringify(SCHEDULE));
  object.status.activeRuns = [{ name: "r", kind: "Scheduled", attempt: 0 }];
  const html = renderRunNowPanel({
    ns: "d1w8", name: "tz", object: object, mayOperate: true, runs: [],
    state: { phase: "idle" },
  });
  assert.match(html, /data-active-notice="tz"/);
  assert.match(html, /neither counted against concurrencyPolicy nor blocked by it/);
  assert.match(html, /<button type="submit">/,
    "D1 section 8.3: an active run does not block a manual one, so the control stays live");
});

test("the_run_a_click_produced_is_shown_with_its_trigger_and_its_copied_revision", () => {
  const answer = decodeManualBackup(console_("manual-backup.json")).value;
  const html = renderRunNowResult("lw-d1w6-20260917131854", {
    replayed: answer.replayed,
    schedule: answer.schedule,
    run: {
      metadata: { name: answer.item.name, namespace: answer.item.namespace },
      spec: { trigger: answer.item.trigger, scheduleRef: answer.item.scheduleRef },
    },
  });
  assert.match(html, /data-replayed="false"/);
  assert.match(html, /logweir-manual-pehgrdiecje5dy3a5ze2mmbglq/);
  assert.match(html, /badge-trigger-manual/);
  assert.match(html, /revision g1/);
  assert.match(html, /sha256:0bc972dc/);
  assert.match(html, /is suspended/, "the schedule it copied was suspended, and the run says so");
});

test("the_second_click_says_it_started_nothing_new", () => {
  const answer = decodeManualBackup(console_("manual-backup-replayed.json")).value;
  const first = decodeManualBackup(console_("manual-backup.json")).value;
  assert.equal(answer.replayed, true);
  assert.equal(answer.item.uid, first.item.uid,
    "the live replay returned the SAME object, which is what the key buys");
  const html = renderRunNowResult("ns", {
    replayed: true,
    run: { metadata: { name: answer.item.name, namespace: "ns" }, spec: {} },
  });
  assert.match(html, /data-replayed="true"/);
  assert.match(html, /had already started this run/);
  assert.match(html, /rather than starting a second one/);
});

test("a_reload_is_answered_with_the_runs_that_exist_and_not_with_a_replayed_key", () => {
  // THERE IS NO BROWSER STORAGE IN THIS TREE, so an intent cannot survive a
  // refresh and this page does not pretend otherwise. What it does instead is
  // SHOW the manual runs that already exist, so the reader sees the run their
  // click made rather than clicking again.
  const backups = {
    items: [
      {
        metadata: { name: "logweir-manual-a", creationTimestamp: "2026-09-18T05:00:00Z" },
        spec: { trigger: { kind: "Manual", attempt: 0 }, scheduleRef: { name: "tz", generation: 1 } },
        status: { phase: "Succeeded" },
      },
      {
        metadata: { name: "logweir-manual-b", creationTimestamp: "2026-09-18T06:00:00Z" },
        spec: { trigger: { kind: "Manual", attempt: 0 }, scheduleRef: { name: "tz", generation: 2 } },
        status: { phase: "Running" },
      },
      {
        metadata: { name: "logweir-backup-tz-20260918-032200" },
        spec: { trigger: { kind: "Scheduled", attempt: 0 }, scheduleRef: { name: "tz" } },
      },
      {
        metadata: { name: "someone-elses" },
        spec: { trigger: { kind: "Manual", attempt: 0 }, scheduleRef: { name: "other" } },
      },
    ],
  };
  const runs = manualRunsOf("tz", backups);
  assert.deepEqual(runs.map((r) => r.metadata.name), ["logweir-manual-b", "logweir-manual-a"],
    "the manual runs of THIS schedule, newest first -- read from the run's own trigger");
  const html = renderRunNowPanel({
    ns: "d1w8", name: "tz", object: SCHEDULE, mayOperate: true, runs: runs,
    state: { phase: "idle" },
  });
  assert.match(html, /logweir-manual-a/);
  assert.match(html, /logweir-manual-b/);
  assert.equal(html.indexOf("logweir-backup-tz-20260918-032200"), -1,
    "a scheduled run is not a manual one");
  assert.match(html, /a click after a reload is a deliberate NEW run and will create a second one/);
  assert.match(html, /The intent is random, not counted/,
    "and the sentence now says WHY a new session cannot reproduce an old key -- which is the " +
      "half review F3 found to be false when the key's body was a counter");
});

test("a_policy_changed_refusal_carries_the_revision_that_is_in_force_now", () => {
  const problem = console_("problem-policy-changed.json");
  assert.equal(problem.code, "policy_changed");
  const detail = decodePolicyChanged(problem);
  assert.equal(detail.currentGeneration, 1);
  assert.equal(decodePolicyChanged({ code: "validation_failed" }), null,
    "every other refusal reaches the page as its own message, not as a contract failure here");
});

// ===========================================================================
// 6. PLAT-09.2 -- coverage, read from the field
// ===========================================================================

test("the_three_coverage_labels_are_the_controllers_own_strings", () => {
  assert.deepEqual(Object.keys(COVERAGE_LABELS),
    ["NamedTopics", "AllUserTopicsAttested", "VisibleUserTopicsOnly"]);
  assert.equal(COVERAGE_LABELS.VisibleUserTopicsOnly,
    "Visible user topics only — completeness not established");
});

test("a_visible_only_run_says_what_the_policy_asked_for_and_what_it_got", () => {
  const run = fixture("backup-visible-only.json");
  const html = renderCoverageLine(run);
  assert.match(html, /data-coverage="VisibleUserTopicsOnly"/);
  assert.match(html, /data-selection-mode="AllUserTopics"/);
  assert.ok(html.indexOf(COVERAGE_LABELS.VisibleUserTopicsOnly) !== -1);
  assert.ok(html.indexOf("The policy asked for " + SELECTION_MODES.AllUserTopics) !== -1,
    "the MODE and the COVERAGE are two facts and are printed as two");
  assert.match(html, /2 the broker refused to describe/,
    "the count this whole block exists to make visible");
  assert.match(html, /badge-pending/);
  assert.equal(html.indexOf("badge-green"), -1,
    "only AllUserTopicsAttested is ever green, because only it means everything");
});

test("a_named_run_is_named_topics_and_is_never_all_topics", () => {
  const run = {
    status: { selection: { mode: "SelectedTopics", coverage: "NamedTopics", resolvedTopicCount: 1 } },
  };
  const html = renderCoverageLine(run);
  assert.match(html, /Named topics/);
  assert.ok(html.indexOf("The policy asked for named topics") !== -1);
  assert.equal(coverageCell(run).indexOf("badge-green"), -1);
  assert.match(coverageCell({
    status: { selection: { mode: "AllUserTopics", coverage: "AllUserTopicsAttested", resolvedTopicCount: 9 } },
  }), /badge-green/, "the one coverage that means everything is the one that is green");
});

test("a_discovery_that_refused_names_the_controllers_own_reason_and_message", () => {
  const run = fixture("backup-discovery-failed.json");
  const failure = discoveryFailure(run);
  assert.equal(failure.reason, "DiscoveryFailed");
  assert.match(failure.message, /did not produce a usable result \(DeadlineExceeded\)/);
  const html = renderCoverageLine(run);
  assert.match(html, /data-topics-resolved="False"/);
  assert.match(html, /DiscoveryFailed/);
  assert.match(html, /the check Job reached its activeDeadlineSeconds/);
  // AND A `TopicsResolved=True` IS NOT A FAILURE LINE.
  const resolved = fixture("backup-visible-only.json");
  assert.equal(discoveryFailure(resolved), null);
});

test("a_run_with_no_selection_block_gets_the_sentence_and_never_a_label", () => {
  const html = renderCoverageLine({ spec: { topics: ["t1"] }, status: {} });
  assert.equal(html, "", "a named run with nothing recorded says nothing rather than guessing");
  const dynamic = renderCoverageLine({
    spec: { topics: [], allUserTopics: { incompleteDiscovery: "Refuse" } }, status: {},
  });
  assert.match(dynamic, /no status\.selection at all/);
  for (const label of Object.values(COVERAGE_LABELS)) {
    assert.equal(dynamic.indexOf(label), -1, "and no coverage label at all: " + label);
  }
});

// ===========================================================================
// 7. PLAT-05.1/06.2 -- the trigger, and the last slot
// ===========================================================================

test("each_trigger_kind_is_read_from_the_runs_own_field", () => {
  const retry = fixture("backup-retry.json");
  const catchUp = fixture("backup-catchup.json");
  assert.equal(triggerLabel(retry.spec.trigger), "Retry, attempt 1");
  assert.equal(triggerLabel(retry.spec.trigger, 3), "Retry 1 of 3");
  assert.equal(triggerLabel(catchUp.spec.trigger), "CatchUp");
  assert.equal(triggerLabel({ kind: "Scheduled", attempt: 0 }), "Scheduled");
  assert.equal(triggerLabel({ kind: "Manual", attempt: 0 }), "Manual");
  assert.equal(triggerLabel(undefined), "");
  assert.ok(triggerBadge(undefined).indexOf(NO_TRIGGER_SENTENCE) !== -1);
});

test("the_retry_ceiling_is_never_invented_where_the_schedule_is_not_at_hand", () => {
  // "Retry 2 of 3" NEEDS THE SCHEDULE'S CURRENT `retry.maxRetries`, and a run
  // carries no copy of it. The Backups table does not read schedules, so it
  // renders the attempt alone; the schedule's own card, which holds the
  // policy, renders the ceiling.
  const retry = fixture("backup-retry.json");
  const list = renderBackupList({ items: [retry] }, "ns");
  assert.match(list, /Retry, attempt 1/);
  assert.equal(list.indexOf("Retry 1 of"), -1);
});

test("the_backups_table_reads_trigger_and_never_triggered_by", () => {
  const retry = fixture("backup-retry.json");
  const legacy = {
    metadata: { name: "old-run" },
    spec: { sourceRef: { name: "source" }, topics: ["t1"], triggeredBy: "schedule" },
    status: { phase: "Succeeded" },
  };
  const html = renderBackupList({ items: [retry, legacy] }, "ns");
  assert.match(html, /<th scope="col">TRIGGER<\/th>/);
  assert.match(html, /Retry, attempt 1/);
  assert.ok(html.indexOf(NO_TRIGGER_SENTENCE) !== -1,
    "the pre-PLAT-05.1 run says its trigger was not recorded");
  assert.equal(html.indexOf(">Scheduled<"), -1,
    "and is never rendered as Scheduled, which is what reading triggeredBy as a kind would do");
});

test("a_runs_detail_shows_its_trigger_its_retry_parent_and_the_revision_it_froze", () => {
  const retry = fixture("backup-retry.json");
  const html = renderBackupDetail(retry);
  assert.match(html, /Retry, attempt 1/);
  assert.match(html, new RegExp(retry.spec.trigger.retryOf.name));
  assert.match(html, /revision g/);
  assert.match(html, new RegExp(retry.spec.scheduleRef.runPolicySha256.slice(0, 16)));
});

test("the_last_slot_is_printed_as_what_happened_and_never_judged", () => {
  const html = renderLastSlot(SCHEDULE);
  assert.match(html, /data-last-slot="1"/);
  assert.match(html, /20260918-032200/);
  assert.match(html, /Admitted/);
  assert.match(html, /logweir-backup-tz-20260918-032200/);
  assert.match(html, /PastStartingDeadline|missed slots/);
});

test("a_capped_missed_count_says_it_is_a_floor", () => {
  const object = JSON.parse(JSON.stringify(SCHEDULE));
  object.status.missedSlots.countCapped = true;
  const html = renderLastSlot(object);
  assert.match(html, /a floor, not a total/);
  assert.match(html, /1000-slot cap/);
});

test("a_console_that_cannot_read_the_last_slot_names_the_projection_that_owes_it", () => {
  const object = { metadata: { name: "tz" }, spec: {}, status: {},
    __contract: { mode: "console", absent: ["status.lastSlot", "status.missedSlots"], unknown: [] } };
  const html = renderLastSlot(object);
  assert.match(html, /data-last-slot="absent"/);
  assert.match(html, /status\.lastSlot and status\.missedSlots/);
  assert.match(html, /the console has nothing to render/);
  assert.match(LAST_SLOT_NOT_PUBLISHED, /status\.lastSlot and status\.missedSlots/);
  // AND A SCHEDULE THAT SIMPLY HAS NO SLOT YET SAYS NOTHING AT ALL.
  assert.equal(renderLastSlot({ metadata: { name: "new" }, status: {} }), "");
});

// ===========================================================================
// 8. the two modes: what the console can read, and what it says it cannot
// ===========================================================================
//
// THESE ROWS DRIVE THE REAL TRANSPORT. `ui/api.js`'s one call site is stubbed
// at the platform boundary, so the identifier under assertion is the one
// `path(...)` built and the headers are the ones `api.js` set. Nothing opens a
// socket.

function transport(answer) {
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (u, init) => {
    seen.push({ url: String(u), init: init || {} });
    const reply = answer(String(u), init || {});
    if (reply === undefined || reply === null) {
      return Promise.reject(new Error("the suite has no answer for " + String(u)));
    }
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      text: () => Promise.resolve(reply.body === undefined ? "" : JSON.stringify(reply.body)),
    });
  };
  return { seen: seen, restore: () => { globalThis.fetch = original; } };
}

// THE SESSION FIXTURE IS THE ONE WITH `manualBackupCreate: true`, and that
// flag is the whole difference. It was permanently `false` before D1 W6 --
// there was no route -- and is now `implemented && allowed`; `session.json`
// still carries the old value, and both are real answers this build can send.
async function consoleMode(document) {
  resetMode();
  return selectMode({
    probe: async () => ({
      ok: true, status: 200, body: document || console_("session-manual-backups.json"),
    }),
  });
}

async function legacyMode() {
  resetMode();
  return selectMode({ probe: async () => ({ ok: false, status: 403, body: null }) });
}

const NS = () => console_("session-manual-backups.json").namespaces[0].name;

test("the_console_projection_carries_the_policy_the_revision_and_the_previews", async () => {
  await consoleMode();
  const ns = NS();
  const answer = console_("schedule-policy.json");
  const net = transport((u) => (u.indexOf("/schedules/tz") === -1
    ? null
    : { status: 200, body: answer }));
  try {
    const object = await apiClient().get(ns, "backupschedules", "tz");
    assert.equal(object.metadata.generation, 1);
    assert.equal(object.spec.timeZone, "Asia/Kathmandu");
    assert.equal(object.spec.activeDeadlineSeconds, 600);
    assert.equal(object.spec.startingDeadlineSeconds, 900);
    assert.equal(object.spec.catchUpPolicy, "Latest");
    assert.deepEqual(object.spec.retry, { maxRetries: 2, delaySeconds: 60 });
    assert.equal(object.status.policy.tzdb, "chrono-tz 0.10.4");
    assert.equal(object.status.nextRuns.length, 5);
    assert.deepEqual(object.status.activeRuns, [],
      "an EMPTY active list came back as empty, which is not the same as absent");
    assert.equal(object.__contract.unknown.length, 0,
      "every field this answer carries is one the client declares: an unknown here is a " +
        "field the page would be silently dropping");
  } finally {
    net.restore();
    resetMode();
  }
});

test("an_absent_active_run_list_survives_the_projection_as_absent", async () => {
  await consoleMode();
  const ns = NS();
  const answer = JSON.parse(JSON.stringify(console_("schedule-policy.json")));
  delete answer.item.status.activeRuns;
  delete answer.item.status.nextRuns;
  const net = transport((u) => (u.indexOf("/schedules/tz") === -1
    ? null
    : { status: 200, body: answer }));
  try {
    const object = await apiClient().get(ns, "backupschedules", "tz");
    assert.equal(object.status.activeRuns, undefined,
      "absent means NOT YET COMPUTED, and defaulting it to [] would read as 'none are running'");
    assert.equal(object.status.nextRuns, undefined);
  } finally {
    net.restore();
    resetMode();
  }
});

test("the_console_projection_carries_a_runs_trigger_and_the_revision_it_froze", async () => {
  await consoleMode();
  const ns = NS();
  const answer = console_("manual-backup.json");
  const net = transport((u) => {
    if (u.indexOf("/backups/") !== -1 && u.indexOf("/operations/") === -1) {
      return { status: 200, body: { requestId: answer.requestId, item: answer.item } };
    }
    return { status: 404, body: { code: "not_found", title: "x", status: 404, type: "t" } };
  });
  try {
    const object = await apiClient().get(ns, "backups", answer.item.name);
    assert.deepEqual(object.spec.trigger, { kind: "Manual", attempt: 0 });
    assert.deepEqual(object.spec.scheduleRef, {
      name: "nightly",
      uid: "73ce6531-c0c1-4430-ae81-971e99bca975",
      generation: 1,
      runPolicySha256: answer.item.scheduleRef.runPolicySha256,
    });
    assert.ok(object.__contract.absent.indexOf("status.selection") !== -1,
      "and the coverage block the product API does NOT publish is named rather than left blank");
  } finally {
    net.restore();
    resetMode();
  }
});

test("back_up_now_sends_the_key_as_a_header_and_the_body_as_body_a", async () => {
  await consoleMode();
  const ns = NS();
  const net = transport((u, init) => (init.method === "POST" && u.indexOf("/backups") !== -1
    ? { status: 201, body: console_("manual-backup.json") }
    : null));
  try {
    const answer = await apiClient().runBackupNow(
      ns, { scheduleRef: { name: "nightly", expectedGeneration: 1 } }, "logweir-ui.manual.1.k",
    );
    assert.equal(net.seen.length, 1);
    assert.equal(net.seen[0].url, "/api/v1/namespaces/" + ns + "/backups");
    assert.equal(net.seen[0].init.headers["Idempotency-Key"], "logweir-ui.manual.1.k");
    assert.equal(answer.replayed, false);
    assert.equal(answer.run.metadata.name, "logweir-manual-pehgrdiecje5dy3a5ze2mmbglq");
    assert.equal(answer.schedule.suspended, true);
  } finally {
    net.restore();
    resetMode();
  }
});

test("the_same_intent_twice_is_the_same_key_and_the_api_answers_replayed", async () => {
  await consoleMode();
  const ns = NS();
  let calls = 0;
  const net = transport((u, init) => {
    if (init.method !== "POST") {
      return null;
    }
    calls += 1;
    return calls === 1
      ? { status: 201, body: console_("manual-backup.json") }
      : { status: 200, body: console_("manual-backup-replayed.json") };
  });
  try {
    const api = apiClient();
    const body = { scheduleRef: { name: "nightly", expectedGeneration: 1 } };
    const first = await api.runBackupNow(ns, body, "logweir-ui.manual.7.k");
    const second = await api.runBackupNow(ns, body, "logweir-ui.manual.7.k");
    assert.equal(first.replayed, false);
    assert.equal(second.replayed, true);
    assert.equal(first.run.metadata.uid, second.run.metadata.uid,
      "ONE RUN. This is the live pair from D1 W6's smoke: the same key returned the same uid");
    assert.deepEqual(
      net.seen.map((s) => s.init.headers["Idempotency-Key"]),
      ["logweir-ui.manual.7.k", "logweir-ui.manual.7.k"],
    );
  } finally {
    net.restore();
    resetMode();
  }
});

test("a_policy_changed_refusal_reaches_the_page_with_its_revision_attached", async () => {
  await consoleMode();
  const ns = NS();
  const net = transport((u, init) => (init.method === "POST"
    ? { status: 409, body: console_("problem-policy-changed.json") }
    : null));
  try {
    await assert.rejects(
      () => apiClient().runBackupNow(
        ns, { scheduleRef: { name: "nightly", expectedGeneration: 99 } }, "logweir-ui.manual.9.k",
      ),
      (error) => {
        assert.equal(error.reason, "policy_changed");
        assert.equal(error.policy.currentGeneration, 1);
        assert.equal(error.policy.currentRunPolicySha256, null,
          "the live 409 carried no digest, and absent is absent rather than a blank string");
        return true;
      },
    );
  } finally {
    net.restore();
    resetMode();
  }
});

test("the_policy_replace_carries_no_idempotency_key_and_names_the_schedule", async () => {
  await consoleMode();
  const ns = NS();
  const net = transport((u, init) => (init.method === "PUT"
    ? { status: 200, body: console_("schedule-preset.json") }
    : null));
  try {
    const object = await apiClient().editSchedulePolicy(ns, "tz", {
      expectedGeneration: 1,
      schedule: "0 2 * * *",
      suspended: false,
      topicSelection: { topics: ["t1"] },
      archive: { url: "s3://kafka/logweir" },
    });
    assert.equal(net.seen[0].url, "/api/v1/namespaces/" + ns + "/schedules/tz");
    assert.equal(net.seen[0].init.headers["Idempotency-Key"], undefined,
      "the route answers 400 for a key: its replay guard is expectedGeneration");
    assert.equal(object.metadata.generation, 7, "the answer carries the NEW revision");
  } finally {
    net.restore();
    resetMode();
  }
});

test("a_preview_reaches_the_non_namespaced_route_with_the_preset_parameters", async () => {
  await consoleMode();
  const net = transport((u) => (u.indexOf("/api/v1/cadence-previews") === 0
    ? { status: 200, body: console_("cadence-preview-repeated.json") }
    : null));
  try {
    const answer = await apiClient().previewCadence({
      preset: "daily", hour: 2, minute: 30, timeZone: "Europe/Berlin", count: 5,
    });
    assert.equal(
      net.seen[0].url,
      "/api/v1/cadence-previews?preset=daily&timeZone=Europe%2FBerlin&minute=30&hour=2&count=5",
    );
    assert.equal(net.seen[0].init.method, "GET");
    assert.equal(answer.schedule, "30 2 * * *", "the canonical expression a preset compiled to");
    assert.equal(answer.runs.length, 3);
  } finally {
    net.restore();
    resetMode();
  }
});

test("an_invented_preview_parameter_is_dropped_before_the_network", async () => {
  await consoleMode();
  const net = transport(() => ({ status: 200, body: console_("cadence-preview-gap.json") }));
  try {
    await apiClient().previewCadence({ schedule: "0 2 * * *", rm: "-rf", limit: 9000 });
    assert.equal(net.seen[0].url, "/api/v1/cadence-previews?schedule=0%202%20*%20*%20*",
      "an allowlist and never a pass-through bag");
  } finally {
    net.restore();
    resetMode();
  }
});

test("legacy_mode_refuses_the_preview_and_the_replace_by_name_and_sends_nothing", async () => {
  // TWO OF THE THREE, AND THE THIRD IS NO LONGER ONE OF THEM (PLAT-06.2).
  // "Back up now" WITH a schedule is a path this mode can walk now -- section 10
  // below walks it against a fake kube-apiserver -- so what stays refused here
  // is the draft cadence preview (a computation `logweir-api` performs, and a
  // second cron implementation in a browser is a second opinion about when a
  // backup runs), the whole-policy replace (built on the product API's
  // `expectedGeneration` precondition, which a merge patch would not have),
  // and the AD-HOC manual body, which has no schedule to copy and no published
  // digest to carry.
  await legacyMode();
  const net = transport(() => ({ status: 500, body: null }));
  try {
    const api = apiClient();
    await assert.rejects(() => api.previewCadence({ schedule: "0 2 * * *" }), /Previewing a cadence/);
    await assert.rejects(() => api.editSchedulePolicy("ns", "tz", {}), /Editing a schedule/);
    await assert.rejects(() => api.runBackupNow("ns", {}, "k".repeat(10)),
      /Backing up an ad-hoc selection/);
    assert.equal(net.seen.length, 0, "not one request was sent");
  } finally {
    net.restore();
    resetMode();
  }
});

// ===========================================================================
// 9. the mount half: what a save, a second backup and a refusal do to the page
// ===========================================================================
//
// THE THREE ROWS BELOW DRIVE THE REAL WIRING, and they exist because a
// reviewer's live browser journey found two defects that every pure-function
// row above was blind to: a successful save left the pre-edit policy on screen
// at a superseded revision (F1), and a deliberate second manual backup had no
// path at all (F2). Both are in the mount half, so the mount half is what they
// assert -- through the same kind of fake node `mutation.spec.js` uses, which
// keeps the markup the page adopted and answers selectors from it.

function attributesOf(tag) {
  const out = Object.create(null);
  const pattern = /([a-zA-Z][\w-]*)(?:="([^"]*)")?/g;
  const body = tag.replace(/^<\w+/, "").replace(/>$/, "");
  let match;
  while ((match = pattern.exec(body)) !== null) {
    out[match[1]] = match[2] === undefined
      ? ""
      : match[2].replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, "\"")
        .replace(/&#39;/g, "'").replace(/&amp;/g, "&");
  }
  return out;
}

/** `tag`, `#id`, `.class`, `[attr="value"]` and any concatenation of them,
 *  plus a comma list. Enough for every selector the schedules page uses, and
 *  no more: a matcher that guessed would make a row pass for the wrong reason. */
function matches(element, selector) {
  for (const one of selector.split(",").map((t) => t.trim())) {
    const tag = /^[a-zA-Z][\w-]*/.exec(one);
    if (tag !== null && element.tagName !== tag[0].toUpperCase()) {
      continue;
    }
    let ok = true;
    const rest = one.slice(tag === null ? 0 : tag[0].length);
    const pattern = /#([\w-]+)|\.([\w-]+)|\[([\w-]+)="([^"]*)"\]/g;
    let part;
    while ((part = pattern.exec(rest)) !== null) {
      if (part[1] !== undefined && element.attributes.id !== part[1]) {
        ok = false;
      } else if (part[2] !== undefined &&
        String(element.attributes.class || "").split(/\s+/).indexOf(part[2]) === -1) {
        ok = false;
      } else if (part[3] !== undefined && element.attributes[part[3]] !== part[4]) {
        ok = false;
      }
    }
    if (ok) {
      return true;
    }
  }
  return false;
}

class Fake {
  constructor(tag, attributes, view) {
    this.tagName = tag.toUpperCase();
    this.attributes = attributes;
    this.view = view;
    this.children = [];
    this.listeners = [];
    this.value = "";
    this.checked = false;
    this.disabled = "disabled" in attributes;
  }
  get firstChild() { return this.children.length === 0 ? null : this.children[0]; }
  removeChild(child) { this.children.splice(this.children.indexOf(child), 1); return child; }
  appendChild(child) {
    this.children.push(child);
    if (child && typeof child.html === "string") {
      this.view.adopt(child.html, this.isRoot === true, this);
    }
    return child;
  }
  addEventListener(type, handler) { this.listeners.push({ type: type, handler: handler }); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  getAttribute(name) { return name in this.attributes ? this.attributes[name] : null; }
  focus() {}
  querySelector(selector) { return this.view.find(selector); }
  querySelectorAll(selector) { return this.view.findAll(selector); }
  async dispatch(type) {
    for (const entry of this.listeners.slice()) {
      if (entry.type === type) {
        await entry.handler({ preventDefault() {} });
      }
    }
  }
}

function fakeView() {
  const view = {
    chunks: [],
    adopt(html, fromRoot, owner) {
      if (fromRoot) {
        view.chunks = [];
      } else if (owner !== undefined && owner.slotOf !== undefined) {
        // A slot replace supersedes whatever that slot held before.
        view.chunks = view.chunks.filter((c) => c.slot !== owner.slotOf);
      }
      view.chunks.push({
        html: html, elements: null, slot: owner === undefined ? null : (owner.slotOf || null),
      });
    },
    html() { return view.chunks.map((c) => c.html).join(""); },
    elementsOf(chunk) {
      if (chunk.elements !== null) {
        return chunk.elements;
      }
      const elements = [];
      const tag = /<(input|select|button|form|div|section|p)\b[^>]*>/g;
      let match;
      while ((match = tag.exec(chunk.html)) !== null) {
        const element = new Fake(match[1], attributesOf(match[0]), view);
        element.start = match.index;
        if (match[1] === "input") {
          element.value = element.attributes.value || "";
          element.checked = "checked" in element.attributes;
        } else if (match[1] === "select") {
          const end = chunk.html.indexOf("</select>", match.index);
          const chosen = /<option value="([^"]*)" selected>/.exec(chunk.html.slice(match.index, end)) ||
            /<option value="([^"]*)"/.exec(chunk.html.slice(match.index, end));
          element.value = chosen === null ? "" : chosen[1];
        } else if (match[1] === "form") {
          element.end = chunk.html.indexOf("</form>", match.index);
        } else if (match[1] === "div" &&
          (element.attributes["data-policy-slot"] || element.attributes["data-run-now-slot"])) {
          element.slotOf = element.attributes["data-policy-slot"] === undefined
            ? "run:" + element.attributes["data-run-now-slot"]
            : "policy:" + element.attributes["data-policy-slot"];
        }
        elements.push(element);
      }
      for (const form of elements.filter((e) => e.tagName === "FORM")) {
        const inside = elements.filter((e) => e.start > form.start && e.start < form.end);
        form.elements = Object.create(null);
        for (const control of inside) {
          if (["INPUT", "SELECT"].indexOf(control.tagName) !== -1 && control.attributes.name) {
            form.elements[control.attributes.name] = control;
          }
        }
        form.querySelectorAll = (selector) => inside.filter((e) => matches(e, selector));
      }
      chunk.elements = elements;
      return elements;
    },
    find(selector) {
      for (let i = view.chunks.length - 1; i >= 0; i -= 1) {
        const found = view.elementsOf(view.chunks[i]).find((e) => matches(e, selector));
        if (found !== undefined) {
          return found;
        }
      }
      return null;
    },
    findAll(selector) {
      const all = [];
      for (const chunk of view.chunks) {
        for (const element of view.elementsOf(chunk)) {
          if (matches(element, selector)) {
            all.push(element);
          }
        }
      }
      return all;
    },
  };
  const root = new Fake("main", Object.create(null), view);
  root.isRoot = true;
  view.root = root;
  return view;
}

const parse = (html) => [{ html: html }];
// The route token `createRouteLifecycle` hands a mount: a signal and the
// question "is this still the current route". Built by hand here rather than
// imported so these rows carry no navigation of their own.
const LIFE = () => ({ generation: 1, signal: undefined, isCurrent: () => true });

/** A schedules page mounted over a stubbed adapter. `objects` is called for
 *  each `list(backupschedules)`, so a re-read can answer differently. */
function mountedPage(ns, objects, overrides) {
  const view = fakeView();
  const calls = { list: 0 };
  const api = Object.assign({
    list(namespace, plural) {
      if (plural === "backupschedules") {
        calls.list += 1;
        return Promise.resolve({ items: [objects(calls.list)] });
      }
      return Promise.resolve({ items: [] });
    },
    destinations() { return Promise.reject(new Error("no destinations route in this stub")); },
  }, overrides || {});
  return { view: view, api: api, calls: calls, ns: ns };
}

test("a_successful_policy_save_re_reads_and_shows_the_stored_revision", async () => {
  // REVIEW F1, MEASURED LIVE: the form and the card kept showing the pre-edit
  // policy at g3 while the API server held g4. `wirePolicy`'s success arm
  // repainted from the object captured at mount; the suspend toggle has always
  // re-read instead, and now so does this.
  const at = (generation, expression) => {
    const object = JSON.parse(JSON.stringify(SCHEDULE));
    object.metadata.generation = generation;
    object.spec.schedule = expression;
    object.status.observedGeneration = generation;
    object.status.policy.generation = generation;
    return object;
  };
  const saved = at(4, "15 4 * * *");
  const page = mountedPage("f1-ns", (n) => (n === 1 ? at(3, "7 9 * * *") : saved), {
    editSchedulePolicy() { return Promise.resolve(saved); },
  });
  await mountSchedules(page.view.root, page.ns, parse, LIFE(), page.api);
  assert.equal(page.calls.list, 1);
  assert.match(page.view.html(), /data-editing-generation="3"/, "the form opened at g3");

  const form = page.view.find("form.policy-form[data-name=\"tz\"]");
  assert.ok(form !== null, "the policy form is on the page");
  await form.dispatch("submit");
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(page.calls.list, 2, "a successful save RE-READS rather than repainting a stale copy");
  const html = page.view.html();
  assert.match(html, /data-editing-generation="4"/,
    "and the form is now open at the revision the response carried");
  assert.match(html, /revision g4/);
  assert.equal(html.indexOf("data-editing-generation=\"3\""), -1,
    "the superseded revision is gone from the page, not merely outnumbered");
  assert.match(html, /15 4 \* \* \*/, "and the stored expression is what is rendered");
});

test("a_deliberate_second_backup_mints_a_new_intent_and_creates_another_run", async () => {
  // REVIEW F2. PLAT-06.2's acceptance is "repeated clicks create one requested
  // run; A DELIBERATE LATER BACKUP CREATES ANOTHER". `newIntent` existed,
  // was unit-tested and was called by nothing, so the second clause had no
  // path: every click on a schedule resent the one intent it ever had, and a
  // console could take exactly one manual backup of it, ever.
  const answer = console_("manual-backup.json");
  const keys = [];
  const object = JSON.parse(JSON.stringify(SCHEDULE));
  const page = mountedPage("f2-ns", () => object, {
    runBackupNow(ns, body, key) {
      keys.push(key);
      return Promise.resolve({
        replayed: keys.length > 1 && keys[keys.length - 1] === keys[keys.length - 2],
        schedule: answer.schedule,
        run: {
          metadata: { name: "logweir-manual-" + String(keys.length), namespace: ns },
          spec: { trigger: { kind: "Manual", attempt: 0 }, scheduleRef: answer.item.scheduleRef },
        },
      });
    },
  });
  await mountSchedules(page.view.root, page.ns, parse, LIFE(), page.api);

  const form = page.view.find("form.run-now-form[data-name=\"tz\"]");
  assert.ok(form !== null);
  await form.dispatch("submit");
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(keys.length, 1);
  assert.match(page.view.html(), /logweir-manual-1/, "the run is on screen");

  // A SECOND SUBMIT WITHOUT THE CONTROL IS THE SAME REQUEST. That half was
  // already true and stays true: the intent has not ended.
  const again = page.view.find("form.run-now-form[data-name=\"tz\"]");
  await again.dispatch("submit");
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.deepEqual(keys[1], keys[0], "a repeat click resends the one intent");

  // AND THE CONTROL IS WHAT ENDS IT.
  const another = page.view.find("button[data-run-again=\"tz\"]");
  assert.ok(another !== null, "the panel offers Back up again once a run exists");
  await another.dispatch("click");
  await new Promise((resolve) => setTimeout(resolve, 0));
  const third = page.view.find("form.run-now-form[data-name=\"tz\"]");
  await third.dispatch("submit");
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(keys.length, 3);
  assert.notEqual(keys[2], keys[0],
    "a deliberate later backup is a NEW intent, so the API creates a second run");
  assert.match(page.view.html(), /logweir-manual-3/);
});

test("a_policy_changed_refusal_shows_the_revision_in_force_and_offers_a_new_intent", () => {
  // REVIEW F2's other half. `ui/client.js` decoded the 409's one extension
  // member onto the error and no page read it, so D1 section 8.5's Conflict row
  // -- "show the new revision; confirmation starts a new intent" -- was
  // unimplemented in both halves.
  const conflict = {
    phase: "failed",
    kind: "conflict",
    error: {
      reason: "policy_changed",
      message: "the schedule's policy changed",
      policy: { currentGeneration: 9, currentRunPolicySha256: "sha256:abc123" },
    },
  };
  const line = renderRunNowConflict(conflict);
  assert.match(line, /data-run-now-conflict="policy_changed"/);
  assert.match(line, /revision g9/, "the revision that is in force NOW, from the extension member");
  assert.match(line, /sha256:abc123/);
  assert.match(line, /expectedGeneration exists to prevent/);
  assert.ok(offersAnotherRun({ state: conflict }),
    "and the control that mints a new intent is offered, because that is the only answer");

  const used = {
    phase: "failed", kind: "conflict",
    error: { reason: "idempotency_conflict", message: "already used with a different request" },
  };
  assert.match(renderRunNowConflict(used), /data-run-now-conflict="idempotency_conflict"/);
  assert.match(renderRunNowConflict(used), /Back up again mints a new intent/);
  assert.ok(offersAnotherRun({ state: used }));

  // EVERY OTHER REFUSAL IS THE GENERIC ONE, and offers no new intent: a 403 or
  // a 422 is not answered by spending a fresh key on the same bad request.
  const forbidden = { phase: "failed", kind: "rejected", error: { reason: "forbidden" } };
  assert.equal(renderRunNowConflict(forbidden), "");
  assert.equal(offersAnotherRun({ state: forbidden }), false);
  assert.equal(offersAnotherRun({ state: { phase: "idle" } }), false);
  assert.ok(RUN_AGAIN_SENTENCE.indexOf("starts a SECOND run") !== -1);
});

test("an_unknown_outcome_on_a_manual_run_names_the_key_and_no_empty_name", () => {
  // REVIEW F4. `mutationStatus`'s unknown-outcome copy was written for creates
  // whose name IS their idempotence. A manual run has no name until the server
  // derives one, so the rendered sentence read "it reuses the name , and" --
  // an empty name, and the wrong reason it is safe to click again.
  const html = renderRunNowPanel({
    ns: "ns", name: "tz", object: SCHEDULE, mayOperate: true, runs: [],
    state: { phase: "failed", kind: "unknown", timedOut: true, error: { message: "no answer" } },
  });
  assert.match(html, /whether Backup was created is unknown/);
  assert.match(html, /resends the idempotency key this click is holding/);
  assert.equal(html.indexOf("it reuses the name"), -1,
    "the name sentence is false of this route and is not rendered for it");
  assert.equal(html.indexOf("Backup :"), -1, "and no sentence carries an empty name");
});

// ===========================================================================
// 10. legacy mode: the same manual run, through kube-apiserver (PLAT-06.2)
// ===========================================================================
//
// WHAT CHANGED AND WHY THESE ROWS EXIST. Until this branch `runBackupNow`
// refused in legacy mode by name, for two reasons D1 W7's review checked and
// accepted: the run's NAME is derived from an idempotency scope, and the
// in-cluster page had no `create backups`. Both are answered now -- the rule
// is shared (`manualBackupName`, pinned by `manual-backup-names.json`) and the
// chart grants the verb -- so the legacy column of D1 section 8.5 is a path a person
// can walk, and these rows walk it against a FAKE CLUSTER that behaves like
// kube-apiserver: it stores objects by name and answers a second create under
// a name it already holds with a `409 AlreadyExists` Status.
//
// THE NEGATIVE CONTROL FOR EACH BEHAVIOUR IS THE ROW BESIDE IT. One run from a
// double click is only interesting beside a second intent that DOES create a
// second object; a replay is only interesting beside a stored request hash
// that differs and is refused instead of adopted.

const NAME_RULE = fixture("manual-backup-names.json");

/** A fake kube-apiserver for one namespace: objects by plural and name, the
 *  `AlreadyExists` Status on a repeat create, and a record of every write. */
function fakeCluster(seed) {
  const store = new Map(Object.entries(seed || {}));
  const writes = [];
  const status = (code, reason, message) => ({
    status: code,
    body: {
      kind: "Status", apiVersion: "v1", status: "Failure",
      code: code, reason: reason, message: message,
    },
  });
  const cluster = {
    store: store,
    writes: writes,
    refuseCreate: null,
    of(plural) {
      return [...store.entries()]
        .filter(([k]) => k.indexOf(plural + "/") === 0)
        .map(([, v]) => v);
    },
    answer(url, init) {
      const path = String(url).split("?")[0];
      const parts = path.split("/");
      const plural = parts[6];
      const name = parts[7];
      if ((init.method || "GET") === "GET") {
        if (name === undefined) {
          return { status: 200, body: { kind: "BackupList", apiVersion: "logweir.dev/v1alpha1", items: cluster.of(plural) } };
        }
        const held = store.get(plural + "/" + name);
        return held === undefined
          ? status(404, "NotFound", plural + ".logweir.dev \"" + name + "\" not found")
          : { status: 200, body: held };
      }
      if (init.method === "POST") {
        const sent = JSON.parse(init.body);
        writes.push(sent);
        if (cluster.refuseCreate !== null) {
          return cluster.refuseCreate;
        }
        const key = plural + "/" + sent.metadata.name;
        if (store.has(key)) {
          return status(409, "AlreadyExists",
            plural + ".logweir.dev \"" + sent.metadata.name + "\" already exists");
        }
        // The API server fills in what it owns. Nothing else is touched: a
        // stored object is the object that was sent.
        const stored = JSON.parse(JSON.stringify(sent));
        stored.apiVersion = "logweir.dev/v1alpha1";
        stored.kind = "Backup";
        stored.metadata.uid = "uid-" + String(store.size);
        stored.metadata.creationTimestamp = "2026-09-21T09:0" + String(store.size) + ":00Z";
        stored.status = { phase: "Pending" };
        store.set(key, stored);
        return { status: 201, body: stored };
      }
      return status(405, "MethodNotAllowed", init.method);
    },
  };
  return cluster;
}

/** The schedule the legacy rows copy: the live `schedule-policy.json` with a
 *  uid and a `status.policy` at the SAME generation as `metadata.generation`,
 *  which is the controller's own statement that the two describe one revision. */
function legacySchedule(overrides) {
  const object = JSON.parse(JSON.stringify(SCHEDULE));
  object.metadata.uid = "8d6be0c6-0000-4000-8000-0000000000ff";
  object.status.policy.generation = object.metadata.generation;
  return Object.assign(object, overrides || {});
}

async function legacyRun(cluster, ns, key, body) {
  const net = transport((u, init) => cluster.answer(u, init));
  try {
    return await apiClient().runBackupNow(
      ns, body || { scheduleRef: { name: "tz", expectedGeneration: 1 } }, key,
    );
  } finally {
    net.restore();
  }
}

test("the_manual_run_name_rule_is_one_rule_and_the_fixture_pins_both_sides", async () => {
  // THE FIXTURE IS THE CONTRACT BETWEEN TWO IMPLEMENTATIONS.
  // `crates/logweir-api/src/idempotency.rs::identity` is the other one, and
  // `scripts/live/d1/run.py`'s L-06-2-cli drives the REAL binary over the same
  // rule and fails if the object kube-apiserver stored is not named what this
  // function says. Here the page's half is held to every recorded row.
  assert.equal(NAME_RULE.rule.prefix, "logweir-manual-");
  assert.equal(NAME_RULE.rule.hashChars, 26);
  assert.deepEqual(NAME_RULE.rule.fieldOrder, ["issuer", "subject", "namespace", "route", "key"]);
  assert.ok(NAME_RULE.rows.length >= 6);
  for (const row of NAME_RULE.rows) {
    assert.equal(await manualBackupName(row.scope), row.name,
      "the page derives a different name from the recorded scope: " + row.note);
    assert.equal(row.name.length, 41, "prefix plus 26 characters is a 41-character DNS-1123 name");
    assert.match(row.name, /^logweir-manual-[a-z2-7]{26}$/,
      "lowercase RFC 4648 base32, which is DNS-label safe");
  }
  assert.equal(new Set(NAME_RULE.rows.map((r) => r.name)).size, NAME_RULE.rows.length,
    "every recorded scope has its own name, including the pair whose fields are the same " +
      "characters cut in two places -- that pair is what the 64-bit length prefix is for");

  // THE DRIFT CONTROL: a guard that cannot fail is not a guard. One character
  // changed in ANY field of the scope must change the name, or the field is
  // not in the digest at all.
  const base = NAME_RULE.rows[0].scope;
  for (const field of NAME_RULE.rule.fieldOrder) {
    const moved = Object.assign({}, base);
    moved[field] = String(base[field]) + "x";
    assert.notEqual(await manualBackupName(moved), NAME_RULE.rows[0].name,
      field + " is not part of the derived name, so two different requests would collide");
  }
  // And the legacy scope is a DIFFERENT scope from the API actor's, so the two
  // never collide by accident either.
  const legacy = NAME_RULE.rows.find((r) => r.scope.issuer === "" && r.scope.subject === "");
  assert.ok(legacy !== undefined, "the fixture records what legacy mode puts in the first two fields");
});

test("legacy_mode_creates_d1_section_8_1s_canonical_backup_and_a_double_click_is_one_run", async () => {
  await legacyMode();
  const ns = "lw-legacy";
  const schedule = legacySchedule();
  const cluster = fakeCluster({ "backupschedules/tz": schedule });
  try {
    const key = "logweir-ui.manual.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const first = await legacyRun(cluster, ns, key);
    const second = await legacyRun(cluster, ns, key);

    // ONE OBJECT. Two clicks, two POSTs, one run: the name is a function of
    // the scope, so the second create is kube-apiserver's own AlreadyExists.
    assert.equal(cluster.writes.length, 2, "both clicks were really sent");
    assert.equal(cluster.of("backups").length, 1, "and exactly one Backup exists");
    assert.equal(first.replayed, false);
    assert.equal(second.replayed, true);
    assert.equal(first.run.metadata.uid, second.run.metadata.uid, "the same run, both times");

    // THE SAME NAME THE PRODUCT API WOULD DERIVE FOR THIS SCOPE.
    assert.equal(first.run.metadata.name, await manualBackupName({
      issuer: "", subject: "", namespace: ns, route: MANUAL_BACKUP_ROUTE, key: key,
    }));

    // D1 section 8.1's OBJECT, field for field: the four labels and no others, the
    // policy copied from the schedule, and the identity block.
    const made = first.run;
    assert.deepEqual(made.metadata.labels, {
      "logweir.dev/trigger": "manual",
      "logweir.dev/attempt": "0",
      "logweir.dev/schedule": "tz",
      "logweir.dev/schedule-uid": schedule.metadata.uid,
    });
    assert.equal(made.spec.triggeredBy, "manual");
    assert.deepEqual(made.spec.trigger, { kind: "Manual", attempt: 0 });
    assert.equal(made.spec.slot, undefined, "a manual run has no slot");
    assert.deepEqual(made.spec.sourceRef, { name: schedule.spec.sourceRef.name });
    assert.deepEqual(made.spec.topics, schedule.spec.topics,
      "the operator's order, verbatim: the digest canonicalises and the object does not");
    assert.equal(made.spec.archive.url, schedule.spec.archive.url);
    assert.equal(made.spec.deadlineSeconds, schedule.spec.activeDeadlineSeconds);
    assert.deepEqual(made.spec.scheduleRef, {
      name: "tz",
      uid: schedule.metadata.uid,
      generation: schedule.metadata.generation,
      runPolicySha256: schedule.status.policy.runPolicySha256,
    });
    // THE DIGEST IS COPIED, NEVER COMPUTED. This is the one number a browser
    // must not produce: the controller recomputes it and refuses the run
    // terminally on a mismatch.
    assert.equal(made.spec.scheduleRef.runPolicySha256, schedule.status.policy.runPolicySha256);
    // AND THE REPLAY GUARD IS ON THE OBJECT, not in this page's memory.
    assert.match(made.metadata.annotations["logweir.dev/request-sha256"], /^sha256:[0-9a-f]{64}$/);
    // The schedule's own state travels with the run, as notices.
    assert.equal(first.schedule.generation, schedule.metadata.generation);
    assert.equal(first.schedule.suspended, schedule.spec.suspend === true);
  } finally {
    resetMode();
  }
});

test("a_deliberate_second_legacy_backup_is_a_second_object_and_a_reused_intent_is_not", async () => {
  await legacyMode();
  const ns = "lw-legacy";
  const cluster = fakeCluster({ "backupschedules/tz": legacySchedule() });
  const key = formKey(ns, RUN_NOW_FORM, "tz");
  dropDraft(key);
  try {
    // The intents the PAGE would hold: one per draft, ended by "Back up again".
    const first = await legacyRun(cluster, ns, intentFor(key));
    const repeat = await legacyRun(cluster, ns, intentFor(key));
    const deliberate = await legacyRun(cluster, ns, newIntent(key));

    assert.equal(cluster.of("backups").length, 2,
      "three clicks, two runs: the repeat replayed and the deliberate one did not");
    assert.equal(repeat.replayed, true);
    assert.equal(repeat.run.metadata.uid, first.run.metadata.uid);
    assert.equal(deliberate.replayed, false);
    assert.notEqual(deliberate.run.metadata.name, first.run.metadata.name,
      "a new intent is a new scope and therefore a new name");
    assert.notEqual(deliberate.run.metadata.uid, first.run.metadata.uid);
  } finally {
    dropDraft(key);
    resetMode();
  }
});

test("the_same_legacy_intent_on_a_different_request_is_refused_and_never_adopted", async () => {
  // THE NEGATIVE CONTROL FOR THE REPLAY. "An object already exists under this
  // name" is not "this run is mine": the stored request hash decides, and a
  // different request under a spent intent is a refusal the page can render,
  // not a run somebody else's click made.
  await legacyMode();
  const ns = "lw-legacy";
  const cluster = fakeCluster({ "backupschedules/tz": legacySchedule() });
  try {
    const key = "logweir-ui.manual.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    await legacyRun(cluster, ns, key);
    await assert.rejects(
      () => legacyRun(cluster, ns, key, {
        scheduleRef: { name: "tz", expectedGeneration: 1 },
        readinessAcknowledgement: { preflight: "pf-1", state: "notReady" },
      }),
      (error) => {
        assert.equal(error.reason, "idempotency_conflict");
        assert.equal(error.status, 409);
        return true;
      },
    );
    assert.equal(cluster.of("backups").length, 1, "and nothing was created or adopted");
  } finally {
    resetMode();
  }
});

test("a_stale_legacy_card_is_a_policy_changed_refusal_carrying_the_revision_in_force", async () => {
  await legacyMode();
  const ns = "lw-legacy";
  const schedule = legacySchedule();
  schedule.metadata.generation = 9;
  schedule.status.policy.generation = 9;
  const cluster = fakeCluster({ "backupschedules/tz": schedule });
  try {
    await assert.rejects(
      () => legacyRun(cluster, ns, "logweir-ui.manual.cccccccccccccccccccccccccccccccc",
        { scheduleRef: { name: "tz", expectedGeneration: 1 } }),
      (error) => {
        assert.equal(error.reason, "policy_changed");
        assert.equal(error.policy.currentGeneration, 9);
        assert.equal(error.policy.currentRunPolicySha256, schedule.status.policy.runPolicySha256);
        return true;
      },
    );
    assert.equal(cluster.writes.length, 0, "nothing was sent: the refusal is BEFORE the create");
    // And the page renders it with the revision in force, in both modes, from
    // the one renderer.
    const line = renderRunNowConflict({
      phase: "failed", kind: "conflict",
      error: {
        reason: "policy_changed",
        policy: { currentGeneration: 9, currentRunPolicySha256: schedule.status.policy.runPolicySha256 },
      },
    });
    assert.match(line, /revision g9/);
  } finally {
    resetMode();
  }
});

test("legacy_mode_refuses_rather_than_computing_a_run_policy_digest_of_its_own", async () => {
  // THE DEFECT CLASS THIS CLOSES. `runPolicySha256` is computed in exactly one
  // place -- `weirkeeper::policy::run_policy_sha256` -- because the controller
  // recomputes it and refuses the run TERMINALLY on a mismatch. A browser that
  // canonicalised the policy itself would be a second opinion about a number
  // whose whole job is to say that two parties agree. So when the controller
  // has not yet published a digest for the revision this page is copying, the
  // page says which number is behind and creates nothing.
  await legacyMode();
  const ns = "lw-legacy";
  const behind = legacySchedule();
  behind.metadata.generation = 4;
  behind.status.policy.generation = 3;
  const cluster = fakeCluster({ "backupschedules/tz": behind });
  try {
    await assert.rejects(
      () => legacyRun(cluster, ns, "logweir-ui.manual.dddddddddddddddddddddddddddddddd",
        { scheduleRef: { name: "tz" } }),
      (error) => {
        assert.equal(error.reason, "PolicyDigestNotPublished");
        assert.equal(error.kind, "refused");
        assert.match(error.message, /still at g3/);
        return true;
      },
    );
    assert.equal(cluster.writes.length, 0);
    // The ad-hoc body has no schedule to copy at all, and is named rather than
    // half-built.
    await assert.rejects(
      () => legacyRun(cluster, ns, "logweir-ui.manual.eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        { sourceRef: { name: "source" } }),
      /Backing up an ad-hoc selection/,
    );
  } finally {
    resetMode();
  }
});

test("a_refused_legacy_create_renders_the_api_servers_own_reason_verbatim", async () => {
  // D1 section 8.5's Rejected row, and the reason the grant is a chart change rather
  // than a sentence: when kube-apiserver refuses this identity, what the
  // person reads is the API SERVER'S message, not a sentence this page
  // composed about a decision it did not make.
  await legacyMode();
  const ns = "lw-legacy";
  const cluster = fakeCluster({ "backupschedules/tz": legacySchedule() });
  const message = "backups.logweir.dev is forbidden: User \"system:serviceaccount:logweir:" +
    "logweir-ui\" cannot create resource \"backups\" in API group \"logweir.dev\" in the " +
    "namespace \"lw-legacy\"";
  cluster.refuseCreate = {
    status: 403,
    body: { kind: "Status", apiVersion: "v1", status: "Failure", code: 403,
      reason: "Forbidden", message: message },
  };
  try {
    let captured = null;
    await assert.rejects(
      () => legacyRun(cluster, ns, "logweir-ui.manual.ffffffffffffffffffffffffffffffff"),
      (error) => {
        captured = error;
        return true;
      },
    );
    assert.equal(captured.status, 403);
    assert.equal(captured.reason, "Forbidden");
    assert.equal(captured.message, message, "the API server's sentence, unedited");

    const html = renderRunNowPanel({
      ns: ns, name: "tz", object: legacySchedule(), mayOperate: true, runs: [],
      state: { phase: "failed", kind: "rejected", error: captured },
    });
    assert.ok(html.indexOf("cannot create resource &quot;backups&quot;") !== -1,
      "the server's own words are on screen: " + html.slice(0, 400));
    assert.match(html, /403 Forbidden/);
    assert.equal(offersAnotherRun({ state: { phase: "failed", error: captured } }), false,
      "and a 403 is not answered by spending a fresh intent on the same refused request");
  } finally {
    resetMode();
  }
});
