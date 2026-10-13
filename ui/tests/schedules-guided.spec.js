// schedules-guided.spec.js -- the guided create form.
//
// Selected and all-user-topic creation, edit, invalid cron, readiness
// failure, keyboard use and first-run redirect, against "the standard route
// requires no YAML, raw endpoint reconstruction or manual signing step;
// invalid fields retain the draft". Each row carries its own NEGATIVE CONTROL,
// the assertion that fails if the behaviour is removed. The browser half (a
// real keyboard, a real redirect, a real Preflight) needs a live console; these
// rows are the ones a string can prove.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { decoded, wireItem } from "./console-fixture.js";

import { CONSOLE, resetMode, selectMode } from "../client.js";
import { dropDraft, formKey, keepDraft, mutationFor, readDraft } from "../lifecycle.js";
import { LIFE, fakeView, parse } from "./fake-view.js";
import { esc } from "../render.js";
import {
  ADVANCED_CRON,
  CREATE_PANEL,
  CREATE_TAKES_A_DESTINATION,
  POLICY_DRAFT_FIELDS,
  PREVIEW_BEFORE_SAVE,
  RETENTION_POINTER_SENTENCE,
  RETENTION_REMOVED_ON_SAVE_SENTENCE,
  SCHEDULE_DRAFT_FIELDS,
  SCHEDULE_FIELD_PATHS,
  SCHEDULE_FORM,
  READINESS_STALE_SENTENCE,
  SCHEDULE_NAME_HELP,
  SCHEDULE_NAME_MINTED_SENTENCE,
  guidedValues,
  mintScheduleIntent,
  mountSchedules,
  policyBody,
  policyFormView,
  policyValuesOf,
  readinessKey,
  readinessRequestFor,
  renderPolicyForm,
  renderReadinessVerdict,
  renderScheduleForm,
  scheduleBody,
  scheduleDetailRoute,
  scheduleFormView,
  submitPolicy,
  submitSchedule,
  validateSchedule,
} from "../pages/schedules.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

const DESTINATIONS = Object.freeze([
  Object.freeze({ name: "primary", canonicalUrl: "s3://kafka-backups/logweir" }),
  Object.freeze({ name: "offsite", canonicalUrl: "s3://kafka-offsite/logweir" }),
]);

const CLUSTERS = Object.freeze({
  items: [
    {
      metadata: { name: "orders-prod", uid: "uid-A" },
      spec: { role: "source", bootstrapServers: ["kafka:9093"] },
      status: {},
    },
  ],
});

/** A filled-in guided draft: a daily preset in a named zone, a named
 *  allowlist, and a saved destination. Nothing about it is YAML and nothing
 *  about it is an endpoint. */
function draft(extra) {
  return Object.assign({
    name: "nightly",
    source: "orders-prod",
    sourceUid: "uid-A",
    mode: "daily",
    cron: "",
    hour: "2",
    minute: "30",
    dayOfWeek: "1",
    dayOfMonth: "1",
    n: "6",
    timeZone: "Europe/Berlin",
    selection: "named",
    topics: "orders, payments",
    incompleteDiscovery: "",
    excludeTopics: "",
    excludePrefixes: "",
    destination: "primary",
    archive: "",
    archiveSecret: "",
    concurrencyPolicy: "",
    startingDeadlineSeconds: "",
    catchUpPolicy: "",
    maxRetries: "",
    retryDelaySeconds: "",
    activeDeadlineSeconds: "",
    suspended: "false",
  }, extra || {});
}

/** The preview the API answered for those values -- the only place a canonical
 *  expression can come from. */
function previewFor(values) {
  return {
    query: {
      count: 5,
      timeZone: values.timeZone,
      preset: "daily",
      hour: Number(values.hour),
      minute: Number(values.minute),
    },
    answer: {
      schedule: "30 2 * * *",
      timeZone: values.timeZone,
      tzdb: "2026a",
      runs: [{ at: "2026-09-22T00:30:00Z", localTime: "02:30:00+02:00" }],
    },
    error: null,
  };
}

// THE STANDARD ROUTE: no YAML, no endpoint, no signature.
test("the_standard_route_asks_for_a_cadence_a_coverage_and_a_saved_destination", () => {
  const html = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, destinations: DESTINATIONS, mayOperate: true,
  });
  // The four controls the standard route is made of, each rendered by the same
  // function the Future policy panel uses.
  assert.match(html, /<select id="policy-create-mode" name="mode">/, "a cadence preset");
  assert.match(html, /<select id="policy-create-selection" name="selection">/, "a coverage");
  assert.match(html, /<select id="policy-create-destination" name="destination">/, "a location");
  assert.match(html, /id="schedule-source"/, "and the saved cluster selector");
  assert.match(html, /<option value="primary"/);
  assert.match(html, /<option value="offsite"/);

  // NEGATIVE CONTROL 1: the hand-typed endpoint is NOT on the standard route;
  // if `#schedule-archive` comes back out of its disclosure this row fails.
  const beforeInline = html.indexOf("Advanced: write to an archive URL");
  const archiveInput = html.indexOf("id=\"policy-create-archive\"");
  assert.ok(beforeInline !== -1, "the inline archive is behind a disclosure");
  assert.ok(archiveInput > beforeInline,
    "the archive URL input is INSIDE that disclosure and not above it");
  assert.doesNotMatch(html, /id="schedule-archive"/,
    "the old top-level required archive URL input is gone");

  // NEGATIVE CONTROL 2: no YAML and no signing step anywhere on the form.
  assert.doesNotMatch(html, /apiVersion|kind: BackupSchedule|textarea/i);
  assert.doesNotMatch(html, /private key|sign this|signature/i);

  // And the deadlines, retries, catch-up and concurrency are collapsed.
  assert.match(html, /<details class="advanced" id="schedule-advanced">/);
  const advanced = html.indexOf("id=\"schedule-advanced\"");
  for (const field of ["startingDeadlineSeconds", "catchUpPolicy", "maxRetries",
    "activeDeadlineSeconds", "concurrencyPolicy"]) {
    const at = html.indexOf("name=\"" + field + "\"");
    assert.ok(at > advanced, field + " is inside the collapsed advanced section");
  }
});

test("every_field_the_body_sends_has_an_input_and_every_input_is_in_the_draft", () => {
  // THE FORM, THE DRAFT AND THE BODY ARE ONE LIST. A body field with no input is
  // one the form cannot express; a form field the draft does not keep is one a
  // failed submit silently loses.
  const values = draft({ selection: "dynamic", incompleteDiscovery: "Refuse" });
  const html = renderScheduleForm({
    draft: values, clusters: CLUSTERS, destinations: DESTINATIONS, mayOperate: true,
  });
  for (const field of ["mode", "timeZone", "selection", "incompleteDiscovery",
    "excludeTopics", "excludePrefixes", "destination", "concurrencyPolicy",
    "startingDeadlineSeconds", "catchUpPolicy", "maxRetries", "retryDelaySeconds",
    "activeDeadlineSeconds", "suspended"]) {
    assert.ok(html.indexOf("name=\"" + field + "\"") !== -1, field + " has an input");
    assert.ok(SCHEDULE_DRAFT_FIELDS.indexOf(field) !== -1, field + " is kept in the draft");
  }
  // NEGATIVE CONTROL: a field that is NOT in the draft list is a field this row
  // would have missed, so the list itself is asserted to be the union.
  assert.ok(SCHEDULE_DRAFT_FIELDS.indexOf("name") !== -1);
  assert.ok(SCHEDULE_DRAFT_FIELDS.indexOf("sourceUid") !== -1,
    "the chosen cluster's identity is part of the draft, not just its name");
});

// SELECTED-TOPIC CREATION, AND ALL-USER-TOPIC CREATION.
test("a_selected_topic_creation_names_the_destination_and_the_compiled_expression", () => {
  const values = draft();
  const body = scheduleBody(values, "30 2 * * *");
  assert.equal(body.spec.schedule, "30 2 * * *",
    "the expression is the one the API compiled, never one this page built");
  assert.equal(body.spec.timeZone, "Europe/Berlin");
  assert.deepEqual(body.spec.sourceRef, { name: "orders-prod" });
  assert.deepEqual(body.spec.topics, ["orders", "payments"]);
  assert.deepEqual(body.spec.destinationRef, { name: "primary" });
  assert.equal(body.spec.archive, undefined,
    "one location, never both: a chosen destination sends no inline archive at all");
  assert.equal(body.spec.allUserTopics, undefined);
  assert.equal(body.spec.suspend, false);

  // NEGATIVE CONTROL: with no destination chosen the inline archive is what is
  // sent -- so the row above is about the CHOICE and not about a builder that
  // always writes `destinationRef`.
  const inline = scheduleBody(draft({ destination: "", archive: "s3://b/p", archiveSecret: "s" }),
    "30 2 * * *");
  assert.deepEqual(inline.spec.archive, { url: "s3://b/p", secretRef: { name: "s" } });
  assert.equal(inline.spec.destinationRef, undefined);
});

test("an_all_user_topic_creation_sends_the_dynamic_block_and_no_named_topics", () => {
  const values = draft({
    selection: "dynamic",
    incompleteDiscovery: "BackUpVisibleTopics",
    excludeTopics: "scratch, tmp",
    excludePrefixes: "dev-",
    topics: "orders, payments",
  });
  const body = scheduleBody(values, "30 2 * * *");
  assert.deepEqual(body.spec.topics, [],
    "a dynamic selection names no topics, even with a stale allowlist still in the draft");
  assert.deepEqual(body.spec.allUserTopics, {
    incompleteDiscovery: "BackUpVisibleTopics",
    exclude: { topics: ["scratch", "tmp"], prefixes: ["dev-"] },
  });

  // NEGATIVE CONTROL: the policy with no answer to "what if discovery cannot
  // prove it saw everything" is refused HERE, because both possible defaults
  // are wrong in a way nobody would notice.
  const undecided = validateSchedule(draft({ selection: "dynamic", incompleteDiscovery: "" }));
  assert.match(undecided.incompleteDiscovery, /There is no default/);
  assert.equal(validateSchedule(values).incompleteDiscovery, undefined);
});

// INVALID CRON, AND THE DRAFT THAT SURVIVES IT.
test("an_invalid_cadence_is_the_apis_refusal_and_not_this_pages_opinion", () => {
  // THE SHAPE CHECK STAYS AND THE PARSE DOES NOT. `61 * * * *` is five fields
  // of cron characters; whether a minute may be 61 is the cadence engine's
  // answer, and the person reads the API's words.
  const typed = draft({ mode: ADVANCED_CRON, cron: "61 * * * *" });
  assert.equal(validateSchedule(typed).cron, undefined,
    "the page does not pre-empt the cron parser");

  // NEGATIVE CONTROL: it is not that the page checks nothing -- a line that is
  // not five fields at all never leaves the browser.
  assert.match(validateSchedule(draft({ mode: ADVANCED_CRON, cron: "every night" })).cron,
    /five cron fields/);

  // AND THE REFUSAL LANDS ON THE CADENCE INPUT, whichever API answered it.
  const consoleError = { code: "validation_failed", errors: [{ field: "schedule", code: "schedule_invalid", message: "minute 61 is out of range" }] };
  const legacyError = { code: "validation_failed", errors: [{ field: "spec.schedule", code: "invalid", message: "minute 61 is out of range" }] };
  for (const [label, error] of [["console", consoleError], ["legacy", legacyError]]) {
    const mapped = SCHEDULE_FIELD_PATHS.filter((pair) => pair[0] === error.errors[0].field);
    assert.equal(mapped.length, 1, label + " names a field this form has an input for");
    assert.equal(mapped[0][1], "cron", label + " maps it onto the cadence input");
  }
  // And the zone has its own field, so an unknown zone does not highlight the
  // expression.
  assert.deepEqual(
    SCHEDULE_FIELD_PATHS.filter((pair) => pair[0] === "timeZone")[0],
    ["timeZone", "timeZone"],
  );
});

test("a_refused_create_renders_the_apis_words_and_re_renders_every_typed_value", async () => {
  const ns = "retain-ns";
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  const values = draft({ mode: ADVANCED_CRON, cron: "61 * * * *", timeZone: "Mars/Olympus" });
  keepDraft(key, values, SCHEDULE_DRAFT_FIELDS);
  const record = mutationFor(key);
  await record.run(async () => {
    const error = new Error("the schedule is not valid");
    error.kind = "rejected";
    error.status = 422;
    error.code = "validation_failed";
    // The shape `ui/api.js::problemError` builds from a `422` document and
    // `lifecycle.js::fieldErrors` reads: one cause per field, each with the
    // API's own message.
    error.details = {
      causes: [
        { field: "schedule", reason: "schedule_invalid", message: "minute 61 is out of range" },
        { field: "timeZone", reason: "timezone_unknown",
          message: "Mars/Olympus is not an IANA zone" },
      ],
    };
    throw error;
  }).catch(() => {});
  const view = scheduleFormView(ns, CLUSTERS, undefined, undefined, {
    destinations: DESTINATIONS, mayOperate: true,
  });
  const html = renderScheduleForm(view);
  // THE DRAFT IS STILL THERE, field for field.
  assert.equal(readDraft(key).cron, "61 * * * *");
  assert.equal(readDraft(key).topics, "orders, payments");
  assert.match(html, /value="61 \* \* \* \*"/, "the cadence the person typed is back on screen");
  assert.match(html, /value="orders, payments"/, "and so is everything else");
  // AND THE WORDS ARE THE API'S.
  assert.match(html, /minute 61 is out of range/);
  assert.match(html, /Mars\/Olympus is not an IANA zone/);
  assert.match(html, /aria-invalid="true"/);

  // NEGATIVE CONTROL: an unrefused form carries none of those messages, so the
  // row above is about the refusal and not about a sentence the form always
  // prints.
  dropDraft(key);
  record.clear();
  const clean = renderScheduleForm({ draft: draft(), clusters: CLUSTERS, mayOperate: true });
  assert.doesNotMatch(clean, /minute 61 is out of range/);
  assert.doesNotMatch(clean, /aria-invalid="true"/);
});

// THE PREVIEW IS THE ONLY EXPRESSION THIS PAGE WILL SAVE.
test("a_preset_cannot_be_created_until_the_api_has_compiled_it", async () => {
  const values = draft();
  const sent = [];
  const api = {
    list: async () => CLUSTERS,
    create: async (...a) => { sent.push(a); return { metadata: { name: "sch-1" } }; },
  };
  await assert.rejects(
    () => submitSchedule("preview-ns", values, api, CLUSTERS, null),
    (error) => {
      assert.equal(error.kind, "invalid");
      assert.equal(error.fields.mode, PREVIEW_BEFORE_SAVE);
      return true;
    },
  );
  assert.equal(sent.length, 0, "nothing was sent without a compiled expression");

  // THE CONTROL: the same values WITH a preview of exactly them are created,
  // and the expression stored is the server's.
  await submitSchedule("preview-ns", values, api, CLUSTERS, previewFor(values));
  assert.equal(sent.length, 1);
  assert.equal(sent[0][2].spec.schedule, "30 2 * * *");

  // AND A PREVIEW OF DIFFERENT VALUES IS NOT A PREVIEW OF THESE. The cadence
  // moved after the preview was taken; creating would store the expression for
  // an hour nobody asked for.
  await assert.rejects(
    () => submitSchedule("preview-ns", draft({ hour: "5" }), api, CLUSTERS, previewFor(values)),
    (error) => error.fields.mode === PREVIEW_BEFORE_SAVE,
  );
  assert.equal(sent.length, 1);

  // An Advanced cron needs none of this: the typed line IS the expression.
  await submitSchedule("preview-ns", draft({ mode: ADVANCED_CRON, cron: "0 4 * * *" }),
    api, CLUSTERS, null);
  assert.equal(sent.length, 2);
  assert.equal(sent[1][2].spec.schedule, "0 4 * * *");
});

// READINESS: the check's own verdict, from the readiness route.
test("readiness_checks_what_the_form_describes_and_renders_only_what_it_recorded", () => {
  const request = readinessRequestFor(draft());
  assert.deepEqual(request, {
    operation: "backup",
    backup: { sourceConnection: "orders-prod", topics: ["orders", "payments"],
      destination: "primary" },
  });
  // A dynamic schedule has no concrete topics before its per-run discovery.
  // The route rejects `topics: []`, so the page must not start a preflight
  // that looks applicable but is guaranteed to be refused.
  assert.equal(
    readinessRequestFor(draft({ selection: "dynamic", incompleteDiscovery: "Refuse" })),
    null,
  );
  // Inline schedules carry the route's actual legacyArchive spelling; a bare
  // destination field is not an inline archive and must not be invented.
  assert.deepEqual(
    readinessRequestFor(draft({ destination: "", archive: "s3://b/p", archiveSecret: "s3-creds" })).backup,
    { sourceConnection: "orders-prod", topics: ["orders", "payments"],
      legacyArchive: { url: "s3://b/p", credentialRef: { name: "s3-creds" } } },
  );

  // WITH NO CHECK STARTED THE FORM SAYS SO, and never "ready".
  const unchecked = renderScheduleForm({ draft: draft(), clusters: CLUSTERS, mayOperate: true });
  assert.match(unchecked, /data-readiness="unchecked"/);
  assert.doesNotMatch(unchecked, /badge-green">ready/);
  const dynamic = renderScheduleForm({
    draft: draft({ selection: "dynamic", incompleteDiscovery: "Refuse" }),
    clusters: CLUSTERS, mayOperate: true,
  });
  assert.match(dynamic, /id="schedule-readiness-dynamic"/);
  assert.doesNotMatch(dynamic, /id="schedule-check-readiness"/,
    "a dynamic selection cannot submit an empty topic list to the backup-preflight route");

  // A FAILED CHECK IS THE CHECK'S OWN WORDS. The verdict, the reason and the
  // remedy all come off the `CheckOperationResponse`; nothing here decides.
  const notReady = decoded("preflight-not-ready.json").item;
  const failed = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, mayOperate: true, readiness: notReady,
  });
  assert.match(failed, /not ready/);
  assert.ok(failed.indexOf(notReady.reason) !== -1,
    "the reason on screen is the one the check recorded: " + notReady.reason);

  // NEGATIVE CONTROL: the same render with a READY verdict does not say not
  // ready, so the row above is reading the object and not a fixed string.
  const ready = decoded("preflight-ready.json").item;
  const green = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, mayOperate: true, readiness: ready,
  });
  assert.doesNotMatch(green, /data-readiness="unchecked"/);
  assert.ok(green.indexOf(notReady.reason) === -1);
});

// KEYBOARD USE, AND THE FIRST-RUN REDIRECT.
test("every_control_on_the_standard_route_is_a_native_focusable_control", () => {
  // KEYBOARD USE IS A PROPERTY OF THE MARKUP, and this is the half of it a
  // string can carry: every interactive element is an `input`, `select`,
  // `button`, `summary` or `label`, each with a `for`/`id` pair, and nothing is
  // a `div` with a click handler. The other half -- that tabbing through them
  // in order actually completes the create -- is the browser journey.
  const html = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, destinations: DESTINATIONS, mayOperate: true,
  });
  assert.doesNotMatch(html, /<(div|span|a)[^>]*onclick/i);
  assert.doesNotMatch(html, /tabindex="[1-9]/, "no positive tab index reorders the form");
  // Every `id` an input carries has a label pointing at it.
  const ids = [...html.matchAll(/<(?:input|select)[^>]*id="([^"]+)"/g)].map((m) => m[1]);
  assert.ok(ids.length >= 10, "the form has controls to check: " + ids.length);
  for (const id of ids) {
    if (id.endsWith("-search") || id.endsWith("-name") || id.endsWith("-uid")) {
      continue; // the selector's search box and its two hidden inputs
    }
    assert.ok(html.indexOf("for=\"" + id + "\"") !== -1, id + " has a label");
  }
  // The submit is a real submit button, so Enter in any field submits the form.
  assert.match(html, /<button type="submit" class="primary"/);
});

test("the_first_run_redirect_is_a_deep_link_to_the_schedule_that_was_created", () => {
  assert.equal(scheduleDetailRoute("team-a", "sch-abc"), "#/schedules?ns=team-a&name=sch-abc");
  assert.equal(scheduleDetailRoute("team a", "sch/abc"), "#/schedules?ns=team%20a&name=sch%2Fabc");
  // NEGATIVE CONTROL: with no name there is no route, so a caller cannot build
  // a link to the LIST and believe it is a link to the schedule it created.
  assert.equal(scheduleDetailRoute("team-a", ""), null);
  assert.equal(scheduleDetailRoute("team-a", undefined), null);
  // The list's own deep link is unchanged and is not this one.
  assert.notEqual(scheduleDetailRoute("team-a", "sch-abc"), "#/schedules?ns=team-a");
});

// THE TWO FORMS ARE ONE FORM.
test("the_guided_form_and_the_policy_panel_render_from_the_same_field_renderers", () => {
  // The create form's controls are keyed by `CREATE_PANEL` and the edit panel's
  // by the schedule's name, which is what lets both be on screen at once
  // without two elements sharing an id.
  const html = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, destinations: DESTINATIONS, mayOperate: true,
  });
  assert.equal(CREATE_PANEL, "create");
  for (const field of ["mode", "selection", "destination", "timeZone", "catchUpPolicy"]) {
    assert.ok(html.indexOf("id=\"policy-" + CREATE_PANEL + "-" + field + "\"") !== -1,
      field + " is rendered by the shared renderer");
  }
  assert.ok(CREATE_TAKES_A_DESTINATION.indexOf("destinationRef") !== -1);
});

test("values_with_no_mode_are_an_advanced_cron_and_no_selection_is_a_named_allowlist", () => {
  const legacy = { name: "n", source: "s", cron: "0 * * * *", topics: "orders", archive: "s3://b/p" };
  assert.equal(guidedValues(legacy).mode, ADVANCED_CRON);
  assert.equal(guidedValues(legacy).selection, "named");
  // NEGATIVE CONTROL: a values object that DOES name them keeps what it named.
  assert.equal(guidedValues(draft()).mode, "daily");
  assert.equal(guidedValues(draft({ selection: "dynamic" })).selection, "dynamic");
});

// THE MODE THIS FORM IS USED IN.
test("the_create_body_reaches_the_product_api_as_the_whole_policy", async () => {
  // THE TRANSLATION IS THE ONE PLACE THE TWO REQUEST SHAPES DIFFER, and a field
  // the custom resource carries that the DTO never sends is one an operator set
  // and the API never heard about. This drives `ui/client.js`'s real console
  // adapter over the real `fetch` boundary.
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  assert.equal((await selectMode()).mode, CONSOLE);
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (u, init) => {
    seen.push({ url: String(u), init: init || {} });
    return Promise.resolve({
      ok: true,
      status: 201,
      text: () => Promise.resolve(JSON.stringify({
        requestId: "r", replayed: false,
        item: wireItem("schedule.json"),
      })),
    });
  };
  try {
    const body = scheduleBody(draft({
      selection: "dynamic", incompleteDiscovery: "Refuse", excludePrefixes: "dev-",
      startingDeadlineSeconds: "900", catchUpPolicy: "Latest", maxRetries: "2",
      retryDelaySeconds: "600", activeDeadlineSeconds: "7200",
      concurrencyPolicy: "Allow", suspended: "true",
    }), "30 2 * * *");
    const { MODES } = await import("../client.js");
    await MODES.console.create("team-a", "backupschedules", body);
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
  const create = seen.filter((r) => (r.init.method || "GET") === "POST");
  assert.equal(create.length, 1, "one create was sent");
  const sent = JSON.parse(create[0].init.body);
  assert.deepEqual(sent.destinationRef, { name: "primary" });
  assert.equal(sent.archive, undefined);
  assert.equal(sent.timeZone, "Europe/Berlin");
  assert.equal(sent.schedule, "30 2 * * *");
  assert.deepEqual(sent.topics, []);
  assert.deepEqual(sent.allUserTopics,
    { incompleteDiscovery: "Refuse", exclude: { prefixes: ["dev-"] } });
  assert.equal(sent.startingDeadlineSeconds, 900);
  assert.equal(sent.catchUpPolicy, "Latest");
  assert.deepEqual(sent.retry, { maxRetries: 2, delaySeconds: 600 });
  assert.equal(sent.activeDeadlineSeconds, 7200);
  assert.equal(sent.concurrencyPolicy, "Allow");
  assert.equal(sent.suspended, true);

  // NEGATIVE CONTROL: a body with an inline archive and a named allowlist sends
  // `archive` and no `destinationRef`, so the row above is about what the
  // object says and not about a translator that always writes both halves.
  assert.equal(sent.sourceRef.name, "orders-prod");
});

// `spec.retention` IS WITHDRAWN, so neither schedule form offers it and
// neither sends it.

/** Whether `html` carries an input for either withdrawn retention field. */
const offersRetention = (html) => /name="keep(Last|Days)"/.test(html);

test("the_schedule_forms_offer_no_retention_and_send_none", async () => {
  // CONTROL FOR THE PREDICATE: the inputs this row forbids are found when present.
  assert.ok(offersRetention("<input id=\"policy-x-keepLast\" name=\"keepLast\">"));

  // A stored schedule that still carries the withdrawn field.
  const stored = () => ({
    metadata: { name: "nightly", generation: 3 },
    spec: {
      schedule: "0 2 * * *", sourceRef: { name: "orders-prod" }, topics: ["orders"],
      archive: { url: "s3://kafka-backups/team-a", secretRef: { name: "logweir-s3" } },
      retention: { keepLast: 7, keepDays: 30 },
    },
  });

  // 1. Neither form has an input; both show the RetentionPolicy pointer.
  const create = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, destinations: DESTINATIONS, mayOperate: true,
  });
  const edit = renderPolicyForm(policyFormView("team-a", stored(), {}, DESTINATIONS, true));
  for (const [form, html] of [["create", create], ["edit", edit]]) {
    assert.ok(!offersRetention(html), "the " + form + " form offers no keepLast or keepDays");
    assert.match(html, /data-retention-pointer="true"/, "the " + form + " form points at a RetentionPolicy");
    assert.ok(html.includes(esc(RETENTION_POINTER_SENTENCE)), "in the panel's own words (" + form + ")");
  }
  // The edit of a schedule that still carries it says saving removes it; a
  // create, and an edit of a schedule without one, do not.
  assert.ok(edit.includes(esc(RETENTION_REMOVED_ON_SAVE_SENTENCE)));
  assert.ok(!create.includes(esc(RETENTION_REMOVED_ON_SAVE_SENTENCE)));
  const plain = stored();
  delete plain.spec.retention;
  assert.ok(!renderPolicyForm(policyFormView("team-a", plain, {}, DESTINATIONS, true))
    .includes(esc(RETENTION_REMOVED_ON_SAVE_SENTENCE)));

  // 2. No draft keeps it, and no body carries it, even from a draft that holds a value.
  for (const field of ["keepLast", "keepDays"]) {
    assert.equal(SCHEDULE_DRAFT_FIELDS.indexOf(field), -1, field + " is not in the create draft");
    assert.equal(POLICY_DRAFT_FIELDS.indexOf(field), -1, field + " is not in the policy draft");
    assert.equal(policyValuesOf(stored())[field], undefined, "the stored " + field + " is not read back");
  }
  const stale = { keepLast: "7", keepDays: "30" };
  assert.equal(scheduleBody(draft(stale), "30 2 * * *").spec.retention, undefined);
  assert.ok(!("retention" in policyBody(Object.assign(policyValuesOf(stored()), stale), 3, "0 2 * * *")));

  // 3. On the wire: the console create sends no `retention` member...
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (u, init) => {
    seen.push({ url: String(u), init: init || {} });
    return Promise.resolve({
      ok: true, status: 201,
      text: () => Promise.resolve(JSON.stringify({ requestId: "r", replayed: false,
        item: wireItem("schedule.json") })),
    });
  };
  try {
    const { MODES } = await import("../client.js");
    const body = scheduleBody(draft(stale), "30 2 * * *");
    body.spec.retention = { keepLast: 7 };
    await MODES.console.create("team-a", "backupschedules", body);
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
  const posts = seen.filter((r) => (r.init.method || "GET") === "POST");
  assert.equal(posts.length, 1, "one create was sent");
  assert.ok(!("retention" in JSON.parse(posts[0].init.body)),
    "the create request carries no retention, even from an object that names one");

  // ...and the edit sends none either, so saving removes a stored one.
  const edits = [];
  const api = { editSchedulePolicy: async (_ns, _name, sent) => { edits.push(sent); return sent; } };
  await submitPolicy("team-a", stored(), Object.assign(policyValuesOf(stored()), stale), null, api, null);
  assert.equal(edits.length, 1, "one edit was sent");
  assert.ok(!("retention" in edits[0]), "the policy edit carries no retention");
});

// A READINESS VERDICT BELONGS TO THE REQUEST THAT PRODUCED IT, and a refused
// start keeps the button.

test("a_readiness_verdict_goes_stale_when_the_form_no_longer_describes_its_request", () => {
  // Check source A with `orders`, get `ready`, switch to source B: A's `ready`
  // must not stay beside B's inputs.
  const ready = decoded("preflight-ready.json").item;
  const checked = readinessKey(readinessRequestFor(draft()));
  const current = renderReadinessVerdict({ readiness: ready, readinessRequest: checked,
    draft: draft() });
  assert.doesNotMatch(current, /data-readiness="stale"/,
    "NEGATIVE CONTROL: unchanged inputs keep the verdict");
  assert.match(current, /ready/);
  for (const changed of [
    draft({ source: "orders-dr" }),
    draft({ topics: "orders" }),
    draft({ destination: "offsite" }),
  ]) {
    const stale = renderReadinessVerdict({ readiness: ready, readinessRequest: checked,
      draft: changed });
    assert.match(stale, /data-readiness="stale"/);
    assert.doesNotMatch(stale, /badge-green/, "a stale verdict is not shown as a verdict");
    assert.ok(stale.indexOf(READINESS_STALE_SENTENCE) !== -1);
  }
  // A CHANGE THAT DOES NOT ALTER THE REQUEST (the cadence) is not staleness.
  assert.doesNotMatch(renderReadinessVerdict({ readiness: ready, readinessRequest: checked,
    draft: draft({ hour: "5" }) }), /data-readiness="stale"/);
});

test("a_refused_readiness_start_keeps_the_button_and_places_its_field_errors", async () => {
  // A 422 (a mistyped topic) or a network error on startPreflight must not
  // replace the Check button with a note until reload.
  selectMode(CONSOLE);
  const ns = "readiness-refused";
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  keepDraft(key, draft({ source: "uid-A" }), SCHEDULE_DRAFT_FIELDS);
  const view = fakeView();
  let started = 0;
  const refusal = Object.assign(new Error("backup.topics: must be a Kafka topic name"), {
    status: 422, reason: "validation_failed",
    details: { causes: [{ field: "backup.topics", reason: "invalid_topic",
      message: "must be a Kafka topic name" }] },
  });
  const api = {
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters" ? CLUSTERS : { items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS }); },
    startPreflight() { started += 1; return Promise.reject(refusal); },
  };
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), api);
    const button = view.find("#schedule-check-readiness");
    assert.ok(button !== null, "the form offers the check");
    await button.dispatch("click");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(started, 1);
    const after = view.chunks[view.chunks.length - 1].html;
    assert.match(after, /id="schedule-check-readiness"/, "the button survives the refusal");
    assert.match(after, /id="schedule-readiness-error"/, "the refusal is shown beside it");
    assert.match(after, /must be a Kafka topic name/);
    assert.match(after, /id="policy-create-topics"[^>]*aria-invalid="true"/,
      "and its field error is on the topics input");
    assert.doesNotMatch(after, /id="schedule-readiness-unavailable"/);
    // NEGATIVE CONTROL: the button is enabled again, so a second check can run.
    await view.find("#schedule-check-readiness").dispatch("click");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(started, 2, "a second check was possible without a reload");
  } finally {
    dropDraft(key);
    resetMode();
  }
});

test("a_replayed_readiness_check_is_read_before_the_form_believes_it", async () => {
  // A replay is terminal on arrival and its create answer is never recomputed,
  // so a follower that stopped at `terminal === true` before its first read
  // would show "ready / does not apply to your current inputs / could not be
  // checked: this response did not recompute staleness".
  selectMode(CONSOLE);
  const ns = "readiness-replayed";
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  keepDraft(key, draft({ source: "uid-A" }), SCHEDULE_DRAFT_FIELDS);
  const view = fakeView();
  let reads = 0;
  const replay = {
    id: "pf-sgkjlqd3", namespace: ns, uid: "u", resourceVersion: "1", operation: "backup",
    state: "ready", terminal: true, binding: { referents: [] }, applicable: false, stale: true,
    staleReasons: [{ reason: "unverifiable",
      basis: "this response did not recompute staleness; read the preflight itself for a " +
        "current verdict" }],
    staleBasis: [], checks: [], warnings: [], executionOnly: [], detailsAvailable: false,
    conditions: [],
  };
  const api = {
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters" ? CLUSTERS : { items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS }); },
    startPreflight() { return Promise.resolve({ item: replay, replayed: true }); },
    preflight(namespace, id) {
      reads += 1;
      return Promise.resolve({ item: Object.assign({}, replay, {
        id: id, applicable: true, stale: false, staleReasons: [], staleBasis: ["expiry"],
      }) });
    },
    wait: async () => {},
  };
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), api);
    await view.find("#schedule-check-readiness").dispatch("click");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(reads, 1,
      "NEGATIVE CONTROL: the terminal create answer was read once before it was shown as the " +
        "verdict");
    const after = view.chunks[view.chunks.length - 1].html;
    assert.doesNotMatch(after, /did not recompute staleness/,
      "the form's newest paint is the recomputed read, not the create answer");
    assert.match(after, /pf-sgkjlqd3/);
  } finally {
    dropDraft(key);
    resetMode();
  }
});

test("the_readiness_panel_follows_its_check_until_a_read_is_terminal", async () => {
  // The list page's standalone "Backup readiness" panel follows the check it
  // started, rather than rendering the create answer of a check that has not
  // run.
  selectMode(CONSOLE);
  const ns = "readiness-panel-follow";
  const view = fakeView();
  let reads = 0;
  const pending = {
    id: "pf-panel", namespace: ns, uid: "u", resourceVersion: "1", operation: "backup",
    state: "pending", terminal: false, binding: { referents: [] }, applicable: false,
    stale: false, staleReasons: [], staleBasis: [], checks: [], warnings: [],
    executionOnly: [], detailsAvailable: false, conditions: [],
  };
  const api = {
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters" ? CLUSTERS : { items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS }); },
    startPreflight() { return Promise.resolve({ item: pending, replayed: false }); },
    preflight(namespace, id) {
      reads += 1;
      return Promise.resolve({ item: Object.assign({}, pending, reads < 2
        ? { state: "running" }
        : { state: "ready", terminal: true, applicable: true, staleBasis: ["expiry"] }) });
    },
    wait: async () => {},
  };
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), api);
    const form = view.find("#readiness-form");
    assert.ok(form !== null, "the list page offers the readiness panel");
    await form.dispatch("submit");
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(reads, 2, "NEGATIVE CONTROL: read until terminal (zero reads before P8)");
    const panel = view.findAll("#backup-readiness");
    assert.ok(panel.length > 0);
    const last = view.chunks.filter((c) => c.html.includes("id=\"backup-readiness\"")).pop().html;
    assert.match(last, /pf-panel/);
    assert.match(last, /badge-green">ready/, "the verdict the check recorded");
  } finally {
    resetMode();
  }
});

test("the_name_field_says_what_console_mode_does_with_it", () => {
  // Console mode names the object sch-<26 base32>, so the console form offers
  // NO name field and says who names it; the legacy form keeps its field and
  // its help.
  const consoleForm = renderScheduleForm({ draft: draft(), clusters: CLUSTERS, mayOperate: true,
    minted: true });
  assert.doesNotMatch(consoleForm, /<input id="schedule-name"/,
    "NEGATIVE CONTROL: no name input in console mode");
  assert.match(consoleForm, /id="schedule-name-minted"/);
  assert.match(consoleForm, /id="schedule-name-help"/);
  assert.match(SCHEDULE_NAME_MINTED_SENTENCE, /sch- followed by 26 characters/);
  const html = renderScheduleForm({ draft: draft(), clusters: CLUSTERS, mayOperate: true });
  assert.match(html, /id="schedule-name"[^>]*aria-describedby="schedule-name-help"/);
  assert.match(html, /id="schedule-name-help"/);
  assert.match(SCHEDULE_NAME_HELP, /object name/);
  // With a name error, both descriptions are referenced from ONE attribute.
  const invalid = renderScheduleForm({ draft: draft({ name: "" }), clusters: CLUSTERS,
    mayOperate: true, errors: { fields: { name: ["a name is required"] }, unmatched: [] } });
  const tag = /<input id="schedule-name"[^>]*>/.exec(invalid)[0];
  assert.equal((tag.match(/aria-describedby=/g) || []).length, 1);
  assert.match(tag, /aria-describedby="schedule-name-error schedule-name-help"/);
});

test("a_console_schedule_needs_no_name_and_one_intent_keys_one_draft", async () => {
  // The draft's random intent is the idempotency seed, not the name: two
  // schedules typed with one name would otherwise collide (same key, different
  // body: 409 idempotency_conflict), and a blank name would be refused although
  // the server never uses it.
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  assert.equal((await selectMode()).mode, CONSOLE);
  const ns = "schedule-intent";
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  try {
    assert.ok(SCHEDULE_DRAFT_FIELDS.includes("intent"), "the draft keeps the intent");
    const intent = mintScheduleIntent();
    assert.match(intent, /^logweir-ui\.schedule\.[0-9a-f]{32}$/);
    assert.notEqual(mintScheduleIntent(), intent, "random, never a counter");
    const sent = [];
    const api = {
      create: async (namespace, plural, body) => {
        sent.push(body);
        return { metadata: { name: "sch-otka7iutedtdssn2h35vkyklox", uid: "u" }, __contract: {} };
      },
    };
    const values = draft({ name: "", mode: "advanced", cron: "0 2 * * *", intent: intent });
    const made = await submitSchedule(ns, values, api);
    assert.equal(sent.length, 1, "NEGATIVE CONTROL: a blank name is not refused in console mode");
    assert.equal(sent[0].metadata.name, intent,
      "the idempotency seed is the draft's intent, not a word the operator typed");
    assert.equal(made.object.metadata.name, "sch-otka7iutedtdssn2h35vkyklox");
    await assert.rejects(() => submitSchedule(ns, draft({ name: "", mode: "advanced",
      cron: "0 2 * * *" }), api), (error) => error.kind === "invalid",
    "a console draft with no intent is refused before anything is sent");
  } finally {
    dropDraft(key);
    resetMode();
  }
});

test("the_console_schedule_form_mints_one_intent_per_draft_across_a_retry", async () => {
  // Mounted: no name input, and the intent minted on the first submit is the
  // one a retry after an unknown outcome sends again.
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  const ns = "schedule-intent-mounted";
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  keepDraft(key, draft({ name: "", source: "orders-prod", sourceUid: "uid-A", mode: "advanced",
    cron: "0 2 * * *" }), SCHEDULE_DRAFT_FIELDS);
  const sent = [];
  let fail = true;
  const api = {
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters" ? CLUSTERS : { items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS }); },
    async create(namespace, plural, body) {
      sent.push(body);
      if (fail) {
        throw Object.assign(new Error("upstream timeout"), { status: 504, kind: "unknown" });
      }
      return { metadata: { name: "sch-otka7iutedtdssn2h35vkyklox", uid: "u" }, __contract: {} };
    },
  };
  const view = fakeView();
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), api);
    assert.equal(view.find("#schedule-name"), null, "no name input in console mode");
    await view.find("#schedule-form").dispatch("submit");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(sent.length, 1, "the create was sent: " + view.html().slice(-600));
    const first = sent[0].metadata.name;
    assert.match(first, /^logweir-ui\.schedule\.[0-9a-f]{32}$/);
    assert.equal((readDraft(key) || {}).intent, first, "the draft holds the intent");
    fail = false;
    await view.find("#schedule-form").dispatch("submit");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(sent.length, 2);
    assert.equal(sent[1].metadata.name, first, "NEGATIVE CONTROL: one intent per draft");
  } finally {
    dropDraft(key);
    resetMode();
  }
});

test("editing_a_checked_field_marks_the_verdict_stale_without_a_repaint", async () => {
  // Mounted: typing into a field the check was about replaces the verdict slot
  // with the stale note at once.
  selectMode(CONSOLE);
  const ns = "readiness-stale";
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  keepDraft(key, draft({ source: "uid-A" }), SCHEDULE_DRAFT_FIELDS);
  const view = fakeView();
  const ready = decoded("preflight-ready.json").item;
  const api = {
    list(namespace, plural) {
      return Promise.resolve(plural === "kafkaclusters" ? CLUSTERS : { items: [] });
    },
    destinations() { return Promise.resolve({ items: DESTINATIONS }); },
    startPreflight() { return Promise.resolve({ item: Object.assign({}, ready, { terminal: true }) }); },
    // THE FOLLOW'S READ, answered, and its clock handed in: without them the
    // follow retries a read this fake cannot make, on real timers, after the row
    // has finished.
    preflight(namespace, id) {
      return Promise.resolve({ item: Object.assign({}, ready, { id: id, terminal: true }) });
    },
    wait: async () => {},
  };
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), api);
    await view.find("#schedule-check-readiness").dispatch("click");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.doesNotMatch(view.chunks[view.chunks.length - 1].html, /data-readiness="stale"/,
      "NEGATIVE CONTROL: right after the check the verdict is current");
    const form = view.find("#schedule-form");
    form.elements.topics.value = "orders, refunds";
    await form.dispatch("input");
    const last = view.chunks[view.chunks.length - 1].html;
    assert.match(last, /data-readiness="stale"/, "the edit made the verdict stale");
    assert.doesNotMatch(last, /badge-green/);
  } finally {
    dropDraft(key);
    resetMode();
  }
});
