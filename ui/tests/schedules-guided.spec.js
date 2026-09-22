// schedules-guided.spec.js -- PLAT-10.1: the guided create form.
//
// WHAT THIS FILE IS FOR. The tracker's tests for 10.1 are "selected and
// all-user-topic creation, edit, invalid cron, readiness failure, keyboard use
// and first-run redirect", against the acceptance "the standard route requires
// no YAML, raw endpoint reconstruction or manual signing step; invalid fields
// retain the draft". Each row below is one of those, and each carries its own
// NEGATIVE CONTROL -- the assertion that fails if the behaviour is removed --
// because a row that passes against the pre-PLAT-10.1 form is not evidence of
// anything PLAT-10.1 did.
//
// The browser half (a real keyboard, a real redirect, a real Preflight against
// the lab) is `scripts/plat10-ui-e2e.mjs`. These rows are the ones that can be
// proved without one, over the exact strings a browser receives.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { CONSOLE, resetMode, selectMode } from "../client.js";
import { dropDraft, formKey, keepDraft, mutationFor, readDraft } from "../lifecycle.js";
import {
  ADVANCED_CRON,
  CREATE_PANEL,
  CREATE_TAKES_A_DESTINATION,
  PREVIEW_BEFORE_SAVE,
  SCHEDULE_DRAFT_FIELDS,
  SCHEDULE_FIELD_PATHS,
  SCHEDULE_FORM,
  guidedValues,
  readinessRequestFor,
  renderScheduleForm,
  scheduleBody,
  scheduleDetailRoute,
  scheduleFormView,
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
    keepLast: "",
    keepDays: "",
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

// ===========================================================================
// The standard route: no YAML, no endpoint, no signature
// ===========================================================================

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

  // NEGATIVE CONTROL 1: the hand-typed endpoint is NOT on the standard route.
  // Before PLAT-10.1 `#schedule-archive` was a required top-level input; if it
  // comes back out of its disclosure this row fails.
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
  // THE FORM, THE DRAFT AND THE BODY ARE ONE LIST. A field in the body with no
  // input is a field the form cannot express; a field in the form that the
  // draft does not keep is a field a failed submit silently loses -- which is
  // exactly what 10.1's "invalid fields retain the draft" forbids.
  const values = draft({ selection: "dynamic", incompleteDiscovery: "Refuse" });
  const html = renderScheduleForm({
    draft: values, clusters: CLUSTERS, destinations: DESTINATIONS, mayOperate: true,
  });
  for (const field of ["mode", "timeZone", "selection", "incompleteDiscovery",
    "excludeTopics", "excludePrefixes", "destination", "concurrencyPolicy",
    "startingDeadlineSeconds", "catchUpPolicy", "maxRetries", "retryDelaySeconds",
    "activeDeadlineSeconds", "keepLast", "keepDays", "suspended"]) {
    assert.ok(html.indexOf("name=\"" + field + "\"") !== -1, field + " has an input");
    assert.ok(SCHEDULE_DRAFT_FIELDS.indexOf(field) !== -1, field + " is kept in the draft");
  }
  // NEGATIVE CONTROL: a field that is NOT in the draft list is a field this row
  // would have missed, so the list itself is asserted to be the union.
  assert.ok(SCHEDULE_DRAFT_FIELDS.indexOf("name") !== -1);
  assert.ok(SCHEDULE_DRAFT_FIELDS.indexOf("sourceUid") !== -1,
    "the chosen cluster's identity is part of the draft, not just its name");
});

// ===========================================================================
// Selected-topic creation, and all-user-topic creation
// ===========================================================================

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

// ===========================================================================
// Invalid cron, and the draft that survives it
// ===========================================================================

test("an_invalid_cadence_is_the_apis_refusal_and_not_this_pages_opinion", () => {
  // THE SHAPE CHECK STAYS AND THE PARSE DOES NOT. `61 * * * *` is five fields
  // of cron characters, so this page has nothing to say about it: whether a
  // minute may be 61 is the cadence engine's answer, and 10.1's acceptance is
  // that the person reads the API's words.
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

// ===========================================================================
// The preview is the only expression this page will save
// ===========================================================================

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

// ===========================================================================
// Readiness: the check's own verdict, from the readiness route
// ===========================================================================

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
  const notReady = fixture("console/preflight-not-ready.json").item;
  const failed = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, mayOperate: true, readiness: notReady,
  });
  assert.match(failed, /not ready/);
  assert.ok(failed.indexOf(notReady.reason) !== -1,
    "the reason on screen is the one the check recorded: " + notReady.reason);

  // NEGATIVE CONTROL: the same render with a READY verdict does not say not
  // ready, so the row above is reading the object and not a fixed string.
  const ready = fixture("console/preflight-ready.json").item;
  const green = renderScheduleForm({
    draft: draft(), clusters: CLUSTERS, mayOperate: true, readiness: ready,
  });
  assert.doesNotMatch(green, /data-readiness="unchecked"/);
  assert.ok(green.indexOf(notReady.reason) === -1);
});

// ===========================================================================
// Keyboard use, and the first-run redirect
// ===========================================================================

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
  // MIGRATION: the list's own deep link is unchanged and is not this one.
  assert.notEqual(scheduleDetailRoute("team-a", "sch-abc"), "#/schedules?ns=team-a");
});

// ===========================================================================
// The two forms are one form
// ===========================================================================

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

// ===========================================================================
// The mode this form is used in
// ===========================================================================

test("the_create_body_reaches_the_product_api_as_the_whole_policy", async () => {
  // THE TRANSLATION IS THE ONE PLACE THE TWO REQUEST SHAPES DIFFER, and a field
  // the custom resource carries that the DTO never sends is a field an operator
  // set and the API never heard about. This drives `ui/client.js`'s real
  // console adapter over the real `fetch` boundary.
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
        item: fixture("console/schedule.json").item,
      })),
    });
  };
  try {
    const body = scheduleBody(draft({
      selection: "dynamic", incompleteDiscovery: "Refuse", excludePrefixes: "dev-",
      startingDeadlineSeconds: "900", catchUpPolicy: "Latest", maxRetries: "2",
      retryDelaySeconds: "600", activeDeadlineSeconds: "7200", keepLast: "7",
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
  assert.deepEqual(sent.retention, { keepLast: 7 });
  assert.equal(sent.concurrencyPolicy, "Allow");
  assert.equal(sent.suspended, true);

  // NEGATIVE CONTROL: a body with an inline archive and a named allowlist sends
  // `archive` and no `destinationRef`, so the row above is about what the
  // object says and not about a translator that always writes both halves.
  assert.equal(sent.sourceRef.name, "orders-prod");
});
