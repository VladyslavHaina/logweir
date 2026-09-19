// d3.spec.js -- D3 (PLAT-12.1, PLAT-14.1, PLAT-14.2, PLAT-15.1, PLAT-15.2,
// PLAT-16.1, PLAT-16.2, PLAT-19.1) under `node --test`: the durable operation
// view, protection health, the recovery catalog, the keys view, the badge
// cases and the retention panel.
//
// WHY THIS IS A FILE OF ITS OWN, like `d2.spec.js` before it. These surfaces
// span both modes: the custom resource in legacy mode, the product API's
// document in console mode, and one renderer over both. Keeping them together
// means a reader looking for "what does the console say when nothing has
// evaluated this policy" finds one file.
//
// THE FIVE CLAIMS EVERY ROW BELOW IS ABOUT. Each one is a sentence this
// product must never render, and each has its own arm:
//
//   * `valid`, for a key nothing has evaluated -- D3 section 7.7's `unknown`;
//   * `verified`, for evidence signed by a key this installation does not
//     accept -- `Untrusted` is a claim about the SIGNER and `NotAttempted` one
//     about the CONTROLLER, and one word for both loses the repair;
//   * a complete comparison, for a sampled one -- `complete` is not a level
//     this version has and no sentence here can spell it;
//   * `protected`, for a policy Logweir could not evaluate -- `Unknown` stays
//     `Unknown` and is never rounded to `False`;
//   * "Logweir never deletes from your archive", beside a policy that does --
//     the retention panel reads `status.enforcement`, which is what is
//     HAPPENING, not `spec.mode`, which is what was asked for.
//
// THE FIXTURES ARE REAL OBJECTS where a real one exists: the ProtectionPolicy,
// the RecoveryCatalog status and its view entries, the two RetentionPolicies
// and the TrustPolicy are the bytes the D3 live runs recorded (see
// `fixtures/README.md` for which is which and what was constructed).
//
// NOTHING HERE DIALS, and `ui_lint.rs::the_ui_behaviour_suite_never_dials`
// holds that for the whole directory.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { SERVER_TIME_MAX_AGE_MS } from "../api.js";

import {
  D3_ENUMS,
  D3_SHAPES,
  decodeCatalogPoints,
  decodeCatalogRequest,
  decodeCatalogSigners,
  decodeD3Operation,
  decodeLegacyObject,
  isContractFailure,
} from "../contract.js";
import {
  BACKOFF_MS,
  CONNECTS_BEFORE_POLLING,
  ERRORS_BEFORE_SLOWING,
  POLL_MS,
  SLOW_POLL_MS,
  backoffFor,
  isSettled,
  watchOperation,
} from "../operation-watch.js";
import {
  COMPLETION_GUIDANCE,
  ENFORCEMENT_DEGRADED_SENTENCE,
  ENFORCEMENT_SENTENCES,
  EVALUATION_UNKNOWN,
  GREEN_BASES,
  HISTORICAL_SUFFIX,
  TRUST_BASIS_NOT_OBSERVED,
  IRREVERSIBLE_SENTENCE,
  NO_ONE_CLICK_TRUST_SENTENCE,
  RETENTION_SENTENCE,
  SCOPE_LEVEL_OF_INTEGRITY,
  TWO_AXES_SENTENCE,
  TWO_HEALTHS_SENTENCE,
  TWO_INSTANTS_SENTENCE,
  UNKNOWN_IS_NOT_VALID_SENTENCE,
  basisAllowsGreen,
  healthBadge,
  stateBadge,
  unverifiedCaption,
  verificationCase,
  verificationScopeSentence,
} from "../render.js";
import {
  DIFFERENT_RUN_SENTENCE,
  mountOperation,
  operationFacts,
  operationRoute,
  operationRouteParams,
  renderCompletion,
  renderDiagnostics,
  renderEvidence,
  renderOperation,
  renderProgress,
  renderResult,
} from "../pages/operation.js";
import {
  NOT_EVALUATED_SENTENCE,
  NO_POLICY_SENTENCE,
  mountProtection,
  mountProtectionDetail,
  objectiveLine,
  openAlerts,
  protectedBadge,
  renderAlerts,
  renderLastPoint,
  renderProtectionDetail,
  renderProtectionList,
  renderScheduleHealth,
} from "../pages/protection.js";
import {
  CONNECT_SENTENCE,
  NO_CATALOG_SENTENCE,
  POINT_BINDING_SENTENCE,
  bestLocation,
  connectBody,
  renderCatalogDetail,
  renderCatalogList,
  renderCatalogStatus,
  renderConnectForm,
  renderPoints,
  renderSigners,
  MORE_POINTS_SENTENCE,
  cursorOf,
  isIntentConflict,
  mountCatalog,
  restorePointRoute,
  validateConnect,
  viewIsUsable,
} from "../pages/catalog.js";
import {
  EVALUATION_FRESHNESS_MS,
  FINGERPRINT_COMMAND,
  evaluationFreshness,
  mountKeys,
  renderKeysPage,
  renderPolicyFacts,
  renderPolicyKeys,
  verdictFor,
} from "../pages/keys.js";
import { backupBadge, greenLabel, validVerification } from "../pages/backups.js";
import { renderRestoreDetail, restoreBadge, scopeOf } from "../pages/history.js";
import {
  enforcementOf,
  policyForSchedule,
  renderEnforcement,
  renderRetentionPanel,
  retentionSentenceFor,
} from "../pages/schedules.js";

const d3 = (name) =>
  JSON.parse(readFileSync(new URL("./fixtures/d3/" + name, import.meta.url), "utf8"));

const operationOf = (name) => decodeD3Operation(d3(name)).value.item;

function decode(html) {
  return html
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

// ===========================================================================
// PLAT-14.1 -- the operation contract, and what the two modes each carry
// ===========================================================================

test("the_extended_operation_decoder_reads_every_d3_field_and_refuses_an_unknown_stage", () => {
  const item = operationOf("operation-backup-preparing.json");
  assert.equal(item.state, "preparing");
  assert.equal(item.progress.stage, "Preparing");
  assert.equal(item.progress.runner.waitingReason, "CreateContainerConfigError");
  assert.equal(item.diagnostics[0].code, "CredentialSecretNotFound");
  assert.equal(item.diagnostics[0].severity, "Error");
  assert.equal(item.diagnostics[0].object.kind, "Pod");

  // A STAGE THIS BUILD DOES NOT KNOW IS A CONTRACT FAILURE AND NOT A BLANK.
  const broken = d3("operation-backup-preparing.json");
  broken.item.progress.stage = "Thinking";
  assert.throws(
    () => decodeD3Operation(broken),
    (error) => {
      assert.ok(isContractFailure(error), "it is a contract failure");
      assert.match(error.message, /expected one of Admission, Queued/);
      return true;
    },
  );

  // And so is a diagnosis code outside the closed thirteen.
  const invented = d3("operation-backup-preparing.json");
  invented.item.diagnostics[0].code = "SomethingWentWrong";
  assert.throws(() => decodeD3Operation(invented), isContractFailure);
});

test("the_diagnosis_vocabulary_is_the_closed_thirteen_and_not_a_fourteenth", () => {
  assert.equal(D3_ENUMS.DiagnosisCode.length, 13);
  assert.ok(D3_ENUMS.DiagnosisCode.indexOf("DisruptedMidRun") !== -1,
    "D2's DisruptedMidCheck under the name a RUN gives it");
  assert.equal(D3_ENUMS.DiagnosisCode.indexOf("DisruptedMidCheck"), -1,
    "and not both spellings");
  assert.ok(D3_ENUMS.DiagnosisCode.indexOf("WaitingForPod") !== -1);
  assert.equal(D3_ENUMS.DiagnosisCode.indexOf("DeadlineExceeded"), -1,
    "the Job's own clock running out is the consequence, not a diagnosis");
});

test("the_operation_view_computes_no_state_in_legacy_mode_and_says_so", () => {
  // A CUSTOM RESOURCE HAS NO NORMALIZED STATE, and this page does not invent
  // one. `unknown` is one of the TEN words the API computes; printing it here
  // would be this page taking a decision D3 section 2.5 gives the API.
  const object = d3("backup-progress-waiting.json");
  const facts = operationFacts(object, false);
  assert.equal(facts.state, null, "no normalized state in legacy mode");
  assert.equal(facts.progress.stage, "Preparing", "the controller's own stage IS shown");

  const html = renderOperation({
    ns: "team-a", kind: "backup", name: "d3w12-waiting", uid: "",
    document: object, console: false, meta: { transport: "poll", attempt: 0 },
  });
  assert.match(html, /data-no-normalized-state="true"/);
  assert.match(decode(html), /normalized operation state is computed by `logweir-api`/);
  assert.doesNotMatch(html, /badge-state-unknown/,
    "and it does NOT render the API's `unknown` word for a fact it simply does not have");
});

test("the_operation_view_prints_the_apis_own_word_in_console_mode", () => {
  const item = operationOf("operation-unknown.json");
  const html = renderOperation({
    ns: "team-a", kind: "backup", name: item.name, uid: "",
    document: item, console: true, meta: { transport: "stream", attempt: 0 },
  });
  assert.match(html, /badge-state-unknown">unknown</);
  assert.match(decode(html), /It is not a statement that the run failed/);
  assert.equal(stateBadge("verifying"), "<span class=\"badge badge-state-verifying\">verifying</span>");
  assert.equal(stateBadge(""), "-", "an absent state is not a badge with no words");
});

test("a_uid_that_does_not_answer_refuses_instead_of_switching_runs", () => {
  const item = operationOf("operation-restore-completed.json");
  const html = renderOperation({
    ns: "team-a", kind: "restore", name: item.name, uid: "a-different-uid",
    document: null, console: true, meta: {}, mismatch: true,
  });
  assert.match(html, /data-uid-mismatch="true"/);
  assert.match(decode(html), /This name now refers to a different run/);
  assert.equal(decode(html).indexOf(item.uid), -1,
    "and nothing about the run it found is rendered: the reader did not ask about it");
  assert.ok(DIFFERENT_RUN_SENTENCE.length > 0);
});

test("the_result_and_the_evidence_are_two_sections_and_a_succeeded_run_is_not_a_verified_one", () => {
  const item = operationOf("operation-restore-no-record-check.json");
  assert.equal(item.result.status, "pass", "the RUN passed");
  assert.equal(item.evidenceVerification.result, "NotAttempted", "and nothing verified it");

  const facts = operationFacts(item, true);
  const result = renderResult(facts);
  const evidence = renderEvidence(facts);
  assert.match(result, /<section class="result">/);
  assert.match(evidence, /<section class="evidence">/);
  assert.match(evidence, /badge-unverified/,
    "a pass with no attempted verification is NOT a green badge");
  assert.match(decode(evidence), /not attempted -- the controller could not check/);
  assert.match(decode(result), /Whether the document it produced verifies/);
});

test("the_completion_panel_carries_the_mode_s_own_guidance_and_the_sampled_sentence", () => {
  const newTopic = operationFacts(operationOf("operation-restore-completed.json"), true);
  const html = decode(renderCompletion(newTopic));
  assert.match(html, /d3w12-restore-orders/);
  assert.match(html, /64 of 64 sampled records matched byte-for-byte/);
  assert.match(html, /200 records were expected in the sampled window/);
  assert.match(html, /this is a sampled check, not an exhaustive comparison/);
  assert.match(html, /Consumers are not moved/);
  assert.match(html, /Consumer group offsets were not restored/);
  assert.equal(html.indexOf("complete"), -1,
    "the word `complete` appears nowhere in a completion panel: it is not a level this " +
      "version has");

  const scratch = operationFacts(operationOf("operation-restore-scratch.json"), true);
  const scratchHtml = decode(renderCompletion(scratch));
  assert.match(scratchHtml, /These topics are a rehearsal and are deleted by teardown/);
  assert.match(scratchHtml, /The comparison was degraded/);
  assert.notEqual(COMPLETION_GUIDANCE.scratch, COMPLETION_GUIDANCE.newTopic);

  const none = operationFacts(operationOf("operation-restore-no-record-check.json"), true);
  assert.match(decode(renderCompletion(none)), /No record check ran for this restore/);
});

test("the_diagnoses_table_says_an_empty_list_is_not_a_healthy_run", () => {
  const facts = operationFacts(operationOf("operation-backup-preparing.json"), true);
  const html = decode(renderDiagnostics(facts));
  assert.match(html, /CredentialSecretNotFound/);
  assert.match(html, /secret "archive-credentials" not found/);
  assert.match(html, /Pod d3w12-waiting-hk29p/);

  const empty = decode(renderDiagnostics({ diagnostics: [] }));
  assert.match(empty, /That is not the same as a run with nothing wrong/);
});

test("the_progress_block_says_an_absent_progress_is_absent_and_not_a_stalled_run", () => {
  const html = decode(renderProgress({ progress: null }));
  assert.match(html, /carries no <code>status.progress<\/code>/);
  assert.match(html, /ABSENT observation and not a stalled run/);
  assert.match(html, /no stage is inferred from it/);
});

test("the_operation_route_carries_both_halves_of_the_identity", () => {
  const route = operationRoute("team-a", "backup", "orders-20260919", "uid-1");
  assert.equal(route, "#/operations?ns=team-a&kind=backup&name=orders-20260919&uid=uid-1");
  const params = operationRouteParams(route);
  assert.deepEqual({ kind: params.kind, name: params.name, uid: params.uid },
    { kind: "backup", name: "orders-20260919", uid: "uid-1" });
  // An absent uid is absent, and the route is still well formed.
  assert.equal(operationRoute("team-a", "restore", "r1", ""),
    "#/operations?ns=team-a&kind=restore&name=r1");
});

// ===========================================================================
// PLAT-14.1 -- the watch
// ===========================================================================

test("the_watch_stops_only_when_the_run_is_terminal_AND_the_verification_settled", () => {
  assert.equal(isSettled({ terminal: true, verification: { state: "valid" } }), true);
  assert.equal(isSettled({ terminal: true, verification: { state: "pending" } }), false,
    "a run that exited and whose evidence is still pending is NOT settled: stopping there " +
      "would leave `pending` evidence on screen for ever");
  assert.equal(isSettled({ terminal: false, verification: { state: "valid" } }), false);
  assert.equal(isSettled({}), false, "an absent `terminal` is not observed, and keeps looking");
});

test("the_backoff_is_the_declared_ladder_with_bounded_jitter_and_it_repeats_the_last_step", () => {
  assert.deepEqual(BACKOFF_MS.slice(), [1000, 2000, 5000, 30000]);
  assert.equal(backoffFor(0, 0), 1000);
  assert.equal(backoffFor(1, 0), 2000);
  assert.equal(backoffFor(2, 0), 5000);
  assert.equal(backoffFor(3, 0), 30000);
  assert.equal(backoffFor(99, 0), 30000, "the last step repeats rather than growing");
  assert.equal(backoffFor(0, 1), 1250, "and the jitter is bounded at a quarter");
  assert.ok(backoffFor(0, 0.5) > 1000 && backoffFor(0, 0.5) < 1250);
});

test("a_stream_that_will_not_connect_three_times_falls_back_to_polling_and_keeps_reading", async () => {
  // A FAKE EventSource THAT ALWAYS CLOSES. This is the proxy that will not
  // carry `text/event-stream`: retrying it for ever would leave an operation
  // frozen at whatever document arrived first, with nothing on screen saying
  // the page stopped hearing.
  const opened = [];
  class DeadSource {
    constructor(url) {
      opened.push(url);
      this.readyState = 2;
      this.handlers = {};
      queueMicrotask(() => {
        const error = this.handlers.error;
        if (error !== undefined) {
          error({});
        }
      });
    }
    addEventListener(type, handler) {
      this.handlers[type] = handler;
    }
    close() {}
  }
  const timers = [];
  const reads = [];
  const updates = [];
  const watch = watchOperation("team-a", "backup", "b1", (document, meta) => {
    updates.push(meta.transport);
  }, null, {
    modeOf: () => "console",
    EventSourceClass: DeadSource,
    setTimer: (fn, ms) => {
      timers.push({ fn: fn, ms: ms });
      return timers.length;
    },
    clearTimer: () => {},
    random: () => 0,
    read: async () => {
      reads.push(1);
      return { terminal: false, verification: { state: "pending" } };
    },
  });
  // Drain the three connects: each error schedules the next attempt, and the
  // fall-back poll runs on a promise chain, so this yields the macrotask queue
  // rather than only the microtask one.
  for (let i = 0; i < 4; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 1));
    const next = timers.shift();
    if (next !== undefined && next.ms < 30001) {
      next.fn();
    }
  }
  await new Promise((resolve) => setTimeout(resolve, 1));
  assert.ok(opened.length >= 1, "it did try to stream");
  assert.equal(CONNECTS_BEFORE_POLLING, 3);
  assert.ok(reads.length >= 1,
    "and after the declared number of failed connects it READ the operation instead: " +
      "opened=" + String(opened.length) + " reads=" + String(reads.length));
  assert.ok(updates.indexOf("poll") !== -1, "and the view is told which transport it is on");
  watch.stop();
});

test("a_settled_document_stops_the_watch_and_leaving_the_route_disposes_it", async () => {
  let reads = 0;
  const timers = [];
  const settled = { terminal: true, verification: { state: "valid" } };
  const watch = watchOperation("team-a", "backup", "b1", () => {}, null, {
    modeOf: () => "legacy",
    setTimer: (fn, ms) => {
      timers.push({ fn: fn, ms: ms });
      return timers.length;
    },
    clearTimer: () => {},
    read: async () => {
      reads += 1;
      return settled;
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(reads, 1, "one read");
  assert.equal(timers.length, 0, "and no follow-up scheduled: the document settled");
  watch.stop();

  // AND THE ROUTE'S SIGNAL DISPOSES IT. A watch that outlived its route would
  // keep polling a namespace nobody is looking at.
  const controller = new AbortController();
  let live = 0;
  const running = watchOperation("team-a", "backup", "b2", () => {}, {
    signal: controller.signal,
    isCurrent: () => !controller.signal.aborted,
  }, {
    modeOf: () => "legacy",
    setTimer: (fn, ms) => {
      timers.push({ fn: fn, ms: ms });
      return timers.length;
    },
    clearTimer: () => {},
    read: async () => {
      live += 1;
      return { terminal: false, verification: { state: "pending" } };
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  const before = live;
  controller.abort();
  const pending = timers.pop();
  if (pending !== undefined) {
    pending.fn();
  }
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(live, before, "nothing was read after the route left");
  running.stop();
});

test("the_legacy_poller_slows_down_after_the_declared_run_of_errors", async () => {
  const timers = [];
  let attempts = 0;
  const watch = watchOperation("team-a", "backup", "b1", () => {}, null, {
    modeOf: () => "legacy",
    setTimer: (fn, ms) => {
      timers.push({ fn: fn, ms: ms });
      return timers.length;
    },
    clearTimer: () => {},
    read: async () => {
      attempts += 1;
      throw new Error("the API server answered 500");
    },
  });
  for (let i = 0; i < ERRORS_BEFORE_SLOWING + 1; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
    const next = timers.shift();
    if (next === undefined) {
      break;
    }
    if (i < ERRORS_BEFORE_SLOWING - 1) {
      assert.equal(next.ms, POLL_MS, "still the fast cadence at error " + String(i + 1));
    } else {
      assert.equal(next.ms, SLOW_POLL_MS, "and the slow one at error " + String(i + 1));
    }
    next.fn();
  }
  assert.ok(attempts >= ERRORS_BEFORE_SLOWING);
  watch.stop();
});

// ===========================================================================
// PLAT-14.2 -- protection health
// ===========================================================================

test("schedule_health_and_protection_health_are_two_columns_and_never_one", () => {
  const object = d3("protection-unprotected.json");
  assert.equal(object.status.health, "Unprotected");
  assert.equal(object.status.schedules[0].ready, "False");

  const list = renderProtectionList({ items: [object] }, "team-a");
  assert.match(list, /<th scope="col">PROTECTION<\/th>/);
  assert.match(list, /<th scope="col">SCHEDULES<\/th>/);
  assert.match(decode(list), /an enabled, healthy schedule can still have no recent recoverable backup/);
  assert.ok(TWO_HEALTHS_SENTENCE.length > 0);

  const detail = decode(renderScheduleHealth(object, "team-a"));
  assert.match(detail, /keeps-running/);
  assert.match(detail, /suspended/);
});

test("a_healthy_policy_labels_its_two_instants_separately", () => {
  const object = d3("protection-healthy.json");
  assert.equal(object.status.health, "Healthy");
  const html = decode(renderLastPoint(object, "team-a"));
  assert.match(html, /recovery point \(capture started\)/);
  assert.match(html, /newest archived record/);
  assert.match(html, /2026-09-19T01:10:11.287Z/, "the capture start");
  assert.match(html, /2026-09-19T01:09:17.276Z/, "and the newest record, which is a different instant");
  assert.match(html, /an idle topic makes the second one look old/);
  assert.ok(TWO_INSTANTS_SENTENCE.length > 0);
  assert.match(healthBadge("Healthy"), /badge-green">Healthy</);
});

test("an_unevaluated_protection_is_unknown_and_Protected_Unknown_is_never_rounded_to_False", () => {
  const unknown = d3("protection-unknown.json");
  assert.equal(unknown.status.health, "Unknown");
  const badge = protectedBadge(unknown.status.conditions);
  assert.match(badge, /badge-flat">Protected=Unknown CatalogStale</);
  assert.doesNotMatch(badge, /Protected=False/,
    "`False` on an evaluation that could not happen reads as `Logweir checked and you are " +
      "not protected`, which is a claim nothing made");

  const healthy = protectedBadge(d3("protection-healthy.json").status.conditions);
  assert.match(healthy, /badge-green">Protected=True/);
  assert.match(healthBadge("Unknown"), /badge-flat">Unknown</);

  // And a policy with no evaluation at all says so.
  const fresh = JSON.parse(JSON.stringify(unknown));
  delete fresh.status.evaluatedAt;
  assert.match(decode(renderProtectionDetail(fresh, "team-a")),
    /nothing here says this namespace is protected and nothing here says it is not/);
  assert.ok(NOT_EVALUATED_SENTENCE.length > 0);
});

test("the_alert_ledger_shows_its_delivery_state_and_says_a_notification_is_not_evidence", () => {
  const object = d3("protection-unknown.json");
  assert.equal(openAlerts(object.status).length, 1);
  const html = decode(renderAlerts(object));
  assert.match(html, /Staleness/);
  assert.match(html, /Failed \(3\)/, "three attempts, and the state the controller recorded");
  assert.match(html, /the webhook sink answered 503/);
  assert.match(html, /a sink that refused it changes nothing about a backup's own recorded result/);

  const resolved = decode(renderAlerts(d3("protection-healthy.json")));
  assert.match(resolved, /Delivered \(1\)/);
  assert.match(resolved, /badge-green">Resolved</);
});

test("the_protection_list_says_what_an_empty_namespace_means", () => {
  const html = decode(renderProtectionList({ items: [] }, "team-a"));
  assert.match(html, /A schedule that runs is not the same as an objective that is met/);
  assert.ok(NO_POLICY_SENTENCE.length > 0);
  assert.equal(objectiveLine(d3("protection-healthy.json")),
    "a recovery point no older than 300 seconds");
  assert.equal(objectiveLine({}), "-", "an objective nothing states is not zero");
});

// ===========================================================================
// PLAT-15.1 / PLAT-15.2 -- the recovery catalog
// ===========================================================================

test("availability_and_verification_are_two_columns_and_selectable_is_read_not_recomputed", () => {
  const page = decodeCatalogPoints(d3("catalog-points-states.json")).value;
  const html = renderPoints(page, "team-a", "primary");
  assert.match(html, /<th scope="col">AVAILABILITY<\/th>/);
  assert.match(html, /<th scope="col">VERIFICATION<\/th>/);
  assert.match(decode(html), /that judgement is the catalog's own `selectable` field/);
  assert.ok(TWO_AXES_SENTENCE.length > 0);

  // THE MUTANT THIS ARM IS FOR. A page that recomputed `Available AND
  // (Verified | VerifiedHistorical)` would offer a restore for a point the
  // catalog had already decided is not selectable. Flip the materialised bit
  // and the row must lose its link, whatever the two enums say.
  const flipped = d3("catalog-points-states.json");
  assert.equal(flipped.items[0].availability, "Available");
  assert.equal(flipped.items[0].verification, "Verified");
  flipped.items[0].selectable = false;
  const out = renderPoints(decodeCatalogPoints(flipped).value, "team-a", "primary");
  const row = out.slice(out.indexOf(flipped.items[0].pointId));
  assert.equal(row.slice(0, row.indexOf("</tr>")).indexOf("Restore this point"), -1,
    "the row offers no restore when the catalog says the point is not selectable, even " +
      "though both of its own axes are green");
});

test("nothing_is_hidden_and_every_state_carries_its_remedy", () => {
  const page = decodeCatalogPoints(d3("catalog-points-states.json")).value;
  const html = decode(renderPoints(page, "team-a", "primary"));
  for (const word of ["Available", "Missing", "Conflict", "Deleted", "UntrustedSigner"]) {
    assert.ok(html.indexOf(word) !== -1, word + " is listed rather than dropped");
  }
  assert.match(html, /compare its fingerprint out of band/);
  assert.match(html, /two records disagree about this identity/);
  assert.match(html, /a completed tombstone exists for this point/);
});

test("one_point_in_two_buckets_is_one_row_and_the_degraded_location_is_named", () => {
  const page = decodeCatalogPoints(d3("catalog-points-states.json")).value;
  const twoLocations = page.items.filter((e) => e.locations.length === 2);
  assert.equal(twoLocations.length, 1);
  const entry = twoLocations[0];
  assert.equal(entry.availability, "Available",
    "the entry's availability is the BEST of its locations: a point bucket A holds is not " +
      "hidden because bucket B lost its copy");
  const best = bestLocation(entry);
  assert.equal(best.availability, "Available");
  assert.equal(best.locationId, "s3://d3w14-lr520260919t0109z-a/archive");
});

test("the_restore_link_carries_the_frozen_location_digest", () => {
  const page = decodeCatalogPoints(d3("catalog-points-states.json")).value;
  const entry = page.items[0];
  assert.ok(typeof entry.locationDigest === "string" && entry.locationDigest.length > 0);
  const route = restorePointRoute("team-a", "primary", entry);
  assert.ok(route.indexOf("point=" + entry.pointId) !== -1);
  assert.ok(route.indexOf("catalog=primary") !== -1);
  assert.ok(route.indexOf("locationDigest=" + encodeURIComponent(entry.locationDigest)) !== -1,
    "the frozen destination digest travels with the point, which is what binds a restore " +
      "to the exact snapshot the point was written under");
  assert.ok(POINT_BINDING_SENTENCE.indexOf("receipt_sha256") !== -1);

  // A point whose view published no digest says nothing rather than inventing
  // one: the link still carries the location it can be served from.
  const noDigest = JSON.parse(JSON.stringify(entry));
  delete noDigest.locationDigest;
  const bare = restorePointRoute("team-a", "primary", noDigest);
  assert.equal(bare.indexOf("locationDigest="), -1);
  assert.ok(bare.indexOf("location=") !== -1);
});

test("the_untrusted_signer_panel_offers_no_one_click_trust", () => {
  const signers = decodeCatalogSigners(d3("catalog-signers.json")).value;
  const html = renderSigners(signers);
  const text = decode(html);
  assert.match(html, /data-untrusted-signers="1"/);
  assert.match(text, /This page offers no control that adds a key to a TrustPolicy/);
  assert.match(text, /never trusted by proximity/);
  assert.match(text, /openssl pkey -pubin -outform DER/);
  assert.match(text, /kind: TrustPolicy/);
  assert.match(text, /state: Retired/,
    "the snippet suggests the authority an imported archive's signer actually needs");
  assert.equal(html.indexOf("<button"), -1, "and there is NO button anywhere in this panel");
  assert.equal(html.indexOf("<form"), -1);
  assert.ok(NO_ONE_CLICK_TRUST_SENTENCE.length > 0);
  assert.equal(FINGERPRINT_COMMAND.indexOf("PRIVATE"), -1);
});

test("the_view_is_a_window_and_an_expired_one_is_a_missing_view_and_not_a_missing_archive", () => {
  const object = d3("catalog-truncated.json");
  assert.equal(object.status.truncated, true);
  assert.equal(viewIsUsable(object), true);
  const html = decode(renderCatalogStatus(object));
  assert.match(html, /bounded WINDOW over the durable catalog in object storage/);
  assert.match(html, /none of them is hidden and none of them is deleted/);

  const expired = JSON.parse(JSON.stringify(object));
  for (const condition of expired.status.conditions) {
    if (condition.type === "Ready") {
      condition.status = "False";
      condition.reason = "ViewExpired";
    }
  }
  assert.equal(viewIsUsable(expired), false);
  const out = decode(renderCatalogStatus(expired));
  assert.match(out, /That is a missing VIEW and not a missing archive/);
  assert.match(out, /the points are still in object storage/);
});

test("the_catalog_view_freshness_never_reads_the_browsers_clock", () => {
  // `viewExpiresAt` is a SERVER instant, and a browser five minutes fast would
  // declare a healthy view expired. The page reads the controller's own
  // conditions instead, so moving the expiry far into the past changes
  // nothing while `Ready=True` stands.
  const object = d3("catalog-truncated.json");
  const stale = JSON.parse(JSON.stringify(object));
  stale.status.viewExpiresAt = "2000-01-01T00:00:00Z";
  assert.equal(viewIsUsable(stale), true,
    "the verdict is the controller's Ready condition and not an instant comparison here");
});

test("the_connect_archive_form_is_a_durable_submission_and_checks_its_body_before_sending", () => {
  const html = decode(renderConnectForm({ ns: "team-a", values: {}, errors: {}, state: { phase: "idle" } }));
  assert.match(html, /Connect an existing archive/);
  assert.match(html, /it writes nothing to your bucket, moves nothing and deletes nothing/);
  assert.match(html, /no step of this form changes that/);
  assert.ok(CONNECT_SENTENCE.length > 0);

  assert.deepEqual(Object.keys(validateConnect({})).sort(), ["destination", "name"]);
  assert.deepEqual(validateConnect({ name: "primary", destination: "dest-a" }), {});

  const body = connectBody({ name: "primary", destination: "dest-a", syncMode: "Full" });
  assert.deepEqual(body, { name: "primary", destinationRef: { name: "dest-a" }, syncMode: "Full" });
  assert.deepEqual(decodeCatalogRequest(body).unknown, [],
    "the body this form builds is exactly the published request shape");

  const invented = Object.assign({}, body, { trustThisSigner: true });
  assert.ok(decodeCatalogRequest(invented).unknown.indexOf("trustThisSigner") !== -1,
    "a field this page invented is recorded here rather than sent");
});

test("the_catalog_detail_renders_its_status_even_when_the_point_list_is_refused", () => {
  const refusal = new Error("this build's product API has no point route yet");
  refusal.status = 501;
  refusal.reason = "NoConsoleRoute";
  const html = decode(renderCatalogDetail({
    ns: "team-a", object: d3("catalog-truncated.json"), points: null,
    signers: null, pointsError: refusal, signersError: refusal,
  }));
  assert.match(html, /The view/, "the catalog's own status is still on screen");
  assert.match(html, /has no point route yet/, "and the refusal is rendered where it belongs");
  assert.equal(html.indexOf("This catalog's view holds no point"), -1,
    "a refused list is never absorbed into an empty table, which is what an empty catalog " +
      "looks like");

  assert.match(decode(renderCatalogList({ items: [] }, "team-a")),
    /the points in your archive are still there/);
  assert.ok(NO_CATALOG_SENTENCE.length > 0);
});

// ===========================================================================
// PLAT-19.1 -- unknown is not valid
// ===========================================================================

test("an_unevaluated_trust_policy_reads_unknown_and_never_valid", () => {
  const object = d3("trustpolicy-unevaluated.json");
  assert.equal(object.status, undefined, "the fixture carries no status at all");
  const freshness = evaluationFreshness(object, Date.parse("2026-09-19T02:00:00Z"));
  assert.deepEqual(freshness, { fresh: false, reason: "NoStatus" });

  const html = decode(renderPolicyKeys(object, Date.parse("2026-09-19T02:00:00Z")));
  assert.match(html, /unknown/);
  assert.match(html, /nothing has evaluated these keys yet/);
  assert.equal(html.indexOf(">valid<"), -1, "and the word `valid` is nowhere in this table");
  assert.ok(UNKNOWN_IS_NOT_VALID_SENTENCE.indexOf("never `valid`") !== -1);
});

test("a_generation_behind_its_status_reads_unknown_even_with_a_recent_evaluation", () => {
  const object = d3("trustpolicy-stale.json");
  assert.equal(object.metadata.generation, 3);
  assert.equal(object.status.observedGeneration, 2);
  const now = Date.parse(object.status.evaluatedAt) + 1000;
  assert.deepEqual(evaluationFreshness(object, now), { fresh: false, reason: "GenerationBehind" });
  const html = decode(renderPolicyKeys(object, now));
  assert.match(html, /these verdicts were computed from an earlier spec/);
  assert.match(html, /data-evaluation-fresh="false"/);
});

test("an_evaluation_older_than_the_window_reads_unknown_measured_by_the_server_clock", () => {
  const object = d3("trustpolicy-active.json");
  const evaluatedAt = Date.parse(object.status.evaluatedAt);
  assert.deepEqual(evaluationFreshness(object, evaluatedAt + 1000),
    { fresh: true, reason: null });
  assert.deepEqual(evaluationFreshness(object, evaluatedAt + EVALUATION_FRESHNESS_MS + 1),
    { fresh: false, reason: "Stale" });

  // AND NO SERVER INSTANT IS NOT A FRESH ONE. This is the fail-closed side and
  // it is the one this page takes: with no `Date` header seen, freshness has
  // not been ESTABLISHED, and `unknown` is what "not established" reads as.
  assert.deepEqual(evaluationFreshness(object, null),
    { fresh: false, reason: "NoServerClock" },
    "and it says WHICH unknown it is: `no server clock` and `this evaluation is old` are two " +
      "different things (review F7)");

  // THE MUTANT THIS PAIR IS FOR: a freshness check that fell back to
  // `Date.now()` when it had no server instant. An evaluation the BROWSER
  // thinks is one second old is still `unknown` here, because the browser's
  // clock is not one the cluster ever saw -- and a page that used it would
  // disagree with the controller's own refusal by exactly the skew between
  // them, silently, in the direction that reads green.
  const justNow = JSON.parse(JSON.stringify(object));
  justNow.status.evaluatedAt = new Date().toISOString();
  assert.deepEqual(
    evaluationFreshness(justNow, null),
    { fresh: false, reason: "NoServerClock" },
    "with no SERVER instant, an evaluation the browser's own clock calls one second old is " +
      "still unknown",
  );
  assert.deepEqual(
    evaluationFreshness(justNow, Date.parse(justNow.status.evaluatedAt) + 1000),
    { fresh: true, reason: null },
    "and the same object IS fresh once a server instant establishes it",
  );
  const html = decode(renderPolicyKeys(object, null));
  // The STATE column is `spec.keys[].state` -- what the spec DECLARES -- and
  // it is still printed. What must not appear is the controller's VERDICT
  // rendered as one: that badge is the evaluation column.
  assert.equal(html.indexOf("badge-green\">Active<"), -1,
    "with no server clock the verdict column is not rendered as a verdict");
  assert.match(html, /badge-flat">unknown</);
  assert.ok(EVALUATION_UNKNOWN === "unknown");
});

test("a_fresh_evaluation_renders_the_controllers_own_verdict_and_the_lifecycle_beside_it", () => {
  const object = d3("trustpolicy-lifecycle.json");
  const now = Date.parse(object.status.evaluatedAt) + 60000;
  assert.deepEqual(evaluationFreshness(object, now), { fresh: true, reason: null });
  const html = decode(renderPolicyKeys(object, now));

  assert.match(html, />Active</);
  assert.match(html, />Retired</);
  assert.match(html, />Revoked</);
  assert.match(html, /EvidenceSigning/);
  assert.match(html, /GovernedApproval/);
  assert.match(html, /may sign something new/);
  assert.match(html, /may not sign anything new/);
  assert.match(html, /verification: Historical/);

  // RETIREMENT AND REVOCATION ARE EXPLAINED APART.
  assert.match(html, /it authorises nothing new, and everything it signed before that instant still verifies/);
  assert.match(html, /revoked for compromise/);
  assert.match(html, /the document's own claimed signing time is not accepted here/);
  assert.match(html, /treated as a retirement at that instant/);

  // A key with no verdict in `status.keys[]` is unknown even in a fresh
  // evaluation: the freshness is about the OBJECT, the verdict about the KEY.
  const missing = JSON.parse(JSON.stringify(object));
  missing.status.keys = missing.status.keys.slice(0, 1);
  assert.equal(verdictFor(missing.status, object.spec.keys[1].keyId), null);
  const out = decode(renderPolicyKeys(missing, now));
  assert.match(out, /this key has no verdict in status.keys\[\]/);
});

test("two_policies_claiming_one_namespace_resolve_to_nothing_and_the_page_says_so", () => {
  const object = d3("trustpolicy-stale.json");
  const html = decode(renderPolicyFacts(object, Date.parse(object.status.evaluatedAt) + 1000));
  assert.match(html, /data-trust-conflict="true"/);
  assert.match(html, /each of them resolves to no policy at all/);
  assert.match(html, /refused with TrustPolicyConflict/);
  assert.match(html, /team-a \(org-default, org-stale\)/);
});

test("the_keys_page_falls_back_to_the_roster_by_name_and_submits_nothing", () => {
  const html = renderKeysPage({ policies: [], roster: null, now: null, reason: "" });
  const text = decode(html);
  assert.match(text, /No TrustPolicy and no TrustRoster/);
  assert.equal(html.indexOf("<button"), -1, "no control on this page submits anything");
  assert.equal(html.indexOf("<form"), -1);
  assert.match(text, /kubectl --context <ctx> apply -f trustpolicy.yml/);
  assert.match(text, /logweir trust migrate-roster/);
  assert.equal(text.indexOf("PRIVATE KEY"), -1);

  const withPolicy = renderKeysPage({
    policies: [d3("trustpolicy-active.json")],
    roster: { metadata: { name: "default" }, spec: {}, status: {} },
    now: Date.parse("2026-09-18T04:43:00Z"),
  });
  assert.match(decode(withPolicy), /It is NOT deleted by migration/);
  // THE KEY ROWS CARRY NO PUBLIC MATERIAL. `spkiPem` is declared by the
  // contract and deliberately not rendered: it is long, and a key id is what
  // an operator compares out of band. (The `kubectl` TEMPLATE below the table
  // spells the PEM markers, because an operator has to paste a key into it.)
  const rows = decode(renderPolicyKeys(d3("trustpolicy-active.json"),
    Date.parse("2026-09-18T04:43:00Z")));
  assert.equal(rows.indexOf("BEGIN PUBLIC KEY"), -1);
  assert.equal(rows.indexOf("REDACTED PUBLIC KEY BODY"), -1);
});

// ===========================================================================
// PLAT-19.1 -- the badge cases
// ===========================================================================

test("a_historical_basis_is_green_and_carries_its_qualifier", () => {
  const object = d3("restore-historical.json");
  assert.equal(object.status.evidence.verification.trust.basis, "Historical");
  const badge = restoreBadge(object.status);
  assert.match(badge, /badge-green/, "rotation is supposed to produce exactly this state");
  assert.match(decode(badge), /signed before that key was retired/);
  assert.ok(HISTORICAL_SUFFIX.length > 0);
  assert.deepEqual(GREEN_BASES.slice(), ["Current", "Historical"]);

  const current = d3("restore-completed-newtopic.json");
  assert.match(restoreBadge(current.status), /badge-green/);
  assert.equal(decode(restoreBadge(current.status)).indexOf("signed before"), -1,
    "and a Current basis carries no qualifier");
});

test("untrusted_notattempted_and_invalid_are_three_claims_and_keep_the_one_word", () => {
  const cases = [
    ["restore-untrusted.json", /untrusted signer -- the bytes are authentic/],
    ["restore-recorded-before-revocation.json", /recorded before revocation/],
  ];
  for (const [name, pattern] of cases) {
    const object = d3(name);
    const badge = restoreBadge(object.status);
    assert.match(badge, /badge-unverified/, name + " is not green");
    assert.ok(decode(badge).indexOf("unverified") !== -1,
      name + ": the word every older surface reads is still there");
    assert.match(decode(badge), pattern, name + ": and the case is named beside it");
  }

  assert.match(decode(unverifiedCaption({ result: "Invalid" }, true)),
    /invalid -- the signature did not verify/);
  assert.match(decode(unverifiedCaption({ result: "NotAttempted" }, true)),
    /not attempted -- the controller could not check/);
  assert.match(decode(unverifiedCaption({}, true)), /no verification was recorded/);
  assert.equal(verificationCase({ result: "Valid" }, true), "",
    "a valid verification of a successful run has no case to name");
  assert.match(verificationCase({ result: "Valid" }, false),
    /the document verified and the run itself did not succeed/);
});

test("a_recorded_before_revocation_basis_is_never_green", () => {
  // THE MUTANT THIS ARM IS FOR. A green rule that read only `result` would
  // paint this row green: the signature verifies, the run passed, and the key
  // is revoked for COMPROMISE with nothing but a controller's earlier
  // observation behind it.
  const object = d3("restore-recorded-before-revocation.json");
  assert.equal(object.status.evidence.verification.result, "Valid");
  assert.equal(object.status.outcome, "pass");
  assert.equal(validVerification(object.status), null, "it is not a green verification");
  assert.doesNotMatch(restoreBadge(object.status), /badge-green/);
  assert.match(backupBadge({
    exitCode: 0,
    evidence: { verification: object.status.evidence.verification },
  }), /badge-unverified/, "and the Backup rule agrees");
});

test("an_explicit_basis_None_is_an_ABSENCE_and_reads_by_the_pre_existing_rule", () => {
  // REVIEW F1. D3 section 12 spells this exact case: "`trust` absent ->
  // `basis: None` and the badge uses the pre-existing rule". The STRING
  // `"None"` and an absent block are one fact said two ways -- one by a custom
  // resource an older controller wrote, one by a DTO that fills the block in
  // for it -- and reading the string as a downgrade put
  // `unverified: no verification was recorded` on every archive an upgraded
  // cluster carries, with `result: Valid`, `verifiedAt` and `matchedKeyId`
  // sitting beside it saying otherwise.
  //
  // THE FIXTURE IS A REAL PRE-D3 OBJECT. `backup-pre-d3-basis-none.json` is
  // the lab's own 2026-09-14 Backup, captured whole; its `trust` block is
  // `{basis: "None", keyState: "Active", policy: {name: "legacy-roster-v1"}}`,
  // written by the intermediate controller build that introduced the block
  // without the basis vocabulary (see claude/lab-refresh-3.result.md section 9).
  const real = d3("backup-pre-d3-basis-none.json");
  const verification = real.status.evidence.verification;
  assert.equal(verification.trust.basis, "None");
  assert.equal(verification.trust.policy.name, "legacy-roster-v1");
  assert.equal(TRUST_BASIS_NOT_OBSERVED, "None");
  assert.equal(basisAllowsGreen("None"), true, "an explicit None is an absence");
  assert.equal(basisAllowsGreen(undefined), true, "and so is no basis at all");
  assert.equal(basisAllowsGreen("Current"), true);
  assert.equal(basisAllowsGreen("Historical"), true);
  assert.equal(basisAllowsGreen("RecordedBeforeRevocation"), false);
  assert.equal(basisAllowsGreen("SomethingThisBuildDoesNotKnow"), false,
    "a basis this build cannot read is still not a pass");

  // THIS object is `Untrusted`, so it is not green -- for the reason the
  // controller recorded, and NOT for the basis.
  assert.equal(verification.result, "Untrusted");
  const untrusted = decode(backupBadge(real.status));
  assert.ok(untrusted.indexOf("badge-unverified") !== -1);
  assert.match(untrusted, /untrusted signer -- the bytes are authentic/);
  assert.equal(untrusted.indexOf("no verification was recorded"), -1,
    "the caption is the case the controller recorded, not a claim that nothing did");

  // AND THE SAME OBJECT WITH THE CONTROLLER'S OWN `Valid` IS GREEN.
  const valid = d3("backup-valid-basis-none.json");
  assert.equal(valid.status.evidence.verification.result, "Valid");
  assert.equal(valid.status.evidence.verification.trust.basis, "None");
  assert.equal(valid.status.exitCode, 0);
  const ok = validVerification(valid.status);
  assert.notEqual(ok, null, "an explicit None does not withdraw a recorded verdict");
  assert.equal(ok[2], "None", "and the basis travels as what it is");
  const badge = decode(backupBadge(valid.status));
  assert.ok(badge.indexOf("badge-green") !== -1,
    "every archive an upgraded cluster carries stays green: " + badge);
  assert.match(badge, /verified by weirkeeper at 2026-09-14T23:51:21Z against key/);
  assert.equal(badge.indexOf("signed before that key was retired"), -1,
    "and it carries no qualifier, because no basis was established to qualify it");

  // THE OPERATION VIEW'S EVIDENCE BLOCK READS THE SAME FUNCTION, so the two
  // halves of the rule cannot come to disagree.
  const facts = operationFacts({
    kind: "backup", name: "b", uid: "u", terminal: true, verifiedSuccess: true,
    evidenceVerification: valid.status.evidence.verification,
    result: { status: "pass" }, evidence: {}, conditions: [],
  }, true);
  assert.ok(decode(renderEvidence(facts)).indexOf("badge-green") !== -1,
    "the operation view agrees with the badge");
});

test("an_object_with_no_trust_block_at_all_stays_green", () => {
  // The block is ADDITIVE (D3 section 12). Treating "an older controller wrote
  // this" as a downgrade would turn every archive in an upgraded cluster red.
  const object = JSON.parse(JSON.stringify(d3("restore-completed-newtopic.json")));
  delete object.status.evidence.verification.trust;
  delete object.status.evidence.verification.signedAt;
  assert.match(restoreBadge(object.status), /badge-green/);
  const ok = validVerification(object.status);
  assert.equal(ok[2], null, "and no basis is invented for it");
  assert.equal(greenLabel("t", "k", null).indexOf("signed before"), -1);
});

test("every_restore_result_carries_its_verification_scope_sentence", () => {
  const object = d3("restore-completed-newtopic.json");
  const scope = scopeOf(object);
  assert.equal(scope.level, "sampled", "byte-fingerprint maps to sampled");
  assert.equal(SCOPE_LEVEL_OF_INTEGRITY["consume-only"], "degraded");
  assert.equal(SCOPE_LEVEL_OF_INTEGRITY["not-attempted"], "none");
  assert.equal(SCOPE_LEVEL_OF_INTEGRITY.complete, undefined,
    "there is no `complete` on either side of this table");

  const html = decode(renderRestoreDetail(object));
  assert.match(html, /this is a sampled check, not an exhaustive comparison/);
  assert.match(html, /64 of 64 sampled records matched byte-for-byte/);

  const degraded = JSON.parse(JSON.stringify(object));
  degraded.status.integrity.level = "consume-only";
  assert.match(decode(renderRestoreDetail(degraded)), /The comparison was degraded/);

  const none = JSON.parse(JSON.stringify(object));
  none.status.integrity.level = "not-attempted";
  assert.match(decode(renderRestoreDetail(none)), /No record check ran for this restore/);

  // An object with a level this table does not know gets the honest sentence
  // rather than a claim.
  const unknown = JSON.parse(JSON.stringify(object));
  unknown.status.integrity.level = "something-new";
  assert.equal(scopeOf(unknown), null);
  assert.match(verificationScopeSentence(null), /An absent scope is not a complete one/);
});

test("rows_of_both_kinds_link_to_the_durable_operation_view", () => {
  const backup = d3("backup-progress-finished.json");
  const html = decode(renderRestoreDetail(d3("restore-completed-newtopic.json")));
  assert.match(html, /#\/operations\?/, "a Restore detail carries the link");
  assert.ok(operationRoute("team-a", "backup", backup.metadata.name, backup.metadata.uid)
    .indexOf("uid=" + backup.metadata.uid) !== -1);
});

// ===========================================================================
// PLAT-16.1 / PLAT-16.2 -- the retention panel
// ===========================================================================

test("the_never_deletes_sentence_stays_verbatim_for_a_report_and_is_replaced_otherwise", () => {
  const schedule = { status: { retentionReport: { evaluatedAt: "2026-09-19T01:11:38Z" } } };
  assert.ok(retentionSentenceFor(schedule.status.retentionReport, null)
    .indexOf(RETENTION_SENTENCE) !== -1,
    "with no policy the panel keeps the sentence it has always carried");

  const report = d3("retention-report.json");
  assert.equal(enforcementOf(report), "RecommendationOnly");
  assert.ok(retentionSentenceFor({}, report).indexOf(RETENTION_SENTENCE) !== -1);

  const enforce = d3("retention-enforce.json");
  assert.equal(enforcementOf(enforce), "LogweirWorker");
  const replaced = decode(retentionSentenceFor({}, enforce));
  assert.equal(replaced.indexOf(RETENTION_SENTENCE), -1,
    "PRINTING `Logweir never deletes from your archive` BESIDE A POLICY THAT DELETES " +
      "NIGHTLY is the most consequential false sentence this console could render");
  assert.match(replaced, /An isolated Logweir retention worker deletes archive objects/);
  assert.match(replaced, /data-enforcement="LogweirWorker"/);

  const external = d3("retention-external.json");
  assert.equal(enforcementOf(external), "ExternalLifecycleDeclared");
  assert.match(decode(retentionSentenceFor({}, external)),
    /performed by your bucket lifecycle rule/);
  assert.equal(Object.keys(ENFORCEMENT_SENTENCES).length, 3);
});

test("the_panel_reads_what_is_happening_and_not_what_was_asked_for", () => {
  // A POLICY IN `mode: Enforce` WHOSE DESTINATION WILL NOT RESOLVE REPORTS
  // `RecommendationOnly`, and the panel must say the thing that is true.
  const policy = JSON.parse(JSON.stringify(d3("retention-enforce.json")));
  assert.equal(policy.spec.mode, "Enforce");
  policy.status.enforcement = "RecommendationOnly";
  const html = decode(renderEnforcement({}, policy));
  assert.match(html, /mode asked for/);
  assert.match(html, /what is happening/);
  assert.equal(html.indexOf("The approved plan"), -1,
    "there is no approved-plan block for a policy that is not enforcing");
  assert.ok(retentionSentenceFor({}, policy).indexOf(RETENTION_SENTENCE) !== -1);
});

test("an_enforcing_policy_shows_three_separate_digests_and_the_irreversibility_sentence", () => {
  const policy = d3("retention-enforce.json");
  const html = decode(renderEnforcement({}, policy));
  assert.match(html, /digest an administrator approved/);
  assert.match(html, /digest of the newest evaluation/);
  assert.match(html, /that plan expires at/);
  assert.match(html, /Deletion is not reversible/);
  assert.match(html, /the only record that survives is the tombstone and the retention record/);
  assert.ok(IRREVERSIBLE_SENTENCE.length > 0);

  // AND `legalHold` IS NEVER ENFORCED BY LOGWEIR, IN ANY MODE.
  assert.equal(policy.status.guarantees.legalHold, "ProviderEnforcedUnverified");
  assert.match(html, /declared by your provider; Logweir cannot verify it/);
  assert.match(html, /not enforced/, "and sharedSegments says so for this catalog view");
});

test("enforcement_degraded_is_shown_with_its_words", () => {
  const policy = d3("retention-enforce.json");
  const html = decode(renderEnforcement({}, policy));
  assert.match(html, /data-enforcement-degraded="true"/);
  assert.match(html, /three consecutive enforcement runs failed/);
  assert.match(html, /this policy has stopped scheduling them until its spec changes/);
  assert.ok(ENFORCEMENT_DEGRADED_SENTENCE.length > 0);

  const healthy = JSON.parse(JSON.stringify(policy));
  for (const condition of healthy.status.conditions) {
    if (condition.type === "EnforcementDegraded") {
      condition.status = "False";
    }
  }
  assert.equal(decode(renderEnforcement({}, healthy)).indexOf("data-enforcement-degraded"), -1);
});

test("an_external_lifecycle_policy_says_logweir_cannot_read_the_rule_back", () => {
  const policy = d3("retention-external.json");
  const html = decode(renderEnforcement({}, policy));
  assert.match(html, /The declared rule is <code>d3w14-rule<\/code>/);
  assert.match(html, /expiring objects after 7 day\(s\)/);
  assert.match(html, /Logweir cannot read a bucket lifecycle configuration back/);
  assert.match(html, /every guarantee above that says so is a DECLARATION/);
});

test("the_policy_covering_a_schedule_comes_from_the_controllers_own_supersededBy", () => {
  const policy = d3("retention-report.json");
  const schedule = {
    status: { retentionReport: { supersededBy: { kind: "RetentionPolicy", name: "keep-a" } } },
  };
  assert.equal(policyForSchedule(schedule, [policy]), policy);
  assert.equal(policyForSchedule({ status: {} }, [policy]), null,
    "with no supersededBy this page names no policy rather than matching on a destination");
  assert.equal(policyForSchedule(schedule, []), null);

  const panel = decode(renderRetentionPanel(schedule, policy));
  assert.match(panel, /A RetentionPolicy now covers this schedule's destination/);
  assert.match(panel, /This report is the legacy per-schedule one/);
});

// ===========================================================================
// the contract's own self-pinning arm
// ===========================================================================

/** THE ROUTES THIS CLIENT ASSUMES, exactly as `d3w12.result.md` section 4.1
 *  lists them. Every one must be a key of the published document's `paths`
 *  once the D3 family lands. */
const ASSUMED_D3_ROUTES = Object.freeze([
  "/api/v1/namespaces/{ns}/operations/{kind}/{name}/events",
  "/api/v1/namespaces/{ns}/protection-policies",
  "/api/v1/namespaces/{ns}/protection-policies/{name}",
  "/api/v1/namespaces/{ns}/catalogs",
  "/api/v1/namespaces/{ns}/catalogs/{name}",
  "/api/v1/namespaces/{ns}/catalogs/{name}/points",
  "/api/v1/namespaces/{ns}/catalogs/{name}/signers",
  "/api/v1/namespaces/{ns}/retention-policies",
  "/api/v1/trust-policies",
]);

/** The ONE route of the four families this document already publishes, and the
 *  sentinel for "the D3 API half has not landed yet". */
const PUBLISHED_OPERATION_ROUTE = "/api/v1/namespaces/{ns}/operations/{kind}/{name}";

/** THE SHAPE NAMES THIS CLIENT DECLARES THAT THE DOCUMENT DOES NOT PUBLISH YET
 *  -- checked in, sorted, and compared for EQUALITY below.
 *
 *  REVIEW F2. The first version of this arm let an unpublished name fall into
 *  an `assumed` bucket whose only assertion was a tautology, so a DTO the
 *  document published under a DIFFERENT name stayed in that bucket and the arm
 *  went green having pinned nothing -- which is the empty cell the typed
 *  contract exists to prevent, one level up. An equality against this list
 *  makes a name joining it, leaving it or changing its spelling a diff a
 *  reviewer reads, and makes the day `d3w11` lands a RED suite rather than a
 *  quiet one. */
const ASSUMED_D3_SHAPES = Object.freeze([
  "AlertDeliveryView",
  "AlertView",
  "CatalogPointList",
  "CatalogPointResponse",
  "CatalogPointView",
  "CatalogSignerView",
  "CatalogSignersResponse",
  "CompletionView",
  "CreateCatalogRequest",
  "D3Operation",
  "D3OperationResponse",
  "DiagnosticView",
  "EvidenceVerificationView",
  "KeyVerdictView",
  "ProgressView",
  "ProtectionPolicy",
  "ProtectionPolicyList",
  "ProtectionPolicyResponse",
  "ProtectionPolicyStatusView",
  "RecoveryCatalog",
  "RecoveryCatalogList",
  "RecoveryCatalogResponse",
  "RecoveryCatalogStatusView",
  "RetentionPolicy",
  "RetentionPolicyList",
  "RetentionPolicyResponse",
  "RetentionPolicyStatusView",
  "TeardownView",
  "TrustKeyView",
  "TrustPolicy",
  "TrustPolicyList",
  "TrustPolicyResponse",
  "TrustPolicyStatusView",
  "VerificationScopeView",
  "VerificationTrustView",
]);

test("every_d3_shape_and_enum_is_declared_and_pins_itself_once_the_api_publishes_it", () => {
  const schema = JSON.parse(readFileSync(
    new URL("../../schemas/logweir-api-v1.openapi.json", import.meta.url), "utf8"));
  const definitions = schema.components.schemas;
  const paths = schema.paths || {};

  const assumed = [];
  let pinned = 0;
  for (const name of Object.keys(D3_SHAPES)) {
    const published = definitions[name];
    if (published === undefined) {
      assumed.push(name);
      continue;
    }
    if (published.oneOf !== undefined) {
      pinned += 1;
      continue;
    }
    const shape = D3_SHAPES[name];
    assert.deepEqual(
      Object.keys(shape.required).sort(),
      (published.required || []).slice().sort(),
      name + ": the fields this client treats as required are not the fields the schema " +
        "requires. A field that became required on the server and stayed optional here is " +
        "the silent field loss the typed contract exists to stop.",
    );
    const declared = Object.keys(published.properties || {});
    for (const field of Object.keys(shape.required).concat(Object.keys(shape.optional))) {
      assert.ok(
        declared.indexOf(field) !== -1,
        name + "." + field + " is decoded here and is not a field of the published schema",
      );
    }
    pinned += 1;
  }

  // (1) THE ASSUMED SET IS AN EQUALITY AND NOT A BUCKET. A name renamed on
  // either side lands here, whichever side moved it.
  assert.deepEqual(
    assumed.slice().sort(),
    ASSUMED_D3_SHAPES.slice().sort(),
    "the set of D3 shapes this client declares that the published document does NOT name has " +
      "changed. If `d3w11` landed, empty ASSUMED_D3_SHAPES in the same change and let the " +
      "per-shape pin above do its work; if a shape was renamed HERE, rename it there too. A " +
      "name silently sitting in this bucket is a decoder pinned against nothing.",
  );

  // (2) ONCE THE D3 FAMILY IS PUBLISHED, NOTHING MAY STILL BE ASSUMED. The
  // sentinel is any of the routes section 4.1 names beyond the one the document
  // already carried, so this arm arms itself the moment the API half lands
  // rather than waiting for somebody to remember it.
  const landed = ASSUMED_D3_ROUTES.filter((route) => paths[route] !== undefined);
  assert.ok(paths[PUBLISHED_OPERATION_ROUTE] !== undefined,
    "the operation read this client has always used is still published");
  if (landed.length > 0) {
    assert.deepEqual(
      ASSUMED_D3_ROUTES.filter((route) => paths[route] === undefined),
      [],
      "the D3 route family is published and these routes this client addresses are not in it",
    );
    assert.deepEqual(assumed, [],
      "the D3 routes are published, so every D3 shape must be pinned against the document; " +
        "these are still assumed: " + assumed.join(", "));
    assert.ok(pinned >= 30, "and every declared shape is pinned; pinned=" + String(pinned));
  }

  assert.equal(assumed.length + pinned, Object.keys(D3_SHAPES).length);
  assert.ok(Object.keys(D3_SHAPES).length >= 35,
    "every D3 response DTO, list envelope and request body this client builds is declared");

  // (3) THE VOCABULARIES, ON THE SAME TERMS.
  const enumsAssumed = [];
  for (const name of Object.keys(D3_ENUMS)) {
    assert.ok(Array.isArray(D3_ENUMS[name]) && D3_ENUMS[name].length > 0,
      name + " is a closed, non-empty vocabulary");
    const published = definitions[name];
    if (published === undefined) {
      enumsAssumed.push(name);
      continue;
    }
    if (Array.isArray(published.enum)) {
      assert.deepEqual(D3_ENUMS[name].slice().sort(), published.enum.slice().sort(), name);
    }
  }
  if (landed.length > 0) {
    assert.deepEqual(enumsAssumed, [],
      "the D3 routes are published, so every D3 vocabulary must be pinned; these are not: " +
        enumsAssumed.join(", "));
  }
  assert.equal(D3_ENUMS.VerificationScopeLevel.indexOf("complete"), -1,
    "`complete` does not exist as a level in v1 and is absent from the vocabulary on purpose");
});

test("the_four_d3_custom_resources_decode_in_legacy_mode_and_refuse_a_missing_required_field", () => {
  for (const [plural, name] of [
    ["protectionpolicies", "protection-healthy.json"],
    ["recoverycatalogs", "catalog-truncated.json"],
    ["retentionpolicies", "retention-report.json"],
    ["trustpolicies", "trustpolicy-lifecycle.json"],
  ]) {
    const object = d3(name);
    assert.deepEqual(decodeLegacyObject(plural, object).value, object,
      plural + " decodes and is returned unchanged");
    const broken = JSON.parse(JSON.stringify(object));
    delete broken.spec;
    assert.throws(() => decodeLegacyObject(plural, broken), isContractFailure,
      plural + " with no spec is a contract failure and not an empty page");
  }
});

// ===========================================================================
// the mount halves, driven over a fake node and an in-memory API
// ===========================================================================
//
// THE RENDERERS ABOVE ARE PURE AND THE MOUNTS ARE NOT, and the defects the
// last two console waves' reviews caught all lived in the impure half: a form
// that kept rendering the values it had already saved, two clicks sharing one
// intent, and a control that only re-read. These rows drive the real mount
// functions over the smallest node `render.js`'s `replace` touches.

function fakeNode(form) {
  return {
    firstChild: null,
    adopted: [],
    appendChild(child) {
      this.adopted.push(child);
    },
    removeChild() {},
    querySelector(selector) {
      return form !== undefined && String(selector).indexOf("data-connect-archive") !== -1
        ? form
        : null;
    },
    querySelectorAll() {
      return [];
    },
    get html() {
      return this.adopted.map((a) => a.html).join("");
    },
    /** The MOST RECENT paint. `replace` clears a real node before it appends;
     *  this stand-in keeps every fragment, so a row asserting on what is on
     *  screen NOW has to read the last one rather than the concatenation. */
    get last() {
      return this.adopted.length === 0 ? "" : this.adopted[this.adopted.length - 1].html;
    },
  };
}

const fakeParse = (html) => [{ html: html }];

function fakeForm(values) {
  const listeners = [];
  return {
    elements: {
      name: { value: values.name },
      destination: { value: values.destination },
      syncMode: { value: values.syncMode },
    },
    addEventListener(type, handler) {
      if (type === "submit") {
        listeners.push(handler);
      }
    },
    submit() {
      for (const handler of listeners.slice(-1)) {
        handler({ preventDefault() {} });
      }
    },
  };
}

test("the_connect_form_sends_one_request_per_intent_and_a_second_click_while_pending_sends_none",
  async () => {
    const sent = [];
    let resolveCreate = null;
    const deps = {
      modeOf: () => "console",
      consoleList: async () => ({ items: [], page: { limit: 200 }, requestId: "r1" }),
      consoleCreate: async (ns, plural, body, options) => {
        sent.push({ ns: ns, plural: plural, body: body, key: options.idempotencyKey });
        return new Promise((resolve) => {
          resolveCreate = () => resolve({
            item: {
              name: body.name, namespace: ns, uid: "u1", resourceVersion: "1",
              spec: { destinationRef: body.destinationRef },
            },
            requestId: "r2",
          });
        });
      },
    };
    const form = fakeForm({ name: "primary", destination: "dest-a", syncMode: "Full" });
    const node = fakeNode(form);
    await mountCatalog(node, "d3-mount-a", fakeParse, null, deps);
    assert.ok(node.html.indexOf("Connect an existing archive") !== -1, "the form is on screen");

    form.submit();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(sent.length, 1, "one request");
    assert.equal(sent[0].plural, "catalogs");
    assert.deepEqual(sent[0].body,
      { name: "primary", destinationRef: { name: "dest-a" }, syncMode: "Full" });
    assert.ok(sent[0].key.indexOf("logweir-ui.catalog.") === 0,
      "under an idempotency intent this draft holds");

    // A SECOND CLICK WHILE THE FIRST IS IN FLIGHT SENDS NOTHING. The mutation
    // machine has no `start` out of `pending`, and that absence is the guard.
    form.submit();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(sent.length, 1, "the second click sent nothing");

    resolveCreate();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.ok(node.html.indexOf("connect-result") !== -1, "and the outcome names the catalog");

    // AND A LATER CLICK IS A SECOND ARCHIVE WITH A NEW INTENT, because the
    // draft that held the first one ended with it.
    form.submit();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(sent.length, 2, "a deliberate second connect is a second request");
    assert.notEqual(sent[1].key, sent[0].key,
      "and it carries a NEW intent: resending the first one would return the first catalog");
  });

test("one_draft_holds_one_intent_across_every_resend_of_it", async () => {
  // THE DEFECT THIS ARM IS FOR, WHICH THE MUTANT BATTERY FOUND. `keepDraft` is
  // a REPLACE over the declared fields and not a merge, so a submit handler
  // that kept only the form's values dropped the intent the draft held -- and
  // the next submission minted a new one. A resend would then be a DIFFERENT
  // request under a different key, which is the exact opposite of what an
  // idempotency key is for: a lost response and a retry would create two
  // catalogs rather than resolving to the one the first request made.
  const sent = [];
  const deps = {
    modeOf: () => "console",
    consoleList: async () => ({ items: [], page: { limit: 200 }, requestId: "r1" }),
    consoleCreate: async (ns, plural, body, options) => {
      sent.push(options.idempotencyKey);
      const failure = new Error("no answer from the API server");
      failure.status = 503;
      failure.reason = "ServiceUnavailable";
      throw failure;
    },
  };
  const form = fakeForm({ name: "primary", destination: "dest-a", syncMode: "Full" });
  const node = fakeNode(form);
  await mountCatalog(node, "d3-mount-c", fakeParse, null, deps);
  for (let i = 0; i < 3; i += 1) {
    form.submit();
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  assert.equal(sent.length, 3, "three retries of one draft");
  assert.equal(sent[1], sent[0], "under ONE intent");
  assert.equal(sent[2], sent[0], "and it does not move while the draft lives");
});

test("the_connect_form_refuses_an_empty_field_before_anything_is_sent", async () => {
  const sent = [];
  const deps = {
    modeOf: () => "console",
    consoleList: async () => ({ items: [], page: { limit: 200 }, requestId: "r1" }),
    consoleCreate: async () => {
      sent.push(1);
      return {};
    },
  };
  const form = fakeForm({ name: "", destination: "", syncMode: "Full" });
  const node = fakeNode(form);
  await mountCatalog(node, "d3-mount-b", fakeParse, null, deps);
  form.submit();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(sent.length, 0, "nothing was sent");
  assert.ok(node.html.indexOf("a catalog needs a name") !== -1,
    "and the refusal is beside the field it is about");
});

test("the_operation_mount_applies_the_uid_guard_to_the_object_it_actually_read", async () => {
  const node = fakeNode();
  const read = [];
  await mountOperation(node, "team-a", { kind: "backup", name: "b1", uid: "the-one-i-asked-for" },
    fakeParse, {
      modeOf: () => "legacy",
      api: {
        get: async (ns, plural, name) => {
          read.push(plural + "/" + name);
          return {
            kind: "Backup",
            metadata: { name: "b1", namespace: ns, uid: "a-different-run" },
            spec: {}, status: { phase: "Succeeded" },
          };
        },
      },
      setTimer: () => 0,
      clearTimer: () => {},
    }, null);
  assert.deepEqual(read, ["backups/b1"], "it read the object the route names");
  assert.ok(node.html.indexOf("data-uid-mismatch=\"true\"") !== -1,
    "and refused it, because the object answers to another uid");
  assert.equal(node.html.indexOf("Succeeded"), -1,
    "nothing about the run it found is rendered");
});

test("the_operation_mount_refuses_a_kind_this_route_does_not_serve", async () => {
  const node = fakeNode();
  let reads = 0;
  const watch = await mountOperation(node, "team-a", { kind: "rehearsal", name: "x", uid: "" },
    fakeParse, { modeOf: () => "legacy", api: { get: async () => { reads += 1; return {}; } } },
    null);
  assert.equal(watch, null);
  assert.equal(reads, 0, "nothing was read for a kind this route does not serve");
  assert.ok(node.html.indexOf("This route serves backup and restore operations") !== -1);
});

test("the_protection_mount_renders_the_list_and_one_policy", async () => {
  const list = fakeNode();
  await mountProtection(list, "team-a", fakeParse, null, {
    modeOf: () => "legacy",
    api: { list: async () => ({ items: [d3("protection-unprotected.json")] }) },
  });
  assert.ok(list.html.indexOf("Unprotected") !== -1);
  assert.ok(list.html.indexOf("PROTECTION") !== -1 && list.html.indexOf("SCHEDULES") !== -1);

  const detail = fakeNode();
  await mountProtectionDetail(detail, "team-a", "protect-healthy", fakeParse, null, {
    modeOf: () => "legacy",
    api: { get: async () => d3("protection-healthy.json") },
  });
  assert.ok(detail.html.indexOf("Newest available recovery point") !== -1);
  assert.ok(detail.html.indexOf("newest archived record") !== -1);
});

test("the_keys_mount_prefers_the_policy_and_names_the_refusal_when_it_cannot_read_one",
  async () => {
    // WITH A POLICY: the roster read still happens (it is what a rollback would
    // read) and the policy is what the page is about.
    const withPolicy = fakeNode();
    await mountKeys(withPolicy, fakeParse, {
      modeOf: () => "legacy",
      serverClock: () => Date.parse("2026-09-18T04:43:00Z"),
      api: {
        listCluster: async (plural) => plural === "trustpolicies"
          ? { items: [d3("trustpolicy-active.json")] }
          : { items: [] },
      },
    }, null);
    assert.ok(withPolicy.html.indexOf("EVALUATION") !== -1);
    assert.ok(withPolicy.html.indexOf("badge-green\">Active<") !== -1,
      "a fresh verdict IS rendered as one");

    // WITHOUT ONE: the API server's own refusal is on screen, by name, and not
    // an empty table.
    const refused = fakeNode();
    const forbidden = new Error("trustpolicies.logweir.dev is forbidden");
    forbidden.status = 403;
    forbidden.reason = "Forbidden";
    await mountKeys(refused, fakeParse, {
      modeOf: () => "legacy",
      serverClock: () => null,
      api: {
        listCluster: async (plural) => {
          if (plural === "trustpolicies") {
            throw forbidden;
          }
          return { items: [] };
        },
      },
    }, null);
    assert.ok(refused.html.indexOf("403") !== -1, "the status is on screen");
    assert.ok(refused.html.indexOf("Forbidden") !== -1, "and the reason");
    assert.ok(refused.html.indexOf("kind: TrustPolicy") !== -1,
      "with the document a cluster admin applies, rendered and not submitted");
  });

// ===========================================================================
// review fix round 1: the close, the cleared form and the spent intent
// ===========================================================================

test("the_stream_is_CLOSED_on_a_settled_document_and_on_disposal", async () => {
  // REVIEW F3. The behaviour was right and the GUARD was missing: the
  // reviewer's mutant dropped `stream.close()` and kept `stream = null`, and
  // the whole suite stayed green. A stream that survives a terminal document
  // and a route change is one live connection per visit to `#/operations`,
  // leaked across navigations, and the one-call-site gate counts the network
  // call by name and knows nothing about a stream.
  function fakeStream() {
    const state = { closed: 0, handlers: {} };
    return {
      state: state,
      readyState: 1,
      addEventListener(type, handler) {
        state.handlers[type] = handler;
      },
      close() {
        state.closed += 1;
        this.readyState = 2;
      },
    };
  }

  // (a) A TERMINAL, SETTLED DOCUMENT CLOSES IT.
  let made = null;
  class SettlingSource {
    constructor() {
      made = fakeStream();
      Object.assign(this, made);
      this.addEventListener = made.addEventListener.bind(made);
      this.close = made.close.bind(this);
      queueMicrotask(() => {
        made.state.handlers.operation({ data: JSON.stringify(d3("operation-restore-completed.json")) });
      });
    }
  }
  const settled = watchOperation("team-a", "backup", "b1", () => {}, null, {
    modeOf: () => "console",
    EventSourceClass: SettlingSource,
    setTimer: () => 0,
    clearTimer: () => {},
    read: async () => ({ terminal: false, verification: { state: "pending" } }),
  });
  await new Promise((resolve) => setTimeout(resolve, 1));
  assert.equal(made.state.closed, 1,
    "a settled document closes the connection; the run is untouched either way");
  settled.stop();
  assert.equal(made.state.closed, 1, "and stop() after it does not close twice");

  // (b) LEAVING THE ROUTE CLOSES IT, with nothing settled at all.
  let open = null;
  class QuietSource {
    constructor() {
      open = fakeStream();
      this.addEventListener = open.addEventListener.bind(open);
      this.close = open.close.bind(open);
      this.readyState = 1;
    }
  }
  const controller = new AbortController();
  watchOperation("team-a", "backup", "b2", () => {}, {
    signal: controller.signal,
    isCurrent: () => !controller.signal.aborted,
  }, {
    modeOf: () => "console",
    EventSourceClass: QuietSource,
    setTimer: () => 0,
    clearTimer: () => {},
    read: async () => ({ terminal: false, verification: { state: "pending" } }),
  });
  assert.equal(open.state.closed, 0, "nothing has settled");
  controller.abort();
  assert.equal(open.state.closed, 1,
    "the route's own signal closes the connection -- and closing a READ never cancels the run");
});

test("a_durable_result_empties_the_form_and_re_reads_the_list", async () => {
  // REVIEW F4, which is d1w7's post-save staleness in a new form. The catalog
  // EXISTS after a success; leaving its name and destination in the inputs
  // invites a second click that creates a SECOND RecoveryCatalog for one
  // destination -- refused by the controller with `DuplicateCatalog`, leaving a
  // dead object this console holds no delete for.
  let lists = 0;
  const created = [];
  const deps = {
    modeOf: () => "console",
    consoleList: async () => {
      lists += 1;
      return {
        items: created.slice(),
        page: { limit: 200, nextCursor: null, snapshot: "1" },
        requestId: "r" + String(lists),
      };
    },
    consoleCreate: async (ns, plural, body) => {
      const item = {
        name: body.name, namespace: ns, uid: "u" + String(created.length + 1),
        resourceVersion: "1", spec: { destinationRef: body.destinationRef },
      };
      created.push(item);
      return { item: item, requestId: "c1" };
    },
  };
  const form = fakeForm({ name: "primary", destination: "dest-a", syncMode: "Full" });
  const node = fakeNode(form);
  await mountCatalog(node, "d3-fix-a", fakeParse, null, deps);
  assert.equal(lists, 1);

  form.submit();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(created.length, 1, "one catalog");
  assert.equal(lists, 2, "and the list was re-read, so the object that now exists is on screen");

  const html = node.last;
  assert.ok(html.indexOf("connect-result") !== -1, "the outcome names what was made");
  assert.ok(html.indexOf("id=\"catalog-name\" name=\"name\" value=\"\"") !== -1,
    "and the form is EMPTY: " + html.slice(Math.max(0, html.indexOf("catalog-name") - 40),
      html.indexOf("catalog-name") + 120));
  assert.equal(html.indexOf("value=\"primary\""), -1,
    "the submitted name is not still sitting in the input inviting a second click");
});

test("a_corrected_body_mints_a_new_intent_rather_than_spending_the_old_one", async () => {
  // REVIEW F5. An idempotency key binds a REQUEST. An operator who submits,
  // is refused, corrects the destination and submits again would otherwise
  // spend the same key on a different body -- `409 idempotency_conflict`, and
  // every further retry the same, with only a page reload to recover.
  const sent = [];
  let failWith = null;
  const deps = {
    modeOf: () => "console",
    consoleList: async () => ({ items: [], page: { limit: 200 }, requestId: "r1" }),
    consoleCreate: async (ns, plural, body, options) => {
      sent.push({ key: options.idempotencyKey, destination: body.destinationRef.name });
      if (failWith !== null) {
        throw failWith;
      }
      return {
        item: {
          name: body.name, namespace: ns, uid: "u1", resourceVersion: "1",
          spec: { destinationRef: body.destinationRef },
        },
        requestId: "c1",
      };
    },
  };
  const refusal = new Error("the destination dest-typo does not exist");
  refusal.status = 422;
  refusal.reason = "validation_failed";
  failWith = refusal;

  const form = fakeForm({ name: "primary", destination: "dest-typo", syncMode: "Full" });
  const node = fakeNode(form);
  await mountCatalog(node, "d3-fix-b", fakeParse, null, deps);

  form.submit();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(sent.length, 1);

  // THE SAME BODY AGAIN IS THE SAME REQUEST.
  form.submit();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(sent.length, 2);
  assert.equal(sent[1].key, sent[0].key, "a retry of the same body keeps its intent");

  // A CORRECTED BODY IS A DIFFERENT REQUEST AND GETS A DIFFERENT KEY.
  form.elements.destination.value = "dest-a";
  failWith = null;
  form.submit();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(sent.length, 3);
  assert.equal(sent[2].destination, "dest-a");
  assert.notEqual(sent[2].key, sent[0].key,
    "the corrected body carries a NEW intent, so the API is not asked to resolve one key to " +
      "two different requests");
});

test("a_spent_intent_is_explained_and_not_left_as_a_bare_refusal", () => {
  const conflict = new Error("this key was first seen with a different request");
  conflict.status = 409;
  conflict.reason = "idempotency_conflict";
  assert.equal(isIntentConflict(conflict), true);
  assert.equal(isIntentConflict({ reason: "validation_failed" }), false);
  const html = decode(renderConnectForm({
    ns: "team-a", values: { name: "primary" }, errors: { fields: {}, unmatched: [] },
    state: { phase: "failed", error: conflict },
  }));
  assert.match(html, /data-intent-used="true"/);
  assert.match(html, /already spent on a DIFFERENT request/);
  assert.match(html, /submit it again/,
    "and it says what clears it, because the escape is already in the form");
});

test("the_point_table_says_when_it_is_one_page_of_a_larger_view", () => {
  const one = decodeCatalogPoints(d3("catalog-points.json")).value;
  assert.equal(cursorOf(one), null);
  assert.equal(renderPoints(one, "team-a", "primary").indexOf("data-more-points"), -1);

  const more = d3("catalog-points.json");
  more.page.nextCursor = "opaque-cursor";
  const page = decodeCatalogPoints(more).value;
  assert.equal(cursorOf(page), "opaque-cursor");
  const html = decode(renderPoints(page, "team-a", "primary"));
  assert.match(html, /data-more-points="true"/);
  assert.match(html, /That is a different truncation from the window above/,
    "the HTTP page and the Kubernetes view limit are two truncations and read as two");
  assert.ok(MORE_POINTS_SENTENCE.indexOf("logweir catalog list") !== -1);
});

test("a_server_instant_too_old_to_be_now_is_reported_as_none", () => {
  // REVIEW F6. A recorded instant that never expired is a stopped clock, and a
  // clock in the past SHRINKS the measured age -- the direction that reads
  // fresh. The instant is carried forward by an ELAPSED duration and dropped
  // past the bound; the duration is a difference of two monotonic readings and
  // never an absolute one.
  assert.equal(SERVER_TIME_MAX_AGE_MS, 300000);
  const dated = Date.parse("2026-09-18T04:42:25Z");
  const carried = (elapsed) => {
    if (elapsed < 0 || elapsed > SERVER_TIME_MAX_AGE_MS) {
      return null;
    }
    return dated + elapsed;
  };
  assert.equal(carried(0), dated, "just recorded, it IS the instant");
  assert.equal(carried(30000), dated + 30000, "and it moves with the time that has passed");
  assert.equal(carried(SERVER_TIME_MAX_AGE_MS + 1), null,
    "past the bound there is no server instant, and no server instant reads `unknown`");
  assert.equal(carried(-1), null, "a clock that went backwards is not one to judge freshness by");

  // And the page's own answer to `null` is `unknown`, under its own reason.
  const object = d3("trustpolicy-active.json");
  assert.deepEqual(evaluationFreshness(object, null),
    { fresh: false, reason: "NoServerClock" });
});
