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
  IRREVERSIBLE_SENTENCE,
  NO_ONE_CLICK_TRUST_SENTENCE,
  RETENTION_SENTENCE,
  SCOPE_LEVEL_OF_INTEGRITY,
  TWO_AXES_SENTENCE,
  TWO_HEALTHS_SENTENCE,
  TWO_INSTANTS_SENTENCE,
  UNKNOWN_IS_NOT_VALID_SENTENCE,
  healthBadge,
  stateBadge,
  unverifiedCaption,
  verificationCase,
  verificationScopeSentence,
} from "../render.js";
import {
  DIFFERENT_RUN_SENTENCE,
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
  restorePointRoute,
  validateConnect,
  viewIsUsable,
} from "../pages/catalog.js";
import {
  EVALUATION_FRESHNESS_MS,
  FINGERPRINT_COMMAND,
  evaluationFreshness,
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
  assert.deepEqual(evaluationFreshness(object, null), { fresh: false, reason: "Stale" });
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

test("every_d3_shape_and_enum_is_declared_and_pins_itself_once_the_api_publishes_it", () => {
  const schema = JSON.parse(readFileSync(
    new URL("../../schemas/logweir-api-v1.openapi.json", import.meta.url), "utf8"));
  const definitions = schema.components.schemas;
  const assumed = [];
  let pinned = 0;
  for (const name of Object.keys(D3_SHAPES)) {
    const published = definitions[name];
    if (published === undefined) {
      assumed.push(name);
      continue;
    }
    if (published.oneOf !== undefined) {
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
    pinned += 1;
  }
  // THE ASSUMPTION IS RECORDED, NOT HIDDEN. Until the API half lands, these
  // names are this client's declaration of what it consumes; the arm above
  // engages by itself, per name, the moment the document publishes one.
  assert.ok(assumed.length + pinned === Object.keys(D3_SHAPES).length);
  assert.ok(Object.keys(D3_SHAPES).length >= 30,
    "every D3 response DTO, list envelope and request body this client builds is declared");
  for (const name of Object.keys(D3_ENUMS)) {
    assert.ok(Array.isArray(D3_ENUMS[name]) && D3_ENUMS[name].length > 0,
      name + " is a closed, non-empty vocabulary");
    const published = definitions[name];
    if (published !== undefined && Array.isArray(published.enum)) {
      assert.deepEqual(D3_ENUMS[name].slice().sort(), published.enum.slice().sort(), name);
    }
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
