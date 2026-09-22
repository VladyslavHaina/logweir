// schedules-detail.spec.js -- PLAT-10.2: one schedule's detail and its
// recovery-point history.
//
// THE TRACKER'S TESTS, ONE ROW EACH: empty history, running/failed/verified
// runs, a paused schedule, an archived (deleted) schedule and navigation to an
// older backup -- against the acceptance "actions retain schedule and
// recovery-point context; unavailable archives and incomplete evidence are
// distinguishable from healthy points".
//
// EVERY ROW CARRIES ITS NEGATIVE CONTROL. The two verdict columns are the ones
// worth being careful about: a row that asserts `Available` renders green is
// worth nothing unless the same render, with a `Missing` point, does NOT -- and
// unless a run the catalog has never seen is distinguishable from both.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  ARCHIVED_SCHEDULE_SENTENCE,
  CATALOG_UNREADABLE_SENTENCE,
  NOT_IN_CATALOG,
  NO_HISTORY_SENTENCE,
  NO_POINTS_SENTENCE,
  TWO_VERDICTS_SENTENCE,
  pointsForRun,
  readSchedulePoints,
  renderArchivedSchedule,
  renderEarlierRuns,
  renderLatestPointAction,
  renderScheduleDetail,
  renderScheduleHistory,
  runsOfSchedule,
  scheduleDetailRoute,
  verdictCells,
} from "../pages/schedules.js";
import { parseHash } from "../app.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

const NS = "team-a";
const SCHEDULE_UID = "8a3d1b02-0000-4000-8000-000000000001";

function schedule(extra) {
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "BackupSchedule",
    metadata: {
      name: "nightly", namespace: NS, uid: SCHEDULE_UID, generation: 3,
      creationTimestamp: "2026-09-01T00:00:00Z",
    },
    spec: Object.assign({
      schedule: "30 2 * * *",
      sourceRef: { name: "orders-prod" },
      topics: ["orders"],
      destinationRef: { name: "primary" },
      archive: { url: "logweir-destination://primary" },
      suspend: false,
    }, (extra || {}).spec || {}),
    status: Object.assign({
      observedGeneration: 3,
      policy: {
        generation: 3, runPolicySha256: "sha256:aa", timeZone: "Europe/Berlin",
        tzdb: "2026a", effectiveSince: "2026-09-01T00:00:00Z",
        evaluatedAt: "2026-09-20T02:30:00Z",
      },
    }, (extra || {}).status || {}),
  };
}

/** A run of `nightly`, in whichever state the caller asks for. */
function run(name, phase, extra) {
  const e = extra || {};
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: {
      name: name, namespace: NS, uid: "uid-" + name,
      creationTimestamp: e.createdAt || "2026-09-20T02:30:00Z",
    },
    spec: {
      scheduleRef: {
        name: "nightly",
        uid: e.scheduleUid === undefined ? SCHEDULE_UID : e.scheduleUid,
        generation: 3,
      },
      slot: e.slot || "20260920-023000",
      sourceRef: { name: "orders-prod" },
      topics: ["orders"],
      trigger: { kind: "Scheduled", attempt: 0 },
      triggeredBy: "schedule",
    },
    status: Object.assign({
      phase: phase,
      backupId: e.backupId === undefined ? "set-" + name : e.backupId,
    }, phase === "Succeeded"
      ? { windowCovered: { fromMs: 1789196400000, toMs: 1789200000000 }, records: 10 }
      : {}),
  };
}

/** The catalog's view entries, in each of the states D3 section 5.4 keeps apart. */
const POINTS = Object.freeze([
  Object.freeze({
    backupId: "set-healthy", pointId: "lwp1-healthy", runId: "r1",
    availability: "Available", verification: "Verified", selectable: true,
    receiptKey: "k", receiptSha256: "sha256:aa",
  }),
  Object.freeze({
    backupId: "set-gone", pointId: "lwp1-gone", runId: "r2",
    availability: "Missing", verification: "Verified", selectable: false,
    receiptKey: "k", receiptSha256: "sha256:bb",
    remedy: "the object is not at any recorded location",
  }),
  Object.freeze({
    backupId: "set-unsigned", pointId: "lwp1-unsigned", runId: "r3",
    availability: "Available", verification: "NotAttempted", selectable: false,
    receiptKey: "k", receiptSha256: "sha256:cc",
  }),
  Object.freeze({
    backupId: "set-stranger", pointId: "lwp1-stranger", runId: "r4",
    availability: "Available", verification: "UntrustedSigner", selectable: false,
    receiptKey: "k", receiptSha256: "sha256:dd",
    signerKeyId: "2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe",
  }),
]);

// ===========================================================================
// Empty history
// ===========================================================================

test("a_schedule_that_has_never_fired_says_so_and_offers_no_restore", () => {
  const html = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(), runs: { mine: [], earlier: [] }, points: [],
  });
  assert.match(html, /data-schedule-detail="nightly"/);
  assert.ok(html.indexOf(NO_HISTORY_SENTENCE) !== -1, "the empty history says what empty means");
  assert.ok(html.indexOf(NO_POINTS_SENTENCE) !== -1, "and there is no latest point to restore");
  assert.doesNotMatch(html, /id="schedule-restore-latest"/);
  // AN EMPTY HISTORY IS NOT A REMOVED ONE, and the sentence is what keeps the
  // two apart for a reader who has just deleted something.
  assert.match(NO_HISTORY_SENTENCE, /runs outlive the schedule that made them/);

  // NEGATIVE CONTROL: the same detail with one finished run has neither
  // sentence and DOES offer the restore, so the row above is about the runs and
  // not about a page that always says "none".
  const withRun = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(),
    runs: { mine: [run("nightly-1", "Succeeded", { backupId: "set-healthy" })], earlier: [] },
    points: POINTS,
  });
  assert.ok(withRun.indexOf(NO_HISTORY_SENTENCE) === -1);
  assert.match(withRun, /id="schedule-restore-latest"/);
});

// ===========================================================================
// Running, failed and verified runs -- and the two verdict columns
// ===========================================================================

test("an_unavailable_archive_and_incomplete_evidence_are_not_rendered_as_healthy_points", () => {
  const runs = [
    run("nightly-healthy", "Succeeded", { backupId: "set-healthy" }),
    run("nightly-gone", "Succeeded", { backupId: "set-gone" }),
    run("nightly-unsigned", "Succeeded", { backupId: "set-unsigned" }),
    run("nightly-stranger", "Succeeded", { backupId: "set-stranger" }),
  ];
  const html = renderScheduleHistory(NS, schedule(), runs, POINTS, null);
  // EVERY WORD IS THE CATALOG'S OWN, rendered rather than translated.
  for (const word of ["Available", "Missing", "Verified", "NotAttempted", "UntrustedSigner"]) {
    assert.ok(html.indexOf(">" + word + "<") !== -1, word + " is on screen verbatim");
  }
  // AND ONLY THE HEALTHY ONE IS GREEN. `selectable` is the catalog's own
  // conjunction; three of these four points are not restorable and exactly one
  // badge says otherwise.
  assert.equal((html.match(/badge-green">Verified</g) || []).length, 1,
    "one verified-and-available point is green");
  assert.equal((html.match(/badge-green">Available</g) || []).length, 3,
    "availability is its own axis: three of the four are still readable");
  assert.equal((html.match(/badge-green">Missing</g) || []).length, 0);
  assert.equal((html.match(/badge-green">NotAttempted</g) || []).length, 0,
    "evidence nobody attempted to verify is never a green verification");
  assert.equal((html.match(/badge-green">UntrustedSigner</g) || []).length, 0,
    "a stranger's signature is never a green verification");

  // NEGATIVE CONTROL: the verdicts come from the POINT and not from the run.
  // The same four runs against a catalog that has none of them render neither
  // word, and are not green either.
  const unknown = renderScheduleHistory(NS, schedule(), runs, [], null);
  assert.equal((unknown.match(/badge-green/g) || []).length, 0,
    "a run the catalog has never seen is not green on either axis");
  assert.equal((unknown.match(new RegExp(NOT_IN_CATALOG, "g")) || []).length, 8,
    "two columns per run say so by name rather than being blank");
});

test("a_running_run_and_a_failed_run_are_listed_and_offer_no_restore", () => {
  const runs = [
    run("nightly-running", "Running", { backupId: "" }),
    run("nightly-failed", "Failed", { backupId: "set-failed" }),
    run("nightly-ok", "Succeeded", { backupId: "set-healthy" }),
  ];
  const html = renderScheduleHistory(NS, schedule(), runs, POINTS, null);
  assert.match(html, /nightly-running/, "a run in flight is part of the history");
  assert.match(html, /nightly-failed/, "and so is one that failed");
  // EXACTLY ONE RESTORE LINK: the one run a plan can be built from.
  assert.equal((html.match(/Restore this point/g) || []).length, 1);
  assert.match(html, /#\/restore\?ns=team-a&amp;backup=nightly-ok&amp;uid=uid-nightly-ok/,
    "and it is bound to that run by uid, which is PLAT-11.1's identity");

  // NEGATIVE CONTROL: a table of only the good rows would agree with itself and
  // disagree with the cluster, so the two unrestorable runs must still be here.
  assert.equal((html.match(/<tr>/g) || []).length, 4, "three rows and the header");
});

test("a_catalog_that_cannot_be_read_says_so_instead_of_leaving_a_blank_column", () => {
  const error = new Error("A recovery catalog's point list is served by the Logweir product API");
  error.kind = "rejected";
  error.status = 501;
  error.reason = "NoConsoleRoute";
  const html = renderScheduleHistory(
    NS, schedule(), [run("nightly-ok", "Succeeded", { backupId: "set-healthy" })], [], error,
  );
  assert.match(html, /id="schedule-catalog-unreadable"/);
  assert.ok(html.indexOf(CATALOG_UNREADABLE_SENTENCE) !== -1);
  assert.ok(html.indexOf("NoConsoleRoute") !== -1 ||
    html.indexOf("served by the Logweir product API") !== -1,
    "the refusal's own words are shown: " + html.slice(0, 200));
  assert.equal((html.match(/badge-green/g) || []).length, 0,
    "an unreadable catalog never reports an archive as readable");

  // NEGATIVE CONTROL: with no error the sentence is absent, so it is a report
  // about this read and not a permanent disclaimer.
  const fine = renderScheduleHistory(
    NS, schedule(), [run("nightly-ok", "Succeeded", { backupId: "set-healthy" })], POINTS, null,
  );
  assert.doesNotMatch(fine, /id="schedule-catalog-unreadable"/);
  assert.ok(fine.indexOf(TWO_VERDICTS_SENTENCE) !== -1,
    "the two-axis sentence is always there; the refusal is not");
});

test("schedule history follows catalog cursors and marks mixed reads incomplete", async () => {
  const calls = [];
  const result = await readSchedulePoints(null, NS, null, {
    listCatalogs: async () => ({ items: [{ metadata: { name: "primary" } }, { metadata: { name: "broken" } }] }),
    readPoints: async (name, query) => {
      calls.push([name, query]);
      if (name === "broken") {
        throw Object.assign(new Error("catalog endpoint refused"), { reason: "Forbidden" });
      }
      if (query.cursor === undefined) {
        return { items: [{ backupId: "set-healthy", availability: "Available", verification: "Verified", selectable: true }],
          page: { nextCursor: "next-page" } };
      }
      return { items: [{ backupId: "set-gone", availability: "Missing", verification: "NotAttempted", selectable: false }],
        page: { nextCursor: null } };
    },
  });
  assert.deepEqual(calls.slice(0, 2), [
    ["primary", { limit: 200 }], ["primary", { limit: 200, cursor: "next-page" }],
  ]);
  assert.equal(result.points.length, 2, "both pages are joined before the history renders");
  assert.equal(result.error.reason, "Forbidden", "a failed second catalog is never hidden by a successful first one");
  const html = renderScheduleHistory(NS, schedule(), [
    run("known", "Succeeded", { backupId: "set-healthy" }),
    run("not-read", "Succeeded", { backupId: "set-never-read" }),
  ], result.points, result.error);
  assert.match(html, /Available/);
  assert.match(html, /catalog incomplete/);
  assert.doesNotMatch(html, /not in the catalog/, "a partial catalog read cannot make an absence claim");
});

test("the_join_is_on_the_backup_set_id_and_takes_every_point_of_the_set", () => {
  const ok = run("nightly-ok", "Succeeded", { backupId: "set-healthy" });
  assert.deepEqual(pointsForRun(ok, POINTS).map((p) => p.pointId), ["lwp1-healthy"]);
  // NEGATIVE CONTROL 1: a run with no backup set id joins to nothing, rather
  // than to the first point in the list.
  assert.deepEqual(pointsForRun(run("x", "Running", { backupId: "" }), POINTS), []);
  assert.deepEqual(pointsForRun(ok, []), []);
  assert.deepEqual(pointsForRun(null, POINTS), []);
  // And the two cells for "no point" are the named state, not empty strings.
  const cells = verdictCells([]);
  assert.equal(cells.length, 2);
  assert.ok(cells[0].indexOf(NOT_IN_CATALOG) !== -1);
  assert.ok(cells[0].indexOf("badge-green") === -1);

  // NEGATIVE CONTROL 2: A SET IS NOT ITS FIRST POINT. Two points under one
  // backup set id, the first healthy and the second missing, is not a healthy
  // set -- and taking the first would have said it was.
  const mixed = [
    { backupId: "set-mixed", pointId: "p1", availability: "Available",
      verification: "Verified", selectable: true },
    { backupId: "set-mixed", pointId: "p2", availability: "Missing",
      verification: "Verified", selectable: false },
  ];
  const verdicts = verdictCells(mixed);
  assert.ok(verdicts[0].indexOf("Available") !== -1 && verdicts[0].indexOf("Missing") !== -1,
    "both words are shown rather than a severity this page invented");
  assert.equal((verdicts[0].match(/badge-green/g) || []).length, 0,
    "a set with a missing point is not a readable set");
  assert.equal((verdicts[1].match(/badge-green/g) || []).length, 0,
    "and not a restorable one, because `selectable` is false for one of its points");
});

// ===========================================================================
// Paused schedule
// ===========================================================================

test("a_paused_schedule_says_it_is_paused_and_still_offers_its_history", () => {
  const paused = schedule({ spec: { suspend: true } });
  const runs = { mine: [run("nightly-ok", "Succeeded", { backupId: "set-healthy" })], earlier: [] };
  const html = renderScheduleDetail({
    ns: NS, name: "nightly", object: paused, runs: runs, points: POINTS,
    extra: { cards: { nightly: {} }, mayOperate: true },
  });
  assert.match(html, /suspended/, "the suspend state is on the page");
  // THE RESUME CONTROL IS THE ONE THE LIST HAS, bound to this schedule.
  assert.match(html, /<form class="suspend"[^>]*data-name="nightly"/);
  // A PAUSED SCHEDULE'S HISTORY IS STILL ITS HISTORY, and its points are still
  // restorable: pausing stops future slots and touches nothing that exists.
  assert.match(html, /Restore this point/);
  assert.match(html, /id="schedule-restore-latest"/);

  // NEGATIVE CONTROL: the same schedule not suspended renders the other control
  // and not the word, so the row reads `spec.suspend` rather than a constant.
  const running = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(), runs: runs, points: POINTS,
    extra: { cards: { nightly: {} }, mayOperate: true },
  });
  assert.notEqual(
    (html.match(/Resume/g) || []).length,
    (running.match(/Resume/g) || []).length,
    "the toggle's caption differs between the two states",
  );
});

test("a_read_only_viewer_gets_schedule_facts_without_mutation_controls", () => {
  const complete = run("nightly-complete", "Succeeded", {
    backupId: "set-healthy", createdAt: "2026-09-20T02:00:00Z",
  });
  complete.status.conditions = [{ type: "Complete", status: "True",
    lastTransitionTime: "2026-09-20T02:30:00Z" }];
  const html = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(), runs: { mine: [complete], earlier: [] },
    points: POINTS, now: "2026-09-20T04:30:00Z",
    extra: { cards: { nightly: {} }, destinations: [{ name: "primary", canonicalUrl: "s3://b/p" }], mayOperate: false },
  });
  for (const fact of ["Schedule facts", "orders-prod", "s3://b/p", "Policy revision",
    "nightly-complete", "2026-09-20T02:30:00Z", "2 hours"]) {
    assert.ok(html.indexOf(fact) !== -1, fact + " is available to a read-only viewer");
  }
  assert.doesNotMatch(html, /<form class="suspend"/);
  assert.doesNotMatch(html, /class="run-now-form"/);
  assert.match(html, /data-suspend-read-only="1"/);
  // NEGATIVE CONTROL: an operator sees the mutating controls; the read-only
  // view did not merely render a broken action panel.
  const operator = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(), runs: { mine: [complete], earlier: [] },
    points: POINTS, extra: { cards: { nightly: {} }, mayOperate: true },
  });
  assert.match(operator, /<form class="suspend"/);
});

// ===========================================================================
// Archived schedule: deleted, with its history retained (PLAT-05.2)
// ===========================================================================

test("a_deleted_schedule_keeps_its_history_and_says_the_schedule_is_gone", () => {
  const runs = {
    mine: [run("nightly-ok", "Succeeded", { backupId: "set-healthy", scheduleUid: "old-uid-a" })],
    earlier: [run("nightly-gone", "Succeeded", { backupId: "set-gone", scheduleUid: "old-uid-b" })],
  };
  const html = renderScheduleDetail({
    ns: NS, name: "nightly", object: null, runs: runs, points: POINTS,
  });
  assert.match(html, /data-archived="1"/);
  assert.ok(html.indexOf(ARCHIVED_SCHEDULE_SENTENCE) !== -1);
  assert.match(ARCHIVED_SCHEDULE_SENTENCE, /removes nothing else/);
  assert.match(ARCHIVED_SCHEDULE_SENTENCE, /a DIFFERENT schedule/);
  // THE HISTORY AND THE RESTORES ARE STILL THERE.
  assert.match(html, /nightly-ok/);
  assert.equal((html.match(/Restore this point/g) || []).length, 2);
  assert.doesNotMatch(html, /id="schedule-restore-latest"/,
    "a deleted name has no single latest point across historical UIDs");
  assert.match(html, /data-archived-schedule-uid="old-uid-a"/);
  assert.match(html, /data-archived-schedule-uid="old-uid-b"/);
  assert.equal((html.match(/Runs and recovery points/g) || []).length, 2,
    "the two historical identities are rendered separately, never merged by name");
  // AND THE ACTIONS THAT NEED A SCHEDULE ARE NOT OFFERED: there is nothing to
  // suspend, nothing to edit and nothing to run.
  assert.doesNotMatch(html, /<form class="suspend"/);
  assert.doesNotMatch(html, /class="policy-form"/);
  assert.doesNotMatch(html, /class="run-now-form"/);

  // NEGATIVE CONTROL: the live schedule DOES offer all three, so their absence
  // above is the archived state and not a renderer that never emits them.
  const live = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(), runs: runs, points: POINTS,
    extra: { cards: { nightly: {} }, mayOperate: true },
  });
  assert.match(live, /<form class="suspend"/);
  assert.match(live, /class="policy-form"/);
  assert.match(live, /class="run-now-form"/);
  assert.doesNotMatch(live, /data-archived="1"/);
});

test("a_recreated_schedule_does_not_inherit_the_previous_ones_runs", () => {
  // PLAT-05.2: a schedule deleted and recreated under the same name is a
  // DIFFERENT schedule. Its runs carry the old uid in `spec.scheduleRef.uid`,
  // and counting them as this schedule's would attribute one policy's
  // protection to another.
  const backups = {
    items: [
      run("new-1", "Succeeded", { backupId: "set-healthy" }),
      run("old-1", "Succeeded", { backupId: "set-gone", scheduleUid: "a-previous-uid" }),
    ],
  };
  const split = runsOfSchedule("nightly", SCHEDULE_UID, backups);
  assert.deepEqual(split.mine.map((r) => r.metadata.name), ["new-1"]);
  assert.deepEqual(split.earlier.map((r) => r.metadata.name), ["old-1"]);
  const html = renderEarlierRuns(NS, split.earlier);
  assert.match(html, /id="schedule-earlier-runs"/);
  assert.match(html, /a-previous-uid/);
  assert.match(html, /Restore this point/, "the earlier schedule's points are still restorable");

  // NEGATIVE CONTROL 1: with no earlier runs the section is not rendered at
  // all, so its presence above means something.
  assert.equal(renderEarlierRuns(NS, []), "");
  // NEGATIVE CONTROL 2: a run with NO schedule uid at all -- what every run
  // created before the uid label existed looks like -- is counted as this
  // schedule's rather than exiled, because there is nothing to tell them apart
  // by and the name is what the controller wrote.
  const legacy = runsOfSchedule("nightly", SCHEDULE_UID, {
    items: [run("legacy-1", "Succeeded", { backupId: "set-healthy", scheduleUid: "" })],
  });
  assert.deepEqual(legacy.mine.map((r) => r.metadata.name), ["legacy-1"]);
  assert.deepEqual(legacy.earlier, []);
});

// ===========================================================================
// Navigation: the deep link, and an older backup
// ===========================================================================

test("the_deep_link_reaches_one_schedule_and_the_old_list_link_still_reaches_the_list", () => {
  // MIGRATION (10.2's note): "keep existing deep links working or redirect them
  // explicitly". `#/schedules?ns=<ns>` was the only schedules link there had
  // ever been, and the router sends a hash with no `name` to the list exactly as
  // it did before -- so there is nothing to redirect, and this row is what says
  // so for the next reader.
  const list = parseHash("#/schedules?ns=team-a", "");
  assert.equal(list.route, "#/schedules");
  assert.equal(list.ns, "team-a");
  assert.equal(list.name, "", "no name means the list, which is the old behaviour");

  const detail = parseHash(scheduleDetailRoute("team-a", "nightly"), "");
  assert.equal(detail.route, "#/schedules");
  assert.equal(detail.ns, "team-a");
  assert.equal(detail.name, "nightly");

  // NEGATIVE CONTROL: a hash carrying an EMPTY name is still the list, so a
  // malformed link cannot land on a detail page for a schedule with no name.
  assert.equal(parseHash("#/schedules?ns=team-a&name=", "").name, "");
});

test("an_older_point_is_reachable_and_each_action_carries_its_own_point", () => {
  // 10.2's "navigation to an older backup": the history is newest first, every
  // row's restore is bound to THAT row, and the page-level Restore is bound to
  // the newest -- so choosing an older one is a different link and not the same
  // link with a different table row highlighted.
  const newest = run("nightly-newest", "Succeeded",
    { backupId: "set-healthy", createdAt: "2026-09-20T02:30:00Z" });
  const older = run("nightly-older", "Succeeded",
    { backupId: "set-unsigned", createdAt: "2026-09-18T02:30:00Z" });
  const runs = { mine: [newest, older], earlier: [] };
  const html = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(), runs: runs, points: POINTS,
    extra: { cards: { nightly: {} }, mayOperate: true },
  });
  const latest = renderLatestPointAction(NS, runs.mine);
  assert.match(latest, /uid=uid-nightly-newest/, "the page-level Restore takes the newest point");
  assert.ok(html.indexOf("uid=uid-nightly-older") !== -1,
    "and the older point has a link of its own");
  assert.ok(html.indexOf("uid=uid-nightly-newest") !== -1);

  // NEGATIVE CONTROL: the two links are NOT the same string. A "restore" that
  // ignored which row it was on would render one identity twice.
  assert.notEqual(
    (html.match(/uid=uid-nightly-older/g) || []).length,
    0,
    "the older point's identity is in the markup",
  );
  const newestLinks = (html.match(/uid=uid-nightly-newest/g) || []).length;
  const olderLinks = (html.match(/uid=uid-nightly-older/g) || []).length;
  assert.ok(newestLinks >= 2 && olderLinks >= 1,
    "the newest appears as the page action and as its own row; the older only as its row");
});

test("every_action_on_the_detail_names_the_schedule_it_acts_on", () => {
  // 10.2's acceptance clause one: "actions retain schedule and recovery-point
  // context". Each control carries the schedule's name in the attribute the
  // wiring binds by, so a page with two schedules' panels could never wire one
  // schedule's button to another's record.
  const html = renderScheduleDetail({
    ns: NS, name: "nightly", object: schedule(),
    runs: { mine: [run("nightly-ok", "Succeeded", { backupId: "set-healthy" })], earlier: [] },
    points: POINTS, extra: { cards: { nightly: {} }, mayOperate: true },
  });
  for (const attribute of ["data-name=\"nightly\"", "data-run-now=\"nightly\"",
    "data-policy=\"nightly\"", "data-suspend-status=\"nightly\""]) {
    assert.ok(html.indexOf(attribute) !== -1, attribute + " binds the control to this schedule");
  }
  // And the crumb goes back to the list for THIS namespace.
  assert.match(html, /<a href="#\/schedules\?ns=team-a">All schedules<\/a>/);

  // NEGATIVE CONTROL: the archived view has no such controls to bind, and says
  // so rather than rendering dead ones.
  const archived = renderArchivedSchedule(NS, "nightly", { mine: [], earlier: [] }, [], null);
  assert.doesNotMatch(archived, /data-run-now="nightly"/);
  assert.doesNotMatch(archived, /data-policy="nightly"/);
  assert.ok(archived.indexOf(NO_HISTORY_SENTENCE) !== -1);
});

test("the_catalog_fixture_the_console_ships_renders_through_these_columns", () => {
  // A FIXTURE TWO SIDES AGREE ON IS READ BY BOTH SIDES. These are the same
  // point documents `ui/tests/fixtures/console/catalog-points-states.json`
  // carries for the catalog page, so the two surfaces cannot disagree about
  // what `Missing` or `NotAttempted` looks like.
  const points = fixture("console/catalog-points-states.json").items;
  assert.ok(points.length > 0);
  const runs = points.map((point, i) =>
    run("fixture-" + String(i), "Succeeded", { backupId: point.backupId }));
  const html = renderScheduleHistory(NS, schedule(), runs, points, null);
  for (const point of points) {
    assert.ok(html.indexOf(">" + point.availability + "<") !== -1,
      point.availability + " renders");
    assert.ok(html.indexOf(">" + point.verification + "<") !== -1,
      point.verification + " renders");
  }
  // AND THE FIXTURE'S OWN SHAPE IS THE POINT OF THIS ROW: five of its six
  // entries share one `backupId`, so the two runs above are a set of five and a
  // set of one -- and the set of five holds `Conflict`, `Deleted` and
  // `UntrustedSigner` beside two `Available`/`Verified` points. Nothing about
  // it may read green.
  const bySet = {};
  for (const point of points) {
    bySet[point.backupId] = (bySet[point.backupId] || 0) + 1;
  }
  assert.ok(Object.keys(bySet).length < points.length,
    "the fixture really does carry several points under one backup set id");
  assert.equal((html.match(/badge-green/g) || []).length, 0,
    "no set in this fixture is wholly available and wholly selectable, so nothing is green");
  assert.match(html, /5 points/, "and the multi-point set says how many points it holds");
});
