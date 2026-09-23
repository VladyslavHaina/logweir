// PLAT-10.2's schedule detail, MOUNTED: what a successful action does to the
// page it was taken on.
//
// WHY THESE ROWS DRIVE THE REAL WIRING. Every row in `schedules-detail.spec.js`
// calls a pure renderer, and the live harness reloaded the page after each
// click -- so a success arm that re-mounted the namespace LIST into the detail
// (review HIGH-1, 2026-09-22) passed both. These rows mount
// `mountScheduleDetail` over a stubbed adapter, press the control, and assert
// the page is still the detail of the same schedule, re-read.
//
// Run with: node --test 'ui/tests/*.spec.js'

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  CATALOG_VIEW_EXPIRED,
  NOT_IN_CATALOG,
  OUTSIDE_CATALOG_VIEW,
  mountScheduleDetail,
} from "../pages/schedules.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const clone = (value) => JSON.parse(JSON.stringify(value));
const settle = () => new Promise((resolve) => setTimeout(resolve, 20));

/** The d1 policy fixture (`tz`), placed in a namespace of the row's own so no
 *  two rows share a mutation record. */
function scheduleIn(ns) {
  const object = clone(fixture("schedule-policy.json"));
  object.metadata.namespace = ns;
  object.metadata.uid = "uid-tz-" + ns;
  object.spec.suspend = false;
  return object;
}

/** A finished run of `tz` that a restore plan can be built from. */
function runOf(schedule, name, backupId) {
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "Backup",
    metadata: { name: name, namespace: schedule.metadata.namespace, uid: "uid-" + name,
      creationTimestamp: "2026-09-20T02:30:00Z" },
    spec: {
      scheduleRef: { name: "tz", uid: schedule.metadata.uid, generation: 1 },
      slot: "20260920-023000", sourceRef: { name: "source" }, topics: ["orders"],
      trigger: { kind: "Manual", attempt: 0 }, triggeredBy: "admin",
    },
    status: { phase: "Succeeded", backupId: backupId, records: 3,
      windowCovered: { fromMs: 1789196400000, toMs: 1789200000000 } },
  };
}

/** The detail over a stub adapter whose answers come from `state`, so a
 *  re-read sees what the API server now holds. `calls` counts every read. */
function detailPage(ns, state, overrides, catalog) {
  const view = fakeView();
  const calls = { get: 0, backups: 0, scheduleList: 0 };
  const points = catalog || (async () => ({ items: [], page: { nextCursor: null },
    truncated: false, viewExpired: false }));
  const api = Object.assign({
    get(namespace, plural, name) {
      calls.get += 1;
      assert.equal(name, "tz");
      return Promise.resolve(clone(state.schedule));
    },
    list(namespace, plural) {
      if (plural === "backups") {
        calls.backups += 1;
        return Promise.resolve({ items: clone(state.backups) });
      }
      if (plural === "backupschedules") {
        calls.scheduleList += 1;
        return Promise.resolve({ items: [clone(state.schedule)] });
      }
      return Promise.resolve({ items: [] });
    },
    // THE DESTINATION THE FIXTURE'S `destinationRef` NAMES (`dest`). An empty
    // list beside a schedule that names one is a destination that is gone, and
    // since PLAT-08.2 review L5 a policy save then asks for the location-move
    // box, as it always did beside a non-empty list without it.
    destinations() {
      return Promise.resolve({
        items: [Object.assign(fixture("console/destination.json").item, { name: "dest" })],
      });
    },
    detailReaders: {
      listCatalogs: async () => ({ items: [{ metadata: { name: "primary" } }] }),
      readPoints: points,
      listRetention: async () => ({ items: [] }),
    },
  }, overrides || {});
  return { view: view, api: api, calls: calls };
}

function assertStillTheDetail(page, label) {
  const html = page.view.html();
  assert.match(html, /id="schedule-detail"/, label + ": the detail is gone");
  assert.match(html, /data-schedule-detail="tz"/, label + ": the detail is not tz's");
  assert.doesNotMatch(html, /<h2>Schedules<\/h2>/,
    label + ": the namespace list was mounted into the detail");
  assert.equal(page.calls.scheduleList, 0, label + ": the list was read");
}

test("a_successful_pause_on_the_detail_re_reads_the_detail_not_the_list", async () => {
  const ns = "detail-pause";
  const state = { schedule: scheduleIn(ns), backups: [] };
  const page = detailPage(ns, state, {
    patchSuspend(namespace, name, next) {
      state.schedule.spec.suspend = next;
      return Promise.resolve(clone(state.schedule));
    },
  });
  await mountScheduleDetail(page.view.root, ns, "tz", parse, LIFE(), page.api);
  assertStillTheDetail(page, "before the click");
  assert.equal(page.calls.get, 1);
  assert.match(page.view.html(), /not suspended/);

  const toggle = page.view.find("form.suspend");
  assert.ok(toggle !== null, "the detail offers the suspend toggle");
  await toggle.dispatch("submit");
  await settle();

  assert.equal(state.schedule.spec.suspend, true, "the pause reached the API");
  assertStillTheDetail(page, "after the pause");
  assert.equal(page.calls.get, 2, "the detail RE-READ the schedule after the pause");
  assert.match(page.view.html(), /data-next="false"><button type="submit">Resume</,
    "and it shows the stored, suspended state: the toggle now offers Resume");
});

test("a_successful_resume_on_the_detail_re_reads_the_detail_not_the_list", async () => {
  const ns = "detail-resume";
  const state = { schedule: scheduleIn(ns), backups: [] };
  state.schedule.spec.suspend = true;
  const page = detailPage(ns, state, {
    patchSuspend(namespace, name, next) {
      state.schedule.spec.suspend = next;
      return Promise.resolve(clone(state.schedule));
    },
  });
  await mountScheduleDetail(page.view.root, ns, "tz", parse, LIFE(), page.api);
  await page.view.find("form.suspend").dispatch("submit");
  await settle();
  assert.equal(state.schedule.spec.suspend, false);
  assertStillTheDetail(page, "after the resume");
  assert.equal(page.calls.get, 2);
  assert.match(page.view.html(), /data-next="true"><button type="submit">Suspend</,
    "the resumed schedule's toggle offers Suspend again");
});

test("a_successful_policy_save_on_the_detail_re_reads_the_detail_at_the_new_revision", async () => {
  const ns = "detail-policy";
  const state = { schedule: scheduleIn(ns), backups: [] };
  const page = detailPage(ns, state, {
    editSchedulePolicy() {
      state.schedule.metadata.generation = 4;
      state.schedule.spec.schedule = "15 4 * * *";
      state.schedule.status.observedGeneration = 4;
      state.schedule.status.policy.generation = 4;
      return Promise.resolve(clone(state.schedule));
    },
  });
  await mountScheduleDetail(page.view.root, ns, "tz", parse, LIFE(), page.api);
  assert.match(page.view.html(), /data-editing-generation="1"/);
  const form = page.view.find("form.policy-form[data-name=\"tz\"]");
  assert.ok(form !== null, "the detail offers the policy form");
  form.elements.cron.value = "15 4 * * *";
  await form.dispatch("submit");
  await settle();

  assertStillTheDetail(page, "after the save");
  assert.equal(page.calls.get, 2, "the detail RE-READ the schedule after the save");
  const html = page.view.html();
  assert.match(html, /data-editing-generation="4"/, "the form reopens at the stored revision");
  assert.match(html, /class="revision" data-generation="4"/, "the revision line is g4");
  assert.doesNotMatch(html, /data-editing-generation="1"/, "the superseded revision is gone");
  assert.match(html, /15 4 \* \* \*/);
});

test("back_up_now_on_the_detail_re_reads_the_history_and_keeps_the_run_on_screen", async () => {
  // Review LOW-5: the run-now panel repainted and the history did not, so the
  // run it had just made was not in the table until the next page load.
  const ns = "detail-run-now";
  const state = { schedule: scheduleIn(ns), backups: [] };
  const answer = fixture("console/manual-backup.json");
  const page = detailPage(ns, state, {
    runBackupNow(namespace) {
      const made = runOf(state.schedule, "logweir-manual-1", "set-new");
      made.status = { phase: "Pending" };
      state.backups.push(made);
      return Promise.resolve({
        replayed: false, schedule: answer.schedule,
        run: { metadata: { name: "logweir-manual-1", namespace: namespace },
          spec: { trigger: { kind: "Manual", attempt: 0 },
            scheduleRef: answer.item.scheduleRef } },
      });
    },
  });
  await mountScheduleDetail(page.view.root, ns, "tz", parse, LIFE(), page.api);
  assert.equal(page.calls.backups, 1);
  await page.view.find("form.run-now-form[data-name=\"tz\"]").dispatch("submit");
  await settle();

  assertStillTheDetail(page, "after Back up now");
  assert.equal(page.calls.backups, 2, "the history was RE-READ after the run was created");
  const html = page.view.html();
  const history = html.slice(html.indexOf("id=\"schedule-history\""));
  assert.match(history, /logweir-manual-1/, "the new run is in the history");
  assert.match(html, /Follow this run/i, "and the panel still shows the run it made");
  // ONE re-read per result: the re-mounted panel does not re-enter the arm.
  await settle();
  assert.equal(page.calls.backups, 2, "the remount did not loop");
});

test("an_expired_catalog_view_reaches_the_mounted_detail_as_expired_not_absent", async () => {
  // Review HIGH-2, through the real mount: the page's own reader receives the
  // view's `viewExpired` and the history says so.
  const ns = "detail-expired";
  const state = { schedule: scheduleIn(ns), backups: [] };
  state.backups.push(runOf(state.schedule, "run-a", "set-a"));
  const page = detailPage(ns, state, {}, async () => ({
    items: [], page: { nextCursor: null }, truncated: false, viewExpired: true,
  }));
  await mountScheduleDetail(page.view.root, ns, "tz", parse, LIFE(), page.api);
  const html = page.view.html();
  assert.match(html, /id="schedule-catalog-expired"/);
  assert.ok(html.indexOf("\">" + CATALOG_VIEW_EXPIRED + "<") !== -1,
    "the unlisted run's cells say catalog view expired");
  assert.ok(html.indexOf("\">" + NOT_IN_CATALOG + "<") === -1,
    "an aged-out view made an absence claim about the archive");

  // NEGATIVE CONTROL: a complete, current view that does not list the run DOES
  // say not in the catalog -- the word above is the flag's doing.
  const fresh = detailPage("detail-fresh", {
    schedule: scheduleIn("detail-fresh"),
    backups: [runOf(scheduleIn("detail-fresh"), "run-a", "set-a")],
  });
  await mountScheduleDetail(fresh.view.root, "detail-fresh", "tz", parse, LIFE(), fresh.api);
  const freshHtml = fresh.view.html();
  assert.ok(freshHtml.indexOf("\">" + NOT_IN_CATALOG + "<") !== -1);
  assert.ok(freshHtml.indexOf("\">" + CATALOG_VIEW_EXPIRED + "<") === -1);
  assert.ok(freshHtml.indexOf("\">" + OUTSIDE_CATALOG_VIEW + "<") === -1);
});
