// reading-place.spec.js -- poc-upgrade-4's P16 and its class: a repaint that
// lands while a check is still running must not throw the reader's place away.
//
// WHAT THE LIVE ROUND SAW (poc-upgrade-4, 16:56Z, `console/p15/DISC_R3.log`).
// Restore step 5 at 390 x 844: after Check readiness, `keepStatusInView` put
// the focused status at 661-716 px (scrollY 282). At the first follow read,
// pending -> running, step 5 was repainted and the page jumped to scrollY 0:
// the status sat at 943-998 px, below the 844 px viewport, until the verdict.
//
// WHY. Every page repaints through `render.js`'s `replace`, which empties the
// view and fills it again. Chromium moved the page during that swap -- over
// the real console it took 282 px to 0 and 336 px to 38 inside the one call
// (a browser measure, `claude/artifacts/poc-fixes-6/`) -- and the follow's
// `show` had nothing after its repaint that put the reader back; the click's
// listener did (`keepStatusInView`), which is why the click passed live.
//
// THE FAKES CANNOT LAY A PAGE OUT, SO EACH ROW GIVES THEM THE PART IT NEEDS.
// The `render.js` rows run in `fake-dom.js` with a window and element rects
// written by the row: a document position per element, the viewport being
// that position less `scrollY`, and a browser that clamps the offset when the
// emptied view is laid out -- which is what the live page did. The follower
// rows mount each page in `fake-view.js` with a window whose `swapped()`
// moves the page to 0 on every view swap, as Chromium did; they see the
// OFFSET and not focus (fake-view keeps no focus). Focus identity across the
// swap is the `fake-dom` rows' part.
//
// Every row carries its NEGATIVE CONTROL: an assertion that fails on the code
// before this fix.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { resetMode } from "../client.js";
import { createRouteLifecycle } from "../app.js";
import { dropDraft, formKey, keepDraft, resetCheckIntents } from "../lifecycle.js";
import {
  append,
  clear,
  disableKeepingFocus,
  focusWithin,
  keepReadingPlace,
  readingPlace,
  replace,
  restoreFocus,
} from "../render.js";
import { SCHEDULE_DRAFT_FIELDS, SCHEDULE_FORM, mountSchedules } from "../pages/schedules.js";
import { mountClusterDetail } from "../pages/clusters.js";
import { mountDestinationDetail } from "../pages/destinations.js";
import {
  clearOfWizardNav,
  keepStatusInView,
  mountRestoreWizard,
  recoveryPoints,
} from "../pages/restore-wizard.js";
import { build, fakeDocument } from "./fake-dom.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const clone = (value) => JSON.parse(JSON.stringify(value));

// ------------------------------------------------------ a page with a layout

/** A window over `doc` at `y`, 844 px tall (the live phone), counting every
 *  programmatic scroll. */
function windowOver(doc, y) {
  const win = {
    scrollX: 0,
    scrollY: y,
    innerHeight: 844,
    scrolls: [],
    scrollTo(x, to) { win.scrollX = x; win.scrollY = Math.max(0, to); win.scrolls.push(["to", to]); },
    scrollBy(dx, dy) { win.scrollY = Math.max(0, win.scrollY + dy); win.scrolls.push(["by", dy]); },
    getComputedStyle: (el) => ({ scrollMarginBottom: el.getAttribute("data-margin") || "0px" }),
  };
  doc.defaultView = win;
  return win;
}

/** Lays `el` out at document position `y`, `h` tall: its rect is where that
 *  sits in the viewport now. */
function at(win, el, y, h) {
  el.getBoundingClientRect = () => ({ top: y - win.scrollY, bottom: y + h - win.scrollY });
  return el;
}

/** Step 5 as a tree: a stepper `above` px tall, the form with its focused
 *  status at document position 943 when `above` is 300 (the live geometry),
 *  and whatever the check says below it. */
function step5(doc, win, above) {
  const kids = [
    build(doc, ["div", { class: "stepper", id: "stepper" }, "steps"]),
    build(doc, ["form", { id: "restore-readiness-form" },
      ["fieldset", { class: "form-body" }, ["button", { id: "restore-readiness-start" }, "Check"]],
      ["div", { class: "form-status", id: "restore-readiness-status", tabindex: "-1" }, "Created"]]),
    build(doc, ["div", { class: "preflight-result", id: "preflight-pf-x" }, "running"]),
  ];
  at(win, kids[0], 0, above);
  at(win, kids[1].querySelector("#restore-readiness-status"), above + 643, 55);
  return kids;
}

/** A browser that lays the emptied view out -- the offset clamps to what an
 *  empty page can scroll, 0 -- as the live page's swap did. */
function layingOutWhenEmptied(view, win) {
  const remove = view.removeChild.bind(view);
  view.removeChild = (child) => {
    const out = remove(child);
    if (view.firstChild === null) {
      win.scrollY = 0;
    }
    return out;
  };
}

// ===========================================================================
// render.js: a repaint keeps the reader's place
// ===========================================================================

test("p16_a_repaint_that_empties_the_view_leaves_the_page_where_the_reader_was", () => {
  const doc = fakeDocument();
  const win = windowOver(doc, 282);
  const view = build(doc, ["main", { id: "view-slot", tabindex: "-1" }]);
  doc.body.appendChild(view);
  append(view, step5(doc, win, 300));
  view.querySelector("#restore-readiness-status").focus();
  layingOutWhenEmptied(view, win);
  const before = view.querySelector("#restore-readiness-status").getBoundingClientRect();
  assert.deepEqual(before, { top: 661, bottom: 716 }, "the live moment: 661-716 at scrollY 282");

  replace(view, step5(doc, win, 300));
  const status = view.querySelector("#restore-readiness-status");
  assert.equal(win.scrollY, 282,
    "NEGATIVE CONTROL: the page is where the reader left it (before: 0, the status at 943)");
  assert.deepEqual(status.getBoundingClientRect(), { top: 661, bottom: 716 });
  assert.equal(doc.activeElement, status, "focus is on the NEW status element");
  assert.equal(doc.activeElement.getAttribute("id"), "restore-readiness-status");

  // THE MODEL CAN FAIL: the same swap without the place kept ends at 0.
  const other = fakeDocument();
  const bare = windowOver(other, 282);
  const slot = build(other, ["main", { id: "view-slot", tabindex: "-1" }]);
  other.body.appendChild(slot);
  append(slot, step5(other, bare, 300));
  layingOutWhenEmptied(slot, bare);
  append(clear(slot), step5(other, bare, 300));
  assert.equal(bare.scrollY, 0, "the bare swap moves the page, as the live one did");
});

test("p16_the_focused_element_stays_where_it_sat_when_content_above_it_changes", () => {
  // Test access's create answer drops the "last test" block above the status:
  // the status the reader is on moves up by that block's height, and the page
  // moves with it.
  const doc = fakeDocument();
  const win = windowOver(doc, 400);
  const view = build(doc, ["main", { id: "view-slot", tabindex: "-1" }]);
  doc.body.appendChild(view);
  append(view, step5(doc, win, 300));
  view.querySelector("#restore-readiness-status").focus();
  const top = view.querySelector("#restore-readiness-status").getBoundingClientRect().top;
  replace(view, step5(doc, win, 208));
  assert.equal(view.querySelector("#restore-readiness-status").getBoundingClientRect().top, top,
    "NEGATIVE CONTROL: the focused status sits where it sat (before: 92 px higher)");
  assert.equal(win.scrollY, 308);
});

test("p16_a_repaint_that_moved_nothing_scrolls_nothing", () => {
  // A scroll the reader is in the middle of is not interrupted by a repaint
  // that left the page alone.
  const doc = fakeDocument();
  const win = windowOver(doc, 282);
  const view = build(doc, ["main", { id: "view-slot", tabindex: "-1" }]);
  doc.body.appendChild(view);
  append(view, step5(doc, win, 300));
  view.querySelector("#restore-readiness-status").focus();
  replace(view, step5(doc, win, 300));
  assert.deepEqual(win.scrolls, [], "no programmatic scroll at all");
  assert.equal(readingPlace({}, null), null, "no document, no place");
  assert.equal(keepReadingPlace(view, null), false);
});

test("p16_a_focus_target_the_browser_refuses_is_not_a_landing", () => {
  // `.form-status:empty` is `display: none`, and `focus()` on it does nothing:
  // Test access and the schedule form's Check readiness left focus on the
  // body. A target now counts only when focus is on it afterwards.
  const doc = fakeDocument();
  const view = build(doc, ["main", { id: "view-slot", tabindex: "-1" },
    ["form", {},
      ["fieldset", { class: "form-body" }, ["button", { type: "submit" }, "Test access"]],
      ["div", { class: "form-status", id: "empty-status", tabindex: "-1" }]]]);
  doc.body.appendChild(view);
  view.querySelector("button").focus();
  const kept = focusWithin(view);
  const next = build(doc, ["form", {},
    ["fieldset", { class: "form-body", disabled: "" }, ["button", { type: "submit" }, "Test access"]],
    ["div", { class: "form-status", id: "empty-status", tabindex: "-1" }]]);
  next.querySelector("button").disabled = true;
  next.querySelector("#empty-status").focus = () => {};
  append(clear(view), [next]);
  doc.activeElement = null;
  restoreFocus(view, kept);
  assert.equal(doc.activeElement, view,
    "NEGATIVE CONTROL: the view takes focus when the status refuses it (before: nothing did)");

  // disableKeepingFocus: the first candidate refuses, the next one is tried.
  const refusing = build(doc, ["div", { class: "form-status", tabindex: "-1" }]);
  refusing.focus = () => {};
  const verdict = build(doc, ["div", { id: "verdict", tabindex: "-1" }]);
  const button = build(doc, ["button", { id: "go" }, "Check readiness"]);
  const form = build(doc, ["form", {}]);
  form.appendChild(button);
  form.appendChild(refusing);
  const slot = build(doc, ["div", { id: "view-slot-2" }]);
  slot.appendChild(form);
  slot.appendChild(verdict);
  doc.body.appendChild(slot);
  const page = build(doc, ["main", { id: "view-slot", tabindex: "-1" }]);
  doc.getElementById = (id) => (id === "view-slot" ? page : doc.body.querySelector("#" + id));
  button.focus();
  disableKeepingFocus(button, true, refusing);
  assert.equal(doc.activeElement, page,
    "NEGATIVE CONTROL: a refused fallback hands focus on (before: it stopped at the refusal)");
  button.disabled = false;
  button.focus();
  disableKeepingFocus(button, true, verdict);
  assert.equal(doc.activeElement, verdict, "a fallback that takes focus keeps it");
});

// ===========================================================================
// restore step 5: the status is kept above the sticky bar after every repaint
// ===========================================================================

/** The sticky bar at 738-844 and the focused status at document position `y`. */
function barAndStatus(y, scrollY) {
  const doc = fakeDocument();
  const win = windowOver(doc, scrollY);
  const node = build(doc, ["div", {},
    ["section", { id: "step-preflight", tabindex: "-1" },
      ["div", { class: "form-status", id: "restore-readiness-status", tabindex: "-1",
        "data-margin": "128px" }, "Created"]],
    ["div", { class: "wizard-nav" }, "Back Next"]]);
  doc.body.appendChild(node);
  const status = node.querySelector("#restore-readiness-status");
  at(win, status, y, 37);
  const nav = node.querySelector(".wizard-nav");
  nav.getBoundingClientRect = () => ({ top: 738, bottom: 844 });
  // CHROMIUM'S `nearest`: nothing moves while the border box is in the viewport.
  const asked = [];
  status.scrollIntoView = (options) => {
    asked.push(options);
    const r = status.getBoundingClientRect();
    if (r.bottom > win.innerHeight) {
      win.scrollBy(0, r.bottom + 128 - win.innerHeight);
    }
  };
  return { doc, win, node, status, asked };
}

test("p16_a_status_in_the_viewport_but_under_the_bar_is_moved_above_it", () => {
  // Measured over the real console at 390 x 844: the status at 734-771, the
  // bar's top at 738 -- `scrollIntoView({block: "nearest"})` did not move it.
  const t = barAndStatus(734, 0);
  t.status.focus();
  assert.equal(keepStatusInView(t.node, "#restore-readiness-status"), true);
  assert.deepEqual(t.asked, [{ block: "nearest" }], "the nearest edge is still asked first");
  const r = t.status.getBoundingClientRect();
  assert.ok(r.bottom <= 738 && r.top >= 0,
    "NEGATIVE CONTROL: the status is above the bar (before: 734-771, under it): " +
      JSON.stringify(r));
  assert.equal(r.bottom, 844 - 128, "its bottom sits where the scroll margin puts it");

  const clear = barAndStatus(500, 0);
  clear.status.focus();
  keepStatusInView(clear.node, "#restore-readiness-status");
  assert.deepEqual(clear.win.scrolls, [], "a status above the bar moves nothing");
  assert.equal(clearOfWizardNav(clear.node, clear.status), false);
});

test("p16_the_step_5_follow_keeps_the_status_in_view_after_every_repaint", () => {
  // THE WIRING, read from the source for the reason R3-1's rows give: the
  // fake views the wizard mounts in have no layout. The follow's `show` asks
  // for the status after EVERY painted repaint -- not only the verdict's --
  // and only when the reader could see it before the repaint, so a reader
  // who scrolled away while the check runs is not pulled back to it.
  const source = readFileSync(UI + "pages/restore-wizard.js", "utf8");
  const follow = source.slice(source.indexOf("function followRestoreReadiness("),
    source.indexOf("/** The exact step-5 request for one recovery point."));
  assert.match(follow,
    /const seen = onScreen\(node\.querySelector\("#restore-readiness-status"\)\);\s*const painted = await renderAndWire\(node, state, parse, api, lifecycle, true\);[\s\S]*?if \(painted === true && seen !== false\) \{\s*keepStatusInView\(node, "#restore-readiness-status"\);\s*\}/,
    "NEGATIVE CONTROL: a non-terminal repaint asks for the status (before: only the verdict)");
  const verdict = follow.indexOf("keepVerdictInView(node, next);");
  const status = follow.indexOf("keepStatusInView(node, \"#restore-readiness-status\");");
  assert.ok(status !== -1 && verdict > status,
    "the verdict is asked for last, so the headline wins when it lands");
});

// ===========================================================================
// Every follower: a non-terminal repaint keeps the page
// ===========================================================================

/** A window at 282 px that the browser moves to 0 on every view swap. */
function movingWindow() {
  const win = {
    scrollX: 0, scrollY: 282, innerHeight: 844, swaps: 0,
    scrollTo(x, y) { win.scrollX = x; win.scrollY = y; },
    scrollBy(dx, dy) { win.scrollY += dy; },
    swapped() { win.swaps += 1; win.scrollY = 0; },
  };
  return win;
}

/** A follow whose waits the row opens one at a time. */
function gate() {
  const waiting = [];
  return {
    wait: () => new Promise((resolve) => { waiting.push(resolve); }),
    open() { const next = waiting.shift(); if (next !== undefined) { next(); } },
    get waiting() { return waiting.length; },
  };
}

async function flush(turns) {
  for (let i = 0; i < (turns || 20); i += 1) {
    await new Promise((resolve) => { setImmediate(resolve); });
  }
}

function newest(view, marker) {
  const chunks = view.chunks.filter((c) => c.html.includes(marker));
  return chunks.length === 0 ? "" : chunks[chunks.length - 1].html;
}

/** A `Preflight` in `state`. */
function preflight(id, state, over) {
  const terminal = state === "ready";
  return Object.assign({
    id: id, namespace: "team-a", uid: "uid-" + id, resourceVersion: "1", operation: "backup",
    state: state, terminal: terminal, binding: { referents: [] }, applicable: terminal,
    stale: false, staleReasons: [], staleBasis: terminal ? ["expiry", "referents"] : [],
    checks: [], warnings: [], executionOnly: [], detailsAvailable: false, conditions: [],
  }, over || {});
}

/** The row's shape: the first read answers `running` and is painted; the page
 *  must be where it was, the swap having moved it. Then the check settles. */
async function nonTerminalRepaintKeepsThePage(t) {
  try {
    await t.start();
    await flush();
    assert.ok(t.win.swaps >= 1, t.what + ": the create answer was painted");
    assert.equal(t.win.scrollY, 282, t.what + ": the pending repaint kept the page");
    assert.equal(t.gate.waiting, 1, t.what + ": the follow waits for its first read");
    const swaps = t.win.swaps;
    t.gate.open();
    await flush();
    assert.equal(t.reads(), 1, t.what + ": one read, answered running");
    assert.match(t.shown(), t.running, t.what + ": the running answer is on screen");
    assert.ok(t.win.swaps > swaps, t.what + ": the running answer was a repaint that swapped");
    assert.equal(t.win.scrollY, 282,
      "NEGATIVE CONTROL: " + t.what + "'s non-terminal repaint left the page where it was " +
        "(before: the swap took it to 0)");
    t.settle();
    t.gate.open();
    await flush();
    assert.equal(t.win.scrollY, 282, t.what + ": and so did the terminal one");
  } finally {
    resetMode();
    resetCheckIntents();
  }
}

test("p16_restore_step_5s_non_terminal_repaint_keeps_the_page", async () => {
  const ns = "p16-wizard";
  const backups = fixture("wizard-backups.json");
  const destination = fixture("console/destination.json").item;
  for (const b of backups.items) {
    b.metadata.namespace = ns;
    b.spec.destinationRef = { name: destination.name, uid: destination.uid };
    b.spec.archive = { url: "logweir-destination://" + destination.name };
    b.status.locationDigest = destination.locationDigest;
  }
  const point = recoveryPoints(backups)[0];
  let settled = false;
  let reads = 0;
  const g = gate();
  const api = {
    list: async (_ns, plural) => clone(plural === "backups" ? backups : fixture("wizard-clusters.json")),
    destinations: async () => ({ items: [{ name: destination.name, uid: destination.uid,
      generation: destination.generation, canonicalUrl: destination.canonicalUrl, default: true,
      status: destination.status }] }),
    destination: async () => ({ item: clone(destination) }),
    startPreflight: async (_ns, request) => ({ item: preflight("pf-p16w", "pending",
      { operation: "restore", binding: { planHash: request.restore.planHash } }), replayed: false }),
    preflight: async (_ns, id, options) => {
      reads += 1;
      return { item: preflight(id, settled ? "ready" : "running",
        { operation: "restore", binding: { planHash: (options || {}).planHash } }) };
    },
    wait: g.wait,
  };
  const win = movingWindow();
  const view = fakeView({ window: win });
  const originalWindow = globalThis.window;
  globalThis.window = { location: { hash: "#/restore?ns=" + ns } };
  try {
    await mountRestoreWizard(view.root, ns,
      { uid: point.metadata.uid, backup: point.metadata.name, step: 5 }, parse, api,
      createRouteLifecycle().begin());
    await nonTerminalRepaintKeepsThePage({
      what: "restore step 5", win: win, gate: g,
      start: () => view.find("#restore-readiness-form").dispatch("submit"),
      reads: () => reads,
      shown: () => newest(view, "id=\"preflight-pf-p16w\""),
      running: /running/,
      settle: () => { settled = true; },
    });
  } finally {
    globalThis.window = originalWindow;
  }
});

/** The schedules page's API; `settled` flips the check to ready. */
function schedulesApi(g, box) {
  return {
    list: (_ns, plural) => Promise.resolve(plural === "kafkaclusters"
      ? { items: [{ metadata: { name: "orders-prod", uid: "uid-A" },
        spec: { role: "source", bootstrapServers: ["kafka:9093"] }, status: {} }] }
      : { items: [] }),
    destinations: () => Promise.resolve({ items: [{ name: "primary", uid: "d-1", default: true,
      canonicalUrl: "s3://kafka-backups/poc" }] }),
    latestDiscoveries: () => Promise.resolve({ latestAttempt: null, lastSuccessful: null }),
    startPreflight: () => Promise.resolve({ item: preflight("pf-p16s" + (box.started += 1),
      "pending"), replayed: false }),
    preflight: (_ns, id) => {
      box.reads += 1;
      return Promise.resolve({ item: preflight(id, box.settled ? "ready" : "running") });
    },
    wait: g.wait,
  };
}

test("p16_the_readiness_panels_non_terminal_repaint_keeps_the_page", async () => {
  const g = gate();
  const box = { started: 0, reads: 0, settled: false };
  const win = movingWindow();
  const view = fakeView({ window: win });
  await mountSchedules(view.root, "p16-panel", parse, LIFE(), schedulesApi(g, box));
  await nonTerminalRepaintKeepsThePage({
    what: "the readiness panel", win: win, gate: g,
    start: () => view.find("#readiness-form").dispatch("submit"),
    reads: () => box.reads,
    shown: () => newest(view, "id=\"backup-readiness\""),
    running: /this page reads it again until then/,
    settle: () => { box.settled = true; },
  });
});

test("p16_the_schedule_forms_non_terminal_repaint_keeps_the_page", async () => {
  const ns = "p16-form";
  const key = formKey(ns, SCHEDULE_FORM);
  dropDraft(key);
  keepDraft(key, {
    name: "nightly", source: "orders-prod", sourceUid: "uid-A", mode: "daily", cron: "",
    hour: "2", minute: "30", dayOfWeek: "1", dayOfMonth: "1", n: "6",
    timeZone: "Europe/Berlin", selection: "named", topics: "orders, payments",
    incompleteDiscovery: "", excludeTopics: "", excludePrefixes: "", destination: "primary",
    archive: "", archiveSecret: "", concurrencyPolicy: "", startingDeadlineSeconds: "",
    catchUpPolicy: "", maxRetries: "", retryDelaySeconds: "", activeDeadlineSeconds: "",
    keepLast: "", keepDays: "", suspended: "false",
  }, SCHEDULE_DRAFT_FIELDS);
  const g = gate();
  const box = { started: 0, reads: 0, settled: false };
  const win = movingWindow();
  const view = fakeView({ window: win });
  try {
    await mountSchedules(view.root, ns, parse, LIFE(), schedulesApi(g, box));
    await nonTerminalRepaintKeepsThePage({
      what: "the schedule form", win: win, gate: g,
      start: () => view.find("#schedule-check-readiness").dispatch("click"),
      reads: () => box.reads,
      shown: () => newest(view, "id=\"schedule-readiness-verdict\""),
      running: /this page reads it again until then/,
      settle: () => { box.settled = true; },
    });
    assert.match(newest(view, "id=\"schedule-readiness-verdict\""),
      /id="schedule-readiness-verdict" tabindex="-1"/,
      "NEGATIVE CONTROL: the verdict's region can take the focus Check readiness gives up " +
        "(before: the form's empty status, which is not rendered, was the only target)");
    // AND IT IS WHERE THAT FOCUS GOES -- read from the source: fake-view keeps
    // no focus, so `disableKeepingFocus` has nothing to move here.
    assert.match(readFileSync(UI + "pages/schedules.js", "utf8"),
      /disableKeepingFocus\(check, true, node\.querySelector\("#schedule-readiness-verdict"\)\);/,
      "NEGATIVE CONTROL: Check readiness hands its focus to the verdict's region");
  } finally {
    dropDraft(key);
  }
});

const CONNECTION = Object.freeze({
  metadata: { name: "orders-prod", namespace: "team-a", uid: "uid-A", generation: 1 },
  spec: { role: "source", bootstrapServers: ["kafka-a:9093"] },
  status: {},
});

function discovery(id, state) {
  const terminal = state === "succeeded";
  return {
    id: id, namespace: "team-a", uid: "uid-" + id, resourceVersion: "1",
    connection: { name: "orders-prod", principal: "logweir" },
    state: state, terminal: terminal, stale: false, staleReasons: [], truncated: false,
    chunkCount: terminal ? 1 : 0, conditions: [], visibility: { state: "unknown" },
    counts: terminal ? { listed: 3, returned: 3, internalExcluded: 0, errored: 0 } : undefined,
  };
}

function clusterApi(g, box) {
  return {
    get: () => Promise.resolve(CONNECTION),
    latestDiscoveries: () => Promise.resolve({ latestAttempt: null, lastSuccessful: null }),
    startPreflight: () => Promise.resolve({ item: preflight("pf-p16c", "pending",
      { operation: "sourceConnection" }), replayed: false }),
    preflight: (_ns, id) => {
      box.reads += 1;
      return Promise.resolve({ item: preflight(id, box.settled ? "ready" : "running",
        { operation: "sourceConnection" }) });
    },
    startDiscovery: () => Promise.resolve({ item: discovery("td-p16", "pending"),
      replayed: false, reused: false }),
    discovery: (_ns, id) => {
      box.reads += 1;
      return Promise.resolve({ item: discovery(id, box.settled ? "succeeded" : "running") });
    },
    discoveryTopics: () => Promise.resolve({ items: [], page: {},
      scan: { complete: true, chunksScanned: 1 } }),
    wait: g.wait,
  };
}

test("p16_test_connections_non_terminal_repaint_keeps_the_page", async () => {
  const g = gate();
  const box = { reads: 0, settled: false };
  const win = movingWindow();
  const view = fakeView({ window: win });
  await mountClusterDetail(view.root, "p16-c", "orders-prod", parse, LIFE(), clusterApi(g, box));
  await nonTerminalRepaintKeepsThePage({
    what: "Test connection", win: win, gate: g,
    start: () => view.find("#connection-check-form").dispatch("submit"),
    reads: () => box.reads,
    shown: () => newest(view, "id=\"connection-check\""),
    running: /this page reads it again until then/,
    settle: () => { box.settled = true; },
  });
});

test("p16_discover_topics_non_terminal_repaint_keeps_the_page", async () => {
  const g = gate();
  const box = { reads: 0, settled: false };
  const win = movingWindow();
  const view = fakeView({ window: win });
  await mountClusterDetail(view.root, "p16-td", "orders-prod", parse, LIFE(), clusterApi(g, box));
  await nonTerminalRepaintKeepsThePage({
    what: "Discover topics", win: win, gate: g,
    start: () => view.find("#discovery-form").dispatch("submit"),
    reads: () => box.reads,
    shown: () => newest(view, "id=\"cluster-discovery\""),
    running: /running/,
    settle: () => { box.settled = true; },
  });
});

test("p16_test_access_non_terminal_repaint_keeps_the_page", async () => {
  const g = gate();
  const box = { reads: 0, settled: false };
  const win = movingWindow();
  const view = fakeView({ window: win });
  const api = {
    destination: () => Promise.resolve({ item: { name: "primary", uid: "d-1", generation: 1,
      canonicalUrl: "s3://kafka-backups/poc", transport: "insecureHttp", access: {},
      conditions: [] } }),
    destinationUsage: () => Promise.resolve({ schedules: [], backups: [] }),
    testDestination: () => Promise.resolve({ item: preflight("pf-p16t", "pending",
      { operation: "destinationAccess" }), replayed: false }),
    preflight: (_ns, id) => {
      box.reads += 1;
      return Promise.resolve({ item: preflight(id, box.settled ? "ready" : "running",
        { operation: "destinationAccess" }) });
    },
    wait: g.wait,
  };
  await mountDestinationDetail(view.root, "p16-d", "primary", parse, LIFE(), api);
  await nonTerminalRepaintKeepsThePage({
    what: "Test access", win: win, gate: g,
    start: () => view.find("#destination-test-form").dispatch("submit"),
    reads: () => box.reads,
    shown: () => newest(view, "id=\"destination-test\""),
    running: /this page reads it again until then/,
    settle: () => { box.settled = true; },
  });
  const panel = newest(view, "id=\"destination-test\"");
  assert.ok(panel.indexOf("id=\"destination-test-status\"") < panel.indexOf("</form>"),
    "NEGATIVE CONTROL: the status is the form's own, where focus looks for it when the " +
      "fieldset is disabled (before: drawn after </form>, so focus fell to the body)");
});
