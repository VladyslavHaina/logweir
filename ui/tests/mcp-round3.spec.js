// mcp-round3.spec.js -- the four small console rows the third human-like pass
// (MCP round 3, 2026-09-25, on sha-a54fb823) left open, each with the
// behaviour it replaced as its NEGATIVE CONTROL:
//
//   R3-1  wizard step 5 at 390 px: after a readiness click, focus moved to the
//         status line and the repaint left it -- and the verdict under it --
//         behind the sticky Back/Next bar.
//   R2-10 step 5's source sentence printed raw Markdown backticks around the
//         destination name and the frozen locationDigest; the other step
//         texts built the same way (step 4's complaints, step 6's plan
//         problem, the catalog-point refusal) did too.
//   R3-2  the catalog offered "Restore this point" to a viewer, whom the
//         route then refuses; the same link on the Backups and Schedules
//         pages did the same.
//   R3-3  the header of a session with no role anywhere said "choose a
//         namespace to see your role" beside a card saying "You have no role
//         in any namespace yet".

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { renderIdentity } from "../app.js";
import { resetMode, selectMode, sessionIdentity } from "../client.js";
import { RESTORE_NEEDS_ROLE_SENTENCE, restorePointLink } from "../render.js";
import { pointRow } from "../pages/catalog.js";
import { restorePointCell } from "../pages/history.js";
import { renderLatestPointAction, restoreCell } from "../pages/schedules.js";
import { renderRetryAction } from "../pages/operation.js";
import {
  initialState,
  keepStatusInView,
  keepVerdictInView,
  mappingProblems,
  readinessSourceSentence,
  recoveryPoints,
  renderCatalogPointRefusal,
  renderPlanStep,
  renderPreflightStep,
  renderTargetStep,
  renderTopicSubset,
} from "../pages/restore-wizard.js";
import { build, fakeDocument } from "./fake-dom.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

/** A step-5 state over a point frozen to the saved destination `primary`. */
function savedState() {
  const backups = fixture("wizard-backups.json");
  const newest = recoveryPoints(backups)[0];
  const point = backups.items.find((item) => item.metadata.uid === newest.metadata.uid);
  const destination = fixture("console/destination.json").item;
  point.spec.destinationRef = { name: "primary", uid: destination.uid };
  point.spec.archive = { url: "logweir-destination://primary" };
  point.status.locationDigest = destination.locationDigest;
  return initialState("logweir-r3", fixture("wizard-clusters.json"), backups,
    { uid: newest.metadata.uid, backup: newest.metadata.name }, destination);
}

/** The text of one element's markup, by its id, in rendered HTML. */
function paragraph(html, id) {
  const at = html.indexOf("id=\"" + id + "\"");
  assert.ok(at !== -1, "rendered #" + id);
  return html.slice(at, html.indexOf("</p>", at));
}

// ===========================================================================
// R3-1: the focused status is never left under the sticky footer
// ===========================================================================

test("r3_1_every_wizard_focus_target_keeps_the_footers_height_clear_below_it", () => {
  const css = readFileSync(UI + "style.css", "utf8");
  const token = /--lw-wizard-nav-clearance:\s*([0-9.]+)rem;/.exec(css);
  assert.ok(token !== null, "the clearance is a token of the console's own layer");
  // The footer wraps to two rows at 390 px: about 110 px, which is 6.9rem.
  assert.ok(Number(token[1]) >= 7, "and it clears the two-row footer: " + token[1] + "rem");
  const rule = /\.wizard-page \.form-status,\s*\.wizard-page \[tabindex\],\s*\.wizard-page \.preflight-head \{\s*scroll-margin-bottom: var\(--lw-wizard-nav-clearance\);/;
  assert.match(css, rule,
    "NEGATIVE CONTROL: without it a status scrolled into view lands under the footer");
  assert.match(css, /\.wizard-nav \{\s*position: sticky;\s*bottom: 0;/,
    "the footer this clears is the sticky one at the bottom");
});

test("r3_1_a_focused_status_is_brought_into_view_and_an_unfocused_one_is_left_alone", () => {
  const doc = fakeDocument();
  const node = build(doc, ["section", { id: "step-preflight" },
    ["div", { class: "form-status", id: "restore-readiness-status", tabindex: "-1" }, "Created"],
    ["button", { id: "restore-readiness-start" }, "Check this plan again"]]);
  doc.body.appendChild(node);
  const status = node.querySelector("#restore-readiness-status");
  const asked = [];
  status.scrollIntoView = (options) => { asked.push(options); };
  node.querySelector("#restore-readiness-start").focus();
  assert.equal(keepStatusInView(node, "#restore-readiness-status"), false,
    "focus is on the button: nothing moves under the reader");
  assert.equal(asked.length, 0);
  status.focus();
  assert.equal(keepStatusInView(node, "#restore-readiness-status"), true);
  assert.deepEqual(asked, [{ block: "nearest" }],
    "NEGATIVE CONTROL: the focused status is scrolled to its nearest edge, which honours the " +
      "scroll margin -- before, nothing brought it out from under the footer");
});

test("r3_1_the_readiness_click_brings_its_status_into_view_once_the_step_is_painted", () => {
  // THE WIRING: the readiness record's listener repaints step 5 and then asks
  // for the status. Read from the source, because the fake views this suite
  // mounts the wizard in have no layout to scroll.
  const source = readFileSync(UI + "pages/restore-wizard.js", "utf8");
  const wire = source.slice(source.indexOf("function wireRestoreReadiness("),
    source.indexOf("listen(form, \"submit\"", source.indexOf("function wireRestoreReadiness(")));
  assert.match(wire,
    /renderAndWire\(node, state, parse, api, lifecycle, true\)\.then\(\(painted\) => \{\s*if \(painted === true\) \{\s*keepStatusInView\(node, "#restore-readiness-status"\);/,
    "NEGATIVE CONTROL: the listener asks for the status once the step is painted");
});

// ===========================================================================
// R2-10: names and digests in the step texts are code, never raw backticks
// ===========================================================================

test("r2_10_step_5s_source_sentence_shows_its_destination_and_digest_as_code", () => {
  const state = savedState();
  const digest = state.point.status.locationDigest;
  assert.match(readinessSourceSentence(state.point), /saved destination `primary`/,
    "the sentence itself spells them the way every message here does");
  const step = renderPreflightStep(state, { bytes: "plan\n", hash: "sha256:" + "a".repeat(64) });
  const sentence = paragraph(step, "readiness-source-destination");
  assert.ok(sentence.includes("<code>primary</code>"), sentence);
  assert.ok(sentence.includes("<code>" + digest + "</code>"), sentence);
  assert.ok(!sentence.includes("`"),
    "NEGATIVE CONTROL: no literal backtick on screen (the round-3 screenshot showed them)");
});

test("r2_10_the_other_step_texts_built_the_same_way_show_code_too", () => {
  // STEP 4: a duplicate mapping and an illegal prefix, whose refusals name
  // topics between backticks.
  const state = savedState();
  const frozen = state.point.status.topics || state.point.spec.topics || [];
  const first = String((Array.isArray(frozen) && frozen.length > 0 ? frozen[0] : "orders"));
  state.fields.topics = [first, first];
  const subset = paragraph(renderTopicSubset(state), "subset-complaint");
  assert.ok(typeof mappingProblems(state).topics === "string");
  assert.ok(subset.includes("<code>" + first + "</code>"), subset);
  assert.ok(!subset.includes("`"), "NEGATIVE CONTROL: no literal backtick in step 4's complaint");
  state.fields.topics = [first];
  state.fields.target.topicPrefix = "has space-";
  const prefix = paragraph(renderTargetStep(state), "prefix-complaint");
  assert.ok(prefix.includes("<code>has space-</code>"), prefix);
  assert.ok(!prefix.includes("`"));

  // STEP 6: a plan the page could not render names the offending value.
  const plan = renderPlanStep({ problem: "point.pointId is `lwp1-x`, not `lwp1-` plus 32 hex" },
    savedState());
  assert.ok(plan.includes("<code>lwp1-x</code>"), plan.slice(0, 400));
  assert.ok(!/is `lwp1-x`/.test(plan), "NEGATIVE CONTROL: step 6's problem shows code");

  // THE CATALOG-POINT REFUSAL the wizard renders in place of a plan.
  const refusal = renderCatalogPointRefusal("logweir-r3", {
    reason: "the point id is not `lwp1-` plus 32 lowercase hex characters",
    pointId: "lwp1-bad", catalog: "archive",
  });
  assert.ok(refusal.includes("<code>lwp1-</code>"), refusal);
  assert.ok(!refusal.includes("`"), "NEGATIVE CONTROL: no literal backtick in the refusal");
});

// ===========================================================================
// R3-2: "Restore this point" is offered only to a role that can restore
// ===========================================================================

async function signedInAs(session) {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: session }) });
}

/** A catalog row the wizard would offer, as `restore-catalog.spec.js` builds it. */
function catalogRow() {
  const set = "3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f-20260922-140000";
  return {
    pointId: "lwp1-0123456789abcdef0123456789abcdef", backupId: set,
    runId: "01JB7Z00000000000000000000", recoveryPointAt: "2026-09-22T14:00:00Z",
    coveredFrom: "2026-09-22T13:00:00Z", coveredTo: "2026-09-22T14:00:00Z",
    availability: "Available", verification: "Verified", selectable: true,
    signerKeyId: "c".repeat(64),
    receiptKey: "logweir/backups/" + set + "/01JB7Z00000000000000000000.receipt.json",
    receiptSha256: "sha256:" + "a1".repeat(32), manifestKey: set + "/manifest.json",
    manifestSha256: "sha256:" + "b2".repeat(32),
    locations: [{ locationId: "s3://kafka-backups/team-a/prod", availability: "Available" }],
  };
}

const catalogPage = () => ({ requestId: "r", items: [], truncated: false, viewExpired: false,
  page: { limit: 200, nextCursor: null } });

test("r3_2_a_role_that_cannot_restore_reads_who_can_instead_of_a_link", async () => {
  // THE HELPER every "Restore this point" goes through.
  assert.equal(restorePointLink("#/restore?ns=a&uid=u", true),
    "<a class=\"action\" href=\"#/restore?ns=a&amp;uid=u\">Restore this point</a>",
    "an allowed role gets the link, byte for byte what the pages printed before");
  const refused = restorePointLink("#/restore?ns=a&uid=u", false);
  assert.match(refused, /data-restore-refused="role"/);
  assert.ok(refused.includes(RESTORE_NEEDS_ROLE_SENTENCE));
  assert.doesNotMatch(refused, /href=/, "NEGATIVE CONTROL: no link for a role that cannot use it");

  // THE PAGES, as a viewer of team-a (`restoreCreate: false` there).
  const viewer = fixture("console/session-viewer.json");
  await signedInAs(viewer);
  try {
    const ns = viewer.namespaces[0].namespace;
    const point = fixture("wizard-backups.json").items.find((b) => recoveryPoints({ items: [b] }).length > 0);
    point.metadata.namespace = ns;
    const catalogCells = pointRow(catalogRow(), ns, "archive", "primary", catalogPage()).join("");
    const backupsCell = restorePointCell(point, ns);
    const scheduleCell = restoreCell(ns, point, null);
    for (const [where, html] of [["catalog", catalogCells], ["backups", backupsCell],
      ["schedules", scheduleCell]]) {
      assert.ok(!html.includes(">Restore this point</a>"),
        "NEGATIVE CONTROL (" + where + "): a viewer is not offered the restore link");
      assert.ok(html.includes(RESTORE_NEEDS_ROLE_SENTENCE), where + " says who can: " + html);
    }
  } finally {
    resetMode();
  }
  // AND AN OPERATOR STILL GETS THE LINK (the local administrator's session).
  await signedInAs(fixture("console/session.json"));
  try {
    const ns = "team-a";
    const catalogCells = pointRow(catalogRow(), ns, "archive", "primary", catalogPage()).join("");
    assert.ok(catalogCells.includes(">Restore this point</a>"), catalogCells.slice(0, 300));
  } finally {
    resetMode();
  }
});

// ===========================================================================
// R3-3: the header of a session with no role anywhere agrees with its card
// ===========================================================================

test("r3_3_a_session_with_no_role_anywhere_reads_no_role_yet_in_the_header", async () => {
  const session = fixture("console/session-viewer.json");
  session.namespaces = session.namespaces.map((n) => Object.assign({}, n, { roles: [] }));
  await signedInAs(session);
  try {
    for (const ns of ["", "team-a"]) {
      const html = renderIdentity(sessionIdentity(ns));
      assert.match(html, /id="session-role">no role yet</);
      assert.doesNotMatch(html, /choose a namespace to see your role/,
        "NEGATIVE CONTROL: the header no longer asks for a namespace no choice will help");
    }
  } finally {
    resetMode();
  }
  // A viewer with no namespace chosen is still asked to choose one: that
  // choice does give them a role to see.
  await signedInAs(fixture("console/session-viewer.json"));
  try {
    assert.match(renderIdentity(sessionIdentity("")), /choose a namespace to see your role/);
  } finally {
    resetMode();
  }
});

// ===========================================================================
// Fix round (review of poc-fixes-5): L6 and L7
// ===========================================================================

test("r3_2_the_schedules_latest_point_and_a_failed_restores_retry_ask_the_same_grant", async () => {
  // REVIEW L6: the sweep missed two links a viewer was still offered -- the
  // schedule detail's "Restore from this point" and the operation page's
  // "Retry to a fresh target", which opens the wizard for a NEW Restore.
  const point = fixture("wizard-backups.json").items.find((b) => recoveryPoints({ items: [b] }).length > 0);
  const failed = { kind: "restore", console: true, terminal: true, state: "failed",
    name: "rst-failed", result: {} };
  const viewer = fixture("console/session-viewer.json");
  const ns = viewer.namespaces[0].namespace;
  point.metadata.namespace = ns;
  await signedInAs(viewer);
  try {
    const latest = renderLatestPointAction(ns, [point], null, null);
    assert.match(latest, /id="schedule-latest-point"/, "the latest point is still named");
    assert.doesNotMatch(latest, /id="schedule-restore-latest"/,
      "NEGATIVE CONTROL: a viewer is not offered Restore from this point");
    assert.ok(latest.includes("An operator or administrator can restore this point."), latest);
    const retry = renderRetryAction(failed, ns);
    assert.match(retry, /id="restore-retry"/, "the failed restore still says what a retry is");
    assert.doesNotMatch(retry, /id="retry-fresh-target"/,
      "NEGATIVE CONTROL: a viewer is not offered Retry to a fresh target");
    assert.ok(retry.includes("An operator or administrator can restore this point."), retry);
  } finally {
    resetMode();
  }
  await signedInAs(fixture("console/session.json"));
  try {
    assert.match(renderLatestPointAction("team-a", [point], null, null),
      /id="schedule-restore-latest"/, "an operating role keeps the link");
    assert.match(renderRetryAction(failed, "team-a"), /id="retry-fresh-target"/);
  } finally {
    resetMode();
  }
});

/** Step 5 with a verdict head, in the fake document, and a spy on its scroll. */
function stepWithVerdict() {
  const doc = fakeDocument();
  const node = build(doc, ["div", {},
    ["section", { id: "step-preflight", tabindex: "-1" },
      ["div", { class: "form-status", id: "restore-readiness-status", tabindex: "-1" }, "Created"],
      ["div", { class: "preflight-result", id: "preflight-pf-v" },
        ["p", { class: "preflight-head" }, "ready"]]],
    ["section", { id: "step-plan", tabindex: "-1" }, "plan"]]);
  doc.body.appendChild(node);
  const head = node.querySelector("#preflight-pf-v .preflight-head");
  const asked = [];
  head.scrollIntoView = (options) => { asked.push(options); };
  return { node: node, asked: asked };
}

test("l7_a_verdict_landing_while_the_reader_is_on_step_5_is_brought_above_the_footer", () => {
  // REVIEW L7 of R3-1: the status line was kept clear at the click, and the
  // verdict under it -- which lands later, through the follow -- was left
  // behind the sticky footer at 390 px.
  const t = stepWithVerdict();
  t.node.querySelector("#restore-readiness-status").focus();
  assert.equal(keepVerdictInView(t.node, { id: "pf-v", terminal: true }), true);
  assert.deepEqual(t.asked, [{ block: "nearest" }],
    "NEGATIVE CONTROL: the verdict head is scrolled to its nearest edge, which honours the " +
      "footer-high scroll margin");
  const away = stepWithVerdict();
  away.node.querySelector("#step-plan").focus();
  assert.equal(keepVerdictInView(away.node, { id: "pf-v", terminal: true }), false,
    "focus elsewhere: nothing moves under the reader");
  assert.equal(away.asked.length, 0);
  const css = readFileSync(UI + "style.css", "utf8");
  assert.match(css, /\.wizard-page \.preflight-head \{\s*scroll-margin-bottom: var\(--lw-wizard-nav-clearance\);/,
    "the verdict head keeps the footer's height clear below it");
});

test("l7_the_step_5_follow_asks_for_the_verdict_when_its_check_turns_terminal", () => {
  // THE WIRING, read from the source for the same reason as R3-1's: the fake
  // views the wizard mounts in have no layout.
  const source = readFileSync(UI + "pages/restore-wizard.js", "utf8");
  const follow = source.slice(source.indexOf("function followRestoreReadiness("),
    source.indexOf("/** The exact step-5 request for one recovery point."));
  assert.match(follow,
    /const painted = await renderAndWire\(node, state, parse, api, lifecycle, true\);[\s\S]*?if \(painted === true && next\.terminal === true && \(now === null \|\| now\.terminal !== true\)\) \{\s*keepVerdictInView\(node, next\);/,
    "NEGATIVE CONTROL: the follow's repaint asks for the verdict once the check turns terminal");
});
