// mcp-round2.spec.js -- the human-like MCP pass's round 2 (2026-09-25), each
// finding a row with the behaviour it replaced as its NEGATIVE CONTROL.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { resetMode, selectMode, holdsNoRole, sessionIdentity } from "../client.js";
import {
  NAV_ROUTES,
  renderNoRole,
  renderRoleRefusal,
  routeAllowed,
  signedOutAddress,
  visibleRoutes,
} from "../app.js";
import { applicabilityLine, readinessHeadline, readyButForDraftApproval } from "../render.js";
import { readinessRefusal } from "../pages/restore-wizard.js";
import { renderConnectForm } from "../pages/catalog.js";
import { renderCountersignPanel } from "../pages/approvals.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const con = (name) => JSON.parse(readFileSync(FIXTURES + "console/" + name, "utf8"));

// ------------------------------------------------ R2-11: a check still running

test("r2_11_a_running_check_says_checking_not_does_not_apply", () => {
  const pending = { id: "pf-1", state: "pending", terminal: false, applicable: false,
    stale: false, staleReasons: [], staleBasis: [] };
  const html = applicabilityLine(pending);
  assert.match(html, /checking\.\.\./);
  assert.doesNotMatch(html, /does not apply to your current inputs/,
    "NEGATIVE CONTROL: a check that has not answered was labelled out of date");
  assert.doesNotMatch(html, /compared: nothing/);
  for (const state of ["queued", "running"]) {
    assert.match(applicabilityLine(Object.assign({}, pending, { state: state })), /checking\.\.\./);
  }
  // A FINISHED check that no longer applies still says so.
  const done = Object.assign({}, pending, { state: "ready", terminal: true, stale: true,
    staleReasons: [{ reason: "expired" }], staleBasis: ["expiry"] });
  assert.match(applicabilityLine(done), /does not apply to your current inputs/);
});

// ------------------------------------------- R2-12: the readiness headline

const row = (id, gating, state, code) => ({ id: id, gating: gating, state: state, code: code });

/** The PoC's step-5 shape (pf-xq5feiegyugdzwo6yqkzly2ey4): every blocking row
 *  ready but the draft's approval, three execution-only rows unknown. */
function pocRestoreCheck(over) {
  return Object.assign({
    id: "pf-x", operation: "restore", state: "unknown", terminal: true, applicable: true,
    stale: false, staleReasons: [], staleBasis: ["expiry"], binding: {},
    checks: [
      row("destination.resolved", "blocking", "ready", "DestinationValid"),
      row("archive.segments", "blocking", "ready", "SegmentsPresent"),
      row("destination.evidenceWritable", "executionOnly", "unknown", "WriteNotProbed"),
      row("signer.rostered", "executionOnly", "unknown", "SignerKeyIdNotObserved"),
      row("configuration.egress", "executionOnly", "unknown", "ExecutionOnly"),
      row("approval.state", "blocking", "skipped", "SubjectNotCreated"),
    ],
  }, over || {});
}

test("r2_12_a_check_unknown_only_for_its_drafts_approval_headlines_ready", () => {
  const html = readinessHeadline(pocRestoreCheck());
  assert.match(html, /badge-green">ready/,
    "NEGATIVE CONTROL: every restore check headlined 'unknown' while the stepper said Done");
  assert.match(html, /4 items are confirmed when the restore runs/);
  // THE TRUST RULE: any other blocking row not ready keeps the aggregate's word.
  const real = pocRestoreCheck();
  real.checks[1] = row("archive.segments", "blocking", "unknown", "SegmentsUnreadable");
  assert.doesNotMatch(readinessHeadline(real), /badge-green/, "a real unknown blocking row is never green");
  const skipped = pocRestoreCheck();
  skipped.checks[1] = row("archive.segments", "blocking", "skipped", "SkippedByRequest");
  assert.doesNotMatch(readinessHeadline(skipped), /badge-green/, "a skip the request asked for is not a pass");
  const refused = pocRestoreCheck({ state: "notReady" });
  assert.match(readinessHeadline(refused), /not ready/);
  const onlyApproval = pocRestoreCheck({ checks: [row("approval.state", "blocking", "skipped",
    "SubjectNotCreated")] });
  assert.doesNotMatch(readinessHeadline(onlyApproval), /badge-green/,
    "nothing checked is not everything passed");
  // A ready check names what the run itself confirms, and a backup says "run".
  const ready = pocRestoreCheck({ state: "ready", operation: "backup", checks: [
    row("connection.resolved", "blocking", "ready"), row("connection.topicsReadable",
      "executionOnly", "unknown")] });
  assert.match(readinessHeadline(ready), /badge-green">ready.*1 item is confirmed when the run executes/);
});

test("r2_12_the_stepper_and_the_create_gate_read_the_same_rule_as_the_headline", () => {
  // STEP 5 IS `done` EXACTLY WHEN `readinessRefusal` ANSWERS NULL, so the two
  // must agree with the headline over every shape above.
  const shapes = [pocRestoreCheck()];
  const real = pocRestoreCheck();
  real.checks[1] = row("archive.segments", "blocking", "unknown", "SegmentsUnreadable");
  shapes.push(real, pocRestoreCheck({ state: "notReady" }), pocRestoreCheck({ state: "ready" }),
    pocRestoreCheck({ checks: [row("approval.state", "blocking", "skipped", "SubjectNotCreated")] }));
  for (const result of shapes) {
    const passes = readinessRefusal({ readiness: { preflight: result } }, {}) === null;
    const green = /badge-green">ready/.test(readinessHeadline(result));
    assert.equal(passes, green, JSON.stringify(result.checks.map((c) => c.state)) + " " + result.state);
  }
  assert.equal(readyButForDraftApproval(pocRestoreCheck()), true);
});

// ------------------------------------ R2-14: a role that cannot use a route

test("r2_14_a_route_the_session_cannot_use_says_so_instead_of_rendering", () => {
  const restore = NAV_ROUTES.find((r) => r.hash === "#/restore");
  const viewer = (flag) => ["connectionsRead", "backupsRead", "restoresRead", "schedulesRead",
    "approvalsRead", "catalogs", "destinations", "protection", "operationsRead"].includes(flag);
  assert.equal(routeAllowed(restore, "team-a", viewer), false,
    "NEGATIVE CONTROL: the viewer's address rendered the whole wizard");
  assert.equal(routeAllowed(restore, "team-a", () => true), true);
  // THE SAME RULE AS THE TABS: a route is usable exactly when it is a tab.
  for (const route of NAV_ROUTES.filter((r) => r.nav !== false)) {
    const tab = visibleRoutes([route], "team-a", viewer).length === 1;
    assert.equal(routeAllowed(route, "team-a", viewer), tab, route.hash);
  }
  const html = renderRoleRefusal(restore, { displayName: "viewer", roles: ["viewer"],
    namespace: "logweir-poc" });
  assert.match(html, /Your role in logweir-poc can't start restores\./);
  assert.match(html, /viewer in logweir-poc/);
  assert.match(html, /the operator or administrator role/);
  assert.doesNotMatch(html, /Check readiness|Create/, "and nothing actionable");
  // EVERY ROUTE SAYS WHAT IT IS FOR.
  for (const route of NAV_ROUTES) {
    assert.ok(typeof route.can === "string" && route.can.length > 0, route.hash);
    assert.ok(typeof route.takes === "string" && route.takes.length > 0, route.hash);
  }
  // AND THE SHELL ASKS BEFORE IT MOUNTS: the gate sits in `render` ahead of
  // every mount arm.
  const app = readFileSync(UI + "app.js", "utf8");
  const body = app.slice(app.indexOf("function render(lifecycle"), app.indexOf("function boot()"));
  const gate = body.indexOf("!routeAllowed(current, here.ns, sessionHas)");
  assert.ok(gate > 0 && gate < body.indexOf("current.detail(main"), "the gate precedes the mounts");
});

test("r2_14_the_catalog_connect_form_and_the_governed_submit_follow_the_grant", () => {
  const form = renderConnectForm({ ns: "team-a", mayConnect: false, values: {} });
  assert.doesNotMatch(form, /data-connect-archive/,
    "NEGATIVE CONTROL: a viewer saw an enabled Connect archive button");
  assert.match(form, /id="catalog-connect-read-only"/);
  assert.match(renderConnectForm({ ns: "team-a", mayConnect: true, values: {} }), /data-connect-archive/);
  const view = {
    subject: { approvalName: "approval-1" }, policy: { name: "gov" },
    confirmation: { metadata: { name: "c" }, spec: { approvalBytes: "a", sidecarBytes: "s" } },
    countersign: {},
  };
  assert.match(renderCountersignPanel(view), /id="countersign-form"/);
  const reader = renderCountersignPanel(Object.assign({}, view, { maySubmit: false }));
  assert.doesNotMatch(reader, /id="countersign-form"/,
    "NEGATIVE CONTROL: a role without approvalSubmit was offered the submit");
  assert.match(reader, /id="countersign-read-only"/);
});

// --------------------------------------------- R2-14b: sign out forgets the page

test("r2_14b_sign_out_leaves_the_browser_on_the_console_with_no_route", () => {
  assert.equal(signedOutAddress({ pathname: "/ui/", search: "",
    hash: "#/restore?ns=logweir-poc&uid=u-1&step=5" }), "/ui/");
  assert.equal(signedOutAddress({ pathname: "/ui/", search: "?x=1", hash: "#/keys" }), "/ui/?x=1");
  const app = readFileSync(UI + "app.js", "utf8");
  const handler = app.slice(app.indexOf("function renderSession(ns)"), app.indexOf("function focusView("));
  assert.match(handler, /window\.location\.replace\(signedOutAddress\(window\.location\)\)/);
  assert.doesNotMatch(handler, /location\.reload\(\)/,
    "NEGATIVE CONTROL: a reload kept the hash, so the next user signed in to this user's deep link");
});

// ------------------------------------------------ R2-16: a user with no role

test("r2_16_a_session_with_no_role_anywhere_lands_on_a_sentence_that_says_so", async () => {
  const session = con("session-viewer.json");
  session.namespaces = session.namespaces.map((n) => Object.assign({}, n, { roles: [] }));
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: session }) });
  try {
    assert.equal(holdsNoRole(), true);
    const html = renderNoRole(sessionIdentity("team-a"));
    assert.match(html, /You have no role in any namespace yet/);
    assert.match(html, /Ask your Logweir administrator/);
  } finally {
    resetMode();
  }
  // A viewer holds a role; localAdmin is the administrator by construction.
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: con("session-viewer.json") }) });
  assert.equal(holdsNoRole(), false, "NEGATIVE CONTROL: a role held is not no role");
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: con("session.json") }) });
  assert.equal(holdsNoRole(), false, "localAdmin");
  resetMode();
  // THE SHELL LANDS THERE FIRST: before the namespace prompt and every mount.
  const app = readFileSync(UI + "app.js", "utf8");
  const body = app.slice(app.indexOf("function render(lifecycle"), app.indexOf("function boot()"));
  const landing = body.indexOf("holdsNoRole()");
  assert.ok(landing > 0 && landing < body.indexOf("namespacePrompt(context.allowed)"));
});

// ------------------------------- R2-2 / R2-3 / R2-4 / R2-17: tables that fit their card

import { renderClusterList } from "../pages/clusters.js";
import { renderRecoveryPoints, renderScheduleHistory, renderScheduleList } from "../pages/schedules.js";
import { renderCatalogTable, renderPointSelector, renderWizardNav } from "../pages/restore-wizard.js";
import { checkTable, messageText, preflightSentence } from "../render.js";
import { MODE_COPY, SHORT_CHIP_CHARS, URL_CHIP_CHARS, markLongTokens } from "../app.js";
import { CONSOLE } from "../client.js";
import { build, fakeDocument } from "./fake-dom.js";

const headers = (html) => (html.match(/<th scope="col"[^>]*>([^<]*)/g) || [])
  .map((th) => th.replace(/<th scope="col"[^>]*>/, ""));
const CSS = () => readFileSync(UI + "style.css", "utf8");

test("r2_3_the_clusters_table_is_five_columns_and_the_reason_is_a_disclosure", () => {
  const cluster = {
    metadata: { name: "conn-cebbzq4qj4hd3zzyzygy75gwtb", uid: "u-1", namespace: "logweir-poc" },
    spec: { role: "target", bootstrapServers: ["k:9092"], auth: { mode: "plaintext" } },
    status: { reachable: true, reason: "NoExitCode", observedAt: "2026-09-24T16:39:04Z",
      clusterId: "tQmDMMCERvy6yIB-vuOZCQ" },
  };
  const html = renderClusterList({ items: [cluster] }, "logweir-poc", Date.parse("2026-09-25T08:20:00Z"));
  assert.deepEqual(headers(html).filter((h) => h.trim().length > 0).map((h) => h.trim()),
    ["NAME", "CONNECTION PROBE", "CLUSTER-ID", "AUTH"],
    "NEGATIVE CONTROL: eight columns clipped Re-read probe at 1440 px (NAME, ROLE, ..., OBSERVED, REASON)");
  assert.match(html, /conn-cebbzq4qj4hd3zzyzygy75gwtb<\/a><span class="cell-sub">target<\/span>/,
    "the role is the name's second line");
  assert.match(html, /<details class="cell-more"><summary><code>NoExitCode<\/code><\/summary>/,
    "R2-2: the code stays visible and its gloss is one disclosure away");
  assert.match(html, /the reachable reading is from an earlier probe/);
  assert.match(html, /<div class="probe-cell">/);
  // The re-read finds the probe cell by its class, not by counting columns.
  const clusters = readFileSync(UI + "pages/clusters.js", "utf8");
  const paint = clusters.slice(clusters.indexOf("function paintRow("), clusters.indexOf("function paintRow(") + 900);
  assert.match(paint, /querySelector\("\.probe-cell"\)/);
  assert.doesNotMatch(paint, /cells\[2\]/);
});

test("r2_3_every_run_table_folds_its_window_and_slot_into_cells", () => {
  const point = {
    metadata: { name: "logweir-backup-pu2-every5-20260925-081000", uid: "b-1", namespace: "n" },
    spec: { scheduleRef: { name: "pu2" }, slot: "20260925-081000", archive: { url: "logweir-destination://primary" } },
    status: { phase: "Succeeded", backupId: "01JB7X00000000000000000009", records: 200,
      windowCovered: { fromMs: 1, toMs: 2 } },
  };
  const schedule = { metadata: { name: "pu2", uid: "s-1" }, spec: {} };
  const points = headers(renderRecoveryPoints("n", schedule, { items: [point] }));
  assert.deepEqual(points, ["", "BACKUP", "TRIGGER", "COVERAGE", "BACKUP SET", "COVERED", "RECORDS"],
    "NEGATIVE CONTROL: nine columns were 270 px wider than the card at 1440");
  const history = headers(renderScheduleHistory("n", schedule, [point], [], null));
  assert.equal(history.length, 8);
  assert.ok(history.includes("COVERED") && !history.includes("COVERED FROM") && !history.includes("SLOT"));
  assert.deepEqual(headers(renderCatalogTable({ items: [point] }, "")),
    ["BACKUP SET", "COVERED", "RECORDS", "PHASE", "BACKUP"]);
  assert.deepEqual(headers(renderScheduleList({ items: [schedule] }, [], false)),
    ["NAME", "SCHEDULE", "DESTINATION", "SUSPEND", "LAST / NEXT", "READY"]);
});

test("r2_17_the_point_selector_carries_the_archive_under_the_run", () => {
  const point = {
    metadata: { name: "logweir-backup-pu2-every5-20260925-081000", uid: "b-1", namespace: "n" },
    spec: { scheduleRef: { name: "pu2" }, slot: "20260925-081000", topics: ["orders"],
      archive: { url: "logweir-destination://primary" } },
    status: { phase: "Succeeded", backupId: "01JB7X00000000000000000009", records: 200,
      windowCovered: { fromMs: 1, toMs: 2 } },
  };
  const html = renderPointSelector({ ns: "n", backups: { items: [point] }, points: [point], query: "" });
  const cols = headers(html);
  assert.equal(cols.indexOf("ARCHIVE"), -1,
    "NEGATIVE CONTROL: the ARCHIVE column was the one a 1024 px window clipped");
  assert.match(html, /logweir-destination:\/\/primary/, "the archive is still on the row");
});

test("r2_3_the_readiness_check_table_prints_nine_fields_in_four_cells", () => {
  const html = checkTable([{ id: "runner.image", state: "ready", gating: "blocking",
    code: "ImageAvailable", message: "started [imageID=docker-pullable://x@sha256:" + "a".repeat(64) + "]",
    scope: { kind: "Pod", name: "p" }, observedAt: "2026-09-25T08:24:48Z" }], "none");
  assert.deepEqual(headers(html), ["CHECK", "VERDICT", "FINDING", "WHEN"],
    "NEGATIVE CONTROL: nine columns were 1171 px wider than step 5's card at 1440");
  assert.match(html, /class="cell-sub-first prose" data-field="message"/,
    "the message is prose, which may break inside a digest");
  assert.match(CSS(), /\.grid td \.prose \{\n {2}overflow-wrap: anywhere;/);
});

test("r2_3_a_table_that_still_scrolls_shows_it_and_a_laptop_table_is_tighter", () => {
  const css = CSS();
  assert.match(css, /\.table-wrap\[data-scroll-region="true"\] \{\n {4}background:/,
    "R2-17: an overflowing table carries edge shadows");
  assert.match(css, /@media \(max-width: 1279\.98px\) \{\n {4}\.grid th,\n {4}\.grid td \{\n {6}padding-left: var\(--cds-global-space-5\);/);
});

// ----------------------------------------- R2-4 / R2-6: tokens that never break

test("r2_4_r2_6_names_urls_and_short_labels_stay_on_one_line", () => {
  const css = CSS();
  for (const prefix of ["conn-", "sch-", "rst-"]) {
    assert.ok(css.indexOf(".grid td a[href*=\"name=" + prefix + "\"]") !== -1, prefix);
  }
  assert.match(css, /\.grid td code:not\(\.long\),\n {2}\.grid td \.badge\.short \{\n {4}white-space: nowrap;/);
  const doc = fakeDocument();
  const table = build(doc, ["table", { class: "grid" }, ["tbody", {}, ["tr", {},
    ["td", {}, ["code", {}, "s3://kafka-backups/orders"]],
    ["td", {}, ["span", { class: "badge badge-flat" }, "not suspended"]],
    ["td", {}, ["span", { class: "badge badge-green" }, "verified by weirkeeper"]],
    ["td", {}, ["code", {}, "sha256:" + "b".repeat(64)]],
  ]]]);
  markLongTokens(table);
  const chips = table.querySelectorAll("td code, td .badge");
  assert.doesNotMatch(chips[0].getAttribute("class") || "", /long/,
    "NEGATIVE CONTROL: a 25-character URL broke as 'order / s'");
  assert.match(chips[1].getAttribute("class"), /short/, "'not / suspended' no longer breaks");
  assert.doesNotMatch(chips[2].getAttribute("class"), /short/, "a longer label still wraps between words");
  assert.match(chips[3].getAttribute("class") || "", /long/, "a digest still breaks anywhere (MCP-6)");
  assert.equal(URL_CHIP_CHARS, 40);
  assert.equal(SHORT_CHIP_CHARS, 16);
});

// ------------------------------------------------ R2-10: the words of step 5

test("r2_10_backticked_spans_are_code_and_one_topic_is_one_topic", () => {
  assert.equal(messageText("the archive prefix `poc` on destination `primary` is listable"),
    "the archive prefix <code>poc</code> on destination <code>primary</code> is listable",
    "NEGATIVE CONTROL: the backticks were printed");
  assert.equal(messageText("an odd ` backtick"), "an odd ` backtick", "an unpaired one stays");
  assert.equal(messageText("<b> `<i>`"), "&lt;b&gt; <code>&lt;i&gt;</code>", "and it is escaped");
  assert.match(preflightSentence(1), /create 1 topic with/, "NEGATIVE CONTROL: '1 topics'");
  assert.match(preflightSentence(2), /create 2 topics with/);
});

// --------------------------------------------- R2-15: the picker does not move

test("r2_15_the_namespace_picker_sits_in_one_place_for_every_role", () => {
  const css = CSS();
  const link = css.slice(css.indexOf(".nav-link {"), css.indexOf(".nav-link {") + 600);
  assert.match(link, /padding: 0 var\(--cds-global-space-5\);/,
    "NEGATIVE CONTROL: space-6 tabs put an administrator's picker on a second row at 1440");
  assert.match(css, /@media \(max-width: 1199\.98px\) \{\n {2}\.ns-form \{\n {4}flex-basis: 100%;/);
});

// --------------------------------------------------- R2-1 / R2-8: the frame

test("r2_1_the_console_footer_names_no_repository_path", () => {
  const segments = MODE_COPY[CONSOLE].colophon.map((s) => (Array.isArray(s) ? s[1] : s)).join("");
  assert.doesNotMatch(segments, /README/, "NEGATIVE CONTROL: the footer pointed users at ui/README.md");
  const html = readFileSync(UI + "index.html", "utf8").replace(/<!--[\s\S]*?-->/g, "");
  assert.doesNotMatch(html.slice(html.indexOf("<body>")), /ui\/README\.md/);
});

test("r2_8_back_on_the_first_step_is_disabled_and_not_drawn", () => {
  assert.match(renderWizardNav({ step: 0 }), /id="wizard-back" data-step="0" disabled/);
  assert.doesNotMatch(renderWizardNav({ step: 2 }), /id="wizard-back"[^>]*disabled/);
  assert.match(CSS(), /\.wizard-nav \.wizard-go:disabled \{\n {2}visibility: hidden;/);
});
