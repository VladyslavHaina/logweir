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
