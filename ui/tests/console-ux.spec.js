// console-ux.spec.js -- the rows for the console UX batch (console-ux-1): the
// findings MCP-1 ... MCP-34 of the human-like Playwright-MCP pass over the
// shared console (2026-09-24). Each row names its finding and fails on the
// code before the fix where the behaviour changed.
//
// Like every suite here it is node's own test runner over pure functions and
// the real transport with the one platform call stubbed; it opens no socket.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  CONSOLE,
  LEGACY,
  SIGNED_OUT,
  apiClient,
  mode,
  resetMode,
  selectMode,
  sessionIdentity,
  signOut,
  signedOutReason,
} from "../client.js";
import { apiError } from "../api.js";
import { errorBlock, errorParts, signInHref } from "../render.js";
import {
  NAV_ROUTES,
  modeDecided,
  namespacePrompt,
  renderIdentity,
  renderSignIn,
  visibleRoutes,
} from "../app.js";
import { fakeDocument } from "./fake-dom.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const con = (name) => fixture("console/" + name);

/** Replaces the one platform call `ui/api.js` makes and records every request. */
function transport(answer) {
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (u, init) => {
    seen.push({ url: u, init: init || {} });
    const reply = answer(u, init || {});
    if (reply === undefined || reply === null) {
      return Promise.reject(new Error("the suite has no answer for " + String(u)));
    }
    const text = reply.body === undefined ? "" : JSON.stringify(reply.body);
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      headers: { get: () => null },
      text: () => Promise.resolve(text),
    });
  };
  return { seen: seen, restore: () => { globalThis.fetch = original; } };
}

/** The product API's own 401 for `/api/v1/session` (`problem.rs`). */
function unauthenticated(code) {
  return {
    type: "https://logweir.dev/problems/" + (code === "session_expired" ? "session-expired" : "unauthenticated"),
    title: code === "session_expired" ? "Session expired" : "Authentication required",
    status: 401,
    code: code || "unauthenticated",
    detail: "This request carries no session. Sign in at /auth/login.",
    requestId: "01M3A6T004K6TEVESNGBZZ2JCY",
    retryable: false,
  };
}

/** An OIDC operator's session over the checked-in session document. */
function operatorSession(extra) {
  const session = con("session.json");
  session.authenticationMode = "oidc";
  session.actor = { id: "https://dex.example#ada", issuer: "https://dex.example", subject: "ada", displayName: "Ada Operator" };
  session.csrfToken = "tok-mcp-5";
  session.bindingRevision = "r7";
  session.namespaces[0].roles = ["operator"];
  return Object.assign(session, extra || {});
}

async function probeWith(answer) {
  resetMode();
  return selectMode({ probe: async () => answer });
}

// ===========================================================================
// MCP-1 / MCP-4: a signed-out visitor is signed out, not legacy
// ===========================================================================

test("mcp_1_a_product_api_401_on_session_is_signed_out_and_never_legacy", async () => {
  const record = await probeWith({ ok: false, status: 401, body: unauthenticated("unauthenticated") });
  assert.equal(record.mode, SIGNED_OUT, "the shared console with nobody signed in");
  assert.equal(mode(), SIGNED_OUT);
  assert.notEqual(mode(), LEGACY, "BEFORE: a 401 was read as 'not the product API' -- legacy mode");
  assert.equal(signedOutReason(), "unauthenticated");

  await probeWith({ ok: false, status: 401, body: unauthenticated("session_expired") });
  assert.equal(signedOutReason(), "session_expired", "an expired session is told so");
});

test("mcp_1_a_kubernetes_401_through_kubectl_proxy_stays_legacy", async () => {
  // kubectl proxy forwards /api/v1/session to kube-apiserver; a proxy whose own
  // credential expired answers a Kubernetes Status with a NUMBER in `code`.
  const status = { kind: "Status", apiVersion: "v1", status: "Failure", message: "Unauthorized", reason: "Unauthorized", code: 401 };
  assert.equal((await probeWith({ ok: false, status: 401, body: status })).mode, LEGACY);
  assert.equal((await probeWith({ ok: false, status: 404, body: null })).mode, LEGACY);
  assert.equal((await probeWith({ ok: false, status: 401, body: null })).mode, LEGACY,
    "a 401 with no problem document is not the product API asking for a sign-in");
  assert.equal(signedOutReason(), null);
});

test("mcp_4_signed_out_the_client_sends_nothing_and_refuses_by_name", async () => {
  await probeWith({ ok: false, status: 401, body: unauthenticated() });
  const wire = transport(() => ({ status: 200, body: {} }));
  try {
    const api = apiClient();
    await assert.rejects(api.list("logweir-poc", "backups"), (error) => {
      assert.equal(error.status, 401);
      assert.equal(error.reason, "unauthenticated");
      assert.match(error.message, /not signed in/);
      return true;
    });
    await assert.rejects(api.destinations("logweir-poc"), /not signed in/);
    assert.equal(wire.seen.length, 0,
      "BEFORE: the legacy half asked /apis/logweir.dev/... and the console answered 404 JSON");
  } finally {
    wire.restore();
  }
});

test("mcp_1_the_sign_in_page_is_one_action_that_returns_to_the_address", () => {
  const html = renderSignIn("unauthenticated", "#/backups?ns=logweir-poc");
  assert.ok(html.includes("id=\"sign-in-link\""), "a Sign in action");
  assert.ok(html.includes(">Sign in</a>"));
  assert.ok(
    html.includes("href=\"/auth/login?next=%2Fui%2F%23%2Fbackups%3Fns%3Dlogweir-poc\""),
    "the link returns to the page that was asked for (the API accepts `next` under /ui/)",
  );
  assert.equal(html.indexOf("Choose a namespace"), -1, "no legacy namespace prompt");
  assert.equal(html.indexOf("{\""), -1, "and no raw document");
  const expired = renderSignIn("session_expired", "");
  assert.match(expired, /Your session has ended/);
  assert.ok(expired.includes("href=\"/auth/login?next=%2Fui%2F\""));
  assert.equal(signInHref("not-a-hash"), "/auth/login?next=%2Fui%2F", "only a hash is carried");
});

test("mcp_1_modeDecided_writes_the_console_copy_for_a_signed_out_visitor", () => {
  const doc = fakeDocument();
  const tagline = doc.createElement("p");
  tagline.setAttribute("id", "masthead-tagline");
  tagline.appendChild(doc.createTextNode("neutral"));
  doc.body.appendChild(tagline);
  modeDecided({ mode: SIGNED_OUT }, doc, { allowed: [], selected: "" }, () => {});
  assert.match(doc.getElementById("masthead-tagline").textContent, /Logweir product API/,
    "a signed-out visitor is in the shared console, and the masthead says so");
  assert.equal(doc.getElementById("masthead-tagline").textContent.indexOf("kubeconfig"), -1);
});

// ===========================================================================
// MCP-4: a problem document is a human message, everywhere
// ===========================================================================

test("mcp_4_a_problem_document_on_a_kubernetes_path_is_read_as_one", () => {
  const body = JSON.stringify({
    type: "https://logweir.dev/problems/not-found", title: "Not found", status: 404,
    code: "not_found", detail: "No such resource.", requestId: "01M3A6T004K6TEVESNGBZZ2JCY",
    retryable: false,
  });
  const error = apiError({ status: 404 }, body);
  assert.equal(error.message, "No such resource.", "BEFORE: the message was the whole JSON document");
  assert.equal(error.reason, "not_found");
  assert.equal(error.requestId, "01M3A6T004K6TEVESNGBZZ2JCY");
  const html = errorBlock(error);
  assert.equal(html.indexOf("{&quot;"), -1, "no JSON in the error box");
  assert.match(html, /Not found/);
  assert.match(html, /HTTP 404 not_found/);

  // JSON that is neither a Status nor a problem is DESCRIBED, never dumped.
  const odd = apiError({ status: 500 }, JSON.stringify({ unexpected: true }));
  assert.equal(odd.message.indexOf("{"), -1);
  assert.match(odd.message, /does not recognise/);
  // And a Kubernetes Status keeps its own reason and message verbatim.
  const k8s = apiError({ status: 403 }, JSON.stringify({ kind: "Status", reason: "Forbidden", message: "backups is forbidden" }));
  assert.equal(k8s.message, "backups is forbidden");
  assert.equal(k8s.reason, "Forbidden");
});

test("mcp_4_the_error_box_leads_with_words_and_keeps_the_servers_own", () => {
  const forbidden = errorParts({ status: 403, reason: "Forbidden", message: "backups is forbidden" });
  assert.equal(forbidden.title, "You do not have permission to do this");
  assert.equal(forbidden.message, "backups is forbidden", "the server's message, verbatim");
  assert.ok(forbidden.detail.includes("403 Forbidden"), "and its status and reason");
  assert.equal(forbidden.signIn, false);

  const signedOut = errorBlock({ status: 401, reason: "unauthenticated", message: "no session" });
  assert.match(signedOut, /You are not signed in/);
  assert.ok(signedOut.includes("href=\"/auth/login?next=%2Fui%2F\""), "a sign-in refusal offers Sign in");
  const expired = errorBlock({ status: 401, reason: "session_expired", message: "expired" });
  assert.match(expired, /Your session has ended/);
  // A Kubernetes 401 is a proxy's credential, and no sign-in fixes it.
  const proxy = errorBlock({ status: 401, reason: "Unauthorized", message: "Unauthorized" });
  assert.equal(proxy.indexOf("/auth/login"), -1);
  assert.match(proxy, /Not authenticated/);
});

// ===========================================================================
// MCP-5: who is signed in, in which role, and Sign out
// ===========================================================================

test("mcp_5_the_header_names_the_signed_in_actor_and_their_role_here", async () => {
  await probeWith({ ok: true, status: 200, body: operatorSession() });
  assert.equal(mode(), CONSOLE);
  const identity = sessionIdentity("team-a");
  const html = renderIdentity(identity);
  assert.match(html, /Ada Operator/, "the display claim");
  assert.match(html, /operator in team-a/, "the role this session holds in the chosen namespace");
  assert.ok(html.includes("id=\"sign-out\""), "and Sign out");
  assert.ok(html.includes("title=\"ada\""), "the subject is one hover away");
  assert.match(renderIdentity(sessionIdentity("")), /choose a namespace to see your role/);
  assert.match(renderIdentity(sessionIdentity("elsewhere")), /no role in elsewhere/);
});

test("mcp_5_a_local_admin_console_names_its_one_actor_and_offers_no_sign_out", async () => {
  await probeWith({ ok: true, status: 200, body: con("session.json") });
  const html = renderIdentity(sessionIdentity("team-a"));
  assert.match(html, /Local administrator/);
  assert.equal(html.indexOf("sign-out"), -1, "localAdmin has no sign-in, so no Sign out");
  await probeWith({ ok: false, status: 404, body: null });
  assert.equal(renderIdentity(sessionIdentity("team-a")), "", "legacy mode names no session");
});

test("mcp_5_sign_out_posts_the_logout_command_with_the_sessions_token", async () => {
  await probeWith({ ok: true, status: 200, body: operatorSession() });
  const wire = transport(() => ({ status: 204 }));
  try {
    assert.equal(await signOut(), true);
    assert.equal(wire.seen.length, 1);
    assert.equal(wire.seen[0].url, "/api/v1/session/logout", "docs/api.md's logout route");
    assert.equal(wire.seen[0].init.method, "POST", "an unsafe method, never a GET");
    assert.equal(wire.seen[0].init.headers["X-CSRF-Token"], "tok-mcp-5", "the session's own token");
    assert.equal(wire.seen[0].init.headers["Content-Type"], "application/json");
  } finally {
    wire.restore();
  }
  await probeWith({ ok: false, status: 404, body: null });
  const none = transport(() => ({ status: 204 }));
  try {
    await assert.rejects(signOut(), /no signed-in session/);
    assert.equal(none.seen.length, 0, "legacy mode has no session to end and sends nothing");
  } finally {
    none.restore();
  }
});

// ===========================================================================
// MCP-19 / MCP-33: the navigation
// ===========================================================================

test("mcp_33_the_navigation_shows_the_tabs_this_session_may_use", () => {
  const titles = (has) => visibleRoutes(NAV_ROUTES, "team-a", has).map((r) => r.title);
  const everything = titles(() => true);
  assert.ok(everything.includes("Keys"), "a session that may read trust sees Keys");
  assert.equal(everything.indexOf("Operations"), -1,
    "MCP-19: Operations is one run's page, reached from a run, and is not a tab");
  const operator = titles((flag) => flag !== "trustPoliciesRead");
  assert.equal(operator.indexOf("Keys"), -1, "BEFORE: an operator saw Keys, which answers 403");
  assert.ok(operator.includes("Backups") && operator.includes("Restore"));
  const approver = titles((flag) => ["backupsRead", "restoresRead", "approvalsRead", "operationsRead"].includes(flag));
  assert.deepEqual(approver, ["Backups", "History", "Approvals"], "an approver's tabs");
  // The operation ROUTE is still there for every link to one run.
  assert.ok(NAV_ROUTES.some((r) => r.hash === "#/operations"));
});

// ===========================================================================
// MCP-2 / MCP-3 / MCP-8 / MCP-34: the frame's proportions
// ===========================================================================

test("mcp_2_the_namespace_prompt_is_an_instruction_without_a_spinner", () => {
  const saved = globalThis.document;
  globalThis.document = fakeDocument();
  try {
    const prompt = namespacePrompt([]);
    assert.equal(prompt.getAttribute("class"), "prompt", "BEFORE: class `pending`, which draws a spinner");
    assert.match(prompt.textContent, /Choose a namespace/);
  } finally {
    globalThis.document = saved;
  }
  const css = readFileSync(UI + "style.css", "utf8");
  const rule = css.slice(css.indexOf(".prompt {"), css.indexOf("}", css.indexOf(".prompt {")));
  assert.equal(rule.indexOf("animation"), -1);
  assert.equal(css.indexOf(".prompt::before"), -1, "no spinner glyph on the prompt");
});

test("mcp_3_mcp_8_mcp_34_the_frame_draws_no_view_ring_no_wrapped_button_no_stuck_hover", () => {
  const css = readFileSync(UI + "style.css", "utf8");
  assert.match(css, /#view-slot\[tabindex\]:focus-visible \{\n {2}outline: none;/,
    "MCP-3: the view the router focuses is not drawn as a focused control");
  const button = css.slice(css.indexOf("\nbutton {"), css.indexOf("}", css.indexOf("\nbutton {")));
  assert.match(button, /white-space: nowrap;/, "MCP-8: a button caption never wraps onto two lines");
  const hoverAt = css.indexOf(".nav-link:hover");
  const mediaAt = css.lastIndexOf("@media (hover: hover)", hoverAt);
  assert.ok(mediaAt !== -1 && hoverAt - mediaAt < 80,
    "MCP-34: the tab hover background only applies where a pointer hovers");
});

// ===========================================================================
// MCP-29 / MCP-27 / MCP-25 / MCP-28 / MCP-26: the restore wizard
// ===========================================================================

import {
  STEPS,
  firstStepWithErrors,
  initialState,
  mountRestoreWizard,
  recoveryPoints,
  renderArchiveStep,
  renderPointSelector,
  renderPreparedWizard,
  preparePlan,
  restoreRouteParams,
  restoreStepRoute,
  stepStates,
} from "../pages/restore-wizard.js";
import { replayWizard } from "../workflow.js";
import { createRouteLifecycle } from "../app.js";
import { fakeView, parse as viewParse } from "./fake-view.js";

const BACKUPS = () => fixture("wizard-backups.json");
const CLUSTERS = () => fixture("wizard-clusters.json");

function newest(list) {
  const point = recoveryPoints(list)[0];
  return { uid: point.metadata.uid, backup: point.metadata.name };
}

/** The pages of a rendered wizard: which step each is, and whether it shows. */
function pagesOf(html) {
  return Array.from(html.matchAll(/<div class="wizard-page" data-wizard-step="(\d)"( hidden)?>/g))
    .map((m) => ({ step: Number(m[1]), shown: m[2] === undefined }));
}

test("mcp_29_the_wizard_shows_one_step_at_a_time_with_back_and_next", async () => {
  const state = initialState("team-a", CLUSTERS(), BACKUPS(), newest(BACKUPS()));
  const html = renderPreparedWizard(state, await preparePlan(state));
  const pages = pagesOf(html);
  assert.deepEqual(pages.map((p) => p.step), [0, 1, 2, 3, 4, 5],
    "all six steps are in the page -- every input is still read, drafted and validated");
  assert.deepEqual(pages.filter((p) => p.shown).map((p) => p.step), [0],
    "BEFORE: all six were shown at once, a 22,686 px column; now one is");
  assert.match(html, /id="wizard-back" data-step="0" disabled>Back<\/button>/, "no Back on step 1");
  assert.match(html, /id="wizard-next" data-step="1">Next: Recovery point<\/button>/);
  assert.match(html, /Step 1 of 6: Archive/);
  assert.equal((html.match(/aria-current="step"/g) || []).length, 1);
  assert.match(html, /<button type="button" class="stepper-link" data-target="step-archive" data-step="0" aria-current="step">/,
    "the stepper marks the step on screen");

  state.step = 5;
  const last = renderPreparedWizard(state, await preparePlan(state));
  assert.deepEqual(pagesOf(last).filter((p) => p.shown).map((p) => p.step), [5]);
  assert.equal(last.indexOf("id=\"wizard-next\""), -1, "the last step's action is Create, not Next");
  assert.ok(last.includes("id=\"create-restore\""));
});

test("mcp_29_a_deep_link_names_a_step_and_the_uid_pinned_route_is_unchanged", () => {
  const point = newest(BACKUPS());
  const hash = "#/restore?ns=team-a&backup=" + point.backup + "&uid=" + point.uid + "&step=3";
  const params = restoreRouteParams(hash);
  assert.equal(params.step, 3);
  assert.equal(params.uid, point.uid, "the identity is read exactly as before");
  const state = initialState("team-a", CLUSTERS(), BACKUPS(), params);
  assert.equal(state.step, 2, "step 3 of 6 is the third page");
  assert.equal(state.pointState, "selected", "and the pinned point still resolves by uid");
  for (const bad of ["0", "7", "x", "2.5", ""]) {
    assert.equal(restoreRouteParams("#/restore?step=" + bad).step, 0, "`" + bad + "` names no step");
  }
  const moved = restoreStepRoute(hash, 4);
  assert.equal(moved, "#/restore?ns=team-a&backup=" + point.backup + "&uid=" + point.uid + "&step=5",
    "the address keeps every parameter and carries the step on screen");
  assert.equal(restoreStepRoute("#/restore?ns=a", 0), "#/restore?ns=a&step=1");
});

test("mcp_29_next_back_and_the_stepper_move_between_steps_and_keep_the_address", async () => {
  const point = newest(BACKUPS());
  const hash = "#/restore?ns=team-a&backup=" + point.backup + "&uid=" + point.uid;
  const saved = globalThis.window;
  const location = { hash: hash };
  globalThis.window = {
    location: location,
    history: { state: null, replaceState(_s, _t, url) { location.hash = url; } },
  };
  try {
    const route = createRouteLifecycle().begin(hash);
    const view = fakeView();
    const api = {
      list: async (_ns, plural) => (plural === "kafkaclusters" ? CLUSTERS() : BACKUPS()),
      approvalPolicy: async () => null,
    };
    await mountRestoreWizard(view.root, "team-a", restoreRouteParams(hash), viewParse, api, route);
    assert.deepEqual(pagesOf(view.html()).filter((p) => p.shown).map((p) => p.step), [0]);
    await view.find("#wizard-next").dispatch("click");
    assert.deepEqual(pagesOf(view.html()).filter((p) => p.shown).map((p) => p.step), [1],
      "Next shows step 2");
    assert.match(location.hash, /&step=2$/, "and writes it into the address");
    assert.equal(route.isCurrent(), true,
      "the route is still current at its new address, so reads and the submit still run");
    await view.find("#wizard-back").dispatch("click");
    assert.deepEqual(pagesOf(view.html()).filter((p) => p.shown).map((p) => p.step), [0], "Back");
    const toPlan = view.findAll(".stepper-link").find((b) => b.getAttribute("data-step") === "5");
    await toPlan.dispatch("click");
    assert.deepEqual(pagesOf(view.html()).filter((p) => p.shown).map((p) => p.step), [5],
      "a stepper entry shows its own step");
    assert.match(location.hash, /&step=6$/);
    // The plan on step 6 is the plan every step contributed to: one hash.
    assert.equal((view.html().match(/id="plan-hash-value"/g) || []).length, 1);
  } finally {
    globalThis.window = saved;
  }
});

test("mcp_29_a_route_is_retargeted_only_to_its_own_path_and_only_while_current", () => {
  const saved = globalThis.window;
  globalThis.window = { location: { hash: "#/restore?ns=a" } };
  try {
    const routes = createRouteLifecycle();
    const route = routes.begin("#/restore?ns=a");
    assert.equal(route.retarget("#/backups?ns=a"), false, "another route is a navigation, not a step");
    assert.equal(route.retarget("#/restore?ns=a&step=2"), true);
    globalThis.window.location.hash = "#/restore?ns=a&step=2";
    assert.equal(route.isCurrent(), true);
    routes.begin("#/history?ns=a");
    assert.equal(route.retarget("#/restore?ns=a&step=3"), false, "a route that has ended is not moved");
  } finally {
    globalThis.window = saved;
  }
});

test("mcp_29_a_refused_submit_shows_the_step_its_field_message_is_about", () => {
  assert.equal(firstStepWithErrors({ topicPrefix: ["prefix refused"] }), 3, "the target step");
  assert.equal(firstStepWithErrors({ pointInTime: ["outside"], topics: ["none"] }), 2,
    "the earliest step with a message");
  assert.equal(firstStepWithErrors({ ticket: ["required"] }), 5);
  assert.equal(firstStepWithErrors({ archiveSecret: [] }), null, "an empty list is no message");
  assert.equal(firstStepWithErrors(null), null);
});

test("mcp_27_readiness_is_done_only_when_a_check_for_this_plan_says_ready", async () => {
  const state = initialState("team-a", CLUSTERS(), BACKUPS(), newest(BACKUPS()));
  const prepared = await preparePlan(state);
  const none = stepStates(state, prepared);
  assert.equal(none[4].status, "unchecked",
    "BEFORE: `done` as soon as a point was chosen, beside 'No readiness check has run'");
  assert.equal(none[5].status, "ready", "an unchecked plan may still be created, with a warning");
  assert.equal(replayWizard(none).current, "submitted", "and the wizard machine walks past it");

  state.readiness = {
    boundHash: prepared.hash,
    preflight: {
      id: "pf-1", terminal: true, applicable: true, stale: false, state: "ready",
      binding: { planHash: prepared.hash }, checks: [{ id: "target.mappedTopics", gating: "blocking", state: "ready", code: "Ok" }],
    },
  };
  assert.equal(stepStates(state, prepared)[4].status, "done", "a ready check for this plan is done");

  state.readiness.preflight = Object.assign({}, state.readiness.preflight, {
    state: "notReady",
    checks: [{ id: "target.mappedTopics", gating: "blocking", state: "notReady", code: "MappedTopicExists" }],
  });
  const refused = stepStates(state, prepared);
  assert.equal(refused[4].status, "attention", "a check that refuses this plan needs attention");
  assert.equal(refused[4].current, true, "and it is where the reader is sent");
  assert.equal(refused[5].status, "todo", "the plan step is not ready while Create refuses");
  const html = renderPreparedWizard(state, prepared);
  assert.match(html, /<span class="stepper-status">needs attention<\/span>/);

  // REVIEW L1: a check owed only its draft's approval is passable -- Create
  // requests the approval -- and step 5 says "needs approval", not "done".
  state.readiness.preflight = Object.assign({}, state.readiness.preflight, {
    state: "unknown",
    checks: [
      { id: "target.mappedTopics", gating: "blocking", state: "ready", code: "Ok" },
      { id: "approval.state", gating: "blocking", state: "skipped", code: "SubjectNotCreated" },
    ],
  });
  const owed = stepStates(state, prepared);
  assert.equal(owed[4].status, "approval", "NEGATIVE CONTROL: it read `done`");
  assert.equal(owed[5].status, "ready", "and the plan step is still where Create is");
  assert.equal(replayWizard(owed).current, "submitted", "and the wizard machine walks past it");
  assert.match(renderPreparedWizard(state, prepared),
    /<span class="stepper-status">needs approval<\/span>/);
});

test("mcp_25_every_recovery_point_row_leads_with_its_action", () => {
  const state = initialState("team-a", CLUSTERS(), BACKUPS(), {});
  const html = renderPointSelector(state);
  const body = html.slice(html.indexOf("<tbody>"), html.indexOf("</tbody>"));
  const rows = body.split("<tr").slice(1);
  assert.ok(rows.length >= 1);
  for (const row of rows) {
    const first = row.slice(row.indexOf("<td>") + 4, row.indexOf("</td>"));
    assert.match(first, /^<a class="button" href="#\/restore\?ns=team-a&amp;backup=[^"]+&amp;uid=[^"]+">Restore this point<\/a>$/,
      "BEFORE: the action was the tenth column and clipped at 1440 px: " + first);
  }
  const head = html.slice(html.indexOf("<thead>"), html.indexOf("</thead>"));
  assert.ok((head.match(/<th /g) || []).length <= 7, "and the table is seven columns, not ten");
});

test("mcp_28_the_wizard_connection_probe_is_a_cell_not_a_paragraph", () => {
  const state = initialState("team-a", CLUSTERS(), BACKUPS(), newest(BACKUPS()));
  state.now = Date.parse("2026-09-11T20:00:00Z");
  const html = renderArchiveStep(state);
  const body = html.slice(html.indexOf("<tbody>"), html.indexOf("</tbody>"));
  assert.match(body, /class="probe-summary"/);
  const visible = body.replace(/title="[^"]*"/g, "");
  assert.equal(visible.indexOf("freshness budget"), -1,
    "BEFORE: 'this observation is older than the 630s freshness budget...' in every cell");
  assert.ok(body.includes("freshness budget"), "the sentence is one hover away, in the title");
});

test("mcp_26_the_selector_renders_before_the_catalog_read_answers", async () => {
  const view = fakeView();
  const calls = [];
  let release;
  const catalogs = new Promise((resolve) => { release = resolve; });
  let listed = 0;
  const api = {
    list: async (_ns, plural) => {
      calls.push("list:" + plural);
      listed += 1;
      return plural === "kafkaclusters" ? CLUSTERS() : BACKUPS();
    },
    approvalPolicy: async () => { calls.push("policy"); return null; },
    catalogReaders: {
      listCatalogs: async () => {
        calls.push("catalogs:" + String(listed));
        await catalogs;
        throw new Error("the catalogs answered late and refused");
      },
      readPoints: async () => ({ items: [], page: {} }),
      readCatalog: async () => ({}),
      ownVerdict: async () => ({}),
    },
  };
  const mounted = mountRestoreWizard(view.root, "team-a", {}, viewParse, api,
    { signal: undefined, isCurrent: () => true });
  for (let i = 0; i < 10; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  assert.ok(calls.indexOf("catalogs:0") !== -1,
    "the catalog read starts beside the two lists, not after them: " + calls.join(","));
  assert.ok(view.html().includes("id=\"step-select-point\""),
    "BEFORE: nothing rendered until the slowest read answered; the selector is up now");
  assert.ok(view.html().includes("id=\"catalog-offers-pending\""), "and says the rest is coming");
  release();
  await mounted;
  assert.match(view.html(), /data-catalog-note="true">the connected archives could not be listed here/,
    "and the catalog section is filled in when its read answers");
});

test("mcp_26_a_console_list_hands_each_page_to_the_caller_before_the_last", async () => {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: con("session.json") }) });
  const first = con("backups-list.json");
  const pageOne = JSON.parse(JSON.stringify(first));
  pageOne.page = Object.assign({}, pageOne.page, { nextCursor: "c2" });
  const wire = transport((u) => ({ status: 200, body: String(u).includes("cursor=c2") ? first : pageOne }));
  try {
    const seen = [];
    const whole = await apiClient().list("team-a", "backups", { onPage: (partial) => seen.push(partial) });
    assert.equal(seen.length, 1, "one page before the last");
    assert.equal(seen[0].__page.partial, true, "marked partial, never passed off as the whole");
    assert.equal(seen[0].items.length, pageOne.items.length);
    assert.equal(whole.items.length, pageOne.items.length + first.items.length, "the whole list");
    assert.equal(whole.__page.partial, undefined);
    assert.ok(wire.seen.every((r) => !String(r.url).includes("onPage")), "the callback is not a query");
  } finally {
    wire.restore();
  }
});

// ===========================================================================
// MCP-6: a short chip never breaks mid-token
// ===========================================================================

import { LONG_TOKEN_CHARS, markLongTokens } from "../app.js";
import { build } from "./fake-dom.js";

test("mcp_6_only_a_long_token_in_a_table_may_break_mid_word", () => {
  const doc = fakeDocument();
  const table = build(doc, ["table", { class: "grid" }, ["tbody", {},
    ["tr", {},
      ["td", {}, ["code", {}, "NoExitCode"]],
      ["td", {}, ["span", { class: "badge badge-green" }, "verified by weirkeeper"]],
      ["td", {}, ["code", {}, "sha256:c55747f788f30e6923b9a3500b407688499f7e7c1df0af629e6dab86c33e64f4"]],
      ["td", {}, ["span", { class: "badge badge-green" }, "against key sha256:" + "a".repeat(64)]],
    ]]]);
  markLongTokens(table);
  const chips = table.querySelectorAll("code, .badge");
  assert.equal(chips[0].getAttribute("class"), null, "NoExitCode stays one token");
  assert.equal(chips[1].getAttribute("class"), "badge badge-green", "a label of short words too");
  assert.equal(chips[2].getAttribute("class"), "long", "a digest may break");
  assert.equal(chips[3].getAttribute("class"), "badge badge-green long");
  assert.ok(LONG_TOKEN_CHARS >= "NoExitCode".length);
  const css = readFileSync(UI + "style.css", "utf8");
  assert.match(css, /\.grid td code,\n\.grid td \.badge \{\n {2}overflow-wrap: normal;/,
    "BEFORE: `.grid td code { overflow-wrap: anywhere }` broke every chip mid-token");
  assert.match(css, /\.grid td code\.long,\n\.grid td \.badge\.long \{\n {2}overflow-wrap: anywhere;/);
});

// ===========================================================================
// MCP-13 / MCP-17: two projections the API now publishes
// ===========================================================================

import { renderLastSlot, renderScheduleRevision } from "../pages/schedules.js";
import { renderBackupList } from "../pages/backups.js";
import { renderHistoryList } from "../pages/history.js";

test("mcp_13_the_console_shows_what_the_last_slot_did_from_the_api", async () => {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: con("session.json") }) });
  // THE SAME FILE `crates/logweir-api/tests/list_projection_additions.rs`
  // asserts is the API's projection of `schedule-policy.json`.
  const golden = con("schedule-last-slot.json");
  const wire = transport(() => ({ status: 200, body: golden }));
  try {
    const object = await apiClient().get("team-a", "backupschedules", golden.item.name);
    assert.equal(object.status.lastSlot.disposition, "Admitted");
    assert.equal(object.__contract.absent.indexOf("status.lastSlot"), -1,
      "a field the API supplied is not named absent");
    const html = renderLastSlot(object);
    assert.match(html, /data-last-slot="1"/,
      "BEFORE: 'read the schedule with kubectl' -- the projection did not exist");
    assert.match(html, /Admitted/);
    assert.match(html, /20260918-032200/);
    assert.equal(html.indexOf("kubectl"), -1);
  } finally {
    wire.restore();
  }
  // A SCHEDULE WITH NO DECIDED SLOT YET publishes neither, and the card says
  // nothing about a projection that no longer exists -- the fields are in the
  // contract now, so their absence is "nothing recorded yet".
  const fresh = con("schedule-last-slot.json");
  delete fresh.item.status.lastSlot;
  delete fresh.item.status.missedSlots;
  const again = transport(() => ({ status: 200, body: fresh }));
  try {
    const object = await apiClient().get("team-a", "backupschedules", fresh.item.name);
    assert.equal(object.__contract.absent.indexOf("status.lastSlot"), -1);
    assert.equal(renderLastSlot(object), "", "no slot yet is no panel, and no kubectl");
  } finally {
    again.restore();
  }
});

test("mcp_13_the_revision_line_keeps_the_digest_and_tz_database_one_disclosure_away", () => {
  const cr = fixture("schedule-policy.json");
  const html = renderScheduleRevision(cr);
  const sentence = html.slice(0, html.indexOf("</p>"));
  assert.equal(sentence.indexOf("sha256:"), -1, "no digest in the sentence an operator reads");
  assert.equal(sentence.indexOf("chrono-tz"), -1, "and no library version");
  assert.match(sentence, /Revision g\d+, in force since <time class="ts"/);
  const details = html.slice(html.indexOf("<details class=\"technical\">"));
  assert.match(details, /sha256:/, "the digest is still on the page");
  assert.match(details, /chrono-tz/);
});

test("mcp_17_a_console_list_row_shows_the_exit_code_and_the_restore_outcome", async () => {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: con("session.json") }) });
  const backups = con("backups-list.json");
  backups.items[0].operation.exitCode = 2;
  const wire = transport(() => ({ status: 200, body: backups }));
  let list;
  try {
    list = await apiClient().list("team-a", "backups");
  } finally {
    wire.restore();
  }
  assert.equal(list.items[0].status.exitCode, 2);
  const html = renderBackupList(list, "team-a");
  const row = html.slice(html.indexOf("<tbody>"), html.indexOf("</tr>", html.indexOf("<tbody>")));
  const cells = row.split("<td").slice(1).map((c) => c.slice(c.indexOf(">") + 1, c.indexOf("</td>")));
  assert.equal(cells[3], "2", "BEFORE: EXIT read `-` on every console row");

  const restores = con("restores-list.json");
  restores.items[0].operation.outcome = "pass";
  restores.items[0].operation.verifiedSuccess = false;
  const w2 = transport(() => ({ status: 200, body: restores }));
  let rlist;
  try {
    rlist = await apiClient().list("team-a", "restores");
  } finally {
    w2.restore();
  }
  const history = renderHistoryList(rlist, { items: [] }, "team-a");
  assert.match(history, /pass/, "the RESULT column carries the outcome");
  assert.match(history, /unverified scorecard claim/,
    "and a restore that did not verify keeps its outcome labelled as a claim");
  // A VERIFIED row's outcome is not a claim: the SIGNED column says verified.
  const green = con("restores-list.json");
  green.items[0].operation.outcome = "pass";
  green.items[0].operation.verifiedSuccess = true;
  green.items[0].operation.verificationState = "valid";
  const w3 = transport(() => ({ status: 200, body: green }));
  let glist;
  try {
    glist = await apiClient().list("team-a", "restores");
  } finally {
    w3.restore();
  }
  const verified = renderHistoryList(glist, { items: [] }, "team-a");
  assert.match(verified, /verified by weirkeeper/);
  assert.doesNotMatch(verified, /unverified scorecard claim/,
    "a row the SIGNED column calls verified does not call its outcome unverified");
});

// ===========================================================================
// MCP-10 / MCP-20 / MCP-23 / MCP-32: forms and empty states that fit the role
// ===========================================================================

import { SOURCE_INPUTS, renderDestinationForm, sourceShows } from "../pages/destinations.js";
import { NO_POLICY_SENTENCE } from "../pages/protection.js";
import { renderConnectForm } from "../pages/catalog.js";
import { KEYS_FORBIDDEN_SENTENCE, mountKeys } from "../pages/keys.js";

test("mcp_10_the_destination_form_opens_from_a_button_and_shows_only_the_chosen_sources_inputs", () => {
  const html = renderDestinationForm({ draft: {}, state: { phase: "idle" } });
  assert.match(html, /^<details class="create-disclosure" id="destination-create-disclosure"><summary class="button primary">Create destination<\/summary>/,
    "BEFORE: the form was always expanded under the list, a 4,300 px page");
  // archiveWrite starts on `existing`: its Secret name shows, the new-key and
  // ServiceAccount inputs do not. archiveRead starts `absent`: nothing shows.
  const role = (name) => html.slice(html.indexOf("id=\"destination-" + name + "\""),
    html.indexOf("</fieldset>", html.indexOf("id=\"destination-" + name + "\"")));
  const write = role("archiveWrite");
  assert.match(write, /data-grant-inputs="secret">/);
  assert.match(write, /data-grant-inputs="sa" hidden>/);
  assert.match(write, /data-grant-inputs="keys" hidden>/);
  const read = role("archiveRead");
  for (const group of ["secret", "sa", "keys"]) {
    assert.match(read, new RegExp("data-grant-inputs=\"" + group + "\" hidden>"),
      "an absent grant shows no input: " + group);
  }
  assert.equal(sourceShows("new", "keys"), true);
  assert.equal(sourceShows("workloadIdentity", "sa"), true);
  assert.equal(sourceShows("absent", "secret"), false);
  assert.deepEqual(Object.keys(SOURCE_INPUTS).sort(),
    ["absent", "archiveReadGrant", "controllerIdentity", "existing", "new", "workloadIdentity"]);
  // A draft or a refusal keeps the form OPEN, so no outcome is hidden.
  assert.match(renderDestinationForm({ draft: { name: "primary" }, state: { phase: "idle" } }),
    /^<details class="create-disclosure" id="destination-create-disclosure" open>/);
  assert.match(renderDestinationForm({ draft: {}, state: { phase: "failed", error: {} } }),
    /^<details [^>]* open>/);
});

test("mcp_20_the_protection_empty_state_says_who_sets_an_objective_without_kubectl", () => {
  assert.doesNotMatch(NO_POLICY_SENTENCE, /kubectl/, "BEFORE: 'create a ProtectionPolicy with kubectl'");
  assert.match(NO_POLICY_SENTENCE, /set by a cluster administrator/);
  assert.match(NO_POLICY_SENTENCE, /does not create them/);
});

test("mcp_23_the_connect_form_picks_a_destination_from_the_namespace", () => {
  const destinations = [
    { name: "primary", canonicalUrl: "s3://kafka-backups/poc", default: true },
    { name: "cold", canonicalUrl: "s3://cold/archive" },
  ];
  const html = renderConnectForm({ ns: "team-a", values: {}, state: {}, destinations: destinations });
  assert.match(html, /<select id="catalog-destination" name="destination" required>/,
    "BEFORE: a free-text box");
  assert.match(html, /<option value="primary">primary -- s3:\/\/kafka-backups\/poc \(default\)<\/option>/);
  assert.match(html, /<option value="cold">cold -- s3:\/\/cold\/archive<\/option>/);
  const unread = renderConnectForm({ ns: "team-a", values: {}, state: {}, destinations: null });
  assert.match(unread, /<input id="catalog-destination" name="destination"/,
    "a list that could not be read leaves the free-text box, so the form still works");
  const none = renderConnectForm({ ns: "team-a", values: {}, state: {}, destinations: [] });
  assert.match(none, /id="catalog-no-destination"/);
  assert.match(none, /href="#\/destinations\?ns=team-a"/);
});

test("mcp_32_a_403_on_keys_in_the_shared_console_says_only_administrators_may_read_trust", async () => {
  const node = { html: "", firstChild: null, removeChild() {}, appendChild(c) { node.html = c.html; } };
  const forbidden = new Error("This route requires the action in at least one granted namespace.");
  forbidden.status = 403;
  forbidden.reason = "forbidden";
  await mountKeys(node, (html) => [{ html: html }], {
    modeOf: () => "console",
    serverClock: () => null,
    listD3: undefined,
    consoleClusterList: async () => { throw forbidden; },
    api: { listCluster: async () => ({ items: [] }) },
  }, null);
  assert.match(node.html, /id="keys-forbidden"/);
  assert.ok(node.html.includes(KEYS_FORBIDDEN_SENTENCE.slice(0, 40)));
  assert.equal(node.html.indexOf("no approval can verify"), -1,
    "BEFORE: a 403 was shown as 'no trust exists ... no approval can verify'");
});

// ===========================================================================
// MCP-24: no internal roadmap reference in user-visible text
// ===========================================================================

import { readdirSync } from "node:fs";

/** The internal task and design references this repository uses: PLAT-/PROD-
 *  task ids, the design notes D0 to D3 (and their waves), the global
 *  constraints (GC n), a bare wave (W n), and the PoC/MCP finding ids. */
export const ROADMAP_REFERENCE =
  /\b(?:PLAT|PROD)-\d|\bD[0-3]\b|\bGC\s?\d+\b|\bW\d{1,2}\b|\bPOC-P\d|\bMCP-\d|\bGlobal Constraint\b/;

/** The string literals of a source file -- `"..."`, `'...'` and template
 *  literals, a template across as many lines as it spans -- with the line each
 *  starts on; comments and regular-expression literals are skipped (review
 *  L2: a per-line scan lost a multi-line template and was thrown by a quote
 *  inside a regex). For the shell (`html`), every line with its HTML comments
 *  removed, TAGS INCLUDED, so an attribute -- a `title`, an `aria-label` -- is
 *  scanned as well as the text between tags (review L2). */
export function quotedSegments(text, html) {
  const out = [];
  const source = String(text);
  if (html) {
    const stripped = source.replace(/<!--[\s\S]*?-->/g, (m) => m.replace(/[^\n]/g, " "));
    stripped.split("\n").forEach((line, i) => {
      if (line.trim().length > 0) {
        out.push({ line: i + 1, text: line });
      }
    });
    return out;
  }
  let line = 1;
  let last = "";
  // The characters after which a `/` opens a regular expression, not a division.
  const REGEX_AFTER = "(,=:[!&|?{};+-*%<>~^";
  for (let k = 0; k < source.length; k += 1) {
    const c = source[k];
    const n = source[k + 1];
    if (c === "\n") {
      line += 1;
      continue;
    }
    if (c === "/" && n === "/") {
      while (k < source.length && source[k] !== "\n") {
        k += 1;
      }
      k -= 1;
      continue;
    }
    if (c === "/" && n === "*") {
      const end = source.indexOf("*/", k + 2);
      const stop = end === -1 ? source.length : end + 2;
      for (let m = k; m < stop; m += 1) {
        if (source[m] === "\n") {
          line += 1;
        }
      }
      k = stop - 1;
      continue;
    }
    if (c === "/" && (last === "" || REGEX_AFTER.indexOf(last) !== -1 ||
      /\b(?:return|typeof|case)$/.test(source.slice(Math.max(0, k - 8), k).trimEnd()))) {
      let inClass = false;
      for (k += 1; k < source.length; k += 1) {
        const r = source[k];
        if (r === "\\") {
          k += 1;
        } else if (r === "[") {
          inClass = true;
        } else if (r === "]") {
          inClass = false;
        } else if (r === "/" && !inClass) {
          break;
        } else if (r === "\n") {
          line += 1;
          break;
        }
      }
      last = "/";
      continue;
    }
    if (c === "\"" || c === "'" || c === "`") {
      const startLine = line;
      let segment = "";
      for (k += 1; k < source.length && source[k] !== c; k += 1) {
        if (source[k] === "\\") {
          segment += source.slice(k, k + 2);
          k += 1;
          continue;
        }
        if (source[k] === "\n") {
          line += 1;
          if (c !== "`") {
            break;
          }
        }
        segment += source[k];
      }
      out.push({ line: startLine, text: segment });
      last = c;
      continue;
    }
    if (!/\s/.test(c)) {
      last = c;
    }
  }
  return out;
}

test("mcp_24_no_shipped_ui_string_names_an_internal_roadmap_reference", () => {
  const files = readdirSync(UI).filter((f) => f.endsWith(".js") || f === "index.html")
    .map((f) => UI + f)
    .concat(readdirSync(UI + "pages").filter((f) => f.endsWith(".js")).map((f) => UI + "pages/" + f));
  assert.ok(files.length >= 20, "the lint reads the whole shipped tree (" + String(files.length) + ")");
  const found = [];
  let segments = 0;
  for (const file of files) {
    const text = readFileSync(file, "utf8");
    for (const segment of quotedSegments(text, file.endsWith(".html"))) {
      segments += 1;
      if (ROADMAP_REFERENCE.test(segment.text)) {
        found.push(file.slice(UI.length) + ":" + String(segment.line) + ": " + segment.text.slice(0, 120));
      }
    }
  }
  assert.ok(segments > 2000, "and it read the strings (" + String(segments) + ")");
  assert.deepEqual(found, [],
    "BEFORE: 'PLAT-05.1' on Backups and 'PLAT-08.1' on Catalog. Internal task ids belong in " +
      "comments, not in what an operator reads:\n" + found.join("\n"));
});

test("mcp_24_the_lint_refuses_a_reference_in_a_string_and_ignores_one_in_a_comment", () => {
  const code = [
    "// PLAT-18.2 owns this, and a comment may say so",
    "/* D3 section 5, W12 */",
    "const a = \"fine words\"; // PLAT-01 in a trailing comment",
    "const b = \"frozen before PLAT-05.1\";",
    " *  GC6 in a block comment continuation",
    "const c = 'see D2 for why';",
    "const d = `wave W7`;",
  ].join("\n");
  const flagged = quotedSegments(code, false).filter((s) => ROADMAP_REFERENCE.test(s.text))
    .map((s) => s.line);
  assert.deepEqual(flagged, [4, 6, 7], "the three strings, and no comment");
  // Review L2: a template across lines, a quote inside a regex, a regex that
  // happens to spell an id (not user-visible), and attributes in the shell.
  const harder = [
    "const quote = /[\"']/g; const after = \"frozen before PLAT-05.1\";",
    "const pattern = /PLAT-\\d/;",
    "const t = `first line",
    "  second line names D2 here`;",
    "const ok = a / b / c; const s = \"fine\";",
  ].join("\n");
  assert.deepEqual(quotedSegments(harder, false).filter((s) => ROADMAP_REFERENCE.test(s.text))
    .map((s) => s.line), [1, 3], "the string after a regex, and a multi-line template");
  const shell = "<!-- PLAT-18.2 in a comment -->\n<p>Owned by PLAT-17.1</p>\n" +
    "<button title=\"see PLAT-18.2\" aria-label=\"Retry\">Retry</button>";
  assert.deepEqual(quotedSegments(shell, true).filter((s) => ROADMAP_REFERENCE.test(s.text))
    .map((s) => s.line), [2, 3], "text between tags and an attribute; never a comment");
});

// ===========================================================================
// The LOW findings: timestamps, booleans, conditions, words
// ===========================================================================

import { BUCKET_FOOTER, conditionBadge, flagBadge, humanInstant, when } from "../render.js";
import { renderClusterList } from "../pages/clusters.js";
import { renderScheduleList, SELECTION_UNKNOWN_SENTENCE, COVERAGE_NOT_PUBLISHED, NO_GENERATION_SENTENCE } from "../pages/schedules.js";
import { renderCatalogList } from "../pages/catalog.js";
import { NO_APPROVAL_SENTENCE, noApprovalSentence } from "../pages/approvals.js";
import { laterProbeNote, probeState } from "../select.js";

test("mcp_7_mcp_15_one_formatter_for_every_instant_whole_seconds_utc_exact_value_in_the_title", () => {
  assert.equal(humanInstant("2026-09-24T15:21:21.720607463Z"), "2026-09-24 15:21:21 UTC",
    "BEFORE: `2026-09-24T15:21:21.720607463Z`, nanoseconds and all");
  assert.equal(humanInstant("2026-09-24T17:26:33Z"), "2026-09-24 17:26:33 UTC");
  assert.equal(humanInstant("2026-10-25T02:30:00+02:00"), "2026-10-25 00:30:00 UTC",
    "an offset is converted, and the zone is said");
  assert.equal(humanInstant(Date.parse("2026-09-24T16:39:04Z")), "2026-09-24 16:39:04 UTC");
  assert.equal(humanInstant("not a time"), null);
  const html = when("2026-09-24T15:21:21.720607463Z");
  assert.equal(html, "<time class=\"ts\" datetime=\"2026-09-24T15:21:21.720607463Z\" " +
    "title=\"2026-09-24T15:21:21.720607463Z\">2026-09-24 15:21:21 UTC</time>",
    "the exact recorded value is one hover away, never lost");
  assert.equal(when(undefined), "-");
  assert.equal(when("20260918-032200"), "20260918-032200", "a value that is not an instant is not guessed at");
  const css = readFileSync(UI + "style.css", "utf8");
  assert.match(css, /\.ts \{\n {2}white-space: nowrap;/, "MCP-7: an instant never wraps mid-value");
  // And the pages use it: the clusters table's probe cell (its own OBSERVED
  // column until MCP round 2's R2-3 folded it in, as the wizard's MCP-28 cell
  // does) prints the age with the exact instant as the title, and the
  // schedules table's LAST/NEXT columns print the human reading.
  const cluster = fixture("cluster-scram.json");
  const clusters = renderClusterList(cluster, "team-a", Date.parse("2026-09-11T20:00:00Z"));
  assert.match(clusters, /<time class="ts" datetime="[^"]+" title="[^"]+">[^<]* ago<\/time>/);
  assert.doesNotMatch(clusters.replace(/"[^"]*"/g, "\"\""), /\d{4}-\d\d-\d\dT\d\d:\d\d/,
    "no raw ISO instant in the clusters table's visible text");
  const schedules = renderScheduleList(fixture("schedule-policy.json"));
  assert.doesNotMatch(schedules.replace(/"[^"]*"/g, "\"\""), /\d{4}-\d\d-\d\dT\d\d:\d\d/,
    "no raw ISO instant in the schedules table's visible text");
});

test("mcp_9_a_reachable_reading_beside_a_later_probes_reason_says_they_are_two_probes", () => {
  const cluster = JSON.parse(JSON.stringify(fixture("cluster-scram.json")));
  const object = Array.isArray(cluster.items) ? cluster.items[0] : cluster;
  object.status = Object.assign({}, object.status, { reachable: true, reason: "NoExitCode" });
  const note = laterProbeNote(probeState(object, Date.parse("2026-09-11T20:00:00Z")));
  assert.match(note, /<code>NoExitCode<\/code>/, "the controller's own reason, verbatim");
  assert.match(note, /the latest probe Job ended without an exit code/);
  assert.match(note, /the reachable reading is from an earlier probe/,
    "BEFORE: `reachable` beside a bare `NoExitCode`, which read as a contradiction");
  const html = renderClusterList(cluster, "team-a", Date.parse("2026-09-11T20:00:00Z"));
  assert.match(html, /the reachable reading is from an earlier probe/);
  object.status.reason = "Reachable";
  assert.equal(laterProbeNote(probeState(object, 0)), "", "a verdict's own reason adds nothing");
});

test("mcp_14_mcp_22_booleans_and_conditions_are_words_in_badges", () => {
  assert.equal(flagBadge(true, "loaded", "not loaded"), "<span class=\"badge badge-flat\">loaded</span>");
  assert.equal(flagBadge(false, "loaded", "not loaded"), "<span class=\"badge badge-flat\">not loaded</span>");
  assert.equal(flagBadge(undefined, "a", "b"), "-", "absent is absent, never false");
  const ready = conditionBadge({ type: "Ready", status: "True", reason: "ViewReady" }, "ready", "not ready");
  assert.equal(ready, "<span class=\"badge badge-green\" title=\"Ready=True ViewReady\">ready</span>",
    "BEFORE: `Ready=True ViewReady` as the cell's text");
  assert.match(conditionBadge({ type: "Ready", status: "False", reason: "ViewExpired" }, "ready", "not ready"),
    /badge-unverified" title="Ready=False ViewExpired">not ready \(ViewExpired\)/);
  assert.match(conditionBadge({ type: "Ready", status: "Unknown" }, "ready", "not ready"), /badge-flat[^>]*>unknown</,
    "Unknown is never the true word");
  const schedules = renderScheduleList(fixture("schedule-policy.json"));
  assert.doesNotMatch(schedules, /<td>True<\/td>/, "BEFORE: the READY column printed `True`");
  const catalogs = renderCatalogList({ items: [fixture("d3/catalog-truncated.json")] }, "team-a");
  assert.doesNotMatch(catalogs, />Ready=True/);
});

test("mcp_18_mcp_21_the_backups_table_says_created_and_the_footer_speaks_plainly", () => {
  const html = renderBackupList(fixture("backup-valid-exit0.json"), "team-a");
  assert.match(html, /<th scope="col">CREATED<\/th>/, "BEFORE: AGE, over a creation instant");
  assert.doesNotMatch(html, /<th scope="col">AGE<\/th>/);
  assert.doesNotMatch(html, /The AGE column is/, "and no paragraph explaining the misnomer");
  assert.doesNotMatch(BUCKET_FOOTER, /authoritative index/, "MCP-21: no jargon");
  assert.match(BUCKET_FOOTER, /signed evidence each run wrote stays in the archive/);
});

test("mcp_31_the_approvals_empty_state_follows_the_namespaces_policy", () => {
  const ordinary = noApprovalSentence({ legacy: false, mode: "ordinary" });
  assert.doesNotMatch(ordinary, /logweir drill approve/,
    "BEFORE: an ordinary-confirmation namespace was told to record files from logweir drill approve");
  assert.match(ordinary, /creating a Restore in the wizard is the confirmation/);
  assert.match(noApprovalSentence({ legacy: false, mode: "governed" }), /countersignature/);
  assert.equal(noApprovalSentence(null), NO_APPROVAL_SENTENCE, "unbound or legacy: the files, as before");
});

test("class_sweep_shared_console_sentences_do_not_send_an_operator_to_kubectl", () => {
  for (const [name, sentence] of [
    ["SELECTION_UNKNOWN_SENTENCE", SELECTION_UNKNOWN_SENTENCE],
    ["COVERAGE_NOT_PUBLISHED", COVERAGE_NOT_PUBLISHED],
    ["NO_GENERATION_SENTENCE", NO_GENERATION_SENTENCE],
    ["NO_POLICY_SENTENCE", NO_POLICY_SENTENCE],
  ]) {
    assert.doesNotMatch(sentence, /kubectl/, name);
  }
});

// ===========================================================================
// Review M1: the LOCAL TIME column stays local, DST rows included
// ===========================================================================

import { humanLocal, nextRunsPanel, whenLocal } from "../render.js";
import { renderScheduleCard } from "../pages/schedules.js";
import { decodeCadencePreview } from "../contract.js";

/** The rows of the next-runs table: [at, local, dst] cell html. */
function nextRunCells(html) {
  const body = html.slice(html.lastIndexOf("<tbody>"), html.lastIndexOf("</tbody>"));
  return body.split("<tr").slice(1).map((row) =>
    row.split("<td").slice(1).map((c) => c.slice(c.indexOf(">") + 1, c.indexOf("</td>"))));
}

test("review_m1_the_fall_back_day_shows_one_wall_time_twice_with_two_offsets", () => {
  const answer = decodeCadencePreview(con("cadence-preview-repeated.json")).value;
  const html = nextRunsPanel({ runs: answer.runs, timeZone: answer.timeZone });
  const rows = nextRunCells(html);
  assert.equal(rows.length, 3);
  // AT (UTC): two different instants.
  assert.match(rows[0][0], />2026-10-25 00:30:00 UTC</);
  assert.match(rows[1][0], />2026-10-25 01:30:00 UTC</);
  // LOCAL TIME: the SAME wall time, in the schedule's zone, with the two offsets
  // that tell them apart -- BEFORE the fix both read "00:30:00 UTC"/"01:30:00 UTC".
  assert.equal(rows[0][1], "<code><time class=\"ts\" datetime=\"2026-10-25T02:30:00+02:00\" " +
    "title=\"2026-10-25T02:30:00+02:00\">2026-10-25 02:30:00 +02:00</time></code>");
  assert.equal(rows[1][1], "<code><time class=\"ts\" datetime=\"2026-10-25T02:30:00+01:00\" " +
    "title=\"2026-10-25T02:30:00+01:00\">2026-10-25 02:30:00 +01:00</time></code>");
  assert.match(rows[0][2], /RepeatedLocalTimeFirst/);
  assert.match(rows[1][2], /RepeatedLocalTimeSecond/);
  for (const row of rows) {
    assert.doesNotMatch(row[1], /UTC</, "a local reading is never re-expressed in UTC");
  }
});

test("review_m1_the_spring_forward_gap_keeps_the_shifted_local_time", () => {
  const answer = decodeCadencePreview(con("cadence-preview-gap.json")).value;
  const rows = nextRunCells(nextRunsPanel({ runs: answer.runs, timeZone: answer.timeZone }));
  assert.match(rows[0][0], />2027-03-28 01:00:00 UTC</);
  assert.match(rows[0][1], />2027-03-28 03:00:00 \+02:00</, "the end of the gap, in local time");
  assert.match(rows[0][2], /NonexistentLocalTimeShifted/);
});

test("review_m1_a_saved_schedule_s_next_runs_keep_its_zone", () => {
  const cr = fixture("schedule-policy.json");
  const html = renderScheduleCard("team-a", cr, { items: [] }, {});
  const panel = html.slice(html.indexOf("data-next-runs"));
  const rows = nextRunCells(panel.slice(0, panel.indexOf("</section>")));
  assert.match(rows[0][0], />2026-09-19 03:22:00 UTC</);
  assert.match(rows[0][1], />2026-09-19 09:07:00 \+05:45</, "Asia/Kathmandu's own wall time");
  assert.match(rows[0][1], /title="2026-09-19T09:07:00\+05:45"/, "the exact value one hover away");
  // The local formatter keeps the reading as written; the instant formatter is
  // the one that converts, and only for instants shown as UTC.
  assert.equal(humanLocal("2026-10-25T02:30:00+01:00"), "2026-10-25 02:30:00 +01:00");
  assert.equal(humanLocal("2026-10-25T00:30:00Z"), "2026-10-25 00:30:00 UTC");
  assert.equal(whenLocal("02:30:00+02:00"), "02:30:00+02:00", "a value that is not an instant passes as written");
  assert.equal(whenLocal(undefined), "-");
});

// ===========================================================================
// Review L1: a console whose first request fails is never legacy mode
// ===========================================================================

import { UNAVAILABLE, servedByConsole, unavailableReason } from "../client.js";
import { renderUnavailable } from "../app.js";

async function probeServed(answerOrThrow, extra) {
  resetMode();
  return selectMode(Object.assign({
    servedByConsole: true,
    probe: typeof answerOrThrow === "function" ? answerOrThrow : async () => answerOrThrow,
  }, extra || {}));
}

test("review_l1_the_console_marker_is_the_services_runtime_js_and_nothing_else", () => {
  const saved = globalThis.LOGWEIR_CONSOLE;
  try {
    delete globalThis.LOGWEIR_CONSOLE;
    assert.equal(servedByConsole(), false, "the legacy file sets no marker");
    globalThis.LOGWEIR_CONSOLE = { servedBy: "logweir-api" };
    assert.equal(servedByConsole(), true);
    globalThis.LOGWEIR_CONSOLE = { servedBy: "something-else" };
    assert.equal(servedByConsole(), false);
  } finally {
    if (saved === undefined) {
      delete globalThis.LOGWEIR_CONSOLE;
    } else {
      globalThis.LOGWEIR_CONSOLE = saved;
    }
  }
  const file = readFileSync(UI + "runtime.js", "utf8");
  assert.doesNotMatch(file.replace(/\/\/.*$/gm, ""), /LOGWEIR_CONSOLE/,
    "ui/runtime.js -- what kubectl proxy serves -- never claims to be the console");
});

test("review_l1_a_failed_probe_behind_the_console_is_unavailable_never_legacy", async () => {
  const problem = (status, code) => ({ ok: false, status: status,
    body: { type: "x", title: "t", status: status, code: code, detail: "the store is down",
      requestId: "r", retryable: true } });
  for (const [said, answer, words] of [
    ["a 503 problem", problem(503, "kubernetes_unavailable"), /503 kubernetes_unavailable: the store is down/],
    ["a 429", problem(429, "rate_limited"), /429 rate_limited/],
    ["a 404 with no body", { ok: false, status: 404, body: null }, /404/],
    ["a session document that does not decode", { ok: true, status: 200, body: { nope: true } },
      /could not read/],
  ]) {
    const record = await probeServed(answer);
    assert.equal(record.mode, UNAVAILABLE, said + ": BEFORE, legacy mode behind the console");
    assert.notEqual(mode(), LEGACY);
    assert.match(unavailableReason(), words, said);
  }
  const failed = await probeServed(async () => { throw new TypeError("Failed to fetch"); });
  assert.equal(failed.mode, UNAVAILABLE, "a network failure");
  assert.match(unavailableReason(), /Failed to fetch/);
  const slow = await probeServed(() => new Promise(() => {}), { timeoutMs: 5 });
  assert.equal(slow.mode, UNAVAILABLE, "a probe that never answers");
  assert.match(unavailableReason(), /did not answer within/);
  // The two answers that DO decide stay what they were.
  assert.equal((await probeServed({ ok: false, status: 401, body: unauthenticated() })).mode, SIGNED_OUT);
  assert.equal((await probeServed({ ok: true, status: 200, body: con("session.json") })).mode, CONSOLE);
});

test("review_l1_a_page_the_console_did_not_serve_still_reads_a_refusal_as_legacy", async () => {
  // NEGATIVE CONTROL: without the marker -- `kubectl proxy` -- the same answers
  // are legacy mode, exactly as before.
  for (const answer of [{ ok: false, status: 404, body: null }, { ok: false, status: 503, body: null }]) {
    resetMode();
    const record = await selectMode({ servedByConsole: false, probe: async () => answer });
    assert.equal(record.mode, LEGACY);
  }
});

test("review_l1_unavailable_sends_nothing_and_renders_retry_not_a_namespace_prompt", async () => {
  await probeServed({ ok: false, status: 503, body: null });
  const wire = transport(() => ({ status: 200, body: {} }));
  try {
    await assert.rejects(apiClient().list("logweir-poc", "backups"), (error) => {
      assert.equal(error.reason, "service_unavailable");
      return true;
    });
    assert.equal(wire.seen.length, 0, "no legacy /apis/... read is made");
  } finally {
    wire.restore();
  }
  const html = renderUnavailable("the service answered 503");
  assert.match(html, /Can't reach the Logweir service/);
  assert.match(html, /id="console-retry"/);
  assert.match(html, /the service answered 503/);
  assert.equal(html.indexOf("Choose a namespace"), -1);
  assert.equal(renderUnavailable("<img src=x onerror=alert(1)>").indexOf("<img"), -1,
    "the reason is escaped");
});

// ===========================================================================
// Review L4 / L6: the probe cell's exact instant, and the escaped header
// ===========================================================================

import { probeSummary } from "../select.js";

test("review_l4_the_probe_cell_carries_the_exact_instant_in_its_title", () => {
  const cluster = { status: { reachable: true, observedAt: "2026-09-24T16:39:04.123456789Z", clusterId: "c" } };
  const at = Date.parse("2026-09-24T17:20:04Z");
  const html = probeSummary(probeState(cluster, at));
  assert.match(html, /<time class="ts" datetime="2026-09-24T16:39:04.123456789Z" title="2026-09-24T16:39:04.123456789Z">40m ago<\/time>/,
    "BEFORE: the <time> carried the exact value in datetime only, not one hover away");
});

test("review_l6_the_identity_header_escapes_every_value_the_session_carries", () => {
  const html = renderIdentity({
    displayName: "<img src=x onerror=alert(1)>",
    subject: "\"><script>alert(2)</script>",
    authenticationMode: "oidc",
    roles: ["op<b>erator"],
    namespace: "team-a&co",
    canSignOut: true,
  });
  assert.equal(html.indexOf("<img"), -1, "a hostile display claim is text");
  assert.equal(html.indexOf("<script>"), -1, "a hostile subject is text, attribute included");
  assert.equal(html.indexOf("<b>"), -1);
  assert.match(html, /&lt;img src=x onerror=alert\(1\)&gt;/);
  assert.match(html, /title="&quot;&gt;&lt;script&gt;/);
  assert.match(html, /op&lt;b&gt;erator in team-a&amp;co/);
});
