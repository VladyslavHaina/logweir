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
  // AN OLDER API that publishes neither still says so -- without kubectl.
  const older = con("schedule-last-slot.json");
  delete older.item.status.lastSlot;
  delete older.item.status.missedSlots;
  const again = transport(() => ({ status: 200, body: older }));
  try {
    const object = await apiClient().get("team-a", "backupschedules", older.item.name);
    const html = renderLastSlot(object);
    assert.match(html, /data-last-slot="absent"/);
    assert.equal(html.indexOf("kubectl"), -1);
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
