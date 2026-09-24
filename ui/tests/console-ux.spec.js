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
