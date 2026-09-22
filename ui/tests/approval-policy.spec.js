// approval-policy.spec.js -- PLAT-19.2 and PLAT-12.1's policy routing, in the
// console: a submitted Restore goes where the namespace's FROZEN approval
// policy sends it, and a governed approver countersigns through one panel.
//
// Three layers, each driven for real:
//   * the client: `ui/api.js`'s one call site is stubbed at the platform
//     boundary, so the identifiers and bodies under assertion are the ones the
//     product API would receive;
//   * the wizard's routing functions, over an in-memory API double;
//   * the approvals page's rendering and its countersign submission.
//
// Every behaviour here has a negative control: the row that proves a thing
// happens sits beside the row that proves it does not happen otherwise.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { apiClient, resetMode, selectMode } from "../client.js";
import { preparePlanDocument } from "../plan.js";
import {
  APPROVE_COMMAND,
  approvalPolicyBlock,
  frozenDecision,
  restoreDestination,
} from "../pages/restore-wizard.js";
import {
  COUNTERSIGN_COMMAND,
  countersignOffered,
  policyMode,
  renderApprovalSubject,
  submitCountersignature,
} from "../pages/approvals.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

function transport(answer) {
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (u, init) => {
    seen.push({ url: u, init: init || {} });
    const reply = answer(u, init || {});
    if (reply === undefined || reply === null) {
      return Promise.reject(new Error("the suite has no answer for " + String(u)));
    }
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      text: () => Promise.resolve(reply.body === undefined ? "" : JSON.stringify(reply.body)),
    });
  };
  return { seen: seen, restore: () => { globalThis.fetch = original; } };
}

/** Console mode, with the approver's capability granted in team-a. */
async function consoleMode() {
  const document = fixture("console/session.json");
  document.capabilities.approvalSubmit = true;
  for (const grant of document.namespaces) {
    grant.capabilities.approvalSubmit = true;
  }
  resetMode();
  return selectMode({ probe: async () => ({ ok: true, status: 200, body: document }) });
}

async function legacyMode() {
  resetMode();
  return selectMode({ probe: async () => ({ ok: false, status: 403, body: null }) });
}

/** A Restore body over a plan THIS PAGE prepared -- the client refuses any
 *  other bytes before the network. */
async function restoreBody() {
  const reviewed = await preparePlanDocument(fixture("plan-fields.json"));
  const body = structuredClone(RESTORE_BODY);
  body.metadata.name = reviewed.restoreName;
  body.spec.planBytes = reviewed.bytes;
  body.spec.approvalRef = { name: reviewed.approvalName };
  return body;
}

const RESTORE_BODY = Object.freeze({
  apiVersion: "logweir.dev/v1alpha1",
  kind: "Restore",
  metadata: { name: "restore-x" },
  spec: {
    planBytes: "replaced by restoreBody()",
    approvalRef: { name: "approval-x" },
    sourceArchive: { url: "s3://kafka-backups/orders" },
    backupSetRef: "01JB7Z0000000000000000000B",
    pointInTime: "2026-09-11T12:00:00Z",
    target: {
      clusterRef: { name: "orders-scratch" }, mode: "scratch",
      topicNaming: { prefix: "drill-20260911-" },
    },
    deadlineSeconds: 3600,
  },
});

const ORDINARY = Object.freeze({
  mode: "ordinary", policy: "team-ordinary", legacy: false, state: "confirmed",
  approvalName: "approval-x", policyDigest: "sha256:" + "a".repeat(64),
  requester: "urn:logweir:local-admin#admin", expiresAt: "2026-09-22T12:15:00Z",
});

// ================================================================ the client

test("a_console_restore_create_carries_the_frozen_policy_decision_on_the_object", async () => {
  await consoleMode();
  const answer = fixture("console/restore.json");
  answer.authorization = ORDINARY;
  const wire = transport((u, init) => (init.method === "POST" ? { status: 201, body: answer } : undefined));
  try {
    const made = await apiClient().create("team-a", "restores", await restoreBody());
    assert.equal(made.__contract.authorization.state, "confirmed");
    assert.equal(made.__contract.authorization.mode, "ordinary");
    assert.equal(frozenDecision(made).requester, "urn:logweir:local-admin#admin");
  } finally {
    wire.restore();
  }
  // NEGATIVE CONTROL: an answer without the block leaves nothing to route on.
  const plain = transport((u, init) =>
    init.method === "POST" ? { status: 201, body: fixture("console/restore.json") } : undefined);
  try {
    const made = await apiClient().create("team-a", "restores", await restoreBody());
    assert.equal(made.__contract.authorization, null);
    assert.equal(frozenDecision(made), null);
  } finally {
    plain.restore();
  }
});

test("the_policy_read_and_the_countersign_submission_are_the_two_published_routes", async () => {
  await consoleMode();
  const policy = {
    requestId: "r1",
    item: {
      namespace: "team-a", name: "prod-governed", mode: "governed", legacy: false,
      requireDistinctPrincipal: true, installationDigest: "sha256:" + "b".repeat(64),
      maxAgeSeconds: 86400, digest: "sha256:" + "c".repeat(64),
      confirmationKeyId: "d".repeat(64),
    },
  };
  const approval = fixture("console/approval.json");
  const wire = transport((u, init) => {
    if (u === "/api/v1/namespaces/team-a/approval-policy") {
      return { status: 200, body: policy };
    }
    if (u === "/api/v1/namespaces/team-a/restores/restore-x/approval" && init.method === "POST") {
      return { status: 201, body: approval };
    }
    return undefined;
  });
  try {
    const api = apiClient();
    const read = await api.approvalPolicy("team-a");
    assert.equal(read.mode, "governed");
    assert.equal(read.legacy, false);
    const made = await api.submitGovernedApproval("team-a", "restore-x", "{\"payloadType\":\"x\"}");
    assert.equal(made.kind, "Approval");
    const post = wire.seen.find((s) => s.init.method === "POST");
    assert.deepEqual(JSON.parse(post.init.body), { sidecarBytes: "{\"payloadType\":\"x\"}" });
    assert.equal(
      post.init.headers["Idempotency-Key"],
      undefined,
      "the route refuses a key: the Approval is named by the Restore's own approvalRef",
    );
  } finally {
    wire.restore();
  }
});

test("legacy_mode_knows_no_policy_and_has_no_countersign_route", async () => {
  await legacyMode();
  const wire = transport(() => undefined);
  try {
    const api = apiClient();
    assert.equal(await api.approvalPolicy("team-a"), null);
    await assert.rejects(api.submitGovernedApproval("team-a", "restore-x", "{}"));
    assert.equal(wire.seen.length, 0, "nothing was sent");
  } finally {
    wire.restore();
  }
});

// ================================================================ the wizard

/** An API double: `get` answers from a table and records every read. */
function readsOnly(table) {
  const reads = [];
  return {
    reads: reads,
    async get(ns, plural, name) {
      reads.push(plural + "/" + name);
      const found = (table || {})[plural + "/" + name];
      if (found === undefined) {
        const error = new Error("not found");
        error.status = 404;
        throw error;
      }
      return found;
    },
  };
}

const STATE = Object.freeze({ ns: "team-a" });
const PREPARED = Object.freeze({
  restoreName: "restore-x", approvalName: "approval-x", hash: "sha256:" + "e".repeat(64),
});

function created(authorization) {
  return {
    metadata: { name: "restore-x", uid: "uid-x" },
    spec: { approvalRef: { name: "approval-x" } },
    __contract: { mode: "console", authorization: authorization },
  };
}

test("an_ordinary_confirmation_routes_straight_to_execution", async () => {
  const api = readsOnly({});
  const route = await restoreDestination(api, STATE, PREPARED, created(ORDINARY));
  assert.equal(route, "#/history?ns=team-a&name=restore-x");
  assert.deepEqual(api.reads, [], "the frozen answer decides; nothing is read");
});

test("a_governed_or_legacy_submission_routes_to_awaiting_approval", async () => {
  const governed = Object.assign({}, ORDINARY, {
    mode: "governed", policy: "prod-governed", state: "awaitingApproval",
    confirmationName: "approval-x-confirmation",
  });
  for (const restore of [created(governed), created(null), { metadata: { name: "restore-x" } }]) {
    const api = readsOnly({});
    const route = await restoreDestination(api, STATE, PREPARED, restore);
    assert.ok(route.startsWith("#/approvals?subject=restore-x"), route);
    assert.deepEqual(api.reads, ["approvals/approval-x"], "today's check still runs");
  }
});

test("an_unknown_state_is_never_read_as_confirmed", async () => {
  const api = readsOnly({});
  const strange = Object.assign({}, ORDINARY, { state: "approvedByMagic" });
  assert.equal(frozenDecision(created(strange)), null);
  const route = await restoreDestination(api, STATE, PREPARED, created(strange));
  assert.ok(route.startsWith("#/approvals?"), route);
});

test("the_submit_step_says_what_the_policy_requires", () => {
  const ordinary = approvalPolicyBlock({ legacy: false, mode: "ordinary", name: "team-ordinary" });
  assert.match(ordinary, /id="approval-policy-ordinary"/);
  assert.ok(!ordinary.includes(APPROVE_COMMAND), "no out-of-band approval under Ordinary");
  const governed = approvalPolicyBlock({ legacy: false, mode: "governed", name: "prod-governed" });
  assert.match(governed, /id="approval-policy-governed"/);
  assert.ok(governed.includes("logweir drill countersign"), governed);
  // NEGATIVE CONTROLS: unknown, legacy and a missing policy are today's words.
  for (const policy of [null, { legacy: true, mode: "governed" }, { legacy: false, mode: "x" }]) {
    const words = approvalPolicyBlock(policy);
    assert.ok(words.includes("logweir drill approve"), JSON.stringify(policy));
    assert.ok(!/approval-policy-ordinary/.test(words));
  }
});

// ============================================================ the approvals page

const SUBJECT = Object.freeze({
  kind: "Restore", name: "restore-x", uid: "uid-x", planHash: "sha256:" + "e".repeat(64),
  approvalName: "approval-x", ns: "team-a",
});

function view(policy, extra) {
  return Object.assign({
    ns: "team-a",
    route: { subject: "restore-x" },
    restore: { metadata: { name: "restore-x" }, status: { phase: "Pending" } },
    subject: SUBJECT,
    approval: null,
    found: { state: "absent" },
    approvalError: null,
    mismatches: [],
    policy: policy,
    confirmation: {
      metadata: { name: "approval-x-confirmation" },
      spec: { approvalBytes: "{\"doc\":1}", sidecarBytes: "{\"sig\":1}" },
    },
    confirmationError: null,
    state: { phase: "idle" },
    countersign: { phase: "idle" },
  }, extra || {});
}

const GOVERNED_POLICY = Object.freeze({ legacy: false, mode: "governed", name: "prod-governed" });
const ORDINARY_POLICY = Object.freeze({ legacy: false, mode: "ordinary", name: "team-ordinary" });

test("a_governed_namespace_offers_the_countersign_panel_and_never_the_v1_form", () => {
  const html = renderApprovalSubject(view(GOVERNED_POLICY));
  assert.match(html, /id="countersign-form"/);
  assert.ok(COUNTERSIGN_COMMAND.startsWith("logweir drill countersign"));
  assert.ok(html.includes("logweir drill countersign --document &lt;approvalBytes&gt;"), html);
  assert.ok(!/id="approval-form"/.test(html), "a v1 approval is refused under a binding");
  assert.equal(policyMode(GOVERNED_POLICY), "governed");
  // Once the referenced Approval exists there is nothing left to submit.
  assert.equal(countersignOffered(view(GOVERNED_POLICY, { found: { state: "awaiting-verification" } })), false);
  // No confirmation: nothing to countersign, said by name.
  const bare = renderApprovalSubject(view(GOVERNED_POLICY, { confirmation: null }));
  assert.match(bare, /id="no-confirmation"/);
  assert.ok(!/id="countersign-form"/.test(bare));
});

test("an_ordinary_namespace_offers_nothing_to_approve", () => {
  const html = renderApprovalSubject(view(ORDINARY_POLICY));
  assert.match(html, /id="ordinary-confirmation"/);
  assert.ok(!/id="countersign-form"/.test(html));
  assert.ok(!/id="approval-form"/.test(html));
});

test("an_unbound_or_unknown_policy_keeps_the_v1_approval_form", () => {
  for (const policy of [null, { legacy: true, mode: "governed", name: "legacy-governed-v1" }]) {
    const html = renderApprovalSubject(view(policy));
    assert.match(html, /id="approval-form"/, JSON.stringify(policy));
    assert.ok(!/id="countersign-form"/.test(html));
    assert.equal(policyMode(policy), null);
  }
});

test("a_countersignature_is_submitted_for_the_subject_and_key_material_never_is", async () => {
  const calls = [];
  const api = {
    async submitGovernedApproval(ns, name, text) {
      calls.push([ns, name, text]);
      return { kind: "Approval" };
    },
  };
  const result = await submitCountersignature(SUBJECT, "{\"payloadType\":\"x\"}", api);
  assert.equal(result.outcome, "created");
  assert.deepEqual(calls, [["team-a", "restore-x", "{\"payloadType\":\"x\"}"]]);
  // NEGATIVE CONTROLS: an empty field and a private key send nothing.
  await assert.rejects(submitCountersignature(SUBJECT, "   ", api), (e) => e.kind === "invalid");
  const pem = "-----BEGIN " + "PRIVATE KEY-----\nabc\n-----END " + "PRIVATE KEY-----\n";
  await assert.rejects(submitCountersignature(SUBJECT, pem, api), (e) => e.kind === "refused");
  assert.equal(calls.length, 1);
});
