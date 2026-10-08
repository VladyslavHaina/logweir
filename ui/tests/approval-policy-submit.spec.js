// approval-policy-submit.spec.js -- PLAT-19.2 review fix round: what the
// wizard's ONE submit sends, and refuses to send, under the namespace's
// approval policy.
//
//   * H1, as PROD-16.1 amends it: a console that cannot sign a `confirm`
//     confirmation (its managed key is not there yet) says so, disables
//     Create, and sends nothing; a localAdmin console that can, confirms like
//     the shared one — the administrator mode now serves `confirm`.
//   * L1: a Governed policy requires a change ticket (D0). The page asks for
//     one, refuses to send without it, and sends it beside the create body;
//     an unbound namespace never sends one.
//
// Every refusal row sits beside the control that sends.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  approvalPolicyBlock,
  initialState,
  policyRefusal,
  preparePlan,
  recoveryPoints,
  renderPlanStep,
  restoreBody,
  submitRestore,
} from "../pages/restore-wizard.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

function wizardState(policy, ticket) {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  const state = initialState("logweir-t27", fixture("wizard-clusters.json"), backups, {
    uid: point.metadata.uid,
    backup: point.metadata.name,
  });
  state.approvalPolicy = policy;
  if (ticket !== undefined) {
    state.ticket = ticket;
  }
  return state;
}

const ORDINARY_SHARED = Object.freeze({
  name: "team-ordinary", mode: "ordinary", legacy: false,
  ordinaryConfirmationAvailable: true, ticketRequired: false,
});
const ORDINARY_LOCAL = Object.freeze(Object.assign({}, ORDINARY_SHARED, {
  ordinaryConfirmationAvailable: false,
}));
const GOVERNED = Object.freeze({
  name: "prod-governed", mode: "governed", legacy: false,
  ordinaryConfirmationAvailable: false, ticketRequired: true,
});
const UNBOUND = Object.freeze({
  name: "legacy-governed-v1", mode: "governed", legacy: true,
  ordinaryConfirmationAvailable: false, ticketRequired: false,
});

/** An API double: records every create, and every read is a 404. */
function recorder() {
  const creates = [];
  return {
    creates: creates,
    async create(ns, plural, body) {
      creates.push({ ns: ns, plural: plural, body: body });
      return Object.assign({}, body, { metadata: Object.assign({ uid: "u-1" }, body.metadata) });
    },
    async get() {
      const error = new Error("not found");
      error.status = 404;
      throw error;
    },
    async list() {
      return { items: [] };
    },
  };
}

test("a_console_without_its_confirmation_key_yet_offers_no_confirm_and_sends_nothing", async () => {
  const state = wizardState(ORDINARY_LOCAL);
  assert.ok(typeof policyRefusal(state) === "string");
  const block = approvalPolicyBlock(ORDINARY_LOCAL);
  assert.ok(block.includes("approval-policy-ordinary-unavailable"), block);
  const prepared = await preparePlan(state);
  const step = renderPlanStep(prepared, state);
  assert.ok(step.includes("id=\"create-restore\" class=\"primary\" disabled"), "Create is disabled");
  const api = recorder();
  await assert.rejects(() => submitRestore(state, api), /does not offer/);
  assert.equal(api.creates.length, 0, "nothing was sent");

  // THE CONTROL: the same binding in a shared console sends, with no ticket.
  const shared = wizardState(ORDINARY_SHARED);
  assert.equal(policyRefusal(shared), null);
  const sent = recorder();
  await submitRestore(shared, sent);
  assert.equal(sent.creates.length, 1);
  assert.equal(sent.creates[0].body.ticket, undefined);
});

test("a_governed_namespace_asks_for_a_ticket_refuses_without_one_and_sends_it", async () => {
  const block = approvalPolicyBlock(GOVERNED, "CHG-1");
  assert.ok(block.includes("id=\"change-ticket\"") && block.includes("value=\"CHG-1\""), block);

  const without = wizardState(GOVERNED, "   ");
  const api = recorder();
  await assert.rejects(() => submitRestore(without, api), (error) => {
    assert.ok(JSON.stringify(error).includes("ticket") || String(error.message).includes("ticket"),
      String(error && error.message));
    return true;
  });
  assert.equal(api.creates.length, 0, "no ticket, nothing sent");

  const withTicket = wizardState(GOVERNED, " CHG-4711 ");
  const sent = recorder();
  await submitRestore(withTicket, sent);
  assert.equal(sent.creates.length, 1);
  assert.equal(sent.creates[0].body.ticket, "CHG-4711", "trimmed, and beside the body");
  assert.equal(sent.creates[0].body.spec.ticket, undefined, "never inside Restore.spec");
});

test("an_unbound_namespace_never_sends_a_ticket", async () => {
  const state = wizardState(UNBOUND, "CHG-1");
  const prepared = await preparePlan(state);
  assert.equal(restoreBody(state, prepared).ticket, undefined);
  assert.ok(!approvalPolicyBlock(UNBOUND).includes("change-ticket"));
  const legacy = wizardState(null, "CHG-1");
  assert.equal(restoreBody(legacy, await preparePlan(legacy)).ticket, undefined);
});

// PROD-16.1: a FRESH install's unbound namespace, served by the localAdmin
// console, is `confirm` — the block says no key is needed and why, and the
// submit sends with no ticket and no refusal. NEGATIVE CONTROL: the same view
// with the key not there yet refuses, naming the key.
const FRESH_CONFIRM = Object.freeze({
  name: "default-confirm-v1", mode: "ordinary", operatorMode: "confirm", basis: "freshInstall",
  legacy: false, ordinaryConfirmationAvailable: true, ticketRequired: false,
});

test("a_fresh_install_confirms_an_unbound_namespace_with_no_key_in_one_submit", async () => {
  const block = approvalPolicyBlock(FRESH_CONFIRM);
  assert.ok(block.includes("id=\"approval-policy-ordinary\""), block);
  assert.match(block, /no key needed/i);
  assert.match(block, /This installation started in confirm/);
  assert.doesNotMatch(block, /logweir drill/, "no command for a key holder");
  const state = wizardState(FRESH_CONFIRM);
  assert.equal(policyRefusal(state), null);
  const api = recorder();
  await submitRestore(state, api);
  assert.equal(api.creates.length, 1);
  assert.equal(api.creates[0].body.ticket, undefined);

  const pending = Object.assign({}, FRESH_CONFIRM, { ordinaryConfirmationAvailable: false });
  assert.match(String(policyRefusal(wizardState(pending))), /key is not there/);
  assert.ok(approvalPolicyBlock(pending).includes("approval-policy-ordinary-unavailable"));
});

test("a_strict_namespace_names_the_personal_key_and_the_legacy_one_the_out_of_band_step", () => {
  assert.match(approvalPolicyBlock(GOVERNED, "CHG-1"), /Strict: an approver's personal key/);
  assert.match(approvalPolicyBlock(UNBOUND), /Approve it out of band \(strict/);
  assert.match(approvalPolicyBlock(null), /Approve it out of band/);
});
