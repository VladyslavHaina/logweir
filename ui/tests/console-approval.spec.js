// console-approval.spec.js -- PROD-16.2: two-person approval in the console.
// A second person signs in and clicks Approve; no key, nothing to copy, sign
// or paste.
//
// Three layers, each driven for real:
//   * the client: `ui/api.js`'s one call site is stubbed at the platform
//     boundary, so the identifiers and the body under assertion are the ones
//     the product API would receive;
//   * the approvals page's rendering of the request the SERVER shows, and the
//     one submission it makes;
//   * the wizard's words for the mode.
//
// WHAT THE PAGE IS HELD TO. It renders facts the server sent, each as text.
// It draws the button only when the server offered it to this session. It
// sends one value, the hash of the request that was shown. It never offers a
// field to paste into, a document to copy or a command to run in this mode.
// None of that is a security boundary -- the server checks every rule again
// on the click, and `crates/logweir-api/tests/console_approval.rs` holds it
// to that -- but a page that drew the button for the requester, or sent a
// field of its own, would teach an approver to trust what they should not.
//
// Every behaviour here has a negative control: the row that proves a thing
// happens sits beside the row that proves it does not happen otherwise.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { apiClient, resetMode, selectMode } from "../client.js";
import { decodeApprovalRequest, isContractFailure } from "../contract.js";
import {
  APPROVE_COMMAND,
  approvalPolicyBlock,
  policyRefusal,
} from "../pages/restore-wizard.js";
import {
  COUNTERSIGN_COMMAND,
  consoleApprovalOffered,
  countersignOffered,
  noApprovalSentence,
  policyMode,
  renderApprovalSubject,
  renderConsoleApprovalPanel,
  submitConsoleApproval,
  twoPersonPolicy,
} from "../pages/approvals.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const request = (name) => fixture("console/approval-request-" + name + ".json").item;

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

/** Console mode; `approver` says whether the session holds `approvalSubmit`. */
async function consoleMode(approver) {
  const document = fixture("console/session.json");
  document.capabilities.approvalSubmit = approver;
  for (const grant of document.namespaces) {
    grant.capabilities.approvalSubmit = approver;
  }
  resetMode();
  return selectMode({ probe: async () => ({ ok: true, status: 200, body: document }) });
}

async function legacyMode() {
  resetMode();
  return selectMode({ probe: async () => ({ ok: false, status: 403, body: null }) });
}

const VIEW_URL = "/api/v1/namespaces/team-a/restores/restore-x/approval-request";
const CLICK_URL = "/api/v1/namespaces/team-a/restores/restore-x/console-approval";

// ================================================================ the client

test("the_request_view_and_the_click_are_the_two_published_routes", async () => {
  await consoleMode(true);
  const shown = fixture("console/approval-request-pending.json");
  const approval = fixture("console/approval.json");
  const wire = transport((u, init) => {
    if (u === VIEW_URL && (init.method === undefined || init.method === "GET")) {
      return { status: 200, body: shown };
    }
    if (u === CLICK_URL && init.method === "POST") {
      return { status: 201, body: approval };
    }
    return undefined;
  });
  try {
    const api = apiClient();
    const read = await api.approvalRequest("team-a", "restore-x");
    assert.equal(read.state, "pending");
    assert.equal(read.requester, "https://idp.example#alice");
    assert.equal(read.approve.offered, true);
    const made = await api.approveInConsole("team-a", "restore-x", read.confirmationSha256);
    assert.equal(made.kind, "Approval");
    const post = wire.seen.find((s) => s.init.method === "POST");
    assert.equal(post.url, CLICK_URL);
    assert.deepEqual(
      JSON.parse(post.init.body),
      { confirmationSha256: shown.item.confirmationSha256 },
      "ONE field: the hash of the request that was shown, and nothing an authorization carries",
    );
    assert.equal(
      post.init.headers["Idempotency-Key"],
      undefined,
      "the route refuses a key: the Approval is named by the Restore's own approvalRef",
    );
    assert.equal(wire.seen.filter((s) => s.init.method === "POST").length, 1);
  } finally {
    wire.restore();
  }
});

test("a_session_without_the_approver_capability_sends_no_click", async () => {
  // NEGATIVE CONTROL for the row above: the same call from a session the
  // server granted no `approvalSubmit` is refused HERE, before the network.
  await consoleMode(false);
  const wire = transport(() => undefined);
  try {
    await assert.rejects(
      apiClient().approveInConsole("team-a", "restore-x", "sha256:" + "5e".repeat(32)),
    );
    assert.equal(wire.seen.length, 0, "nothing was sent");
  } finally {
    wire.restore();
  }
});

test("legacy_mode_shows_no_request_and_has_no_click", async () => {
  await legacyMode();
  const wire = transport(() => undefined);
  try {
    const api = apiClient();
    assert.equal(await api.approvalRequest("team-a", "restore-x"), null);
    await assert.rejects(api.approveInConsole("team-a", "restore-x", "sha256:" + "5e".repeat(32)));
    assert.equal(wire.seen.length, 0, "nothing was sent");
  } finally {
    wire.restore();
  }
});

test("a_request_view_that_lies_about_its_shape_is_a_contract_failure", () => {
  // The decoder requires what the schema requires. A view with no `approve`
  // block is not rendered as "not offered": it is not rendered.
  const whole = fixture("console/approval-request-pending.json");
  const decoded = decodeApprovalRequest(whole);
  assert.deepEqual(decoded.unknown, [], "every field the server sends is declared here");
  assert.equal(decoded.value.item.approve.sentence, whole.item.approve.sentence);
  for (const field of ["approve", "state", "stateSentence", "restoreUid", "policyDigest"]) {
    const broken = structuredClone(whole);
    delete broken.item[field];
    assert.throws(
      () => decodeApprovalRequest(broken),
      (error) => isContractFailure(error) && error.contract.path === "item." + field,
      field,
    );
  }
  const offer = structuredClone(whole);
  delete offer.item.approve.offered;
  assert.throws(() => decodeApprovalRequest(offer), (error) => isContractFailure(error));
});

// ============================================================ the approvals page

const SUBJECT = Object.freeze({
  kind: "Restore", name: "restore-x", uid: "uid-x", planHash: "sha256:" + "e".repeat(64),
  approvalName: "approval-x", ns: "team-a",
});

const TWO_PERSON = Object.freeze(fixture("console/approval-policy-two-person.json").item);
const STRICT = Object.freeze(Object.assign({}, TWO_PERSON, {
  name: "prod-governed", operatorMode: "strict", consoleApprovalAvailable: false,
}));
const CONFIRM = Object.freeze(Object.assign({}, TWO_PERSON, {
  name: "team-ordinary", mode: "ordinary", operatorMode: "confirm",
  consoleApprovalAvailable: false, ordinaryConfirmationAvailable: true, ticketRequired: false,
}));

function view(policy, extra) {
  return Object.assign({
    ns: "team-a",
    route: { subject: "restore-x" },
    restore: { metadata: { name: "restore-x", uid: "uid-x" }, status: { phase: "Pending" } },
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
    request: request("pending"),
    requestError: null,
    maySubmit: true,
    state: { phase: "idle" },
    countersign: { phase: "idle" },
    consoleApproval: { phase: "idle" },
  }, extra || {});
}

test("a_two_person_namespace_shows_the_request_and_one_button_and_nothing_to_paste", () => {
  const html = renderApprovalSubject(view(TWO_PERSON));
  assert.equal(twoPersonPolicy(TWO_PERSON), true);
  assert.match(html, /id="console-approval-section"/);
  assert.match(html, /id="console-approval-form"/);
  assert.match(html, /<button type="submit" class="primary" id="approve-in-console">Approve this Restore<\/button>/);
  // What is being approved, as the server showed it.
  assert.ok(html.includes("<code id=\"request-requester\">https://idp.example#alice</code>"), html);
  assert.ok(html.includes("<code id=\"request-plan-hash\">sha256:" + "e".repeat(64) + "</code>"));
  assert.ok(html.includes("<code>prod-pair</code>"));
  assert.ok(html.includes("sha256:" + "c".repeat(64)), "the policy digest");
  assert.ok(html.includes("CHG-4711"), "the ticket");
  assert.ok(html.includes("2026-10-10T13:00:00Z"), "the expiry");
  assert.ok(html.includes(request("pending").approve.sentence.replace(/'/g, "&#39;")) ||
    html.includes(request("pending").approve.sentence), "the server's own sentence");
  // NOTHING TO COPY, SIGN OR PASTE: no field, no stored document, no command.
  assert.ok(!/<textarea/.test(html), "no field to paste into");
  assert.ok(!/<input/.test(html), "no input of any kind");
  assert.ok(!html.includes("{\"doc\":1}") && !html.includes("&quot;doc&quot;"),
    "the stored confirmation is not displayed");
  assert.ok(!html.includes("logweir drill countersign") && !html.includes("logweir drill approve"));
  assert.ok(!/id="countersign-form"/.test(html), "no countersignature is taken in this mode");
  assert.ok(!/id="approval-form"/.test(html), "no v1 approval form either");
  assert.equal(countersignOffered(view(TWO_PERSON)), false);
  assert.ok(COUNTERSIGN_COMMAND.length > 0);

  // NEGATIVE CONTROLS: every other policy keeps the flow it had, and none of
  // them draws the button -- a mode word nobody knows included.
  const strict = renderApprovalSubject(view(STRICT));
  assert.match(strict, /id="countersign-form"/);
  assert.ok(!/id="approve-in-console"/.test(strict));
  assert.ok(!/id="console-approval-section"/.test(strict));
  const confirm = renderApprovalSubject(view(CONFIRM));
  assert.match(confirm, /id="ordinary-confirmation"/);
  assert.ok(!/id="approve-in-console"/.test(confirm));
  for (const word of ["Two-Person", "twoPerson", "", undefined, "console"]) {
    const unknown = Object.assign({}, TWO_PERSON, { operatorMode: word });
    assert.equal(twoPersonPolicy(unknown), false, String(word));
    const page = renderApprovalSubject(view(unknown));
    assert.ok(!/id="approve-in-console"/.test(page), "an unknown word never draws the button");
    assert.match(page, /id="countersign-form"/, "it reads as strict: " + String(word));
  }
  for (const policy of [null, Object.assign({}, TWO_PERSON, { legacy: true })]) {
    assert.equal(twoPersonPolicy(policy), false);
    assert.equal(policyMode(policy), null);
    assert.ok(!/id="approve-in-console"/.test(renderApprovalSubject(view(policy))));
  }
});

test("the_button_is_drawn_only_when_the_server_offered_it_to_this_session", () => {
  // THE CONTROL first: the pending request, offered, to an approver.
  assert.equal(consoleApprovalOffered(view(TWO_PERSON)), true);
  const withOffer = (offer) => Object.assign({}, request("pending"), { approve: offer });
  const refused = "You requested this Restore, so you cannot approve it: a two-person " +
    "approval needs a second person.";
  const cases = [
    ["the server did not offer it",
      { request: withOffer({ offered: false, refusal: "requester", sentence: refused }) }],
    ["offered is not exactly true",
      { request: withOffer({ offered: "true", sentence: "x" }) }],
    ["no approver capability", { maySubmit: false }],
    ["expired", { request: Object.assign({}, request("pending"), { state: "expired" }) }],
    ["already approved", { request: request("approved") }],
    ["not confirmed", { request: request("not-confirmed") }],
    ["a state this page does not know",
      { request: Object.assign({}, request("pending"), { state: "approvedByMagic" }) }],
    ["a request for another Restore of this name",
      { request: Object.assign({}, request("pending"), { restoreUid: "uid-older" }) }],
    ["a request for another name",
      { request: Object.assign({}, request("pending"), { restore: "restore-y" }) }],
    ["no hash", { request: Object.assign({}, request("pending"), { confirmationSha256: undefined }) }],
    ["a malformed hash",
      { request: Object.assign({}, request("pending"), { confirmationSha256: "sha256:XYZ" }) }],
    ["a link that does not match the Restore",
      { mismatches: [{ field: "hash", claimed: "a", actual: "b" }] }],
    ["this console takes no approvals",
      { policy: Object.assign({}, TWO_PERSON, { consoleApprovalAvailable: false }) }],
    ["the request was not read", { request: null }],
    ["the request could not be read",
      { request: null, requestError: { status: 503, reason: "unavailable", message: "later" } }],
    ["no Restore", { restore: null }],
  ];
  for (const [label, change] of cases) {
    const v = view(TWO_PERSON, change);
    assert.equal(consoleApprovalOffered(v), false, label);
    if (v.restore !== null) {
      const html = renderConsoleApprovalPanel(v);
      assert.ok(!/id="approve-in-console"/.test(html), label + ": no button");
      assert.ok(!/id="console-approval-form"/.test(html), label + ": no form");
    }
  }
  // The server's reason is what the page says, in the server's words.
  const html = renderConsoleApprovalPanel(view(TWO_PERSON, {
    request: withOffer({ offered: false, refusal: "requester", sentence: refused }),
  }));
  assert.ok(html.includes("<p class=\"note\" id=\"approve-not-offered\">" + refused + "</p>"), html);
  // An approver-less login is told which role is missing, not offered a form.
  const reader = renderConsoleApprovalPanel(view(TWO_PERSON, { maySubmit: false }));
  assert.match(reader, /id="approve-not-offered"/);
  assert.ok(reader.includes("needs the approver role"), reader);
  // A console that cannot take part says so and shows no request at all.
  const admin = renderConsoleApprovalPanel(view(TWO_PERSON, {
    policy: Object.assign({}, TWO_PERSON, { consoleApprovalAvailable: false }),
  }));
  assert.match(admin, /id="console-approval-unavailable"/);
  assert.ok(!admin.includes("https://idp.example#alice"), "nothing of the request is shown");
});

test("a_request_this_console_did_not_confirm_shows_none_of_its_fields", () => {
  const honest = renderConsoleApprovalPanel(view(TWO_PERSON, { request: request("not-confirmed") }));
  assert.match(honest, /id="console-approval-state"/);
  assert.ok(honest.includes("not confirmed by this console"));
  assert.ok(honest.includes(request("not-confirmed").stateSentence));
  assert.ok(!/id="request-requester"/.test(honest));
  // A view that says notConfirmed and CARRIES fields all the same (a server
  // this page should not have to trust for that): still none is shown.
  const lying = Object.assign({}, request("pending"), {
    state: "notConfirmed", stateSentence: "not this console's",
  });
  const html = renderConsoleApprovalPanel(view(TWO_PERSON, { request: lying }));
  assert.ok(!html.includes("https://idp.example#alice"), html);
  assert.ok(!html.includes("CHG-4711"));
  assert.ok(!/id="request-plan-hash"/.test(html));
  assert.ok(!/id="approve-in-console"/.test(html));
  // NEGATIVE CONTROL: the pending request shows them.
  const pending = renderConsoleApprovalPanel(view(TWO_PERSON));
  assert.match(pending, /id="request-requester"/);
  assert.ok(pending.includes("CHG-4711"));
});

test("an_approved_request_names_the_approver_and_is_never_green", () => {
  const html = renderConsoleApprovalPanel(view(TWO_PERSON, { request: request("approved") }));
  assert.ok(html.includes("<code id=\"request-approver\">https://idp.example#bob</code>"), html);
  assert.ok(html.includes("2026-10-10T12:04:00Z"));
  assert.ok(!/badge-green/.test(html),
    "the console's record of an approval is not weirkeeper's verdict on it");
  assert.ok(!/id="approve-in-console"/.test(html));
  // NEGATIVE CONTROL: a pending request names no approver.
  assert.ok(!/id="request-approver"/.test(renderConsoleApprovalPanel(view(TWO_PERSON))));
});

test("an_original_name_request_lists_the_topics_being_approved", () => {
  const named = request("original-name");
  const html = renderConsoleApprovalPanel(view(TWO_PERSON, { request: named }));
  assert.match(html, /id="request-original-topics"/);
  assert.ok(html.includes("<li><code>orders</code></li><li><code>payments</code></li>"), html);
  assert.ok(html.includes("these 2 original topic name(s)"), html);
  assert.match(html, /id="approve-in-console"/, "shown its names, the approver may approve");
  // More names than the view lists: the page says how many it does not show.
  const many = Object.assign({}, named, { originalTopicsCount: 140 });
  assert.ok(renderConsoleApprovalPanel(view(TWO_PERSON, { request: many }))
    .includes("138 more are in the plan and are not listed here."));
  // The names could not be read: no button, and the page says why.
  for (const topics of [undefined, []]) {
    const unread = Object.assign({}, named, { originalTopics: topics });
    const v = view(TWO_PERSON, { request: unread });
    assert.equal(consoleApprovalOffered(v), false);
    const page = renderConsoleApprovalPanel(v);
    assert.ok(!/id="approve-in-console"/.test(page), "nobody approves names they were not shown");
    if (topics === undefined) {
      assert.match(page, /id="request-topics-unread"/);
    }
  }
  // NEGATIVE CONTROL: an ordinary request has no such block, and its button
  // does not wait on one.
  const ordinary = renderConsoleApprovalPanel(view(TWO_PERSON));
  assert.ok(!/id="request-original-topics"/.test(ordinary));
  assert.match(ordinary, /id="approve-in-console"/);
});

test("every_fact_of_the_request_is_rendered_as_text", () => {
  const hostile = Object.assign({}, request("original-name"), {
    requester: "https://idp.example#<img src=x onerror=alert(1)>",
    ticket: "\"><script>alert(2)</script>",
    policy: "<b>prod</b>",
    stateSentence: "<i>waiting</i>",
    originalTopics: ["<svg onload=alert(3)>", "orders"],
    approve: { offered: true, sentence: "<u>approve</u> & run" },
  });
  const html = renderApprovalSubject(view(
    Object.assign({}, TWO_PERSON, { name: "<em>pair</em>" }), { request: hostile },
  ));
  for (const raw of ["<img", "<script>", "<b>prod", "<i>waiting", "<svg", "<u>approve", "<em>pair"]) {
    assert.ok(!html.includes(raw), raw + " reached the page as markup");
  }
  for (const text of ["&lt;img src=x onerror=alert(1)&gt;", "&lt;script&gt;alert(2)&lt;/script&gt;",
    "&lt;b&gt;prod&lt;/b&gt;", "&lt;i&gt;waiting&lt;/i&gt;", "&lt;svg onload=alert(3)&gt;",
    "&lt;u&gt;approve&lt;/u&gt; &amp; run", "&lt;em&gt;pair&lt;/em&gt;"]) {
    assert.ok(html.includes(text), text + " is shown as text");
  }
});

test("the_click_sends_the_hash_that_was_shown_and_nothing_else", async () => {
  const calls = [];
  const api = {
    async approveInConsole(...args) {
      calls.push(args);
      return { kind: "Approval" };
    },
  };
  const result = await submitConsoleApproval(view(TWO_PERSON), api);
  assert.equal(result.outcome, "created");
  assert.deepEqual(calls, [["team-a", "restore-x", request("pending").confirmationSha256]],
    "the namespace, the Restore and the server's own hash: three values, none of them typed");
  // NEGATIVE CONTROLS: whatever the page was not offered, it does not send.
  const withOffer = (offer) => Object.assign({}, request("pending"), { approve: offer });
  for (const change of [
    { request: withOffer({ offered: false, sentence: "you are the requester" }) },
    { maySubmit: false },
    { request: request("approved") },
    { request: Object.assign({}, request("pending"), { confirmationSha256: "sha256:nope" }) },
    { policy: STRICT },
    { policy: CONFIRM },
    { policy: null },
  ]) {
    await assert.rejects(
      submitConsoleApproval(view(TWO_PERSON, change), api),
      (error) => error.kind === "refused",
      JSON.stringify(Object.keys(change)),
    );
  }
  assert.equal(calls.length, 1, "nothing else was sent");
});

test("the_empty_approvals_table_says_what_approving_means_under_two_person", () => {
  const words = noApprovalSentence(TWO_PERSON);
  assert.ok(words.includes("two-person") && words.includes("No key is involved"), words);
  assert.ok(!words.includes("countersignature") && !words.includes("logweir drill"));
  // NEGATIVE CONTROLS: the other modes keep their own sentences.
  assert.ok(noApprovalSentence(STRICT).includes("strict"));
  assert.ok(noApprovalSentence(STRICT).includes("countersignature"));
  assert.ok(noApprovalSentence(CONFIRM).includes("confirm"));
  assert.ok(noApprovalSentence(null).includes("logweir drill approve"));
});

// ================================================================ the wizard

test("the_submit_step_says_two_person_needs_a_second_person_and_no_key", () => {
  const block = approvalPolicyBlock(TWO_PERSON, "CHG-1");
  assert.match(block, /id="approval-policy-two-person"/);
  assert.ok(block.includes("a second person"), block);
  assert.ok(block.includes("No key is involved"), block);
  assert.match(block, /id="change-ticket"/, "a two-person request carries a change ticket");
  assert.ok(!block.includes(COUNTERSIGN_COMMAND) && !block.includes(APPROVE_COMMAND),
    "no command to run: nothing is signed outside the console");
  assert.ok(!block.includes("logweir drill"), block);
  assert.equal(policyRefusal({ approvalPolicy: TWO_PERSON }), null);

  // A console that cannot take part says so before anything is sent.
  const admin = Object.assign({}, TWO_PERSON, { consoleApprovalAvailable: false });
  const refused = approvalPolicyBlock(admin);
  assert.match(refused, /id="approval-policy-two-person-unavailable"/);
  assert.ok(!/id="change-ticket"/.test(refused), "no form for a request that cannot be made");
  assert.ok(policyRefusal({ approvalPolicy: admin }).includes("two-person"));

  // NEGATIVE CONTROLS: strict keeps its countersign command, and a mode word
  // nobody knows is read as strict -- never as "no key needed".
  const strict = approvalPolicyBlock(STRICT);
  assert.match(strict, /id="approval-policy-governed"/);
  assert.ok(strict.includes("logweir drill countersign"), strict);
  assert.equal(policyRefusal({ approvalPolicy: STRICT }), null);
  for (const word of ["Two-Person", "pair", undefined]) {
    const unknown = approvalPolicyBlock(Object.assign({}, TWO_PERSON, { operatorMode: word }));
    assert.match(unknown, /id="approval-policy-governed"/, String(word));
    assert.ok(!/approval-policy-two-person/.test(unknown));
  }
  assert.match(approvalPolicyBlock(CONFIRM), /id="approval-policy-ordinary"/);
});
