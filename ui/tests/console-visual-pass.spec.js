// console-visual-pass.spec.js -- the console defects PROD-14.1's visual pass
// found as each role (claude/ui-pass.result.md: M2 to M8, M10, M11 and the
// cheap LOWs), one row each, with the behaviour it replaced as its NEGATIVE
// CONTROL. A layout row asserts what decides the visible property -- the
// markup a cell carries and the stylesheet rule that holds it -- because this
// suite has no browser; the measured widths are in the branch's report.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { decoded } from "./console-fixture.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

import { resetMode, selectMode } from "../client.js";
import { NAV_ROUTES, landingRedirect, landingRoute } from "../app.js";
import { mountClusters } from "../pages/clusters.js";
import { mountSchedules } from "../pages/schedules.js";

import {
  NO_TOPIC_READINESS_SENTENCE,
  initialState,
  preparePlanOrProblem,
  recoveryPoints,
  renderPreflightStep,
  restoreDestination,
} from "../pages/restore-wizard.js";
import {
  PRINCIPAL_SHORT_CHARS,
  principalShort,
  renderApprovalList,
  restoreProgressSentence,
} from "../pages/approvals.js";
import { ident } from "../render.js";
import { LONG_TOKEN_CHARS, SHORT_CHIP_CHARS } from "../app.js";
import { operationFacts, renderEvidence, renderOperation } from "../pages/operation.js";

const PREPARED = Object.freeze({
  restoreName: "restore-x", approvalName: "approval-x", hash: "sha256:" + "e".repeat(64),
});

// M2. Step 6 promises "its operation view"; Create opened the History detail,
// which said "Pending: no Job exists, and none will until its approval is
// Verified=True" for a confirmation seconds old.

test("m2_create_opens_the_operation_view_step_6_promises", async () => {
  const restore = {
    metadata: { name: "restore-x", uid: "uid-x" },
    spec: { approvalRef: { name: "approval-x" } },
    __contract: { mode: "console", authorization: { state: "confirmed", mode: "ordinary" } },
  };
  const route = await restoreDestination({ get: async () => null }, { ns: "team-a" }, PREPARED,
    restore);
  assert.ok(route.startsWith("#/operations?"),
    "NEGATIVE CONTROL: Create opened " + route + ", the History detail");
  assert.equal(route, "#/operations?ns=team-a&kind=restore&name=restore-x&uid=uid-x",
    "the operation view of exactly this run");
});

test("m2_a_restore_waiting_for_its_approval_says_so_and_not_none_will", () => {
  const pending = { status: { phase: "Pending", reason: "ApprovalNotVerified" } };
  const sentence = restoreProgressSentence(pending);
  assert.match(sentence, /^Waiting for approval: /);
  assert.doesNotMatch(sentence, /none will/,
    "NEGATIVE CONTROL: a seconds-old Restore read as one that will never run");

  // And the operation view it now opens says the same, in its own row.
  const item = decoded("operation-restore-completed.json", (body) => {
    Object.assign(body.item, {
      state: "pending", stateReason: "ApprovalNotVerified", terminal: false,
      awaitingApproval: true, stage: "admission",
    });
  }).item;
  const html = renderOperation({
    ns: "team-a", kind: "restore", name: item.name, uid: "",
    document: item, console: true, meta: { transport: "stream", attempt: 0 },
  });
  assert.match(html, /<dt>awaiting approval<\/dt><dd>yes: waiting for approval/);
});

// M3 and M4. A viewer got the whole Create a KafkaCluster form with an enabled
// CREATE (password and key fields included), and the whole editable Create a
// BackupSchedule form. Destinations hides its form for such a role.

const FIXTURES = fileURLToPath(new URL("./fixtures/console/", import.meta.url));
const session = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

/** Runs `body` under the session document `doc`, with every network call
 *  refused: a mount's own reads go to the stub it is handed, and the one read
 *  through the default client (the retention list) fails, as its panel allows. */
async function signedInAs(doc, body) {
  const original = globalThis.fetch;
  globalThis.fetch = () => Promise.reject(new Error("this suite never dials"));
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: doc }) });
  try {
    return await body();
  } finally {
    resetMode();
    globalThis.fetch = original;
  }
}

const listsNothing = {
  list() { return Promise.resolve({ items: [] }); },
  destinations() { return Promise.resolve({ items: [] }); },
};

test("m3_a_role_that_cannot_create_a_connection_is_not_offered_the_form", async () => {
  const viewer = session("session-viewer.json");
  assert.equal(viewer.namespaces[0].capabilities.connectionCreate, false);
  const read = await signedInAs(viewer, async () => {
    const view = fakeView();
    await mountClusters(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.doesNotMatch(read, /id="cluster-form"/,
    "NEGATIVE CONTROL: a viewer got the create form, CREATE enabled");
  assert.doesNotMatch(read, /type="password"|private key/);
  assert.match(read, /id="clusters-read-only"/);
  // A role that may create still gets it.
  const operator = session("session.json");
  assert.equal(operator.namespaces[0].capabilities.connectionCreate, true);
  const full = await signedInAs(operator, async () => {
    const view = fakeView();
    await mountClusters(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.match(full, /id="cluster-form"/);
  assert.doesNotMatch(full, /id="clusters-read-only"/);
});

test("m4_a_role_that_cannot_create_a_schedule_is_not_offered_the_form", async () => {
  const viewer = session("session-viewer.json");
  assert.equal(viewer.namespaces[0].capabilities.scheduleCreate, false);
  const read = await signedInAs(viewer, async () => {
    const view = fakeView();
    await mountSchedules(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.doesNotMatch(read, /id="schedule-form"/,
    "NEGATIVE CONTROL: a viewer got the whole editable create form");
  assert.match(read, /id="schedules-read-only"/);
  const full = await signedInAs(session("session.json"), async () => {
    const view = fakeView();
    await mountSchedules(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.match(full, /id="schedule-form"/);
  assert.doesNotMatch(full, /id="schedules-read-only"/);
});

// M5. An approver landed on #/clusters, which refused them ("This page needs
// any role"): the landing must be a tab the role can use.

/** The flags a role's grant publishes, by the role table in
 *  `crates/logweir-api/src/authz.rs` (`Role::allows`). */
const ROLE_FLAGS = {
  approver: ["preflight", "backupsRead", "restoresRead", "approvalsRead", "approvalPacketRead",
    "operationsRead", "operationEvents", "approvalSubmit"],
  viewer: ["connectionsRead", "destinations", "topicDiscovery", "preflight", "schedulesRead",
    "backupsRead", "restoresRead", "approvalsRead", "operationsRead", "operationEvents",
    "protection", "catalogs", "retention"],
};
const holding = (role) => (flag) => ROLE_FLAGS[role].includes(flag);
const route = (hash) => NAV_ROUTES.find((r) => r.hash === hash);

test("m5_an_approver_lands_on_approvals_and_a_viewer_on_clusters", () => {
  assert.equal(landingRoute(NAV_ROUTES, "team-a", holding("approver")).hash, "#/approvals",
    "NEGATIVE CONTROL: the approver landed on the refused #/clusters");
  assert.equal(landingRoute(NAV_ROUTES, "team-a", holding("viewer")).hash, "#/clusters",
    "the default stays the landing for a role that can use it");
  assert.equal(landingRoute(NAV_ROUTES, "team-a", () => false), null, "no tab, no landing");

  // The redirect is for the LANDING only: an address someone chose is still
  // refused by name.
  const clusters = route("#/clusters");
  assert.equal(landingRedirect(true, clusters, "team-a", holding("approver")).hash, "#/approvals");
  assert.equal(landingRedirect(false, clusters, "team-a", holding("approver")), null);
  assert.equal(landingRedirect(true, clusters, "team-a", holding("viewer")), null,
    "a usable landing is not moved");

  // AND THE SHELL ASKS IT: the refusal arm of `render` redirects a landing
  // before it refuses, and `boot` marks the bare and sign-in addresses.
  const app = readFileSync(fileURLToPath(new URL("../app.js", import.meta.url)), "utf8");
  const body = app.slice(app.indexOf("function render(lifecycle"), app.indexOf("function boot()"));
  const asked = body.indexOf("landingRedirect(landing, current, here.ns, routeGateHas)");
  assert.ok(asked > 0 && asked < body.indexOf("renderRoleRefusal(current"),
    "the landing is redirected before the refusal renders");
  assert.match(app, /context\.landing = window\.location\.hash === "" \|\| window\.location\.hash === DEFAULT_HASH;/);
});

// M6. The Recorded approvals table: the APPROVER (an unwrapped Dex subject
// URL) took about 60% of the width, the VERIFIED chip wrapped one word per
// line, KEY-ID wrapped over 10 lines, and AGE sat behind a sideways scroll.

const CSS = () => readFileSync(fileURLToPath(new URL("../style.css", import.meta.url)), "utf8");
const KEY = "4e6b22944519f669fcc67dd85456c23576306de97121709c2ee382b0750251f7";
const DEX = "https://dex.localtest.me#CiQxZjBjOWE0ZS0wMDA3LTRhMWUtOWUxYS0wMDAwMDAwMDAwMDcSBWxvY2Fs";

/** A recorded Approval as the page is handed it, shaped like the PoC's. */
function pocApproval(name, approver) {
  return {
    metadata: { name: name, creationTimestamp: "2026-10-12T09:00:00Z" },
    spec: { subjectRef: { kind: "Restore", name: "rst-" + name }, planHash: "sha256:" + "a".repeat(64) },
    status: {
      verified: true, matchedKeyId: KEY, approver: approver,
      conditions: [{ type: "Verified", status: "True", reason: "Verified" }],
    },
  };
}

/** The cells of the first body row of `html`'s first table, as HTML. */
function firstRowCells(html) {
  const body = html.slice(html.indexOf("<tbody>"), html.indexOf("</tbody>"));
  return body.split("<td").slice(1).map((c) => c.slice(c.indexOf(">") + 1, c.lastIndexOf("</td>")));
}

test("m6_the_approvals_table_keeps_one_line_a_row_and_every_identifier_whole", () => {
  const html = renderApprovalList({ items: [pocApproval("approval-1", DEX)] },
    Date.parse("2026-10-12T09:30:00Z"));
  const [, verified, approver, keyId, age] = firstRowCells(html);
  // THE CHIP IS ONE SHORT WORD, so `markLongTokens` marks it `short` and the
  // stylesheet keeps it on one line; its sentence is the hover title.
  assert.match(verified, /^<span class="badge badge-green" title="verified by weirkeeper against key 4e6b/);
  const words = verified.replace(/<[^>]*>/g, "");
  assert.equal(words, "verified",
    "NEGATIVE CONTROL: the chip carried the 64-hex key and wrapped one word per line");
  assert.ok(words.length <= SHORT_CHIP_CHARS);
  // THE KEY ID AND THE APPROVER are identifiers on one line, whole on hover,
  // in the copy payload and in the text.
  for (const [cellHtml, full] of [[keyId, KEY], [approver, DEX]]) {
    assert.ok(cellHtml.startsWith("<span class=\"ident\">"),
      "NEGATIVE CONTROL: an unbounded <code> or text cell: " + cellHtml);
    assert.ok(cellHtml.includes("title=\"" + full + "\""), "the full value on hover");
    assert.ok(cellHtml.includes("data-copy=\"" + full + "\""), "and on the copy button");
    assert.ok(cellHtml.replace(/<[^>]*>/g, "").includes(full), "and in the text");
  }
  assert.equal(age, "30m", "and AGE is a cell like any other");
  // THE RULE THAT HOLDS THE LINE: one line, an ellipsis, a bounded width, and
  // the width is a token.
  const css = CSS();
  const rule = css.slice(css.indexOf(".ident-text {"), css.indexOf("}", css.indexOf(".ident-text {")));
  for (const declaration of ["white-space: nowrap;", "overflow: hidden;", "text-overflow: ellipsis;",
    "max-width: var(--lw-ident-width);"]) {
    assert.ok(rule.includes(declaration), ".ident-text carries " + declaration + "; it was:\n" + rule);
  }
  assert.match(css, /--lw-ident-width: 14ch;/);
  // THE COPY BUTTON IS WIRED ONCE, for every page: the full value it carries
  // goes onto the clipboard.
  const app = readFileSync(fileURLToPath(new URL("../app.js", import.meta.url)), "utf8");
  assert.match(app, /target\.closest\("\.ident-copy"\)/);
  assert.match(app, /navigator\.clipboard\.writeText\(String\(button\.getAttribute\("data-copy"\)/);
  // And an absent identifier is the absent mark, not an empty copy button.
  assert.equal(ident(null), "-");
  assert.ok(LONG_TOKEN_CHARS < KEY.length, "the key id is a long token the old cell broke anywhere");
});

// M7. The approver was shown only as `https://dex.localtest.me#CiQx...`:
// alice and bob could not be told apart at a glance. The signed approval
// carries no name or email, so the cell shows a short form of the subject,
// the full principal on hover -- and "you" for the signed-in actor.

const BOB = "https://dex.localtest.me#CiQxZjBjOWE0ZS0wMDAyLTRhMWUtOWUxYS0wMDAwMDAwMDAwMDISBWxvY2Fs";

/** What a person SEES in the approver cell: the drawn short form. */
function seen(cellHtml) {
  const shown = /<span aria-hidden="true">([^<]*)<\/span>/.exec(cellHtml);
  return shown === null ? cellHtml.replace(/<[^>]*>/g, "").replace(/copy$/, "") : shown[1];
}

test("m7_two_approvers_read_differently_at_a_glance_and_the_signed_in_one_is_you", async () => {
  const two = renderApprovalList({ items: [pocApproval("a-1", DEX), pocApproval("a-2", BOB)] },
    Date.parse("2026-10-12T09:30:00Z"));
  const rows = two.slice(two.indexOf("<tbody>"), two.indexOf("</tbody>")).split("<tr").slice(1);
  const approvers = rows.map((row) => seen(firstRowCells("<tbody><tr" + row + "</tbody>")[2]));
  assert.notEqual(approvers[0], approvers[1],
    "NEGATIVE CONTROL: both read https://dex.localtest.me#CiQx... in a cut-off cell");
  for (const shown of approvers) {
    assert.ok(!shown.includes("dex.localtest.me"), "the issuer is on hover, not in the way: " + shown);
    assert.ok(shown.length <= 6 + 3 + 12, shown);
  }
  assert.deepEqual(approvers, ["CiQxZj...MDcSBWxvY2Fs", "CiQxZj...MDISBWxvY2Fs"]);
  // A subject that is already short is shown whole.
  assert.equal(principalShort("https://idp.example#bob", null), "bob");
  assert.ok("bob".length <= PRINCIPAL_SHORT_CHARS);
  // THE SESSION NAMES ITS OWN ACTOR, so the signed-in approver reads "you".
  const viewer = session("session-viewer.json");
  const mine = await signedInAs(viewer, async () =>
    renderApprovalList({ items: [pocApproval("a-3", viewer.actor.id)] }, 0));
  assert.equal(seen(firstRowCells(mine)[2]), "you (Dana Viewer)");
  assert.ok(firstRowCells(mine)[2].includes("title=\"oidc#dana\""), "with the principal on hover");
});

// M8. On wizard step 5 with no topic ticked, Check readiness did nothing: no
// message and no Preflight (the plan cannot be rendered, so `start` returned).

const WIZARD = fileURLToPath(new URL("./fixtures/", import.meta.url));
const wizardFixture = (name) => JSON.parse(readFileSync(WIZARD + name, "utf8"));

function wizardState() {
  const backups = wizardFixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  return initialState("logweir-t27", wizardFixture("wizard-clusters.json"), backups,
    { uid: point.metadata.uid, backup: point.metadata.name });
}

test("m8_check_readiness_with_no_topic_is_disabled_and_says_why", async () => {
  const state = wizardState();
  const ready = renderPreflightStep(state, await preparePlanOrProblem(state));
  assert.match(ready, /<button type="submit" id="restore-readiness-start">Check readiness/,
    "with a topic the button is live");
  assert.doesNotMatch(ready, /id="restore-readiness-blocked"/);

  state.fields.topics = [];
  const prepared = await preparePlanOrProblem(state);
  assert.equal(typeof prepared.problem, "string", "no topic, no plan");
  const html = renderPreflightStep(state, prepared);
  assert.match(html, /<button type="submit" id="restore-readiness-start" disabled aria-describedby="restore-readiness-blocked">/,
    "NEGATIVE CONTROL: an enabled button that did nothing on a click");
  assert.match(html, /<p class="complaint" id="restore-readiness-blocked">Select at least one topic in step 4/);
  assert.ok(NO_TOPIC_READINESS_SENTENCE.toLowerCase().includes("select at least one topic"));
});

// M10. The operation view labelled a restore's scorecard "receipt key"
// (`logweir/drills/....json`), and "scorecard key" read "-".

/** The value a fact list gives `label`, or `null` when it has no such row. */
function factValue(html, label) {
  const at = html.indexOf("<dt>" + label + "</dt><dd>");
  return at === -1 ? null : html.slice(at + label.length + 13, html.indexOf("</dd>", at));
}

test("m10_the_operation_view_labels_a_scorecard_as_a_scorecard_and_a_receipt_as_a_receipt", () => {
  const restore = operationFacts(decoded("operation-restore-completed.json").item, true);
  const shown = renderEvidence(restore);
  assert.equal(factValue(shown, "scorecard key"),
    "logweir/drills/01M2VKDDD95HA4NQTHGFRJ2WCJ.scorecard.json",
    "NEGATIVE CONTROL: it read \"-\" while the scorecard was labelled the receipt");
  assert.equal(factValue(shown, "receipt key"), null, "a restore has no receipt row");
  assert.match(factValue(shown, "scorecard sha256"), /^sha256:0d90834b/);

  const backup = operationFacts(decoded("operation-backup.json").item, true);
  const theirs = renderEvidence(backup);
  assert.match(factValue(theirs, "receipt key"), /receipt\.json$/);
  assert.equal(factValue(theirs, "scorecard key"), null, "a backup has no scorecard row");
});
