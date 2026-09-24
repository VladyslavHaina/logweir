// shared-console.spec.js -- the PoC round's shared-console defects and the
// class sweep behind them: console code written for the legacy direct-CR mode
// (the page behind `kubectl proxy`, reading custom resources) that is wrong
// when the same files are served by `logweir-api` (the shared console,
// reading the product API's projections).
//
//   * POC-P4  -- "Connect an existing archive" was refused 403 because the
//                request carried no `X-CSRF-Token`;
//   * POC-P2  -- the restore wizard's recovery points read "signed:
//                unverified" and "no manifest recorded" beside an API that
//                said `verificationState: valid, verifiedSuccess: true`;
//   * POC-P1  -- the shared console printed the legacy page's "runs with the
//                authority of the kubeconfig that started the proxy";
//   * CONSOLE-COMPLETION-HEADING-ON-FAILED -- a failed Restore showed "What
//                this restore produced";
//   * the class sweep's other rows (see each test's comment).
//
// EVERY ROW DRIVES THE CODE A BROWSER RUNS. The console rows go through
// `ui/client.js` and `ui/api.js` with only `fetch` replaced, so the headers
// asserted are the headers `api.js` set and the objects rendered are the
// projections `client.js` built from the API's own bytes -- two of them
// captured live on the PoC install (`fixtures/console/backup-poc.json`,
// `fixtures/backup-poc-cr.json`; see `fixtures/README.md`). Each row fails on
// the code before its fix; the report records which assertion does.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { CONSOLE, LEGACY, apiClient, resetMode, selectMode, sessionToken } from "../client.js";
import { connectArchive, listD3 } from "../operation-watch.js";
import { mountCatalog } from "../pages/catalog.js";
import {
  MANIFEST_ATTESTED,
  archiveAvailability,
  initialState,
  renderPointSelector,
  renderRecoveryPointStep,
} from "../pages/restore-wizard.js";
import { LIST_VERIFIED_CAPTION, RETENTION_SENTENCE } from "../render.js";
import { MODE_COPY, applyModeCopy, modeDecided } from "../app.js";
import { build, fakeDocument } from "./fake-dom.js";
import { operationFacts, renderCompletion, renderOperation } from "../pages/operation.js";
import { renderRestoreDetail } from "../pages/history.js";
import {
  COMPLETION_INSTANT_NOT_PUBLISHED,
  REPORT_TRUNCATED_SENTENCE,
  RETENTION_COVERAGE_UNKNOWN_SENTENCE,
  renderRetentionPanel,
  renderScheduleFacts,
} from "../pages/schedules.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const con = (name) => fixture("console/" + name);

function decode(html) {
  return String(html)
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

/** The `<dd>` a facts list carries for `label`, or `null`. */
function factOf(html, label) {
  const open = "<dt>" + label + "</dt><dd>";
  const at = html.indexOf(open);
  if (at === -1) {
    return null;
  }
  const from = at + open.length;
  return html.slice(from, html.indexOf("</dd>", from));
}

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
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      headers: { get: () => null },
      text: () => Promise.resolve(JSON.stringify(reply.body)),
    });
  };
  return { seen: seen, restore: () => { globalThis.fetch = original; } };
}

/** Pins the shared console from a session document carrying `token`. */
async function sharedConsole(token) {
  const session = con("session.json");
  session.csrfToken = token === undefined ? "poc-session-token-1" : token;
  resetMode();
  return selectMode({ probe: async () => ({ ok: true, status: 200, body: session }) });
}

/** Pins legacy mode: what a `kubectl proxy` path filter answers. */
async function legacyMode() {
  resetMode();
  return selectMode({ probe: async () => ({ ok: false, status: 403, body: null }) });
}

/** A product-API list envelope around `items`. */
function listOf(items) {
  return { items: items, page: { limit: 200, snapshot: "6845822" }, requestId: "01POCLIST" };
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

// ===========================================================================
// POC-P4: the connect-archive write carries the SESSION's synchroniser token
// ===========================================================================

const CONNECT_BODY = Object.freeze({
  name: "primary", destinationRef: { name: "dest-a" }, syncMode: "full",
});

/** A `CatalogResponse` the decoder accepts, for the create's answer. */
function catalogAnswer(ns, body) {
  const answer = con("catalog.json");
  answer.item.name = body.name;
  answer.item.namespace = ns;
  answer.item.destinationRef = body.destinationRef;
  return answer;
}

test("poc_p4_connect_archive_sends_the_session_s_csrf_token", async () => {
  await sharedConsole("poc-session-token-1");
  assert.equal(sessionToken(), "poc-session-token-1", "the session's token, held in memory");
  const wire = transport((u, init) =>
    init.method === "POST" ? { status: 201, body: catalogAnswer("team-a", JSON.parse(init.body)) }
      : undefined);
  try {
    // NO `deps`: exactly how the shell's catalog route reaches this function.
    await connectArchive("team-a", CONNECT_BODY, "logweir-ui.catalog.team-a.primary.k1");
    assert.equal(wire.seen.length, 1);
    assert.equal(wire.seen[0].url, "/api/v1/namespaces/team-a/catalogs");
    assert.equal(wire.seen[0].init.headers["X-CSRF-Token"], "poc-session-token-1",
      "THE DEFECT: the request carried no X-CSRF-Token, and the shared console answered 403 " +
        "'the synchronizer token does not match this session'");
    assert.equal(wire.seen[0].init.headers["Idempotency-Key"],
      "logweir-ui.catalog.team-a.primary.k1", "and the durable-create key rides beside it");
  } finally {
    wire.restore();
  }
  // A `deps.token` is NOT a way in any more: a write's token is the session's.
  await sharedConsole("poc-session-token-2");
  const again = transport((u, init) =>
    init.method === "POST" ? { status: 201, body: catalogAnswer("team-a", JSON.parse(init.body)) }
      : undefined);
  try {
    await connectArchive("team-a", CONNECT_BODY, "logweir-ui.catalog.team-a.primary.k2",
      { token: "a-token-handed-in" });
    assert.equal(again.seen[0].init.headers["X-CSRF-Token"], "poc-session-token-2",
      "a token handed in beside the call is not the session's and is not sent");
  } finally {
    again.restore();
  }
});

/** The smallest node `render.js`'s `replace` touches, holding one form. */
function fakeNode(form) {
  return {
    firstChild: null,
    adopted: [],
    appendChild(child) { this.adopted.push(child); },
    removeChild() {},
    querySelector(selector) {
      return String(selector).indexOf("data-connect-archive") !== -1 ? form : null;
    },
    querySelectorAll() { return []; },
    get last() { return this.adopted.length === 0 ? "" : this.adopted[this.adopted.length - 1].html; },
  };
}

function fakeForm(values) {
  const listeners = [];
  return {
    elements: {
      name: { value: values.name },
      destination: { value: values.destination },
      syncMode: { value: values.syncMode },
    },
    addEventListener(type, handler) {
      if (type === "submit") {
        listeners.push(handler);
      }
    },
    submit() {
      for (const handler of listeners.slice(-1)) {
        handler({ preventDefault() {} });
      }
    },
  };
}

test("poc_p4_the_catalog_page_mounted_as_the_shell_mounts_it_sends_the_token", async () => {
  // THE WHOLE PATH THE POC DROVE: `app.js` mounts the catalog route as
  // `mount(main, ns, parseFragment, lifecycle)` -- no `deps` -- and the form's
  // submit reaches the product API through `connectArchive`.
  await sharedConsole("poc-session-token-3");
  const wire = transport((u, init) => {
    if (init.method === "POST") {
      return { status: 201, body: catalogAnswer("p4-mount", JSON.parse(init.body)) };
    }
    return u.indexOf("/api/v1/namespaces/p4-mount/catalogs") === 0
      ? { status: 200, body: { items: [], page: { limit: 200 }, requestId: "r1" } }
      : undefined;
  });
  try {
    const form = fakeForm({ name: "primary", destination: "dest-a", syncMode: "full" });
    const node = fakeNode(form);
    await mountCatalog(node, "p4-mount", (html) => [{ html: html }], null);
    form.submit();
    for (let i = 0; i < 5; i += 1) {
      await tick();
    }
    const posts = wire.seen.filter((r) => r.init.method === "POST");
    assert.equal(posts.length, 1, "one connect request");
    assert.equal(posts[0].init.headers["X-CSRF-Token"], "poc-session-token-3",
      "the page the shell mounts sends the session's token");
    assert.ok(decode(node.last).indexOf("connect-result") !== -1,
      "and the outcome is a catalog, not a refusal: " + decode(node.last).slice(0, 300));
  } finally {
    wire.restore();
  }
});

test("class_a_every_product_api_write_in_ui_takes_its_token_from_the_session", () => {
  // THE CLASS GUARD (sweep item a). Every call of the four product-API write
  // functions outside `ui/api.js` must pass the session's token -- `tokenNow()`
  // or `sessionToken()` in `ui/client.js`'s module memory -- and nothing a
  // caller could hand in. Found by scanning, so a fifth write added later
  // without a token fails here rather than as a 403 in front of an operator.
  const WRITES = /\b(consoleCreate|consoleAction|consoleSetSuspension|consoleSchedulePolicy)\)?\(/g;
  const SESSION_TOKEN = /\btoken:\s*(tokenNow\(\)|sessionToken\(\)|decided === null \? null : decided\.token)/;
  const files = ["client.js", "operation-watch.js", "app.js", "workflow.js", "select.js",
    "lifecycle.js", "render.js"].map((f) => UI + f)
    .concat(["approvals.js", "backups.js", "catalog.js", "clusters.js", "destinations.js",
      "history.js", "keys.js", "operation.js", "protection.js", "restore-wizard.js",
      "schedules.js"].map((f) => UI + "pages/" + f));
  let calls = 0;
  for (const file of files) {
    const text = readFileSync(file, "utf8");
    for (const match of text.matchAll(WRITES)) {
      const open = match.index + match[0].length - 1;
      const args = argumentsAt(text, open);
      calls += 1;
      assert.match(args, SESSION_TOKEN,
        file.slice(UI.length) + ": " + match[1] + "(...) at offset " + String(match.index) +
          " does not take its X-CSRF-Token from the session: " + args.slice(0, 240));
    }
  }
  assert.ok(calls >= 14, "the scan found the product API's writes (" + String(calls) + ")");
});

/** The text between the parenthesis at `open` and its match, skipping strings. */
function argumentsAt(text, open) {
  let depth = 0;
  let quote = null;
  for (let i = open; i < text.length; i += 1) {
    const c = text[i];
    if (quote !== null) {
      if (c === "\\") {
        i += 1;
      } else if (c === quote) {
        quote = null;
      }
      continue;
    }
    if (c === "\"" || c === "'" || c === "`") {
      quote = c;
    } else if (c === "(") {
      depth += 1;
    } else if (c === ")") {
      depth -= 1;
      if (depth === 0) {
        return text.slice(open, i + 1);
      }
    }
  }
  return text.slice(open);
}

// ===========================================================================
// POC-P2: the recovery point reads the API's verdict and its receipt
// ===========================================================================

/** The live PoC Backup, as the shared console lists it: the product API's
 *  bytes, projected by `ui/client.js`. */
async function pocPointsFromTheApi(mutate) {
  const item = con("backup-poc.json").item;
  if (typeof mutate === "function") {
    mutate(item);
  }
  const wire = transport((u) =>
    u.indexOf("/api/v1/namespaces/team-a/backups") === 0
      ? { status: 200, body: listOf([item]) }
      : undefined);
  try {
    return await apiClient().list("team-a", "backups");
  } finally {
    wire.restore();
  }
}

function stepFor(backups) {
  const point = backups.items[0];
  const state = initialState("logweir-poc", { items: [] }, backups,
    { backup: point.metadata.name, uid: point.metadata.uid });
  assert.equal(state.pointState, "selected", "the PoC point resolves by its uid");
  return decode(renderRecoveryPointStep(state));
}

test("poc_p2_a_shared_console_point_verified_by_the_api_reads_verified_and_attested", async () => {
  await sharedConsole();
  const backups = await pocPointsFromTheApi();
  const point = backups.items[0];
  assert.equal(point.status.evidence, undefined,
    "a product-API list item carries no status.evidence: that is the shared-mode shape");
  assert.equal(point.status.manifestKey, undefined,
    "and no manifestKey: no controller writes one, so the API projects none");

  const step = stepFor(backups);
  const signed = factOf(step, "signed");
  assert.equal(signed, "<span class=\"badge badge-green\">" + LIST_VERIFIED_CAPTION + "</span>",
    "THE DEFECT: this read `unverified` beside verificationState=valid, verifiedSuccess=true");
  const archive = factOf(step, "archive");
  assert.equal(archive, "s3://kafka-backups/poc -- " + MANIFEST_ATTESTED,
    "THE DEFECT: this read `no manifest recorded`");

  // The selector's row for the same point says the same.
  const selector = decode(renderPointSelector(initialState("logweir-poc", { items: [] }, backups)));
  assert.ok(selector.indexOf(LIST_VERIFIED_CAPTION) !== -1, "the selector row is verified too");
  assert.ok(selector.indexOf(MANIFEST_ATTESTED) !== -1, "and attested");
  assert.equal(selector.indexOf("no manifest recorded"), -1);
});

test("poc_p2_a_shared_console_point_the_api_did_not_verify_is_never_green", async () => {
  // THE OTHER DIRECTION, WHICH THE FIX MUST NOT BREAK: never green for
  // unverified. Each summary the API can publish for a point that did not
  // verify reads `unverified` with its case, and its archive is not attested.
  await sharedConsole();
  for (const [state, success, words] of [
    ["notAttempted", false, /unverified: not attempted/],
    ["invalid", false, /unverified: invalid/],
    ["pending", false, /unverified: pending/],
    ["noEvidence", false, /unverified: no evidence/],
    // `valid` with `verifiedSuccess: false`: the signature verified and the
    // controller's green rule did not hold (an untrusted basis, a failed run).
    ["valid", false, /unverified: the document verified, and the run did not succeed/],
    // And a claimed success on a state that is not `valid` is still not green.
    ["notAttempted", true, /unverified: not attempted/],
  ]) {
    const backups = await pocPointsFromTheApi((item) => {
      item.operation.verificationState = state;
      item.operation.verifiedSuccess = success;
    });
    const step = stepFor(backups);
    const signed = factOf(step, "signed");
    assert.equal(signed.indexOf("badge-green"), -1, state + "/" + success + " is not green");
    assert.match(signed, words, state + "/" + success);
    assert.match(factOf(step, "archive"), / -- no manifest recorded$/,
      state + "/" + success + ": nothing attests the manifest");
  }
});

test("poc_p2_legacy_mode_reads_the_real_custom_resource_by_the_same_rule", () => {
  // LEGACY MODE KEEPS WORKING, and now reads the real object correctly as
  // well: `backup-poc-cr.json` is the Backup the PoC's controller wrote, with
  // `status.evidence.verification.result: Valid` on basis `Current`, exit 0
  // and -- like every Backup a controller in this tree writes -- no
  // `status.manifestKey`. The old column read `Valid` as a bare word and
  // "no manifest recorded".
  const cr = fixture("backup-poc-cr.json");
  assert.equal(cr.status.manifestKey, undefined, "the real controller wrote no manifestKey");
  const backups = { items: [cr] };
  const step = stepFor(backups);
  assert.equal(factOf(step, "signed"),
    "<span class=\"badge badge-green\">verified by weirkeeper at 2026-09-24T13:36:04Z against " +
      "key 28cf560620b0ea46bc91fd548e4cd28dfc0599daaf5816e0a401180181512a5a</span>",
    "the Backups page's own green label, with the key and the instant the object records");
  assert.equal(archiveAvailability(cr), MANIFEST_ATTESTED);

  // AND ITS CONTROLS: the same object with its verdict taken away, with a
  // refused verdict, or with a non-zero exit is not green and not attested.
  for (const [what, change] of [
    ["no verification block", (s) => { delete s.evidence.verification; }],
    ["result Untrusted", (s) => { s.evidence.verification.result = "Untrusted"; }],
    ["basis RecordedBeforeRevocation",
      (s) => { s.evidence.verification.trust.basis = "RecordedBeforeRevocation"; }],
    ["exit 2", (s) => { s.exitCode = 2; }],
  ]) {
    const changed = fixture("backup-poc-cr.json");
    change(changed.status);
    const html = stepFor({ items: [changed] });
    assert.equal(factOf(html, "signed").indexOf("badge-green"), -1, what + " is not green");
    assert.match(factOf(html, "signed"), /unverified/, what);
    assert.equal(archiveAvailability(changed), "no manifest recorded", what);
  }
  // A controller that DID record a manifest key keeps saying so.
  const recorded = fixture("backup-poc-cr.json");
  recorded.status.manifestKey = "logweir/backups/x/manifest.json";
  delete recorded.status.evidence.verification;
  assert.equal(archiveAvailability(recorded), "manifest recorded");
});

// ===========================================================================
// POC-P1: the masthead says whose authority the page carries, per mode
// ===========================================================================

test("poc_p1_the_static_page_prints_no_legacy_authority_sentence", () => {
  const html = readFileSync(UI + "index.html", "utf8").replace(/<!--[\s\S]*?-->/g, "");
  const body = html.slice(html.indexOf("<body>"));
  for (const legacyOnly of ["kubeconfig", "kubectl proxy", "the proxy"]) {
    assert.equal(body.indexOf(legacyOnly), -1,
      "THE DEFECT: index.html's visible copy says `" + legacyOnly + "`, which the shared " +
        "console printed to every signed-in user");
  }
  assert.ok(body.indexOf("id=\"masthead-tagline\"") !== -1, "the tagline is addressable");
  assert.ok(body.indexOf("id=\"colophon-serving\"") !== -1, "and so is the serving line");
});

function mastheadDocument() {
  const doc = fakeDocument("");
  doc.body.appendChild(build(doc, ["p", { id: "masthead-tagline" }, "neutral tagline"]));
  doc.body.appendChild(build(doc, ["p", { id: "colophon-serving" }, "neutral serving line"]));
  return doc;
}

test("poc_p1_each_mode_writes_its_own_authority_sentence", () => {
  const shared = mastheadDocument();
  assert.equal(applyModeCopy(shared, CONSOLE), true);
  const sharedText = shared.getElementById("masthead-tagline").textContent + " " +
    shared.getElementById("colophon-serving").textContent;
  for (const legacyOnly of ["kubeconfig", "kubectl", "proxy"]) {
    assert.equal(sharedText.indexOf(legacyOnly), -1, "the shared console says no `" + legacyOnly + "`");
  }
  assert.match(sharedText, /Logweir product API/);
  assert.match(sharedText, /logweir-api/);
  assert.equal(shared.getElementById("colophon-serving").children[0].tagName, "CODE",
    "a code segment is an element, never parsed markup");

  const legacy = mastheadDocument();
  applyModeCopy(legacy, LEGACY);
  assert.match(legacy.getElementById("masthead-tagline").textContent,
    /runs with the authority of the kubeconfig that started the proxy serving it/,
    "the legacy page keeps its disclosure");
  assert.match(legacy.getElementById("colophon-serving").textContent, /Served by kubectl proxy/);

  // Before the probe answers, or for a word this build does not know, nothing
  // changes: the neutral copy is true of both.
  const undecided = mastheadDocument();
  assert.equal(applyModeCopy(undecided, null), false);
  assert.equal(undecided.getElementById("masthead-tagline").textContent, "neutral tagline");
  assert.deepEqual(Object.keys(MODE_COPY).sort(), [CONSOLE, LEGACY].sort());
});

test("poc_p1_the_shell_writes_the_decided_mode_s_sentence_when_the_probe_answers", async () => {
  // `boot` cannot run under node, so what it does with the probe's answer is
  // `modeDecided`, and this is the row that holds the wiring.
  await sharedConsole();
  const shared = mastheadDocument();
  let renders = 0;
  modeDecided({ mode: CONSOLE }, shared, { allowed: [], selected: "" }, () => { renders += 1; });
  assert.match(shared.getElementById("masthead-tagline").textContent, /Logweir product API/,
    "the shared console's own sentence, once the probe said console");
  assert.equal(renders, 1, "and its grants replaced the namespace list, one more render");

  await legacyMode();
  const legacy = mastheadDocument();
  modeDecided({ mode: LEGACY }, legacy, { allowed: ["a"], selected: "a" }, () => { renders += 1; });
  assert.match(legacy.getElementById("masthead-tagline").textContent, /kubeconfig/,
    "the legacy page's disclosure, once the probe was refused");
  assert.equal(renders, 1, "and no render: legacy mode has no grants");
});

// ===========================================================================
// CONSOLE-COMPLETION-HEADING-ON-FAILED
// ===========================================================================

test("a_failed_restore_has_no_completion_section_in_either_mode", () => {
  const HEADING = "What this restore produced";
  // Console: the operation view's own document for a run that ended failed --
  // with no completion, and with one an older controller copied anyway.
  const base = con("operation-restore-completed.json").item;
  for (const [what, change] of [
    ["failed, no completion", (o) => { o.state = "failed"; delete o.completion; }],
    ["failed, a stale completion", (o) => { o.state = "failed"; }],
    ["refused", (o) => { o.state = "refused"; delete o.completion; }],
    ["cancelled", (o) => { o.state = "cancelled"; }],
  ]) {
    const document = JSON.parse(JSON.stringify(base));
    change(document);
    const page = decode(renderOperation({ ns: "team-a", name: document.name, console: true,
      document: document }));
    assert.equal(page.indexOf(HEADING), -1, "console, " + what + ": no completion heading");
    assert.equal(page.indexOf("No completion was recorded"), -1, what);
  }
  // Legacy: the custom resource of a restore that failed its integrity check.
  const failed = fixture("restore-valid-failintegrity.json");
  const object = Array.isArray(failed.items) ? failed.items[0] : failed;
  const legacyPage = decode(renderOperation({ ns: "team-a", name: object.metadata.name,
    console: false, document: object }));
  assert.equal(legacyPage.indexOf(HEADING), -1, "legacy, phase Failed: no completion heading");

  // CONTROL: a succeeded run keeps its panel, with and without counts.
  assert.ok(decode(renderCompletion(operationFacts(base, true))).indexOf(HEADING) !== -1,
    "a succeeded run with a completion shows it");
  const noCounts = JSON.parse(JSON.stringify(base));
  delete noCounts.completion;
  assert.match(decode(renderCompletion(operationFacts(noCounts, true))),
    /data-completion="unverified"/, "and a succeeded run without one says it is not yet verified");
});

// ===========================================================================
// the class sweep's other rows (item b)
// ===========================================================================

test("sweep_a_shared_console_restore_detail_shows_the_scope_the_api_published", async () => {
  // `history.js` `scopeOf` reads `status.verificationScope`, else the custom
  // resource's `status.integrity`. The console detail carried neither --
  // `decodeOperation` keeps the frozen sixteen fields and drops D3's
  // `verificationScope` -- so the page said "No verification scope was
  // recorded" about a run the API had scoped.
  await sharedConsole();
  const wire = transport((u) => {
    if (u.indexOf("/api/v1/namespaces/team-a/operations/restore/") === 0) {
      return { status: 200, body: con("operation-restore-completed.json") };
    }
    return u.indexOf("/api/v1/namespaces/team-a/restores/") === 0
      ? { status: 200, body: con("restore.json") }
      : undefined;
  });
  try {
    const restore = await apiClient().get("team-a", "restores", con("restore.json").item.name);
    assert.deepEqual(restore.status.verificationScope,
      { level: "sampled", recordsSampled: 64, recordsSampledMatching: 64, recordsExpected: 200 });
    const html = decode(renderRestoreDetail(restore));
    assert.match(html, /64 of 64 sampled records matched byte-for-byte; 200 records were expected/,
      "THE DEFECT: this read 'No verification scope was recorded for this run'");
    assert.equal(html.indexOf("No verification scope was recorded"), -1);
  } finally {
    wire.restore();
  }
  // CONTROL: an operation route that published no scope keeps the absent sentence.
  const unscoped = con("operation-restore-completed.json");
  delete unscoped.item.verificationScope;
  const bare = transport((u) => {
    if (u.indexOf("/api/v1/namespaces/team-a/operations/restore/") === 0) {
      return { status: 200, body: unscoped };
    }
    return u.indexOf("/api/v1/namespaces/team-a/restores/") === 0
      ? { status: 200, body: con("restore.json") }
      : undefined;
  });
  try {
    const restore = await apiClient().get("team-a", "restores", con("restore.json").item.name);
    assert.equal(restore.status.verificationScope, undefined);
    assert.match(decode(renderRestoreDetail(restore)), /No verification scope was recorded/);
  } finally {
    bare.restore();
  }
});

/** The console schedule fixture with a retention report the API bounded. */
async function consoleSchedule(mutateReport) {
  const list = con("schedules-list.json");
  const item = list.items[0];
  if (typeof mutateReport === "function") {
    mutateReport(item.status.retentionReport);
  }
  const wire = transport((u) =>
    u.indexOf("/api/v1/namespaces/team-a/schedules") === 0 ? { status: 200, body: list } : undefined);
  try {
    return (await apiClient().list("team-a", "backupschedules")).items[0];
  } finally {
    wire.restore();
  }
}

test("sweep_a_shared_console_retention_report_keeps_its_note_its_cut_and_its_skipped_count",
  async () => {
    // The API publishes `note`, `truncated` and `skippedManifests`; the
    // projection dropped all three, so a cut list read as the whole report, a
    // report with unreadable manifests read as one with none, and "why nothing
    // would be removed" read `-`.
    await sharedConsole();
    const schedule = await consoleSchedule((report) => {
      report.note = "every set is inside keepLast";
      report.truncated = true;
      report.skippedManifests = 2;
    });
    const panel = decode(renderRetentionPanel(schedule, null, []));
    assert.equal(factOf(panel, "note"), "every set is inside keepLast", "THE DEFECT: `-`");
    assert.ok(panel.indexOf(REPORT_TRUNCATED_SENTENCE) !== -1, "the cut is said");
    assert.match(panel, /data-skipped-manifests="2">2 manifests could not be read/,
      "and the unreadable manifests are counted");

    // CONTROL: an uncut report with nothing skipped says neither.
    const plain = await consoleSchedule((report) => {
      report.truncated = false;
      report.skippedManifests = 0;
    });
    const quiet = decode(renderRetentionPanel(plain, null, []));
    assert.equal(quiet.indexOf(REPORT_TRUNCATED_SENTENCE), -1);
    assert.equal(quiet.indexOf("Manifests that could not be read"), -1);
  });

test("sweep_no_deletion_is_never_claimed_beside_a_policy_that_deletes", async () => {
  // `policyForSchedule` reads `status.retentionReport.supersededBy`, which no
  // BackupSchedule CRD in this tree has, no controller writes and the API does
  // not project. So the panel printed "Logweir never deletes from your
  // archive" beside a namespace whose RetentionPolicy deletes (both modes; the
  // shared console reaches the policies through the product API).
  await sharedConsole();
  const schedule = await consoleSchedule();
  const wire = transport((u) =>
    u.indexOf("/api/v1/namespaces/team-a/retention-policies") === 0
      ? { status: 200, body: con("retention-policies-list.json") }
      : undefined);
  let policies;
  try {
    policies = (await listD3("retention", "team-a", {})).items;
  } finally {
    wire.restore();
  }
  const panel = decode(renderRetentionPanel(schedule, null, policies));
  assert.equal(panel.indexOf(RETENTION_SENTENCE), -1,
    "THE DEFECT: 'Logweir never deletes from your archive' beside enforce-a (LogweirWorker)");
  assert.ok(panel.indexOf(RETENTION_COVERAGE_UNKNOWN_SENTENCE) !== -1, "the page says it cannot tell");
  assert.match(panel, /<td>enforce-a<\/td><td>dest-a<\/td><td>LogweirWorker<\/td>/);
  assert.match(panel, /<td>declared-c<\/td><td>dest-b<\/td><td>ExternalLifecycleDeclared<\/td>/);
  assert.equal(panel.indexOf("<td>keep-a</td>"), -1, "a recommendation-only policy is not listed");

  // Legacy mode, over the custom resource the D3 live run recorded.
  const legacyPolicy = JSON.parse(readFileSync(FIXTURES + "d3/retention-enforce.json", "utf8"));
  assert.equal(decode(renderRetentionPanel(schedule, null, [legacyPolicy]))
    .indexOf(RETENTION_SENTENCE), -1, "legacy: the same refusal to claim");

  // CONTROLS: no policy, or only recommendation-only ones, keep the sentence;
  // and a report that names its covering policy keeps that policy's own words.
  const onlyRecommend = policies.filter((p) => p.status.enforcement === "RecommendationOnly");
  for (const [what, list] of [["no policy", []], ["recommendation only", onlyRecommend]]) {
    assert.ok(decode(renderRetentionPanel(schedule, null, list)).indexOf(RETENTION_SENTENCE) !== -1,
      what + " keeps the no-deletion sentence");
  }
});

test("sweep_the_latest_point_s_completion_instant_says_when_it_is_the_creation_instant",
  async () => {
    // A shared-console Backup list carries no conditions, so "Latest point
    // completed" printed the run's CREATION instant as its completion.
    await sharedConsole();
    const backups = await pocPointsFromTheApi();
    const schedule = await consoleSchedule();
    const html = decode(renderScheduleFacts(schedule, { mine: backups.items }, [],
      "2026-09-24T14:00:00Z"));
    assert.match(factOf(html, "Latest point completed"),
      /^2026-09-24T13:40:00Z <span class="note" data-completed-from="creation">/,
      "THE DEFECT: the creation instant, unlabelled, as the completion");
    assert.ok(html.indexOf(COMPLETION_INSTANT_NOT_PUBLISHED) !== -1);

    // CONTROL: legacy mode's custom resource records `Complete` and says that.
    const cr = fixture("backup-poc-cr.json");
    const legacy = decode(renderScheduleFacts(schedule, { mine: [cr] }, [],
      "2026-09-24T14:00:00Z"));
    assert.equal(factOf(legacy, "Latest point completed"), "2026-09-24T13:36:04.184994136Z",
      "the Complete condition's own instant, with no caveat");
  });

test("the_suite_leaves_the_mode_undecided", async () => {
  // Other files decide their own mode; this one leaves none behind.
  await legacyMode();
  resetMode();
});
