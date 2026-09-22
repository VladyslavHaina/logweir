// restore-catalog.spec.js -- PLAT-15.2's console arm: restoring a
// catalog-verified point with no Backup object behind it, and
// CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW (a run the controller wrote no window
// for, offered from its catalog row only when its own verdict is absent or
// NotAttempted).
//
// EVERY ROW CARRIES ITS NEGATIVE CONTROL: the same input with the one fact the
// behaviour depends on changed, which must flip the answer. A row whose control
// stays green proves nothing, so the controls are the second half of each row
// and never a separate, optional one.
//
// Pure functions and fake readers, as everything in this directory is: no DOM,
// no network, no clock.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  applyWizardDraft,
  backupCatalogOffer,
  backupCatalogOfferFrom,
  catalogPointOffer,
  catalogRecoveryPoint,
  catalogRowsForBackup,
  catalogWindow,
  findCatalogPoint,
  initialState,
  isCatalogPoint,
  mountRestoreWizard,
  noteCatalogSource,
  ownVerdictOf,
  parseTopicList,
  readOwnVerdicts,
  preparePlan,
  readCatalogOffers,
  recoveryPoints,
  renderCatalogOffers,
  renderNoCompletedBackup,
  renderPreparedWizard,
  resolveCatalogChoice,
  restoreBody,
  restoreCatalogPointRoute,
  restoreReadinessRequest,
  restoreRouteParams,
  setCatalogTopics,
  validateRestore,
  wizardDraftValues,
  CATALOG_POINT_PAGE_BUDGET,
} from "../pages/restore-wizard.js";
import {
  BACKUP_VERDICTS_INCOMPLETE_SENTENCE,
  pointRow,
  renderPoints,
} from "../pages/catalog.js";
import { latestRestorablePoint, restoreCell } from "../pages/schedules.js";
import { renderPlanBytes } from "../plan.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

const NS = "lw-p152";
const POINT = "lwp1-0123456789abcdef0123456789abcdef";
const OTHER = "lwp1-ffffffffffffffffffffffffffffffff";
const SET = "3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f-20260922-140000";
const RECEIPT_KEY = "logweir/backups/" + SET + "/01JB7Z00000000000000000000.receipt.json";
const RECEIPT = "sha256:" + "a1".repeat(32);
const MANIFEST = "sha256:" + "b2".repeat(32);

/** One point, as `GET .../catalogs/{name}/points` publishes it. */
function row(over) {
  return Object.assign({
    pointId: POINT,
    backupId: SET,
    runId: "01JB7Z00000000000000000000",
    recoveryPointAt: "2026-09-22T14:00:00Z",
    coveredFrom: "2026-09-22T13:00:00Z",
    coveredTo: "2026-09-22T14:00:00Z",
    availability: "Available",
    verification: "Verified",
    selectable: true,
    signerKeyId: "c".repeat(64),
    receiptKey: RECEIPT_KEY,
    receiptSha256: RECEIPT,
    manifestKey: SET + "/manifest.json",
    manifestSha256: MANIFEST,
    locations: [{ locationId: "s3://kafka-backups/team-a/prod", availability: "Available" }],
  }, over || {});
}

function catalogObject(over) {
  return Object.assign({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "RecoveryCatalog",
    metadata: { name: "archive", namespace: NS, uid: "cat-uid" },
    spec: { destinationRef: { name: "primary" } },
    status: {},
  }, over || {});
}

function destination() {
  return fixture("console/destination.json").item;
}

/** A run the controller verified NOTHING for: Succeeded, a set, no window. */
function unverifiedRun(verdict, receipt) {
  const evidence = { receiptSha256: receipt === undefined ? RECEIPT : receipt };
  if (verdict !== null) {
    evidence.verification = { result: verdict };
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: { name: "nightly-1", namespace: NS, uid: "run-uid-1",
      creationTimestamp: "2026-09-22T14:00:05Z" },
    spec: {
      sourceRef: { name: "source" },
      topics: ["orders"],
      destinationRef: { name: "primary", uid: destination().uid },
      archive: { url: "logweir-destination://primary" },
      scheduleRef: { name: "nightly" },
    },
    status: {
      phase: "Succeeded",
      backupId: SET,
      locationDigest: destination().locationDigest,
      evidence: evidence,
    },
  };
}

/** The readers the catalog half uses. `verdicts` answers the ONE operation
 *  read per run -- by name -- that the page makes for a run's own verdict;
 *  absent, it reads the verdict off the fixture object as a legacy-mode
 *  custom resource would carry it. */
function readersOver(pages, catalog, verdicts) {
  const calls = [];
  return {
    calls: calls,
    ownVerdict: async (name) => {
      calls.push("verdict:" + name);
      if (verdicts !== undefined) {
        const answer = verdicts[name];
        if (answer instanceof Error) {
          throw answer;
        }
        return answer;
      }
      return { verdict: null };
    },
    listCatalogs: async () => ({ items: [catalog || catalogObject()] }),
    readCatalog: async (name) => {
      calls.push("catalog:" + name);
      if (name !== "archive") {
        const error = new Error("not found");
        error.status = 404;
        throw error;
      }
      return catalog || catalogObject();
    },
    readPoints: async (name, query) => {
      calls.push("points:" + name + ":" + String(query.cursor || ""));
      const index = query.cursor === undefined ? 0 : Number(query.cursor);
      return pages[index];
    },
  };
}

function page(items, over, next) {
  return Object.assign({
    requestId: "r", items: items, truncated: false, viewExpired: false,
    page: { limit: 200, nextCursor: next === undefined ? null : next },
  }, over || {});
}

const apiWithDestination = {
  destination: async () => ({ item: destination() }),
};

async function catalogState(entry, extra) {
  const readers = readersOver([page([entry || row()])]);
  const choice = await resolveCatalogChoice(
    apiWithDestination, NS, Object.assign({ catalog: "archive", point: POINT }, extra || {}),
    { items: [] }, readers, undefined,
  );
  assert.equal(choice.state, "selected", "the fixture resolves: " + String(choice.reason));
  return initialState(NS, fixture("wizard-clusters.json"), { items: [] },
    { catalog: "archive", point: POINT }, choice.destination, choice);
}

// --------------------------------------------------------- 1. the plan golden

test("the_catalog_bound_plan_golden_is_what_the_emitter_renders", () => {
  // ARM 2 of the point golden; arm 1 is ui_lint.rs, which deserialises the
  // same file into the runner's RestoreSpec and asserts the binding arrives.
  const fields = fixture("plan-point-fields.json");
  const golden = readFileSync(FIXTURES + "plan-point.golden.yaml", "utf8");
  assert.equal(renderPlanBytes(fields), golden, "byte for byte");
  assert.match(golden, /\n {2}point:\n {4}point_id: "lwp1-/);
  // CONTROL: the Backup-bound golden carries no binding, and the same fields
  // without `point` render it with none.
  assert.doesNotMatch(readFileSync(FIXTURES + "plan.golden.yaml", "utf8"), /point_id/);
  const bare = Object.assign({}, fields);
  delete bare.point;
  assert.doesNotMatch(renderPlanBytes(bare), /point_id/);
});

test("a_malformed_binding_is_refused_before_an_approver_is_asked_to_sign_it", () => {
  const fields = fixture("plan-point-fields.json");
  for (const [field, value] of [
    ["pointId", "lwp1-0123"],
    ["pointId", "lwp1-0123456789ABCDEF0123456789ABCDEF"],
    ["receiptKey", " "],
    ["receiptSha256", "sha256:abc"],
    ["manifestSha256", "sha256:" + "B2".repeat(32)],
    ["manifestSha256", ""],
  ]) {
    const bad = Object.assign({}, fields, { point: Object.assign({}, fields.point, { [field]: value }) });
    assert.throws(() => renderPlanBytes(bad), /point\./, field + " = " + JSON.stringify(value));
  }
  // CONTROL: the well-formed binding renders.
  assert.match(renderPlanBytes(fields), /receipt_sha256: "sha256:/);
});

// ---------------------------------------------------- 2. the offer rule itself

test("a_selectable_complete_unredacted_row_is_offered_and_each_missing_fact_refuses_it", () => {
  assert.deepEqual(catalogPointOffer(row(), page([])), { offer: true, reason: null });
  const refusals = [
    ["a controller refusal of its Backup", row({ backupVerdict: "Invalid" }), page([]),
      /refused this point's own Backup evidence \(Invalid\)/],
    ["an incomplete verdict join", row(), page([], { backupVerdictsIncomplete: "Unavailable" }),
      /could not read every Backup verdict/],
    ["a truncated verdict join", row(), page([], { backupVerdictsIncomplete: "Truncated" }),
      /Truncated/],
    ["an expired view", row(), page([], { viewExpired: true }), /aged out/],
    ["a row the catalog does not mark selectable",
      row({ selectable: false, verification: "UntrustedSigner" }), page([]), /UntrustedSigner/],
    ["a redacted receipt key", row({ receiptKey: "[redacted].receipt.json" }), page([]),
      /redactor/],
    ["no manifest digest", row({ manifestSha256: undefined }), page([]), /manifest digest/],
    ["a malformed point id", row({ pointId: "p1" }), page([]), /point id/],
    ["no covered window", row({ coveredTo: undefined }), page([]), /covered window/],
    ["an empty window", row({ coveredTo: "2026-09-22T13:00:00Z" }), page([]), /covered window/],
  ];
  for (const [why, entry, onPage, reason] of refusals) {
    const verdict = catalogPointOffer(entry, onPage);
    assert.equal(verdict.offer, false, why);
    assert.match(verdict.reason, reason, why);
  }
});

test("the_catalog_window_ends_one_millisecond_before_coveredTo", () => {
  const window = catalogWindow(row());
  assert.equal(window.fromMs, Date.parse("2026-09-22T13:00:00Z"));
  assert.equal(window.toMs, Date.parse("2026-09-22T14:00:00Z") - 1,
    "coveredTo is EXCLUSIVE; the last instant a plan may name is one millisecond before it");
});

// ------------------------------------ 3. CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW

test("a_run_with_no_window_is_offered_from_its_row_only_when_its_own_verdict_defers", () => {
  for (const verdict of [null, "NotAttempted"]) {
    const offer = backupCatalogOffer(unverifiedRun(verdict), [row()], page([]));
    assert.equal(offer.offer, true, String(verdict) + ": " + String(offer.reason));
    assert.equal(offer.entry.pointId, POINT);
  }
  // THE RULE THE DEFECT IS ABOUT: never over a reached refusal.
  for (const verdict of ["Invalid", "Untrusted", "Pending", "SomethingNew"]) {
    const offer = backupCatalogOffer(unverifiedRun(verdict), [row()], page([]));
    assert.equal(offer.offer, false, verdict);
    assert.match(offer.reason, new RegExp(verdict));
  }
  // A run with its own window is not this rule's to answer.
  const windowed = unverifiedRun("Valid");
  windowed.status.windowCovered = { fromMs: 1, toMs: 2 };
  assert.equal(backupCatalogOffer(windowed, [row()], page([])).offer, false);
  // The row itself must still be offerable.
  assert.equal(backupCatalogOffer(unverifiedRun("NotAttempted"),
    [row({ selectable: false })], page([])).offer, false);
  assert.equal(backupCatalogOffer(unverifiedRun("NotAttempted"), [row()],
    page([], { backupVerdictsIncomplete: "Unavailable" })).offer, false);
});

test("the_row_is_the_one_for_the_runs_own_receipt_and_an_ambiguous_set_is_not_guessed", () => {
  const mine = row();
  const upstream = row({ pointId: OTHER, receiptSha256: "sha256:" + "c3".repeat(32) });
  assert.deepEqual(catalogRowsForBackup(unverifiedRun("NotAttempted"), [upstream, mine]), [mine],
    "two points under one set: the run's own receipt decides");
  const offer = backupCatalogOffer(unverifiedRun("NotAttempted"), [upstream, mine], page([]));
  assert.equal(offer.entry.pointId, POINT);
  // CONTROL: a run that reported no digest cannot choose between two points.
  const digestless = unverifiedRun("NotAttempted", "");
  assert.equal(backupCatalogOffer(digestless, [upstream, mine], page([])).offer, false);
  assert.equal(backupCatalogOffer(digestless, [mine], page([])).offer, true,
    "and with one point in the set there is nothing to choose between");
});

test("the_schedule_detail_offers_the_catalog_window_and_never_over_a_refusal", () => {
  const points = [noteCatalogSource(row(), "archive", page([]))];
  const cell = restoreCell(NS, unverifiedRun("NotAttempted"), points);
  assert.match(cell, /data-restore-from="catalog"/);
  assert.ok(cell.indexOf(restoreCatalogPointRoute(NS, "archive", POINT,
    unverifiedRun("NotAttempted")).replace(/&/g, "&amp;")) !== -1, cell);
  // CONTROLS.
  assert.doesNotMatch(restoreCell(NS, unverifiedRun("Invalid"), points), /Restore/,
    "a reached refusal is never made restorable by a row");
  const incomplete = [noteCatalogSource(row(), "archive",
    page([], { backupVerdictsIncomplete: "Unavailable" }))];
  assert.doesNotMatch(restoreCell(NS, unverifiedRun("NotAttempted"), incomplete), /Restore/);
  assert.doesNotMatch(restoreCell(NS, unverifiedRun("NotAttempted"), [row()]), /Restore/,
    "a row with no recorded catalog cannot name one in a link");
});

test("the_latest_point_action_counts_a_catalog_window_run_in_completion_order", () => {
  const points = [noteCatalogSource(row(), "archive", page([]))];
  const choice = latestRestorablePoint([unverifiedRun("NotAttempted")], points);
  assert.equal(choice.point.metadata.name, "nightly-1");
  // CONTROL: the same run refused is not a point at all.
  assert.equal(latestRestorablePoint([unverifiedRun("Invalid")], points).all.length, 0);
});

// ------------------------------------------------ 4. resolving a catalog point

test("the_route_carries_the_catalog_and_the_point_and_nothing_the_page_would_trust", () => {
  const route = restoreCatalogPointRoute(NS, "archive", POINT);
  assert.equal(route, "#/restore?ns=lw-p152&catalog=archive&point=" + POINT);
  const read = restoreRouteParams(route + "&receiptSha256=sha256%3Aforged");
  assert.equal(read.catalog, "archive");
  assert.equal(read.point, POINT);
  assert.equal(read.receiptSha256, undefined, "a digest in the address is never read");
});

test("a_catalog_point_is_found_across_pages_and_the_budget_is_said_as_such", async () => {
  const readers = readersOver([
    page([row({ pointId: OTHER })], {}, "1"),
    page([row()], { backupVerdictsIncomplete: undefined }),
  ]);
  const found = await findCatalogPoint((q) => readers.readPoints("archive", q), POINT);
  assert.equal(found.entry.pointId, POINT);
  assert.deepEqual(readers.calls, ["points:archive:", "points:archive:1"]);
  // A flag on ANY page is a fact about the read.
  const flagged = readersOver([
    page([row({ pointId: OTHER })], { backupVerdictsIncomplete: "Truncated" }, "1"),
    page([row()]),
  ]);
  const hit = await findCatalogPoint((q) => flagged.readPoints("archive", q), POINT);
  assert.equal(hit.page.backupVerdictsIncomplete, "Truncated");
  assert.equal(catalogPointOffer(hit.entry, hit.page).offer, false);
  // The budget.
  const endless = {
    readPoints: async (_name, query) => page([row({ pointId: OTHER })], {},
      String(Number(query.cursor || 0) + 1)),
  };
  const miss = await findCatalogPoint((q) => endless.readPoints("archive", q), POINT);
  assert.equal(miss.entry, null);
  assert.equal(miss.page.budgetExhausted, true);
  assert.ok(CATALOG_POINT_PAGE_BUDGET >= 25);
});

test("a_point_that_cannot_be_offered_is_a_refusal_naming_it_and_never_a_substitute", async () => {
  const cases = [
    ["not in the view", [page([row({ pointId: OTHER })])], /does not list this point/],
    ["refused by the controller", [page([row({ backupVerdict: "Untrusted" })])], /Untrusted/],
    ["join incomplete", [page([row()], { backupVerdictsIncomplete: "Unavailable" })],
      /could not read every Backup verdict/],
  ];
  for (const [why, pages, reason] of cases) {
    const choice = await resolveCatalogChoice(apiWithDestination, NS,
      { catalog: "archive", point: POINT }, { items: [] }, readersOver(pages), undefined);
    assert.equal(choice.state, "refused", why);
    assert.match(choice.reason, reason, why);
  }
  // CONTROL: the same point, offerable, resolves.
  const choice = await resolveCatalogChoice(apiWithDestination, NS,
    { catalog: "archive", point: POINT }, { items: [] }, readersOver([page([row()])]), undefined);
  assert.equal(choice.state, "selected");
  assert.ok(isCatalogPoint(choice.point));
});

test("an_offer_from_a_backup_must_be_read_through_the_destination_that_run_froze", async () => {
  const backups = { items: [unverifiedRun("NotAttempted")] };
  const via = { catalog: "archive", point: POINT, backup: "nightly-1", uid: "run-uid-1" };
  const notAttempted = { "nightly-1": { verdict: "NotAttempted", receiptSha256: RECEIPT } };
  const ok = await resolveCatalogChoice(apiWithDestination, NS, via, backups,
    readersOver([page([row()])], undefined, notAttempted), undefined);
  assert.equal(ok.state, "selected", String(ok.reason));
  assert.equal(ok.point.catalogPoint.backup.uid, "run-uid-1");
  // A different destination under the catalog.
  const elsewhere = catalogObject({ spec: { destinationRef: { name: "secondary" } } });
  const moved = await resolveCatalogChoice({
    destination: async () => ({ item: Object.assign({}, destination(), { name: "secondary" }) }),
  }, NS, via, backups, readersOver([page([row()])], elsewhere, notAttempted), undefined);
  assert.equal(moved.state, "refused");
  assert.match(moved.reason, /written through destination primary/);
  // The destination recreated under the run's name.
  const recreated = await resolveCatalogChoice({
    destination: async () => ({ item: Object.assign({}, destination(), { uid: "new-uid" }) }),
  }, NS, via, backups, readersOver([page([row()])], undefined, notAttempted), undefined);
  assert.equal(recreated.state, "refused");
  assert.match(recreated.reason, /recreated/);
  // The run's own verdict, READ NOW, is a refusal -- whatever the object this
  // page listed said (here it says NotAttempted).
  const refused = await resolveCatalogChoice(apiWithDestination, NS, via, backups,
    readersOver([page([row()])], undefined, { "nightly-1": { verdict: "Invalid" } }), undefined);
  assert.equal(refused.state, "refused");
  assert.match(refused.reason, /Invalid/);
  // A read that FAILS is not a deferring verdict: the listed object's own
  // word is not trusted in its place.
  const unreadable = await resolveCatalogChoice(apiWithDestination, NS, via, backups,
    readersOver([page([row()])], undefined, { "nightly-1": new Error("503") }), undefined)
    .catch((error) => ({ state: "threw", reason: String(error.message) }));
  assert.notEqual(unreadable.state, "selected");
  // The run is gone.
  const gone = await resolveCatalogChoice(apiWithDestination, NS, via, { items: [] },
    readersOver([page([row()])]), undefined);
  assert.equal(gone.state, "refused");
  assert.match(gone.reason, /no Backup in this namespace answers/);
});

// ----------------------------------------- 5. the plan, the check and the body

test("the_catalog_plan_is_bound_to_the_point_and_a_backup_plan_is_not", async () => {
  const state = await catalogState();
  setCatalogTopics(state, ["orders", "payments"]);
  const prepared = await preparePlan(state);
  assert.match(prepared.bytes, new RegExp("backup: \"" + SET + "\""));
  assert.match(prepared.bytes, new RegExp("point_id: \"" + POINT + "\""));
  assert.ok(prepared.bytes.indexOf("receipt_key: \"" + RECEIPT_KEY + "\"") !== -1);
  assert.ok(prepared.bytes.indexOf("receipt_sha256: \"" + RECEIPT + "\"") !== -1);
  assert.ok(prepared.bytes.indexOf("manifest_sha256: \"" + MANIFEST + "\"") !== -1);
  assert.match(prepared.bytes, /point_in_time: "2026-09-22T13:59:59.999Z"/,
    "the default point in time is the inclusive end of the catalog's window");
  assert.match(prepared.bytes, /bucket: "kafka-backups"/,
    "the archive is the catalog destination's public location");
  // CONTROL: a Backup-bound state renders no binding.
  const backups = fixture("wizard-backups.json");
  const chosen = recoveryPoints(backups)[0];
  const plain = initialState("t", fixture("wizard-clusters.json"), backups,
    { uid: chosen.metadata.uid, backup: chosen.metadata.name });
  assert.equal(plain.pointState, "selected");
  assert.equal(plain.fields.point, undefined);
  assert.doesNotMatch((await preparePlan(plain)).bytes, /point_id/);
});

test("the_topics_are_named_by_the_operator_and_an_empty_list_is_refused", async () => {
  const state = await catalogState();
  assert.deepEqual(state.fields.topics, [], "a catalog point publishes no topic list");
  assert.ok(validateRestore(state).topics, "nothing is restored until topics are named");
  assert.deepEqual(parseTopicList(" orders, payments  orders "), ["orders", "payments", "orders"],
    "duplicates are KEPT so the mapping check can refuse them by name");
  setCatalogTopics(state, ["orders"]);
  assert.equal(validateRestore(state).topics, undefined);
  assert.equal(validateRestore(state).backupSet, undefined);
});

test("the_readiness_check_names_the_catalog_point_and_only_it", async () => {
  const state = await catalogState(undefined, {});
  setCatalogTopics(state, ["orders"]);
  const prepared = await preparePlan(state);
  const request = restoreReadinessRequest(state, prepared);
  assert.deepEqual(request.restore.catalogPoint, { catalog: "archive", pointId: POINT });
  assert.equal(request.restore.recoveryPoint, undefined,
    "one point per check (CRD rule P10): never a Backup beside it");
  assert.equal(request.restore.sourceDestination, "primary");
  assert.equal(request.restore.evidenceDestination, "primary");
  assert.equal(request.restore.planBytes, prepared.bytes);
});

test("the_restore_body_reads_the_catalog_destination_and_pins_the_set", async () => {
  const state = await catalogState();
  setCatalogTopics(state, ["orders"]);
  const prepared = await preparePlan(state);
  const body = restoreBody(state, prepared);
  assert.equal(body.spec.backupSetRef, SET);
  assert.deepEqual(body.spec.sourceDestinationRef, { name: "primary" });
  assert.deepEqual(body.spec.evidenceDestinationRef, { name: "primary" });
  assert.equal(body.spec.sourceArchive.url, "logweir-destination://primary");
  assert.equal(body.spec.sourceArchive.secretRef, undefined);
  assert.equal(body.spec.planBytes, prepared.bytes);
});

test("a_legacy_archive_catalog_restores_through_its_archive_and_secret_name", async () => {
  const legacy = catalogObject({
    spec: { legacyArchive: { url: "s3://old-bucket/archive", credentialRef: { name: "reader" } } },
  });
  const choice = await resolveCatalogChoice({}, NS, { catalog: "archive", point: POINT },
    { items: [] }, readersOver([page([row()])], legacy), undefined);
  assert.equal(choice.state, "selected", String(choice.reason));
  const state = initialState(NS, fixture("wizard-clusters.json"), { items: [] },
    { catalog: "archive", point: POINT }, null, choice);
  setCatalogTopics(state, ["orders"]);
  const prepared = await preparePlan(state);
  assert.match(prepared.bytes, /bucket: "old-bucket"/);
  const body = restoreBody(state, prepared);
  assert.equal(body.spec.sourceArchive.url, "s3://old-bucket/archive");
  assert.deepEqual(body.spec.sourceArchive.secretRef, { name: "reader" });
  assert.equal(body.spec.sourceDestinationRef, undefined);
});

test("a_draft_is_applied_only_to_the_point_it_was_made_for", async () => {
  const state = await catalogState();
  setCatalogTopics(state, ["orders", "payments"]);
  const draft = wizardDraftValues(state);
  assert.equal(draft.catalogPointId, "catalog/archive/" + POINT);
  const fresh = await catalogState();
  assert.equal(applyWizardDraft(fresh, draft), true);
  assert.deepEqual(fresh.fields.topics, ["orders", "payments"]);
  // CONTROL: a Backup draft of the same set is not an edit to the catalog point.
  const backupDraft = Object.assign({}, draft, { catalogPointId: "" });
  assert.equal(applyWizardDraft(await catalogState(), backupDraft), false);
});

test("the_wizard_renders_the_catalog_point_and_its_binding", async () => {
  const state = await catalogState();
  setCatalogTopics(state, ["orders"]);
  const html = renderPreparedWizard(state, await preparePlan(state));
  assert.match(html, /id="catalog-topics"/);
  assert.match(html, /id="catalog-binding"/);
  assert.match(html, new RegExp("id=\"point-name\">" + POINT));
  assert.match(html, /id="catalog-archive"/);
  assert.match(html, /covered to \(inclusive\)/);
});

test("the_mount_opens_on_a_catalog_point_and_refuses_one_it_cannot_offer", async () => {
  const mount = async (pages) => {
    const node = {
      children: [],
      get firstChild() { return this.children.length === 0 ? null : this.children[0]; },
      removeChild() { return this.children.shift(); },
      appendChild(child) { this.children.push(child); return child; },
      querySelector() { return null; },
      querySelectorAll() { return []; },
    };
    const readers = readersOver(pages);
    const api = {
      list: async (_ns, plural) => plural === "backups"
        ? { items: [] }
        : fixture("wizard-clusters.json"),
      destination: async () => ({ item: destination() }),
      catalogReaders: readers,
    };
    await mountRestoreWizard(node, NS, { catalog: "archive", point: POINT },
      (html) => [{ html: html }], api, { signal: undefined, isCurrent: () => true });
    return node.children.map((child) => child.html || "").join("");
  };
  const html = await mount([page([row()])]);
  assert.match(html, /id="catalog-topics"/, "the six steps over the catalog point");
  assert.doesNotMatch(html, /catalog-point-refusal/);
  // CONTROL: the same mount over a refused row renders the refusal and no plan.
  const refused = await mount([page([row({ backupVerdict: "Invalid" })])]);
  assert.match(refused, /id="catalog-point-refusal"/);
  assert.doesNotMatch(refused, /id="catalog-topics"/);
  assert.doesNotMatch(refused, /id="create-restore"/);
});

// ------------------------------------------------ 6. the selector and catalog

test("a_namespace_with_no_backup_offers_the_connected_archives_points", async () => {
  const offers = await readCatalogOffers(NS, readersOver([page([row(),
    row({ pointId: OTHER, selectable: false })])]), undefined);
  assert.equal(offers.offers.length, 1, "only offerable rows are listed");
  const html = renderNoCompletedBackup(NS, { items: [] }, offers);
  assert.match(html, /Recovery points from connected archives/);
  assert.ok(html.indexOf(restoreCatalogPointRoute(NS, "archive", POINT).replace(/&/g, "&amp;")) !== -1);
  assert.doesNotMatch(html, /Wait for a scheduled run/,
    "the wait advice is not the whole story when the archive has points");
  // CONTROL: no catalog point, the empty state and the connect hint.
  const none = renderNoCompletedBackup(NS, { items: [] }, { offers: [], notes: [] });
  assert.match(none, /Wait for a scheduled run/);
  assert.match(none, /Connect an existing archive/);
  assert.equal(renderCatalogOffers(NS, { offers: [], notes: [] }), "");
});

test("a_catalog_whose_points_cannot_be_read_is_a_note_never_an_empty_answer", async () => {
  const readers = {
    listCatalogs: async () => ({ items: [catalogObject()] }),
    readPoints: async () => { throw new Error("A recovery catalog's point list has no route"); },
  };
  const offers = await readCatalogOffers(NS, readers, undefined);
  assert.equal(offers.offers.length, 0);
  assert.match(offers.notes[0], /could not be read/);
  assert.match(renderCatalogOffers(NS, offers), /data-catalog-note="true"/);
});

test("the_catalog_table_links_through_the_wizards_rule_and_says_why_it_does_not", () => {
  const cells = pointRow(row(), NS, "archive", "primary", page([]));
  assert.ok(cells[7].indexOf(restoreCatalogPointRoute(NS, "archive", POINT).replace(/&/g, "&amp;")) !== -1);
  const refused = pointRow(row({ backupVerdict: "Invalid", selectable: false }), NS, "archive",
    "primary", page([]));
  assert.doesNotMatch(refused[7], /Restore this point/);
  assert.match(refused[3], /Backup verdict Invalid/);
  const incomplete = pointRow(row(), NS, "archive", "primary",
    page([], { backupVerdictsIncomplete: "Unavailable" }));
  assert.doesNotMatch(incomplete[7], /Restore this point/);
  assert.match(incomplete[7], /data-restore-refused="wizard"/);
  const html = renderPoints(page([row()], { backupVerdictsIncomplete: "Truncated" }), NS,
    "archive", "primary");
  assert.match(html, /data-backup-verdicts-incomplete="Truncated"/);
  assert.ok(html.indexOf(BACKUP_VERDICTS_INCOMPLETE_SENTENCE.slice(0, 40)) !== -1);
  // CONTROL: a complete page carries no such banner.
  assert.doesNotMatch(renderPoints(page([row()]), NS, "archive", "primary"),
    /data-backup-verdicts-incomplete/);
});

test("the_offer_from_several_catalogs_names_the_catalog_its_row_came_from", () => {
  const offer = backupCatalogOfferFrom(unverifiedRun("NotAttempted"),
    [noteCatalogSource(row(), "second-archive", page([]))]);
  assert.equal(offer.offer, true);
  assert.equal(offer.catalog, "second-archive");
});

test("the_catalog_point_carries_the_live_destination_it_was_frozen_to", () => {
  const point = catalogRecoveryPoint(catalogObject(), row(), destination(), null);
  assert.equal(point.spec.destinationRef.name, "primary");
  assert.equal(point.spec.destinationRef.uid, destination().uid);
  assert.equal(point.status.locationDigest, destination().locationDigest);
  assert.equal(point.catalogPoint.backup, null);
});

test("a_console_list_does_not_publish_a_verdict_so_an_unread_run_is_never_offered", async () => {
  // A console-mode projection carries `__contract.mode: "console"` and no
  // `status.evidence` on a list. Its absence is "not published", not "none".
  const listed = unverifiedRun(null);
  delete listed.status.evidence;
  listed.__contract = { mode: "console", absent: [], unknown: [] };
  const points = [noteCatalogSource(row(), "archive", page([]))];
  assert.equal(backupCatalogOfferFrom(listed, points).offer, false,
    "unread: the list's silence is never read as a deferring verdict");
  // READ: the one operation read per candidate run notes the verdict and the
  // receipt digest, and the offer follows the READ verdict.
  const reads = [];
  await readOwnVerdicts([listed], points, async (name) => {
    reads.push(name);
    return { verdict: "NotAttempted", receiptSha256: RECEIPT };
  }, undefined);
  assert.deepEqual(reads, ["nightly-1"]);
  assert.equal(backupCatalogOfferFrom(listed, points).offer, true);
  // CONTROL: the same run read as Unknown (an Untrusted verdict the API
  // normalises to `unknown`) is refused.
  const other = unverifiedRun(null);
  delete other.status.evidence;
  other.__contract = { mode: "console", absent: [], unknown: [] };
  await readOwnVerdicts([other], points, async () => ({ verdict: "Unknown" }), undefined);
  assert.equal(backupCatalogOfferFrom(other, points).offer, false);
  // And a run with no row of its set, or with its own window, is not read.
  const windowed = unverifiedRun("Valid");
  windowed.status.windowCovered = { fromMs: 1, toMs: 2 };
  const unrelated = unverifiedRun(null);
  unrelated.status.backupId = "another-set";
  const skipped = [];
  await readOwnVerdicts([windowed, unrelated], points, async (name) => {
    skipped.push(name);
    return {};
  }, undefined);
  assert.deepEqual(skipped, []);
});

test("the_operation_words_map_to_the_verdicts_the_rule_reads", () => {
  assert.equal(ownVerdictOf({ verification: { state: "notAttempted" } }), "NotAttempted");
  assert.equal(ownVerdictOf({ verification: { state: "pending" } }), null,
    "pending on a finished run is the API's word for no verdict written");
  assert.equal(ownVerdictOf({ verification: { state: "unknown" } }), "Unknown");
  assert.equal(ownVerdictOf({ verification: { state: "invalid" } }), "Invalid");
  assert.equal(ownVerdictOf({ verification: { state: "somethingNew" } }), "Unknown");
  assert.equal(ownVerdictOf({ status: { evidence: { verification: { result: "Untrusted" } } } }),
    "Untrusted", "a legacy-mode custom resource is read as it is written");
  assert.equal(ownVerdictOf({ status: {} }), null);
});
