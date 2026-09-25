// pages/catalog.js -- THE DURABLE RECOVERY CATALOG (PLAT-15.1, PLAT-15.2).
//
// WHAT THIS PAGE IS FOR: the archive is the truth, and this is the view of it.
// A recovery point exists because a signed receipt exists in object storage,
// not because a Backup custom resource does -- so a cluster rebuilt from
// nothing, or an archive written by another installation entirely, still has
// every point it ever had, and connecting it here is how you see them.
//
// ===========================================================================
// THE FOUR RULES THIS PAGE WILL NOT BEND
// ===========================================================================
//
// 1. AVAILABILITY AND VERIFICATION ARE TWO COLUMNS, NEVER ONE. "The archive
//    can still serve this point" and "its receipt verifies under a key this
//    installation accepts" are different facts with different repairs, and D3
//    section 5.4 keeps them as separate axes for exactly that reason. Whether a point
//    may be restored from is the catalog's OWN materialised `selectable`
//    field; this page reads it and never recomputes `Available AND (Verified
//    or VerifiedHistorical)` from the two enums beside it.
//
// 2. THERE IS NO ONE-CLICK TRUST, AND THERE WILL NOT BE ONE HERE. A point
//    signed by a key this installation does not list comes back
//    `UntrustedSigner`. The honest surface for that is the key id -- the
//    SHA-256 of the DER SPKI, the same number `openssl` prints -- and the
//    command to compute it from the public half its holder gives you, out of
//    band. A button that added the key would turn "this archive says it is
//    signed by X" into "this installation trusts X" on the strength of the
//    archive's own claim, which is precisely what D3 section 5.5 step 3 and
//    `docs/keys.md` forbid: a key arriving beside an archive is never trusted
//    by proximity.
//
// 3. NOTHING IS HIDDEN. Every entry is listed with its exact state and its
//    remedy sentence. A `Missing` point is shown as missing; a `Conflict` is
//    shown as a conflict. Dropping the rows that are not green would make the
//    inventory agree with itself and disagree with the bucket.
//
// 4. THE VIEW IS A WINDOW AND SAYS SO. The Kubernetes side materialises the
//    newest `viewLimit` points into page ConfigMaps whose lifetime is the sync
//    Job's TTL. When it expires, THE VIEW is gone and the archive is untouched
//    -- and this page says that, because "the catalog is empty" and "this
//    cluster has forgotten how to list the catalog" look identical in a table
//    and could not be more different in an incident.
//
// IT SUBMITS EXACTLY ONE THING: the connect-archive create. That is a durable
// create with an `Idempotency-Key` (D3 section 5.5 step 1), and it creates a
// RecoveryCatalog -- a read-only view over a bucket. It grants no trust, moves
// no object and deletes nothing.

import {
  ABSENT,
  CATALOG_WINDOW_SENTENCE,
  NO_ONE_CLICK_TRUST_SENTENCE,
  TWO_AXES_SENTENCE,
  VIEW_EXPIRED_SENTENCE,
  badge,
  cell,
  copyBlock,
  detailLink,
  errorBlock,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  listFooter,
  mutationStatus,
  replace,
  table,
  conditionBadge,
  flagBadge,
  when,
} from "../render.js";
import {
  active,
  cancelled,
  dropDraft,
  fieldErrors,
  formKey,
  keepDraft,
  mutationFor,
  readDraft,
  readOptions,
  refusal,
  watchMutation,
} from "../lifecycle.js";
import {
  connectArchive,
  listD3,
  readCatalogPoints,
  readCatalogSigners,
  readD3,
} from "../operation-watch.js";
import { FINGERPRINT_COMMAND } from "./keys.js";
import { apiClient } from "../client.js";
import {
  REDACTION_MARKER,
  catalogPointOffer,
  isRedacted,
  restoreCatalogPointRoute,
} from "./restore-wizard.js";

// The product API's reads, for the destinations the connect form offers.
const API = apiClient();

/** The sentence a namespace with no catalog carries. */
export const NO_CATALOG_SENTENCE =
  "No RecoveryCatalog exists in this namespace. Without one, the points in your archive are " +
  "still there and nothing in Kubernetes can list them: create one below to connect a " +
  "destination you already hold a read-only credential for.";

/** The sentence the point table carries when the window holds no entry. */
export const NO_POINT_SENTENCE =
  "This catalog's view holds no point. That is a statement about the VIEW: check the sync " +
  "conditions above before concluding anything about the archive.";

/** The two sync modes the connect form offers, in the REQUEST's own spelling.
 *
 *  LOWERCASE, AND THAT IS NOT A STYLE CHOICE. `RecoveryCatalog.spec.sync.mode`
 *  is `Index`/`Full` on the custom resource and the request body spells the
 *  same two `index`/`full`; the API translates once. Sending the CRD's
 *  spelling here would be a `422` the form could not place.
 *
 *  `full` is what "connect an existing archive" needs: it rescans every
 *  catalog record under `logweir/catalog/v1/points/` rather than only the day
 *  shards of the index -- and it reads RECORDS, not bare receipts (see
 *  [`CATALOG_MODE_HELP`]). */
export const SYNC_MODES = Object.freeze(["full", "index"]);

/** What the two sync modes read, said under the selector (PoC defect P6).
 *
 *  It used to say that Full "walks the receipts and manifests in the bucket".
 *  It does not: both modes read the durable catalog's signed RECORDS under
 *  `logweir/catalog/v1/` (`docs/kubernetes.md` section 7d), and the sync Job is
 *  read-only by design, so it can never write the record a receipt is missing.
 *  A point written by a release before the catalog existed (`v0.1.5` and
 *  earlier) has a receipt and no record, and neither mode shows it until the
 *  operator backfills the records once with `logweir catalog sync`. */
export const CATALOG_MODE_HELP =
  "Full rescans every catalog record under logweir/catalog/v1/points/; Index reads the day " +
  "shards of the catalog index, newest first. Neither reads a backup receipt that has no " +
  "catalog record: points written before the catalog existed (v0.1.5 and earlier) appear only " +
  "after their records are backfilled once with logweir catalog sync, run with a key that may " +
  "write under logweir/catalog/v1/ and the public keys the receipts must verify under.";

/** What connecting an archive does, and what it does not. */
export const CONNECT_SENTENCE =
  "Connecting an archive creates a RecoveryCatalog: a read-only view over a destination you " +
  "already hold a credential for. It reads; it writes nothing to your bucket, moves nothing and " +
  "deletes nothing. Points it finds signed by a key this installation does not list come back " +
  "as untrusted, and no step of this form changes that.";

/** The draft this form keeps: the three inputs, the intent that makes its
 *  submit durable, and THE BODY THAT INTENT WAS SPENT ON.
 *
 *  `spentOn` is the last one and the least obvious. An idempotency key binds a
 *  REQUEST, not a form: PLAT-17.1's rule answers `409 idempotency_conflict`
 *  when a key arrives with a different request than the one it was first seen
 *  with. So an operator who submits, gets a refusal, CORRECTS the destination
 *  and submits again would spend the same key on a different body -- and every
 *  further retry would answer 409 until the page was reloaded. Recording what
 *  the key was spent on is what lets [`connectIntent`] mint a new one when the
 *  body moves, which is the same escape `ui/pages/schedules.js` gives a manual
 *  run through "Back up again". */
export const CONNECT_FIELDS = Object.freeze([
  "name", "destination", "syncMode", "intent", "spentOn",
]);

/** The form's key, per namespace. */
export function connectKey(ns) {
  return formKey(ns, "connect-archive", "");
}

/** A fresh idempotency intent for one connect draft. Random, for the reasons
 *  `ui/pages/schedules.js` gives at `mintIntent`: a counter collides across
 *  page loads and a clock collides between two tabs. */
export function mintConnectIntent() {
  const source = globalThis.crypto;
  if (source === undefined || source === null || typeof source.getRandomValues !== "function") {
    throw refusal(
      "this page will not connect an archive here: minting an idempotency key needs the " +
        "platform's random source, and it is unavailable. Without a unique key a second click " +
        "could return somebody else's catalog instead of creating yours.",
    );
  }
  const bytes = source.getRandomValues(new Uint8Array(16));
  let hex = "";
  for (const byte of bytes) {
    hex += byte.toString(16).padStart(2, "0");
  }
  return "logweir-ui.catalog." + hex;
}

/** The body one intent is spent on, as the string the draft keeps. */
export function spentOn(body) {
  return JSON.stringify([
    String((body || {}).name || ""),
    String(((body || {}).destinationRef || {}).name || ""),
    String((body || {}).syncMode || ""),
  ]);
}

/** THE INTENT THIS DRAFT HOLDS, for the body it is about.
 *
 *  Minted on first use and kept for the life of the draft, so a double click,
 *  a lost response and a retry after a 503 are ONE request. **Re-minted when
 *  the body moves**, because an idempotency key binds a request: spending a
 *  key on a corrected body is a `409 idempotency_conflict` that no further
 *  retry can clear (see [`CONNECT_FIELDS`]). `body` absent means "just tell me
 *  what this draft holds", and re-mints nothing. */
export function connectIntent(key, body) {
  const draft = readDraft(key);
  const held = draft === null ? undefined : draft.intent;
  const was = draft === null ? undefined : draft.spentOn;
  const now = body === undefined ? undefined : spentOn(body);
  const moved = now !== undefined && typeof was === "string" && was !== now;
  if (typeof held === "string" && held.length >= 8 && !moved) {
    return held;
  }
  const minted = mintConnectIntent();
  const kept = Object.assign({}, draft || {}, { intent: minted });
  if (now !== undefined) {
    kept.spentOn = now;
  }
  keepDraft(key, kept, CONNECT_FIELDS);
  return minted;
}

function itemsOf(collection) {
  if (Array.isArray(collection)) {
    return collection;
  }
  const items = (collection || {}).items;
  return Array.isArray(items) ? items : [];
}

function statusOf(object) {
  return (object && object.status) || {};
}

/** One condition out of a list, by type. */
export function conditionOf(conditions, type) {
  for (const condition of Array.isArray(conditions) ? conditions : []) {
    if ((condition || {}).type === type) {
      return condition;
    }
  }
  return null;
}

/** Whether this catalog's Kubernetes view is one the page can list from.
 *
 *  READ FROM THE CONDITIONS THE CONTROLLER WROTE, and never from a comparison
 *  between `viewExpiresAt` and the browser's clock: the expiry is a server
 *  instant and a browser that is five minutes fast would declare a healthy
 *  view expired. `Ready=False/ViewExpired` and `Stale=True` are the
 *  controller's own words and they are what this reads. */
export function viewIsUsable(object) {
  const ready = conditionOf(statusOf(object).conditions, "Ready");
  if (ready === null) {
    return false;
  }
  return String(ready.status) === "True";
}

/** The catalogs table. */
export function renderCatalogList(collection, ns) {
  const rows = itemsOf(collection).map((object) => {
    const meta = object.metadata || {};
    const status = statusOf(object);
    const counts = status.counts || {};
    const ready = conditionOf(status.conditions, "Ready");
    return [
      typeof meta.name === "string" && meta.name.length > 0
        ? detailLink("catalog", ns || meta.namespace || "", meta.name)
        : cell(null),
      cell(((object.spec || {}).destinationRef || {}).name),
      // THE WORD, NOT THE SYNTAX (MCP-22): `Ready=True ViewReady` was the cell.
      conditionBadge(ready, "ready", "not ready"),
      cell(counts.total),
      cell(counts.available),
      cell(counts.untrustedSigner),
      when(status.syncedAt),
      when(status.viewExpiresAt),
    ];
  });
  return (
    "<h2>Recovery catalog</h2>" +
    "<p class=\"blurb\">Every RecoveryCatalog in this namespace: the durable inventory of " +
    "recovery points in an archive, as this cluster last managed to read it.</p>" +
    "<p class=\"note\">" + esc(CATALOG_WINDOW_SENTENCE) + "</p>" +
    table(
      ["NAME", "DESTINATION", "READY", "POINTS", "AVAILABLE", "UNTRUSTED SIGNER", "SYNCED", "VIEW EXPIRES"],
      rows,
      NO_CATALOG_SENTENCE,
      undefined,
      { id: "catalogs", label: "catalogs" },
    ) +
    listFooter()
  );
}

/** The catalog's own state: what the last sync saw, what it could not read,
 *  and whether the view is still usable. */
export function renderCatalogStatus(object) {
  const status = statusOf(object);
  const counts = status.counts || {};
  const cursor = status.cursor || {};
  const job = status.lastSyncJob || {};
  const usable = viewIsUsable(object);
  return (
    "<section class=\"catalog-status\"><h3>The view</h3>" +
    (usable ? "" : "<p class=\"complaint\">" + esc(VIEW_EXPIRED_SENTENCE) + "</p>") +
    facts([
      ["synced at", when(status.syncedAt)],
      ["view expires at", when(status.viewExpiresAt)],
      ["points materialised in this view", cell(status.viewPoints)],
      ["truncated", flagBadge(status.truncated, "truncated: the archive holds more", "not truncated")],
      ["view expired", cell(status.viewExpired)],
      ["walk complete", flagBadge(cursor.complete, "complete", "not complete")],
      ["index shard reached", cell(cursor.indexShard)],
      ["last sync job", cell(job.name) + " exit " + cell(job.exitCode) + " " +
        cell(job.refusalReason)],
    ]) +
    table(
      ["TOTAL", "AVAILABLE", "MISSING", "UNREADABLE", "UNVERIFIED", "UNTRUSTED", "INVALID",
        "CONFLICT", "DELETED", "UNSUPPORTED"],
      [[
        cell(counts.total), cell(counts.available), cell(counts.missing), cell(counts.unreadable),
        cell(counts.unverified), cell(counts.untrustedSigner), cell(counts.invalid),
        cell(counts.conflict), cell(counts.deleted), cell(counts.unsupportedFormat),
      ]],
      "This catalog has recorded no counts.",
    ) +
    (status.truncated === true
      ? "<p class=\"note\">" + esc(CATALOG_WINDOW_SENTENCE) + "</p>"
      : "") +
    "</section>"
  );
}

/** THE ROUTE A "Restore this point" LINK CARRIES: the point's identity, the
 *  catalog that holds it, and THE PLAN BINDING.
 *
 *  D3 section 5.5 step 4's plan carries `source.point {point_id, receipt_key,
 *  receipt_sha256, manifest_sha256}`, and the runner re-checks that binding
 *  before it constructs a client -- a mismatch is exit 3
 *  `PointBindingMismatch`. Those four are REQUIRED fields of a published point
 *  (three of them; the manifest digest is optional and travels when it is
 *  there), so the link can carry what a plan is built from rather than a name
 *  and a hope.
 *
 *  IT CARRIES NO `locationDigest`, and this is the reconciliation's one
 *  surrendered field. The catalog's view entry has never held one -- the
 *  frozen destination digest is a fact about a BACKUP's own destination
 *  snapshot, not about a point read out of a bucket -- so the API publishes
 *  none and this page invents none. What names the location instead is the
 *  CATALOG's `destinationRef` (one destination per catalog, immutable by CEL)
 *  plus the `locationId` of the location that can serve the point. */
// THE WIZARD READS ONLY `catalog` AND `point` FROM THIS LINK (PLAT-15.2): it
// re-reads the point from the product API and builds the binding from that
// answer. The richer spelling below is kept for links minted before the wizard
// could restore a catalog point; the table now links through
// `restoreCatalogPointRoute`, the wizard's own helper.
export function restorePointRoute(ns, catalog, entry, destination) {
  const e = entry || {};
  const location = bestLocation(e);
  const at = (name, value) =>
    (typeof value === "string" && value.length > 0
      ? "&" + name + "=" + encodeURIComponent(value)
      : "");
  return (
    "#/restore?ns=" + encodeURIComponent(String(ns || "")) +
    "&catalog=" + encodeURIComponent(String(catalog || "")) +
    "&point=" + encodeURIComponent(String(e.pointId || "")) +
    (isRedacted(e.receiptKey) ? "" : at("receiptKey", e.receiptKey)) +
    at("receiptSha256", e.receiptSha256) +
    at("manifestSha256", e.manifestSha256) +
    at("destination", typeof destination === "string" ? destination : "") +
    (location === null ? "" : at("location", String(location.locationId || "")))
  );
}

// THE REDACTION MARKER. A REDACTED KEY IS NOT A KEY, AND THIS PAGE WILL NOT
// PASS ONE ON. The catalog sync runs every archive key it records through
// `logweir::check::redact_path`, and a runner that predates the ULID exemption
// (CATALOG-RECEIPTKEY-REDACTED) rewrote the 26-character run id, so a point
// comes back with `receiptKey: "[redacted].receipt.json"`. Carrying that into
// a plan would build `source.point.receipt_key` out of the redactor's output
// and earn exit 3 `PointBindingMismatch` from the runner, one step later and
// one layer further from the cause. The rule lives beside the wizard's point
// selection (PLAT-15.2), which refuses such a point too, and is re-exported
// here so this page and the wizard cannot disagree about it.
export { REDACTION_MARKER, isRedacted };

/** The location a restore would read from: the first one the catalog reports
 *  `Available`, or the first one there is.
 *
 *  A CHOICE BETWEEN RECORDED FACTS, NOT A VERDICT. The entry's own
 *  `availability` is already the best of its locations (D3 section 5.4, amended);
 *  this only picks WHICH location to name in the link. */
export function bestLocation(entry) {
  const locations = Array.isArray((entry || {}).locations) ? entry.locations : [];
  for (const location of locations) {
    if ((location || {}).availability === "Available") {
      return location;
    }
  }
  return locations.length === 0 ? null : locations[0];
}

/** What the restore link carries -- WHAT THE POINT ROUTE DELIVERS, and no more.
 *
 *  The first spelling of this sentence promised "the whole plan binding". It
 *  is not this page's to promise: the link carries exactly the fields
 *  `GET .../catalogs/{name}/points` published for that point, and whether
 *  those are enough to build a plan is a fact about the API's answer and not
 *  about the link. See [`POINT_BINDING_REDACTED_SENTENCE`] for the case where
 *  they are not. */
export const POINT_BINDING_SENTENCE =
  "This link carries what the point route published for this point: the point id, the receipt " +
  "key and digest, the manifest digest where the catalog has one, and the destination the " +
  "catalog reads. Building the plan around `source.point {point_id, receipt_key, " +
  "receipt_sha256, manifest_sha256}` is the restore wizard's own step, and the " +
  "runner re-checks that binding before it constructs a client: a mismatch is a refusal, not a " +
  "restore of something else.";

/** ONE POINT AS A ROW: the two axes, the signer, and the remedy.
 *
 *  THE RESTORE CELL IS THE WIZARD'S OWN QUESTION (PLAT-15.2). A row the catalog
 *  marks `selectable` is offered only when `catalogPointOffer` -- the rule the
 *  wizard applies when it opens -- offers it too: a complete Backup-verdict
 *  join, an unredacted receipt key and both digests. A selectable row it
 *  refuses says why in the cell, so the table never offers a link the wizard
 *  will refuse. The controller's verdict on the point's own `Backup`
 *  (`backupVerdict`) is printed beside the catalog's verification, because it
 *  is the reason a green-looking row can be unselectable. */
export function pointRow(entry, ns, catalog, destination, page) {
  const e = entry || {};
  const location = bestLocation(e);
  const offer = catalogPointOffer(e, page || {});
  return [
    // THE ACTION FIRST (MCP-25's rule, the review's class sweep): the eighth of
    // eight columns is the one a narrow window scrolls away.
    e.selectable !== true
      ? ABSENT
      : (offer.offer
        ? "<a class=\"action\" href=\"" + esc(restoreCatalogPointRoute(ns, catalog, e.pointId)) +
          "\" data-restore-point=\"" + esc(e.pointId) + "\">Restore this point</a>"
        : "<span class=\"note\" data-restore-refused=\"wizard\">not offered: " +
          esc(offer.reason) + "</span>"),
    "<code>" + cell(e.pointId) + "</code>",
    when(e.recoveryPointAt),
    badge(e.availability === "Available" ? "green" : "unverified", String(e.availability || "")),
    badge(
      e.verification === "Verified" || e.verification === "VerifiedHistorical"
        ? "green"
        : "unverified",
      String(e.verification || ""),
    ) +
      (typeof e.backupVerdict === "string" && e.backupVerdict.length > 0
        ? " " + badge("unverified", "Backup verdict " + e.backupVerdict)
        : ""),
    location === null ? ABSENT : cell(location.locationId),
    "<code>" + cell(e.signerKeyId) + "</code>",
    cell(e.remedy),
  ];
}

/** Said above the point table when the product API could not read every
 *  `Backup` verdict for this page (`backupVerdictsIncomplete`). */
export const BACKUP_VERDICTS_INCOMPLETE_SENTENCE =
  "The product API could not read every Backup verdict in this namespace for this page, so a " +
  "row's selectable reflects the catalog alone for the Backups it could not read. No restore is " +
  "offered from this page while that is so: a catalog row never outranks a verdict the " +
  "controller reached, and one nobody could read might be such a verdict.";

/** The point table. Every entry, with its exact state. */
export function renderPoints(page, ns, catalog, destination) {
  const entries = itemsOf(page);
  return (
    "<section class=\"points\"><h3>Recovery points</h3>" +
    "<p class=\"note\">" + esc(TWO_AXES_SENTENCE) + "</p>" +
    table(
      ["RESTORE", "POINT", "RECOVERY POINT", "AVAILABILITY", "VERIFICATION", "LOCATION", "SIGNER",
        "REMEDY"],
      entries.map((entry) => pointRow(entry, ns, catalog, destination, page)),
      NO_POINT_SENTENCE,
      undefined,
      { id: "catalog-points", label: "recovery points",
        scope: String(ns) + "/" + String(((catalog || {}).metadata || {}).name || (catalog || {}).name || "") },
    ) +
    (typeof (page || {}).backupVerdictsIncomplete === "string" &&
      page.backupVerdictsIncomplete.length > 0
      ? "<p class=\"complaint\" data-backup-verdicts-incomplete=\"" +
        esc(page.backupVerdictsIncomplete) + "\">" + esc(BACKUP_VERDICTS_INCOMPLETE_SENTENCE) +
        " (" + esc(page.backupVerdictsIncomplete) + ")</p>"
      : "") +
    // ONE HTTP PAGE, SAID AS ONE. `CATALOG_WINDOW_SENTENCE` above is about the
    // Kubernetes VIEW's own `viewLimit`; this is the separate truncation of
    // this REQUEST, and reading the two as one would make a 200-row table look
    // like the whole window (review F9). This build follows no cursor here.
    (cursorOf(page) === null
      ? ""
      : "<p class=\"note\" data-more-points=\"true\">" + esc(MORE_POINTS_SENTENCE) + "</p>") +
    "<p class=\"note\">" + esc(POINT_BINDING_SENTENCE) + "</p>" +
    (entries.some((entry) => isRedacted((entry || {}).receiptKey))
      ? "<p class=\"complaint\" data-redacted-binding=\"true\">" +
        esc(POINT_BINDING_REDACTED_SENTENCE) + "</p>"
      : "") +
    "</section>"
  );
}

/** What a row says when the key the plan needs came back redacted.
 *
 *  A COMPLAINT, NOT A NOTE, and it names the field and the repair. The
 *  alternative was to carry the redactor's output into the link and let the
 *  runner refuse the plan with exit 3 `PointBindingMismatch` -- a correct
 *  refusal, one step later, about a value nothing on screen said was wrong. */
export const POINT_BINDING_REDACTED_SENTENCE =
  "At least one point above published its receipt key as `[redacted]`: the product's own " +
  "archive-key redactor rewrote it on the way into the catalog view, so what the API serves is not the key " +
  "in the bucket. The restore link for that point omits `receiptKey` rather than carrying the " +
  "redactor's output, and the plan cannot be completed from this page until the catalog " +
  "publishes the key itself. The point id, the receipt digest and the manifest digest are " +
  "unaffected; nothing in your archive is missing or unreadable because of this."

/** The next cursor this page did NOT follow, or `null`. */
export function cursorOf(page) {
  const next = ((page || {}).page || {}).nextCursor;
  return typeof next === "string" && next.length > 0 ? next : null;
}

/** What a table that stopped at one page says about itself. */
export const MORE_POINTS_SENTENCE =
  "This table is ONE page of this catalog's view and the view holds more. That is a different " +
  "truncation from the window above: this one is the size of this request, and this build does " +
  "not follow the cursor here. Use `logweir catalog list` against the archive for the rest.";

/** THE UNTRUSTED-SIGNER PANEL.
 *
 *  It shows the key id, how many points it signed, and the command that turns
 *  a public key its holder gives you into that same number. It offers NO
 *  control that adds the key anywhere, and the `kubectl` snippet below is
 *  rendered and never submitted -- the same shape `ui/pages/keys.js` has for
 *  the roster, for the same reason. */
export function renderSigners(signers) {
  const list = Array.isArray((signers || {}).items)
    ? signers.items
    : (Array.isArray(signers) ? signers : []);
  const untrusted = list.filter((s) => (s || {}).trusted !== true);
  const rows = list.map((s) => {
    const signer = s || {};
    return [
      "<code>" + cell(signer.keyId) + "</code>",
      cell(signer.principalHint),
      cell(signer.points),
      signer.trusted === true
        ? badge("green", "listed in this namespace's TrustPolicy")
        : badge("unverified", "not listed in this namespace's TrustPolicy"),
    ];
  });
  return (
    "<section class=\"signers\"><h3>Who signed these points</h3>" +
    table(
      ["KEY ID", "PRINCIPAL", "POINTS", "TRUST"],
      rows,
      "This catalog recorded no signer. A view with no signer row is a view that verified " +
        "nothing, which is not the same as an archive nobody signed.",
    ) +
    (untrusted.length === 0
      ? ""
      : "<p class=\"complaint\" data-untrusted-signers=\"" + String(untrusted.length) + "\">" +
        esc(NO_ONE_CLICK_TRUST_SENTENCE) + "</p>" +
        "<p class=\"note\">Run this against the PUBLIC half the key's holder gives you and " +
        "compare the digest with the KEY ID column above.</p>" +
        // THE API SENDS THE COMMAND AND THE PAGE PREFERS IT. One string, one
        // place: a page that printed its own copy could drift from the digest
        // the installation actually computes a key id with.
        copyBlock([typeof (signers || {}).fingerprintCommand === "string" &&
          signers.fingerprintCommand.length > 0
          ? signers.fingerprintCommand
          : FINGERPRINT_COMMAND]) +
        renderTrustSnippet(untrusted)) +
    "</section>"
  );
}

/** The document a trust administrator applies. RENDERED AND NEVER SUBMITTED.
 *
 *  `state: Retired` is the value this snippet suggests, deliberately. An
 *  imported archive's signer signed in the past; `Retired` verifies everything
 *  it already signed (D3 section 7.4's `Historical` basis) and authorises nothing
 *  new, which is exactly the authority an adopted archive's key needs. */
export function renderTrustSnippet(signers) {
  const lines = [
    "# Add the key to the TrustPolicy that governs this namespace.",
    "# `state: Retired` verifies what it already signed and authorises nothing new.",
    "apiVersion: logweir.dev/v1alpha1",
    "kind: TrustPolicy",
    "metadata:",
    "  name: <the policy that names this namespace>",
    "spec:",
    "  keys:",
  ];
  for (const s of signers) {
    lines.push("    - keyId: " + String((s || {}).keyId || "<the key id you compared>"));
    lines.push("      spkiPem: |");
    lines.push("        <the PUBLIC half, after you compared its fingerprint out of band>");
    lines.push("      algorithm: p256");
    lines.push("      usages: [EvidenceSigning]");
    lines.push("      principal: {id: \"install:<who holds it>\"}");
    lines.push("      state: Retired");
  }
  lines.push("");
  lines.push("kubectl --context <ctx> apply -f trustpolicy.yml");
  return (
    "<section class=\"check\"><h3>Trusting this key is a cluster-admin step</h3>" +
    "<p class=\"note\">This page shows the document and does not apply it. `trustpolicies` is " +
    "cluster-scoped, no page in this tree may write it, and the product API serves no trust " +
    "write at all in v1.</p>" +
    copyBlock(lines) +
    "</section>"
  );
}

/** What a spent key answers, and what clears it.
 *
 *  It is rendered rather than pre-empted because the escape is already in the
 *  form: correcting any field moves the body, and a moved body mints a new
 *  intent on the next submit. The sentence says so, so an operator who sees a
 *  409 knows the answer is "submit again", not "reload the page". */
export const INTENT_USED_SENTENCE =
  "The idempotency intent this form was holding was already spent on a DIFFERENT request, so " +
  "the API refused rather than guessing which of the two you meant. The submission below mints " +
  "a new intent because its body has changed; submit it again.";

/** Whether a refusal is the product API's spent-key answer. */
export function isIntentConflict(error) {
  const code = (error || {}).reason;
  return code === "idempotency_conflict" || code === "idempotency_key_invalid";
}

/** THE CONNECT-ARCHIVE FORM. One durable submission, one intent. */
export function renderConnectForm(view) {
  const v = view || {};
  const values = v.values || {};
  // ONE ERROR SHAPE, THE ONE EVERY OTHER FORM IN THIS TREE USES:
  // `{fields, unmatched}` -- `fields` keyed by this form's own input names, and
  // `unmatched` for a server cause no input on screen is about, which is shown
  // beside the outcome rather than dropped.
  const errors = ((v.errors || {}).fields) || {};
  const unmatched = (v.errors || {}).unmatched;
  const state = v.state || { phase: "idle" };
  const pending = state.phase === "pending";
  return (
    "<section class=\"connect\"><h3>Connect an existing archive</h3>" +
    "<p class=\"note\">" + esc(CONNECT_SENTENCE) + "</p>" +
    "<form data-connect-archive=\"" + esc(String(v.ns || "")) + "\">" +
    "<div class=\"field\"><label for=\"catalog-name\">catalog name</label>" +
    "<input id=\"catalog-name\" name=\"name\" value=\"" + esc(values.name || "") + "\"" +
    invalidAttributes("catalog-name", errors.name) + " required></div>" +
    fieldErrorLine("catalog-name", errors.name) +
    renderDestinationChoice(v, values, errors) +
    fieldErrorLine("catalog-destination", errors.destination) +
    "<p class=\"help\">The saved destination whose bucket holds the archive. Its credential is " +
    "the one the sync Job uses, and a read-only one is enough.</p>" +
    "<div class=\"field\"><label for=\"catalog-mode\">sync mode</label>" +
    "<select id=\"catalog-mode\" name=\"syncMode\">" +
    SYNC_MODES.map((m) =>
      "<option value=\"" + esc(m) + "\"" +
      ((values.syncMode || SYNC_MODES[0]) === m ? " selected" : "") + ">" + esc(m) + "</option>"
    ).join("") +
    "</select></div>" +
    "<p class=\"help\" id=\"catalog-mode-help\">" + esc(CATALOG_MODE_HELP) + "</p>" +
    "<div class=\"actions\"><button type=\"submit\"" + (pending ? " disabled" : "") +
    ">Connect archive</button></div>" +
    "<div class=\"form-status\" data-connect-status=\"true\" tabindex=\"-1\">" +
    mutationStatus(state, { kind: "RecoveryCatalog", name: values.name || "", idempotencyKey: true },
      unmatched) +
    (isIntentConflict(state.error)
      ? "<p class=\"complaint\" data-intent-used=\"true\">" +
        badge("unverified", "intent already used") + " " + esc(INTENT_USED_SENTENCE) + "</p>"
      : "") +
    "</div>" +
    "</form>" +
    (v.result ? renderConnectResult(v.ns, v.result) : "") +
    "</section>"
  );
}

/** THE DESTINATION, CHOSEN FROM THE NAMESPACE'S OWN (MCP-23). A pick-list of
 *  the saved destinations the page read -- name and location -- rather than a
 *  free-text box a typo turns into a 422; the free-text box stays when the
 *  list could not be read, so the form never stops working because a read
 *  failed. An empty list says where a destination is made. */
export function renderDestinationChoice(view, values, errors) {
  const v = view || {};
  const chosen = String((values || {}).destination || "");
  const invalid = invalidAttributes("catalog-destination", (errors || {}).destination);
  const list = Array.isArray(v.destinations) ? v.destinations : null;
  if (list !== null && list.length > 0) {
    const known = list.some((d) => d.name === chosen);
    return (
      "<div class=\"field\"><label for=\"catalog-destination\">destination</label>" +
      "<select id=\"catalog-destination\" name=\"destination\"" + invalid + " required>" +
      "<option value=\"\"" + (chosen.length === 0 ? " selected" : "") +
      ">Choose a saved destination</option>" +
      list.map((d) =>
        "<option value=\"" + esc(d.name) + "\"" + (d.name === chosen ? " selected" : "") + ">" +
        esc(d.name) + (typeof d.canonicalUrl === "string" ? " -- " + esc(d.canonicalUrl) : "") +
        (d.default === true ? " (default)" : "") + "</option>").join("") +
      (chosen.length > 0 && !known
        ? "<option value=\"" + esc(chosen) + "\" selected>" + esc(chosen) +
          " (not in this namespace's list)</option>"
        : "") +
      "</select></div>"
    );
  }
  return (
    "<div class=\"field\"><label for=\"catalog-destination\">destination</label>" +
    "<input id=\"catalog-destination\" name=\"destination\" value=\"" + esc(chosen) + "\"" +
    invalid + " required></div>" +
    (list !== null && list.length === 0
      ? "<p class=\"note\" id=\"catalog-no-destination\">No saved destination exists in this " +
        "namespace yet. <a href=\"#/destinations?ns=" + esc(encodeURIComponent(String(v.ns || ""))) +
        "\">Create one on Destinations</a> first; it names the bucket this archive is in.</p>"
      : "")
  );
}

/** What the create produced, with the replay case named. */
export function renderConnectResult(ns, result) {
  const made = result || {};
  const meta = made.metadata || {};
  return (
    "<p class=\"connect-result\" data-replayed=\"" +
    (made.__replayed === true ? "true" : "false") + "\">" +
    (made.__replayed === true
      ? badge("pending", "already connected") +
        " That click had already created this catalog, so the API answered with the one it made " +
        "the first time rather than creating a second. "
      : badge("green", "connected") + " ") +
    detailLink("catalog", String(ns || ""), String(meta.name || "")) +
    " Syncing is the controller's; this page does not wait for it.</p>"
  );
}

/** One catalog, in full. */
export function renderCatalogDetail(view) {
  const v = view || {};
  const object = v.object || {};
  const meta = object.metadata || {};
  const spec = object.spec || {};
  const sync = spec.sync || {};
  return (
    "<h2>Catalog " + cell(meta.name) + "</h2>" +
    facts([
      ["destination", cell((spec.destinationRef || {}).name)],
      ["sync mode", cell(sync.mode)],
      ["sync interval (s)", cell(sync.intervalSeconds)],
      ["deep check", cell(sync.deepCheck)],
      ["view limit", cell(sync.viewLimit)],
    ]) +
    renderCatalogStatus(object) +
    // A REFUSED SUB-READ IS RENDERED IN THE SECTION IT BELONGS TO, as a
    // string, because these renderers are pure functions from JSON to HTML.
    // It is never absorbed into an empty table: an empty table is what an
    // empty catalog looks like, and the two could not be more different.
    (v.pointsError
      ? "<section class=\"points\"><h3>Recovery points</h3>" +
        errorBlock(v.pointsError, false) + "</section>"
      : renderPoints(v.points, v.ns, meta.name, (spec.destinationRef || {}).name)) +
    (v.signersError
      ? "<section class=\"signers\"><h3>Who signed these points</h3>" +
        errorBlock(v.signersError, false) + "</section>"
      : renderSigners(v.signers))
  );
}

/** The sentence the page carries while its list has not answered yet. An
 *  empty table before the read would say "there is no catalog in this
 *  namespace", which is a different fact from "this has not been read". */
export const READING_SENTENCE = "Reading the recovery catalogs in this namespace...";

/** The whole list page, with the connect form under it. */
export function renderCatalogPage(view) {
  const v = view || {};
  if (v.loaded !== true && (v.collection === null || v.collection === undefined)) {
    return "<h2>Recovery catalog</h2>" +
      "<p class=\"pending\" role=\"status\">" + esc(READING_SENTENCE) + "</p>" +
      renderConnectForm(v);
  }
  return renderCatalogList(v.collection, v.ns) + renderConnectForm(v);
}

// --------------------------------------------------------------- mount half

/** The product API's own field paths mapped onto this form's input names, so a
 *  422 lands beside the control it is about. A LIST OF PAIRS, which is what
 *  `lifecycle.js`'s `fieldForPath` walks; a field name nothing here matches
 *  travels in `unmatched` and is shown beside the outcome. */
export const CATALOG_FIELD_PATHS = Object.freeze([
  ["name", "name"],
  ["destinationRef", "destination"],
  ["destinationRef.name", "destination"],
  ["legacyArchive", "destination"],
  ["syncMode", "syncMode"],
]);

/** What this page checks before it sends, so a refusal lands beside its
 *  field rather than arriving as a 422 the form cannot place. */
export function validateConnect(values) {
  const errors = {};
  const name = String((values || {}).name || "").trim();
  const destination = String((values || {}).destination || "").trim();
  if (name.length === 0) {
    errors.name = ["a catalog needs a name; it is also what makes this submission repeatable"];
  }
  if (destination.length === 0) {
    errors.destination = ["name the saved destination whose bucket holds the archive"];
  }
  return errors;
}

/** The body this form sends. */
export function connectBody(values) {
  return {
    name: String((values || {}).name || "").trim(),
    destinationRef: { name: String((values || {}).destination || "").trim() },
    syncMode: String((values || {}).syncMode || SYNC_MODES[0]),
  };
}

// The destinations the connect form offers: `deps.destinations` for the
// suite, the client's product-API read otherwise; `null` when unreadable.
async function readConnectDestinations(ns, deps, lifecycle) {
  const reader = deps !== undefined && deps !== null
    ? deps.destinations
    : (namespace, options) => API.destinations(namespace, options);
  if (typeof reader !== "function") {
    return null;
  }
  try {
    const answer = await reader(ns, readOptions(lifecycle));
    return Array.isArray((answer || {}).items) ? answer.items : null;
  } catch (unread) {
    return null;
  }
}

export async function mountCatalog(node, ns, parse, lifecycle, deps) {
  const key = connectKey(ns);
  const mutation = mutationFor(key);
  const view = {
    ns: ns, collection: null, loaded: false,
    values: readDraft(key) || { syncMode: SYNC_MODES[0] },
    errors: { fields: {}, unmatched: [] }, state: mutation.state, result: null,
  };
  const paint = () => {
    if (!active(lifecycle)) {
      return;
    }
    replace(node, parse(renderCatalogPage(view)));
    wire(node, ns, key, mutation, view, paint, deps, lifecycle);
  };
  watchMutation(node, key, mutation, (state) => {
    view.state = state;
    paint();
  }, lifecycle);
  // THE NAMESPACE'S DESTINATIONS, READ BESIDE THE CATALOGS for the pick-list
  // (MCP-23). A read that fails leaves the free-text box; it is never a reason
  // the page fails.
  const destinationsRead = readConnectDestinations(ns, deps, lifecycle);
  try {
    view.collection = await listD3("catalog", ns, readOptions(lifecycle), deps);
    view.destinations = await destinationsRead;
    view.loaded = true;
    paint();
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, parse(errorBlock(error) + renderConnectForm(view)));
      wire(node, ns, key, mutation, view, paint, deps, lifecycle);
    }
  }
}

// The form's listeners, re-attached on every paint because the paint replaces
// the nodes they were on. `watchMutation` replaces this owner's previous
// subscription, so no listener outlives the view it belongs to.
function wire(node, ns, key, mutation, view, paint, deps, lifecycle) {
  const form = node.querySelector === undefined
    ? null
    : node.querySelector("form[data-connect-archive]");
  if (form === null) {
    return;
  }
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const values = {
      name: String((form.elements.name || {}).value || "").trim(),
      destination: String((form.elements.destination || {}).value || "").trim(),
      syncMode: String((form.elements.syncMode || {}).value || SYNC_MODES[0]),
    };
    view.values = values;
    const body = connectBody(values);
    // THE INTENT IS READ BEFORE THE DRAFT IS REWRITTEN, AND IT IS WRITTEN BACK
    // WITH IT. `keepDraft` is a REPLACE over the declared fields, not a merge:
    // keeping `values` alone would drop the intent this draft holds, and the
    // next submission would mint a new one -- so a resend of the SAME draft
    // would stop being the same request, which is the whole property an
    // idempotency key exists to provide.
    //
    // AND IT IS MINTED FOR THIS BODY. A corrected destination is a DIFFERENT
    // request, and spending the old key on it is a 409 no retry can clear.
    const intent = connectIntent(key, body);
    keepDraft(
      key,
      Object.assign({}, values, { intent: intent, spentOn: spentOn(body) }),
      CONNECT_FIELDS,
    );
    const problems = validateConnect(values);
    if (Object.keys(problems).length > 0) {
      view.errors = { fields: problems, unmatched: [] };
      paint();
      return;
    }
    view.errors = { fields: {}, unmatched: [] };
    if (mutation.pending()) {
      return;
    }
    mutation.run(async () => {
      try {
        const made = await connectArchive(ns, body, intent, deps);
        view.result = made;
        view.errors = { fields: {}, unmatched: [] };
        // A DURABLE RESULT ENDS THE DRAFT AND EMPTIES THE FORM (PLAT-13.2, and
        // d1w7's review F1 in a new form). The catalog EXISTS now: leaving the
        // name and the destination in the inputs invites a second click that
        // creates a SECOND RecoveryCatalog for one destination -- which D3
        // section 5.5 refuses with `Ready=False/DuplicateCatalog`, leaving a
        // dead object in a namespace this console holds no delete for. The
        // outcome line above says what was made and links to it; the form
        // below it is empty and ready for a different archive.
        dropDraft(key);
        view.values = { syncMode: SYNC_MODES[0] };
        // AND THE LIST IS RE-READ, so the catalog that now exists is on screen
        // rather than a table built before it did.
        try {
          view.collection = await listD3("catalog", ns, readOptions(lifecycle), deps);
          view.loaded = true;
        } catch (stale) {
          // A refused re-list does not un-make the catalog. The outcome line
          // is the durable fact; the table catches up on the next visit.
          view.listError = stale;
        }
        return made;
      } catch (error) {
        view.errors = fieldErrors(error, CATALOG_FIELD_PATHS);
        throw error;
      }
    });
  }, lifecycleSignal(lifecycle));
}

function lifecycleSignal(lifecycle) {
  if (lifecycle === undefined || lifecycle === null || lifecycle.signal === undefined) {
    return undefined;
  }
  return { signal: lifecycle.signal };
}

export async function mountCatalogDetail(node, ns, name, parse, lifecycle, deps) {
  const view = {
    ns: ns, object: null, points: null, signers: null,
    pointsError: null, signersError: null,
  };
  try {
    view.object = await readD3("catalog", ns, name, readOptions(lifecycle), deps);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
    return;
  }
  // THE TWO SUB-READS ARE ALLOWED TO FAIL WITHOUT TAKING THE PAGE WITH THEM.
  // A catalog whose point list cannot be read is a catalog whose STATUS is
  // still worth showing -- the counts, the conditions and the sync job are
  // exactly what says why the list is not there. The refusal is rendered in
  // the section it belongs to, never absorbed into an empty table.
  try {
    view.points = await readCatalogPoints(ns, name, { limit: 200 }, readOptions(lifecycle), deps);
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      return;
    }
    view.pointsError = error;
  }
  try {
    view.signers = await readCatalogSigners(ns, name, readOptions(lifecycle), deps);
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      return;
    }
    view.signersError = error;
  }
  if (active(lifecycle)) {
    replace(node, parse(renderCatalogDetail(view)));
  }
}
