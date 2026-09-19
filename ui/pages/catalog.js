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
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  listFooter,
  mutationStatus,
  replace,
  rfc3339,
  table,
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

/** The sentence a namespace with no catalog carries. */
export const NO_CATALOG_SENTENCE =
  "No RecoveryCatalog exists in this namespace. Without one, the points in your archive are " +
  "still there and nothing in Kubernetes can list them: create one below to connect a " +
  "destination you already hold a read-only credential for.";

/** The sentence the point table carries when the window holds no entry. */
export const NO_POINT_SENTENCE =
  "This catalog's view holds no point. That is a statement about the VIEW: check the sync " +
  "conditions above before concluding anything about the archive.";

/** The two sync modes the connect form offers, and what each one costs.
 *  `Full` is what "connect an existing archive" needs: it walks receipts and
 *  manifests rather than only the catalog index a Logweir writer would have
 *  left behind. */
export const SYNC_MODES = Object.freeze(["Full", "Index"]);

/** What connecting an archive does, and what it does not. */
export const CONNECT_SENTENCE =
  "Connecting an archive creates a RecoveryCatalog: a read-only view over a destination you " +
  "already hold a credential for. It reads; it writes nothing to your bucket, moves nothing and " +
  "deletes nothing. Points it finds signed by a key this installation does not list come back " +
  "as untrusted, and no step of this form changes that.";

/** The draft this form keeps, and the intent that makes its submit durable. */
export const CONNECT_FIELDS = Object.freeze(["name", "destination", "syncMode", "intent"]);

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

/** The intent this draft holds, minted on first use and kept for its life. */
export function connectIntent(key) {
  const draft = readDraft(key);
  const held = draft === null ? undefined : draft.intent;
  if (typeof held === "string" && held.length >= 8) {
    return held;
  }
  const minted = mintConnectIntent();
  keepDraft(key, Object.assign({}, draft || {}, { intent: minted }), CONNECT_FIELDS);
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
      ready === null ? ABSENT : badge(
        String(ready.status) === "True" ? "green" : "unverified",
        "Ready=" + String(ready.status) + " " + String(ready.reason || ""),
      ),
      cell(counts.total),
      cell(counts.available),
      cell(counts.untrustedSigner),
      cell(status.syncedAt),
      cell(status.viewExpiresAt),
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
      ["synced at", cell(status.syncedAt)],
      ["view expires at", cell(status.viewExpiresAt)],
      ["truncated", cell(status.truncated)],
      ["walk complete", cell(cursor.complete)],
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

/** THE ROUTE A "Restore this point" LINK CARRIES.
 *
 *  The point's own identity, the catalog that holds it, and THE FROZEN
 *  LOCATION -- `locationDigest` when the entry publishes one, and the
 *  `locationId` of the location that can serve it otherwise. The digest is
 *  what binds a restore to the exact destination snapshot the point was
 *  written under, which is the check D2 asks a restore's source step to make
 *  and the one a bare bucket URL cannot support. */
export function restorePointRoute(ns, catalog, entry) {
  const e = entry || {};
  const location = bestLocation(e);
  return (
    "#/restore?ns=" + encodeURIComponent(String(ns || "")) +
    "&catalog=" + encodeURIComponent(String(catalog || "")) +
    "&point=" + encodeURIComponent(String(e.pointId || "")) +
    (typeof e.locationDigest === "string" && e.locationDigest.length > 0
      ? "&locationDigest=" + encodeURIComponent(e.locationDigest)
      : "") +
    (location === null ? "" : "&location=" + encodeURIComponent(String(location.locationId || "")))
  );
}

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

/** What the restore link can and cannot carry today. */
export const POINT_BINDING_SENTENCE =
  "This link carries the point id, its catalog and the frozen location the point was written " +
  "under. Binding a restore plan to that point -- `source.point {point_id, receipt_key, " +
  "receipt_sha256, manifest_sha256}` -- is the restore wizard's own step (PLAT-15.2), and the " +
  "runner re-checks the binding before it constructs a client.";

/** ONE POINT AS A ROW: the two axes, the signer, and the remedy. */
export function pointRow(entry, ns, catalog) {
  const e = entry || {};
  const location = bestLocation(e);
  return [
    "<code>" + cell(e.pointId) + "</code>",
    cell(rfc3339(e.recoveryPointAtMs)),
    badge(e.availability === "Available" ? "green" : "unverified", String(e.availability || "")),
    badge(
      e.verification === "Verified" || e.verification === "VerifiedHistorical"
        ? "green"
        : "unverified",
      String(e.verification || ""),
    ),
    location === null ? ABSENT : cell(location.locationId),
    "<code>" + cell(e.signerKeyId) + "</code>",
    cell(e.remedy),
    e.selectable === true
      ? "<a href=\"" + esc(restorePointRoute(ns, catalog, e)) + "\">Restore this point</a>"
      : ABSENT,
  ];
}

/** The point table. Every entry, with its exact state. */
export function renderPoints(page, ns, catalog) {
  const entries = itemsOf(page);
  return (
    "<section class=\"points\"><h3>Recovery points</h3>" +
    "<p class=\"note\">" + esc(TWO_AXES_SENTENCE) + "</p>" +
    table(
      ["POINT", "RECOVERY POINT", "AVAILABILITY", "VERIFICATION", "LOCATION", "SIGNER", "REMEDY",
        "RESTORE"],
      entries.map((entry) => pointRow(entry, ns, catalog)),
      NO_POINT_SENTENCE,
    ) +
    "<p class=\"note\">" + esc(POINT_BINDING_SENTENCE) + "</p>" +
    "</section>"
  );
}

/** THE UNTRUSTED-SIGNER PANEL.
 *
 *  It shows the key id, how many points it signed, and the command that turns
 *  a public key its holder gives you into that same number. It offers NO
 *  control that adds the key anywhere, and the `kubectl` snippet below is
 *  rendered and never submitted -- the same shape `ui/pages/keys.js` has for
 *  the roster, for the same reason. */
export function renderSigners(signers) {
  const list = Array.isArray((signers || {}).signers)
    ? signers.signers
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
        copyBlock([FINGERPRINT_COMMAND]) +
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

/** THE CONNECT-ARCHIVE FORM. One durable submission, one intent. */
export function renderConnectForm(view) {
  const v = view || {};
  const values = v.values || {};
  const errors = v.errors || {};
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
    "<div class=\"field\"><label for=\"catalog-destination\">destination</label>" +
    "<input id=\"catalog-destination\" name=\"destination\" value=\"" +
    esc(values.destination || "") + "\"" +
    invalidAttributes("catalog-destination", errors.destination) + " required></div>" +
    fieldErrorLine("catalog-destination", errors.destination) +
    "<p class=\"help\">The saved destination (PLAT-08.1) whose bucket holds the archive. Its " +
    "credential is the one the sync Job uses, and a read-only one is enough.</p>" +
    "<div class=\"field\"><label for=\"catalog-mode\">sync mode</label>" +
    "<select id=\"catalog-mode\" name=\"syncMode\">" +
    SYNC_MODES.map((m) =>
      "<option value=\"" + esc(m) + "\"" +
      ((values.syncMode || SYNC_MODES[0]) === m ? " selected" : "") + ">" + esc(m) + "</option>"
    ).join("") +
    "</select></div>" +
    "<p class=\"help\">Full walks the receipts and manifests in the bucket, which is what an " +
    "archive Logweir did not write needs. Index reads only the catalog shards a Logweir writer " +
    "left behind.</p>" +
    "<div class=\"actions\"><button type=\"submit\"" + (pending ? " disabled" : "") +
    ">Connect archive</button></div>" +
    "<div class=\"form-status\" data-connect-status=\"true\" tabindex=\"-1\">" +
    mutationStatus(state, { kind: "RecoveryCatalog", name: values.name || "", idempotencyKey: true },
      ((errors || {}).unmatched)) +
    "</div>" +
    "</form>" +
    (v.result ? renderConnectResult(v.ns, v.result) : "") +
    "</section>"
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
    (v.pointsError
      ? "<section class=\"points\"><h3>Recovery points</h3>" + errorBox(v.pointsError) +
        "</section>"
      : renderPoints(v.points, v.ns, meta.name)) +
    (v.signersError
      ? "<section class=\"signers\"><h3>Who signed these points</h3>" + errorBox(v.signersError) +
        "</section>"
      : renderSigners(v.signers))
  );
}

/** The whole list page, with the connect form under it. */
export function renderCatalogPage(view) {
  const v = view || {};
  return renderCatalogList(v.collection, v.ns) + renderConnectForm(v);
}

// --------------------------------------------------------------- mount half

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

export async function mountCatalog(node, ns, parse, lifecycle, deps) {
  const key = connectKey(ns);
  const mutation = mutationFor(key);
  const view = {
    ns: ns, collection: null, values: readDraft(key) || { syncMode: SYNC_MODES[0] },
    errors: {}, state: mutation.state, result: null,
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
  try {
    view.collection = await listD3("catalog", ns, readOptions(lifecycle), deps);
    paint();
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, parse(errorBox(error) + renderConnectForm(view)));
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
    keepDraft(key, values, CONNECT_FIELDS);
    view.errors = validateConnect(values);
    if (Object.keys(view.errors).length > 0) {
      paint();
      return;
    }
    if (mutation.pending()) {
      return;
    }
    mutation.run(async () => {
      try {
        const made = await connectArchive(ns, connectBody(values), connectIntent(key), deps);
        view.result = made;
        view.errors = {};
        // THE DRAFT ENDS WITH THE INTENT IT HELD. A second connect is a second
        // archive and mints its own key.
        dropDraft(key);
        return { outcome: "succeeded", object: made };
      } catch (error) {
        view.errors = fieldErrors(error, ["name", "destinationRef.name", "syncMode"]);
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
