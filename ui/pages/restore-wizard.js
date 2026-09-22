// pages/restore-wizard.js -- six steps: archive, backup set, point in time,
// target and naming, target-topic preflight, and the rendered plan with its
// hash and the two names the next two creates will use.
//
// THE BYTES ARE THE PRODUCT OF THIS PAGE. `../plan.js` renders the runner's
// own document, this page shows it, hashes it and submits it -- and between
// the `<pre>` a viewer reads and the string `create` sends there is no
// transformation of any kind. Not a parse, not a re-emit, not a normalise, not
// a trim. The sha256 on screen is the sha256 the controller recomputes only if
// those are the same bytes, and "nearly the same bytes" is a different
// document with a different hash that `logweir drill approve` would sign
// instead.
//
// TWO NAMES, MINTED BEFORE EITHER CREATE. `Restore.spec.approvalRef` and
// `Approval.spec.subjectRef` name each other and both specs are CEL-immutable
// (`self == oldSelf`, spec section 3.2), so neither reference can be filled in
// afterwards and two peer "create" buttons with no stated order cannot produce
// a working pair. So: mint both names from the plan bytes, create the
// `Restore` FIRST with `spec.approvalRef.name` set to an approval that does
// not exist yet, and let the reconciler requeue at 30 s on
// `ApprovalNotVerified` until it does (spec section 7 amendment 4; Task 20's
// requeue). The same suffix on both is what lets an operator read the pair off a
// `kubectl get` without a join.
//
// AND NO KEY IS EVER SEEN HERE. This page does not mint an approval, does not
// generate a keypair, does not upload or proxy one, and offers no "Approve"
// button that produces a signature (Global Constraint 28). It prints the exact
// command an approver runs on their own machine, and the approvals page takes
// the two documents that command wrote.
//
// EDITING IS CREATING. `Restore.spec` is immutable, so there is no in-place
// edit: "edit" prefills a NEW draft, whose bytes hash differently, and the
// page says so above the form.
//
// ONE CHOSEN RECOVERY POINT (PLAT-11.1). This page no longer picks a Backup
// for you. A recovery point is chosen -- from a history row, from a schedule
// card, or from this page's own selector -- and travels in the route as
// `#/restore?ns=<ns>&backup=<name>&uid=<uid>`. The UID is the identity and the
// name is for reading: a Backup deleted and recreated under the same name is a
// different run over a different archive, and a page that resolved by name
// would follow the new one without saying so. Because the identity is in the
// address, a newer Backup completing mid-wizard cannot move the selection: the
// list is read again and the same UID is found again. A point that has gone,
// or that is not Succeeded, is a REFUSAL naming it -- no plan, no hash, no
// submit -- because the alternative is a wizard that quietly restores from
// something else.
//
// TRANSPORT SECURITY IS NEVER DERIVED (D-SEAMS S5, defect UI-HTTPDOWNGRADE).
// Addressing style and plaintext transport are two controls and this page
// keeps them apart: `path_style` addressing says how a bucket is named in a
// URL, and it has never had the authority to turn off encryption in transit.
// The plan's insecure-transport flag is set by one explicit checkbox, which
// defaults OFF, and by nothing else -- not by the addressing style, not by the
// shape of an endpoint, not by any environment value.
//
// THE TARGET IS CHOSEN BY IDENTITY (PLAT-07.2). Step 4's select carries each
// saved connection's UID, not its name, and the draft keeps the pair. A
// KafkaCluster RENAMED between opening the wizard and submitting it keeps the
// selection -- same object, same brokers, same credential, and the plan is
// built from what it says now. One DELETED AND RECREATED under the same name is
// a REFUSAL naming both uids: it is a different set of brokers reached with a
// different credential, and writing a restore into it because the label matched
// is exactly the substitution PLAT-11.1 removed from the recovery point. Both
// wizard sides read the same probe words as the clusters page, from
// `../select.js`, so "connection probe" means the same thing in all three
// places and nothing here says "ready".
//
// ONE GUIDED SUBMIT (PLAT-12.1). "Create the Restore" is the only action, and
// it does the whole journey in order: check that the plan about to be sent is
// the plan on screen, create the Restore idempotently -- its name is minted
// from those bytes, so the same plan submitted twice, or retried after a lost
// response, resolves to the Restore the first request made -- and then open
// what it needs next. Under today's approval semantics every Restore waits for
// a verified Approval: when one already authorises this exact Restore, that is
// the Restore's operation view; otherwise it is the Restore's approval page,
// Awaiting approval. There is no second button that navigates without
// creating, and no create that forgets where it was going.

import { apiClient } from "../client.js";
import {
  active,
  cancelled,
  dropDraft,
  fieldErrors,
  formKey,
  invalidInput,
  keepDraft,
  listen,
  mutationFor,
  readDraft,
  readOptions,
  refusal,
  resolveExisting,
  watchMutation,
} from "../lifecycle.js";
import {
  COPY_CAVEAT,
  RESTORE_IMMUTABLE_SENTENCE,
  UNVERIFIED,
  badge,
  bucketOf,
  cell,
  COMPLETION_GUIDANCE,
  copyBlock,
  epochMs,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  mutationStatus,
  prefixOf,
  preflightSentence,
  replace,
  rfc3339,
  staleReasonLine,
  table,
  TARGET_MODE_MEANING,
  windowMessage,
} from "../render.js";
import { defaultTopicPrefix, TARGET_MODES, preparePlanDocument } from "../plan.js";
import {
  clusterUid,
  filterSelectorOptions,
  probeLine,
  probeState,
  readClusterSelection,
  renderClusterSelector,
  resolveClusterSelection,
} from "../select.js";
import { isObjectName, itemsOf } from "./clusters.js";
import { renderPreflight } from "./destinations.js";
import { approvalAuthorizes, restoreOperationRoute } from "./approvals.js";

const PLURAL = "restores";
const CLUSTERS = "kafkaclusters";
const BACKUPS = "backups";
const APPROVALS = "approvals";

/** The wizard's identity in the draft and mutation registries. */
export const WIZARD_FORM = "restore-wizard";

/** The wizard fields a draft keeps: the values a person can change in steps
 *  1, 3 and 4. None is a credential -- the archive credential is a Secret's
 *  NAME -- and the plan bytes themselves are never kept: they are rendered
 *  again, and hashed again, from these. */
export const WIZARD_DRAFT_FIELDS = Object.freeze([
  "backupSetRef", "pointInTime", "mode", "topicPrefix", "targetCluster", "targetClusterUid",
  "endpoint", "region", "pathStyle", "allowHttp", "evidenceBucket", "archiveSecret",
  // PLAT-11.2: which failed run this draft belongs to, so a retry never picks
  // up an ordinary restore's kept prefix (`applyWizardDraft`).
  "retryOf",
  // PLAT-11.2: the chosen subset is an edit like any other, and a draft that
  // dropped it would restore a plan over every topic the point froze -- which
  // is precisely the choice the operator made and this page lost.
  "topics",
]);

/** The API server's field paths, mapped to the wizard's inputs. `archive` and
 *  `backupSet` are not inputs: they were read from a Backup, and their
 *  messages are shown beside the submit button. */
export const WIZARD_FIELD_PATHS = Object.freeze([
  ["spec.pointInTime", "pointInTime"],
  ["spec.target.topicNaming", "topicPrefix"],
  ["spec.target.clusterRef", "targetCluster"],
  ["spec.target.mode", "mode"],
  ["spec.sourceArchive.secretRef", "archiveSecret"],
  ["spec.sourceArchive", "archive"],
  ["spec.backupSetRef", "backupSet"],
  // The product API's five mapping refusals arrive on `topicMapping[i].source`
  // or `topicMapping[i].target`; the longest-prefix match sends every one of
  // them to the subset control they are about.
  ["topicMapping", "topics"],
]);

/** The fields of `WIZARD_FIELD_PATHS` with no input of their own. */
const NOT_INPUTS = Object.freeze(["archive", "backupSet"]);

/** `Restore.spec` has no server default and no field that may change, so the
 *  comparison with an existing Restore is exact. */
const RESTORE_SPEC_RULES = Object.freeze({});

/** The evidence prefix, and it is NOT a field an operator sets. Global
 *  Constraint 6: `logweir` writes only under `logweir/`, and
 *  `Store::from_url` refuses an evidence prefix that is not exactly that --
 *  trailing slash included. A plan that named another one would be refused at
 *  phase 0 after the approver had already signed it. */
const EVIDENCE_PREFIX = "logweir/";

/** The default api surface. The mount half takes an override so the behaviour
 *  suite can hand in a stub that RECORDS every write and a stub that THROWS on
 *  one; there is no DOM and no network under `node --test`, and a page whose
 *  write half could only be exercised in a browser is a page whose write half
 *  is exercised nowhere. */
const API = apiClient();

// ------------------------------------------- the recovery point (PLAT-11.1)

/** THE ROUTE'S RECOVERY POINT: `#/restore?ns=<ns>&backup=<name>&uid=<uid>`.
 *
 *  `uid` is the IDENTITY and `backup` is the display name. A Backup's UID is
 *  the one identifier here that cannot be re-used: an object deleted and
 *  recreated under the same name is a different run over a different archive,
 *  and a wizard that resolved by name alone would follow the new one silently.
 *  The name travels beside it so a refusal can still say which point was asked
 *  for when nothing answers to the UID. A link that carries only `backup`
 *  still works -- it resolves by name and then PINS the UID it found -- which
 *  is what keeps a hand-typed or older link usable.
 *
 *  Extracted here rather than in `app.js` for `approvalRouteParams`'s reason:
 *  that module touches `window` at module scope and cannot be imported under
 *  `node --test`, so a hand-off written inline there is a hand-off no test in
 *  either language can reach -- and two values swapped in it leave the whole
 *  suite green. */
export function restoreRouteParams(hash) {
  const text = typeof hash === "string" ? hash : "";
  const route = { ns: "", uid: "", backup: "", retryOf: "" };
  const question = text.indexOf("?");
  if (question === -1) {
    return route;
  }
  for (const pair of text.slice(question + 1).split("&")) {
    const equals = pair.indexOf("=");
    if (equals === -1) {
      continue;
    }
    const key = pair.slice(0, equals);
    const value = decodeURIComponent(pair.slice(equals + 1)).trim();
    if (key === "uid") {
      route.uid = value;
    } else if (key === "backup") {
      route.backup = value;
    } else if (key === "ns" && value.length > 0) {
      route.ns = value;
    } else if (key === "retryOf") {
      // PLAT-11.2: the failed Restore this one retries. It is carried for the
      // PREFIX it derives and for the banner that names it, and for nothing
      // else -- the wizard reads no field of that object and writes to none.
      route.retryOf = value;
    }
  }
  return route;
}

/** The link that opens the wizard ON one recovery point. Both halves of the
 *  identity travel, percent-encoded, and the namespace is explicit: `default`
 *  is a real selected namespace and must cross a hand-off rather than be
 *  elided. */
export function restorePointRoute(ns, backup) {
  const meta = ((backup || {}).metadata) || {};
  const n = typeof ns === "string" ? ns.trim() : "";
  const name = typeof meta.name === "string" ? meta.name : "";
  const uid = typeof meta.uid === "string" ? meta.uid : "";
  return (
    "#/restore?" +
    (n.length > 0 ? "ns=" + encodeURIComponent(n) + "&" : "") +
    "backup=" + encodeURIComponent(name) +
    "&uid=" + encodeURIComponent(uid)
  );
}

/** The wizard with NO point chosen: its selector. */
export function restoreSelectorRoute(ns) {
  const n = typeof ns === "string" ? ns.trim() : "";
  return "#/restore" + (n.length > 0 ? "?ns=" + encodeURIComponent(n) : "");
}

// ----------------------------------- the fresh-target retry (PLAT-11.2, PLAT-12.2)

/** How many characters of the failed Restore's name the retry prefix carries.
 *
 *  Eight, off the END. A minted restore name is `rst-` plus a content-derived
 *  suffix (`logweir-api`'s `idempotency.rs`), so the tail is the part that
 *  distinguishes two runs; taking it keeps the prefix short enough to leave a
 *  long topic name inside the broker's 249. A collision between two tails is
 *  not a correctness problem -- it would make two retries share a prefix, and
 *  the readiness check's `target.mappedTopics` row refuses the second by
 *  name -- which is why this is allowed to be a tail rather than a hash. */
export const RETRY_TAIL_CHARS = 8;

/** THE PREFIX A FRESH-TARGET RETRY USES, and why it cannot be the default one.
 *
 *  `defaultTopicPrefix` is a PURE FUNCTION OF THE RECOVERY POINT. That is
 *  exactly right for a first restore and exactly wrong for a retry: retrying
 *  the same point with the same prefix renders the same plan bytes, which mint
 *  the same Restore name and the same Approval name -- so the retry would
 *  collide with the failed run's own name and, worse, would be authorised by
 *  the Approval bound to it. PLAT-12.2's clause is that "retrying failed work
 *  creates a new execution with deliberate fresh-target/approval handling
 *  rather than colliding with the old name", and this function is the
 *  deliberate part.
 *
 *  DETERMINISTIC, so retrying twice is the same retry: the same failed run
 *  gives the same prefix, the same bytes, the same minted name, and the second
 *  submit is the idempotent replay every other create in this wizard is. It
 *  reads no clock -- the instant comes from the point, as everywhere else. */
export function freshTargetPrefix(pointInTime, retryOf) {
  // AN INPUT THAT IS NOT AN INSTANT HAS NO DEFAULT PREFIX, and saying so is
  // this function's job rather than throwing. The retry route reaches the
  // SELECTOR first -- `#/restore?ns=...&retryOf=...` names no recovery point, on
  // purpose, because a `Restore` carries a backup set id and not the `Backup`
  // it came from -- so there is no point in time to derive from until one is
  // chosen. The first cut called `defaultTopicPrefix` unconditionally and a
  // live journey met the whole page replaced by `defaultTopicPrefix(): - is
  // not an instant this page can read`. `prefixFor` has always had this arm
  // for the same reason.
  if (typeof pointInTime !== "string" || Number.isNaN(new Date(pointInTime).getTime())) {
    return "";
  }
  const base = defaultTopicPrefix(pointInTime);
  const name = typeof retryOf === "string" ? retryOf : "";
  let tail = "";
  for (const c of name.slice(-RETRY_TAIL_CHARS).toLowerCase()) {
    if ((c >= "a" && c <= "z") || (c >= "0" && c <= "9")) {
      tail += c;
    }
  }
  return base + "retry-" + tail + "-";
}

/** The link that opens the wizard as a FRESH-TARGET RETRY of one failed
 *  Restore, on the same recovery point. */
export function restoreRetryRoute(ns, backup, restoreName) {
  const name = typeof restoreName === "string" ? restoreName : "";
  return restorePointRoute(ns, backup) + "&retryOf=" + encodeURIComponent(name);
}

/** The link a FAILED Restore offers: the wizard's selector, carrying which run
 *  is being retried but choosing no point.
 *
 *  IT CANNOT NAME THE POINT, AND THAT IS A PROPERTY OF THE OBJECT RATHER THAN
 *  A SHORTCUT. `Restore.spec` carries `backupSetRef` -- a backup SET id inside
 *  the archive -- and no reference to the `Backup` that produced it, so a page
 *  looking at a Restore knows which archive set to read and not which recovery
 *  point object it came from. Guessing one by searching the namespace for a
 *  Backup with a matching set id would be this page inventing an identity the
 *  product does not record. So the operator chooses the point, on the
 *  selector, with the retry travelling beside the choice. */
export function restoreRetryFromOperationRoute(ns, restoreName) {
  const name = typeof restoreName === "string" ? restoreName : "";
  return restoreSelectorRoute(ns) +
    (restoreSelectorRoute(ns).indexOf("?") === -1 ? "?" : "&") +
    "retryOf=" + encodeURIComponent(name);
}

/** The banner a retry opens with: what it is retrying, what is different, and
 *  the three things it does NOT do.
 *
 *  THE OLD APPROVAL IS NEVER REUSED, AND THAT IS STRUCTURAL RATHER THAN
 *  PROMISED. `restoreBody` takes `spec.approvalRef.name` from the prepared
 *  document's minted names and from nowhere else, and the names are minted
 *  from the plan bytes -- so a plan with a fresh prefix mints a different
 *  Approval name, and no route through this module can send the failed run's
 *  own Approval reference. The controller's `PlanHashMismatch` refusal is the
 *  backstop if one ever were. */
export function renderRetryBanner(state) {
  const s = state || {};
  const of = typeof s.retryOf === "string" ? s.retryOf : "";
  if (of.length === 0) {
    return "";
  }
  return (
    "<div class=\"retry-note\" id=\"retry-banner\" role=\"status\">" +
    "<h3>Retrying to a fresh target</h3>" +
    "<p class=\"note\">This is a NEW restore of the same recovery point, retrying <code>" +
    esc(of) + "</code>. " + esc(RETRY_IDENTITY_SENTENCE) + "</p>" +
    "<p class=\"note\" id=\"retry-untouched\">" + esc(RETRY_OLD_RUN_SENTENCE) + "</p>" +
    "</div>"
  );
}

/** What a retry changes, said where it is offered. */
export const RETRY_IDENTITY_SENTENCE =
  "The topic prefix is a fresh one derived from the run being retried, so every target topic " +
  "name is new and nothing the failed run created is written to. A different prefix is a " +
  "different plan, a different plan hash, a different minted Restore name and a different " +
  "minted Approval name: the approval that authorised the failed run is not reused and cannot " +
  "be -- both names come from these bytes, and a forged reference to it is refused by the " +
  "controller with PlanHashMismatch. Where the namespace's policy is governed, this restore " +
  "waits for a new approval of its own.";

/** What a retry leaves alone. */
export const RETRY_OLD_RUN_SENTENCE =
  "The failed restore is not touched: this wizard writes to nothing that exists. Its object, " +
  "its evidence and the topics it had already created stay exactly as they are, and resume is " +
  "not implemented -- this is a new run from the beginning, not a continuation of that one.";

/** Whether a `Backup` is a recovery point a plan can be built from:
 *  `phase: Succeeded`, a non-empty `status.backupId`, AND a covered window of
 *  two integers.
 *
 *  THE WINDOW IS PART OF IT, because every other field of the plan is derived
 *  from it: the point in time defaults to the window's end, the sample window
 *  is the window itself, and the default topic prefix is a function of that
 *  instant. A `Succeeded` run with no `windowCovered` -- which is what a run
 *  that completed before the controller wrote its status looks like for a
 *  moment -- is not a point this page can offer; it is a row in the catalog
 *  table with nothing to restore from. */
export function isRecoveryPoint(backup) {
  const status = (backup || {}).status || {};
  const covered = status.windowCovered || {};
  return (
    status.phase === "Succeeded" &&
    typeof status.backupId === "string" &&
    status.backupId.length > 0 &&
    typeof covered.fromMs === "number" &&
    typeof covered.toMs === "number"
  );
}

/** The recovery points a namespace holds, NEWEST COMPLETION FIRST.
 *
 *  Ordered by the `Complete` condition's transition time, falling back to the
 *  creation timestamp -- never by the covered window, because the window is
 *  about the RECORDS and two runs can cover windows that end in the other
 *  order from the order they ran. A tie keeps list order, which is
 *  `kubectl`'s. Pure; no clock. */
export function recoveryPoints(backups) {
  return itemsOf(backups)
    .filter(isRecoveryPoint)
    .map((point, index) => ({ point: point, index: index }))
    .sort((a, b) => {
      const left = completedAt(a.point);
      const right = completedAt(b.point);
      if (left === right || left === null || right === null) {
        return a.index - b.index;
      }
      return left < right ? 1 : -1;
    })
    .map((entry) => entry.point);
}

/** WHAT A ROUTE IDENTITY RESOLVES TO against the Backups a namespace holds.
 *  Pure, and the ONE place the wizard decides which point it is bound to.
 *
 *  Four answers, and none of them is "the newest one instead":
 *   - `none`      -- nothing was asked for; the page is the selector.
 *   - `selected`  -- the UID is here and it is a recovery point.
 *   - `unusable`  -- the object is here and is not `Succeeded` with a set and
 *                    a window; its phase is carried so the refusal can say so.
 *   - `missing`   -- nothing answers to that UID. When the NAME is now held by
 *                    a different object, `renamed` says so: that is a point
 *                    deleted and recreated, and following it would be
 *                    restoring from a run nobody chose. */
export function resolvePoint(backups, selection) {
  const wanted = selection || {};
  const uid = typeof wanted.uid === "string" ? wanted.uid.trim() : "";
  const name = typeof wanted.backup === "string" ? wanted.backup.trim() : "";
  const blank = { state: "none", point: null, uid: "", name: "", phase: null, renamed: false };
  if (uid.length === 0 && name.length === 0) {
    return blank;
  }
  const all = itemsOf(backups);
  let found = null;
  for (const backup of all) {
    const meta = (backup || {}).metadata || {};
    if (uid.length > 0 ? meta.uid === uid : meta.name === name) {
      found = backup;
      break;
    }
  }
  if (found === null) {
    let renamed = false;
    if (uid.length > 0 && name.length > 0) {
      for (const backup of all) {
        if (((backup || {}).metadata || {}).name === name) {
          renamed = true;
          break;
        }
      }
    }
    return { state: "missing", point: null, uid: uid, name: name, phase: null, renamed: renamed };
  }
  const meta = found.metadata || {};
  const identity = {
    uid: typeof meta.uid === "string" && meta.uid.length > 0 ? meta.uid : uid,
    name: typeof meta.name === "string" && meta.name.length > 0 ? meta.name : name,
  };
  if (!isRecoveryPoint(found)) {
    return {
      state: "unusable",
      point: null,
      uid: identity.uid,
      name: identity.name,
      phase: ((found.status || {}).phase),
      renamed: false,
    };
  }
  return {
    state: "selected",
    point: found,
    uid: identity.uid,
    name: identity.name,
    phase: "Succeeded",
    renamed: false,
  };
}

/** The lowercased text one point is searched by: its name, its schedule, its
 *  slot, its backup set, its source cluster, its archive and its topics. */
export function pointHaystack(backup) {
  const b = backup || {};
  const meta = b.metadata || {};
  const spec = b.spec || {};
  const status = b.status || {};
  const parts = [
    meta.name,
    (spec.scheduleRef || {}).name,
    spec.slot,
    (spec.sourceRef || {}).name,
    (spec.archive || {}).url,
    status.backupId,
  ].concat(Array.isArray(spec.topics) ? spec.topics : []);
  return parts
    .filter((part) => typeof part === "string" && part.length > 0)
    .join(" ")
    .toLowerCase();
}

/** Whether a haystack matches a query. EVERY whitespace-separated term must
 *  appear, so `orders 1406` narrows rather than widens. An empty query matches
 *  everything -- a filter nobody typed hides nothing. Pure. */
export function matchesQuery(haystack, query) {
  const text = typeof haystack === "string" ? haystack.toLowerCase() : "";
  const terms = (typeof query === "string" ? query : "")
    .toLowerCase()
    .split(/\s+/)
    .filter((term) => term.length > 0);
  for (const term of terms) {
    if (text.indexOf(term) === -1) {
      return false;
    }
  }
  return true;
}

/** The signed verdict a point carries, in words. `unverified` is a real state
 *  and not a missing value: the controller writes `evidence.verification` only
 *  after it has fetched the receipt and checked it. */
export function pointVerdict(backup) {
  const verification = (((backup || {}).status || {}).evidence || {}).verification || {};
  return typeof verification.result === "string" && verification.result.length > 0
    ? verification.result
    : UNVERIFIED;
}

/** WHAT "ARCHIVE AVAILABILITY" MEANS TODAY, and it is a statement about the
 *  STATUS and not about the bucket. Logweir holds no list capability against
 *  an archive and this page holds no bucket credential at all, so the nearest
 *  thing to availability the cluster can tell it is whether the run that wrote
 *  the set recorded a manifest key for it. A point with no manifest key is a
 *  point whose set the runner will have to find by name alone.
 *
 *  PLAT-15.1's durable catalog is what turns this into a real answer: it
 *  records, per set, whether the objects are still there. Until then this page
 *  says exactly what it knows and no more. */
export function archiveAvailability(backup) {
  const status = (backup || {}).status || {};
  return typeof status.manifestKey === "string" && status.manifestKey.length > 0
    ? "manifest recorded"
    : "no manifest recorded";
}

// ---------------------------------------------------------------- the steps

/** Step 1 -- the archive. A `KafkaCluster` with `role: source`, and the
 *  archives this namespace's `Backup` objects name.
 *
 *  THE PAGE HOLDS NO BUCKET CREDENTIAL AND DOES NOT LIST OBJECT STORAGE. Every
 *  archive and every backup set on this page was read from a Kubernetes
 *  object, which is the only thing this page can read. */
export function renderArchiveStep(state) {
  const s = state || {};
  const sources = itemsOf(s.clusters).filter((c) => ((c.spec || {}).role) === "source");
  const rows = sources.map((cluster) => {
    const meta = cluster.metadata || {};
    const status = cluster.status || {};
    return [
      cell(meta.name),
      cell(((cluster.spec || {}).bootstrapServers || []).join(", ")),
      cell(status.clusterId),
      probeLine(probeState(cluster, s.now, s.freshSeconds)),
      cell(archiveFor(s, meta.name)),
      cell(archiveSecretFor(s, meta.name)),
    ];
  });
  return (
    "<section class=\"step\" id=\"step-archive\" tabindex=\"-1\"><h3>1. Archive</h3>" +
    "<p class=\"blurb\">The source cluster this restore reads an archive of. The archives " +
    "below were read from this namespace's Backup objects: this page holds no bucket " +
    "credential and lists no object storage.</p>" +
    renderSourceBinding(s) +
    table(
      ["SOURCE CLUSTER", "BOOTSTRAP", "CLUSTER ID", "CONNECTION PROBE", "ARCHIVE",
        "ARCHIVE CREDENTIAL"],
      rows,
      "no KafkaCluster in this namespace carries role: source",
    ) +
    renderArchiveCredentialField(s) +
    renderStoreFields(s) +
    "</section>"
  );
}

/** THE SOURCE SIDE'S OWN BINDING: the saved connection the chosen recovery
 *  point was taken from, resolved against the connections that exist now.
 *
 *  A `Backup` records `spec.sourceRef.name` and no uid -- the reference was
 *  written before saved connections had identities -- so this resolves BY NAME
 *  and says what it resolved to, including the uid it pinned. That is the
 *  honest statement available here: the archive this restore reads is on
 *  object storage and does not depend on the source cluster still existing, so
 *  a source that is gone is a NOTE and not a refusal -- unlike the target,
 *  which the run writes into.
 *
 *  The probe beside it is the same probe the clusters page renders, with the
 *  same freshness budget and the same refusal to call anything ready. */
export function renderSourceBinding(state) {
  const s = state || {};
  const point = s.point || null;
  const named = (((point || {}).spec || {}).sourceRef || {}).name;
  if (typeof named !== "string" || named.length === 0) {
    return "";
  }
  const resolved = resolveClusterSelection(s.clusters, { uid: "", name: named });
  if (resolved.state !== "selected") {
    return (
      "<p class=\"note\" id=\"source-binding\">This recovery point was taken from the " +
      "KafkaCluster <code>" + esc(named) + "</code>, which is no longer in this namespace. The " +
      "archive is on object storage and does not need it, so the restore can still be built; " +
      "nothing below was substituted for it.</p>"
    );
  }
  return (
    "<p class=\"note\" id=\"source-binding\">Source connection: <code>" +
    esc(resolved.name) + "</code>, uid <code>" + esc(resolved.uid) + "</code> (role: " +
    cell(resolved.role) + "). " + probeLine(probeState(resolved.cluster, s.now, s.freshSeconds)) +
    "</p>"
  );
}

/** The NAME of the Secret the runner reads the archive with -- never a value.
 *
 *  `ArchiveRef` is `{url, secretRef}` and both halves belong to the `Restore`
 *  this wizard creates: `spec.sourceArchive.url` says where the archive is and
 *  `spec.sourceArchive.secretRef.name` says what reaches it. The controller
 *  injects the object-store credential into the runner Job ONLY when that
 *  second half is present, so a `Restore` submitted without it is admitted by
 *  the API server -- the field is optional in the CRD -- and then fails at the
 *  ARCHIVE rather than at admission: the Job starts, reaches for the first
 *  object, and cannot read it.
 *
 *  Prefilled from the Backup object's own archive reference, which is where
 *  this page read the URL beside it. Blank when that archive names none, which
 *  is an archive reached anonymously or by an instance role -- a real
 *  configuration, and the reason this is an input and not a refusal. */
export function renderArchiveCredentialField(state) {
  const s = state || {};
  const name = typeof s.archiveSecretName === "string" ? s.archiveSecretName : "";
  const errors = errorsOf(s);
  return (
    "<h4>The credential that reaches that archive</h4>" +
    "<div class=\"field\">" +
    "<label for=\"archive-secret\">ARCHIVE CREDENTIAL (Secret name)</label>" +
    "<input id=\"archive-secret\" name=\"archiveSecret\" value=\"" + esc(name) + "\"" +
    invalidAttributes("archive-secret", errors.archiveSecret) + ">" +
    fieldErrorLine("archive-secret", errors.archiveSecret) +
    "<p class=\"note\">The runner reads the archive with this credential: weirkeeper mounts " +
    "the named Secret's keys into the runner Job as its object-store credential, and does so " +
    "only when spec.sourceArchive.secretRef is set. A Restore created without it is " +
    "ADMITTED and then fails at the archive, not at admission -- the field is optional in " +
    "the CRD, so nothing refuses it until the Job cannot read an object. Leave it blank only " +
    "for an archive reached anonymously or by an instance role. This page shows and sends " +
    "the NAME; it never reads the Secret.</p>" +
    "</div>"
  );
}

/** The three object-store settings NO Kubernetes object in this product
 *  records, and the evidence bucket.
 *
 *  `ArchiveRef` is `{url, secretRef}` -- a location and a credential name --
 *  so an endpoint, a region and the `path_style` flag are nowhere in the
 *  cluster's own view. The runner reads them out of THESE BYTES and from
 *  nowhere else, which is why they are inputs here rather than something an
 *  operator edits into the downloaded plan afterwards: a plan edited after it
 *  was hashed is a plan the approval no longer covers.
 *
 *  The evidence PREFIX is not among them. Global Constraint 6 fixes it, and a
 *  plan naming another one is refused at phase 0 after the signature. */
export function renderStoreFields(state) {
  const s = state || {};
  const store = ((s.fields || {}).source) || {};
  return (
    "<h4>Where that archive actually is</h4>" +
    "<p class=\"note\">Leave the endpoint blank for AWS S3. These three values are not on " +
    "any object in the cluster, and the runner reads them from the plan bytes.</p>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"store-endpoint\">endpoint</label>" +
    "<input id=\"store-endpoint\" name=\"endpoint\" value=\"" + esc(store.endpoint) + "\"></div>" +
    "<div class=\"field\"><label for=\"store-region\">region</label>" +
    "<input id=\"store-region\" name=\"region\" value=\"" + esc(store.region) + "\"></div>" +
    "</div>" +
    "<label class=\"inline\" for=\"store-pathStyle\">" +
    "<input type=\"checkbox\" id=\"store-pathStyle\" name=\"pathStyle\"" +
    (store.pathStyle === true ? " checked" : "") + "> path_style addressing</label>" +
    "<p class=\"note\">" + ADDRESSING_NOTE + "</p>" +
    renderInsecureTransportField(s) +
    "<div class=\"field\"><label for=\"evidence-bucket\">evidence bucket</label>" +
    "<input id=\"evidence-bucket\" name=\"evidenceBucket\" value=\"" +
    esc(((s.fields || {}).evidence || {}).bucket) + "\"" +
    invalidAttributes("evidence-bucket", errorsOf(s).evidenceBucket) + ">" +
    fieldErrorLine("evidence-bucket", errorsOf(s).evidenceBucket) +
    "<p class=\"note\">The evidence prefix is fixed at " + esc(EVIDENCE_PREFIX) + " by Global " +
    "Constraint 6 and is not an input: a plan naming another one is refused at phase 0, " +
    "after the approver has already signed it.</p></div>"
  );
}

/** Said beside the addressing checkbox, so a reader knows what it does and --
 *  as of decision D2 section 13.2, task W13a -- what it does NOT do. */
export const ADDRESSING_NOTE =
  "path_style addressing says where a bucket's name goes in a URL: after the host rather " +
  "than in front of it, which is what most on-premises object stores need. It says nothing " +
  "about transport security and sets nothing else in this plan.";

/** THE ONE CONTROL THAT ALLOWS PLAINTEXT TRANSPORT, and it is checked by hand.
 *
 *  Until this was split out, the wizard set the plan's insecure-transport flag
 *  from the ADDRESSING checkbox (defect UI-HTTPDOWNGRADE): an operator ticking
 *  "path_style" for a MinIO or Ceph endpoint -- which is what every
 *  on-premises store needs -- silently also told the runner it could send the
 *  archive credential and every restored record over an unencrypted
 *  connection. The two are independent controls over independent things, and
 *  D-SEAMS S5 states the rule for the whole product: neither addressing style,
 *  nor endpoint shape, nor any environment value may enable plaintext
 *  transport. So the flag has its own box, it defaults OFF, and nothing in
 *  this page reads any other control to set it.
 *
 *  IT IS A PLAN FIELD, NOT A PAGE SETTING. The runner reads it out of the
 *  signed bytes, so an approver sees it in the document they sign -- which is
 *  the other half of why deriving it was a defect: the derived value went into
 *  a signed document nobody had stated. */
export function renderInsecureTransportField(state) {
  const s = state || {};
  const store = ((s.fields || {}).source) || {};
  return (
    "<label class=\"inline\" for=\"store-allow-insecure\">" +
    "<input type=\"checkbox\" id=\"store-allow-insecure\" name=\"allowHttp\"" +
    (store.allowHttp === true ? " checked" : "") +
    "> Allow insecure HTTP (explicit, local development only)</label>" +
    "<p class=\"complaint\" id=\"insecure-transport-warning\">" + INSECURE_TRANSPORT_WARNING +
    "</p>"
  );
}

/** The warning printed under that box, whether or not it is ticked. */
export const INSECURE_TRANSPORT_WARNING =
  "Ticking this writes allow_" + "http" + " true into the plan an approver signs, and the " +
  "runner then reaches the archive and the evidence store over an unencrypted connection: " +
  "the object-store credential and every restored record cross the network in the clear. " +
  "Nothing else on this page sets it -- not the addressing style, not the endpoint, not any " +
  "value from the environment -- and it defaults to off. Tick it only for a store on your " +
  "own machine.";

/** The sentence step 2 prints under the point this wizard is bound to.
 *
 *  It replaces the old reload advice, and it replaces it because the advice
 *  was true: this page used to pick the newest `Succeeded` Backup for itself,
 *  and Task 28 measured three `Backup` objects arriving in six minutes against
 *  a two-minute schedule -- with the chosen set, the covered window, the plan
 *  bytes, the plan hash and BOTH minted names moving under the operator on
 *  every reload. The identity now lives in this page's address, so a reload
 *  finds the same point and a newer completion does not move it. */
export const POINT_PINNED_SENTENCE =
  "this point is named in this page's address by its UID, so a newer backup completing while " +
  "you read this does not move it: a reload, and every re-read this page makes, resolve to " +
  "the same run. Choose a different recovery point below to change it.";

/** Printed in the catalog table's caption when NO run in this namespace has
 *  reached `Succeeded` with a set and a covered window. */
export const NO_SUCCEEDED_SENTENCE =
  "no run in this archive has reached phase Succeeded with a backup set and a covered window; " +
  "a set no completed run wrote is a set the runner will not find.";

/** Printed as the WHOLE page when no run in this namespace has completed with
 *  a backup set: the wizard builds a plan document from a completed run's
 *  set and covered window, and without one there is nothing to hash. Before
 *  Task 28a this case threw inside the plan renderer and the page was an
 *  error box (Task 28a's review, measured pre-existing at 352a4b8). */
export const NO_COMPLETED_BACKUP_SENTENCE =
  "no Backup in this namespace has completed with a backup set yet; the restore wizard " +
  "needs one to build a plan document. Wait for a scheduled run to reach phase Succeeded, " +
  "or create a Backup, then reload this page. The runs the namespace does hold are listed " +
  "below.";

/** The `Backup`s a plan can be built from: see [`isRecoveryPoint`]. Kept under
 *  its old name because it is what the mount half asks before it offers a
 *  wizard at all. Pure. */
export function completedBackups(backups) {
  return itemsOf(backups).filter(isRecoveryPoint);
}

/** The page rendered instead of the selector when [`completedBackups`] is
 *  empty: the heading, the sentence, and the catalog table so the reader sees
 *  what the namespace does hold. Pure; never throws on an empty or
 *  running-only list. */
export function renderNoCompletedBackup(ns, backups) {
  return (
    "<h2>Restore wizard</h2>" +
    "<div class=\"empty-state\"><p class=\"note\">" + NO_COMPLETED_BACKUP_SENTENCE + "</p></div>" +
    "<section class=\"step\" id=\"step-catalog\" tabindex=\"-1\"><h3>What this namespace holds</h3>" +
    renderCatalogTable(itemsOf(backups), null) +
    "<p class=\"note\">" + NO_SUCCEEDED_SENTENCE + "</p></section>"
  );
}

/** Every `Backup` in the list with its set, its covered range CONVERTED TO RFC
 *  3339 (interface I22: the field is two integers, and a viewer reading
 *  `1757253900000` learns nothing) and its phase -- and the chosen one, named.
 *  This is the CATALOG, not the selector: it shows the running and failed runs
 *  too, because "what is here" is the question it answers. */
export function renderCatalogTable(backups, chosenName) {
  const rows = itemsOf(backups).map((backup) => {
    const status = backup.status || {};
    const covered = status.windowCovered || {};
    const name = (backup.metadata || {}).name;
    return [
      cell(status.backupId),
      cell(rfc3339(covered.fromMs)),
      cell(rfc3339(covered.toMs)),
      cell(status.records),
      cell(status.phase),
      cell(name === chosenName && typeof name === "string" ? name + " (chosen)" : name),
    ];
  });
  return table(
    ["BACKUP SET", "COVERED FROM", "COVERED TO", "RECORDS", "PHASE", "BACKUP"],
    rows,
    "no Backup names this archive in this namespace",
  );
}

/** Step 2 -- THE RECOVERY POINT this wizard is bound to, and what it covers.
 *
 *  Every value here was read from the chosen `Backup`'s own spec and status.
 *  "Coverage" is `status.windowCovered`, two epoch-millisecond integers, shown
 *  as RFC 3339 and closed at both ends; the topics are the frozen list the run
 *  archived (`spec.topics`); the source cluster is `spec.sourceRef`; the
 *  signed verdict is what weirkeeper recorded when it verified the receipt;
 *  and "archive" is [`archiveAvailability`]'s statement about the status, not
 *  about the bucket -- this page holds no bucket credential and lists no
 *  object storage. */
export function renderRecoveryPointStep(state) {
  const s = state || {};
  const point = s.point || null;
  const meta = (point || {}).metadata || {};
  const spec = (point || {}).spec || {};
  const status = (point || {}).status || {};
  const covered = status.windowCovered || {};
  const topics = Array.isArray(spec.topics) ? spec.topics : [];
  return (
    "<section class=\"step\" id=\"step-backup-set\" tabindex=\"-1\"><h3>2. Recovery point</h3>" +
    "<p class=\"blurb\">The run this restore reads, chosen explicitly and pinned by UID. " +
    "Everything below was read from that Backup's own spec and status; the covered range is " +
    "Backup.status.windowCovered, two epoch-millisecond integers, shown as RFC 3339.</p>" +
    facts([
      ["Backup", "<code id=\"point-name\">" + esc(meta.name) + "</code>"],
      ["uid", "<code id=\"point-uid\">" + esc(meta.uid) + "</code>"],
      ["backup set", cell(status.backupId)],
      ["schedule", cell((spec.scheduleRef || {}).name)],
      ["slot", cell(spec.slot)],
      ["source cluster", cell((spec.sourceRef || {}).name)],
      ["covered from", cell(rfc3339(covered.fromMs))],
      ["covered to", cell(rfc3339(covered.toMs))],
      ["topics", topics.length === 0 ? cell(null) : esc(topics.join(", "))],
      ["records", cell(status.records)],
      ["signed", cell(pointVerdict(point))],
      ["archive", cell((spec.archive || {}).url) + " -- " + esc(archiveAvailability(point))],
    ]) +
    "<p class=\"note\">" + POINT_PINNED_SENTENCE + "</p>" +
    "<div class=\"actions\"><a class=\"nav-link\" id=\"choose-another-point\" href=\"" +
    esc(restoreSelectorRoute(s.ns)) + "\">Choose a different recovery point</a></div>" +
    "<h4>What this namespace holds</h4>" +
    renderCatalogTable(backupsOf(s), meta.name) +
    "</section>"
  );
}

// ------------------------------------------------------------- the selector

/** The selector's blurb: what a row is and what choosing one does. */
export const SELECTOR_SENTENCE =
  "Every completed run this namespace holds, newest completion first. Choosing one pins it " +
  "to this page's address by its UID, so the wizard stays on it while newer backups arrive. " +
  "A run still in flight, or one that completed without a backup set, is not offered: there " +
  "is nothing to restore from yet.";

/** Printed in place of the table when a search matches no row. */
export const NO_MATCH_SENTENCE = "no recovery point matches that search";

/** THE SELECTOR: the page the wizard is when no point has been chosen.
 *
 *  It is a page and not a step, because there are no steps yet: a plan
 *  document is built from a point's set, window and topics, so with no point
 *  there is nothing to render, nothing to hash and nothing to submit. Choosing
 *  a row is a LINK, not a button: the identity belongs in the address, which
 *  is what makes it survive a reload and be sharable, and a selection kept
 *  only in this page's memory would be a selection a refresh threw away. */
export function renderPointSelector(state) {
  const s = state || {};
  const points = recoveryPoints(s.backups);
  const query = typeof s.query === "string" ? s.query : "";
  const rows = points.map((point) => {
    const meta = point.metadata || {};
    const spec = point.spec || {};
    const status = point.status || {};
    const covered = status.windowCovered || {};
    const topics = Array.isArray(spec.topics) ? spec.topics : [];
    return [
      cell(meta.name),
      cell((spec.scheduleRef || {}).name),
      cell(spec.slot),
      cell(rfc3339(covered.fromMs)),
      cell(rfc3339(covered.toMs)),
      topics.length === 0 ? cell(null) : esc(topics.join(", ")),
      cell(status.records),
      cell(pointVerdict(point)),
      cell((spec.archive || {}).url) + " -- " + esc(archiveAvailability(point)),
      // A RETRY ARRIVING HERE KEEPS ITS IDENTITY (PLAT-11.2). The operation
      // view of a failed Restore knows which run failed and not which Backup
      // it came from -- `Restore.spec` carries a backup SET id, not the
      // point's name or uid -- so the retry link lands on this selector and
      // the choice of point is made here, with `retryOf` travelling on.
      "<a href=\"" +
        esc(typeof s.retryOf === "string" && s.retryOf.length > 0
          ? restoreRetryRoute(s.ns, point, s.retryOf)
          : restorePointRoute(s.ns, point)) +
        "\">" + (typeof s.retryOf === "string" && s.retryOf.length > 0
          ? "Retry to a fresh target from this point"
          : "Restore this point") + "</a>",
    ];
  });
  const attributes = points.map(
    (point) =>
      "data-point=\"" + esc(((point.metadata || {}).uid)) + "\" data-search=\"" +
      esc(pointHaystack(point)) + "\"",
  );
  return (
    "<h2>Restore wizard</h2>" +
    "<p class=\"blurb\">" + SELECTOR_SENTENCE + "</p>" +
    renderRetryBanner(s) +
    "<section class=\"step\" id=\"step-select-point\" tabindex=\"-1\">" +
    "<h3>Choose a recovery point</h3>" +
    "<div class=\"field\"><label for=\"point-search\">search</label>" +
    "<input id=\"point-search\" name=\"q\" value=\"" + esc(query) + "\">" +
    "<p class=\"help\">Filters the rows below by name, schedule, slot, source cluster, " +
    "archive, backup set or topic. Every word must match.</p></div>" +
    table(
      ["BACKUP", "SCHEDULE", "SLOT", "COVERED FROM", "COVERED TO", "TOPICS", "RECORDS",
        "SIGNED", "ARCHIVE", ""],
      rows,
      NO_COMPLETED_BACKUP_SENTENCE,
      attributes,
    ) +
    "<p class=\"note\" id=\"no-match\" hidden>" + NO_MATCH_SENTENCE + "</p>" +
    "<h4>What this namespace holds</h4>" +
    renderCatalogTable(itemsOf(s.backups), null) +
    "</section>"
  );
}

// ------------------------------------------------------------- the refusals

/** THE REFUSAL a selected point that is gone, or that is not a recovery point,
 *  gets. It names the identity that was asked for and offers the selector; it
 *  renders NO plan, no hash and no submit, because there is nothing to build
 *  one from and "nearly the point you chose" is a different restore. */
export function renderPointRefusal(state) {
  const s = state || {};
  const asked =
    "<p class=\"note\">Asked for: Backup <code>" + esc(s.pointName) + "</code>, uid <code>" +
    esc(s.pointUid) + "</code>.</p>";
  const why = s.pointState === "unusable"
    ? "<p class=\"refusal\">That Backup is in phase <code>" + esc(s.pointPhase) +
      "</code> and is not a recovery point: a plan is built from a completed run's backup " +
      "set and covered window, and this run has not recorded both. Nothing was read and " +
      "nothing was sent.</p>"
    : "<p class=\"refusal\">No Backup in this namespace carries that uid, so the recovery " +
      "point this link names is gone. It was not replaced by another: this page will not " +
      "restore from a run nobody chose." +
      (s.pointRenamed === true
        ? " A DIFFERENT object now answers to that name, which is what a Backup deleted and " +
          "recreated looks like -- a different run, over a different archive window."
        : "") +
      "</p>";
  return (
    "<h2>Restore wizard</h2>" +
    "<div class=\"refusal-block\" id=\"point-refusal\" role=\"alert\">" + why + asked +
    "<p class=\"note\"><a href=\"" + esc(restoreSelectorRoute(s.ns)) +
    "\">Choose a recovery point</a></p></div>" +
    "<h3>What this namespace holds</h3>" +
    renderCatalogTable(itemsOf(s.backups), null)
  );
}

/** Step 3 -- the point in time, defaulted to the set's `toMs` and checked
 *  against `[fromMs, toMs]` BY CONVERTING THE INPUT BACK TO EPOCH
 *  MILLISECONDS.
 *
 *  THE CHECK IS A CONVENIENCE AND NEVER THE GATE, and the page says so. The
 *  controller recomputes the hash from the referent's own bytes and phase 0
 *  refuses a point the archive does not cover; this input exists so an
 *  operator finds out before signing rather than after. */
export function renderPointInTimeStep(state) {
  const s = state || {};
  const covered = coveredOf(s);
  const value =
    typeof (s.fields || {}).pointInTime === "string" && s.fields.pointInTime.length > 0
      ? s.fields.pointInTime
      : rfc3339(covered.toMs);
  const complaint = windowComplaint(value, covered);
  const errors = errorsOf(s);
  return (
    "<section class=\"step\" id=\"step-point-in-time\" tabindex=\"-1\"><h3>3. Point in time</h3>" +
    "<p class=\"blurb\">An RFC 3339 instant. The window is closed at both ends: a record " +
    "whose timestamp equals this exactly is restored. Its FLOOR is the archive's own " +
    "earliest covered timestamp, read from the manifest by the runner, and is never a " +
    "field of this plan.</p>" +
    "<div class=\"field\">" +
    "<label for=\"point-in-time\">point in time</label>" +
    "<input id=\"point-in-time\" name=\"pointInTime\" value=\"" + esc(value) + "\"" +
    invalidAttributes("point-in-time", errors.pointInTime) + ">" +
    fieldErrorLine("point-in-time", errors.pointInTime) +
    "<p class=\"window\">" + windowMessage(covered.fromMs, covered.toMs) + "</p>" +
    "</div>" +
    // THE COMPLAINT CARRIES AN ID, and it needs one: the `window` line above
    // states the same sentence unconditionally (it is the bound, printed
    // beside the input), so "the page contains the window message" is true of
    // every state and cannot tell a refused point from an accepted one. A live
    // journey asserted exactly that and its own negative control caught it.
    (complaint === null
      ? ""
      : "<p class=\"complaint\" id=\"point-in-time-complaint\">" + complaint + "</p>") +
    "<p class=\"note\">" + WINDOW_REFUSAL_SENTENCE + "</p>" +
    "</section>"
  );
}

/** The sentence step 4 prints when no `KafkaCluster` in the namespace carries
 *  `role: target`, and the source cluster is preselected instead.
 *
 *  IT IS A LABEL AND NOT AN AUTHORISATION, AND THE CRD SAYS SO.
 *  `crates/weirkeeper/src/crds/kafka_cluster.rs` documents `role` as "a
 *  free-form string ... a label the adopter picks and the controller reports,
 *  and `allowedClusterIds` on the cluster-scoped `TrustRoster` is what
 *  actually authorises a target, never a role written next to the address it
 *  authorises". The runner agrees: `drill/phase0_admit.rs` branches on
 *  `spec.target.mode`, and all three cluster checks -- the allowlist, the
 *  target != source rule and the marker topic -- live in the `Scratch` arm
 *  alone. The `NewTopic` arm is EMPTY, with a comment saying the source
 *  cluster is exactly where a point-in-time recovery belongs. */
export const TARGET_ROLE_SENTENCE =
  "no cluster is labelled role: target; the source cluster is preselected. The role is a " +
  "label, not an authorisation: for mode newTopic the runner accepts any reachable target, " +
  "the source cluster included; mode scratch is refused by the runner unless the target is " +
  "among the approval's allowed cluster ids, differs from the source, and proves it is " +
  "scratch with its marker topic.";

/** The warning step 4 prints for `mode: scratch` against a cluster whose spec
 *  declares no `markerTopic`. A WARNING and not a refusal: the runner refuses,
 *  at phase 0, against the cluster it actually reaches -- and this page reads
 *  a spec field, which is a statement of intent rather than an observation. */
export const SCRATCH_MARKER_WARNING =
  "this cluster's spec declares no markerTopic, and mode scratch is refused at phase 0 " +
  "unless the target proves it is scratch by carrying one. The runner checks the broker; " +
  "this line only checks the object.";

/** Printed under step 3, in place of the convenience sentence it used to carry.
 *
 *  The old sentence said the client-side window check was a convenience and
 *  never the gate, which was true of a check that only complained. Since
 *  PLAT-11.1 this page REFUSES a point outside the disclosed coverage and sends
 *  nothing, so the sentence had to change with the behaviour -- but the second
 *  half of it is still true and still worth saying: the archive's own manifest
 *  is what guard G-WIN reads, and this page has never seen one. */
export const WINDOW_REFUSAL_SENTENCE =
  "This page refuses a point outside the coverage above before anything is sent, and keeps " +
  "every value you typed. It is not the last word: the runner reads the archive's own " +
  "manifest and guard G-WIN refuses a point it does not cover, which is a narrower window " +
  "than this one when the archive holds less than the run recorded.";

// ------------------------ the topic subset, its mapping and the limits (PLAT-11.2)

/** The longest name a Kafka broker accepts for a topic.
 *
 *  249 AND NOT 255, and it is not this page's number: it is
 *  `logweir_core::guard::MAX_TOPIC_NAME_CHARS`, which the API's own
 *  `validate::is_topic_name` and the CRD's `TOPIC_NAME_PATTERN` both hold to.
 *  The two halves are pinned against each other by
 *  `ui/tests/fixtures/restore-limits.json`, which this page's suite reads and
 *  `crates/logweir-api/tests/resources.rs` compares with the Rust constants --
 *  so a drift in either language fails a test in both. */
export const MAX_TOPIC_NAME_CHARS = 249;

/** Whether `name` is a name a Kafka broker would accept: the same four ASCII
 *  classes `logweir_core::guard::topic_name_is_kafka_legal` admits, and the
 *  same bound. A character predicate rather than a pattern for that function's
 *  own reason -- a `.` that matches a newline cannot defeat it. */
export function isKafkaTopicName(name) {
  if (typeof name !== "string" || name.length === 0 || name.length > MAX_TOPIC_NAME_CHARS) {
    return false;
  }
  for (const c of name) {
    const ok =
      (c >= "a" && c <= "z") || (c >= "A" && c <= "Z") || (c >= "0" && c <= "9") ||
      c === "." || c === "_" || c === "-";
    if (!ok) {
      return false;
    }
  }
  return true;
}

/** THE ONE PLACE THE TOPIC PREFIX IS WRITTEN, and it writes BOTH keys.
 *
 *  THE RUNNER'S GRAMMAR CARRIES TWO, AND READS A DIFFERENT ONE PER MODE.
 *  `logweir_core::spec::target_topic_prefix` takes `target.topic_naming.prefix`
 *  for `newTopic` and `target.topic_mapping_prefix` for `scratch`. The wizard
 *  offers both modes in step 4, and until this function existed only the first
 *  was updated by an edit: `initialState` set the two equal once and the prefix
 *  input, a restored draft and an edit prefill each moved one of them. In
 *  `scratch` mode the preview, the declared mapping and the product API's rail
 *  therefore all used a prefix THE RUN DOES NOT USE -- a direct falsification
 *  of "submitted topics/mapping equal the preview" for that mode, found by the
 *  independent review and reproduced.
 *
 *  So there is one setter, every write goes through it, and the two keys are
 *  equal by construction rather than by three call sites remembering. Nothing
 *  else in this module assigns either key. */
export function setTopicPrefix(state, value) {
  const s = state || {};
  const target = (s.fields || {}).target;
  if (target === undefined || target === null) {
    return;
  }
  const next = typeof value === "string" ? value : "";
  target.topicPrefix = next;
  target.topicMappingPrefix = next;
}

/** The prefix the RUN will map through, for the mode this plan is in -- the
 *  JavaScript half of `logweir_core::spec::target_topic_prefix` over the
 *  domain this page can produce.
 *
 *  THE RUNNER HAS A THIRD ARM AND THIS HAS TWO, and the difference is
 *  unreachable here: `target_topic_prefix` falls back to
 *  `default_topic_prefix(<the recovery point>)` when a `newTopic` spec states
 *  no `topic_naming` at all, and `ui/plan.js` renders that key through
 *  `needed()`, which throws on an absent or empty value. So every document
 *  this wizard can render states a prefix, and the fallback arm describes a
 *  plan the page cannot emit.
 *
 *  It exists so the preview cannot be a statement about the other mode's key
 *  even if the two ever came apart again: `topicMapping` reads THIS, and a row
 *  asserts it equals what `renderPlanBytes` emits for the same fields in both
 *  modes. */
export function effectivePrefix(state) {
  const target = ((state || {}).fields || {}).target || {};
  return target.mode === "scratch"
    ? target.topicMappingPrefix
    : target.topicPrefix;
}

/** THE MAPPING RULE, AND THIS BUILD HAS NO OTHER ONE.
 *
 *  `logweir_core::spec::target_topic_prefix` gives the whole grammar: mode
 *  `newTopic` takes `target.topic_naming.prefix`, mode `scratch` takes
 *  `target.topic_mapping_prefix`, and NEITHER admits a per-topic rename. So a
 *  mapped name is a concatenation, this function is that concatenation, and
 *  every row the preview shows, every row the submit declares and every name
 *  phase 0 creates comes through here. A page with a second rule would be a
 *  page whose preview and whose submission could disagree. */
export function mappedTopicName(prefix, topic) {
  return String(prefix === undefined || prefix === null ? "" : prefix) + String(topic);
}

/** THE POINT'S FROZEN TOPIC LIST, and never a list this page assembled.
 *
 *  `Backup.spec.topics` is what the run froze -- for a dynamic selection it is
 *  what discovery resolved and the controller wrote back -- and a subset is
 *  only meaningful against it. A topic that is not in it is refused here and
 *  again by the readiness check's `plan.bindings` row
 *  (`PlanTopicsNotInRecoveryPoint`, D2 section 6.3). */
export function frozenTopicsOf(state) {
  const spec = (((state || {}).point) || {}).spec || {};
  return Array.isArray(spec.topics) ? spec.topics.slice() : [];
}

/** The subset this wizard will restore, in the frozen list's own order.
 *
 *  THE ORDER IS THE POINT'S AND NOT THE CLICK ORDER, because the plan bytes
 *  carry this list and a list that reordered itself with each click would
 *  change the plan hash -- and therefore the approval -- for no change of
 *  meaning. */
export function selectedTopics(state) {
  const s = state || {};
  const chosen = Array.isArray((s.fields || {}).topics) ? s.fields.topics : [];
  const wanted = new Set(chosen.map(String));
  const frozen = frozenTopicsOf(s);
  const inOrder = frozen.filter((t) => wanted.has(String(t)));
  // A SELECTION THE FROZEN LIST DOES NOT HOLD IS KEPT, NOT DROPPED. It is what
  // `mappingProblems` refuses by name; silently discarding it would leave the
  // page submitting a different subset from the one it was showing.
  const extra = chosen.filter((t) => frozen.indexOf(t) === -1);
  return inOrder.concat(extra);
}

/** The exact source-to-target mapping, row by row: what the preview shows and
 *  what the submit declares, from ONE call. */
export function topicMapping(state) {
  const prefix = effectivePrefix(state);
  return selectedTopics(state).map((source) => ({
    source: source,
    target: mappedTopicName(prefix, source),
  }));
}

/** The page's refusals over the subset and the mapping, by input -- every one
 *  of them made BEFORE anything is sent, and every one naming the value an
 *  operator has to go and fix.
 *
 *  FIVE REFUSALS, AND THE SAME FIVE THE PRODUCT API MAKES
 *  (`crates/logweir-api/src/routes/restores.rs::validate_topic_mapping`):
 *  an empty subset, a topic the point did not freeze, a duplicate mapping
 *  naming BOTH rows, an illegal prefix, and a mapped name a broker would
 *  refuse. The duplicate can only be a repeated source, because a prefix map
 *  over distinct sources is injective -- there is no rename in the grammar. */
export function mappingProblems(state) {
  const s = state || {};
  const prefix = effectivePrefix(s);
  const problems = Object.create(null);
  const frozen = frozenTopicsOf(s);
  const chosen = selectedTopics(s);
  // THE DUPLICATE CHECK READS THE RAW LIST, NOT THE CANONICAL ONE, AND THAT
  // IS THE WHOLE REASON IT IS REACHABLE. `selectedTopics` filters the frozen
  // list, so it collapses a repeated entry to one -- and a page that silently
  // deduplicated would submit a list that is not the list it was given, which
  // is exactly the class of surprise this task exists to remove. So the raw
  // array is what is checked, and a duplicate is REFUSED rather than quietly
  // fixed. (A checkbox cannot produce one; a restored draft, an "edit this
  // restore" prefill and a caller constructing a state can.)
  const raw = Array.isArray((s.fields || {}).topics) ? s.fields.topics : [];
  const rawSeen = new Set();
  for (const source of raw) {
    const key = String(source);
    if (rawSeen.has(key)) {
      problems.topics =
        "`" + key + "` and `" + key + "` both map to the target topic `" +
        mappedTopicName(prefix, key) + "`; one restore cannot write two source topics into " +
        "one target, and the mapping rule is a prefix, so a duplicate target can only be a " +
        "duplicate source";
      return problems;
    }
    rawSeen.add(key);
  }
  if (chosen.length === 0) {
    problems.topics =
      "choose at least one topic: a restore of no topic is not a restore, and the plan's " +
      "grammar has no empty list";
    return problems;
  }
  const stranger = chosen.find((t) => frozen.indexOf(t) === -1);
  if (stranger !== undefined) {
    problems.topics =
      "`" + String(stranger) + "` is not in this recovery point's frozen topic list, so this " +
      "archive holds nothing for it; the readiness check refuses the same plan with " +
      "PlanTopicsNotInRecoveryPoint";
    return problems;
  }
  const seen = new Map();
  for (const source of chosen) {
    const target = mappedTopicName(prefix, source);
    const first = seen.get(target);
    if (first !== undefined) {
      problems.topics =
        "`" + String(first) + "` and `" + String(source) + "` both map to the target topic `" +
        target + "`; one restore cannot write two source topics into one target";
      return problems;
    }
    seen.set(target, source);
  }
  if (typeof prefix !== "string" || prefix.length === 0) {
    problems.topicPrefix =
      "the prefix every restored topic's name starts with. An empty prefix maps every topic " +
      "onto itself, which is a restore writing over the topic it came from";
    return problems;
  }
  if (!isKafkaTopicName(prefix)) {
    problems.topicPrefix =
      "`" + prefix + "` is not a name a broker accepts: letters, digits, '.', '_' and '-' " +
      "only, and at most " + String(MAX_TOPIC_NAME_CHARS) + " characters";
    return problems;
  }
  for (const source of chosen) {
    const target = mappedTopicName(prefix, source);
    if (!isKafkaTopicName(target)) {
      problems.topicPrefix =
        "the mapped name for `" + String(source) + "` is `" + target + "`, which is not a name " +
        "a broker accepts (at most " + String(MAX_TOPIC_NAME_CHARS) + " characters); shorten " +
        "the prefix";
      return problems;
    }
    if (target === source) {
      problems.topicPrefix =
        "this prefix maps `" + String(source) + "` onto itself; a restore writes to a NEW " +
        "topic and the target must differ from the source";
      return problems;
    }
  }
  return problems;
}

/** The subset control and the mapping preview: one checkbox per frozen topic,
 *  and the exact name each selected topic becomes. */
export function renderTopicSubset(state) {
  const s = state || {};
  const frozen = frozenTopicsOf(s);
  const chosen = new Set(selectedTopics(s).map(String));
  const errors = errorsOf(s);
  const problems = mappingProblems(s);
  const rows = topicMapping(s).map((row) => [esc(row.source), "<code>" + esc(row.target) + "</code>"]);
  const boxes = frozen
    .map(
      (topic, i) =>
        "<li><label><input type=\"checkbox\" class=\"topic-box\" " +
        "id=\"topic-" + String(i) + "\" data-topic=\"" + esc(topic) + "\"" +
        (chosen.has(String(topic)) ? " checked" : "") + "> " + esc(topic) + "</label></li>",
    )
    .join("");
  return (
    "<h4 id=\"topic-subset\">Topics to restore</h4>" +
    "<p class=\"blurb\">" + esc(SUBSET_SENTENCE) + "</p>" +
    (frozen.length === 0
      ? "<p class=\"note\" id=\"no-frozen-topics\">" + esc(NO_FROZEN_TOPICS) + "</p>"
      : "<ul class=\"topic-subset\">" + boxes + "</ul>") +
    "<div class=\"actions\">" +
    "<button type=\"button\" id=\"select-all-topics\">Select all</button>" +
    "<button type=\"button\" id=\"select-no-topics\">Clear</button>" +
    "</div>" +
    fieldErrorLine("topic-subset", errors.topics) +
    // THE REFUSAL IS COMPUTED HERE AND SHOWN ON THE KEYSTROKE, not only after
    // a submit has been refused. `errorsOf` carries what the SERVER said about
    // the last attempt; a page that only rendered those would leave an
    // operator looking at a mapping this page has already decided it will not
    // send. The window refusal in step 3 works the same way, for the same
    // reason (PLAT-11.1).
    (typeof problems.topics === "string"
      ? "<p class=\"complaint\" id=\"subset-complaint\">" + esc(problems.topics) + "</p>"
      : "") +
    "<h4>The mapping, before you submit</h4>" +
    "<p class=\"blurb\">" + esc(MAPPING_SENTENCE) + "</p>" +
    table(["SOURCE TOPIC", "TARGET TOPIC"], rows, "No topic is selected, so nothing is mapped.") +
    "<p class=\"note\" id=\"mapping-identity\">" + esc(MAPPING_RULE_SENTENCE) + "</p>"
  );
}

/** What a subset IS, said where it is chosen. */
export const SUBSET_SENTENCE =
  "Every topic this recovery point froze. Untick the ones this restore must not write: the " +
  "plan carries exactly the list ticked here, and the archive's other topics are not read.";

/** Printed when the chosen point froze no topic list at all. */
export const NO_FROZEN_TOPICS =
  "this recovery point records no frozen topic list, so there is no subset to choose from and " +
  "no mapping this page can show. Choose another point.";

/** What the table beneath the boxes is. */
export const MAPPING_SENTENCE =
  "The exact name each selected topic becomes on the target. These rows are what the plan " +
  "carries and what the create request declares: the product API recomputes every one of them " +
  "from the prefix it stores and refuses the request if a row disagrees.";

/** The rule itself, said once, with its owner named. */
export const MAPPING_RULE_SENTENCE =
  "The rule is the prefix and nothing else (logweir_core::spec::target_topic_prefix): there is " +
  "no per-topic rename in this version. Two source topics therefore cannot map to one target " +
  "name unless the same topic is listed twice, which is refused here and again by the API.";

// ------------------------------------------------- what recovery changes (D3 3.5)

/** Why a pre-run partition count is not shown, and who owes it.
 *
 *  A PROJECTION GAP, NAMED RATHER THAN GUESSED. Per-topic partition counts
 *  exist in this product only AFTER a run -- `Restore.status.completion
 *  .newTopics[].partitions`, built from `target_diff.would_create` -- and
 *  neither `Backup.status` nor the product API's `Backup` projection carries
 *  the archive manifest's counts. The runner reads the manifest; this page has
 *  never seen one. So the count is absent and said to be absent, which is the
 *  difference between this line and a number a page invented. */
export const PARTITION_COUNT_NOT_PUBLISHED =
  "Partition counts are not shown before the run: this build publishes a per-topic partition " +
  "count only after one, on the Restore's own completion (status.completion.newTopics[].partitions, " +
  "from the target diff). The archive manifest holds the source counts and the runner reads it; " +
  "no field of a Backup or of its product-API projection carries them, so there is nothing here " +
  "to read and this page will not guess. A topic discovery publishes a live partition count for a " +
  "SOURCE CLUSTER, which is a different fact: it is a probe of the cluster now, not the manifest's " +
  "count at this recovery point, and it hangs off a connection rather than off this Backup.";

/** The replication factor every created topic is asked for, from the PLAN's
 *  own field. `target.default_replication_factor` in the runner's grammar,
 *  defaulted by `logweir_core::spec::rf1`; the broker refuses a factor above
 *  its broker count and the readiness check says so first
 *  (`ReplicationFactorExceedsBrokers`, D2 section 6.3). */
export function replicationFactorOf(state) {
  const rf = (((state || {}).fields || {}).target || {}).replicationFactor;
  return typeof rf === "number" ? rf : null;
}

/** The verification this restore will perform, from the PLAN's sample block --
 *  and never the word "exhaustive".
 *
 *  D3 section 3.5's rule is that the counts are labelled exactly and that the
 *  last clause is not optional: a sampled comparison presented without it
 *  reads as a full one. Before the run there are no counts, so what is said is
 *  what was ASKED FOR -- `sample.records_per_partition` and `sample.anchor`,
 *  both plan fields -- with the same closing clause `verificationScopeSentence`
 *  ends with after the run. No level in this version compares every record. */
export function verificationPlanSentence(state) {
  const sample = ((state || {}).fields || {}).sample || {};
  const n = typeof sample.recordsPerPartition === "number" ? String(sample.recordsPerPartition) : "?";
  const anchor = typeof sample.anchor === "string" ? sample.anchor : "?";
  return (
    "This restore will compare " + n + " records per partition, anchored at " + anchor +
    ", inside the sample window above. That is a sampled check, not an exhaustive comparison: " +
    "no level in this version compares every restored record, and the result will say so beside " +
    "its counts."
  );
}

/** Resume: not implemented, said before the run and not after it.
 *
 *  PLAT-11's own scope line puts "advanced in-place recovery and crash resume"
 *  in product expansion, and the runner has no resume: a Restore that fails
 *  part way is a failed Restore, and the way forward is a NEW one to a fresh
 *  target (which is what this wizard's retry does). Saying so here is the
 *  migration note PLAT-11.2 asks for -- "clearly identify unimplemented
 *  resume" -- in the one place an operator decides to start a restore. */
export const RESUME_NOT_IMPLEMENTED =
  "Resume is not implemented. A restore that fails part way through cannot be continued from " +
  "where it stopped: there is no checkpoint to resume from, the topics it had already created " +
  "stay as they are, and the way forward is a new restore to a fresh target. Advanced in-place " +
  "recovery and crash resume are product expansion, not this version.";

/** The limits panel: what recovery changes, and what it does not. */
export function renderRecoveryLimits(state) {
  const s = state || {};
  const mode = String(((s.fields || {}).target || {}).mode);
  const cutover = COMPLETION_GUIDANCE[mode];
  const meaning = TARGET_MODE_MEANING[mode];
  return (
    "<h4 id=\"recovery-limits\">What this recovery changes, and what it does not</h4>" +
    facts([
      ["target replication factor", cell(replicationFactorOf(s))],
      ["target partition counts", cell(null)],
      ["target mode", cell(mode)],
    ]) +
    "<p class=\"note\" id=\"partition-counts\">" + esc(PARTITION_COUNT_NOT_PUBLISHED) + "</p>" +
    "<p class=\"scope\" id=\"verification-plan\">" + esc(verificationPlanSentence(s)) + "</p>" +
    (typeof meaning === "string"
      ? "<p class=\"note\" id=\"target-mode-meaning\">" + esc(meaning) + "</p>"
      : "") +
    (typeof cutover === "string"
      ? "<p class=\"guidance\" id=\"cutover-limit\" data-target-mode=\"" + esc(mode) + "\">" +
        esc(cutover) + "</p>"
      : "<p class=\"note\" id=\"cutover-limit\">" + esc(NO_CUTOVER_GUIDANCE) + "</p>") +
    "<p class=\"note\" id=\"resume-limit\">" + esc(RESUME_NOT_IMPLEMENTED) + "</p>"
  );
}

/** Said when the mode is not one this version knows, so no fixed sentence
 *  applies. The page names the gap rather than picking a sentence. */
export const NO_CUTOVER_GUIDANCE =
  "this plan names no target mode this version knows, so no cutover guidance is shown: the " +
  "guidance depends on which mode it uses and this page will not guess one.";

/** Step 4 -- target and naming. EVERY `KafkaCluster` in the namespace with its
 *  role beside it, the two modes `TargetMode` accepts and nothing else, and
 *  the prefix PREFILLED with `default_topic_prefix`'s own output for the
 *  chosen instant.
 *
 *  EVERY CLUSTER, BECAUSE THE RUNNER'S GUARD IS THE GATE AND THIS IS NOT.
 *  Until Task 28a this select was built from `role === "target"` alone, so a
 *  namespace with one `role: source` cluster -- which is what Demo 1 is, and
 *  what `scripts/k8s-demo.sh` runs a `newTopic` restore against, green --
 *  rendered an EMPTY select, left `target.bootstrapServers` empty, and made
 *  the plan grammar throw. The page was refusing what the product supports.
 *  So: list them all, say what each is labelled, preselect the sensible one,
 *  and let phase 0 decide. */
export function renderTargetStep(state) {
  const s = state || {};
  const fields = s.fields || {};
  const target = fields.target || {};
  const clusters = itemsOf(s.clusters);
  const chosen = targetCluster(s);
  const labelled = clusters.some((c) => ((c.spec || {}).role) === "target");
  const options = TARGET_MODES.map(
    (mode) =>
      "<option value=\"" + esc(mode) + "\"" +
      (target.mode === mode ? " selected" : "") +
      ">" + esc(mode) + "</option>",
  ).join("");
  const current = effectivePrefix(s);
  const prefix =
    typeof current === "string" && current.length > 0 ? current : defaultPrefixFor(s);
  const mapping = mappingProblems(s);
  const markerWarning =
    target.mode === "scratch" && typeof ((chosen || {}).spec || {}).markerTopic !== "string"
      ? "<p class=\"complaint\">" + SCRATCH_MARKER_WARNING + "</p>"
      : "";
  const errors = errorsOf(s);
  return (
    "<section class=\"step\" id=\"step-target\" tabindex=\"-1\">" +
    "<h3>4. Target, topic subset and naming</h3>" +
    "<p class=\"blurb\">Where the restored records are written. Nothing that already " +
    "exists is written to: a Restore only ever creates topics that did not exist, and " +
    "refuses outright if a mapped target topic is already there.</p>" +
    renderClusterSelector({
      id: "target-cluster",
      name: "targetCluster",
      label: "target cluster",
      help: "Every saved connection in this namespace, with its capability label and its own " +
        "connection probe. Chosen by uid: a rename keeps this selection, a delete-and-recreate " +
        "under the same name is refused.",
      prefer: "target",
      clusters: s.clusters,
      selection: { uid: s.targetClusterUid, name: s.targetClusterName },
      now: s.now,
      freshSeconds: s.freshSeconds,
      errors: errors,
    }) +
    fieldErrorLine("target-cluster", errors.targetCluster) +
    "<div class=\"field\"><label for=\"target-mode\">mode</label>" +
    "<select id=\"target-mode\" name=\"mode\"" + invalidAttributes("target-mode", errors.mode) + ">" +
    options + "</select>" +
    "<p class=\"help\">newTopic writes beside what is there; scratch needs a target that " +
    "proves it is scratch.</p>" + fieldErrorLine("target-mode", errors.mode) + "</div>" +
    (labelled ? "" : "<p class=\"note\">" + TARGET_ROLE_SENTENCE + "</p>") +
    markerWarning +
    "<div class=\"field\"><label for=\"topic-prefix\">topicNaming.prefix</label>" +
    "<input id=\"topic-prefix\" name=\"topicPrefix\" value=\"" + esc(prefix) + "\"" +
    invalidAttributes("topic-prefix", errors.topicPrefix) + ">" +
    fieldErrorLine("topic-prefix", errors.topicPrefix) +
    (typeof mapping.topicPrefix === "string"
      ? "<p class=\"complaint\" id=\"prefix-complaint\">" + esc(mapping.topicPrefix) + "</p>"
      : "") +
    "<p class=\"note\">The prefix defaults to what logweir_core::spec::default_topic_prefix " +
    "produces for this instant, so a topic name says both what it is and what point it was " +
    "recovered to. It is editable.</p></div>" +
    renderTopicSubset(s) +
    renderRecoveryLimits(s) +
    "</section>"
  );
}

/** What a restore readiness check is bound to, and what invalidates it. */
export const READINESS_BINDING_SENTENCE =
  "A readiness result is bound to the EXACT plan on screen. Edit the point in time, the target, " +
  "the topic prefix or anything else that changes the document, and the hash changes with it -- " +
  "so the result stops describing what you are about to submit and is shown as out of date " +
  "rather than as a verdict. Run it again against the new plan.";

/** Why this step sends an inline source archive rather than a destination, and
 *  who owes the field that would change that.
 *
 *  D2 section 9 asks step 1 to take the source destination from the recovery
 *  point's FROZEN destination and match it by `locationDigest`. The CRD has
 *  the field (`weirkeeper/src/crds/backup.rs`); the product API's `Backup`
 *  projection publishes neither `destinationRef` nor `locationDigest`, so
 *  there is nothing here to read. It is a projection gap, not a model gap. */
export const SOURCE_DESTINATION_NOT_PUBLISHED =
  "This check reads the archive from the recovery point's own inline URL. A destination-backed " +
  "point would let it name the saved destination instead and match the frozen locationDigest, " +
  "which is what D2 asks for -- but this build's product API publishes neither destinationRef " +
  "nor locationDigest on a Backup. PLAT-08.2 (D2 W10) owes that projection; until it lands this " +
  "step cannot tell you which saved destination a recovery point came from.";

/** Why a readiness result is not a promise about the run. */
export const READINESS_CAVEAT_SENTENCE =
  "A ready verdict says these checks passed when they ran. It is not a promise about the run: a " +
  "credential can be rotated, a topic created and an ACL changed in the minute after it, and the " +
  "execution-time guards remain the authority whatever this says.";

/** Step 5 -- operation readiness for this exact plan.
 *
 *  WHAT CHANGED IN D2, AND WHY IT IS A DIFFERENT KIND OF THING. This step used
 *  to show the TARGET CLUSTER'S probe status and one sentence about what the
 *  run would attempt. Neither was a check of this restore: a probe is the
 *  controller's own periodic dial of the brokers, and the sentence was a
 *  description of an intention. An operator reading a step called "preflight"
 *  and seeing "reachable: true" could reasonably have concluded that the
 *  restore had been checked, and nothing had been.
 *
 *  Now the step carries a real `Preflight`, bound to this plan's hash, whose
 *  verdict comes from a check Job's own recorded result. The cluster status
 *  stays, because it is useful context, but it is labelled as what it is and
 *  it is no longer the thing the step is about.
 *
 *  THE STALE BANNER IS COMPUTED HERE FROM THE HASH ON SCREEN, and it is the
 *  SECOND of two guards. The first is the product API's: `GET
 *  .../preflights/{id}?planHash=` recomputes `applicable` and `staleReasons`
 *  server side against the plan the caller is looking at. This one exists
 *  because the wizard changes the plan locally, without asking anything, and
 *  an operator editing a field must see the result go stale on that keystroke
 *  and not on the next round trip. */
export function renderPreflightStep(state, prepared) {
  const s = state || {};
  const p = prepared || {};
  const cluster = targetCluster(s);
  const status = (cluster || {}).status || {};
  const topics = (s.fields || {}).topics || [];
  const readiness = s.readiness || {};
  const result = readiness.preflight || null;
  const record = readiness.state || {};
  const pending = record.phase === "pending";
  const currentHash = typeof p.hash === "string" ? p.hash : "";
  // THE RESULT IS ABOUT THE PLAN IT WAS STARTED FOR, and `boundHash` is that
  // plan's hash as this page recorded it when the check was started. The
  // binding the server publishes is compared too -- either disagreeing with
  // the plan on screen is staleness.
  const boundHash = typeof readiness.boundHash === "string" ? readiness.boundHash : "";
  const serverHash = ((result || {}).binding || {}).planHash;
  const edited = result !== null && currentHash.length > 0 &&
    ((boundHash.length > 0 && boundHash !== currentHash) ||
      (typeof serverHash === "string" && serverHash.length > 0 && serverHash !== currentHash));
  return (
    "<section class=\"step\" id=\"step-preflight\" tabindex=\"-1\"><h3>5. Operation readiness</h3>" +
    "<p class=\"blurb\">A readiness check for THIS plan, plus the target cluster's own most " +
    "recent probe as context.</p>" +
    "<p class=\"note\">" + esc(READINESS_BINDING_SENTENCE) + "</p>" +
    (readiness.unavailable === true
      ? "<p class=\"note\" id=\"restore-readiness-unavailable\">" +
        cell(readiness.unavailableReason) + "</p>"
      : "<form id=\"restore-readiness-form\" novalidate" +
        (pending ? " aria-busy=\"true\"" : "") + ">" +
        "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
        "<div class=\"actions\">" +
        "<button type=\"submit\" id=\"restore-readiness-start\">" +
        (result === null ? "Check readiness" : "Check this plan again") + "</button>" +
        (result !== null && result.terminal === false
          ? "<button type=\"button\" id=\"restore-readiness-cancel\">Cancel</button>"
          : "") +
        "</div></fieldset>" +
        "<div class=\"form-status\" id=\"restore-readiness-status\" tabindex=\"-1\">" +
        mutationStatus(record, { kind: "Preflight", name: (result || {}).id || "" }, null) +
        "</div></form>") +
    (edited
      ? "<div class=\"stale-banner\" id=\"readiness-stale\" role=\"alert\">" +
        badge("unverified", "out of date: the plan changed") +
        "<p class=\"note\">This result was produced for plan <code>" +
        esc(boundHash.length > 0 ? boundHash : String(serverHash || "")) + "</code>. The plan on " +
        "screen now hashes to <code>" + esc(currentHash) + "</code>, so the verdict below is " +
        "about a document you are no longer about to submit. Run the check again.</p></div>"
      : "") +
    (result === null
      ? "<p class=\"note\" id=\"readiness-none\">No readiness check has run for this plan. " +
        "Nothing below claims this restore will work.</p>"
      : renderPreflight(result)) +
    "<p class=\"note\">" + esc(READINESS_CAVEAT_SENTENCE) + "</p>" +
    "<p class=\"note\" id=\"readiness-source-destination\">" +
    esc(SOURCE_DESTINATION_NOT_PUBLISHED) + "</p>" +
    "<h4>Target cluster probe (context, not a verdict)</h4>" +
    facts([
      ["target cluster", cell((((cluster || {}).metadata) || {}).name)],
      ["reachable", cell(status.reachable)],
      ["cluster id", cell(status.clusterId)],
      ["observed at", cell(status.observedAt)],
      ["reason", cell(status.reason)],
    ]) +
    "<p class=\"preflight\">" + preflightSentence(topics.length) + "</p>" +
    "</section>"
  );
}

// --------------------------------- the readiness gate before a submit (PLAT-11.2)

/** Why the submit is refused by the readiness check on screen, or `null`.
 *
 *  THIS IS THE CLAUSE "NO EXISTING TARGET TOPIC IS OVERWRITTEN BY THE ORDINARY
 *  PATH", MADE INTO A REFUSAL THIS PAGE MAKES FIRST. The collision itself is
 *  detected server-side and always was -- `target.mappedTopics` answers
 *  `MappedTopicExists` (D2 section 6.3, 5 m expiry) and the runner's phase 0
 *  refuses the run whatever a console does -- but until this function existed
 *  the wizard rendered that `notReady` verdict and then let the operator spend
 *  an approver's signature on the plan anyway. A refusal that is displayed and
 *  not enforced is a refusal an incident walks straight past.
 *
 *  FIVE ARMS, IN THE ORDER AN OPERATOR MEETS THEM:
 *
 *   1. NOTHING HAS RUN for this plan -- which is also legacy mode, whose
 *      absent `Preflight` route leaves the result absent.
 *   2. THE RESULT IS ABOUT ANOTHER PLAN -- the prefix changed, the subset
 *      changed, the point in time changed. Any edit that moves the plan bytes
 *      moves the hash, and D2 section 6.6's invalidation rule says the verdict
 *      stops describing what is about to be submitted. A change of TARGET is
 *      handled one step earlier, in `selectTarget`, because two clusters can
 *      render identical bytes.
 *   3. IT HAS BEEN INVALIDATED (`stale: true`) -- an expiry, a referent that
 *      moved, a policy or CA that changed. Asked BEFORE completion, because an
 *      invalidated check is about inputs that are no longer these whether or
 *      not it has finished. (`target.mappedTopics` expires in five minutes,
 *      the shortest budget in the catalogue and exactly the check a slow
 *      review outlives.)
 *   4. IT HAS NOT FINISHED.
 *   5. IT FINISHED WITHOUT BECOMING APPLICABLE.
 *   6. THE VERDICT IS NOT `ready` -- which is where a collision lands, named
 *      by its own check id and code.
 *
 *  Pure: no DOM, no network, no clock. The freshness judgement is the
 *  SERVER's (`applicable`/`stale`, recomputed on every GET against the caller's
 *  own plan hash), never a comparison this page makes against a browser clock. */
export function readinessRefusal(state, prepared) {
  const s = state || {};
  const p = prepared || {};
  const readiness = s.readiness || {};
  const result = readiness.preflight || null;
  const hash = typeof p.hash === "string" ? p.hash : "";
  if (result === null) {
    // NOT A REFUSAL, AND DELIBERATELY NOT ONE. D2 section 6.3's rule is about
    // a verdict that has stopped applying, not about the absence of one, and
    // the readiness check is advisory by construction: the runner's phase 0 is
    // the authority and refuses a collision whether or not a console asked
    // first. Refusing here would also make every restore in this product
    // conditional on a route legacy mode does not have. So an unchecked plan
    // may be submitted, and step 6 says in words that nothing has looked for
    // an existing target topic yet.
    return null;
  }
  const boundHash = typeof readiness.boundHash === "string" ? readiness.boundHash : "";
  const serverHash = ((result.binding || {}).planHash);
  const elsewhere =
    hash.length > 0 &&
    ((boundHash.length > 0 && boundHash !== hash) ||
      (typeof serverHash === "string" && serverHash.length > 0 && serverHash !== hash));
  if (elsewhere) {
    return (
      "the readiness check on screen was produced for plan " +
      (boundHash.length > 0 ? boundHash : String(serverHash || "")) +
      " and this plan hashes to " + hash + ". Something that changes the document -- the " +
      "target, the prefix, the subset, the point in time -- changed since it ran, so it is " +
      "not a verdict about what you are about to submit. Run it again."
    );
  }
  // STALENESS IS ASKED ABOUT BEFORE COMPLETION, and the order matters. A check
  // that has been INVALIDATED -- by a target swap, an expiry, a referent that
  // moved -- is about inputs that are no longer these, whether or not it has
  // finished: waiting for its verdict would be waiting for an answer to the
  // old question. `stale` is the field that says exactly that, and the product
  // API sets it only when it means it; `applicable: false` ALONE does not, and
  // is checked below, because it is also what every freshly started check
  // reports before it completes.
  if (result.stale === true) {
    const reasons = (Array.isArray(result.staleReasons) ? result.staleReasons : [])
      .map((r) => staleReasonLine(r))
      .join("; ");
    return (
      "the readiness check for this plan no longer applies to the inputs it was run against" +
      (reasons.length > 0 ? " (" + reasons + ")" : "") +
      ". Run it again before submitting."
    );
  }
  if (result.terminal === false) {
    return "the readiness check for this plan has not finished; wait for its verdict.";
  }
  if (result.applicable !== true) {
    return (
      "the readiness check for this plan finished without becoming applicable to the inputs " +
      "it was run against. Run it again before submitting."
    );
  }
  if (result.state !== "ready") {
    const failing = (Array.isArray(result.checks) ? result.checks : [])
      .filter((c) => (c || {}).gating === "blocking" && (c || {}).state !== "ready")
      .map((c) => String(c.id) + " (" + String(c.code) + ")")
      .join(", ");
    return (
      "the readiness check for this plan is " + String(result.state) + " and not ready" +
      (failing.length > 0 ? ": " + failing : "") +
      ". Nothing is sent while a blocking check refuses this plan."
    );
  }
  return null;
}

/** Said when no readiness check has run for the plan on screen.
 *
 *  A WARNING AND NOT A REFUSAL -- see [`readinessRefusal`]'s absent-result arm
 *  for why. It still says the one thing an operator needs to know before
 *  clicking: nothing has looked for an existing target topic yet. */
export const READINESS_NOT_RUN_WARNING =
  "No readiness check has run for this plan, so nothing has looked for an existing target topic " +
  "with a mapped name, for a reachable target, or for an approver key that outlives the " +
  "deadline. The restore may still be created: the runner's phase 0 refuses a mapped topic that " +
  "already exists and nothing is overwritten either way. Run the check in step 5 to find out " +
  "before an approver signs rather than after.";

/** Step 6 -- the rendered plan, its hash, the two minted names, and the one
 *  guided submit.
 *
 *  `prepared.problem` is set instead of the bytes when the fields do not make
 *  a document the runner's grammar accepts; then there is no hash to show and
 *  nothing to submit, and the step says why. `state.submission` is the
 *  wizard's mutation record: while it is pending the button is disabled, and
 *  its outcome is shown beside the button in the words every form uses. */
export function renderPlanStep(prepared, state) {
  const p = prepared || {};
  const s = state || {};
  const submission = s.submission || {};
  const pending = submission.phase === "pending";
  // THE READINESS GATE, ON THE BUTTON AND IN WORDS BESIDE IT (PLAT-11.2). A
  // disabled control with no sentence is a page that has stopped working for
  // a reason it will not say, so the refusal is rendered whether or not the
  // reader can see the button's state.
  const blocked = readinessRefusal(s, p);
  const errors = errorsOf(s);
  const beside = (s.errorsUnmatched || []).concat(
    NOT_INPUTS.reduce((all, name) => all.concat(errors[name] || []), []),
  );
  const renderable = typeof p.problem !== "string";
  const plan = renderable
    ? "<pre class=\"plan-bytes\" id=\"plan-bytes\">" + esc(p.bytes) + "</pre>"
    : "<p class=\"complaint\" id=\"plan-problem\">The plan cannot be rendered from these values, " +
      "so there is no hash and nothing to submit: " + esc(p.problem) + "</p>";
  return (
    "<section class=\"step\" id=\"step-plan\" tabindex=\"-1\"><h3>6. Plan, hash and names</h3>" +
    "<p class=\"blurb\">The document an approver signs, exactly as it will be sent, with " +
    "its sha256 and the two names minted from it.</p>" +
    plan +
    facts([
      ["plan hash", renderable ? "<code id=\"plan-hash-value\">" + esc(p.hash) + "</code>" : cell(null)],
      ["Restore metadata.name", renderable ? "<code>" + esc(p.restoreName) + "</code>" : cell(null)],
      ["Approval metadata.name", renderable ? "<code>" + esc(p.approvalName) + "</code>" : cell(null)],
    ]) +
    "<p class=\"note\">Both names are minted from the plan bytes before either object " +
    "exists. The Restore is created first, naming an Approval that is not there yet; the " +
    "reconciler requeues every 30 s until it arrives. Neither name is ever edited, because " +
    "neither spec can be.</p>" +
    "<div class=\"actions\">" +
    "<button type=\"button\" id=\"copy-plan\"" + (renderable ? "" : " disabled") + ">Copy plan</button>" +
    "<button type=\"button\" id=\"download-plan\"" + (renderable ? "" : " disabled") + ">Download plan</button>" +
    "</div>" +
    "<p class=\"caveat\">" + esc(COPY_CAVEAT) + "</p>" +
    "<h4>Approve it out of band</h4>" +
    "<p class=\"note\">Run this on the machine that holds the approver's private key. This " +
    "page never sees it.</p>" +
    copyBlock([APPROVE_COMMAND]) +
    "<div class=\"actions actions-final\">" +
    "<button type=\"button\" id=\"create-restore\" class=\"primary\"" +
    (pending || !renderable || blocked !== null ? " disabled" : "") +
    (pending ? " aria-busy=\"true\"" : "") +
    ">Create the Restore</button>" +
    "</div>" +
    // ONE ARM FOR "NOTHING HAS RUN", AND IT CARRIES LEGACY MODE TOO. An earlier
    // draft had a second arm keyed on `state.readiness.unavailable`, which
    // NOTHING in this wizard ever sets -- a refusal-bypass and a sentence no
    // reader could reach. Legacy mode has no readiness route, so its result is
    // absent, so it lands here and reads the same true sentence.
    (blocked === null
      ? ((s.readiness || {}).preflight
        ? ""
        : "<p class=\"note\" id=\"readiness-not-run\">" +
          esc(READINESS_NOT_RUN_WARNING) + "</p>")
      : "<p class=\"complaint\" id=\"readiness-blocked\" role=\"alert\">Nothing is sent: " +
        esc(blocked) + "</p>") +
    "<p class=\"note\">" + GUIDED_SUBMIT_SENTENCE + "</p>" +
    "<div class=\"form-status\" id=\"restore-submit-status\" tabindex=\"-1\">" +
    submissionStatus(submission, p, beside, s.ns) +
    "</div>" +
    "</section>"
  );
}

/** What the one submit button does, said beside it. */
export const GUIDED_SUBMIT_SENTENCE =
  "Create the Restore sends exactly the plan above, then opens what the Restore needs next: its " +
  "approval page while it waits for a verified Approval, or its operation view once one " +
  "authorises it. Submitting this plan again -- a second click, a retry after a lost response, " +
  "or the same plan after a reload -- never creates a second Restore, because its name is minted " +
  "from these bytes.";

/** Whether an outcome is about a plan this page is no longer showing.
 *
 *  An attempt records the plan it sent (`about`, see `createMutation`). A
 *  field edited while that attempt was outstanding leaves the two apart: the
 *  page then shows one plan and holds an outcome about another, and every
 *  sentence that says "submit again" is false of it. An attempt that recorded
 *  nothing (there is none in this file, and a caller may still pass one) is
 *  treated as being about what is shown, which is the older behaviour. */
export function outcomeIsElsewhere(submission, prepared) {
  const about = ((submission || {}).about) || {};
  const hash = (prepared || {}).hash;
  if (typeof about.hash !== "string" || about.hash.length === 0) {
    return false;
  }
  return typeof hash !== "string" || about.hash !== hash;
}

/** The name the attempt these words are about actually sent, which is not the
 *  name on screen once a field has changed. */
function submittedName(submission, prepared) {
  const about = ((submission || {}).about) || {};
  return typeof about.restoreName === "string" && about.restoreName.length > 0
    ? about.restoreName
    : (prepared || {}).restoreName;
}

function submissionStatus(submission, prepared, beside, ns) {
  const s = submission || {};
  const settled = s.phase === "succeeded" || s.phase === "failed";
  // THE OUTCOME MAY BE ABOUT A PLAN THAT IS NO LONGER ON SCREEN. `about` is
  // absent only before the first attempt of this page's life, so an outcome
  // that carries none is treated as being about what is shown.
  const elsewhere = settled && outcomeIsElsewhere(s, prepared);
  const name = submittedName(s, prepared);
  const aside = elsewhere
    ? "<p class=\"note\" id=\"submitted-elsewhere\">These words are about Restore <code>" +
      esc(name) + "</code>, the plan that was submitted -- not the plan shown above, which the " +
      "fields have changed since. Submitting now creates <code>" +
      esc((prepared || {}).restoreName) + "</code> instead.</p>"
    : "";
  if (s.phase === "succeeded") {
    const result = s.result || {};
    const meta = ((result.object || {}).metadata) || {};
    const shown = typeof meta.name === "string" && meta.name.length > 0 ? meta.name : name;
    // THE DURABLE LINK IS SHOWN EVEN WHEN THE DRAFT MOVED ON. An outcome about
    // another plan is exactly the case where the object would otherwise never
    // be mentioned again, and the one page that says whether it exists and
    // where it stands is its own operation view -- not the next step the
    // submit had chosen for a plan this page is no longer showing.
    const route = elsewhere || typeof result.route !== "string" || result.route.length === 0
      ? restoreOperationRoute(ns, shown)
      : result.route;
    return (
      mutationStatus(s, { kind: "Restore", name: shown }) +
      "<p class=\"note\"><a href=\"" + esc(route) + "\">Open Restore " + esc(shown) + "</a></p>" +
      aside
    );
  }
  return (
    mutationStatus(s, { kind: "Restore", name: name, resubmits: !elsewhere }, beside) +
    (elsewhere
      ? aside + "<p class=\"note\"><a href=\"" + esc(restoreOperationRoute(ns, name)) +
        "\">Open Restore " + esc(name) + " to see whether it exists</a></p>"
      : "")
  );
}

/** The field messages the wizard state carries, by input. */
function errorsOf(state) {
  return (((state || {}).errors) || {});
}

/** The exact command an approver runs, verbatim.
 *
 *  `--out` is SHOWN rather than left implicit because it defaults to
 *  `approval.json` in the caller's own working directory: an approver who did
 *  not know that has written two files somewhere they did not expect, and the
 *  sidecar lands beside `--out` with the extension replaced. The command runs
 *  on the approver's own machine, against a copy of the plan they downloaded
 *  from here; nothing about it reaches this page. */
export const APPROVE_COMMAND =
  "logweir drill approve --spec <file> --key <privkey> --approver <id> " +
  "--ticket <id> --subject-kind Restore --out <file>";

/** The six steps as the stepper names them: id, number and title, in the
 *  order the sections render. The titles are the section headings' own
 *  words. */
export const STEPS = Object.freeze([
  { id: "step-archive", title: "Archive" },
  { id: "step-backup-set", title: "Recovery point" },
  { id: "step-point-in-time", title: "Point in time" },
  { id: "step-target", title: "Target, topic subset and naming" },
  { id: "step-preflight", title: "Operation readiness" },
  { id: "step-plan", title: "Plan, hash and names" },
]);

/** THE STEPPER'S STATE: which steps are done, which one you are on, what is
 *  next, and which one needs attention -- one entry per step, in order.
 *
 *  All six sections are on the page at once, so "the step you are on" is the
 *  FIRST one whose inputs are not yet whole: an archive with no URL, a chosen
 *  run with no backup set, a point outside the covered window, a target with
 *  no mode or no prefix, a target cluster whose recorded status is not
 *  reachable. When the five input steps are whole, the plan step is the
 *  current one and reads `ready`. A step that is not whole because the page
 *  has a complaint about it reads `attention`; one that merely waits its
 *  turn reads `todo`.
 *
 *  THIS DECIDES NOTHING THE RUNNER DECIDES. It reads the same state the six
 *  sections read and summarises it; the create button is never gated on it,
 *  because the client-side checks are a convenience and never the gate. Pure:
 *  no DOM, no network, no clock. */
export function stepStates(state) {
  const s = state || {};
  const fields = s.fields || {};
  const targetFields = fields.target || {};
  const chosen = chosenBackup(s);
  const covered = coveredOf(s);
  const value =
    typeof fields.pointInTime === "string" && fields.pointInTime.length > 0
      ? fields.pointInTime
      : rfc3339(covered.toMs);
  const target = targetCluster(s);
  const targetStatus = (target || {}).status || {};
  const targetSpec = (target || {}).spec || {};
  // A CHOSEN POINT ALWAYS CARRIES A SET. `isRecoveryPoint` requires a non-empty
  // `status.backupId`, and `resolvePoint` hands back a point only when that
  // holds, so `state.point !== null` implies this. It is still read rather than
  // assumed, because step 2 is `todo` for a state with no point at all -- which
  // is what the selector and the refusal pages render over.
  const setChosen =
    chosen !== null &&
    typeof ((chosen.status || {}).backupId) === "string" &&
    chosen.status.backupId.length > 0;
  // PLAT-11.2: step 4 now carries the subset and the mapping too, so it is
  // whole only when those hold. The stepper still decides nothing -- the
  // create button is gated by `validateRestore` and by the readiness check,
  // not by this -- but a step whose mapping is refused must not read "done".
  const mapping = mappingProblems(s);
  const whole = [
    typeof s.archiveUrl === "string" && s.archiveUrl.length > 0,
    setChosen,
    windowComplaint(value, covered) === null,
    target !== null &&
      TARGET_MODES.indexOf(targetFields.mode) !== -1 &&
      typeof targetFields.topicPrefix === "string" &&
      targetFields.topicPrefix.length > 0 &&
      Object.keys(mapping).length === 0,
    target !== null && targetStatus.reachable === true,
  ];
  const attention = [
    false,
    // Step 2 has no `attention` state: a point either resolved -- in which case
    // it carries a set and the step is done -- or it did not, in which case
    // this stepper is not on screen. The arm that used to stand here described
    // a chosen run with no backup set, which `isRecoveryPoint` no longer lets
    // through.
    false,
    !whole[2],
    (target !== null &&
      targetFields.mode === "scratch" &&
      typeof targetSpec.markerTopic !== "string") ||
      Object.keys(mapping).length > 0,
    target !== null && !whole[4],
  ];
  let firstOpen = 5;
  for (let i = 0; i < 5; i += 1) {
    if (!whole[i]) {
      firstOpen = i;
      break;
    }
  }
  return STEPS.map((step, i) => {
    let status;
    if (i === 5) {
      status = firstOpen === 5 ? "ready" : "todo";
    } else if (whole[i]) {
      status = attention[i] ? "attention" : "done";
    } else {
      status = attention[i] ? "attention" : "todo";
    }
    return {
      id: step.id,
      number: i + 1,
      title: step.title,
      status: status,
      current: i === firstOpen,
    };
  });
}

/** The word a stepper entry carries beside its title. Text, so the state is
 *  never colour alone. */
function stepWord(step) {
  if (step.status === "done") {
    return "done";
  }
  if (step.status === "attention") {
    return "needs attention";
  }
  if (step.status === "ready") {
    return "review and create";
  }
  return step.current ? "you are here" : "next";
}

/** The stepper: an ordered list of the six steps, each a button that scrolls
 *  to its section, carrying the step's number, its title and its state in
 *  words. The current one is marked `aria-current="step"`. */
export function renderStepper(state) {
  const items = stepStates(state)
    .map((step) => {
      const classes =
        "stepper-item is-" + step.status + (step.current ? " is-current" : "");
      return (
        "<li class=\"" + classes + "\">" +
        "<button type=\"button\" class=\"stepper-link\" data-target=\"" + step.id + "\"" +
        (step.current ? " aria-current=\"step\"" : "") + ">" +
        "<span class=\"stepper-num\">" + String(step.number) + "</span>" +
        "<span class=\"stepper-title\">" + esc(step.title) + "</span>" +
        "<span class=\"stepper-status\">" + stepWord(step) + "</span>" +
        "</button></li>"
      );
    })
    .join("");
  return "<ol class=\"stepper\" aria-label=\"The six steps\">" + items + "</ol>";
}

/** The whole wizard, all six steps, over one state. */
export async function renderRestoreWizard(state) {
  return renderPreparedWizard(state, await preparePlanOrProblem(state));
}

/** The same wizard over a plan already prepared -- so the mount half knows the
 *  exact hash it put on screen, and can refuse to submit any other. */
export function renderPreparedWizard(state, prepared) {
  const s = state || {};
  return (
    "<h2>Restore wizard</h2>" +
    "<p class=\"blurb\">Six steps, all on this page: the archive, the backup set, the " +
    "point in time, the target, the preflight, and the plan whose bytes the Restore " +
    "carries. Every value was read from this namespace's own objects or is editable " +
    "below.</p>" +
    renderRetryBanner(state) +
    (s.editing
      ? "<p class=\"immutable-note\">" + RESTORE_IMMUTABLE_SENTENCE + "</p>"
      : "") +
    (s.draftRestored === true
      ? "<div class=\"draft-note\"><p class=\"note\">" + DRAFT_RESTORED_SENTENCE + "</p>" +
        "<div class=\"actions\"><button type=\"button\" id=\"discard-draft\">Discard these edits</button></div></div>"
      : "") +
    renderStepper(state) +
    renderArchiveStep(state) +
    renderRecoveryPointStep(state) +
    renderPointInTimeStep(state) +
    renderTargetStep(state) +
    renderPreflightStep(state, prepared) +
    renderPlanStep(prepared, state)
  );
}

/** Said when the wizard reopens with edits made earlier in this page's life. */
export const DRAFT_RESTORED_SENTENCE =
  "Your unsubmitted edits to this plan, made earlier on this page for the same backup set, are " +
  "back. They lived in this page's memory only: a reload would have started the wizard afresh.";

/** [`preparePlan`], or `{problem}` naming the field the runner's grammar
 *  needs when the values do not make a document. Never throws for that. */
export async function preparePlanOrProblem(state) {
  try {
    return await preparePlan(state);
  } catch (error) {
    if (error instanceof TypeError || error instanceof RangeError) {
      return { problem: String(error.message) };
    }
    throw error;
  }
}

/** The page's own checks on the values a Restore is created from, by input.
 *
 *  A point that is not an instant at all is here, because the API server
 *  refuses it (`format: date-time`) -- and so, SINCE PLAT-11.1, is a point
 *  outside the coverage the selected recovery point discloses. That second one
 *  used to be a grey complaint on the reasoning that the client-side checks are
 *  a convenience and never the gate. They still are not the gate: guard G-WIN
 *  in the runner refuses a point the archive does not cover, against the
 *  manifest, and this function cannot see a manifest. What changed is that the
 *  page now refuses to spend an approver's signature on a document phase 0 was
 *  always going to refuse -- so the check is a REFUSAL THIS PAGE MAKES FIRST,
 *  not a second gate, and nothing is sent when it fires. */
export function validateRestore(state) {
  const s = state || {};
  const fields = s.fields || {};
  const target = fields.target || {};
  const problems = Object.create(null);
  if (epochMs(fields.pointInTime) === null) {
    problems.pointInTime = "an RFC 3339 instant, such as 2026-09-07T14:05:00Z";
  } else if (windowComplaint(fields.pointInTime, coveredOf(s)) !== null) {
    // THE DISCLOSED COVERAGE IS A BOUND, NOT A HINT (PLAT-11.1). The window is
    // CLOSED AT BOTH ENDS: a point equal to `fromMs` or to `toMs` is inside
    // it, and only a point strictly outside is refused. The refusal is the
    // page's own and nothing is sent, so every value the operator typed stays
    // where it was -- which is the difference between this and the grey
    // complaint it replaces, which let a plan be built for a point the archive
    // never covered and left phase 0 to say so after an approver had signed
    // it. Guard G-WIN in the runner is still the gate; this is the page
    // refusing to waste a signature on a document that cannot pass it.
    problems.pointInTime =
      "outside the coverage this recovery point discloses -- " +
      windowMessage(coveredOf(s).fromMs, coveredOf(s).toMs) +
      ", and both bounds are inside it";
  }
  if (TARGET_MODES.indexOf(target.mode) === -1) {
    problems.mode = "one of " + TARGET_MODES.join(", ");
  }
  // THE SUBSET AND THE MAPPING (PLAT-11.2), in one call so the page and the
  // product API refuse the same five things in the same words. It REPLACES
  // the bare non-empty prefix check that used to stand here: an empty prefix
  // is one of its five, and it is the identity map rather than a missing
  // value.
  Object.assign(problems, mappingProblems(s));
  const resolvedTarget = resolveTarget(s);
  if (resolvedTarget.state === "recreated") {
    problems.targetCluster =
      "the KafkaCluster this wizard selected (uid " + resolvedTarget.uid + ") is gone and a " +
      "different object now answers to the name " + resolvedTarget.name + " (uid " +
      resolvedTarget.recreatedUid + "). A recreated connection is a different set of brokers " +
      "reached with a different credential; choose the target you mean";
  } else if (resolvedTarget.state === "missing") {
    problems.targetCluster =
      "the KafkaCluster this wizard selected (" + resolvedTarget.name + ", uid " +
      resolvedTarget.uid + ") is not in this namespace any more; choose the target you mean";
  } else if (resolvedTarget.state !== "selected") {
    problems.targetCluster =
      "choose a saved connection: nothing is selected, so there is no cluster for this restore " +
      "to write to";
  }
  if (typeof ((fields.evidence || {}).bucket) !== "string" || fields.evidence.bucket.length === 0) {
    problems.evidenceBucket = "the bucket the signed evidence is written to";
  }
  if (typeof s.archiveSecretName === "string" && s.archiveSecretName.length > 0 &&
    !isObjectName(s.archiveSecretName)) {
    problems.archiveSecret = "a Secret name is lowercase letters, digits, '-' and '.'";
  }
  if (typeof s.archiveUrl !== "string" || s.archiveUrl.length === 0) {
    problems.archive = "no archive URL was read from a Backup in this namespace";
  }
  if (typeof fields.backupSetRef !== "string" || fields.backupSetRef.length === 0) {
    problems.backupSet = "no completed backup set is chosen";
  }
  // AND THE POINT ITSELF, because every field above was derived from it. A
  // state whose point did not resolve has no business reaching `create`, and
  // the mount half never renders a submit for one -- this is the arm that
  // holds when `submitRestore` is called directly.
  if (s.pointState !== "selected") {
    problems.backupSet =
      "no recovery point is selected (" + String(s.pointState || "none") + "); choose one " +
      "before a plan can be built";
  }
  return problems;
}

/** The wizard's editable values, as a draft keeps them. */
export function wizardDraftValues(state) {
  const s = state || {};
  const f = s.fields || {};
  const target = f.target || {};
  const source = f.source || {};
  return {
    // WHICH RUN THIS DRAFT WAS MADE FOR, beside the set it was made for. A
    // retry shares the recovery point -- and therefore the backup set -- with
    // the run it retries, so `backupSetRef` alone would let an ordinary
    // restore's kept prefix apply to a retry and put the failed run's own
    // target names back. That is the collision the fresh prefix exists to
    // avoid, restored from a draft.
    retryOf: typeof s.retryOf === "string" ? s.retryOf : "",
    backupSetRef: f.backupSetRef,
    pointInTime: f.pointInTime,
    mode: target.mode,
    topicPrefix: target.topicPrefix,
    targetCluster: s.targetClusterName,
    targetClusterUid: s.targetClusterUid,
    endpoint: source.endpoint,
    region: source.region,
    pathStyle: source.pathStyle === true,
    allowHttp: source.allowHttp === true,
    evidenceBucket: (f.evidence || {}).bucket,
    archiveSecret: s.archiveSecretName,
    topics: selectedTopics(s),
  };
}

/** Puts a kept draft back into a freshly built state -- only when the draft
 *  was made for the backup set this state chose, because a point in time and
 *  a prefix chosen for one set are not edits to another. Returns whether it
 *  applied. */
export function applyWizardDraft(state, draft) {
  const d = draft || {};
  if (typeof d.backupSetRef !== "string" || d.backupSetRef !== ((state || {}).fields || {}).backupSetRef) {
    return false;
  }
  // AND FOR THE SAME RUN IDENTITY. A draft with no `retryOf` is an older one
  // and reads as the ordinary restore it was, so the comparison is against ""
  // on both sides and the legacy case is unchanged.
  const retrying = typeof ((state || {}).retryOf) === "string" ? state.retryOf : "";
  if ((typeof d.retryOf === "string" ? d.retryOf : "") !== retrying) {
    return false;
  }
  if (typeof d.pointInTime === "string") {
    state.fields.pointInTime = d.pointInTime;
  }
  if (typeof d.mode === "string") {
    state.fields.target.mode = d.mode;
  }
  // THE UID FIRST, THE NAME AS THE LEGACY PATH. A draft kept before PLAT-07.2
  // carries a name and no uid; it is resolved by name and PINNED to whatever
  // uid answers, exactly as an existing `sourceRef.name` is. A draft that
  // carries a uid is applied even when that uid no longer answers, BECAUSE the
  // refusal has to be reachable: dropping an unresolvable selection here would
  // silently restore the default target, which is the substitution this page
  // refuses everywhere else.
  if (typeof d.targetClusterUid === "string" && d.targetClusterUid.length > 0) {
    selectTarget(state, d.targetClusterUid, typeof d.targetCluster === "string" ? d.targetCluster : "");
  } else if (typeof d.targetCluster === "string" && d.targetCluster.length > 0) {
    const byName = resolveClusterSelection(state.clusters, { uid: "", name: d.targetCluster });
    if (byName.state === "selected") {
      selectTarget(state, byName.uid, byName.name);
    }
  }
  if (typeof d.topicPrefix === "string") {
    setTopicPrefix(state, d.topicPrefix);
  }
  for (const block of [state.fields.source, state.fields.evidence]) {
    if (typeof d.endpoint === "string") {
      block.endpoint = d.endpoint;
    }
    if (typeof d.region === "string") {
      block.region = d.region;
    }
    if (typeof d.pathStyle === "boolean") {
      block.pathStyle = d.pathStyle;
    }
    // AND NEVER FROM THE ADDRESSING STYLE. This is the other half of defect
    // UI-HTTPDOWNGRADE: a draft restored after a refusal used to re-derive the
    // insecure-transport flag from `pathStyle`, so even a plan whose box was
    // never ticked came back with it set.
    if (typeof d.allowHttp === "boolean") {
      block.allowHttp = d.allowHttp;
    }
  }
  if (typeof d.evidenceBucket === "string") {
    state.fields.evidence.bucket = d.evidenceBucket;
    state.evidenceBucket = d.evidenceBucket;
  }
  if (typeof d.archiveSecret === "string") {
    state.archiveSecretName = d.archiveSecret;
  }
  // AN EMPTY KEPT SUBSET IS A REAL EDIT and is applied as one: it is the state
  // `mappingProblems` refuses by name, and dropping it here would silently put
  // every frozen topic back.
  if (Array.isArray(d.topics)) {
    state.fields.topics = d.topics.slice();
    state.fields.topics = selectedTopics(state);
  }
  return true;
}

/** The prefill an "edit" produces: a NEW draft, never a patch.
 *
 *  `Restore.spec` is CEL-immutable, so the only thing an edit can mean is
 *  "start from these values". The fields the CRD carries are lifted across;
 *  the rest of the plan document -- the sample window, the evidence sink, the
 *  bootstrap servers -- is not on `Restore.spec` at all and stays as the
 *  wizard has it. */
export function draftFrom(object, fields) {
  const spec = ((object || {}).spec) || {};
  const target = spec.target || {};
  const base = fields || {};
  const nextTarget = Object.assign({}, base.target || {});
  if (typeof target.mode === "string") {
    nextTarget.mode = target.mode;
  }
  if (typeof (target.topicNaming || {}).prefix === "string") {
    // BOTH KEYS, for `setTopicPrefix`'s reason. This one builds a fields
    // object rather than mutating a state, so it writes them here; the row
    // `the_prefix_is_one_value_in_both_modes` walks this call site too.
    nextTarget.topicPrefix = target.topicNaming.prefix;
    nextTarget.topicMappingPrefix = target.topicNaming.prefix;
  }
  const next = Object.assign({}, base, { target: nextTarget });
  if (typeof spec.pointInTime === "string") {
    next.pointInTime = spec.pointInTime;
  }
  if (typeof spec.backupSetRef === "string") {
    next.backupSetRef = spec.backupSetRef;
  }
  return next;
}

// -------------------------------------------------------------- the plan half

/** Step 6's one state transition: the plan bytes, hashed and named.
 *
 *  The bytes are rendered ONCE and read from `state.planBytes` by everything
 *  that follows. A page that rendered them again for the submit would be a
 *  page with two documents and one hash. */
export async function preparePlan(state) {
  const s = state || {};
  // ONE FUNCTION PRODUCES THE PLAN, FOR REVIEW AND FOR SUBMISSION (PLAT-18.1).
  // `preparePlanDocument` renders, hashes and mints ONCE per distinct document
  // and hands back the SAME FROZEN OBJECT for the same bytes, so the document
  // the review step shows and the document the submit sends are one object and
  // not two that happen to be equal.
  return preparePlanDocument(
    s.fields,
    typeof s.planBytes === "string" ? { bytes: s.planBytes } : undefined,
  );
}

// EVERYTHING BETWEEN THE TWO MARKERS BELOW IS THE SUBMIT REGION, and
// `crates/logweir/tests/ui_lint.rs::the_wizard_never_reserialises_the_plan_bytes`
// asserts that six string-transforming tokens -- the two JSON entry points, the
// structured clone, the two whitespace/Unicode normalisers and the plan
// document's file extension -- appear nowhere inside it. The token list is
// spelled out in the Rust test and deliberately NOT here, because a comment
// naming the tokens would itself be a hit; this paragraph sits outside the
// region for the same reason.
//
// The behaviour arm asserts the same property the only way that can actually
// hold it -- by byte comparison over a fixture whose plan ends in two spaces
// and a newline and carries a non-ASCII name. JavaScript has no string
// identity operator, so an `===` arm would hold under a mutant that
// reserialised to an equal string and would fail to catch the one that did
// not.

// SUBMIT-REGION-BEGIN

/** The object `create` posts. `metadata.name` is the minted restore name and
 *  `spec.approvalRef.name` is the minted approval name, which DOES NOT EXIST
 *  YET -- the reconciler requeues at 30 s on `ApprovalNotVerified` until it
 *  does (interface I19). `spec.planBytes` is the string from `prepared`,
 *  unchanged.
 *
 *  `spec.sourceArchive` is BOTH HALVES of `ArchiveRef`: the URL, and the name
 *  of the Secret the runner reads the archive with. The second is what makes
 *  the first usable -- the controller injects the object-store credential into
 *  the runner Job only when `secretRef` is set -- and an object that omitted it
 *  is accepted by the API server and fails later, at the archive.
 *
 *  THE KEY IS ABSENT AND NEVER `null` WHEN THERE IS NO NAME. `secretRef` is an
 *  OBJECT in the CRD schema, and `status` and `spec` are both structural: a
 *  literal `null` is a type error the API server reports as a 422 over a field
 *  the operator deliberately left blank. */
export function restoreBody(state, prepared) {
  const s = state || {};
  const p = prepared || {};
  const fields = s.fields || {};
  const target = fields.target || {};
  const sourceArchive = { url: s.archiveUrl };
  if (typeof s.archiveSecretName === "string" && s.archiveSecretName.length > 0) {
    sourceArchive.secretRef = { name: s.archiveSecretName };
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Restore",
    metadata: { name: p.restoreName },
    spec: {
      planBytes: p.bytes,
      approvalRef: { name: p.approvalName },
      sourceArchive: sourceArchive,
      backupSetRef: fields.backupSetRef,
      pointInTime: fields.pointInTime,
      target: {
        clusterRef: { name: s.targetClusterName },
        mode: target.mode,
        topicNaming: { prefix: target.topicPrefix },
      },
      deadlineSeconds: typeof s.deadlineSeconds === "number" ? s.deadlineSeconds : 3600,
    },
    // THE MAPPING THE PREVIEW SHOWED, DECLARED BESIDE THE REQUEST (PLAT-11.2).
    // `Restore.spec` has no topic list -- the subset lives in the plan bytes --
    // so this rides on the create REQUEST and is dropped by the API server in
    // legacy mode, where the CRD's structural schema prunes what it does not
    // declare. That asymmetry is deliberate and it is not a hole: the product
    // API recomputes every row from the prefix it stores and refuses a request
    // whose preview and submission disagree, and in legacy mode the plan bytes
    // -- which carry the same list, from the same `selectedTopics` call -- are
    // what phase 0 reads. One function produces both.
    // AND ONLY IN `newTopic`. In `scratch` the runner maps through the PLAN's
    // `topic_mapping_prefix`, which the product API never parses, so it holds
    // no value it could check a declaration against and refuses one by name
    // (`topicMapping`/`unsupported_for_mode`). Sending it there would be this
    // page asking for a verdict nobody can give. The preview is still exact in
    // both modes -- `effectivePrefix` reads the key the run reads -- and in
    // `scratch` the rails are the page's own and phase 0's.
    topicMapping: target.mode === "scratch" ? undefined : topicMapping(s),
  };
}

/** The route "Request approval" navigates to. `subjectRef` and `planHash` come
 *  from HERE and never from the approval documents: the approvals page is
 *  forbidden from parsing those, so the hash cannot be lifted out of them
 *  either. */
export function approvalRoute(state, prepared) {
  const s = state || {};
  const p = prepared || {};
  // There is no implicit namespace on a router that deliberately refuses to
  // guess one. In particular, `default` is a real selected namespace and
  // must cross this hand-off explicitly rather than being elided as a legacy
  // shorthand.
  const ns = typeof s.ns === "string" ? s.ns.trim() : "";
  return (
    "#/approvals?subject=" +
    encodeURIComponent(p.restoreName) +
    "&hash=" +
    encodeURIComponent(p.hash) +
    "&name=" +
    encodeURIComponent(p.approvalName) +
    (ns.length > 0 ? "&ns=" + encodeURIComponent(ns) : "")
  );
}

/** THE GUIDED SUBMIT: creates the `Restore` -- the FIRST of the two creates,
 *  with a dangling `approvalRef` -- and says where the journey goes next.
 *
 *  In order, and nothing is sent until the first three pass:
 *   1. the page's own checks on the values (`validateRestore`);
 *   2. the plan is prepared -- rendered, hashed and named -- and when a route
 *      token is given and has left meanwhile, nothing is sent (`null`);
 *   3. when `options.reviewedHash` is given, the prepared hash must be it: the
 *      bytes a click sends are the bytes that were on screen when it was
 *      clicked, and a field changed in between is a refusal, not a surprise;
 *   4. one create, WITHOUT a route signal. Its name is minted from the bytes,
 *      so `409 AlreadyExists` is this plan submitted before: an existing
 *      Restore with exactly this spec is this operation, and any other is a
 *      conflict (`resolveExisting`);
 *   5. the destination: the Restore's operation view when an Approval already
 *      authorises exactly this Restore, else its approval page.
 *
 *  Returns `{outcome, object, route, prepared}`, or `null` when step 2 found
 *  the route gone. */
export async function submitRestore(state, deps, lifecycle, options) {
  const api = deps || API;
  const s = state || {};
  const problems = validateRestore(s);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  const prepared = await preparePlan(s);
  // Hashing and minting are client-side preparation, not a submitted durable
  // operation. Navigation while they run therefore disarms the pending
  // action; once `create` starts, deliberately pass no route signal so an
  // accepted server mutation can finish after navigation.
  if (!active(lifecycle)) {
    return null;
  }
  // THE READINESS GATE IS CHECKED HERE AND NOT ONLY ON THE BUTTON (PLAT-11.2),
  // for `validateRestore`'s own reason: a disabled attribute is a rendering,
  // and this is the arm that holds when `submitRestore` is called directly --
  // by the harness, by a test, or by a click that raced a re-render.
  const blocked = readinessRefusal(s, prepared);
  if (blocked !== null) {
    throw refusal(blocked, { planHash: prepared.hash });
  }
  const reviewed = (options || {}).reviewedHash;
  if (typeof reviewed === "string" && reviewed !== prepared.hash) {
    throw refusal(
      "the plan changed after it was displayed (the page showed " + reviewed + ", the current " +
        "values hash to " + prepared.hash + "); review the plan shown now and submit again",
      { reviewedHash: reviewed, preparedHash: prepared.hash },
    );
  }
  const body = restoreBody(s, prepared);
  let created;
  try {
    created = { outcome: "created", object: await api.create(s.ns, PLURAL, body) };
  } catch (error) {
    created = await resolveExisting(api, s.ns, PLURAL, body, RESTORE_SPEC_RULES, error);
  }
  const route = await restoreDestination(api, s, prepared, created.object);
  return { outcome: created.outcome, object: created.object, route: route, prepared: prepared };
}

/** Where a submitted Restore goes next, under today's approval semantics:
 *  every Restore waits for a verified Approval. The operation view when the
 *  Approval its `spec.approvalRef` names already authorises exactly this
 *  Restore -- this name, this namespace, this UID, this plan -- and its
 *  approval page otherwise. A read that fails is not an authorisation, so it
 *  lands on the approval page, which reads the state again for itself. */
export async function restoreDestination(api, state, prepared, restore) {
  const s = state || {};
  const p = prepared || {};
  const meta = (restore || {}).metadata || {};
  const approvalName = (((restore || {}).spec || {}).approvalRef || {}).name || p.approvalName;
  let approval = null;
  try {
    approval = await api.get(s.ns, APPROVALS, approvalName);
  } catch (unread) {
    approval = null;
  }
  if (approval !== null && approvalAuthorizes(approval, restore, p.hash)) {
    return restoreOperationRoute(s.ns, typeof meta.name === "string" ? meta.name : p.restoreName);
  }
  return approvalRoute(s, p);
}

/** Reads the namespace's saved connections again and puts them on the state,
 *  so every later resolution -- `validateRestore`'s target check included --
 *  is made against what the namespace holds NOW. It sets `state.clusters` and
 *  touches no field of the plan.
 *
 *  The read carries no route signal: a submit in flight is a durable operation
 *  and navigation must not cancel it (PLAT-13.2), and this read is part of
 *  that operation. */
export async function confirmClusters(state, api) {
  const s = state || {};
  try {
    s.clusters = await api.list(s.ns, CLUSTERS);
  } catch (unread) {
    throw invalidInput({
      targetCluster: "the saved connections could not be read again before submitting, so the " +
        "target this plan names could not be confirmed (" + String(unread && unread.message) +
        "). Nothing was sent; the plan and every value you typed are still here.",
    });
  }
  // AND THE LABEL FOLLOWS THE IDENTITY (review finding F4). `restoreBody`
  // spells `target.clusterRef.name` from `state.targetClusterName`, so a
  // connection renamed between opening the wizard and submitting it would be
  // sent under the name it no longer has. The schedule form already does this
  // (`submitSchedule` sends `resolved.name`); this is the same rule for a
  // restore, and it touches `state.fields` not at all -- the plan bytes and
  // the reviewed-hash guard are unaffected.
  const resolved = resolveTarget(s);
  if (resolved.state === "selected") {
    s.targetClusterName = resolved.name;
    s.targetClusterUid = resolved.uid;
  }
  return s.clusters;
}

// SUBMIT-REGION-END

// --------------------------------------------------------------- private half

function archiveFor(state, clusterName) {
  for (const backup of itemsOf(state.backups)) {
    const spec = backup.spec || {};
    if (((spec.sourceRef || {}).name) === clusterName) {
      return (spec.archive || {}).url;
    }
  }
  return null;
}

/** The name beside the URL above, off the SAME archive reference. `null` when
 *  that archive names no Secret, which the table renders as an empty cell. */
function archiveSecretFor(state, clusterName) {
  for (const backup of itemsOf(state.backups)) {
    const spec = backup.spec || {};
    if (((spec.sourceRef || {}).name) === clusterName) {
      const secret = ((spec.archive || {}).secretRef) || {};
      return typeof secret.name === "string" ? secret.name : null;
    }
  }
  return null;
}

function backupsOf(state) {
  return itemsOf(state.backups).filter((backup) => {
    if (typeof state.archiveUrl !== "string" || state.archiveUrl.length === 0) {
      return true;
    }
    return (((backup.spec || {}).archive) || {}).url === state.archiveUrl;
  });
}

/** THE COVERED WINDOW, READ FROM `windowCovered.fromMs`/`toMs`.
 *
 *  Interface I22 names those two keys and they are integers. A page that read
 *  `covered.from`/`covered.to` would get `undefined` twice, default step 3 to
 *  nothing, and render the window message with `Invalid Date` on both bounds --
 *  which is what the behaviour suite's window row exists to catch. */
function coveredOf(state) {
  const chosen = chosenBackup(state);
  const covered = ((chosen || {}).status || {}).windowCovered || {};
  return { fromMs: covered.fromMs, toMs: covered.toMs };
}

/** WHEN A RUN COMPLETED, read off the `Complete` condition rather than off
 *  `windowCovered.toMs`: the covered window is about the RECORDS, and two runs
 *  can cover windows that end in the other order from the order they ran. The
 *  creation timestamp is the fallback; `null` when neither is readable. */
function completedAt(backup) {
  const conditions = ((backup || {}).status || {}).conditions;
  if (Array.isArray(conditions)) {
    for (const condition of conditions) {
      if ((condition || {}).type === "Complete") {
        const at = epochMs(condition.lastTransitionTime);
        if (at !== null) {
          return at;
        }
      }
    }
  }
  return epochMs(((backup || {}).metadata || {}).creationTimestamp);
}

/** THE POINT THIS STATE IS BOUND TO, and nothing else.
 *
 *  This used to be a search: with no `backupSetRef` it answered with the
 *  newest `Succeeded` object, which meant the page chose for the operator and
 *  changed its mind whenever a schedule completed (Task 28 measured it doing
 *  exactly that). It is now a field, resolved ONCE by `resolvePoint` from the
 *  identity in the route, and every later render reads the same object. There
 *  is no fallback: a state with no point renders the selector or a refusal,
 *  never a plan. */
function chosenBackup(state) {
  return ((state || {}).point) || null;
}

/** The chosen target: the one named in the state if it is still in the list,
 *  else [`firstTarget`]'s default. The two must agree, because step 4's
 *  `<select>` marks the same cluster `selected` and `bootstrapOf` reads this
 *  one into the plan. */
function targetCluster(state) {
  const resolved = resolveTarget(state);
  return resolved.state === "selected" ? resolved.cluster : null;
}

/** THE TARGET SELECTION, RESOLVED. The one place this page turns the pair the
 *  state holds -- `{targetClusterUid, targetClusterName}` -- into an answer,
 *  and it is `../select.js`'s answer, so the wizard, the schedule form and the
 *  clusters page all refuse the same things for the same reasons.
 *
 *  THERE IS NO FALLBACK TO "THE FIRST ONE INSTEAD". `firstTarget` still picks
 *  the DEFAULT for a fresh state, in `initialState`, where it is visible in the
 *  rendered select before anything is sent. Here -- after a selection exists --
 *  a UID that no longer answers is `missing` or `recreated` and the wizard says
 *  so: silently sliding onto another cluster is how a restore lands in a
 *  namespace's production brokers because somebody rebuilt a scratch cluster. */
export function resolveTarget(state) {
  const s = state || {};
  return resolveClusterSelection(s.clusters, {
    uid: s.targetClusterUid,
    name: s.targetClusterName,
  });
}

/** The prefix this state defaults to: a retry's fresh one when this wizard is
 *  retrying a failed run, and the recovery point's own otherwise. The ONE
 *  place that choice is made, so the field's placeholder, the field's
 *  emptied-value fallback and the initial state cannot disagree about it. */
export function defaultPrefixFor(state) {
  const s = state || {};
  const at = (s.fields || {}).pointInTime;
  return typeof s.retryOf === "string" && s.retryOf.length > 0
    ? freshTargetPrefix(at, s.retryOf)
    : prefixFor(at);
}

/** The default prefix for an instant, or the empty string when there is no
 *  instant to derive one from. */
function prefixFor(pointInTime) {
  // An input that is not (yet) an instant has no default prefix; it is the
  // point-in-time field's message that says so, not a thrown render.
  if (typeof pointInTime !== "string" || epochMs(pointInTime) === null) {
    return "";
  }
  return defaultTopicPrefix(pointInTime);
}

/** The client-side window complaint, or `null`. It compares MILLISECONDS on
 *  both sides. */
function windowComplaint(value, covered) {
  const at = epochMs(value);
  if (at === null) {
    return windowMessage(covered.fromMs, covered.toMs);
  }
  if (typeof covered.fromMs !== "number" || typeof covered.toMs !== "number") {
    return windowMessage(covered.fromMs, covered.toMs);
  }
  if (at < covered.fromMs || at > covered.toMs) {
    return windowMessage(covered.fromMs, covered.toMs);
  }
  return null;
}

// --------------------------------------------------------------- mount half

/** The wizard, over one namespace and one route identity.
 *
 *  `params` is [`restoreRouteParams`]'s answer for the current hash. Three
 *  pages live behind this one mount, and which one it is depends only on that
 *  identity and on what the namespace holds:
 *   - no identity          -> the selector (or the empty state, when there is
 *                             nothing completed to select);
 *   - an identity that does not resolve -> a refusal naming it;
 *   - an identity that resolves         -> the six steps.
 *  The identity is carried back into every re-mount this page makes, so
 *  discarding a draft -- the one place the wizard re-reads the namespace --
 *  cannot land on a different point. */
export async function mountRestoreWizard(node, ns, params, parse, deps, lifecycle) {
  const api = deps || API;
  const selection = params || {};
  try {
    const collections = await Promise.all([
      api.list(ns, CLUSTERS, readOptions(lifecycle)),
      api.list(ns, BACKUPS, readOptions(lifecycle)),
    ]);
    if (!active(lifecycle)) {
      return;
    }
    const clusters = collections[0];
    const backups = collections[1];
    const state = initialState(ns, clusters, backups, selection);
    if (state.pointState === "none") {
      if (completedBackups(backups).length === 0) {
        replace(node, parse(renderNoCompletedBackup(ns, backups)));
        return;
      }
      replace(node, parse(renderPointSelector(state)));
      wireSelector(node, state, lifecycle);
      return;
    }
    if (state.pointState !== "selected") {
      replace(node, parse(renderPointRefusal(state)));
      return;
    }
    const key = formKey(ns, WIZARD_FORM);
    const record = mutationFor(key);
    if (record.state.phase === "succeeded") {
      dropDraft(key);
    }
    const draft = readDraft(key);
    if (draft !== null) {
      if (applyWizardDraft(state, draft)) {
        state.draftRestored = true;
      } else {
        dropDraft(key);
      }
    }
    await renderAndWire(node, state, parse, api, lifecycle);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** Renders the wizard over `state` -- with its mutation record and its field
 *  messages -- and wires what was rendered to the plan it shows. */
async function renderAndWire(node, state, parse, api, lifecycle) {
  const record = mutationFor(formKey(state.ns, WIZARD_FORM));
  state.submission = record.state;
  if (record.state.phase === "failed") {
    const found = fieldErrors(record.state.error, WIZARD_FIELD_PATHS);
    state.errors = found.fields;
    state.errorsUnmatched = found.unmatched;
  } else {
    state.errors = null;
    state.errorsUnmatched = [];
  }
  const prepared = await preparePlanOrProblem(state);
  if (!active(lifecycle)) {
    return false;
  }
  replace(node, parse(renderPreparedWizard(state, prepared)));
  wire(node, state, parse, api, lifecycle, prepared);
  return true;
}

/** The state the six steps read: the two collections, THE RESOLVED RECOVERY
 *  POINT, the chosen archive and target, and the WHOLE plan document's fields.
 *
 *  `selection` is the route's identity -- `{uid, backup}` from
 *  [`restoreRouteParams`] -- and it is the only thing that decides which point
 *  this state is bound to.
 *
 *  THIS IS ALSO THE RE-READ, and there is no other one: every fresh look at
 *  the namespace -- a reload, a route change and back, the re-mount that
 *  discarding a draft performs -- comes back through `mountRestoreWizard` into
 *  this function with the SAME `selection`, and `resolvePoint` resolves it
 *  against the list that was just read rather than against the object the
 *  previous state happened to be holding. So a Backup that completed since does not
 *  become the choice, and a point DELETED since resolves to `missing` -- a
 *  refusal on the next render -- instead of a stale object the page keeps
 *  drawing a plan from. Absent, `pointState` is `none` and the mount half
 *  renders the selector; present and unresolvable, it is `missing` or
 *  `unusable` and the mount half renders a refusal. In neither case is a plan
 *  built, and in NO case does this function look for "the newest one instead":
 *  that search is the defect PLAT-11.1 removes.
 *
 *  THE FIELDS OBJECT IS COMPLETE FROM THE FIRST RENDER, because a plan is
 *  hashed as a whole and an approver signs a whole document. A wizard that
 *  built half a document and left the rest to a later edit would be offering
 *  to invalidate its own approval. Every value below is either read from the
 *  chosen point, read from a Kubernetes object, fixed by a constraint, or
 *  editable in a step -- and the three object-store settings that no CRD
 *  records (endpoint, region and the `path_style` flag) plus the explicit
 *  insecure-transport flag are editable in step 1 rather than left out,
 *  because the runner reads them from these bytes and from nowhere else. */
export function initialState(ns, clusters, backups, selection) {
  const resolved = resolvePoint(backups, selection);
  // PLAT-11.2 / PLAT-12.2: a retry of a failed run, or "" for an ordinary
  // restore. It changes exactly one value -- the default topic prefix -- and
  // through that value it changes the plan bytes, the plan hash and both
  // minted names, which is the whole of "a new execution rather than a
  // collision with the old name".
  const retryOf = typeof (selection || {}).retryOf === "string" ? selection.retryOf : "";
  const point = resolved.point;
  const spec = (point || {}).spec || {};
  const status = (point || {}).status || {};
  const covered = status.windowCovered || {};
  const pointInTime = rfc3339(covered.toMs);
  const archive = spec.archive || {};
  const archiveUrl = archive.url;
  // BOTH HALVES OF THE ARCHIVE REFERENCE, FROM THE SAME OBJECT -- and that
  // object is the chosen point. A URL taken from one Backup and a credential
  // taken from another would be two archives and one name for them.
  const archiveSecretName =
    typeof ((archive.secretRef || {}).name) === "string" ? archive.secretRef.name : "";
  const target = firstTarget(clusters);
  // `allowHttp` STARTS FALSE AND IS NEVER DERIVED (D-SEAMS S5). It is set by
  // the one explicit checkbox in step 1 and by nothing else.
  const store = { region: "", endpoint: "", pathStyle: false, allowHttp: false };
  const prefix = retryOf.length > 0
    ? freshTargetPrefix(pointInTime, retryOf)
    : prefixFor(pointInTime);
  return {
    ns: ns,
    retryOf: retryOf,
    clusters: clusters,
    backups: backups,
    selection: { uid: resolved.uid, backup: resolved.name },
    point: point,
    pointState: resolved.state,
    pointUid: resolved.uid,
    pointName: resolved.name,
    pointPhase: resolved.phase,
    pointRenamed: resolved.renamed,
    query: "",
    archiveUrl: archiveUrl,
    archiveSecretName: archiveSecretName,
    evidenceBucket: "logweir-evidence",
    targetClusterName: ((target || {}).metadata || {}).name,
    // THE IDENTITY, BESIDE THE NAME. The default is a preselect and nothing
    // more, but it is a preselect BY UID from the first render, so the very
    // first thing the draft keeps is an identity rather than a label.
    targetClusterUid: clusterUid(target),
    targetClusterState: target === null ? "none" : "selected",
    editing: null,
    deadlineSeconds: 3600,
    fields: {
      backupSetRef: status.backupId,
      topics: Array.isArray(spec.topics) ? spec.topics : [],
      pointInTime: pointInTime,
      source: Object.assign(
        { bucket: bucketOf(archiveUrl), prefix: prefixOf(archiveUrl) },
        store,
      ),
      target: {
        bootstrapServers: ((target || {}).spec || {}).bootstrapServers || [],
        auth: targetAuth(target),
        mode: TARGET_MODES[1],
        // BOTH KEYS, ALWAYS THE SAME STRING -- the invariant `setTopicPrefix`
        // maintains from here on. Switching the mode select is then a
        // one-value change and never a document that maps through a prefix
        // the page is not showing.
        topicPrefix: prefix,
        // UNREAD in `newTopic` mode and still required by the grammar (it has
        // no serde default, because an empty prefix maps every source topic
        // onto ITSELF). The same string, so switching the mode select is a
        // one-value change and never a document that will not parse -- and a
        // retry's fresh prefix therefore applies in BOTH modes.
        topicMappingPrefix: prefix,
        markerTopic: "logweir.scratch",
        replicationFactor: 1,
        teardown: "delete",
      },
      // THE SAMPLE WINDOW IS NOT THE RESTORE WINDOW. This one bounds the
      // per-record reconciliation and defaults to the point's own covered
      // range, which is the only window the cluster told this page about; the
      // RESTORE's floor is the archive's earliest covered timestamp, read from
      // the manifest by the runner, and is never a field here.
      sample: {
        windowStart: rfc3339(covered.fromMs),
        windowEnd: rfc3339(covered.toMs),
        recordsPerPartition: 25,
        anchor: "head",
      },
      objectives: {},
      evidence: Object.assign(
        { bucket: "logweir-evidence", prefix: EVIDENCE_PREFIX },
        store,
      ),
    },
  };
}

// Copy public settings only. The controller projects the selected cluster's
// Secret into the runner; its reference and contents do not belong in a plan.
function targetAuth(cluster) {
  const auth = ((cluster || {}).spec || {}).auth || {};
  if (!auth.mode || auth.mode === "plaintext") {
    return undefined;
  }
  return { mode: auth.mode, username: auth.username, tls: auth.tls === true };
}
/** Binds the wizard to one saved connection BY UID, with its name beside it.
 *
 *  `name` is optional and is only what the option said: the resolved object's
 *  own name wins when the UID answers, so a rename is followed rather than
 *  recorded twice. When the UID does NOT answer, the name given is kept as
 *  what was asked for, because that is what the refusal has to be able to
 *  print. */
export function selectTarget(state, uid, name) {
  const wanted = typeof uid === "string" ? uid : "";
  // D2 SECTION 6.6's SECOND INVALIDATION CAUSE, AND IT IS NOT THE PLAN HASH.
  // A readiness result is applicable only while
  // "`binding.inputsDigest ==` the API's recomputation from current objects",
  // and the reasons include `referentChanged:<Kind>/<name>` -- "Choosing
  // another target or destination, or a recreated one, changes a referent UID,
  // so the result is stale". The hash arm cannot see that: two `KafkaCluster`
  // objects with the same bootstrap servers and the same auth render IDENTICAL
  // plan bytes, so swapping between them left a `ready` verdict about the
  // OTHER cluster in place and the submit was allowed. The review found it,
  // and the harness's own journey had been sitting in exactly that blind spot.
  //
  // So a change of the selected UID drops the cached verdict here. The gate
  // then falls to its "nothing has run" arm, which says in words that nothing
  // has looked for an existing target topic on THIS cluster, and the operator
  // re-runs the check. Dropping it is the fail-closed half of the choice: the
  // alternative is re-reading the preflight with `?planHash=` and believing a
  // verdict until the round trip answers.
  const changed = wanted !== state.targetClusterUid;
  const held = (state.readiness || {}).preflight || null;
  if (changed && held !== null) {
    // MARKED STALE, NOT DELETED. Deleting it dropped the verdict -- which was
    // already better than keeping a green one about another cluster -- but it
    // landed the page on the "nothing has run" arm, which WARNS and permits
    // the submit. D2 section 6.6 puts a referent change and a plan-hash change
    // under ONE rule ("otherwise `applicable=false, stale=true` with
    // `staleReasons` ..."), so the two must produce the same answer: refused
    // until the check is run again. Marking it makes the existing stale arm
    // fire, with `referentChanged` and the cluster named -- the server's own
    // vocabulary, not a sentence this page invented for the occasion.
    //
    // The mark is this page's, and it is allowed to be: it is strictly more
    // conservative than the server's own recomputation, which would answer
    // `referentChanged` for exactly this input. Nothing here turns a stale
    // verdict into a fresh one.
    state.readiness = Object.assign({}, state.readiness, {
      preflight: Object.assign({}, held, {
        applicable: false,
        stale: true,
        staleReasons: [{
          reason: "referentChanged",
          kind: "KafkaCluster",
          name: typeof name === "string" && name.length > 0 ? name : wanted,
        }],
      }),
    });
  }
  state.targetClusterUid = wanted;
  if (wanted.length === 0) {
    // THE EMPTY OPTION CLEARS THE NAME TOO (review finding F1). A refused
    // selector opens on `<option value="">`, and leaving the refused NAME
    // behind would let `resolveClusterSelection` fall back to resolving by
    // name -- onto the very object the refusal is about, which is the
    // substitution the refusal exists to prevent.
    state.targetClusterName = "";
  } else if (typeof name === "string" && name.length > 0) {
    state.targetClusterName = name;
  }
  const resolved = resolveTarget(state);
  state.targetClusterState = resolved.state;
  if (resolved.state === "selected") {
    state.targetClusterName = resolved.name;
    state.targetClusterUid = resolved.uid;
  }
  const cluster = resolved.state === "selected" ? resolved.cluster : null;
  state.fields.target.bootstrapServers = ((cluster || {}).spec || {}).bootstrapServers || [];
  state.fields.target.auth = targetAuth(cluster);
}

/** The cluster step 4 preselects: one labelled `role: target` if the namespace
 *  has one, else the SOURCE cluster, else the first cluster there is.
 *
 *  NEVER `null` WHEN THE NAMESPACE HAS A CLUSTER. The old version returned
 *  `null` for a namespace whose only cluster is `role: source`, which left
 *  `target.bootstrapServers` empty and made `renderPlanBytes` throw -- on the
 *  one-cluster `newTopic` walk that IS Demo 1. The runner has no such rule
 *  (`drill/phase0_admit.rs`'s `TargetMode::NewTopic` arm is empty), so the
 *  page had invented a requirement the product does not have. */
function firstTarget(clusters) {
  const all = itemsOf(clusters);
  for (const cluster of all) {
    if (((cluster.spec || {}).role) === "target") {
      return cluster;
    }
  }
  for (const cluster of all) {
    if (((cluster.spec || {}).role) === "source") {
      return cluster;
    }
  }
  return all.length > 0 ? all[0] : null;
}

/** The wizard's own readiness form identity. */
export const RESTORE_READINESS_FORM = "restore-readiness";

/** Step 5's control: start a restore `Preflight` bound to the EXACT bytes on
 *  screen, and read it back against the same hash.
 *
 *  THE BYTES SENT ARE THE BYTES PREPARED, AND THE HASH IS THE PREPARED ONE.
 *  `prepared` is the frozen document the review step is showing -- the same
 *  object, not an equal one -- so the check is about the plan the operator is
 *  looking at, and the service compares the hash against exactly those bytes
 *  and answers `422 hash_mismatch` if they ever came apart. This page never
 *  re-renders or re-hashes the plan to start a check.
 *
 *  A PLAN THAT COULD NOT BE RENDERED HAS NOTHING TO CHECK. `prepared.problem`
 *  means the fields do not make a document the runner's grammar accepts, so
 *  there is no hash, nothing to submit, and the button sends nothing. */
function wireRestoreReadiness(node, state, parse, api, lifecycle, prepared) {
  const form = node.querySelector("#restore-readiness-form");
  if (form === null) {
    return;
  }
  const key = formKey(state.ns, RESTORE_READINESS_FORM);
  const mutation = mutationFor(key);
  const p = prepared || {};

  watchMutation(node, key, mutation, (record) => {
    if (!active(lifecycle)) {
      return;
    }
    state.readiness = Object.assign({}, state.readiness || {}, {
      state: record,
      preflight: record.phase === "succeeded"
        ? (record.result || {}).item
        : (state.readiness || {}).preflight || null,
      boundHash: record.phase === "succeeded"
        ? ((record.about || {}).planHash || "")
        : ((state.readiness || {}).boundHash || ""),
    });
    renderAndWire(node, state, parse, api, lifecycle);
  }, lifecycle);

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    if (typeof p.bytes !== "string" || typeof p.hash !== "string") {
      return;
    }
    const cluster = targetCluster(state);
    const point = (state.point || {}).metadata || {};
    const request = {
      operation: "restore",
      restore: {
        planBytes: p.bytes,
        planHash: p.hash,
        target: ((cluster || {}).metadata || {}).name,
      },
    };
    if (typeof point.name === "string" && point.name.length > 0) {
      request.restore.recoveryPoint = { backupName: point.name };
      if (typeof point.uid === "string" && point.uid.length > 0) {
        request.restore.recoveryPoint.backupUid = point.uid;
      }
    }
    // THE SOURCE ARCHIVE IS INLINE, BECAUSE THE RECOVERY POINT PUBLISHES NO
    // DESTINATION. `Backup`'s product-API projection carries `archive` and no
    // `destination`/`locationDigest`, so D2 section 9's "take the source destination
    // from the recovery point's frozen destination" has nothing to read on
    // this build. The legacy inline archive is sent instead -- which is what
    // the `Restore` this wizard creates would carry anyway -- and the step
    // says so rather than offering a destination selector that would have to
    // guess which destination that URL belongs to.
    const url = ((state.point || {}).spec || {}).archive || {};
    if (typeof url.url === "string" && url.url.length > 0) {
      request.restore.legacySourceArchive = { url: url.url };
      const secretName = String(state.archiveSecretName || "").trim();
      if (secretName.length > 0) {
        request.restore.legacySourceArchive.credentialRef = { name: secretName };
      }
    }
    mutation.run(() => api.startPreflight(state.ns, request), { about: { planHash: p.hash } });
  }, lifecycle);

  const cancel = node.querySelector("#restore-readiness-cancel");
  if (cancel !== null) {
    listen(cancel, "click", () => {
      const current = (state.readiness || {}).preflight;
      if (!active(lifecycle) || current === null || current === undefined) {
        return;
      }
      cancel.disabled = true;
      api.cancelPreflight(state.ns, current.id).then(
        () => {
          if (active(lifecycle)) {
            renderAndWire(node, state, parse, api, lifecycle);
          }
        },
        () => {
          cancel.disabled = false;
        },
      );
    }, lifecycle);
  }
}

function wire(node, state, parse, api, lifecycle, prepared) {
  const key = formKey(state.ns, WIZARD_FORM);
  const record = mutationFor(key);
  const point = node.querySelector("#point-in-time");
  const prefix = node.querySelector("#topic-prefix");
  const mode = node.querySelector("#target-mode");
  const cluster = node.querySelector("#target-cluster");
  const endpoint = node.querySelector("#store-endpoint");
  const region = node.querySelector("#store-region");
  const pathStyle = node.querySelector("#store-pathStyle");
  const allowInsecure = node.querySelector("#store-allow-insecure");
  const evidenceBucket = node.querySelector("#evidence-bucket");
  const archiveSecret = node.querySelector("#archive-secret");
  const refresh = async () => {
    if (!active(lifecycle)) {
      return;
    }
    if (point !== null) {
      state.fields.pointInTime = valueOf(point);
    }
    if (mode !== null) {
      state.fields.target.mode = valueOf(mode);
    }
    if (prefix !== null) {
      // An emptied prefix is the default prefix again: the field SHOWS the
      // default when the value is empty, and the plan must be what it shows.
      setTopicPrefix(state, valueOf(prefix) || defaultPrefixFor(state));
    }
    if (cluster !== null) {
      // THE UID THE OPTION CARRIES, AND THE NAME IT SHOWED. Reading the
      // select's value alone would give a uid with no name, so a refusal --
      // which fires exactly when that uid has stopped resolving -- could then
      // only print half an identity.
      const picked = readClusterSelection(node, "target-cluster");
      selectTarget(state, picked.uid, picked.name);
    }
    for (const block of [state.fields.source, state.fields.evidence]) {
      if (endpoint !== null) {
        block.endpoint = valueOf(endpoint);
      }
      if (region !== null) {
        block.region = valueOf(region);
      }
      if (pathStyle !== null) {
        block.pathStyle = pathStyle.checked === true;
      }
      // TWO BOXES, TWO FIELDS, AND NO ARROW BETWEEN THEM (D-SEAMS S5, defect
      // UI-HTTPDOWNGRADE). The line that used to stand here read
      // `pathStyle.checked` into `allowHttp`, on the reasoning that an
      // on-premises store needs both -- which is true of some deployments and
      // is not a reason for a page to write a security setting nobody asked
      // for into a document an approver signs. `allowHttp` is this checkbox
      // and only this checkbox.
      if (allowInsecure !== null) {
        block.allowHttp = allowInsecure.checked === true;
      }
    }
    if (evidenceBucket !== null) {
      state.fields.evidence.bucket = valueOf(evidenceBucket);
      state.evidenceBucket = state.fields.evidence.bucket;
    }
    // THE NAME ONLY, AND IT IS NOT A PLAN FIELD. It goes on the `Restore`'s
    // own spec, beside the archive URL, and never into the bytes an approver
    // signs: the runner takes the credential from its environment, which the
    // controller fills from the named Secret.
    if (archiveSecret !== null) {
      state.archiveSecretName = valueOf(archiveSecret);
    }
    // THE SUBSET, READ FROM THE BOXES THAT ARE ON SCREEN (PLAT-11.2) -- and
    // only when there ARE boxes, so a refusal page or a point with no frozen
    // list cannot empty the selection behind the operator's back. The list is
    // canonicalised through `selectedTopics` so the plan bytes, and therefore
    // the hash an approver signs, do not move with the order of the clicks.
    const boxes = node.querySelectorAll(".topic-box");
    if (boxes.length > 0) {
      const ticked = [];
      for (const box of boxes) {
        if (box.checked === true) {
          ticked.push(box.getAttribute("data-topic"));
        }
      }
      state.fields.topics = ticked;
      state.fields.topics = selectedTopics(state);
    }
    if (!active(lifecycle)) {
      return;
    }
    // THE EDIT IS KEPT BEFORE ANYTHING ELSE, and a settled outcome about the
    // previous plan is cleared: it described bytes that are no longer these.
    //
    // WHICH OUTCOMES AN EDIT CLEARS. An outcome about these BYTES -- a refusal,
    // a 422, a conflict -- described bytes that are no longer these, so it
    // goes. An outcome about whether an OBJECT EXISTS does not: editing a
    // field does not un-create a Restore, and does not settle one whose fate
    // is unknown.
    //
    // That is the defect this rule replaces. A create that timed out was NOT
    // cancelled and may still be accepted; clearing the record on the next
    // edit took its attempt number with it, so the late 201 for that Restore
    // found no answerable attempt and was dropped -- leaving an object nobody
    // was ever told about. Now the record is kept, and `submissionStatus` says
    // which plan it is about and links to it by name.
    keepDraft(key, wizardDraftValues(state), WIZARD_DRAFT_FIELDS);
    if (record.state.phase === "failed" && record.state.kind !== "unknown") {
      record.clear();
    }
    await renderAndWire(node, state, parse, api, lifecycle);
  };
  for (const field of [
    point,
    prefix,
    mode,
    cluster,
    endpoint,
    region,
    pathStyle,
    allowInsecure,
    evidenceBucket,
    archiveSecret,
  ]) {
    if (field !== null) {
      listen(field, "change", refresh, lifecycle);
    }
  }
  for (const box of node.querySelectorAll(".topic-box")) {
    listen(box, "change", refresh, lifecycle);
  }
  // THE TWO BULK CONTROLS SET THE BOXES AND THEN GO THROUGH `refresh`, so the
  // selection is read back off the DOM exactly as a click on one box is. A
  // shortcut that wrote `state.fields.topics` directly would be a second way
  // of choosing a subset, and the two could disagree.
  const selectAll = node.querySelector("#select-all-topics");
  if (selectAll !== null) {
    listen(selectAll, "click", () => {
      for (const box of node.querySelectorAll(".topic-box")) {
        box.checked = true;
      }
      refresh();
    }, lifecycle);
  }
  const selectNone = node.querySelector("#select-no-topics");
  if (selectNone !== null) {
    listen(selectNone, "click", () => {
      for (const box of node.querySelectorAll(".topic-box")) {
        box.checked = false;
      }
      refresh();
    }, lifecycle);
  }

  wireRestoreReadiness(node, state, parse, api, lifecycle, prepared);

  // The stepper: each entry scrolls its section into view and hands it focus,
  // so a keyboard reader lands where a pointer reader looks. Motion follows
  // the reader's own preference.
  wireTargetSearch(node, lifecycle);

  for (const link of node.querySelectorAll(".stepper-link")) {
    listen(link, "click", () => {
      if (!active(lifecycle)) {
        return;
      }
      const section = node.querySelector("#" + link.getAttribute("data-target"));
      if (section === null) {
        return;
      }
      const still =
        typeof window.matchMedia === "function" &&
        window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      section.scrollIntoView({ behavior: still ? "auto" : "smooth", block: "start" });
      section.focus({ preventScroll: true });
    }, lifecycle);
  }

  const copy = node.querySelector("#copy-plan");
  if (copy !== null) {
    listen(copy, "click", async () => {
      if (!active(lifecycle)) {
        return;
      }
      const pre = node.querySelector("#plan-bytes");
      if (pre !== null && navigator.clipboard) {
        await navigator.clipboard.writeText(pre.textContent);
      }
    }, lifecycle);
  }

  const download = node.querySelector("#download-plan");
  if (download !== null) {
    listen(download, "click", async () => {
      if (!active(lifecycle)) {
        return;
      }
      const prepared = await preparePlan(state);
      if (active(lifecycle)) {
        downloadPlan(prepared);
      }
    }, lifecycle);
  }

  const discard = node.querySelector("#discard-draft");
  if (discard !== null) {
    listen(discard, "click", () => {
      if (!active(lifecycle) || record.pending()) {
        return;
      }
      dropDraft(key);
      record.clear();
      // THE IDENTITY GOES BACK IN. This is the wizard's one re-read of the
      // namespace, and a re-read that dropped the route's `uid` would rebuild
      // the page with no point at all -- or, under the behaviour this task
      // replaces, with whichever run happened to be newest by then.
      mountRestoreWizard(node, state.ns, state.selection, parse, api, lifecycle);
    }, lifecycle);
  }

  // THE ONE RECORD FOR THIS NAMESPACE'S WIZARD. Pending disables the button in
  // place; a failure re-renders the wizard with its messages and keeps every
  // value; success opens the destination the submit chose -- while this route
  // is still the current one. A route left in between keeps the outcome in the
  // record, and the next mount of the wizard shows it with a link.
  watchMutation(node, key, record, (settled) => {
    // A SETTLEMENT ABOUT THE PLAN ON SCREEN OWNS THE PAGE; one about a plan the
    // fields have moved on from does not. A late answer to a timed-out attempt
    // arrives after the operator has started editing: navigating away from
    // those edits, or dropping them as "consumed", would answer one problem by
    // causing another. Instead the wizard re-renders in place and
    // `submissionStatus` shows the durable link to the Restore that settled.
    if (settled.phase === "succeeded" && !outcomeIsElsewhere(settled, prepared)) {
      dropDraft(key);
      const route = ((settled.result || {}).route);
      if (typeof route === "string" && route.length > 0 && typeof window !== "undefined") {
        window.location.hash = route;
        return;
      }
    }
    if (settled.phase === "pending") {
      const button = node.querySelector("#create-restore");
      if (button !== null) {
        button.disabled = true;
        button.setAttribute("aria-busy", "true");
      }
      const status = node.querySelector("#restore-submit-status");
      if (status !== null) {
        replace(status, parse(submissionStatus(settled, prepared, [])));
      }
      return;
    }
    renderAndWire(node, state, parse, api, lifecycle).then((rendered) => {
      if (rendered && settled.phase === "failed") {
        const target = node.querySelector("[aria-invalid=\"true\"]") ||
          node.querySelector("#restore-submit-status");
        if (target !== null && typeof target.focus === "function") {
          target.focus();
        }
      }
    });
  }, lifecycle);

  const submit = node.querySelector("#create-restore");
  if (submit !== null) {
    listen(submit, "click", () => {
      if (!active(lifecycle) || record.pending()) {
        return;
      }
      const reviewed = typeof (prepared || {}).hash === "string" ? prepared.hash : undefined;
      // The record turns pending BEFORE the first await inside it, so a second
      // click in the same instant finds it pending and sends nothing. The
      // attempt carries WHAT IT IS ABOUT -- the reviewed plan's hash and the
      // name minted from it -- so an outcome that arrives after the fields have
      // changed can still be told, and named, for what it is.
      record.run(async () => {
        // THE CONNECTIONS ARE READ AGAIN FIRST (PLAT-07.2). The target was
        // resolved against the list this view read when it mounted, and the
        // window that matters is the one between reviewing a plan and
        // submitting it. The re-read REPLACES the list the state resolves
        // against and NOTHING ELSE: the plan's own fields are untouched, so
        // `preparePlan` renders the same bytes and the reviewed-hash guard
        // still compares the hash that was on screen. What changes is that
        // `validateRestore` now refuses a target whose uid has stopped
        // answering -- including one deleted and recreated under the same
        // name. A read that fails is a refusal, not a shrug: the draft is
        // kept, and "I could not find out" is not "it is still there".
        await confirmClusters(state, api);
        const result = await submitRestore(state, api, lifecycle, { reviewedHash: reviewed });
        return result === null ? { outcome: "abandoned" } : result;
      }, { about: { hash: reviewed, restoreName: (prepared || {}).restoreName } });
    }, lifecycle);
  }
}

/** THE SELECTOR'S SEARCH, filtered IN PLACE.
 *
 *  The rows are all rendered and the query hides the ones that do not match,
 *  rather than re-rendering the table on every keystroke: a re-render replaces
 *  the input the operator is typing into and takes the caret with it. Each row
 *  carries its own haystack in `data-search`, so the filter never counts
 *  positions and never has to be told the list again.
 *
 *  `style.display` AND `hidden`, on purpose. Below 720 px the stylesheet turns
 *  every `table.grid` row into a card with `display: block`, and an author
 *  rule beats the user agent's `[hidden] { display: none }`; an inline style
 *  beats both. `hidden` is still set, because that is what an assistive
 *  technology reads. */
function wireSelector(node, state, lifecycle) {
  const search = node.querySelector("#point-search");
  if (search === null) {
    return;
  }
  const rows = Array.from(node.querySelectorAll("tr[data-search]"));
  const empty = node.querySelector("#no-match");
  const filter = () => {
    if (!active(lifecycle)) {
      return;
    }
    state.query = String(search.value);
    let shown = 0;
    for (const row of rows) {
      const matches = matchesQuery(row.getAttribute("data-search"), state.query);
      row.hidden = !matches;
      row.style.display = matches ? "" : "none";
      if (matches) {
        shown += 1;
      }
    }
    if (empty !== null) {
      empty.hidden = shown !== 0 || rows.length === 0;
    }
  };
  listen(search, "input", filter, lifecycle);
  listen(search, "change", filter, lifecycle);
}

/** THE TARGET SELECTOR'S SEARCH, filtered in place.
 *
 *  It hides options and never removes them, and never touches `selected`, so
 *  the answer this form would submit is the same before and after a search. It
 *  issues no request: the list was read once by the mount half and this is a
 *  filter over what is already on screen. */
function wireTargetSearch(node, lifecycle) {
  const search = node.querySelector("#target-cluster-search");
  const select = node.querySelector("#target-cluster");
  const note = node.querySelector("#target-cluster-no-match");
  if (search === null || select === null) {
    return;
  }
  listen(search, "input", () => {
    if (!active(lifecycle)) {
      return;
    }
    const visible = filterSelectorOptions(select, search.value);
    if (note !== null) {
      note.hidden = visible > 0;
    }
  }, lifecycle);
}

/** A form control's value. Kept here rather than inline so the submit region
 *  above stays free of every string-transforming token: what a viewer typed is
 *  read and trimmed HERE, and what is submitted is the document rendered from
 *  it, untouched. */
function valueOf(field) {
  return String(field.value).trim();
}

// DOWNLOAD-BEGIN
//
// THE DOWNLOAD IS CLIENT-SIDE AND WRITES NOTHING TO THE CLUSTER. A `Blob` over
// exactly the bytes in the `<pre>`, named after the minted restore, handed to
// the browser's own save flow. No request is issued, no object is created, and
// no identifier outside this origin is named -- `ui_lint.rs`'s
// `the_plan_download_writes_no_scheme_and_no_cluster_write` asserts all three.
// This region is also the only place in this file the plan document's file
// extension appears, which is what lets the submit-region scan forbid that token
// without forbidding the download.
function downloadPlan(prepared) {
  const blob = new Blob([prepared.bytes], { type: "text/yaml" });
  const handle = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.setAttribute("href", handle);
  anchor.setAttribute("download", prepared.restoreName + ".yaml");
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(handle);
}
// DOWNLOAD-END
