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

import { CONSOLE, apiClient } from "../client.js";
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
  owesRead,
  readDraft,
  readOptions,
  refusal,
  resolveExisting,
  watchMutation,
} from "../lifecycle.js";
import {
  COPY_CAVEAT,
  RESTORE_IMMUTABLE_SENTENCE,
  badge,
  bucketOf,
  cell,
  COMPLETION_GUIDANCE,
  copyBlock,
  datagrid,
  disableKeepingFocus,
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
  announce,
  listVerifiedNote,
  coveredCell,
  flagBadge,
  isBlockingRow,
  isDraftApprovalRow,
  readyButForDraftApproval,
  when,
  windowMessage,
} from "../render.js";
import { defaultTopicPrefix, TARGET_MODES, preparePlanDocument } from "../plan.js";
import {
  clusterUid,
  filterSelectorOptions,
  probeLine,
  probeState,
  probeSummary,
  readClusterSelection,
  renderClusterSelector,
  resolveClusterSelection,
} from "../select.js";
import { isObjectName, itemsOf } from "./clusters.js";
import { listD3, readCatalogPoints, readD3, readOperation } from "../operation-watch.js";
import { renderPreflight } from "./destinations.js";
import { backupBadge, validVerification } from "./backups.js";
import { COUNTERSIGN_COMMAND, approvalAuthorizes, restoreOperationRoute } from "./approvals.js";

const PLURAL = "restores";
const CLUSTERS = "kafkaclusters";
const BACKUPS = "backups";
const APPROVALS = "approvals";

let readinessAttempts = 0;
let readinessNonce = null;

/** One deliberate readiness click is one observation of mutable external
 * state. A per-load nonce and click ordinal keep its idempotency retries
 * stable while ensuring "Check this plan again" does not replay the earlier
 * answer after a target topic or saved reference changes. */
export function nextRestoreReadinessAttempt(ns, pointUid) {
  if (readinessNonce === null) {
    const source = globalThis.crypto;
    if (source === undefined || source === null || typeof source.getRandomValues !== "function") {
      throw refusal(
        "this page will not start a readiness check here: minting a distinct check needs the " +
        "platform's random source, and it is unavailable",
      );
    }
    const bytes = source.getRandomValues(new Uint8Array(16));
    readinessNonce = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  }
  readinessAttempts += 1;
  return String(ns) + "." + String(pointUid) + "." + readinessNonce + "-" +
    String(readinessAttempts);
}

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
  // PLAT-08.2: the evidence store is its own choice. A saved point keeps the
  // evidence destination it chose BY IDENTITY (name beside uid); a legacy
  // point keeps whether the evidence store shares the archive store's
  // settings, and its own four when it does not. None is a credential.
  "evidenceDestination", "evidenceDestinationUid",
  "evidenceSameAsArchive", "evidenceEndpoint", "evidenceRegion", "evidencePathStyle",
  "evidenceAllowHttp",
  // PLAT-15.2: which catalog point the draft was made for (`""` for a Backup),
  // and the topic list typed for it -- a catalog point publishes none.
  "catalogPointId", "catalogTopics",
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
  // PLAT-08.2: the two saved references, each beside the control it is about.
  ["evidenceDestinationRef", "evidenceDestination"],
  ["spec.evidenceDestinationRef", "evidenceDestination"],
  // PLAT-19.2: the change ticket a Governed policy requires (D0).
  ["ticket", "ticket"],
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
  const route = { ns: "", uid: "", backup: "", retryOf: "", catalog: "", point: "", step: 0 };
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
    } else if (key === "catalog") {
      // PLAT-15.2: a catalog-verified point, named by the catalog that lists
      // it and by its content-derived id. Every other value the link may carry
      // (the receipt key, the digests) is IGNORED here: the wizard reads the
      // point again from the product API and builds the binding from that
      // answer, never from an address somebody could have edited.
      route.catalog = value;
    } else if (key === "point") {
      route.point = value;
    } else if (key === "retryOf") {
      // PLAT-11.2: the failed Restore this one retries. It is carried for the
      // PREFIX it derives and for the banner that names it, and for nothing
      // else -- the wizard reads no field of that object and writes to none.
      route.retryOf = value;
    } else if (key === "step") {
      // MCP-29: WHICH OF THE SIX STEPS IS ON SCREEN, 1 to 6. A deep link to a
      // step; `0` -- no step named, or a value that is not one -- opens the
      // first. It selects what is SHOWN and nothing else: every step's inputs
      // are on the page whichever one is visible, so the plan, its hash and
      // the readiness binding do not depend on it.
      route.step = /^[1-6]$/.test(value) ? Number(value) : 0;
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

/** THE SAME ADDRESS ON ANOTHER STEP (MCP-29): `hash` with `step=` set to the
 *  1-based `index + 1`, every other parameter kept exactly as it was spelled.
 *  The wizard writes this into the address as the reader moves between steps,
 *  so a reload or a copied link opens the step that was on screen. Pure. */
export function restoreStepRoute(hash, index) {
  const text = typeof hash === "string" ? hash : "";
  const question = text.indexOf("?");
  const route = question === -1 ? text : text.slice(0, question);
  const pairs = question === -1
    ? []
    : text.slice(question + 1).split("&").filter((pair) => pair.length > 0 &&
      pair.slice(0, pair.indexOf("=") === -1 ? pair.length : pair.indexOf("=")) !== "step");
  const n = typeof index === "number" && index >= 0 && index < STEPS.length ? index + 1 : 1;
  pairs.push("step=" + String(n));
  return route + "?" + pairs.join("&");
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
export function recoveryPoints(backups, accept) {
  // `accept` widens the filter for a caller that can offer more than a run's
  // own window -- the schedule detail offers a run from its catalog row
  // (PLAT-15.2) -- and leaves the ORDER, which is this function's, alone.
  return itemsOf(backups)
    .filter(typeof accept === "function" ? accept : isRecoveryPoint)
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

/** THE SIGNED VERDICT A POINT CARRIES, by the Backups page's own badge rule
 *  (`backups.js` `backupBadge`) and in BOTH modes (POC-P2).
 *
 *  This used to print `status.evidence.verification.result` as a word. A
 *  shared-console LIST item carries no `status.evidence` at all -- the product
 *  API publishes the run's verdict on a list as `OperationSummary`
 *  (`verificationState`, `verifiedSuccess`), which `ui/client.js` keeps under
 *  `status.__summary` -- so every point the shared console offered read
 *  `unverified`, beside an API that said `verificationState: valid,
 *  verifiedSuccess: true`. The badge rule already reads the summary when the
 *  object carries no recorded block, and is green exactly when the Backups
 *  and History tables are: never for a verdict that is not `Valid` on an
 *  accepted basis with exit 0, and never "unverified" for one that is. */
export function pointSigned(backup) {
  return backupBadge(((backup || {}).status) || {});
}

/** The words a point's archive column carries when its verified receipt
 *  attests the manifest (see [`archiveAvailability`]). */
export const MANIFEST_ATTESTED = "manifest attested by its verified receipt";

/** WHAT "ARCHIVE AVAILABILITY" MEANS TODAY, and it is a statement about the
 *  STATUS and not about the bucket. Logweir holds no list capability against
 *  an archive and this page holds no bucket credential at all, so the nearest
 *  thing to availability the cluster can tell it is whether the run that wrote
 *  the set recorded its manifest.
 *
 *  TWO RECORDS SAY SO, AND THE SECOND IS THE ONE A CONTROLLER WRITES (POC-P2).
 *  `status.manifestKey` is a field of the `Backup` CRD that no controller in
 *  this tree writes (`weirkeeper` records the signed receipt under
 *  `status.evidence` instead), so reading it alone called every real point
 *  "no manifest recorded" -- in legacy mode, and in the shared console, where
 *  the product API projects the same absent field. The receipt IS the record:
 *  a backup receipt names its manifest key exactly when its run exited 0
 *  (`logweir-core` `backup_receipt.rs`, "exit_code == 0 iff
 *  archive.manifest_key is non-empty"), so a receipt the controller verified,
 *  on a run that exited 0, attests the manifest. That is the badge rule's
 *  green condition, read the same way in both modes: the recorded block when
 *  the object carries one, else the list summary's `verifiedSuccess`. A point
 *  with neither is still offered, and says what is not known.
 *
 *  PLAT-15.1's durable catalog is what turns this into a real answer: it
 *  records, per set, whether the objects are still there. Until then this page
 *  says exactly what it knows and no more. */
export function archiveAvailability(backup) {
  const status = (backup || {}).status || {};
  if (typeof status.manifestKey === "string" && status.manifestKey.length > 0) {
    return "manifest recorded";
  }
  return receiptVerified(status) ? MANIFEST_ATTESTED : "no manifest recorded";
}

/** Whether a run's own receipt verified and the run exited 0: the Backup badge
 *  rule's green condition, from the recorded block when the object carries
 *  one and from the console list summary when it carries only that. */
function receiptVerified(status) {
  const s = status || {};
  if (((s.evidence || {}).verification) === undefined && s.__summary !== undefined) {
    const summary = s.__summary || {};
    return summary.verifiedSuccess === true && summary.verificationState === "valid";
  }
  return validVerification(s) !== null && s.exitCode === 0;
}

// ------------------------------------ the catalog-verified point (PLAT-15.2)
//
// A RECOVERY POINT IS A SIGNED RECEIPT IN AN ARCHIVE, NOT A CUSTOM RESOURCE.
// The durable catalog (D3 section 5) lists every point an archive holds, read
// back from object storage by a sync Job and judged by the controller against
// the namespace's trust -- so a cluster that lost every `Backup` object, or a
// fresh installation pointed at an archive another one wrote, still has the
// points. This section is how the wizard restores one of them.
//
// THE RULE IS D3 section 5.5 step 4 AND THE CONTROLLER-VERDICT PRECEDENCE
// RULE (`dffe118`, `95c2279`): the catalog decides only where the controller
// could not look. A point is offered from the catalog only when
//
//   * the product API published the row `selectable: true` -- the
//     controller's own conjunction of "the archive can serve it" and "its
//     receipt verifies under a key this namespace accepts", joined server
//     side with the namespace's `Backup` verdicts (`backupVerdict`);
//   * that join was COMPLETE (`backupVerdictsIncomplete` absent) -- a join the
//     API could not finish cannot say that no `Backup` refused this receipt,
//     and this page does not guess;
//   * the row carries everything the plan binding needs, unredacted: the
//     point id, the receipt key, and both digests;
//   * and, when the offer came from a `Backup` (a destination-backed run the
//     controller could not verify itself, so it wrote no window), that
//     Backup's OWN verdict is absent or `NotAttempted` -- never a reached
//     refusal -- and the row is the one for that run's receipt.
//
// AND THE PLAN IS BOUND TO THE POINT, NOT TO A NAME. The plan carries
// `source.point {point_id, receipt_key, receipt_sha256, manifest_sha256}` and
// `source.backup` pinned to the point's set; the runner re-reads the receipt
// and the manifest those name BEFORE it constructs a client and refuses a
// mismatch with exit 3 `PointBindingMismatch` (execution contract v2). The
// approver signs those bytes, so the approval covers WHICH archive object the
// restore recovers from.

/** How many points one catalog page read asks for. */
export const CATALOG_POINT_PAGE = 200;

/** How many cursor pages the wizard follows looking for one point. The view is
 *  at most 5000 points (D3 section 5.3), so 25 pages of 200 read all of it. */
export const CATALOG_POINT_PAGE_BUDGET = 25;

/** The marker the product's archive-key redactor leaves behind. A key that
 *  carries it is the redactor's output and not a key, and it is never put
 *  into a plan binding (`ui/pages/catalog.js` re-exports this). */
export const REDACTION_MARKER = "[redacted]";

/** Whether a published archive key is the redactor's output rather than a key. */
export function isRedacted(value) {
  return typeof value === "string" && value.indexOf(REDACTION_MARKER) !== -1;
}

/** The two verdicts that let a catalog row answer for a `Backup`: none at all,
 *  or `NotAttempted` -- the controller could not look. Everything else it
 *  wrote is a verdict it REACHED (`Valid` included: a Valid run has its own
 *  window and never needs the catalog's). */
export const DEFERRING_VERDICTS = Object.freeze(["NotAttempted"]);

// THE RUN'S OWN VERDICT, AS READ FOR THIS OFFER. A console-mode `Backup` LIST
// does not publish a run's verification (`ui/client.js` merges it only on a
// detail read, and even then keeps only three of the operation's six words),
// so "no verdict on this object" there means "not published", never "the
// controller wrote none". The verdict an offer relies on is therefore READ --
// `readOwnVerdict`, one operation read -- and kept beside the object here, out
// of the object itself.
const ownVerdicts = new WeakMap();

/** The word a console-mode run's verdict reads as until it has been read:
 *  never a deferring verdict, so an unread run is never offered. */
export const UNREAD_VERDICT = "Unread";

/** Records what the operation read found for `backup`: its verdict (`null` =
 *  none written) and, when the read carried one, the digest of the receipt it
 *  signed. Returns the object. */
export function noteOwnVerdict(backup, verdict, receiptSha256) {
  if (backup !== null && typeof backup === "object") {
    ownVerdicts.set(backup, typeof verdict === "string" && verdict.length > 0 ? verdict : null);
    if (typeof receiptSha256 === "string" && receiptSha256.length > 0) {
      ownReceipts.set(backup, receiptSha256);
    }
  }
  return backup;
}

const ownReceipts = new WeakMap();

/** The receipt digest a run reported: the one an operation read noted, else
 *  the custom resource's `status.evidence.receiptSha256`, else `null`. */
export function backupReceiptOf(backup) {
  if (backup !== null && typeof backup === "object" && ownReceipts.has(backup)) {
    return ownReceipts.get(backup);
  }
  const digest = ((((backup || {}).status || {}).evidence) || {}).receiptSha256;
  return typeof digest === "string" && digest.length > 0 ? digest : null;
}

/** The receipt digest from what `readOperation` answers, or `null`. */
export function ownReceiptOf(read) {
  const r = read || {};
  const e = r.evidence;
  if (e !== null && typeof e === "object" && typeof e.payloadSha256 === "string") {
    return e.payloadSha256;
  }
  const digest = ((((r.status || {}).evidence) || {})).receiptSha256;
  return typeof digest === "string" && digest.length > 0 ? digest : null;
}

/** What the Backup's own evidence verdict is, or `null` when it wrote none.
 *
 *  A verdict noted by [`noteOwnVerdict`] wins. Otherwise a custom resource's
 *  own `status.evidence.verification.result` is read -- and a console-mode
 *  projection, which does not carry that field on a list, answers
 *  [`UNREAD_VERDICT`] rather than an absence it cannot vouch for. */
export function backupOwnVerdict(backup) {
  if (backup !== null && typeof backup === "object" && ownVerdicts.has(backup)) {
    return ownVerdicts.get(backup);
  }
  const b = backup || {};
  if (((b.__contract || {}).mode) === CONSOLE) {
    return UNREAD_VERDICT;
  }
  const verification = ((b.status || {}).evidence || {}).verification || {};
  return typeof verification.result === "string" && verification.result.length > 0
    ? verification.result
    : null;
}

/** The product API's normalized verification states, in the custom
 *  resource's words. `pending` is the API's word for BOTH "no verdict written
 *  yet" and an evidence-fetch Job's `Pending` (`logweir-api` `status.rs` maps
 *  `None | Some("Pending")` to it), so it cannot be read as the absence this
 *  rule defers on: it reads as `Pending`, which waits for the controller's own
 *  answer. `unknown` -- an `Untrusted`, or a word this build does not know --
 *  is never deferred on either. Legacy mode reads the custom resource itself,
 *  where an absent verdict IS an absence. */
const OPERATION_VERDICTS = Object.freeze({
  notAttempted: "NotAttempted",
  pending: "Pending",
  valid: "Valid",
  invalid: "Invalid",
  noEvidence: "NoEvidence",
  unknown: "Unknown",
});

/** The run's own verdict from what `readOperation` answers: the product API's
 *  `Operation` (console) or the custom resource (legacy). */
export function ownVerdictOf(read) {
  const r = read || {};
  const v = r.verification;
  if (v !== null && typeof v === "object" && typeof v.state === "string") {
    return Object.prototype.hasOwnProperty.call(OPERATION_VERDICTS, v.state)
      ? OPERATION_VERDICTS[v.state]
      : "Unknown";
  }
  const verification = (((r.status || {}).evidence) || {}).verification || {};
  return typeof verification.result === "string" && verification.result.length > 0
    ? verification.result
    : null;
}

/** Whether a `Backup`'s own verdict lets the catalog decide for it. */
export function catalogMayAnswerFor(backup) {
  const verdict = backupOwnVerdict(backup);
  return verdict === null || DEFERRING_VERDICTS.indexOf(verdict) !== -1;
}

/** THE POINTS IN TIME A RUNNER ACCEPTS FOR A COVERED WINDOW
 *  (WIZARD-DEFAULT-PIT-EXCLUSIVE).
 *
 *  A covered window -- `Backup.status.windowCovered`, or a catalog point's
 *  `coveredFrom`/`coveredTo` -- is HALF-OPEN: `fromMs` is the oldest segment's
 *  inclusive start and `toMs` is the newest segment's end PLUS ONE
 *  millisecond, the first instant the archive does not cover
 *  (`crates/logweir/src/backup/phase_run.rs`, `docs/stability.md`). The
 *  runner's `archive.coverage` check then refuses a point in time AT OR
 *  BEFORE the floor (`PointInTimeBeforeCoverage`: a restore window of one
 *  instant restores nothing) and one AFTER the newest record
 *  (`PointInTimeAfterCoverage`) -- `oldest < pit <= newest`
 *  (`crates/logweir/src/check/kinds/restore.rs` `coverage_row`). So the
 *  closed range a plan may name is `[fromMs + 1, toMs - 1]`, and its end is
 *  the default. `null` when that range is empty or the window is unreadable.
 *  The restore window itself stays closed at both ends (the engine's filter
 *  is `timestamp >= start && timestamp <= end`), so the default restores the
 *  newest record. */
export function restorableWindow(covered) {
  const c = covered || {};
  if (typeof c.fromMs !== "number" || typeof c.toMs !== "number") {
    return null;
  }
  const first = c.fromMs + 1;
  const last = c.toMs - 1;
  return last < first ? null : { fromMs: first, toMs: last };
}

/** The catalog's covered window, half-open exactly as the receipt writes it
 *  (`coveredTo` exclusive) -- the shape `Backup.status.windowCovered` has, so
 *  one rule ([`restorableWindow`]) turns either into the points in time a plan
 *  may name. `null` when either bound is unreadable. */
export function catalogWindow(entry) {
  const e = entry || {};
  const from = epochMs(e.coveredFrom);
  const to = epochMs(e.coveredTo);
  if (from === null || to === null) {
    return null;
  }
  return { fromMs: from, toMs: to };
}

const POINT_ID_SHAPE = /^lwp1-[0-9a-f]{32}$/;
const DIGEST_SHAPE = /^sha256:[0-9a-f]{64}$/;

/** WHETHER ONE CATALOG ROW MAY BE OFFERED AS A RESTORE, and if not, why.
 *
 *  `page` is the point page the row came from (its `viewExpired` and
 *  `backupVerdictsIncomplete` are facts about every row on it). Returns
 *  `{offer: true, reason: null}` or `{offer: false, reason: <sentence>}`.
 *  Pure; no clock. The ORDER of the refusals is the order an operator would
 *  repair them in, and each names the value that is wrong. */
export function catalogPointOffer(entry, page) {
  const e = entry || null;
  const p = page || {};
  const no = (reason) => ({ offer: false, reason: reason });
  if (e === null) {
    return no("the catalog's view does not list this point");
  }
  if (p.viewExpired === true) {
    return no("the catalog's view has aged out (viewExpired); sync the catalog again");
  }
  if (typeof e.backupVerdict === "string" && e.backupVerdict.length > 0) {
    return no(
      "the controller refused this point's own Backup evidence (" + e.backupVerdict + "); a " +
        "catalog row never outranks a verdict the controller reached",
    );
  }
  if (typeof p.backupVerdictsIncomplete === "string" && p.backupVerdictsIncomplete.length > 0) {
    return no(
      "the product API could not read every Backup verdict in this namespace (" +
        p.backupVerdictsIncomplete + "), so it cannot say that no Backup refused this " +
        "receipt; this page does not offer a point over a verdict nobody could read",
    );
  }
  if (e.selectable !== true) {
    return no(
      "the catalog does not list this point as restorable: availability " +
        String(e.availability || "unknown") + ", verification " +
        String(e.verification || "unknown") +
        (typeof e.remedy === "string" && e.remedy.length > 0 ? " (" + e.remedy + ")" : ""),
    );
  }
  if (typeof e.pointId !== "string" || !POINT_ID_SHAPE.test(e.pointId)) {
    return no("the point id is not `lwp1-` plus 32 lowercase hex characters");
  }
  if (typeof e.receiptKey !== "string" || e.receiptKey.trim().length === 0) {
    return no("the catalog published no receipt key for this point");
  }
  if (isRedacted(e.receiptKey)) {
    return no(
      "the catalog published this point's receipt key as `" + e.receiptKey + "`: the " +
        "archive-key redactor rewrote it, so the plan binding cannot name the object the " +
        "runner must re-read; re-sync the catalog with a runner that keeps a ULID run id",
    );
  }
  if (typeof e.receiptSha256 !== "string" || !DIGEST_SHAPE.test(e.receiptSha256)) {
    return no("the catalog published no well-formed receipt digest for this point");
  }
  if (typeof e.manifestSha256 !== "string" || !DIGEST_SHAPE.test(e.manifestSha256)) {
    return no(
      "the catalog published no manifest digest for this point, and the plan binding needs " +
        "one: the runner checks the manifest the receipt attests before any data moves",
    );
  }
  if (typeof e.backupId !== "string" || !/^[A-Za-z0-9._-]{1,128}$/.test(e.backupId)) {
    return no("the catalog published no usable backup set id for this point");
  }
  if (restorableWindow(catalogWindow(e)) === null) {
    return no("the catalog published no covered window a point in time can be chosen in");
  }
  return { offer: true, reason: null };
}

/** The catalog rows that belong to ONE run: the run's set, and -- when the
 *  run reported the digest of the receipt it signed -- that exact receipt. A
 *  set can hold several points (an upstream append writes a second receipt
 *  under the same backup id), and a join that took the first would bind the
 *  plan to a point this run did not produce. */
export function catalogRowsForBackup(backup, points) {
  const status = (backup || {}).status || {};
  const id = typeof status.backupId === "string" ? status.backupId : "";
  if (id.length === 0 || !Array.isArray(points)) {
    return [];
  }
  const digest = backupReceiptOf(backup);
  return points.filter((point) => {
    const p = point || {};
    if (p.backupId !== id) {
      return false;
    }
    return digest === null || p.receiptSha256 === digest;
  });
}

/** CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW: whether a `Backup` the controller
 *  wrote no window for may be restored from the catalog's, and from which row.
 *
 *  Only a `Succeeded` run with a set, whose OWN verdict is absent or
 *  `NotAttempted`, and for which exactly one catalog row answers that
 *  [`catalogPointOffer`] offers. A run with its own window is not this
 *  function's to answer ([`isRecoveryPoint`] already offers it), and a run
 *  whose verdict the controller reached -- `Invalid`, `Untrusted`, or a word
 *  this build does not know -- is never made restorable by a row. A run still
 *  `Pending` (its evidence-fetch Job is reading the receipt) is not offered
 *  either: the console waits for the controller's own answer rather than
 *  pre-empting it with the catalog's, which is stricter than the server-side
 *  join (`catalog_view::is_reached_refusal` defers on `Pending`) and never
 *  looser. */
export function backupCatalogOffer(backup, points, page) {
  const b = backup || null;
  const no = (reason) => ({ offer: false, reason: reason, entry: null });
  if (b === null) {
    return no("no Backup");
  }
  if (isRecoveryPoint(b)) {
    return no("the controller wrote this run's own covered window; it needs no catalog row");
  }
  const status = b.status || {};
  if (status.phase !== "Succeeded") {
    return no("the run is in phase " + String(status.phase || "(none)"));
  }
  if (typeof status.backupId !== "string" || status.backupId.length === 0) {
    return no("the run recorded no backup set");
  }
  if (!catalogMayAnswerFor(b)) {
    return no(
      "the controller reached a verdict on this run's own evidence (" + backupOwnVerdict(b) +
        "), and a catalog row never outranks it",
    );
  }
  const rows = catalogRowsForBackup(b, points);
  if (rows.length === 0) {
    return no("no catalog row answers this run's receipt");
  }
  if (rows.length > 1) {
    // TWO DIFFERENT FACTS (the PLAT-15.2 review's L-2). With no digest the
    // run cannot say which of its set's points is its own; WITH one, every
    // row is that same receipt, listed more than once -- by more than one
    // catalog over this archive -- and the page will not pick a catalog.
    return no(backupReceiptOf(b) === null
      ? "the catalog lists " + String(rows.length) + " points for this run's set and the run " +
        "reported no receipt digest to choose between them"
      : String(rows.length) + " catalog rows list this run's own receipt -- more than one " +
        "catalog reads this archive -- so this page does not choose a catalog; open the point " +
        "from one catalog's page");
  }
  const verdict = catalogPointOffer(rows[0], page);
  return verdict.offer ? { offer: true, reason: null, entry: rows[0] } : no(verdict.reason);
}

// WHICH CATALOG A ROW WAS READ FROM, kept beside the row and not on it. A page
// that reads several catalogs (the schedule detail) hands its renderers one
// flat list of rows; the offer needs the catalog's NAME for the link and the
// page's flags (`viewExpired`, `backupVerdictsIncomplete`) for the rule, and
// neither is a field of the API's row. A WeakMap keeps them out of the row
// itself, so a decoded row stays exactly what the API published.
const catalogSources = new WeakMap();

/** Records that `row` was read from catalog `catalog`, which reads the
 *  `BackupDestination` named `destination` (its `spec.destinationRef.name`),
 *  on a page carrying `page`'s flags. Returns the row. */
export function noteCatalogSource(row, catalog, page, destination) {
  if (row !== null && typeof row === "object") {
    const p = page || {};
    catalogSources.set(row, {
      catalog: String(catalog || ""),
      destination: typeof destination === "string" ? destination : "",
      page: {
        viewExpired: p.viewExpired === true,
        backupVerdictsIncomplete: typeof p.backupVerdictsIncomplete === "string"
          ? p.backupVerdictsIncomplete
          : null,
      },
    });
  }
  return row;
}

/** `{catalog, page}` for a row [`noteCatalogSource`] recorded, else `null`. */
export function catalogSourceOf(row) {
  return row !== null && typeof row === "object" ? (catalogSources.get(row) || null) : null;
}

/** [`backupCatalogOffer`] over rows read from possibly several catalogs, each
 *  carrying its source: the offer, plus the catalog its row came from. A row
 *  with no recorded source is never offered -- the link would have no catalog
 *  to name.
 *
 *  ONLY A CATALOG OVER THE RUN'S OWN DESTINATION ANSWERS FOR IT (the PLAT-15.2
 *  review's L-1). A catalog over another destination that happens to list the
 *  same set id -- a copied archive, a second bucket -- describes other bytes,
 *  and the wizard would refuse the link it offered
 *  (`an_offer_from_a_backup_must_be_read_through_the_destination_that_run_froze`).
 *  A run with no `spec.destinationRef` carries an inline archive no catalog
 *  reads, so it is never offered from one. */
export function backupCatalogOfferFrom(backup, points) {
  const own = String(((((backup || {}).spec || {}).destinationRef) || {}).name || "");
  if (own.length === 0) {
    return { offer: false, reason: "the run names no BackupDestination, so no catalog reads " +
      "its archive", entry: null, catalog: "" };
  }
  const ours = (Array.isArray(points) ? points : []).filter((point) => {
    const source = catalogSourceOf(point);
    return source !== null && source.destination === own;
  });
  const rows = catalogRowsForBackup(backup, ours);
  const source = rows.length === 1 ? catalogSourceOf(rows[0]) : null;
  const offer = backupCatalogOffer(backup, ours, source === null ? {} : source.page);
  if (offer.offer && (source === null || source.catalog.length === 0)) {
    return { offer: false, reason: "the catalog row carries no catalog name", entry: null,
      catalog: "" };
  }
  return Object.assign({}, offer, { catalog: source === null ? "" : source.catalog });
}

/** How many runs' own verdicts one page reads for catalog-window offers. */
export const OWN_VERDICT_READ_BUDGET = 25;

/** READS THE OWN VERDICT OF EVERY RUN A CATALOG ROW COULD ANSWER FOR -- a
 *  `Succeeded` run with a set, no window of its own, and at least one row of
 *  its set in `points` -- and notes it beside the run ([`noteOwnVerdict`]).
 *  At most [`OWN_VERDICT_READ_BUDGET`] reads; a run past the budget, or whose
 *  read fails, keeps no noted verdict and is therefore not offered. Returns
 *  how many were read. Throws only a cancelled read. */
export async function readOwnVerdicts(runs, points, readVerdict, lifecycle) {
  let read = 0;
  for (const run of Array.isArray(runs) ? runs : []) {
    const status = (run || {}).status || {};
    if (isRecoveryPoint(run) || status.phase !== "Succeeded" ||
      typeof status.backupId !== "string" || status.backupId.length === 0) {
      continue;
    }
    if (!(Array.isArray(points) && points.some((p) => (p || {}).backupId === status.backupId))) {
      continue;
    }
    if (read >= OWN_VERDICT_READ_BUDGET) {
      break;
    }
    read += 1;
    try {
      const own = await readVerdict(String(((run || {}).metadata || {}).name || "")) || {};
      noteOwnVerdict(run, own.verdict, own.receiptSha256);
    } catch (error) {
      if (cancelled(error, lifecycle)) {
        throw error;
      }
      // UNREAD IS NOT DEFERRING: the run keeps no noted verdict and is not
      // offered from the catalog.
    }
  }
  return read;
}

/** The link that opens the wizard on ONE catalog point:
 *  `#/restore?ns=<ns>&catalog=<name>&point=<pointId>`, plus the run's own
 *  identity when the offer came from a `Backup`. The point id is the identity;
 *  everything the plan is built from is read again from the product API. */
export function restoreCatalogPointRoute(ns, catalog, pointId, backup) {
  const n = typeof ns === "string" ? ns.trim() : "";
  const meta = ((backup || {}).metadata) || {};
  return (
    "#/restore?" +
    (n.length > 0 ? "ns=" + encodeURIComponent(n) + "&" : "") +
    "catalog=" + encodeURIComponent(String(catalog || "")) +
    "&point=" + encodeURIComponent(String(pointId || "")) +
    (typeof meta.name === "string" && meta.name.length > 0
      ? "&backup=" + encodeURIComponent(meta.name)
      : "") +
    (typeof meta.uid === "string" && meta.uid.length > 0
      ? "&uid=" + encodeURIComponent(meta.uid)
      : "")
  );
}

/** FIND ONE POINT IN A CATALOG'S VIEW, by its id, following the cursor.
 *
 *  `readPoints(query)` is one page read. Returns `{entry, page}`: the row (or
 *  `null`) and the page-level facts every page read carried -- `viewExpired`
 *  and `backupVerdictsIncomplete` from ANY page, because a flag on one page is
 *  a fact about the whole read. A view that has more pages than the budget and
 *  did not contain the point is `{entry: null, page: {budgetExhausted: true}}`,
 *  said as such and never as "not in the catalog". */
export async function findCatalogPoint(readPoints, pointId) {
  const flags = { viewExpired: false, backupVerdictsIncomplete: null, incomplete: false,
    budgetExhausted: false };
  let cursor = null;
  for (let read = 0; read < CATALOG_POINT_PAGE_BUDGET; read += 1) {
    const query = { limit: CATALOG_POINT_PAGE };
    if (cursor !== null) {
      query.cursor = cursor;
    }
    const page = (await readPoints(query)) || {};
    if (page.viewExpired === true) {
      flags.viewExpired = true;
    }
    if (typeof page.backupVerdictsIncomplete === "string" &&
      page.backupVerdictsIncomplete.length > 0) {
      flags.backupVerdictsIncomplete = page.backupVerdictsIncomplete;
    }
    if (page.incomplete === true) {
      flags.incomplete = true;
    }
    for (const item of (Array.isArray(page.items) ? page.items : [])) {
      if ((item || {}).pointId === pointId) {
        return { entry: item, page: flags };
      }
    }
    cursor = ((page.page || {}).nextCursor) || null;
    if (cursor === null || flags.incomplete) {
      return { entry: null, page: flags };
    }
  }
  flags.budgetExhausted = true;
  return { entry: null, page: flags };
}

/** The destination a catalog reads, by name, or `""` for a legacy archive. */
export function catalogDestinationName(catalog) {
  const ref = (((catalog || {}).spec || {}).destinationRef) || {};
  return typeof ref.name === "string" ? ref.name : "";
}

/** A legacy-archive catalog's `{url, secretName}`, in either spelling the two
 *  modes project (`credentialRef` from the product API, `secretRef` from the
 *  custom resource), or `null`. */
export function catalogLegacyArchive(catalog) {
  const legacy = (((catalog || {}).spec || {}).legacyArchive) || null;
  if (legacy === null || typeof legacy.url !== "string" || legacy.url.length === 0) {
    return null;
  }
  const secret = legacy.credentialRef || legacy.secretRef || {};
  return { url: legacy.url, secretName: typeof secret.name === "string" ? secret.name : "" };
}

/** THE CHOSEN CATALOG POINT, IN THE SHAPE EVERY STEP OF THIS WIZARD READS.
 *
 *  The six steps were written over a `Backup`, and they read five things off
 *  it: its identity, its set, its covered window, its topics and its frozen
 *  destination. A catalog point has all five, from different places, and this
 *  puts them where the steps look -- so the steps, the draft, the readiness
 *  request and the submit build ONE plan from ONE point, and the one field a
 *  Backup never had -- `catalogPoint`, the binding -- rides beside them.
 *
 *  THE DESTINATION IS FROZEN HERE, AT MOUNT. A destination-backed catalog's
 *  point is read through that destination; its UID and location digest as the
 *  API served them now are recorded as `spec.destinationRef.uid` and
 *  `status.locationDigest`, which is what [`confirmFrozenDestination`]
 *  re-checks before the create: a destination deleted, recreated or moved
 *  while the plan was being reviewed is a refusal, not a different archive.
 *
 *  THE TOPICS START EMPTY. The catalog view does not publish a point's topic
 *  list, so the operator names the topics to restore; the readiness check
 *  reads the manifest for exactly those names, and the runner restores
 *  nothing the set does not hold. */
export function catalogRecoveryPoint(catalog, entry, destination, backup) {
  const e = entry || {};
  const window = catalogWindow(e) || {};
  const destinationName = catalogDestinationName(catalog);
  const legacy = catalogLegacyArchive(catalog);
  const live = destination || {};
  const spec = { topics: [] };
  if (destinationName.length > 0) {
    spec.destinationRef = { name: destinationName, uid: live.uid };
    spec.archive = { url: "logweir-destination://" + destinationName };
  } else if (legacy !== null) {
    spec.archive = legacy.secretName.length > 0
      ? { url: legacy.url, secretRef: { name: legacy.secretName } }
      : { url: legacy.url };
  }
  const location = (Array.isArray(e.locations) ? e.locations : [])
    .find((l) => (l || {}).availability === "Available") || null;
  return {
    kind: "CatalogPoint",
    metadata: { name: e.pointId, uid: "" },
    spec: spec,
    status: {
      phase: "Succeeded",
      backupId: e.backupId,
      windowCovered: { fromMs: window.fromMs, toMs: window.toMs },
      locationDigest: live.locationDigest,
    },
    catalogPoint: {
      catalog: String((((catalog || {}).metadata) || {}).name || ""),
      pointId: e.pointId,
      receiptKey: e.receiptKey,
      receiptSha256: e.receiptSha256,
      manifestSha256: e.manifestSha256,
      availability: e.availability,
      verification: e.verification,
      signerKeyId: e.signerKeyId,
      recoveryPointAt: e.recoveryPointAt,
      coveredFrom: e.coveredFrom,
      coveredTo: e.coveredTo,
      locationId: location === null ? null : location.locationId,
      runId: e.runId,
      backup: backup === null || backup === undefined
        ? null
        : { name: (backup.metadata || {}).name, uid: (backup.metadata || {}).uid },
    },
  };
}

/** Whether the wizard's chosen point is a catalog point. */
export function isCatalogPoint(point) {
  return ((point || {}).catalogPoint || null) !== null &&
    typeof point.catalogPoint === "object";
}

/** The identity a catalog point is kept under in this page's registries (the
 *  readiness attempt key, the selection): the catalog and the point id. Not a
 *  Kubernetes UID, and never sent as one. */
export function catalogPointUid(point) {
  const c = ((point || {}).catalogPoint) || {};
  return "catalog/" + String(c.catalog || "") + "/" + String(c.pointId || "");
}

/** The plan's `source.point` binding for a catalog point, or `undefined` for a
 *  Backup-bound plan -- whose document therefore stays byte-identical to the
 *  one it always was. */
export function pointBindingOf(point) {
  if (!isCatalogPoint(point)) {
    return undefined;
  }
  const c = point.catalogPoint;
  return {
    pointId: c.pointId,
    receiptKey: c.receiptKey,
    receiptSha256: c.receiptSha256,
    manifestSha256: c.manifestSha256,
  };
}

/** Parses the operator's topic list: names separated by commas or whitespace,
 *  in the order typed, blanks dropped. Duplicates are KEPT, so the mapping
 *  check can refuse them by name rather than this page silently collapsing
 *  them. */
export function parseTopicList(text) {
  return String(text === undefined || text === null ? "" : text)
    .split(/[\s,]+/)
    .filter((topic) => topic.length > 0);
}

/** Sets the catalog point's topic list: the list the subset is chosen from AND
 *  the subset itself, so every named topic is restored unless unticked. */
export function setCatalogTopics(state, topics) {
  const s = state || {};
  const list = Array.isArray(topics) ? topics.slice() : [];
  if (isCatalogPoint(s.point)) {
    s.point.spec.topics = list.slice();
  }
  if (s.fields) {
    s.fields.topics = list.slice();
  }
  s.catalogTopicsText = list.join(", ");
}

/** What the recovery-point step says about a catalog point's topics. */
export const CATALOG_TOPICS_SENTENCE =
  "The catalog's view does not publish a recovery point's topic list, so name the topics to " +
  "restore, separated by commas. The readiness check reads the point's manifest for exactly " +
  "these names before an approver signs, and the runner restores nothing the backup set does " +
  "not hold.";

/** What a catalog-bound plan carries, and why. */
export const CATALOG_BINDING_SENTENCE =
  "This plan is bound to the recovery point itself: its bytes carry source.point {point_id, " +
  "receipt_key, receipt_sha256, manifest_sha256}, and the runner re-reads that receipt and the " +
  "manifest it attests BEFORE it contacts any broker. A receipt or manifest that no longer " +
  "matches is refused (exit 3, PointBindingMismatch) -- the restore never runs from a " +
  "different object than the one the approver signed for.";

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
      cell(meta.name) + "<span class=\"cell-sub\">" +
        cell(((cluster.spec || {}).bootstrapServers || []).join(", ")) + "</span>",
      cell(status.clusterId),
      // A CELL, NOT A PARAGRAPH (MCP-28): the badges and the age, with the
      // explanation one hover away.
      probeSummary(probeState(cluster, s.now, s.freshSeconds)),
      cell(archiveFor(s, meta.name)),
      cell(archiveSecretFor(s, meta.name)),
    ];
  });
  return (
    "<section class=\"step\" id=\"step-archive\" tabindex=\"-1\"><h3>1. Archive</h3>" +
    (isCatalogPoint(s.point)
      ? "<p class=\"blurb\" id=\"catalog-archive\">This restore reads the archive catalog <code>" +
        esc(s.point.catalogPoint.catalog) + "</code> reads, not an archive named by a Backup " +
        "object: the recovery point was found in that archive, and the source cluster is " +
        "never contacted by a restore. The saved connections below are context only.</p>"
      : "<p class=\"blurb\">The source cluster this restore reads an archive of. The archives " +
        "below were read from this namespace's Backup objects: this page holds no bucket " +
        "credential and lists no object storage.</p>") +
    renderSourceBinding(s) +
    table(
      ["SOURCE CLUSTER", "CLUSTER ID", "CONNECTION PROBE", "ARCHIVE", "ARCHIVE CREDENTIAL"],
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
    "<p class=\"note\" id=\"source-binding\">Source connection: <code title=\"uid " +
    esc(resolved.uid) + "\">" + esc(resolved.name) + "</code> (role: " + cell(resolved.role) +
    "). " + probeSummary(probeState(resolved.cluster, s.now, s.freshSeconds)) +
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
  const destination = savedDestinationName(s);
  if (destination.length > 0) {
    return (
      "<h4>The credential that reaches that archive</h4>" +
      "<p class=\"note\">Saved destination <code>" + esc(destination) + "</code> owns the " +
      "archive and evidence credentials. This restore uses that saved access configuration; " +
      "there is no legacy Secret override on this recovery point.</p>"
    );
  }
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
  const evidence = ((s.fields || {}).evidence) || {};
  const destination = savedDestinationName(s);
  const errors = errorsOf(s);
  if (destination.length > 0) {
    return (
      "<h4>Where that archive actually is</h4>" +
      "<p class=\"note\">These signed-plan values come from the public location and transport " +
      "settings of saved destination <code>" + esc(destination) + "</code>. They are fixed for " +
      "this recovery point; credentials and Secret values are never read or shown here.</p>" +
      storeFacts("archive", store, store.bucket + "/" + store.prefix) +
      renderEvidenceDestinationField(s, errors) +
      storeFacts("evidence", evidence, String(evidence.bucket || "") + "/" +
        String(evidence.prefix || ""))
    );
  }
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
    "<h4>Where the evidence is written</h4>" +
    "<div class=\"field\"><label for=\"evidence-bucket\">evidence bucket</label>" +
    "<input id=\"evidence-bucket\" name=\"evidenceBucket\" value=\"" +
    esc(evidence.bucket) + "\"" +
    invalidAttributes("evidence-bucket", errors.evidenceBucket) + ">" +
    fieldErrorLine("evidence-bucket", errors.evidenceBucket) +
    "<p class=\"note\" id=\"legacy-evidence-bucket\">" + esc(LEGACY_EVIDENCE_BUCKET_SENTENCE) +
    "</p>" +
    "<p class=\"note\">The evidence prefix is fixed at " + esc(EVIDENCE_PREFIX) + " by Global " +
    "Constraint 6 and is not an input: a plan naming another one is refused at phase 0, " +
    "after the approver has already signed it.</p></div>" +
    renderEvidenceStoreFields(s)
  );
}

/** One storage block, as the facts the plan signs for it. */
function storeFacts(role, block, location) {
  const b = block || {};
  return (
    "<dl id=\"" + esc(role) + "-store-facts\"><dt>" + esc(role) + " bucket / prefix</dt><dd><code>" +
    esc(location) + "</code></dd><dt>region</dt><dd><code>" +
    esc(b.region || "(default)") + "</code></dd><dt>endpoint</dt><dd><code>" +
    esc(b.endpoint || "AWS S3") + "</code></dd><dt>addressing</dt><dd data-addressing=\"" +
    esc(role) + "\">" + (b.pathStyle === true ? "pathStyle" : "virtualHosted") +
    "</dd><dt>transport</dt><dd data-transport=\"" + esc(role) + "\">" +
    (b.allowHttp === true ? "insecureHttp" : "TLS") + "</dd></dl>"
  );
}

/** Said above the evidence destination selector. */
export const EVIDENCE_DESTINATION_SENTENCE =
  "The run writes its signed scorecard and offsets to this destination's evidence store -- its " +
  "bucket under logweir/, reached with its own endpoint, addressing, transport and evidenceWrite " +
  "credential. It starts as the recovery point's own destination, so nothing has to be entered; " +
  "choosing another one keeps the archive where it is and changes only where the evidence goes, " +
  "which is part of the plan an approver signs.";

/** THE EVIDENCE DESTINATION, CHOSEN BY IDENTITY (PLAT-08.2, D2 section 9).
 *
 *  `Restore.spec` carries TWO saved references, `sourceDestinationRef` and
 *  `evidenceDestinationRef`, and the controller checks the plan's evidence
 *  block against the second one (D2 section 3.6, check 8). Until this control
 *  existed the page always sent the SOURCE destination twice, so a restore
 *  could not keep its evidence apart from the archive it reads. The option
 *  VALUE is the uid: a destination deleted and recreated under the same name
 *  is a different store reached with a different credential. */
export function renderEvidenceDestinationField(state, errors) {
  const s = state || {};
  const e = errors || {};
  const options = evidenceDestinationOptions(s);
  const pinned = s.evidenceDestination || {};
  const refused = typeof s.evidenceDestinationProblem === "string" &&
    s.evidenceDestinationProblem.length > 0;
  const own = String((((s.point || {}).spec || {}).destinationRef || {}).name || "");
  return (
    "<h4>Where the evidence is written</h4>" +
    "<div class=\"field\"><label for=\"evidence-destination\">evidence destination</label>" +
    (refused
      ? "<p class=\"refusal\" id=\"evidence-destination-refusal\">" +
        esc(s.evidenceDestinationProblem) + "</p>"
      : "") +
    "<select id=\"evidence-destination\" name=\"evidenceDestination\"" +
    invalidAttributes("evidence-destination", e.evidenceDestination) + ">" +
    (refused ? "<option value=\"\" selected>choose an evidence destination</option>" : "") +
    options.map((d) =>
      "<option value=\"" + esc(d.uid) + "\"" +
      (!refused && d.uid === pinned.uid ? " selected" : "") + ">" +
      esc(d.name + " -- " + String(d.canonicalUrl || "") + " (" +
        String(typeof d.transport === "string" ? d.transport : ((d.transport || {}).security) || "?") +
        ", " + String(((d.storage || {}).addressing) || d.addressing || "?") + ")" +
        (d.name === own ? " -- this recovery point's destination" : "")) +
      "</option>").join("") +
    "</select>" +
    fieldErrorLine("evidence-destination", e.evidenceDestination) +
    "<p class=\"note\">" + esc(EVIDENCE_DESTINATION_SENTENCE) + "</p>" +
    (s.evidenceDestinationsUnavailable === true
      ? "<p class=\"note\" id=\"evidence-destinations-unavailable\">The other destinations in " +
        "this namespace could not be read, so only the recovery point's own destination is " +
        "offered.</p>"
      : "") +
    "</div>"
  );
}

/** Said beside the "evidence store uses the archive store's settings" box. */
export const EVIDENCE_SAME_STORE_SENTENCE =
  "Ticked, the evidence block of the plan is reached with exactly the endpoint, region, " +
  "addressing and transport of the archive store above. Untick it when the evidence bucket is on " +
  "another store: the evidence store then gets controls of its own, and nothing on the archive " +
  "side changes them -- nor do they change the archive side.";

/** THE LEGACY EVIDENCE STORE'S OWN SETTINGS (PLAT-08.2, second half of defect
 *  UI-HTTPDOWNGRADE). The plan has two storage blocks and the runner reaches
 *  each with its own four settings; this page used to write the archive's four
 *  into both, so an evidence bucket on another store could not be named at
 *  all. The default is the old behaviour, SAID on screen as a ticked box, so an
 *  existing walkthrough's plan bytes do not move; unticking it gives the
 *  evidence store an endpoint, a region, an addressing box and an explicit
 *  insecure-transport box of its own, and the last defaults OFF and is set by
 *  nothing but itself. */
export function renderEvidenceStoreFields(state) {
  const s = state || {};
  const evidence = ((s.fields || {}).evidence) || {};
  const same = s.evidenceSameAsArchive !== false;
  return (
    "<label class=\"inline\" for=\"evidence-same-store\">" +
    "<input type=\"checkbox\" id=\"evidence-same-store\" name=\"evidenceSameAsArchive\"" +
    (same ? " checked" : "") + "> the evidence store uses the archive store's endpoint, " +
    "region, addressing and transport</label>" +
    "<p class=\"note\">" + esc(EVIDENCE_SAME_STORE_SENTENCE) + "</p>" +
    (same
      ? ""
      : "<fieldset class=\"evidence-store\" id=\"evidence-store\"><legend>evidence store</legend>" +
        "<div class=\"field-row\">" +
        "<div class=\"field\"><label for=\"evidence-endpoint\">evidence endpoint</label>" +
        "<input id=\"evidence-endpoint\" name=\"evidenceEndpoint\" value=\"" +
        esc(evidence.endpoint) + "\"></div>" +
        "<div class=\"field\"><label for=\"evidence-region\">evidence region</label>" +
        "<input id=\"evidence-region\" name=\"evidenceRegion\" value=\"" +
        esc(evidence.region) + "\"></div>" +
        "</div>" +
        "<label class=\"inline\" for=\"evidence-pathStyle\">" +
        "<input type=\"checkbox\" id=\"evidence-pathStyle\" name=\"evidencePathStyle\"" +
        (evidence.pathStyle === true ? " checked" : "") + "> path_style addressing for the " +
        "evidence store</label>" +
        "<p class=\"note\">" + ADDRESSING_NOTE + "</p>" +
        "<label class=\"inline\" for=\"evidence-allow-insecure\">" +
        "<input type=\"checkbox\" id=\"evidence-allow-insecure\" name=\"evidenceAllowHttp\"" +
        (evidence.allowHttp === true ? " checked" : "") +
        "> Allow insecure HTTP to the evidence store (explicit, local development only)</label>" +
        "<p class=\"complaint\" id=\"evidence-insecure-transport-warning\">" +
        EVIDENCE_INSECURE_TRANSPORT_WARNING + "</p>" +
        "</fieldset>")
  );
}

/** The warning under the evidence store's own insecure-transport box. */
export const EVIDENCE_INSECURE_TRANSPORT_WARNING =
  "Ticking this writes allow_" + "http" + " true into the EVIDENCE block of the plan an approver " +
  "signs: the runner then writes the signed scorecard to the evidence store over an unencrypted " +
  "connection, with the evidence credential in the clear. It is independent of the archive " +
  "store's box and of every addressing box, and it defaults to off.";

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
  "runner then reaches the archive store -- and the evidence store too, while the evidence " +
  "store uses the archive store's settings -- over an unencrypted connection: the " +
  "object-store credential and every restored record cross the network in the clear. " +
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
export function renderNoCompletedBackup(ns, backups, catalogOffers) {
  const offered = (((catalogOffers || {}).offers) || []).length > 0;
  return (
    "<h2>Restore wizard</h2>" +
    // A NAMESPACE WITH NO COMPLETED BACKUP MAY STILL HOLD AN ARCHIVE (PLAT-15.2):
    // the cluster lost its objects, or this is a fresh installation pointed at
    // an archive another one wrote. The connected archives' points come first,
    // and the "wait for a scheduled run" advice is only the whole story when
    // there are none.
    (offered
      ? ""
      : "<div class=\"empty-state\"><p class=\"note\">" + NO_COMPLETED_BACKUP_SENTENCE +
        "</p><p class=\"note\" id=\"connect-archive-hint\">" + esc(CONNECT_ARCHIVE_HINT) +
        " <a href=\"#/catalog?ns=" + esc(encodeURIComponent(String(ns || ""))) +
        "\">Connect an existing archive</a></p></div>") +
    renderCatalogOffers(ns, catalogOffers) +
    "<section class=\"step\" id=\"step-catalog\" tabindex=\"-1\"><h3>What this namespace holds</h3>" +
    renderCatalogTable(itemsOf(backups), null) +
    "<p class=\"note\">" + NO_SUCCEEDED_SENTENCE + "</p></section>"
  );
}

/** Said where a namespace holds nothing to restore from. */
export const CONNECT_ARCHIVE_HINT =
  "If the archive survived and the Backup objects did not -- a rebuilt cluster, or a fresh " +
  "installation -- connect the archive: its catalog lists every point it holds, and a point " +
  "the catalog verifies is restorable from here without any Backup object.";

/** Every `Backup` in the list with its set, its covered range CONVERTED TO RFC
 *  3339 (interface I22: the field is two integers, and a viewer reading
 *  `1757253900000` learns nothing) and its phase -- and the chosen one, named.
 *  This is the CATALOG, not the selector: it shows the running and failed runs
 *  too, because "what is here" is the question it answers. */
export function renderCatalogTable(backups, chosenName, grid) {
  const rows = itemsOf(backups).map((backup) => {
    const status = backup.status || {};
    const covered = status.windowCovered || {};
    const name = (backup.metadata || {}).name;
    return [
      "<code>" + cell(status.backupId) + "</code>",
      coveredCell(rfc3339(covered.fromMs), rfc3339(covered.toMs)),
      cell(status.records),
      cell(status.phase),
      cell(name === chosenName && typeof name === "string" ? name + " (chosen)" : name),
    ];
  });
  // A DATAGRID WHERE THE CALLER NAMES ONE: 258 runs are 258 rows, and the
  // selector used to lay out every one of them below its own table (MCP-26).
  return table(
    ["BACKUP SET", "COVERED", "RECORDS", "PHASE", "BACKUP"],
    rows,
    "no Backup names this archive in this namespace",
    undefined,
    grid,
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
  if (isCatalogPoint(point)) {
    return renderCatalogPointStep(s);
  }
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
      ["covered from", when(rfc3339(covered.fromMs))],
      ["covered to", when(rfc3339(covered.toMs))],
      ["topics", topics.length === 0 ? cell(null) : esc(topics.join(", "))],
      ["records", cell(status.records)],
      ["signed", pointSigned(point)],
      ["archive", cell((spec.archive || {}).url) + " -- " + esc(archiveAvailability(point))],
    ]) +
    "<p class=\"note\">" + POINT_PINNED_SENTENCE + "</p>" +
    "<div class=\"actions\"><a class=\"nav-link\" id=\"choose-another-point\" href=\"" +
    esc(restoreSelectorRoute(s.ns)) + "\">Choose a different recovery point</a></div>" +
    "<h4>What this namespace holds</h4>" +
    renderCatalogTable(backupsOf(s), meta.name, { id: "restore-point-holds", label: "runs", scope: s.ns }) +
    "</section>"
  );
}

/** Step 2 for a CATALOG point: what the catalog says about it, the binding
 *  the plan will carry, and the topics to restore.
 *
 *  Every value was read from the product API's point view -- the controller's
 *  two verdicts, kept as two columns, and the keys and digests the binding is
 *  built from. The covered window shown is the one the plan may name:
 *  `coveredTo` is exclusive, so the last instant is one millisecond before it. */
export function renderCatalogPointStep(state) {
  const s = state || {};
  const point = s.point || {};
  const c = point.catalogPoint || {};
  const covered = ((point.status || {}).windowCovered) || {};
  const errors = errorsOf(s);
  const typed = typeof s.catalogTopicsText === "string" ? s.catalogTopicsText : "";
  return (
    "<section class=\"step\" id=\"step-backup-set\" tabindex=\"-1\"><h3>2. Recovery point</h3>" +
    "<p class=\"blurb\">A point read back from a connected archive by its catalog, pinned by " +
    "its content-derived id. No Backup object is involved: everything below was read from " +
    "the catalog's view through the product API.</p>" +
    facts([
      ["catalog", "<code id=\"point-catalog\">" + esc(c.catalog) + "</code>"],
      ["point", "<code id=\"point-name\">" + esc(c.pointId) + "</code>"],
      ["backup set", cell((point.status || {}).backupId)],
      ["run", cell(c.runId)],
      ["recovery point", when(c.recoveryPointAt)],
      ["covered from", when(rfc3339(covered.fromMs))],
      ["covered to (exclusive)", when(rfc3339(covered.toMs))],
      ["availability", badge("green", String(c.availability || ""))],
      ["verification", badge("green", String(c.verification || ""))],
      ["signer key id", "<code>" + cell(c.signerKeyId) + "</code>"],
      ["location", cell(c.locationId)],
      ["receipt", "<code id=\"point-receipt-key\">" + esc(c.receiptKey) + "</code>"],
      ["receipt sha256", "<code id=\"point-receipt-sha256\">" + esc(c.receiptSha256) +
        "</code>"],
      ["manifest sha256", "<code id=\"point-manifest-sha256\">" + esc(c.manifestSha256) +
        "</code>"],
      ["offered from", c.backup === null || c.backup === undefined
        ? "the catalog (no Backup object)"
        : "Backup <code>" + esc(c.backup.name) + "</code>, uid <code>" + esc(c.backup.uid) +
          "</code>, whose own verdict is absent or NotAttempted"],
    ]) +
    "<p class=\"note\" id=\"catalog-binding\">" + esc(CATALOG_BINDING_SENTENCE) + "</p>" +
    "<div class=\"field\"><label for=\"catalog-topics\">topics to restore</label>" +
    "<input id=\"catalog-topics\" name=\"catalogTopics\" value=\"" + esc(typed) + "\"" +
    invalidAttributes("catalog-topics", errors.topics) + ">" +
    "<p class=\"note\">" + esc(CATALOG_TOPICS_SENTENCE) + "</p></div>" +
    "<div class=\"actions\"><a class=\"nav-link\" id=\"choose-another-point\" href=\"" +
    esc(restoreSelectorRoute(s.ns)) + "\">Choose a different recovery point</a></div>" +
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
    const schedule = (spec.scheduleRef || {}).name;
    return [
      // THE ACTION IS THE FIRST COLUMN (MCP-25). It was the tenth, and at
      // 1440 px the table overflowed its card and clipped exactly that column
      // -- the one thing the selector is for -- with no visible scrollbar.
      //
      // A RETRY ARRIVING HERE KEEPS ITS IDENTITY (PLAT-11.2). The operation
      // view of a failed Restore knows which run failed and not which Backup
      // it came from -- `Restore.spec` carries a backup SET id, not the
      // point's name or uid -- so the retry link lands on this selector and
      // the choice of point is made here, with `retryOf` travelling on.
      "<a class=\"button\" href=\"" +
        esc(typeof s.retryOf === "string" && s.retryOf.length > 0
          ? restoreRetryRoute(s.ns, point, s.retryOf)
          : restorePointRoute(s.ns, point)) +
        "\">" + (typeof s.retryOf === "string" && s.retryOf.length > 0
          ? "Retry to a fresh target from this point"
          : "Restore this point") + "</a>",
      // The run, with its schedule and slot beneath it rather than in two
      // columns of their own.
      // THE ARCHIVE UNDER THE RUN (R2-17): as its own last column it was the
      // one a 1024 px window clipped, with no sign the table scrolled.
      cell(meta.name) +
        "<span class=\"cell-sub\">" +
        (typeof schedule === "string" && schedule.length > 0 ? "schedule " + esc(schedule) : "no schedule") +
        (typeof spec.slot === "string" && spec.slot.length > 0 ? " &middot; slot " + when(spec.slot) : "") +
        "</span>" +
        "<span class=\"cell-sub\">" + cell((spec.archive || {}).url) + " &middot; " +
        esc(archiveAvailability(point)) + "</span>",
      // The covered window, both bounds, one per line.
      coveredCell(rfc3339(covered.fromMs), rfc3339(covered.toMs)),
      topics.length === 0 ? cell(null) : esc(topics.join(", ")),
      cell(status.records),
      pointSigned(point),
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
      ["", "BACKUP", "COVERED", "TOPICS", "RECORDS", "SIGNED"],
      rows,
      NO_COMPLETED_BACKUP_SENTENCE,
      attributes,
    ) +
    "<p class=\"note\" id=\"no-match\" hidden>" + NO_MATCH_SENTENCE + "</p>" +
    "<div class=\"actions\" id=\"point-more-bar\" hidden><p class=\"note\" id=\"point-count\" " +
    "role=\"status\"></p><button type=\"button\" id=\"point-more\">Show more recovery points" +
    "</button></div>" +
    listVerifiedNote(points) +
    // THE CONNECTED ARCHIVES' POINTS ARRIVE LATER (MCP-26): the selector is on
    // screen as soon as the Backups are read, and this section is filled in
    // when the catalog read answers, rather than the whole page waiting for
    // the slowest read it makes.
    "<div id=\"catalog-offers-slot\">" +
    (s.catalogOffers === undefined
      ? "<p class=\"pending\" id=\"catalog-offers-pending\" role=\"status\">Reading the " +
        "connected archives' recovery points...</p>"
      : renderCatalogOffers(s.ns, s.catalogOffers)) +
    "</div>" +
    "<h4>What this namespace holds</h4>" +
    renderCatalogTable(itemsOf(s.backups), null, { id: "restore-holds", label: "runs", scope: s.ns }) +
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
  // A SET, NOT `indexOf` IN A LOOP (PLAT-18.2): with a 2,000-topic point the
  // quadratic scan was most of a keystroke's render time. Same membership,
  // strict equality both ways.
  const frozenSet = new Set(frozen);
  const extra = chosen.filter((t) => !frozenSet.has(t));
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
  const frozenSet = new Set(frozen);
  const stranger = chosen.find((t) => !frozenSet.has(t));
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
      // A DATAGRID IN LIST FORM (PLAT-18.2): a point can freeze thousands of
      // topics, so the boxes get Clarity's filter and pagination. Every box
      // stays in the document -- the ones off the page are hidden, not
      // removed -- because `refresh` reads the selection off every box.
      : datagrid({ id: "subset-topics", label: "topics",
        scope: String(((((s.point || {}).metadata) || {}).uid) || (s.point || {}).uid || "") },
        "<ul class=\"topic-subset\">" + boxes + "</ul>")) +
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
    table(["SOURCE TOPIC", "TARGET TOPIC"], rows, "No topic is selected, so nothing is mapped.",
      undefined, { id: "topic-mapping", label: "mapped topics",
        scope: String(((((s.point || {}).metadata) || {}).uid) || (s.point || {}).uid || "") }) +
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

/** Which archive identity step 5 sends, stated from the recovery point itself. */
export function readinessSourceSentence(point) {
  const p = point || {};
  if (isCatalogPoint(p)) {
    const name = (((p.spec || {}).destinationRef) || {}).name;
    return "This check reads the archive the catalog " + String(p.catalogPoint.catalog) +
      " reads" + (typeof name === "string" && name.length > 0
      ? ", saved destination `" + name + "`"
      : ", an inline archive") +
      ", and names the catalog point so the controller re-reads its row -- availability, " +
      "verification and any Backup verdict against it -- when the check runs.";
  }
  const destination = ((p.spec || {}).destinationRef || {});
  if (typeof destination.name === "string" && destination.name.length > 0) {
    const digest = (p.status || {}).locationDigest;
    return "This check reads the saved destination `" + destination.name + "` named by the " +
      "recovery point. Its frozen locationDigest is " +
      (typeof digest === "string" && digest.length > 0 ? "`" + digest + "`" : "not recorded") +
      "; the controller compares that frozen fact with the destination it resolves now.";
  }
  return "Legacy recovery point: this check reads the inline archive URL recorded on the Backup, " +
    "with the Secret that Backup named -- the location and the credential the restore Job " +
    "itself will use. Only a point with no destinationRef uses legacySourceArchive.";
}

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
    esc(readinessSourceSentence(s.point)) + "</p>" +
    "<h4>Target cluster probe (context, not a verdict)</h4>" +
    facts([
      ["target cluster", cell((((cluster || {}).metadata) || {}).name)],
      ["reachable", flagBadge(status.reachable, "reachable", "not reachable")],
      ["cluster id", cell(status.clusterId)],
      ["observed at", when(status.observedAt)],
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
 *      by its own check id and code. One shape is not a refusal: a draft's
 *      `approval.state` `skipped`/`SubjectNotCreated` when EVERY other
 *      blocking check is `ready` (see [`isDraftApprovalRow`]).
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
  // THE CREDENTIAL IS NOT IN THE PLAN (review M1(a)): a check of a point with
  // no saved destination read the archive with the Secret it was started with,
  // and the Restore projects the one on screen now.
  const boundSecret = typeof readiness.boundSecret === "string" ? readiness.boundSecret : null;
  const secretNow = String(s.archiveSecretName || "").trim();
  if (boundSecret !== null && boundSecret !== secretNow) {
    return (
      "the readiness check on screen read the archive with " +
      (boundSecret.length > 0 ? "Secret " + boundSecret : "no Secret") +
      ", and this Restore would project " +
      (secretNow.length > 0 ? "Secret " + secretNow : "no Secret") +
      ". A check is about the credential the restore will use; run it again."
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
    // FAIL-CLOSED (review L2): a row with a gating this build does not
    // recognise is blocking.
    const gating = (Array.isArray(result.checks) ? result.checks : []).filter(isBlockingRow);
    const blocking = gating.filter((c) => (c || {}).state !== "ready");
    const others = blocking.filter((c) => !isDraftApprovalRow(c));
    // ONE RULE FOR THE GATE, THE STEPPER AND THE HEADLINE (`render.js`'s
    // `readyButForDraftApproval`), so step 5 cannot read Done under a
    // headline that says anything but ready (MCP round 2, R2-12).
    // THE ONE ROW A DRAFT CANNOT MAKE READY (DRAFT-PREFLIGHT-NEVER-READY). See
    // [`isDraftApprovalRow`]: every blocking check but that one is `ready`, the
    // aggregate is `unknown` for that reason alone, and the Restore this click
    // creates is exactly what turns the row into a real approval verdict. At
    // least one OTHER blocking check must have come back `ready`, for the
    // aggregate's own reason: nothing checked is not everything passed.
    if (readyButForDraftApproval(result)) {
      return null;
    }
    const failing = (others.length > 0 ? others : blocking)
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

/** The approval row of a readiness check run against a DRAFT plan.
 *
 *  DRAFT-PREFLIGHT-NEVER-READY. The controller answers `approval.state` for a
 *  plan no Restore has been created for with `skipped` + `SubjectNotCreated`
 *  (`weirkeeper::controllers::preflight::approval_rows`, `ApprovalFacts::Draft`)
 *  -- there is no Approval to verify because no approver has been asked yet --
 *  and `logweir_core::check_contract::aggregate` DELIBERATELY makes a skipped
 *  blocking row `unknown` overall, because a verdict about an unfinished
 *  subject is not `ready`. Both rules are right and stay as they are. But the
 *  wizard used to refuse every verdict that was not `ready`, so after ANY
 *  readiness check the Create button was refused, and the only way to submit
 *  was not to run the check at all.
 *
 *  WHY THE CONSOLE AND NOT A `draftReady` FIELD ON THE API RESPONSE. The API
 *  projects the controller's verdict; it has no idea whether its caller is
 *  about to create the subject or has just opened a page about it. "A draft
 *  may be submitted when this is the only thing not ready" is a rule about the
 *  NEXT action, and the next action is this page's: the click that creates
 *  the Restore is the one that makes the row answerable. Putting it on the
 *  wire would add a second aggregate beside the real one for every client to
 *  misread as the verdict. The runner's phase 0 and the controller's
 *  admission still refuse an unverified approval whatever this page decides.
 *
 *  PRECISELY THIS SHAPE, NOTHING WIDER: the id, the `skipped` state and the
 *  `SubjectNotCreated` code together. `approval.state` `notReady` (an Approval
 *  that exists and does not verify), a `skipped` row with another code, or any
 *  other check that is `unknown` or `skipped` all still refuse by id and code. */
export { isDraftApprovalRow };

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
    approvalPolicyBlock(s.approvalPolicy, s.ticket, errors.ticket) +
    "<div class=\"actions actions-final\">" +
    "<button type=\"button\" id=\"create-restore\" class=\"primary\"" +
    (pending || !renderable || blocked !== null || policyRefusal(s) !== null ? " disabled" : "") +
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
          esc(READINESS_NOT_RUN_WARNING) + "</p>" + GO_TO_READINESS)
      : "<p class=\"complaint\" id=\"readiness-blocked\" role=\"alert\">Nothing is sent: " +
        esc(blocked) + "</p>" + GO_TO_READINESS) +
    "<p class=\"note\">" + GUIDED_SUBMIT_SENTENCE + "</p>" +
    "<div class=\"form-status\" id=\"restore-submit-status\" tabindex=\"-1\">" +
    submissionStatus(submission, p, beside, s.ns) +
    "</div>" +
    "</section>"
  );
}

/** With one step on screen (MCP-29), a sentence about step 5 carries the way
 *  there. */
const GO_TO_READINESS =
  "<p class=\"actions\"><button type=\"button\" class=\"wizard-go\" id=\"go-to-readiness\" " +
  "data-go-step=\"4\">" +
  "Go to step 5: Operation readiness</button></p>";

/** What the one submit button does, said beside it. */
export const GUIDED_SUBMIT_SENTENCE =
  "Create the Restore sends exactly the plan above, then opens what the Restore needs next " +
  "under this namespace's approval policy: its operation view when the console's confirmation " +
  "is the authorization (ordinary confirmation) or an Approval already authorises it, and its " +
  "approval page while it waits for one. Submitting this plan again -- a second click, a retry " +
  "after a lost response, or the same plan after a reload -- never creates a second Restore, " +
  "because its name is minted from these bytes.";


/** The approval step under the namespace's EFFECTIVE policy (PLAT-19.2).
 *
 *  `policy` is the product API's `ApprovalPolicyView`, or `null` when the mode
 *  cannot know it (legacy `kubectl proxy`) or the read failed -- and `null`
 *  renders exactly today's governed instructions, which is the fail-safe
 *  reading: nothing here ever tells an operator a Restore will run on their
 *  confirmation unless the console said the namespace is bound Ordinary. */
export function approvalPolicyBlock(policy, ticket, ticketErrors) {
  const p = policy !== null && typeof policy === "object" ? policy : null;
  if (p !== null && p.legacy === false && p.mode === "ordinary" &&
    p.ordinaryConfirmationAvailable === false) {
    return (
      "<h4 id=\"approval-policy-ordinary-unavailable\">Ordinary confirmation is not " +
      "available here</h4>" +
      "<p class=\"complaint\">This namespace is bound to approval policy <code>" +
      esc(p.name) + "</code> (ordinary confirmation), and this console runs in the " +
      "administrator mode, which does not offer ordinary confirmation: its one identity is " +
      "whoever holds the port-forward, not a person the confirmation could attest. Submit " +
      "this restore through the shared console.</p>"
    );
  }
  if (p !== null && p.legacy === false && p.mode === "ordinary") {
    return (
      "<h4 id=\"approval-policy-ordinary\">Ordinary confirmation</h4>" +
      "<p class=\"note\">This namespace is bound to approval policy <code>" + esc(p.name) +
      "</code> (ordinary confirmation). Create the Restore is your confirmation: the console " +
      "signs, as its attestation that you asked, a document naming exactly this Restore, its " +
      "UID and this plan hash, and the Restore runs once weirkeeper verifies it. No approver " +
      "and no out-of-band signature are involved.</p>"
    );
  }
  if (p !== null && p.legacy === false && p.mode === "governed") {
    return (
      "<h4 id=\"approval-policy-governed\">Governed approval</h4>" +
      "<p class=\"note\">This namespace is bound to approval policy <code>" + esc(p.name) +
      "</code> (governed approval). Create the Restore records the console's confirmation of " +
      "you as the requester; the Restore runs only after an approver who is NOT you " +
      "countersigns that confirmation on their own machine and submits it on the Restore's " +
      "approval page.</p>" +
      "<div class=\"field\"><label for=\"change-ticket\">Change ticket (required)</label>" +
      "<input id=\"change-ticket\" name=\"ticket\" maxlength=\"128\" value=\"" +
      esc(typeof ticket === "string" ? ticket : "") + "\"" +
      invalidAttributes("change-ticket", ticketErrors) + ">" +
      fieldErrorLine("change-ticket", ticketErrors) +
      "<p class=\"note\">Signed into the confirmation with the plan hash; the approver sees " +
      "it before countersigning.</p></div>" +
      copyBlock([COUNTERSIGN_COMMAND])
    );
  }
  return (
    "<h4>Approve it out of band</h4>" +
    "<p class=\"note\">Run this on the machine that holds the approver's private key. This " +
    "page never sees it.</p>" +
    copyBlock([APPROVE_COMMAND])
  );
}

/** THE FROZEN POLICY'S ANSWER for a created Restore (PLAT-19.2 / PLAT-12.1):
 *  the product API's `authorization` block, carried on the created object by
 *  `ui/client.js` as `__contract.authorization`. `null` in legacy mode, where
 *  the page falls back to reading the Approval itself.
 *
 *  ONLY THE TWO KNOWN STATES ARE ROUTED ON. An unrecognised value is `null`
 *  and takes the fallback: a newer server's state this page does not know is
 *  never read as "confirmed". */
export function frozenDecision(restore) {
  const contract = (restore || {}).__contract;
  const found = contract !== null && typeof contract === "object" ? contract.authorization : null;
  if (found === null || typeof found !== "object") {
    return null;
  }
  if (found.state !== "confirmed" && found.state !== "awaitingApproval") {
    return null;
  }
  return found;
}

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

/** The 0-based step a route's 1-based `step` names, or 0. */
export function stepIndexOf(value) {
  const n = typeof value === "number" ? value : Number(value);
  return Number.isInteger(n) && n >= 1 && n <= STEPS.length ? n - 1 : 0;
}

/** The step on screen, clamped to the six. */
export function shownStep(state) {
  const n = ((state || {}).step);
  return Number.isInteger(n) && n >= 0 && n < STEPS.length ? n : 0;
}

/** THE STEPPER'S STATE: which steps are done, which one needs you next, which
 *  one is on screen, and which needs attention -- one entry per step, in order.
 *
 *  `current` is the FIRST step whose inputs are not yet whole: an archive with
 *  no URL, a chosen run with no backup set, a point outside the covered
 *  window, a target with no mode or no prefix, a target cluster whose
 *  recorded status is not reachable, or a readiness check that refuses this
 *  plan. When the five input steps are whole, the plan step is the current one
 *  and reads `ready`. `shown` is the step on screen (MCP-29: the wizard shows
 *  one step at a time); the two differ whenever the reader goes back to look.
 *
 *  STEP 5 IS NOT DONE UNTIL A CHECK SAYS SO (MCP-27). It read `done` as soon as
 *  the target's recorded probe was reachable, beside "No readiness check has
 *  run for this plan". Now it is `done` only when a readiness check for THIS
 *  plan came back applicable and ready (`readinessRefusal` answers `null` for
 *  a held result); `unchecked` when none has run -- passable, because the
 *  check is advisory and Create is allowed with a warning -- and `attention`
 *  when a held check refuses the plan, which Create refuses too.
 *
 *  THIS DECIDES NOTHING THE RUNNER DECIDES. It reads the same state the six
 *  sections read and summarises it; the create button is never gated on it,
 *  because the client-side checks are a convenience and never the gate. Pure:
 *  no DOM, no network, no clock. `prepared` is the plan on screen, when the
 *  caller has it, so a held check bound to another plan reads as such. */
export function stepStates(state, prepared) {
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
  const held = ((s.readiness || {}).preflight) || null;
  const readinessRefused = held !== null && readinessRefusal(s, prepared) !== null;
  const whole = [
    typeof s.archiveUrl === "string" && s.archiveUrl.length > 0,
    setChosen,
    windowComplaint(value, covered) === null,
    target !== null &&
      TARGET_MODES.indexOf(targetFields.mode) !== -1 &&
      typeof targetFields.topicPrefix === "string" &&
      targetFields.topicPrefix.length > 0 &&
      Object.keys(mapping).length === 0,
    target !== null && targetStatus.reachable === true && !readinessRefused,
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
    (target !== null && !whole[4]) || readinessRefused,
  ];
  let firstOpen = 5;
  for (let i = 0; i < 5; i += 1) {
    if (!whole[i]) {
      firstOpen = i;
      break;
    }
  }
  const shown = shownStep(s);
  return STEPS.map((step, i) => {
    let status;
    if (i === 5) {
      status = firstOpen === 5 ? "ready" : "todo";
    } else if (whole[i]) {
      // STEP 5 SAYS WHAT ITS HEADLINE SAYS (review L1): a check whose one
      // unresolved blocking row is the draft's approval is passable -- Create
      // requests the approval -- and it is not `done`.
      status = attention[i]
        ? "attention"
        : (i === 4 && held === null
          ? "unchecked"
          : (i === 4 && readyButForDraftApproval(held) ? "approval" : "done"));
    } else {
      status = attention[i] ? "attention" : "todo";
    }
    return {
      id: step.id,
      number: i + 1,
      title: step.title,
      status: status,
      current: i === firstOpen,
      shown: i === shown,
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
  if (step.status === "unchecked") {
    return "not checked yet";
  }
  if (step.status === "approval") {
    return "needs approval";
  }
  return step.current ? "to do next" : "to do";
}

/** The stepper: an ordered list of the six steps, each a button that shows its
 *  step, carrying the step's number, its title and its state in words. The
 *  step on screen is marked `aria-current="step"` (MCP-29). */
export function renderStepper(state, prepared) {
  const items = stepStates(state, prepared)
    .map((step) => {
      const classes =
        "stepper-item is-" + step.status + (step.shown ? " is-current" : "");
      return (
        "<li class=\"" + classes + "\">" +
        "<button type=\"button\" class=\"stepper-link\" data-target=\"" + step.id + "\"" +
        " data-step=\"" + String(step.number - 1) + "\"" +
        (step.shown ? " aria-current=\"step\"" : "") + ">" +
        "<span class=\"stepper-num\">" + String(step.number) + "</span>" +
        "<span class=\"stepper-title\">" + esc(step.title) + "</span>" +
        "<span class=\"stepper-status\">" + stepWord(step) + "</span>" +
        "</button></li>"
      );
    })
    .join("");
  return "<ol class=\"stepper\" aria-label=\"The six steps\">" + items + "</ol>";
}

/** The wizard's footer (MCP-29, Clarity's wizard anatomy): Back, where you are,
 *  and Next. The last step has no Next -- its primary action is Create the
 *  Restore, inside the step. */
export function renderWizardNav(state) {
  const shown = shownStep(state);
  const last = shown === STEPS.length - 1;
  return (
    "<div class=\"wizard-nav\" role=\"group\" aria-label=\"Move between the steps\">" +
    "<button type=\"button\" class=\"wizard-go\" id=\"wizard-back\" data-step=\"" + String(Math.max(0, shown - 1)) +
    "\"" + (shown === 0 ? " disabled" : "") + ">Back</button>" +
    "<span class=\"wizard-position\" id=\"wizard-position\">Step " + String(shown + 1) +
    " of " + String(STEPS.length) + ": " + esc(STEPS[shown].title) + "</span>" +
    (last
      ? ""
      : "<button type=\"button\" class=\"primary wizard-go\" id=\"wizard-next\" data-step=\"" +
        String(shown + 1) + "\">Next: " + esc(STEPS[shown + 1].title) + "</button>") +
    "</div>"
  );
}

/** One step's page: the section, shown when it is the step on screen and
 *  `hidden` otherwise. Every step stays IN the document, so its inputs are
 *  still read, drafted and validated whichever step is visible. */
function wizardPage(state, index, html) {
  return (
    "<div class=\"wizard-page\" data-wizard-step=\"" + String(index) + "\"" +
    (index === shownStep(state) ? "" : " hidden") + ">" + html + "</div>"
  );
}

/** The whole wizard, all six steps, over one state. */
export async function renderRestoreWizard(state) {
  return renderPreparedWizard(state, await preparePlanOrProblem(state));
}

/** The same wizard over a plan already prepared -- so the mount half knows the
 *  exact hash it put on screen, and can refuse to submit any other.
 *
 *  ONE STEP AT A TIME (MCP-29). All six sections are rendered, and every one
 *  but the step on screen is `hidden`: the page used to be one 22,686 px
 *  column. The stepper above and Back / Next below move between them; nothing
 *  is created until step 6's own button. */
export function renderPreparedWizard(state, prepared) {
  const s = state || {};
  return (
    "<h2>Restore wizard</h2>" +
    "<p class=\"blurb\">Six steps, one at a time: the archive, the recovery point, the point " +
    "in time, the target, the readiness check, and the plan whose bytes the Restore carries. " +
    "Every value was read from this namespace's own objects or is editable in its step, and " +
    "nothing is created until step 6.</p>" +
    renderRetryBanner(state) +
    (s.editing
      ? "<p class=\"immutable-note\">" + RESTORE_IMMUTABLE_SENTENCE + "</p>"
      : "") +
    (s.draftRestored === true
      ? "<div class=\"draft-note\"><p class=\"note\">" + DRAFT_RESTORED_SENTENCE + "</p>" +
        "<div class=\"actions\"><button type=\"button\" id=\"discard-draft\">Discard these edits</button></div></div>"
      : "") +
    renderStepper(state, prepared) +
    wizardPage(s, 0, renderArchiveStep(state)) +
    wizardPage(s, 1, renderRecoveryPointStep(state)) +
    wizardPage(s, 2, renderPointInTimeStep(state)) +
    wizardPage(s, 3, renderTargetStep(state)) +
    wizardPage(s, 4, renderPreflightStep(state, prepared)) +
    wizardPage(s, 5, renderPlanStep(prepared, state)) +
    renderWizardNav(s)
  );
}

/** WHICH STEP A FIELD'S MESSAGE BELONGS TO, so a refused submit shows the step
 *  that holds the input it is about (MCP-29). A message with no input of its
 *  own (`archive`, `backupSet`) is shown beside the submit, in step 6. */
export const FIELD_STEP = Object.freeze({
  archiveSecret: 0,
  evidenceDestination: 0,
  pointInTime: 2,
  topicPrefix: 3,
  targetCluster: 3,
  mode: 3,
  topics: 3,
  ticket: 5,
  archive: 5,
  backupSet: 5,
});

/** The first step, in order, carrying a field message, or `null`. */
export function firstStepWithErrors(errors) {
  const keys = Object.keys(errors || {}).filter((k) =>
    Array.isArray(errors[k]) ? errors[k].length > 0 : Boolean(errors[k]));
  let best = null;
  for (const key of keys) {
    const at = Object.prototype.hasOwnProperty.call(FIELD_STEP, key) ? FIELD_STEP[key] : null;
    if (at !== null && (best === null || at < best)) {
      best = at;
    }
  }
  return best;
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
  const destinationName = savedDestinationName(s);
  if (destinationName.length > 0 && !savedDestinationResolved(s)) {
    problems.archive = typeof s.savedDestinationProblem === "string"
      ? s.savedDestinationProblem
      : "saved destination " + destinationName +
        " could not be resolved to its public location and transport settings";
  }
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
  if (destinationName.length > 0 && (typeof s.evidenceDestinationProblem === "string" ||
    evidenceDestinationName(s).length === 0)) {
    problems.evidenceDestination = typeof s.evidenceDestinationProblem === "string"
      ? s.evidenceDestinationProblem
      : "choose the saved destination the evidence is written to";
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
    evidenceDestination: evidenceDestinationName(s),
    evidenceDestinationUid: ((s.evidenceDestination || {}).uid) || "",
    evidenceSameAsArchive: s.evidenceSameAsArchive !== false,
    evidenceEndpoint: (f.evidence || {}).endpoint,
    evidenceRegion: (f.evidence || {}).region,
    evidencePathStyle: (f.evidence || {}).pathStyle === true,
    evidenceAllowHttp: (f.evidence || {}).allowHttp === true,
    catalogPointId: isCatalogPoint(s.point) ? catalogPointUid(s.point) : "",
    catalogTopics: isCatalogPoint(s.point) ? frozenTopicsOf(s) : [],
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
  // AND FOR THE SAME KIND OF POINT (PLAT-15.2). A catalog point and a Backup can
  // share a backup set -- the catalog lists the Backup's own receipt -- and a
  // draft made over one must not become edits to the other: their topic lists
  // and bindings are different documents. An older draft carries no
  // `catalogPointId` and reads as the Backup draft it was.
  const pointKey = isCatalogPoint((state || {}).point) ? catalogPointUid(state.point) : "";
  if ((typeof d.catalogPointId === "string" ? d.catalogPointId : "") !== pointKey) {
    return false;
  }
  if (pointKey.length > 0 && Array.isArray(d.catalogTopics)) {
    setCatalogTopics(state, d.catalogTopics.map(String));
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
  // A saved destination is the frozen source of these signed-plan values.
  // Older drafts may contain legacy controls, but must never override it.
  const legacyStorage = savedDestinationName(state).length === 0;
  // THE ARCHIVE BLOCK FROM THE ARCHIVE CONTROLS, AND ONLY THE ARCHIVE BLOCK
  // (PLAT-08.2). The evidence block follows below from its own kept values --
  // or, for a draft kept before the evidence store had controls of its own,
  // from the "same store" rule that draft was made under.
  for (const block of legacyStorage ? [state.fields.source] : []) {
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
  if (legacyStorage) {
    state.evidenceSameAsArchive = d.evidenceSameAsArchive !== false;
    if (state.evidenceSameAsArchive) {
      syncEvidenceStore(state);
    } else {
      const evidence = state.fields.evidence;
      if (typeof d.evidenceEndpoint === "string") {
        evidence.endpoint = d.evidenceEndpoint;
      }
      if (typeof d.evidenceRegion === "string") {
        evidence.region = d.evidenceRegion;
      }
      if (typeof d.evidencePathStyle === "boolean") {
        evidence.pathStyle = d.evidencePathStyle;
      }
      // Its OWN kept flag, never the archive's and never an addressing box.
      if (typeof d.evidenceAllowHttp === "boolean") {
        evidence.allowHttp = d.evidenceAllowHttp;
      }
    }
  } else if (typeof d.evidenceDestinationUid === "string" && d.evidenceDestinationUid.length > 0) {
    // THE KEPT EVIDENCE DESTINATION BY UID, applied even when that uid no
    // longer answers, for `targetClusterUid`'s reason: dropping it would put
    // the point's own destination back silently, which is a substitution.
    selectEvidenceDestination(state, d.evidenceDestinationUid,
      typeof d.evidenceDestination === "string" ? d.evidenceDestination : "");
  }
  if (legacyStorage && typeof d.evidenceBucket === "string") {
    state.fields.evidence.bucket = d.evidenceBucket;
    state.evidenceBucket = d.evidenceBucket;
  }
  if (legacyStorage && typeof d.archiveSecret === "string") {
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
  const destination = (((s.point || {}).spec || {}).destinationRef) || {};
  const sourceArchive = { url: s.archiveUrl };
  if (
    (typeof destination.name !== "string" || destination.name.length === 0) &&
    typeof s.archiveSecretName === "string" &&
    s.archiveSecretName.length > 0
  ) {
    sourceArchive.secretRef = { name: s.archiveSecretName };
  }
  const spec = {
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
  };
  if (typeof destination.name === "string" && destination.name.length > 0) {
    spec.sourceDestinationRef = { name: destination.name };
    // THE EVIDENCE DESTINATION THE OPERATOR CHOSE (PLAT-08.2) -- the point's
    // own unless step 1 named another. Never a fallback to the source: a
    // refused choice has already stopped the submit in `validateRestore`.
    spec.evidenceDestinationRef = { name: evidenceDestinationName(s) };
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Restore",
    metadata: { name: p.restoreName },
    spec: spec,
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
    // PLAT-19.2: the change ticket, a field of the create REQUEST that the
    // console signs into the authorization document. Sent only under an
    // explicit binding; an unbound namespace signs nothing.
    ticket: ticketFor(s),
  };
}

/** The ticket this state sends, or `undefined`. */
function ticketFor(state) {
  const s = state || {};
  const p = s.approvalPolicy;
  const bound = p !== null && typeof p === "object" && p.legacy === false;
  const ticket = typeof s.ticket === "string" ? s.ticket.trim() : "";
  return bound && ticket.length > 0 ? ticket : undefined;
}

/** A refusal the namespace's approval POLICY makes before anything is sent
 *  (PLAT-19.2), or `null`: an Ordinary binding in a console that does not
 *  offer ordinary confirmation (D0: the administrator mode "does not expose
 *  Ordinary"). The product API refuses the same request; this says so first. */
export function policyRefusal(state) {
  const p = (state || {}).approvalPolicy;
  if (p !== null && typeof p === "object" && p.legacy === false && p.mode === "ordinary" &&
    p.ordinaryConfirmationAvailable === false) {
    return "namespace policy " + String(p.name) + " is ordinary confirmation, which this " +
      "console mode does not offer; submit through the shared console";
  }
  return null;
}

/** The route "Request approval" navigates to. `subjectRef` and `planHash` come
 *  from HERE and never from the approval documents: the approvals page is
 *  forbidden from parsing those, so the hash cannot be lifted out of them
 *  either. */
export function approvalRoute(state, prepared, restore) {
  const s = state || {};
  const p = prepared || {};
  // THE SUBJECT IS THE OBJECT THAT WAS CREATED (PLAT-19.2, found live). In
  // console mode the product API mints the Restore's name (`rst-...`) and
  // the page's `restoreName` is never an object; routing to it opened an
  // approvals page about a Restore that does not exist. The created object's
  // own name and `spec.approvalRef` win whenever there is one.
  const meta = (restore || {}).metadata || {};
  const ref = (((restore || {}).spec || {}).approvalRef || {}).name;
  const subject = typeof meta.name === "string" && meta.name.length > 0 ? meta.name : p.restoreName;
  const approvalName = typeof ref === "string" && ref.length > 0 ? ref : p.approvalName;
  // There is no implicit namespace on a router that deliberately refuses to
  // guess one. In particular, `default` is a real selected namespace and
  // must cross this hand-off explicitly rather than being elided as a legacy
  // shorthand.
  const ns = typeof s.ns === "string" ? s.ns.trim() : "";
  return (
    "#/approvals?subject=" +
    encodeURIComponent(subject) +
    "&hash=" +
    encodeURIComponent(p.hash) +
    "&name=" +
    encodeURIComponent(approvalName) +
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
 *   4. for a saved point, re-read its Destination and require the same frozen
 *      name, UID and location digest. This touches no plan field, so the bytes
 *      reviewed in step 3 remain the bytes submitted;
 *   5. one create, WITHOUT a route signal. Its name is minted from the bytes,
 *      so `409 AlreadyExists` is this plan submitted before: an existing
 *      Restore with exactly this spec is this operation, and any other is a
 *      conflict (`resolveExisting`);
 *   6. the destination: the Restore's operation view when an Approval already
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
  // AND AGAINST A FRESH READ OF THE CHECK (PLAT-08.2): a destination edited
  // during the draft moves no byte of the plan and does make its verdict stale.
  if (!await confirmReadiness(s, api, prepared, lifecycle)) {
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
  const unoffered = policyRefusal(s);
  if (unoffered !== null) {
    throw refusal(unoffered);
  }
  if (((s.approvalPolicy || {}).ticketRequired) === true && ticketFor(s) === undefined) {
    throw invalidInput({
      ticket: "a namespace under a Governed approval policy requires a change ticket; it is " +
        "signed into the confirmation the approver countersigns",
    });
  }
  const reviewed = (options || {}).reviewedHash;
  if (typeof reviewed === "string" && reviewed !== prepared.hash) {
    throw refusal(
      "the plan changed after it was displayed (the page showed " + reviewed + ", the current " +
        "values hash to " + prepared.hash + "); review the plan shown now and submit again",
      { reviewedHash: reviewed, preparedHash: prepared.hash },
    );
  }
  if (!await confirmFrozenDestination(s, api, lifecycle)) {
    return null;
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

/** Where a submitted Restore goes next, UNDER THE FROZEN APPROVAL POLICY
 *  (PLAT-19.2 / PLAT-12.1).
 *
 *  1. The product API answered `confirmed`: the namespace is bound Ordinary
 *     and the console's signed confirmation IS the Approval the Restore
 *     names, so the submission goes to EXECUTION -- the operation view, which
 *     shows weirkeeper admitting it (or refusing it, with the reason).
 *  2. Otherwise -- `awaitingApproval`, or legacy mode with no answer at all --
 *     today's rule: the operation view when the Approval its
 *     `spec.approvalRef` names already authorises exactly this Restore (this
 *     name, namespace, UID and plan), and its approval page otherwise. A read
 *     that fails is not an authorisation, so it lands on the approval page,
 *     which reads the state again for itself. */
export async function restoreDestination(api, state, prepared, restore) {
  const s = state || {};
  const p = prepared || {};
  const meta = (restore || {}).metadata || {};
  const frozen = frozenDecision(restore);
  if (frozen !== null && frozen.state === "confirmed") {
    return restoreOperationRoute(s.ns, typeof meta.name === "string" ? meta.name : p.restoreName);
  }
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
  return approvalRoute(s, p, restore);
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

/** Re-read a saved Destination at the last non-mutating edge before create.
 *
 * The mount check protects the plan initially rendered. This check protects
 * the interval after that render: a Destination deleted, recreated or moved
 * while the operator reviews the plan cannot make the cached public settings
 * authoritative again merely because its name stayed the same. Nothing here
 * replaces `savedDestination` or any plan field; the reviewed bytes stay
 * frozen, and a mismatch sends no Restore.
 *
 * Like [`confirmClusters`], this read is part of the durable submit attempt and
 * carries no route signal. A failed or absent read is uncertainty, never
 * evidence that the frozen object still exists. */
export async function confirmFrozenDestination(state, api, lifecycle) {
  const s = state || {};
  const name = savedDestinationName(s);
  if (name.length === 0) {
    return true;
  }
  let read;
  try {
    read = await api.destination(s.ns, name);
  } catch (unread) {
    throw invalidInput({
      archive: "saved destination " + name +
        " could not be read again before submitting (" +
        String(unread && unread.message) +
        "). Nothing was sent; the reviewed plan is still here.",
    });
  }
  if (!active(lifecycle)) {
    return false;
  }
  const live = read === null || read === undefined ? null : read.item;
  if (live === null || live === undefined) {
    throw invalidInput({
      archive: "saved destination " + name +
        " is absent. Nothing was sent; the reviewed plan is still here.",
    });
  }
  const problem = frozenDestinationProblem(s.point, live);
  if (problem !== null) {
    throw invalidInput({
      archive: problem + ". Nothing was sent; the reviewed plan is still here.",
    });
  }
  // AND THE EVIDENCE DESTINATION, WHEN IT IS ANOTHER ONE (PLAT-08.2). It was
  // pinned by uid and location digest when it was chosen; the same interval
  // this read protects for the archive applies to it, and the same answer: a
  // replacement under the same name is not the store the plan names.
  const pin = s.evidenceDestination || {};
  if (typeof pin.uid === "string" && pin.uid.length > 0 && pin.uid !== live.uid) {
    let other;
    try {
      other = await api.destination(s.ns, pin.name);
    } catch (unread) {
      throw invalidInput({
        evidenceDestination: "evidence destination " + pin.name +
          " could not be read again before submitting (" + String(unread && unread.message) +
          "). Nothing was sent; the reviewed plan is still here.",
      });
    }
    if (!active(lifecycle)) {
      return false;
    }
    const item = other === null || other === undefined ? null : other.item;
    const moved = item === null || item === undefined
      ? "is absent"
      : (item.uid !== pin.uid
        ? "was recreated: the plan names uid " + pin.uid + " and the live one is " +
          String(item.uid || "(absent)")
        : (item.locationDigest !== pin.locationDigest
          ? "moved: the plan names location digest " + pin.locationDigest + " and the live one " +
            "is " + String(item.locationDigest || "(absent)")
          : null));
    if (moved !== null) {
      throw invalidInput({
        evidenceDestination: "evidence destination " + pin.name + " " + moved +
          ". Nothing was sent; the reviewed plan is still here.",
      });
    }
  }
  return true;
}

/** Re-reads the readiness check on screen, at the last edge before create,
 *  against the reviewed plan's hash (PLAT-08.2, D2 section 6.6).
 *
 *  THE VERDICT ON SCREEN IS A MEMORY, and the inputs it was about can move
 *  while an operator reviews: a destination's access rotated, a CA replaced, a
 *  connection edited -- each changes a referent's generation and the product
 *  API then answers `stale` with `referentChanged:<Kind>/<name>`, while the
 *  plan bytes, and so the hash, stay exactly the same. A page that trusted its
 *  cached `ready` would submit past that. So the held check is read again
 *  here, the answer replaces the cached one (staleness stays one-way, see
 *  [`mergeReadiness`]), and `readinessRefusal` then judges the fresh one.
 *
 *  No held check means nothing to re-read, and that is `readinessRefusal`'s
 *  "nothing has run" arm, unchanged. A read that fails, or a 2xx answer that
 *  carries no check, is a refusal: "I could not find out" is not "it is still
 *  ready". No route signal, for
 *  [`confirmClusters`]'s reason. */
export async function confirmReadiness(state, api, prepared, lifecycle) {
  const s = state || {};
  const held = (s.readiness || {}).preflight || null;
  if (held === null || typeof held.id !== "string" || held.id.length === 0 ||
    typeof (api || {}).preflight !== "function") {
    return true;
  }
  const hash = typeof (prepared || {}).hash === "string" ? prepared.hash : "";
  let answer;
  try {
    answer = await api.preflight(s.ns, held.id, hash.length > 0 ? { planHash: hash } : {});
  } catch (unread) {
    throw refusal(
      "the readiness check " + held.id + " could not be read again before submitting (" +
        String(unread && unread.message) + "), so whether it still applies to this plan is " +
        "unknown. Nothing was sent; the reviewed plan is still here.",
      { planHash: hash },
    );
  }
  if (!active(lifecycle)) {
    return false;
  }
  // AN ANSWER WITHOUT A CHECK IS NOT AN ANSWER (PLAT-08.2 review L2). A 2xx
  // with no `item` -- a proxy's empty body, an older API -- would leave
  // `mergeReadiness` holding the cached verdict, which is the "I could not find
  // out" case wearing a success status. It is refused the same way.
  const fresh = (answer || {}).item;
  if (fresh === null || typeof fresh !== "object" || Array.isArray(fresh)) {
    throw refusal(
      "the readiness check " + held.id + " was read again before submitting, but the answer " +
        "carried no check, so whether it still applies to this plan is unknown. Nothing was " +
        "sent; the reviewed plan is still here.",
      { planHash: hash },
    );
  }
  s.readiness = Object.assign({}, s.readiness, {
    preflight: mergeReadiness(held, fresh),
  });
  return true;
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
  // THE RANGE A PLAN MAY NAME, NOT THE RAW WINDOW (WIZARD-DEFAULT-PIT-EXCLUSIVE):
  // `windowCovered.toMs` is exclusive and the runner refuses the floor itself,
  // so every check, message and default on this page reads `restorableWindow`.
  const chosen = chosenBackup(state);
  const window = restorableWindow(((chosen || {}).status || {}).windowCovered);
  return window === null ? { fromMs: undefined, toMs: undefined } : window;
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
  // EVERY READ THAT DOES NOT WAIT ON ANOTHER STARTS NOW, TOGETHER (MCP-26).
  // At 258 points the wizard was usable after 12.4 s while the API answered
  // the Backups in 3.0 s and the catalog's points in 6.6 s: the reads were
  // made one after another -- connections and Backups, then the approval
  // policy, then the catalogs, then every page of their points -- and the
  // page rendered nothing until the last one answered. Now the approval
  // policy and, on the selector, the connected archives' points are read
  // beside the two lists; the selector renders as soon as the Backups are
  // in, and the catalog section fills in when its read answers.
  const selector = !(String(selection.uid || "").length > 0 ||
    String(selection.backup || "").length > 0 || String(selection.catalog || "").length > 0);
  const policyRead = readApprovalPolicy(api, ns, lifecycle);
  const offersRead = selector
    ? readCatalogOffers(ns, catalogReadersOf(api, ns, lifecycle), lifecycle)
    : null;
  // A rejection that is never awaited (the route left first) is not an
  // unhandled one: both are awaited below wherever they are used.
  policyRead.catch(() => null);
  if (offersRead !== null) {
    offersRead.catch(() => null);
  }
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
    // PLAT-15.2: A CATALOG POINT IS ITS OWN MOUNT, and it never falls back to
    // a Backup. A link naming a catalog and a point that do not resolve is a
    // refusal naming them, exactly as a Backup UID that does not resolve is.
    if (typeof selection.catalog === "string" && selection.catalog.length > 0) {
      await mountCatalogPoint(node, ns, selection, parse, api, lifecycle, clusters, backups,
        policyRead);
      return;
    }
    const resolved = resolvePoint(backups, selection);
    const destinationName = savedDestinationName({ point: resolved.point });
    // THE POINT'S DESTINATION AND THE OTHER SAVED DESTINATIONS, TOGETHER. The
    // list is for the evidence selector (PLAT-08.2); a read that fails costs
    // the choice and nothing else: the point's own destination is still
    // offered, and the page says the others were not read.
    const kept = readDraft(formKey(ns, WIZARD_FORM));
    const keptName = kept !== null && typeof kept.evidenceDestination === "string"
      ? kept.evidenceDestination : "";
    let destination = null;
    let destinations;
    if (resolved.state === "selected" && destinationName.length > 0) {
      const listing = typeof api.destinations === "function"
        ? api.destinations(ns, readOptions(lifecycle)).then((answer) => answer.items, (unread) => {
          if (cancelled(unread, lifecycle)) {
            throw unread;
          }
          return null;
        })
        : Promise.resolve(undefined);
      // A KEPT EVIDENCE CHOICE IS READ IN FULL BEFORE THE DRAFT IS APPLIED: the
      // list is summaries, and a summary carries no bucket to sign.
      const keptRead = typeof api.destinations === "function" && keptName.length > 0 &&
        keptName !== destinationName
        ? api.destination(ns, keptName, readOptions(lifecycle)).then(
          (answer) => (answer || {}).item || null,
          (unread) => {
            if (cancelled(unread, lifecycle)) {
              throw unread;
            }
            return null;
          })
        : Promise.resolve(null);
      listing.catch(() => null);
      keptRead.catch(() => null);
      const read = await api.destination(ns, destinationName, readOptions(lifecycle));
      if (!active(lifecycle)) {
        return;
      }
      destination = requireFrozenDestination(resolved.point, read.item);
      destinations = await listing;
      const keptItem = await keptRead;
      if (!active(lifecycle)) {
        return;
      }
      if (Array.isArray(destinations) && keptItem !== null) {
        destinations = destinations.filter((d) => d.name !== keptName).concat([keptItem]);
      }
    }
    const state = initialState(ns, clusters, backups, selection, destination, destinations);
    if (state.pointState === "none") {
      // THE CONNECTED ARCHIVES' POINTS, beside the Backups (PLAT-15.2). A
      // namespace that lost every Backup object still has its archive, and the
      // selector is where an operator looks for something to restore.
      if (completedBackups(backups).length === 0) {
        state.catalogOffers = await (offersRead || readCatalogOffers(ns,
          catalogReadersOf(api, ns, lifecycle), lifecycle));
        if (!active(lifecycle)) {
          return;
        }
        replace(node, parse(renderNoCompletedBackup(ns, backups, state.catalogOffers)));
        return;
      }
      // THE SELECTOR FIRST, THE CATALOG SECTION WHEN IT ANSWERS.
      state.catalogOffers = undefined;
      replace(node, parse(renderPointSelector(state)));
      wireSelector(node, state, lifecycle);
      state.catalogOffers = await (offersRead || readCatalogOffers(ns,
        catalogReadersOf(api, ns, lifecycle), lifecycle));
      if (!active(lifecycle)) {
        return;
      }
      const slot = node.querySelector("#catalog-offers-slot");
      if (slot !== null) {
        replace(slot, parse(renderCatalogOffers(ns, state.catalogOffers)));
      }
      return;
    }
    if (state.pointState !== "selected") {
      replace(node, parse(renderPointRefusal(state)));
      return;
    }
    // PLAT-19.2: the namespace's effective approval policy, for the submit
    // step's words. NOT a gate: routing uses the answer the create returns,
    // and an unread policy renders today's governed instructions. It was
    // started with the lists.
    state.approvalPolicy = await policyRead;
    if (!active(lifecycle)) {
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

/** The readers the catalog half uses: the product API's catalog routes, or
 *  the stand-ins a test hands in as `api.catalogReaders` (the same arrangement
 *  as the schedule detail's `detailReaders`). */
export function catalogReadersOf(api, ns, lifecycle) {
  const given = (api || {}).catalogReaders || {};
  return {
    listCatalogs: given.listCatalogs ||
      (() => listD3("catalog", ns, readOptions(lifecycle))),
    readCatalog: given.readCatalog ||
      ((name) => readD3("catalog", ns, name, readOptions(lifecycle))),
    readPoints: given.readPoints ||
      ((name, query) => readCatalogPoints(ns, name, query, readOptions(lifecycle))),
    // ONE RUN'S OWN VERDICT, read rather than taken from a list that does not
    // publish it (see `backupOwnVerdict`).
    ownVerdict: given.ownVerdict ||
      (async (name) => {
        const read = await readOperation(ns, "backup", name, readOptions(lifecycle));
        return { verdict: ownVerdictOf(read), receiptSha256: ownReceiptOf(read) };
      }),
  };
}

/** RESOLVE A CATALOG POINT FOR THE WIZARD, or say exactly why not.
 *
 *  Reads the catalog, finds the point in its view by id, and applies
 *  [`catalogPointOffer`]. When the route also names a `Backup` -- the offer
 *  came from a run the controller wrote no window for -- that run must still
 *  be the one named (same UID), [`backupCatalogOffer`] must offer THIS row
 *  for it, and the catalog must read the destination the run froze. Returns
 *  `{state: "selected", point}` or `{state: "refused", reason, ...}`; a read
 *  that FAILS is thrown, because "could not read" is not "refused". */
export async function resolveCatalogChoice(api, ns, selection, backups, readers, lifecycle) {
  const wanted = selection || {};
  const catalogName = String(wanted.catalog || "");
  const pointId = String(wanted.point || "");
  const refused = (reason, extra) => Object.assign(
    { state: "refused", reason: reason, catalog: catalogName, pointId: pointId }, extra || {});
  if (pointId.length === 0) {
    return refused("the link names a catalog and no recovery point");
  }
  const catalog = await readers.readCatalog(catalogName);
  if (!active(lifecycle)) {
    return null;
  }
  const found = await findCatalogPoint((query) => readers.readPoints(catalogName, query), pointId);
  if (!active(lifecycle)) {
    return null;
  }
  if (found.entry === null) {
    return refused(
      found.page.budgetExhausted
        ? "the catalog's view holds more pages than this page reads, and the point was not in " +
          "the ones it read"
        : (found.page.viewExpired
          ? "the catalog's view has aged out (viewExpired); sync the catalog again"
          : (found.page.incomplete
            ? "a page of the catalog's view disappeared while it was being read; reload"
            : "the catalog's view does not list this point")),
    );
  }
  let backup = null;
  const uid = String(wanted.uid || "");
  const name = String(wanted.backup || "");
  if (uid.length > 0 || name.length > 0) {
    backup = itemsOf(backups).find((b) => {
      const meta = (b || {}).metadata || {};
      return uid.length > 0 ? meta.uid === uid : meta.name === name;
    }) || null;
    if (backup === null) {
      return refused(
        "the link names Backup " + (name || "(unnamed)") + " (uid " + (uid || "none") + ") and " +
          "no Backup in this namespace answers to it; open the point from the catalog instead",
      );
    }
    // THE RUN'S OWN VERDICT, READ NOW. The list this page holds may not publish
    // it, and a verdict a link carried would be a verdict anyone could edit.
    const own = await readers.ownVerdict(String((backup.metadata || {}).name)) || {};
    noteOwnVerdict(backup, own.verdict, own.receiptSha256);
    if (!active(lifecycle)) {
      return null;
    }
    const offer = backupCatalogOffer(backup, [found.entry], found.page);
    if (!offer.offer) {
      return refused(offer.reason, { entry: found.entry });
    }
  } else {
    const offer = catalogPointOffer(found.entry, found.page);
    if (!offer.offer) {
      return refused(offer.reason, { entry: found.entry });
    }
  }
  const destinationName = catalogDestinationName(catalog);
  let destination = null;
  if (destinationName.length > 0) {
    const read = await api.destination(ns, destinationName, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return null;
    }
    destination = (read || {}).item || null;
    if (destination === null) {
      return refused("the catalog reads saved destination " + destinationName +
        ", which is absent");
    }
    if (savedDestinationStore(destination, destinationName) === null) {
      return refused("saved destination " + destinationName + " does not publish an S3 " +
        "location this page can put into a plan");
    }
  } else if (catalogLegacyArchive(catalog) === null) {
    return refused("the catalog names neither a saved destination nor an archive");
  }
  if (backup !== null) {
    // A RUN IS RESTORED FROM THE DESTINATION IT WAS WRITTEN TO (D2 section
    // 3.12), so an offer that came from a Backup must be read through the
    // destination that run froze -- the same name, the same object, the same
    // location digest.
    const frozen = (((backup.spec || {}).destinationRef) || {}).name || "";
    if (frozen !== destinationName) {
      return refused(
        "Backup " + String((backup.metadata || {}).name) + " was written through destination " +
          (frozen || "(inline archive)") + " and this catalog reads " +
          (destinationName || "an inline archive"),
      );
    }
    if (destinationName.length > 0) {
      const problem = frozenDestinationProblem(backup, destination);
      if (problem !== null) {
        return refused(problem);
      }
    }
  }
  return {
    state: "selected",
    point: catalogRecoveryPoint(catalog, found.entry, destination, backup),
    destination: destination,
  };
}

/** The wizard over one catalog point: resolve it, refuse by name, or build the
 *  six steps over it. */
async function mountCatalogPoint(node, ns, selection, parse, api, lifecycle, clusters, backups,
  policyRead) {
  const choice = await resolveCatalogChoice(api, ns, selection, backups,
    catalogReadersOf(api, ns, lifecycle), lifecycle);
  if (choice === null || !active(lifecycle)) {
    return;
  }
  if (choice.state !== "selected") {
    replace(node, parse(renderCatalogPointRefusal(ns, choice)));
    return;
  }
  // The evidence selector's other destinations (PLAT-08.2) are not read for a
  // catalog point: `undefined` offers the point's own destination, as before.
  const state = initialState(ns, clusters, backups, selection, choice.destination, undefined,
    choice);
  // PLAT-19.2: the namespace's effective approval policy, read exactly as the
  // Backup-point mount reads it, so a catalog point's submission is routed by
  // the same policy as any other restore -- started with the lists (MCP-26).
  state.approvalPolicy = await (policyRead || readApprovalPolicy(api, ns, lifecycle));
  if (!active(lifecycle)) {
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
}

/** THE REFUSAL a catalog point that cannot be offered gets: the catalog and the
 *  point asked for, the reason in the catalog's own words, and a way back. No
 *  plan, no hash, no submit. */
export function renderCatalogPointRefusal(ns, choice) {
  const c = choice || {};
  const n = typeof ns === "string" ? ns : "";
  return (
    "<h2>Restore wizard</h2>" +
    "<div class=\"refusal-block\" id=\"catalog-point-refusal\" role=\"alert\">" +
    "<p class=\"refusal\">This recovery point is not offered for a restore: " +
    esc(c.reason) + ". Nothing was sent, and no other point was put in its place.</p>" +
    "<p class=\"note\">Asked for: point <code>" + esc(c.pointId) + "</code> in catalog " +
    "<code>" + esc(c.catalog) + "</code>.</p>" +
    "<p class=\"note\"><a href=\"#/catalog?ns=" + esc(encodeURIComponent(n)) + "&name=" +
    esc(encodeURIComponent(String(c.catalog || ""))) + "\">Open the catalog</a> &middot; " +
    "<a href=\"" + esc(restoreSelectorRoute(n)) + "\">Choose a recovery point</a></p></div>"
  );
}

/** EVERY CATALOG POINT THE SELECTOR MAY OFFER, across every catalog in the
 *  namespace: `{offers: [{catalog, entry}], notes: [sentence]}`.
 *
 *  Only rows [`catalogPointOffer`] offers are listed; a catalog whose points
 *  could not be read is a NOTE naming it, never an empty table that reads as
 *  "the archive holds nothing". Legacy mode has no point route at all and says
 *  so once. Never throws, except for a cancelled read. */
export async function readCatalogOffers(ns, readers, lifecycle) {
  const result = { offers: [], notes: [] };
  let catalogs;
  try {
    catalogs = itemsOf(await readers.listCatalogs());
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      throw error;
    }
    result.notes.push("the connected archives could not be listed here (" +
      String((error || {}).message || error) + ")");
    return result;
  }
  for (const catalog of catalogs) {
    const name = String((((catalog || {}).metadata) || {}).name || "");
    if (name.length === 0) {
      continue;
    }
    try {
      let cursor = null;
      let complete = false;
      for (let read = 0; read < CATALOG_POINT_PAGE_BUDGET; read += 1) {
        const query = { limit: CATALOG_POINT_PAGE, selectable: "true" };
        if (cursor !== null) {
          query.cursor = cursor;
        }
        const page = (await readers.readPoints(name, query)) || {};
        for (const entry of (Array.isArray(page.items) ? page.items : [])) {
          if (catalogPointOffer(entry, page).offer) {
            result.offers.push({ catalog: name, entry: entry });
          }
        }
        if (typeof page.backupVerdictsIncomplete === "string" &&
          page.backupVerdictsIncomplete.length > 0) {
          result.notes.push("catalog " + name + ": the product API could not read every " +
            "Backup verdict (" + page.backupVerdictsIncomplete + "), so its points are not " +
            "offered here");
        }
        cursor = ((page.page || {}).nextCursor) || null;
        if (cursor === null) {
          complete = true;
          break;
        }
      }
      if (!complete) {
        result.notes.push("catalog " + name + " holds more points than this page reads; open " +
          "it for the rest");
      }
    } catch (error) {
      if (cancelled(error, lifecycle)) {
        throw error;
      }
      result.notes.push("catalog " + name + "'s points could not be read (" +
        String((error || {}).message || error) + ")");
    }
  }
  return result;
}

/** The selector's catalog section: the points connected archives offer, each a
 *  link that opens the wizard on it. */
export function renderCatalogOffers(ns, offers) {
  const o = offers || { offers: [], notes: [] };
  const list = Array.isArray(o.offers) ? o.offers : [];
  const notes = Array.isArray(o.notes) ? o.notes : [];
  if (list.length === 0 && notes.length === 0) {
    return "";
  }
  const rows = list.map((offer) => {
    const e = offer.entry || {};
    return [
      // The action first, for the selector's reason (MCP-25).
      "<a class=\"button\" href=\"" + esc(restoreCatalogPointRoute(ns, offer.catalog, e.pointId)) +
        "\">Restore this point</a>",
      cell(offer.catalog) + "<span class=\"cell-sub\"><code>" + cell(e.pointId) + "</code></span>",
      cell(e.backupId),
      when(e.recoveryPointAt),
      "<span class=\"cell-sub-first\">from " + when(e.coveredFrom) + "</span>" +
        "<span class=\"cell-sub\">to " + when(e.coveredTo) + "</span>",
      badge("green", String(e.availability || "")) + " " +
        badge("green", String(e.verification || "")),
    ];
  });
  return (
    "<section class=\"step\" id=\"step-catalog-points\" tabindex=\"-1\">" +
    "<h3>Recovery points from connected archives</h3>" +
    "<p class=\"blurb\">" + esc(CATALOG_OFFERS_SENTENCE) + "</p>" +
    table(
      ["", "CATALOG AND POINT", "BACKUP SET", "RECOVERY POINT", "COVERED", "AVAILABLE AND VERIFIED"],
      rows,
      "no connected archive lists a point this page may offer",
      undefined,
      { id: "restore-catalog-offers", label: "points", scope: ns },
    ) +
    notes.map((note) => "<p class=\"note\" data-catalog-note=\"true\">" + esc(note) +
      "</p>").join("") +
    "</section>"
  );
}

/** What the selector's catalog section is. */
export const CATALOG_OFFERS_SENTENCE =
  "Points read back from an archive this namespace has connected (a RecoveryCatalog), listed " +
  "only where the catalog marks them restorable and the product API found no Backup verdict " +
  "refusing them. No Backup object is needed: a plan built from one of these is bound to the " +
  "point's own receipt, which the runner re-verifies before any data moves.";

/** The namespace's approval policy, or `null` when this mode has none to
 *  offer or the read failed. A cancelled read is still a cancellation. */
async function readApprovalPolicy(api, ns, lifecycle) {
  if (typeof (api || {}).approvalPolicy !== "function") {
    return null;
  }
  try {
    return await api.approvalPolicy(ns, readOptions(lifecycle));
  } catch (unread) {
    if (cancelled(unread, lifecycle)) {
      throw unread;
    }
    return null;
  }
}

/** Renders the wizard over `state` -- with its mutation record and its field
 *  messages -- and wires what was rendered to the plan it shows. */
/** The wizard's text inputs: each commits its value to the state on `change`
 *  (blur or Enter), not on every keystroke, because a commit re-renders the
 *  plan and its hash. */
export const WIZARD_TEXT_INPUTS = Object.freeze([
  "point-in-time", "topic-prefix", "store-endpoint", "store-region", "evidence-bucket",
  "archive-secret", "evidence-endpoint", "evidence-region", "catalog-topics",
]);

/** Whether a control's value is not the one the last render gave it -- what
 *  the reader typed and has not committed. A browser keeps the rendered value
 *  as `defaultValue`; the suites' fakes keep it as the `value` attribute. */
export function typedSinceRender(control) {
  if (control === null || control === undefined) {
    return false;
  }
  const rendered = typeof control.defaultValue === "string"
    ? control.defaultValue
    : (typeof control.getAttribute === "function" ? (control.getAttribute("value") || "") : "");
  return String(control.value === undefined || control.value === null ? "" : control.value) !==
    rendered;
}

/** Whether the reader is part-way through typing into one of the wizard's
 *  text inputs. */
export function editingWizard(node) {
  return WIZARD_TEXT_INPUTS.some((id) => typedSinceRender(node.querySelector("#" + id)));
}

/** Renders the wizard from `state` and wires it.
 *
 *  `landing` IS AN ANSWER ARRIVING, not the reader acting: a followed check's
 *  read, a mutation settling, a cancel answered. SUCH A REPAINT WAITS FOR THE
 *  READER (P13's class): the wizard commits a text input on `change`, so a
 *  repaint under an input being typed into rendered it from the state and the
 *  keystrokes since the last commit were gone. The answer is already in
 *  `state`; the paint is owed, and the reader's own commit -- the `change`
 *  that ends the edit -- renders it with everything else. */
async function renderAndWire(node, state, parse, api, lifecycle, landing) {
  if (landing === true && editingWizard(node)) {
    state.paintOwed = true;
    return false;
  }
  state.paintOwed = false;
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
  if (state.jumpToErrors === true) {
    state.jumpToErrors = false;
    const at = firstStepWithErrors(state.errors);
    if (at !== null) {
      state.step = at;
    } else if (record.state.phase === "failed") {
      // A refusal no input claims is shown beside the submit, in step 6.
      state.step = STEPS.length - 1;
    }
  }
  const prepared = await preparePlanOrProblem(state);
  if (!active(lifecycle)) {
    return false;
  }
  replace(node, parse(renderPreparedWizard(state, prepared)));
  wire(node, state, parse, api, lifecycle, prepared);
  return true;
}

/** SHOW ONE STEP (MCP-29): record it, write it into the address, re-render
 *  from the same state and move focus to the step. `index` outside the six is
 *  ignored. Exported for the suite, which drives it over a fake node. */
export async function showWizardStep(node, state, parse, api, lifecycle, index) {
  if (!Number.isInteger(index) || index < 0 || index >= STEPS.length || !active(lifecycle)) {
    return false;
  }
  state.step = index;
  syncStepAddress(lifecycle, index);
  const rendered = await renderAndWire(node, state, parse, api, lifecycle);
  if (!rendered) {
    return false;
  }
  const section = node.querySelector("#" + STEPS[index].id);
  if (section !== null && typeof section.focus === "function") {
    section.focus({ preventScroll: true });
    if (typeof section.scrollIntoView === "function") {
      section.scrollIntoView({ block: "nearest" });
    }
  }
  announce("Step " + String(index + 1) + " of " + String(STEPS.length) + ": " +
    STEPS[index].title);
  return true;
}

// THE STEP GOES INTO THE ADDRESS WITHOUT A NAVIGATION. `replaceState` fires no
// `hashchange`, so nothing re-mounts and nothing is read again; the route's
// lifecycle is told the new address in the same synchronous turn, because it
// judges "is this route still current" by comparing the address it began on.
// A lifecycle that cannot be retargeted (the suites' fakes) leaves the address
// alone rather than making every later read look like a stale one.
function syncStepAddress(lifecycle, index) {
  if (typeof window === "undefined" || !window.location || !window.history ||
    typeof window.history.replaceState !== "function" ||
    lifecycle === null || lifecycle === undefined || typeof lifecycle.retarget !== "function") {
    return;
  }
  const next = restoreStepRoute(window.location.hash, index);
  if (next === window.location.hash) {
    return;
  }
  window.history.replaceState(window.history.state, "", next);
  lifecycle.retarget(next);
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
export function initialState(ns, clusters, backups, selection, savedDestination, destinations,
  catalogChoice) {
  // PLAT-15.2: a catalog-verified point the mount half has already resolved
  // and checked ([`resolveCatalogChoice`]). It REPLACES the Backup resolution
  // -- there may be no Backup at all -- and nothing below searches for a
  // different point when it is absent.
  const chosenFromCatalog = ((catalogChoice || {}).point) || null;
  const resolved = chosenFromCatalog !== null
    ? {
      state: "selected",
      point: chosenFromCatalog,
      uid: catalogPointUid(chosenFromCatalog),
      name: chosenFromCatalog.catalogPoint.pointId,
      phase: "Succeeded",
      renamed: false,
    }
    : resolvePoint(backups, selection);
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
  // THE DEFAULT IS THE LAST INSTANT THE RUNNER ACCEPTS, `toMs - 1`, never the
  // exclusive `toMs` itself (WIZARD-DEFAULT-PIT-EXCLUSIVE).
  const restorable = restorableWindow(covered) || {};
  const pointInTime = rfc3339(restorable.toMs);
  const archive = spec.archive || {};
  const archiveUrl = archive.url;
  const destinationName = savedDestinationName({ point: point });
  const destinationProblem = frozenDestinationProblem(point, savedDestination);
  const destinationSettings = destinationProblem === null
    ? savedDestinationStore(savedDestination, destinationName)
    : null;
  const savedPoint = destinationName.length > 0;
  // BOTH HALVES OF THE ARCHIVE REFERENCE, FROM THE SAME OBJECT -- and that
  // object is the chosen point. A URL taken from one Backup and a credential
  // taken from another would be two archives and one name for them.
  const archiveSecretName = destinationName.length > 0 ? "" :
    (typeof ((archive.secretRef || {}).name) === "string" ? archive.secretRef.name : "");
  const target = firstTarget(clusters);
  // `allowHttp` STARTS FALSE AND IS NEVER DERIVED (D-SEAMS S5). It is set by
  // the one explicit checkbox in step 1 and by nothing else.
  const store = destinationSettings ||
    { region: "", endpoint: "", pathStyle: false, allowHttp: false };
  const prefix = retryOf.length > 0
    ? freshTargetPrefix(pointInTime, retryOf)
    : prefixFor(pointInTime);
  return {
    ns: ns,
    retryOf: retryOf,
    clusters: clusters,
    backups: backups,
    // THE ROUTE'S IDENTITY, KEPT FOR EVERY RE-MOUNT: for a catalog point the
    // catalog and the point id, and the run it was offered from when there
    // was one.
    selection: chosenFromCatalog !== null
      ? {
        catalog: chosenFromCatalog.catalogPoint.catalog,
        point: chosenFromCatalog.catalogPoint.pointId,
        uid: ((chosenFromCatalog.catalogPoint.backup || {}).uid) || "",
        backup: ((chosenFromCatalog.catalogPoint.backup || {}).name) || "",
        retryOf: retryOf,
      }
      : { uid: resolved.uid, backup: resolved.name },
    catalogTopicsText: "",
    // MCP-29: the step on screen, 0-based. The route's `step` (1 to 6) when it
    // named one, the first step otherwise.
    step: stepIndexOf((selection || {}).step),
    point: point,
    pointState: resolved.state,
    pointUid: resolved.uid,
    pointName: resolved.name,
    pointPhase: resolved.phase,
    pointRenamed: resolved.renamed,
    query: "",
    archiveUrl: archiveUrl,
    archiveSecretName: archiveSecretName,
    savedDestination: destinationSettings === null ? null : savedDestination,
    savedDestinationProblem: destinationProblem,
    // PLAT-08.2: WHERE THE EVIDENCE GOES, as its own choice. A saved point
    // starts on its own destination -- inherited, nothing re-entered -- and may
    // name another saved destination; a legacy point starts with its evidence
    // store sharing the archive store's settings, said on screen as a ticked
    // box, so an existing walkthrough's bytes do not move.
    evidenceDestinations: Array.isArray(destinations) ? destinations : [],
    evidenceDestinationsUnavailable: savedPoint && destinations === null,
    evidenceDestination: destinationSettings === null ? null : destinationPin(savedDestination),
    evidenceDestinationProblem: null,
    evidenceSameAsArchive: true,
    evidenceBucket: savedPoint
      ? (destinationSettings === null ? "" : destinationSettings.bucket)
      : legacyEvidenceBucket(archiveUrl),
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
      // THE RECOVERY POINT BINDING (PLAT-15.2): present for a catalog point,
      // ABSENT for a Backup -- whose plan is therefore the document it always
      // was, byte for byte.
      point: pointBindingOf(point),
      pointInTime: pointInTime,
      source: Object.assign(
        !savedPoint
          ? { bucket: bucketOf(archiveUrl), prefix: prefixOf(archiveUrl) }
          : (destinationSettings === null
            ? { bucket: "", prefix: "" }
            : { bucket: destinationSettings.bucket, prefix: destinationSettings.prefix }),
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
        windowEnd: rfc3339(restorable.toMs),
        recordsPerPartition: 25,
        anchor: "head",
      },
      objectives: {},
      evidence: Object.assign({}, store, {
        bucket: savedPoint
          ? (destinationSettings === null ? "" : destinationSettings.bucket)
          : legacyEvidenceBucket(archiveUrl),
        // Evidence never inherits the archive prefix. Global Constraint 6 is
        // the saved-destination evidence location too.
        prefix: EVIDENCE_PREFIX,
      }),
    },
  };
}

/** WHERE A POINT WITH NO DESTINATION WRITES ITS EVIDENCE: the archive's own
 *  bucket (PoC defect P5).
 *
 *  This used to be the literal `logweir-evidence`. The controller reads an
 *  inline-archive run's scorecard ONLY through its own archive handle
 *  (`LOGWEIR_ARCHIVE_URL`), and only in that handle's bucket; the runner writes
 *  it with the archive's credential, the one that already wrote this point's
 *  receipt under `<archive bucket>/logweir/`. A plan naming any other bucket
 *  restored the data and then published no verification and no completion
 *  (`weirkeeper` `destination::legacy_evidence_scope`). The archive's bucket is
 *  the one the archive credential is known to write, and on an installation
 *  whose handle is over its archive -- the chart's default -- the one the
 *  controller reads. The field stays editable; the readiness check warns
 *  (`destination.evidenceReadable`) when the plan names a bucket the
 *  controller will not read. An archive URL with no bucket leaves the field
 *  EMPTY, so the plan is refused as incomplete rather than pointed anywhere. */
export function legacyEvidenceBucket(archiveUrl) {
  if (typeof archiveUrl !== "string" || archiveUrl.indexOf("://") === -1) {
    return "";
  }
  const bucket = bucketOf(archiveUrl);
  return bucket === bucketOf("") ? "" : bucket;
}

/** Said under a legacy point's evidence bucket field. */
export const LEGACY_EVIDENCE_BUCKET_SENTENCE =
  "It starts as this recovery point's own archive bucket. Weirkeeper verifies the signed " +
  "scorecard of a point with no saved destination only through its own archive handle " +
  "(LOGWEIR_ARCHIVE_URL), in that handle's bucket: name another bucket and the restore still " +
  "runs, but its verification reads NotAttempted and no completion is written.";

/** The saved destination frozen onto a recovery point, if this is not legacy. */
function savedDestinationName(state) {
  const destination = ((((state || {}).point || {}).spec || {}).destinationRef) || {};
  return typeof destination.name === "string" ? destination.name : "";
}

/** A destination-backed point is pinned by all three frozen public facts.
 * The live object answering the name must still be that exact destination,
 * at that exact location. A replacement under the same name is not a source
 * for this recovery point. */
export function frozenDestinationProblem(point, destination) {
  const p = point || {};
  const ref = ((p.spec || {}).destinationRef) || {};
  const expectedName = typeof ref.name === "string" ? ref.name : "";
  if (expectedName.length === 0) {
    return null;
  }
  const expectedUid = typeof ref.uid === "string" ? ref.uid : "";
  const expectedDigest = typeof ((p.status || {}).locationDigest) === "string"
    ? p.status.locationDigest
    : "";
  if (expectedUid.length === 0 || expectedDigest.length === 0) {
    return "recovery point " + String((p.metadata || {}).name || "(unknown)") +
      " does not publish the frozen destination UID and location digest required to restore";
  }
  const live = destination || {};
  if (live.name !== expectedName) {
    return "saved destination " + expectedName + " resolved as a different name";
  }
  if (live.uid !== expectedUid) {
    return "saved destination " + expectedName + " was recreated: recovery point UID " +
      expectedUid + " does not match live UID " + String(live.uid || "(absent)");
  }
  if (live.locationDigest !== expectedDigest) {
    return "saved destination " + expectedName + " moved: recovery point location digest " +
      expectedDigest + " does not match live digest " +
      String(live.locationDigest || "(absent)");
  }
  return null;
}

/** The mount gate: no wizard state, plan bytes or controls exist until the
 * live public destination still matches the recovery point's frozen binding. */
export function requireFrozenDestination(point, destination) {
  const problem = frozenDestinationProblem(point, destination);
  if (problem !== null) {
    throw refusal(problem);
  }
  return destination;
}

/** Only public destination fields become signed storage settings. */
function savedDestinationStore(destination, expectedName) {
  const item = destination || {};
  const storage = item.storage || {};
  const transport = item.transport || {};
  if (typeof expectedName !== "string" || expectedName.length === 0 ||
      item.name !== expectedName || storage.provider !== "s3" ||
      typeof storage.bucket !== "string" || storage.bucket.length === 0 ||
      typeof storage.prefix !== "string") {
    return null;
  }
  return {
    bucket: storage.bucket,
    prefix: storage.prefix,
    region: typeof storage.region === "string" ? storage.region : "",
    endpoint: typeof storage.endpoint === "string" ? storage.endpoint : "",
    pathStyle: storage.addressing === "pathStyle",
    allowHttp: transport.security === "insecureHttp",
  };
}

function savedDestinationResolved(state) {
  const name = savedDestinationName(state);
  return name.length === 0 ||
    ((state || {}).savedDestinationProblem === null &&
      savedDestinationStore((state || {}).savedDestination, name) !== null);
}

// ------------------------------------ the evidence destination (PLAT-08.2)

/** The three public facts that pin a saved destination: its name, its uid and
 *  the location digest its controller published. */
function destinationPin(destination) {
  const d = destination || {};
  return {
    name: typeof d.name === "string" ? d.name : "",
    uid: typeof d.uid === "string" ? d.uid : "",
    locationDigest: typeof d.locationDigest === "string" ? d.locationDigest : "",
  };
}

/** A saved destination's EVIDENCE store: its bucket rooted at Global
 *  Constraint 6's `logweir/`, reached with its own endpoint, region,
 *  addressing and transport -- `BackupDestination::evidence_storage_url`,
 *  field for field. `null` when the destination publishes no usable storage. */
export function evidenceStoreOf(destination) {
  const d = destination || {};
  const store = savedDestinationStore(d, d.name);
  if (store === null) {
    return null;
  }
  return {
    bucket: store.bucket,
    prefix: EVIDENCE_PREFIX,
    region: store.region,
    endpoint: store.endpoint,
    pathStyle: store.pathStyle,
    allowHttp: store.allowHttp,
  };
}

/** The saved destinations the evidence selector offers: the recovery point's
 *  own first, then every other one this page read, each once by uid.
 *
 *  THE LIST IS `DestinationSummary`, which names a destination and publishes
 *  its canonical URL, endpoint, addressing and transport but NOT its bucket,
 *  prefix, region or location digest -- so an option is a choice to offer, and
 *  the storage signed for it comes from the FULL destination, read when it is
 *  chosen (`wire`, `mountRestoreWizard`) and pinned then. */
export function evidenceDestinationOptions(state) {
  const s = state || {};
  const own = s.savedDestination || null;
  const seen = new Set();
  const out = [];
  for (const d of [own].concat(Array.isArray(s.evidenceDestinations) ? s.evidenceDestinations : [])) {
    if (d === null || d === undefined || typeof d.uid !== "string" || d.uid.length === 0 ||
      typeof d.name !== "string" || d.name.length === 0 || seen.has(d.uid)) {
      continue;
    }
    seen.add(d.uid);
    out.push(d);
  }
  return out;
}

/** The evidence destination's NAME, or "" for a legacy point. */
/** The Secret a readiness request names for an inline archive, or `null` for
 *  a request that names none (a saved destination's check). */
export function readinessSecretOf(request) {
  const legacy = ((((request || {}).restore) || {}).legacySourceArchive) || null;
  if (legacy === null) {
    return null;
  }
  const name = ((legacy.credentialRef || {}).name);
  return typeof name === "string" ? name : "";
}

function boundSecretOf(about) {
  const a = about || {};
  return typeof a.archiveSecret === "string" ? a.archiveSecret : null;
}

/** THE ARCHIVE SECRET, AND WHAT A CHANGE OF IT DOES TO A HELD CHECK (review
 *  M1(a)).
 *
 *  For a point with no saved destination the readiness check reads the
 *  archive with the Secret the Restore will project -- that is P3's whole
 *  claim -- and the Secret is not in the plan bytes, so a plan-hash
 *  comparison cannot see it move. A change marks a held verdict stale exactly
 *  as a change of target or evidence destination does (`referentChanged`,
 *  kind `Secret`), and `readinessRefusal` also compares the Secret the check
 *  was started with against the one on screen, so the Create stays refused
 *  until the check is run again with this Secret. */
export function setArchiveSecret(state, value) {
  const next = String(typeof value === "string" ? value : "").trim();
  const before = String(state.archiveSecretName || "").trim();
  const held = (state.readiness || {}).preflight || null;
  if (next !== before && held !== null) {
    state.readiness = Object.assign({}, state.readiness, {
      preflight: Object.assign({}, held, {
        applicable: false,
        stale: true,
        staleReasons: [{
          reason: "referentChanged",
          kind: "Secret",
          name: next.length > 0 ? next : "(none)",
        }],
      }),
    });
  }
  state.archiveSecretName = typeof value === "string" ? value : "";
}

export function evidenceDestinationName(state) {
  const pin = (state || {}).evidenceDestination || null;
  return pin !== null && typeof pin.name === "string" ? pin.name : "";
}

/** Chooses the evidence destination BY UID, and rebuilds the plan's evidence
 *  block from it and from nothing else.
 *
 *  A uid that no longer answers is a REFUSAL, not a fallback: the evidence
 *  block is emptied, so no plan can be built, and the selector says why. A
 *  change of uid also marks a held readiness verdict stale, for
 *  `selectTarget`'s reason -- two destinations can publish identical storage
 *  and so render identical bytes, and the verdict was about the other one. */
export function selectEvidenceDestination(state, uid, name, item) {
  const wanted = typeof uid === "string" ? uid : "";
  const before = ((state.evidenceDestination || {}).uid) || "";
  const option = evidenceDestinationOptions(state).find((d) => d.uid === wanted) || null;
  // THE FULL OBJECT, WHEN THE CALLER READ IT, and only when it IS the chosen
  // uid: a read that answered with another uid is a recreated destination.
  const full = item !== undefined && item !== null && item.uid === wanted ? item : option;
  const found = full !== null && evidenceStoreOf(full) !== null ? full : null;
  const held = (state.readiness || {}).preflight || null;
  if (wanted !== before && held !== null) {
    state.readiness = Object.assign({}, state.readiness, {
      preflight: Object.assign({}, held, {
        applicable: false,
        stale: true,
        staleReasons: [{
          reason: "referentChanged",
          kind: "BackupDestination",
          name: found !== null ? found.name : (typeof name === "string" ? name : wanted),
        }],
      }),
    });
  }
  if (found === null) {
    const label = option !== null ? option.name
      : (typeof name === "string" && name.length > 0 ? name : "(unnamed)");
    state.evidenceDestination = { name: label, uid: wanted, locationDigest: "" };
    state.evidenceDestinationProblem = wanted.length === 0
      ? "choose the saved destination the evidence is written to"
      : (option !== null
        ? "saved destination " + label + " (uid " + wanted + ") could not be read in full, so " +
          "its evidence location and transport are unknown and nothing is signed for it; " +
          "choose it again, or another one"
        : "saved destination " + label + " (uid " + wanted + ") is not in this namespace any " +
          "more, or was recreated under a new uid; a different destination is a different " +
          "store reached with a different credential, so choose the evidence destination you " +
          "mean");
    state.fields.evidence = Object.assign({}, state.fields.evidence, { bucket: "" });
    state.evidenceBucket = "";
    return;
  }
  state.evidenceDestinations = (Array.isArray(state.evidenceDestinations)
    ? state.evidenceDestinations : []).map((d) => (d && d.uid === found.uid ? found : d));
  state.evidenceDestination = destinationPin(found);
  state.evidenceDestinationProblem = null;
  state.fields.evidence = evidenceStoreOf(found);
  state.evidenceBucket = state.fields.evidence.bucket;
}

/** For a LEGACY point whose "same store" box is ticked: copies the archive
 *  store's four transport-and-address settings into the evidence block. The
 *  caller decides whether the box is ticked; a saved point is never touched.
 *  The bucket and the prefix are never copied: evidence has its own bucket
 *  and Global Constraint 6's prefix. */
export function syncEvidenceStore(state) {
  const s = state || {};
  if (savedDestinationName(s).length > 0) {
    return;
  }
  const source = (s.fields || {}).source || {};
  const evidence = (s.fields || {}).evidence || {};
  evidence.endpoint = source.endpoint;
  evidence.region = source.region;
  evidence.pathStyle = source.pathStyle === true;
  evidence.allowHttp = source.allowHttp === true;
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
      // THE SECRET THE CHECK READ WITH (review M1(a)). It is not in the plan
      // bytes, so the hash above cannot see it change; `readinessRefusal`
      // compares it with the Secret the Restore would project.
      boundSecret: record.phase === "succeeded"
        ? boundSecretOf(record.about)
        : ((state.readiness || {}).boundSecret === undefined
          ? null
          : state.readiness.boundSecret),
    });
    renderAndWire(node, state, parse, api, lifecycle, true);
    if (record.phase === "succeeded") {
      followRestoreReadiness(node, state, parse, api, lifecycle);
    }
  }, lifecycle);

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    if (typeof p.bytes !== "string" || typeof p.hash !== "string") {
      return;
    }
    const request = restoreReadinessRequest(state, p);
    mutation.run(() => api.startPreflight(state.ns, request, {
      attempt: nextRestoreReadinessAttempt(state.ns, state.pointUid),
    }), { about: { planHash: p.hash, archiveSecret: readinessSecretOf(request) } });
  }, lifecycle);

  const cancel = node.querySelector("#restore-readiness-cancel");
  if (cancel !== null) {
    listen(cancel, "click", () => {
      const current = (state.readiness || {}).preflight;
      if (!active(lifecycle) || current === null || current === undefined) {
        return;
      }
      disableKeepingFocus(cancel, true);
      api.cancelPreflight(state.ns, current.id).then(
        () => {
          if (active(lifecycle)) {
            renderAndWire(node, state, parse, api, lifecycle, true);
          }
        },
        () => {
          cancel.disabled = false;
        },
      );
    }, lifecycle);
  }
}

/** How many times step 5 re-reads a started check, and the gap between reads.
 *  Bounded on purpose, as the schedules form is: a form is not a watcher. */
export const RESTORE_READINESS_POLLS = 45;
export const RESTORE_READINESS_INTERVAL_MS = 2000;

/** A held verdict, replaced by a fresher read of the SAME check -- except that
 *  staleness is one-way on this page. A mark `selectTarget` or
 *  `selectEvidenceDestination` made is about a choice the server cannot see
 *  (the check is bound to the referent it was started against, which may be
 *  perfectly unchanged), so a fresher `applicable: true` must not erase it.
 *  Only a NEW check, started for the current choices, does.
 *
 *  `unverifiable` IS NOT ONE OF THOSE MARKS (defect P8's class). It is the
 *  SERVER saying what it could not compare in THAT answer -- above all the
 *  create answer's "this response did not recompute staleness; read the
 *  preflight itself" -- and a fresher read is exactly the answer it asks for.
 *  Carrying it one-way left a replayed check reading "could not be checked"
 *  after the read that did check it. */
export function mergeReadiness(held, fresh) {
  if (fresh === null || fresh === undefined) {
    return held === undefined ? null : held;
  }
  if (held === null || held === undefined || held.id !== fresh.id || held.stale !== true) {
    return fresh;
  }
  const marks = (Array.isArray(held.staleReasons) ? held.staleReasons : [])
    .filter((r) => (r || {}).reason !== "unverifiable");
  if (marks.length === 0) {
    return fresh;
  }
  const reasons = (Array.isArray(fresh.staleReasons) ? fresh.staleReasons : []).slice();
  for (const r of marks) {
    if (!reasons.some((x) => x.reason === r.reason && x.kind === r.kind && x.name === r.name)) {
      reasons.push(r);
    }
  }
  return Object.assign({}, fresh, { applicable: false, stale: true, staleReasons: reasons });
}

/** Re-reads the check step 5 started until it is terminal, asking the product
 *  API about the plan on screen NOW (`?planHash=`), and repaints when the
 *  answer changes. Before PLAT-08.2 the wizard showed only the create answer
 *  -- a check that had not run yet -- and never learned its verdict. */
async function followRestoreReadiness(node, state, parse, api, lifecycle) {
  const first = (state.readiness || {}).preflight || null;
  if (first === null || typeof first.id !== "string" || typeof api.preflight !== "function") {
    return;
  }
  const id = first.id;
  const wait = typeof api.wait === "function"
    ? api.wait
    : (ms) => new Promise((done) => { globalThis.setTimeout(done, ms); });
  for (let read = 0; read < RESTORE_READINESS_POLLS; read += 1) {
    const held = (state.readiness || {}).preflight || null;
    // THE CREATE ANSWER IS NOT A READ (P8), even when it is terminal.
    if (held === null || held.id !== id || !owesRead(held, read)) {
      return;
    }
    await wait(RESTORE_READINESS_INTERVAL_MS);
    if (!active(lifecycle)) {
      return;
    }
    const current = await preparePlanOrProblem(state);
    let answer;
    try {
      answer = await api.preflight(state.ns, id, Object.assign({}, readOptions(lifecycle),
        typeof current.hash === "string" ? { planHash: current.hash } : {}));
    } catch (error) {
      // A FAILED RE-READ IS NOT A VERDICT: the one on screen stays what the
      // check last recorded, and the submit re-reads it again anyway.
      return;
    }
    if (!active(lifecycle)) {
      return;
    }
    const now = (state.readiness || {}).preflight || null;
    if (now === null || now.id !== id) {
      return;
    }
    const merged = mergeReadiness(now, (answer || {}).item);
    const moved = merged.state !== now.state || merged.terminal !== now.terminal ||
      merged.stale !== now.stale || merged.applicable !== now.applicable;
    state.readiness = Object.assign({}, state.readiness, { preflight: merged });
    if (moved) {
      await renderAndWire(node, state, parse, api, lifecycle, true);
    }
  }
}

/** The exact step-5 request for one recovery point.
 *
 * A destination-backed point names the saved destination for both archive
 * and evidence reads. Its `recoveryPoint` reference makes the controller read
 * that Backup and compare `status.destination.locationDigest` with the live
 * destination; the browser never recomputes a digest. Only a point with no
 * `destinationRef` is a legacy inline-archive point. */
export function restoreReadinessRequest(state, prepared) {
  const s = state || {};
  const p = prepared || {};
  const cluster = targetCluster(s);
  const point = s.point || {};
  const meta = point.metadata || {};
  const spec = point.spec || {};
  const request = {
    operation: "restore",
    restore: {
      planBytes: p.bytes,
      planHash: p.hash,
      target: ((cluster || {}).metadata || {}).name,
    },
  };
  if (isCatalogPoint(point)) {
    // A CATALOG POINT IS NAMED BY ITS CATALOG AND ITS ID, and by nothing else:
    // one check answers `recoveryPoint.state` about ONE point (CRD rule P10),
    // and the catalog row is the one the plan is bound to. The controller
    // re-reads that row when the check runs -- joined with every Backup
    // verdict on the same receipt, including the run this offer came from --
    // so a point that became Missing or refused since this page read it is
    // not ready.
    const c = point.catalogPoint;
    request.restore.catalogPoint = { catalog: c.catalog, pointId: c.pointId };
  } else if (typeof meta.name === "string" && meta.name.length > 0) {
    request.restore.recoveryPoint = { backupName: meta.name };
    if (typeof meta.uid === "string" && meta.uid.length > 0) {
      request.restore.recoveryPoint.backupUid = meta.uid;
    }
  }
  const destination = spec.destinationRef || {};
  if (typeof destination.name === "string" && destination.name.length > 0) {
    request.restore.sourceDestination = destination.name;
    request.restore.evidenceDestination = evidenceDestinationName(s);
    return request;
  }
  const archive = spec.archive || {};
  if (typeof archive.url === "string" && archive.url.length > 0) {
    request.restore.legacySourceArchive = { url: archive.url };
    const secretName = String(s.archiveSecretName || "").trim();
    if (secretName.length > 0) {
      request.restore.legacySourceArchive.credentialRef = { name: secretName };
    }
  }
  return request;
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
  const evidenceSame = node.querySelector("#evidence-same-store");
  const evidenceEndpoint = node.querySelector("#evidence-endpoint");
  const evidenceRegion = node.querySelector("#evidence-region");
  const evidencePathStyle = node.querySelector("#evidence-pathStyle");
  const evidenceInsecure = node.querySelector("#evidence-allow-insecure");
  const evidenceDestination = node.querySelector("#evidence-destination");
  const catalogTopics = node.querySelector("#catalog-topics");
  const refresh = async () => {
    if (!active(lifecycle)) {
      return;
    }
    // THE CATALOG POINT'S TOPIC LIST (PLAT-15.2), read FIRST: it is the list
    // the subset boxes below are drawn from, so a new list replaces the subset
    // with every topic it names, and the boxes are read only when the list did
    // not change in this edit.
    let topicsRetyped = false;
    if (catalogTopics !== null && isCatalogPoint(state.point)) {
      const typed = parseTopicList(valueOf(catalogTopics));
      if (typed.join("\n") !== frozenTopicsOf(state).join("\n")) {
        setCatalogTopics(state, typed);
        topicsRetyped = true;
      }
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
    // THE ARCHIVE CONTROLS WRITE THE ARCHIVE BLOCK (PLAT-08.2). Until the
    // evidence store had controls of its own, this loop wrote the same four
    // values into BOTH blocks; now the evidence block gets them only through
    // `syncEvidenceStore`, and only while the "same store" box says so.
    for (const block of [state.fields.source]) {
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
    if (evidenceSame !== null) {
      state.evidenceSameAsArchive = evidenceSame.checked === true;
    }
    if (state.evidenceSameAsArchive !== false) {
      syncEvidenceStore(state);
    } else {
      // THE EVIDENCE STORE'S OWN FOUR, from its own controls and from no other.
      if (evidenceEndpoint !== null) {
        state.fields.evidence.endpoint = valueOf(evidenceEndpoint);
      }
      if (evidenceRegion !== null) {
        state.fields.evidence.region = valueOf(evidenceRegion);
      }
      if (evidencePathStyle !== null) {
        state.fields.evidence.pathStyle = evidencePathStyle.checked === true;
      }
      if (evidenceInsecure !== null) {
        state.fields.evidence.allowHttp = evidenceInsecure.checked === true;
      }
    }
    if (evidenceDestination !== null) {
      // THE OPTION CARRIES A UID AND NOTHING ELSE: the name comes from the list
      // this page read, by that uid, so a label can never choose a store.
      const picked = valueOf(evidenceDestination);
      if (picked !== (((state.evidenceDestination || {}).uid) || "") ||
        typeof state.evidenceDestinationProblem === "string") {
        const option = evidenceDestinationOptions(state).find((d) => d.uid === picked) || null;
        let item = null;
        if (option !== null && evidenceStoreOf(option) === null && typeof api.destination === "function") {
          try {
            item = ((await api.destination(state.ns, option.name, readOptions(lifecycle))) || {}).item || null;
          } catch (unread) {
            if (cancelled(unread, lifecycle)) {
              return;
            }
            item = null;
          }
          if (!active(lifecycle)) {
            return;
          }
        }
        selectEvidenceDestination(state, picked,
          option !== null ? option.name : (((state.evidenceDestination || {}).name) || ""), item);
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
      setArchiveSecret(state, valueOf(archiveSecret));
    }
    // THE SUBSET, READ FROM THE BOXES THAT ARE ON SCREEN (PLAT-11.2) -- and
    // only when there ARE boxes, so a refusal page or a point with no frozen
    // list cannot empty the selection behind the operator's back. The list is
    // canonicalised through `selectedTopics` so the plan bytes, and therefore
    // the hash an approver signs, do not move with the order of the clicks.
    const boxes = node.querySelectorAll(".topic-box");
    if (boxes.length > 0 && !topicsRetyped) {
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
    evidenceSame,
    evidenceEndpoint,
    evidenceRegion,
    evidencePathStyle,
    evidenceInsecure,
    evidenceDestination,
    catalogTopics,
  ]) {
    if (field !== null) {
      listen(field, "change", refresh, lifecycle);
    }
  }
  // AN OWED PAINT IS PAID WHEN AN EDIT ENDS WITHOUT A COMMIT: the reader typed
  // and put the text back, so no `change` fires and nothing else would render
  // the answer that landed meanwhile.
  for (const id of WIZARD_TEXT_INPUTS) {
    const field = node.querySelector("#" + id);
    if (field !== null) {
      listen(field, "blur", () => {
        if (active(lifecycle) && state.paintOwed === true && !editingWizard(node)) {
          renderAndWire(node, state, parse, api, lifecycle);
        }
      }, lifecycle);
    }
  }
  // THE TICKET IS NOT IN THE PLAN BYTES, so typing it re-renders nothing: it
  // is read into the state and sent beside the create body (PLAT-19.2).
  const ticket = node.querySelector("#change-ticket");
  if (ticket !== null) {
    listen(ticket, "input", () => {
      state.ticket = valueOf(ticket);
    }, lifecycle);
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

  // ONE STEP AT A TIME (MCP-29). The stepper's buttons, Back, Next and every
  // "go to step" link show THEIR step: the state records it, the address
  // records it (so a reload or a copied link opens the same step), the wizard
  // re-renders from the same state -- no read, no new plan -- and focus moves
  // to the step's own heading, which a screen reader then announces.
  for (const control of node.querySelectorAll(".stepper-link, .wizard-go")) {
    listen(control, "click", () => {
      if (!active(lifecycle)) {
        return;
      }
      const raw = control.getAttribute("data-go-step") !== null
        ? control.getAttribute("data-go-step")
        : control.getAttribute("data-step");
      return showWizardStep(node, state, parse, api, lifecycle, Number(raw));
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
      mountRestoreWizard(node, state.ns,
        Object.assign({}, state.selection, { step: shownStep(state) + 1 }), parse, api, lifecycle);
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
        disableKeepingFocus(button, true);
        button.setAttribute("aria-busy", "true");
      }
      const status = node.querySelector("#restore-submit-status");
      if (status !== null) {
        replace(status, parse(submissionStatus(settled, prepared, [])));
      }
      return;
    }
    // A REFUSED SUBMIT SHOWS THE STEP ITS FIRST FIELD MESSAGE IS ABOUT (MCP-29):
    // with one step on screen, a message beside an input three steps back is a
    // message nobody sees. Once, on the settlement -- an edit made afterwards
    // does not pull the reader back.
    if (settled.phase === "failed") {
      state.jumpToErrors = true;
    }
    renderAndWire(node, state, parse, api, lifecycle, true).then((rendered) => {
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
 *  `hidden` AND NOTHING ELSE. Below 768 px (Clarity's `sm` width) the
 *  stylesheet turns every `table.grid` row into a card with `display: block`,
 *  and an author rule beats the user agent's `[hidden] { display: none }`; the
 *  stylesheet's base section therefore restates `[hidden]` with `!important`,
 *  so the attribute an assistive technology reads is also the one that hides
 *  the row, and no page writes an inline style (PLAT-18.2's token lint
 *  forbids one). */
function wireSelector(node, state, lifecycle) {
  const search = node.querySelector("#point-search");
  if (search === null) {
    return;
  }
  const rows = Array.from(node.querySelectorAll("tr[data-search]"));
  const empty = node.querySelector("#no-match");
  const moreBar = node.querySelector("#point-more-bar");
  const more = node.querySelector("#point-more");
  const count = node.querySelector("#point-count");
  // THE SELECTOR SHOWS A PAGE OF MATCHES, NOT ALL OF THEM (MCP-26). 258 points
  // were 258 rows laid out at once -- a 50,000 px page at 1440 px and three
  // times that at 1024. The search still reads EVERY row; what is shown is the
  // first `limit` that match, and "Show more" adds a page. A row past the
  // limit is `hidden`, never removed, exactly as a row the search excludes.
  let limit = SELECTOR_PAGE_SIZE;
  const filter = () => {
    if (!active(lifecycle)) {
      return;
    }
    state.query = String(search.value);
    let matched = 0;
    let shown = 0;
    for (const row of rows) {
      const matches = matchesQuery(row.getAttribute("data-search"), state.query);
      if (matches) {
        matched += 1;
      }
      const visible = matches && matched <= limit;
      row.hidden = !visible;
      if (visible) {
        shown += 1;
      }
    }
    if (empty !== null) {
      empty.hidden = matched !== 0 || rows.length === 0;
    }
    if (moreBar !== null) {
      moreBar.hidden = matched <= SELECTOR_PAGE_SIZE;
    }
    if (count !== null) {
      count.textContent = "Showing " + String(shown) + " of " + String(matched) +
        (state.query.trim().length > 0 ? " matching" : "") + " recovery points.";
    }
    if (more !== null) {
      more.disabled = shown >= matched;
    }
  };
  const typed = () => {
    limit = SELECTOR_PAGE_SIZE;
    filter();
  };
  listen(search, "input", typed, lifecycle);
  listen(search, "change", typed, lifecycle);
  if (more !== null) {
    listen(more, "click", () => {
      limit += SELECTOR_PAGE_SIZE;
      filter();
    }, lifecycle);
  }
  filter();
}

/** How many matching recovery points the selector shows at once, and adds per
 *  "Show more" (MCP-26). Clarity's datagrid default page. */
export const SELECTOR_PAGE_SIZE = 20;

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
