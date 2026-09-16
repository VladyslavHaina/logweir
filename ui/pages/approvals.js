// pages/approvals.js -- the Approval list, the Restores still waiting for one,
// and the create form that records an approval for ONE chosen Restore from two
// documents an approver's own machine produced.
//
// THERE IS NO "APPROVE" BUTTON HERE, AND THERE MUST NOT BE. This page renders
// nothing that produces a signature. The approver runs `logweir drill approve`
// where their private key lives, and this form takes the two files it wrote --
// `approval.json` and `approval.sig` -- as UTF-8 text, VERBATIM, and does one
// `create` on `approvals` with them (interface I18: the document text, never
// base64). An encoding step between the approver's file and the hashed bytes
// is the class of transformation `planBytes` exists to forbid.
//
// IT REFUSES KEY MATERIAL, BY NAME AND BY CONTENT, AND SAYS WHICH. A file
// named the way a key file is named -- ending `.pem`, `.key`, `.p8`, `.p12`,
// `.pfx`, `.jks` or `.ppk`, or beginning `id_` -- and any text spelling the
// words that open a private-key PEM, in any case and across any spacing, is
// refused before anything is sent, with one message and no `create` at all;
// the refused text is cleared from the field and never kept in the draft.
// WHAT THAT DOES NOT COVER is a renamed, headerless blob, which spells nothing
// and is named nothing: the controller refuses it as a document that is not a
// DSSE envelope. v0.1 has no key lifecycle, and a page that "helpfully" filled
// that gap would be inventing the most consequential missing subsystem in the
// product inside a browser (Global Constraint 28).
//
// THE SUBJECT IS READ FROM THE CLUSTER, NOT FROM THE ROUTE (PLAT-12.2).
// `Approval.spec` is `{subjectRef{kind,name}, planHash, approvalBytes,
// sidecarBytes}`, and this page is forbidden from parsing the two documents,
// so the subject and the plan hash cannot come from them. They used to come
// from the route, beside an EDITABLE subject field the form then ignored: the
// page could show one subject and submit another. Now the route only names
// which Restore; the page reads that Restore and derives every value it will
// submit from the object itself -- the name, the UID, `spec.approvalRef.name`
// and the sha256 of `spec.planBytes`, computed here -- shows them read-only,
// and at submission checks that what is SHOWN is what it would SEND and that
// the Restore is still the one it read. A route that names a different hash or
// approval name than that Restore carries is a mismatch, and no form is
// offered from it. The controller recomputes the hash from the referent's own
// bytes regardless (Task 16, check 7), and the Restore controller refuses an
// approval bound to any other subject identity.
//
// AN APPROVAL THAT EXISTS IS NOT AN APPROVAL. One whose status the controller
// set to `Verified=True` is. A submission that does not verify stays in the
// cluster with `Verified=False` and its reason, which is the audit trail of a
// rejected attempt and not a failure of this page -- and this page never offers
// to reuse an Approval bound to another subject or another execution.

import { create, get, list } from "../api.js";
import {
  active,
  cancelled,
  carriesKeyMaterial,
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
  PRIVATE_KEY_REFUSAL,
  SELF_ATTESTED_FALSE,
  SELF_ATTESTED_TRUE,
  UNVERIFIED,
  badge,
  cell,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  listFooter,
  mutationStatus,
  phaseBadge,
  replace,
  table,
} from "../render.js";
import { planHash } from "../plan.js";
import { itemsOf } from "./clusters.js";

const PLURAL = "approvals";
const RESTORES = "restores";

/** A route with no namespace has no implicit authority. `app.js` asks the
 *  viewer to select one before it mounts this page. */
const DEFAULT_NAMESPACE = "";

/** The one kind this form may name. `Switchover` is tag 2 and is not in the
 *  CRD's enum: the subject kind is part of the bytes an approval binds, and
 *  without it an approved restore would double as "a valid signature by a
 *  rostered key exists in this namespace". */
export const SUBJECT_KIND = "Restore";

/** The form's identity in the draft and mutation registries; each subject has
 *  its own record. */
export const APPROVAL_FORM = "approval-form";

/** The two fields a draft of this form keeps: the two documents, which are
 *  signed public statements and not secrets. Private-key text is never kept,
 *  whichever field it was pasted into. */
export const APPROVAL_DRAFT_FIELDS = Object.freeze(["approvalBytes", "sidecarBytes"]);

/** The API server's field paths, mapped to the form's two inputs. */
export const APPROVAL_FIELD_PATHS = Object.freeze([
  ["spec.approvalBytes", "approvalBytes"],
  ["spec.sidecarBytes", "sidecarBytes"],
]);

/** The values the wizard or a link hands this page, read out of the hash:
 *  `#/approvals?subject=<restore>&hash=<planHash>&name=<approval>` plus the
 *  optional `ns`.
 *
 *  WHAT THEY ARE NOW. `subject` says WHICH Restore this visit is about, and
 *  nothing more. `hash` and `name` are what the link's author believed that
 *  Restore carries; the page reads the Restore and refuses the link when they
 *  disagree (see `routeMismatches`), and never submits either of them.
 *
 *  WHY THE EXTRACTION LIVES HERE AND NOT IN THE ROUTER. `app.js` touches
 *  `window` at module scope and cannot be imported under `node --test` (it
 *  fails with `ReferenceError: window is not defined`), so an extraction
 *  written inline in the router was unreachable by construction. Swapping
 *  `subject` and `name` there left the whole suite green. A pure function of
 *  the hash string can be called from a test, so this is one, and `app.js`
 *  calls it.
 *
 *  An absent parameter is the empty string and never `undefined`, so a blanked
 *  value is visible to an assertion rather than merely falsy. */
export function approvalRouteParams(hash) {
  const text = typeof hash === "string" ? hash : "";
  const route = { ns: DEFAULT_NAMESPACE, subject: "", hash: "", name: "" };
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
    if (key === "subject") {
      route.subject = value;
    } else if (key === "hash") {
      route.hash = value;
    } else if (key === "name") {
      route.name = value;
    } else if (key === "ns" && value.length > 0) {
      route.ns = value;
    }
  }
  return route;
}

/** The link to one Restore's approval page. `hash` and `name`, when given, are
 *  the identity the linking page reviewed; this page checks them against the
 *  Restore it reads. */
export function approvalSubjectRoute(ns, subject, hash, name) {
  const n = typeof ns === "string" ? ns.trim() : "";
  return (
    "#/approvals?subject=" + encodeURIComponent(subject) +
    (typeof hash === "string" && hash.length > 0 ? "&hash=" + encodeURIComponent(hash) : "") +
    (typeof name === "string" && name.length > 0 ? "&name=" + encodeURIComponent(name) : "") +
    (n.length > 0 ? "&ns=" + encodeURIComponent(n) : "")
  );
}

/** The durable operation view of one Restore: its detail page under History. */
export function restoreOperationRoute(ns, name) {
  return "#/history?ns=" + encodeURIComponent(ns) + "&name=" + encodeURIComponent(name);
}

const API = { create: create, get: get, list: list };

// The two shapes a private key arrives in. Names first, because a file input
// gives a name before anything is read; content second, because a paste has no
// name at all.
//
// THE NAME RULE IS THE HALF THAT CATCHES A KEY THAT SPELLS NOTHING. A DER or
// PKCS#12 blob and a header-stripped base64 body carry no words at all, so the
// content rule (`carriesKeyMaterial`) cannot see them; what they do carry is a
// conventional file name. These are the names OpenSSL, OpenSSH, Java and PuTTY
// write, and an approval document has never been called any of them.
const KEY_SUFFIXES = [".pem", ".key", ".p8", ".p12", ".pfx", ".jks", ".ppk"];

/** The names `ssh-keygen` writes by default. Matched as a PREFIX of the file's
 *  own name, so `id_rsa`, `id_ed25519` and `id_ecdsa.pub` are all refused. */
const KEY_NAME_PREFIX = "id_";

/** The refusal, or `null`. It is a pure function of the file's name and its
 *  text, so it is checkable without a browser and runs before any write.
 *
 *  NEITHER HALF IS EXHAUSTIVE AND THE PROSE SAYS SO. Between them they catch
 *  every PEM shape in any case or spacing and every conventionally named key
 *  file; a renamed, headerless blob pasted into the textarea is not caught
 *  here, and is refused by the controller as a document that is not a DSSE
 *  envelope. This page's promise is the refusal it can keep. */
export function refuseKeyMaterial(fileName, text) {
  const name = typeof fileName === "string" ? fileName.toLowerCase() : "";
  const base = name.slice(name.lastIndexOf("/") + 1);
  for (const suffix of KEY_SUFFIXES) {
    if (name.length >= suffix.length && name.slice(-suffix.length) === suffix) {
      return PRIVATE_KEY_REFUSAL;
    }
  }
  if (base.indexOf(KEY_NAME_PREFIX) === 0) {
    return PRIVATE_KEY_REFUSAL;
  }
  if (carriesKeyMaterial(text)) {
    return PRIVATE_KEY_REFUSAL;
  }
  return null;
}

/** Which of the two documents carry key material, by field name. */
export function keyMaterialFields(documents) {
  const d = documents || {};
  const out = [];
  if (refuseKeyMaterial(d.approvalFileName, d.approvalBytes) !== null) {
    out.push("approvalBytes");
  }
  if (refuseKeyMaterial(d.sidecarFileName, d.sidecarBytes) !== null) {
    out.push("sidecarBytes");
  }
  return out;
}

/** The sentence the approvals table carries when the namespace holds none. */
export const NO_APPROVAL_SENTENCE =
  "No Approval in this namespace yet. Choose a Restore waiting for one above, and record it " +
  "there from the two files logweir drill approve wrote on the approver's own machine.";

/** The sentence a standalone visit shows when no Restore waits for approval. */
export const NO_AWAITING_SENTENCE =
  "No Restore in this namespace is waiting for an approval. The restore wizard's Create the " +
  "Restore creates one and brings you to its approval; an approval is only ever recorded for " +
  "a Restore chosen here or arrived at from it.";

/** Every `Approval` in the namespace. */
export function renderApprovalList(collection, now) {
  return (
    "<h2>Approvals</h2>" +
    "<p class=\"blurb\">An Approval that exists is not an approval; an Approval whose " +
    "status weirkeeper set to Verified=True is. One that did not verify stays here with " +
    "its reason, as the record of a rejected attempt.</p>" +
    approvalTable(collection, now) +
    listFooter()
  );
}

function approvalTable(collection, now) {
  const at = typeof now === "number" ? now : Date.now();
  const rows = itemsOf(collection).map((object) => {
    const meta = object.metadata || {};
    const spec = object.spec || {};
    const status = object.status || {};
    const subject = spec.subjectRef || {};
    return [
      cell(subject.kind ? subject.kind + "/" + String(subject.name) : subject.name),
      verifiedBadge(status),
      cell(status.approver),
      typeof status.matchedKeyId === "string" && status.matchedKeyId.length > 0
        ? "<code>" + esc(status.matchedKeyId) + "</code>"
        : cell(null),
      cell(ageOf(meta.creationTimestamp, at)),
    ];
  });
  return table(["SUBJECT", "VERIFIED", "APPROVER", "KEY-ID", "AGE"], rows, NO_APPROVAL_SENTENCE);
}

/** One approval's recorded status, with `selfAttestedRisk` rendered as a
 *  SENTENCE and never as a bare boolean.
 *
 *  `false` here means only that the two matched key ids differ. One operator
 *  holding both keys satisfies it, so a tick or the word `false` beside "self
 *  attested" would read as "two people signed off", which nothing established. */
export function renderApprovalStatus(object) {
  const meta = (object || {}).metadata || {};
  const status = (object || {}).status || {};
  const spec = (object || {}).spec || {};
  const subject = spec.subjectRef || {};
  const condition = verifiedCondition(status);
  return (
    "<section class=\"approval\"><div class=\"card-head\"><h3>" + cell(meta.name) + "</h3>" +
    verifiedBadge(status) + "</div>" +
    facts([
      ["subject kind", cell(subject.kind)],
      ["subject name", cell(subject.name)],
      ["plan hash", "<code>" + esc(spec.planHash) + "</code>"],
      ["verified subject uid", cell((status.verifiedSubjectRef || {}).uid)],
      ["Verified reason", cell((condition || {}).reason)],
      ["matched key id", cell(status.matchedKeyId)],
      ["approver", cell(status.approver)],
      ["ticket", cell(status.ticket)],
    ]) +
    "<p class=\"self-attested\">" + selfAttestedSentence(status) + "</p>" +
    "</section>"
  );
}

/** The sentence a `selfAttestedRisk` value gets. An unset field gets neither:
 *  a page that printed the `false` sentence over an approval the controller
 *  has not evaluated would be answering a question nobody asked it. */
export function selfAttestedSentence(status) {
  const value = (status || {}).selfAttestedRisk;
  if (value === true) {
    return SELF_ATTESTED_TRUE;
  }
  if (value === false) {
    return SELF_ATTESTED_FALSE;
  }
  return "self-attestation: not evaluated yet";
}

/** The list and every approval's status panel -- the page's reading half,
 *  whatever the route. */
export function renderApprovalsPage(collection, route, now) {
  const panels = itemsOf(collection).map(renderApprovalStatus).join("");
  return renderApprovalList(collection, now) + panels;
}

// ------------------------------------------------------- subject and state

/** The subject an approval for `restore` must name, derived from the object
 *  itself: `{ns, kind, name, uid, planHash, approvalName}`. `hash` is
 *  `planHash(restore.spec.planBytes)`, computed by the caller. */
export function subjectOf(restore, hash, ns) {
  const meta = (restore || {}).metadata || {};
  const spec = (restore || {}).spec || {};
  return {
    ns: typeof ns === "string" && ns.length > 0 ? ns : (typeof meta.namespace === "string" ? meta.namespace : ""),
    kind: SUBJECT_KIND,
    name: typeof meta.name === "string" ? meta.name : "",
    uid: typeof meta.uid === "string" ? meta.uid : "",
    planHash: typeof hash === "string" ? hash : "",
    approvalName: typeof ((spec.approvalRef || {}).name) === "string" ? spec.approvalRef.name : "",
  };
}

/** Where a route disagrees with the Restore it names: each entry says what the
 *  link claimed and what the Restore carries. Empty when they agree, or when
 *  the route claims nothing beyond the Restore's name. */
export function routeMismatches(route, subject) {
  const r = route || {};
  const s = subject || {};
  const out = [];
  if (typeof r.subject === "string" && r.subject.length > 0 && r.subject !== s.name) {
    out.push({ field: "subject name", claimed: r.subject, actual: s.name });
  }
  if (typeof r.hash === "string" && r.hash.length > 0 && r.hash !== s.planHash) {
    out.push({ field: "plan hash", claimed: r.hash, actual: s.planHash });
  }
  if (typeof r.name === "string" && r.name.length > 0 && r.name !== s.approvalName) {
    out.push({ field: "Approval name", claimed: r.name, actual: s.approvalName });
  }
  return out;
}

function verifiedCondition(status) {
  const conditions = ((status || {}).conditions);
  if (!Array.isArray(conditions)) {
    return null;
  }
  for (const condition of conditions) {
    if ((condition || {}).type === "Verified") {
      return condition;
    }
  }
  return null;
}

/** Where one Approval stands for one subject, read from its own status and
 *  never from a clock or a guess:
 *
 *    absent                 -- no Approval under the Restore's approvalRef;
 *    foreign-subject        -- it names another subject;
 *    foreign-execution      -- weirkeeper verified it for another object of
 *                              that name (another UID), and never rebinds it;
 *    plan-mismatch          -- its planHash is not this Restore's plan;
 *    verified               -- Verified=True, for exactly this UID;
 *    expired                -- refused with KeyIdExpired;
 *    refused                -- refused with any other reason;
 *    awaiting-verification  -- recorded, not yet decided.
 *
 *  The three that bind another subject, execution or plan are checked before
 *  the verdict, because a verdict about something else is not a verdict about
 *  this Restore. */
export function approvalState(approval, subject) {
  if (approval === null || approval === undefined) {
    return { state: "absent" };
  }
  const s = subject || {};
  const spec = approval.spec || {};
  const ref = spec.subjectRef || {};
  const status = approval.status || {};
  if (ref.kind !== SUBJECT_KIND || ref.name !== s.name) {
    return { state: "foreign-subject", detail: String(ref.kind) + "/" + String(ref.name) };
  }
  const bound = status.verifiedSubjectRef;
  if (bound !== null && typeof bound === "object" && (
    bound.uid !== s.uid || bound.name !== s.name || bound.kind !== SUBJECT_KIND ||
    (typeof s.ns === "string" && s.ns.length > 0 && bound.namespace !== s.ns)
  )) {
    return { state: "foreign-execution", detail: String(bound.uid) };
  }
  if (typeof s.planHash === "string" && s.planHash.length > 0 && spec.planHash !== s.planHash) {
    return { state: "plan-mismatch", detail: String(spec.planHash) };
  }
  const condition = verifiedCondition(status);
  if (status.verified === true && bound !== null && typeof bound === "object") {
    return { state: "verified", key: status.matchedKeyId };
  }
  if (condition !== null && condition.status === "False") {
    return {
      state: condition.reason === "KeyIdExpired" ? "expired" : "refused",
      reason: typeof condition.reason === "string" ? condition.reason : "",
      message: typeof condition.message === "string" ? condition.message : "",
    };
  }
  return { state: "awaiting-verification" };
}

/** True when `approval` authorises exactly `restore` today. */
export function approvalAuthorizes(approval, restore, hash) {
  return approvalState(approval, subjectOf(restore, hash)).state === "verified";
}

/** A badge and a sentence for an approval state. Every sentence is ours and
 *  every value in it is escaped. */
export function renderApprovalState(found, subject, approvalName) {
  const f = found || {};
  const name = esc(approvalName);
  switch (f.state) {
    case "absent":
      return stateBlock("warn", "awaiting approval",
        "No Approval named " + name + " exists yet. The Restore holds at phase Pending " +
        "(reason ApprovalNotVerified), creates no Job, and weirkeeper looks again every 30 s. " +
        "Record the approval below.");
    case "awaiting-verification":
      return stateBlock("info", "awaiting verification",
        "Approval " + name + " is recorded and weirkeeper has not verified it yet. The Restore " +
        "runs only once it is Verified=True for this Restore's UID.");
    case "verified":
      return stateBlock("green", "approved: verified by weirkeeper",
        "weirkeeper verified Approval " + name + " against key " + esc(f.key) + " for Restore " +
        esc((subject || {}).name) + " (uid " + esc((subject || {}).uid) + ").");
    case "expired":
      return stateBlock("danger", "expired",
        "weirkeeper refused Approval " + name + " because the approver key is past its " +
        "notAfter (reason KeyIdExpired): " + esc(f.message));
    case "refused":
      return stateBlock("danger", "refused by weirkeeper",
        "weirkeeper refused Approval " + name + " (reason " + esc(f.reason) + "): " +
        esc(f.message) + " The Restore does not run while this stands. weirkeeper re-evaluates " +
        "it every five minutes, so a refusal about the roster can clear once the roster is " +
        "fixed; a refusal about the documents cannot, because Approval.spec is immutable.");
    case "foreign-subject":
      return stateBlock("danger", "bound to another subject",
        "Approval " + name + " exists but names " + esc(f.detail) + ", not this Restore. It " +
        "cannot approve this Restore, and this page will not reuse it.");
    case "foreign-execution":
      return stateBlock("danger", "bound to another execution",
        "Approval " + name + " was verified for an earlier object with this name (uid " +
        esc(f.detail) + "), not for this Restore (uid " + esc((subject || {}).uid) + "). " +
        "weirkeeper never rebinds it and this page will not reuse it: this Restore needs a " +
        "new plan, and so a new Restore and a new Approval.");
    case "plan-mismatch":
      return stateBlock("danger", "names another plan",
        "Approval " + name + " names plan hash " + esc(f.detail) + ", but this Restore's plan " +
        "hashes to " + esc((subject || {}).planHash) + ". It does not describe this plan, and " +
        "this page will not reuse it.");
    default:
      return "";
  }
}

function stateBlock(kind, words, sentence) {
  return (
    "<div class=\"approval-state\">" + badge(kind, words) +
    "<p class=\"note\">" + sentence + "</p></div>"
  );
}

/** A Restore's own progress, as weirkeeper recorded it: the phase and reason,
 *  in words. */
export function restoreProgressSentence(restore) {
  const status = (restore || {}).status || {};
  if (typeof status.phase !== "string" || status.phase.length === 0) {
    return "weirkeeper has not reconciled this Restore yet.";
  }
  if (status.phase === "Pending" && status.reason === "ApprovalNotVerified") {
    return "Pending: no Job exists, and none will until its approval is Verified=True.";
  }
  if (status.phase === "Failed") {
    return "Failed, reason " + esc(status.reason) + ". Restore.spec is immutable; a refused " +
      "Restore is never retried under this name.";
  }
  return esc(status.phase) + (typeof status.reason === "string" && status.reason.length > 0
    ? ", reason " + esc(status.reason) + "."
    : ".");
}

/** The Restores that still wait for an approval: nothing has run and nothing
 *  refused them. */
export function awaitingRestores(restores) {
  return itemsOf(restores).filter((restore) => {
    const phase = ((restore || {}).status || {}).phase;
    return phase === undefined || phase === null || phase === "" || phase === "Pending";
  });
}

/** A standalone visit: the Restores waiting for an approval, each a link to
 *  its own approval page, or a sentence saying there are none -- and the
 *  recorded approvals. No form: an approval is recorded for a chosen Restore. */
export function renderApprovalsIndex(ns, approvals, restores, now, restoresError, approvalsError) {
  // A MAP, NOT AN OBJECT. The key is a Kubernetes name, and `constructor`,
  // `toString` and `__proto__` are all valid DNS-1123 subdomains: a plain
  // `{}` would answer such a lookup out of `Object.prototype` and this table
  // would then state something about the cluster that is not there. A `Map`
  // has no inherited entries, so a name nobody created reads `undefined`.
  const byName = new Map();
  for (const approval of itemsOf(approvals)) {
    byName.set(((approval || {}).metadata || {}).name, approval);
  }
  const rows = awaitingRestores(restores).map((restore) => {
    const meta = restore.metadata || {};
    const approvalName = (((restore.spec || {}).approvalRef) || {}).name;
    const found = approvalState(byName.get(approvalName) || null, {
      ns: ns, kind: SUBJECT_KIND, name: meta.name, uid: meta.uid, planHash: "",
    });
    return [
      "<a href=\"" + esc(approvalSubjectRoute(ns, meta.name)) + "\">" + esc(meta.name) + "</a>",
      phaseBadge(((restore.status || {}).phase)) === "-" ? cell("not reconciled") : phaseBadge(restore.status.phase),
      cell(approvalName),
      // An unreadable approvals list is not an absent approval. Saying "none
      // recorded" here would be a claim about the cluster this page has no
      // grounds for, so the column says what is true: it could not be read.
      cell(approvalsError ? "unknown -- not readable" : STATE_WORDS[found.state]),
      cell(meta.creationTimestamp),
    ];
  });
  const listing = restoresError
    ? "<p class=\"note\">The Restores in this namespace could not be listed, so none can be " +
      "chosen here:</p>" + errorLine(restoresError)
    : table(["RESTORE", "PHASE", "APPROVAL", "APPROVAL STATE", "CREATED"], rows, NO_AWAITING_SENTENCE);
  // A LIST THIS VIEWER MAY NOT READ IS A WARNING BESIDE THE PAGE, NOT INSTEAD
  // OF IT. An approver whose role grants `create` on approvals but not `list`
  // still has to reach a Restore's own approval page, and that page does its
  // own `get`. The same rule as `history.js`: an unreadable Approval leaves
  // the rest of the view standing.
  const approvalsWarning = approvalsError
    ? "<p class=\"note\">The Approvals in this namespace could not be listed, so this page " +
      "cannot say which Restores already have one. Every Restore below is still selectable, " +
      "and its own page reads its Approval directly:</p>" + errorLine(approvalsError)
    : "";
  const panels = approvalsError ? "" : itemsOf(approvals).map(renderApprovalStatus).join("");
  return (
    "<h2>Approvals</h2>" +
    "<p class=\"blurb\">An Approval that exists is not an approval; an Approval whose " +
    "status weirkeeper set to Verified=True is. One that did not verify stays here with " +
    "its reason, as the record of a rejected attempt.</p>" +
    "<section class=\"step\" id=\"awaiting-approval\"><h3>Restores waiting for an approval</h3>" +
    "<p class=\"note\">Choose one to see its plan hash and record its approval.</p>" +
    approvalsWarning + listing + "</section>" +
    "<h3>Recorded approvals</h3>" +
    (approvalsError
      ? "<p class=\"note\">Not listed here: this viewer may not list Approvals in " + esc(ns) +
        " (see above). Nothing is claimed about which exist.</p>"
      : approvalTable(approvals, now) + listFooter() + panels)
  );
}

// A LOOKUP TABLE WITH NO PROTOTYPE, read by a state name. The eight keys are
// this module's own closed set, and a table read by a name is never a plain
// `{}` here: the rule holds even where today's key cannot come from outside.
const STATE_WORDS = Object.freeze(Object.assign(Object.create(null), {
  "absent": "none recorded",
  "awaiting-verification": "awaiting verification",
  "verified": "verified",
  "expired": "expired",
  "refused": "refused",
  "foreign-subject": "bound to another subject",
  "foreign-execution": "bound to another execution",
  "plan-mismatch": "names another plan",
}));

function errorLine(error) {
  const e = error || {};
  return (
    "<p class=\"complaint\">" + esc(e.status ? String(e.status) + " " + String(e.reason || "") : "error") +
    ": " + esc(e.message) + "</p>"
  );
}

/** Whether a form may be offered for this subject: the Restore exists and has
 *  neither run nor been refused, its approval name is free, and the route
 *  agrees with it.
 *
 *  AN APPROVAL THIS VIEWER MAY NOT READ IS NOT A REFUSAL TO OFFER THE FORM.
 *  An approver-only role -- `create` on approvals, no `get` -- is the reason
 *  this page exists, and withholding the form from it would make PLAT-12.2's
 *  "a standalone visit is usable" false for exactly the person the visit is
 *  for. Offering it concedes nothing: the subject still comes from the
 *  Restore, the create still names the one Approval `spec.approvalRef` names,
 *  and `createOnce` answers an existing object with the same content as that
 *  object and different content as a conflict it never overwrites. The page
 *  says the state is unknown rather than implying it is absent. */
export function formOffered(view) {
  const v = view || {};
  if (v.restore === null || v.restore === undefined) {
    return false;
  }
  if (Array.isArray(v.mismatches) && v.mismatches.length > 0) {
    return false;
  }
  if (typeof ((v.subject || {}).approvalName) !== "string" || v.subject.approvalName.length === 0) {
    return false;
  }
  const phase = ((v.restore.status || {}).phase);
  if (phase !== undefined && phase !== null && phase !== "" && phase !== "Pending") {
    return false;
  }
  if (v.approvalError) {
    return true;
  }
  return ((v.found || {}).state) === "absent";
}

/** One Restore's approval page. `view` is `{ns, route, restore, subject,
 *  approval, found, mismatches, state, errors}`; `restore` is `null` when the
 *  named Restore does not exist. */
export function renderApprovalSubject(view, now) {
  const v = view || {};
  const route = v.route || {};
  const ns = typeof v.ns === "string" ? v.ns : "";
  const back = "<p class=\"note\"><a href=\"" + esc("#/approvals?ns=" + encodeURIComponent(ns)) +
    "\">All approvals and Restores waiting for one</a></p>";
  if (v.restore === null || v.restore === undefined) {
    return (
      "<h2>Approval</h2>" +
      "<div class=\"empty-state\"><p class=\"note\">No Restore named " + esc(route.subject) +
      " exists in namespace " + esc(ns) + ", so there is nothing to approve and no form is " +
      "offered. An approval is recorded for a Restore that exists.</p></div>" + back
    );
  }
  const s = v.subject || {};
  const found = v.found || { state: "absent" };
  const mismatches = Array.isArray(v.mismatches) ? v.mismatches : [];
  // The same sentence `history.js` renders for the same condition: the
  // Approval's state is UNKNOWN. The subject facts above it were read from the
  // Restore and are unaffected.
  const stateBlockOrWarning = v.approvalError
    ? "<div class=\"approval-state\">" + badge("warn", "state unknown") +
      "<p class=\"note\">Approval " + esc(s.approvalName) + " could not be read, so whether one " +
      "exists for this Restore -- and whether weirkeeper verified it -- is unknown from here. " +
      "Recording one below is still safe: an Approval with exactly this content is recognised " +
      "rather than duplicated, and one with different content is reported as a conflict and " +
      "never overwritten.</p>" + errorLine(v.approvalError) + "</div>"
    : renderApprovalState(found, s, s.approvalName);
  const heading = v.approvalError
    ? "Approval for Restore "
    : (found.state === "verified" ? "Approved: Restore " : "Awaiting approval: Restore ");
  const mismatchBlock = mismatches.length === 0
    ? ""
    : "<div class=\"refusal-block\" role=\"alert\"><p class=\"refusal\">This link does not match " +
      "Restore " + esc(s.name) + ", so nothing will be submitted from it.</p><ul class=\"sets\">" +
      mismatches.map((m) => "<li>" + esc(m.field) + ": the link says <code>" + esc(m.claimed) +
        "</code>; the Restore carries <code>" + esc(m.actual) + "</code></li>").join("") +
      "</ul><p class=\"note\">Open the Restore's own approval page instead: <a href=\"" +
      esc(approvalSubjectRoute(ns, s.name)) + "\">" + esc(s.name) + "</a>.</p></div>";
  const existing = v.approval ? renderApprovalStatus(v.approval) : "";
  return (
    "<h2>" + heading + esc(s.name) + "</h2>" +
    "<p class=\"blurb\">Every value below was read from Restore " + esc(s.name) + " itself: its " +
    "UID, the Approval name its spec.approvalRef names, and the sha256 of its spec.planBytes, " +
    "computed on this page. They are exactly what an Approval for it carries, and they are not " +
    "editable.</p>" +
    mismatchBlock +
    "<section class=\"approval-subject\"><h3>Subject</h3>" +
    facts([
      ["subject kind", esc(s.kind)],
      ["subject name", "<code>" + esc(s.name) + "</code>"],
      ["subject uid", "<code>" + esc(s.uid) + "</code>"],
      ["namespace", esc(ns)],
      ["plan hash", "<code>" + esc(s.planHash) + "</code>"],
      ["Approval metadata.name", "<code>" + esc(s.approvalName) + "</code>"],
      ["Restore phase", phaseBadge(((v.restore.status || {}).phase))],
      ["progress", restoreProgressSentence(v.restore)],
    ]) +
    stateBlockOrWarning +
    "<p class=\"note\"><a href=\"" + esc(restoreOperationRoute(ns, s.name)) + "\">Open the " +
    "Restore's operation view</a></p>" +
    "</section>" +
    existing +
    (formOffered(v) ? renderApprovalForm(s, v) : "") +
    back
  );
}

/** The create form: the subject, read-only, and the two documents.
 *
 *  THE SUBJECT IS SHOWN, NOT EDITED. Kind, name, UID, plan hash and Approval
 *  name are read-only inputs holding exactly the values `approvalBody` will
 *  send; submission reads them back and refuses if any was changed. The two
 *  documents' text is placed into the textareas by the mount half, through the
 *  DOM, so no byte of either passes through markup on its way back into the
 *  field. */
export function renderApprovalForm(subject, view) {
  const s = subject || {};
  const v = view || {};
  const errors = ((v.errors || {}).fields) || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  return (
    "<section class=\"step\" id=\"approval-form-section\"><h3>Record the approval</h3>" +
    "<p class=\"blurb\">Paste or upload the two files `logweir drill approve` wrote over this " +
    "Restore's plan. They are submitted as text, exactly as they arrived: the controller hashes " +
    "the bytes the approver signed, so anything this page did to them in between would be a " +
    "different document.</p>" +
    "<form id=\"approval-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"subject-kind\">SUBJECT KIND</label>" +
    "<input id=\"subject-kind\" name=\"subjectKind\" readonly value=\"" + esc(s.kind) + "\">" +
    "<p class=\"help\">The one kind an approval may name in this release.</p></div>" +
    "<div class=\"field\"><label for=\"subject-name\">SUBJECT NAME</label>" +
    "<input id=\"subject-name\" name=\"subjectName\" readonly value=\"" + esc(s.name) + "\">" +
    "<p class=\"help\">The Restore this approval covers, as read from the cluster.</p></div>" +
    "</div>" +
    "<div class=\"field\"><label for=\"subject-uid\">SUBJECT UID</label>" +
    "<input id=\"subject-uid\" name=\"subjectUid\" readonly value=\"" + esc(s.uid) + "\">" +
    "<p class=\"help\">weirkeeper binds a verified approval to this UID and never to a " +
    "recreated object of the same name.</p></div>" +
    "<div class=\"field\"><label for=\"plan-hash\">PLAN HASH</label>" +
    "<input id=\"plan-hash\" name=\"planHash\" readonly value=\"" + esc(s.planHash) + "\">" +
    "<p class=\"help\">Read-only: the sha256 of this Restore's own spec.planBytes, computed on " +
    "this page. It must equal the plan_hash logweir drill approve printed; the controller " +
    "recomputes it from the same bytes.</p></div>" +
    "<div class=\"field\"><label for=\"approval-name\">APPROVAL NAME</label>" +
    "<input id=\"approval-name\" name=\"approvalName\" readonly value=\"" + esc(s.approvalName) + "\">" +
    "<p class=\"help\">metadata.name, as this Restore's spec.approvalRef names it. Neither name " +
    "is ever edited: both specs are immutable.</p></div>" +
    "<div class=\"field\"><label for=\"approval-json\">approval.json</label>" +
    "<input type=\"file\" id=\"approval-json-file\" name=\"approvalFile\">" +
    "<textarea id=\"approval-json\" name=\"approvalBytes\" rows=\"8\" autocomplete=\"off\" " +
    "spellcheck=\"false\"" + invalidAttributes("approval-json", errors.approvalBytes) + "></textarea>" +
    "<p class=\"help\">Choose the file, or paste its text. It is sent exactly as it is here.</p>" +
    fieldErrorLine("approval-json", errors.approvalBytes) + "</div>" +
    "<div class=\"field\"><label for=\"approval-sig\">approval.sig</label>" +
    "<input type=\"file\" id=\"approval-sig-file\" name=\"sidecarFile\">" +
    "<textarea id=\"approval-sig\" name=\"sidecarBytes\" rows=\"8\" autocomplete=\"off\" " +
    "spellcheck=\"false\"" + invalidAttributes("approval-sig", errors.sidecarBytes) + "></textarea>" +
    "<p class=\"help\">The signature sidecar the same command wrote beside it.</p>" +
    fieldErrorLine("approval-sig", errors.sidecarBytes) + "</div>" +
    "<p class=\"refusal-rule\">" + PRIVATE_KEY_REFUSAL + ". A file named the way a key file " +
    "is named, or any text spelling the words that open a private-key PEM, is refused here, " +
    "cleared from the field, and nothing is sent.</p>" +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Create the Approval</button></div>" +
    "</fieldset>" +
    "<div class=\"form-status\" id=\"approval-form-status\" tabindex=\"-1\">" +
    mutationStatus(state, { kind: "Approval", name: s.approvalName }, ((v.errors || {}).unmatched)) +
    "</div>" +
    "</form>" +
    "<p class=\"note\">The two documents are kept in this page's memory until the Approval " +
    "exists -- through an error, a lost response or a visit to another page -- and never written " +
    "to browser storage; a reload starts empty.</p>" +
    "</section>"
  );
}

// SUBMIT-REGION-BEGIN

/** The object `create` posts: all four spec fields, every one of them from the
 *  SUBJECT derived from the Restore and none from the route.
 *
 *  `approvalBytes` and `sidecarBytes` are the strings as they arrived --
 *  interface I18, UTF-8 document text, never base64, never parsed. */
export function approvalBody(subject, documents) {
  const s = subject || {};
  const d = documents || {};
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Approval",
    metadata: { name: s.approvalName },
    spec: {
      subjectRef: { kind: SUBJECT_KIND, name: s.name },
      planHash: s.planHash,
      approvalBytes: d.approvalBytes,
      sidecarBytes: d.sidecarBytes,
    },
  };
}

const SUBJECT_FIELDS = Object.freeze(["kind", "name", "uid", "planHash", "approvalName"]);

/** Records the approval for `subject`, or refuses.
 *
 *  In order, and nothing is sent until every step passes:
 *   1. what the page SHOWS (`options.displayed`, read back from the read-only
 *      inputs) must be exactly the subject it would SEND -- an edited or forged
 *      field is a refusal;
 *   2. neither document may carry key material;
 *   3. both documents must be present;
 *   4. the Restore must still be the object the page read: the same UID, the
 *      same approvalRef and the same plan hash, re-read and re-hashed now;
 *   5. then one idempotent create: a retry after a lost response resolves to
 *      the Approval the first request made, and a different Approval under the
 *      name is a conflict.
 *
 *  Throws a `refused` error for 1, 2 and 4, an `invalid` one for 3. Returns
 *  `{outcome, object}`. */
export async function submitApproval(subject, documents, deps, options) {
  const s = subject || {};
  const d = documents || {};
  const displayed = (options || {}).displayed;
  if (displayed !== null && typeof displayed === "object") {
    for (const field of SUBJECT_FIELDS) {
      if (displayed[field] !== undefined && displayed[field] !== s[field]) {
        throw refusal(
          "the approval subject shown on this page (" + field + " " + String(displayed[field]) +
            ") is not the subject it would submit (" + String(s[field]) + "); reload the " +
            "Restore's approval page",
          { mismatch: field },
        );
      }
    }
  }
  const keyed = refuseKeyMaterial(d.approvalFileName, d.approvalBytes) ||
    refuseKeyMaterial(d.sidecarFileName, d.sidecarBytes);
  if (keyed !== null) {
    throw refusal(keyed, { fields: keyMaterialFields(d) });
  }
  const missing = {};
  if (typeof d.approvalBytes !== "string" || d.approvalBytes.length === 0) {
    missing.approvalBytes = "choose approval.json or paste its text";
  }
  if (typeof d.sidecarBytes !== "string" || d.sidecarBytes.length === 0) {
    missing.sidecarBytes = "choose approval.sig or paste its text";
  }
  if (Object.keys(missing).length > 0) {
    throw invalidInput(missing);
  }
  const api = deps || API;
  let current;
  try {
    current = await api.get(s.ns, RESTORES, s.name);
  } catch (error) {
    if (error !== null && typeof error === "object" && error.status === 404) {
      throw refusal("Restore " + String(s.name) + " no longer exists in this namespace");
    }
    throw error;
  }
  const now = subjectOf(current, await planHash((((current || {}).spec) || {}).planBytes), s.ns);
  for (const field of SUBJECT_FIELDS) {
    if (now[field] !== s[field]) {
      throw refusal(
        "Restore " + String(s.name) + " changed since this page read it (" + field + " was " +
          String(s[field]) + ", is now " + String(now[field]) + "); reload its approval page",
        { mismatch: field },
      );
    }
  }
  const body = approvalBody(s, d);
  try {
    return { outcome: "created", object: await api.create(s.ns, PLURAL, body) };
  } catch (error) {
    return resolveExisting(api, s.ns, PLURAL, body, null, error);
  }
}

// SUBMIT-REGION-END

// --------------------------------------------------------------- private half

/** The verified badge. It reads `status.verified` and nothing else: this page
 *  verified nothing, and neither did the browser. */
function verifiedBadge(status) {
  const verified = (status || {}).verified;
  if (verified === true) {
    return badge("green", "verified by weirkeeper against key " + String((status || {}).matchedKeyId));
  }
  return badge("unverified", UNVERIFIED);
}

function ageOf(creationTimestamp, now) {
  if (typeof creationTimestamp !== "string" || creationTimestamp.length === 0) {
    return null;
  }
  const at = Date.parse(creationTimestamp);
  if (isNaN(at)) {
    return null;
  }
  const seconds = Math.max(0, Math.floor((now - at) / 1000));
  if (seconds < 120) {
    return String(seconds) + "s";
  }
  if (seconds < 7200) {
    return String(Math.floor(seconds / 60)) + "m";
  }
  if (seconds < 172800) {
    return String(Math.floor(seconds / 3600)) + "h";
  }
  return String(Math.floor(seconds / 86400)) + "d";
}

// --------------------------------------------------------------- mount half

/** Reads what one subject's page needs: the Restore, its Approval and the
 *  hash of its plan. A missing Restore or Approval is a state, not an error. */
export async function loadApprovalSubject(api, ns, route, lifecycle) {
  const r = route || {};
  let restore = null;
  try {
    restore = await api.get(ns, RESTORES, r.subject, readOptions(lifecycle));
  } catch (error) {
    if (!(error !== null && typeof error === "object" && error.status === 404)) {
      throw error;
    }
  }
  if (restore === null) {
    return {
      ns: ns, route: r, restore: null, subject: null, approval: null, found: null,
      approvalError: null, mismatches: [],
    };
  }
  const hash = await planHash((((restore || {}).spec) || {}).planBytes || "");
  const subject = subjectOf(restore, hash, ns);
  let approval = null;
  let approvalError = null;
  if (subject.approvalName.length > 0) {
    try {
      approval = await api.get(ns, PLURAL, subject.approvalName, readOptions(lifecycle));
    } catch (error) {
      if (cancelled(error, lifecycle)) {
        throw error;
      }
      // NOT READ IS NOT ABSENT, and it is not a reason to take the page away
      // either: the same rule `history.js:loadRestoreOperation` already
      // follows. A 404 IS absent -- the API server answered.
      if (!(error !== null && typeof error === "object" && error.status === 404)) {
        approvalError = error;
      }
    }
  }
  return {
    ns: ns,
    route: r,
    restore: restore,
    subject: subject,
    approval: approval,
    found: approvalError === null ? approvalState(approval, subject) : null,
    approvalError: approvalError,
    mismatches: routeMismatches(r, subject),
  };
}

/** What the subject page's form renders from: the record and its messages. */
export function approvalFormView(view) {
  const v = view || {};
  const key = formKey(v.ns, APPROVAL_FORM, ((v.subject || {}).name) || "");
  const state = mutationFor(key).state;
  return Object.assign({}, v, {
    state: state,
    errors: state.phase === "failed" ? fieldErrors(state.error, APPROVAL_FIELD_PATHS) : null,
  });
}

export async function mountApprovals(node, ns, route, parse, deps, lifecycle) {
  const api = deps || API;
  const r = route || {};
  try {
    if (typeof r.subject !== "string" || r.subject.length === 0) {
      // NEITHER LIST TAKES THE PAGE AWAY WHEN IT FAILS. Both are caught to a
      // state and rendered beside the other one, so a viewer who may read only
      // one of the two kinds -- an approver with `create` on approvals and no
      // `list`, the commonest shape of this role -- still gets a usable page.
      const lists = await Promise.all([
        listOrError(api, ns, PLURAL, lifecycle),
        listOrError(api, ns, RESTORES, lifecycle),
      ]);
      if (!active(lifecycle)) {
        return;
      }
      replace(node, parse(renderApprovalsIndex(
        ns, lists[0].collection, lists[1].collection, undefined, lists[1].error, lists[0].error,
      )));
      return;
    }
    const loaded = await loadApprovalSubject(api, ns, r, lifecycle);
    if (!active(lifecycle)) {
      return;
    }
    const view = approvalFormView(loaded);
    replace(node, parse(renderApprovalSubject(view)));
    if (formOffered(view)) {
      wire(node, view, parse, api, lifecycle);
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** One list read as a state: `{collection, error}`. A cancelled read is still
 *  a cancellation and is re-thrown, so a left route renders nothing. */
async function listOrError(api, ns, plural, lifecycle) {
  try {
    return { collection: await api.list(ns, plural, readOptions(lifecycle)), error: null };
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      throw error;
    }
    return { collection: null, error: error };
  }
}

/** The documents as the form holds them. */
export function readDocuments(form) {
  return {
    approvalBytes: String(form.elements.approvalBytes.value),
    sidecarBytes: String(form.elements.sidecarBytes.value),
    approvalFileName: fileNameOf(form.elements.approvalFile),
    sidecarFileName: fileNameOf(form.elements.sidecarFile),
  };
}

/** The subject as the form SHOWS it, read back from the read-only inputs. */
export function readDisplayedSubject(form) {
  const e = form.elements;
  return {
    kind: String(e.subjectKind.value),
    name: String(e.subjectName.value),
    uid: String(e.subjectUid.value),
    planHash: String(e.planHash.value),
    approvalName: String(e.approvalName.value),
  };
}

const AREAS = Object.freeze({ approvalBytes: "#approval-json", sidecarBytes: "#approval-sig" });
const FILES = Object.freeze({ approvalBytes: "#approval-json-file", sidecarBytes: "#approval-sig-file" });

function wire(node, view, parse, api, lifecycle) {
  const form = node.querySelector("#approval-form");
  if (form === null) {
    return;
  }
  const subject = view.subject;
  const key = formKey(view.ns, APPROVAL_FORM, subject.name);
  const mutation = mutationFor(key);

  // The draft's text goes back into the fields THROUGH THE DOM, never through
  // markup, so the bytes a retry sends are the bytes that were kept.
  const draft = readDraft(key) || {};
  for (const field of APPROVAL_DRAFT_FIELDS) {
    const area = node.querySelector(AREAS[field]);
    if (area !== null && typeof draft[field] === "string") {
      area.value = draft[field];
    }
  }

  const showStatus = (state) => {
    const slot = node.querySelector("#approval-form-status");
    if (slot !== null) {
      replace(slot, parse(mutationStatus(state, { kind: "Approval", name: subject.approvalName })));
    }
  };
  const clearKeyMaterial = (fields) => {
    for (const field of fields) {
      const area = node.querySelector(AREAS[field]);
      if (area !== null) {
        area.value = "";
      }
      const file = node.querySelector(FILES[field]);
      if (file !== null) {
        file.value = "";
      }
    }
    keepDraft(key, readDocuments(form), APPROVAL_DRAFT_FIELDS);
  };

  const remember = () => {
    if (active(lifecycle)) {
      keepDraft(key, readDocuments(form), APPROVAL_DRAFT_FIELDS);
    }
  };
  listen(form, "input", remember, lifecycle);

  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      dropDraft(key);
      mountApprovals(node, view.ns, view.route, parse, api, lifecycle);
      return;
    }
    if (state.phase === "failed" && state.kind === "refused" && Array.isArray((state.error || {}).fields)) {
      clearKeyMaterial(state.error.fields);
    }
    const section = node.querySelector("#approval-form-section");
    if (state.phase === "failed" && section !== null && state.kind === "invalid") {
      // Field messages need the form re-rendered; the text returns from the draft.
      mountApprovals(node, view.ns, view.route, parse, api, lifecycle);
      return;
    }
    const body = form.querySelector("fieldset");
    if (body !== null) {
      body.disabled = state.phase === "pending";
    }
    showStatus(state);
    if (state.phase === "failed") {
      const slot = node.querySelector("#approval-form-status");
      if (slot !== null && typeof slot.focus === "function") {
        slot.focus();
      }
    }
  }, lifecycle);

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const documents = readDocuments(form);
    const keyed = keyMaterialFields(documents);
    if (keyed.length > 0) {
      // Cleared BEFORE anything else happens, so the key text is gone from the
      // field and from the draft whatever the record does next.
      clearKeyMaterial(keyed);
      mutation.run(() => Promise.reject(refusal(PRIVATE_KEY_REFUSAL, { fields: keyed })));
      return;
    }
    keepDraft(key, documents, APPROVAL_DRAFT_FIELDS);
    const displayed = readDisplayedSubject(form);
    mutation.run(() => submitApproval(subject, documents, api, { displayed: displayed }));
  }, lifecycle);

  // A chosen file is READ AS TEXT into the textarea beside it, so the bytes
  // that are submitted are the bytes a viewer can see, and the refusal runs
  // over the same two values either way.
  for (const field of APPROVAL_DRAFT_FIELDS) {
    const input = node.querySelector(FILES[field]);
    const area = node.querySelector(AREAS[field]);
    if (input === null || area === null) {
      continue;
    }
    listen(input, "change", async () => {
      if (!active(lifecycle)) {
        return;
      }
      const chosen = input.files && input.files[0];
      if (!chosen) {
        return;
      }
      if (refuseKeyMaterial(chosen.name, "") !== null) {
        clearKeyMaterial([field]);
        showStatus({ phase: "failed", kind: "refused", error: refusal(PRIVATE_KEY_REFUSAL) });
        return;
      }
      const text = await chosen.text();
      if (!active(lifecycle)) {
        return;
      }
      if (refuseKeyMaterial("", text) !== null) {
        clearKeyMaterial([field]);
        showStatus({ phase: "failed", kind: "refused", error: refusal(PRIVATE_KEY_REFUSAL) });
        return;
      }
      area.value = text;
      remember();
    }, lifecycle);
  }
}

function fileNameOf(input) {
  const chosen = input && input.files && input.files[0];
  return chosen ? chosen.name : "";
}
