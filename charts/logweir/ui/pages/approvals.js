// pages/approvals.js -- the Approval list, and the create form that takes two
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
// IT REFUSES KEY MATERIAL, BY NAME AND BY CONTENT. A file whose name ends
// `.pem` or `.key`, or whose text carries the words that open a private-key
// PEM, is refused before anything is sent, with one message and no `create` at
// all. v0.1 has no key lifecycle, and a page that "helpfully" filled that gap
// would be inventing the most consequential missing subsystem in the product
// inside a browser (Global Constraint 28).
//
// FOUR SPEC FIELDS, NOT TWO. `Approval.spec` is
// `{subjectRef{kind,name}, planHash, approvalBytes, sidecarBytes}` and the
// other two cannot be derived here: this page is forbidden from parsing the
// documents, so the hash cannot be lifted out of `approval.json` either. Both
// come from the ROUTE the wizard navigated to, and the controller recomputes
// the hash from the referent's own bytes regardless (Task 16, check 5). A form
// that posted only the two documents posts an object the CRD schema rejects.
//
// AN APPROVAL THAT EXISTS IS NOT AN APPROVAL. One whose status the controller
// set to `Verified=True` is. A submission that does not verify stays in the
// cluster with `Verified=False` and its reason, which is the audit trail of a
// rejected attempt and not a failure of this page.

import { create, list } from "../api.js";
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
  listFooter,
  replace,
  table,
} from "../render.js";
import { itemsOf } from "./clusters.js";

const PLURAL = "approvals";

/** The namespace a hash that names none means. The same default `app.js`
 *  carries, because the two read the same hash. */
const DEFAULT_NAMESPACE = "default";

/** The one kind this form may name. `Switchover` is tag 2 and is not in the
 *  CRD's enum: the subject kind is part of the bytes an approval binds, and
 *  without it an approved restore would double as "a valid signature by a
 *  rostered key exists in this namespace". */
export const SUBJECT_KIND = "Restore";

/** The three values the wizard hands this page, read out of the hash it
 *  navigated to: `#/approvals?subject=<restore>&hash=<planHash>&name=<approval>`
 *  plus the optional `ns`.
 *
 *  WHY THE EXTRACTION LIVES HERE AND NOT IN THE ROUTER. These three are where
 *  `Approval.spec.subjectRef.name`, `spec.planHash` and `metadata.name` come
 *  from -- this page is forbidden from parsing the two approval documents, so
 *  none of them can be recovered from the bytes if the hop delivers them
 *  swapped or empty. It is therefore the most consequential hand-off in the
 *  application and it was, until this function existed, the only one no test in
 *  either language could reach: `app.js` touches `window` at module scope and
 *  cannot be imported under `node --test` (it fails with
 *  `ReferenceError: window is not defined`), so an extraction written inline in
 *  the router was unreachable by construction. Swapping `subject` and `name`
 *  there left the whole suite green. A pure function of the hash string can be
 *  called from a test, so this is one, and `app.js` calls it.
 *
 *  An absent parameter is the empty string and never `undefined`, so the form
 *  renders an empty control rather than the word "undefined", and a blanked
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

const API = { create: create, list: list };

// The two shapes a private key arrives in. Names first, because a file input
// gives a name before anything is read; content second, because a paste has no
// name at all.
const KEY_SUFFIXES = [".pem", ".key"];
const KEY_MARKER = "PRIVATE KEY";

/** The refusal, or `null`. It is a pure function of the file's name and its
 *  text, so it is checkable without a browser and runs before any write. */
export function refuseKeyMaterial(fileName, text) {
  const name = typeof fileName === "string" ? fileName.toLowerCase() : "";
  for (const suffix of KEY_SUFFIXES) {
    if (name.length >= suffix.length && name.slice(-suffix.length) === suffix) {
      return PRIVATE_KEY_REFUSAL;
    }
  }
  if (typeof text === "string" && text.indexOf(KEY_MARKER) !== -1) {
    return PRIVATE_KEY_REFUSAL;
  }
  return null;
}

/** Every `Approval` in the namespace. */
export function renderApprovalList(collection, now) {
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
      cell(status.matchedKeyId),
      cell(ageOf(meta.creationTimestamp, at)),
    ];
  });
  return (
    "<h2>Approvals</h2>" +
    "<p class=\"blurb\">An Approval that exists is not an approval; an Approval whose " +
    "status weirkeeper set to Verified=True is. One that did not verify stays here with " +
    "its reason, as the record of a rejected attempt.</p>" +
    table(["SUBJECT", "VERIFIED", "APPROVER", "KEY-ID", "AGE"], rows) +
    listFooter()
  );
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
  return (
    "<section class=\"approval\"><h3>" + cell(meta.name) + "</h3>" +
    facts([
      ["subject kind", cell(subject.kind)],
      ["subject name", cell(subject.name)],
      ["plan hash", "<code>" + esc(spec.planHash) + "</code>"],
      ["verified", verifiedBadge(status)],
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

/** The create form: FOUR inputs, two of them from the route.
 *
 *  `SUBJECT KIND` is fixed. `SUBJECT NAME` and `PLAN HASH` are prefilled from
 *  the route the wizard navigated to -- `#/approvals?subject=...&hash=...
 *  &name=...` -- and `PLAN HASH` is read-only, because the value that matters
 *  is the one the wizard showed beside the bytes and the controller recomputes
 *  from the referent. */
export function renderApprovalForm(route) {
  const r = route || {};
  return (
    "<section class=\"step\" id=\"approval-form-section\"><h3>Record an approval</h3>" +
    "<p class=\"blurb\">Paste or upload the two files `logweir drill approve` wrote. They " +
    "are submitted as text, exactly as they arrived: the controller hashes the bytes the " +
    "approver signed, so anything this page did to them in between would be a different " +
    "document.</p>" +
    "<form id=\"approval-form\">" +
    "<label for=\"subject-kind\">SUBJECT KIND</label>" +
    "<select id=\"subject-kind\" name=\"subjectKind\">" +
    "<option value=\"" + esc(SUBJECT_KIND) + "\" selected>" + esc(SUBJECT_KIND) + "</option>" +
    "</select>" +
    "<label for=\"subject-name\">SUBJECT NAME</label>" +
    "<input id=\"subject-name\" name=\"subjectName\" value=\"" + esc(r.subject) + "\">" +
    "<label for=\"plan-hash\">PLAN HASH</label>" +
    "<input id=\"plan-hash\" name=\"planHash\" readonly value=\"" + esc(r.hash) + "\">" +
    "<label for=\"approval-json\">approval.json</label>" +
    "<input type=\"file\" id=\"approval-json-file\" name=\"approvalFile\">" +
    "<textarea id=\"approval-json\" name=\"approvalBytes\" rows=\"8\"></textarea>" +
    "<label for=\"approval-sig\">approval.sig</label>" +
    "<input type=\"file\" id=\"approval-sig-file\" name=\"sidecarFile\">" +
    "<textarea id=\"approval-sig\" name=\"sidecarBytes\" rows=\"8\"></textarea>" +
    "<p class=\"note\">metadata.name is " + esc(r.name) + ", minted from the plan bytes " +
    "before the Restore was created. Neither name is ever edited: both specs are " +
    "immutable.</p>" +
    "<p class=\"refusal-rule\">" + PRIVATE_KEY_REFUSAL + ". A file named for one, or any " +
    "text carrying a private-key header, is refused here and nothing is sent.</p>" +
    "<button type=\"submit\">Create the Approval</button>" +
    "</form></section>"
  );
}

/** The whole page. */
export function renderApprovalsPage(collection, route, now) {
  const panels = itemsOf(collection).map(renderApprovalStatus).join("");
  return renderApprovalList(collection, now) + panels + renderApprovalForm(route);
}

// SUBMIT-REGION-BEGIN

/** The object `create` posts: all four spec fields.
 *
 *  `approvalBytes` and `sidecarBytes` are the strings as they arrived --
 *  interface I18, UTF-8 document text, never base64, never parsed. */
export function approvalBody(route, documents) {
  const r = route || {};
  const d = documents || {};
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Approval",
    metadata: { name: r.name },
    spec: {
      subjectRef: { kind: SUBJECT_KIND, name: r.subject },
      planHash: r.hash,
      approvalBytes: d.approvalBytes,
      sidecarBytes: d.sidecarBytes,
    },
  };
}

/** Refuses key material, or creates the `Approval`. Returns the refusal
 *  message when it refused, and `null` when it created -- so a caller can
 *  render the message without a thrown error standing in for a decision this
 *  page made deliberately. */
export async function submitApproval(route, documents, deps) {
  const d = documents || {};
  const refusal =
    refuseKeyMaterial(d.approvalFileName, d.approvalBytes) ||
    refuseKeyMaterial(d.sidecarFileName, d.sidecarBytes);
  if (refusal !== null) {
    return refusal;
  }
  const api = deps || API;
  await api.create((route || {}).ns, PLURAL, approvalBody(route, d));
  return null;
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

export async function mountApprovals(node, ns, route, parse, deps) {
  const api = deps || API;
  try {
    const collection = await api.list(ns, PLURAL);
    replace(node, parse(renderApprovalsPage(collection, route)));
    wire(node, ns, route, parse, api);
  } catch (error) {
    replace(node, errorBox(error));
  }
}

function wire(node, ns, route, parse, api) {
  const form = node.querySelector("#approval-form");
  if (form === null) {
    return;
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const documents = {
      approvalBytes: form.elements.approvalBytes.value,
      sidecarBytes: form.elements.sidecarBytes.value,
      approvalFileName: fileNameOf(form.elements.approvalFile),
      sidecarFileName: fileNameOf(form.elements.sidecarFile),
    };
    try {
      const refusal = await submitApproval(
        { ns: ns, subject: route.subject, hash: route.hash, name: route.name },
        documents,
        api,
      );
      if (refusal !== null) {
        replace(node, parse("<p class=\"refusal\">" + refusal + "</p>"));
        return;
      }
      await mountApprovals(node, ns, route, parse, api);
    } catch (error) {
      replace(node, errorBox(error));
    }
  });

  // A chosen file is READ AS TEXT into the textarea beside it, so the bytes
  // that are submitted are the bytes a viewer can see, and the refusal runs
  // over the same two values either way.
  for (const pair of [
    ["#approval-json-file", "#approval-json"],
    ["#approval-sig-file", "#approval-sig"],
  ]) {
    const input = node.querySelector(pair[0]);
    const area = node.querySelector(pair[1]);
    if (input === null || area === null) {
      continue;
    }
    input.addEventListener("change", async () => {
      const chosen = input.files && input.files[0];
      if (!chosen) {
        return;
      }
      if (refuseKeyMaterial(chosen.name, "") !== null) {
        input.value = "";
        area.value = "";
        replace(node, parse("<p class=\"refusal\">" + PRIVATE_KEY_REFUSAL + "</p>"));
        return;
      }
      area.value = await chosen.text();
    });
  }
}

function fileNameOf(input) {
  const chosen = input && input.files && input.files[0];
  return chosen ? chosen.name : "";
}
