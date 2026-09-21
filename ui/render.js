// render.js -- DOM helpers. This module issues no network request of any kind;
// `api.js` is the only module in this tree that does.
//
// WHAT IS DELIBERATELY NOT HERE. There is no function in this file that
// decides whether an object verified, and there must not be. The verdict a
// page shows is read from the object's own status -- weirkeeper writes
// `status.evidence.verification` and a `Verified` condition, and the badge
// rules are a conjunction over those recorded fields and the run's own outcome.
// A helper here that took a guess at that conjunction, or that defaulted a
// missing field to a favourable answer, would be this page inventing a claim
// about signed evidence it never read. The shell renders no verdict at all;
// the pages that render one arrive with their own tests.
//
// Everything below is structural: it builds elements and sets TEXT. Nothing
// here writes `innerHTML`, so a name, a reason or an API server message drawn
// from the cluster is inserted as text and can never become markup.

/** Creates an element. `attrs` are set as attributes; `children` may be
 *  strings (inserted as text) or nodes. */
export function el(tag, attrs, children) {
  const node = document.createElement(tag);
  if (attrs) {
    for (const key of Object.keys(attrs)) {
      const value = attrs[key];
      if (value !== null && value !== undefined) {
        node.setAttribute(key, String(value));
      }
    }
  }
  append(node, children);
  return node;
}

/** Appends children to a node. A string child becomes a text node -- never
 *  markup. */
export function append(node, children) {
  if (children === null || children === undefined) {
    return node;
  }
  const list = Array.isArray(children) ? children : [children];
  for (const child of list) {
    if (child === null || child === undefined) {
      continue;
    }
    if (typeof child === "string" || typeof child === "number") {
      node.appendChild(document.createTextNode(String(child)));
    } else {
      node.appendChild(child);
    }
  }
  return node;
}

/** Empties a node. */
export function clear(node) {
  while (node.firstChild !== null) {
    node.removeChild(node.firstChild);
  }
  return node;
}

/** Replaces a node's children in one step. */
export function replace(node, children) {
  return append(clear(node), children);
}

/** Renders an error from `api.js` as the API server reported it: its own
 *  status code, its own `reason`, its own `message`, and nothing added.
 *
 *  A 403 here is the API server's 403 about the viewer's own RBAC. This
 *  function neither softens it nor explains it away. */
export function errorBox(error) {
  const status = error && error.status ? String(error.status) : "error";
  const reason = error && error.reason ? String(error.reason) : "";
  const message = error && error.message ? String(error.message) : String(error);
  return el("div", { class: "error", role: "alert" }, [
    el("span", { class: "error-status" }, reason.length > 0 ? status + " " + reason : status),
    el("p", { class: "error-message" }, message),
  ]);
}

// ===========================================================================
// THE STRING HALF -- the primitives the four page modules are built from.
// ===========================================================================
//
// WHY A STRING AND NOT A NODE. Everything below is a pure function from data
// to an HTML string: no `document`, no `window`, no network, no clock. That is
// what makes the page rules testable. `node --test` has no DOM, and this tree
// has no bundler, no npm and no DOM shim (Global Constraints 17 and 21), so a
// rule expressed as "the detail view renders this exact line" can only be
// asserted if the thing a page produces is a value a test can read. It is a
// string, and `scripts/check-ui-behaviour.sh` runs the assertions on every
// `just lint`.
//
// AND NOTHING BELOW WRITES `innerHTML`. The mount half in `app.js` parses a
// string with `DOMParser`, which does not execute script content, and adopts
// the nodes. Every value that comes from the cluster passes through `esc`
// before it reaches a string, so a topic name, a Secret name or an API server
// message is text in the output and can never become markup. The two claims
// `ui/README.md` and this file's own header make -- "sets text, never
// `innerHTML`" -- stay true of this module and of the whole tree.
//
// STILL NO VERDICT HERE. `badge(kind, text)` is structural: it puts a class
// and a caption on a span. It decides nothing. The two badge RULES -- one per
// kind, because a `Backup` carries no `outcome` -- live in the page modules
// with their fixtures and their tests, and `evidenceBlock` renders what the
// controller RECORDED without re-deriving any part of it.

/** The rightwards arrow, written as an escape because every file under `ui/`
 *  outside `tests/` is plain ASCII (`the_ui_sources_are_ascii_only`). The
 *  OUTPUT carries the character; the source carries six ASCII bytes. */
export const ARROW = "\u2192";

/** `evidence.immutable` is `false` in every document this product writes, and
 *  the field carries no information at all (C38, spec section 12). It is rendered as
 *  this exact line, and NEVER as a tick: a tick beside the word `immutable`
 *  tells a viewer that a document is WORM-protected, and nothing in this
 *  product established that. */
export const IMMUTABLE_LINE =
  "immutable: false (this field carries no information in format_version 1.0.0)";

/** `engine_subreport` is `null` in every scorecard, because the engine adapter
 *  never overrides the validation-run hook. A page that OMITTED the row would
 *  render "no caveat", which is a different and false claim from "not produced
 *  by this engine version". */
export const ENGINE_SUBREPORT_LINE =
  "engine sub-report: not produced by this engine version";

/** Every list view carries this, verbatim. A custom resource is a cluster's
 *  view of a run; the authoritative index is the evidence bucket, because
 *  deleting the object does not delete the signed document it names. */
export const BUCKET_FOOTER =
  "this list is the cluster's view; the authoritative index is the evidence bucket";

/** The retention panel's sentence. Logweir holds no delete capability of any
 *  kind against an archive (Global Constraint 6): the panel reports, and the
 *  commands beside it are the operator's to run. */
export const RETENTION_SENTENCE =
  "Logweir never deletes from your archive. These are the commands you would run.";

/** The word a badge carries when it is not green. Never "pass", never
 *  "verified in your browser": this page verified nothing. */
export const UNVERIFIED = "unverified";

// ---------------------------------------------------------------------------
// THE RESTORE-SIDE FIXED SENTENCES (Task 27). Same rule as the five above:
// each is a sentence of OURS, rendered verbatim and never through `esc`, and
// each is asserted by name in `ui/tests/pages.spec.js`. They live here rather
// than in the three pages that print them for one reason -- a sentence that
// exists in two files is a sentence two editors can make disagree.
// ---------------------------------------------------------------------------

/** `Restore.spec` is CEL-immutable (`self == oldSelf`, spec section 3.2), so
 *  there is no such thing as editing a plan in place: "edit" prefills a NEW
 *  draft whose bytes hash differently, and the approval that covered the old
 *  one does not cover it. The page says so IN THE PAGE, above the form, rather
 *  than discovering it at the 422 (C48). */
export const RESTORE_IMMUTABLE_SENTENCE =
  "Restore.spec is immutable. This creates a NEW Restore with a new plan hash; " +
  "the existing approval does not cover it.";

/** Why a download button exists beside the copy button.
 *
 *  The plan bytes are the thing an approval binds, so "nearly the same bytes"
 *  is not a degraded copy -- it is a different document with a different
 *  sha256, and `logweir drill approve` would sign the wrong one. Several
 *  browsers strip trailing whitespace from a clipboard copy of a `<pre>`, and
 *  a plan whose last line ends in spaces is not exotic. So the page offers the
 *  download, and names the `kubectl` route to the same bytes for anyone who
 *  would rather take them from the cluster. */
export const COPY_CAVEAT =
  "copy loses trailing whitespace in some browsers; download, or run kubectl " +
  "--context docker-desktop get restore <name> -o jsonpath='{.spec.planBytes}' > " +
  "<name>.yaml, and hash exactly what you downloaded.";

/** The client-side window lint is a CONVENIENCE and never the gate. The
 *  controller recomputes the hash from the referent's own bytes and phase 0
 *  refuses a point the archive does not cover; a page that presented its own
 *  check as the gate would be claiming an authority it does not have. */
export const CONVENIENCE_SENTENCE =
  "this check is a convenience and never the gate: the controller and phase 0 " +
  "are the gate, and they read the bytes you submit.";

/** The approvals page's refusal, verbatim. It is the whole of the message: a
 *  page that took a private key would be the most consequential missing
 *  subsystem in this product reimplemented inside a browser (Global
 *  Constraint 28). */
export const PRIVATE_KEY_REFUSAL = "this page never accepts a private key";

/** `Approval.status.selfAttestedRisk` rendered as a SENTENCE and never as a
 *  bare boolean, for `true`.
 *
 *  A tick or a `false` beside the words "self attested" reads as "two people
 *  signed off". It means only that the two matched key ids differ, and one
 *  operator may hold both keys (spec section 9). */
export const SELF_ATTESTED_TRUE =
  "self-attested: the approver key and the signing key are the same; " +
  "\"not self-attested\" means only two different keys, and one operator may hold both";

/** The same field for `false`: the second half of [`SELF_ATTESTED_TRUE`]
 *  alone, because the first half is a statement about THIS approval and only
 *  the second is true of it. */
export const SELF_ATTESTED_FALSE =
  "\"not self-attested\" means only two different keys, and one operator may hold both";

/** The window the archive covers, both bounds as RFC 3339, and the refusal a
 *  point outside it gets. Interface I22: `Backup.status.windowCovered` is two
 *  INTEGERS, so a page that read `covered.from`/`covered.to` would render
 *  `Invalid Date` on both bounds and default step 3 to nothing. */
export function windowMessage(fromMs, toMs) {
  return (
    "the archive covers [" +
    rfc3339(fromMs) +
    ", " +
    rfc3339(toMs) +
    "]; a point outside it cannot be restored"
  );
}

/** What the RUN will do to the target before the engine starts, named in the
 *  page so an operator reads it before the plan is signed rather than out of a
 *  refusal afterwards (spec section 6.1, guard G-TS).
 *
 *  IT IS NOT A READINESS VERDICT, AND SINCE D2 IT SAYS SO. The wizard's step 5
 *  now carries a real `Preflight`, whose aggregate comes from a check Job's own
 *  recorded result; this sentence describes what EXECUTION will attempt, which
 *  is a different fact and one no check can confirm in advance -- the broker's
 *  answer to a topic create is only knowable when the create is made, which is
 *  why `target.logAppendTime` is an execution-only check (D2 section 6.3, G12).
 *
 *  The old copy called this "preflight". That word now names the object above
 *  it, and one word for two things is how an operator comes to read a
 *  description of an intention as a statement that it was checked. */
export function preflightSentence(topicCount) {
  return (
    "At execution time Logweir will create " +
    String(topicCount) +
    " topics with message.timestamp.type=CreateTime and retention.ms=-1 before the " +
    "engine runs, and will refuse if the broker is LogAppendTime and rejects the override. " +
    "This says what the run will attempt; it is not a check and nothing above has confirmed it."
  );
}

/** An RFC 3339 instant as epoch milliseconds, or `null` when it is not an
 *  instant this page can read.
 *
 *  THE COMPARISON RUNS IN MILLISECONDS, NOT IN STRINGS. `windowCovered` is two
 *  integers and the point-in-time field is text; comparing the two as strings
 *  would silently accept every point, because a string is never less than an
 *  integer under a relational operator in JavaScript -- it is `NaN`, and every
 *  comparison against `NaN` is `false`. */
export function epochMs(instant) {
  if (typeof instant !== "string" || instant.length === 0) {
    return null;
  }
  const at = Date.parse(instant);
  return isNaN(at) ? null : at;
}

/** What a page prints where a field the API server never set would go. */
export const ABSENT = "-";

/** HTML-escapes a value and returns it as a string. EVERY value that came from
 *  the cluster goes through this before it reaches a rendered string. */
export function esc(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/** A value for display: the value escaped, or [`ABSENT`] when it is not set.
 *  `false` and `0` are values, not absences, and render as themselves. */
export function cell(value) {
  if (value === null || value === undefined || value === "") {
    return ABSENT;
  }
  return esc(value);
}

/** The sentence an empty table carries when its caller gives it none. */
export const EMPTY_TABLE_SENTENCE = "no object of this kind in this namespace";

/** A table. `columns` are header captions; `rows` are arrays of ALREADY
 *  RENDERED cells -- a caller that wants a badge in a cell passes the badge.
 *  A row shorter than `columns` is padded, so a missing status field cannot
 *  shift a column silently.
 *
 *  `empty` is the sentence shown when there is no row: a list view passes one
 *  that says what to do next, and a caller that passes none gets
 *  [`EMPTY_TABLE_SENTENCE`]. It is a sentence of OURS and is not escaped.
 *
 *  The table sits in a `div.table-wrap`, which scrolls sideways on a narrow
 *  laptop and lets the stylesheet stack the rows into cards below 720 px; the
 *  column captions those cards show are copied from the header row by
 *  `app.js` when the nodes are adopted, so the string here carries them once.
 *
 *  `rowAttributes[i]`, when given, is spelled into row `i`'s opening tag. It
 *  exists for ONE thing: naming a row so the page can find it again without
 *  counting -- the restore wizard's point selector marks each row with the
 *  Backup's UID and filters in place, and an index-counted lookup would follow
 *  the wrong row the moment the list changed under it. It is a string OF OURS
 *  (a caller renders it from `esc`'d values) and is never a value read out of
 *  the cluster unescaped. */
export function table(columns, rows, empty, rowAttributes) {
  const attributes = Array.isArray(rowAttributes) ? rowAttributes : [];
  const head = columns.map((c) => "<th scope=\"col\">" + esc(c) + "</th>").join("");
  const body = rows
    .map((row, index) => {
      const cells = [];
      for (let i = 0; i < columns.length; i += 1) {
        cells.push("<td>" + (row[i] === undefined ? ABSENT : row[i]) + "</td>");
      }
      const extra = typeof attributes[index] === "string" && attributes[index].length > 0
        ? " " + attributes[index]
        : "";
      return "<tr" + extra + ">" + cells.join("") + "</tr>";
    })
    .join("");
  const sentence = typeof empty === "string" && empty.length > 0 ? empty : EMPTY_TABLE_SENTENCE;
  const none = rows.length === 0
    ? "<tr><td class=\"empty\" colspan=\"" + columns.length + "\">" + sentence + "</td></tr>"
    : "";
  return (
    "<div class=\"table-wrap\"><table class=\"grid\"><thead><tr>" +
    head +
    "</tr></thead><tbody>" +
    body +
    none +
    "</tbody></table></div>"
  );
}

/** A link to one object's detail view: `#/<route>?ns=<ns>&name=<name>`.
 *
 *  A HASH, AND NOTHING BUT A HASH. The whole router lives after the `#`, so
 *  the browser never asks the file server for a route it cannot serve -- the
 *  static half of `kubectl proxy` is a plain file server with no rewrite rule
 *  and no 404 hook. Both values are percent-encoded, and the caption is
 *  escaped like any other value from the cluster. */
export function detailLink(route, ns, name) {
  const target =
    "#/" +
    route +
    "?ns=" +
    encodeURIComponent(ns) +
    "&name=" +
    encodeURIComponent(name);
  return "<a href=\"" + esc(target) + "\">" + esc(name) + "</a>";
}

/** A badge. STRUCTURAL ONLY: `kind` becomes a class suffix and `text` becomes
 *  the caption. Which kind a run gets is the calling page's decision, made
 *  from the object's own recorded fields. */
export function badge(kind, text) {
  return (
    "<span class=\"badge badge-" + esc(kind) + "\">" + esc(text) + "</span>"
  );
}

/** A recorded `status.phase` as a badge whose caption IS the phase, verbatim.
 *
 *  STRUCTURAL, LIKE [`badge`]. The kind is the phase lowercased into a class
 *  suffix -- `Succeeded` gives `badge-phase-succeeded` -- and the stylesheet
 *  colours the phases it knows; an unknown phase is a neutral badge that still
 *  says its own name. Nothing here decides what a phase means, and an absent
 *  phase renders as [`ABSENT`] rather than as a badge with no words. */
export function phaseBadge(phase) {
  if (typeof phase !== "string" || phase.length === 0) {
    return ABSENT;
  }
  const kind = "phase-" + phase.toLowerCase().replace(/[^a-z0-9]+/g, "-");
  return badge(kind, phase);
}

// ===========================================================================
// D2 (PLAT-08, PLAT-09.1, PLAT-03): the words for destinations, topic
// visibility and operation readiness
// ===========================================================================
//
// THREE SENTENCES THIS PRODUCT MUST NEVER RENDER, and the reason each one is
// forbidden. They are kept together because they are one rule seen three
// times: a page states what was OBSERVED and by WHOM, and never a conclusion
// nobody drew.
//
//   1. "complete", for a topic inventory. An all-topics Kafka Metadata request
//      SILENTLY OMITS topics the principal cannot DESCRIBE -- no error, no
//      count, nothing to notice. A successful listing therefore proves nothing
//      about completeness, which is why `visibility.state` is `unknown` for
//      one and why `unknown` is that field's HEALTHY default rather than a
//      fault. [`visibilityLine`] carries the words.
//   2. "verified by Logweir", for an attestation. `attestedComplete` is an
//      ADMINISTRATOR'S CLAIM recorded in a policy ConfigMap. Logweir checked
//      that the claim matches this cluster, this principal and this moment; it
//      did not check that the claim is TRUE, and it cannot. So the attestation
//      is always rendered with its author, its instant and the disclaimer.
//   3. "ready", for anything this page decided. A readiness verdict comes from
//      a `Preflight`'s own recorded aggregate and from nowhere else -- not
//      from "no checks failed", not from "the list came back", not from an
//      empty `checks` array. [`preflightVerdict`] reads `state` and renders
//      `unknown` for every shape it does not recognise.

/** The disclaimer every rendered attestation carries. Not a suffix a caller
 *  may leave off: [`attestationLine`] appends it, and the specs assert that
 *  no attestation reaches the page without it. */
export const ATTESTATION_DISCLAIMER = "not verified by Logweir";

/** What a successful listing alone means, said plainly. */
export const VISIBILITY_UNKNOWN_SENTENCE =
  "Kafka hides topics this principal cannot describe, and hides them without saying so, so a " +
  "successful listing is not proof that this is every topic. Logweir records that as unknown " +
  "rather than calling it complete.";

/** What `limited` means. */
export const VISIBILITY_LIMITED_SENTENCE =
  "An authorization omission was observed: at least one topic exists that this principal may " +
  "not describe. The list below is a subset, and Logweir cannot say how large a subset.";

/** What an empty result means, which is NOT "the cluster is empty". */
export const EMPTY_INVENTORY_SENTENCE =
  "No visible topics. Kafka hides topics this principal cannot describe; this is not proof " +
  "that the cluster is empty.";

/** What "ready" does NOT cover. Rendered beside every ready verdict that
 *  carries execution-only checks, because "ready" never means those passed. */
export const EXECUTION_ONLY_SENTENCE =
  "These can only be answered while the run executes. They are excluded from the verdict " +
  "above, and a ready verdict never means they passed.";

/** What a destination test proves and what it does not. */
export const DESTINATION_TEST_SENTENCE =
  "A test starts a Preflight: a real Job, with this destination's own credentials, against " +
  "this destination's own endpoint. The verdict below is that check's recorded result, read " +
  "back from its status -- this page performs no I/O of its own and decides nothing.";

/** The one sentence that says what a readiness result is ABOUT. */
export const APPLICABILITY_SENTENCE =
  "Applicability is recomputed on every read against the objects as they are now. A result " +
  "that no longer describes your current inputs is shown as out of date, never as a verdict.";

/** How an attestation is rendered, always: who, when, and the disclaimer.
 *
 *  THE PRODUCT API PUBLISHES ONE STRING for it, not a `{by, at}` pair, so this
 *  function does not invent a structure it was not given: it prints the
 *  recorded claim verbatim and appends the disclaimer. What it refuses to do
 *  is print the claim ALONE. */
export function attestationLine(attestation) {
  if (typeof attestation !== "string" || attestation.length === 0) {
    return "";
  }
  return esc(attestation) + "; " + ATTESTATION_DISCLAIMER;
}

/** The visibility banner: the state, what it means, and the basis the check
 *  recorded for it.
 *
 *  AN ABSENT `visibility` IS `unknown`, NOT A BLANK. A discovery that has not
 *  finished has no visibility block at all, and "we do not know" is exactly
 *  what that is -- so the same words are used, with no basis line. */
export function visibilityLine(visibility) {
  const v = visibility || {};
  const state = typeof v.state === "string" ? v.state : "unknown";
  const basis = Array.isArray(v.basis) ? v.basis : [];
  let sentence = VISIBILITY_UNKNOWN_SENTENCE;
  let kind = "visibility-unknown";
  if (state === "limited") {
    sentence = VISIBILITY_LIMITED_SENTENCE;
    kind = "visibility-limited";
  } else if (state === "attestedComplete") {
    sentence = "An administrator attests that this principal sees every topic: " +
      attestationLine(v.attestation) + ".";
    kind = "visibility-attested";
  }
  return (
    "<div class=\"visibility " + kind + "\" role=\"status\">" +
    badge(kind, "visibility: " + state) +
    "<p class=\"note\">" + (state === "attestedComplete" ? sentence : esc(sentence)) + "</p>" +
    (basis.length === 0
      ? ""
      : "<p class=\"basis\">basis: " + esc(basis.join(", ")) + "</p>") +
    "</div>"
  );
}

/** A destination's `status.valid`, in words.
 *
 *  ABSENT IS "NOT JUDGED". `null` there means the controller has not reached a
 *  verdict -- which is what every object one second old looks like, and what
 *  EVERY object looks like on a cluster whose controller predates these kinds.
 *  Rendering it as "invalid" would report a missing controller as a broken
 *  destination. */
export function destinationVerdict(status) {
  const s = status || {};
  if (s.valid === true) {
    return badge("green", "valid" + (typeof s.reason === "string" && s.reason.length > 0
      ? " (" + s.reason + ")" : ""));
  }
  if (s.valid === false) {
    return badge("unverified", "not valid" + (typeof s.reason === "string" && s.reason.length > 0
      ? " (" + s.reason + ")" : ""));
  }
  return badge("pending", "not judged yet");
}

/** The sentence beside [`destinationVerdict`] when nothing has judged it. */
export const NOT_JUDGED_SENTENCE =
  "No controller has recorded a verdict for this destination yet. That is what a new object " +
  "looks like, and it is also what an installation whose controller predates these kinds looks " +
  "like. It is not a claim that the destination is wrong, and it is not a claim that it works.";

/** A preflight's aggregate, as a badge. The RECORDED state and nothing else:
 *  a shape this build does not recognise is `unknown`, never `ready`. */
export function preflightVerdict(state) {
  if (state === "ready") {
    return badge("green", "ready");
  }
  if (state === "notReady") {
    return badge("unverified", "not ready");
  }
  if (state === "failed") {
    return badge("unverified", "failed: no result");
  }
  if (state === "cancelled") {
    return badge("pending", "cancelled: no result");
  }
  if (state === "pending" || state === "queued" || state === "running") {
    return badge("pending", state);
  }
  return badge("pending", "unknown");
}

/** One stale reason, in words, with its subject when it has one.
 *
 *  `unverifiable` IS NOT A KIND OF STALENESS and is not rendered as one. It is
 *  this service saying it could not COMPARE something, and its `basis` says
 *  what -- so the words are "could not be checked", never "out of date". */
export function staleReasonLine(reason) {
  const r = reason || {};
  const subject = typeof r.kind === "string" && r.kind.length > 0
    ? " (" + r.kind + (typeof r.name === "string" && r.name.length > 0 ? "/" + r.name : "") + ")"
    : "";
  if (r.reason === "unverifiable") {
    return "could not be checked" + subject +
      (typeof r.basis === "string" && r.basis.length > 0 ? ": " + r.basis : "");
  }
  return String(r.reason) + subject;
}

/** The applicability banner: whether this result still describes the caller's
 *  inputs, why not, and what the comparison actually covered.
 *
 *  `staleBasis` IS RENDERED, and that is the point of it. An empty
 *  `staleReasons` means "I compared these and they match"; without the list of
 *  "these" a reader cannot tell a verdict that was re-checked against live
 *  objects from one where the check was skipped, and those two look identical
 *  in every other field. */
export function applicabilityLine(preflight) {
  const p = preflight || {};
  const reasons = Array.isArray(p.staleReasons) ? p.staleReasons : [];
  const basis = Array.isArray(p.staleBasis) ? p.staleBasis : [];
  const head = p.applicable === true
    ? badge("green", "applies to your current inputs")
    : badge("unverified", "does not apply to your current inputs");
  return (
    "<div class=\"applicability\" role=\"status\">" +
    head +
    (reasons.length === 0
      ? ""
      : "<ul class=\"stale-reasons\">" +
        reasons.map((r) => "<li>" + esc(staleReasonLine(r)) + "</li>").join("") +
        "</ul>") +
    (basis.length === 0
      ? "<p class=\"basis\">compared: nothing. An empty comparison is not a match.</p>"
      : "<p class=\"basis\">compared: " + esc(basis.join(", ")) + "</p>") +
    "<p class=\"note\">" + esc(APPLICABILITY_SENTENCE) + "</p>" +
    "</div>"
  );
}

/** The verdict of one check, as a badge. `skipped` is never a pass. */
export function checkVerdict(state) {
  if (state === "ready") {
    return badge("green", "ready");
  }
  if (state === "notReady") {
    return badge("unverified", "not ready");
  }
  if (state === "skipped") {
    return badge("pending", "skipped (never a pass)");
  }
  return badge("pending", "unknown");
}

/** The check table: one row per recorded entry, with the gating, the code, the
 *  message, the remedy, and when the fact stops counting.
 *
 *  EVERY COLUMN IS A RECORDED FIELD. Nothing here is computed from the others,
 *  and an absent field prints [`ABSENT`] rather than a guess: a check with no
 *  `expiresAt` is one whose expiry the producer did not record, which is a
 *  different thing from one that never expires. */
export function checkTable(checks, empty) {
  const rows = (Array.isArray(checks) ? checks : []).map((c) => [
    "<code>" + cell(c.id) + "</code>",
    checkVerdict(c.state),
    cell(c.gating),
    cell(c.code),
    cell(c.message),
    cell(c.remedy),
    checkScope(c.scope),
    cell(c.observedAt),
    cell(c.expiresAt),
  ]);
  return table(
    ["CHECK", "VERDICT", "GATING", "CODE", "MESSAGE", "REMEDY", "SCOPE", "OBSERVED", "EXPIRES"],
    rows,
    empty,
  );
}

/** What a row is ABOUT, as `Kind/name`.
 *
 *  PLAT-03.1's acceptance sentence is "names each failed prerequisite and its
 *  remedy, with check time **and scope**", and until review F5 this table
 *  rendered the first two and dropped the third: the controller fills every
 *  row's `scope`, the typed client declares it, and no console surface printed
 *  it. A `notReady` row whose subject is invisible makes an operator guess
 *  which of two connections or two destinations a refusal is about.
 *
 *  A scope with no `kind` and no `name` prints [`ABSENT`] like every other
 *  unrecorded field -- a verdict stored by an older controller carries none,
 *  and inventing one would be a guess about which object was checked. */
export function checkScope(scope) {
  const s = scope || {};
  const kind = typeof s.kind === "string" ? s.kind : "";
  const name = typeof s.name === "string" ? s.name : "";
  if (kind.length === 0 && name.length === 0) {
    return ABSENT;
  }
  if (kind.length === 0 || name.length === 0) {
    return esc(kind + name);
  }
  return esc(kind + "/" + name);
}

/** The execution-only list, with the sentence that says what "ready" does not
 *  cover. Rendered whenever the result names any, whatever the aggregate. */
export function executionOnlyBlock(entries) {
  const list = Array.isArray(entries) ? entries : [];
  if (list.length === 0) {
    return "";
  }
  return (
    "<section class=\"execution-only\"><h4>Only knowable at execution time</h4>" +
    "<p class=\"note\">" + esc(EXECUTION_ONLY_SENTENCE) + "</p>" +
    "<ul>" +
    list.map((e) => "<li><code>" + cell(e.id) + "</code> " + cell(e.note) + "</li>").join("") +
    "</ul></section>"
  );
}

/** The footer every list view carries. */
export function listFooter() {
  // NOT `esc`-ed, deliberately. This is a fixed sentence of ours, not a value
  // from the cluster: it carries no `<`, `>` or `&`, and escaping it would
  // turn its apostrophe into an entity -- so the sentence in the bytes would
  // stop being the sentence the product promises, and every assertion on it
  // would be an assertion on a different string. Only cluster data goes
  // through `esc`, and all of it does.
  return "<p class=\"list-footer\">" + BUCKET_FOOTER + "</p>";
}

/** A definition list of `[term, alreadyRenderedValue]` pairs. */
export function facts(pairs) {
  const items = pairs
    .map((p) => "<dt>" + esc(p[0]) + "</dt><dd>" + p[1] + "</dd>")
    .join("");
  return "<dl class=\"facts\">" + items + "</dl>";
}

/** A block a viewer copies. `lines` are escaped and joined with newlines. */
export function copyBlock(lines) {
  return "<pre class=\"copy\">" + lines.map((l) => esc(l)).join("\n") + "</pre>";
}

/** An epoch-millisecond instant as RFC 3339, UTC, second precision.
 *
 *  `Backup.status.windowCovered` is two INTEGERS (interface I22), because the
 *  receipt it mirrors carries integers and a controller that converted between
 *  the two representations is a controller that can round a window boundary.
 *  A page is the other end of that: a viewer reading `1757253900000` learns
 *  nothing, so the conversion happens HERE, once, and the raw integer is never
 *  printed. */
export function rfc3339(ms) {
  if (typeof ms !== "number" || !isFinite(ms)) {
    return ABSENT;
  }
  const iso = new Date(ms).toISOString();
  return iso.length === 24 && iso.slice(19, 24) === ".000Z"
    ? iso.slice(0, 19) + "Z"
    : iso;
}

/** The covered window, both bounds as RFC 3339 and never a bare integer. */
export function coveredWindow(windowCovered) {
  const w = windowCovered || {};
  return (
    "covered: " + rfc3339(w.fromMs) + " " + ARROW + " " + rfc3339(w.toMs)
  );
}

/** The bucket an object-store URL names: the first segment after the scheme.
 *  Used only to render a copyable fetch command; nothing here addresses a
 *  store. */
export function bucketOf(url) {
  if (typeof url !== "string" || url.length === 0) {
    return "<your evidence bucket>";
  }
  const marker = ":" + "//";
  const at = url.indexOf(marker);
  const rest = at === -1 ? url : url.slice(at + marker.length);
  const slash = rest.indexOf("/");
  const bucket = slash === -1 ? rest : rest.slice(0, slash);
  return bucket.length === 0 ? "<your evidence bucket>" : bucket;
}

/** The key prefix an object-store URL names: everything after the bucket.
 *  The twin of [`bucketOf`], and built the same way, so a plan document's
 *  `storage.prefix` and the fetch command beside it come from one reading of
 *  one string. An empty prefix is the empty string, which is what the runner's
 *  `StorageUrl` defaults to. */
export function prefixOf(url) {
  if (typeof url !== "string" || url.length === 0) {
    return "";
  }
  const marker = ":" + "//";
  const at = url.indexOf(marker);
  const rest = at === -1 ? url : url.slice(at + marker.length);
  const slash = rest.indexOf("/");
  if (slash === -1) {
    return "";
  }
  const prefix = rest.slice(slash + 1);
  return prefix;
}

/** THE INDEPENDENT CHECK, AND IT FETCHES BEFORE IT VERIFIES.
 *
 *  `status.evidence.receiptKey` and `scorecardKey` are OBJECT-STORE KEYS, and
 *  `logweir drill verify`'s `--scorecard`, `--signature` and `--public-key`
 *  are all filesystem paths (`crates/logweir/src/cli.rs`). Two verify lines
 *  carrying an object key are two lines that cannot run. So the block is FOUR
 *  lines: two fetches, then the two verifiers -- the Rust one and the Python
 *  one, which is the whole point of a second implementation -- over the local
 *  files the fetches just wrote.
 *
 *  `payloadType` is `backup-receipt` for a `Backup` and `scorecard` for a
 *  `Restore`. `document` is the local filename the fetch writes. */
export function independentCheck(payloadType, bucket, documentKey, sidecarKey, documentFile, sidecarFile) {
  return copyBlock([
    "aws s3 cp s3://" + bucket + "/" + documentKey + " ./" + documentFile,
    "aws s3 cp s3://" + bucket + "/" + sidecarKey + " ./" + sidecarFile,
    "logweir drill verify --payload-type " +
      payloadType +
      " --scorecard ./" +
      documentFile +
      " --signature ./" +
      sidecarFile +
      " --public-key <your key>",
    "python3 verify_scorecard.py --payload-type " +
      payloadType +
      " --scorecard ./" +
      documentFile +
      " --signature ./" +
      sidecarFile +
      " --public-key <your key>",
  ]);
}

// ---------------------------------------------------------------------------
// THE FORM HALF (PLAT-13.2): field-level errors and one mutation status that
// every form renders the same way. Strings, like everything in this half, so
// the behaviour suite reads exactly what a browser adopts.
// ---------------------------------------------------------------------------

/** An error from `api.js` as a string: the twin of [`errorBox`], with the API
 *  server's own status, reason and message, escaped and nothing added.
 *  `live` false drops `role="alert"` for an error nested inside a region that
 *  already announces itself. */
export function errorBlock(error, live) {
  const status = error && error.status ? String(error.status) : "error";
  const reason = error && error.reason ? String(error.reason) : "";
  const message = error && error.message ? String(error.message) : String(error);
  return (
    "<div class=\"error\"" + (live === false ? "" : " role=\"alert\"") + ">" +
    "<span class=\"error-status\">" + esc(reason.length > 0 ? status + " " + reason : status) +
    "</span><p class=\"error-message\">" + esc(message) + "</p></div>"
  );
}

/** The attributes an input carries when the page has something to say about
 *  it: `aria-invalid` and a pointer to the message, so a screen reader reads
 *  the message with the field. The empty string when there is nothing. */
export function invalidAttributes(id, messages) {
  return Array.isArray(messages) && messages.length > 0
    ? " aria-invalid=\"true\" aria-describedby=\"" + esc(id) + "-error\""
    : "";
}

/** The message line under a field, or the empty string. */
export function fieldErrorLine(id, messages) {
  if (!Array.isArray(messages) || messages.length === 0) {
    return "";
  }
  return (
    "<p class=\"field-error\" id=\"" + esc(id) + "-error\">" +
    messages.map((m) => esc(m)).join(" ") + "</p>"
  );
}

/** THE ONE MUTATION STATUS every form renders: pending, succeeded or failed,
 *  in words, for the object `subject` names.
 *
 *  `subject` is `{kind, name}` plus, when the request is not a create, what it
 *  actually was:
 *
 *    `verb`      -- `"create"` (the default) or `"patch"`;
 *    `field`     -- for a patch, the ONE field it sets (`spec.suspend`);
 *    `value`     -- for a patch, the value it sets that field to;
 *    `resubmits` -- `false` when submitting the form as it stands now would
 *                   send a DIFFERENT request than the attempt these words are
 *                   about, so "submit again" cannot resolve this outcome.
 *
 *  THE WORDS FOR A FAILURE SAY WHAT IS KNOWN, AND SAY IT ABOUT THE REQUEST
 *  THAT WAS MADE. A create's unknown outcome is safe to retry because the name
 *  is chosen before the request and an object that already exists with exactly
 *  this content is recognised rather than duplicated. NONE OF THAT IS TRUE OF
 *  A PATCH: it creates nothing, it is safe to repeat because it sets a named
 *  field of an object that already exists to the same value, and saying the
 *  create sentence over a suspend toggle would be narration this tree does not
 *  do. `unmatched` is the API server's field causes no input claimed. */
export function mutationStatus(state, subject, unmatched) {
  const s = state || {};
  const who = subject || {};
  const kind = esc(who.kind);
  const name = esc(who.name);
  // A SUBJECT WITH NO NAME IS A KIND, NOT A KIND AND A GAP (review F4). Every
  // create in this tree until D1 W7 named its object BEFORE sending it -- that
  // name is what made the create idempotent -- so `kind + " " + name` was
  // always two words. A manual `Backup` has no name until the server derives
  // one from the authenticated subject, so its subject carries `name: ""` and
  // every sentence below would have read "Backup : sent" with a hole in it.
  const subject_ = name.length > 0 ? kind + " " + name : kind;
  const patch = who.verb === "patch";
  if (s.phase === "pending") {
    return statusRegion(
      "pending",
      "<p>" + subject_ + ": sent, waiting for the API server. Submitting again is " +
        "disabled until it answers, so one click makes one request.</p>",
    );
  }
  const kept = patch ? "" : keptClause(who);
  if (s.phase === "succeeded") {
    const result = s.result || {};
    const meta = ((result.object || {}).metadata) || {};
    const shown = esc(typeof meta.name === "string" && meta.name.length > 0 ? meta.name : who.name);
    const uid = esc(meta.uid);
    if (patch) {
      return statusRegion(
        "succeeded",
        "<p>" + esc(who.field) + " is now " + esc(String(who.value)) + " on " + subject_ +
          ". Nothing was created.</p>",
      );
    }
    return statusRegion(
      "succeeded",
      result.outcome === "existing"
        ? "<p>" + kind + " " + shown + " already existed with exactly this content (uid " + uid +
          "); nothing new was created.</p>"
        : "<p>Created " + kind + " " + shown + " (uid " + uid + ").</p>",
    );
  }
  if (s.phase !== "failed") {
    return "";
  }
  const error = s.error || {};
  const extra = Array.isArray(unmatched) && unmatched.length > 0
    ? "<ul class=\"field-error-list\">" + unmatched.map((m) => "<li>" + esc(m) + "</li>").join("") + "</ul>"
    : "";
  if (s.kind === "unknown") {
    const silence = s.timedOut === true
      ? "The API server did not answer in time"
      : "No answer reached this page";
    return statusRegion(
      "unknown",
      "<p>" + silence + ", so " + unknownOutcome(who, patch, kind, name, subject_) + "</p>" +
        errorBlock(error, false),
    );
  }
  if (s.kind === "conflict" && !patch) {
    const existing = error.existing || {};
    const differences = Array.isArray(error.differences) ? error.differences : [];
    return statusRegion(
      "failed",
      "<p>" + kind + " " + esc(existing.name || who.name) + " already exists with different " +
        "content" + (existing.uid ? " (uid " + esc(existing.uid) + ")" : "") + "; nothing was " +
        "changed." + keptClause(who) +
        (differences.length > 0 ? " It differs at: " + differences.map((d) => esc(d)).join(", ") + "." : "") +
        "</p>" + errorBlock(error, false),
    );
  }
  if (s.kind === "invalid") {
    return statusRegion(
      "failed",
      "<p>" + subject_ + (patch
        ? " was not changed: the API server refused the change."
        : " was not created: fix the fields marked below." + kept) + "</p>" +
        extra + (typeof error.status === "number" ? errorBlock(error, false) : ""),
    );
  }
  if (s.kind === "refused") {
    return statusRegion(
      "failed",
      "<p>Nothing was sent: " + esc(error.message) + "</p>",
    );
  }
  return statusRegion(
    "failed",
    "<p>The API server refused " + (patch ? "the change to " : "") + subject_ + "." +
      (patch ? " Nothing was changed." : kept) + "</p>" + extra + errorBlock(error, false),
  );
}

/** What an unknown outcome leaves undecided, and what repeating the request
 *  would actually do. One sentence per verb, each true of its own request. */
function unknownOutcome(who, patch, kind, name, subject_) {
  // AND FOR A ROUTE WHOSE IDEMPOTENCE IS A KEY, THE SENTENCE IS ABOUT THE KEY
  // (review F4). "It reuses the name" is true of every create this page made
  // until D1 W7 and is false of a manual `Backup`: the product API derives
  // that name itself, and what makes the resend safe is the `Idempotency-Key`
  // the click is holding. Rendering the name sentence there printed an empty
  // name AND told an operator the wrong reason it was safe to click again.
  if (who.idempotencyKey === true) {
    return (
      "whether " + subject_ + " was created is unknown." + keptClause(who) + " Submitting again " +
      "is safe, and it is the RIGHT thing to do: it resends the idempotency key this click is " +
      "holding, and the API answers a repeat of that key with the run the first request made " +
      "rather than starting a second one."
    );
  }
  if (patch) {
    return (
      "whether " + subject_ + " was changed is unknown. Nothing was created either " +
      "way: the request sets " + esc(who.field) + " to " + esc(String(who.value)) + " on an " +
      "object that already exists. Sending it again sets the same field to the same value, so " +
      "it either makes the change or finds it already made."
    );
  }
  if (who.resubmits === false) {
    return (
      "whether " + subject_ + " was created is unknown. The values on this page have " +
      "changed since it was sent, so submitting now is a DIFFERENT request under a different " +
      "name and would not settle this one; " + name + " stays unknown until it is opened."
    );
  }
  return (
    "whether " + subject_ + " was created is unknown." + keptClause(who) + " Submitting " +
    "again is safe: it reuses the name " + name + ", and an object that already exists with " +
    "exactly this content is recognised instead of duplicated."
  );
}

/** WHAT A CREDENTIAL FORM'S FAILURE MAY SAY ABOUT WHAT IT KEPT.
 *
 *  "Your input is kept" is true of every form in this tree except the two that
 *  take a credential, and on those it is the exact opposite of what happened:
 *  the draft allowlist drops every credential value, so the re-render an
 *  operator is reading has just emptied the boxes the sentence is about. The
 *  observed failure (review F3) is an operator reading "nothing was changed.
 *  Your input is kept", pressing the button again, and sending an empty
 *  credential -- which fails closed at the API, but only because the API is
 *  careful, not because the page was honest.
 *
 *  A form declares `clearsCredentials: true` on its subject and gets the true
 *  sentence instead. It is a property of the FORM and not of the error, so it
 *  is on the subject beside `kind` and `name`. */
export const CREDENTIALS_CLEARED_CLAUSE =
  " What you typed is kept, except the credential fields: those are cleared on every render and " +
  "are never kept anywhere, so type them again.";

function keptClause(who) {
  return (who || {}).clearsCredentials === true
    ? CREDENTIALS_CLEARED_CLAUSE
    : " Your input is kept.";
}

function statusRegion(phase, body) {
  const role = phase === "failed" || phase === "unknown" ? "alert" : "status";
  return (
    "<div class=\"mutation-status mutation-" + phase + "\" role=\"" + role + "\">" + body + "</div>"
  );
}

/** The evidence block: WHERE the signed document is, and WHAT THE CONTROLLER
 *  RECORDED about it. Nothing here re-derives a verdict.
 *
 *  Three fixed rules live in it:
 *   - every key is shown as a key, so a reader knows to fetch it;
 *   - `immutable` renders as [`IMMUTABLE_LINE`] and never as a tick;
 *   - the recorded `verification` block is printed field by field, as
 *     recorded, including `detail` -- which is where an `Invalid` says why. */
export function evidenceBlock(evidence) {
  const e = evidence || {};
  const v = e.verification || {};
  const rows = [];
  if (typeof e.receiptKey === "string") {
    rows.push(["receipt key", "<code>" + esc(e.receiptKey) + "</code>"]);
  }
  if (typeof e.receiptSha256 === "string") {
    rows.push(["receipt sha256", "<code>" + esc(e.receiptSha256) + "</code>"]);
  }
  if (typeof e.scorecardKey === "string") {
    rows.push(["scorecard key", "<code>" + esc(e.scorecardKey) + "</code>"]);
  }
  if (typeof e.scorecardSha256 === "string") {
    rows.push(["scorecard sha256", "<code>" + esc(e.scorecardSha256) + "</code>"]);
  }
  if (typeof e.sidecarKey === "string") {
    rows.push(["sidecar key", "<code>" + esc(e.sidecarKey) + "</code>"]);
  }
  if (typeof e.offsetReportKey === "string") {
    rows.push(["offset report key", "<code>" + esc(e.offsetReportKey) + "</code>"]);
  }
  rows.push(["recorded result", cell(v.result)]);
  rows.push(["matched key id", cell(v.matchedKeyId)]);
  rows.push(["payload type", cell(v.payloadType)]);
  rows.push(["verified at", cell(v.verifiedAt)]);
  rows.push(["detail", cell(v.detail)]);
  return (
    "<section class=\"evidence\"><h3>Evidence</h3>" +
    facts(rows) +
    "<p class=\"immutable\">" +
    IMMUTABLE_LINE +
    "</p></section>"
  );
}

// ===========================================================================
// D1 W7 (PLAT-04.2, PLAT-05.1, PLAT-06.2, PLAT-09.2): cadence, revision and
// trigger, in the words the controller wrote
// ===========================================================================
//
// FOUR RULES LIVE IN THIS SECTION, AND EACH ONE IS A SENTENCE THIS PAGE
// REFUSES TO WRITE.
//
//   1. NOTHING HERE EVALUATES CRON. The instants below came from the
//      controller (`status.nextRuns`) or from `GET /api/v1/cadence-previews`,
//      which is the same engine over a draft. A second implementation in a
//      browser is a second opinion about when a backup runs, and D1 section 4.4
//      forbids one in as many words.
//   2. AN ADJUSTMENT IS NAMED, NEVER SMOOTHED. A fixed local time inside a
//      repeated hour FIRES TWICE, and both rows are shown with their offsets:
//      hiding the second would make the page disagree with the cluster about
//      how many backups happen that night.
//   3. A REVISION THAT WAS NOT RECORDED IS NOT REVISION ZERO. Every run frozen
//      before PLAT-05.1 carries no `scheduleRef.generation`, and so does every
//      run in a console whose API does not publish one.
//   4. STALENESS IS `nextRuns[0].at` IN THE PAST, AND NOTHING ELSE.
//      `status.policy.evaluatedAt` is when the status last MOVED -- the
//      controller writes nothing when nothing changed -- so comparing it with
//      the requeue interval would label every healthy schedule stale. D1
//      section 4.9's amendment says this in the CRD's own description.

/** The three DST markers, keyed by the controller's own PascalCase, with what
 *  each one means for the instant it is on.
 *
 *  THE KEYS ARE THE CONTRACT AND THE VALUES ARE THE PROSE. `ui/contract.js`
 *  refuses a marker outside this set at the decoder, so a word this build does
 *  not know never reaches here; what reaches here is always one of three, and
 *  each gets a sentence rather than a symbol. */
export const ADJUSTMENT_WORDS = Object.freeze({
  NonexistentLocalTimeShifted:
    "this local time does not exist on that date (the clocks go forward), so the run is at the " +
    "end of the gap",
  RepeatedLocalTimeFirst:
    "this local time happens twice on that date (the clocks go back); this is the FIRST " +
    "occurrence, and the second one below is a separate run",
  RepeatedLocalTimeSecond:
    "this local time happens twice on that date; this is the SECOND occurrence, and it is a " +
    "separate run from the first",
});

/** The note a preview or a saved policy carries when its zone is UTC because
 *  no zone was named -- which is what every schedule written before PLAT-04.2
 *  carries, and what an empty time-zone field means. */
export const UTC_FALLBACK_NOTE =
  "No time zone is set, so the cron fields are read in UTC. That is what an absent timeZone " +
  "has always meant and it is not a default this page chose: the local times below are UTC " +
  "times. Name a zone to have the fields read as local wall-clock time there.";

/** What an empty list of next runs is, and what it is not. */
export const NO_NEXT_RUNS_SENTENCE =
  "No further firing. A suspended schedule, a policy the controller refused, and a cadence " +
  "that has genuinely run out all look like this list, and the Ready condition above says " +
  "which one it is -- an empty list is an answer, not a failure to compute one.";

/** What an out-of-date preview is, said without accusing the controller of
 *  being down. */
export const STALE_NEXT_RUNS_SENTENCE =
  "The first firing below is in the past, so the controller has not rewritten these previews " +
  "since it came due. It rewrites them when the generation changes or when the first entry " +
  "passes, so this is the one staleness signal a schedule has; status.policy.evaluatedAt is " +
  "NOT one, because nothing is written when nothing changed.";

/** One firing, as a row: the UTC instant, the same instant in the schedule's
 *  own zone with its offset, and the DST marker when there is one. */
export function nextRunRow(run) {
  const r = run || {};
  const marker = typeof r.adjustment === "string" && ADJUSTMENT_WORDS[r.adjustment] !== undefined
    ? badge("pending", r.adjustment)
    : "";
  return [cell(r.at), "<code>" + cell(r.localTime) + "</code>", marker];
}

/** The next-run panel, over a saved schedule's `status.nextRuns` or a draft's
 *  preview `runs`. ONE renderer, because they are one shape.
 *
 *  `view` is `{runs, timeZone, tzdb, heading, now}`. `runs` absent (`null` or
 *  `undefined`) is "this build has not computed any", which is NOT the same as
 *  an empty array and is rendered as its own sentence. `now` is epoch
 *  milliseconds and is OPTIONAL: with no clock the staleness arm renders
 *  nothing, which keeps every other view in this tree computable without
 *  reading one. */
export function nextRunsPanel(view) {
  const v = view || {};
  const runs = v.runs;
  const zone = typeof v.timeZone === "string" && v.timeZone.length > 0 ? v.timeZone : "UTC";
  const heading = typeof v.heading === "string" ? v.heading : "Next runs";
  if (runs === null || runs === undefined) {
    return (
      "<section class=\"next-runs\" data-next-runs=\"absent\"><h4>" + esc(heading) + "</h4>" +
      "<p class=\"note\">This build has not computed the next firings for this schedule. " +
      "The browser will not compute them either: cron is evaluated by the controller and by " +
      "the cadence-preview route, and a second implementation here could disagree with " +
      "both.</p></section>"
    );
  }
  const list = Array.isArray(runs) ? runs : [];
  const stale = list.length > 0 && typeof v.now === "number" &&
    epochMs(list[0].at) !== null && epochMs(list[0].at) < v.now;
  return (
    "<section class=\"next-runs\" data-next-runs=\"" + String(list.length) + "\">" +
    "<h4>" + esc(heading) + "</h4>" +
    "<p class=\"note\">Read in <code>" + esc(zone) + "</code>" +
    (typeof v.tzdb === "string" && v.tzdb.length > 0
      ? ", against <code>" + esc(v.tzdb) + "</code> compiled into the controller and the API"
      : "") +
    ". The slot identity is always the UTC instant, which is why the names stay unique and " +
    "monotonic whatever the zone.</p>" +
    (zone === "UTC" ? "<p class=\"note\" data-utc-fallback=\"1\">" + esc(UTC_FALLBACK_NOTE) +
      "</p>" : "") +
    (stale ? "<p class=\"note\" data-stale=\"1\">" + badge("unverified", "out of date") + " " +
      esc(STALE_NEXT_RUNS_SENTENCE) + "</p>" : "") +
    table(["AT (UTC)", "LOCAL TIME", "DST"], list.map(nextRunRow), NO_NEXT_RUNS_SENTENCE) +
    (list.filter((r) => (r || {}).adjustment !== undefined && (r || {}).adjustment !== null)
      .map((r) => "<p class=\"note\" data-adjustment=\"" + esc(String(r.adjustment)) + "\">" +
        "<code>" + esc(String(r.at)) + "</code>: " +
        esc(ADJUSTMENT_WORDS[r.adjustment] || "") + "</p>").join("")) +
    "</section>"
  );
}

/** What "no trigger" means on a run, said rather than guessed. */
export const NO_TRIGGER_SENTENCE =
  "trigger not recorded (frozen before PLAT-05.1, or not published by this API)";

/** What "no revision" means, said the same way. */
export const NO_REVISION_SENTENCE =
  "revision not recorded (frozen before PLAT-05.1, or not published by this API)";

/** WHICH KIND OF RUN THIS IS, from `spec.trigger` and from nothing else.
 *
 *  `maxRetries` is the schedule's own `spec.retry.maxRetries` when the caller
 *  has the schedule at hand, and is what turns "attempt 2" into "Retry 2 of
 *  3". A caller that does NOT have it gets "Retry, attempt 2": the ceiling is
 *  a property of the schedule's CURRENT policy and a run carries no copy of
 *  it, so printing a guess would be printing a number this page made up.
 *
 *  `spec.triggeredBy` is NOT read here. That older field says `manual` or
 *  `schedule` and cannot tell a catch-up or a retry from an ordinary slot, so
 *  reading it as a kind would turn three facts into one. An absent trigger is
 *  [`NO_TRIGGER_SENTENCE`]. */
export function triggerLabel(trigger, maxRetries) {
  const t = trigger || {};
  if (typeof t.kind !== "string" || t.kind.length === 0) {
    return "";
  }
  if (t.kind !== "Retry") {
    return t.kind;
  }
  const attempt = typeof t.attempt === "number" ? String(t.attempt) : "?";
  return typeof maxRetries === "number" && maxRetries > 0
    ? "Retry " + attempt + " of " + String(maxRetries)
    : "Retry, attempt " + attempt;
}

/** The trigger as a badge, or the sentence that says there is none. */
export function triggerBadge(trigger, maxRetries) {
  const words = triggerLabel(trigger, maxRetries);
  if (words.length === 0) {
    return "<span class=\"note\">" + esc(NO_TRIGGER_SENTENCE) + "</span>";
  }
  const kind = String((trigger || {}).kind).toLowerCase();
  return badge("trigger-" + kind.replace(/[^a-z0-9]+/g, "-"), words);
}

/** WHICH REVISION A RUN FROZE: `revision g7 - policy sha256:ab12...`.
 *
 *  A SUSPEND FLIP MOVES THE GENERATION AND NOT THE DIGEST, and printing both
 *  is what makes that visible: `generation` counts every spec change including
 *  `suspend`, while `runPolicySha256` is over what a RUN does, so two runs of
 *  different generations with the same digest did the same thing. */
export function revisionLine(ref) {
  const r = ref || {};
  const parts = [];
  if (typeof r.generation === "number") {
    parts.push("revision g" + String(r.generation));
  }
  if (typeof r.runPolicySha256 === "string" && r.runPolicySha256.length > 0) {
    parts.push("policy " + r.runPolicySha256);
  }
  if (parts.length === 0) {
    return "<span class=\"note\">" + esc(NO_REVISION_SENTENCE) + "</span>";
  }
  return "<code>" + esc(parts.join(" \u00b7 ")) + "</code>";
}

// ===========================================================================
// D3 (PLAT-12, PLAT-14, PLAT-15, PLAT-16, PLAT-19): the words for a durable
// operation, protection health, a recovery catalog, retention enforcement and
// key trust
// ===========================================================================
//
// THE SAME RULE AS EVERY SECTION ABOVE IT, AND IT IS THE ONLY RULE HERE: a
// fixed sentence of OURS is written down once, in this file, and rendered
// verbatim. NO VERDICT IS COMPUTED HERE. Every function below is a lookup from
// a value the cluster wrote to a sentence about that value; none of them reads
// a clock, compares two instants, or decides whether something is fresh, valid
// or safe. The one place that looks like an exception -- `evaluationWord` --
// is handed the decision as an argument and only picks the word for it.
//
// FOUR CLAIMS THIS SECTION EXISTS TO STOP THE PAGE MAKING.
//
//   1. "unknown is valid". `ui/pages/keys.js` used to print `valid` for any
//      key not in `status.expiredKeyIds`, INCLUDING when the object carried no
//      status at all. D3 section 7.7: an unevaluated or stale evaluation reads
//      `unknown`, and `valid`/`expired` are only ever rendered for a fresh one.
//   2. "verified", for evidence signed by a key this installation does not
//      accept. D3 section 7.4 adds `Untrusted` beside `Invalid` and
//      `NotAttempted`, and the three are three different claims -- about the
//      SIGNER, about the DOCUMENT and about the CONTROLLER. A single word for
//      all three destroys the distinction that says what to go and fix.
//   3. "exhaustive". A restore's record check is a SAMPLE. `verificationScope`
//      carries the sampled counts and [`verificationScopeSentence`] renders
//      them with the words "this is a sampled check, not an exhaustive
//      comparison" -- never "complete", which is a level that does not exist
//      in v1 (D3 section 2.5).
//   4. "protected", for a policy Logweir could not evaluate. `health: Unknown`
//      is rendered as unknown, and the `Protected` condition's `Unknown` is
//      never rounded to `False`: "Logweir checked and you are not protected"
//      is a claim nothing made.

/** The suffix a green badge carries when the evidence verified under a key
 *  that has since been retired (`trust.basis: Historical`).
 *
 *  IT IS A PASS AND NOT A WARNING. D3 section 7.6's rotation procedure is
 *  supposed to produce exactly this state: the key was valid when it signed,
 *  the archive still verifies, and the public material can never be edited out
 *  of the policy. The qualifier says which key and when, so a reader is not
 *  left wondering why a retired id appears on a green row. */
export const HISTORICAL_SUFFIX = " (signed before that key was retired)";

/** The evidence cases that are NOT green, each named by what it is a claim
 *  about. The key is `status.evidence.verification.result` (plus the two
 *  synthetic rows below); the value is appended to [`UNVERIFIED`] so the badge
 *  still carries the one word every older surface reads, and still says which
 *  of the cases this is.
 *
 *  `RecordedBeforeRevocation` IS NOT GREEN AND IS NOT SILENT. It is the case
 *  where a compromise-revoked key signed evidence a controller had already
 *  observed before the revocation took effect: the observation is real, and it
 *  is not a substitute for a signature this installation still trusts. */
export const VERIFICATION_CASES = Object.freeze({
  Untrusted:
    "untrusted signer -- the bytes are authentic and this installation does not accept the key",
  Invalid: "invalid -- the signature did not verify, or a digest did not match",
  NotAttempted: "not attempted -- the controller could not check this document",
  RecordedBeforeRevocation:
    "recorded before revocation -- a compromised key signed it, and a controller had seen it " +
    "before the revocation took effect",
  NotRecorded: "no verification was recorded for this run",
  RunNotSucceeded: "the document verified and the run itself did not succeed",
});

/** THE TWO TRUST STATES A GREEN BADGE MAY CARRY, in the CONSOLE's vocabulary.
 *
 *  `OperationTrust.state` is D3 section 2.5's own word and it arrives ALREADY
 *  COMBINED: `logweir-api` computed it from the controller's `result` and its
 *  `trust.basis`, which is exactly the normalization a console asks an API
 *  for. So in console mode the page reads the word instead of re-deriving it,
 *  and the two-document rule stays honest: legacy mode has no such word and
 *  keeps the page's own rule over `result` + PascalCase `basis`. */
export const GREEN_TRUST_STATES = Object.freeze(["verified", "verifiedHistorical"]);

/** Each non-green trust state, named by what it is a claim ABOUT -- the same
 *  distinction [`VERIFICATION_CASES`] draws for a custom resource, in the
 *  words the API publishes. */
export const TRUST_STATE_CASES = Object.freeze({
  untrusted:
    "untrusted signer -- the bytes are authentic and this installation does not accept the key",
  invalid: "invalid -- the signature did not verify, or a digest did not match",
  notAttempted: "not attempted -- the controller could not check this document",
  notApplicable: "not applicable -- this run writes no signed document",
  pending: "pending -- the run has not finished, or its verdict is not written yet",
});

/** Which non-green case a console trust state is, as a word. A state this
 *  build does not know renders AS ITSELF rather than as a neighbour: the
 *  vocabulary is closed on the wire and a word outside it is still a word the
 *  API chose to say. */
export function trustStateCase(state, runSucceeded) {
  if (GREEN_TRUST_STATES.indexOf(state) !== -1) {
    return runSucceeded === true ? "" : VERIFICATION_CASES.RunNotSucceeded;
  }
  const named = TRUST_STATE_CASES[state];
  if (typeof named === "string") {
    return named;
  }
  return typeof state === "string" && state.length > 0
    ? state
    : VERIFICATION_CASES.NotRecorded;
}

/** The caption a console badge that is not green carries. */
export function unverifiedTrustCaption(state, runSucceeded) {
  const said = trustStateCase(state, runSucceeded);
  return said.length === 0 ? UNVERIFIED : UNVERIFIED + ": " + said;
}

/** THE BASIS THAT MEANS "NO TRUST EVALUATION WAS RECORDED", AND WHY IT IS NOT
 *  A DOWNGRADE.
 *
 *  D3 section 12 spells the absent-field rule for this exact block: "`trust`
 *  absent -> `basis: None` and the badge uses the pre-existing rule". So the
 *  STRING `"None"` and an ABSENT `trust` block are the same fact said two
 *  ways -- one by a custom resource an older controller wrote, one by a DTO
 *  that fills the block in for it -- and both mean the trust layer has nothing
 *  to say about this verdict, NOT that the verdict is worse.
 *
 *  Reading it as a downgrade puts `unverified: no verification was recorded` on
 *  every archive an upgraded cluster carries, while `result: Valid`,
 *  `verifiedAt` and `matchedKeyId` all sit beside it saying otherwise. That is
 *  a caption this page would be inventing, about a controller that recorded a
 *  verdict, and it is the review's own finding F1.
 *
 *  It is a CONSTANT and not a literal in three files because the name `None`
 *  collides with the "no verdict at all" case one line above, and that
 *  collision is how the defect got written in the first place. */
export const TRUST_BASIS_NOT_OBSERVED = "None";

/** The `trust.basis` values a green badge is allowed to carry. An object
 *  written by an older controller carries no `trust` block at all, and its
 *  absence is NOT a downgrade: the block is additive (D3 section 12). */
export const GREEN_BASES = Object.freeze(["Current", "Historical"]);

/** Which non-green case a recorded verification is, as a word.
 *
 *  A LOOKUP AND NOT A JUDGEMENT. It reads `result` and, for the one value that
 *  splits, `trust.basis`; every string outside [`VERIFICATION_CASES`] falls to
 *  `None`, which says "no verification was recorded" rather than inventing a
 *  name for a value this build does not know. */
export function verificationCase(verification, runSucceeded) {
  const v = verification || {};
  const trust = v.trust || {};
  if (v.result === "Valid") {
    if (trust.basis === "RecordedBeforeRevocation") {
      return VERIFICATION_CASES.RecordedBeforeRevocation;
    }
    if (!basisAllowsGreen(trust.basis)) {
      return VERIFICATION_CASES.NotRecorded;
    }
    return runSucceeded === true ? "" : VERIFICATION_CASES.RunNotSucceeded;
  }
  const named = VERIFICATION_CASES[v.result];
  return typeof named === "string" ? named : VERIFICATION_CASES.NotRecorded;
}

/** Whether a recorded `trust.basis` leaves a `Valid` verdict green.
 *
 *  THREE ANSWERS COLLAPSE TO YES, and they are three different facts: the
 *  trust layer said `Current`, it said `Historical`, or it said nothing at all
 *  -- as an absent block, or as [`TRUST_BASIS_NOT_OBSERVED`], which is the same
 *  absence spelled by a document that has to spell something. What is left is
 *  a basis this build does not know, and that one is not green, because a word
 *  this page cannot read is not a word it may treat as a pass. */
export function basisAllowsGreen(basis) {
  if (typeof basis !== "string" || basis === TRUST_BASIS_NOT_OBSERVED) {
    return true;
  }
  return GREEN_BASES.indexOf(basis) !== -1;
}

/** The caption a badge that is not green carries: the one word every older
 *  reader looks for, and the case it actually is. */
export function unverifiedCaption(verification, runSucceeded) {
  const said = verificationCase(verification, runSucceeded);
  return said.length === 0 ? UNVERIFIED : UNVERIFIED + ": " + said;
}

/** The sentence beside every restore result, from `verificationScope`
 *  (D3 section 2.5 and section 3.5).
 *
 *  THE COUNTS ARE LABELLED EXACTLY, and the last clause is not optional: a
 *  sampled comparison presented without it reads as a full one. `complete`
 *  does not exist as a level in v1 and this function cannot render it. */
export function verificationScopeSentence(scope) {
  const s = scope || null;
  if (s === null || typeof s !== "object") {
    return "No verification scope was recorded for this run, so how much of it was compared is " +
      "not known here. An absent scope is not a complete one.";
  }
  if (s.level === "none") {
    return "No record check ran for this restore: the run's own evidence attests what was " +
      "written and by whom, and no records were compared.";
  }
  if (typeof s.recordsSampled !== "number" && typeof s.recordsSampledMatching !== "number") {
    return "This restore's record check ran at level " + String(s.level) +
      " and recorded no sampled counts, so how many records were compared is not known here. " +
      "Whatever it compared, it was a sample: no level in this version performs an exhaustive " +
      "comparison.";
  }
  const sampled = typeof s.recordsSampled === "number" ? String(s.recordsSampled) : ABSENT;
  const matching = typeof s.recordsSampledMatching === "number"
    ? String(s.recordsSampledMatching)
    : ABSENT;
  const expected = typeof s.recordsExpected === "number" ? String(s.recordsExpected) : ABSENT;
  const degraded = s.level === "degraded"
    ? " The comparison was degraded: the records were consumed and counted, not compared " +
      "byte for byte."
    : "";
  return matching + " of " + sampled + " sampled records matched byte-for-byte; " + expected +
    " records were expected in the sampled window; this is a sampled check, not an exhaustive " +
    "comparison." + degraded;
}

/** D3 section 2.5's TABLE from the custom resource's own integrity vocabulary to the
 *  scope level the API publishes. A LOOKUP AND NOT A JUDGEMENT: the three
 *  values on the left are `Restore.status.integrity.level`'s whole vocabulary
 *  and the three on the right are `verificationScope.level`'s.
 *
 *  It exists because legacy mode has no `logweir-api` to do the mapping and a
 *  page that showed no scope at all there would be showing a result with
 *  nothing beside it saying how much of it was checked -- which is the reading
 *  the sentence exists to prevent. There is no fourth row, and `complete` is
 *  not a value on either side. */
export const SCOPE_LEVEL_OF_INTEGRITY = Object.freeze({
  "byte-fingerprint": "sampled",
  "consume-only": "degraded",
  "not-attempted": "none",
});

/** The completion guidance for a terminal Restore, keyed by
 *  `spec.target.mode`. D3 section 3.5: fixed sentences owned by this file,
 *  never server-authored prose. */
export const COMPLETION_GUIDANCE = Object.freeze({
  newTopic:
    "Consumers are not moved. Logweir wrote nothing to the original topics. Consumer group " +
    "offsets were not restored; the engine's offset report is informational only. Point " +
    "applications at the new names and validate reads before retiring anything.",
  scratch:
    "These topics are a rehearsal and are deleted by teardown; never point an application at " +
    "them.",
});

/** What a Restore's target mode MEANS, from the moment the run exists.
 *
 *  SEPARATE FROM [`COMPLETION_GUIDANCE`], AND EARLIER THAN IT. The completion
 *  guidance is about what a run PRODUCED and belongs beside its scorecard;
 *  this is about what the run IS, and the published DTO carries `targetMode`
 *  at the top level for exactly that reason -- "a rehearsal is a rehearsal
 *  before its scorecard exists", in the document's own words. A console that
 *  read the mode only out of the completion panel could not label a Restore
 *  that had not finished, which is precisely the case where knowing it is a
 *  rehearsal matters most. */
export const TARGET_MODE_MEANING = Object.freeze({
  newTopic:
    "This run restores into NEW topics. Logweir writes nothing to the original topics and moves " +
    "no consumer; the original data is not touched by it.",
  scratch:
    "This run is a REHEARSAL into a scratch cluster. Its topics are deleted by teardown when it " +
    "finishes, so nothing an application depends on may be pointed at them.",
});

/** The three evaluation words a keys view may print, and the ONE that is
 *  printed whenever the freshness question has no answer. */
export const EVALUATION_UNKNOWN = "unknown";

/** Why an evaluation reads `unknown`, by the reason the caller established.
 *  D3 section 7.7's three causes, each said in its own words. */
export const EVALUATION_UNKNOWN_REASONS = Object.freeze({
  NoStatus:
    "this object carries no status at all, so nothing has evaluated these keys yet",
  GenerationBehind:
    "status.observedGeneration is behind metadata.generation, so these verdicts were computed " +
    "from an earlier spec",
  Stale:
    "status.evaluatedAt is older than the freshness window, measured against the server's own " +
    "clock and never the browser's",
  NoServerClock:
    "no answer this page has received carried a server instant to measure freshness against, " +
    "and the browser's own clock is not one the cluster ever saw",
  NoVerdict: "this key has no verdict in status.keys[]",
});

/** The word an evaluation column prints. `decision` is what the caller
 *  established from the object's own fields; `state` is the verdict.
 *
 *  THE BROWSER'S CLOCK IS NEVER AN INPUT HERE. Freshness is decided by the
 *  caller from a SERVER instant, and this function only picks the word for the
 *  decision it was handed -- which is what keeps "a page renders no verdict
 *  from a clock the cluster never saw" true of this column. */
export function evaluationWord(fresh, state) {
  if (fresh !== true) {
    return EVALUATION_UNKNOWN;
  }
  return typeof state === "string" && state.length > 0 ? state : EVALUATION_UNKNOWN;
}

/** The sentence a keys view carries above the table when the evaluation is
 *  not fresh. */
export const UNKNOWN_IS_NOT_VALID_SENTENCE =
  "An evaluation this page could not confirm is fresh reads `unknown`, never `valid`. " +
  "`valid` and `expired` are rendered only for a verdict the controller computed from the " +
  "generation on this object, recently enough to still be about it.";

/** The fingerprint command an operator runs OUT OF BAND against a public key,
 *  and the whole of what this product offers for establishing trust in one.
 *  There is NO one-click trust anywhere in this tree (D3 section 5.5 step 3). */
export const NO_ONE_CLICK_TRUST_SENTENCE =
  "This page offers no control that adds a key to a TrustPolicy. A key found beside an archive " +
  "is a claim, never trusted by proximity: compare the fingerprint below with its holder out of " +
  "band, then have an administrator add it with kubectl.";

/** The four retention enforcement modes, as the sentence each one earns.
 *
 *  KEYED BY `status.enforcement` -- WHAT IS ACTUALLY HAPPENING -- and not by
 *  `spec.mode`, which is what was ASKED FOR. A policy in `mode: Enforce` whose
 *  destination will not resolve reports `RecommendationOnly`, and the panel
 *  must say what is happening rather than what was requested. */
export const ENFORCEMENT_SENTENCES = Object.freeze({
  RecommendationOnly:
    "Logweir reports what would be removed under this policy and removes nothing.",
  LogweirWorker:
    "An isolated Logweir retention worker deletes archive objects under this policy, with its " +
    "own delete-capable credential that this controller never reads.",
  ExternalLifecycleDeclared:
    "Deletion at this destination is performed by your bucket lifecycle rule; Logweir reports " +
    "and cannot protect individual points here.",
});

/** The sentence an `Enforce` policy carries, verbatim, beside its plan.
 *  Deleting an archive object is not reversible and the panel says so before
 *  an administrator approves a digest, not after. */
export const IRREVERSIBLE_SENTENCE =
  "Approving a plan authorises deletion of the archive objects it names. Deletion is not " +
  "reversible: the points it removes cannot be restored from afterwards, and the only record " +
  "that survives is the tombstone and the retention record under logweir/.";

/** `status.guarantees`'s three values as words. `ProviderEnforcedUnverified`
 *  is the one that must never read as enforcement BY LOGWEIR: it means the
 *  operator declared a provider mechanism and Logweir cannot read it back. */
export const GUARANTEE_WORDS = Object.freeze({
  LogweirEnforced: "enforced by Logweir",
  ProviderEnforcedUnverified: "declared by your provider; Logweir cannot verify it",
  NotEnforced: "not enforced",
});

/** The sentence an `EnforcementDegraded=True` policy carries. */
export const ENFORCEMENT_DEGRADED_SENTENCE =
  "EnforcementDegraded: three consecutive enforcement runs failed, so this policy has stopped " +
  "scheduling them until its spec changes. Nothing was deleted by the failed runs beyond what " +
  "their own records name.";

/** The sentence the legacy schedule retention report carries once a
 *  RetentionPolicy covers the same destination. */
export const SUPERSEDED_SENTENCE =
  "A RetentionPolicy now covers this schedule's destination, and it is the evaluation that " +
  "counts. This report is the legacy per-schedule one and is kept for continuity.";

/** Health as a badge kind, by `ProtectionPolicy.status.health`. STRUCTURAL:
 *  the caption is the recorded word and the kind is a class suffix. */
export const HEALTH_KINDS = Object.freeze({
  Healthy: "green",
  AtRisk: "warn",
  Stale: "unverified",
  Unprotected: "unverified",
  Unknown: "flat",
});

/** `ProtectionPolicy.status.health` as a badge. An absent health is [`ABSENT`]
 *  and never a badge with no word. */
export function healthBadge(health) {
  if (typeof health !== "string" || health.length === 0) {
    return ABSENT;
  }
  const kind = HEALTH_KINDS[health];
  return badge(typeof kind === "string" ? kind : "flat", health);
}

/** The sentence beside protection health that keeps the two health questions
 *  apart. D3 section 3.2 is explicit that they are never collapsed. */
export const TWO_HEALTHS_SENTENCE =
  "Schedule health and protection health are two different questions and this page never " +
  "collapses them: an enabled, healthy schedule can still have no recent recoverable backup.";

/** What `recoveryPointAt` and `newestRecordAt` each are. They are different
 *  instants and D3 section 3.2 requires them labelled separately. */
export const TWO_INSTANTS_SENTENCE =
  "The recovery point instant is when the capture STARTED, which is what the objective is " +
  "measured against. The newest archived record is the last record instant the archive covers. " +
  "They are different numbers and an idle topic makes the second one look old for a reason " +
  "that is not a gap in protection.";

/** A notification is not evidence, and a delivery failure is not a run
 *  failure. Both halves are rendered beside the alert ledger. */
export const NOTIFICATION_NOT_EVIDENCE_SENTENCE =
  "An alert is a notification and never evidence: the event document is unsigned, and a sink " +
  "that refused it changes nothing about a backup's own recorded result.";

/** The two axes a catalog entry carries, said before the table that shows
 *  them, because "available" and "verified" are routinely read as one word. */
export const TWO_AXES_SENTENCE =
  "Availability and verification are separate axes. Availability is whether the archive can " +
  "still serve this point; verification is whether its receipt verifies under a key this " +
  "installation accepts. A point is selectable for a restore only when it is Available AND " +
  "Verified or VerifiedHistorical, and that judgement is the catalog's own `selectable` field, " +
  "not one this page recomputes.";

/** The sentence a truncated catalog view carries. The window is bounded and
 *  the durable truth is in object storage; nothing is silently hidden. */
export const CATALOG_WINDOW_SENTENCE =
  "This view is a bounded WINDOW over the durable catalog in object storage. Points beyond it " +
  "are counted and histogrammed here and listed by `logweir catalog list` against the archive " +
  "itself; none of them is hidden and none of them is deleted.";

/** The sentence an expired or never-synced catalog view carries. */
export const VIEW_EXPIRED_SENTENCE =
  "The Kubernetes view of this catalog has expired or has never been synced. That is a missing " +
  "VIEW and not a missing archive: the points are still in object storage. Sync the catalog to " +
  "get the window back.";

/** A normalized operation state as a badge. STRUCTURAL, like [`phaseBadge`]:
 *  the caption is the API's own word and nothing here decides what it means.
 *
 *  TEN WORDS, FOUR OF WHICH ARE PHASES. `pending`, `running`, `succeeded` and
 *  `failed` are the resource's own; `queued`, `preparing`, `verifying`,
 *  `refused`, `cancelled` and `unknown` are distinctions `logweir-api` draws
 *  that the resource does not record, and the page shows the API's word rather
 *  than rounding it to a phase the controller never wrote. */
export function stateBadge(state) {
  if (typeof state !== "string" || state.length === 0) {
    return ABSENT;
  }
  return badge("state-" + state.toLowerCase().replace(/[^a-z0-9]+/g, "-"), state);
}

/** The sentence an `unknown` operation state carries: what the API could not
 *  establish, and that it is not a verdict about the run. */
export const STATE_UNKNOWN_SENTENCE =
  "`unknown` is what this operation's status could not establish -- an active stage whose last " +
  "observation is old, a phase this build does not know, or no status at all yet. It is not a " +
  "statement that the run failed.";

/** The sentence an operation view carries about what aborting a watch does.
 *  D3 section 2.6: a watch is a read, and leaving a page never cancels work
 *  the cluster has already accepted. */
export const WATCH_SENTENCE =
  "This view follows the operation while it is open and stops when it reaches a terminal state " +
  "or when you navigate away. Closing it cancels the READ and never the run.";

/** The sentence the legacy mode's operation view carries: which facts it is
 *  showing and which one it deliberately does not invent. */
export const LEGACY_OPERATION_SENTENCE =
  "This mode reads the custom resource directly, so it shows the controller's own " +
  "`status.progress` -- stage, reason, last transition and the diagnoses it recorded. The " +
  "normalized operation state is computed by `logweir-api` and is not available here; this page " +
  "does not compute one of its own.";

/** One diagnostic as a table row's worth of already-rendered cells. The code
 *  is the REPAIR (which Secret, which image) and the severity is the
 *  controller's; neither is reworded here. */
export function diagnosticRow(entry) {
  const d = entry || {};
  const object = d.object || {};
  return [
    "<code>" + cell(d.code) + "</code>",
    cell(d.severity),
    cell(d.message),
    cell(object.kind) + " " + cell(object.name),
    cell(d.count),
    cell(d.lastSeen),
  ];
}

/** The diagnoses table. Empty is a STATE and says so: "nothing was recorded"
 *  is not "everything is fine". */
export function diagnosticsTable(entries) {
  const list = Array.isArray(entries) ? entries : [];
  return table(
    ["CODE", "SEVERITY", "MESSAGE", "OBJECT", "COUNT", "LAST SEEN"],
    list.map(diagnosticRow),
    "The controller recorded no diagnosis for this run. That is not the same as a run with " +
      "nothing wrong: a diagnosis is written when there is a cause to write down.",
  );
}
