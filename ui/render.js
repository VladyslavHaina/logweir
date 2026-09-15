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

/** What the run will do to the target BEFORE the engine starts, named in the
 *  page so an operator reads it before the plan is signed rather than out of a
 *  refusal afterwards (spec section 6.1, guard G-TS). */
export function preflightSentence(topicCount) {
  return (
    "Logweir will create " +
    String(topicCount) +
    " topics with message.timestamp.type=CreateTime and retention.ms=-1 before the " +
    "engine runs, and will refuse if the broker is LogAppendTime and rejects the override."
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
 *  `app.js` when the nodes are adopted, so the string here carries them once. */
export function table(columns, rows, empty) {
  const head = columns.map((c) => "<th scope=\"col\">" + esc(c) + "</th>").join("");
  const body = rows
    .map((row) => {
      const cells = [];
      for (let i = 0; i < columns.length; i += 1) {
        cells.push("<td>" + (row[i] === undefined ? ABSENT : row[i]) + "</td>");
      }
      return "<tr>" + cells.join("") + "</tr>";
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
 *  in words, for the object `subject` names (`{kind, name}`).
 *
 *  THE WORDS FOR A FAILURE SAY WHAT IS KNOWN. A refusal says nothing was
 *  created and the input is kept. An UNKNOWN outcome -- no answer, a timeout,
 *  a 5xx -- says exactly that, and says why submitting again is safe: the
 *  retry reuses the name, and an existing object with the same content is
 *  recognised rather than duplicated. A conflict names the object that is in
 *  the way. `unmatched` is the API server's field causes no input claimed. */
export function mutationStatus(state, subject, unmatched) {
  const s = state || {};
  const who = subject || {};
  const kind = esc(who.kind);
  const name = esc(who.name);
  if (s.phase === "pending") {
    return statusRegion(
      "pending",
      "<p>" + kind + " " + name + ": sent, waiting for the API server. Submitting again is " +
        "disabled until it answers, so one click makes one request.</p>",
    );
  }
  if (s.phase === "succeeded") {
    const result = s.result || {};
    const meta = ((result.object || {}).metadata) || {};
    const shown = esc(typeof meta.name === "string" && meta.name.length > 0 ? meta.name : who.name);
    const uid = esc(meta.uid);
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
    return statusRegion(
      "unknown",
      "<p>" + (s.timedOut === true
        ? "The API server did not answer in time"
        : "No answer reached this page") +
        ", so whether " + kind + " " + name + " was created is unknown. Your input is kept. " +
        "Submitting again is safe: it reuses the name " + name + ", and an object that already " +
        "exists with exactly this content is recognised instead of duplicated.</p>" +
        errorBlock(error, false),
    );
  }
  if (s.kind === "conflict") {
    const existing = error.existing || {};
    const differences = Array.isArray(error.differences) ? error.differences : [];
    return statusRegion(
      "failed",
      "<p>" + kind + " " + esc(existing.name || who.name) + " already exists with different " +
        "content" + (existing.uid ? " (uid " + esc(existing.uid) + ")" : "") + "; nothing was " +
        "changed. Your input is kept." +
        (differences.length > 0 ? " It differs at: " + differences.map((d) => esc(d)).join(", ") + "." : "") +
        "</p>" + errorBlock(error, false),
    );
  }
  if (s.kind === "invalid") {
    return statusRegion(
      "failed",
      "<p>" + kind + " " + name + " was not created: fix the fields marked below. Your input is " +
        "kept.</p>" + extra + (typeof error.status === "number" ? errorBlock(error, false) : ""),
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
    "<p>The API server refused " + kind + " " + name + ". Your input is kept.</p>" + extra +
      errorBlock(error, false),
  );
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
