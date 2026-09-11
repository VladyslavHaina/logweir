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
  return el("div", { class: "error" }, [
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

/** A table. `columns` are header captions; `rows` are arrays of ALREADY
 *  RENDERED cells -- a caller that wants a badge in a cell passes the badge.
 *  A row shorter than `columns` is padded, so a missing status field cannot
 *  shift a column silently. */
export function table(columns, rows) {
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
  const empty = rows.length === 0
    ? "<tr><td class=\"empty\" colspan=\"" + columns.length + "\">no object of this kind in this namespace</td></tr>"
    : "";
  return (
    "<table class=\"grid\"><thead><tr>" +
    head +
    "</tr></thead><tbody>" +
    body +
    empty +
    "</tbody></table>"
  );
}

/** A badge. STRUCTURAL ONLY: `kind` becomes a class suffix and `text` becomes
 *  the caption. Which kind a run gets is the calling page's decision, made
 *  from the object's own recorded fields. */
export function badge(kind, text) {
  return (
    "<span class=\"badge badge-" + esc(kind) + "\">" + esc(text) + "</span>"
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
