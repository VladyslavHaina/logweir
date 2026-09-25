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

/** Replaces a node's children in one step -- AND KEEPS THE READER'S PLACE.
 *
 *  FOCUS SURVIVES A RE-RENDER (PLAT-18.2; the PLAT-10 review's LOW). Every
 *  page here re-renders by replacing its whole subtree: the restore wizard on
 *  every change, a schedule detail after every action. Replacing the node a
 *  keyboard reader is on moves their focus to the document body, so the next
 *  Tab starts again at the skip link -- the page worked for a mouse and was
 *  lost to a keyboard. So when the focused element is inside `node` and has
 *  an `id`, the element with the same `id` in the new subtree takes focus
 *  again, with its caret and selection when it is a text field, and without
 *  scrolling. An element that is gone is not replaced by a guess: focus then
 *  goes to `node` itself when it can take it (the view slot is
 *  `tabindex="-1"`), which is where a screen reader expects a re-read page to
 *  start.
 *
 *  THE PAGE STAYS WHERE IT WAS, TOO (poc-upgrade-4's P16). Emptying `node`
 *  and filling it again moved the page under the reader: the browser lays the
 *  emptied view out, or loses the scroll anchor it had chosen inside it, and
 *  the window's offset came back smaller -- restore step 5's first follow
 *  read took 282 px to 0 and left the focused status below the fold, where no
 *  later read brought it back. So the offset is read before the swap and put
 *  back after it when the swap moved it, and when focus was inside `node` on
 *  an element that is there again, that element is kept where it sat in the
 *  viewport ([`readingPlace`], [`keepReadingPlace`]).
 *
 *  Nothing here runs without a document: the node suites drive `replace`
 *  through fake nodes that have no `ownerDocument`, and for them this is the
 *  plain replace it always was. */
export function replace(node, children) {
  const kept = focusWithin(node);
  const place = readingPlace(node, kept);
  append(clear(node), children);
  if (kept !== null) {
    restoreFocus(node, kept);
  }
  keepReadingPlace(node, place);
  return node;
}

/** Where the reader is, as something that survives a re-render of `node`:
 *  the window's scroll offset and, when focus is inside `node` on an element
 *  with an id ([`focusWithin`]'s `kept`), where that element's top sits in the
 *  viewport. `null` where there is no window to scroll (the node suites'
 *  fakes). */
export function readingPlace(node, kept) {
  const doc = node && node.ownerDocument;
  const view = doc && doc.defaultView;
  if (!view || typeof view.scrollTo !== "function" ||
    typeof view.scrollX !== "number" || typeof view.scrollY !== "number") {
    return null;
  }
  const place = { x: view.scrollX, y: view.scrollY, id: null, top: 0 };
  const id = kept !== null && kept !== undefined && typeof kept.id === "string" ? kept.id : null;
  const focused = id === null || typeof doc.getElementById !== "function"
    ? null
    : doc.getElementById(id);
  if (focused !== null && focused !== undefined &&
    typeof focused.getBoundingClientRect === "function") {
    place.id = id;
    place.top = focused.getBoundingClientRect().top;
  }
  return place;
}

/** Puts the reader back where [`readingPlace`] found them, after `node` was
 *  re-rendered: the window's offset, when the swap moved it, and then -- when
 *  the element focus was on is in the new subtree -- the page is moved by as
 *  much as that element moved, so it sits where it sat. Nothing is scrolled
 *  when nothing moved: a scroll the reader is in the middle of is not
 *  interrupted by a repaint that left the page alone. */
export function keepReadingPlace(node, place) {
  if (place === null || place === undefined) {
    return false;
  }
  const doc = node.ownerDocument;
  const view = doc.defaultView;
  if (view.scrollX !== place.x || view.scrollY !== place.y) {
    view.scrollTo(place.x, place.y);
  }
  if (place.id !== null && typeof view.scrollBy === "function") {
    const again = doc.getElementById(place.id);
    if (again !== null && again !== undefined && typeof again.getBoundingClientRect === "function" &&
      (typeof node.contains !== "function" || node.contains(again))) {
      const moved = again.getBoundingClientRect().top - place.top;
      if (Math.abs(moved) >= 1) {
        view.scrollBy(0, moved);
      }
    }
  }
  return true;
}

/** Where focus is inside `node`, as something that survives a re-render:
 *  the focused element's `id` and text selection, and -- for a control with
 *  no id, or one the re-render disables (a form going pending disables its
 *  own submit button) -- which form it was in and where in that form. `null`
 *  when there is no document, or focus is elsewhere. */
export function focusWithin(node) {
  const doc = node && node.ownerDocument;
  if (!doc || typeof node.contains !== "function") {
    return null;
  }
  const active = doc.activeElement;
  if (!active || active === doc.body || active === node || !node.contains(active)) {
    return null;
  }
  const id = typeof active.getAttribute === "function" ? active.getAttribute("id") : null;
  let selection = null;
  try {
    if (typeof active.selectionStart === "number" && typeof active.selectionEnd === "number") {
      selection = [active.selectionStart, active.selectionEnd];
    }
  } catch (notText) {
    // a checkbox or a select has no text selection; asking some throws
    selection = null;
  }
  const form = typeof active.closest === "function" ? active.closest("form") : null;
  let formIndex = -1;
  let controlIndex = -1;
  let onStatus = false;
  if (form !== null && typeof node.querySelectorAll === "function") {
    formIndex = Array.from(node.querySelectorAll("form")).indexOf(form);
    controlIndex = Array.from(form.querySelectorAll(FOCUSABLE)).indexOf(active);
    onStatus = typeof active.matches === "function" && active.matches(".form-status");
  }
  return {
    id: typeof id === "string" && id.length > 0 ? id : null,
    selection: selection,
    formIndex: formIndex,
    controlIndex: controlIndex,
    onStatus: onStatus,
    tag: String(active.tagName || ""),
  };
}

/** What a keyboard can land on inside a form, in document order. */
const FOCUSABLE = "a[href], button, input, select, textarea, summary, [tabindex]";

/** Whether focus can land on `element`. A control inside a disabled
 *  `fieldset` -- how every form here shows it is pending -- has
 *  `disabled === false` and still refuses focus, so `:disabled` is asked
 *  where the element can answer it. */
function canTakeFocus(element) {
  if (element === null || element === undefined || typeof element.focus !== "function" ||
    element.disabled === true || element.hidden === true) {
    return false;
  }
  try {
    return typeof element.matches !== "function" || !element.matches(":disabled");
  } catch (unsupported) {
    return true;
  }
}

/** Puts focus back where [`focusWithin`] found it, in the new subtree: the
 *  same id; else the same position in the same form; else -- the control is
 *  gone or disabled, as a submit button is while its request is pending --
 *  that form's status region, which is where the outcome is announced; else
 *  the view itself. Never a guess outside the form the reader was in. */
export function restoreFocus(node, kept) {
  const doc = node && node.ownerDocument;
  if (!doc || kept === null || kept === undefined) {
    return false;
  }
  // A TARGET THE BROWSER REFUSES IS NOT A LANDING (P16's sweep): an empty
  // status region is `display: none` (`.form-status:empty`), and `focus()` on
  // it does nothing and says nothing -- Test access and the schedule form's
  // Check readiness left focus on the body that way. So a target counts only
  // when focus is on it afterwards, and otherwise the next one is tried.
  const take = (target) => {
    target.focus({ preventScroll: true });
    if (doc.activeElement !== target) {
      return false;
    }
    if (kept.selection !== null && typeof target.setSelectionRange === "function") {
      try {
        target.setSelectionRange(kept.selection[0], kept.selection[1]);
      } catch (notText) {
        // the new element is not a text field; its focus is what matters
      }
    }
    return true;
  };
  const byId = kept.id === null ? null : doc.getElementById(kept.id);
  if (byId !== null && node.contains(byId) && canTakeFocus(byId) && take(byId)) {
    return true;
  }
  if (kept.formIndex !== -1 && typeof node.querySelectorAll === "function") {
    const form = Array.from(node.querySelectorAll("form"))[kept.formIndex] || null;
    if (form !== null) {
      const status = form.querySelector(".form-status[tabindex]");
      if (!kept.onStatus && kept.controlIndex !== -1) {
        const same = Array.from(form.querySelectorAll(FOCUSABLE))[kept.controlIndex] || null;
        if (same !== null && String(same.tagName) === kept.tag && canTakeFocus(same) &&
          (kept.id === null || same.getAttribute("id") === kept.id) && take(same)) {
          return true;
        }
      }
      if (canTakeFocus(status) && take(status)) {
        return true;
      }
    }
  }
  if (typeof node.focus === "function" && typeof node.hasAttribute === "function" &&
    node.hasAttribute("tabindex")) {
    node.focus({ preventScroll: true });
  }
  return false;
}

/** Disables (or re-enables) a control IN PLACE without stranding the reader.
 *
 *  A button disabled while it holds focus drops that focus to the document
 *  body. So when `disabled` is true and focus is on `control` or inside it,
 *  focus first moves to `fallback` (a status region the outcome will be
 *  written into), else to the status region of the form the control is in,
 *  else to the view slot -- then the control is disabled. The class of defect
 *  `replace()`'s bookkeeping closes for a re-render, closed for the pages that
 *  disable a control without one (PLAT-18.2 class sweep). */
export function disableKeepingFocus(control, disabled, fallback) {
  if (control === null || control === undefined) {
    return;
  }
  const doc = control.ownerDocument;
  if (disabled === true && doc && typeof control.contains === "function" &&
    doc.activeElement !== null && control.contains(doc.activeElement)) {
    const form = typeof control.closest === "function" ? control.closest("form") : null;
    const candidates = [
      fallback,
      form === null ? null : form.querySelector(".form-status[tabindex]"),
      doc.getElementById("view-slot"),
    ];
    for (const candidate of candidates) {
      if (candidate !== null && candidate !== undefined && candidate !== control &&
        !control.contains(candidate) && typeof candidate.focus === "function") {
        candidate.focus({ preventScroll: true });
        // A REFUSED TARGET IS NOT A LANDING ([`restoreFocus`]'s rule): the
        // schedule form's empty status is not rendered, so the next is tried.
        if (doc.activeElement === candidate) {
          break;
        }
      }
    }
  }
  control.disabled = disabled === true;
}

/** THE TWO PROBLEM CODES THAT MEAN "SIGN IN" (MCP-1, MCP-4): the product API's
 *  `401` for a request with no session, and for one whose session expired
 *  (`docs/api.md`, *The session and the CSRF token*). `ui/client.js` reads
 *  the boot probe's answer against this list, and the error box offers a
 *  Sign in link for exactly these -- never for a Kubernetes `401`, which is a
 *  `kubectl proxy` whose own credential failed and which no sign-in fixes. */
export const SIGN_IN_CODES = Object.freeze(["unauthenticated", "session_expired"]);

/** The console's sign-in route with `next` pointing back at `hash`: the
 *  product API sends the browser there after the provider round trip
 *  (`/auth/login?next=`, which it accepts only as a path under `/ui/`). A
 *  path on this origin, never a URL. */
export function signInHref(hash) {
  const h = typeof hash === "string" && hash.charAt(0) === "#" ? hash : "";
  return "/auth/login?next=" + encodeURIComponent("/ui/" + h);
}

// The address the reader is on, for the Sign in link's `next`; empty where
// there is no window (the node suites).
function hereHash() {
  return typeof window !== "undefined" && window.location ? String(window.location.hash || "") : "";
}

// The headline an error box leads with, in words (MCP-4). The server's own
// status, code and message are still shown, verbatim, underneath: this adds a
// sentence a person reads first and changes nothing they could search for.
function errorTitle(status, reason) {
  if (SIGN_IN_CODES.indexOf(reason) !== -1) {
    return reason === "session_expired" ? "Your session has ended" : "You are not signed in";
  }
  if (status === 401) {
    return "Not authenticated";
  }
  if (status === 403) {
    return "You do not have permission to do this";
  }
  if (status === 404) {
    return "Not found";
  }
  if (status === 409) {
    return "This conflicts with what already exists";
  }
  if (status === 412) {
    return "This changed since the page read it";
  }
  if (status === 400 || status === 422) {
    return "The request was refused";
  }
  if (status === 429) {
    return "Too many requests";
  }
  if (typeof status === "number" && status >= 500) {
    return "The server could not complete this";
  }
  if (reason === "ContractViolation") {
    return "The server's answer was not what this page expected";
  }
  return "This could not be done";
}

/** What an error box says, in three parts: a headline in words, the server's
 *  own message, and the status, code and request id a reader can quote --
 *  plus whether to offer a sign-in. Pure. */
export function errorParts(error) {
  const e = error || {};
  const status = typeof e.status === "number" ? e.status : 0;
  const reason = typeof e.reason === "string" ? e.reason : "";
  const message = e.message ? String(e.message) : String(error);
  const codeLine = status > 0 ? "HTTP " + String(status) : "";
  const detail = [
    [codeLine, reason].filter((part) => part.length > 0).join(" "),
    typeof e.requestId === "string" && e.requestId.length > 0 ? "request " + e.requestId : "",
  ].filter((part) => part.length > 0).join(" \u00b7 ");
  return {
    title: errorTitle(status, reason),
    message: message,
    detail: detail,
    signIn: SIGN_IN_CODES.indexOf(reason) !== -1,
  };
}

/** Renders an error from `api.js`: a headline in words, then the API server's
 *  own message, then its status, code and request id, verbatim (MCP-4).
 *
 *  A 403 here is still the API server's 403 about the viewer's own authority.
 *  This function neither softens it nor explains it away; it says in words
 *  what the number means before it shows the number. A sign-in refusal from
 *  the product API carries a Sign in link that returns to this address. */
export function errorBox(error) {
  const parts = errorParts(error);
  return el("div", { class: "error", role: "alert" }, [
    el("p", { class: "error-title" }, parts.title),
    el("p", { class: "error-message" }, parts.message),
    parts.detail.length > 0 ? el("span", { class: "error-status" }, parts.detail) : null,
    parts.signIn
      ? el("p", { class: "actions" },
        el("a", { class: "button primary", href: signInHref(hereHash()) }, "Sign in"))
      : null,
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
 *  deleting the object does not delete the signed document it names. Said in
 *  an operator's words since MCP-21: "the authoritative index is the evidence
 *  bucket" was jargon, repeated on every list. */
export const BUCKET_FOOTER =
  "This list is what the cluster holds now. The signed evidence each run wrote stays in the " +
  "archive even if the object listed here is deleted.";

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
 *  would rather take them from the cluster -- under THEIR OWN context: the
 *  caveat used to hard-code `--context docker-desktop`, this repository's lab
 *  context, on every installation (MCP-30). */
export const COPY_CAVEAT =
  "copy loses trailing whitespace in some browsers; download, or read the same bytes from " +
  "the cluster with your own kubectl context -- kubectl get restore <name> --namespace " +
  "<namespace> -o jsonpath='{.spec.planBytes}' > <name>.yaml -- and hash exactly what you " +
  "downloaded.";

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
    // ONE TOPIC IS ONE TOPIC (MCP round 2, R2-10: "1 topics").
    (topicCount === 1 ? " topic" : " topics") +
    " with message.timestamp.type=CreateTime and retention.ms=-1 before the " +
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
 *  laptop and lets the stylesheet stack the rows into cards below 768 px; the
 *  column captions those cards show are copied from the header row by
 *  `app.js` when the nodes are adopted, so the string here carries them once.
 *
 *  `rowAttributes[i]`, when given, is spelled into row `i`'s opening tag. It
 *  exists for ONE thing: naming a row so the page can find it again without
 *  counting -- the restore wizard's point selector marks each row with the
 *  Backup's UID and filters in place, and an index-counted lookup would follow
 *  the wrong row the moment the list changed under it. It is a string OF OURS
 *  (a caller renders it from `esc`'d values) and is never a value read out of
 *  the cluster unescaped.
 *
 *  `grid`, when given, DECLARES THE TABLE A DATAGRID (PLAT-18.2): `{id,
 *  label}`, where `id` names it for the page's lifetime and `label` is the
 *  plural noun its filter and pagination speak of ("backups"). The table is
 *  then wrapped in `div.datagrid[data-datagrid]`, and `enhanceDatagrids` --
 *  run on every parsed fragment by `app.js` -- adds Clarity's filter, sortable
 *  column headers and pagination footer around the SAME rows. The string
 *  gains the wrapper and nothing else, so every row a test asserts on is
 *  exactly the row a browser receives. */
export function table(columns, rows, empty, rowAttributes, grid) {
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
  const html =
    "<div class=\"table-wrap\"><table class=\"grid\"><thead><tr>" +
    head +
    "</tr></thead><tbody>" +
    body +
    none +
    "</tbody></table></div>";
  return grid === undefined || grid === null ? html : datagrid(grid, html);
}

// ===========================================================================
// PLAT-18.2: THE DATAGRID -- Clarity's filter, sort and pagination over a
// table the page already rendered.
// ===========================================================================
//
// WHY AN ENHANCEMENT AND NOT A NEW RENDERER. Every list here is a pure string
// from a JSON object, asserted as such under `node --test`. A datagrid that
// re-rendered the rows itself would be a second renderer the suites never see.
// So the page declares a table a datagrid (the `grid` argument of `table`),
// the string gains one wrapper, and `enhanceDatagrids` works on the parsed
// rows: it FILTERS them, SORTS them and SHOWS ONE PAGE of them, and never
// changes what a row says. Its arithmetic is [`datagridView`], a pure
// function the suites drive directly.
//
// WHY NOT VIRTUALISATION. Measured before this was written
// (`scripts/plat18-2-measure.mjs`, `scripts/plat18-2-ui-e2e.mjs`): the strings
// for 1,000 history rows render in under 5 ms; what a large list costs is
// the browser laying out every row. Pagination keeps one page of rows in the
// layout -- the others are detached (a read-only table) or hidden (a table
// whose rows hold controls a page wires after it parses) -- which is the
// benefit virtualisation would buy, without a scroll model a keyboard and a
// screen reader then have to fight. The figures are in the PLAT-18.2 report.
//
// THE STATE IS MEMORY, NOT STORAGE. A grid's filter, sort and page live in
// the map below for the life of the loaded page, keyed by the grid's id, so a
// re-render (the detail after an action, the wizard after an edit) keeps the
// reader where they were. Nothing is written to browser storage; the offline
// gate's rule 2 forbids it and a reload starts clean.

/** Clarity's datagrid page sizes, and the one a grid starts at. */
export const DATAGRID_PAGE_SIZES = Object.freeze([10, 20, 50, 100]);
export const DATAGRID_DEFAULT_PAGE_SIZE = 20;

/** Rows at or below this many get sorting and a count, but no filter and no
 *  pager: a control that can only ever show everything is noise. */
export const DATAGRID_CONTROLS_ABOVE = 10;

/** The wrapper `table` puts around a declared datagrid. */
export function datagrid(grid, html) {
  const g = grid || {};
  const scope = g.scope === undefined || g.scope === null || String(g.scope).length === 0
    ? ""
    : " data-datagrid-scope=\"" + esc(g.scope) + "\"";
  return (
    "<div class=\"datagrid\" data-datagrid=\"" + esc(g.id) + "\" data-datagrid-label=\"" +
    esc(g.label || "rows") + "\"" + scope + ">" + html + "</div>"
  );
}

/** A filter query as the terms every visible row must contain. */
export function datagridTerms(query) {
  return String(query === undefined || query === null ? "" : query)
    .toLowerCase()
    .split(/\s+/)
    .filter((t) => t.length > 0);
}

function absentText(value) {
  const text = String(value === undefined || value === null ? "" : value).trim();
  return text === "" || text === ABSENT;
}

/** Compares two cell texts the way a reader expects a column to sort:
 *  numbers as numbers, everything else as text (an RFC 3339 instant sorts
 *  correctly as text). An absent value (`-` or empty) is not compared here;
 *  [`datagridView`] puts it last in either direction. */
export function datagridCompare(a, b) {
  const left = String(a === undefined || a === null ? "" : a).trim();
  const right = String(b === undefined || b === null ? "" : b).trim();
  const numeric = /^-?\d+(\.\d+)?$/;
  if (numeric.test(left) && numeric.test(right)) {
    return Number(left) - Number(right);
  }
  return left < right ? -1 : (left > right ? 1 : 0);
}

/** THE DATAGRID'S ARITHMETIC, pure: which rows one page shows, in which
 *  order, and the numbers the footer says.
 *
 *  `rows` are `{text, cells}` -- the row's whole text and its cells' texts;
 *  `state` is `{query, sortColumn, sortDirection, page, pageSize}`. A row
 *  matches when it contains every term of the query, case-insensitively.
 *  Sorting is stable: equal keys keep the page's own order, and `sortColumn`
 *  -1 is that order. An absent value sorts last in both directions. `page`
 *  is clamped into range, so a page that no longer exists (the list shrank,
 *  the filter narrowed it) is the last one that does, never an empty screen. */
export function datagridView(rows, state) {
  const list = Array.isArray(rows) ? rows : [];
  const s = state || {};
  const terms = datagridTerms(s.query);
  const matched = [];
  for (let i = 0; i < list.length; i += 1) {
    const text = String((list[i] || {}).text || "").toLowerCase();
    if (terms.every((t) => text.indexOf(t) !== -1)) {
      matched.push(i);
    }
  }
  const column = typeof s.sortColumn === "number" ? s.sortColumn : -1;
  if (column >= 0) {
    const sign = s.sortDirection === "descending" ? -1 : 1;
    matched.sort((x, y) => {
      const cx = ((list[x] || {}).cells || [])[column];
      const cy = ((list[y] || {}).cells || [])[column];
      const ax = absentText(cx);
      const ay = absentText(cy);
      if (ax || ay) {
        return ax === ay ? x - y : (ax ? 1 : -1);
      }
      const order = datagridCompare(cx, cy);
      return order !== 0 ? sign * order : x - y;
    });
  }
  const size = DATAGRID_PAGE_SIZES.indexOf(s.pageSize) === -1 ? DATAGRID_DEFAULT_PAGE_SIZE : s.pageSize;
  const pages = Math.max(1, Math.ceil(matched.length / size));
  const wanted = typeof s.page === "number" && isFinite(s.page) ? Math.floor(s.page) : 1;
  const page = Math.min(Math.max(1, wanted), pages);
  const start = (page - 1) * size;
  const shown = matched.slice(start, start + size);
  return {
    indices: shown,
    order: matched,
    total: list.length,
    matched: matched.length,
    page: page,
    pages: pages,
    pageSize: size,
    first: shown.length === 0 ? 0 : start + 1,
    last: start + shown.length,
    filtered: terms.length > 0,
  };
}

/** What the footer says about one view, in words, for sight and for the live
 *  region alike. */
export function datagridSummary(view, label) {
  const v = view || {};
  const noun = typeof label === "string" && label.length > 0 ? label : "rows";
  if (v.total === 0) {
    return "No " + noun + ".";
  }
  if (v.matched === 0) {
    return "No " + noun + " match the filter (" + String(v.total) + " in all).";
  }
  const range = String(v.first) + "-" + String(v.last) + " of " + String(v.matched) + " " + noun;
  return v.filtered ? range + " matching the filter (" + String(v.total) + " in all)." : range + ".";
}

const DATAGRID_STATE = new Map();

/** THE KEY A GRID'S STATE IS KEPT UNDER: its id, the route it is on, and the
 *  list instance it shows (review HIGH-1). A grid id is a constant --
 *  `history`, `subset-topics` -- shared by every namespace and every recovery
 *  point, so keyed on the id alone a filter typed over one namespace's 15 runs
 *  silently emptied the next namespace's 5, whose grid rendered no box to
 *  clear it. The route is the whole hash (namespace, name, uid); `scope` is the
 *  instance a page names itself (a schedule's uid, a point's uid). */
export function datagridScopeKey(id, route, scope) {
  return [String(id || ""), String(route || ""), String(scope || "")].join("\n");
}

/** WHAT A GRID OF `rowCount` ROWS SHOWS, given its kept state (review HIGH-1).
 *
 *  The filter box is rendered when the list is long enough to need one OR a
 *  filter is in force -- so a query can never apply without the box that shows
 *  it and clears it. The pager is rendered only for a long list; a short one
 *  is always page 1. Pure, so the suite drives it. */
export function datagridPlan(rowCount, state) {
  const s = state || {};
  const query = String(s.query || "");
  const long = rowCount > DATAGRID_CONTROLS_ABOVE;
  const filter = long || query.trim().length > 0;
  return {
    filter: filter,
    pager: long,
    state: {
      query: filter ? query : "",
      sortColumn: typeof s.sortColumn === "number" ? s.sortColumn : -1,
      sortDirection: s.sortDirection === "descending" ? "descending" : "ascending",
      page: long ? s.page : 1,
      pageSize: long ? s.pageSize : DATAGRID_PAGE_SIZES[DATAGRID_PAGE_SIZES.length - 1],
    },
  };
}

/** How many rows the filter hides, in words, or "" when it hides none. */
export function datagridHiddenSentence(view, label) {
  const v = view || {};
  const hidden = (v.total || 0) - (v.matched || 0);
  if (!v.filtered || hidden <= 0) {
    return "";
  }
  return String(hidden) + " " + (label || "rows") + " hidden by the filter.";
}

/** ROWS THAT NEED ATTENTION AND ARE NOT ON THIS PAGE (review LOW-2): an
 *  unavailable archive or a failed run on page 3 must not be invisible on page
 *  1. `attention[i]` says whether row `i` carries such a state. Returns the
 *  count and the pages they are on, over the rows the filter keeps. */
export function datagridOffPageAttention(view, attention) {
  const v = view || {};
  const order = Array.isArray(v.order) ? v.order : [];
  const flags = Array.isArray(attention) ? attention : [];
  const size = v.pageSize || DATAGRID_DEFAULT_PAGE_SIZE;
  const pages = new Set();
  let count = 0;
  order.forEach((row, position) => {
    const page = Math.floor(position / size) + 1;
    if (flags[row] === true && page !== v.page) {
      count += 1;
      pages.add(page);
    }
  });
  return { count: count, pages: Array.from(pages).sort((a, b) => a - b) };
}

/** The footer's words for [`datagridOffPageAttention`], or "". */
export function datagridAttentionSentence(off, label) {
  const o = off || {};
  if (!o.count) {
    return "";
  }
  return String(o.count) + " needing attention (failed, unverified, refused or unavailable) " +
    (o.count === 1 ? "is" : "are") + " not on this page: " +
    (o.pages.length === 1 ? "page " : "pages ") + o.pages.join(", ") + ".";
}

/** A row that carries a state an operator must not miss. */
export const DATAGRID_ATTENTION =
  ".badge-danger, .badge-phase-failed, .badge-unverified, .badge-warn, .complaint, .refusal";

/** The state one grid carries for the life of the loaded page, under the key
 *  [`datagridScopeKey`] builds (an id alone is the key of a grid shown once). */
export function datagridState(id) {
  const key = String(id || "");
  if (!DATAGRID_STATE.has(key)) {
    DATAGRID_STATE.set(key, {
      query: "",
      sortColumn: -1,
      sortDirection: "ascending",
      page: 1,
      pageSize: DATAGRID_DEFAULT_PAGE_SIZE,
    });
  }
  return DATAGRID_STATE.get(key);
}

/** ONE POLITE LIVE REGION FOR THE WHOLE PAGE (PLAT-18.2: accessible progress
 *  announcements). A view that re-renders by replacing its subtree replaces
 *  any live region inside it, and a region that is removed and re-added is
 *  not reliably announced. `index.html` carries `#announcer`, which no view
 *  replaces; a page hands it a sentence when something a reader is waiting
 *  on changed -- an operation's state or stage -- and the same sentence twice
 *  in a row is said once. Nothing happens without a document. */
let lastAnnouncement = "";
export function announce(message) {
  const text = String(message === undefined || message === null ? "" : message).trim();
  if (text.length === 0 || text === lastAnnouncement || typeof document === "undefined") {
    return false;
  }
  const region = document.getElementById("announcer");
  if (region === null) {
    return false;
  }
  lastAnnouncement = text;
  region.textContent = text;
  return true;
}

/** THE SCROLLING REGIONS, MADE REACHABLE (PLAT-18.2; axe-core's
 *  `scrollable-region-focusable`). A wide table or a long plan block scrolls
 *  sideways inside its own box; a region that scrolls and holds nothing
 *  focusable cannot be scrolled from a keyboard at all. So every `.table-wrap`
 *  and every code block (`pre`) under `root` that ACTUALLY overflows takes
 *  `tabindex="0"`, `role="region"` and a name (its nearest heading's words),
 *  and one that no longer overflows gives them back -- so a table that fits
 *  adds no stop to the Tab order. It reads layout, so it runs after the view
 *  is in the document: `app.js` calls it whenever the view changes and when
 *  the window is resized. Only attributes this function set are removed. */
export function markScrollRegions(root) {
  if (!root || typeof root.querySelectorAll !== "function") {
    return 0;
  }
  let marked = 0;
  for (const region of Array.from(root.querySelectorAll(".table-wrap, pre"))) {
    const overflows = region.scrollWidth > region.clientWidth + 1 ||
      region.scrollHeight > region.clientHeight + 1;
    const ours = region.getAttribute("data-scroll-region") === "true";
    if (overflows && !region.hasAttribute("tabindex")) {
      const section = region.closest("section, form, main");
      const heading = section === null ? null : section.querySelector("h2, h3, h4");
      region.setAttribute("tabindex", "0");
      region.setAttribute("role", "region");
      region.setAttribute("aria-label",
        (heading === null ? "Scrollable content" : heading.textContent.trim()) + " (scrolls)");
      region.setAttribute("data-scroll-region", "true");
      marked += 1;
    } else if (!overflows && ours) {
      region.removeAttribute("tabindex");
      region.removeAttribute("role");
      region.removeAttribute("aria-label");
      region.removeAttribute("data-scroll-region");
    }
  }
  return marked;
}

/** Enhances every declared datagrid in `root` (and `root` itself). Called by
 *  `app.js` on every parsed fragment, BEFORE a page wires its controls, so a
 *  grid whose rows hold controls keeps every row in the document (hidden, not
 *  detached) and the page's own `querySelectorAll` still finds them. */
export function enhanceDatagrids(root) {
  if (!root || typeof root.querySelectorAll !== "function") {
    return 0;
  }
  const found = [];
  if (typeof root.matches === "function" && root.matches("[data-datagrid]")) {
    found.push(root);
  }
  for (const one of Array.from(root.querySelectorAll("[data-datagrid]"))) {
    found.push(one);
  }
  for (const container of found) {
    enhanceOne(container);
  }
  return found.length;
}

function make(doc, tag, attrs, text) {
  const node = doc.createElement(tag);
  for (const key of Object.keys(attrs || {})) {
    if (attrs[key] !== null && attrs[key] !== undefined) {
      node.setAttribute(key, String(attrs[key]));
    }
  }
  if (text !== undefined && text !== null) {
    node.appendChild(doc.createTextNode(String(text)));
  }
  return node;
}

/** Clarity's signpost, on the native disclosure element: the trigger is a
 *  real `summary` (a button to the keyboard and to a screen reader, with its
 *  expanded state announced by the platform), the body is a note, and Escape
 *  closes it and returns focus to the trigger. */
export function signpost(doc, id, label, text) {
  const details = make(doc, "details", { class: "signpost", id: id });
  const summary = make(doc, "summary", { "aria-label": label }, "i");
  const body = make(doc, "div", { class: "signpost-body", role: "note" }, text);
  details.appendChild(summary);
  details.appendChild(body);
  details.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && details.open) {
      details.open = false;
      summary.focus();
    }
  });
  return details;
}

function enhanceOne(container) {
  if (container.getAttribute("data-datagrid-ready") === "true") {
    return;
  }
  container.setAttribute("data-datagrid-ready", "true");
  const doc = container.ownerDocument;
  const id = container.getAttribute("data-datagrid") || "grid";
  const label = container.getAttribute("data-datagrid-label") || "rows";
  const table = container.querySelector("table");
  const list = table === null ? container.querySelector("ul, ol") : null;
  const body = table !== null ? table.tBodies[0] : list;
  if (!body) {
    return;
  }
  const items = Array.from(body.children).filter((row) =>
    row.querySelector("td.empty") === null);
  if (items.length === 0) {
    return;
  }
  // A row that holds a control stays in the document: the page wires it
  // after this runs, by querying for it.
  const keepAll = body.querySelector("input, select, textarea, button, form") !== null;
  const rows = items.map((row) => ({
    text: row.textContent,
    cells: table !== null ? Array.from(row.children).map((c) => c.textContent) : [],
  }));
  const route = doc.defaultView && doc.defaultView.location
    ? String(doc.defaultView.location.hash || "")
    : "";
  const state = datagridState(datagridScopeKey(id, route,
    container.getAttribute("data-datagrid-scope") || ""));
  const attention = items.map((row) => row.querySelector(DATAGRID_ATTENTION) !== null);
  const columns = table !== null ? Array.from(table.querySelectorAll("thead th")) : [];
  const plan = datagridPlan(items.length, state);
  // A SHORT LIST NEVER CARRIES A PAGE OR A SIZE IT CANNOT SHOW; its query is
  // kept only while the box that shows it is on screen (always, by the plan).
  state.query = plan.state.query;
  if (!plan.pager) {
    state.page = 1;
  }
  const controls = plan.pager;
  const filtering = plan.filter;
  const regionId = id + "-grid";
  (table !== null ? table : body).setAttribute("id", regionId);

  // The live count, which is also what a screen reader hears after a change.
  const count = make(doc, "p", { class: "datagrid-count", id: id + "-count", role: "status",
    "aria-live": "polite" });

  let filter = null;
  let hiddenLine = null;
  let hiddenText = null;
  let clear = null;
  if (filtering) {
    const toolbar = make(doc, "div", { class: "datagrid-toolbar" });
    const field = make(doc, "div", { class: "field" });
    const caption = make(doc, "div", { class: "datagrid-filter-caption" });
    caption.appendChild(make(doc, "label", { for: id + "-filter" }, "Filter " + label));
    caption.appendChild(signpost(doc, id + "-filter-help", "About filtering " + label,
      "Every word you type must appear somewhere in a row, in any column, in any case. " +
      "The filter, the sort and the page are kept while this page is open and forgotten on a " +
      "reload; nothing is stored in the browser. A column header sorts by that column: once " +
      "ascending, again descending, a third time back to the page's own order."));
    field.appendChild(caption);
    filter = make(doc, "input", { type: "search", id: id + "-filter", "aria-controls": regionId,
      "aria-describedby": id + "-count", autocomplete: "off", spellcheck: "false" });
    filter.value = state.query;
    field.appendChild(filter);
    toolbar.appendChild(field);
    // WHENEVER THE FILTER HIDES ROWS, SAY HOW MANY AND OFFER TO CLEAR IT.
    hiddenLine = make(doc, "p", { class: "datagrid-hidden", id: id + "-hidden" });
    hiddenText = make(doc, "span", {});
    clear = make(doc, "button", { type: "button", id: id + "-clear", "aria-controls": regionId },
      "Clear filter");
    hiddenLine.appendChild(hiddenText);
    hiddenLine.appendChild(doc.createTextNode(" "));
    hiddenLine.appendChild(clear);
    toolbar.appendChild(hiddenLine);
    container.insertBefore(toolbar, container.firstChild);
  }

  // Sortable headers: the caption becomes a button; `aria-sort` on the th.
  const sorters = [];
  if (table !== null && items.length > 1) {
    columns.forEach((th, index) => {
      const caption = th.textContent;
      // A column with no caption (a row's action column) is not a sort key a
      // reader can name, and a button without words has no accessible name.
      if (th.hasAttribute("colspan") || caption.trim().length === 0) {
        return;
      }
      const button = make(doc, "button", { type: "button", class: "datagrid-sort",
        id: id + "-sort-" + String(index) }, caption);
      while (th.firstChild !== null) {
        th.removeChild(th.firstChild);
      }
      th.appendChild(button);
      sorters.push({ th: th, button: button, index: index });
    });
  }

  const footer = make(doc, "div", { class: "datagrid-footer" });
  footer.appendChild(count);
  const attentionLine = make(doc, "p", { class: "datagrid-attention", id: id + "-attention" });
  footer.appendChild(attentionLine);
  let size = null;
  let first = null;
  let previous = null;
  let next = null;
  let last = null;
  let pageOf = null;
  if (controls) {
    const sizeLabel = make(doc, "label", { for: id + "-page-size" }, "Rows per page");
    size = make(doc, "select", { id: id + "-page-size", "aria-controls": regionId });
    for (const option of DATAGRID_PAGE_SIZES) {
      const o = make(doc, "option", { value: String(option) }, String(option));
      if (option === state.pageSize) {
        o.setAttribute("selected", "");
      }
      size.appendChild(o);
    }
    sizeLabel.appendChild(size);
    footer.appendChild(sizeLabel);
    const pager = make(doc, "div", { class: "datagrid-pages", role: "group",
      "aria-label": "Pages of " + label });
    first = make(doc, "button", { type: "button", id: id + "-first", "aria-label": "First page",
      "aria-controls": regionId }, "\u00ab");
    previous = make(doc, "button", { type: "button", id: id + "-previous",
      "aria-label": "Previous page", "aria-controls": regionId }, "\u2039");
    pageOf = make(doc, "span", { class: "datagrid-page-of", id: id + "-page-of" });
    next = make(doc, "button", { type: "button", id: id + "-next", "aria-label": "Next page",
      "aria-controls": regionId }, "\u203a");
    last = make(doc, "button", { type: "button", id: id + "-last", "aria-label": "Last page",
      "aria-controls": regionId }, "\u00bb");
    for (const part of [first, previous, pageOf, next, last]) {
      pager.appendChild(part);
    }
    footer.appendChild(pager);
  }
  container.appendChild(footer);

  const noMatch = make(doc, table !== null ? "tr" : "li", { class: "datagrid-no-match" });
  const noMatchCell = table !== null
    ? make(doc, "td", { class: "empty", colspan: String(Math.max(1, columns.length)) })
    : noMatch;
  if (table !== null) {
    noMatch.appendChild(noMatchCell);
  }

  const update = () => {
    const view = datagridView(rows, plan.pager ? state
      : Object.assign({}, state, { page: 1, pageSize: plan.state.pageSize }));
    state.page = view.page;
    const show = new Set(view.indices);
    const order = state.sortColumn >= 0
      ? view.indices.concat(items.map((_, i) => i).filter((i) => !show.has(i)))
      : items.map((_, i) => i);
    while (body.firstChild !== null) {
      body.removeChild(body.firstChild);
    }
    for (const i of order) {
      if (show.has(i)) {
        items[i].hidden = false;
        body.appendChild(items[i]);
      } else if (keepAll) {
        items[i].hidden = true;
        body.appendChild(items[i]);
      }
    }
    if (view.matched === 0) {
      noMatchCell.textContent =
        "No " + label + " match the filter. Clear the filter above to see all " +
        String(view.total) + ".";
      body.appendChild(noMatch);
    }
    count.textContent = datagridSummary(view, label);
    if (hiddenLine !== null) {
      const said = datagridHiddenSentence(view, label);
      hiddenText.textContent = said;
      hiddenLine.hidden = said.length === 0;
    }
    const off = datagridAttentionSentence(datagridOffPageAttention(view, attention), label);
    attentionLine.textContent = off;
    attentionLine.hidden = off.length === 0;
    for (const s of sorters) {
      if (s.index === state.sortColumn) {
        s.th.setAttribute("aria-sort", state.sortDirection);
      } else {
        s.th.removeAttribute("aria-sort");
      }
    }
    if (pageOf !== null) {
      pageOf.textContent = "Page " + String(view.page) + " of " + String(view.pages);
      first.disabled = view.page <= 1;
      previous.disabled = view.page <= 1;
      next.disabled = view.page >= view.pages;
      last.disabled = view.page >= view.pages;
    }
  };

  if (filter !== null) {
    filter.addEventListener("input", () => {
      state.query = filter.value;
      state.page = 1;
      update();
    });
    clear.addEventListener("click", () => {
      state.query = "";
      state.page = 1;
      filter.value = "";
      update();
      filter.focus();
    });
  }
  for (const s of sorters) {
    s.button.addEventListener("click", () => {
      if (state.sortColumn !== s.index) {
        state.sortColumn = s.index;
        state.sortDirection = "ascending";
      } else if (state.sortDirection === "ascending") {
        state.sortDirection = "descending";
      } else {
        state.sortColumn = -1;
        state.sortDirection = "ascending";
      }
      update();
    });
  }
  if (size !== null) {
    size.addEventListener("change", () => {
      state.pageSize = Number(size.value);
      state.page = 1;
      update();
    });
    // A pager button that disables itself under the reader's focus would drop
    // that focus to the body; it moves to the control that still works.
    const go = (to, fallback) => () => {
      state.page = to();
      update();
      const active = doc.activeElement;
      if (active === null || active === doc.body || active.disabled === true) {
        fallback().focus();
      }
    };
    first.addEventListener("click", go(() => 1, () => next));
    previous.addEventListener("click", go(() => state.page - 1, () => next));
    next.addEventListener("click", go(() => state.page + 1, () => previous));
    last.addEventListener("click", go(() => Number.MAX_SAFE_INTEGER, () => previous));
  }
  update();
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

/** A covered window as ONE cell, both bounds, one per line (MCP round 2,
 *  R2-3): two instant columns side by side were the widest pair on every
 *  run table, each `nowrap` by MCP-7's rule. `from` and `to` are RFC 3339
 *  strings (or anything `when` takes). */
export function coveredCell(from, to) {
  return "<span class=\"cell-sub-first\">from " + when(from) + "</span>" +
    "<span class=\"cell-sub\">to " + when(to) + "</span>";
}

/** A schedule's last and next firing as ONE cell (R2-3), the same shape. */
export function firingsCell(last, next) {
  return "<span class=\"cell-sub-first\">last " + when(last) + "</span>" +
    "<span class=\"cell-sub\">next " + when(next) + "</span>";
}

/** A badge. STRUCTURAL ONLY: `kind` becomes a class suffix and `text` becomes
 *  the caption. Which kind a run gets is the calling page's decision, made
 *  from the object's own recorded fields. */
export function badge(kind, text) {
  return (
    "<span class=\"badge badge-" + esc(kind) + "\">" + esc(text) + "</span>"
  );
}

/** A badge with a hover title: the exact recorded value one hover away from
 *  the words (MCP-14, MCP-22). `title` is plain text and is escaped here. */
export function titledBadge(kind, text, title) {
  return (
    "<span class=\"badge badge-" + esc(kind) + "\" title=\"" + esc(title) + "\">" + esc(text) +
    "</span>"
  );
}

/** A CONDITION AS A BADGE, NOT AS ITS SYNTAX (MCP-22): `Ready=True ViewReady`
 *  in a table cell becomes the word, in the status colour, with the recorded
 *  type, status and reason as its title. A `False` carries its reason in the
 *  words too, because that is what the reader acts on; `Unknown` -- or any
 *  status this build does not know -- is `unknown` and never the true word.
 *  [`ABSENT`] when there is no condition. */
export function conditionBadge(condition, trueWord, falseWord) {
  if (condition === null || condition === undefined) {
    return ABSENT;
  }
  const c = condition;
  const reason = typeof c.reason === "string" && c.reason.length > 0 ? c.reason : "";
  const exact = String(c.type || "") + "=" + String(c.status || "") +
    (reason.length > 0 ? " " + reason : "") +
    (typeof c.message === "string" && c.message.length > 0 ? ": " + c.message : "");
  if (String(c.status) === "True") {
    return titledBadge("green", trueWord, exact);
  }
  if (String(c.status) === "False") {
    return titledBadge("unverified", falseWord + (reason.length > 0 ? " (" + reason + ")" : ""),
      exact);
  }
  return titledBadge("flat", "unknown" + (reason.length > 0 ? " (" + reason + ")" : ""), exact);
}

/** A BOOLEAN AS WORDS (MCP-14): `true`/`false` in a cell become the words the
 *  field means, as a neutral badge -- neither word is a verdict. [`ABSENT`]
 *  for a value that is not a boolean, never a guess. */
export function flagBadge(value, trueWords, falseWords) {
  if (value === true) {
    return badge("flat", trueWords);
  }
  if (value === false) {
    return badge("flat", falseWords);
  }
  return ABSENT;
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

/** A RUN's recorded phase as a badge: [`phaseBadge`], except that a manual run
 *  waiting for a slot in its namespace's manual-run pool (P10) also says how
 *  many runs of its kind may be active there at once -- "Queued (limit 4
 *  active)". The number is the controller's own `status.queue.limit`, COPIED
 *  and never computed here; a `Queued` phase with no such block (nothing an
 *  older controller writes has one) is the plain `Queued` badge, and every
 *  other phase is exactly [`phaseBadge`]'s. */
export function runPhaseBadge(status) {
  const s = status || {};
  const queue = s.queue || {};
  if (s.phase === "Queued" && Number.isInteger(queue.limit) && queue.limit > 0) {
    // A QUEUED RESTORE'S APPROVAL KEEPS ITS CLOCK (P10 review M2): the queue
    // does not extend an approval's maximum age, so the deadline the
    // controller copied onto the object is part of what "queued" means here.
    const deadline = typeof queue.authorizationExpiresAt === "string" &&
      queue.authorizationExpiresAt.length > 0
      ? "; approval expires " + queue.authorizationExpiresAt
      : "";
    return badge("phase-queued", "Queued (limit " + queue.limit + " active" + deadline + ")");
  }
  return phaseBadge(s.phase);
}

/** What a queued manual run is, in the page's own fixed words (P10). */
export const QUEUED_RUN_SENTENCE =
  "Queued: this manual run is waiting for a slot. Its namespace lets a fixed number of manual " +
  "runs of this kind be active at once, and this one starts, in creation order, when one of " +
  "them finishes. Nothing has been created for it yet -- no plan and no Job. Scheduled runs " +
  "are not counted and are never queued. A queued restore keeps its approval's deadline: the " +
  "queue does not extend it, so a restore still waiting when its approval expires is refused " +
  "and must be confirmed again.";

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
/** What a check still running says instead of an applicability verdict. */
export const CHECKING_SENTENCE =
  "The check has not finished. Whether its result applies to your current inputs is decided " +
  "when it has one; this page reads it again until then.";

/** THE WORDS A ROLE THAT CANNOT RESTORE READS WHERE A RESTORE LINK WOULD BE
 *  (MCP round 3, R3-2): the route refuses such a role by name (R2-14), so a
 *  link to it is an offer the page already knows it will refuse. The same
 *  words the catalog's Connect panel uses for its own role. */
export const RESTORE_NEEDS_ROLE_SENTENCE =
  "an operator or administrator can restore this point";

/** Who can restore, in place of a restore link: the words alone for a table
 *  cell, or -- `asSentence` -- capitalised and closed, for a place where the
 *  link stood inside a sentence of its own. */
export function restoreNeedsRole(asSentence) {
  const words = asSentence === true
    ? RESTORE_NEEDS_ROLE_SENTENCE.charAt(0).toUpperCase() + RESTORE_NEEDS_ROLE_SENTENCE.slice(1) + "."
    : RESTORE_NEEDS_ROLE_SENTENCE;
  return "<span class=\"note\" data-restore-refused=\"role\">" + esc(words) + "</span>";
}

/** "Restore this point": the link to the wizard, or -- for a role the session
 *  says cannot create a Restore in this namespace (`allowed === false`) -- the
 *  sentence saying who can. `href` is the route (escaped here); `attributes`
 *  is extra markup the caller has already escaped. Pure. */
export function restorePointLink(href, allowed, label, attributes) {
  if (allowed === false) {
    return restoreNeedsRole(false);
  }
  return "<a class=\"action\" href=\"" + esc(href) + "\"" + (attributes || "") + ">" +
    esc(label || "Restore this point") + "</a>";
}

/** What a check says when this page stopped following it because the
 *  longest time a check may take has passed without a result (`lifecycle.js`'s
 *  `followCheck`). It replaces [`CHECKING_SENTENCE`], whose promise to read
 *  again would no longer be true. */
export const CHECK_UNFINISHED_SENTENCE =
  "The check did not finish in the longest time a check may take, so this page stopped " +
  "reading it. It was not cancelled, and nothing it records later is shown here. Run the " +
  "check again to start a new one.";

/** What a check says when a read of it was refused outright, so this page
 *  stopped following it. */
export const CHECK_UNREADABLE_SENTENCE =
  "This page could not read the check again, so it stopped following it. The check was not " +
  "cancelled. Run the check again to start a new one.";

/** The two reasons a page stops following a check, spelled as `lifecycle.js`
 *  spells them (`FOLLOW_DEADLINE`, `FOLLOW_UNREADABLE`); this module imports
 *  nothing, and the suite holds the two spellings together. */
const STOPPED_DEADLINE = "deadline";
const STOPPED_UNREADABLE = "unreadable";

function stoppedReason(check) {
  const why = (check || {}).followStopped;
  return why === STOPPED_DEADLINE || why === STOPPED_UNREADABLE ? why : "";
}

/** The block a check carries once this page stopped following it: why, in
 *  words, and "Run the check again" -- a `.check-retry` button naming the
 *  check it is about, which the page wires to its own start
 *  (`lifecycle.js`'s `wireCheckRetry`). Empty while the page still follows
 *  the check or never stopped. `noun` is what the button starts again:
 *  "check" (a `Preflight`) or "discovery". */
export function checkStoppedBlock(check, noun) {
  const c = check || {};
  const why = stoppedReason(c);
  if (why === "") {
    return "";
  }
  const what = typeof noun === "string" && noun.length > 0 ? noun : "check";
  const said = why === STOPPED_DEADLINE ? CHECK_UNFINISHED_SENTENCE : CHECK_UNREADABLE_SENTENCE;
  const words = what === "check" ? said : said.split("check").join(what);
  return (
    "<div class=\"check-stopped\" role=\"status\" data-check-stopped=\"" + esc(why) + "\">" +
    "<p class=\"note\">" + esc(words) +
    (why === STOPPED_UNREADABLE && typeof c.followError === "string" && c.followError.length > 0
      ? " The read answered: " + esc(c.followError)
      : "") +
    "</p>" +
    "<div class=\"actions\"><button type=\"button\" class=\"check-retry\" data-check=\"" +
    esc(String(c.id || "")) + "\">Run the " + esc(what) + " again</button></div>" +
    "</div>"
  );
}

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
  // THE REASON IS SHOWN WHEN IT SAYS SOMETHING THE WORD DOES NOT (MCP-11):
  // "valid (Valid)" repeated itself; "not valid (EndpointUnreachable)" does not.
  const beside = (echo) => (typeof s.reason === "string" && s.reason.length > 0 &&
    s.reason !== echo ? " (" + s.reason + ")" : "");
  if (s.valid === true) {
    return badge("green", "valid" + beside("Valid"));
  }
  if (s.valid === false) {
    return badge("unverified", "not valid" + beside("Invalid"));
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

/** The approval row of a readiness check run against a DRAFT plan: `approval.state`,
 *  `skipped`, `SubjectNotCreated` together and nothing wider -- the one blocking
 *  row a draft cannot make ready, because no Restore exists yet for an approver
 *  to sign (DRAFT-PREFLIGHT-NEVER-READY; the wizard's `readinessRefusal` says
 *  why the console, not the API, applies it). */
export function isDraftApprovalRow(check) {
  const c = check || {};
  return (
    c.id === "approval.state" &&
    c.state === "skipped" &&
    c.code === "SubjectNotCreated"
  );
}

/** Whether a check row gates the verdict. FAIL-CLOSED (review L2): only the two
 *  gatings the contract names as not gating -- `advisory` and `executionOnly`
 *  -- are left out; `blocking`, an absent gating and one this build does not
 *  recognise (the API maps an unknown value to `null`) all count as blocking. */
export function isBlockingRow(check) {
  const gating = (check || {}).gating;
  return gating !== "advisory" && gating !== "executionOnly";
}

/** Whether a row is one only the run itself can answer, and has not: an
 *  `executionOnly` row that is `unknown` or `skipped`. An execution-only row
 *  that says `notReady` is an answer, and is never summarised as "confirmed
 *  later" (review L2). */
export function answeredAtRun(check) {
  const c = check || {};
  return c.gating === "executionOnly" && (c.state === "unknown" || c.state === "skipped");
}

/** Whether a finished check is `unknown` for ONE reason only: its draft's
 *  approval row. Every other blocking row is `ready`, and at least one is --
 *  nothing checked is not everything passed -- and no execution-only row has
 *  said `notReady`. The wizard's step 5, its Create gate and the headline below
 *  all read this one rule. */
export function readyButForDraftApproval(preflight) {
  const p = preflight || {};
  if (p.state !== "unknown" || p.terminal !== true) {
    return false;
  }
  const checks = Array.isArray(p.checks) ? p.checks : [];
  const blocking = checks.filter(isBlockingRow);
  const others = blocking.filter((c) => !isDraftApprovalRow(c));
  const refusedAtRun = checks.some((c) => (c || {}).gating === "executionOnly" &&
    c.state === "notReady");
  return blocking.some(isDraftApprovalRow) && others.length > 0 &&
    others.every((c) => c.state === "ready") && !refusedAtRun;
}

/** A readiness result's headline (MCP round 2, R2-12; review L1).
 *
 *  NEVER `ready` OVER AN UNRESOLVED BLOCKING ROW. The aggregate's own badge,
 *  with two refinements that add words and never a green one:
 *
 *   - A restore check `unknown` only for its draft's approval row reads
 *     "needs approval": every other blocking check is ready, so the Restore
 *     can be created, and creating it is what asks an approver. That row is
 *     named for what it is and is NOT counted among the items the run
 *     confirms.
 *   - A `ready` check says how many execution-only rows only the run itself
 *     can answer ("N items are confirmed when ... runs"). Only those rows, and
 *     only while they are unknown or skipped, are summarised that way. */
export function readinessHeadline(preflight) {
  const p = preflight || {};
  const later = (Array.isArray(p.checks) ? p.checks : []).filter(answeredAtRun).length;
  const when = p.operation === "restore" ? "the restore runs" : "the run executes";
  const laterWords = String(later) + (later === 1 ? " item is" : " items are") +
    " confirmed when " + when;
  if (readyButForDraftApproval(p)) {
    return badge("pending", "needs approval") + " <span class=\"headline-qualifier\" " +
      "data-headline=\"needs-approval\">-- every other blocking check is ready, so the " +
      "Restore can be created; creating it requests the approval it needs before it runs" +
      (later > 0 ? ". " + laterWords.charAt(0).toUpperCase() + laterWords.slice(1) : "") +
      "</span>";
  }
  if (p.state === "ready" && later > 0) {
    return badge("green", "ready") + " <span class=\"headline-qualifier\" " +
      "data-headline=\"ready\">-- " + laterWords + "</span>";
  }
  return preflightVerdict(p.state);
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
  // A CHECK STILL RUNNING HAS NO RESULT TO APPLY OR NOT (MCP round 2, R2-11).
  // It read "does not apply to your current inputs / compared: nothing" until
  // it settled, which says the check is out of date when it has not answered
  // yet. Applicability is a property of a result; until there is one the
  // line says the check is running.
  if (p.terminal !== true && ["pending", "queued", "running"].indexOf(p.state) !== -1) {
    // AND ONE THIS PAGE STOPPED FOLLOWING says so instead: "this page reads it
    // again until then" would be a promise nothing keeps. The words and the
    // way to ask again are `checkStoppedBlock`'s, rendered beside this line.
    const stopped = stoppedReason(p);
    if (stopped !== "") {
      return (
        "<div class=\"applicability\" role=\"status\" data-applicability=\"unfinished\">" +
        badge("unverified",
          stopped === STOPPED_DEADLINE ? "did not finish" : "could not be read again") +
        "</div>"
      );
    }
    return (
      "<div class=\"applicability\" role=\"status\" data-applicability=\"checking\">" +
      badge("pending", "checking...") +
      "<p class=\"note\">" + esc(CHECKING_SENTENCE) + "</p>" +
      "</div>"
    );
  }
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
/** A controller-authored message, escaped, with its backticked spans shown as
 *  code (MCP round 2, R2-10): the controller writes "`primary`" and
 *  "`sha256:...`" and the page printed the backticks. Only a PAIRED span
 *  becomes code; an unpaired backtick stays the character it is. */
export function messageText(value) {
  if (typeof value !== "string" || value.length === 0) {
    return cell(value);
  }
  const parts = value.split("`");
  if (parts.length < 3) {
    return esc(value);
  }
  let out = "";
  for (let i = 0; i < parts.length; i += 1) {
    const last = i === parts.length - 1;
    if (i % 2 === 1 && !last) {
      out += "<code>" + esc(parts[i]) + "</code>";
    } else {
      out += (i % 2 === 1 ? "`" : "") + esc(parts[i]);
    }
  }
  return out;
}

export function checkTable(checks, empty) {
  // NINE FIELDS IN FOUR CELLS (MCP round 2, R2-3): nine columns were more
  // than a thousand pixels wider than step 5's card at 1440 px, because the
  // messages carry unbroken tokens -- an image digest, a signer key id -- and
  // two instants sat side by side. Every field is still printed, an absent
  // one as absent: the code under its check, the gating under the verdict,
  // the remedy and the scope under the message, and the two instants one per
  // line. The message and the remedy are prose and may break anywhere.
  const rows = (Array.isArray(checks) ? checks : []).map((c) => [
    "<code>" + cell(c.id) + "</code>" +
      "<span class=\"cell-sub prose\" data-field=\"code\">" + cell(c.code) + "</span>",
    checkVerdict(c.state) +
      "<span class=\"cell-sub\" data-field=\"gating\">" + cell(c.gating) + "</span>",
    "<span class=\"cell-sub-first prose\" data-field=\"message\">" + messageText(c.message) +
      "</span>" +
      "<span class=\"cell-sub prose\" data-field=\"remedy\">remedy: " + messageText(c.remedy) +
      "</span>" +
      "<span class=\"cell-sub prose\" data-field=\"scope\">scope: " + checkScope(c.scope) + "</span>",
    "<span class=\"cell-sub-first\" data-field=\"observed\">observed " + when(c.observedAt) +
      "</span><span class=\"cell-sub\" data-field=\"expires\">expires " + when(c.expiresAt) +
      "</span>",
  ]);
  return table(
    ["CHECK", "VERDICT", "FINDING", "WHEN"],
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

// ---------------------------------------------------------------------------
// THE ONE TIMESTAMP FORMATTER (MCP-7, MCP-15). Every instant a page SHOWS goes
// through [`when`]: a human-readable UTC reading with whole seconds --
// `2026-09-24 16:39:04 UTC` -- that never wraps mid-value, inside a `<time>`
// whose `datetime` and `title` carry the EXACT value the object recorded,
// nanoseconds and offset included, so nothing is lost and a hover or a copy
// gets the original. Tables used to print `2026-09-24T15:21:21.720607463Z`,
// broken across two lines at the `-`.
//
// UTC ON PURPOSE. The page reads no clock and no locale for a verdict; a
// reading in the viewer's own zone would make two people on one incident call
// read different times off one screen. The suffix says which zone it is.
// ---------------------------------------------------------------------------

const RFC3339 = /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$/;

/** The human reading of one instant: `YYYY-MM-DD HH:MM:SS UTC`, or `null`
 *  when `value` is not an RFC 3339 instant (or epoch milliseconds) this page
 *  can read. A value already in UTC is read digit for digit, so a fraction
 *  finer than JavaScript's milliseconds is never rounded into the seconds. */
export function humanInstant(value) {
  if (typeof value === "number") {
    return isFinite(value) ? humanInstant(new Date(value).toISOString()) : null;
  }
  if (typeof value !== "string") {
    return null;
  }
  const m = RFC3339.exec(value.trim());
  if (m === null) {
    return null;
  }
  if (m[8] === "Z" || m[8] === "z" || m[8] === "+00:00" || m[8] === "-00:00") {
    return m[1] + "-" + m[2] + "-" + m[3] + " " + m[4] + ":" + m[5] + ":" + m[6] + " UTC";
  }
  const at = Date.parse(m[1] + "-" + m[2] + "-" + m[3] + "T" + m[4] + ":" + m[5] + ":" + m[6] + m[8]);
  if (isNaN(at)) {
    return null;
  }
  const iso = new Date(at).toISOString();
  return iso.slice(0, 10) + " " + iso.slice(11, 19) + " UTC";
}

/** AN INSTANT, AS A PAGE SHOWS IT: [`humanInstant`]'s reading in a `<time>`
 *  carrying the exact recorded value in `datetime` and `title`; [`ABSENT`] for
 *  no value; and a value that is not an instant escaped as it arrived, never
 *  guessed at. Epoch milliseconds are accepted and shown the same way. */
export function when(value) {
  if (value === null || value === undefined || value === "") {
    return ABSENT;
  }
  const exact = typeof value === "number" ? rfc3339(value) : String(value);
  const human = humanInstant(value);
  if (human === null) {
    return esc(exact);
  }
  return "<time class=\"ts\" datetime=\"" + esc(exact) + "\" title=\"" + esc(exact) + "\">" +
    esc(human) + "</time>";
}

/** A LOCAL WALL-CLOCK READING, KEPT IN ITS OWN ZONE (console-ux-1 review M1).
 *  The controller renders a firing in the schedule's zone with its offset --
 *  `2026-10-25T02:30:00+02:00` -- and that reading IS the fact: on the
 *  fall-back day two firings share one wall time and differ only in offset.
 *  So the reading keeps its date, time and offset as written
 *  (`2026-10-25 02:30:00 +02:00`, `Z` as `UTC`), and is never converted to
 *  another zone; `null` for a value that is not an RFC 3339 instant. */
export function humanLocal(value) {
  if (typeof value !== "string") {
    return null;
  }
  const m = RFC3339.exec(value.trim());
  if (m === null) {
    return null;
  }
  const zone = m[8] === "Z" || m[8] === "z" ? "UTC" : m[8];
  return m[1] + "-" + m[2] + "-" + m[3] + " " + m[4] + ":" + m[5] + ":" + m[6] + " " + zone;
}

/** [`humanLocal`] as a page shows it: the wall time in its own zone inside a
 *  `<time>` whose `datetime` and `title` carry the exact value; the value
 *  escaped as it arrived when it is not an instant; [`ABSENT`] for none. */
export function whenLocal(value) {
  if (value === null || value === undefined || value === "") {
    return ABSENT;
  }
  const exact = String(value);
  const human = humanLocal(exact);
  if (human === null) {
    return esc(exact);
  }
  return "<time class=\"ts\" datetime=\"" + esc(exact) + "\" title=\"" + esc(exact) + "\">" +
    esc(human) + "</time>";
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

/** The bucket a restore PLAN writes its evidence to: `evidence.bucket` of the
 *  plan bytes, the runner's own `StorageUrl` block. `""` when the bytes carry no
 *  such block, or none this reader can see.
 *
 *  WHY THE PLAN AND NOT THE ARCHIVE. A restore's signed scorecard is written
 *  where its approved plan's `evidence:` block says -- for a point with no
 *  saved destination that is whatever bucket the plan named, and for a
 *  destination-backed one it is the evidence destination's bucket, while
 *  `spec.sourceArchive.url` is the `logweir-destination://` sentinel whose
 *  "bucket" is a destination NAME. A fetch command built from the source
 *  archive therefore pointed at the wrong bucket in both cases (PoC defect P5's
 *  class sweep). Used only to render a copyable command; nothing here
 *  addresses a store.
 *
 *  A line reader, not a YAML parser: the block is the one the console renders
 *  (`plan.js`, two-space indentation, JSON-quoted values), and a hand-written
 *  plan in another shape reads as `""`, which renders the placeholder. */
export function planEvidenceBucket(planBytes) {
  if (typeof planBytes !== "string" || planBytes.length === 0) {
    return "";
  }
  let inEvidence = false;
  for (const raw of planBytes.split("\n")) {
    const line = raw.replace(/\r$/, "");
    if (/^\S/.test(line)) {
      if (inEvidence) {
        return "";
      }
      inEvidence = /^evidence:\s*$/.test(line);
      continue;
    }
    if (!inEvidence) {
      continue;
    }
    const match = /^ {2}bucket:\s*(.*?)\s*$/.exec(line);
    if (match !== null) {
      const value = match[1];
      const quoted = /^"(.*)"$/.exec(value) || /^'(.*)'$/.exec(value);
      const bucket = quoted !== null ? quoted[1] : value;
      // A BUCKET NAME OR NOTHING (review L5). This value is plan text an
      // approver signed, not an API-validated URL, and it is pasted into a
      // shell: anything outside the S3 bucket grammar renders the placeholder.
      return isBucketName(bucket) ? bucket : "";
    }
  }
  return "";
}

/** The S3 bucket-name grammar the destination form and the API enforce:
 *  3-63 characters, lowercase letters, digits, dots and hyphens, beginning and
 *  ending with a letter or digit. */
export function isBucketName(value) {
  return typeof value === "string" && /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(value);
}

/** One POSIX shell word: unchanged when it is made only of characters no shell
 *  reads specially, otherwise single-quoted with every `'` closed, escaped and
 *  reopened (review L5). The fetch commands below are copied into a terminal,
 *  and their bucket and keys come from a status and a plan, not from this
 *  page. */
export function shellWord(value) {
  const text = String(value);
  if (/^[A-Za-z0-9._/:=@+%,-]+$/.test(text)) {
    return text;
  }
  return "'" + text.replace(/'/g, "'\\''") + "'";
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
    "aws s3 cp " + shellWord("s3://" + bucket + "/" + documentKey) + " ./" + documentFile,
    "aws s3 cp " + shellWord("s3://" + bucket + "/" + sidecarKey) + " ./" + sidecarFile,
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

/** An error from `api.js` as a string: the twin of [`errorBox`] -- the same
 *  headline, the server's own message and its status, code and request id,
 *  escaped. `live` false drops `role="alert"` for an error nested inside a
 *  region that already announces itself. */
export function errorBlock(error, live) {
  const parts = errorParts(error);
  return (
    "<div class=\"error\"" + (live === false ? "" : " role=\"alert\"") + ">" +
    "<p class=\"error-title\">" + esc(parts.title) + "</p>" +
    "<p class=\"error-message\">" + esc(parts.message) + "</p>" +
    (parts.detail.length > 0
      ? "<span class=\"error-status\">" + esc(parts.detail) + "</span>"
      : "") +
    (parts.signIn
      ? "<p class=\"actions\"><a class=\"button primary\" href=\"" + esc(signInHref(hereHash())) +
        "\">Sign in</a></p>"
      : "") +
    "</div>"
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
      // A NAME OR A UID THE ANSWER DID NOT CARRY IS LEFT OUT, NOT PRINTED
      // EMPTY (found by the PLAT-18.2 live pass): a manual Backup's answer is
      // projected without `metadata`, and the line read "Created Backup  (uid
      // )." -- a hole where an identity should be. The run's own name and uid
      // are in the panel's result block beside this line.
      result.outcome === "existing"
        ? "<p>" + kind + (shown ? " " + shown : "") + " already existed with exactly this " +
          "content" + (uid ? " (uid " + uid + ")" : "") + "; nothing new was created.</p>"
        : "<p>Created " + kind + (shown ? " " + shown : "") + (uid ? " (uid " + uid + ")" : "") +
          ".</p>",
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
  rows.push(["verified at", when(v.verifiedAt)]);
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
  // THE LOCAL TIME STAYS LOCAL (review M1): `when` would read it as an
  // instant and print it in UTC -- the AT (UTC) cell a second time -- and the
  // wall time the schedule fires at, and the repeated 02:30 of a fall-back
  // day, would be gone from the page.
  return [when(r.at), "<code>" + whenLocal(r.localTime) + "</code>", marker];
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
    "<p class=\"note\">Read in <code>" + esc(zone) + "</code>. The slot identity is always " +
    "the UTC instant, which is why the names stay unique and monotonic whatever the zone.</p>" +
    (typeof v.tzdb === "string" && v.tzdb.length > 0
      ? technicalDetails("the time-zone database compiled into the controller and the API: <code>" +
        esc(v.tzdb) + "</code>")
      : "") +
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
  "trigger not recorded (the run predates recorded triggers, or this API does not publish it)";

/** What "no revision" means, said the same way. */
export const NO_REVISION_SENTENCE =
  "revision not recorded (the run predates recorded revisions, or this API does not publish it)";

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
/** FACTS FOR WHOEVER DEBUGS, NOT FOR WHOEVER OPERATES (MCP-13): a digest, the
 *  time-zone library's version. They stay on the page, one disclosure away,
 *  rather than in the sentence an operator reads. `html` is OURS (a caller
 *  escapes every value in it). */
export function technicalDetails(html) {
  return "<details class=\"technical\"><summary>Technical details</summary><p class=\"note\">" +
    html + "</p></details>";
}

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

/** A FACT COPIED FROM A RUN'S SCORECARD, SHOWN FOR WHAT IT IS
 *  (SCORECARD-FACTS-UNVERIFIED-SHOWN). A Restore's `outcome`, `integrity` and
 *  `measured` are copied from its scorecard whatever the scorecard's verdict:
 *  until that document has verified they are the scorecard's CLAIM, and a page
 *  that printed them plainly beside an unverified badge would be vouching for
 *  numbers nobody checked. `verified` is the page's own verdict rule (the
 *  badge rule); an absent value stays the absent marker, with no caption. */
export const SCORECARD_CLAIM_CAPTION = "unverified scorecard claim";

export const SCORECARD_CLAIM_SENTENCE =
  "The outcome, integrity and measured values on this run are copied from its scorecard. The " +
  "scorecard's signature has not verified, so each is shown as the scorecard's claim, not as a " +
  "verified fact; the evidence section says why it is not verified.";

export function scorecardClaim(valueHtml, verified) {
  if (verified === true || valueHtml === ABSENT) {
    return valueHtml;
  }
  return valueHtml + " <span class=\"badge badge-unverified\" data-scorecard-claim=\"true\">" +
    SCORECARD_CLAIM_CAPTION + "</span>";
}

/** A CONSOLE LIST ROW'S VERDICT (CONSOLE-HISTORY-VALID-SHOWN-UNVERIFIED).
 *
 *  A product-API list item carries `OperationSummary`: `verificationState` and
 *  `verifiedSuccess` -- the latter computed by the API with the controller's
 *  own green-badge rule (`weirkeeper::verification::{backup,restore}_badge`,
 *  trust basis and a Restore's `outcome: pass` included) -- and no key id, no
 *  verification instant and no exit code. So a list row is green exactly when
 *  `verifiedSuccess` says so, and the green caption SAYS what the list does not
 *  carry instead of inventing it; every other row names its case from the
 *  summary's own word. The run's own page (and legacy mode, which reads the
 *  custom resource) still shows the full block. */
export const LIST_VERIFIED_CAPTION = "verified by weirkeeper";

/** WHAT A GREEN LIST BADGE DOES NOT CARRY, said ONCE under the table (MCP-16)
 *  rather than inside every row's badge, where it tripled the row height at
 *  1440 px and made a row eight lines tall at 1024 px. */
export const LIST_VERIFIED_NOTE =
  "SIGNED: a list row carries weirkeeper's verdict and not the key id or the instant it " +
  "verified at; the run's own page shows both.";

/** [`LIST_VERIFIED_NOTE`] as the line under a list, when any of `items` is a
 *  console list row (one carrying the API's summary verdict); the empty string
 *  otherwise, because a custom resource's own badge names its key and instant. */
export function listVerifiedNote(items) {
  const list = Array.isArray(items) ? items : [];
  return list.some((item) => (((item || {}).status) || {}).__summary !== undefined)
    ? "<p class=\"note\" id=\"list-verified-note\">" + esc(LIST_VERIFIED_NOTE) + "</p>"
    : "";
}

/** Each non-green `verificationState` of a list summary, in words. */
export const LIST_VERDICT_CASES = Object.freeze({
  valid: "the document verified, and the run did not succeed or its signer is not accepted now " +
    "-- the run's own page says which",
  invalid: "invalid -- the signature did not verify, or a digest did not match",
  notAttempted: "not attempted -- the controller could not check this document",
  noEvidence: "no evidence -- this run recorded no signed document",
  pending: "pending -- the run has not finished, or its verdict is not written yet",
  unknown: "unknown -- the controller's verdict could not be read",
});

/** The badge for a console list row, from its `__summary`. */
export function summaryBadge(summary) {
  const s = summary || {};
  if (s.verifiedSuccess === true && s.verificationState === "valid") {
    return badge("green", LIST_VERIFIED_CAPTION);
  }
  const named = LIST_VERDICT_CASES[s.verificationState];
  return badge("unverified", UNVERIFIED + ": " +
    (typeof named === "string" ? named : VERIFICATION_CASES.NotRecorded));
}

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
  // `RecordedBeforeRevocation` NAMES ITS OWN CASE on either result that can
  // carry it. The controller writes it beside `result: Untrusted` (D3 section
  // 7.4's row), and "rendered with the recorded instant" is owed to that
  // object, not only to a `Valid` one no controller writes.
  if (trust.basis === "RecordedBeforeRevocation" &&
    (v.result === "Valid" || v.result === "Untrusted")) {
    return VERIFICATION_CASES.RecordedBeforeRevocation;
  }
  if (v.result === "Valid") {
    if (!trustBlockAllowsGreen(v.trust)) {
      return VERIFICATION_CASES.NotRecorded;
    }
    return runSucceeded === true ? "" : VERIFICATION_CASES.RunNotSucceeded;
  }
  const named = VERIFICATION_CASES[v.result];
  return typeof named === "string" ? named : VERIFICATION_CASES.NotRecorded;
}

/** Whether a `trust.basis` the PRODUCT API published (`OperationTrust.basis`)
 *  leaves a `Valid` verdict green.
 *
 *  FOR THE API'S DTO ONLY -- NOT FOR A CUSTOM RESOURCE. The DTO has to spell
 *  something, and it spells an absent block as [`TRUST_BASIS_NOT_OBSERVED`];
 *  so here three answers collapse to yes: `Current`, `Historical`, and that
 *  absence. What is left is a basis this build does not know, and that one is
 *  not green, because a word this page cannot read is not a word it may treat
 *  as a pass. A custom resource CAN say "no block" by having none, so a
 *  present `basis: None` there is not an absence: read a raw resource with
 *  [`trustBlockAllowsGreen`]. */
export function basisAllowsGreen(basis) {
  if (typeof basis !== "string" || basis === TRUST_BASIS_NOT_OBSERVED) {
    return true;
  }
  return GREEN_BASES.indexOf(basis) !== -1;
}

/** Whether a RAW `status.evidence.verification.trust` leaves a `Valid`
 *  verdict green -- legacy mode's rule, and the rule for every object shaped
 *  like a custom resource (TRUST-VALID-BASIS-CLASS).
 *
 *  THE CONTROLLER'S RULE, `weirkeeper::verification::ValidBasis`: NO `trust`
 *  key at all -- or `trust: null`, the same absence (the CRD field is
 *  nullable, and every typed reader and the API read `null` as no block) --
 *  is D3 section 12's absent field and keeps the pre-existing rule (green); a
 *  PRESENT block is green only on `Current` or `Historical`. So
 *  `{basis: "None"}`, `{}`, `{basis: null}`, `Unverified`,
 *  `RecordedBeforeRevocation` and a word this build does not know are all not
 *  green -- the controller badge says `VerificationUntrusted` or
 *  `VerificationNotAttempted` for each, and `logweir-api` says `untrusted` or
 *  `notAttempted`. Reading a present `None` as absence here (review finding
 *  F1's rule, which belongs to the DTO) made legacy mode the one surface that
 *  painted those objects green (review LOW-1 of `claude/api-trust-state`). */
export function trustBlockAllowsGreen(trust) {
  if (trust === undefined || trust === null) {
    return true;
  }
  return typeof trust === "object" && typeof trust.basis === "string" &&
    GREEN_BASES.indexOf(trust.basis) !== -1;
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
  "not one this page recomputes. The product API also joins it with the controller's own " +
  "verdict on the point's Backup: a row whose Backup the controller refused (backupVerdict) " +
  "is never selectable, whatever its two axes say.";

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
