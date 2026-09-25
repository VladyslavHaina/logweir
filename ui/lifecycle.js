// lifecycle.js -- what lives and dies with one mounted route, and what
// deliberately does not.
//
// TWO HALVES, KEPT APART ON PURPOSE.
//
// THE ROUTE HALF. Reads belong to a view and may be cancelled when that view
// leaves. A route token (see `createRouteLifecycle` in `app.js`) carries an
// AbortSignal and a generation check; page modules use it before rendering,
// binding a listener, or starting a mutation, and every route-bound listener
// is removed when the token aborts.
//
// THE DURABLE HALF. A POST or PATCH the API server has accepted is not a read,
// and navigation must never cancel it or forget its answer. So the durable
// half never receives, stores or consults a route signal: a mutation record
// and a draft are keyed by NAMESPACE and FORM, they outlive the view that
// started them, and the next mount of the same form in the same namespace
// reads them back. Cancelling a read is a route concern; acknowledging a
// mutation is not, and the two share nothing but this file.
//
// WHAT A DRAFT IS, AND WHERE IT LIVES. The values a person typed into a form
// that has not yet produced a durable object. It is held IN THIS MODULE'S
// MEMORY and nowhere else: the page stores nothing in browser storage (rule 2
// of `scripts/check-ui-offline.sh`), so a draft survives a validation error, an
// API refusal, a network failure, a timeout and navigation between routes of
// the same loaded page -- and it does not survive a reload or a closed tab.
// That is the whole persistence contract. Only the fields a form declares are
// kept, and a value spelling the words that open a private-key PEM is dropped
// even from a declared field, whatever field it arrived in. That test is what
// `carriesKeyMaterial` below does and all it does: it reads words, so a
// headerless blob that spells none is beyond it, and the paragraph over that
// function says so rather than promising otherwise.
//
// WHY A RETRY CANNOT DUPLICATE AN OBJECT. Every create this page issues names
// its object: a name the viewer typed, or a name minted from the plan bytes.
// A retry after a lost response therefore sends the SAME name, and the API
// server answers `409 AlreadyExists` instead of creating a second object.
// `createOnce` then reads the stored object back and compares its spec with
// the draft's: the same content is the same operation, resolved to the object
// that exists; different content is a conflict, reported and never overwritten.

import {
  MUTATION_MACHINE,
  MUTATION_PHASES,
  startMachine,
  transitionError,
} from "./workflow.js";

// ============================================================== route half

export function active(lifecycle) {
  return lifecycle === undefined || lifecycle === null || lifecycle.isCurrent();
}

export function readOptions(lifecycle) {
  if (lifecycle === undefined || lifecycle === null || lifecycle.signal === undefined) {
    return undefined;
  }
  return { signal: lifecycle.signal };
}

export function cancelled(error, lifecycle) {
  return (
    (lifecycle !== undefined && lifecycle !== null && lifecycle.signal !== undefined &&
      lifecycle.signal.aborted) ||
    (error !== null && error !== undefined && error.name === "AbortError")
  );
}

/** Route-bound DOM listeners are removed when navigation aborts the lifecycle.
 *  Detached nodes cannot retain an actionable subscription after exit. */
export function listen(target, type, handler, lifecycle) {
  if (lifecycle !== undefined && lifecycle !== null && lifecycle.signal !== undefined) {
    target.addEventListener(type, handler, { signal: lifecycle.signal });
    return;
  }
  target.addEventListener(type, handler);
}

/** Whether a followed `Preflight` still owes a READ -- defect P8 (poc-install,
 *  2026-09-24), and the one rule every page that starts a check follows it by.
 *
 *  A CREATE ANSWER IS NEVER THE VERDICT. The product API answers a create --
 *  and a REPLAY of one, which is already terminal -- with the stored object
 *  projected WITHOUT recomputing staleness: `applicable: false` and one
 *  `unverifiable` reason, "this response did not recompute staleness; read the
 *  preflight itself for a current verdict". A follower that stopped at a
 *  terminal create answer therefore painted "ready / does not apply to your
 *  current inputs / could not be checked" for a replayed check, and a page with
 *  no follower at all (Destinations -> Test access) painted `pending` for a
 *  check that had been `ready` for minutes. So the first read is owed whatever
 *  the create said, and after it a follower stops at the first TERMINAL read.
 *
 *  @param {object|null|undefined} preflight what the follower holds now
 *  @param {number} reads how many GETs it has already made for it
 *  @returns {boolean} */
export function owesRead(preflight, reads) {
  if (preflight === null || preflight === undefined || typeof preflight.id !== "string" ||
    preflight.id.length === 0) {
    return false;
  }
  return reads === 0 || preflight.terminal !== true;
}

/** Runs `release` when the route token aborts. A view subscribes to a durable
 *  mutation record with this, so the SUBSCRIPTION ends with the view while the
 *  record -- and the request behind it -- carries on. */
export function whenLeft(lifecycle, release) {
  if (lifecycle === undefined || lifecycle === null || lifecycle.signal === undefined) {
    return;
  }
  if (lifecycle.signal.aborted) {
    release();
    return;
  }
  lifecycle.signal.addEventListener("abort", release, { once: true });
}

// ============================================================ durable half

/** How long a mutation waits for the API server before its outcome is
 *  reported as UNKNOWN. The request is not aborted -- an accepted create
 *  cannot be un-sent -- and a late answer still settles the record. */
export const MUTATION_TIMEOUT_MS = 30000;

/** THE WORDS THAT OPEN A PRIVATE-KEY PEM, and only those.
 *
 *  Every label OpenSSL and OpenSSH write for a private key spells the same two
 *  words -- `PRIVATE KEY` (PKCS#8), `RSA`/`EC`/`DSA PRIVATE KEY` (PKCS#1 and
 *  SEC1), `ENCRYPTED PRIVATE KEY`, `OPENSSH PRIVATE KEY` -- so the two words
 *  are the first alternative, IN ANY CASE and across any run of whitespace
 *  including a line break. The earlier `indexOf("PRIVATE KEY")` matched
 *  neither a lower-case paste nor a reflowed header, and a guard either of
 *  those walks past is not a guard.
 *
 *  The second alternative is a PEM `BEGIN` line whose label joins the words
 *  with a dash or an underscore. It is deliberately NOT the first
 *  alternative's job: `private-key` and `private_key` on their own are legal
 *  Kubernetes object names, and this test also runs over a draft's ordinary
 *  fields -- an archive Secret honestly called `minio-private-key` must not be
 *  silently dropped from a form.
 *
 *  WHAT IT DOES NOT CATCH, said here rather than promised away: a headerless
 *  base64 body, a DER or PKCS#12 blob, and anything else that never spells the
 *  words. `refuseKeyMaterial` in `pages/approvals.js` carries the other half
 *  of the rule -- the file's NAME -- for exactly that reason. */
const KEY_MARKER = /private\s+key|-{3,}\s*begin[^\n]{0,40}?private[\s_-]*key/i;

/** Whether `text` spells the words that open a private-key PEM. Pure, so it is
 *  checkable without a browser, and used by both halves of the rule: a draft
 *  never keeps such a value, and the approvals form never sends one. */
export function carriesKeyMaterial(text) {
  return typeof text === "string" && KEY_MARKER.test(text);
}

/** The key a draft and a mutation record share: one per form per namespace,
 *  plus an optional subject for forms that are about one object. A namespace
 *  name cannot contain a slash, so the parts cannot run into each other. */
export function formKey(ns, form, subject) {
  const base = String(ns || "") + "/" + String(form || "");
  return typeof subject === "string" && subject.length > 0 ? base + "/" + subject : base;
}

const drafts = new Map();

/** A copy of the draft kept under `key`, or `null`.
 *
 *  THE COPY HAS NO PROTOTYPE, because a page reads it by field name and a
 *  field name is data. `{}.constructor` is a function; `Object.create(null)`
 *  has no such answer to give, so a lookup that finds nothing reads
 *  `undefined` -- see the same rule in `renderApprovalsIndex`. */
export function readDraft(key) {
  const kept = drafts.get(key);
  return kept === undefined ? null : Object.assign(Object.create(null), kept);
}

/** Keeps the declared `fields` of `values`, and nothing else.
 *
 *  AN ALLOWLIST, NOT A DENYLIST. A field a form does not name -- a password
 *  input added later, a file input, anything -- is never captured, so the
 *  safe default for a new field is "forgotten". A string carrying private-key
 *  text is dropped even from a declared field. Returns the kept copy. */
export function keepDraft(key, values, fields) {
  const kept = Object.create(null);
  const source = values || {};
  for (const field of Array.isArray(fields) ? fields : []) {
    const value = source[field];
    if (typeof value === "boolean") {
      kept[field] = value;
    } else if (typeof value === "string" && !carriesKeyMaterial(value)) {
      kept[field] = value;
    }
  }
  if (Object.keys(kept).length === 0) {
    drafts.delete(key);
    return null;
  }
  drafts.set(key, kept);
  return Object.assign(Object.create(null), kept);
}

/** Forgets a draft: after the object it described exists, or on request. */
export function dropDraft(key) {
  drafts.delete(key);
}

/** What kind of failure an error is, for the words a form shows beside it.
 *
 *    invalid   -- the input was refused (422/400, or the page's own checks);
 *    conflict  -- an object with this name exists with different content;
 *    rejected  -- the API server refused the request (403, 404, ...);
 *    refused   -- this page refused before sending anything;
 *    unknown   -- no answer, a timeout or a 5xx: the object may or may not
 *                 exist, and only a retry under the same name can tell. */
export function failureKind(error) {
  if (error !== null && typeof error === "object" && typeof error.kind === "string") {
    return error.kind;
  }
  if (error instanceof RangeError) {
    return "refused";
  }
  const status = error !== null && typeof error === "object" && typeof error.status === "number"
    ? error.status
    : 0;
  if (status === 0 || status === 408 || status >= 500) {
    return "unknown";
  }
  if (status === 409) {
    return "conflict";
  }
  if (status === 400 || status === 422) {
    return "invalid";
  }
  return "rejected";
}

/** An error for a refusal this page made before sending anything. */
export function refusal(message, extra) {
  const error = new Error(message);
  error.kind = "refused";
  return Object.assign(error, extra || {});
}

/** An error for input the page's own checks refused, carrying the message for
 *  each field by the form's own field name. Nothing was sent. */
export function invalidInput(fields, message) {
  const error = new Error(
    typeof message === "string" && message.length > 0
      ? message
      : "the form was not submitted: fix the fields marked below",
  );
  error.kind = "invalid";
  error.fields = fields || {};
  return error;
}

/** The field-level messages an error carries, by the form's own field names.
 *
 *  Two sources, merged: the page's own checks (`error.fields`), and the API
 *  server's `Status.details.causes[]`, whose `field` is a JSON path such as
 *  `spec.bootstrapServers` or `metadata.name`. `paths` maps a path prefix to a
 *  form field; the longest matching prefix wins. A cause no prefix matches is
 *  returned in `unmatched`, so it is still shown -- beside the form rather than
 *  beside a field. The API server's messages are passed on verbatim.
 *
 *  `fields` has no prototype: a page reads it as `errors[name]`, and a bag
 *  read by a name is never a plain `{}` in this tree. */
export function fieldErrors(error, paths) {
  const fields = Object.create(null);
  const unmatched = [];
  const own = (error || {}).fields;
  if (own !== null && typeof own === "object") {
    for (const name of Object.keys(own)) {
      addMessage(fields, name, own[name]);
    }
  }
  const causes = (((error || {}).details) || {}).causes;
  if (Array.isArray(causes)) {
    for (const cause of causes) {
      const c = cause || {};
      const path = typeof c.field === "string" ? c.field : "";
      const message = typeof c.message === "string" && c.message.length > 0
        ? c.message
        : String(c.reason || "invalid");
      const target = fieldForPath(path, paths);
      if (target === null) {
        unmatched.push(path.length > 0 ? path + ": " + message : message);
      } else {
        addMessage(fields, target, message);
      }
    }
  }
  return { fields: fields, unmatched: unmatched };
}

function addMessage(fields, name, message) {
  if (typeof message !== "string" || message.length === 0) {
    return;
  }
  if (!Array.isArray(fields[name])) {
    fields[name] = [];
  }
  fields[name].push(message);
}

function fieldForPath(path, paths) {
  let best = null;
  let length = -1;
  for (const pair of Array.isArray(paths) ? paths : []) {
    const prefix = pair[0];
    const matches = path === prefix ||
      path.indexOf(prefix + ".") === 0 ||
      path.indexOf(prefix + "[") === 0;
    if (matches && prefix.length > length) {
      best = pair[1];
      length = prefix.length;
    }
  }
  return best;
}

/** True for the API server's answer to a create whose name is taken. */
export function alreadyExists(error) {
  return (
    error !== null && typeof error === "object" &&
    error.status === 409 && error.reason === "AlreadyExists"
  );
}

/** Compares the spec a draft would create with the spec an existing object
 *  carries.
 *
 *  `rules.defaults` names the fields the API server fills in when a create
 *  omits them (`{"concurrencyPolicy": "Forbid"}`), applied to both sides so an
 *  omitted default is not a difference. `rules.ignore` names fields that may
 *  legitimately change after creation and so say nothing about whether this
 *  is the same operation (`spec.suspend`). A `null` is treated as absent.
 *  Everything else is compared exactly: strings byte for byte, arrays in
 *  order. Returns `{equal, differences}`, the differences as `spec.` paths. */
export function compareSpec(submitted, stored, rules) {
  const r = rules || {};
  const left = pruned(submitted);
  const right = pruned(stored);
  const defaults = r.defaults || {};
  for (const path of Object.keys(defaults)) {
    fillDefault(left, path, defaults[path]);
    fillDefault(right, path, defaults[path]);
  }
  for (const path of Array.isArray(r.ignore) ? r.ignore : []) {
    removePath(left, path);
    removePath(right, path);
  }
  const differences = [];
  collectDifferences(left, right, "spec", differences);
  return { equal: differences.length === 0, differences: differences };
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function pruned(value) {
  if (Array.isArray(value)) {
    return value.map(pruned);
  }
  if (isPlainObject(value)) {
    const out = {};
    for (const key of Object.keys(value)) {
      if (value[key] !== null && value[key] !== undefined) {
        out[key] = pruned(value[key]);
      }
    }
    return out;
  }
  return value === undefined ? null : value;
}

function fillDefault(root, path, value) {
  if (!isPlainObject(root)) {
    return;
  }
  const parts = path.split(".");
  let at = root;
  for (let i = 0; i < parts.length - 1; i += 1) {
    if (!isPlainObject(at[parts[i]])) {
      return;
    }
    at = at[parts[i]];
  }
  const last = parts[parts.length - 1];
  if (at[last] === undefined) {
    at[last] = value;
  }
}

function removePath(root, path) {
  if (!isPlainObject(root)) {
    return;
  }
  const parts = path.split(".");
  let at = root;
  for (let i = 0; i < parts.length - 1; i += 1) {
    if (!isPlainObject(at[parts[i]])) {
      return;
    }
    at = at[parts[i]];
  }
  delete at[parts[parts.length - 1]];
}

function collectDifferences(left, right, path, out) {
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
      out.push(path);
      return;
    }
    for (let i = 0; i < left.length; i += 1) {
      collectDifferences(left[i], right[i], path + "[" + String(i) + "]", out);
    }
    return;
  }
  if (isPlainObject(left) || isPlainObject(right)) {
    if (!isPlainObject(left) || !isPlainObject(right)) {
      out.push(path);
      return;
    }
    const keys = Object.keys(left);
    for (const key of Object.keys(right)) {
      if (keys.indexOf(key) === -1) {
        keys.push(key);
      }
    }
    keys.sort();
    for (const key of keys) {
      collectDifferences(left[key], right[key], path + "." + key, out);
    }
    return;
  }
  if (left !== right) {
    out.push(path);
  }
}

/** Creates `body` in `ns`, idempotently by name.
 *
 *  `201` is `{outcome: "created", object}`. A `409 AlreadyExists` is answered
 *  by reading the stored object back: the same spec (see `compareSpec`) is
 *  `{outcome: "existing", object}` -- the retry of a create whose response was
 *  lost resolves to the object that first request made, and a second click
 *  can never make a second object. A different spec throws a `conflict` error
 *  naming the stored object's UID and the differing fields; nothing is
 *  overwritten, because this page has no update to overwrite with.
 *
 *  NO ROUTE SIGNAL, IN EITHER REQUEST. The read-back is part of acknowledging
 *  a durable operation, not a view's read, so navigation cannot cut it short. */
export async function createOnce(api, ns, plural, body, rules) {
  try {
    const object = await api.create(ns, plural, body);
    return { outcome: createdOutcome(object), object: object };
  } catch (error) {
    return resolveExisting(api, ns, plural, body, rules, error);
  }
}

/** Whether a create MADE the object or RESOLVED TO one that already existed.
 *
 *  TWO MODES, ONE ANSWER. In legacy mode a retry after a lost response is a
 *  `409 AlreadyExists` and [`resolveExisting`] decides; the create that
 *  actually made the object never carries this marker and reads `created`. In
 *  console mode the product API resolves the retry itself -- the idempotency
 *  key is the same, so the same object is returned with `replayed: true` --
 *  and `ui/client.js` records that on the projected object. Either way the
 *  form says "already existed" for the one and "created" for the other, and a
 *  second click can never claim to have made a second object. */
export function createdOutcome(object) {
  const record = (object || {}).__contract;
  return record !== null && typeof record === "object" && record.replayed === true
    ? "existing"
    : "created";
}

/** The second half of [`createOnce`], for a caller that issues its own
 *  `create` and hands this the error: anything but `409 AlreadyExists` is
 *  rethrown untouched; `AlreadyExists` is resolved to the stored object when
 *  its spec matches `body`'s, and becomes a `conflict` when it does not. */
export async function resolveExisting(api, ns, plural, body, rules, error) {
  if (!alreadyExists(error)) {
    throw error;
  }
  const name = (((body || {}).metadata) || {}).name;
  let stored;
  try {
    stored = await api.get(ns, plural, name);
  } catch (readError) {
    throw unconfirmed(name, error, readError);
  }
  const compared = compareSpec((body || {}).spec, (stored || {}).spec, rules);
  if (compared.equal) {
    return { outcome: "existing", object: stored };
  }
  const meta = (stored || {}).metadata || {};
  const conflict = new Error(error.message);
  conflict.kind = "conflict";
  conflict.status = error.status;
  conflict.reason = error.reason;
  conflict.differences = compared.differences;
  conflict.existing = {
    name: typeof meta.name === "string" ? meta.name : name,
    uid: typeof meta.uid === "string" ? meta.uid : "",
    creationTimestamp: typeof meta.creationTimestamp === "string" ? meta.creationTimestamp : "",
  };
  throw conflict;
}

function unconfirmed(name, createError, readError) {
  const error = new Error(
    "an object named " + String(name) + " already exists, and reading it back to compare it " +
      "with this draft failed: " + String((readError || {}).message || readError),
  );
  error.status = typeof (readError || {}).status === "number" ? readError.status : createError.status;
  error.reason = typeof (readError || {}).reason === "string" && readError.reason.length > 0
    ? readError.reason
    : createError.reason;
  // A read-back that found NOTHING raced a deletion; one that could not be made
  // at all leaves the question open. Either way a retry is the way to find out.
  error.kind = error.status === 403 || error.status === 401 ? "rejected" : "unknown";
  return error;
}

const IDLE = Object.freeze({
  phase: "idle",
  attempt: 0,
  result: null,
  error: null,
  kind: null,
  timedOut: false,
  about: null,
});

/** One form's mutation record: `idle`, `pending`, `succeeded` or `failed`.
 *
 *  `run(executor)` starts an attempt and returns a promise of the settled
 *  state -- or `null`, without calling the executor, while an attempt is still
 *  pending. That refusal is the duplicate-submission guard: it holds whatever
 *  the button looks like, because the pending phase is set BEFORE the executor
 *  runs its first await.
 *
 *  An executor that returns `{outcome: "abandoned"}` sent nothing (its route
 *  left during client-side preparation) and returns the record to `idle`.
 *
 *  A TIMEOUT IS AN UNKNOWN OUTCOME, NOT A FAILURE OF THE REQUEST. After
 *  `timeoutMs` the record reads `failed` with `kind: "unknown"` and
 *  `timedOut: true`, and the promise resolves so the form can offer a retry.
 *  The request itself is left alone; if it answers later and no newer attempt
 *  has started, its answer replaces the timeout.
 *
 *  AN ATTEMPT SAYS WHAT IT WAS ABOUT. `run(executor, {about})` copies `about`
 *  onto every state that attempt publishes, so a form can tell an outcome
 *  about the values on screen now from an outcome about the values that were
 *  on screen when the button was clicked. It is the form's own small record
 *  (the wizard keeps the plan hash and the minted name there) and nothing
 *  here reads inside it. */
export function createMutation(options) {
  const opts = options || {};
  const setTimer = typeof opts.setTimer === "function"
    ? opts.setTimer
    : (fn, ms) => globalThis.setTimeout(fn, ms);
  const clearTimer = typeof opts.clearTimer === "function"
    ? opts.clearTimer
    : (id) => globalThis.clearTimeout(id);
  // THE RECORD'S MOVES ARE NAMED (PLAT-18.1). Every publish below goes through
  // `workflow.js`'s mutation machine, which refuses an event the current state
  // does not accept -- so "settle an attempt that is not in flight" and
  // "start a second attempt over a pending one" are transition ERRORS with a
  // name, not expressions repeated beside each call site. The published shape
  // is unchanged: `phase` and `timedOut` come from the machine's own table.
  const run = startMachine(MUTATION_MACHINE);
  let state = IDLE;
  const listeners = [];

  function publish(event, next) {
    // A SETTLE THAT CANNOT MOVE MOVES NOTHING, AND SAYS SO WHERE SOMEBODY IS
    // LISTENING. `run()`'s two settle handlers publish from inside a promise
    // chain nobody awaits, so a TransitionError thrown here would leave the
    // browser with an unhandled rejection and no record of what happened. The
    // machine's table is the contract and an illegal move is still refused --
    // `send` above throws for every caller that can catch it, including the
    // exported `send` the suite drives -- but the answer to a move this record
    // cannot make is to leave the record where it is.
    if (!run.can(event)) {
      return transitionError(MUTATION_MACHINE.name, run.state, event, run.accepts());
    }
    run.send(event);
    const shape = MUTATION_PHASES[run.state];
    state = Object.freeze(
      Object.assign({}, next, { phase: shape.phase, timedOut: shape.timedOut }),
    );
    for (const listener of listeners.slice()) {
      try {
        listener(state);
      } catch (ignored) {
        // A view's listener cannot corrupt the record other views read.
      }
    }
    return null;
  }

  function answerable(attempt) {
    return state.attempt === attempt && (run.state === "pending" || run.state === "unanswered");
  }

  return {
    get state() {
      return state;
    },
    /** The machine's own state: `idle`, `pending`, `unanswered`, `succeeded`
     *  or `failed`. `unanswered` is the one the published `phase` cannot tell
     *  apart from `failed`, and it is the only state a late answer may still
     *  settle. */
    get machineState() {
      return run.state;
    },
    /** The names of the transitions this record has taken, in order. */
    get transitions() {
      return run.events;
    },
    pending() {
      return run.state === "pending";
    },
    /** Sends a named transition directly, and THROWS a `TransitionError` when
     *  the record is not in a state that accepts it. Nothing on the page calls
     *  this -- `run` and `clear` below drive the machine -- and it is exported
     *  so the suite can assert that an illegal move is refused by name rather
     *  than absorbed. */
    send(event) {
      return run.send(event);
    },
    subscribe(listener) {
      listeners.push(listener);
      return () => {
        const at = listeners.indexOf(listener);
        if (at !== -1) {
          listeners.splice(at, 1);
        }
      };
    },
    /** Returns a settled record to `idle`. A pending attempt is never
     *  cleared: its answer is still on its way, and the machine has no `clear`
     *  out of `pending` to express that with. This asks before it sends, so an
     *  ordinary clear on an in-flight record is a no-op and not a throw --
     *  which is the behaviour every form on this page already relies on. */
    clear() {
      if (run.can("clear") && run.state !== "idle") {
        publish("clear", Object.assign({}, IDLE, { attempt: state.attempt }));
      }
    },
    run(executor, runOptions) {
      // `start` is absent from the machine's `pending` state, and that absence
      // IS the duplicate-submission guard. Asking rather than sending keeps a
      // second click a no-op for the caller while leaving the move illegal.
      if (!run.can("start")) {
        return null;
      }
      const attempt = state.attempt + 1;
      const configured = (runOptions || {}).timeoutMs;
      const timeoutMs = typeof configured === "number" && configured > 0
        ? configured
        : (typeof opts.timeoutMs === "number" && opts.timeoutMs > 0 ? opts.timeoutMs : MUTATION_TIMEOUT_MS);
      const about = (runOptions || {}).about;
      const said = about === undefined ? null : about;
      publish("start", {
        attempt: attempt, result: null, error: null, kind: null, about: said,
      });
      return new Promise((resolve) => {
        let answered = false;
        const answer = () => {
          if (!answered) {
            answered = true;
            resolve(state);
          }
        };
        const timer = setTimer(() => {
          if (state.attempt === attempt && run.state === "pending") {
            const late = new Error(
              "no answer from the API server within " + String(Math.round(timeoutMs / 1000)) +
                " s; the request was not cancelled and its outcome is unknown",
            );
            late.kind = "unknown";
            publish("timeout", {
              attempt: attempt, result: null, error: late, kind: "unknown", about: said,
            });
          }
          answer();
        }, timeoutMs);
        let started;
        try {
          started = Promise.resolve(executor());
        } catch (thrown) {
          started = Promise.reject(thrown);
        }
        started.then(
          (result) => {
            clearTimer(timer);
            if (answerable(attempt)) {
              if (result !== null && typeof result === "object" && result.outcome === "abandoned") {
                publish("abandon", Object.assign({}, IDLE, { attempt: attempt }));
              } else {
                publish("succeed", {
                  attempt: attempt, result: result, error: null, kind: null, about: said,
                });
              }
            }
            answer();
          },
          (error) => {
            clearTimer(timer);
            if (answerable(attempt)) {
              publish("fail", {
                attempt: attempt, result: null, error: error,
                kind: failureKind(error), about: said,
              });
            }
            answer();
          },
        );
      });
    },
  };
}

const mutations = new Map();

/** The one mutation record for `key` (see `formKey`) for the life of the
 *  loaded page. Every mount of the same form in the same namespace reads the
 *  same record, so a create still pending when its view was left is still
 *  pending -- and still refuses a second click -- when the view comes back.
 *  `options` (timers, timeout) apply only when this call creates the record. */
export function mutationFor(key, options) {
  let record = mutations.get(key);
  if (record === undefined) {
    record = createMutation(options);
    mutations.set(key, record);
  }
  return record;
}

const watchers = new WeakMap();

/** Subscribes ONE view `owner` (the node a page renders into) to a mutation
 *  record under `key`, replacing that owner's previous subscription to the same
 *  key -- a form re-rendered after an answer must not leave its earlier
 *  listener behind -- and ending when the route token aborts. The listener
 *  only runs while the route is current. */
export function watchMutation(owner, key, mutation, listener, lifecycle) {
  let byKey = watchers.get(owner);
  if (byKey === undefined) {
    byKey = new Map();
    watchers.set(owner, byKey);
  }
  const prior = byKey.get(key);
  if (prior !== undefined) {
    prior();
  }
  const off = mutation.subscribe((state) => {
    if (active(lifecycle)) {
      listener(state);
    }
  });
  const release = () => {
    off();
    if (byKey.get(key) === release) {
      byKey.delete(key);
    }
  };
  byKey.set(key, release);
  whenLeft(lifecycle, release);
  return release;
}

// ============================================================ asking a check again

// ASKING A CHECK AGAIN IS NOT REPLAYING THE LAST ANSWER (P14, poc-upgrade-2).
//
// A check -- a `Preflight`, a `TopicDiscovery` -- answers a question about a
// moment. The product API names the object it creates from the request's
// `Idempotency-Key` (D0: one key, one object, for ever), so a key composed
// from the question alone is the SAME key every time the same inputs are
// asked about: the schedule form's "Check readiness", clicked with unchanged
// inputs after its check had expired, came back `200 replayed: true` with that
// expired check -- "does not apply to your current inputs" -- and no click
// could make a fresh one until the controller garbage-collected the old
// object an hour later. D0 does not allow the API to answer that key with a
// second object, and it says what a client does instead: "a later deliberate
// operation uses a new key".
//
// SO THE KEY CARRIES AN INTENT TOKEN, kept exactly as long as the check it
// named can still be the answer. Asking again while that check is pending,
// running or current -- a retry after a lost response, a second click inside
// its validity -- sends the same token and replays, which is what an
// idempotent retry is for. Once the check is SPENT (the page read it expired,
// inapplicable, failed or cancelled; or a replay answers with it already
// spent), the token is renewed and the next ask is a new check. The page load
// mints its own tokens, so a reload asks afresh rather than inheriting a key.
//
// The token lives in this module's memory with the drafts, and like them it
// holds nothing but a random string.

const checkIntents = new Map();

function mintIntentToken() {
  const source = globalThis.crypto;
  if (source === undefined || source === null || typeof source.getRandomValues !== "function") {
    throw refusal(
      "this page will not start a check here: keeping a check's key distinct from an earlier " +
        "one needs the platform's random source, and it is unavailable",
    );
  }
  const bytes = source.getRandomValues(new Uint8Array(16));
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** The intent under which the form `intent` asks `question` now: the one it
 *  holds for that question, or a new one. `held` is the check the page shows
 *  for this form, and when it is the one this intent produced and `spent`
 *  says it can no longer be the answer, the intent is renewed first. */
export function checkIntent(intent, question, held, spent) {
  let entry = checkIntents.get(intent);
  const heldId = ((held || {}).id) || "";
  if (entry !== undefined && entry.question === question && heldId.length > 0 &&
    entry.checkId === heldId && typeof spent === "function" && spent(held)) {
    entry = undefined;
  }
  if (entry === undefined || entry.question !== question) {
    entry = { question: question, token: mintIntentToken(), checkId: "" };
    checkIntents.set(intent, entry);
  }
  return entry;
}

/** Forgets every intent. THE SUITE'S SEAM; a page never calls it. */
export function resetCheckIntents() {
  checkIntents.clear();
}

/** Asks a check's question: `start(token)` sends it under the form's intent
 *  token and answers `{item, replayed}`. A REPLAY THAT IS ALREADY SPENT IS
 *  NOT AN ANSWER: the intent is renewed and the question asked once more,
 *  under a new key, and the answer says which check it replaced
 *  (`renewedFrom`). At most two creates per ask. */
export async function askCheck(ask) {
  const a = ask || {};
  let entry = checkIntent(a.intent, a.question, a.held, a.spent);
  let answer = await a.start(entry.token);
  let item = ((answer || {}).item) || null;
  entry.checkId = ((item || {}).id) || "";
  if (answer && answer.replayed === true && item !== null && typeof a.spent === "function" &&
    a.spent(item)) {
    const spentId = item.id;
    // COMPARE-AND-SWAP (review L4): renew only while the intent is still the
    // entry this ask used. An overlapping ask that renewed first has already
    // started the new check, and this one asks under ITS key -- a replay of
    // that check, not a second one.
    if (checkIntents.get(a.intent) === entry) {
      checkIntents.delete(a.intent);
    }
    entry = checkIntent(a.intent, a.question, null, null);
    answer = await a.start(entry.token);
    item = ((answer || {}).item) || null;
    entry.checkId = ((item || {}).id) || "";
    answer = Object.assign({}, answer, { renewedFrom: spentId });
  }
  return answer;
}

/** Whether a `Preflight` the page holds can no longer answer the same
 *  question: it is terminal and either produced no verdict (failed,
 *  cancelled), or says its validity has passed, or -- on a read -- no longer
 *  applies to the inputs. A pending or running check is never spent: it is
 *  the answer being waited for. */
export function preflightSpent(check) {
  const c = check || {};
  // A CHECK THIS PAGE STOPPED FOLLOWING IS SPENT, finished or not (see
  // `followCheck`): it did not answer within the longest time a check may
  // take, or it could not be read again, and "Run the check again" means a
  // new check -- a replay would hand back the one that did not finish.
  if (followStopped(c) !== "") {
    return true;
  }
  if (c.terminal !== true) {
    return false;
  }
  if (c.state === "failed" || c.state === "cancelled") {
    return true;
  }
  // EVERY STALE REASON BUT ONE SAYS THE CHECK NO LONGER ANSWERS THE QUESTION:
  // `expired` (which a replay now names itself), `planHashChanged`,
  // `referentChanged` and the rest. `unverifiable` is "this answer could not
  // compare" -- above all a create answer's "not recomputed" -- and is never
  // a verdict about the check: the follow's read decides whether it applies.
  const reasons = Array.isArray(c.staleReasons) ? c.staleReasons : [];
  return reasons.some((r) => (r || {}).reason !== "unverifiable");
}

/** Whether a `TopicDiscovery` can no longer answer the same question: it is
 *  terminal and produced no inventory, or its inventory is stale. */
export function discoverySpent(discovery) {
  const d = discovery || {};
  if (followStopped(d) !== "") {
    return true;
  }
  return d.terminal === true && (d.state !== "succeeded" || d.stale === true);
}

// ============================================================ following a check

// A CHECK IS FOLLOWED UNTIL IT ANSWERS OR UNTIL IT CAN NO LONGER ANSWER
// (poc-upgrade-3's P15). Every page that starts a check used to read it back a
// fixed number of times -- Test connection 30 s, the schedule form and the
// list page's Backup readiness panel 40 s, Test access 60 s, restore step 5
// 90 s -- while a `Preflight` may run for its own `timeoutSeconds`, 120 by
// default and up to 600, plus the 90 s its Job is given to start. A check that
// took 64 s was left on the panel as "this page reads it again until then" for
// as long as the page stayed open, and nothing read it again.
//
// THE DEADLINE IS THE LONGEST A CHECK MAY TAKE, NOT A GUESS AT A TYPICAL ONE.
// The product API publishes no deadline for a check in flight: `expiresAt` is
// its RESULT's validity and exists only once there is a result (by which
// time the check is terminal and the follow is over), and neither the check's
// `timeoutSeconds` nor its Job's `activeDeadlineSeconds` is in the view. So a
// follow reads for the product's documented maximum -- the largest
// `timeoutSeconds` the product API accepts, plus the Job's start margin
// (`DEADLINE_MARGIN_SECONDS`, weirkeeper `check/job.rs`), plus a grace for the
// controller's status write -- measured from when THIS PAGE began to follow,
// on this page's own clock. A follow starts after its check was created, so
// it never gives up before the check's own deadline; and no server timestamp
// is compared with a browser clock that may be minutes off. A check the
// controller settles (a `DeadlineExceeded` Job included) ends the follow at
// its first terminal read, long before that bound.
//
// POLITELY. The gap starts at two seconds and grows by half each read, to ten
// at most: a check that settles in ten seconds is seen about as fast as
// before, and one the controller never settles costs about seventy-five reads
// over twelve minutes -- never a hot loop. A read that fails for a reason that
// passes (the network, `408`, `429`, a `5xx`) is tried again on the same
// schedule, inside the same deadline -- up to five times in a row; a sixth
// is no longer a blip, and the page says it could not read the check.
//
// AND IT ENDS IN WORDS, NEVER ON THE CHECKING SENTENCE. When the deadline
// passes without a result, or a read is refused outright, the page is handed
// the check marked `followStopped` (`deadline` or `unreadable`). The shared
// renderer then says the check did not finish -- or could not be read -- and
// offers "Run the check again", and `preflightSpent`/`discoverySpent` count
// that check as spent, so the next ask is a NEW check and not a replay of
// the one that did not finish.

/** The largest `timeoutSeconds` a `Preflight` may ask for: the product API
 *  refuses anything outside 30..600 (`routes/preflights.rs`) and the CRD's
 *  schema says the same (`crds/preflight.rs`). */
export const PREFLIGHT_TIMEOUT_CEILING_SECONDS = 600;

/** The largest `timeoutSeconds` a `TopicDiscovery` may ask for
 *  (`routes/topic_discoveries.rs`'s `MAX_TIMEOUT_SECONDS`). */
export const DISCOVERY_TIMEOUT_CEILING_SECONDS = 300;

/** What a check Job is given beyond its own budget to be scheduled, pulled
 *  and started: `activeDeadlineSeconds = timeoutSeconds + 90` (weirkeeper
 *  `check/job.rs`'s `DEADLINE_MARGIN_SECONDS`, D2 section 4.3). */
export const CHECK_JOB_MARGIN_SECONDS = 90;

/** The controller's time to notice a finished or expired Job and write the
 *  terminal status: the Job is watched, and a running check is requeued every
 *  ten seconds besides (`REQUEUE_RUNNING_SECS`). */
export const CHECK_SETTLE_GRACE_SECONDS = 30;

/** How long a page follows a `Preflight` it holds: twelve minutes. */
export const PREFLIGHT_FOLLOW_MS =
  (PREFLIGHT_TIMEOUT_CEILING_SECONDS + CHECK_JOB_MARGIN_SECONDS + CHECK_SETTLE_GRACE_SECONDS) * 1000;

/** How long a page follows a `TopicDiscovery` it started: seven minutes. */
export const DISCOVERY_FOLLOW_MS =
  (DISCOVERY_TIMEOUT_CEILING_SECONDS + CHECK_JOB_MARGIN_SECONDS + CHECK_SETTLE_GRACE_SECONDS) * 1000;

/** How many failed reads in a row a follow tries past: five is about half a
 *  minute of an API that does not answer, which covers a restart. */
export const FOLLOW_FAILURES_TOLERATED = 5;

/** The first gap between reads, and the most any gap grows to. */
export const FOLLOW_FIRST_GAP_MS = 2000;
export const FOLLOW_MAX_GAP_MS = 10000;

/** The wait before read `attempt` (0-based): 2 s, 3 s, 4.5 s, 6.75 s, then
 *  10 s for every read after. */
export function followGap(attempt) {
  const n = Math.max(0, Math.floor(Number(attempt) || 0));
  return Math.min(FOLLOW_MAX_GAP_MS, Math.round(FOLLOW_FIRST_GAP_MS * Math.pow(1.5, n)));
}

/** Why a page stopped following a check: its deadline passed without a
 *  result, or a read was refused outright. `render.js` spells the same two
 *  strings (it imports nothing), and the suite holds them together. */
export const FOLLOW_DEADLINE = "deadline";
export const FOLLOW_UNREADABLE = "unreadable";

/** Why this page stopped following `check`, or `""` while it has not. */
export function followStopped(check) {
  const why = (check || {}).followStopped;
  return why === FOLLOW_DEADLINE || why === FOLLOW_UNREADABLE ? why : "";
}

/** `check`, marked as no longer followed and why. The mark is the page's own
 *  and is never sent anywhere; an `unreadable` one carries the refusal's
 *  words so the page can say what the read answered. */
export function stopFollowing(check, why, error) {
  const marked = Object.assign({}, check || {}, { followStopped: why });
  if (why === FOLLOW_UNREADABLE) {
    marked.followError = String(((error || {}).message) || "the read failed");
  }
  return marked;
}

/** A fresher answer for a check the page stopped following KEEPS the mark
 *  while that answer still has no result: a read the page makes for another
 *  reason (the restore submit's re-read) is not a follow, and without the mark
 *  the check would read "this page reads it again until then" once more. A
 *  terminal answer is a result and replaces the mark. */
export function keepStopMark(held, fresh) {
  if (fresh === null || fresh === undefined || held === null || held === undefined ||
    fresh.id !== held.id || fresh.terminal === true || followStopped(held) === "") {
    return fresh;
  }
  const marked = Object.assign({}, fresh, { followStopped: held.followStopped });
  if (held.followError !== undefined) {
    marked.followError = held.followError;
  }
  return marked;
}

/** Whether a failed read is worth asking again: a network failure (a fetch
 *  rejects with no status), `408`, `429` or a `5xx`. Anything else -- the
 *  check is gone, the session may no longer read it, this page refused to ask
 *  -- will not change by asking again. */
export function passingReadFailure(error) {
  const e = error || {};
  if (e.kind === "refused") {
    return false;
  }
  const status = e.status;
  if (status === undefined || status === null) {
    return true;
  }
  return status === 408 || status === 429 || status >= 500;
}

// THE FOLLOWS RUNNING NOW, by the id of the check each reads, so a page that
// mounts again over a check it remembers can tell whether something is still
// reading it (`isFollowed`). Each entry is the follow's own `keep`, which is
// the answer to "is this follow still alive": a follow whose route has left
// is not, even before it wakes up to notice.
const follows = new Map();

/** Whether a live follow is reading the check `id` now. */
export function isFollowed(id) {
  const keep = follows.get(String(id || ""));
  return keep !== undefined && keep() === true;
}

/** Follows one check until a read answers terminal, the check's deadline
 *  passes, a read is refused outright, or `keep()` says the page no longer
 *  wants it. Answers the check it ended on (marked when it stopped), or
 *  `null` when `keep()` ended it.
 *
 *  `first` is the check the page holds (a create answer owes one read even
 *  when it is terminal: `owesRead`); `read(current)` makes one GET and
 *  answers the check to hold next; `show(check)` paints it; `cancelled(error)`
 *  says a failed read was the route's own abort; `budgetMs` is the deadline
 *  (`PREFLIGHT_FOLLOW_MS` unless given). `wait` and `now` default to this
 *  page's `setTimeout` and `Date.now`. The deadline is measured both on the
 *  clock and as the sum of the gaps waited, whichever is further, so a
 *  `wait` that returns at once still reaches it. */
export async function followCheck(follow) {
  const f = follow || {};
  const wait = typeof f.wait === "function"
    ? f.wait
    : (ms) => new Promise((done) => { globalThis.setTimeout(done, ms); });
  const now = typeof f.now === "function" ? f.now : () => Date.now();
  const keep = typeof f.keep === "function" ? () => f.keep() === true : () => true;
  const show = typeof f.show === "function" ? f.show : () => {};
  const budget = typeof f.budgetMs === "number" ? f.budgetMs : PREFLIGHT_FOLLOW_MS;
  const id = String(((f.first || {}).id) || "");
  const started = now();
  let waited = 0;
  let attempts = 0;
  let reads = 0;
  let failures = 0;
  let current = f.first;
  if (id.length > 0) {
    follows.set(id, keep);
  }
  try {
    for (;;) {
      if (!owesRead(current, reads)) {
        return current;
      }
      const left = budget - Math.max(now() - started, waited);
      if (left <= 0) {
        // THE DEADLINE PASSED WITHOUT A RESULT, after one last read at it.
        const stopped = stopFollowing(current, FOLLOW_DEADLINE);
        if (keep()) {
          await show(stopped);
        }
        return stopped;
      }
      const gap = Math.min(followGap(attempts), left);
      await wait(gap);
      waited += gap;
      attempts += 1;
      if (!keep()) {
        return null;
      }
      let fresh;
      try {
        fresh = await f.read(current);
      } catch (error) {
        if (!keep() || (typeof f.cancelled === "function" && f.cancelled(error) === true)) {
          return null;
        }
        failures += 1;
        if (passingReadFailure(error) && failures <= FOLLOW_FAILURES_TOLERATED) {
          continue;
        }
        const stopped = stopFollowing(current, FOLLOW_UNREADABLE, error);
        await show(stopped);
        return stopped;
      }
      if (!keep()) {
        return null;
      }
      reads += 1;
      failures = 0;
      current = fresh === null || fresh === undefined ? current : fresh;
      await show(current);
    }
  } finally {
    if (id.length > 0 && follows.get(id) === keep) {
      follows.delete(id);
    }
  }
}

/** Wires every "Run the check again" control rendered for `check` under
 *  `node` to `again`. The control is `render.js`'s `checkStoppedBlock`; the
 *  page decides what asking again means (its own start, which now counts the
 *  stopped check as spent). */
export function wireCheckRetry(node, check, again, lifecycle) {
  const id = String(((check || {}).id) || "");
  if (id.length === 0 || node === null || node === undefined ||
    typeof node.querySelectorAll !== "function") {
    return;
  }
  for (const button of node.querySelectorAll(".check-retry")) {
    if (button.getAttribute("data-check") === id) {
      listen(button, "click", () => {
        if (active(lifecycle)) {
          again();
        }
      }, lifecycle);
    }
  }
}
