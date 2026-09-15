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
// kept, and a value carrying private-key text is never kept at all, whatever
// field it arrived in.
//
// WHY A RETRY CANNOT DUPLICATE AN OBJECT. Every create this page issues names
// its object: a name the viewer typed, or a name minted from the plan bytes.
// A retry after a lost response therefore sends the SAME name, and the API
// server answers `409 AlreadyExists` instead of creating a second object.
// `createOnce` then reads the stored object back and compares its spec with
// the draft's: the same content is the same operation, resolved to the object
// that exists; different content is a conflict, reported and never overwritten.

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

/** The words that open a private-key PEM. A draft never keeps a value that
 *  carries them, whichever field it was typed into. */
const KEY_MARKER = "PRIVATE KEY";

/** The key a draft and a mutation record share: one per form per namespace,
 *  plus an optional subject for forms that are about one object. A namespace
 *  name cannot contain a slash, so the parts cannot run into each other. */
export function formKey(ns, form, subject) {
  const base = String(ns || "") + "/" + String(form || "");
  return typeof subject === "string" && subject.length > 0 ? base + "/" + subject : base;
}

const drafts = new Map();

/** A copy of the draft kept under `key`, or `null`. */
export function readDraft(key) {
  const kept = drafts.get(key);
  return kept === undefined ? null : Object.assign({}, kept);
}

/** Keeps the declared `fields` of `values`, and nothing else.
 *
 *  AN ALLOWLIST, NOT A DENYLIST. A field a form does not name -- a password
 *  input added later, a file input, anything -- is never captured, so the
 *  safe default for a new field is "forgotten". A string carrying private-key
 *  text is dropped even from a declared field. Returns the kept copy. */
export function keepDraft(key, values, fields) {
  const kept = {};
  const source = values || {};
  for (const field of Array.isArray(fields) ? fields : []) {
    const value = source[field];
    if (typeof value === "boolean") {
      kept[field] = value;
    } else if (typeof value === "string" && value.indexOf(KEY_MARKER) === -1) {
      kept[field] = value;
    }
  }
  if (Object.keys(kept).length === 0) {
    drafts.delete(key);
    return null;
  }
  drafts.set(key, kept);
  return Object.assign({}, kept);
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
 *  beside a field. The API server's messages are passed on verbatim. */
export function fieldErrors(error, paths) {
  const fields = {};
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
    return { outcome: "created", object: object };
  } catch (error) {
    return resolveExisting(api, ns, plural, body, rules, error);
  }
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
 *  has started, its answer replaces the timeout. */
export function createMutation(options) {
  const opts = options || {};
  const setTimer = typeof opts.setTimer === "function"
    ? opts.setTimer
    : (fn, ms) => globalThis.setTimeout(fn, ms);
  const clearTimer = typeof opts.clearTimer === "function"
    ? opts.clearTimer
    : (id) => globalThis.clearTimeout(id);
  let state = IDLE;
  const listeners = [];

  function publish(next) {
    state = Object.freeze(next);
    for (const listener of listeners.slice()) {
      try {
        listener(state);
      } catch (ignored) {
        // A view's listener cannot corrupt the record other views read.
      }
    }
  }

  function answerable(attempt) {
    return (
      state.attempt === attempt &&
      (state.phase === "pending" || (state.phase === "failed" && state.timedOut === true))
    );
  }

  return {
    get state() {
      return state;
    },
    pending() {
      return state.phase === "pending";
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
     *  cleared: its answer is still on its way. */
    clear() {
      if (state.phase !== "pending" && state.phase !== "idle") {
        publish(Object.assign({}, IDLE, { attempt: state.attempt }));
      }
    },
    run(executor, runOptions) {
      if (state.phase === "pending") {
        return null;
      }
      const attempt = state.attempt + 1;
      const configured = (runOptions || {}).timeoutMs;
      const timeoutMs = typeof configured === "number" && configured > 0
        ? configured
        : (typeof opts.timeoutMs === "number" && opts.timeoutMs > 0 ? opts.timeoutMs : MUTATION_TIMEOUT_MS);
      publish({ phase: "pending", attempt: attempt, result: null, error: null, kind: null, timedOut: false });
      return new Promise((resolve) => {
        let answered = false;
        const answer = () => {
          if (!answered) {
            answered = true;
            resolve(state);
          }
        };
        const timer = setTimer(() => {
          if (state.attempt === attempt && state.phase === "pending") {
            const late = new Error(
              "no answer from the API server within " + String(Math.round(timeoutMs / 1000)) +
                " s; the request was not cancelled and its outcome is unknown",
            );
            late.kind = "unknown";
            publish({
              phase: "failed", attempt: attempt, result: null, error: late, kind: "unknown", timedOut: true,
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
                publish(Object.assign({}, IDLE, { attempt: attempt }));
              } else {
                publish({
                  phase: "succeeded", attempt: attempt, result: result, error: null, kind: null, timedOut: false,
                });
              }
            }
            answer();
          },
          (error) => {
            clearTimer(timer);
            if (answerable(attempt)) {
              publish({
                phase: "failed", attempt: attempt, result: null, error: error,
                kind: failureKind(error), timedOut: false,
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
