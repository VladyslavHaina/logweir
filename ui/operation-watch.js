// operation-watch.js -- FOLLOWING ONE DURABLE OPERATION, AND THE BOUNDED D3
// READS THE FOUR NEW SURFACES MAKE.
//
// ===========================================================================
// WHAT A WATCH IS, AND THE ONE THING IT IS NOT
// ===========================================================================
//
// A watch is a READ that repeats. It opens the product API's server-sent
// stream for one operation, renders every document that arrives, and stops --
// on a terminal operation whose verification has settled, or when the route
// that owns it goes away.
//
// ABORTING A WATCH NEVER CANCELS THE OPERATION (D3 section 2.6, and the same
// P17 rule `ui/api.js` states for a create: "a route leaving after a click
// must not cancel an operation the API server has already accepted"). Closing
// this stream closes a connection. The Job keeps running, the controller keeps
// reconciling, and coming back to the same route resumes the same operation
// because the custom resource is the source of truth -- not because anything
// was kept alive in this page.
//
// THAT IS ALSO WHY A REFRESH AND A NEW TAB WORK. Nothing about this watch is
// state: the route carries the namespace, the kind, the name and the UID, and
// the DTO that arrives is complete every time. There is no incremental
// reduction to lose and no cursor to resume.
//
// ===========================================================================
// THE TWO TRANSPORTS, AND WHY THE FALLBACK IS NOT A DEGRADED MODE
// ===========================================================================
//
// CONSOLE MODE gets the stream: `GET .../operations/{kind}/{name}/events`,
// same origin, no token in the identifier, opened by `ui/api.js` -- the only
// module in this tree that issues a request of any kind. A connection that
// fails is retried with backoff 1 s, 2 s, 5 s, 30 s plus jitter, and after
// THREE failed connects this module stops trying to stream and polls instead.
// Three, and not "for ever with a longer delay", because the interesting
// cause of a failed stream is not a flaky network: it is a proxy that buffers
// or refuses `text/event-stream`, and that one never resolves on its own. A
// page that retried it silently would show an operation frozen at the last
// document it happened to receive.
//
// LEGACY MODE has no stream to open -- `kubectl proxy` serves the custom
// resource and there is no SSE route in front of it -- so it polls the object
// every 5 s while it is active, and every 30 s after five consecutive errors.
// The slow-down is not a retreat: five consecutive failures is a cluster that
// is not answering, and hammering it every 5 s while an operator reads the
// error box helps nobody.
//
// NOTHING HERE READS A CLOCK TO DECIDE ANYTHING. `Date.now` is used for
// jitter -- a number that makes two tabs reconnect at different instants --
// and for nothing else. Whether an operation is stale, fresh, finished or
// verified is decided by the API and the controller, from fields on the
// document; this module reads `terminal` and the recorded verification state
// and stops. It never compares an instant with the browser's own clock.
//
// ===========================================================================
// THE SECOND HALF OF THIS FILE: THE D3 READS
// ===========================================================================
//
// `ui/client.js` is the one object every page reads through, and it is not
// this task's to extend (D3 section 11 names `api.js`, this module, the pages
// and `contract.js`; it does not name `client.js`). So the four D3 surfaces
// read through the small, explicit layer below: it asks `client.js` which mode
// the page is in -- the one probe, decided once at boot, which this module
// reads and never re-decides -- and then calls `ui/api.js` directly, decoding
// every answer with `ui/contract.js`'s declared shapes.
//
// EVERY ROUTE IT CAN REACH IS IN ONE FROZEN TABLE below, for the same reason
// `CONSOLE_ACTIONS` is one in `api.js`: the set of identifiers any D3 page can
// address is a list a reviewer reads in one place.
//
// IN LEGACY MODE each of these is the CUSTOM RESOURCE, read by `kubectl proxy`
// with the viewer's own credential, and validated by the same `contract.js`
// against the kind's own required fields. Where a flow has no legacy shape at
// all -- a catalog's point list lives in page ConfigMaps, which this page
// holds no verb on and must not -- the call is refused BY NAME with a sentence
// saying which API serves it, exactly as D2's three domains are.

import {
  consoleClusterGet,
  consoleClusterList,
  consoleCreate,
  consoleGet,
  consoleList,
  consoleOperation,
  consoleSub,
  openOperationStream,
  serverTime,
} from "./api.js";
import { CONSOLE, apiClient, mode, sessionToken } from "./client.js";
import {
  decodeCatalogPoints,
  decodeCatalogRequest,
  decodeCatalogSigners,
  decodeD3Item,
  decodeD3List,
  decodeD3Operation,
  decodeD3OperationFrame,
  contractFailure,
} from "./contract.js";
import { active, cancelled, readOptions } from "./lifecycle.js";

/** THE SERVER'S OWN CLOCK, RE-EXPORTED UNDER THE NAME A PAGE USES.
 *
 *  `ui/api.js` records the `Date` of the most recent answer -- the
 *  `logweir-api` process in console mode, kube-apiserver through `kubectl
 *  proxy` in legacy mode. The keys view needs it to decide whether an
 *  evaluation is fresh (D3 section 7.7), and a page module may not name an
 *  `api.js` export outside the six the page contract allows
 *  (`ui_lint.rs::the_suspend_toggle_is_the_only_update`) -- which is the right
 *  rule and not one to widen for a clock. So it travels through this module,
 *  as `serverClock`, with the api.js name appearing nowhere under `ui/pages/`. */
export function serverClock() {
  return serverTime();
}

// ===========================================================================
// the watch
// ===========================================================================

/** The reconnect delays, in order, and the last one repeats. */
export const BACKOFF_MS = Object.freeze([1000, 2000, 5000, 30000]);

/** The three reasons the server's `end` frame carries, and what each MEANS
 *  (`StreamEnd` in `crates/logweir-api/src/status.rs`).
 *
 *  `end` IS NOT A DOCUMENT. Its payload is `{"reason": "..."}` and nothing
 *  else -- no operation, no envelope. The first round decoded it as an
 *  operation and stopped the watch on every one of them, which is wrong in
 *  BOTH directions: a normal close raised a decode error on screen, and a
 *  `maxDuration` close -- the 300-second connection ceiling, which a running
 *  backup hits while it is still running -- stopped the watch and left the
 *  page showing a mid-run snapshot with nothing saying it had stopped looking.
 *
 *  `maxDuration` IS THE SERVER SAYING "RECONNECT", and it is the only one of
 *  the three that is not the end of anything. `settled` is the end of the
 *  operation; `vanished` is the end of the object. */
export const STREAM_END_REASONS = Object.freeze({
  /** Terminal, and the verification verdict is in. Nothing more is coming. */
  settled: "settled",
  /** The CONNECTION's 300-second ceiling. The operation is untouched. */
  maxDuration: "maxDuration",
  /** The object is gone. A new object under the same name is a different run. */
  vanished: "vanished",
});

/** The reason an `end` frame carries, or `null` for one this build cannot read.
 *
 *  An UNREADABLE `end` IS TREATED AS `maxDuration` BY THE CALLER, which is the
 *  side that costs a connection rather than the side that shows a running
 *  operation as a finished one. */
export function endReason(data) {
  let parsed;
  try {
    parsed = JSON.parse(data);
  } catch (notJson) {
    return null;
  }
  const reason = (parsed || {}).reason;
  return typeof reason === "string" && Object.prototype.hasOwnProperty.call(
    STREAM_END_REASONS, reason,
  )
    ? reason
    : null;
}

/** How much jitter one delay may carry, as a fraction of itself. Two tabs
 *  watching the same operation must not reconnect in lockstep. */
export const JITTER = 0.25;

/** How often the legacy poller reads while the operation is active. */
export const POLL_MS = 5000;

/** And after [`ERRORS_BEFORE_SLOWING`] consecutive errors. */
export const SLOW_POLL_MS = 30000;

/** Five consecutive errors is a cluster that is not answering. */
export const ERRORS_BEFORE_SLOWING = 5;

/** Three failed connects is a transport that will not work here. */
export const CONNECTS_BEFORE_POLLING = 3;

/** The delay before reconnect attempt `attempt` (0-based), with jitter.
 *
 *  PURE, AND THAT IS WHY IT TAKES `fraction`. The caller supplies a number in
 *  [0, 1); the suite supplies 0 and 0.999 and asserts both bounds. A function
 *  that reached for `Math.random` itself would be one no test could pin. */
export function backoffFor(attempt, fraction) {
  const index = attempt < 0 ? 0 : Math.min(attempt, BACKOFF_MS.length - 1);
  const base = BACKOFF_MS[index];
  const f = typeof fraction === "number" && isFinite(fraction) ? Math.min(Math.max(fraction, 0), 1) : 0;
  return Math.round(base * (1 + JITTER * f));
}

/** Whether this document is one the watch may stop on.
 *
 *  TWO CONDITIONS, NOT ONE. `terminal` alone is not enough: a run that has
 *  exited still has a verification the controller has not written yet, and a
 *  watch that stopped at the exit would leave the page showing `pending`
 *  evidence for ever with nothing on screen saying it stopped looking. D3's
 *  own `end` event has the same two conditions.
 *
 *  A DOCUMENT THIS FUNCTION DOES NOT UNDERSTAND IS NOT SETTLED, and a
 *  document it understands is one of TWO. Console mode is handed the product
 *  API's DTO and reads `terminal` + `verification.state`. LEGACY MODE IS
 *  HANDED THE CUSTOM RESOURCE, which publishes neither: it has no top-level
 *  `terminal` and no normalized `verification.state`, so the DTO rule alone
 *  answered `false` for every legacy document ever written and a console left
 *  open on a FINISHED run polled the kube-apiserver every 5 s for ever. The
 *  same two conditions are read off `status.phase` and
 *  `status.evidence.verification.result` below. */
export function isSettled(operation) {
  const o = operation || {};
  if (typeof o.terminal === "boolean") {
    if (o.terminal !== true) {
      return false;
    }
    const verification = o.verification || {};
    return verification.state !== "pending";
  }
  // NO `terminal` AT ALL IS THE CUSTOM RESOURCE, NOT AN UNREADABLE DTO.
  const status = o.status;
  if (status !== null && typeof status === "object" && !Array.isArray(status)) {
    return legacySettled(status);
  }
  return false;
}

/** The phases the custom resource writes that nothing follows.
 *
 *  `Cancelled` IS DELIBERATELY NOT HERE. `PHASE_OF` in `ui/client.js` can
 *  spell it because the normalized vocabulary has the word, but no cancel
 *  route exists in v1 and no controller writes the phase; listing it would be
 *  this page claiming to know a state the product does not produce. A phase
 *  this build does not recognise keeps the watch open, which is the side that
 *  costs a poll rather than the side that shows a running run as finished. */
export const LEGACY_TERMINAL_PHASES = Object.freeze(["Succeeded", "Failed", "Refused"]);

/** The keys a status names when it recorded a document worth verifying. */
const LEGACY_EVIDENCE_KEYS = Object.freeze([
  "receiptKey", "payloadKey", "scorecardKey", "sidecarKey", "offsetReportKey",
]);

/** THE SAME TWO CONDITIONS, READ OFF THE DOCUMENT LEGACY MODE ACTUALLY GETS.
 *
 *  `status.phase` is the custom resource's own word and
 *  `status.evidence.verification.result` is its own verdict; neither is
 *  spelled the way the DTO spells them and neither is derived here. A
 *  TERMINAL phase with a recorded verdict is settled, and so is a terminal
 *  phase that named no evidence at all -- a run refused before it executed, or
 *  one that failed without writing a document, has no verdict coming and
 *  waiting for one is waiting for ever.
 *
 *  A RESIDUE THIS CANNOT CLOSE, SAID OUT LOUD. `logweir-api` PROJECTS an
 *  absent verification beside recorded evidence keys to `notAttempted` with
 *  the controller's reason; the custom resource has no such projection, so in
 *  legacy mode a terminal run whose controller recorded keys and never wrote a
 *  verdict keeps the watch open. That is the conservative side and the page
 *  says the verification is not recorded, but it is a poll that does not stop
 *  and the mode is the one without a normalizing API in front of it. */
function legacySettled(status) {
  if (LEGACY_TERMINAL_PHASES.indexOf(status.phase) === -1) {
    return false;
  }
  const evidence = status.evidence;
  if (evidence === null || evidence === undefined || typeof evidence !== "object") {
    return true;
  }
  const result = (evidence.verification || {}).result;
  if (typeof result === "string" && result.length > 0) {
    return true;
  }
  return !LEGACY_EVIDENCE_KEYS.some(
    (key) => typeof evidence[key] === "string" && evidence[key].length > 0,
  );
}

/** FOLLOWS ONE OPERATION UNTIL IT SETTLES OR THE ROUTE LEAVES.
 *
 *  `onUpdate(document, meta)` is called with each decoded operation and
 *  `{transport, attempt, error}` -- the transport in use, how many connects
 *  have failed, and the last error if there is one -- so the view can say what
 *  it is doing rather than going quiet.
 *
 *  Returns `{stop}`. `stop()` is idempotent and is also wired to
 *  `lifecycle.signal`, so navigation disposes the watch without the page
 *  remembering to.
 *
 *  `deps` is the seam the suite drives: `{now, setTimer, clearTimer, random,
 *  EventSourceClass, read, modeOf}`. Nothing in it is optional for a test and
 *  nothing in it is needed by a browser. */
export function watchOperation(ns, kind, name, onUpdate, lifecycle, deps) {
  const d = deps || {};
  const setTimer = d.setTimer || ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = d.clearTimer || ((handle) => clearTimeout(handle));
  const random = d.random || (() => Math.random());
  const modeOf = d.modeOf || mode;
  const read = d.read || ((options) => readOperation(ns, kind, name, options));

  let stopped = false;
  let timer = null;
  let stream = null;
  let failedConnects = 0;
  // Consecutive `end` frames that did NOT end the operation, with no document
  // in between. A server that closed a stream immediately and for ever would
  // otherwise be reconnected to for ever; this counter falls back to polling
  // on the same threshold a failed connect does.
  let emptyEnds = 0;
  let pollErrors = 0;
  let transport = modeOf() === CONSOLE && streamable(d.EventSourceClass)
    ? "stream"
    : "poll";

  function stop() {
    if (stopped) {
      return;
    }
    stopped = true;
    if (timer !== null) {
      clearTimer(timer);
      timer = null;
    }
    closeStream();
  }

  function closeStream() {
    if (stream !== null) {
      // `close()` ends the CONNECTION. The operation is untouched.
      stream.close();
      stream = null;
    }
  }

  function done() {
    return stopped || !active(lifecycle);
  }

  function deliver(document, error) {
    if (done()) {
      return;
    }
    onUpdate(document, {
      transport: transport,
      attempt: failedConnects,
      error: error === undefined ? null : error,
    });
    if (document !== null && isSettled(document)) {
      stop();
    }
  }

  function openStream() {
    if (done()) {
      return;
    }
    let source;
    try {
      source = openOperationStream(ns, kind, name, d.EventSourceClass);
    } catch (noStream) {
      transport = "poll";
      poll();
      return;
    }
    stream = source;
    source.onmessage = null;
    // FOUR NAMED EVENT TYPES AND NO DEFAULT HANDLER (D3 section 2.6).
    // `operation` carries the bare view, `reset` carries the same shape as a
    // fresh snapshot after the server dropped the resume point, `end` carries
    // `{reason}` and says why this CONNECTION closed, and `heartbeat` says
    // only that the connection is alive. TWO OF THE FOUR ARE DOCUMENTS AND
    // TWO ARE NOT, which is why `end` has its own handler rather than sharing
    // this one. An unnamed `message` is a document this contract does not
    // describe, and it is deliberately not handled: a stream that started
    // sending something else is a contract change, not a render.
    const document = (event) => {
      failedConnects = 0;
      emptyEnds = 0;
      try {
        // THE FRAME IS THE BARE VIEW, NOT THE READ ROUTE'S ENVELOPE. See
        // `decodeD3OperationFrame`: `send_view` serializes an `OperationView`
        // and the GET serializes an `OperationViewResponse` around one.
        deliver(decodeD3OperationFrame(JSON.parse(event.data)).value);
      } catch (bad) {
        deliver(null, bad);
      }
    };
    source.addEventListener("operation", document);
    source.addEventListener("reset", document);
    source.addEventListener("end", (event) => {
      // `end` CARRIES A REASON AND NO DOCUMENT, and only two of the three
      // reasons are an end. Feeding it to `document` was a decode error per
      // stream; stopping on all three was a watch that gave up every 300 s on
      // any operation that took longer than that.
      const reason = endReason(event.data);
      if (reason === STREAM_END_REASONS.settled) {
        stop();
        return;
      }
      closeStream();
      if (reason === STREAM_END_REASONS.vanished) {
        // THE OBJECT IS GONE AND THIS PAGE DOES NOT GO QUIET ABOUT IT. The
        // last snapshot stays on screen -- it is what was true -- with the
        // server's own reason beside it, and the watch stops rather than
        // re-reading a name that now belongs to nobody.
        const gone = new Error(
          "The API server no longer has this operation: it was deleted while this page was " +
            "following it. Nothing here was cancelled by this page, and an object created " +
            "later under the same name is a different run -- re-open it from the list to read " +
            "that one.",
        );
        gone.reason = "OperationVanished";
        deliver(null, gone);
        stop();
        return;
      }
      // `maxDuration`, or an `end` this build cannot read: the CONNECTION
      // ended, not the operation. Reconnect, and give up on the stream only
      // after as many empty closes as failed connects.
      if (done()) {
        return;
      }
      emptyEnds += 1;
      if (emptyEnds >= CONNECTS_BEFORE_POLLING) {
        transport = "poll";
        poll();
        return;
      }
      timer = setTimer(openStream, backoffFor(emptyEnds - 1, random()));
    });
    source.addEventListener("heartbeat", () => {
      failedConnects = 0;
    });
    source.addEventListener("error", () => {
      if (done()) {
        return;
      }
      // A BROWSER RECONNECTS BY ITSELF while the stream is merely interrupted
      // (`readyState === CONNECTING`), and that reconnect carries
      // `Last-Event-ID` -- which is the resume this page cannot spell by hand,
      // because `EventSource` takes no headers. So this arm acts only on a
      // CLOSED stream, which is the one the browser has given up on.
      if (source.readyState !== CLOSED) {
        return;
      }
      closeStream();
      failedConnects += 1;
      if (failedConnects >= CONNECTS_BEFORE_POLLING) {
        transport = "poll";
        poll();
        return;
      }
      timer = setTimer(openStream, backoffFor(failedConnects - 1, random()));
    });
  }

  async function poll() {
    if (done()) {
      return;
    }
    try {
      const document = await read(readOptions(lifecycle));
      pollErrors = 0;
      deliver(document);
    } catch (error) {
      if (cancelled(error, lifecycle)) {
        stop();
        return;
      }
      pollErrors += 1;
      deliver(null, error);
    }
    if (done()) {
      return;
    }
    timer = setTimer(poll, pollErrors >= ERRORS_BEFORE_SLOWING ? SLOW_POLL_MS : POLL_MS);
  }

  if (lifecycle !== undefined && lifecycle !== null && lifecycle.signal !== undefined) {
    if (lifecycle.signal.aborted) {
      stopped = true;
    } else {
      lifecycle.signal.addEventListener("abort", stop, { once: true });
    }
  }

  if (!stopped) {
    if (transport === "stream") {
      openStream();
    } else {
      poll();
    }
  }
  return { stop: stop, transportNow: () => transport };
}

// `EventSource.CLOSED`. Spelled here as the number the specification fixes,
// because the constant lives on a constructor this module does not import in
// legacy mode and may not exist at all in a test's fake.
const CLOSED = 2;

function streamable(EventSourceClass) {
  return typeof EventSourceClass === "function" || typeof globalThis.EventSource === "function";
}

// ===========================================================================
// the D3 reads
// ===========================================================================

/** THE ROUTES THIS PAGE MAY ADDRESS FOR D3, AND NOTHING ELSE.
 *
 *  `console` is the product API plural; `legacy` is the custom-resource plural
 *  `kubectl proxy` serves, or `null` where there is no legacy shape and the
 *  flow is refused by name. `cluster` marks the one cluster-scoped kind. */
export const D3_ROUTES = Object.freeze({
  protection: Object.freeze({
    console: "protection-policies", legacy: "protectionpolicies", cluster: false,
    what: "Protection policies",
  }),
  catalog: Object.freeze({
    console: "catalogs", legacy: "recoverycatalogs", cluster: false,
    what: "Recovery catalogs",
  }),
  retention: Object.freeze({
    console: "retention-policies", legacy: "retentionpolicies", cluster: false,
    what: "Retention policies",
  }),
  trust: Object.freeze({
    console: "trust-policies", legacy: "trustpolicies", cluster: true,
    what: "Trust policies",
  }),
});

/** The refusal a flow with no route in this mode answers. It names WHICH API
 *  serves the flow and what to run instead, and it is never a silent empty
 *  table. */
export function noD3Route(what) {
  const error = new Error(
    what + " is served by the Logweir product API and not by the Kubernetes API this page is " +
      "talking to. Open the console (`logweir-api`) to use it, or read the custom resources " +
      "with kubectl; this page will not pretend to a route it cannot reach.",
  );
  error.kind = "rejected";
  error.status = 501;
  error.reason = "NoConsoleRoute";
  return error;
}

/** True when the page is talking to the product API. Read from `client.js`'s
 *  one recorded probe; never re-decided here. */
export function inConsole(modeOf) {
  return (modeOf || mode)() === CONSOLE;
}

/** One D3 object, in the CUSTOM RESOURCE's own shape in both modes.
 *
 *  THE CONSOLE ANSWER IS PROJECTED, NOT RENDERED DIRECTLY, and that is the
 *  same decision `ui/client.js` made for the five older kinds: one renderer,
 *  one vocabulary, and a page that cannot read one way in one mode and another
 *  way in the other. The projection is mechanical -- identity into
 *  `metadata`, `spec` and `status` verbatim -- because D3's controllers and
 *  D3's routes publish the same status contract (D3 sections 3.1, 5.3, 6.2,
 *  7.1). */
export async function readD3(family, ns, name, options, deps) {
  const route = D3_ROUTES[family];
  if (route === undefined) {
    throw contractFailure("D3Route", String(family), "no such D3 family");
  }
  const d = deps || {};
  if (inConsole(d.modeOf)) {
    const answer = route.cluster === true
      ? await (d.consoleClusterGet || consoleClusterGet)(route.console, name, options)
      : await (d.consoleGet || consoleGet)(ns, route.console, name, options);
    return projectD3(family, decodeD3Item(route.console, answer).value.item, ns);
  }
  const api = d.api || apiClient();
  return route.cluster === true
    ? oneOf_(await api.listCluster(route.legacy, options), name)
    : await api.get(ns, route.legacy, name, options);
}

/** Every D3 object of one family in `ns`, in the custom resource's shape. */
export async function listD3(family, ns, options, deps) {
  const route = D3_ROUTES[family];
  if (route === undefined) {
    throw contractFailure("D3Route", String(family), "no such D3 family");
  }
  const d = deps || {};
  if (inConsole(d.modeOf)) {
    const answer = route.cluster === true
      ? await (d.consoleClusterList || consoleClusterList)(route.console, options)
      : await (d.consoleList || consoleList)(ns, route.console, options);
    const decoded = decodeD3List(route.console, answer);
    return {
      apiVersion: "logweir.dev/v1alpha1",
      kind: route.console + "List",
      items: decoded.value.items.map((item) => projectD3(family, item, ns)),
      __page: decoded.value.page,
    };
  }
  const api = d.api || apiClient();
  return route.cluster === true
    ? await api.listCluster(route.legacy, options)
    : await api.list(ns, route.legacy, options);
}

/** ONE PAGE OF A CATALOG'S POINTS. CONSOLE ONLY, AND SAID SO BY NAME.
 *
 *  The materialised view lives in page ConfigMaps the sync Job owns, and the
 *  page holds no verb on `configmaps` -- deliberately (D2 section 5.6, the
 *  chart's own ClusterRole): reading those pages means checking their owner
 *  UID, their immutability and their digest, which is the console API's job
 *  and not a browser's. So in legacy mode the catalog view renders the
 *  catalog's own `status` -- counts, signers, conditions, the sync job -- and
 *  refuses the point list by name instead of showing an empty table. */
export async function readCatalogPoints(ns, name, query, options, deps) {
  const d = deps || {};
  if (!inConsole(d.modeOf)) {
    throw noD3Route("A recovery catalog's point list");
  }
  const answer = await (d.consoleSub || consoleSub)(
    ns, "catalogs", name, "points", Object.assign({}, query || {}, options || {}),
  );
  return decodeCatalogPoints(answer).value;
}

/** The untrusted-signer panel's data. Console only, for the same reason. */
export async function readCatalogSigners(ns, name, options, deps) {
  const d = deps || {};
  if (!inConsole(d.modeOf)) {
    throw noD3Route("A recovery catalog's signer list");
  }
  return decodeCatalogSigners(
    await (d.consoleSub || consoleSub)(ns, "catalogs", name, "signers", options || {}),
  ).value;
}

/** CONNECT AN EXISTING ARCHIVE (D3 section 5.5 step 1).
 *
 *  A DURABLE CREATE, WITH THE KEY THAT MAKES IT ONE. The body is checked
 *  against the published request shape before anything is sent -- a field this
 *  page invented is a named contract failure here rather than a 422 in front
 *  of an operator -- and the `Idempotency-Key` is what makes a double click, a
 *  lost response and a reload all resolve to the catalog the first request
 *  made.
 *
 *  IT CREATES A CATALOG AND NOT A TRUST DECISION. Connecting an archive says
 *  "read this bucket and tell me what is in it". Every point it finds signed
 *  by a key this installation does not list comes back `UntrustedSigner`, and
 *  nothing on this page can change that; see `ui/pages/catalog.js`. */
export async function connectArchive(ns, body, key, deps) {
  const d = deps || {};
  if (!inConsole(d.modeOf)) {
    throw noD3Route("Connecting an existing archive");
  }
  const checked = decodeCatalogRequest(body);
  if (checked.unknown.length > 0) {
    throw contractFailure(
      "CreateCatalogRequest",
      checked.unknown[0],
      "this page built a field the published request schema does not declare, and a mutation " +
        "input that carries one is a 422 from the product API",
    );
  }
  // THE TOKEN IS THE SESSION'S, READ FROM THE TYPED CLIENT AT SEND TIME
  // (POC-P4). This used to be `deps.token`, which no caller supplies -- the
  // shell mounts the catalog page with no `deps` at all -- so the request
  // went out without `X-CSRF-Token` and the shared console answered every
  // "Connect an existing archive" with 403. It is not a `deps` seam any more:
  // a write whose token can be handed in is a write whose token can be wrong.
  const answer = await (d.consoleCreate || consoleCreate)(ns, "catalogs", body, {
    idempotencyKey: key,
    token: sessionToken(),
  });
  const decoded = decodeD3Item("catalogs", answer);
  const made = projectD3("catalog", decoded.value.item, ns);
  made.__replayed = decoded.value.replayed === true;
  return made;
}

/** ONE OPERATION, NORMALIZED (console) OR AS THE CONTROLLER WROTE IT (legacy).
 *
 *  The two are DIFFERENT DOCUMENTS and the caller is told which it got.
 *  Console mode returns the product API's `Operation`, whose `state` is one of
 *  ten normalized words `logweir-api` computes. Legacy mode returns the custom
 *  resource, whose `status.progress` is the controller's own stage and reason
 *  -- and NO normalized state at all, because computing one here would be a
 *  second implementation of D3 section 2.5's table living in a browser. */
export async function readOperation(ns, kind, name, options, deps) {
  const d = deps || {};
  if (inConsole(d.modeOf)) {
    return decodeD3Operation(
      await (d.consoleOperation || consoleOperation)(ns, kind, name, options),
    ).value.item;
  }
  const api = d.api || apiClient();
  return api.get(ns, kind === "backup" ? "backups" : "restores", name, options);
}

// --------------------------------------------------------------- the private

/** THE FLAT VIEW INTO THE CUSTOM RESOURCE'S OWN THREE BLOCKS, PER FAMILY.
 *
 *  The published D3 views are FLAT -- "DTOs are separate from CRDs" is
 *  PLAT-17.1's rule and every projection this console already reads is flat.
 *  The page renderers speak the custom resource's vocabulary, because legacy
 *  mode hands them exactly that, so the projection happens here, once, in a
 *  table a reader can check field by field against
 *  `schemas/logweir-api-v1.openapi.json`. It is the same decision
 *  `ui/client.js` made for the five older kinds and it buys the same thing: one
 *  renderer, one vocabulary, and no page that reads one way in one mode and
 *  another way in the other.
 *
 *  NOTHING IS INVENTED HERE. A field the view does not carry is absent from
 *  the projection, and every renderer already treats an absent field as "not
 *  observed" (D3 section 12). Three of them are named in the projections
 *  below, because a reader looking for a Job name, a ConfigMap name or a
 *  `locationDigest` will not find one and should learn why from the code
 *  rather than from a blank cell. */
const D3_PROJECTIONS = Object.freeze({
  protection: projectProtection,
  catalog: projectCatalog,
  retention: projectRetention,
  trust: projectTrust,
});

function projectD3(family, item, ns) {
  const project = D3_PROJECTIONS[family];
  return project === undefined ? item : project(item, ns);
}

/** The identity block every projection starts from. */
function metaOf(item, ns) {
  const meta = { name: item.name, uid: item.uid, resourceVersion: item.resourceVersion };
  if (typeof item.namespace === "string") {
    meta.namespace = item.namespace;
  } else if (typeof ns === "string" && ns.length > 0) {
    meta.namespace = ns;
  }
  if (typeof item.generation === "number") {
    meta.generation = item.generation;
  }
  if (typeof item.createdAt === "string") {
    meta.creationTimestamp = item.createdAt;
  }
  return meta;
}

function shell(item, ns, kind, spec, status) {
  const object = {
    apiVersion: "logweir.dev/v1alpha1",
    kind: kind,
    metadata: metaOf(item, ns),
    spec: spec,
    status: status,
    __contract: { console: true },
  };
  return object;
}

/** `ProtectionPolicyView` -> `ProtectionPolicy`. The three flattened groups --
 *  `missed`, `rehearsal` and the objectives -- go back into the nesting the
 *  CRD has and the renderer reads. */
function projectProtection(item, ns) {
  return shell(item, ns, "ProtectionPolicy", {
    protects: item.protects,
    objectives: item.objectives,
    notifications: item.notifications,
    evaluationIntervalSeconds: item.evaluationIntervalSeconds,
  }, {
    observedGeneration: item.observedGeneration,
    evaluatedAt: item.evaluatedAt,
    health: item.health,
    availabilityBasis: item.availabilityBasis,
    lastAvailablePoint: item.lastAvailablePoint,
    lastAttempt: item.lastAttempt,
    consecutiveFailedRuns: item.consecutiveFailedRuns,
    missed: { lastMissedSlot: item.lastMissedSlot, sinceLastFire: item.sinceLastFire },
    schedules: item.schedules,
    rehearsal: rehearsalOf(item),
    staleSince: item.staleSince,
    alerts: item.alerts,
    conditions: item.conditions,
  });
}

/** The rehearsal block, or `null` when the view carries none of its four
 *  flattened fields -- so an absent rehearsal stays absent rather than
 *  becoming an empty panel. */
function rehearsalOf(item) {
  const block = {
    lastSucceededAt: item.rehearsalLastSucceededAt,
    lastFailedAt: item.rehearsalLastFailedAt,
    lastReason: item.rehearsalLastReason,
    lastRestoreRef: item.rehearsalLastRestoreRef,
  };
  for (const key of Object.keys(block)) {
    if (block[key] !== null && block[key] !== undefined) {
      return block;
    }
  }
  return null;
}

/** `CatalogView` -> `RecoveryCatalog`.
 *
 *  NO `status.pages` AND NO `status.indexConfigMap`: those are ConfigMap names,
 *  which this API does not publish and this page holds no verb on. The facts
 *  that replace them are `viewPoints`, `truncated` and `viewExpired`. */
function projectCatalog(item, ns) {
  return shell(item, ns, "RecoveryCatalog", {
    destinationRef: item.destinationRef,
    legacyArchive: item.legacyArchive,
    sync: {
      intervalSeconds: item.intervalSeconds,
      mode: item.mode,
      deepCheck: item.deepCheck,
      viewLimit: item.viewLimit,
    },
  }, {
    observedGeneration: item.observedGeneration,
    observedSyncRequest: item.observedSyncRequest,
    syncedAt: item.syncedAt,
    viewExpiresAt: item.viewExpiresAt,
    viewExpired: item.viewExpired,
    viewPoints: item.viewPoints,
    cursor: item.cursor,
    counts: item.counts,
    truncated: item.truncated,
    histogram: item.histogram,
    signers: item.signers,
    lastSyncJob: item.lastSync,
    conditions: item.conditions,
  });
}

/** `RetentionPolicyView` -> `RetentionPolicy`. `scopePrefix` goes back under
 *  `scope.prefix`, the three rules back under `rules`, and
 *  `enforcementSettings` back under `enforcement` -- which is the CRD's name
 *  for the SPEC block, and is not `status.enforcement`, the word for what is
 *  actually happening. The API kept them apart by renaming one; this puts them
 *  back where the renderer reads them, in their two different blocks. */
function projectRetention(item, ns) {
  return shell(item, ns, "RetentionPolicy", {
    destinationRef: item.destinationRef,
    catalogRef: item.catalogRef,
    scope: { prefix: item.scopePrefix },
    rules: {
      keepLast: item.keepLast,
      keepDays: item.keepDays,
      minUsablePoints: item.minUsablePoints,
    },
    holds: item.holds,
    mode: item.mode,
    externalLifecycle: item.externalLifecycle,
    enforcement: item.enforcementSettings,
  }, {
    observedGeneration: item.observedGeneration,
    enforcement: item.enforcement,
    guarantees: item.guarantees,
    lastEvaluation: item.lastEvaluation,
    lastEnforcement: item.lastEnforcement,
    leasedPoints: item.leasedPoints,
    consecutiveRunFailures: item.consecutiveRunFailures,
    approvedPlanState: item.approvedPlanState,
    enforcementDegraded: item.enforcementDegraded,
    conditions: item.conditions,
  });
}

/** `TrustPolicyView` -> `TrustPolicy`, plus the two facts the custom resource
 *  has no room for.
 *
 *  ONE ROW PER KEY BECOMES TWO HALVES AGAIN. The view carries the declaration
 *  and the verdict on one object, which is what section 7.7 asks a ROW to
 *  show; the CRD keeps them in `spec.keys[]` and `status.keys[]`, and the
 *  renderer joins them. Splitting the one row back into two is mechanical and
 *  loses nothing: every field goes to exactly one side.
 *
 *  `__evaluation` and `__namespacesFiltered` are carried under names no custom
 *  resource has, because no custom resource has them: the API decides
 *  freshness and says when a namespace list was narrowed to what this actor
 *  administers. The keys page reads them when they are there. */
function projectTrust(item, ns) {
  const keys = Array.isArray(item.keys) ? item.keys : [];
  const object = shell(item, ns, "TrustPolicy", {
    default: item.default,
    namespaces: item.namespaces,
    allowedTargetClusterIds: item.allowedTargetClusterIds,
    keys: keys,
  }, {
    observedGeneration: item.observedGeneration,
    evaluatedAt: (item.evaluation || {}).evaluatedAt,
    loaded: item.loaded,
    keyCount: item.keyCount,
    keys: keys.map((key) => ({
      keyId: key.keyId,
      effectiveState: key.effectiveState,
      usableForNewSignatures: key.usableForNewSignatures,
      usableForVerification: key.usableForVerification,
    })),
    boundNamespaces: item.boundNamespaces,
    conflicts: item.conflicts,
    conditions: item.conditions,
  });
  delete object.metadata.namespace;
  // THE EVALUATION IS RE-KEYED ON THE WAY THROUGH, for one reason a reader
  // should not have to guess at: `ui_lint.rs::the_suspend_toggle_is_the_only_
  // update` forbids a page module from naming any `ui/api.js` export outside
  // the six a page may use, and the instant this block carries happens to share
  // its spelling with one of them. The rule is right and is not weakened for a
  // field name, so the rename happens here, in the module that already
  // translates between the two documents.
  object.__evaluation = item.evaluation === null || item.evaluation === undefined
    ? null
    : {
      state: item.evaluation.state,
      reason: item.evaluation.reason,
      decidedAt: item.evaluation.serverTime,
      freshWithinSeconds: item.evaluation.freshWithinSeconds,
      ageSeconds: item.evaluation.ageSeconds,
      evaluatedAt: item.evaluation.evaluatedAt,
    };
  object.__namespacesFiltered = item.namespacesFiltered === true;
  object.__keysTruncated = item.keysTruncated === true;
  object.__namespacesTruncated = item.namespacesTruncated === true;
  return object;
}

/** One named object out of a cluster-scoped legacy list, or `null`. A name
 *  that answers to nothing is a state the page renders, not a throw. */
function oneOf_(collection, name) {
  const items = (collection && Array.isArray(collection.items)) ? collection.items : [];
  for (const object of items) {
    if (((object.metadata || {}).name) === name) {
      return object;
    }
  }
  return null;
}
